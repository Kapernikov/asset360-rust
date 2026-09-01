//! The plan is the program; the executor is a loop over it.
//!
//! Earlier revisions treated pushdown as a *verdict*: the planner said eligible
//! or blocked, and the caller chose one of two whole-query routes. That put the
//! query's meaning in the caller. `views.py` grew a routing branch, a
//! fallback-viability heuristic, a feature-detection flag and a new 422 --
//! sixty lines of the executor guessing at what the planner already knew.
//!
//! Worse, a plan made of optional lists can be *under-read*. A consumer built
//! before `Star::path_filters` existed ignored it and answered 21956 where the
//! answer was 4108, and called the plan exact while doing so. A plan that can
//! be under-read will be.
//!
//! So an [`ExecutionPlan`] describes an ordered set of [`Pass`]es and the
//! [`Obligation`]s each one discharges. Two rules make it trustworthy:
//!
//! * **The ledger balances.** Every obligation of the query appears exactly
//!   once, in a pass or in [`ExecutionPlan::residual`]. `residual.is_empty()`
//!   is what "the plan describes the whole query" used to mean as a flag.
//! * **The executor fails closed.** It refuses a `contract` it does not know
//!   and a pass kind it cannot name, rather than running the part it
//!   understands. A forgotten field then costs an error instead of a plausible
//!   number.
//!
//! The default direction is inherited from the scoper's working set and must
//! not be inverted: an obligation is residual *unless* a pass claims it. That
//! inversion is what turned four silent drop sites into loud ones.

use std::collections::{BTreeSet, HashMap};
use std::fmt;

use spargebra::Query;

use crate::sparql_scoper::{Inexact, ScopeError};

/// Bumped when a pass kind or an obligation kind is added.
///
/// An executor that does not recognise the version refuses the plan. That makes
/// a planner/executor version skew a loud failure rather than a wrong number,
/// which is the failure this whole module is shaped around.
pub const PLAN_CONTRACT: u32 = 1;

/// Index into [`ExecutionPlan::obligations`]. Printed as `o1`, `o2`, ... so a
/// human can check the ledger by eye.
pub type ObligationId = usize;

/// One thing the query asks for.
///
/// Granularity is per triple pattern and per filter, which is what the scoper
/// already tracks. Finer would let a pass discharge half a pattern and make
/// "exactly once" harder to check for no benefit anyone has needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Obligation {
    /// `?s a asset360:Signal` -- the pattern that scopes a subject to a class.
    Type { subject: String, class_iri: String },
    /// Any other triple pattern.
    Triple {
        subject: String,
        predicate: String,
        object: String,
    },
    /// A `FILTER`, or a constant in the object position, that constrains values.
    Filter { detail: String },
    /// `GROUP BY`.
    Group { variables: Vec<String> },
    /// One aggregate, under the name the query gave it.
    Aggregate { variable: String, function: String },
    /// `ORDER BY`.
    Order { detail: String },
    /// `LIMIT` / `OFFSET`.
    Slice { limit: Option<usize>, offset: usize },
    /// `DISTINCT`.
    Distinct,
}

impl fmt::Display for Obligation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Type { subject, class_iri } => {
                write!(f, "type      {subject} a {}", shorten(class_iri))
            }
            Self::Triple {
                subject,
                predicate,
                object,
            } => write!(
                f,
                "triple    {subject} {} {}",
                shorten(predicate),
                shorten(object)
            ),
            Self::Filter { detail } => write!(f, "filter    {detail}"),
            Self::Group { variables } => {
                write!(f, "group     GROUP BY {}", variables.join(" "))
            }
            Self::Aggregate { variable, function } => {
                write!(f, "aggregate {function} AS {variable}")
            }
            Self::Order { detail } => write!(f, "order     {detail}"),
            Self::Slice { limit, offset } => match limit {
                Some(limit) => write!(f, "slice     LIMIT {limit} OFFSET {offset}"),
                None => write!(f, "slice     OFFSET {offset}"),
            },
            Self::Distinct => write!(f, "distinct  DISTINCT"),
        }
    }
}

/// What a pass does. Closed: an executor that meets a kind it does not know
/// refuses the plan rather than skipping the pass.
#[derive(Debug, Clone)]
pub enum PassKind {
    /// Rows out of Postgres. The operator set is closed -- scan, filter,
    /// unnest, group, aggregate -- and does not grow with the query language.
    ///
    /// Boxed because it carries the whole star decomposition: an unboxed
    /// variant would make every `Engine` pass as large as an `Sql` one.
    Sql(Box<SqlPass>),
    /// The remaining algebra, evaluated by the engine over this pass's inputs.
    Engine(EnginePass),
}

/// The SQL leaf.
///
/// Holds the star decomposition the scoper already produces, and the aggregate
/// spec when the pass groups. Splitting those two apart is not worth a new
/// representation: the renderer reads both today and they describe one scan.
#[derive(Debug, Clone)]
pub struct SqlPass {
    pub plan: crate::sparql_scoper::QueryPlan,
    pub solution: Option<crate::sparql_pushdown::SolutionSpec>,
}

/// The engine leg: what the SQL passes could not answer.
#[derive(Debug, Clone)]
pub struct EnginePass {
    /// Why the engine is needed at all -- the causes recorded where the
    /// planner dropped something. Empty when the engine runs for a reason
    /// other than a loss (it does not, today).
    pub causes: Vec<Inexact>,
}

/// One step of the plan.
#[derive(Debug, Clone)]
pub struct Pass {
    pub id: usize,
    /// Ids of the passes whose solutions this one consumes. Several, because a
    /// join has two sides -- so the passes form a tree, not a pipeline.
    pub inputs: Vec<usize>,
    /// What this pass enforces. The ledger is the union of these plus the
    /// residual.
    pub discharges: Vec<ObligationId>,
    /// Variables this pass binds.
    pub emits: Vec<String>,
    pub kind: PassKind,
}

/// One artifact: what is selected, with which filters and aggregates, and what
/// runs after.
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    pub contract: u32,
    pub obligations: Vec<Obligation>,
    pub passes: Vec<Pass>,
    /// Obligations no pass discharges. Empty means the passes together answer
    /// exactly the question asked.
    pub residual: Vec<ObligationId>,
}

impl ExecutionPlan {
    /// Whether the passes answer the whole question.
    ///
    /// This is the exactness rule that used to be a flag beside the plan,
    /// derived instead of asserted.
    pub fn is_exact(&self) -> bool {
        self.residual.is_empty()
    }

    /// Every obligation appears exactly once, in a pass or in the residual.
    ///
    /// The invariant the design rests on, checkable rather than argued. Returns
    /// the ids that are missing and the ones claimed twice.
    pub fn ledger_balances(&self) -> Result<(), LedgerError> {
        let mut seen: Vec<usize> = vec![0; self.obligations.len()];
        for pass in &self.passes {
            for id in &pass.discharges {
                if let Some(slot) = seen.get_mut(*id) {
                    *slot += 1;
                }
            }
        }
        for id in &self.residual {
            if let Some(slot) = seen.get_mut(*id) {
                *slot += 1;
            }
        }
        let missing: Vec<ObligationId> = seen
            .iter()
            .enumerate()
            .filter(|(_, count)| **count == 0)
            .map(|(id, _)| id)
            .collect();
        let duplicated: Vec<ObligationId> = seen
            .iter()
            .enumerate()
            .filter(|(_, count)| **count > 1)
            .map(|(id, _)| id)
            .collect();
        if missing.is_empty() && duplicated.is_empty() {
            Ok(())
        } else {
            Err(LedgerError {
                missing,
                duplicated,
            })
        }
    }
}

/// An unbalanced ledger: the plan does not account for the query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerError {
    pub missing: Vec<ObligationId>,
    pub duplicated: Vec<ObligationId>,
}

impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "plan ledger does not balance: {} unaccounted, {} claimed twice",
            self.missing.len(),
            self.duplicated.len()
        )
    }
}

/// Human-readable, and complete: an obligation that is not in this string is
/// not in the plan.
///
/// Written for two readers. Someone debugging a slow or wrong answer sees which
/// filters reached SQL and which did not, with the term shape that decides
/// whether a comparison is a number or text. And a reviewer reads a corpus of
/// plans instead of the planner -- every `oN` appears exactly once below, so
/// the ledger can be checked by eye.
impl fmt::Display for ExecutionPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let exactness = if self.is_exact() {
            "exact".to_owned()
        } else {
            format!("INEXACT — {} obligation(s) unclaimed", self.residual.len())
        };
        writeln!(f, "ExecutionPlan (contract {}, {exactness})", self.contract)?;

        for pass in &self.passes {
            match &pass.kind {
                PassKind::Sql(sql) => {
                    let classes: Vec<&str> = sql
                        .plan
                        .root
                        .all_stars()
                        .iter()
                        .map(|star| star.class_uri.as_str())
                        .collect();
                    writeln!(
                        f,
                        "  pass {}  SQL     {}{}",
                        pass.id,
                        classes
                            .iter()
                            .map(|class| shorten(class))
                            .collect::<Vec<_>>()
                            .join(", "),
                        emits(&pass.emits)
                    )?;
                    write_sql_body(f, sql)?;
                }
                PassKind::Engine(engine) => {
                    writeln!(
                        f,
                        "  pass {}  ENGINE  input {:?}{}",
                        pass.id,
                        pass.inputs,
                        emits(&pass.emits)
                    )?;
                    for cause in &engine.causes {
                        writeln!(f, "      because   {} — {}", cause.as_str(), cause.detail())?;
                    }
                }
            }
            if !pass.discharges.is_empty() {
                writeln!(f, "      discharges {}", ids(&pass.discharges))?;
            }
        }

        if self.residual.is_empty() {
            writeln!(f, "  residual  (empty)")?;
        } else {
            writeln!(f, "  residual")?;
            for id in &self.residual {
                if let Some(obligation) = self.obligations.get(*id) {
                    writeln!(f, "      o{id}  {obligation}")?;
                }
            }
        }

        writeln!(f, "\nobligations")?;
        for (id, obligation) in self.obligations.iter().enumerate() {
            writeln!(f, "  o{id}  {obligation}")?;
        }
        Ok(())
    }
}

fn emits(vars: &[String]) -> String {
    if vars.is_empty() {
        String::new()
    } else {
        format!("   → {}", vars.join(" "))
    }
}

fn ids(ids: &[ObligationId]) -> String {
    ids.iter()
        .map(|id| format!("o{id}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn write_sql_body(f: &mut fmt::Formatter<'_>, sql: &SqlPass) -> fmt::Result {
    for star in sql.plan.root.all_stars() {
        writeln!(
            f,
            "      scan      {}  as ?{}",
            shorten(&star.class_uri),
            star.variable
        )?;
        if !star.identifier_values.is_empty() {
            writeln!(f, "      identity  {}", star.identifier_values.join(", "))?;
        }
        let mut fields: Vec<&String> = star.filters.keys().collect();
        fields.sort();
        for field in fields {
            for condition in &star.filters[field] {
                writeln!(f, "      filter    {field} {condition}")?;
            }
        }
        for path_filter in &star.path_filters {
            for condition in &path_filter.conditions {
                writeln!(
                    f,
                    "      filter    {} {condition}{}",
                    path_filter.slot_path.join("."),
                    if path_filter.numeric {
                        "   numeric"
                    } else {
                        ""
                    }
                )?;
            }
        }
        for field in &star.multivalued_fields {
            writeln!(f, "      unnest    {field}")?;
        }
    }

    let Some(solution) = &sql.solution else {
        return Ok(());
    };
    let bindings = &solution.bindings;
    for key in &solution.group_keys {
        if let Some(binding) = bindings.get(*key) {
            writeln!(
                f,
                "      group     ?{} ← {}   {}",
                binding.var,
                if binding.slot_path.is_empty() {
                    "<identity>".to_owned()
                } else {
                    binding.slot_path.join(".")
                },
                binding.descriptor.shape()
            )?;
        }
    }
    for measure in &solution.measures {
        writeln!(
            f,
            "      aggregate ?{} ← {}",
            measure.var,
            measure.func.render()
        )?;
    }
    for order in &solution.order_by {
        writeln!(f, "      order     {order}")?;
    }
    if solution.distinct {
        writeln!(f, "      distinct")?;
    }
    if solution.limit.is_some() || solution.offset > 0 {
        writeln!(
            f,
            "      limit     {} offset {}",
            solution
                .limit
                .map(|limit| limit.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            solution.offset
        )?;
    }
    Ok(())
}

/// Enumerate what a query asks for.
///
/// One pass over the algebra, in a fixed order, so the same query always
/// produces the same ids -- a plan string is only reviewable if it is stable.
pub fn obligations_of(query: &Query) -> Result<Vec<Obligation>, ScopeError> {
    let mut out = Vec::new();
    let pattern = match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => pattern,
    };

    let mut triples = Vec::new();
    crate::sparql_scoper::tag_triples_by_depth(pattern, 0, &mut triples)?;
    for (triple, _depth) in triples {
        let subject = term_text(&triple.subject.clone());
        let object = term_text(&triple.object);
        match &triple.predicate {
            spargebra::term::NamedNodePattern::NamedNode(node)
                if node.as_str() == crate::sparql_scoper::RDF_TYPE =>
            {
                out.push(Obligation::Type {
                    subject,
                    class_iri: strip_angles(&object),
                });
            }
            predicate => out.push(Obligation::Triple {
                subject,
                predicate: format!("{predicate}"),
                object,
            }),
        }
    }

    // `(COUNT(*) AS ?n)` is a `Group` binding an internal variable plus an
    // `Extend` aliasing it to `?n`, so the name the author wrote is one level
    // up from the aggregate. Printing the internal one gives
    // `?bd1a8feb2eb7c1b061520830b70d6d3a`, which is not a plan a human reads.
    let mut aliases = HashMap::new();
    collect_aliases(pattern, &mut aliases);
    collect_modifiers(pattern, &aliases, &mut out);
    Ok(out)
}

/// Internal aggregate variable → the name the query gave it.
fn collect_aliases(pattern: &spargebra::algebra::GraphPattern, out: &mut HashMap<String, String>) {
    use spargebra::algebra::{Expression, GraphPattern};
    match pattern {
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            if let Expression::Variable(source) = expression {
                out.insert(format!("{source}"), format!("{variable}"));
            }
            collect_aliases(inner, out);
        }
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Filter { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. } => collect_aliases(inner, out),
        _ => {}
    }
}

fn collect_modifiers(
    pattern: &spargebra::algebra::GraphPattern,
    aliases: &HashMap<String, String>,
    out: &mut Vec<Obligation>,
) {
    use spargebra::algebra::GraphPattern;
    match pattern {
        GraphPattern::Filter { expr, inner } => {
            out.push(Obligation::Filter {
                detail: format!("{expr}"),
            });
            collect_modifiers(inner, aliases, out);
        }
        GraphPattern::Group {
            variables,
            aggregates,
            inner,
        } => {
            if !variables.is_empty() {
                out.push(Obligation::Group {
                    variables: variables.iter().map(|v| format!("{v}")).collect(),
                });
            }
            for (variable, aggregate) in aggregates {
                let internal = format!("{variable}");
                out.push(Obligation::Aggregate {
                    variable: aliases.get(&internal).cloned().unwrap_or(internal),
                    function: format!("{aggregate}"),
                });
            }
            collect_modifiers(inner, aliases, out);
        }
        GraphPattern::OrderBy { inner, expression } => {
            out.push(Obligation::Order {
                detail: expression
                    .iter()
                    .map(|e| format!("{e}"))
                    .collect::<Vec<_>>()
                    .join(", "),
            });
            collect_modifiers(inner, aliases, out);
        }
        GraphPattern::Distinct { inner } => {
            out.push(Obligation::Distinct);
            collect_modifiers(inner, aliases, out);
        }
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            out.push(Obligation::Slice {
                limit: *length,
                offset: *start,
            });
            collect_modifiers(inner, aliases, out);
        }
        GraphPattern::Project { inner, .. }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. } => collect_modifiers(inner, aliases, out),
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_modifiers(left, aliases, out);
            collect_modifiers(right, aliases, out);
        }
        _ => {}
    }
}

fn term_text(term: &spargebra::term::TermPattern) -> String {
    format!("{term}")
}

/// `<https://data.infrabel.be/asset360/Signal>` → `asset360:Signal`.
///
/// The prefixes the shared parser preloads, which are the ones a reader of a
/// plan has in their head. Anything else prints in full rather than guessing at
/// a prefix the query did not declare.
fn shorten(iri: &str) -> String {
    const KNOWN: [(&str, &str); 4] = [
        ("https://data.infrabel.be/asset360/", "asset360"),
        ("https://data.infrabel.be/asset360-rsm-subset/", "irsm"),
        ("http://www.w3.org/2001/XMLSchema#", "xsd"),
        ("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf"),
    ];
    let bare = iri.trim_start_matches('<').trim_end_matches('>');
    for (namespace, prefix) in KNOWN {
        if let Some(local) = bare.strip_prefix(namespace) {
            return format!("{prefix}:{local}");
        }
    }
    iri.to_owned()
}

fn strip_angles(text: &str) -> String {
    text.trim_start_matches('<')
        .trim_end_matches('>')
        .to_owned()
}

/// Ids of every obligation, for a pass that answers the whole query.
pub fn all_ids(obligations: &[Obligation]) -> Vec<ObligationId> {
    (0..obligations.len()).collect()
}

/// Ids not in `claimed`, in ascending order.
pub fn unclaimed(
    obligations: &[Obligation],
    claimed: &BTreeSet<ObligationId>,
) -> Vec<ObligationId> {
    (0..obligations.len())
        .filter(|id| !claimed.contains(id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparql_scoper::{parse_query, tests::test_schema_view};

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

    /// The obligations are what the query asks for, one entry each, in a stable
    /// order -- a plan string is only reviewable if the same query prints the
    /// same ids every time.
    #[test]
    fn a_query_enumerates_its_obligations() {
        let query = parse_query(&format!(
            "{PREFIX}SELECT ?kind (COUNT(*) AS ?n) WHERE {{ \
             ?s a asset360:Signal ; asset360:kind ?kind ; asset360:length ?len . \
             FILTER(?len >= 10) }} GROUP BY ?kind ORDER BY DESC(?n) LIMIT 5"
        ))
        .unwrap();

        let obligations = obligations_of(&query).unwrap();
        let rendered: Vec<String> = obligations.iter().map(|o| o.to_string()).collect();

        assert!(
            rendered.iter().any(|line| line.starts_with("type")),
            "the rdf:type is its own obligation: {rendered:#?}"
        );
        assert_eq!(
            rendered.iter().filter(|l| l.starts_with("triple")).count(),
            2,
            "one per non-type pattern: {rendered:#?}"
        );
        for expected in ["filter", "group", "aggregate", "order", "slice"] {
            assert!(
                rendered.iter().any(|line| line.starts_with(expected)),
                "no {expected} obligation in {rendered:#?}"
            );
        }

        // Stable: the same query twice gives the same ids, or a snapshot of a
        // plan is worthless as a diff.
        let again = obligations_of(&query).unwrap();
        assert_eq!(obligations, again);
    }

    /// The invariant, stated as a test rather than as prose: every obligation
    /// is claimed exactly once, by a pass or by the residual.
    #[test]
    fn the_ledger_must_balance() {
        let obligations = vec![
            Obligation::Distinct,
            Obligation::Group {
                variables: vec!["?a".to_owned()],
            },
        ];

        let balanced = ExecutionPlan {
            contract: PLAN_CONTRACT,
            obligations: obligations.clone(),
            passes: vec![Pass {
                id: 0,
                inputs: Vec::new(),
                discharges: vec![0],
                emits: vec!["?a".to_owned()],
                kind: PassKind::Engine(EnginePass { causes: Vec::new() }),
            }],
            residual: vec![1],
        };
        assert!(balanced.ledger_balances().is_ok());
        assert!(!balanced.is_exact(), "a non-empty residual is not exact");

        // The failure this catches: a pass silently not claiming something.
        let leaky = ExecutionPlan {
            residual: Vec::new(),
            ..balanced.clone()
        };
        assert_eq!(
            leaky.ledger_balances().unwrap_err().missing,
            vec![1],
            "an obligation no pass claims must be reported, not ignored"
        );

        // And the opposite: one obligation claimed twice, which would apply a
        // filter in two places and count its rows once too often.
        let mut doubled = balanced.clone();
        doubled.residual = vec![0, 1];
        assert_eq!(doubled.ledger_balances().unwrap_err().duplicated, vec![0]);
    }

    /// The plan prints completely: an obligation that is not in the string is
    /// not in the plan, so a reader can audit the ledger by eye.
    #[test]
    fn the_plan_prints_every_obligation_once() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?kind (COUNT(*) AS ?n) WHERE {{ \
             ?s a asset360:Signal ; asset360:kind ?kind }} GROUP BY ?kind"
        );
        let parsed = parse_query(&query).unwrap();
        let obligations = obligations_of(&parsed).unwrap();
        let scoped = crate::sparql_scoper::scope_parsed(&parsed, &sv).unwrap();

        let plan = ExecutionPlan {
            contract: PLAN_CONTRACT,
            obligations: obligations.clone(),
            passes: vec![Pass {
                id: 0,
                inputs: Vec::new(),
                discharges: all_ids(&obligations),
                emits: vec!["?kind".to_owned(), "?n".to_owned()],
                kind: PassKind::Sql(Box::new(SqlPass {
                    plan: scoped,
                    solution: None,
                })),
            }],
            residual: Vec::new(),
        };
        plan.ledger_balances().unwrap();

        let printed = plan.to_string();
        for id in 0..obligations.len() {
            assert!(
                printed.contains(&format!("o{id}")),
                "obligation o{id} is missing from:\n{printed}"
            );
        }
        assert!(printed.contains("exact"), "{printed}");
        assert!(printed.contains("pass 0  SQL"), "{printed}");
        assert!(printed.contains("scan"), "{printed}");
        assert!(printed.contains("residual  (empty)"), "{printed}");

        // Printed here so a failing run shows the format a human is meant to
        // read, not just an assertion.
        println!("{printed}");
    }
}
