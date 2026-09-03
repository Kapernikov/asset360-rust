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
//! [`Obligation`]s each one discharges.
//!
//! **The vocabulary, once:** an *obligation* is one thing the query demands --
//! a triple pattern, one conjunct of a filter, an inline table, the grouping,
//! an aggregate, the ordering, the limit. To *discharge* one is for a pass to take care of it. The *residual*
//! is whatever no pass took care of, which means the plan answers a different
//! question than the one asked. `o0`, `o1`, ... are just their positions in
//! the list, so a pass can point at them without repeating the text.
//!
//! Two rules make it trustworthy:
//!
//! * **The ledger balances.** Every obligation of the query appears exactly
//!   once, in a pass or in [`ExecutionPlan::residual`]. A non-empty residual
//!   is an obligation with no pass at all -- a planner bug, or a consumer that
//!   has no engine to fall back on. What the old `exact` flag actually asked
//!   is [`ExecutionPlan::sql_only`]: are all the passes SQL, so the question
//!   is answered without materialising anything.
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

use linkml_schemaview::schemaview::SchemaView;

use crate::sparql_scoper::{Inexact, ScopeError};

/// Bumped when a pass kind or an obligation kind is added.
///
/// An executor that does not recognise the version refuses the plan. That makes
/// a planner/executor version skew a loud failure rather than a wrong number,
/// which is the failure this whole module is shaped around.
///
/// 3 added [`crate::sparql_ops::Op::Filter`]'s reading, which is not a new
/// kind but *is* a new obligation on a renderer: one that ignores it renders a
/// containment test over an array as an equality on the column and matches
/// nothing. A consumer built against 2 renders that wrongly rather than
/// refusing, which is exactly the skew this number exists to make loud.
///
/// 2 added [`Obligation::Values`]. No consumer branches on this yet -- the
/// endpoint reads passes and renders obligations as text -- so the bump is the
/// marker the next consumer checks against, not a live gate. Splitting a
/// conjunction into one obligation per conjunct did *not* bump it: that
/// changes how many `Filter` obligations a query raises, not what kinds exist,
/// and a consumer that reads the list rather than counting it is unaffected.
pub const PLAN_CONTRACT: u32 = 3;

/// Index into [`ExecutionPlan::obligations`]. Printed as `o1`, `o2`, ... so a
/// human can check the ledger by eye.
pub type ObligationId = usize;

/// One thing the query asks for.
///
/// Granularity is per triple pattern and per *conjunct* of a filter. Per
/// filter as the query wrote it would make syntax decide the accounting --
/// spargebra conjoins `FILTER(a) FILTER(b)` into one node, so the same
/// question would raise one obligation or two depending on how it was typed --
/// and it would put a pass that pushes `a` and leaves `b` in the position of
/// having to split a claim. Finer than a conjunct would let a pass discharge
/// half a comparison, which "exactly once" cannot check for no benefit anyone
/// has needed.
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
    /// A `VALUES` block: an inline table the query joins against.
    ///
    /// Its own kind rather than a filter, because a `VALUES` that binds a
    /// variable nothing else binds is not a constraint on existing rows -- it
    /// *adds* rows and columns. Calling that a filter would let a consumer
    /// apply it as a `WHERE` and answer a narrower question than the query
    /// asked.
    Values { variables: Vec<String>, rows: usize },
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
            Self::Values { variables, rows } => write!(
                f,
                "values    VALUES {} × {rows} row(s)",
                variables.join(" ")
            ),
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

/// The SQL leaf: the pass the database executes, as operators.
///
/// It carried two other representations of itself until every consumer read
/// the nodes -- the star decomposition and, when it grouped, the whole
/// solution spec. Both were shaped for rendering, which is why a rewrite had
/// nothing local to edit, and keeping three descriptions of one pass is how a
/// consumer comes to read the stale one.
///
/// Scan, filter, join, unnest, group, sort, distinct, slice, project. Each
/// node carries the obligations it discharges, so a rewrite is checkable
/// against the ledger.
#[derive(Debug, Clone)]
pub struct SqlPass {
    pub ops: crate::sparql_ops::OpTree,
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
    /// Why an aggregate could not be answered in SQL, when that is what
    /// happened.
    ///
    /// The engine pass records *causes* — enough to explain a slow answer.
    /// This is the other audience: an aggregate the engine may not be able to
    /// answer at all, where the caller has to tell whoever wrote the query
    /// which construct blocked it and what shape to use instead. Keeping it on
    /// the artifact is what lets one call replace the two that each re-parsed.
    pub blocked: Option<crate::sparql_pushdown::Blocked>,
    /// Obligations no pass discharges. Empty means the passes together answer
    /// exactly the question asked.
    pub residual: Vec<ObligationId>,
    /// Whether these operators came from the refinement pipeline, and if not,
    /// why not. See [`plan_query_refined`].
    pub refinement: Refinement,
}

impl ExecutionPlan {
    /// Whether every obligation has a pass to enforce it.
    ///
    /// False means the plan answers a *different* question than the one asked,
    /// and the caller must refuse rather than run it.
    pub fn is_accounted(&self) -> bool {
        self.residual.is_empty()
    }

    /// Whether SQL answers the whole question, with no engine pass.
    ///
    /// The question the old `exact` flag was really being asked -- "can this be
    /// answered without materialising objects" -- and the one an SQL-only
    /// consumer (a stored minibi question, which has no fallback) must ask
    /// before running a plan.
    pub fn sql_only(&self) -> bool {
        self.passes
            .iter()
            .all(|pass| matches!(pass.kind, PassKind::Sql(_)))
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
        // Three states, and the difference matters to a reader. A plan whose
        // passes are all SQL answers without materialising anything. A plan
        // with an engine pass still answers, more slowly. A plan with a
        // residual answers *nothing* -- some obligation has no pass at all,
        // which is a planner bug or a consumer that refuses the engine.
        let state = if !self.residual.is_empty() {
            format!("UNACCOUNTED — {} obligation(s)", self.residual.len())
        } else if self.sql_only() {
            "all in SQL".to_owned()
        } else {
            "SQL narrows, engine finishes".to_owned()
        };
        writeln!(f, "ExecutionPlan (contract {}, {state})", self.contract)?;

        for pass in &self.passes {
            match &pass.kind {
                PassKind::Sql(sql) => {
                    let classes: Vec<&str> = sql
                        .ops
                        .nodes
                        .iter()
                        .filter_map(|node| match &node.op {
                            crate::sparql_ops::Op::Scan { class_uri, .. } => {
                                Some(class_uri.as_str())
                            }
                            _ => None,
                        })
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

        // A refusal is not a failure of the plan — the engine pass still
        // answers — but it is the reason SQL could not, and the thing to show
        // whoever wrote the query.
        if let Some(refusal) = &self.blocked {
            writeln!(f, "  not pushable")?;
            writeln!(f, "      code      {}", refusal.code.as_str())?;
            writeln!(f, "      because   {}", refusal.detail)?;
            if let Some(at) = &refusal.at {
                writeln!(f, "      at        {at}")?;
            }
            writeln!(f, "      instead   {}", refusal.instead())?;
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

/// Print an SQL pass, operator by operator.
///
/// Reads the nodes rather than the two structures it used to, so the printout
/// is the tree that runs: a rewrite shows up here, and a node this does not
/// name is a node nobody renders.
fn write_sql_body(f: &mut fmt::Formatter<'_>, sql: &SqlPass) -> fmt::Result {
    use crate::sparql_ops::{Enforcement, Op};

    for node in &sql.ops.nodes {
        match &node.op {
            Op::Scan {
                star_var,
                class_uri,
                identifier_values,
                is_optional,
                ..
            } => {
                writeln!(
                    f,
                    "      scan      {}  as ?{star_var}{}",
                    shorten(class_uri),
                    if *is_optional { "   optional" } else { "" }
                )?;
                if !identifier_values.is_empty() {
                    writeln!(f, "      identity  {}", identifier_values.join(", "))?;
                }
            }
            Op::Filter {
                slot_path,
                condition,
                enforcement,
                numeric,
                ..
            } => writeln!(
                f,
                "      filter    {} {condition}{}{}",
                slot_path.join("."),
                if *numeric { "   numeric" } else { "" },
                // Says whether removing this node would change the answer or
                // only the speed -- the question a rewrite has to ask.
                match enforcement {
                    Enforcement::Enforces => "",
                    Enforcement::Narrows => "   narrows",
                }
            )?,
            Op::Unnest { slot_path, .. } => writeln!(f, "      unnest    {}", slot_path.join("."))?,
            Op::Join {
                left_star,
                right_star,
                right_slot,
                kind,
                ..
            } => writeln!(
                f,
                "      join      ?{right_star}.{right_slot} = ?{left_star}{}",
                match kind {
                    crate::sparql_scoper::JoinType::Inner => "",
                    crate::sparql_scoper::JoinType::Left => "   left",
                }
            )?,
            Op::Group {
                bindings,
                keys,
                measures,
                ..
            } => {
                for key in keys {
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
                for measure in measures {
                    writeln!(
                        f,
                        "      aggregate ?{} ← {}",
                        measure.var,
                        measure.func.render()
                    )?;
                }
            }
            Op::Sort { terms, .. } => {
                for term in terms {
                    writeln!(f, "      order     {term}")?;
                }
            }
            Op::Distinct { .. } => writeln!(f, "      distinct")?,
            Op::Slice { limit, offset, .. } => writeln!(
                f,
                "      limit     {} offset {offset}",
                limit
                    .map(|limit| limit.to_string())
                    .unwrap_or_else(|| "-".to_owned())
            )?,
            // The projection is the pass's emitted variables, already printed
            // on the pass line.
            Op::Project { .. } => {}
        }
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
        out.push(obligation_of_triple(triple));
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

/// The obligation one triple pattern raises.
///
/// Extracted so a second planner can ask what a pattern obliges without
/// re-deriving the answer: [`crate::sparql_refine`] matches a node against
/// this rather than trusting that the obligation at the same index came from
/// the same triple.
pub(crate) fn obligation_of_triple(triple: &spargebra::term::TriplePattern) -> Obligation {
    let subject = term_text(&triple.subject);
    let object = term_text(&triple.object);
    match &triple.predicate {
        spargebra::term::NamedNodePattern::NamedNode(node)
            if node.as_str() == crate::sparql_scoper::RDF_TYPE =>
        {
            Obligation::Type {
                subject,
                class_iri: strip_angles(&object),
            }
        }
        predicate => Obligation::Triple {
            subject,
            predicate: format!("{predicate}"),
            object,
        },
    }
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
            push_filter_obligations(expr, out);
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
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            // The condition spargebra lifts out of `OPTIONAL { ... FILTER(x) }`
            // and into the join itself. Enumerated because the ledger's whole
            // value is that a dropped constraint cannot hide, and this one
            // could: the planner leaves it to the engine every time (it
            // decides whether the optional side *matched*, so pushing it drops
            // the rows the LEFT JOIN exists to keep), and with no obligation
            // for it a plan that lost it balanced anyway.
            if let Some(expression) = expression {
                push_filter_obligations(expression, out);
            }
            collect_modifiers(left, aliases, out);
            collect_modifiers(right, aliases, out);
        }
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_modifiers(left, aliases, out);
            collect_modifiers(right, aliases, out);
        }
        GraphPattern::Values {
            variables,
            bindings,
        } => out.push(Obligation::Values {
            variables: variables.iter().map(|v| format!("{v}")).collect(),
            rows: bindings.len(),
        }),
        _ => {}
    }
}

/// One obligation per top-level conjunct of a `FILTER`.
///
/// `FILTER(a) FILTER(b)` and `FILTER(a && b)` are the same query -- spargebra
/// conjoins the first form into the second -- so accounting for them
/// differently would make syntax decide the ledger. Per conjunct, a pass can
/// push the comparison it can express and leave the `REGEX` above it, each
/// claimed by the node that applies it; with one obligation for the
/// conjunction, pushing half of it would mean splitting a claim, which
/// "discharged exactly once" forbids.
///
/// Top-level conjuncts only. A disjunction stays whole: neither half of
/// `a || b` constrains anything on its own, so there is nothing a pass could
/// discharge separately.
fn push_filter_obligations(expr: &spargebra::algebra::Expression, out: &mut Vec<Obligation>) {
    let mut conjuncts = Vec::new();
    flatten_conjunction(expr, &mut conjuncts);
    for conjunct in conjuncts {
        out.push(Obligation::Filter {
            detail: format!("{conjunct}"),
        });
    }
}

fn flatten_conjunction<'e>(
    expr: &'e spargebra::algebra::Expression,
    out: &mut Vec<&'e spargebra::algebra::Expression>,
) {
    match expr {
        spargebra::algebra::Expression::And(left, right) => {
            flatten_conjunction(left, out);
            flatten_conjunction(right, out);
        }
        other => out.push(other),
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
pub(crate) fn shorten(iri: &str) -> String {
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

/// Plan a query: one parse, one scope, one analysis, one artifact.
///
/// The entry point the executor should use. `sparql_scope` and
/// `sparql_pushdown` remain for callers that predate this and each re-parse;
/// they answer questions this artifact already contains.
///
/// Two shapes come out today, and adding a capability changes which shape is
/// emitted rather than adding a branch to the executor:
///
/// * one `Sql` pass, when the SQL leaf can answer the whole question;
/// * an `Sql` scan feeding an `Engine` pass, when it cannot -- the scan still
///   narrows the rows, and the engine applies what is left.
pub fn plan_query(query_str: &str, schema_view: &SchemaView) -> Result<ExecutionPlan, ScopeError> {
    let parsed = crate::sparql_scoper::parse_query(query_str)?;
    let obligations = obligations_of(&parsed)?;
    let scoped = crate::sparql_scoper::scope_parsed(&parsed, schema_view)?;
    let verdict =
        crate::sparql_pushdown::analyse_pushdown_scoped(query_str, schema_view, Some(&scoped))?;

    // How many of the leading obligations are triples: `obligations_of` emits
    // them first, in the order `tag_triples_by_depth` produced, which is the
    // order the scoper's `unconsumed` indexes. Same source, same order -- the
    // one coupling here, asserted below rather than assumed.
    let triple_count = obligations
        .iter()
        .filter(|obligation| {
            matches!(
                obligation,
                Obligation::Type { .. } | Obligation::Triple { .. }
            )
        })
        .count();
    debug_assert!(
        obligations
            .iter()
            .take(triple_count)
            .all(|o| matches!(o, Obligation::Type { .. } | Obligation::Triple { .. })),
        "obligations_of must emit every triple before any modifier"
    );

    let emits = emitted_variables(&verdict, &scoped);
    // Taken before the verdict is consumed below. Only a refusal carries one:
    // a query that never asked for an aggregate has nothing to be told.
    let blocked = match &verdict {
        crate::sparql_pushdown::Pushdown::Blocked(refusal) => Some(refusal.clone()),
        _ => None,
    };

    if let crate::sparql_pushdown::Pushdown::Eligible { solution, plan } = verdict {
        // The analyser refuses anything the plan does not fully describe, so an
        // eligible verdict *is* the statement that SQL discharges everything.
        let discharges = all_ids(&obligations);
        let ops = crate::sparql_ops::lower_sql_pass(&plan, Some(&solution), &discharges, true);
        return Ok(ExecutionPlan {
            contract: PLAN_CONTRACT,
            passes: vec![Pass {
                id: 0,
                inputs: Vec::new(),
                discharges,
                emits,
                kind: PassKind::Sql(Box::new(SqlPass { ops })),
            }],
            residual: Vec::new(),
            obligations,
            blocked: None,
            refinement: Refinement::NotAttempted,
        });
    }

    // Otherwise SQL claims exactly the triples the scoper represented, and the
    // engine takes the rest. Claiming more would be the old mistake in a new
    // shape: a pass that says it enforced something it did not.
    let unconsumed: BTreeSet<usize> = scoped.unconsumed.iter().copied().collect();
    let sql_claims: Vec<ObligationId> = (0..triple_count)
        .filter(|index| !unconsumed.contains(index))
        .collect();
    let engine_claims: Vec<ObligationId> = (0..obligations.len())
        .filter(|id| !sql_claims.contains(id))
        .collect();

    let causes = scoped.inexact.into_iter().collect();
    Ok(ExecutionPlan {
        contract: PLAN_CONTRACT,
        passes: vec![
            Pass {
                id: 0,
                inputs: Vec::new(),
                discharges: sql_claims.clone(),
                emits: Vec::new(),
                kind: PassKind::Sql(Box::new(SqlPass {
                    ops: crate::sparql_ops::lower_sql_pass(&scoped, None, &sql_claims, false),
                })),
            },
            Pass {
                id: 1,
                inputs: vec![0],
                discharges: engine_claims,
                emits,
                kind: PassKind::Engine(EnginePass { causes }),
            },
        ],
        residual: Vec::new(),
        obligations,
        blocked,
        refinement: Refinement::NotAttempted,
    })
}

/// What happened when a plan was asked for through the refinement pipeline.
///
/// Carried on the artifact rather than logged in Rust, because the caller has
/// the query text and the logger. A fallback nobody can see is a fallback that
/// becomes permanent, and the point of the gate is to tell us when the gap is
/// actually closed instead of assuming it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refinement {
    /// The plan came from the single-pass planner; nothing was refined.
    NotAttempted,
    /// The refined plan's operators are this plan's operators.
    ///
    /// The note, when there is one, is a *ledger* difference the gate
    /// deliberately did not veto: the two statements read the same rows and do
    /// the same work, while the obligations SQL claims are not the same. That
    /// is the tier-two backlog, made observable rather than argued about --
    /// today it is an `OPTIONAL` read, where the refined plan declines to
    /// claim optionality a narrowing scan does not render.
    Used(Option<String>),
    /// The refined plan answers a question the single-pass planner refuses
    /// outright, and was admitted on its own soundness rather than by
    /// comparison. The string says what today's planner refused and why.
    ///
    /// A different risk from a substitution, so it reads differently in a log:
    /// nothing was compared, because there was nothing to compare against.
    UsedAlone(String),
    /// The refined plan was built and not used, for this reason.
    Fallback(String),
}

impl Refinement {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotAttempted => "not_attempted",
            Self::Used(_) => "used",
            Self::UsedAlone(_) => "used_alone",
            Self::Fallback(_) => "fallback",
        }
    }

    /// Why the fallback fired, for a log line.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Fallback(reason) => Some(reason),
            _ => None,
        }
    }

    /// What the refined plan claims differently, when it was used anyway --
    /// or, for [`Refinement::UsedAlone`], what today's planner refused.
    pub fn note(&self) -> Option<&str> {
        match self {
            Self::Used(note) => note.as_deref(),
            Self::UsedAlone(refusal) => Some(refusal),
            _ => None,
        }
    }
}

/// Plan a query, preferring the refinement pipeline, and fall back to
/// [`plan_query`] unless the refined plan's row source is no worse.
///
/// The gate is the whole point: **a regression is impossible by construction
/// rather than by argument.** Both planners run, and the refined operators are
/// used only when [`crate::sparql_ops::OpTree::is_no_worse_than`] can show the
/// lowered statement fetches no more rows and leaves no more work to the
/// engine. Anything it cannot show -- a rule that stopped firing, a shape tier
/// one does not cover, an aggregate today pushes whole -- keeps today's plan,
/// and the artifact says why so the caller can log it.
///
/// **The comparand was wrong at first, and the correction is the interesting
/// part.** The gate compared the *claim ledgers*: SQL had to claim a superset
/// of what today's SQL pass claimed. For every tier-one rule that coincides
/// with "does more work reach the engine" -- a rule that pushes work claims
/// what it pushed -- so the difference did not show until an `OPTIONAL` read.
/// There both statements deliver the same column, the SQL is identical, no
/// work moved, and the ledgers differ only because a narrowing scan declines
/// to claim optionality it does not render. Comparing claims rejected the
/// *more truthful* plan for a difference that costs nothing, and the missing
/// value bucket 28c calls D11 -- the report the whole feature exists for --
/// was the case it rejected. So the row source decides and the ledger is
/// reported: [`Refinement::note`] carries a claim difference the gate allowed,
/// which is the tier-two backlog made observable.
///
/// Nothing is adjusted to buy a route. The substituted passes carry the
/// refined plan's own claims, so an obligation SQL applies without claiming
/// stays the engine's and the ledger still balances.
///
/// The refined plan also has to be *lowerable*: its frontier must be one
/// island, since a pass is one statement. See
/// [`crate::sparql_ops::LoweringRefusal`].
/// The refined plan for a query, as text.
///
/// For diagnostics only, and it exists because a fallback hides its own
/// evidence: the artifact `plan_query_refined` returns carries today's
/// operators, so the plan the gate rejected is gone by the time anyone reads
/// the reason. This is that plan.
pub fn refined_plan_text(
    query: &str,
    schema: &linkml_schemaview::schemaview::SchemaView,
) -> Result<String, String> {
    let naive = crate::sparql_refine::naive_plan_of(query).map_err(|e| e.to_string())?;
    let rules = crate::sparql_rules::tier_one_rules(schema);
    let borrowed: Vec<&dyn crate::sparql_rules::Rule> =
        rules.iter().map(|rule| rule.as_ref()).collect();
    let mut plan = naive;
    crate::sparql_rules::refine(&mut plan, &borrowed).map_err(|failure| failure.to_string())?;
    Ok(plan.to_string())
}

pub fn plan_query_refined(
    query_str: &str,
    schema_view: &SchemaView,
) -> Result<ExecutionPlan, ScopeError> {
    let mut plan = plan_query(query_str, schema_view)?;
    let today: BTreeSet<ObligationId> = plan
        .passes
        .iter()
        .filter(|pass| matches!(pass.kind, PassKind::Sql(_)))
        .flat_map(|pass| pass.discharges.iter().copied())
        .collect();
    let Some(today_ops) = plan.passes.iter().find_map(|pass| match &pass.kind {
        PassKind::Sql(sql) => Some(sql.ops.clone()),
        PassKind::Engine(_) => None,
    }) else {
        // Every plan the planner emits has an SQL pass, even one the engine
        // finishes. Without one there is nothing to compare against, and a
        // comparison against nothing is not a gate.
        plan.refinement = Refinement::Fallback("the plan has no SQL pass".to_owned());
        return Ok(plan);
    };

    // What the fetch may skip, decided by the analysis that owns that
    // question. See `lower_refined`.
    let fetch_bound = crate::sparql_ops::fetch_bound_of(&today_ops);

    let refined = match refined_ops(query_str, schema_view, fetch_bound) {
        Ok(refined) => refined,
        Err(reason) => {
            plan.refinement = Refinement::Fallback(reason);
            return Ok(plan);
        }
    };

    // Which admission path, decided by whether there is a plan to compare
    // against at all -- and the two are *exclusive* rather than ordered.
    //
    // Path B is not a second chance for a plan path A refused. If the
    // comparator says a substitution is worse, that is a regression, and
    // trying a different argument for the same plan would convert it into a
    // capability claim. So a query today's planner can answer goes through the
    // comparator and nowhere else, and only a query it refuses outright
    // reaches the other path.
    let refused = plan.blocked.clone();
    if let Some(refusal) = refused {
        let because = format!(
            "the single-pass planner refuses this query ({}: {})",
            refusal.code.as_str(),
            refusal.detail
        );
        return Ok(admit_alone(plan, refined, &because));
    }

    // Path A: the gate.
    if let Err(reason) = refined.is_no_worse_than(&today_ops) {
        plan.refinement = Refinement::Fallback(reason);
        return Ok(plan);
    }

    // Reported, not vetoed: an obligation today's SQL pass claims and the
    // refined plan does not, while both statements read the same rows and do
    // the same work. See the note on [`Refinement::Used`].
    let claimed: BTreeSet<ObligationId> = refined.claims().into_iter().collect();
    let unclaimed: Vec<String> = today
        .difference(&claimed)
        .map(|id| {
            plan.obligations
                .get(*id)
                .map(ToString::to_string)
                .unwrap_or_else(|| format!("o{id}"))
        })
        .collect();
    let note = (!unclaimed.is_empty()).then(|| {
        format!(
            "the row sources agree while SQL applies but does not claim {}",
            unclaimed.join("; ")
        )
    });

    Ok(substitute(plan, refined, note))
}

/// Admission with the comparator removed — what the gate becomes once the
/// single-pass planner is gone.
///
/// **A rehearsal, and it exists to be run against the frozen inventory while
/// the old planner is still here to contradict it.** After the deletion every
/// query asks one question — does this plan answer the whole query in SQL, on
/// its own terms — and a plan that does not gets its statement used as a
/// *fetch*, with the engine finishing, which is what happens today whenever
/// the aggregate route refuses. There is no third outcome and nothing to
/// compare against, so a plan that would be wrongly admitted alone has
/// nothing standing between it and an answer. Finding those now is the whole
/// point.
///
/// It still calls [`plan_query`] for the artifact around the statement — the
/// obligation list, the passes, the fetch bound — so this is the *admission*
/// rehearsed, not the deletion. Owning those three things is the second half
/// of the work.
pub fn plan_query_refined_alone(
    query_str: &str,
    schema_view: &SchemaView,
) -> Result<ExecutionPlan, ScopeError> {
    let mut plan = plan_query(query_str, schema_view)?;
    let fetch_bound = plan
        .passes
        .iter()
        .find_map(|pass| match &pass.kind {
            PassKind::Sql(sql) => Some(crate::sparql_ops::fetch_bound_of(&sql.ops)),
            PassKind::Engine(_) => None,
        })
        .flatten();

    let refined = match refined_ops(query_str, schema_view, fetch_bound) {
        Ok(refined) => refined,
        Err(reason) => {
            plan.refinement = Refinement::Fallback(reason);
            return Ok(plan);
        }
    };

    // The one question, asked by the same predicate the gated path asks.
    if answers_alone(&plan, &refined).is_ok() {
        return Ok(admit_alone(plan, refined, "there is no comparator"));
    }
    // Not the whole query, so the statement is a fetch and the engine
    // finishes — the substitution, with no gate in front of it.
    Ok(substitute(plan, refined, None))
}

/// Put the refined statement in place of today's, and rebalance the ledger.
///
/// Shared by the gated path and by the rehearsal that has no gate: the
/// substitution is the same edit either way, and the difference is only
/// whether anything compared first.
fn substitute(
    mut plan: ExecutionPlan,
    refined: crate::sparql_ops::OpTree,
    note: Option<String>,
) -> ExecutionPlan {
    let claimed: BTreeSet<ObligationId> = refined.claims().into_iter().collect();
    // The ledger has to balance against the *new* division of labour: what SQL
    // now claims, the engine no longer does. Recomputing it rather than
    // trusting the two to agree is what keeps "exactly once" true across the
    // substitution.
    let sql_claims: Vec<ObligationId> = claimed.iter().copied().collect();
    let engine_claims: Vec<ObligationId> = (0..plan.obligations.len())
        .filter(|id| !claimed.contains(id))
        .collect();
    let has_engine = plan
        .passes
        .iter()
        .any(|pass| matches!(pass.kind, PassKind::Engine(_)));
    for pass in &mut plan.passes {
        match &mut pass.kind {
            PassKind::Sql(sql) => {
                sql.ops = refined.clone();
                pass.discharges = sql_claims.clone();
            }
            PassKind::Engine(_) => pass.discharges = engine_claims.clone(),
        }
    }
    // With an engine pass the complement has a home. Without one -- the route
    // where SQL answers alone -- anything SQL does not claim is unaccounted
    // for, and `is_accounted` is what stops a caller running a plan that
    // answers a different question. Reachable only if the refined plan claims
    // everything (the gate above compares against a pass that claims
    // everything), in which case the complement is empty; computing it anyway
    // is what makes that an assertion rather than an assumption.
    plan.residual = if has_engine {
        Vec::new()
    } else {
        engine_claims
    };
    plan.refinement = Refinement::Used(note);
    plan
}

/// Admit a refined plan for a question the single-pass planner refuses.
///
/// **Path B.** There is no plan to be no worse than, so admission rests on the
/// refined plan's own soundness -- and that is a *stronger* condition than the
/// comparator's, not a weaker one. The comparator says "this substitution
/// loses nothing"; this says "this statement answers the whole question by
/// construction":
///
/// * every node runs in SQL, so the statement *is* the query rather than a
///   fetch for an engine that finishes it. (The frontier being a cut is an
///   invariant, checked at every rule application, so an all-`Sql` plan has no
///   engine node hiding beneath one.)
/// * every obligation is discharged by those nodes, so the residual is empty
///   and the ledger balances.
/// * every invariant holds, `fanout_restored` in its strengthened form
///   included -- which is what makes a count over a fanned-out read a count of
///   solutions rather than of records.
///
/// Anything less falls back, and the reason says which condition failed.
///
/// The evidence is elsewhere and it is not an argument: **every shape admitted
/// this way carries an oracle test against the engine leg**, which materialises
/// instances and re-runs the whole query, so it answers shapes the SQL route
/// refuses. A comparator cannot judge these plans; the engine can.
/// Whether a statement answers the whole query on its own, or why not.
///
/// Two conditions, and the second was latent until the comparator was
/// rehearsed away:
///
/// * **it claims every obligation.** Anything unclaimed is work nobody does.
/// * **it can emit the answer.** Claiming every obligation is not the same
///   thing: the serialiser reads a *solution* — columns with the term
///   descriptor that says how each becomes an RDF term — and only a grouping
///   node carries those, because only a query with a solution layer has them.
///   A fetch's rows are records, which oxigraph turns into triples and answers
///   from.
///
/// A plain `SELECT ?s ?nm WHERE { … FILTER(…) }` claims its type, its triple
/// and its filter, so the first condition alone admitted it — and the artifact
/// then said SQL answers a query whose columns no renderer could name. Path A
/// took every non-blocked query first, so nothing reached the second
/// condition to find out it was missing. The deletion removes path A, which is
/// exactly why [`plan_query_refined_alone`] exists before it.
fn answers_alone(plan: &ExecutionPlan, refined: &crate::sparql_ops::OpTree) -> Result<(), String> {
    let claimed: BTreeSet<ObligationId> = refined.claims().into_iter().collect();
    let unclaimed: Vec<String> = (0..plan.obligations.len())
        .filter(|id| !claimed.contains(id))
        .map(|id| plan.obligations[id].to_string())
        .collect();
    if !unclaimed.is_empty() {
        return Err(format!(
            "the refined plan does not answer it alone either: {}",
            unclaimed.join("; ")
        ));
    }
    if !refined
        .nodes
        .iter()
        .any(|node| matches!(node.op, crate::sparql_ops::Op::Group { .. }))
    {
        return Err(
            "the refined statement fetches rows rather than emitting solutions, so it \
             cannot answer alone"
                .to_owned(),
        );
    }
    Ok(())
}

fn admit_alone(
    mut plan: ExecutionPlan,
    refined: crate::sparql_ops::OpTree,
    // Why nobody is comparing: today's refusal, or the fact that the
    // comparator is gone. The admission question is the same either way --
    // does this plan answer the whole query in SQL -- and only the sentence in
    // the artifact differs.
    because: &str,
) -> ExecutionPlan {
    if let Err(why) = answers_alone(&plan, &refined) {
        plan.refinement = Refinement::Fallback(format!("{because}, and {why}"));
        return plan;
    }
    let claimed: BTreeSet<ObligationId> = refined.claims().into_iter().collect();

    // One pass, and the refusal goes with it: an artifact that says an
    // aggregate is blocked while serving it from SQL would mislead every
    // reader of it, including the routing code that asks whether this plan
    // answers alone.
    plan.passes = vec![Pass {
        id: 0,
        inputs: Vec::new(),
        discharges: claimed.iter().copied().collect(),
        emits: emitted_from(&refined),
        kind: PassKind::Sql(Box::new(SqlPass { ops: refined })),
    }];
    plan.residual = Vec::new();
    plan.refinement = Refinement::UsedAlone(format!(
        "{because}, and the refined plan answers it in SQL alone"
    ));
    plan.blocked = None;
    plan
}

/// The variables a lowered statement binds, from its projection.
fn emitted_from(ops: &crate::sparql_ops::OpTree) -> Vec<String> {
    ops.nodes
        .iter()
        .find_map(|node| match &node.op {
            crate::sparql_ops::Op::Project { vars, .. } => {
                Some(vars.iter().map(|var| format!("?{var}")).collect())
            }
            _ => None,
        })
        .unwrap_or_default()
}

/// The refined plan's operators, or why there are none.
fn refined_ops(
    query_str: &str,
    schema_view: &SchemaView,
    fetch_bound: Option<usize>,
) -> Result<crate::sparql_ops::OpTree, String> {
    let mut plan = crate::sparql_refine::naive_plan_of(query_str)
        .map_err(|error| format!("no naive plan: {error}"))?;
    let rules = crate::sparql_rules::tier_one_rules(schema_view);
    let borrowed: Vec<&dyn crate::sparql_rules::Rule> =
        rules.iter().map(|rule| rule.as_ref()).collect();
    crate::sparql_rules::refine(&mut plan, &borrowed)
        .map_err(|failure| format!("a rule broke the plan: {failure}"))?;
    crate::sparql_ops::lower_refined(&plan, schema_view, fetch_bound)
        .map_err(|refusal| format!("not lowerable: {refusal}"))
}

/// The variables the plan's last pass binds.
fn emitted_variables(
    verdict: &crate::sparql_pushdown::Pushdown,
    _scoped: &crate::sparql_scoper::QueryPlan,
) -> Vec<String> {
    match verdict {
        crate::sparql_pushdown::Pushdown::Eligible { solution, .. } => solution
            .projected
            .iter()
            .map(|var| format!("?{var}"))
            .collect(),
        // Without a solution the projection is the engine's business, and
        // guessing at it here would put a second answer in the plan.
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparql_scoper::{parse_query, tests::test_schema_view};

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

    /// The obligations are what the query asks for, one entry each, in a stable
    /// order -- a plan string is only reviewable if the same query prints the
    /// A refusal travels on the artifact, so one call gives the caller both
    /// the route to take *and* what to tell whoever wrote the query. Before
    /// this it took a second call that re-parsed to recover the code and the
    /// hint.
    #[test]
    fn a_refused_aggregate_carries_its_reason() {
        let sv = test_schema_view();
        let plan = plan_query(
            &format!(
                "{PREFIX}SELECT (GROUP_CONCAT(?name) AS ?names) \
                 WHERE {{ ?s a asset360:Signal ; asset360:name ?name }}"
            ),
            &sv,
        )
        .expect("a refused aggregate still plans — the engine answers it");

        let refusal = plan
            .blocked
            .as_ref()
            .expect("GROUP_CONCAT is outside the subset, so a reason is owed");
        assert_eq!(refusal.code.as_str(), "unsupported_aggregate");
        assert!(!refusal.instead().is_empty(), "a refusal owes a rewrite");

        // Still a usable plan: the engine pass answers, so the endpoint is not
        // obliged to refuse the request.
        assert!(plan.is_accounted(), "{plan}");
        assert!(!plan.sql_only(), "{plan}");

        let printed = plan.to_string();
        assert!(printed.contains("not pushable"), "{printed}");
        assert!(printed.contains("unsupported_aggregate"), "{printed}");
        assert!(printed.contains("instead"), "{printed}");
    }

    /// Syntax must not decide the accounting: `FILTER(a) FILTER(b)` and
    /// `FILTER(a && b)` are the same query -- spargebra turns the first into
    /// the second -- so they raise the same obligations, one per conjunct.
    ///
    /// The granularity is what lets a pass push the comparison and leave the
    /// regex above it, each claimed by whoever applies it. With one obligation
    /// for the conjunction, pushing half of it would mean splitting a claim,
    /// and "discharged exactly once" has no room for half.
    #[test]
    fn a_conjunction_is_one_obligation_per_conjunct() {
        let separate = obligations_of(
            &parse_query(&format!(
                "{PREFIX}SELECT ?nm WHERE {{ ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(?nm > \"A\") FILTER(REGEX(?nm, \"^A\")) }}"
            ))
            .unwrap(),
        )
        .unwrap();
        let conjoined = obligations_of(
            &parse_query(&format!(
                "{PREFIX}SELECT ?nm WHERE {{ ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(?nm > \"A\" && REGEX(?nm, \"^A\")) }}"
            ))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(separate, conjoined, "one query, one ledger");
        assert_eq!(
            separate
                .iter()
                .filter(|obligation| matches!(obligation, Obligation::Filter { .. }))
                .count(),
            2,
            "{separate:#?}"
        );

        // A three-way conjunction flattens, however the parser nested it.
        let nested = obligations_of(
            &parse_query(&format!(
                "{PREFIX}SELECT ?nm WHERE {{ ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER((?nm > \"A\" && ?nm < \"B\") && REGEX(?nm, \"^A\")) }}"
            ))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            nested
                .iter()
                .filter(|obligation| matches!(obligation, Obligation::Filter { .. }))
                .count(),
            3,
            "{nested:#?}"
        );

        // A disjunction stays whole: neither half of `a || b` constrains
        // anything on its own, so there is nothing a pass could discharge
        // separately.
        let disjunction = obligations_of(
            &parse_query(&format!(
                "{PREFIX}SELECT ?nm WHERE {{ ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(?nm > \"A\" || ?nm < \"B\") }}"
            ))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            disjunction
                .iter()
                .filter(|obligation| matches!(obligation, Obligation::Filter { .. }))
                .count(),
            1,
            "{disjunction:#?}"
        );
    }

    /// The two constraints a plan could lose while its ledger still balanced:
    /// the condition spargebra lifts out of an `OPTIONAL`, and a `VALUES`
    /// block. Both are enumerated now, so losing either costs an unclaimed
    /// obligation instead of nothing.
    #[test]
    fn an_optional_condition_and_a_values_block_are_accounted() {
        let sv = test_schema_view();

        let plan = plan_query(
            &format!(
                "{PREFIX}SELECT ?s ?nm WHERE {{ ?s a asset360:Signal . \
                 OPTIONAL {{ ?s asset360:name ?nm . FILTER(?nm > \"A\") }} }}"
            ),
            &sv,
        )
        .unwrap();
        plan.ledger_balances().unwrap();
        let lifted = plan
            .obligations
            .iter()
            .position(|obligation| matches!(obligation, Obligation::Filter { .. }))
            .expect("the lifted condition is an obligation");
        // Nobody pushes it -- it decides whether the optional side matched --
        // so it must sit with the engine, said rather than assumed.
        let engine = plan
            .passes
            .iter()
            .find(|pass| matches!(pass.kind, PassKind::Engine(_)))
            .expect("the engine finishes this");
        assert!(engine.discharges.contains(&lifted), "{plan}");

        // A VALUES block the scoper cannot represent: its own obligation,
        // claimed by the engine.
        let plan = plan_query(
            &format!(
                "{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:kind ?k . \
                 VALUES ?k {{ \"KSS\" }} }}"
            ),
            &sv,
        )
        .unwrap();
        plan.ledger_balances().unwrap();
        let values = plan
            .obligations
            .iter()
            .position(|obligation| matches!(obligation, Obligation::Values { .. }))
            .expect("a VALUES block is an obligation");
        assert!(!plan.sql_only(), "{plan}");
        let engine = plan
            .passes
            .iter()
            .find(|pass| matches!(pass.kind, PassKind::Engine(_)))
            .expect("the engine finishes this");
        assert!(engine.discharges.contains(&values), "{plan}");

        // And one the scoper *does* represent: SQL claims it, and the claim is
        // honest because the pass renders it as the IN it hoisted.
        let plan = plan_query(
            &format!(
                "{PREFIX}SELECT ?nm (COUNT(*) AS ?n) WHERE {{ ?s a asset360:Signal ; \
                 asset360:name ?nm . VALUES ?nm {{ \"BX1\" \"BX2\" }} }} GROUP BY ?nm"
            ),
            &sv,
        )
        .unwrap();
        plan.ledger_balances().unwrap();
        assert!(plan.sql_only(), "{plan}");
        let printed = plan.to_string();
        assert!(printed.contains("values    VALUES ?nm"), "{printed}");
        assert!(printed.contains("IN ('BX1', 'BX2')"), "{printed}");
    }

    /// The gate, in the direction that matters: the refined operators are used
    /// only when they claim at least as much as today's SQL pass.
    #[test]
    fn a_refined_plan_is_used_when_it_claims_as_much() {
        let sv = test_schema_view();
        for query in [
            "SELECT ?s WHERE { ?s a asset360:Signal }",
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             FILTER(?nm > \"A\") }",
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name \"BX517\" }",
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t a asset360:Track ; asset360:hasName ?tn }",
            "SELECT ?lon WHERE { ?s a asset360:Signal ; asset360:location ?loc . \
             ?loc asset360:longitude ?lon . FILTER(?lon > 3) }",
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             VALUES ?nm { \"a\" \"b\" } }",
            // The first collapsing shape, and the first plan whose answer is
            // SQL's rather than a narrowing of what the engine will decide.
            "SELECT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm } GROUP BY ?nm",
            // An `OPTIONAL` over a second star, which used to be two islands
            // and is now one statement with a `LEFT JOIN` in it.
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             OPTIONAL { ?t a asset360:Track ; asset360:hasName ?tn } }",
        ] {
            let plan = plan_query_refined(&format!("{PREFIX}{query}"), &sv).expect("should plan");
            assert!(
                matches!(plan.refinement, Refinement::Used(_)),
                "{query}: {:?}",
                plan.refinement.reason()
            );
            plan.ledger_balances()
                .unwrap_or_else(|error| panic!("{error} for {query}\n{plan}"));
            assert!(plan.is_accounted(), "{query}\n{plan}");

            // What the gate promises, stated in the quantity it compares:
            // the lowered statement reads no more rows and does no less work.
            let today = plan_query(&format!("{PREFIX}{query}"), &sv).expect("should plan");
            let ops = |plan: &ExecutionPlan| -> crate::sparql_ops::OpTree {
                plan.passes
                    .iter()
                    .find_map(|pass| match &pass.kind {
                        PassKind::Sql(sql) => Some(sql.ops.clone()),
                        PassKind::Engine(_) => None,
                    })
                    .expect("every plan has an SQL pass")
            };
            ops(&plan)
                .is_no_worse_than(&ops(&today))
                .unwrap_or_else(|reason| panic!("{query}: {reason}"));
        }
    }

    /// **Path B.** A query the single-pass planner refuses outright, answered
    /// in SQL alone.
    ///
    /// `?k` is read and never grouped or aggregated, so today's planner
    /// refuses: a multivalued read with no binding "has no container and no
    /// instruction", and counting one row per record would answer a different
    /// question. The refined plan has the instruction -- an `Unnest` below the
    /// grouping -- so it counts one row per value, which is one per solution.
    ///
    /// The first query this architecture serves that the endpoint could not
    /// answer before.
    #[test]
    fn a_query_todays_planner_refuses_can_be_answered_alone() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?nm (COUNT(*) AS ?n) WHERE {{ ?s a asset360:Signal ; \
             asset360:name ?nm ; asset360:trafficKinds ?k }} GROUP BY ?nm"
        );

        // Today: refused, with a reason.
        let today = plan_query(&query, &sv).expect("should plan");
        assert!(today.blocked.is_some(), "{today}");
        assert!(!today.sql_only(), "today fetches and lets the engine group");

        let plan = plan_query_refined(&query, &sv).expect("should plan");
        let note = match &plan.refinement {
            Refinement::UsedAlone(note) => note.clone(),
            other => panic!("expected admission on its own soundness, got {other:?}"),
        };
        assert!(note.contains("refuses this query"), "{note}");

        // What admission rests on, asserted rather than assumed.
        assert!(plan.sql_only(), "the statement is the query:\n{plan}");
        assert!(plan.is_accounted(), "{plan}");
        plan.ledger_balances().unwrap();
        assert!(
            plan.blocked.is_none(),
            "an artifact must not say an aggregate is blocked while serving it"
        );

        // And the fan-out reached the statement, which is the whole difference
        // between counting solutions and counting records.
        let ops = plan
            .passes
            .iter()
            .find_map(|pass| match &pass.kind {
                PassKind::Sql(sql) => Some(sql.ops.clone()),
                PassKind::Engine(_) => None,
            })
            .expect("one pass, and it is SQL");
        assert_eq!(ops.find("group").len(), 1, "{plan}");
        let crate::sparql_ops::Op::Group { bindings, keys, .. } =
            &ops.nodes[ops.find("group")[0]].op
        else {
            panic!("{plan}");
        };
        assert_eq!(keys, &vec![0], "the name is the only key");
        assert_eq!(
            bindings.len(),
            2,
            "the fan-out is a binding too: {bindings:?}"
        );
        assert!(
            bindings[1]
                .containers
                .iter()
                .any(|container| *container != crate::sparql_pushdown::Container::Single),
            "the second binding is the collection the renderer unnests: {bindings:?}"
        );
        println!("{plan}");
    }

    /// The collapsing surface, end to end: the rule pushes it, the lowering
    /// renders it, and the gate admits it.
    ///
    /// The rule-level test asserts the plan is all `Sql`; this asserts the
    /// three stages agree, which is the property that ships. They caught
    /// different failures -- a rename the rule folded into a measure left an
    /// `ORDER BY` naming spargebra's internal variable, which the plan called
    /// pushed and the lowering could not render.
    #[test]
    fn the_collapsing_surface_reaches_sql() {
        let sv = test_schema_view();
        for query in [
            // Measures, over a column and over its distinct values.
            "SELECT ?nm (COUNT(?len) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm ; asset360:length ?len } GROUP BY ?nm",
            "SELECT ?nm (COUNT(DISTINCT ?len) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm ; asset360:length ?len } GROUP BY ?nm",
            "SELECT ?nm (MIN(?len) AS ?lo) (MAX(?len) AS ?hi) WHERE { \
             ?s a asset360:Signal ; asset360:name ?nm ; asset360:length ?len } GROUP BY ?nm",
            "SELECT ?nm (SUM(?len) AS ?t) (AVG(?len) AS ?a) WHERE { \
             ?s a asset360:Signal ; asset360:name ?nm ; asset360:length ?len } GROUP BY ?nm",
            // Key arity: none, and several.
            "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal }",
            "SELECT ?nm ?k (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm ; asset360:kind ?k } GROUP BY ?nm ?k",
            // The modifiers above.
            "SELECT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm } GROUP BY ?nm ORDER BY DESC(?n) LIMIT 3",
            "SELECT DISTINCT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm } GROUP BY ?nm",
            "SELECT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm } GROUP BY ?nm ORDER BY DESC(COUNT(*))",
            // A key that is one element of an array, and a key that is a
            // record's own identity.
            "SELECT ?k (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:trafficKinds ?k } GROUP BY ?k",
            "SELECT ?t (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:locatedOnTrack ?t . ?t a asset360:Track } GROUP BY ?t",
            // And a condition on the grouped rows.
            "SELECT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm } GROUP BY ?nm HAVING (COUNT(*) > 1)",
        ] {
            let plan = plan_query_refined(&format!("{PREFIX}{query}"), &sv).expect("should plan");
            assert!(
                matches!(
                    plan.refinement,
                    Refinement::Used(_) | Refinement::UsedAlone(_)
                ),
                "{query}: {:?}",
                plan.refinement
            );
            assert!(plan.blocked.is_none(), "{query}: {:?}", plan.blocked);
            // Answered in SQL: no operator was handed back to an engine that
            // cannot recompute an aggregate.
            assert!(
                plan.sql_only(),
                "{query} is a collapse, so it is whole or it is nothing"
            );
        }
    }

    /// Every spelling of an identity restriction still declines to answer an
    /// aggregate alone, including the several-identifier one.
    ///
    /// The standing constraint as the identity surface widened, and the reason
    /// it is a constraint rather than a preference: the writer emits no triple
    /// for an identifier, so the engine's answer to such a query is empty --
    /// and an aggregate is the one case where the engine's answer is the
    /// answer. Falling back leaves these queries with today's planner, which
    /// serves them, so the refusal costs nothing and prevents an empty result.
    #[test]
    fn no_spelling_of_an_identity_answers_an_aggregate_alone() {
        let sv = test_schema_view();
        for (query, spelling) in [
            (
                "SELECT (COUNT(*) AS ?n) WHERE { \
                 <https://data.infrabel.be/asset360/sig-1> a asset360:Signal ; \
                 asset360:name ?nm }",
                "a constant subject",
            ),
            (
                "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
                 asset360:name ?nm . VALUES ?s { \
                 <https://data.infrabel.be/asset360/sig-1> \
                 <https://data.infrabel.be/asset360/sig-2> } }",
                "two identifiers, which render as an IN",
            ),
            (
                "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
                 asset360:name ?nm . \
                 FILTER(?s = <https://data.infrabel.be/asset360/sig-1>) }",
                "an equality filter",
            ),
        ] {
            let plan = plan_query_refined(&format!("{PREFIX}{query}"), &sv).expect("should plan");
            let reason = plan.refinement.reason().unwrap_or_else(|| {
                panic!("{spelling} answered an aggregate: {:?}", plan.refinement)
            });
            assert!(
                reason.contains("identifier restriction"),
                "{spelling}: {reason}"
            );
        }
    }

    /// The rehearsal: admission with the comparator removed asks one question,
    /// and answers it the same way for a query today's planner serves as for
    /// one it refuses.
    ///
    /// Three outcomes and no fourth. A statement that claims the whole query
    /// answers alone; one that claims part of it is a fetch the engine
    /// finishes; a plan that does not lower at all leaves today's statement in
    /// place, which is the one thing the deletion will have to replace.
    #[test]
    fn admission_without_the_comparator_has_three_outcomes() {
        let sv = test_schema_view();

        // Answers alone: everything the query asks for is in the statement.
        let plan = plan_query_refined_alone(
            &format!(
                "{PREFIX}SELECT ?nm (COUNT(*) AS ?n) WHERE {{ ?s a asset360:Signal ; \
                 asset360:name ?nm }} GROUP BY ?nm"
            ),
            &sv,
        )
        .expect("should plan");
        assert!(
            matches!(plan.refinement, Refinement::UsedAlone(_)),
            "{:?}",
            plan.refinement
        );
        assert!(plan.sql_only(), "{plan}");

        // A fetch: the statement narrows and the engine answers. Today's gate
        // calls this `used` too, so the deletion changes nothing here -- which
        // is the reassuring half of the rehearsal.
        let plan = plan_query_refined_alone(
            &format!(
                "{PREFIX}SELECT ?s ?nm WHERE {{ ?s a asset360:Signal ; \
                 asset360:name ?nm . FILTER(?nm > \"A\") }}"
            ),
            &sv,
        )
        .expect("should plan");
        assert!(
            matches!(plan.refinement, Refinement::Used(_)),
            "{:?}",
            plan.refinement
        );
        assert!(!plan.sql_only(), "the engine still answers it:\n{plan}");

        // And a plan with an engine node in it is *not* admitted alone, which
        // is the failure this rehearsal exists to catch: with no comparator,
        // nothing else stands between such a plan and an answer.
        let plan = plan_query_refined_alone(
            &format!(
                "{PREFIX}SELECT ?nm (COUNT(*) AS ?n) WHERE {{ ?s a asset360:Signal ; \
                 asset360:name ?nm . FILTER(REGEX(?nm, \"^A\")) }} GROUP BY ?nm"
            ),
            &sv,
        )
        .expect("should plan");
        assert!(
            !plan.sql_only(),
            "a regex the statement cannot apply must not be answered from \
             it:\n{plan}"
        );
    }

    /// Path B is not a second chance. A query today's planner *can* answer
    /// goes through the comparator and nowhere else, so a refined plan the
    /// comparator calls worse falls back rather than being re-admitted on its
    /// own soundness.
    ///
    /// Structural rather than conventional: the two paths are chosen by
    /// whether the artifact carries a refusal, so there is no ordering to get
    /// wrong.
    #[test]
    fn a_plan_the_comparator_refuses_is_not_admitted_by_the_other_path() {
        let sv = test_schema_view();
        // `COUNT(DISTINCT *)`: today answers it, and the grouping rule
        // declines it, so the refined statement would hand the grouping back.
        // (Today renders it as `count(*)`, which counts solutions rather than
        // distinct ones -- the difference this rule refuses to reproduce.)
        let query = format!(
            "{PREFIX}SELECT (COUNT(DISTINCT *) AS ?n) WHERE {{ ?s a asset360:Signal ; \
             asset360:name ?nm }}"
        );
        let today = plan_query(&query, &sv).expect("should plan");
        assert!(today.blocked.is_none(), "today answers this one");

        let plan = plan_query_refined(&query, &sv).expect("should plan");
        assert!(
            matches!(plan.refinement, Refinement::Fallback(_)),
            "a substitution the comparator refuses must not become a capability \
             claim: {:?}",
            plan.refinement
        );
        assert!(
            plan.refinement.reason().unwrap().contains("leaves 'group"),
            "{:?}",
            plan.refinement
        );
    }

    /// And path B refuses a plan that does not answer alone, which is most of
    /// them: a refused aggregate whose refined plan is still a fetch stays a
    /// fetch, with the reason naming both halves.
    #[test]
    fn path_b_refuses_a_plan_that_does_not_answer_alone() {
        let sv = test_schema_view();
        // `GROUP_CONCAT` is outside the pushable set, so today refuses -- and
        // no rule pushes that grouping either.
        let query = format!(
            "{PREFIX}SELECT (GROUP_CONCAT(?nm) AS ?names) WHERE {{ ?s a asset360:Signal ; \
             asset360:name ?nm }}"
        );
        let plan = plan_query_refined(&query, &sv).expect("should plan");
        let reason = match &plan.refinement {
            Refinement::Fallback(reason) => reason.clone(),
            other => panic!("expected a fallback, got {other:?}"),
        };
        assert!(reason.contains("refuses this query"), "{reason}");
        assert!(reason.contains("does not answer it alone"), "{reason}");
        // The refusal stays on the artifact, because it is still true.
        assert!(plan.blocked.is_some(), "{plan}");
    }

    /// A pushed grouping answers alone: SQL claims every obligation, there is
    /// no engine pass, and the plan says so.
    ///
    /// That last part is what the endpoint reads to take the aggregate route
    /// at all — a plan with an engine pass is a fetch, however much SQL
    /// claims. The first refined plan for which it is true.
    #[test]
    fn a_pushed_grouping_answers_without_the_engine() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?nm (COUNT(*) AS ?n) WHERE {{ ?s a asset360:Signal ; \
             asset360:name ?nm }} GROUP BY ?nm"
        );
        let plan = plan_query_refined(&query, &sv).expect("should plan");

        assert!(
            matches!(plan.refinement, Refinement::Used(None)),
            "refined, and with no ledger difference to report: {:?}",
            plan.refinement
        );
        assert!(plan.sql_only(), "no engine pass:\n{plan}");
        assert!(plan.is_accounted(), "{plan}");
        plan.ledger_balances().unwrap();

        // Every obligation, claimed in SQL -- including the grouping and the
        // aggregate, which no rule could claim before this one.
        let sql_claims: Vec<String> = plan
            .passes
            .iter()
            .filter(|pass| matches!(pass.kind, PassKind::Sql(_)))
            .flat_map(|pass| pass.discharges.iter())
            .map(|id| plan.obligations[*id].to_string())
            .collect();
        assert_eq!(sql_claims.len(), plan.obligations.len(), "{plan}");
        assert!(
            sql_claims.iter().any(|claim| claim.contains("group")),
            "{sql_claims:?}"
        );
        assert!(
            sql_claims.iter().any(|claim| claim.contains("aggregate")),
            "{sql_claims:?}"
        );

        // And the operators are a grouping, which is what the endpoint reads
        // to decide the route.
        let ops = plan
            .passes
            .iter()
            .find_map(|pass| match &pass.kind {
                PassKind::Sql(sql) => Some(sql.ops.clone()),
                PassKind::Engine(_) => None,
            })
            .expect("every plan has an SQL pass");
        assert_eq!(ops.find("group").len(), 1, "{plan}");
        println!("{plan}");
    }

    /// The case the gate was corrected for, and the gap it was corrected
    /// around is now closed at the source.
    ///
    /// The gate used to compare claim ledgers, and rejected this shape because
    /// the refined plan declined to let a narrowing scan claim optionality it
    /// did not render. Comparing the row source instead admitted it *with* a
    /// reported difference. Now there is no difference to report: the optional
    /// read is absorbed into the scan as a bound nullable column, so the scan
    /// renders the optional semantics and the claim is its own.
    ///
    /// Three rounds, three positions, and only the last one is stable: the
    /// claim follows whoever answers, and the way to make a claim honest is to
    /// make the node answer rather than to argue about the ledger.
    #[test]
    fn an_optional_read_no_longer_costs_a_ledger_difference() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?s ?nm WHERE {{ ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL {{ ?s asset360:name ?nm }} }}"
        );
        let plan = plan_query_refined(&query, &sv).expect("should plan");

        assert_eq!(
            plan.refinement,
            Refinement::Used(None),
            "the ledgers agree now: {:?}",
            plan.refinement
        );
        plan.ledger_balances().unwrap();
        assert!(plan.is_accounted(), "{plan}");

        // The optional triple is SQL's, because the scan answers it.
        let sql_claims: Vec<String> = plan
            .passes
            .iter()
            .filter(|pass| matches!(pass.kind, PassKind::Sql(_)))
            .flat_map(|pass| pass.discharges.iter())
            .map(|id| plan.obligations[*id].to_string())
            .collect();
        assert!(
            sql_claims
                .iter()
                .any(|claim| claim.contains("asset360:name")),
            "{sql_claims:?}"
        );

        // And the statement is still today's, which is what the comparator
        // checked before there was anything else to check.
        let today = plan_query(&query, &sv).expect("should plan");
        let ops = |plan: &ExecutionPlan| -> crate::sparql_ops::OpTree {
            plan.passes
                .iter()
                .find_map(|pass| match &pass.kind {
                    PassKind::Sql(sql) => Some(sql.ops.clone()),
                    PassKind::Engine(_) => None,
                })
                .expect("every plan has an SQL pass")
        };
        ops(&plan)
            .is_no_worse_than(&ops(&today))
            .unwrap_or_else(|reason| panic!("{reason}"));
    }

    /// And in the other direction: a shape tier one cannot serve keeps today's
    /// plan, with a reason the caller can log.
    #[test]
    fn a_plan_the_rules_cannot_serve_falls_back() {
        let sv = test_schema_view();
        for (query, expected) in [
            (
                // An aggregate the grouping rule declines: `COUNT(DISTINCT *)`
                // counts distinct solutions, which `count(*)` does not, so the
                // refined statement would hand the grouping back -- the
                // regression the gate exists to refuse. Today answers it, by
                // rendering the distinct away.
                "SELECT (COUNT(DISTINCT *) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm }",
                "leaves 'group",
            ),
            (
                // An aggregate over a record the query named. Today answers it
                // from the statement; the rules refuse to, because the writer
                // emits no triple for an identifier and the engine's answer to
                // such a query is empty -- so a statement that answered alone
                // would be inventing one. Deliberately left as a fallback: the
                // spelling is the user's decision to make, not the planner's.
                "SELECT (COUNT(*) AS ?n) WHERE { \
             <https://data.infrabel.be/asset360/sig-1> a asset360:Signal ; \
             asset360:name ?nm }",
                "identifier restriction",
            ),
        ] {
            let plan = plan_query_refined(&format!("{PREFIX}{query}"), &sv).expect("should plan");
            let reason = plan
                .refinement
                .reason()
                .unwrap_or_else(|| panic!("{query} was refined: {:?}", plan.refinement));
            assert!(reason.contains(expected), "{query}: {reason}");

            // Falling back is today's plan, unchanged: same claims, same
            // operators, same answer.
            let today = plan_query(&format!("{PREFIX}{query}"), &sv).expect("should plan");
            assert_eq!(plan.to_string(), today.to_string(), "{query}");
        }
    }

    /// The substitution moves the division of labour, not the question: what
    /// SQL now claims the engine no longer does, every obligation is still
    /// claimed exactly once, and nothing is left unaccounted for.
    #[test]
    fn refining_moves_claims_between_passes_and_balances() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?nm WHERE {{ ?s a asset360:Signal ; asset360:name ?nm . \
             FILTER(?nm > \"A\") FILTER(REGEX(?nm, \"^A\")) }}"
        );
        let today = plan_query(&query, &sv).expect("should plan");
        let refined = plan_query_refined(&query, &sv).expect("should plan");
        assert!(
            matches!(refined.refinement, Refinement::Used(_)),
            "{:?}",
            refined.refinement
        );
        assert_eq!(refined.obligations, today.obligations, "same question");

        let sql_claims = |plan: &ExecutionPlan| -> Vec<String> {
            plan.passes
                .iter()
                .filter(|pass| matches!(pass.kind, PassKind::Sql(_)))
                .flat_map(|pass| pass.discharges.iter())
                .map(|id| plan.obligations[*id].to_string())
                .collect()
        };
        // The comparison filter is SQL's now, and it was the engine's before.
        assert!(
            sql_claims(&refined)
                .iter()
                .any(|claim| claim.contains("(?nm > \"A\")")),
            "{refined}"
        );
        assert!(
            !sql_claims(&today)
                .iter()
                .any(|claim| claim.contains("(?nm > \"A\")")),
            "{today}"
        );
        // ...and the regex is still the engine's, in both.
        assert!(
            !sql_claims(&refined)
                .iter()
                .any(|claim| claim.contains("REGEX")),
            "{refined}"
        );
        refined.ledger_balances().unwrap();
        assert!(refined.is_accounted(), "{refined}");
    }

    /// The fetch bound survives the substitution.
    ///
    /// It claims nothing, so no ledger check would miss it and no answer would
    /// be wrong -- the engine still applies the query's own `LIMIT`. What
    /// happens without it is that `LIMIT 1` fetches every row of the class,
    /// which is the regression `test_single_star_limit_1_returns_exactly_one`
    /// caught the last time a planner mislaid it.
    #[test]
    fn the_fetch_bound_survives_refinement() {
        let sv = test_schema_view();
        let query = format!("{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal }} LIMIT 1");
        let bound_of = |plan: &ExecutionPlan| -> Option<usize> {
            plan.passes
                .iter()
                .find_map(|pass| match &pass.kind {
                    PassKind::Sql(sql) => Some(&sql.ops),
                    PassKind::Engine(_) => None,
                })
                .and_then(crate::sparql_ops::fetch_bound_of)
        };

        let today = plan_query(&query, &sv).expect("should plan");
        assert_eq!(bound_of(&today), Some(1), "{today}");

        let refined = plan_query_refined(&query, &sv).expect("should plan");
        assert!(
            matches!(refined.refinement, Refinement::Used(_)),
            "{refined}"
        );
        assert_eq!(bound_of(&refined), Some(1), "{refined}");

        // And it still claims nothing, which is what makes reading it safe.
        let sql_claims: Vec<&Pass> = refined
            .passes
            .iter()
            .filter(|pass| matches!(pass.kind, PassKind::Sql(_)))
            .collect();
        assert!(
            !sql_claims.iter().any(|pass| pass
                .discharges
                .iter()
                .any(|id| matches!(refined.obligations[*id], Obligation::Slice { .. }))),
            "the query's own LIMIT is the engine's: {refined}"
        );
    }

    /// A query that never asked for an aggregate is owed no explanation.
    #[test]
    fn an_ordinary_query_carries_no_refusal() {
        let sv = test_schema_view();
        let plan = plan_query(
            &format!("{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal }}"),
            &sv,
        )
        .unwrap();

        assert!(plan.blocked.is_none(), "{plan}");
        assert!(!plan.to_string().contains("not pushable"));
    }

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

    /// A question the SQL leaf can answer whole: one pass, nothing left over.
    #[test]
    fn a_pushable_question_plans_as_one_sql_pass() {
        let sv = test_schema_view();
        let plan = plan_query(
            &format!(
                "{PREFIX}SELECT ?kind (COUNT(*) AS ?n) WHERE {{ \
                 ?s a asset360:Signal ; asset360:kind ?kind }} GROUP BY ?kind"
            ),
            &sv,
        )
        .unwrap();

        plan.ledger_balances().unwrap();
        assert_eq!(plan.passes.len(), 1, "{plan}");
        assert!(matches!(plan.passes[0].kind, PassKind::Sql(_)), "{plan}");
        assert_eq!(
            plan.passes[0].discharges.len(),
            plan.obligations.len(),
            "one pass answering the question claims all of it:\n{plan}"
        );
        assert!(plan.sql_only(), "{plan}");
    }

    /// A question it cannot: the scan still narrows, and what is left is the
    /// engine's -- named, in the plan, rather than inferred by the caller.
    #[test]
    fn an_unpushable_filter_plans_as_a_scan_plus_an_engine_pass() {
        let sv = test_schema_view();
        let plan = plan_query(
            &format!(
                "{PREFIX}SELECT ?nm (COUNT(*) AS ?n) WHERE {{ \
                 ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(REGEX(?nm, \"^BX\")) }} GROUP BY ?nm"
            ),
            &sv,
        )
        .unwrap();

        plan.ledger_balances().unwrap();
        assert_eq!(plan.passes.len(), 2, "{plan}");
        assert!(matches!(plan.passes[0].kind, PassKind::Sql(_)), "{plan}");
        let PassKind::Engine(engine) = &plan.passes[1].kind else {
            panic!("second pass must be the engine:\n{plan}");
        };
        assert!(
            !engine.causes.is_empty(),
            "the engine pass says why it exists:\n{plan}"
        );
        assert!(!plan.sql_only(), "{plan}");

        // The scan still claims the triples it represented -- the point of a
        // residual rather than a refusal.
        assert!(
            !plan.passes[0].discharges.is_empty(),
            "the scan narrows even when the engine finishes:\n{plan}"
        );
        // ...and the aggregate is not among them: SQL did not group here.
        let sql_claims = &plan.passes[0].discharges;
        assert!(
            !sql_claims.iter().any(|id| matches!(
                plan.obligations[*id],
                Obligation::Aggregate { .. } | Obligation::Group { .. }
            )),
            "a scan-only pass must not claim the grouping:\n{plan}"
        );

        println!("{plan}");
    }

    /// Every plan balances, over the whole corpus of shapes this planner
    /// accepts. The invariant is only worth stating if it is checked on real
    /// plans and not just on hand-built ones.
    #[test]
    fn every_planned_query_balances_its_ledger() {
        let sv = test_schema_view();
        for query in [
            "SELECT ?s WHERE { ?s a asset360:Signal }",
            "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal }",
            "SELECT ?kind (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:kind ?kind } GROUP BY ?kind ORDER BY DESC(?n) LIMIT 3",
            "SELECT (SUM(?len) AS ?total) WHERE { ?s a asset360:Signal ; \
             asset360:length ?len . FILTER(?len >= 10) }",
            "SELECT ?lo WHERE { ?s a asset360:Signal ; asset360:location ?c . \
             ?c asset360:longitude ?lo }",
            "SELECT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm . FILTER(?nm != \"BX\") } GROUP BY ?nm",
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t a asset360:Track ; asset360:hasName ?tn }",
        ] {
            let plan = plan_query(&format!("{PREFIX}{query}"), &sv).unwrap();
            plan.ledger_balances()
                .unwrap_or_else(|err| panic!("{err} for {query}\n{plan}"));
        }
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
            blocked: None,
            refinement: Refinement::NotAttempted,
        };
        assert!(balanced.ledger_balances().is_ok());
        assert!(
            !balanced.is_accounted(),
            "an obligation with no pass is not accounted for"
        );

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
                    ops: crate::sparql_ops::lower_sql_pass(
                        &scoped,
                        None,
                        &all_ids(&obligations),
                        true,
                    ),
                })),
            }],
            residual: Vec::new(),
            blocked: None,
            refinement: Refinement::NotAttempted,
        };
        plan.ledger_balances().unwrap();

        let printed = plan.to_string();
        for id in 0..obligations.len() {
            assert!(
                printed.contains(&format!("o{id}")),
                "obligation o{id} is missing from:\n{printed}"
            );
        }
        assert!(printed.contains("all in SQL"), "{printed}");
        assert!(printed.contains("pass 0  SQL"), "{printed}");
        assert!(printed.contains("scan"), "{printed}");
        assert!(printed.contains("residual  (empty)"), "{printed}");

        // Printed here so a failing run shows the format a human is meant to
        // read, not just an assertion.
        println!("{printed}");
    }
}
