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
    /// Obligations no pass discharges. Empty means the passes together answer
    /// exactly the question asked.
    pub residual: Vec<ObligationId>,
    /// How this plan is to be run, and what the engine was left. See
    /// [`plan_query_refined`].
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

    /// The aggregate the statement did not take, when the query asked for one.
    ///
    /// **What replaced the refusal, and why it is a different thing.** The
    /// deleted planner decided eligibility for the grouped question as a whole
    /// and said `Blocked(code, detail, instead)` when it could not serve it —
    /// a closed vocabulary of codes, on the artifact, whether or not the
    /// engine could answer. There is no such decision now: a rule either takes
    /// the grouping or it does not, and what is left shows up in the ledger
    /// like anything else. So this reads the ledger: a `GROUP BY` or an
    /// aggregate an *engine* pass discharges is an aggregate SQL did not push.
    ///
    /// The caller needs it for one judgement, unchanged by the deletion: an
    /// aggregate the engine must answer means materialising the whole class,
    /// so over a class too large to hold it is refused up front rather than
    /// after thirty seconds. What is gone with the codes is the machine-
    /// readable `code` and the `instead` rewrite hint; inventing either from
    /// the obligation would be guessing.
    pub fn unpushed_aggregate(&self) -> Option<String> {
        self.passes
            .iter()
            .filter(|pass| matches!(pass.kind, PassKind::Engine(_)))
            .flat_map(|pass| pass.discharges.iter())
            .chain(self.residual.iter())
            .filter_map(|id| self.obligations.get(*id))
            .find(|obligation| {
                matches!(
                    obligation,
                    Obligation::Group { .. } | Obligation::Aggregate { .. }
                )
            })
            .map(ToString::to_string)
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

        // An aggregate the statement did not take is not a failure of the
        // plan — the engine pass still answers it — but it is the reason SQL
        // could not, and the thing a caller weighs against the size of the
        // class it would have to materialise.
        if let Some(obligation) = self.unpushed_aggregate() {
            writeln!(f, "  not pushed")?;
            writeln!(f, "      {obligation}")?;
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

/// How the plan is to be run: SQL alone, SQL feeding the engine, or the
/// scoper's fetch feeding the engine.
///
/// Carried on the artifact rather than logged in Rust, because the caller has
/// the query text and the logger.
///
/// **What this used to be, and is not.** While there were two SQL planners
/// this said which one won and why the gate chose it, with a `NotAttempted`
/// for a plan that never went through the pipeline and a `Used` note carrying
/// the *ledger difference* between the two — an obligation SQL applied without
/// claiming, reported rather than vetoed. There is one planner now, so there
/// is no ledger to differ from: the note says what the statement left for the
/// engine, which is the question anyone actually asks when a query is slow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refinement {
    /// The statement narrows the rows and the engine finishes the query.
    ///
    /// The note says what stopped it answering alone — the obligations the
    /// statement does not claim, or the fact that a fetch has no solution to
    /// emit. Most queries are this shape, and it is not a defect: the engine
    /// re-runs the whole query over what SQL fetched, so the answer is the
    /// engine's either way.
    Used(Option<String>),
    /// The statement answers the whole query in SQL; no engine pass.
    ///
    /// Admitted on the plan's own soundness — every node in SQL, every
    /// obligation discharged, the residual empty, the invariants holding, and
    /// a solution it can emit. Every shape admitted this way carries an oracle
    /// test against the engine leg, because there is nothing else to check it
    /// against.
    UsedAlone(String),
    /// The pipeline produced no statement, so the scoper's fetch is used and
    /// the engine answers over it. No shape in the frozen inventory does this.
    Fallback(String),
}

impl Refinement {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Used(_) => "used",
            Self::UsedAlone(_) => "used_alone",
            Self::Fallback(_) => "fallback",
        }
    }

    /// Why the pipeline produced no statement, for a log line.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Fallback(reason) => Some(reason),
            _ => None,
        }
    }

    /// What the statement left for the engine, or -- for
    /// [`Refinement::UsedAlone`] -- that it left nothing.
    pub fn note(&self) -> Option<&str> {
        match self {
            Self::Used(note) => note.as_deref(),
            Self::UsedAlone(note) => Some(note),
            _ => None,
        }
    }
}

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

/// Plan a query: one parse, one scope, one refinement, one artifact.
///
/// **The only planner.** A naive plan of the whole query, refined by rules to
/// a fixpoint, lowered into the operators `sql_builder.py` renders. There used
/// to be a second one — a single-pass analysis that decided eligibility for
/// the grouped question as a whole — and a runtime gate that compared the two.
/// Both are gone: two planners is two things to maintain, and the comparison's
/// knowledge is kept as invariants on one plan rather than as a diff against
/// another. See `doc_book/src/design/28d-plan-refinement.md`.
///
/// **Admission asks one question**, the one path B always asked: does this
/// statement answer the whole query on its own terms — every node in SQL,
/// every obligation discharged, the residual empty, the invariants holding,
/// and a solution it can actually emit? If so, SQL answers. If not, the
/// statement is a *fetch* that narrows the rows and the engine finishes, which
/// is what has always happened whenever the pushdown route declined.
///
/// **The engine leg stays permanently.** It re-runs the whole query over the
/// materialised instances, which makes every partial push correct and makes it
/// the oracle every differential in this design rests on. Deleting the second
/// SQL planner is not the same thing as removing the fallback.
///
/// **The three inputs the deleted planner used to supply, and where each comes
/// from now:**
///
/// * **the obligation list** — [`obligations_of`], which takes the parsed
///   query and nothing else. It was already the single derivation: the naive
///   plan builder calls it too, so the refined plan's ledger and this list are
///   the same list rather than two lists that agree. Nothing moved, and that
///   is the point — a second derivation that agrees today is the failure mode
///   worth avoiding.
/// * **the fetch bound** — `QueryPlan::sql_limit`, straight from the scoper.
///   Whether a `LIMIT` may reach the fetch is an analysis with a subtlety in
///   it (a dropped filter makes it unsafe), so it stays where that analysis
///   lives. Reading it here rather than recovering it from another planner's
///   operators removes a dependency without adding a derivation.
/// * **the pass structure** — built below. Two shapes and no third: one SQL
///   pass when the statement answers the whole query, or an SQL fetch plus an
///   engine pass that finishes it.
///
/// The scoper is still called, and not only for the bound: it is what refuses
/// `UNION`, `MINUS` and an unscoped subject, which the endpoint turns into a
/// 422 rather than a wrong answer. Those refusals are the user's, not the
/// planner's, and they predate all of this.
pub fn plan_query_refined(
    query_str: &str,
    schema_view: &SchemaView,
) -> Result<ExecutionPlan, ScopeError> {
    let parsed = crate::sparql_scoper::parse_query(query_str)?;
    let obligations = obligations_of(&parsed)?;
    let scoped = crate::sparql_scoper::scope_parsed(&parsed, schema_view)?;

    let mut refined = match crate::sparql_refine::naive_plan(&parsed) {
        Ok(plan) => plan,
        // A naive plan the builder cannot make is a query this pipeline does
        // not represent. The scoper has already accepted it, so there are rows
        // to fetch: hand back the star decomposition's fetch and let the
        // engine answer over it.
        Err(error) => return Ok(fetch_only(obligations, &scoped, error.to_string())),
    };
    let rules = crate::sparql_rules::tier_one_rules(schema_view);
    let borrowed: Vec<&dyn crate::sparql_rules::Rule> =
        rules.iter().map(|rule| rule.as_ref()).collect();
    if let Err(failure) = crate::sparql_rules::refine(&mut refined, &borrowed) {
        return Ok(fetch_only(
            obligations,
            &scoped,
            format!("a rule broke the plan: {failure}"),
        ));
    }

    let ops = match crate::sparql_ops::lower_refined(&refined, schema_view, scoped.sql_limit) {
        Ok(ops) => ops,
        // No statement the renderer can express. Every shape in the inventory
        // lowers, so this is a guard rather than a path -- and the guard has
        // to be a *fetch* rather than an error, because a query that answers
        // slowly today must not start refusing.
        Err(refusal) => {
            return Ok(fetch_only(
                obligations,
                &scoped,
                format!("not lowerable: {refusal}"),
            ));
        }
    };

    let mut plan = ExecutionPlan {
        contract: PLAN_CONTRACT,
        passes: Vec::new(),
        residual: Vec::new(),
        obligations,
        // Decided by `answers_alone` below; both arms overwrite it.
        refinement: Refinement::Used(None),
    };

    match answers_alone(&plan, &ops) {
        Ok(()) => {
            let claimed: Vec<ObligationId> = ops.claims();
            plan.passes = vec![Pass {
                id: 0,
                inputs: Vec::new(),
                discharges: claimed,
                emits: emitted_from(&ops),
                kind: PassKind::Sql(Box::new(SqlPass { ops })),
            }];
            plan.refinement =
                Refinement::UsedAlone("the statement answers the whole query in SQL".to_owned());
        }
        Err(why) => {
            // A fetch, and the engine finishes. What the statement does not
            // claim is the engine's, computed rather than assumed, because
            // "exactly once" is the invariant this whole design rests on.
            let claimed: BTreeSet<ObligationId> = ops.claims().into_iter().collect();
            let engine_claims: Vec<ObligationId> = (0..plan.obligations.len())
                .filter(|id| !claimed.contains(id))
                .collect();
            plan.passes = vec![
                Pass {
                    id: 0,
                    inputs: Vec::new(),
                    discharges: claimed.iter().copied().collect(),
                    emits: Vec::new(),
                    kind: PassKind::Sql(Box::new(SqlPass { ops })),
                },
                Pass {
                    id: 1,
                    inputs: vec![0],
                    discharges: engine_claims,
                    emits: Vec::new(),
                    kind: PassKind::Engine(EnginePass {
                        causes: scoped.inexact.iter().cloned().collect(),
                    }),
                },
            ];
            plan.refinement = Refinement::Used(Some(why));
        }
    }
    Ok(plan)
}

/// A fetch from the star decomposition, with the engine answering over it.
///
/// The last resort, and deliberately not a planner: the scoper decided which
/// records the query reads, and this states that decision as operators. No
/// aggregate, no solution, nothing claimed beyond the triples the scoper
/// represented -- which is what the endpoint has always fetched when the
/// aggregate route refused.
fn fetch_only(
    obligations: Vec<Obligation>,
    scoped: &crate::sparql_scoper::QueryPlan,
    why: String,
) -> ExecutionPlan {
    let triple_count = obligations
        .iter()
        .filter(|obligation| {
            matches!(
                obligation,
                Obligation::Type { .. } | Obligation::Triple { .. }
            )
        })
        .count();
    let unconsumed: BTreeSet<usize> = scoped.unconsumed.iter().copied().collect();
    // Exactly the triples the scoper represented, and nothing else: a pass
    // that claimed more would be saying it enforced something it did not.
    let sql_claims: Vec<ObligationId> = (0..triple_count)
        .filter(|index| !unconsumed.contains(index))
        .collect();
    let engine_claims: Vec<ObligationId> = (0..obligations.len())
        .filter(|id| !sql_claims.contains(id))
        .collect();
    ExecutionPlan {
        contract: PLAN_CONTRACT,
        passes: vec![
            Pass {
                id: 0,
                inputs: Vec::new(),
                discharges: sql_claims.clone(),
                emits: Vec::new(),
                kind: PassKind::Sql(Box::new(SqlPass {
                    ops: crate::sparql_ops::lower_sql_pass(scoped, &sql_claims),
                })),
            },
            Pass {
                id: 1,
                inputs: vec![0],
                discharges: engine_claims,
                emits: Vec::new(),
                kind: PassKind::Engine(EnginePass {
                    causes: scoped.inexact.iter().cloned().collect(),
                }),
            },
        ],
        residual: Vec::new(),
        obligations,
        refinement: Refinement::Fallback(why),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparql_scoper::{parse_query, tests::test_schema_view};

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

    /// An aggregate no rule takes is named on the artifact, so one call gives
    /// the caller both the route and something to tell whoever wrote the
    /// query.
    ///
    /// **This used to be a refusal with a vocabulary.** The deleted planner
    /// classified the grouped question as a whole and answered
    /// `Blocked(code, detail, instead)` -- a stable code and a rewrite hint.
    /// There is no such decision any more: a rule takes the grouping or it
    /// does not, and what is left shows up in the ledger like every other
    /// obligation. So this reads the ledger, and what went with the codes is
    /// the machine-readable `code` and the suggested rewrite.
    #[test]
    fn an_aggregate_no_rule_takes_is_named_on_the_artifact() {
        let sv = test_schema_view();
        let plan = plan_query_refined(
            &format!(
                "{PREFIX}SELECT (GROUP_CONCAT(?name) AS ?names) \
                 WHERE {{ ?s a asset360:Signal ; asset360:name ?name }}"
            ),
            &sv,
        )
        .expect("an aggregate no rule takes still plans -- the engine answers it");

        let unpushed = plan
            .unpushed_aggregate()
            .expect("GROUP_CONCAT is nobody's rule, so the engine has it");
        assert!(unpushed.contains("GROUP_CONCAT"), "{unpushed}");

        // Still a usable plan: the engine pass answers, so the endpoint is not
        // obliged to refuse the request.
        assert!(plan.is_accounted(), "{plan}");
        assert!(!plan.sql_only(), "{plan}");

        let printed = plan.to_string();
        assert!(printed.contains("not pushed"), "{printed}");
        assert!(printed.contains("GROUP_CONCAT"), "{printed}");
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

        let plan = plan_query_refined(
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
        let plan = plan_query_refined(
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
        let plan = plan_query_refined(
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

    /// A fan-out below a grouping: counted per value, which is per solution.
    ///
    /// `?k` is read and never grouped or aggregated. The deleted planner
    /// refused this outright -- a multivalued read with no binding "has no
    /// container and no instruction", and counting one row per record answers
    /// a different question -- and it was the first query admitted on the
    /// refined plan's own soundness rather than by comparison. That admission
    /// is now the only one there is.
    #[test]
    fn a_fanout_below_a_grouping_counts_solutions() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?nm (COUNT(*) AS ?n) WHERE {{ ?s a asset360:Signal ; \
             asset360:name ?nm ; asset360:trafficKinds ?k }} GROUP BY ?nm"
        );

        let plan = plan_query_refined(&query, &sv).expect("should plan");
        assert!(
            matches!(plan.refinement, Refinement::UsedAlone(_)),
            "expected the statement to answer alone, got {:?}",
            plan.refinement
        );

        // What admission rests on, asserted rather than assumed.
        assert!(plan.sql_only(), "the statement is the query:\n{plan}");
        assert!(plan.is_accounted(), "{plan}");
        plan.ledger_balances().unwrap();
        assert!(
            plan.unpushed_aggregate().is_none(),
            "an artifact must not say an aggregate was left behind while serving it"
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
            assert!(
                plan.unpushed_aggregate().is_none(),
                "{query}: {:?}",
                plan.unpushed_aggregate()
            );
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

    /// Admission asks one question, and there are three outcomes and no
    /// fourth. A statement that claims the whole query answers alone; one that
    /// claims part of it is a fetch the engine finishes; a plan that does not
    /// lower at all leaves the scoper's fetch in place.
    ///
    /// Written as a rehearsal while the comparator was still there to
    /// contradict it, and it earned that: `admit_alone` had one condition
    /// where it needs two -- claiming every obligation is not the same as
    /// being able to *emit* the answer -- and path A took every non-blocked
    /// query first, so nothing ever reached the missing one.
    #[test]
    fn admission_has_three_outcomes() {
        let sv = test_schema_view();

        // Answers alone: everything the query asks for is in the statement.
        let plan = plan_query_refined(
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
        let plan = plan_query_refined(
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
        let plan = plan_query_refined(
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

    /// A statement that does not answer alone is a fetch, and the note says
    /// what it left behind.
    #[test]
    fn a_statement_that_does_not_answer_alone_is_a_fetch() {
        let sv = test_schema_view();
        // `GROUP_CONCAT` is outside the pushable set, and no rule pushes that
        // grouping either.
        let query = format!(
            "{PREFIX}SELECT (GROUP_CONCAT(?nm) AS ?names) WHERE {{ ?s a asset360:Signal ; \
             asset360:name ?nm }}"
        );
        let plan = plan_query_refined(&query, &sv).expect("should plan");
        let note = match &plan.refinement {
            Refinement::Used(Some(note)) => note.clone(),
            other => panic!("expected a fetch with a reason, got {other:?}"),
        };
        assert!(note.contains("does not answer it alone"), "{note}");
        assert!(!plan.sql_only(), "{plan}");
        // And the aggregate it left behind is named, because it is still true.
        assert!(plan.unpushed_aggregate().is_some(), "{plan}");
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
            matches!(plan.refinement, Refinement::UsedAlone(_)),
            "the statement answers it: {:?}",
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

    /// An optional read is claimed by the scan that answers it.
    ///
    /// **The shape that corrected the gate, kept for what it settled.** The
    /// gate first compared claim *ledgers*, and rejected this one because the
    /// refined plan declined to let a narrowing scan claim optionality it did
    /// not render -- rejecting the more truthful plan for a difference that
    /// cost nothing. Comparing the row source admitted it with the difference
    /// reported; absorbing the read into the scan removed the difference.
    ///
    /// Three rounds, three positions, and only the last is stable: the claim
    /// follows whoever answers, and the way to make a claim honest is to make
    /// the node answer rather than to argue about the ledger. Both the gate
    /// and the ledger report are gone; the invariant is not.
    #[test]
    fn an_optional_read_is_claimed_by_the_scan_that_answers_it() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?s ?nm WHERE {{ ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL {{ ?s asset360:name ?nm }} }}"
        );
        let plan = plan_query_refined(&query, &sv).expect("should plan");

        assert!(
            matches!(plan.refinement, Refinement::Used(_)),
            "a fetch the engine finishes: {:?}",
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
    }

    /// And in the other direction: a shape the rules cannot serve is left to
    /// the engine, with a sentence the caller can log.
    ///
    /// Two ways that happens, and they read differently on the artifact: a
    /// statement that pushes part of the query is a *fetch* with a note
    /// saying what it left behind, and one the lowering refuses at all is a
    /// *fallback* to the scoper's own fetch with a reason.
    #[test]
    fn a_shape_the_rules_cannot_serve_is_left_to_the_engine() {
        let sv = test_schema_view();
        for (query, expected) in [
            (
                // An aggregate the grouping rule declines: `COUNT(DISTINCT *)`
                // counts distinct solutions, which `count(*)` does not, so the
                // refined statement would hand the grouping back -- the
                // regression the rule refuses to reproduce. The deleted
                // planner answered it by rendering the distinct away.
                "SELECT (COUNT(DISTINCT *) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm }",
                "COUNT(DISTINCT *)",
            ),
            (
                // An aggregate over a record the query named. The deleted
                // planner answered it from the statement, which is the one
                // answer this deletion changes -- from an invented 1 to the
                // RDF-correct 0. The rules refuse it because the writer
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
            let why = plan
                .refinement
                .reason()
                .or_else(|| plan.refinement.note())
                .unwrap_or_else(|| panic!("{query} answered alone: {:?}", plan.refinement));
            assert!(why.contains(expected), "{query}: {why}");

            // A fallback is still a usable plan: the scoper's fetch narrows
            // the rows and the engine answers over them.
            assert!(!plan.sql_only(), "{query}\n{plan}");
            assert!(plan.is_accounted(), "{query}\n{plan}");
            plan.ledger_balances()
                .unwrap_or_else(|error| panic!("{error} for {query}\n{plan}"));
        }
    }

    /// The division of labour is computed, not assumed: what SQL claims the
    /// engine does not, every obligation is claimed exactly once, and nothing
    /// is left unaccounted for.
    #[test]
    fn the_passes_split_the_claims_and_balance() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?nm WHERE {{ ?s a asset360:Signal ; asset360:name ?nm . \
             FILTER(?nm > \"A\") FILTER(REGEX(?nm, \"^A\")) }}"
        );
        let refined = plan_query_refined(&query, &sv).expect("should plan");
        assert!(
            matches!(refined.refinement, Refinement::Used(_)),
            "{:?}",
            refined.refinement
        );

        let sql_claims = |plan: &ExecutionPlan| -> Vec<String> {
            plan.passes
                .iter()
                .filter(|pass| matches!(pass.kind, PassKind::Sql(_)))
                .flat_map(|pass| pass.discharges.iter())
                .map(|id| plan.obligations[*id].to_string())
                .collect()
        };
        // The comparison filter is SQL's -- a rule pushed it.
        assert!(
            sql_claims(&refined)
                .iter()
                .any(|claim| claim.contains("(?nm > \"A\")")),
            "{refined}"
        );
        // ...and the regex is the engine's, because no rule renders one.
        assert!(
            !sql_claims(&refined)
                .iter()
                .any(|claim| claim.contains("REGEX")),
            "{refined}"
        );
        refined.ledger_balances().unwrap();
        assert!(refined.is_accounted(), "{refined}");
    }

    /// The statement carries the scoper's fetch bound.
    ///
    /// It claims nothing, so no ledger check would miss it and no answer would
    /// be wrong -- the engine still applies the query's own `LIMIT`. What
    /// happens without it is that `LIMIT 1` fetches every row of the class,
    /// which is the regression `test_single_star_limit_1_returns_exactly_one`
    /// caught the last time a planner mislaid it.
    #[test]
    fn the_statement_carries_the_fetch_bound() {
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
    fn an_ordinary_query_names_no_unpushed_aggregate() {
        let sv = test_schema_view();
        let plan = plan_query_refined(
            &format!("{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal }}"),
            &sv,
        )
        .unwrap();

        assert!(plan.unpushed_aggregate().is_none(), "{plan}");
        assert!(!plan.to_string().contains("not pushed"));
    }

    /// The obligations are what the query asks for, one entry each, in a
    /// stable order -- a plan string is only reviewable if the same query
    /// prints the same ids every time.
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
        let plan = plan_query_refined(
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
        let plan = plan_query_refined(
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
            let plan = plan_query_refined(&format!("{PREFIX}{query}"), &sv).unwrap();
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
            refinement: Refinement::Used(None),
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
                    ops: crate::sparql_ops::lower_sql_pass(&scoped, &all_ids(&obligations)),
                })),
            }],
            residual: Vec::new(),
            refinement: Refinement::UsedAlone("hand-built".to_owned()),
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
