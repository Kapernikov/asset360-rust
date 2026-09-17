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
/// 4 added [`crate::sparql_ops::Op::Project`]'s bindings: an ungrouped
/// projection that carries them is a statement that *answers*, and its
/// `sort` and `slice` are the query's own rather than a fetch bound. A
/// consumer built against 3 reads such a `slice` as the fetch bound -- with
/// the offset dropped -- and pages wrong: every page the lowest records.
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
pub const PLAN_CONTRACT: u32 = 4;

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

        // A partial refusal, stated. The kept list is the justification doc
        // 28h §5 requires for answering by the slower route; the refused list
        // is what 28g used to lose without saying so, and it is the more
        // useful of the two to read.
        if let Refinement::Fallback { why, narrowings } = &self.refinement {
            writeln!(f, "  fallback  {why}")?;
            for kept in &narrowings.kept {
                writeln!(f, "      kept      {kept}")?;
            }
            for refused in &narrowings.refused {
                writeln!(f, "      refused   {refused}")?;
            }
            if narrowings.kept.is_empty() && narrowings.refused.is_empty() {
                writeln!(f, "      kept      (no rule derived a narrowing)")?;
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
            // Printed with the same `filter` lead-in as a single condition,
            // because it *is* one filter -- what differs is that its shape is
            // a tree, and the tree's own `Display` shows the connectives. A
            // printout that hid them would hide the one thing worth reading
            // here: which branches a disjunction lifted.
            Op::FilterTree {
                tree, enforcement, ..
            } => writeln!(
                f,
                "      filter    {tree}{}",
                match enforcement {
                    Enforcement::Enforces => "",
                    Enforcement::Narrows => "   narrows",
                }
            )?,
            Op::Unnest { slot_path, .. } => writeln!(f, "      unnest    {}", slot_path.join("."))?,
            // `ALL` spelled out, because which of the two SQL spellings this
            // is is the one thing a reader checks here: a deduplicating
            // `UNION` would drop solutions SPARQL's multiset union keeps.
            Op::Union { left, right } => writeln!(f, "      union all n{left}, n{right}")?,
            Op::Join {
                left_star,
                right_star,
                right_slot,
                right_path,
                right_multivalued,
                kind,
                ..
            } => writeln!(
                f,
                "      join      ?{right_star}.{}{right_slot}{} = ?{left_star}{}",
                right_path
                    .iter()
                    .map(|hop| format!("{hop}."))
                    .collect::<String>(),
                if *right_multivalued { "[]" } else { "" },
                match kind {
                    crate::sparql_scoper::JoinType::Inner => "",
                    crate::sparql_scoper::JoinType::Left => "   left",
                    crate::sparql_scoper::JoinType::Anti => "   anti",
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
            // The projected variables are the pass's emitted ones, already
            // printed on the pass line; what an *answering* projection reads
            // each column from is not, and it is where the term shape lives.
            Op::Project { bindings, .. } => {
                for binding in bindings {
                    writeln!(
                        f,
                        "      column    ?{} ← {}   {}",
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

/// The top-level conjuncts of an expression, in the order it wrote them.
///
/// The obligation count and the plan builder read the same function, because
/// they have to agree on how many conjuncts a `FILTER` has: one obligation per
/// conjunct, one node per conjunct.
pub(crate) fn flatten_conjunction_of(
    expr: &spargebra::algebra::Expression,
) -> Vec<&spargebra::algebra::Expression> {
    let mut out = Vec::new();
    flatten_conjunction(expr, &mut out);
    out
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
    /// The renderer could not express the refined plan as one statement, so
    /// the scoper's decomposition is the fetch and the engine answers over it.
    ///
    /// **Partial, not total.** `why` is the lowering's refusal; `narrowings`
    /// is every restriction the refined plan derived that was merged into that
    /// decomposition anyway, and every one that was refused, each with its
    /// reason. Both lists are the *record* doc 28h §5 requires of a
    /// degrade-and-report: a kept narrowing whose justification nothing states
    /// is the arrangement that produced findings 3, 4 and 10.
    ///
    /// `SeveralIslands` is the shape that matters here -- any equality join
    /// between two stars produces it -- and it used to discard 28g's schema-side
    /// pushdown wholesale.
    Fallback {
        why: String,
        narrowings: Box<KeptNarrowings>,
    },
}

impl Refinement {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Used(_) => "used",
            Self::UsedAlone(_) => "used_alone",
            Self::Fallback { .. } => "fallback",
        }
    }

    /// Why the pipeline produced no statement, for a log line.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Fallback { why, .. } => Some(why),
            _ => None,
        }
    }

    /// What a partial refusal kept and what it refused, or `None` when the
    /// refined plan was used and there was nothing to refuse.
    pub fn narrowings(&self) -> Option<&KeptNarrowings> {
        match self {
            Self::Fallback { narrowings, .. } => Some(narrowings),
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
/// The plan the refinement pipeline *starts* from: every node the engine's.
///
/// Diagnostics, and the counterpart to [`refined_plan_text`]. Reading a refined
/// plan alone shows where the work ended up and not what moved: a node that was
/// always going to be the engine's reads the same as one a rule declined to
/// move. Printing both makes the difference the pipeline actually made legible,
/// which is the whole claim of the design.
///
/// Takes no schema: the naive plan is a transcription of the query's own
/// algebra, and nothing about it depends on what the data looks like. That is
/// the property the ledger rests on -- the obligations are read off the query,
/// so a rule can only ever discharge them, never invent one.
pub fn naive_plan_text(query: &str) -> Result<String, String> {
    crate::sparql_refine::naive_plan_of(query)
        .map(|plan| plan.to_string())
        .map_err(|e| e.to_string())
}

/// The refined plan for a query, as text.
///
/// For diagnostics only, and it exists because a fallback hides its own
/// evidence: the artifact `plan_query_refined` returns carries today's
/// operators, so the plan the gate rejected is gone by the time anyone reads
/// the reason. This is that plan.
///
/// `schema_graph_iri` is the graph the active datamodel serves its schema in,
/// and it is a parameter here for the same reason it is one everywhere else:
/// it decides which rules exist, so a diagnostic that left it out would print a
/// plan production never builds.
pub fn refined_plan_text(
    query: &str,
    schema: &linkml_schemaview::schemaview::SchemaView,
    schema_graph_iri: Option<&str>,
) -> Result<String, String> {
    let naive = crate::sparql_refine::naive_plan_of(query).map_err(|e| e.to_string())?;
    let rules = crate::sparql_rules::tier_one_rules(schema, schema_graph_iri);
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
///
/// `schema_graph_iri` is the named graph the active datamodel serves its schema
/// in, or `None` when it configures none. The scoper needs it to tell a pattern
/// about the datamodel apart from a pattern about a golden record; it is a
/// parameter and not a constant because the deployment's datamodel decides it
/// (see [`crate::sparql_schema_graph`]).
pub fn plan_query_refined(
    query_str: &str,
    schema_view: &SchemaView,
) -> Result<ExecutionPlan, ScopeError> {
    plan_query_refined_with_schema_graph(query_str, schema_view, None)
}

/// [`plan_query_refined`], for a deployment that serves a schema graph.
///
/// See [`crate::sparql_scoper::sparql_scope_with_schema_graph`] for what
/// `schema_graph_iri` is and why it is not a constant.
pub fn plan_query_refined_with_schema_graph(
    query_str: &str,
    schema_view: &SchemaView,
    schema_graph_iri: Option<&str>,
) -> Result<ExecutionPlan, ScopeError> {
    let mut parsed = crate::sparql_scoper::parse_query(query_str)?;
    // Before anything reads a predicate: one slot has two legitimate IRIs when
    // it declares a `slot_uri`, and both routes have to be looking at the same
    // one. Resolved here, on the plan, rather than by rewriting the query text
    // the client sent. See [`crate::sparql_alias`].
    crate::sparql_alias::canonicalize_predicates(&mut parsed, schema_view)
        .map_err(|e| ScopeError::UnsupportedConstruct(e.to_string()))?;
    let parsed = parsed;
    let obligations = obligations_of(&parsed)?;
    let scoped = crate::sparql_scoper::scope_parsed_with_schema_graph(
        &parsed,
        schema_view,
        schema_graph_iri,
    )?;

    let mut refined = match crate::sparql_refine::naive_plan(&parsed) {
        Ok(plan) => plan,
        // A naive plan the builder cannot make is a query this pipeline does
        // not represent. The scoper has already accepted it, so there are rows
        // to fetch: hand back the star decomposition's fetch and let the
        // engine answer over it.
        // No refined plan exists, so there is nothing to keep: a
        // partial refusal is partial in what the *rules* proved.
        Err(error) => return Ok(fetch_only(obligations, &scoped, error.to_string(), None)),
    };
    let rules = crate::sparql_rules::tier_one_rules(schema_view, schema_graph_iri);
    let borrowed: Vec<&dyn crate::sparql_rules::Rule> =
        rules.iter().map(|rule| rule.as_ref()).collect();
    if let Err(failure) = crate::sparql_rules::refine(&mut refined, &borrowed) {
        // A plan a rule broke is a plan whose narrowings nothing vouches
        // for -- the invariant that would have vouched for them is the one
        // that failed. Refuse them all.
        return Ok(fetch_only(
            obligations,
            &scoped,
            format!("a rule broke the plan: {failure}"),
            None,
        ));
    }

    let mut ops = match crate::sparql_ops::lower_refined(
        &refined,
        schema_view,
        scoped.sql_limit,
        scoped.sql_limit_if_unioned,
    ) {
        Ok(ops) => ops,
        // No statement the renderer can express. Every shape in the inventory
        // lowers, so this is a guard rather than a path -- and the guard has
        // to be a *fetch* rather than an error, because a query that answers
        // slowly today must not start refusing.
        Err(refusal) => {
            // **The partial refusal.** The renderer cannot express this
            // plan as one statement, which says nothing about whether an
            // individual narrowing holds of every answer -- so the refined
            // plan comes along and each narrowing is judged on its own.
            return Ok(fetch_only(
                obligations,
                &scoped,
                format!("not lowerable: {refusal}"),
                Some((&refined, schema_view)),
            ));
        }
    };

    // The planner's last decision, and the one the fetch obeys: what each scan
    // must retrieve. It runs here, after the rules and after lowering, because
    // the answer depends on the *query* — the engine is handed the original
    // query text, so what it can still observe is a property of the query and
    // not of what the rules made of it. See `sparql_ops::Retrieval`.
    crate::sparql_ops::declare_retrieval(&mut ops, &parsed, schema_view);

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

/// What a partial refusal kept, and what it refused, each with its reason.
///
/// Both lists, always. A kept narrowing without a recorded justification is
/// the arrangement doc 28h §5 forbids, and a refused one that nobody records
/// is 28g evaporating silently -- which is the defect this whole milestone
/// exists to stop. The strings are diagnostics, printed by
/// [`ExecutionPlan`]'s `Display`; nothing switches on them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeptNarrowings {
    pub kept: Vec<String>,
    pub refused: Vec<String>,
}

/// Merge into the scoper's decomposition every narrowing the refined plan
/// *proved*, and record every one it could not.
///
/// **Why this exists.** `lower_refined` refuses a plan it cannot render as one
/// SQL statement -- `SeveralIslands` above all, which any equality join between
/// two stars produces. Until now that refusal discarded the refined plan
/// entirely and re-derived the fetch from the star decomposition, so every
/// rule-derived narrowing survived *only* while the whole plan collapsed to one
/// island. 28g's schema-side pushdown worked on its motivating shape and
/// evaporated one operator away from it, silently and completely (doc 28h §4).
///
/// **What makes a narrowing keepable is a property, not a shape**, and the
/// property is doc 28h §4.1's sufficiency condition rather than the weaker
/// phrase this function was first written against ("the fetch is a superset of
/// what the query needs"). The engine re-runs the *original query* over the
/// store built from the fetched records, so the fallback is correct exactly
/// when
///
/// ```text
/// eval(Q, fetched) = eval(Q, complete)
/// ```
///
/// as solution bags with RDF term identity. That decomposes into three demands,
/// and this function owns the first:
///
/// * **D1 — every record a solution reads is fetched.** Precondition 1 below.
/// * **D2 — every fetched record is fetched whole.** Not this function's to
///   enforce and this function's to *not break*: it writes only `filters`,
///   `path_filters` and `multivalued_fields`, never `required_fields` or a
///   retrieval, because a fetch that narrows *projection* rather than rows
///   invents answers — omit a present `:p` and `FILTER NOT EXISTS { ?s :p ?v }`
///   reports a solution the database does not have. Asserted by
///   `the_merge_never_adds_a_required_slot`.
/// * **D3 — restrictions do not compose across roles.** Each condition is
///   written to the one star it names, so two roles of one class stay two scans
///   and two `UNION` branches stay two stars. Asserted by
///   `a_narrowing_on_one_role_does_not_restrict_another`.
///
/// Six preconditions, each of which refuses rather than assumes:
///
/// 1. **It reaches every answer (D1).** [`crate::sparql_rules::applies_to_every_answer`]
///    -- the exhaustive one, whose missing `AntiJoin` arm *was* review finding
///    10. A condition under a `LeftJoin`'s optional side, a `Union` branch, a
///    `Minus`/`AntiJoin` right side or a `Service` decides a binding or a
///    negation and not which records exist, so merging it would drop rows.
/// 2. **It is a conjunction.** A `Star`'s `filters` map is `slot -> [condition]`
///    and implicitly conjunctive, so a within-star disjunction (the shape
///    `to_sql` declines and `to_sql_tree` carries) has nowhere to live here.
///    Merging one branch as a conjunct answers a narrower question.
/// 3. **The star is one the fallback fetches**, with the class the refined
///    scan read it as. A condition on a star the decomposition does not have
///    would be dropped by the renderer, and one on a star it scanned as another
///    class would be rendered against the wrong column.
/// 4. **The reading agrees.** `lower_sql_pass` recomputes `Column` versus
///    `AnyElement` from `Star::multivalued_fields`, so a condition the refined
///    plan derived as an element test must land on a slot that star lists as
///    multivalued -- otherwise the renderer compares a constant against the
///    array's text and matches nothing (`ConditionReadsACollection`, the one
///    defect this pipeline shipped). The slot is *added* to that list when the
///    refined plan says it holds several values, because the list's contract is
///    "every slot mentioned on this star"; a disagreement in the other
///    direction refuses.
/// 5. **It is not the identifier slot.** An identifier restriction is
///    `Star::identifier_values`, rendered against the indexed `asset360_uri`
///    column, and the scoper already derives its own. Appending to a list that
///    renders as a conjunctive `IN` from a second source is an intersection
///    nobody asked for (`IdentityUnfolded` is the same fact one level up).
/// 6. **`to_sql` can state it at all.** The same call `lower_refined` makes, so
///    a condition that does not render is refused here exactly as it would be
///    there.
///
/// Note what is *not* a precondition: the refusal that brought us here. Why the
/// renderer cannot express the plan as one statement says nothing about whether
/// an individual condition holds of every answer -- that is the whole content
/// of "partial".
///
/// And note what these preconditions are *not* sufficient for. A restriction a
/// rule derived by **evaluating an expression early** is admitted by §2's
/// conservative rejection of context-dependent and volatile expressions, not by
/// D1--D3: this function cannot tell such a condition from any other, and if
/// that rejection were relaxed without an effect analysis, D1 would still hold
/// of a condition computed from the wrong draw.
fn keep_what_the_rules_proved(
    scoped: &mut crate::sparql_scoper::QueryPlan,
    refined: &crate::sparql_refine::Plan,
    schema: &SchemaView,
) -> KeptNarrowings {
    use crate::sparql_refine::{Executor, PlanOp as RefinedOp};

    let mut out = KeptNarrowings::default();

    // Which class each star was scanned as, and which of its slots the refined
    // plan read as multivalued. Both come from the refined scans, so
    // precondition 3 and 4 are answered by the plan under discussion rather
    // than by a second derivation.
    let mut classes: HashMap<String, String> = HashMap::new();
    let mut multivalued: HashMap<(String, String), bool> = HashMap::new();
    for node in &refined.nodes {
        if let RefinedOp::Scan {
            star_var,
            class_uri,
            slots,
            ..
        } = &node.op
        {
            classes.insert(star_var.clone(), class_uri.clone());
            for slot in slots {
                if let [name] = slot.path.as_slice() {
                    multivalued.insert((star_var.clone(), name.clone()), slot.multivalued);
                }
            }
        }
    }

    for (id, node) in refined.nodes.iter().enumerate() {
        let RefinedOp::Filter { condition, .. } = &node.op else {
            continue;
        };
        // A filter the rules left with the engine narrows nothing here either:
        // it is a condition SQL was never asked to apply.
        if node.executor != Executor::Sql {
            continue;
        }
        // Precondition 1.
        if !crate::sparql_rules::applies_to_every_answer(refined, id) {
            out.refused.push(format!(
                "n{id}: an operator above it makes the constraint conditional, so \
                 the records it excludes can still be in an answer"
            ));
            continue;
        }
        // Preconditions 2 and 6, in one call: `to_sql` declines both a shape it
        // cannot render and a within-star disjunction.
        let Some(conditions) = condition.to_sql(schema, &classes) else {
            out.refused.push(format!(
                "n{id}: not a conjunction of conditions a star's filter map can \
                 hold -- a disjunction merged as a conjunct narrows the answer"
            ));
            continue;
        };
        for condition in conditions {
            let Some(class_uri) = classes.get(&condition.star_var).cloned() else {
                out.refused.push(format!(
                    "n{id}: ?{} is not a star the refined plan scanned",
                    condition.star_var
                ));
                continue;
            };
            let identifier = crate::sparql_ops::identifier_slot_of(schema, &class_uri);
            // Precondition 5.
            if identifier
                .as_deref()
                .is_some_and(|slot| condition.slot_path.as_slice() == [slot.to_owned()])
            {
                out.refused.push(format!(
                    "n{id}: a restriction on the identifier slot belongs against \
                     the indexed column as identifier_values, which the \
                     decomposition derives itself"
                ));
                continue;
            }
            // Precondition 3.
            let Some(star) = scoped
                .root
                .all_stars_mut()
                .into_iter()
                .find(|star| star.variable == condition.star_var)
            else {
                out.refused.push(format!(
                    "n{id}: ?{} is not a star this fetch reads",
                    condition.star_var
                ));
                continue;
            };
            if star.class_uri != class_uri {
                out.refused.push(format!(
                    "n{id}: ?{} is scanned as {} here and as {class_uri} there",
                    condition.star_var, star.class_uri
                ));
                continue;
            }
            match condition.slot_path.as_slice() {
                [] => out
                    .refused
                    .push(format!("n{id}: a condition on no slot at all")),
                // A column of the record itself.
                [slot] => {
                    // Precondition 4.
                    let says_several = matches!(
                        condition.reading,
                        crate::sparql_ops::SlotReading::AnyElement
                            | crate::sparql_ops::SlotReading::BoundElement
                    );
                    let listed = star.multivalued_fields.iter().any(|field| field == slot);
                    match (says_several, listed) {
                        (true, false) => {
                            // The refined plan read it as an array and this
                            // star has not mentioned it. Add the fact rather
                            // than the condition-with-the-wrong-reading: the
                            // list's contract is every slot mentioned on the
                            // star, and it is now mentioned.
                            star.multivalued_fields.push(slot.clone());
                            star.multivalued_fields.sort();
                        }
                        (false, true) => {
                            out.refused.push(format!(
                                "n{id}: reads ?{}.{slot} as a column and this fetch \
                                 holds several values there, so the comparison would \
                                 be against the array's text",
                                condition.star_var
                            ));
                            continue;
                        }
                        _ => {}
                    }
                    let existing = star.filters.entry(slot.clone()).or_default();
                    if existing.contains(&condition.condition) {
                        // Already derived by the scoper. Not a refusal and not
                        // a second conjunct -- the same condition twice renders
                        // as two identical predicates.
                        continue;
                    }
                    existing.push(condition.condition.clone());
                    out.kept.push(format!(
                        "n{id}: ?{}.{slot} -- the constraint reaches every answer",
                        condition.star_var
                    ));
                }
                // A value inside one of the record's columns. Same
                // preconditions; a different field, because the two render
                // differently and `numeric` cannot be read off
                // `numeric_fields` for a nested value.
                path => {
                    let numeric = crate::sparql_scoper::numeric_at_path(schema, &class_uri, path);
                    if let Some(existing) = star
                        .path_filters
                        .iter_mut()
                        .find(|filter| filter.slot_path == path)
                    {
                        if existing.conditions.contains(&condition.condition) {
                            continue;
                        }
                        existing.conditions.push(condition.condition.clone());
                    } else {
                        star.path_filters.push(crate::sparql_scoper::PathFilter {
                            slot_path: path.to_vec(),
                            conditions: vec![condition.condition.clone()],
                            numeric,
                        });
                    }
                    out.kept.push(format!(
                        "n{id}: ?{}.{} -- the constraint reaches every answer",
                        condition.star_var,
                        path.join(".")
                    ));
                }
            }
        }
    }

    out
}

/// A fetch from the star decomposition, with the engine answering over it.
///
/// The last resort, and deliberately not a planner: the scoper decided which
/// records the query reads, and this states that decision as operators. No
/// aggregate, no solution, nothing claimed beyond the triples the scoper
/// represented -- which is what the endpoint has always fetched when the
/// aggregate route refused.
///
/// **Partial since 28h §4.** When a refined plan exists, the narrowings it
/// proved are merged into that decomposition by
/// [`keep_what_the_rules_proved`], and each one kept or refused is recorded on
/// `Refinement::Fallback`. The decomposition is still the *baseline* -- a
/// refusal is a refusal of the lowering, and the scoper's fetch is the superset
/// every kept narrowing then restricts.
fn fetch_only(
    obligations: Vec<Obligation>,
    scoped: &crate::sparql_scoper::QueryPlan,
    why: String,
    refined: Option<(&crate::sparql_refine::Plan, &SchemaView)>,
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
    // The partial refusal. The decomposition is the baseline either way; what
    // a refined plan adds is every narrowing it can prove applies to every
    // answer. The claims above are untouched by design: a merged narrowing
    // *narrows* and claims nothing, so the ledger says exactly what it said
    // before -- see `Enforcement::Narrows`.
    let mut narrowed = scoped.clone();
    let narrowings = match refined {
        Some((refined, schema)) => keep_what_the_rules_proved(&mut narrowed, refined, schema),
        None => KeptNarrowings::default(),
    };
    ExecutionPlan {
        contract: PLAN_CONTRACT,
        passes: vec![
            Pass {
                id: 0,
                inputs: Vec::new(),
                discharges: sql_claims.clone(),
                emits: Vec::new(),
                kind: PassKind::Sql(Box::new(SqlPass {
                    ops: crate::sparql_ops::lower_sql_pass(&narrowed, &sql_claims),
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
        refinement: Refinement::Fallback {
            why,
            narrowings: Box::new(narrowings),
        },
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
    // A statement emits solutions through a grouping or through a projection
    // that carries its columns; without either its rows are records, a
    // fetch.
    if !refined.nodes.iter().any(|node| {
        matches!(node.op, crate::sparql_ops::Op::Group { .. })
            || matches!(&node.op, crate::sparql_ops::Op::Project { bindings, .. } if !bindings.is_empty())
    }) {
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

    /// A `UNION` whose every arm is SQL plans as **one** statement, stacking
    /// the arms with `UNION ALL`.
    ///
    /// It used to plan as the scoper's branch-merged decomposition — one
    /// statement per arm — because `lower_refined` refused any union whole.
    /// That is still what a *mixed* union does (see the test below); what
    /// changed is that the all-SQL case is lowered, which is the only way
    /// anything above the union can reach the database.
    #[test]
    fn a_union_plans_as_one_union_all_statement() {
        let sv = test_schema_view();
        let plan = plan_query_refined(
            &format!(
                "{PREFIX}SELECT ?s WHERE {{ {{ ?s a asset360:Signal }} \
                 UNION {{ ?s a asset360:BaliseGroup }} }}"
            ),
            &sv,
        )
        .expect("a UNION plans");

        assert!(
            !matches!(plan.refinement, Refinement::Fallback { .. }),
            "an all-SQL union lowers rather than falling back: {plan}"
        );
        assert!(plan.is_accounted(), "{plan}");
        assert!(
            !plan.sql_only(),
            "the statement is still a fetch — the engine answers: {plan}"
        );

        let printed = plan.to_string();
        assert!(
            printed.contains("union all"),
            "the arms are stacked, and with ALL: {printed}"
        );
        for class in ["Signal", "BaliseGroup"] {
            assert!(
                printed.contains(class),
                "the fetch must read {class}: {printed}"
            );
        }
    }

    /// The payoff, and the thing issue #410 (pepibru GitLab) measured as
    /// missing: a `LIMIT` above an all-SQL union reaches the statement.
    ///
    /// The same bound as the single-class one, which is the point: the union
    /// stopped being the shape that loses it. And the same refusal: under an
    /// `OFFSET` the engine pages in its own order, not the fetch's, so a bound
    /// there answered page one for every page (`pushable_limit`).
    ///
    /// **And the shape that must not get it**: arms that are each a rooted
    /// `OPTIONAL` join. On its own such an arm carries a bound — on its
    /// *driving scan*, never on its rows (`LimitScope::DrivingScan`) — and the
    /// union has only one place to put a bound, an outer `LIMIT` on the
    /// stacked join product. The argument that makes the bound sound is made
    /// of the driving scan (each of its rows is worth at least one solution)
    /// and says nothing about the product's rows, so a cap there is a cap
    /// nobody has argued for — the kind that answers short with no error the
    /// day a row is not a solution. The union declines, and the fetch is
    /// unbounded rather than reasoned about by luck.
    #[test]
    fn a_limit_above_an_all_sql_union_reaches_the_statement() {
        let sv = test_schema_view();
        let single_star_arms = "{ ?s a asset360:Signal } UNION { ?s a asset360:BaliseGroup }";
        // Each arm is one mandatory star with an optional star hanging off it
        // by a reference, which is the shape whose `LEFT JOIN` the rules push
        // -- so the union is all-SQL and stacks, and the leak would be live.
        let optional_join_arms = "{ ?s a asset360:Signal . \
             OPTIONAL { ?bg a asset360:BaliseGroup ; asset360:refersToSignal ?s ; \
             asset360:asset360_uri ?n } } \
             UNION { ?s a asset360:Track . \
             OPTIONAL { ?x a asset360:Signal ; asset360:locatedOnTrack ?s ; \
             asset360:name ?n } }";
        for (arms, modifiers, expected) in [
            (single_star_arms, "LIMIT 1", Some(1)),
            (single_star_arms, "LIMIT 10 OFFSET 20", None),
            // No limit at all is no bound, rather than a bound of nothing.
            (single_star_arms, "", None),
            // A bound on a joined arm is a bound on its driving scan, and the
            // stacked statement has nowhere to put that. See above.
            (optional_join_arms, "LIMIT 10", None),
        ] {
            let plan = plan_query_refined(
                &format!("{PREFIX}SELECT ?s ?n WHERE {{ {arms} }} {modifiers}"),
                &sv,
            )
            .expect("a UNION plans");
            let Some(crate::sparql_plan::PassKind::Sql(sql)) =
                plan.passes.first().map(|pass| &pass.kind)
            else {
                panic!("the first pass is the statement: {plan}");
            };
            assert_eq!(
                crate::sparql_ops::fetch_bound_of(&sql.ops),
                expected,
                "for modifiers {modifiers:?}: {plan}"
            );
        }
    }

    /// A condition in one arm is resolved against **that arm's** class.
    ///
    /// The arms of a union are the only shape where one variable is scanned as
    /// two classes, and the lowering used to hold one plan-wide
    /// `star -> class` map built from every scan, so the last one won.
    /// `:length` is a slot of `Signal` and not of `BaliseGroup`, which makes
    /// the difference observable as a route: resolved against the wrong arm
    /// the condition does not render at all and the whole query falls back to
    /// the engine. The same mistake on a slot both classes *have* is not a
    /// route change, it is the wrong column — which is why this is pinned on
    /// the shape that shows.
    #[test]
    fn a_condition_in_a_union_arm_resolves_against_its_own_arms_class() {
        let sv = test_schema_view();
        let plan = plan_query_refined(
            &format!(
                "{PREFIX}SELECT ?s WHERE {{ \
                 {{ ?s a asset360:Signal ; asset360:length ?l . FILTER(?l > 10) }} \
                 UNION {{ ?s a asset360:BaliseGroup }} }}"
            ),
            &sv,
        )
        .expect("a UNION plans");

        assert!(
            !matches!(plan.refinement, Refinement::Fallback { .. }),
            "the arm's own class is what resolves its condition: {plan}"
        );
        let printed = plan.to_string();
        assert!(
            printed.contains("union all"),
            "and the arms are still one statement: {printed}"
        );
        assert!(
            printed.contains("length") && printed.contains("numeric"),
            "the condition renders, against Signal's integer column: {printed}"
        );
    }

    /// The same resolution, on a slot **both** arms' classes carry.
    ///
    /// The variant above shows as a route, because `:length` is a slot only
    /// `Signal` has. This one cannot: `spanCount` is an integer on
    /// `TunnelComplex` and a string on `CivilEngineeringAsset`, so a condition
    /// resolved against the other arm's class still renders -- against a
    /// column of the wrong type, comparing `"9" > "10"` lexically or casting a
    /// name to a number. No fallback, no error, a different answer. Both
    /// orderings, because a plan-wide map is last-scan-wins and one ordering
    /// would pass with it broken.
    #[test]
    fn a_condition_resolves_against_its_own_arm_for_a_slot_both_classes_carry() {
        let sv = test_schema_view();
        for (first, condition, second, numeric) in [
            (
                "asset360:TunnelComplex",
                "FILTER(?n > 10)",
                "asset360:CivilEngineeringAsset",
                true,
            ),
            (
                "asset360:CivilEngineeringAsset",
                "FILTER(?n = \"10\")",
                "asset360:TunnelComplex",
                false,
            ),
        ] {
            let plan = plan_query_refined(
                &format!(
                    "{PREFIX}SELECT ?s WHERE {{ \
                     {{ ?s a {first} ; asset360:spanCount ?n . {condition} }} \
                     UNION {{ ?s a {second} }} }}"
                ),
                &sv,
            )
            .expect("a UNION plans");

            assert!(
                !matches!(plan.refinement, Refinement::Fallback { .. }),
                "for {first}: {plan}"
            );
            let printed = plan.to_string();
            assert!(
                printed.contains("union all") && printed.contains("spanCount"),
                "for {first}: {printed}"
            );
            assert_eq!(
                printed.contains("numeric"),
                numeric,
                "the condition is rendered against {first}'s column: {printed}"
            );
        }
    }

    /// A union with one arm the rules could not push keeps the per-arm fetch
    /// it has always had.
    ///
    /// The refusal this asserts is not a limitation to remove later: the arms
    /// bind the same variables, so an arm that lowered and an arm that did not
    /// look like one island with a residual above it, and the statement would
    /// fetch one arm's records for a query that needs both. Short answer,
    /// balanced ledger, no error.
    #[test]
    fn a_union_with_an_engine_arm_keeps_the_per_arm_fetch() {
        let sv = test_schema_view();
        let plan = plan_query_refined(
            &format!(
                "{PREFIX}SELECT ?s WHERE {{ {{ ?s a asset360:Signal }} UNION \
                 {{ ?s a asset360:BaliseGroup ; asset360:name ?nm . \
                 FILTER(STRLEN(?nm) > 3) }} }} LIMIT 1"
            ),
            &sv,
        )
        .expect("a UNION plans");

        assert!(
            matches!(plan.refinement, Refinement::Fallback { ref why, .. } if why.contains("UNION")),
            "a mixed union is refused whole rather than half-pushed: {plan}"
        );
        assert!(plan.is_accounted(), "{plan}");
        let Some(crate::sparql_plan::PassKind::Sql(sql)) =
            plan.passes.first().map(|pass| &pass.kind)
        else {
            panic!("the first pass is the fetch: {plan}");
        };
        assert_eq!(
            crate::sparql_ops::fetch_bound_of(&sql.ops),
            None,
            "and the bound stays off a per-arm fetch, where ten rows of one \
             arm are not the ten the query asked for: {plan}"
        );
    }

    /// The two spellings of one mapped slot plan to the same statement.
    ///
    /// Not "both work": *identical*. The endpoint has two routes and they are
    /// required to answer alike, so an alias that reached the SQL leg as one
    /// column and the engine as another would replace #447's silent empty
    /// column with a silent route-dependent answer. Resolving the alias on the
    /// parse -- before obligations, scoping, rules or lowering see it -- is
    /// what makes that impossible rather than merely tested.
    #[test]
    fn both_spellings_of_a_mapped_slot_plan_to_the_same_statement() {
        let sv = test_schema_view();
        let native = plan_query_refined(
            &format!("{PREFIX}SELECT ?n WHERE {{ ?t a asset360:Track ; asset360:rsmName ?n }}"),
            &sv,
        )
        .expect("the readable spelling plans");
        let canonical = plan_query_refined(
            &format!(
                "{PREFIX}SELECT ?n WHERE \
                 {{ ?t a asset360:Track ; <http://ontorail.org/src/Eulynx/EAID_NAME> ?n }}"
            ),
            &sv,
        )
        .expect("the declared spelling plans");

        assert_eq!(format!("{native:?}"), format!("{canonical:?}"));
    }

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

        // A projection over a fully pushed pattern answers alone too: its
        // rows are the solutions, so the statement is the answer and not a
        // fetch (it was a fetch before `PushProjection`, and the engine
        // applied the projection over the same rows).
        let plan = plan_query_refined(
            &format!(
                "{PREFIX}SELECT ?s ?nm WHERE {{ ?s a asset360:Signal ; \
                 asset360:name ?nm . FILTER(?nm > \"A\") }}"
            ),
            &sv,
        )
        .expect("should plan");
        assert!(
            matches!(plan.refinement, Refinement::UsedAlone(_)),
            "{:?}",
            plan.refinement
        );
        assert!(plan.sql_only(), "the statement answers it:\n{plan}");

        // A fetch: one conjunct the statement cannot take keeps the engine,
        // and the statement narrows.
        let plan = plan_query_refined(
            &format!(
                "{PREFIX}SELECT ?s ?nm WHERE {{ ?s a asset360:Signal ; \
                 asset360:name ?nm . FILTER(?nm > \"A\") \
                 FILTER(REGEX(STR(?s), \"^x\")) }}"
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
        // A fetch, kept one by a conjunct no statement takes: the claim
        // under test is the scan's, whichever pass finishes.
        let query = format!(
            "{PREFIX}SELECT ?s ?nm WHERE {{ ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL {{ ?s asset360:name ?nm }} FILTER(REGEX(STR(?s), \"^x\")) }}"
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

    /// A fetch carries the scoper's fetch bound; a statement that answers
    /// carries the query's own slice instead.
    ///
    /// The bound claims nothing, so no ledger check would miss it and no
    /// answer would be wrong -- the engine still applies the query's own
    /// `LIMIT`. What happens without it is that `LIMIT 1` fetches every row of
    /// the class, which is the regression
    /// `test_single_star_limit_1_returns_exactly_one` caught the last time a
    /// planner mislaid it.
    #[test]
    fn the_statement_carries_the_fetch_bound() {
        let sv = test_schema_view();
        // A fetch: the projected structure has no term shape a statement
        // could emit, so the projection stays the engine's -- and nothing is
        // dropped, so the scoper's bound holds. (A dropped filter would
        // withdraw the bound too, which is a different test.)
        let query = format!(
            "{PREFIX}SELECT ?s ?loc WHERE {{ ?s a asset360:Signal ; \
             asset360:location ?loc }} LIMIT 1"
        );
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

        // Fully pushed, the statement answers: the slice is the query's own,
        // claimed, and there is no fetch bound to read -- a consumer that
        // read the claimed slice as one would drop its offset (issue 457,
        // consolidator-server).
        let query = format!("{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal }} LIMIT 1");
        let answered = plan_query_refined(&query, &sv).expect("should plan");
        assert!(answered.sql_only(), "{answered}");
        assert_eq!(bound_of(&answered), None, "{answered}");
        let claimed: Vec<&Pass> = answered.passes.iter().collect();
        assert!(
            claimed.iter().any(|pass| pass
                .discharges
                .iter()
                .any(|id| matches!(answered.obligations[*id], Obligation::Slice { .. }))),
            "the query's own LIMIT is the statement's: {answered}"
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

    /// The naive text and the refined text must *differ* for a query the
    /// pipeline actually improves, and both must carry the same obligations.
    ///
    /// This is the claim of the design stated as an assertion: refinement moves
    /// work without changing what the query demands. A naive plan that already
    /// read as SQL, or a refined plan that had lost an obligation, would both
    /// pass a test that only checked one of the two strings.
    #[test]
    fn the_naive_text_shows_what_refinement_moved() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?kind (COUNT(*) AS ?n) WHERE {{ \
             ?s a asset360:Signal ; asset360:kind ?kind }} GROUP BY ?kind"
        );

        let naive = naive_plan_text(&query).expect("the naive plan is a transcription");
        let refined = refined_plan_text(&query, &sv, None).expect("this shape refines");

        assert_ne!(naive, refined, "refinement moved nothing");
        // The naive plan is the engine's throughout; the refined one is not.
        assert!(
            naive.contains("[E]"),
            "naive plan should be all engine:\n{naive}"
        );
        assert!(
            !naive.contains("[S]"),
            "naive plan should have no SQL:\n{naive}"
        );
        assert!(
            refined.contains("[S]"),
            "refined plan should reach SQL:\n{refined}"
        );
        // Same demands, before and after.
        for obligation in ["group", "aggregate"] {
            assert!(naive.contains(obligation), "naive lost {obligation}");
            assert!(refined.contains(obligation), "refined lost {obligation}");
        }
    }

    /// A `NOT EXISTS` the statement can express becomes a correlated
    /// anti-join, and the rows the query excludes never leave the database.
    #[test]
    fn a_not_exists_block_becomes_an_anti_join() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?complex WHERE {{ ?complex a asset360:TunnelComplex . \
             FILTER NOT EXISTS {{ ?component a asset360:CivilEngineeringAsset ; \
             asset360:belongsToTunnelComplex ?complex }} }}"
        );
        let plan = plan_query_refined(&query, &sv).unwrap();
        let printed = plan.to_string();

        assert!(
            printed.contains("?component.belongsToTunnelComplex = ?complex   anti"),
            "the negation should be an anti-join:\n{printed}"
        );
        // And it is the statement that applies it: the filter obligation is
        // claimed by the SQL pass rather than left for the engine.
        let sql_claims = match &plan.passes[0].kind {
            PassKind::Sql(_) => plan.passes[0].discharges.clone(),
            PassKind::Engine(_) => panic!("pass 0 should be the statement:\n{printed}"),
        };
        assert_eq!(
            sql_claims.len(),
            plan.obligations.len(),
            "every obligation should be in SQL:\n{printed}"
        );
        plan.ledger_balances().unwrap();
    }

    /// The records a `NOT EXISTS` asks about are in the fetch even when the
    /// statement cannot express the negation.
    ///
    /// The bug this pins: the walk that enumerates triples skipped the filter
    /// *expression*, so the pattern inside was scoped away -- no scan, no
    /// records fetched -- and oxigraph evaluated `NOT EXISTS` against a class
    /// with nothing in it. Every row came back as lacking a component, which
    /// is a wrong answer with a balanced ledger and nothing in the plan to
    /// say so.
    ///
    /// `groupsLines` is a *multivalued* reference, which no rule pushes as an
    /// edge, so this is the declining path: a left-joined fetch of both
    /// classes and the filter left to the engine.
    #[test]
    fn a_not_exists_block_the_statement_declines_is_still_fetched() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?l WHERE {{ ?l a asset360:Line . \
             FILTER NOT EXISTS {{ ?g a asset360:LineGroup ; \
             asset360:groupsLines ?l }} }}"
        );
        let plan = plan_query_refined(&query, &sv).unwrap();
        let printed = plan.to_string();

        // The class the filter asks about is fetched, and fetched optionally:
        // a line in no group is exactly what the query selects, so the join
        // must not delete it.
        assert!(
            printed.contains("scan      asset360:LineGroup"),
            "the NOT EXISTS class is not fetched:\n{printed}"
        );
        assert!(
            printed.contains("as ?g   optional"),
            "the NOT EXISTS scan must be optional:\n{printed}"
        );
        assert!(printed.contains("pass 1  ENGINE"), "{printed}");
        plan.ledger_balances().unwrap();
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

    // -- partial refusal (doc 28h §4) ------------------------------------

    /// The query this milestone exists for: **one equality join away from the
    /// shape that works**.
    ///
    /// A two-column `VALUES` joined onto a scan is a semi-join reduction the
    /// rules derive and the star decomposition does not
    /// (`ValuesNarrowTheJoinedScan`). Add a second star joined to the first on
    /// a value, and the `Sql` frontier is two islands, which the renderer
    /// refuses because a pass is one statement. Until this change that refusal
    /// discarded the refined plan whole and the fetch read the entire class.
    ///
    /// The control is the point of the test: the *same* scoped plan lowered on
    /// its own carries no condition at all, so the assertion says the merge
    /// produced this rather than pinning a string the scoper would satisfy
    /// anyway.
    #[test]
    fn a_narrowing_survives_a_plan_the_renderer_cannot_lower() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?nm ?tag WHERE {{ \
             ?s a asset360:Signal ; asset360:name ?nm . \
             ?t a asset360:Track ; asset360:hasName ?nm . \
             VALUES (?nm ?tag) {{ (\"a\" \"x\") (\"b\" \"y\") }} }}"
        );
        let plan = plan_query_refined(&query, &sv).expect("plans");

        let Refinement::Fallback {
            ref why,
            ref narrowings,
        } = plan.refinement
        else {
            panic!("a value join between two stars is two islands: {plan}");
        };
        assert!(why.contains("islands"), "{why}");
        assert_eq!(narrowings.kept.len(), 1, "{plan}");
        assert!(narrowings.kept[0].contains("?s.name"), "{plan}");

        let printed = plan.to_string();
        assert!(
            printed.contains("filter    name IN ('a', 'b')"),
            "the narrowing must reach the fetch: {printed}"
        );

        // The control. The decomposition on its own derives nothing here, so
        // the condition above is the refined plan's and not the scoper's.
        let parsed = parse_query(&query).expect("parses");
        let scoped = crate::sparql_scoper::scope_parsed_with_schema_graph(&parsed, &sv, None)
            .expect("scopes");
        let bare = crate::sparql_ops::lower_sql_pass(&scoped, &[]);
        assert!(
            !bare
                .nodes
                .iter()
                .any(|node| matches!(node.op, crate::sparql_ops::Op::Filter { .. })),
            "the control must derive nothing, or this test proves nothing: {plan}"
        );
    }

    /// The same, for a value *inside* a column: it lands in `path_filters`
    /// rather than `filters`, and it has to carry its own `numeric`, which
    /// `Star::numeric_fields` cannot answer for a nested value.
    #[test]
    fn a_narrowing_on_a_nested_value_survives_as_a_path_filter() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?lo ?tag WHERE {{ \
             ?s a asset360:Signal ; asset360:location ?loc . ?loc asset360:longitude ?lo . \
             ?t a asset360:Track ; asset360:hasName ?h . \
             VALUES (?lo ?tag) {{ (1 \"x\") (2 \"y\") }} }}"
        );
        let plan = plan_query_refined(&query, &sv).expect("plans");
        let printed = plan.to_string();
        assert!(
            printed.contains("location.longitude IN ('1', '2')") && printed.contains("numeric"),
            "a nested narrowing must reach the fetch, and as a number: {printed}"
        );
    }

    /// And the multivalued case, where getting the *reading* wrong is a wrong
    /// answer rather than a slow query: a containment test rendered as a
    /// comparison against the array's text matches nothing
    /// (`LoweringRefusal::ConditionReadsACollection`, the one defect this
    /// pipeline shipped).
    #[test]
    fn a_narrowing_on_a_multivalued_slot_keeps_its_containment_reading() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?k ?tag WHERE {{ \
             ?s a asset360:Signal ; asset360:trafficKinds ?k . \
             ?t a asset360:Track ; asset360:hasName ?h . \
             VALUES (?k ?tag) {{ (\"m\" \"x\") (\"f\" \"y\") }} }}"
        );
        let plan = plan_query_refined(&query, &sv).expect("plans");
        let Refinement::Fallback { ref narrowings, .. } = plan.refinement else {
            panic!("{plan}");
        };
        assert_eq!(narrowings.kept.len(), 1, "{plan}");

        let Some(PassKind::Sql(sql)) = plan.passes.first().map(|pass| &pass.kind) else {
            panic!("{plan}");
        };
        let readings: Vec<crate::sparql_ops::SlotReading> = sql
            .ops
            .nodes
            .iter()
            .filter_map(|node| match &node.op {
                crate::sparql_ops::Op::Filter { reading, .. } => Some(*reading),
                _ => None,
            })
            .collect();
        assert_eq!(
            readings,
            vec![crate::sparql_ops::SlotReading::AnyElement],
            "a condition on an array is a containment test: {plan}"
        );
    }

    /// **The refusal half, and it is the half that matters.**
    ///
    /// A test of the *code*, not of a query, in the genre doc 28h §3 asks for:
    /// no rule in today's set pushes a narrowing inside a negation — each one
    /// checks its own precondition — so the shape is reached by making the
    /// edit a future rule would make, and asserting the merge stands down.
    ///
    /// The plan is the working two-island plan with an `AntiJoin` appended over
    /// its narrowing. That is exactly review finding 10 one level up: a
    /// constraint inside a negated pattern says what must *not* be there, so
    /// turning it into a restriction on the records fetched inverts the query
    /// and drops rows the negation keeps. `applies_to_every_answer` is the
    /// exhaustive predicate that answers it, and this asserts the merge
    /// consults it.
    #[test]
    fn a_narrowing_inside_a_negation_is_refused_rather_than_kept() {
        use crate::sparql_refine::{Node, PlanOp as RefinedOp};

        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?nm ?tag WHERE {{ \
             ?s a asset360:Signal ; asset360:name ?nm . \
             ?t a asset360:Track ; asset360:hasName ?nm . \
             VALUES (?nm ?tag) {{ (\"a\" \"x\") (\"b\" \"y\") }} }}"
        );
        let parsed = parse_query(&query).expect("parses");
        let mut refined = crate::sparql_refine::naive_plan(&parsed).expect("a naive plan");
        let rules = crate::sparql_rules::tier_one_rules(&sv, None);
        let borrowed: Vec<&dyn crate::sparql_rules::Rule> =
            rules.iter().map(|rule| rule.as_ref()).collect();
        crate::sparql_rules::refine(&mut refined, &borrowed).expect("refines");

        let narrowing = refined
            .nodes
            .iter()
            .position(|node| matches!(node.op, RefinedOp::Filter { .. }))
            .expect("the rules derived a narrowing, or the shape moved");
        let root = refined.nodes.len() - 1;
        refined.nodes.push(Node::engine(
            RefinedOp::AntiJoin {
                left: root,
                right: narrowing,
                reference: None,
            },
            Vec::new(),
        ));

        let mut scoped = crate::sparql_scoper::scope_parsed_with_schema_graph(&parsed, &sv, None)
            .expect("scopes");
        let narrowings = keep_what_the_rules_proved(&mut scoped, &refined, &sv);

        assert!(narrowings.kept.is_empty(), "{narrowings:?}");
        assert_eq!(narrowings.refused.len(), 1, "{narrowings:?}");
        assert!(
            narrowings.refused[0].contains("conditional"),
            "the refusal has to say why: {narrowings:?}"
        );
        assert!(
            scoped
                .root
                .all_stars()
                .iter()
                .all(|star| star.filters.is_empty()),
            "nothing may reach the fetch: {:?}",
            scoped.root.all_stars()
        );
    }

    /// A narrowing on the identifier slot is refused rather than merged.
    ///
    /// Not because it is unsound as a *fetch* restriction — it is the one
    /// narrowing `LoweringRefusal::IdentityIsNotATriple` says is fine for a
    /// fetch and wrong for an answer — but because it belongs in
    /// `Star::identifier_values`, against the indexed column, and the
    /// decomposition derives its own. Appending to a list that renders as a
    /// conjunctive `IN` from a second source is an intersection nobody asked
    /// for.
    #[test]
    fn a_narrowing_on_the_identifier_slot_is_left_to_the_decomposition() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?id ?tag WHERE {{ \
             ?s a asset360:Signal ; asset360:asset360_uri ?id . \
             ?t a asset360:Track ; asset360:hasName ?h . \
             VALUES (?id ?tag) {{ (\"one\" \"x\") (\"two\" \"y\") }} }}"
        );
        let plan = plan_query_refined(&query, &sv).expect("plans");
        let Refinement::Fallback { ref narrowings, .. } = plan.refinement else {
            panic!("{plan}");
        };
        // Either nothing was derived, or it was derived and refused. What must
        // not happen is a second `identifier_values` source.
        assert!(
            narrowings.kept.is_empty(),
            "an identifier restriction is not a column filter: {plan}"
        );
    }

    /// **D2, and it is the demand "keep candidate answers" misses.**
    ///
    /// `eval(Q, fetched) = eval(Q, complete)` is not satisfied by fetching every
    /// record a solution reads: the engine decides `NOT EXISTS { ?s :p ?v }` by
    /// the *absence* of a `:p` triple in the store it built. Fetch the record
    /// and omit a `:p` it actually has, and the store says absent where the
    /// database says present -- so the engine reports a solution the complete
    /// dataset has not got. It does not lose an answer, it **invents** one, and
    /// that is why a present triple which makes a negative pattern false is a
    /// *negative witness* and part of what must be fetched.
    ///
    /// Two halves, both asserted here because both are true today by the shape
    /// of the code and were recorded nowhere: every fallback scan retrieves the
    /// whole record, and the merge never turns a narrowing into a *presence*
    /// requirement. The second is review finding 10's mechanism exactly --
    /// `required_fields` is what renders as `object_data ? 'hasName'`.
    #[test]
    fn the_merge_never_narrows_the_projection() {
        let sv = test_schema_view();
        // Each of these falls back, and the last is the negative-witness shape.
        for query in [
            "SELECT ?nm ?tag WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             ?t a asset360:Track ; asset360:hasName ?nm . \
             VALUES (?nm ?tag) { (\"a\" \"x\") (\"b\" \"y\") } }",
            // A *mixed* union: since asset360-rust#41 an all-SQL union lowers
            // and no longer falls back, so the shape that still exercises the
            // fallback here is the one with an arm the rules could not push.
            "SELECT ?s WHERE { { ?s a asset360:Signal } UNION \
             { ?s a asset360:BaliseGroup ; asset360:name ?nm . FILTER(STRLEN(?nm) > 3) } }",
            "SELECT ?nm ?tag WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             ?t a asset360:Track ; asset360:hasName ?h . \
             VALUES (?nm ?tag) { (\"a\" \"x\") } \
             FILTER NOT EXISTS { ?s asset360:kind ?k } }",
        ] {
            let query = format!("{PREFIX}{query}");
            let plan = plan_query_refined(&query, &sv).expect("plans");
            assert!(
                matches!(plan.refinement, Refinement::Fallback { .. }),
                "this test only says anything about the fallback: {plan}"
            );
            let Some(PassKind::Sql(sql)) = plan.passes.first().map(|pass| &pass.kind) else {
                panic!("{plan}");
            };
            for node in &sql.ops.nodes {
                let crate::sparql_ops::Op::Scan {
                    retrieval,
                    required_slots,
                    ..
                } = &node.op
                else {
                    continue;
                };
                assert_eq!(
                    *retrieval,
                    crate::sparql_ops::Retrieval::Whole,
                    "a fallback scan that projects cannot answer a negation: {plan}"
                );
                // The merge writes conditions, never presence requirements.
                // The scoper's own `required_fields` are the query's reads and
                // are not this function's; what must not appear is a slot only
                // a *narrowing* mentioned.
                assert!(
                    !required_slots.iter().any(|slot| slot == "kind"),
                    "a slot read only inside a negation must not become required: {plan}"
                );
            }
        }
    }

    /// **D3.** A record narrowed in one role is still needed unrestricted in
    /// another. Two stars of the same class are two scans, and the store is the
    /// *union* of their fetches, so a condition written to `?s` must not reach
    /// `?o` -- which is the shape in which restrictions from two `UNION`
    /// branches would otherwise meet as a conjunction on one scan.
    #[test]
    fn a_narrowing_on_one_role_does_not_restrict_another() {
        let sv = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?s ?o ?tag WHERE {{ \
             ?s a asset360:Signal ; asset360:name ?nm . \
             ?o a asset360:Signal ; asset360:length ?len . \
             ?t a asset360:Track ; asset360:hasName ?nm . \
             VALUES (?nm ?tag) {{ (\"a\" \"x\") }} }}"
        );
        let plan = plan_query_refined(&query, &sv).expect("plans");
        let Some(PassKind::Sql(sql)) = plan.passes.first().map(|pass| &pass.kind) else {
            panic!("{plan}");
        };
        let narrowed: Vec<&String> = sql
            .ops
            .nodes
            .iter()
            .filter_map(|node| match &node.op {
                crate::sparql_ops::Op::Filter { star_var, .. } => Some(star_var),
                _ => None,
            })
            .collect();
        // `?s` and `?t` both bind `?nm` in a slot position, so the semi-join
        // reduction reaches both and D1 holds of each: every answer's `?s` has
        // `name = 'a'` and every answer's `?t` has `hasName = 'a'`. `?o` is the
        // point -- it scans the *same class* as `?s` in a different role, binds
        // nothing the `VALUES` constrains, and must come back unrestricted.
        assert!(narrowed.contains(&&"s".to_owned()), "{plan}");
        assert!(
            !narrowed.contains(&&"o".to_owned()),
            "a second role of the same class must not inherit the restriction: {plan}"
        );
    }
}
