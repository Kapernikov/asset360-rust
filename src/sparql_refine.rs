//! The naive plan: the query, faithfully, with nothing pushed down.
//!
//! The single-pass planner this replaced decided pushdown once, for the query
//! as a whole: `analyse_pushdown` answered eligible or blocked, and one filter
//! it could not express in SQL cost the entire grouping. That cliff is a
//! property of *when* the decision is made. A planner that starts from "the
//! engine does everything" and moves work down one step at a time ends
//! wherever it runs out of rules, which is partial pushdown by construction.
//!
//! This module is the starting point of that: the algebra as the query wrote
//! it, every node tagged [`Executor::Engine`], nothing folded. Three things
//! make it expressible where the operator set in [`crate::sparql_ops`] does
//! not:
//!
//! * an [`Expr`] tree that holds anything spargebra parsed -- `REGEX`,
//!   arithmetic, a function call -- so "can this be pushed" stops being a fact
//!   decided during scoping and becomes a question asked of a node
//!   ([`Expr::to_sql`], which is partial);
//! * nodes for the constructs SQL cannot do at all -- union, minus, a left
//!   join as an operator, a property path, a bind, a sub-select -- so a
//!   refusal becomes "no rule pushed this" rather than a verdict code;
//! * an [`Executor`] tag per node, which a rule flips bottom-up, and an
//!   [`OutputKind`] per node so `CONSTRUCT` / `DESCRIBE` / `ASK` are
//!   representable before they are pushable.
//!
//! **The vocabulary is [`crate::sparql_plan`]'s**: an *obligation* is one
//! thing the query demands, to *discharge* one is for a node to take care of
//! it, and the *residual* is whatever no node took care of. A naive plan
//! discharges everything -- it is already correct, being what the endpoint
//! does today when nothing is pushed -- so its residual is empty.
//!
//! # Why this is a second artifact rather than a change to `ExecutionPlan`
//!
//! An [`crate::sparql_plan::ExecutionPlan`] is passes made of operators, and
//! it is what `views.py` and `sql_builder.py` read. Refinement works in a
//! different shape — one tree cut by a frontier — and
//! [`crate::sparql_plan::plan_query_refined`] *lowers* the refined tree into
//! those operators, so the two artifacts stay separate: this module's [`Plan`]
//! is what the rules rewrite, an `ExecutionPlan` is what the callers render.
//!
//! Historically this separation also bought safety. While a second, single-pass
//! planner still existed, nothing here was reachable from it, so the machinery
//! could be added without changing an answer. That planner is gone and
//! refinement is the only one; the split now survives only because it is the
//! rules' shape versus the renderers'.
//!
//! # The invariants
//!
//! Four are the design's, re-checked after every rule application because a
//! rule that forgets to move an obligation, claims one twice, or reparents a
//! node without renumbering must fail a check rather than quietly answer a
//! different question:
//!
//! 1. [`Plan::ledger_balances`] -- every obligation discharged exactly once.
//! 2. [`Plan::well_formed`] -- every node's inputs precede it.
//! 3. [`Plan::frontier_is_a_cut`] -- no `Sql` node above an `Engine` node,
//!    since SQL cannot consume engine output.
//! 4. [`Plan::root_matches_form`] -- the root's output kind is what the query
//!    form produces.
//!
//! There is a fifth, and it is the reason [`ScanSlot`] carries `multivalued`
//! rather than a bare slot name. 28d states the fold rule's precondition --
//! fold a multivalued slot only together with its unnest -- and says no
//! invariant can catch a violation, because the obligations are still all
//! claimed exactly once by the right nodes. That is true of a scan whose slots
//! are just names: multiplicity is then a fact about the schema, and the plan
//! cannot see it. Carrying it on the slot makes the check local, so
//! [`Plan::fanout_restored`] catches the missing unnest that would otherwise
//! only show up as a count of 1 where the answer was 3.

use std::collections::{BTreeSet, HashMap};
use std::fmt;

use linkml_schemaview::schemaview::SchemaView;

use spargebra::Query;
use spargebra::algebra::{
    AggregateExpression, Expression, GraphPattern, OrderExpression, PropertyPathExpression,
};
use spargebra::term::{GroundTerm, NamedNodePattern, Term, TermPattern, TriplePattern, Variable};

pub use crate::sparql_ops::SlotReading;
use crate::sparql_plan::{LedgerError, Obligation, ObligationId, obligation_of_triple, shorten};
use crate::sparql_scoper::{FilterCondition, LikeAnchor, PushForm, ScopeError, literal_pushable};

/// Index into [`Plan::nodes`]. Printed as `n0`, `n1`, ... so a reader can
/// follow a node's inputs by eye, the way `o0`, `o1` work for obligations.
pub type NodeId = usize;

// ---------------------------------------------------------------------------
// Executor, output kind, query form
// ---------------------------------------------------------------------------

/// Who runs a node.
///
/// The naive plan sets every node to [`Executor::Engine`]; rules flip nodes to
/// [`Executor::Sql`], bottom up. The tag partitions the tree at the frontier:
/// the maximal `Sql` subtree becomes the statement, the rest is the engine's.
///
/// This is what [`crate::sparql_ops::Enforcement`] collapses into once the
/// split is load-bearing. A node is `Sql` while the engine also re-applies it
/// -- exactly today's `Narrows` -- because the engine leg re-runs the whole
/// original query string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Executor {
    Sql,
    Engine,
}

impl Executor {
    /// The one-character column in a plan printout, as 28d writes it.
    pub fn tag(&self) -> &'static str {
        match self {
            Self::Sql => "[S]",
            Self::Engine => "[E]",
        }
    }
}

/// What a node produces.
///
/// Carried per node so the query forms can sit as shaping nodes at the top of
/// a plan without disturbing the frontier below them. A `CONSTRUCT` whose plan
/// roots in a `Project` is a planner bug, and before this it would have been
/// caught by nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputKind {
    /// A solution sequence: variable bindings. Everything the endpoint
    /// answers today.
    Solutions,
    /// An RDF graph.
    Triples,
    /// A single boolean.
    Boolean,
}

impl fmt::Display for OutputKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Solutions => "solutions",
            Self::Triples => "triples",
            Self::Boolean => "boolean",
        })
    }
}

/// Which of the four query forms a plan answers.
///
/// SPARQL Update is permanently out of scope -- writes reach golden records
/// through publish, which validates -- so there is no fifth variant and a plan
/// is a tree to be evaluated rather than a program with effects. That is what
/// lets a rule reorder or duplicate work freely, and what makes executing a
/// plan twice (rules on, rules off) a valid test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryForm {
    Select,
    Construct,
    Describe,
    Ask,
}

impl QueryForm {
    pub fn of(query: &Query) -> Self {
        match query {
            Query::Select { .. } => Self::Select,
            Query::Construct { .. } => Self::Construct,
            Query::Describe { .. } => Self::Describe,
            Query::Ask { .. } => Self::Ask,
        }
    }

    /// The output kind this form produces, which the plan's root must match.
    pub fn expects(&self) -> OutputKind {
        match self {
            Self::Select => OutputKind::Solutions,
            Self::Construct | Self::Describe => OutputKind::Triples,
            Self::Ask => OutputKind::Boolean,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Construct => "CONSTRUCT",
            Self::Describe => "DESCRIBE",
            Self::Ask => "ASK",
        }
    }
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

/// A comparison, as the query wrote it.
///
/// Wider than [`crate::sparql_scoper::CmpOp`] on purpose. That type is the
/// four ordering comparisons only; `!=` maps to [`FilterCondition::Ne`]
/// directly rather than through a `CmpOp`, because its renderer owes an extra
/// `IS NOT NULL` that an ordering comparison does not -- see
/// [`CompareOp::as_condition`]. A naive plan has to hold the comparison the
/// query wrote regardless, so `Ne` is representable here whether or not the
/// rule below it can push it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl CompareOp {
    /// The condition SQL renders for this comparison against `value`, when
    /// there is one.
    ///
    /// The one mapping from SPARQL's comparison vocabulary to the renderer's,
    /// shared by [`Expr::to_sql`] and by the `HAVING` lowering.
    ///
    /// `!=` maps to [`FilterCondition::Ne`], and what that owes the renderer
    /// is not the same as `<>`: SPARQL's inequality is false for an unbound
    /// variable where SQL's `<>` on `NULL` is unknown, so a bare `<>` drops
    /// exactly the rows an `OPTIONAL` keeps and the groups whose aggregate is
    /// unbound. The renderer has to compensate -- `expr IS NOT NULL AND expr
    /// <> %s` -- which is why `Ne` is its own [`FilterCondition`] arm rather
    /// than `Cmp` with an operator string of `"<>"`: a renderer reading `Cmp`
    /// has no way to know it owes the extra `IS NOT NULL`, while a renderer
    /// reading `Ne` cannot render it without supplying one.
    pub fn as_condition(&self, value: String) -> Option<FilterCondition> {
        use crate::sparql_scoper::CmpOp;
        let op = match self {
            Self::Eq => return Some(FilterCondition::Eq(value)),
            Self::Ne => return Some(FilterCondition::Ne(value)),
            Self::Lt => CmpOp::Lt,
            Self::Lte => CmpOp::Lte,
            Self::Gt => CmpOp::Gt,
            Self::Gte => CmpOp::Gte,
        };
        Some(FilterCondition::Cmp { op, value })
    }

    /// The same comparison written the other way round: `3 < ?n` asks what
    /// `?n > 3` asks, so a flipped spelling is not a different question.
    pub fn flipped(&self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::Ne => Self::Ne,
            Self::Lt => Self::Gt,
            Self::Lte => Self::Gte,
            Self::Gt => Self::Lt,
            Self::Gte => Self::Lte,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "!=",
            Self::Lt => "<",
            Self::Lte => "<=",
            Self::Gt => ">",
            Self::Gte => ">=",
        }
    }
}

/// A value expression, as the query wrote it.
///
/// The point of the tree is that the pushable subset stops being a constant of
/// the planner and becomes the sum of what the rules accept: [`Expr::to_sql`]
/// is partial, and a rule declines what it cannot render.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A SPARQL variable, without the `?`.
    Var(String),
    Literal(Term),
    /// A path from a star's root to a value: what the SQL side can read.
    ///
    /// Not producible from the query alone -- deciding that `?name` is
    /// `?s.hasName` needs the schema and the star decomposition. The rule that
    /// rewrites a [`Expr::Var`] into one of these is the filter rule, once a
    /// [`PlanOp::Scan`] below it says which slot binds the variable. Until
    /// then a naive filter cannot render, which is the honest answer: a
    /// variable is not a column.
    Slot {
        star_var: String,
        slot_path: Vec<String>,
        /// Which of the three values at that address this is. See
        /// [`SlotReading`]; without it the address is ambiguous on any
        /// multivalued slot.
        reading: SlotReading,
        /// Whether the scan that reads this slot requires the value or only
        /// allows it. See [`SlotPresence`].
        ///
        /// Carried the same way `reading` already is, rather than re-derived
        /// from the plan when a rule needs it: the fact lives on the
        /// [`ScanSlot`] a scan reads, and `sparql_rules::Visible::collect`
        /// already visits every such slot while building a
        /// `sparql_rules::SlotBinding` -- it used to read `slot.presence` and
        /// then drop it. A `!bound` filter is sound to push only on an
        /// `Optional` slot (a `Required` one's scan already enforces
        /// existence, so `!bound` on it is unsatisfiable and the plan has no
        /// way to state that), and that question is asked once a variable has
        /// already become a slot -- inside [`Expr::to_sql`], which does not
        /// see the scan, only the expression. Riding along on `Expr::Slot` is
        /// what makes the fact available there without widening `to_sql`'s
        /// own signature, the way `numeric` and `right_multivalued` already
        /// ride on other join and scan facts elsewhere in this crate.
        presence: SlotPresence,
    },
    Compare {
        op: CompareOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Membership, kept apart from a chain of `Or`s because SQL has `IN` and
    /// [`FilterCondition::In`] is what the renderer already speaks.
    In {
        value: Box<Expr>,
        candidates: Vec<Expr>,
    },
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    /// Anything else spargebra parsed as a call: `REGEX`, `BOUND`, `IF`,
    /// arithmetic, a user function. Kept whole so the engine can evaluate it
    /// and a rule can decline it.
    Function {
        name: String,
        args: Vec<Expr>,
    },
    /// An expression carrying a graph pattern -- `EXISTS`, `NOT EXISTS` --
    /// kept as the text the query wrote.
    ///
    /// [`Expr::Function`] cannot hold one: its arguments are expressions, and
    /// a pattern is not one. Text rather than a nested plan because the only
    /// consumer that has to *evaluate* this is the engine, and the engine
    /// re-runs the original query string; a plan needs the expression in order
    /// to reason about it, and "there is an EXISTS here, and no rule pushes
    /// it" is the whole of that reasoning today.
    Opaque(String),
}

/// A comparison SQL can express, in the vocabulary the renderer already
/// speaks.
///
/// [`Expr::to_sql`] returns these rather than a string because SQL text needs
/// a table alias, a JSONB path and the slot's numeric-ness, none of which live
/// on an expression -- and a "SQL string" that is not runnable SQL is a trap
/// for the next reader. `sql_builder.py` renders a
/// (star, path, [`FilterCondition`]) triple already.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCondition {
    pub star_var: String,
    pub slot_path: Vec<String>,
    pub condition: FilterCondition,
    /// Which value at that address the condition holds of. A renderer that
    /// ignores this renders a containment test as an equality on an array,
    /// which matches nothing.
    pub reading: SlotReading,
}

/// A condition SQL can ask whose shape is a *tree* rather than a conjunction.
///
/// The representation `FILTER(A || B)` needed and did not have. Every
/// [`SqlCondition`] a filter operator carries is implicitly one conjunct of an
/// implicitly conjunctive list -- `_OpScan.filters` on the Python side is
/// `slot -> [conditions]` -- so a disjunction had nowhere to go: it was
/// accepted, scoped, and never lifted. That is not a slow query but a failed
/// one, because a filter that does not narrow makes the fetch read the whole
/// class and the engine leg is bounded by `TripleLimitExceeded`.
///
/// **A contract, not an internal detail.** A PyO3 class mirrors it and
/// `sql_builder.py` renders it, so the shape is read in two other repos.
/// Three things about it are load-bearing:
///
/// * **The leaf is an [`SqlCondition`] plus the physical facts a renderer
///   needs**, rather than a slot path and an operator string. One shape for
///   "a condition SQL can ask" means a leaf cannot drift from what
///   [`Expr::to_sql`] produces -- and `numeric` rides on the *leaf* because
///   `?nm = "BX517" || ?len > 10` is one operator whose two branches disagree
///   about it. A per-operator flag would cast one of them wrong, and
///   comparing a number as text makes `'9' >= '10'` true.
/// * **`All` and `Any` are n-ary and never empty.** `Any([])` is `FALSE`: a
///   pushed condition that selects *nothing* makes the engine re-run the query
///   over no records and answer empty to a query that has an answer, which is
///   the one direction a pushed condition is not free in. `to_sql_tree`
///   declines an empty branch list rather than emitting one.
/// * **A consumer must refuse a node it does not know.** There is no
///   "unknown condition" node and no wildcard in the walks below, so a new
///   [`FilterCondition`] arm or a new node kind is a compile error at every
///   consumer in this crate rather than a branch silently skipped -- and a
///   silently skipped branch of a disjunction narrows the answer to the other
///   branch.
///
/// **`Not` is representable and is not produced today**, deliberately. SQL's
/// three-valued logic makes `NOT (col = 'x')` *unknown* rather than true where
/// the column is NULL, so a negated subtree drops exactly the rows a narrowing
/// fetch has to keep -- the same debt [`FilterCondition::Ne`] pays with an
/// explicit `IS NOT NULL`, which a renderer reading a bare `Not` has no way to
/// know it owes. Negation therefore reaches SQL as a *leaf* (`Ne`,
/// `NotBound`), whose renderer knows what it owes. The second reason is the
/// reading weakening a narrowing pass applies (see
/// `sparql_ops::reading_for_enforcement`): relaxing a leaf from "the element
/// this row bound" to "some element of the array" only relaxes the whole
/// condition under monotone connectives, and `Not` is not one. A producer of
/// `Not` owes an answer to both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionTree {
    /// Every branch holds. Non-empty.
    All(Vec<ConditionTree>),
    /// Some branch holds. Non-empty -- see the type's doc comment.
    Any(Vec<ConditionTree>),
    /// The branch does not hold. Never produced by [`Expr::to_sql_tree`]; see
    /// the type's doc comment for the two things a producer owes first.
    Not(Box<ConditionTree>),
    Leaf(ConditionLeaf),
}

/// One condition at the bottom of a [`ConditionTree`], with the physical facts
/// the address does not carry.
///
/// `numeric` is here rather than on the operator because the branches of one
/// disjunction name different slots: see [`ConditionTree`]'s doc comment.
/// `reading` is already inside the [`SqlCondition`], for the same reason it is
/// there for a single filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConditionLeaf {
    pub condition: SqlCondition,
    /// Whether the value compares as a number rather than as text.
    ///
    /// Resolved where the schema is -- [`Expr::to_sql_tree`] -- through the
    /// same `numeric_at_path` a single filter's lowering asks, so the two
    /// cannot disagree about a column.
    pub numeric: bool,
}

impl ConditionTree {
    /// Every leaf, left to right.
    ///
    /// The walk every consumer of the tree needs and the reason `Not` is a
    /// node rather than a flag: a check that has to hold of each leaf --
    /// "its star is scanned", "it does not read a collection as a column" --
    /// asks it here rather than re-implementing the recursion, and cannot
    /// therefore forget a branch.
    pub fn leaves(&self) -> Vec<&ConditionLeaf> {
        let mut out = Vec::new();
        self.collect_leaves(&mut out);
        out
    }

    fn collect_leaves<'a>(&'a self, out: &mut Vec<&'a ConditionLeaf>) {
        match self {
            Self::All(branches) | Self::Any(branches) => {
                for branch in branches {
                    branch.collect_leaves(out);
                }
            }
            Self::Not(inner) => inner.collect_leaves(out),
            Self::Leaf(leaf) => out.push(leaf),
        }
    }

    /// The one star every leaf names, or `None` when the leaves name several
    /// (or none at all).
    ///
    /// **The boundary of what a tree may lift, and it is a hard refusal.** A
    /// disjunction spanning two stars cannot narrow either star's fetch
    /// independently: the branches only separate after the join, so rendering
    /// one side would answer a different question and rendering both as a
    /// conjunction a narrower one. Such a filter stays unlifted and the engine
    /// finishes it.
    ///
    /// One function rather than a check in `to_sql_tree` and a second
    /// derivation where the operator names its star: a derivation performed
    /// twice is how an operator comes to disagree with the tree it carries.
    pub fn star(&self) -> Option<&str> {
        let leaves = self.leaves();
        let (first, rest) = leaves.split_first()?;
        let star = first.condition.star_var.as_str();
        rest.iter()
            .all(|leaf| leaf.condition.star_var == star)
            .then_some(star)
    }

    /// The same tree with every leaf rewritten.
    ///
    /// For the one physical fact the lowering rather than the expression
    /// decides: whether a narrowing pass, having dropped the fan-out, still
    /// has an element for a leaf to name. Sound only because `All` and `Any`
    /// are monotone in their branches -- see this type's note on `Not`.
    pub fn map_leaves(&self, rewrite: &impl Fn(&ConditionLeaf) -> ConditionLeaf) -> Self {
        let branches = |branches: &[Self]| -> Vec<Self> {
            branches
                .iter()
                .map(|branch| branch.map_leaves(rewrite))
                .collect()
        };
        match self {
            Self::All(parts) => Self::All(branches(parts)),
            Self::Any(parts) => Self::Any(branches(parts)),
            Self::Not(inner) => Self::Not(Box::new(inner.map_leaves(rewrite))),
            Self::Leaf(leaf) => Self::Leaf(rewrite(leaf)),
        }
    }
}

impl fmt::Display for ConditionTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let joined = |f: &mut fmt::Formatter<'_>, branches: &[Self], sep: &str| -> fmt::Result {
            f.write_str("(")?;
            for (index, branch) in branches.iter().enumerate() {
                if index > 0 {
                    write!(f, " {sep} ")?;
                }
                write!(f, "{branch}")?;
            }
            f.write_str(")")
        };
        match self {
            Self::All(branches) => joined(f, branches, "AND"),
            Self::Any(branches) => joined(f, branches, "OR"),
            Self::Not(inner) => write!(f, "NOT {inner}"),
            // Both physical facts the leaf carries are printed, the way the
            // single-condition line prints them: a printout that hid them
            // would hide what a renderer depends on, and inside a tree they
            // differ from branch to branch.
            Self::Leaf(leaf) => write!(
                f,
                "{}{}{} {}",
                leaf.condition.slot_path.join("."),
                leaf.condition.reading,
                if leaf.numeric { "[num]" } else { "" },
                leaf.condition.condition
            ),
        }
    }
}

impl Expr {
    /// The conditions this expression *has the shape of*, without asking
    /// whether they mean what the query means.
    ///
    /// Not public, and the name says why: the shape is only half the test.
    /// A condition compares stored text, so it asks the query's question only
    /// when the constant is the term the column's values render as -- and
    /// deciding that needs the schema, which an expression does not have.
    /// [`Expr::to_sql`] is the public entry and asks both halves. See
    /// [`constant_is_the_columns_term`] for what the second half prevents.
    ///
    /// Partial in two directions, and both are the point:
    ///
    /// * by construct -- `REGEX`, arithmetic, `!=`, a disjunction, a
    ///   comparison between two slots all decline;
    /// * by *state* -- an expression over [`Expr::Var`] declines because a
    ///   variable is not a column. It becomes renderable when a rule has
    ///   rewritten the variable into an [`Expr::Slot`], which is the same
    ///   thing as saying a filter can only be pushed once the scan below it
    ///   exists.
    ///
    /// A conjunction is all-or-nothing here: splitting it -- pushing the half
    /// that renders and leaving the rest above -- changes who *claims* the
    /// filter obligation, and that is a rule's decision to make and to record,
    /// not a rendering detail.
    /// The same expression with one variable replaced by an expression.
    ///
    /// For the rule that sinks a condition on a group key below the grouping:
    /// the key is a variable in the query's vocabulary and a column in SQL's,
    /// and this is the substitution between them.
    pub fn substitute_var(&self, from: &str, to: &Expr) -> Self {
        match self {
            Self::Var(var) if var == from => to.clone(),
            Self::And(parts) => Self::And(
                parts
                    .iter()
                    .map(|part| part.substitute_var(from, to))
                    .collect(),
            ),
            Self::Or(parts) => Self::Or(
                parts
                    .iter()
                    .map(|part| part.substitute_var(from, to))
                    .collect(),
            ),
            Self::Not(inner) => Self::Not(Box::new(inner.substitute_var(from, to))),
            Self::Compare { op, left, right } => Self::Compare {
                op: *op,
                left: Box::new(left.substitute_var(from, to)),
                right: Box::new(right.substitute_var(from, to)),
            },
            Self::In { value, candidates } => Self::In {
                value: Box::new(value.substitute_var(from, to)),
                candidates: candidates.clone(),
            },
            other => other.clone(),
        }
    }

    /// The same expression with one variable renamed.
    ///
    /// For a rule that folds a rename into the node that produces the value:
    /// the alias disappears, so every reference to the old name has to become
    /// a reference to the new one or it names nothing.
    pub fn rename_var(&self, from: &str, to: &str) -> Self {
        match self {
            Self::Var(var) if var == from => Self::Var(to.to_owned()),
            Self::And(parts) => {
                Self::And(parts.iter().map(|part| part.rename_var(from, to)).collect())
            }
            Self::Or(parts) => {
                Self::Or(parts.iter().map(|part| part.rename_var(from, to)).collect())
            }
            Self::Not(inner) => Self::Not(Box::new(inner.rename_var(from, to))),
            Self::Compare { op, left, right } => Self::Compare {
                op: *op,
                left: Box::new(left.rename_var(from, to)),
                right: Box::new(right.rename_var(from, to)),
            },
            Self::In { value, candidates } => Self::In {
                value: Box::new(value.rename_var(from, to)),
                candidates: candidates.clone(),
            },
            other => other.clone(),
        }
    }

    pub(crate) fn sql_shape_unchecked(&self) -> Option<Vec<SqlCondition>> {
        match self {
            Self::And(parts) => {
                let mut out = Vec::new();
                for part in parts {
                    out.extend(part.sql_shape_unchecked()?);
                }
                Some(out)
            }
            Self::Compare { op, left, right } => {
                let (slot, value) = slot_and_value(left, right)?;
                let condition = op.as_condition(value)?;
                let Self::Slot {
                    star_var,
                    slot_path,
                    reading,
                    ..
                } = slot
                else {
                    return None;
                };
                Some(vec![SqlCondition {
                    star_var: star_var.clone(),
                    slot_path: slot_path.clone(),
                    condition,
                    reading: *reading,
                }])
            }
            Self::In { value, candidates } => {
                let Self::Slot {
                    star_var,
                    slot_path,
                    reading,
                    ..
                } = value.as_ref()
                else {
                    return None;
                };
                let mut values = Vec::with_capacity(candidates.len());
                for candidate in candidates {
                    match candidate {
                        Self::Literal(term) => values.push(lexical(term)),
                        _ => return None,
                    }
                }
                Some(vec![SqlCondition {
                    star_var: star_var.clone(),
                    slot_path: slot_path.clone(),
                    condition: FilterCondition::In(values),
                    reading: *reading,
                }])
            }
            // `!=` -- spargebra has no `NotEqual` variant (confirmed by
            // reading `spargebra::algebra::Expression`), so `From<&Expression>
            // for Expr` spells it `Not(Compare { op: Eq, .. })` the same way
            // `sparql_scoper::extract_equality_from_expr`'s `Expression::Not`
            // arm reads `Expression::Not(Expression::Equal(..))`. Without this
            // arm `CompareOp::Ne` is dead: nothing else ever builds an
            // `Expr::Compare { op: Ne, .. }` from a parsed query, only from a
            // test that constructs one by hand.
            Self::Not(inner) => match inner.as_ref() {
                Self::Compare {
                    op: CompareOp::Eq,
                    left,
                    right,
                } => {
                    let (slot, value) = slot_and_value(left, right)?;
                    let condition = CompareOp::Ne.as_condition(value)?;
                    let Self::Slot {
                        star_var,
                        slot_path,
                        reading,
                        ..
                    } = slot
                    else {
                        return None;
                    };
                    Some(vec![SqlCondition {
                        star_var: star_var.clone(),
                        slot_path: slot_path.clone(),
                        condition,
                        reading: *reading,
                    }])
                }
                // `!bound(?v)`, once `?v` has resolved to a slot. Sound only
                // under the same two preconditions the scoper's identical
                // lift applies (see `extract_equality_from_expr`'s
                // `Expression::Bound` arm):
                //
                // * **one hop.** A nested path's absence is "the key at this
                //   step is missing", a different predicate from "the leaf
                //   value is absent" -- no `FilterCondition` arm renders that,
                //   and the renderer only ever walks a multi-hop path with
                //   `->>`.
                // * **optional.** A `Required` slot's scan already enforces
                //   existence (`object_data ? 'field'`), so `!bound` on it is
                //   unsatisfiable and the plan has no way to state "selects
                //   nothing" as a condition.
                //
                // The scoper reads these off a query-syntax fact
                // (`optional_fields`, built from each triple's `OPTIONAL`
                // nesting depth). This route reads the same distinction off
                // `Expr::Slot::presence`, which travels with the slot itself
                // rather than needing a separate map -- see that field's doc
                // comment for why carrying it there, rather than widening
                // `to_sql`'s signature, was the right fix once the earlier
                // draft of this comment (which declined `!bound` altogether,
                // reasoning that the fact was not reachable here) turned out
                // to have the wrong premise: `Visible::collect` had the fact
                // in scope the whole time and was dropping it on the floor
                // building `SlotBinding`.
                //
                // A **third** scoper precondition -- "resolvable", i.e. the
                // variable names a slot at all -- has no separate check here:
                // it is what the `let [Self::Slot { .. }] = args.as_slice()`
                // pattern below already asks, since an unresolved variable is
                // still `Expr::Var` at this point and simply fails to match.
                //
                // A **fourth** thing the scoper computes explicitly --
                // "depth 0", i.e. whether the read sits inside an `OPTIONAL`
                // at all -- has no separate check here either, and not by
                // oversight: `SlotPresence` is not a per-triple depth count
                // to be compared against zero, it is the *destination* fact
                // depth-counting exists to produce -- the plan's own,
                // already-resolved answer to "does this scan's read of this
                // slot require the value or only allow it", set once by
                // whichever rule folded the `OPTIONAL` (or its absence) into
                // the scan. By the time a `ScanSlot` exists at all, its
                // `presence` already accounts for every `OPTIONAL` the query
                // wrote around it; there is no further depth for this
                // function to re-derive.
                Self::Function { name, args } if name == "BOUND" => {
                    let [
                        Self::Slot {
                            star_var,
                            slot_path,
                            reading,
                            presence,
                        },
                    ] = args.as_slice()
                    else {
                        return None;
                    };
                    if slot_path.len() != 1 || *presence != SlotPresence::Optional {
                        return None;
                    }
                    Some(vec![SqlCondition {
                        star_var: star_var.clone(),
                        slot_path: slot_path.clone(),
                        condition: FilterCondition::NotBound,
                        reading: *reading,
                    }])
                }
                // Every other negation -- `!CONTAINS`, `!(A && B)`, `!BOUND`
                // of anything but a resolved single-hop optional slot --
                // declines.
                _ => None,
            },
            // The three substring predicates, `STRSTARTS`/`STRENDS`/`CONTAINS`,
            // the same shape as `sparql_scoper::extract_equality_from_expr`
            // lifts for the fetch. `LCASE` on the haystack folds the column
            // for a case-insensitive match; `LCASE` on the needle is a
            // different question (it folds the constant, not the column) and
            // is not unwrapped -- `peel_lcase` only ever looks at the
            // haystack position.
            Self::Function { name, args } => {
                let anchor = like_anchor(name)?;
                let [haystack, needle] = args.as_slice() else {
                    return None;
                };
                let (haystack, case_insensitive) = peel_lcase(haystack);
                let Self::Slot {
                    star_var,
                    slot_path,
                    reading,
                    ..
                } = haystack
                else {
                    return None;
                };
                let Self::Literal(term) = needle else {
                    return None;
                };
                Some(vec![SqlCondition {
                    star_var: star_var.clone(),
                    slot_path: slot_path.clone(),
                    condition: FilterCondition::Like {
                        value: lexical(term),
                        anchor,
                        case_insensitive,
                    },
                    reading: *reading,
                }])
            }
            // A disjunction is not a conjunction of conditions, and there is
            // no rule that renders it as one. `Not` has its own arm above --
            // it is not always a decline, since `!=` is one -- so it does not
            // belong in this group.
            Self::Or(_) | Self::Var(_) | Self::Literal(_) | Self::Slot { .. } | Self::Opaque(_) => {
                None
            }
        }
    }

    /// The conditions this expression is worth in SQL, or `None` when SQL
    /// cannot ask the same question.
    ///
    /// The only public way to obtain a [`SqlCondition`], and it takes a schema
    /// because half the test needs one. 28d argues a pushed condition carries
    /// no correctness risk, since the engine leg re-runs the whole query and
    /// SQL therefore only ever *narrows*. That holds for a condition selecting
    /// a superset of the answer and fails for one selecting *nothing*: the
    /// engine then re-runs the query over no instances and reports an empty
    /// answer to a query that has one. Comparing stored text against a term
    /// the column never spells is exactly that case.
    ///
    /// `class_of_star` says which class each star variable was scanned as,
    /// which is what turns a slot path into a column whose stored form can be
    /// asked about. A star the map does not name declines: an address nobody
    /// can resolve is not a condition.
    pub fn to_sql(
        &self,
        schema: &SchemaView,
        class_of_star: &HashMap<String, String>,
    ) -> Option<Vec<SqlCondition>> {
        if !self.constants_are_the_columns_terms(schema, class_of_star) {
            return None;
        }
        self.sql_shape_unchecked()
    }

    /// The [`ConditionTree`] this expression is worth in SQL, or `None` when
    /// SQL cannot ask the same question.
    ///
    /// A **separate entry point** from [`Expr::to_sql`], and that is the
    /// design rather than an accident:
    ///
    /// * it returns `None` when there is no `Or` anywhere, so a pure
    ///   conjunction stays on the conjunctive fast path and **no shape in the
    ///   corpus changes route**. `filters` is read by the fetch narrowing and
    ///   by star-data as well as by the statement renderer; a conjunction that
    ///   started arriving as one opaque tree would need every one of those
    ///   consumers to learn the tree for nothing gained.
    /// * `sql_shape_unchecked`'s `Or` arm keeps declining for the same reason.
    ///
    /// It asks the *same first half* [`Expr::to_sql`] asks --
    /// [`Expr::constants_are_the_columns_terms`] -- before shaping anything,
    /// and the reason is sharper here than there. A pushed condition is free
    /// only while it selects a superset; one selecting *nothing* makes the
    /// engine re-run the query over no records and answer empty. In a
    /// disjunction a dead branch does not merely fail to narrow, it narrows to
    /// the *other* branch: `FILTER(?nm = "BX517" || ?kind = "GSA")` on an enum
    /// column that stores `GSA` and renders as `eul:GSA` would answer
    /// "the ones called BX517", which is a different question. So a tree shape
    /// this check does not walk is that bug reintroduced.
    ///
    /// Declines, each with what it prevents:
    ///
    /// * **any leaf declines** -- a disjunction is all-or-nothing, since
    ///   dropping one branch narrows the answer to the rest;
    /// * **the leaves name more than one star** -- see [`ConditionTree::star`];
    /// * **no `Or` anywhere** -- the fast path above;
    /// * **an empty `All` or `Any`** -- `Any([])` is `FALSE`, which selects
    ///   nothing. Not reachable from a parsed query (`flatten_or` builds at
    ///   least two branches), and refused rather than trusted to stay
    ///   unreachable.
    pub fn to_sql_tree(
        &self,
        schema: &SchemaView,
        class_of_star: &HashMap<String, String>,
    ) -> Option<ConditionTree> {
        if !self.constants_are_the_columns_terms(schema, class_of_star) {
            return None;
        }
        if !self.contains_disjunction() {
            return None;
        }
        let tree = self.tree_shape_unchecked(schema, class_of_star)?;
        tree.star()?;
        Some(tree)
    }

    /// Whether an `Or` appears anywhere in this expression.
    ///
    /// The gate that keeps the conjunctive fast path exactly as it was: only a
    /// shape that has no representation as a list of conjuncts takes the tree
    /// route. Walks the connectives only -- an `Or` buried inside a
    /// [`Expr::Function`]'s arguments is inside a shape that declines
    /// regardless, and inside an [`Expr::Opaque`] there is no expression to
    /// walk.
    fn contains_disjunction(&self) -> bool {
        match self {
            Self::Or(_) => true,
            Self::And(parts) => parts.iter().any(Self::contains_disjunction),
            Self::Not(inner) => inner.contains_disjunction(),
            Self::Compare { .. }
            | Self::In { .. }
            | Self::Var(_)
            | Self::Literal(_)
            | Self::Slot { .. }
            | Self::Function { .. }
            | Self::Opaque(_) => false,
        }
    }

    /// The tree this expression has the *shape* of, with the physical facts
    /// filled in.
    ///
    /// The counterpart of [`Expr::sql_shape_unchecked`], and it delegates to
    /// it at every leaf rather than re-deriving one: a leaf that could drift
    /// from what `to_sql` produces is a leaf a renderer would render
    /// differently from the same condition written without an `Or` around it.
    /// Every shape that is not a connective therefore has to produce **exactly
    /// one** condition to be a leaf -- an arm that produced several would be a
    /// hidden conjunction, and this declines it rather than guessing which
    /// connective it meant.
    ///
    /// Takes the schema for one fact only: `numeric`, which is a property of
    /// the column and not of the condition, resolved through the same
    /// `numeric_at_path` a single filter's lowering asks.
    fn tree_shape_unchecked(
        &self,
        schema: &SchemaView,
        class_of_star: &HashMap<String, String>,
    ) -> Option<ConditionTree> {
        let branches = |parts: &[Self]| -> Option<Vec<ConditionTree>> {
            if parts.is_empty() {
                // `Any([])` selects nothing; see `to_sql_tree`.
                return None;
            }
            parts
                .iter()
                .map(|part| part.tree_shape_unchecked(schema, class_of_star))
                .collect()
        };
        match self {
            Self::Or(parts) => Some(ConditionTree::Any(branches(parts)?)),
            Self::And(parts) => Some(ConditionTree::All(branches(parts)?)),
            // The one wildcard, and it is not a hole: everything it catches
            // is handed to `sql_shape_unchecked`, which *is* exhaustive over
            // `Expr`, so a new variant is a compile error there rather than a
            // leaf silently invented here.
            other => {
                let conditions = other.sql_shape_unchecked()?;
                let [condition] = <[SqlCondition; 1]>::try_from(conditions).ok()?;
                let numeric = class_of_star
                    .get(&condition.star_var)
                    .is_some_and(|class_uri| {
                        crate::sparql_scoper::numeric_at_path(
                            schema,
                            class_uri,
                            &condition.slot_path,
                        )
                    });
                Some(ConditionTree::Leaf(ConditionLeaf { condition, numeric }))
            }
        }
    }

    /// Whether every constant this expression compares against a slot is the
    /// term that slot's values render as.
    ///
    /// Only the shapes [`Expr::sql_shape_unchecked`] turns into conditions are
    /// checked; the rest it declines on its own, and declining twice for two
    /// reasons is not a stronger claim.
    fn constants_are_the_columns_terms(
        &self,
        schema: &SchemaView,
        class_of_star: &HashMap<String, String>,
    ) -> bool {
        let comparable = |star_var: &String, slot_path: &[String], term: &Term| {
            class_of_star.get(star_var).is_some_and(|class_uri| {
                constant_is_the_columns_term(schema, class_uri, slot_path, term)
            })
        };
        match self {
            Self::Compare { left, right, .. } => match (left.as_ref(), right.as_ref()) {
                (
                    Self::Slot {
                        star_var,
                        slot_path,
                        ..
                    },
                    Self::Literal(term),
                )
                | (
                    Self::Literal(term),
                    Self::Slot {
                        star_var,
                        slot_path,
                        ..
                    },
                ) => comparable(star_var, slot_path, term),
                _ => true,
            },
            Self::In { value, candidates } => match value.as_ref() {
                Self::Slot {
                    star_var,
                    slot_path,
                    ..
                } => candidates.iter().all(|candidate| match candidate {
                    Self::Literal(term) => comparable(star_var, slot_path, term),
                    _ => true,
                }),
                _ => true,
            },
            Self::And(parts) | Self::Or(parts) => parts
                .iter()
                .all(|part| part.constants_are_the_columns_terms(schema, class_of_star)),
            // The substring predicates are the one `Function` shape
            // `sql_shape_unchecked` turns into a condition, so it is the one
            // walked here -- everything else declines there regardless of
            // what this reports. An enum column stores a code and translates
            // backwards through its meanings, so a substring of a *label*
            // matches no code: pushing `CONTAINS(?kind, "GS")` against an
            // enum column the way `literal_pushable` already gates `=` and
            // `IN` is exactly the check that stops the statement selecting
            // nothing instead of narrowing it. Reuses `literal_pushable`
            // through `comparable` -> `constant_is_the_columns_term`, the
            // same gate the scoper's identical lift applies (see
            // `extract_equality_from_expr`'s `Expression::FunctionCall` arm)
            // -- staying consistent rather than duplicating the rule.
            Self::Function { name, args } => match like_anchor(name) {
                None => true,
                Some(_) => match args.as_slice() {
                    [haystack, Self::Literal(term)] => match peel_lcase(haystack).0 {
                        Self::Slot {
                            star_var,
                            slot_path,
                            ..
                        } => comparable(star_var, slot_path, term),
                        _ => true,
                    },
                    _ => true,
                },
            },
            // `!=` is `Not(Compare { op: Eq, .. })` -- see
            // `sql_shape_unchecked`'s `Not` arm -- and the term check the
            // inner `Compare` runs does not read `op`, so recursing here asks
            // the right question for `!=` too: the enum-code gate on `= "x"`
            // and on `!= "x"` is the same gate. `!bound` is also `Not(...)`
            // (a negated `Function { name: "BOUND", .. }`), and it compares
            // against no constant at all, so recursing into the `Function`
            // arm above correctly reports `true` for it (`like_anchor`
            // returns `None` for `"BOUND"`) -- there is nothing here for a
            // term check to gate.
            Self::Not(inner) => inner.constants_are_the_columns_terms(schema, class_of_star),
            Self::Var(_) | Self::Literal(_) | Self::Slot { .. } | Self::Opaque(_) => true,
        }
    }
}

/// Whether a constant the query wrote is the same RDF term the value at this
/// path renders as.
///
/// The reason a schema reaches into an expression at all. An enum column
/// storing `GSA` whose values render as `eul:GSA` answers a pushed
/// `= 'http://ontorail.org/src/Eulynx/GSA'` with no rows, and `= 'GSA'` with
/// every row the query wanted excluded -- so the same test the star
/// decomposition applies, from the same function, gates a pushed constant
/// here.
///
/// Enum columns decline rather than translate. Selecting the codes that render
/// as the term is a *rewrite* of the condition, and the rule that translates
/// backwards is a rule of its own.
fn constant_is_the_columns_term(
    schema: &SchemaView,
    class_uri: &str,
    slot_path: &[String],
    term: &Term,
) -> bool {
    let form = crate::sparql_scoper::push_form_of_path(schema, class_uri, slot_path);
    match (&form, term) {
        (PushForm::Literal { .. }, Term::Literal(literal)) => literal_pushable(literal, &form),
        (PushForm::Iri, Term::NamedNode(_)) => true,
        _ => false,
    }
}

/// The [`LikeAnchor`] a SPARQL function name asks for, when it names one of
/// the three substring predicates.
///
/// The name is the SPARQL keyword, not a URI or a custom `Display` -- see
/// `From<&Expression> for Expr`, which builds `Expr::Function::name` as
/// `function.to_string()`, and `spargebra::algebra::Function`'s own
/// `Display`, which spells these `"STRSTARTS"` / `"STRENDS"` / `"CONTAINS"`.
/// Verified by printing a naive plan for `FILTER(CONTAINS(?nm, "x"))` rather
/// than assumed.
fn like_anchor(name: &str) -> Option<LikeAnchor> {
    match name {
        "STRSTARTS" => Some(LikeAnchor::Prefix),
        "STRENDS" => Some(LikeAnchor::Suffix),
        "CONTAINS" => Some(LikeAnchor::Anywhere),
        _ => None,
    }
}

/// Peels an `LCASE(...)` wrapper off a substring function's haystack
/// argument, reporting whether it was there.
///
/// Only the haystack position is ever passed here. `LCASE` on the *needle*
/// folds the constant rather than the column -- a different question, not a
/// case-insensitive spelling of the same one -- so it is never unwrapped, by
/// construction: nothing calls this on the needle.
fn peel_lcase(haystack: &Expr) -> (&Expr, bool) {
    match haystack {
        Expr::Function { name, args } if name == "LCASE" => match args.as_slice() {
            [only] => (only, true),
            // The wrong arity for `LCASE`: not a column either way, so the
            // caller's own `Expr::Slot` match fails and declines.
            _ => (haystack, false),
        },
        other => (other, false),
    }
}

/// One side a slot, the other a constant. Returns them in that order, or
/// `None` when the comparison is between two variables, two constants or two
/// slots -- none of which is a column against a value.
fn slot_and_value<'e>(left: &'e Expr, right: &'e Expr) -> Option<(&'e Expr, String)> {
    match (left, right) {
        (slot @ Expr::Slot { .. }, Expr::Literal(term)) => Some((slot, lexical(term))),
        // A reversed comparison would need its operator flipped, which the
        // caller has already turned into a condition. Left to the rule that
        // normalises the expression instead of being guessed at here.
        _ => None,
    }
}

/// The stored text of a term, for a caller outside this module.
///
/// Used by the identity fold, where a literal and an IRI carrying the same URI
/// are the same question -- see `FoldIdentityConstant`.
pub fn lexical_of(term: &Term) -> String {
    lexical(term)
}

/// The stored text of a term: a literal's lexical form, an IRI's string.
///
/// The SQL side compares against JSONB text, so the datatype and the language
/// tag are not part of the value -- which is exactly why a pushed comparison
/// only ever *narrows* while the engine re-applies SPARQL's term semantics.
fn lexical(term: &Term) -> String {
    match term {
        Term::NamedNode(node) => node.as_str().to_owned(),
        Term::Literal(literal) => literal.value().to_owned(),
        other => other.to_string(),
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(name) => write!(f, "?{name}"),
            Self::Literal(term) => write!(f, "{term}"),
            Self::Slot {
                star_var,
                slot_path,
                reading,
                ..
            } => write!(f, "?{star_var}.{}{reading}", slot_path.join(".")),
            Self::Compare { op, left, right } => {
                write!(f, "({left} {} {right})", op.as_str())
            }
            Self::In { value, candidates } => write!(
                f,
                "({value} IN ({}))",
                candidates
                    .iter()
                    .map(|candidate| candidate.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::And(parts) => write!(f, "({})", join_exprs(parts, " && ")),
            Self::Or(parts) => write!(f, "({})", join_exprs(parts, " || ")),
            Self::Not(inner) => write!(f, "!{inner}"),
            Self::Function { name, args } => write!(f, "{name}({})", join_exprs(args, ", ")),
            Self::Opaque(text) => f.write_str(text),
        }
    }
}

fn join_exprs(parts: &[Expr], separator: &str) -> String {
    parts
        .iter()
        .map(|part| part.to_string())
        .collect::<Vec<_>>()
        .join(separator)
}

impl From<&Expression> for Expr {
    /// Total: every expression spargebra can parse has a representation here,
    /// because a naive plan is the query and not a subset of it.
    fn from(expression: &Expression) -> Self {
        let binary = |op: CompareOp, left: &Expression, right: &Expression| Expr::Compare {
            op,
            left: Box::new(Expr::from(left)),
            right: Box::new(Expr::from(right)),
        };
        match expression {
            Expression::NamedNode(node) => Expr::Literal(node.clone().into()),
            Expression::Literal(literal) => Expr::Literal(literal.clone().into()),
            Expression::Variable(variable) => Expr::Var(variable.as_str().to_owned()),
            // Flattened, so a three-way conjunction is one node with three
            // parts rather than a nest a rule has to walk to find the
            // conjunct it can push.
            Expression::And(left, right) => Expr::And(flatten_and(left, right)),
            Expression::Or(left, right) => Expr::Or(flatten_or(left, right)),
            Expression::Equal(left, right) => binary(CompareOp::Eq, left, right),
            Expression::Greater(left, right) => binary(CompareOp::Gt, left, right),
            Expression::GreaterOrEqual(left, right) => binary(CompareOp::Gte, left, right),
            Expression::Less(left, right) => binary(CompareOp::Lt, left, right),
            Expression::LessOrEqual(left, right) => binary(CompareOp::Lte, left, right),
            Expression::In(value, candidates) => Expr::In {
                value: Box::new(Expr::from(value.as_ref())),
                candidates: candidates.iter().map(Expr::from).collect(),
            },
            Expression::Not(inner) => Expr::Not(Box::new(Expr::from(inner.as_ref()))),
            // `sameTerm` is not `=`: it compares terms rather than values, so
            // it is a function and not a `Compare` a rule might push as an
            // equality.
            Expression::SameTerm(left, right) => Expr::Function {
                name: "sameTerm".to_owned(),
                args: vec![Expr::from(left.as_ref()), Expr::from(right.as_ref())],
            },
            Expression::Add(left, right) => arithmetic("+", left, right),
            Expression::Subtract(left, right) => arithmetic("-", left, right),
            Expression::Multiply(left, right) => arithmetic("*", left, right),
            Expression::Divide(left, right) => arithmetic("/", left, right),
            Expression::UnaryPlus(inner) => Expr::Function {
                name: "+".to_owned(),
                args: vec![Expr::from(inner.as_ref())],
            },
            Expression::UnaryMinus(inner) => Expr::Function {
                name: "-".to_owned(),
                args: vec![Expr::from(inner.as_ref())],
            },
            Expression::Bound(variable) => Expr::Function {
                name: "BOUND".to_owned(),
                args: vec![Expr::Var(variable.as_str().to_owned())],
            },
            Expression::If(condition, then, otherwise) => Expr::Function {
                name: "IF".to_owned(),
                args: vec![
                    Expr::from(condition.as_ref()),
                    Expr::from(then.as_ref()),
                    Expr::from(otherwise.as_ref()),
                ],
            },
            Expression::Coalesce(args) => Expr::Function {
                name: "COALESCE".to_owned(),
                args: args.iter().map(Expr::from).collect(),
            },
            Expression::FunctionCall(function, args) => Expr::Function {
                name: function.to_string(),
                args: args.iter().map(Expr::from).collect(),
            },
            Expression::Exists(pattern) => Expr::Opaque(format!("EXISTS {{ {pattern} }}")),
        }
    }
}

fn arithmetic(name: &str, left: &Expression, right: &Expression) -> Expr {
    Expr::Function {
        name: name.to_owned(),
        args: vec![Expr::from(left), Expr::from(right)],
    }
}

fn flatten_and(left: &Expression, right: &Expression) -> Vec<Expr> {
    let mut out = Vec::new();
    for side in [left, right] {
        match Expr::from(side) {
            Expr::And(parts) => out.extend(parts),
            other => out.push(other),
        }
    }
    out
}

fn flatten_or(left: &Expression, right: &Expression) -> Vec<Expr> {
    let mut out = Vec::new();
    for side in [left, right] {
        match Expr::from(side) {
            Expr::Or(parts) => out.extend(parts),
            other => out.push(other),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// One aggregate of a grouping, under the variable spargebra bound it to.
///
/// That variable is the internal one for `(COUNT(*) AS ?n)` -- the name the
/// author wrote is on the [`PlanOp::Bind`] one level up, which a naive plan
/// keeps as a node, so nothing is lost by not resolving it here.
#[derive(Debug, Clone)]
pub struct Measure {
    pub var: String,
    pub aggregate: AggregateExpression,
}

/// One `ORDER BY` term.
#[derive(Debug, Clone)]
pub struct SortTerm {
    pub expr: Expr,
    pub desc: bool,
}

/// One value a [`PlanOp::Scan`] reads, and whether reading it fans out.
///
/// `multivalued` is the field the fold rule's precondition rests on. 28d
/// writes the scan as `requires [hasName, hasTrafficKind]` and observes that
/// no invariant can catch a fold that drops the unnest; with multiplicity on
/// the slot, [`Plan::fanout_restored`] can.
///
/// A *path* and not a name, because a value inside an inlined structure is
/// still a value the SQL side can read: `?s :location ?loc . ?loc :longitude
/// ?lon` reads `["location", "longitude"]`, which is what the star
/// decomposition calls a `PathFilter` and renders by walking into the JSON.
/// One name is the common case and a one-element path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanSlot {
    pub path: Vec<String>,
    /// The variable the folded `match` bound to this slot's value, when the
    /// read binds one.
    ///
    /// `None` for a read that constrains the value without naming it: `?s
    /// :name "BX517"` asserts that the slot exists *and* what it holds, and
    /// the existence half is a slot the scan reads while the value half is a
    /// condition above it. Today's star says the same thing by listing the
    /// name in `required_fields` with no entry in `slot_variables`.
    ///
    /// A slot with no variable binds nothing, so nothing above the scan can
    /// read it: no condition resolves through it, no join keys on it, and a
    /// multivalued one owes no unnest -- there is no binding whose
    /// multiplicity could be lost.
    pub var: Option<String>,
    /// Whether the slot holds an array, so one record answers the query once
    /// per value.
    pub multivalued: bool,
    pub presence: SlotPresence,
}

/// Whether a scan *requires* the value it reads, or only allows it.
///
/// The distinction 28d did not have, and the reason the ledger and the plan
/// could disagree about an `OPTIONAL`. Today's star decomposition carries it
/// as `required_fields` versus `optional_fields`, and it decides one thing in
/// the SQL: whether an `object_data ? 'slot'` existence check is emitted.
///
/// **Orthogonal to whether the read binds a variable**, which is
/// [`ScanSlot::var`]. The two facts together are the whole of what a scan does
/// with a value, and keeping them apart is what let the optional read stop
/// being a special case:
///
/// | presence | `var` | what it is |
/// |---|---|---|
/// | `Required` | `Some` | a mandatory read: no value, no row |
/// | `Required` | `None` | the existence half of `?s :name "BX"` |
/// | `Optional` | `None` | delivered for the engine's benefit, read by nothing in SQL |
/// | `Optional` | `Some` | an *absorbed* optional read: a nullable column the SQL side binds |
///
/// The last row is what makes a missing-value bucket possible, and it is also
/// what makes the claim honest. A column delivered but bound by nothing does
/// not produce the solution an `OPTIONAL` asks for -- the engine does, and a
/// scan claiming it would be a node saying it did something it did not. A
/// nullable column the SQL side *binds* produces exactly those solutions: the
/// value where there is one, and no binding where there is not. So the claim
/// follows the binding rather than the existence check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotPresence {
    /// `object_data ? 'slot'` (and not JSON `null`): a record without a value
    /// is not a row.
    Required,
    /// No existence check, so a record without a value is still a row and the
    /// column reads as `NULL` there.
    Optional,
}

/// The reference a pushed join joins on, recorded rather than re-derived.
///
/// The same edge as [`crate::sparql_scoper::JoinEdge`], whose `left` is the
/// referenced star, `right` the star holding the foreign key and `right_slot`
/// the slot holding it -- named here for what each is, because "left" and
/// "right" also name the sides of the plan node and the two need not line up.
///
/// A rule could leave this out: `on` plus the scans below determine it, which
/// is the derivation the rule performs. Recording it means a consumer does not
/// repeat that derivation -- and a derivation performed twice is where a
/// renderer comes to disagree with the plan it is rendering. It also gives
/// [`Plan::reference_joins_agree`] something to check, so a rule that records
/// the wrong direction fails at the rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceEdge {
    /// The star whose records are referenced: its identifier is the value the
    /// foreign key holds.
    pub referenced: String,
    /// The star holding the foreign key.
    pub holder: String,
    /// The slot on `holder` whose value is `referenced`'s identifier.
    pub slot: String,
}

/// One step of a plan.
///
/// Inputs are node indices, so the tree is a flat vector and a rewrite is an
/// edit to it. Nothing here is nested, which is what lets a rule insert or
/// drop a node and have [`Plan::well_formed`] check the result.
#[derive(Debug, Clone)]
pub enum PlanOp {
    /// The single empty solution: what an empty group graph pattern matches,
    /// and the identity a join over nothing needs.
    Unit,
    /// One triple pattern, matched against the graph.
    ///
    /// There is no scan in a naive plan -- a scan is already a pushdown
    /// decision, so naively each triple pattern is a match, and the joins
    /// between them are nodes too.
    Match {
        pattern: Box<TriplePattern>,
    },
    /// A property path. Engine-only, and no rule pushes one.
    Path {
        subject: TermPattern,
        path: Box<PropertyPathExpression>,
        object: TermPattern,
    },
    /// An inline table.
    Values {
        variables: Vec<Variable>,
        rows: Vec<Vec<Option<GroundTerm>>>,
    },
    /// A natural join. `on` is the variables both sides bind, which is what
    /// the join is on in SPARQL.
    Join {
        left: NodeId,
        right: NodeId,
        on: Vec<String>,
        /// The reference edge this join joins on, once a rule has pushed it.
        ///
        /// `None` in a naive plan and on any join no rule pushed: a natural
        /// join on a shared variable is not necessarily a reference, and
        /// recording one that is not is what
        /// [`Plan::reference_joins_agree`] refuses.
        reference: Option<ReferenceEdge>,
    },
    /// `OPTIONAL`, as an operator rather than a plan-level node.
    LeftJoin {
        left: NodeId,
        right: NodeId,
        /// The reference edge this join joins on, once a rule has pushed it.
        ///
        /// Same field and same reason as [`PlanOp::Join`]'s: the direction is
        /// recorded rather than re-derived, and
        /// [`Plan::reference_joins_agree`] checks it against the scans.
        reference: Option<ReferenceEdge>,
        /// The condition spargebra lifts out of `OPTIONAL { ... FILTER(x) }`.
        ///
        /// Claimed by this node, per conjunct: nothing pushes it (it decides
        /// whether the optional side matched, so applying it to the fetch
        /// drops the rows the join exists to keep), and an obligation nobody
        /// claims is what makes losing it visible.
        condition: Option<Expr>,
    },
    /// `FILTER NOT EXISTS { ... }`: rows of `left` with no solution in
    /// `right`.
    ///
    /// A node rather than an opaque condition above a left join, because the
    /// negation is the whole question and a plan that cannot see it cannot
    /// push it. It binds `left`'s variables only — `NOT EXISTS` tests, it does
    /// not bind — and it claims the filter obligation the block raised.
    ///
    /// **Why this may narrow the fetch, where a positive `EXISTS` may not.**
    /// The engine re-runs the whole query over the fetched instances. Pushing
    /// this leaves the fetch holding exactly the rows with no match, and no
    /// record of the right side at all — over which `NOT EXISTS` holds
    /// vacuously, which is the same answer the anti-join computed. A pushed
    /// `EXISTS` would leave the fetch holding rows whose matches are *not*
    /// fetched, over which `EXISTS` is vacuously false: every row would be
    /// dropped. So the negation pushes and the positive one does not.
    AntiJoin {
        left: NodeId,
        right: NodeId,
        /// The reference edge the negation correlates on, once a rule has
        /// pushed it. Same field and same reason as [`PlanOp::LeftJoin`]'s.
        reference: Option<ReferenceEdge>,
    },
    Union {
        left: NodeId,
        right: NodeId,
    },
    Minus {
        left: NodeId,
        right: NodeId,
    },
    Filter {
        input: NodeId,
        condition: Expr,
    },
    /// `BIND`, and the `Extend` spargebra uses to alias an aggregate.
    Bind {
        input: NodeId,
        var: String,
        expr: Expr,
    },
    Group {
        input: NodeId,
        keys: Vec<String>,
        measures: Vec<Measure>,
        /// Conditions on the grouped rows: SPARQL's `HAVING`, as SQL's.
        ///
        /// Empty until the grouping rule pushes the filters above the grouping
        /// into it, which it may do only because SQL says the same thing with
        /// a `HAVING` clause -- a filter over aggregates is not an operator
        /// above a grouping in SQL, it is part of it. Each condition names a
        /// key or a measure; which column that is, and whether the constant it
        /// compares against is one SQL may compare against that column, is the
        /// lowering's question.
        having: Vec<Expr>,
    },
    Sort {
        input: NodeId,
        terms: Vec<SortTerm>,
    },
    Distinct {
        input: NodeId,
    },
    /// `REDUCED`. Discharges nothing: the query may drop duplicates or keep
    /// them, so there is no obligation to enumerate.
    Reduced {
        input: NodeId,
    },
    Slice {
        input: NodeId,
        limit: Option<usize>,
        offset: usize,
    },
    /// The variables the query asked for, in `SELECT` order.
    Project {
        input: NodeId,
        vars: Vec<String>,
    },
    /// A sub-`SELECT`: a projection that is not the query's own, and therefore
    /// a barrier the variables above it cannot see through.
    SubSelect {
        input: NodeId,
        vars: Vec<String>,
    },
    Graph {
        input: NodeId,
        name: String,
    },
    Service {
        input: NodeId,
        name: String,
        silent: bool,
    },
    /// Rows of one class, with the existence checks the folded matches imply.
    ///
    /// Build one with [`scan_with_fanout`] rather than by hand: a scan yields
    /// one row per record, and a multivalued slot needs its [`PlanOp::Unnest`]
    /// in the same edit.
    Scan {
        star_var: String,
        class_uri: String,
        slots: Vec<ScanSlot>,
        /// Values the query fixed the record's *identity* to.
        ///
        /// Not a slot and not a JSONB path: the identifier is the record's own
        /// URI, which lives in an indexed column, and today's star
        /// decomposition carries it apart from `filters` for exactly that
        /// reason. A scan with one of these reads one record; a scan without
        /// reads the class.
        ///
        /// The writer emits *no triple* for the identifier -- it is the
        /// subject IRI -- so a constant here narrows the fetch and is never an
        /// answer on its own. `LoweringRefusal::IdentityIsNotATriple` is what
        /// keeps that true.
        identifier_values: Vec<String>,
    },
    /// One row per element of a multivalued slot, so row count matches
    /// solution count. Without it a record with three values counts once.
    Unnest {
        input: NodeId,
        star_var: String,
        slot_path: Vec<String>,
        /// The variable this element binds -- the one the folded `match` read
        /// the slot into.
        ///
        /// Without it, a condition above the unnest can only name the array,
        /// and `(?s, [trafficKinds])` then means the column to one reader and
        /// an element to another. With it, [`SlotReading::BoundElement`] has
        /// something to refer to and [`Plan::fanout_restored`] can check that
        /// the unnest restoring a slot's fan-out is the one binding that
        /// slot's variable.
        var: String,
        /// Whether the row survives a collection with no elements.
        ///
        /// [`SlotPresence::Required`] is a `CROSS JOIN LATERAL`: a record with
        /// an empty array supplies no rows, which is what a required read
        /// means. [`SlotPresence::Optional`] is a `LEFT JOIN LATERAL`: the
        /// record answers once with the variable unbound, which is what an
        /// `OPTIONAL` over a collection means -- and getting it wrong is a
        /// smaller count with nothing to say a record was dropped.
        ///
        /// Stated on the fan-out as well as on the slot it fans out, for the
        /// reason `reading` and `optional_side` are stated on a filter: a fact
        /// a renderer has to fetch from another node is a fact it can forget
        /// to fetch. The lowering refuses a plan where the two disagree.
        presence: SlotPresence,
    },
    /// Solutions to triples.
    Construct {
        input: NodeId,
        template: Vec<TriplePattern>,
    },
    /// The triples *about* the resources a solution names. The expansion rule
    /// is a separate question from the plan shape, so the node carries the
    /// variables and nothing else.
    Describe {
        input: NodeId,
        vars: Vec<String>,
    },
    /// Solutions to a boolean.
    Ask {
        input: NodeId,
    },
}

impl PlanOp {
    /// The inputs this node consumes, for a walk that does not match on the
    /// variant.
    pub fn inputs(&self) -> Vec<NodeId> {
        match self {
            Self::Unit | Self::Match { .. } | Self::Path { .. } | Self::Values { .. } => Vec::new(),
            Self::Scan { .. } => Vec::new(),
            Self::Join { left, right, .. }
            | Self::LeftJoin { left, right, .. }
            | Self::AntiJoin { left, right, .. }
            | Self::Union { left, right }
            | Self::Minus { left, right } => vec![*left, *right],
            Self::Filter { input, .. }
            | Self::Bind { input, .. }
            | Self::Group { input, .. }
            | Self::Sort { input, .. }
            | Self::Distinct { input }
            | Self::Reduced { input }
            | Self::Slice { input, .. }
            | Self::Project { input, .. }
            | Self::SubSelect { input, .. }
            | Self::Graph { input, .. }
            | Self::Service { input, .. }
            | Self::Unnest { input, .. }
            | Self::Construct { input, .. }
            | Self::Describe { input, .. }
            | Self::Ask { input } => vec![*input],
        }
    }

    /// Renumber this node's inputs. What a rule that drops or inserts a node
    /// has to do to every node above it, and the invariant
    /// [`Plan::well_formed`] exists to catch when it does not.
    pub fn map_inputs(&mut self, mut remap: impl FnMut(NodeId) -> NodeId) {
        match self {
            Self::Unit
            | Self::Match { .. }
            | Self::Path { .. }
            | Self::Values { .. }
            | Self::Scan { .. } => {}
            Self::Join { left, right, .. }
            | Self::LeftJoin { left, right, .. }
            | Self::AntiJoin { left, right, .. }
            | Self::Union { left, right }
            | Self::Minus { left, right } => {
                *left = remap(*left);
                *right = remap(*right);
            }
            Self::Filter { input, .. }
            | Self::Bind { input, .. }
            | Self::Group { input, .. }
            | Self::Sort { input, .. }
            | Self::Distinct { input }
            | Self::Reduced { input }
            | Self::Slice { input, .. }
            | Self::Project { input, .. }
            | Self::SubSelect { input, .. }
            | Self::Graph { input, .. }
            | Self::Service { input, .. }
            | Self::Unnest { input, .. }
            | Self::Construct { input, .. }
            | Self::Describe { input, .. }
            | Self::Ask { input } => *input = remap(*input),
        }
    }

    /// A short name, for a printout and for a rule that looks for its own
    /// shape.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Unit => "unit",
            Self::Match { .. } => "match",
            Self::Path { .. } => "path",
            Self::Values { .. } => "values",
            Self::Join { .. } => "join",
            Self::LeftJoin { .. } => "leftjoin",
            Self::AntiJoin { .. } => "antijoin",
            Self::Union { .. } => "union",
            Self::Minus { .. } => "minus",
            Self::Filter { .. } => "filter",
            Self::Bind { .. } => "bind",
            Self::Group { .. } => "group",
            Self::Sort { .. } => "sort",
            Self::Distinct { .. } => "distinct",
            Self::Reduced { .. } => "reduced",
            Self::Slice { .. } => "slice",
            Self::Project { .. } => "project",
            Self::SubSelect { .. } => "subselect",
            Self::Graph { .. } => "graph",
            Self::Service { .. } => "service",
            Self::Scan { .. } => "scan",
            Self::Unnest { .. } => "unnest",
            Self::Construct { .. } => "construct",
            Self::Describe { .. } => "describe",
            Self::Ask { .. } => "ask",
        }
    }

    /// Whether this operator's result depends on how many rows it is given.
    ///
    /// The question the fifth invariant asks of a fan-out: a `COUNT` counts
    /// rows, a `DISTINCT` removes duplicate ones, a `SLICE` takes the first
    /// few. Restoring multiplicity *after* one of these has already read the
    /// rows is a wrong answer -- a count of records where the query counts
    /// solutions -- so the unnest has to be below them.
    ///
    /// A `Sort` is not here: it reorders rows without changing how many there
    /// are, so a fan-out on either side of one produces the same multiset.
    pub fn collapses_rows(&self) -> bool {
        matches!(
            self,
            Self::Group { .. } | Self::Distinct { .. } | Self::Reduced { .. } | Self::Slice { .. }
        )
    }

    /// What this operator produces, which is a property of the operator and
    /// not a choice, so a node's [`Node::output`] cannot disagree with its op.
    pub fn output_kind(&self) -> OutputKind {
        match self {
            Self::Construct { .. } | Self::Describe { .. } => OutputKind::Triples,
            Self::Ask { .. } => OutputKind::Boolean,
            _ => OutputKind::Solutions,
        }
    }
}

/// One node with its bookkeeping.
#[derive(Debug, Clone)]
pub struct Node {
    pub op: PlanOp,
    pub executor: Executor,
    /// What this node produces. Redundant with
    /// [`PlanOp::output_kind`] and kept as a field because 28d asks every node
    /// to *declare* it; [`Plan::well_formed`] checks the two agree, so a hand
    /// built node cannot claim to emit triples from a filter.
    pub output: OutputKind,
    /// Obligations this node discharges.
    pub discharges: Vec<ObligationId>,
}

impl Node {
    /// An engine-executed node: what every node of a naive plan is.
    pub fn engine(op: PlanOp, discharges: Vec<ObligationId>) -> Self {
        Self {
            output: op.output_kind(),
            op,
            executor: Executor::Engine,
            discharges,
        }
    }

    /// An SQL-executed node, for a rule that has pushed one down.
    pub fn sql(op: PlanOp, discharges: Vec<ObligationId>) -> Self {
        Self {
            output: op.output_kind(),
            op,
            executor: Executor::Sql,
            discharges,
        }
    }
}

/// A scan and the unnests its multivalued slots demand, as one edit.
///
/// The precondition from 28d, made structural: a `match` on a multivalued slot
/// fans out -- one solution per value, which is what SPARQL means -- while a
/// scan yields one row per record. Folding one without its unnest silently
/// collapses multiplicity, so a record with three traffic kinds counts once
/// instead of three times. There is one way to build a scan for a plan, and it
/// is this function, which derives the unnests from the same slot list that
/// decides what the scan reads.
///
/// Returns the nodes bottom-up: the scan, then one unnest per multivalued
/// slot, each consuming the one before it. The caller appends them in order
/// and treats the last as the folded subtree's root.
pub fn scan_with_fanout(
    star_var: &str,
    class_uri: &str,
    slots: Vec<ScanSlot>,
    // `identifier_values`: the record's identity, when the query named it
    // rather than a variable. A constant subject scans one row, against the
    // indexed identifier column. Empty for a variable subject, whose identity
    // a rule folds later if the query constrains it.
    identifier_values: Vec<String>,
    at: NodeId,
    discharges: Vec<ObligationId>,
) -> Vec<Node> {
    // A slot that *binds* a variable owes a fan-out, whatever its presence: a
    // consumer counting solutions counts one per element. A delivered slot
    // exposes no binding above the scan, so nothing counts its values and
    // there is no multiplicity to restore -- fanning one out would multiply
    // rows for a read nobody reads.
    let fanning: Vec<(Vec<String>, String, SlotPresence)> = slots
        .iter()
        .filter(|slot| slot.multivalued)
        .filter_map(|slot| {
            slot.var
                .clone()
                .map(|var| (slot.path.clone(), var, slot.presence))
        })
        .collect();
    let mut out = vec![Node::sql(
        PlanOp::Scan {
            star_var: star_var.to_owned(),
            class_uri: class_uri.to_owned(),
            slots,
            identifier_values,
        },
        discharges,
    )];
    for (path, var, presence) in fanning {
        let input = at + out.len() - 1;
        out.push(Node::sql(
            PlanOp::Unnest {
                input,
                star_var: star_var.to_owned(),
                slot_path: path,
                var,
                presence,
            },
            Vec::new(),
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// The plan
// ---------------------------------------------------------------------------

/// A query as a tree of nodes, with the root last.
#[derive(Debug, Clone)]
pub struct Plan {
    pub form: QueryForm,
    pub obligations: Vec<Obligation>,
    pub nodes: Vec<Node>,
    /// Obligations no node discharges. Empty in a naive plan, which is why a
    /// naive plan is already an answer.
    pub residual: Vec<ObligationId>,
}

/// A plan that violates one of the invariants.
///
/// One variant per invariant, naming the nodes involved: "a rule broke
/// something" is not actionable, and the whole point of checking after every
/// application is to be told which rule and where.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanDefect {
    /// No nodes at all, so there is no root to match against the query form.
    Empty,
    Ledger(LedgerError),
    /// An input that does not precede its node -- what inserting a node
    /// without renumbering leaves behind, and what no type checks.
    IllFormed {
        node: NodeId,
        input: NodeId,
    },
    /// A node whose declared output kind is not what its operator produces.
    MislabelledOutput {
        node: NodeId,
    },
    /// An `Sql` node consuming an `Engine` node. SQL cannot read engine
    /// output, so the frontier has to be a cut.
    FrontierBreach {
        sql: NodeId,
        engine: NodeId,
    },
    /// The root produces something other than the query form.
    WrongOutput {
        form: QueryForm,
        root: OutputKind,
    },
    /// A scan folded a multivalued slot without the unnest that restores its
    /// multiplicity. Not one of 28d's four, and the only one that catches a
    /// *cardinality* error -- see the module docs.
    LostFanout {
        scan: NodeId,
        slot: String,
    },
    /// An `Unnest` for a slot no scan below it folded as multivalued -- the
    /// converse of `LostFanout`, and checkable only because the unnest names
    /// the variable it binds. See [`Plan::fanout_restored`].
    StrayFanout {
        unnest: NodeId,
    },
    /// A fan-out that happens after something already read the rows: a
    /// `COUNT`, a `DISTINCT` or a `SLICE` between the scan and its `Unnest`.
    /// The multiplicity is restored, and too late to be counted.
    FanoutAfterCollapse {
        unnest: NodeId,
        collapse: NodeId,
    },
    /// A join whose recorded [`ReferenceEdge`] is not what the scans below it
    /// say. Not one of 28d's, and the invariant that exists because the plan
    /// now records the direction rather than leaving it to be re-derived.
    MisrecordedJoin {
        join: NodeId,
    },
}

impl fmt::Display for PlanDefect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("plan has no nodes, so it has no root"),
            Self::Ledger(error) => write!(f, "{error}"),
            Self::IllFormed { node, input } => {
                write!(f, "node n{node} reads n{input}, which does not precede it")
            }
            Self::MislabelledOutput { node } => write!(
                f,
                "node n{node} declares an output kind its operator does not produce"
            ),
            Self::FrontierBreach { sql, engine } => write!(
                f,
                "n{sql} runs in SQL but reads n{engine}, which runs in the engine"
            ),
            Self::WrongOutput { form, root } => write!(
                f,
                "a {} query must root in a node producing {}, not {root}",
                form.as_str(),
                form.expects()
            ),
            Self::LostFanout { scan, slot } => write!(
                f,
                "n{scan} folded the multivalued slot '{slot}' without the unnest \
                 that binds its variable, so a record with several values counts once"
            ),
            Self::MisrecordedJoin { join } => write!(
                f,
                "n{join} records a reference edge the scans below it do not support"
            ),
            Self::FanoutAfterCollapse { unnest, collapse } => write!(
                f,
                "n{collapse} reads rows the fan-out at n{unnest} has not produced yet, \
                 so it counts records where the query counts solutions"
            ),
            Self::StrayFanout { unnest } => write!(
                f,
                "n{unnest} fans out a slot no scan below it read as multivalued, \
                 so it multiplies rows nothing asked for"
            ),
        }
    }
}

impl Plan {
    /// The root: the node every other node feeds.
    pub fn root(&self) -> Option<&Node> {
        self.nodes.last()
    }

    /// All six invariants, in the order a reader of 28d expects them: its four,
    /// the fan-out one stage 1 added, and the join-agreement one stage 2 added
    /// with [`ReferenceEdge`].
    ///
    /// Cheap -- linear in the plan -- which is what makes checking after every
    /// single rule application affordable, and the difference between a rule
    /// chain that can be trusted and one that cannot.
    pub fn check(&self) -> Result<(), PlanDefect> {
        self.ledger_balances().map_err(PlanDefect::Ledger)?;
        self.well_formed()?;
        self.frontier_is_a_cut()?;
        self.root_matches_form()?;
        self.fanout_restored()?;
        self.reference_joins_agree()
    }

    /// **Invariant 1.** Every obligation appears exactly once, in a node or in
    /// the residual.
    pub fn ledger_balances(&self) -> Result<(), LedgerError> {
        let mut seen: Vec<usize> = vec![0; self.obligations.len()];
        for id in self
            .nodes
            .iter()
            .flat_map(|node| node.discharges.iter())
            .chain(self.residual.iter())
        {
            if let Some(slot) = seen.get_mut(*id) {
                *slot += 1;
            }
        }
        let missing = seen
            .iter()
            .enumerate()
            .filter(|(_, count)| **count == 0)
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        let duplicated = seen
            .iter()
            .enumerate()
            .filter(|(_, count)| **count > 1)
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        if missing.is_empty() && duplicated.is_empty() {
            Ok(())
        } else {
            Err(LedgerError {
                missing,
                duplicated,
            })
        }
    }

    /// **Invariant 2.** Every node's inputs exist and precede it, and every
    /// node's declared output kind is the one its operator produces.
    pub fn well_formed(&self) -> Result<(), PlanDefect> {
        for (index, node) in self.nodes.iter().enumerate() {
            for input in node.op.inputs() {
                if input >= index {
                    return Err(PlanDefect::IllFormed { node: index, input });
                }
            }
            if node.output != node.op.output_kind() {
                return Err(PlanDefect::MislabelledOutput { node: index });
            }
        }
        Ok(())
    }

    /// **Invariant 3.** No `Sql` node above an `Engine` node.
    ///
    /// Checking direct inputs is enough for the transitive claim: if an `Sql`
    /// node reads an `Sql` node that reads an `Engine` one, the breach is
    /// reported at the lower pair.
    pub fn frontier_is_a_cut(&self) -> Result<(), PlanDefect> {
        for (index, node) in self.nodes.iter().enumerate() {
            if node.executor != Executor::Sql {
                continue;
            }
            for input in node.op.inputs() {
                if self
                    .nodes
                    .get(input)
                    .is_some_and(|below| below.executor == Executor::Engine)
                {
                    return Err(PlanDefect::FrontierBreach {
                        sql: index,
                        engine: input,
                    });
                }
            }
        }
        Ok(())
    }

    /// **Invariant 4.** The root produces what the query form produces.
    pub fn root_matches_form(&self) -> Result<(), PlanDefect> {
        let Some(root) = self.root() else {
            return Err(PlanDefect::Empty);
        };
        if root.output == self.form.expects() {
            Ok(())
        } else {
            Err(PlanDefect::WrongOutput {
                form: self.form,
                root: root.output,
            })
        }
    }

    /// **Invariant 5.** Every multivalued slot a scan folded has its unnest
    /// above it.
    ///
    /// The one check that sees a cardinality error. The ledger cannot: the
    /// obligations are all still claimed exactly once, by the right nodes.
    pub fn fanout_restored(&self) -> Result<(), PlanDefect> {
        for (scan_id, node) in self.nodes.iter().enumerate() {
            let PlanOp::Scan {
                star_var, slots, ..
            } = &node.op
            else {
                continue;
            };
            // Any read that *binds* a multivalued slot owes a fan-out,
            // whatever its presence. An optional one needs the fan-out inside
            // the optional side, which is a `LEFT JOIN LATERAL` -- the
            // presence on the [`PlanOp::Unnest`] is what says so.
            for slot in slots
                .iter()
                .filter(|slot| slot.multivalued && slot.var.is_some())
            {
                let restored = self.nodes.iter().enumerate().find_map(|(id, above)| {
                    let matches = matches!(
                        &above.op,
                        PlanOp::Unnest {
                            star_var: unnest_star,
                            slot_path,
                            var,
                            ..
                        } if unnest_star == star_var
                            && slot_path == &slot.path
                            // The variable too, and not only the path. A
                            // condition above the unnest addresses the element
                            // *through* the name it bound
                            // ([`SlotReading::BoundElement`]), so an unnest
                            // that fans out the right slot under the wrong
                            // name leaves that condition naming an element
                            // nothing bound.
                            && Some(var) == slot.var.as_ref()
                    ) && self.feeds(scan_id, id);
                    matches.then_some(id)
                });
                let Some(unnest) = restored else {
                    return Err(PlanDefect::LostFanout {
                        scan: scan_id,
                        slot: slot.path.join("."),
                    });
                };
                // Restored, and restored *in time*. A fan-out below a scan
                // says nothing about where it sits relative to a `COUNT`: an
                // unnest above the grouping would count records where the
                // query counts solutions, and every other check would pass.
                //
                // Stated only once a plan could group. Written for stage 1 it
                // would have been unfalsifiable -- nothing collapsed rows --
                // and this is the shape the invariant was always meant to
                // catch.
                if let Some(collapse) = self.nodes.iter().enumerate().position(|(id, node)| {
                    node.op.collapses_rows() && self.feeds(scan_id, id) && !self.feeds(unnest, id)
                }) {
                    return Err(PlanDefect::FanoutAfterCollapse { unnest, collapse });
                }
            }
        }

        // The converse, which the invariant needs now that a reading refers to
        // an unnest: an unnest nothing folded multiplies rows by an array no
        // row set has, and a `BoundElement` condition above it would name an
        // element of a slot the scan never read.
        for (id, node) in self.nodes.iter().enumerate() {
            let PlanOp::Unnest {
                star_var,
                slot_path,
                var,
                ..
            } = &node.op
            else {
                continue;
            };
            let folded = self.nodes.iter().enumerate().any(|(scan_id, below)| {
                matches!(
                    &below.op,
                    PlanOp::Scan {
                        star_var: scan_star,
                        slots,
                        ..
                    } if scan_star == star_var
                        && slots.iter().any(|slot| {
                            slot.multivalued
                                && slot_path == &slot.path
                                && slot.var.as_ref() == Some(var)
                        })
                ) && self.feeds(scan_id, id)
            });
            if !folded {
                return Err(PlanDefect::StrayFanout { unnest: id });
            }
        }
        Ok(())
    }

    /// **Invariant 6.** A recorded reference edge says what the scans below
    /// say.
    ///
    /// The direction is a fact about the schema, so no invariant can tell that
    /// a *slot* is really a foreign key -- that is the pushing rule's job,
    /// asked of the same `SlotInlineMode::Reference` the star decomposition
    /// uses. What is checkable, and what would otherwise be a wrong join in a
    /// plan every invariant passed, is *agreement*: the referenced star is
    /// scanned on one side, the holder on the other, the recorded slot is the
    /// one that scan bound to the joined variable, and the join is on that
    /// variable and nothing else.
    pub fn reference_joins_agree(&self) -> Result<(), PlanDefect> {
        for (id, node) in self.nodes.iter().enumerate() {
            // Both join kinds: a left join records the same edge, and a
            // reversed one is the same wrong answer with the rows the
            // `OPTIONAL` keeps thrown in.
            let (left, right, on, edge) = match &node.op {
                PlanOp::Join {
                    left,
                    right,
                    on,
                    reference: Some(edge),
                } => (left, right, Some(on), edge),
                PlanOp::LeftJoin {
                    left,
                    right,
                    reference: Some(edge),
                    ..
                } => (left, right, None, edge),
                _ => continue,
            };
            let scanned_on = |side: NodeId, star: &str| -> Option<&Vec<ScanSlot>> {
                self.nodes
                    .iter()
                    .enumerate()
                    .find_map(|(scan, below)| match &below.op {
                        PlanOp::Scan {
                            star_var, slots, ..
                        } if star_var == star && self.feeds(scan, side) => Some(slots),
                        _ => None,
                    })
            };
            let holds_the_key = |side: NodeId| {
                scanned_on(side, &edge.holder).is_some_and(|slots| {
                    slots
                        .iter()
                        // A one-element path: a foreign key is a column of
                        // the record, and `JoinEdge::right_slot` is one name.
                        // A reference *inside* an inlined structure is not an
                        // edge this vocabulary can express.
                        .any(|slot| {
                            slot.path.as_slice() == [edge.slot.clone()]
                                && slot.var.as_deref() == Some(edge.referenced.as_str())
                                // A delivered read is not a binding, so it
                                // cannot be the key a join reads.
                                && slot.presence == SlotPresence::Required
                                // A *collection* of identifiers is not a
                                // foreign key an equality can compare: the
                                // renderer would emit
                                // `object_data->>'slot' = uri`, compare the
                                // array's text, and match nothing. No rule
                                // pushes one — `foreign_key_on` filters
                                // `!multivalued` — and stating it here makes
                                // that a property of the plan rather than a
                                // promise made elsewhere, which is what lets
                                // the lowering record
                                // `right_multivalued: false` without asking
                                // the schema again.
                                && !slot.multivalued
                        })
                })
            };
            // A left join has no `on`: the variables the two sides share are
            // the query's business, and what the renderer needs is the edge.
            let joins_on_the_edge = on.is_none_or(|on| on.as_slice() == [edge.referenced.clone()]);
            let agrees = joins_on_the_edge
                && ((scanned_on(*left, &edge.referenced).is_some() && holds_the_key(*right))
                    || (scanned_on(*right, &edge.referenced).is_some() && holds_the_key(*left)));
            if !agrees {
                return Err(PlanDefect::MisrecordedJoin { join: id });
            }
        }
        Ok(())
    }

    /// The variables a node's solutions bind.
    ///
    /// Derived from the nodes rather than remembered from the query, because a
    /// rule changes it: folding a match into a scan moves a binding from one
    /// node to another, and collapsing a join removes the node that recorded
    /// which variables the two sides shared. See
    /// [`crate::sparql_rules::refresh_join_variables`].
    pub fn variables_of(&self, node: NodeId) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        let Some(node) = self.nodes.get(node) else {
            return out;
        };
        match &node.op {
            PlanOp::Scan {
                star_var, slots, ..
            } => {
                out.insert(star_var.clone());
                // A delivered read binds nothing *here*: the `match` that
                // stayed above is what binds it, and counting it twice would
                // put an optional variable in a mandatory join's `on`.
                out.extend(
                    slots
                        .iter()
                        .filter(|slot| slot.presence == SlotPresence::Required)
                        .filter_map(|slot| slot.var.clone()),
                );
            }
            PlanOp::Match { pattern } => {
                add_term_var(&pattern.subject, &mut out);
                if let NamedNodePattern::Variable(variable) = &pattern.predicate {
                    out.insert(variable.as_str().to_owned());
                }
                add_term_var(&pattern.object, &mut out);
            }
            PlanOp::Path {
                subject, object, ..
            } => {
                add_term_var(subject, &mut out);
                add_term_var(object, &mut out);
            }
            PlanOp::Values { variables, .. } => {
                out.extend(variables.iter().map(|v| v.as_str().to_owned()));
            }
            PlanOp::Group { keys, measures, .. } => {
                out.extend(keys.iter().cloned());
                out.extend(measures.iter().map(|measure| measure.var.clone()));
            }
            PlanOp::Project { vars, .. }
            | PlanOp::SubSelect { vars, .. }
            | PlanOp::Describe { vars, .. } => out.extend(vars.iter().cloned()),
            PlanOp::Bind { input, var, .. } => {
                out.extend(self.variables_of(*input));
                out.insert(var.clone());
            }
            PlanOp::Minus { left, .. } => out.extend(self.variables_of(*left)),
            other => {
                for input in other.inputs() {
                    out.extend(self.variables_of(input));
                }
            }
        }
        out
    }

    /// Whether `lower`'s rows reach `upper`, following inputs.
    pub fn feeds(&self, lower: NodeId, upper: NodeId) -> bool {
        if lower == upper {
            return true;
        }
        let Some(node) = self.nodes.get(upper) else {
            return false;
        };
        node.op
            .inputs()
            .into_iter()
            .any(|input| self.feeds(lower, input))
    }

    /// Nodes of one kind, for a rule that looks for its own shape.
    pub fn find(&self, kind: &str) -> Vec<NodeId> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.op.kind() == kind)
            .map(|(id, _)| id)
            .collect()
    }
}

/// Human-readable, and complete: a node or an obligation that is not in this
/// string is not in the plan.
///
/// No frontier line, deliberately. 28d's printouts draw one because its
/// worked examples happen to put every `Sql` node below every `Engine` one,
/// but the frontier is a *cut* and not a prefix: two stars, one folded and one
/// not, interleave `[S]` and `[E]` in index order and no single line separates
/// them. The per-node tag says the same thing without lying about the shape.
impl fmt::Display for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Plan ({} → {})", self.form.as_str(), self.form.expects())?;
        for (id, node) in self.nodes.iter().enumerate() {
            let claims = if node.discharges.is_empty() {
                String::new()
            } else {
                format!(
                    "  claims {}",
                    node.discharges
                        .iter()
                        .map(|id| format!("o{id}"))
                        .collect::<Vec<_>>()
                        .join(" ")
                )
            };
            writeln!(
                f,
                "  n{id:<3} {:<9} {:<44} {}{claims}",
                node.op.kind(),
                node.op.describe(),
                node.executor.tag()
            )?;
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

impl PlanOp {
    /// The part of a node a reader needs beyond its kind.
    ///
    /// A method rather than a private helper of the printout because a rule --
    /// and a test asserting which nodes a rule pushed -- needs to say what a
    /// node *is* without re-deriving it from the operator. Two orderings of
    /// the same filters produce the same pushed nodes at different indices, so
    /// "the same plan" has to be stated in terms of node descriptions rather
    /// than of `n3`.
    pub fn describe(&self) -> String {
        match self {
            PlanOp::Unit => "{}".to_owned(),
            PlanOp::Match { pattern } => triple_text(pattern),
            PlanOp::Path {
                subject,
                path,
                object,
            } => format!("{subject} {path} {object}"),
            PlanOp::Values { variables, rows } => format!(
                "{} × {} row(s)",
                variables
                    .iter()
                    .map(|variable| variable.to_string())
                    .collect::<Vec<_>>()
                    .join(" "),
                rows.len()
            ),
            PlanOp::Join {
                left,
                right,
                on,
                reference,
            } => format!(
                "n{left}, n{right}  on {}{}",
                on.iter()
                    .map(|var| format!("?{var}"))
                    .collect::<Vec<_>>()
                    .join(" "),
                match reference {
                    Some(edge) => format!("  via ?{}.{}", edge.holder, edge.slot),
                    None => String::new(),
                }
            ),
            PlanOp::LeftJoin {
                left,
                right,
                reference,
                condition,
            } => {
                let edge = match reference {
                    Some(edge) => format!("  via ?{}.{}", edge.holder, edge.slot),
                    None => String::new(),
                };
                match condition {
                    Some(condition) => format!("n{left}, n{right}{edge}  if {condition}"),
                    None => format!("n{left}, n{right}{edge}"),
                }
            }
            PlanOp::AntiJoin {
                left,
                right,
                reference,
            } => {
                let edge = match reference {
                    Some(edge) => format!("  via ?{}.{}", edge.holder, edge.slot),
                    None => String::new(),
                };
                format!("n{left} without n{right}{edge}")
            }
            PlanOp::Union { left, right } | PlanOp::Minus { left, right } => {
                format!("n{left}, n{right}")
            }
            PlanOp::Filter { condition, .. } => condition.to_string(),
            PlanOp::Bind { var, expr, .. } => format!("?{var} ← {expr}"),
            PlanOp::Group {
                keys,
                measures,
                having,
                ..
            } => format!(
                "keys=[{}] measures=[{}]{}",
                keys.iter()
                    .map(|key| format!("?{key}"))
                    .collect::<Vec<_>>()
                    .join(" "),
                measures
                    .iter()
                    .map(|measure| format!("?{} ← {}", measure.var, measure.aggregate))
                    .collect::<Vec<_>>()
                    .join(", "),
                if having.is_empty() {
                    String::new()
                } else {
                    format!(
                        " having=[{}]",
                        having
                            .iter()
                            .map(|condition| condition.to_string())
                            .collect::<Vec<_>>()
                            .join(" && ")
                    )
                }
            ),
            PlanOp::Sort { terms, .. } => terms
                .iter()
                .map(|term| format!("{}{}", term.expr, if term.desc { " desc" } else { " asc" }))
                .collect::<Vec<_>>()
                .join(", "),
            PlanOp::Distinct { .. } | PlanOp::Reduced { .. } | PlanOp::Ask { .. } => String::new(),
            PlanOp::Slice { limit, offset, .. } => match limit {
                Some(limit) => format!("limit {limit} offset {offset}"),
                None => format!("offset {offset}"),
            },
            PlanOp::Project { vars, .. }
            | PlanOp::SubSelect { vars, .. }
            | PlanOp::Describe { vars, .. } => vars
                .iter()
                .map(|var| format!("?{var}"))
                .collect::<Vec<_>>()
                .join(" "),
            PlanOp::Graph { name, .. } => name.clone(),
            PlanOp::Service { name, silent, .. } => {
                format!("{name}{}", if *silent { " silent" } else { "" })
            }
            PlanOp::Scan {
                star_var,
                class_uri,
                slots,
                identifier_values,
            } => format!(
                "{} as ?{star_var}{}, requires [{}]",
                shorten(class_uri),
                if identifier_values.is_empty() {
                    String::new()
                } else {
                    format!(" is {}", identifier_values.join(", "))
                },
                slots
                    .iter()
                    .map(|slot| format!(
                        "{}{}{}{}",
                        slot.path.join("."),
                        match &slot.var {
                            Some(var) => format!("→?{var}"),
                            None => String::new(),
                        },
                        if slot.multivalued { "[]" } else { "" },
                        if slot.presence == SlotPresence::Optional {
                            "?"
                        } else {
                            ""
                        }
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            PlanOp::Unnest {
                star_var,
                slot_path,
                var,
                ..
            } => format!("?{star_var}.{} → ?{var}", slot_path.join(".")),
            PlanOp::Construct { template, .. } => format!("{} triple(s)", template.len()),
        }
    }
}

/// A triple pattern as a plan reads it: `?s a asset360:Signal`, with the
/// prefixes a reader of a plan already has in their head.
fn triple_text(pattern: &TriplePattern) -> String {
    let predicate = match &pattern.predicate {
        NamedNodePattern::NamedNode(node) if node.as_str() == crate::sparql_scoper::RDF_TYPE => {
            "a".to_owned()
        }
        NamedNodePattern::NamedNode(node) => shorten(node.as_str()),
        NamedNodePattern::Variable(variable) => variable.to_string(),
    };
    format!(
        "{} {predicate} {}",
        term_text(&pattern.subject),
        term_text(&pattern.object)
    )
}

fn term_text(term: &TermPattern) -> String {
    match term {
        TermPattern::NamedNode(node) => shorten(node.as_str()),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// The naive builder
// ---------------------------------------------------------------------------

/// Why a naive plan could not be built.
#[derive(Debug)]
pub enum RefineError {
    /// The query could not be parsed, or enumerating its obligations refused
    /// it. `UNION`, `MINUS` and property paths arrive here: see the note on
    /// [`naive_plan`].
    Scope(ScopeError),
    /// The plan came out violating an invariant, which means the builder and
    /// [`crate::sparql_plan::obligations_of`] disagree about the query.
    Defect(PlanDefect),
}

impl fmt::Display for RefineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scope(error) => write!(f, "{error}"),
            Self::Defect(defect) => write!(f, "naive plan is not sound: {defect}"),
        }
    }
}

impl From<ScopeError> for RefineError {
    fn from(error: ScopeError) -> Self {
        Self::Scope(error)
    }
}

/// Parse a query and build its naive plan.
pub fn naive_plan_of(query_str: &str) -> Result<Plan, RefineError> {
    let parsed = crate::sparql_scoper::parse_query(query_str)?;
    naive_plan(&parsed)
}

/// The query, faithfully, with every node engine-executed and nothing folded.
///
/// The obligations are [`crate::sparql_plan::obligations_of`]'s, unchanged, so
/// a naive plan and an [`crate::sparql_plan::ExecutionPlan`] for the same
/// query account for the same question. Every one of them is claimed by the
/// node that came from the same piece of algebra, and the residual is empty --
/// which is another way of saying this plan is already correct: it is what the
/// endpoint does when nothing is pushed.
///
/// One consequence of reusing that enumeration, and it is its property rather
/// than this builder's: `UNION`, `MINUS`, property paths, and `LATERAL` are
/// refused, because `tag_triples_by_depth` refuses them. [`PlanOp::Union`],
/// [`PlanOp::Minus`] and [`PlanOp::Path`] exist so that lifting the refusal is
/// a change to obligation enumeration and not a change to the plan shape, but
/// no query reaches them through this function yet. `LATERAL` differs from
/// the other three: there is no `PlanOp::Lateral` placeholder, because it is
/// refused permanently rather than pending support.
///
/// The plan is checked before it is returned, in every build rather than
/// behind a debug assertion. The builder's one assumption is that the
/// obligation list is ordered the way it walks the algebra; checking turns a
/// broken assumption into an error instead of a plan that answers a different
/// question.
pub fn naive_plan(query: &Query) -> Result<Plan, RefineError> {
    let obligations = crate::sparql_plan::obligations_of(query)?;
    let form = QueryForm::of(query);
    let pattern = match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => pattern,
    };

    // Where the modifier obligations start: `obligations_of` emits every
    // triple first, in the order `tag_triples_by_depth` produced.
    let triple_count = obligations
        .iter()
        .take_while(|obligation| {
            matches!(
                obligation,
                Obligation::Type { .. } | Obligation::Triple { .. }
            )
        })
        .count();

    let mut builder = Builder {
        obligations: &obligations,
        nodes: Vec::new(),
        vars: Vec::new(),
        next_triple: 0,
        next_modifier: triple_count,
    };
    let mut root = builder.pattern(pattern, true);

    // The shaping nodes sit above everything, so none of them disturbs the
    // frontier below.
    match query {
        Query::Select { .. } => {}
        Query::Construct { template, .. } => {
            root = builder.push(
                PlanOp::Construct {
                    input: root,
                    template: template.clone(),
                },
                Vec::new(),
                BTreeSet::new(),
            );
        }
        Query::Describe { .. } => {
            // The described resources are the ones the solution names, which
            // is the projection below. `DESCRIBE <iri>` names none.
            let vars = match &builder.nodes[root].op {
                PlanOp::Project { vars, .. } => vars.clone(),
                _ => Vec::new(),
            };
            root = builder.push(
                PlanOp::Describe { input: root, vars },
                Vec::new(),
                BTreeSet::new(),
            );
        }
        Query::Ask { .. } => {
            root = builder.push(PlanOp::Ask { input: root }, Vec::new(), BTreeSet::new());
        }
    }
    debug_assert_eq!(root, builder.nodes.len() - 1, "the root is the last node");

    let nodes = builder.nodes;
    let plan = Plan {
        form,
        obligations,
        nodes,
        residual: Vec::new(),
    };
    plan.check().map_err(RefineError::Defect)?;
    Ok(plan)
}

/// Walks the algebra once, in the same order the obligation enumeration walks
/// it, so a node can claim the obligation that came from the same place.
struct Builder<'o> {
    obligations: &'o [Obligation],
    nodes: Vec<Node>,
    /// The variables each node binds, parallel to `nodes`. Needed only to
    /// give a join the variables it joins on, so it stays here rather than on
    /// the plan.
    vars: Vec<BTreeSet<String>>,
    next_triple: usize,
    next_modifier: usize,
}

impl Builder<'_> {
    fn push(
        &mut self,
        op: PlanOp,
        discharges: Vec<ObligationId>,
        vars: BTreeSet<String>,
    ) -> NodeId {
        self.nodes.push(Node::engine(op, discharges));
        self.vars.push(vars);
        self.nodes.len() - 1
    }

    /// The id of the obligation this triple pattern raised.
    ///
    /// Compared by content rather than trusted by position: if the
    /// enumeration order ever changes, this claims nothing and the ledger
    /// check reports the unclaimed obligation, instead of a node claiming a
    /// filter because it happened to sit at the same index.
    fn claim_triple(&mut self, pattern: &TriplePattern) -> Vec<ObligationId> {
        let id = self.next_triple;
        match self.obligations.get(id) {
            Some(obligation) if *obligation == obligation_of_triple(pattern) => {
                self.next_triple += 1;
                vec![id]
            }
            _ => Vec::new(),
        }
    }

    /// The id of the next modifier obligation, when it is the kind this node
    /// raises. Same contract as [`Self::claim_triple`]: a mismatch claims
    /// nothing and fails the ledger.
    fn claim_modifier(&mut self, expected: impl Fn(&Obligation) -> bool) -> Vec<ObligationId> {
        let id = self.next_modifier;
        match self.obligations.get(id) {
            Some(obligation) if expected(obligation) => {
                self.next_modifier += 1;
                vec![id]
            }
            _ => Vec::new(),
        }
    }

    /// Build one graph pattern and return its node.
    ///
    /// `on_spine` says whether this pattern is still on the path from the root
    /// through solution modifiers only. A `Project` there is the query's own
    /// projection; anywhere else it is a sub-`SELECT`, which is a barrier
    /// rather than a projection.
    fn pattern(&mut self, pattern: &GraphPattern, on_spine: bool) -> NodeId {
        match pattern {
            GraphPattern::Bgp { patterns } => self.bgp(patterns),
            GraphPattern::Path {
                subject,
                path,
                object,
            } => {
                let mut vars = BTreeSet::new();
                add_term_var(subject, &mut vars);
                add_term_var(object, &mut vars);
                self.push(
                    PlanOp::Path {
                        subject: subject.clone(),
                        path: Box::new(path.clone()),
                        object: object.clone(),
                    },
                    Vec::new(),
                    vars,
                )
            }
            GraphPattern::Join { left, right } => {
                let left = self.pattern(left, false);
                let right = self.pattern(right, false);
                self.join(left, right)
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                // The lifted condition is enumerated per conjunct like any
                // other filter, and claimed here rather than on a node of its
                // own: it decides whether the optional side *matched*, which
                // is a property of the join and not a filter above it.
                let condition = expression.as_ref().map(Expr::from);
                let mut claims = Vec::new();
                let conjuncts = condition.clone().map(conjuncts_of).unwrap_or_default();
                for _ in &conjuncts {
                    claims.extend(self.claim_modifier(|obligation| {
                        matches!(obligation, Obligation::Filter { .. })
                    }));
                }
                let left = self.pattern(left, false);
                let right = self.pattern(right, false);
                let vars: BTreeSet<String> =
                    self.vars[left].union(&self.vars[right]).cloned().collect();
                let node = self.push(
                    PlanOp::LeftJoin {
                        left,
                        right,
                        reference: None,
                        condition,
                    },
                    claims,
                    vars,
                );
                // An `EXISTS` in the lifted condition reads records too, and
                // the order — left, right, then the condition — is the one
                // the obligation enumeration walks.
                match expression {
                    Some(expression) => self.read_exists_blocks(expression, node),
                    None => node,
                }
            }
            GraphPattern::Union { left, right } => {
                let left = self.pattern(left, false);
                let right = self.pattern(right, false);
                let vars: BTreeSet<String> =
                    self.vars[left].union(&self.vars[right]).cloned().collect();
                self.push(PlanOp::Union { left, right }, Vec::new(), vars)
            }
            // Refused during obligation enumeration, by
            // `tag_triples_by_depth` — the same thing that keeps the `Union`
            // arm above unreached. There is deliberately no `PlanOp::Lateral`:
            // the placeholder variants exist for constructs we mean to support
            // later, and LATERAL is not one. If this ever fires, the refusal
            // and this builder have stopped agreeing about the query, which is
            // a defect to fix rather than a plan to build.
            GraphPattern::Lateral { .. } => unreachable!(
                "LATERAL reached the plan builder; tag_triples_by_depth must refuse it first"
            ),
            GraphPattern::Minus { left, right } => {
                let left = self.pattern(left, false);
                let right = self.pattern(right, false);
                let vars = self.vars[left].clone();
                self.push(PlanOp::Minus { left, right }, Vec::new(), vars)
            }
            GraphPattern::Filter { expr, inner } => {
                // One node per top-level conjunct, chained, each claiming its
                // own obligation. That is what makes pushing one of them a
                // decision about a node: `FILTER(?name > "A" && REGEX(?name,
                // "^A"))` becomes a comparison a rule can push and a regex it
                // declines, and the claim moves with the node instead of
                // having to be split.
                // Conjuncts as spargebra wrote them, because one of them may be
                // a `NOT EXISTS` and that is a *pattern* rather than a value
                // test: the block it negates becomes nodes, so the same
                // flattening has to serve both. It is
                // `sparql_plan::flatten_conjunction`, the one the enumeration
                // counts obligations with, for the reason `conjuncts_of`
                // documents — a disagreement about how many conjuncts a filter
                // has leaves an obligation unclaimed.
                let conjuncts = crate::sparql_plan::flatten_conjunction_of(expr);
                // Claimed before descending, because that is the order the
                // enumeration pushes them: a filter's obligations come before
                // anything under it.
                let mut claims = Vec::with_capacity(conjuncts.len());
                for _ in &conjuncts {
                    claims.push(self.claim_modifier(|obligation| {
                        matches!(obligation, Obligation::Filter { .. })
                    }));
                }
                let mut input = self.pattern(inner, on_spine);
                // First conjunct nearest the input, so the plan reads in the
                // order the query wrote it.
                for (conjunct, claim) in conjuncts.into_iter().zip(claims) {
                    input = match negated_exists(conjunct) {
                        // The negation as a node: an anti-join over the block,
                        // which a rule can push into the statement. The block
                        // is built first, so its triples are claimed in the
                        // order the enumeration walked them.
                        Some(block) => {
                            let right = self.pattern(block, false);
                            // An anti-join binds nothing new: `NOT EXISTS`
                            // tests the left row and drops it or keeps it.
                            let vars = self.vars[input].clone();
                            self.push(
                                PlanOp::AntiJoin {
                                    left: input,
                                    right,
                                    reference: None,
                                },
                                claim,
                                vars,
                            )
                        }
                        None => {
                            let input = self.read_exists_blocks(conjunct, input);
                            let vars = self.vars[input].clone();
                            self.push(
                                PlanOp::Filter {
                                    input,
                                    condition: Expr::from(conjunct),
                                },
                                claim,
                                vars,
                            )
                        }
                    };
                }
                input
            }
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => {
                let input = self.pattern(inner, on_spine);
                let input = self.read_exists_blocks(expression, input);
                let mut vars = self.vars[input].clone();
                vars.insert(variable.as_str().to_owned());
                self.push(
                    PlanOp::Bind {
                        input,
                        var: variable.as_str().to_owned(),
                        expr: Expr::from(expression),
                    },
                    Vec::new(),
                    vars,
                )
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => {
                // The enumeration emits the grouping only when there are
                // keys -- a bare aggregate groups nothing -- then one
                // obligation per aggregate, in order.
                let mut claims = Vec::new();
                if !variables.is_empty() {
                    claims.extend(self.claim_modifier(|obligation| {
                        matches!(obligation, Obligation::Group { .. })
                    }));
                }
                for _ in aggregates {
                    claims.extend(self.claim_modifier(|obligation| {
                        matches!(obligation, Obligation::Aggregate { .. })
                    }));
                }
                let input = self.pattern(inner, on_spine);
                let keys: Vec<String> = variables
                    .iter()
                    .map(|variable| variable.as_str().to_owned())
                    .collect();
                let measures: Vec<Measure> = aggregates
                    .iter()
                    .map(|(variable, aggregate)| Measure {
                        var: variable.as_str().to_owned(),
                        aggregate: aggregate.clone(),
                    })
                    .collect();
                // A grouping collapses rows: only its keys and measures
                // survive it, which is what makes it the boundary the engine
                // cannot finish a query past.
                let mut vars: BTreeSet<String> = keys.iter().cloned().collect();
                vars.extend(measures.iter().map(|measure| measure.var.clone()));
                self.push(
                    PlanOp::Group {
                        input,
                        keys,
                        measures,
                        having: Vec::new(),
                    },
                    claims,
                    vars,
                )
            }
            GraphPattern::OrderBy { inner, expression } => {
                let claims = self
                    .claim_modifier(|obligation| matches!(obligation, Obligation::Order { .. }));
                let input = self.pattern(inner, on_spine);
                let vars = self.vars[input].clone();
                let terms = expression
                    .iter()
                    .map(|term| match term {
                        OrderExpression::Asc(expr) => SortTerm {
                            expr: Expr::from(expr),
                            desc: false,
                        },
                        OrderExpression::Desc(expr) => SortTerm {
                            expr: Expr::from(expr),
                            desc: true,
                        },
                    })
                    .collect();
                self.push(PlanOp::Sort { input, terms }, claims, vars)
            }
            GraphPattern::Distinct { inner } => {
                let claims =
                    self.claim_modifier(|obligation| matches!(obligation, Obligation::Distinct));
                let input = self.pattern(inner, on_spine);
                let vars = self.vars[input].clone();
                self.push(PlanOp::Distinct { input }, claims, vars)
            }
            GraphPattern::Reduced { inner } => {
                let input = self.pattern(inner, on_spine);
                let vars = self.vars[input].clone();
                self.push(PlanOp::Reduced { input }, Vec::new(), vars)
            }
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => {
                let claims = self
                    .claim_modifier(|obligation| matches!(obligation, Obligation::Slice { .. }));
                let input = self.pattern(inner, on_spine);
                let vars = self.vars[input].clone();
                self.push(
                    PlanOp::Slice {
                        input,
                        limit: *length,
                        offset: *start,
                    },
                    claims,
                    vars,
                )
            }
            GraphPattern::Project { inner, variables } => {
                // Past a projection nothing below is the query's own, so a
                // second one is a sub-select.
                let input = self.pattern(inner, false);
                let vars: Vec<String> = variables
                    .iter()
                    .map(|variable| variable.as_str().to_owned())
                    .collect();
                let bound: BTreeSet<String> = vars.iter().cloned().collect();
                let op = if on_spine {
                    PlanOp::Project { input, vars }
                } else {
                    PlanOp::SubSelect { input, vars }
                };
                self.push(op, Vec::new(), bound)
            }
            GraphPattern::Graph { name, inner } => {
                let input = self.pattern(inner, on_spine);
                let vars = self.vars[input].clone();
                self.push(
                    PlanOp::Graph {
                        input,
                        name: name.to_string(),
                    },
                    Vec::new(),
                    vars,
                )
            }
            GraphPattern::Service {
                name,
                inner,
                silent,
            } => {
                let input = self.pattern(inner, on_spine);
                let vars = self.vars[input].clone();
                self.push(
                    PlanOp::Service {
                        input,
                        name: name.to_string(),
                        silent: *silent,
                    },
                    Vec::new(),
                    vars,
                )
            }
            GraphPattern::Values {
                variables,
                bindings,
            } => {
                let vars: BTreeSet<String> = variables
                    .iter()
                    .map(|variable| variable.as_str().to_owned())
                    .collect();
                let claims = self
                    .claim_modifier(|obligation| matches!(obligation, Obligation::Values { .. }));
                self.push(
                    PlanOp::Values {
                        variables: variables.clone(),
                        rows: bindings.clone(),
                    },
                    claims,
                    vars,
                )
            }
        }
    }

    /// One match per triple pattern, joined left-deep.
    /// Build every `EXISTS` block a condition holds, as a left-join side.
    ///
    /// The condition itself stays where it is — an opaque expression the
    /// engine evaluates — and this is only about the records it reads: they
    /// have to be in the fetch, or the engine evaluates `NOT EXISTS` against
    /// a class with nothing in it and reports every row as lacking. A *left*
    /// join because the block is not a constraint on the rows the query
    /// selects: `NOT EXISTS` keeps exactly the rows it does not match.
    ///
    /// The blocks come from [`crate::sparql_scoper::exists_patterns_of`], the
    /// same walk in the same order the obligation enumeration used, so each
    /// triple inside is claimed by the `Match` node built from it.
    fn read_exists_blocks(&mut self, expr: &Expression, input: NodeId) -> NodeId {
        let mut blocks = Vec::new();
        crate::sparql_scoper::exists_patterns_of(expr, &mut blocks);
        let mut current = input;
        for block in blocks {
            let right = self.pattern(block, false);
            let vars: BTreeSet<String> = self.vars[current]
                .union(&self.vars[right])
                .cloned()
                .collect();
            current = self.push(
                PlanOp::LeftJoin {
                    left: current,
                    right,
                    reference: None,
                    condition: None,
                },
                Vec::new(),
                vars,
            );
        }
        current
    }

    fn bgp(&mut self, patterns: &[TriplePattern]) -> NodeId {
        let mut current: Option<NodeId> = None;
        for pattern in patterns {
            let claims = self.claim_triple(pattern);
            let mut vars = BTreeSet::new();
            add_term_var(&pattern.subject, &mut vars);
            if let NamedNodePattern::Variable(variable) = &pattern.predicate {
                vars.insert(variable.as_str().to_owned());
            }
            add_term_var(&pattern.object, &mut vars);
            let node = self.push(
                PlanOp::Match {
                    pattern: Box::new(pattern.clone()),
                },
                claims,
                vars,
            );
            current = Some(match current {
                Some(left) => self.join(left, node),
                None => node,
            });
        }
        // An empty group graph pattern matches the single empty solution, and
        // a join needs that identity to build on.
        current.unwrap_or_else(|| self.push(PlanOp::Unit, Vec::new(), BTreeSet::new()))
    }

    fn join(&mut self, left: NodeId, right: NodeId) -> NodeId {
        let on: Vec<String> = self.vars[left]
            .intersection(&self.vars[right])
            .cloned()
            .collect();
        let vars: BTreeSet<String> = self.vars[left].union(&self.vars[right]).cloned().collect();
        self.push(
            PlanOp::Join {
                left,
                right,
                on,
                reference: None,
            },
            Vec::new(),
            vars,
        )
    }
}

/// The block a conjunct negates, when the conjunct is exactly `NOT EXISTS`.
///
/// Exactly: `!EXISTS { ... }` and nothing else. `!(EXISTS { ... } && ?x > 3)`
/// is a negated conjunction whose rows are not the anti-join's, and
/// `EXISTS { ... }` on its own is the positive form, which does not push (see
/// [`PlanOp::AntiJoin`]). Both keep the opaque-filter path, which is correct
/// and slower.
fn negated_exists(expr: &Expression) -> Option<&GraphPattern> {
    match expr {
        Expression::Not(inner) => match inner.as_ref() {
            Expression::Exists(block) => Some(block),
            _ => None,
        },
        _ => None,
    }
}

/// The top-level conjuncts of a condition, in the order the query wrote them.
///
/// [`Expr::from`] already flattens a nest of `And`s into one node, so this is
/// the one place that decides how many obligations a `FILTER` raises here --
/// and it has to agree with `obligations_of`, which flattens the spargebra
/// expression the same way. A disagreement leaves an obligation unclaimed and
/// fails the ledger rather than passing quietly.
fn conjuncts_of(condition: Expr) -> Vec<Expr> {
    match condition {
        Expr::And(parts) => parts,
        other => vec![other],
    }
}

fn add_term_var(term: &TermPattern, out: &mut BTreeSet<String>) {
    if let TermPattern::Variable(variable) = term {
        out.insert(variable.as_str().to_owned());
    }
}

/// The variable a triple pattern's subject binds, when it is one.
pub fn subject_variable(pattern: &TriplePattern) -> Option<&str> {
    match &pattern.subject {
        TermPattern::Variable(variable) => Some(variable.as_str()),
        _ => None,
    }
}

/// The IRI a triple pattern's subject names, when the query named a record
/// rather than a variable.
///
/// `<uri> a asset360:Signal ; asset360:name ?nm` is the canonical way to ask
/// about one asset, and it is the spelling the form configurations should be
/// moving *to* -- so a planner that reads it as "every Signal, narrowed later"
/// is a planner nobody should migrate onto.
pub fn subject_iri(pattern: &TriplePattern) -> Option<&str> {
    match &pattern.subject {
        TermPattern::NamedNode(node) => Some(node.as_str()),
        _ => None,
    }
}

/// The variable a triple pattern's object binds, when it is one.
pub fn object_variable(pattern: &TriplePattern) -> Option<&str> {
    match &pattern.object {
        TermPattern::Variable(variable) => Some(variable.as_str()),
        _ => None,
    }
}

/// The IRI a triple pattern's predicate names, when it is constant.
pub fn predicate_iri(pattern: &TriplePattern) -> Option<&str> {
    match &pattern.predicate {
        NamedNodePattern::NamedNode(node) => Some(node.as_str()),
        NamedNodePattern::Variable(_) => None,
    }
}

/// The class IRI a triple pattern scopes its subject to, when it is an
/// `rdf:type` against a constant.
pub fn type_class_iri(pattern: &TriplePattern) -> Option<&str> {
    if predicate_iri(pattern)? != crate::sparql_scoper::RDF_TYPE {
        return None;
    }
    match &pattern.object {
        TermPattern::NamedNode(node) => Some(node.as_str()),
        _ => None,
    }
}

/// Whether a triple pattern's predicate is `rdf:type`, whatever its object.
pub fn is_type_pattern(pattern: &TriplePattern) -> bool {
    predicate_iri(pattern) == Some(crate::sparql_scoper::RDF_TYPE)
}

/// Which nodes are connected to which by plain joins only.
///
/// A rule that folds matches together has to know they are joined
/// *mandatorily*: two matches in different arms of a `UNION`, or one on the
/// preserved side of a `LEFT JOIN` and one inside it, are not the same row
/// set, and folding them into one scan with an existence check would drop rows
/// the query keeps.
pub fn inner_join_groups(plan: &Plan) -> Vec<usize> {
    let mut group: Vec<usize> = (0..plan.nodes.len()).collect();
    fn root_of(group: &mut [usize], mut id: usize) -> usize {
        while group[id] != id {
            let parent = group[id];
            group[id] = group[parent];
            id = group[id];
        }
        id
    }
    let unite = |group: &mut [usize], left: usize, right: usize| {
        let (left, right) = (root_of(group, left), root_of(group, right));
        if left != right {
            group[right] = left;
        }
    };
    for (id, node) in plan.nodes.iter().enumerate() {
        // Everything but a plain join is a boundary on purpose: only a join
        // makes two node sets one mandatory row set.
        if let PlanOp::Join { left, right, .. } = &node.op {
            unite(&mut group, id, *left);
            unite(&mut group, id, *right);
        }
    }
    (0..plan.nodes.len())
        .map(|id| root_of(&mut group, id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use spargebra::term::{Literal, NamedNode};

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

    fn plan_of(query: &str) -> Plan {
        naive_plan_of(&format!("{PREFIX}{query}")).expect("should build a naive plan")
    }

    /// The shapes a naive plan has to hold, and the ledger balancing on every
    /// one of them. The invariant is only worth stating if it is checked on
    /// plans the builder produced rather than on hand-built ones.
    #[test]
    fn every_naive_plan_balances_its_ledger() {
        for query in [
            "SELECT ?s WHERE { ?s a asset360:Signal }",
            "SELECT * WHERE { }",
            "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal }",
            "SELECT ?kind (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:kind ?kind } GROUP BY ?kind ORDER BY DESC(?n) LIMIT 10 OFFSET 5",
            "SELECT DISTINCT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             FILTER(REGEX(?nm, \"^A\")) FILTER(?nm > \"A\") }",
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal . \
             OPTIONAL { ?s asset360:name ?nm } }",
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal . \
             OPTIONAL { ?s asset360:name ?nm . FILTER(?nm > \"A\" && ?nm < \"B\") } }",
            "SELECT ?s ?len WHERE { ?s a asset360:Signal ; asset360:length ?len . \
             FILTER(?len >= 10 && ?len < 100) }",
            "SELECT ?s ?doubled WHERE { ?s a asset360:Signal ; asset360:length ?len . \
             BIND(?len * 2 AS ?doubled) }",
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t a asset360:Track ; asset360:hasName ?tn }",
            "SELECT ?s WHERE { ?s a asset360:Signal . FILTER EXISTS { \
             ?s asset360:name ?nm } }",
            "SELECT ?s WHERE { { SELECT ?s WHERE { ?s a asset360:Signal } LIMIT 3 } }",
            "SELECT ?s WHERE { VALUES ?s { \"a\" \"b\" } }",
            "ASK WHERE { ?s a asset360:Signal }",
            "CONSTRUCT { ?s asset360:name ?nm } WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm }",
            "DESCRIBE ?s WHERE { ?s a asset360:Signal }",
        ] {
            let plan = plan_of(query);
            plan.check()
                .unwrap_or_else(|defect| panic!("{defect} for {query}\n{plan}"));
            assert!(
                plan.residual.is_empty(),
                "a naive plan answers the whole question: {query}\n{plan}"
            );
            assert!(
                plan.nodes
                    .iter()
                    .all(|node| node.executor == Executor::Engine),
                "nothing is pushed in a naive plan: {query}\n{plan}"
            );
        }
    }

    /// There is no scan in a naive plan: a scan is already a pushdown
    /// decision, so each triple pattern is a match and the joins between them
    /// are nodes too.
    #[test]
    fn the_naive_plan_is_matches_and_joins() {
        let plan = plan_of(
            "SELECT ?kind (COUNT(*) AS ?n) WHERE { \
             ?s a asset360:TunnelComplex ; asset360:hasName ?nm ; \
             asset360:trafficKinds ?kind . FILTER(?nm > \"A\") \
             FILTER(REGEX(?nm, \"^A\")) } GROUP BY ?kind ORDER BY DESC(?n) LIMIT 10",
        );
        assert!(plan.find("scan").is_empty(), "{plan}");
        assert_eq!(plan.find("match").len(), 3, "{plan}");
        assert_eq!(plan.find("join").len(), 2, "{plan}");
        // Two filter nodes for two `FILTER`s, even though spargebra conjoins
        // them into a single `Filter { And(..) }` before anyone sees them.
        // The split is per top-level conjunct precisely so that
        // `FILTER(a) FILTER(b)` and `FILTER(a && b)` account identically --
        // they are the same query -- and so that pushing the comparison while
        // leaving the regex above is a decision about a node rather than a
        // claim that has to be split in two.
        assert_eq!(plan.find("filter").len(), 2, "{plan}");
        let filters: Vec<String> = plan
            .nodes
            .iter()
            .filter_map(|node| match &node.op {
                PlanOp::Filter { condition, .. } => Some(condition.to_string()),
                _ => None,
            })
            .collect();
        assert!(filters[0].contains('>'), "{filters:?}");
        assert!(filters[1].contains("REGEX"), "{filters:?}");
        assert!(
            plan.nodes
                .iter()
                .filter(|node| node.op.kind() == "filter")
                .all(|node| node.discharges.len() == 1),
            "each conjunct claims its own obligation:\n{plan}"
        );
        assert_eq!(plan.find("group").len(), 1, "{plan}");
        assert_eq!(plan.find("sort").len(), 1, "{plan}");
        assert_eq!(plan.find("slice").len(), 1, "{plan}");
        assert_eq!(plan.find("project").len(), 1, "{plan}");

        // Every obligation of the query is claimed by the node that came from
        // the same piece of algebra, which is what makes the naive plan an
        // answer rather than a sketch.
        for (id, obligation) in plan.obligations.iter().enumerate() {
            let claimant = plan
                .nodes
                .iter()
                .find(|node| node.discharges.contains(&id))
                .unwrap_or_else(|| panic!("o{id} ({obligation}) unclaimed\n{plan}"));
            let expected = match obligation {
                Obligation::Type { .. } | Obligation::Triple { .. } => "match",
                Obligation::Filter { .. } => "filter",
                Obligation::Group { .. } | Obligation::Aggregate { .. } => "group",
                Obligation::Order { .. } => "sort",
                Obligation::Slice { .. } => "slice",
                Obligation::Distinct => "distinct",
                Obligation::Values { .. } => "values",
            };
            assert_eq!(claimant.op.kind(), expected, "o{id} ({obligation})\n{plan}");
        }
        println!("{plan}");
    }

    /// The two constraints that used to have no obligation at all, claimed by
    /// the nodes that hold them. An `OPTIONAL`'s lifted condition belongs to
    /// the join (it decides whether the optional side matched), and a `VALUES`
    /// block belongs to the inline table itself -- not to a filter above it,
    /// which is what modelling it as one would have implied.
    #[test]
    fn the_optional_condition_and_the_values_block_are_claimed() {
        let plan = plan_of(
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal . \
             OPTIONAL { ?s asset360:name ?nm . FILTER(?nm > \"A\" && ?nm < \"B\") } }",
        );
        let leftjoin = plan.find("leftjoin");
        assert_eq!(leftjoin.len(), 1, "{plan}");
        assert_eq!(
            plan.nodes[leftjoin[0]].discharges.len(),
            2,
            "one claim per conjunct of the lifted condition:\n{plan}"
        );
        assert!(
            plan.find("filter").is_empty(),
            "the lifted condition is the join's, not a filter above it:\n{plan}"
        );
        plan.check().unwrap();

        let plan = plan_of("SELECT ?s WHERE { VALUES ?s { \"a\" \"b\" } }");
        let values = plan.find("values");
        assert_eq!(values.len(), 1, "{plan}");
        assert!(
            matches!(
                plan.obligations[plan.nodes[values[0]].discharges[0]],
                Obligation::Values { rows: 2, .. }
            ),
            "{plan}"
        );
        println!("{plan}");
    }

    /// The query forms are representable, and the fourth invariant is what
    /// keeps them honest as more are added.
    #[test]
    fn the_query_forms_root_in_their_own_output_kind() {
        for (query, form, kind) in [
            (
                "SELECT ?s WHERE { ?s a asset360:Signal }",
                QueryForm::Select,
                OutputKind::Solutions,
            ),
            (
                "ASK WHERE { ?s a asset360:Signal }",
                QueryForm::Ask,
                OutputKind::Boolean,
            ),
            (
                "CONSTRUCT { ?s asset360:name ?nm } WHERE { ?s a asset360:Signal ; \
                 asset360:name ?nm }",
                QueryForm::Construct,
                OutputKind::Triples,
            ),
            (
                "DESCRIBE ?s WHERE { ?s a asset360:Signal }",
                QueryForm::Describe,
                OutputKind::Triples,
            ),
        ] {
            let plan = plan_of(query);
            assert_eq!(plan.form, form, "{plan}");
            assert_eq!(plan.root().unwrap().output, kind, "{plan}");
            plan.root_matches_form().unwrap();
        }
    }

    /// A sub-select is a barrier, not a projection: the outer query cannot see
    /// through it, so the two must not be the same node kind.
    #[test]
    fn a_nested_projection_is_a_subselect() {
        let plan =
            plan_of("SELECT ?s WHERE { { SELECT ?s WHERE { ?s a asset360:Signal } LIMIT 3 } }");
        assert_eq!(plan.find("project").len(), 1, "{plan}");
        assert_eq!(plan.find("subselect").len(), 1, "{plan}");
    }

    /// `UNION`, `MINUS` and property paths have nodes, and no query reaches
    /// them: obligation enumeration refuses those constructs, so a naive plan
    /// for one would have nothing to claim. Pinned as a test because the
    /// difference between "representable" and "reachable" is exactly what a
    /// reader of the node list would otherwise get wrong.
    #[test]
    fn the_constructs_obligations_refuse_do_not_plan_yet() {
        for query in [
            "SELECT ?s WHERE { { ?s a asset360:Signal } UNION { ?s a asset360:Track } }",
            "SELECT ?s WHERE { ?s a asset360:Signal MINUS { ?s asset360:name ?nm } }",
            "SELECT ?nm WHERE { ?s asset360:locatedOnTrack+/asset360:hasName ?nm }",
        ] {
            let error = naive_plan_of(&format!("{PREFIX}{query}"))
                .expect_err("obligation enumeration refuses this");
            assert!(
                matches!(error, RefineError::Scope(_)),
                "refused for the enumeration's reason, not a defect: {error} for {query}"
            );
        }
    }

    /// Each invariant fails on a plan that violates it, and passes on the one
    /// it was derived from. Deliberately hand-built: a check nothing can fail
    /// is not a check.
    #[test]
    fn each_invariant_catches_its_own_violation() {
        let sound = plan_of("SELECT ?s WHERE { ?s a asset360:Signal }");
        sound.check().unwrap();

        // 1. The ledger: a node stops claiming what it discharged.
        let mut unbalanced = sound.clone();
        unbalanced.nodes[0].discharges.clear();
        assert_eq!(
            unbalanced.check(),
            Err(PlanDefect::Ledger(LedgerError {
                missing: vec![0],
                duplicated: Vec::new(),
            })),
        );
        // ...and the same obligation claimed twice, which is how a filter gets
        // applied in two places and its rows counted once too often.
        let mut doubled = sound.clone();
        doubled.nodes[1].discharges.push(0);
        assert_eq!(
            doubled.check(),
            Err(PlanDefect::Ledger(LedgerError {
                missing: Vec::new(),
                duplicated: vec![0],
            })),
        );

        // 2. Well-formedness: an input that does not precede its node, which
        // is what inserting a node without renumbering leaves behind.
        let mut ill_formed = sound.clone();
        if let PlanOp::Project { input, .. } = &mut ill_formed.nodes[1].op {
            *input = 1;
        }
        assert_eq!(
            ill_formed.check(),
            Err(PlanDefect::IllFormed { node: 1, input: 1 })
        );

        // ...and a node whose declared output kind is not its operator's.
        let mut mislabelled = sound.clone();
        mislabelled.nodes[1].output = OutputKind::Triples;
        assert_eq!(
            mislabelled.check(),
            Err(PlanDefect::MislabelledOutput { node: 1 })
        );

        // 3. The frontier is a cut: SQL cannot consume engine output.
        let mut breached = sound.clone();
        breached.nodes[1].executor = Executor::Sql;
        assert_eq!(
            breached.check(),
            Err(PlanDefect::FrontierBreach { sql: 1, engine: 0 })
        );
        // The other direction is legal, and has to be: an engine node reading
        // an SQL one is the whole point of a frontier.
        let mut pushed = sound.clone();
        pushed.nodes[0].executor = Executor::Sql;
        pushed.check().unwrap();

        // 4. The root's output kind is the query form's.
        let mut wrong_form = sound.clone();
        wrong_form.form = QueryForm::Ask;
        assert_eq!(
            wrong_form.check(),
            Err(PlanDefect::WrongOutput {
                form: QueryForm::Ask,
                root: OutputKind::Solutions,
            })
        );
        let empty = Plan {
            form: QueryForm::Select,
            obligations: Vec::new(),
            nodes: Vec::new(),
            residual: Vec::new(),
        };
        assert_eq!(empty.check(), Err(PlanDefect::Empty));

        // 5. A scan that folded a multivalued slot without its unnest. The
        // ledger, well-formedness and the frontier all hold here -- this is
        // the plan 28d says nothing catches.
        let mut lost_fanout = sound.clone();
        lost_fanout.nodes[0] = Node::sql(
            PlanOp::Scan {
                star_var: "s".to_owned(),
                class_uri: "https://data.infrabel.be/asset360/Signal".to_owned(),
                identifier_values: Vec::new(),
                slots: vec![ScanSlot {
                    path: vec!["trafficKinds".to_owned()],
                    var: Some("kind".to_owned()),
                    multivalued: true,
                    presence: SlotPresence::Required,
                }],
            },
            vec![0],
        );
        lost_fanout.ledger_balances().unwrap();
        lost_fanout.well_formed().unwrap();
        lost_fanout.frontier_is_a_cut().unwrap();
        lost_fanout.root_matches_form().unwrap();
        assert_eq!(
            lost_fanout.check(),
            Err(PlanDefect::LostFanout {
                scan: 0,
                slot: "trafficKinds".to_owned(),
            })
        );
    }

    /// A scan cannot be built without the unnests its multivalued slots
    /// demand, because the same list decides both.
    #[test]
    fn a_scan_comes_with_its_fanout() {
        let slots = vec![
            ScanSlot {
                path: vec!["hasName".to_owned()],
                var: Some("nm".to_owned()),
                multivalued: false,
                presence: SlotPresence::Required,
            },
            ScanSlot {
                path: vec!["trafficKinds".to_owned()],
                var: Some("kind".to_owned()),
                multivalued: true,
                presence: SlotPresence::Required,
            },
        ];
        let nodes = scan_with_fanout(
            "s",
            "https://data.infrabel.be/asset360/TunnelComplex",
            slots,
            Vec::new(),
            0,
            vec![0],
        );
        let kinds: Vec<&str> = nodes.iter().map(|node| node.op.kind()).collect();
        assert_eq!(kinds, vec!["scan", "unnest"]);
        assert!(
            nodes.iter().all(|node| node.executor == Executor::Sql),
            "a folded subtree runs in SQL"
        );
    }

    /// The classes the stars of these tests were scanned as.
    fn tunnel_star() -> HashMap<String, String> {
        HashMap::from([(
            "s".to_owned(),
            "https://data.infrabel.be/asset360/TunnelComplex".to_owned(),
        )])
    }

    /// The rendering half is partial in two directions, and both are the
    /// point: by construct, and by whether a rule has yet resolved the
    /// variable to a column.
    #[test]
    fn the_sql_shape_declines_what_sql_cannot_express() {
        let slot = Expr::Slot {
            star_var: "s".to_owned(),
            slot_path: vec!["hasName".to_owned()],
            reading: SlotReading::Column,
            presence: SlotPresence::Required,
        };
        let value = Expr::Literal(Literal::new_simple_literal("A").into());

        let pushable = Expr::Compare {
            op: CompareOp::Gt,
            left: Box::new(slot.clone()),
            right: Box::new(value.clone()),
        };
        assert_eq!(
            pushable.sql_shape_unchecked(),
            Some(vec![SqlCondition {
                star_var: "s".to_owned(),
                slot_path: vec!["hasName".to_owned()],
                condition: FilterCondition::Cmp {
                    op: crate::sparql_scoper::CmpOp::Gt,
                    value: "A".to_owned(),
                },
                reading: SlotReading::Column,
            }])
        );

        // A conjunction of pushable comparisons is the conditions of both.
        let conjunction = Expr::And(vec![pushable.clone(), pushable.clone()]);
        assert_eq!(conjunction.sql_shape_unchecked().map(|c| c.len()), Some(2));

        // A variable is not a column: until a rule has rewritten it into a
        // slot, nothing about this filter can be rendered.
        let unresolved = Expr::Compare {
            op: CompareOp::Gt,
            left: Box::new(Expr::Var("nm".to_owned())),
            right: Box::new(value.clone()),
        };
        assert_eq!(unresolved.sql_shape_unchecked(), None);

        // `!=` maps to `Ne`, which renders as its own condition -- the extra
        // `IS NOT NULL` `Ne` needs (SPARQL's inequality is false for an
        // unbound variable where SQL's `<>` on NULL is unknown) is the
        // renderer's obligation, not this shape check's; see
        // `CompareOp::as_condition`.
        let inequality = Expr::Compare {
            op: CompareOp::Ne,
            left: Box::new(slot.clone()),
            right: Box::new(value.clone()),
        };
        assert_eq!(
            inequality.sql_shape_unchecked(),
            Some(vec![SqlCondition {
                star_var: "s".to_owned(),
                slot_path: vec!["hasName".to_owned()],
                condition: FilterCondition::Ne("A".to_owned()),
                reading: SlotReading::Column,
            }])
        );

        // REGEX is held whole and declines -- unlike `!=`, nothing renders
        // it as a condition.
        let regex = Expr::Function {
            name: "REGEX".to_owned(),
            args: vec![slot.clone(), value.clone()],
        };
        assert_eq!(regex.sql_shape_unchecked(), None);

        // And a whole conjunction declines when one conjunct does: pushing
        // the half that renders moves who claims the obligation, which is a
        // rule's decision and not a rendering detail.
        let half = Expr::And(vec![pushable, regex.clone()]);
        assert_eq!(half.sql_shape_unchecked(), None);

        // Arithmetic and a disjunction are also held whole and decline.
        assert_eq!(Expr::Or(vec![regex.clone()]).sql_shape_unchecked(), None);
        assert_eq!(
            Expr::Function {
                name: "+".to_owned(),
                args: vec![slot, value]
            }
            .sql_shape_unchecked(),
            None
        );

        // Membership does render: SQL has `IN`, and it is the condition the
        // renderer already speaks.
        let members = Expr::In {
            value: Box::new(Expr::Slot {
                star_var: "s".to_owned(),
                slot_path: vec!["hasName".to_owned()],
                reading: SlotReading::Column,
                presence: SlotPresence::Required,
            }),
            candidates: vec![
                Expr::Literal(Literal::new_simple_literal("A").into()),
                Expr::Literal(Literal::new_simple_literal("B").into()),
            ],
        };
        assert_eq!(
            members.sql_shape_unchecked(),
            Some(vec![SqlCondition {
                star_var: "s".to_owned(),
                slot_path: vec!["hasName".to_owned()],
                condition: FilterCondition::In(vec!["A".to_owned(), "B".to_owned()]),
                reading: SlotReading::Column,
            }])
        );
        // ...and the public entry agrees, because these constants are the
        // terms a string column's values render as.
        let schema = crate::sparql_scoper::tests::test_schema_view();
        assert!(members.to_sql(&schema, &tunnel_star()).is_some());
    }

    // -----------------------------------------------------------------------
    // `to_sql_tree`: the within-star disjunction
    // -----------------------------------------------------------------------

    fn signal_star() -> HashMap<String, String> {
        HashMap::from([(
            "s".to_owned(),
            "https://data.infrabel.be/asset360/Signal".to_owned(),
        )])
    }

    /// An equality on one slot of `?s`, as a rule would have resolved it.
    fn slot_equals(star_var: &str, slot: &str, value: &str) -> Expr {
        Expr::Compare {
            op: CompareOp::Eq,
            left: Box::new(Expr::Slot {
                star_var: star_var.to_owned(),
                slot_path: vec![slot.to_owned()],
                reading: SlotReading::Column,
                presence: SlotPresence::Required,
            }),
            right: Box::new(Expr::Literal(Literal::new_simple_literal(value).into())),
        }
    }

    /// `Any([])` is `FALSE`, and a pushed condition selecting *nothing* is the
    /// one direction a pushed condition is not free in: the engine re-runs the
    /// query over no records and answers empty to a query that has an answer.
    ///
    /// Not reachable from a parsed query -- `flatten_or` builds at least two
    /// branches -- so the expression is built by hand, which is also the
    /// point: the shape is a legal value of the type and the refusal is the
    /// type's, not the parser's.
    ///
    /// The **nested** case is the one only this gate catches. An empty
    /// disjunction at the top has no leaves at all, so
    /// [`ConditionTree::star`] declines it as well; one buried in a
    /// conjunction leaves the other branch's leaf naming a perfectly good
    /// star, and a tree that passes every other check while selecting nothing
    /// is exactly the empty answer to an answerable query.
    #[test]
    fn to_sql_tree_declines_an_empty_disjunction() {
        let schema = crate::sparql_scoper::tests::test_schema_view();
        assert_eq!(
            Expr::And(vec![
                slot_equals("s", "name", "BX517"),
                Expr::Or(Vec::new()),
            ])
            .to_sql_tree(&schema, &signal_star()),
            None,
            "a nested FALSE makes the whole conjunction select nothing"
        );
        assert_eq!(
            Expr::Or(Vec::new()).to_sql_tree(&schema, &signal_star()),
            None
        );
        // And the emptiness is what it declined, not the shape: the same
        // disjunction with two pushable branches lifts.
        assert!(
            Expr::Or(vec![
                slot_equals("s", "name", "BX517"),
                slot_equals("s", "name", "BX518"),
            ])
            .to_sql_tree(&schema, &signal_star())
            .is_some()
        );
    }

    /// A pure conjunction stays on the [`Expr::to_sql`] fast path: the tree
    /// entry point declines it outright.
    ///
    /// Asked of the function rather than of a query because the fast path is
    /// protected twice over -- `lower_refined` and `PushComparisonFilter` both
    /// ask `to_sql` *first*, so a route change would need this gate and that
    /// ordering to fail together. This is the half that can be isolated: with
    /// no `Or` anywhere there is nothing a tree says that a list of conjuncts
    /// does not, and `filters` is read by the fetch narrowing and by star-data
    /// as well as by the statement renderer -- every one of which would have
    /// to learn the tree for nothing gained.
    #[test]
    fn to_sql_tree_declines_a_pure_conjunction() {
        let schema = crate::sparql_scoper::tests::test_schema_view();
        let above = Expr::Compare {
            op: CompareOp::Gt,
            left: Box::new(Expr::Slot {
                star_var: "s".to_owned(),
                slot_path: vec!["name".to_owned()],
                reading: SlotReading::Column,
                presence: SlotPresence::Required,
            }),
            right: Box::new(Expr::Literal(Literal::new_simple_literal("A").into())),
        };
        let conjunction = Expr::And(vec![slot_equals("s", "name", "BX517"), above]);
        assert_eq!(
            conjunction
                .to_sql(&schema, &signal_star())
                .map(|conditions| conditions.len()),
            Some(2),
            "the fast path renders it as two conditions"
        );
        assert_eq!(conjunction.to_sql_tree(&schema, &signal_star()), None);
    }

    /// **`Not` is representable and never produced**, and the walks must still
    /// descend into one.
    ///
    /// The reasons `to_sql_tree` does not build a `Not` are on
    /// [`ConditionTree`]: SQL's three-valued logic makes a negated subtree
    /// *unknown* rather than true over a NULL column, and the reading
    /// weakening a narrowing pass applies is only sound under monotone
    /// connectives. So this tree cannot be manufactured from a query today and
    /// is built by hand -- what it pins is that a leaf under a `Not` is not
    /// invisible: [`ConditionTree::leaves`] is the walk every per-leaf check
    /// uses, so a blind spot here would be a blind spot in the same-star gate
    /// and in `claims_are_backed_by_rendered_work` at once.
    #[test]
    fn a_leaf_under_a_negation_is_still_walked() {
        let leaf = |star_var: &str, slot: &str| {
            ConditionTree::Leaf(ConditionLeaf {
                condition: SqlCondition {
                    star_var: star_var.to_owned(),
                    slot_path: vec![slot.to_owned()],
                    condition: FilterCondition::Eq("BX517".to_owned()),
                    reading: SlotReading::Column,
                },
                numeric: false,
            })
        };
        let negated = |inner: ConditionTree| ConditionTree::Not(Box::new(inner));

        let one_star = ConditionTree::Any(vec![leaf("s", "name"), negated(leaf("s", "length"))]);
        assert_eq!(
            one_star.leaves().len(),
            2,
            "the negated branch is a leaf too"
        );
        assert_eq!(one_star.star(), Some("s"));

        // A second star hidden under a negation is still a second star.
        let two_stars = ConditionTree::Any(vec![leaf("s", "name"), negated(leaf("c", "hasName"))]);
        assert_eq!(two_stars.star(), None);
    }

    /// The other half of the public entry, and the reason it is the only one:
    /// a condition with the right *shape* can still be the wrong question.
    ///
    /// `sql_shape_unchecked` accepts every one of these -- they are a slot
    /// against a constant -- and `to_sql` declines them, because the stored
    /// text is not what the query wrote. A pushed condition that matches
    /// nothing is not a narrowing: the engine leg re-runs the query over no
    /// instances and answers nothing.
    #[test]
    fn to_sql_declines_a_constant_the_column_never_spells() {
        let schema = crate::sparql_scoper::tests::test_schema_view();
        let signal = HashMap::from([(
            "s".to_owned(),
            "https://data.infrabel.be/asset360/Signal".to_owned(),
        )]);
        let compare = |slot: &str, term: Term| Expr::Compare {
            op: CompareOp::Eq,
            left: Box::new(Expr::Slot {
                star_var: "s".to_owned(),
                slot_path: vec![slot.to_owned()],
                reading: SlotReading::Column,
                presence: SlotPresence::Required,
            }),
            right: Box::new(Expr::Literal(term)),
        };

        for (expr, why) in [
            (
                // `GSA` carries a `meaning`, so a record storing it renders as
                // `eul:GSA` and none answers the plain literal.
                compare("kind", Literal::new_simple_literal("GSA").into()),
                "an enum code is not the term it renders as",
            ),
            (
                compare(
                    "kind",
                    NamedNode::new_unchecked("http://ontorail.org/src/Eulynx/GSA").into(),
                ),
                "an enum column is translated backwards, not compared",
            ),
            (
                compare(
                    "name",
                    Literal::new_language_tagged_literal_unchecked("BX", "en").into(),
                ),
                "a tagged literal is a different term from the plain one",
            ),
            (
                compare(
                    "length",
                    Literal::new_typed_literal(
                        "003",
                        NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
                    )
                    .into(),
                ),
                "the stored text does not spell three as 003",
            ),
        ] {
            assert!(
                expr.sql_shape_unchecked().is_some(),
                "the shape is a column against a value: {why}"
            );
            assert_eq!(expr.to_sql(&schema, &signal), None, "{why}");
        }

        // A star the map does not name is an address nobody can resolve, so
        // even a faithful constant declines.
        assert_eq!(
            compare("name", Literal::new_simple_literal("BX").into())
                .to_sql(&schema, &HashMap::new()),
            None
        );
        // ...and with the class, it pushes.
        assert!(
            compare("name", Literal::new_simple_literal("BX").into())
                .to_sql(&schema, &signal)
                .is_some()
        );
    }

    /// Anything spargebra parses reaches a node, so a naive plan is the query
    /// and not a subset of it. The failure this prevents: an expression the
    /// tree cannot hold, which would have to be dropped, which is the silent
    /// loss the whole ledger exists to make loud.
    #[test]
    fn the_expression_tree_holds_what_spargebra_parsed() {
        let plan = plan_of(
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:length ?len ; \
             asset360:name ?nm . \
             FILTER(?len + 1 > 10 || !BOUND(?nm)) \
             FILTER(REGEX(?nm, \"^A\", \"i\")) \
             FILTER(?nm IN (\"A\", \"B\")) \
             FILTER(EXISTS { ?s asset360:kind ?k }) }",
        );
        let conditions: Vec<String> = plan
            .nodes
            .iter()
            .filter_map(|node| match &node.op {
                PlanOp::Filter { condition, .. } => Some(condition.to_string()),
                _ => None,
            })
            .collect();
        // Four nodes: spargebra conjoins the four `FILTER`s of one group, and
        // the plan splits the conjunction back into the constraints the query
        // wrote.
        assert_eq!(conditions.len(), 4, "{plan}");
        let all = conditions.join(" | ");
        for expected in ["||", "BOUND", "REGEX", "IN", "EXISTS"] {
            assert!(all.contains(expected), "{expected} missing from {all}");
        }
        // Every one of them declines even on shape alone: a naive filter has
        // variables, not columns.
        for node in &plan.nodes {
            if let PlanOp::Filter { condition, .. } = &node.op {
                assert_eq!(condition.sql_shape_unchecked(), None, "{condition}");
            }
        }
    }

    /// The printout is complete: an obligation or a node that is not in the
    /// string is not in the plan, so a reviewer reads a corpus of plans
    /// instead of the builder.
    #[test]
    fn the_plan_prints_every_node_and_obligation() {
        let plan = plan_of(
            "SELECT ?kind (COUNT(*) AS ?n) WHERE { ?s a asset360:TunnelComplex ; \
             asset360:trafficKinds ?kind } GROUP BY ?kind LIMIT 10",
        );
        let printed = plan.to_string();
        for id in 0..plan.obligations.len() {
            assert!(printed.contains(&format!("o{id}")), "o{id}\n{printed}");
        }
        for id in 0..plan.nodes.len() {
            assert!(printed.contains(&format!("n{id}")), "n{id}\n{printed}");
        }
        assert!(printed.contains("[E]"), "{printed}");
        assert!(printed.contains("asset360:TunnelComplex"), "{printed}");
        assert!(printed.contains("residual  (empty)"), "{printed}");
        println!("{printed}");
    }

    /// Only a plain join makes two matches one mandatory row set. A rule that
    /// folds them together has to be able to tell, or an `OPTIONAL` becomes an
    /// existence check and drops the rows it exists to keep.
    #[test]
    fn inner_join_groups_stop_at_an_optional() {
        let plan = plan_of(
            "SELECT ?s ?nm ?k WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { ?s asset360:name ?nm } }",
        );
        let groups = inner_join_groups(&plan);
        let matches = plan.find("match");
        assert_eq!(matches.len(), 3, "{plan}");

        // The type and the mandatory slot are joined; the optional one is not
        // in their group.
        let inside_optional = plan
            .nodes
            .iter()
            .enumerate()
            .find(|(_, node)| match &node.op {
                PlanOp::Match { pattern } => {
                    predicate_iri(pattern) == Some("https://data.infrabel.be/asset360/name")
                }
                _ => false,
            })
            .map(|(id, _)| id)
            .expect("the optional match is a node");
        let type_match = plan
            .nodes
            .iter()
            .enumerate()
            .find(|(_, node)| matches!(&node.op, PlanOp::Match { pattern } if is_type_pattern(pattern)))
            .map(|(id, _)| id)
            .expect("the type match is a node");
        let mandatory = matches
            .iter()
            .copied()
            .find(|id| *id != type_match && *id != inside_optional)
            .expect("the mandatory slot match is a node");

        assert_eq!(groups[type_match], groups[mandatory], "{plan}");
        assert_ne!(groups[type_match], groups[inside_optional], "{plan}");
    }

    /// A refined plan, to fixpoint. Mirrors `sparql_ops::tests::refined_plan`
    /// -- kept local rather than shared because that one is private to its
    /// own module's `mod tests`.
    fn refined_plan(query: &str, sv: &SchemaView) -> Plan {
        let rules = crate::sparql_rules::tier_one_rules(sv);
        let borrowed: Vec<&dyn crate::sparql_rules::Rule> =
            rules.iter().map(|rule| rule.as_ref()).collect();
        let mut plan = plan_of(query);
        crate::sparql_rules::refine(&mut plan, &borrowed).expect("every invariant holds");
        plan
    }

    /// The class each star variable was scanned as, the way
    /// `claims_are_backed_by_rendered_work` derives it from a lowered
    /// `OpTree` -- lifted here because `Expr::to_sql` needs the same map but
    /// this test works with a `Plan`, not the tree that comes after it.
    fn class_of_star(plan: &Plan) -> HashMap<String, String> {
        plan.nodes
            .iter()
            .filter_map(|node| match &node.op {
                PlanOp::Scan {
                    star_var,
                    class_uri,
                    ..
                } => Some((star_var.clone(), class_uri.clone())),
                _ => None,
            })
            .collect()
    }

    /// The refined route lifts what the scoper lifts.
    ///
    /// Two derivations of "what can SQL ask" and only one taught the new
    /// operators means the fetch narrows and the statement does not -- the
    /// difference between a query that answers and one that hits the triple
    /// limit.
    #[test]
    fn the_refined_route_lifts_the_new_operators() {
        let sv = crate::sparql_scoper::tests::test_schema_view();
        let conditions = |query: &str| -> Vec<FilterCondition> {
            let plan = refined_plan(query, &sv);
            plan.nodes
                .iter()
                .filter(|node| node.executor == Executor::Sql)
                .filter_map(|node| match &node.op {
                    PlanOp::Filter { condition, .. } => Some(condition.clone()),
                    _ => None,
                })
                .flat_map(|expr| expr.to_sql(&sv, &class_of_star(&plan)).unwrap_or_default())
                .map(|sql| sql.condition)
                .collect()
        };

        assert_eq!(
            conditions(
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(STRSTARTS(LCASE(?nm), \"bx\")) }"
            ),
            vec![FilterCondition::Like {
                value: "bx".to_owned(),
                anchor: LikeAnchor::Prefix,
                case_insensitive: true,
            }],
        );
        assert_eq!(
            conditions(
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(?nm != \"BX517\") }"
            ),
            vec![FilterCondition::Ne("BX517".to_owned())],
        );
        // `!bound` on a genuinely optional one-hop slot is the one case this
        // route actually reaches from a real parsed query: the fold-tests
        // below (`not_bound_declines_on_a_required_slot`,
        // `not_bound_declines_on_a_multi_hop_path`) had to be built by hand
        // because a *declining* filter never gets its `Var` substituted to a
        // `Slot` (see `to_sql_declines_a_substring_against_an_enum_column`'s
        // doc comment for why), but this one pushes, so pulling it from a
        // real refined plan demonstrates the whole path end to end.
        assert_eq!(
            conditions(
                "SELECT ?s WHERE { ?s a asset360:Signal . \
                 OPTIONAL { ?s asset360:name ?nm } FILTER(!BOUND(?nm)) }"
            ),
            vec![FilterCondition::NotBound],
        );
    }

    /// `!bound` on a *required* slot does not lift, on the refined route the
    /// same way it does not on the scoper's (see
    /// `sparql_scoper::tests::unbound_on_a_required_slot_does_not_lift`).
    ///
    /// The scan already carries an existence check for it, so the query
    /// selects nothing -- and "nothing" is not a condition `to_sql` can
    /// state. Built by hand rather than pulled from a refined plan, for the
    /// same reason `to_sql_declines_a_substring_against_an_enum_column` is:
    /// a query written as `?s asset360:name ?nm . FILTER(!BOUND(?nm))` never
    /// gets `?nm` substituted to a `Slot` at all (`PushComparisonFilter`
    /// only commits the rewrite together with a successful push), so pulling
    /// the filter out of that plan would decline for the unrelated "variable
    /// is not a column" reason and not isolate this gate.
    #[test]
    fn not_bound_declines_on_a_required_slot() {
        let schema = crate::sparql_scoper::tests::test_schema_view();
        let signal = HashMap::from([(
            "s".to_owned(),
            "https://data.infrabel.be/asset360/Signal".to_owned(),
        )]);
        let expr = Expr::Not(Box::new(Expr::Function {
            name: "BOUND".to_owned(),
            args: vec![Expr::Slot {
                star_var: "s".to_owned(),
                slot_path: vec!["name".to_owned()],
                reading: SlotReading::Column,
                presence: SlotPresence::Required,
            }],
        }));
        assert_eq!(expr.to_sql(&schema, &signal), None, "{expr}");

        // Load-bearing: with only the presence check in place (one hop,
        // required), the shape is otherwise exactly the one that lifts --
        // see `not_bound_lifts_on_an_optional_one_hop_slot` below for the
        // `Optional` counterpart of this same expression.
        assert!(
            expr.sql_shape_unchecked().is_none(),
            "the presence gate itself must refuse this, not just the shape: {expr}"
        );
    }

    /// `!bound` on a *multi-hop* path does not lift, regardless of whether
    /// the slot is optional -- mirroring
    /// `sparql_scoper::tests::unbound_on_a_two_hop_path_does_not_lift`, whose
    /// own comment records that its query-derived example cannot isolate the
    /// one-hop gate from the optional gate (a mandatory root cannot carry an
    /// optional two-hop leaf in that schema, so both gates would decline it
    /// there regardless).
    ///
    /// Built by hand rather than pulled from a plan, for two independent
    /// reasons: the general one (a declining filter never gets substituted,
    /// as above), and a second, refined-route-specific one found while
    /// writing this test -- a `FILTER(!BOUND(?lon))` over a nested
    /// `OPTIONAL { ?s asset360:location ?loc . ?loc asset360:longitude ?lon }`
    /// does not even fold into a `PlanOp::Scan` on this route today (checked
    /// empirically: the plan keeps `?s asset360:location ?loc` and `?loc
    /// asset360:longitude ?lon` as two separate `Engine` matches under a
    /// `leftjoin`, never a scan slot), so `?lon` stays unresolved and there
    /// is no query this route can parse today that reaches a two-hop
    /// `Expr::Slot` with `presence: Optional` at all. Building the shape by
    /// hand is not fabricating unreachable state to fake precision: the gate
    /// itself is exercised on the same `Expr::Slot` shape a future rule could
    /// legitimately produce (nothing in `Expr::Slot`'s own definition forbids
    /// a multi-hop, optional address), only today's rule set does not reach
    /// it from a query yet.
    #[test]
    fn not_bound_declines_on_a_multi_hop_path() {
        let schema = crate::sparql_scoper::tests::test_schema_view();
        let signal = HashMap::from([(
            "s".to_owned(),
            "https://data.infrabel.be/asset360/Signal".to_owned(),
        )]);
        let expr = Expr::Not(Box::new(Expr::Function {
            name: "BOUND".to_owned(),
            args: vec![Expr::Slot {
                star_var: "s".to_owned(),
                slot_path: vec!["location".to_owned(), "longitude".to_owned()],
                reading: SlotReading::Column,
                // Optional on purpose: with the one-hop gate the only one
                // that can refuse this, a failure to refuse would prove the
                // gate un-isolated from the presence check rather than
                // merely untested.
                presence: SlotPresence::Optional,
            }],
        }));
        assert_eq!(expr.to_sql(&schema, &signal), None, "{expr}");
        assert!(
            expr.sql_shape_unchecked().is_none(),
            "the one-hop gate itself must refuse this, not just the shape: {expr}"
        );
    }

    /// The `Optional` counterpart of `not_bound_declines_on_a_required_slot`:
    /// the identical one-hop shape, differing only in `presence`, lifts.
    /// Read the two together -- each changes exactly one gate from the
    /// other, so between them they show the presence check is neither always
    /// satisfied nor always refused by this shape.
    #[test]
    fn not_bound_lifts_on_an_optional_one_hop_slot() {
        let schema = crate::sparql_scoper::tests::test_schema_view();
        let signal = HashMap::from([(
            "s".to_owned(),
            "https://data.infrabel.be/asset360/Signal".to_owned(),
        )]);
        let expr = Expr::Not(Box::new(Expr::Function {
            name: "BOUND".to_owned(),
            args: vec![Expr::Slot {
                star_var: "s".to_owned(),
                slot_path: vec!["name".to_owned()],
                reading: SlotReading::Column,
                presence: SlotPresence::Optional,
            }],
        }));
        assert_eq!(
            expr.to_sql(&schema, &signal),
            Some(vec![SqlCondition {
                star_var: "s".to_owned(),
                slot_path: vec!["name".to_owned()],
                condition: FilterCondition::NotBound,
                reading: SlotReading::Column,
            }]),
            "{expr}"
        );
    }

    /// The enum-column term gate, walked through
    /// `constants_are_the_columns_terms`'s new `Function` arm rather than
    /// just `sql_shape_unchecked`'s new shape.
    ///
    /// `kind` on `asset360:Signal` is the same enum slot
    /// `sparql_scoper::tests::substring_does_not_lift_onto_an_enum_column` (the
    /// scoper's identical gate test) uses. Built by hand, the way
    /// `to_sql_declines_a_constant_the_column_never_spells` above does,
    /// rather than pulled from a refined plan: `PushComparisonFilter` only
    /// commits the `Var` -> `Slot` rewrite together with a successful push,
    /// so a filter node that `to_sql` declined still holds the query's
    /// original `Expr::Var` -- which would decline for an unrelated reason
    /// (a variable is not a column) and not isolate this gate at all.
    #[test]
    fn to_sql_declines_a_substring_against_an_enum_column() {
        let schema = crate::sparql_scoper::tests::test_schema_view();
        let signal = HashMap::from([(
            "s".to_owned(),
            "https://data.infrabel.be/asset360/Signal".to_owned(),
        )]);
        let expr = Expr::Function {
            name: "CONTAINS".to_owned(),
            args: vec![
                Expr::Slot {
                    star_var: "s".to_owned(),
                    slot_path: vec!["kind".to_owned()],
                    reading: SlotReading::Column,
                    presence: SlotPresence::Required,
                },
                Expr::Literal(Literal::new_simple_literal("GS").into()),
            ],
        };
        assert_eq!(expr.to_sql(&schema, &signal), None, "{expr}");

        // The gate is load-bearing: with the term check skipped,
        // `sql_shape_unchecked` alone accepts the same expression and would
        // push a substring test the enum column's codes never satisfy --
        // `kind` stores `GSA`/`KSS`, never a label, so
        // `object_data->>'kind' LIKE '%GS%'` selects nothing for a query
        // that has an answer.
        assert!(
            expr.sql_shape_unchecked().is_some(),
            "the shape is right; only the term check must refuse it: {expr}"
        );
    }
}
