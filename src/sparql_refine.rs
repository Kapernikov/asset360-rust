//! The naive plan: the query, faithfully, with nothing pushed down.
//!
//! [`crate::sparql_plan::plan_query`] decides pushdown once, for the query as
//! a whole: `analyse_pushdown` answers eligible or blocked, and one filter it
//! cannot express in SQL costs the entire grouping. That cliff is a property
//! of *when* the decision is made. A planner that starts from "the engine does
//! everything" and moves work down one step at a time ends wherever it runs
//! out of rules, which is partial pushdown by construction.
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
//! it is what `views.py` and `sql_builder.py` read today. Refinement replaces
//! that shape with one tree cut by a frontier, which is stage 3 of the plan in
//! `28d-plan-refinement.md`. Until the rules can carry a query as far as the
//! single-pass analysis does, two artifacts is the only way to add the
//! machinery without changing an answer: nothing here is reachable from
//! `plan_query`.
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

use std::collections::BTreeSet;
use std::fmt;

use spargebra::Query;
use spargebra::algebra::{
    AggregateExpression, Expression, GraphPattern, OrderExpression, PropertyPathExpression,
};
use spargebra::term::{GroundTerm, NamedNodePattern, Term, TermPattern, TriplePattern, Variable};

use crate::sparql_plan::{LedgerError, Obligation, ObligationId, obligation_of_triple, shorten};
use crate::sparql_scoper::{FilterCondition, ScopeError};

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
/// *pushable* set and deliberately excludes `!=`, because SPARQL's inequality
/// is false for an unbound variable where SQL's `<>` on NULL is unknown and
/// would drop rows the query keeps. A naive plan has to hold the comparison
/// the query wrote whether or not it can be pushed, so `Ne` is representable
/// here and declined by [`Expr::to_sql`].
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
}

impl Expr {
    /// The conditions this expression is worth in SQL, or `None` when SQL
    /// cannot express it.
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
    pub fn to_sql(&self) -> Option<Vec<SqlCondition>> {
        match self {
            Self::And(parts) => {
                let mut out = Vec::new();
                for part in parts {
                    out.extend(part.to_sql()?);
                }
                Some(out)
            }
            Self::Compare { op, left, right } => {
                let (slot, value) = slot_and_value(left, right)?;
                let condition = match op {
                    CompareOp::Eq => FilterCondition::Eq(value),
                    CompareOp::Lt => FilterCondition::Cmp {
                        op: crate::sparql_scoper::CmpOp::Lt,
                        value,
                    },
                    CompareOp::Lte => FilterCondition::Cmp {
                        op: crate::sparql_scoper::CmpOp::Lte,
                        value,
                    },
                    CompareOp::Gt => FilterCondition::Cmp {
                        op: crate::sparql_scoper::CmpOp::Gt,
                        value,
                    },
                    CompareOp::Gte => FilterCondition::Cmp {
                        op: crate::sparql_scoper::CmpOp::Gte,
                        value,
                    },
                    // Not an oversight: SPARQL's `!=` is false for an unbound
                    // variable, SQL's `<>` on NULL is unknown, and the two
                    // disagree about exactly the rows an OPTIONAL keeps.
                    CompareOp::Ne => return None,
                };
                let Self::Slot {
                    star_var,
                    slot_path,
                } = slot
                else {
                    return None;
                };
                Some(vec![SqlCondition {
                    star_var: star_var.clone(),
                    slot_path: slot_path.clone(),
                    condition,
                }])
            }
            Self::In { value, candidates } => {
                let Self::Slot {
                    star_var,
                    slot_path,
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
                }])
            }
            // A disjunction is not a conjunction of conditions, and a
            // negation is the `!=` problem one level up. Both are left to the
            // engine until there is a rule that renders them as one condition
            // rather than several.
            Self::Or(_)
            | Self::Not(_)
            | Self::Var(_)
            | Self::Literal(_)
            | Self::Slot { .. }
            | Self::Function { .. }
            | Self::Opaque(_) => None,
        }
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
            } => write!(f, "?{star_var}.{}", slot_path.join(".")),
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

/// One slot a [`PlanOp::Scan`] reads, and whether reading it fans out.
///
/// `multivalued` is the field the fold rule's precondition rests on. 28d
/// writes the scan as `requires [hasName, hasTrafficKind]` and observes that
/// no invariant can catch a fold that drops the unnest; with multiplicity on
/// the slot, [`Plan::fanout_restored`] can.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanSlot {
    pub slot: String,
    /// The variable the folded `match` bound to this slot's value.
    pub var: String,
    /// Whether the slot holds an array, so one record answers the query once
    /// per value.
    pub multivalued: bool,
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
    },
    /// `OPTIONAL`, as an operator rather than a plan-level node.
    LeftJoin {
        left: NodeId,
        right: NodeId,
        /// The condition spargebra lifts into the join itself.
        ///
        /// It discharges nothing, because
        /// [`crate::sparql_plan::obligations_of`] does not enumerate it --
        /// see the note on [`naive_plan`].
        condition: Option<Expr>,
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
    },
    /// One row per element of a multivalued slot, so row count matches
    /// solution count. Without it a record with three values counts once.
    Unnest {
        input: NodeId,
        star_var: String,
        slot_path: Vec<String>,
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
    at: NodeId,
    discharges: Vec<ObligationId>,
) -> Vec<Node> {
    let fanning: Vec<String> = slots
        .iter()
        .filter(|slot| slot.multivalued)
        .map(|slot| slot.slot.clone())
        .collect();
    let mut out = vec![Node::sql(
        PlanOp::Scan {
            star_var: star_var.to_owned(),
            class_uri: class_uri.to_owned(),
            slots,
        },
        discharges,
    )];
    for slot in fanning {
        let input = at + out.len() - 1;
        out.push(Node::sql(
            PlanOp::Unnest {
                input,
                star_var: star_var.to_owned(),
                slot_path: vec![slot],
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
                "n{scan} folded the multivalued slot '{slot}' without its unnest, \
                 so a record with several values counts once"
            ),
        }
    }
}

impl Plan {
    /// The root: the node every other node feeds.
    pub fn root(&self) -> Option<&Node> {
        self.nodes.last()
    }

    /// All five invariants, in the order a reader of 28d expects them.
    ///
    /// Cheap -- linear in the plan -- which is what makes checking after every
    /// single rule application affordable, and the difference between a rule
    /// chain that can be trusted and one that cannot.
    pub fn check(&self) -> Result<(), PlanDefect> {
        self.ledger_balances().map_err(PlanDefect::Ledger)?;
        self.well_formed()?;
        self.frontier_is_a_cut()?;
        self.root_matches_form()?;
        self.fanout_restored()
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
            for slot in slots.iter().filter(|slot| slot.multivalued) {
                let restored = self.nodes.iter().enumerate().any(|(id, above)| {
                    matches!(
                        &above.op,
                        PlanOp::Unnest {
                            star_var: unnest_star,
                            slot_path,
                            ..
                        } if unnest_star == star_var
                            && slot_path.as_slice() == std::slice::from_ref(&slot.slot)
                    ) && self.feeds(scan_id, id)
                });
                if !restored {
                    return Err(PlanDefect::LostFanout {
                        scan: scan_id,
                        slot: slot.slot.clone(),
                    });
                }
            }
        }
        Ok(())
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
                describe(&node.op),
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

/// The part of a node a reader needs beyond its kind.
fn describe(op: &PlanOp) -> String {
    match op {
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
        PlanOp::Join { left, right, on } => format!(
            "n{left}, n{right}  on {}",
            on.iter()
                .map(|var| format!("?{var}"))
                .collect::<Vec<_>>()
                .join(" ")
        ),
        PlanOp::LeftJoin {
            left,
            right,
            condition,
        } => match condition {
            Some(condition) => format!("n{left}, n{right}  if {condition}"),
            None => format!("n{left}, n{right}"),
        },
        PlanOp::Union { left, right } | PlanOp::Minus { left, right } => {
            format!("n{left}, n{right}")
        }
        PlanOp::Filter { condition, .. } => condition.to_string(),
        PlanOp::Bind { var, expr, .. } => format!("?{var} ← {expr}"),
        PlanOp::Group { keys, measures, .. } => format!(
            "keys=[{}] measures=[{}]",
            keys.iter()
                .map(|key| format!("?{key}"))
                .collect::<Vec<_>>()
                .join(" "),
            measures
                .iter()
                .map(|measure| format!("?{} ← {}", measure.var, measure.aggregate))
                .collect::<Vec<_>>()
                .join(", ")
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
        } => format!(
            "{} as ?{star_var}, requires [{}]",
            shorten(class_uri),
            slots
                .iter()
                .map(|slot| format!(
                    "{}→?{}{}",
                    slot.slot,
                    slot.var,
                    if slot.multivalued { "[]" } else { "" }
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PlanOp::Unnest {
            star_var,
            slot_path,
            ..
        } => format!("?{star_var}.{}", slot_path.join(".")),
        PlanOp::Construct { template, .. } => format!("{} triple(s)", template.len()),
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
/// Two consequences of reusing that enumeration, both of which are its
/// properties and not this builder's:
///
/// * `UNION`, `MINUS` and property paths are refused, because
///   `tag_triples_by_depth` refuses them. [`PlanOp::Union`], [`PlanOp::Minus`]
///   and [`PlanOp::Path`] exist so that lifting the refusal is a change to
///   obligation enumeration and not a change to the plan shape, but no query
///   reaches them through this function yet.
/// * the condition spargebra lifts into a `LeftJoin` is not enumerated, so
///   [`PlanOp::LeftJoin::condition`] discharges nothing. A plan therefore
///   cannot notice that condition being dropped -- a gap in the ledger that
///   predates this module and that fixing would change `plan_query`'s output.
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
                let left = self.pattern(left, false);
                let right = self.pattern(right, false);
                let vars: BTreeSet<String> =
                    self.vars[left].union(&self.vars[right]).cloned().collect();
                self.push(
                    PlanOp::LeftJoin {
                        left,
                        right,
                        condition: expression.as_ref().map(Expr::from),
                    },
                    Vec::new(),
                    vars,
                )
            }
            GraphPattern::Union { left, right } => {
                let left = self.pattern(left, false);
                let right = self.pattern(right, false);
                let vars: BTreeSet<String> =
                    self.vars[left].union(&self.vars[right]).cloned().collect();
                self.push(PlanOp::Union { left, right }, Vec::new(), vars)
            }
            GraphPattern::Minus { left, right } => {
                let left = self.pattern(left, false);
                let right = self.pattern(right, false);
                let vars = self.vars[left].clone();
                self.push(PlanOp::Minus { left, right }, Vec::new(), vars)
            }
            GraphPattern::Filter { expr, inner } => {
                // Claimed before descending, because that is the order the
                // enumeration pushes them: a filter's obligation comes before
                // anything under it.
                let claims = self
                    .claim_modifier(|obligation| matches!(obligation, Obligation::Filter { .. }));
                let input = self.pattern(inner, on_spine);
                let vars = self.vars[input].clone();
                self.push(
                    PlanOp::Filter {
                        input,
                        condition: Expr::from(expr),
                    },
                    claims,
                    vars,
                )
            }
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => {
                let input = self.pattern(inner, on_spine);
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
                self.push(
                    PlanOp::Values {
                        variables: variables.clone(),
                        rows: bindings.clone(),
                    },
                    Vec::new(),
                    vars,
                )
            }
        }
    }

    /// One match per triple pattern, joined left-deep.
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
        self.push(PlanOp::Join { left, right, on }, Vec::new(), vars)
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
        // One filter node, not two, and the reason is worth pinning: spargebra
        // conjoins consecutive `FILTER`s of one group graph pattern into a
        // single `Filter`, so `obligations_of` raises one filter obligation
        // for both. 28d's worked example writes them as two nodes claiming
        // `o3` and `o4` separately; they arrive as `(?nm > "A" && REGEX(...))`
        // claiming one id. See the note on [`Expr::to_sql`]: splitting the
        // conjunction is therefore also splitting an obligation.
        assert_eq!(plan.find("filter").len(), 1, "{plan}");
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
            };
            assert_eq!(claimant.op.kind(), expected, "o{id} ({obligation})\n{plan}");
        }
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
                slots: vec![ScanSlot {
                    slot: "trafficKinds".to_owned(),
                    var: "kind".to_owned(),
                    multivalued: true,
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
                slot: "hasName".to_owned(),
                var: "nm".to_owned(),
                multivalued: false,
            },
            ScanSlot {
                slot: "trafficKinds".to_owned(),
                var: "kind".to_owned(),
                multivalued: true,
            },
        ];
        let nodes = scan_with_fanout(
            "s",
            "https://data.infrabel.be/asset360/TunnelComplex",
            slots,
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

    /// `to_sql` is partial in two directions, and both are the point: by
    /// construct, and by whether a rule has yet resolved the variable to a
    /// column.
    #[test]
    fn to_sql_declines_what_sql_cannot_express() {
        let slot = Expr::Slot {
            star_var: "s".to_owned(),
            slot_path: vec!["hasName".to_owned()],
        };
        let value = Expr::Literal(Literal::new_simple_literal("A").into());

        let pushable = Expr::Compare {
            op: CompareOp::Gt,
            left: Box::new(slot.clone()),
            right: Box::new(value.clone()),
        };
        assert_eq!(
            pushable.to_sql(),
            Some(vec![SqlCondition {
                star_var: "s".to_owned(),
                slot_path: vec!["hasName".to_owned()],
                condition: FilterCondition::Cmp {
                    op: crate::sparql_scoper::CmpOp::Gt,
                    value: "A".to_owned(),
                },
            }])
        );

        // A conjunction of pushable comparisons is the conditions of both.
        let conjunction = Expr::And(vec![pushable.clone(), pushable.clone()]);
        assert_eq!(conjunction.to_sql().map(|c| c.len()), Some(2));

        // A variable is not a column: until a rule has rewritten it into a
        // slot, nothing about this filter can be rendered.
        let unresolved = Expr::Compare {
            op: CompareOp::Gt,
            left: Box::new(Expr::Var("nm".to_owned())),
            right: Box::new(value.clone()),
        };
        assert_eq!(unresolved.to_sql(), None);

        // `!=` is not `<>`: SPARQL's inequality is false for an unbound
        // variable where SQL's is unknown, so it declines rather than
        // dropping rows the query keeps.
        let inequality = Expr::Compare {
            op: CompareOp::Ne,
            left: Box::new(slot.clone()),
            right: Box::new(value.clone()),
        };
        assert_eq!(inequality.to_sql(), None);

        // And a whole conjunction declines when one conjunct does: pushing
        // the half that renders moves who claims the obligation, which is a
        // rule's decision and not a rendering detail.
        let half = Expr::And(vec![pushable, inequality]);
        assert_eq!(half.to_sql(), None);

        // REGEX, arithmetic and a disjunction are all held whole and all
        // decline.
        let regex = Expr::Function {
            name: "REGEX".to_owned(),
            args: vec![slot.clone(), value.clone()],
        };
        assert_eq!(regex.to_sql(), None);
        assert_eq!(Expr::Or(vec![regex.clone()]).to_sql(), None);
        assert_eq!(
            Expr::Function {
                name: "+".to_owned(),
                args: vec![slot, value]
            }
            .to_sql(),
            None
        );

        // Membership does render: SQL has `IN`, and it is the condition the
        // renderer already speaks.
        let members = Expr::In {
            value: Box::new(Expr::Slot {
                star_var: "s".to_owned(),
                slot_path: vec!["kind".to_owned()],
            }),
            candidates: vec![
                Expr::Literal(Literal::new_simple_literal("KSS").into()),
                Expr::Literal(NamedNode::new_unchecked("http://x/GSA").into()),
            ],
        };
        assert_eq!(
            members.to_sql(),
            Some(vec![SqlCondition {
                star_var: "s".to_owned(),
                slot_path: vec!["kind".to_owned()],
                condition: FilterCondition::In(vec!["KSS".to_owned(), "http://x/GSA".to_owned()]),
            }])
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
        // One node: the four `FILTER`s of one group are one conjunction by the
        // time spargebra is done with them.
        assert_eq!(conditions.len(), 1, "{plan}");
        let all = conditions.join(" | ");
        for expected in ["||", "BOUND", "REGEX", "IN", "EXISTS"] {
            assert!(all.contains(expected), "{expected} missing from {all}");
        }
        // Every one of them declines: a naive filter has variables, not
        // columns.
        for node in &plan.nodes {
            if let PlanOp::Filter { condition, .. } = &node.op {
                assert_eq!(condition.to_sql(), None, "{condition}");
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
}
