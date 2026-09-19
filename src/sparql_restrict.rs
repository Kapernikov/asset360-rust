//! Op 3: a restriction at a boundary, then pushed down.
//!
//! `docs/design/sparql-scopes-as-relations.md`, *Op 3*. Two rules, because
//! the semi-join argument licenses restricting the *completed* relation at
//! the join and says nothing about restricting a scan under a `Slice`:
//!
//! * [`RestrictScopeAtBoundary`] (3a) places a [`Predicate`] of `?v` at the
//!   root of a join side whose partner binds `?v` as the identity of
//!   `class`, guaranteed on both sides, with the proof made where the
//!   relation is complete: the class, `?v ∈ Identity(class)`, when the
//!   side does not type `?v` itself; and, once it does, each row test the
//!   partner applies to every row it hands the join and that reads
//!   nothing but `?v`'s record (`?v.kind = 'GSA'`). It raises a derived
//!   obligation per predicate -- the provenance the review asked for in
//!   place of a flag -- keyed by the join occurrence, side and predicate.
//! * [`PushRestrictionDown`] (3b) carries the filter one operator down at a
//!   time, each step a commutation rule of the table, stopping at a `Slice`,
//!   a keyless `Group`, a `Path`, and anything it does not know. Through a
//!   barrier the step is a *transfer* into the nested scope, recorded on
//!   the obligation; at a `Union` it *splits*, one obligation per arm. A
//!   row test that can move no further becomes SQL there, by this rule
//!   and no other.
//!
//! `FoldMatchesIntoScan` reads a class restriction as the star's type when
//! it reaches the matches, which is how an untyped body gets its scan; a
//! row test rests above that scan as the body's own filter.
//!
//! **Would a bad application be caught?** [`Plan::restriction_chains_hold`]
//! walks the *plan* from each restriction's discharge up to the join that
//! justified it and requires every node passed to be a "yes" arm for the
//! node as it is now, every barrier passed to export the variable, and
//! every node on the path to have exactly one consumer. The recorded path
//! is a log asserted against that walk.

use std::collections::BTreeSet;

use linkml_schemaview::schemaview::SchemaView;

use crate::sparql_plan::{Obligation, ObligationId};
use crate::sparql_refine::{Executor, Expr, Node, NodeId, NodeKey, Plan, PlanOp};
use crate::sparql_rules::Rule;
use crate::sparql_scopes::TermOf;

/// Which side of a join a restriction sits on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Side {
    Left,
    Right,
}

/// The join occurrence that justified a restriction: the join, by key, and
/// the side restricted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinOccurrence {
    pub node: NodeKey,
    pub side: Side,
}

/// One step op 3b took, as a log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Passed below this node, by the arm named.
    Through { rule: &'static str, node: NodeKey },
    /// Crossed into the scope this barrier wraps.
    Transfer { via: NodeKey },
    /// Split into one obligation per arm of this union.
    Split { at: NodeKey },
}

/// What a boundary restriction says of `?v`: the class of the record it
/// names, or a row test the other side applies to every row and that reads
/// nothing but that record.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// `?v` is the identity of a record of this class. The fold reads it
    /// as the star's type when 3b carries it to the matches.
    Class(String),
    /// A condition over slots of `?v`'s record -- `?v.kind = 'GSA'` -- that
    /// the other side's every row satisfies. It lands as a filter above
    /// the side's scan of `?v`, where SQL applies it; it is never the type.
    Condition(Expr),
}

impl std::fmt::Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Class(class_uri) => write!(f, "∈ {}", crate::sparql_plan::shorten(class_uri)),
            Self::Condition(condition) => write!(f, "where {condition}"),
        }
    }
}

/// A boundary restriction: the derived obligation op 3a raises.
#[derive(Debug, Clone, PartialEq)]
pub struct Restriction {
    pub var: String,
    pub predicate: Predicate,
    pub at: JoinOccurrence,
    /// The obligation this one was split from, at a `Union`.
    pub parent: Option<ObligationId>,
    pub path: Vec<Step>,
}

impl Restriction {
    /// The canonical key: what makes two restrictions the same one. The
    /// predicate by its text, so a condition's key is the condition and
    /// not the node that holds it.
    pub fn key(&self) -> (NodeKey, Side, &str, String) {
        (
            self.at.node,
            self.at.side,
            &self.var,
            match &self.predicate {
                Predicate::Class(class_uri) => class_uri.clone(),
                Predicate::Condition(condition) => format!("{condition}"),
            },
        )
    }

    /// The class this restriction carries, when it is one.
    pub fn class_uri(&self) -> Option<&str> {
        match &self.predicate {
            Predicate::Class(class_uri) => Some(class_uri),
            Predicate::Condition(_) => None,
        }
    }

    /// The filter condition that spells this restriction in a plan.
    pub fn condition(&self) -> Expr {
        match &self.predicate {
            Predicate::Class(class_uri) => Expr::InClass {
                var: self.var.clone(),
                class_uri: class_uri.clone(),
            },
            Predicate::Condition(condition) => condition.clone(),
        }
    }
}

impl std::fmt::Display for Restriction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "?{} {} at {} {:?}",
            self.var, self.predicate, self.at.node, self.at.side
        )?;
        if let Some(parent) = self.parent {
            write!(f, " (split from o{parent})")?;
        }
        // The path is a log of which steps 3b took, and two legal schedules
        // may take different ones to the same plan; it is printed by
        // [`Restriction::path_text`] and not here, so a plan printout is a
        // plan and not a history.
        Ok(())
    }
}

impl Restriction {
    /// The steps 3b logged, for a printout or a test.
    pub fn path_text(&self) -> String {
        self.path
            .iter()
            .map(|step| match step {
                Step::Through { rule, node } => format!("→ {rule}@{node}"),
                Step::Transfer { via } => format!("⇒ {via}"),
                Step::Split { at } => format!("⋔ {at}"),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// The class restriction a node spells, when it is one.
pub fn restriction_of(plan: &Plan, node: NodeId) -> Option<(String, String)> {
    match &plan.nodes[node].op {
        PlanOp::Filter {
            condition: Expr::InClass { var, class_uri },
            ..
        } => Some((var.clone(), class_uri.clone())),
        _ => None,
    }
}

/// The boundary restriction a filter node carries -- its one claim is a
/// `Boundary` obligation -- as the obligation, the variable and the
/// condition the filter holds.
pub fn restriction_filter(plan: &Plan, node: NodeId) -> Option<(ObligationId, String, Expr)> {
    let PlanOp::Filter { condition, .. } = &plan.nodes[node].op else {
        return None;
    };
    let [claim] = plan.nodes[node].discharges.as_slice() else {
        return None;
    };
    match &plan.obligations[*claim] {
        Obligation::Boundary(restriction) => {
            Some((*claim, restriction.var.clone(), condition.clone()))
        }
        _ => None,
    }
}

impl Plan {
    /// Whether a node is the filter that spells a boundary restriction.
    pub fn is_restriction_filter(&self, node: NodeId) -> bool {
        restriction_filter(self, node).is_some()
    }
}

// ---------------------------------------------------------------------------
// Op 3a
// ---------------------------------------------------------------------------

/// A filter at a join side's root, with the semi-join proof.
///
/// **Match.** A subtree *S* that is a side of a `Join`, or the **right**
/// side of a `LeftJoin`, with `?v` shared; `?v ∈ guaranteed(S)` and
/// `guaranteed(other)`; `identity_class(other, ?v) = class`; the join is
/// *S*'s only consumer; and one of two predicates is not yet carried:
///
/// * **the class**, when *S* binds `?v` as nothing a column can carry (a
///   match's binding -- a side that already binds it as an identity of
///   `class` carries the restriction, and one that binds it as a slot is a
///   reference join, the edge rules' business), and no restriction with
///   this canonical key is in the ledger;
/// * **a row test of the other side**, when *S* already produces `?v` as
///   `Identity(class)` with a scan of it visible at the insertion point:
///   each condition the other side applies to *every* row it hands the
///   join ([`conditions_on`]) and that reads nothing but `?v`'s own record
///   ([`is_record_predicate`]) -- `?v.kind = 'GSA'` -- unless *S* already
///   applies the same one or the ledger already holds it.
///
/// **Edit.** When *S* is a barrier, `input := Filter(input, p)` -- inside
/// the boundary node, so the scope root stays the barrier and the filter
/// lands above the sub-query's `Slice`, where the semi-join proof is made;
/// otherwise `S := Filter(S, p)` in the enclosing scope. The filter claims
/// the new obligation. A row test is inserted as `Sql` where the frontier
/// already is (a pushed barrier), as the engine's otherwise; 3b flips it
/// where it comes to rest.
///
/// **Equivalence.** Rows of *S* whose `?v` no row of the other side carries
/// contribute nothing to a `Join`, and nothing to a `LeftJoin` when *S* is
/// the right side (a left row with no partner is kept unchanged either
/// way). Every `?v` the other side carries is an IRI of `class`, bound in
/// every row, and satisfies every row test the other side applies on the
/// way to the join; a row test that reads only the record's own slots has
/// one value per record, so it says the same of `?v` on either side. So
/// the filter removes only rows that joined nothing.
///
/// **What it declines.** The left side of a `LeftJoin`; `Minus` and
/// `AntiJoin`; a `Union` arm as a side; a `?v` either side binds optionally;
/// a row test applied under the other side's own `OPTIONAL` or in one arm
/// of its `UNION`, on another star, on an element the other side fanned
/// out, or through a function.
pub struct RestrictScopeAtBoundary<'s> {
    schema: &'s SchemaView,
}

impl<'s> RestrictScopeAtBoundary<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }
}

/// Whether the ledger already holds a restriction with this key.
fn restriction_is_recorded(plan: &Plan, key: (NodeKey, Side, &str, String)) -> bool {
    plan.obligations.iter().any(|obligation| {
        matches!(obligation, Obligation::Boundary(restriction)
            if restriction.key() == key)
    })
}

/// Whether the ledger already holds the class restriction, or the side's
/// root already produces `?v` as the identity of `class`.
fn restriction_is_implied(
    plan: &Plan,
    schema: &SchemaView,
    side: NodeId,
    join: NodeKey,
    which: Side,
    var: &str,
    class_uri: &str,
) -> bool {
    restriction_is_recorded(plan, (join, which, var, class_uri.to_owned()))
        || (plan.guaranteed(side).contains(var)
            && plan.identity_class(schema, side, var).as_deref() == Some(class_uri))
}

/// Whether a scan of `var` as a record of `class_uri` is visible from
/// `at`: what a row test on `?var` lands on, and what the oracle reads it
/// back through.
pub(crate) fn scan_visible(plan: &Plan, at: NodeId, var: &str, class_uri: &str) -> bool {
    plan.nodes.iter().enumerate().any(|(id, node)| {
        matches!(&node.op, PlanOp::Scan { star_var, class_uri: scanned, .. }
            if star_var == var && scanned == class_uri)
            && plan.feeds_visibly(id, at)
    })
}

/// Whether an expression reads nothing but slots of `var`'s own record --
/// a value with one reading per record, the same on every scan of it.
///
/// A `BoundElement` reading is a row of the fan-out, not the record; an
/// `AnyElement` test is one the oracle cannot yet spell back, so it is not
/// carried either. A variable is not a column, a function is not audited
/// for its arguments' presence, a pattern is not an expression, and a
/// class restriction is carried by its own arm.
pub(crate) fn is_record_predicate(expr: &Expr, var: &str) -> bool {
    match expr {
        Expr::Literal(_) => true,
        Expr::Slot {
            star_var,
            slot_path,
            reading,
            ..
        } => {
            star_var == var
                && !slot_path.is_empty()
                && *reading == crate::sparql_ops::SlotReading::Column
        }
        Expr::Compare { left, right, .. } => {
            is_record_predicate(left, var) && is_record_predicate(right, var)
        }
        Expr::In { value, candidates } => {
            is_record_predicate(value, var)
                && candidates.iter().all(|c| is_record_predicate(c, var))
        }
        Expr::And(parts) | Expr::Or(parts) => parts.iter().all(|p| is_record_predicate(p, var)),
        Expr::Not(inner) => is_record_predicate(inner, var),
        Expr::Var(_) | Expr::Function { .. } | Expr::Opaque(_) | Expr::InClass { .. } => false,
    }
}

/// The row tests on `var`'s record that every row `node` produces
/// satisfies: the record predicates of the filters on every path from a
/// scan of `var` up to `node`, through the operators that pass their
/// input's rows on.
///
/// | operator | rows below reach every row above |
/// |---|---|
/// | `Filter`, `Sort`, `Distinct`, `Reduced`, `Slice`, `Unnest` | yes: a subset, an order, a dedup, a fan-out |
/// | `Bind` not binding `?v` | yes |
/// | `Project`, `SubSelect` keeping `?v` | yes |
/// | `Group` with `?v` among its keys | yes: every group is rows of one `?v` |
/// | `Join` | yes, from each side that guarantees `?v` |
/// | `LeftJoin` | from the left side only |
/// | `Minus`, `AntiJoin` | from the left side only |
/// | `Union`, a `Bind` of `?v`, a keyless `Group`, anything else | stop |
///
/// A conjunction is split into its conjuncts, so a body that spells one
/// of them is not asked for the whole.
pub(crate) fn conditions_on(plan: &Plan, node: NodeId, var: &str) -> Vec<Expr> {
    let mut out: Vec<Expr> = Vec::new();
    let mut push = |expr: &Expr| {
        let parts: Vec<&Expr> = match expr {
            Expr::And(parts) => parts.iter().collect(),
            other => vec![other],
        };
        for part in parts {
            if is_record_predicate(part, var) && !out.contains(part) {
                out.push(part.clone());
            }
        }
    };
    let below: Vec<NodeId> = match &plan.nodes[node].op {
        PlanOp::Filter { input, condition } => {
            push(condition);
            vec![*input]
        }
        PlanOp::Sort { input, .. }
        | PlanOp::Distinct { input }
        | PlanOp::Reduced { input }
        | PlanOp::Slice { input, .. }
        | PlanOp::Unnest { input, .. } => vec![*input],
        PlanOp::Bind {
            input, var: bound, ..
        } if bound != var => vec![*input],
        PlanOp::Project { input, vars, .. } | PlanOp::SubSelect { input, vars, .. }
            if vars.iter().any(|v| v == var) =>
        {
            vec![*input]
        }
        PlanOp::Group { input, keys, .. } if keys.iter().any(|k| k == var) => vec![*input],
        PlanOp::Join { left, right, .. } => [*left, *right]
            .into_iter()
            .filter(|side| plan.guaranteed(*side).contains(var))
            .collect(),
        PlanOp::LeftJoin { left, .. }
        | PlanOp::Minus { left, .. }
        | PlanOp::AntiJoin { left, .. } => {
            vec![*left]
        }
        _ => Vec::new(),
    };
    for input in below {
        for condition in conditions_on(plan, input, var) {
            push(&condition);
        }
    }
    out
}

impl Rule for RestrictScopeAtBoundary<'_> {
    fn name(&self) -> &'static str {
        "restrict_scope_at_boundary"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for id in 0..plan.nodes.len() {
            // A left join's shared variables are derived from both sides,
            // which is a walk of each: taken once the join is known to be
            // at a boundary, not for every join in the plan.
            let (left, right, shared, sides): (NodeId, NodeId, Option<Vec<String>>, Vec<Side>) =
                match &plan.nodes[id].op {
                    PlanOp::Join {
                        left, right, on, ..
                    } => (
                        *left,
                        *right,
                        Some(on.clone()),
                        vec![Side::Left, Side::Right],
                    ),
                    PlanOp::LeftJoin {
                        left,
                        right,
                        condition: None,
                        ..
                    } => (*left, *right, None, vec![Side::Right]),
                    _ => continue,
                };
            for which in sides {
                let (side, other) = match which {
                    Side::Left => (left, right),
                    Side::Right => (right, left),
                };
                // A restriction *at a boundary*: one side of the join is a
                // scope's barrier -- the sub-query beside a typed read, or
                // the untyped read beside a sub-query. A join inside one
                // scope is the fold's and the constant rules' business: the
                // star's scan already types every read of it there, through
                // `subject_site`, and a restriction beside them would be a
                // second type on the star.
                let boundary = matches!(plan.nodes[side].op, PlanOp::SubSelect { .. })
                    || matches!(plan.nodes[other].op, PlanOp::SubSelect { .. });
                if !boundary {
                    continue;
                }
                if crate::sparql_rules::consumers_of(plan, side).len() != 1 {
                    continue;
                }
                let shared: Vec<String> = match &shared {
                    Some(on) => on.clone(),
                    None => plan
                        .variables_of(left)
                        .intersection(&plan.variables_of(right))
                        .cloned()
                        .collect(),
                };
                for var in &shared {
                    if !plan.guaranteed(side).contains(var) || !plan.guaranteed(other).contains(var)
                    {
                        continue;
                    }
                    let Some(class_uri) = plan.identity_class(self.schema, other, var) else {
                        continue;
                    };
                    let join_key = plan.key_of(id);
                    // The insertion point: inside the barrier, or above the
                    // side.
                    let (input, consumer) = match &plan.nodes[side].op {
                        PlanOp::SubSelect { input, .. } => (*input, side),
                        _ => (side, id),
                    };
                    let placed = |plan: &mut Plan, predicate: Predicate, node: Node| {
                        let restriction = Restriction {
                            var: var.clone(),
                            predicate,
                            at: JoinOccurrence {
                                node: join_key,
                                side: which,
                            },
                            parent: None,
                            path: Vec::new(),
                        };
                        let obligation = plan.obligations.len();
                        plan.obligations
                            .push(Obligation::Boundary(Box::new(restriction)));
                        let mut node = node;
                        node.discharges = vec![obligation];
                        insert_between(plan, input, consumer, node);
                        // The origin scope of the new obligation: where the
                        // filter now is.
                        let filter_id = plan
                            .nodes
                            .iter()
                            .position(|node| node.discharges.contains(&obligation))
                            .expect("the filter this rule inserted");
                        let origin = plan.scope_of(filter_id).map(|barrier| plan.key_of(barrier));
                        plan.origin.push(origin);
                    };

                    // **The class.** The side binds `?v` as a match's
                    // binding, not as a column: an identity of `class` is
                    // implied, another identity is a contradiction the
                    // engine answers, and a slot is a reference join.
                    let binds_as_column = plan
                        .term_of(self.schema, side, var)
                        .iter()
                        .any(|term| !matches!(term, TermOf::Computed));
                    // A side that types `?v` itself -- an `rdf:type` match
                    // the fold has not reached yet -- needs no restriction:
                    // the fold will make the scan, and a restriction beside
                    // a type would read as two classes on one star. And the
                    // side must read `?v` as a *star* -- the subject of a
                    // match -- and never as a value: an object binding is a
                    // slot once folded, and a slot against an identity is
                    // the reference join's edge, not a restriction.
                    //
                    // A side that reads `?v` only through references to
                    // stars it scans -- `OPTIONAL { ?v :ref ?t . ?t a T … }`
                    // -- is the absorb rules' shape: the reference becomes
                    // the left join's own edge and the read a delivered
                    // column, which renders as the flat `LEFT JOIN` it always
                    // did. A restriction there would make the body scan `?v`
                    // again and join it back on identity: the same answer as
                    // a derived table, and a different statement for a shape
                    // that had a cheaper one. Any other read of `?v` -- a
                    // slot, a second star -- is the two-read shape, which the
                    // restriction serves.
                    //
                    // Either insertion point must be the engine's, or the
                    // frontier would break.
                    let absorb_shape = (only_reference_reads(plan, self.schema, side, var)
                        && absorb_reference_shape(plan, side))
                        || single_read_body(plan, side, var);
                    let class_wanted = !binds_as_column
                        && !types_itself(plan, side, var)
                        && reads_as_a_star(plan, side, var)
                        && !absorb_shape;
                    if class_wanted
                        && !restriction_is_implied(
                            plan,
                            self.schema,
                            side,
                            join_key,
                            which,
                            var,
                            &class_uri,
                        )
                        && plan.nodes[consumer].executor == Executor::Engine
                    {
                        let filter = Node::engine(
                            PlanOp::Filter {
                                input,
                                condition: Expr::InClass {
                                    var: var.clone(),
                                    class_uri: class_uri.clone(),
                                },
                            },
                            Vec::new(),
                        );
                        placed(plan, Predicate::Class(class_uri), filter);
                        return true;
                    }

                    // **A row test of the other side.** Only once the side
                    // produces `?v` as the identity of the same class, with
                    // its scan visible where the test lands: a test on a
                    // record needs the record's scan to be applied to, and
                    // to be read back by the oracle.
                    if plan.identity_class(self.schema, side, var).as_deref() != Some(&class_uri)
                        || !scan_visible(plan, input, var, &class_uri)
                    {
                        continue;
                    }
                    let applied = conditions_on(plan, input, var);
                    for condition in conditions_on(plan, other, var) {
                        if applied.contains(&condition)
                            || restriction_is_recorded(
                                plan,
                                (join_key, which, var, format!("{condition}")),
                            )
                        {
                            continue;
                        }
                        // Where the frontier already is, the filter must be
                        // `Sql` and must render there; elsewhere it is the
                        // engine's until 3b brings it to rest.
                        let filter = if plan.nodes[consumer].executor == Executor::Sql {
                            let Some(rendered) = crate::sparql_rules::render_condition_below(
                                self.schema,
                                plan,
                                input,
                                &condition,
                            ) else {
                                continue;
                            };
                            Node::sql(
                                PlanOp::Filter {
                                    input,
                                    condition: rendered,
                                },
                                Vec::new(),
                            )
                        } else {
                            Node::engine(
                                PlanOp::Filter {
                                    input,
                                    condition: condition.clone(),
                                },
                                Vec::new(),
                            )
                        };
                        placed(plan, Predicate::Condition(condition), filter);
                        return true;
                    }
                }
            }
        }
        false
    }
}

/// Whether the subtree under `side` holds an `rdf:type` match on `var` --
/// a type of the side's own, which the fold turns into a scan.
fn types_itself(plan: &Plan, side: NodeId, var: &str) -> bool {
    (0..plan.nodes.len()).any(|id| {
        plan.feeds(id, side)
            && matches!(&plan.nodes[id].op, PlanOp::Match { pattern }
                if crate::sparql_refine::subject_variable(pattern) == Some(var)
                    && crate::sparql_refine::type_class_iri(pattern).is_some())
    })
}

/// Whether every match on `var` under `side` reads a single-valued
/// reference slot into a variable the side scans as a star: the shape
/// `AbsorbOptionalReference` absorbs into the join's edge.
fn only_reference_reads(plan: &Plan, schema: &SchemaView, side: NodeId, var: &str) -> bool {
    let scanned: BTreeSet<String> = (0..plan.nodes.len())
        .filter(|id| plan.feeds(*id, side))
        .filter_map(|id| match &plan.nodes[id].op {
            PlanOp::Scan { star_var, .. } => Some(star_var.clone()),
            PlanOp::Match { pattern }
                if crate::sparql_refine::type_class_iri(pattern).is_some() =>
            {
                crate::sparql_refine::subject_variable(pattern).map(str::to_owned)
            }
            _ => None,
        })
        .collect();
    let mut any = false;
    for id in 0..plan.nodes.len() {
        if !plan.feeds(id, side) {
            continue;
        }
        let PlanOp::Match { pattern } = &plan.nodes[id].op else {
            continue;
        };
        if crate::sparql_refine::subject_variable(pattern) != Some(var) {
            continue;
        }
        any = true;
        let Some(object) = crate::sparql_refine::object_variable(pattern) else {
            return false;
        };
        if !scanned.contains(object) {
            return false;
        }
        let Some(predicate) = crate::sparql_refine::predicate_iri(pattern) else {
            return false;
        };
        let Some(slot) = schema.get_slot_by_uri(predicate).ok().flatten() else {
            return false;
        };
        if slot.get_range_class().is_none()
            || slot.determine_slot_inline_mode()
                != linkml_schemaview::slotview::SlotInlineMode::Reference
            || slot.determine_slot_container_mode()
                != linkml_schemaview::slotview::SlotContainerMode::SingleValue
        {
            return false;
        }
    }
    any
}

/// Whether `side` is an `OPTIONAL` body of the shape `AbsorbOptionalReference`
/// takes: under whatever left joins the body nests, a join of a match and
/// the rest, with nothing else (no sunk condition) between.
fn absorb_reference_shape(plan: &Plan, side: NodeId) -> bool {
    let PlanOp::SubSelect {
        input,
        domain: None,
        ..
    } = &plan.nodes[side].op
    else {
        return false;
    };
    let mut core = *input;
    while let PlanOp::LeftJoin { left, .. } = &plan.nodes[core].op {
        core = *left;
    }
    match &plan.nodes[core].op {
        PlanOp::Join { left, right, .. } => {
            matches!(plan.nodes[*left].op, PlanOp::Match { .. })
                || matches!(plan.nodes[*right].op, PlanOp::Match { .. })
        }
        _ => false,
    }
}

/// Whether `side` is an `OPTIONAL` body that is exactly one match reading a
/// slot of `var`: `AbsorbOptionalRead`'s shape, which becomes a nullable
/// column of the preserved scan and needs no scan of its own.
fn single_read_body(plan: &Plan, side: NodeId, var: &str) -> bool {
    let PlanOp::SubSelect {
        input,
        domain: None,
        ..
    } = &plan.nodes[side].op
    else {
        return false;
    };
    matches!(&plan.nodes[*input].op, PlanOp::Match { pattern }
        if crate::sparql_refine::subject_variable(pattern) == Some(var)
            && crate::sparql_refine::type_class_iri(pattern).is_none())
}

/// Whether the subtree under `side` reads `var` as a match's subject and
/// never as a match's object.
fn reads_as_a_star(plan: &Plan, side: NodeId, var: &str) -> bool {
    let mut subject = false;
    for id in 0..plan.nodes.len() {
        if !plan.feeds(id, side) {
            continue;
        }
        if let PlanOp::Match { pattern } = &plan.nodes[id].op {
            if crate::sparql_refine::object_variable(pattern) == Some(var) {
                return false;
            }
            if crate::sparql_refine::subject_variable(pattern) == Some(var) {
                subject = true;
            }
        }
    }
    subject
}

/// Insert `node` between `input` and `consumer`: the new node reads `input`,
/// and `consumer` reads the new node where it read `input`.
fn insert_between(plan: &mut Plan, input: NodeId, consumer: NodeId, mut node: Node) {
    let mut nodes: Vec<Node> = Vec::with_capacity(plan.nodes.len() + 1);
    let mut remap: Vec<Option<NodeId>> = vec![None; plan.nodes.len()];
    let mut inserted: Option<NodeId> = None;
    for (old, existing) in plan.nodes.iter().enumerate() {
        if old == consumer {
            node.op
                .map_inputs(|_| remap[input].expect("the input precedes the consumer"));
            nodes.push(node.clone());
            inserted = Some(nodes.len() - 1);
        }
        let mut op = existing.op.clone();
        op.map_inputs(|i| {
            if old == consumer && i == input {
                inserted.expect("just pushed")
            } else {
                remap[i].expect("inputs precede their node")
            }
        });
        nodes.push(Node {
            op,
            executor: existing.executor,
            output: existing.output,
            key: existing.key,
            discharges: existing.discharges.clone(),
        });
        remap[old] = Some(nodes.len() - 1);
    }
    plan.rebuild(nodes, &remap);
}

// ---------------------------------------------------------------------------
// Op 2: sink a lifted condition into the unit
// ---------------------------------------------------------------------------

/// The condition spargebra lifts out of `OPTIONAL { … FILTER(c) }` moves
/// into the body, when the body decides it alone.
///
/// **Match.** `LeftJoin { right: SubSelect(body), condition: Some(c) }`
/// where every variable of `c` is in `guaranteed(body root)` **and**
/// `c.evaluates_the_same_out_of_context()`.
///
/// **Edit.** `condition = None`; `body := Filter(body, c)`; the join's
/// claims for the condition move to the new filter, inside the scope the
/// obligation was raised in.
///
/// **Equivalence.** §18.5: `LeftJoin(Ω1, Ω2, c)` keeps `merge(μ1, μ2)` when
/// compatible and `c(merge)` holds, else `μ1`. When `c` reads only variables
/// Ω2 binds in every solution, `c(merge(μ1, μ2)) = c(μ2)`, so both sets
/// equal those of `LeftJoin(Ω1, Filter(c, Ω2))`. A `c` reading a left-only
/// variable, or one the body binds optionally, is not sinkable -- and a `c`
/// with an effect (`RAND()`) is drawn once per body row instead of once per
/// pair, so the second precondition is the rule's contract rather than a
/// renderer's later refusal.
pub struct SinkLiftedCondition;

impl Rule for SinkLiftedCondition {
    fn name(&self) -> &'static str {
        "sink_lifted_condition"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for id in 0..plan.nodes.len() {
            let PlanOp::LeftJoin {
                right,
                condition: Some(condition),
                ..
            } = &plan.nodes[id].op
            else {
                continue;
            };
            let (right, condition) = (*right, condition.clone());
            let PlanOp::SubSelect { input, .. } = &plan.nodes[right].op else {
                continue;
            };
            let input = *input;
            if plan.nodes[right].executor != Executor::Engine {
                continue;
            }
            let guaranteed = plan.guaranteed(input);
            if !crate::sparql_refine::variables_used(&condition)
                .iter()
                .all(|var| guaranteed.contains(var))
            {
                continue;
            }
            if !condition.evaluates_the_same_out_of_context() {
                continue;
            }
            let claims = std::mem::take(&mut plan.nodes[id].discharges);
            if let PlanOp::LeftJoin { condition, .. } = &mut plan.nodes[id].op {
                *condition = None;
            }
            let filter = Node::engine(PlanOp::Filter { input, condition }, claims);
            insert_between(plan, input, right, filter);
            return true;
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Op 3b
// ---------------------------------------------------------------------------

/// The restriction filter moves one operator down, by the table's arm for
/// that operator, or stops.
///
/// | operator below the filter | may the filter pass below it? |
/// |---|---|
/// | `Filter`, `Bind` (not binding `?v`), `Sort`, `Distinct`, `Reduced` | yes |
/// | `Project`, `SubSelect` exporting `?v` | yes, to the input; a barrier is a transfer |
/// | `Join` | yes, to the first side with `?v ∈ guaranteed(side)` -- for a row test, the side whose scan of `?v` is visible |
/// | `LeftJoin` | to the left side only, if `?v ∈ guaranteed(left)` |
/// | `Union` | to both arms: a split |
/// | `Minus`, `AntiJoin` | to the left side only |
/// | `Group` with `?v` among its keys | yes |
/// | `Group` without | **stop** |
/// | `Slice`, `Path`, `Service`, `Scan`, `Match`, `Values`, `Unnest` | **stop** |
///
/// Ownership: every node the filter passes must have exactly one consumer,
/// or the step declines and the restriction stays above it -- still
/// correct, since every step preserved the multiset.
///
/// **Where a row test comes to rest it becomes SQL.** A class restriction
/// is read by the fold and never rendered; a row test (`Predicate::
/// Condition`) is a filter like any other once it can move no further, and
/// this rule -- not `PushComparisonFilter`, which leaves restriction
/// filters alone -- flips it to `Sql` when the node below runs in SQL and
/// the condition renders over the scans visible there. One rule owns the
/// filter's position, so no schedule can land it above a grouping the walk
/// would have passed.
pub struct PushRestrictionDown<'s> {
    schema: &'s SchemaView,
}

impl<'s> PushRestrictionDown<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }
}

impl Rule for PushRestrictionDown<'_> {
    fn name(&self) -> &'static str {
        "push_restriction_down"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for filter in 0..plan.nodes.len() {
            let Some((claim, var, condition)) = restriction_filter(plan, filter) else {
                continue;
            };
            let is_class = matches!(condition, Expr::InClass { .. });
            let below = plan.nodes[filter].op.inputs()[0];
            if crate::sparql_rules::consumers_of(plan, below).len() != 1 {
                continue;
            }
            let below_key = plan.key_of(below);
            // A row test that sits directly above the frontier and can move
            // no further along an arm below is at rest; whether it is is
            // decided after the arms, so a stop is one place.
            let rest = |plan: &mut Plan| -> bool {
                if is_class
                    || plan.nodes[filter].executor == Executor::Sql
                    || plan.nodes[below].executor != Executor::Sql
                {
                    return false;
                }
                let Some(rendered) = crate::sparql_rules::render_condition_below(
                    self.schema,
                    plan,
                    below,
                    &condition,
                ) else {
                    return false;
                };
                plan.nodes[filter].op = PlanOp::Filter {
                    input: below,
                    condition: rendered,
                };
                plan.nodes[filter].executor = Executor::Sql;
                true
            };
            // Where the filter goes: one target, or one per arm.
            let targets: Vec<NodeId> = match &plan.nodes[below].op {
                PlanOp::Filter { input, .. }
                | PlanOp::Sort { input, .. }
                | PlanOp::Distinct { input }
                | PlanOp::Reduced { input } => vec![*input],
                PlanOp::Bind {
                    input, var: bound, ..
                } if *bound != var => vec![*input],
                PlanOp::Project { input, vars, .. } | PlanOp::SubSelect { input, vars, .. }
                    if vars.contains(&var) =>
                {
                    vec![*input]
                }
                PlanOp::Group { input, keys, .. } if keys.contains(&var) => vec![*input],
                PlanOp::Join { left, right, .. } => {
                    let side = [*left, *right].into_iter().find(|side| {
                        plan.guaranteed(*side).contains(&var)
                            && (is_class
                                || plan
                                    .identity_class(self.schema, *side, &var)
                                    .is_some_and(|class| scan_visible(plan, *side, &var, &class)))
                    });
                    match side {
                        Some(side) => vec![side],
                        None => {
                            if rest(plan) {
                                return true;
                            }
                            continue;
                        }
                    }
                }
                PlanOp::LeftJoin { left, .. } if plan.guaranteed(*left).contains(&var) => {
                    vec![*left]
                }
                PlanOp::Minus { left, .. } | PlanOp::AntiJoin { left, .. } => vec![*left],
                PlanOp::Union { left, right } => vec![*left, *right],
                _ => {
                    if rest(plan) {
                        return true;
                    }
                    continue;
                }
            };
            let is_barrier = matches!(plan.nodes[below].op, PlanOp::SubSelect { .. });
            let is_union = matches!(plan.nodes[below].op, PlanOp::Union { .. });
            let arm = match &plan.nodes[below].op {
                PlanOp::Filter { .. } => "filter",
                PlanOp::Sort { .. } => "sort",
                PlanOp::Distinct { .. } => "distinct",
                PlanOp::Reduced { .. } => "reduced",
                PlanOp::Bind { .. } => "bind",
                PlanOp::Project { .. } => "project",
                PlanOp::SubSelect { .. } => "transfer",
                PlanOp::Group { .. } => "group",
                PlanOp::Join { .. } => "join",
                PlanOp::LeftJoin { .. } => "leftjoin",
                PlanOp::Minus { .. } => "minus",
                PlanOp::AntiJoin { .. } => "antijoin",
                PlanOp::Union { .. } => "split",
                _ => unreachable!("not a target"),
            };
            // A filter moved below an `Sql` node sits on an `Sql` input and
            // must be `Sql` itself, or the frontier is no cut: a row test
            // renders over the scans visible from its new input, and a
            // class restriction -- which nothing renders -- declines the
            // step and stays above the frontier. Below an engine node the
            // filter stays the engine's.
            let moved_node =
                |plan: &Plan, input: NodeId, claims: Vec<ObligationId>| -> Option<Node> {
                    if plan.nodes[below].executor == Executor::Sql {
                        let rendered = crate::sparql_rules::render_condition_below(
                            self.schema,
                            plan,
                            input,
                            &condition,
                        )?;
                        Some(Node::sql(
                            PlanOp::Filter {
                                input,
                                condition: rendered,
                            },
                            claims,
                        ))
                    } else {
                        Some(Node::engine(
                            PlanOp::Filter {
                                input,
                                condition: condition.clone(),
                            },
                            claims,
                        ))
                    }
                };
            let mut moved: Vec<(NodeId, Node)> = Vec::with_capacity(targets.len());
            let mut declined = false;
            for target in &targets {
                match moved_node(plan, *target, Vec::new()) {
                    Some(node) => moved.push((*target, node)),
                    None => {
                        declined = true;
                        break;
                    }
                }
            }
            if declined {
                continue;
            }

            if is_union {
                // Split: the parent is discharged at the union, and one
                // child per arm is raised, each on its own path.
                let parent = match &plan.obligations[claim] {
                    Obligation::Boundary(restriction) => (**restriction).clone(),
                    _ => continue,
                };
                let mut children = Vec::new();
                for _ in &targets {
                    let mut child = parent.clone();
                    child.parent = Some(claim);
                    child.path.push(Step::Split { at: below_key });
                    children.push(plan.obligations.len());
                    plan.obligations.push(Obligation::Boundary(Box::new(child)));
                    plan.origin.push(plan.origin[claim]);
                }
                // The union takes the parent's claim; the filter goes.
                plan.nodes[below].discharges.push(claim);
                plan.nodes[below].discharges.sort_unstable();
                let filters: Vec<(NodeId, Node)> = moved
                    .into_iter()
                    .zip(children)
                    .map(|((target, mut node), child)| {
                        node.discharges = vec![child];
                        (target, node)
                    })
                    .collect();
                remove_and_insert_below(plan, filter, below, filters);
                return true;
            }

            let [target] = targets.as_slice() else {
                continue;
            };
            let target = *target;
            // Log the step on the obligation.
            if let Obligation::Boundary(restriction) = &mut plan.obligations[claim] {
                restriction.path.push(if is_barrier {
                    Step::Transfer { via: below_key }
                } else {
                    Step::Through {
                        rule: arm,
                        node: below_key,
                    }
                });
            }
            let Some((_, mut moved)) = moved.pop() else {
                unreachable!("one target, one moved filter");
            };
            moved.discharges = vec![claim];
            remove_and_insert_below(plan, filter, below, vec![(target, moved)]);
            return true;
        }
        false
    }
}

/// Remove `filter` (its consumers read `below`), and insert each new filter
/// between its target and `below`.
fn remove_and_insert_below(
    plan: &mut Plan,
    filter: NodeId,
    below: NodeId,
    inserted: Vec<(NodeId, Node)>,
) {
    let mut nodes: Vec<Node> = Vec::with_capacity(plan.nodes.len() + inserted.len());
    let mut remap: Vec<Option<NodeId>> = vec![None; plan.nodes.len()];
    let mut placed: Vec<(NodeId, NodeId)> = Vec::new(); // (target old id, new filter id)
    for (old, existing) in plan.nodes.iter().enumerate() {
        if old == filter {
            remap[old] = remap[below];
            continue;
        }
        if old == below {
            for (target, node) in &inserted {
                let mut node = node.clone();
                node.op
                    .map_inputs(|_| remap[*target].expect("the target precedes the node"));
                nodes.push(node);
                placed.push((*target, nodes.len() - 1));
            }
        }
        let mut op = existing.op.clone();
        op.map_inputs(|i| {
            if old == below
                && let Some((_, new)) = placed.iter().find(|(target, _)| *target == i)
            {
                return *new;
            }
            remap[i].expect("inputs precede their node")
        });
        nodes.push(Node {
            op,
            executor: existing.executor,
            output: existing.output,
            key: existing.key,
            discharges: existing.discharges.clone(),
        });
        remap[old] = Some(nodes.len() - 1);
    }
    plan.rebuild(nodes, &remap);
}

// ---------------------------------------------------------------------------
// The invariant: a boundary restriction's path is a chain of 3b arms
// ---------------------------------------------------------------------------

/// Why a restriction's chain does not hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainDefect {
    /// The restriction's obligation is discharged by no node.
    Undischarged { obligation: ObligationId },
    /// The join occurrence that justified it resolves to nothing.
    OriginGone { obligation: ObligationId },
    /// A node on the chain is not a "yes" arm for the variable.
    NotAnArm {
        obligation: ObligationId,
        node: NodeId,
    },
    /// A node on the chain has other than one consumer.
    Shared {
        obligation: ObligationId,
        node: NodeId,
    },
    /// The chain does not reach the justifying join's side.
    DoesNotReachTheJoin { obligation: ObligationId },
}

impl std::fmt::Display for ChainDefect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Undischarged { obligation } => {
                write!(f, "restriction o{obligation} is discharged by no node")
            }
            Self::OriginGone { obligation } => write!(
                f,
                "restriction o{obligation}'s justifying join resolves to nothing"
            ),
            Self::NotAnArm { obligation, node } => write!(
                f,
                "restriction o{obligation} passed n{node}, which is not a commutation arm for it"
            ),
            Self::Shared { obligation, node } => write!(
                f,
                "restriction o{obligation}'s path crosses n{node}, which has other than one consumer"
            ),
            Self::DoesNotReachTheJoin { obligation } => write!(
                f,
                "restriction o{obligation}'s chain does not reach the join that justified it"
            ),
        }
    }
}

impl Plan {
    /// **A boundary restriction's path is a chain of 3b arms.** For each
    /// restriction, walk *up* from its discharge through single consumers:
    /// every node passed must be a "yes" arm of the table for the node as
    /// it is now, a barrier passed must export the variable, a union passed
    /// must have discharged the parent, every node must have one consumer,
    /// and the walk must reach the justifying join's side -- resolved
    /// through `retired` when a later rule merged joins.
    pub fn restriction_chains_hold(&self) -> Result<(), ChainDefect> {
        for (obligation, entry) in self.obligations.iter().enumerate() {
            let Obligation::Boundary(restriction) = entry else {
                continue;
            };
            let Some(discharge) = self
                .nodes
                .iter()
                .position(|node| node.discharges.contains(&obligation))
            else {
                if self.residual.contains(&obligation) {
                    continue;
                }
                return Err(ChainDefect::Undischarged { obligation });
            };
            let Some(join) = self.resolve(restriction.at.node) else {
                return Err(ChainDefect::OriginGone { obligation });
            };
            let side = match (&self.nodes[join].op, restriction.at.side) {
                (PlanOp::Join { left, .. } | PlanOp::LeftJoin { left, .. }, Side::Left) => *left,
                (PlanOp::Join { right, .. } | PlanOp::LeftJoin { right, .. }, Side::Right) => {
                    *right
                }
                _ => return Err(ChainDefect::OriginGone { obligation }),
            };
            // A union's discharge of a parent is the split; the chain of
            // each child is checked from its own discharge.
            if matches!(self.nodes[discharge].op, PlanOp::Union { .. }) {
                continue;
            }
            // The walk must arrive at the side's root -- the barrier, or the
            // side subtree's root -- and every node strictly between the
            // discharge and it must be an arm.
            let var = restriction.var.as_str();
            let mut current = discharge;
            let mut visited = 0;
            loop {
                if current == side {
                    break;
                }
                visited += 1;
                if visited > self.nodes.len() {
                    return Err(ChainDefect::DoesNotReachTheJoin { obligation });
                }
                let consumers = crate::sparql_rules::consumers_of(self, current);
                let [next] = consumers.as_slice() else {
                    return Err(ChainDefect::Shared {
                        obligation,
                        node: current,
                    });
                };
                let next = *next;
                if next == side {
                    break;
                }
                let arm = match &self.nodes[next].op {
                    PlanOp::Filter { .. }
                    | PlanOp::Sort { .. }
                    | PlanOp::Distinct { .. }
                    | PlanOp::Reduced { .. } => true,
                    // The fan-out the fold emits with the scan that absorbed
                    // the restriction: a row test on the record commutes
                    // with fanning its collection out.
                    PlanOp::Unnest { .. } => true,
                    PlanOp::Bind { var: bound, .. } => bound != var,
                    PlanOp::Project { vars, .. } | PlanOp::SubSelect { vars, .. } => {
                        vars.iter().any(|v| v == var)
                    }
                    PlanOp::Group { keys, .. } => keys.iter().any(|k| k == var),
                    PlanOp::Join { .. } => self.guaranteed(current).contains(var),
                    PlanOp::LeftJoin { left, .. } => {
                        *left == current && self.guaranteed(current).contains(var)
                    }
                    PlanOp::Minus { left, .. } | PlanOp::AntiJoin { left, .. } => *left == current,
                    PlanOp::Union { .. } => {
                        // Through a split: the union discharged the parent.
                        restriction
                            .parent
                            .is_some_and(|parent| self.nodes[next].discharges.contains(&parent))
                    }
                    _ => false,
                };
                if !arm {
                    return Err(ChainDefect::NotAnArm {
                        obligation,
                        node: next,
                    });
                }
                current = next;
            }
        }
        Ok(())
    }
}

/// The variables a restriction chain may name, for a test.
pub fn restrictions_in(plan: &Plan) -> BTreeSet<String> {
    plan.obligations
        .iter()
        .filter_map(|obligation| match obligation {
            Obligation::Boundary(restriction) => Some(restriction.var.clone()),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparql_plan::{Outcome, outcome_of};
    use crate::sparql_rules::{Rule, refine, tier_one_rules};
    use crate::sparql_scoper::tests::test_schema_view;

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

    fn refined(query: &str) -> Plan {
        let schema = test_schema_view();
        let mut plan = crate::sparql_refine::naive_plan_of(&format!("{PREFIX}{query}")).unwrap();
        let rules = tier_one_rules(&schema, None);
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        refine(&mut plan, &borrowed).unwrap_or_else(|failure| panic!("{failure}\n{plan}"));
        plan.check_with(&schema)
            .unwrap_or_else(|defect| panic!("{defect}\n{plan}"));
        plan
    }

    fn restrictions(plan: &Plan) -> Vec<&Restriction> {
        plan.obligations
            .iter()
            .filter_map(|obligation| match obligation {
                Obligation::Boundary(restriction) => Some(restriction.as_ref()),
                _ => None,
            })
            .collect()
    }

    /// The untyped outer read beside a typed sub-query: 3a on the outer
    /// side (the relation column is `Identity(Signal)`, guaranteed), 3b
    /// through nothing, the fold reads the restriction as the type, and the
    /// outcome is a `Statement` -- where the scoper alone refused it and
    /// today's fallback answered it wrong.
    #[test]
    fn an_untyped_outer_read_is_typed_from_the_relation_column() {
        let plan = refined(
            "SELECT ?nm WHERE { { SELECT ?s WHERE { ?s a asset360:Signal } ORDER BY ?s LIMIT 3 } \
             ?s asset360:name ?nm }",
        );
        assert_eq!(plan.find("scan").len(), 2, "{plan}");
        assert!(
            plan.nodes.iter().all(|node| node.executor == Executor::Sql),
            "{plan}"
        );
        let found = restrictions(&plan);
        let [restriction] = found.as_slice() else {
            panic!("one restriction:\n{plan}");
        };
        assert_eq!(
            (restriction.var.as_str(), restriction.at.side),
            ("s", Side::Right)
        );
        // Discharged by the scan the fold made of it.
        let outer_scan = plan.find("scan")[1];
        assert!(
            plan.nodes[outer_scan]
                .discharges
                .iter()
                .any(|id| matches!(plan.obligations[*id], Obligation::Boundary(_))),
            "{plan}"
        );
        let schema = test_schema_view();
        assert_eq!(
            outcome_of(
                &format!(
                    "{PREFIX}SELECT ?nm WHERE {{ {{ SELECT ?s WHERE {{ ?s a asset360:Signal }} \
                     ORDER BY ?s LIMIT 3 }} ?s asset360:name ?nm }}"
                ),
                &schema,
                None
            ),
            Outcome::Statement
        );
        // The review's nested case: two grouped sub-selects joined inside a
        // third, the outer read untyped in its own domain.
        assert_eq!(
            outcome_of(
                &format!(
                    "{PREFIX}SELECT ?s ?nx ?ny ?nm WHERE {{ {{ SELECT ?s ?nx ?ny WHERE {{ \
                     {{ SELECT ?s (COUNT(?k) AS ?nx) WHERE {{ ?s a asset360:Signal ; asset360:trafficKinds ?k }} GROUP BY ?s }} \
                     {{ SELECT ?s (COUNT(?d) AS ?ny) WHERE {{ ?s a asset360:Signal ; asset360:documents ?d }} GROUP BY ?s }} }} }} \
                     ?s asset360:name ?nm }}"
                ),
                &schema,
                None
            ),
            Outcome::Statement
        );
    }

    /// **The review's op-3 counter-example** (design test 7): the restriction
    /// lands above the `Slice` and stops there, the match below stays
    /// untyped, and the outcome is `Rejected` -- with the restriction
    /// visible above the slice in the printout.
    #[test]
    fn a_restriction_stops_above_a_slice_and_the_query_is_rejected() {
        let plan = refined(
            "SELECT ?s WHERE { ?s a asset360:Signal . \
             { SELECT ?s WHERE { ?s asset360:name ?x } ORDER BY ?s LIMIT 1 } }",
        );
        let filter = plan
            .nodes
            .iter()
            .position(|node| {
                matches!(
                    &node.op,
                    PlanOp::Filter {
                        condition: Expr::InClass { .. },
                        ..
                    }
                )
            })
            .unwrap_or_else(|| panic!("the restriction stays a filter:\n{plan}"));
        let input = plan.nodes[filter].op.inputs()[0];
        assert!(
            matches!(plan.nodes[input].op, PlanOp::Slice { .. }),
            "above the slice:\n{plan}"
        );
        assert_eq!(
            plan.find("scan").len(),
            1,
            "the inner match stays untyped:\n{plan}"
        );
        let schema = test_schema_view();
        let Outcome::Rejected(message) = outcome_of(
            &format!(
                "{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal . \
                 {{ SELECT ?s WHERE {{ ?s asset360:name ?x }} ORDER BY ?s LIMIT 1 }} }}"
            ),
            &schema,
            None,
        ) else {
            panic!("rejected");
        };
        assert!(message.contains("?s (in sub-select 1)"), "{message}");
    }

    /// **Rewrite lifecycle** (design test 11): (i) 3a, 3b through the
    /// sub-query's `Project`, the fold, then `PushProjection` absorbing that
    /// `Project` -- the step's key resolves through `retired`, the
    /// recomputed chain holds, the plan lowers; (ii) 3a offered again
    /// declines on the canonical key: no duplicate obligation.
    #[test]
    fn a_restriction_survives_the_rewrites_around_it() {
        let schema = test_schema_view();
        // The typed star beside an untyped read two sub-selects down: the
        // restriction goes *into* the outer barrier, through the inner
        // barrier (a transfer, S → T → U), through the projection, to the
        // match.
        let query = "SELECT ?nm ?x WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             { SELECT ?s ?x WHERE { { SELECT ?s ?x WHERE { ?s asset360:length ?x } } } } }";
        let plan = refined(query);
        assert!(
            plan.nodes.iter().all(|node| node.executor == Executor::Sql),
            "{plan}"
        );
        let found = restrictions(&plan);
        let [restriction] = found.as_slice() else {
            panic!("one restriction:\n{plan}");
        };
        // The transfer through the barrier is logged, and the barrier is
        // still live.
        assert!(
            restriction
                .path
                .iter()
                .any(|step| matches!(step, Step::Transfer { .. })),
            "{}\n{plan}",
            restriction.path_text()
        );
        for step in &restriction.path {
            let key = match step {
                Step::Through { node, .. }
                | Step::Transfer { via: node }
                | Step::Split { at: node } => *node,
            };
            assert!(plan.resolve(key).is_some(), "{step:?} resolves\n{plan}");
        }
        // The sub-query's own `Project` was absorbed by `PushProjection`
        // (flipped, not removed): the chain still walks through it.
        plan.check_with(&schema).unwrap();
        crate::sparql_ops::lower_refined(&plan, &schema, None, None)
            .unwrap_or_else(|refusal| panic!("{refusal}\n{plan}"));
        // (ii) offered again: declines on the canonical key.
        let mut again = plan.clone();
        let rule = RestrictScopeAtBoundary::new(&schema);
        assert!(!rule.apply(&mut again), "{again}");
        assert_eq!(restrictions(&again).len(), 1);
        assert_eq!(
            outcome_of(&format!("{PREFIX}{query}"), &schema, None),
            Outcome::Statement
        );
    }

    /// (iii) A split at a union: one obligation per arm, the parent
    /// discharged at the union, each child's chain checked on its own --
    /// and a fold in one arm that removes the node that arm's step named.
    #[test]
    fn a_restriction_splits_at_a_union() {
        let schema = test_schema_view();
        let plan = refined(
            "SELECT ?nm WHERE { ?s a asset360:Signal . \
             { SELECT ?s ?nm WHERE { { ?s asset360:name ?nm } UNION { ?s asset360:hasName ?nm } } } }",
        );
        let all = restrictions(&plan);
        let parents: Vec<&&Restriction> = all.iter().filter(|r| r.parent.is_none()).collect();
        let children: Vec<&&Restriction> = all.iter().filter(|r| r.parent.is_some()).collect();
        assert_eq!(parents.len(), 1, "{plan}");
        assert_eq!(children.len(), 2, "{plan}");
        let union = plan.find("union")[0];
        assert!(
            plan.nodes[union].discharges.iter().any(
                |id| matches!(&plan.obligations[*id], Obligation::Boundary(r) if r.parent.is_none())
            ),
            "the union discharges the parent:\n{plan}"
        );
        plan.check_with(&schema).unwrap();
        // The arm reading `name` folds into a scan; the arm reading
        // `hasName` (no such slot on Signal) keeps its restriction filter.
        assert_eq!(plan.find("scan").len(), 2, "{plan}");
    }

    /// **A bad rule** pushing a restriction below a `Slice`, and one that
    /// carries it into a barrier that does not export the variable: the
    /// chain invariant fails at the rule.
    #[test]
    fn a_restriction_pushed_past_a_stop_fails_the_chain() {
        use crate::sparql_refine::PlanDefect;
        let plan = refined(
            "SELECT ?s WHERE { ?s a asset360:Signal . \
             { SELECT ?s WHERE { ?s asset360:name ?x } ORDER BY ?s LIMIT 1 } }",
        );
        let filter = plan
            .nodes
            .iter()
            .position(|node| {
                matches!(
                    &node.op,
                    PlanOp::Filter {
                        condition: Expr::InClass { .. },
                        ..
                    }
                )
            })
            .unwrap();
        let slice = plan.nodes[filter].op.inputs()[0];
        let below = plan.nodes[slice].op.inputs()[0];
        let claim = plan.nodes[filter].discharges[0];
        let (var, class_uri) = restriction_of(&plan, filter).unwrap();
        let mut bad = plan.clone();
        let moved = Node::engine(
            PlanOp::Filter {
                input: below,
                condition: Expr::InClass { var, class_uri },
            },
            vec![claim],
        );
        remove_and_insert_below(&mut bad, filter, slice, vec![(below, moved)]);
        assert!(
            matches!(
                bad.check(),
                Err(PlanDefect::Chain(ChainDefect::NotAnArm { .. }))
            ),
            "{bad}\n{:?}",
            bad.check()
        );
    }

    /// **The fallback with a hidden variable** (design test 6): the review's
    /// `COUNT` case is refused outright -- the private inner `?s` has no
    /// class in its domain -- and the alpha-renamed spelling identically;
    /// and the case where the narrowing *is* valid, an exported `?s` typed
    /// from the outside by the restriction, is a statement whose inner scan
    /// carries the class.
    #[test]
    fn a_hidden_variable_narrows_nothing_and_an_exported_one_is_typed() {
        let schema = test_schema_view();
        for inner in ["?s", "?hidden"] {
            let query = format!(
                "{PREFIX}SELECT ?s ?n WHERE {{ ?s a asset360:Signal . \
                 {{ SELECT (COUNT(*) AS ?n) WHERE {{ {inner} asset360:length 3 }} }} }}"
            );
            let Outcome::Rejected(message) = outcome_of(&query, &schema, None) else {
                panic!("{inner}: rejected");
            };
            assert!(message.contains("(in sub-select 1)"), "{message}");
        }
        let exported = format!(
            "{PREFIX}SELECT ?s ?n WHERE {{ ?s a asset360:Signal . \
             {{ SELECT ?s (COUNT(?len) AS ?n) WHERE {{ ?s asset360:length ?len }} GROUP BY ?s }} }}"
        );
        assert_eq!(outcome_of(&exported, &schema, None), Outcome::Statement);
        let plan = refined(
            "SELECT ?s ?n WHERE { ?s a asset360:Signal . \
             { SELECT ?s (COUNT(?len) AS ?n) WHERE { ?s asset360:length ?len } GROUP BY ?s } }",
        );
        assert_eq!(
            plan.find("scan").len(),
            2,
            "the inner star is scanned:\n{plan}"
        );
    }

    /// The condition the other side's every row satisfies, on the same
    /// record. Shared by the three shapes below.
    fn restriction_filters(plan: &Plan) -> Vec<String> {
        plan.nodes
            .iter()
            .filter(|node| {
                matches!(&node.op, PlanOp::Filter { .. })
                    && node
                        .discharges
                        .iter()
                        .any(|id| matches!(plan.obligations[*id], Obligation::Boundary(_)))
            })
            .map(|node| match &node.op {
                PlanOp::Filter { condition, .. } => format!("{condition}"),
                _ => unreachable!(),
            })
            .collect()
    }

    /// **#470 / #466 with an outer restriction.** The body is typed in its
    /// own domain, so the class adds nothing; what crosses the barrier is
    /// the outer scan's row test on the shared identity, `kind = GSA`,
    /// which lands as the body's own filter above its scan. Both the
    /// mandatory and the `OPTIONAL { { SELECT … } }` spelling; the outcome
    /// is a `Statement` whose relation reads the restricted class only.
    #[test]
    fn an_outer_row_test_crosses_into_a_typed_body() {
        let schema = test_schema_view();
        for (spelling, side) in [
            (
                "{ SELECT ?s (COUNT(?d) AS ?n) WHERE { ?s a asset360:Signal ; asset360:documents ?d } GROUP BY ?s }",
                Side::Right,
            ),
            (
                "OPTIONAL { { SELECT ?s (COUNT(?d) AS ?n) WHERE { ?s a asset360:Signal ; asset360:documents ?d } GROUP BY ?s } }",
                Side::Right,
            ),
        ] {
            let query = format!(
                "SELECT ?nm ?n WHERE {{ ?s a asset360:Signal ; asset360:kind <http://ontorail.org/src/Eulynx/GSA> ; asset360:name ?nm . {spelling} }}"
            );
            let plan = refined(&query);
            let found = restrictions(&plan);
            let [restriction] = found.as_slice() else {
                panic!("one restriction, the row test:\n{plan}");
            };
            assert_eq!((restriction.var.as_str(), restriction.at.side), ("s", side));
            assert!(
                matches!(&restriction.predicate, Predicate::Condition(_)),
                "{restriction}\n{plan}"
            );
            // Two filters spell the row test: the outer one and the copy in
            // the body, both `Sql`, and the copy sits above the body's scan.
            let filters = restriction_filters(&plan);
            assert_eq!(filters.len(), 1, "{plan}");
            assert!(filters[0].contains("kind"), "{filters:?}\n{plan}");
            assert!(
                plan.nodes.iter().all(|node| node.executor == Executor::Sql),
                "{plan}"
            );
            assert_eq!(
                outcome_of(&format!("{PREFIX}{query}"), &schema, None),
                Outcome::Statement,
                "{plan}"
            );
        }
    }

    /// The other direction: an untyped outer read beside a sub-select whose
    /// body filters the identity it exports. The class crosses first (the
    /// existing 3a), then the body's row test, and the outer scan carries
    /// both.
    #[test]
    fn a_body_row_test_crosses_out_to_the_outer_read() {
        let plan = refined(
            "SELECT ?nm WHERE { { SELECT ?s WHERE { ?s a asset360:Signal ; asset360:length ?len . FILTER(?len > 2) } } \
             ?s asset360:name ?nm }",
        );
        let found = restrictions(&plan);
        assert_eq!(found.len(), 2, "class and row test:\n{plan}");
        assert!(
            found.iter().any(
                |r| matches!(&r.predicate, Predicate::Condition(_)) && r.at.side == Side::Right
            ),
            "{plan}"
        );
        assert_eq!(restriction_filters(&plan).len(), 1, "{plan}");
        assert!(
            plan.nodes.iter().all(|node| node.executor == Executor::Sql),
            "{plan}"
        );
    }

    /// What is **not** carried: a row test the other side applies only
    /// conditionally -- inside its own `OPTIONAL`, in one `UNION` arm --
    /// a test on another star, and the preserved side of a `LeftJoin`
    /// (which never receives a restriction). And a body that already
    /// spells the same test gets no second copy.
    #[test]
    fn a_conditional_row_test_stays_on_its_side() {
        for query in [
            // The outer test is under the outer's own OPTIONAL.
            "SELECT ?nm ?n WHERE { ?s a asset360:Signal ; asset360:name ?nm . OPTIONAL { ?s asset360:kind <http://ontorail.org/src/Eulynx/GSA> } \
             { SELECT ?s (COUNT(?d) AS ?n) WHERE { ?s a asset360:Signal ; asset360:documents ?d } GROUP BY ?s } }",
            // The outer test is in one union arm.
            "SELECT ?nm ?n WHERE { { ?s a asset360:Signal ; asset360:name ?nm ; asset360:kind <http://ontorail.org/src/Eulynx/GSA> } UNION { ?s a asset360:Signal ; asset360:name ?nm ; asset360:length 3 } \
             { SELECT ?s (COUNT(?d) AS ?n) WHERE { ?s a asset360:Signal ; asset360:documents ?d } GROUP BY ?s } }",
            // The outer test is on another star.
            "SELECT ?nm ?n WHERE { ?s a asset360:Signal ; asset360:name ?nm ; asset360:locatedOnTrack ?t . ?t a asset360:Track ; asset360:hasName \"Main\" . \
             { SELECT ?s (COUNT(?d) AS ?n) WHERE { ?s a asset360:Signal ; asset360:documents ?d } GROUP BY ?s } }",
            // The body already spells the test: no second copy.
            "SELECT ?nm ?n WHERE { ?s a asset360:Signal ; asset360:kind <http://ontorail.org/src/Eulynx/GSA> ; asset360:name ?nm . \
             { SELECT ?s (COUNT(?d) AS ?n) WHERE { ?s a asset360:Signal ; asset360:kind <http://ontorail.org/src/Eulynx/GSA> ; asset360:documents ?d } GROUP BY ?s } }",
        ] {
            let plan = refined(query);
            assert!(
                restrictions(&plan)
                    .iter()
                    .all(|r| !matches!(&r.predicate, Predicate::Condition(_))),
                "{query}\n{plan}"
            );
        }
        // The body's test never reaches the preserved side of a left join.
        let plan = refined(
            "SELECT ?nm ?n WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             OPTIONAL { { SELECT ?s (COUNT(?d) AS ?n) WHERE { ?s a asset360:Signal ; asset360:kind <http://ontorail.org/src/Eulynx/GSA> ; asset360:documents ?d } GROUP BY ?s } } }",
        );
        assert!(restrictions(&plan).is_empty(), "{plan}");
    }

    /// **#464 with the constant on the element.** The body's `?s` is typed
    /// by the class restriction, its reads fold into a scan with the
    /// fan-out, and the constant object on the unnested element becomes a
    /// `BoundElement` filter above the unnest -- so the block is one
    /// derived table and the outcome a `Statement`. On the real schema,
    /// where the element holds the reference (its `isReference` flag is
    /// not in the fixture; `hasSequenceNumber 1` is the same shape, a
    /// constant on a slot of the element beside a reference read off it).
    #[test]
    fn a_constant_on_an_unnested_element_is_a_row_test_of_the_element() {
        use crate::sparql_scoper::tests::asset360_fixture_schema_view;
        let schema = asset360_fixture_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?s ?tn WHERE {{ ?s a asset360:TunnelComplex ; asset360:typeURI ?n . \
             OPTIONAL {{ ?s asset360:hasCoveredSection ?cs . ?cs asset360:hasSequenceNumber 1 ; asset360:belongsToTrack ?t ; \
             asset360:belongsToLine ?line . ?t a asset360:Track ; asset360:typeURI ?tn . \
             OPTIONAL {{ ?t asset360:refersToLine ?l2 }} }} }}"
        );
        assert_eq!(outcome_of(&query, &schema, None), Outcome::Statement);
        let plan = crate::sparql_plan::plan_query_refined(&query, &schema).unwrap();
        let rendered = format!("{plan}");
        assert!(
            rendered.contains("filter    hasCoveredSection.hasSequenceNumber = '1'"),
            "{rendered}"
        );
        assert!(rendered.contains("all in SQL"), "{rendered}");
    }
}
