//! Op 3: a restriction at a boundary, then pushed down.
//!
//! `docs/design/sparql-scopes-as-relations.md`, *Op 3*. Two rules, because
//! the semi-join argument licenses restricting the *completed* relation at
//! the join and says nothing about restricting a scan under a `Slice`:
//!
//! * [`RestrictScopeAtBoundary`] (3a) places `?v ∈ Identity(class)` at the
//!   root of a join side whose partner binds `?v` as the identity of
//!   `class`, guaranteed on both sides, with the proof made where the
//!   relation is complete. It raises a derived obligation -- the provenance
//!   the review asked for in place of a flag -- keyed by the join
//!   occurrence and side.
//! * [`PushRestrictionDown`] (3b) carries the filter one operator down at a
//!   time, each step a commutation rule of the table, stopping at a `Slice`,
//!   a keyless `Group`, a `Path`, and anything it does not know. Through a
//!   barrier the step is a *transfer* into the nested scope, recorded on
//!   the obligation; at a `Union` it *splits*, one obligation per arm.
//!
//! `FoldMatchesIntoScan` reads the restriction as the star's type when it
//! reaches the matches, which is how an untyped body gets its scan.
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

/// A boundary restriction: the derived obligation op 3a raises.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Restriction {
    pub var: String,
    pub class_uri: String,
    pub at: JoinOccurrence,
    /// The obligation this one was split from, at a `Union`.
    pub parent: Option<ObligationId>,
    pub path: Vec<Step>,
}

impl Restriction {
    /// The canonical key: what makes two restrictions the same one.
    pub fn key(&self) -> (NodeKey, Side, &str, &str) {
        (self.at.node, self.at.side, &self.var, &self.class_uri)
    }
}

impl std::fmt::Display for Restriction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "?{} ∈ {} at {} {:?}",
            self.var,
            crate::sparql_plan::shorten(&self.class_uri),
            self.at.node,
            self.at.side
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

/// The restriction filter's condition, when a node is one.
pub fn restriction_of(plan: &Plan, node: NodeId) -> Option<(String, String)> {
    match &plan.nodes[node].op {
        PlanOp::Filter {
            condition: Expr::InClass { var, class_uri },
            ..
        } => Some((var.clone(), class_uri.clone())),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Op 3a
// ---------------------------------------------------------------------------

/// A filter at a join side's root, with the semi-join proof.
///
/// **Match.** A subtree *S* that is a side of a `Join`, or the **right**
/// side of a `LeftJoin`, with `?v` shared; `?v ∈ guaranteed(S)` and
/// `guaranteed(other)`; `identity_class(other, ?v) = class`; *S* binds `?v`
/// as nothing a column can carry (a match's binding -- a side that already
/// binds it as an identity of `class` carries the restriction, and one that
/// binds it as a slot is a reference join, the edge rules' business);
/// no restriction with this canonical key in the ledger; and the join is
/// *S*'s only consumer.
///
/// **Edit.** When *S* is a barrier, `input := Filter(input, ?v ∈ class)` --
/// inside the boundary node, so the scope root stays the barrier and the
/// filter lands above the sub-query's `Slice`, where the semi-join proof is
/// made; otherwise `S := Filter(S, …)` in the enclosing scope. The filter
/// claims the new obligation.
///
/// **Equivalence.** Rows of *S* whose `?v` no row of the other side carries
/// contribute nothing to a `Join`, and nothing to a `LeftJoin` when *S* is
/// the right side (a left row with no partner is kept unchanged either
/// way). Every `?v` the other side carries is an IRI of `class`, bound in
/// every row; records of one URI in two classes do not exist. So the filter
/// removes only rows that joined nothing.
///
/// **What it declines.** The left side of a `LeftJoin`; `Minus` and
/// `AntiJoin`; a `Union` arm as a side; a `?v` either side binds optionally.
pub struct RestrictScopeAtBoundary<'s> {
    schema: &'s SchemaView,
}

impl<'s> RestrictScopeAtBoundary<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }
}

/// Whether the ledger already holds a restriction with this key, or the
/// side's root already produces `?v` as the identity of `class`.
fn restriction_is_implied(
    plan: &Plan,
    schema: &SchemaView,
    side: NodeId,
    join: NodeKey,
    which: Side,
    var: &str,
    class_uri: &str,
) -> bool {
    let recorded = plan.obligations.iter().any(|obligation| {
        matches!(obligation, Obligation::Boundary(restriction)
            if restriction.key() == (join, which, var, class_uri))
    });
    recorded
        || (plan.guaranteed(side).contains(var)
            && plan.identity_class(schema, side, var).as_deref() == Some(class_uri))
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
                    // The side binds `?v` as a match's binding, not as a
                    // column: an identity of `class` is implied, another
                    // identity is a contradiction the engine answers, and a
                    // slot is a reference join.
                    let binds_as_column = plan
                        .term_of(self.schema, side, var)
                        .iter()
                        .any(|term| !matches!(term, TermOf::Computed));
                    if binds_as_column {
                        continue;
                    }
                    // A side that types `?v` itself -- an `rdf:type` match
                    // the fold has not reached yet -- needs no restriction:
                    // the fold will make the scan, and a restriction beside
                    // a type would read as two classes on one star. And the
                    // side must read `?v` as a *star* -- the subject of a
                    // match -- and never as a value: an object binding is a
                    // slot once folded, and a slot against an identity is
                    // the reference join's edge, not a restriction.
                    if types_itself(plan, side, var) || !reads_as_a_star(plan, side, var) {
                        continue;
                    }
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
                    if (only_reference_reads(plan, self.schema, side, var)
                        && absorb_reference_shape(plan, side))
                        || single_read_body(plan, side, var)
                    {
                        continue;
                    }
                    let join_key = plan.key_of(id);
                    if restriction_is_implied(
                        plan,
                        self.schema,
                        side,
                        join_key,
                        which,
                        var,
                        &class_uri,
                    ) {
                        continue;
                    }
                    // The insertion point: inside the barrier, or above the
                    // side. Either must be the engine's, or the frontier
                    // would break.
                    let (input, consumer) = match &plan.nodes[side].op {
                        PlanOp::SubSelect { input, .. } => (*input, side),
                        _ => (side, id),
                    };
                    if plan.nodes[consumer].executor != Executor::Engine {
                        continue;
                    }
                    let restriction = Restriction {
                        var: var.clone(),
                        class_uri: class_uri.clone(),
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
                    let filter = Node::engine(
                        PlanOp::Filter {
                            input,
                            condition: Expr::InClass {
                                var: var.clone(),
                                class_uri,
                            },
                        },
                        vec![obligation],
                    );
                    insert_between(plan, input, consumer, filter);
                    // The origin scope of the new obligation: where the
                    // filter now is.
                    let filter_id = plan
                        .nodes
                        .iter()
                        .position(|node| node.discharges.contains(&obligation))
                        .expect("the filter this rule inserted");
                    let origin = plan.scope_of(filter_id).map(|barrier| plan.key_of(barrier));
                    plan.origin.push(origin);
                    return true;
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
/// | `Join` | yes, to the first side with `?v ∈ guaranteed(side)` |
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
pub struct PushRestrictionDown;

impl Rule for PushRestrictionDown {
    fn name(&self) -> &'static str {
        "push_restriction_down"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for filter in 0..plan.nodes.len() {
            let Some((var, class_uri)) = restriction_of(plan, filter) else {
                continue;
            };
            if plan.nodes[filter].executor != Executor::Engine {
                continue;
            }
            let below = plan.nodes[filter].op.inputs()[0];
            if crate::sparql_rules::consumers_of(plan, below).len() != 1 {
                continue;
            }
            let [claim] = plan.nodes[filter].discharges.as_slice() else {
                continue;
            };
            let claim = *claim;
            let below_key = plan.key_of(below);
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
                    let side = [*left, *right]
                        .into_iter()
                        .find(|side| plan.guaranteed(*side).contains(&var));
                    match side {
                        Some(side) => vec![side],
                        None => continue,
                    }
                }
                PlanOp::LeftJoin { left, .. } if plan.guaranteed(*left).contains(&var) => {
                    vec![*left]
                }
                PlanOp::Minus { left, .. } | PlanOp::AntiJoin { left, .. } => vec![*left],
                PlanOp::Union { left, right } => vec![*left, *right],
                _ => continue,
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
                let filters: Vec<(NodeId, Node)> = targets
                    .iter()
                    .zip(children)
                    .map(|(target, child)| {
                        (
                            *target,
                            Node::engine(
                                PlanOp::Filter {
                                    input: *target,
                                    condition: Expr::InClass {
                                        var: var.clone(),
                                        class_uri: class_uri.clone(),
                                    },
                                },
                                vec![child],
                            ),
                        )
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
            let moved = Node::engine(
                PlanOp::Filter {
                    input: target,
                    condition: Expr::InClass { var, class_uri },
                },
                vec![claim],
            );
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
}
