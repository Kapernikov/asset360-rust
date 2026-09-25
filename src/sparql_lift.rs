//! **M2's rewrites: getting the SQL part to the bottom of the plan.**
//!
//! `docs/design/sparql-schema-relations-and-row-finish.md`, *M2*. When SQL
//! cannot answer a query alone, the engine can finish over the statement's
//! *rows* instead of re-running the whole query over fetched records -- but
//! only if the SQL part is one island at the bottom of the plan, under an
//! engine region that reads nothing but those rows and the schema graph.
//! Engine-only nodes nested inside `OPTIONAL`s sit between SQL nodes and
//! split the island. Three rewrites move them up:
//!
//! * **R1** ([`LiftOptionalRightSide`]): a left join against an engine-only
//!   right side moves out of the `OPTIONAL` that encloses it.
//! * **R2** ([`LiftOptionalExtension`]): a `BIND` directly inside an
//!   `OPTIONAL` body moves above the left join.
//! * **R3** ([`PageBelowOneToOne`]): the query's `ORDER BY` / `LIMIT` /
//!   `OFFSET` move below one-to-one engine operators into the statement, and
//!   the order crosses back as data: the executor numbers the statement's
//!   rows ([`PlanOp::Number`]) and a restoring sort ([`SortOrigin::Ordinal`])
//!   reads the number.
//!
//! R1 and R2 share the **match witness**: a fresh `?__m{n}` the left join
//! binds to `true` exactly where its right side matched. The lifted work
//! reads `BOUND(?__m{n})`, which -- unlike a test on a shared variable --
//! nothing else can bind and which does not depend on what the sides share.
//! A projection that drops the witness sits directly above its last
//! consumer, built as *scope minus what it drops*, never as a keep list.
//!
//! Each rule is stated as *Match / Guards / Edit / Equivalence* on the
//! struct, and each guard's failure is a named decline in the refined
//! printout ([`declined`]). **Progress**: every application lowers
//! Φ = (islands, engine barrier depth, query modifiers above the engine),
//! compared lexicographically ([`progress`]); the refine driver checks it
//! per application. The rules exist to make `UsedRows` possible: the
//! planner keeps them only when placement reaches it, and refines again
//! without them otherwise (`crate::sparql_plan`).

use std::collections::BTreeSet;

use crate::sparql_refine::{
    Executor, Expr, LiftedExport, Node, NodeId, Plan, PlanOp, ScopeTransfer, SortOrigin, SortTerm,
    variables_used,
};
use crate::sparql_rules::Rule;

/// The rule names, as the refine log and the printouts spell them.
pub const R1: &str = "lift_optional_right_side";
pub const R2: &str = "lift_optional_extension";
pub const R3: &str = "page_below_one_to_one";
pub const R4: &str = "lift_extension_over_join";

// ---------------------------------------------------------------------------
// Facts shared by the rules
// ---------------------------------------------------------------------------

/// Every expression a node evaluates.
fn expressions(op: &PlanOp) -> Vec<&Expr> {
    match op {
        PlanOp::Filter { condition, .. } => vec![condition],
        PlanOp::Bind { expr, .. } => vec![expr],
        PlanOp::LeftJoin { condition, .. } => condition.iter().collect(),
        PlanOp::Sort { terms, .. } => terms.iter().map(|term| &term.expr).collect(),
        PlanOp::Group { having, .. } => having.iter().collect(),
        _ => Vec::new(),
    }
}

/// Whether an aggregate of a grouping is effect-free, as an expression is.
fn measures_effect_free(op: &PlanOp) -> bool {
    match op {
        PlanOp::Group { measures, .. } => measures.iter().all(|measure| match &measure.aggregate {
            spargebra::algebra::AggregateExpression::CountSolutions { .. } => true,
            spargebra::algebra::AggregateExpression::FunctionCall { expr, .. } => {
                Expr::from(expr).evaluates_the_same_out_of_context()
            }
        }),
        _ => true,
    }
}

/// Every name any node of the plan mentions: what a fresh name must avoid.
/// The parser accepts `?__x`, so a query may already use one.
fn names(plan: &Plan) -> BTreeSet<String> {
    let mut out: BTreeSet<String> = BTreeSet::new();
    for set in plan.variables_table() {
        out.extend(set);
    }
    for node in &plan.nodes {
        for expr in expressions(&node.op) {
            out.extend(variables_used(expr));
        }
        match &node.op {
            PlanOp::Project { vars, .. } | PlanOp::SubSelect { vars, .. } => {
                out.extend(vars.iter().cloned());
            }
            PlanOp::LeftJoin {
                witness: Some(witness),
                ..
            } => {
                out.insert(witness.clone());
            }
            _ => {}
        }
    }
    out
}

/// A name no node of the plan mentions: `stem` itself when free (`__ord`),
/// else `stem{n}` for the smallest free `n`. With `numbered`, always
/// `stem{n}` (`__m1`, `__m2`, …).
pub fn fresh(plan: &Plan, stem: &str, numbered: bool) -> String {
    let taken = names(plan);
    if !numbered && !taken.contains(stem) {
        return stem.to_owned();
    }
    (1..)
        .map(|n| format!("{stem}{n}"))
        .find(|candidate| !taken.contains(candidate))
        .expect("an unbounded range has a free name")
}

fn consumers(plan: &Plan, id: NodeId) -> Vec<NodeId> {
    crate::sparql_rules::consumers_of(plan, id)
}

/// The nodes of the subtree rooted at `root`.
fn subtree(plan: &Plan, root: NodeId) -> Vec<NodeId> {
    plan.reaching(root)
        .into_iter()
        .enumerate()
        .filter(|(_, reached)| *reached)
        .map(|(id, _)| id)
        .collect()
}

fn var_list(vars: &BTreeSet<String>) -> String {
    vars.iter()
        .map(|var| format!("?{var}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// **The engine region reads no instance data, inside its expressions
/// too** (M2 eligibility check 2). Over the nodes `region`: a `Match` or
/// `Path` outside a `GRAPH` naming the schema graph, a `GRAPH ?g`, another
/// graph, a `Service`, a `Scan` or an `Unnest` read instances; so does an
/// expression holding an `Opaque` (`EXISTS` substitutes the mapping into a
/// pattern over the default graph) or an `InClass` (`EXISTS { ?v a C }`).
/// `Err` names the first node that reads one.
pub fn reads_schema_only(
    plan: &Plan,
    region: &[NodeId],
    schema_graph_iri: Option<&str>,
) -> Result<(), String> {
    let mut in_schema_graph: BTreeSet<NodeId> = BTreeSet::new();
    for &id in region {
        if let PlanOp::Graph { input, name } = &plan.nodes[id].op
            && schema_graph_iri.is_some_and(|iri| name.trim_matches(['<', '>']) == iri)
        {
            in_schema_graph.extend(subtree(plan, *input));
        }
    }
    for &id in region {
        let op = &plan.nodes[id].op;
        match op {
            PlanOp::Match { .. } | PlanOp::Path { .. } if !in_schema_graph.contains(&id) => {
                return Err(format!("n{id} reads a triple of the default graph"));
            }
            PlanOp::Graph { name, .. }
                if schema_graph_iri.is_none_or(|iri| name.trim_matches(['<', '>']) != iri) =>
            {
                return Err(format!(
                    "n{id} reads the graph {name}, not the schema graph"
                ));
            }
            PlanOp::Service { .. } => return Err(format!("n{id} is a SERVICE")),
            PlanOp::Scan { .. } | PlanOp::Unnest { .. } => {
                return Err(format!("n{id} reads records"));
            }
            _ => {}
        }
        for expr in expressions(op) {
            if expr.contains_an_opaque_subquery() || contains_in_class(expr) {
                return Err(format!(
                    "n{id} evaluates an expression that reads a pattern (EXISTS)"
                ));
            }
        }
    }
    Ok(())
}

fn contains_in_class(expr: &Expr) -> bool {
    match expr {
        Expr::InClass { .. } => true,
        Expr::Compare { left, right, .. } => contains_in_class(left) || contains_in_class(right),
        Expr::In { value, candidates } => {
            contains_in_class(value) || candidates.iter().any(contains_in_class)
        }
        Expr::And(parts) | Expr::Or(parts) | Expr::Function { args: parts, .. } => {
            parts.iter().any(contains_in_class)
        }
        Expr::Not(inner) => contains_in_class(inner),
        Expr::Var(_) | Expr::Literal(_) | Expr::Slot { .. } | Expr::Opaque(_) => false,
    }
}

/// Whether every expression on every node of `nodes` is effect-free.
pub fn effect_free(plan: &Plan, nodes: &[NodeId]) -> bool {
    nodes.iter().all(|id| {
        let op = &plan.nodes[*id].op;
        expressions(op)
            .iter()
            .all(|expr| expr.evaluates_the_same_out_of_context())
            && measures_effect_free(op)
    })
}

// ---------------------------------------------------------------------------
// The progress measure and the ordinal's path
// ---------------------------------------------------------------------------

/// A node the measure does not count: a projection, a barrier, the
/// executor's numbering, the restoring sort -- bookkeeping, which evaluates
/// nothing a lift could move.
fn bookkeeping(op: &PlanOp) -> bool {
    matches!(
        op,
        PlanOp::Project { .. }
            | PlanOp::SubSelect { .. }
            | PlanOp::Number { .. }
            | PlanOp::Sort {
                origin: SortOrigin::Ordinal,
                ..
            }
    )
}

/// **Φ = (I, D, S, J)**, compared lexicographically, which every
/// application of R1–R4 lowers strictly:
///
/// * `I`, the number of SQL islands (an SQL node no SQL node reads roots
///   one);
/// * `D`, the sum over the engine nodes that evaluate something of the
///   number of `OPTIONAL`-body barriers above each -- how deep engine work
///   sits inside `OPTIONAL` bodies. A projection, a barrier, the numbering
///   and the restoring sort are not counted: a lift *introduces* the
///   witness-dropping projection, and counting it would charge the rule for
///   its own bookkeeping;
/// * `S`, the number of the query's own `Sort` and `Slice` nodes the engine
///   evaluates;
/// * `J`, the sum over the same evaluating engine nodes of the number of
///   joins and left joins above each -- how many joins engine work keeps
///   in the engine. The fourth component is not the design's: it is what
///   [`LiftExtensionOverJoin`] lowers, the lift the integration found the
///   design's composition needed (see there).
///
/// R2 moves one `Bind` up one barrier (`D` − 1); R1 moves `C` up a barrier
/// and removes the inner left join (`D` − 2 at least); R3 moves the query's
/// `Sort`/`Slice` into the statement (`S` − 1 or − 2); R4 moves a `Bind`
/// above one join (`J` − 1). `Φ` lives in ℕ⁴, which is well-ordered
/// lexicographically, so the rules fire finitely often.
pub fn progress(plan: &Plan) -> (usize, usize, usize, usize) {
    let islands = (0..plan.nodes.len())
        .filter(|id| {
            plan.nodes[*id].executor == Executor::Sql
                && !consumers(plan, *id)
                    .iter()
                    .any(|consumer| plan.nodes[*consumer].executor == Executor::Sql)
        })
        .count();
    let evaluating = |id: NodeId| {
        plan.nodes[id].executor == Executor::Engine && !bookkeeping(&plan.nodes[id].op)
    };
    let mut depth = 0usize;
    let mut under_joins = 0usize;
    for node in &plan.nodes {
        match &node.op {
            PlanOp::SubSelect {
                input,
                domain: None,
                ..
            } => {
                depth += subtree(plan, *input)
                    .into_iter()
                    .filter(|id| evaluating(*id))
                    .count();
            }
            PlanOp::Join { left, right, .. } | PlanOp::LeftJoin { left, right, .. } => {
                let below: BTreeSet<NodeId> = subtree(plan, *left)
                    .into_iter()
                    .chain(subtree(plan, *right))
                    .collect();
                under_joins += below.into_iter().filter(|id| evaluating(*id)).count();
            }
            _ => {}
        }
    }
    let modifiers = plan
        .nodes
        .iter()
        .filter(|node| {
            node.executor == Executor::Engine
                && matches!(
                    node.op,
                    PlanOp::Sort {
                        origin: SortOrigin::Query,
                        ..
                    } | PlanOp::Slice { .. }
                )
        })
        .count();
    (islands, depth, modifiers, under_joins)
}

/// **The ordinal reaches its sort.** For every [`PlanOp::Number`], each node
/// from it up to the [`SortOrigin::Ordinal`] sort that reads its variable
/// has the variable in scope, and the sort is reached. Checked after every
/// rule application by the refine driver: a projection built as a keep list
/// between the two drops the ordinal, and the restoring sort would then
/// order by nothing.
pub fn ordinal_reaches_its_sort(plan: &Plan) -> Result<(), crate::sparql_scopes::TransitionDefect> {
    for (number, node) in plan.nodes.iter().enumerate() {
        let PlanOp::Number { var, .. } = &node.op else {
            continue;
        };
        let lost = |at: NodeId| crate::sparql_scopes::TransitionDefect::OrdinalLost {
            node: plan.key_of(at),
            var: var.clone(),
        };
        let mut current = number;
        loop {
            if !plan.variables_of(current).contains(var) {
                return Err(lost(current));
            }
            if let PlanOp::Sort {
                origin: SortOrigin::Ordinal,
                terms,
                ..
            } = &plan.nodes[current].op
                && terms
                    .iter()
                    .any(|term| matches!(&term.expr, Expr::Var(name) if name == var))
            {
                break;
            }
            let above = consumers(plan, current);
            let [next] = above.as_slice() else {
                return Err(lost(current));
            };
            current = *next;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Edits
// ---------------------------------------------------------------------------

/// The plan's nodes with `removed` dropped (each consumer reading what the
/// removed node read, through `through`), `inserted` placed directly after
/// `after` (reading new positions: `Insert::Input` names an old node, and
/// `Insert::Previous` the node inserted just before it), and every old
/// consumer of `after` other than the inserted ones reading the last
/// inserted node instead. Returns the new positions of the inserted nodes.
struct Edit {
    removed: Vec<(NodeId, NodeId)>,
    after: NodeId,
    inserted: Vec<Node>,
}

/// The input of an inserted node, before the rebuild.
const PREVIOUS: NodeId = usize::MAX;

fn apply_edit(plan: &mut Plan, edit: Edit) -> Vec<NodeId> {
    let old_len = plan.nodes.len();
    let mut nodes: Vec<Node> = Vec::with_capacity(old_len + edit.inserted.len());
    let mut remap: Vec<Option<NodeId>> = vec![None; old_len];
    let mut inserted_at: Vec<NodeId> = Vec::new();
    // Positions first.
    for (old, node) in plan.nodes.iter().enumerate() {
        if edit.removed.iter().any(|(removed, _)| *removed == old) {
            continue;
        }
        nodes.push(node.clone());
        remap[old] = Some(nodes.len() - 1);
        if old == edit.after {
            for node in &edit.inserted {
                nodes.push(node.clone());
                inserted_at.push(nodes.len() - 1);
            }
        }
    }
    // A removed node's consumers read what it stood for.
    let resolve = |mut old: NodeId, remap: &[Option<NodeId>]| -> NodeId {
        loop {
            if let Some(new) = remap[old] {
                return new;
            }
            let (_, through) = edit
                .removed
                .iter()
                .find(|(removed, _)| *removed == old)
                .expect("a node without a position was removed");
            old = *through;
        }
    };
    let after_new = remap[edit.after].expect("the anchor is kept");
    let top = inserted_at.last().copied();
    let mut new_index = 0usize;
    let mut old_of_new: Vec<Option<NodeId>> = vec![None; nodes.len()];
    for (old, slot) in remap.iter().enumerate() {
        if let Some(new) = slot {
            old_of_new[*new] = Some(old);
        }
    }
    for (position, node) in nodes.iter_mut().enumerate() {
        match old_of_new[position] {
            Some(_) => {
                node.op.map_inputs(|input| {
                    if input == edit.after {
                        top.unwrap_or(after_new)
                    } else {
                        resolve(input, &remap)
                    }
                });
            }
            None => {
                // An inserted node: its inputs name old nodes, or the node
                // inserted just before it.
                let previous = if new_index == 0 {
                    after_new
                } else {
                    inserted_at[new_index - 1]
                };
                node.op.map_inputs(|input| {
                    if input == PREVIOUS {
                        previous
                    } else {
                        resolve(input, &remap)
                    }
                });
                new_index += 1;
            }
        }
    }
    // A removed node's work went to the node it stood for, unless the rule
    // says otherwise (it retires it by hand after the rebuild).
    let mut rebuild_remap = remap.clone();
    for (removed, through) in &edit.removed {
        rebuild_remap[*removed] = Some(resolve(*through, &remap));
    }
    plan.rebuild(nodes, &rebuild_remap);
    inserted_at
}

fn bound_witness(witness: &str) -> Expr {
    Expr::Function {
        name: "BOUND".to_owned(),
        args: vec![Expr::Var(witness.to_owned())],
    }
}

// ---------------------------------------------------------------------------
// R2: an extension over an optional side moves up
// ---------------------------------------------------------------------------

/// **R2.** `A ⟕ SubSelect∅(Extend(B, ?x, e))` →
/// `π₋ₘ( Extend(A ⟕ₘ SubSelect∅(B), ?x, IF(BOUND(?m), e, ?__never)) )`.
///
/// **Match.** An engine `LeftJoin` whose right input is an engine barrier
/// directly over a `Bind`.
///
/// **Guards.** The barrier opens no naming domain (a sub-`SELECT`'s
/// modifiers and private variables stay inside); **G2d** the left join has
/// no condition (the original evaluates it with `?x` available); **G2a**
/// `?x ∉ scope(A)`; **G2b** `e` holds no `EXISTS`, and `vars(e) ∩ scope(A) ⊆
/// bound(B)` -- input preservation; **G2c** `e` is effect-free; **G2e** `A`
/// and `B` are SQL.
///
/// **Equivalence.** For a row `a` matched by `b`: by G2b every variable of
/// `e` has the same binding in `a ⊕ b` as in `b`, `BOUND(?m)` holds, and by
/// G2c the same inputs give the same value. For an unmatched `a`, `?m` is
/// unbound, the `IF` evaluates the fresh `?__never`, which errors, and `?x`
/// stays unbound -- what the original gives. `Extend` is one row in, one
/// out. No strictness of `e` is used.
///
/// **Edit.** The barrier's `vars` loses `?x` and gains `vars(e)` it can
/// export; the join gains the witness (or reuses its own); the lifted
/// `Bind` sits directly above the join; a projection dropping the witness
/// sits above it, built as scope minus the witness. An existing witness is
/// reused, and so is the projection above its last consumer.
pub struct LiftOptionalExtension;

struct R2Match {
    join: NodeId,
    barrier: NodeId,
    bind: NodeId,
    a: NodeId,
    b: NodeId,
    var: String,
    expr: Expr,
}

impl LiftOptionalExtension {
    fn candidate(plan: &Plan, join: NodeId) -> Option<Result<R2Match, String>> {
        let node = &plan.nodes[join];
        let PlanOp::LeftJoin {
            left,
            right,
            condition,
            ..
        } = &node.op
        else {
            return None;
        };
        if node.executor != Executor::Engine {
            return None;
        }
        let PlanOp::SubSelect { input, domain, .. } = &plan.nodes[*right].op else {
            return None;
        };
        let PlanOp::Bind {
            input: b,
            var,
            expr,
        } = &plan.nodes[*input].op
        else {
            return None;
        };
        let (a, barrier, bind, b) = (*left, *right, *input, *b);
        Some((|| {
            if let Some(domain) = domain {
                return Err(format!(
                    "the body is a sub-SELECT (domain {domain}): its modifiers and private \
                     variables stay inside"
                ));
            }
            if condition.is_some() {
                return Err(
                    "G2d: the left join carries a condition, which the original evaluates with \
                     the extension in scope"
                        .to_owned(),
                );
            }
            let scope_a = plan.variables_of(a);
            if scope_a.contains(var) {
                return Err(format!("G2a: ?{var} is in scope in n{a}"));
            }
            if expr.contains_an_opaque_subquery() {
                return Err("G2b: the expression reads a pattern (EXISTS)".to_owned());
            }
            let bound_b = plan.definitely_bound_of(b);
            let leaked: BTreeSet<String> = variables_used(expr)
                .into_iter()
                .filter(|used| scope_a.contains(used) && !bound_b.contains(used))
                .collect();
            if !leaked.is_empty() {
                return Err(format!(
                    "G2b: {} in scope in n{a} and not bound in every solution of n{b}",
                    var_list(&leaked)
                ));
            }
            if !expr.evaluates_the_same_out_of_context() {
                return Err("G2c: the expression is not effect-free".to_owned());
            }
            for (side, name) in [(a, "A"), (b, "B")] {
                if plan.nodes[side].executor != Executor::Sql {
                    return Err(format!("G2e: {name} (n{side}) is not SQL"));
                }
            }
            Ok(R2Match {
                join,
                barrier,
                bind,
                a,
                b,
                var: var.clone(),
                expr: expr.clone(),
            })
        })())
    }

    pub fn declined(plan: &Plan) -> Vec<(NodeId, String)> {
        (0..plan.nodes.len())
            .filter_map(|join| match Self::candidate(plan, join)? {
                Err(why) => Some((join, why)),
                Ok(_) => None,
            })
            .collect()
    }
}

impl Rule for LiftOptionalExtension {
    fn name(&self) -> &'static str {
        R2
    }

    fn measured(&self) -> bool {
        true
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        let Some(found) =
            (0..plan.nodes.len()).find_map(|join| Self::candidate(plan, join).and_then(Result::ok))
        else {
            return false;
        };
        let R2Match {
            join,
            barrier,
            bind,
            a,
            b,
            var,
            expr,
        } = found;
        let existing = match &plan.nodes[join].op {
            PlanOp::LeftJoin { witness, .. } => witness.clone(),
            _ => unreachable!("matched a left join"),
        };
        let reused = existing.is_some();
        let witness = existing.unwrap_or_else(|| fresh(plan, "__m", true));
        let never = fresh(plan, "__never", false);
        let inputs: BTreeSet<String> = variables_used(&expr).into_iter().collect();
        let scope_b = plan.variables_of(b);
        let bind_key = plan.key_of(bind);
        let barrier_key = plan.key_of(barrier);
        let b_key = plan.key_of(b);
        let bind_claims = plan.nodes[bind].discharges.clone();

        // The barrier: loses `?x`, gains what `e` reads that the body binds.
        if let PlanOp::SubSelect { vars, .. } = &mut plan.nodes[barrier].op {
            vars.retain(|exported| *exported != var);
            for used in &inputs {
                if scope_b.contains(used) && !vars.contains(used) {
                    vars.push(used.clone());
                }
            }
        }
        if let PlanOp::LeftJoin { witness: slot, .. } = &mut plan.nodes[join].op {
            *slot = Some(witness.clone());
        }

        let lifted = Node::engine(
            PlanOp::Bind {
                input: PREVIOUS,
                var: var.clone(),
                expr: Expr::Function {
                    name: "IF".to_owned(),
                    args: vec![bound_witness(&witness), expr, Expr::Var(never)],
                },
            },
            bind_claims,
        );
        let mut inserted = vec![lifted];
        if !reused {
            // Scope minus the witness: the join's scope after the edit (its
            // left side, the barrier's new exports, the witness) and `?x`.
            let mut scope: BTreeSet<String> = plan.variables_of(a);
            if let PlanOp::SubSelect { vars, .. } = &plan.nodes[barrier].op {
                scope.extend(vars.iter().cloned());
            }
            scope.insert(var.clone());
            scope.remove(&witness);
            inserted.push(Node::engine(
                PlanOp::Project {
                    input: PREVIOUS,
                    vars: scope.into_iter().collect(),
                },
                Vec::new(),
            ));
        }
        let at = apply_edit(
            plan,
            Edit {
                removed: vec![(bind, b)],
                after: join,
                inserted,
            },
        );
        let lifted_at = at[0];
        let lifted_key = plan.key_of(lifted_at);
        plan.retire(bind_key, Some(lifted_key));
        plan.lifted_exports.push(LiftedExport {
            barrier: barrier_key,
            var: var.clone(),
            producer: lifted_key,
        });
        let join_ref = plan
            .key_of(plan.nodes[lifted_at].op.inputs()[0])
            .reference();
        let b_ref = b_key.reference();
        plan.rewrites.push(format!(
            "R2  {}  ?{var} lifted from under {join_ref}; inputs {} ⊆ bound({b_ref}) or outside \
             scope(A); effect-free; witness ?{witness}{}",
            lifted_key.reference(),
            if inputs.is_empty() {
                "{}".to_owned()
            } else {
                format!("{{{}}}", var_list(&inputs))
            },
            if reused { " (reused)" } else { "" }
        ));
        true
    }
}

// ---------------------------------------------------------------------------
// R1: a left join against an engine-only right side moves up
// ---------------------------------------------------------------------------

/// **R1.** `A ⟕ SubSelect∅(B ⟕_f C)` →
/// `π₋ₘ( (A ⟕ₘ SubSelect∅(B)) ⟕_{BOUND(?m) ∧ f} C )`.
///
/// **Match.** An engine `LeftJoin` whose right input is an engine barrier
/// directly over an engine `LeftJoin` whose right input `C` is engine-only
/// and reads nothing but the schema graph.
///
/// **Guards.** The barrier opens no naming domain; **G1a** the outer join
/// has no condition; the inner join has no witness of its own; **G1b**
/// `vars(C) ∩ scope(A) ⊆ bound(B)`; **G1c** `f` holds no `EXISTS` and
/// `vars(f) ∩ scope(A) ⊆ bound(B) ∪ bound(C)`; **G1d** `f` and every
/// expression in `C` are effect-free; **G1e** `A` and `B` are SQL.
///
/// **Equivalence** (the design's, in brief). For a left row `a` and a pair
/// `(b, c)`: `a ~ b ⊕ c` iff `a ~ b` and `a ~ c` on what they share, and by
/// G1b the shared variables are bound in `b`, so `c ~ b` and `a ~ b` give
/// `a ~ c`. By G1c `f` sees the same inputs either way, and by G1d the same
/// inputs give the same value however often it is evaluated. An `a` with no
/// compatible `b` carries no `?m`, so `BOUND(?m)` rejects every `c` and it
/// stays `a` -- where testing a shared variable would fail, because `C` can
/// bind it. Multiplicity: each `(a, b)` contributes its qualifying `c`s, or
/// one row, on both sides.
///
/// **Obligations.** The inner join's own claims (its condition) move one
/// scope up, to the new join: recorded as a [`ScopeTransfer`], which *an
/// obligation stays in its scope* validates.
pub struct LiftOptionalRightSide<'a> {
    schema: &'a linkml_schemaview::schemaview::SchemaView,
    schema_graph_iri: Option<&'a str>,
}

impl<'a> LiftOptionalRightSide<'a> {
    pub fn new(
        schema: &'a linkml_schemaview::schemaview::SchemaView,
        schema_graph_iri: Option<&'a str>,
    ) -> Self {
        Self {
            schema,
            schema_graph_iri,
        }
    }
}

struct R1Match {
    outer: NodeId,
    barrier: NodeId,
    inner: NodeId,
    a: NodeId,
    b: NodeId,
    c: NodeId,
    condition: Option<Expr>,
}

impl LiftOptionalRightSide<'_> {
    fn candidate(&self, plan: &Plan, outer: NodeId) -> Option<Result<R1Match, String>> {
        let node = &plan.nodes[outer];
        let PlanOp::LeftJoin {
            left,
            right,
            condition: outer_condition,
            ..
        } = &node.op
        else {
            return None;
        };
        if node.executor != Executor::Engine {
            return None;
        }
        let PlanOp::SubSelect { input, domain, .. } = &plan.nodes[*right].op else {
            return None;
        };
        let inner = *input;
        let PlanOp::LeftJoin {
            left: b,
            right: c,
            condition,
            witness: inner_witness,
            ..
        } = &plan.nodes[inner].op
        else {
            return None;
        };
        if plan.nodes[inner].executor != Executor::Engine
            || plan.nodes[*c].executor != Executor::Engine
        {
            return None;
        }
        let (a, barrier, b, c) = (*left, *right, *b, *c);
        // A table M1 can lower is M1's: an SQL join is cheaper than an
        // engine one over every row, and R1 is for what M1 declines.
        if crate::sparql_constant::table_under(plan, c).is_some()
            && crate::sparql_constant::key_facts(self.schema, plan, inner).is_ok()
        {
            return None;
        }
        let c_nodes = subtree(plan, c);
        // Engine-only: every node of `C` is the engine's. A side with SQL
        // in it is not R1's -- lifting it would split that island.
        if c_nodes
            .iter()
            .any(|id| plan.nodes[*id].executor != Executor::Engine)
        {
            return None;
        }
        Some((|| {
            if let Some(domain) = domain {
                return Err(format!(
                    "the body is a sub-SELECT (domain {domain}): its modifiers and private \
                     variables stay inside"
                ));
            }
            if outer_condition.is_some() {
                return Err(
                    "G1a: the outer left join carries a condition, evaluated over a ⊕ b ⊕ c"
                        .to_owned(),
                );
            }
            if inner_witness.is_some() {
                return Err("the inner left join carries a witness of its own".to_owned());
            }
            reads_schema_only(plan, &c_nodes, self.schema_graph_iri)
                .map_err(|why| format!("C (n{c}) reads more than the schema graph: {why}"))?;
            let scope_a = plan.variables_of(a);
            let bound_b = plan.definitely_bound_of(b);
            let unpinned: BTreeSet<String> = plan
                .variables_of(c)
                .into_iter()
                .filter(|var| scope_a.contains(var) && !bound_b.contains(var))
                .collect();
            if !unpinned.is_empty() {
                return Err(format!(
                    "G1b: {} shared by C (n{c}) and A (n{a}), not bound in every solution of B \
                     (n{b})",
                    var_list(&unpinned)
                ));
            }
            if let Some(f) = condition {
                if f.contains_an_opaque_subquery() {
                    return Err("G1c: the inner condition reads a pattern (EXISTS)".to_owned());
                }
                let bound_c = plan.definitely_bound_of(c);
                let from_a: BTreeSet<String> = variables_used(f)
                    .into_iter()
                    .filter(|var| {
                        scope_a.contains(var) && !bound_b.contains(var) && !bound_c.contains(var)
                    })
                    .collect();
                if !from_a.is_empty() {
                    return Err(format!(
                        "G1c: the inner condition reads {} which only A (n{a}) may supply",
                        var_list(&from_a)
                    ));
                }
                if !f.evaluates_the_same_out_of_context() {
                    return Err("G1d: the inner condition is not effect-free".to_owned());
                }
            }
            if !effect_free(plan, &c_nodes) {
                return Err(format!(
                    "G1d: an expression inside C (n{c}) is not effect-free"
                ));
            }
            for (side, name) in [(a, "A"), (b, "B")] {
                if plan.nodes[side].executor != Executor::Sql {
                    return Err(format!("G1e: {name} (n{side}) is not SQL"));
                }
            }
            Ok(R1Match {
                outer,
                barrier,
                inner,
                a,
                b,
                c,
                condition: condition.clone(),
            })
        })())
    }

    pub fn declined(&self, plan: &Plan) -> Vec<(NodeId, String)> {
        (0..plan.nodes.len())
            .filter_map(|outer| match self.candidate(plan, outer)? {
                Err(why) => Some((outer, why)),
                Ok(_) => None,
            })
            .collect()
    }
}

impl Rule for LiftOptionalRightSide<'_> {
    fn name(&self) -> &'static str {
        R1
    }

    fn measured(&self) -> bool {
        true
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        let Some(found) = (0..plan.nodes.len())
            .find_map(|outer| self.candidate(plan, outer).and_then(Result::ok))
        else {
            return false;
        };
        let R1Match {
            outer,
            barrier,
            inner,
            a,
            b,
            c,
            condition,
        } = found;
        let existing = match &plan.nodes[outer].op {
            PlanOp::LeftJoin { witness, .. } => witness.clone(),
            _ => unreachable!("matched a left join"),
        };
        let reused = existing.is_some();
        let witness = existing.unwrap_or_else(|| fresh(plan, "__m", true));
        let inner_key = plan.key_of(inner);
        let barrier_key = plan.key_of(barrier);
        let inner_claims = plan.nodes[inner].discharges.clone();
        // What moves with `C`: every obligation a node of it claims. Each
        // leaves the body's scope with it, one step up, to the new join.
        let moved_claims: Vec<crate::sparql_plan::ObligationId> = subtree(plan, c)
            .into_iter()
            .flat_map(|id| plan.nodes[id].discharges.clone())
            .collect();
        let scope_b = plan.variables_of(b);
        let mut dropped: Vec<String> = Vec::new();
        if let PlanOp::SubSelect { vars, .. } = &mut plan.nodes[barrier].op {
            vars.retain(|var| {
                let keep = scope_b.contains(var);
                if !keep {
                    dropped.push(var.clone());
                }
                keep
            });
        }
        if let PlanOp::LeftJoin { witness: slot, .. } = &mut plan.nodes[outer].op {
            *slot = Some(witness.clone());
        }
        let lifted_condition = match condition {
            Some(f) => Expr::And(vec![bound_witness(&witness), f]),
            None => bound_witness(&witness),
        };
        let mut inserted = vec![Node::engine(
            PlanOp::LeftJoin {
                left: PREVIOUS,
                right: c,
                reference: None,
                key: None,
                condition: Some(lifted_condition),
                witness: None,
            },
            inner_claims.clone(),
        )];
        if !reused {
            let mut scope: BTreeSet<String> = plan.variables_of(a);
            if let PlanOp::SubSelect { vars, .. } = &plan.nodes[barrier].op {
                scope.extend(vars.iter().cloned());
            }
            scope.extend(plan.variables_of(c));
            scope.remove(&witness);
            inserted.push(Node::engine(
                PlanOp::Project {
                    input: PREVIOUS,
                    vars: scope.into_iter().collect(),
                },
                Vec::new(),
            ));
        }
        let at = apply_edit(
            plan,
            Edit {
                removed: vec![(inner, b)],
                after: outer,
                inserted,
            },
        );
        let lifted_at = at[0];
        let lifted_key = plan.key_of(lifted_at);
        plan.retire(inner_key, Some(lifted_key));
        for obligation in inner_claims.into_iter().chain(moved_claims) {
            plan.transfers.push(ScopeTransfer {
                obligation,
                via: lifted_key,
                rule: R1,
            });
        }
        for var in &dropped {
            plan.lifted_exports.push(LiftedExport {
                barrier: barrier_key,
                var: var.clone(),
                producer: lifted_key,
            });
        }
        let c_ref = match &plan.nodes[lifted_at].op {
            PlanOp::LeftJoin { right, .. } => plan.key_of(*right).reference(),
            _ => String::new(),
        };
        plan.rewrites.push(format!(
            "R1  {}  {c_ref} lifted out of its OPTIONAL; C's variables shared with A are bound \
             by B; condition and C effect-free; witness ?{witness}{}",
            lifted_key.reference(),
            if reused { " (reused)" } else { "" }
        ));
        true
    }
}

// ---------------------------------------------------------------------------
// R4: an extension moves above a join it does not affect
// ---------------------------------------------------------------------------

/// **R4** -- not one of the design's three, and the reason is on this doc.
/// `J(π?(Extend(X, ?x, e)), C)` → `π'?(Extend(J(X, C), ?x, e))`, for `J` a
/// join or a left join with the extension on its preserved side.
///
/// **Why it exists.** R2 lifts a `BIND` out of its `OPTIONAL` to *directly
/// above* the left join. In the #494 query fifteen more `OPTIONAL`s sit above
/// that one, and each left join's preserved side is now the engine's lifted
/// `BIND`, so none of them can be pushed and the SQL part stays fifteen
/// islands. The design's worked example assumed the `BIND` reached the top;
/// its three rules do not take it there. This rule does, one join at a time,
/// and nothing about it is specific to that query: it is the classic
/// extension pull-up.
///
/// **Match.** An engine join or left join whose (preserved) input is an
/// engine `Bind` -- or an engine `Project` directly over one, R2's
/// witness-dropping projection -- over an SQL `X`.
///
/// **Guards.** **H1** `?x ∉ scope(C)`: a `C` that binds `?x` joins on it.
/// **H2** `e` holds no `EXISTS` and `vars(e) ∩ scope(C) ⊆ bound(X)`: `C`
/// cannot supply an input `e` did not have. **H3** `e` is effect-free: it is
/// evaluated once per joined row instead of once per row of `X`. **H4** the
/// join's condition reads neither `?x` nor a pattern. **H5** what the
/// projection drops is neither in `scope(C)` nor read by the condition.
/// **H6** `X` is SQL: the lift exists to let the join be pushed.
///
/// **Equivalence.** Fix a row `x` of `X`, extended to `x' = x ⊕ {?x ↦
/// e(x)}` (or `x` where `e` errors). By H1, `c ~ x'` iff `c ~ x`, and the
/// merge is `x ⊕ c` extended. By H4 the condition takes the same value
/// either way. By H2 every variable of `e` that `c` could bind is already
/// bound in `x` with the same term, so `e(x ⊕ c) = e(x)` in inputs, and by
/// H3 in value. An unmatched `x` of a left join is kept alone on both sides
/// and extended once. Multiplicity: one row per compatible `(x, c)`, or one.
/// The projection commutes by H5: what it drops is not compared by the join.
pub struct LiftExtensionOverJoin;

struct R4Match {
    join: NodeId,
    /// The join's input the extension is: the `Bind`, or the projection over
    /// it.
    side: NodeId,
    projection: Option<NodeId>,
    bind: NodeId,
    x: NodeId,
    var: String,
    expr: Expr,
}

impl LiftExtensionOverJoin {
    fn candidate(plan: &Plan, join: NodeId) -> Option<Result<R4Match, String>> {
        let node = &plan.nodes[join];
        if node.executor != Executor::Engine {
            return None;
        }
        // A recorded reference edge is part of what the join compares: its
        // holder and its referenced star, like the variables the sides
        // share (checked by H1 and H5 below).
        let (sides, condition, edge): (Vec<(NodeId, NodeId)>, Option<&Expr>, _) = match &node.op {
            PlanOp::Join {
                left,
                right,
                reference,
                key: None,
                ..
            } => (
                vec![(*left, *right), (*right, *left)],
                None,
                reference.as_ref(),
            ),
            PlanOp::LeftJoin {
                left,
                right,
                reference,
                key: None,
                condition,
                ..
            } => (
                vec![(*left, *right)],
                condition.as_ref(),
                reference.as_ref(),
            ),
            _ => return None,
        };
        let on_edge: BTreeSet<String> = edge
            .map(|edge| [edge.holder.clone(), edge.referenced.clone()].into())
            .unwrap_or_default();
        let (side, c, projection, bind) = sides.into_iter().find_map(|(side, c)| {
            if plan.nodes[side].executor != Executor::Engine {
                return None;
            }
            match &plan.nodes[side].op {
                PlanOp::Bind { .. } => Some((side, c, None, side)),
                PlanOp::Project { input, .. }
                    if matches!(plan.nodes[*input].op, PlanOp::Bind { .. })
                        && plan.nodes[*input].executor == Executor::Engine =>
                {
                    Some((side, c, Some(side), *input))
                }
                _ => None,
            }
        })?;
        let PlanOp::Bind {
            input: x,
            var,
            expr,
        } = &plan.nodes[bind].op
        else {
            unreachable!("matched a bind");
        };
        let x = *x;
        Some((|| {
            let scope_c = plan.variables_of(c);
            if scope_c.contains(var) {
                return Err(format!("H1: ?{var} is in scope in n{c}"));
            }
            if on_edge.contains(var) {
                return Err(format!("H1: ?{var} is an end of the join's reference edge"));
            }
            if expr.contains_an_opaque_subquery() {
                return Err("H2: the expression reads a pattern (EXISTS)".to_owned());
            }
            let bound_x = plan.definitely_bound_of(x);
            let supplied: BTreeSet<String> = variables_used(expr)
                .into_iter()
                .filter(|used| scope_c.contains(used) && !bound_x.contains(used))
                .collect();
            if !supplied.is_empty() {
                return Err(format!(
                    "H2: {} in scope in n{c} and not bound in every solution of n{x}",
                    var_list(&supplied)
                ));
            }
            if !expr.evaluates_the_same_out_of_context() {
                return Err("H3: the expression is not effect-free".to_owned());
            }
            let read_by_condition: BTreeSet<String> = match condition {
                Some(condition) => {
                    if condition.contains_an_opaque_subquery() {
                        return Err("H4: the join's condition reads a pattern (EXISTS)".to_owned());
                    }
                    variables_used(condition).into_iter().collect()
                }
                None => BTreeSet::new(),
            };
            if read_by_condition.contains(var) {
                return Err(format!("H4: the join's condition reads ?{var}"));
            }
            if let Some(projection) = projection {
                let PlanOp::Project { vars, .. } = &plan.nodes[projection].op else {
                    unreachable!("matched a projection");
                };
                let dropped: BTreeSet<String> = plan
                    .variables_of(bind)
                    .into_iter()
                    .filter(|v| !vars.contains(v))
                    .collect();
                let compared: BTreeSet<String> = dropped
                    .iter()
                    .filter(|v| {
                        scope_c.contains(*v)
                            || read_by_condition.contains(*v)
                            || on_edge.contains(*v)
                    })
                    .cloned()
                    .collect();
                if !compared.is_empty() {
                    return Err(format!(
                        "H5: the projection n{projection} drops {}, which the join compares",
                        var_list(&compared)
                    ));
                }
            }
            if plan.nodes[x].executor != Executor::Sql {
                return Err(format!("H6: n{x} is not SQL"));
            }
            Ok(R4Match {
                join,
                side,
                projection,
                bind,
                x,
                var: var.clone(),
                expr: expr.clone(),
            })
        })())
    }

    pub fn declined(plan: &Plan) -> Vec<(NodeId, String)> {
        (0..plan.nodes.len())
            .filter_map(|join| match Self::candidate(plan, join)? {
                Err(why) => Some((join, why)),
                Ok(_) => None,
            })
            .collect()
    }
}

impl Rule for LiftExtensionOverJoin {
    fn name(&self) -> &'static str {
        R4
    }

    fn measured(&self) -> bool {
        true
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        let Some(found) =
            (0..plan.nodes.len()).find_map(|join| Self::candidate(plan, join).and_then(Result::ok))
        else {
            return false;
        };
        let R4Match {
            join,
            side,
            projection,
            bind,
            x,
            var,
            expr,
        } = found;
        let join_key = plan.key_of(join);
        let bind_key = plan.key_of(bind);
        let projection_key = projection.map(|projection| plan.key_of(projection));
        let bind_claims = plan.nodes[bind].discharges.clone();
        // What the projection dropped, dropped again above the join: scope
        // minus what it drops, never a keep list.
        let dropped: BTreeSet<String> = match projection {
            Some(projection) => match &plan.nodes[projection].op {
                PlanOp::Project { vars, .. } => plan
                    .variables_of(bind)
                    .into_iter()
                    .filter(|v| !vars.contains(v))
                    .collect(),
                _ => unreachable!("matched a projection"),
            },
            None => BTreeSet::new(),
        };
        let mut inserted = vec![Node::engine(
            PlanOp::Bind {
                input: PREVIOUS,
                var: var.clone(),
                expr,
            },
            bind_claims,
        )];
        if projection.is_some() {
            let mut scope = plan.variables_of(join);
            scope.extend(plan.variables_of(bind));
            for gone in &dropped {
                scope.remove(gone);
            }
            inserted.push(Node::engine(
                PlanOp::Project {
                    input: PREVIOUS,
                    vars: scope.into_iter().collect(),
                },
                Vec::new(),
            ));
        }
        let mut removed = vec![(bind, x)];
        if let Some(projection) = projection {
            removed.push((projection, x));
        }
        let _ = side;
        let at = apply_edit(
            plan,
            Edit {
                removed,
                after: join,
                inserted,
            },
        );
        let lifted_key = plan.key_of(at[0]);
        plan.retire(bind_key, Some(lifted_key));
        if let (Some(key), Some(new)) = (projection_key, at.get(1)) {
            plan.retire(key, Some(plan.key_of(*new)));
        }
        plan.rewrites.push(format!(
            "R4  {}  ?{var} lifted above {}; ?{var} ∉ scope of the other side; its inputs \
             bound below; effect-free",
            lifted_key.reference(),
            join_key.reference(),
        ));
        true
    }
}

// ---------------------------------------------------------------------------
// R3: ORDER BY / LIMIT / OFFSET move below one-to-one engine operators
// ---------------------------------------------------------------------------

/// **R3.** `Slice(Project(Sort(E(X), k)))` →
/// `Project(Sort(E(Number(Slice(Sort(X, k)))), ?__ord))`.
///
/// **Match.** The root scope's modifier chain -- a `Project`, a `Sort` of
/// the query's own (`origin: Query`) and at most one `Slice` -- over a chain
/// `E` of engine nodes down to an SQL node `X`.
///
/// **Guards.** No `DISTINCT` or `REDUCED` in the chain (the slice would
/// count rows the deduplication merges); **G3a** every node of `E` gives
/// exactly one output row per input row: a `Bind`, a `Project`, an
/// `OPTIONAL`-body barrier, or a `LeftJoin` whose right side is a `Values`
/// `C` with a key set `K ⊆ scope(X') ∩ vars(C)` that is bound in every left
/// row, `UNDEF`-free in `C`, and unique among `C`'s rows by `sameTerm` -- so
/// every left row is compatible with at most one row of `C`, and a left
/// join keeps it either way; **G3b** every sort term is a variable of
/// `scope(X)` that no node of `E` binds; **G3c** each is a column the
/// statement can read.
///
/// **Equivalence.** `X` yields `x₁ … xₙ` in `k` order; `E` maps each `xᵢ`
/// to exactly one `yᵢ` with the same `k` values (G3a, G3b), so `Sort(E(X),
/// k)` is `y₁ … yₙ` and the slice takes `y_{o+1} … y_{o+l}` =
/// `E(x_{o+1} … x_{o+l})`. The numbering and the restoring sort reinstate
/// that sequence whatever order `E`'s evaluation emits. Ties are settled by
/// SQL, which is a conforming order of a non-unique key.
///
/// **Effects.** No expression moves: `E` is evaluated once per row that
/// reaches the answer, over the same input row, on both routes.
///
/// **Edit, the ordinal's path.** Every `Project` and barrier of `E` gains
/// `?__ord`; only the query's final projection drops it.
pub struct PageBelowOneToOne<'s> {
    schema: &'s linkml_schemaview::schemaview::SchemaView,
}

impl<'s> PageBelowOneToOne<'s> {
    pub fn new(schema: &'s linkml_schemaview::schemaview::SchemaView) -> Self {
        Self { schema }
    }
}

struct R3Match {
    project: NodeId,
    /// `None` for a `LIMIT` / `OFFSET` with no `ORDER BY`: any page of the
    /// solutions is an answer, so the slice moves and no order crosses.
    sort: Option<NodeId>,
    slice: Option<NodeId>,
    /// `E`, top down.
    chain: Vec<NodeId>,
    x: NodeId,
    sort_vars: Vec<String>,
}

/// **G3a**, on one node of `E` read from `below`: whether it gives exactly
/// one output row per input row, or why not.
pub fn one_to_one(plan: &Plan, node: NodeId, below: NodeId) -> Result<(), String> {
    match &plan.nodes[node].op {
        PlanOp::Bind { .. } | PlanOp::Project { .. } | PlanOp::Number { .. } => Ok(()),
        PlanOp::Sort { .. } => Ok(()),
        PlanOp::SubSelect { domain: None, .. } => Ok(()),
        PlanOp::SubSelect {
            domain: Some(domain),
            ..
        } => Err(format!(
            "G3a: n{node} is a sub-SELECT (domain {domain}), whose modifiers count rows of their own"
        )),
        PlanOp::LeftJoin { left, right, .. } if *left == below => {
            let table = crate::sparql_constant::table_under(plan, *right);
            let Some(PlanOp::Values { variables, rows }) = table.map(|table| &plan.nodes[table].op)
            else {
                return Err(format!(
                    "G3a: n{node} left-joins n{right}, which is not an inline table, so a row \
                     may meet several"
                ));
            };
            // Through a barrier, only what it exports is compared.
            let exported = plan.variables_of(*right);
            let scope = plan.variables_of(*left);
            let bound = plan.definitely_bound_of(*left);
            // The largest key set the proof admits: shared, bound in every
            // left row, and UNDEF-free in the table.
            let key: Vec<usize> = variables
                .iter()
                .enumerate()
                .filter(|(column, variable)| {
                    scope.contains(variable.as_str())
                        && exported.contains(variable.as_str())
                        && bound.contains(variable.as_str())
                        && rows
                            .iter()
                            .all(|row| row.get(*column).is_some_and(Option::is_some))
                })
                .map(|(column, _)| column)
                .collect();
            let mut seen: BTreeSet<Vec<String>> = BTreeSet::new();
            for row in rows {
                let at: Vec<String> = key
                    .iter()
                    .map(|column| {
                        row[*column]
                            .as_ref()
                            .map(|term| term.to_string())
                            .unwrap_or_default()
                    })
                    .collect();
                if !seen.insert(at) {
                    return Err(format!(
                        "G3a: two rows of n{right} agree on the key {{{}}} bound in every row of \
                         n{left}, so a left row may be compatible with both",
                        key.iter()
                            .map(|column| format!("?{}", variables[*column].as_str()))
                            .collect::<Vec<_>>()
                            .join(" ")
                    ));
                }
            }
            Ok(())
        }
        other => Err(format!("G3a: n{node} ({}) is not one-to-one", other.kind())),
    }
}

impl PageBelowOneToOne<'_> {
    fn candidate(&self, plan: &Plan) -> Option<Result<R3Match, String>> {
        if plan.form != crate::sparql_refine::QueryForm::Select {
            return None;
        }
        let root = plan.nodes.len().checked_sub(1)?;
        // The root scope's modifier chain.
        let mut current = root;
        let (mut project, mut sort, mut slice) = (None, None, None);
        let mut distinct = false;
        loop {
            let node = &plan.nodes[current];
            if node.executor != Executor::Engine {
                break;
            }
            let input = match &node.op {
                PlanOp::Project { input, .. } if project.is_none() => {
                    project = Some(current);
                    *input
                }
                PlanOp::Sort {
                    input,
                    origin: SortOrigin::Query,
                    ..
                } if sort.is_none() => {
                    sort = Some(current);
                    *input
                }
                PlanOp::Slice { input, .. } if slice.is_none() && sort.is_none() => {
                    slice = Some(current);
                    *input
                }
                PlanOp::Distinct { input } | PlanOp::Reduced { input } => {
                    distinct = true;
                    *input
                }
                _ => break,
            };
            current = input;
            if sort.is_some() {
                break;
            }
        }
        let project = project?;
        if sort.is_none() && slice.is_none() {
            return None;
        }
        // The lowest modifier: the sort, or -- a `LIMIT` with no `ORDER BY`
        // -- the projection.
        let (terms, input): (Vec<SortTerm>, NodeId) = match sort {
            Some(sort) => match &plan.nodes[sort].op {
                PlanOp::Sort { terms, input, .. } => (terms.clone(), *input),
                _ => unreachable!("matched a sort"),
            },
            None => match &plan.nodes[project].op {
                PlanOp::Project { input, .. } => (Vec::new(), *input),
                _ => unreachable!("matched a projection"),
            },
        };
        // `E`: engine nodes from the lowest modifier's input down to the
        // island.
        let mut chain: Vec<NodeId> = Vec::new();
        let mut at = input;
        while plan.nodes[at].executor == Executor::Engine {
            chain.push(at);
            at = match &plan.nodes[at].op {
                PlanOp::LeftJoin { left, .. } => *left,
                other => match other.inputs().as_slice() {
                    [only] => *only,
                    _ => {
                        return Some(Err(format!(
                            "G3a: n{at} ({}) is not one-to-one",
                            other.kind()
                        )));
                    }
                },
            };
        }
        if chain.is_empty() {
            // The whole chain is over SQL already: the projection rule's.
            return None;
        }
        let x = at;
        Some((|| {
            if distinct {
                return Err(
                    "a DISTINCT or REDUCED in the modifier chain merges rows the slice counts"
                        .to_owned(),
                );
            }
            let mut below = x;
            for node in chain.iter().rev() {
                one_to_one(plan, *node, below)?;
                below = *node;
            }
            let scope_x = plan.variables_of(x);
            let bound_by_e: BTreeSet<String> = chain
                .iter()
                .flat_map(|node| {
                    let mut out = plan.variables_of(*node);
                    for var in &scope_x {
                        out.remove(var);
                    }
                    out
                })
                .collect();
            let mut sort_vars = Vec::with_capacity(terms.len());
            for term in &terms {
                let Expr::Var(var) = &term.expr else {
                    return Err(format!("G3b: the sort term {} is an expression", term.expr));
                };
                if !scope_x.contains(var) || bound_by_e.contains(var) {
                    return Err(format!(
                        "G3b: ?{var} is not a variable the statement (n{x}) binds"
                    ));
                }
                if !crate::sparql_rules::PushGrouping::new(self.schema)
                    .key_is_readable(plan, x, var, true)
                {
                    return Err(format!(
                        "G3c: ?{var} is not a column the statement (n{x}) can read"
                    ));
                }
                sort_vars.push(var.clone());
            }
            Ok(R3Match {
                project,
                sort,
                slice,
                chain: chain.clone(),
                x,
                sort_vars,
            })
        })())
    }

    pub fn declined(&self, plan: &Plan) -> Vec<(NodeId, String)> {
        match self.candidate(plan) {
            Some(Err(why)) => {
                let at = plan.nodes.len().saturating_sub(1);
                vec![(at, why)]
            }
            _ => Vec::new(),
        }
    }
}

impl Rule for PageBelowOneToOne<'_> {
    fn name(&self) -> &'static str {
        R3
    }

    fn measured(&self) -> bool {
        true
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        let Some(Ok(found)) = self.candidate(plan) else {
            return false;
        };
        let R3Match {
            project,
            sort,
            slice,
            chain,
            x,
            sort_vars,
        } = found;
        let sorted = match sort {
            Some(sort) => match &plan.nodes[sort].op {
                PlanOp::Sort { terms, .. } => {
                    Some((terms.clone(), plan.nodes[sort].discharges.clone()))
                }
                _ => unreachable!("matched a sort"),
            },
            None => None,
        };
        let sliced = match slice {
            Some(slice) => match &plan.nodes[slice].op {
                PlanOp::Slice { limit, offset, .. } => {
                    Some((*limit, *offset, plan.nodes[slice].discharges.clone()))
                }
                _ => unreachable!("matched a slice"),
            },
            None => None,
        };
        // An order crosses the boundary only when there is one.
        let ordinal = sorted.as_ref().map(|_| fresh(plan, "__ord", false));
        let top_of_e = chain[0];

        // The ordinal's path: every projection and barrier of `E` keeps it.
        if let Some(ordinal) = &ordinal {
            for node in &chain {
                match &mut plan.nodes[*node].op {
                    PlanOp::Project { vars, .. } | PlanOp::SubSelect { vars, .. }
                        if !vars.contains(ordinal) =>
                    {
                        vars.push(ordinal.clone());
                    }
                    _ => {}
                }
            }
        }

        // Below `E`: the query's sort and slice, in SQL, then the numbering.
        let mut below: Vec<Node> = Vec::new();
        if let Some((terms, claims)) = &sorted {
            below.push(Node::sql(
                PlanOp::Sort {
                    input: PREVIOUS,
                    terms: terms.clone(),
                    origin: SortOrigin::Query,
                },
                claims.clone(),
            ));
        }
        if let Some((limit, offset, claims)) = &sliced {
            below.push(Node::sql(
                PlanOp::Slice {
                    input: PREVIOUS,
                    limit: *limit,
                    offset: *offset,
                },
                claims.clone(),
            ));
        }
        if let Some(ordinal) = &ordinal {
            below.push(Node::engine(
                PlanOp::Number {
                    input: PREVIOUS,
                    var: ordinal.clone(),
                },
                Vec::new(),
            ));
        }
        let sort_key = sort.map(|sort| plan.key_of(sort));
        let slice_key = slice.map(|slice| plan.key_of(slice));
        let x_key = plan.key_of(x);
        let top_key = plan.key_of(top_of_e);
        let project_key = plan.key_of(project);
        // First edit: the moved modifiers directly above `X` -- `X`'s one
        // consumer is `E`'s bottom, which now reads them -- and the old ones
        // gone, their consumers reading what they read.
        let mut removed: Vec<(NodeId, NodeId)> = Vec::new();
        if let Some(sort) = sort {
            removed.push((sort, top_of_e));
        }
        if let Some(slice) = slice {
            removed.push((slice, project));
        }
        let at = apply_edit(
            plan,
            Edit {
                removed,
                after: x,
                inserted: below,
            },
        );
        let keys: Vec<crate::sparql_refine::NodeKey> =
            at.iter().map(|id| plan.key_of(*id)).collect();
        let mut index = 0;
        let moved_sort = sorted.as_ref().map(|_| {
            index += 1;
            keys[index - 1]
        });
        let moved_slice = sliced.as_ref().map(|_| {
            index += 1;
            keys[index - 1]
        });
        let number = ordinal.as_ref().map(|_| keys[index]);
        if let (Some(key), Some(moved)) = (sort_key, moved_sort) {
            plan.retire(key, Some(moved));
        }
        if let (Some(key), Some(moved)) = (slice_key, moved_slice) {
            plan.retire(key, Some(moved));
        }
        // Then the restoring sort, directly above `E`'s top, read by the
        // projection -- which is the root now.
        let mut restoring_key = None;
        if let Some(ordinal) = &ordinal {
            let top = plan.node(top_key).expect("E's top is kept");
            let restoring = Node::engine(
                PlanOp::Sort {
                    input: PREVIOUS,
                    terms: vec![SortTerm {
                        expr: Expr::Var(ordinal.clone()),
                        desc: false,
                    }],
                    origin: SortOrigin::Ordinal,
                },
                Vec::new(),
            );
            let at = apply_edit(
                plan,
                Edit {
                    removed: Vec::new(),
                    after: top,
                    inserted: vec![restoring],
                },
            );
            restoring_key = Some(plan.key_of(at[0]));
        }
        let now = |key: Option<crate::sparql_refine::NodeKey>| -> String {
            key.map(|key| key.reference()).unwrap_or_default()
        };
        let moved = [now(moved_sort), now(moved_slice)]
            .into_iter()
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        let (_, _, modifiers, _) = progress(plan);
        let order = match (&ordinal, number) {
            (Some(ordinal), Some(number)) => format!(
                "; ?{ordinal} in scope {} → {}, dropped at {}",
                now(Some(number)),
                now(restoring_key),
                now(Some(project_key))
            ),
            _ => "; no ORDER BY, so no order crosses".to_owned(),
        };
        let keys_text = if sort_vars.is_empty() {
            String::new()
        } else {
            format!(
                "; sort key {} ∈ scope({})",
                sort_vars
                    .iter()
                    .map(|var| format!("?{var}"))
                    .collect::<Vec<_>>()
                    .join(" "),
                now(Some(x_key))
            )
        };
        plan.rewrites.push(format!(
            "R3  {moved}  moved below {} one-to-one node(s){keys_text}; query modifiers above \
             the engine → {modifiers}{order}",
            chain.len()
        ));
        true
    }
}

/// The lifting rules, in the order they are offered: the inner lifts first,
/// then the paging.
pub fn lifting_rules<'a>(
    schema: &'a linkml_schemaview::schemaview::SchemaView,
    schema_graph_iri: Option<&'a str>,
) -> Vec<Box<dyn Rule + 'a>> {
    vec![
        Box::new(LiftOptionalExtension),
        Box::new(LiftOptionalRightSide::new(schema, schema_graph_iri)),
        Box::new(LiftExtensionOverJoin),
        Box::new(PageBelowOneToOne::new(schema)),
    ]
}

/// Every decline R1–R3 can explain on a refined plan.
pub fn declined(
    plan: &Plan,
    schema: &linkml_schemaview::schemaview::SchemaView,
    schema_graph_iri: Option<&str>,
) -> Vec<(&'static str, NodeId, String)> {
    let mut out = Vec::new();
    for (node, why) in LiftOptionalExtension::declined(plan) {
        out.push((R2, node, why));
    }
    for (node, why) in LiftOptionalRightSide::new(schema, schema_graph_iri).declined(plan) {
        out.push((R1, node, why));
    }
    for (node, why) in LiftExtensionOverJoin::declined(plan) {
        out.push((R4, node, why));
    }
    for (node, why) in PageBelowOneToOne::new(schema).declined(plan) {
        out.push((R3, node, why));
    }
    out
}

/// Insert `node` (its input written as the node it sits on) directly above
/// `below`, every consumer of `below` reading it instead. Returns its
/// position.
pub fn insert_above(plan: &mut Plan, below: NodeId, mut node: Node) -> NodeId {
    node.op.map_inputs(|_| PREVIOUS);
    apply_edit(
        plan,
        Edit {
            removed: Vec::new(),
            after: below,
            inserted: vec![node],
        },
    )[0]
}

/// **G3a for a whole region**: whether the path from `boundary` up to the
/// plan's root is one-to-one node by node, so the region answers exactly
/// one row per row it is handed (`EngineInput::Solutions::preserves_rows`).
pub fn preserves_rows(plan: &Plan, boundary: NodeId) -> bool {
    let root = plan.nodes.len() - 1;
    let mut below = boundary;
    while below != root {
        let above = consumers(plan, below);
        let [next] = above.as_slice() else {
            return false;
        };
        if one_to_one(plan, *next, below).is_err() {
            return false;
        }
        below = *next;
    }
    true
}

#[cfg(all(test, feature = "sparql-endpoint"))]
pub(crate) mod tests {
    use crate::sparql_oracle::{Oracle, fixture, probe, probe_sequence};
    use crate::sparql_plan::{EngineInput, ExecutionPlan, PassKind, Refinement};
    use crate::sparql_refine::Plan;
    use crate::sparql_scoper::tests::test_schema_view;
    use linkml_schemaview::schemaview::SchemaView;

    pub(crate) const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

    /// The plan production refines a query to with the lifts on.
    pub(crate) fn lifted_plan(query: &str, schema: &SchemaView) -> Plan {
        let mut plan = crate::sparql_refine::naive_plan_for(query, schema).unwrap();
        let rules = crate::sparql_plan::refine_rules(schema, None, true);
        let borrowed: Vec<&dyn crate::sparql_rules::Rule> =
            rules.iter().map(|rule| rule.as_ref()).collect();
        crate::sparql_rules::refine(&mut plan, &borrowed)
            .unwrap_or_else(|failure| panic!("{failure}\n{plan}"));
        plan
    }

    /// What the refined printout says, `declined` section included.
    pub(crate) fn printout(query: &str, schema: &SchemaView) -> String {
        crate::sparql_plan::with_declined(&lifted_plan(query, schema), schema, None)
    }

    /// **The rows route, end to end on the oracle.** The query is planned
    /// the way production plans it; its statement's rows are the logical
    /// statement -- the island, written back -- evaluated on the oracle in
    /// the order it returns them; [`crate::sparql_executor::sparql_finish`]
    /// finishes over them exactly as the executor does. Returns the plan and
    /// the answer rows, in order.
    pub(crate) fn rows_route_answer(
        query: &str,
        schema: &SchemaView,
        oracle: &Oracle,
    ) -> (ExecutionPlan, Vec<serde_json::Value>) {
        let plan = crate::sparql_plan::plan_query_refined(query, schema).unwrap();
        assert!(
            matches!(plan.refinement, Refinement::UsedRows(_)),
            "expected the rows route:\n{plan}"
        );
        // The same placement, for the island it placed.
        let parsed = crate::sparql_scoper::parse_query_for(query, schema).unwrap();
        let obligations = crate::sparql_plan::obligations_of(&parsed).unwrap();
        let mut lifted = crate::sparql_refine::naive_plan(&parsed).unwrap();
        let lifts = plan.passes.iter().any(|pass| {
            matches!(&pass.kind, PassKind::Engine(engine)
                if matches!(&engine.input, EngineInput::Solutions { .. }))
        });
        let rules = crate::sparql_plan::refine_rules(schema, None, lifts);
        let borrowed: Vec<&dyn crate::sparql_rules::Rule> =
            rules.iter().map(|rule| rule.as_ref()).collect();
        crate::sparql_rules::refine(&mut lifted, &borrowed).unwrap();
        let placement = crate::sparql_plan::rows_placement(
            &obligations,
            &lifted,
            &parsed,
            schema,
            None,
            &Default::default(),
        )
        .unwrap_or_else(|why| panic!("{why}\n{lifted}"));
        let island =
            crate::sparql_algebra::plan_to_algebra(&placement.working, schema, placement.island)
                .expect("the island writes back");
        let (vars, rows) = oracle.json_rows(spargebra::Query::Select {
            dataset: None,
            pattern: island,
            base_iri: None,
        });
        let json = serde_json::json!({
            "head": { "vars": vars },
            "results": { "bindings": rows },
        })
        .to_string();
        let answer = crate::sparql_executor::sparql_finish(
            &plan,
            &json,
            schema,
            crate::sparql_executor::ExecuteLimits::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("{error}\n{plan}"));
        let parsed: serde_json::Value = serde_json::from_str(&answer.body).unwrap();
        let rows = parsed["results"]["bindings"].as_array().unwrap().clone();
        (plan, rows)
    }

    /// The oracle's own answer to the original query, in its order.
    pub(crate) fn oracle_rows(query: &str, oracle: &Oracle) -> Vec<serde_json::Value> {
        oracle
            .json_rows(crate::sparql_scoper::parse_query(query).unwrap())
            .1
    }

    pub(crate) fn as_bag(rows: &[serde_json::Value]) -> Vec<String> {
        let mut out: Vec<String> = rows.iter().map(|row| row.to_string()).collect();
        out.sort();
        out
    }

    fn finish_of(plan: &ExecutionPlan) -> &str {
        plan.passes
            .iter()
            .find_map(|pass| match &pass.kind {
                PassKind::Engine(engine) => match &engine.input {
                    EngineInput::Solutions { finish, .. } => Some(finish.as_str()),
                    EngineInput::Records => None,
                },
                PassKind::Sql(_) => None,
            })
            .expect("a rows-route plan")
    }

    /// **`r3_after_r2_one_island`** -- P4 of the design, the whole pipeline.
    /// R2 lifts the `BIND` and leaves one island; R3 then moves the query's
    /// `ORDER BY` and `LIMIT` into the statement (islands 1 → 1, query
    /// modifiers above the engine 2 → 0) and is **kept**. The statement
    /// carries the order and the page, the finish carries neither and ends
    /// in `ORDER BY ?__ord`, and the answer is the oracle's.
    #[test]
    fn r3_after_r2_one_island() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let query = format!(
            "{PREFIX}SELECT ?s ?name ?tail WHERE {{ ?s a asset360:Signal ; asset360:name ?name . \
             OPTIONAL {{ ?s asset360:locatedOnTrack ?e . \
             BIND(STRAFTER(STR(?e), \"/track/\") AS ?tail) }} }} ORDER BY ?name LIMIT 10"
        );
        let refined = printout(&query, &schema);
        let r2 = refined.find("R2  ").expect(&refined);
        let r3 = refined.find("R3  ").expect(&refined);
        assert!(r2 < r3, "R2 fires before R3:\n{refined}");
        assert!(refined.contains("witness ?__m1"), "{refined}");
        assert!(
            refined.contains("query modifiers above the engine → 0"),
            "{refined}"
        );
        assert!(refined.contains("origin ordinal"), "{refined}");

        let (plan, rows) = rows_route_answer(&query, &schema, &oracle);
        let printed = plan.to_string();
        assert!(printed.contains("order     binding"), "{printed}");
        assert!(printed.contains("limit     10 offset 0"), "{printed}");
        let finish = finish_of(&plan);
        assert!(finish.ends_with("ORDER BY ASC(?__ord)"), "{finish}");
        assert!(!finish.contains("LIMIT"), "{finish}");
        let expected = oracle_rows(&query, &oracle);
        assert_eq!(as_bag(&rows), as_bag(&expected), "{plan}");
        // As a sequence on the sort key; ties are SQL's to settle.
        let names = |rows: &[serde_json::Value]| -> Vec<String> {
            rows.iter().map(|row| row["name"].to_string()).collect()
        };
        assert_eq!(names(&rows), names(&expected), "{plan}");
    }

    /// **`p2_join_reorders_values`** (design appendix): the reason order
    /// crosses as data. Forty rows in descending `?id`, inner-joined to a
    /// seven-row table, come back in another order.
    #[test]
    fn p2_join_reorders_values() {
        let rows: String = (1..=40)
            .rev()
            .map(|id| format!("({id} {})", id % 7))
            .collect::<Vec<_>>()
            .join(" ");
        let table: String = (0..7)
            .map(|k| format!("({k} \"l{k}\")"))
            .collect::<Vec<_>>()
            .join(" ");
        let plain = probe_sequence(
            "",
            &format!("SELECT ?id WHERE {{ VALUES (?id ?k) {{ {rows} }} }}"),
        );
        let joined = probe_sequence(
            "",
            &format!(
                "SELECT ?id WHERE {{ VALUES (?id ?k) {{ {rows} }} VALUES (?k ?label) {{ {table} }} }}"
            ),
        );
        assert_eq!(plain.len(), joined.len());
        assert_ne!(plain, joined, "the engine keeps no order through a join");
    }

    /// **`m2_exists_in_bind_reads_instances`** (design appendix): an
    /// `EXISTS` evaluated over the rows and the schema graph only answers
    /// `false` where the records say `true`, so the region that holds one
    /// finishes over records.
    #[test]
    fn m2_exists_in_bind_reads_instances() {
        let data = "<urn:s1> <urn:p> <urn:v> .";
        let original = probe(
            data,
            "SELECT ?found { VALUES ?s { <urn:s1> } BIND(EXISTS { ?s <urn:p> ?v } AS ?found) }",
        );
        let rows_only = probe(
            "",
            "SELECT ?found { VALUES ?s { <urn:s1> } BIND(EXISTS { ?s <urn:p> ?v } AS ?found) }",
        );
        assert_ne!(original, rows_only);

        let schema = test_schema_view();
        let plan = crate::sparql_plan::plan_query_refined(
            &format!(
                "{PREFIX}SELECT ?s ?found WHERE {{ ?s a asset360:Signal ; asset360:name ?nm . \
                 BIND(EXISTS {{ ?s asset360:length ?len }} AS ?found) }}"
            ),
            &schema,
        )
        .unwrap();
        let Refinement::Used(Some(note)) = &plan.refinement else {
            panic!("{plan}");
        };
        assert!(note.contains("reads a pattern (EXISTS)"), "{note}");
    }
}

/// The design's per-rule regressions (appendix): every counterexample of
/// the three reviews. A rule that must **fire** is compared against the
/// oracle over the rows route; one that must **decline** names its guard
/// in the printout, and the rewrite it refuses is evaluated and shown to
/// differ, so the counterexample stays honest if the engine changes.
#[cfg(all(test, feature = "sparql-endpoint"))]
mod regressions {
    use super::tests::{PREFIX, as_bag, lifted_plan, oracle_rows, printout, rows_route_answer};
    use crate::sparql_oracle::{fixture, probe, probe_sequence};
    use crate::sparql_scoper::tests::test_schema_view;

    // --- R2 ---------------------------------------------------------------

    /// **`r2_qualifying_strafter`**: the #494 shape must fire, and answer
    /// what the engine answers.
    #[test]
    fn r2_qualifying_strafter() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let query = format!(
            "{PREFIX}SELECT ?s ?tail WHERE {{ ?s a asset360:Signal . \
             OPTIONAL {{ ?s asset360:locatedOnTrack ?e . \
             BIND(STRAFTER(STR(?e), \"/track/\") AS ?tail) }} }}"
        );
        assert!(printout(&query, &schema).contains("R2  "));
        let (_plan, rows) = rows_route_answer(&query, &schema, &oracle);
        assert_eq!(as_bag(&rows), as_bag(&oracle_rows(&query, &oracle)));
        // The probe the design ran: equal.
        let data = "<urn:s1> <urn:j> <urn:E1> .";
        assert_eq!(
            probe(
                data,
                "SELECT * { ?s <urn:j> ?j OPTIONAL { ?s <urn:j> ?e BIND(STRAFTER(STR(?e), \"urn:\") AS ?t) } }"
            ),
            probe(
                data,
                "SELECT ?s ?j ?e ?t { ?s <urn:j> ?j OPTIONAL { ?s <urn:j> ?e BIND(true AS ?m) } \
                 BIND(IF(BOUND(?m), STRAFTER(STR(?e), \"urn:\"), ?never) AS ?t) }"
            )
        );
    }

    /// **`r2_non_strict_expression`**: `COALESCE` over an unmatched side is
    /// not strict, and the witness makes that irrelevant.
    #[test]
    fn r2_non_strict_expression() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let query = format!(
            "{PREFIX}SELECT ?s ?x WHERE {{ ?s a asset360:Signal . \
             OPTIONAL {{ ?s asset360:length ?len . BIND(COALESCE(?len, 0) AS ?x) }} }}"
        );
        assert!(printout(&query, &schema).contains("R2  "));
        let (_plan, rows) = rows_route_answer(&query, &schema, &oracle);
        assert_eq!(as_bag(&rows), as_bag(&oracle_rows(&query, &oracle)));
        assert_eq!(
            probe(
                "",
                "SELECT * { VALUES ?a {10} OPTIONAL { VALUES ?b {2} FILTER(false) BIND(COALESCE(?b, 0) AS ?x) } }"
            ),
            probe(
                "",
                "SELECT ?a ?b ?x { VALUES ?a {10} OPTIONAL { VALUES ?b {2} FILTER(false) BIND(true AS ?m) } \
                 BIND(IF(BOUND(?m), COALESCE(?b, 0), ?never) AS ?x) }"
            )
        );
    }

    /// **`r2_input_from_a`** (G2b): `?b + ?a` computed inside the body sees
    /// `?a` unbound; lifted, it would see the left row's.
    #[test]
    fn r2_input_from_a() {
        let schema = test_schema_view();
        let refined = printout(
            &format!(
                "{PREFIX}SELECT ?s ?x WHERE {{ ?s a asset360:Signal ; asset360:length ?a . \
                 OPTIONAL {{ ?s asset360:name ?b . BIND(CONCAT(?b, STR(?a)) AS ?x) }} }}"
            ),
            &schema,
        );
        assert!(refined.contains("G2b: ?a in scope in"), "{refined}");
        assert!(!refined.contains("R2  "), "{refined}");
        assert_ne!(
            probe(
                "",
                "SELECT * { VALUES ?a {10} OPTIONAL { VALUES ?b {2} BIND(?b + ?a AS ?x) } }"
            ),
            probe(
                "",
                "SELECT * { VALUES ?a {10} OPTIONAL { VALUES ?b {2} } BIND(?b + ?a AS ?x) }"
            ),
        );
    }

    /// **`r2_bnode_once_per_b_row`** (G2c): one blank node per body row in
    /// the original, one per joined row after a lift.
    #[test]
    fn r2_bnode_once_per_b_row() {
        let schema = test_schema_view();
        let refined = printout(
            &format!(
                "{PREFIX}SELECT ?s ?x WHERE {{ ?s a asset360:Signal . \
                 OPTIONAL {{ ?s asset360:name ?nm . BIND(BNODE() AS ?x) }} }}"
            ),
            &schema,
        );
        assert!(
            refined.contains("G2c: the expression is not effect-free"),
            "{refined}"
        );
        assert_ne!(
            probe(
                "",
                "SELECT (COUNT(DISTINCT ?x) AS ?n) { VALUES ?a {1 2} OPTIONAL { { SELECT ?x { VALUES ?b {7} BIND(BNODE() AS ?x) } } } }"
            ),
            probe(
                "",
                "SELECT (COUNT(DISTINCT ?x) AS ?n) { VALUES ?a {1 2} OPTIONAL { VALUES ?b {7} } BIND(BNODE() AS ?x) }"
            ),
        );
    }

    // --- R1 ---------------------------------------------------------------

    /// The R1 shape on the fixture: a label table joined on a numeric key,
    /// which M1 declines (K4), inside the `OPTIONAL` that reads the key.
    fn r1_query(condition: &str) -> String {
        format!(
            "{PREFIX}SELECT ?s ?len ?lbl WHERE {{ ?s a asset360:Signal . \
             OPTIONAL {{ ?s asset360:length ?len . \
             OPTIONAL {{ VALUES (?len ?lbl) {{ (3 \"three\") (7 \"seven\") }} {condition} }} }} }}"
        )
    }

    /// **`r1_effect_free_condition`** / the shape R1 exists for: fires, and
    /// the finish over rows is the oracle's answer.
    #[test]
    fn r1_effect_free_condition() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        for condition in ["", "FILTER(STRLEN(?lbl) = 5)"] {
            let query = r1_query(condition);
            let refined = printout(&query, &schema);
            assert!(refined.contains("R1  "), "{condition}\n{refined}");
            let (_plan, rows) = rows_route_answer(&query, &schema, &oracle);
            assert_eq!(
                as_bag(&rows),
                as_bag(&oracle_rows(&query, &oracle)),
                "{condition}"
            );
        }
    }

    /// **`r1_volatile_condition`** (G1d): `RAND()` in the moved condition.
    /// Asserted deterministically: R1 declines and says so. The random draw
    /// is the design appendix's, and never runs here.
    #[test]
    fn r1_volatile_condition() {
        let schema = test_schema_view();
        let refined = printout(&r1_query("FILTER(RAND() < 0.5)"), &schema);
        assert!(!refined.contains("R1  "), "{refined}");
        assert!(refined.contains("G1d"), "{refined}");
    }

    /// **`r1_volatile_inside_c`** (G1d, the precaution): a volatile
    /// expression inside `C` declines too.
    #[test]
    fn r1_volatile_inside_c() {
        let schema = test_schema_view();
        let refined = printout(
            &format!(
                "{PREFIX}SELECT ?s ?len ?lbl WHERE {{ ?s a asset360:Signal . \
                 OPTIONAL {{ ?s asset360:length ?len . \
                 OPTIONAL {{ {{ VALUES (?len ?lbl) {{ (3 \"three\") }} FILTER(RAND() < 0.5) }} }} }} }}"
            ),
            &schema,
        );
        assert!(!refined.contains("R1  "), "{refined}");
    }

    /// **`r1_bound_j_tests_merged_mapping`** and **`r1_empty_shared_set`**:
    /// why the witness and not `BOUND(?j)`. `C` binds `?j` itself, so a test
    /// on it is true for a left row the body never matched; the witness is
    /// not, whatever the sides share.
    #[test]
    fn r1_the_witness_is_what_tests_the_match() {
        let bound_j = "SELECT * { VALUES ?a {1} OPTIONAL { VALUES ?j {2} FILTER(false) \
             OPTIONAL { VALUES (?j ?label) {(2 \"label\")} } } }";
        assert_ne!(
            probe("", bound_j),
            probe(
                "",
                "SELECT * { VALUES ?a {1} OPTIONAL { VALUES ?j {2} FILTER(false) } \
                 OPTIONAL { VALUES (?j ?label) {(2 \"label\")} FILTER(BOUND(?j)) } }"
            ),
            "revision 1's guard"
        );
        assert_eq!(
            probe("", bound_j),
            probe(
                "",
                "SELECT ?a ?j ?label { VALUES ?a {1} OPTIONAL { VALUES ?j {2} FILTER(false) BIND(true AS ?m) } \
                 OPTIONAL { VALUES (?j ?label) {(2 \"label\")} FILTER(BOUND(?m)) } }"
            ),
            "the witness"
        );
        let empty = "SELECT * { VALUES ?a {1} OPTIONAL { VALUES ?b {2} FILTER(false) \
             OPTIONAL { VALUES ?label {\"x\"} } } }";
        assert_eq!(
            probe("", empty),
            probe(
                "",
                "SELECT ?a ?b ?label { VALUES ?a {1} OPTIONAL { VALUES ?b {2} FILTER(false) BIND(true AS ?m) } \
                 OPTIONAL { VALUES ?label {\"x\"} FILTER(BOUND(?m)) } }"
            ),
        );
    }

    /// **`r1_c_shares_with_a_not_pinned_by_b`** (G1b).
    #[test]
    fn r1_c_shares_with_a_not_pinned_by_b() {
        let schema = test_schema_view();
        let refined = printout(
            &format!(
                "{PREFIX}SELECT ?s ?nm ?lbl WHERE {{ ?s a asset360:Signal ; asset360:name ?nm . \
                 OPTIONAL {{ ?s asset360:length ?len . \
                 OPTIONAL {{ VALUES (?len ?nm ?lbl) {{ (3 \"Alpha\" \"a\") }} }} }} }}"
            ),
            &schema,
        );
        assert!(refined.contains("G1b: ?nm shared by C"), "{refined}");
        assert_ne!(
            probe(
                "",
                "SELECT * { VALUES (?a ?k) {(1 7)} OPTIONAL { VALUES ?j {2} \
                 OPTIONAL { VALUES (?j ?k ?label) {(2 8 \"label\")} } } }"
            ),
            probe(
                "",
                "SELECT ?a ?k ?j ?label { VALUES (?a ?k) {(1 7)} OPTIONAL { VALUES ?j {2} BIND(true AS ?m) } \
                 OPTIONAL { VALUES (?j ?k ?label) {(2 8 \"label\")} FILTER(BOUND(?m)) } }"
            ),
        );
    }

    /// **`r1_inner_condition_reads_a`** (G1c).
    #[test]
    fn r1_inner_condition_reads_a() {
        assert_ne!(
            probe(
                "",
                "SELECT * { VALUES ?a {1} OPTIONAL { VALUES ?j {2} \
                 OPTIONAL { VALUES (?j ?label) {(2 \"label\")} FILTER(!BOUND(?a)) } } }"
            ),
            probe(
                "",
                "SELECT ?a ?j ?label { VALUES ?a {1} OPTIONAL { VALUES ?j {2} BIND(true AS ?m) } \
                 OPTIONAL { VALUES (?j ?label) {(2 \"label\")} FILTER(BOUND(?m) && !BOUND(?a)) } }"
            ),
        );
        let schema = test_schema_view();
        let refined = printout(
            &format!(
                "{PREFIX}SELECT ?s ?len ?lbl WHERE {{ ?s a asset360:Signal ; asset360:name ?nm . \
                 OPTIONAL {{ ?s asset360:length ?len . \
                 OPTIONAL {{ VALUES (?len ?lbl) {{ (3 \"three\") }} FILTER(!BOUND(?nm)) }} }} }}"
            ),
            &schema,
        );
        assert!(
            refined.contains("G1c: the inner condition reads ?nm"),
            "{refined}"
        );
    }

    // --- R3 ---------------------------------------------------------------

    /// **`r3_qualifying_page`**: a left join against a table unique on a key
    /// the left side binds is one-to-one, so the page moves into the
    /// statement -- and paging through it with `OFFSET` tiles the whole
    /// ordered answer.
    #[test]
    fn r3_qualifying_page() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let full = |page: &str| {
            format!(
                "{PREFIX}SELECT ?s ?len ?lbl WHERE {{ ?s a asset360:Signal ; asset360:length ?len . \
                 OPTIONAL {{ VALUES (?len ?lbl) {{ (3 \"three\") (7 \"seven\") (1 \"one\") }} }} }} \
                 ORDER BY ?s {page}"
            )
        };
        let refined = printout(&full("LIMIT 2"), &schema);
        assert!(refined.contains("R3  "), "{refined}");
        let expected = oracle_rows(&full(""), &oracle);
        let mut tiled = Vec::new();
        for offset in (0..expected.len()).step_by(2) {
            let (_plan, rows) =
                rows_route_answer(&full(&format!("LIMIT 2 OFFSET {offset}")), &schema, &oracle);
            tiled.extend(rows);
        }
        assert_eq!(tiled, expected, "the pages tile the ordered answer");
        assert_eq!(
            probe_sequence(
                "",
                "SELECT ?id ?label { VALUES (?id ?j) {(3 2) (1 3) (2 1)} \
                 OPTIONAL { VALUES (?j ?label) {(1 \"a\") (2 \"b\") (3 \"c\")} } } ORDER BY ?id LIMIT 2"
            ),
            probe_sequence(
                "",
                "SELECT ?id ?label { VALUES (?__ord ?id ?j) {(1 1 3) (2 2 1)} \
                 OPTIONAL { VALUES (?j ?label) {(1 \"a\") (2 \"b\") (3 \"c\")} } } ORDER BY ?__ord"
            ),
        );
    }

    /// **`r3_undef_left_key_fans_out`** (G3a): a key the left side may leave
    /// unbound is compatible with every row of the table.
    #[test]
    fn r3_undef_left_key_fans_out() {
        assert_ne!(
            probe(
                "",
                "SELECT * { VALUES (?id ?j) {(1 UNDEF) (2 3)} \
                 OPTIONAL { VALUES (?j ?label) {(1 \"a\") (2 \"b\") (3 \"c\")} } } ORDER BY ?id LIMIT 1"
            ),
            probe(
                "",
                "SELECT * { { SELECT * { VALUES (?id ?j) {(1 UNDEF) (2 3)} } ORDER BY ?id LIMIT 1 } \
                 OPTIONAL { VALUES (?j ?label) {(1 \"a\") (2 \"b\") (3 \"c\")} } }"
            ),
        );
        let schema = test_schema_view();
        let refined = printout(
            &format!(
                "{PREFIX}SELECT ?s ?len ?lbl WHERE {{ ?s a asset360:Signal . \
                 OPTIONAL {{ ?s asset360:length ?len }} \
                 OPTIONAL {{ VALUES (?len ?lbl) {{ (3 \"three\") (7 \"seven\") }} }} }} \
                 ORDER BY ?s LIMIT 2"
            ),
            &schema,
        );
        assert!(!refined.contains("R3  "), "{refined}");
    }

    /// **`r3_duplicate_key_in_c`** (G3a): two rows of the table on one key.
    #[test]
    fn r3_duplicate_key_in_c() {
        let schema = test_schema_view();
        let refined = printout(
            &format!(
                "{PREFIX}SELECT ?s ?len ?lbl WHERE {{ ?s a asset360:Signal ; asset360:length ?len . \
                 OPTIONAL {{ VALUES (?len ?lbl) {{ (3 \"three\") (3 \"drei\") }} }} }} \
                 ORDER BY ?s LIMIT 2"
            ),
            &schema,
        );
        assert!(refined.contains("G3a: two rows of"), "{refined}");
        assert!(!refined.contains("R3  "), "{refined}");
    }

    /// **`r3_volatile_bind_in_e`**: a `RAND()` above the page is evaluated
    /// once per answer row on both routes, so R3 fires. Only the `?s`
    /// sequence is compared.
    #[test]
    fn r3_volatile_bind_in_e() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let query = format!(
            "{PREFIX}SELECT ?s ?r WHERE {{ ?s a asset360:Signal . BIND(RAND() AS ?r) }} \
             ORDER BY ?s LIMIT 2"
        );
        assert!(printout(&query, &schema).contains("R3  "));
        let (_plan, rows) = rows_route_answer(&query, &schema, &oracle);
        let ids = |rows: &[serde_json::Value]| -> Vec<String> {
            rows.iter().map(|row| row["s"].to_string()).collect()
        };
        assert_eq!(ids(&rows), ids(&oracle_rows(&query, &oracle)));
        assert!(rows.iter().all(|row| row.get("r").is_some()));
    }

    /// A `LIMIT` with no `ORDER BY` moves too: any page is an answer, and no
    /// order crosses.
    #[test]
    fn r3_a_page_with_no_order() {
        let schema = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?s ?x WHERE {{ ?s a asset360:Signal . BIND(STR(?s) AS ?x) }} LIMIT 2"
        );
        let plan = lifted_plan(&query, &schema);
        let printed = plan.to_string();
        assert!(
            printed.contains("no ORDER BY, so no order crosses"),
            "{printed}"
        );
        assert!(!printed.contains("number"), "{printed}");
        let oracle = fixture(&schema);
        let (_plan, rows) = rows_route_answer(&query, &schema, &oracle);
        assert_eq!(rows.len(), 2);
    }
}

/// Composition, fallbacks and the evaluation context: the pipeline as a
/// whole rather than one rule at a time.
#[cfg(all(test, feature = "sparql-endpoint"))]
mod composition {
    use super::tests::{PREFIX, as_bag, lifted_plan, oracle_rows, printout, rows_route_answer};
    use crate::sparql_oracle::fixture;
    use crate::sparql_plan::{PassKind, Refinement};
    use crate::sparql_refine::{Node, Plan, PlanDefect, PlanOp};
    use crate::sparql_rules::Rule;
    use crate::sparql_scoper::tests::test_schema_view;

    /// P4 widened: the path from the statement to the restoring sort holds
    /// R2's witness-dropping projection and a left join against a table
    /// unique on a bound key (G3a).
    fn widened() -> String {
        format!(
            "{PREFIX}SELECT ?s ?name ?tail ?lbl WHERE {{ ?s a asset360:Signal ; \
             asset360:name ?name ; asset360:length ?len . \
             OPTIONAL {{ ?s asset360:locatedOnTrack ?e . \
             BIND(STRAFTER(STR(?e), \"/track/\") AS ?tail) }} \
             OPTIONAL {{ VALUES (?len ?lbl) {{ (3 \"three\") (7 \"seven\") }} }} }} \
             ORDER BY ?name LIMIT 10"
        )
    }

    /// **`r3_ordinal_through_projections`**: `?__ord` is in scope at every
    /// node between the numbering and the restoring sort, projections
    /// included, and only the query's final projection drops it; the answer
    /// is the oracle's.
    #[test]
    fn r3_ordinal_through_projections() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let query = widened();
        let plan = lifted_plan(&query, &schema);
        let printed = plan.to_string();
        assert!(
            printed.contains("R2  ") && printed.contains("R3  "),
            "{printed}"
        );
        super::ordinal_reaches_its_sort(&plan).unwrap();
        let number = plan
            .nodes
            .iter()
            .position(|node| matches!(node.op, PlanOp::Number { .. }))
            .expect(&printed);
        let mut at = number;
        let mut projections = 0;
        while !matches!(
            plan.nodes[at].op,
            PlanOp::Sort {
                origin: crate::sparql_refine::SortOrigin::Ordinal,
                ..
            }
        ) {
            assert!(plan.variables_of(at).contains("__ord"), "n{at}\n{printed}");
            if matches!(plan.nodes[at].op, PlanOp::Project { .. }) {
                projections += 1;
            }
            at = crate::sparql_rules::consumers_of(&plan, at)[0];
        }
        assert!(
            projections >= 1,
            "the path holds R2's projection:\n{printed}"
        );
        let root = plan.nodes.len() - 1;
        assert!(!plan.variables_of(root).contains("__ord"), "{printed}");

        let (_plan, rows) = rows_route_answer(&query, &schema, &oracle);
        let expected = oracle_rows(&query, &oracle);
        assert_eq!(as_bag(&rows), as_bag(&expected));
        let names = |rows: &[serde_json::Value]| -> Vec<String> {
            rows.iter().map(|row| row["name"].to_string()).collect()
        };
        assert_eq!(names(&rows), names(&expected));
    }

    /// **The mutation twin**: a rule that builds its projection as a keep
    /// list -- R2's edit done wrong -- drops the ordinal between the
    /// numbering and its sort, and the driver refuses it as a transition
    /// defect naming the rule.
    #[test]
    fn a_projection_built_as_a_keep_list_loses_the_ordinal() {
        struct KeepListProjection;
        impl Rule for KeepListProjection {
            fn name(&self) -> &'static str {
                super::R2
            }
            fn apply(&self, plan: &mut Plan) -> bool {
                // Above the numbering: a projection of the variables the
                // query selects, and nothing else.
                let Some(number) = plan
                    .nodes
                    .iter()
                    .position(|node| matches!(node.op, PlanOp::Number { .. }))
                else {
                    return false;
                };
                if plan
                    .nodes
                    .iter()
                    .any(|node| matches!(&node.op, PlanOp::Project { vars, .. } if vars == &["s".to_owned()]))
                {
                    return false;
                }
                super::insert_above(
                    plan,
                    number,
                    Node::engine(
                        PlanOp::Project {
                            input: number,
                            vars: vec!["s".to_owned()],
                        },
                        Vec::new(),
                    ),
                );
                true
            }
        }
        let schema = test_schema_view();
        let mut plan = lifted_plan(&widened(), &schema);
        let failure = crate::sparql_rules::refine(&mut plan, &[&KeepListProjection])
            .expect_err("the ordinal is lost");
        match failure.defect {
            PlanDefect::Transition {
                rule,
                defect: crate::sparql_scopes::TransitionDefect::OrdinalLost { .. },
            } => assert_eq!(rule, super::R2),
            other => panic!("{other}"),
        }
    }

    /// **The replanning path** (the reviewer's non-blocking ask): R2 fires,
    /// but the rows route is refused -- the region also holds an `EXISTS`,
    /// which reads instance data the rows do not carry -- so the planner
    /// refines again from the original plan, without the lifts. The records
    /// route then carries no witness and no ordinal, and its fetch no page
    /// or order of the query's that would drop records the engine needs.
    /// (The design's example, a dataset clause, is refused by the scoper
    /// before planning: `FROM` is an unsupported construct.)
    #[test]
    fn a_lift_the_rows_route_refuses_is_undone() {
        let schema = test_schema_view();
        let query = format!(
            "{PREFIX}SELECT ?s ?name ?tail WHERE {{ ?s a asset360:Signal ; \
             asset360:name ?name . OPTIONAL {{ ?s asset360:locatedOnTrack ?e . \
             BIND(STRAFTER(STR(?e), \"/track/\") AS ?tail) }} \
             FILTER(EXISTS {{ ?s asset360:length ?len }}) }} ORDER BY ?name LIMIT 1"
        );
        // The lift fires on it.
        let lifted = lifted_plan(&query, &schema).to_string();
        assert!(lifted.contains("R2  "), "{lifted}");
        let plan = crate::sparql_plan::plan_query_refined(&query, &schema).unwrap();
        assert!(
            !matches!(plan.refinement, Refinement::UsedRows(_)),
            "{plan}"
        );
        let printed = plan.to_string();
        assert!(!printed.contains("__ord"), "{printed}");
        assert!(!printed.contains("__m1"), "{printed}");
        for pass in &plan.passes {
            if let PassKind::Sql(sql) = &pass.kind {
                assert!(
                    !sql.ops
                        .nodes
                        .iter()
                        .any(|node| matches!(node.op, crate::sparql_ops::Op::Sort { .. })),
                    "the fetch carries no ORDER BY of the query's: {plan}"
                );
            }
        }
        // The records route re-runs the query itself over what it fetches,
        // so there is nothing further to compare: what this test holds is
        // that no lift's artifact reached that fetch.
    }

    /// **A mixed-column table stays the engine's and still answers through
    /// the rows route**: M1 declines it (K7), R1 lifts it out of its
    /// `OPTIONAL`, and the finish evaluates it.
    #[test]
    fn a_mixed_column_table_answers_over_rows() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let query = format!(
            "{PREFIX}SELECT ?s ?nm ?tag WHERE {{ ?s a asset360:Signal . \
             OPTIONAL {{ ?s asset360:name ?nm . \
             OPTIONAL {{ VALUES (?nm ?tag) {{ (\"Alpha\" \"a\") (\"Echo\" <urn:echo>) }} }} }} }}"
        );
        // Where it stood, M1 declines it on K7 ...
        let unlifted = crate::sparql_plan::with_declined(
            &{
                let mut plan = crate::sparql_refine::naive_plan_for(&query, &schema).unwrap();
                let rules = crate::sparql_plan::refine_rules(&schema, None, false);
                let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
                crate::sparql_rules::refine(&mut plan, &borrowed).unwrap();
                plan
            },
            &schema,
            None,
        );
        assert!(
            unlifted.contains("K7: column ?tag is not uniform"),
            "{unlifted}"
        );
        // ... so R1 lifts it, and it stays the engine's.
        let refined = printout(&query, &schema);
        assert!(refined.contains("R1  "), "{refined}");
        let (_plan, rows) = rows_route_answer(&query, &schema, &oracle);
        assert_eq!(as_bag(&rows), as_bag(&oracle_rows(&query, &oracle)));
    }

    /// **The evaluation context**: a `BASE` the query declares reaches the
    /// finish query, so an `IRI()` the engine evaluates over the rows
    /// resolves as the records route would resolve it.
    #[test]
    fn the_finish_query_keeps_the_base() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let query = format!(
            "BASE <https://example.org/base/> {PREFIX}SELECT ?s ?x WHERE {{ ?s a asset360:Signal . \
             BIND(IRI(\"relative\") AS ?x) }}"
        );
        let (plan, rows) = rows_route_answer(&query, &schema, &oracle);
        assert!(
            plan.to_string()
                .contains("BASE <https://example.org/base/>"),
            "{plan}"
        );
        assert_eq!(as_bag(&rows), as_bag(&oracle_rows(&query, &oracle)));
    }

    /// **A region with no write-back is the records route's**, decided at
    /// plan time: a `SERVICE` has no algebra this crate writes back, and it
    /// reads another dataset anyway.
    #[test]
    fn a_region_that_reads_instances_finishes_over_records() {
        let schema = test_schema_view();
        let plan = crate::sparql_plan::plan_query_refined(
            &format!(
                "{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal . \
                 FILTER(EXISTS {{ ?s asset360:name ?nm }}) }}"
            ),
            &schema,
        )
        .unwrap();
        assert!(matches!(plan.refinement, Refinement::Used(_)), "{plan}");
    }

    /// **Fresh names are fresh**: a query that already uses `?__m1` gets
    /// the witness `?__m2`.
    #[test]
    fn a_witness_name_the_query_uses_is_skipped() {
        let schema = test_schema_view();
        let printed = printout(
            &format!(
                "{PREFIX}SELECT ?s ?__m1 ?tail WHERE {{ ?s a asset360:Signal ; asset360:name ?__m1 . \
                 OPTIONAL {{ ?s asset360:locatedOnTrack ?e . \
                 BIND(STRAFTER(STR(?e), \"/track/\") AS ?tail) }} }}"
            ),
            &schema,
        );
        assert!(printed.contains("witness ?__m2"), "{printed}");
    }
}

#[cfg(all(test, feature = "sparql-endpoint"))]
mod r4 {
    use super::tests::{PREFIX, as_bag, oracle_rows, printout, rows_route_answer};
    use crate::sparql_oracle::{fixture, probe};
    use crate::sparql_scoper::tests::test_schema_view;

    /// The #494 composition in miniature: a `BIND` in one `OPTIONAL` and a
    /// reference in the next. R2 lifts the `BIND` above its own left join,
    /// R4 above the next one, and R3 pages the statement; the answer is the
    /// oracle's.
    #[test]
    fn an_extension_climbs_the_left_joins_above_it() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let query = format!(
            "{PREFIX}SELECT ?s ?name ?tail ?bg WHERE {{ ?s a asset360:Signal ; asset360:name ?name . \
             OPTIONAL {{ ?s asset360:locatedOnTrack ?e . \
             BIND(STRAFTER(STR(?e), \"/track/\") AS ?tail) }} \
             OPTIONAL {{ ?bg a asset360:BaliseGroup ; asset360:refersToSignal ?s }} }} \
             ORDER BY ?name LIMIT 10"
        );
        let refined = printout(&query, &schema);
        for rule in ["R2  ", "R4  ", "R3  "] {
            assert!(refined.contains(rule), "{rule}\n{refined}");
        }
        let (_plan, rows) = rows_route_answer(&query, &schema, &oracle);
        assert_eq!(as_bag(&rows), as_bag(&oracle_rows(&query, &oracle)));
    }

    /// Over a left join whose reference edge a tier-one rule recorded before
    /// the extension got there (`AbsorbOptionalReference`): the edge's ends
    /// are what the join compares, and the extension is neither.
    #[test]
    fn an_extension_climbs_over_a_recorded_reference() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let query = format!(
            "{PREFIX}SELECT ?s ?tail ?tn WHERE {{ ?s a asset360:Signal ; asset360:length ?len . \
             OPTIONAL {{ ?s asset360:name ?n2 . BIND(STRLEN(?n2) AS ?tail) }} \
             OPTIONAL {{ ?s asset360:locatedOnTrack ?t2 . ?t2 a asset360:Track ; asset360:hasName ?tn }} }}"
        );
        let refined = printout(&query, &schema);
        assert!(refined.contains("R4  "), "{refined}");
        assert!(refined.contains("via ?s.locatedOnTrack"), "{refined}");
        let (_plan, rows) = rows_route_answer(&query, &schema, &oracle);
        assert_eq!(as_bag(&rows), as_bag(&oracle_rows(&query, &oracle)));
    }

    /// **H2**: the other side supplies an input the extension did not have.
    #[test]
    fn an_input_the_other_side_supplies_declines() {
        assert_ne!(
            probe(
                "",
                "SELECT * { { VALUES ?a {1} BIND(COALESCE(?y, 0) AS ?x) } OPTIONAL { VALUES (?a ?y) {(1 5)} } }"
            ),
            probe(
                "",
                "SELECT * { VALUES ?a {1} OPTIONAL { VALUES (?a ?y) {(1 5)} } BIND(COALESCE(?y, 0) AS ?x) }"
            ),
        );
        let schema = test_schema_view();
        let refined = printout(
            &format!(
                "{PREFIX}SELECT ?s ?tail ?nm2 WHERE {{ ?s a asset360:Signal . \
                 OPTIONAL {{ ?s asset360:locatedOnTrack ?e . BIND(COALESCE(?nm2, STR(?e)) AS ?tail) }} \
                 OPTIONAL {{ ?s asset360:locatedOnTrack ?t2 . ?t2 a asset360:Track ; asset360:hasName ?nm2 }} }}"
            ),
            &schema,
        );
        assert!(refined.contains("H2: ?nm2 in scope in"), "{refined}");
    }

    /// **H1** and **H4**: the other side binds the extension's variable, or
    /// the join's condition reads it.
    #[test]
    fn a_join_on_the_extension_declines() {
        let schema = test_schema_view();
        let binds = printout(
            &format!(
                "{PREFIX}SELECT ?s ?tail WHERE {{ ?s a asset360:Signal . \
                 OPTIONAL {{ ?s asset360:locatedOnTrack ?e . BIND(STR(?e) AS ?tail) }} \
                 OPTIONAL {{ ?s asset360:locatedOnTrack ?t2 . ?t2 a asset360:Track ; asset360:hasName ?tail }} }}"
            ),
            &schema,
        );
        assert!(binds.contains("H1: ?tail is in scope in"), "{binds}");
        let reads = printout(
            &format!(
                "{PREFIX}SELECT ?s ?tail ?n2 WHERE {{ ?s a asset360:Signal . \
                 OPTIONAL {{ ?s asset360:locatedOnTrack ?e . BIND(STR(?e) AS ?tail) }} \
                 OPTIONAL {{ ?s asset360:locatedOnTrack ?t2 . ?t2 a asset360:Track ; asset360:hasName ?n2 \
                 FILTER(?n2 != ?tail) }} }}"
            ),
            &schema,
        );
        assert!(
            reads.contains("H4: the join's condition reads ?tail"),
            "{reads}"
        );
    }
}

/// **Alpha-renaming** (design, *Scopes, barriers and renaming*): renaming a
/// sub-select's private variables leaves the refined plan identical up to
/// the names -- which rules fired, which declined, and why.
#[cfg(all(test, feature = "sparql-endpoint"))]
mod alpha_renaming {
    use super::tests::{PREFIX, printout};
    use crate::sparql_scoper::tests::test_schema_view;

    /// `?name` as a whole variable, replaced by `?_`, and the printout's
    /// column padding collapsed -- a longer name shifts the columns.
    fn normalised(text: &str, name: &str) -> String {
        let needle = format!("?{name}");
        let mut out = String::new();
        let mut rest = text;
        while let Some(at) = rest.find(&needle) {
            let after = &rest[at + needle.len()..];
            let whole = !after
                .chars()
                .next()
                .is_some_and(|c| c.is_alphanumeric() || c == '_');
            out.push_str(&rest[..at]);
            out.push_str(if whole { "?_" } else { &needle });
            rest = after;
        }
        out.push_str(rest);
        out.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn same_up_to(original: &str, renamed: &str, from: &str, to: &str) {
        let schema = test_schema_view();
        let a = normalised(&printout(&format!("{PREFIX}{original}"), &schema), from);
        let b = normalised(&printout(&format!("{PREFIX}{renamed}"), &schema), to);
        assert_eq!(a, b, "renaming ?{from} to ?{to} changed the plan");
    }

    #[test]
    fn a_private_variable_renamed_plans_the_same() {
        // M1 inside a sub-select body (P5): the private key `?k`.
        same_up_to(
            "SELECT ?s ?lbl WHERE { ?s a asset360:Signal . OPTIONAL { { SELECT ?s ?lbl WHERE { \
             ?s a asset360:Signal ; asset360:name ?k . \
             VALUES (?k ?lbl) { (\"Alpha\" \"a\") (\"Echo\" \"e\") } } } } }",
            "SELECT ?s ?lbl WHERE { ?s a asset360:Signal . OPTIONAL { { SELECT ?s ?lbl WHERE { \
             ?s a asset360:Signal ; asset360:name ?kprivate . \
             VALUES (?kprivate ?lbl) { (\"Alpha\" \"a\") (\"Echo\" \"e\") } } } } }",
            "k",
            "kprivate",
        );
        // R2 declines a sub-select body, whatever its private names (P5's
        // decline): the private `?e`.
        same_up_to(
            "SELECT ?s ?tail WHERE { ?s a asset360:Signal . OPTIONAL { { SELECT ?s ?tail WHERE { \
             ?s a asset360:Signal ; asset360:locatedOnTrack ?e . \
             BIND(STR(?e) AS ?tail) } } } }",
            "SELECT ?s ?tail WHERE { ?s a asset360:Signal . OPTIONAL { { SELECT ?s ?tail WHERE { \
             ?s a asset360:Signal ; asset360:locatedOnTrack ?eprivate . \
             BIND(STR(?eprivate) AS ?tail) } } } }",
            "e",
            "eprivate",
        );
    }
}

/// **The route differential over the whole property grammar**: every
/// grammar query the planner sends down the rows route answers what the
/// oracle answers, through the statement's rows and `sparql_finish`.
#[cfg(all(test, feature = "sparql-endpoint"))]
mod grammar_differential {
    use super::tests::{PREFIX, as_bag, oracle_rows, rows_route_answer};
    use crate::sparql_oracle::fixture;
    use crate::sparql_plan::Refinement;
    use crate::sparql_scoper::tests::test_schema_view;

    #[test]
    fn every_rows_route_of_the_grammar_answers_the_oracle() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let mut routed = 0;
        for body in crate::sparql_algebra::equivalence::grammar() {
            let query = format!("{PREFIX}{body}");
            let Ok(plan) = crate::sparql_plan::plan_query_refined(&query, &schema) else {
                continue;
            };
            if !matches!(plan.refinement, Refinement::UsedRows(_)) {
                continue;
            }
            routed += 1;
            let (_plan, rows) = rows_route_answer(&query, &schema, &oracle);
            assert_eq!(
                as_bag(&rows),
                as_bag(&oracle_rows(&query, &oracle)),
                "{body}\n{plan}"
            );
        }
        assert!(routed > 0, "no grammar query took the rows route");
        println!("{routed} grammar queries through the rows route");
    }
}
