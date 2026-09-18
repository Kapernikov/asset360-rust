//! The rules that read a scope: prune what nothing demands, push a barrier
//! as a relation, push a join on a column key.
//!
//! Ops 4 and 5 and `PruneUnusedExports` of
//! `docs/design/sparql-scopes-as-relations.md`. None of them names a
//! predicate, a class or a query shape: each is a precondition on the
//! derived properties of [`crate::sparql_scopes`] and an equivalence
//! argument, stated on the rule.

use linkml_schemaview::schemaview::SchemaView;

use crate::sparql_refine::{Executor, JoinKey, NodeId, Plan, PlanOp};
use crate::sparql_rules::Rule;
use crate::sparql_scopes::{TermOf, representable};

// ---------------------------------------------------------------------------
// Prune unused exports
// ---------------------------------------------------------------------------

/// An export nothing above the barrier observes leaves its interface.
///
/// **Match.** A barrier and a `?v` among its `vars` that is not in
/// [`Plan::demand_above`] of the barrier -- the sixth derived property,
/// which says what a consumer *observes*: a name it reads, or every output
/// of its input for an operator that compares whole mappings (`DISTINCT`,
/// `REDUCED`, `COUNT(DISTINCT *)`) or decides compatibility on shared
/// variables (a join, a `MINUS`). "No reference above resolves to it" is
/// not the fact this needs: those observe a column and name it nowhere.
///
/// **Edit.** `vars := vars \ {?v}`.
///
/// **Equivalence.** Two halves. SPARQL `Project` is a bag projection
/// (§18.5), so removing a column changes no row count and no other
/// column's value; *and* by `demand`'s contract no consumer's answer depends
/// on the column's presence or value.
///
/// **Caught by** the transition check in the driver, *no demanded export is
/// dropped* -- a state invariant cannot see this class of lie, because
/// `demand` recomputed on the pruned plan agrees with the pruned interface.
///
/// Demand-driven pruning, and not "change the user's `SELECT` list": the
/// query's own projection demands exactly what it names.
pub struct PruneUnusedExports;

impl Rule for PruneUnusedExports {
    fn name(&self) -> &'static str {
        "prune_unused_exports"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for barrier in plan.barriers() {
            let demanded = plan.demand_above(barrier);
            let PlanOp::SubSelect { vars, .. } = &plan.nodes[barrier].op else {
                continue;
            };
            if vars.iter().all(|var| demanded.contains(var)) {
                continue;
            }
            let kept: Vec<String> = vars
                .iter()
                .filter(|var| demanded.contains(*var))
                .cloned()
                .collect();
            if let PlanOp::SubSelect { vars, .. } = &mut plan.nodes[barrier].op {
                *vars = kept;
            }
            crate::sparql_rules::refresh_join_variables(plan);
            return true;
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Op 4: push a barrier
// ---------------------------------------------------------------------------

/// A scope whose body is entirely SQL and whose every export is a column
/// becomes a relation: the projection rule, at a scope root.
///
/// **Match.** An `Exporting` barrier that is `[E]`, whose input is `[S]`,
/// whose `correlated_inputs` is empty, and every export of which either has
/// no producer (unbound in every row -- legal, and nothing to render) or
/// resolves through the body to a *representable* column: an identity, a
/// slot with a term, a measure, or a structure by occurrence. The barrier
/// is the scope root by construction, so the sub-query's own `Project`,
/// `Sort`, `Distinct` and `Slice` are nodes of its input, and `input [S]`
/// means the rules anchored at the barrier took every one of them. A
/// `Testing` scope never matches: it is not a relation the outside joins.
///
/// **Edit.** Flip the barrier to `[S]`. The lowering emits a derived table.
///
/// **Equivalence.** The body's rows *are* its solutions -- "every node below
/// is SQL" plus the statement's emit condition, the admission 28d's path B
/// applies to a whole query, applied to a scope. A `SubSelect` over
/// solutions is a column selection, and a derived table over a statement's
/// rows is the same column selection.
///
/// **Inherited refusals, unchanged.** Every decline of `PushGrouping` and
/// `PushProjection` leaves an `[E]` node in the body, so the barrier does
/// not match; there is no new refusal vocabulary.
///
/// **Caught by** `frontier_is_a_cut` (a barrier flipped over an `[E]` node),
/// the lowering's `Unrenderable` (an export no column binds), and
/// `fanout_restored` (a body that fans out above a collapse).
pub struct PushBarrier<'s> {
    schema: &'s SchemaView,
}

impl<'s> PushBarrier<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }
}

impl Rule for PushBarrier<'_> {
    fn name(&self) -> &'static str {
        "push_barrier"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for barrier in plan.barriers() {
            if plan.nodes[barrier].executor != Executor::Engine {
                continue;
            }
            let PlanOp::SubSelect { input, vars, .. } = &plan.nodes[barrier].op else {
                continue;
            };
            let (input, vars) = (*input, vars.clone());
            if plan.nodes[input].executor != Executor::Sql {
                continue;
            }
            if !plan.correlated_inputs(barrier).is_empty() {
                continue;
            }
            let representable = vars.iter().all(|var| {
                plan.producers_of(input, var).is_empty()
                    || representable(self.schema, plan, input, var, false)
            });
            if !representable {
                continue;
            }
            plan.nodes[barrier].executor = Executor::Sql;
            return true;
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Op 5: push a join on a column key
// ---------------------------------------------------------------------------

/// A join whose sides are both SQL and whose one shared variable is the
/// same record's identity on both -- or the same element's occurrence, or
/// no shared variable at all -- becomes SQL, keyed by column.
///
/// The widening of `PushReferenceJoin` and `PushLeftJoin` the design names
/// op 5: those two join a foreign key to an identity, this joins two
/// identities. It is what lets a relation be joined to a star, since a
/// relation exports identities and never a foreign key.
///
/// **Match.** An `[E]` `Join` or `LeftJoin` whose sides are both `[S]` and
/// whose `on` is:
///
/// * **empty**, with at least one side a pushed relation: a natural join on
///   no shared variable is every pair, `CROSS JOIN` (a `LeftJoin` with no
///   condition is `LEFT JOIN … ON true`). Restricted to a relation side
///   because that is the scalar-beside-every-row shape; two bare scans with
///   nothing in common stay two islands, as today;
/// * **one variable `?v`** that both sides bind as the identity of one
///   class ([`Plan::identity_class`]) **and** guarantee
///   ([`Plan::guaranteed`]) -- two derived properties, asked separately: an
///   identity that passed through a `LeftJoin` right side has the first and
///   not the second;
/// * **one variable `?v`** that both sides bind as a structure of one
///   `(holder class, path)`, guaranteed on both: equality on the whole
///   occurrence identifier, which is SPARQL's term equality on a blank node
///   within one graph.
///
/// A `LeftJoin`'s `condition` must be `None`. A join on two or more
/// variables is a later widening.
///
/// **Edit.** Flip to `[S]`; record the [`JoinKey`].
///
/// **Equivalence.** SPARQL joins on *term equality* of the shared variable.
/// Two identities of one class are two IRIs, the identity column holds the
/// IRI text, and text equality of two IRIs is term equality -- which is why
/// the rule is identity (and occurrence) only: a value join compares stored
/// text where SPARQL compares terms, and `"1"` and `"1.0"` are equal numbers
/// and different terms. **Guaranteed, again:** an unbound `?v` is compatible
/// with everything in SPARQL and `NULL = x` is never true in SQL, so the
/// rule requires `?v` bound on both sides.
///
/// **Caught by** [`Plan::join_keys_agree`].
pub struct PushJoinOnIdentity<'s> {
    schema: &'s SchemaView,
}

impl<'s> PushJoinOnIdentity<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }

    /// The key two sides may be joined on, when there is one.
    fn key_between(
        &self,
        plan: &Plan,
        left: NodeId,
        right: NodeId,
        on: &[String],
    ) -> Option<JoinKey> {
        match on {
            [] => {
                // A scalar relation: a pushed barrier whose body yields at
                // most one row -- a keyless grouping, or `LIMIT 1` -- under
                // the projection and its renames. A cross product of two row
                // sets is the same equivalence and a fetch the size of the
                // product; it stays two islands, as today, until measured.
                let scalar = |side: NodeId| {
                    matches!(plan.nodes[side].op, PlanOp::SubSelect { .. })
                        && plan.nodes[side].executor == Executor::Sql
                        && !plan.transparent(side)
                        && at_most_one_row(plan, side)
                };
                (scalar(left) || scalar(right)).then_some(JoinKey::Cross)
            }
            [var] => {
                if !plan.guaranteed(left).contains(var) || !plan.guaranteed(right).contains(var) {
                    return None;
                }
                if let (Some(a), Some(b)) = (
                    plan.identity_class(self.schema, left, var),
                    plan.identity_class(self.schema, right, var),
                ) {
                    return (a == b).then_some(JoinKey::Identity {
                        var: var.clone(),
                        class_uri: a,
                    });
                }
                let structure = |side: NodeId| -> Option<(String, Vec<String>)> {
                    let terms = plan.term_of(self.schema, side, var);
                    let [
                        TermOf::Structure {
                            class_uri: _,
                            holder_star,
                            path,
                        },
                    ] = terms.as_slice()
                    else {
                        return None;
                    };
                    let holder_class = plan
                        .term_of(self.schema, side, holder_star)
                        .into_iter()
                        .find_map(|term| match term {
                            TermOf::Identity { class_uri } => Some(class_uri),
                            _ => None,
                        })?;
                    Some((holder_class, path.clone()))
                };
                let (a, b) = (structure(left)?, structure(right)?);
                (a == b).then_some(JoinKey::Element {
                    var: var.clone(),
                    holder_class_uri: a.0,
                    path: a.1,
                })
            }
            _ => None,
        }
    }
}

impl Rule for PushJoinOnIdentity<'_> {
    fn name(&self) -> &'static str {
        "push_join_on_identity"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for id in 0..plan.nodes.len() {
            if plan.nodes[id].executor != Executor::Engine {
                continue;
            }
            let (left, right, on, is_left_join) = match &plan.nodes[id].op {
                PlanOp::Join {
                    left,
                    right,
                    on,
                    reference: None,
                    key: None,
                } => (*left, *right, on.clone(), false),
                PlanOp::LeftJoin {
                    left,
                    right,
                    reference: None,
                    key: None,
                    condition: None,
                } => {
                    let on: Vec<String> = plan
                        .variables_of(*left)
                        .intersection(&plan.variables_of(*right))
                        .cloned()
                        .collect();
                    (*left, *right, on, true)
                }
                _ => continue,
            };
            if plan.nodes[left].executor != Executor::Sql
                || plan.nodes[right].executor != Executor::Sql
            {
                continue;
            }
            let Some(key) = self.key_between(plan, left, right, &on) else {
                continue;
            };
            let _ = is_left_join;
            plan.nodes[id].executor = Executor::Sql;
            match &mut plan.nodes[id].op {
                PlanOp::Join { key: slot, .. } | PlanOp::LeftJoin { key: slot, .. } => {
                    *slot = Some(key);
                }
                _ => unreachable!("matched above"),
            }
            return true;
        }
        false
    }
}

/// Whether a subtree yields at most one row: a keyless `Group` or a `LIMIT
/// 1` slice, reached through nodes that never add rows.
fn at_most_one_row(plan: &Plan, node: NodeId) -> bool {
    match &plan.nodes[node].op {
        PlanOp::Group { keys, .. } => keys.is_empty(),
        PlanOp::Slice {
            limit: Some(limit), ..
        } => *limit <= 1,
        PlanOp::SubSelect { input, .. }
        | PlanOp::Project { input, .. }
        | PlanOp::Bind { input, .. }
        | PlanOp::Filter { input, .. }
        | PlanOp::Sort { input, .. }
        | PlanOp::Distinct { input }
        | PlanOp::Reduced { input }
        | PlanOp::Slice { input, .. } => at_most_one_row(plan, *input),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::sparql_refine::{Executor, Plan, naive_plan_of};
    use crate::sparql_rules::{refine, tier_one_rules};
    use crate::sparql_scoper::tests::test_schema_view;

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

    fn refined(query: &str) -> Plan {
        let schema = test_schema_view();
        let mut plan = naive_plan_of(&format!("{PREFIX}{query}")).unwrap();
        let rules = tier_one_rules(&schema, None);
        let borrowed: Vec<&dyn crate::sparql_rules::Rule> =
            rules.iter().map(|rule| rule.as_ref()).collect();
        refine(&mut plan, &borrowed).unwrap_or_else(|failure| panic!("{failure}\n{plan}"));
        plan
    }

    /// #466, plan by plan: a count beside a row. Two stars of `?s`, one per
    /// scope, each folded; the grouping pushed inside the sub-query, anchored
    /// at its barrier; the barrier pushed (op 4); the join on the identity
    /// pushed (op 5); the root projection taken. Every node `[S]`.
    #[test]
    fn a_grouped_sub_select_on_the_driving_variable_is_one_statement() {
        let plan = refined(
            "SELECT ?nm ?n WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             { SELECT ?s (COUNT(?d) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:documents ?d } GROUP BY ?s } }",
        );
        println!("{plan}");
        assert_eq!(plan.find("scan").len(), 2, "a star per scope:\n{plan}");
        assert!(
            plan.nodes.iter().all(|node| node.executor == Executor::Sql),
            "every node runs in SQL:\n{plan}"
        );
        let schema = test_schema_view();
        let ops = crate::sparql_ops::lower_refined(&plan, &schema, None, None)
            .unwrap_or_else(|refusal| panic!("{refusal}\n{plan}"));
        let printed = format!(
            "{}",
            crate::sparql_plan::ExecutionPlan {
                contract: crate::sparql_plan::PLAN_CONTRACT,
                passes: vec![crate::sparql_plan::Pass {
                    id: 0,
                    inputs: Vec::new(),
                    discharges: ops.claims(),
                    emits: Vec::new(),
                    kind: crate::sparql_plan::PassKind::Sql(Box::new(
                        crate::sparql_plan::SqlPass { ops: ops.clone() }
                    )),
                }],
                residual: Vec::new(),
                obligations: plan.obligations.clone(),
                refinement: crate::sparql_plan::Refinement::UsedAlone(String::new()),
            }
        );
        println!("{printed}");
        assert!(printed.contains("relation  q0"), "{printed}");
        assert!(
            printed.contains("join      identity s.<identity> = q0.s"),
            "{printed}"
        );
        assert!(
            printed.contains("discharges o0 o1 o2 o3 o4 o5"),
            "{printed}"
        );
    }
}
