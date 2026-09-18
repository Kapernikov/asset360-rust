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
                            holder_class_uri,
                            path,
                            ..
                        },
                    ] = terms.as_slice()
                    else {
                        return None;
                    };
                    Some((holder_class_uri.clone(), path.clone()))
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
    /// **Invariants driven by a bad rule** (design test 3): each lie a rule
    /// could tell about a scope fails at the rule, not in a result. Every
    /// case builds a sound plan, edits it the way a wrong rule would, and
    /// names the invariant that refuses it.
    #[test]
    fn a_bad_rule_fails_the_scope_invariant_that_catches_it() {
        use crate::sparql_refine::{JoinKey, PlanDefect, PlanOp};
        use crate::sparql_scopes::ScopeDefect;
        let schema = test_schema_view();

        // A join key on a slot column: `?nm` is a value, not an identity.
        let mut plan = refined(
            "SELECT ?nm ?n WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             { SELECT ?s (COUNT(?d) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:documents ?d } GROUP BY ?s } }",
        );
        let join = plan.find("join")[0];
        if let PlanOp::Join { key, .. } = &mut plan.nodes[join].op {
            *key = Some(JoinKey::Identity {
                var: "nm".to_owned(),
                class_uri: "https://data.infrabel.be/asset360/Signal".to_owned(),
            });
        }
        assert!(
            matches!(
                plan.check_with(&schema),
                Err(PlanDefect::Scope(ScopeDefect::MisrecordedKey { .. }))
            ),
            "{plan}"
        );

        // An identity join on a key `term_of` accepts and `guaranteed` does
        // not: the relation's `?s` comes through a left join, so an inner
        // join above keyed on it lies about boundness.
        let mut plan = refined(
            "SELECT ?nm ?n WHERE { ?s a asset360:Signal ; asset360:name ?nm . OPTIONAL { \
             { SELECT ?s (COUNT(?d) AS ?n) WHERE { ?s a asset360:Signal ; asset360:documents ?d } \
             GROUP BY ?s } } }",
        );
        let leftjoin = plan.find("leftjoin")[0];
        let (left, right) = match &plan.nodes[leftjoin].op {
            PlanOp::LeftJoin { left, right, .. } => (*left, *right),
            _ => unreachable!(),
        };
        // Swap the sides: the relation becomes the *preserved* side and the
        // outer scan the optional one, so `?s` is no longer guaranteed on
        // the right -- and the recorded key now lies.
        if let PlanOp::LeftJoin {
            left: l, right: r, ..
        } = &mut plan.nodes[leftjoin].op
        {
            *l = right;
            *r = left;
        }
        crate::sparql_rules::refresh_join_variables(&mut plan);
        let _ = plan.check_with(&schema); // may or may not agree on the swap itself
        // The clean version of the same lie: key the join on a variable the
        // right side binds optionally.
        let mut plan = refined(
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             OPTIONAL { ?s asset360:locatedOnTrack ?t . ?t a asset360:Track } }",
        );
        let root = plan.nodes.len() - 1;
        let input = plan.nodes[root].op.inputs()[0];
        // Wrap the root's input in a join with itself keyed on `?t`, which
        // the optional side binds and nothing guarantees.
        let bad = crate::sparql_refine::Node::sql(
            PlanOp::Join {
                left: input,
                right: input,
                on: vec!["t".to_owned()],
                reference: None,
                key: Some(JoinKey::Identity {
                    var: "t".to_owned(),
                    class_uri: "https://data.infrabel.be/asset360/Track".to_owned(),
                }),
            },
            Vec::new(),
        );
        let mut nodes = plan.nodes.clone();
        nodes.insert(root, bad);
        nodes[root + 1].op.map_inputs(|_| root);
        let remap: Vec<Option<usize>> = (0..plan.nodes.len())
            .map(|old| Some(if old == root { root + 1 } else { old }))
            .collect();
        plan.rebuild(nodes, &remap);
        assert!(
            matches!(
                plan.check_with(&schema),
                Err(PlanDefect::Scope(ScopeDefect::MisrecordedKey { .. }))
            ),
            "{plan}"
        );

        // An optional-side filter reparented to the outer scope: the
        // obligation was raised inside the `OPTIONAL`, and a node outside
        // may not discharge it.
        // (Written as a sub-select inside the `OPTIONAL`, since spargebra
        // lifts a body's own `FILTER` into the left join's condition.)
        let mut plan = refined(
            "SELECT ?s ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             OPTIONAL { { SELECT ?t ?tn WHERE { ?t a asset360:Track ; asset360:hasName ?tn . \
             FILTER(REGEX(?tn, \"^A\")) } } } }",
        );
        let filter = plan.find("filter")[0];
        let claims = std::mem::take(&mut plan.nodes[filter].discharges);
        let root = plan.nodes.len() - 1;
        plan.nodes[root].discharges.extend(claims);
        assert!(
            matches!(
                plan.check(),
                Err(PlanDefect::Scope(
                    ScopeDefect::ObligationLeftItsScope { .. }
                ))
            ),
            "{plan}"
        );

        // A barrier read by two nodes: not one relation any more.
        let mut plan = refined(
            "SELECT ?nm ?n WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             { SELECT ?s (COUNT(?d) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:documents ?d } GROUP BY ?s } }",
        );
        let barrier = plan.barriers()[0];
        let root = plan.nodes.len() - 1;
        plan.nodes[root].op.map_inputs(|_| barrier);
        assert!(
            matches!(
                plan.check(),
                Err(PlanDefect::Scope(ScopeDefect::BarrierConsumers {
                    consumers: 2,
                    ..
                }))
            ),
            "{plan}"
        );

        // A pushed filter naming a slot of the star inside the relation:
        // resolved from outside its barrier.
        let mut plan = refined(
            "SELECT ?nm ?n WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             { SELECT ?s (COUNT(?d) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:documents ?d } GROUP BY ?s } }",
        );
        let join = plan.find("join")[0];
        let root = plan.nodes.len() - 1;
        let bad = crate::sparql_refine::Node::sql(
            PlanOp::Filter {
                input: join,
                condition: crate::sparql_refine::Expr::Compare {
                    op: crate::sparql_refine::CompareOp::Eq,
                    left: Box::new(crate::sparql_refine::Expr::Slot {
                        // The inner scan's star is also `?s`, and its slot
                        // `documents` is one the outer scan does not read --
                        // but closure is about the *scan*, so name a star no
                        // visible scan has.
                        star_var: "d".to_owned(),
                        slot_path: vec!["docId".to_owned()],
                        reading: crate::sparql_refine::SlotReading::Column,
                        presence: crate::sparql_refine::SlotPresence::Required,
                    }),
                    right: Box::new(crate::sparql_refine::Expr::Literal(
                        spargebra::term::Term::Literal(
                            spargebra::term::Literal::new_simple_literal("x"),
                        ),
                    )),
                },
            },
            Vec::new(),
        );
        let mut nodes = plan.nodes.clone();
        nodes.insert(root, bad);
        nodes[root + 1].op.map_inputs(|_| root);
        let remap: Vec<Option<usize>> = (0..plan.nodes.len())
            .map(|old| Some(if old == root { root + 1 } else { old }))
            .collect();
        plan.rebuild(nodes, &remap);
        assert!(
            matches!(
                plan.check(),
                Err(PlanDefect::Scope(ScopeDefect::SlotOutOfScope { .. }))
            ),
            "{plan}"
        );

        // A rule that retires a key the ledger names with no successor.
        let mut plan = refined(
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             OPTIONAL { { SELECT ?t ?tn WHERE { ?t a asset360:Track ; asset360:hasName ?tn . \
             FILTER(REGEX(?tn, \"^A\")) } } } }",
        );
        let barrier = plan.barriers()[0];
        let key = plan.key_of(barrier);
        plan.retire(key, None);
        // Still live, so it resolves; now drop it from the node list the
        // way a careless rule would, with no successor recorded.
        plan.nodes[barrier].key = crate::sparql_refine::NodeKey(plan.next_key + 100);
        assert!(
            matches!(
                plan.check(),
                Err(PlanDefect::Scope(ScopeDefect::EvidenceLost { .. }))
            ),
            "{plan}"
        );
    }

    /// **The transition check** (design test 3, round 6): an export pruned
    /// under `COUNT(DISTINCT *)`, under `DISTINCT *`, and one shared with a
    /// `MINUS` right side, each applied *through `refine`* by a rule that
    /// prunes with no match -- so *no demanded export is dropped* is what
    /// rejects them -- and each asserting that `Plan::check` *passes* on
    /// the pruned plan, which is why the check is a transition and not a
    /// state invariant: `demand` recomputed on the pruned plan agrees with
    /// the pruned interface.
    #[test]
    fn a_prune_past_its_match_fails_at_the_rule_and_nowhere_after() {
        use crate::sparql_refine::{Node, PlanDefect, PlanOp, QueryForm};
        use crate::sparql_rules::Rule;
        use spargebra::term::{GroundTerm, Literal, Variable};

        struct PrunesY;
        impl Rule for PrunesY {
            fn name(&self) -> &'static str {
                "prunes_y_without_looking"
            }
            fn apply(&self, plan: &mut Plan) -> bool {
                for barrier in plan.barriers() {
                    if let PlanOp::SubSelect { vars, .. } = &mut plan.nodes[barrier].op
                        && vars.iter().any(|var| var == "y")
                    {
                        vars.retain(|var| var != "y");
                        crate::sparql_rules::refresh_join_variables(plan);
                        return true;
                    }
                }
                false
            }
        }

        let values = || {
            let one = |value: &str| {
                Some(GroundTerm::Literal(Literal::new_typed_literal(
                    value,
                    spargebra::term::NamedNode::new_unchecked(
                        "http://www.w3.org/2001/XMLSchema#integer",
                    ),
                )))
            };
            PlanOp::Values {
                variables: vec![Variable::new_unchecked("x"), Variable::new_unchecked("y")],
                rows: vec![vec![one("1"), one("10")], vec![one("1"), one("20")]],
            }
        };
        let sub_select = |nodes: &mut Vec<Node>| -> usize {
            nodes.push(Node::engine(values(), Vec::new()));
            nodes.push(Node::engine(
                PlanOp::Project {
                    input: 0,
                    vars: vec!["x".to_owned(), "y".to_owned()],
                },
                Vec::new(),
            ));
            nodes.push(Node::engine(
                PlanOp::SubSelect {
                    input: 1,
                    vars: vec!["x".to_owned(), "y".to_owned()],
                    domain: Some(1),
                },
                Vec::new(),
            ));
            2
        };

        // `SELECT (COUNT(DISTINCT *) AS ?n) WHERE { { SELECT ?x ?y … } }`
        let mut nodes = Vec::new();
        let barrier = sub_select(&mut nodes);
        nodes.push(Node::engine(
            PlanOp::Group {
                input: barrier,
                keys: Vec::new(),
                measures: vec![crate::sparql_refine::Measure {
                    var: "n".to_owned(),
                    aggregate: spargebra::algebra::AggregateExpression::CountSolutions {
                        distinct: true,
                    },
                }],
                having: Vec::new(),
            },
            Vec::new(),
        ));
        nodes.push(Node::engine(
            PlanOp::Project {
                input: barrier + 1,
                vars: vec!["n".to_owned()],
            },
            Vec::new(),
        ));
        let count_distinct = Plan::from_nodes(QueryForm::Select, Vec::new(), nodes);

        // `SELECT DISTINCT * WHERE { { SELECT ?x ?y … } }`
        let mut nodes = Vec::new();
        let barrier = sub_select(&mut nodes);
        nodes.push(Node::engine(
            PlanOp::Distinct { input: barrier },
            Vec::new(),
        ));
        nodes.push(Node::engine(
            PlanOp::Project {
                input: barrier + 1,
                vars: vec!["x".to_owned(), "y".to_owned()],
            },
            Vec::new(),
        ));
        let distinct_star = Plan::from_nodes(QueryForm::Select, Vec::new(), nodes);

        // `SELECT ?x WHERE { { SELECT ?x ?y … } MINUS { ?z :p ?y } }`
        let mut nodes = Vec::new();
        let barrier = sub_select(&mut nodes);
        let triple = spargebra::term::TriplePattern {
            subject: spargebra::term::TermPattern::Variable(Variable::new_unchecked("z")),
            predicate: spargebra::term::NamedNodePattern::NamedNode(
                spargebra::term::NamedNode::new_unchecked("https://data.infrabel.be/asset360/p"),
            ),
            object: spargebra::term::TermPattern::Variable(Variable::new_unchecked("y")),
        };
        nodes.push(Node::engine(
            PlanOp::Match {
                pattern: Box::new(triple),
            },
            Vec::new(),
        ));
        nodes.push(Node::engine(
            PlanOp::Minus {
                left: barrier,
                right: barrier + 1,
            },
            Vec::new(),
        ));
        nodes.push(Node::engine(
            PlanOp::Project {
                input: barrier + 2,
                vars: vec!["x".to_owned()],
            },
            Vec::new(),
        ));
        let minus = Plan::from_nodes(QueryForm::Select, Vec::new(), nodes);

        for (name, plan) in [
            ("COUNT(DISTINCT *)", count_distinct),
            ("DISTINCT *", distinct_star),
            ("MINUS on ?y", minus),
        ] {
            plan.check()
                .unwrap_or_else(|defect| panic!("{name}: {defect}\n{plan}"));
            // The legitimate rule does not touch `?y`: it is demanded.
            let mut untouched = plan.clone();
            assert!(
                !super::PruneUnusedExports.apply(&mut untouched),
                "{name}\n{untouched}"
            );
            // The bad rule prunes it anyway, and the driver refuses the edit
            // -- while the pruned plan passes every state check, which is
            // the point.
            let mut pruned = plan.clone();
            let failure = refine(&mut pruned, &[&PrunesY]).expect_err(name);
            assert!(
                matches!(
                    failure.defect,
                    PlanDefect::Transition {
                        rule: "prunes_y_without_looking",
                        ..
                    }
                ),
                "{name}: {failure}"
            );
            assert!(failure.to_string().contains("?y"), "{name}: {failure}");
            pruned.check().unwrap_or_else(|defect| {
                panic!("{name}: a state check saw it: {defect}\n{pruned}")
            });
        }
    }
    /// **The structure interface** (design test 12): #464 as a scalar
    /// projection is a `Statement` with `?cs` pruned from the relation; as
    /// `SELECT *` it is a `Fetch` -- the root projection cannot emit a blank
    /// node, and the engine does; an element joined in an enclosing scope is
    /// a `Statement` on `JoinKey::Element`; a `GROUP BY` on an element
    /// groups by occurrence; and the nested occurrence fans out twice, each
    /// step by occurrence, so `parts[0].children[1]` and
    /// `parts[1].children[1]` are two rows.
    #[test]
    fn the_structure_interface() {
        use crate::sparql_ops::{ColumnKind, JoinKey, Op, UnnestDedup};
        use crate::sparql_plan::{Outcome, outcome_of};
        use crate::sparql_scoper::tests::asset360_fixture_schema_view;
        let fixture = asset360_fixture_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";
        let body = "?a a asset360:TunnelComplex ; asset360:typeURI ?name . \
             OPTIONAL { ?a asset360:hasCoveredSection ?cs . \
             ?cs asset360:belongsToTrack ?track ; asset360:hasSequenceNumber ?seq . \
             ?track a asset360:Track ; asset360:typeURI ?trackName }";
        // Scalar projection: statement, `?cs` pruned.
        let scalar = format!("{prefix}SELECT ?name ?seq ?track ?trackName WHERE {{ {body} }}");
        assert_eq!(outcome_of(&scalar, &fixture, None), Outcome::Statement);
        let plan = crate::sparql_plan::plan_query_refined(&scalar, &fixture).unwrap();
        let ops = match &plan.passes[0].kind {
            crate::sparql_plan::PassKind::Sql(sql) => &sql.ops,
            _ => unreachable!(),
        };
        let relation = ops
            .nodes
            .iter()
            .find_map(|node| match &node.op {
                Op::Relation { columns, body, .. } => Some((columns, body)),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{plan}"));
        assert!(
            !relation.0.iter().any(|column| column.var == "cs"),
            "?cs is pruned:\n{plan}"
        );
        assert!(
            relation
                .0
                .iter()
                .any(|column| column.var == "seq" && matches!(column.kind, ColumnKind::Slot(_))),
            "{plan}"
        );
        // The body's own join is the element-held edge, read off the element.
        assert!(
            relation.1.nodes.iter().any(|node| matches!(&node.op,
                Op::Join { right_path, right_reading, .. }
                    if right_path.as_slice() == ["hasCoveredSection".to_owned()]
                        && *right_reading == crate::sparql_ops::SlotReading::BoundElement)),
            "{plan}"
        );
        // `SELECT *`: a fetch, since a structure has no term to emit.
        let star = format!("{prefix}SELECT * WHERE {{ {body} }}");
        assert!(
            matches!(outcome_of(&star, &fixture, None), Outcome::Fetch(_)),
            "{:?}",
            outcome_of(&star, &fixture, None)
        );

        // An element joined in an enclosing scope, on the test schema's
        // nested arrays: the assembly's parts each with a count of their
        // children.
        let schema = test_schema_view();
        let joined = "SELECT ?l ?n WHERE { ?a a asset360:Assembly ; asset360:parts ?p . \
             ?p asset360:label ?l . \
             { SELECT ?p (COUNT(?c) AS ?n) WHERE { ?a a asset360:Assembly ; asset360:parts ?p . \
             ?p asset360:children ?c } GROUP BY ?p } }";
        let plan = refined(joined);
        let join = plan.find("join")[0];
        assert!(
            matches!(&plan.nodes[join].op, crate::sparql_refine::PlanOp::Join {
                key: Some(crate::sparql_refine::JoinKey::Element { path, .. }), .. }
                if path.as_slice() == ["parts".to_owned()]),
            "{plan}"
        );
        assert_eq!(
            outcome_of(&format!("{PREFIX}{joined}"), &schema, None),
            Outcome::Statement
        );
        let lowered = crate::sparql_ops::lower_refined(&plan, &schema, None, None)
            .unwrap_or_else(|refusal| panic!("{refusal}\n{plan}"));
        assert!(
            lowered.nodes.iter().any(|node| matches!(&node.op,
                Op::Join { key: JoinKey::Element { left, right }, .. }
                    if left.column.is_none() && right.column.as_deref() == Some("p"))),
            "{lowered:?}"
        );
        // With the element projected at the root: a fetch.
        assert!(matches!(
            outcome_of(
                &format!(
                    "{PREFIX}SELECT ?p ?n WHERE {{ ?a a asset360:Assembly ; asset360:parts ?p . \
                     {{ SELECT ?p (COUNT(?c) AS ?n) WHERE {{ ?a a asset360:Assembly ; \
                     asset360:parts ?p . ?p asset360:children ?c }} GROUP BY ?p }} }}"
                ),
                &schema,
                None
            ),
            Outcome::Fetch(_)
        ));

        // The nested occurrence: two fan-outs, each by occurrence.
        let nested = refined(
            "SELECT (COUNT(?c) AS ?n) WHERE { ?h a asset360:Assembly ; asset360:parts ?p . \
             ?p asset360:children ?c }",
        );
        assert_eq!(
            plan_unnests(&nested),
            vec![vec!["parts"], vec!["parts", "children"]],
            "{nested}"
        );
        let lowered = crate::sparql_ops::lower_refined(&nested, &schema, None, None)
            .unwrap_or_else(|refusal| panic!("{refusal}\n{nested}"));
        let dedups: Vec<UnnestDedup> = lowered
            .nodes
            .iter()
            .filter_map(|node| match &node.op {
                Op::Unnest { dedup, .. } => Some(*dedup),
                _ => None,
            })
            .collect();
        assert_eq!(
            dedups,
            vec![UnnestDedup::ByOccurrence, UnnestDedup::ByOccurrence]
        );
        assert!(
            nested
                .nodes
                .iter()
                .all(|node| node.executor == Executor::Sql),
            "{nested}"
        );
        // And a scalar collection dedups by value.
        let scalar = refined(
            "SELECT (COUNT(?k) AS ?n) WHERE { ?s a asset360:Signal ; asset360:trafficKinds ?k }",
        );
        let lowered = crate::sparql_ops::lower_refined(&scalar, &schema, None, None).unwrap();
        assert!(lowered.nodes.iter().any(|node| matches!(
            &node.op,
            Op::Unnest {
                dedup: UnnestDedup::ByValue,
                ..
            }
        )));
    }

    fn plan_unnests(plan: &Plan) -> Vec<Vec<&str>> {
        plan.nodes
            .iter()
            .filter_map(|node| match &node.op {
                crate::sparql_refine::PlanOp::Unnest { slot_path, .. } => {
                    Some(slot_path.iter().map(String::as_str).collect())
                }
                _ => None,
            })
            .collect()
    }
}
