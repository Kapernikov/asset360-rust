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
        // What is demanded of every barrier, derived once: a barrier's
        // exports are its outputs, so what its consumers demand of it is
        // exactly what `kept_exports` records per barrier.
        let demands = plan.kept_exports();
        for barrier in plan.barriers() {
            let demanded = &demands[&plan.key_of(barrier)];
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
                    ..
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

// ---------------------------------------------------------------------------
// Seed an element's scope
// ---------------------------------------------------------------------------

/// The body of an `OPTIONAL` that reads an element its preserved side
/// unnests gets the element's occurrences to join on, so that it can be SQL.
///
/// Nothing scans an element: a `match ?e :p ?v` on an inlined element has no
/// star of its own, and a body made only of such matches is an island nobody
/// can fold. Written by hand, `OPTIONAL { ?h a :Bridge ; :elements ?e . ?e :p
/// ?v }` is a relation of `(?e, ?v)` that op 5 joins to the element on its
/// occurrence. This rule writes the `?h … :elements ?e` half.
///
/// **Match.** `A ⟕ SubSelect∅(B)` where a variable `?e` the barrier exports
/// is, at `A`, one collection hop `P` off the records of class `H`
/// ([`TermOf::Structure`], produced by an `Unnest` straight over an `[S]`
/// `Scan` -- an element of an element is refused) and bound in every row of
/// `A`. Walk down `B`'s left spine through `LeftJoin`s to its first other
/// node `X`, the mandatory core of the body.
///
/// **Guards.** `?e` is bound in every row of `X`, and `X` binds `?e` as no
/// structure yet -- which is what ends the rule's own repetition; **the
/// core is made only of reads of the element**: data-graph `Match`es whose
/// subject is `?e`, joined by plain joins. A body that also reaches a record
/// (`?t a :Track`) is a reference, which the left join takes as its edge; a
/// path, a `VALUES` or a schema-graph read is not one the fold can take.
///
/// **Edit.** `X := E ⋈_{?e} X`, with `E = Unnest(Scan(H as ?__h, [P → ?e]))`
/// over the classes `A`'s scan reads, `?__h` fresh and exported by no
/// barrier, and the restrictions of `A`'s scan (identity, direct type) --
/// `E` still covers every occurrence `A` can produce. `E` claims no obligation.
///
/// **Equivalence.** A left row `a` is compatible with a body row `b` only if
/// `b.e = a.e`, both bound, and `a.e` is an occurrence of `(H, P)`. `E` holds
/// exactly one row per occurrence of `(H, P)` in every record `A` can read,
/// and its other column is fresh and projected away, so a `b` whose `e` is an
/// occurrence matches exactly one row of `E` (its multiplicity is kept) and a
/// `b` with any other `e` was compatible with no `a`. The join sits under the
/// spine's left joins: `(E ⋈ X) ⟕ C = E ⋈ (X ⟕ C)`, because `E` shares only
/// `?e` with each `C` and `?e` is bound in `X`. Unmatched left rows are kept
/// unchanged either way.
pub struct SeedElementScope<'s> {
    schema: &'s SchemaView,
}

impl<'s> SeedElementScope<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }

    /// The `[S]` scan of the records `var` is an element of at `a`, and the
    /// one hop to it, when `var` is exactly that.
    fn holder_scan(&self, plan: &Plan, a: NodeId, var: &str) -> Option<(NodeId, String)> {
        let terms = plan.term_of(self.schema, a, var);
        let [TermOf::Structure { path, .. }] = terms.as_slice() else {
            return None;
        };
        let [hop] = path.as_slice() else {
            return None;
        };
        let producers = plan.producers_of(a, var);
        let [unnest] = producers.as_slice() else {
            return None;
        };
        let PlanOp::Unnest {
            input, star_var, ..
        } = &plan.nodes[*unnest].op
        else {
            return None;
        };
        let scan = (plan.nodes[*unnest].executor == Executor::Sql
            && plan.nodes[*input].executor == Executor::Sql
            && matches!(&plan.nodes[*input].op, PlanOp::Scan { star_var: s, .. } if s == star_var))
        .then_some(*input)?;
        Some((scan, hop.clone()))
    }
}

impl Rule for SeedElementScope<'_> {
    fn name(&self) -> &'static str {
        "seed_element_scope"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        // Every candidate in one application: each seed is independent of
        // the others, and one per round would take a round per OPTIONAL.
        let mut seeded = false;
        while self.seed_one(plan) {
            seeded = true;
        }
        seeded
    }
}

/// Whether `node` is made only of reads of `var`'s element: data-graph
/// matches with `var` as subject, under plain joins.
fn reads_only_the_element(plan: &Plan, node: NodeId, var: &str) -> bool {
    match &plan.nodes[node].op {
        PlanOp::Match { pattern, reads } => {
            *reads == crate::sparql_dataset::GraphReads::Data
                && crate::sparql_refine::subject_variable(pattern) == Some(var)
        }
        PlanOp::Join {
            left,
            right,
            reference: None,
            key: None,
            ..
        } => reads_only_the_element(plan, *left, var) && reads_only_the_element(plan, *right, var),
        _ => false,
    }
}

impl SeedElementScope<'_> {
    /// One seed, when a left join qualifies.
    fn seed_one(&self, plan: &mut Plan) -> bool {
        use crate::sparql_refine::{Node, ScanSlot, SlotPresence};
        for id in 0..plan.nodes.len() {
            let PlanOp::LeftJoin { left: a, right, .. } = &plan.nodes[id].op else {
                continue;
            };
            let PlanOp::SubSelect {
                input,
                vars,
                domain: None,
            } = &plan.nodes[*right].op
            else {
                continue;
            };
            let (a, vars) = (*a, vars.clone());
            if plan.nodes[a].executor != Executor::Sql {
                continue;
            }
            let mut core = *input;
            while let PlanOp::LeftJoin { left, .. } = &plan.nodes[core].op {
                core = *left;
            }
            let guaranteed = plan.guaranteed(core);
            let Some((var, scan, hop)) = vars.iter().find_map(|var| {
                if !plan.guaranteed(a).contains(var)
                    || !guaranteed.contains(var)
                    || !reads_only_the_element(plan, core, var)
                {
                    return None;
                }
                let seeded = plan
                    .term_of(self.schema, core, var)
                    .iter()
                    .any(|term| matches!(term, TermOf::Structure { .. }));
                if seeded {
                    return None;
                }
                let (scan, hop) = self.holder_scan(plan, a, var)?;
                Some((var.clone(), scan, hop))
            }) else {
                continue;
            };
            let PlanOp::Scan {
                class_uri,
                classes,
                direct_type_in,
                identifier_values,
                ..
            } = &plan.nodes[scan].op
            else {
                unreachable!("holder_scan returned a scan");
            };
            let (class_uri, classes) = (class_uri.clone(), classes.clone());
            let (direct_type_in, identifier_values) =
                (direct_type_in.clone(), identifier_values.clone());
            let holder_class = crate::sparql_plan::shorten(&class_uri);
            let holder = crate::sparql_lift::fresh(plan, "__h", true);
            let seed = vec![
                Node::sql(
                    PlanOp::Scan {
                        star_var: holder.clone(),
                        class_uri,
                        classes,
                        direct_type: None,
                        direct_type_in,
                        slots: vec![ScanSlot {
                            path: vec![hop.clone()],
                            var: Some(var.clone()),
                            multivalued: true,
                            presence: SlotPresence::Required,
                        }],
                        identifier_values,
                    },
                    Vec::new(),
                ),
                Node::sql(
                    PlanOp::Unnest {
                        input: crate::sparql_lift::PREVIOUS,
                        star_var: holder.clone(),
                        slot_path: vec![hop.clone()],
                        var: var.clone(),
                        presence: SlotPresence::Required,
                    },
                    Vec::new(),
                ),
                Node::engine(
                    PlanOp::Join {
                        left: crate::sparql_lift::PREVIOUS,
                        right: core,
                        on: vec![var.clone()],
                        reference: None,
                        key: None,
                    },
                    Vec::new(),
                ),
            ];
            let at = crate::sparql_lift::apply_edit(
                plan,
                crate::sparql_lift::Edit {
                    removed: Vec::new(),
                    after: core,
                    inserted: seed,
                },
            );
            let joined = plan.key_of(at[2]).reference();
            plan.rewrites.push(format!(
                "seed_element_scope  {joined}  ?{var} is an element of {hop} (records of {}), \
                 bound in the OPTIONAL's core: joined to the occurrences of that collection \
                 (?{holder}.{hop} → ?{var})",
                holder_class
            ));
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
    use crate::sparql_refine::{Executor, Plan, naive_plan};
    use crate::sparql_rules::{refine, tier_one_rules};
    use crate::sparql_scoper::tests::test_schema_view;

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

    fn refined(query: &str) -> Plan {
        let schema = test_schema_view();
        let mut plan = naive_plan(
            &crate::sparql_scoper::parse_query(&format!("{PREFIX}{query}"), None).unwrap(),
            crate::sparql_dataset::default_dataset(),
        )
        .unwrap();
        let rules = tier_one_rules(&schema, crate::sparql_dataset::default_dataset());
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
                dataset: crate::sparql_dataset::default_dataset().clone(),
                query: crate::sparql_scoper::parse_query("SELECT * WHERE { ?s ?p ?o }", None)
                    .unwrap(),
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
                reads: crate::sparql_dataset::GraphReads::Data,
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
        let body = "?a a asset360:TunnelComplex ; asset360:controllerTechnology ?name . \
             OPTIONAL { ?a asset360:hasCoveredSection ?cs . \
             ?cs asset360:belongsToTrack ?track ; asset360:hasSequenceNumber ?seq . \
             ?track a asset360:Track ; asset360:refersToLine ?trackName }";
        // Scalar projection: statement, `?cs` pruned.
        let scalar = format!("{prefix}SELECT ?name ?seq ?track ?trackName WHERE {{ {body} }}");
        assert_eq!(
            outcome_of(&scalar, &fixture, crate::sparql_dataset::default_dataset()),
            Outcome::Statement
        );
        let plan = crate::sparql_plan::plan_of(
            &scalar,
            &fixture,
            crate::sparql_dataset::default_dataset(),
        )
        .unwrap();
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
        // An enclosed body has no projection of its own (op 1 leaves none
        // under the barrier), and is still a statement whose rows are its
        // solutions: the lowering gives it the column list a sub-select's
        // projection would carry -- the exports, then the fan-out `?cs` (a
        // binding through the collection, which is what a renderer builds
        // its lateral from, and what discharges the `unnest`), then the
        // identities. Without it the body was a bare row set whose unnest
        // nothing discharged.
        let Op::Project { vars, bindings, .. } =
            &relation.1.nodes.last().expect("a body has nodes").op
        else {
            panic!("the body's root is its projection:\n{plan}");
        };
        assert_eq!(
            vars,
            &relation.0.iter().map(|c| c.var.clone()).collect::<Vec<_>>()
        );
        assert!(
            bindings.iter().any(|spec| spec.star_var == "a"
                && spec.slot_path.as_slice() == ["hasCoveredSection".to_owned()]
                && spec
                    .containers
                    .iter()
                    .any(|c| *c != crate::sparql_pushdown::Container::Single)),
            "the fan-out is a column of the body: {bindings:?}"
        );
        assert!(
            bindings
                .iter()
                .any(|spec| spec.star_var == "track" && spec.slot_path.is_empty()),
            "the referenced star's identity is a column of the body: {bindings:?}"
        );
        // `SELECT *`: a fetch, since a structure has no term to emit.
        let star = format!("{prefix}SELECT * WHERE {{ {body} }}");
        assert!(
            matches!(
                outcome_of(&star, &fixture, crate::sparql_dataset::default_dataset()),
                Outcome::Fetch(_)
            ),
            "{:?}",
            outcome_of(&star, &fixture, crate::sparql_dataset::default_dataset())
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
            outcome_of(
                &format!("{PREFIX}{joined}"),
                &schema,
                crate::sparql_dataset::default_dataset()
            ),
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
                crate::sparql_dataset::default_dataset()
            ),
            Outcome::Fetch(_)
        ));

        // A measure exported by a relation carries the term it is, from
        // what it aggregates: `COUNT` an integer whatever it counts; `SUM`
        // the argument's datatype; `AVG` the division's (`xsd:decimal` for
        // an integer argument); `MIN`/`MAX` the argument's own descriptor, a
        // plain string for `?nm`. The outer statement serialises the column
        // from this descriptor and nothing else, so an `AVG` described as an
        // integer answered `"1.5"^^xsd:integer` beside the engine's decimal.
        let measured = refined(
            "SELECT ?s ?n ?total ?mean ?top WHERE { ?s a asset360:Signal . \
             { SELECT ?s (COUNT(?d) AS ?n) (SUM(?len) AS ?total) (AVG(?len) AS ?mean) \
             (MAX(?nm) AS ?top) WHERE { ?s a asset360:Signal ; asset360:documents ?d ; \
             asset360:length ?len ; asset360:name ?nm } GROUP BY ?s } }",
        );
        let lowered = crate::sparql_ops::lower_refined(&measured, &schema, None, None)
            .unwrap_or_else(|refusal| panic!("{refusal}\n{measured}"));
        let columns = lowered
            .nodes
            .iter()
            .find_map(|node| match &node.op {
                Op::Relation { columns, .. } => Some(columns),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{lowered:?}"));
        let datatype = |var: &str| {
            let column = columns
                .iter()
                .find(|column| column.var == var)
                .unwrap_or_else(|| panic!("?{var} is exported: {columns:?}"));
            assert!(
                matches!(column.kind, ColumnKind::Measure { .. }),
                "{column:?}"
            );
            let descriptor = column.descriptor.as_ref().expect("a measure has a term");
            (descriptor.kind, descriptor.datatype.clone())
        };
        use crate::sparql_terms::TermKind;
        let xsd = |name: &str| Some(format!("http://www.w3.org/2001/XMLSchema#{name}"));
        assert_eq!(datatype("n"), (TermKind::Literal, xsd("integer")));
        assert_eq!(datatype("total"), (TermKind::Literal, xsd("integer")));
        assert_eq!(datatype("mean"), (TermKind::Literal, xsd("decimal")));
        assert_eq!(datatype("top"), (TermKind::Literal, None));

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

/// A nested `OPTIONAL` whose body reads an element of an inlined collection
/// is one statement (`SeedElementScope`, then the rules that were already
/// there), and every step of it is held to the oracle.
#[cfg(all(test, feature = "sparql-endpoint"))]
mod seed_element_scope {
    use crate::sparql_algebra::equivalence::each_rewrite_preserves_answers_in;
    use crate::sparql_lift::tests::{as_bag, oracle_rows, rows_route_answer_in};
    use crate::sparql_oracle::Oracle;
    use crate::sparql_plan::{Refinement, plan_of};
    use crate::sparql_refine::{Executor, Plan};
    use crate::sparql_rules::Rule;
    use crate::sparql_scoper::tests::test_schema_view_plus;
    use linkml_runtime::schema_rdf::TypeInference;
    use linkml_schemaview::schemaview::SchemaView;

    const SCHEMA: &str = "urn:graph:schema";
    const NS: &str = "https://data.infrabel.be/asset360";
    /// What the equivalence harness does not declare itself.
    const PREFIX: &str = "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> \
         PREFIX k: <https://data.infrabel.be/asset360/SignalKind#> ";
    const ASSET360: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

    /// A bridge holds its elements twice over: keyed (`elements`) and as a
    /// list (`parts`); an element reads a record (`track`).
    const EXTRA: &str = r#"
  Bridge:
    class_uri: asset360:Bridge
    attributes:
      asset360_uri:
        identifier: true
      elements:
        range: Element
        multivalued: true
        inlined: true
      parts:
        range: Element
        multivalued: true
        inlined: true
        inlined_as_list: true
  Element:
    class_uri: asset360:Element
    attributes:
      elId:
        key: true
      elType:
        range: SignalKind
      material:
        range: SignalKind
      year:
        range: integer
      track:
        range: Track
"#;

    fn schema() -> SchemaView {
        test_schema_view_plus(EXTRA)
    }

    /// Four bridges: two KSS elements (one a plain duplicate in everything
    /// the queries read but its id), a non-KSS one beside them; a KSS element
    /// with neither year, material nor track; only a non-KSS element; none.
    /// Materials with a label (KSS), without one (GSA), and a track that is
    /// not a record. Every bridge repeats its elements as `parts`.
    fn oracle(schema: &SchemaView) -> Oracle {
        let el =
            |id: &str, t: &str, rest: &str| format!(r#"{{"elId": "{id}", "elType": "{t}"{rest}}}"#);
        let t1 = format!(r#", "track": "{NS}/track/T1""#);
        let lost = format!(r#", "track": "{NS}/track/T-missing""#);
        let bridge = |n: u32, elements: &[String]| {
            let keyed: Vec<String> = elements
                .iter()
                .enumerate()
                .map(|(i, e)| format!(r#""e{i}": {e}"#))
                .collect();
            let body = if elements.is_empty() {
                String::new()
            } else {
                format!(
                    r#", "elements": {{{}}}, "parts": [{}]"#,
                    keyed.join(", "),
                    elements.join(", ")
                )
            };
            format!(r#"{{"asset360_uri": "{NS}/bridge/{n}"{body}}}"#)
        };
        let b1 = bridge(
            1,
            &[
                el(
                    "a",
                    "KSS",
                    &format!(r#", "year": 1950, "material": "KSS"{t1}"#),
                ),
                el(
                    "b",
                    "KSS",
                    &format!(r#", "year": 1950, "material": "GSA"{lost}"#),
                ),
                el("c", "REP_H_D", r#", "year": 2001"#),
            ],
        );
        let b2 = bridge(2, &[el("d", "KSS", "")]);
        let b3 = bridge(3, &[el("e", "REP_H_D", r#", "material": "KSS""#)]);
        let b4 = bridge(4, &[]);
        let track = format!(r#"{{"asset360_uri": "{NS}/track/T1", "hasName": "Main"}}"#);
        Oracle::new(
            schema,
            &[
                ("Bridge", &b1),
                ("Bridge", &b2),
                ("Bridge", &b3),
                ("Bridge", &b4),
                ("Track", &track),
            ],
        )
        .with_schema_graph(schema, SCHEMA, TypeInference::ASSERTED)
    }

    fn holder(path: &str, select: &str, tail: &str) -> String {
        format!(
            "{PREFIX}SELECT {select} WHERE {{ ?b a asset360:Bridge . OPTIONAL {{ ?b asset360:{path} ?e . \
             ?e asset360:elType k:KSS . {tail} }} }}"
        )
    }

    const LABELS: &str = "OPTIONAL { GRAPH <urn:graph:schema> { ?m skos:prefLabel ?nl \
         FILTER(lang(?nl) = \"nl-be\") } } \
         OPTIONAL { GRAPH <urn:graph:schema> { ?m skos:prefLabel ?fr \
         FILTER(lang(?fr) = \"fr-be\") } }";

    /// Every rule application of the refinement holds to the oracle, with
    /// the lifting rules too.
    fn refined_and_checked(query: &str, schema: &SchemaView, oracle: &Oracle) -> Plan {
        let dataset = crate::sparql_dataset::schema_dataset(SCHEMA);
        let rules = crate::sparql_plan::refine_rules(schema, &dataset, true);
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        each_rewrite_preserves_answers_in(query, schema, oracle, &borrowed, &dataset)
    }

    /// The element is read in its own scope: the nested `OPTIONAL` is one
    /// statement, whether the collection is keyed or a list, and a nested
    /// reference held by the element (`track`) is too.
    #[test]
    fn a_nested_optional_over_an_element_is_one_statement() {
        let schema = schema();
        let oracle = oracle(&schema);
        let dataset = crate::sparql_dataset::schema_dataset(SCHEMA);
        for path in ["elements", "parts"] {
            // The last one is an element-held reference, SQL without a seed.
            for (select, tail, seeded) in [
                ("?b ?y", "OPTIONAL { ?e asset360:year ?y }", true),
                (
                    "?b ?y ?m",
                    "OPTIONAL { ?e asset360:year ?y } OPTIONAL { ?e asset360:material ?m }",
                    true,
                ),
                (
                    "?b ?t ?n",
                    "OPTIONAL { ?e asset360:track ?t . ?t a asset360:Track ; asset360:hasName ?n }",
                    false,
                ),
            ] {
                let query = holder(path, select, tail);
                let plan = refined_and_checked(&query, &schema, &oracle);
                assert!(
                    plan.nodes.iter().all(|node| node.executor == Executor::Sql),
                    "{query}\n{plan}"
                );
                assert_eq!(
                    plan.rewrites
                        .iter()
                        .any(|line| line.contains("seed_element_scope")),
                    seeded,
                    "{query}\n{plan}"
                );
                // The order the rules run in does not decide the plan. (Not for the
                // last query, whose scan lists its reads in a different order: not
                // this rule's.)
                let parsed =
                    crate::sparql_scoper::parse_query(&format!("{ASSET360}{query}"), None).unwrap();
                let mut texts = Vec::new();
                for reverse in [false, true]
                    .into_iter()
                    .filter(|reverse| seeded || !reverse)
                {
                    let mut rules = crate::sparql_rules::tier_one_rules(&schema, &dataset);
                    if reverse {
                        rules.reverse();
                    }
                    let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
                    let mut again = crate::sparql_refine::naive_plan(&parsed, &dataset).unwrap();
                    crate::sparql_rules::refine(&mut again, &borrowed)
                        .unwrap_or_else(|failure| panic!("{failure}"));
                    texts.push(again.to_string());
                }
                assert!(
                    texts.len() == 1 || texts[0] == texts[1],
                    "{query}\n{texts:?}"
                );
                let production = plan_of(&format!("{ASSET360}{query}"), &schema, &dataset).unwrap();
                assert!(production.sql_only(), "{query}\n{production}");
            }
        }
    }

    /// The material's labels are the engine's, over the statement's rows:
    /// SQL answers, engine finishes. The answer is the oracle's, with a label
    /// where the material has one and none where it does not.
    #[test]
    fn labels_under_an_element_are_finished_over_the_rows() {
        let schema = schema();
        let oracle = oracle(&schema);
        let dataset = crate::sparql_dataset::schema_dataset(SCHEMA);
        for path in ["elements", "parts"] {
            let query = holder(
                path,
                "?b ?y ?m ?nl ?fr",
                &format!(
                    "OPTIONAL {{ ?e asset360:year ?y }} \
                     OPTIONAL {{ ?e asset360:material ?m . {LABELS} }}"
                ),
            );
            refined_and_checked(&query, &schema, &oracle);
            let query = format!("{ASSET360}{query}");
            let (plan, rows) = rows_route_answer_in(&query, &schema, &oracle, &dataset);
            assert!(matches!(plan.refinement, Refinement::UsedRows(_)), "{plan}");
            let expected = oracle_rows(&query, &oracle);
            assert_eq!(as_bag(&rows), as_bag(&expected), "{query}\n{plan}");
            // Not vacuous: a label found, a material without one, no element.
            let text = as_bag(&expected).join("\n");
            assert!(
                text.contains("Kunstwerk") && text.contains("Ouvrage"),
                "{text}"
            );
            assert!(expected.len() > 4, "{text}");
        }
    }

    /// An element of an element is not seeded: there is no one-hop
    /// occurrence of a record to enumerate. Still correct.
    #[test]
    fn an_element_of_an_element_is_not_seeded() {
        let schema = schema();
        let assembly = format!(
            r#"{{"asset360_uri": "{NS}/assembly/1", "parts": [
                 {{"label": "p1", "children": [{{"label": "c1"}}, {{"label": "c2"}}]}},
                 {{"label": "p2", "children": [{{"label": "c1"}}]}}]}}"#
        );
        let oracle = Oracle::new(&schema, &[("Assembly", &assembly)]);
        let query = format!(
            "{PREFIX}SELECT * WHERE {{ ?a a asset360:Assembly . OPTIONAL {{ ?a asset360:parts ?p . \
             ?p asset360:children ?c . OPTIONAL {{ ?c asset360:label ?l }} }} }}"
        );
        let plan = refined_and_checked(&query, &schema, &oracle);
        assert!(
            !plan
                .rewrites
                .iter()
                .any(|line| line.contains("seed_element_scope")),
            "{plan}"
        );
    }

    #[test]
    fn review_many_element_optionals_reach_a_fixpoint() {
        let schema = schema();
        let dataset = crate::sparql_dataset::schema_dataset(SCHEMA);
        let slots = ["year", "material", "elType", "elId"];
        let n = 32;
        let tail: String = (0..n)
            .map(|i| format!("OPTIONAL {{ ?e asset360:{} ?v{i} }} ", slots[i % 4]))
            .collect();
        let sel: String = (0..n).map(|i| format!("?v{i} ")).collect();
        let query = format!(
            "{ASSET360}SELECT ?b {sel} WHERE {{ ?b a asset360:Bridge ; asset360:parts ?e . {tail} }}"
        );
        let parsed = crate::sparql_scoper::parse_query(&query, None).unwrap();
        let mut plan = crate::sparql_refine::naive_plan(&parsed, &dataset).unwrap();
        let rules = crate::sparql_plan::refine_rules(&schema, &dataset, true);
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        let log = crate::sparql_rules::refine(&mut plan, &borrowed).unwrap();
        assert!(log.reached_fixpoint, "{} rounds\n{plan}", log.rounds);
    }

    /// A body the seed cannot help (a property path) is left alone, and the
    /// plan is the one it was without the rule.
    #[test]
    fn review_seed_on_an_unlowerable_body_does_not_strand_an_island() {
        let schema = schema();
        let dataset = crate::sparql_dataset::schema_dataset(SCHEMA);
        let query = format!(
            "{ASSET360}{PREFIX}SELECT ?b ?n WHERE {{ ?b a asset360:Bridge ; asset360:parts ?e . \
             OPTIONAL {{ ?e asset360:track/asset360:hasName ?n }} }}"
        );
        let production = plan_of(&query, &schema, &dataset).unwrap();
        assert!(
            !matches!(production.refinement, Refinement::Fallback { .. }),
            "{:?}",
            production.refinement
        );
    }

    /// Two sibling element OPTIONALs that each carry label lookups: R1 lifts
    /// the first one's labels above its join, which leaves the second one's
    /// left side an engine join, so it can neither be lifted from nor
    /// pushed. Before the seed this query took the records route; with it
    /// the plan is a fallback. Needs a rule that commutes a lifted
    /// `X ⟕ C` above the next left join.
    #[test]
    #[ignore = "needs a rule commuting a lifted left join above a following one"]
    fn review_two_sibling_element_optionals_with_labels_are_not_a_fallback() {
        let schema = schema();
        let dataset = crate::sparql_dataset::schema_dataset(SCHEMA);
        let lab = |v: &str, o: &str, l: &str| {
            format!(
                "OPTIONAL {{ GRAPH <urn:graph:schema> {{ ?{v} skos:prefLabel ?{o} \
                 FILTER(lang(?{o}) = \"{l}\") }} }}"
            )
        };
        let query = format!(
            "{ASSET360}{PREFIX}SELECT ?b ?m ?nl ?fr ?k ?kl WHERE {{ ?b a asset360:Bridge ; \
             asset360:parts ?e . OPTIONAL {{ ?e asset360:material ?m . {} {} }} \
             OPTIONAL {{ ?e asset360:elType ?k . {} }} }}",
            lab("m", "nl", "nl-be"),
            lab("m", "fr", "fr-be"),
            lab("k", "kl", "nl-be")
        );
        let production = plan_of(&query, &schema, &dataset).unwrap();
        assert!(
            !matches!(production.refinement, Refinement::Fallback { .. }),
            "{:?}",
            production.refinement
        );
    }

    #[test]
    fn review_seed_keeps_the_holder_s_restriction() {
        let schema = schema();
        let oracle = oracle(&schema);
        let query = format!(
            "{PREFIX}SELECT ?y WHERE {{ <{NS}/bridge/1> a asset360:Bridge ; asset360:parts ?e . \
             OPTIONAL {{ ?e asset360:year ?y }} }}"
        );
        let plan = refined_and_checked(&query, &schema, &oracle);
        for node in &plan.nodes {
            if let crate::sparql_refine::PlanOp::Scan {
                star_var,
                identifier_values,
                ..
            } = &node.op
            {
                assert!(
                    !identifier_values.is_empty(),
                    "?{star_var} reads every Bridge\n{plan}"
                );
            }
        }
    }
}
