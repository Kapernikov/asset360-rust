//! Rules, a fixpoint driver, and the first rule: fold matches into a scan.
//!
//! A rule is a function on a plan: match a shape, edit nodes, move claims.
//! Applied to fixpoint in a fixed order, with every invariant re-checked after
//! each application -- cheap, and the difference between a rule chain that can
//! be trusted and one that cannot. A rule that forgets to move an obligation,
//! claims one twice, or reparents a node without renumbering then fails a
//! check instead of quietly answering a different question, which is the
//! failure this whole line of work exists to prevent.
//!
//! Refinement is correct by construction here for a reason that is worth
//! stating once: **the engine leg re-runs the whole query.** `views.py` hands
//! oxigraph the original SPARQL string together with the materialised
//! instances, so whatever SQL filtered, the engine filters again. Every rule
//! below the first row-collapsing operator therefore only narrows, and a
//! narrowing cannot change an answer. The rules that would move a *collapsing*
//! operator -- group, distinct, slice, sort -- are not here, and they are the
//! ones that need a residual evaluator.

use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::slotview::SlotContainerMode;

use crate::sparql_plan::ObligationId;
use crate::sparql_refine::{
    Executor, Node, NodeId, Plan, PlanOp, ScanSlot, inner_join_groups, is_type_pattern,
    object_variable, predicate_iri, scan_with_fanout, subject_variable, type_class_iri,
};

/// One rewrite.
pub trait Rule {
    /// Stable name, for the log a reader checks a plan against.
    fn name(&self) -> &'static str;
    /// Edit the plan; `true` when something changed.
    ///
    /// A rule that cannot fire must leave the plan untouched and return
    /// `false`, or the driver never reaches a fixpoint.
    fn apply(&self, plan: &mut Plan) -> bool;
}

/// How many passes over the rule list the driver will make.
///
/// A bound rather than trust: a rule that undoes another's work would
/// otherwise hang the planner. Stopping early is *safe* -- every plan in the
/// chain is a correct plan, just a less refined one -- so the cap is not an
/// error, but it is a bug, and the debug assertion in [`refine`] says so.
pub const MAX_ROUNDS: usize = 64;

/// A rule left the plan violating an invariant.
///
/// Carries the log as well as the defect, because the driver checks the result
/// rather than each step in a release build: the rules that fired are the
/// suspects, and without them a reader has a broken plan and no shortlist.
#[derive(Debug, Clone)]
pub struct RuleFailure {
    pub defect: crate::sparql_refine::PlanDefect,
    pub log: RefineLog,
}

impl std::fmt::Display for RuleFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "after applying {:?}: {}", self.log.applied, self.defect)
    }
}

/// What the driver did, in order.
#[derive(Debug, Clone, Default)]
pub struct RefineLog {
    /// The rules that fired, in the order they fired.
    pub applied: Vec<&'static str>,
    /// Passes over the rule list, including the last one that changed
    /// nothing.
    pub rounds: usize,
    /// Whether the driver stopped because nothing changed rather than because
    /// it ran out of rounds.
    pub reached_fixpoint: bool,
}

/// Apply the rules in order until nothing changes.
///
/// Checked twice over, and the two checks answer different questions:
///
/// * **after every single application, behind a debug assertion.** This is the
///   one that says *which rule*, and it is the reason to check per application
///   at all. An assertion rather than an error because a rule breaking an
///   invariant is a programming mistake with no sensible recovery -- the plan
///   is already edited, and rolling back silently would hide the rule that
///   needs fixing.
/// * **the result, in every build.** A caller must never receive a plan a rule
///   broke. "Nothing executes these plans yet" is true today and is exactly
///   the assumption that stops being true once a renderer reads them, by which
///   point a release-only gap would be invisible. Linear in the plan, so the
///   cost is not a reason to skip it.
pub fn refine(plan: &mut Plan, rules: &[&dyn Rule]) -> Result<RefineLog, RuleFailure> {
    let mut log = RefineLog::default();
    for _ in 0..MAX_ROUNDS {
        log.rounds += 1;
        let mut changed = false;
        for rule in rules {
            if rule.apply(plan) {
                changed = true;
                log.applied.push(rule.name());
                debug_assert!(
                    plan.check().is_ok(),
                    "rule '{}' broke an invariant: {}\n{plan}",
                    rule.name(),
                    plan.check().unwrap_err()
                );
            }
        }
        if !changed {
            log.reached_fixpoint = true;
            break;
        }
    }
    // Not reaching a fixpoint leaves a *correct* plan -- every plan in the
    // chain is one, just less refined -- so it is not an error to return. It
    // is still a bug in the rule set, which is what the assertion says.
    debug_assert!(
        log.reached_fixpoint,
        "rule set did not reach a fixpoint in {MAX_ROUNDS} rounds; \
         the last rules to fire were {:?}",
        log.applied.iter().rev().take(8).collect::<Vec<_>>()
    );
    match plan.check() {
        Ok(()) => Ok(log),
        Err(defect) => Err(RuleFailure { defect, log }),
    }
}

// ---------------------------------------------------------------------------
// Fold matches into a scan
// ---------------------------------------------------------------------------

/// Matches sharing a subject, one of them an `rdf:type`, become a scan; the
/// joins between them disappear.
///
/// The type-scope rule, and the first one for a reason: its result is already
/// known -- it is what the star decomposition produces -- so the machinery is
/// what is being proved rather than the outcome.
///
/// It is also the one rule that can change an answer while every invariant of
/// 28d still holds, which is why the preconditions below are stated rather
/// than left to be rediscovered:
///
/// * **A multivalued slot folds only with its unnest.** A `match` on one fans
///   out -- one solution per value, which is what SPARQL means -- while a scan
///   yields one row per record, so folding without the unnest makes a record
///   with three traffic kinds count once. [`scan_with_fanout`] is the only way
///   this rule builds a scan, and it derives the unnests from the same slot
///   list that decides what the scan reads.
/// * **Only matches joined to the type by plain joins fold.** A match on the
///   preserved side of a `LEFT JOIN` and one inside it are not the same row
///   set: folding the optional one in as an existence check drops exactly the
///   rows the `OPTIONAL` exists to keep. Same for the arms of a `UNION`.
/// * **Only a variable object folds.** A constant object is a filter, and a
///   filter is a claim about values rather than about existence -- the rule
///   that pushes one is a different rule, with a different obligation to move.
/// * **The same multivalued slot read twice does not fold.** Two matches on
///   one array are a cross product in SPARQL -- three values answer nine ways
///   -- and one unnest restores three. Left to the engine.
/// * **Two `rdf:type`s on one subject do not fold.** That is an intersection
///   of classes; a scan of one of them counts every instance of it.
pub struct FoldMatchesIntoScan<'s> {
    schema: &'s SchemaView,
}

impl<'s> FoldMatchesIntoScan<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }

    /// The slots of `class` that the matches on `star` can fold into a scan.
    ///
    /// Returns the match node and the slot it reads, in node order, so the
    /// scan's slot list is stable for a printout.
    fn foldable_slots(
        &self,
        plan: &Plan,
        star: &str,
        class_uri: &str,
        group: &[usize],
        type_group: usize,
    ) -> Vec<(NodeId, ScanSlot)> {
        let Ok(Some(class)) = self.schema.get_class_by_uri(class_uri) else {
            return Vec::new();
        };
        let mut found: Vec<(NodeId, ScanSlot)> = Vec::new();
        for (id, node) in plan.nodes.iter().enumerate() {
            if node.executor != Executor::Engine || group[id] != type_group {
                continue;
            }
            let PlanOp::Match { pattern } = &node.op else {
                continue;
            };
            if subject_variable(pattern) != Some(star) || is_type_pattern(pattern) {
                continue;
            }
            let (Some(predicate), Some(var)) = (predicate_iri(pattern), object_variable(pattern))
            else {
                continue;
            };
            // Asked of the class rather than of the slot's own definition:
            // the same slot name on two classes may differ, and it is this
            // class's column that will be read.
            let Ok(Some(slot)) = self.schema.get_slot_by_uri(predicate) else {
                continue;
            };
            let Some(on_class) = class.slot(&Identifier::Name(slot.name.clone())) else {
                continue;
            };
            found.push((
                id,
                ScanSlot {
                    slot: slot.name.clone(),
                    var: var.to_owned(),
                    multivalued: on_class.determine_slot_container_mode()
                        != SlotContainerMode::SingleValue,
                },
            ));
        }

        // A multivalued slot read twice is a cross product, and one unnest is
        // not one. Both matches stay with the engine.
        let repeated: Vec<String> = found
            .iter()
            .filter(|(_, slot)| slot.multivalued)
            .filter(|(_, slot)| {
                found
                    .iter()
                    .filter(|(_, other)| other.slot == slot.slot)
                    .count()
                    > 1
            })
            .map(|(_, slot)| slot.slot.clone())
            .collect();
        found.retain(|(_, slot)| !repeated.contains(&slot.slot));
        found
    }
}

impl Rule for FoldMatchesIntoScan<'_> {
    fn name(&self) -> &'static str {
        "fold_matches_into_scan"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        let groups = inner_join_groups(plan);
        for (type_node, node) in plan.nodes.iter().enumerate() {
            if node.executor != Executor::Engine {
                continue;
            }
            let PlanOp::Match { pattern } = &node.op else {
                continue;
            };
            let (Some(star), Some(class_uri)) =
                (subject_variable(pattern), type_class_iri(pattern))
            else {
                continue;
            };
            // An intersection of classes, or a type read into a variable
            // alongside a constant one: a scan of one class is not either of
            // those questions.
            let types_on_star = plan
                .nodes
                .iter()
                .filter(|other| match &other.op {
                    PlanOp::Match { pattern } => {
                        is_type_pattern(pattern) && subject_variable(pattern) == Some(star)
                    }
                    _ => false,
                })
                .count();
            if types_on_star != 1 {
                continue;
            }
            if self
                .schema
                .get_class_by_uri(class_uri)
                .ok()
                .flatten()
                .is_none()
            {
                // A class the schema does not know has no columns and no
                // multiplicities, so nothing here can be decided safely.
                continue;
            }
            let star = star.to_owned();
            let class_uri = class_uri.to_owned();
            let slots = self.foldable_slots(plan, &star, &class_uri, &groups, groups[type_node]);
            fold(plan, type_node, &star, &class_uri, slots);
            return true;
        }
        false
    }
}

/// Replace the type match and the folded slot matches with a scan, renumbering
/// everything above them.
///
/// The joins that connected the folded matches disappear: after renumbering
/// both of a join's sides are the same node, and a join of one thing with
/// itself is that thing.
fn fold(
    plan: &mut Plan,
    type_node: NodeId,
    star: &str,
    class_uri: &str,
    slots: Vec<(NodeId, ScanSlot)>,
) {
    let mut folded: Vec<NodeId> = slots.iter().map(|(id, _)| *id).collect();
    folded.push(type_node);
    folded.sort_unstable();
    let first = folded[0];

    let mut claims: Vec<ObligationId> = folded
        .iter()
        .flat_map(|id| plan.nodes[*id].discharges.clone())
        .collect();
    claims.sort_unstable();

    let scan_slots: Vec<ScanSlot> = slots.into_iter().map(|(_, slot)| slot).collect();
    let mut nodes: Vec<Node> = Vec::with_capacity(plan.nodes.len());
    // Old id -> new id. Every node above the fold reads through this, which is
    // the renumbering the well-formedness invariant exists to catch the
    // absence of.
    let mut remap: Vec<Option<NodeId>> = vec![None; plan.nodes.len()];

    for (old, node) in plan.nodes.iter().enumerate() {
        if old == first {
            // At the position of the earliest folded match, because every
            // consumer of a folded match comes after it and the scan has to
            // precede them all.
            let emitted = scan_with_fanout(
                star,
                class_uri,
                scan_slots.clone(),
                nodes.len(),
                claims.clone(),
            );
            nodes.extend(emitted);
            let top = nodes.len() - 1;
            for id in &folded {
                remap[*id] = Some(top);
            }
            continue;
        }
        if folded.contains(&old) {
            continue;
        }
        let mut op = node.op.clone();
        op.map_inputs(|input| remap[input].expect("inputs precede their node"));
        if let PlanOp::Join { left, right, .. } = &op {
            // The join between two folded matches is gone once both sides are
            // the scan, and so is a join whose one side the other already
            // joined in: a natural join with a row set that is already inside
            // it adds no constraint and no column.
            //
            // The second case is not tidiness. Two stars fold in two
            // applications, and the left-deep chain of the naive plan then
            // holds two joins over the same pair -- and re-joining a fanned
            // out row set with itself squares the multiplicity of a repeated
            // array value, which is the very count the unnest exists to get
            // right.
            let redundant = if joined_in(&nodes, *right, *left) {
                Some(*left)
            } else if joined_in(&nodes, *left, *right) {
                Some(*right)
            } else {
                None
            };
            if let Some(target) = redundant {
                remap[old] = Some(target);
                // A naive join claims nothing, so this is defensive rather
                // than load-bearing -- but a claim on a node that disappears
                // has exactly one honest home, and losing it would unbalance
                // the ledger in a release build where the check is only an
                // assertion.
                debug_assert!(
                    node.discharges.is_empty(),
                    "a join that collapses carried claims: {:?}",
                    node.discharges
                );
                nodes[target].discharges.extend(node.discharges.iter());
                nodes[target].discharges.sort_unstable();
                continue;
            }
        }
        nodes.push(Node {
            op,
            executor: node.executor,
            output: node.output,
            discharges: node.discharges.clone(),
        });
        remap[old] = Some(nodes.len() - 1);
    }

    plan.nodes = nodes;
}

/// Whether `lower`'s rows are joined into `upper` by plain joins only.
///
/// Stops at anything else on purpose. Reaching `lower` through a `LEFT JOIN`
/// would say something false: the preserved side keeps rows where `lower`'s
/// variables are unbound, so `upper` joined with `lower` again is *not*
/// `upper`.
fn joined_in(nodes: &[Node], lower: NodeId, upper: NodeId) -> bool {
    if lower == upper {
        return true;
    }
    match &nodes[upper].op {
        PlanOp::Join { left, right, .. } => {
            joined_in(nodes, lower, *left) || joined_in(nodes, lower, *right)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparql_refine::naive_plan_of;
    use crate::sparql_scoper::tests::test_schema_view;

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

    fn plan_of(query: &str) -> Plan {
        naive_plan_of(&format!("{PREFIX}{query}")).expect("should build a naive plan")
    }

    /// Every slot the scan folded, as (slot, variable, multivalued).
    fn scan_slots(plan: &Plan) -> Vec<(String, String, bool)> {
        plan.nodes
            .iter()
            .filter_map(|node| match &node.op {
                PlanOp::Scan { slots, .. } => Some(slots.clone()),
                _ => None,
            })
            .flatten()
            .map(|slot| (slot.slot, slot.var, slot.multivalued))
            .collect()
    }

    /// What the rule is for: three matches and two joins become one scan, and
    /// the scan runs in SQL.
    #[test]
    fn matches_sharing_a_type_become_a_scan() {
        let schema = test_schema_view();
        let rule = FoldMatchesIntoScan::new(&schema);
        let mut plan = plan_of(
            "SELECT ?nm ?len WHERE { ?s a asset360:Signal ; asset360:name ?nm ; \
             asset360:length ?len }",
        );
        assert_eq!(plan.find("match").len(), 3, "{plan}");
        assert_eq!(plan.find("join").len(), 2, "{plan}");

        let log = refine(&mut plan, &[&rule]).expect("the fold preserves every invariant");
        assert_eq!(log.applied, vec!["fold_matches_into_scan"], "{plan}");
        assert!(log.reached_fixpoint, "{plan}");

        assert_eq!(plan.find("scan").len(), 1, "{plan}");
        assert!(plan.find("match").is_empty(), "{plan}");
        assert!(
            plan.find("join").is_empty(),
            "the joins between folded matches disappear:\n{plan}"
        );
        assert_eq!(
            scan_slots(&plan),
            vec![
                ("name".to_owned(), "nm".to_owned(), false),
                ("length".to_owned(), "len".to_owned(), false),
            ],
            "{plan}"
        );
        assert!(
            plan.nodes[plan.find("scan")[0]].executor == Executor::Sql,
            "a scan is the pushed-down form:\n{plan}"
        );
        // ...and the projection above it stays with the engine, which is what
        // makes the frontier a cut rather than a verdict.
        assert_eq!(
            plan.nodes.last().unwrap().executor,
            Executor::Engine,
            "{plan}"
        );
        println!("{plan}");
    }

    /// The rule moves claims, it does not create or drop them. Stated as the
    /// obligations being untouched rather than as the ledger balancing,
    /// because a rule could balance a ledger it had rewritten.
    #[test]
    fn folding_leaves_the_obligations_unchanged() {
        let schema = test_schema_view();
        let rule = FoldMatchesIntoScan::new(&schema);
        for query in [
            "SELECT ?s WHERE { ?s a asset360:Signal }",
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm }",
            "SELECT ?kind (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:trafficKinds ?kind } GROUP BY ?kind ORDER BY DESC(?n) LIMIT 10",
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             FILTER(REGEX(?nm, \"^A\")) }",
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t a asset360:Track ; asset360:hasName ?tn }",
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal . \
             OPTIONAL { ?s asset360:name ?nm } }",
            "ASK WHERE { ?s a asset360:Signal ; asset360:name ?nm }",
        ] {
            let naive = plan_of(query);
            let mut refined = naive.clone();
            refine(&mut refined, &[&rule])
                .unwrap_or_else(|failure| panic!("{failure} for {query}"));

            assert_eq!(
                refined.obligations, naive.obligations,
                "a rule edits nodes, never the question: {query}"
            );
            assert_eq!(refined.residual, naive.residual, "{query}");
            let claims = |plan: &Plan| {
                let mut ids: Vec<ObligationId> = plan
                    .nodes
                    .iter()
                    .flat_map(|node| node.discharges.iter().copied())
                    .collect();
                ids.sort_unstable();
                ids
            };
            assert_eq!(
                claims(&refined),
                claims(&naive),
                "the same obligations, claimed once each: {query}\n{refined}"
            );
            refined
                .check()
                .unwrap_or_else(|defect| panic!("{defect} for {query}\n{refined}"));
        }
    }

    /// The correctness trap. A `match` on a multivalued slot fans out -- one
    /// solution per value -- and a scan yields one row per record, so the fold
    /// has to bring the unnest with it or a record with three traffic kinds
    /// counts once instead of three times.
    ///
    /// The expectation comes from the schema and not from the rule: the test
    /// asks which of the folded slots hold arrays, and requires exactly that
    /// many fan-out nodes.
    #[test]
    fn a_multivalued_slot_folds_with_its_unnest() {
        let schema = test_schema_view();
        let rule = FoldMatchesIntoScan::new(&schema);
        let mut plan = plan_of(
            "SELECT ?kind (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm ; asset360:trafficKinds ?kind } GROUP BY ?kind",
        );
        refine(&mut plan, &[&rule]).expect("the fold preserves every invariant");

        // Which folded slots fan out, asked of the schema directly.
        let class = schema
            .get_class_by_uri("https://data.infrabel.be/asset360/Signal")
            .unwrap()
            .unwrap();
        let fanning: Vec<String> = scan_slots(&plan)
            .into_iter()
            .filter(|(slot, _, _)| {
                class
                    .slot(&Identifier::Name(slot.clone()))
                    .is_some_and(|slot| {
                        slot.determine_slot_container_mode() != SlotContainerMode::SingleValue
                    })
            })
            .map(|(slot, _, _)| slot)
            .collect();
        assert_eq!(fanning, vec!["trafficKinds".to_owned()], "{plan}");

        let unnests: Vec<Vec<String>> = plan
            .nodes
            .iter()
            .filter_map(|node| match &node.op {
                PlanOp::Unnest { slot_path, .. } => Some(slot_path.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            unnests,
            vec![vec!["trafficKinds".to_owned()]],
            "one unnest per multivalued folded slot, no more and no fewer:\n{plan}"
        );

        // And it is above the scan, not beside it: the fan-out has to happen
        // before anything counts the rows.
        let scan = plan.find("scan")[0];
        let unnest = plan.find("unnest")[0];
        assert!(plan.feeds(scan, unnest), "{plan}");
        assert!(
            scan_slots(&plan)
                .iter()
                .any(|(_, _, multivalued)| *multivalued),
            "the scan carries the multiplicity that made the unnest necessary:\n{plan}"
        );

        // The single-valued slot in the same fold gets no unnest: fanning out
        // a scalar is noise, and the count of unnests is what the invariant
        // reads.
        assert_eq!(plan.find("unnest").len(), 1, "{plan}");
        println!("{plan}");
    }

    /// The same array read twice is a cross product in SPARQL, and one unnest
    /// restores three rows where the query asks for nine. Neither match folds.
    #[test]
    fn one_array_read_twice_does_not_fold() {
        let schema = test_schema_view();
        let rule = FoldMatchesIntoScan::new(&schema);
        let mut plan = plan_of(
            "SELECT ?a ?b WHERE { ?s a asset360:Signal ; asset360:trafficKinds ?a ; \
             asset360:trafficKinds ?b }",
        );
        refine(&mut plan, &[&rule]).expect("the fold preserves every invariant");

        assert_eq!(scan_slots(&plan), Vec::new(), "{plan}");
        assert_eq!(
            plan.find("match").len(),
            2,
            "both reads stay with the engine:\n{plan}"
        );
        assert!(plan.find("unnest").is_empty(), "{plan}");
    }

    /// A match inside an `OPTIONAL` is not the same row set as the type it
    /// shares a subject with. Folding it in as an existence check would drop
    /// exactly the rows the `OPTIONAL` exists to keep.
    #[test]
    fn an_optional_match_is_not_folded() {
        let schema = test_schema_view();
        let rule = FoldMatchesIntoScan::new(&schema);
        let mut plan = plan_of(
            "SELECT ?s ?nm ?k WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { ?s asset360:name ?nm } }",
        );
        refine(&mut plan, &[&rule]).expect("the fold preserves every invariant");

        assert_eq!(
            scan_slots(&plan),
            vec![("kind".to_owned(), "k".to_owned(), false)],
            "the mandatory slot folds, the optional one does not:\n{plan}"
        );
        assert_eq!(plan.find("match").len(), 1, "{plan}");
        assert_eq!(plan.find("leftjoin").len(), 1, "{plan}");
        // The left join reads the scan, which is legal: an engine node over an
        // SQL one is what a frontier is.
        plan.frontier_is_a_cut().unwrap();
        println!("{plan}");
    }

    /// Two stars fold in two applications, one each, and the join between them
    /// survives -- it is a join between row sets, not between matches of one
    /// star.
    #[test]
    fn two_stars_fold_one_at_a_time() {
        let schema = test_schema_view();
        let rule = FoldMatchesIntoScan::new(&schema);
        let mut plan = plan_of(
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t a asset360:Track ; asset360:hasName ?tn }",
        );
        let log = refine(&mut plan, &[&rule]).expect("the fold preserves every invariant");

        assert_eq!(log.applied.len(), 2, "one application per star:\n{plan}");
        assert_eq!(plan.find("scan").len(), 2, "{plan}");
        assert_eq!(
            plan.find("join").len(),
            1,
            "one join between the two stars survives, and only one:\n{plan}"
        );
        assert!(plan.find("match").is_empty(), "{plan}");
        println!("{plan}");
    }

    /// An intersection of classes is not a scan of one of them.
    #[test]
    fn two_types_on_one_subject_do_not_fold() {
        let schema = test_schema_view();
        let rule = FoldMatchesIntoScan::new(&schema);
        let mut plan = plan_of("SELECT ?s WHERE { ?s a asset360:Signal ; a asset360:Track }");
        let log = refine(&mut plan, &[&rule]).expect("the fold preserves every invariant");

        assert!(log.applied.is_empty(), "{plan}");
        assert_eq!(plan.find("match").len(), 2, "{plan}");
    }

    /// A class the schema does not know has no columns and no multiplicities,
    /// so there is nothing to decide safely and the rule declines.
    #[test]
    fn an_unknown_class_does_not_fold() {
        let schema = test_schema_view();
        let rule = FoldMatchesIntoScan::new(&schema);
        let mut plan = plan_of("SELECT ?s WHERE { ?s a asset360:NoSuchClass ; asset360:name ?nm }");
        let log = refine(&mut plan, &[&rule]).expect("the fold preserves every invariant");

        assert!(log.applied.is_empty(), "{plan}");
        assert!(plan.find("scan").is_empty(), "{plan}");
    }

    /// A constant object is a filter, not an existence check, so it is not
    /// this rule's to fold -- the obligation it would have to move is a claim
    /// about values.
    #[test]
    fn a_constant_object_stays_a_match() {
        let schema = test_schema_view();
        let rule = FoldMatchesIntoScan::new(&schema);
        let mut plan = plan_of(
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name \"BX517\" ; \
             asset360:kind ?k }",
        );
        refine(&mut plan, &[&rule]).expect("the fold preserves every invariant");

        assert_eq!(
            scan_slots(&plan),
            vec![("kind".to_owned(), "k".to_owned(), false)],
            "{plan}"
        );
        assert_eq!(plan.find("match").len(), 1, "{plan}");
    }

    /// The invariants after *every* application, explicitly and not only
    /// behind the driver's debug assertion -- the point of checking per
    /// application is that a bad rule fails at the rule rather than in a
    /// result.
    #[test]
    fn every_invariant_holds_after_every_application() {
        let schema = test_schema_view();
        let rule = FoldMatchesIntoScan::new(&schema);
        for query in [
            "SELECT ?s WHERE { ?s a asset360:Signal }",
            "SELECT ?kind (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm ; asset360:trafficKinds ?kind . FILTER(?nm > \"A\") } \
             GROUP BY ?kind ORDER BY DESC(?n) LIMIT 10",
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t a asset360:Track ; asset360:hasName ?tn }",
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal . \
             OPTIONAL { ?s asset360:name ?nm } }",
            "CONSTRUCT { ?s asset360:name ?nm } WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm }",
            "ASK WHERE { ?s a asset360:Signal ; asset360:trafficKinds ?k }",
            "DESCRIBE ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm }",
            "SELECT ?s WHERE { { SELECT ?s WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm } LIMIT 3 } }",
        ] {
            let mut plan = plan_of(query);
            plan.check()
                .unwrap_or_else(|defect| panic!("naive: {defect} for {query}\n{plan}"));
            let mut applications = 0;
            while rule.apply(&mut plan) {
                applications += 1;
                plan.check().unwrap_or_else(|defect| {
                    panic!("after application {applications}: {defect} for {query}\n{plan}")
                });
                assert!(applications < 16, "no fixpoint for {query}\n{plan}");
            }
        }
    }

    /// The driver stops when nothing changes, and a second run is a no-op.
    /// Without that a rule chain has no defined result to test against.
    #[test]
    fn the_driver_reaches_a_fixpoint() {
        let schema = test_schema_view();
        let rule = FoldMatchesIntoScan::new(&schema);
        let mut plan = plan_of(
            "SELECT ?nm ?tn WHERE { ?s a asset360:Signal ; asset360:name ?nm ; \
             asset360:locatedOnTrack ?t . ?t a asset360:Track ; asset360:hasName ?tn }",
        );
        let first = refine(&mut plan, &[&rule]).unwrap();
        assert!(first.reached_fixpoint);
        assert!(!first.applied.is_empty());
        let before = plan.to_string();

        let second = refine(&mut plan, &[&rule]).unwrap();
        assert!(second.applied.is_empty(), "{plan}");
        assert_eq!(second.rounds, 1, "one round to see there is nothing to do");
        assert_eq!(before, plan.to_string(), "a fixpoint is a fixpoint");
    }

    /// The driver refuses to hand back a plan a rule broke, in every build and
    /// not only under a debug assertion.
    ///
    /// The rule here edits the plan and reports no change, which is the one
    /// shape the per-application check cannot see -- it only looks after a
    /// rule that said it did something -- and is a plausible bug: a rule that
    /// returns `false` on a path where it already moved a node would otherwise
    /// hand back a plan whose ledger is short one claim.
    #[test]
    fn a_rule_that_breaks_a_plan_is_not_handed_back() {
        struct DropsAClaimQuietly;
        impl Rule for DropsAClaimQuietly {
            fn name(&self) -> &'static str {
                "drops_a_claim_quietly"
            }
            fn apply(&self, plan: &mut Plan) -> bool {
                plan.nodes[0].discharges.clear();
                false
            }
        }

        let mut plan = plan_of("SELECT ?s WHERE { ?s a asset360:Signal }");
        let failure = refine(&mut plan, &[&DropsAClaimQuietly])
            .expect_err("an unbalanced ledger must not be returned as a plan");
        assert!(
            matches!(failure.defect, crate::sparql_refine::PlanDefect::Ledger(_)),
            "{failure}"
        );
    }

    /// Rules apply in the order they are given, and an empty rule set is a
    /// fixpoint at once -- which is what makes "rules on versus rules off" a
    /// comparison of the same plan builder rather than of two planners.
    #[test]
    fn no_rules_is_a_fixpoint() {
        let mut plan = plan_of("SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm }");
        let before = plan.to_string();
        let log = refine(&mut plan, &[]).unwrap();
        assert!(log.reached_fixpoint);
        assert!(log.applied.is_empty());
        assert_eq!(before, plan.to_string());
    }
}
