//! Rules, a fixpoint driver, and tier one: fold matches into a scan, fold a
//! nested read into a path, turn a constant object into a filter, turn a
//! `VALUES` over a bound variable into one, push a comparison filter, push a
//! reference join.
//!
//! Together they are at capability parity with the planner that serves
//! queries today -- `the_refined_plan_claims_everything_todays_sql_pass_claims`
//! holds them to it, over both what a plan *claims* and what conditions it
//! pushes, with the two remaining gaps named in `KNOWN_GAPS` and beside
//! [`Visible`]. That parity is the gate on executing these plans: switching
//! onto a planner that pushed less would regress answers or lose pushdown.
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
//!
//! That argument has one hole, and the tier-one rules are stricter than 28d
//! because of it: a condition SQL applies is a narrowing only if it selects a
//! *superset* of the answer, and a constant compared against a column whose
//! values never spell it that way selects nothing at all. The engine then
//! re-runs the query over no instances and reports no answer. See
//! [`Expr::to_sql`], the only way to obtain a condition, which asks the same
//! question of a constant that the star decomposition does -- and which is why
//! it takes a schema.
//!
//! One requirement runs through the rules and is not in 28d: **the order a
//! query wrote its filters in must not decide the plan.** Obligations are per
//! top-level conjunct and the naive builder chains one `Filter` node per
//! conjunct, so a pushable conjunct can end up above an unpushable one, where
//! the frontier-is-a-cut invariant forbids pushing it. Filters commute, so
//! [`PushComparisonFilter`] sinks the pushable one below -- see
//! [`landing_site`] for the argument and the cases that stop it.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::slotview::{SlotContainerMode, SlotInlineMode};

use spargebra::algebra::AggregateExpression;
use spargebra::term::{GroundTerm, Term, TermPattern, TriplePattern};

use crate::sparql_plan::ObligationId;
use crate::sparql_refine::{
    CompareOp, Executor, Expr, Node, NodeId, Plan, PlanOp, ReferenceEdge, ScanSlot, SlotPresence,
    SlotReading, inner_join_groups, is_type_pattern, object_variable, predicate_iri,
    scan_with_fanout, subject_variable, type_class_iri,
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
                    path: vec![slot.name.clone()],
                    var: Some(var.to_owned()),
                    multivalued: on_class.determine_slot_container_mode()
                        != SlotContainerMode::SingleValue,
                    // A read the query wrote outside an `OPTIONAL` requires
                    // the value: a record without it is not an answer.
                    presence: SlotPresence::Required,
                },
            ));
        }

        // A multivalued slot read twice is a cross product, and one unnest is
        // not one. Both matches stay with the engine.
        let repeated: Vec<Vec<String>> = found
            .iter()
            .filter(|(_, slot)| slot.multivalued)
            .filter(|(_, slot)| {
                found
                    .iter()
                    .filter(|(_, other)| other.path == slot.path)
                    .count()
                    > 1
            })
            .map(|(_, slot)| slot.path.clone())
            .collect();
        found.retain(|(_, slot)| !repeated.contains(&slot.path));
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
    refresh_join_variables(plan);
}

/// Recompute every plain join's `on` from the variables its sides now bind.
///
/// A naive `on` is the variables the two *matches* shared, and a rule can make
/// that stale in both directions. Folding a star's matches into a scan moves
/// their bindings onto the scan, and collapsing a join that has become
/// redundant removes the node that recorded the shared variable -- so
/// `?c a :TunnelComplex . ?comp a :CivilEngineeringAsset ;
/// :belongsToTunnelComplex ?c` ends up with the surviving join carrying the
/// *cross product's* empty `on` while its sides do share `?c`.
///
/// That was a live defect and not a cosmetic one: the reference-join rule and
/// [`Plan::reference_joins_agree`] both read `on`, so the same query pushed or
/// declined depending on which order its two stars were written in -- the same
/// class of bug as the filter ordering, found by the runtime gate's own log
/// rather than by a test.
///
/// Called by every rule that removes or rewires a node, so `on` is a fact
/// about the plan as it is rather than as it was parsed.
pub fn refresh_join_variables(plan: &mut Plan) {
    for id in 0..plan.nodes.len() {
        let PlanOp::Join { left, right, .. } = &plan.nodes[id].op else {
            continue;
        };
        let shared: Vec<String> = plan
            .variables_of(*left)
            .intersection(&plan.variables_of(*right))
            .cloned()
            .collect();
        if let PlanOp::Join { on, .. } = &mut plan.nodes[id].op {
            *on = shared;
        }
    }
}

/// Whether every solution `upper` produces takes its bindings from a row of
/// `lower`.
///
/// The mandatory-row-set question [`inner_join_groups`] answers for a naive
/// plan, asked of a plan a rule has already edited: a scan's rows reach a node
/// through plain joins, filters and unnests without any of them being able to
/// produce a solution the scan did not contribute to.
///
/// Everything else stops the walk, and the left join is the case that matters.
/// Joining a solution that leaves `?s` unbound against a pattern that binds it
/// keeps the row -- the two are compatible -- so a constraint moved onto the
/// preserved side of a left join is not the constraint the join applied.
fn mandatorily_feeds(plan: &Plan, lower: NodeId, upper: NodeId) -> bool {
    if lower == upper {
        return true;
    }
    match &plan.nodes[upper].op {
        PlanOp::Join { left, right, .. } => {
            mandatorily_feeds(plan, lower, *left) || mandatorily_feeds(plan, lower, *right)
        }
        PlanOp::Filter { input, .. } | PlanOp::Unnest { input, .. } => {
            mandatorily_feeds(plan, lower, *input)
        }
        _ => false,
    }
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

// ---------------------------------------------------------------------------
// What an `Sql` scan makes visible
// ---------------------------------------------------------------------------

/// One variable an `Sql` [`PlanOp::Scan`] binds to a slot of its record, and
/// which value at that address the variable stands for.
struct SlotBinding {
    star_var: String,
    path: Vec<String>,
    /// [`SlotReading::Column`] for a single-valued slot, and
    /// [`SlotReading::BoundElement`] for a multivalued one whose
    /// [`PlanOp::Unnest`] is below the node being asked -- the variable then
    /// stands for one element, which is what SPARQL bound it to.
    reading: SlotReading,
}

/// The variables the `Sql` scans feeding a node have bound, and what to.
///
/// Built per landing site rather than once per plan: a variable is a column
/// only where the scan that binds it is actually below, and two stars can bind
/// one variable (`?a :name ?nm . ?b :hasName ?nm` is a value join) in which
/// case naming either column answers a different question. An ambiguous
/// variable is recorded as `None` rather than dropped, so a rule declines it
/// instead of silently resolving the first match.
struct Visible {
    slots: HashMap<String, Option<SlotBinding>>,
    class_of_star: HashMap<String, String>,
}

impl Visible {
    /// Everything the `Sql` scans below `base` bind.
    ///
    /// `feeds` and not [`mandatorily_feeds`]: a landing site runs in SQL, the
    /// frontier is a cut, so its whole subtree is `Sql` -- and no rule pushes
    /// a left join, a union or a minus. Every path from a scan to an `Sql`
    /// node is therefore already mandatory.
    fn below(plan: &Plan, base: NodeId) -> Self {
        Self::collect(plan, base, false)
    }

    /// The same, of a node that is *not* known to run in SQL.
    ///
    /// A rule that rewrites a join into a filter needs the stronger question:
    /// the variable has to be bound in every row of the side that stays, and a
    /// scan reached through the optional side of a left join is not that.
    fn mandatorily_below(plan: &Plan, base: NodeId) -> Self {
        Self::collect(plan, base, true)
    }

    fn collect(plan: &Plan, base: NodeId, mandatory: bool) -> Self {
        let mut slots: HashMap<String, Option<SlotBinding>> = HashMap::new();
        let mut class_of_star: HashMap<String, String> = HashMap::new();
        for (id, node) in plan.nodes.iter().enumerate() {
            let PlanOp::Scan {
                star_var,
                class_uri,
                slots: scan_slots,
            } = &node.op
            else {
                continue;
            };
            let reaches = if mandatory {
                mandatorily_feeds(plan, id, base)
            } else {
                plan.feeds(id, base)
            };
            if node.executor != Executor::Sql || !reaches {
                continue;
            }
            class_of_star.insert(star_var.clone(), class_uri.clone());
            // The star variable names the *record*, not a slot of it. Its
            // pushdown is the indexed identifier column rather than a JSONB
            // path, which no `SqlCondition` can say, so it is entered as
            // ambiguous: a rule then declines `FILTER(?s = <iri>)` rather than
            // resolving `?s` through some other star's slot of the same name.
            //
            // That is a capability gap against today's planner, which pushes
            // such a constraint as `Star::identifier_values`. Closing it needs
            // an *address* for a record's identity, which no `Expr::Slot` has
            // -- a representation change rather than a rule. A constraint
            // written on the identifier slot itself
            // (`?s :asset360_uri "u"`) does push, through the slot.
            slots.insert(star_var.clone(), None);
            // A slot that binds no variable is invisible here without a
            // special case: there is no name to resolve. That covers the read
            // an `OPTIONAL` delivers for the engine's benefit, which no SQL
            // node above may read -- and it stops covering it the moment a
            // rule *absorbs* that read and gives it a variable, which is
            // exactly when reading it becomes correct.
            for slot in scan_slots {
                // A multivalued slot's variable is one *element*, and it is
                // the unnest below that bound it. Without the unnest under
                // this node the element has no name here -- the fan-out has
                // not happened yet -- so the variable resolves to nothing
                // rather than to the array, which would select records where
                // the query selects rows.
                // A read that binds no variable is not visible to anything
                // above: there is no name to resolve.
                let Some(bound) = slot.var.clone() else {
                    continue;
                };
                let reading = if slot.multivalued {
                    if unnest_below(plan, base, star_var, &slot.path, &bound) {
                        SlotReading::BoundElement
                    } else {
                        slots.insert(bound, None);
                        continue;
                    }
                } else {
                    SlotReading::Column
                };
                let binding = SlotBinding {
                    star_var: star_var.clone(),
                    path: slot.path.clone(),
                    reading,
                };
                match slots.entry(bound) {
                    Entry::Vacant(entry) => {
                        entry.insert(Some(binding));
                    }
                    Entry::Occupied(mut entry) => {
                        entry.insert(None);
                    }
                }
            }
        }
        Self {
            slots,
            class_of_star,
        }
    }

    /// The slot a variable reads, when exactly one does.
    fn slot_of(&self, var: &str) -> Option<&SlotBinding> {
        match self.slots.get(var) {
            Some(Some(binding)) => Some(binding),
            _ => None,
        }
    }
}

/// Whether the unnest that bound `var` to an element of `star.slot` is at or
/// below `node`.
fn unnest_below(plan: &Plan, node: NodeId, star: &str, path: &[String], var: &str) -> bool {
    plan.nodes.iter().enumerate().any(|(id, above)| {
        matches!(
            &above.op,
            PlanOp::Unnest { star_var, slot_path, var: bound, .. }
                if star_var == star && slot_path.as_slice() == path && bound == var
        ) && plan.feeds(id, node)
    })
}

/// Every node that reads `id`.
///
/// A plan is a tree as the naive builder writes it, but a rule that collapses
/// a join makes two nodes read one -- [`fold`] does exactly that -- so
/// "the node above this one" is a question with several answers and a rule that
/// assumes one has to check.
fn consumers(plan: &Plan, id: NodeId) -> Vec<NodeId> {
    plan.nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| node.op.inputs().contains(&id))
        .map(|(consumer, _)| consumer)
        .collect()
}

// ---------------------------------------------------------------------------
// Deliver an optional read
// ---------------------------------------------------------------------------

/// A read inside an `OPTIONAL` on a star an `Sql` scan already scans becomes a
/// *delivered* slot of that scan. The `match` stays where it is.
///
/// The one rule that adds to a scan without taking anything away, and the
/// reason is the whole point of [`SlotPresence`]. The SQL leg has to hand the
/// column over -- the engine cannot bind `?nm` from a record it was not given
/// -- while nothing about the optionality is decided in SQL: no existence
/// check, no condition, no claim. Today's star decomposition says exactly this
/// with `optional_fields`, and lowering a scan without the slot would emit a
/// statement that agrees only by accident (the prefetch selects whole
/// records).
///
/// It claims nothing, which is the correction this rule exists to make
/// precise. A delivered read enforces no triple; the `match` above keeps that
/// obligation and the engine's left join decides it. When a later stage
/// renders the optional semantics in SQL, the claim becomes honest and belongs
/// to whichever node renders it -- not to this scan.
///
/// Preconditions, each with the wrong answer it prevents:
///
/// * **The match is on the optional side of a `LeftJoin` whose preserved side
///   takes every row from the scan.** Anything else is either the fold rule's
///   job (a mandatory read) or a row set this scan does not feed.
/// * **The scan does not read the slot already.** Otherwise the rule would add
///   a slot per round and never reach a fixpoint. A slot the query reads both
///   mandatorily and optionally stays *required*, which is the stronger of the
///   two and what the mandatory read already established.
pub struct DeliverOptionalRead<'s> {
    schema: &'s SchemaView,
}

impl<'s> DeliverOptionalRead<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }
}

impl Rule for DeliverOptionalRead<'_> {
    fn name(&self) -> &'static str {
        "deliver_optional_read"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for id in 0..plan.nodes.len() {
            if plan.nodes[id].executor != Executor::Engine {
                continue;
            }
            let PlanOp::Match { pattern } = &plan.nodes[id].op else {
                continue;
            };
            if is_type_pattern(pattern) {
                continue;
            }
            let (Some(star), Some(predicate), Some(var)) = (
                subject_variable(pattern),
                predicate_iri(pattern),
                object_variable(pattern),
            ) else {
                continue;
            };
            let (star, predicate, var) = (star.to_owned(), predicate.to_owned(), var.to_owned());

            // The optional shape: this match is inside the optional side of a
            // left join whose preserved side is fed by the scan.
            let Some((scan, class_uri)) = plan.nodes.iter().enumerate().find_map(|(scan, node)| {
                let PlanOp::Scan {
                    star_var,
                    class_uri,
                    slots,
                } = &node.op
                else {
                    return None;
                };
                if node.executor != Executor::Sql || star_var != &star {
                    return None;
                }
                if slots
                    .iter()
                    .any(|slot| slot.var.as_deref() == Some(var.as_str()))
                {
                    return None;
                }
                let optional = plan.nodes.iter().any(|above| {
                    matches!(&above.op, PlanOp::LeftJoin { left, right, .. }
                        if plan.feeds(id, *right) && mandatorily_feeds(plan, scan, *left))
                });
                optional.then(|| (scan, class_uri.clone()))
            }) else {
                continue;
            };

            let Some(slot) = self.schema.get_slot_by_uri(&predicate).ok().flatten() else {
                continue;
            };
            let Some(class) = self.schema.get_class_by_uri(&class_uri).ok().flatten() else {
                continue;
            };
            let Some(on_class) = class.slot(&Identifier::Name(slot.name.clone())) else {
                continue;
            };
            let delivered = ScanSlot {
                path: vec![slot.name.clone()],
                // Bound by nothing here. The `match` above is what binds the
                // variable, and the engine's left join decides it; this slot
                // exists so the column reaches the engine at all. Binding it
                // would make it readable by SQL nodes that must not read it --
                // and, if it were multivalued, would owe a fan-out inside the
                // optional side that no rule builds.
                var: None,
                multivalued: on_class.determine_slot_container_mode()
                    != SlotContainerMode::SingleValue,
                presence: SlotPresence::Optional,
            };
            if let PlanOp::Scan { slots, .. } = &mut plan.nodes[scan].op {
                if slots.contains(&delivered) {
                    continue;
                }
                slots.push(delivered);
            }
            return true;
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Absorb an optional read
// ---------------------------------------------------------------------------

/// `OPTIONAL { ?s :name ?nm }` over a star the scan below already reads
/// becomes a *bound* nullable column, and the left join disappears.
///
/// The rule that makes a missing-value bucket possible. [`DeliverOptionalRead`]
/// puts the column on the scan so the engine can bind it; this one gives it
/// the variable, so the SQL side can. The difference is the whole of what an
/// `OPTIONAL` over one star means: a record with the value answers with it, a
/// record without answers with the variable unbound — which is a `NULL`
/// column, and a group of its own.
///
/// **It is also where the claim becomes honest.** A column delivered and bound
/// by nothing does not produce the solution an `OPTIONAL` asks for; the engine
/// does. A nullable column the SQL side binds produces exactly those
/// solutions, so the scan takes the triple's obligation with it — which is the
/// conditional claim [`SlotPresence`] describes, arriving at the condition
/// that makes it true.
///
/// Preconditions, each with the wrong answer it prevents:
///
/// * **No lifted condition on the left join.** `OPTIONAL { ?s :name ?nm .
///   FILTER(?nm > "A") }` decides whether the *value* binds, not whether the
///   row survives: a record whose name is `"A"` stays, with `?nm` unbound.
///   Rendering that needs a conditional binding expression -- a `CASE` around
///   the column -- which this does not build. As a `WHERE` it would delete the
///   row, which is the single most common way a left-join translation is
///   wrong, and it fails quietly with a smaller answer.
/// * **The optional side is exactly one `match` on the same star.** Anything
///   else is a second row set, which is a real left join and a different rule.
/// * **A single-valued slot.** A multivalued optional read fans out *inside*
///   the optional side; the fan-out would have to be part of the join rather
///   than above the scan, and the fifth invariant refuses the shape this rule
///   would otherwise build.
/// * **The scan runs in SQL and takes every row of the preserved side.**
pub struct AbsorbOptionalRead<'s> {
    schema: &'s SchemaView,
}

impl<'s> AbsorbOptionalRead<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }
}

impl Rule for AbsorbOptionalRead<'_> {
    fn name(&self) -> &'static str {
        "absorb_optional_read"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for id in 0..plan.nodes.len() {
            let PlanOp::LeftJoin {
                left,
                right,
                condition,
            } = &plan.nodes[id].op
            else {
                continue;
            };
            if condition.is_some() {
                continue;
            }
            let (left, right) = (*left, *right);

            // The optional side: one match, reading one slot of one star.
            let PlanOp::Match { pattern } = &plan.nodes[right].op else {
                continue;
            };
            if plan.nodes[right].executor != Executor::Engine || is_type_pattern(pattern) {
                continue;
            }
            let (Some(star), Some(predicate), Some(var)) = (
                subject_variable(pattern),
                predicate_iri(pattern),
                object_variable(pattern),
            ) else {
                continue;
            };
            let (star, predicate, var) = (star.to_owned(), predicate.to_owned(), var.to_owned());
            let Some(slot) = self.schema.get_slot_by_uri(&predicate).ok().flatten() else {
                continue;
            };
            let path = vec![slot.name.clone()];

            // The preserved side: a scan of that same star, in SQL, whose rows
            // all reach the join.
            let Some(scan) = plan.nodes.iter().enumerate().position(|(scan, node)| {
                matches!(&node.op, PlanOp::Scan { star_var, .. } if star_var == &star)
                    && node.executor == Executor::Sql
                    && mandatorily_feeds(plan, scan, left)
            }) else {
                continue;
            };
            let PlanOp::Scan { slots, .. } = &plan.nodes[scan].op else {
                continue;
            };
            let Some(delivered) = slots.iter().position(|slot| slot.path == path) else {
                continue;
            };
            if slots[delivered].multivalued || slots[delivered].var.is_some() {
                continue;
            }

            // Bind it. From here the column is the read: a value where there
            // is one, `NULL` where there is not.
            if let PlanOp::Scan { slots, .. } = &mut plan.nodes[scan].op {
                slots[delivered].var = Some(var);
                slots[delivered].presence = SlotPresence::Optional;
            }
            let mut claims = plan.nodes[right].discharges.clone();
            claims.extend(plan.nodes[id].discharges.iter().copied());
            plan.nodes[scan].discharges.extend(claims);
            plan.nodes[scan].discharges.sort_unstable();

            drop_optional_pair(plan, id, right, left);
            return true;
        }
        false
    }
}

/// Remove the left join and the match it made optional, leaving the preserved
/// side in their place.
fn drop_optional_pair(plan: &mut Plan, leftjoin: NodeId, matched: NodeId, left: NodeId) {
    let mut nodes: Vec<Node> = Vec::with_capacity(plan.nodes.len() - 2);
    let mut remap: Vec<Option<NodeId>> = vec![None; plan.nodes.len()];
    for (old, node) in plan.nodes.iter().enumerate() {
        if old == matched {
            continue;
        }
        if old == leftjoin {
            // Everything that read the left join reads the preserved side:
            // the optional value is a column of it now.
            remap[old] = remap[left];
            continue;
        }
        nodes.push(node.clone());
        remap[old] = Some(nodes.len() - 1);
    }
    for node in &mut nodes {
        node.op
            .map_inputs(|input| remap[input].expect("inputs precede their node"));
    }
    plan.nodes = nodes;
    refresh_join_variables(plan);
}

// ---------------------------------------------------------------------------
// Where a match's subject lives
// ---------------------------------------------------------------------------

/// The record, and the place inside it, that a match's subject variable names.
///
/// Two shapes, and the second is what makes a nested path pushable: the
/// subject is either a star an `Sql` scan scanned, or a structure *inside* one
/// of its records that a scan slot bound. `?s :location ?loc` binds `?loc` to
/// the inlined `location` value, so `?loc :longitude 5` constrains
/// `["location", "longitude"]` of the same record -- a `PathFilter` in the
/// star decomposition's vocabulary.
struct SubjectSite {
    scan: NodeId,
    star_var: String,
    /// The class of the *record*, which is what a slot path is resolved
    /// against.
    class_uri: String,
    /// The path from the record's root to the structure the subject names.
    /// Empty when the subject is the star itself.
    prefix: Vec<String>,
}

/// Where a subject variable's rows come from, when they mandatorily come from
/// one `Sql` scan feeding `other`.
///
/// [`mandatorily_feeds`] rather than `feeds` for the reason the constant rule
/// gives: a scan reached through the optional side of a left join is not a row
/// set a constraint can be moved onto.
fn subject_site(plan: &Plan, other: NodeId, subject: &str) -> Option<SubjectSite> {
    plan.nodes.iter().enumerate().find_map(|(scan, node)| {
        let PlanOp::Scan {
            star_var,
            class_uri,
            slots,
        } = &node.op
        else {
            return None;
        };
        if node.executor != Executor::Sql || !mandatorily_feeds(plan, scan, other) {
            return None;
        }
        if star_var == subject {
            return Some(SubjectSite {
                scan,
                star_var: star_var.clone(),
                class_uri: class_uri.clone(),
                prefix: Vec::new(),
            });
        }
        slots
            .iter()
            // A multivalued slot's variable is one element of an array,
            // and walking into it would address a field of that element --
            // a third reading of the address, which nothing renders. The
            // star decomposition walks single-valued hops only, for the
            // same reason.
            .find(|slot| {
                slot.var.as_deref() == Some(subject)
                    && !slot.multivalued
                    // Walking into a value that may be absent would put a
                    // condition two slots down on the preserved side of a left
                    // join, dropping the rows it exists to keep.
                    && slot.presence == SlotPresence::Required
            })
            .map(|slot| SubjectSite {
                scan,
                star_var: star_var.clone(),
                class_uri: class_uri.clone(),
                prefix: slot.path.clone(),
            })
    })
}

/// The class of the structure at `path`, when every hop is one a plan may walk
/// into.
///
/// Declines a *reference* hop, which is the distinction that keeps this from
/// answering a different question: a reference stores the target's identifier,
/// so there is no nested JSON to walk and the value two hops down lives in
/// another record -- the case the star decomposition covers with a join edge
/// and refuses to treat as a path. Declines a multivalued hop for the reason
/// in [`subject_site`].
fn class_at_path(
    schema: &SchemaView,
    class_uri: &str,
    path: &[String],
) -> Option<linkml_schemaview::classview::ClassView> {
    let mut class = schema.get_class_by_uri(class_uri).ok().flatten()?;
    for name in path {
        let slot = class.slot(&Identifier::Name(name.clone()))?;
        if slot.determine_slot_container_mode() != SlotContainerMode::SingleValue {
            return None;
        }
        if slot.determine_slot_inline_mode() == SlotInlineMode::Reference {
            return None;
        }
        class = slot.get_range_class()?;
    }
    Some(class)
}

// ---------------------------------------------------------------------------
// Fold a nested match into a path
// ---------------------------------------------------------------------------

/// A `match` reading a slot of a structure *inside* a scanned record folds
/// into that scan as a path, and the join that carried it disappears.
///
/// The remaining half of what the star decomposition pushes and the refined
/// plan rejected: `?s :location ?loc . ?loc :longitude ?lon` reads
/// `["location", "longitude"]` of one record, which `Star::path_filters`
/// renders by walking into the JSON. Without this the whole star declines any
/// filter on `?lon`, which is a capability gap against the planner that serves
/// queries today rather than a conservative choice.
///
/// Only a *variable* object folds here. A constant one is a value constraint
/// and [`ConstantObjectBecomesFilter`] resolves the same nested subject
/// through the same [`subject_site`], so `?loc :longitude 5` becomes a filter
/// on that path rather than a slot the scan reads.
///
/// Preconditions, each with the wrong answer it prevents:
///
/// * **The subject is a structure inside a scanned record**, reached by
///   single-valued inlined hops only ([`class_at_path`]). A reference hop
///   stores an identifier, so the value beyond it lives in *another record*
///   and is a join rather than a path -- walking into it would read a column
///   that does not exist.
/// * **The nested slot is single-valued.** A multivalued one fans out, and a
///   scan owes an [`PlanOp::Unnest`] for every multivalued slot it reads
///   (invariant five). This rule extends a scan that already exists, so it
///   cannot honour that debt through [`scan_with_fanout`] -- the one function
///   allowed to build a scan -- and paying it by hand is exactly the
///   discipline stage 1 replaced with a constructor. Lifting this means
///   rebuilding the scan through that function, not editing it here.
/// * **The match's one consumer is a plain `Join` on the subject variable,
///   whose other side takes every row from that scan.** As for a constant
///   object: a nested read inside an `OPTIONAL` is not a column of the
///   preserved side.
pub struct FoldNestedMatchIntoPath<'s> {
    schema: &'s SchemaView,
}

impl<'s> FoldNestedMatchIntoPath<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }
}

impl Rule for FoldNestedMatchIntoPath<'_> {
    fn name(&self) -> &'static str {
        "fold_nested_match_into_path"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for id in 0..plan.nodes.len() {
            if plan.nodes[id].executor != Executor::Engine {
                continue;
            }
            let PlanOp::Match { pattern } = &plan.nodes[id].op else {
                continue;
            };
            if is_type_pattern(pattern) {
                continue;
            }
            let (Some(subject), Some(predicate), Some(var)) = (
                subject_variable(pattern),
                predicate_iri(pattern),
                object_variable(pattern),
            ) else {
                continue;
            };
            let (subject, predicate, var) =
                (subject.to_owned(), predicate.to_owned(), var.to_owned());

            let above = consumers(plan, id);
            let [consumer] = above.as_slice() else {
                continue;
            };
            let PlanOp::Join {
                left, right, on, ..
            } = &plan.nodes[*consumer].op
            else {
                continue;
            };
            if on.as_slice() != [subject.clone()] {
                continue;
            }
            let (consumer, other) = (*consumer, if *left == id { *right } else { *left });

            let Some(site) = subject_site(plan, other, &subject) else {
                continue;
            };
            // An empty prefix is the star itself, which is the fold rule's:
            // that one has the type pattern to fold with and the multiplicity
            // machinery to fold it safely.
            if site.prefix.is_empty() {
                continue;
            }
            let Some(class) = class_at_path(self.schema, &site.class_uri, &site.prefix) else {
                continue;
            };
            let Some(slot) = self.schema.get_slot_by_uri(&predicate).ok().flatten() else {
                continue;
            };
            let Some(on_class) = class.slot(&Identifier::Name(slot.name.clone())) else {
                continue;
            };
            if on_class.determine_slot_container_mode() != SlotContainerMode::SingleValue {
                continue;
            }
            let mut path = site.prefix.clone();
            path.push(slot.name.clone());
            fold_into_scan(
                plan,
                id,
                consumer,
                other,
                site.scan,
                ScanSlot {
                    path,
                    var: Some(var),
                    multivalued: false,
                    presence: SlotPresence::Required,
                },
            );
            return true;
        }
        false
    }
}

/// Add a path to a scan's slot list, drop the match that asked for it, and
/// collapse the join that carried it.
///
/// The claim moves to the scan, which is where the other reads of the same
/// record are claimed. The join disappears rather than becoming anything: a
/// natural join with a row set already inside it adds no constraint and no
/// column, which is the same collapse [`fold`] performs.
fn fold_into_scan(
    plan: &mut Plan,
    matched: NodeId,
    join: NodeId,
    other: NodeId,
    scan: NodeId,
    slot: ScanSlot,
) {
    debug_assert!(
        !slot.multivalued,
        "a scan built outside `scan_with_fanout` cannot owe an unnest"
    );
    let mut claims = plan.nodes[matched].discharges.clone();
    claims.extend(plan.nodes[join].discharges.iter().copied());
    if let PlanOp::Scan { slots, .. } = &mut plan.nodes[scan].op {
        slots.push(slot);
    }
    plan.nodes[scan].discharges.extend(claims);
    plan.nodes[scan].discharges.sort_unstable();

    let mut nodes: Vec<Node> = Vec::with_capacity(plan.nodes.len());
    let mut remap: Vec<Option<NodeId>> = vec![None; plan.nodes.len()];
    for (old, node) in plan.nodes.iter().enumerate() {
        if old == matched {
            continue;
        }
        if old == join {
            // Everything that read the join reads the side that stays.
            remap[old] = remap[other];
            continue;
        }
        nodes.push(node.clone());
        remap[old] = Some(nodes.len() - 1);
    }
    for node in &mut nodes {
        node.op
            .map_inputs(|input| remap[input].expect("nothing reads the match but the join"));
    }

    plan.nodes = nodes;
    refresh_join_variables(plan);
}

// ---------------------------------------------------------------------------
// Turn a constant object into a filter
// ---------------------------------------------------------------------------

/// A `match` with a constant in the object position is a claim about a
/// *value*, so it becomes a `Filter` on the scan of its subject; the join that
/// carried it disappears.
///
/// [`FoldMatchesIntoScan`] declines these deliberately -- "a constant object
/// is a filter, and a filter is a claim about values rather than about
/// existence" -- and this is that rule. It rewrites rather than pushes: the
/// filter it leaves behind is `Engine`, and [`PushComparisonFilter`] is what
/// decides whether SQL can express it. Two rules because there are two
/// questions, and the second one already answers "is this constant the term
/// the column stores" for every filter in the plan.
///
/// Preconditions, each with the wrong answer it prevents:
///
/// * **The match's one consumer is a plain `Join` whose other side takes every
///   row from an `Sql` scan the subject lives on.** A constant object inside an
///   `OPTIONAL` is consumed by a `LeftJoin`, and turning it into a filter over
///   the preserved side drops exactly the rows the join exists to keep -- the
///   loss the star decomposition records as `Inexact::ConstantInOptional`.
///   [`mandatorily_feeds`] is the other half: joining a solution that leaves
///   `?s` unbound against a match that binds it *keeps* the row, so a scan
///   reached through the optional side of a left join is not a row set this
///   constraint can be moved onto.
/// * **The subject is a star, or a structure inside a scanned record.**
///   [`subject_site`] answers both, so `?loc :longitude 5` constrains
///   `["location", "longitude"]` of the record `?loc` came out of -- the
///   `PathFilter` the star decomposition pushes -- and the rule does not need
///   a second copy of that resolution.
/// * **A multivalued slot is a containment test, not a column.**
///   `:trafficKinds "m"` asks whether the record carries that triple at all,
///   which matches a record once however many values it holds -- so the
///   condition is [`SlotReading::AnyElement`] and the rewrite is still
///   cardinality-preserving. Rendering it as an equality on the column would
///   compare the array's text and match nothing.
/// * **The constant is the term the column's values render as.** See
///   [`constant_is_the_columns_term`]. Without it this rule would be the one
///   that turns "no record renders as `eul:GSA`" into "no rows", which is a
///   different answer rather than a narrower fetch.
pub struct ConstantObjectBecomesFilter<'s> {
    schema: &'s SchemaView,
}

impl<'s> ConstantObjectBecomesFilter<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }

    /// The condition a constant-object match is worth on a scanned class, when
    /// SQL can ask the same question.
    ///
    /// The term test is not repeated here: [`Expr::to_sql`] is the only way to
    /// obtain a condition and asks it, so a constant no record spells declines
    /// at the same place for this rule as for every other.
    fn condition_for(&self, site: &SubjectSite, predicate: &str, term: &Term) -> Option<Read> {
        let class = class_at_path(self.schema, &site.class_uri, &site.prefix)?;
        let slot = self.schema.get_slot_by_uri(predicate).ok().flatten()?;
        let on_class = class.slot(&Identifier::Name(slot.name.clone()))?;
        // A constant object on a multivalued slot is a containment test, and
        // *not* a fan-out: `?s :trafficKinds "m"` binds nothing, so a record
        // whose array carries the value answers the query once however many
        // values it holds. That is what makes replacing the match with a
        // filter cardinality-preserving here, and it is the reading the star
        // decomposition's renderer already performs.
        let reading = if on_class.determine_slot_container_mode() == SlotContainerMode::SingleValue
        {
            SlotReading::Column
        } else {
            SlotReading::AnyElement
        };
        let mut slot_path = site.prefix.clone();
        slot_path.push(slot.name.clone());
        let condition = Expr::Compare {
            op: CompareOp::Eq,
            left: Box::new(Expr::Slot {
                star_var: site.star_var.clone(),
                slot_path,
                reading,
            }),
            right: Box::new(Expr::Literal(term.clone())),
        };
        let classes = HashMap::from([(site.star_var.clone(), site.class_uri.clone())]);
        condition.to_sql(self.schema, &classes)?;
        Some(Read {
            condition,
            slot: slot.name.clone(),
            multivalued: reading != SlotReading::Column,
        })
    }
}

/// What a constant-object match is worth: the condition on the value, and the
/// slot whose presence it also asserts.
struct Read {
    condition: Expr,
    slot: String,
    multivalued: bool,
}

impl Rule for ConstantObjectBecomesFilter<'_> {
    fn name(&self) -> &'static str {
        "constant_object_becomes_filter"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for id in 0..plan.nodes.len() {
            if plan.nodes[id].executor != Executor::Engine {
                continue;
            }
            let PlanOp::Match { pattern } = &plan.nodes[id].op else {
                continue;
            };
            // A type pattern with a constant object is the *scope*, not a
            // value constraint on a slot, and folding it is the other rule's.
            if is_type_pattern(pattern) {
                continue;
            }
            let (Some(subject), Some(predicate), Some(term)) = (
                subject_variable(pattern),
                predicate_iri(pattern),
                constant_object(pattern),
            ) else {
                continue;
            };
            let (subject, predicate) = (subject.to_owned(), predicate.to_owned());

            // The join that carried the match is the node that disappears, so
            // the shape is checked before the schema: without it there is no
            // edit to make even for a slot this rule could constrain.
            let above = consumers(plan, id);
            let [consumer] = above.as_slice() else {
                continue;
            };
            let PlanOp::Join {
                left, right, on, ..
            } = &plan.nodes[*consumer].op
            else {
                continue;
            };
            if on.as_slice() != [subject.clone()] {
                continue;
            }
            let (consumer, other) = (*consumer, if *left == id { *right } else { *left });

            let Some(site) = subject_site(plan, other, &subject) else {
                continue;
            };
            let Some(read) = self.condition_for(&site, &predicate, &term) else {
                continue;
            };
            // The existence half of the triple, on the scan. `?s :name
            // "BX517"` asserts that the slot is there as well as what it
            // holds, and today's star says so too: the name is in
            // `required_fields` beside the condition in `filters`. It is not
            // pedantry -- `object_data ? 'name'` is the half a GIN index can
            // answer, and dropping it changes which index the statement can
            // use.
            //
            // It claims nothing: the filter is what takes care of the
            // obligation, and this is the narrowing that comes with it.
            let mut path = site.prefix.clone();
            path.push(read.slot);
            let existence = ScanSlot {
                path,
                var: None,
                multivalued: read.multivalued,
                presence: SlotPresence::Required,
            };
            if let PlanOp::Scan { slots, .. } = &mut plan.nodes[site.scan].op
                && !slots
                    .iter()
                    .any(|slot| slot.path == existence.path && slot.var.is_none())
            {
                slots.push(existence);
            }
            replace_match_with_filter(plan, id, consumer, other, read.condition);
            return true;
        }
        false
    }
}

/// The term a triple pattern's object holds, when it is a constant.
fn constant_object(pattern: &TriplePattern) -> Option<Term> {
    match &pattern.object {
        TermPattern::NamedNode(node) => Some(Term::NamedNode(node.clone())),
        TermPattern::Literal(literal) => Some(Term::Literal(literal.clone())),
        // A variable is a binding and not a constraint; a blank node names an
        // existence the plan has no column for.
        _ => None,
    }
}

/// Drop a leaf, and turn the join that carried it into the filter, reading the
/// side that stays.
///
/// Serves a constant-object `match` and a `VALUES` block alike: both are a
/// value constraint the join applied, and both leave exactly one node behind.
///
/// The claims of both move to the filter. A naive join claims nothing, so the
/// join's are empty in practice -- but a claim on a node that disappears has
/// exactly one honest home, and losing it unbalances the ledger in a release
/// build where the per-application check is only an assertion.
fn replace_match_with_filter(
    plan: &mut Plan,
    matched: NodeId,
    join: NodeId,
    input: NodeId,
    condition: Expr,
) {
    let mut claims = plan.nodes[matched].discharges.clone();
    claims.extend(plan.nodes[join].discharges.iter().copied());
    claims.sort_unstable();

    let mut nodes: Vec<Node> = Vec::with_capacity(plan.nodes.len());
    let mut remap: Vec<Option<NodeId>> = vec![None; plan.nodes.len()];
    for (old, node) in plan.nodes.iter().enumerate() {
        if old == matched {
            continue;
        }
        if old == join {
            // In the join's place, because everything that read the join
            // reads the same solutions from the filter.
            nodes.push(Node::engine(
                PlanOp::Filter {
                    input,
                    condition: condition.clone(),
                },
                claims.clone(),
            ));
        } else {
            nodes.push(node.clone());
        }
        remap[old] = Some(nodes.len() - 1);
    }
    for node in &mut nodes {
        node.op
            .map_inputs(|input| remap[input].expect("nothing reads the match but the join"));
    }

    plan.nodes = nodes;
    refresh_join_variables(plan);
}

// ---------------------------------------------------------------------------
// Turn a VALUES over a bound variable into a filter
// ---------------------------------------------------------------------------

/// A `VALUES` block joined on a variable a scan already binds is a set
/// membership test, so it becomes a `Filter` with an `IN`.
///
/// The case that makes [`crate::sparql_plan::Obligation::Values`] its own kind
/// worth having. That kind exists because a `VALUES` binding a variable
/// nothing else binds *adds rows and columns*, so calling it a filter would
/// invite a consumer to apply it as a `WHERE` and answer a narrower question.
/// The precondition here is exactly the discriminator that warning implies: a
/// `VALUES` whose variable a scan below already binds adds nothing, and then
/// it is a filter -- which is what the star decomposition does with it, as an
/// `In` condition on the slot.
///
/// Preconditions, each with the wrong answer it prevents:
///
/// * **One variable, and a scan below the other side binds it.** Two
///   variables is a table, not a set of values for a column; a variable
///   nothing else binds adds rows.
/// * **No `UNDEF`.** An unbound cell makes the row match anything, so the
///   block is not a set of values at all.
/// * **No duplicate row.** A join against `{ "a" "a" }` returns each matching
///   solution twice -- `VALUES` is a *bag* -- while `IN ('a', 'a')` returns it
///   once. Rejecting duplicates is the only reading that keeps the row count.
/// * **The side that stays binds the variable in every row**
///   ([`Visible::mandatorily_below`]), for the same reason a constant object
///   inside an `OPTIONAL` cannot become a filter.
pub struct ValuesBecomesFilter<'s> {
    schema: &'s SchemaView,
}

impl<'s> ValuesBecomesFilter<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }
}

impl Rule for ValuesBecomesFilter<'_> {
    fn name(&self) -> &'static str {
        "values_becomes_filter"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for id in 0..plan.nodes.len() {
            if plan.nodes[id].executor != Executor::Engine {
                continue;
            }
            let PlanOp::Values { variables, rows } = &plan.nodes[id].op else {
                continue;
            };
            let [variable] = variables.as_slice() else {
                continue;
            };
            let var = variable.as_str().to_owned();
            let mut terms: Vec<Term> = Vec::with_capacity(rows.len());
            for row in rows {
                let [Some(ground)] = row.as_slice() else {
                    // An `UNDEF`, or a row of the wrong width: neither is a
                    // value this variable takes.
                    terms.clear();
                    break;
                };
                terms.push(ground_term(ground));
            }
            if terms.is_empty() {
                continue;
            }
            // A bag, not a set: see the precondition above.
            if (1..terms.len()).any(|i| terms[..i].contains(&terms[i])) {
                continue;
            }

            let above = consumers(plan, id);
            let [consumer] = above.as_slice() else {
                continue;
            };
            let PlanOp::Join {
                left, right, on, ..
            } = &plan.nodes[*consumer].op
            else {
                continue;
            };
            if on.as_slice() != [var.clone()] {
                continue;
            }
            let (consumer, other) = (*consumer, if *left == id { *right } else { *left });

            let visible = Visible::mandatorily_below(plan, other);
            let Some(binding) = visible.slot_of(&var) else {
                continue;
            };
            let condition = Expr::In {
                value: Box::new(Expr::Slot {
                    star_var: binding.star_var.clone(),
                    slot_path: binding.path.clone(),
                    reading: binding.reading,
                }),
                candidates: terms.into_iter().map(Expr::Literal).collect(),
            };
            if condition
                .to_sql(self.schema, &visible.class_of_star)
                .is_none()
            {
                continue;
            }
            replace_match_with_filter(plan, id, consumer, other, condition);
            return true;
        }
        false
    }
}

/// A `VALUES` cell as a term. Total, because the cell kinds are the term kinds.
fn ground_term(ground: &GroundTerm) -> Term {
    match ground {
        GroundTerm::NamedNode(node) => Term::NamedNode(node.clone()),
        GroundTerm::Literal(literal) => Term::Literal(literal.clone()),
    }
}

// ---------------------------------------------------------------------------
// Push a comparison filter
// ---------------------------------------------------------------------------

/// A `Filter` whose expression renders as SQL conditions over the slots an
/// `Sql` scan below it binds becomes `Sql`, keeping its own obligation.
///
/// Two things happen here that 28d states separately, and both are the rule's
/// rather than the expression's:
///
/// * **A variable becomes a slot.** A naive filter compares `Expr::Var`, and a
///   variable is not a column, so [`Expr::to_sql`] declines every naive filter
///   by construction. Which slot binds `?name` is a fact about the scan below,
///   which is why the rewrite lives in the rule that can see one. The rewrite
///   is committed only together with the flip to `Sql`: a node that stays with
///   the engine keeps the expression the query wrote, because that is what the
///   engine evaluates.
/// * **A pushable conjunct sinks below an unpushable one.** See
///   [`landing_site`]. Without it, `FILTER(REGEX(..)) FILTER(?nm > "A")` pushes
///   nothing while `FILTER(?nm > "A") FILTER(REGEX(..))` pushes the
///   comparison, for two spellings of one query.
///
/// Preconditions, each with the wrong answer it prevents:
///
/// * **Every variable resolves to exactly one slot of a scan that feeds the
///   landing site.** A variable two stars bind is a value join, and naming
///   either column answers a different question. A variable bound by a
///   *multivalued* slot resolves to [`SlotReading::BoundElement`] -- the
///   element its [`PlanOp::Unnest`] bound -- and only when that unnest is
///   below the landing site; before the fan-out the element has no name, and
///   naming the array instead would select records where the query selects
///   rows. This declined altogether until the unnest carried the variable it
///   binds, which is 28d's lesson twice over: the rule could not be written
///   safely because the representation was missing a fact.
/// * **The constant is the term the column's values render as.** See
///   [`constant_is_the_columns_term`]: the pushed-conditions-only-narrow
///   argument fails for a constant no stored value spells.
/// * **The landing site runs in SQL.** The frontier is a cut, so a filter over
///   an engine node cannot be `Sql` however well it renders.
pub struct PushComparisonFilter<'s> {
    schema: &'s SchemaView,
}

impl<'s> PushComparisonFilter<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }

    /// The condition with its variables resolved to slots, when SQL can
    /// express the result.
    fn render(&self, condition: &Expr, visible: &Visible) -> Option<Expr> {
        let resolved = substitute_slots(condition, visible)?;
        // The rendering test itself, and the one 28d asks a rule to *ask*
        // rather than to decide: the pushable subset is the sum of what
        // `to_sql` accepts, not a constant of this rule. It is also where the
        // constant-is-the-column's-term half is asked, which is why it takes
        // the schema and the classes the scans below were scanned as.
        resolved.to_sql(self.schema, &visible.class_of_star)?;
        Some(resolved)
    }
}

impl Rule for PushComparisonFilter<'_> {
    fn name(&self) -> &'static str {
        "push_comparison_filter"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for id in 0..plan.nodes.len() {
            if plan.nodes[id].executor != Executor::Engine {
                continue;
            }
            let PlanOp::Filter { input, condition } = &plan.nodes[id].op else {
                continue;
            };
            let (input, condition) = (*input, condition.clone());
            let Some(base) = landing_site(plan, id) else {
                continue;
            };
            let Some(rendered) = self.render(&condition, &Visible::below(plan, base)) else {
                continue;
            };
            if base == input {
                plan.nodes[id].op = PlanOp::Filter {
                    input,
                    condition: rendered,
                };
                plan.nodes[id].executor = Executor::Sql;
            } else {
                sink_below_engine_filters(plan, id, base, rendered);
            }
            return true;
        }
        false
    }
}

/// Where a filter node can be pushed to: the highest `Sql` node it could sit
/// directly above, commuting past engine filters on the way.
///
/// `None` when there is no such node, which is the case whenever anything but
/// a privately-consumed engine filter is in the way.
///
/// **Why commuting is sound.** Two filters in sequence select the solutions
/// satisfying both, and "both" does not depend on order: a filter is
/// row-preserving (it drops solutions, never adds, reorders or duplicates
/// them), binds nothing, and SPARQL's treatment of an expression that errors
/// is that the solution is simply not selected -- so an error is a rejection
/// like any other and not an outcome the second filter could have avoided by
/// running first. Nothing in the algebra mutates, so evaluating a predicate on
/// more rows or fewer has no consequence beyond its result.
///
/// **Why it is necessary.** Obligations are per top-level conjunct and the
/// naive builder chains one `Filter` per conjunct with the first nearest the
/// input. So `FILTER(?nm > "A") FILTER(REGEX(?nm, "^A"))` puts the comparison
/// directly on the scan and it pushes, while
/// `FILTER(REGEX(?nm, "^A")) FILTER(?nm > "A")` -- the same query -- puts the
/// comparison above an engine node, where the frontier-is-a-cut invariant
/// forbids pushing it. Without sinking, the plan a query gets depends on the
/// order its filters were typed in.
///
/// **What stops the walk, and why each one has to.**
///
/// * A non-filter node. A `Bind` between the two would be the case that
///   matters: `BIND(?len * 2 AS ?d) FILTER(?d > 3)` cannot sink below the node
///   that binds `?d`. Stopping at every non-filter covers it without having to
///   reason about which nodes bind what.
/// * A filter with more than one consumer. Sinking rewires the chain so that
///   the node above the moved filter is the old top of the chain; every node
///   in between therefore starts filtering by the moved predicate, which is
///   correct for the chain's own consumer and wrong for anyone else reading a
///   node in the middle of it.
fn landing_site(plan: &Plan, filter: NodeId) -> Option<NodeId> {
    let mut current = match &plan.nodes[filter].op {
        PlanOp::Filter { input, .. } => *input,
        _ => return None,
    };
    loop {
        if plan.nodes[current].executor == Executor::Sql {
            return Some(current);
        }
        let PlanOp::Filter { input, .. } = &plan.nodes[current].op else {
            return None;
        };
        if consumers(plan, current).len() != 1 {
            return None;
        }
        current = *input;
    }
}

/// Move `filter` down to sit directly above `base`, pushed, leaving the engine
/// filters it commuted past in their order above it.
///
/// The chain `base → c_k → … → c_1 → filter` becomes
/// `base → filter' → c_k → … → c_1`, so what used to read `filter` reads `c_1`
/// -- which now selects the same solutions, because `filter'` is below it.
fn sink_below_engine_filters(plan: &mut Plan, filter: NodeId, base: NodeId, condition: Expr) {
    let pushed = Node::sql(
        PlanOp::Filter {
            input: base,
            condition,
        },
        plan.nodes[filter].discharges.clone(),
    );
    // The old top of the chain: what the moved node's consumers read instead.
    let chain_top = match &plan.nodes[filter].op {
        PlanOp::Filter { input, .. } => *input,
        _ => unreachable!("only a filter node is sunk"),
    };
    // The chain's lowest node, the one whose input becomes the moved filter.
    let mut chain_bottom = chain_top;
    while let PlanOp::Filter { input, .. } = &plan.nodes[chain_bottom].op {
        if *input == base {
            break;
        }
        chain_bottom = *input;
    }

    let mut nodes: Vec<Node> = Vec::with_capacity(plan.nodes.len() + 1);
    let mut origin: Vec<Option<NodeId>> = Vec::with_capacity(plan.nodes.len() + 1);
    let mut remap: Vec<Option<NodeId>> = vec![None; plan.nodes.len()];
    for (old, node) in plan.nodes.iter().enumerate() {
        if old == filter {
            continue;
        }
        nodes.push(node.clone());
        origin.push(Some(old));
        remap[old] = Some(nodes.len() - 1);
        if old == base {
            // Directly above the landing site, so every node of the chain --
            // all of which come after it -- still reads something that
            // precedes it.
            nodes.push(pushed.clone());
            origin.push(None);
        }
    }
    let base_landed = remap[base].expect("the landing site is not the node being moved");
    let landed = base_landed + 1;

    for index in 0..nodes.len() {
        let Some(old) = origin[index] else {
            nodes[index].op.map_inputs(|_| base_landed);
            continue;
        };
        nodes[index].op.map_inputs(|input| {
            if input == filter {
                remap[chain_top].expect("the chain is not the node being moved")
            } else if input == base && old == chain_bottom {
                landed
            } else {
                remap[input].expect("inputs precede their node")
            }
        });
    }

    plan.nodes = nodes;
    refresh_join_variables(plan);
}

/// The condition with every variable rewritten into the slot that binds it.
///
/// `None` when any variable does not resolve, which is the honest answer
/// rather than a partial rewrite: a condition mentioning one variable the SQL
/// side cannot read is not a condition the SQL side can apply.
fn substitute_slots(expr: &Expr, visible: &Visible) -> Option<Expr> {
    let all = |parts: &[Expr]| -> Option<Vec<Expr>> {
        parts
            .iter()
            .map(|part| substitute_slots(part, visible))
            .collect()
    };
    Some(match expr {
        Expr::Var(name) => {
            let binding = visible.slot_of(name)?;
            Expr::Slot {
                star_var: binding.star_var.clone(),
                slot_path: binding.path.clone(),
                reading: binding.reading,
            }
        }
        Expr::Literal(term) => Expr::Literal(term.clone()),
        Expr::Slot { .. } => expr.clone(),
        Expr::Compare { op, left, right } => Expr::Compare {
            op: *op,
            left: Box::new(substitute_slots(left, visible)?),
            right: Box::new(substitute_slots(right, visible)?),
        },
        Expr::In { value, candidates } => Expr::In {
            value: Box::new(substitute_slots(value, visible)?),
            candidates: all(candidates)?,
        },
        Expr::And(parts) => Expr::And(all(parts)?),
        Expr::Or(parts) => Expr::Or(all(parts)?),
        Expr::Not(inner) => Expr::Not(Box::new(substitute_slots(inner, visible)?)),
        Expr::Function { name, args } => Expr::Function {
            name: name.clone(),
            args: all(args)?,
        },
        // A graph pattern in an expression position. Nothing renders it, and a
        // rewrite of the text would be a rewrite of a query, not of a plan.
        Expr::Opaque(_) => return None,
    })
}

// ---------------------------------------------------------------------------
// Push a reference join
// ---------------------------------------------------------------------------

/// A `Join` between two `Sql` scans on a reference slot becomes `Sql`.
///
/// The edge is [`crate::sparql_scoper::JoinEdge`]'s: the *right* star holds the
/// foreign key -- a slot whose value is the left star's `asset360_uri` -- and
/// the plan spells that as one scan binding the variable to a slot while the
/// other scans it as a star. `?s :locatedOnTrack ?t . ?t a Track` folds into
/// exactly that pair, so the rule reads the join's variable and asks which
/// side is which.
///
/// The direction is recorded on the node as a [`ReferenceEdge`], so a consumer
/// does not repeat the derivation this rule performed -- a derivation performed
/// twice is where a renderer comes to disagree with the plan it renders. What
/// an invariant can then check is *agreement* with the scans below
/// ([`Plan::reference_joins_agree`]); that a slot really is a foreign key is
/// this rule's question, asked of the schema.
///
/// Preconditions, each with the wrong answer it prevents:
///
/// * **Exactly one join variable.** Two stars sharing two variables are joined
///   by more than the reference, and a SQL join on the foreign key alone
///   answers a weaker question than the query asked.
/// * **One side scans the variable as a star, the other binds it to a slot of
///   its own class.** A variable two stars bind as *slots* (`?a :name ?nm .
///   ?b :hasName ?nm`) is a value join between two columns, which is not this
///   edge and does not render as one.
/// * **The slot stores a reference.** An inlined slot holds the structure
///   itself, so there is no column holding the other record's identifier and
///   no row to join to -- the loss the star decomposition records as
///   `Inexact::TypedNestedStructure`. `?s :documents ?d . ?d a Document` is
///   that case.
/// * **The slot is single-valued.** A multivalued reference is an array of
///   identifiers, and while its scan's [`PlanOp::Unnest`] does restore the
///   fan-out, the join condition would then have to name the *element* rather
///   than the column -- the same missing fact that stops
///   [`PushComparisonFilter`] on a multivalued slot, and declined here for the
///   same reason rather than left to a renderer to guess.
/// * **Both sides already run in SQL.** The frontier is a cut.
pub struct PushReferenceJoin<'s> {
    schema: &'s SchemaView,
}

impl<'s> PushReferenceJoin<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }

    /// The star and slot on the foreign-key side of the edge: a scan feeding
    /// `holder` that binds `joined` to a single-valued reference slot of its
    /// own class.
    fn foreign_key_on(
        &self,
        plan: &Plan,
        holder: NodeId,
        joined: &str,
    ) -> Option<(String, String)> {
        plan.nodes
            .iter()
            .enumerate()
            .find_map(|(id, node)| match &node.op {
                PlanOp::Scan {
                    star_var,
                    class_uri,
                    slots,
                } if node.executor == Executor::Sql && plan.feeds(id, holder) => slots
                    .iter()
                    .find(|slot| {
                        slot.var.as_deref() == Some(joined)
                            && slot.presence == SlotPresence::Required
                            && !slot.multivalued
                            // A foreign key is a column of the record. A
                            // reference inside an inlined structure is not
                            // something `JoinEdge`'s one slot name can
                            // address, so it is not this rule's edge.
                            && slot.path.len() == 1
                            && self.stores_a_reference(class_uri, &slot.path[0])
                    })
                    .map(|slot| (star_var.clone(), slot.path[0].clone())),
                _ => None,
            })
    }

    /// Whether this slot of this class holds another record's identifier.
    ///
    /// Asked of the class rather than of the slot's own definition, and with
    /// the same test the star decomposition uses to raise a join edge, so the
    /// two cannot disagree about what a reference is.
    fn stores_a_reference(&self, class_uri: &str, slot_name: &str) -> bool {
        self.schema
            .get_class_by_uri(class_uri)
            .ok()
            .flatten()
            .and_then(|class| class.slot(&Identifier::Name(slot_name.to_owned())))
            .is_some_and(|slot| slot.determine_slot_inline_mode() == SlotInlineMode::Reference)
    }
}

impl Rule for PushReferenceJoin<'_> {
    fn name(&self) -> &'static str {
        "push_reference_join"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for id in 0..plan.nodes.len() {
            if plan.nodes[id].executor != Executor::Engine {
                continue;
            }
            // A left join is a different operator, and no rule pushes one: the
            // preserved side keeps rows the optional side did not match, which
            // is not what an inner join on the foreign key returns.
            let PlanOp::Join {
                left, right, on, ..
            } = &plan.nodes[id].op
            else {
                continue;
            };
            let (left, right) = (*left, *right);
            let [joined] = on.as_slice() else {
                continue;
            };
            let joined = joined.clone();
            if plan.nodes[left].executor != Executor::Sql
                || plan.nodes[right].executor != Executor::Sql
            {
                continue;
            }
            // Which side scans the joined variable as a star. `feeds` rather
            // than [`mandatorily_feeds`] for the reason given in
            // [`Visible::below`]: an `Sql` subtree holds no left join.
            let scans_the_star = |side: NodeId| {
                plan.nodes.iter().enumerate().any(|(scan, node)| {
                    matches!(&node.op, PlanOp::Scan { star_var, .. } if star_var == &joined)
                        && node.executor == Executor::Sql
                        && plan.feeds(scan, side)
                })
            };
            let referenced = if scans_the_star(left) {
                left
            } else if scans_the_star(right) {
                right
            } else {
                continue;
            };
            let holder = if referenced == left { right } else { left };
            let Some((holder_star, slot)) = self.foreign_key_on(plan, holder, &joined) else {
                continue;
            };
            plan.nodes[id].executor = Executor::Sql;
            // Recorded, not left to be re-derived: see [`ReferenceEdge`]. The
            // agreement invariant reads it back against these same scans, so a
            // rule that recorded the direction backwards fails at the rule.
            if let PlanOp::Join { reference, .. } = &mut plan.nodes[id].op {
                *reference = Some(ReferenceEdge {
                    referenced: joined,
                    holder: holder_star,
                    slot,
                });
            }
            return true;
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Push a scalar-key count grouping
// ---------------------------------------------------------------------------

/// A `GROUP BY` one scalar key with `COUNT(*)`, and the projection above it,
/// become `Sql`.
///
/// The first rule that moves a *collapsing* operator, and the last one this
/// stage writes. After a grouping the rows are aggregates rather than triples,
/// and oxigraph evaluates queries over RDF graphs -- so the engine leg, which
/// re-runs the whole original query over materialised instances, cannot finish
/// a query whose grouping already happened. That is why every rule before this
/// one was free: a narrowing cannot change an answer while the engine remains
/// authoritative. This one takes the answer.
///
/// **So it collapses fully or not at all.** A rule that grouped in SQL and
/// left algebra above would be the first thing in this design able to change
/// an answer, and there is no residual evaluator to catch it -- see 28d. The
/// projection is therefore part of the same rewrite rather than a second rule:
/// a grouping whose `Project` stayed with the engine is a partial collapse
/// wearing a complete one's clothes.
///
/// Preconditions, each with the wrong answer it prevents:
///
/// * **Everything below is already `Sql`.** A grouping over an engine node
///   would group rows a filter has not seen. The frontier-is-a-cut invariant
///   says the same thing after the fact; the rule declines before it, so a
///   plan the driver would refuse is never built.
/// * **One group key, bound by a scan below as a column or as an unnested
///   element.** Grouping on a multivalued slot groups its *values*, which the
///   fan-out below has already turned into solutions -- so the key is the
///   element, never the array. One key, so the rule cannot quietly serve a
///   shape whose ordering of keys the renderer decides.
/// * **Exactly `COUNT(*)`.** `COUNT(?v)` must not count solutions where `?v`
///   is unbound, and `COUNT(DISTINCT ?v)` counts values rather than
///   solutions; both are real shapes with their own reasoning, and neither is
///   this one.
/// * **Nothing above but the alias and the projection.** An `ORDER BY`, a
///   `LIMIT` or a `DISTINCT` above the grouping is more collapsing work, and
///   leaving it to the engine is the partial collapse this rule refuses to
///   make possible.
/// * **The fan-out is below.** Guaranteed by the fifth invariant rather than
///   by this rule -- and it is this rule that makes that invariant
///   falsifiable, since nothing collapsed rows before.
pub struct PushScalarCountGrouping<'s> {
    schema: &'s SchemaView,
}

impl<'s> PushScalarCountGrouping<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }
}

impl Rule for PushScalarCountGrouping<'_> {
    fn name(&self) -> &'static str {
        "push_scalar_count_grouping"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for id in 0..plan.nodes.len() {
            if plan.nodes[id].executor != Executor::Engine {
                continue;
            }
            let PlanOp::Group {
                input,
                keys,
                measures,
            } = &plan.nodes[id].op
            else {
                continue;
            };
            let (input, keys, measures) = (*input, keys.clone(), measures.clone());

            if plan.nodes[input].executor != Executor::Sql {
                continue;
            }
            let [key] = keys.as_slice() else {
                continue;
            };
            let [measure] = measures.as_slice() else {
                continue;
            };
            if !matches!(
                measure.aggregate,
                AggregateExpression::CountSolutions { distinct: false }
            ) {
                continue;
            }

            // The key has to be a value an `Sql` scan below binds, and
            // `Visible` answers both which one and how to read it.
            //
            // Two readings are keys. A `Column` is the scalar case. A
            // `BoundElement` is one element of an array, and grouping on it is
            // what SPARQL means by grouping on a multivalued slot: the fan-out
            // below has already turned each value into its own solution, so
            // the groups are values and the count is one per solution. That
            // the unnest is below is not assumed -- `Visible` only reports
            // `BoundElement` when it is, and the fifth invariant refuses a
            // plan where it drifts above the grouping.
            //
            // `AnyElement` is not a key: it is a test over a record's array,
            // and there is no single value to group by.
            let visible = Visible::below(plan, input);
            let Some(binding) = visible.slot_of(key) else {
                continue;
            };
            if !matches!(
                binding.reading,
                SlotReading::Column | SlotReading::BoundElement
            ) {
                continue;
            }
            // Resolvable as a column, or the lowering could not render it.
            let Some(class_uri) = visible.class_of_star.get(&binding.star_var) else {
                continue;
            };
            if crate::sparql_scoper::push_form_of_path(self.schema, class_uri, &binding.path)
                == crate::sparql_scoper::PushForm::Tagged
            {
                continue;
            }

            // What sits above: an optional alias for the measure, then the
            // query's own projection, and nothing else.
            let Some(shape) = grouping_tail(plan, id, &measure.var) else {
                continue;
            };
            push_grouping(plan, id, shape);
            return true;
        }
        false
    }
}

/// The nodes above a grouping that this rule takes with it.
struct GroupingTail {
    /// The `Bind` that names the aggregate, when the query wrote `AS ?n`.
    alias: Option<(NodeId, String)>,
    project: NodeId,
}

/// The shape above a grouping, when it is one this rule can push whole.
///
/// `None` for anything else, and "anything else" is the point: an `ORDER BY`,
/// a `LIMIT`, a `DISTINCT` or a second projection is collapsing or reordering
/// work, and pushing the grouping under it would leave that work to an engine
/// that cannot do it.
fn grouping_tail(plan: &Plan, group: NodeId, measure_var: &str) -> Option<GroupingTail> {
    let mut current = single_consumer(plan, group)?;
    let mut alias = None;
    if let PlanOp::Bind { var, expr, .. } = &plan.nodes[current].op {
        // Only the rename spargebra emits for `(COUNT(*) AS ?n)`. A `BIND` of
        // anything else computes a value, which SQL would have to evaluate.
        if *expr != Expr::Var(measure_var.to_owned()) {
            return None;
        }
        alias = Some((current, var.clone()));
        current = single_consumer(plan, current)?;
    }
    if !matches!(plan.nodes[current].op, PlanOp::Project { .. }) {
        return None;
    }
    // The projection is the top: a node above it is a shaping node
    // (`CONSTRUCT`, `ASK`) or a modifier this rule does not push, and either
    // way the grouping would be feeding something the engine still does.
    if current != plan.nodes.len() - 1 {
        return None;
    }
    Some(GroupingTail {
        alias,
        project: current,
    })
}

/// The one node that reads this one, when exactly one does.
fn single_consumer(plan: &Plan, node: NodeId) -> Option<NodeId> {
    match consumers(plan, node).as_slice() {
        [only] => Some(*only),
        _ => None,
    }
}

/// Flip the grouping and its projection to `Sql`, folding the alias into the
/// measure it names.
///
/// The alias node disappears: SQL names a result column, so a rename above the
/// grouping is a fact about the grouping rather than a step after it -- and
/// the lowering has no operator for a `Bind`. Its claims move to the grouping,
/// though a naive `Bind` has none.
fn push_grouping(plan: &mut Plan, group: NodeId, shape: GroupingTail) {
    if let Some((_, alias)) = &shape.alias
        && let PlanOp::Group { measures, .. } = &mut plan.nodes[group].op
        && let [measure] = measures.as_mut_slice()
    {
        // Renamed here so the lowered measure carries the name the query gave
        // it. Without this the result column is spargebra's internal hash,
        // which is not an answer anyone asked for.
        measure.var = alias.clone();
    }
    plan.nodes[group].executor = Executor::Sql;
    plan.nodes[shape.project].executor = Executor::Sql;

    let Some((alias_node, _)) = shape.alias else {
        return;
    };
    let mut claims = plan.nodes[alias_node].discharges.clone();
    claims.sort_unstable();
    plan.nodes[group].discharges.extend(claims);
    plan.nodes[group].discharges.sort_unstable();

    let input = plan.nodes[alias_node].op.inputs()[0];
    let mut nodes: Vec<Node> = Vec::with_capacity(plan.nodes.len() - 1);
    let mut remap: Vec<Option<NodeId>> = vec![None; plan.nodes.len()];
    for (old, node) in plan.nodes.iter().enumerate() {
        if old == alias_node {
            remap[old] = remap[input];
            continue;
        }
        nodes.push(node.clone());
        remap[old] = Some(nodes.len() - 1);
    }
    for node in &mut nodes {
        node.op
            .map_inputs(|input| remap[input].expect("inputs precede their node"));
    }
    plan.nodes = nodes;
    refresh_join_variables(plan);
}

// ---------------------------------------------------------------------------
// The tier-one rule set
// ---------------------------------------------------------------------------

/// Every rule, in the order 28d lists them: scope a type, fold a nested read
/// into a path, deliver an optional read, turn a constant object into a
/// filter, turn a `VALUES` over a bound variable into one, push a comparison,
/// push a reference join, and -- the one that is not tier one --
/// [`PushScalarCountGrouping`].
///
/// The name is now half a lie and kept anyway: seven of the eight are
/// non-collapsing, and the eighth is the first rule that takes the answer
/// rather than narrowing what the engine will decide. Renaming it would say
/// less than this paragraph does.
///
/// Tier one is the set that needs nothing new from the executor. The engine
/// leg re-runs the whole query, so a node below the first row-collapsing
/// operator only ever narrows what SQL hands over -- provided the condition it
/// applies is the query's own, which is the one place these rules are stricter
/// than that argument (see [`constant_is_the_columns_term`]).
///
/// The order is a preference and not a requirement: each rule is monotone --
/// three of them remove a `match`, two turn an `Engine` node `Sql`, one adds a
/// delivered slot to a scan, and none of them ever does the reverse -- so
/// the
/// driver reaches the same fixpoint from any order, and
/// `the_rule_order_does_not_decide_the_fixpoint` holds it to that. What the
/// order buys is rounds: a filter cannot push before the scan below it exists,
/// so running the fold first reaches the fixpoint in fewer passes.
pub fn tier_one_rules(schema: &SchemaView) -> Vec<Box<dyn Rule + '_>> {
    vec![
        Box::new(FoldMatchesIntoScan::new(schema)),
        Box::new(FoldNestedMatchIntoPath::new(schema)),
        Box::new(DeliverOptionalRead::new(schema)),
        Box::new(AbsorbOptionalRead::new(schema)),
        Box::new(ConstantObjectBecomesFilter::new(schema)),
        Box::new(ValuesBecomesFilter::new(schema)),
        Box::new(PushComparisonFilter::new(schema)),
        Box::new(PushReferenceJoin::new(schema)),
        Box::new(PushScalarCountGrouping::new(schema)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparql_plan::Obligation;
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
            .map(|slot| {
                (
                    slot.path.join("."),
                    slot.var.unwrap_or_default(),
                    slot.multivalued,
                )
            })
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

    /// What the rule is for: two stars joined on a reference slot become one
    /// SQL statement -- the scan of the referenced star, the scan of the star
    /// holding the foreign key, and the join between them.
    #[test]
    fn a_reference_join_between_two_scans_is_pushed() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t a asset360:Track ; asset360:hasName ?tn }",
        );
        let log = refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &PushReferenceJoin::new(&schema),
            ],
        )
        .expect("every invariant holds");
        assert!(log.applied.contains(&"push_reference_join"), "{plan}");

        let join = plan.find("join")[0];
        assert_eq!(plan.nodes[join].executor, Executor::Sql, "{plan}");
        assert!(
            plan.nodes
                .iter()
                .filter(|node| node.op.kind() == "scan")
                .all(|node| node.executor == Executor::Sql),
            "{plan}"
        );
        assert_eq!(
            plan.nodes.last().unwrap().executor,
            Executor::Engine,
            "the projection is still the engine's:\n{plan}"
        );
        println!("{plan}");
    }

    /// The pushed join records which side holds the foreign key, in the
    /// vocabulary `JoinEdge` uses, so a consumer does not repeat the
    /// derivation the rule already performed.
    #[test]
    fn a_pushed_join_records_the_reference_it_joins_on() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t a asset360:Track ; asset360:hasName ?tn }",
        );
        refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &PushReferenceJoin::new(&schema),
            ],
        )
        .expect("every invariant holds");

        let join = plan.find("join")[0];
        let PlanOp::Join {
            reference: Some(edge),
            ..
        } = &plan.nodes[join].op
        else {
            panic!("a pushed join carries its edge:\n{plan}");
        };
        assert_eq!(edge.referenced, "t", "{plan}");
        assert_eq!(edge.holder, "s", "{plan}");
        assert_eq!(edge.slot, "locatedOnTrack", "{plan}");
        // And it is in the printout, because a reader checking a plan should
        // not have to reconstruct the direction either.
        assert!(
            plan.nodes[join]
                .op
                .describe()
                .contains("via ?s.locatedOnTrack"),
            "{plan}"
        );

        // A join no rule pushed records nothing: a natural join on a shared
        // variable is not necessarily a reference.
        let mut value_join = plan_of(
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             ?t a asset360:Track ; asset360:hasName ?nm }",
        );
        refine(
            &mut value_join,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &PushReferenceJoin::new(&schema),
            ],
        )
        .expect("every invariant holds");
        assert!(
            matches!(
                &value_join.nodes[value_join.find("join")[0]].op,
                PlanOp::Join {
                    reference: None,
                    ..
                }
            ),
            "{value_join}"
        );
    }

    /// The same query, its two stars written in either order, must push the
    /// same join.
    ///
    /// The naive plan's join shape depends on that order: with the reference
    /// in the first star the surviving join carries `on = [?t]`, and with the
    /// two stars written separately it is a *cross product* whose `on` is
    /// empty until the fold moves the reference onto a scan. A rule reading a
    /// stale `on` pushed one and declined the other -- the filter-ordering bug
    /// again, in a different operator, and found by the runtime gate's log
    /// rather than by a test.
    #[test]
    fn the_order_two_stars_are_written_in_does_not_decide_the_join() {
        let schema = test_schema_view();
        let pushed = |query: &str| -> bool {
            let plan = refined(query, &schema, false);
            let joins = plan.find("join");
            assert_eq!(joins.len(), 1, "{plan}");
            plan.nodes[joins[0]].executor == Executor::Sql
        };

        assert!(
            pushed(
                "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
                 ?t a asset360:Track ; asset360:hasName ?tn }"
            ),
            "the reference in the first star"
        );
        assert!(
            pushed(
                "SELECT ?bn WHERE { ?sig a asset360:Signal . \
                 ?bg a asset360:BaliseGroup ; asset360:refersToSignal ?sig }"
            ),
            "the reference in the second star, which the naive plan joins as a \
             cross product"
        );
    }

    /// The sixth invariant. A recorded direction cannot be checked against the
    /// *schema* by a plan -- whether a slot is a foreign key is the rule's
    /// question -- but it can be checked against the scans the join joins, and
    /// a wrong direction is otherwise a wrong join in a plan every other
    /// invariant passes.
    #[test]
    fn a_reference_edge_that_disagrees_with_the_scans_is_a_defect() {
        /// Turns the edge around: `?t` holds no `locatedOnTrack`, so the
        /// recorded holder is not the scan that bound the joined variable.
        struct ReversesTheEdge;
        impl Rule for ReversesTheEdge {
            fn name(&self) -> &'static str {
                "reverses_the_edge"
            }
            fn apply(&self, plan: &mut Plan) -> bool {
                for node in &mut plan.nodes {
                    if let PlanOp::Join {
                        reference: Some(edge),
                        ..
                    } = &mut node.op
                    {
                        std::mem::swap(&mut edge.referenced, &mut edge.holder);
                    }
                }
                // Reports no change, so the driver's result check is what has
                // to catch it.
                false
            }
        }

        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t a asset360:Track ; asset360:hasName ?tn }",
        );
        refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &PushReferenceJoin::new(&schema),
            ],
        )
        .expect("every invariant holds");

        let failure = refine(&mut plan, &[&ReversesTheEdge])
            .expect_err("a reversed edge must not be handed back");
        assert!(
            matches!(
                failure.defect,
                crate::sparql_refine::PlanDefect::MisrecordedJoin { .. }
            ),
            "{failure}"
        );
    }

    /// A left join is a different operator and no rule pushes one, even with
    /// both sides pushed: the preserved side keeps rows the optional side did
    /// not match, which is not what an inner join on the foreign key returns.
    #[test]
    fn a_left_join_is_not_pushed() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             OPTIONAL { ?t a asset360:Track ; asset360:hasName ?tn } }",
        );
        refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &PushReferenceJoin::new(&schema),
            ],
        )
        .expect("every invariant holds");

        let leftjoin = plan.find("leftjoin")[0];
        assert_eq!(plan.nodes[leftjoin].executor, Executor::Engine, "{plan}");
        // Both sides *are* pushed, so this is the rule declining rather than
        // the frontier being unreachable -- which is the case worth testing.
        for input in plan.nodes[leftjoin].op.inputs() {
            assert_eq!(plan.nodes[input].executor, Executor::Sql, "{plan}");
        }
        assert!(plan.find("join").is_empty(), "{plan}");
        println!("{plan}");
    }

    /// An inlined slot holds the structure itself, so there is no column
    /// holding the other record's identifier and no row to join to. The same
    /// query without the nested `rdf:type` is a path into the JSON, which is a
    /// different plan and not this rule's.
    #[test]
    fn a_join_into_an_inlined_structure_is_not_pushed() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?title WHERE { ?s a asset360:Signal ; asset360:documents ?d . \
             ?d a asset360:Document ; asset360:title ?title }",
        );
        refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &PushReferenceJoin::new(&schema),
            ],
        )
        .expect("every invariant holds");

        assert_eq!(
            plan.nodes[plan.find("join")[0]].executor,
            Executor::Engine,
            "{plan}"
        );
        println!("{plan}");
    }

    /// A multivalued reference is an array of identifiers. Its unnest does
    /// restore the fan-out, but the join would have to compare the *element*
    /// and the plan has no name for one -- the same missing fact that stops a
    /// comparison on a multivalued slot.
    #[test]
    fn a_multivalued_reference_join_is_not_pushed() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?ln WHERE { ?g a asset360:LineGroup ; asset360:groupsLines ?l . \
             ?l a asset360:Line ; asset360:hasName ?ln }",
        );
        refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &PushReferenceJoin::new(&schema),
            ],
        )
        .expect("every invariant holds");

        assert_eq!(
            plan.nodes[plan.find("join")[0]].executor,
            Executor::Engine,
            "{plan}"
        );
        // The fan-out is still restored, which is what makes the *scan* side
        // of this correct however the join is executed.
        assert_eq!(plan.find("unnest").len(), 1, "{plan}");
        println!("{plan}");
    }

    /// A variable two stars bind as slots is a value join between two columns,
    /// not a reference edge: neither side scans it as a star.
    #[test]
    fn a_value_join_between_two_columns_is_not_pushed() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             ?t a asset360:Track ; asset360:hasName ?nm }",
        );
        refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &PushReferenceJoin::new(&schema),
            ],
        )
        .expect("every invariant holds");

        assert_eq!(
            plan.nodes[plan.find("join")[0]].executor,
            Executor::Engine,
            "{plan}"
        );
    }

    /// The whole rule set, for a test that does not care which rule fired.
    fn refine_with_tier_one(query: &str, schema: &SchemaView) -> Plan {
        refined(query, schema, false)
    }

    /// What the rule is for: a `VALUES` whose variable a scan already binds is
    /// a set membership test, which is what the star decomposition makes of it
    /// too -- an `In` condition on the slot.
    #[test]
    fn values_over_a_bound_variable_becomes_an_in_filter() {
        let schema = test_schema_view();
        let plan = refined(
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             VALUES ?nm { \"a\" \"b\" } }",
            &schema,
            false,
        );

        assert!(plan.find("values").is_empty(), "{plan}");
        assert!(plan.find("join").is_empty(), "{plan}");
        let filter = plan.find("filter")[0];
        assert_eq!(
            plan.nodes[filter].op.describe(),
            "(?s.name IN (\"a\", \"b\"))",
            "{plan}"
        );
        assert_eq!(plan.nodes[filter].executor, Executor::Sql, "{plan}");
        // The `Values` obligation moves with the constraint -- and the refined
        // plan claims it in SQL, which today's ledger does not.
        assert_eq!(
            plan.nodes[filter]
                .discharges
                .iter()
                .map(|id| plan.obligations[*id].to_string())
                .collect::<Vec<_>>(),
            vec!["values    VALUES ?nm × 2 row(s)"],
            "{plan}"
        );
        println!("{plan}");
    }

    /// The three shapes that are not a set of values for a column, and the
    /// fourth that is a set but not a filter.
    #[test]
    fn a_values_block_that_is_not_a_membership_test_stays_a_join() {
        let schema = test_schema_view();
        for (query, why) in [
            (
                // A bag, not a set: the join returns each matching solution
                // twice and `IN ('a', 'a')` returns it once.
                "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 VALUES ?nm { \"a\" \"a\" } }",
                "a duplicate row multiplies solutions",
            ),
            (
                // An unbound cell matches anything, so the block is not a set
                // of values for the column.
                "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 VALUES ?nm { \"a\" UNDEF } }",
                "UNDEF is not a value",
            ),
            (
                // Two variables is a table, not a column's values.
                "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm ; \
                 asset360:length ?len . VALUES (?nm ?len) { (\"a\" 1) } }",
                "two variables is a table",
            ),
            (
                // The star variable is the record's identity, and the plan has
                // no address for that -- only for a record's slots. Today's
                // planner pushes it as `identifier_values`, so this is a gap
                // and not a refusal: see the note on `Visible`.
                "SELECT ?s WHERE { ?s a asset360:Signal . VALUES ?s { \"u\" } }",
                "a star variable is not a slot",
            ),
        ] {
            let plan = refined(query, &schema, false);
            assert_eq!(
                plan.find("values").len(),
                1,
                "{why}, so the block stays: {query}\n{plan}"
            );
            assert!(plan.find("filter").is_empty(), "{why}: {query}\n{plan}");
        }
    }

    /// An `OPTIONAL` read of a scanned star becomes a bound nullable column,
    /// and the left join disappears.
    ///
    /// Two rules in sequence, and the pair is the point.
    /// [`DeliverOptionalRead`] puts the column on the scan with no existence
    /// check and *no variable*, so the engine can bind it and no SQL node can.
    /// [`AbsorbOptionalRead`] gives it the variable, which is the moment the
    /// SQL side starts producing the solution the `OPTIONAL` asks for: the
    /// value where there is one, `NULL` where there is not.
    ///
    /// And that is where the claim becomes honest. The scan takes the triple's
    /// obligation because it now answers it -- the conditional claim
    /// [`SlotPresence`] describes, arriving at the condition that makes it
    /// true. Before this the `match` kept the claim, and the two planners
    /// disagreed about the ledger for every `OPTIONAL` read.
    #[test]
    fn an_optional_read_becomes_a_bound_nullable_column() {
        let schema = test_schema_view();
        let plan = refined(
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { ?s asset360:name ?nm } }",
            &schema,
            false,
        );

        assert!(plan.find("match").is_empty(), "{plan}");
        assert!(plan.find("leftjoin").is_empty(), "{plan}");
        let scan = plan.find("scan")[0];
        let PlanOp::Scan { slots, .. } = &plan.nodes[scan].op else {
            panic!("{plan}");
        };
        let [required, optional] = slots.as_slice() else {
            panic!("two reads: {slots:?}");
        };
        assert_eq!(required.presence, SlotPresence::Required, "{plan}");
        assert_eq!(optional.presence, SlotPresence::Optional, "{plan}");
        assert_eq!(
            optional.var.as_deref(),
            Some("nm"),
            "absorbed, so the SQL side binds it:\n{plan}"
        );

        // The scan claims the optional triple, because it is what answers it.
        assert_eq!(
            plan.nodes[scan]
                .discharges
                .iter()
                .map(|id| plan.obligations[*id].to_string())
                .collect::<Vec<_>>(),
            vec![
                "type      ?s a asset360:Signal".to_owned(),
                "triple    ?s asset360:kind ?k".to_owned(),
                "triple    ?s asset360:name ?nm".to_owned(),
            ],
            "{plan}"
        );
        plan.check()
            .unwrap_or_else(|defect| panic!("{defect}\n{plan}"));
        println!("{plan}");
    }

    /// A lifted condition is not absorbed, and this is the placement error the
    /// rule exists to refuse.
    ///
    /// `OPTIONAL { ?s :name ?nm . FILTER(?nm > "A") }` decides whether the
    /// *value* binds, not whether the row survives: a signal named "A" is
    /// still an answer, with `?nm` unbound. Rendering the condition as a
    /// `WHERE` would delete that row -- turning the left join into an inner
    /// one, quietly, with a smaller answer and no error.
    #[test]
    fn a_condition_inside_the_optional_is_not_absorbed() {
        let schema = test_schema_view();
        let plan = refined(
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { ?s asset360:name ?nm . FILTER(?nm > \"A\") } }",
            &schema,
            false,
        );

        assert_eq!(plan.find("leftjoin").len(), 1, "{plan}");
        assert_eq!(
            plan.nodes[plan.find("leftjoin")[0]].executor,
            Executor::Engine,
            "{plan}"
        );
        assert_eq!(plan.find("match").len(), 1, "{plan}");
        // The column is still delivered, so the engine has what it needs.
        assert!(
            scan_slots(&plan).contains(&("name".to_owned(), String::new(), false)),
            "{plan}"
        );
        println!("{plan}");
    }

    /// What an absorbed optional read may and may not be read by.
    ///
    /// A *filter* may: every condition this pushes is false or unknown against
    /// `NULL` -- `=`, `IN` and the four orderings alike -- and SPARQL says the
    /// same, since a comparison with an unbound variable is an error and an
    /// error does not select the solution. The filter node sits above the
    /// former left join, which is exactly where the query wrote it.
    ///
    /// A *join* may not: a foreign key that may be absent is not a key, and an
    /// inner join on it would drop the row the `OPTIONAL` keeps. Nor may a
    /// nested walk go through one, for the same reason one hop further down.
    #[test]
    fn what_may_read_an_absorbed_optional_column() {
        let schema = test_schema_view();

        let filtered = refined(
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { ?s asset360:name ?nm } FILTER(?nm > \"A\") }",
            &schema,
            false,
        );
        assert_eq!(
            filtered.nodes[filtered.find("filter")[0]].executor,
            Executor::Sql,
            "{filtered}"
        );

        // A reference read optionally is not a foreign key a join may use.
        let joined = refined(
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { ?s asset360:locatedOnTrack ?t . ?t a asset360:Track ; \
             asset360:hasName ?tn } }",
            &schema,
            false,
        );
        assert!(
            joined
                .find("join")
                .iter()
                .all(|id| joined.nodes[*id].executor == Executor::Engine),
            "{joined}"
        );

        // And a nested read is not walked into through one.
        let nested = refined(
            "SELECT ?lon WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { ?s asset360:location ?loc . ?loc asset360:longitude ?lon } }",
            &schema,
            false,
        );
        assert!(
            !scan_slots(&nested)
                .iter()
                .any(|(path, _, _)| path.contains('.')),
            "{nested}"
        );
    }

    /// A multivalued optional read stays delivered and unbound: absorbing it
    /// would owe a fan-out *inside* the optional side, which no rule builds --
    /// and the fifth invariant refuses that shape rather than trusting a rule
    /// to decline it, since any *bound* multivalued read owes an unnest.
    #[test]
    fn a_delivered_array_does_not_fan_out() {
        let schema = test_schema_view();
        let plan = refined(
            "SELECT ?s ?k WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             OPTIONAL { ?s asset360:trafficKinds ?k } }",
            &schema,
            false,
        );

        assert!(
            scan_slots(&plan).contains(&("trafficKinds".to_owned(), String::new(), true)),
            "{plan}"
        );
        assert!(plan.find("unnest").is_empty(), "{plan}");
        plan.check()
            .unwrap_or_else(|defect| panic!("{defect}\n{plan}"));
        println!("{plan}");
    }

    /// A nested read becomes a path on the scan, and a filter on it pushes.
    /// This is `Star::path_filters` reached by rules: `?z` lives two slots
    /// down, which no column key can name, and dropping the condition counted
    /// every record where the query counts some.
    #[test]
    fn a_nested_read_folds_into_a_path_a_filter_can_push() {
        let schema = test_schema_view();
        let plan = refine_with_tier_one(
            "SELECT ?lon WHERE { ?s a asset360:Signal ; asset360:location ?loc . \
             ?loc asset360:longitude ?lon . FILTER(?lon > 3) }",
            &schema,
        );

        assert_eq!(
            scan_slots(&plan),
            vec![
                ("location".to_owned(), "loc".to_owned(), false),
                ("location.longitude".to_owned(), "lon".to_owned(), false),
            ],
            "{plan}"
        );
        assert!(plan.find("match").is_empty(), "{plan}");
        assert!(
            plan.find("join").is_empty(),
            "the join that carried the nested read collapses:\n{plan}"
        );
        let filter = plan.find("filter")[0];
        assert_eq!(
            plan.nodes[filter].op.describe(),
            "(?s.location.longitude > \"3\"^^<http://www.w3.org/2001/XMLSchema#integer>)",
            "{plan}"
        );
        assert_eq!(plan.nodes[filter].executor, Executor::Sql, "{plan}");
        println!("{plan}");
    }

    /// A constant two slots down is the same path, reached by the constant
    /// rule through the same resolution -- one place decides where a subject
    /// lives.
    #[test]
    fn a_nested_constant_becomes_a_path_filter() {
        let schema = test_schema_view();
        let plan = refine_with_tier_one(
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:location ?loc . \
             ?loc asset360:longitude 5 }",
            &schema,
        );

        let filter = plan.find("filter")[0];
        assert_eq!(
            plan.nodes[filter].op.describe(),
            "(?s.location.longitude = \"5\"^^<http://www.w3.org/2001/XMLSchema#integer>)",
            "{plan}"
        );
        assert_eq!(plan.nodes[filter].executor, Executor::Sql, "{plan}");
        assert!(plan.find("match").is_empty(), "{plan}");
        println!("{plan}");
    }

    /// Three hops, because a path is a path: the rule applies to its own
    /// output, one hop per application.
    #[test]
    fn a_path_can_be_deeper_than_two_slots() {
        let schema = test_schema_view();
        let plan = refine_with_tier_one(
            "SELECT ?v WHERE { ?s a asset360:Signal ; asset360:location ?loc . \
             ?loc asset360:detail ?d . ?d asset360:value ?v . FILTER(?v = \"x\") }",
            &schema,
        );

        assert!(
            scan_slots(&plan).contains(&(
                "location.detail.value".to_owned(),
                "v".to_owned(),
                false
            )),
            "{plan}"
        );
        assert_eq!(
            plan.nodes[plan.find("filter")[0]].op.describe(),
            "(?s.location.detail.value = \"x\")",
            "{plan}"
        );
        assert_eq!(
            plan.nodes[plan.find("filter")[0]].executor,
            Executor::Sql,
            "{plan}"
        );
        println!("{plan}");
    }

    /// A reference hop is not walked into, and this is the distinction that
    /// keeps a path from answering a different question: the foreign key holds
    /// an identifier, so `?tn` lives in *another record*. The star
    /// decomposition refuses the same shape, and the query with the nested
    /// `rdf:type` is the one that becomes a join.
    #[test]
    fn a_reference_hop_is_not_walked_into() {
        let schema = test_schema_view();
        let plan = refine_with_tier_one(
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t asset360:hasName ?tn . FILTER(?tn > \"A\") }",
            &schema,
        );

        assert_eq!(
            scan_slots(&plan),
            vec![("locatedOnTrack".to_owned(), "t".to_owned(), false)],
            "the reference folds as a column; what is beyond it does not:\n{plan}"
        );
        assert_eq!(plan.find("match").len(), 1, "{plan}");
        assert_eq!(
            plan.nodes[plan.find("filter")[0]].executor,
            Executor::Engine,
            "{plan}"
        );
        println!("{plan}");
    }

    /// A multivalued hop does not fold. The value beyond it belongs to one
    /// element of an array, which is a third reading of an address and one
    /// nothing renders; and a scan owing an unnest can only be built by
    /// `scan_with_fanout`, which this rule does not go through.
    #[test]
    fn a_multivalued_hop_is_not_walked_into() {
        let schema = test_schema_view();
        let plan = refine_with_tier_one(
            "SELECT ?ti WHERE { ?s a asset360:Signal ; asset360:documents ?d . \
             ?d asset360:title ?ti }",
            &schema,
        );

        assert_eq!(
            scan_slots(&plan),
            vec![("documents".to_owned(), "d".to_owned(), true)],
            "{plan}"
        );
        assert_eq!(plan.find("match").len(), 1, "{plan}");
        // The array itself still folds, so its fan-out is still restored.
        assert_eq!(plan.find("unnest").len(), 1, "{plan}");
        println!("{plan}");
    }

    /// A nested read inside an `OPTIONAL` is not a column of the preserved
    /// side, the same way a constant object is not.
    #[test]
    fn a_nested_read_inside_an_optional_stays_a_match() {
        let schema = test_schema_view();
        let plan = refine_with_tier_one(
            "SELECT ?lon WHERE { ?s a asset360:Signal ; asset360:location ?loc . \
             OPTIONAL { ?loc asset360:longitude ?lon } }",
            &schema,
        );

        assert_eq!(
            scan_slots(&plan),
            vec![("location".to_owned(), "loc".to_owned(), false)],
            "{plan}"
        );
        assert_eq!(plan.find("match").len(), 1, "{plan}");
        assert_eq!(plan.find("leftjoin").len(), 1, "{plan}");
        println!("{plan}");
    }

    /// What the rule is for: a constant object is a value constraint, so it
    /// becomes a filter over the scan of its subject and the join that
    /// carried it disappears. The filter is then pushable like any other.
    #[test]
    fn a_constant_object_becomes_a_pushed_filter() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?len WHERE { ?s a asset360:Signal ; asset360:name \"BX517\" ; \
             asset360:length ?len }",
        );
        let log = refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &ConstantObjectBecomesFilter::new(&schema),
                &PushComparisonFilter::new(&schema),
            ],
        )
        .expect("every invariant holds");
        assert!(
            log.applied.contains(&"constant_object_becomes_filter"),
            "{plan}"
        );

        assert!(
            plan.find("match").is_empty(),
            "the constant match is gone:\n{plan}"
        );
        assert!(
            plan.find("join").is_empty(),
            "and so is the join that carried it:\n{plan}"
        );
        let filter = plan.find("filter")[0];
        assert_eq!(
            plan.nodes[filter].op.describe(),
            "(?s.name = \"BX517\")",
            "{plan}"
        );
        assert_eq!(plan.nodes[filter].executor, Executor::Sql, "{plan}");
        // The obligation moves with the constraint: a constant object raises a
        // *triple* obligation, and the filter that replaced the match is what
        // takes care of it.
        assert_eq!(
            plan.nodes[filter]
                .discharges
                .iter()
                .map(|id| plan.obligations[*id].to_string())
                .collect::<Vec<_>>(),
            vec!["triple    ?s asset360:name \"BX517\""],
            "{plan}"
        );
        println!("{plan}");
    }

    /// Two constants on one star are two applications and two filters, and the
    /// second one lands on the first -- which is what makes the chain the
    /// filter rule sinks through something a rule can build as well as parse.
    #[test]
    fn two_constant_objects_become_two_filters() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name \"BX517\" ; \
             asset360:length 3 }",
        );
        refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &ConstantObjectBecomesFilter::new(&schema),
                &PushComparisonFilter::new(&schema),
            ],
        )
        .expect("every invariant holds");

        assert_eq!(plan.find("filter").len(), 2, "{plan}");
        assert!(
            plan.find("filter")
                .iter()
                .all(|id| plan.nodes[*id].executor == Executor::Sql),
            "{plan}"
        );
        assert!(plan.find("match").is_empty(), "{plan}");
        assert!(plan.find("join").is_empty(), "{plan}");
        println!("{plan}");
    }

    /// A constant object inside an `OPTIONAL` is consumed by a `LeftJoin`, and
    /// a filter over the preserved side drops exactly the rows the join exists
    /// to keep: a signal with no name would stop being an answer.
    #[test]
    fn a_constant_object_inside_an_optional_stays_a_match() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { ?s asset360:name \"BX517\" } }",
        );
        let log = refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &ConstantObjectBecomesFilter::new(&schema),
                &PushComparisonFilter::new(&schema),
            ],
        )
        .expect("every invariant holds");

        assert!(
            !log.applied.contains(&"constant_object_becomes_filter"),
            "{plan}"
        );
        assert_eq!(plan.find("match").len(), 1, "{plan}");
        assert_eq!(plan.find("leftjoin").len(), 1, "{plan}");
        assert!(plan.find("filter").is_empty(), "{plan}");
        println!("{plan}");
    }

    /// The other half of the optional precondition, and the one a structural
    /// check alone misses: here the constant match *is* consumed by a plain
    /// join, but the side it joins to reaches the scan of `?s` through the
    /// optional side of a left join. A solution that leaves `?s` unbound is
    /// compatible with a pattern that binds it, so that join keeps the row --
    /// and a filter on the scan's column does not.
    #[test]
    fn a_constant_object_joined_through_an_optional_stays_a_match() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?s ?t WHERE { ?t a asset360:Track . \
             OPTIONAL { ?s a asset360:Signal } . ?s asset360:name \"BX517\" }",
        );
        let log = refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &ConstantObjectBecomesFilter::new(&schema),
            ],
        )
        .expect("every invariant holds");

        assert!(
            !log.applied.contains(&"constant_object_becomes_filter"),
            "the scan is reached through a left join:\n{plan}"
        );
        assert_eq!(plan.find("match").len(), 1, "{plan}");
        println!("{plan}");
    }

    /// The same term test the filter rule applies, at the point the constant
    /// enters the plan: an enum column stores `GSA` and its values render as
    /// `eul:GSA`, so neither spelling is a condition on the column.
    #[test]
    fn a_constant_the_enum_column_never_spells_stays_a_match() {
        let schema = test_schema_view();
        let rules: [&dyn Rule; 3] = [
            &FoldMatchesIntoScan::new(&schema),
            &ConstantObjectBecomesFilter::new(&schema),
            &PushComparisonFilter::new(&schema),
        ];
        for query in [
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:kind \"GSA\" }",
            "PREFIX eul: <http://ontorail.org/src/Eulynx/> \
             SELECT ?s WHERE { ?s a asset360:Signal ; asset360:kind eul:GSA }",
        ] {
            let mut plan = plan_of(query);
            refine(&mut plan, &rules).expect("every invariant holds");
            assert_eq!(plan.find("match").len(), 1, "{query}\n{plan}");
            assert!(plan.find("filter").is_empty(), "{query}\n{plan}");
        }
    }

    /// A star with no scan has no column to constrain, so the constant stays
    /// where the query put it. The rule needs the class, not just the
    /// predicate: which slot a predicate reads, and whether it holds one value,
    /// is a fact about the class.
    #[test]
    fn a_constant_object_without_a_scan_stays_a_match() {
        let schema = test_schema_view();
        let mut plan = plan_of("SELECT ?s WHERE { ?s asset360:name \"BX517\" }");
        let log = refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &ConstantObjectBecomesFilter::new(&schema),
            ],
        )
        .expect("every invariant holds");

        assert!(log.applied.is_empty(), "{plan}");
        assert_eq!(plan.find("match").len(), 1, "{plan}");
    }

    /// Every `Sql` node as (kind, description, the obligations it claims).
    ///
    /// The unit two plans are compared in. Node *indices* cannot be: two
    /// orderings of the same filters reach the same pushed work at different
    /// positions, and the obligation a node claims has a different id in each
    /// -- so the claim is compared by what it says.
    fn pushed(plan: &Plan) -> Vec<(String, String, Vec<String>)> {
        let mut out: Vec<(String, String, Vec<String>)> = plan
            .nodes
            .iter()
            .filter(|node| node.executor == Executor::Sql)
            .map(|node| {
                (
                    node.op.kind().to_owned(),
                    node.op.describe(),
                    node.discharges
                        .iter()
                        .map(|id| plan.obligations[*id].to_string())
                        .collect(),
                )
            })
            .collect();
        out.sort();
        out
    }

    /// What the rule is for: a comparison over a slot an `Sql` scan binds
    /// becomes `Sql`, keeping its own obligation, and the variable it compared
    /// is a slot afterwards -- a column is what SQL can read.
    #[test]
    fn a_comparison_over_a_scanned_slot_is_pushed() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             FILTER(?nm > \"A\") }",
        );
        let log = refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &PushComparisonFilter::new(&schema),
            ],
        )
        .expect("both rules preserve every invariant");
        assert_eq!(
            log.applied,
            vec!["fold_matches_into_scan", "push_comparison_filter"],
            "{plan}"
        );

        let filter = plan.find("filter")[0];
        assert_eq!(plan.nodes[filter].executor, Executor::Sql, "{plan}");
        assert_eq!(
            plan.nodes[filter].op.describe(),
            "(?s.name > \"A\")",
            "the variable is rewritten into the slot that binds it:\n{plan}"
        );
        assert_eq!(
            plan.nodes[filter].discharges.len(),
            1,
            "the filter claims its own obligation, and only its own:\n{plan}"
        );
        assert_eq!(
            plan.nodes.last().unwrap().executor,
            Executor::Engine,
            "the projection above it stays with the engine:\n{plan}"
        );
        println!("{plan}");
    }

    /// The requirement 28d does not spell out. Obligations are per top-level
    /// conjunct and the naive builder chains one `Filter` per conjunct, first
    /// nearest the input -- so in one spelling the pushable comparison sits on
    /// the scan and in the other it sits above an unpushable regex, where the
    /// frontier-is-a-cut invariant forbids pushing it.
    ///
    /// The two spellings are the same query, so they must reach the same
    /// pushed work. Filters commute -- row-preserving, binding nothing, and an
    /// expression that errors simply does not select the solution -- so the
    /// rule sinks the pushable conjunct below the unpushable one.
    #[test]
    fn the_order_two_filters_are_written_in_does_not_decide_the_plan() {
        let schema = test_schema_view();
        let rules: [&dyn Rule; 2] = [
            &FoldMatchesIntoScan::new(&schema),
            &PushComparisonFilter::new(&schema),
        ];
        let refined = |query: &str| {
            let mut plan = plan_of(query);
            refine(&mut plan, &rules).expect("every invariant holds");
            plan
        };

        let comparison_first = refined(
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             FILTER(?nm > \"A\") FILTER(REGEX(?nm, \"^A\")) }",
        );
        let regex_first = refined(
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             FILTER(REGEX(?nm, \"^A\")) FILTER(?nm > \"A\") }",
        );

        assert_eq!(
            pushed(&comparison_first),
            pushed(&regex_first),
            "the same query pushed differently:\n{comparison_first}\n{regex_first}"
        );
        // ...and it is the comparison that pushed, in both. A test that only
        // compared the two would pass if neither pushed anything.
        assert!(
            pushed(&regex_first)
                .iter()
                .any(|(kind, text, _)| kind == "filter" && text == "(?s.name > \"A\")"),
            "{regex_first}"
        );
        assert_eq!(
            pushed(&regex_first).len(),
            2,
            "the scan and the comparison, and nothing else:\n{regex_first}"
        );

        // The sunk filter reads the scan, and the regex reads the sunk filter:
        // the chain kept its order above the node that moved.
        let sunk = *regex_first
            .find("filter")
            .iter()
            .find(|id| regex_first.nodes[**id].executor == Executor::Sql)
            .expect("the comparison pushed");
        let scan = regex_first.find("scan")[0];
        assert!(regex_first.feeds(scan, sunk), "{regex_first}");
        let regex = *regex_first
            .find("filter")
            .iter()
            .find(|id| regex_first.nodes[**id].executor == Executor::Engine)
            .expect("the regex did not push");
        assert!(
            regex_first.feeds(sunk, regex),
            "the regex still runs, above the comparison:\n{regex_first}"
        );
        println!("{regex_first}");
    }

    /// A filter the rule declines stays `Engine`, and says nothing about its
    /// neighbours: the scan below it is still pushed and the grouping above it
    /// is still the engine's, which is the whole point of a frontier that is
    /// local rather than a verdict for the query.
    #[test]
    fn an_unpushable_filter_leaves_its_neighbours_alone() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?kind (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm ; asset360:trafficKinds ?kind . \
             FILTER(REGEX(?nm, \"^A\")) } GROUP BY ?kind",
        );
        refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &PushComparisonFilter::new(&schema),
            ],
        )
        .expect("every invariant holds");

        let filter = plan.find("filter")[0];
        assert_eq!(plan.nodes[filter].executor, Executor::Engine, "{plan}");
        assert_eq!(
            plan.nodes[filter].op.describe(),
            "REGEX(?nm, \"^A\")",
            "an engine node keeps the expression the query wrote:\n{plan}"
        );
        assert_eq!(
            plan.nodes[plan.find("scan")[0]].executor,
            Executor::Sql,
            "{plan}"
        );
        assert_eq!(
            plan.nodes[plan.find("unnest")[0]].executor,
            Executor::Sql,
            "{plan}"
        );
        assert_eq!(
            plan.nodes[plan.find("group")[0]].executor,
            Executor::Engine,
            "the grouping sits above an engine filter, so it cannot be SQL's \
             -- and the plan names the node that blocked it:\n{plan}"
        );
    }

    /// A comparison between two slots is a column against a column, which
    /// `to_sql` declines: the condition vocabulary is (column, value).
    #[test]
    fn a_comparison_between_two_slots_is_not_pushed() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm ; \
             asset360:locatedOnTrack ?t . ?t a asset360:Track ; \
             asset360:hasName ?tn . FILTER(?nm = ?tn) }",
        );
        refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &PushComparisonFilter::new(&schema),
            ],
        )
        .expect("every invariant holds");

        let filter = plan.find("filter")[0];
        assert_eq!(plan.nodes[filter].executor, Executor::Engine, "{plan}");
    }

    /// A variable bound by a multivalued slot stands for one *element* -- the
    /// one the unnest below bound -- so the condition is on the element and
    /// selects rows, which is what SPARQL means: one solution per matching
    /// value, not every value of a matching record.
    ///
    /// This declined until the plan could say which of the three values at
    /// `(?s, [trafficKinds])` a condition meant. The unnest now names the
    /// variable it binds, so `[each]` has something to refer to.
    #[test]
    fn a_comparison_on_a_multivalued_slot_is_pushed_as_the_element() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?k WHERE { ?s a asset360:Signal ; asset360:trafficKinds ?k . \
             FILTER(?k = \"m\") }",
        );
        refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &PushComparisonFilter::new(&schema),
            ],
        )
        .expect("every invariant holds");

        let filter = plan.find("filter")[0];
        assert_eq!(plan.nodes[filter].executor, Executor::Sql, "{plan}");
        assert_eq!(
            plan.nodes[filter].op.describe(),
            "(?s.trafficKinds[each] = \"m\")",
            "the condition is on the element, not on the array:\n{plan}"
        );
        // ...and the element it names is the one the unnest below bound.
        let unnest = plan.find("unnest")[0];
        assert_eq!(
            plan.nodes[unnest].op.describe(),
            "?s.trafficKinds → ?k",
            "{plan}"
        );
        assert!(plan.feeds(unnest, filter), "{plan}");
        println!("{plan}");
    }

    /// A constant object on a multivalued slot is the other reading: it binds
    /// nothing, so a record whose array carries the value answers once however
    /// many values it holds. That is a containment test -- what the star
    /// decomposition's renderer already performs -- and there is no unnest,
    /// because there is no fan-out to restore.
    #[test]
    fn a_constant_on_a_multivalued_slot_is_pushed_as_containment() {
        let schema = test_schema_view();
        let mut plan =
            plan_of("SELECT ?s WHERE { ?s a asset360:Signal ; asset360:trafficKinds \"m\" }");
        refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &ConstantObjectBecomesFilter::new(&schema),
                &PushComparisonFilter::new(&schema),
            ],
        )
        .expect("every invariant holds");

        assert!(plan.find("match").is_empty(), "{plan}");
        let filter = plan.find("filter")[0];
        assert_eq!(
            plan.nodes[filter].op.describe(),
            "(?s.trafficKinds[any] = \"m\")",
            "{plan}"
        );
        assert_eq!(plan.nodes[filter].executor, Executor::Sql, "{plan}");
        assert!(
            plan.find("unnest").is_empty(),
            "a constant read does not fan out, so nothing has to restore it:\n{plan}"
        );
        println!("{plan}");
    }

    /// Both readings of one slot in one query, which is the case that shows
    /// they are different questions rather than two spellings of one.
    ///
    /// `:trafficKinds "m" ; :trafficKinds ?k` asks for records carrying "m",
    /// once per value of `?k`. So the constant is a containment test on the
    /// record and the variable is an element the unnest bound -- and the star
    /// decomposition refuses this shape outright
    /// (`Inexact::ConstantAndVariableOnSlot`) because one `Star` cannot say
    /// both about one slot.
    #[test]
    fn one_multivalued_slot_can_carry_both_readings() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?k WHERE { ?s a asset360:Signal ; asset360:trafficKinds \"m\" ; \
             asset360:trafficKinds ?k }",
        );
        let rules = tier_one_rules(&schema);
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        refine(&mut plan, &borrowed).expect("every invariant holds");

        assert_eq!(plan.find("unnest").len(), 1, "{plan}");
        assert_eq!(
            plan.find("filter")
                .into_iter()
                .map(|id| plan.nodes[id].op.describe())
                .collect::<Vec<_>>(),
            vec!["(?s.trafficKinds[any] = \"m\")".to_owned()],
            "{plan}"
        );
        assert!(plan.find("match").is_empty(), "{plan}");
        println!("{plan}");
    }

    /// A pushed condition compares stored text, so it asks the query's
    /// question only when the constant is the term the column's values render
    /// as. Where it is not, the condition selects *nothing* -- and the engine
    /// leg then re-runs the query over no instances, which is a wrong answer
    /// and not a narrowing.
    #[test]
    fn a_constant_the_column_never_spells_is_not_pushed() {
        let schema = test_schema_view();
        let rules: [&dyn Rule; 2] = [
            &FoldMatchesIntoScan::new(&schema),
            &PushComparisonFilter::new(&schema),
        ];
        for (query, why) in [
            (
                // `GSA` carries a `meaning`, so records storing it render as
                // `eul:GSA` and no record answers the plain literal.
                "SELECT ?k WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
                 FILTER(?k = \"GSA\") }",
                "an enum code is not the term it renders as",
            ),
            (
                // The other direction: the IRI is the term, and the column
                // stores the code. Pushing it needs a translation backwards,
                // which is a rule of its own rather than a rendering.
                "PREFIX eul: <http://ontorail.org/src/Eulynx/> \
                 SELECT ?k WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
                 FILTER(?k = eul:GSA) }",
                "an enum column is not compared, it is translated",
            ),
            (
                "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(?nm = \"BX\"@en) }",
                "a tagged literal is a different term from the plain one",
            ),
            (
                "SELECT ?len WHERE { ?s a asset360:Signal ; asset360:length ?len . \
                 FILTER(?len = \"003\"^^<http://www.w3.org/2001/XMLSchema#integer>) }",
                "the stored text does not spell three as 003",
            ),
        ] {
            let mut plan = plan_of(query);
            refine(&mut plan, &rules).expect("every invariant holds");
            assert_eq!(
                plan.nodes[plan.find("filter")[0]].executor,
                Executor::Engine,
                "{why}: {query}\n{plan}"
            );
        }
    }

    /// The canonical constant on the same column does push, so the test above
    /// is about the term and not about the operator.
    #[test]
    fn the_canonical_form_of_a_number_is_pushed() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?len WHERE { ?s a asset360:Signal ; asset360:length ?len . \
             FILTER(?len >= 10) }",
        );
        refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &PushComparisonFilter::new(&schema),
            ],
        )
        .expect("every invariant holds");

        assert_eq!(
            plan.nodes[plan.find("filter")[0]].executor,
            Executor::Sql,
            "{plan}"
        );
    }

    /// A filter over an engine node cannot be `Sql` however well it renders,
    /// and the walk that looks for a landing site stops at anything but a
    /// privately-consumed filter. Two shapes, two reasons.
    #[test]
    fn a_filter_with_no_sql_node_below_it_does_not_push() {
        let schema = test_schema_view();
        let rules: [&dyn Rule; 2] = [
            &FoldMatchesIntoScan::new(&schema),
            &PushComparisonFilter::new(&schema),
        ];
        // A left join is not a filter, so the walk stops there -- which is the
        // answer that keeps the rows an `OPTIONAL` exists to preserve: `?nm`
        // is unbound where the optional side did not match, and a pushed
        // comparison drops exactly those.
        let mut over_optional = plan_of(
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { ?s asset360:name ?nm } FILTER(?k = \"KSS\") }",
        );
        refine(&mut over_optional, &rules).expect("every invariant holds");
        assert_eq!(
            over_optional.nodes[over_optional.find("filter")[0]].executor,
            Executor::Engine,
            "{over_optional}"
        );

        // A `BIND` is not a filter either, and here it is load-bearing: `?d`
        // is bound by the node below, so sinking past it would compare a
        // variable nothing has bound yet.
        let mut over_bind = plan_of(
            "SELECT ?d WHERE { ?s a asset360:Signal ; asset360:length ?len . \
             BIND(?len * 2 AS ?d) FILTER(?d > 3) }",
        );
        refine(&mut over_bind, &rules).expect("every invariant holds");
        assert_eq!(
            over_bind.nodes[over_bind.find("filter")[0]].executor,
            Executor::Engine,
            "{over_bind}"
        );
    }

    /// Three pushable conjuncts, one unpushable, in the worst order: every
    /// pushable one has to sink past the regex, and the plan has to come out
    /// with all three below it.
    #[test]
    fn several_conjuncts_sink_past_one_regex() {
        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?nm ?len WHERE { ?s a asset360:Signal ; asset360:name ?nm ; \
             asset360:length ?len . FILTER(REGEX(?nm, \"^A\")) FILTER(?nm > \"A\") \
             FILTER(?len >= 10) FILTER(?len < 100) }",
        );
        refine(
            &mut plan,
            &[
                &FoldMatchesIntoScan::new(&schema),
                &PushComparisonFilter::new(&schema),
            ],
        )
        .expect("every invariant holds");

        let sql: Vec<NodeId> = plan
            .find("filter")
            .into_iter()
            .filter(|id| plan.nodes[*id].executor == Executor::Sql)
            .collect();
        assert_eq!(sql.len(), 3, "{plan}");
        let regex = *plan
            .find("filter")
            .iter()
            .find(|id| plan.nodes[**id].executor == Executor::Engine)
            .expect("the regex stays");
        for pushed in sql {
            assert!(
                plan.feeds(pushed, regex),
                "every pushed conjunct is below the one that did not push:\n{plan}"
            );
        }
        println!("{plan}");
    }

    /// The queries the tier-one rules are asked about as a set, rather than
    /// one shape per rule. Every cross-rule test below runs all of them.
    const CORPUS: &[&str] = &[
        "SELECT ?s WHERE { ?s a asset360:Signal }",
        "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
         FILTER(?nm > \"A\") }",
        // The worked example of 28d, on the fixture's classes.
        "SELECT ?kind (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
         asset360:name ?nm ; asset360:trafficKinds ?kind . FILTER(?nm > \"A\") \
         FILTER(REGEX(?nm, \"^A\")) } GROUP BY ?kind ORDER BY DESC(?n) LIMIT 10",
        // ...and the same query with its filters the other way round.
        "SELECT ?kind (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
         asset360:name ?nm ; asset360:trafficKinds ?kind . \
         FILTER(REGEX(?nm, \"^A\")) FILTER(?nm > \"A\") } GROUP BY ?kind \
         ORDER BY DESC(?n) LIMIT 10",
        "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name \"BX517\" ; \
         asset360:length 3 }",
        "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
         ?t a asset360:Track ; asset360:hasName ?tn }",
        "SELECT ?tn ?ln WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
         ?t a asset360:Track ; asset360:hasName ?tn ; asset360:belongsToLine ?l . \
         ?l a asset360:Line ; asset360:hasName ?ln . FILTER(?tn > \"A\") }",
        "SELECT ?s ?nm WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
         OPTIONAL { ?s asset360:name ?nm . FILTER(?nm > \"A\") } }",
        "SELECT ?s ?k WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
         OPTIONAL { ?s asset360:trafficKinds ?k } }",
        "SELECT ?d WHERE { ?s a asset360:Signal ; asset360:length ?len . \
         BIND(?len * 2 AS ?d) FILTER(?d > 3) }",
        "SELECT DISTINCT ?k WHERE { ?s a asset360:Signal ; asset360:trafficKinds ?k . \
         FILTER(?k = \"m\") }",
        "SELECT ?s WHERE { { SELECT ?s WHERE { ?s a asset360:Signal ; \
         asset360:name ?nm } LIMIT 3 } }",
        "SELECT ?s WHERE { VALUES ?s { \"a\" \"b\" } }",
        "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:asset360_uri \"u\" }",
        "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
         VALUES ?nm { \"a\" \"b\" } }",
        "SELECT ?kind (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
         asset360:name ?nm ; asset360:trafficKinds ?kind . FILTER(?nm > \"A\") } \
         GROUP BY ?kind ORDER BY DESC(?n) LIMIT 10",
        "SELECT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
         asset360:name ?nm } GROUP BY ?nm",
        "SELECT ?lon WHERE { ?s a asset360:Signal ; asset360:location ?loc . \
         ?loc asset360:longitude ?lon . FILTER(?lon > 3) }",
        "SELECT ?v WHERE { ?s a asset360:Signal ; asset360:location ?loc . \
         ?loc asset360:detail ?d . ?d asset360:value \"x\" }",
        "ASK WHERE { ?s a asset360:Signal ; asset360:name \"BX517\" }",
        "CONSTRUCT { ?s asset360:name ?nm } WHERE { ?s a asset360:Signal ; \
         asset360:name ?nm . FILTER(?nm > \"A\") }",
        "DESCRIBE ?s WHERE { ?s a asset360:Signal ; asset360:length ?len . \
         FILTER(?len >= 10) }",
    ];

    /// The whole rule set, applied to fixpoint in a given order.
    ///
    /// Takes a plan rather than a query because two *parses* of one query are
    /// not the same plan: spargebra names the internal variable of
    /// `(COUNT(*) AS ?n)` freshly each time, so comparing two plans built from
    /// two parses compares those names as well.
    fn refine_naive(naive: &Plan, schema: &SchemaView, reverse: bool) -> Plan {
        let mut rules = tier_one_rules(schema);
        if reverse {
            rules.reverse();
        }
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        let mut plan = naive.clone();
        refine(&mut plan, &borrowed).unwrap_or_else(|failure| panic!("{failure}"));
        plan
    }

    fn refined(query: &str, schema: &SchemaView, reverse: bool) -> Plan {
        refine_naive(&plan_of(query), schema, reverse)
    }

    /// What today's planner's SQL pass claims, as obligation *text*.
    ///
    /// Text and not ids: the two planners enumerate the same obligations, but
    /// a comparison by id would pass for the wrong reason if either ever
    /// reordered them.
    ///
    /// `None` when today's planner refuses the query outright, which is not a
    /// parity failure -- there is nothing to be at parity with.
    fn claimed_by_sql_today(query: &str, schema: &SchemaView) -> Option<Vec<String>> {
        let plan = crate::sparql_plan::plan_query(&format!("{PREFIX}{query}"), schema).ok()?;
        Some(
            plan.passes
                .iter()
                .filter(|pass| matches!(pass.kind, crate::sparql_plan::PassKind::Sql(_)))
                .flat_map(|pass| pass.discharges.iter())
                .filter(|id| tier_one_shaped(&plan.obligations[**id]))
                .map(|id| plan.obligations[*id].to_string())
                .collect(),
        )
    }

    /// Whether an obligation is one tier one could take care of at all.
    ///
    /// A grouping, an ordering, a slice and a `DISTINCT` all *collapse or
    /// reorder rows*, and every rule here is non-collapsing by construction --
    /// the engine leg re-runs the query, which is what makes a partial push
    /// correct, and it cannot re-run one over rows that are already
    /// aggregates. So today's eligible route claims them in SQL and the
    /// refined plan does not, and that difference is tier two rather than a
    /// regression. Comparing them would only assert that tier two is unbuilt.
    fn tier_one_shaped(obligation: &Obligation) -> bool {
        matches!(
            obligation,
            Obligation::Type { .. }
                | Obligation::Triple { .. }
                | Obligation::Filter { .. }
                | Obligation::Values { .. }
        )
    }

    /// The obligations the refined plan's `Sql` nodes claim, as text.
    fn claimed_by_sql_refined(plan: &Plan) -> Vec<String> {
        plan.nodes
            .iter()
            .filter(|node| node.executor == Executor::Sql)
            .flat_map(|node| node.discharges.iter())
            .filter(|id| tier_one_shaped(&plan.obligations[**id]))
            .map(|id| plan.obligations[*id].to_string())
            .collect()
    }

    /// The value conditions today's star decomposition pushes, as
    /// `?star.path <condition>`.
    ///
    /// `identifier_values` is rendered against the class's identifier slot,
    /// which is where it came from: the star keeps it apart from `filters`
    /// because it renders against the indexed `asset360_uri` column rather
    /// than the JSONB payload, and that is a *rendering* difference over the
    /// same constraint.
    fn conditions_pushed_today(query: &str, schema: &SchemaView) -> Vec<String> {
        let Ok(scoped) = crate::sparql_scoper::sparql_scope(&format!("{PREFIX}{query}"), schema)
        else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for star in scoped.root.all_stars() {
            let mut slots: Vec<(&String, &Vec<crate::sparql_scoper::FilterCondition>)> =
                star.filters.iter().collect();
            slots.sort_by(|a, b| a.0.cmp(b.0));
            for (slot, conditions) in slots {
                for condition in conditions {
                    out.push(format!("?{}.{slot} {condition}", star.variable));
                }
            }
            for path in &star.path_filters {
                for condition in &path.conditions {
                    out.push(format!(
                        "?{}.{} {condition}",
                        star.variable,
                        path.slot_path.join(".")
                    ));
                }
            }
            if !star.identifier_values.is_empty() {
                let identifier = match schema.get_class_by_uri(&star.class_uri).ok().flatten() {
                    Some(class) => class
                        .identifier_slot()
                        .map(|slot| slot.name.clone())
                        .unwrap_or_default(),
                    None => String::new(),
                };
                let condition = if star.identifier_values.len() == 1 {
                    crate::sparql_scoper::FilterCondition::Eq(star.identifier_values[0].clone())
                } else {
                    crate::sparql_scoper::FilterCondition::In(star.identifier_values.clone())
                };
                out.push(format!("?{}.{identifier} {condition}", star.variable));
            }
        }
        out.sort();
        out
    }

    /// The same, of a refined plan: every condition its `Sql` filters render.
    ///
    /// The *reading* is dropped from the comparison, deliberately. On a
    /// multivalued slot today's renderer performs a containment test over the
    /// record's array while the refined plan names the element its unnest
    /// bound, which selects rows rather than records -- the stricter and, for
    /// SPARQL, the correct one. Comparing the reading would report that
    /// difference as a parity failure when it is an improvement; the reading
    /// itself is asserted by
    /// `a_comparison_on_a_multivalued_slot_is_pushed_as_the_element`.
    fn conditions_pushed_refined(plan: &Plan, schema: &SchemaView) -> Vec<String> {
        let classes: HashMap<String, String> = plan
            .nodes
            .iter()
            .filter(|node| node.executor == Executor::Sql)
            .filter_map(|node| match &node.op {
                PlanOp::Scan {
                    star_var,
                    class_uri,
                    ..
                } => Some((star_var.clone(), class_uri.clone())),
                _ => None,
            })
            .collect();
        let mut out = Vec::new();
        for node in &plan.nodes {
            if node.executor != Executor::Sql {
                continue;
            }
            let PlanOp::Filter { condition, .. } = &node.op else {
                continue;
            };
            // Every pushed filter must render: the rule pushed it *because*
            // `to_sql` accepted it, so a `None` here would mean the plan says
            // SQL applies a condition SQL cannot state.
            let rendered = condition
                .to_sql(schema, &classes)
                .unwrap_or_else(|| panic!("a pushed filter that does not render:\n{plan}"));
            for condition in rendered {
                out.push(format!(
                    "?{}.{} {}",
                    condition.star_var,
                    condition.slot_path.join("."),
                    condition.condition
                ));
            }
        }
        out.sort();
        out
    }

    /// **The gate on stage 3.** Whatever today's SQL pass claims, the refined
    /// plan's `Sql` nodes claim too -- and whatever conditions today's star
    /// decomposition pushes, the refined plan pushes too.
    ///
    /// Stated as containment rather than equality, in that direction: the
    /// refined plan legitimately claims *more* (a `VALUES` over a bound
    /// variable, a filter today's ledger applies without claiming). Switching
    /// the endpoint onto a planner that pushed *less* would regress answers or
    /// lose pushdown, which is what this refuses to let happen quietly.
    ///
    /// Where parity is not reached, the gap is named below rather than
    /// smoothed over: an entry in `KNOWN_GAPS` fails the test if it stops
    /// being a gap, so the list cannot rot in either direction.
    #[test]
    fn the_refined_plan_claims_everything_todays_sql_pass_claims() {
        let schema = test_schema_view();
        for query in CORPUS {
            let Some(today) = claimed_by_sql_today(query, &schema) else {
                continue;
            };
            let plan = refined(query, &schema, false);
            let refined_claims = claimed_by_sql_refined(&plan);
            let missing: Vec<&String> = today
                .iter()
                .filter(|claim| !refined_claims.contains(claim))
                .collect();
            let expected: Vec<&str> = KNOWN_GAPS
                .iter()
                .filter(|(gap_query, _)| gap_query == query)
                .map(|(_, claim)| *claim)
                .collect();
            assert_eq!(
                missing
                    .iter()
                    .map(|claim| claim.as_str())
                    .collect::<Vec<_>>(),
                expected,
                "claim parity for {query}\n{plan}"
            );

            let today_conditions = conditions_pushed_today(query, &schema);
            let refined_conditions = conditions_pushed_refined(&plan, &schema);
            for condition in &today_conditions {
                assert!(
                    refined_conditions.contains(condition),
                    "today pushes {condition} and the refined plan does not, for \
                     {query}\n{plan}\nrefined pushes {refined_conditions:?}"
                );
            }
        }
    }

    /// Every claim today's SQL pass makes that the refined plan does not, with
    /// why. One entry, and it is not a pushdown difference: it is a
    /// disagreement about what claiming a triple *means*.
    ///
    /// A read inside an `OPTIONAL` reaches today's star as an
    /// `optional_fields` entry: the prefetch delivers the column without an
    /// existence check, and the pass claims the triple because the data
    /// reaches oxigraph. The refined plan delivers the same column and does
    /// not claim it, so the difference is in the ledger and not in the SQL.
    ///
    /// **Two entries left, where there used to be one for every optional
    /// read.** [`AbsorbOptionalRead`] closes the general case by giving the
    /// delivered column its variable, at which point the scan answers the read
    /// and the claim is its own -- the argument for keeping it unclaimed was
    /// right about a column nothing binds, and the way to settle it was to
    /// make the node answer rather than to keep arguing about the ledger.
    ///
    /// What remains is the two shapes absorption declines: a condition lifted
    /// into the left join (which decides whether the *value* binds, not
    /// whether the row survives) and a multivalued optional read (whose
    /// fan-out would have to live inside the optional side). Both keep the
    /// `match`, and the `match` keeps the claim.
    const KNOWN_GAPS: &[(&str, &str)] = &[
        (
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { ?s asset360:name ?nm . FILTER(?nm > \"A\") } }",
            "triple    ?s asset360:name ?nm",
        ),
        (
            // The same gap, listed per query rather than folded in: a list of
            // queries is what says whether the gap is one thing or several.
            "SELECT ?s ?k WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             OPTIONAL { ?s asset360:trafficKinds ?k } }",
            "triple    ?s asset360:trafficKinds ?k",
        ),
    ];

    /// The first collapsing rule: the grouping and the projection above it are
    /// SQL's, the alias the query wrote is on the measure, and the plan is
    /// answered without the engine.
    #[test]
    fn a_scalar_key_count_grouping_is_pushed_whole() {
        let schema = test_schema_view();
        let plan = refined(
            "SELECT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm } GROUP BY ?nm",
            &schema,
            false,
        );

        assert!(
            plan.nodes.iter().all(|node| node.executor == Executor::Sql),
            "a grouping pushes whole or not at all:\n{plan}"
        );
        assert!(
            plan.find("bind").is_empty(),
            "the alias is a fact about the grouping, not a step after it:\n{plan}"
        );
        let group = plan.find("group")[0];
        assert_eq!(
            plan.nodes[group].op.describe(),
            "keys=[?nm] measures=[?n ← COUNT(*)]",
            "the measure carries the name the query gave it, not spargebra's hash:\n{plan}"
        );
        // The grouping keeps its own claims: it is what takes care of them
        // now, and the ledger still balances.
        assert_eq!(
            plan.nodes[group]
                .discharges
                .iter()
                .map(|id| plan.obligations[*id].to_string())
                .collect::<Vec<_>>(),
            vec![
                "group     GROUP BY ?nm".to_owned(),
                "aggregate COUNT(*) AS ?n".to_owned()
            ],
            "{plan}"
        );
        plan.check()
            .unwrap_or_else(|defect| panic!("{defect}\n{plan}"));
        println!("{plan}");
    }

    /// Grouping on a multivalued slot groups its *values*, and the count is
    /// one per solution -- which is one per value, not one per record.
    ///
    /// The fan-out below has already turned each value into its own row, so
    /// the key is the element the unnest bound. Reading the array instead
    /// would make one group per record's whole list, which is a different
    /// question with the same shape.
    #[test]
    fn a_multivalued_key_groups_its_elements() {
        let schema = test_schema_view();
        let plan = refined(
            "SELECT ?k (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:trafficKinds ?k } GROUP BY ?k",
            &schema,
            false,
        );

        assert!(
            plan.nodes.iter().all(|node| node.executor == Executor::Sql),
            "{plan}"
        );
        assert_eq!(plan.find("unnest").len(), 1, "{plan}");
        // The fan-out is below the grouping, which is what makes the count one
        // per value -- and what the fifth invariant now refuses to let drift.
        let unnest = plan.find("unnest")[0];
        let group = plan.find("group")[0];
        assert!(plan.feeds(unnest, group), "{plan}");
        plan.check()
            .unwrap_or_else(|defect| panic!("{defect}\n{plan}"));
        println!("{plan}");
    }

    /// Every shape this rule declines, and the wrong answer each would be.
    ///
    /// The list is long on purpose: a collapsing rule takes the answer rather
    /// than narrowing what the engine will decide, so the preconditions are
    /// the whole of its correctness.
    #[test]
    fn what_the_grouping_rule_declines_and_why() {
        let schema = test_schema_view();
        let grouped = |query: &str| -> bool {
            let plan = refined(query, &schema, false);
            plan.find("group")
                .iter()
                .any(|id| plan.nodes[*id].executor == Executor::Sql)
        };

        // An engine node below: the grouping would group rows the regex has
        // not filtered yet. The frontier invariant would refuse the plan; the
        // rule declines before building it.
        assert!(
            !grouped(
                "SELECT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
                 asset360:name ?nm . FILTER(REGEX(?nm, \"^A\")) } GROUP BY ?nm"
            ),
            "a grouping over an engine filter"
        );

        // `COUNT(?v)` must not count solutions where `?v` is unbound, and
        // `COUNT(DISTINCT ?v)` counts values rather than solutions. Both are
        // real shapes with their own reasoning.
        assert!(
            !grouped(
                "SELECT ?nm (COUNT(?len) AS ?n) WHERE { ?s a asset360:Signal ; \
                 asset360:name ?nm ; asset360:length ?len } GROUP BY ?nm"
            ),
            "COUNT of a variable"
        );
        assert!(
            !grouped(
                "SELECT ?nm (COUNT(DISTINCT ?len) AS ?n) WHERE { ?s a asset360:Signal ; \
                 asset360:name ?nm ; asset360:length ?len } GROUP BY ?nm"
            ),
            "COUNT DISTINCT"
        );

        // More collapsing work above. Pushing the grouping under it would
        // leave that work to an engine that cannot do it -- the partial
        // collapse this design refuses to make possible.
        for (query, why) in [
            (
                "SELECT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
                 asset360:name ?nm } GROUP BY ?nm ORDER BY DESC(?n)",
                "an ordering above",
            ),
            (
                "SELECT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
                 asset360:name ?nm } GROUP BY ?nm LIMIT 3",
                "a slice above",
            ),
            (
                "SELECT DISTINCT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
                 asset360:name ?nm } GROUP BY ?nm",
                "a DISTINCT above",
            ),
        ] {
            assert!(!grouped(query), "{why}");
        }

        // Two keys, and a bare aggregate with none: one key is what the
        // renderer's column convention is written for here.
        assert!(
            !grouped(
                "SELECT ?nm ?k (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
                 asset360:name ?nm ; asset360:kind ?k } GROUP BY ?nm ?k"
            ),
            "two keys"
        );
        assert!(
            !grouped("SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal }"),
            "a bare aggregate groups nothing"
        );

        // A measure over a value the plan cannot read as a column.
        assert!(
            !grouped(
                "SELECT ?loc (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
                 asset360:location ?loc } GROUP BY ?loc"
            ),
            "an inlined structure is a blank node, not a column"
        );
    }

    /// **Invariant, not argument.** A grouping over an engine node is refused
    /// by the frontier check, so a rule that pushed one anyway could not hand
    /// the plan back.
    ///
    /// The rule declines this shape, which is why the test drives a bad rule:
    /// the guarantee has to hold for the rule nobody has written yet.
    #[test]
    fn a_grouping_over_an_engine_node_is_refused_by_the_frontier() {
        struct PushesTheGroupingAnyway;
        impl Rule for PushesTheGroupingAnyway {
            fn name(&self) -> &'static str {
                "pushes_the_grouping_anyway"
            }
            fn apply(&self, plan: &mut Plan) -> bool {
                for id in plan.find("group") {
                    plan.nodes[id].executor = Executor::Sql;
                }
                // Reports no change, so the driver's result check is what has
                // to catch it.
                false
            }
        }

        let schema = test_schema_view();
        let mut plan = plan_of(
            "SELECT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm . FILTER(REGEX(?nm, \"^A\")) } GROUP BY ?nm",
        );
        let rules = tier_one_rules(&schema);
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        refine(&mut plan, &borrowed).expect("the regex keeps the grouping with the engine");
        assert_eq!(
            plan.nodes[plan.find("group")[0]].executor,
            Executor::Engine,
            "{plan}"
        );

        let failure = refine(&mut plan, &[&PushesTheGroupingAnyway])
            .expect_err("a grouping over an engine filter must not be handed back");
        assert!(
            matches!(
                failure.defect,
                crate::sparql_refine::PlanDefect::FrontierBreach { .. }
            ),
            "{failure}"
        );
    }

    /// **The fifth invariant, finally falsifiable.** A fan-out above the
    /// grouping restores multiplicity too late: the count is one per record
    /// where the query counts one per value.
    ///
    /// Stated in stage 1 and unfalsifiable until now, because nothing
    /// collapsed rows. It needed strengthening to catch this: "there is an
    /// unnest downstream of the scan" was true of the broken plan too.
    #[test]
    fn a_fanout_above_the_grouping_is_a_defect() {
        struct MovesTheFanoutUp;
        impl Rule for MovesTheFanoutUp {
            fn name(&self) -> &'static str {
                "moves_the_fanout_up"
            }
            fn apply(&self, plan: &mut Plan) -> bool {
                let (Some(unnest), Some(group)) = (
                    plan.find("unnest").first().copied(),
                    plan.find("group").first().copied(),
                ) else {
                    return false;
                };
                if unnest > group {
                    return false;
                }
                // Lift the unnest over the grouping: the grouping now reads
                // the scan directly, and the unnest reads the grouping.
                let below = plan.nodes[unnest].op.inputs()[0];
                let mut nodes = plan.nodes.clone();
                nodes[group].op.map_inputs(|_| below);
                nodes[unnest].op.map_inputs(|_| group);
                nodes.swap(unnest, group);
                // ...and renumber, since the two exchanged positions.
                for node in nodes.iter_mut() {
                    node.op.map_inputs(|input| {
                        if input == unnest {
                            group
                        } else if input == group {
                            unnest
                        } else {
                            input
                        }
                    });
                }
                plan.nodes = nodes;
                false
            }
        }

        let schema = test_schema_view();
        // A scalar key *and* a fanned-out read: the grouping pushes, so every
        // node is `Sql` and moving the unnest leaves the frontier intact --
        // which is what makes this the fan-out defect and not another one.
        // The query also counts solutions rather than records: two traffic
        // kinds on one signal are two solutions, and that is precisely the
        // multiplicity the unnest carries.
        let mut plan = plan_of(
            "SELECT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm ; asset360:trafficKinds ?k } GROUP BY ?nm",
        );
        let rules = tier_one_rules(&schema);
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        refine(&mut plan, &borrowed).expect("the fold inserts the unnest below everything");
        assert_eq!(plan.find("unnest").len(), 1, "{plan}");
        assert!(
            plan.nodes.iter().all(|node| node.executor == Executor::Sql),
            "{plan}"
        );

        let failure = refine(&mut plan, &[&MovesTheFanoutUp])
            .expect_err("a fan-out after a collapse must not be handed back");
        assert!(
            matches!(
                failure.defect,
                crate::sparql_refine::PlanDefect::FanoutAfterCollapse { .. }
            ),
            "{failure}"
        );
    }

    /// A rule chain is only testable if its result does not depend on the
    /// order the rules were listed in. Every tier-one rule is monotone -- two
    /// of them remove a `match`, two turn an `Engine` node `Sql`, none does
    /// the reverse -- so the fixpoint is the same and only the number of
    /// rounds differs.
    #[test]
    fn the_rule_order_does_not_decide_the_fixpoint() {
        let schema = test_schema_view();
        for query in CORPUS {
            let naive = plan_of(query);
            let forwards = refine_naive(&naive, &schema, false);
            let backwards = refine_naive(&naive, &schema, true);
            assert_eq!(
                forwards.to_string(),
                backwards.to_string(),
                "the rule order decided the plan for {query}"
            );
        }
    }

    /// Refinement edits nodes, never the question. Stated as the obligation
    /// list being untouched and every one of them still claimed exactly once,
    /// because a rule could balance a ledger it had rewritten.
    #[test]
    fn refinement_never_changes_the_obligations() {
        let schema = test_schema_view();
        for query in CORPUS {
            let naive = plan_of(query);
            let refined = refined(query, &schema, false);

            assert_eq!(refined.obligations, naive.obligations, "{query}");
            assert_eq!(
                refined.obligations.len(),
                naive.obligations.len(),
                "{query}"
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

    /// Every invariant after every single application, over the whole rule set
    /// -- not only behind the driver's debug assertion, and not only for one
    /// rule at a time. A rule that breaks something has to fail at the rule.
    #[test]
    fn every_invariant_holds_after_every_application_of_every_rule() {
        let schema = test_schema_view();
        let rules = tier_one_rules(&schema);
        for query in CORPUS {
            let mut plan = plan_of(query);
            plan.check()
                .unwrap_or_else(|defect| panic!("naive: {defect} for {query}\n{plan}"));
            let mut applications = 0;
            let mut changed = true;
            while changed {
                changed = false;
                for rule in &rules {
                    if rule.apply(&mut plan) {
                        changed = true;
                        applications += 1;
                        plan.check().unwrap_or_else(|defect| {
                            panic!(
                                "{}, application {applications}: {defect} for {query}\n{plan}",
                                rule.name()
                            )
                        });
                        assert!(applications < 32, "no fixpoint for {query}\n{plan}");
                    }
                }
            }
        }
    }

    /// The worked example of 28d, and the shape the document draws: the scan
    /// and its unnest and the comparison in SQL, the regex and everything
    /// above it with the engine.
    ///
    /// The grouping is the case worth understanding. It is refused not by a
    /// verdict about the query but by the frontier-is-a-cut invariant -- it
    /// sits above an engine filter, and SQL cannot group rows the regex has
    /// not filtered yet -- and the plan names the node that blocked it.
    #[test]
    fn the_worked_example_ends_in_the_shape_28d_draws() {
        let schema = test_schema_view();
        let plan = refined(
            "SELECT ?kind (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm ; asset360:trafficKinds ?kind . FILTER(?nm > \"A\") \
             FILTER(REGEX(?nm, \"^A\")) } GROUP BY ?kind ORDER BY DESC(?n) LIMIT 10",
            &schema,
            false,
        );

        let by_kind = |kind: &str| -> Vec<(String, Executor)> {
            plan.find(kind)
                .into_iter()
                .map(|id| (plan.nodes[id].op.describe(), plan.nodes[id].executor))
                .collect()
        };
        assert_eq!(
            by_kind("scan"),
            vec![(
                "asset360:Signal as ?s, requires [name→?nm, trafficKinds→?kind[]]".to_owned(),
                Executor::Sql
            )],
            "{plan}"
        );
        assert_eq!(
            by_kind("unnest"),
            vec![("?s.trafficKinds → ?kind".to_owned(), Executor::Sql)],
            "the fan-out the fold owed, still in SQL:\n{plan}"
        );
        assert_eq!(
            by_kind("filter"),
            vec![
                ("(?s.name > \"A\")".to_owned(), Executor::Sql),
                ("REGEX(?nm, \"^A\")".to_owned(), Executor::Engine),
            ],
            "{plan}"
        );
        for kind in ["group", "sort", "slice", "bind", "project"] {
            assert!(
                by_kind(kind)
                    .iter()
                    .all(|(_, executor)| *executor == Executor::Engine),
                "{kind} is above the regex, so it is the engine's:\n{plan}"
            );
        }
        // The node that blocked the grouping, named: the group's input is the
        // regex, and it runs in the engine.
        let group = plan.find("group")[0];
        let below = plan.nodes[group].op.inputs();
        let [input] = below.as_slice() else {
            panic!("a grouping has one input:\n{plan}");
        };
        assert_eq!(plan.nodes[*input].op.kind(), "filter", "{plan}");
        assert_eq!(plan.nodes[*input].executor, Executor::Engine, "{plan}");
        assert!(plan.find("match").is_empty(), "{plan}");
        assert!(plan.find("join").is_empty(), "{plan}");
        println!("{plan}");
    }

    /// Three stars, two reference joins and a comparison, all pushed: the
    /// rules compose into one SQL statement without anything having been
    /// decided for the query as a whole.
    #[test]
    fn a_chain_of_reference_joins_pushes_whole() {
        let schema = test_schema_view();
        let plan = refined(
            "SELECT ?tn ?ln WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t a asset360:Track ; asset360:hasName ?tn ; asset360:belongsToLine ?l . \
             ?l a asset360:Line ; asset360:hasName ?ln . FILTER(?tn > \"A\") }",
            &schema,
            false,
        );

        assert_eq!(plan.find("scan").len(), 3, "{plan}");
        assert_eq!(plan.find("join").len(), 2, "{plan}");
        assert!(
            plan.nodes[..plan.nodes.len() - 1]
                .iter()
                .all(|node| node.executor == Executor::Sql),
            "everything below the projection is one statement:\n{plan}"
        );
        println!("{plan}");
    }

    /// Where 28d says refinement wins: drop the regex and the frontier moves
    /// up to the grouping's input, so everything below the first collapsing
    /// operator is one SQL statement. The grouping itself is tier two and
    /// stays with the engine -- no rule here moves a collapsing operator.
    #[test]
    fn without_the_regex_the_frontier_reaches_the_grouping() {
        let schema = test_schema_view();
        let plan = refined(
            "SELECT ?kind (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm ; asset360:trafficKinds ?kind . FILTER(?nm > \"A\") } \
             GROUP BY ?kind ORDER BY DESC(?n) LIMIT 10",
            &schema,
            false,
        );

        let group = plan.find("group")[0];
        assert!(
            plan.nodes[..group]
                .iter()
                .all(|node| node.executor == Executor::Sql),
            "everything below the grouping is SQL's:\n{plan}"
        );
        assert_eq!(plan.nodes[group].executor, Executor::Engine, "{plan}");
        println!("{plan}");
    }

    /// The fifth invariant now reads the unnest's *variable*, because a
    /// condition above it addresses the element through that name. Two rules
    /// that a plan could previously pass with, and cannot now.
    #[test]
    fn an_unnest_that_binds_the_wrong_name_is_a_defect() {
        /// Fans out the right slot under a name nothing bound. Before the
        /// unnest carried a variable this plan passed every invariant, and a
        /// `[each]` condition above it named an element no scan had read.
        struct RenamesTheElement;
        impl Rule for RenamesTheElement {
            fn name(&self) -> &'static str {
                "renames_the_element"
            }
            fn apply(&self, plan: &mut Plan) -> bool {
                for node in &mut plan.nodes {
                    if let PlanOp::Unnest { var, .. } = &mut node.op {
                        *var = "other".to_owned();
                        break;
                    }
                }
                // Reports no change, so the driver's *result* check is what
                // has to catch it -- the same shape as
                // `a_rule_that_breaks_a_plan_is_not_handed_back`, and the
                // reason the failure is a returned error rather than a panic.
                false
            }
        }

        /// Fans out a slot no scan folded as multivalued -- rows multiplied by
        /// an array no row set has.
        struct InventsAFanout;
        impl Rule for InventsAFanout {
            fn name(&self) -> &'static str {
                "invents_a_fanout"
            }
            fn apply(&self, plan: &mut Plan) -> bool {
                let below = plan.nodes.len() - 2;
                let stray = Node::sql(
                    PlanOp::Unnest {
                        input: below,
                        star_var: "s".to_owned(),
                        slot_path: vec!["name".to_owned()],
                        var: "nm".to_owned(),
                    },
                    Vec::new(),
                );
                plan.nodes.insert(below + 1, stray);
                for node in plan.nodes.iter_mut().skip(below + 2) {
                    node.op
                        .map_inputs(|input| if input > below { input + 1 } else { input });
                }
                // Reports no change, for the reason above.
                false
            }
        }

        let schema = test_schema_view();
        let query = "SELECT ?k WHERE { ?s a asset360:Signal ; asset360:name ?nm ; \
                     asset360:trafficKinds ?k }";

        let mut renamed = plan_of(query);
        refine(&mut renamed, &[&FoldMatchesIntoScan::new(&schema)]).unwrap();
        let failure = refine(&mut renamed, &[&RenamesTheElement])
            .expect_err("an unnest under the wrong name must not be handed back");
        assert!(
            matches!(
                failure.defect,
                crate::sparql_refine::PlanDefect::LostFanout { .. }
            ),
            "{failure}"
        );

        let mut invented = plan_of(query);
        refine(&mut invented, &[&FoldMatchesIntoScan::new(&schema)]).unwrap();
        let failure = refine(&mut invented, &[&InventsAFanout])
            .expect_err("a fan-out nothing folded must not be handed back");
        assert!(
            matches!(
                failure.defect,
                crate::sparql_refine::PlanDefect::StrayFanout { .. }
            ),
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
