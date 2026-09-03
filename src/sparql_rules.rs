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

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::slotview::SlotContainerMode;

use spargebra::term::{Term, TermPattern, TriplePattern};

use crate::sparql_plan::ObligationId;
use crate::sparql_refine::{
    CompareOp, Executor, Expr, Node, NodeId, Plan, PlanOp, ScanSlot, inner_join_groups,
    is_type_pattern, object_variable, predicate_iri, scan_with_fanout, subject_variable,
    type_class_iri,
};
use crate::sparql_scoper::{PushForm, literal_pushable, push_form};

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

/// One variable an `Sql` [`PlanOp::Scan`] binds to a slot of its record.
struct SlotBinding {
    star_var: String,
    slot: String,
    /// Whether the slot holds an array. Carried because a condition on one is
    /// not a condition on a column -- see [`PushComparisonFilter`].
    multivalued: bool,
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
    fn below(plan: &Plan, base: NodeId) -> Self {
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
            // `feeds` and not [`mandatorily_feeds`]: a landing site runs in
            // SQL, the frontier is a cut, so its whole subtree is `Sql` -- and
            // no rule pushes a left join, a union or a minus. Every path from a
            // scan to an `Sql` node is therefore already mandatory, and asking
            // the stronger question here would only hide a future rule that
            // pushed one of them.
            if node.executor != Executor::Sql || !plan.feeds(id, base) {
                continue;
            }
            class_of_star.insert(star_var.clone(), class_uri.clone());
            // The star variable names the *record*, not a slot of it. Its
            // pushdown is the indexed identifier column rather than a JSONB
            // path, which no `SqlCondition` can say, so it is entered as
            // ambiguous: a rule then declines `FILTER(?s = <iri>)` rather than
            // resolving `?s` through some other star's slot of the same name.
            slots.insert(star_var.clone(), None);
            for slot in scan_slots {
                let binding = SlotBinding {
                    star_var: star_var.clone(),
                    slot: slot.slot.clone(),
                    multivalued: slot.multivalued,
                };
                match slots.entry(slot.var.clone()) {
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

    /// The slot a variable reads, when exactly one single-valued slot does.
    fn slot_of(&self, var: &str) -> Option<&SlotBinding> {
        match self.slots.get(var) {
            Some(Some(binding)) if !binding.multivalued => Some(binding),
            _ => None,
        }
    }
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

/// Whether a constant the query wrote is the same RDF term the column's values
/// render as.
///
/// The reason this is asked at all: 28d argues that a pushed condition is free
/// of correctness risk because the engine re-runs the whole query, so SQL only
/// ever *narrows*. That is true of a condition that selects a superset of the
/// answer and false of one that selects nothing -- and comparing stored text
/// against a term the column never spells that way selects nothing. An enum
/// column storing `GSA` whose values render as `eul:GSA` answers a pushed
/// `= 'http://ontorail.org/src/Eulynx/GSA'` with no rows at all, and the
/// engine then re-runs the query over no instances and reports an empty answer
/// where the query has one. So the same test the star decomposition applies --
/// [`crate::sparql_scoper::push_form`] with
/// [`crate::sparql_scoper::literal_pushable`] -- gates a pushed constant here.
///
/// Enum columns decline rather than translate. Selecting the codes that render
/// as the term is a *rewrite* of the condition, and `Expr::to_sql` has no
/// schema to do it with; the rule that translates backwards is a rule of its
/// own.
fn constant_is_the_columns_term(
    schema: &SchemaView,
    class_uri: &str,
    slot: &str,
    term: &Term,
) -> bool {
    let form = push_form(schema, class_uri, slot);
    match (&form, term) {
        (PushForm::Literal { .. }, Term::Literal(literal)) => literal_pushable(literal, &form),
        (PushForm::Iri, Term::NamedNode(_)) => true,
        _ => false,
    }
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
///   row from an `Sql` scan of the same star.** A constant object inside an
///   `OPTIONAL` is consumed by a `LeftJoin`, and turning it into a filter over
///   the preserved side drops exactly the rows the join exists to keep -- the
///   loss the star decomposition records as `Inexact::ConstantInOptional`.
///   [`mandatorily_feeds`] is the other half: joining a solution that leaves
///   `?s` unbound against a match that binds it *keeps* the row, so a scan
///   reached through the optional side of a left join is not a row set this
///   constraint can be moved onto.
/// * **The slot is single-valued.** `:trafficKinds "m"` asks whether the array
///   *contains* the value, which is not what a condition naming the column
///   says. See [`PushComparisonFilter`] for why that ambiguity is declined
///   rather than guessed.
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

    /// The slot a constant-object match constrains on a scanned class, when
    /// this rule can constrain it.
    fn constrained_slot(&self, class_uri: &str, predicate: &str, term: &Term) -> Option<String> {
        let class = self.schema.get_class_by_uri(class_uri).ok().flatten()?;
        let slot = self.schema.get_slot_by_uri(predicate).ok().flatten()?;
        let on_class = class.slot(&Identifier::Name(slot.name.clone()))?;
        if on_class.determine_slot_container_mode() != SlotContainerMode::SingleValue {
            return None;
        }
        if !constant_is_the_columns_term(self.schema, class_uri, &slot.name, term) {
            return None;
        }
        Some(slot.name.clone())
    }
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
            let (Some(star), Some(predicate), Some(term)) = (
                subject_variable(pattern),
                predicate_iri(pattern),
                constant_object(pattern),
            ) else {
                continue;
            };
            let (star, predicate) = (star.to_owned(), predicate.to_owned());

            // The join that carried the match is the node that disappears, so
            // the shape is checked before the schema: without it there is no
            // edit to make even for a slot this rule could constrain.
            let above = consumers(plan, id);
            let [consumer] = above.as_slice() else {
                continue;
            };
            let PlanOp::Join { left, right, on } = &plan.nodes[*consumer].op else {
                continue;
            };
            if on.as_slice() != [star.clone()] {
                continue;
            }
            let (consumer, other) = (*consumer, if *left == id { *right } else { *left });

            let Some(class_uri) =
                plan.nodes
                    .iter()
                    .enumerate()
                    .find_map(|(scan, node)| match &node.op {
                        PlanOp::Scan {
                            star_var,
                            class_uri,
                            ..
                        } if node.executor == Executor::Sql
                            && star_var == &star
                            && mandatorily_feeds(plan, scan, other) =>
                        {
                            Some(class_uri.clone())
                        }
                        _ => None,
                    })
            else {
                continue;
            };
            let Some(slot) = self.constrained_slot(&class_uri, &predicate, &term) else {
                continue;
            };

            let condition = Expr::Compare {
                op: CompareOp::Eq,
                left: Box::new(Expr::Slot {
                    star_var: star,
                    slot_path: vec![slot],
                }),
                right: Box::new(Expr::Literal(term)),
            };
            replace_match_with_filter(plan, id, consumer, other, condition);
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

/// Drop the match, and turn the join that carried it into the filter, reading
/// the side that stays.
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
/// * **Every variable resolves to exactly one single-valued slot of a scan
///   that feeds the landing site.** A variable two stars bind is a value join,
///   and naming either column answers a different question. A variable bound
///   by a *multivalued* slot is worse than ambiguous: the same
///   `(star, slot_path)` address means the array below its [`PlanOp::Unnest`]
///   and one element above it, so a condition carrying that address means one
///   thing to a renderer reading it as a column and another to one reading it
///   after the fan-out -- a containment test and an equality, which select
///   different rows. That ambiguity is a missing fact in the representation
///   (nothing names the *element*), and 28d's own lesson is that a rule facing
///   one should not paper over it: this declines until an unnest binds the
///   element.
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
        if !self.constants_are_terms(&resolved, visible) {
            return None;
        }
        // The rendering test itself, and the one 28d asks a rule to *ask*
        // rather than to decide: the pushable subset is the sum of what
        // `to_sql` accepts, not a constant of this rule.
        resolved.to_sql()?;
        Some(resolved)
    }

    /// Whether every constant this condition compares against a slot is the
    /// term that slot's values render as.
    ///
    /// Only the shapes [`Expr::to_sql`] turns into conditions are checked; the
    /// rest it declines on its own, and declining twice for two reasons is not
    /// a stronger claim.
    fn constants_are_terms(&self, expr: &Expr, visible: &Visible) -> bool {
        let comparable = |star_var: &String, slot_path: &Vec<String>, term: &Term| {
            let [slot] = slot_path.as_slice() else {
                // A path into a record, which no `ScanSlot` binds and this rule
                // never builds.
                return false;
            };
            visible
                .class_of_star
                .get(star_var)
                .is_some_and(|class_uri| {
                    constant_is_the_columns_term(self.schema, class_uri, slot, term)
                })
        };
        match expr {
            Expr::Compare { left, right, .. } => match (left.as_ref(), right.as_ref()) {
                (
                    Expr::Slot {
                        star_var,
                        slot_path,
                    },
                    Expr::Literal(term),
                )
                | (
                    Expr::Literal(term),
                    Expr::Slot {
                        star_var,
                        slot_path,
                    },
                ) => comparable(star_var, slot_path, term),
                _ => true,
            },
            Expr::In { value, candidates } => match value.as_ref() {
                Expr::Slot {
                    star_var,
                    slot_path,
                } => candidates.iter().all(|candidate| match candidate {
                    Expr::Literal(term) => comparable(star_var, slot_path, term),
                    _ => true,
                }),
                _ => true,
            },
            Expr::And(parts) | Expr::Or(parts) | Expr::Function { args: parts, .. } => parts
                .iter()
                .all(|part| self.constants_are_terms(part, visible)),
            Expr::Not(inner) => self.constants_are_terms(inner, visible),
            Expr::Var(_) | Expr::Literal(_) | Expr::Slot { .. } | Expr::Opaque(_) => true,
        }
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
                slot_path: vec![binding.slot.clone()],
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

    /// A constant on a multivalued slot asks whether the array contains the
    /// value, which is not what a condition naming the column says -- and the
    /// scan's own fold declines it too, so the match stays whole.
    #[test]
    fn a_constant_on_a_multivalued_slot_stays_a_match() {
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

        assert_eq!(plan.find("match").len(), 1, "{plan}");
        assert!(plan.find("filter").is_empty(), "{plan}");
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

    /// A variable bound by a multivalued slot is not a column. The same
    /// `(star, slot_path)` address means the array below its unnest and one
    /// element above it, and a condition carrying it would be a containment
    /// test to one reader and an equality to another -- different rows. The
    /// missing fact is a name for the element, which is a change to the
    /// representation and not to this rule.
    #[test]
    fn a_comparison_on_a_multivalued_slot_is_not_pushed() {
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

        assert_eq!(
            plan.nodes[plan.find("filter")[0]].executor,
            Executor::Engine,
            "{plan}"
        );
        assert_eq!(
            plan.nodes[plan.find("scan")[0]].executor,
            Executor::Sql,
            "the scan still folds it; only the condition on it declines:\n{plan}"
        );
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
