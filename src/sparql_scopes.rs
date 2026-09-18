//! Scopes and the properties every relational subtree derives.
//!
//! The one representational fact the planner lacked, and the vocabulary the
//! rules read it through -- `docs/design/sparql-scopes-as-relations.md`,
//! *The fact: a scope* and *What a relational subtree derives*.
//!
//! A **scope** is a subtree SPARQL evaluates to a solution multiset of its
//! own before combining it with the outside: a sub-`SELECT`, an `OPTIONAL`
//! body, the block of an `EXISTS`. The plan states one with a
//! [`PlanOp::SubSelect`] *barrier* over the complete unit, and the builder
//! constructs every barrier, so a scope is a fact of the plan whether or not
//! anything lowers it. [`Plan::scope_of`] says which scope a node is in;
//! [`Scope::of`] reads a barrier's interface off the plan.
//!
//! Every node answers the same derived questions -- what it outputs
//! ([`Plan::variables_of`]), what it guarantees ([`Plan::guaranteed`]), what
//! kind of term each output is ([`Plan::term_of`]), which outer variables it
//! reads ([`Plan::correlated_inputs`]) and which of its input's outputs it
//! *observes* ([`Plan::demand`]) -- each with a transfer function per
//! operator. Rules consume the answers and re-walk nothing; a new operator
//! adds one arm per property and every rule applies to it unchanged. The
//! answers are computed from the current plan when asked and never cached
//! across a rewrite, which is what makes "property updates follow mutations"
//! true by construction (*Evidence that survives rewriting*).

use std::collections::{BTreeMap, BTreeSet};

use linkml_schemaview::schemaview::SchemaView;

use spargebra::algebra::{AggregateExpression, AggregateFunction};

use crate::sparql_refine::{
    Executor, Expr, NodeId, NodeKey, Plan, PlanOp, SlotPresence, SlotReading, variables_used,
};

/// A variable *and the node that produces it*.
///
/// Two scopes binding `?a` are two producers; a reference from outside
/// resolves to one of them or to nothing, never to "some `?a`". Held by key,
/// so it survives renumbering.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OutputSlot {
    pub node: NodeKey,
    pub var: String,
}

/// One variable a scope lets the outside read, with the slot inside that
/// produces it -- or `None` when the body binds it in no row (a projected
/// variable nothing below binds is legal SPARQL and unbound everywhere).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Export {
    pub var: String,
    pub producer: Option<OutputSlot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeKind {
    /// Rows come out and are joined: a sub-`SELECT`, an `OPTIONAL` body.
    Exporting,
    /// Rows are only tested against: the right side of a `NOT EXISTS` or a
    /// `MINUS`. Exports nothing.
    Testing,
}

/// Where a subtree is a unit of its own. Computed by [`Scope::of`], never
/// written by hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub root: NodeId,
    pub kind: ScopeKind,
    pub exports: Vec<Export>,
    /// What the body reads from the outside. Empty for every kind but a
    /// `Testing` scope under an `AntiJoin` (`EXISTS` is evaluated by
    /// substitution, §18.6).
    pub correlated_inputs: Vec<String>,
}

impl Scope {
    /// The scope rooted at `root`: a barrier, or the right side of an
    /// `AntiJoin`/`Minus`. `None` for a node that roots no scope.
    pub fn of(plan: &Plan, root: NodeId) -> Option<Scope> {
        match &plan.nodes[root].op {
            PlanOp::SubSelect { input, vars, .. } => {
                let exports = vars
                    .iter()
                    .map(|var| Export {
                        var: var.clone(),
                        producer: plan.producers_of(*input, var).first().map(|producer| {
                            OutputSlot {
                                node: plan.key_of(*producer),
                                var: var.clone(),
                            }
                        }),
                    })
                    .collect();
                Some(Scope {
                    root,
                    kind: ScopeKind::Exporting,
                    exports,
                    correlated_inputs: Vec::new(),
                })
            }
            _ => {
                // A testing scope is the right side of a negation; its root
                // is whatever node sits there.
                let testing = plan.nodes.iter().find_map(|node| match &node.op {
                    PlanOp::AntiJoin { left, right, .. } if *right == root => Some(Some(*left)),
                    PlanOp::Minus { right, .. } if *right == root => Some(None),
                    _ => None,
                })?;
                let correlated_inputs = match testing {
                    Some(left) => plan
                        .variables_of(root)
                        .intersection(&plan.variables_of(left))
                        .cloned()
                        .collect(),
                    None => Vec::new(),
                };
                Some(Scope {
                    root,
                    kind: ScopeKind::Testing,
                    exports: Vec::new(),
                    correlated_inputs,
                })
            }
        }
    }
}

/// What kind of term a variable is at a node, when bound. One entry per
/// producer, so a variable two producers disagree about is visibly
/// ambiguous rather than resolved to whichever was seen first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TermOf {
    /// A scanned record's own identity.
    Identity { class_uri: String },
    /// A slot value of a scanned record, at a path, read one way.
    Slot {
        star_var: String,
        path: Vec<String>,
        class_uri: String,
        reading: SlotReading,
        presence: SlotPresence,
    },
    /// An aggregate's result.
    Measure { guaranteed: bool },
    /// An inlined element, identified by its occurrence: the holder's
    /// identity and one hop per collection step on `path`.
    Structure {
        holder_star: String,
        path: Vec<String>,
        class_uri: String,
    },
    /// A multivalued slot whose fan-out has not happened below this node, so
    /// the variable stands for no single value here.
    Collection { star_var: String, path: Vec<String> },
    /// A value the engine computes: a `BIND` of an expression, a triple
    /// pattern's binding, a `VALUES` cell. Not a column.
    Computed,
}

impl Plan {
    /// Every barrier, in index order -- which is inner before outer, since a
    /// barrier is pushed after its whole input subtree.
    pub fn barriers(&self) -> Vec<NodeId> {
        self.find("subselect")
    }

    /// The scope a node is in: the innermost barrier whose input subtree
    /// holds it, or `None` for the outermost scope. A barrier is in the scope
    /// *enclosing* it, not in its own.
    pub fn scope_of(&self, node: NodeId) -> Option<NodeId> {
        self.scopes()[node]
    }

    /// [`Plan::scope_of`] for every node at once.
    pub fn scopes(&self) -> Vec<Option<NodeId>> {
        let mut scope: Vec<Option<NodeId>> = vec![None; self.nodes.len()];
        // Inner barriers precede outer ones, so the first barrier to claim a
        // node is the innermost.
        for barrier in self.barriers() {
            let PlanOp::SubSelect { input, .. } = &self.nodes[barrier].op else {
                continue;
            };
            for (id, slot) in scope.iter_mut().enumerate() {
                if slot.is_none() && self.feeds(id, *input) {
                    *slot = Some(barrier);
                }
            }
        }
        scope
    }

    /// The nodes of one scope, in index order.
    pub fn scope_members(&self, scope: Option<NodeId>) -> Vec<NodeId> {
        self.scopes()
            .into_iter()
            .enumerate()
            .filter(|(_, in_scope)| *in_scope == scope)
            .map(|(id, _)| id)
            .collect()
    }

    /// Whether `lower`'s rows reach `upper` without crossing a barrier: a
    /// barrier is a leaf of the walk, so a node inside one is reachable only
    /// as far as the barrier itself. The question a rule asks when it may
    /// read a scan's columns directly.
    pub fn feeds_within_scope(&self, lower: NodeId, upper: NodeId) -> bool {
        if lower == upper {
            return true;
        }
        let Some(node) = self.nodes.get(upper) else {
            return false;
        };
        if matches!(node.op, PlanOp::SubSelect { .. }) {
            return false;
        }
        node.op
            .inputs()
            .into_iter()
            .any(|input| self.feeds_within_scope(lower, input))
    }

    /// [`Plan::feeds_within_scope`], reading through a barrier the lowering
    /// elides ([`Plan::transparent`]): what a rule that resolves a variable
    /// to a scan's column may see.
    pub fn feeds_visibly(&self, lower: NodeId, upper: NodeId) -> bool {
        if lower == upper {
            return true;
        }
        let Some(node) = self.nodes.get(upper) else {
            return false;
        };
        if matches!(node.op, PlanOp::SubSelect { .. }) && !self.transparent(upper) {
            return false;
        }
        node.op
            .inputs()
            .into_iter()
            .any(|input| self.feeds_visibly(lower, input))
    }

    /// The barrier between `lower` and `upper`, when one is crossed on the
    /// way: the outermost barrier below `upper` that `lower` feeds.
    pub fn barrier_between(&self, lower: NodeId, upper: NodeId) -> Option<NodeId> {
        if lower == upper {
            return None;
        }
        let node = self.nodes.get(upper)?;
        for input in node.op.inputs() {
            if !self.feeds(lower, input) {
                continue;
            }
            if matches!(self.nodes[input].op, PlanOp::SubSelect { .. }) && input != lower {
                return Some(input);
            }
            return self.barrier_between(lower, input);
        }
        None
    }

    /// Whether a node binds `var` itself, as opposed to passing it through.
    fn binds_here(&self, id: NodeId, var: &str) -> bool {
        match &self.nodes[id].op {
            PlanOp::Scan {
                star_var, slots, ..
            } => star_var == var || slots.iter().any(|slot| slot.var.as_deref() == Some(var)),
            PlanOp::Unnest { var: bound, .. } => bound == var,
            PlanOp::Match { .. } | PlanOp::Path { .. } | PlanOp::Values { .. } => {
                self.variables_of(id).contains(var)
            }
            PlanOp::Bind { var: bound, .. } => bound == var,
            PlanOp::Group { measures, .. } => measures.iter().any(|measure| measure.var == var),
            _ => false,
        }
    }

    /// The nodes that produce `var` as seen from `node`'s output: the
    /// binders reached by walking down, through a barrier only where the
    /// barrier exports the variable. Empty when nothing visible binds it.
    ///
    /// A rename (`BIND(?agg AS ?n)`) and a group key pass through to what
    /// they rename or key on, so the answer for a sub-query's exported `?a`
    /// is the scan inside it -- which is what `resolve` asks.
    pub fn producers_of(&self, node: NodeId, var: &str) -> Vec<NodeId> {
        let mut out = Vec::new();
        self.collect_producers(node, var, &mut out);
        out
    }

    fn collect_producers(&self, id: NodeId, var: &str, out: &mut Vec<NodeId>) {
        if self.binds_here(id, var) {
            if !out.contains(&id) {
                out.push(id);
            }
            return;
        }
        match &self.nodes[id].op {
            PlanOp::Unit
            | PlanOp::Match { .. }
            | PlanOp::Path { .. }
            | PlanOp::Values { .. }
            | PlanOp::Scan { .. } => {}
            PlanOp::Join { left, right, .. } | PlanOp::Union { left, right } => {
                self.collect_producers(*left, var, out);
                self.collect_producers(*right, var, out);
            }
            PlanOp::LeftJoin { left, right, .. } => {
                let before = out.len();
                self.collect_producers(*left, var, out);
                if out.len() == before {
                    self.collect_producers(*right, var, out);
                }
            }
            PlanOp::Minus { left, .. } | PlanOp::AntiJoin { left, .. } => {
                self.collect_producers(*left, var, out);
            }
            PlanOp::Bind { input, expr, .. } => {
                // A rename passes through; any other bound variable is
                // caught by `binds_here` above.
                if let Expr::Var(_) = expr {}
                self.collect_producers(*input, var, out);
            }
            PlanOp::Group { input, keys, .. } => {
                if keys.iter().any(|key| key == var) {
                    self.collect_producers(*input, var, out);
                }
            }
            PlanOp::Project { input, vars, .. } | PlanOp::SubSelect { input, vars, .. } => {
                if vars.iter().any(|v| v == var) {
                    self.collect_producers(*input, var, out);
                }
            }
            PlanOp::Filter { input, .. }
            | PlanOp::Sort { input, .. }
            | PlanOp::Distinct { input }
            | PlanOp::Reduced { input }
            | PlanOp::Slice { input, .. }
            | PlanOp::Graph { input, .. }
            | PlanOp::Service { input, .. }
            | PlanOp::Unnest { input, .. }
            | PlanOp::Construct { input, .. }
            | PlanOp::Describe { input, .. }
            | PlanOp::Ask { input } => self.collect_producers(*input, var, out),
        }
    }

    /// Whether a variable is bound anywhere below `node`, barriers or not.
    /// The complement of [`Plan::producers_of`] that scope closure needs: a
    /// reference with no visible producer *and* a hidden one is a reference
    /// into a scope.
    pub fn bound_anywhere_below(&self, node: NodeId, var: &str) -> bool {
        (0..self.nodes.len()).any(|id| self.feeds(id, node) && self.binds_here(id, var))
    }

    /// The variables a node binds in every solution it emits: the
    /// *certainly bound* property, [`Plan::definitely_bound_of`] plus the
    /// one measure whose aggregate guarantees a value. `COUNT` is defined
    /// over an empty group (it is `0`), so a count is bound in every row;
    /// `MIN`/`MAX` error over nothing and `SUM`/`AVG` are kept out
    /// conservatively (design, open question 9).
    pub fn guaranteed(&self, node: NodeId) -> BTreeSet<String> {
        let mut out = self.definitely_bound_of(node);
        if let PlanOp::Group { measures, .. } = &self.nodes[node].op {
            for measure in measures {
                if aggregate_guarantees_a_value(&measure.aggregate) {
                    out.insert(measure.var.clone());
                }
            }
        }
        // A rename of a guaranteed measure is guaranteed too.
        if let PlanOp::Bind {
            input,
            var,
            expr: Expr::Var(renamed),
        } = &self.nodes[node].op
            && self.guaranteed(*input).contains(renamed)
        {
            out.insert(var.clone());
        }
        // A projection keeps a guarantee its input has, including a measure's.
        if let PlanOp::Project { input, vars, .. } | PlanOp::SubSelect { input, vars, .. } =
            &self.nodes[node].op
        {
            let below = self.guaranteed(*input);
            out.extend(vars.iter().filter(|var| below.contains(*var)).cloned());
        }
        if let PlanOp::Filter { input, .. }
        | PlanOp::Sort { input, .. }
        | PlanOp::Distinct { input }
        | PlanOp::Reduced { input }
        | PlanOp::Slice { input, .. } = &self.nodes[node].op
        {
            out.extend(self.guaranteed(*input));
        }
        if let PlanOp::Join { left, right, .. } = &self.nodes[node].op {
            out.extend(self.guaranteed(*left));
            out.extend(self.guaranteed(*right));
        }
        if let PlanOp::LeftJoin { left, .. } = &self.nodes[node].op {
            out.extend(self.guaranteed(*left));
        }
        out
    }

    /// What kind of term `var` is at `node`'s output, one answer per
    /// producer. Empty when nothing visible binds it.
    pub fn term_of(&self, schema: &SchemaView, node: NodeId, var: &str) -> Vec<TermOf> {
        let mut out = Vec::new();
        for producer in self.producers_of(node, var) {
            let term = match &self.nodes[producer].op {
                PlanOp::Scan {
                    star_var,
                    class_uri,
                    slots,
                    ..
                } => {
                    if star_var == var {
                        TermOf::Identity {
                            class_uri: class_uri.clone(),
                        }
                    } else {
                        let slot = slots
                            .iter()
                            .find(|slot| slot.var.as_deref() == Some(var))
                            .expect("binds_here said so");
                        if slot.multivalued {
                            TermOf::Collection {
                                star_var: star_var.clone(),
                                path: slot.path.clone(),
                            }
                        } else {
                            TermOf::Slot {
                                star_var: star_var.clone(),
                                path: slot.path.clone(),
                                class_uri: class_uri.clone(),
                                reading: SlotReading::Column,
                                presence: slot.presence,
                            }
                        }
                    }
                }
                PlanOp::Unnest {
                    star_var,
                    slot_path,
                    presence,
                    ..
                } => {
                    let class_uri = self.class_of_star_below(producer, star_var);
                    match class_uri {
                        Some(class_uri) => {
                            match crate::sparql_terms::resolve_column(schema, &class_uri, slot_path)
                            {
                                // A term at the end of the path: one element.
                                Some((descriptor, _))
                                    if !is_structure(schema, &class_uri, slot_path) =>
                                {
                                    let _ = descriptor;
                                    TermOf::Slot {
                                        star_var: star_var.clone(),
                                        path: slot_path.clone(),
                                        class_uri,
                                        reading: SlotReading::BoundElement,
                                        presence: *presence,
                                    }
                                }
                                _ => TermOf::Structure {
                                    holder_star: star_var.clone(),
                                    path: slot_path.clone(),
                                    class_uri: class_at_path_of(schema, &class_uri, slot_path)
                                        .unwrap_or_default(),
                                },
                            }
                        }
                        None => TermOf::Computed,
                    }
                }
                PlanOp::Group { measures, .. } => {
                    let measure = measures
                        .iter()
                        .find(|measure| measure.var == var)
                        .expect("binds_here said so");
                    TermOf::Measure {
                        guaranteed: aggregate_guarantees_a_value(&measure.aggregate),
                    }
                }
                PlanOp::Bind {
                    input,
                    expr: Expr::Var(renamed),
                    ..
                } => {
                    out.extend(self.term_of(schema, *input, renamed));
                    continue;
                }
                _ => TermOf::Computed,
            };
            if !out.contains(&term) {
                out.push(term);
            }
        }
        out
    }

    /// The class a star is scanned as, among the scans feeding `node` in its
    /// own scope.
    fn class_of_star_below(&self, node: NodeId, star_var: &str) -> Option<String> {
        self.nodes
            .iter()
            .enumerate()
            .find_map(|(id, below)| match &below.op {
                PlanOp::Scan {
                    star_var: scanned,
                    class_uri,
                    ..
                } if scanned == star_var && self.feeds_within_scope(id, node) => {
                    Some(class_uri.clone())
                }
                _ => None,
            })
    }

    /// The one identity class `var` has at `node`, when every producer agrees
    /// it is a scanned record's own identity of one class.
    pub fn identity_class(&self, schema: &SchemaView, node: NodeId, var: &str) -> Option<String> {
        let terms = self.term_of(schema, node, var);
        let mut classes = terms.iter().filter_map(|term| match term {
            TermOf::Identity { class_uri } => Some(class_uri.clone()),
            _ => None,
        });
        let class = classes.next()?;
        if classes.next().is_some() {
            return None;
        }
        // An identity beside a slot binding of the same variable (a reference
        // join) is still an identity: on a join both hold the same IRI.
        Some(class)
    }

    /// The outer variables a subtree reads: empty everywhere but the right
    /// side of an `AntiJoin`.
    pub fn correlated_inputs(&self, node: NodeId) -> Vec<String> {
        for other in &self.nodes {
            if let PlanOp::AntiJoin { left, right, .. } = &other.op
                && (*right == node || self.feeds(node, *right))
            {
                return self
                    .variables_of(*right)
                    .intersection(&self.variables_of(*left))
                    .cloned()
                    .collect();
            }
        }
        Vec::new()
    }

    /// Which of `input`'s outputs the node at `id` *observes*: a producer it
    /// reads by name, or one whose presence or value changes its answer with
    /// no name in sight. The sixth derived property; the transfer table is
    /// the design's, and an operator the table does not name observes
    /// everything.
    pub fn demand(&self, id: NodeId, input: NodeId) -> BTreeSet<String> {
        let everything = || self.variables_of(input);
        let node = &self.nodes[id];
        match &node.op {
            PlanOp::Project { vars, .. } | PlanOp::SubSelect { vars, .. } => {
                vars.iter().cloned().collect()
            }
            PlanOp::Describe { vars, .. } => vars.iter().cloned().collect(),
            PlanOp::Filter { condition, .. } => {
                if condition.contains_an_opaque_subquery() {
                    return everything();
                }
                variables_used(condition).into_iter().collect()
            }
            PlanOp::Bind { expr, .. } => {
                if expr.contains_an_opaque_subquery() {
                    return everything();
                }
                variables_used(expr).into_iter().collect()
            }
            PlanOp::Sort { terms, .. } => terms
                .iter()
                .flat_map(|term| variables_used(&term.expr))
                .collect(),
            PlanOp::Group {
                keys,
                measures,
                having,
                ..
            } => {
                let mut out: BTreeSet<String> = keys.iter().cloned().collect();
                for measure in measures {
                    match &measure.aggregate {
                        AggregateExpression::CountSolutions { distinct: true } => {
                            return everything();
                        }
                        AggregateExpression::CountSolutions { distinct: false } => {}
                        AggregateExpression::FunctionCall { expr, .. } => {
                            out.extend(variables_used(&Expr::from(expr)));
                        }
                    }
                }
                for condition in having {
                    out.extend(variables_used(condition));
                }
                out
            }
            PlanOp::Distinct { .. } | PlanOp::Reduced { .. } => everything(),
            PlanOp::Join { left, right, .. } => {
                let mut out: BTreeSet<String> = self
                    .variables_of(*left)
                    .intersection(&self.variables_of(*right))
                    .cloned()
                    .collect();
                out.retain(|var| self.variables_of(input).contains(var));
                out
            }
            PlanOp::LeftJoin {
                left,
                right,
                condition,
                ..
            } => {
                let mut out: BTreeSet<String> = self
                    .variables_of(*left)
                    .intersection(&self.variables_of(*right))
                    .cloned()
                    .collect();
                if let Some(condition) = condition {
                    if condition.contains_an_opaque_subquery() {
                        return everything();
                    }
                    out.extend(variables_used(condition));
                }
                out.retain(|var| self.variables_of(input).contains(var));
                out
            }
            PlanOp::Minus { left, right } => {
                let mut out: BTreeSet<String> = self
                    .variables_of(*left)
                    .intersection(&self.variables_of(*right))
                    .cloned()
                    .collect();
                out.retain(|var| self.variables_of(input).contains(var));
                out
            }
            PlanOp::AntiJoin { .. } => self.correlated_inputs(id).into_iter().collect(),
            PlanOp::Union { .. } | PlanOp::Slice { .. } => BTreeSet::new(),
            PlanOp::Unnest { .. } => BTreeSet::new(),
            PlanOp::Ask { .. } => BTreeSet::new(),
            PlanOp::Construct { template, .. } => {
                let mut out = BTreeSet::new();
                for triple in template {
                    for term in [&triple.subject, &triple.object] {
                        if let spargebra::term::TermPattern::Variable(variable) = term {
                            out.insert(variable.as_str().to_owned());
                        }
                    }
                    if let spargebra::term::NamedNodePattern::Variable(variable) = &triple.predicate
                    {
                        out.insert(variable.as_str().to_owned());
                    }
                }
                out
            }
            // Leaves observe nothing; anything else this table does not
            // name observes everything, which is the conservative default.
            PlanOp::Unit
            | PlanOp::Match { .. }
            | PlanOp::Path { .. }
            | PlanOp::Values { .. }
            | PlanOp::Scan { .. } => BTreeSet::new(),
            PlanOp::Graph { .. } | PlanOp::Service { .. } => everything(),
        }
    }

    /// The outputs of `input` that pass through the node at `id` unchanged,
    /// so a demand above it on them is a demand on `input`.
    fn passes_through(&self, id: NodeId, input: NodeId) -> BTreeSet<String> {
        let inputs_outputs = self.variables_of(input);
        let node = &self.nodes[id];
        let own: BTreeSet<String> = match &node.op {
            PlanOp::Project { vars, .. } | PlanOp::SubSelect { vars, .. } => {
                vars.iter().cloned().collect()
            }
            PlanOp::Group { keys, .. } => keys.iter().cloned().collect(),
            PlanOp::Minus { left, .. } | PlanOp::AntiJoin { left, .. } => {
                if *left == input {
                    inputs_outputs.clone()
                } else {
                    BTreeSet::new()
                }
            }
            PlanOp::Construct { .. } | PlanOp::Describe { .. } | PlanOp::Ask { .. } => {
                BTreeSet::new()
            }
            _ => inputs_outputs.clone(),
        };
        own.intersection(&inputs_outputs).cloned().collect()
    }

    /// Everything the consumers of `node` demand of it, transitively: each
    /// consumer's own demand plus what is demanded of the consumer on the
    /// variables it passes through.
    pub fn demand_above(&self, node: NodeId) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for consumer in crate::sparql_rules::consumers_of(self, node) {
            out.extend(self.demand(consumer, node));
            let passed = self.passes_through(consumer, node);
            if !passed.is_empty() {
                let above = self.demand_above(consumer);
                out.extend(passed.into_iter().filter(|var| above.contains(var)));
            }
        }
        out
    }

    /// What every barrier's consumers demand of it, as it stands: the fact
    /// the driver records before a rule edits the plan and checks after.
    /// One entry per barrier, keyed by its [`NodeKey`].
    pub fn kept_exports(&self) -> BTreeMap<NodeKey, BTreeSet<String>> {
        self.barriers()
            .into_iter()
            .map(|barrier| {
                let outputs = self.variables_of(barrier);
                let kept: BTreeSet<String> = self
                    .demand_above(barrier)
                    .into_iter()
                    .filter(|var| outputs.contains(var))
                    .collect();
                (self.key_of(barrier), kept)
            })
            .collect()
    }

    /// **The transition check: no demanded export is dropped.** Every export
    /// a barrier's consumers demanded before an edit must still be an output
    /// of the node that barrier resolves to after it -- itself, or its
    /// successor through `retired`. A retirement with no successor fails.
    ///
    /// A transition rather than a state invariant because `demand` is
    /// recomputed from the plan and shrinks with the interface it guards:
    /// after a bad prune of `?y` under `COUNT(DISTINCT *)` the group demands
    /// `{?x}`, the barrier exports `{?x}`, and every state check holds of the
    /// wrong plan. The driver holds `kept` for one application and no
    /// further.
    pub fn check_transition(
        &self,
        kept: &BTreeMap<NodeKey, BTreeSet<String>>,
    ) -> Result<(), TransitionDefect> {
        for (key, vars) in kept {
            if vars.is_empty() {
                continue;
            }
            let Some(now) = self.resolve(*key) else {
                return Err(TransitionDefect::BarrierGone { barrier: *key });
            };
            let outputs = self.variables_of(now);
            if let Some(var) = vars.iter().find(|var| !outputs.contains(*var)) {
                return Err(TransitionDefect::ExportDropped {
                    barrier: *key,
                    var: var.clone(),
                });
            }
        }
        Ok(())
    }

    /// A pushed barrier the lowering elides, so the statement reads exactly
    /// as it did before barriers existed: the body of an `OPTIONAL` that is
    /// one scan with its filters and fan-outs, under a left join with a
    /// recorded reference edge -- the shape `AbsorbOptionalReference` and
    /// `PushLeftJoin` served, rendered as the `LEFT JOIN … ON` they emit.
    /// `Visible` reads through such a barrier and the lowering renders no
    /// derived table for it, and both ask this one function so the two
    /// cannot disagree.
    pub fn transparent(&self, barrier: NodeId) -> bool {
        if self.nodes[barrier].executor != Executor::Sql || !self.transparent_shape(barrier) {
            return false;
        }
        self.nodes.iter().any(|node| {
            matches!(&node.op, PlanOp::LeftJoin { right, reference: Some(_), .. }
                if *right == barrier && node.executor == Executor::Sql)
        })
    }

    /// The shape half of [`Plan::transparent`]: an `OPTIONAL` body barrier
    /// whose body is scans, fan-outs, filters and further such optional
    /// bodies only -- what a flat `LEFT JOIN` chain renders.
    pub fn transparent_shape(&self, barrier: NodeId) -> bool {
        let PlanOp::SubSelect {
            input,
            domain: None,
            ..
        } = &self.nodes[barrier].op
        else {
            return false;
        };
        let input = *input;
        (0..self.nodes.len())
            .filter(|id| self.feeds(*id, input))
            .all(|id| {
                matches!(
                    &self.nodes[id].op,
                    PlanOp::Scan { .. }
                        | PlanOp::Unnest { .. }
                        | PlanOp::Filter { .. }
                        | PlanOp::LeftJoin { .. }
                        | PlanOp::SubSelect { domain: None, .. }
                )
            })
    }
}

/// A rule's edit dropped an export its consumers demanded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionDefect {
    BarrierGone { barrier: NodeKey },
    ExportDropped { barrier: NodeKey, var: String },
}

impl std::fmt::Display for TransitionDefect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BarrierGone { barrier } => write!(
                f,
                "barrier {barrier} was retired with no successor while a consumer demanded its exports"
            ),
            Self::ExportDropped { barrier, var } => write!(
                f,
                "barrier {barrier} no longer outputs ?{var}, which a consumer demanded before the edit"
            ),
        }
    }
}

/// Whether an aggregate is bound in every group: `COUNT` is `0` over nothing.
pub fn aggregate_guarantees_a_value(aggregate: &AggregateExpression) -> bool {
    match aggregate {
        AggregateExpression::CountSolutions { .. } => true,
        AggregateExpression::FunctionCall { name, .. } => {
            matches!(name, AggregateFunction::Count)
        }
    }
}

/// Whether the value at the end of a path is an inlined structure rather
/// than a term.
pub fn is_structure(schema: &SchemaView, class_uri: &str, path: &[String]) -> bool {
    crate::sparql_terms::resolve_column(schema, class_uri, path).is_none()
        && class_at_path_of(schema, class_uri, path).is_some()
}

/// The class of the value at the end of a path of *inlined* hops, collections
/// allowed: the element's class for `[hasCoveredSection]`, the nested one for
/// `[parts, children]`. `None` past a reference hop (the value is in another
/// record) or where the range is not a class.
pub fn class_at_path_of(schema: &SchemaView, class_uri: &str, path: &[String]) -> Option<String> {
    use linkml_schemaview::identifier::Identifier;
    use linkml_schemaview::slotview::SlotInlineMode;
    let mut class = schema.get_class_by_uri(class_uri).ok().flatten()?;
    for name in path {
        let slot = class.slot(&Identifier::Name(name.clone()))?;
        if slot.determine_slot_inline_mode() == SlotInlineMode::Reference {
            return None;
        }
        class = slot.get_range_class()?;
    }
    Some(class.canonical_uri().to_string())
}

/// Whether `var` at `node` is a column a derived table can carry, and --
/// with `serialise` -- one the final projection can emit as an RDF term.
///
/// *Representable* and *serialisable* are two properties: every kind but
/// `Structure` is both, a structure is the first only (a blank node has no
/// term a statement can spell), and an ambiguous or computed variable is
/// neither.
pub fn representable(
    schema: &SchemaView,
    plan: &Plan,
    node: NodeId,
    var: &str,
    serialise: bool,
) -> bool {
    let terms = plan.term_of(schema, node, var);
    let [term] = terms.as_slice() else {
        // Two producers disagreeing, or none: not one column.
        return terms.len() > 1 && terms.iter().all(|term| term == &terms[0]);
    };
    match term {
        TermOf::Identity { .. } | TermOf::Measure { .. } => true,
        TermOf::Slot {
            class_uri, path, ..
        } => crate::sparql_terms::resolve_column(schema, class_uri, path).is_some(),
        TermOf::Structure { .. } => !serialise,
        TermOf::Collection { .. } | TermOf::Computed => false,
    }
}
