//! **Materialisation**: evaluating a subplan now, and replacing it in the plan
//! with the relation it denotes.
//!
//! # The shape
//!
//! A *materialisation pass* is two things and nothing else:
//!
//! * a **criterion** — which subplans this pass can evaluate;
//! * a **dataset** — what it evaluates them against.
//!
//! Everything else is shared and lives here: finding the maximal subplan that
//! qualifies, writing it back as a query, running it, and swapping it into the
//! plan as a [`PlanOp::Values`]. A pass supplies the two answers and inherits
//! the rest, which is what makes a second pass an implementation of
//! [`Materialisation`] rather than an edit to the first.
//!
//! [`SchemaGraphMaterialisation`] is the first instance, and the one asset360
//! GitLab issue #409 is about. It is not the only shape there is: a subplan
//! built only from `VALUES` blocks depends on no dataset at all and could be
//! materialised the same way, and `a_second_pass_needs_no_edit_to_the_first`
//! exercises exactly that — a second `Materialisation` defined entirely in the
//! test module, running through the same rule, touching none of this.
//!
//! # `PlanOp::Values` is the interface
//!
//! What a materialisation produces is an ordinary inline table, and that is
//! deliberate. Nothing downstream knows a materialisation happened: the rules
//! that turn a relation into a narrower fetch — `values_becomes_filter` and
//! `values_narrow_the_joined_scan` — consume a `Values` whether the client
//! wrote it, the schema produced it, or a pass nobody has written yet did.
//! Those rules are the "variable resolver" half, and they are general because
//! the thing they consume is.
//!
//! # It is a fixpoint, not a pipeline
//!
//! Materialising is not a step that runs once before resolving. A pass is an
//! ordinary rule in [`crate::sparql_rules::refine`]'s loop, so:
//!
//! * a materialisation can **enable another one**. Replacing a subplan with a
//!   `Values` makes it read nothing, so a *larger* region around it can become
//!   closed and qualify in the next round;
//! * a resolution can enable a materialisation, and the other way round, for
//!   the same reason.
//!
//! **Termination.** Each materialisation replaces a subtree of *n* nodes with
//! one, and never fires on a node that is already a `Values`, so it can happen
//! at most once per node in the plan. Resolution adds a node per (relation,
//! join variable) pair and refuses to add one twice
//! (`values_narrow_the_joined_scan`'s `already_constrained`), so it is bounded
//! by the same count. `refine`'s round limit is the backstop, not the argument.
//!
//! **Order does not matter.** Two subplans that qualify independently are
//! disjoint — [`subtree_is_private`] is what says so — and replacing a subplan
//! by the relation it denotes is an equivalence, so doing either first leaves
//! the same plan. `two_independent_regions_are_both_materialised` asserts that
//! both happen; that the order between them is immaterial is the equivalence,
//! not a separate mechanism.
//!
//! **What a cascade needs, stated honestly.** The loop *permits* one pass to
//! enable another, and that is worth having for free rather than sequencing by
//! hand. It is not reachable with the two criteria that exist today: both treat
//! a `Values` as reading nothing, so a region containing a materialised one
//! already qualified before it was materialised. A pass with a narrower
//! criterion — one that does not look through a relation — would cascade, and
//! nothing would have to change here for it to.
//!
//! # Why the criterion is the pass's and the structure is not
//!
//! [`SchemaGraphMaterialisation`]'s criterion is:
//!
//! > **no dependency on anything outside the schema** — it reads only the
//! > schema graph, and it binds every variable it names.
//!
//! A property computed on the plan, not a list of query shapes: a shape nobody
//! has thought of yet either satisfies it or does not, and no code is written
//! either way. A different pass has a different criterion and the same
//! machinery; what they share is the *shape* of the claim — **this subplan can
//! be evaluated now** — which is what [`Materialisation::qualifies`] names.
//!
//! # Why the schema case is worth it
//!
//! Since every permissible value of an enum became a concept IRI, the only way
//! to ask "which signals have a type whose code contains `GS`" is to join the
//! instance side to the datamodel:
//!
//! ```sparql
//! SELECT ?s ?code WHERE {
//!   ?s a asset360:Signal ; asset360:signalType ?t .
//!   GRAPH <…/schema> { ?t skos:notation ?code }
//!   FILTER(CONTAINS(?code, "GS"))
//! }
//! ```
//!
//! The only selective predicate lives on the schema side, so the scan has
//! nothing to narrow with and the whole class is materialised — `Signal` is
//! 23,503 objects on dev against a `MAX_RESULT_ROWS` of 10,000, so the query is
//! refused outright. The schema graph is thousands of triples; evaluating the
//! predicate over nine permissible values instead of 23,503 rows is the whole
//! of the fix.
//!
//! # Three things follow from replacement, and they are why this is a plan pass
//!
//! **Replacement is context-free.** Substituting a subplan by the relation it
//! denotes is an equivalence under SPARQL's bottom-up semantics, and an
//! equivalence holds wherever the subplan sits: under an `OPTIONAL`, inside a
//! `MINUS`, inside `NOT EXISTS`, in one arm of a `UNION`. None of those is a
//! case anything here handles, because none of them is a case. An earlier
//! attempt injected a `VALUES` *beside* the schema pattern, which is sound only
//! when the pattern is in required position — and that precondition is exactly
//! what forces a catalogue of shapes to be enumerated and excluded.
//!
//! **A correlated variable is not a dependency.** The subplan above mentions
//! `?t`, which the instance side also binds. Bottom-up, that is not a
//! dependency: the subplan is evaluated on its own and joined afterwards. So
//! evaluating it standalone and replacing it with *all* of its solutions is
//! exactly correct, and the join is what narrows.
//!
//! **A free variable is.** A `FILTER` naming a variable nothing below it binds
//! depends on whatever does bind it — see [`no_free_variables`].
//!
//! # What is genuinely out, and why none of it is a query shape
//!
//! 1. **A blank node among the results.** [`PlanOp::Values`] rows are ground
//!    terms and a blank node is not one. Dropping it would shrink the relation,
//!    so the subplan is left alone.
//! 2. **A positive `EXISTS` inside a filter expression in the subplan.**
//!    [`crate::sparql_refine::Expr::Opaque`] keeps it as its *rendering*, so
//!    the subplan cannot be written back as a query to evaluate.
//!    (`FILTER NOT EXISTS` is fine: it is already `PlanOp::AntiJoin`.)
//! 3. **More rows than [`MAX_MATERIALISED_ROWS`].** A cost policy.
//!
//! All three are about representability or cost. There is no fourth kind.

use std::cell::OnceCell;

use linkml_schemaview::schemaview::SchemaView;
use oxigraph::model::{GraphName, NamedNode, Quad, Term as OxTerm};
use oxigraph::sparql::{QueryResults, SparqlEvaluator};
use oxigraph::store::Store;
use spargebra::Query;
use spargebra::algebra::{Expression, GraphPattern, OrderExpression};
use spargebra::term::{GroundTerm, NamedNodePattern, Variable};

use crate::sparql_refine::{NodeId, Plan, PlanOp};

/// A relation, as the pair a [`PlanOp::Values`] holds: its columns, and its
/// rows in that column order.
pub type Relation = (Vec<Variable>, Vec<Vec<Option<GroundTerm>>>);

/// The most rows this will materialise into a plan node.
///
/// An enum is single digits to low hundreds of values, which is the case this
/// exists for. Past this the node stops being a narrowing and starts being a
/// second scan written out longhand, so the subplan keeps the behaviour it has
/// today.
pub const MAX_MATERIALISED_ROWS: usize = 500;

// ---------------------------------------------------------------------------
// What a pass supplies
// ---------------------------------------------------------------------------

/// One materialisation pass: a criterion, and a dataset to evaluate against.
///
/// Implementing this is the whole of adding a pass. The maximal-subplan search,
/// the write-back, the evaluation, the row cap and the replacement are
/// [`materialisable_root`]'s and [`materialise`]'s, and they do not know which
/// pass they are serving.
pub trait Materialisation {
    /// Stable name, for the log a reader checks a plan against.
    fn name(&self) -> &'static str;

    /// **Can this subplan be evaluated now?**
    ///
    /// The pass's whole judgement, and it should be a *property* of the plan
    /// rather than a list of query shapes — see
    /// [`SchemaGraphMaterialisation`]'s, which is "depends on nothing outside
    /// the schema". A criterion stated as a property answers for a shape nobody
    /// has thought of yet; a criterion stated as a list does not.
    ///
    /// It does not need to check that the node emits solutions, that it is not
    /// already a relation, or that its subtree is private to it. Those are
    /// conditions for replacing *any* subplan with a relation, so they are
    /// asked once, in [`materialisable_root`].
    fn qualifies(&self, plan: &Plan, node: NodeId) -> bool;

    /// What to evaluate against, or `None` when the pass cannot answer right
    /// now — a store it failed to build, say. `None` leaves the plan untouched.
    ///
    /// A pass over subplans that read nothing at all would return an empty
    /// store here, not `None`: "nothing to read" is a dataset, and "I cannot
    /// answer" is not.
    fn dataset(&self) -> Option<&Store>;
}

// ---------------------------------------------------------------------------
// The schema graph's criterion
// ---------------------------------------------------------------------------

/// Whether this subplan depends on nothing outside the schema graph.
///
/// `schema_graph_iri` is the graph the active datamodel serves its schema in —
/// a parameter and never a constant, see [`crate::sparql_schema_graph`].
///
/// The walk carries one bit of context, *which graph a read would read from*,
/// because that is the only thing a `GRAPH` node changes about the nodes below
/// it. Everything that is not a read inherits the answer from its inputs, which
/// is why a node kind added later needs no entry here unless it reads.
pub fn schema_only(plan: &Plan, node: NodeId, schema_graph_iri: &str) -> bool {
    fn walk(plan: &Plan, node: NodeId, in_schema_graph: bool, iri: &str) -> bool {
        match &plan.nodes[node].op {
            // A read: schema-only exactly when it reads the schema graph.
            PlanOp::Match { .. } | PlanOp::Path { .. } => in_schema_graph,
            // A `GRAPH` decides what its whole subtree reads, so it replaces
            // the context rather than inheriting it. A nested one naming
            // something else takes its subtree back out of the schema.
            PlanOp::Graph { input, name } => {
                walk(plan, *input, names_the_schema_graph(name, iri), iri)
            }
            // Somebody else's dataset.
            PlanOp::Service { .. } => false,
            // Instance reads by construction: a rule builds one from triple
            // patterns in the default graph.
            PlanOp::Scan { .. } | PlanOp::Unnest { .. } => false,
            // Read nothing, so they depend on nothing.
            PlanOp::Values { .. } | PlanOp::Unit => true,
            other => other
                .inputs()
                .iter()
                .all(|input| walk(plan, *input, in_schema_graph, iri)),
        }
    }
    walk(plan, node, false, schema_graph_iri) && no_free_variables(plan, node)
}

/// Whether every variable the subplan *uses* is one it also binds.
///
/// The second half of "no dependency on anything outside the schema", and the
/// half that is about values rather than about reads. A `FILTER` naming a
/// variable nothing below it binds is a dependency on whatever does bind it —
/// evaluating the subplan on its own would evaluate that filter against an
/// unbound variable, which in SPARQL is an error, which excludes the solution.
/// The relation would come back empty and the plan would answer nothing for a
/// query that has an answer.
///
/// Checked per node rather than per subplan: a node's expressions may only name
/// what its own inputs bind, which is the same statement one level at a time.
fn no_free_variables(plan: &Plan, node: NodeId) -> bool {
    subtree(plan, node).into_iter().all(|id| {
        let bound: Vec<String> = plan.nodes[id]
            .op
            .inputs()
            .iter()
            .flat_map(|input| plan.variables_of(*input))
            .collect();
        expressions_of(&plan.nodes[id].op)
            .into_iter()
            .flat_map(crate::sparql_refine::variables_used)
            .all(|used| bound.contains(&used))
    })
}

/// Every expression a node carries. One place, so a node kind that grows an
/// expression is a compile error here rather than a silent gap.
fn expressions_of(op: &PlanOp) -> Vec<&crate::sparql_refine::Expr> {
    match op {
        PlanOp::Filter { condition, .. } => vec![condition],
        PlanOp::Bind { expr, .. } => vec![expr],
        PlanOp::LeftJoin { condition, .. } => condition.iter().collect(),
        PlanOp::Group { having, .. } => having.iter().collect(),
        PlanOp::Sort { terms, .. } => terms.iter().map(|term| &term.expr).collect(),
        PlanOp::Unit
        | PlanOp::Match { .. }
        | PlanOp::Path { .. }
        | PlanOp::Values { .. }
        | PlanOp::Join { .. }
        | PlanOp::AntiJoin { .. }
        | PlanOp::Union { .. }
        | PlanOp::Minus { .. }
        | PlanOp::Distinct { .. }
        | PlanOp::Reduced { .. }
        | PlanOp::Slice { .. }
        | PlanOp::Project { .. }
        | PlanOp::SubSelect { .. }
        | PlanOp::Graph { .. }
        | PlanOp::Service { .. }
        | PlanOp::Scan { .. }
        | PlanOp::Unnest { .. }
        | PlanOp::Construct { .. }
        | PlanOp::Describe { .. }
        | PlanOp::Ask { .. } => Vec::new(),
    }
}

/// Replace the subplan rooted at `root` with one node.
///
/// The new node claims every obligation the subplan claimed, which is what it
/// is: those triples and filters are the question its rows answer.
pub fn replace_subtree(plan: &mut Plan, root: NodeId, op: PlanOp) {
    let inside = subtree(plan, root);
    let mut discharges: Vec<crate::sparql_plan::ObligationId> = Vec::new();
    for id in &inside {
        discharges.extend(plan.nodes[*id].discharges.iter().copied());
    }
    discharges.sort_unstable();
    let replacement = crate::sparql_refine::Node::engine(op, discharges);

    let mut nodes = Vec::with_capacity(plan.nodes.len());
    let mut remap: Vec<Option<NodeId>> = vec![None; plan.nodes.len()];
    for (old, node) in plan.nodes.iter().enumerate() {
        if old == root {
            nodes.push(replacement.clone());
        } else if inside.contains(&old) {
            continue;
        } else {
            nodes.push(node.clone());
        }
        remap[old] = Some(nodes.len() - 1);
    }
    for node in &mut nodes {
        node.op
            .map_inputs(|input| remap[input].expect("inputs precede their node"));
    }
    plan.nodes = nodes;
    crate::sparql_rules::refresh_join_variables(plan);
}

/// Whether a `PlanOp::Graph`'s name reads the schema graph.
///
/// The name is `NamedNodePattern::to_string()` — `<iri>` for a constant, `?g`
/// for a variable (see the naive plan's `Graph` arm).
///
/// **A variable counts**, and that rests on a dataset invariant rather than on
/// optimism: this endpoint serves exactly one named graph, and the instances
/// live in the default graph (see [`crate::sparql_schema_graph`]). So
/// `GRAPH ?g { … }` can only bind `?g` to the schema graph. The probe evaluates
/// against a store shaped the same way — the schema quads in that one named
/// graph, the default graph empty — so `?g` binds there exactly as it would in
/// the engine, and the invariant is executed rather than assumed.
fn names_the_schema_graph(name: &str, schema_graph_iri: &str) -> bool {
    name.starts_with('?') || name == format!("<{schema_graph_iri}>")
}

/// The root of a maximal subplan this pass can evaluate, if there is one.
///
/// *Maximal* because a subplan's consumer, if it also qualifies, is a bigger
/// subplan with the same answer and one fewer join to leave behind.
///
/// Three conditions here are **every** pass's, which is why they are not
/// [`Materialisation::qualifies`]'s to repeat:
///
/// * the node is not already a [`PlanOp::Values`] — it is its own answer, and
///   replacing it with itself would never reach a fixpoint;
/// * it emits solutions, since a relation cannot replace a boolean or a graph;
/// * its subtree is private to it, or it is not a subtree to lift out.
pub fn materialisable_root(plan: &Plan, pass: &dyn Materialisation) -> Option<NodeId> {
    (0..plan.nodes.len()).rev().find(|&node| {
        !matches!(plan.nodes[node].op, PlanOp::Values { .. })
            && plan.nodes[node].output == crate::sparql_refine::OutputKind::Solutions
            && pass.qualifies(plan, node)
            && is_the_top_of_its_region(plan, node, pass)
            && subtree_is_private(plan, node)
            && nothing_is_still_sinking_into_it(plan, node)
    })
}

/// Whether a filter above this subplan is still on its way *into* it.
///
/// A filter that reads only what the subplan binds belongs inside it — that is
/// what `sink_filter_into_an_evaluable_side` is for — and evaluating the
/// subplan before it arrives gives an answer that is correct and useless: the
/// region without its own predicate is the whole datamodel, which materialises
/// into a relation that narrows nothing.
///
/// Both rules live in the same fixpoint and there is no ordering between them
/// to rely on, so this is asked of the plan rather than of the rule set: wait
/// while a filter that could still land here has not.
fn nothing_is_still_sinking_into_it(plan: &Plan, node: NodeId) -> bool {
    let bound = plan.variables_of(node);
    let inside = subtree(plan, node);
    !plan.nodes.iter().enumerate().any(|(id, above)| {
        let PlanOp::Filter { condition, .. } = &above.op else {
            return false;
        };
        // A filter that has already arrived is part of the subplan, not
        // something still on its way into it.
        if inside.contains(&id) || !plan.feeds(node, id) {
            return false;
        }
        let used = crate::sparql_refine::variables_used(condition);
        !used.is_empty() && used.iter().all(|name| bound.contains(name))
    })
}

/// Whether no consumer of this node also qualifies — i.e. this node is the top
/// of its region.
fn is_the_top_of_its_region(plan: &Plan, node: NodeId, pass: &dyn Materialisation) -> bool {
    !plan
        .nodes
        .iter()
        .enumerate()
        .any(|(id, other)| other.op.inputs().contains(&node) && pass.qualifies(plan, id))
}

/// Whether every node below `root` feeds `root` and nothing else.
///
/// A naive plan is a tree, so this holds; it is checked rather than assumed
/// because lifting a shared node out would silently drop the other consumer's
/// input.
fn subtree_is_private(plan: &Plan, root: NodeId) -> bool {
    let inside = subtree(plan, root);
    plan.nodes.iter().enumerate().all(|(id, node)| {
        // Reading the root is what a consumer is supposed to do -- it will read
        // the replacement instead. Reading anything *below* the root from
        // outside is what makes the subtree not a subtree, and lifting it out
        // would leave that reader without an input.
        inside.contains(&id)
            || !node
                .op
                .inputs()
                .iter()
                .any(|input| *input != root && inside.contains(input))
    })
}

/// Every node reachable from `root` through inputs, `root` included.
fn subtree(plan: &Plan, root: NodeId) -> Vec<NodeId> {
    let mut out = vec![root];
    let mut index = 0;
    while index < out.len() {
        for input in plan.nodes[out[index]].op.inputs() {
            if !out.contains(&input) {
                out.push(input);
            }
        }
        index += 1;
    }
    out
}

// ---------------------------------------------------------------------------
// Writing a subplan back as a query
// ---------------------------------------------------------------------------

/// The graph pattern this subplan is, so it can be evaluated.
///
/// The inverse of the naive plan's own walk, and partial in the one place
/// [`crate::sparql_refine::Expr::try_as_expression`] is: a filter whose
/// expression survives only as its rendering cannot be written back. Every
/// other node kind is a pattern this rebuilds exactly.
pub fn pattern_of(plan: &Plan, node: NodeId) -> Option<GraphPattern> {
    let child = |id: &NodeId| pattern_of(plan, *id).map(Box::new);
    Some(match &plan.nodes[node].op {
        PlanOp::Unit => GraphPattern::Bgp {
            patterns: Vec::new(),
        },
        PlanOp::Match { pattern } => GraphPattern::Bgp {
            patterns: vec![(**pattern).clone()],
        },
        PlanOp::Path {
            subject,
            path,
            object,
        } => GraphPattern::Path {
            subject: subject.clone(),
            path: (**path).clone(),
            object: object.clone(),
        },
        PlanOp::Values { variables, rows } => GraphPattern::Values {
            variables: variables.clone(),
            bindings: rows.clone(),
        },
        PlanOp::Join { left, right, .. } => GraphPattern::Join {
            left: child(left)?,
            right: child(right)?,
        },
        PlanOp::LeftJoin {
            left,
            right,
            condition,
            ..
        } => GraphPattern::LeftJoin {
            left: child(left)?,
            right: child(right)?,
            expression: match condition {
                Some(condition) => Some(condition.try_as_expression()?),
                None => None,
            },
        },
        // `NOT EXISTS` is a filter, not a difference: `MINUS` and `NOT EXISTS`
        // disagree when the two sides share no variable, so writing this back
        // as a `Minus` would be a different question.
        PlanOp::AntiJoin { left, right, .. } => GraphPattern::Filter {
            expr: Expression::Not(Box::new(Expression::Exists(child(right)?))),
            inner: child(left)?,
        },
        PlanOp::Union { left, right } => GraphPattern::Union {
            left: child(left)?,
            right: child(right)?,
        },
        PlanOp::Minus { left, right } => GraphPattern::Minus {
            left: child(left)?,
            right: child(right)?,
        },
        PlanOp::Filter { input, condition } => GraphPattern::Filter {
            expr: condition.try_as_expression()?,
            inner: child(input)?,
        },
        PlanOp::Bind { input, var, expr } => GraphPattern::Extend {
            inner: child(input)?,
            variable: Variable::new_unchecked(var.clone()),
            expression: expr.try_as_expression()?,
        },
        PlanOp::Group {
            input,
            keys,
            measures,
            having,
        } => {
            let mut pattern = GraphPattern::Group {
                inner: child(input)?,
                variables: keys
                    .iter()
                    .map(|k| Variable::new_unchecked(k.clone()))
                    .collect(),
                aggregates: measures
                    .iter()
                    .map(|m| (Variable::new_unchecked(m.var.clone()), m.aggregate.clone()))
                    .collect(),
            };
            // `HAVING` is a filter over the grouped rows, which is what it was
            // before the grouping rule moved it in.
            for condition in having {
                pattern = GraphPattern::Filter {
                    expr: condition.try_as_expression()?,
                    inner: Box::new(pattern),
                };
            }
            pattern
        }
        PlanOp::Sort { input, terms } => GraphPattern::OrderBy {
            inner: child(input)?,
            expression: terms
                .iter()
                .map(|term| {
                    let expr = term.expr.try_as_expression()?;
                    Some(if term.desc {
                        OrderExpression::Desc(expr)
                    } else {
                        OrderExpression::Asc(expr)
                    })
                })
                .collect::<Option<Vec<_>>>()?,
        },
        PlanOp::Distinct { input } => GraphPattern::Distinct {
            inner: child(input)?,
        },
        PlanOp::Reduced { input } => GraphPattern::Reduced {
            inner: child(input)?,
        },
        PlanOp::Slice {
            input,
            limit,
            offset,
        } => GraphPattern::Slice {
            inner: child(input)?,
            start: *offset,
            length: *limit,
        },
        PlanOp::Project { input, vars } | PlanOp::SubSelect { input, vars } => {
            GraphPattern::Project {
                inner: child(input)?,
                variables: vars
                    .iter()
                    .map(|v| Variable::new_unchecked(v.clone()))
                    .collect(),
            }
        }
        PlanOp::Graph { input, name } => GraphPattern::Graph {
            name: graph_name(name)?,
            inner: child(input)?,
        },
        // A scan or an unnest is never inside a schema-only subplan, and the
        // three query forms are roots whose output is not a relation — both
        // are refused by `evaluable_root` before this is reached, and refused
        // again here rather than approximated.
        PlanOp::Service { .. }
        | PlanOp::Scan { .. }
        | PlanOp::Unnest { .. }
        | PlanOp::Construct { .. }
        | PlanOp::Describe { .. }
        | PlanOp::Ask { .. } => return None,
    })
}

/// A `PlanOp::Graph`'s name, back as the pattern it was rendered from.
fn graph_name(name: &str) -> Option<NamedNodePattern> {
    match name.strip_prefix('?') {
        Some(variable) => Some(NamedNodePattern::Variable(Variable::new(variable).ok()?)),
        None => {
            let iri = name.strip_prefix('<')?.strip_suffix('>')?;
            Some(NamedNodePattern::NamedNode(
                spargebra::term::NamedNode::new(iri).ok()?,
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Evaluating one
// ---------------------------------------------------------------------------

/// The one materialisation pass this change adds: evaluate what depends on
/// nothing but the datamodel.
///
/// Its criterion is [`schema_only`]; its dataset is the datamodel as quads, in
/// the one named graph this endpoint serves it in — the same shape the engine
/// leg loads, so a probe answers what the engine would answer.
///
/// The store is built lazily and once. A plan with no `GRAPH` node naming the
/// schema graph never asks, so the overwhelming majority of requests pay
/// nothing; a plan that does asks once, however many subplans it has.
pub struct SchemaGraphMaterialisation<'a> {
    schema_view: &'a SchemaView,
    schema_graph_iri: &'a str,
    store: OnceCell<Option<Store>>,
}

impl<'a> SchemaGraphMaterialisation<'a> {
    pub fn new(schema_view: &'a SchemaView, schema_graph_iri: &'a str) -> Self {
        Self {
            schema_view,
            schema_graph_iri,
            store: OnceCell::new(),
        }
    }
}

impl Materialisation for SchemaGraphMaterialisation<'_> {
    fn name(&self) -> &'static str {
        "schema_graph"
    }

    fn qualifies(&self, plan: &Plan, node: NodeId) -> bool {
        schema_only(plan, node, self.schema_graph_iri)
    }

    fn dataset(&self) -> Option<&Store> {
        self.store
            .get_or_init(|| {
                let graph = crate::sparql_schema_graph::SchemaGraph::build(
                    self.schema_view,
                    self.schema_graph_iri,
                )
                .ok()?;
                let store = Store::new().ok()?;
                let name = GraphName::NamedNode(NamedNode::new(self.schema_graph_iri).ok()?);
                for quad in graph.quads {
                    store
                        .insert(&Quad::new(
                            quad.subject,
                            quad.predicate,
                            quad.object,
                            name.clone(),
                        ))
                        .ok()?;
                }
                Some(store)
            })
            .as_ref()
    }
}

/// The relation a subplan denotes, as the variables and rows a
/// [`PlanOp::Values`] holds.
///
/// Shared by every pass: the write-back, the evaluation, the blank-node refusal
/// and the row cap are properties of *replacing a subplan with a relation*, not
/// of any one criterion.
///
/// `None` when the subplan cannot be written back as a query, when the pass has
/// no dataset to answer against, when a solution carries a blank node, or when
/// there are more rows than [`MAX_MATERIALISED_ROWS`] — each of which leaves
/// the plan exactly as it was.
///
/// Duplicate rows are **kept**. SPARQL solutions are a bag and so is a `VALUES`
/// block; folding duplicates away here would change a count.
pub fn materialise(plan: &Plan, root: NodeId, pass: &dyn Materialisation) -> Option<Relation> {
    let pattern = pattern_of(plan, root)?;
    let mut variables: Vec<Variable> = Vec::new();
    pattern.on_in_scope_variable(|variable| {
        if !variables.contains(variable) {
            variables.push(variable.clone());
        }
    });
    if variables.is_empty() {
        // Nothing to bind, so nothing a relation can say that the plan does
        // not already say.
        return None;
    }
    variables.sort_by(|left, right| left.as_str().cmp(right.as_str()));

    let query = Query::Select {
        dataset: None,
        pattern: GraphPattern::Project {
            inner: Box::new(pattern),
            variables: variables.clone(),
        },
        base_iri: None,
    };
    let QueryResults::Solutions(solutions) = SparqlEvaluator::new()
        .for_query(query)
        .on_store(pass.dataset()?)
        .execute()
        .ok()?
    else {
        return None;
    };

    let mut rows: Vec<Vec<Option<GroundTerm>>> = Vec::new();
    for solution in solutions {
        let solution = solution.ok()?;
        let mut row = Vec::with_capacity(variables.len());
        for variable in &variables {
            row.push(match solution.get(variable.as_str()) {
                Some(term) => Some(ground(term)?),
                None => None,
            });
        }
        rows.push(row);
        if rows.len() > MAX_MATERIALISED_ROWS {
            return None;
        }
    }
    // A bag, so sorting changes nothing about what it denotes -- and it makes
    // the node a reader and a test can rely on, where the evaluator's solution
    // order is not something to rely on.
    rows.sort_by_key(|row| {
        row.iter()
            .map(|cell| match cell {
                Some(term) => term.to_string(),
                None => String::new(),
            })
            .collect::<Vec<_>>()
    });
    Some((variables, rows))
}

/// A solution term as a `VALUES` cell. `None` for a blank node, which a
/// `VALUES` cannot write — and which must not simply be skipped, since that
/// would shrink the relation.
fn ground(term: &OxTerm) -> Option<GroundTerm> {
    match term {
        OxTerm::NamedNode(node) => Some(GroundTerm::NamedNode(node.clone())),
        OxTerm::Literal(literal) => Some(GroundTerm::Literal(literal.clone())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use linkml_meta::SchemaDefinition;
    use std::path::Path;

    const SCHEMA_GRAPH: &str = "https://data.infrabel.be/asset360/schema";

    fn asset360_schema_view() -> SchemaView {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("data");
        let mut sv = SchemaView::new();
        for name in ["types.yaml", "rsm.yaml", "eulynx.yaml", "asset360.yaml"] {
            let yaml = std::fs::read_to_string(dir.join(name)).unwrap();
            let deser = serde_yml::Deserializer::from_str(&yaml);
            let schema: SchemaDefinition = serde_path_to_error::deserialize(deser).unwrap();
            sv.add_schema(schema).unwrap();
        }
        sv
    }

    /// The plan the criterion produces for the query of asset360 GitLab issue
    /// #409, read end to end. Three things had to happen, and each is a
    /// separate general rule:
    ///
    /// 1. the `FILTER` sank onto the side that binds `?code`, because SPARQL
    ///    puts a group's filter above the group's join;
    /// 2. the subplan below it depended on nothing outside the schema, so it
    ///    was evaluated and replaced by the one row it denotes;
    /// 3. that relation, joined to the scan, narrowed it.
    ///
    /// The lowered statement is the acceptance criterion: `signalType = 'GSA'`
    /// is the code the column stores, reached from the concept IRI the schema
    /// graph names, over nine permissible values rather than 23,503 rows.
    #[test]
    fn the_schema_side_is_evaluated_and_the_scan_is_narrowed() {
        let plan = plan_for(&format!(
            "SELECT ?s ?code WHERE {{ \
             ?s a asset360:Signal ; asset360:signalType ?t . \
             GRAPH <{SCHEMA_GRAPH}> {{ ?t skos:notation ?code }} \
             FILTER(CONTAINS(?code, \"GS\")) }}"
        ));
        assert!(
            plan.contains("filter    signalType = 'GSA'"),
            "the fetch must be narrowed to the code the schema filter permits:\n{plan}"
        );
    }

    /// The same query with no schema graph configured: no rule exists, nothing
    /// is evaluated, and the fetch is what it was before any of this.
    #[test]
    fn without_a_schema_graph_nothing_is_evaluated() {
        let schema_view = asset360_schema_view();
        let plan = crate::sparql_plan::plan_query_refined_with_schema_graph(
            &query(&format!(
                "SELECT ?s ?code WHERE {{ \
                 ?s a asset360:Signal ; asset360:signalType ?t . \
                 GRAPH <{SCHEMA_GRAPH}> {{ ?t skos:notation ?code }} \
                 FILTER(CONTAINS(?code, \"GS\")) }}"
            )),
            &schema_view,
            None,
        )
        .expect("the query scopes");
        assert!(
            !format!("{plan}").contains("filter    signalType"),
            "{plan}"
        );
    }

    /// Nothing here is about enums, and nothing about `skos:notation`. The
    /// same shape over `rdfs:label`, and over a *join inside the schema graph*
    /// with no filter at all, narrows the same way -- because the criterion
    /// asks what the subplan depends on and not what it says.
    #[test]
    fn the_criterion_is_not_a_predicate_whitelist() {
        let by_label = plan_for(&format!(
            "SELECT ?s WHERE {{ \
             ?s a asset360:Signal ; asset360:signalType ?t . \
             GRAPH <{SCHEMA_GRAPH}> {{ ?t rdfs:label ?l }} \
             FILTER(STRSTARTS(?l, \"VSS\")) }}"
        ));
        assert!(
            by_label.contains("signalType IN ('VSS', 'VSS_HO', 'VSS_LO')"),
            "three of the nine values start with VSS:\n{by_label}"
        );

        // Two schema triples joined to each other, and the scheme is the
        // selective predicate. An enum-aware rewrite of `skos:notation` would
        // not have seen this at all.
        let by_scheme = plan_for(&format!(
            "SELECT ?s WHERE {{ \
             ?s a asset360:Signal ; asset360:signalType ?t . \
             GRAPH <{SCHEMA_GRAPH}> {{ \
             ?t skos:inScheme ?scheme . ?scheme rdfs:label \"SignalTypes\" }} }}"
        ));
        assert!(
            by_scheme.contains("signalType IN ('GSA', 'KSS', 'OTHER'"),
            "the nine values of SignalTypes and no others:\n{by_scheme}"
        );
    }

    /// The shapes the previous design had to exclude one by one, and which the
    /// criterion does not distinguish at all: replacing a subplan by the
    /// relation it denotes is an equivalence, so it holds wherever the subplan
    /// sits.
    ///
    /// Each of these is asserted on the *answer-preserving* side rather than on
    /// the narrowing: under an `OPTIONAL` the narrowing must **not** reach the
    /// scan -- the rows whose value the schema does not describe are answers --
    /// while the evaluation itself is still sound and still happens.
    #[test]
    fn an_optional_schema_subplan_is_evaluated_but_narrows_nothing() {
        let plan = plan_for(&format!(
            "SELECT ?s ?code WHERE {{ \
             ?s a asset360:Signal . \
             OPTIONAL {{ ?s asset360:signalType ?t . \
             GRAPH <{SCHEMA_GRAPH}> {{ ?t skos:notation ?code }} \
             FILTER(CONTAINS(?code, \"GS\")) }} }}"
        ));
        assert!(
            !plan.contains("filter    signalType"),
            "an optional match must not narrow the fetch:\n{plan}"
        );
    }

    /// A `UNION` arm is a subplan like any other: the rule never looks at the
    /// union, and the arm carrying the schema filter is evaluated and replaced
    /// exactly as it would be on its own. This is the composability claim,
    /// executed.
    ///
    /// Asserted on the *refined* plan and not on the statement, because
    /// turning a narrowed arm into a narrowed statement is the per-arm
    /// lowering of asset360 GitLab issue #410, which is a separate change on a
    /// separate branch. On this base the two arms still lower to one wide
    /// fetch; once that lands the condition rides along with no change here,
    /// which is the whole point of the two changes being independent.
    #[test]
    fn a_union_arm_narrows_on_its_own() {
        let plan = refined_for(&format!(
            "SELECT ?s WHERE {{ \
             {{ ?s a asset360:Signal ; asset360:signalType ?t . \
                GRAPH <{SCHEMA_GRAPH}> {{ ?t skos:notation ?code }} \
                FILTER(CONTAINS(?code, \"GS\")) }} \
             UNION \
             {{ ?s a asset360:BaliseGroup }} }}"
        ));
        assert!(
            plan.contains("values    ?code ?t"),
            "the arm's schema subplan is evaluated in place:\n{plan}"
        );
        assert!(
            plan.contains("match     ?s a asset360:BaliseGroup"),
            "the other arm is untouched:\n{plan}"
        );
    }

    /// **The test that says the structure is right rather than asserting it:**
    /// a second materialisation, defined entirely in this test module, running
    /// through the same rule, touching nothing in the pass that already exists.
    ///
    /// Its criterion is different from the schema pass's — "reads nothing at
    /// all" rather than "reads only the schema graph" — and its dataset is an
    /// empty store, which is what "nothing to read" *is*. Everything else, the
    /// maximal-subplan search and the write-back and the row cap and the
    /// replacement, it inherits.
    ///
    /// If adding this had needed an edit to `SchemaGraphMaterialisation`, the
    /// design would have the same defect as the version before it: a
    /// mechanism that is general in the prose and specific in the code.
    #[test]
    fn a_second_pass_needs_no_edit_to_the_first() {
        let mut plan = crate::sparql_refine::naive_plan_of(
            "SELECT ?x ?y WHERE { VALUES ?x { 1 2 } VALUES ?y { 3 } }",
        )
        .expect("a naive plan");
        let rule = crate::sparql_rules::Materialise::new(ReadsNothing::default());
        crate::sparql_rules::refine(&mut plan, &[&rule]).expect("every invariant holds");

        let printed = format!("{plan}");
        assert_eq!(plan.find("values").len(), 1, "{printed}");
        assert!(printed.contains("values    ?x ?y × 2 row(s)"), "{printed}");
    }

    /// Two regions that qualify independently are both materialised. The order
    /// between them is immaterial because replacing a subplan by the relation
    /// it denotes is an equivalence and the two are disjoint — that is an
    /// argument, and this is the part of it that can be run.
    #[test]
    fn two_independent_regions_are_both_materialised() {
        let plan = refined_for(&format!(
            "SELECT ?s WHERE {{ \
             ?s a asset360:Signal ; asset360:signalType ?t ; asset360:regime ?r . \
             GRAPH <{SCHEMA_GRAPH}> {{ ?t skos:notation ?tc }} \
             GRAPH <{SCHEMA_GRAPH}> {{ ?r skos:notation ?rc }} \
             FILTER(CONTAINS(?tc, \"GS\")) FILTER(CONTAINS(?rc, \"VNS\")) }}"
        ));
        assert_eq!(
            plan.matches("values").count(),
            2,
            "both regions are evaluated:\n{plan}"
        );
        let statement = plan_for(&format!(
            "SELECT ?s WHERE {{ \
             ?s a asset360:Signal ; asset360:signalType ?t ; asset360:regime ?r . \
             GRAPH <{SCHEMA_GRAPH}> {{ ?t skos:notation ?tc }} \
             GRAPH <{SCHEMA_GRAPH}> {{ ?r skos:notation ?rc }} \
             FILTER(CONTAINS(?tc, \"GS\")) FILTER(CONTAINS(?rc, \"VNS\")) }}"
        ));
        assert!(statement.contains("signalType = 'GSA'"), "{statement}");
        assert!(statement.contains("regime = 'VNS'"), "{statement}");
    }

    /// A materialisation over a subplan that reads nothing at all. Deliberately
    /// trivial: the point is that a criterion and a dataset are the whole of a
    /// pass.
    #[derive(Default)]
    struct ReadsNothing {
        store: OnceCell<Option<Store>>,
    }

    impl Materialisation for ReadsNothing {
        fn name(&self) -> &'static str {
            "reads_nothing"
        }

        fn qualifies(&self, plan: &Plan, node: NodeId) -> bool {
            fn walk(plan: &Plan, node: NodeId) -> bool {
                match &plan.nodes[node].op {
                    PlanOp::Values { .. } | PlanOp::Unit => true,
                    PlanOp::Match { .. }
                    | PlanOp::Path { .. }
                    | PlanOp::Graph { .. }
                    | PlanOp::Service { .. }
                    | PlanOp::Scan { .. }
                    | PlanOp::Unnest { .. } => false,
                    other => other.inputs().iter().all(|input| walk(plan, *input)),
                }
            }
            walk(plan, node) && no_free_variables(plan, node)
        }

        fn dataset(&self) -> Option<&Store> {
            // An empty store: "nothing to read" is a dataset, not a refusal.
            self.store.get_or_init(|| Store::new().ok()).as_ref()
        }
    }

    /// A `GRAPH` naming a variable is the schema graph, because this endpoint
    /// serves exactly one named graph -- and the probe binds `?g` to it rather
    /// than assuming nobody looks.
    #[test]
    fn a_graph_naming_a_variable_is_the_schema_graph() {
        let plan = plan_for(&format!(
            "SELECT ?s ?g WHERE {{ \
             ?s a asset360:Signal ; asset360:signalType ?t . \
             GRAPH ?g {{ ?t skos:notation ?code }} \
             FILTER(CONTAINS(?code, \"GS\")) }}"
        ));
        assert!(plan.contains("signalType = 'GSA'"), "{plan}");
        let _ = SCHEMA_GRAPH;
    }

    /// The dataset invariant the case above rests on, asserted rather than
    /// described: the endpoint puts the datamodel in one named graph and the
    /// instances in the default one, so a variable graph can bind to nothing
    /// else. If a second named graph is ever served, this fails and
    /// `names_the_schema_graph` has to be narrowed with it.
    #[test]
    fn the_endpoint_serves_exactly_one_named_graph() {
        let graph =
            crate::sparql_schema_graph::SchemaGraph::build(&asset360_schema_view(), SCHEMA_GRAPH)
                .unwrap();
        let names: std::collections::BTreeSet<String> = graph
            .quads
            .iter()
            .map(|quad| quad.graph_name.to_string())
            .collect();
        assert_eq!(names.len(), 1, "{names:?}");
    }

    /// A predicate no permissible value satisfies answers nothing, and the
    /// plan says so: an empty relation is representable, and it is the
    /// *correct* plan rather than a shape to decline.
    #[test]
    fn a_schema_predicate_matching_nothing_becomes_an_empty_relation() {
        let plan = refined_for(&format!(
            "SELECT ?s WHERE {{ \
             ?s a asset360:Signal ; asset360:signalType ?t . \
             GRAPH <{SCHEMA_GRAPH}> {{ ?t skos:notation ?code }} \
             FILTER(CONTAINS(?code, \"no such code\")) }}"
        ));
        assert!(plan.contains("values"), "{plan}");
        assert!(plan.contains("0 row(s)"), "{plan}");
    }

    /// A filter that reads an instance variable is a dependency on something
    /// outside the schema, so the subplan under it is not closed and the filter
    /// is not evaluated with that variable unbound -- which would be an error,
    /// which would exclude every solution, which would answer nothing for a
    /// query that has an answer.
    #[test]
    fn a_filter_reading_the_instance_side_is_not_evaluated() {
        let plan = refined_for(&format!(
            "SELECT ?s WHERE {{ \
             ?s a asset360:Signal ; asset360:signalType ?t ; asset360:NationalUniqueID ?id . \
             GRAPH <{SCHEMA_GRAPH}> {{ ?t skos:notation ?code }} \
             FILTER(CONTAINS(?code, ?id)) }}"
        ));
        assert!(
            plan.contains("filter    CONTAINS(?code, ?id)"),
            "the filter stays where the engine can evaluate it:\n{plan}"
        );
    }

    /// An instance query pays nothing: no `GRAPH` node names the schema graph,
    /// so no subplan qualifies and the store is never built.
    #[test]
    fn an_instance_query_is_untouched() {
        let plan = refined_for("SELECT ?s WHERE { ?s a asset360:Signal }");
        assert!(!plan.contains("values"), "{plan}");
    }

    fn query(body: &str) -> String {
        format!(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \
             PREFIX skos: <http://www.w3.org/2004/02/skos/core#> {body}"
        )
    }

    /// The lowered execution plan, which is what the endpoint renders.
    fn plan_for(body: &str) -> String {
        let schema_view = asset360_schema_view();
        let plan = crate::sparql_plan::plan_query_refined_with_schema_graph(
            &query(body),
            &schema_view,
            Some(SCHEMA_GRAPH),
        )
        .expect("the query scopes");
        format!("{plan}")
    }

    /// The refined plan, for the cases that are about a node rather than about
    /// the statement.
    fn refined_for(body: &str) -> String {
        crate::sparql_plan::refined_plan_text(
            &query(body),
            &asset360_schema_view(),
            Some(SCHEMA_GRAPH),
        )
        .expect("the query refines")
    }
}
