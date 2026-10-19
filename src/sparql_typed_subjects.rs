//! Typed subjects: the most-specific-type spelling is the direct type.
//!
//! One rule, [`MostSpecificTypeIsTheDirectType`].
//!
//! # The spelling
//!
//! ```sparql
//! ?s a ?t
//! FILTER NOT EXISTS { ?s a ?sub . GRAPH <G> { ?sub rdfs:subClassOf+ ?t } }
//! ```
//!
//! "the types of `?s` that no other type of `?s` is below" -- the most specific
//! type. Written that way the engine evaluates a transitive path over the whole
//! schema once per subject. Under an inferred regime the data already says it:
//! `?s sesame:directType ?t`, one triple. This rule recognises the spelling,
//! proves the two agree under the stated conditions, and says the short form.
//! It is the plan-level counterpart of the `sesame:directType` fold into a
//! scan (`PlanOp::Scan::direct_type`), which then reads the `asset_type`
//! column.
//!
//! # Proof
//!
//! Fix a subject `s` whose record is of concrete class `c`, and let `T(c) =
//! rdf_types(c, r)` be the set of its `rdf:type` objects under the data regime
//! `r` the two `a` matches share (they must be in the same regime: otherwise
//! `?t` and `?sub` range over different sets). Let `E` be the closure of the
//! `rdfs:subClassOf` edges `G` publishes and `E+` its transitive closure, which
//! is what `rdfs:subClassOf+` over `G` enumerates. `?t` is kept for `s` exactly
//! when `t ∈ T(c)` and no `sub ∈ T(c)` has `(sub, t) ∈ E+`. Claim: under the
//! conditions below that holds exactly for `t = c`, which is
//! `s sesame:directType c`.
//!
//! Conditions, each with the wrong answer it prevents:
//!
//! 1. **`G` is a schema graph of known regime `g`** ([`DatasetSpec::schema_regime`]).
//!    For any other graph nothing is known about its edges.
//! 2. **`G` covers what the data regime uses**: for every class `c` and every
//!    `t ∈ T(c)`, `t ≠ c`: `(c, t) ∈ E+`. Without it a type of `s` can lack the
//!    edge that would exclude it, and the NOT EXISTS keeps a non-minimal type
//!    that `directType` does not name. (Asserted schema under mixin data fails
//!    this; asserted schema under is-a data passes, since the transitive closure
//!    of the direct `is_a` edges is the is-a ancestry.)
//! 3. **`E+` is irreflexive**: no `(c, c)`. `subclass_edges` never emits a self
//!    edge; a cycle in the schema would still make `E+` reflexive, which is why
//!    the closure is checked and not assumed.
//!
//! *`t = c` is kept.* Suppose `sub ∈ T(c)` with `(sub, c) ∈ E+`. If `sub = c`,
//! `(c, c) ∈ E+`, contradicting 3. Otherwise `(c, sub) ∈ E+` by 2, and with
//! `(sub, c) ∈ E+` that is `(c, c) ∈ E+`, contradicting 3.
//!
//! *`t ≠ c` is dropped.* `c ∈ T(c)` (the `Own` partition is in every regime),
//! and `(c, t) ∈ E+` by 2, so `sub = c` witnesses the NOT EXISTS.
//!
//! The multiplicity is one on both sides: the data graph is a set of triples, so
//! `?s a ?t` has one row per distinct `t`, and `directType` one row per `s`.
//!
//! **Under the asserted regime** `T(c) = {c}` and `directType` is not published,
//! so the rewrite is the *removal* of the NOT EXISTS, leaving `?s a ?t`: the only
//! candidate `sub` is `c = t`, and `(c, c) ∉ E+` by 3. Conditions 1 and 3 apply;
//! 2 is vacuous.
//!
//! **Shape.** The NOT EXISTS is a `PlanOp::AntiJoin` whose right side is a join
//! of `?s a ?sub` with `GRAPH <G> { ?sub rdfs:subClassOf+ ?t }`, and whose left
//! side is the `?s a ?t` match, or a tree of inner joins with that match as a
//! leaf. The predicate on a left row depends only on its `(s, t)`, which the
//! match binds in every row, so filtering the match before the join is the same
//! as filtering after it, multiplicities included. Every other shape is left
//! alone.
//!
//! **Precondition: the body's other variables are private to it.** The claim
//! that the predicate depends only on `(s, t)` holds only if `?sub` is bound by
//! nothing outside the NOT EXISTS. If the left side binds it (say
//! `VALUES ?sub { :Asset }`), the NOT EXISTS tests one fixed `sub`, not "some
//! type of `s`", and it no longer computes the most specific type. So the rule
//! refuses when anything in the *whole* left subtree binds a body variable
//! other than `?s` and `?t`, and when the body is anything but the two
//! patterns (no extra triple or filter).
//!
//! **One spelling, on purpose.** Only this exact shape is recognised: `?s a
//! ?t` against `?s a ?sub . GRAPH <G> { ?sub rdfs:subClassOf+ ?t }` in a NOT
//! EXISTS. Equivalent spellings (`FILTER NOT EXISTS` with the patterns
//! reordered inside other shapes, `MINUS`, `rdfs:subClassOf*`, an `OPTIONAL` +
//! `!BOUND`, ...) are not rewritten and the engine answers them as written:
//! each would need its own proof, and a recogniser that guesses is how a
//! rewrite changes answers.
//!
//! # The dataset
//!
//! The one question this rule asks the dataset is *what regime does the schema
//! graph with this IRI publish?* ([`DatasetSpec::schema_regime`]). An IRI the
//! dataset does not serve as a schema graph leaves the plan alone.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use linkml_runtime::schema_rdf::{RDFS_SUBCLASS_OF, TypeRegime, rdf_types, subclass_edges};
use linkml_schemaview::schemaview::SchemaView;
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::{NamedNode, NamedNodePattern, TermPattern};

use crate::sparql_dataset::DatasetSpec;
use crate::sparql_refine::{
    Executor, Node, NodeId, Plan, PlanOp, is_type_pattern, object_variable, subject_variable,
};
use crate::sparql_rules::{Rule, consumers_of};

// ---------------------------------------------------------------------------
// Edge arithmetic
// ---------------------------------------------------------------------------

fn class_key(class: &linkml_schemaview::classview::ClassView) -> String {
    class.canonical_uri().to_string()
}

/// `E+`: the transitive closure of the `rdfs:subClassOf` edges a schema graph
/// of regime `regime` publishes, as `(sub, super)` pairs of canonical class
/// IRIs. `None` when the schema cannot enumerate its edges.
fn subclass_closure(sv: &SchemaView, regime: TypeRegime) -> Option<HashSet<(String, String)>> {
    let mut above: HashMap<String, Vec<String>> = HashMap::new();
    for (sub, sup) in subclass_edges(sv, regime).ok()? {
        above
            .entry(class_key(&sub))
            .or_default()
            .push(class_key(&sup));
    }
    let mut closure = HashSet::new();
    for start in above.keys() {
        let mut seen: HashSet<&String> = HashSet::new();
        let mut queue: VecDeque<&String> = above[start].iter().collect();
        while let Some(next) = queue.pop_front() {
            if !seen.insert(next) {
                continue;
            }
            closure.insert((start.clone(), next.clone()));
            queue.extend(above.get(next).into_iter().flatten());
        }
    }
    Some(closure)
}

/// Conditions 2 and 3 of the proof: whether a schema graph of regime
/// `graph_regime` makes "no type of `s` lies below `t`" the same as "`t` is
/// `s`'s direct type" for data of regime `data_regime`.
fn most_specific_is_direct(
    sv: &SchemaView,
    graph_regime: TypeRegime,
    data_regime: TypeRegime,
) -> bool {
    let Some(closure) = subclass_closure(sv, graph_regime) else {
        return false;
    };
    // 3: irreflexive.
    if closure.iter().any(|(sub, sup)| sub == sup) {
        return false;
    }
    if data_regime == TypeRegime::Asserted {
        return true;
    }
    // 2: every type the data regime writes for a class is above it in `E+`.
    let Ok(classes) = sv.class_views() else {
        return false;
    };
    classes.iter().all(|class| {
        let Ok(types) = rdf_types(class, data_regime) else {
            return false;
        };
        let own = class_key(class);
        types
            .iter()
            .map(class_key)
            .filter(|t| *t != own)
            .all(|t| closure.contains(&(own.clone(), t)))
    })
}

// ---------------------------------------------------------------------------
// The rule
// ---------------------------------------------------------------------------

/// `?s a ?t FILTER NOT EXISTS { ?s a ?sub . GRAPH <G> { ?sub rdfs:subClassOf+ ?t } }`
/// is `?s sesame:directType ?t`. See the module documentation for the proof and
/// the conditions.
pub struct MostSpecificTypeIsTheDirectType<'s> {
    schema: &'s SchemaView,
    dataset: &'s DatasetSpec,
}

impl<'s> MostSpecificTypeIsTheDirectType<'s> {
    pub fn new(schema: &'s SchemaView, dataset: &'s DatasetSpec) -> Self {
        Self { schema, dataset }
    }
}

/// A match `?subject a ?object`, both variables, in a data graph.
fn typed_pair(plan: &Plan, id: NodeId) -> Option<(String, String, TypeRegime)> {
    let PlanOp::Match { pattern, reads } = &plan.nodes[id].op else {
        return None;
    };
    if plan.nodes[id].executor != Executor::Engine || !is_type_pattern(pattern) {
        return None;
    }
    Some((
        subject_variable(pattern)?.to_owned(),
        object_variable(pattern)?.to_owned(),
        reads.data_regime()?,
    ))
}

/// `GRAPH <iri> { ?sub rdfs:subClassOf+ ?t }`: the graph IRI and the two
/// variables.
fn subclass_plus(plan: &Plan, id: NodeId) -> Option<(String, String, String, NodeId)> {
    let PlanOp::Graph { input, name } = &plan.nodes[id].op else {
        return None;
    };
    let iri = name.strip_prefix('<')?.strip_suffix('>')?;
    let PlanOp::Path {
        subject: TermPattern::Variable(sub),
        path,
        object: TermPattern::Variable(sup),
    } = &plan.nodes[*input].op
    else {
        return None;
    };
    let PropertyPathExpression::OneOrMore(step) = path.as_ref() else {
        return None;
    };
    let PropertyPathExpression::NamedNode(step) = step.as_ref() else {
        return None;
    };
    (step.as_str() == RDFS_SUBCLASS_OF).then(|| {
        (
            iri.to_owned(),
            sub.as_str().to_owned(),
            sup.as_str().to_owned(),
            *input,
        )
    })
}

/// Every node at or below `root`.
fn subtree(plan: &Plan, root: NodeId) -> BTreeSet<NodeId> {
    let mut out = BTreeSet::from([root]);
    let mut stack = vec![root];
    while let Some(id) = stack.pop() {
        for input in plan.nodes[id].op.inputs() {
            if out.insert(input) {
                stack.push(input);
            }
        }
    }
    out
}

/// What the rule matched.
struct Found {
    anti: NodeId,
    /// The left side of the anti-join.
    kept: NodeId,
    /// The `?s a ?t` match.
    matched: NodeId,
    /// The NOT EXISTS body, which goes.
    body: BTreeSet<NodeId>,
    regime: TypeRegime,
    graph_iri: String,
}

impl MostSpecificTypeIsTheDirectType<'_> {
    fn recognise(&self, plan: &Plan, anti: NodeId) -> Option<Found> {
        let PlanOp::AntiJoin {
            left,
            right,
            reference: None,
        } = &plan.nodes[anti].op
        else {
            return None;
        };
        let (kept, right) = (*left, *right);
        // The body: `?s a ?sub` joined with the graph-scoped path.
        let PlanOp::Join {
            left: a, right: b, ..
        } = &plan.nodes[right].op
        else {
            return None;
        };
        let (sub_match, graph) = if subclass_plus(plan, *b).is_some() {
            (*a, *b)
        } else {
            (*b, *a)
        };
        let (graph_iri, path_sub, path_sup, _) = subclass_plus(plan, graph)?;
        let (s, sub, sub_regime) = typed_pair(plan, sub_match)?;
        if sub != path_sub {
            return None;
        }
        let body = subtree(plan, right);
        // A private, still-engine subtree: nothing else reads it, and no rule
        // has pushed any of it.
        for id in &body {
            if plan.nodes[*id].executor != Executor::Engine {
                return None;
            }
            if *id != right && consumers_of(plan, *id).iter().any(|c| !body.contains(c)) {
                return None;
            }
        }
        if consumers_of(plan, right) != [anti] {
            return None;
        }
        // The left side: the `?s a ?t` match, below inner joins only.
        let mut candidates = Vec::new();
        let mut spine = vec![kept];
        let mut stack = vec![kept];
        while let Some(id) = stack.pop() {
            match &plan.nodes[id].op {
                PlanOp::Join {
                    left,
                    right,
                    reference: None,
                    key: None,
                    ..
                } if plan.nodes[id].executor == Executor::Engine => {
                    for side in [*left, *right] {
                        spine.push(side);
                        stack.push(side);
                    }
                }
                PlanOp::Match { .. } => {
                    if let Some((subject, object, regime)) = typed_pair(plan, id)
                        && subject == s
                        && object == path_sup
                    {
                        candidates.push((id, regime));
                    }
                }
                _ => {}
            }
        }
        let [(matched, regime)] = candidates.as_slice() else {
            return None;
        };
        // The spine is a tree: nothing outside it reads a node of it, so
        // editing the leaf edits one place.
        for id in &spine {
            let readers = consumers_of(plan, *id);
            let expected = if *id == kept { vec![anti] } else { Vec::new() };
            if *id != kept && readers.len() != 1 {
                return None;
            }
            if *id == kept && readers != expected {
                return None;
            }
        }
        // Three distinct variables, and the same data regime on both matches.
        if s == sub || s == path_sup || sub == path_sup || sub_regime != *regime {
            return None;
        }
        // The body is exactly the four nodes the spelling names (the join,
        // the `?s a ?sub` match, the graph and its path): no extra triple or
        // filter hides in it.
        if body.len() != 4 {
            return None;
        }
        // Precondition of the proof: the filter depends on `(?s, ?t)` alone.
        // The body's other variables (`?sub`) must be private to it, so
        // nothing in the whole left subtree -- a VALUES, another pattern, a
        // BIND -- may bind any variable of the body besides `?s` and `?t`.
        let bound_left = plan.variables_of(kept);
        let body_vars = plan.variables_of(right);
        if body_vars
            .iter()
            .any(|v| *v != s && *v != path_sup && bound_left.contains(v))
        {
            return None;
        }
        Some(Found {
            anti,
            kept,
            matched: *matched,
            body,
            regime: *regime,
            graph_iri,
        })
    }
}

impl Rule for MostSpecificTypeIsTheDirectType<'_> {
    fn name(&self) -> &'static str {
        "most_specific_type_is_the_direct_type"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for anti in 0..plan.nodes.len() {
            let Some(found) = self.recognise(plan, anti) else {
                continue;
            };
            let Some(graph_regime) = self.dataset.schema_regime(&found.graph_iri) else {
                continue;
            };
            if !most_specific_is_direct(self.schema, graph_regime, found.regime) {
                continue;
            }
            rewrite(plan, &found);
            return true;
        }
        false
    }
}

/// Apply the rewrite: the match becomes `directType` (inferred regimes) or
/// stays as it is (asserted), the anti-join and its body go, and the match
/// takes over their obligations.
fn rewrite(plan: &mut Plan, found: &Found) {
    let mut claims = plan.nodes[found.matched].discharges.clone();
    for id in found.body.iter().chain([&found.anti]) {
        claims.extend(plan.nodes[*id].discharges.iter().copied());
    }
    claims.sort_unstable();
    claims.dedup();

    let mut nodes: Vec<Node> = Vec::with_capacity(plan.nodes.len());
    let mut remap: Vec<Option<NodeId>> = vec![None; plan.nodes.len()];
    let mut matched_at = None;
    for (old, node) in plan.nodes.iter().enumerate() {
        if old == found.anti || found.body.contains(&old) {
            continue;
        }
        let mut node = node.clone();
        if old == found.matched {
            node.discharges = claims.clone();
            if found.regime != TypeRegime::Asserted
                && let PlanOp::Match { pattern, .. } = &mut node.op
            {
                pattern.predicate = NamedNodePattern::NamedNode(NamedNode::new_unchecked(
                    linkml_runtime::schema_rdf::SESAME_DIRECT_TYPE,
                ));
            }
        }
        nodes.push(node);
        remap[old] = Some(nodes.len() - 1);
        if old == found.matched {
            matched_at = remap[old];
        }
    }
    // What read the anti-join reads its left side now; what was inside its
    // body is retired to the match that took its obligations over.
    remap[found.anti] = remap[found.kept];
    for id in &found.body {
        remap[*id] = matched_at;
    }
    for node in &mut nodes {
        node.op
            .map_inputs(|input| remap[input].expect("inputs precede their node"));
    }
    plan.rebuild(nodes, &remap);
}

#[cfg(all(test, feature = "sparql-endpoint"))]
mod tests {
    use super::*;
    use crate::sparql_rules::refine;
    use linkml_meta::SchemaDefinition;

    /// `Root <- Mid <- Leaf` by `is_a`, and `Leaf` also mixes in `Tag`: a
    /// chain, so the is-a closure matters, and a mixin, so the regimes differ.
    const SCHEMA: &str = "
id: https://example.org/typed
name: typed
prefixes:
  ex: https://example.org/typed/
default_prefix: ex
imports:
  - linkml:types
default_range: string
classes:
  Root:
    attributes:
      name: {range: string}
  Mid:
    is_a: Root
  Leaf:
    is_a: Mid
    mixins: [Tag]
  Tag:
    mixin: true
";

    fn schema_view() -> SchemaView {
        let mut sv = SchemaView::new();
        let types: SchemaDefinition = serde_yml::from_str(include_str!("../tests/data/types.yaml"))
            .expect("types schema parses");
        sv.add_schema(types).unwrap();
        let schema: SchemaDefinition = serde_yml::from_str(SCHEMA).expect("schema parses");
        sv.add_schema(schema).unwrap();
        sv
    }

    const G: &str = "https://example.org/schema";

    /// The canonical spelling, with `data` the regime of the data graph.
    fn refined(sv: &SchemaView, dataset: &DatasetSpec, spelling: &str) -> (bool, String) {
        let query = format!("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> {spelling}");
        let parsed = crate::sparql_scoper::parse_query_for(&query, sv).expect("parses");
        let mut plan = crate::sparql_refine::naive_plan(&parsed, dataset).expect("plans");
        // The graphs the query can see, as production hands the rules.
        let visible = dataset.seen_through(crate::sparql_scoper::dataset_of(&parsed));
        let rule = MostSpecificTypeIsTheDirectType::new(sv, &visible);
        let rules: [&dyn Rule; 1] = [&rule];
        let log = refine(&mut plan, &rules).expect("every invariant holds");
        (!log.applied.is_empty(), plan.to_string())
    }

    fn canonical(graph: &str) -> String {
        format!(
            "SELECT ?s ?t WHERE {{ ?s a ?t \
             FILTER NOT EXISTS {{ ?s a ?sub . \
             GRAPH <{graph}> {{ ?sub rdfs:subClassOf+ ?t }} }} }}"
        )
    }

    /// A dataset whose default graph is data of regime `data` and which serves
    /// `G` as a schema graph publishing `graph`.
    fn serving(data: TypeRegime, graph: TypeRegime) -> DatasetSpec {
        DatasetSpec::builder(data)
            .with_schema_graph(G, graph)
            .build()
            .expect("a valid dataset")
    }

    #[test]
    fn the_canonical_spelling_under_is_a_data_is_the_direct_type() {
        let sv = schema_view();
        for graph in [
            TypeRegime::IsA,
            TypeRegime::IsAAndMixins,
            TypeRegime::Asserted,
        ] {
            let (fired, plan) = refined(&sv, &serving(TypeRegime::IsA, graph), &canonical(G));
            assert!(fired, "{graph:?}:\n{plan}");
            assert!(plan.contains("directType"), "{graph:?}:\n{plan}");
        }
        let (fired, plan) = refined(
            &sv,
            &serving(TypeRegime::IsA, TypeRegime::IsA),
            &canonical(G),
        );
        assert!(fired, "{plan}");
        assert!(
            plan.contains("directType"),
            "the plan names the direct type:\n{plan}"
        );
        assert!(!plan.contains("antijoin"), "{plan}");
        // The ledger stayed balanced: `refine` checked it.
    }

    /// A `FROM` without a `FROM NAMED` leaves no named graph (SPARQL 1.1
    /// 13.2), so the schema graph inside the NOT EXISTS is invisible and the
    /// subclass test is empty: the rule has nothing to prove and declines. It
    /// fires once the clause names the schema graph.
    #[test]
    fn a_from_clause_hides_the_schema_graph_and_the_rule_declines() {
        const A: &str = "urn:graph:a";
        let sv = schema_view();
        let dataset = DatasetSpec::builder(TypeRegime::IsA)
            .with_data_graph(A, TypeRegime::IsA)
            .with_default_graph(A)
            .with_schema_graph(G, TypeRegime::IsA)
            .build()
            .expect("a valid dataset");
        let with_clause = |clause: &str| {
            canonical(G).replace(
                "SELECT ?s ?t WHERE",
                &format!("SELECT ?s ?t {clause} WHERE"),
            )
        };
        let (fired, plan) = refined(&sv, &dataset, &with_clause(&format!("FROM <{A}>")));
        assert!(!fired && !plan.contains("directType"), "{plan}");
        let (fired, plan) = refined(
            &sv,
            &dataset,
            &with_clause(&format!("FROM <{A}> FROM NAMED <{G}>")),
        );
        assert!(fired && plan.contains("directType"), "{plan}");
        // The same for `FROM NAMED` of some other graph.
        let (fired, _) = refined(
            &sv,
            &dataset,
            &with_clause(&format!("FROM <{A}> FROM NAMED <urn:other>")),
        );
        assert!(!fired);
    }

    /// Asserted data has no `directType`: the NOT EXISTS is simply true, and
    /// what is left is `?s a ?t`.
    #[test]
    fn under_asserted_data_the_not_exists_is_dropped() {
        let sv = schema_view();
        let (fired, plan) = refined(
            &sv,
            &serving(TypeRegime::Asserted, TypeRegime::Asserted),
            &canonical(G),
        );
        assert!(fired, "{plan}");
        assert!(
            !plan.contains("directType") && !plan.contains("antijoin"),
            "{plan}"
        );
        assert!(plan.contains("?s a ?t"), "{plan}");
    }

    /// Mixin data over an is-a-only schema graph: `Tag` is a type of a `Leaf`
    /// that no edge of the graph puts above `Leaf`, so the NOT EXISTS would keep
    /// it and `directType` would not.
    #[test]
    fn a_schema_graph_missing_the_mixin_edges_is_left_alone() {
        let sv = schema_view();
        let (fired, plan) = refined(
            &sv,
            &serving(TypeRegime::IsAAndMixins, TypeRegime::IsA),
            &canonical(G),
        );
        assert!(!fired, "{plan}");
        assert!(plan.contains("antijoin"), "{plan}");
    }

    /// The anti-join sits above a join with another read: the rewrite edits
    /// the match inside the join and nothing else.
    #[test]
    fn the_match_may_sit_in_a_join() {
        let sv = schema_view();
        let (fired, plan) = refined(
            &sv,
            &serving(TypeRegime::IsA, TypeRegime::IsA),
            &format!(
                "SELECT ?s ?t ?n WHERE {{ ?s a ?t . ?s <https://example.org/typed/name> ?n \
                 FILTER NOT EXISTS {{ ?s a ?sub . GRAPH <{G}> {{ ?sub rdfs:subClassOf+ ?t }} }} }}"
            ),
        );
        assert!(fired, "{plan}");
        assert!(
            plan.contains("directType") && plan.contains("name"),
            "{plan}"
        );
    }

    /// F2 of the rules review: `?sub` bound outside the NOT EXISTS makes the
    /// filter depend on more than `(?s, ?t)`. With `?sub` fixed to `Root`, only
    /// the types below `Root` are dropped, so the filter is not "most specific
    /// type" and the rewrite would change the answer. Any variable of the body
    /// the left side binds, whatever binds it, refuses the rewrite.
    #[test]
    fn a_body_variable_bound_on_the_left_leaves_the_plan_alone() {
        let sv = schema_view();
        let dataset = serving(TypeRegime::IsA, TypeRegime::IsA);
        let not_exists = format!(
            "FILTER NOT EXISTS {{ ?s a ?sub . GRAPH <{G}> {{ ?sub rdfs:subClassOf+ ?t }} }}"
        );
        for left in [
            "VALUES ?sub { <https://example.org/typed/Root> } ?s a ?t",
            "?s a ?t VALUES ?sub { <https://example.org/typed/Root> }",
            "?s a ?t . ?s <https://example.org/typed/name> ?sub",
        ] {
            let (fired, plan) = refined(
                &sv,
                &dataset,
                &format!("SELECT ?s ?t WHERE {{ {left} {not_exists} }}"),
            );
            assert!(!fired, "{left}:\n{plan}");
            assert!(plan.contains("antijoin"), "{left}:\n{plan}");
        }
    }

    #[test]
    fn a_graph_that_is_not_a_known_schema_graph_leaves_the_plan_alone() {
        let sv = schema_view();
        let (fired, plan) = refined(
            &sv,
            &serving(TypeRegime::IsA, TypeRegime::IsA),
            &canonical("https://other/g"),
        );
        assert!(!fired, "{plan}");
        assert!(plan.contains("antijoin"), "{plan}");
    }

    #[test]
    fn a_schema_graph_that_misses_the_edges_the_data_uses_is_left_alone() {
        let sv = schema_view();
        // Mixin data needs mixin edges; the is-a graph has none for `Tag`.
        assert!(!most_specific_is_direct(
            &sv,
            TypeRegime::IsA,
            TypeRegime::IsAAndMixins
        ));
        // The asserted graph's direct edges close to the is-a ancestry.
        assert!(most_specific_is_direct(
            &sv,
            TypeRegime::Asserted,
            TypeRegime::IsA
        ));
        assert!(most_specific_is_direct(
            &sv,
            TypeRegime::IsAAndMixins,
            TypeRegime::IsAAndMixins
        ));
        assert!(!most_specific_is_direct(
            &sv,
            TypeRegime::Asserted,
            TypeRegime::IsAAndMixins
        ));
    }

    /// The dataset names its schema graphs by regime; the rule reads the one the
    /// query names. `inferred-mixins` over is-a data covers every edge the data
    /// uses, so the rewrite fires; `asserted` over mixin data does not, and is
    /// left alone. Neither is the default regime, so the dataset (and not a
    /// default) is what decided.
    #[test]
    fn the_rule_reads_the_named_graphs_own_regime() {
        const INFERRED_MIXINS: &str = "https://example.org/schema/inferred-mixins";
        const ASSERTED: &str = "https://example.org/schema/asserted";
        let sv = schema_view();
        let both = |data| {
            DatasetSpec::builder(data)
                .with_schema_graph(INFERRED_MIXINS, TypeRegime::IsAAndMixins)
                .with_schema_graph(ASSERTED, TypeRegime::Asserted)
                .build()
                .expect("a valid dataset")
        };
        let (fired, plan) = refined(&sv, &both(TypeRegime::IsA), &canonical(INFERRED_MIXINS));
        assert!(fired && plan.contains("directType"), "{plan}");
        let (fired, plan) = refined(
            &sv,
            &both(TypeRegime::IsAAndMixins),
            &canonical(INFERRED_MIXINS),
        );
        assert!(fired && plan.contains("directType"), "{plan}");
        let (fired, plan) = refined(&sv, &both(TypeRegime::IsAAndMixins), &canonical(ASSERTED));
        assert!(!fired, "{plan}");
        assert!(plan.contains("antijoin"), "{plan}");
    }

    /// Condition 3 of the proof rests on this: `rdfs:subClassOf+` over what
    /// `subclass_edges` publishes never relates a class to itself. If upstream
    /// ever emits a reflexive edge, this fails -- and the rule's closure check
    /// declines in the meantime.
    #[test]
    fn subclass_edges_are_irreflexive_in_every_regime() {
        let sv = schema_view();
        for regime in TypeRegime::ALL {
            for (sub, sup) in subclass_edges(&sv, regime).unwrap() {
                assert_ne!(class_key(&sub), class_key(&sup), "{regime:?}");
            }
            let closure = subclass_closure(&sv, regime).unwrap();
            assert!(
                closure.iter().all(|(sub, sup)| sub != sup),
                "{regime:?}: the closure relates a class to itself"
            );
        }
    }
}
