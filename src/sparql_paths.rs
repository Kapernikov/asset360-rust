//! Property paths whose walk is only its zero-length step.
//!
//! # The shape this is for
//!
//! `?s a/rdfs:subClassOf* asset360:Asset` is how a SPARQL author spells "an
//! instance of `Asset` or of any subclass". Against a store that holds the
//! schema as triples it means something; against *this* dataset it is a class
//! scan, and it used to be refused wholesale ("SPARQL property paths are not
//! supported") because the planner has no operator for a path.
//!
//! # Why it reduces (the proof)
//!
//! Which predicates can a data graph hold? Records are written by the
//! instance writer, and the writer emits exactly three kinds of predicate:
//! the canonical IRI of a slot, `rdf:type`, and (in the inferred regimes)
//! `sesame:directType`. It never writes a schema triple, so
//! `rdfs:subClassOf` — or any other IRI that is not one of those — is a
//! predicate **no data graph carries**. Schema triples live only in the named
//! schema graph, and this pass does not touch a pattern inside any `GRAPH`
//! clause (a named graph might be the schema graph; `GRAPH ?g` might range
//! over it).
//!
//! SPARQL 1.1 §18.4 gives the semantics of the path operators on the active
//! graph. Let `P` be a path expression whose every predicate is carried by no
//! data graph. Then the one-step relation of `P` over the data graph is empty
//! (every one-step match needs a triple with one of `P`'s predicates; the
//! data graph has none; inverse and alternative and sequence only combine
//! such steps). Therefore, over a data graph:
//!
//! * `P*` is its zero-length step alone. With exactly one end a constant
//!   term `C`, the zero-length path binds the other end to `C` — *even if `C`
//!   appears in no triple* (§18.4, `ZeroLengthPath`: the term is a member of
//!   the path's reflexive closure regardless of the graph). So
//!   `?t P* C` ≡ `{ BIND(C AS ?t) }`, and `C P* ?t` likewise.
//! * `P?` is the zero-length step plus one step, so the same reduction holds.
//! * `P+` is `P/P*`: its first step is empty, so it has no solution at all.
//! * With both ends constants, `C1 P* C2` has the empty-binding solution iff
//!   `C1` and `C2` are the same term, else none.
//!
//! Both ends variables would be "every node of the graph", which is not a
//! finite statement about the query, so that case is **declined**: the path
//! is left in place and the refusal downstream stands. So is every path that
//! mentions a slot predicate, `rdf:type` or `sesame:directType`, a negated
//! property set (it matches *any* other predicate), and every path inside a
//! `GRAPH`.
//!
//! # The blank-node variable
//!
//! The parser translates a sequence `?s a/rdfs:subClassOf* C` by itself
//! (SPARQL 1.1 §18.1.7: a sequence is two patterns joined through a fresh
//! blank node), giving `?s a _:v . _:v rdfs:subClassOf* C`. That is the
//! sequence split; there is nothing left for this pass to do about it. A
//! blank node in a query pattern is a variable that cannot be projected, and
//! its label is unique to the group the parser built it for. When the path
//! reduces to "`_:v` is `C`", the label is therefore *eliminated*: the path
//! becomes the unit pattern and `C` is written in place of `_:v` wherever the
//! label occurs. That is the join of the other patterns with the single
//! solution `{ _:v -> C }`, i.e. substitution. The result,
//! `?s a C`, is the pattern the author would have written for a store with no
//! hierarchy to walk — and since a `Join` of two basic graph patterns is one
//! basic graph pattern, the pieces the removal leaves are merged, so the
//! plan prints as that one pattern and nothing else.
//!
//! A named variable (`?v rdfs:subClassOf* C`) may be projected or shared, so
//! it is not substituted: it becomes a `BIND` over the unit pattern, which
//! is the equivalence stated above verbatim.
//!
//! # A dataset clause declines the whole pass
//!
//! `FROM` / `FROM NAMED` (spargebra's `dataset: Some(_)`) changes what the
//! default graph *is*: `FROM <schema> FROM <inferred>` makes it a merge that
//! does contain `rdfs:subClassOf` triples, and then the premise above (no
//! schema triple in the graph the path reads) is false. So a query with a
//! dataset clause is left untouched. This can be widened later to "every
//! `FROM` graph is a data graph", which is the premise restated.
//!
//! # Where it runs
//!
//! First step of [`crate::sparql_alias::canonicalize`], which every entry
//! point (planner, engine leg, string scoper) already calls. So the query the
//! planner plans and the query the engine evaluates are the same rewritten
//! algebra, and the rewrite — being an equivalence — cannot make the two
//! routes disagree. It runs *before* the class spelling is resolved so that
//! the `C` it moves into an `rdf:type` object is resolved like any other.

use std::collections::{BTreeSet, HashMap};

use spargebra::Query;
use spargebra::algebra::{Expression, GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNode, TermPattern, TriplePattern};

use linkml_schemaview::schemaview::SchemaView;

/// Reduce every path the data graph cannot walk. See the module docs.
pub(crate) fn reduce_paths(query: &mut Query, schema_view: &SchemaView) {
    let (Query::Select { dataset, .. }
    | Query::Construct { dataset, .. }
    | Query::Describe { dataset, .. }
    | Query::Ask { dataset, .. }) = &*query;
    if dataset.is_some() {
        return;
    }
    let pattern = match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => pattern,
    };
    if !contains_path(pattern) {
        return;
    }
    let mut pass = Pass {
        schema_view,
        carried: None,
        eliminated: HashMap::new(),
    };
    pass.pattern(pattern);
    if !pass.eliminated.is_empty() {
        substitute(pattern, &pass.eliminated);
    }
}

struct Pass<'a> {
    schema_view: &'a SchemaView,
    /// Every IRI a slot is written under, built the first time a path needs
    /// it: a query with no path pays nothing.
    carried: Option<BTreeSet<String>>,
    /// `blank node label -> the constant it is`, for the substitution.
    eliminated: HashMap<String, NamedNode>,
}

/// What a path with a constant end reduces to.
enum Reduction {
    /// Exactly the one solution the zero-length step gives.
    Bind(TermPattern, NamedNode),
    /// A solution with no bindings: the pattern holds.
    Holds,
    /// No solution.
    Empty,
}

impl Pass<'_> {
    fn carried(&mut self) -> &BTreeSet<String> {
        self.carried.get_or_insert_with(|| {
            let conv = self.schema_view.converter();
            let mut set = BTreeSet::new();
            set.insert(crate::sparql_scoper::RDF_TYPE.to_owned());
            set.insert(linkml_runtime::schema_rdf::SESAME_DIRECT_TYPE.to_owned());
            // The schema's slots, and every class's induced ones: a
            // `slot_usage` may give an inherited slot another `slot_uri` that
            // no declared slot carries.
            let declared = self.schema_view.slot_views().unwrap_or_default();
            let induced = self
                .schema_view
                .class_views()
                .unwrap_or_default()
                .into_iter()
                .flat_map(|class| class.slots().to_vec());
            for slot in declared.into_iter().chain(induced) {
                if let Ok(canonical) = slot.canonical_uri().to_uri(&conv) {
                    set.insert(canonical.0);
                }
                if let Ok(native) = self
                    .schema_view
                    .get_uri(slot.schema_id(), &slot.name)
                    .to_uri(&conv)
                {
                    set.insert(native.0);
                }
            }
            set
        })
    }

    /// Whether no data graph carries any predicate this path mentions.
    fn walks_nothing(&mut self, path: &PropertyPathExpression) -> bool {
        match path {
            PropertyPathExpression::NamedNode(node) => !self.carried().contains(node.as_str()),
            PropertyPathExpression::Reverse(inner)
            | PropertyPathExpression::ZeroOrMore(inner)
            | PropertyPathExpression::OneOrMore(inner)
            | PropertyPathExpression::ZeroOrOne(inner) => self.walks_nothing(inner),
            PropertyPathExpression::Sequence(left, right)
            | PropertyPathExpression::Alternative(left, right) => {
                self.walks_nothing(left) && self.walks_nothing(right)
            }
            // Matches every predicate *except* the listed ones.
            PropertyPathExpression::NegatedPropertySet(_) => false,
        }
    }

    fn reduction(
        &mut self,
        subject: &TermPattern,
        path: &PropertyPathExpression,
        object: &TermPattern,
    ) -> Option<Reduction> {
        let zero_length = match path {
            PropertyPathExpression::ZeroOrMore(inner)
            | PropertyPathExpression::ZeroOrOne(inner) => {
                if !self.walks_nothing(inner) {
                    return None;
                }
                true
            }
            PropertyPathExpression::OneOrMore(inner) => {
                if !self.walks_nothing(inner) {
                    return None;
                }
                false
            }
            _ => return None,
        };
        let constant = |term: &TermPattern| match term {
            TermPattern::NamedNode(node) => Some(node.clone()),
            _ => None,
        };
        let is_open = |term: &TermPattern| {
            matches!(term, TermPattern::Variable(_) | TermPattern::BlankNode(_))
        };
        match (constant(subject), constant(object)) {
            (Some(left), Some(right)) => {
                // `P+` over nothing is empty whatever the ends are.
                Some(if zero_length && left == right {
                    Reduction::Holds
                } else {
                    Reduction::Empty
                })
            }
            (None, Some(node)) if is_open(subject) => Some(if zero_length {
                Reduction::Bind(subject.clone(), node)
            } else {
                Reduction::Empty
            }),
            (Some(node), None) if is_open(object) => Some(if zero_length {
                Reduction::Bind(object.clone(), node)
            } else {
                Reduction::Empty
            }),
            // Both ends open: every node of the graph. Declined.
            _ => None,
        }
    }

    /// Returns whether it changed anything.
    fn pattern(&mut self, pattern: &mut GraphPattern) -> bool {
        match pattern {
            GraphPattern::Path {
                subject,
                path,
                object,
            } => {
                let Some(reduction) = self.reduction(subject, path, object) else {
                    return false;
                };
                let replacement = match reduction {
                    Reduction::Holds => unit(),
                    Reduction::Empty => empty(),
                    Reduction::Bind(TermPattern::Variable(variable), node) => {
                        GraphPattern::Extend {
                            inner: Box::new(unit()),
                            variable,
                            expression: Expression::NamedNode(node),
                        }
                    }
                    Reduction::Bind(TermPattern::BlankNode(blank), node) => {
                        // One label, one constant: a second, different one
                        // would be a conjunction the substitution cannot say.
                        match self.eliminated.get(blank.as_str()) {
                            Some(existing) if *existing != node => return false,
                            _ => {}
                        }
                        self.eliminated.insert(blank.as_str().to_owned(), node);
                        unit()
                    }
                    Reduction::Bind(..) => return false,
                };
                *pattern = replacement;
                true
            }
            // A named graph is not the data graph. See the module docs.
            GraphPattern::Graph { .. } | GraphPattern::Service { .. } => false,
            GraphPattern::Bgp { .. } | GraphPattern::Values { .. } => false,
            GraphPattern::Join { left, right } => {
                let changed = self.pattern(left) | self.pattern(right);
                if changed {
                    simplify_join(pattern);
                }
                changed
            }
            GraphPattern::Union { left, right }
            | GraphPattern::Lateral { left, right }
            | GraphPattern::Minus { left, right } => self.pattern(left) | self.pattern(right),
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                let mut changed = self.pattern(left) | self.pattern(right);
                if let Some(expression) = expression {
                    changed |= self.expression(expression);
                }
                changed
            }
            GraphPattern::Filter { expr, inner } => self.expression(expr) | self.pattern(inner),
            GraphPattern::Extend {
                inner, expression, ..
            } => self.pattern(inner) | self.expression(expression),
            GraphPattern::OrderBy { inner, .. }
            | GraphPattern::Project { inner, .. }
            | GraphPattern::Distinct { inner }
            | GraphPattern::Reduced { inner }
            | GraphPattern::Slice { inner, .. }
            | GraphPattern::Group { inner, .. } => self.pattern(inner),
        }
    }

    /// Only `EXISTS` holds a pattern; every other operator is walked for the
    /// `EXISTS` inside it.
    fn expression(&mut self, expression: &mut Expression) -> bool {
        match expression {
            Expression::Exists(pattern) => self.pattern(pattern),
            Expression::Or(left, right)
            | Expression::And(left, right)
            | Expression::Equal(left, right)
            | Expression::SameTerm(left, right)
            | Expression::Greater(left, right)
            | Expression::GreaterOrEqual(left, right)
            | Expression::Less(left, right)
            | Expression::LessOrEqual(left, right)
            | Expression::Add(left, right)
            | Expression::Subtract(left, right)
            | Expression::Multiply(left, right)
            | Expression::Divide(left, right) => self.expression(left) | self.expression(right),
            Expression::In(value, candidates) => {
                let mut changed = self.expression(value);
                for candidate in candidates {
                    changed |= self.expression(candidate);
                }
                changed
            }
            Expression::UnaryPlus(inner)
            | Expression::UnaryMinus(inner)
            | Expression::Not(inner) => self.expression(inner),
            Expression::If(condition, then, otherwise) => {
                self.expression(condition) | self.expression(then) | self.expression(otherwise)
            }
            Expression::Coalesce(parts) | Expression::FunctionCall(_, parts) => {
                let mut changed = false;
                for part in parts {
                    changed |= self.expression(part);
                }
                changed
            }
            Expression::NamedNode(_)
            | Expression::Literal(_)
            | Expression::Variable(_)
            | Expression::Bound(_) => false,
        }
    }
}

/// The unit pattern: one solution, no bindings.
fn unit() -> GraphPattern {
    GraphPattern::Bgp {
        patterns: Vec::new(),
    }
}

/// The empty pattern: no solution.
fn empty() -> GraphPattern {
    GraphPattern::Values {
        variables: Vec::new(),
        bindings: Vec::new(),
    }
}

fn is_unit(pattern: &GraphPattern) -> bool {
    matches!(pattern, GraphPattern::Bgp { patterns } if patterns.is_empty())
}

/// `Join(unit, X) = X`, and the join of two basic graph patterns is one basic
/// graph pattern (a conjunction of triple patterns; their blank-node labels
/// are distinct, so merging shares nothing). Only called on a join one side
/// of which this pass just rewrote.
fn simplify_join(pattern: &mut GraphPattern) {
    let GraphPattern::Join { left, right } = pattern else {
        return;
    };
    if is_unit(left) {
        *pattern = std::mem::replace(&mut **right, unit());
    } else if is_unit(right) {
        *pattern = std::mem::replace(&mut **left, unit());
    } else if let (GraphPattern::Bgp { patterns: l }, GraphPattern::Bgp { patterns: r }) =
        (&mut **left, &mut **right)
    {
        let mut merged = std::mem::take(l);
        merged.append(r);
        *pattern = GraphPattern::Bgp { patterns: merged };
    }
}

fn contains_path(pattern: &GraphPattern) -> bool {
    match pattern {
        GraphPattern::Path { .. } => true,
        GraphPattern::Bgp { .. }
        | GraphPattern::Values { .. }
        | GraphPattern::Graph { .. }
        | GraphPattern::Service { .. } => false,
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Minus { left, right }
        | GraphPattern::LeftJoin { left, right, .. } => contains_path(left) || contains_path(right),
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. } => contains_path(inner),
    }
}

/// Write the constant in place of the eliminated blank node.
fn substitute(pattern: &mut GraphPattern, eliminated: &HashMap<String, NamedNode>) {
    let swap = |term: &mut TermPattern| {
        if let TermPattern::BlankNode(blank) = term
            && let Some(node) = eliminated.get(blank.as_str())
        {
            *term = TermPattern::NamedNode(node.clone());
        }
    };
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for TriplePattern {
                subject, object, ..
            } in patterns
            {
                swap(subject);
                swap(object);
            }
        }
        GraphPattern::Path {
            subject, object, ..
        } => {
            swap(subject);
            swap(object);
        }
        GraphPattern::Values { .. } | GraphPattern::Graph { .. } | GraphPattern::Service { .. } => {
        }
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Minus { left, right }
        | GraphPattern::LeftJoin { left, right, .. } => {
            substitute(left, eliminated);
            substitute(right, eliminated);
        }
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. } => substitute(inner, eliminated),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparql_scoper::{parse_query, tests::test_schema_view};

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> \
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> ";

    fn reduced(query: &str) -> Query {
        let sv = test_schema_view();
        let mut parsed = parse_query(&format!("{PREFIX}{query}"), None).expect("parses");
        reduce_paths(&mut parsed, &sv);
        parsed
    }

    fn has_path(query: &Query) -> bool {
        let (Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. }) = query;
        contains_path(pattern)
    }

    /// The reduction is visible in the algebra, and the sequence spelling ends
    /// up as the very pattern the author would write without a hierarchy.
    #[test]
    fn the_zero_length_step_is_all_a_path_over_unwritten_predicates_has() {
        let class_scan = reduced("SELECT ?s WHERE { ?s a asset360:Signal }").to_string();
        for spelling in [
            "SELECT ?s WHERE { ?s a/rdfs:subClassOf* asset360:Signal }",
            "SELECT ?s WHERE { ?s a/(rdfs:subClassOf|rdfs:subPropertyOf)? asset360:Signal }",
            // Both orders of the same step, and a reversed one.
            "SELECT ?s WHERE { ?s a/^rdfs:subClassOf* asset360:Signal }",
            // Another triple on each side of the path: the pieces merge.
            "SELECT ?s WHERE { ?s a/rdfs:subClassOf* asset360:Signal . ?s asset360:name ?n }",
        ] {
            let got = reduced(spelling);
            assert!(!has_path(&got), "{spelling}: {got}");
        }
        assert_eq!(
            reduced("SELECT ?s WHERE { ?s a/rdfs:subClassOf* asset360:Signal }").to_string(),
            class_scan
        );
        assert_eq!(
            reduced(
                "SELECT ?s ?n WHERE { ?s a/rdfs:subClassOf* asset360:Signal . ?s asset360:name ?n }"
            )
            .to_string(),
            reduced("SELECT ?s ?n WHERE { ?s a asset360:Signal . ?s asset360:name ?n }")
                .to_string()
        );

        // A named variable is bound, not substituted: it may be projected.
        let bound = reduced("SELECT ?t WHERE { ?t rdfs:subClassOf* asset360:Signal }").to_string();
        assert!(bound.contains("AS ?t"), "{bound}");
        // `+` has no zero-length step, so nothing matches.
        let empty = reduced("SELECT ?t WHERE { ?t rdfs:subClassOf+ asset360:Signal }");
        assert!(!has_path(&empty), "{empty}");
        assert!(empty.to_string().contains("VALUES"), "{empty}");
    }

    /// A predicate a class writes under its own `slot_uri` -- an attribute's,
    /// or a `slot_usage` override of an inherited slot -- is carried by the
    /// data, so a path over it is not the zero-length step.
    #[test]
    fn a_class_local_slot_uri_is_a_carried_predicate() {
        let sv = crate::sparql_scoper::tests::test_schema_view_plus(
            r#"
  Marker:
    class_uri: asset360:Marker
    attributes:
      asset360_uri:
        identifier: true
      attrOnly:
        slot_uri: asset360:attrOnlyPredicate
        range: string
  Relabelled:
    class_uri: asset360:Relabelled
    is_a: Signal
    slot_usage:
      name:
        slot_uri: asset360:usageOnlyPredicate
"#,
        );
        for predicate in ["attrOnlyPredicate", "usageOnlyPredicate"] {
            let mut parsed = parse_query(
                &format!("{PREFIX}SELECT ?s WHERE {{ ?s asset360:{predicate}* asset360:Signal }}"),
                None,
            )
            .expect("parses");
            reduce_paths(&mut parsed, &sv);
            assert!(has_path(&parsed), "{predicate}: {parsed}");
        }
    }

    /// Everything outside the proof is left as it was, to be refused as before.
    #[test]
    fn what_the_proof_does_not_cover_is_left_alone() {
        for query in [
            // A slot predicate: the data graph carries it.
            "SELECT ?s WHERE { ?s asset360:documents* asset360:X }",
            // `rdf:type` and the inferred `directType` are carried.
            "SELECT ?s WHERE { ?s a* asset360:Signal }",
            "SELECT ?s WHERE { ?s <http://www.openrdf.org/schema/sesame#directType>* asset360:Signal }",
            // Every node of the graph.
            "SELECT ?s ?t WHERE { ?s rdfs:subClassOf* ?t }",
            // Matches any predicate but the listed one.
            "SELECT ?s WHERE { ?s !rdfs:label* asset360:Signal }",
            // A dataset clause may make the default graph hold schema triples.
            "SELECT ?s FROM <https://data.infrabel.be/asset360/schema> WHERE { ?s a/rdfs:subClassOf* asset360:Signal }",
            // A named graph is not the data graph.
            "SELECT ?s WHERE { GRAPH ?g { ?s rdfs:subClassOf* asset360:Signal } }",
            "SELECT ?s WHERE { GRAPH <https://data.infrabel.be/asset360/schema> { ?s rdfs:subClassOf+ asset360:Signal } }",
        ] {
            let sv = test_schema_view();
            let parsed = parse_query(&format!("{PREFIX}{query}"), None).expect("parses");
            let mut got = parsed.clone();
            reduce_paths(&mut got, &sv);
            assert_eq!(got.to_string(), parsed.to_string(), "{query}");
        }
    }

    /// Both routes see the reduced query, so the reduction has to be an
    /// equivalence *in the engine*: the original and the reduced query answer
    /// alike over a store that holds instances and no schema triples.
    #[cfg(feature = "sparql-endpoint")]
    #[test]
    fn the_reduction_answers_what_the_engine_answers() {
        let sv = test_schema_view();
        let oracle = crate::sparql_oracle::fixture(&sv);
        for query in [
            "SELECT ?s WHERE { ?s a/rdfs:subClassOf* asset360:Signal }",
            "SELECT (COUNT(?s) AS ?n) WHERE { ?s a/rdfs:subClassOf* asset360:Signal }",
            "SELECT ?s ?n WHERE { ?s a/rdfs:subClassOf* asset360:Signal . ?s asset360:name ?n }",
            "SELECT ?s WHERE { ?s a/rdfs:subClassOf? asset360:Signal }",
            "SELECT ?s WHERE { ?s a/rdfs:subClassOf+ asset360:Signal }",
            "SELECT ?t WHERE { ?t rdfs:subClassOf* asset360:Signal }",
            "SELECT ?t WHERE { asset360:Signal rdfs:subClassOf* ?t }",
            "SELECT ?t WHERE { ?t rdfs:subClassOf+ asset360:Nowhere }",
            "SELECT ?x WHERE { asset360:Signal rdfs:subClassOf* asset360:Signal . BIND(1 AS ?x) }",
            "SELECT ?x WHERE { asset360:A rdfs:subClassOf* asset360:B . BIND(1 AS ?x) }",
            "SELECT ?s WHERE { ?s a asset360:Signal FILTER EXISTS { ?s a/rdfs:subClassOf* asset360:Signal } }",
            "SELECT ?s ?t WHERE { ?s a asset360:Signal OPTIONAL { ?s a/rdfs:subClassOf* asset360:Track . BIND(1 AS ?t) } }",
        ] {
            let text = format!("{PREFIX}{query}");
            let original = parse_query(&text, None).expect("parses");
            let mut rewritten = original.clone();
            reduce_paths(&mut rewritten, &sv);
            assert!(!has_path(&rewritten), "{query}: {rewritten}");
            assert_eq!(
                oracle.answers(original),
                oracle.answers(rewritten),
                "{query}"
            );
        }
    }

    /// SPARQL 1.1 §18.4: a zero-length path binds its open end to a constant
    /// end *even when no triple mentions the constant*. The engine's own path
    /// evaluator answers such a query with no solution when the term is
    /// absent from the store, so this is asserted against the specification's
    /// answer, not against the unreduced query.
    #[cfg(feature = "sparql-endpoint")]
    #[test]
    fn a_constant_no_triple_mentions_still_binds_under_a_zero_length_step() {
        let sv = test_schema_view();
        let oracle = crate::sparql_oracle::fixture(&sv);
        let mut query = parse_query(
            &format!("{PREFIX}SELECT ?t WHERE {{ ?t rdfs:subClassOf* asset360:Nowhere }}"),
            None,
        )
        .expect("parses");
        reduce_paths(&mut query, &sv);
        let bag = oracle.answers(query);
        assert_eq!(bag.len(), 1, "{bag:?}");
        let (row, count) = bag.iter().next().unwrap();
        assert_eq!(*count, 1);
        assert_eq!(
            row.get("t").map(String::as_str),
            Some("<https://data.infrabel.be/asset360/Nowhere>")
        );
    }
}
