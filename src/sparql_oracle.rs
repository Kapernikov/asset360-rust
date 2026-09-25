//! The in-memory oracle: the original query over the complete fixture, with
//! no fetch in front of it.
//!
//! Test instrument only (`docs/design/sparql-scopes-as-relations.md`, *Tests
//! that would prove equivalence*). The engine leg re-runs the query over a
//! materialised fetch, and a fetch narrowed by a wrong provenance makes both
//! routes agree on a wrong answer; this evaluates against the whole seed in
//! an oxigraph store, so it is independent of every fetch. Answers are
//! compared as bags of solution mappings over RDF terms
//! ([`crate::sparql_algebra::Bag`]): unbound is a value, multiplicity
//! counts, order only where the query defines one.

#![cfg(all(test, feature = "sparql-endpoint"))]

use std::collections::BTreeMap;

use linkml_runtime::LinkMLInstance;
use linkml_runtime::turtle::{TurtleOptions, turtle_to_string};
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::SchemaView;
use oxigraph::io::RdfFormat;
use oxigraph::sparql::QueryResults;
use oxigraph::store::Store;

use crate::sparql_algebra::Bag;

/// A store holding the fixture, ready to answer.
pub struct Oracle {
    store: Store,
}

impl Oracle {
    /// The seed loaded as records of `class` from JSON, one per string,
    /// turtled the way the engine leg turtles them (`skolem: false`, so an
    /// inlined element is a blank node of its own).
    pub fn new(schema: &SchemaView, records: &[(&str, &str)]) -> Self {
        let store = Store::new().unwrap();
        let conv = schema.converter();
        let primary = schema.primary_schema().unwrap();
        for (class, json) in records {
            let cv = schema
                .get_class(&Identifier::new(class), &conv)
                .unwrap()
                .unwrap_or_else(|| panic!("no class {class}"));
            let instance: LinkMLInstance = linkml_runtime::load_json_str(json, schema, &cv, &conv)
                .unwrap_or_else(|e| panic!("{class}: {e}\n{json}"))
                .into_instance_tolerate_errors()
                .unwrap();
            let turtle = turtle_to_string(
                &instance,
                schema,
                &primary,
                &conv,
                TurtleOptions { skolem: false },
            )
            .unwrap();
            store
                .load_from_reader(RdfFormat::Turtle, turtle.as_bytes())
                .unwrap_or_else(|e| panic!("{e}\n{turtle}"));
        }
        Self { store }
    }

    /// The answer to an already-parsed query, as a bag.
    pub fn answers(&self, query: spargebra::Query) -> Bag {
        let results = crate::sparql_executor::geosparql_evaluator()
            .for_query(query.clone())
            .on_store(&self.store)
            .execute()
            .unwrap_or_else(|e| panic!("{e}\n{query}"));
        let QueryResults::Solutions(solutions) = results else {
            panic!("expected solutions for {query}");
        };
        let vars: Vec<String> = solutions
            .variables()
            .iter()
            .map(|v| v.as_str().to_owned())
            .collect();
        let mut bag = Bag::new();
        for solution in solutions {
            let solution = solution.unwrap();
            let mut row = BTreeMap::new();
            for var in &vars {
                if let Some(term) = solution.get(var.as_str()) {
                    // A blank node's label is per evaluation: what identifies
                    // it across two evaluations is nothing at all, so it is
                    // compared as "some blank node" and its multiplicity does
                    // the rest.
                    let text = match term {
                        oxigraph::model::Term::BlankNode(_) => "_:".to_owned(),
                        other => other.to_string(),
                    };
                    row.insert(var.clone(), text);
                }
            }
            *bag.entry(row).or_insert(0) += 1;
        }
        bag
    }

    /// The answer to a query string.
    pub fn answers_to(&self, query: &str) -> Bag {
        self.answers(crate::sparql_scoper::parse_query(query).unwrap())
    }
}

/// The ordered answer to a query: rows in evaluation order, for a query
/// whose `ORDER BY` defines one.
pub fn ordered_answers(oracle: &Oracle, query: spargebra::Query) -> Vec<BTreeMap<String, String>> {
    let results = crate::sparql_executor::geosparql_evaluator()
        .for_query(query.clone())
        .on_store(&oracle.store)
        .execute()
        .unwrap();
    let QueryResults::Solutions(solutions) = results else {
        panic!("expected solutions");
    };
    let vars: Vec<String> = solutions
        .variables()
        .iter()
        .map(|v| v.as_str().to_owned())
        .collect();
    solutions
        .map(|solution| {
            let solution = solution.unwrap();
            vars.iter()
                .filter_map(|var| {
                    solution
                        .get(var.as_str())
                        .map(|term| (var.clone(), term.to_string()))
                })
                .collect()
        })
        .collect()
}

/// The seed of the design's test plan: an empty class (`Line`), a record
/// with a duplicate array entry (scalar on a signal, structure on an
/// assembly), two records identical in every read slot, an assembly whose
/// `parts[0].children[1]` equals `parts[1].children[1]`, and a reference hop
/// with and without a target.
pub fn fixture(schema: &SchemaView) -> Oracle {
    Oracle::new(
        schema,
        &[
            (
                "Track",
                r#"{"asset360_uri": "https://data.infrabel.be/asset360/track/T1", "hasName": "Main"}"#,
            ),
            (
                "Track",
                r#"{"asset360_uri": "https://data.infrabel.be/asset360/track/T2", "hasName": "Side"}"#,
            ),
            // Two signals identical in every read slot: duplicate mappings.
            (
                "Signal",
                r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/A", "name": "Alpha", "length": 3,
                    "trafficKinds": ["m", "m", "p"],
                    "documents": {"d1": {"docId": "d1", "title": "One"}, "d2": {"docId": "d2", "title": "Two"}},
                    "locatedOnTrack": "https://data.infrabel.be/asset360/track/T1"}"#,
            ),
            (
                "Signal",
                r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/B", "name": "Alpha", "length": 3,
                    "trafficKinds": ["m", "m", "p"],
                    "documents": {"d1": {"docId": "d1", "title": "One"}, "d2": {"docId": "d2", "title": "Two"}},
                    "locatedOnTrack": "https://data.infrabel.be/asset360/track/T1"}"#,
            ),
            // No array entries, no track, no name.
            (
                "Signal",
                r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/C", "length": 7}"#,
            ),
            // A track reference to a record that is not there.
            (
                "Signal",
                r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/D", "name": "Delta", "length": 3,
                    "trafficKinds": ["p"],
                    "documents": {"d9": {"docId": "d9", "title": "Nine"}},
                    "locatedOnTrack": "https://data.infrabel.be/asset360/track/T-missing"}"#,
            ),
            (
                "Signal",
                r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/E", "name": "Echo", "length": 1,
                    "locatedOnTrack": "https://data.infrabel.be/asset360/track/T2"}"#,
            ),
            (
                "BaliseGroup",
                r#"{"asset360_uri": "https://data.infrabel.be/asset360/bg/G1", "refersToSignal": "https://data.infrabel.be/asset360/signal/A"}"#,
            ),
            (
                "BaliseGroup",
                r#"{"asset360_uri": "https://data.infrabel.be/asset360/bg/G2", "refersToSignal": "https://data.infrabel.be/asset360/signal/A"}"#,
            ),
            // The nested occurrence: parts[0].children[1] = parts[1].children[1];
            // and a duplicated structure, parts[2] = parts[3], which are two
            // blank nodes (`documents` is a mapping, so it cannot hold one).
            (
                "Assembly",
                r#"{"asset360_uri": "https://data.infrabel.be/asset360/assembly/X",
                    "parts": [{"label": "p0", "children": [{"label": "c0"}, {"label": "same"}]},
                              {"label": "p1", "children": [{"label": "c1"}, {"label": "same"}]},
                              {"label": "dup", "children": [{"label": "d"}]},
                              {"label": "dup", "children": [{"label": "d"}]}]}"#,
            ),
        ],
    )
}

/// A design probe: `query` over a store holding exactly `turtle` (N-Triples
/// or Turtle, no records), as a bag. What the appendix of
/// `docs/design/sparql-schema-relations-and-row-finish.md` ran on
/// PyOxigraph, kept as a regression so a counterexample stays honest if the
/// engine changes: each one asserts that a rewrite a guard rejects really
/// does answer differently.
pub fn probe(turtle: &str, query: &str) -> Bag {
    let store = Store::new().unwrap();
    if !turtle.is_empty() {
        store
            .load_from_reader(RdfFormat::Turtle, turtle.as_bytes())
            .unwrap_or_else(|e| panic!("{e}\n{turtle}"));
    }
    Oracle { store }.answers_to(query)
}

/// [`probe`], as the sequence the engine emitted.
pub fn probe_sequence(turtle: &str, query: &str) -> Vec<BTreeMap<String, String>> {
    let store = Store::new().unwrap();
    if !turtle.is_empty() {
        store
            .load_from_reader(RdfFormat::Turtle, turtle.as_bytes())
            .unwrap_or_else(|e| panic!("{e}\n{turtle}"));
    }
    ordered_answers(
        &Oracle { store },
        crate::sparql_scoper::parse_query(query).unwrap(),
    )
}
