//! The active datamodel, as RDF, in a named graph this endpoint chooses.
//!
//! The triplification itself is not here. It is generic — it says nothing about
//! asset360, about this endpoint, or about graphs — and lives upstream in
//! [`linkml_runtime::schema_rdf`], which returns plain triples and takes no
//! position on graph naming. What is left here is the two decisions that *are*
//! this endpoint's: which graph the triples go in, and when to build them at
//! all.
//!
//! # Why a named graph
//!
//! The triples never go into the default graph. The endpoint has two execution
//! routes — SQL pushdown over stored JSONB, and the oxigraph leg over
//! materialised instances — and a differential oracle in consolidator-server
//! asserts they answer every corpus question identically. Schema triples in the
//! default graph would be visible to the oxigraph leg and invisible to SQL, so
//! any `?s ?p ?o`-shaped question would diverge and the oracle would fail,
//! correctly. Confining them to a named graph leaves the default graph
//! byte-identical, so parity holds and clients opt in explicitly:
//!
//! ```sparql
//! SELECT ?code WHERE {
//!   GRAPH <https://data.infrabel.be/asset360/schema> {
//!     ?v skos:inScheme ?e . ?e rdfs:label "SignalTypes" . ?v skos:notation ?code
//!   }
//! }
//! ```
//!
//! # Why the graph IRI is not a constant
//!
//! It used to be one, `https://data.infrabel.be/asset360/schema`. That is wrong
//! for every deployment that is not asset360, and there are others: the server
//! runs with a `DATAMODEL` environment variable, and under `DATAMODEL=rinf` the
//! active datamodel is ERA RINF, whose data lives under
//! `http://data.europa.eu/949/`. A compiled-in infrabel IRI cannot be correct
//! for both, and silently naming a graph after infrabel inside an unrelated
//! deployment is worse than having no schema graph at all.
//!
//! So the IRI is a parameter, supplied by the caller and ultimately by the
//! active datamodel's configuration (`schema_graph_iri` in
//! `asset360_model/datamodels/<name>.yaml`). A datamodel that does not
//! configure one has no schema graph: the endpoint serves instance data exactly
//! as it did before this feature existed, and nothing is invented on its
//! behalf. That is why the entry points here and in
//! [`crate::sparql_graph_clauses`] take `Option<&str>` rather than defaulting —
//! there is no defensible default.

#[cfg(feature = "sparql-endpoint")]
use linkml_runtime::schema_rdf::{SchemaRdfOptions, schema_triples};
#[cfg(feature = "sparql-endpoint")]
use linkml_schemaview::schemaview::SchemaView;
#[cfg(feature = "sparql-endpoint")]
use oxigraph::model::{GraphName, NamedNode, Quad};

/// The caller's graph IRI was not an absolute IRI.
///
/// Returned, not panicked on and not worked around: the value comes from
/// deployment configuration, so a bad one is a misconfiguration the operator
/// has to see, and substituting a valid IRI would put triples in a graph nobody
/// asked for.
#[cfg(feature = "sparql-endpoint")]
#[derive(Debug)]
pub struct InvalidGraphIri {
    /// The offending value, as configured.
    pub iri: String,
    /// Why it was rejected.
    pub reason: String,
}

#[cfg(feature = "sparql-endpoint")]
impl std::fmt::Display for InvalidGraphIri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "configured schema graph IRI {:?} is not an absolute IRI: {}",
            self.iri, self.reason
        )
    }
}

#[cfg(feature = "sparql-endpoint")]
impl std::error::Error for InvalidGraphIri {}

/// The triplified datamodel, in one named graph, plus what could not be
/// triplified.
#[cfg(feature = "sparql-endpoint")]
#[derive(Debug)]
pub struct SchemaGraph {
    /// Every quad, all of them in the graph the caller named.
    pub quads: Vec<Quad>,
    /// Terms dropped because their IRI would not have been absolute — an
    /// unexpandable CURIE, most likely a prefix the converter never saw. Kept
    /// rather than discarded so the endpoint can say *what* is missing instead
    /// of the client wondering why a label does not resolve.
    pub skipped: Vec<String>,
}

#[cfg(feature = "sparql-endpoint")]
impl SchemaGraph {
    /// Build the schema graph for one [`SchemaView`], in the named graph
    /// `graph_iri`.
    ///
    /// The triples come from [`linkml_runtime::schema_rdf`] unchanged; this
    /// only decides where they live.
    pub fn build(sv: &SchemaView, graph_iri: &str) -> Result<Self, InvalidGraphIri> {
        let graph =
            GraphName::NamedNode(NamedNode::new(graph_iri).map_err(|err| InvalidGraphIri {
                iri: graph_iri.to_owned(),
                reason: err.to_string(),
            })?);

        let built = schema_triples(sv, &SchemaRdfOptions::default());
        Ok(SchemaGraph {
            quads: built
                .triples
                .into_iter()
                .map(|t| {
                    Quad::new(
                        into_oxigraph_subject(t.subject),
                        NamedNode::new_unchecked(t.predicate.into_string()),
                        into_oxigraph_term(t.object),
                        graph.clone(),
                    )
                })
                .collect(),
            skipped: built.skipped,
        })
    }

    /// The graph as N-Triples, for tests and for a human reading it.
    ///
    /// N-Triples and not N-Quads because the graph name is implicit — the
    /// caller asked for *this* graph.
    pub fn to_ntriples(&self) -> String {
        let mut out = String::new();
        for quad in &self.quads {
            out.push_str(&quad.subject.to_string());
            out.push(' ');
            out.push_str(&quad.predicate.to_string());
            out.push(' ');
            out.push_str(&quad.object.to_string());
            out.push_str(" .\n");
        }
        out
    }
}

/// oxigraph 0.4 vendors oxrdf **0.2**, while `linkml_runtime` — and this crate's
/// SHACL parser — are on oxrdf **0.3**. They are the same data model but
/// different Rust types, so the triples upstream hands back have to be
/// re-typed on the way into the store. Nothing is reinterpreted here: an IRI
/// stays that IRI (already validated absolute upstream, hence
/// `new_unchecked`), a literal keeps its lexical form, language tag and
/// datatype, a blank node keeps its id. The match is exhaustive, so a term
/// shape neither side handles is a compile error rather than a silently
/// dropped triple.
///
/// When the two crates converge on one oxrdf version this whole function
/// disappears.
#[cfg(feature = "sparql-endpoint")]
fn into_oxigraph_subject(subject: oxrdf::NamedOrBlankNode) -> oxigraph::model::NamedOrBlankNode {
    match subject {
        oxrdf::NamedOrBlankNode::NamedNode(node) => oxigraph::model::NamedOrBlankNode::NamedNode(
            NamedNode::new_unchecked(node.into_string()),
        ),
        oxrdf::NamedOrBlankNode::BlankNode(node) => oxigraph::model::NamedOrBlankNode::BlankNode(
            oxigraph::model::BlankNode::new_unchecked(node.into_string()),
        ),
    }
}

/// See [`into_oxigraph_subject`].
#[cfg(feature = "sparql-endpoint")]
fn into_oxigraph_term(term: oxrdf::Term) -> oxigraph::model::Term {
    use oxigraph::model::{Literal as OxiLiteral, Term as OxiTerm};

    match term {
        oxrdf::Term::NamedNode(node) => {
            OxiTerm::NamedNode(NamedNode::new_unchecked(node.into_string()))
        }
        oxrdf::Term::BlankNode(node) => OxiTerm::BlankNode(
            oxigraph::model::BlankNode::new_unchecked(node.into_string()),
        ),
        oxrdf::Term::Literal(literal) => {
            let language = literal.language().map(str::to_owned);
            let datatype = literal.datatype().as_str().to_owned();
            let value = literal.destruct().0;
            OxiTerm::Literal(match language {
                Some(language) => {
                    OxiLiteral::new_language_tagged_literal_unchecked(value, language)
                }
                None => OxiLiteral::new_typed_literal(value, NamedNode::new_unchecked(datatype)),
            })
        }
    }
}

#[cfg(all(test, feature = "sparql-endpoint"))]
mod tests {
    use super::*;
    use linkml_meta::SchemaDefinition;
    use linkml_runtime::schema_rdf::{OWL_CLASS, RDF_TYPE, RDFS_LABEL, SKOS_IN_SCHEME};
    use linkml_schemaview::identifier::Identifier;
    use oxigraph::model::Term;
    use std::path::Path;

    /// What the asset360 datamodel's config sets `schema_graph_iri` to. A test
    /// value here, not a fallback: the production value comes from
    /// `asset360_model/datamodels/asset360.yaml`.
    const ASSET360_SCHEMA_GRAPH: &str = "https://data.infrabel.be/asset360/schema";

    /// The whole fixture, imports included. Loading `asset360.yaml` alone would
    /// leave out RSM and Eulynx, and those are where the fixture's `class_uri`
    /// declarations and enum `meaning`s live — i.e. exactly the cases these
    /// tests exist for.
    fn asset360_schema_view() -> SchemaView {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("data");
        let mut sv = SchemaView::new();
        for name in ["types.yaml", "rsm.yaml", "eulynx.yaml", "asset360.yaml"] {
            let path = dir.join(name);
            let yaml = std::fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
            let deser = serde_yml::Deserializer::from_str(&yaml);
            let schema: SchemaDefinition = serde_path_to_error::deserialize(deser).unwrap();
            sv.add_schema(schema).unwrap();
        }
        sv
    }

    /// The number of quads the asset360 fixture triplifies to. Pinned so a
    /// change in the upstream triplifier cannot silently change what this
    /// endpoint serves. (The *live* asset360 schema is much larger; the
    /// consolidator-server test suite pins that count.)
    #[test]
    fn the_fixture_quad_count_is_pinned() {
        let graph = SchemaGraph::build(&asset360_schema_view(), ASSET360_SCHEMA_GRAPH).unwrap();
        assert_eq!(graph.quads.len(), 1417);
    }

    #[test]
    fn every_quad_lands_in_the_graph_the_caller_named() {
        let graph = SchemaGraph::build(&asset360_schema_view(), ASSET360_SCHEMA_GRAPH).unwrap();
        assert!(!graph.quads.is_empty(), "expected a non-empty schema graph");

        let expected = GraphName::NamedNode(NamedNode::new(ASSET360_SCHEMA_GRAPH).unwrap());
        for quad in &graph.quads {
            assert_eq!(quad.graph_name, expected, "quad escaped the schema graph");
        }
    }

    /// A different datamodel gets a different graph and no infrabel IRI
    /// anywhere. This is the whole reason the IRI stopped being a constant.
    #[test]
    fn a_non_asset360_graph_iri_contains_no_infrabel_graph_name() {
        let rinf_graph = "http://data.europa.eu/949/schema";
        let graph = SchemaGraph::build(&asset360_schema_view(), rinf_graph).unwrap();
        let expected = GraphName::NamedNode(NamedNode::new(rinf_graph).unwrap());
        for quad in &graph.quads {
            assert_eq!(quad.graph_name, expected);
            assert!(
                !quad.graph_name.to_string().contains("infrabel"),
                "an infrabel-named graph leaked into a non-asset360 deployment"
            );
        }
    }

    /// A misconfigured IRI is an error the operator sees, never a substitution.
    #[test]
    fn a_relative_graph_iri_is_refused() {
        let err = SchemaGraph::build(&asset360_schema_view(), "not an iri")
            .expect_err("a relative IRI must be refused");
        assert!(err.to_string().contains("not an iri"));
    }

    #[test]
    fn every_emitted_iri_is_absolute() {
        let graph = SchemaGraph::build(&asset360_schema_view(), ASSET360_SCHEMA_GRAPH).unwrap();
        for quad in &graph.quads {
            // `NamedNode::new` on the way in already rejected relative IRIs;
            // re-check here so the invariant is asserted on the output, not on
            // the code path that produced it.
            for iri in [
                Some(quad.subject.to_string()),
                Some(quad.predicate.to_string()),
                match &quad.object {
                    Term::NamedNode(node) => Some(node.to_string()),
                    _ => None,
                },
            ]
            .into_iter()
            .flatten()
            {
                let bare = iri.trim_start_matches('<').trim_end_matches('>');
                NamedNode::new(bare)
                    .unwrap_or_else(|err| panic!("non-absolute IRI {bare} emitted: {err}"));
            }
        }
    }

    /// The join that makes the feature useful, asserted on the asset360
    /// fixture: the IRI an instance's `rdf:type` names is the IRI the schema
    /// graph describes. The upstream crate pins the rule; this pins that the
    /// rule still holds for *this* schema, which is the one with the distinct
    /// `class_uri` declarations.
    #[test]
    fn class_subject_matches_the_instance_rdf_type_spelling() {
        let sv = asset360_schema_view();
        let conv = sv.converter();
        let ntriples = SchemaGraph::build(&sv, ASSET360_SCHEMA_GRAPH)
            .unwrap()
            .to_ntriples();

        let with_distinct_class_uri: Vec<_> = sv
            .class_views()
            .unwrap()
            .into_iter()
            .filter(|cv| {
                let native = cv.get_uri(&conv, true, true).map(|id| id.to_string());
                let canonical = cv.get_uri(&conv, false, true).map(|id| id.to_string());
                matches!((native, canonical), (Ok(n), Ok(c)) if n != c)
            })
            .collect();
        assert!(
            !with_distinct_class_uri.is_empty(),
            "fixture has no class with a class_uri distinct from its native URI, \
             so this test would not be testing anything"
        );

        for cv in &with_distinct_class_uri {
            // What the turtle writer puts on the right of `rdf:type`.
            let instance_type_iri = cv.get_uri(&conv, false, true).unwrap().to_string();
            assert!(
                ntriples.contains(&format!("<{instance_type_iri}> <{RDF_TYPE}> <{OWL_CLASS}>")),
                "class {} is typed as <{instance_type_iri}> in instance data but the \
                 schema graph does not describe that IRI",
                cv.name()
            );
        }
    }

    /// The motivating case, on the real datamodel's shape: an enum value's code
    /// is recoverable from its opaque `meaning` IRI.
    #[test]
    fn enum_values_with_a_meaning_get_their_code_as_a_label() {
        let sv = asset360_schema_view();
        let conv = sv.converter();
        let ntriples = SchemaGraph::build(&sv, ASSET360_SCHEMA_GRAPH)
            .unwrap()
            .to_ntriples();

        let mut checked = 0usize;
        for ev in sv.enum_views().unwrap() {
            let Some(values) = ev.definition().permissible_values.clone() else {
                continue;
            };
            for (code, pv) in values {
                let Some(meaning) = pv.meaning.as_ref() else {
                    continue;
                };
                let Ok(iri) = Identifier::new(meaning).to_uri(&conv) else {
                    continue;
                };
                if NamedNode::new(&iri.0).is_err() {
                    continue;
                }
                assert!(
                    ntriples.contains(&format!("<{}> <{RDFS_LABEL}> \"{code}\"", iri.0)),
                    "enum {} value {code} has meaning {} but no rdfs:label",
                    ev.name(),
                    iri.0
                );
                assert!(
                    ntriples.contains(&format!("<{}> <{SKOS_IN_SCHEME}> <", iri.0)),
                    "enum value {code} is not linked back to its scheme"
                );
                checked += 1;
            }
        }
        assert!(
            checked > 0,
            "fixture has no enum value carrying a meaning, so this test would \
             not be testing anything"
        );
    }

    #[test]
    fn output_is_deterministic() {
        let sv = asset360_schema_view();
        let first = SchemaGraph::build(&sv, ASSET360_SCHEMA_GRAPH)
            .unwrap()
            .to_ntriples();
        let second = SchemaGraph::build(&sv, ASSET360_SCHEMA_GRAPH)
            .unwrap()
            .to_ntriples();
        assert_eq!(first, second, "schema graph must not depend on map order");
    }
}
