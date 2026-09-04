//! The datamodel itself, as RDF, in a named graph.
//!
//! The endpoint's graph holds instance data only: golden records materialised
//! as triples. Nothing in it says what a class is called, what a slot means, or
//! which values an enum permits. That gap is felt hardest on enum values: a
//! permissible value carrying a `meaning` is emitted as that IRI, so
//! `signalType` comes back as
//! `http://ontorail.org/src/Eulynx/eul2207a/EAID_28C3D8B9_...` and there is no
//! triple anywhere that says the code behind it is `GSA`. A client had to
//! hardcode the IRI to get a readable answer.
//!
//! This module triplifies the active [`SchemaView`] so a client can ask
//! instead. Scope is *discovery*: what things are called, how they relate, and
//! which values are allowed. It is deliberately not an OWL axiomatisation.
//!
//! # Why a named graph
//!
//! The triples go into [`SCHEMA_GRAPH_IRI`], never the default graph. The
//! endpoint has two execution routes — SQL pushdown over stored JSONB, and this
//! oxigraph leg over materialised instances — and a differential oracle in
//! consolidator-server asserts they answer every corpus question identically.
//! Schema triples in the default graph would be visible to the oxigraph leg and
//! invisible to SQL, so any `?s ?p ?o`-shaped question would diverge and the
//! oracle would fail, correctly. Confining them to a named graph leaves the
//! default graph byte-identical, so parity holds and clients opt in explicitly:
//!
//! ```sparql
//! SELECT ?code WHERE {
//!   GRAPH <https://data.infrabel.be/asset360/schema> {
//!     <http://ontorail.org/src/Eulynx/eul2207a/EAID_28C3D8B9_69BF_44c8_BF11_A28EA7F1A248>
//!       rdfs:label ?code
//!   }
//! }
//! ```
//!
//! # Vocabulary
//!
//! Nothing here is invented. Three well-known vocabularies carry it:
//!
//! * `rdfs:label` / `rdfs:comment` for the name and the description of every
//!   term. This is the one predicate a client can be expected to try first, so
//!   *everything* nameable gets an `rdfs:label`.
//! * `owl:Class` for classes and `rdf:Property` for slots — the weakest type
//!   assertions that are still true. `owl:ObjectProperty` /
//!   `owl:DatatypeProperty` are deliberately not asserted: a LinkML slot's
//!   range can be redeclared per class, so the distinction is not a property of
//!   the slot, and asserting it would be a guess.
//! * `skos:ConceptScheme` / `skos:Concept` / `skos:inScheme` / `skos:notation`
//!   for enums and their permissible values. SKOS is what a controlled value
//!   list *is*; modelling permissible values as OWL individuals of an
//!   `owl:Class`, as linkml's OWL generator does, asserts an ontological
//!   commitment the schema never made.
//! * `schema:domainIncludes` / `schema:rangeIncludes` for the class↔slot and
//!   slot↔range links. Not `rdfs:domain`: a LinkML slot is reused across
//!   unrelated classes, and several `rdfs:domain` triples on one property mean
//!   the *intersection* of those classes, which would be false. schema.org
//!   introduced the `*Includes` pair for exactly this — a non-committal "this
//!   is one of the places it is used". `rdfs:range` is additionally emitted
//!   when a slot has exactly one range, where it is not a guess.
//!
//! # IRIs must be absolute
//!
//! Every IRI is produced through the schema's own [`Converter`] — the same path
//! the instance turtle writer uses — and then through `NamedNode::new`, which
//! rejects anything that is not an absolute IRI. A term whose CURIE the
//! converter cannot expand is *skipped* and counted in
//! [`SchemaGraph::skipped`], never emitted as a bare CURIE. This matters
//! because a pre-existing defect elsewhere does emit an unexpandable enum
//! `meaning` CURIE into instance turtle, which then fails to re-parse; the
//! schema graph must not inherit it.
//!
//! # Which IRI names a class
//!
//! A class can have two legitimate spellings: its schema-native URI
//! (`<default prefix><ClassName>`) and its declared `class_uri`. The instance
//! writer emits `get_uri(conv, native = false, expand = true)` as the
//! `rdf:type` object, so that — the canonical, `class_uri`-preferring,
//! fully-expanded spelling — is what this module uses as a class subject. If
//! the two disagreed, a client joining instance types to the schema graph would
//! get no matches, which is the whole point of the feature.

#[cfg(feature = "sparql-endpoint")]
use std::collections::BTreeSet;

#[cfg(feature = "sparql-endpoint")]
use linkml_schemaview::converter::Converter;
#[cfg(feature = "sparql-endpoint")]
use linkml_schemaview::identifier::Identifier;
#[cfg(feature = "sparql-endpoint")]
use linkml_schemaview::schemaview::SchemaView;
#[cfg(feature = "sparql-endpoint")]
use oxigraph::model::{GraphName, Literal, NamedNode, NamedNodeRef, Quad, Term};

/// The graph the datamodel lives in.
///
/// Under the asset360 namespace because it describes the asset360 datamodel and
/// nothing else; `/schema` because that is what it holds. It is a name, not a
/// resolvable document — the endpoint serves it, no HTTP GET does.
pub const SCHEMA_GRAPH_IRI: &str = "https://data.infrabel.be/asset360/schema";

#[cfg(feature = "sparql-endpoint")]
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
#[cfg(feature = "sparql-endpoint")]
const RDF_PROPERTY: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
#[cfg(feature = "sparql-endpoint")]
const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
#[cfg(feature = "sparql-endpoint")]
const RDFS_COMMENT: &str = "http://www.w3.org/2000/01/rdf-schema#comment";
#[cfg(feature = "sparql-endpoint")]
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
#[cfg(feature = "sparql-endpoint")]
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
#[cfg(feature = "sparql-endpoint")]
const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
#[cfg(feature = "sparql-endpoint")]
const SKOS_CONCEPT_SCHEME: &str = "http://www.w3.org/2004/02/skos/core#ConceptScheme";
#[cfg(feature = "sparql-endpoint")]
const SKOS_CONCEPT: &str = "http://www.w3.org/2004/02/skos/core#Concept";
#[cfg(feature = "sparql-endpoint")]
const SKOS_IN_SCHEME: &str = "http://www.w3.org/2004/02/skos/core#inScheme";
#[cfg(feature = "sparql-endpoint")]
const SKOS_NOTATION: &str = "http://www.w3.org/2004/02/skos/core#notation";
#[cfg(feature = "sparql-endpoint")]
const SCHEMA_DOMAIN_INCLUDES: &str = "https://schema.org/domainIncludes";
#[cfg(feature = "sparql-endpoint")]
const SCHEMA_RANGE_INCLUDES: &str = "https://schema.org/rangeIncludes";

/// The triplified datamodel, plus what could not be triplified.
#[cfg(feature = "sparql-endpoint")]
pub struct SchemaGraph {
    /// Every quad, all of them in [`SCHEMA_GRAPH_IRI`].
    pub quads: Vec<Quad>,
    /// Terms dropped because their IRI would not have been absolute — an
    /// unexpandable CURIE, most likely a prefix the converter never saw. Kept
    /// rather than discarded so the endpoint can say *what* is missing instead
    /// of the client wondering why a label does not resolve.
    pub skipped: Vec<String>,
}

#[cfg(feature = "sparql-endpoint")]
impl SchemaGraph {
    /// Build the schema graph for one [`SchemaView`].
    ///
    /// Covers every class, slot and enum reachable from the view, including
    /// imported schemas — instance data types can come from any of them, so
    /// restricting to the primary schema would leave a client unable to look up
    /// exactly the imported vocabulary terms that are hardest to guess.
    pub fn build(sv: &SchemaView) -> Self {
        let conv = sv.converter();
        let graph = match NamedNode::new(SCHEMA_GRAPH_IRI) {
            Ok(node) => GraphName::NamedNode(node),
            // A compile-time constant that is a valid absolute IRI. Unreachable
            // without editing the constant to something invalid, and a test
            // pins it.
            Err(err) => panic!("SCHEMA_GRAPH_IRI is not a valid IRI: {err}"),
        };
        let mut builder = Builder {
            graph,
            quads: Vec::new(),
            skipped: Vec::new(),
        };

        builder.classes_and_slots(sv, &conv);
        builder.enums(sv, &conv);

        // Deterministic and duplicate-free: a slot reached through several
        // classes describes itself once, and the endpoint's output should not
        // depend on schema iteration order (the metamodel stores permissible
        // values and schemas in hash maps).
        builder.quads.sort_by_key(|q| q.to_string());
        builder.quads.dedup();
        builder.skipped.sort();
        builder.skipped.dedup();

        SchemaGraph {
            quads: builder.quads,
            skipped: builder.skipped,
        }
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

/// Whether a query could observe the schema graph at all.
///
/// A named graph is invisible unless the query says `GRAPH`: oxigraph's default
/// query dataset is the store's default graph, and neither `FROM NAMED` nor a
/// `DESCRIBE` reaches a named graph on its own. So for the overwhelming
/// majority of requests — every instance query — the schema graph would be
/// built, inserted and never read.
///
/// This is a precise test, not a substring guess: the query is parsed and its
/// algebra walked for a `GRAPH` node. A query that does not parse is treated as
/// *possibly* reading it, so the schema graph is built and oxigraph is left to
/// report the real parse error rather than this function inventing one.
pub fn query_reads_named_graphs(query: &str) -> bool {
    use spargebra::algebra::GraphPattern;
    use spargebra::{Query, SparqlParser};

    fn walk(pattern: &GraphPattern) -> bool {
        match pattern {
            GraphPattern::Graph { .. } => true,
            GraphPattern::Join { left, right }
            | GraphPattern::Union { left, right }
            | GraphPattern::Minus { left, right } => walk(left) || walk(right),
            GraphPattern::LeftJoin { left, right, .. } => walk(left) || walk(right),
            GraphPattern::Filter { inner, .. }
            | GraphPattern::Extend { inner, .. }
            | GraphPattern::OrderBy { inner, .. }
            | GraphPattern::Project { inner, .. }
            | GraphPattern::Distinct { inner }
            | GraphPattern::Reduced { inner }
            | GraphPattern::Slice { inner, .. }
            | GraphPattern::Group { inner, .. }
            | GraphPattern::Service { inner, .. } => walk(inner),
            GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => {
                false
            }
        }
    }

    let Ok(parsed) = SparqlParser::new().parse_query(query) else {
        return true;
    };
    let pattern = match &parsed {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => pattern,
    };
    walk(pattern)
}

/// Whether every triple pattern in the query reads the schema graph.
///
/// Such a query asks about the datamodel and about no golden record, so the
/// scoper — whose whole job is deciding which records to fetch — correctly
/// refuses it as unscoped. The endpoint answers it from the schema graph with
/// no instances loaded, and this is the predicate that says when that is the
/// right thing to do rather than a query with a genuinely missing scope.
///
/// A query that does not parse is not schema-only: the real parse error must
/// surface, not be turned into an empty answer.
pub fn reads_only_the_schema_graph(query: &str) -> bool {
    use spargebra::algebra::GraphPattern;
    use spargebra::term::NamedNodePattern;
    use spargebra::{Query, SparqlParser};

    // Same rule the scoper applies: the named schema graph, or a variable that
    // might bind to it. Any other constant graph names something the endpoint
    // does not have, and a query reading only that has no scope and no answer
    // here — which is what the unscoped refusal already says.
    fn reads_the_schema_graph(name: &NamedNodePattern) -> bool {
        match name {
            NamedNodePattern::NamedNode(node) => node.as_str() == SCHEMA_GRAPH_IRI,
            NamedNodePattern::Variable(_) => true,
        }
    }

    /// `(triples seen, triples seen inside the schema graph)`.
    fn walk(pattern: &GraphPattern, inside: bool, total: &mut usize, in_graph: &mut usize) {
        match pattern {
            GraphPattern::Bgp { patterns } => {
                *total += patterns.len();
                if inside {
                    *in_graph += patterns.len();
                }
            }
            // A property path is a triple pattern in every sense that matters
            // here: it reads data, and it reads it from whichever graph it sits
            // in.
            GraphPattern::Path { .. } => {
                *total += 1;
                if inside {
                    *in_graph += 1;
                }
            }
            GraphPattern::Graph { name, inner } => walk(
                inner,
                inside || reads_the_schema_graph(name),
                total,
                in_graph,
            ),
            GraphPattern::Join { left, right }
            | GraphPattern::Union { left, right }
            | GraphPattern::Minus { left, right } => {
                walk(left, inside, total, in_graph);
                walk(right, inside, total, in_graph);
            }
            GraphPattern::LeftJoin { left, right, .. } => {
                walk(left, inside, total, in_graph);
                walk(right, inside, total, in_graph);
            }
            GraphPattern::Filter { inner, .. }
            | GraphPattern::Extend { inner, .. }
            | GraphPattern::OrderBy { inner, .. }
            | GraphPattern::Project { inner, .. }
            | GraphPattern::Distinct { inner }
            | GraphPattern::Reduced { inner }
            | GraphPattern::Slice { inner, .. }
            | GraphPattern::Group { inner, .. }
            | GraphPattern::Service { inner, .. } => walk(inner, inside, total, in_graph),
            GraphPattern::Values { .. } => {}
        }
    }

    let Ok(parsed) = SparqlParser::new().parse_query(query) else {
        return false;
    };
    let pattern = match &parsed {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => pattern,
    };
    let (mut total, mut in_graph) = (0usize, 0usize);
    walk(pattern, false, &mut total, &mut in_graph);
    total > 0 && total == in_graph
}

#[cfg(feature = "sparql-endpoint")]
struct Builder {
    graph: GraphName,
    quads: Vec<Quad>,
    skipped: Vec<String>,
}

#[cfg(feature = "sparql-endpoint")]
impl Builder {
    /// An absolute-IRI node, or `None` with the offender recorded.
    fn node(&mut self, iri: &str, what: &str) -> Option<NamedNode> {
        match NamedNode::new(iri) {
            Ok(node) => Some(node),
            Err(_) => {
                self.skipped.push(format!("{what}: {iri}"));
                None
            }
        }
    }

    /// Expand a LinkML `uriorcurie` through the schema's own converter, then
    /// demand an absolute IRI. No hand-rolled prefix handling: prefix
    /// resolution is a live defect area in this codebase and the converter is
    /// the only thing that gets it right.
    fn expanded(&mut self, raw: &str, conv: &Converter, what: &str) -> Option<NamedNode> {
        let expanded = match Identifier::new(raw).to_uri(conv) {
            Ok(uri) => uri.0,
            // Not expandable — fall through to `node`, which will reject it and
            // record it, rather than emitting a bare CURIE.
            Err(_) => raw.to_owned(),
        };
        self.node(&expanded, what)
    }

    fn quad(&mut self, subject: &NamedNode, predicate: &str, object: Term) {
        let Some(predicate) = NamedNode::new(predicate).ok() else {
            // Every predicate here is a constant above; a test pins them.
            panic!("built-in predicate IRI is invalid: {predicate}");
        };
        let graph = self.graph.clone();
        self.quads
            .push(Quad::new(subject.clone(), predicate, object, graph));
    }

    fn label(&mut self, subject: &NamedNode, text: &str) {
        self.quad(
            subject,
            RDFS_LABEL,
            Literal::new_simple_literal(text).into(),
        );
    }

    fn comment(&mut self, subject: &NamedNode, text: Option<&String>) {
        if let Some(text) = text {
            self.quad(
                subject,
                RDFS_COMMENT,
                Literal::new_simple_literal(text).into(),
            );
        }
    }

    fn type_of(&mut self, subject: &NamedNode, class: &str) {
        let object = Term::NamedNode(NamedNodeRef::new_unchecked(class).into_owned());
        self.quad(subject, RDF_TYPE, object);
    }

    fn classes_and_slots(&mut self, sv: &SchemaView, conv: &Converter) {
        let class_views = match sv.class_views() {
            Ok(views) => views,
            // A view that cannot enumerate its own classes is a broken schema,
            // and the endpoint has already failed on it long before this point.
            // There is nothing to describe, so describe nothing rather than
            // failing a query that never asked about the schema.
            Err(_) => return,
        };

        for cv in &class_views {
            let Ok(class_id) = cv.get_uri(conv, false, true) else {
                self.skipped.push(format!("class: {}", cv.name()));
                continue;
            };
            let Some(class_node) = self.expanded(&class_id.to_string(), conv, "class") else {
                continue;
            };

            self.type_of(&class_node, OWL_CLASS);
            self.label(&class_node, cv.name());
            self.comment(&class_node, cv.def().description.as_ref());

            if let Ok(Some(parent)) = cv.parent_class()
                && let Ok(parent_id) = parent.get_uri(conv, false, true)
                && let Some(parent_node) =
                    self.expanded(&parent_id.to_string(), conv, "parent class")
            {
                self.quad(&class_node, RDFS_SUBCLASS_OF, parent_node.into());
            }

            for slot in cv.slots() {
                let slot_id = slot.canonical_uri();
                let Some(slot_node) = self.expanded(&slot_id.to_string(), conv, "slot") else {
                    continue;
                };

                self.type_of(&slot_node, RDF_PROPERTY);
                self.label(&slot_node, &slot.name);
                self.comment(&slot_node, slot.definition().description.as_ref());
                self.quad(
                    &slot_node,
                    SCHEMA_DOMAIN_INCLUDES,
                    class_node.clone().into(),
                );

                let ranges = self.slot_ranges(slot, conv);
                for range in &ranges {
                    self.quad(&slot_node, SCHEMA_RANGE_INCLUDES, range.clone().into());
                }
                // Only when it is not a guess: one range, so `rdfs:range`'s
                // "every value is one of these" is exactly what the schema says.
                if let [only] = ranges.as_slice() {
                    self.quad(&slot_node, RDFS_RANGE, only.clone().into());
                }
            }
        }
    }

    /// The IRIs a slot's values can have, deduplicated. A slot with `any_of`
    /// contributes one per branch.
    fn slot_ranges(
        &mut self,
        slot: &linkml_schemaview::slotview::SlotView,
        conv: &Converter,
    ) -> Vec<NamedNode> {
        let mut seen: BTreeSet<String> = BTreeSet::new();

        if let Some(range_class) = slot.get_range_class()
            && let Ok(id) = range_class.get_uri(conv, false, true)
        {
            seen.insert(id.to_string());
        }
        if let Some(range_enum) = slot.get_range_enum() {
            seen.insert(range_enum.canonical_uri().to_string());
        }
        if seen.is_empty() {
            for info in slot.get_range_info() {
                if let Some(datatype) = &info.rdf_datatype_iri {
                    seen.insert(datatype.clone());
                }
            }
        }

        seen.into_iter()
            .filter_map(|raw| self.expanded(&raw, conv, "slot range"))
            .collect()
    }

    fn enums(&mut self, sv: &SchemaView, conv: &Converter) {
        let enum_views = match sv.enum_views() {
            Ok(views) => views,
            Err(_) => return,
        };

        for ev in &enum_views {
            let enum_id = ev.canonical_uri();
            let Some(enum_node) = self.expanded(&enum_id.to_string(), conv, "enum") else {
                continue;
            };

            self.type_of(&enum_node, SKOS_CONCEPT_SCHEME);
            self.label(&enum_node, ev.name());
            self.comment(&enum_node, ev.definition().description.as_ref());

            let Some(values) = ev.definition().permissible_values.as_ref() else {
                continue;
            };
            for (code, pv) in values {
                // A value with no `meaning` is rendered by the instance writer
                // as a plain literal. There is no IRI to hang a label on, and
                // minting one would describe a resource that appears nowhere in
                // the data — so it gets no triples, and the code is already
                // legible in the instance data without them.
                let Some(meaning) = pv.meaning.as_ref() else {
                    continue;
                };
                let Some(value_node) =
                    self.expanded(meaning, conv, &format!("enum value {}.{code}", ev.name()))
                else {
                    continue;
                };

                self.type_of(&value_node, SKOS_CONCEPT);
                // The code, on `rdfs:label`, is the whole motivation: this is
                // what turns an opaque EAID IRI back into `GSA`.
                self.label(&value_node, code);
                self.quad(
                    &value_node,
                    SKOS_NOTATION,
                    Literal::new_simple_literal(code).into(),
                );
                self.comment(&value_node, pv.description.as_ref());
                self.quad(&value_node, SKOS_IN_SCHEME, enum_node.clone().into());
            }
        }
    }
}

#[cfg(all(test, feature = "sparql-endpoint"))]
mod tests {
    use super::*;
    use linkml_meta::SchemaDefinition;
    use std::path::Path;

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

    #[test]
    fn graph_iri_is_a_valid_absolute_iri() {
        NamedNode::new(SCHEMA_GRAPH_IRI).expect("graph IRI must be absolute");
    }

    #[test]
    fn every_emitted_iri_is_absolute_and_in_the_schema_graph() {
        let graph = SchemaGraph::build(&asset360_schema_view());
        assert!(!graph.quads.is_empty(), "expected a non-empty schema graph");

        let expected = GraphName::NamedNode(NamedNode::new(SCHEMA_GRAPH_IRI).unwrap());
        for quad in &graph.quads {
            assert_eq!(quad.graph_name, expected, "quad escaped the schema graph");
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

    #[test]
    fn classes_carry_a_label_and_are_typed() {
        let sv = asset360_schema_view();
        let graph = SchemaGraph::build(&sv);
        let ntriples = graph.to_ntriples();

        let cv = sv
            .class_views()
            .unwrap()
            .into_iter()
            .find(|cv| cv.name() == "Signal")
            .expect("asset360 fixture has a Signal class");
        let class_iri = cv
            .get_uri(&sv.converter(), false, true)
            .unwrap()
            .to_string();

        assert!(
            ntriples.contains(&format!("<{class_iri}> <{RDFS_LABEL}> \"Signal\"")),
            "expected an rdfs:label for {class_iri}"
        );
        assert!(
            ntriples.contains(&format!("<{class_iri}> <{RDF_TYPE}> <{OWL_CLASS}>")),
            "expected {class_iri} to be typed owl:Class"
        );
    }

    /// The join that makes the feature useful: the IRI an instance's
    /// `rdf:type` names must be the IRI the schema graph describes. The
    /// instance writer uses `get_uri(conv, native = false, expand = true)`;
    /// this asserts the schema graph agrees, for a class that actually declares
    /// a distinct `class_uri`.
    #[test]
    fn class_subject_matches_the_instance_rdf_type_spelling() {
        let sv = asset360_schema_view();
        let conv = sv.converter();
        let graph = SchemaGraph::build(&sv);
        let ntriples = graph.to_ntriples();

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

    #[test]
    fn enum_values_with_a_meaning_get_their_code_as_a_label() {
        let sv = asset360_schema_view();
        let conv = sv.converter();
        let graph = SchemaGraph::build(&sv);
        let ntriples = graph.to_ntriples();

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

    /// A permissible value with no `meaning` renders as a plain literal in
    /// instance data. There is no IRI to describe, and inventing one would
    /// describe a resource that appears nowhere — so nothing is emitted.
    #[test]
    fn enum_values_without_a_meaning_get_no_iri() {
        let sv = asset360_schema_view();
        let graph = SchemaGraph::build(&sv);
        let subjects: BTreeSet<String> = graph
            .quads
            .iter()
            .map(|quad| quad.subject.to_string())
            .collect();

        for ev in sv.enum_views().unwrap() {
            let Some(values) = ev.definition().permissible_values.clone() else {
                continue;
            };
            for (code, pv) in values {
                if pv.meaning.is_some() {
                    continue;
                }
                let enum_iri = ev.canonical_uri().to_string();
                let minted = format!("<{}/{code}>", enum_iri.trim_end_matches('/'));
                assert!(
                    !subjects.contains(&minted),
                    "meaning-less value {code} should not have been given an IRI"
                );
            }
        }
    }

    #[test]
    fn only_a_graph_clause_can_reach_the_schema_graph() {
        // Nothing to build for: no GRAPH clause anywhere.
        assert!(!query_reads_named_graphs("SELECT ?s WHERE { ?s ?p ?o }"));
        // `FROM NAMED` alone reaches no named graph without a GRAPH clause.
        assert!(!query_reads_named_graphs(
            "SELECT ?s FROM NAMED <urn:g> WHERE { ?s ?p ?o }"
        ));
        assert!(query_reads_named_graphs(
            "SELECT ?s WHERE { GRAPH <urn:g> { ?s ?p ?o } }"
        ));
        assert!(query_reads_named_graphs(
            "SELECT ?s WHERE { ?s ?p ?o OPTIONAL { GRAPH ?g { ?s ?q ?r } } }"
        ));
        // An unparseable query is treated as possibly reading it, so oxigraph
        // reports the real parse error rather than this function inventing one.
        assert!(query_reads_named_graphs("SELECT ?s WHERE {"));
    }

    #[test]
    fn schema_only_queries_are_told_apart_from_mixed_ones() {
        let schema = format!("SELECT ?c WHERE {{ GRAPH <{SCHEMA_GRAPH_IRI}> {{ ?c ?p ?o }} }}");
        assert!(reads_only_the_schema_graph(&schema));
        // A variable graph might bind to the schema graph.
        assert!(reads_only_the_schema_graph(
            "SELECT ?c WHERE { GRAPH ?g { ?c ?p ?o } }"
        ));
        // Some other named graph: the endpoint has none, and the unscoped
        // refusal already says so.
        assert!(!reads_only_the_schema_graph(
            "SELECT ?c WHERE { GRAPH <urn:g> { ?c ?p ?o } }"
        ));
        let mixed =
            format!("SELECT ?c WHERE {{ ?s ?p ?o . GRAPH <{SCHEMA_GRAPH_IRI}> {{ ?c ?q ?r }} }}");
        assert!(!reads_only_the_schema_graph(&mixed));
        assert!(!reads_only_the_schema_graph("SELECT ?s WHERE { ?s ?p ?o }"));
        // No triple pattern at all is not a schema query.
        assert!(!reads_only_the_schema_graph(
            "SELECT ?x WHERE { BIND(1 AS ?x) }"
        ));
        assert!(!reads_only_the_schema_graph("SELECT ?s WHERE {"));
    }

    #[test]
    fn output_is_deterministic() {
        let sv = asset360_schema_view();
        let first = SchemaGraph::build(&sv).to_ntriples();
        let second = SchemaGraph::build(&sv).to_ntriples();
        assert_eq!(first, second, "schema graph must not depend on map order");
    }
}
