//! Language-tagged labels for enum concepts, read from an annotation LinkML
//! does not standardise.
//!
//! **This module is a shim.** LinkML has no standard way to say "the Dutch
//! label of this permissible value is X"; asset360's schemas say it with a
//! plain annotation, `data.infrabel.be/linkml/label/nl-be: X`. That is a
//! convention of this deployment, so its interpretation lives here, downstream,
//! and not in `rust-linkml-core`, whose triplifier
//! ([`linkml_runtime::schema_rdf`]) deliberately emits nothing it cannot
//! justify from the metamodel. The day LinkML grows a standard spelling and
//! upstream emits it, this module is one deletion: remove the file, the `mod`
//! line, and the one call in [`crate::sparql_schema_graph::SchemaGraph::build`].
//!
//! # What it emits
//!
//! For every permissible value of every enum, one `skos:prefLabel` per
//! annotation in [`LABEL_ANNOTATIONS`] the value carries, as a language-tagged
//! literal:
//!
//! ```text
//! <…/Material#Prestressed_Concrete> skos:prefLabel "Beton voorgespannen"@nl-be .
//! <…/Material#Prestressed_Concrete> skos:prefLabel "Béton précontraint"@fr-be .
//! ```
//!
//! so a client asks `?concept skos:prefLabel ?l . FILTER(lang(?l) = "nl-be")`
//! and gets a label it can show a Dutch user. That is #446 (pepibru GitLab
//! issue, asset360/consolidator-server), and the consumer is #440.
//!
//! # What it does not do
//!
//! * It does not touch `rdfs:label` (the code, which existing consumers key
//!   on) or `skos:notation` (the code again). Those stay exactly as upstream
//!   emits them.
//! * It does not invent a label. A value annotated in one language gets that
//!   one language; a value with no annotation gets nothing. The LinkML
//!   `description` is not a label and is in whatever language the modeller
//!   wrote it in, so it is never promoted to one.
//! * It reads only the tags in the table. `label/short/<lang>` and
//!   `label/long/<lang>` exist on slots, never on permissible values, and a
//!   parse of "whatever follows `label/`" would have turned them into the
//!   language `short/nl-be`. The table is the whole interpretation.
//! * It does not de-duplicate across values. Two permissible values sharing
//!   a `meaning` IRI share a subject, so each would hang its own
//!   `skos:prefLabel` there — two labels in one language on one concept.
//!   No enum in the live datamodel does that today (one value carries a
//!   `meaning` at all); if one ever does, the labels are the modeller's to
//!   reconcile, not this module's to pick between.
//! * It says nothing about classes and slots. They carry the same annotations
//!   and want the same treatment, but that is a separate decision on a
//!   separate subject, and this module is scoped to what #446 asked for.

#[cfg(feature = "sparql-endpoint")]
use linkml_runtime::schema_rdf::permissible_value_iri;
#[cfg(feature = "sparql-endpoint")]
use linkml_schemaview::schemaview::SchemaView;
#[cfg(feature = "sparql-endpoint")]
use oxigraph::model::{GraphName, Literal, NamedNode, Quad};

/// `skos:prefLabel` — SKOS's one preferred label per language, which is what
/// a per-language annotation is.
#[cfg(feature = "sparql-endpoint")]
pub const SKOS_PREF_LABEL: &str = "http://www.w3.org/2004/02/skos/core#prefLabel";

/// The interpretation, in full: which annotation tag carries which language.
///
/// The right-hand side is the RDF language tag the literal gets (BCP 47;
/// oxigraph lowercases it, so `LANG()` answers exactly this string). Nothing is
/// derived from the tag's spelling — a tag not in this table is not a label.
#[cfg(feature = "sparql-endpoint")]
pub const LABEL_ANNOTATIONS: &[(&str, &str)] = &[
    ("data.infrabel.be/linkml/label/nl-be", "nl-be"),
    ("data.infrabel.be/linkml/label/fr-be", "fr-be"),
    ("data.infrabel.be/linkml/label/en-us", "en-us"),
];

/// One `skos:prefLabel` quad per (permissible value, annotated language).
///
/// The concept subject is spelled by [`permissible_value_iri`], the same
/// function upstream's triplifier uses, so the label lands on the same IRI the
/// `skos:Concept` and the instance data carry — by construction, not by
/// re-implementing the rule. A value whose IRI will not expand is skipped
/// here exactly as upstream skips its concept; there is nothing to hang the
/// label on.
#[cfg(feature = "sparql-endpoint")]
pub(crate) fn enum_label_quads(sv: &SchemaView, graph: &GraphName) -> Vec<Quad> {
    let Ok(pref_label) = NamedNode::new(SKOS_PREF_LABEL) else {
        return Vec::new();
    };
    let conv = sv.converter();
    let mut quads = Vec::new();
    for ev in sv.enum_views().unwrap_or_default() {
        let Ok(enum_iri) = ev.canonical_uri().to_uri(&conv) else {
            continue;
        };
        let Some(values) = ev.definition().permissible_values.as_ref() else {
            continue;
        };
        for (code, pv) in values {
            let Some(annotations) = pv.annotations.as_ref() else {
                continue;
            };
            let Ok(concept) = permissible_value_iri(&enum_iri.0, code, pv, &conv)
                .and_then(|iri| NamedNode::new(iri).map_err(|err| err.to_string()))
            else {
                continue;
            };
            for (tag, lang) in LABEL_ANNOTATIONS {
                let Some(text) = annotations.get(*tag).and_then(annotation_text) else {
                    continue;
                };
                let Ok(literal) = Literal::new_language_tagged_literal(text, *lang) else {
                    continue;
                };
                quads.push(Quad::new(
                    concept.clone(),
                    pref_label.clone(),
                    literal,
                    graph.clone(),
                ));
            }
        }
    }
    // Permissible values live in a hash map; the graph must not depend on its
    // order, for the same reason upstream sorts its triples.
    quads.sort_by_key(|q| q.to_string());
    quads
}

/// The annotation's value as text, or `None` when it is not a string — a
/// label that is a list or a map is not a label, and is not guessed at.
#[cfg(feature = "sparql-endpoint")]
fn annotation_text(ann: &linkml_meta::Annotation) -> Option<String> {
    match serde_json::to_value(&ann.extension_value).ok()? {
        serde_json::Value::String(s) => Some(s),
        _ => None,
    }
}
