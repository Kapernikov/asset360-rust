//! One slot, two spellings — and the planner accepts both.
//!
//! A LinkML slot that declares a `slot_uri` has two legitimate IRIs. The
//! **canonical** one is what the declaration says, and it is what the instance
//! writer puts in the data: on the asset360 datamodel, `Track`'s `name` is
//! written as `<http://rsm.uic.org/RSM12#EAID_080C70AE_7680_4515_B580_0B30E8066364>`.
//! The **native** one is the spelling LinkML derives from the slot's own name
//! in its schema's default prefix — `irsm:name` — and it appears nowhere in the
//! data at all.
//!
//! Nothing there is wrong, but the failure mode is: a query author who writes
//! the readable spelling gets a 200 with a permanently empty column, because
//! inside an `OPTIONAL` an unmatched predicate is not an error. Nobody guesses
//! an EAID, so in practice the slot is unreachable. That is
//! [#447](https://gitlab.pp.kapernikov.com/asset360/consolidator-server/-/issues/447).
//!
//! So this module resolves the alias **in the plan**, once, before either route
//! sees the query: a fixed predicate IRI that names a slot by its native
//! spelling is replaced by that slot's canonical IRI. The SQL leg and the
//! oxigraph leg are then answering the same question by construction rather
//! than by two implementations agreeing — which matters, because the SQL leg
//! resolves a predicate through [`SchemaView::get_slot_by_uri`] (which has
//! always accepted both spellings) while the engine matches the IRI against
//! triples (which only ever carry the canonical one). Left alone, the two
//! routes answer *differently* for the native spelling, and a silent empty
//! column would have become a silent route-dependent answer.
//!
//! # What is deliberately not rewritten
//!
//! * **A variable predicate.** `?s ?p ?o` asks what the data says, and the data
//!   says the canonical IRI. Rewriting nothing here is what keeps the two
//!   routes in step for the open-predicate shapes the differential oracle
//!   asks.
//! * **A predicate inside a `GRAPH` clause.** Named graphs are not instance
//!   data. The only one this endpoint serves is the schema graph, whose
//!   predicates are RDF/RDFS/OWL/SKOS terms and never slots.
//! * **A `CONSTRUCT` template.** Its predicates are *output* spelling, chosen
//!   by the author. Rewriting them would change the graph a client asked to be
//!   given back.
//!
//! # Ambiguity is refused, not resolved
//!
//! Two slots can in principle share one native spelling and disagree about
//! their canonical one — two schemas with the same `default_prefix`, each
//! declaring an attribute of the same name with a different `slot_uri`. There
//! is no defensible pick between them, so the alias is refused by name and the
//! author is told to write the canonical IRI. An IRI that is *itself* some
//! slot's canonical spelling is never rewritten either: it already names a
//! predicate that exists in the data, and preferring the alias reading would
//! lose matches.

use std::collections::{BTreeSet, HashMap};

use linkml_schemaview::schemaview::SchemaView;
use spargebra::Query;
use spargebra::algebra::{Expression, GraphPattern, PropertyPathExpression};
use spargebra::term::NamedNodePattern;

/// An alias that names more than one slot, with no way to choose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmbiguousAlias {
    /// The IRI as the query spelled it.
    pub alias: String,
    /// Every canonical IRI it could have meant, sorted.
    pub canonical: Vec<String>,
}

impl std::fmt::Display for AmbiguousAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "predicate <{}> is the native spelling of more than one slot ({}); \
             write the one you mean",
            self.alias,
            self.canonical
                .iter()
                .map(|c| format!("<{c}>"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl std::error::Error for AmbiguousAlias {}

/// What one rewrite did, for a log line and for the tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rewrite {
    /// The IRI the author wrote.
    pub from: String,
    /// The IRI the data uses.
    pub to: String,
}

/// Rewrite every fixed predicate that names a slot by its native spelling.
///
/// Returns what it changed, in no particular order and deduplicated — the
/// caller logs it. `Ok(vec![])` is the overwhelmingly common answer and costs
/// one index lookup per fixed predicate.
pub fn canonicalize_predicates(
    query: &mut Query,
    schema_view: &SchemaView,
) -> Result<Vec<Rewrite>, AmbiguousAlias> {
    let mut predicates: BTreeSet<String> = BTreeSet::new();
    match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => collect(pattern, &mut predicates),
    }
    if predicates.is_empty() {
        return Ok(Vec::new());
    }

    let mapping = alias_mapping(&predicates, schema_view)?;
    if mapping.is_empty() {
        return Ok(Vec::new());
    }

    match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => apply(pattern, &mapping),
    }

    let mut rewrites: Vec<Rewrite> = mapping
        .into_iter()
        .map(|(from, to)| Rewrite { from, to })
        .collect();
    rewrites.sort_by(|a, b| a.from.cmp(&b.from));
    Ok(rewrites)
}

/// Which of `predicates` are native spellings, and what each one means.
///
/// The expensive half — a pass over every slot in the view — runs only when a
/// candidate survives the cheap half, so a query written in canonical IRIs (or
/// in no slot IRIs at all, which is most of them) pays nothing but an indexed
/// lookup per predicate.
fn alias_mapping(
    predicates: &BTreeSet<String>,
    schema_view: &SchemaView,
) -> Result<HashMap<String, String>, AmbiguousAlias> {
    let conv = schema_view.converter();
    let mut candidates: Vec<&String> = Vec::new();
    for iri in predicates {
        let Ok(Some(slot)) = schema_view.get_slot_by_uri(iri) else {
            continue;
        };
        let Ok(canonical) = slot.canonical_uri().to_uri(&conv) else {
            continue;
        };
        if canonical.0 != *iri {
            candidates.push(iri);
        }
    }
    if candidates.is_empty() {
        return Ok(HashMap::new());
    }

    // Every canonical spelling in the view, and every native spelling that
    // differs from it. Built once, here, because the answer for one alias
    // depends on what *every* slot claims — an alias is only unambiguous if no
    // second slot claims it, and it is only an alias at all if it is not
    // itself a canonical IRI.
    let mut canonical_spellings: BTreeSet<String> = BTreeSet::new();
    let mut claims: HashMap<String, BTreeSet<String>> = HashMap::new();
    for slot in schema_view.slot_views().unwrap_or_default() {
        let Ok(canonical) = slot.canonical_uri().to_uri(&conv) else {
            continue;
        };
        canonical_spellings.insert(canonical.0.clone());
        let native = schema_view.get_uri(slot.schema_id(), &slot.name);
        let Ok(native) = native.to_uri(&conv) else {
            continue;
        };
        if native.0 != canonical.0 {
            claims.entry(native.0).or_default().insert(canonical.0);
        }
    }

    let mut mapping = HashMap::new();
    for alias in candidates {
        // The IRI names a predicate that really is in the data. Whatever else
        // it is the native spelling of, that reading stands.
        if canonical_spellings.contains(alias) {
            continue;
        }
        let Some(canonical) = claims.get(alias) else {
            continue;
        };
        if canonical.len() > 1 {
            return Err(AmbiguousAlias {
                alias: alias.clone(),
                canonical: canonical.iter().cloned().collect(),
            });
        }
        if let Some(only) = canonical.iter().next() {
            mapping.insert(alias.clone(), only.clone());
        }
    }
    Ok(mapping)
}

/// Every fixed predicate the pattern reads instance data through.
fn collect(pattern: &GraphPattern, out: &mut BTreeSet<String>) {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for triple in patterns {
                if let NamedNodePattern::NamedNode(node) = &triple.predicate {
                    out.insert(node.as_str().to_owned());
                }
            }
        }
        GraphPattern::Path { path, .. } => collect_path(path, out),
        // Not instance data — see the module docs.
        GraphPattern::Graph { .. } => {}
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Minus { left, right } => {
            collect(left, out);
            collect(right, out);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            collect(left, out);
            collect(right, out);
            if let Some(expression) = expression {
                collect_expression(expression, out);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            collect_expression(expr, out);
            collect(inner, out);
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            collect_expression(expression, out);
            collect(inner, out);
        }
        GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Service { inner, .. } => collect(inner, out),
        GraphPattern::Values { .. } => {}
    }
}

fn collect_path(path: &PropertyPathExpression, out: &mut BTreeSet<String>) {
    match path {
        PropertyPathExpression::NamedNode(node) => {
            out.insert(node.as_str().to_owned());
        }
        PropertyPathExpression::Reverse(inner)
        | PropertyPathExpression::ZeroOrMore(inner)
        | PropertyPathExpression::OneOrMore(inner)
        | PropertyPathExpression::ZeroOrOne(inner) => collect_path(inner, out),
        PropertyPathExpression::Sequence(left, right)
        | PropertyPathExpression::Alternative(left, right) => {
            collect_path(left, out);
            collect_path(right, out);
        }
        PropertyPathExpression::NegatedPropertySet(nodes) => {
            for node in nodes {
                out.insert(node.as_str().to_owned());
            }
        }
    }
}

/// Exhaustive over [`Expression`] on purpose, so a spargebra release that adds
/// a variant is a compile error rather than one more position where an alias is
/// silently not resolved.
fn collect_expression(expr: &Expression, out: &mut BTreeSet<String>) {
    match expr {
        Expression::Exists(pattern) => collect(pattern, out),
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
        | Expression::Divide(left, right) => {
            collect_expression(left, out);
            collect_expression(right, out);
        }
        Expression::In(value, candidates) => {
            collect_expression(value, out);
            for candidate in candidates {
                collect_expression(candidate, out);
            }
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            collect_expression(inner, out)
        }
        Expression::If(condition, then, otherwise) => {
            collect_expression(condition, out);
            collect_expression(then, out);
            collect_expression(otherwise, out);
        }
        Expression::Coalesce(parts) | Expression::FunctionCall(_, parts) => {
            for part in parts {
                collect_expression(part, out);
            }
        }
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => {}
    }
}

/// The same walk as [`collect`], writing instead of reading.
fn apply(pattern: &mut GraphPattern, mapping: &HashMap<String, String>) {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for triple in patterns.iter_mut() {
                if let NamedNodePattern::NamedNode(node) = &triple.predicate
                    && let Some(canonical) = mapping.get(node.as_str())
                    && let Ok(replacement) = spargebra::term::NamedNode::new(canonical)
                {
                    triple.predicate = NamedNodePattern::NamedNode(replacement);
                }
            }
        }
        GraphPattern::Path { path, .. } => apply_path(path, mapping),
        GraphPattern::Graph { .. } => {}
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Minus { left, right } => {
            apply(left, mapping);
            apply(right, mapping);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            apply(left, mapping);
            apply(right, mapping);
            if let Some(expression) = expression {
                apply_expression(expression, mapping);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            apply_expression(expr, mapping);
            apply(inner, mapping);
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            apply_expression(expression, mapping);
            apply(inner, mapping);
        }
        GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Service { inner, .. } => apply(inner, mapping),
        GraphPattern::Values { .. } => {}
    }
}

fn apply_path(path: &mut PropertyPathExpression, mapping: &HashMap<String, String>) {
    match path {
        PropertyPathExpression::NamedNode(node) => {
            if let Some(canonical) = mapping.get(node.as_str())
                && let Ok(replacement) = spargebra::term::NamedNode::new(canonical)
            {
                *node = replacement;
            }
        }
        PropertyPathExpression::Reverse(inner)
        | PropertyPathExpression::ZeroOrMore(inner)
        | PropertyPathExpression::OneOrMore(inner)
        | PropertyPathExpression::ZeroOrOne(inner) => apply_path(inner, mapping),
        PropertyPathExpression::Sequence(left, right)
        | PropertyPathExpression::Alternative(left, right) => {
            apply_path(left, mapping);
            apply_path(right, mapping);
        }
        PropertyPathExpression::NegatedPropertySet(nodes) => {
            for node in nodes.iter_mut() {
                if let Some(canonical) = mapping.get(node.as_str())
                    && let Ok(replacement) = spargebra::term::NamedNode::new(canonical)
                {
                    *node = replacement;
                }
            }
        }
    }
}

fn apply_expression(expr: &mut Expression, mapping: &HashMap<String, String>) {
    match expr {
        Expression::Exists(pattern) => apply(pattern, mapping),
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
        | Expression::Divide(left, right) => {
            apply_expression(left, mapping);
            apply_expression(right, mapping);
        }
        Expression::In(value, candidates) => {
            apply_expression(value, mapping);
            for candidate in candidates.iter_mut() {
                apply_expression(candidate, mapping);
            }
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            apply_expression(inner, mapping)
        }
        Expression::If(condition, then, otherwise) => {
            apply_expression(condition, mapping);
            apply_expression(then, mapping);
            apply_expression(otherwise, mapping);
        }
        Expression::Coalesce(parts) | Expression::FunctionCall(_, parts) => {
            for part in parts.iter_mut() {
                apply_expression(part, mapping);
            }
        }
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use linkml_meta::SchemaDefinition;
    use serde_path_to_error as p2e;
    use serde_yml as yml;

    /// Two schemas, the shape the asset360 datamodel has: an RSM subset whose
    /// `name` declares a `slot_uri`, and the project schema whose own `name`
    /// declares none.
    fn schema_view() -> SchemaView {
        let rsm = r#"
id: https://w3id.org/infrabel/rsm
name: rsm
prefixes:
  linkml: https://w3id.org/linkml/
  RSM: http://rsm.uic.org/RSM12
  irsm: https://data.infrabel.be/asset360-rsm-subset/
default_prefix: irsm
default_range: string
classes:
  Track:
    class_uri: RSM:#EAID_TRACK
    attributes:
      asset360_uri:
        identifier: true
      name:
        range: string
        slot_uri: RSM:#EAID_NAME
"#;
        let asset360 = r#"
id: https://data.infrabel.be/asset360
name: asset360
prefixes:
  linkml: https://w3id.org/linkml/
  asset360: https://data.infrabel.be/asset360/
default_prefix: asset360
default_range: string
classes:
  Zone:
    class_uri: asset360:Zone
    attributes:
      asset360_uri:
        identifier: true
      name:
        range: string
"#;
        let mut sv = SchemaView::new();
        for raw in [rsm, asset360] {
            let schema: SchemaDefinition =
                p2e::deserialize(yml::Deserializer::from_str(raw)).unwrap();
            sv.add_schema(schema).unwrap();
        }
        sv
    }

    const NATIVE: &str = "https://data.infrabel.be/asset360-rsm-subset/name";
    const CANONICAL: &str = "http://rsm.uic.org/RSM12#EAID_NAME";
    const ZONE_NAME: &str = "https://data.infrabel.be/asset360/name";

    fn rewrite(query: &str) -> (Query, Vec<Rewrite>) {
        let sv = schema_view();
        let mut parsed = crate::sparql_scoper::parse_query(query).expect("parses");
        let rewrites = canonicalize_predicates(&mut parsed, &sv).expect("no ambiguity");
        (parsed, rewrites)
    }

    #[test]
    fn the_native_spelling_becomes_the_one_the_data_uses() {
        let (parsed, rewrites) = rewrite(&format!("SELECT ?n WHERE {{ ?t <{NATIVE}> ?n }}"));
        assert_eq!(
            rewrites,
            vec![Rewrite {
                from: NATIVE.to_owned(),
                to: CANONICAL.to_owned(),
            }]
        );
        assert!(parsed.to_string().contains(CANONICAL));
        assert!(!parsed.to_string().contains(NATIVE));
    }

    #[test]
    fn the_canonical_spelling_is_left_alone() {
        let (parsed, rewrites) = rewrite(&format!("SELECT ?n WHERE {{ ?t <{CANONICAL}> ?n }}"));
        assert!(rewrites.is_empty());
        assert!(parsed.to_string().contains(CANONICAL));
    }

    /// A slot that declares no `slot_uri` has one spelling, and it is already
    /// the one in the data. Nothing about this feature may touch it.
    #[test]
    fn a_slot_without_a_slot_uri_is_untouched() {
        let (parsed, rewrites) = rewrite(&format!("SELECT ?n WHERE {{ ?z <{ZONE_NAME}> ?n }}"));
        assert!(rewrites.is_empty());
        assert!(parsed.to_string().contains(ZONE_NAME));
    }

    /// The alias reaches every position a predicate can be read from, because
    /// the failure this fixes is an `OPTIONAL` answering an empty column.
    #[test]
    fn the_alias_is_resolved_inside_optional_union_and_exists() {
        for query in [
            format!("SELECT ?n WHERE {{ ?t a <urn:x> OPTIONAL {{ ?t <{NATIVE}> ?n }} }}"),
            format!("SELECT ?n WHERE {{ {{ ?t <{NATIVE}> ?n }} UNION {{ ?t a <urn:x> }} }}"),
            format!("SELECT ?t WHERE {{ ?t a <urn:x> FILTER EXISTS {{ ?t <{NATIVE}> ?n }} }}"),
            format!("SELECT ?n WHERE {{ ?t <{NATIVE}>+ ?n }}"),
        ] {
            let (parsed, rewrites) = rewrite(&query);
            assert_eq!(rewrites.len(), 1, "{query}");
            assert!(parsed.to_string().contains(CANONICAL), "{query}");
            assert!(!parsed.to_string().contains(NATIVE), "{query}");
        }
    }

    /// A variable predicate asks what the data says, and the data says the
    /// canonical IRI. Rewriting is not possible here and must not be faked.
    #[test]
    fn an_open_predicate_is_not_rewritten() {
        let (_, rewrites) = rewrite("SELECT ?p WHERE { ?t ?p ?o }");
        assert!(rewrites.is_empty());
    }

    /// The `CONSTRUCT` template is the author's chosen output spelling.
    #[test]
    fn a_construct_template_keeps_the_spelling_it_was_given() {
        let (parsed, rewrites) = rewrite(&format!(
            "CONSTRUCT {{ ?t <{NATIVE}> ?n }} WHERE {{ ?t <{NATIVE}> ?n }}"
        ));
        assert_eq!(rewrites.len(), 1);
        let rendered = parsed.to_string();
        assert!(rendered.contains(NATIVE), "template kept: {rendered}");
        assert!(rendered.contains(CANONICAL), "where rewritten: {rendered}");
    }

    /// Named graphs are not instance data; the only one served holds the
    /// datamodel, whose predicates are RDF vocabulary.
    #[test]
    fn a_predicate_inside_a_graph_clause_is_left_alone() {
        let (parsed, rewrites) = rewrite(&format!(
            "SELECT ?n WHERE {{ GRAPH <urn:g> {{ ?t <{NATIVE}> ?n }} }}"
        ));
        assert!(rewrites.is_empty());
        assert!(parsed.to_string().contains(NATIVE));
    }

    /// Two schemas sharing a default prefix, each with a `name` carrying a
    /// different `slot_uri`: there is no defensible pick, so the alias is
    /// refused by name rather than resolved to whichever the index preferred.
    #[test]
    fn an_alias_naming_two_slots_is_refused() {
        let one = r#"
id: https://example.org/one
name: one
prefixes:
  linkml: https://w3id.org/linkml/
  shared: https://example.org/shared/
  ext: https://example.org/ext/
default_prefix: shared
default_range: string
classes:
  A:
    attributes:
      label:
        slot_uri: ext:labelA
"#;
        let two = r#"
id: https://example.org/two
name: two
prefixes:
  linkml: https://w3id.org/linkml/
  shared: https://example.org/shared/
  ext: https://example.org/ext/
default_prefix: shared
default_range: string
classes:
  B:
    attributes:
      label:
        slot_uri: ext:labelB
"#;
        let mut sv = SchemaView::new();
        for raw in [one, two] {
            let schema: SchemaDefinition =
                p2e::deserialize(yml::Deserializer::from_str(raw)).unwrap();
            sv.add_schema(schema).unwrap();
        }
        let mut parsed = crate::sparql_scoper::parse_query(
            "SELECT ?l WHERE { ?a <https://example.org/shared/label> ?l }",
        )
        .expect("parses");
        let error = canonicalize_predicates(&mut parsed, &sv).expect_err("ambiguous");
        assert_eq!(error.alias, "https://example.org/shared/label");
        assert_eq!(
            error.canonical,
            vec![
                "https://example.org/ext/labelA".to_owned(),
                "https://example.org/ext/labelB".to_owned(),
            ]
        );
    }
}
