//! SHACL Turtle parser → AST.
//!
//! Parses `.shacl.ttl` files into [`ShapeResult`] structs. Feature-gated
//! behind `shacl-parser` (uses `oxttl`/`oxrdf`).

use std::collections::HashMap;
use std::fmt;

use oxrdf::{Literal, NamedOrBlankNode, Term};
use oxttl::TurtleParser;

use crate::shacl_ast::*;

// ── Well-known IRIs ──────────────────────────────────────────────────

const SH: &str = "http://www.w3.org/ns/shacl#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";

const ASSET360: &str = "https://data.infrabel.be/asset360/";

fn sh(local: &str) -> String {
    format!("{SH}{local}")
}

fn a360(local: &str) -> String {
    format!("{ASSET360}{local}")
}

// ── Error type ───────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ParseError {
    Turtle(String),
    UnsupportedConstruct(String),
    MissingField(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Turtle(e) => write!(f, "Turtle parse error: {e}"),
            ParseError::UnsupportedConstruct(msg) => {
                write!(f, "Unsupported SHACL construct: {msg}")
            }
            ParseError::MissingField(msg) => write!(f, "Missing required field: {msg}"),
        }
    }
}

impl std::error::Error for ParseError {}

// ── Triple store ─────────────────────────────────────────────────────

/// Simple in-memory triple store for walking parsed RDF.
struct TripleStore {
    /// All triples indexed by subject string.
    by_subject: HashMap<String, Vec<(String, Term)>>,
}

impl TripleStore {
    fn parse(ttl: &str) -> Result<Self, ParseError> {
        let mut by_subject: HashMap<String, Vec<(String, Term)>> = HashMap::new();
        let parser = TurtleParser::new().for_reader(ttl.as_bytes());
        for result in parser {
            let triple = result.map_err(|e| ParseError::Turtle(e.to_string()))?;
            let subj_key = subject_key(&triple.subject);
            by_subject
                .entry(subj_key)
                .or_default()
                .push((triple.predicate.as_str().to_owned(), triple.object));
        }
        Ok(Self { by_subject })
    }

    fn objects(&self, subject: &str, predicate: &str) -> Vec<&Term> {
        self.by_subject
            .get(subject)
            .map(|pairs| {
                pairs
                    .iter()
                    .filter(|(p, _)| p == predicate)
                    .map(|(_, o)| o)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn first_object(&self, subject: &str, predicate: &str) -> Option<&Term> {
        self.objects(subject, predicate).into_iter().next()
    }

    fn first_str(&self, subject: &str, predicate: &str) -> Option<String> {
        self.first_object(subject, predicate).and_then(term_str)
    }

    fn first_literal(&self, subject: &str, predicate: &str) -> Option<String> {
        self.first_object(subject, predicate).and_then(|t| match t {
            Term::Literal(lit) => Some(lit.value().to_owned()),
            _ => None,
        })
    }

    /// Pick the best literal for `predicate` given a preferred language.
    ///
    /// Priority: exact language match > untagged literal > first available.
    fn literal_for_language(
        &self,
        subject: &str,
        predicate: &str,
        language: &str,
    ) -> Option<String> {
        let literals: Vec<&Literal> = self
            .objects(subject, predicate)
            .into_iter()
            .filter_map(|t| match t {
                Term::Literal(lit) => Some(lit),
                _ => None,
            })
            .collect();
        if literals.is_empty() {
            return None;
        }
        // Exact language match
        if let Some(lit) = literals.iter().find(|l| l.language() == Some(language)) {
            return Some(lit.value().to_owned());
        }
        // Untagged fallback
        if let Some(lit) = literals.iter().find(|l| l.language().is_none()) {
            return Some(lit.value().to_owned());
        }
        // First available
        Some(literals[0].value().to_owned())
    }

    /// List the distinct predicates present on a given subject node (for diagnostics).
    fn list_predicates(&self, subject: &str) -> Vec<String> {
        let mut predicates: Vec<String> = self
            .by_subject
            .get(subject)
            .map(|pairs| {
                pairs
                    .iter()
                    .map(|(p, _)| iri_local_name(p).to_owned())
                    .collect()
            })
            .unwrap_or_default();
        predicates.sort();
        predicates.dedup();
        predicates
    }

    /// Collect an RDF list (rdf:first/rdf:rest chain) starting from a term.
    fn collect_rdf_list<'a>(&'a self, head: &'a Term) -> Vec<&'a Term> {
        let mut result = Vec::new();
        let mut current = head;
        loop {
            let key = term_key(current);
            if key == RDF_NIL {
                break;
            }
            if let Some(first) = self.first_object(&key, RDF_FIRST) {
                result.push(first);
            } else {
                break;
            }
            if let Some(rest) = self.first_object(&key, RDF_REST) {
                current = rest;
            } else {
                break;
            }
        }
        result
    }
}

fn subject_key(s: &NamedOrBlankNode) -> String {
    match s {
        NamedOrBlankNode::NamedNode(n) => n.as_str().to_owned(),
        NamedOrBlankNode::BlankNode(b) => format!("_:{}", b.as_str()),
    }
}

#[allow(unreachable_patterns)]
fn term_key(t: &Term) -> String {
    match t {
        Term::NamedNode(n) => n.as_str().to_owned(),
        Term::BlankNode(b) => format!("_:{}", b.as_str()),
        Term::Literal(l) => l.value().to_owned(),
        _ => String::new(),
    }
}

fn term_str(t: &Term) -> Option<String> {
    match t {
        Term::NamedNode(n) => Some(n.as_str().to_owned()),
        Term::Literal(l) => Some(l.value().to_owned()),
        _ => None,
    }
}

fn iri_local_name(iri: &str) -> &str {
    iri.rsplit_once('#')
        .or_else(|| iri.rsplit_once('/'))
        .map(|(_, name)| name)
        .unwrap_or(iri)
}

// ── Public API ───────────────────────────────────────────────────────

/// Parse a SHACL Turtle file and extract shapes targeting `target_class`.
///
/// If `target_class` is empty, all shapes are returned.
/// `language` selects the preferred language for `sh:message` (e.g. `"nl"`, `"en"`).
/// When empty, the first available literal is used.
pub fn parse_shacl(
    ttl: &str,
    target_class: &str,
    language: &str,
) -> Result<Vec<ShapeResult>, ParseError> {
    let store = TripleStore::parse(ttl)?;
    let mut results = Vec::new();

    // Find all sh:NodeShape subjects
    for (subj, pairs) in &store.by_subject {
        let is_node_shape = pairs
            .iter()
            .any(|(p, o)| p == RDF_TYPE && term_key(o) == sh("NodeShape"));
        if !is_node_shape {
            continue;
        }

        // Check target class
        let shape_target = store.first_str(subj, &sh("targetClass"));
        if !target_class.is_empty() {
            if let Some(ref tc) = shape_target {
                let tc_local = iri_local_name(tc);
                if tc_local != target_class && tc != target_class {
                    continue;
                }
            } else {
                continue;
            }
        }

        let target_class_name = shape_target
            .as_deref()
            .map(iri_local_name)
            .unwrap_or("")
            .to_owned();

        // Read annotations
        let enforcement_str = store
            .first_literal(subj, &a360("enforcementLevel"))
            .unwrap_or_else(|| "serious".to_owned());
        let enforcement_level = match enforcement_str.as_str() {
            "critical" => EnforcementLevel::Critical,
            "serious" => EnforcementLevel::Serious,
            "error" => EnforcementLevel::Error,
            "unlikely" => EnforcementLevel::Unlikely,
            _ => EnforcementLevel::default(),
        };

        let introspectable_ann = store
            .first_literal(subj, &a360("introspectable"))
            .map(|s| s == "true")
            .unwrap_or(true); // default: attempt introspection

        let message = store
            .literal_for_language(subj, &sh("message"), language)
            .unwrap_or_default();

        // Check if shape uses SPARQL
        let sparql_node = store.first_object(subj, &sh("sparql"));
        if let Some(sparql_term) = sparql_node {
            let sparql_key = term_key(sparql_term);
            let select = store
                .first_literal(&sparql_key, &sh("select"))
                .unwrap_or_default();
            let sparql_message = store
                .literal_for_language(&sparql_key, &sh("message"), language)
                .unwrap_or_else(|| message.clone());

            // Extract affected fields from SPARQL BIND patterns
            let affected_fields = extract_bind_fields_from_sparql(&select);

            results.push(ShapeResult {
                shape_uri: subj.clone(),
                target_class: target_class_name,
                enforcement_level,
                message: sparql_message,
                affected_fields,
                introspectable: false,
                ast: None,
                sparql: Some(select),
            });
            continue;
        }

        // Try to parse as introspectable AST
        match parse_shape_ast(&store, subj) {
            Ok(ast) => {
                let affected_fields = collect_affected_fields(&ast);
                results.push(ShapeResult {
                    shape_uri: subj.clone(),
                    target_class: target_class_name,
                    enforcement_level,
                    message,
                    affected_fields,
                    introspectable: introspectable_ann,
                    ast: Some(ast),
                    sparql: None,
                });
            }
            Err(e) if introspectable_ann => {
                return Err(e);
            }
            Err(_) => {
                // Annotation says non-introspectable, but no SPARQL either.
                // Treat as non-introspectable with no AST.
                results.push(ShapeResult {
                    shape_uri: subj.clone(),
                    target_class: target_class_name,
                    enforcement_level,
                    message,
                    affected_fields: vec![],
                    introspectable: false,
                    ast: None,
                    sparql: None,
                });
            }
        }
    }
    Ok(results)
}

// ── AST building ─────────────────────────────────────────────────────

fn parse_shape_ast(store: &TripleStore, shape_key: &str) -> Result<ShaclAst, ParseError> {
    // Collect all constraint components on this shape node
    let mut constraints = Vec::new();

    // sh:not
    for obj in store.objects(shape_key, &sh("not")) {
        let inner = parse_constraint_node(store, &term_key(obj))?;
        constraints.push(ShaclAst::Not {
            child: Box::new(inner),
        });
    }

    // sh:and (top-level)
    for obj in store.objects(shape_key, &sh("and")) {
        let items = store.collect_rdf_list(obj);
        let children = items
            .into_iter()
            .map(|item| parse_constraint_node(store, &term_key(item)))
            .collect::<Result<Vec<_>, _>>()?;
        constraints.push(ShaclAst::And { children });
    }

    // sh:or (top-level)
    for obj in store.objects(shape_key, &sh("or")) {
        let items = store.collect_rdf_list(obj);
        let children = items
            .into_iter()
            .map(|item| parse_constraint_node(store, &term_key(item)))
            .collect::<Result<Vec<_>, _>>()?;
        constraints.push(ShaclAst::Or { children });
    }

    // sh:property (top-level property shapes)
    for obj in store.objects(shape_key, &sh("property")) {
        let prop_ast = parse_property_shape(store, &term_key(obj))?;
        constraints.push(prop_ast);
    }

    // sh:equals (property pair, top-level)
    if let (Some(path_a_term), Some(path_b_term)) = (
        store.first_object(shape_key, &sh("path")),
        store.first_object(shape_key, &sh("equals")),
    ) {
        constraints.push(ShaclAst::PathEquals {
            path_a: parse_path(store, path_a_term)?,
            path_b: parse_path(store, path_b_term)?,
        });
    }

    // sh:disjoint (property pair, top-level)
    if let (Some(path_a_term), Some(path_b_term)) = (
        store.first_object(shape_key, &sh("path")),
        store.first_object(shape_key, &sh("disjoint")),
    ) {
        constraints.push(ShaclAst::PathDisjoint {
            path_a: parse_path(store, path_a_term)?,
            path_b: parse_path(store, path_b_term)?,
        });
    }

    match constraints.len() {
        0 => Err(ParseError::MissingField(format!(
            "no constraint components found on shape {shape_key}"
        ))),
        1 => Ok(constraints.into_iter().next().unwrap()),
        _ => Ok(ShaclAst::And {
            children: constraints,
        }),
    }
}

fn parse_constraint_node(store: &TripleStore, key: &str) -> Result<ShaclAst, ParseError> {
    // Check what constraint components exist on this blank node

    // sh:not
    if let Some(inner) = store.first_object(key, &sh("not")) {
        let child = parse_constraint_node(store, &term_key(inner))?;
        return Ok(ShaclAst::Not {
            child: Box::new(child),
        });
    }

    // sh:and
    if let Some(list_head) = store.first_object(key, &sh("and")) {
        let items = store.collect_rdf_list(list_head);
        let children = items
            .into_iter()
            .map(|item| parse_constraint_node(store, &term_key(item)))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(ShaclAst::And { children });
    }

    // sh:or
    if let Some(list_head) = store.first_object(key, &sh("or")) {
        let items = store.collect_rdf_list(list_head);
        let children = items
            .into_iter()
            .map(|item| parse_constraint_node(store, &term_key(item)))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(ShaclAst::Or { children });
    }

    // sh:property (nested property shape)
    if let Some(prop_node) = store.first_object(key, &sh("property")) {
        return parse_property_shape(store, &term_key(prop_node));
    }

    // This node might itself be a property shape (has sh:path)
    if store.first_object(key, &sh("path")).is_some() {
        return parse_property_shape(store, key);
    }

    let predicates = store.list_predicates(key);
    Err(ParseError::UnsupportedConstruct(format!(
        "Unsupported SHACL construct on node {key}.\n\
         Found predicates: [{}].\n\
         Supported: sh:not, sh:and, sh:or, sh:property (with sh:path + value constraint).\n\
         Hint: Set `asset360:introspectable false` and use `sh:sparql` instead.",
        predicates.join(", ")
    )))
}

fn parse_property_shape(store: &TripleStore, key: &str) -> Result<ShaclAst, ParseError> {
    let path_term = store.first_object(key, &sh("path")).ok_or_else(|| {
        ParseError::MissingField(format!("sh:path missing on property shape {key}"))
    })?;
    let path = parse_path(store, path_term)?;

    // sh:hasValue
    if let Some(val_term) = store.first_object(key, &sh("hasValue")) {
        let value = term_to_json_value(val_term);
        return Ok(ShaclAst::PropEquals { path, value });
    }

    // sh:in
    if let Some(list_head) = store.first_object(key, &sh("in")) {
        let items = store.collect_rdf_list(list_head);
        let values = items.into_iter().map(term_to_json_value).collect();
        return Ok(ShaclAst::PropIn { path, values });
    }

    // sh:minCount / sh:maxCount
    let min_count = store
        .first_literal(key, &sh("minCount"))
        .and_then(|s| s.parse::<u32>().ok());
    let max_count = store
        .first_literal(key, &sh("maxCount"))
        .and_then(|s| s.parse::<u32>().ok());
    if min_count.is_some() || max_count.is_some() {
        return Ok(ShaclAst::PropCount {
            path,
            min: min_count,
            max: max_count,
        });
    }

    // sh:equals (property pair constraint)
    if let Some(other_path_term) = store.first_object(key, &sh("equals")) {
        let other_path = parse_path(store, other_path_term)?;
        return Ok(ShaclAst::PathEquals {
            path_a: path,
            path_b: other_path,
        });
    }

    // sh:disjoint (property pair constraint)
    if let Some(other_path_term) = store.first_object(key, &sh("disjoint")) {
        let other_path = parse_path(store, other_path_term)?;
        return Ok(ShaclAst::PathDisjoint {
            path_a: path,
            path_b: other_path,
        });
    }

    let path_name = path.local_name().unwrap_or("(complex path)");
    let predicates = store.list_predicates(key);
    Err(ParseError::UnsupportedConstruct(format!(
        "Unsupported value constraint on property \"{path_name}\" (node {key}).\n\
         Found predicates: [{}].\n\
         Supported property constraints: sh:hasValue, sh:in, sh:minCount, sh:maxCount, sh:equals, sh:disjoint.\n\
         Common unsupported: sh:pattern, sh:class, sh:nodeKind, sh:datatype, sh:minInclusive/maxInclusive, sh:minLength/maxLength.\n\
         Hint: Set `asset360:introspectable false` and use `sh:sparql` for this constraint.",
        predicates.join(", ")
    )))
}

fn parse_path(store: &TripleStore, term: &Term) -> Result<PropertyPath, ParseError> {
    match term {
        Term::NamedNode(n) => Ok(PropertyPath::iri(n.as_str())),
        Term::BlankNode(b) => {
            let key = format!("_:{}", b.as_str());

            // sh:inversePath
            if let Some(inner) = store.first_object(&key, &sh("inversePath")) {
                let inner_path = parse_path(store, inner)?;
                return Ok(PropertyPath::inverse(inner_path));
            }

            // RDF list (sequence path)
            let items = store.collect_rdf_list(term);
            if !items.is_empty() {
                let steps = items
                    .into_iter()
                    .map(|item| parse_path(store, item))
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(PropertyPath::sequence(steps));
            }

            let predicates = store.list_predicates(&key);
            Err(ParseError::UnsupportedConstruct(format!(
                "Unsupported property path at blank node {key}.\n\
                 Found predicates: [{}].\n\
                 Supported paths: simple IRI, sequence (RDF list), sh:inversePath.\n\
                 Hint: sh:alternativePath, sh:zeroOrMorePath etc. are not supported.",
                predicates.join(", ")
            )))
        }
        _ => Err(ParseError::UnsupportedConstruct(format!(
            "Unexpected term in path position: {term:?}.\n\
             Paths must be IRIs (e.g. asset360:fieldName) or structured (sh:inversePath, sequence)."
        ))),
    }
}

#[allow(unreachable_patterns)]
fn term_to_json_value(t: &Term) -> serde_json::Value {
    match t {
        Term::Literal(l) => {
            // Try numeric
            if let Ok(n) = l.value().parse::<i64>() {
                return serde_json::Value::Number(n.into());
            }
            if let Ok(n) = l.value().parse::<f64>()
                && let Some(num) = serde_json::Number::from_f64(n)
            {
                return serde_json::Value::Number(num);
            }
            // Boolean
            match l.value() {
                "true" => return serde_json::Value::Bool(true),
                "false" => return serde_json::Value::Bool(false),
                _ => {}
            }
            serde_json::Value::String(l.value().to_owned())
        }
        Term::NamedNode(n) => serde_json::Value::String(n.as_str().to_owned()),
        Term::BlankNode(b) => serde_json::Value::String(format!("_:{}", b.as_str())),
        _ => serde_json::Value::Null,
    }
}

fn extract_bind_fields_from_sparql(sparql: &str) -> Vec<String> {
    // Extract field names from BIND(prefix:xxx AS ?path) patterns in SPARQL
    let mut fields = Vec::new();
    for line in sparql.lines() {
        let trimmed = line.trim();
        // Find BIND( anywhere in the line (handles { BIND(...) } wrapping)
        if let Some(start) = trimmed.find("BIND(") {
            let rest = &trimmed[start + 5..];
            if let Some(end) = rest.find(" AS") {
                let iri = rest[..end].trim();
                // Handle both full IRIs (asset360/foo) and prefixed names (asset360:foo)
                let local = iri
                    .rsplit_once('#')
                    .or_else(|| iri.rsplit_once('/'))
                    .or_else(|| iri.rsplit_once(':'))
                    .map(|(_, name)| name)
                    .unwrap_or(iri);
                fields.push(local.to_owned());
            }
        }
    }
    fields.sort();
    fields.dedup();
    fields
}

fn collect_affected_fields(ast: &ShaclAst) -> Vec<String> {
    let mut fields = Vec::new();
    collect_fields_recursive(ast, &mut fields);
    fields.sort();
    fields.dedup();
    fields
}

fn collect_fields_recursive(ast: &ShaclAst, fields: &mut Vec<String>) {
    match ast {
        ShaclAst::And { children } | ShaclAst::Or { children } => {
            for child in children {
                collect_fields_recursive(child, fields);
            }
        }
        ShaclAst::Not { child } => collect_fields_recursive(child, fields),
        ShaclAst::PropEquals { path, .. }
        | ShaclAst::PropIn { path, .. }
        | ShaclAst::PropCount { path, .. } => {
            if let Some(name) = path.local_name() {
                fields.push(name.to_owned());
            }
        }
        ShaclAst::PathEquals { path_a, path_b } | ShaclAst::PathDisjoint { path_a, path_b } => {
            if let Some(name) = path_a.local_name() {
                fields.push(name.to_owned());
            }
            if let Some(name) = path_b.local_name() {
                fields.push(name.to_owned());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATUS_COMBO_TTL: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

asset360:TunnelComponent_ForbiddenStatusComboShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComponent ;
  asset360:enforcementLevel "serious" ;
  asset360:introspectable true ;
  sh:message "Forbidden: ceAssetPrimaryStatus incompatible with ceAssetSecondaryStatus." ;
  sh:not [
    sh:or (
      [
        sh:and (
          [ sh:property [ sh:path asset360:ceAssetPrimaryStatus ; sh:hasValue "In_voorbereiding" ] ]
          [ sh:property [ sh:path asset360:ceAssetSecondaryStatus ; sh:hasValue "Verkocht" ] ]
        )
      ]
      [
        sh:and (
          [ sh:property [ sh:path asset360:ceAssetPrimaryStatus ; sh:hasValue "In_voorbereiding" ] ]
          [ sh:property [ sh:path asset360:ceAssetSecondaryStatus ; sh:hasValue "Afgebroken" ] ]
        )
      ]
    )
  ] .
"#;

    const DELEGATE_TTL: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TunnelComponent_DelegateUniquenessShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComponent ;
  asset360:enforcementLevel "serious" ;
  asset360:introspectable false ;
  sh:sparql [
    sh:message "Only one tunnel component per tunnel complex can be marked as delegate." ;
    sh:select """
      SELECT $this ?path
      WHERE {
        $this asset360:belongsToTunnelComplex ?complex ;
              asset360:isTunnelDelegate true .
        ?other asset360:belongsToTunnelComplex ?complex ;
               asset360:isTunnelDelegate true .
        FILTER(?other != $this)
        { BIND(asset360:isTunnelDelegate AS ?path) }
        UNION
        { BIND(asset360:belongsToTunnelComplex AS ?path) }
      }
    """ ;
  ] .
"#;

    #[test]
    fn test_parse_introspectable_shape() {
        let results = parse_shacl(STATUS_COMBO_TTL, "TunnelComponent", "").unwrap();
        assert_eq!(results.len(), 1);
        let shape = &results[0];
        assert!(shape.introspectable);
        assert_eq!(shape.target_class, "TunnelComponent");
        assert_eq!(shape.enforcement_level, EnforcementLevel::Serious);
        assert!(shape.ast.is_some());
        assert!(shape.sparql.is_none());

        // AST should be Not(Or(And(...), And(...)))
        let ast = shape.ast.as_ref().unwrap();
        match ast {
            ShaclAst::Not { child } => match child.as_ref() {
                ShaclAst::Or { children } => {
                    assert_eq!(children.len(), 2);
                    // Each child should be And with 2 PropEquals
                    for child in children {
                        match child {
                            ShaclAst::And { children: inner } => {
                                assert_eq!(inner.len(), 2);
                            }
                            _ => panic!("expected And, got {child:?}"),
                        }
                    }
                }
                _ => panic!("expected Or, got {child:?}"),
            },
            _ => panic!("expected Not, got {ast:?}"),
        }

        // Affected fields
        assert!(
            shape
                .affected_fields
                .contains(&"ceAssetPrimaryStatus".to_owned())
        );
        assert!(
            shape
                .affected_fields
                .contains(&"ceAssetSecondaryStatus".to_owned())
        );
    }

    #[test]
    fn test_parse_sparql_shape() {
        let results = parse_shacl(DELEGATE_TTL, "TunnelComponent", "").unwrap();
        assert_eq!(results.len(), 1);
        let shape = &results[0];
        assert!(!shape.introspectable);
        assert!(shape.ast.is_none());
        assert!(shape.sparql.is_some());
        assert_eq!(shape.enforcement_level, EnforcementLevel::Serious);
        assert!(shape.message.contains("Only one tunnel component"));

        // SPARQL-extracted fields
        assert!(
            shape
                .affected_fields
                .contains(&"isTunnelDelegate".to_owned())
        );
        assert!(
            shape
                .affected_fields
                .contains(&"belongsToTunnelComplex".to_owned())
        );
    }

    #[test]
    fn test_parse_combined_file() {
        let combined = format!("{STATUS_COMBO_TTL}\n{DELEGATE_TTL}");
        let results = parse_shacl(&combined, "TunnelComponent", "").unwrap();
        assert_eq!(results.len(), 2);

        let introspectable_count = results.iter().filter(|r| r.introspectable).count();
        let sparql_count = results.iter().filter(|r| r.sparql.is_some()).count();
        assert_eq!(introspectable_count, 1);
        assert_eq!(sparql_count, 1);
    }

    #[test]
    fn test_parse_empty_target_class_returns_all() {
        let results = parse_shacl(STATUS_COMBO_TTL, "", "").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_parse_wrong_target_class_returns_empty() {
        let results = parse_shacl(STATUS_COMBO_TTL, "Signal", "").unwrap();
        assert_eq!(results.len(), 0);
    }

    // ── Error message quality tests ─────────────────────────────────

    fn assert_error_contains(result: Result<Vec<ShapeResult>, ParseError>, needles: &[&str]) {
        let err = result.expect_err("expected a parse error");
        let msg = err.to_string();
        for needle in needles {
            assert!(
                msg.contains(needle),
                "Error message missing expected substring \"{needle}\".\nFull message:\n{msg}"
            );
        }
    }

    #[test]
    fn test_unsupported_sh_pattern_error() {
        let ttl = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TestShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComponent ;
  asset360:introspectable true ;
  sh:property [
    sh:path asset360:name ;
    sh:pattern "^[A-Z]"
  ] .
"#;
        let result = parse_shacl(ttl, "TunnelComponent", "");
        assert_error_contains(
            result,
            &[
                "Unsupported value constraint",
                "name",
                "sh:hasValue",
                "introspectable false",
                "pattern",
            ],
        );
    }

    #[test]
    fn test_unsupported_sh_class_error() {
        let ttl = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TestShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComponent ;
  asset360:introspectable true ;
  sh:property [
    sh:path asset360:belongsToTunnelComplex ;
    sh:class asset360:TunnelComplex
  ] .
"#;
        let result = parse_shacl(ttl, "TunnelComponent", "");
        assert_error_contains(
            result,
            &[
                "Unsupported value constraint",
                "belongsToTunnelComplex",
                "sh:hasValue",
                "sh:class",
                "introspectable false",
            ],
        );
    }

    #[test]
    fn test_unsupported_sh_datatype_error() {
        let ttl = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TestShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComponent ;
  asset360:introspectable true ;
  sh:property [
    sh:path asset360:length ;
    sh:datatype xsd:decimal
  ] .
"#;
        let result = parse_shacl(ttl, "TunnelComponent", "");
        assert_error_contains(
            result,
            &[
                "Unsupported value constraint",
                "length",
                "sh:datatype",
                "introspectable false",
            ],
        );
    }

    #[test]
    fn test_unsupported_alternative_path_error() {
        let ttl = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TestShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComponent ;
  asset360:introspectable true ;
  sh:property [
    sh:path [ sh:alternativePath ( asset360:name asset360:identification ) ] ;
    sh:minCount 1
  ] .
"#;
        let result = parse_shacl(ttl, "TunnelComponent", "");
        assert_error_contains(
            result,
            &[
                "Unsupported property path",
                "alternativePath",
                "sh:inversePath",
            ],
        );
    }

    // ── Language-tagged message tests ────────────────────────────────

    const MULTILANG_TTL: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TestShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComponent ;
  asset360:enforcementLevel "serious" ;
  asset360:introspectable true ;
  sh:message "Forbidden status combination"@en ;
  sh:message "Verboden statuscombinatie"@nl ;
  sh:message "Combinaison de statuts interdite"@fr ;
  sh:not [
    sh:and (
      [ sh:property [ sh:path asset360:ceAssetPrimaryStatus ; sh:hasValue "In_voorbereiding" ] ]
      [ sh:property [ sh:path asset360:ceAssetSecondaryStatus ; sh:hasValue "Verkocht" ] ]
    )
  ] .
"#;

    #[test]
    fn test_language_tagged_message_exact_match() {
        let results = parse_shacl(MULTILANG_TTL, "TunnelComponent", "nl").unwrap();
        assert_eq!(results[0].message, "Verboden statuscombinatie");
    }

    #[test]
    fn test_language_tagged_message_different_lang() {
        let results = parse_shacl(MULTILANG_TTL, "TunnelComponent", "fr").unwrap();
        assert_eq!(results[0].message, "Combinaison de statuts interdite");
    }

    #[test]
    fn test_language_tagged_message_fallback_to_first() {
        // No "de" tag available — should fall back to first available
        let results = parse_shacl(MULTILANG_TTL, "TunnelComponent", "de").unwrap();
        // No untagged literal, so picks first available
        assert!(!results[0].message.is_empty());
    }

    #[test]
    fn test_language_tagged_message_empty_lang_picks_any() {
        let results = parse_shacl(MULTILANG_TTL, "TunnelComponent", "").unwrap();
        assert!(!results[0].message.is_empty());
    }

    #[test]
    fn test_untagged_message_still_works() {
        // Original STATUS_COMBO_TTL has an untagged sh:message
        let results = parse_shacl(STATUS_COMBO_TTL, "TunnelComponent", "nl").unwrap();
        assert!(results[0].message.contains("Forbidden"));
    }
}
