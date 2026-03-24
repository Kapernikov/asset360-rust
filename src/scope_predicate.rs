//! Scope predicate derivation: determine which objects are relevant for a constraint.
//!
//! For single-object constraints (e.g. status combo), no scope is needed.
//! For cross-object constraints (e.g. delegate uniqueness), the scope predicate
//! selects peer objects that share a common attribute with the focus object.
//!
//! The returned Predicate can be evaluated via filter_query_interpreter to fetch
//! exactly the relevant peers from the database.

use crate::predicate::Predicate;
use crate::shacl_ast::ShapeResult;

/// Derive a scope predicate for a shape, given the focus object's data.
///
/// Returns `None` if the shape is single-object (no peer data needed) or if
/// the scope cannot be determined.
///
/// The `uri_field` parameter specifies the JSON field name that holds the
/// object's URI (typically `"asset360_uri"`).
pub fn derive_scope_predicate(
    shape: &ShapeResult,
    focus_data: &serde_json::Map<String, serde_json::Value>,
    uri_field: &str,
) -> Option<Predicate> {
    // Try explicit annotation first (future: asset360:scopePredicate)
    // Not yet implemented — will be added when annotation schema is defined.

    // For SPARQL-based shapes, try to extract scope from the query pattern
    if let Some(ref sparql) = shape.sparql {
        return derive_scope_from_sparql(sparql, focus_data, uri_field);
    }

    // For introspectable ASTs, check if cross-object paths are used
    if let Some(ref ast) = shape.ast {
        return derive_scope_from_ast(ast, focus_data, uri_field);
    }

    None
}

/// Extract scope predicate from SPARQL join patterns.
///
/// Recognizes the "shared attribute" pattern:
///   $this prefix:attr ?var .
///   ?other prefix:attr ?var .
///   FILTER(?other != $this)
///
/// This produces: AND(attr = focus[attr], NOT(uri = focus[uri]))
fn derive_scope_from_sparql(
    sparql: &str,
    focus_data: &serde_json::Map<String, serde_json::Value>,
    uri_field: &str,
) -> Option<Predicate> {
    let shared_attrs = extract_shared_attribute_joins(sparql);
    if shared_attrs.is_empty() {
        return None;
    }

    let focus_uri = focus_data.get(uri_field).filter(|v| !v.is_null())?;

    let mut predicates: Vec<Predicate> = Vec::new();

    // Add equality constraints for each shared attribute.
    // Skip when the focus value is null/missing — a null join attribute means
    // the cross-object constraint doesn't apply (e.g. a CivilEngineeringAsset
    // that doesn't belong to any TunnelComplex has no delegate uniqueness to check).
    for attr in &shared_attrs {
        let value = focus_data.get(attr).filter(|v| !v.is_null())?;
        predicates.push(Predicate::simple(attr, "equals", value.clone()));
    }

    // Exclude the focus object itself (FILTER(?other != $this))
    predicates.push(Predicate::negate(Predicate::simple(
        uri_field,
        "equals",
        focus_uri.clone(),
    )));

    Some(Predicate::and(predicates))
}

/// Extract shared attribute names from a SPARQL join pattern.
///
/// Parses the SPARQL into an algebra tree (via spargebra) and walks it to find
/// the "shared attribute" pattern:
///   $this prefix:attr ?joinVar .
///   ?other prefix:attr ?joinVar .
///
/// Falls back to text-based parsing when the `shacl-parser` feature is disabled.
fn extract_shared_attribute_joins(sparql: &str) -> Vec<String> {
    use spargebra::term::{NamedNodePattern, TermPattern};
    use spargebra::{Query, SparqlParser};

    let parser = SparqlParser::new()
        .with_prefix("asset360", "https://data.infrabel.be/asset360/")
        .expect("hardcoded prefix is valid");

    let query = match parser.parse_query(sparql) {
        Ok(q) => q,
        Err(_) => return Vec::new(),
    };

    let pattern = match query {
        Query::Select { pattern, .. } => pattern,
        _ => return Vec::new(),
    };

    // Collect all BGP triples from the algebra tree
    let mut triples = Vec::new();
    collect_bgp_triples(&pattern, &mut triples);

    // Classify triples by subject: $this vs other variables
    // this_bindings: (predicate_iri, var_name) where subject is ?this ($this)
    // other_bindings: (predicate_iri, var_name) where subject is any other ?variable
    let mut this_bindings: Vec<(&str, &str)> = Vec::new();
    let mut other_bindings: Vec<(&str, &str)> = Vec::new();

    for tp in &triples {
        let pred_iri = match &tp.predicate {
            NamedNodePattern::NamedNode(nn) => nn.as_str(),
            _ => continue,
        };

        let obj_var = match &tp.object {
            TermPattern::Variable(v) => v.as_str(),
            _ => continue,
        };

        match &tp.subject {
            TermPattern::Variable(v) if v.as_str() == "this" => {
                this_bindings.push((pred_iri, obj_var));
            }
            TermPattern::Variable(_) => {
                other_bindings.push((pred_iri, obj_var));
            }
            _ => {}
        }
    }

    // Find predicates shared between $this and another variable via the same join variable
    let mut shared = Vec::new();
    for (pred, var) in &this_bindings {
        for (other_pred, other_var) in &other_bindings {
            if pred == other_pred && var == other_var {
                shared.push(iri_local_name(pred).to_owned());
            }
        }
    }

    shared.sort();
    shared.dedup();
    shared
}

/// Recursively collect all BGP triple patterns from a SPARQL algebra tree.
fn collect_bgp_triples<'a>(
    pattern: &'a spargebra::algebra::GraphPattern,
    triples: &mut Vec<&'a spargebra::term::TriplePattern>,
) {
    use spargebra::algebra::GraphPattern;

    match pattern {
        GraphPattern::Bgp { patterns } => {
            triples.extend(patterns.iter());
        }
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_bgp_triples(left, triples);
            collect_bgp_triples(right, triples);
        }
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. } => {
            collect_bgp_triples(inner, triples);
        }
        GraphPattern::Path { .. } | GraphPattern::Values { .. } => {}
    }
}

/// Extract the local name from an IRI or prefixed name.
fn iri_local_name(iri: &str) -> &str {
    iri.rsplit_once('#')
        .or_else(|| iri.rsplit_once('/'))
        .or_else(|| iri.rsplit_once(':'))
        .map(|(_, name)| name)
        .unwrap_or(iri)
}

/// Derive scope from an introspectable AST.
///
/// For ASTs containing PathEquals or PathDisjoint, the cross-object paths
/// indicate which attributes are shared between the focus and peer objects.
/// Currently returns None — will be implemented when such shapes exist.
fn derive_scope_from_ast(
    _ast: &crate::shacl_ast::ShaclAst,
    _focus_data: &serde_json::Map<String, serde_json::Value>,
    _uri_field: &str,
) -> Option<Predicate> {
    // No cross-object AST shapes exist yet.
    // When PathEquals/PathDisjoint shapes are added, this will extract
    // the shared path and produce a scope predicate.
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shacl_ast::{EnforcementLevel, ShapeResult};
    use serde_json::json;

    fn delegate_shape() -> ShapeResult {
        ShapeResult {
            shape_uri: "https://data.infrabel.be/asset360/TunnelComponent_DelegateUniquenessShape"
                .to_owned(),
            target_class: "TunnelComponent".to_owned(),
            enforcement_level: EnforcementLevel::Serious,
            message: "Only one tunnel component per tunnel complex can be marked as delegate."
                .to_owned(),
            affected_fields: vec![
                "belongsToTunnelComplex".to_owned(),
                "isTunnelDelegate".to_owned(),
            ],
            introspectable: false,
            ast: None,
            sparql: Some(
                r#"
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
                "#
                .to_owned(),
            ),
        }
    }

    #[test]
    fn test_derive_scope_for_delegate_uniqueness() {
        let shape = delegate_shape();
        let mut focus = serde_json::Map::new();
        focus.insert(
            "asset360_uri".into(),
            json!("https://example.org/tunnel-component-42"),
        );
        focus.insert("belongsToTunnelComplex".into(), json!("complex-7"));
        focus.insert("isTunnelDelegate".into(), json!(true));

        let pred = derive_scope_predicate(&shape, &focus, "asset360_uri");
        assert!(pred.is_some(), "should produce a scope predicate");

        let pred = pred.unwrap();
        let json = serde_json::to_value(&pred).unwrap();

        // Should be AND(belongsToTunnelComplex = "complex-7", NOT(asset360_uri = focus.uri))
        assert_eq!(json["operator"], "AND");
        let predicates = json["predicates"].as_array().unwrap();
        assert_eq!(predicates.len(), 2);

        // First: shared attribute equality
        assert_eq!(predicates[0]["fieldId"], "belongsToTunnelComplex");
        assert_eq!(predicates[0]["predicateTypeId"], "equals");
        assert_eq!(predicates[0]["value"], "complex-7");

        // Second: exclude self
        assert_eq!(predicates[1]["operator"], "NOT");
        let inner = &predicates[1]["predicate"];
        assert_eq!(inner["fieldId"], "asset360_uri");
        assert_eq!(inner["value"], "https://example.org/tunnel-component-42");
    }

    #[test]
    fn test_no_scope_for_single_object_shape() {
        // Status combo shape has no SPARQL and no cross-object paths
        let shape = ShapeResult {
            shape_uri: "https://data.infrabel.be/asset360/StatusComboShape".to_owned(),
            target_class: "TunnelComponent".to_owned(),
            enforcement_level: EnforcementLevel::Serious,
            message: "Forbidden status combination".to_owned(),
            affected_fields: vec![
                "ceAssetPrimaryStatus".to_owned(),
                "ceAssetSecondaryStatus".to_owned(),
            ],
            introspectable: true,
            ast: Some(crate::shacl_ast::ShaclAst::Not {
                child: Box::new(crate::shacl_ast::ShaclAst::And { children: vec![] }),
            }),
            sparql: None,
        };

        let mut focus = serde_json::Map::new();
        focus.insert("asset360_uri".into(), json!("https://example.org/obj-1"));

        let pred = derive_scope_predicate(&shape, &focus, "asset360_uri");
        assert!(pred.is_none(), "single-object shapes need no scope");
    }

    #[test]
    fn test_missing_focus_data_returns_none() {
        let shape = delegate_shape();
        let focus = serde_json::Map::new(); // Missing all fields

        let pred = derive_scope_predicate(&shape, &focus, "asset360_uri");
        assert!(
            pred.is_none(),
            "missing focus data → can't fill scope predicate"
        );
    }

    #[test]
    fn test_null_shared_attribute_returns_none() {
        // When the focus object has belongsToTunnelComplex = null, there is no
        // tunnel complex to check delegate uniqueness against → no scope needed.
        // Previously this would produce belongsToTunnelComplex = null, matching
        // ALL objects where the field is null (potentially thousands).
        let shape = delegate_shape();
        let mut focus = serde_json::Map::new();
        focus.insert(
            "asset360_uri".into(),
            json!("https://example.org/tunnel-component-42"),
        );
        focus.insert("belongsToTunnelComplex".into(), serde_json::Value::Null);
        focus.insert("isTunnelDelegate".into(), json!(true));

        let pred = derive_scope_predicate(&shape, &focus, "asset360_uri");
        assert!(
            pred.is_none(),
            "null shared attribute → no scope (constraint does not apply)"
        );
    }

    #[test]
    fn test_null_uri_field_returns_none() {
        let shape = delegate_shape();
        let mut focus = serde_json::Map::new();
        focus.insert("asset360_uri".into(), serde_json::Value::Null);
        focus.insert("belongsToTunnelComplex".into(), json!("complex-7"));

        let pred = derive_scope_predicate(&shape, &focus, "asset360_uri");
        assert!(pred.is_none(), "null URI → no scope");
    }

    #[test]
    fn test_extract_shared_joins() {
        let sparql = r#"
            SELECT $this ?path
            WHERE {
                $this asset360:belongsToTunnelComplex ?complex ;
                      asset360:isTunnelDelegate true .
                ?other asset360:belongsToTunnelComplex ?complex ;
                       asset360:isTunnelDelegate true .
                FILTER(?other != $this)
            }
        "#;
        let shared = extract_shared_attribute_joins(sparql);
        assert_eq!(shared, vec!["belongsToTunnelComplex"]);
    }

    // ---- Robustness tests (T003) ----
    // These edge cases would break the old text-based parser but work with spargebra.

    #[test]
    fn test_multiline_triple_patterns() {
        // Triple pattern split across multiple lines — the old line-by-line
        // parser would not see the predicate and object on separate lines.
        let sparql = r#"
            SELECT $this ?path
            WHERE {
                $this
                    asset360:belongsToTunnelComplex
                    ?complex .
                ?other
                    asset360:belongsToTunnelComplex
                    ?complex .
                FILTER(?other != $this)
            }
        "#;
        let shared = extract_shared_attribute_joins(sparql);
        assert_eq!(shared, vec!["belongsToTunnelComplex"]);
    }

    #[test]
    fn test_sparql_comments_ignored() {
        // Comments in the SPARQL query — old parser would try to parse them
        // as triple patterns.
        let sparql = r#"
            # This query checks delegate uniqueness
            SELECT $this ?path
            WHERE {
                # Match the focus object's tunnel complex
                $this asset360:belongsToTunnelComplex ?complex .
                # Find other objects in the same complex
                ?other asset360:belongsToTunnelComplex ?complex .
                FILTER(?other != $this)
            }
        "#;
        let shared = extract_shared_attribute_joins(sparql);
        assert_eq!(shared, vec!["belongsToTunnelComplex"]);
    }

    #[test]
    fn test_string_literals_with_spaces() {
        // String literals containing spaces — old parser would split on
        // whitespace and misparse the triple.
        let sparql = r#"
            SELECT $this ?path
            WHERE {
                $this asset360:belongsToTunnelComplex ?complex ;
                      asset360:name "some name with spaces" .
                ?other asset360:belongsToTunnelComplex ?complex .
                FILTER(?other != $this)
            }
        "#;
        let shared = extract_shared_attribute_joins(sparql);
        assert_eq!(shared, vec!["belongsToTunnelComplex"]);
    }

    #[test]
    fn test_optional_pattern_in_constraint() {
        // OPTIONAL block — the algebra walker handles LeftJoin patterns.
        let sparql = r#"
            SELECT $this ?path
            WHERE {
                $this asset360:belongsToTunnelComplex ?complex .
                ?other asset360:belongsToTunnelComplex ?complex .
                OPTIONAL {
                    $this asset360:zone ?z .
                    ?other asset360:zone ?z .
                }
                FILTER(?other != $this)
            }
        "#;
        let shared = extract_shared_attribute_joins(sparql);
        // Both belongsToTunnelComplex and zone are shared
        assert!(shared.contains(&"belongsToTunnelComplex".to_owned()));
        assert!(shared.contains(&"zone".to_owned()));
    }

    #[test]
    fn test_no_shared_join_returns_empty() {
        // Query where $this and ?other don't share any join variable
        let sparql = r#"
            SELECT $this ?path
            WHERE {
                $this asset360:name ?thisName .
                ?other asset360:code ?otherCode .
                FILTER(?other != $this)
            }
        "#;
        let shared = extract_shared_attribute_joins(sparql);
        assert!(shared.is_empty(), "no shared join variable → empty");
    }

    #[test]
    fn test_invalid_sparql_returns_empty() {
        let shared = extract_shared_attribute_joins("NOT VALID SPARQL AT ALL {{{");
        assert!(shared.is_empty(), "parse failure → empty, no panic");
    }
}
