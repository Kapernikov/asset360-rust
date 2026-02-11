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

    let focus_uri = focus_data.get(uri_field)?;

    let mut predicates: Vec<Predicate> = Vec::new();

    // Add equality constraints for each shared attribute
    for attr in &shared_attrs {
        let value = focus_data.get(attr)?;
        predicates.push(Predicate::simple(attr, "equals", value.clone()));
    }

    // Exclude the focus object itself (FILTER(?other != $this))
    predicates.push(Predicate::not(Predicate::simple(
        uri_field,
        "equals",
        focus_uri.clone(),
    )));

    Some(Predicate::and(predicates))
}

/// Extract shared attribute names from a SPARQL join pattern.
///
/// Looks for the pattern where `$this` and another variable (`?other`, `?x`, etc.)
/// are both bound to the same intermediate variable via the same predicate:
///   $this prefix:attr ?joinVar .
///   ?other prefix:attr ?joinVar .
fn extract_shared_attribute_joins(sparql: &str) -> Vec<String> {
    // Parse triple patterns: subject predicate object
    // We're looking for pairs where:
    //   1. $this has predicate P binding to ?var
    //   2. Another ?variable has the same predicate P binding to same ?var

    let mut this_bindings: Vec<(String, String)> = Vec::new(); // (predicate_local, ?var)
    let mut other_bindings: Vec<(String, String)> = Vec::new(); // (predicate_local, ?var)

    for line in sparql.lines() {
        let trimmed = line.trim().trim_end_matches(';').trim_end_matches('.');
        let parts: Vec<&str> = trimmed.split_whitespace().collect();

        // Match patterns like: $this prefix:attr ?var
        // or continuation patterns: prefix:attr ?var (after semicolon)
        if parts.len() >= 3 {
            let subject = parts[0];
            let predicate = parts[1];
            let object = parts[2];

            if object.starts_with('?') && !predicate.starts_with("FILTER") && !predicate.starts_with("BIND") {
                let pred_local = iri_local_name(predicate);
                if subject == "$this" {
                    this_bindings.push((pred_local.to_owned(), object.to_owned()));
                } else if subject.starts_with('?') {
                    other_bindings.push((pred_local.to_owned(), object.to_owned()));
                }
            }
        }
    }

    // Find predicates that appear in both $this and ?other with the same join variable
    let mut shared = Vec::new();
    for (pred, var) in &this_bindings {
        for (other_pred, other_var) in &other_bindings {
            if pred == other_pred && var == other_var {
                shared.push(pred.clone());
            }
        }
    }

    shared.sort();
    shared.dedup();
    shared
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
                child: Box::new(crate::shacl_ast::ShaclAst::And {
                    children: vec![],
                }),
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
}
