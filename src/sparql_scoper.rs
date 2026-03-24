//! SPARQL query scoping: analyse a query to determine which asset types and
//! objects need to be fetched from the database.
//!
//! The scoper parses SPARQL via `spargebra`, walks the algebra tree, and extracts:
//! - `rdf:type` patterns → which asset types to fetch
//! - URI constants → which specific objects to fetch
//! - FILTER conditions → pushable to SQL
//! - Whether the query is bounded (can be safely executed)

use std::collections::{HashMap, HashSet};

use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::{Query, SparqlParser};

use linkml_schemaview::schemaview::SchemaView;

/// Result of scoping a SPARQL query.
#[derive(Debug, Clone)]
pub struct ScopeResult {
    /// Asset type names to fetch from the database (e.g. ["Signal", "BaliseGroup"]).
    pub asset_types: Vec<String>,
    /// Specific URIs referenced in the query (for direct lookup).
    pub uri_filters: Vec<String>,
    /// Field-level filter conditions pushable to SQL.
    pub predicate_filters: HashMap<String, Vec<FilterCondition>>,
    /// Whether the query scope is bounded (safe to execute).
    pub is_bounded: bool,
    /// Estimated number of objects, if determinable.
    pub estimated_count: Option<usize>,
    /// Whether this is a schema-only query (no instance data needed).
    pub schema_only: bool,
    /// SQL LIMIT to push down (for single-type queries with top-level LIMIT).
    pub sql_limit: Option<usize>,
}

/// A filter condition that can be pushed down to SQL.
#[derive(Debug, Clone)]
pub enum FilterCondition {
    Eq(String),
    In(Vec<String>),
}

/// Errors from query scoping.
#[derive(Debug)]
pub enum ScopeError {
    ParseError(String),
    Unscoped(String),
    UpdateRejected,
}

impl std::fmt::Display for ScopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScopeError::ParseError(msg) => write!(f, "SPARQL parse error: {msg}"),
            ScopeError::Unscoped(msg) => write!(f, "Query is unscoped: {msg}"),
            ScopeError::UpdateRejected => {
                write!(f, "SPARQL Update (INSERT/DELETE) is not supported. This endpoint is read-only.")
            }
        }
    }
}

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_CLASS: &str = "http://www.w3.org/2000/01/rdf-schema#Class";
const RDF_PROPERTY: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
/// Analyse a SPARQL query and extract scoping information.
///
/// Returns a `ScopeResult` describing which data to fetch, or a `ScopeError`
/// if the query cannot be scoped (unscoped, parse error, or UPDATE).
pub fn sparql_scope(query_str: &str, schema_view: &SchemaView) -> Result<ScopeResult, ScopeError> {
    // Reject SPARQL Update (INSERT/DELETE/LOAD)
    if SparqlParser::new().parse_update(query_str).is_ok() {
        return Err(ScopeError::UpdateRejected);
    }

    let parser = SparqlParser::new()
        .with_prefix("asset360", "https://data.infrabel.be/asset360/")
        .expect("hardcoded prefix is valid")
        .with_prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        .expect("hardcoded prefix is valid")
        .with_prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
        .expect("hardcoded prefix is valid")
        .with_prefix("xsd", "http://www.w3.org/2001/XMLSchema#")
        .expect("hardcoded prefix is valid");

    let query = parser
        .parse_query(query_str)
        .map_err(|e| ScopeError::ParseError(e.to_string()))?;

    let pattern = match &query {
        Query::Select { pattern, .. } => pattern,
        Query::Construct { pattern, .. } => pattern,
        Query::Describe { pattern, .. } => pattern,
        Query::Ask { pattern, .. } => pattern,
    };

    // Collect all BGP triples from the algebra tree
    let mut triples = Vec::new();
    collect_bgp_triples(pattern, &mut triples);

    // Extract rdf:type patterns
    let mut type_iris: HashSet<String> = HashSet::new();
    let mut type_variables: HashSet<String> = HashSet::new();
    let mut schema_type_iris: HashSet<String> = HashSet::new();
    let mut uri_filters: HashSet<String> = HashSet::new();

    for tp in &triples {
        let pred_iri = match &tp.predicate {
            NamedNodePattern::NamedNode(nn) => nn.as_str(),
            _ => continue,
        };

        if pred_iri == RDF_TYPE {
            match &tp.object {
                TermPattern::NamedNode(nn) => {
                    let obj_iri = nn.as_str();
                    // Check if this is a schema-level type (rdfs:Class, rdf:Property)
                    if obj_iri == RDFS_CLASS || obj_iri == RDF_PROPERTY {
                        schema_type_iris.insert(obj_iri.to_owned());
                    } else {
                        type_iris.insert(obj_iri.to_owned());
                    }
                }
                TermPattern::Variable(v) => {
                    type_variables.insert(v.as_str().to_owned());
                }
                _ => {}
            }
        }

        // Collect URI constants used as subjects (for URI-based filtering)
        if let TermPattern::NamedNode(nn) = &tp.subject {
            uri_filters.insert(nn.as_str().to_owned());
        }
    }

    // Also collect URIs from VALUES clauses
    collect_values_uris(pattern, &mut uri_filters);

    // Check if type variables are constrained by VALUES
    let constrained_type_vars = check_type_variable_constraints(pattern, &type_variables);
    for iri in constrained_type_vars {
        type_iris.insert(iri);
    }

    // Resolve type IRIs to asset type names via schema_view
    let mut asset_types: Vec<String> = Vec::new();
    for iri in &type_iris {
        if let Ok(Some(cv)) = schema_view.get_class_by_uri(iri) {
            asset_types.push(cv.name().to_owned());
        }
    }
    asset_types.sort();
    asset_types.dedup();

    // Determine if this is a schema-only query
    let schema_only = asset_types.is_empty()
        && !schema_type_iris.is_empty()
        && type_variables.is_empty();

    // Determine if bounded
    let is_bounded = !asset_types.is_empty()
        || !uri_filters.is_empty()
        || schema_only;

    if !is_bounded {
        let suggestion = if type_variables.is_empty() {
            "Add a triple pattern like '?s rdf:type asset360:Signal' to scope the query."
        } else {
            "The type variable is unconstrained. Add VALUES or FILTER to bind it to specific types."
        };
        return Err(ScopeError::Unscoped(suggestion.to_owned()));
    }

    // --- Phase 7: Filter pushdown ---

    // Build variable→field binding map from BGP triples:
    // ?s asset360:name ?name  →  var_to_field["name"] = "name"
    // Resolve predicate IRI to slot name via SchemaView.
    let mut var_to_field: HashMap<String, String> = HashMap::new();
    for tp in &triples {
        let pred_iri = match &tp.predicate {
            NamedNodePattern::NamedNode(nn) => nn.as_str(),
            _ => continue,
        };
        if pred_iri == RDF_TYPE {
            continue;
        }
        if let TermPattern::Variable(v) = &tp.object {
            // Resolve IRI to slot name via schema
            if let Ok(Some(slot_view)) = schema_view.get_slot_by_uri(pred_iri) {
                var_to_field.insert(v.as_str().to_owned(), slot_view.name.clone());
            }
        }
    }

    // Extract FILTER equality conditions on bound variables (T022)
    let mut predicate_filters: HashMap<String, Vec<FilterCondition>> = HashMap::new();
    collect_filter_conditions(pattern, &var_to_field, &mut predicate_filters);

    // Extract VALUES on subject variables for URI pushdown (T023)
    // Already handled by collect_values_uris above — subject URIs from VALUES
    // are in uri_filters. Also collect VALUES on bound variables.
    collect_values_filters(pattern, &var_to_field, &mut predicate_filters);

    // Extract SQL LIMIT for single-type queries (T024)
    let sql_limit = if asset_types.len() == 1 {
        extract_top_level_limit(pattern)
    } else {
        None
    };

    Ok(ScopeResult {
        asset_types,
        uri_filters: uri_filters.into_iter().collect(),
        predicate_filters,
        is_bounded,
        estimated_count: None,
        schema_only,
        sql_limit,
    })
}

/// Recursively collect all BGP triple patterns from a SPARQL algebra tree.
fn collect_bgp_triples<'a>(
    pattern: &'a GraphPattern,
    triples: &mut Vec<&'a TriplePattern>,
) {
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

/// Collect URI constants from VALUES clauses.
fn collect_values_uris(pattern: &GraphPattern, uris: &mut HashSet<String>) {
    match pattern {
        GraphPattern::Values { bindings, .. } => {
            for row in bindings {
                for val in row {
                    if let Some(spargebra::term::GroundTerm::NamedNode(nn)) = val {
                        uris.insert(nn.as_str().to_owned());
                    }
                }
            }
        }
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_values_uris(left, uris);
            collect_values_uris(right, uris);
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
            collect_values_uris(inner, uris);
        }
        _ => {}
    }
}

/// Check if type variables are constrained by VALUES clauses.
fn check_type_variable_constraints(
    pattern: &GraphPattern,
    type_vars: &HashSet<String>,
) -> Vec<String> {
    let mut constrained_iris = Vec::new();
    collect_type_var_values(pattern, type_vars, &mut constrained_iris);
    constrained_iris
}

fn collect_type_var_values(
    pattern: &GraphPattern,
    type_vars: &HashSet<String>,
    iris: &mut Vec<String>,
) {
    match pattern {
        GraphPattern::Values { variables, bindings } => {
            for (i, var) in variables.iter().enumerate() {
                if type_vars.contains(var.as_str()) {
                    for row in bindings {
                        if let Some(Some(spargebra::term::GroundTerm::NamedNode(nn))) = row.get(i) {
                            iris.push(nn.as_str().to_owned());
                        }
                    }
                }
            }
        }
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_type_var_values(left, type_vars, iris);
            collect_type_var_values(right, type_vars, iris);
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
            collect_type_var_values(inner, type_vars, iris);
        }
        _ => {}
    }
}

/// T022: Extract FILTER equality conditions on variables bound to known predicates.
///
/// Recognizes patterns like:
///   FILTER(?name = "BX517")
///   FILTER("BX517" = ?name)
/// where ?name is bound via `?s asset360:name ?name`.
fn collect_filter_conditions(
    pattern: &GraphPattern,
    var_to_field: &HashMap<String, String>,
    filters: &mut HashMap<String, Vec<FilterCondition>>,
) {
    match pattern {
        GraphPattern::Filter { expr, inner } => {
            extract_equality_from_expr(expr, var_to_field, filters);
            collect_filter_conditions(inner, var_to_field, filters);
        }
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_filter_conditions(left, var_to_field, filters);
            collect_filter_conditions(right, var_to_field, filters);
        }
        GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. } => {
            collect_filter_conditions(inner, var_to_field, filters);
        }
        _ => {}
    }
}

/// Extract equality conditions from a FILTER expression.
fn extract_equality_from_expr(
    expr: &Expression,
    var_to_field: &HashMap<String, String>,
    filters: &mut HashMap<String, Vec<FilterCondition>>,
) {
    match expr {
        Expression::Equal(left, right) => {
            // Check ?var = "literal" or "literal" = ?var
            if let Some((field, value)) = match_var_literal(left, right, var_to_field)
                .or_else(|| match_var_literal(right, left, var_to_field))
            {
                filters
                    .entry(field)
                    .or_default()
                    .push(FilterCondition::Eq(value));
            }
        }
        Expression::And(left, right) => {
            extract_equality_from_expr(left, var_to_field, filters);
            extract_equality_from_expr(right, var_to_field, filters);
        }
        _ => {}
    }
}

/// Match a (variable, literal) pair in a FILTER expression.
fn match_var_literal(
    var_expr: &Expression,
    lit_expr: &Expression,
    var_to_field: &HashMap<String, String>,
) -> Option<(String, String)> {
    let var_name = match var_expr {
        Expression::Variable(v) => v.as_str(),
        _ => return None,
    };
    let field = var_to_field.get(var_name)?;
    let value = match lit_expr {
        Expression::Literal(lit) => lit.value().to_owned(),
        _ => return None,
    };
    Some((field.clone(), value))
}

/// T023: Extract VALUES on bound variables and push as filter conditions.
///
/// Recognizes:
///   VALUES ?name { "BX517" "BX518" }
/// where ?name is bound via `?s asset360:name ?name`.
fn collect_values_filters(
    pattern: &GraphPattern,
    var_to_field: &HashMap<String, String>,
    filters: &mut HashMap<String, Vec<FilterCondition>>,
) {
    match pattern {
        GraphPattern::Values { variables, bindings } => {
            for (i, var) in variables.iter().enumerate() {
                if let Some(field) = var_to_field.get(var.as_str()) {
                    let mut values = Vec::new();
                    for row in bindings {
                        if let Some(Some(term)) = row.get(i) {
                            match term {
                                spargebra::term::GroundTerm::NamedNode(nn) => {
                                    values.push(nn.as_str().to_owned());
                                }
                                spargebra::term::GroundTerm::Literal(lit) => {
                                    values.push(lit.value().to_owned());
                                }
                            }
                        }
                    }
                    if !values.is_empty() {
                        filters
                            .entry(field.clone())
                            .or_default()
                            .push(FilterCondition::In(values));
                    }
                }
            }
        }
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_values_filters(left, var_to_field, filters);
            collect_values_filters(right, var_to_field, filters);
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
            collect_values_filters(inner, var_to_field, filters);
        }
        _ => {}
    }
}

/// T024: Extract top-level LIMIT from the query pattern.
///
/// Only extracts LIMIT from single-type queries where the LIMIT
/// is at the outermost level (Slice pattern wrapping the rest).
fn extract_top_level_limit(pattern: &GraphPattern) -> Option<usize> {
    match pattern {
        GraphPattern::Slice { length, .. } => *length,
        // Look through Project (SELECT wraps in Project → Slice)
        GraphPattern::Project { inner, .. } => extract_top_level_limit(inner),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema_view() -> SchemaView {
        use linkml_meta::SchemaDefinition;
        use serde_path_to_error as p2e;
        use serde_yml as yml;

        let schema_yaml = r#"
id: https://data.infrabel.be/asset360
name: asset360
prefixes:
  asset360:
    prefix_reference: https://data.infrabel.be/asset360/
  linkml:
    prefix_reference: https://w3id.org/linkml/
default_prefix: asset360
default_range: string

classes:
  Signal:
    class_uri: asset360:Signal
    attributes:
      asset360_uri:
        identifier: true
      name:
        range: string
  BaliseGroup:
    class_uri: asset360:BaliseGroup
    attributes:
      asset360_uri:
        identifier: true
      refersToSignal:
        range: Signal
"#;
        let schema: SchemaDefinition =
            p2e::deserialize(yml::Deserializer::from_str(schema_yaml)).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        sv
    }

    #[test]
    fn test_single_type_query() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "SELECT ?s ?name WHERE { ?s a asset360:Signal ; asset360:name ?name }",
            &sv,
        )
        .unwrap();
        assert_eq!(result.asset_types, vec!["Signal"]);
        assert!(result.is_bounded);
        assert!(!result.schema_only);
    }

    #[test]
    fn test_multi_type_join() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "SELECT ?sig ?bg WHERE { \
             ?sig a asset360:Signal . \
             ?bg a asset360:BaliseGroup ; asset360:refersToSignal ?sig . \
             }",
            &sv,
        )
        .unwrap();
        assert!(result.asset_types.contains(&"Signal".to_owned()));
        assert!(result.asset_types.contains(&"BaliseGroup".to_owned()));
        assert!(result.is_bounded);
    }

    #[test]
    fn test_unscoped_query_rejected() {
        let sv = test_schema_view();
        let result = sparql_scope("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", &sv);
        assert!(matches!(result, Err(ScopeError::Unscoped(_))));
    }

    #[test]
    fn test_variable_type_without_constraint_rejected() {
        let sv = test_schema_view();
        let result = sparql_scope("SELECT ?s ?t WHERE { ?s a ?t }", &sv);
        assert!(matches!(result, Err(ScopeError::Unscoped(_))));
    }

    #[test]
    fn test_sparql_update_rejected() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "INSERT DATA { <http://example.org/s> <http://example.org/p> \"value\" }",
            &sv,
        );
        assert!(matches!(result, Err(ScopeError::UpdateRejected)));
    }

    #[test]
    fn test_schema_introspection_query() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "SELECT ?c WHERE { ?c a rdfs:Class }",
            &sv,
        )
        .unwrap();
        assert!(result.asset_types.is_empty());
        assert!(result.is_bounded);
        assert!(result.schema_only);
    }

    #[test]
    fn test_construct_query_scoped() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "CONSTRUCT { ?s a asset360:Signal ; asset360:name ?n } \
             WHERE { ?s a asset360:Signal ; asset360:name ?n }",
            &sv,
        )
        .unwrap();
        assert_eq!(result.asset_types, vec!["Signal"]);
        assert!(result.is_bounded);
    }

    #[test]
    fn test_ask_query_scoped() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "ASK { ?s a asset360:Signal ; asset360:name \"BX517\" }",
            &sv,
        )
        .unwrap();
        assert_eq!(result.asset_types, vec!["Signal"]);
    }

    #[test]
    fn test_parse_error() {
        let sv = test_schema_view();
        let result = sparql_scope("NOT VALID {{{", &sv);
        assert!(matches!(result, Err(ScopeError::ParseError(_))));
    }

    // ---- Phase 7: Filter pushdown tests ----

    #[test]
    fn test_filter_equality_pushdown() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?name . FILTER(?name = \"BX517\") }",
            &sv,
        )
        .unwrap();
        assert_eq!(result.asset_types, vec!["Signal"]);
        let name_filters = result.predicate_filters.get("name").expect("should have name filter");
        assert_eq!(name_filters.len(), 1);
        match &name_filters[0] {
            FilterCondition::Eq(v) => assert_eq!(v, "BX517"),
            other => panic!("expected Eq, got {:?}", other),
        }
    }

    #[test]
    fn test_filter_reversed_equality() {
        // "BX517" = ?name (literal on left)
        let sv = test_schema_view();
        let result = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?name . FILTER(\"BX517\" = ?name) }",
            &sv,
        )
        .unwrap();
        let name_filters = result.predicate_filters.get("name").expect("should have name filter");
        assert_eq!(name_filters.len(), 1);
        match &name_filters[0] {
            FilterCondition::Eq(v) => assert_eq!(v, "BX517"),
            other => panic!("expected Eq, got {:?}", other),
        }
    }

    #[test]
    fn test_values_field_pushdown() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?name . VALUES ?name { \"BX517\" \"BX518\" } }",
            &sv,
        )
        .unwrap();
        let name_filters = result.predicate_filters.get("name").expect("should have name filter");
        assert_eq!(name_filters.len(), 1);
        match &name_filters[0] {
            FilterCondition::In(vals) => {
                assert!(vals.contains(&"BX517".to_owned()));
                assert!(vals.contains(&"BX518".to_owned()));
            }
            other => panic!("expected In, got {:?}", other),
        }
    }

    #[test]
    fn test_limit_pushdown_single_type() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s a asset360:Signal } LIMIT 10",
            &sv,
        )
        .unwrap();
        assert_eq!(result.sql_limit, Some(10));
    }

    #[test]
    fn test_no_limit_pushdown_multi_type() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s ?bg WHERE { ?s a asset360:Signal . ?bg a asset360:BaliseGroup } LIMIT 10",
            &sv,
        )
        .unwrap();
        // Multi-type query — LIMIT should not be pushed to SQL
        assert_eq!(result.sql_limit, None);
    }

    #[test]
    fn test_no_filter_for_unbound_var() {
        // FILTER on a variable that's not bound to a predicate should not produce a filter
        let sv = test_schema_view();
        let result = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s a asset360:Signal . FILTER(?s = <http://example.org/x>) }",
            &sv,
        )
        .unwrap();
        assert!(result.predicate_filters.is_empty());
    }
}
