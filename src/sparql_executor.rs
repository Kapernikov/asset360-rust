//! SPARQL executor: load LinkML instances into Oxigraph, execute queries, return results.
//!
//! This module wraps Oxigraph's in-memory store to:
//! 1. Pre-load schema triples (class hierarchy, property definitions)
//! 2. Convert LinkMLInstance objects to RDF via `as_turtle()` and load into store
//! 3. Execute SPARQL queries and serialize results

#[cfg(feature = "sparql-endpoint")]
use oxigraph::io::RdfFormat;
#[cfg(feature = "sparql-endpoint")]
use oxigraph::sparql::QueryResults;
#[cfg(feature = "sparql-endpoint")]
use oxigraph::store::Store;

use linkml_runtime::LinkMLInstance;
use linkml_runtime::turtle::{TurtleOptions, turtle_to_string};
use linkml_schemaview::schemaview::SchemaView;

/// Errors from SPARQL execution.
#[derive(Debug)]
pub enum ExecuteError {
    ConversionError { object_uri: String, message: String },
    TripleLimitExceeded { count: usize, limit: usize },
    ResultLimitExceeded { count: usize, limit: usize },
    QueryError(String),
    StoreError(String),
}

impl std::fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecuteError::ConversionError { object_uri, message } => {
                write!(f, "Failed to convert object {object_uri} to RDF: {message}")
            }
            ExecuteError::TripleLimitExceeded { count, limit } => {
                write!(f, "Triple count {count} exceeds limit {limit}")
            }
            ExecuteError::ResultLimitExceeded { count, limit } => {
                write!(f, "Result row count {count} exceeds limit {limit}")
            }
            ExecuteError::QueryError(msg) => write!(f, "Query execution error: {msg}"),
            ExecuteError::StoreError(msg) => write!(f, "Store error: {msg}"),
        }
    }
}

/// Configuration limits for query execution.
pub struct ExecuteLimits {
    pub max_triples: usize,
    pub max_result_rows: usize,
}

impl Default for ExecuteLimits {
    fn default() -> Self {
        Self {
            max_triples: 500_000,
            max_result_rows: 10_000,
        }
    }
}

/// Generate RDF triples (as Turtle) from the LinkML schema.
///
/// Produces rdfs:Class, rdfs:subClassOf, rdf:Property, rdfs:domain, rdfs:range
/// triples for all classes and slots in the schema.
pub fn schema_to_triples(schema_view: &SchemaView) -> String {
    let mut turtle = String::new();
    turtle.push_str("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n");
    turtle.push_str("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n");
    turtle.push_str("@prefix asset360: <https://data.infrabel.be/asset360/> .\n\n");

    let converter = schema_view.converter();

    let class_views = match schema_view.class_views() {
        Ok(cvs) => cvs,
        Err(_) => return turtle,
    };

    for cv in &class_views {
        let class_uri = match cv.canonical_uri().to_uri(&converter) {
            Ok(uri) => uri.to_string(),
            Err(_) => continue,
        };

        turtle.push_str(&format!("<{class_uri}> a rdfs:Class .\n"));

        // rdfs:subClassOf for parent class
        if let Ok(Some(parent)) = cv.parent_class() {
            if let Ok(parent_uri) = parent.canonical_uri().to_uri(&converter) {
                turtle.push_str(&format!(
                    "<{class_uri}> rdfs:subClassOf <{parent_uri}> .\n"
                ));
            }
        }

        // Properties for each slot
        for sv in cv.slots() {
            let slot_uri = match sv.canonical_uri().to_uri(&converter) {
                Ok(uri) => uri.to_string(),
                Err(_) => continue,
            };

            turtle.push_str(&format!("<{slot_uri}> a rdf:Property .\n"));
            turtle.push_str(&format!("<{slot_uri}> rdfs:domain <{class_uri}> .\n"));

            // rdfs:range — use the range class URI if it's a class reference
            if let Some(range_cv) = sv.get_range_class() {
                if let Ok(range_uri) = range_cv.canonical_uri().to_uri(&converter) {
                    turtle.push_str(&format!("<{slot_uri}> rdfs:range <{range_uri}> .\n"));
                }
            }
        }
    }

    turtle
}

/// Execute a SPARQL query against a set of LinkML instances.
///
/// 1. Creates an in-memory Oxigraph store
/// 2. Loads schema triples
/// 3. Converts each instance to Turtle and loads into store
/// 4. Executes the query
/// 5. Serializes results to the requested format
#[cfg(feature = "sparql-endpoint")]
pub fn sparql_execute(
    query_str: &str,
    instances: &[&LinkMLInstance],
    schema_view: &SchemaView,
    format: &str,
    limits: ExecuteLimits,
) -> Result<String, ExecuteError> {
    let store = Store::new().map_err(|e| ExecuteError::StoreError(e.to_string()))?;

    // Load schema triples
    let schema_turtle = schema_to_triples(schema_view);
    store
        .load_from_reader(RdfFormat::Turtle, schema_turtle.as_bytes())
        .map_err(|e| ExecuteError::StoreError(format!("Failed to load schema triples: {e}")))?;

    // Load instance data
    let converter = schema_view.converter();
    let primary_schema = schema_view
        .primary_schema()
        .ok_or_else(|| ExecuteError::StoreError("No primary schema found".to_owned()))?;

    for instance in instances {
        let object_uri = instance
            .node_id()
            .to_string();

        let turtle_str =
            turtle_to_string(instance, schema_view, &primary_schema, &converter, TurtleOptions { skolem: false })
                .map_err(|e| ExecuteError::ConversionError {
                    object_uri: object_uri.clone(),
                    message: e.to_string(),
                })?;

        store
            .load_from_reader(RdfFormat::Turtle, turtle_str.as_bytes())
            .map_err(|e| ExecuteError::ConversionError {
                object_uri: object_uri.clone(),
                message: format!("Failed to load turtle into store: {e}"),
            })?;
    }

    // Check triple limit
    let triple_count = store.len().map_err(|e| ExecuteError::StoreError(e.to_string()))?;
    if triple_count > limits.max_triples {
        return Err(ExecuteError::TripleLimitExceeded {
            count: triple_count,
            limit: limits.max_triples,
        });
    }

    // Execute query
    let results = store
        .query(query_str)
        .map_err(|e| ExecuteError::QueryError(e.to_string()))?;

    // Serialize results
    match results {
        QueryResults::Solutions(solutions) => {
            let vars: Vec<String> = solutions
                .variables()
                .iter()
                .map(|v| v.as_str().to_owned())
                .collect();

            let mut bindings: Vec<serde_json::Value> = Vec::new();
            for solution in solutions {
                let solution =
                    solution.map_err(|e| ExecuteError::QueryError(e.to_string()))?;

                if bindings.len() >= limits.max_result_rows {
                    return Err(ExecuteError::ResultLimitExceeded {
                        count: bindings.len() + 1,
                        limit: limits.max_result_rows,
                    });
                }

                let mut binding = serde_json::Map::new();
                for var in &vars {
                    if let Some(term) = solution.get(var.as_str()) {
                        binding.insert(var.clone(), term_to_json(term));
                    }
                }
                bindings.push(serde_json::Value::Object(binding));
            }

            let result = serde_json::json!({
                "head": { "vars": vars },
                "results": { "bindings": bindings }
            });
            serde_json::to_string(&result)
                .map_err(|e| ExecuteError::QueryError(e.to_string()))
        }
        QueryResults::Boolean(b) => {
            let result = serde_json::json!({ "boolean": b });
            serde_json::to_string(&result)
                .map_err(|e| ExecuteError::QueryError(e.to_string()))
        }
        QueryResults::Graph(triples) => {
            if format == "turtle" || format == "text/turtle" {
                let mut buf = Vec::new();
                for triple in triples {
                    let triple =
                        triple.map_err(|e| ExecuteError::QueryError(e.to_string()))?;
                    use std::io::Write;
                    writeln!(buf, "{} {} {} .", triple.subject, triple.predicate, triple.object)
                        .map_err(|e| ExecuteError::QueryError(e.to_string()))?;
                }
                String::from_utf8(buf)
                    .map_err(|e| ExecuteError::QueryError(e.to_string()))
            } else {
                Err(ExecuteError::QueryError(format!(
                    "Unsupported format for graph results: {format}"
                )))
            }
        }
    }
}

/// Convert an RDF term to SPARQL JSON Results format.
#[cfg(feature = "sparql-endpoint")]
fn term_to_json(term: &oxigraph::model::Term) -> serde_json::Value {
    use oxigraph::model::Term;
    match term {
        Term::NamedNode(nn) => serde_json::json!({
            "type": "uri",
            "value": nn.as_str()
        }),
        Term::BlankNode(bn) => serde_json::json!({
            "type": "bnode",
            "value": bn.as_str()
        }),
        Term::Literal(lit) => {
            let mut obj = serde_json::Map::new();
            obj.insert("type".into(), serde_json::json!("literal"));
            obj.insert("value".into(), serde_json::json!(lit.value()));
            if let Some(lang) = lit.language() {
                obj.insert("xml:lang".into(), serde_json::json!(lang));
            } else {
                let dt = lit.datatype().as_str();
                if dt != "http://www.w3.org/2001/XMLSchema#string" {
                    obj.insert("datatype".into(), serde_json::json!(dt));
                }
            }
            serde_json::Value::Object(obj)
        }
        Term::Triple(_) => serde_json::json!({
            "type": "triple",
            "value": term.to_string()
        }),
    }
}

#[cfg(all(test, feature = "sparql-endpoint"))]
mod tests {
    use super::*;
    use linkml_runtime::load_json_str;
    use linkml_schemaview::identifier::Identifier;
    use serde_json::json;

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

    fn load_signal(sv: &SchemaView, json_str: &str) -> LinkMLInstance {
        let conv = sv.converter();
        let id = Identifier::new("Signal");
        let cv = sv.get_class(&id, &conv).unwrap().unwrap();
        let result = load_json_str(json_str, sv, &cv, &conv).unwrap();
        result.into_instance_tolerate_errors().unwrap()
    }

    fn signal_instances(sv: &SchemaView) -> Vec<LinkMLInstance> {
        vec![
            load_signal(sv, r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/BX517", "name": "BX517"}"#),
            load_signal(sv, r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/BX518", "name": "BX518"}"#),
        ]
    }

    #[test]
    fn test_select_query() {
        let sv = test_schema_view();
        let instances = signal_instances(&sv);
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();
        let result = sparql_execute(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s ?name WHERE { ?s a asset360:Signal ; asset360:name ?name } ORDER BY ?name",
            &refs,
            &sv,
            "json",
            ExecuteLimits::default(),
        )
        .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let bindings = parsed["results"]["bindings"].as_array().unwrap();
        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings[0]["name"]["value"], "BX517");
        assert_eq!(bindings[1]["name"]["value"], "BX518");
    }

    #[test]
    fn test_ask_query() {
        let sv = test_schema_view();
        let instances = signal_instances(&sv);
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();
        let result = sparql_execute(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             ASK { ?s a asset360:Signal ; asset360:name \"BX517\" }",
            &refs,
            &sv,
            "json",
            ExecuteLimits::default(),
        )
        .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["boolean"], true);
    }

    #[test]
    fn test_ask_query_false() {
        let sv = test_schema_view();
        let instances = signal_instances(&sv);
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();
        let result = sparql_execute(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             ASK { ?s a asset360:Signal ; asset360:name \"NONEXISTENT\" }",
            &refs,
            &sv,
            "json",
            ExecuteLimits::default(),
        )
        .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["boolean"], false);
    }

    #[test]
    fn test_schema_introspection() {
        let sv = test_schema_view();
        let result = sparql_execute(
            "SELECT ?c WHERE { ?c a <http://www.w3.org/2000/01/rdf-schema#Class> }",
            &[],
            &sv,
            "json",
            ExecuteLimits::default(),
        )
        .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let bindings = parsed["results"]["bindings"].as_array().unwrap();
        assert!(bindings.len() >= 2, "Expected at least 2 classes, got {}", bindings.len());

        let class_uris: Vec<&str> = bindings
            .iter()
            .map(|b| b["c"]["value"].as_str().unwrap())
            .collect();
        assert!(class_uris.contains(&"https://data.infrabel.be/asset360/Signal"));
        assert!(class_uris.contains(&"https://data.infrabel.be/asset360/BaliseGroup"));
    }

    #[test]
    fn test_result_limit_exceeded() {
        let sv = test_schema_view();
        let instances = signal_instances(&sv);
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();
        let result = sparql_execute(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s ?name WHERE { ?s a asset360:Signal ; asset360:name ?name }",
            &refs,
            &sv,
            "json",
            ExecuteLimits {
                max_triples: 500_000,
                max_result_rows: 1,
            },
        );

        assert!(matches!(result, Err(ExecuteError::ResultLimitExceeded { .. })));
    }

    #[test]
    fn test_triple_limit_exceeded() {
        let sv = test_schema_view();
        let instances = signal_instances(&sv);
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();
        let result = sparql_execute(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s a asset360:Signal }",
            &refs,
            &sv,
            "json",
            ExecuteLimits {
                max_triples: 1,
                max_result_rows: 10_000,
            },
        );

        assert!(matches!(result, Err(ExecuteError::TripleLimitExceeded { .. })));
    }

    #[test]
    fn test_schema_to_triples_produces_valid_turtle() {
        let sv = test_schema_view();
        let turtle = schema_to_triples(&sv);

        assert!(turtle.contains("rdfs:Class"), "Should declare classes");
        assert!(turtle.contains("rdf:Property"), "Should declare properties");
        assert!(turtle.contains("@prefix"), "Should have prefix declarations");
    }

    #[test]
    fn test_construct_query() {
        let sv = test_schema_view();
        let instances = signal_instances(&sv);
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();
        let result = sparql_execute(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             CONSTRUCT { ?s a asset360:Signal ; asset360:name ?n } \
             WHERE { ?s a asset360:Signal ; asset360:name ?n }",
            &refs,
            &sv,
            "turtle",
            ExecuteLimits::default(),
        )
        .unwrap();

        assert!(result.contains("BX517"), "Should contain signal name");
        assert!(result.contains("Signal"), "Should contain type");
    }
}
