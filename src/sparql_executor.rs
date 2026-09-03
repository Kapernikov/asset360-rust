//! SPARQL query execution against in-memory Oxigraph.
//!
//! This is the second half of the virtual SPARQL endpoint pipeline. After the
//! scoper ([`crate::sparql_scoper`]) has determined which objects to fetch and
//! the Django view has loaded them as [`LinkMLInstance`] objects, this module:
//!
//! 1. Creates a fresh in-memory Oxigraph store.
//! 2. Converts each [`LinkMLInstance`] to Turtle via `as_turtle()` and loads
//!    the resulting RDF triples into the store.
//! 3. Executes the SPARQL query against the store.
//! 4. Serialises the results to SPARQL JSON Results (for SELECT/ASK) or
//!    N-Triples (for CONSTRUCT/DESCRIBE).
//!
//! No data persists between queries — the store is created and destroyed per
//! request. Caching is planned as a future optimisation.

#[cfg(feature = "sparql-endpoint")]
use oxigraph::io::RdfFormat;
#[cfg(feature = "sparql-endpoint")]
use oxigraph::sparql::QueryResults;
#[cfg(feature = "sparql-endpoint")]
use oxigraph::store::Store;

use linkml_runtime::LinkMLInstance;
use linkml_runtime::turtle::{TurtleOptions, turtle_to_string};
use linkml_schemaview::schemaview::SchemaView;

/// Errors that can occur during SPARQL query execution.
#[derive(Debug)]
pub enum ExecuteError {
    /// A [`LinkMLInstance`] could not be converted to RDF triples.
    ///
    /// This is a data quality issue — the object's JSON data is malformed or
    /// incompatible with the LinkML schema. The `object_uri` identifies which
    /// object failed so the user can investigate.
    ///
    /// The endpoint returns this as HTTP 500 with the object URI in the
    /// response body. The spec requires failing the entire query rather than
    /// silently skipping the bad object.
    ConversionError { object_uri: String, message: String },

    /// The total number of RDF triples in the store exceeds the configured
    /// limit. This prevents memory exhaustion from queries that scope to a
    /// large number of wide objects (many properties per object).
    TripleLimitExceeded { count: usize, limit: usize },

    /// The query produced more result rows than the configured limit.
    /// The endpoint returns HTTP 422 with a suggestion to narrow the query.
    ResultLimitExceeded { count: usize, limit: usize },

    /// Oxigraph returned an error while executing the SPARQL query.
    QueryError(String),

    /// Internal error creating or loading data into the Oxigraph store.
    StoreError(String),
}

impl std::fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecuteError::ConversionError {
                object_uri,
                message,
            } => {
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

/// Resource limits for query execution.
///
/// These prevent denial-of-service from expensive queries. When a limit is
/// exceeded, the executor returns a descriptive error (not a generic timeout)
/// so the user knows which limit was hit and how to narrow their query.
pub struct ExecuteLimits {
    /// Maximum number of RDF triples allowed in the in-memory store.
    ///
    /// Checked after loading all instance data. Each object produces roughly
    /// `1 + number_of_slots` triples (one `rdf:type` + one per property).
    /// Default: 500,000.
    pub max_triples: usize,

    /// Maximum number of result rows returned by a SELECT query.
    ///
    /// Checked during result iteration — if the query produces more rows
    /// than this limit, execution stops and an error is returned.
    /// Default: 10,000.
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

/// Execute a SPARQL query against a set of LinkML instances.
///
/// This is the main entry point for query execution. The caller (Django view)
/// has already used [`crate::sparql_scoper::sparql_scope`] to determine which
/// objects to fetch and has loaded them as [`LinkMLInstance`] objects.
///
/// # Arguments
///
/// * `query_str` — The SPARQL query string (SELECT, ASK, CONSTRUCT, or DESCRIBE).
/// * `instances` — The LinkML instances to query against. Each instance is
///   converted to RDF triples via `as_turtle()` and loaded into an ephemeral
///   in-memory Oxigraph store.
/// * `schema_view` — Used for Turtle serialisation of instances.
/// * `format` — Output serialisation format:
///   - `"json"` → SPARQL JSON Results (`application/sparql-results+json`)
///     for SELECT and ASK queries.
///   - `"turtle"` or `"text/turtle"` → N-Triples output for CONSTRUCT and
///     DESCRIBE queries.
/// * `limits` — Resource limits (max triples, max result rows) to prevent
///   denial-of-service from expensive queries.
///
/// # Errors
///
/// * [`ExecuteError::ConversionError`] — an instance's `as_turtle()` failed
///   (data quality issue). The entire query fails; no partial results.
/// * [`ExecuteError::TripleLimitExceeded`] — too many triples in the store.
/// * [`ExecuteError::ResultLimitExceeded`] — too many result rows.
/// * [`ExecuteError::QueryError`] — Oxigraph query execution error.
/// * [`ExecuteError::StoreError`] — internal store creation/loading error.
#[cfg(feature = "sparql-endpoint")]
pub fn sparql_execute(
    query_str: &str,
    instances: &[&LinkMLInstance],
    schema_view: &SchemaView,
    format: &str,
    limits: ExecuteLimits,
) -> Result<String, ExecuteError> {
    let store = Store::new().map_err(|e| ExecuteError::StoreError(e.to_string()))?;

    // Load instance data
    let converter = schema_view.converter();
    let primary_schema = schema_view
        .primary_schema()
        .ok_or_else(|| ExecuteError::StoreError("No primary schema found".to_owned()))?;

    for instance in instances {
        let object_uri = instance.node_id().to_string();

        let turtle_str = turtle_to_string(
            instance,
            schema_view,
            &primary_schema,
            &converter,
            TurtleOptions { skolem: false },
        )
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
    let triple_count = store
        .len()
        .map_err(|e| ExecuteError::StoreError(e.to_string()))?;
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
                let solution = solution.map_err(|e| ExecuteError::QueryError(e.to_string()))?;

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
            serde_json::to_string(&result).map_err(|e| ExecuteError::QueryError(e.to_string()))
        }
        QueryResults::Boolean(b) => {
            let result = serde_json::json!({ "boolean": b });
            serde_json::to_string(&result).map_err(|e| ExecuteError::QueryError(e.to_string()))
        }
        QueryResults::Graph(triples) => {
            if format == "turtle" || format == "text/turtle" {
                let mut buf = Vec::new();
                for triple in triples {
                    let triple = triple.map_err(|e| ExecuteError::QueryError(e.to_string()))?;
                    use std::io::Write;
                    writeln!(
                        buf,
                        "{} {} {} .",
                        triple.subject, triple.predicate, triple.object
                    )
                    .map_err(|e| ExecuteError::QueryError(e.to_string()))?;
                }
                String::from_utf8(buf).map_err(|e| ExecuteError::QueryError(e.to_string()))
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
            load_signal(
                sv,
                r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/BX517", "name": "BX517"}"#,
            ),
            load_signal(
                sv,
                r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/BX518", "name": "BX518"}"#,
            ),
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

        assert!(matches!(
            result,
            Err(ExecuteError::ResultLimitExceeded { .. })
        ));
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

        assert!(matches!(
            result,
            Err(ExecuteError::TripleLimitExceeded { .. })
        ));
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

/// A differential oracle over the *documented* rendering of a pushed filter.
///
/// Six bugs in this area shared one shape: a rule applied at three call sites
/// out of four, or to one operator and not its twin, so the plan claimed to
/// describe a query whose answer it changed. Each was found by hand and fixed by
/// hand. This sweeps the grid instead — every column kind against every way of
/// writing a constant — and asks the only question that matters: when the plan
/// says it is exact, does rendering its filter the way the docs say produce the
/// answer oxigraph produces?
///
/// It deliberately does not care *which* refusal an inexact plan gives. A
/// refusal is always safe; claiming exactness and being wrong is not.
#[cfg(all(test, feature = "sparql-endpoint"))]
mod pushed_filters_match_sparql {
    use crate::sparql_scoper::{FilterCondition, sparql_scope};
    use linkml_runtime::{LinkMLInstance, load_json_str};
    use linkml_schemaview::identifier::Identifier;
    use linkml_schemaview::schemaview::SchemaView;

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> \
                          PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

    /// One column per kind the term rule distinguishes, plus the text each
    /// stores — which is what `object_data->>'slot'` would yield.
    const STORED: [(&str, &[&str]); 5] = [
        ("name", &["BX1"]),
        ("length", &["3"]),
        ("description", &["hello"]),
        ("seeAlso", &["https://data.infrabel.be/asset360/track/T1"]),
        ("trafficKinds", &["m", "p"]),
    ];

    fn schema() -> SchemaView {
        use linkml_meta::SchemaDefinition;
        use serde_path_to_error as p2e;
        use serde_yml as yml;
        let yaml = r#"
id: https://data.infrabel.be/asset360
name: asset360
prefixes:
  asset360:
    prefix_reference: https://data.infrabel.be/asset360/
  linkml:
    prefix_reference: https://w3id.org/linkml/
  xsd:
    prefix_reference: http://www.w3.org/2001/XMLSchema#
default_prefix: asset360
default_range: string
types:
  string:
    uri: xsd:string
    base: str
  integer:
    uri: xsd:integer
    base: int
  uriorcurie:
    uri: xsd:anyURI
    base: URIorCURIE
classes:
  Signal:
    class_uri: asset360:Signal
    attributes:
      asset360_uri:
        identifier: true
      name:
        range: string
      length:
        range: integer
      description:
        range: string
        in_language: en
      seeAlso:
        range: uriorcurie
      trafficKinds:
        range: string
        multivalued: true
"#;
        let schema: SchemaDefinition = p2e::deserialize(yml::Deserializer::from_str(yaml)).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        sv
    }

    fn instance(sv: &SchemaView) -> LinkMLInstance {
        let conv = sv.converter();
        let cv = sv
            .get_class(&Identifier::new("Signal"), &conv)
            .unwrap()
            .unwrap();
        load_json_str(
            r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/A",
                "name": "BX1", "length": 3, "description": "hello",
                "seeAlso": "https://data.infrabel.be/asset360/track/T1",
                "trafficKinds": ["m", "p"]}"#,
            sv,
            &cv,
            &conv,
        )
        .unwrap()
        .into_instance_tolerate_errors()
        .unwrap()
    }

    /// Does SPARQL itself match, over the real serialisation?
    fn sparql_matches(sv: &SchemaView, inst: &LinkMLInstance, triple: &str) -> bool {
        let refs = vec![inst];
        let json = super::sparql_execute(
            &format!("{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal ; {triple} }}"),
            &refs,
            sv,
            "json",
            super::ExecuteLimits::default(),
        )
        .expect("query executes");
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        !parsed["results"]["bindings"].as_array().unwrap().is_empty()
    }

    /// Does the plan's own documented rendering match?
    ///
    /// `object_data->>'field' = 'value'` for a single-valued column, and the
    /// containment test `multivalued_fields` prescribes for an array one.
    fn rendering_matches(
        conditions: &[FilterCondition],
        stored: &[&str],
        multivalued: bool,
    ) -> bool {
        conditions.iter().all(|condition| match condition {
            FilterCondition::Eq(v) => {
                if multivalued {
                    stored.contains(&v.as_str())
                } else {
                    stored.first() == Some(&v.as_str())
                }
            }
            FilterCondition::In(vs) => stored.iter().any(|s| vs.iter().any(|v| v == s)),
            // Text ordering, which is what the documented SQL does.
            FilterCondition::Cmp { op, value } => {
                stored.first().is_some_and(|s| match op.as_str() {
                    ">" => *s > value.as_str(),
                    ">=" => *s >= value.as_str(),
                    "<" => *s < value.as_str(),
                    _ => *s <= value.as_str(),
                })
            }
        })
    }

    #[test]
    fn every_pushed_constant_answers_what_sparql_answers() {
        let sv = schema();
        let inst = instance(&sv);

        // Every column kind against every way of writing a constant: matching
        // and non-matching, canonical and not, tagged and bare, IRI and literal.
        let constants = [
            "\"BX1\"",
            "\"hello\"",
            "\"hello\"@en",
            "\"hello\"@fr",
            "\"m\"",
            "\"3\"",
            "3",
            "\"003\"^^xsd:integer",
            "\"+3\"^^xsd:integer",
            "\"3\"^^xsd:string",
            "\"BX1\"@en",
            "<https://data.infrabel.be/asset360/track/T1>",
            "\"https://data.infrabel.be/asset360/track/T1\"",
        ];

        let mut checked = 0;
        for (slot, stored) in STORED {
            for constant in constants {
                let triple = format!("asset360:{slot} {constant}");
                let query =
                    format!("{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal ; {triple} }}");
                let plan = sparql_scope(&query, &sv).expect("scopes");

                // An inexact plan has already said it does not describe the
                // query; nothing is claimed and nothing is owed.
                if plan.inexact.is_some() {
                    continue;
                }

                let star = &plan.root.all_stars()[0];
                let multivalued = star.multivalued_fields.iter().any(|f| f == slot);
                let rendered = match star.filters.get(slot) {
                    Some(conditions) => rendering_matches(conditions, stored, multivalued),
                    // Nothing pushed: the fetch is wider and oxigraph decides.
                    None => continue,
                };

                checked += 1;
                assert_eq!(
                    rendered,
                    sparql_matches(&sv, &inst, &triple),
                    "exact plan disagrees with SPARQL for `{triple}`: rendering \
                     said {rendered}"
                );
            }
        }

        // Guard the guard: a grid that silently stopped exercising anything
        // would pass forever.
        assert!(
            checked >= 8,
            "only {checked} constants were actually pushed"
        );
    }

    /// The other half, and the half a "refusals are always safe" oracle cannot
    /// see: a constant written *as the column stores it* must actually push.
    ///
    /// Refusing everything is safe and useless, and this is how the language
    /// check in `literal_pushable` came to be dead — every constant on a
    /// language-tagged column was refused, including the only one that was
    /// right, and no wrong-answer test could notice.
    #[test]
    fn a_constant_in_the_column_s_own_form_is_pushed() {
        let sv = schema();
        let inst = instance(&sv);

        for (slot, constant) in [
            ("name", "\"BX1\""),
            ("length", "3"),
            ("description", "\"hello\"@en"),
            ("seeAlso", "<https://data.infrabel.be/asset360/track/T1>"),
            ("trafficKinds", "\"m\""),
        ] {
            let triple = format!("asset360:{slot} {constant}");
            assert!(
                sparql_matches(&sv, &inst, &triple),
                "the fixture must actually match, or the case proves nothing: {triple}"
            );

            let plan = sparql_scope(
                &format!("{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal ; {triple} }}"),
                &sv,
            )
            .expect("scopes");
            assert_eq!(
                plan.inexact, None,
                "the column's own form must be pushable: {triple}"
            );
            assert!(
                plan.root.all_stars()[0].filters.contains_key(slot),
                "exact, but nothing pushed for {triple}"
            );
        }
    }
}
