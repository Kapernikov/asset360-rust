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
//!
//! The active datamodel is also loaded, into its own named graph, so a client
//! can discover classes, slots and enum values instead of being told them out
//! of band. See [`crate::sparql_schema_graph`] for what it holds and why it is
//! a *named* graph. It is built only for a query that carries a `GRAPH` clause
//! — nothing else can read it — so an instance query pays nothing for it. A
//! query that does read it pays the build every time: ~11,500 quads and
//! ~80-100 ms against the live asset360 schema (the count is pinned by
//! `test_the_graph_size_is_pinned` in the consolidator-server suite), which is
//! where a cache keyed on the schema view would go if discovery queries ever
//! become frequent.

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

    /// The engine ran past the wall-clock ceiling the caller set.
    ///
    /// The one limit that bounds *work* rather than input or output. A query
    /// whose evaluation is a cartesian product -- label `OPTIONAL`s written
    /// as siblings of the slot instead of nested under it -- stays under the
    /// triple cap (the store is small) and never reaches the row cap (the
    /// first row is what takes minutes), so before this it pinned a worker
    /// at 100 % CPU for as long as it took (#460, pepibru GitLab). The
    /// endpoint returns this as HTTP 422, like the other two caps.
    EvaluationTimeExceeded { millis: u64 },

    /// Too many evaluations this process gave up on are still running.
    ///
    /// The ceiling frees the *caller*, not the worker: an evaluation past its
    /// deadline is cancelled at its next store read, and one that never
    /// reads again -- a hash join or an `ORDER BY` over a product -- runs to
    /// its end in the background, holding its store. Each such evaluation is
    /// counted until it finishes, and a request that would start another
    /// while [`MAX_ABANDONED_EVALUATIONS`] are already running is refused
    /// at the top of [`sparql_execute`], before it loads a store of its own,
    /// so the backlog is bounded
    /// instead of compounding to an OOM after every caller has been told
    /// "gave up" (#460, pepibru GitLab). The endpoint returns this as HTTP
    /// 503: it is the process's state, not the query's shape.
    EvaluationBacklog { abandoned: usize, limit: usize },

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
                // `count` is what the store held when loading stopped, not
                // what the whole fetch would have made: loading is abandoned
                // as soon as the ceiling is crossed, which is the point (see
                // `sparql_execute`). Saying so keeps the number honest — it
                // used to be the full total, paid for with the whole load.
                write!(
                    f,
                    "Triple count {count} exceeds limit {limit}; loading was \
                     stopped there, so the query's full data may be larger"
                )
            }
            ExecuteError::ResultLimitExceeded { count, limit } => {
                write!(f, "Result row count {count} exceeds limit {limit}")
            }
            ExecuteError::EvaluationTimeExceeded { millis } => {
                write!(f, "Evaluation time exceeds limit of {millis} ms")
            }
            ExecuteError::EvaluationBacklog { abandoned, limit } => {
                write!(
                    f,
                    "Evaluation backlog full: {abandoned} evaluations are still running past \
                     their deadline in this process, and {limit} is the most it carries"
                )
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
    /// Checked while instance data loads, after each chunk, so a query over
    /// the ceiling is refused instead of loading everything first and then
    /// being told. Each object produces roughly `1 + number_of_slots` triples
    /// (one `rdf:type` + one per property). Default: 500,000.
    pub max_triples: usize,

    /// Maximum number of result rows returned by a SELECT query.
    ///
    /// Checked during result iteration — if the query produces more rows
    /// than this limit, execution stops and an error is returned.
    /// Default: 10,000.
    pub max_result_rows: usize,

    /// Wall-clock ceiling on the engine's evaluation, in milliseconds.
    ///
    /// Counted from the moment the query is handed to the evaluator -- the
    /// store is already loaded, and that phase is bounded by `max_triples`.
    /// The caller is answered at the deadline whatever the evaluation is
    /// doing; the evaluation itself is stopped through oxigraph's
    /// `CancellationToken` at its next store read (see [`sparql_execute`]
    /// for what that does and does not cover). `None` is no ceiling, which
    /// is what a caller that has its own gets. Default: none.
    pub max_eval_millis: Option<u64>,
}

/// How many evaluations past their deadline one process runs before the
/// ceiling refuses to start another.
///
/// Small on purpose. An abandoned evaluation holds a store of up to
/// `max_triples` quads plus whatever product it is computing, on a thread
/// nothing can stop until it next reads the store; two of those is a worker
/// under strain, a third is the request that must not start. Process-wide
/// rather than per call because the memory is the process's.
pub const MAX_ABANDONED_EVALUATIONS: usize = 2;

/// Evaluations the ceiling gave up on that have not finished yet.
///
/// Counted up by the caller that stops waiting and down by the worker when
/// it ends -- the two hand-offs are ordered by the per-evaluation state in
/// [`sparql_execute`], so the count never goes below zero.
#[cfg(feature = "sparql-endpoint")]
static ABANDONED_EVALUATIONS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Evaluations the ceiling gave up on that are still running, right now.
///
/// What the endpoint can log beside a refusal; what a test reads to see the
/// count come back down.
#[cfg(feature = "sparql-endpoint")]
pub fn abandoned_evaluations() -> usize {
    ABANDONED_EVALUATIONS.load(std::sync::atomic::Ordering::SeqCst)
}

impl Default for ExecuteLimits {
    fn default() -> Self {
        Self {
            max_triples: 500_000,
            max_result_rows: 10_000,
            max_eval_millis: None,
        }
    }
}

/// What a query answered, and how it is serialised.
///
/// The content type is the executor's to state because the *result type* is:
/// `SELECT`/`ASK` can only be SPARQL results, a `CONSTRUCT`/`DESCRIBE` graph
/// can only be RDF. A caller that had to work it out for itself would be
/// re-deriving what oxigraph already told this function.
///
/// The graph body is N-Triples, which is a subset of Turtle — hence the Turtle
/// content type, which is what an HTTP client is served and what parses.
#[cfg(feature = "sparql-endpoint")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparqlAnswer {
    /// The MIME type of `body`.
    pub content_type: String,
    /// The serialised answer.
    pub body: String,
}

#[cfg(feature = "sparql-endpoint")]
impl SparqlAnswer {
    /// SELECT or ASK: SPARQL Query Results JSON.
    pub fn solutions(body: String) -> Self {
        Self {
            content_type: "application/sparql-results+json".to_owned(),
            body,
        }
    }

    /// CONSTRUCT or DESCRIBE: an RDF graph.
    pub fn graph(body: String) -> Self {
        Self {
            content_type: "text/turtle".to_owned(),
            body,
        }
    }
}

/// An evaluator carrying the GeoSPARQL functions.
///
/// oxigraph ships no geometry support; the functions live in `spargeo`, which
/// exports them as `(name, fn(&[Term]) -> Option<Term>)` pairs — exactly what
/// `with_custom_function` takes. Registering all of them rather than a chosen
/// few means the engine's vocabulary does not have to be kept in step by hand
/// with whatever the pushdown learns to lift.
///
/// A geometry argument is only read from a literal typed `geo:wktLiteral` or
/// `geo:geoJSONLiteral`, and only in CRS84. A plain string yields no geometry,
/// so the function returns unbound and the filter is false — silently. That is
/// the failure mode to watch for when a geo query matches nothing.
#[cfg(feature = "sparql-endpoint")]
pub(crate) fn geosparql_evaluator() -> oxigraph::sparql::SparqlEvaluator {
    let mut evaluator = oxigraph::sparql::SparqlEvaluator::new();
    for (name, function) in spargeo::GEOSPARQL_EXTENSION_FUNCTIONS {
        evaluator = evaluator.with_custom_function(name.into_owned(), function);
    }
    evaluator
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
/// * `limits` — Resource limits (max triples, max result rows) to prevent
///   denial-of-service from expensive queries.
///
/// # The serialisation is not a parameter
///
/// It used to be, and the caller could contradict the query: a `CONSTRUCT`
/// with `format = "json"` reached the `Graph` arm below and failed with
/// `Unsupported format for graph results: json`. The caller had to read the
/// query's form to avoid that, which meant a second, cruder parser on the
/// other side of the FFI boundary — and the consolidator-server one read
/// `query.strip().upper().startswith("CONSTRUCT")`, so every `CONSTRUCT`
/// behind a `PREFIX` line — nearly all of them — answered 500.
///
/// Nothing was gained for it: oxigraph hands back `QueryResults::{Solutions,
/// Boolean, Graph}`, so the *result type* already says which serialisation is
/// possible, and the two non-graph arms ignored `format` entirely. The answer
/// therefore carries its own content type.
///
/// # Errors
///
/// * [`ExecuteError::ConversionError`] — an instance's `as_turtle()` failed
///   (data quality issue). The entire query fails; no partial results.
/// * [`ExecuteError::TripleLimitExceeded`] — too many triples in the store.
///   Raised *during* loading, as soon as a chunk crosses the ceiling.
/// * [`ExecuteError::ResultLimitExceeded`] — too many result rows.
/// * [`ExecuteError::QueryError`] — Oxigraph query execution error.
/// * [`ExecuteError::StoreError`] — internal store creation/loading error.
#[cfg(feature = "sparql-endpoint")]
pub fn sparql_execute(
    query_str: &str,
    instances: &[&LinkMLInstance],
    schema_view: &SchemaView,
    limits: ExecuteLimits,
    schema_graph_iri: Option<&str>,
) -> Result<SparqlAnswer, ExecuteError> {
    // The bound on the backlog, before anything is allocated: what a full
    // backlog is bounding is memory, and the store built below is up to
    // `max_triples` quads of it. A call without a ceiling never abandons an
    // evaluation and is not this bound's business.
    if limits.max_eval_millis.is_some() {
        let abandoned = ABANDONED_EVALUATIONS.load(std::sync::atomic::Ordering::SeqCst);
        if abandoned >= MAX_ABANDONED_EVALUATIONS {
            return Err(ExecuteError::EvaluationBacklog {
                abandoned,
                limit: MAX_ABANDONED_EVALUATIONS,
            });
        }
    }

    let store = Store::new().map_err(|e| ExecuteError::StoreError(e.to_string()))?;

    // Load instance data
    let converter = schema_view.converter();
    let primary_schema = schema_view
        .primary_schema()
        .ok_or_else(|| ExecuteError::StoreError("No primary schema found".to_owned()))?;

    // Loaded a chunk at a time, and the ceiling is read between chunks.
    //
    // Two things used to be wrong here, and they were the same line. Each
    // instance was serialised to Turtle and handed to `load_from_reader` on
    // its own: one store transaction per record, which measured ~2.4 ms per
    // record on DEV -- a fixed floor under every query the engine answers
    // (#494, pepibru GitLab). Now each record is still *parsed* on its own,
    // and a chunk of `CHUNK` records is inserted in one transaction.
    //
    // Parsed on its own, and not as one concatenated document: a record's
    // inlined structures are blank nodes, and a blank-node label is scoped to
    // the document it appears in. Concatenated, two records' `_:b0` became
    // one node, and a query over an inlined structure answered a cross
    // product of every record in the chunk (found by the consolidator's
    // oracle over `sourceMetaInfo`). Each parse renames its blank nodes
    // apart, which is what loading the records one by one always did.
    //
    // And the triple ceiling was checked only once everything was in. The
    // all-CivilEngineeringAssets query spent ~45 s loading 1.76 M triples in
    // order to be told 500 000 was the limit. Now the count is read after
    // each chunk, so a query over the cap is refused while loading, at worst
    // one chunk past the line.
    //
    // `CHUNK` is a balance between the two: large enough that the per-load
    // overhead is amortised, small enough that overshooting the ceiling costs
    // a bounded amount of loading. At the tens of triples a record makes, 64
    // records is a few hundred triples of overshoot.
    const CHUNK: usize = 64;
    let mut quads: Vec<oxigraph::model::Quad> = Vec::new();
    for batch in instances.chunks(CHUNK) {
        quads.clear();
        for instance in batch {
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
            for quad in oxigraph::io::RdfParser::from_format(RdfFormat::Turtle)
                .rename_blank_nodes()
                .for_slice(turtle_str.as_bytes())
            {
                quads.push(quad.map_err(|e| ExecuteError::ConversionError {
                    object_uri: object_uri.clone(),
                    message: format!("Failed to load turtle into store: {e}"),
                })?);
            }
        }
        store
            .extend(quads.drain(..))
            .map_err(|e| ExecuteError::StoreError(e.to_string()))?;

        let triple_count = store
            .len()
            .map_err(|e| ExecuteError::StoreError(e.to_string()))?;
        if triple_count > limits.max_triples {
            return Err(ExecuteError::TripleLimitExceeded {
                count: triple_count,
                limit: limits.max_triples,
            });
        }
    }

    // The datamodel, in its own named graph. Deliberately *after* the triple
    // limit check: the limit is about how much instance data a query scoped to,
    // and folding a fixed schema overhead into it would change which queries
    // are refused without telling anyone. It is deliberately in a named graph
    // and not the default one — see [`crate::sparql_schema_graph`] — so the
    // default graph stays byte-identical and the two routes still agree.
    //
    // Built only when the query could actually read it. That is not an
    // optimisation bolted on afterwards but the same fact stated twice: a named
    // graph is unreachable without a `GRAPH` clause, so for an instance query
    // the work would be pure waste. It also means this feature adds exactly
    // zero cost to every request that existed before it.
    //
    // Which graph it goes in is the caller's, threaded down from the active
    // datamodel's configuration: `None` means this datamodel serves no schema
    // graph, and then there is nothing to insert.
    if let Some(schema_graph_iri) = schema_graph_iri
        && crate::sparql_graph_clauses::query_reads_named_graphs(query_str, schema_view)
    {
        let schema_graph =
            crate::sparql_schema_graph::SchemaGraph::build(schema_view, schema_graph_iri)
                .map_err(|e| ExecuteError::StoreError(e.to_string()))?;
        for quad in &schema_graph.quads {
            store
                .insert(quad)
                .map_err(|e| ExecuteError::StoreError(e.to_string()))?;
        }
    }

    // Parse with the parser the rest of the endpoint uses
    // ([`crate::sparql_scoper::sparql_parser`]) and execute *that*, rather than
    // handing oxigraph the string to parse again with a bare parser of its own.
    //
    // Two entry points with two parsers accept two different languages, which
    // is the bug this fixes: the scoper pre-registers `rdf`, `rdfs` and `xsd`,
    // so it planned `?s rdf:type <Class>` — the canonical scoped pattern — and
    // oxigraph then refused the same string as a syntax error, reported as a
    // 500 naming no prefix. `parse_query` also rejects a SPARQL Update by name
    // instead of leaving the engine to fail on it.
    //
    // The parsed algebra goes to oxigraph as-is. oxigraph 0.5 pins the same
    // `spargebra 0.4.7` this crate parses with, so its `Query` wrapper wraps
    // *this crate's* `spargebra::Query`, and the `From<spargebra::Query>` impl
    // therefore applies. Until 0.5 they were unrelated (oxigraph 0.4 pinned
    // `spargebra =0.3.5`), and what crossed instead was the query *rendered
    // back to SPARQL* for oxigraph's own parser to read again — a round trip
    // whose only job was to bridge two versions of one crate.
    let mut parsed = crate::sparql_scoper::parse_query_for(query_str, schema_view)
        .map_err(|e| ExecuteError::QueryError(e.to_string()))?;

    // One slot, two spellings -- and one class, two spellings. The data
    // carries the canonical IRI (the declared `slot_uri` / `class_uri`); a
    // query author writes the readable native one. The planner resolves the
    // alias on its own parse, so this leg has to resolve it on this one --
    // otherwise the SQL leg answers the native spelling and the engine answers
    // nothing, which is a silent route-dependent answer rather than the silent
    // empty column it replaced. The same pass refuses a predicate the
    // subject's class cannot carry, so the two legs refuse the same queries
    // too. See [`crate::sparql_alias`].
    crate::sparql_alias::canonicalize(&mut parsed, schema_view)
        .map_err(|e| ExecuteError::QueryError(e.to_string()))?;

    evaluate_with_ceiling(parsed, store, limits)
}

/// Evaluate a parsed query over a loaded store under the caller's limits,
/// the wall-clock ceiling included: the one evaluation both
/// [`sparql_execute`] and [`sparql_finish`] run, so the two routes are held
/// to the same ceiling, the same backlog bound and the same row cap.
#[cfg(feature = "sparql-endpoint")]
fn evaluate_with_ceiling(
    parsed: spargebra::Query,
    store: Store,
    limits: ExecuteLimits,
) -> Result<SparqlAnswer, ExecuteError> {
    match limits.max_eval_millis {
        None => evaluate(parsed, &store, &limits, None),
        // The ceiling. Evaluation runs on its own thread and the caller waits
        // for its answer until the deadline; past it the caller gets the
        // refusal and the thread is told to stop through the token.
        //
        // Why a thread and not only the token: the token is polled by the
        // evaluator on every quad it reads from the store, and a
        // cartesian-product evaluation spends its time *between* store reads
        // -- spareval 0.2.7's `HashLeftJoinIterator` combines tuples in memory
        // and never asks. Measured on the sibling-label shape of #460: with
        // the token alone, ten label blocks stopped at the deadline and twelve
        // ran on for minutes. So the wait is what bounds the caller, and the
        // token is what bounds the evaluation as soon as it next touches the
        // store. An evaluation that never does runs to completion in the
        // background, holding its store, and its answer is dropped: the
        // request is released, the CPU is not. That is the gap upstream
        // owns, and it is why the planner refuses the one shape known to
        // reach it (`crate::sparql_optional_binding`) rather than relying on
        // this -- and why the number of such evaluations is bounded, below.
        Some(millis) => {
            use std::sync::atomic::{AtomicU8, Ordering};

            // Who let go of this evaluation first: the caller, at the
            // deadline, or the worker, by finishing. Exactly one of them
            // moves the state off RUNNING, and that one owns the count --
            // the caller counts an evaluation it abandoned, and the worker
            // uncounts it when it ends. The caller counts *before* it
            // claims the state and uncounts again if the claim fails, so
            // the worker's decrement can never run ahead of the increment.
            const RUNNING: u8 = 0;
            const FINISHED: u8 = 1;
            const ABANDONED: u8 = 2;
            let state = std::sync::Arc::new(AtomicU8::new(RUNNING));

            let token = oxigraph::sparql::CancellationToken::new();
            let (sender, receiver) = std::sync::mpsc::channel();
            let worker_token = token.clone();
            let worker_state = state.clone();
            std::thread::spawn(move || {
                // The worker's hand-off runs on the way out whatever the
                // way out is: a panic in the evaluator would otherwise
                // leave an abandoned evaluation counted forever, and two of
                // those would refuse every ceilinged call until restart.
                struct Ended(std::sync::Arc<AtomicU8>);
                impl Drop for Ended {
                    fn drop(&mut self) {
                        if self
                            .0
                            .compare_exchange(RUNNING, FINISHED, Ordering::SeqCst, Ordering::SeqCst)
                            .is_err()
                        {
                            ABANDONED_EVALUATIONS.fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                }
                let _ended = Ended(worker_state);
                let answer = evaluate(parsed, &store, &limits, Some(worker_token));
                // The receiver is gone when the caller gave up waiting; the
                // answer has nobody to go to, and that is fine.
                let _ = sender.send(answer);
            });
            match receiver.recv_timeout(std::time::Duration::from_millis(millis)) {
                Ok(answer) => answer,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    token.cancel();
                    ABANDONED_EVALUATIONS.fetch_add(1, Ordering::SeqCst);
                    if state
                        .compare_exchange(RUNNING, ABANDONED, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        // Finished between the timeout and the claim: the
                        // answer is a moment late, and nothing is running.
                        ABANDONED_EVALUATIONS.fetch_sub(1, Ordering::SeqCst);
                    }
                    Err(ExecuteError::EvaluationTimeExceeded { millis })
                }
                // The worker panicked. Nothing to report but that.
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    Err(ExecuteError::QueryError(
                        "the evaluation thread ended without an answer".into(),
                    ))
                }
            }
        }
    }
}

/// Evaluate a parsed, canonicalised query against a loaded store and
/// serialise the answer.
///
/// Everything after the store is built, so that the ceiling can run it on a
/// thread of its own: nothing here borrows the caller's schema or instances.
/// A `SELECT` is evaluated lazily as its solutions are read, so the
/// serialisation loop is where the work happens and the row cap is checked.
#[cfg(feature = "sparql-endpoint")]
fn evaluate(
    parsed: spargebra::Query,
    store: &Store,
    limits: &ExecuteLimits,
    token: Option<oxigraph::sparql::CancellationToken>,
) -> Result<SparqlAnswer, ExecuteError> {
    // What the engine's error means here. `Cancelled` can only come from the
    // ceiling -- nothing else holds the token -- so it *is* the ceiling.
    let eval_error = |e: oxigraph::sparql::QueryEvaluationError| match (e, limits.max_eval_millis) {
        (oxigraph::sparql::QueryEvaluationError::Cancelled, Some(millis)) => {
            ExecuteError::EvaluationTimeExceeded { millis }
        }
        (e, _) => ExecuteError::QueryError(e.to_string()),
    };
    let mut evaluator = geosparql_evaluator();
    if let Some(token) = token {
        evaluator = evaluator.with_cancellation_token(token);
    }
    let results = evaluator
        .for_query(parsed)
        .on_store(store)
        .execute()
        .map_err(eval_error)?;

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
                let solution = solution.map_err(eval_error)?;

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
            let body = serde_json::to_string(&result)
                .map_err(|e| ExecuteError::QueryError(e.to_string()))?;
            Ok(SparqlAnswer::solutions(body))
        }
        QueryResults::Boolean(b) => {
            let result = serde_json::json!({ "boolean": b });
            let body = serde_json::to_string(&result)
                .map_err(|e| ExecuteError::QueryError(e.to_string()))?;
            Ok(SparqlAnswer::solutions(body))
        }
        QueryResults::Graph(triples) => {
            let mut buf = Vec::new();
            for triple in triples {
                let triple = triple.map_err(eval_error)?;
                use std::io::Write;
                writeln!(
                    buf,
                    "{} {} {} .",
                    triple.subject, triple.predicate, triple.object
                )
                .map_err(|e| ExecuteError::QueryError(e.to_string()))?;
            }
            let body =
                String::from_utf8(buf).map_err(|e| ExecuteError::QueryError(e.to_string()))?;
            Ok(SparqlAnswer::graph(body))
        }
    }
}

/// Every pattern of a query, mutably, for the placeholder walk. Exhaustive
/// over what the planner writes back; a variant it does not visit holds no
/// placeholder the count can find, so an unvisited shape fails closed at
/// plan time rather than being filled wrongly here.
fn visit_patterns(
    pattern: &mut spargebra::algebra::GraphPattern,
    f: &mut dyn FnMut(&mut spargebra::algebra::GraphPattern),
) {
    use spargebra::algebra::GraphPattern as P;
    f(pattern);
    match pattern {
        P::Join { left, right }
        | P::LeftJoin { left, right, .. }
        | P::Lateral { left, right }
        | P::Union { left, right }
        | P::Minus { left, right } => {
            visit_patterns(left, f);
            visit_patterns(right, f);
        }
        P::Filter { inner, .. }
        | P::Graph { inner, .. }
        | P::Extend { inner, .. }
        | P::OrderBy { inner, .. }
        | P::Project { inner, .. }
        | P::Distinct { inner }
        | P::Reduced { inner }
        | P::Slice { inner, .. }
        | P::Group { inner, .. }
        | P::Service { inner, .. } => visit_patterns(inner, f),
        P::Bgp { .. } | P::Path { .. } | P::Values { .. } => {}
    }
}

fn query_pattern(query: &mut spargebra::Query) -> &mut spargebra::algebra::GraphPattern {
    match query {
        spargebra::Query::Select { pattern, .. }
        | spargebra::Query::Construct { pattern, .. }
        | spargebra::Query::Describe { pattern, .. }
        | spargebra::Query::Ask { pattern, .. } => pattern,
    }
}

/// How many `VALUES` blocks of `query` are the statement's placeholder: no
/// rows, and exactly `vars` in that order. The planner requires one before
/// it writes a finish query into a plan, so the executor can never fill the
/// wrong table.
pub fn placeholders_in(query: &spargebra::Query, vars: &[spargebra::term::Variable]) -> usize {
    let mut query = query.clone();
    let mut count = 0usize;
    visit_patterns(query_pattern(&mut query), &mut |pattern| {
        if let spargebra::algebra::GraphPattern::Values {
            variables,
            bindings,
        } = pattern
            && bindings.is_empty()
            && variables.as_slice() == vars
        {
            count += 1;
        }
    });
    count
}

/// A SPARQL-results term, back as a ground term.
#[cfg(feature = "sparql-endpoint")]
fn ground_of_json(value: &serde_json::Value) -> Result<spargebra::term::GroundTerm, ExecuteError> {
    use spargebra::term::{GroundTerm, Literal, NamedNode};
    let bad = |why: &str| ExecuteError::QueryError(format!("a solution cell {why}: {value}"));
    let kind = value
        .get("type")
        .and_then(|kind| kind.as_str())
        .ok_or_else(|| bad("has no type"))?;
    let text = value
        .get("value")
        .and_then(|text| text.as_str())
        .ok_or_else(|| bad("has no value"))?;
    Ok(match kind {
        "uri" => GroundTerm::NamedNode(NamedNode::new(text).map_err(|_| bad("is not an IRI"))?),
        "literal" | "typed-literal" => {
            if let Some(lang) = value.get("xml:lang").and_then(|lang| lang.as_str()) {
                GroundTerm::Literal(
                    Literal::new_language_tagged_literal(text, lang)
                        .map_err(|_| bad("has a bad language tag"))?,
                )
            } else if let Some(datatype) = value.get("datatype").and_then(|dt| dt.as_str()) {
                GroundTerm::Literal(Literal::new_typed_literal(
                    text,
                    NamedNode::new(datatype).map_err(|_| bad("has a bad datatype"))?,
                ))
            } else {
                GroundTerm::Literal(Literal::new_simple_literal(text))
            }
        }
        // The statement never emits a blank node: a structure is
        // representable and never serialisable.
        _ => return Err(bad("is not an IRI or a literal")),
    })
}

/// **Finish a query over the statement's rows** (M2 of
/// `docs/design/sparql-schema-relations-and-row-finish.md`).
///
/// `plan` is a `UsedRows` plan: its engine pass carries
/// [`crate::sparql_plan::EngineInput::Solutions`], whose `finish` is the
/// engine region written back at plan time with the statement as a
/// placeholder `VALUES`. `solutions_json` is the statement's rows as SPARQL
/// Query Results JSON, **in the order the statement returned them**: when
/// the plan names an ordinal, row *i* (from 1) is bound to it here, so a
/// caller that reorders the rows reorders the answer.
///
/// The rows replace the placeholder, and the finish query is evaluated over
/// a store holding **only the schema graph** (built only when the finish
/// reads a named graph), by the same evaluator and under the same limits as
/// [`sparql_execute`]. There are no instance triples, so the triple ceiling
/// bounds the **cells** handed over instead -- rows × bound columns, the
/// ordinal included -- which a cell costs no more than the triple it would
/// have been on the records route; over it, the answer is
/// [`ExecuteError::TripleLimitExceeded`], counting cells.
///
/// Nothing is derived from the plan here: the query evaluated is the one
/// the planner verified and printed.
#[cfg(feature = "sparql-endpoint")]
pub fn sparql_finish(
    plan: &crate::sparql_plan::ExecutionPlan,
    solutions_json: &str,
    schema_view: &SchemaView,
    limits: ExecuteLimits,
    schema_graph_iri: Option<&str>,
) -> Result<SparqlAnswer, ExecuteError> {
    use crate::sparql_plan::{EngineInput, PassKind};

    let Some((vars, ordinal, finish)) = plan.passes.iter().find_map(|pass| match &pass.kind {
        PassKind::Engine(engine) => match &engine.input {
            EngineInput::Solutions {
                vars,
                ordinal,
                finish,
                ..
            } => Some((vars, ordinal, finish)),
            EngineInput::Records => None,
        },
        PassKind::Sql(_) => None,
    }) else {
        return Err(ExecuteError::QueryError(
            "the plan finishes over records, not rows: run it with sparql_execute".to_owned(),
        ));
    };
    if limits.max_eval_millis.is_some() {
        let abandoned = ABANDONED_EVALUATIONS.load(std::sync::atomic::Ordering::SeqCst);
        if abandoned >= MAX_ABANDONED_EVALUATIONS {
            return Err(ExecuteError::EvaluationBacklog {
                abandoned,
                limit: MAX_ABANDONED_EVALUATIONS,
            });
        }
    }

    let results: serde_json::Value = serde_json::from_str(solutions_json)
        .map_err(|e| ExecuteError::QueryError(format!("the statement's rows: {e}")))?;
    let bindings = results
        .get("results")
        .and_then(|results| results.get("bindings"))
        .and_then(|bindings| bindings.as_array())
        .ok_or_else(|| {
            ExecuteError::QueryError("the statement's rows carry no results.bindings".to_owned())
        })?;

    // The cell budget, before anything is built.
    let mut cells = 0usize;
    for binding in bindings {
        cells += vars
            .iter()
            .filter(|var| binding.get(var.as_str()).is_some())
            .count();
        if ordinal.is_some() {
            cells += 1;
        }
        if cells > limits.max_triples {
            return Err(ExecuteError::TripleLimitExceeded {
                count: cells,
                limit: limits.max_triples,
            });
        }
    }

    let mut placeholder: Vec<spargebra::term::Variable> = vars
        .iter()
        .map(|var| spargebra::term::Variable::new_unchecked(var.clone()))
        .collect();
    if let Some(ordinal) = ordinal {
        placeholder.push(spargebra::term::Variable::new_unchecked(ordinal.clone()));
    }
    let mut rows: Vec<Vec<Option<spargebra::term::GroundTerm>>> =
        Vec::with_capacity(bindings.len());
    for (index, binding) in bindings.iter().enumerate() {
        let mut row = Vec::with_capacity(placeholder.len());
        for var in vars {
            row.push(match binding.get(var.as_str()) {
                Some(cell) => Some(ground_of_json(cell)?),
                None => None,
            });
        }
        if ordinal.is_some() {
            row.push(Some(spargebra::term::GroundTerm::Literal(
                spargebra::term::Literal::from((index + 1) as i64),
            )));
        }
        rows.push(row);
    }

    let mut parsed = crate::sparql_scoper::parse_query(finish)
        .map_err(|e| ExecuteError::QueryError(format!("the finish query: {e}")))?;
    let mut filled = 0usize;
    let mut rows = Some(rows);
    visit_patterns(query_pattern(&mut parsed), &mut |pattern| {
        if let spargebra::algebra::GraphPattern::Values {
            variables,
            bindings,
        } = pattern
            && bindings.is_empty()
            && variables.as_slice() == placeholder.as_slice()
            && let Some(rows) = rows.take()
        {
            *bindings = rows;
            filled += 1;
        }
    });
    if filled != 1 {
        return Err(ExecuteError::QueryError(
            "the finish query has no placeholder for the statement's rows".to_owned(),
        ));
    }

    let store = Store::new().map_err(|e| ExecuteError::StoreError(e.to_string()))?;
    if let Some(schema_graph_iri) = schema_graph_iri
        && crate::sparql_graph_clauses::query_reads_named_graphs(finish, schema_view)
    {
        let schema_graph =
            crate::sparql_schema_graph::SchemaGraph::build(schema_view, schema_graph_iri)
                .map_err(|e| ExecuteError::StoreError(e.to_string()))?;
        for quad in &schema_graph.quads {
            store
                .insert(quad)
                .map_err(|e| ExecuteError::StoreError(e.to_string()))?;
        }
    }
    evaluate_with_ceiling(parsed, store, limits)
}

/// Convert an RDF term to SPARQL JSON Results format.
#[cfg(feature = "sparql-endpoint")]
pub(crate) fn term_to_json(term: &oxigraph::model::Term) -> serde_json::Value {
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
            ExecuteLimits::default(),
            None,
        )
        .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&result.body).unwrap();
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
            ExecuteLimits::default(),
            None,
        )
        .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&result.body).unwrap();
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
            ExecuteLimits::default(),
            None,
        )
        .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&result.body).unwrap();
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
            ExecuteLimits {
                max_triples: 500_000,
                max_result_rows: 1,
                max_eval_millis: None,
            },
            None,
        );

        assert!(matches!(
            result,
            Err(ExecuteError::ResultLimitExceeded { .. })
        ));
    }

    /// The two ceiling tests share the process-wide backlog count, and one of
    /// them fills it on purpose; they take turns.
    static BACKLOG: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// The ceiling answers the caller while an evaluation is still computing
    /// its first row: a four-way cross product under an `ORDER BY`
    /// materialises every combination before it yields one, so neither the
    /// triple cap (a dozen triples) nor the row cap (checked per row yielded)
    /// ever fires (#460). The abandoned evaluation finishes on its own thread
    /// in the background -- a hundred thousand rows here, a moment of CPU --
    /// and is counted for exactly as long as it runs.
    #[test]
    fn the_engine_ceiling_stops_an_evaluation_before_its_first_row() {
        let _turn = BACKLOG.lock().unwrap_or_else(|e| e.into_inner());
        let sv = test_schema_view();
        let instances = signal_instances(&sv);
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();
        let query = "SELECT * WHERE { ?a ?p ?o . ?b ?q ?r . ?c ?x ?y . ?d ?z ?w } \
                     ORDER BY ?a ?b ?c ?d";
        let started = std::time::Instant::now();
        let result = sparql_execute(
            query,
            &refs,
            &sv,
            ExecuteLimits {
                max_triples: 500_000,
                max_result_rows: 10,
                max_eval_millis: Some(1),
            },
            None,
        );
        assert!(
            matches!(
                result,
                Err(ExecuteError::EvaluationTimeExceeded { millis: 1 })
            ),
            "expected the ceiling, got {result:?}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "the ceiling fired late: {:?}",
            started.elapsed()
        );
        // The abandoned evaluation was counted, and is uncounted when it
        // ends: at most one is running, and within a few seconds none.
        assert!(abandoned_evaluations() <= 1, "{}", abandoned_evaluations());
        let settled = std::time::Instant::now();
        while abandoned_evaluations() > 0 {
            assert!(
                settled.elapsed() < std::time::Duration::from_secs(30),
                "the abandoned evaluation never uncounted itself"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }

        // The same store, a query that finishes: the ceiling is not a cost.
        let answer = sparql_execute(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s a asset360:Signal }",
            &refs,
            &sv,
            ExecuteLimits {
                max_triples: 500_000,
                max_result_rows: 10_000,
                max_eval_millis: Some(30_000),
            },
            None,
        )
        .expect("a query under the ceiling answers");
        let parsed: serde_json::Value = serde_json::from_str(&answer.body).unwrap();
        assert_eq!(
            parsed["results"]["bindings"].as_array().unwrap().len(),
            refs.len()
        );
    }

    /// A process already carrying `MAX_ABANDONED_EVALUATIONS` runaway
    /// evaluations refuses to start another under a ceiling, by name and
    /// before it loads a store; a call without a ceiling is not its business.
    #[test]
    fn the_engine_leg_is_refused_while_the_backlog_is_full() {
        use std::sync::atomic::Ordering;
        let _turn = BACKLOG.lock().unwrap_or_else(|e| e.into_inner());
        let sv = test_schema_view();
        let instances = signal_instances(&sv);
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();
        let query = "PREFIX asset360: <https://data.infrabel.be/asset360/> \
                     SELECT ?s WHERE { ?s a asset360:Signal }";
        // A triple cap no store can meet: were the store loaded first, its
        // refusal would win, so the backlog refusal winning is the proof
        // that nothing was loaded.
        let with_ceiling = ExecuteLimits {
            max_triples: 0,
            max_result_rows: 10_000,
            max_eval_millis: Some(30_000),
        };

        // Stand in for evaluations the ceiling gave up on and that are still
        // running; undone below whatever the assertions say.
        ABANDONED_EVALUATIONS.fetch_add(MAX_ABANDONED_EVALUATIONS, Ordering::SeqCst);
        let refused = sparql_execute(query, &refs, &sv, with_ceiling, None);
        let unbounded = sparql_execute(query, &refs, &sv, ExecuteLimits::default(), None);
        ABANDONED_EVALUATIONS.fetch_sub(MAX_ABANDONED_EVALUATIONS, Ordering::SeqCst);

        assert!(
            matches!(
                refused,
                Err(ExecuteError::EvaluationBacklog {
                    abandoned: MAX_ABANDONED_EVALUATIONS,
                    limit: MAX_ABANDONED_EVALUATIONS
                })
            ),
            "expected the backlog refusal, got {refused:?}"
        );
        assert!(unbounded.is_ok(), "no ceiling, no backlog: {unbounded:?}");

        // The backlog gone, the same call answers.
        let answered = sparql_execute(
            query,
            &refs,
            &sv,
            ExecuteLimits {
                max_triples: 500_000,
                max_result_rows: 10_000,
                max_eval_millis: Some(30_000),
            },
            None,
        );
        assert!(answered.is_ok(), "{answered:?}");
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
            ExecuteLimits {
                max_triples: 1,
                max_result_rows: 10_000,
                max_eval_millis: None,
            },
            None,
        );

        assert!(matches!(
            result,
            Err(ExecuteError::TripleLimitExceeded { .. })
        ));
    }

    #[test]
    fn the_triple_ceiling_stops_the_load_instead_of_reading_it_all_first() {
        // The all-CivilEngineeringAssets query spent ~45 s loading 1.76 M
        // triples in order to be told the limit was 500 000 (#494, pepibru
        // GitLab). The ceiling is read while loading now, so what reaches the
        // store is bounded by the ceiling plus one chunk -- which is what
        // this asserts, because "it was faster" is not a property a test can
        // hold on to.
        let sv = test_schema_view();
        let instances: Vec<LinkMLInstance> = (0..1000)
            .map(|i| {
                load_signal(
                    &sv,
                    &format!(
                        r#"{{"asset360_uri": "https://data.infrabel.be/asset360/signal/S{i}", "name": "S{i}"}}"#
                    ),
                )
            })
            .collect();
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();
        let result = sparql_execute(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s a asset360:Signal }",
            &refs,
            &sv,
            ExecuteLimits {
                max_triples: 100,
                max_result_rows: 10_000,
                max_eval_millis: None,
            },
            None,
        );

        let Err(ExecuteError::TripleLimitExceeded { count, limit }) = result else {
            panic!("expected the triple ceiling, got {result:?}");
        };
        assert_eq!(limit, 100);
        // 1000 signals make thousands of triples. Stopping means the store
        // never held them: one chunk of 64 records past the ceiling at most.
        assert!(
            count < 1000,
            "loading should have stopped near the ceiling, but {count} triples were read"
        );
    }

    /// Two records' inlined structures stay two blank nodes when they are
    /// loaded in one chunk: each record is parsed on its own, so the `_:b0`
    /// every record's Turtle starts from is renamed apart. Concatenated into
    /// one document, the two became one node and the query answered a cross
    /// product -- four rows for two signals (found by the consolidator's
    /// oracle over `sourceMetaInfo`, #494 pepibru GitLab).
    #[test]
    fn a_chunk_keeps_each_records_blank_nodes_its_own() {
        let sv = crate::sparql_scoper::tests::test_schema_view();
        let conv = sv.converter();
        let class = sv
            .get_class(&Identifier::new("Signal"), &conv)
            .unwrap()
            .unwrap();
        let instances: Vec<LinkMLInstance> = [("A", 1), ("B", 2)]
            .iter()
            .map(|(id, lon)| {
                load_json_str(
                    &format!(
                        r#"{{"asset360_uri": "https://data.infrabel.be/asset360/signal/{id}", "location": {{"longitude": {lon}}}}}"#
                    ),
                    &sv,
                    &class,
                    &conv,
                )
                .unwrap()
                .into_instance_tolerate_errors()
                .unwrap()
            })
            .collect();
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();
        let result = sparql_execute(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s ?lon WHERE { ?s a asset360:Signal ; asset360:location ?l . \
             ?l asset360:longitude ?lon }",
            &refs,
            &sv,
            ExecuteLimits::default(),
            None,
        )
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result.body).unwrap();
        let bindings = parsed["results"]["bindings"].as_array().unwrap();
        assert_eq!(
            bindings.len(),
            2,
            "one row per signal, not a product: {bindings:?}"
        );
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
            ExecuteLimits::default(),
            None,
        )
        .unwrap();

        assert!(result.body.contains("BX517"), "Should contain signal name");
        assert!(result.body.contains("Signal"), "Should contain type");
        // The form decides the serialisation, and the form is parsed here — a
        // caller cannot ask for a graph as SPARQL-results JSON any more.
        assert_eq!(result.content_type, "text/turtle");
    }

    #[test]
    fn a_query_may_use_a_preregistered_prefix_it_did_not_declare() {
        // `?s rdf:type <Class>` is the canonical scoped pattern, and the
        // scoper's own refusal message asks for it. It used to reach oxigraph
        // as an unparsed string, whose parser knows no prefix it was not
        // handed, so the endpoint planned the query and then failed to execute
        // it — a syntax error naming no prefix, served as a 500.
        let sv = test_schema_view();
        let instances = signal_instances(&sv);
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();
        let result = sparql_execute(
            "PREFIX p: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s rdf:type p:Signal }",
            &refs,
            &sv,
            ExecuteLimits::default(),
            None,
        )
        .unwrap();

        assert_eq!(result.content_type, "application/sparql-results+json");
        let parsed: serde_json::Value = serde_json::from_str(&result.body).unwrap();
        assert_eq!(parsed["results"]["bindings"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn a_query_keeps_its_own_binding_of_a_preregistered_prefix() {
        // The pre-registered prefixes are defaults, not overrides: a query that
        // binds `rdf:` itself means what it says, and here that is a predicate
        // nothing in the store carries. Answering it as though it said
        // `rdf:type` would be a wrong answer rather than an empty one.
        let sv = test_schema_view();
        let instances = signal_instances(&sv);
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();
        let result = sparql_execute(
            "PREFIX rdf: <urn:not-rdf#> \
             PREFIX p: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s rdf:type p:Signal }",
            &refs,
            &sv,
            ExecuteLimits::default(),
            None,
        )
        .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&result.body).unwrap();
        assert!(parsed["results"]["bindings"].as_array().unwrap().is_empty());
    }

    #[test]
    fn an_update_is_rejected_by_the_engine_leg_too() {
        // Both legs parse with `sparql_scoper::parse_query`, so neither can be
        // the one that executes a mutation.
        let sv = test_schema_view();
        let result = sparql_execute(
            "PREFIX p: <https://data.infrabel.be/asset360/> \
             INSERT DATA { <urn:a> p:name \"x\" }",
            &[],
            &sv,
            ExecuteLimits::default(),
            None,
        );

        let message = match result {
            Err(ExecuteError::QueryError(message)) => message,
            other => panic!("expected a query error, got {other:?}"),
        };
        assert!(message.contains("read-only"), "{message}");
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
///
/// This module also hosts one test that is not about pushdown at all —
/// `a_prefixed_query_executes_and_keeps_language_tags`, a regression guard for
/// the executor's query hand-off. It lives here rather than in `mod tests`
/// because it needs this module's `schema()`/`instance()`/`PREFIX` fixtures,
/// which are nontrivial enough that duplicating or importing them was worse
/// than the mislabeling.
#[cfg(all(test, feature = "sparql-endpoint"))]
mod pushed_filters_match_sparql {
    use crate::sparql_scoper::{FilterCondition, LikeAnchor, sparql_scope};
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
            super::ExecuteLimits::default(),
            None,
        )
        .expect("query executes");
        let parsed: serde_json::Value = serde_json::from_str(&json.body).unwrap();
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
            FilterCondition::Like {
                value,
                anchor,
                case_insensitive,
            } => stored.iter().any(|s| {
                let (haystack, needle) = if *case_insensitive {
                    (s.to_lowercase(), value.to_lowercase())
                } else {
                    (s.to_string(), value.clone())
                };
                match anchor {
                    LikeAnchor::Prefix => haystack.starts_with(&needle),
                    LikeAnchor::Suffix => haystack.ends_with(&needle),
                    LikeAnchor::Anywhere => haystack.contains(&needle),
                }
            }),
            // `IS NOT NULL AND <>`: absent from `stored` fails the null test,
            // same as it would in SQL.
            FilterCondition::Ne(value) => stored.first().is_some_and(|s| *s != value.as_str()),
            // `IS NOT PRESENT`. `stored.is_empty()` is the reference answer
            // for what `!bound` sees *as this harness can express it*:
            // `null` ("checked, empty") and a missing key ("don't know")
            // are a real semantic difference in this project, but the
            // triplifier emits no triple for either, so `stored` cannot
            // distinguish them and neither can this oracle.
            //
            // The real SQL renderer (a later task) is specified to emit
            // both halves of the check —
            // `NOT (object_data ? 'field' AND jsonb_typeof(object_data->'field') <> 'null')`
            // — precisely because key existence alone is wrong for an
            // explicit null: `object_data ? 'field'` is *true* for a
            // present-but-null value, so a renderer that checked only that
            // half would disagree with this oracle on exactly a record
            // whose slot is explicitly `null` (this harness reports
            // `NotBound` true there; key-existence-only reports it false).
            FilterCondition::NotBound => stored.is_empty(),
            // The same function table `geosparql_evaluator` registers into
            // oxigraph (`spargeo::GEOSPARQL_EXTENSION_FUNCTIONS`), so this
            // oracle can never drift from what the engine leg itself
            // decides for `geof:sfIntersects` -- see that function's doc
            // comment for why a plain string reads no geometry at all.
            //
            // Unreachable through `every_pushed_constant_answers_what_sparql_answers`'s
            // grid as it stands today: that grid only ever consults
            // `star.filters` (single-hop), and `Intersects` is at least two
            // hops by construction (`sparql_columns.rs`'s registry tail), so
            // it only ever lands in `star.path_filters`. Real rather than a
            // placeholder anyway -- a caller who does wire a
            // geometry-bearing fixture into the grid gets a correct answer,
            // not a silent pass.
            FilterCondition::Intersects { wkt } => {
                stored.iter().any(|s| geometry_intersects(s, wkt))
            }
        })
    }

    /// The real `geof:sfIntersects`, called directly against two `wktLiteral`
    /// terms built from plain WKT text -- the same function
    /// `geosparql_evaluator` wires into oxigraph, so `rendering_matches`'s
    /// `Intersects` arm can be checked against the engine's own answer rather
    /// than a hand-rolled approximation of one.
    ///
    /// `None` (an unbound result -- neither term parses, or the datatype is
    /// wrong) reads as `false`: the same "silently no rows" answer
    /// `FILTER(geof:sfIntersects(...))` gives SPARQL itself.
    fn geometry_intersects(stored: &str, wkt: &str) -> bool {
        let datatype =
            spargebra::term::NamedNode::new_unchecked(crate::sparql_scoper::WKT_LITERAL_IRI);
        let a = spargebra::term::Term::Literal(spargebra::term::Literal::new_typed_literal(
            stored,
            datatype.clone(),
        ));
        let b = spargebra::term::Term::Literal(spargebra::term::Literal::new_typed_literal(
            wkt, datatype,
        ));
        let sf_intersects = spargeo::GEOSPARQL_EXTENSION_FUNCTIONS
            .iter()
            .find(|(name, _)| name.as_str() == crate::sparql_scoper::SF_INTERSECTS_IRI)
            .map(|(_, function)| function)
            .expect("spargeo ships geof:sfIntersects");
        match sf_intersects(&[a, b]) {
            Some(spargebra::term::Term::Literal(result)) => result.value() == "true",
            _ => false,
        }
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

    /// A prefixed query executes, and a language-tagged literal keeps its tag.
    ///
    /// The executor hands oxigraph the parsed algebra, not a re-rendered
    /// string. This is the property that hand-off must preserve: the crate's
    /// own parser resolves the prefixes, and nothing downstream re-parses.
    #[test]
    fn a_prefixed_query_executes_and_keeps_language_tags() {
        let sv = schema();
        let inst = instance(&sv);
        let refs = vec![&inst];

        let answer = super::sparql_execute(
            &format!(
                "{PREFIX}SELECT ?d WHERE {{ ?s a asset360:Signal ; asset360:description ?d }}"
            ),
            &refs,
            &sv,
            super::ExecuteLimits::default(),
            None,
        )
        .expect("query executes");

        let parsed: serde_json::Value = serde_json::from_str(&answer.body).unwrap();
        let bindings = parsed["results"]["bindings"].as_array().unwrap();
        assert_eq!(bindings.len(), 1, "one signal, one description");
        assert_eq!(bindings[0]["d"]["value"], "hello");
        assert_eq!(
            bindings[0]["d"]["xml:lang"], "en",
            "the schema declares in_language: en, so the tag must survive"
        );
    }
}

/// The schema named graph, seen from the query engine.
///
/// [`crate::sparql_schema_graph`] tests what is emitted; these test what a
/// client actually gets back, and — the load-bearing one — that its presence
/// does not change a single instance answer.
#[cfg(all(test, feature = "sparql-endpoint"))]
mod schema_graph_tests {
    use super::*;
    /// What the asset360 datamodel's config sets `schema_graph_iri` to.
    /// A test value: production reads it from
    /// `asset360_model/datamodels/asset360.yaml`.
    const SCHEMA_GRAPH_IRI: &str = "https://data.infrabel.be/asset360/schema";
    use linkml_runtime::load_json_str;
    use linkml_schemaview::identifier::Identifier;

    const GSA_IRI: &str = "http://ontorail.org/src/Eulynx/eul2207a/EAID_28C3D8B9";

    fn schema() -> SchemaView {
        use linkml_meta::SchemaDefinition;
        use serde_path_to_error as p2e;
        use serde_yml as yml;

        let schema_yaml = r#"
id: https://data.infrabel.be/asset360
name: asset360
prefixes:
  asset360:
    prefix_reference: https://data.infrabel.be/asset360/
  eulynx:
    prefix_reference: http://ontorail.org/src/Eulynx/eul2207a/
  linkml:
    prefix_reference: https://w3id.org/linkml/
default_prefix: asset360
default_range: string

enums:
  SignalTypeEnum:
    description: The kind of signal.
    permissible_values:
      GSA:
        description: Group start signal, type A.
        meaning: eulynx:EAID_28C3D8B9
      LOCAL_ONLY: {}

classes:
  Signal:
    class_uri: asset360:Signal
    description: A signal.
    attributes:
      asset360_uri:
        identifier: true
      name:
        range: string
      signalType:
        range: SignalTypeEnum
"#;
        let schema: SchemaDefinition =
            p2e::deserialize(yml::Deserializer::from_str(schema_yaml)).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        sv
    }

    fn instances(sv: &SchemaView) -> Vec<LinkMLInstance> {
        let conv = sv.converter();
        let cv = sv
            .get_class(&Identifier::new("Signal"), &conv)
            .unwrap()
            .unwrap();
        [
            r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/BX517", "name": "BX517", "signalType": "GSA"}"#,
            r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/BX518", "name": "BX518", "signalType": "LOCAL_ONLY"}"#,
        ]
        .iter()
        .map(|json| {
            load_json_str(json, sv, &cv, &conv)
                .unwrap()
                .into_instance_tolerate_errors()
                .unwrap()
        })
        .collect()
    }

    fn run(sv: &SchemaView, instances: &[LinkMLInstance], query: &str) -> serde_json::Value {
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();
        let raw = sparql_execute(
            query,
            &refs,
            sv,
            ExecuteLimits::default(),
            Some(SCHEMA_GRAPH_IRI),
        )
        .unwrap_or_else(|err| panic!("query failed: {err}\n{query}"));
        serde_json::from_str(&raw.body).unwrap()
    }

    /// The question that motivated the feature: an enum value comes back as an
    /// opaque IRI, and the client wants the code.
    #[test]
    fn the_schema_graph_answers_a_label_lookup_for_an_enum_value() {
        let sv = schema();
        let instances = instances(&sv);
        let answer = run(
            &sv,
            &instances,
            &format!(
                "SELECT ?code WHERE {{ GRAPH <{SCHEMA_GRAPH_IRI}> {{ \
                 <{GSA_IRI}> <http://www.w3.org/2000/01/rdf-schema#label> ?code }} }}"
            ),
        );
        let bindings = answer["results"]["bindings"].as_array().unwrap();
        assert_eq!(bindings.len(), 1, "expected exactly one label: {answer}");
        assert_eq!(bindings[0]["code"]["value"], "GSA");
    }

    /// Discovery without knowing any IRI up front: from the class, to its
    /// slot, to the slot's enum, to the enum's permissible values.
    #[test]
    fn a_client_can_walk_from_a_class_to_an_enum_s_permissible_values() {
        let sv = schema();
        let instances = instances(&sv);
        let answer = run(
            &sv,
            &instances,
            &format!(
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \
                 PREFIX skos: <http://www.w3.org/2004/02/skos/core#> \
                 PREFIX schema: <https://schema.org/> \
                 SELECT ?code WHERE {{ GRAPH <{SCHEMA_GRAPH_IRI}> {{ \
                   ?class rdfs:label \"Signal\" . \
                   ?slot schema:domainIncludes ?class ; rdfs:label \"signalType\" ; rdfs:range ?enum . \
                   ?value skos:inScheme ?enum ; rdfs:label ?code . \
                 }} }} ORDER BY ?code"
            ),
        );
        let bindings = answer["results"]["bindings"].as_array().unwrap();
        let codes: Vec<&str> = bindings
            .iter()
            .map(|b| b["code"]["value"].as_str().unwrap())
            .collect();
        // Every permissible value is a member of its scheme, whether or not it
        // carries a `meaning`: `GSA` under its expanded meaning IRI, and
        // `LOCAL_ONLY` under the `<enum_uri>#<code>` IRI upstream mints for a
        // meaning-less value. The minted IRI joins nothing in the instance data
        // — `LOCAL_ONLY` is a plain literal there — but the schema graph still
        // has to answer "which values does this enum permit?" with all of them.
        assert_eq!(codes, vec!["GSA", "LOCAL_ONLY"], "{answer}");
    }

    /// The parity guard, asserted directly rather than trusted: the schema
    /// graph must not add, remove or change one instance solution — including
    /// for the wide-open `?s ?p ?o` shape the differential oracle uses.
    #[test]
    fn instance_answers_are_unchanged_by_the_schema_graph() {
        let sv = schema();
        let instances = instances(&sv);
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();

        // The second half of each pair carries a `GRAPH` clause that can never
        // match, so it binds nothing and drops no row — but it does force the
        // schema graph to be built and loaded. Without it the executor's
        // "only build when a GRAPH clause could read it" gate would mean these
        // cases never see a loaded schema graph, and the test would prove the
        // gate rather than the isolation.
        let noop_graph = format!(
            "OPTIONAL {{ GRAPH <{SCHEMA_GRAPH_IRI}> {{ <urn:x:none> <urn:x:none> <urn:x:none> }} }}"
        );
        let cases = [
            (
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o".to_owned(),
                format!("SELECT ?s ?p ?o WHERE {{ ?s ?p ?o {noop_graph} }} ORDER BY ?s ?p ?o"),
            ),
            (
                "PREFIX asset360: <https://data.infrabel.be/asset360/> \
                 SELECT ?s ?name WHERE { ?s a asset360:Signal ; asset360:name ?name } \
                 ORDER BY ?name"
                    .to_owned(),
                format!(
                    "PREFIX asset360: <https://data.infrabel.be/asset360/> \
                     SELECT ?s ?name WHERE {{ ?s a asset360:Signal ; asset360:name ?name . \
                     {noop_graph} }} ORDER BY ?name"
                ),
            ),
            (
                "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }".to_owned(),
                format!("SELECT (COUNT(*) AS ?n) WHERE {{ ?s ?p ?o {noop_graph} }}"),
            ),
        ];

        for (query, forced) in &cases {
            let query = query.as_str();
            let with_schema = sparql_execute(
                query,
                &refs,
                &sv,
                ExecuteLimits::default(),
                Some(SCHEMA_GRAPH_IRI),
            )
            .unwrap_or_else(|err| panic!("{query}: {err}"));
            let with_schema_loaded = sparql_execute(
                forced,
                &refs,
                &sv,
                ExecuteLimits::default(),
                Some(SCHEMA_GRAPH_IRI),
            )
            .unwrap_or_else(|err| panic!("{forced}: {err}"));

            // The same query against a store holding instance data only.
            let store = Store::new().unwrap();
            let conv = sv.converter();
            let primary = sv.primary_schema().unwrap();
            for instance in &refs {
                let turtle = turtle_to_string(
                    instance,
                    &sv,
                    &primary,
                    &conv,
                    TurtleOptions { skolem: false },
                )
                .unwrap();
                store
                    .load_from_reader(RdfFormat::Turtle, turtle.as_bytes())
                    .unwrap();
            }
            let baseline = match super::geosparql_evaluator()
                .for_query(query.parse::<spargebra::Query>().unwrap())
                .on_store(&store)
                .execute()
                .unwrap()
            {
                QueryResults::Solutions(solutions) => {
                    let vars: Vec<String> = solutions
                        .variables()
                        .iter()
                        .map(|v| v.as_str().to_owned())
                        .collect();
                    let mut rows = Vec::new();
                    for solution in solutions {
                        let solution = solution.unwrap();
                        let mut row = serde_json::Map::new();
                        for var in &vars {
                            if let Some(term) = solution.get(var.as_str()) {
                                row.insert(var.clone(), term_to_json(term));
                            }
                        }
                        rows.push(serde_json::Value::Object(row));
                    }
                    serde_json::json!({
                        "head": { "vars": vars },
                        "results": { "bindings": rows }
                    })
                }
                _ => panic!("expected solutions for {query}"),
            };

            let with_schema: serde_json::Value = serde_json::from_str(&with_schema.body).unwrap();
            let with_schema_loaded: serde_json::Value =
                serde_json::from_str(&with_schema_loaded.body).unwrap();
            assert_eq!(
                with_schema, baseline,
                "the schema graph changed a default-graph answer: {query}"
            );
            assert_eq!(
                with_schema_loaded, baseline,
                "with the schema graph actually loaded, a default-graph answer changed: {forced}"
            );
        }
    }

    /// A query mixing schema and instance patterns must answer, not error: the
    /// instance half from the default graph, the schema half from the named
    /// one. Tier 1 does not teach the SQL planner about schema patterns, so
    /// this shape is expected to be finished by the engine leg — which is the
    /// leg under test here.
    #[test]
    fn a_mixed_schema_and_instance_query_answers() {
        let sv = schema();
        let instances = instances(&sv);
        let answer = run(
            &sv,
            &instances,
            &format!(
                "PREFIX asset360: <https://data.infrabel.be/asset360/> \
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \
                 SELECT ?name ?code WHERE {{ \
                   ?s a asset360:Signal ; asset360:name ?name ; asset360:signalType ?type . \
                   OPTIONAL {{ GRAPH <{SCHEMA_GRAPH_IRI}> {{ ?type rdfs:label ?code }} }} \
                 }} ORDER BY ?name"
            ),
        );
        let bindings = answer["results"]["bindings"].as_array().unwrap();
        assert_eq!(bindings.len(), 2, "{answer}");
        assert_eq!(bindings[0]["name"]["value"], "BX517");
        assert_eq!(
            bindings[0]["code"]["value"], "GSA",
            "the enum IRI should have resolved to its code: {answer}"
        );
        assert_eq!(bindings[1]["name"]["value"], "BX518");
        // `LOCAL_ONLY` declares no meaning, and is described all the same: its
        // IRI is minted, so the OPTIONAL binds. This used to be the hole the
        // whole join fell into — one enum answering labels for the values a
        // schema happened to map and silence for the rest.
        assert_eq!(
            bindings[1]["code"]["value"], "LOCAL_ONLY",
            "a value without a meaning is described too: {answer}"
        );
    }

    /// A schema pattern in the *default* graph must still find nothing. This is
    /// what keeps the two execution routes in agreement, so it is asserted
    /// rather than left implied by the named-graph loading code.
    #[test]
    fn schema_triples_are_invisible_in_the_default_graph() {
        let sv = schema();
        let instances = instances(&sv);
        // The UNION's second branch loads the schema graph and proves it is
        // non-empty; the first branch asks the default graph the same question.
        // Every solution must therefore carry a bound `?g`.
        let answer = run(
            &sv,
            &instances,
            "SELECT ?g ?l WHERE { \
               { ?t <http://www.w3.org/2000/01/rdf-schema#label> ?l } UNION \
               { GRAPH ?g { ?t <http://www.w3.org/2000/01/rdf-schema#label> ?l } } }",
        );
        let bindings = answer["results"]["bindings"].as_array().unwrap();
        assert!(
            !bindings.is_empty(),
            "the schema graph should have supplied labels: {answer}"
        );
        for binding in bindings {
            assert_eq!(
                binding["g"]["value"], SCHEMA_GRAPH_IRI,
                "an rdfs:label was visible outside the schema graph: {answer}"
            );
        }
    }
}

/// GeoSPARQL functions answer over instance data.
///
/// The statement route serves only a query it can answer whole; a geometry
/// filter mixed with anything unpushable falls to this engine. If the engine
/// cannot evaluate `geof:sfIntersects`, the two routes answer *differently*
/// rather than at different speeds.
#[cfg(all(test, feature = "sparql-endpoint"))]
mod geosparql_is_available {
    use linkml_runtime::{LinkMLInstance, load_json_str};
    use linkml_schemaview::identifier::Identifier;
    use linkml_schemaview::schemaview::SchemaView;

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> \
                          PREFIX geo: <http://www.opengis.net/ont/geosparql#> \
                          PREFIX geof: <http://www.opengis.net/def/function/geosparql/> ";

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
  geo:
    prefix_reference: http://www.opengis.net/ont/geosparql#
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
  wktLiteral:
    uri: geo:wktLiteral
    base: str
classes:
  Zone:
    class_uri: asset360:Zone
    attributes:
      asset360_uri:
        identifier: true
      asWKT:
        range: wktLiteral
"#;
        let schema: SchemaDefinition = p2e::deserialize(yml::Deserializer::from_str(yaml)).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        sv
    }

    fn zone(sv: &SchemaView, uri: &str, wkt: &str) -> LinkMLInstance {
        let conv = sv.converter();
        let cv = sv
            .get_class(&Identifier::new("Zone"), &conv)
            .unwrap()
            .unwrap();
        load_json_str(
            &format!(r#"{{"asset360_uri": "{uri}", "asWKT": "{wkt}"}}"#),
            sv,
            &cv,
            &conv,
        )
        .unwrap()
        .into_instance_tolerate_errors()
        .unwrap()
    }

    /// The `asWKT` slot must triplify as a `geo:wktLiteral`, not a plain
    /// `xsd:string`.
    ///
    /// `sf_intersects_selects_by_geometry` proves this only by inference: if
    /// the datatype were wrong, `spargeo`'s `extract_argument` would return
    /// `None`, the filter would be false for every row, and that test would
    /// fail with an empty match set. That failure would look identical to a
    /// real geometry-semantics bug, so pin the datatype directly where the
    /// next reader will look.
    #[test]
    fn as_wkt_triplifies_as_geo_wkt_literal() {
        use linkml_runtime::turtle::{TurtleOptions, turtle_to_string};

        let sv = schema();
        let instance = zone(
            &sv,
            "https://data.infrabel.be/asset360/zone/in",
            "POINT(4.35 50.85)",
        );
        let conv = sv.converter();
        let primary = sv.primary_schema().unwrap();
        let turtle = turtle_to_string(
            &instance,
            &sv,
            &primary,
            &conv,
            TurtleOptions { skolem: false },
        )
        .unwrap();

        // The serialiser may write the datatype either prefixed
        // (`^^geo:wktLiteral`, with a `geo:` prefix bound to the GeoSPARQL
        // namespace) or as a full IRI (`^^<http://www.opengis.net/ont/geosparql#wktLiteral>`).
        // Both spellings name the same datatype, so accept either — a purely
        // cosmetic serialiser change (prefix spacing/ordering, or switching
        // to full IRIs) must not fail this test, only a wrong datatype may.
        let prefixed = turtle.contains("@prefix geo: <http://www.opengis.net/ont/geosparql#>")
            && turtle.contains("^^geo:wktLiteral");
        let full_iri = turtle.contains("^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
        assert!(
            prefixed || full_iri,
            "asWKT must serialise with the geo:wktLiteral datatype (prefixed \
             or as a full IRI) — spargeo silently returns no rows (not an \
             error) for any other datatype, which is what MR 3's pushdown \
             approach depends on. Got:\n{turtle}"
        );
    }

    /// A point inside the box matches, a point outside it does not, and a
    /// linestring crossing the boundary does.
    #[test]
    fn sf_intersects_selects_by_geometry() {
        let sv = schema();
        let inside = zone(
            &sv,
            "https://data.infrabel.be/asset360/zone/in",
            "POINT(4.35 50.85)",
        );
        let outside = zone(
            &sv,
            "https://data.infrabel.be/asset360/zone/out",
            "POINT(9.99 20.0)",
        );
        let crossing = zone(
            &sv,
            "https://data.infrabel.be/asset360/zone/line",
            "LINESTRING(4.1 50.1, 4.9 50.9)",
        );
        let refs = vec![&inside, &outside, &crossing];

        let answer = super::sparql_execute(
            &format!(
                r#"{PREFIX}SELECT ?s WHERE {{
                     ?s a asset360:Zone ; asset360:asWKT ?wkt .
                     FILTER(geof:sfIntersects(?wkt,
                       "POLYGON((4 50, 5 50, 5 51, 4 51, 4 50))"^^geo:wktLiteral))
                   }}"#
            ),
            &refs,
            &sv,
            super::ExecuteLimits::default(),
            None,
        )
        .expect("query executes");

        let parsed: serde_json::Value = serde_json::from_str(&answer.body).unwrap();
        let mut matched: Vec<String> = parsed["results"]["bindings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["s"]["value"].as_str().unwrap().to_owned())
            .collect();
        matched.sort();

        assert_eq!(
            matched,
            vec![
                "https://data.infrabel.be/asset360/zone/in".to_owned(),
                "https://data.infrabel.be/asset360/zone/line".to_owned(),
            ],
            "the inside point and the crossing line intersect the box; the outside point does not"
        );
    }
}

/// The readable spelling of an RSM-mapped slot, through the engine.
///
/// The defect this pins is #447 (pepibru GitLab issue, asset360/consolidator-server):
/// `Track`'s `name` is written into the data under its declared `slot_uri`, an
/// RSM EAID nobody can guess, and the readable `irsm:name` matched nothing at
/// all — a 200 with an empty column, not an error. The resolution itself is
/// unit-tested in [`crate::sparql_alias`]; what is tested here is that the
/// engine leg, the one that matches IRIs against triples, now answers it.
#[cfg(all(test, feature = "sparql-endpoint"))]
mod alias_tests {
    use super::*;
    use linkml_runtime::load_json_str;
    use linkml_schemaview::identifier::Identifier;

    /// The two spellings of `name` on an RSM-mapped class, and the third that
    /// belongs to a different slot entirely.
    const NATIVE: &str = "https://data.infrabel.be/asset360-rsm-subset/name";
    const CANONICAL: &str = "http://rsm.uic.org/RSM12#EAID_080C70AE_7680_4515_B580_0B30E8066364";
    const ASSET360_NAME: &str = "https://data.infrabel.be/asset360/name";

    fn schema() -> SchemaView {
        use linkml_meta::SchemaDefinition;
        use serde_path_to_error as p2e;
        use serde_yml as yml;

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
        slot_uri: RSM:#EAID_080C70AE_7680_4515_B580_0B30E8066364
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

    fn tracks(sv: &SchemaView) -> Vec<LinkMLInstance> {
        let conv = sv.converter();
        let cv = sv
            .get_class(&Identifier::new("Track"), &conv)
            .unwrap()
            .unwrap();
        vec![
            load_json_str(
                r#"{"asset360_uri": "https://data.infrabel.be/asset360/track/1", "name": "L50A"}"#,
                sv,
                &cv,
                &conv,
            )
            .unwrap()
            .into_instance_tolerate_errors()
            .unwrap(),
        ]
    }

    fn names_for(predicate: &str) -> Vec<String> {
        let sv = schema();
        let instances = tracks(&sv);
        let refs: Vec<&LinkMLInstance> = instances.iter().collect();
        let answer = sparql_execute(
            &format!("SELECT ?n WHERE {{ ?t <{predicate}> ?n }}"),
            &refs,
            &sv,
            ExecuteLimits::default(),
            None,
        )
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&answer.body).unwrap();
        parsed["results"]["bindings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["n"]["value"].as_str().unwrap().to_owned())
            .collect()
    }

    #[test]
    fn both_spellings_of_an_rsm_mapped_slot_answer_the_same() {
        assert_eq!(names_for(CANONICAL), vec!["L50A".to_owned()]);
        assert_eq!(
            names_for(NATIVE),
            vec!["L50A".to_owned()],
            "the readable spelling used to answer an empty column"
        );
    }

    /// `asset360:name` is not a spelling of this slot. It is a different slot
    /// — the one `Zone` declares — and the datamodel keeps them apart. Making
    /// it match here would conflate two slots, so it still answers nothing,
    /// and the schema graph is where a client finds out which IRI to write.
    #[test]
    fn an_unrelated_slots_iri_still_matches_nothing() {
        assert!(names_for(ASSET360_NAME).is_empty());
    }
}
