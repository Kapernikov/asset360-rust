//! What a query's `GRAPH` clauses say about routing it.
//!
//! Two questions, both answered by walking an already-parsed SPARQL algebra
//! rather than by guessing at the query text. Both are about *this endpoint's
//! routes* — there is nothing generic here, which is why this sits beside the
//! scoper and not in the upstream LinkML crates:
//!
//! * [`query_reads_named_graphs`] decides whether building the schema graph is
//!   worth anything at all for this request.
//! * [`reads_only_the_schema_graph`] decides whether the scoper's "unscoped"
//!   refusal is the right answer or the wrong one.
//!
//! The schema graph's IRI is a parameter, never a constant: see
//! [`crate::sparql_schema_graph`] for why. `None` means the active datamodel
//! configures no schema graph, in which case no query can be reading it.
//!
//! Both parse with [`crate::sparql_scoper::sparql_parser`], the parser the
//! endpoint's other entry points use. A bare `SparqlParser::new()` here knew
//! none of the pre-registered vocabularies, so `GRAPH <…> { ?c rdfs:label ?l }`
//! — a datamodel-discovery query, written the only way it can be written —
//! did not parse, both functions took their "does not parse" branch, and the
//! query came back refused as *unscoped*: a parse failure reported as a
//! question about scope.

/// Whether a query could observe a named graph at all.
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
    use spargebra::Query;
    use spargebra::algebra::GraphPattern;

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

    let Ok(parsed) = crate::sparql_scoper::sparql_parser().parse_query(query) else {
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
/// `schema_graph_iri` is the graph the active datamodel serves its schema in.
/// `None` — no schema graph configured for this datamodel — makes this always
/// false: there is no schema graph, so no query reads only it, and the unscoped
/// refusal stands.
///
/// A query that does not parse is not schema-only: the real parse error must
/// surface, not be turned into an empty answer.
pub fn reads_only_the_schema_graph(query: &str, schema_graph_iri: Option<&str>) -> bool {
    use spargebra::Query;
    use spargebra::algebra::GraphPattern;
    use spargebra::term::NamedNodePattern;

    let Some(schema_graph_iri) = schema_graph_iri else {
        return false;
    };

    // Same rule the scoper applies: the named schema graph, or a variable that
    // might bind to it. Any other constant graph names something the endpoint
    // does not have, and a query reading only that has no scope and no answer
    // here — which is what the unscoped refusal already says.
    let reads_the_schema_graph = |name: &NamedNodePattern| match name {
        NamedNodePattern::NamedNode(node) => node.as_str() == schema_graph_iri,
        NamedNodePattern::Variable(_) => true,
    };

    /// `(triples seen, triples seen inside the schema graph)`.
    fn walk(
        pattern: &GraphPattern,
        inside: bool,
        total: &mut usize,
        in_graph: &mut usize,
        is_schema_graph: &dyn Fn(&NamedNodePattern) -> bool,
    ) {
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
                inside || is_schema_graph(name),
                total,
                in_graph,
                is_schema_graph,
            ),
            GraphPattern::Join { left, right }
            | GraphPattern::Union { left, right }
            | GraphPattern::Minus { left, right } => {
                walk(left, inside, total, in_graph, is_schema_graph);
                walk(right, inside, total, in_graph, is_schema_graph);
            }
            GraphPattern::LeftJoin { left, right, .. } => {
                walk(left, inside, total, in_graph, is_schema_graph);
                walk(right, inside, total, in_graph, is_schema_graph);
            }
            GraphPattern::Filter { inner, .. }
            | GraphPattern::Extend { inner, .. }
            | GraphPattern::OrderBy { inner, .. }
            | GraphPattern::Project { inner, .. }
            | GraphPattern::Distinct { inner }
            | GraphPattern::Reduced { inner }
            | GraphPattern::Slice { inner, .. }
            | GraphPattern::Group { inner, .. }
            | GraphPattern::Service { inner, .. } => {
                walk(inner, inside, total, in_graph, is_schema_graph)
            }
            GraphPattern::Values { .. } => {}
        }
    }

    let Ok(parsed) = crate::sparql_scoper::sparql_parser().parse_query(query) else {
        return false;
    };
    let pattern = match &parsed {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => pattern,
    };
    let (mut total, mut in_graph) = (0usize, 0usize);
    walk(
        pattern,
        false,
        &mut total,
        &mut in_graph,
        &reads_the_schema_graph,
    );
    total > 0 && total == in_graph
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The graph the asset360 datamodel configures. A test value: production
    /// reads it from `asset360_model/datamodels/asset360.yaml`.
    const SCHEMA_GRAPH: &str = "https://data.infrabel.be/asset360/schema";

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
    fn a_discovery_query_parses_with_the_endpoint_s_prefixes() {
        // The only way to write a schema-graph query is with the standard
        // vocabularies, and this module used to parse with a bare parser that
        // knew none of them. Both functions then took their "does not parse"
        // branch: the schema graph was built for nothing, and the query was
        // refused as *unscoped* — a parse failure reported as a question about
        // scope, on the one query shape the schema graph exists to serve.
        let query = format!(
            "SELECT ?l WHERE {{ GRAPH <{SCHEMA_GRAPH}> {{ ?c a owl:Class ; rdfs:label ?l }} }}"
        );
        assert!(query_reads_named_graphs(&query));
        assert!(reads_only_the_schema_graph(&query, Some(SCHEMA_GRAPH)));
    }

    #[test]
    fn schema_only_queries_are_told_apart_from_mixed_ones() {
        let schema = format!("SELECT ?c WHERE {{ GRAPH <{SCHEMA_GRAPH}> {{ ?c ?p ?o }} }}");
        assert!(reads_only_the_schema_graph(&schema, Some(SCHEMA_GRAPH)));
        // A variable graph might bind to the schema graph.
        assert!(reads_only_the_schema_graph(
            "SELECT ?c WHERE { GRAPH ?g { ?c ?p ?o } }",
            Some(SCHEMA_GRAPH)
        ));
        // Some other named graph: the endpoint has none, and the unscoped
        // refusal already says so.
        assert!(!reads_only_the_schema_graph(
            "SELECT ?c WHERE { GRAPH <urn:g> { ?c ?p ?o } }",
            Some(SCHEMA_GRAPH)
        ));
        let mixed =
            format!("SELECT ?c WHERE {{ ?s ?p ?o . GRAPH <{SCHEMA_GRAPH}> {{ ?c ?q ?r }} }}");
        assert!(!reads_only_the_schema_graph(&mixed, Some(SCHEMA_GRAPH)));
        assert!(!reads_only_the_schema_graph(
            "SELECT ?s WHERE { ?s ?p ?o }",
            Some(SCHEMA_GRAPH)
        ));
        // No triple pattern at all is not a schema query.
        assert!(!reads_only_the_schema_graph(
            "SELECT ?x WHERE { BIND(1 AS ?x) }",
            Some(SCHEMA_GRAPH)
        ));
        assert!(!reads_only_the_schema_graph(
            "SELECT ?s WHERE {",
            Some(SCHEMA_GRAPH)
        ));
    }

    /// The IRI is the caller's, so the same query is schema-only under one
    /// datamodel and not under another. This is the whole point of it not
    /// being a constant.
    #[test]
    fn the_schema_graph_iri_is_the_callers() {
        let rinf = "http://data.europa.eu/949/schema";
        let asset360_query = format!("SELECT ?c WHERE {{ GRAPH <{SCHEMA_GRAPH}> {{ ?c ?p ?o }} }}");
        assert!(reads_only_the_schema_graph(
            &asset360_query,
            Some(SCHEMA_GRAPH)
        ));
        assert!(!reads_only_the_schema_graph(&asset360_query, Some(rinf)));

        let rinf_query = format!("SELECT ?c WHERE {{ GRAPH <{rinf}> {{ ?c ?p ?o }} }}");
        assert!(reads_only_the_schema_graph(&rinf_query, Some(rinf)));
        assert!(!reads_only_the_schema_graph(
            &rinf_query,
            Some(SCHEMA_GRAPH)
        ));
    }

    /// No schema graph configured: nothing reads only it, not even a query
    /// naming a variable graph.
    #[test]
    fn without_a_configured_schema_graph_nothing_is_schema_only() {
        let schema = format!("SELECT ?c WHERE {{ GRAPH <{SCHEMA_GRAPH}> {{ ?c ?p ?o }} }}");
        assert!(!reads_only_the_schema_graph(&schema, None));
        assert!(!reads_only_the_schema_graph(
            "SELECT ?c WHERE { GRAPH ?g { ?c ?p ?o } }",
            None
        ));
    }
}
