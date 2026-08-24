//! SPARQL query planning for the virtual SPARQL endpoint.
//!
//! Analyses a SPARQL query and produces a [`QueryPlan`] — a structured
//! representation of what to fetch from PostgreSQL and how to join it.
//!
//! The plan decomposes the query into **stars** (groups of triple patterns
//! sharing one subject variable, each bound to one `rdf:type`). Stars
//! connected by reference properties produce **join edges** that Python
//! translates to SQL JOINs. Stars without join edges are fetched
//! independently. Patterns that can't be decomposed (property paths,
//! complex FILTER expressions) fall back to Oxigraph.
//!
//! The full SPARQL query is always executed in Oxigraph against the loaded
//! data. The plan only determines *what* to load efficiently.

use std::collections::{HashMap, HashSet};

use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::{Query, SparqlParser};

use linkml_schemaview::schemaview::SchemaView;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A structured plan for fetching data from PostgreSQL.
///
/// Shaped as an algebra tree rooted at [`PlanNode`], so future SPARQL
/// constructs (`UNION`, `MINUS`, `NOT EXISTS`, …) can be added as new
/// node variants without breaking the existing `Bgp` / `LeftJoin`
/// consumers. Today exactly two node kinds are emitted.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// Root of the algebra tree.
    pub root: PlanNode,

    /// SQL LIMIT — only set for single-star, zero-join, zero-OPTIONAL
    /// queries with a top-level SPARQL LIMIT.
    pub sql_limit: Option<usize>,

    /// Variables reached by walking *into* a star's nested structures, as
    /// `variable -> (star, path of slots)`.
    ///
    /// `?s :location ?l . ?l :longitude ?v` binds `?v` two slots down from
    /// `?s`, which no star can describe: `?l` has no `rdf:type` and is not an
    /// object of its own, it is part of `?s`'s JSON. A consumer reading
    /// `object_data->'location'->>'longitude'` needs the path, and the star
    /// decomposition is already walking these triples to find join edges.
    ///
    /// Only *scalar* leaves appear. An intermediate variable stands for the
    /// nested structure itself, which serialises as a blank node — nothing a
    /// consumer can reproduce — so it is traversable but not bindable.
    pub path_bindings: HashMap<String, PathBinding>,

    /// Why this plan is *not* a complete representation of the query, if it
    /// isn't.
    ///
    /// Every extraction step here is deliberately lossy in the safe direction:
    /// a constraint that cannot be expressed is dropped, the fetch widens, and
    /// oxigraph re-applies the real query to what came back. That makes a
    /// dropped constraint invisible — which is fine for a prefetch and fatal
    /// for anything treating the plan as the answer, where the same loss is a
    /// plausible wrong number with no error.
    ///
    /// `false` means something in the query is not in this plan: a `FILTER`
    /// this cannot express, a triple whose subject is not a scoped class, a
    /// sub-`SELECT`, a `FILTER` inside `OPTIONAL`. A consumer that needs an
    /// exact plan — the aggregate pushdown — must refuse; a consumer that only
    /// needs a superset may ignore it.
    ///
    /// Also gates `sql_limit`: a LIMIT is only pushable when the fetch it
    /// bounds is the real row set. With a dropped `FILTER(REGEX(...))`, LIMIT
    /// 10 fetches ten arbitrary rows and oxigraph filters them down to a
    /// handful, where the query asked for ten matches.
    ///
    /// Recorded *where the loss happens* — at each point that drops part of the
    /// query — rather than reconstructed afterwards from what survived. An
    /// after-the-fact check can only look at what it knows to look for, and the
    /// first version of it missed four drop sites: a variable predicate, a
    /// predicate matching no slot, an inline constant inside `OPTIONAL`, and
    /// `VALUES` on an unknown variable. Each produced a plan that claimed to be
    /// exact while counting every row of the class.
    pub inexact: Option<Inexact>,
}

/// What the planner had to leave out of a plan.
///
/// One variant per drop site, so a refusal can say which one fired: a generic
/// "something was dropped" forces a hint listing every possible rewrite, most
/// of which do not apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inexact {
    /// A `FILTER` expression that cannot be expressed as a pushable condition:
    /// `!=`, `||`, `!`, `REGEX`, `BOUND`, a comparison between two variables.
    FilterExpression,
    /// A `FILTER` inside an `OPTIONAL`, including the condition spargebra lifts
    /// into the `LeftJoin` itself. Pushing one would drop the rows the join
    /// exists to preserve.
    FilterInOptional,
    /// A triple whose predicate is a variable, so which slot it reads is not
    /// known until the query runs.
    VariablePredicate,
    /// A triple whose predicate matches no slot in the schema, so its
    /// constraint is invisible to the plan.
    UnknownPredicate,
    /// A subject that is neither a variable nor an IRI, so it cannot be a star.
    UnscopedSubject,
    /// A subject with no resolvable class: its triples are not represented at
    /// all, and a count over the remaining stars is one per group.
    UntypedSubject,
    /// A constant object inside an `OPTIONAL`. Pushing it would filter out rows
    /// the join preserves, so it is left to oxigraph.
    ConstantInOptional,
    /// A `VALUES` block over a variable the plan does not bind.
    UnboundValues,
    /// A sub-`SELECT`, which has its own projection and modifiers.
    Subquery,
    /// A `GRAPH` block. The plan reads one relation — the default graph — so a
    /// named-graph pattern would be answered from the wrong graph.
    NamedGraph,
    /// A `SERVICE` block. The data lives on another endpoint; answering it from
    /// local SQL answers a different question entirely.
    RemoteService,
    /// One variable bound by two different slots, which is an equality between
    /// them that the plan does not carry.
    ImpliedEquality,
}

impl Inexact {
    /// Stable string form, for an error payload or a lint code.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FilterExpression => "filter_expression",
            Self::FilterInOptional => "filter_in_optional",
            Self::VariablePredicate => "variable_predicate",
            Self::UnknownPredicate => "unknown_predicate",
            Self::UnscopedSubject => "unscoped_subject",
            Self::UntypedSubject => "untyped_subject",
            Self::ConstantInOptional => "constant_in_optional",
            Self::UnboundValues => "unbound_values",
            Self::Subquery => "subquery",
            Self::NamedGraph => "named_graph",
            Self::RemoteService => "remote_service",
            Self::ImpliedEquality => "implied_equality",
        }
    }

    /// What was left out, in terms of the query.
    pub fn detail(&self) -> &'static str {
        match self {
            Self::FilterExpression => {
                "a FILTER this cannot turn into a SQL condition was left for the \
                 SPARQL engine, so the plan describes a weaker constraint than \
                 the query"
            }
            Self::FilterInOptional => {
                "a FILTER inside an OPTIONAL cannot be applied to the fetch \
                 without dropping the rows the OPTIONAL preserves"
            }
            Self::VariablePredicate => {
                "a triple has a variable predicate, so which slot it reads is \
                 not known before the query runs"
            }
            Self::UnknownPredicate => {
                "a triple uses a predicate that matches no slot in the schema, \
                 so its constraint is not in the plan"
            }
            Self::UnscopedSubject => "a triple has a subject that cannot be scoped",
            Self::UntypedSubject => {
                "a subject has no resolvable rdf:type, so its triples are not \
                 represented in the plan at all"
            }
            Self::ConstantInOptional => {
                "a constant value inside an OPTIONAL cannot be applied to the \
                 fetch without dropping rows the OPTIONAL preserves"
            }
            Self::UnboundValues => "a VALUES block constrains a variable the plan does not bind",
            Self::Subquery => {
                "a sub-SELECT has its own projection and limits, which this plan \
                 does not carry"
            }
            Self::NamedGraph => {
                "a GRAPH block names a graph, and the plan reads only the \
                 default one"
            }
            Self::RemoteService => {
                "a SERVICE block reads another endpoint, which local SQL cannot \
                 answer"
            }
            Self::ImpliedEquality => {
                "one variable is bound by two different slots, which requires \
                 those two values to be equal — a constraint the plan does not \
                 carry"
            }
        }
    }

    /// The rewrite that makes the query expressible — one per cause, rather
    /// than a list of four where three never apply.
    pub fn instead(&self) -> &'static str {
        match self {
            Self::FilterExpression => {
                "Constrain values with `=`, `IN`, or a `<` / `>` comparison \
                 against a literal. `!=`, `||`, `!`, REGEX and BOUND cannot be \
                 pushed to the database."
            }
            Self::FilterInOptional | Self::ConstantInOptional => {
                "Move the condition out of the OPTIONAL block, or drop the \
                 OPTIONAL if the value is required after all."
            }
            Self::VariablePredicate => {
                "Name the predicate, e.g. `?s asset360:status ?v` rather than \
                 `?s ?p ?v`."
            }
            Self::UnknownPredicate => {
                "Use a predicate the schema defines; check the spelling and the \
                 prefix."
            }
            Self::UnscopedSubject | Self::UntypedSubject => {
                "Give every subject an rdf:type, e.g. `?s a asset360:Signal`, so \
                 the class it belongs to is known."
            }
            Self::UnboundValues => {
                "Bind the variable with a triple pattern before constraining it \
                 with VALUES."
            }
            Self::Subquery => "Ask the sub-query as a separate question.",
            Self::NamedGraph => {
                "Query the default graph: drop the GRAPH wrapper, or ask the \
                 named graph through an endpoint that serves it."
            }
            Self::RemoteService => "Ask the remote endpoint directly.",
            Self::ImpliedEquality => {
                "Use a different variable for each slot, and compare them with \
                 a FILTER if the equality is what you meant."
            }
        }
    }
}

/// Where a nested variable's value lives, relative to a star.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathBinding {
    /// The star this path starts from.
    pub star_var: String,
    /// Slots to follow from the object root. Always at least two — a
    /// single-slot binding is already in [`Star::slot_variables`].
    pub slot_path: Vec<String>,
}

/// How deep to follow nested structures.
///
/// A schema may be cyclic (a class with a slot of its own range), so the walk
/// needs a bound rather than a visited set: revisiting a class is legitimate
/// (`?a :child ?b . ?b :child ?c`), it is unbounded *depth* that has to stop.
/// Real instances are shallow; four is well past anything in the data.
const MAX_PATH_DEPTH: usize = 4;

/// One node in the query plan algebra tree.
///
/// Only two variants are produced today. Future features will add more
/// (`Union`, `Minus`, `NotExists`, `Path`) — each variant is added as a
/// new enum case so Python consumers that don't recognise it can cleanly
/// reject the query rather than silently miscomputing.
#[derive(Debug, Clone)]
pub enum PlanNode {
    /// A Basic Graph Pattern: a group of stars joined by inner joins.
    /// This is the single "mandatory" block of triples in the query.
    Bgp {
        stars: Vec<Star>,
        joins: Vec<JoinEdge>,
    },
    /// SPARQL `OPTIONAL { ... }` — left-join semantics. The `left` side
    /// is the mandatory pattern; the `right` side is the optional block.
    /// Oxigraph evaluates the original SPARQL query against the fetched
    /// instances, so the only job of this node is to keep the SQL
    /// prefetch from filtering out mandatory rows.
    LeftJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
    },
}

impl PlanNode {
    /// Walk the tree (pre-order) and collect every star into one flat
    /// list. Used by Python's SQL builder and by legacy accessors.
    pub fn all_stars(&self) -> Vec<&Star> {
        let mut out = Vec::new();
        self.visit_stars(&mut out);
        out
    }

    /// Walk the tree (pre-order) and collect every join edge into one
    /// flat list.
    pub fn all_joins(&self) -> Vec<&JoinEdge> {
        let mut out = Vec::new();
        self.visit_joins(&mut out);
        out
    }

    fn visit_stars<'a>(&'a self, out: &mut Vec<&'a Star>) {
        match self {
            PlanNode::Bgp { stars, .. } => {
                for s in stars {
                    out.push(s);
                }
            }
            PlanNode::LeftJoin { left, right } => {
                left.visit_stars(out);
                right.visit_stars(out);
            }
        }
    }

    fn visit_joins<'a>(&'a self, out: &mut Vec<&'a JoinEdge>) {
        match self {
            PlanNode::Bgp { joins, .. } => {
                for j in joins {
                    out.push(j);
                }
            }
            PlanNode::LeftJoin { left, right } => {
                left.visit_joins(out);
                right.visit_joins(out);
            }
        }
    }
}

/// A group of triple patterns sharing the same subject variable,
/// bound to one `rdf:type` (one LinkML class).
///
/// Named after the SPARQL algebra concept of "star-shaped sub-pattern."
///
/// Python translates each star to SQL conditions:
/// - `class_uri` → `WHERE asset_type = '<full-iri>'`
/// - `identifier_values` → `WHERE asset360_uri IN (...)`  (indexed column)
/// - `required_fields` → `WHERE object_data ? 'fieldName'`
/// - `optional_fields` → fetched without existence check
/// - `filters` → `WHERE object_data->>'field' = 'value'`
#[derive(Debug, Clone)]
pub struct Star {
    /// The SPARQL variable name (without `?`), e.g. `"complex"`.
    pub variable: String,

    /// The full RDF class IRI, e.g.
    /// `"https://data.infrabel.be/asset360/TunnelComplex"`. Captured
    /// verbatim from the `?s a <iri>` triple — no stripping to a local
    /// name. Downstream callers compare this against the indexed
    /// `asset_type` column with `=`, not `LIKE`.
    pub class_uri: String,

    /// Values bound to this class's LinkML identifier slot (the slot
    /// marked `identifier: true`) — schema-resolved, never assumed to
    /// be named `"id"`. Collected from inline literals, inline IRIs,
    /// `FILTER(?id = "v")`, `FILTER(?id IN (...))`, and `VALUES ?id { ... }`.
    /// Empty when the query has no identifier predicate bound.
    ///
    /// The identifier slot does NOT appear in `filters` or
    /// `required_fields` — the existence check is structurally always
    /// true (every row has an identifier by construction), and value
    /// pushdown happens against the indexed `asset360_uri` column
    /// rather than the JSONB payload.
    pub identifier_values: Vec<String>,

    /// Slots that MUST be present on the object. Python emits
    /// `WHERE object_data ? 'fieldName'` for each.
    pub required_fields: Vec<String>,

    /// Slots that MAY be present (appear only inside an `OPTIONAL`
    /// block relative to this star). Python does NOT emit a
    /// `WHERE object_data ? 'fieldName'` check for these, but they
    /// still flow through to oxigraph via the JSONB payload.
    pub optional_fields: Vec<String>,

    /// True if this star itself only appears inside one or more
    /// `OPTIONAL` blocks (its `rdf:type` was declared at a non-zero
    /// OPTIONAL depth). Python wraps its `WHERE` conditions in
    /// `(... OR <alias>.asset360_uri IS NULL)` so that a missing
    /// LEFT JOIN row doesn't get filtered out.
    pub is_optional: bool,

    /// Value-level filter conditions per slot, pushable to SQL.
    /// From `FILTER(?var = "literal")` and `VALUES ?var { ... }`
    /// where `?var` is bound to a known slot in this star.
    ///
    /// Does NOT include the identifier slot — see `identifier_values`.
    pub filters: HashMap<String, Vec<FilterCondition>>,

    /// Which SPARQL variable each slot binds to: `?s :hasName ?name`
    /// contributes `"name" -> "name"` (slot name → variable name).
    ///
    /// The star decomposition has to work this out anyway to detect join
    /// edges; exposing it lets a consumer answer "which column does
    /// `?name` come from" without re-walking the query — the question
    /// both the aggregate pushdown (which slot is this group key?) and a
    /// column projection have to ask. Only object *variables* appear
    /// here: a constant object is a filter, not a binding.
    pub slot_variables: HashMap<String, String>,
}

/// A join between two stars, pushable to a SQL JOIN.
///
/// The `right` star has a slot (`right_slot`) whose value is the
/// `asset360_uri` of the `left` star's subject. Python translates to:
///
/// ```sql
/// JOIN goldenrecords t1
///   ON t1.object_data->>'right_slot' = t0.asset360_uri
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinEdge {
    /// Variable of the referenced star (the join target).
    pub left: String,

    /// Variable of the star holding the foreign key.
    pub right: String,

    /// The slot on the right star whose value equals left's `asset360_uri`.
    /// E.g. `"belongsToTunnelComplex"`.
    pub right_slot: String,

    /// Join type.
    pub join_type: JoinType,
}

/// Join type for a [`JoinEdge`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// SQL INNER JOIN — both sides must have matching rows.
    Inner,
    /// SQL LEFT JOIN — left side always present, right may be NULL.
    /// Future: used for SPARQL OPTIONAL patterns.
    Left,
}

/// A filter condition extracted from the SPARQL query, pushable to SQL.
#[derive(Debug, Clone)]
pub enum FilterCondition {
    /// Equality: `FILTER(?var = "value")` → `WHERE object_data->>'field' = 'value'`
    Eq(String),
    /// Set membership: `VALUES ?var { "a" "b" }` → `WHERE object_data->>'field' IN ('a', 'b')`
    In(Vec<String>),
    /// An ordering comparison: `FILTER(?len > 10)`.
    ///
    /// The consumer must compare the way SPARQL does, which is not how text
    /// compares: a numeric slot casts (so 9 < 10), and a string slot needs
    /// codepoint collation. The slot's term descriptor says which, so this
    /// carries only the operator and the value.
    Cmp { op: CmpOp, value: String },
}

/// Ordering operators liftable from a `FILTER` into SQL.
///
/// Deliberately not `!=`: SPARQL's inequality is false for an *unbound*
/// variable, where SQL's `<>` on NULL is unknown and would drop rows the
/// query keeps. Equality is already covered by [`FilterCondition::Eq`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Gt,
    Gte,
    Lt,
    Lte,
}

impl CmpOp {
    /// The SQL operator. Safe to inline: it comes from this closed set, never
    /// from the request.
    pub fn as_sql(&self) -> &'static str {
        match self {
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::Lt => "<",
            Self::Lte => "<=",
        }
    }

    /// Stable string form for the Python boundary.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Gt => "gt",
            Self::Gte => "gte",
            Self::Lt => "lt",
            Self::Lte => "lte",
        }
    }
}

/// Errors from query planning.
#[derive(Debug)]
pub enum ScopeError {
    /// The SPARQL query could not be parsed (syntax error).
    ParseError(String),
    /// The query has no `rdf:type` constraint and cannot be scoped.
    Unscoped(String),
    /// The input is a SPARQL Update (INSERT/DELETE), not supported.
    UpdateRejected,
    /// The query uses a SPARQL construct the scoper recognises but does
    /// not yet support (`UNION`, `MINUS`, property paths, disconnected
    /// `OPTIONAL`, `NOT EXISTS`, …). Reject with a clear message rather
    /// than silently returning wrong results.
    UnsupportedConstruct(String),
}

impl std::fmt::Display for ScopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScopeError::ParseError(msg) => write!(f, "SPARQL parse error: {msg}"),
            ScopeError::Unscoped(msg) => write!(f, "Query is unscoped: {msg}"),
            ScopeError::UpdateRejected => {
                write!(
                    f,
                    "SPARQL Update (INSERT/DELETE) is not supported. This endpoint is read-only."
                )
            }
            ScopeError::UnsupportedConstruct(msg) => {
                write!(f, "unsupported_construct: {msg}")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// Analyse a SPARQL query and produce a [`QueryPlan`].
///
/// Parses the query via `spargebra`, decomposes the BGP into stars,
/// detects join edges between stars, and collects filter conditions.
///
/// # Errors
///
/// - [`ScopeError::ParseError`] — invalid SPARQL syntax.
/// - [`ScopeError::Unscoped`] — no `rdf:type` or URI constraints.
/// - [`ScopeError::UpdateRejected`] — input is a SPARQL Update.
pub fn sparql_scope(query_str: &str, schema_view: &SchemaView) -> Result<QueryPlan, ScopeError> {
    let query = parse_query(query_str)?;
    scope_parsed(&query, schema_view)
}

/// The parser every entry point must use.
///
/// It preloads the prefixes a caller may leave implicit, which makes it part of
/// the endpoint's contract rather than a convenience: a query that omits
/// `PREFIX asset360:` parses here and nowhere else. Two entry points with two
/// parsers would accept two different languages — one would scope a query the
/// other rejects as a syntax error.
pub fn sparql_parser() -> SparqlParser {
    SparqlParser::new()
        .with_prefix("asset360", "https://data.infrabel.be/asset360/")
        .expect("hardcoded prefix")
        .with_prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        .expect("hardcoded prefix")
        .with_prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
        .expect("hardcoded prefix")
        .with_prefix("xsd", "http://www.w3.org/2001/XMLSchema#")
        .expect("hardcoded prefix")
}

/// Parse a query, rejecting SPARQL Update.
pub fn parse_query(query_str: &str) -> Result<Query, ScopeError> {
    // An Update parses as an Update and not as a Query, so this check has to
    // precede the query parse to give the specific error rather than a syntax
    // one.
    if sparql_parser().parse_update(query_str).is_ok() {
        return Err(ScopeError::UpdateRejected);
    }

    sparql_parser()
        .parse_query(query_str)
        .map_err(|e| ScopeError::ParseError(e.to_string()))
}

/// Plan an already-parsed query.
///
/// Separate from [`sparql_scope`] so a caller that needs the parsed algebra for
/// its own analysis — the aggregate pushdown does — can parse once and share
/// the result, instead of parsing the same string again with a parser that
/// might not match.
pub fn scope_parsed(query: &Query, schema_view: &SchemaView) -> Result<QueryPlan, ScopeError> {
    let pattern = match query {
        Query::Select { pattern, .. } => pattern,
        Query::Construct { pattern, .. } => pattern,
        Query::Describe { pattern, .. } => pattern,
        Query::Ask { pattern, .. } => pattern,
    };

    // Phase 0: Depth-tag every BGP triple, rejecting unsupported
    // constructs along the way (UNION, MINUS, property paths).
    let mut triples_with_depth: Vec<(&TriplePattern, usize)> = Vec::new();
    tag_triples_by_depth(pattern, 0, &mut triples_with_depth)?;

    // Anything dropped along the way is recorded here, at the point it is
    // dropped. The first cause wins: one actionable reason beats a list.
    let mut inexact: Option<Inexact> = None;
    // Subject variables whose class could not be resolved. Cleared below by
    // whichever of them the path walk explains.
    let mut unresolved_subjects: HashSet<String> = HashSet::new();
    let mut drop = |cause: Inexact| {
        if inexact.is_none() {
            inexact = Some(cause);
        }
    };

    // Phase 1: Build stars — group triples by subject variable,
    // tracking the minimum OPTIONAL depth at which each slot and each
    // star itself was introduced.
    let mut star_map: HashMap<String, StarBuilder> = HashMap::new();
    // Synthetic, SQL-safe variable key per distinct constant-IRI subject,
    // assigned on first encounter so the same IRI maps to the same star.
    let mut const_subject_keys: HashMap<String, String> = HashMap::new();

    for (tp, depth) in &triples_with_depth {
        // A triple subject is either a query variable or a constant IRI.
        // Constant-IRI subjects become identifier-scoped stars (keyed by a
        // synthetic variable name; the IRI itself is the identifier value).
        // Literal subjects can't occur (the SPARQL parser rejects them);
        // blank-node subjects act as anonymous variables and are left to
        // oxigraph — both fall through to the skip arm.
        let (subj_var, const_iri) = match &tp.subject {
            TermPattern::Variable(v) => (v.as_str().to_owned(), None),
            TermPattern::NamedNode(nn) => {
                let iri = nn.as_str().to_owned();
                let next = const_subject_keys.len();
                let key = const_subject_keys
                    .entry(iri.clone())
                    .or_insert_with(|| format!("_const_subject_{next}"))
                    .clone();
                (key, Some(iri))
            }
            _ => {
                drop(Inexact::UnscopedSubject);
                continue;
            }
        };

        let pred_iri = match &tp.predicate {
            NamedNodePattern::NamedNode(nn) => nn.as_str(),
            _ => {
                // `?s ?p ?o`: which slot this reads is unknown until the query
                // runs, so the triple constrains nothing here.
                drop(Inexact::VariablePredicate);
                continue;
            }
        };

        let builder = star_map
            .entry(subj_var.clone())
            .or_insert_with(|| StarBuilder {
                variable: subj_var,
                const_iri: const_iri.clone(),
                type_iri: None,
                type_depth: usize::MAX,
                slot_depth: HashMap::new(),
                object_variables: HashMap::new(),
                inline_filters: HashMap::new(),
            });

        if pred_iri == RDF_TYPE {
            if let TermPattern::NamedNode(nn) = &tp.object
                && *depth < builder.type_depth
            {
                builder.type_iri = Some(nn.as_str().to_owned());
                builder.type_depth = *depth;
            }
        } else if let Ok(Some(slot_view)) = schema_view.get_slot_by_uri(pred_iri) {
            // Handled below. The `else` after this branch is the drop site for
            // a predicate the schema does not know.
            let slot_name = slot_view.name.clone();
            let current = builder
                .slot_depth
                .get(&slot_name)
                .copied()
                .unwrap_or(usize::MAX);
            builder
                .slot_depth
                .insert(slot_name.clone(), current.min(*depth));
            match &tp.object {
                TermPattern::Variable(v) => {
                    builder
                        .object_variables
                        .insert(slot_name, v.as_str().to_owned());
                }
                // Inline NamedNode constant: `?s :foo <uri>` →
                // pushable equality filter `object_data->>'foo' = '<uri>'`.
                // Only at depth 0 — inside an OPTIONAL we leave it to
                // oxigraph to avoid breaking LEFT JOIN row preservation.
                TermPattern::NamedNode(nn) if *depth == 0 => {
                    builder
                        .inline_filters
                        .entry(slot_name)
                        .or_default()
                        .push(FilterCondition::Eq(nn.as_str().to_owned()));
                }
                // Inline literal constant: `?s :foo "bar"`.
                TermPattern::Literal(lit) if *depth == 0 => {
                    builder
                        .inline_filters
                        .entry(slot_name)
                        .or_default()
                        .push(FilterCondition::Eq(lit.value().to_owned()));
                }
                // A constant object inside an OPTIONAL: pushing it would
                // filter out rows the LEFT JOIN preserves, so it is left to
                // oxigraph — and the plan no longer says everything the query
                // does.
                TermPattern::NamedNode(_) | TermPattern::Literal(_) => {
                    drop(Inexact::ConstantInOptional);
                }
                _ => {}
            }
        } else {
            // A predicate that matches no slot: its constraint is invisible to
            // the plan, so a consumer reading the plan as exact would count
            // rows the query excludes.
            drop(Inexact::UnknownPredicate);
        }
    }

    // Resolve type IRIs to class names, build Star structs with
    // required / optional field split.
    let mut stars: Vec<Star> = Vec::new();
    let mut var_to_class: HashMap<String, String> = HashMap::new();
    // Track the min OPTIONAL depth at which each star first appears.
    let mut star_depths: HashMap<String, usize> = HashMap::new();
    // Track the identifier slot name (schema-resolved, `identifier: true`)
    // per star variable — consumed by the Phase 3 filter merge below
    // so identifier-slot values land in `identifier_values`, not `filters`.
    let mut var_to_identifier_slot: HashMap<String, String> = HashMap::new();

    for builder in star_map.values() {
        // Resolve the class (and its identifier slot). A variable subject we
        // can't scope yields `None` and is skipped (oxigraph handles it). A
        // constant-IRI subject we can't scope yields `Err` and rejects the
        // whole query — never a silent drop that returns wrong data.
        let (class_uri, identifier_slot_name) = match resolve_star_class(builder, schema_view)? {
            Some(resolved) => resolved,
            None => {
                // A variable subject whose class cannot be resolved. Two very
                // different things look like this, and only Phase 6 can tell
                // them apart: a step inside another star's nested structure
                // (`?s :location ?loc . ?loc :longitude ?v`), which the plan
                // *does* represent as a path, and a subject nothing accounts
                // for (`?sig :locatedOnTrack ?t` with ?sig untyped), whose
                // triples vanish and leave a count of one per group.
                //
                // So record the name rather than the verdict, and let the path
                // walk clear the ones it explains.
                unresolved_subjects.insert(builder.variable.clone());
                continue;
            }
        };
        let star_is_optional = builder.type_depth > 0;
        let mut required_fields: Vec<String> = Vec::new();
        let mut optional_fields: Vec<String> = Vec::new();
        for (slot, depth) in &builder.slot_depth {
            if !star_is_optional && *depth == 0 {
                required_fields.push(slot.clone());
            } else {
                optional_fields.push(slot.clone());
            }
        }
        required_fields.sort();
        required_fields.dedup();
        optional_fields.sort();
        optional_fields.dedup();

        // Hoist inline-constant values on the identifier slot into
        // identifier_values (Phase 1 source). FILTER/VALUES sources
        // are hoisted in the Phase 3 merge below. The identifier slot
        // itself is stripped from required_fields / optional_fields:
        // every row has an identifier by construction, so a JSONB
        // existence check would be pointless (and value pushdown
        // happens against the indexed `asset360_uri` column, not the
        // JSONB payload).
        let mut identifier_values: Vec<String> = Vec::new();
        // A constant-IRI subject is identified by its own URI, regardless of
        // whether the class declares a named identifier slot.
        if let Some(iri) = &builder.const_iri {
            identifier_values.push(iri.clone());
        }
        let mut inline_filters = builder.inline_filters.clone();
        if let Some(id_name) = identifier_slot_name.as_deref() {
            if let Some(conds) = inline_filters.remove(id_name) {
                // Equality and set membership become an `asset360_uri` lookup
                // against the indexed column. An ordering comparison cannot —
                // there is no finite value list — so it stays a filter and the
                // renderer targets the same column with the operator.
                let mut kept: Vec<FilterCondition> = Vec::new();
                for cond in conds {
                    match cond {
                        FilterCondition::Eq(v) => identifier_values.push(v),
                        FilterCondition::In(vs) => identifier_values.extend(vs),
                        cmp @ FilterCondition::Cmp { .. } => kept.push(cmp),
                    }
                }
                if !kept.is_empty() {
                    inline_filters.insert(id_name.to_owned(), kept);
                }
            }
            required_fields.retain(|s| s != id_name);
            optional_fields.retain(|s| s != id_name);
        }

        var_to_class.insert(builder.variable.clone(), class_uri.clone());
        star_depths.insert(builder.variable.clone(), builder.type_depth);
        if let Some(id_name) = identifier_slot_name.clone() {
            var_to_identifier_slot.insert(builder.variable.clone(), id_name);
        }

        stars.push(Star {
            variable: builder.variable.clone(),
            class_uri,
            identifier_values,
            required_fields,
            optional_fields,
            is_optional: star_is_optional,
            // Inline-constant filters from Phase 1 (e.g. `?s :foo <uri>`),
            // minus any entries on the identifier slot that were hoisted
            // above. FILTER(...)/VALUES filters from Phase 3 are merged
            // in below.
            filters: inline_filters,
            slot_variables: builder.object_variables.clone(),
        });
    }

    if stars.is_empty() {
        return Err(ScopeError::Unscoped(
            "Add a triple pattern like '?s rdf:type asset360:Signal' to scope the query."
                .to_owned(),
        ));
    }

    // Sort stars deterministically: mandatory ones first (so the SQL
    // builder picks a mandatory star as the FROM table), then by
    // variable name.
    stars.sort_by(|a, b| {
        a.is_optional
            .cmp(&b.is_optional)
            .then_with(|| a.variable.cmp(&b.variable))
    });

    // Phase 2: Detect join edges. A join is `Left` if either endpoint
    // only appears inside an OPTIONAL block, OR the slot itself was
    // first mentioned inside an OPTIONAL block.
    //
    // Iterate `star_map` in a deterministic order (sorted by subject
    // variable name) so the resulting `joins` vector is reproducible
    // across runs. The Python SQL builder is order-tolerant, but
    // determinism still matters for tests, debugging and SQL plan
    // caching.
    let mut joins: Vec<JoinEdge> = Vec::new();
    let mut sorted_builders: Vec<&StarBuilder> = star_map.values().collect();
    sorted_builders.sort_by(|a, b| a.variable.cmp(&b.variable));

    for builder in sorted_builders {
        if !var_to_class.contains_key(&builder.variable) {
            continue;
        }
        let mut sorted_slots: Vec<(&String, &String)> = builder.object_variables.iter().collect();
        sorted_slots.sort_by(|a, b| a.0.cmp(b.0));
        for (slot_name, obj_var) in sorted_slots {
            if var_to_class.contains_key(obj_var) {
                let slot_d = *builder.slot_depth.get(slot_name).unwrap_or(&0);
                let left_d = *star_depths.get(obj_var).unwrap_or(&0);
                let right_d = *star_depths.get(&builder.variable).unwrap_or(&0);
                let join_type = if slot_d > 0 || left_d > 0 || right_d > 0 {
                    JoinType::Left
                } else {
                    JoinType::Inner
                };
                joins.push(JoinEdge {
                    left: obj_var.clone(),
                    right: builder.variable.clone(),
                    right_slot: slot_name.clone(),
                    join_type,
                });
            }
        }
    }

    // Phase 2b: Reject disconnected OPTIONAL. Every star declared
    // inside an OPTIONAL block MUST share at least one join edge with
    // either a mandatory star or transitively with another star that
    // does. Compute reachability from mandatory stars and reject any
    // orphaned optional star.
    {
        use std::collections::HashSet;
        let mut reachable: HashSet<String> = stars
            .iter()
            .filter(|s| !s.is_optional)
            .map(|s| s.variable.clone())
            .collect();
        let mut changed = true;
        while changed {
            changed = false;
            for j in &joins {
                if reachable.contains(&j.left) && !reachable.contains(&j.right) {
                    reachable.insert(j.right.clone());
                    changed = true;
                }
                if reachable.contains(&j.right) && !reachable.contains(&j.left) {
                    reachable.insert(j.left.clone());
                    changed = true;
                }
            }
        }
        for s in &stars {
            if s.is_optional && !reachable.contains(&s.variable) {
                return Err(ScopeError::UnsupportedConstruct(format!(
                    "OPTIONAL block introduces ?{} which shares no variable with the mandatory pattern; \
                     disconnected OPTIONAL is not supported yet",
                    s.variable
                )));
            }
        }
    }

    // Phase 3: Collect filter conditions per star.
    let mut var_to_field: HashMap<String, (String, String)> = HashMap::new();
    // Map: object_variable → (star_variable, slot_name)
    for builder in star_map.values() {
        if !var_to_class.contains_key(&builder.variable) {
            continue;
        }
        for (slot_name, obj_var) in &builder.object_variables {
            if !var_to_class.contains_key(obj_var) {
                // obj_var is a value variable (not another star's subject)
                var_to_field.insert(
                    obj_var.clone(),
                    (builder.variable.clone(), slot_name.clone()),
                );
            }
        }
    }

    let mut star_filters: HashMap<String, HashMap<String, Vec<FilterCondition>>> = HashMap::new();
    if let Some(cause) = collect_filter_conditions(pattern, 0, &var_to_field, &mut star_filters) {
        drop(cause);
    }
    if let Some(cause) = collect_values_filters(pattern, 0, &var_to_field, &mut star_filters) {
        drop(cause);
    }

    for star in &mut stars {
        if let Some(extra) = star_filters.remove(&star.variable) {
            let id_slot = var_to_identifier_slot.get(&star.variable).cloned();
            // Merge into any inline-constant filters seeded in Phase 1.
            // Identifier-slot filters get hoisted into identifier_values
            // instead of star.filters — they pushdown against the indexed
            // `asset360_uri` column, not JSONB.
            for (slot, conds) in extra {
                if Some(&slot) == id_slot.as_ref() {
                    for cond in conds {
                        match cond {
                            FilterCondition::Eq(v) => star.identifier_values.push(v),
                            FilterCondition::In(vs) => star.identifier_values.extend(vs),
                            // See the Phase-1 note: an ordering comparison has
                            // no value list to hoist, so it stays a filter.
                            cmp @ FilterCondition::Cmp { .. } => {
                                star.filters.entry(slot.clone()).or_default().push(cmp);
                            }
                        }
                    }
                } else {
                    star.filters.entry(slot).or_default().extend(conds);
                }
            }
        }
    }

    // Phase 5: Wrap the result into a PlanNode tree. If the original
    // pattern has no OPTIONAL (all joins inner, no optional stars), emit
    // a single `Bgp` node. If any OPTIONAL is present, split mandatory
    // and optional stars into the left / right of a single `LeftJoin`.
    // Nested OPTIONAL flattens to this two-level shape — all the
    // non-trivial semantics (all-or-nothing per block, sibling
    // independence) live in oxigraph, not in the plan tree.
    // Facts about the shape, captured before the stars and joins move into the
    // plan tree, and used by the LIMIT decision at the end.
    let has_optional = !stars.iter().all(|s| !s.is_optional)
        || joins.iter().any(|j| j.join_type == JoinType::Left);
    // A LIMIT can only bound a fetch that returns one row per solution. With
    // more than one star, or any join, a row is a combination and the top N
    // rows are not the top N solutions.
    let single_relation = stars.len() == 1 && joins.is_empty();

    let root = if has_optional {
        let mandatory_vars: HashSet<String> = stars
            .iter()
            .filter(|s| !s.is_optional)
            .map(|s| s.variable.clone())
            .collect();
        let mandatory_stars: Vec<Star> = stars.iter().filter(|s| !s.is_optional).cloned().collect();
        let optional_stars: Vec<Star> = stars.iter().filter(|s| s.is_optional).cloned().collect();
        let mandatory_joins: Vec<JoinEdge> = joins
            .iter()
            .filter(|j| {
                mandatory_vars.contains(&j.left)
                    && mandatory_vars.contains(&j.right)
                    && j.join_type == JoinType::Inner
            })
            .cloned()
            .collect();
        let optional_joins: Vec<JoinEdge> = joins
            .iter()
            .filter(|j| {
                !(mandatory_vars.contains(&j.left)
                    && mandatory_vars.contains(&j.right)
                    && j.join_type == JoinType::Inner)
            })
            .cloned()
            .collect();
        PlanNode::LeftJoin {
            left: Box::new(PlanNode::Bgp {
                stars: mandatory_stars,
                joins: mandatory_joins,
            }),
            right: Box::new(PlanNode::Bgp {
                stars: optional_stars,
                joins: optional_joins,
            }),
        }
    } else {
        PlanNode::Bgp { stars, joins }
    };

    // Phase 6: paths into nested structures. Done after the stars exist so a
    // variable that *is* a star is never mistaken for a step inside one.
    let (path_bindings, traversed) = collect_path_bindings(&star_map, &var_to_class, schema_view);

    // A subject the path walk reached is a step inside a star, which the plan
    // describes. Anything left is a subject nothing accounts for.
    if unresolved_subjects
        .iter()
        .any(|var| !traversed.contains(var))
    {
        drop(Inexact::UntypedSubject);
    }

    // A variable bound by two slots is an equality between them —
    // `?s :hasName ?v ; :asset360_uri ?v` says the name equals the id — and the
    // plan carries the slots separately with no condition tying them. Which
    // binding a consumer picked would then decide the answer, hash order and
    // all.
    let mut bound_once: HashSet<&String> = HashSet::new();
    let mut bound_twice = false;
    for builder in star_map.values() {
        for object_var in builder.object_variables.values() {
            if var_to_class.contains_key(object_var) {
                // A typed object variable is a join edge, which the plan does
                // carry.
                continue;
            }
            if !bound_once.insert(object_var) {
                bound_twice = true;
            }
        }
    }
    if bound_twice {
        drop(Inexact::ImpliedEquality);
    }

    if contains_subquery(pattern) {
        drop(Inexact::Subquery);
    }
    if let Some(cause) = contains_foreign_scope(pattern) {
        drop(cause);
    }

    // Phase 7: the SQL LIMIT.
    //
    // `pushable_limit` owns the modifier question — whether an operator must
    // see every solution before the limit applies — and this owns the rest: a
    // LIMIT bounds the *fetch*, so it is only sound when the fetch returns the
    // real row set. With anything dropped, ten rows off the top are ten
    // arbitrary rows and the engine filters them down to fewer than the query
    // asked for. One assignment, so there is no second owner to disagree with.
    let sql_limit = if inexact.is_none() && single_relation && !has_optional {
        pushable_limit(pattern)
    } else {
        None
    };

    Ok(QueryPlan {
        root,
        sql_limit,
        path_bindings,
        inexact,
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

struct StarBuilder {
    variable: String,
    /// `Some(iri)` when the subject is a constant IRI rather than a query
    /// variable. The IRI is the instance's identifier (seeded into
    /// `identifier_values`); `variable` then holds a synthetic key.
    const_iri: Option<String>,
    type_iri: Option<String>,
    /// Minimum OPTIONAL depth at which this star's `rdf:type` appears.
    /// `0` = mandatory; `> 0` = inside one or more `OPTIONAL` blocks.
    type_depth: usize,
    /// Map: slot name → minimum OPTIONAL depth at which the slot is
    /// referenced on this subject.
    slot_depth: HashMap<String, usize>,
    /// Map: slot_name → object variable name (for join detection + filters).
    object_variables: HashMap<String, String>,
    /// Filters extracted from triples whose object is an inline literal
    /// or NamedNode (e.g. `?s :hasName "X"` or `?s :foo <some/uri>`).
    /// Only collected at OPTIONAL depth 0 — inside an OPTIONAL block
    /// these filters can't be safely pushed to SQL without breaking
    /// LEFT JOIN semantics, so they're left for oxigraph to apply.
    inline_filters: HashMap<String, Vec<FilterCondition>>,
}

/// Resolve the LinkML class (and identifier slot name) for one star.
///
/// Returns:
/// - `Ok(Some((class_uri, identifier_slot_name)))` — the star is scopable.
/// - `Ok(None)` — a *variable* subject we can't scope (no/unknown `rdf:type`).
///   The caller skips it and lets oxigraph evaluate it against the (superset)
///   prefetch, exactly as before.
/// - `Err(UnsupportedConstruct)` — a *constant-IRI* subject we can't scope.
///   Dropping it silently would return wrong data, so the whole query is
///   rejected with an actionable message instead.
///
/// An explicit `rdf:type` always wins and is the escape hatch to disambiguate
/// a constant subject whose class can't be inferred. Without one, a
/// constant-IRI subject's class is inferred from the slots it uses: a class is
/// a candidate when it has *every* slot mentioned on the subject. Exactly one
/// candidate → inferred; zero or several → rejected (ambiguous).
fn resolve_star_class(
    builder: &StarBuilder,
    schema_view: &SchemaView,
) -> Result<Option<(String, Option<String>)>, ScopeError> {
    if let Some(iri) = &builder.type_iri {
        return match schema_view.get_class_by_uri(iri) {
            // Schema knows this class — keep the full IRI as the canonical
            // identifier crossing the Rust↔Python boundary. The identifier
            // slot may be None if the class declares none.
            Ok(Some(cv)) => Ok(Some((
                iri.clone(),
                cv.identifier_slot().map(|s| s.name.clone()),
            ))),
            _ => match &builder.const_iri {
                Some(subj) => Err(ScopeError::UnsupportedConstruct(format!(
                    "constant subject <{subj}> has rdf:type <{iri}>, which is not a known class"
                ))),
                None => Ok(None), // unknown type on a variable subject — skip
            },
        };
    }

    // No explicit rdf:type.
    let Some(subj) = &builder.const_iri else {
        return Ok(None); // variable subject without rdf:type — can't scope, skip
    };

    // Constant-IRI subject: infer the class from the slots it uses.
    let used_slots: Vec<&String> = builder.slot_depth.keys().collect();
    let mut candidates: Vec<linkml_schemaview::classview::ClassView> = Vec::new();
    if !used_slots.is_empty() {
        let all_classes = schema_view
            .class_views()
            .map_err(|e| ScopeError::ParseError(e.to_string()))?;
        for cv in all_classes {
            if used_slots
                .iter()
                .all(|slot| cv.slots().iter().any(|s| s.name == **slot))
            {
                candidates.push(cv);
            }
        }
    }

    match candidates.as_slice() {
        [cv] => Ok(Some((
            cv.canonical_uri().to_string(),
            cv.identifier_slot().map(|s| s.name.clone()),
        ))),
        [] => Err(ScopeError::UnsupportedConstruct(format!(
            "constant subject <{subj}> has no rdf:type and its class cannot be inferred from \
             the slots it uses; add an explicit `<{subj}> a asset360:<Class>`"
        ))),
        many => {
            let mut names: Vec<&str> = many.iter().map(|c| c.name()).collect();
            names.sort_unstable();
            Err(ScopeError::UnsupportedConstruct(format!(
                "constant subject <{subj}> matches multiple classes ({}); add an explicit \
                 `<{subj}> a asset360:<Class>` to disambiguate",
                names.join(", ")
            )))
        }
    }
}

/// Recursively walk the SPARQL algebra tree and collect every BGP
/// triple pattern, tagged with the OPTIONAL nesting depth at which it
/// occurs. `depth == 0` means the triple is in the mandatory part of
/// the query; `depth > 0` means it is inside one or more nested
/// `OPTIONAL { ... }` blocks.
///
/// Along the way, unsupported constructs (`UNION`, `MINUS`, property
/// paths) are rejected with [`ScopeError::UnsupportedConstruct`].
fn tag_triples_by_depth<'a>(
    pattern: &'a GraphPattern,
    depth: usize,
    out: &mut Vec<(&'a TriplePattern, usize)>,
) -> Result<(), ScopeError> {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for tp in patterns {
                out.push((tp, depth));
            }
            Ok(())
        }
        GraphPattern::Join { left, right } => {
            tag_triples_by_depth(left, depth, out)?;
            tag_triples_by_depth(right, depth, out)
        }
        GraphPattern::LeftJoin { left, right, .. } => {
            // Left side stays at the current depth — it's the mandatory
            // pattern from the point of view of this OPTIONAL.
            tag_triples_by_depth(left, depth, out)?;
            // Right side is one level deeper — it's inside the OPTIONAL.
            tag_triples_by_depth(right, depth + 1, out)
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
        | GraphPattern::Service { inner, .. } => tag_triples_by_depth(inner, depth, out),
        GraphPattern::Values { .. } => Ok(()),
        GraphPattern::Union { .. } => Err(ScopeError::UnsupportedConstruct(
            "UNION is not supported yet; issue separate queries and merge client-side".into(),
        )),
        GraphPattern::Minus { .. } => Err(ScopeError::UnsupportedConstruct(
            "MINUS is not supported yet".into(),
        )),
        GraphPattern::Path { .. } => Err(ScopeError::UnsupportedConstruct(
            "SPARQL property paths are not supported; use explicit triple patterns".into(),
        )),
    }
}

/// Collect FILTER equality conditions, now keyed by (star_variable, slot_name).
/// Collect the FILTER conditions that can be pushed to SQL.
///
/// Returns `false` when anything was left behind, which the caller records as
/// inexactness. Two ways that happens:
///
/// * an expression this cannot express — `!=`, `||`, `!`, `REGEX`, `BOUND`, a
///   comparison between two variables;
/// * a `FILTER` inside an `OPTIONAL`. Pushing one into the fetch drops rows the
///   LEFT JOIN is supposed to preserve, which is why inline constants are
///   depth-gated; this recursed into `LeftJoin` with no gate.
fn collect_filter_conditions(
    pattern: &GraphPattern,
    depth: usize,
    var_to_field: &HashMap<String, (String, String)>,
    star_filters: &mut HashMap<String, HashMap<String, Vec<FilterCondition>>>,
) -> Option<Inexact> {
    match pattern {
        GraphPattern::Filter { expr, inner } => {
            let here = if depth == 0 {
                if extract_equality_from_expr(expr, var_to_field, star_filters) {
                    None
                } else {
                    Some(Inexact::FilterExpression)
                }
            } else {
                // Inside an OPTIONAL: leave it entirely to oxigraph.
                Some(Inexact::FilterInOptional)
            };
            here.or(collect_filter_conditions(
                inner,
                depth,
                var_to_field,
                star_filters,
            ))
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            let l = collect_filter_conditions(left, depth, var_to_field, star_filters);
            let r = collect_filter_conditions(right, depth + 1, var_to_field, star_filters);
            // `OPTIONAL { ... FILTER(...) }` does not leave a Filter node:
            // spargebra lifts the condition into the LeftJoin itself. It is not
            // pushable — it decides whether the optional side *matched*, so
            // applying it to the fetch would drop rows the LEFT JOIN exists to
            // keep — and it is not represented in the plan either, so a
            // consumer treating the plan as exact has to know.
            let lifted = expression.as_ref().map(|_| Inexact::FilterInOptional);
            l.or(r).or(lifted)
        }
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_filter_conditions(left, depth, var_to_field, star_filters).or(
                collect_filter_conditions(right, depth, var_to_field, star_filters),
            )
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
            collect_filter_conditions(inner, depth, var_to_field, star_filters)
        }
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => None,
    }
}

/// Push what this expression says into `star_filters`.
///
/// Returns `false` when any part of it could not be expressed, so the caller
/// knows the plan no longer describes the whole query. Silence here is what
/// turned a dropped `REGEX` into ten arbitrary rows.
fn extract_equality_from_expr(
    expr: &Expression,
    var_to_field: &HashMap<String, (String, String)>,
    star_filters: &mut HashMap<String, HashMap<String, Vec<FilterCondition>>>,
) -> bool {
    match expr {
        Expression::Equal(left, right) => {
            if let Some((star_var, field, value)) = match_var_literal(left, right, var_to_field)
                .or_else(|| match_var_literal(right, left, var_to_field))
            {
                star_filters
                    .entry(star_var)
                    .or_default()
                    .entry(field)
                    .or_default()
                    .push(FilterCondition::Eq(value));
                true
            } else {
                // A comparison between two variables, or against something
                // that is not a literal.
                false
            }
        }
        Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right) => {
            // Written either way round: `?len > 10` and `10 < ?len` are the
            // same constraint, so a reversed match flips the operator.
            // Exhaustive over the four variants this arm matches. A catch-all
            // was survivable while the plan was only a prefetch — a wrong
            // operator merely widened the fetch — but the plan is now the
            // answer, so a fifth comparison variant silently becoming `<=`
            // would be a wrong number. `unreachable!` cannot fire: the outer
            // pattern admits exactly these four.
            let forward = match expr {
                Expression::Greater(..) => CmpOp::Gt,
                Expression::GreaterOrEqual(..) => CmpOp::Gte,
                Expression::Less(..) => CmpOp::Lt,
                Expression::LessOrEqual(..) => CmpOp::Lte,
                _ => unreachable!("outer match admits only the four comparisons"),
            };
            let (op, found) = match match_var_literal(left, right, var_to_field) {
                Some(found) => (forward, Some(found)),
                None => (
                    match forward {
                        CmpOp::Gt => CmpOp::Lt,
                        CmpOp::Gte => CmpOp::Lte,
                        CmpOp::Lt => CmpOp::Gt,
                        CmpOp::Lte => CmpOp::Gte,
                    },
                    match_var_literal(right, left, var_to_field),
                ),
            };
            if let Some((star_var, field, value)) = found {
                star_filters
                    .entry(star_var)
                    .or_default()
                    .entry(field)
                    .or_default()
                    .push(FilterCondition::Cmp { op, value });
                true
            } else {
                false
            }
        }
        // `FILTER(?v IN ("a", "b"))` is the same constraint as
        // `VALUES ?v { "a" "b" }`, which is already pushed. Refusing one while
        // accepting the other made the supported subset depend on how the query
        // happened to be written.
        Expression::In(target, options) => {
            let Expression::Variable(var) = target.as_ref() else {
                return false;
            };
            let Some((star_var, field)) = var_to_field.get(var.as_str()) else {
                return false;
            };
            let mut values = Vec::with_capacity(options.len());
            for option in options {
                match option {
                    Expression::Literal(lit) => values.push(lit.value().to_owned()),
                    Expression::NamedNode(nn) => values.push(nn.as_str().to_owned()),
                    // A computed member would have to be evaluated first.
                    _ => return false,
                }
            }
            if values.is_empty() {
                // `IN ()` is never true, which the plan has no way to say.
                return false;
            }
            star_filters
                .entry(star_var.clone())
                .or_default()
                .entry(field.clone())
                .or_default()
                .push(FilterCondition::In(values));
            true
        }
        Expression::And(left, right) => {
            // Both halves must land: `A && B` with B dropped is a weaker
            // filter, which over-fetches — safe for a prefetch, wrong for an
            // exact plan, and the caller can only tell if it hears about it.
            let l = extract_equality_from_expr(left, var_to_field, star_filters);
            let r = extract_equality_from_expr(right, var_to_field, star_filters);
            l & r
        }
        // Everything else — `!=`, `||`, `!`, REGEX, BOUND, arithmetic — is left
        // to oxigraph, and the plan is no longer a complete description.
        _ => false,
    }
}

fn match_var_literal(
    var_expr: &Expression,
    lit_expr: &Expression,
    var_to_field: &HashMap<String, (String, String)>,
) -> Option<(String, String, String)> {
    let var_name = match var_expr {
        Expression::Variable(v) => v.as_str(),
        _ => return None,
    };
    let (star_var, field) = var_to_field.get(var_name)?;
    let value = match lit_expr {
        Expression::Literal(lit) => lit.value().to_owned(),
        _ => return None,
    };
    Some((star_var.clone(), field.clone(), value))
}

/// Collect VALUES conditions, now keyed by (star_variable, slot_name).
fn collect_values_filters(
    pattern: &GraphPattern,
    depth: usize,
    var_to_field: &HashMap<String, (String, String)>,
    star_filters: &mut HashMap<String, HashMap<String, Vec<FilterCondition>>>,
) -> Option<Inexact> {
    match pattern {
        // Inside an OPTIONAL a VALUES block narrows the optional side only.
        // Pushing it into the fetch would drop rows the join preserves, exactly
        // as a FILTER there would.
        GraphPattern::Values { .. } if depth > 0 => Some(Inexact::FilterInOptional),
        GraphPattern::Values {
            variables,
            bindings,
        } => {
            let mut dropped = None;
            for (i, var) in variables.iter().enumerate() {
                let Some((star_var, field)) = var_to_field.get(var.as_str()) else {
                    // A VALUES over a variable no star binds constrains
                    // something this plan does not describe.
                    dropped = Some(Inexact::UnboundValues);
                    continue;
                };
                {
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
                    if values.is_empty() {
                        // Nothing to constrain with, so the VALUES block says
                        // something the plan does not.
                        dropped = Some(Inexact::UnboundValues);
                    } else {
                        star_filters
                            .entry(star_var.clone())
                            .or_default()
                            .entry(field.clone())
                            .or_default()
                            .push(FilterCondition::In(values));
                    }
                }
            }
            dropped
        }
        GraphPattern::LeftJoin { left, right, .. } => {
            collect_values_filters(left, depth, var_to_field, star_filters).or(
                collect_values_filters(right, depth + 1, var_to_field, star_filters),
            )
        }
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_values_filters(left, depth, var_to_field, star_filters).or(
                collect_values_filters(right, depth, var_to_field, star_filters),
            )
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
            collect_values_filters(inner, depth, var_to_field, star_filters)
        }
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } => None,
    }
}

/// Whether the pattern contains a sub-`SELECT`.
///
/// A subquery has its own projection and its own modifiers — a `LIMIT 5` inside
/// bounds what the outer query can see — and none of that reaches the star
/// decomposition, which walks straight through to the triples. Reported as
/// inexact rather than partially parsed: half-understanding a subquery is how a
/// count over five rows becomes a count over all of them.
fn contains_subquery(pattern: &GraphPattern) -> bool {
    // The outermost Project is the query's own; anything deeper is a subquery.
    fn walk(pattern: &GraphPattern, inside: bool) -> bool {
        match pattern {
            GraphPattern::Project { inner, .. } => inside || walk(inner, true),
            GraphPattern::Filter { inner, .. }
            | GraphPattern::Extend { inner, .. }
            | GraphPattern::OrderBy { inner, .. }
            | GraphPattern::Distinct { inner }
            | GraphPattern::Reduced { inner }
            | GraphPattern::Slice { inner, .. }
            | GraphPattern::Group { inner, .. }
            | GraphPattern::Graph { inner, .. }
            | GraphPattern::Service { inner, .. } => walk(inner, inside),
            GraphPattern::Join { left, right }
            | GraphPattern::LeftJoin { left, right, .. }
            | GraphPattern::Union { left, right }
            | GraphPattern::Minus { left, right } => walk(left, inside) || walk(right, inside),
            GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => {
                false
            }
        }
    }
    walk(pattern, false)
}

/// How many rows the object fetch may be limited to, if it may be limited.
///
/// This is `OFFSET + LIMIT`, not `LIMIT`: the fetch has to cover the whole
/// window the query asks for, because the engine applies the offset to what
/// comes back. `LIMIT 10 OFFSET 20` needs thirty rows — fetching ten and then
/// skipping twenty of them returns nothing.
///
/// One function decides, because the bug this replaced was two of them
/// disagreeing: extraction walked `Slice`/`Project` while the eligibility check
/// looked at stars and joins, so nothing owned the question "is this LIMIT safe
/// to push?" and a `GROUP BY` slipped between them. Finding a limit and
/// refusing to push it are the same decision, so they are one walk — there is
/// no way to get a limit out of here without passing the check.
///
/// The operators that consume the whole sequence before LIMIT applies:
///
/// * `Group` — both `GROUP BY` and a bare aggregate, which spargebra models as
///   a `Group` with no grouping variables. A pushed LIMIT would aggregate over
///   an arbitrary subset and return a plausible, wrong number.
/// * `OrderBy` — would sort an arbitrary subset rather than the top of the full
///   ordering.
/// * `Distinct` / `Reduced` — would deduplicate an arbitrary subset, returning
///   fewer distinct values than exist. That is the shape a filter dropdown uses.
fn pushable_limit(pattern: &GraphPattern) -> Option<usize> {
    match pattern {
        // Holistic: nothing below may be pushed, whatever it says.
        GraphPattern::Group { .. }
        | GraphPattern::OrderBy { .. }
        | GraphPattern::Distinct { .. }
        | GraphPattern::Reduced { .. } => None,

        // A limit, pushable only if nothing below must see every solution.
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            if is_holistic(inner) || contains_slice(inner) {
                // A nested Slice would need composing with this one, and the
                // composition is not `min`: an inner LIMIT applies before an
                // outer OFFSET, so the two interact. Nested slices only arise
                // from sub-queries, which are refused as inexact — rather than
                // carry unreachable arithmetic that would be wrong if it ever
                // did run, refuse to push anything.
                None
            } else {
                // Cover the window: the rows skipped by OFFSET still have to
                // be fetched, or the engine offsets into a short result and
                // returns fewer rows than the query asked for — or none.
                length.map(|length| length.saturating_add(*start))
            }
        }

        // Transparent: keep looking underneath.
        GraphPattern::Project { inner, .. }
        | GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. } => pushable_limit(inner),

        // No limit here.
        GraphPattern::Join { .. }
        | GraphPattern::LeftJoin { .. }
        | GraphPattern::Union { .. }
        | GraphPattern::Minus { .. }
        | GraphPattern::Bgp { .. }
        | GraphPattern::Path { .. }
        | GraphPattern::Values { .. } => None,
    }
}

/// Walk each star's nested structures, recording where scalar leaves live.
///
/// Only slots that are *inlined* are followed. A reference slot holds another
/// object's URI, so its value is a join key, not a place to read through: the
/// nested data simply is not there to walk.
fn collect_path_bindings(
    star_map: &HashMap<String, StarBuilder>,
    var_to_class: &HashMap<String, String>,
    schema_view: &SchemaView,
) -> (HashMap<String, PathBinding>, HashSet<String>) {
    let mut out: HashMap<String, PathBinding> = HashMap::new();
    // Variables the walk explained *as steps*: the intermediate nodes it passed
    // through. Scalar leaves are deliberately absent — a leaf holds a value, so
    // a leaf variable used as a subject is a subject nothing accounts for, and
    // clearing it would reinstate the hole this set exists to close.
    //
    // Used only to clear a recorded drop, never to re-derive the check.
    let mut traversed: HashSet<String> = HashSet::new();

    // Deterministic order: two stars could in principle reach the same variable,
    // and which path wins must not depend on hash iteration order.
    let mut star_vars: Vec<&String> = var_to_class.keys().collect();
    star_vars.sort();

    for star_var in star_vars {
        let Ok(Some(class_view)) = schema_view.get_class_by_uri(&var_to_class[star_var]) else {
            continue;
        };
        walk_paths(
            star_var,
            star_var,
            &class_view,
            &[],
            star_map,
            var_to_class,
            &mut out,
            &mut traversed,
        );
    }

    (out, traversed)
}

#[allow(clippy::too_many_arguments)]
fn walk_paths(
    star_var: &str,
    current_var: &str,
    current_class: &linkml_schemaview::classview::ClassView,
    path_so_far: &[String],
    star_map: &HashMap<String, StarBuilder>,
    var_to_class: &HashMap<String, String>,
    out: &mut HashMap<String, PathBinding>,
    traversed: &mut HashSet<String>,
) {
    if path_so_far.len() >= MAX_PATH_DEPTH {
        return;
    }
    let Some(builder) = star_map.get(current_var) else {
        return;
    };

    let mut slots: Vec<(&String, &String)> = builder.object_variables.iter().collect();
    slots.sort_by(|a, b| a.0.cmp(b.0));

    for (slot_name, object_var) in slots {
        // A typed object variable is its own star, reached by a join edge.
        if var_to_class.contains_key(object_var) {
            continue;
        }
        let Some(slot) = current_class.slots().iter().find(|s| s.name == *slot_name) else {
            continue;
        };

        let mut path = path_so_far.to_vec();
        path.push(slot_name.clone());

        match slot.get_range_class() {
            // A nested structure: keep walking. The variable standing for the
            // structure itself is deliberately not recorded — it serialises as
            // a blank node, which no consumer can reproduce, so it is a step
            // rather than a value.
            Some(range_class) => {
                if matches!(
                    slot.determine_slot_inline_mode(),
                    linkml_schemaview::slotview::SlotInlineMode::Inline
                ) {
                    traversed.insert(object_var.clone());
                    walk_paths(
                        star_var,
                        object_var,
                        &range_class,
                        &path,
                        star_map,
                        var_to_class,
                        out,
                        traversed,
                    );
                }
            }
            // A scalar leaf. Depth one is already in Star::slot_variables;
            // recording it again would give two answers to one question.
            None => {
                if path.len() > 1 {
                    out.insert(
                        object_var.clone(),
                        PathBinding {
                            star_var: star_var.to_owned(),
                            slot_path: path,
                        },
                    );
                }
            }
        }
    }
}

/// A `GRAPH` or `SERVICE` block anywhere in the pattern.
///
/// Both are walked transparently by everything else here, which is right for a
/// prefetch — the triples inside still say which classes to load — and wrong
/// for an exact plan: the plan reads the default graph of the local database,
/// so a named-graph pattern is answered from the wrong graph and a remote
/// pattern from the wrong endpoint.
fn contains_foreign_scope(pattern: &GraphPattern) -> Option<Inexact> {
    match pattern {
        GraphPattern::Graph { .. } => Some(Inexact::NamedGraph),
        GraphPattern::Service { .. } => Some(Inexact::RemoteService),
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. } => contains_foreign_scope(inner),
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            contains_foreign_scope(left).or(contains_foreign_scope(right))
        }
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => None,
    }
}

/// Whether a `Slice` appears below this point.
fn contains_slice(pattern: &GraphPattern) -> bool {
    match pattern {
        GraphPattern::Slice { .. } => true,
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. } => contains_slice(inner),
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => contains_slice(left) || contains_slice(right),
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => false,
    }
}

/// Whether any operator below this point must see every solution.
///
/// Only [`pushable_limit`] calls this, to tell "no limit found" apart from
/// "found one that must not be pushed".
fn is_holistic(pattern: &GraphPattern) -> bool {
    match pattern {
        GraphPattern::Group { .. }
        | GraphPattern::OrderBy { .. }
        | GraphPattern::Distinct { .. }
        | GraphPattern::Reduced { .. } => true,
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. } => is_holistic(inner),
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => is_holistic(left) || is_holistic(right),
        // Left exhaustive on purpose: a new GraphPattern variant should be a
        // compile error here, not a silent "safe to push the LIMIT down".
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Small hand-written schema shared with the pushdown analyser's tests.
    pub(crate) fn test_schema_view() -> SchemaView {
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
  Document:
    class_uri: asset360:Document
    attributes:
      docId:
        key: true
      title:
        range: string
  Coordinates:
    class_uri: asset360:Coordinates
    attributes:
      longitude:
        range: integer
      latitude:
        range: integer
  Signal:
    class_uri: asset360:Signal
    attributes:
      asset360_uri:
        identifier: true
      name:
        range: string
      length:
        range: integer
      location:
        range: Coordinates
        inlined: true
      trafficKinds:
        range: string
        multivalued: true
      documents:
        range: Document
        multivalued: true
        inlined: true
      locatedOnTrack:
        range: Track
  BaliseGroup:
    class_uri: asset360:BaliseGroup
    attributes:
      asset360_uri:
        identifier: true
      refersToSignal:
        range: Signal
  TunnelComplex:
    class_uri: asset360:TunnelComplex
    attributes:
      asset360_uri:
        identifier: true
      hasName:
        range: string
  CivilEngineeringAsset:
    class_uri: asset360:CivilEngineeringAsset
    attributes:
      asset360_uri:
        identifier: true
      hasName:
        range: string
      belongsToTunnelComplex:
        range: TunnelComplex
  Track:
    class_uri: asset360:Track
    attributes:
      asset360_uri:
        identifier: true
      hasName:
        range: string
      belongsToLine:
        range: Line
  Line:
    class_uri: asset360:Line
    attributes:
      asset360_uri:
        identifier: true
      hasName:
        range: string
"#;
        let schema: SchemaDefinition =
            p2e::deserialize(yml::Deserializer::from_str(schema_yaml)).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        sv
    }

    fn find_star<'a>(plan: &'a QueryPlan, var: &str) -> &'a Star {
        plan.root
            .all_stars()
            .into_iter()
            .find(|s| s.variable == var)
            .unwrap_or_else(|| panic!("no star for variable '{var}'"))
    }

    fn all_stars(plan: &QueryPlan) -> Vec<&Star> {
        plan.root.all_stars()
    }

    fn all_joins(plan: &QueryPlan) -> Vec<&JoinEdge> {
        plan.root.all_joins()
    }

    // ---- Single type ----

    #[test]
    fn test_single_type() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "SELECT ?s ?name WHERE { ?s a asset360:Signal ; asset360:name ?name }",
            &sv,
        )
        .unwrap();

        assert_eq!(all_stars(&plan).len(), 1);
        assert_eq!(all_joins(&plan).len(), 0);

        let stars = all_stars(&plan);
        let star = stars[0];
        assert_eq!(star.class_uri, "https://data.infrabel.be/asset360/Signal");
        assert!(star.required_fields.contains(&"name".to_owned()));
        assert!(!star.is_optional);
        assert!(star.optional_fields.is_empty());
        // No OPTIONAL → plan root is a single Bgp node.
        assert!(matches!(&plan.root, PlanNode::Bgp { .. }));
    }

    #[test]
    fn test_single_type_with_filter() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?name . FILTER(?name = \"BX517\") }",
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "s");
        assert_eq!(star.class_uri, "https://data.infrabel.be/asset360/Signal");
        let name_filters = star.filters.get("name").expect("should have name filter");
        assert!(matches!(&name_filters[0], FilterCondition::Eq(v) if v == "BX517"));
    }

    #[test]
    fn test_single_type_with_limit() {
        let sv = test_schema_view();
        let plan = sparql_scope("SELECT ?s WHERE { ?s a asset360:Signal } LIMIT 10", &sv).unwrap();

        assert_eq!(plan.sql_limit, Some(10));
    }

    #[test]
    fn test_nested_structure_yields_a_path_binding() {
        // ?lon lives inside ?s's JSON, two slots down. No star can describe it:
        // ?loc has no rdf:type and is not an object of its own.
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?lon WHERE { ?s a asset360:Signal ; asset360:location ?loc . \
             ?loc asset360:longitude ?lon }",
            &sv,
        )
        .unwrap();

        let binding = plan
            .path_bindings
            .get("lon")
            .expect("?lon should resolve to a path");
        assert_eq!(binding.star_var, "s");
        assert_eq!(binding.slot_path, vec!["location", "longitude"]);

        // The intermediate variable is traversable, not bindable: it stands for
        // the nested structure, which serialises as a blank node.
        assert!(!plan.path_bindings.contains_key("loc"));
    }

    #[test]
    fn test_direct_slots_are_not_duplicated_as_paths() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?name WHERE { ?s a asset360:Signal ; asset360:name ?name }",
            &sv,
        )
        .unwrap();

        // Depth one belongs to Star::slot_variables; two answers to one
        // question is how they drift apart.
        assert!(plan.path_bindings.is_empty());
        let star = find_star(&plan, "s");
        assert_eq!(
            star.slot_variables.get("name").map(String::as_str),
            Some("name")
        );
    }

    #[test]
    fn test_reference_slots_are_not_walked_into() {
        // `locatedOnTrack` holds another object's URI, so its value is a join
        // key. Reading through it in JSONB would look for data that is not
        // there.
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t asset360:hasName ?tn }",
            &sv,
        )
        .unwrap();

        assert!(
            !plan.path_bindings.contains_key("tn"),
            "a reference must not become a JSON path: {:?}",
            plan.path_bindings
        );
    }

    #[test]
    fn test_comparison_filters_are_pushed_down() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        let plan = sparql_scope(
            &format!(
                "{prefix}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:length ?len . \
                 FILTER(?len > 10) }}"
            ),
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "s");
        let conds = star.filters.get("length").expect("length filter");
        assert!(matches!(
            &conds[0],
            FilterCondition::Cmp {
                op: CmpOp::Gt,
                value
            } if value == "10"
        ));
    }

    #[test]
    fn test_reversed_comparison_flips_the_operator() {
        // `10 < ?len` constrains ?len the same way `?len > 10` does; written
        // the other way round the operator has to flip, or the filter would
        // exclude exactly the rows it should keep.
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s a asset360:Signal ; asset360:length ?len . FILTER(10 < ?len) }",
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "s");
        let conds = star.filters.get("length").expect("length filter");
        assert!(
            matches!(&conds[0], FilterCondition::Cmp { op: CmpOp::Gt, value } if value == "10"),
            "expected > 10, got {:?}",
            conds[0]
        );
    }

    #[test]
    fn test_range_filters_combine() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s a asset360:Signal ; asset360:length ?len . \
             FILTER(?len >= 10 && ?len <= 20) }",
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "s");
        let conds = star.filters.get("length").expect("length filter");
        assert_eq!(conds.len(), 2, "both bounds should be pushed: {conds:?}");
    }

    /// LIMIT must NOT be pushed into the object fetch when an operator has to
    /// see every solution first: the fetch would feed the aggregate / sort /
    /// dedup an arbitrary subset and return a plausible wrong answer with no
    /// error.
    #[test]
    fn test_limit_not_pushed_past_holistic_modifiers() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        for (label, query) in [
            (
                "group by + count",
                "SELECT ?name (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; asset360:name ?name } \
                 GROUP BY ?name LIMIT 10",
            ),
            (
                "bare aggregate",
                "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal } LIMIT 10",
            ),
            (
                "order by",
                "SELECT ?s ?name WHERE { ?s a asset360:Signal ; asset360:name ?name } \
                 ORDER BY ?name LIMIT 10",
            ),
            (
                "distinct",
                "SELECT DISTINCT ?name WHERE { ?s a asset360:Signal ; asset360:name ?name } LIMIT 10",
            ),
            // A LIMIT also bounds the *fetch*, so it is only sound when the
            // fetch returns the real row set. These plans drop part of the
            // query, so ten rows off the top are ten arbitrary rows and the
            // engine then filters them down to fewer than the query asked for.
            (
                "dropped REGEX filter",
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(REGEX(?nm, \"^BX\")) } LIMIT 10",
            ),
            (
                "dropped != filter",
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(?nm != \"BX517\") } LIMIT 10",
            ),
            (
                "unknown predicate",
                "SELECT ?s WHERE { ?s a asset360:Signal . ?s <urn:unknown> \"x\" } LIMIT 10",
            ),
            (
                "variable predicate",
                "SELECT ?s WHERE { ?s a asset360:Signal . ?s ?p \"x\" } LIMIT 10",
            ),
        ] {
            let plan = sparql_scope(&format!("{prefix}{query}"), &sv).unwrap();
            assert_eq!(plan.sql_limit, None, "sql_limit must be None for: {label}");
        }
    }

    /// Every point that drops part of the query must say so, because a LIMIT is
    /// only pushable when the fetch returns the real row set, and an exact
    /// consumer must refuse. Each of these reported `exact` before, and each
    /// answered a weaker question than it was asked.
    #[test]
    fn dropping_part_of_the_query_is_recorded_at_the_drop_site() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        for (expected, query) in [
            (
                Inexact::UnknownPredicate,
                "SELECT ?s WHERE { ?s a asset360:Signal . ?s <urn:unknown> \"x\" }",
            ),
            (
                Inexact::VariablePredicate,
                "SELECT ?s WHERE { ?s a asset360:Signal . ?s ?p \"x\" }",
            ),
            (
                Inexact::ConstantInOptional,
                "SELECT ?s WHERE { ?s a asset360:Signal . \
                 OPTIONAL { ?s asset360:name \"BX\" } }",
            ),
            (
                Inexact::UnboundValues,
                "SELECT ?s WHERE { ?s a asset360:Signal . VALUES ?zz { \"a\" } }",
            ),
            (
                Inexact::UntypedSubject,
                "SELECT ?t WHERE { ?sig asset360:locatedOnTrack ?t . ?t a asset360:Track }",
            ),
            (
                Inexact::FilterExpression,
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(REGEX(?nm, \"^BX\")) }",
            ),
            (
                Inexact::FilterInOptional,
                "SELECT ?s WHERE { ?s a asset360:Signal . \
                 OPTIONAL { ?s asset360:length ?l . FILTER(?l > 5) } }",
            ),
            (
                Inexact::Subquery,
                "SELECT ?s WHERE { { SELECT ?s WHERE { ?s a asset360:Signal } LIMIT 5 } }",
            ),
        ] {
            let plan = sparql_scope(&format!("{prefix}{query}"), &sv).unwrap();
            assert_eq!(
                plan.inexact,
                Some(expected),
                "wrong cause recorded for: {query}"
            );
        }
    }

    /// Each of these was measured Eligible with a plan that did not describe
    /// the query, and each answered a weaker question than it was asked.
    #[test]
    fn more_drop_sites_are_recorded() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        for (expected, query) in [
            // A GRAPH block names a graph; the plan reads the default one.
            (
                Inexact::NamedGraph,
                "SELECT ?s WHERE { GRAPH <urn:g> { ?s a asset360:Signal } }",
            ),
            // A SERVICE block reads another endpoint entirely.
            (
                Inexact::RemoteService,
                "SELECT ?s WHERE { SERVICE <urn:remote> { ?s a asset360:Signal } }",
            ),
            // One variable bound by two slots is an equality between them.
            (
                Inexact::ImpliedEquality,
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?v ; \
                 asset360:length ?v }",
            ),
            // A VALUES inside OPTIONAL narrows the optional side only.
            (
                Inexact::FilterInOptional,
                "SELECT ?s WHERE { ?s a asset360:Signal . \
                 OPTIONAL { ?s asset360:name ?nm . VALUES ?nm { \"a\" } } }",
            ),
            // A scalar leaf used as a subject: a literal cannot be a subject,
            // and the path walk must not clear it just because it is a leaf.
            (
                Inexact::UntypedSubject,
                "SELECT ?x WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 ?nm asset360:name ?x }",
            ),
        ] {
            let plan = sparql_scope(&format!("{prefix}{query}"), &sv).unwrap();
            assert_eq!(plan.inexact, Some(expected), "wrong cause for: {query}");
        }
    }

    /// A LIMIT bounds the fetch, so the fetch has to cover the whole window the
    /// query asks for. `LIMIT 10 OFFSET 20` needs thirty rows: fetching ten and
    /// then offsetting twenty of them returns nothing at all.
    #[test]
    fn offset_is_included_in_the_pushed_row_count() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        for (query, expected) in [
            (
                "SELECT ?s WHERE { ?s a asset360:Signal } LIMIT 10",
                Some(10),
            ),
            (
                "SELECT ?s WHERE { ?s a asset360:Signal } LIMIT 10 OFFSET 20",
                Some(30),
            ),
            (
                "SELECT ?s WHERE { ?s a asset360:Signal } OFFSET 20",
                // No LIMIT bounds nothing, however large the offset.
                None,
            ),
        ] {
            let plan = sparql_scope(&format!("{prefix}{query}"), &sv).unwrap();
            assert_eq!(plan.sql_limit, expected, "for: {query}");
        }
    }

    /// `FILTER(?v IN (...))` is the same constraint as `VALUES ?v { ... }`,
    /// which was already pushed — accepting one and refusing the other made the
    /// supported subset depend on how the query happened to be written.
    #[test]
    fn filter_in_is_pushed_like_values() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        let plan = sparql_scope(
            &format!(
                "{prefix}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(?nm IN (\"a\", \"b\")) }}"
            ),
            &sv,
        )
        .unwrap();

        assert_eq!(plan.inexact, None);
        let star = find_star(&plan, "s");
        let conds = star.filters.get("name").expect("name filter");
        assert!(
            matches!(&conds[0], FilterCondition::In(values) if values.len() == 2),
            "expected an IN filter, got {:?}",
            conds[0]
        );
    }

    #[test]
    fn a_fully_expressible_query_is_exact() {
        // The check must not refuse what the plan does describe: a nested path
        // is a legitimate untyped subject, and a pushable FILTER is no loss.
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        for query in [
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . FILTER(?nm = \"BX\") }",
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:length ?l . FILTER(?l > 5) }",
            "SELECT ?lon WHERE { ?s a asset360:Signal ; asset360:location ?loc . \
             ?loc asset360:longitude ?lon }",
            "SELECT ?s WHERE { ?s a asset360:Signal } LIMIT 10",
        ] {
            let plan = sparql_scope(&format!("{prefix}{query}"), &sv).unwrap();
            assert_eq!(plan.inexact, None, "should be exact: {query}");
        }

        // And the LIMIT survives when nothing was dropped.
        let plan = sparql_scope(
            &format!("{prefix}SELECT ?s WHERE {{ ?s a asset360:Signal }} LIMIT 10"),
            &sv,
        )
        .unwrap();
        assert_eq!(plan.sql_limit, Some(10));
    }

    // ---- Two-type inner join ----

    #[test]
    fn test_two_type_join() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?complex ?complexName ?component ?componentName WHERE { \
               ?complex a asset360:TunnelComplex ; asset360:hasName ?complexName . \
               ?component a asset360:CivilEngineeringAsset ; \
                          asset360:belongsToTunnelComplex ?complex ; \
                          asset360:hasName ?componentName . \
             }",
            &sv,
        )
        .unwrap();

        assert_eq!(all_stars(&plan).len(), 2);
        assert_eq!(all_joins(&plan).len(), 1);

        let tc = find_star(&plan, "complex");
        assert_eq!(
            tc.class_uri,
            "https://data.infrabel.be/asset360/TunnelComplex"
        );
        assert!(tc.required_fields.contains(&"hasName".to_owned()));

        let cea = find_star(&plan, "component");
        assert_eq!(
            cea.class_uri,
            "https://data.infrabel.be/asset360/CivilEngineeringAsset"
        );
        assert!(cea.required_fields.contains(&"hasName".to_owned()));
        assert!(
            cea.required_fields
                .contains(&"belongsToTunnelComplex".to_owned())
        );

        let joins = all_joins(&plan);
        let join = joins[0];
        assert_eq!(join.left, "complex");
        assert_eq!(join.right, "component");
        assert_eq!(join.right_slot, "belongsToTunnelComplex");
        assert_eq!(join.join_type, JoinType::Inner);

        // Multi-type join → no SQL LIMIT pushdown
        assert_eq!(plan.sql_limit, None);
    }

    // ---- Reverse direction join ----

    #[test]
    fn test_reverse_join_direction() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?bg ?sig ?name WHERE { \
               ?bg a asset360:BaliseGroup ; asset360:refersToSignal ?sig . \
               ?sig a asset360:Signal ; asset360:name ?name . \
             }",
            &sv,
        )
        .unwrap();

        assert_eq!(all_stars(&plan).len(), 2);
        assert_eq!(all_joins(&plan).len(), 1);

        let joins = all_joins(&plan);
        let join = joins[0];
        assert_eq!(join.left, "sig"); // Signal is referenced
        assert_eq!(join.right, "bg"); // BaliseGroup holds the FK
        assert_eq!(join.right_slot, "refersToSignal");
    }

    // ---- Three-type chain ----

    #[test]
    fn test_three_type_chain() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?line ?track ?sig WHERE { \
               ?line a asset360:Line ; asset360:hasName ?ln . \
               ?track a asset360:Track ; asset360:belongsToLine ?line ; asset360:hasName ?tn . \
               ?sig a asset360:Signal ; asset360:locatedOnTrack ?track ; asset360:name ?sn . \
             }",
            &sv,
        )
        .unwrap();

        assert_eq!(all_stars(&plan).len(), 3);
        assert_eq!(all_joins(&plan).len(), 2);

        let joins = all_joins(&plan);

        // Track → Line join
        let line_track_join = joins
            .iter()
            .find(|j| j.right_slot == "belongsToLine")
            .expect("should have belongsToLine join");
        assert_eq!(line_track_join.left, "line");
        assert_eq!(line_track_join.right, "track");

        // Signal → Track join
        let track_sig_join = joins
            .iter()
            .find(|j| j.right_slot == "locatedOnTrack")
            .expect("should have locatedOnTrack join");
        assert_eq!(track_sig_join.left, "track");
        assert_eq!(track_sig_join.right, "sig");
    }

    // ---- Constant-IRI subject ----

    #[test]
    fn test_const_iri_subject_inferred_class() {
        let sv = test_schema_view();
        // Bug repro: a triple whose SUBJECT is a constant IRI must not be
        // silently dropped. Its class is inferred from the slot it uses —
        // `belongsToTunnelComplex` is declared only on CivilEngineeringAsset.
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?tc WHERE { \
               <https://data.infrabel.be/asset360/cea/X1> asset360:belongsToTunnelComplex ?tc . \
               ?tc a asset360:TunnelComplex . \
             }",
            &sv,
        )
        .unwrap();

        assert_eq!(all_stars(&plan).len(), 2);

        // The constant-IRI subject became an identifier-scoped star.
        let subj = all_stars(&plan)
            .into_iter()
            .find(|s| s.identifier_values == ["https://data.infrabel.be/asset360/cea/X1"])
            .expect("constant-IRI subject should become an identifier-scoped star");
        assert_eq!(
            subj.class_uri,
            "https://data.infrabel.be/asset360/CivilEngineeringAsset"
        );

        // ...joined to the ?tc star via the slot it used.
        let joins = all_joins(&plan);
        assert_eq!(joins.len(), 1);
        assert_eq!(joins[0].left, "tc");
        assert_eq!(joins[0].right_slot, "belongsToTunnelComplex");
        assert_eq!(joins[0].right, subj.variable);
    }

    #[test]
    fn test_const_iri_subject_explicit_type_disambiguates() {
        let sv = test_schema_view();
        // `hasName` is declared on many classes, so the class can't be
        // inferred — but an explicit rdf:type resolves it unambiguously.
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?n WHERE { \
               <https://data.infrabel.be/asset360/track/T1> a asset360:Track ; \
                                                            asset360:hasName ?n . \
             }",
            &sv,
        )
        .unwrap();

        assert_eq!(all_stars(&plan).len(), 1);
        let s = &all_stars(&plan)[0];
        assert_eq!(s.class_uri, "https://data.infrabel.be/asset360/Track");
        assert_eq!(
            s.identifier_values,
            ["https://data.infrabel.be/asset360/track/T1"]
        );
        assert!(s.required_fields.contains(&"hasName".to_owned()));
    }

    #[test]
    fn test_const_iri_subject_ambiguous_class_rejected() {
        let sv = test_schema_view();
        // `hasName` is declared on TunnelComplex, CivilEngineeringAsset, Track
        // and Line; with no rdf:type the subject's class is ambiguous. Reject
        // loudly rather than silently dropping the triple (the old bug).
        let result = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?n WHERE { \
               <https://data.infrabel.be/asset360/thing/Y> asset360:hasName ?n . \
             }",
            &sv,
        );
        assert!(matches!(result, Err(ScopeError::UnsupportedConstruct(_))));
    }

    // ---- Error cases ----

    #[test]
    fn test_unscoped_query_rejected() {
        let sv = test_schema_view();
        let result = sparql_scope("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", &sv);
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
    fn test_parse_error() {
        let sv = test_schema_view();
        let result = sparql_scope("NOT VALID {{{", &sv);
        assert!(matches!(result, Err(ScopeError::ParseError(_))));
    }

    // ---- Filter pushdown ----

    #[test]
    fn test_values_filter() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { \
               ?s a asset360:Signal ; asset360:name ?name . \
               VALUES ?name { \"BX517\" \"BX518\" } \
             }",
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "s");
        let name_filters = star.filters.get("name").expect("should have name filter");
        match &name_filters[0] {
            FilterCondition::In(vals) => {
                assert!(vals.contains(&"BX517".to_owned()));
                assert!(vals.contains(&"BX518".to_owned()));
            }
            other => panic!("expected In, got {:?}", other),
        }
    }

    // ---- ASK / CONSTRUCT ----

    #[test]
    fn test_ask_query() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "ASK { ?s a asset360:Signal ; asset360:name \"BX517\" }",
            &sv,
        )
        .unwrap();

        assert_eq!(all_stars(&plan).len(), 1);
        assert_eq!(
            all_stars(&plan)[0].class_uri,
            "https://data.infrabel.be/asset360/Signal"
        );
    }

    #[test]
    fn test_construct_query() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "CONSTRUCT { ?s a asset360:Signal ; asset360:name ?n } \
             WHERE { ?s a asset360:Signal ; asset360:name ?n }",
            &sv,
        )
        .unwrap();

        assert_eq!(all_stars(&plan).len(), 1);
        assert_eq!(
            all_stars(&plan)[0].class_uri,
            "https://data.infrabel.be/asset360/Signal"
        );
    }

    // ---- OPTIONAL support ----

    /// Simple OPTIONAL on a reference property: one mandatory star,
    /// one optional star reached via a LEFT join.
    #[test]
    fn test_simple_optional() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?complex ?component WHERE { \
               ?complex a asset360:TunnelComplex ; asset360:hasName ?cn . \
               OPTIONAL { \
                 ?component a asset360:CivilEngineeringAsset ; \
                            asset360:belongsToTunnelComplex ?complex ; \
                            asset360:hasName ?compn . \
               } \
             }",
            &sv,
        )
        .unwrap();

        assert_eq!(all_stars(&plan).len(), 2);
        assert_eq!(all_joins(&plan).len(), 1);

        let complex = find_star(&plan, "complex");
        assert!(!complex.is_optional);
        assert!(complex.required_fields.contains(&"hasName".to_owned()));

        let component = find_star(&plan, "component");
        assert!(component.is_optional);
        // Inside an OPTIONAL → slots become optional_fields, not required.
        assert!(component.required_fields.is_empty());
        assert!(component.optional_fields.contains(&"hasName".to_owned()));
        assert!(
            component
                .optional_fields
                .contains(&"belongsToTunnelComplex".to_owned())
        );

        let joins = all_joins(&plan);
        assert_eq!(joins[0].join_type, JoinType::Left);

        // Root is a LeftJoin wrapping mandatory Bgp + optional Bgp.
        match &plan.root {
            PlanNode::LeftJoin { left, right } => {
                match left.as_ref() {
                    PlanNode::Bgp { stars, .. } => {
                        assert_eq!(stars.len(), 1);
                        assert_eq!(stars[0].variable, "complex");
                    }
                    _ => panic!("expected left Bgp"),
                }
                match right.as_ref() {
                    PlanNode::Bgp { stars, joins } => {
                        assert_eq!(stars.len(), 1);
                        assert_eq!(stars[0].variable, "component");
                        assert_eq!(joins.len(), 1);
                        assert_eq!(joins[0].join_type, JoinType::Left);
                    }
                    _ => panic!("expected right Bgp"),
                }
            }
            _ => panic!("expected LeftJoin at root"),
        }
    }

    /// Nested OPTIONAL — three levels deep; inner slots become
    /// optional_fields on their respective stars.
    #[test]
    fn test_nested_optional() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT * WHERE { \
               ?line a asset360:Line ; asset360:hasName ?ln . \
               OPTIONAL { \
                 ?track a asset360:Track ; asset360:belongsToLine ?line . \
                 OPTIONAL { \
                   ?sig a asset360:Signal ; asset360:locatedOnTrack ?track ; asset360:name ?sn . \
                 } \
               } \
             }",
            &sv,
        )
        .unwrap();

        let line = find_star(&plan, "line");
        assert!(!line.is_optional);

        let track = find_star(&plan, "track");
        assert!(track.is_optional);

        let sig = find_star(&plan, "sig");
        assert!(sig.is_optional);

        // Every join involving an optional star must be a LEFT join.
        for j in all_joins(&plan) {
            assert_eq!(j.join_type, JoinType::Left, "join {j:?} should be LEFT");
        }
    }

    /// Attribute-level OPTIONAL on the mandatory entity: the slot is
    /// parked in `optional_fields`, not `required_fields`, and no new
    /// star / join is introduced.
    #[test]
    fn test_attribute_level_optional() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { \
               ?s a asset360:Signal . \
               OPTIONAL { ?s asset360:name ?n } \
             }",
            &sv,
        )
        .unwrap();

        assert_eq!(all_stars(&plan).len(), 1);
        let star = find_star(&plan, "s");
        assert!(!star.is_optional);
        assert!(!star.required_fields.contains(&"name".to_owned()));
        assert!(star.optional_fields.contains(&"name".to_owned()));
    }

    /// Mixing mandatory and optional slots on the same subject.
    #[test]
    fn test_optional_mixed_with_mandatory() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { \
               ?s a asset360:Signal ; asset360:name ?n . \
               OPTIONAL { ?s asset360:locatedOnTrack ?t } \
             }",
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "s");
        assert!(star.required_fields.contains(&"name".to_owned()));
        assert!(star.optional_fields.contains(&"locatedOnTrack".to_owned()));
        assert!(!star.required_fields.contains(&"locatedOnTrack".to_owned()));
    }

    // ---- Unsupported constructs ----

    #[test]
    fn test_union_rejected() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT * WHERE { { ?s a asset360:Signal } UNION { ?s a asset360:BaliseGroup } }",
            &sv,
        );
        assert!(
            matches!(result, Err(ScopeError::UnsupportedConstruct(ref m)) if m.contains("UNION")),
            "expected UnsupportedConstruct with UNION, got {result:?}"
        );
    }

    #[test]
    fn test_minus_rejected() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT * WHERE { ?s a asset360:Signal . MINUS { ?s asset360:name \"X\" } }",
            &sv,
        );
        assert!(
            matches!(result, Err(ScopeError::UnsupportedConstruct(ref m)) if m.contains("MINUS")),
            "expected UnsupportedConstruct with MINUS, got {result:?}"
        );
    }

    // ---- Inline-constant filter extraction (B2 regression) ----

    /// Triples whose object is an inline NamedNode (`?s :foo <uri>`)
    /// must produce a pushable equality FilterCondition, not just a
    /// silent existence check.
    #[test]
    fn test_inline_namednode_object_becomes_filter() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?comp WHERE { \
               ?comp a asset360:CivilEngineeringAsset ; \
                     asset360:belongsToTunnelComplex <https://data.infrabel.be/data/TunnelComplexes/abc> . \
             }",
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "comp");
        let f = star
            .filters
            .get("belongsToTunnelComplex")
            .expect("inline-NamedNode should produce a filter");
        match &f[0] {
            FilterCondition::Eq(v) => {
                assert_eq!(v, "https://data.infrabel.be/data/TunnelComplexes/abc");
            }
            other => panic!("expected Eq, got {other:?}"),
        }
        // The slot still appears in required_fields so the JSON key
        // existence check stays; the filter is layered on top.
        assert!(
            star.required_fields
                .contains(&"belongsToTunnelComplex".to_owned())
        );
    }

    /// Triples whose object is an inline literal (`?s :foo "bar"`)
    /// must also produce a pushable equality FilterCondition.
    #[test]
    fn test_inline_literal_object_becomes_filter() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name \"BX517\" }",
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "s");
        let f = star
            .filters
            .get("name")
            .expect("inline literal should produce a filter");
        assert!(matches!(&f[0], FilterCondition::Eq(v) if v == "BX517"));
    }

    // ---- Identifier-slot hoist (schema-resolved, never bare "id") ----

    /// Inline literal on the IDENTIFIER slot must be hoisted to
    /// `identifier_values` — not stored in `filters` (which goes to
    /// JSONB) and not added to `required_fields` (every row has an
    /// identifier by construction).
    #[test]
    fn test_inline_literal_on_identifier_slot_is_hoisted() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s a asset360:Signal ; asset360:asset360_uri \"abc\" }",
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "s");
        assert_eq!(
            star.identifier_values,
            vec!["abc".to_owned()],
            "inline literal on identifier slot must populate identifier_values"
        );
        assert!(
            !star.filters.contains_key("asset360_uri"),
            "identifier slot must NOT appear in filters (saw {:?})",
            star.filters
        );
        assert!(
            !star.required_fields.contains(&"asset360_uri".to_owned()),
            "identifier slot must NOT appear in required_fields (saw {:?})",
            star.required_fields
        );
    }

    /// Same hoist as the literal case, but with an inline IRI object.
    #[test]
    fn test_inline_iri_on_identifier_slot_is_hoisted() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { \
               ?s a asset360:Signal ; \
                  asset360:asset360_uri <https://data.infrabel.be/data/Signals/abc> . \
             }",
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "s");
        assert_eq!(
            star.identifier_values,
            vec!["https://data.infrabel.be/data/Signals/abc".to_owned()],
        );
        assert!(!star.filters.contains_key("asset360_uri"));
        assert!(!star.required_fields.contains(&"asset360_uri".to_owned()));
    }

    /// `?s :asset360_uri ?id` (variable object, no filter) must not
    /// add the identifier slot to required_fields: every row has an
    /// identifier by construction, so the JSONB existence check is
    /// structurally always true.
    #[test]
    fn test_variable_identifier_not_in_required_fields() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s ?id WHERE { ?s a asset360:Signal ; asset360:asset360_uri ?id }",
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "s");
        assert!(
            star.identifier_values.is_empty(),
            "no values bound, identifier_values must stay empty"
        );
        assert!(
            !star.required_fields.contains(&"asset360_uri".to_owned()),
            "identifier slot must not appear in required_fields (saw {:?})",
            star.required_fields
        );
        assert!(!star.filters.contains_key("asset360_uri"));
    }

    /// VALUES on the identifier slot's bound variable must hoist into
    /// identifier_values, not filters. Indexed `asset360_uri IN (...)`
    /// is the right SQL shape — JSONB extraction would defeat the index.
    #[test]
    fn test_values_on_identifier_slot_is_hoisted() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { \
               ?s a asset360:Signal ; asset360:asset360_uri ?id . \
               VALUES ?id { \"abc\" \"def\" \"ghi\" } \
             }",
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "s");
        let mut got = star.identifier_values.clone();
        got.sort();
        assert_eq!(
            got,
            vec!["abc".to_owned(), "def".to_owned(), "ghi".to_owned()]
        );
        assert!(!star.filters.contains_key("asset360_uri"));
        assert!(!star.required_fields.contains(&"asset360_uri".to_owned()));
    }

    /// FILTER(?id = "abc") on identifier-bound variable hoists into
    /// identifier_values via the Phase 3 merge path.
    #[test]
    fn test_filter_equality_on_identifier_slot_is_hoisted() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { \
               ?s a asset360:Signal ; asset360:asset360_uri ?id . \
               FILTER(?id = \"abc\") \
             }",
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "s");
        assert_eq!(star.identifier_values, vec!["abc".to_owned()]);
        assert!(!star.filters.contains_key("asset360_uri"));
    }

    /// Inline filters inside an OPTIONAL block must NOT be pushed to
    /// SQL — they would break LEFT JOIN row preservation. Oxigraph will
    /// apply them after the prefetch.
    #[test]
    fn test_inline_filter_inside_optional_not_pushed() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT * WHERE { \
               ?s a asset360:Signal . \
               OPTIONAL { ?s asset360:name \"BX517\" } \
             }",
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "s");
        // No pushed filter for `name` — it's inside an OPTIONAL.
        assert!(
            !star.filters.contains_key("name"),
            "filter on optional slot must not be pushed: {:?}",
            star.filters
        );
        // But the slot is still tracked as optional_fields so the
        // SELECT projection includes it for oxigraph.
        assert!(star.optional_fields.contains(&"name".to_owned()));
    }

    /// 3-star fan-out: one delegate star referenced by two sibling
    /// stars (sub-zone + inspection-section). Both join edges must be
    /// produced and the join order must be deterministic across runs.
    #[test]
    fn test_three_star_fan_out_join_order_deterministic() {
        let sv = test_schema_view();
        let q = "PREFIX asset360: <https://data.infrabel.be/asset360/> \
                 SELECT * WHERE { \
                   ?s a asset360:BaliseGroup ; asset360:refersToSignal ?sig . \
                   ?sig a asset360:Signal ; asset360:locatedOnTrack ?t . \
                   ?t a asset360:Track ; asset360:hasName ?tn . \
                 }";
        // Run several times — sort order must be stable.
        let plan1 = sparql_scope(q, &sv).unwrap();
        let plan2 = sparql_scope(q, &sv).unwrap();
        let plan3 = sparql_scope(q, &sv).unwrap();

        let stars1: Vec<&str> = all_stars(&plan1)
            .iter()
            .map(|s| s.variable.as_str())
            .collect();
        let stars2: Vec<&str> = all_stars(&plan2)
            .iter()
            .map(|s| s.variable.as_str())
            .collect();
        let stars3: Vec<&str> = all_stars(&plan3)
            .iter()
            .map(|s| s.variable.as_str())
            .collect();
        assert_eq!(stars1, stars2);
        assert_eq!(stars2, stars3);

        let joins1: Vec<&str> = all_joins(&plan1)
            .iter()
            .map(|j| j.right_slot.as_str())
            .collect();
        let joins2: Vec<&str> = all_joins(&plan2)
            .iter()
            .map(|j| j.right_slot.as_str())
            .collect();
        assert_eq!(joins1, joins2);

        // Both join edges must be present.
        assert_eq!(all_joins(&plan1).len(), 2);
        let slots: std::collections::HashSet<&str> = all_joins(&plan1)
            .iter()
            .map(|j| j.right_slot.as_str())
            .collect();
        assert!(slots.contains("refersToSignal"));
        assert!(slots.contains("locatedOnTrack"));
    }

    #[test]
    fn test_disconnected_optional_rejected() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT * WHERE { \
               ?a a asset360:Signal . \
               OPTIONAL { ?b a asset360:BaliseGroup } \
             }",
            &sv,
        );
        assert!(
            matches!(result, Err(ScopeError::UnsupportedConstruct(ref m)) if m.contains("disconnected")),
            "expected UnsupportedConstruct with disconnected, got {result:?}"
        );
    }
}
