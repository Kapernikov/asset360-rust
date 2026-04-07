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
}

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
/// - `class_name` → `WHERE asset_type LIKE '%ClassName'`
/// - `required_fields` → `WHERE object_data ? 'fieldName'`
/// - `optional_fields` → fetched without existence check
/// - `filters` → `WHERE object_data->>'field' = 'value'`
#[derive(Debug, Clone)]
pub struct Star {
    /// The SPARQL variable name (without `?`), e.g. `"complex"`.
    pub variable: String,

    /// The LinkML class name, e.g. `"TunnelComplex"`.
    pub class_name: String,

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
    pub filters: HashMap<String, Vec<FilterCondition>>,
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
    // Reject SPARQL Update
    if SparqlParser::new().parse_update(query_str).is_ok() {
        return Err(ScopeError::UpdateRejected);
    }

    let parser = SparqlParser::new()
        .with_prefix("asset360", "https://data.infrabel.be/asset360/")
        .expect("hardcoded prefix")
        .with_prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        .expect("hardcoded prefix")
        .with_prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
        .expect("hardcoded prefix")
        .with_prefix("xsd", "http://www.w3.org/2001/XMLSchema#")
        .expect("hardcoded prefix");

    let query = parser
        .parse_query(query_str)
        .map_err(|e| ScopeError::ParseError(e.to_string()))?;

    let pattern = match &query {
        Query::Select { pattern, .. } => pattern,
        Query::Construct { pattern, .. } => pattern,
        Query::Describe { pattern, .. } => pattern,
        Query::Ask { pattern, .. } => pattern,
    };

    // Phase 0: Depth-tag every BGP triple, rejecting unsupported
    // constructs along the way (UNION, MINUS, property paths).
    let mut triples_with_depth: Vec<(&TriplePattern, usize)> = Vec::new();
    tag_triples_by_depth(pattern, 0, &mut triples_with_depth)?;

    // Phase 1: Build stars — group triples by subject variable,
    // tracking the minimum OPTIONAL depth at which each slot and each
    // star itself was introduced.
    let mut star_map: HashMap<String, StarBuilder> = HashMap::new();

    for (tp, depth) in &triples_with_depth {
        let subj_var = match &tp.subject {
            TermPattern::Variable(v) => v.as_str().to_owned(),
            _ => continue,
        };

        let pred_iri = match &tp.predicate {
            NamedNodePattern::NamedNode(nn) => nn.as_str(),
            _ => continue,
        };

        let builder = star_map
            .entry(subj_var.clone())
            .or_insert_with(|| StarBuilder {
                variable: subj_var,
                type_iri: None,
                type_depth: usize::MAX,
                slot_depth: HashMap::new(),
                object_variables: HashMap::new(),
            });

        if pred_iri == RDF_TYPE {
            if let TermPattern::NamedNode(nn) = &tp.object
                && *depth < builder.type_depth
            {
                builder.type_iri = Some(nn.as_str().to_owned());
                builder.type_depth = *depth;
            }
        } else if let Ok(Some(slot_view)) = schema_view.get_slot_by_uri(pred_iri) {
            let slot_name = slot_view.name.clone();
            let current = builder
                .slot_depth
                .get(&slot_name)
                .copied()
                .unwrap_or(usize::MAX);
            builder.slot_depth.insert(slot_name.clone(), current.min(*depth));
            if let TermPattern::Variable(v) = &tp.object {
                builder
                    .object_variables
                    .insert(slot_name, v.as_str().to_owned());
            }
        }
    }

    // Resolve type IRIs to class names, build Star structs with
    // required / optional field split.
    let mut stars: Vec<Star> = Vec::new();
    let mut var_to_class: HashMap<String, String> = HashMap::new();
    // Track the min OPTIONAL depth at which each star first appears.
    let mut star_depths: HashMap<String, usize> = HashMap::new();

    for builder in star_map.values() {
        let class_name = match &builder.type_iri {
            Some(iri) => match schema_view.get_class_by_uri(iri) {
                Ok(Some(cv)) => cv.name().to_owned(),
                _ => continue, // unknown type IRI, skip this star
            },
            None => continue, // no rdf:type, can't scope
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

        var_to_class.insert(builder.variable.clone(), class_name.clone());
        star_depths.insert(builder.variable.clone(), builder.type_depth);

        stars.push(Star {
            variable: builder.variable.clone(),
            class_name,
            required_fields,
            optional_fields,
            is_optional: star_is_optional,
            filters: HashMap::new(), // populated below
        });
    }

    if stars.is_empty() {
        return Err(ScopeError::Unscoped(
            "Add a triple pattern like '?s rdf:type asset360:Signal' to scope the query."
                .to_owned(),
        ));
    }

    // Phase 2: Detect join edges. A join is `Left` if either endpoint
    // only appears inside an OPTIONAL block, OR the slot itself was
    // first mentioned inside an OPTIONAL block.
    let mut joins: Vec<JoinEdge> = Vec::new();

    for builder in star_map.values() {
        if !var_to_class.contains_key(&builder.variable) {
            continue;
        }
        for (slot_name, obj_var) in &builder.object_variables {
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
    collect_filter_conditions(pattern, &var_to_field, &mut star_filters);
    collect_values_filters(pattern, &var_to_field, &mut star_filters);

    for star in &mut stars {
        if let Some(filters) = star_filters.remove(&star.variable) {
            star.filters = filters;
        }
    }

    // Phase 4: SQL LIMIT — only if the query has a single star, no
    // joins, and no OPTIONAL blocks (otherwise the LIMIT could slice
    // off the wrong rows).
    let has_optional = !stars.iter().all(|s| !s.is_optional)
        || joins.iter().any(|j| j.join_type == JoinType::Left);
    let sql_limit = if stars.len() == 1 && joins.is_empty() && !has_optional {
        extract_top_level_limit(pattern)
    } else {
        None
    };

    // Phase 5: Wrap the result into a PlanNode tree. If the original
    // pattern has no OPTIONAL (all joins inner, no optional stars), emit
    // a single `Bgp` node. If any OPTIONAL is present, split mandatory
    // and optional stars into the left / right of a single `LeftJoin`.
    // Nested OPTIONAL flattens to this two-level shape — all the
    // non-trivial semantics (all-or-nothing per block, sibling
    // independence) live in oxigraph, not in the plan tree.
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
            .filter(|j| !(mandatory_vars.contains(&j.left) && mandatory_vars.contains(&j.right) && j.join_type == JoinType::Inner))
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

    Ok(QueryPlan { root, sql_limit })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

struct StarBuilder {
    variable: String,
    type_iri: Option<String>,
    /// Minimum OPTIONAL depth at which this star's `rdf:type` appears.
    /// `0` = mandatory; `> 0` = inside one or more `OPTIONAL` blocks.
    type_depth: usize,
    /// Map: slot name → minimum OPTIONAL depth at which the slot is
    /// referenced on this subject.
    slot_depth: HashMap<String, usize>,
    /// Map: slot_name → object variable name (for join detection + filters).
    object_variables: HashMap<String, String>,
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
        | GraphPattern::Service { inner, .. } => {
            tag_triples_by_depth(inner, depth, out)
        }
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
fn collect_filter_conditions(
    pattern: &GraphPattern,
    var_to_field: &HashMap<String, (String, String)>,
    star_filters: &mut HashMap<String, HashMap<String, Vec<FilterCondition>>>,
) {
    match pattern {
        GraphPattern::Filter { expr, inner } => {
            extract_equality_from_expr(expr, var_to_field, star_filters);
            collect_filter_conditions(inner, var_to_field, star_filters);
        }
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_filter_conditions(left, var_to_field, star_filters);
            collect_filter_conditions(right, var_to_field, star_filters);
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
            collect_filter_conditions(inner, var_to_field, star_filters);
        }
        _ => {}
    }
}

fn extract_equality_from_expr(
    expr: &Expression,
    var_to_field: &HashMap<String, (String, String)>,
    star_filters: &mut HashMap<String, HashMap<String, Vec<FilterCondition>>>,
) {
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
            }
        }
        Expression::And(left, right) => {
            extract_equality_from_expr(left, var_to_field, star_filters);
            extract_equality_from_expr(right, var_to_field, star_filters);
        }
        _ => {}
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
    var_to_field: &HashMap<String, (String, String)>,
    star_filters: &mut HashMap<String, HashMap<String, Vec<FilterCondition>>>,
) {
    match pattern {
        GraphPattern::Values {
            variables,
            bindings,
        } => {
            for (i, var) in variables.iter().enumerate() {
                if let Some((star_var, field)) = var_to_field.get(var.as_str()) {
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
                        star_filters
                            .entry(star_var.clone())
                            .or_default()
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
            collect_values_filters(left, var_to_field, star_filters);
            collect_values_filters(right, var_to_field, star_filters);
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
            collect_values_filters(inner, var_to_field, star_filters);
        }
        _ => {}
    }
}

/// Extract top-level LIMIT from the query pattern.
fn extract_top_level_limit(pattern: &GraphPattern) -> Option<usize> {
    match pattern {
        GraphPattern::Slice { length, .. } => *length,
        GraphPattern::Project { inner, .. } => extract_top_level_limit(inner),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
        assert_eq!(star.class_name, "Signal");
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
        assert_eq!(star.class_name, "Signal");
        let name_filters = star.filters.get("name").expect("should have name filter");
        assert!(matches!(&name_filters[0], FilterCondition::Eq(v) if v == "BX517"));
    }

    #[test]
    fn test_single_type_with_limit() {
        let sv = test_schema_view();
        let plan = sparql_scope("SELECT ?s WHERE { ?s a asset360:Signal } LIMIT 10", &sv).unwrap();

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
        assert_eq!(tc.class_name, "TunnelComplex");
        assert!(tc.required_fields.contains(&"hasName".to_owned()));

        let cea = find_star(&plan, "component");
        assert_eq!(cea.class_name, "CivilEngineeringAsset");
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
        assert_eq!(all_stars(&plan)[0].class_name, "Signal");
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
        assert_eq!(all_stars(&plan)[0].class_name, "Signal");
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
