//! Classifies a SPARQL query for **aggregate pushdown**: can its grouping and
//! aggregation be answered by SQL over the stored JSONB, instead of loading
//! objects and letting oxigraph aggregate them?
//!
//! The distinction from [`crate::sparql_scoper`] is worth stating: the scoper
//! decides *what to load* and the full query is still executed in oxigraph.
//! This module decides whether the query can be answered *without loading
//! anything*. An aggregate has no selective filter — it touches every object of
//! its class by definition — so the load-then-aggregate shape cannot serve it
//! at any realistic size.
//!
//! The verdict is deliberately three-way (see [`Pushdown`]). "Not an aggregate"
//! and "an aggregate we cannot push down" need different handling and must not
//! collapse into one `None`: the first keeps the existing route, the second is
//! reportable to whoever wrote the query.
//!
//! Every refusal carries a [`BlockedCode`] from a closed set plus a rewrite
//! hint, because the boundary of the supported subset is meant to be learned
//! from the errors rather than from a document.

use spargebra::algebra::{
    AggregateExpression, AggregateFunction, Expression, GraphPattern, OrderExpression,
};
use spargebra::term::Variable;
use spargebra::{Query, SparqlParser};

use linkml_schemaview::schemaview::SchemaView;

use crate::sparql_scoper::{ScopeError, Star, sparql_scope};

/// LinkML type names whose values are numbers. `SUM`/`AVG` are refused on
/// anything else: SPARQL raises a type error where SQL would silently cast,
/// and the two routes must not disagree.
const NUMERIC_RANGES: &[&str] = &[
    "integer",
    "float",
    "double",
    "decimal",
    "nonNegativeInteger",
    "positiveInteger",
    "negativeInteger",
    "nonPositiveInteger",
];

/// Verdict for one query.
#[derive(Debug, Clone)]
pub enum Pushdown {
    /// Not a grouping/aggregating query at all — nothing to push down, and
    /// nothing wrong with it. Keeps the existing route.
    NotApplicable,
    /// Aggregate-shaped, but outside the supported subset.
    Blocked(Blocked),
    /// Answerable in SQL.
    Eligible(SolutionSpec),
}

/// A refusal, shaped so a caller can both branch on it and show it to whoever
/// wrote the query.
#[derive(Debug, Clone)]
pub struct Blocked {
    /// Stable machine-readable code from a closed set.
    pub code: BlockedCode,
    /// What blocked, in terms of the query and the data model.
    pub detail: String,
    /// Where in the query, when it can be located: a variable, a predicate, or
    /// an operator name. `None` when the whole query shape is the problem.
    pub at: Option<String>,
}

impl Blocked {
    fn new(code: BlockedCode, detail: impl Into<String>, at: Option<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
            at,
        }
    }

    /// A supported shape to use instead. Kept on the code rather than composed
    /// per call site so the same refusal always suggests the same fix.
    pub fn instead(&self) -> &'static str {
        self.code.instead()
    }
}

/// Closed set of reasons an aggregate query cannot be pushed down.
///
/// Adding a variant is how the subset changes; nothing else may enumerate
/// shapes independently, so the lint, the API error and the documentation all
/// stay derived from this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockedCode {
    /// `GROUP_CONCAT` / `SAMPLE` / a custom aggregate.
    UnsupportedAggregate,
    /// An aggregate over an expression rather than a variable, `BIND`, or any
    /// other computed value.
    UnsupportedExpression,
    /// A pattern the star decomposition cannot express as one SQL query:
    /// `UNION`, `MINUS`, property paths, subqueries, or several stars.
    UnsupportedPattern,
    /// `SUM`/`AVG` over a slot whose declared range is not numeric.
    NonNumericMeasure,
    /// A grouping or measure variable that is not bound to a slot of a scoped
    /// class — including a variable bound to an inline (blank node) object,
    /// whose identity SQL cannot reproduce.
    UnscopedBinding,
}

impl BlockedCode {
    /// Stable string form, for an error payload or a lint code.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UnsupportedAggregate => "unsupported_aggregate",
            Self::UnsupportedExpression => "unsupported_expression",
            Self::UnsupportedPattern => "unsupported_pattern",
            Self::NonNumericMeasure => "non_numeric_measure",
            Self::UnscopedBinding => "unscoped_binding",
        }
    }

    /// The rewrite hint. This is the field that turns a rejection into a
    /// repair, so it names a shape rather than restating the problem.
    pub fn instead(&self) -> &'static str {
        match self {
            Self::UnsupportedAggregate => {
                "Use COUNT, SUM, AVG, MIN or MAX. To collect values into one \
                 row, ask for them ungrouped instead."
            }
            Self::UnsupportedExpression => {
                "Aggregate a variable bound directly by a triple pattern, e.g. \
                 `?s asset360:length ?len` then `SUM(?len)`, rather than an \
                 expression."
            }
            Self::UnsupportedPattern => {
                "Group over one class at a time. Ask a separate question per \
                 branch instead of UNION/MINUS, and spell out each step of a \
                 path as its own triple pattern."
            }
            Self::NonNumericMeasure => {
                "SUM and AVG need a numeric slot. Use COUNT to count values, \
                 or MIN/MAX to bound them."
            }
            Self::UnscopedBinding => {
                "Bind the variable with a triple pattern on a typed subject, \
                 e.g. `?s a asset360:Signal ; asset360:status ?status`. A \
                 variable standing for a nested structure itself cannot be \
                 grouped — group by a value inside it."
            }
        }
    }
}

/// What SQL must produce: one row per solution, then grouping on top.
#[derive(Debug, Clone)]
pub struct SolutionSpec {
    /// One entry per projected variable. Index is the column position, so the
    /// generated SQL never has to name a SPARQL variable: those are
    /// user-controlled, and unquoted SQL identifiers case-fold and truncate.
    pub bindings: Vec<BindingSpec>,
    /// Indices into `bindings` — the `GROUP BY` keys. Empty is legal and means
    /// one row over the whole input, which is what SPARQL returns for a bare
    /// aggregate (a SQL `GROUP BY` over no rows would return none).
    pub group_keys: Vec<usize>,
    pub measures: Vec<MeasureSpec>,
    pub order_by: Vec<OrderTerm>,
    pub distinct: bool,
    pub limit: Option<usize>,
    pub offset: usize,
}

/// One projected variable, resolved to where its value lives.
#[derive(Debug, Clone)]
pub struct BindingSpec {
    /// SPARQL variable name, used only to label the result column.
    pub var: String,
    /// The star's variable, which maps to a table alias.
    pub star_var: String,
    /// Path from the object root to the value. One element today; a path
    /// through nested inline objects when traversal lands.
    pub slot_path: Vec<String>,
    /// Pointer to the schema position, for the term descriptor. Deliberately
    /// not a copy of the rendering decision: that stays on the schema side.
    pub term_ref: TermRef,
    /// Whether the declared range is numeric.
    ///
    /// Decided here rather than in the renderer because it is schema
    /// knowledge, and the renderer needs it for two separate reasons: a
    /// numeric column casts before `SUM`/`AVG`/`MIN`/`MAX`, while a text
    /// column has to sort under `COLLATE "C"` to match SPARQL's codepoint
    /// ordering rather than the database's locale collation.
    pub numeric: bool,
}

/// Where a binding sits in the schema. The renderer asks the schema how to turn
/// a stored value into an RDF term; the plan only says which value.
#[derive(Debug, Clone)]
pub struct TermRef {
    pub class_uri: String,
    pub slot_path: Vec<String>,
}

/// One aggregate in the SELECT list.
#[derive(Debug, Clone)]
pub struct MeasureSpec {
    /// The result variable (the `AS` name).
    pub var: String,
    pub func: Measure,
}

/// The aggregate function and its argument.
///
/// The argument lives inside the variant so illegal states are unrepresentable:
/// there is no way to build a `COUNT(*)` that also has an argument, or a `SUM`
/// that has none.
#[derive(Debug, Clone)]
pub enum Measure {
    /// `COUNT(*)` when `arg` is `None`, `COUNT(?v)` when it is a binding index.
    Count {
        arg: Option<usize>,
        distinct: bool,
    },
    Sum {
        arg: usize,
    },
    Avg {
        arg: usize,
    },
    Min {
        arg: usize,
    },
    Max {
        arg: usize,
    },
}

/// `ORDER BY` term. The key says whether it sorts on a projected value or on an
/// aggregate result, which the renderer needs to place it inside or outside the
/// grouping.
#[derive(Debug, Clone)]
pub struct OrderTerm {
    pub key: OrderKey,
    pub desc: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderKey {
    Binding(usize),
    Measure(usize),
}

/// The `Group` node's three interesting parts: grouping variables, the
/// aggregates and the variable each binds, and the pattern being grouped.
struct GroupParts<'a> {
    variables: &'a [Variable],
    aggregates: &'a [(Variable, AggregateExpression)],
    /// The pattern *under* the grouping — where a `BIND` would hide, since it
    /// applies before the grouping does.
    inner: &'a GraphPattern,
}

/// The solution modifiers wrapped around a query's pattern, peeled off in one
/// pass. `group` is `None` for a query that does not aggregate.
struct Peeled<'a> {
    limit: Option<usize>,
    offset: usize,
    distinct: bool,
    order_by: &'a [OrderExpression],
    /// `Extend` nodes, outermost first, as (bound variable, expression).
    ///
    /// Most of these are not computed values at all: spargebra models
    /// `SELECT (COUNT(*) AS ?n)` as a `Group` whose aggregate binds an internal
    /// variable, plus an `Extend` aliasing it to `?n`. So an `Extend` whose
    /// expression is a bare variable is a projection alias, while anything else
    /// (`BIND`, arithmetic over an aggregate) is a real computation the SQL side
    /// does not reproduce.
    extends: Vec<(&'a Variable, &'a Expression)>,
    group: Option<GroupParts<'a>>,
}

/// First `BIND` found inside a pattern, if any.
///
/// A `BIND` before the grouping (`BIND(...) ... GROUP BY ?computed`) sits *under*
/// the `Group` node, so peeling the outer modifiers never sees it. Without this
/// the computed variable would look like an ordinary variable that happens not
/// to be bound to any slot, and the refusal would name the wrong problem.
fn first_bind_variable(pattern: &GraphPattern) -> Option<&Variable> {
    match pattern {
        GraphPattern::Extend { variable, .. } => Some(variable),
        GraphPattern::Filter { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. } => first_bind_variable(inner),
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            first_bind_variable(left).or_else(|| first_bind_variable(right))
        }
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => None,
    }
}

/// Peel solution modifiers from the outside in, stopping at the pattern.
///
/// The modifier order is not fixed by the algebra (`Slice` may wrap `Distinct`
/// which wraps `Project` which wraps `OrderBy`…), so this walks whatever nesting
/// spargebra produced rather than assuming a shape.
fn peel<'a>(pattern: &'a GraphPattern, out: &mut Peeled<'a>) {
    match pattern {
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            out.offset = *start;
            out.limit = *length;
            peel(inner, out);
        }
        GraphPattern::Distinct { inner } | GraphPattern::Reduced { inner } => {
            out.distinct = true;
            peel(inner, out);
        }
        GraphPattern::Project { inner, .. } => peel(inner, out),
        GraphPattern::OrderBy { inner, expression } => {
            out.order_by = expression;
            peel(inner, out);
        }
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            out.extends.push((variable, expression));
            peel(inner, out);
        }
        GraphPattern::Group {
            variables,
            aggregates,
            inner,
        } => {
            out.group = Some(GroupParts {
                variables,
                aggregates,
                inner,
            });
        }
        // Anything else is the pattern itself; the scoper analyses that.
        _ => {}
    }
}

/// Classify `query` for aggregate pushdown.
///
/// Returns `Err` only when the query cannot be parsed or scoped at all — the
/// same errors [`sparql_scope`] reports. A query that parses but cannot be
/// pushed down is a [`Pushdown::Blocked`] verdict, not an error.
pub fn analyse_pushdown(query: &str, schema_view: &SchemaView) -> Result<Pushdown, ScopeError> {
    let parsed = SparqlParser::new()
        .parse_query(query)
        .map_err(|e| ScopeError::ParseError(e.to_string()))?;
    let pattern = match &parsed {
        Query::Select { pattern, .. } => pattern,
        // CONSTRUCT/DESCRIBE/ASK produce graphs or booleans, not solution
        // tables; there is nothing to group.
        _ => return Ok(Pushdown::NotApplicable),
    };

    let mut peeled = Peeled {
        limit: None,
        offset: 0,
        distinct: false,
        order_by: &[],
        extends: Vec::new(),
        group: None,
    };
    peel(pattern, &mut peeled);

    let Some(GroupParts {
        variables: group_vars,
        aggregates,
        inner: group_inner,
    }) = peeled.group
    else {
        return Ok(Pushdown::NotApplicable);
    };

    if let Some(bound) = first_bind_variable(group_inner) {
        return Ok(blocked(
            BlockedCode::UnsupportedExpression,
            format!(
                "?{} is computed by a BIND before the grouping; only values read \
                 directly from stored slots can be grouped or aggregated in SQL",
                bound.as_str()
            ),
            Some(format!("?{}", bound.as_str())),
        ));
    }

    // Sort the Extend nodes into projection aliases (internal aggregate
    // variable → the name the query gave it) and real computations, which are
    // refused: reproducing SPARQL expression semantics in SQL is not something
    // the two routes could be held to.
    let mut aliases: Vec<(&str, &str)> = Vec::new();
    for (variable, expression) in &peeled.extends {
        let is_aggregate_alias = matches!(expression, Expression::Variable(v)
            if aggregates.iter().any(|(agg_var, _)| agg_var == v));
        if !is_aggregate_alias {
            return Ok(blocked(
                BlockedCode::UnsupportedExpression,
                format!(
                    "?{} is computed by the query (BIND, or an expression over an \
                     aggregate); only values read directly from stored slots can be \
                     pushed to SQL",
                    variable.as_str()
                ),
                Some(format!("?{}", variable.as_str())),
            ));
        }
        if let Expression::Variable(inner) = expression {
            aliases.push((inner.as_str(), variable.as_str()));
        }
    }

    // Reuse the star decomposition rather than re-walking the triples: it
    // already resolves classes, slots and join edges.
    let plan = sparql_scope(query, schema_view)?;
    let stars = plan.root.all_stars();
    let joins = plan.root.all_joins();

    if stars.len() != 1 || !joins.is_empty() {
        return Ok(blocked(
            BlockedCode::UnsupportedPattern,
            format!(
                "pushdown currently handles one class per question; this query has {} \
                 scoped classes and {} joins",
                stars.len(),
                joins.len()
            ),
            None,
        ));
    }
    let star = stars[0];

    let mut bindings: Vec<BindingSpec> = Vec::new();

    // Group keys first, so their binding indices are stable and low.
    let mut group_keys: Vec<usize> = Vec::new();
    for var in group_vars {
        match binding_for(var.as_str(), star, schema_view, &mut bindings) {
            Ok(idx) => group_keys.push(idx),
            Err(b) => return Ok(Pushdown::Blocked(b)),
        }
    }

    let mut measures: Vec<MeasureSpec> = Vec::new();
    for (internal_var, aggregate) in aggregates {
        // Report the measure under the name the query gave it, not the internal
        // variable spargebra invented for the aggregate.
        let result_name = aliases
            .iter()
            .find(|(inner, _)| *inner == internal_var.as_str())
            .map(|(_, alias)| *alias)
            .unwrap_or(internal_var.as_str());
        match measure_for(aggregate, result_name, star, schema_view, &mut bindings) {
            Ok(spec) => measures.push(spec),
            Err(b) => return Ok(Pushdown::Blocked(b)),
        }
    }

    let mut order_by: Vec<OrderTerm> = Vec::new();
    for order in peeled.order_by {
        let (expr, desc) = match order {
            OrderExpression::Asc(e) => (e, false),
            OrderExpression::Desc(e) => (e, true),
        };
        let Expression::Variable(v) = expr else {
            return Ok(blocked(
                BlockedCode::UnsupportedExpression,
                "ORDER BY over an expression cannot be pushed to SQL",
                None,
            ));
        };
        let name = v.as_str();
        let key = if let Some(i) = measures.iter().position(|m| m.var == name) {
            OrderKey::Measure(i)
        } else if let Some(i) = bindings.iter().position(|b| b.var == name) {
            OrderKey::Binding(i)
        } else {
            return Ok(blocked(
                BlockedCode::UnscopedBinding,
                format!("ORDER BY ?{name}, which the query neither groups by nor aggregates"),
                Some(format!("?{name}")),
            ));
        };
        order_by.push(OrderTerm { key, desc });
    }

    Ok(Pushdown::Eligible(SolutionSpec {
        bindings,
        group_keys,
        measures,
        order_by,
        distinct: peeled.distinct,
        limit: peeled.limit,
        offset: peeled.offset,
    }))
}

fn blocked(code: BlockedCode, detail: impl Into<String>, at: Option<String>) -> Pushdown {
    Pushdown::Blocked(Blocked::new(code, detail, at))
}

/// Resolve a SPARQL variable to a binding index, adding the binding if this is
/// the first mention.
fn binding_for(
    var_name: &str,
    star: &Star,
    schema_view: &SchemaView,
    bindings: &mut Vec<BindingSpec>,
) -> Result<usize, Blocked> {
    if let Some(i) = bindings.iter().position(|b| b.var == var_name) {
        return Ok(i);
    }

    // The star's subject variable binds the object's own IRI.
    if star.variable == var_name {
        bindings.push(BindingSpec {
            var: var_name.to_owned(),
            star_var: star.variable.clone(),
            slot_path: Vec::new(),
            term_ref: TermRef {
                class_uri: star.class_uri.clone(),
                slot_path: Vec::new(),
            },
            // An object's own IRI is never a number.
            numeric: false,
        });
        return Ok(bindings.len() - 1);
    }

    let slot = star
        .slot_variables
        .iter()
        .find(|(_slot, bound_var)| bound_var.as_str() == var_name)
        .map(|(slot, _)| slot.clone())
        .ok_or_else(|| {
            Blocked::new(
                BlockedCode::UnscopedBinding,
                format!("?{var_name} is not bound to a slot of <{}>", star.class_uri),
                Some(format!("?{var_name}")),
            )
        })?;

    let numeric = is_numeric_slot(&star.class_uri, &slot, schema_view);
    bindings.push(BindingSpec {
        var: var_name.to_owned(),
        star_var: star.variable.clone(),
        slot_path: vec![slot.clone()],
        term_ref: TermRef {
            class_uri: star.class_uri.clone(),
            slot_path: vec![slot],
        },
        numeric,
    });
    Ok(bindings.len() - 1)
}

fn measure_for(
    aggregate: &AggregateExpression,
    result_var: &str,
    star: &Star,
    schema_view: &SchemaView,
    bindings: &mut Vec<BindingSpec>,
) -> Result<MeasureSpec, Blocked> {
    let var = result_var.to_owned();

    match aggregate {
        AggregateExpression::CountSolutions { distinct } => Ok(MeasureSpec {
            var,
            func: Measure::Count {
                arg: None,
                distinct: *distinct,
            },
        }),
        AggregateExpression::FunctionCall {
            name,
            expr,
            distinct,
        } => {
            // Only a plain variable: an expression would have to be evaluated
            // with SPARQL semantics, which the SQL side does not reproduce.
            let Expression::Variable(v) = expr else {
                return Err(Blocked::new(
                    BlockedCode::UnsupportedExpression,
                    "aggregate over an expression; aggregate a variable bound by a \
                     triple pattern instead",
                    None,
                ));
            };
            let arg = binding_for(v.as_str(), star, schema_view, bindings)?;

            let func = match name {
                AggregateFunction::Count => Measure::Count {
                    arg: Some(arg),
                    distinct: *distinct,
                },
                AggregateFunction::Sum | AggregateFunction::Avg => {
                    if !bindings[arg].numeric {
                        return Err(Blocked::new(
                            BlockedCode::NonNumericMeasure,
                            format!(
                                "?{} is not a numeric slot of <{}>",
                                v.as_str(),
                                star.class_uri
                            ),
                            Some(format!("?{}", v.as_str())),
                        ));
                    }
                    if matches!(name, AggregateFunction::Sum) {
                        Measure::Sum { arg }
                    } else {
                        Measure::Avg { arg }
                    }
                }
                AggregateFunction::Min => Measure::Min { arg },
                AggregateFunction::Max => Measure::Max { arg },
                AggregateFunction::GroupConcat { .. }
                | AggregateFunction::Sample
                | AggregateFunction::Custom(_) => {
                    return Err(Blocked::new(
                        BlockedCode::UnsupportedAggregate,
                        "GROUP_CONCAT, SAMPLE and custom aggregates have no defined \
                         result order, so the SQL and oxigraph routes could not be \
                         held to the same answer",
                        None,
                    ));
                }
            };

            Ok(MeasureSpec { var, func })
        }
    }
}

/// Whether a slot's declared range is numeric, so a cast is legal.
///
/// Decided from the schema, never from the stored values: publish
/// range-validates everything that enters the system, so the declared range is
/// the authority.
fn is_numeric_slot(class_uri: &str, slot_name: &str, schema_view: &SchemaView) -> bool {
    let Ok(Some(class_view)) = schema_view.get_class_by_uri(class_uri) else {
        return false;
    };
    class_view
        .slots()
        .iter()
        .find(|s| s.name == slot_name)
        .and_then(|s| s.definition().range.clone())
        .map(|range| NUMERIC_RANGES.contains(&range.as_str()))
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparql_scoper::tests::test_schema_view;

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

    fn analyse(query: &str) -> Pushdown {
        let sv = test_schema_view();
        analyse_pushdown(&format!("{PREFIX}{query}"), &sv).expect("query should parse and scope")
    }

    fn eligible(query: &str) -> SolutionSpec {
        match analyse(query) {
            Pushdown::Eligible(spec) => spec,
            other => panic!("expected Eligible, got {other:?}"),
        }
    }

    fn blocked_code(query: &str) -> BlockedCode {
        match analyse(query) {
            Pushdown::Blocked(b) => {
                // Every refusal must be actionable, not just labelled.
                assert!(!b.detail.is_empty(), "blocked without a detail");
                assert!(!b.instead().is_empty(), "blocked without a rewrite hint");
                b.code
            }
            other => panic!("expected Blocked, got {other:?}"),
        }
    }

    #[test]
    fn plain_query_is_not_applicable() {
        assert!(matches!(
            analyse("SELECT ?s WHERE { ?s a asset360:Signal }"),
            Pushdown::NotApplicable
        ));
    }

    #[test]
    fn construct_is_not_applicable() {
        assert!(matches!(
            analyse("CONSTRUCT { ?s a asset360:Signal } WHERE { ?s a asset360:Signal }"),
            Pushdown::NotApplicable
        ));
    }

    #[test]
    fn count_grouped_by_a_slot() {
        let spec = eligible(
            "SELECT ?name (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; asset360:name ?name } \
             GROUP BY ?name",
        );

        assert_eq!(spec.group_keys.len(), 1);
        let key = &spec.bindings[spec.group_keys[0]];
        assert_eq!(key.var, "name");
        assert_eq!(key.slot_path, vec!["name".to_owned()]);
        assert_eq!(
            key.term_ref.class_uri,
            "https://data.infrabel.be/asset360/Signal"
        );

        assert_eq!(spec.measures.len(), 1);
        assert_eq!(spec.measures[0].var, "n");
        assert!(matches!(
            spec.measures[0].func,
            Measure::Count {
                arg: None,
                distinct: false
            }
        ));
    }

    #[test]
    fn bare_aggregate_has_no_group_keys() {
        // SPARQL returns exactly one row here, even over zero input rows —
        // unlike a SQL GROUP BY, which would return none.
        let spec = eligible("SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal }");

        assert!(spec.group_keys.is_empty());
        assert_eq!(spec.measures.len(), 1);
    }

    #[test]
    fn min_max_over_a_slot() {
        let spec = eligible(
            "SELECT (MIN(?name) AS ?lo) (MAX(?name) AS ?hi) \
             WHERE { ?s a asset360:Signal ; asset360:name ?name }",
        );

        assert_eq!(spec.measures.len(), 2);
        assert!(matches!(spec.measures[0].func, Measure::Min { .. }));
        assert!(matches!(spec.measures[1].func, Measure::Max { .. }));
        // Both measures share one binding for ?name.
        assert_eq!(spec.bindings.len(), 1);
    }

    #[test]
    fn sum_needs_a_numeric_slot() {
        let spec = eligible(
            "SELECT (SUM(?len) AS ?total) WHERE { ?s a asset360:Signal ; asset360:length ?len }",
        );
        assert!(matches!(spec.measures[0].func, Measure::Sum { .. }));

        assert_eq!(
            blocked_code(
                "SELECT (SUM(?name) AS ?total) \
                 WHERE { ?s a asset360:Signal ; asset360:name ?name }"
            ),
            BlockedCode::NonNumericMeasure
        );
    }

    #[test]
    fn order_by_distinguishes_measure_from_binding() {
        let spec = eligible(
            "SELECT ?name (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; asset360:name ?name } \
             GROUP BY ?name ORDER BY DESC(?n) ?name LIMIT 20 OFFSET 5",
        );

        assert_eq!(spec.limit, Some(20));
        assert_eq!(spec.offset, 5);
        assert_eq!(spec.order_by.len(), 2);
        assert_eq!(spec.order_by[0].key, OrderKey::Measure(0));
        assert!(spec.order_by[0].desc);
        assert_eq!(spec.order_by[1].key, OrderKey::Binding(0));
        assert!(!spec.order_by[1].desc);
    }

    #[test]
    fn group_concat_and_sample_are_refused() {
        for query in [
            "SELECT (GROUP_CONCAT(?name) AS ?names) \
             WHERE { ?s a asset360:Signal ; asset360:name ?name }",
            "SELECT (SAMPLE(?name) AS ?one) \
             WHERE { ?s a asset360:Signal ; asset360:name ?name }",
        ] {
            assert_eq!(blocked_code(query), BlockedCode::UnsupportedAggregate);
        }
    }

    #[test]
    fn computed_values_are_refused() {
        assert_eq!(
            blocked_code(
                "SELECT ?upper (COUNT(*) AS ?n) WHERE { \
                 ?s a asset360:Signal ; asset360:name ?name . BIND(UCASE(?name) AS ?upper) } \
                 GROUP BY ?upper"
            ),
            BlockedCode::UnsupportedExpression
        );
    }

    #[test]
    fn multi_class_questions_are_refused_for_now() {
        assert_eq!(
            blocked_code(
                "SELECT ?cn (COUNT(*) AS ?n) WHERE { \
                 ?c a asset360:TunnelComplex ; asset360:hasName ?cn . \
                 ?comp a asset360:CivilEngineeringAsset ; asset360:belongsToTunnelComplex ?c } \
                 GROUP BY ?cn"
            ),
            BlockedCode::UnsupportedPattern
        );
    }

    #[test]
    fn grouping_by_an_unbound_variable_is_refused() {
        // ?other is never bound to a slot of Signal, so SQL has no column for
        // it. Oxigraph would happily return one unbound group.
        assert_eq!(
            blocked_code(
                "SELECT ?other (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal } GROUP BY ?other"
            ),
            BlockedCode::UnscopedBinding
        );
    }

    #[test]
    fn subject_variable_is_a_valid_binding() {
        // COUNT(DISTINCT ?s) counts objects by their own IRI, which lives in
        // an indexed column rather than the JSONB payload.
        let spec = eligible("SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s a asset360:Signal }");

        assert!(matches!(
            spec.measures[0].func,
            Measure::Count {
                arg: Some(_),
                distinct: true
            }
        ));
        assert!(spec.bindings[0].slot_path.is_empty());
    }
}
