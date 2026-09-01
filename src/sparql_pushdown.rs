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

use spargebra::Query;
use spargebra::algebra::{
    AggregateExpression, AggregateFunction, Expression, GraphPattern, OrderExpression,
};
use spargebra::term::Variable;

use linkml_schemaview::schemaview::SchemaView;

use crate::sparql_scoper::{PathBinding, QueryPlan, ScopeError, Star, parse_query, scope_parsed};
use crate::sparql_terms::{TermDescriptor, resolve_column};

/// Verdict for one query.
#[derive(Debug, Clone)]
pub enum Pushdown {
    /// Not a grouping/aggregating query at all — nothing to push down, and
    /// nothing wrong with it. Keeps the existing route.
    NotApplicable,
    /// Aggregate-shaped, but outside the supported subset.
    Blocked(Blocked),
    /// Answerable in SQL — from the [`SolutionSpec`] **and** the [`QueryPlan`]
    /// it was derived from, together.
    ///
    /// The spec says what to project, group and aggregate. Everything about
    /// *which rows* — the classes, their filters, the join edges — lives in the
    /// plan, and a consumer that reads only the spec silently drops every
    /// constraint: `FILTER(?l > 5)` on a `COUNT(*)` produces a spec with no
    /// bindings at all, and for a bare aggregate the spec does not even name
    /// the class.
    ///
    /// So the plan travels with the verdict rather than being fetched again by
    /// a second `sparql_scope` call, which would also re-parse and could in
    /// principle disagree.
    Eligible {
        solution: SolutionSpec,
        plan: QueryPlan,
    },
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
    /// A hint more specific than the code's own, when the cause is known more
    /// precisely than the code. `IncompletePlan` covers nine distinct causes,
    /// and a hint listing all nine rewrites is a hint for none of them.
    instead: Option<&'static str>,
}

impl Blocked {
    fn new(code: BlockedCode, detail: impl Into<String>, at: Option<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
            at,
            instead: None,
        }
    }

    /// A supported shape to use instead — the cause's own hint where there is
    /// one, else the code's.
    pub fn instead(&self) -> &'static str {
        self.instead.unwrap_or_else(|| self.code.instead())
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
    /// `UNION`, `MINUS`, property paths, subqueries, or unconnected classes.
    UnsupportedPattern,
    /// The plan does not describe the whole query, so answering from it would
    /// answer a *different* question — usually a weaker one.
    ///
    /// Distinct from `UnsupportedPattern` because the cause is not the shape of
    /// the query but what the planner had to leave out of it: a `FILTER` it
    /// cannot express, a triple whose subject is not a scoped class, a
    /// sub-`SELECT`, a `FILTER` inside `OPTIONAL`.
    IncompletePlan,
    /// `HAVING`: a condition on the grouped rows.
    ///
    /// Its own code because its rewrite is its own: every other unsupported
    /// pattern is a shape to be written differently, and this one is a feature
    /// SQL has and this does not translate yet. Sending the generic pattern
    /// hint told an author to avoid UNION in a query with no UNION in it.
    UnsupportedHaving,
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
            Self::UnsupportedHaving => "unsupported_having",
            Self::IncompletePlan => "incomplete_plan",
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
            Self::IncompletePlan => {
                // Only reached if a cause somehow carries no hint of its own;
                // `Inexact::instead()` is the real answer for this code.
                "Part of this question cannot be turned into SQL, so it cannot \
                 be aggregated in the database."
            }
            Self::UnsupportedHaving => {
                "Ask for the groups without HAVING and drop the ones you do \
                 not want from the result — the counts are already there."
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
    /// The variables the query asks for, in `SELECT` order.
    ///
    /// Not every binding or measure is projected. A variable may exist only to
    /// be grouped by, and an aggregate written as `ORDER BY DESC(COUNT(*))` has
    /// no `AS` name at all — spargebra invents an internal one, and emitting it
    /// would hand the caller a column named after a hash. Anything absent from
    /// this list is machinery, not an answer.
    pub projected: Vec<String>,
}

/// One projected variable, resolved to where its value lives.
#[derive(Debug, Clone)]
pub struct BindingSpec {
    /// SPARQL variable name, used only to label the result column.
    pub var: String,
    /// The star's variable, which maps to a table alias.
    pub star_var: String,
    /// Path from the object root to the value: one slot per step.
    pub slot_path: Vec<String>,
    /// Parallel to `slot_path`: whether each step holds a collection rather
    /// than a single value.
    ///
    /// RDF gives one triple per element of a collection, so a query over a
    /// multivalued slot has one solution per element — which a consumer must
    /// reproduce by unnesting, or the counts come out as one-per-record. Kept
    /// per step because any hop along a path may be a collection, and each one
    /// multiplies the rows.
    pub containers: Vec<Container>,
    /// Class IRI the slot path is resolved against — the schema position, not a
    /// copy of the rendering decision, which lives in `descriptor`.
    pub class_uri: String,
    /// How this column's stored text becomes an RDF term, resolved from the
    /// schema once per column.
    ///
    /// Carried here rather than left to the renderer because it is schema
    /// knowledge, and the renderer needs it twice over: to answer in the same
    /// terms the oxigraph route would, and to know whether a column casts
    /// (numeric) or compares under `COLLATE "C"` (text, matching SPARQL's
    /// codepoint ordering rather than the database's locale collation).
    pub descriptor: TermDescriptor,
}

/// How one step of a path is stored, mirroring the schema's own three-way
/// distinction rather than inventing a fourth vocabulary
/// (`SlotContainerMode::{SingleValue, List, Mapping}` upstream).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Container {
    /// One value.
    Single,
    /// A JSON array: unnest its elements.
    List,
    /// A JSON object keyed by the range's identifier: unnest its values. The
    /// turtle writer iterates the values too, so the keys are not part of the
    /// graph.
    Mapping,
}

impl Container {
    /// Stable string form for the Python boundary.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::List => "list",
            Self::Mapping => "mapping",
        }
    }

    /// Map the schema's own three-way distinction to the string form the
    /// Python boundary uses. Not a parallel vocabulary — a rendering of one.
    fn from_mode(mode: &linkml_schemaview::slotview::SlotContainerMode) -> Self {
        use linkml_schemaview::slotview::SlotContainerMode;
        match mode {
            SlotContainerMode::SingleValue => Self::Single,
            SlotContainerMode::List => Self::List,
            SlotContainerMode::Mapping => Self::Mapping,
        }
    }
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

impl Measure {
    /// `count(*)`, `count(distinct #2)`, `sum(#0)` -- the argument as the
    /// binding index it is, since the plan prints the bindings beside it.
    pub fn render(&self) -> String {
        match self {
            Self::Count { arg: None, .. } => "count(*)".to_owned(),
            Self::Count {
                arg: Some(arg),
                distinct,
            } => {
                let distinct = if *distinct { "distinct " } else { "" };
                format!("count({distinct}#{arg})")
            }
            Self::Sum { arg } => format!("sum(#{arg})"),
            Self::Avg { arg } => format!("avg(#{arg})"),
            Self::Min { arg } => format!("min(#{arg})"),
            Self::Max { arg } => format!("max(#{arg})"),
        }
    }
}

impl std::fmt::Display for OrderTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key = match self.key {
            OrderKey::Binding(index) => format!("binding #{index}"),
            OrderKey::Measure(index) => format!("measure #{index}"),
        };
        write!(f, "{key} {}", if self.desc { "desc" } else { "asc" })
    }
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
    /// Whether a `FILTER` sits *above* the grouping -- which is what `HAVING`
    /// is. Peeling stops at the `Group`, so any filter reached here is one.
    having: bool,
    /// Variables from the outermost `Project` — what the query asks for.
    projection: Vec<String>,
    /// Two Slices on one path, which would need composing rather than
    /// overwriting.
    nested_slice: bool,
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
            // A second Slice would have to be *composed* with this one, and
            // the composition is not obvious — an inner LIMIT applies before an
            // outer OFFSET. Nested slices only come from sub-queries, which are
            // refused as inexact before this runs, so rather than carry
            // arithmetic that is unreachable and would be wrong if it ever ran,
            // record the nesting and refuse.
            if out.limit.is_some() || out.offset != 0 {
                out.nested_slice = true;
            }
            out.offset = *start;
            out.limit = *length;
            peel(inner, out);
        }
        GraphPattern::Distinct { inner } | GraphPattern::Reduced { inner } => {
            out.distinct = true;
            peel(inner, out);
        }
        GraphPattern::Project { inner, variables } => {
            // The outermost projection is the query's; a nested one belongs to
            // a subquery, which is refused before this point.
            if out.projection.is_empty() {
                out.projection = variables.iter().map(|v| v.as_str().to_owned()).collect();
            }
            peel(inner, out)
        }
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
        GraphPattern::Filter { inner, .. } => {
            // A `FILTER` in the WHERE clause lives inside the group's own
            // inner pattern, which this never descends into. One out here
            // constrains the grouped rows, and that is a HAVING.
            out.having = true;
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
    // Parse once, with the shared parser, and hand the result to the scoper.
    // A parser of its own here would accept a different language: the shared
    // one preloads prefixes a caller may leave implicit, so a query without
    // `PREFIX asset360:` would scope fine and then fail to parse here — a
    // syntax error on a query that works.
    let parsed = parse_query(query)?;
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
        having: false,
        projection: Vec::new(),
        nested_slice: false,
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

    if peeled.having {
        // Reported rather than ignored. Left as `NotApplicable` -- which is
        // what a query with no aggregate at all returns -- a HAVING fell
        // through to the engine, and on any class worth grouping the engine
        // cannot answer: 30 seconds, then a triple limit, about a query the
        // planner could have named. A SQL `HAVING` would express it, and that
        // is a feature rather than a rewrite; until then, say so.
        return Ok(blocked(
            BlockedCode::UnsupportedHaving,
            "HAVING filters the grouped rows, which this does not translate yet",
            None,
        ));
    }

    if peeled.nested_slice {
        return Ok(blocked(
            BlockedCode::UnsupportedPattern,
            "the query has nested LIMIT/OFFSET clauses, which have to be \
             composed rather than applied independently",
            None,
        ));
    }

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
    // already resolves classes, slots and join edges. Passing the parsed query
    // keeps this to one parse per call.
    let plan = scope_parsed(&parsed, schema_view)?;

    // The plan is built to decide what to *load*, and every extraction step is
    // lossy in the safe direction: a constraint it cannot express is dropped,
    // the fetch widens, and oxigraph re-applies the real query afterwards. Used
    // as an exact plan, that same loss is a plausible wrong number with no
    // error — a dropped `FILTER(REGEX(...))` counts every row, a triple whose
    // subject was never scoped counts one per group.
    //
    // So the question to ask is not "did I recognise enough of this query" but
    // "did the planner drop anything at all".
    if let Some(cause) = plan.inexact {
        // The cause travels with the refusal: it names what was dropped and the
        // one rewrite that fixes it, rather than a list of every rewrite that
        // might.
        return Ok(Pushdown::Blocked(Blocked {
            code: BlockedCode::IncompletePlan,
            detail: cause.detail().to_owned(),
            at: None,
            instead: Some(cause.instead()),
        }));
    }

    let stars = plan.root.all_stars();
    let joins = plan.root.all_joins();

    if stars.is_empty() {
        return Ok(blocked(
            BlockedCode::UnsupportedPattern,
            "no class is scoped, so there is nothing to group over",
            None,
        ));
    }

    // Several classes are fine when join edges connect them all: each edge
    // becomes a SQL JOIN on `right.object_data->>slot = left.asset360_uri`.
    // A class with no path to the others is independent, and the SQL would be a
    // cross product where oxigraph evaluates two unrelated patterns — different
    // questions, so refuse rather than answer the wrong one.
    //
    // Counting edges is not enough: three classes with two edges *between the
    // same two of them* leaves the third disconnected while the count looks
    // right. So walk the graph.
    if let Some(isolated) = disconnected_star(&stars, &joins) {
        return Ok(blocked(
            BlockedCode::UnsupportedPattern,
            format!(
                "?{isolated} shares no reference with the other classes in this \
                 question, so joining them would multiply unrelated rows"
            ),
            Some(format!("?{isolated}")),
        ));
    }

    // An OPTIONAL *edge* between two mandatory classes does not relate them:
    // `{ ?s a Signal . ?t a Track . OPTIONAL { ?s :locatedOnTrack ?t } }` asks
    // for every Signal paired with every Track, and rendering the edge as a
    // join answers a much smaller question. The star-level check below misses
    // this because neither star is optional — only the edge is — and the edge
    // also makes the two look connected.
    if let Some(edge) = joins
        .iter()
        .find(|join| join.join_type == crate::sparql_scoper::JoinType::Left)
    {
        return Ok(blocked(
            BlockedCode::UnsupportedPattern,
            format!(
                "the reference between ?{} and ?{} is inside an OPTIONAL, so the \
                 two classes are not required to be related and the answer \
                 pairs every one with every other",
                edge.left, edge.right
            ),
            Some(format!("?{}", edge.right)),
        ));
    }

    // A left-joined star can contribute unbound rows, which changes what the
    // aggregates count. Supported for the values *inside* a star, not yet for
    // an optional star of its own.
    if let Some(optional) = stars.iter().find(|s| s.is_optional) {
        return Ok(blocked(
            BlockedCode::UnsupportedPattern,
            format!(
                "?{} is introduced inside an OPTIONAL block; grouping across an \
                 optional class is not supported yet",
                optional.variable
            ),
            Some(format!("?{}", optional.variable)),
        ));
    }

    let mut bindings: Vec<BindingSpec> = Vec::new();

    // Group keys first, so their binding indices are stable and low.
    let mut group_keys: Vec<usize> = Vec::new();
    for var in group_vars {
        match binding_for(
            var.as_str(),
            &stars,
            &plan.path_bindings,
            schema_view,
            &mut bindings,
        ) {
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
        match measure_for(
            aggregate,
            result_name,
            &stars,
            &plan.path_bindings,
            schema_view,
            &mut bindings,
        ) {
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

    // Everything the query asks for must be something this can answer;
    // anything it can answer but was not asked for is machinery and stays out
    // of the result.
    let known: Vec<&str> = bindings
        .iter()
        .map(|b| b.var.as_str())
        .chain(measures.iter().map(|m| m.var.as_str()))
        .collect();
    if let Some(missing) = peeled
        .projection
        .iter()
        .find(|var| !known.contains(&var.as_str()))
    {
        return Ok(blocked(
            BlockedCode::UnscopedBinding,
            format!("?{missing} is projected but is neither grouped nor aggregated"),
            Some(format!("?{missing}")),
        ));
    }

    // A multivalued slot read through a variable that nothing groups or
    // aggregates still multiplies solutions: `?s a Signal ; :trafficKinds ?k`
    // with `COUNT(*)` counts one solution *per kind*, while a plan that never
    // mentions ?k counts one row per Signal. `BindingSpec::containers` is how a
    // consumer reproduces that multiplicity, and a variable with no binding has
    // no container and no instruction.
    //
    // Both ways in: a slot read directly off a star, and a hop inside a path.
    let unbound_multivalued = stars
        .iter()
        .flat_map(|star| {
            star.slot_variables.iter().map(move |(slot, var)| {
                (
                    var,
                    star.variable.as_str(),
                    star.class_uri.as_str(),
                    std::slice::from_ref(slot),
                )
            })
        })
        .chain(plan.path_bindings.iter().filter_map(|(var, binding)| {
            let star = stars.iter().find(|s| s.variable == binding.star_var)?;
            Some((
                var,
                star.variable.as_str(),
                star.class_uri.as_str(),
                binding.slot_path.as_slice(),
            ))
        }))
        .find(|(var, star_var, class_uri, slot_path)| {
            if bindings.iter().any(|b| b.var == **var) {
                return false;
            }
            // A step on the way to a value the plan *does* carry is not a loss:
            // that value's `containers` describe every hop of its path,
            // including this one. Only a multiplying read that nothing extends
            // is unaccounted for.
            //
            // On the *same* star, though. Two classes can own slots of the same
            // name — `Signal.documents` and `Track.documents` — and matching a
            // path by spelling alone let a binding on one excuse an unaccounted
            // multiplication on the other.
            let carried_by_a_value = bindings.iter().any(|b| {
                b.star_var == *star_var
                    && b.slot_path.len() > slot_path.len()
                    && b.slot_path.starts_with(slot_path)
            });
            !carried_by_a_value && path_multiplies(schema_view, class_uri, slot_path)
        });

    if let Some((var, _, _, slot_path)) = unbound_multivalued {
        return Ok(blocked(
            BlockedCode::UnsupportedPattern,
            format!(
                "?{var} reads {}, which holds several values per record, and the \
                 question neither groups by it nor aggregates it — so each \
                 record stands for as many solutions as it has values",
                slot_path.join(".")
            ),
            Some(format!("?{var}")),
        ));
    }

    Ok(Pushdown::Eligible {
        solution: SolutionSpec {
            bindings,
            group_keys,
            measures,
            order_by,
            distinct: peeled.distinct,
            limit: peeled.limit,
            offset: peeled.offset,
            projected: peeled.projection,
        },
        plan,
    })
}

/// A star that no chain of join edges connects to the first one, if any.
///
/// The walk itself is [`crate::sparql_scoper::stars_reachable_from`], shared
/// with the scoper's disconnected-OPTIONAL check so that "connected" means one
/// thing in this crate.
fn disconnected_star(stars: &[&Star], joins: &[&crate::sparql_scoper::JoinEdge]) -> Option<String> {
    let first = stars.first()?;
    let edges: Vec<(&str, &str)> = joins
        .iter()
        .map(|join| (join.left.as_str(), join.right.as_str()))
        .collect();
    let reached = crate::sparql_scoper::stars_reachable_from(
        std::iter::once(first.variable.as_str()),
        &edges,
    );

    stars
        .iter()
        .find(|star| !reached.contains(star.variable.as_str()))
        .map(|star| star.variable.clone())
}

/// Whether any step of this path holds more than one value per record.
///
/// Asked of the slot rather than of its term descriptor: a multivalued *inlined*
/// range has no describable term (it serialises as a blank node), so a
/// descriptor-shaped question answers "no container" for `documents` — which
/// reads as single-valued and is how this multiplicity stayed invisible.
fn path_multiplies(schema_view: &SchemaView, class_uri: &str, slot_path: &[String]) -> bool {
    use linkml_schemaview::identifier::Identifier;
    use linkml_schemaview::slotview::SlotContainerMode;

    let Ok(Some(mut class_view)) = schema_view.get_class_by_uri(class_uri) else {
        return false;
    };
    for (i, slot_name) in slot_path.iter().enumerate() {
        let Some(slot) = class_view.slot(&Identifier::Name(slot_name.clone())) else {
            return false;
        };
        if slot.determine_slot_container_mode() != SlotContainerMode::SingleValue {
            return true;
        }
        if i + 1 == slot_path.len() {
            return false;
        }
        match slot.get_range_class() {
            Some(next) => class_view = next,
            None => return false,
        }
    }
    false
}

fn blocked(code: BlockedCode, detail: impl Into<String>, at: Option<String>) -> Pushdown {
    Pushdown::Blocked(Blocked::new(code, detail, at))
}

/// Resolve a SPARQL variable to a binding index, adding the binding if this is
/// the first mention.
fn binding_for(
    var_name: &str,
    stars: &[&Star],
    path_bindings: &std::collections::HashMap<String, PathBinding>,
    schema_view: &SchemaView,
    bindings: &mut Vec<BindingSpec>,
) -> Result<usize, Blocked> {
    if let Some(i) = bindings.iter().position(|b| b.var == var_name) {
        return Ok(i);
    }

    // Which star owns this variable, and where in it the value lives. One
    // resolution: which route matched and where the value sits are the same
    // question, and answering it twice is how the second refusal below became
    // unreachable.
    let (star, slot_path) = stars
        .iter()
        .copied()
        .find_map(|s| {
            // The star's subject variable binds the object's own IRI, which is
            // the empty path — resolve_column maps that to subject_iri().
            if s.variable == var_name {
                return Some((s, Vec::new()));
            }
            if let Some((slot, _)) = s
                .slot_variables
                .iter()
                .find(|(_slot, bound_var)| bound_var.as_str() == var_name)
            {
                return Some((s, vec![slot.clone()]));
            }
            // Or inside one of the star's nested structures —
            // `?s :location ?l . ?l :longitude ?v` binds ?v two slots down.
            path_bindings
                .get(var_name)
                .filter(|b| b.star_var == s.variable)
                .map(|b| (s, b.slot_path.clone()))
        })
        .ok_or_else(|| {
            Blocked::new(
                BlockedCode::UnscopedBinding,
                format!("?{var_name} is not bound by any scoped class in this query"),
                Some(format!("?{var_name}")),
            )
        })?;

    // A required and an optional nested read look identical in the plan and
    // have different answers: required excludes the records that lack the
    // value, optional keeps them with the variable unbound. Until the plan can
    // say which, refuse rather than pick one.
    if let Some(binding) = path_bindings.get(var_name)
        && binding.star_var == star.variable
        && binding.optional
    {
        return Err(Blocked::new(
            BlockedCode::IncompletePlan,
            format!(
                "?{var_name} is read inside an OPTIONAL through a nested path, \
                 which the plan cannot tell apart from a required read"
            ),
            Some(format!("?{var_name}")),
        ));
    }

    let (descriptor, container_modes) = resolve_column(schema_view, &star.class_uri, &slot_path)
        .ok_or_else(|| {
        Blocked::new(
            BlockedCode::UnscopedBinding,
            format!(
                "?{var_name} resolves to {slot_path:?} on <{}>, which the schema cannot describe \
                 as an RDF term",
                star.class_uri
            ),
            Some(format!("?{var_name}")),
        )
    })?;

    let containers: Vec<Container> = container_modes.iter().map(Container::from_mode).collect();
    bindings.push(BindingSpec {
        var: var_name.to_owned(),
        star_var: star.variable.clone(),
        slot_path,
        containers,
        class_uri: star.class_uri.clone(),
        descriptor,
    });
    Ok(bindings.len() - 1)
}

fn measure_for(
    aggregate: &AggregateExpression,
    result_var: &str,
    stars: &[&Star],
    path_bindings: &std::collections::HashMap<String, PathBinding>,
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
            let arg = binding_for(v.as_str(), stars, path_bindings, schema_view, bindings)?;

            let func = match name {
                AggregateFunction::Count => Measure::Count {
                    arg: Some(arg),
                    distinct: *distinct,
                },
                AggregateFunction::Sum | AggregateFunction::Avg => {
                    if !bindings[arg].descriptor.numeric {
                        return Err(Blocked::new(
                            BlockedCode::NonNumericMeasure,
                            format!(
                                "?{} is not a numeric slot of <{}>",
                                v.as_str(),
                                bindings[arg].class_uri
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
            Pushdown::Eligible { solution, .. } => solution,
            other => panic!("expected Eligible, got {other:?}"),
        }
    }

    /// The plan that travels with an eligible verdict — where the filters,
    /// classes and join edges live.
    fn eligible_plan(query: &str) -> crate::sparql_scoper::QueryPlan {
        match analyse(query) {
            Pushdown::Eligible { plan, .. } => plan,
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

    /// The endpoint's parser preloads `asset360:`, so a query may leave it
    /// implicit — and every other test here prepends PREFIX, which is exactly
    /// how a parser mismatch stays hidden. With a parser of its own, this
    /// query scoped fine and then failed to parse in the analyser: a syntax
    /// error on a query that works, surfacing as a 500.
    #[test]
    fn implicit_prefixes_parse_the_same_as_the_scoper() {
        let sv = test_schema_view();
        let query = "SELECT ?name (COUNT(*) AS ?n) WHERE { \
                     ?s a asset360:Signal ; asset360:name ?name } GROUP BY ?name";

        // Both entry points must accept it, or they accept different languages.
        crate::sparql_scoper::sparql_scope(query, &sv).expect("scoper accepts it");
        let verdict = analyse_pushdown(query, &sv).expect("analyser accepts it");
        assert!(
            matches!(verdict, Pushdown::Eligible { .. }),
            "got {verdict:?}"
        );
    }

    #[test]
    fn an_update_is_rejected_not_misread() {
        // Shared parsing means the analyser reports an Update as an Update,
        // rather than as a syntax error or (worse) a query with nothing to
        // group.
        let sv = test_schema_view();
        let err = analyse_pushdown(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             INSERT DATA { <urn:a> a asset360:Signal }",
            &sv,
        )
        .expect_err("an update must not be planned");
        assert!(matches!(err, ScopeError::UpdateRejected), "got {err:?}");
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
        assert_eq!(key.class_uri, "https://data.infrabel.be/asset360/Signal");

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
    fn groups_by_a_value_inside_a_nested_structure() {
        let spec = eligible(
            "SELECT ?lon (COUNT(*) AS ?n) WHERE { \
             ?s a asset360:Signal ; asset360:location ?loc . \
             ?loc asset360:longitude ?lon } GROUP BY ?lon",
        );

        let key = &spec.bindings[spec.group_keys[0]];
        assert_eq!(key.slot_path, vec!["location", "longitude"]);
        // Two slots down, and still numeric — the descriptor followed the path.
        assert!(key.descriptor.numeric);
    }

    #[test]
    fn grouping_by_the_nested_structure_itself_is_refused() {
        // ?loc stands for the structure, which serialises as a blank node: SQL
        // cannot reproduce the label, and neither can a second oxigraph run.
        assert_eq!(
            blocked_code(
                "SELECT ?loc (COUNT(*) AS ?n) WHERE { \
                 ?s a asset360:Signal ; asset360:location ?loc } GROUP BY ?loc"
            ),
            BlockedCode::UnscopedBinding
        );
    }

    /// Every one of these scoped fine and came back eligible, and every one
    /// would have answered a *weaker* question than it was asked: the planner
    /// drops what it cannot express, because for a prefetch that only means
    /// over-fetching and oxigraph re-applies the query afterwards.
    #[test]
    fn a_plan_that_drops_part_of_the_query_is_refused() {
        for (label, query) in [
            // FILTER shapes the extractor does not handle. Dropped, the SQL
            // counts every row of the class.
            (
                "!=",
                "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
                    asset360:name ?nm . FILTER(?nm != \"BX517\") }",
            ),
            (
                "REGEX",
                "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
                       asset360:name ?nm . FILTER(REGEX(?nm, \"^BX\")) }",
            ),
            (
                "||",
                "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
                    asset360:name ?nm . FILTER(?nm = \"a\" || ?nm = \"b\") }",
            ),
            (
                "BOUND",
                "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
                       asset360:name ?nm . FILTER(BOUND(?nm)) }",
            ),
            // Half of a conjunction landing is still a weaker filter.
            (
                "partial &&",
                "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
                            asset360:name ?nm ; asset360:length ?l . \
                            FILTER(?l > 5 && REGEX(?nm, \"^BX\")) }",
            ),
            // A FILTER inside OPTIONAL must not reach the fetch at all: it
            // would drop the rows the LEFT JOIN exists to preserve.
            (
                "FILTER in OPTIONAL",
                "SELECT (COUNT(*) AS ?n) WHERE { \
                                    ?s a asset360:Signal . \
                                    OPTIONAL { ?s asset360:length ?l . FILTER(?l > 5) } }",
            ),
            // A subject with no rdf:type has no star, so its triples vanish
            // and the count collapses to one per group.
            (
                "untyped subject",
                "SELECT ?t (COUNT(*) AS ?n) WHERE { \
                                 ?sig asset360:locatedOnTrack ?t . \
                                 ?t a asset360:Track } GROUP BY ?t",
            ),
            // A sub-SELECT's own LIMIT never reaches the plan.
            (
                "sub-SELECT",
                "SELECT (COUNT(*) AS ?n) WHERE { \
                            { SELECT ?s WHERE { ?s a asset360:Signal } LIMIT 5 } }",
            ),
            // Drop sites an after-the-fact check missed: it inspected triple
            // subjects, and each of these loses a triple for another reason.
            (
                "unknown predicate",
                "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal . \
                 ?s <urn:unknown> \"x\" }",
            ),
            (
                "variable predicate",
                "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal . ?s ?p \"x\" }",
            ),
            (
                "constant in OPTIONAL",
                "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal . \
                 OPTIONAL { ?s asset360:name \"BX\" } }",
            ),
            (
                "VALUES on unbound var",
                "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal . \
                 VALUES ?zz { \"a\" } }",
            ),
        ] {
            assert_eq!(
                blocked_code(query),
                BlockedCode::IncompletePlan,
                "should be refused as incomplete: {label}"
            );
        }
    }

    #[test]
    fn an_ordering_only_aggregate_is_not_projected() {
        // `ORDER BY DESC(COUNT(*))` creates an aggregate with no AS name;
        // spargebra invents an internal one. Emitting it would hand the caller
        // a column named after a hash.
        let spec = eligible(
            "SELECT ?name WHERE { ?s a asset360:Signal ; asset360:name ?name } \
             GROUP BY ?name ORDER BY DESC(COUNT(*))",
        );

        assert_eq!(spec.projected, vec!["name".to_owned()]);
        assert_eq!(
            spec.measures.len(),
            1,
            "the aggregate still has to be computed"
        );
        assert!(
            !spec.projected.contains(&spec.measures[0].var),
            "an internal aggregate name must not be projected: {:?}",
            spec.measures[0].var
        );
    }

    #[test]
    fn the_projection_is_captured_in_select_order() {
        let spec = eligible(
            "SELECT ?name (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?name } GROUP BY ?name",
        );
        assert_eq!(spec.projected, vec!["name".to_owned(), "n".to_owned()]);
    }

    #[test]
    fn a_pushable_filter_still_works() {
        // The completeness check must not refuse what it can express.
        let spec = eligible(
            "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; asset360:length ?l . \
             FILTER(?l > 5 && ?l <= 20) }",
        );
        assert_eq!(spec.measures.len(), 1);
    }

    /// An eligible verdict is not a description of the query on its own: the
    /// constraints live in the plan. `FILTER(?l > 5)` on a `COUNT(*)` produces
    /// a solution with no bindings at all, so a consumer reading only the
    /// solution would count every row.
    #[test]
    fn the_plan_travels_with_an_eligible_verdict() {
        let query = "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
                     asset360:length ?l . FILTER(?l > 5) }";

        let spec = eligible(query);
        assert!(
            spec.bindings.is_empty(),
            "nothing is projected, so the filter cannot be in the solution"
        );

        let plan = eligible_plan(query);
        let star = &plan.root.all_stars()[0];
        assert_eq!(
            star.class_uri, "https://data.infrabel.be/asset360/Signal",
            "the class is only knowable from the plan"
        );
        assert!(
            star.filters.contains_key("length"),
            "the filter must reach the consumer through the plan: {:?}",
            star.filters
        );
    }

    /// An OPTIONAL between two mandatory classes does not relate them: the
    /// answer pairs every one with every other. The star-level check missed
    /// this because neither star is optional — only the edge — and the edge
    /// also made them look connected.
    #[test]
    fn an_optional_reference_between_classes_is_refused() {
        assert_eq!(
            blocked_code(
                "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal . \
                 ?t a asset360:Track . OPTIONAL { ?s asset360:locatedOnTrack ?t } }"
            ),
            BlockedCode::UnsupportedPattern
        );
    }

    /// A multivalued read nobody grouped or aggregated multiplies solutions,
    /// and a plan that never mentions the variable counts records instead.
    /// Measured against oxigraph: two Signals with two and three kinds answer
    /// 5, while the plan describes 2.
    #[test]
    fn an_unaggregated_multivalued_read_is_refused() {
        for query in [
            "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:trafficKinds ?k }",
            "SELECT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm ; asset360:trafficKinds ?k } GROUP BY ?nm",
            // A multivalued *inlined* range has no describable term, so asking
            // its descriptor answered "single-valued" and this counted records
            // where SPARQL counts documents.
            "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:documents ?d }",
            // Same multiplicity one hop in: the leaf is describable, the hop
            // that multiplies is not the leaf.
            "SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:documents ?d . ?d asset360:title ?ti }",
            // Two classes can own a slot of the same name. Matching the carried
            // path by spelling alone let ?ti's binding on the Track star excuse
            // ?d1's unaccounted multiplication on the Signal star.
            "SELECT (COUNT(?ti) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:documents ?d1 ; asset360:locatedOnTrack ?t . \
             ?t a asset360:Track ; asset360:documents ?d2 . \
             ?d2 asset360:title ?ti }",
        ] {
            assert_eq!(
                blocked_code(query),
                BlockedCode::UnsupportedPattern,
                "should refuse: {query}"
            );
        }

        // Grouping by it *is* answerable — the multiplicity is then the answer,
        // and `containers` tells the consumer how to reproduce it.
        let spec = eligible(
            "SELECT ?k (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:trafficKinds ?k } GROUP BY ?k",
        );
        assert_eq!(spec.group_keys.len(), 1);
    }

    /// A required and an optional nested read are the same slots and different
    /// answers — required excludes the records that lack the value, optional
    /// keeps them unbound — so the plan must not present them identically.
    #[test]
    fn an_optional_nested_path_is_refused_while_a_required_one_is_not() {
        assert_eq!(
            blocked_code(
                "SELECT ?lon (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal . \
                 OPTIONAL { ?s asset360:location ?loc . ?loc asset360:longitude ?lon } } \
                 GROUP BY ?lon"
            ),
            BlockedCode::IncompletePlan
        );

        // The required form still works, and is the shape reports use.
        let spec = eligible(
            "SELECT ?lon (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:location ?loc . ?loc asset360:longitude ?lon } GROUP BY ?lon",
        );
        assert_eq!(
            spec.bindings[spec.group_keys[0]].slot_path,
            vec!["location", "longitude"]
        );
    }

    #[test]
    fn a_class_sharing_no_reference_is_refused() {
        // Three classes, two edges — but both edges are between the same two, so
        // the third is a cross product. Counting edges said this was fine.
        assert_eq!(
            blocked_code(
                "SELECT ?cn (COUNT(*) AS ?n) WHERE { \
                 ?c a asset360:TunnelComplex ; asset360:hasName ?cn . \
                 ?comp a asset360:CivilEngineeringAsset ; \
                 asset360:belongsToTunnelComplex ?c ; asset360:hasName ?compn . \
                 ?sig a asset360:Signal } \
                 GROUP BY ?cn"
            ),
            BlockedCode::UnsupportedPattern
        );
    }

    #[test]
    fn multivalued_and_mapping_containers_are_carried() {
        // Container::List and ::Mapping had no test at all, so nothing said
        // which unnest a consumer owes — and getting that wrong is a silent
        // count error.
        let spec = eligible(
            "SELECT ?kind (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:trafficKinds ?kind } GROUP BY ?kind",
        );
        let key = &spec.bindings[spec.group_keys[0]];
        assert_eq!(key.containers, vec![Container::List]);

        let spec = eligible(
            "SELECT ?doc (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:documents ?d . ?d asset360:title ?doc } GROUP BY ?doc",
        );
        let key = &spec.bindings[spec.group_keys[0]];
        assert_eq!(
            key.containers,
            vec![Container::Mapping, Container::Single],
            "a mapping hop unnests with jsonb_each, and its keys are not in the graph"
        );
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
    fn groups_across_a_reference_between_classes() {
        // "components per tunnel complex": two classes, connected by a
        // reference, which becomes a SQL JOIN.
        let spec = eligible(
            "SELECT ?cn (COUNT(*) AS ?n) WHERE { \
             ?c a asset360:TunnelComplex ; asset360:hasName ?cn . \
             ?comp a asset360:CivilEngineeringAsset ; asset360:belongsToTunnelComplex ?c } \
             GROUP BY ?cn",
        );

        let key = &spec.bindings[spec.group_keys[0]];
        assert_eq!(key.var, "cn");
        // The group key belongs to the complex, not to the component — a
        // binding names its own star so the renderer reads the right alias.
        assert_eq!(key.star_var, "c");
        assert_eq!(key.slot_path, vec!["hasName"]);
    }

    #[test]
    fn measures_can_come_from_the_other_class() {
        let spec = eligible(
            "SELECT ?cn (COUNT(?compName) AS ?n) WHERE { \
             ?c a asset360:TunnelComplex ; asset360:hasName ?cn . \
             ?comp a asset360:CivilEngineeringAsset ; asset360:belongsToTunnelComplex ?c ; \
             asset360:hasName ?compName } \
             GROUP BY ?cn",
        );

        let arg = match spec.measures[0].func {
            Measure::Count { arg: Some(i), .. } => i,
            ref other => panic!("expected COUNT(?compName), got {other:?}"),
        };
        assert_eq!(spec.bindings[arg].star_var, "comp");
    }

    #[test]
    fn unconnected_classes_are_refused() {
        // Without a reference between them the SQL would be a cross product,
        // where oxigraph evaluates two independent patterns. Different
        // questions, so answering either one would be wrong.
        assert_eq!(
            blocked_code(
                "SELECT ?cn (COUNT(*) AS ?n) WHERE { \
                 ?c a asset360:TunnelComplex ; asset360:hasName ?cn . \
                 ?sig a asset360:Signal } \
                 GROUP BY ?cn"
            ),
            BlockedCode::UnsupportedPattern
        );
    }

    #[test]
    fn an_optional_class_is_refused() {
        assert_eq!(
            blocked_code(
                "SELECT ?cn (COUNT(*) AS ?n) WHERE { \
                 ?c a asset360:TunnelComplex ; asset360:hasName ?cn . \
                 OPTIONAL { ?comp a asset360:CivilEngineeringAsset ; \
                 asset360:belongsToTunnelComplex ?c } } \
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
    /// A `HAVING` used to read as "no aggregate here": `peel` never handled a
    /// `Filter`, so the `Group` beneath it was never found. The query then
    /// went to the engine, which on a real class spends thirty seconds before
    /// reporting a triple limit -- for a shape the planner can name up front.
    #[test]
    fn having_is_refused_rather_than_unrecognised() {
        let sv = crate::sparql_scoper::tests::test_schema_view();
        let verdict = analyse_pushdown(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?n (COUNT(*) AS ?c) WHERE { ?s a asset360:Signal ; \
             asset360:name ?n } GROUP BY ?n HAVING (COUNT(*) > 1)",
            &sv,
        )
        .unwrap();

        match verdict {
            Pushdown::Blocked(blocked) => {
                assert_eq!(blocked.code, BlockedCode::UnsupportedHaving);
                assert!(blocked.detail.contains("HAVING"), "{}", blocked.detail);
                // Its own hint: the generic pattern advice told an author to
                // avoid UNION in a query that contains none.
                assert!(
                    blocked.instead().contains("HAVING"),
                    "the hint must be about HAVING, got: {}",
                    blocked.instead()
                );
            }
            other => panic!("expected a refusal naming HAVING, got {other:?}"),
        }
    }
}
