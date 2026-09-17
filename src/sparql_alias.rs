//! One slot, two spellings — and the planner accepts both.
//!
//! A LinkML slot that declares a `slot_uri` has two legitimate IRIs. The
//! **canonical** one is what the declaration says, and it is what the instance
//! writer puts in the data: on the asset360 datamodel, `Track`'s `name` is
//! written as `<http://rsm.uic.org/RSM12#EAID_080C70AE_7680_4515_B580_0B30E8066364>`.
//! The **native** one is the spelling LinkML derives from the slot's own name
//! in its schema's default prefix — `irsm:name` — and it appears nowhere in the
//! data at all.
//!
//! Nothing there is wrong, but the failure mode is: a query author who writes
//! the readable spelling gets a 200 with a permanently empty column, because
//! inside an `OPTIONAL` an unmatched predicate is not an error. Nobody guesses
//! an EAID, so in practice the slot is unreachable. That is
//! [#447](https://gitlab.pp.kapernikov.com/asset360/consolidator-server/-/issues/447).
//!
//! So this module resolves the alias **in the plan**, once, before either route
//! sees the query: a fixed predicate IRI that names a slot by its native
//! spelling is replaced by that slot's canonical IRI. The SQL leg and the
//! oxigraph leg are then answering the same question by construction rather
//! than by two implementations agreeing — which matters, because the SQL leg
//! resolves a predicate through [`SchemaView::get_slot_by_uri`] (which has
//! always accepted both spellings) while the engine matches the IRI against
//! triples (which only ever carry the canonical one). Left alone, the two
//! routes answer *differently* for the native spelling, and a silent empty
//! column would have become a silent route-dependent answer.
//!
//! # What is deliberately not rewritten
//!
//! * **A variable predicate.** `?s ?p ?o` asks what the data says, and the data
//!   says the canonical IRI. Rewriting nothing here is what keeps the two
//!   routes in step for the open-predicate shapes the differential oracle
//!   asks.
//! * **A predicate inside a `GRAPH` clause.** Named graphs are not instance
//!   data. The only one this endpoint serves is the schema graph, whose
//!   predicates are RDF/RDFS/OWL/SKOS terms and never slots.
//! * **A `CONSTRUCT` template.** Its predicates are *output* spelling, chosen
//!   by the author. Rewriting them would change the graph a client asked to be
//!   given back.
//!
//! # Ambiguity is refused, not resolved
//!
//! Two slots can in principle share one native spelling and disagree about
//! their canonical one — two schemas with the same `default_prefix`, each
//! declaring an attribute of the same name with a different `slot_uri`. There
//! is no defensible pick between them, so the alias is refused by name and the
//! author is told to write the canonical IRI. An IRI that is *itself* some
//! slot's canonical spelling is never rewritten either: it already names a
//! predicate that exists in the data, and preferring the alias reading would
//! lose matches.
//!
//! # A class has two spellings too
//!
//! A class that declares a `class_uri` is written into the data as *that* IRI
//! — its `rdf:type` triple and its `asset_type` column both carry it — and the
//! native spelling, the class's own name in its schema's default prefix,
//! appears in no triple. `?s a irsm:Track` therefore matched nothing on the
//! engine leg while the SQL leg resolved it through the `SchemaView` and
//! scanned. That is
//! [#451](https://gitlab.pp.kapernikov.com/asset360/consolidator-server/-/issues/451),
//! and it is the same gap on the constant object of an `rdf:type` pattern that
//! #447 was on a predicate, closed the same way: the native spelling becomes
//! the declared one on the parse, and the schema graph publishes the pair as
//! `owl:equivalentClass` (see [`crate::sparql_schema_graph`]).
//!
//! # A predicate the class cannot carry is refused
//!
//! The opposite verdict from the same lookup. Once every spelling is the one
//! the data uses, a fixed predicate on a subject typed with a known class
//! either names a slot that class carries or names nothing that any of its
//! records can hold — and a pattern that can never match is not a question,
//! it is a mistake. Left alone it is a 200 with an empty column, or inside an
//! `OPTIONAL` an unbound column on every row, and on the SQL leg it was worse:
//! the statement resolved `asset360:name` by *local name* and read a `Track`'s
//! `name` key, answering `2` where the engine answered `0`. That is
//! [#453](https://gitlab.pp.kapernikov.com/asset360/consolidator-server/-/issues/453).
//!
//! So it is refused at parse time, by name, and the message names the slot
//! the author probably meant — the one on that class with the same slot name
//! under its own namespace, in both its spellings, because the fix a query
//! author needs is the spelling and the `SchemaView` is what knows it. It is
//! the sibling of the untyped-subject refusal in [`crate::sparql_scoper`]: a
//! typed subject with a predicate its class cannot carry.
//!
//! What is *not* refused, because the lookup cannot judge it: a predicate on
//! a subject with no `rdf:type` (the scoper's, see above), on a subject typed
//! with a class the schema does not know (also the scoper's), on a subject
//! typed with two different classes (an intersection the scoper already
//! handles), inside a `GRAPH` or `SERVICE` block, or reached through a
//! property path. A sub-`SELECT` is its own scope: a variable it does not
//! project is not the outer query's, so its subjects are judged on their own.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use linkml_schemaview::schemaview::SchemaView;
use spargebra::Query;
use spargebra::algebra::{Expression, GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};

use crate::sparql_scoper::RDF_TYPE;

/// Which kind of schema element an alias names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AliasKind {
    /// A predicate, i.e. a slot with a `slot_uri`.
    Slot,
    /// The constant object of an `rdf:type`, i.e. a class with a `class_uri`.
    Class,
}

/// An alias that names more than one element, with no way to choose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmbiguousAlias {
    /// What the alias would have named.
    pub kind: AliasKind,
    /// The IRI as the query spelled it.
    pub alias: String,
    /// Every canonical IRI it could have meant, sorted.
    pub canonical: Vec<String>,
}

impl std::fmt::Display for AmbiguousAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (what, element) = match self.kind {
            AliasKind::Slot => ("predicate", "slot"),
            AliasKind::Class => ("class", "class"),
        };
        write!(
            f,
            "{what} <{}> is the native spelling of more than one {element} ({}); \
             write the one you mean",
            self.alias,
            self.canonical
                .iter()
                .map(|c| format!("<{c}>"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl std::error::Error for AmbiguousAlias {}

/// A fixed predicate on a subject whose class carries no such slot.
///
/// The pattern can never match: every record of the class is written with
/// the class's slots and nothing else, so no triple carries this predicate on
/// a subject of this type. See the module docs ("A predicate the class cannot
/// carry is refused").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotASlotOfClass {
    /// The subject as the query wrote it: `?t`, `<iri>` or `_:b`.
    pub subject: String,
    /// The predicate IRI, after alias resolution.
    pub predicate: String,
    /// The class the subject is typed with, as its declared IRI.
    pub class_iri: String,
    /// The class's name in the schema.
    pub class_name: String,
    /// The slot of that class the author most likely meant: the one with the
    /// same slot name as the predicate resolves to, or failing that the same
    /// local name. Its declared IRI and, when it differs, the native spelling
    /// the schema graph publishes as its `owl:equivalentProperty`.
    pub suggestion: Option<SlotSpellings>,
    /// The predicate and the suggestion as CURIEs, where the schema's prefixes
    /// can compress them. For the message only.
    pub curies: HashMap<String, String>,
}

/// One slot's two spellings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotSpellings {
    /// The IRI the data uses.
    pub canonical: String,
    /// The IRI derived from the slot's name in its schema's default prefix,
    /// when it differs from the canonical one.
    pub native: Option<String>,
}

impl NotASlotOfClass {
    /// The CURIE where the schema's prefixes give one, else the bare IRI.
    fn short(&self, iri: &str) -> String {
        match self.curies.get(iri) {
            Some(curie) => curie.clone(),
            None => format!("<{iri}>"),
        }
    }

    /// The CURIE with the IRI it stands for, else the bare IRI.
    fn full(&self, iri: &str) -> String {
        match self.curies.get(iri) {
            Some(curie) => format!("{curie} (<{iri}>)"),
            None => format!("<{iri}>"),
        }
    }
}

impl std::fmt::Display for NotASlotOfClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} is not a slot of {} (<{}>), so `{} {} …` can never match",
            self.full(&self.predicate),
            self.class_name,
            self.class_iri,
            self.subject,
            self.short(&self.predicate),
        )?;
        match &self.suggestion {
            Some(SlotSpellings {
                canonical,
                native: Some(native),
            }) => write!(
                f,
                "; did you mean {}, written in the data as <{canonical}>?",
                self.full(native)
            ),
            Some(SlotSpellings {
                canonical,
                native: None,
            }) => write!(f, "; did you mean {}?", self.full(canonical)),
            None => write!(
                f,
                ". The slots {} carries are the ?p of `GRAPH <schema> {{ ?p \
                 <https://schema.org/domainIncludes> <{}> }}`.",
                self.class_name, self.class_iri
            ),
        }
    }
}

impl std::error::Error for NotASlotOfClass {}

/// Why a query's spellings could not be resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpellingError {
    /// A native spelling that names more than one element.
    Ambiguous(AmbiguousAlias),
    /// A predicate on a subject whose class cannot carry it. Boxed: it
    /// carries the message's spellings, and `Ok` is the common path.
    NotASlotOfClass(Box<NotASlotOfClass>),
}

impl std::fmt::Display for SpellingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpellingError::Ambiguous(inner) => inner.fmt(f),
            SpellingError::NotASlotOfClass(inner) => inner.fmt(f),
        }
    }
}

impl std::error::Error for SpellingError {}

impl From<AmbiguousAlias> for SpellingError {
    fn from(inner: AmbiguousAlias) -> Self {
        SpellingError::Ambiguous(inner)
    }
}

impl From<Box<NotASlotOfClass>> for SpellingError {
    fn from(inner: Box<NotASlotOfClass>) -> Self {
        SpellingError::NotASlotOfClass(inner)
    }
}

impl From<SpellingError> for crate::sparql_scoper::ScopeError {
    /// The endpoint's refusal vocabulary has one code for "well-formed SPARQL
    /// this planner will not plan", and a spelling it refuses is that: the
    /// same code the ambiguity refusal has carried since #447.
    fn from(error: SpellingError) -> Self {
        crate::sparql_scoper::ScopeError::UnsupportedConstruct(error.to_string())
    }
}

/// What one rewrite did, for a log line and for the tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rewrite {
    /// The IRI the author wrote.
    pub from: String,
    /// The IRI the data uses.
    pub to: String,
}

/// Resolve every spelling in the query to the one the data uses, and refuse
/// the ones the data cannot carry.
///
/// Three steps on one parse, in this order: predicates that name a slot by its
/// native spelling become the declared IRI ([`canonicalize_predicates`]); the
/// constant object of an `rdf:type` that names a class by its native spelling
/// becomes the declared IRI ([`canonicalize_classes`]); and then, with every
/// spelling settled, a fixed predicate on a subject typed with a known class
/// that carries no such slot is refused ([`refuse_uncarried_predicates`]).
/// The order matters: the refusal judges canonical spellings, so both aliases
/// have to be resolved before it looks.
///
/// Every entry point that parses a query calls this — the planner, the engine
/// leg and the string scoper — so the three read one query.
///
/// Returns what it rewrote, sorted by the spelling the author wrote.
pub fn canonicalize(
    query: &mut Query,
    schema_view: &SchemaView,
) -> Result<Vec<Rewrite>, SpellingError> {
    let mut rewrites = canonicalize_predicates(query, schema_view)?;
    rewrites.extend(canonicalize_classes(query, schema_view)?);
    rewrites.sort_by(|a, b| a.from.cmp(&b.from));
    refuse_uncarried_predicates(query, schema_view)?;
    Ok(rewrites)
}

/// Rewrite every fixed predicate that names a slot by its native spelling.
///
/// Returns what it changed, in no particular order and deduplicated — the
/// caller logs it. `Ok(vec![])` is the overwhelmingly common answer and costs
/// one index lookup per fixed predicate.
pub fn canonicalize_predicates(
    query: &mut Query,
    schema_view: &SchemaView,
) -> Result<Vec<Rewrite>, AmbiguousAlias> {
    let mut predicates: BTreeSet<String> = BTreeSet::new();
    match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => collect(pattern, &mut predicates),
    }
    if predicates.is_empty() {
        return Ok(Vec::new());
    }

    let mapping = alias_mapping(&predicates, schema_view)?;
    if mapping.is_empty() {
        return Ok(Vec::new());
    }

    match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => apply(pattern, &mapping),
    }

    let mut rewrites: Vec<Rewrite> = mapping
        .into_iter()
        .map(|(from, to)| Rewrite { from, to })
        .collect();
    rewrites.sort_by(|a, b| a.from.cmp(&b.from));
    Ok(rewrites)
}

/// Which of `predicates` are native spellings, and what each one means.
///
/// The expensive half — a pass over every slot in the view — runs only when a
/// candidate survives the cheap half, so a query written in canonical IRIs (or
/// in no slot IRIs at all, which is most of them) pays nothing but an indexed
/// lookup per predicate.
fn alias_mapping(
    predicates: &BTreeSet<String>,
    schema_view: &SchemaView,
) -> Result<HashMap<String, String>, AmbiguousAlias> {
    let conv = schema_view.converter();
    let mut candidates: Vec<&String> = Vec::new();
    for iri in predicates {
        let Ok(Some(slot)) = schema_view.get_slot_by_uri(iri) else {
            continue;
        };
        let Ok(canonical) = slot.canonical_uri().to_uri(&conv) else {
            continue;
        };
        if canonical.0 != *iri {
            candidates.push(iri);
        }
    }
    if candidates.is_empty() {
        return Ok(HashMap::new());
    }

    // Every canonical spelling in the view, and every native spelling that
    // differs from it. Built once, here, because the answer for one alias
    // depends on what *every* slot claims — an alias is only unambiguous if no
    // second slot claims it, and it is only an alias at all if it is not
    // itself a canonical IRI.
    let mut canonical_spellings: BTreeSet<String> = BTreeSet::new();
    let mut claims: HashMap<String, BTreeSet<String>> = HashMap::new();
    for slot in schema_view.slot_views().unwrap_or_default() {
        let Ok(canonical) = slot.canonical_uri().to_uri(&conv) else {
            continue;
        };
        canonical_spellings.insert(canonical.0.clone());
        let native = schema_view.get_uri(slot.schema_id(), &slot.name);
        let Ok(native) = native.to_uri(&conv) else {
            continue;
        };
        if native.0 != canonical.0 {
            claims.entry(native.0).or_default().insert(canonical.0);
        }
    }

    let mut mapping = HashMap::new();
    for alias in candidates {
        // The IRI names a predicate that really is in the data. Whatever else
        // it is the native spelling of, that reading stands.
        if canonical_spellings.contains(alias) {
            continue;
        }
        let Some(canonical) = claims.get(alias) else {
            continue;
        };
        if canonical.len() > 1 {
            return Err(AmbiguousAlias {
                kind: AliasKind::Slot,
                alias: alias.clone(),
                canonical: canonical.iter().cloned().collect(),
            });
        }
        if let Some(only) = canonical.iter().next() {
            mapping.insert(alias.clone(), only.clone());
        }
    }
    Ok(mapping)
}

// ---------------------------------------------------------------------------
// Classes: the constant object of an `rdf:type`
// ---------------------------------------------------------------------------

/// Rewrite every `rdf:type` constant that names a class by its native
/// spelling to the class's declared IRI.
///
/// The mirror of [`canonicalize_predicates`] for #451, with the same rules:
/// a variable type asks the data, a `GRAPH` clause is not instance data, a
/// `CONSTRUCT` template is output spelling, an IRI that is itself some class's
/// declared spelling is never rewritten, and a native spelling two classes
/// claim is refused by name.
pub fn canonicalize_classes(
    query: &mut Query,
    schema_view: &SchemaView,
) -> Result<Vec<Rewrite>, AmbiguousAlias> {
    let mut types: BTreeSet<String> = BTreeSet::new();
    match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => collect_types(pattern, &mut types),
    }
    if types.is_empty() {
        return Ok(Vec::new());
    }

    let mapping = class_alias_mapping(&types, schema_view)?;
    if mapping.is_empty() {
        return Ok(Vec::new());
    }

    match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => apply_types(pattern, &mapping),
    }

    let mut rewrites: Vec<Rewrite> = mapping
        .into_iter()
        .map(|(from, to)| Rewrite { from, to })
        .collect();
    rewrites.sort_by(|a, b| a.from.cmp(&b.from));
    Ok(rewrites)
}

/// The class-side twin of [`alias_mapping`]: which of `types` are native
/// spellings of a class with a `class_uri`, and what each one means.
fn class_alias_mapping(
    types: &BTreeSet<String>,
    schema_view: &SchemaView,
) -> Result<HashMap<String, String>, AmbiguousAlias> {
    let conv = schema_view.converter();
    let mut candidates: Vec<&String> = Vec::new();
    for iri in types {
        let Ok(Some(class)) = schema_view.get_class_by_uri(iri) else {
            continue;
        };
        let Ok(canonical) = class.canonical_uri().to_uri(&conv) else {
            continue;
        };
        if canonical.0 != *iri {
            candidates.push(iri);
        }
    }
    if candidates.is_empty() {
        return Ok(HashMap::new());
    }

    let mut canonical_spellings: BTreeSet<String> = BTreeSet::new();
    let mut claims: HashMap<String, BTreeSet<String>> = HashMap::new();
    for class in schema_view.class_views().unwrap_or_default() {
        let Ok(canonical) = class.canonical_uri().to_uri(&conv) else {
            continue;
        };
        canonical_spellings.insert(canonical.0.clone());
        let native = schema_view.get_uri(class.schema_id(), class.name());
        let Ok(native) = native.to_uri(&conv) else {
            continue;
        };
        if native.0 != canonical.0 {
            claims.entry(native.0).or_default().insert(canonical.0);
        }
    }

    let mut mapping = HashMap::new();
    for alias in candidates {
        if canonical_spellings.contains(alias) {
            continue;
        }
        let Some(canonical) = claims.get(alias) else {
            continue;
        };
        if canonical.len() > 1 {
            return Err(AmbiguousAlias {
                kind: AliasKind::Class,
                alias: alias.clone(),
                canonical: canonical.iter().cloned().collect(),
            });
        }
        if let Some(only) = canonical.iter().next() {
            mapping.insert(alias.clone(), only.clone());
        }
    }
    Ok(mapping)
}

/// Every constant object of an `rdf:type` in instance data.
fn collect_types(pattern: &GraphPattern, out: &mut BTreeSet<String>) {
    walk(pattern, &mut |bgp| {
        for triple in bgp {
            if let (NamedNodePattern::NamedNode(predicate), TermPattern::NamedNode(object)) =
                (&triple.predicate, &triple.object)
                && predicate.as_str() == RDF_TYPE
            {
                out.insert(object.as_str().to_owned());
            }
        }
    });
}

/// The same positions as [`collect_types`], writing instead of reading.
fn apply_types(pattern: &mut GraphPattern, mapping: &HashMap<String, String>) {
    walk_mut(pattern, &mut |bgp| {
        for triple in bgp.iter_mut() {
            if let (NamedNodePattern::NamedNode(predicate), TermPattern::NamedNode(object)) =
                (&triple.predicate, &triple.object)
                && predicate.as_str() == RDF_TYPE
                && let Some(canonical) = mapping.get(object.as_str())
                && let Ok(replacement) = spargebra::term::NamedNode::new(canonical)
            {
                triple.object = TermPattern::NamedNode(replacement);
            }
        }
    });
}

/// Visit every basic graph pattern that reads instance data, in the same
/// positions [`collect`] reads predicates from: not inside `GRAPH`, not a
/// `VALUES`, and through every filter expression's `EXISTS`.
fn walk<'a>(pattern: &'a GraphPattern, visit: &mut dyn FnMut(&'a [TriplePattern])) {
    match pattern {
        GraphPattern::Bgp { patterns } => visit(patterns),
        GraphPattern::Path { .. } | GraphPattern::Graph { .. } | GraphPattern::Values { .. } => {}
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Minus { left, right } => {
            walk(left, visit);
            walk(right, visit);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            walk(left, visit);
            walk(right, visit);
            if let Some(expression) = expression {
                walk_expression(expression, visit);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            walk_expression(expr, visit);
            walk(inner, visit);
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            walk_expression(expression, visit);
            walk(inner, visit);
        }
        GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Service { inner, .. } => walk(inner, visit),
    }
}

fn walk_expression<'a>(expr: &'a Expression, visit: &mut dyn FnMut(&'a [TriplePattern])) {
    match expr {
        Expression::Exists(pattern) => walk(pattern, visit),
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            walk_expression(left, visit);
            walk_expression(right, visit);
        }
        Expression::In(value, candidates) => {
            walk_expression(value, visit);
            for candidate in candidates {
                walk_expression(candidate, visit);
            }
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            walk_expression(inner, visit)
        }
        Expression::If(condition, then, otherwise) => {
            walk_expression(condition, visit);
            walk_expression(then, visit);
            walk_expression(otherwise, visit);
        }
        Expression::Coalesce(parts) | Expression::FunctionCall(_, parts) => {
            for part in parts {
                walk_expression(part, visit);
            }
        }
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => {}
    }
}

fn walk_mut(pattern: &mut GraphPattern, visit: &mut dyn FnMut(&mut Vec<TriplePattern>)) {
    match pattern {
        GraphPattern::Bgp { patterns } => visit(patterns),
        GraphPattern::Path { .. } | GraphPattern::Graph { .. } | GraphPattern::Values { .. } => {}
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Minus { left, right } => {
            walk_mut(left, visit);
            walk_mut(right, visit);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            walk_mut(left, visit);
            walk_mut(right, visit);
            if let Some(expression) = expression {
                walk_expression_mut(expression, visit);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            walk_expression_mut(expr, visit);
            walk_mut(inner, visit);
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            walk_expression_mut(expression, visit);
            walk_mut(inner, visit);
        }
        GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Service { inner, .. } => walk_mut(inner, visit),
    }
}

fn walk_expression_mut(expr: &mut Expression, visit: &mut dyn FnMut(&mut Vec<TriplePattern>)) {
    match expr {
        Expression::Exists(pattern) => walk_mut(pattern, visit),
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            walk_expression_mut(left, visit);
            walk_expression_mut(right, visit);
        }
        Expression::In(value, candidates) => {
            walk_expression_mut(value, visit);
            for candidate in candidates.iter_mut() {
                walk_expression_mut(candidate, visit);
            }
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            walk_expression_mut(inner, visit)
        }
        Expression::If(condition, then, otherwise) => {
            walk_expression_mut(condition, visit);
            walk_expression_mut(then, visit);
            walk_expression_mut(otherwise, visit);
        }
        Expression::Coalesce(parts) | Expression::FunctionCall(_, parts) => {
            for part in parts.iter_mut() {
                walk_expression_mut(part, visit);
            }
        }
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => {}
    }
}

// ---------------------------------------------------------------------------
// The refusal: a predicate the subject's class cannot carry
// ---------------------------------------------------------------------------

/// What one query says about one subject: the classes it is typed with and
/// the fixed predicates it is read through.
#[derive(Debug, Default)]
struct SubjectSpellings {
    types: BTreeSet<String>,
    predicates: BTreeSet<String>,
}

/// One variable scope's subjects, keyed by the subject as the query wrote it.
type Scope = BTreeMap<String, SubjectSpellings>;

/// Refuse a fixed predicate on a subject typed with a known class that
/// carries no slot under that IRI.
///
/// Runs after both alias resolutions, so the predicate it judges is the one
/// the data would have to carry. See the module docs for what it leaves alone
/// and why.
pub fn refuse_uncarried_predicates(
    query: &Query,
    schema_view: &SchemaView,
) -> Result<(), Box<NotASlotOfClass>> {
    let pattern = match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => pattern,
    };
    let mut scopes: Vec<Scope> = vec![Scope::new()];
    collect_scopes(pattern, &mut scopes, 0, false);

    let conv = schema_view.converter();
    // The canonical IRIs of every slot a class carries, built once per class
    // named — most queries name one or two.
    let mut carried: HashMap<String, Option<(String, BTreeSet<String>)>> = HashMap::new();
    for scope in &scopes {
        for (subject, spellings) in scope {
            // Exactly one type, and one the schema knows. Two types is an
            // intersection the scoper judges; none, or an unknown one, is the
            // scoper's untyped-subject refusal.
            let mut types = spellings.types.iter();
            let (Some(class_iri), None) = (types.next(), types.next()) else {
                continue;
            };
            let entry = carried.entry(class_iri.clone()).or_insert_with(|| {
                let class = schema_view.get_class_by_uri(class_iri).ok().flatten()?;
                let slots: BTreeSet<String> = class
                    .slots()
                    .iter()
                    .filter_map(|slot| slot.canonical_uri().to_uri(&conv).ok().map(|u| u.0))
                    .collect();
                Some((class.name().to_owned(), slots))
            });
            let Some((class_name, slots)) = entry else {
                continue;
            };
            for predicate in &spellings.predicates {
                if slots.contains(predicate) {
                    continue;
                }
                return Err(Box::new(uncarried(
                    subject,
                    predicate,
                    class_iri,
                    class_name,
                    schema_view,
                )));
            }
        }
    }
    Ok(())
}

/// The refusal for one predicate, with the slot the author probably meant.
fn uncarried(
    subject: &str,
    predicate: &str,
    class_iri: &str,
    class_name: &str,
    schema_view: &SchemaView,
) -> NotASlotOfClass {
    let conv = schema_view.converter();
    // The slot name the predicate resolves to anywhere in the schema, or
    // failing that its local name: `asset360:name` on a `Track` means the
    // `name` that `Track` carries under RSM's IRI.
    let wanted: Option<String> = schema_view
        .get_slot_by_uri(predicate)
        .ok()
        .flatten()
        .map(|slot| slot.name.clone())
        .or_else(|| local_name(predicate).map(str::to_owned));
    let suggestion = schema_view
        .get_class_by_uri(class_iri)
        .ok()
        .flatten()
        .and_then(|class| {
            let wanted = wanted.as_deref()?;
            let slot = class.slots().iter().find(|slot| slot.name == wanted)?;
            let canonical = slot.canonical_uri().to_uri(&conv).ok()?.0;
            let native = schema_view
                .get_uri(slot.schema_id(), &slot.name)
                .to_uri(&conv)
                .ok()
                .map(|u| u.0)
                .filter(|native| *native != canonical);
            Some(SlotSpellings { canonical, native })
        });

    let mut curies = HashMap::new();
    let mut spelled: Vec<&str> = vec![predicate];
    if let Some(SlotSpellings { canonical, native }) = &suggestion {
        spelled.push(canonical);
        if let Some(native) = native {
            spelled.push(native);
        }
    }
    for iri in spelled {
        if let Ok(curie) = conv.compress(iri)
            && curie != iri
        {
            curies.insert(iri.to_owned(), curie);
        }
    }

    NotASlotOfClass {
        subject: subject.to_owned(),
        predicate: predicate.to_owned(),
        class_iri: class_iri.to_owned(),
        class_name: class_name.to_owned(),
        suggestion,
        curies,
    }
}

/// The part of an IRI after its last `#` or `/`, when there is one.
fn local_name(iri: &str) -> Option<&str> {
    let tail = &iri[iri.rfind(['#', '/'])? + 1..];
    (!tail.is_empty()).then_some(tail)
}

/// Gather what every basic graph pattern says about its subjects, one
/// [`Scope`] per variable scope.
///
/// `scope` indexes `scopes`; a sub-`SELECT` — a `Project` below the query's
/// own, which is the one `under_project` records — opens a new one, because
/// a variable it does not project is not the enclosing query's variable and
/// the two must not be judged as one subject. Everything else shares the
/// scope it is in: `OPTIONAL`, `UNION`, `MINUS`, `FILTER EXISTS`. A `GRAPH`
/// or `SERVICE` block is not instance data and contributes nothing; a
/// property path is left to the scoper.
fn collect_scopes(
    pattern: &GraphPattern,
    scopes: &mut Vec<Scope>,
    scope: usize,
    under_project: bool,
) {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for triple in patterns {
                let subject = match &triple.subject {
                    TermPattern::Variable(v) => format!("?{}", v.as_str()),
                    TermPattern::NamedNode(n) => format!("<{}>", n.as_str()),
                    TermPattern::BlankNode(b) => format!("_:{}", b.as_str()),
                    TermPattern::Literal(_) => continue,
                    #[allow(unreachable_patterns)]
                    _ => continue,
                };
                let NamedNodePattern::NamedNode(predicate) = &triple.predicate else {
                    continue;
                };
                let entry = scopes[scope].entry(subject).or_default();
                if predicate.as_str() == RDF_TYPE {
                    if let TermPattern::NamedNode(class) = &triple.object {
                        entry.types.insert(class.as_str().to_owned());
                    }
                } else {
                    entry.predicates.insert(predicate.as_str().to_owned());
                }
            }
        }
        GraphPattern::Path { .. }
        | GraphPattern::Graph { .. }
        | GraphPattern::Service { .. }
        | GraphPattern::Values { .. } => {}
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_scopes(left, scopes, scope, under_project);
            collect_scopes(right, scopes, scope, under_project);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            collect_scopes(left, scopes, scope, under_project);
            collect_scopes(right, scopes, scope, under_project);
            if let Some(expression) = expression {
                collect_scopes_expression(expression, scopes, scope, under_project);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            collect_scopes_expression(expr, scopes, scope, under_project);
            collect_scopes(inner, scopes, scope, under_project);
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            collect_scopes_expression(expression, scopes, scope, under_project);
            collect_scopes(inner, scopes, scope, under_project);
        }
        GraphPattern::Project { inner, .. } => {
            if under_project {
                scopes.push(Scope::new());
                let fresh = scopes.len() - 1;
                collect_scopes(inner, scopes, fresh, true);
            } else {
                collect_scopes(inner, scopes, scope, true);
            }
        }
        GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. } => collect_scopes(inner, scopes, scope, under_project),
    }
}

fn collect_scopes_expression(
    expr: &Expression,
    scopes: &mut Vec<Scope>,
    scope: usize,
    under_project: bool,
) {
    match expr {
        Expression::Exists(pattern) => collect_scopes(pattern, scopes, scope, under_project),
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            collect_scopes_expression(left, scopes, scope, under_project);
            collect_scopes_expression(right, scopes, scope, under_project);
        }
        Expression::In(value, candidates) => {
            collect_scopes_expression(value, scopes, scope, under_project);
            for candidate in candidates {
                collect_scopes_expression(candidate, scopes, scope, under_project);
            }
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            collect_scopes_expression(inner, scopes, scope, under_project)
        }
        Expression::If(condition, then, otherwise) => {
            collect_scopes_expression(condition, scopes, scope, under_project);
            collect_scopes_expression(then, scopes, scope, under_project);
            collect_scopes_expression(otherwise, scopes, scope, under_project);
        }
        Expression::Coalesce(parts) | Expression::FunctionCall(_, parts) => {
            for part in parts {
                collect_scopes_expression(part, scopes, scope, under_project);
            }
        }
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => {}
    }
}

/// Every fixed predicate the pattern reads instance data through.
fn collect(pattern: &GraphPattern, out: &mut BTreeSet<String>) {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for triple in patterns {
                if let NamedNodePattern::NamedNode(node) = &triple.predicate {
                    out.insert(node.as_str().to_owned());
                }
            }
        }
        GraphPattern::Path { path, .. } => collect_path(path, out),
        // Not instance data — see the module docs.
        GraphPattern::Graph { .. } => {}
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Minus { left, right } => {
            collect(left, out);
            collect(right, out);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            collect(left, out);
            collect(right, out);
            if let Some(expression) = expression {
                collect_expression(expression, out);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            collect_expression(expr, out);
            collect(inner, out);
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            collect_expression(expression, out);
            collect(inner, out);
        }
        GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Service { inner, .. } => collect(inner, out),
        GraphPattern::Values { .. } => {}
    }
}

fn collect_path(path: &PropertyPathExpression, out: &mut BTreeSet<String>) {
    match path {
        PropertyPathExpression::NamedNode(node) => {
            out.insert(node.as_str().to_owned());
        }
        PropertyPathExpression::Reverse(inner)
        | PropertyPathExpression::ZeroOrMore(inner)
        | PropertyPathExpression::OneOrMore(inner)
        | PropertyPathExpression::ZeroOrOne(inner) => collect_path(inner, out),
        PropertyPathExpression::Sequence(left, right)
        | PropertyPathExpression::Alternative(left, right) => {
            collect_path(left, out);
            collect_path(right, out);
        }
        PropertyPathExpression::NegatedPropertySet(nodes) => {
            for node in nodes {
                out.insert(node.as_str().to_owned());
            }
        }
    }
}

/// Exhaustive over [`Expression`] on purpose, so a spargebra release that adds
/// a variant is a compile error rather than one more position where an alias is
/// silently not resolved.
fn collect_expression(expr: &Expression, out: &mut BTreeSet<String>) {
    match expr {
        Expression::Exists(pattern) => collect(pattern, out),
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            collect_expression(left, out);
            collect_expression(right, out);
        }
        Expression::In(value, candidates) => {
            collect_expression(value, out);
            for candidate in candidates {
                collect_expression(candidate, out);
            }
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            collect_expression(inner, out)
        }
        Expression::If(condition, then, otherwise) => {
            collect_expression(condition, out);
            collect_expression(then, out);
            collect_expression(otherwise, out);
        }
        Expression::Coalesce(parts) | Expression::FunctionCall(_, parts) => {
            for part in parts {
                collect_expression(part, out);
            }
        }
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => {}
    }
}

/// The same walk as [`collect`], writing instead of reading.
fn apply(pattern: &mut GraphPattern, mapping: &HashMap<String, String>) {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for triple in patterns.iter_mut() {
                if let NamedNodePattern::NamedNode(node) = &triple.predicate
                    && let Some(canonical) = mapping.get(node.as_str())
                    && let Ok(replacement) = spargebra::term::NamedNode::new(canonical)
                {
                    triple.predicate = NamedNodePattern::NamedNode(replacement);
                }
            }
        }
        GraphPattern::Path { path, .. } => apply_path(path, mapping),
        GraphPattern::Graph { .. } => {}
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Minus { left, right } => {
            apply(left, mapping);
            apply(right, mapping);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            apply(left, mapping);
            apply(right, mapping);
            if let Some(expression) = expression {
                apply_expression(expression, mapping);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            apply_expression(expr, mapping);
            apply(inner, mapping);
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            apply_expression(expression, mapping);
            apply(inner, mapping);
        }
        GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Service { inner, .. } => apply(inner, mapping),
        GraphPattern::Values { .. } => {}
    }
}

fn apply_path(path: &mut PropertyPathExpression, mapping: &HashMap<String, String>) {
    match path {
        PropertyPathExpression::NamedNode(node) => {
            if let Some(canonical) = mapping.get(node.as_str())
                && let Ok(replacement) = spargebra::term::NamedNode::new(canonical)
            {
                *node = replacement;
            }
        }
        PropertyPathExpression::Reverse(inner)
        | PropertyPathExpression::ZeroOrMore(inner)
        | PropertyPathExpression::OneOrMore(inner)
        | PropertyPathExpression::ZeroOrOne(inner) => apply_path(inner, mapping),
        PropertyPathExpression::Sequence(left, right)
        | PropertyPathExpression::Alternative(left, right) => {
            apply_path(left, mapping);
            apply_path(right, mapping);
        }
        PropertyPathExpression::NegatedPropertySet(nodes) => {
            for node in nodes.iter_mut() {
                if let Some(canonical) = mapping.get(node.as_str())
                    && let Ok(replacement) = spargebra::term::NamedNode::new(canonical)
                {
                    *node = replacement;
                }
            }
        }
    }
}

fn apply_expression(expr: &mut Expression, mapping: &HashMap<String, String>) {
    match expr {
        Expression::Exists(pattern) => apply(pattern, mapping),
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            apply_expression(left, mapping);
            apply_expression(right, mapping);
        }
        Expression::In(value, candidates) => {
            apply_expression(value, mapping);
            for candidate in candidates.iter_mut() {
                apply_expression(candidate, mapping);
            }
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            apply_expression(inner, mapping)
        }
        Expression::If(condition, then, otherwise) => {
            apply_expression(condition, mapping);
            apply_expression(then, mapping);
            apply_expression(otherwise, mapping);
        }
        Expression::Coalesce(parts) | Expression::FunctionCall(_, parts) => {
            for part in parts.iter_mut() {
                apply_expression(part, mapping);
            }
        }
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use linkml_meta::SchemaDefinition;
    use serde_path_to_error as p2e;
    use serde_yml as yml;

    /// Two schemas, the shape the asset360 datamodel has: an RSM subset whose
    /// `name` declares a `slot_uri`, and the project schema whose own `name`
    /// declares none.
    fn schema_view() -> SchemaView {
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
        slot_uri: RSM:#EAID_NAME
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

    const NATIVE: &str = "https://data.infrabel.be/asset360-rsm-subset/name";
    const CANONICAL: &str = "http://rsm.uic.org/RSM12#EAID_NAME";
    const ZONE_NAME: &str = "https://data.infrabel.be/asset360/name";

    const NATIVE_CLASS: &str = "https://data.infrabel.be/asset360-rsm-subset/Track";
    const CANONICAL_CLASS: &str = "http://rsm.uic.org/RSM12#EAID_TRACK";
    const ZONE: &str = "https://data.infrabel.be/asset360/Zone";

    fn rewrite(query: &str) -> (Query, Vec<Rewrite>) {
        let sv = schema_view();
        let mut parsed = crate::sparql_scoper::parse_query(query).expect("parses");
        let rewrites = canonicalize(&mut parsed, &sv).expect("accepted");
        (parsed, rewrites)
    }

    fn refuse(query: &str) -> NotASlotOfClass {
        let sv = schema_view();
        let mut parsed = crate::sparql_scoper::parse_query(query).expect("parses");
        match canonicalize(&mut parsed, &sv) {
            Err(SpellingError::NotASlotOfClass(refusal)) => *refusal,
            other => panic!("{query}: expected a refusal, got {other:?}"),
        }
    }

    #[test]
    fn the_native_spelling_becomes_the_one_the_data_uses() {
        let (parsed, rewrites) = rewrite(&format!("SELECT ?n WHERE {{ ?t <{NATIVE}> ?n }}"));
        assert_eq!(
            rewrites,
            vec![Rewrite {
                from: NATIVE.to_owned(),
                to: CANONICAL.to_owned(),
            }]
        );
        assert!(parsed.to_string().contains(CANONICAL));
        assert!(!parsed.to_string().contains(NATIVE));
    }

    #[test]
    fn the_canonical_spelling_is_left_alone() {
        let (parsed, rewrites) = rewrite(&format!("SELECT ?n WHERE {{ ?t <{CANONICAL}> ?n }}"));
        assert!(rewrites.is_empty());
        assert!(parsed.to_string().contains(CANONICAL));
    }

    /// A slot that declares no `slot_uri` has one spelling, and it is already
    /// the one in the data. Nothing about this feature may touch it.
    #[test]
    fn a_slot_without_a_slot_uri_is_untouched() {
        let (parsed, rewrites) = rewrite(&format!("SELECT ?n WHERE {{ ?z <{ZONE_NAME}> ?n }}"));
        assert!(rewrites.is_empty());
        assert!(parsed.to_string().contains(ZONE_NAME));
    }

    /// The alias reaches every position a predicate can be read from, because
    /// the failure this fixes is an `OPTIONAL` answering an empty column.
    #[test]
    fn the_alias_is_resolved_inside_optional_union_and_exists() {
        for query in [
            format!("SELECT ?n WHERE {{ ?t a <urn:x> OPTIONAL {{ ?t <{NATIVE}> ?n }} }}"),
            format!("SELECT ?n WHERE {{ {{ ?t <{NATIVE}> ?n }} UNION {{ ?t a <urn:x> }} }}"),
            format!("SELECT ?t WHERE {{ ?t a <urn:x> FILTER EXISTS {{ ?t <{NATIVE}> ?n }} }}"),
            format!("SELECT ?n WHERE {{ ?t <{NATIVE}>+ ?n }}"),
        ] {
            let (parsed, rewrites) = rewrite(&query);
            assert_eq!(rewrites.len(), 1, "{query}");
            assert!(parsed.to_string().contains(CANONICAL), "{query}");
            assert!(!parsed.to_string().contains(NATIVE), "{query}");
        }
    }

    /// A variable predicate asks what the data says, and the data says the
    /// canonical IRI. Rewriting is not possible here and must not be faked.
    #[test]
    fn an_open_predicate_is_not_rewritten() {
        let (_, rewrites) = rewrite("SELECT ?p WHERE { ?t ?p ?o }");
        assert!(rewrites.is_empty());
    }

    /// The `CONSTRUCT` template is the author's chosen output spelling.
    #[test]
    fn a_construct_template_keeps_the_spelling_it_was_given() {
        let (parsed, rewrites) = rewrite(&format!(
            "CONSTRUCT {{ ?t <{NATIVE}> ?n }} WHERE {{ ?t <{NATIVE}> ?n }}"
        ));
        assert_eq!(rewrites.len(), 1);
        let rendered = parsed.to_string();
        assert!(rendered.contains(NATIVE), "template kept: {rendered}");
        assert!(rendered.contains(CANONICAL), "where rewritten: {rendered}");
    }

    /// Named graphs are not instance data; the only one served holds the
    /// datamodel, whose predicates are RDF vocabulary.
    #[test]
    fn a_predicate_inside_a_graph_clause_is_left_alone() {
        let (parsed, rewrites) = rewrite(&format!(
            "SELECT ?n WHERE {{ GRAPH <urn:g> {{ ?t <{NATIVE}> ?n }} }}"
        ));
        assert!(rewrites.is_empty());
        assert!(parsed.to_string().contains(NATIVE));
    }

    // -- #451: the constant object of an rdf:type ---------------------------

    /// `?s a irsm:Track` names the class by the spelling the datamodel gives
    /// it, and the data carries the declared `class_uri`. Same rule as the
    /// predicate, same place.
    #[test]
    fn the_native_class_spelling_becomes_the_one_the_data_uses() {
        let (parsed, rewrites) = rewrite(&format!("SELECT ?t WHERE {{ ?t a <{NATIVE_CLASS}> }}"));
        assert_eq!(
            rewrites,
            vec![Rewrite {
                from: NATIVE_CLASS.to_owned(),
                to: CANONICAL_CLASS.to_owned(),
            }]
        );
        assert!(parsed.to_string().contains(CANONICAL_CLASS));
        assert!(!parsed.to_string().contains(NATIVE_CLASS));
    }

    #[test]
    fn the_declared_class_spelling_and_a_class_without_one_are_left_alone() {
        for iri in [CANONICAL_CLASS, ZONE] {
            let (parsed, rewrites) = rewrite(&format!("SELECT ?t WHERE {{ ?t a <{iri}> }}"));
            assert!(rewrites.is_empty(), "{iri}");
            assert!(parsed.to_string().contains(iri), "{iri}");
        }
    }

    /// Both aliases on one parse, and the rewrites reported together.
    #[test]
    fn a_class_alias_and_a_slot_alias_resolve_in_one_pass() {
        let (parsed, rewrites) = rewrite(&format!(
            "SELECT ?n WHERE {{ ?t a <{NATIVE_CLASS}> OPTIONAL {{ ?t <{NATIVE}> ?n }} }}"
        ));
        assert_eq!(rewrites.len(), 2, "{rewrites:?}");
        let rendered = parsed.to_string();
        assert!(rendered.contains(CANONICAL_CLASS) && rendered.contains(CANONICAL));
        assert!(!rendered.contains(NATIVE_CLASS) && !rendered.contains(NATIVE));
    }

    /// The same three exclusions as a predicate: a variable type asks the
    /// data, a `GRAPH` clause is not instance data, a `CONSTRUCT` template is
    /// the author's output.
    #[test]
    fn a_class_is_not_rewritten_where_a_predicate_would_not_be() {
        let (_, rewrites) = rewrite("SELECT ?c WHERE { ?t a ?c }");
        assert!(rewrites.is_empty());

        let (parsed, rewrites) = rewrite(&format!(
            "SELECT ?t WHERE {{ GRAPH <urn:g> {{ ?t a <{NATIVE_CLASS}> }} }}"
        ));
        assert!(rewrites.is_empty());
        assert!(parsed.to_string().contains(NATIVE_CLASS));

        let (parsed, rewrites) = rewrite(&format!(
            "CONSTRUCT {{ ?t a <{NATIVE_CLASS}> }} WHERE {{ ?t a <{NATIVE_CLASS}> }}"
        ));
        assert_eq!(rewrites.len(), 1);
        let rendered = parsed.to_string();
        assert!(rendered.contains(NATIVE_CLASS), "template kept: {rendered}");
        assert!(
            rendered.contains(CANONICAL_CLASS),
            "where rewritten: {rendered}"
        );
    }

    // -- #453: a predicate the class cannot carry ----------------------------

    /// The reporter's guess, and the answer it gets now: not an empty column
    /// but the spelling to write. `asset360:name` is `Zone`'s slot; `Track`'s
    /// `name` is written under RSM's IRI and readable as `irsm:name`.
    #[test]
    fn a_predicate_the_class_cannot_carry_is_refused_with_the_spelling_to_write() {
        for class in [NATIVE_CLASS, CANONICAL_CLASS] {
            let refusal = refuse(&format!(
                "SELECT ?n WHERE {{ ?t a <{class}> ; <{ZONE_NAME}> ?n }}"
            ));
            assert_eq!(refusal.subject, "?t");
            assert_eq!(refusal.predicate, ZONE_NAME);
            assert_eq!(
                refusal.class_iri, CANONICAL_CLASS,
                "judged after the class alias"
            );
            assert_eq!(refusal.class_name, "Track");
            assert_eq!(
                refusal.suggestion,
                Some(SlotSpellings {
                    canonical: CANONICAL.to_owned(),
                    native: Some(NATIVE.to_owned()),
                })
            );
            let message = refusal.to_string();
            assert_eq!(
                message,
                format!(
                    "asset360:name (<{ZONE_NAME}>) is not a slot of Track (<{CANONICAL_CLASS}>), \
                     so `?t asset360:name …` can never match; did you mean irsm:name (<{NATIVE}>), \
                     written in the data as <{CANONICAL}>?"
                ),
            );
        }
    }

    /// The other direction: RSM's IRI guessed on a `Zone`. The predicate
    /// resolves to a slot named `name`, `Zone` has one, and it has one
    /// spelling only.
    #[test]
    fn the_suggestion_follows_the_slot_name_the_predicate_resolves_to() {
        let refusal = refuse(&format!(
            "SELECT ?n WHERE {{ ?z a <{ZONE}> ; <{CANONICAL}> ?n }}"
        ));
        assert_eq!(
            refusal.suggestion,
            Some(SlotSpellings {
                canonical: ZONE_NAME.to_owned(),
                native: None,
            })
        );
        assert!(
            refusal
                .to_string()
                .ends_with(&format!("did you mean asset360:name (<{ZONE_NAME}>)?")),
            "{refusal}"
        );
    }

    /// A predicate that is no slot anywhere still cannot match, and the
    /// message says where the class's slots are published instead of
    /// guessing.
    #[test]
    fn a_predicate_that_is_no_slot_at_all_is_refused_without_a_guess() {
        let refusal = refuse(&format!(
            "SELECT ?x WHERE {{ ?z a <{ZONE}> ; <urn:nothing> ?x }}"
        ));
        assert_eq!(refusal.suggestion, None);
        assert_eq!(
            refusal.to_string(),
            format!(
                "<urn:nothing> is not a slot of Zone (<{ZONE}>), so `?z <urn:nothing> …` can \
                 never match. The slots Zone carries are the ?p of `GRAPH <schema> {{ ?p \
                 <https://schema.org/domainIncludes> <{ZONE}> }}`."
            )
        );
    }

    /// The refusal reaches the positions an empty column hides in.
    #[test]
    fn the_refusal_reaches_optional_union_minus_and_exists() {
        for query in [
            format!(
                "SELECT ?n WHERE {{ ?t a <{CANONICAL_CLASS}> OPTIONAL {{ ?t <{ZONE_NAME}> ?n }} }}"
            ),
            format!(
                "SELECT ?n WHERE {{ ?t a <{CANONICAL_CLASS}> {{ ?t <{CANONICAL}> ?n }} UNION {{ ?t <{ZONE_NAME}> ?n }} }}"
            ),
            format!(
                "SELECT ?t WHERE {{ ?t a <{CANONICAL_CLASS}> MINUS {{ ?t <{ZONE_NAME}> ?n }} }}"
            ),
            format!(
                "SELECT ?t WHERE {{ ?t a <{CANONICAL_CLASS}> FILTER NOT EXISTS {{ ?t <{ZONE_NAME}> ?n }} }}"
            ),
            format!("ASK {{ <urn:track-1> a <{CANONICAL_CLASS}> ; <{ZONE_NAME}> ?n }}"),
        ] {
            let refusal = refuse(&query);
            assert_eq!(refusal.predicate, ZONE_NAME, "{query}");
        }
    }

    /// What the lookup cannot judge, it leaves to the scoper: no type, an
    /// unknown type, two types, a `GRAPH` block, a property path, and a
    /// sub-`SELECT`'s private variable.
    #[test]
    fn what_the_refusal_leaves_alone() {
        for query in [
            format!("SELECT ?n WHERE {{ ?t <{ZONE_NAME}> ?n }}"),
            format!("SELECT ?n WHERE {{ ?t a <urn:NotAClass> ; <{ZONE_NAME}> ?n }}"),
            format!(
                "SELECT ?n WHERE {{ ?t a <{CANONICAL_CLASS}> ; a <{ZONE}> ; <{ZONE_NAME}> ?n }}"
            ),
            format!(
                "SELECT ?n WHERE {{ ?t a <{CANONICAL_CLASS}> GRAPH <urn:g> {{ ?t <{ZONE_NAME}> ?n }} }}"
            ),
            format!("SELECT ?n WHERE {{ ?t a <{CANONICAL_CLASS}> ; <{ZONE_NAME}>+ ?n }}"),
            format!(
                "SELECT ?n WHERE {{ ?t a <{CANONICAL_CLASS}> . \
                 {{ SELECT ?n WHERE {{ ?t a <{ZONE}> ; <{ZONE_NAME}> ?n }} }} }}"
            ),
            // And the ordinary case: every predicate is one the class carries,
            // in either spelling.
            format!(
                "SELECT ?n WHERE {{ ?t a <{NATIVE_CLASS}> ; <{NATIVE}> ?n ; <{CANONICAL}> ?m }}"
            ),
        ] {
            let sv = schema_view();
            let mut parsed = crate::sparql_scoper::parse_query(&query).expect("parses");
            canonicalize(&mut parsed, &sv).unwrap_or_else(|e| panic!("{query}: {e}"));
        }
    }

    /// Two schemas sharing a default prefix, each with a `name` carrying a
    /// different `slot_uri`: there is no defensible pick, so the alias is
    /// refused by name rather than resolved to whichever the index preferred.
    #[test]
    fn an_alias_naming_two_slots_is_refused() {
        let one = r#"
id: https://example.org/one
name: one
prefixes:
  linkml: https://w3id.org/linkml/
  shared: https://example.org/shared/
  ext: https://example.org/ext/
default_prefix: shared
default_range: string
classes:
  A:
    attributes:
      label:
        slot_uri: ext:labelA
"#;
        let two = r#"
id: https://example.org/two
name: two
prefixes:
  linkml: https://w3id.org/linkml/
  shared: https://example.org/shared/
  ext: https://example.org/ext/
default_prefix: shared
default_range: string
classes:
  B:
    attributes:
      label:
        slot_uri: ext:labelB
"#;
        let mut sv = SchemaView::new();
        for raw in [one, two] {
            let schema: SchemaDefinition =
                p2e::deserialize(yml::Deserializer::from_str(raw)).unwrap();
            sv.add_schema(schema).unwrap();
        }
        let mut parsed = crate::sparql_scoper::parse_query(
            "SELECT ?l WHERE { ?a <https://example.org/shared/label> ?l }",
        )
        .expect("parses");
        let error = canonicalize_predicates(&mut parsed, &sv).expect_err("ambiguous");
        assert_eq!(error.alias, "https://example.org/shared/label");
        assert_eq!(
            error.canonical,
            vec![
                "https://example.org/ext/labelA".to_owned(),
                "https://example.org/ext/labelB".to_owned(),
            ]
        );
    }
}
