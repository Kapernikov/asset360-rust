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

use linkml_schemaview::identifier::Identifier;
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

    /// Indices into the query's depth-tagged triples that no part of this plan
    /// represents.
    ///
    /// The working set, kept rather than collapsed. `inexact` says *a* triple
    /// was dropped and names one cause; this says *which* triples, which is
    /// what a plan needs in order to hand them to another pass instead of
    /// refusing the whole query.
    pub unconsumed: Vec<usize>,
    /// How many rows the object fetch may be limited to — `OFFSET + LIMIT`,
    /// not `LIMIT`.
    ///
    /// The fetch has to cover the whole window the query asks for, because the
    /// engine applies the offset to whatever comes back: `LIMIT 10 OFFSET 20`
    /// needs thirty rows, and fetching ten then skipping twenty returns
    /// nothing.
    ///
    /// Only set for a single-class, zero-join, zero-OPTIONAL query whose plan
    /// describes the whole question (see `inexact`) and whose modifiers let a
    /// limit apply before them.
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

/// Declares [`Inexact`] together with the list of every one of its variants.
///
/// `as_str`, `detail` and `instead` are exhaustive matches, so the compiler
/// already refuses a variant without them. What it does not refuse is a
/// *hand-written* list falling behind — `const ALL: [Inexact; 19]` kept
/// compiling when a twentieth cause arrived, and the test built on it went on
/// passing while the cause was missing from it and from the Python contract.
/// Generating the list from the same rows as the enum makes that
/// unrepresentable.
macro_rules! inexact_variants {
    ($($(#[$meta:meta])* $variant:ident,)+) => {
        /// What the planner had to leave out of a plan.
        ///
        /// One variant per drop site, so a refusal can say which one fired: a
        /// generic "something was dropped" forces a hint listing every possible
        /// rewrite, most of which do not apply.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Inexact {
            $($(#[$meta])* $variant,)+
        }

        impl Inexact {
            /// Every cause. Generated with the enum, so it cannot fall behind.
            pub const ALL: &'static [Inexact] = &[$(Inexact::$variant,)+];
        }
    };
}

inexact_variants! {
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
    /// A triple no part of the plan claimed.
    ///
    /// The catch-all, and the point of the working set: a triple is inexact by
    /// default and only a path that fully represents it says otherwise. A cause
    /// added later is a better message; this one means the plan is still
    /// honest about not describing the query.
    UnrepresentedTriple,
    /// A slot read through two variables — `:kinds ?x ; :kinds ?y` — which is a
    /// self-join over its values.
    DuplicateSlotBinding,
    /// A second, different `rdf:type` on one subject: an intersection of
    /// classes, where the plan holds one.
    RepeatedType,
    /// A constant object carrying a language tag or a non-string datatype. The
    /// pushed condition compares stored text, so it would match on the value
    /// alone and accept rows the query excludes.
    TaggedConstant,
    /// A constant on an enum column that no stored value renders as — the
    /// literal spelling of a code that carries a `meaning`, or an IRI no code
    /// maps to. The answer is *no records*, which an equality on stored text
    /// cannot state: pushing the constant's own text would match the code that
    /// renders as an IRI.
    EnumConstantUnmatched,
    /// A `VALUES` row with `UNDEF` for a variable: that row places no
    /// constraint, so dropping it turns a union into an intersection.
    UndefInValues,
    /// A `VALUES` block over several variables with more than one row. The rows
    /// are *tuples* — `("BX1" 4) ("BX2" 3)` admits two combinations — and one
    /// independent `IN` per column admits all four.
    ValuesTuple,
    /// A nested structure given its own `rdf:type`, so it became a class of its
    /// own. A join edge would claim the slot stores the other class's URI, and
    /// an inlined slot stores the structure itself.
    TypedNestedStructure,
    /// A constant and a variable read of one multivalued slot
    /// (`:kinds "p" ; :kinds ?x`): two independent reads of the same values,
    /// which the plan collapses into one filtered read.
    ConstantAndVariableOnSlot,
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
            Self::UnrepresentedTriple => "unrepresented_triple",
            Self::DuplicateSlotBinding => "duplicate_slot_binding",
            Self::RepeatedType => "repeated_type",
            Self::TaggedConstant => "tagged_constant",
            Self::EnumConstantUnmatched => "enum_constant_unmatched",
            Self::UndefInValues => "undef_in_values",
            Self::TypedNestedStructure => "typed_nested_structure",
            Self::ValuesTuple => "values_tuple",
            Self::ConstantAndVariableOnSlot => "constant_and_variable_on_slot",
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
            Self::UnrepresentedTriple => {
                "a triple pattern is not represented in the plan, so the plan \
                 describes fewer constraints than the query"
            }
            Self::DuplicateSlotBinding => {
                "one slot is read through two variables, which pairs its values \
                 with each other — the plan describes a single read"
            }
            Self::RepeatedType => {
                "a subject is given two different rdf:types, which is an \
                 intersection of classes; the plan holds one class"
            }
            Self::TaggedConstant => {
                "a constant object carries a language tag or datatype, and the \
                 pushed condition compares stored text only"
            }
            Self::EnumConstantUnmatched => {
                "a constant on an enum-valued slot is a term no stored value \
                 renders as, so the question selects no records at all"
            }
            Self::UndefInValues => {
                "a VALUES row uses UNDEF, which places no constraint at all — \
                 dropping it would turn a union into an intersection"
            }
            Self::TypedNestedStructure => {
                "a nested structure is given its own rdf:type, which makes it a \
                 second class; the plan can only relate two classes by a \
                 reference, and this slot stores the structure itself"
            }
            Self::ValuesTuple => {
                "a VALUES block pairs several variables per row, and the plan \
                 can only say which values each column may take — which admits \
                 combinations the query does not list"
            }
            Self::ConstantAndVariableOnSlot => {
                "one multivalued slot is read both as a constant and through a \
                 variable, which pairs its values with each other; the plan \
                 describes a single filtered read"
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
            Self::UnrepresentedTriple | Self::DuplicateSlotBinding => {
                "Ask about one value per slot; read a slot twice as two \
                 questions if you need to pair its values."
            }
            Self::RepeatedType => {
                "Give each subject one rdf:type. Two types means the \
                 intersection, which is usually empty."
            }
            Self::TaggedConstant => {
                "Compare with a FILTER, e.g. `FILTER(?name = \"BX1\"@en)`, or \
                 drop the tag if the stored value is untagged."
            }
            Self::EnumConstantUnmatched => {
                "Group by the slot first to see the terms its values render \
                 as: a permissible value with a `meaning` is that IRI, not its \
                 code spelled as a literal."
            }
            Self::UndefInValues => {
                "Leave the row out instead of using UNDEF, or ask the \
                 unconstrained case as its own question."
            }
            Self::TypedNestedStructure => {
                "Drop the rdf:type on the nested variable — the schema already \
                 says what it is — and the same question is answered by reading \
                 through it as a path."
            }
            Self::ValuesTuple => {
                "Use one VALUES per variable if the columns are independent, \
                 or ask each listed combination as its own question."
            }
            Self::ConstantAndVariableOnSlot => {
                "Read the slot once — drop the constant and filter the \
                 variable instead."
            }
        }
    }
}

/// What RDF term one column's values become, as far as a *pushed comparison* is
/// concerned.
///
/// A pushed condition compares the stored text. That is the same question
/// SPARQL asks only when the term is the text: `"BX1"@en` is not `"BX1"`, and
/// an enum value that serialises as an IRI is not its stored code. Decided from
/// the slot, because it is the stored form that settles it — not the form the
/// query happened to write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PushForm {
    /// A literal, with the datatype and language its values carry. `None`
    /// datatype means a plain literal, whose datatype *is* `xsd:string`.
    Literal {
        datatype: Option<String>,
        lang: Option<String>,
        /// Whether the range is a number. Not part of "is this the same term",
        /// but it decides whether SPARQL compares the constant by *value*.
        numeric: bool,
    },
    /// A named node: comparable, against an IRI constant only.
    Iri,
    /// Enum-valued: the stored text is a permissible value, and the term it
    /// renders as is that value's `meaning` IRI when it has one and the plain
    /// literal otherwise. So a constant is not compared against the column —
    /// it is translated *backwards*, from the term the query wrote to the code
    /// the column stores. See [`enum_codes`].
    ///
    /// Carries `(code, meaning IRI)` for the values that have one. A partially
    /// mapped enum is the normal case, not a corner: `signalType` stores `GSA`
    /// with a meaning beside `KSS` without one, so one column answers a literal
    /// constant and an IRI constant, each for different records.
    Enum { meanings: Vec<(String, String)> },
    /// A column whose term this cannot describe at all, so no comparison
    /// against a query constant is the same question.
    Tagged,
}

/// Which stored codes a constant on an enum column selects.
///
/// `None` means the constant is not a term any stored value renders as, so the
/// question has an answer — no records — that a pushed equality cannot state.
/// The caller records that as a loss rather than inventing a condition:
/// pushing the constant's own text would match the code that renders as an IRI
/// and answer 12072 where SPARQL answers 0.
///
/// Empty is impossible by construction: a literal that matches nothing returns
/// `None`, and a literal that is not a mapped code selects itself, which is
/// also what a value outside the enum stores.
fn enum_codes(meanings: &[(String, String)], term: &TermPattern) -> Option<Vec<String>> {
    match term {
        // An IRI constant selects every code whose meaning is that IRI. Two
        // codes may share one, which is why this is a list.
        TermPattern::NamedNode(nn) => {
            let codes: Vec<String> = meanings
                .iter()
                .filter(|(_code, iri)| iri == nn.as_str())
                .map(|(code, _iri)| code.clone())
                .collect();
            (!codes.is_empty()).then_some(codes)
        }
        TermPattern::Literal(lit) => {
            // An enum value renders either as its meaning IRI or as a plain
            // literal, never as a typed or tagged one.
            if lit.language().is_some() || lit.datatype().as_str() != XSD_STRING_IRI {
                return None;
            }
            // A code that has a meaning renders as that IRI, so the plain
            // literal spelling of it is a term no record carries.
            if meanings.iter().any(|(code, _iri)| code == lit.value()) {
                return None;
            }
            // Otherwise the stored text is the term: an unmapped permissible
            // value, or a value the data holds that the enum does not declare.
            Some(vec![lit.value().to_owned()])
        }
        _ => None,
    }
}

/// One condition from the codes an enum constant selects.
fn enum_condition(codes: Vec<String>) -> FilterCondition {
    match <[String; 1]>::try_from(codes) {
        Ok([only]) => FilterCondition::Eq(only),
        Err(several) => FilterCondition::In(several),
    }
}

/// Value variables, each with the column it reads and how that column compares.
type ValueColumns = HashMap<String, (String, Vec<String>, PushForm)>;

/// Filter conditions per star, keyed by the path they read.
///
/// A path of one slot is a column of the record itself; a longer one reads
/// inside its JSON. Keyed by path rather than by slot name because
/// `maintenanceUnit.zoneName` and a top-level `zoneName` are different columns
/// that a flat key would merge.
type StarFilters = HashMap<String, HashMap<Vec<String>, Vec<FilterCondition>>>;

/// Where a nested variable's value lives, relative to a star.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathBinding {
    /// The star this path starts from.
    pub star_var: String,
    /// Slots to follow from the object root. Always at least two — a
    /// single-slot binding is already in [`Star::slot_variables`].
    pub slot_path: Vec<String>,
    /// Whether any hop of the path was introduced inside an `OPTIONAL`.
    ///
    /// A required and an optional nested read produce the same slots, and
    /// different answers: required excludes the records that lack the value,
    /// optional keeps them with the variable unbound. Without this the two are
    /// byte-identical in the plan, so one of the two answers is necessarily
    /// wrong.
    pub optional: bool,
}

/// The datatype every plain literal carries in RDF 1.1.
const XSD_STRING_IRI: &str = "http://www.w3.org/2001/XMLSchema#string";

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
    ///
    /// A field listed in [`Self::multivalued_fields`] holds an array, and a
    /// condition on it is a test that the array *contains* the value. Rendering
    /// it as `object_data->>'field' = 'value'` compares the array's text and
    /// matches nothing.
    pub filters: HashMap<String, Vec<FilterCondition>>,

    /// Which of this star's slots hold several values per record.
    ///
    /// Load-bearing for two things, and wrong answers either way. A condition
    /// in [`Self::filters`] on one of these is a containment test — in
    /// Postgres, `EXISTS (SELECT 1 FROM
    /// jsonb_array_elements_text(object_data->'field') v WHERE v.value = ...)`
    /// rather than an equality. And a value read off one multiplies solutions:
    /// a record with three values answers a SPARQL question three times, so a
    /// row-per-record count is not a count of solutions.
    ///
    /// Covers every slot mentioned on this star, whether it is filtered, bound
    /// to a variable, or only required to exist.
    pub multivalued_fields: Vec<String>,

    /// Conditions on values inside this record's JSON, one entry per path.
    ///
    /// Separate from [`Self::filters`] because the two render differently: a
    /// filter names a column, a path filter walks into it. A consumer that
    /// renders only `filters` answers a weaker question than the query asked,
    /// so this is not optional to read.
    pub path_filters: Vec<PathFilter>,

    /// Which of this star's slots compare as numbers rather than as text.
    ///
    /// A [`FilterCondition::Cmp`] on one of these has to cast, because the
    /// stored JSONB text does not order the way the number does: `'9' >= '10'`
    /// is true as text and false as a number, and `'2001' > '9'` is false as
    /// text. Getting it wrong is silent in both directions — the aggregate
    /// route reports a group too many, and the prefetch route drops every row
    /// and lets the engine aggregate nothing.
    ///
    /// A binding carries this on its own term descriptor, but a slot that only
    /// appears in a `FILTER` has no binding, so a consumer holding only
    /// `filters` cannot ask. Resolved through the same `resolve_column` the
    /// descriptors come from, so the two cannot disagree about a column.
    ///
    /// Covers every slot mentioned on this star, like `multivalued_fields`.
    pub numeric_fields: Vec<String>,

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

/// A condition on a value *inside* a record's JSON, rather than on a column of
/// the record itself.
///
/// `?s :maintenanceUnit ?m . ?m :zoneName "Charleroi"` constrains a value two
/// slots down, which no key of [`Star::filters`] can name. Rendered by walking
/// the path: `object_data->'maintenanceUnit'->>'zoneName' = 'Charleroi'`.
///
/// Only single-valued hops appear here, and only outside `OPTIONAL`. A
/// multivalued hop would make the condition a containment test over the
/// elements, and an optional one would drop the rows a `LEFT JOIN` exists to
/// keep -- both are left to the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathFilter {
    /// Slots from the record's root to the value, e.g.
    /// `["maintenanceUnit", "zoneName"]`. Always at least two long: one slot is
    /// a column, and lives in [`Star::filters`].
    pub slot_path: Vec<String>,
    /// What the value must satisfy. Same vocabulary as a column's conditions.
    pub conditions: Vec<FilterCondition>,
    /// Whether this value compares as a number rather than as text.
    ///
    /// `Star::numeric_fields` cannot answer it: that lists the record's own
    /// slots, and this value is inside one of them. Without it a comparison on
    /// a nested number compares text, where `'9' >= '10'` is true -- the same
    /// wrong answer that `numeric_fields` was added to prevent one level up.
    pub numeric: bool,
}

/// A filter condition extracted from the SPARQL query, pushable to SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
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

impl std::fmt::Display for FilterCondition {
    /// As the condition reads in SQL, which is how a reader of a plan checks
    /// it: `= 'KSS'`, `IN ('a', 'b')`, `>= '10'`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq(value) => write!(f, "= '{value}'"),
            Self::In(values) => write!(
                f,
                "IN ({})",
                values
                    .iter()
                    .map(|value| format!("'{value}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::Cmp { op, value } => write!(f, "{} '{value}'", op.as_sql()),
        }
    }
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
    /// The same demand with the sides swapped: `3 < COUNT(*)` is
    /// `COUNT(*) > 3`.
    ///
    /// A comparison the query wrote the other way round is the same question,
    /// and refusing it would refuse a spelling.
    pub fn flipped(self) -> Self {
        match self {
            Self::Gt => Self::Lt,
            Self::Gte => Self::Lte,
            Self::Lt => Self::Gt,
            Self::Lte => Self::Gte,
        }
    }

    /// The SQL spelling, for a plan a human reads.
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

pub(crate) const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

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
    // Stars that did not survive. Borrowed, not copied: `star_map` outlives the
    // loop and every field this needs is already on the builder.
    let mut discarded_claims: Vec<&StarBuilder> = Vec::new();
    // Named for what it does. This was `drop`, which shadowed `std::mem::drop`
    // for the rest of the function and read as if it destroyed the cause rather
    // than recording it.
    let mut record_loss = |cause: Inexact| {
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

    // Every triple starts unconsumed, and only a path that *fully represents*
    // it marks it consumed. Whatever is left over at the end makes the plan
    // inexact, whether or not anyone thought to enumerate that case.
    //
    // Three rounds of review each closed the drop sites of the round before and
    // found a new one: subjects, then predicates, then a repeated rdf:type and a
    // slot bound twice. That is what auditing every `continue` by hand gets you.
    // Inverting the default is the fix: a triple nobody claimed is a triple the
    // plan does not describe.
    let mut unconsumed: HashSet<usize> = (0..triples_with_depth.len()).collect();

    for (index, (tp, depth)) in triples_with_depth.iter().enumerate() {
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
                record_loss(Inexact::UnscopedSubject);
                continue;
            }
        };

        let pred_iri = match &tp.predicate {
            NamedNodePattern::NamedNode(nn) => nn.as_str(),
            _ => {
                // `?s ?p ?o`: which slot this reads is unknown until the query
                // runs, so the triple constrains nothing here.
                record_loss(Inexact::VariablePredicate);
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
                claimed: Vec::new(),
            });

        if pred_iri == RDF_TYPE {
            if let TermPattern::NamedNode(nn) = &tp.object {
                if builder.type_iri.is_none() {
                    builder.type_iri = Some(nn.as_str().to_owned());
                    builder.type_depth = *depth;
                    unconsumed.remove(&index);
                    builder.claimed.push(index);
                } else if builder.type_iri.as_deref() == Some(nn.as_str()) {
                    // The same type stated twice says nothing new.
                    unconsumed.remove(&index);
                    builder.claimed.push(index);
                } else if *depth < builder.type_depth {
                    // A second, *different* rdf:type is an intersection —
                    // `?s a :Signal ; a :Track` matches nothing unless one
                    // subclasses the other — and a plan holding one class
                    // counts every instance of it. Take the shallower one and
                    // leave this triple unconsumed.
                    builder.type_iri = Some(nn.as_str().to_owned());
                    builder.type_depth = *depth;
                }
            }
        } else if let Ok(Some(slot_view)) = schema_view.get_slot_by_uri(pred_iri) {
            // Handled below. The `else` after this branch is the drop site for
            // a predicate the schema does not know.
            let slot_name = slot_view.name.clone();
            let multivalued = slot_view.determine_slot_container_mode()
                != linkml_schemaview::slotview::SlotContainerMode::SingleValue;
            // How this column's values render, so an inline constant is judged
            // by the same rule as one in a FILTER or a VALUES. This arm used to
            // ask only whether the *query* wrote a plain literal, which accepts
            // `:length "3"` on a column storing `3` and refuses the `3` that
            // matches — the inversion the FILTER route was fixed for.
            let form = push_form_of_slot(schema_view, &slot_view);
            // An identity slot is the object's identity rather than a stored
            // value: the writer emits no triple for it, and a constant here is
            // hoisted into `identifier_values` for an indexed `asset360_uri`
            // lookup instead of a JSONB text compare. The term rule is about
            // that compare, so it does not apply — and the identity may be
            // written either as a literal or as an IRI.
            let identity_slot = slot_view.definition().identifier.unwrap_or(false)
                || slot_view.definition().key.unwrap_or(false);
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
                    // One slot bound to two variables — `:kinds ?x ; :kinds ?y`
                    // — is a self-join over the slot's values, and the map holds
                    // one variable per slot. Keep the first and leave this
                    // triple unconsumed: overwriting silently described a
                    // single read where the query has two. The mirror of the
                    // one-variable-on-two-slots case, which is caught.
                    match builder.object_variables.get(&slot_name) {
                        Some(existing) if existing != v.as_str() => {}
                        // A constant already read this slot. On a multivalued
                        // slot that is the same self-join in the other
                        // direction: `:kinds "p" ; :kinds ?x` pairs the values
                        // with each other, while the plan describes one
                        // filtered read. Single-valued is different — the
                        // constant just fixes what the variable binds.
                        _ if multivalued && builder.inline_filters.contains_key(&slot_name) => {
                            record_loss(Inexact::ConstantAndVariableOnSlot);
                        }
                        _ => {
                            builder
                                .object_variables
                                .insert(slot_name, v.as_str().to_owned());
                            unconsumed.remove(&index);
                            builder.claimed.push(index);
                        }
                    }
                }
                // Inline NamedNode constant: `?s :foo <uri>` →
                // pushable equality filter `object_data->>'foo' = '<uri>'`.
                // Only at depth 0 — inside an OPTIONAL we leave it to
                // oxigraph to avoid breaking LEFT JOIN row preservation.
                TermPattern::NamedNode(nn) if *depth == 0 => {
                    if multivalued && builder.object_variables.contains_key(&slot_name) {
                        // See the variable arm above: two reads of one
                        // multivalued slot, found in the other order.
                        record_loss(Inexact::ConstantAndVariableOnSlot);
                    } else if identity_slot || form == PushForm::Iri {
                        builder
                            .inline_filters
                            .entry(slot_name)
                            .or_default()
                            .push(FilterCondition::Eq(nn.as_str().to_owned()));
                        unconsumed.remove(&index);
                        builder.claimed.push(index);
                    } else if let PushForm::Enum { meanings } = &form {
                        // An enum column stores a code, not the IRI it renders
                        // as, so the constant is translated backwards.
                        match enum_codes(meanings, &tp.object) {
                            Some(codes) => {
                                builder
                                    .inline_filters
                                    .entry(slot_name)
                                    .or_default()
                                    .push(enum_condition(codes));
                                unconsumed.remove(&index);
                                builder.claimed.push(index);
                            }
                            None => record_loss(Inexact::EnumConstantUnmatched),
                        }
                    } else {
                        // An IRI where the column stores a literal: the two are
                        // different terms and oxigraph matches neither.
                        record_loss(Inexact::TaggedConstant);
                    }
                }
                // Inline literal constant: `?s :foo "bar"`.
                //
                // Pushed only when the constant is the term this column's
                // values render as — the same rule the FILTER and VALUES routes
                // apply, from the same function.
                TermPattern::Literal(lit) if *depth == 0 => {
                    if let PushForm::Enum { meanings } = &form
                        && !identity_slot
                    {
                        // Same rule as the IRI arm above: a literal selects the
                        // codes that render as it, which is the code itself
                        // only when it carries no `meaning`.
                        match enum_codes(meanings, &tp.object) {
                            Some(codes) => {
                                if multivalued && builder.object_variables.contains_key(&slot_name)
                                {
                                    record_loss(Inexact::ConstantAndVariableOnSlot);
                                } else {
                                    builder
                                        .inline_filters
                                        .entry(slot_name)
                                        .or_default()
                                        .push(enum_condition(codes));
                                    unconsumed.remove(&index);
                                    builder.claimed.push(index);
                                }
                            }
                            None => record_loss(Inexact::EnumConstantUnmatched),
                        }
                    } else if identity_slot || literal_pushable(lit, &form) {
                        if multivalued && builder.object_variables.contains_key(&slot_name) {
                            // See the variable arm above.
                            record_loss(Inexact::ConstantAndVariableOnSlot);
                        } else {
                            builder
                                .inline_filters
                                .entry(slot_name)
                                .or_default()
                                .push(FilterCondition::Eq(lit.value().to_owned()));
                            unconsumed.remove(&index);
                            builder.claimed.push(index);
                        }
                    } else {
                        record_loss(Inexact::TaggedConstant);
                    }
                }
                // A constant object inside an OPTIONAL: pushing it would
                // filter out rows the LEFT JOIN preserves, so it is left to
                // oxigraph — and the plan no longer says everything the query
                // does.
                TermPattern::NamedNode(_) | TermPattern::Literal(_) => {
                    record_loss(Inexact::ConstantInOptional);
                }
                _ => {}
            }
        } else {
            // A predicate that matches no slot: its constraint is invisible to
            // the plan, so a consumer reading the plan as exact would count
            // rows the query excludes.
            record_loss(Inexact::UnknownPredicate);
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
                // walk clear the ones it explains — and hand the triples this
                // builder claimed back to the working set unless the walk turns
                // out to represent every one of them.
                unresolved_subjects.insert(builder.variable.clone());
                discarded_claims.push(builder);
                continue;
            }
        };
        // `type_depth` is the OPTIONAL depth of the `rdf:type` triple, and it
        // starts at `usize::MAX` for a subject that has none. A constant-IRI
        // subject usually has none — `<.../signal/A> :name ?nm` names the
        // instance instead — and its class is inferred from the slots it uses,
        // so reading the sentinel as a depth made every such star "optional"
        // and got the query refused for a nonexistent OPTIONAL block. Where no
        // type was stated, the star is as optional as the shallowest triple
        // that mentions it.
        let star_is_optional = if builder.type_iri.is_some() {
            builder.type_depth > 0
        } else {
            builder.slot_depth.values().min().is_some_and(|d| *d > 0)
        };
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

        // Which of this star's slots hold arrays. Asked of the schema once here,
        // because a consumer cannot tell from `filters` alone and getting it
        // wrong silently drops every row.
        let mut multivalued_fields: Vec<String> = builder
            .slot_depth
            .keys()
            .filter(|slot_name| {
                schema_view
                    .get_class_by_uri(&class_uri)
                    .ok()
                    .flatten()
                    .and_then(|cv| {
                        cv.slot(&linkml_schemaview::identifier::Identifier::Name(
                            (*slot_name).clone(),
                        ))
                    })
                    .is_some_and(|slot| {
                        slot.determine_slot_container_mode()
                            != linkml_schemaview::slotview::SlotContainerMode::SingleValue
                    })
            })
            .cloned()
            .collect();
        multivalued_fields.sort();

        // Which of them compare as numbers. Asked through `resolve_column`
        // rather than the schema directly, so a filter and a group key on the
        // same slot cannot end up disagreeing about its type.
        let mut numeric_fields: Vec<String> = builder
            .slot_depth
            .keys()
            .filter(|slot_name| {
                crate::sparql_terms::resolve_column(
                    schema_view,
                    &class_uri,
                    std::slice::from_ref(*slot_name),
                )
                .is_some_and(|(descriptor, _)| descriptor.numeric)
            })
            .cloned()
            .collect();
        numeric_fields.sort();

        stars.push(Star {
            variable: builder.variable.clone(),
            class_uri,
            multivalued_fields,
            numeric_fields,
            // Filled in Phase 3, once the paths into this record are known.
            path_filters: Vec::new(),
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
                // A join edge says "this slot holds the other class's URI".
                // That is only true of a *reference*: an inlined slot holds the
                // structure itself, so there is no column to compare and no
                // row to join to. The same question without the nested
                // `rdf:type` is a path, which the plan does carry — so refuse
                // rather than invent an edge, and say that in the hint.
                let stores_a_reference = schema_view
                    .get_class_by_uri(&var_to_class[&builder.variable])
                    .ok()
                    .flatten()
                    .and_then(|cv| {
                        cv.slot(&linkml_schemaview::identifier::Identifier::Name(
                            slot_name.clone(),
                        ))
                    })
                    .is_some_and(|slot| {
                        slot.determine_slot_inline_mode()
                            == linkml_schemaview::slotview::SlotInlineMode::Reference
                    });
                if !stores_a_reference {
                    record_loss(Inexact::TypedNestedStructure);
                    continue;
                }
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
        let edges: Vec<(&str, &str)> = joins
            .iter()
            .map(|j| (j.left.as_str(), j.right.as_str()))
            .collect();
        let reachable = stars_reachable_from(
            stars
                .iter()
                .filter(|s| !s.is_optional)
                .map(|s| s.variable.as_str()),
            &edges,
        );
        for s in &stars {
            if s.is_optional && !reachable.contains(s.variable.as_str()) {
                return Err(ScopeError::UnsupportedConstruct(format!(
                    "OPTIONAL block introduces ?{} which shares no variable with the mandatory pattern; \
                     disconnected OPTIONAL is not supported yet",
                    s.variable
                )));
            }
        }
    }

    // Paths into nested structures. Done after the stars exist, so a variable
    // that *is* a star is never mistaken for a step inside one -- and before
    // the filters, so a condition on a nested value has a path to be attached
    // to. Reading `?m :zoneName ?z . FILTER(?z = "Charleroi")` without the
    // paths is how that filter used to be dropped.
    let (path_bindings, traversed, nested_constants) =
        collect_path_bindings(&star_map, &var_to_class, schema_view);

    // Phase 3: Collect filter conditions per star.
    let mut var_to_field: ValueColumns = HashMap::new();
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
                    (
                        builder.variable.clone(),
                        vec![slot_name.clone()],
                        push_form(schema_view, &var_to_class[&builder.variable], slot_name),
                    ),
                );
            }
        }
    }

    // Which paths compare as numbers, for the conditions materialised below.
    let mut numeric_paths: HashMap<Vec<String>, bool> = HashMap::new();

    // A value inside a nested structure is filterable too, on the path that
    // reaches it. `?m :zoneName ?z . FILTER(?z = "Charleroi")` used to be
    // dropped for want of a key that could name it.
    for (var, binding) in &path_bindings {
        if binding.optional || var_to_field.contains_key(var) {
            continue;
        }
        let Some((form, numeric)) = path_push_form(schema_view, &var_to_class, binding) else {
            continue;
        };
        numeric_paths.insert(binding.slot_path.clone(), numeric);
        var_to_field.insert(
            var.clone(),
            (binding.star_var.clone(), binding.slot_path.clone(), form),
        );
    }

    // The same value written as a constant on the step instead of through a
    // FILTER: `?c :longitude 4`. Phase 1 already applied the column's term rule
    // when it recorded these -- it reads the form from the slot, which is the
    // same slot this path ends on -- so what is left to check is the path
    // itself.
    let mut carried_constants: HashSet<String> = HashSet::new();
    let mut star_filters: StarFilters = HashMap::new();
    for constant in &nested_constants {
        let binding = PathBinding {
            star_var: constant.star_var.clone(),
            slot_path: constant.slot_path.clone(),
            optional: false,
        };
        let Some((_form, numeric)) = path_push_form(schema_view, &var_to_class, &binding) else {
            continue;
        };
        numeric_paths.insert(constant.slot_path.clone(), numeric);
        star_filters
            .entry(constant.star_var.clone())
            .or_default()
            .entry(constant.slot_path.clone())
            .or_default()
            .extend(constant.conditions.iter().cloned());
        carried_constants.insert(constant.nested_var.clone());
    }

    if let Some(cause) = collect_filter_conditions(pattern, 0, &var_to_field, &mut star_filters) {
        record_loss(cause);
    }
    if let Some(cause) = collect_values_filters(pattern, 0, &var_to_field, &mut star_filters) {
        record_loss(cause);
    }

    for star in &mut stars {
        if let Some(extra) = star_filters.remove(&star.variable) {
            let id_slot = var_to_identifier_slot.get(&star.variable).cloned();
            // Merge into any inline-constant filters seeded in Phase 1.
            // Identifier-slot filters get hoisted into identifier_values
            // instead of star.filters — they pushdown against the indexed
            // `asset360_uri` column, not JSONB.
            for (path, conds) in extra {
                // A path of several slots reads inside the JSON, which no
                // column key can name.
                let slot = match <[String; 1]>::try_from(path) {
                    Ok([slot]) => slot,
                    Err(slot_path) => {
                        let numeric = numeric_paths.get(&slot_path).copied().unwrap_or(false);
                        star.path_filters.push(PathFilter {
                            slot_path,
                            conditions: conds,
                            numeric,
                        });
                        continue;
                    }
                };
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

    // A subject the path walk reached is a step inside a star, which the plan
    // describes. Anything left is a subject nothing accounts for.
    if unresolved_subjects
        .iter()
        .any(|var| !traversed.contains(var))
    {
        record_loss(Inexact::UntypedSubject);
    }

    // Hand back what a discarded star had claimed. Reaching the subject is not
    // the same as representing its triples: `?c :longitude ?lo ; :hasName ?x`
    // is walked as far as ?lo, and `hasName` — a slot of some other class —
    // is left describing nothing. Only a value the walk turned into a path
    // binding is actually carried.
    for discarded in &discarded_claims {
        // A constant on a nested step is carried when a path filter took it,
        // which is what `carried_constants` records. An `rdf:type` on such a
        // step never is.
        let represented = (discarded.inline_filters.is_empty()
            || carried_constants.contains(&discarded.variable))
            && discarded.type_iri.is_none()
            && discarded.object_variables.values().all(|var| {
                // A leaf comes back as a path binding. An *intermediate* node
                // deliberately does not — it serialises as a blank node, so the
                // walk records it as a step and keeps going — and the paths that
                // continue past it carry the hop that introduced it.
                path_bindings.contains_key(var) || traversed.contains(var)
            });
        if !represented {
            // Both, deliberately. The `extend` keeps the working set meaning
            // what it says — these triples are unclaimed again — and the cause
            // is named here because `cause_for_unconsumed` reads a returned
            // triple in isolation, where `?d :title ?ti` looks like a duplicate
            // slot binding rather than the casualty of a discarded star.
            unconsumed.extend(discarded.claimed.iter().copied());
            record_loss(Inexact::UnrepresentedTriple);
        }
    }

    // Whatever no path claimed. This is what makes the property structural: a
    // triple is inexact until something says otherwise, so the next drop site
    // nobody thought of is reported rather than silent.
    if let Some(index) = unconsumed.iter().min() {
        let (tp, depth) = &triples_with_depth[*index];
        record_loss(cause_for_unconsumed(tp, *depth, schema_view));
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
        record_loss(Inexact::ImpliedEquality);
    }

    if contains_subquery(pattern) {
        record_loss(Inexact::Subquery);
    }
    if let Some(cause) = contains_foreign_scope(pattern) {
        record_loss(cause);
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

    let mut unconsumed_indices: Vec<usize> = unconsumed.into_iter().collect();
    unconsumed_indices.sort_unstable();

    Ok(QueryPlan {
        root,
        unconsumed: unconsumed_indices,
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
    /// Triples this builder claimed to represent.
    ///
    /// A claim is only as good as the star it was made against: this one may
    /// still be discarded, and its triples are then represented by nothing
    /// unless the path walk picks them up. Kept so they can be handed back.
    claimed: Vec<usize>,
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
            // Existence only, so index the class's own names once rather than
            // rescanning them per used slot — and never materialise a SlotView,
            // which is what `slot()` would cost here for nothing. This runs
            // over every class in the schema.
            let matches = {
                let names: HashSet<&str> = cv.slots().iter().map(|s| s.name.as_str()).collect();
                used_slots.iter().all(|slot| names.contains(slot.as_str()))
            };
            if matches {
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
pub(crate) fn tag_triples_by_depth<'a>(
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

/// Collect the FILTER conditions that can be pushed to SQL.
///
/// Returns the cause when anything was left behind, which the caller records as
/// the plan's inexactness. Two ways that happens:
///
/// * an expression this cannot express — `!=`, `||`, `!`, `REGEX`, `BOUND`, a
///   comparison between two variables;
/// * a `FILTER` inside an `OPTIONAL`. Pushing one into the fetch drops rows the
///   LEFT JOIN is supposed to preserve, which is why inline constants are
///   depth-gated; this recursed into `LeftJoin` with no gate.
fn collect_filter_conditions(
    pattern: &GraphPattern,
    depth: usize,
    var_to_field: &ValueColumns,
    star_filters: &mut StarFilters,
) -> Option<Inexact> {
    match pattern {
        GraphPattern::Filter { expr, inner } => {
            let here = if contains_group(inner) {
                // A condition on the *grouped* rows -- a `HAVING`. Not a row
                // filter, so failing to express it is not a loss of the kind
                // `inexact` reports: it cannot be applied to the fetch at all,
                // and the aggregate route renders it as a SQL `HAVING` or
                // refuses the aggregate by name. Recording a loss here made
                // every `HAVING` query an incomplete plan, which is what
                // blocked the feature before it was written.
                //
                // Extraction is still attempted, and only succeeds for a
                // condition on a group *key*: that value is per-row, so
                // narrowing the fetch by it is sound -- and it is what the
                // fetch route has always done with these queries, so the row
                // set does not change. A `HAVING` over an aggregate has no
                // column to extract and quietly extracts nothing.
                //
                // It is a narrowing and not the whole demand: on a multivalued
                // key, keeping records with *some* element past the bound still
                // leaves the record's other elements as groups, which only the
                // `HAVING` removes.
                extract_equality_from_expr(expr, var_to_field, star_filters);
                None
            } else if depth == 0 {
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

/// Whether this pattern groups, anywhere below.
///
/// What makes a `FILTER` a `HAVING`: its own subtree holds the grouping. A
/// `FILTER` in the `WHERE` clause sits *inside* the group's inner pattern, so
/// its subtree has none. Local and exact, which is why the walk needs no flag
/// threaded through it.
fn contains_group(pattern: &GraphPattern) -> bool {
    match pattern {
        GraphPattern::Group { .. } => true,
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. } => contains_group(inner),
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => contains_group(left) || contains_group(right),
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => false,
    }
}

/// Push what this expression says into `star_filters`.
///
/// Returns `false` when any part of it could not be expressed, so the caller
/// knows the plan no longer describes the whole query. Silence here is what
/// turned a dropped `REGEX` into ten arbitrary rows.
fn extract_equality_from_expr(
    expr: &Expression,
    var_to_field: &ValueColumns,
    star_filters: &mut StarFilters,
) -> bool {
    match expr {
        Expression::Equal(left, right) => {
            if let Some((star_var, field, texts)) = match_var_constant(left, right, var_to_field)
                .or_else(|| match_var_constant(right, left, var_to_field))
            {
                star_filters
                    .entry(star_var)
                    .or_default()
                    .entry(field)
                    .or_default()
                    .push(enum_condition(texts));
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
            let Some((star_var, path, form)) = var_to_field.get(var.as_str()) else {
                return false;
            };
            let mut values = Vec::with_capacity(options.len());
            for option in options {
                // Only when the stored term *is* its text, and the query wrote
                // that same plain term: `IN ("BX1"@en)` compared as text
                // matches a row oxigraph excludes. A computed member would have
                // to be evaluated first. An enum member translates backwards
                // through its meanings, and may select more than one code.
                match constant_texts(option, form) {
                    Some(texts) => values.extend(texts),
                    None => return false,
                }
            }
            if values.is_empty() {
                // `IN ()` is never true, which the plan has no way to say.
                return false;
            }
            star_filters
                .entry(star_var.clone())
                .or_default()
                .entry(path.clone())
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

/// How a value at the end of a path compares, or `None` when it cannot be
/// filtered there at all.
///
/// Two refusals, both silent if skipped. Every hop has to be single-valued, or
/// the condition is a containment test over the elements rather than an
/// equality. And the path has to resolve: a slot the schema cannot describe has
/// no term rule, and without one there is no way to know whether comparing
/// stored text asks what the query asks.
fn path_push_form(
    schema_view: &SchemaView,
    var_to_class: &HashMap<String, String>,
    binding: &PathBinding,
) -> Option<(PushForm, bool)> {
    let class_uri = var_to_class.get(&binding.star_var)?;
    let (descriptor, containers) =
        crate::sparql_terms::resolve_column(schema_view, class_uri, &binding.slot_path)?;
    containers
        .iter()
        .all(|mode| *mode == linkml_schemaview::slotview::SlotContainerMode::SingleValue)
        .then(|| (push_form_of(&descriptor), descriptor.numeric))
}

/// The stored texts a constant selects on a column, or `None` when no
/// comparison on stored text asks what the query asks.
///
/// One rule behind `=`, `IN` and an inline constant. They were three copies
/// once, and the copies drifted: only `IN` learned that a reference column
/// compares against an IRI. An enum translates *backwards* through its
/// meanings; every other column compares the term against itself.
fn constant_texts(expr: &Expression, form: &PushForm) -> Option<Vec<String>> {
    if let PushForm::Enum { meanings } = form {
        let term = match expr {
            Expression::Literal(lit) => TermPattern::Literal(lit.clone()),
            Expression::NamedNode(nn) => TermPattern::NamedNode(nn.clone()),
            _ => return None,
        };
        return enum_codes(meanings, &term);
    }
    match expr {
        Expression::Literal(lit) if literal_pushable(lit, form) => {
            Some(vec![lit.value().to_owned()])
        }
        Expression::NamedNode(nn) if *form == PushForm::Iri => Some(vec![nn.as_str().to_owned()]),
        _ => None,
    }
}

/// A variable compared for equality against a constant, as the codes it selects.
fn match_var_constant(
    var_expr: &Expression,
    const_expr: &Expression,
    var_to_field: &ValueColumns,
) -> Option<(String, Vec<String>, Vec<String>)> {
    let Expression::Variable(v) = var_expr else {
        return None;
    };
    let (star_var, path, form) = var_to_field.get(v.as_str())?;
    let texts = constant_texts(const_expr, form)?;
    Some((star_var.clone(), path.clone(), texts))
}

fn match_var_literal(
    var_expr: &Expression,
    lit_expr: &Expression,
    var_to_field: &ValueColumns,
) -> Option<(String, Vec<String>, String)> {
    let var_name = match var_expr {
        Expression::Variable(v) => v.as_str(),
        _ => return None,
    };
    let (star_var, path, form) = var_to_field.get(var_name)?;
    // Same rule as the IN arm: the pushed condition compares text, so it is the
    // query's question only when the column's term is its text and the query
    // wrote it that way.
    let value = match lit_expr {
        Expression::Literal(lit) if literal_pushable(lit, form) => lit.value().to_owned(),
        // An IRI column compares against an IRI, the same way `IN` does — these
        // two arms are one rule and drifted apart when only `IN` learned it.
        Expression::NamedNode(nn) if *form == PushForm::Iri => nn.as_str().to_owned(),
        _ => return None,
    };
    Some((star_var.clone(), path.clone(), value))
}

/// How a column's values compare, from the same descriptor the renderer uses.
///
/// Unresolvable means unrepresentable, which is `Tagged`: refusing to push is
/// always safe, and this runs before the class has been fully validated.
pub(crate) fn push_form(schema_view: &SchemaView, class_uri: &str, slot_name: &str) -> PushForm {
    push_form_of_path(
        schema_view,
        class_uri,
        std::slice::from_ref(&slot_name.to_owned()),
    )
}

/// Whether the value at this path compares as a number rather than as text.
///
/// The fact a `SqlCondition` deliberately does not carry: it names a slot, and
/// how that slot's values compare is the renderer's to resolve -- from the
/// same `resolve_column` `Star::numeric_fields` comes from, so a lowered
/// condition and a scoped one cannot disagree about a column.
pub(crate) fn numeric_at_path(
    schema_view: &SchemaView,
    class_uri: &str,
    slot_path: &[String],
) -> bool {
    matches!(
        push_form_of_path(schema_view, class_uri, slot_path),
        PushForm::Literal { numeric: true, .. }
    )
}

/// The same question about a value further inside the record.
///
/// `resolve_column` walks a path already -- that is how a `PathFilter` learns
/// whether a nested value compares as a number -- so a condition on
/// `["location", "longitude"]` is gated by exactly the test a condition on a
/// column is. Without this, a rule pushing a nested constant would have no way
/// to ask, and "no way to ask" is how an unfaithful condition gets pushed.
pub(crate) fn push_form_of_path(
    schema_view: &SchemaView,
    class_uri: &str,
    slot_path: &[String],
) -> PushForm {
    let Some((descriptor, _)) =
        crate::sparql_terms::resolve_column(schema_view, class_uri, slot_path)
    else {
        return PushForm::Tagged;
    };
    push_form_of(&descriptor)
}

/// The same question asked of a slot the caller already holds.
///
/// Phase 1 has the `SlotView` but not yet the class, and it was left comparing
/// datatypes by hand — a fourth copy of this rule, and the one that stayed
/// wrong when the other three were fixed.
fn push_form_of_slot(
    schema_view: &SchemaView,
    slot: &linkml_schemaview::slotview::SlotView,
) -> PushForm {
    match crate::sparql_terms::describe_slot(schema_view, slot) {
        Some(descriptor) => push_form_of(&descriptor),
        None => PushForm::Tagged,
    }
}

/// The push form of a column a caller already has the descriptor for.
///
/// Same question as [`push_form`], asked where the schema walk has already
/// happened -- an aggregate's argument carries its descriptor on the binding,
/// so a `HAVING` over `MIN(?name)` can ask the column's term rule without
/// resolving the path a second time.
pub(crate) fn push_form_of_descriptor(
    descriptor: &crate::sparql_terms::TermDescriptor,
) -> PushForm {
    push_form_of(descriptor)
}

fn push_form_of(descriptor: &crate::sparql_terms::TermDescriptor) -> PushForm {
    use crate::sparql_terms::TermKind;
    match descriptor.kind {
        TermKind::Iri => PushForm::Iri,
        TermKind::EnumIri => PushForm::Enum {
            meanings: descriptor.enum_map.clone(),
        },
        TermKind::Literal => PushForm::Literal {
            datatype: descriptor.datatype.clone(),
            lang: descriptor.lang.clone(),
            numeric: descriptor.numeric,
        },
    }
}

/// Whether comparing this constant as text asks what the query asks.
///
/// Only when the constant is the *same RDF term* the column's values render as.
/// The comparison is on stored text, and the text is the term only if the
/// datatype and language agree: `"BX1"@en` is not `"BX1"`, and — depending on
/// whether the schema resolves its `integer` type to an IRI — a length of three
/// is either the plain literal `"3"` or `3`, and the other one matches nothing.
///
/// So this asks the column, never the operator. Whether an *ordering* is
/// meaningful is a separate question, answered by `TermDescriptor::numeric`.
pub(crate) fn literal_pushable(lit: &spargebra::term::Literal, form: &PushForm) -> bool {
    let PushForm::Literal {
        datatype,
        lang,
        numeric,
    } = form
    else {
        return false;
    };

    // Language first, and decisively. A language-tagged literal's datatype is
    // `rdf:langString`, which never equals a column's, so comparing datatypes
    // first refused every constant on a language-tagged column — including the
    // one that was right.
    match (lit.language(), lang.as_deref()) {
        (Some(a), Some(b)) => return a == b,
        (None, None) => {}
        _ => return false,
    }

    // A plain literal's datatype is `xsd:string`, so the column's `None` and a
    // query literal with no explicit type are the same term.
    if lit.datatype().as_str() != datatype.as_deref().unwrap_or(XSD_STRING_IRI) {
        return false;
    }

    // Same datatype is still not SPARQL `=`: on a number the query compares
    // *values* and the pushed condition compares text, so `= "003"^^xsd:integer`
    // selects a record that `object_data->>'length' = '003'` never finds. Push
    // only the form the stored text is written in.
    !numeric || is_canonical_number(lit.value())
}

/// Whether this is the way the number would be written back out.
///
/// Round-tripping is the test rather than a grammar: `003`, `+3` and `1.50` all
/// name values the stored text does not spell that way, and comparing text
/// against any of them silently matches nothing.
fn is_canonical_number(lexical: &str) -> bool {
    if let Ok(int) = lexical.parse::<i64>() {
        return int.to_string() == lexical;
    }
    if let Ok(float) = lexical.parse::<f64>() {
        return float.to_string() == lexical;
    }
    false
}

/// Collect VALUES conditions, now keyed by (star_variable, slot_name).
fn collect_values_filters(
    pattern: &GraphPattern,
    depth: usize,
    var_to_field: &ValueColumns,
    star_filters: &mut StarFilters,
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
            // Rows are tuples. `VALUES (?nm ?l) { ("BX1" 4) ("BX2" 3) }` admits
            // two pairs, and the plan can only say `nm IN (BX1,BX2)` and
            // `l IN (4,3)` — which admits four. One column, or one row, is the
            // same question either way.
            if variables.len() > 1 && bindings.len() > 1 {
                return Some(Inexact::ValuesTuple);
            }
            let mut dropped = None;
            for (i, var) in variables.iter().enumerate() {
                let Some((star_var, field, form)) = var_to_field.get(var.as_str()) else {
                    // A VALUES over a variable no star binds constrains
                    // something this plan does not describe.
                    dropped = Some(Inexact::UnboundValues);
                    continue;
                };
                {
                    let mut values = Vec::new();
                    let mut has_undef = false;
                    let mut mismatched = false;
                    for row in bindings {
                        match row.get(i) {
                            Some(Some(spargebra::term::GroundTerm::NamedNode(nn)))
                                if *form == PushForm::Iri =>
                            {
                                values.push(nn.as_str().to_owned());
                            }
                            // As in a FILTER: text is the term only for a plain
                            // literal on a plain-valued column.
                            Some(Some(spargebra::term::GroundTerm::Literal(lit)))
                                if literal_pushable(lit, form) =>
                            {
                                values.push(lit.value().to_owned());
                            }
                            Some(Some(_)) => mismatched = true,
                            // UNDEF means *no constraint* for that row, so the
                            // block as a whole constrains nothing. Skipping the
                            // cell turned a union into an intersection:
                            // `VALUES ?nm { "BX1" UNDEF }` became `= "BX1"`.
                            _ => has_undef = true,
                        }
                    }
                    if has_undef {
                        dropped = Some(Inexact::UndefInValues);
                        continue;
                    }
                    if mismatched {
                        dropped = Some(Inexact::TaggedConstant);
                        continue;
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
            if blocks_limit_push(inner) {
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
/// A constant found on a nested step, before the schema has vetted it.
///
/// `?c :longitude 4` inside `?s :location ?c` says what a value two slots down
/// must equal. The walk finds it; whether it can be *pushed* is a question for
/// the schema, answered in one place with the same rule the FILTER route uses.
struct NestedConstant {
    star_var: String,
    /// The variable standing for the nested structure, so the caller can tell
    /// which discarded star this accounts for.
    nested_var: String,
    slot_path: Vec<String>,
    conditions: Vec<FilterCondition>,
}

fn collect_path_bindings(
    star_map: &HashMap<String, StarBuilder>,
    var_to_class: &HashMap<String, String>,
    schema_view: &SchemaView,
) -> (
    HashMap<String, PathBinding>,
    HashSet<String>,
    Vec<NestedConstant>,
) {
    let mut out: HashMap<String, PathBinding> = HashMap::new();
    // Variables the walk explained *as steps*: the intermediate nodes it passed
    // through. Scalar leaves are deliberately absent — a leaf holds a value, so
    // a leaf variable used as a subject is a subject nothing accounts for, and
    // clearing it would reinstate the hole this set exists to close.
    //
    // Used only to clear a recorded drop, never to re-derive the check.
    let mut traversed: HashSet<String> = HashSet::new();
    let mut constants: Vec<NestedConstant> = Vec::new();

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
            &mut constants,
            false,
        );
    }

    (out, traversed, constants)
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
    constants: &mut Vec<NestedConstant>,
    optional_so_far: bool,
) {
    if path_so_far.len() >= MAX_PATH_DEPTH {
        return;
    }
    let Some(builder) = star_map.get(current_var) else {
        return;
    };

    // Constants written on this step: `?c :longitude 4`. Only inside a nested
    // structure -- at the root the same conditions are columns of the record,
    // seeded in Phase 1 -- and never through an OPTIONAL, where pushing a
    // condition drops the rows the LEFT JOIN exists to keep.
    if !path_so_far.is_empty() && !optional_so_far {
        let mut slots: Vec<(&String, &Vec<FilterCondition>)> =
            builder.inline_filters.iter().collect();
        slots.sort_by(|a, b| a.0.cmp(b.0));
        for (slot_name, conditions) in slots {
            let mut slot_path = path_so_far.to_vec();
            slot_path.push(slot_name.clone());
            constants.push(NestedConstant {
                star_var: star_var.to_owned(),
                nested_var: current_var.to_owned(),
                slot_path,
                conditions: conditions.clone(),
            });
        }
    }

    let mut slots: Vec<(&String, &String)> = builder.object_variables.iter().collect();
    slots.sort_by(|a, b| a.0.cmp(b.0));

    for (slot_name, object_var) in slots {
        // A typed object variable is its own star, reached by a join edge.
        if var_to_class.contains_key(object_var) {
            continue;
        }
        // The class's own O(1) name index rather than a scan. It hands back an
        // owned SlotView, which this did not need before — worth it for a wide
        // class (TunnelComplex has 95 slots), a wash for a narrow one.
        let Some(slot) = current_class.slot(&Identifier::Name(slot_name.clone())) else {
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
                    let hop_optional = builder
                        .slot_depth
                        .get(slot_name)
                        .is_some_and(|depth| *depth > 0);
                    walk_paths(
                        star_var,
                        object_var,
                        &range_class,
                        &path,
                        star_map,
                        var_to_class,
                        out,
                        traversed,
                        constants,
                        optional_so_far || hop_optional,
                    );
                }
            }
            // A scalar leaf. Depth one is already in Star::slot_variables;
            // recording it again would give two answers to one question.
            None => {
                if path.len() > 1 {
                    // Optional anywhere along the path makes the read optional:
                    // a missing hop leaves the leaf unbound just as a missing
                    // leaf does.
                    let optional = optional_so_far
                        || builder
                            .slot_depth
                            .get(slot_name)
                            .is_some_and(|depth| *depth > 0);
                    out.insert(
                        object_var.clone(),
                        PathBinding {
                            star_var: star_var.to_owned(),
                            slot_path: path,
                            optional,
                        },
                    );
                }
            }
        }
    }
}

/// The stars reachable from `seeds` by any chain of join edges.
///
/// Edges are undirected here: a join constrains both of its ends, so a star is
/// related to the seeds whichever side of the edge it sits on. Shared by the
/// two questions that need it — which OPTIONAL stars hang off the mandatory
/// pattern, and whether every class in an aggregate is actually related — so
/// the two cannot disagree about what "connected" means.
pub(crate) fn stars_reachable_from<'a>(
    seeds: impl IntoIterator<Item = &'a str>,
    edges: &[(&'a str, &'a str)],
) -> HashSet<&'a str> {
    let mut reached: HashSet<&'a str> = seeds.into_iter().collect();
    let mut progress = true;
    while progress {
        progress = false;
        for (left, right) in edges {
            if reached.contains(left) && !reached.contains(right) {
                reached.insert(right);
                progress = true;
            } else if reached.contains(right) && !reached.contains(left) {
                reached.insert(left);
                progress = true;
            }
        }
    }
    reached
}

/// Name the reason a triple went unrepresented, for the message only.
///
/// The verdict does not depend on getting this right: the triple is already
/// inexact by virtue of being unconsumed. This just turns "something was
/// dropped" into something the author can act on.
fn cause_for_unconsumed(tp: &TriplePattern, depth: usize, schema_view: &SchemaView) -> Inexact {
    let NamedNodePattern::NamedNode(pred) = &tp.predicate else {
        return Inexact::VariablePredicate;
    };
    if pred.as_str() == RDF_TYPE {
        return Inexact::RepeatedType;
    }
    match schema_view.get_slot_by_uri(pred.as_str()) {
        Ok(Some(_)) => match &tp.object {
            TermPattern::Variable(_) => Inexact::DuplicateSlotBinding,
            TermPattern::Literal(_) | TermPattern::NamedNode(_) if depth > 0 => {
                Inexact::ConstantInOptional
            }
            TermPattern::Literal(_) => Inexact::TaggedConstant,
            _ => Inexact::UnrepresentedTriple,
        },
        _ => Inexact::UnknownPredicate,
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

/// Whether anything below this point stops a LIMIT from being pushed.
///
/// Two reasons, and they are one question: an operator that must see every
/// solution first (`GROUP BY`, a bare aggregate, `ORDER BY`,
/// `DISTINCT`/`REDUCED`), or a second `Slice`, which would have to be
/// *composed* with the outer one rather than applied independently — an inner
/// LIMIT applies before an outer OFFSET, so `min` is not the composition.
/// Nested slices only arise from sub-queries, which are refused as inexact, so
/// rather than carry arithmetic that is unreachable and would be wrong if it
/// ever ran, refuse to push anything.
///
/// Only [`pushable_limit`] calls this, from inside its own `Slice` arm, so "a
/// `Slice` below this point" and "a `Slice` nested inside the one I am looking
/// at" are the same condition.
fn blocks_limit_push(pattern: &GraphPattern) -> bool {
    match pattern {
        GraphPattern::Group { .. }
        | GraphPattern::OrderBy { .. }
        | GraphPattern::Distinct { .. }
        | GraphPattern::Reduced { .. }
        | GraphPattern::Slice { .. } => true,
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. } => blocks_limit_push(inner),
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            blocks_limit_push(left) || blocks_limit_push(right)
        }
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
  xsd:
    prefix_reference: http://www.w3.org/2001/XMLSchema#
  eul:
    prefix_reference: http://ontorail.org/src/Eulynx/
default_prefix: asset360
default_range: string

# Declared, as the real schema declares them: `tests/data/asset360.yaml`
# imports `./types`, where `integer` resolves to `xsd:integer`. Without this the
# fixture serialises a number as a plain literal, which is a different RDF term
# and answers comparison questions differently — so a fixture without it tests a
# configuration production does not run.
types:
  string:
    uri: xsd:string
    base: str
  integer:
    uri: xsd:integer
    base: int

# Partially mapped, as the real `signalType` is: `GSA` carries a `meaning` and
# renders as that IRI, while `KSS` has none and renders as the plain literal
# it stores. One enum answers both halves of the rule.
enums:
  SignalKind:
    permissible_values:
      GSA:
        meaning: eul:GSA
      KSS: {}
      REP_H_D: {}

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
      detail:
        range: Detail
        inlined: true
  Detail:
    class_uri: asset360:Detail
    attributes:
      value:
        range: string
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
      kind:
        range: SignalKind
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
      # Deliberately the same slot name Signal uses, so a check that matches a
      # carried path by spelling alone is caught rather than trusted.
      documents:
        range: Document
        multivalued: true
        inlined: true
      belongsToLine:
        range: Line
  Line:
    class_uri: asset360:Line
    attributes:
      asset360_uri:
        identifier: true
      hasName:
        range: string
  # A multivalued *reference*: an array of other records' identifiers, as
  # opposed to `documents`, which is an array of inlined structures. The two
  # are the same shape to a check that only asks whether a slot is
  # multivalued, and different questions in SQL.
  LineGroup:
    class_uri: asset360:LineGroup
    attributes:
      asset360_uri:
        identifier: true
      groupsLines:
        range: Line
        multivalued: true
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
    ///
    /// One table on purpose: these are one question asked of every drop site,
    /// and a new site belongs here as a row rather than as a fourth test with
    /// the same body.
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
            // A triple nothing claimed makes the plan inexact by default, which
            // is what stops the *next* unenumerated drop site from being silent.
            // Each of the following was measured Eligible with a wrong number.
            // One slot read through two variables pairs its values with each
            // other; the plan describes a single read.
            (
                Inexact::DuplicateSlotBinding,
                "SELECT ?x ?y WHERE { ?s a asset360:Signal ; asset360:trafficKinds ?x ; \
                 asset360:trafficKinds ?y }",
            ),
            // Two rdf:types is an intersection, and the answer used to depend
            // on which one came first in the query.
            (
                Inexact::RepeatedType,
                "SELECT ?s WHERE { ?s a asset360:Signal ; a asset360:Track }",
            ),
            (
                Inexact::RepeatedType,
                "SELECT ?s WHERE { ?s a asset360:Track ; a asset360:Signal }",
            ),
            // A tagged literal is not the same term as its text, and the pushed
            // condition compares text.
            (
                Inexact::TaggedConstant,
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name \"BX1\"@en }",
            ),
            // UNDEF means "no constraint", so dropping the cell turned a union
            // into an intersection.
            (
                Inexact::UndefInValues,
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 VALUES ?nm { \"BX1\" UNDEF } }",
            ),
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
            // A VALUES over several variables lists tuples; one IN per column
            // admits combinations the query does not.
            (
                Inexact::ValuesTuple,
                "SELECT ?nm ?l WHERE { ?s a asset360:Signal ; asset360:name ?nm ; \
                 asset360:length ?l . VALUES (?nm ?l) { (\"BX1\" 4) (\"BX2\" 3) } }",
            ),
            // A tag does not survive a text comparison, in a FILTER or a
            // VALUES any more than inline: `"BX1"@en` is not `"BX1"`.
            (
                Inexact::FilterExpression,
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(?nm = \"BX1\"@en) }",
            ),
            (
                Inexact::FilterExpression,
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(?nm IN (\"BX1\"@en)) }",
            ),
            (
                Inexact::TaggedConstant,
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 VALUES ?nm { \"BX1\"@en } }",
            ),
            // A multivalued slot read as a constant and through a variable is
            // the same self-join as two variables, in the other direction.
            (
                Inexact::ConstantAndVariableOnSlot,
                "SELECT ?x WHERE { ?s a asset360:Signal ; asset360:trafficKinds \"p\" ; \
                 asset360:trafficKinds ?x }",
            ),
            (
                Inexact::ConstantAndVariableOnSlot,
                "SELECT ?x WHERE { ?s a asset360:Signal ; asset360:trafficKinds ?x ; \
                 asset360:trafficKinds \"p\" }",
            ),
            // A claim is only as good as the star it was made against. Each of
            // these built a star for the nested subject, claimed its triples,
            // and then lost the star — leaving a constraint the plan does not
            // carry, with `exact` still true.
            (
                // A constant on a nested step: the path walk records where a
                // value lives, never what it must equal.
                Inexact::UnrepresentedTriple,
                "SELECT ?ti WHERE { ?s a asset360:Signal ; asset360:documents ?d . \
                 ?d asset360:title ?ti ; asset360:docId \"D1\" }",
            ),
            (
                // A type the schema does not know, on the nested subject: the
                // star cannot resolve, so nothing represents its triples.
                Inexact::UnrepresentedTriple,
                "SELECT ?lo WHERE { ?s a asset360:Signal ; asset360:location ?c . \
                 ?c a <https://example.org/NotAClass> ; asset360:longitude ?lo }",
            ),
            (
                // A slot the schema knows but the *intermediate* class does not:
                // claimed in Phase 1 against the schema, dropped by the walk.
                Inexact::UnrepresentedTriple,
                "SELECT ?lo WHERE { ?s a asset360:Signal ; asset360:location ?c . \
                 ?c asset360:longitude ?lo ; asset360:name ?x }",
            ),
            // A blank-node property list has no variable to scope, so nothing
            // can claim its triples — and the same query written with a named
            // intermediate variable is exact and eligible.
            (
                Inexact::UnscopedSubject,
                "SELECT ?v WHERE { ?s a asset360:Signal ; \
                 asset360:location [ asset360:longitude ?v ] }",
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

    /// Every cause carries all three strings, and no two share a wire form.
    ///
    /// Over [`Inexact::ALL`], which the enum declaration generates — an earlier
    /// version of this test restated the list by hand, went stale, and passed
    /// while a cause was missing from both it and the Python contract.
    #[test]
    fn every_inexact_cause_is_fully_described() {
        let mut seen: HashSet<&str> = HashSet::new();
        for cause in Inexact::ALL {
            let wire = cause.as_str();
            assert!(!wire.is_empty(), "{cause:?} has no wire form");
            assert!(!cause.detail().is_empty(), "{wire} has no detail");
            assert!(!cause.instead().is_empty(), "{wire} has no repair");
            assert!(seen.insert(wire), "{wire} is used by two causes");
        }
    }

    /// A pushed condition compares stored text, so a constant is pushable only
    /// when it is the same RDF term the column's values render as.
    ///
    /// Both directions matter, and which one is which depends on the schema: an
    /// `integer` range that resolves to `xsd:integer` stores `3`, so `= 3` is
    /// the question and `= "3"` matches nothing. Were the type left unresolved
    /// the writer would emit a plain `"3"` and the two would swap — which is
    /// why this asks the descriptor rather than assuming either.
    #[test]
    fn a_constant_is_pushed_only_as_the_column_s_own_term() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> \
                      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

        for (query, pushed) in [
            // A typed integer column takes a number, not its text.
            (
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:length ?l . \
                 FILTER(?l = 3) }",
                true,
            ),
            (
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:length ?l . \
                 FILTER(?l = \"3\") }",
                false,
            ),
            // A string column is the mirror image.
            (
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(?nm = \"BX\") }",
                true,
            ),
            (
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(?nm = 3) }",
                false,
            ),
            // A tag is a different term whatever the column.
            (
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(?nm = \"BX\"@en) }",
                false,
            ),
            // Same rule through VALUES and IN, not just `=`.
            (
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:length ?l . \
                 VALUES ?l { 3 } }",
                true,
            ),
            (
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:length ?l . \
                 FILTER(?l IN (3)) }",
                true,
            ),
        ] {
            let plan = sparql_scope(&format!("{prefix}{query}"), &sv).unwrap();
            assert_eq!(
                plan.inexact.is_none(),
                pushed,
                "wrong verdict for: {query} (got {:?})",
                plan.inexact
            );
        }
    }

    /// The plainest query there is: name one instance, ask for one of its
    /// values. Its class is inferred from the slot, so no `rdf:type` triple
    /// exists — and reading that absence as an OPTIONAL depth got the query
    /// refused for a block it does not contain.
    #[test]
    fn a_constant_subject_without_a_type_is_not_optional() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?nm WHERE { <https://data.infrabel.be/asset360/signal/A> \
             asset360:name ?nm }",
            &sv,
        )
        .expect("an ordinary constant-subject query is not a disconnected OPTIONAL");

        let star = &plan.root.all_stars()[0];
        assert!(!star.is_optional, "nothing here is optional");
        assert_eq!(
            star.identifier_values,
            ["https://data.infrabel.be/asset360/signal/A"]
        );
    }

    /// A join edge claims the slot holds the other class's URI. An inlined slot
    /// holds the structure itself, so there is no such column — and the same
    /// question without the nested `rdf:type` is a path the plan does carry.
    ///
    /// Ground truth from the writer: an inlined object *is* given an
    /// `rdf:type` triple, so the typed form is a legal question answering
    /// exactly what the untyped one answers — which is why the hint says to
    /// drop the type rather than to change the question.
    #[test]
    fn an_inlined_structure_is_never_joined_as_a_reference() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        // Typed: refused, with a cause that names the fix.
        let plan = sparql_scope(
            &format!(
                "{prefix}SELECT ?lo WHERE {{ ?s a asset360:Signal ; asset360:location ?c . \
                 ?c a asset360:Coordinates ; asset360:longitude ?lo }}"
            ),
            &sv,
        )
        .unwrap();
        assert_eq!(plan.inexact, Some(Inexact::TypedNestedStructure));
        // The star for ?c is still built — Phase 1 makes one for any typed
        // subject — but nothing joins to it, which is the part that was wrong.
        // Unjoined it costs a fetch returning no rows (an inlined structure is
        // not a record of its own), and the recorded loss is what stops
        // anything answering from this plan.
        assert!(
            plan.root.all_joins().is_empty(),
            "an inlined slot is not a foreign key"
        );

        // A wrong type on the nested variable is the same shape, not a
        // different one — it used to emit the same bogus edge.
        let plan = sparql_scope(
            &format!(
                "{prefix}SELECT ?lo WHERE {{ ?s a asset360:Signal ; asset360:location ?c . \
                 ?c a asset360:Track ; asset360:longitude ?lo }}"
            ),
            &sv,
        )
        .unwrap();
        assert!(
            plan.inexact.is_some(),
            "a wrong nested type is still a loss"
        );

        // Untyped: the path, exact.
        let plan = sparql_scope(
            &format!(
                "{prefix}SELECT ?lo WHERE {{ ?s a asset360:Signal ; asset360:location ?c . \
                 ?c asset360:longitude ?lo }}"
            ),
            &sv,
        )
        .unwrap();
        assert_eq!(plan.inexact, None);
        assert!(plan.path_bindings.contains_key("lo"));

        // A real reference still joins.
        let plan = sparql_scope(
            &format!(
                "{prefix}SELECT ?tn WHERE {{ ?s a asset360:Signal ; \
                 asset360:locatedOnTrack ?t . ?t a asset360:Track ; asset360:hasName ?tn }}"
            ),
            &sv,
        )
        .unwrap();
        assert_eq!(plan.inexact, None);
        assert_eq!(plan.root.all_joins().len(), 1, "a reference is a join");
    }

    /// A filter on a multivalued slot is a containment test, and a consumer
    /// cannot tell from `filters` alone — so the star says which fields hold
    /// arrays. `:trafficKinds "m"` matches a record whose array contains "m";
    /// comparing the array's text matches nothing.
    #[test]
    fn a_star_says_which_of_its_fields_hold_arrays() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s WHERE { ?s a asset360:Signal ; asset360:trafficKinds \"m\" ; \
             asset360:name \"BX\" }",
            &sv,
        )
        .unwrap();

        let star = &plan.root.all_stars()[0];
        assert!(star.filters.contains_key("trafficKinds"));
        assert_eq!(star.multivalued_fields, ["trafficKinds"]);
        assert!(
            !star.multivalued_fields.contains(&"name".to_owned()),
            "a single-valued slot is not an array"
        );
    }

    /// A condition on a value inside a nested structure. `?m :zoneName ?z .
    /// FILTER(?z = "X")` reads two slots down, which no column key can name --
    /// and dropping it counted every record where the query counts some.
    #[test]
    fn a_filter_on_a_nested_value_is_pushed_as_a_path() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        let plan = sparql_scope(
            &format!(
                "{prefix}SELECT (COUNT(*) AS ?n) WHERE {{ ?s a asset360:Signal ; \
                 asset360:location ?c . ?c asset360:longitude ?lo . FILTER(?lo = 4) }}"
            ),
            &sv,
        )
        .unwrap();
        assert_eq!(plan.inexact, None, "a nested FILTER is pushable");
        let star = &plan.root.all_stars()[0];
        assert_eq!(
            star.path_filters,
            vec![PathFilter {
                slot_path: vec!["location".to_owned(), "longitude".to_owned()],
                conditions: vec![FilterCondition::Eq("4".to_owned())],
                numeric: true,
            }],
            "the condition names the path, not a column"
        );
        assert!(
            star.filters.is_empty(),
            "a nested value is not a column of the record"
        );
    }

    /// The same value written as a constant on the nested step rather than
    /// through a FILTER. One question, so one answer.
    #[test]
    fn a_constant_on_a_nested_step_is_pushed_as_a_path() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:location ?c . ?c asset360:longitude 4 }",
            &sv,
        )
        .unwrap();
        assert_eq!(plan.inexact, None, "a nested constant is pushable");
        assert_eq!(
            plan.root.all_stars()[0].path_filters,
            vec![PathFilter {
                slot_path: vec!["location".to_owned(), "longitude".to_owned()],
                conditions: vec![FilterCondition::Eq("4".to_owned())],
                numeric: true,
            }]
        );
    }

    /// An enum column stores a code, not the term it renders as. All three
    /// shapes of that, on one partially mapped enum: `GSA` carries a meaning
    /// and renders as an IRI, `KSS` carries none and renders as itself.
    #[test]
    fn an_enum_constant_is_translated_back_to_the_stored_code() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        // An IRI constant selects the code whose meaning it is.
        let plan = sparql_scope(
            &format!(
                "{prefix}SELECT ?s WHERE {{ ?s a asset360:Signal ; \
                 asset360:kind <http://ontorail.org/src/Eulynx/GSA> }}"
            ),
            &sv,
        )
        .unwrap();
        assert_eq!(plan.inexact, None, "an IRI constant on an enum is pushable");
        assert_eq!(
            plan.root.all_stars()[0].filters["kind"],
            vec![FilterCondition::Eq("GSA".to_owned())],
            "the pushed condition names the stored code, not the IRI"
        );

        // A literal constant selects itself, when the value has no meaning.
        let plan = sparql_scope(
            &format!("{prefix}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:kind \"KSS\" }}"),
            &sv,
        )
        .unwrap();
        assert_eq!(plan.inexact, None);
        assert_eq!(
            plan.root.all_stars()[0].filters["kind"],
            vec![FilterCondition::Eq("KSS".to_owned())]
        );

        // The literal spelling of a mapped code is a term nothing renders as.
        // Pushing it would answer with every GSA record where SPARQL answers
        // with none.
        let plan = sparql_scope(
            &format!("{prefix}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:kind \"GSA\" }}"),
            &sv,
        )
        .unwrap();
        assert_eq!(plan.inexact, Some(Inexact::EnumConstantUnmatched));
        assert!(
            !plan.root.all_stars()[0].filters.contains_key("kind"),
            "nothing is pushed for a constant no record renders as"
        );
    }

    /// The same rule through `FILTER(?k = ...)` and `IN` -- the two routes that
    /// drifted from the inline one before.
    #[test]
    fn an_enum_constant_is_translated_in_a_filter_too() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        for (query, expected) in [
            (
                format!(
                    "{prefix}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:kind ?k . \
                     FILTER(?k = <http://ontorail.org/src/Eulynx/GSA>) }}"
                ),
                FilterCondition::Eq("GSA".to_owned()),
            ),
            (
                format!(
                    "{prefix}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:kind ?k . \
                     FILTER(?k = \"KSS\") }}"
                ),
                FilterCondition::Eq("KSS".to_owned()),
            ),
            (
                format!(
                    "{prefix}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:kind ?k . \
                     FILTER(?k IN (<http://ontorail.org/src/Eulynx/GSA>, \"KSS\")) }}"
                ),
                FilterCondition::In(vec!["GSA".to_owned(), "KSS".to_owned()]),
            ),
        ] {
            let plan = sparql_scope(&query, &sv).unwrap();
            assert_eq!(plan.inexact, None, "not pushed: {query}");
            assert_eq!(
                plan.root.all_stars()[0].filters["kind"],
                vec![expected],
                "wrong condition for: {query}"
            );
        }
    }

    /// A comparison on a numeric slot has to cast, and a consumer holding only
    /// `filters` cannot tell which slot that is: the slot appears in no
    /// binding, so no term descriptor reaches it. `'9' >= '10'` is true as text
    /// and false as a number, which is a wrong answer in silence.
    #[test]
    fn a_star_says_which_of_its_fields_compare_as_numbers() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:length ?len ; \
             asset360:name ?nm . FILTER(?len >= 10) }",
            &sv,
        )
        .unwrap();

        let star = &plan.root.all_stars()[0];
        assert!(star.filters.contains_key("length"), "the comparison pushed");
        assert_eq!(star.numeric_fields, ["length"]);
        assert!(
            !star.numeric_fields.contains(&"name".to_owned()),
            "a string slot compares by codepoint, not by value"
        );
    }

    /// `= <iri>` and `IN (<iri>)` are one rule, and they drifted: only `IN`
    /// learned that a reference column compares against an IRI.
    #[test]
    fn an_iri_constant_is_pushed_by_equality_as_well_as_in() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        for query in [
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             FILTER(?t = <https://data.infrabel.be/asset360/track/T1>) }",
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             FILTER(?t IN (<https://data.infrabel.be/asset360/track/T1>)) }",
        ] {
            let plan = sparql_scope(&format!("{prefix}{query}"), &sv).unwrap();
            assert_eq!(plan.inexact, None, "should push: {query}");
        }

        // And the cross-term cases stay refused in both: a literal is not an
        // IRI, whichever operator asks.
        for query in [
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             FILTER(?t = \"T1\") }",
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             FILTER(?nm = <https://data.infrabel.be/asset360/track/T1>) }",
        ] {
            let plan = sparql_scope(&format!("{prefix}{query}"), &sv).unwrap();
            assert!(plan.inexact.is_some(), "should refuse: {query}");
        }
    }

    /// A path deeper than two hops is still one path, and the plan carries the
    /// whole chain. The intermediate node is a step rather than a value, so it
    /// never becomes a path binding — which must not be read as "unrepresented".
    #[test]
    fn a_three_hop_nested_path_is_exact() {
        let sv = test_schema_view();
        let query = "PREFIX asset360: <https://data.infrabel.be/asset360/> \
                     SELECT ?v WHERE { ?s a asset360:Signal ; asset360:location ?c . \
                     ?c asset360:detail ?d . ?d asset360:value ?v }";

        let plan = sparql_scope(query, &sv).unwrap();
        assert_eq!(plan.inexact, None, "a walked path is not a loss");
        let binding = plan.path_bindings.get("v").expect("?v is a path binding");
        assert_eq!(binding.slot_path, ["location", "detail", "value"]);
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
            // A single-valued slot read as a constant and through a variable is
            // one read: the constant fixes what the variable binds. Only the
            // multivalued shape is a self-join.
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name \"BX\" ; \
             asset360:name ?nm }",
            // One column, several rows, and one row over several columns: both
            // are the same question as the IN the plan carries.
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             VALUES ?nm { \"BX1\" \"BX2\" } }",
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm ; \
             asset360:length ?l . VALUES (?nm ?l) { (\"BX1\" 4) } }",
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
