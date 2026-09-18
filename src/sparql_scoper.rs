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

use spargebra::algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression};
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
/// What a [`QueryPlan::sql_limit`] bounds.
///
/// Both variants bound *the scan the fetch drives from*; they differ in
/// whether that scan is also the statement's row set. See
/// [`bound_applies_to_the_driving_scan`] for the join shape and why its bound
/// must not land on the product.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitScope {
    /// One relation, no join, no `OPTIONAL`: a row is a solution, and the
    /// bound is an outer `LIMIT` on the rows.
    Rows,
    /// A rooted `OPTIONAL` join: the bound is on the mandatory star's scan
    /// only, and the joined rows are **not** capped.
    DrivingScan,
}

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
    /// How many rows the object fetch may be limited to: the query's `LIMIT`,
    /// and only when it carries no `OFFSET`.
    ///
    /// An offset is a position in a sequence the fetch and the engine do not
    /// share, so no bound covers it — see [`pushable_limit`]. A paged query
    /// is fetched whole and paged by the engine.
    ///
    /// **It bounds the scan the fetch drives from**, which for a single-class
    /// query is the whole row set and for a joined one is the mandatory star's
    /// scan — see [`bound_applies_to_the_driving_scan`] for which joined shapes
    /// carry a bound at all, and why applying it to the join *product* instead
    /// returns fewer solutions than the query asked for.
    ///
    /// Only set for a plan that describes the whole question (see `inexact`)
    /// and whose modifiers let a limit apply before them.
    pub sql_limit: Option<usize>,

    /// What `sql_limit` bounds, for the consumer that has to tell the two
    /// shapes apart — and the reason it is a field rather than something
    /// re-derived from the stars and joins.
    ///
    /// [`LimitScope::Rows`] is the single-relation shape, where the fetch's
    /// rows stand one-for-one with the query's solutions and the bound may
    /// be applied as an outer `LIMIT` on whatever statement carries them.
    /// [`LimitScope::DrivingScan`] is the rooted-`OPTIONAL` join, where the
    /// rows are a product and the bound holds only for the mandatory star's
    /// scan. `scope_union` stacks branches into one statement and applies
    /// one bound to the stack, which is sound for the first and not for the
    /// second; it reads this rather than repeating the analysis.
    ///
    /// `None` exactly when `sql_limit` is `None`.
    pub sql_limit_scope: Option<LimitScope>,

    /// The same bound, for the one shape this struct cannot decide alone: a
    /// `UNION` that the lowering turns into a **single** `UNION ALL`
    /// statement.
    ///
    /// `sql_limit` is `None` for every union, and stays that way, because a
    /// union is several statements unless the lowering manages to stack it:
    /// ten rows of one arm are not the ten the query asked for. One statement
    /// over all the arms *is* the union's rows, and then the same bound is
    /// sound for the same reason it is sound for a single class.
    ///
    /// Set only when **every** branch's own plan carries a `sql_limit` whose
    /// scope is [`LimitScope::Rows`] — so every branch is exact,
    /// single-relation and `OPTIONAL`-free, and each branch's rows stand
    /// one-for-one with its solutions. A branch bounded on its *driving scan*
    /// declines: its rows are a join product, and an outer `LIMIT` on the
    /// stacked statement would cap that product, which is the short answer
    /// with no error that the driving-scan bound exists to avoid. That is the
    /// scoper's existing analysis, asked once per branch rather than
    /// re-derived, which is what keeps one owner for the question of whether a
    /// limit may reach a fetch at all.
    ///
    /// `None` everywhere else, including for every non-union plan: a consumer
    /// reading this instead of `sql_limit` would bound a fetch that is not a
    /// union, and `lower_refined` picks between the two by whether the
    /// statement it built is actually rooted in a `UNION ALL`.
    pub sql_limit_if_unioned: Option<usize>,

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

    /// Stars this scoping *recorded* as untyped rather than refused: the
    /// qualified variable (`s`, or `s__d1` for the `?s` of naming domain 1)
    /// of every subject its own domain leaves without a class.
    ///
    /// Empty unless the plan was scoped in [`Scoping::Record`] mode, which
    /// the refinement pipeline uses so that the refined plan gets its one
    /// chance to type such a star (a boundary restriction carried to its
    /// scan) before `resolve` refuses it -- the only place an unscoped
    /// refusal is final (design, *The pipeline*). Their triples are among
    /// `unconsumed` and the plan is inexact.
    pub untyped: Vec<String>,
}

/// What a scoping does with a star its domain leaves untyped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scoping<'h> {
    /// Refuse it, as the scoper always has: `ScopeError::Unscoped`.
    Refuse,
    /// Record it in [`QueryPlan::untyped`] and scope the rest.
    Record,
    /// Type it from the classes the refined plan derived, keyed by qualified
    /// variable; refuse what the plan did not type either.
    Resolve(&'h HashMap<String, String>),
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
    /// A `UNION`. The plan holds the stars of every branch, so the fetch
    /// covers all of them, but no branch's constraints hold of the query's
    /// answers as a whole: one branch's `FILTER` does not narrow the other's
    /// rows, and the plan carries no way to say which star belongs to which
    /// branch.
    UnionBranch,
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
            Self::UnionBranch => "union_branch",
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
            Self::UnionBranch => {
                "the query is a UNION, and the plan is the union of its \
                 branches' fetches — wide enough to answer, but it does not \
                 say which branch a constraint belongs to"
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
            Self::UnscopedSubject => {
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
            Self::UnionBranch => {
                "Nothing: a UNION is answered from the branches' combined \
                 fetch. Issue each branch as its own query if you need the \
                 narrower fetch each one allows."
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
pub(crate) fn enum_codes(meanings: &[(String, String)], term: &TermPattern) -> Option<Vec<String>> {
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

/// A nested read the scan restates as a presence check, so that a fetched
/// row is a row the query has a solution for.
///
/// `?s :superStructure ?c . ?c :hasMaterial ?v` requires a value at the end
/// of a path. [`Star::required_fields`] restates the first hop — the record
/// holds a `superStructure` — and stops there, so a record whose structure
/// lacks the material was fetched and yielded nothing. Harmless while the
/// engine re-runs the query over everything fetched; fatal under a fetch
/// bound, whose premise is "each fetched row yields at least one solution":
/// `LIMIT 50` read fifty records, twelve of them empty, and answered 38 with
/// no error (issue #455, pepibru GitLab).
///
/// One entry per mandatory nested read of a mandatory star. A consumer
/// renders every entry of one star as **one** predicate over the record —
/// two leaves under the same collection hop have to be found on the *same*
/// element, because that is the one blank node the two triples share — with
/// each leaf present and not JSON `null` (an explicit `null` emits no
/// triple, see `sql_builder._presence_expression`). In Postgres that is
/// `jsonb_path_exists(object_data, '$."a"[*]."b"[*] ? (@ != null)')`,
/// with `[*]` at a list hop, `.*` at a mapping hop and nothing at a
/// single-valued one.
///
/// **Not optional to read.** A renderer that leaves these out and applies
/// [`QueryPlan::sql_limit`] answers a short page silently, which is the
/// defect this exists to close.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequiredPath {
    /// Slots from the record's root to the value, always at least two long:
    /// a single slot is a column and lives in [`Star::required_fields`].
    pub slot_path: Vec<String>,
    /// Parallel to `slot_path`: how each step is stored, which decides how
    /// the predicate steps through it.
    pub containers: Vec<crate::sparql_pushdown::Container>,
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

    /// [`Self::all_stars`], for a caller that edits a star in place.
    ///
    /// One caller: `sparql_plan::keep_what_the_rules_proved`, which merges the
    /// narrowings a refined plan derived into the decomposition the fallback
    /// fetches. It edits rather than rebuilds because a star carries facts the
    /// refined plan does not restate -- `multivalued_fields` for every slot the
    /// query mentions, `numeric_fields`, the optional marking -- and a rebuilt
    /// star would have to re-derive all of them from a second source.
    pub fn all_stars_mut(&mut self) -> Vec<&mut Star> {
        let mut out = Vec::new();
        self.visit_stars_mut(&mut out);
        out
    }

    fn visit_stars_mut<'a>(&'a mut self, out: &mut Vec<&'a mut Star>) {
        match self {
            PlanNode::Bgp { stars, .. } => {
                for s in stars {
                    out.push(s);
                }
            }
            PlanNode::LeftJoin { left, right } => {
                left.visit_stars_mut(out);
                right.visit_stars_mut(out);
            }
        }
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

    /// Nested reads the scan restates as presence checks — see
    /// [`RequiredPath`] for what one is and why a renderer must not skip it.
    ///
    /// Only for a mandatory star and only for reads outside any `OPTIONAL`:
    /// the fetch bound rests on this star's rows, and an optional read leaves
    /// a variable unbound rather than costing the row its solution. Sorted by
    /// path, so two plans of one query render the same predicate.
    ///
    /// Filled in Phase 3 alongside `path_filters`. A mandatory nested read
    /// that cannot be restated here declines the fetch bound instead
    /// (`nested_presence_of`), never a third option where the bound stays and
    /// the page is short.
    pub required_paths: Vec<RequiredPath>,

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
///
/// — but only when the slot is single-valued. A *multivalued* reference holds
/// a JSON **array** of identifiers, and `->>` on an array yields the array's
/// own text (`["…/Ports/1", "…/Ports/2"]`), which equals no identifier — so
/// that ON clause matches nothing and the join is silently empty. Which of the
/// two it is, is what [`JoinEdge::right_multivalued`] says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinEdge {
    /// Variable of the referenced star (the join target).
    pub left: String,

    /// Variable of the star holding the foreign key.
    pub right: String,

    /// The slot on the right star whose value equals left's `asset360_uri`.
    /// E.g. `"belongsToTunnelComplex"`.
    pub right_slot: String,

    /// The inline hops from the right record's root to `right_slot`, when the
    /// reference sits *inside* an inline structure rather than on the record
    /// itself. Empty for a column, which every edge was until the path walk
    /// started raising edges.
    ///
    /// `?s :hasCoveredSection ?cs . ?cs :belongsToTrack ?t . ?t a :Track` is
    /// `right_path = ["hasCoveredSection"]`, `right_slot = "belongsToTrack"`:
    /// the identifier is two slots down, and the first of them is a list. A
    /// renderer cannot state that as a column comparison, so the fact is on
    /// the edge -- a renderer that overlooks it compares
    /// `object_data->>'belongsToTrack'` on a record that has no such column
    /// and answers *empty*, which is the failure `right_multivalued` exists to
    /// prevent one level up.
    pub right_path: Vec<String>,

    /// Whether `right_slot` holds a *collection* of identifiers rather than
    /// one. On a path edge: whether *any* hop, or the slot, does.
    ///
    /// Stated here for the reason [`Star::multivalued_fields`] and
    /// `PlanOp::reading` are: a renderer that has to fetch this fact from the
    /// schema is a renderer that can forget to fetch it, and the failure mode
    /// when it forgets is an equality against an array's text — no error, no
    /// rows, and a join that reports "no such data" for data that is there.
    pub right_multivalued: bool,

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
    /// An anti-join: rows of the left with *no* matching row on the right,
    /// which is what `FILTER NOT EXISTS { ... }` asks for. Rendered as a
    /// correlated `NOT EXISTS` rather than as a `JOIN` clause, so the right
    /// side contributes no columns and no records — there are none to
    /// contribute.
    Anti,
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
    /// A substring match lifted from `STRSTARTS` / `STRENDS` / `CONTAINS`.
    ///
    /// Carries the value the query wrote, *unescaped*: `%` and `_` are
    /// metacharacters of the renderer's `LIKE`, not of SPARQL, so escaping
    /// them is the renderer's convention and a pre-built pattern would hide
    /// the difference between a wildcard and a user searching for `50%`.
    Like {
        value: String,
        anchor: LikeAnchor,
        /// The query wrapped the *column* in `LCASE(...)`. The renderer emits
        /// `ILIKE`.
        ///
        /// Load-bearing and silent when wrong: the engine leg folds case and
        /// SQL's `LIKE` does not, so the two routes answer different row sets
        /// and neither reports anything. `LCASE` on the *constant* is a
        /// different question and does not set this — see
        /// `lcase_on_the_constant_does_not_become_case_insensitive`.
        case_insensitive: bool,
    },
    /// `FILTER(?v != "x")` — `expr IS NOT NULL AND expr <> 'x'`.
    ///
    /// Not a fifth [`CmpOp`], and the null test in that rendering is why.
    /// SPARQL's inequality is false for an unbound variable where SQL's `<>`
    /// on NULL is unknown, so a bare `<>` drops exactly the rows an
    /// `OPTIONAL` exists to keep. A separate arm makes the renderer state
    /// the asymmetry instead of inheriting a rendering that ignores it.
    Ne(String),
    /// `OPTIONAL { ?s :slot ?x } FILTER(!bound(?x))` — the slot is absent.
    ///
    /// Rendered as the negation of the presence check the builder
    /// deliberately skips for an optional field. No value: absence is not a
    /// comparison.
    ///
    /// Only ever pushed for a one-hop *optional* slot. A required slot
    /// already carries the positive check, so `!bound` on it selects nothing
    /// — which the plan cannot say — and a nested path's absence is a
    /// different predicate from a missing key.
    NotBound,
    /// `FILTER(geof:sfIntersects(?wkt, "..."^^geo:wktLiteral))` on a slot the
    /// broken-out column registry claims.
    ///
    /// The WKT body, with a CRS84 prefix stripped. Only CRS84 lifts: `spargeo`
    /// reads a geometry only from a `wktLiteral`/`geoJSONLiteral` in CRS84
    /// and returns unbound otherwise, which makes the FILTER silently false —
    /// so a lifted non-CRS84 geometry would answer rows on the statement
    /// route where the engine answers none.
    ///
    /// `geof:area` and `geof:distance` are deliberately not here: they are
    /// geodesic in `spargeo` and planar in PostGIS on `geometry(4326)`, so the
    /// two routes would not agree. Topological predicates do.
    Intersects { wkt: String },
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
            Self::Like {
                value,
                anchor,
                case_insensitive,
            } => {
                let op = if *case_insensitive { "ILIKE" } else { "LIKE" };
                let pattern = match anchor {
                    LikeAnchor::Prefix => format!("{value}%"),
                    LikeAnchor::Suffix => format!("%{value}"),
                    LikeAnchor::Anywhere => format!("%{value}%"),
                };
                write!(f, "{op} '{pattern}'")
            }
            Self::Ne(value) => write!(f, "IS NOT NULL AND <> '{value}'"),
            Self::NotBound => write!(f, "IS NOT PRESENT"),
            Self::Intersects { wkt } => write!(f, "INTERSECTS '{wkt}'"),
        }
    }
}

/// The IRI `geof:sfIntersects` parses to, confirmed by printing a parse of
/// `FILTER(geof:sfIntersects(...))` rather than assumed: it arrives as
/// `Function::Custom(NamedNode { iri: .. })`, and this is that IRI's text.
pub(crate) const SF_INTERSECTS_IRI: &str =
    "http://www.opengis.net/def/function/geosparql/sfIntersects";

/// The GeoSPARQL datatype `geo:wktLiteral`. `spargeo` reads a geometry only
/// from a literal typed exactly this or `geo:geoJSONLiteral`
/// (`parse.rs::extract_argument`); anything else — an `xsd:string`, most
/// obviously — makes the function return unbound, so
/// `FILTER(geof:sfIntersects(...))` is silently false there. Lifting a
/// non-`wktLiteral` constant would answer rows on the statement route where
/// the engine answers none — the quietest possible route disagreement.
pub(crate) const WKT_LITERAL_IRI: &str = "http://www.opengis.net/ont/geosparql#wktLiteral";

/// The only coordinate reference system `spargeo` accepts a leading `<uri>`
/// prefix for (`parse.rs::parse_wkt_literal`); any other CRS makes it return
/// `None`. PostGIS transformed the stored value to 4326 on ingest and would
/// happily match a different CRS's numbers as if they were already in it, so
/// lifting one here would make the two routes disagree.
const CRS84_URI: &str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84";

/// The WKT body a `geof:sfIntersects` constant lifts as, or `None` for either
/// of the two literal-shaped refusals `spargeo`'s own parser enforces
/// (`parse.rs::extract_argument`, `parse_wkt_literal`): not typed exactly
/// `wktLiteral` (this also catches `geoJSONLiteral`, representable in
/// principle via `ST_GeomFromGeoJSON` but out of scope here — declining
/// leaves the engine answering it correctly), or prefixed with a CRS other
/// than CRS84.
///
/// Mirrors `parse_wkt_literal`'s trim-then-strip-prefix exactly, short of the
/// final `Geometry::try_from_wkt_str` — SQL, not this crate, is what
/// validates the WKT syntax itself. The returned body is therefore
/// CRS-stripped and trimmed, never re-validated as parseable WKT; the Python
/// renderer consumes it verbatim.
pub(crate) fn intersects_wkt_from_literal(literal: &spargebra::term::Literal) -> Option<String> {
    if literal.datatype().as_str() != WKT_LITERAL_IRI {
        return None;
    }
    let mut value = literal.value().trim();
    if let Some(rest) = value.strip_prefix('<') {
        let (system, rest) = rest.split_once('>').unwrap_or((rest, ""));
        if system != CRS84_URI {
            return None;
        }
        value = rest.trim_start();
    }
    Some(value.to_owned())
}

/// Where a [`FilterCondition::Like`] puts its wildcards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LikeAnchor {
    /// `STRSTARTS` — `LIKE 'value%'`.
    Prefix,
    /// `STRENDS` — `LIKE '%value'`.
    Suffix,
    /// `CONTAINS` — `LIKE '%value%'`.
    Anywhere,
}

impl LikeAnchor {
    /// The operator name the PyO3 boundary carries, without the case prefix.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prefix => "startswith",
            Self::Suffix => "endswith",
            Self::Anywhere => "contains",
        }
    }
}

/// Ordering operators liftable from a `FILTER` into SQL.
///
/// `!=` is not a fifth variant here, even though it lifts: it needs
/// `expr IS NOT NULL AND expr <> 'x'`, and no ordering comparison needs that
/// null test — SPARQL's inequality is false for an *unbound* variable, where
/// SQL's bare `<>` on NULL is unknown and would drop rows the query keeps. See
/// [`FilterCondition::Ne`] for that separate arm; equality is
/// [`FilterCondition::Eq`].
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
    ///
    /// `rewrite` is the clause to add, when the schema can spell it whole --
    /// `?t a <Class>` for the object of a reference, the `GRAPH` clause for
    /// a concept's labels -- and `None` when the best advice is the generic
    /// one (name a class; here is the namespace), which the endpoint owns
    /// because it knows the deployment's IRIs. Carried as data rather than
    /// as a sentence so the endpoint can tell the two apart without reading
    /// the prose: the `Display` puts [`UNSCOPED_REWRITE_NAMED`] in front of
    /// a refusal that names its rewrite, and that token is the whole
    /// contract.
    Unscoped {
        message: String,
        rewrite: Option<String>,
    },
    /// The input is a SPARQL Update (INSERT/DELETE), not supported.
    UpdateRejected,
    /// The query uses a SPARQL construct the scoper recognises but does
    /// not yet support (`UNION`, `MINUS`, property paths, disconnected
    /// `OPTIONAL`, `NOT EXISTS`, …). Reject with a clear message rather
    /// than silently returning wrong results.
    UnsupportedConstruct(String),
}

/// What an unscoped refusal that names its own rewrite opens with.
///
/// The endpoint attaches its generic "add a class" suggestion to a
/// `query_unscoped` refusal unless the message opens with this, because a
/// generic suggestion beside a spelled rewrite contradicts it -- for a
/// concept's labels there is no class to add (#465, pepibru GitLab). The
/// message is prose and may be reworded; this token is the one string the
/// endpoint depends on.
pub const UNSCOPED_REWRITE_NAMED: &str = "Query is unscoped (rewrite named):";

impl std::fmt::Display for ScopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScopeError::ParseError(msg) => write!(f, "SPARQL parse error: {msg}"),
            ScopeError::Unscoped {
                message,
                rewrite: None,
            } => write!(f, "Query is unscoped: {message}"),
            ScopeError::Unscoped {
                message,
                rewrite: Some(rewrite),
            } => write!(f, "{UNSCOPED_REWRITE_NAMED} {message} Add `{rewrite}`."),
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
    sparql_scope_with_schema_graph(query_str, schema_view, None)
}

/// [`sparql_scope`], for a deployment that serves a schema graph.
///
/// `schema_graph_iri` is the named graph the active datamodel serves its
/// datamodel in, which the endpoint reads from that datamodel's configuration.
/// It is a parameter and not a constant because `DATAMODEL` decides it — see
/// [`crate::sparql_schema_graph`]. `None`, which is what plain
/// [`sparql_scope`] passes, means no schema graph exists, so no pattern is in
/// one and every triple is scoped as an instance pattern.
pub fn sparql_scope_with_schema_graph(
    query_str: &str,
    schema_view: &SchemaView,
    schema_graph_iri: Option<&str>,
) -> Result<QueryPlan, ScopeError> {
    let mut query = parse_query(query_str)?;
    // The same spelling pass every other entry point runs, so a caller that
    // only scopes -- the config linter, through the `sparql_scope` binding --
    // accepts and refuses exactly what the planner and the engine leg do. See
    // [`crate::sparql_alias`].
    crate::sparql_alias::canonicalize(&mut query, schema_view)?;
    scope_parsed_with_schema_graph(&query, schema_view, schema_graph_iri)
}

/// The parser every entry point must use.
///
/// It preloads the prefixes a caller may leave implicit, which makes it part of
/// the endpoint's contract rather than a convenience: a query that omits
/// `PREFIX asset360:` parses here and nowhere else. Two entry points with two
/// parsers would accept two different languages — one would scope a query the
/// other rejects as a syntax error. That is not hypothetical: the executor and
/// [`crate::sparql_graph_clauses`] each parsed with a bare parser, so the
/// canonical `?s rdf:type <Class>` planned here and then failed to execute,
/// and a schema-graph discovery query was refused as *unscoped* because it did
/// not parse there.
///
/// The set is the four W3C vocabularies every SPARQL engine pre-registers plus
/// the two the datamodel's own schema graph is written in — `skos` for enum
/// values (`skos:notation`, `skos:inScheme`) and `schema` for slot domains
/// (`schema:domainIncludes`). Without those, the only way to write a discovery
/// query is with declarations that the query it is discovering *for* does not
/// need, which is a difference no caller can be expected to guess.
///
/// These are defaults, not overrides: a query that declares a label itself wins
/// (the parser's in-query declaration overwrites the seeded one), so a caller
/// who binds `rdf:` to something else gets the query they wrote.
pub fn sparql_parser() -> SparqlParser {
    SparqlParser::new()
        .with_prefix("asset360", "https://data.infrabel.be/asset360/")
        .expect("hardcoded prefix")
        .with_prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        .expect("hardcoded prefix")
        .with_prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
        .expect("hardcoded prefix")
        .with_prefix("owl", "http://www.w3.org/2002/07/owl#")
        .expect("hardcoded prefix")
        .with_prefix("xsd", "http://www.w3.org/2001/XMLSchema#")
        .expect("hardcoded prefix")
        .with_prefix("skos", "http://www.w3.org/2004/02/skos/core#")
        .expect("hardcoded prefix")
        .with_prefix("schema", "https://schema.org/")
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
    scope_parsed_with_schema_graph(query, schema_view, None)
}

/// [`scope_parsed`], for a deployment that serves a schema graph.
///
/// See [`sparql_scope_with_schema_graph`] for what `schema_graph_iri` is and
/// why it is not a constant.
pub fn scope_parsed_with_schema_graph(
    query: &Query,
    schema_view: &SchemaView,
    schema_graph_iri: Option<&str>,
) -> Result<QueryPlan, ScopeError> {
    scope_parsed_as(query, schema_view, schema_graph_iri, Scoping::Refuse)
}

/// [`scope_parsed_with_schema_graph`] with a choice of what to do with an
/// untyped star. The pipeline's entry point.
///
/// **A star is a `(naming domain, variable)`.** The query is first read with
/// every variable inside a sub-select qualified by its domain
/// ([`crate::sparql_domains::qualify`]), so the star construction below --
/// which keys by variable name and is otherwise unchanged -- is per domain
/// by construction: a private inner `?s` is `?s__d1`, a star of its own,
/// typed from its own domain's triples or not at all.
pub fn scope_parsed_as(
    query: &Query,
    schema_view: &SchemaView,
    schema_graph_iri: Option<&str>,
    scoping: Scoping<'_>,
) -> Result<QueryPlan, ScopeError> {
    let qualified = crate::sparql_domains::qualify(query);
    scope_qualified(&qualified, schema_view, schema_graph_iri, scoping)
}

/// The scoping proper, over a query whose variables are already qualified
/// by naming domain. A `UNION` branch re-enters here, not above: the
/// branch is a pattern of the qualified query, and qualifying it twice would
/// suffix a name twice.
fn scope_qualified(
    query: &Query,
    schema_view: &SchemaView,
    schema_graph_iri: Option<&str>,
    scoping: Scoping<'_>,
) -> Result<QueryPlan, ScopeError> {
    // A `FROM` / `FROM NAMED` clause redefines the dataset the query is asked
    // against, and nothing downstream of here carries it: every arm below
    // discards it with `..`, the plan has no field for it, and
    // `sparql_materialise` rebuilds a query with `dataset: None`. So
    //
    //     SELECT (COUNT(*) AS ?n) FROM <urn:empty> WHERE { ?s a :TunnelComplex }
    //
    // was planned as *all in SQL*, admitted, and answered `2` from the table
    // — against a dataset holding no triples, where the answer is `0`. The
    // engine gets the original query text and honours the clause, so the two
    // routes answered different questions and which one you got depended on
    // whether a rule happened to lift the pattern.
    //
    // Refused by name, and refused for *both* routes rather than merely kept
    // out of SQL. Routing it to the engine would answer this query correctly
    // today, and it would rest on "no analysis's conclusion changes under a
    // dataset clause" — an invariant that is true by inspection and recorded
    // nowhere, which is the arrangement that produced this defect. When the
    // dataset is a field of the plan it can be honoured; until then the
    // refusal is the only answer that is checkable. See doc 28h.
    if dataset_of(query).is_some() {
        return Err(ScopeError::UnsupportedConstruct(
            "a FROM / FROM NAMED dataset clause is not supported; the endpoint serves one \
             dataset and the query plan does not carry a dataset restriction, so honouring \
             the clause on one route and not the other would answer a different question"
                .into(),
        ));
    }

    // An `OPTIONAL` joined to what precedes it only through a variable that
    // may be unbound there is a cartesian product per the algebra, and never
    // what the author meant. Refused for both routes, with the nested
    // spelling; see `crate::sparql_optional_binding`.
    if let Some(shape) = crate::sparql_optional_binding::optional_on_a_maybe_unbound_variable(query)
    {
        return Err(ScopeError::UnsupportedConstruct(shape.to_string()));
    }

    let pattern = match query {
        Query::Select { pattern, .. } => pattern,
        Query::Construct { pattern, .. } => pattern,
        Query::Describe { pattern, .. } => pattern,
        Query::Ask { pattern, .. } => pattern,
    };

    // A `UNION` is scoped one branch at a time and the fetches merged: a
    // star holds one class, so two arms typing one variable differently
    // cannot share one. See `union_branches` for why distributing is sound
    // for a *fetch* even though it is not a rewrite of the query.
    if let Some(branches) = union_branches(pattern)? {
        return scope_union(query, &branches, schema_view, schema_graph_iri, scoping);
    }

    // Phase 0: Depth-tag every BGP triple, rejecting unsupported
    // constructs along the way (UNION, MINUS, property paths).
    let mut triples_with_depth: Vec<(&TriplePattern, usize)> = Vec::new();
    tag_triples_by_depth(pattern, 0, &mut triples_with_depth)?;

    // Phase 0b: forget what a `GRAPH` clause asked of the *schema* graph.
    //
    // Scoping decides which golden records to fetch. The endpoint serves one
    // named graph and it holds the datamodel, so a pattern inside it names
    // schema terms and no record at all. Feeding those into star building asks
    // the wrong question and gets a wrong answer: a schema pattern with a
    // constant IRI subject — which is how every enum-value lookup is written —
    // was rejected as an unscopable instance subject.
    //
    // Any *other* named graph keeps the behaviour it had. The endpoint holds no
    // such graph, so the triples inside are still walked into the fetch and the
    // plan is still marked `Inexact::NamedGraph`, leaving the engine to answer
    // from a graph that is empty. Over-fetching for a graph nobody has is
    // wasteful, not wrong, and narrowing it is a separate change.
    //
    // Filtered here rather than inside `tag_triples_by_depth`, because that
    // enumeration is also the obligation list the plan refiner consumes
    // positionally: dropping triples there would leave the refiner's algebra
    // walk claiming obligations that no longer exist.
    let schema_triples = triples_in_the_schema_graph(pattern, schema_graph_iri);
    if !schema_triples.is_empty() {
        triples_with_depth
            .retain(|(triple, _)| !schema_triples.contains(&std::ptr::from_ref(*triple)));
    }

    // Anything dropped along the way is recorded here, at the point it is
    // dropped. The first cause wins: one actionable reason beats a list.
    let mut inexact: Option<Inexact> = None;
    // Stars recorded as untyped, in `Scoping::Record` mode.
    let mut untyped: Vec<String> = Vec::new();
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

    // Subjects with at least one triple in the default graph -- the ones that
    // stand for golden records, and the only ones the untyped-subject refusal
    // below is about. A subject seen only inside a `GRAPH` or `SERVICE` block
    // is somebody else's, and the block's own inexactness accounts for it.
    let foreign_triples = triples_outside_the_default_graph(pattern);
    let mut default_graph_subjects: HashSet<String> = HashSet::new();

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

        if !foreign_triples.contains(&std::ptr::from_ref(*tp)) {
            default_graph_subjects.insert(subj_var.clone());
        }

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
        // can't scope yields `None` and is set aside for the path walk. A
        // constant-IRI subject we can't scope yields `Err` and rejects the
        // whole query — never a silent drop that returns wrong data.
        let hints: &HashMap<String, String> = match scoping {
            Scoping::Resolve(hints) => hints,
            _ => &HashMap::new(),
        };
        let (class_uri, identifier_slot_name) =
            match resolve_star_class(builder, schema_view, hints)? {
                Some(resolved) => resolved,
                None => {
                    // A variable subject whose class cannot be resolved. Two very
                    // different things look like this, and only the path walk can
                    // tell them apart: a step inside another star's nested
                    // structure (`?s :location ?loc . ?loc :longitude ?v`), which
                    // the plan *does* represent as a path, and a subject nothing
                    // accounts for (`?sig :locatedOnTrack ?t` with ?sig untyped),
                    // whose records no star fetches -- refused after the walk,
                    // with the class to add where the schema knows it.
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
                        // A substring match is not a finite value list either
                        // — same reasoning as `Cmp` — so it stays a filter
                        // and the renderer targets `asset360_uri` with LIKE.
                        like @ FilterCondition::Like { .. } => kept.push(like),
                        // Nor is `!=`: "not this one value" has no list to
                        // hoist, so it stays a filter and the renderer's null
                        // test runs against `asset360_uri` itself.
                        ne @ FilterCondition::Ne(_) => kept.push(ne),
                        // `inline_filters` here holds constants seeded from
                        // triple patterns (Phase 1), never a `!bound` --
                        // that only ever comes from `FILTER` and lands in
                        // `star_filters`, handled separately below. Kept
                        // rather than dropped anyway, on the same "no value
                        // list to hoist" reasoning as `Ne`, so the match
                        // stays exhaustive without asserting unreachability
                        // it cannot prove.
                        nb @ FilterCondition::NotBound => kept.push(nb),
                        // Same reasoning again, and doubly unreachable: a
                        // geometry predicate has no value list to hoist
                        // either, and `inline_filters` never carries one
                        // regardless -- `lift_intersects` only ever writes
                        // into `star_filters` (see its own doc comment),
                        // and the identifier slot this branch is keyed on is
                        // never geometry-typed. Kept for the same "match
                        // stays exhaustive without asserting unreachability
                        // it cannot prove" reason as `NotBound`.
                        geo @ FilterCondition::Intersects { .. } => kept.push(geo),
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
            required_paths: Vec::new(),
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
        return Err(ScopeError::Unscoped {
            message: "Add a triple pattern like '?s rdf:type asset360:Signal' to scope the query."
                .to_owned(),
            rewrite: None,
        });
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
                //
                // The same lookup answers the second question the renderer
                // has to ask — whether the slot holds one identifier or a
                // collection of them — so it is resolved once here rather
                // than twice, in two places that could disagree.
                let referenced_slot = schema_view
                    .get_class_by_uri(&var_to_class[&builder.variable])
                    .ok()
                    .flatten()
                    .and_then(|cv| {
                        cv.slot(&linkml_schemaview::identifier::Identifier::Name(
                            slot_name.clone(),
                        ))
                    })
                    .filter(|slot| {
                        slot.determine_slot_inline_mode()
                            == linkml_schemaview::slotview::SlotInlineMode::Reference
                    });
                let Some(referenced_slot) = referenced_slot else {
                    record_loss(Inexact::TypedNestedStructure);
                    continue;
                };
                let right_multivalued = referenced_slot.determine_slot_container_mode()
                    != linkml_schemaview::slotview::SlotContainerMode::SingleValue;
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
                    right_path: Vec::new(),
                    right_multivalued,
                    join_type,
                });
            }
        }
    }

    // Paths into nested structures. Done after the stars exist, so a variable
    // that *is* a star is never mistaken for a step inside one -- and before
    // the filters, so a condition on a nested value has a path to be attached
    // to. Reading `?m :zoneName ?z . FILTER(?z = "Charleroi")` without the
    // paths is how that filter used to be dropped.
    //
    // Before the connectivity check too, because the walk is what finds the
    // edges that run *through* a structure.
    let PathWalk {
        bindings: path_bindings,
        traversed,
        constants: nested_constants,
        references,
    } = collect_path_bindings(&star_map, &var_to_class, schema_view);

    // Phase 2a: a reference reached through an inline structure is a join
    // edge too. `?s :hasCoveredSection ?cs . ?cs :belongsToTrack ?t . ?t a
    // :Track` holds ?t's identifier two slots down in ?s's record, and the
    // loop above only looked at the record's own columns -- so ?t had no edge,
    // was refused as *disconnected* inside an OPTIONAL, and was fetched whole
    // as an island outside one. It is connected, by exactly this edge; what is
    // different is only how the renderer has to state it, which the path on
    // the edge is for.
    //
    // Column references (`slot_path.len() == 1`) are the loop above's, and
    // are not raised twice.
    {
        let mut reached: Vec<(&String, &ReferenceReach)> = references
            .iter()
            .filter(|(var, reach)| var_to_class.contains_key(*var) && reach.slot_path.len() > 1)
            .collect();
        reached.sort_by(|a, b| a.0.cmp(b.0));
        for (var, reach) in reached {
            let (right_slot, right_path) = reach
                .slot_path
                .split_last()
                .map(|(last, rest)| (last.clone(), rest.to_vec()))
                .unwrap_or_default();
            let left_d = *star_depths.get(var).unwrap_or(&0);
            let right_d = *star_depths.get(&reach.star_var).unwrap_or(&0);
            let join_type = if reach.optional || left_d > 0 || right_d > 0 {
                JoinType::Left
            } else {
                JoinType::Inner
            };
            joins.push(JoinEdge {
                left: var.clone(),
                right: reach.star_var.clone(),
                right_slot,
                right_path,
                right_multivalued: reach.multivalued,
                join_type,
            });
        }
    }

    // Phase 2b: Reject disconnected OPTIONAL. Every star declared
    // inside an OPTIONAL block MUST share at least one join edge with
    // either a mandatory star or transitively with another star that
    // does. Compute reachability from mandatory stars and reject any
    // orphaned optional star.
    //
    // A sub-select's exported variable connects the star inside it to the
    // one outside spelled the same: `{ SELECT ?s (COUNT(?d) AS ?n) … }`
    // inside an `OPTIONAL` joins the outer `?s` on the export, and the
    // qualified inner star `?s__d1` is reachable through that link. Read off
    // the qualified query, so no second walk decides what a sub-select
    // exports.
    let exports = crate::sparql_domains::exports(query);
    {
        let edges: Vec<(&str, &str)> = joins
            .iter()
            .map(|j| (j.left.as_str(), j.right.as_str()))
            .chain(
                exports
                    .iter()
                    .map(|(inner, outer)| (inner.as_str(), outer.as_str())),
            )
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
                    "an OPTIONAL or EXISTS block introduces ?{} which shares no variable with the \
                     mandatory pattern; a disconnected block is not supported yet",
                    s.variable
                )));
            }
        }
    }

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

    // Star variable → its optional slots, for `!bound`'s liftability gate:
    // a required slot already carries the positive existence check, so
    // `!bound` on it is unsatisfiable and cannot become a pushed condition.
    // Built from `stars` (populated above, in Phase 1) rather than from
    // `star_filters` itself -- `extract_equality_from_expr` only ever sees
    // the latter, and it does not carry optionality.
    let optional_fields: HashMap<String, Vec<String>> = stars
        .iter()
        .map(|star| (star.variable.clone(), star.optional_fields.clone()))
        .collect();

    if let Some(cause) = collect_filter_conditions(
        pattern,
        0,
        &var_to_field,
        &mut star_filters,
        &optional_fields,
        &var_to_class,
    ) {
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
                            // Same reasoning: a substring match has no value
                            // list to hoist onto `identifier_values`, so it
                            // stays a filter against `asset360_uri`.
                            like @ FilterCondition::Like { .. } => {
                                star.filters.entry(slot.clone()).or_default().push(like);
                            }
                            // Same reasoning again: `!=` has no value list
                            // either, so it stays a filter and the renderer's
                            // null test runs against `asset360_uri`.
                            ne @ FilterCondition::Ne(_) => {
                                star.filters.entry(slot.clone()).or_default().push(ne);
                            }
                            // `!bound` cannot actually reach here either:
                            // its gate requires the slot to be in
                            // `optional_fields`, and the identifier slot is
                            // never optional. Handled the same way as `Ne`
                            // regardless, for the same "no value list"
                            // reasoning.
                            nb @ FilterCondition::NotBound => {
                                star.filters.entry(slot.clone()).or_default().push(nb);
                            }
                            // Unreachable in truth, and not just by the
                            // "no value list" reasoning the other arms give:
                            // this branch only ever sees a *single*-slot
                            // path (`<[String; 1]>::try_from(path)`, above),
                            // and the registry never claims one -- its tail
                            // is two segments (`sparql_columns.rs`). Handled
                            // the same way anyway, so the match stays
                            // exhaustive without asserting a stronger
                            // unreachability claim than the code proves.
                            geo @ FilterCondition::Intersects { .. } => {
                                star.filters.entry(slot.clone()).or_default().push(geo);
                            }
                        }
                    }
                } else {
                    star.filters.entry(slot).or_default().extend(conds);
                }
            }
        }
    }

    // Phase 3b: the presence of every mandatory nested read, restated on the
    // star's scan. `required_fields` covers the first hop; this covers the
    // rest, so a fetched row of a mandatory star is a row the query answers
    // through. That is the premise the fetch bound (Phase 7) rests on, and it
    // is either restated here or the bound is declined there — see
    // `RequiredPath`.
    //
    // A read inside an `OPTIONAL` requires nothing: a missing value leaves
    // the variable unbound and the row keeps its solution. A read on an
    // optional star is the join's business, and the bound never rests on
    // that star's rows.
    let mut nested_presence_restated = true;
    {
        // Sorted, so the order of `required_paths` (and so the rendered
        // predicate) does not depend on hash order.
        let mut mandatory_reads: Vec<&PathBinding> = path_bindings
            .values()
            .filter(|binding| !binding.optional)
            .collect();
        mandatory_reads
            .sort_by(|a, b| (&a.star_var, &a.slot_path).cmp(&(&b.star_var, &b.slot_path)));
        for binding in mandatory_reads {
            let Some(star) = stars
                .iter_mut()
                .find(|star| star.variable == binding.star_var)
            else {
                continue;
            };
            if star.is_optional {
                continue;
            }
            match nested_presence_of(schema_view, &star.class_uri, &binding.slot_path) {
                Some(required) => {
                    if !star.required_paths.contains(&required) {
                        star.required_paths.push(required);
                    }
                }
                // A read the scan cannot restate. The plan still describes
                // the query -- the engine re-applies it over what is fetched
                // -- but the fetched rows are no longer each worth a
                // solution, so no bound may rest on them.
                None => nested_presence_restated = false,
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
    // …but a *driving scan* can be bounded even when the rows are a
    // combination. See `bound_applies_to_the_driving_scan`.
    let driving_scan_carries_the_bound = bound_applies_to_the_driving_scan(&stars, &joins);

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
    // describes. Anything left is a subject nothing accounts for -- and that
    // is a refusal, not a loss to record: the engine sees exactly the records
    // the stars fetched, so the triples of a subject no star fetches match
    // *nothing*, and a plan marked inexact would still answer zero rows (or an
    // unbound column) with a 200. Doc 28h: refuse, and say what to write.
    {
        let mut unaccounted: Vec<&String> = unresolved_subjects
            .iter()
            .filter(|var| !traversed.contains(*var) && default_graph_subjects.contains(*var))
            .collect();
        unaccounted.sort();
        if let Some(var) = unaccounted.first() {
            if scoping != Scoping::Record {
                return Err(untyped_subject_refusal(
                    var,
                    &star_map,
                    &var_to_class,
                    &references,
                    &path_bindings,
                    schema_view,
                    schema_graph_iri,
                ));
            }
            // Recorded, not refused: the refined plan gets its one chance to
            // type the star, and `resolve` asks again with what it found.
            untyped.extend(unaccounted.iter().map(|var| (*var).clone()));
        }
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
                // continue past it carry the hop that introduced it. A typed
                // variable reached through a reference is a star of its own,
                // and Phase 2a raised the edge that represents the triple.
                path_bindings.contains_key(var)
                    || traversed.contains(var)
                    || (var_to_class.contains_key(var) && references.contains_key(var))
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
    //
    // Two shapes carry a bound, and they bound *the same thing*: the scan the
    // fetch drives from. For a single relation that scan is the whole row set,
    // which is why the bound has always read as a row cap. For the shape
    // `bound_applies_to_the_driving_scan` admits it is the mandatory star's
    // scan, and the consumer applies it there rather than to the join product
    // — `sparql/fetch.py`, `execute_plan`. `sql_limit_scope` says which, for
    // the consumer that has one place to put a bound: `scope_union` stacks its
    // arms and may only cap the stack with a `Rows` bound. One that applies a
    // join's bound to the product returns fewer solutions than the query asked
    // for, silently.
    //
    // Both shapes share one more premise: each fetched row of the bounded
    // scan yields at least one solution. The scan restates the class, the
    // top-level slots (`required_fields`) and every nested read it can
    // (`required_paths`); a nested read it cannot restate (Phase 3b) breaks
    // the premise, and a bound whose premise the scan cannot guarantee is
    // declined. The fetch is then unbounded, which is slow and right, rather
    // than bounded and short with nothing to say so (issue #455, pepibru
    // GitLab).
    let sql_limit_scope = if inexact.is_some() || !nested_presence_restated {
        None
    } else if single_relation && !has_optional {
        Some(LimitScope::Rows)
    } else if driving_scan_carries_the_bound {
        Some(LimitScope::DrivingScan)
    } else {
        None
    };
    let sql_limit = sql_limit_scope.and_then(|_| pushable_limit(pattern));
    let sql_limit_scope = sql_limit_scope.filter(|_| sql_limit.is_some());

    let mut unconsumed_indices: Vec<usize> = unconsumed.into_iter().collect();
    unconsumed_indices.sort_unstable();

    Ok(QueryPlan {
        root,
        unconsumed: unconsumed_indices,
        sql_limit,
        sql_limit_scope,
        // Not a union: `scope_union` is the only place this is ever set.
        sql_limit_if_unioned: None,
        path_bindings,
        inexact,
        untyped,
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
    hints: &HashMap<String, String>,
) -> Result<Option<(String, Option<String>)>, ScopeError> {
    // A class the refined plan derived for a star its own domain left
    // untyped -- read back at the pipeline's `resolve` step, never inferred
    // here a second way. Consulted before the type triple so the two are
    // checked to agree where both exist.
    if let Some(class_uri) = hints.get(&builder.variable) {
        if let Some(iri) = &builder.type_iri
            && iri != class_uri
        {
            return Err(ScopeError::UnsupportedConstruct(format!(
                "?{} is typed <{iri}> by its own domain and <{class_uri}> by the refined plan; \
                 the scoper and the plan must agree",
                builder.variable
            )));
        }
        return Ok(Some((
            class_uri.clone(),
            schema_view
                .get_class_by_uri(class_uri)
                .ok()
                .flatten()
                .and_then(|cv| cv.identifier_slot().map(|s| s.name.clone())),
        )));
    }
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

/// How many branches a `UNION` query may be scoped as before it is refused.
///
/// Branches multiply: `n` nested unions are `2^n` conjunctive queries, and
/// each one is scoped in full. Sixteen is four nested unions, which is more
/// than any configuration writes and small enough that the planning cost stays
/// invisible. A query past it is refused by name rather than planned slowly.
const MAX_UNION_BRANCHES: usize = 16;

/// The conjunctive queries a `UNION` query is the union of, or `None` when
/// there is no `UNION` in it.
///
/// **Why the star decomposition cannot just walk a `UNION` in place.** Stars
/// are keyed by subject variable, and a star holds one class. Two arms typing
/// the same variable differently -- `{ ?s a :Signal } UNION { ?s a :Track }`,
/// which is the shape the construct exists for -- would collapse into one
/// star: the plan would fetch Signals, the engine would find no Track to
/// answer the second arm with, and the query would come back short with no
/// error. `Inexact::RepeatedType` records that collapse, and recording it is
/// not enough, because an inexact plan still *fetches* what it says.
///
/// So the union is distributed out instead. Each branch is a conjunctive
/// query the existing decomposition already handles, and the fetch is the
/// union of the branches' fetches -- which covers every record any branch
/// could need, because every triple of the query appears in at least one
/// branch. Distribution is not a semantics-preserving rewrite of the *query*
/// (the engine still runs the original), and it does not have to be: it is
/// only ever asked which records to load.
///
/// The arms are distributed over every binary node, including the ones this
/// endpoint refuses further down (`MINUS`, `LATERAL`). Refusing them stays the
/// refusal's job; duplicating it here would be two places to keep in step.
fn union_branches(pattern: &GraphPattern) -> Result<Option<Vec<GraphPattern>>, ScopeError> {
    if !contains_union(pattern) {
        return Ok(None);
    }
    let branches = distribute_unions(pattern)?;
    Ok(Some(branches))
}

/// Whether a `UNION` appears anywhere in the pattern.
fn contains_union(pattern: &GraphPattern) -> bool {
    match pattern {
        GraphPattern::Union { .. } => true,
        GraphPattern::Join { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Minus { left, right }
        | GraphPattern::LeftJoin { left, right, .. } => {
            contains_union(left) || contains_union(right)
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
        | GraphPattern::Service { inner, .. } => contains_union(inner),
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => false,
    }
}

/// The pattern rewritten as a list of union-free patterns.
///
/// Exhaustive on `GraphPattern` with no `_` arm, for the reason
/// [`tag_triples_by_depth`] is: a spargebra release that adds a variant must
/// be a compile error here rather than a construct silently dropped from every
/// branch.
fn distribute_unions(pattern: &GraphPattern) -> Result<Vec<GraphPattern>, ScopeError> {
    fn cap(branches: Vec<GraphPattern>) -> Result<Vec<GraphPattern>, ScopeError> {
        if branches.len() > MAX_UNION_BRANCHES {
            return Err(ScopeError::UnsupportedConstruct(format!(
                "this query's UNIONs make {} branches to scope, past the {MAX_UNION_BRANCHES} \
                 this endpoint plans; ask fewer alternatives, or issue them as separate queries",
                branches.len()
            )));
        }
        Ok(branches)
    }

    /// Both sides' branches, paired -- the cartesian product a binary node
    /// over two unions is.
    fn pair(
        left: &GraphPattern,
        right: &GraphPattern,
        build: impl Fn(GraphPattern, GraphPattern) -> GraphPattern,
    ) -> Result<Vec<GraphPattern>, ScopeError> {
        let lefts = distribute_unions(left)?;
        let rights = distribute_unions(right)?;
        let mut out = Vec::with_capacity(lefts.len() * rights.len());
        for l in &lefts {
            for r in &rights {
                out.push(build(l.clone(), r.clone()));
            }
        }
        cap(out)
    }

    /// One unary node's branches: the inner pattern's, each rewrapped.
    fn wrap(
        inner: &GraphPattern,
        build: impl Fn(GraphPattern) -> GraphPattern,
    ) -> Result<Vec<GraphPattern>, ScopeError> {
        Ok(distribute_unions(inner)?.into_iter().map(build).collect())
    }

    match pattern {
        GraphPattern::Union { left, right } => {
            let mut out = distribute_unions(left)?;
            out.extend(distribute_unions(right)?);
            cap(out)
        }
        GraphPattern::Join { left, right } => pair(left, right, |left, right| GraphPattern::Join {
            left: Box::new(left),
            right: Box::new(right),
        }),
        GraphPattern::Lateral { left, right } => {
            pair(left, right, |left, right| GraphPattern::Lateral {
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        GraphPattern::Minus { left, right } => {
            pair(left, right, |left, right| GraphPattern::Minus {
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            let expression = expression.clone();
            pair(left, right, move |left, right| GraphPattern::LeftJoin {
                left: Box::new(left),
                right: Box::new(right),
                expression: expression.clone(),
            })
        }
        GraphPattern::Filter { expr, inner } => wrap(inner, |inner| GraphPattern::Filter {
            expr: expr.clone(),
            inner: Box::new(inner),
        }),
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => wrap(inner, |inner| GraphPattern::Extend {
            inner: Box::new(inner),
            variable: variable.clone(),
            expression: expression.clone(),
        }),
        GraphPattern::OrderBy { inner, expression } => wrap(inner, |inner| GraphPattern::OrderBy {
            inner: Box::new(inner),
            expression: expression.clone(),
        }),
        GraphPattern::Project { inner, variables } => wrap(inner, |inner| GraphPattern::Project {
            inner: Box::new(inner),
            variables: variables.clone(),
        }),
        GraphPattern::Distinct { inner } => wrap(inner, |inner| GraphPattern::Distinct {
            inner: Box::new(inner),
        }),
        GraphPattern::Reduced { inner } => wrap(inner, |inner| GraphPattern::Reduced {
            inner: Box::new(inner),
        }),
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => wrap(inner, |inner| GraphPattern::Slice {
            inner: Box::new(inner),
            start: *start,
            length: *length,
        }),
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => wrap(inner, |inner| GraphPattern::Group {
            inner: Box::new(inner),
            variables: variables.clone(),
            aggregates: aggregates.clone(),
        }),
        GraphPattern::Graph { inner, name } => wrap(inner, |inner| GraphPattern::Graph {
            inner: Box::new(inner),
            name: name.clone(),
        }),
        GraphPattern::Service {
            inner,
            name,
            silent,
        } => wrap(inner, |inner| GraphPattern::Service {
            inner: Box::new(inner),
            name: name.clone(),
            silent: *silent,
        }),
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => {
            Ok(vec![pattern.clone()])
        }
    }
}

/// The same query with its pattern replaced, for scoping one branch.
fn with_pattern(query: &Query, pattern: GraphPattern) -> Query {
    match query {
        Query::Select {
            dataset, base_iri, ..
        } => Query::Select {
            dataset: dataset.clone(),
            pattern,
            base_iri: base_iri.clone(),
        },
        Query::Construct {
            template,
            dataset,
            base_iri,
            ..
        } => Query::Construct {
            template: template.clone(),
            dataset: dataset.clone(),
            pattern,
            base_iri: base_iri.clone(),
        },
        Query::Describe {
            dataset, base_iri, ..
        } => Query::Describe {
            dataset: dataset.clone(),
            pattern,
            base_iri: base_iri.clone(),
        },
        Query::Ask {
            dataset, base_iri, ..
        } => Query::Ask {
            dataset: dataset.clone(),
            pattern,
            base_iri: base_iri.clone(),
        },
    }
}

/// Scope every branch of a `UNION` and merge the fetches.
///
/// The merged plan is one `Bgp` holding every branch's stars. Two stars from
/// different branches are never joined -- a join edge only ever connects stars
/// of the branch it came from -- so the consumer fetches each group
/// independently and the engine sees the union of the records, which is what
/// evaluating the original query over them needs.
///
/// **A branch that cannot be scoped fails the whole query.** Its records would
/// simply never be fetched and that arm of the union would answer empty, which
/// is the wrong answer rather than a slow one. The refusal the branch raised is
/// the one the caller sees, because it names the actual problem -- an unscoped
/// subject in the second arm reads as an unscoped subject.
///
/// **Nothing is claimed.** `unconsumed` lists every triple of the *whole*
/// query, so the fetch narrows but the ledger credits the engine with
/// enforcing everything. The branch triple lists are renumbered relative to
/// their own branch and there is no honest mapping back onto the query's
/// enumeration; claiming by index anyway would credit the statement with
/// enforcing a constraint that belongs to another triple. See
/// `sparql_plan::fetch_only`, which reads exactly this field.
fn scope_union(
    query: &Query,
    branches: &[GraphPattern],
    schema_view: &SchemaView,
    schema_graph_iri: Option<&str>,
    scoping: Scoping<'_>,
) -> Result<QueryPlan, ScopeError> {
    let pattern = query_pattern(query);
    let mut triples_with_depth: Vec<(&TriplePattern, usize)> = Vec::new();
    tag_triples_by_depth(pattern, 0, &mut triples_with_depth)?;

    let mut stars: Vec<Star> = Vec::new();
    let mut joins: Vec<JoinEdge> = Vec::new();
    let mut path_bindings: HashMap<String, PathBinding> = HashMap::new();
    // A key that ignores the variable name: two branches that scope to the
    // same fetch contribute one star, not two identical ones.
    //
    // **Only join-free stars are ever entered here**, and that restriction is
    // the whole reason "no join edge crosses a branch boundary" is true rather
    // than merely intended. A star that a branch joins is a star whose fetch
    // the join *narrows*; share it with another branch and that branch's fetch
    // is narrowed by a constraint its own arm never wrote. The shape that
    // showed it:
    //
    // ```sparql
    // { ?s a :TunnelComplex ; :hasName ?n }
    // UNION
    // { ?s a :CivilEngineeringAsset ; :belongsTo ?t . ?t a :TunnelComplex ; :hasName ?n }
    // ```
    //
    // Both arms' `TunnelComplex` stars have the same shape, so `?t` deduplicated
    // onto `?s` and the second arm's join edge was retargeted at the first arm's
    // star. The statement then fetched only those tunnel complexes some civil
    // engineering asset points at, and the first arm lost every other one —
    // short answer, balanced ledger, no error. Exactly the failure the
    // distribution exists to prevent, reintroduced by the optimisation.
    //
    // Keeping the key to join-free stars keeps the saving where it is safe (two
    // arms scanning the same class is one scan) and gives it up where it is not.
    let mut seen: HashMap<String, String> = HashMap::new();
    let mut taken: HashSet<String> = HashSet::new();

    // The bound each branch would have accepted *on its rows*. A branch that
    // declines one -- inexact, or with a modifier the limit cannot pass --
    // makes the whole union decline, because the statement's rows are every
    // branch's rows together. So does a branch whose bound is on its driving
    // scan rather than on its rows (`LimitScope::DrivingScan`): the union
    // applies one outer `LIMIT` to the stacked statement, and on a joined
    // arm that caps the join product, which is a relation the driving-scan
    // argument says nothing about. A cap nobody has argued for is the one
    // that answers short with no error. See `QueryPlan::sql_limit_if_unioned`.
    let mut branch_limits: Vec<Option<usize>> = Vec::new();
    let mut untyped: Vec<String> = Vec::new();

    for (index, branch) in branches.iter().enumerate() {
        let branch_query = with_pattern(query, branch.clone());
        // A branch is scoped as a query of its own, so a type written in
        // another arm does not reach it. The refusal says so, because the
        // rewrite it names -- `?m a <Municipality>` -- may be one the author
        // already wrote, in the other arm, and being told to add it again is
        // worse than not being told which arm is short.
        let plan = scope_qualified(&branch_query, schema_view, schema_graph_iri, scoping).map_err(
            |err| match err {
                ScopeError::Unscoped { message, rewrite } => ScopeError::Unscoped {
                    message: format!(
                        "in UNION branch {} of {}: {message} Each branch is scoped on its own, \
                         so a type written in another branch does not carry over; the triple \
                         has to be in every branch that uses the variable.",
                        index + 1,
                        branches.len()
                    ),
                    rewrite,
                },
                other => other,
            },
        )?;
        branch_limits.push(
            plan.sql_limit
                .filter(|_| plan.sql_limit_scope == Some(LimitScope::Rows)),
        );
        for var in &plan.untyped {
            if !untyped.contains(var) {
                untyped.push(var.clone());
            }
        }

        // The stars this branch joins. Sharing one of them across branches is
        // how a join edge would end up narrowing another branch's fetch — see
        // `seen` above.
        let joined: HashSet<String> = plan
            .root
            .all_joins()
            .iter()
            .flat_map(|join| [join.left.clone(), join.right.clone()])
            .collect();

        // Old name -> name in the merged plan, for this branch only.
        let mut renamed: HashMap<String, String> = HashMap::new();
        for star in plan.root.all_stars() {
            let shareable = !joined.contains(&star.variable);
            let shape = star_shape(star);
            if shareable && let Some(existing) = seen.get(&shape) {
                renamed.insert(star.variable.clone(), existing.clone());
                continue;
            }
            let mut name = star.variable.clone();
            let mut suffix = 1usize;
            while taken.contains(&name) {
                name = format!("{}__u{suffix}", star.variable);
                suffix += 1;
            }
            taken.insert(name.clone());
            if shareable {
                seen.insert(shape, name.clone());
            }
            renamed.insert(star.variable.clone(), name.clone());
            let mut star = star.clone();
            star.variable = name;
            stars.push(star);
        }

        for join in plan.root.all_joins() {
            let (Some(left), Some(right)) = (renamed.get(&join.left), renamed.get(&join.right))
            else {
                // Unreachable: every join edge names two stars of the same
                // plan, and every star of that plan was just renamed.
                continue;
            };
            let mut join = join.clone();
            join.left = left.clone();
            join.right = right.clone();
            joins.push(join);
        }

        // A path binding is a promise about one query variable, and two
        // branches may bind the same variable through different records. The
        // first branch to bind it wins and a disagreeing second one removes
        // it: a consumer reading the wrong record's path gets a wrong value,
        // where a missing binding only costs it the shortcut.
        for (variable, binding) in &plan.path_bindings {
            let Some(star_var) = renamed.get(&binding.star_var) else {
                continue;
            };
            let mut binding = binding.clone();
            binding.star_var = star_var.clone();
            match path_bindings.get(variable) {
                Some(existing) if *existing != binding => {
                    path_bindings.remove(variable);
                }
                Some(_) => {}
                None => {
                    path_bindings.insert(variable.clone(), binding);
                }
            }
        }
    }

    Ok(QueryPlan {
        root: PlanNode::Bgp { stars, joins },
        // Every triple, unclaimed. See this function's own doc comment.
        unconsumed: (0..triples_with_depth.len()).collect(),
        // A `LIMIT` bounds the query's answers, and a branch's fetch is not
        // its answers: ten rows of one arm are not the ten the query asked
        // for. Never pushed.
        sql_limit: None,
        sql_limit_scope: None,
        // The same bound, sound only if the arms end up in one statement --
        // which the lowering decides, not this. Every branch has to have
        // accepted it; `max` rather than `min` because the bound covers the
        // window and a larger one covers a smaller one, and in practice every
        // branch reports the same `LIMIT` because they share the query's
        // modifiers.
        sql_limit_if_unioned: branch_limits
            .iter()
            .copied()
            .try_fold(0usize, |widest, limit| limit.map(|limit| widest.max(limit)))
            .filter(|_| !branch_limits.is_empty()),
        path_bindings,
        // The cause that matters most, and it holds of the whole plan rather
        // than of one dropped triple: a branch's own cause, if it had one, is
        // a fact about a pattern this plan no longer has a node for.
        inexact: Some(Inexact::UnionBranch),
        untyped,
    })
}

/// A star's fetch, as a string, so two branches that ask for the same records
/// contribute one star. Name-free on purpose: the variable is what differs
/// between an identical star in two branches.
fn star_shape(star: &Star) -> String {
    let mut star = star.clone();
    star.variable = String::new();
    format!("{star:?}")
}

/// The pattern of a query, whatever form it takes.
fn query_pattern(query: &Query) -> &GraphPattern {
    match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => pattern,
    }
}

/// Recursively walk the SPARQL algebra tree and collect every BGP
/// triple pattern, tagged with the OPTIONAL nesting depth at which it
/// occurs. `depth == 0` means the triple is in the mandatory part of
/// the query; `depth > 0` means it is inside one or more nested
/// `OPTIONAL { ... }` blocks.
///
/// Along the way, unsupported constructs (`UNION`, `MINUS`, property
/// paths, `LATERAL`) are rejected with [`ScopeError::UnsupportedConstruct`].
///
/// The match on [`GraphPattern`] below is exhaustive on purpose — no `_`
/// arm — so a spargebra release that adds a variant is a compile error here
/// rather than one more construct that is silently accepted and dropped.
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
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            // Left side stays at the current depth — it's the mandatory
            // pattern from the point of view of this OPTIONAL.
            tag_triples_by_depth(left, depth, out)?;
            // Right side is one level deeper — it's inside the OPTIONAL.
            tag_triples_by_depth(right, depth + 1, out)?;
            // The condition spargebra lifted out of `OPTIONAL { ... FILTER }`,
            // which may hold an `EXISTS` like any other filter. Read last, so
            // the order is the one the obligation enumeration and the plan
            // builder both walk: left, right, then the condition.
            match expression {
                Some(expression) => read_exists_patterns(expression, depth, out),
                None => Ok(()),
            }
        }
        // An expression is part of the query, and an `EXISTS` inside one reads
        // records like any triple pattern does. The three arms below are the
        // positions where those records are read into the fetch; the two after
        // them are the positions where they are refused. Skipping the
        // expression — which is what every one of these arms used to do — left
        // `FILTER NOT EXISTS { ?seg a :TunnelSegment }` fetching no segment at
        // all, so the filter held for every row and the endpoint answered a
        // different question without saying so.
        GraphPattern::Filter { expr, inner } => {
            tag_triples_by_depth(inner, depth, out)?;
            read_exists_patterns(expr, depth, out)
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            tag_triples_by_depth(inner, depth, out)?;
            read_exists_patterns(expression, depth, out)
        }
        // `ORDER BY EXISTS { ... }` and `SUM(?x + EXISTS { ... })` are read by
        // nothing here: the ordering and the aggregate are computed over rows
        // the fetch produced, and there is no node that would claim the
        // pattern's triples. Refused rather than fetched-and-hoped, which is
        // the shape of the bug this walk is being fixed for.
        GraphPattern::OrderBy { inner, expression } => {
            for term in expression {
                let (OrderExpression::Asc(expr) | OrderExpression::Desc(expr)) = term;
                refuse_exists_patterns(expr, "an ORDER BY expression")?;
            }
            tag_triples_by_depth(inner, depth, out)
        }
        GraphPattern::Group {
            inner, aggregates, ..
        } => {
            for (_, aggregate) in aggregates {
                if let AggregateExpression::FunctionCall { expr, .. } = aggregate {
                    refuse_exists_patterns(expr, "an aggregate's argument")?;
                }
            }
            tag_triples_by_depth(inner, depth, out)
        }
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. } => tag_triples_by_depth(inner, depth, out),
        GraphPattern::Values { .. } => Ok(()),
        // Both arms, left then right — the order
        // `sparql_refine::Builder::pattern` walks them in, because the
        // obligation list this produces is consumed positionally by the plan
        // builder and the two must agree about which triple is which.
        //
        // `depth + 1` on both sides, for the same reason the right side of a
        // `LeftJoin` gets it: a triple in one arm does not hold of an answer
        // that came through the other, so it is not an existence check the
        // fetch may apply. Depth is what the star decomposition reads that
        // from, and the answer here is "not mandatory".
        //
        // This walk is the query's *whole* triple list, which is what the
        // obligation ledger needs. The star decomposition does not use it for
        // a UNION — see `union_branches`, which scopes each branch separately
        // so two arms typing the same variable differently cannot collapse
        // into one class.
        GraphPattern::Union { left, right } => {
            tag_triples_by_depth(left, depth + 1, out)?;
            tag_triples_by_depth(right, depth + 1, out)
        }
        GraphPattern::Lateral { .. } => Err(ScopeError::UnsupportedConstruct(
            "LATERAL is not supported; it is a SPARQL extension this endpoint does not serve"
                .into(),
        )),
        GraphPattern::Minus { .. } => Err(ScopeError::UnsupportedConstruct(
            "MINUS is not supported yet".into(),
        )),
        GraphPattern::Path { .. } => Err(ScopeError::UnsupportedConstruct(
            "SPARQL property paths are not supported; use explicit triple patterns".into(),
        )),
    }
}

/// Read every `EXISTS` block this expression holds into the fetch.
///
/// One level deeper than the expression's own pattern, and that is the whole
/// point: the block is not a constraint on the rows the query selects. A
/// complex with no segment is exactly what `FILTER NOT EXISTS { ?seg
/// :belongsTo ?complex }` keeps, so its star is optional — the fetch left-joins
/// it and pushes nothing from inside it as a condition — and the engine, which
/// re-runs the whole query over the fetched instances, has the segments it
/// needs to answer.
fn read_exists_patterns<'a>(
    expr: &'a Expression,
    depth: usize,
    out: &mut Vec<(&'a TriplePattern, usize)>,
) -> Result<(), ScopeError> {
    let mut patterns = Vec::new();
    exists_patterns_of(expr, &mut patterns);
    for pattern in patterns {
        tag_triples_by_depth(pattern, depth + 1, out)?;
    }
    Ok(())
}

/// Refuse an `EXISTS` in an expression position nothing reads it from.
///
/// The alternative is what this module did before: walk past it, fetch none of
/// the records it names, and let the engine evaluate it against an empty class
/// — a wrong answer with nothing in the plan to say so.
fn refuse_exists_patterns(expr: &Expression, position: &'static str) -> Result<(), ScopeError> {
    let mut patterns = Vec::new();
    exists_patterns_of(expr, &mut patterns);
    if patterns.is_empty() {
        return Ok(());
    }
    Err(ScopeError::UnsupportedConstruct(format!(
        "EXISTS in {position} is not supported; move it into a FILTER, or bind \
         it with BIND(EXISTS {{ ... }} AS ?flag) first"
    )))
}

/// Every `EXISTS` block an expression holds, in the order it writes them.
///
/// Exhaustive over [`Expression`] on purpose — no `_` arm — so a spargebra
/// release that adds a variant is a compile error here rather than one more
/// position where a pattern is silently not fetched.
pub(crate) fn exists_patterns_of<'a>(expr: &'a Expression, out: &mut Vec<&'a GraphPattern>) {
    match expr {
        Expression::Exists(pattern) => out.push(pattern),
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
            exists_patterns_of(left, out);
            exists_patterns_of(right, out);
        }
        Expression::In(value, candidates) => {
            exists_patterns_of(value, out);
            for candidate in candidates {
                exists_patterns_of(candidate, out);
            }
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            exists_patterns_of(inner, out)
        }
        Expression::If(condition, then, otherwise) => {
            exists_patterns_of(condition, out);
            exists_patterns_of(then, out);
            exists_patterns_of(otherwise, out);
        }
        Expression::Coalesce(parts) | Expression::FunctionCall(_, parts) => {
            for part in parts {
                exists_patterns_of(part, out);
            }
        }
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => {}
    }
}

/// The query's `FROM` / `FROM NAMED` clause, if it wrote one.
///
/// Exhaustive over [`Query`]: every form can carry a dataset clause, and a
/// form that carried one unnoticed is the defect this exists to refuse.
fn dataset_of(query: &Query) -> Option<&spargebra::algebra::QueryDataset> {
    match query {
        Query::Select { dataset, .. }
        | Query::Construct { dataset, .. }
        | Query::Describe { dataset, .. }
        | Query::Ask { dataset, .. } => dataset.as_ref(),
    }
}

/// Every graph pattern hiding in *this node's* expressions, and none deeper.
///
/// The single answer to "where can a pattern be that is not a child pattern".
/// A walker over [`GraphPattern`] recurses into `inner`, `left` and `right`
/// and, doing only that, walks straight past `FILTER EXISTS { … }`: the block
/// is a pattern held by an [`Expression`], not a child of the node. Three
/// separate walkers made exactly that mistake and produced three wrong
/// answers, so the knowledge lives here once and each walker asks for it in
/// one line rather than growing its own half of it.
///
/// Exhaustive over [`GraphPattern`] with no `_` arm, for the same reason
/// [`exists_patterns_of`] is exhaustive over [`Expression`]: a spargebra
/// release that adds a node carrying an expression must be a compile error
/// here, not a fourth silent omission.
///
/// `ORDER BY` and aggregate positions are included even though
/// [`tag_triples_by_depth`] refuses an `EXISTS` there. A caller is not obliged
/// to have scoped first — `observable_slots` and
/// [`crate::sparql_graph_clauses`] both run before or without the scoper — and
/// an analysis whose correctness depends on another analysis having refused
/// first is the arrangement this function exists to end.
pub(crate) fn exists_patterns_in_expressions_of(pattern: &GraphPattern) -> Vec<&GraphPattern> {
    let mut out: Vec<&GraphPattern> = Vec::new();
    match pattern {
        GraphPattern::Filter { expr, .. } => exists_patterns_of(expr, &mut out),
        GraphPattern::Extend { expression, .. } => exists_patterns_of(expression, &mut out),
        GraphPattern::OrderBy { expression, .. } => {
            for term in expression {
                let (OrderExpression::Asc(expr) | OrderExpression::Desc(expr)) = term;
                exists_patterns_of(expr, &mut out);
            }
        }
        GraphPattern::Group { aggregates, .. } => {
            for (_, aggregate) in aggregates {
                if let AggregateExpression::FunctionCall { expr, .. } = aggregate {
                    exists_patterns_of(expr, &mut out);
                }
            }
        }
        GraphPattern::LeftJoin { expression, .. } => {
            if let Some(expr) = expression {
                exists_patterns_of(expr, &mut out);
            }
        }
        GraphPattern::Bgp { .. }
        | GraphPattern::Path { .. }
        | GraphPattern::Join { .. }
        | GraphPattern::Union { .. }
        | GraphPattern::Lateral { .. }
        | GraphPattern::Minus { .. }
        | GraphPattern::Graph { .. }
        | GraphPattern::Project { .. }
        | GraphPattern::Distinct { .. }
        | GraphPattern::Reduced { .. }
        | GraphPattern::Slice { .. }
        | GraphPattern::Service { .. }
        | GraphPattern::Values { .. } => {}
    }
    out
}

/// Every triple pattern that reads the schema graph, by identity.
///
/// Identity and not value: two textually identical patterns, one inside the
/// `GRAPH` and one outside, are different obligations and only the first is a
/// schema pattern.
///
/// A `GRAPH` naming a *variable* counts too. It may bind to the schema graph,
/// and a pattern that might be about the datamodel cannot be scoped as if it
/// were certainly about golden records. A `GRAPH` naming some other constant
/// IRI does not: the endpoint has no such graph, and its long-standing
/// behaviour — walk the triples into the fetch, mark the plan inexact, let the
/// engine answer from an empty graph — is left exactly as it was.
fn triples_in_the_schema_graph(
    pattern: &GraphPattern,
    schema_graph_iri: Option<&str>,
) -> HashSet<*const TriplePattern> {
    // No schema graph configured for the active datamodel: there is no such
    // graph, so no pattern is in it and every triple is scoped as before.
    let Some(schema_graph_iri) = schema_graph_iri else {
        return HashSet::new();
    };
    let reads_the_schema_graph = |name: &spargebra::term::NamedNodePattern| match name {
        spargebra::term::NamedNodePattern::NamedNode(node) => node.as_str() == schema_graph_iri,
        spargebra::term::NamedNodePattern::Variable(_) => true,
    };
    triples_in_blocks(pattern, &reads_the_schema_graph, false)
}

/// The triples the endpoint's own store does not hold: inside any `GRAPH`
/// block, or inside a `SERVICE` block.
///
/// A subject that appears only there is not a golden record, so "give it an
/// rdf:type" is the wrong thing to tell its author; the plan's inexactness for
/// the block (`Inexact::NamedGraph`, `Inexact::RemoteService`) is what
/// accounts for it.
fn triples_outside_the_default_graph(pattern: &GraphPattern) -> HashSet<*const TriplePattern> {
    triples_in_blocks(pattern, &|_| true, true)
}

/// The triples inside the `GRAPH` blocks `is_the_graph` accepts -- and inside
/// `SERVICE` blocks too when `and_services` is set.
fn triples_in_blocks(
    pattern: &GraphPattern,
    is_the_graph: &dyn Fn(&spargebra::term::NamedNodePattern) -> bool,
    and_services: bool,
) -> HashSet<*const TriplePattern> {
    fn walk(
        pattern: &GraphPattern,
        inside: bool,
        out: &mut HashSet<*const TriplePattern>,
        is_the_graph: &dyn Fn(&spargebra::term::NamedNodePattern) -> bool,
        and_services: bool,
    ) {
        match pattern {
            GraphPattern::Bgp { patterns } => {
                if inside {
                    for triple in patterns {
                        out.insert(std::ptr::from_ref(triple));
                    }
                }
            }
            GraphPattern::Graph { name, inner } => walk(
                inner,
                inside || is_the_graph(name),
                out,
                is_the_graph,
                and_services,
            ),
            GraphPattern::Service { inner, .. } => walk(
                inner,
                inside || and_services,
                out,
                is_the_graph,
                and_services,
            ),
            GraphPattern::Join { left, right }
            | GraphPattern::Union { left, right }
            | GraphPattern::Lateral { left, right }
            | GraphPattern::Minus { left, right } => {
                walk(left, inside, out, is_the_graph, and_services);
                walk(right, inside, out, is_the_graph, and_services);
            }
            GraphPattern::LeftJoin { left, right, .. } => {
                walk(left, inside, out, is_the_graph, and_services);
                walk(right, inside, out, is_the_graph, and_services);
            }
            GraphPattern::Filter { inner, .. }
            | GraphPattern::Extend { inner, .. }
            | GraphPattern::OrderBy { inner, .. }
            | GraphPattern::Project { inner, .. }
            | GraphPattern::Distinct { inner }
            | GraphPattern::Reduced { inner }
            | GraphPattern::Slice { inner, .. }
            | GraphPattern::Group { inner, .. } => {
                walk(inner, inside, out, is_the_graph, and_services)
            }
            GraphPattern::Path { .. } | GraphPattern::Values { .. } => {}
        }
    }

    let mut out = HashSet::new();
    walk(pattern, false, &mut out, is_the_graph, and_services);
    out
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
    optional_fields: &HashMap<String, Vec<String>>,
    var_to_class: &HashMap<String, String>,
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
                extract_equality_from_expr(
                    expr,
                    var_to_field,
                    star_filters,
                    optional_fields,
                    var_to_class,
                );
                None
            } else if depth == 0 {
                if extract_equality_from_expr(
                    expr,
                    var_to_field,
                    star_filters,
                    optional_fields,
                    var_to_class,
                ) {
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
                optional_fields,
                var_to_class,
            ))
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            let l = collect_filter_conditions(
                left,
                depth,
                var_to_field,
                star_filters,
                optional_fields,
                var_to_class,
            );
            let r = collect_filter_conditions(
                right,
                depth + 1,
                var_to_field,
                star_filters,
                optional_fields,
                var_to_class,
            );
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
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Minus { left, right } => collect_filter_conditions(
            left,
            depth,
            var_to_field,
            star_filters,
            optional_fields,
            var_to_class,
        )
        .or(collect_filter_conditions(
            right,
            depth,
            var_to_field,
            star_filters,
            optional_fields,
            var_to_class,
        )),
        GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. } => collect_filter_conditions(
            inner,
            depth,
            var_to_field,
            star_filters,
            optional_fields,
            var_to_class,
        ),
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
        | GraphPattern::Lateral { left, right }
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
    optional_fields: &HashMap<String, Vec<String>>,
    var_to_class: &HashMap<String, String>,
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
        Expression::Not(inner) => match inner.as_ref() {
            // `FILTER(?v != "x")` — spargebra 0.4 spells it `Not(Equal(..))`,
            // not a dedicated `NotEqual` variant (confirmed by reading
            // `spargebra::algebra::Expression`, which has no such variant).
            Expression::Equal(left, right) => {
                let Some((star_var, field, texts)) = match_var_constant(left, right, var_to_field)
                    .or_else(|| match_var_constant(right, left, var_to_field))
                else {
                    return false;
                };
                // An enum constant can select several codes, and "not any of
                // these" is not one condition — it would need `NOT IN`, which
                // no `FilterCondition` arm renders. Left to the engine rather
                // than approximated as a single `<>`.
                let Ok([only]) = <[String; 1]>::try_from(texts) else {
                    return false;
                };
                star_filters
                    .entry(star_var)
                    .or_default()
                    .entry(field)
                    .or_default()
                    .push(FilterCondition::Ne(only));
                true
            }
            // `FILTER(!bound(?x))` — the slot ?x resolves to was never read.
            // Rendered as the negation of the presence check the builder
            // already skips for an optional field, so the gate below is not
            // about whether the check is *expressible* (it always is) but
            // whether pushing it is *sound*.
            Expression::Bound(var) => {
                let Some((star_var, path, _form)) = var_to_field.get(var.as_str()) else {
                    return false;
                };
                if path.len() != 1 {
                    // A nested path's absence is "the key at this step is
                    // missing", which is a different predicate from "the
                    // leaf value is absent" — no `FilterCondition` arm
                    // renders it, and the renderer only ever walks a
                    // multi-hop path with `->>`.
                    return false;
                }
                // A required slot already carries the positive existence
                // check (`object_data ? 'field'`), so on that slot `!bound`
                // is unsatisfiable and the query selects nothing — which the
                // plan has no way to state. Checked against `optional_fields`
                // rather than assumed: an unknown star must decline too, or
                // a star nobody can resolve would silently lift a presence
                // check for it.
                let is_optional = optional_fields
                    .get(star_var)
                    .is_some_and(|fields| fields.contains(&path[0]));
                if !is_optional {
                    return false;
                }
                star_filters
                    .entry(star_var.clone())
                    .or_default()
                    .entry(path.clone())
                    .or_default()
                    .push(FilterCondition::NotBound);
                true
            }
            // Every other negation — `!CONTAINS`, `!(A && B)` — is left to
            // oxigraph.
            _ => false,
        },
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
            let l = extract_equality_from_expr(
                left,
                var_to_field,
                star_filters,
                optional_fields,
                var_to_class,
            );
            let r = extract_equality_from_expr(
                right,
                var_to_field,
                star_filters,
                optional_fields,
                var_to_class,
            );
            l & r
        }
        // The substring functions. `STRSTARTS(?nm, "BX")` narrows the fetch
        // the way `LIKE 'BX%'` does, and until now fell to the arm below —
        // correct, because the engine re-applied it, and unusable on a large
        // class, because nothing narrowed the fetch and the triple limit
        // bounds it.
        Expression::FunctionCall(function, args) => {
            let anchor = match function {
                spargebra::algebra::Function::StrStarts => LikeAnchor::Prefix,
                spargebra::algebra::Function::StrEnds => LikeAnchor::Suffix,
                spargebra::algebra::Function::Contains => LikeAnchor::Anywhere,
                // Every other call — REGEX, arithmetic, a custom function —
                // is left to oxigraph, as before. `geof:sfIntersects` is
                // handled in `lift_intersects`, called below.
                _ => {
                    return lift_intersects(
                        function,
                        args,
                        var_to_field,
                        star_filters,
                        optional_fields,
                        var_to_class,
                    );
                }
            };
            let [haystack, needle] = args.as_slice() else {
                return false;
            };
            // `LCASE(?nm)` folds the column; a bare `?nm` does not. Anything
            // else in the haystack position is not a column.
            let (haystack, case_insensitive) = match haystack {
                Expression::FunctionCall(spargebra::algebra::Function::LCase, inner) => {
                    match inner.as_slice() {
                        [only] => (only, true),
                        _ => return false,
                    }
                }
                other => (other, false),
            };
            let Expression::Variable(var) = haystack else {
                return false;
            };
            let Some((star_var, path, form)) = var_to_field.get(var.as_str()) else {
                return false;
            };
            let Expression::Literal(literal) = needle else {
                return false;
            };
            // Only a plain literal, and only on a column whose stored term is
            // its own text. `literal_pushable` is the same gate `=` and `IN`
            // apply through `constant_texts` — it destructures
            // `PushForm::Literal` and returns `false` for anything else, so an
            // `Enum` column (which stores a code and translates backwards
            // through its meanings, where a substring of a *label* matches no
            // code), an `Iri` column and a `Tagged` one are all refused here
            // the same way. `constant_texts` itself cannot be reused: it
            // returns the *codes* an equal constant selects, and a substring
            // is not a term to translate — there is no `PushForm::Text`
            // variant to name instead, so the existing gate is reused
            // directly rather than duplicated.
            if !literal_pushable(literal, form) {
                return false;
            }
            star_filters
                .entry(star_var.clone())
                .or_default()
                .entry(path.clone())
                .or_default()
                .push(FilterCondition::Like {
                    value: literal.value().to_owned(),
                    anchor,
                    case_insensitive,
                });
            true
        }
        // Everything else — `||`, other negations, REGEX, BOUND, arithmetic —
        // is left to oxigraph, and the plan is no longer a complete
        // description. `!=` is handled above, in `Expression::Not`.
        _ => false,
    }
}

/// Lift `geof:sfIntersects` onto a slot with a broken-out geometry column.
///
/// Five gates, each declining rather than approximating: the function has to
/// be `geof:sfIntersects` itself (`geof:area`/`geof:distance` and the other
/// 41 GeoSPARQL functions are geodesic in `spargeo` and planar in PostGIS on
/// `geometry(4326)`, so only a topological predicate agrees between the two
/// routes); the first argument has to resolve, through `var_to_field`, to a
/// slot path; the star variable has to resolve, through `var_to_class`, to a
/// class (an address nobody can resolve is not a condition — the same rule
/// `constants_are_the_columns_terms` applies to its own `class_of_star`
/// lookup, one file over); that `(class_uri, slot_path)` pair has to be one
/// the [`crate::sparql_columns`] registry claims a column for; the second
/// argument has to be a literal [`intersects_wkt_from_literal`] accepts
/// (typed `wktLiteral` in CRS84 — see its doc comment for the two ways a
/// literal refuses).
///
/// `optional_fields` is unused: unlike `!bound`, a geometry predicate over an
/// absent optional slot is not a presence check with a different rendering —
/// it is an ordinary comparison that the slot's own presence in the row
/// (`?g asset360:asWKT ?w`, a required triple pattern of the FILTER's own
/// variable) already has to satisfy for `?w` to be bound at all, the same as
/// every other arm above this one in `extract_equality_from_expr` that does
/// not consult it.
fn lift_intersects(
    function: &spargebra::algebra::Function,
    args: &[Expression],
    var_to_field: &ValueColumns,
    star_filters: &mut StarFilters,
    _optional_fields: &HashMap<String, Vec<String>>,
    var_to_class: &HashMap<String, String>,
) -> bool {
    let spargebra::algebra::Function::Custom(node) = function else {
        return false;
    };
    if node.as_str() != SF_INTERSECTS_IRI {
        return false;
    }
    let [Expression::Variable(var), Expression::Literal(literal)] = args else {
        return false;
    };
    let Some((star_var, path, _form)) = var_to_field.get(var.as_str()) else {
        return false;
    };
    // The registry is keyed on `(class_uri, slot_path)`. A star the map does
    // not name is not a condition -- the same refusal `constants_are_the_columns_terms`
    // makes through `class_of_star.get(star_var).is_some_and(...)` in
    // `sparql_refine.rs`, and the same shape Task 3's optional-slot lookup
    // uses: no entry does not default to passing the gate.
    let Some(class_uri) = var_to_class.get(star_var.as_str()) else {
        return false;
    };
    let Some(crate::sparql_columns::BrokenOutColumn::Geometry) =
        crate::sparql_columns::broken_out_column(class_uri.as_str(), path)
    else {
        return false;
    };
    let Some(wkt) = intersects_wkt_from_literal(literal) else {
        return false;
    };
    star_filters
        .entry(star_var.clone())
        .or_default()
        .entry(path.clone())
        .or_default()
        .push(FilterCondition::Intersects { wkt });
    true
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

/// The presence check a scan can state for a nested read, or `None` when it
/// cannot.
///
/// The walk that produced the binding already resolved every step against
/// the schema, so a path that fails to resolve here is a defect in one of
/// the two walks rather than a query shape -- and the answer to a defect is
/// to decline the bound, not to guess. The one shape declined on purpose: a
/// leaf that is the **key slot of a mapping element**. A mapping is stored
/// keyed by that slot's value, and the loader injects the key into the
/// element when the payload leaves it out (`rust-linkml-core`,
/// `build_mapping_entry_for_slot`), so the stored JSON may not carry it and
/// no predicate over the payload can say whether the triple exists. Every
/// other shape is restated: single-valued, list and mapping hops, a
/// single- or multivalued leaf, and several leaves under one element.
fn nested_presence_of(
    schema_view: &SchemaView,
    class_uri: &str,
    slot_path: &[String],
) -> Option<RequiredPath> {
    use linkml_schemaview::slotview::SlotContainerMode;
    if slot_path.len() < 2 {
        return None;
    }
    let mut class = schema_view.get_class_by_uri(class_uri).ok().flatten()?;
    let mut containers = Vec::with_capacity(slot_path.len());
    for (index, name) in slot_path.iter().enumerate() {
        let slot = class.slot(&Identifier::Name(name.clone()))?;
        let mode = slot.determine_slot_container_mode();
        containers.push(mode);
        if index + 1 == slot_path.len() {
            break;
        }
        let range = slot.get_range_class()?;
        if mode == SlotContainerMode::Mapping
            && index + 2 == slot_path.len()
            && range
                .key_or_identifier_slot()
                .is_some_and(|key| key.name == slot_path[index + 1])
        {
            return None;
        }
        class = range;
    }
    Some(RequiredPath {
        slot_path: slot_path.to_vec(),
        containers: containers
            .iter()
            .map(crate::sparql_pushdown::Container::from_mode)
            .collect(),
    })
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
        | GraphPattern::Lateral { left, right }
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
            | GraphPattern::Lateral { left, right }
            | GraphPattern::Minus { left, right } => walk(left, inside) || walk(right, inside),
            GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => {
                false
            }
        }
    }
    walk(pattern, false)
}

/// Whether a fetch bound may be applied to the scan this shape drives from.
///
/// **The question a join makes hard.** A `LIMIT` counts *solutions*. For one
/// star a row is a solution, so a row cap is a solution cap and the bound is
/// obviously safe. Join two stars and a row is a combination: N rows of the
/// product are not N solutions, and capping the product cuts records the first
/// N solutions need — the engine then answers with one side of an edge
/// missing, which under `OPTIONAL` is a *wrong* binding rather than a missing
/// one. That is why every join has had no bound at all, and why the fetch read
/// the whole class for a `LIMIT 50` (issue #443, asset360 pepibru GitLab).
///
/// There is a shape where the bound is sound anyway, applied one level down —
/// to the **driving scan** rather than to the product:
///
/// * exactly one star is mandatory. It is the scan the fetch drives from, and
///   its rows already satisfy the mandatory pattern, so each one yields **at
///   least one** solution. N of them therefore cover N solutions.
/// * every join is a `LEFT` join, so no row of the driving scan can be
///   eliminated by a match that is not there. One inner join and a driving row
///   may yield no solution at all, which is the short answer this must not
///   produce.
/// * every star is reachable from that one through the edges. A record kept by
///   the bound brings **all** of its matches with it, so the engine sees each
///   fetched record's neighbourhood whole — which is what makes an `OPTIONAL`
///   bind where it should rather than come back unbound.
///
/// Reachability is walked undirected: an edge's `left` is the referenced star
/// and its `right` the one holding the identifier, and either may be the
/// optional side.
///
/// The caller still owns the other half of the question — nothing is pushed
/// through a `GROUP BY`, an `ORDER BY` or a `DISTINCT` ([`pushable_limit`]),
/// and nothing is pushed at all through a plan that dropped part of the query
/// (`inexact`), where the fetched rows are not the real row set.
fn bound_applies_to_the_driving_scan(stars: &[Star], joins: &[JoinEdge]) -> bool {
    let mut mandatory = stars.iter().filter(|star| !star.is_optional);
    let (Some(root), None) = (mandatory.next(), mandatory.next()) else {
        return false;
    };
    if !joins.iter().all(|join| join.join_type == JoinType::Left) {
        return false;
    }
    let mut reached: HashSet<&str> = HashSet::from([root.variable.as_str()]);
    loop {
        let mut newly: Vec<&str> = Vec::new();
        for join in joins {
            if reached.contains(join.left.as_str()) && !reached.contains(join.right.as_str()) {
                newly.push(join.right.as_str());
            }
            if reached.contains(join.right.as_str()) && !reached.contains(join.left.as_str()) {
                newly.push(join.left.as_str());
            }
        }
        if newly.is_empty() {
            break;
        }
        reached.extend(newly);
    }
    stars
        .iter()
        .all(|star| reached.contains(star.variable.as_str()))
}

/// How many rows the object fetch may be limited to, if it may be limited.
///
/// The query's `LIMIT`, and only without an `OFFSET`. It used to be
/// `OFFSET + LIMIT`, on the reasoning that the fetch has to cover the whole
/// window because the engine applies the offset to what comes back — which is
/// true and not enough: the engine applies it in *its* order, not the fetch's,
/// so the covered window was the wrong window (the `Slice` arm below).
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
            } else if *start > 0 {
                // An OFFSET is a position in a sequence, and the fetch and
                // the engine do not agree on the sequence. Bounding the
                // fetch to `OFFSET + LIMIT` rows covers the window only if
                // the engine skips `OFFSET` rows *in the fetch's order*; it
                // enumerates the fetched store in its own order instead, so
                // `LIMIT 2 OFFSET 4` fetched six lowest records, skipped
                // four from the engine's top, and answered the same two
                // records `OFFSET 0` did. Every page was page one, with no
                // error (consolidator-server !995 review round 2, and issue
                // #456 for the single-star shape; both pepibru GitLab).
                // Until order is a contract — an `ORDER BY` the driving
                // scan honours, pushed with the bound, and total over the
                // solutions, because the engine sorts unstably and a tie
                // is not a position either — a paged query is fetched whole
                // and paged by the engine, as it was before any bound
                // existed: slower, and right.
                None
            } else {
                *length
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
        | GraphPattern::Lateral { .. }
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

/// A variable the path walk reached through a *reference* slot: the record it
/// stands for is another class's, held by identifier, and never inside the
/// star's own JSON.
///
/// Recorded whether or not the variable is typed, because the two need it for
/// opposite reasons. A typed one is a star of its own, and this says which
/// slot -- at which depth -- holds its identifier, which is what a join edge
/// through an inline structure is made of. An untyped one names records the
/// fetch never reads, and this says which class the author has to name to
/// make it readable.
struct ReferenceReach {
    /// The star whose record holds the reference.
    star_var: String,
    /// Slots from that record's root to the reference slot. Length one is a
    /// column of the record; longer is a reference inside an inline structure.
    slot_path: Vec<String>,
    /// The class the reference points at.
    range_class_uri: String,
    /// Whether any hop, or the reference slot itself, holds a collection --
    /// in which case the record holds *several* identifiers at this path.
    multivalued: bool,
    /// Whether any hop was introduced inside an `OPTIONAL`.
    optional: bool,
}

/// Everything the path walk learned, named so a caller cannot mix up two
/// maps keyed on the same variables.
struct PathWalk {
    /// Scalar leaves, by the variable bound to them.
    bindings: HashMap<String, PathBinding>,
    /// Variables the walk passed *through*: the inline structures.
    traversed: HashSet<String>,
    /// Constants written on a nested step.
    constants: Vec<NestedConstant>,
    /// Variables reached through a reference slot, by variable.
    references: HashMap<String, ReferenceReach>,
}

fn collect_path_bindings(
    star_map: &HashMap<String, StarBuilder>,
    var_to_class: &HashMap<String, String>,
    schema_view: &SchemaView,
) -> PathWalk {
    let mut out: HashMap<String, PathBinding> = HashMap::new();
    // Variables the walk explained *as steps*: the intermediate nodes it passed
    // through. Scalar leaves are deliberately absent — a leaf holds a value, so
    // a leaf variable used as a subject is a subject nothing accounts for, and
    // clearing it would reinstate the hole this set exists to close.
    //
    // Used only to clear a recorded drop, never to re-derive the check.
    let mut traversed: HashSet<String> = HashSet::new();
    let mut constants: Vec<NestedConstant> = Vec::new();
    let mut references: HashMap<String, ReferenceReach> = HashMap::new();

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
            &mut references,
            false,
            false,
        );
    }

    PathWalk {
        bindings: out,
        traversed,
        constants,
        references,
    }
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
    references: &mut HashMap<String, ReferenceReach>,
    optional_so_far: bool,
    multivalued_so_far: bool,
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
        // The class's own O(1) name index rather than a scan. It hands back an
        // owned SlotView, which this did not need before — worth it for a wide
        // class (TunnelComplex has 95 slots), a wash for a narrow one.
        let Some(slot) = current_class.slot(&Identifier::Name(slot_name.clone())) else {
            continue;
        };

        let mut path = path_so_far.to_vec();
        path.push(slot_name.clone());
        let hop_optional = builder
            .slot_depth
            .get(slot_name)
            .is_some_and(|depth| *depth > 0);
        let hop_multivalued = slot.determine_slot_container_mode()
            != linkml_schemaview::slotview::SlotContainerMode::SingleValue;

        // A reference: the value is another record's identifier, and there is
        // nothing here to walk into. Recorded for whoever holds the other end
        // -- typed or not -- and, deliberately, only the first path to reach
        // a variable: the walk is in sorted order, so which one that is does
        // not depend on hash order.
        let is_reference = matches!(
            slot.determine_slot_inline_mode(),
            linkml_schemaview::slotview::SlotInlineMode::Reference
        );
        if let Some(range_class) = slot.get_range_class().filter(|_| is_reference) {
            references
                .entry(object_var.clone())
                .or_insert_with(|| ReferenceReach {
                    star_var: star_var.to_owned(),
                    slot_path: path.clone(),
                    range_class_uri: range_class.canonical_uri().to_string(),
                    multivalued: multivalued_so_far || hop_multivalued,
                    optional: optional_so_far || hop_optional,
                });
            continue;
        }

        // A typed object variable is its own star, reached by a join edge --
        // or, if the slot stores the structure itself, a typed nested
        // structure, which Phase 2 reports.
        if var_to_class.contains_key(object_var) {
            continue;
        }

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
                        constants,
                        references,
                        optional_so_far || hop_optional,
                        multivalued_so_far || hop_multivalued,
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
                    let optional = optional_so_far || hop_optional;
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
/// The refusal for a subject variable no star and no path accounts for.
///
/// One message per way of getting here, each naming the rewrite, because the
/// four look identical in the query and need different fixes. The most useful
/// is the reference: the schema knows which class the slot points at, so the
/// message can spell the triple to add.
fn untyped_subject_refusal(
    var: &str,
    star_map: &HashMap<String, StarBuilder>,
    var_to_class: &HashMap<String, String>,
    references: &HashMap<String, ReferenceReach>,
    path_bindings: &HashMap<String, PathBinding>,
    schema_view: &SchemaView,
    schema_graph_iri: Option<&str>,
) -> ScopeError {
    // The name as the query wrote it, with the sub-select it is private to:
    // `?s__d1` is the `?s` of sub-select 1, and the author wrote `?s`.
    let shown = display_name(var);
    // Typed with a class the schema does not know: not "add a type" but "fix
    // the one you wrote".
    if let Some(iri) = star_map.get(var).and_then(|b| b.type_iri.as_deref()) {
        return ScopeError::Unscoped {
            message: format!(
                "?{shown} has rdf:type <{iri}>, which is not a class in the schema, so no \
                 records can be read for it. Check the class IRI and its prefix."
            ),
            rewrite: None,
        };
    }
    // Reached through a reference: the class is known, so say it.
    if let Some(reach) = references.get(var) {
        let path = reach.slot_path.join(".");
        return ScopeError::Unscoped {
            message: format!(
                "?{shown} is the object of `{path}` on ?{holder}, a reference to <{class}>, but \
                 has no rdf:type; only typed subjects are read from the database, so a triple \
                 with ?{shown} as its subject can never match.",
                holder = display_name(&reach.star_var),
                class = reach.range_class_uri,
            ),
            rewrite: Some(format!(
                "?{shown} a <{class}>",
                class = reach.range_class_uri
            )),
        };
    }
    // Bound to a value: a literal has no triples, so the pattern is empty by
    // construction rather than for want of a fetch.
    //
    // A value that is a *concept* -- the object of an enum-ranged slot -- is
    // the one case where the author's question has an answer, in another
    // graph: an enum value is a concept IRI whose labels and code the schema
    // graph holds (`crate::sparql_schema_graph`), unreachable without a
    // `GRAPH` clause naming it. The rewrite names the clause with the real
    // IRI, because the IRI is not discoverable from the endpoint (#465,
    // pepibru GitLab). It names no language: which tag the labels carry is
    // the datamodel's, and this function refuses to guess the graph IRI for
    // the same reason.
    let concept_slot = star_map.values().find_map(|builder| {
        let class_uri = var_to_class.get(&builder.variable)?;
        let class = schema_view.get_class_by_uri(class_uri).ok().flatten()?;
        builder
            .object_variables
            .iter()
            .find(|(_, object_var)| object_var.as_str() == var)
            .and_then(|(slot_name, _)| {
                let slot = class.slot(&Identifier::Name(slot_name.clone()))?;
                let enum_view = slot.get_range_enum()?;
                Some((
                    builder.variable.clone(),
                    slot_name.clone(),
                    enum_view.canonical_uri().to_string(),
                ))
            })
    });
    if let Some((holder, slot_name, enum_iri)) = concept_slot {
        let graph = match schema_graph_iri {
            Some(iri) => format!("<{iri}>"),
            None => "<schema-graph>".to_owned(),
        };
        return ScopeError::Unscoped {
            message: format!(
                "?{shown} is bound to a concept of <{enum_iri}> (the value of `{slot_name}` on \
                 ?{holder}), not a record: its labels and code are in the schema graph, which \
                 a triple pattern reaches only inside a GRAPH clause -- written inside the \
                 OPTIONAL, if the pattern was optional. `skos:notation` there is the code, \
                 `rdfs:label` the name."
            ),
            rewrite: Some(format!(
                "GRAPH {graph} {{ ?{shown} <http://www.w3.org/2004/02/skos/core#prefLabel> ?label }}"
            )),
        };
    }
    let is_value = path_bindings.contains_key(var)
        || star_map.values().any(|builder| {
            let Some(class_uri) = var_to_class.get(&builder.variable) else {
                return false;
            };
            builder
                .object_variables
                .iter()
                .any(|(slot_name, object_var)| {
                    object_var == var
                        && schema_view
                            .get_class_by_uri(class_uri)
                            .ok()
                            .flatten()
                            .and_then(|cv| cv.slot(&Identifier::Name(slot_name.clone())))
                            .is_some_and(|slot| slot.get_range_class().is_none())
                })
        });
    if is_value {
        return ScopeError::Unscoped {
            message: format!(
                "?{shown} is bound to a value, not a record, so it cannot be the subject of a \
                 triple pattern. Use a variable that names a record."
            ),
            rewrite: None,
        };
    }
    // The holder of a reference to a typed object: the schema knows which
    // classes declare such a slot, so the message can list them rather than
    // leave `<Class>` for the author to look up — the mirror of the reference
    // arm above, where the slot's range names the object's class.
    let declared_on = holder_candidates(var, star_map, var_to_class, schema_view);
    if let [class] = declared_on.as_slice() {
        // One class declares the combination: that is the rewrite.
        return ScopeError::Unscoped {
            message: format!(
                "?{shown} has no rdf:type; only typed subjects are read from the database, so a \
                 triple with ?{shown} as its subject can never match. The slots it reads are \
                 declared on <{class}>."
            ),
            rewrite: Some(format!("?{shown} a <{class}>")),
        };
    }
    if !declared_on.is_empty() {
        let listed = declared_on
            .iter()
            .map(|class| format!("<{class}>"))
            .collect::<Vec<_>>()
            .join(", ");
        return ScopeError::Unscoped {
            message: format!(
                "?{shown} has no rdf:type; only typed subjects are read from the database, so a \
                 triple with ?{shown} as its subject can never match. Add `?{shown} a <Class>`; \
                 the slots it reads are declared on {listed}."
            ),
            rewrite: None,
        };
    }
    ScopeError::Unscoped {
        message: format!(
            "?{shown} has no rdf:type; only typed subjects are read from the database, so a \
             triple with ?{shown} as its subject can never match. Add `?{shown} a <Class>`, \
             naming the class whose records it stands for."
        ),
        rewrite: None,
    }
}

/// A qualified variable as the author wrote it, naming the sub-select it is
/// private to: `s__d1` reads `s (in sub-select 1)`.
fn display_name(var: &str) -> String {
    let (base, domain) = crate::sparql_domains::split(var);
    if domain == 0 {
        base
    } else {
        format!("{base} (in sub-select {domain})")
    }
}

/// The classes an untyped subject could be, read off the references it holds.
///
/// `?l :belongsToMunicipality ?m . ?m a :Municipality` reads one slot whose
/// object is typed; every class declaring `belongsToMunicipality` as a
/// reference to `Municipality` is a candidate, and a subject reading several
/// such slots must be a class declaring all of them. Sorted, so the message is
/// stable. Empty when no slot on the subject leads to a typed object, or the
/// schema declares the combination on no class.
fn holder_candidates(
    var: &str,
    star_map: &HashMap<String, StarBuilder>,
    var_to_class: &HashMap<String, String>,
    schema_view: &SchemaView,
) -> Vec<String> {
    let Some(builder) = star_map.get(var) else {
        return Vec::new();
    };
    let mut typed_slots: Vec<(&String, &String)> = builder
        .object_variables
        .iter()
        .filter_map(|(slot, object_var)| var_to_class.get(object_var).map(|class| (slot, class)))
        .collect();
    typed_slots.sort();
    if typed_slots.is_empty() {
        return Vec::new();
    }
    let Ok(classes) = schema_view.class_views() else {
        return Vec::new();
    };
    let mut candidates: Vec<String> = classes
        .iter()
        .filter(|cv| {
            typed_slots.iter().all(|(slot_name, class_uri)| {
                cv.slot(&Identifier::Name((*slot_name).clone()))
                    .filter(|slot| {
                        matches!(
                            slot.determine_slot_inline_mode(),
                            linkml_schemaview::slotview::SlotInlineMode::Reference
                        )
                    })
                    .and_then(|slot| slot.get_range_class())
                    .is_some_and(|range| range.canonical_uri().to_string() == **class_uri)
            })
        })
        .map(|cv| cv.canonical_uri().to_string())
        .collect();
    candidates.sort();
    candidates.dedup();
    candidates
}

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
/// Either one makes the plan inexact, so the engine re-applies the whole query:
/// the plan reads one relation — the local default graph — so a named-graph
/// pattern is answered from the wrong graph and a remote pattern from the wrong
/// endpoint.
///
/// A `SERVICE` block is still walked transparently by the triple enumeration,
/// on the old assumption that the triples inside say which classes to load. A
/// `GRAPH` block no longer is: the endpoint's only named graph holds the
/// datamodel, so its patterns name schema terms rather than golden records.
/// `triples_inside_named_graphs` drops them before star building, and this
/// inexactness is what still routes the query to the engine.
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
        | GraphPattern::Lateral { left, right }
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
        | GraphPattern::Lateral { left, right }
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

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

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
# renders as that IRI, while `KSS` has none and renders as the IRI minted for
# it, `<…/SignalKind#KSS>`. Both are concepts — where the mapped one's IRI
# comes from is the only difference — so one enum shows that the rule does not
# depend on it.
enums:
  SignalKind:
    permissible_values:
      GSA:
        meaning: eul:GSA
      KSS: {}
      REP_H_D: {}
  # A second, additive enum — not a change to `SignalKind` — purely so a
  # `!=` FILTER can be given a constant that selects more than one code.
  # `AMB1` and `AMB2` deliberately share a `meaning`: `<eul:Amb>` translates
  # backwards to both, which is exactly the case the `Ne` arm must decline
  # rather than approximate as a single `<>`.
  AmbiguousKind:
    permissible_values:
      AMB1:
        meaning: eul:Amb
      AMB2:
        meaning: eul:Amb

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
      # Additive-only slot, paired with `AmbiguousKind` above — exists only
      # so a test can put a shared-meaning constant on the right-hand side
      # of `!=` without touching `kind`/`SignalKind`, which ~30 other tests
      # in this module depend on.
      ambiguousKind:
        range: AmbiguousKind
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
      # Additive, and deliberately paired with `CivilEngineeringAsset`'s slot
      # of the same name and a different range: a union arm's condition
      # resolved against the other arm's class is then the wrong *column
      # type*, with no route change to show for it.
      spanCount:
        range: integer
  CivilEngineeringAsset:
    class_uri: asset360:CivilEngineeringAsset
    attributes:
      asset360_uri:
        identifier: true
      hasName:
        range: string
      spanCount:
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
      # Additive-only, and the one slot in this fixture that declares a
      # `slot_uri` -- so the data carries `<eul:EAID_NAME>` while the readable
      # spelling is `asset360:rsmName`. That is the asset360 datamodel's RSM
      # shape in miniature, and what `crate::sparql_alias` exists for.
      rsmName:
        range: string
        slot_uri: eul:EAID_NAME
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
  # Backs the `geof:sfIntersects` lift tests. The path tail
  # `[hasGeometry, asWKT]` is exactly what `sparql_columns::broken_out_column`
  # keys on, so `PostalCode` is the class the registry claims a column for;
  # `Geometry` is a separate inlined class rather than an attribute on
  # `PostalCode` directly, matching how the real `postalcode.yaml` spells it
  # (see `sparql_columns.rs`'s module doc).
  PostalCode:
    class_uri: asset360:PostalCode
    attributes:
      asset360_uri:
        identifier: true
      hasGeometry:
        range: Geometry
        inlined: true
  Geometry:
    class_uri: asset360:Geometry
    attributes:
      asWKT:
        range: string
  # A two-level array of inlined structures, for the occurrence identifier:
  # `parts[0].children[1]` and `parts[1].children[1]` are two elements.
  Assembly:
    class_uri: asset360:Assembly
    attributes:
      asset360_uri:
        identifier: true
      parts:
        range: Part
        multivalued: true
        inlined: true
  Part:
    class_uri: asset360:Part
    attributes:
      label:
        range: string
      children:
        range: Child
        multivalued: true
        inlined: true
  Child:
    class_uri: asset360:Child
    attributes:
      label:
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

    /// `locatedOnTrack` holds another object's URI, so its value is a join
    /// key. Reading through it in JSONB would look for data that is not
    /// there -- and a fetch that reads only `?s`'s class never holds the
    /// record `?t` names, so `?t asset360:hasName ?tn` matched nothing and
    /// the query answered zero rows with a 200. Refused now, naming the class
    /// the reference points at, which is the one thing the author has to add.
    #[test]
    fn test_reference_slots_are_not_walked_into() {
        let sv = test_schema_view();
        let err = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t asset360:hasName ?tn }",
            &sv,
        )
        .unwrap_err();

        let ScopeError::Unscoped { .. } = err else {
            panic!("expected an unscoped refusal, got {err:?}");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("?t is the object of `locatedOnTrack` on ?s")
                && msg.contains("Add `?t a <https://data.infrabel.be/asset360/Track>`"),
            "{msg}"
        );
    }

    /// Every way a subject can end up naming records no star fetches, and
    /// what each is told. One table for the same reason
    /// `dropping_part_of_the_query_is_recorded_at_the_drop_site` is one: a new
    /// way in belongs here as a row.
    ///
    /// These were `Inexact::UntypedSubject` -- recorded, and the engine left
    /// to finish over a store that does not hold the records. That answers
    /// zero rows for a mandatory pattern and an unbound column for an
    /// optional one, 200 either way. Doc 28h: refuse.
    #[test]
    fn a_subject_no_star_fetches_is_refused_with_the_rewrite() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        for (expected, query) in [
            // The holder of a reference, untyped: the referenced class is
            // fetched, the holder never is. The schema knows which classes
            // declare the slot, so the message lists them.
            (
                "declared on <https://data.infrabel.be/asset360/Signal>. \
                 Add `?sig a <https://data.infrabel.be/asset360/Signal>`.",
                "SELECT ?t WHERE { ?sig asset360:locatedOnTrack ?t . ?t a asset360:Track }",
            ),
            // The same subject in one arm of a UNION, typed in the other: the
            // rewrite the message names is one the author wrote already, so
            // the message says which arm is short and why the type does not
            // carry over.
            (
                "in UNION branch 2 of 2: ?t is the object of `locatedOnTrack` on ?s",
                "SELECT ?n WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
                 { ?t a asset360:Track ; asset360:hasName ?n } UNION { ?t asset360:hasName ?n } }",
            ),
            // The object of a reference, untyped: the schema knows the class.
            (
                "Add `?t a <https://data.infrabel.be/asset360/Track>`",
                "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
                 ?t asset360:hasName ?tn }",
            ),
            // The same, inside an OPTIONAL: the column would have come back
            // unbound on every row.
            (
                "Add `?t a <https://data.infrabel.be/asset360/Track>`",
                "SELECT ?tn WHERE { ?s a asset360:Signal . \
                 OPTIONAL { ?s asset360:locatedOnTrack ?t . ?t asset360:hasName ?tn } }",
            ),
            // A scalar leaf used as a subject: a literal cannot be a subject,
            // and the path walk must not clear it just because it is a leaf.
            (
                "?nm is bound to a value, not a record",
                "SELECT ?x WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 ?nm asset360:name ?x }",
            ),
            // Typed, with a class the schema does not know: the fix is the
            // IRI, not a missing type.
            (
                "?s has rdf:type <https://data.infrabel.be/asset360/Sginal>, which is not a class",
                "SELECT ?s WHERE { ?s a asset360:Sginal . ?o a asset360:Signal }",
            ),
            // Free-floating: nothing reaches it, nothing fetches it.
            (
                "?x has no rdf:type",
                "SELECT ?x WHERE { ?s a asset360:Signal . ?x asset360:name ?n }",
            ),
            // A concept as a subject (#465): the label read anyone writes
            // first. The answer is in the schema graph, and the message says
            // the GRAPH clause with the graph's real IRI.
            (
                "?k is bound to a concept of <https://data.infrabel.be/asset360/SignalKind> \
                 (the value of `kind` on ?s), not a record: its labels and code are in the \
                 schema graph, which a triple pattern reaches only inside a GRAPH clause -- \
                 written inside the OPTIONAL, if the pattern was optional. `skos:notation` \
                 there is the code, `rdfs:label` the name. Add `GRAPH <urn:schema> { ?k \
                 <http://www.w3.org/2004/02/skos/core#prefLabel> ?label }`.",
                "SELECT ?l WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
                 OPTIONAL { ?k <http://www.w3.org/2004/02/skos/core#prefLabel> ?l } }",
            ),
        ] {
            let err = sparql_scope_with_schema_graph(
                &format!("{prefix}{query}"),
                &sv,
                Some("urn:schema"),
            )
            .unwrap_err();
            let ScopeError::Unscoped { .. } = &err else {
                panic!("{query}: expected an unscoped refusal, got {err:?}");
            };
            let msg = err.to_string();
            assert!(msg.contains(expected), "{query}: {msg}");
        }
    }

    /// A subject that lives only inside a `GRAPH` or `SERVICE` block is not a
    /// golden record, so it is not refused for lacking a type: the block's own
    /// inexactness routes the query to the engine, as before.
    #[test]
    fn a_subject_seen_only_inside_a_foreign_block_is_not_refused() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";
        for query in [
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             GRAPH <urn:other> { ?k <urn:label> ?l } }",
            "SELECT ?s WHERE { ?s a asset360:Signal . SERVICE <urn:remote> { ?x <urn:p> ?o } }",
        ] {
            let plan = sparql_scope(&format!("{prefix}{query}"), &sv)
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            assert!(
                plan.inexact.is_some(),
                "{query}: the block must still route to the engine"
            );
        }
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

    /// The three substring functions lift, and the `LCASE` wrapper the UI's
    /// `IContains`/`IStartsWith` operators produce is recognised rather than
    /// dropped.
    ///
    /// Dropping it is the asymmetric failure the differential oracle exists for:
    /// the engine leg folds case and a `LIKE` does not, so the two routes answer
    /// different row sets with nothing to say so.
    #[test]
    fn substring_filters_lift_with_their_case_folding() {
        let lifted = |query: &str| -> Vec<FilterCondition> {
            let scope =
                sparql_scope(&format!("{PREFIX}{query}"), &test_schema_view()).expect("scopes");
            all_stars(&scope)
                .iter()
                .flat_map(|star| star.filters.values())
                .flatten()
                .cloned()
                .collect()
        };

        assert_eq!(
            lifted(
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(STRSTARTS(?nm, \"BX\")) }"
            ),
            vec![FilterCondition::Like {
                value: "BX".to_owned(),
                anchor: LikeAnchor::Prefix,
                case_insensitive: false,
            }],
        );
        assert_eq!(
            lifted(
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(STRENDS(?nm, \"17\")) }"
            ),
            vec![FilterCondition::Like {
                value: "17".to_owned(),
                anchor: LikeAnchor::Suffix,
                case_insensitive: false,
            }],
        );
        assert_eq!(
            lifted(
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(CONTAINS(?nm, \"X5\")) }"
            ),
            vec![FilterCondition::Like {
                value: "X5".to_owned(),
                anchor: LikeAnchor::Anywhere,
                case_insensitive: false,
            }],
        );
        // `CONTAINS(LCASE(?nm), "x5")` is the idiomatic case-insensitive
        // spelling, and the only one the UI produces.
        assert_eq!(
            lifted(
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(CONTAINS(LCASE(?nm), \"x5\")) }"
            ),
            vec![FilterCondition::Like {
                value: "x5".to_owned(),
                anchor: LikeAnchor::Anywhere,
                case_insensitive: true,
            }],
        );
    }

    /// `LCASE` on the *needle* is not the same question, and must not lift.
    ///
    /// `CONTAINS(?nm, LCASE("X5"))` folds the constant, not the column, so a
    /// case-sensitive comparison against a lowered constant is what it asks. An
    /// `ILIKE` there would match rows the engine excludes.
    #[test]
    fn lcase_on_the_constant_does_not_become_case_insensitive() {
        let scope = sparql_scope(
            &format!(
                "{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(CONTAINS(?nm, LCASE(\"X5\"))) }}"
            ),
            &test_schema_view(),
        )
        .expect("scopes");
        assert!(
            all_stars(&scope).iter().all(|star| star.filters.is_empty()),
            "a folded constant is not a folded column"
        );
    }

    /// A substring never lifts onto an enum column.
    ///
    /// An enum column stores a *code* and translates backwards through its
    /// meanings — `kind` stores `GSA` or `KSS`, never a label — so a substring
    /// of a label matches no code and `object_data->>'kind' LIKE '%GS%'` would
    /// select nothing. If this gate is ever dropped (in `literal_pushable` or
    /// in the `FunctionCall` arm), the statement route silently starts
    /// answering an empty (or wrong) result while the engine leg still
    /// answers real rows — the asymmetric, unreported disagreement between
    /// the two routes this whole feature exists to prevent.
    #[test]
    fn substring_does_not_lift_onto_an_enum_column() {
        let scope = sparql_scope(
            &format!(
                "{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:kind ?k . \
                 FILTER(CONTAINS(?k, \"GS\")) }}"
            ),
            &test_schema_view(),
        )
        .expect("scopes");
        assert!(
            all_stars(&scope).iter().all(|star| star.filters.is_empty()),
            "a substring on an enum column must not become a pushed LIKE"
        );
    }

    /// `!=` lifts as its own arm, not as a fifth ordering comparison.
    ///
    /// The two differ in exactly the rows an OPTIONAL keeps: SPARQL's inequality
    /// is false for an unbound variable, SQL's `<>` on NULL is unknown. `Ne` is
    /// separate so the renderer is *made* to state the null test rather than
    /// inheriting `Cmp`'s rendering, which would drop those rows silently.
    #[test]
    fn inequality_lifts_as_its_own_arm() {
        let scope = sparql_scope(
            &format!(
                "{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(?nm != \"BX517\") }}"
            ),
            &test_schema_view(),
        )
        .expect("scopes");
        let conditions: Vec<_> = all_stars(&scope)
            .iter()
            .flat_map(|star| star.filters.values())
            .flatten()
            .cloned()
            .collect();
        assert_eq!(conditions, vec![FilterCondition::Ne("BX517".to_owned())]);
    }

    /// `!=` against a constant that selects more than one code does not lift.
    ///
    /// `AmbiguousKind`'s `AMB1` and `AMB2` share one `meaning`, so
    /// `<eul:Amb>` translates backwards to both codes: "not any of these" is
    /// not one condition — it would need `NOT IN`, which no `FilterCondition`
    /// arm renders. The failure mode if this ever regresses is silent wrong
    /// narrowing: approximating it as a single `<>` against one of the two
    /// codes would exclude rows the query keeps.
    #[test]
    fn inequality_against_a_multi_code_enum_constant_does_not_lift() {
        let scope = sparql_scope(
            &format!(
                "{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:ambiguousKind ?k . \
                 FILTER(?k != <http://ontorail.org/src/Eulynx/Amb>) }}"
            ),
            &test_schema_view(),
        )
        .expect("scopes");
        assert!(
            all_stars(&scope).iter().all(|star| star.filters.is_empty()),
            "a constant selecting several codes must not become a single pushed Ne"
        );
    }

    /// Once `!=` lifts, a `FILTER(?nm != "x")` no longer forces the fetch to
    /// be inexact, so a `LIMIT` alongside it is safe to push too.
    ///
    /// This used to be one of `test_limit_not_pushed_past_holistic_modifiers`'s
    /// "dropped" cases (`sql_limit` had to be `None`): before this task,
    /// `!=` fell to the catch-all, the fetch returned an arbitrary row set,
    /// and pushing the `LIMIT` into it could silently answer fewer rows than
    /// the query asked for. Pinning the opposite here is the direct evidence
    /// that the arm actually lifts, not just that it produces a
    /// `FilterCondition::Ne` in isolation.
    #[test]
    fn inequality_filter_does_not_block_limit_pushdown() {
        let plan = sparql_scope(
            &format!(
                "{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(?nm != \"BX517\") }} LIMIT 10"
            ),
            &test_schema_view(),
        )
        .expect("scopes");
        assert_eq!(plan.sql_limit, Some(10));
    }

    /// `OPTIONAL { ... } FILTER(!bound(?x))` lifts to a presence check.
    ///
    /// The one thing the builder deliberately skips for an optional field —
    /// `object_data ? 'field'` — is precisely what this asks for, negated.
    #[test]
    fn unbound_on_an_optional_slot_lifts() {
        let plan = sparql_scope(
            &format!(
                "{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal . \
                 OPTIONAL {{ ?s asset360:name ?nm }} FILTER(!bound(?nm)) }}"
            ),
            &test_schema_view(),
        )
        .expect("scopes");
        let conditions: Vec<_> = all_stars(&plan)
            .into_iter()
            .flat_map(|star| star.filters.values())
            .flatten()
            .cloned()
            .collect();
        assert_eq!(conditions, vec![FilterCondition::NotBound]);
    }

    /// `!bound` on a *required* slot does not lift.
    ///
    /// The star already carries an existence check for it, so the query
    /// selects nothing — and "nothing" is not a condition the plan can
    /// state. Lifting a presence check here would render a contradiction and
    /// answer zero rows for the right reason by accident; leaving it to the
    /// engine answers zero rows for the reason the query gives.
    #[test]
    fn unbound_on_a_required_slot_does_not_lift() {
        let plan = sparql_scope(
            &format!(
                "{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:name ?nm . \
                 FILTER(!bound(?nm)) }}"
            ),
            &test_schema_view(),
        )
        .expect("scopes");
        assert!(all_stars(&plan).iter().all(|star| star.filters.is_empty()));
    }

    /// `!bound` on a two-hop path does not lift, even though the path is
    /// fully mandatory (so the *other* gate, "is this slot optional",
    /// would also decline it here).
    ///
    /// `location` -> `longitude` is the same two-hop path
    /// `test_nested_structure_yields_a_path_binding` pins as a plain
    /// equality target, reused here under `!bound`. A nested path's
    /// absence is "the key at this step is missing", a different
    /// predicate from "the leaf value is absent" -- the renderer only
    /// ever walks a multi-hop path with `->>`, which has no way to state
    /// key-presence -- so no `FilterCondition` arm renders it and the gate
    /// declines regardless of optionality.
    #[test]
    fn unbound_on_a_two_hop_path_does_not_lift() {
        let plan = sparql_scope(
            &format!(
                "{PREFIX}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:location ?loc . \
                 ?loc asset360:longitude ?lon . FILTER(!bound(?lon)) }}"
            ),
            &test_schema_view(),
        )
        .expect("scopes");
        let conditions: Vec<_> = all_stars(&plan)
            .into_iter()
            .flat_map(|star| {
                star.filters
                    .values()
                    .flatten()
                    .chain(star.path_filters.iter().flat_map(|pf| pf.conditions.iter()))
            })
            .collect();
        assert!(conditions.is_empty());
    }

    /// `geof:sfIntersects` over a slot with a broken-out geometry column
    /// lifts.
    #[test]
    fn sf_intersects_lifts_on_a_geometry_slot() {
        const GEOF: &str = "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> \
                            PREFIX geo: <http://www.opengis.net/ont/geosparql#> ";
        let path_conditions = |query: &str| -> Vec<FilterCondition> {
            let scope = sparql_scope(&format!("{PREFIX}{GEOF}{query}"), &test_schema_view())
                .expect("scopes");
            scope
                .root
                .all_stars()
                .iter()
                .flat_map(|star| star.path_filters.iter())
                .flat_map(|filter| filter.conditions.iter())
                .cloned()
                .collect()
        };

        let box_wkt = "POLYGON((4 50, 5 50, 5 51, 4 51, 4 50))";
        assert_eq!(
            path_conditions(&format!(
                "SELECT ?s WHERE {{ ?s a asset360:PostalCode ; asset360:hasGeometry ?g . \
                 ?g asset360:asWKT ?w . \
                 FILTER(geof:sfIntersects(?w, \"{box_wkt}\"^^geo:wktLiteral)) }}"
            )),
            vec![FilterCondition::Intersects {
                wkt: box_wkt.to_owned()
            }],
        );
        // A CRS84 prefix is accepted and stripped; the SQL side takes a bare
        // WKT.
        assert_eq!(
            path_conditions(&format!(
                "SELECT ?s WHERE {{ ?s a asset360:PostalCode ; asset360:hasGeometry ?g . \
                 ?g asset360:asWKT ?w . \
                 FILTER(geof:sfIntersects(?w, \
                 \"<http://www.opengis.net/def/crs/OGC/1.3/CRS84> {box_wkt}\"^^geo:wktLiteral)) }}"
            )),
            vec![FilterCondition::Intersects {
                wkt: box_wkt.to_owned()
            }],
        );
        // An xsd:string makes spargeo return unbound, so the engine answers
        // no rows. Lifting it would make SQL answer rows instead -- the
        // quietest possible route disagreement.
        assert!(
            path_conditions(&format!(
                "SELECT ?s WHERE {{ ?s a asset360:PostalCode ; asset360:hasGeometry ?g . \
                 ?g asset360:asWKT ?w . \
                 FILTER(geof:sfIntersects(?w, \"{box_wkt}\")) }}"
            ))
            .is_empty(),
        );
        // A different CRS is rejected by spargeo, so it must not lift
        // either.
        assert!(
            path_conditions(&format!(
                "SELECT ?s WHERE {{ ?s a asset360:PostalCode ; asset360:hasGeometry ?g . \
                 ?g asset360:asWKT ?w . \
                 FILTER(geof:sfIntersects(?w, \
                 \"<http://www.opengis.net/def/crs/EPSG/0/31370> {box_wkt}\"^^geo:wktLiteral)) }}"
            ))
            .is_empty(),
        );
    }

    /// A `geoJSONLiteral` constant declines even though the shape is
    /// otherwise identical to a lifting `wktLiteral` call. `spargeo` reads a
    /// geometry from `geoJSONLiteral` too (`parse.rs::extract_argument`), so
    /// this is not the "unbound" gate above -- it is deliberately out of
    /// scope, so the fetch must not narrow on the strength of a datatype
    /// this task never validates against `ST_GeomFromGeoJSON`.
    #[test]
    fn sf_intersects_on_a_geojson_literal_does_not_lift() {
        const GEOF: &str = "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> \
                            PREFIX geo: <http://www.opengis.net/ont/geosparql#> ";
        let scope = sparql_scope(
            &format!(
                "{PREFIX}{GEOF}SELECT ?s WHERE {{ ?s a asset360:PostalCode ; \
                 asset360:hasGeometry ?g . ?g asset360:asWKT ?w . \
                 FILTER(geof:sfIntersects(?w, \
                 \"{{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[1,2]}}\"^^geo:geoJSONLiteral)) }}"
            ),
            &test_schema_view(),
        )
        .expect("scopes");
        assert!(
            all_stars(&scope)
                .iter()
                .flat_map(|star| star.path_filters.iter())
                .all(|filter| filter.conditions.is_empty()),
        );
    }

    /// `geof:sfIntersects` on a slot the registry does not claim a column
    /// for does not lift, even though the function, the constant and its
    /// datatype are all exactly the ones that lift on `PostalCode`.
    ///
    /// Isolates the registry gate from the literal-validity gates the other
    /// tests here isolate: `TunnelComplex.hasName` is an ordinary string
    /// slot, and `sparql_columns::broken_out_column` claims no column for
    /// it.
    #[test]
    fn sf_intersects_on_a_slot_the_registry_does_not_claim_does_not_lift() {
        const GEOF: &str = "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> \
                            PREFIX geo: <http://www.opengis.net/ont/geosparql#> ";
        let scope = sparql_scope(
            &format!(
                "{PREFIX}{GEOF}SELECT ?s WHERE {{ ?s a asset360:TunnelComplex ; \
                 asset360:hasName ?w . \
                 FILTER(geof:sfIntersects(?w, \"POINT(1 2)\"^^geo:wktLiteral)) }}"
            ),
            &test_schema_view(),
        )
        .expect("scopes");
        assert!(
            all_stars(&scope)
                .iter()
                .all(|star| star.filters.is_empty() && star.path_filters.is_empty()),
        );
    }

    /// A different custom function, over the same slot and the same
    /// `wktLiteral` constant, does not lift: `geof:sfIntersects` is the only
    /// spelling this task recognises.
    #[test]
    fn a_different_function_name_does_not_lift() {
        const GEOF: &str = "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> \
                            PREFIX geo: <http://www.opengis.net/ont/geosparql#> ";
        let scope = sparql_scope(
            &format!(
                "{PREFIX}{GEOF}SELECT ?s WHERE {{ ?s a asset360:PostalCode ; \
                 asset360:hasGeometry ?g . ?g asset360:asWKT ?w . \
                 FILTER(geof:sfWithin(?w, \"POINT(1 2)\"^^geo:wktLiteral)) }}"
            ),
            &test_schema_view(),
        )
        .expect("scopes");
        assert!(
            all_stars(&scope)
                .iter()
                .flat_map(|star| star.path_filters.iter())
                .all(|filter| filter.conditions.is_empty()),
        );
    }

    /// `lift_intersects` looks up the real class URI through `var_to_class`,
    /// not the star variable's own name — and declines when the star is not
    /// in that map, the same refusal `constants_are_the_columns_terms`
    /// applies through `class_of_star.get(star_var).is_some_and(...)` in
    /// `sparql_refine.rs`.
    ///
    /// Not reachable through a parsed query: every `star_var` a `PathBinding`
    /// can name is, by construction, a key of `var_to_class` (both are built
    /// from the same `stars` in Phase 1 — see the `var_to_class.contains_key`
    /// check a few lines above `collect_path_bindings`'s call site, which is
    /// exactly the mechanism that keeps a *path*'s variable out of
    /// `var_to_field` as a `star_var` in the first place). So this is called
    /// directly, the same way `sparql_refine.rs`'s empty-disjunction test
    /// builds an `Expr` by hand for a shape `flatten_or` cannot produce: the
    /// shape is a legal value of the types involved and the refusal is
    /// `lift_intersects`'s, not the parser's.
    ///
    /// What this does **not** pin, and cannot while `broken_out_column`'s own
    /// `class_uri` gate stays "non-empty, not which class"
    /// (`sparql_columns.rs`): a test asserting only that the lift *succeeds*
    /// on a resolved star cannot distinguish "the real class URI was passed"
    /// from "the star variable's name was passed instead" — both are
    /// non-empty strings, and the registry does not (yet) look past that.
    /// `sf_intersects_lifts_on_a_geometry_slot` already exercises the
    /// resolved case end to end; this test's job is narrower and sharper:
    /// pin that an *unresolved* star actually declines, which only holds if
    /// the lookup is real (a lookup that defaulted to "found" for a missing
    /// entry would pass every existing test here undetected). Once the
    /// registry grows a real per-class check, a further test can assert the
    /// two failure modes ("star unresolved" vs "class resolved but not the
    /// one the registry wants") disagree — today they cannot, because the
    /// registry cannot tell them apart either.
    #[test]
    fn lift_intersects_declines_a_star_var_to_class_does_not_name() {
        let var_to_field: ValueColumns = HashMap::from([(
            "w".to_owned(),
            (
                "s".to_owned(),
                vec!["hasGeometry".to_owned(), "asWKT".to_owned()],
                PushForm::Literal {
                    datatype: None,
                    lang: None,
                    numeric: false,
                },
            ),
        )]);
        // Deliberately empty: "s" (the only star `var_to_field` names) is not
        // a key here, as if the builder that would have inserted it never
        // ran.
        let var_to_class: HashMap<String, String> = HashMap::new();
        let mut star_filters: StarFilters = HashMap::new();
        let optional_fields: HashMap<String, Vec<String>> = HashMap::new();

        let function = spargebra::algebra::Function::Custom(
            spargebra::term::NamedNode::new_unchecked(SF_INTERSECTS_IRI),
        );
        let args = vec![
            Expression::Variable(spargebra::term::Variable::new("w").unwrap()),
            Expression::Literal(spargebra::term::Literal::new_typed_literal(
                "POINT(1 2)",
                spargebra::term::NamedNode::new_unchecked(WKT_LITERAL_IRI),
            )),
        ];

        let lifted = lift_intersects(
            &function,
            &args,
            &var_to_field,
            &mut star_filters,
            &optional_fields,
            &var_to_class,
        );
        assert!(!lifted, "an unresolved star must not lift");
        assert!(
            star_filters.is_empty(),
            "nothing should have been pushed either"
        );
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
            // An unknown predicate on a typed subject used to be a fourth
            // case here; it is refused at the parse now (see
            // `crate::sparql_alias`), so there is no plan to bound.
            (
                "variable predicate",
                "SELECT ?s WHERE { ?s a asset360:Signal . ?s ?p \"x\" } LIMIT 10",
            ),
        ] {
            let plan = sparql_scope(&format!("{prefix}{query}"), &sv).unwrap();
            assert_eq!(plan.sql_limit, None, "sql_limit must be None for: {label}");
        }
    }

    /// A `LIMIT` over a join reaches the fetch as a bound on the **driving
    /// scan**, and only where every row of that scan is worth at least one
    /// solution.
    ///
    /// Before this, a join of any kind meant no bound at all: `LIMIT 50` and
    /// `LIMIT 500` fetched the same 20 000 records and the same 1.5M triples,
    /// and the engine's own cap refused the query after forty seconds of
    /// database work (issue #443, asset360 pepibru GitLab). The narrowing had
    /// to come from a `FILTER`, which is not what a client paging an export
    /// writes.
    ///
    /// The refusals in the table are the point of the rule and not its
    /// leftovers: an inner join can eliminate a driving row, so N driving rows
    /// are not N solutions, and a bound there returns fewer rows than the
    /// query asked for with nothing to say so.
    #[test]
    fn a_limit_bounds_the_driving_scan_of_a_rooted_optional_join() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";
        let optional_hop = "?c a asset360:CivilEngineeringAsset ; asset360:hasName ?h . \
             OPTIONAL { ?c asset360:belongsToTunnelComplex ?t . \
             ?t a asset360:TunnelComplex ; asset360:hasName ?n }";

        for (label, expected, scope, query) in [
            (
                "one OPTIONAL hop off one mandatory star",
                Some(50),
                Some(LimitScope::DrivingScan),
                format!("SELECT ?c ?n WHERE {{ {optional_hop} }} LIMIT 50"),
            ),
            (
                "an OFFSET is a position in a sequence the fetch does not share",
                None,
                None,
                format!("SELECT ?c ?n WHERE {{ {optional_hop} }} LIMIT 50 OFFSET 100"),
            ),
            (
                "an ORDER BY still has to see every solution first",
                None,
                None,
                format!("SELECT ?c ?n WHERE {{ {optional_hop} }} ORDER BY ?h LIMIT 50"),
            ),
            (
                "a mandatory hop is an inner join, which can drop a driving row",
                None,
                None,
                "SELECT ?c ?n WHERE { ?c a asset360:CivilEngineeringAsset ; \
                 asset360:hasName ?h ; asset360:belongsToTunnelComplex ?t . \
                 ?t a asset360:TunnelComplex ; asset360:hasName ?n } LIMIT 50"
                    .to_owned(),
            ),
            // The single-relation bound is the other scope, and the only one
            // a `UNION` may stack: its rows *are* its solutions.
            (
                "one star, no join: the bound is on the rows themselves",
                Some(50),
                Some(LimitScope::Rows),
                "SELECT ?c WHERE { ?c a asset360:CivilEngineeringAsset } LIMIT 50".to_owned(),
            ),
        ] {
            let plan = sparql_scope(&format!("{prefix}{query}"), &sv).unwrap();
            assert_eq!(plan.sql_limit, expected, "for: {label}");
            assert_eq!(plan.sql_limit_scope, scope, "scope, for: {label}");
        }
    }

    /// The premise of the fetch bound, restated: a mandatory read through a
    /// nested path is a presence check on the scan, so every fetched row is
    /// one the query has a solution for. `LIMIT 50` over
    /// `?s :superStructure ?c . ?c :hasMaterial ?v` fetched fifty records,
    /// twelve without a material, and answered 38 with no error (issue #455,
    /// pepibru GitLab).
    ///
    /// One table: which nested reads a scan restates, and which shape
    /// declines the bound instead. Nothing in between.
    #[test]
    fn a_nested_read_is_restated_on_the_scan_or_the_bound_is_declined() {
        use crate::sparql_pushdown::Container::{Mapping, Single};
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";
        let path =
            |slots: &[&str], containers: &[crate::sparql_pushdown::Container]| RequiredPath {
                slot_path: slots.iter().map(|s| (*s).to_owned()).collect(),
                containers: containers.to_vec(),
            };

        for (label, query, expected, limit) in [
            (
                "the issue's shape: one hop into a structure, single-valued",
                "SELECT ?s ?v WHERE { ?s a asset360:Signal ; asset360:location ?c . \
                 ?c asset360:longitude ?v } LIMIT 50",
                vec![path(&["location", "longitude"], &[Single, Single])],
                Some(50),
            ),
            (
                "two leaves under one structure: two entries, sorted by path",
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:location ?c . \
                 ?c asset360:longitude ?lo ; asset360:latitude ?la } LIMIT 50",
                vec![
                    path(&["location", "latitude"], &[Single, Single]),
                    path(&["location", "longitude"], &[Single, Single]),
                ],
                Some(50),
            ),
            (
                "three hops: the whole chain",
                "SELECT ?v WHERE { ?s a asset360:Signal ; asset360:location ?c . \
                 ?c asset360:detail ?d . ?d asset360:value ?v } LIMIT 5",
                vec![path(
                    &["location", "detail", "value"],
                    &[Single, Single, Single],
                )],
                Some(5),
            ),
            (
                "through a mapping to an ordinary slot: restated, `.*` at the hop",
                "SELECT ?s ?t WHERE { ?s a asset360:Signal ; asset360:documents ?d . \
                 ?d asset360:title ?t } LIMIT 50",
                vec![path(&["documents", "title"], &[Mapping, Single])],
                Some(50),
            ),
            (
                "a read inside OPTIONAL requires nothing, and the bound stays",
                "SELECT ?s ?v WHERE { ?s a asset360:Signal ; asset360:name ?n . \
                 OPTIONAL { ?s asset360:location ?c . ?c asset360:longitude ?v } } LIMIT 50",
                vec![],
                Some(50),
            ),
            (
                "the key slot of a mapping element lives in the dict key, not the \
                 payload: nothing the scan can state, so the bound is declined",
                "SELECT ?s ?k WHERE { ?s a asset360:Signal ; asset360:documents ?d . \
                 ?d asset360:docId ?k } LIMIT 50",
                vec![],
                None,
            ),
        ] {
            let plan = sparql_scope(&format!("{prefix}{query}"), &sv).unwrap();
            assert_eq!(plan.inexact, None, "a walked path is not a loss: {label}");
            let star = &plan.root.all_stars()[0];
            assert_eq!(star.required_paths, expected, "restated, for: {label}");
            assert_eq!(plan.sql_limit, limit, "bound, for: {label}");
        }
    }

    /// The same premise on the driving scan of a rooted `OPTIONAL` join: the
    /// bound is on the mandatory star's rows, so it is that star's nested
    /// reads that are restated, and an optional star's never are.
    #[test]
    fn a_driving_scan_restates_its_own_nested_reads_and_keeps_its_bound() {
        use crate::sparql_pushdown::Container::Single;
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s ?v ?n WHERE { ?s a asset360:Signal ; asset360:location ?c . \
             ?c asset360:longitude ?v . \
             OPTIONAL { ?s asset360:locatedOnTrack ?t . ?t a asset360:Track ; \
             asset360:documents ?d . ?d asset360:title ?n } } LIMIT 50",
            &sv,
        )
        .unwrap();
        assert_eq!(plan.inexact, None);
        assert_eq!(plan.sql_limit, Some(50));
        assert_eq!(plan.sql_limit_scope, Some(LimitScope::DrivingScan));
        let stars = plan.root.all_stars();
        let driving = stars.iter().find(|s| s.variable == "s").unwrap();
        let optional = stars.iter().find(|s| s.variable == "t").unwrap();
        assert_eq!(
            driving.required_paths,
            vec![RequiredPath {
                slot_path: vec!["location".to_owned(), "longitude".to_owned()],
                containers: vec![Single, Single],
            }]
        );
        assert!(
            optional.required_paths.is_empty(),
            "an optional star's read leaves the variable unbound, it does not cost a row"
        );
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
            // On a subject with one known type this is refused at the parse
            // (see `crate::sparql_alias`); the drop site is still reachable
            // through a subject the refusal leaves to the scoper, such as one
            // typed twice.
            (
                Inexact::UnknownPredicate,
                "SELECT ?s WHERE { ?s a asset360:Signal ; a asset360:Track . ?s <urn:unknown> \"x\" }",
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
            // A GRAPH block over a graph the endpoint does not have: the plan
            // reads the default one. (The *schema* graph is different — its
            // patterns are dropped before star building, and the query below
            // has none.)
            (
                Inexact::NamedGraph,
                "SELECT ?s WHERE { GRAPH <urn:g> { ?s a asset360:Signal } }",
            ),
            // UNDEF means "no constraint", so dropping the cell turned a union
            // into an intersection.
            (
                Inexact::UndefInValues,
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                 VALUES ?nm { \"BX1\" UNDEF } }",
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
                // Reached through two slots of different ranges, because a
                // node one slot reaches is judged by that slot's range at the
                // parse and refused there (#459, pepibru GitLab; see
                // `sparql_alias`).
                Inexact::UnrepresentedTriple,
                "SELECT ?lo WHERE { ?s a asset360:Signal ; asset360:location ?c ; \
                 asset360:documents ?c . ?c asset360:longitude ?lo ; asset360:name ?x }",
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
            let plan = sparql_scope(&format!("{prefix}{query}"), &sv)
                .unwrap_or_else(|e| panic!("scope failed for {query}: {e:?}"));
            assert_eq!(
                plan.inexact,
                Some(expected),
                "wrong cause recorded for: {query}"
            );
        }
    }

    /// A `GRAPH` pattern is not an instance pattern.
    ///
    /// The endpoint's only named graph holds the datamodel, so a pattern inside
    /// one says nothing about which golden records to fetch. Two things follow,
    /// and both used to be wrong: the instance scope must come from the default
    /// graph alone, and a constant IRI subject inside a `GRAPH` — a schema term,
    /// which is how every enum-value lookup is written — must not be refused as
    /// an unscopable instance subject.
    #[test]
    fn a_named_graph_pattern_does_not_scope_the_fetch() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/>                       PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> ";

        // The asset360 datamodel's configured schema graph. Passed in, because
        // it is configuration and not a constant -- see
        // `crate::sparql_schema_graph`.
        let schema_graph = "https://data.infrabel.be/asset360/schema";
        let plan = sparql_scope_with_schema_graph(
            &format!(
                "{prefix}SELECT ?s ?l WHERE {{ ?s a asset360:Signal .                  GRAPH <{schema_graph}> {{                  <https://example.org/term> rdfs:label ?l }} }}"
            ),
            &sv,
            Some(schema_graph),
        )
        .expect("a schema pattern must not make the query unscopable");

        let stars = plan.root.all_stars();
        assert_eq!(
            stars.len(),
            1,
            "the fetch must come from the default graph alone, got {stars:?}"
        );
        assert!(stars[0].class_uri.ends_with("Signal"), "{stars:?}");
        // The engine has to finish it, because the plan cannot read the graph.
        assert_eq!(plan.inexact, Some(Inexact::NamedGraph));
    }

    /// A query that reads *only* a named graph has no instance scope at all.
    ///
    /// This is the honest answer rather than a fetch of everything: the scoper
    /// owns golden records and this query asks about none. The endpoint answers
    /// it from the schema graph with no objects loaded.
    #[test]
    fn a_query_reading_only_a_named_graph_is_unscoped() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>              SELECT ?c ?l WHERE { GRAPH <https://data.infrabel.be/asset360/schema>              { ?c rdfs:label ?l } }",
            &sv,
        );
        assert!(
            matches!(result, Err(ScopeError::Unscoped { .. })),
            "expected Unscoped, got {result:?}"
        );
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

        // A wrong type on the nested variable used to be the same shape, and
        // emitted the same bogus edge; it is refused at the parse now, because
        // `Track` carries no `longitude` (see `crate::sparql_alias`).
        let err = sparql_scope(
            &format!(
                "{prefix}SELECT ?lo WHERE {{ ?s a asset360:Signal ; asset360:location ?c . \
                 ?c a asset360:Track ; asset360:longitude ?lo }}"
            ),
            &sv,
        )
        .unwrap_err();
        assert!(
            matches!(&err, ScopeError::UnsupportedConstruct(msg) if msg.contains("not a slot of Track")),
            "a wrong nested type is refused by name: {err:?}"
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

        // Untyped with a predicate `Coordinates` cannot carry: the class of
        // ?c is known from the slot it was reached through, so this is
        // refused at the parse like the typed form, naming the hop -- in the
        // mandatory pattern and inside an OPTIONAL, where it used to be an
        // unbound column on every row (#459, pepibru GitLab).
        for query in [
            format!(
                "{prefix}SELECT ?x WHERE {{ ?s a asset360:Signal ; asset360:location ?c . \
                 ?c asset360:hasName ?x }}"
            ),
            format!(
                "{prefix}SELECT ?x WHERE {{ ?s a asset360:Signal . \
                 OPTIONAL {{ ?s asset360:location ?c . ?c asset360:hasName ?x }} }}"
            ),
        ] {
            let err = sparql_scope(&query, &sv).unwrap_err();
            assert!(
                matches!(&err, ScopeError::UnsupportedConstruct(msg)
                    if msg.contains("not a slot of Coordinates")
                        && msg.contains("`?s asset360:location ?c` reaches")),
                "a wrong predicate on a reached node is refused by name: {err:?}"
            );
        }

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

    /// An enum column stores a code, not the term it renders as — and it
    /// renders as a concept IRI for every value, whether the schema mapped it
    /// to an ontology (`GSA`) or the IRI was minted for it (`KSS`).
    #[test]
    fn an_enum_constant_is_translated_back_to_the_stored_code() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        // A declared meaning selects the code whose meaning it is.
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

        // A minted IRI is not a lesser kind of constant: same rule, same push.
        let plan = sparql_scope(
            &format!(
                "{prefix}SELECT ?s WHERE {{ ?s a asset360:Signal ; \
                 asset360:kind <https://data.infrabel.be/asset360/SignalKind#KSS> }}"
            ),
            &sv,
        )
        .unwrap();
        assert_eq!(plan.inexact, None, "a minted IRI is pushable too");
        assert_eq!(
            plan.root.all_stars()[0].filters["kind"],
            vec![FilterCondition::Eq("KSS".to_owned())]
        );

        // A literal is a term no record renders as -- for any value, not just
        // the mapped ones -- and is refused at the parse, naming the concept
        // IRI (`crate::sparql_alias`, #461).
        for (code, concept) in [
            ("GSA", "http://ontorail.org/src/Eulynx/GSA"),
            ("KSS", "https://data.infrabel.be/asset360/SignalKind#KSS"),
        ] {
            let err = sparql_scope(
                &format!(
                    "{prefix}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:kind \"{code}\" }}"
                ),
                &sv,
            )
            .unwrap_err();
            let ScopeError::UnsupportedConstruct(msg) = &err else {
                panic!("literal {code:?}: expected a refusal, got {err:?}");
            };
            assert!(
                msg.contains(&format!("<{concept}>")),
                "literal {code:?} is refused naming its concept: {msg}"
            );
        }
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
                     FILTER(?k = <https://data.infrabel.be/asset360/SignalKind#KSS>) }}"
                ),
                FilterCondition::Eq("KSS".to_owned()),
            ),
            (
                format!(
                    "{prefix}SELECT ?s WHERE {{ ?s a asset360:Signal ; asset360:kind ?k . \
                     FILTER(?k IN (<http://ontorail.org/src/Eulynx/GSA>, \
                     <https://data.infrabel.be/asset360/SignalKind#KSS>)) }}"
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

    /// An `OFFSET` declines the bound, on a single star as on a join.
    ///
    /// This used to push `OFFSET + LIMIT`, on the reasoning that the fetch has
    /// to cover the window and the engine skips the offset from what comes
    /// back. It does — in its own order, which is not the fetch's, so `LIMIT 10
    /// OFFSET 20` fetched the thirty lowest records and the engine handed back
    /// the same ten `OFFSET 0` did: page one for every page, with no error
    /// (consolidator-server !995 review round 2, and issue #456 for this
    /// single-star shape; both pepibru GitLab). The fetch is unbounded under an
    /// offset until an order is pushed with the bound.
    #[test]
    fn an_offset_declines_the_bound() {
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        for (query, expected) in [
            (
                "SELECT ?s WHERE { ?s a asset360:Signal } LIMIT 10",
                Some(10),
            ),
            (
                "SELECT ?s WHERE { ?s a asset360:Signal } LIMIT 10 OFFSET 20",
                None,
            ),
            (
                // `OFFSET 0` is written but is no offset: the window starts
                // where the fetch does.
                "SELECT ?s WHERE { ?s a asset360:Signal } LIMIT 10 OFFSET 0",
                Some(10),
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

    #[test]
    fn a_not_exists_pattern_is_read_into_the_fetch() {
        // The engine answers `NOT EXISTS` from the fetched instances, so the
        // records the pattern asks about have to be in the fetch. Scoped away,
        // the filter sees an empty class and holds for every row.
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?complex WHERE { ?complex a asset360:TunnelComplex . \
             FILTER NOT EXISTS { ?component a asset360:CivilEngineeringAsset ; \
             asset360:belongsToTunnelComplex ?complex } }",
            &sv,
        )
        .unwrap();

        let star = find_star(&plan, "component");
        assert_eq!(
            star.class_uri,
            "https://data.infrabel.be/asset360/CivilEngineeringAsset"
        );
        // Not a constraint on the outer rows: a complex with no component is
        // exactly what the query selects, so the fetch must keep it.
        assert!(star.is_optional);
        assert_ne!(plan.inexact, None);
    }

    #[test]
    fn an_exists_the_fetch_cannot_read_is_refused_rather_than_skipped() {
        // The second half of the same bug. A pattern nothing reads into the
        // fetch must refuse the query, because the alternative is what this
        // walk used to do: skip it, fetch none of the records it names, and
        // let the engine evaluate it against an empty class.
        let sv = test_schema_view();
        let prefix = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

        for query in [
            // Ordered by a pattern, which is computed over rows the fetch
            // already produced.
            "SELECT ?s WHERE { ?s a asset360:Signal } \
             ORDER BY (EXISTS { ?s asset360:belongsToLine ?l })",
            // Aggregated over one, same reason.
            "SELECT (SUM(IF(EXISTS { ?s asset360:belongsToLine ?l }, 1, 0)) AS ?n) \
             WHERE { ?s a asset360:Signal }",
        ] {
            let error = sparql_scope(&format!("{prefix}{query}"), &sv)
                .expect_err("should refuse rather than answer: {query}");
            let ScopeError::UnsupportedConstruct(message) = &error else {
                panic!("expected an unsupported-construct refusal, got {error:?}");
            };
            assert!(
                message.contains("EXISTS"),
                "the refusal should name the construct: {message}"
            );
        }
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
        assert!(
            !join.right_multivalued,
            "belongsToTunnelComplex holds one identifier"
        );
        assert_eq!(join.join_type, JoinType::Inner);

        // Multi-type join → no SQL LIMIT pushdown
        assert_eq!(plan.sql_limit, None);
    }

    /// A join across a *multivalued* reference says so, and the renderer needs
    /// it to.
    ///
    /// `groupsLines` holds an array of identifiers, so
    /// `object_data->>'groupsLines' = uri` compares the array's own text —
    /// `["…/Line/1", "…/Line/2"]` — and matches nothing, for a record whose
    /// data is there. That is a join that answers *empty* rather than
    /// erroring, which a caller cannot tell from "no such data": the worst
    /// available failure. The renderer avoids it only by being told, so the
    /// edge carries the fact.
    #[test]
    fn a_multivalued_reference_edge_says_it_is_multivalued() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?g ?l ?ln WHERE { \
               ?g a asset360:LineGroup ; asset360:groupsLines ?l . \
               ?l a asset360:Line ; asset360:hasName ?ln . \
             }",
            &sv,
        )
        .unwrap();

        let joins = all_joins(&plan);
        assert_eq!(joins.len(), 1, "{joins:?}");
        let join = joins[0];
        assert_eq!(join.left, "l");
        assert_eq!(join.right, "g");
        assert_eq!(join.right_slot, "groupsLines");
        assert!(join.right_multivalued);
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
        assert!(matches!(result, Err(ScopeError::Unscoped { .. })));
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

    /// The shape the construct exists for, and the one a star map cannot
    /// hold: one variable, two classes. Both have to be fetched, or the
    /// second arm answers empty.
    #[test]
    fn test_union_scopes_every_branch() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT * WHERE { { ?s a asset360:Signal } UNION { ?s a asset360:BaliseGroup } }",
            &sv,
        )
        .expect("a UNION is scopable");
        let mut classes: Vec<&str> = plan
            .root
            .all_stars()
            .iter()
            .map(|s| s.class_uri.as_str())
            .collect();
        classes.sort_unstable();
        assert_eq!(
            classes,
            vec![
                "https://data.infrabel.be/asset360/BaliseGroup",
                "https://data.infrabel.be/asset360/Signal",
            ],
            "both arms have to be fetched: {plan:?}"
        );
        assert_eq!(plan.inexact, Some(Inexact::UnionBranch));
        assert_eq!(
            plan.sql_limit, None,
            "a LIMIT does not bound a branch fetch"
        );
    }

    /// Two stars in one plan cannot share a name: the name is the SQL alias.
    #[test]
    fn test_union_branches_get_distinct_star_names() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT * WHERE { { ?s a asset360:Signal } UNION { ?s a asset360:BaliseGroup } }",
            &sv,
        )
        .expect("a UNION is scopable");
        let names: HashSet<&str> = plan
            .root
            .all_stars()
            .iter()
            .map(|s| s.variable.as_str())
            .collect();
        assert_eq!(names.len(), 2, "one alias per star: {plan:?}");
    }

    /// A join in one branch must not narrow another branch's fetch.
    ///
    /// Found by driving the endpoint from Django rather than from here: the
    /// first arm silently lost every tunnel complex that no civil engineering
    /// asset points at. Both arms scope a `TunnelComplex` star of the same
    /// shape, the deduplication merged them, and the second arm's join edge
    /// was retargeted onto the first arm's star — so the statement joined a
    /// fetch the first arm never asked to have joined. A short answer with a
    /// balanced ledger and no error, which is the one failure mode the whole
    /// distribution exists to prevent.
    ///
    /// The property, asserted rather than the fix: no join edge may name a
    /// star that more than one branch reads.
    #[test]
    fn test_a_join_in_one_branch_does_not_narrow_another() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s ?tn WHERE { \
               { ?s a asset360:TunnelComplex ; asset360:hasName ?tn } \
               UNION \
               { ?s a asset360:CivilEngineeringAsset ; \
                    asset360:belongsToTunnelComplex ?t . \
                 ?t a asset360:TunnelComplex ; asset360:hasName ?tn } \
             }",
            &sv,
        )
        .expect("a UNION is scopable");

        // Three stars: each arm's own, and the joined one is the second arm's
        // alone. Two would mean the first arm's star is the join's target.
        assert_eq!(
            all_stars(&plan).len(),
            3,
            "the joined star is the second arm's own: {plan:?}"
        );

        let joins = all_joins(&plan);
        assert_eq!(joins.len(), 1, "{plan:?}");
        // The first arm's star is named for the query variable it came from,
        // and it is the one that must stay unjoined.
        let unjoined = find_star(&plan, "s");
        assert_eq!(
            unjoined.class_uri,
            "https://data.infrabel.be/asset360/TunnelComplex"
        );
        for join in &joins {
            assert!(
                join.left != unjoined.variable && join.right != unjoined.variable,
                "a join edge narrows the other branch's fetch: {join:?} in {plan:?}"
            );
        }
    }

    /// A triple outside the union belongs to every branch, and a triple inside
    /// one belongs only to its own: two stars of the same class, each
    /// requiring what its branch reads, and neither requiring the other's.
    #[test]
    fn test_union_does_not_make_one_arms_read_mandatory() {
        let sv = test_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT * WHERE { ?s a asset360:Signal . \
             { ?s asset360:name ?n } UNION { ?s asset360:length ?t } }",
            &sv,
        )
        .expect("a UNION is scopable");
        for star in plan.root.all_stars() {
            assert!(
                !(star.required_fields.contains(&"name".to_owned())
                    && star.required_fields.contains(&"length".to_owned())),
                "one branch's read is not the other's: {star:?}"
            );
        }
    }

    /// A branch that cannot be scoped is the whole query's refusal: its
    /// records would never be fetched and that arm would answer empty.
    #[test]
    fn test_union_branch_that_cannot_be_scoped_refuses_the_query() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT * WHERE { { ?s a asset360:Signal } UNION { ?x ?p ?o } }",
            &sv,
        );
        assert!(
            matches!(result, Err(ScopeError::Unscoped { .. })),
            "an unscopable arm refuses the query, got {result:?}"
        );
    }

    /// Branches multiply, and the planner says so rather than planning
    /// 2^n conjunctive queries.
    #[test]
    fn test_too_many_union_branches_are_refused() {
        let sv = test_schema_view();
        let arms = (0..5)
            .map(|_| "{ { ?s a asset360:Signal } UNION { ?s a asset360:BaliseGroup } }".to_owned())
            .collect::<Vec<_>>()
            .join(" ");
        let result = sparql_scope(
            &format!(
                "PREFIX asset360: <https://data.infrabel.be/asset360/> SELECT * WHERE {{ {arms} }}"
            ),
            &sv,
        );
        assert!(
            matches!(result, Err(ScopeError::UnsupportedConstruct(ref m)) if m.contains("branches")),
            "expected a branch-count refusal, got {result:?}"
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

    /// Review finding 4. A dataset clause used to be discarded by the `..` in
    /// every arm of [`scope_parsed_with_schema_graph`], so
    /// `FROM <urn:empty>` was planned as all-in-SQL, admitted, and answered
    /// from the table — `2` for a dataset in which the answer is `0`. Refused
    /// by name now, for both routes: see doc 28h on why routing it to the
    /// engine would be correct today and still the wrong default.
    #[test]
    fn a_dataset_clause_is_refused_by_name() {
        let sv = test_schema_view();
        for query in [
            "SELECT (COUNT(*) AS ?n) FROM <urn:empty> WHERE { ?s a asset360:Signal }",
            "SELECT ?s FROM NAMED <urn:empty> WHERE { ?s a asset360:Signal }",
            "ASK FROM <urn:empty> WHERE { ?s a asset360:Signal }",
        ] {
            let result = sparql_scope(
                &format!("PREFIX asset360: <https://data.infrabel.be/asset360/> {query}"),
                &sv,
            );
            assert!(
                matches!(result, Err(ScopeError::UnsupportedConstruct(ref m)) if m.contains("FROM")),
                "expected a FROM refusal for `{query}`, got {result:?}"
            );
        }
        // And no dataset clause is still scoped, which is every query the
        // application sends.
        assert!(
            sparql_scope(
                "PREFIX asset360: <https://data.infrabel.be/asset360/> \
                 SELECT ?s WHERE { ?s a asset360:Signal }",
                &sv,
            )
            .is_ok()
        );
    }

    #[test]
    fn test_lateral_rejected() {
        let sv = test_schema_view();
        let result = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT * WHERE { ?s a asset360:Signal . LATERAL { ?s asset360:name ?n } }",
            &sv,
        );
        assert!(
            matches!(result, Err(ScopeError::UnsupportedConstruct(ref m)) if m.contains("LATERAL")),
            "expected UnsupportedConstruct with LATERAL, got {result:?}"
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

    // ---- A reference reached through an inline structure (issue #444) ----

    /// The real schema, because the shape needs an inline *list* whose
    /// elements hold a reference: `TunnelComplex.hasCoveredSection` is a list
    /// of `CoveredSection`, and `CoveredSection.belongsToTrack` references
    /// `Track`. The inline test schema has no such slot.
    fn asset360_fixture_schema_view() -> SchemaView {
        use linkml_meta::SchemaDefinition;
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("data");
        let mut sv = SchemaView::new();
        for name in ["types.yaml", "rsm.yaml", "eulynx.yaml", "asset360.yaml"] {
            let yaml = std::fs::read_to_string(dir.join(name)).unwrap();
            let deser = serde_yml::Deserializer::from_str(&yaml);
            let schema: SchemaDefinition = serde_path_to_error::deserialize(deser).unwrap();
            sv.add_schema(schema).unwrap();
        }
        sv
    }

    const TUNNEL_TRACK_OPTIONAL: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> \
        SELECT ?s ?tn WHERE { \
          ?s a asset360:TunnelComplex ; asset360:typeURI ?n . \
          OPTIONAL { ?s asset360:hasCoveredSection ?cs . \
                     ?cs asset360:belongsToTrack ?t . \
                     ?t a asset360:Track ; asset360:typeURI ?tn } }";

    /// `?t` is reached from `?s` through `?cs`, an element of an inline list.
    /// That is a join -- ?t's identifier is in ?s's record, two slots down --
    /// and the edge says where: the path on it is what a renderer has to walk,
    /// because there is no `belongsToTrack` column on a TunnelComplex row.
    #[test]
    fn a_reference_inside_an_inline_list_is_a_join_edge_with_a_path() {
        let sv = asset360_fixture_schema_view();
        let plan = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s ?tn WHERE { \
               ?s a asset360:TunnelComplex ; asset360:typeURI ?n . \
               ?s asset360:hasCoveredSection ?cs . \
               ?cs asset360:belongsToTrack ?t . \
               ?t a asset360:Track ; asset360:typeURI ?tn }",
            &sv,
        )
        .unwrap();

        let joins = all_joins(&plan);
        assert_eq!(joins.len(), 1, "{joins:?}");
        let join = joins[0];
        assert_eq!(join.left, "t");
        assert_eq!(join.right, "s");
        assert_eq!(join.right_path, vec!["hasCoveredSection".to_owned()]);
        assert_eq!(join.right_slot, "belongsToTrack");
        assert!(
            join.right_multivalued,
            "the list hop makes the path hold several identifiers"
        );
        assert_eq!(join.join_type, JoinType::Inner);
        // The edge represents `?cs :belongsToTrack ?t`, so nothing is left to
        // the engine.
        assert_eq!(plan.inexact, None, "{:?}", plan.unconsumed);
    }

    /// The same edge inside an OPTIONAL. This was refused as *disconnected*
    /// -- "?t shares no variable with the mandatory pattern" -- because only
    /// column references raised edges and ?t is reached through ?cs. It is
    /// connected by exactly this edge, and the edge is a left join.
    #[test]
    fn an_optional_through_an_inline_list_is_connected() {
        let sv = asset360_fixture_schema_view();
        let plan = sparql_scope(TUNNEL_TRACK_OPTIONAL, &sv).unwrap();

        let joins = all_joins(&plan);
        assert_eq!(joins.len(), 1, "{joins:?}");
        assert_eq!(joins[0].right_path, vec!["hasCoveredSection".to_owned()]);
        assert_eq!(joins[0].join_type, JoinType::Left);
        assert!(find_star(&plan, "t").is_optional);
        assert!(!find_star(&plan, "s").is_optional);
    }

    /// Admitting the path edge admits nothing else: an OPTIONAL star that no
    /// path reaches is still disconnected, inline list or not.
    #[test]
    fn a_path_edge_does_not_connect_an_unrelated_optional() {
        let sv = asset360_fixture_schema_view();
        let result = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s ?tn WHERE { \
               ?s a asset360:TunnelComplex ; asset360:hasCoveredSection ?cs . \
               ?cs asset360:belongsToTrack ?t . ?t a asset360:Track . \
               OPTIONAL { ?u a asset360:Line ; asset360:typeURI ?tn } }",
            &sv,
        );
        assert!(
            matches!(result, Err(ScopeError::UnsupportedConstruct(ref m)) if m.contains("?u") && m.contains("disconnected")),
            "{result:?}"
        );
    }

    /// Through the whole pipeline: the refined plan has no rule for a path
    /// edge (`PushReferenceJoin` pushes column references only), so the
    /// statement route declines and the fetch is the scoper's -- *with* the
    /// edge, so the fetch reads the Tracks the covered sections name rather
    /// than every Track there is, and the engine finishes the OPTIONAL over
    /// both sides.
    #[test]
    fn the_execution_plan_carries_the_path_edge_into_the_fetch() {
        let sv = asset360_fixture_schema_view();
        let plan = crate::sparql_plan::plan_query_refined(TUNNEL_TRACK_OPTIONAL, &sv).unwrap();
        let rendered = format!("{plan}");
        assert!(
            rendered.contains("join      ?s.hasCoveredSection.belongsToTrack[] = ?t   left"),
            "{rendered}"
        );
        assert!(rendered.contains("engine finishes"), "{rendered}");
    }

    /// The other half of issue #444, and the worse one: drop `?t a
    /// asset360:Track` and the query used to be *accepted* -- ?t was an
    /// untyped subject, recorded as inexact, and the engine finished over a
    /// store holding no Track at all. Every row came back with `?tn` unbound.
    /// A refusal that names the type to add is the only honest answer.
    #[test]
    fn an_untyped_reference_through_an_inline_list_is_refused_by_name() {
        let sv = asset360_fixture_schema_view();
        let result = sparql_scope(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s ?tn WHERE { \
               ?s a asset360:TunnelComplex ; asset360:typeURI ?n . \
               OPTIONAL { ?s asset360:hasCoveredSection ?cs . \
                          ?cs asset360:belongsToTrack ?t . \
                          ?t asset360:typeURI ?tn } }",
            &sv,
        );
        let Err(ScopeError::Unscoped { .. }) = &result else {
            panic!("expected an unscoped refusal, got {result:?}");
        };
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("?t is the object of `hasCoveredSection.belongsToTrack` on ?s")
                && msg.contains("Add `?t a <https://data.infrabel.be/asset360/Track>`"),
            "{msg}"
        );
    }
}
