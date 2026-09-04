//! **What a grouped statement is made of**: the column specs a renderer reads
//! — one `BindingSpec` per projected variable, resolved to where its value
//! lives and how its stored text becomes an RDF term, plus the measures, the
//! `HAVING` terms and the `ORDER BY` keys that address them by position.
//!
//! **This module used to be a planner**, and the name is what is left of it.
//! It classified a whole query for aggregate pushdown in one pass —
//! `analyse_pushdown` answering eligible or blocked, with a closed vocabulary
//! of refusal codes and a rewrite hint for each — and that analysis is
//! deleted. Deciding eligibility for the grouped question as a whole is
//! exactly the cliff [`crate::sparql_rules`] exists to remove: one filter SQL
//! cannot express cost the entire grouping. The refinement pipeline pushes
//! what it can and leaves the rest to the engine, so there is no verdict to
//! give. See `doc_book/src/design/28d-plan-refinement.md`.
//!
//! What survives is what a *statement* is built from, and the few judgements
//! the lowering still needs: resolving a column against the schema
//! ([`binding_spec`]), whether a path multiplies rows ([`path_multiplies`]),
//! and whether a `HAVING` constant is the term its column renders as
//! ([`having_constant`]). One derivation each — two would be how two planners
//! come to disagree about an answer, which is the thing being removed.

use linkml_schemaview::schemaview::SchemaView;

use crate::sparql_scoper::FilterCondition;
use crate::sparql_terms::{TermDescriptor, resolve_column};

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

/// One `HAVING` comparison: a solution column against a constant.
///
/// The key is an [`OrderKey`] because it is the same question `ORDER BY`
/// asks -- which column of the grouped result -- and reusing it means a
/// `HAVING` over an aggregate names the *same* measure the projection
/// computes. That is the point rather than a convenience: `HAVING (COUNT(*) >
/// 1)` beside `(COUNT(*) AS ?n)` is one entry in spargebra's aggregate list,
/// so the renderer emits one expression for both, and there is no second
/// derivation to disagree with the column beside it.
///
/// A measure the projection does *not* ask for works the same way, and is
/// supported deliberately: `HAVING (MAX(?len) > 3)` hoists an aggregate
/// spargebra names internally, which becomes a measure that is computed and
/// not projected -- the shape `ORDER BY DESC(COUNT(*))` already relies on.
#[derive(Debug, Clone)]
pub struct HavingTerm {
    pub key: OrderKey,
    /// The same condition vocabulary a `FILTER` pushes, so a `HAVING` and a
    /// `WHERE` are held to one notion of what SQL can compare -- including the
    /// term test that decides whether a constant is the query's own.
    pub condition: FilterCondition,
    /// Whether the comparison is numeric rather than textual. On an aggregate
    /// this is a property of the *result*: a count is an integer whatever it
    /// counted, while `MIN`/`MAX` carry the term of the column they read.
    pub numeric: bool,
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

/// The constant a `HAVING` compares against, and whether the comparison is
/// numeric -- or why the constant is not one SQL may compare against that
/// column.
///
/// Term fidelity, and the same rule as everywhere else: a comparison asks the
/// query's question only when the constant is the term the value renders as.
/// Wrong here would select no groups rather than the wrong ones, which is a
/// wrong answer and not a narrowing -- there is no engine leg to re-apply
/// anything past a grouping, on either route.
///
/// Shared with the refined pipeline's lowering, which reaches the same
/// `HavingTerm` from a different syntax. One judgement, two callers: two
/// derivations of "is this constant faithful" is how two planners come to
/// disagree about an answer.
pub(crate) fn having_constant(
    form: &MeasureForm,
    bindings: &[BindingSpec],
    literal: &spargebra::term::Literal,
) -> Result<(String, bool), String> {
    match form {
        // A count is an integer whatever it counted; a sum or an average over a
        // numeric range is a number. Both compare by *value*, so any numeric
        // literal is the same question -- unlike a stored-text comparison,
        // where the lexical form has to match.
        MeasureForm::Number => match numeric_value(literal) {
            Some(value) => Ok((value, true)),
            None => Err(format!(
                "HAVING compares an aggregate against {literal}, which is not a number"
            )),
        },
        // `MIN`/`MAX` hand back one of the column's own values, so the column's
        // term rule decides, exactly as it would for a `FILTER` on that slot.
        MeasureForm::Column(index) => {
            let binding = &bindings[*index];
            let form = crate::sparql_scoper::push_form_of_descriptor(&binding.descriptor);
            if !crate::sparql_scoper::literal_pushable(literal, &form) {
                return Err(format!(
                    "HAVING compares ?{} against {literal}, which is not the term \
                     that column's values render as",
                    binding.var
                ));
            }
            Ok((literal.value().to_owned(), binding.descriptor.numeric))
        }
    }
}

/// What an aggregate's result compares as.
pub(crate) enum MeasureForm {
    /// A number in its own right: a count, a sum, an average.
    Number,
    /// One of a column's own values, so that column's term rule applies.
    Column(usize),
}

pub(crate) fn measure_form(func: &Measure) -> MeasureForm {
    match func {
        Measure::Count { .. } | Measure::Sum { .. } | Measure::Avg { .. } => MeasureForm::Number,
        Measure::Min { arg } | Measure::Max { arg } => MeasureForm::Column(*arg),
    }
}

/// A literal's value when it is a number SPARQL would compare numerically.
///
/// A plain literal is not one: `COUNT(*) > "3"` compares an integer with a
/// string, which is a type error in SPARQL and selects no group -- so pushing
/// it as a numeric comparison would answer a question the query did not ask.
fn numeric_value(literal: &spargebra::term::Literal) -> Option<String> {
    if literal.language().is_some() {
        return None;
    }
    const NUMERIC: &[&str] = &[
        "http://www.w3.org/2001/XMLSchema#integer",
        "http://www.w3.org/2001/XMLSchema#decimal",
        "http://www.w3.org/2001/XMLSchema#double",
        "http://www.w3.org/2001/XMLSchema#float",
        "http://www.w3.org/2001/XMLSchema#long",
        "http://www.w3.org/2001/XMLSchema#int",
        "http://www.w3.org/2001/XMLSchema#short",
        "http://www.w3.org/2001/XMLSchema#byte",
        "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
        "http://www.w3.org/2001/XMLSchema#positiveInteger",
    ];
    if !NUMERIC.contains(&literal.datatype().as_str()) {
        return None;
    }
    // It has to parse as one too: the datatype is what the query claims, and a
    // value the database cannot read as a number would be a runtime error
    // rather than an answer.
    literal
        .value()
        .parse::<f64>()
        .ok()
        .map(|_| literal.value().to_owned())
}

/// Whether any step of this path holds more than one value per record.
///
/// Asked of the slot rather than of its term descriptor: a multivalued *inlined*
/// range has no describable term (it serialises as a blank node), so a
/// descriptor-shaped question answers "no container" for `documents` — which
/// reads as single-valued and is how this multiplicity stayed invisible.
pub(crate) fn path_multiplies(
    schema_view: &SchemaView,
    class_uri: &str,
    slot_path: &[String],
) -> bool {
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

/// One projected column, resolved against the schema.
///
/// The whole of what a `BindingSpec` needs beyond its names: how the column's
/// stored text becomes an RDF term, and which of its hops hold collections.
/// Extracted so a second planner can build one without a second copy of the
/// resolution -- the refined plan's lowering has the star and the path
/// already, and reproducing this would be two derivations of one column.
pub(crate) fn binding_spec(
    schema_view: &SchemaView,
    star_var: &str,
    class_uri: &str,
    var_name: &str,
    slot_path: Vec<String>,
) -> Option<BindingSpec> {
    let (descriptor, container_modes) = resolve_column(schema_view, class_uri, &slot_path)?;
    Some(BindingSpec {
        var: var_name.to_owned(),
        star_var: star_var.to_owned(),
        slot_path,
        containers: container_modes.iter().map(Container::from_mode).collect(),
        class_uri: class_uri.to_owned(),
        descriptor,
    })
}
