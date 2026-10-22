//! **M1: a schema relation is a relation the statement can join.**
//!
//! `docs/design/sparql-schema-relations-and-row-finish.md`, *M1*. The
//! planner already turns a region that reads only the schema graph into an
//! inline table ([`PlanOp::Values`]), and a client may write one. Until this
//! module, such a table could narrow a scan (`values_becomes_filter`,
//! `values_narrow_the_joined_scan`) and nothing more: it never ran in SQL,
//! so every one of them split the SQL frontier. [`LowerConstantRelation`]
//! gives it a lowering, a constant derived table `(VALUES …) AS c{n}(…)`
//! that the statement joins, left-joins and projects.
//!
//! Nothing here knows about enums, labels or the schema graph. The rule
//! reads a `Values` node and the join above it, and whatever produced the
//! table gets the lowering.
//!
//! # The precondition: when SQL equality *is* compatibility
//!
//! A SPARQL join matches two mappings that are *compatible* (they agree by
//! `sameTerm` on every variable both bind). An SQL join matches two rows
//! whose key columns are *equal*. They agree only under conditions, and the
//! rule fires only when all of them hold ([`key_facts`] checks each and
//! names the one that failed):
//!
//! * **K1** every shared variable is bound in every solution of the other
//!   side. An unbound `?j` is compatible with every row of the table, and
//!   SQL's `NULL = v` matches none.
//! * **K2** no key cell is `UNDEF`, for the same reason from the table's
//!   side.
//! * **K3** exactly one shared variable. None is a cross product, which
//!   stays with the engine; several would need a composite key, which no
//!   query has needed.
//! * **K4** the other side's column maps stored text to terms injectively,
//!   and each cell's inverse image is computable: a record identity, an
//!   enum code, a plain or fixed-language string. Numbers and dates decline
//!   (the engine canonicalises `"01"^^xsd:integer`, stored JSON need not be
//!   canonical), and so does a reference slot whose stored form may be a
//!   CURIE. A column on an element of an inlined collection is read off the
//!   element's own row when the other side unnests that one collection
//!   exactly once; a column behind two collections, or behind one the other
//!   side does not unnest, declines.
//! * **K5** multiplicity is kept: every row of the table is rendered,
//!   duplicates included, never under `DISTINCT`.
//! * **K6** a left join carries no condition.
//! * **K7** each column is one kind of term, so it carries one descriptor.
//!
//! **Dropping a row is sound only because of K1.** A key cell whose term is
//! outside the other column's image cannot be `sameTerm`-equal to any value
//! that column holds, and the other side binds the variable in every
//! solution, so the row is compatible with nothing. That is the argument
//! `values_narrow_the_joined_scan` already relies on (#409).
//!
//! # What it claims
//!
//! The `Values` node keeps the obligations it already claims -- the
//! materialised region's triples and filters were folded into its
//! accounting when it was materialised -- and the join claims what it
//! claimed. Nothing is moved, so the ledger is untouched.

use linkml_schemaview::schemaview::SchemaView;
use spargebra::term::GroundTerm;

use crate::sparql_refine::{Executor, JoinKey, KeyTranslation, NodeId, Plan, PlanOp};
use crate::sparql_rules::Rule;
use crate::sparql_scopes::TermOf;
use crate::sparql_terms::{TermDescriptor, TermKind};

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

/// Datatypes whose lexical form the engine canonicalises on the way out:
/// `"01"^^xsd:integer` answers as `"1"`. A cell of one of these carries the
/// lexical form the query (or the materialisation) wrote, which need not be
/// the one the engine answers with, so such a column is not one a statement
/// can emit term for term. Declined (K7), rather than re-spelled here.
fn engine_canonicalises(datatype: &str) -> bool {
    let Some(local) = datatype.strip_prefix(XSD) else {
        return false;
    };
    matches!(
        local,
        "boolean"
            | "float"
            | "double"
            | "decimal"
            | "integer"
            | "nonPositiveInteger"
            | "negativeInteger"
            | "long"
            | "int"
            | "short"
            | "byte"
            | "nonNegativeInteger"
            | "unsignedLong"
            | "unsignedInt"
            | "unsignedShort"
            | "unsignedByte"
            | "positiveInteger"
            | "dateTime"
            | "dateTimeStamp"
            | "date"
            | "time"
            | "gYearMonth"
            | "gYear"
            | "gMonthDay"
            | "gDay"
            | "gMonth"
            | "duration"
            | "yearMonthDuration"
            | "dayTimeDuration"
    )
}

/// The descriptor of one cell's term.
fn descriptor_of_term(term: &GroundTerm) -> Result<TermDescriptor, String> {
    match term {
        GroundTerm::NamedNode(_) => Ok(TermDescriptor::subject_iri()),
        GroundTerm::Literal(literal) => {
            if let Some(lang) = literal.language() {
                return Ok(TermDescriptor {
                    kind: TermKind::Literal,
                    datatype: None,
                    lang: Some(lang.to_ascii_lowercase()),
                    enum_map: Vec::new(),
                    numeric: false,
                });
            }
            let datatype = literal.datatype().as_str();
            if datatype == XSD_STRING {
                return Ok(TermDescriptor {
                    kind: TermKind::Literal,
                    datatype: None,
                    lang: None,
                    enum_map: Vec::new(),
                    numeric: false,
                });
            }
            if engine_canonicalises(datatype) {
                return Err(format!(
                    "a {datatype} cell, whose lexical form the engine canonicalises"
                ));
            }
            Ok(TermDescriptor {
                kind: TermKind::Literal,
                datatype: Some(datatype.to_owned()),
                lang: None,
                enum_map: Vec::new(),
                numeric: false,
            })
        }
    }
}

/// **K7**: the one kind of term a column's non-`UNDEF` cells are. An
/// all-`UNDEF` column is a plain literal column that is `NULL` in every
/// row, which reads back as unbound -- what `UNDEF` is.
pub fn uniform_descriptor(
    rows: &[Vec<Option<GroundTerm>>],
    column: usize,
) -> Result<TermDescriptor, String> {
    let mut seen: Option<TermDescriptor> = None;
    for row in rows {
        let Some(Some(term)) = row.get(column) else {
            continue;
        };
        let descriptor = descriptor_of_term(term)?;
        match &seen {
            Some(first) if *first != descriptor => {
                return Err(format!(
                    "it holds both {} and {} terms",
                    first.shape(),
                    descriptor.shape()
                ));
            }
            Some(_) => {}
            None => seen = Some(descriptor),
        }
    }
    Ok(seen.unwrap_or(TermDescriptor {
        kind: TermKind::Literal,
        datatype: None,
        lang: None,
        enum_map: Vec::new(),
        numeric: false,
    }))
}

/// The stored text of a non-key cell: an IRI's own text, a literal's
/// lexical form. The column's descriptor turns it back into the term.
fn stored_text(term: &GroundTerm) -> String {
    match term {
        GroundTerm::NamedNode(node) => node.as_str().to_owned(),
        GroundTerm::Literal(literal) => literal.value().to_owned(),
    }
}

/// Everything the rule proved about one `Values` and the join above it, and
/// what the lowering renders from it.
#[derive(Debug, Clone)]
pub struct KeyFacts {
    /// The `Values` node.
    pub values: NodeId,
    /// The join's input the table is: the `Values` node, or the barrier
    /// directly over it.
    pub side: NodeId,
    /// The join's other input.
    pub other: NodeId,
    /// Whether the table is the join's left input (an inner join only).
    pub values_on_left: bool,
    /// The one shared variable.
    pub var: String,
    /// Its position among the table's columns.
    pub key_column: usize,
    pub translation: KeyTranslation,
    /// The other side's column for `var`: how *its* stored text becomes a
    /// term, which is how the translated key cells read back.
    pub key_descriptor: TermDescriptor,
    /// The other side's column, for a reader: the slot path, or
    /// `identity`.
    pub column_label: String,
    /// One descriptor per column of the table, in column order; the key
    /// column's is `key_descriptor`.
    pub descriptors: Vec<TermDescriptor>,
    /// The rows as stored text, after the translation: a dropped row is
    /// gone, a concept two codes mean is one row per code. `None` is
    /// `UNDEF`.
    pub rows: Vec<Vec<Option<String>>>,
    /// How many rows of the table matched nothing the other column can
    /// hold, and were dropped.
    pub dropped: usize,
}

impl KeyFacts {
    /// How many rows the constant renders.
    pub fn kept(&self) -> usize {
        self.rows.len()
    }
}

/// The table under one input of a join: the input itself, or the
/// `OPTIONAL`-body barrier (`domain: None`) directly over it -- the shape a
/// client-written `OPTIONAL { VALUES … }` plans as. The barrier is a bag
/// projection of the table, so the table's rows are the side's rows; the
/// columns the barrier does not export are simply not read.
pub fn table_under(plan: &Plan, side: NodeId) -> Option<NodeId> {
    match &plan.nodes[side].op {
        PlanOp::Values { .. } => Some(side),
        PlanOp::SubSelect {
            input,
            domain: None,
            ..
        } if matches!(plan.nodes[*input].op, PlanOp::Values { .. }) => Some(*input),
        _ => None,
    }
}

/// The join a `Values` node is joined by, and the other side, when the
/// shape is one M1 matches: an inner join with the table on either side, or
/// a left join with the table as the optional side. `Err` when the node is
/// a join of another shape. Returns `(table, table side, other side, table
/// is the left input)`.
fn join_sides(plan: &Plan, join: NodeId) -> Result<(NodeId, NodeId, NodeId, bool), String> {
    match &plan.nodes[join].op {
        PlanOp::Join {
            left,
            right,
            reference: None,
            key: None | Some(JoinKey::Value { .. }),
            ..
        } => {
            if let Some(table) = table_under(plan, *right) {
                Ok((table, *right, *left, false))
            } else if let Some(table) = table_under(plan, *left) {
                Ok((table, *left, *right, true))
            } else {
                Err("neither side is an inline table".to_owned())
            }
        }
        PlanOp::LeftJoin {
            left,
            right,
            reference: None,
            key: None | Some(JoinKey::Value { .. }),
            condition,
            ..
        } => {
            let Some(table) = table_under(plan, *right) else {
                return Err(if table_under(plan, *left).is_some() {
                    "the table is the preserved side of the left join".to_owned()
                } else {
                    "neither side is an inline table".to_owned()
                });
            };
            if condition.is_some() {
                return Err(
                    "K6: the left join carries a condition, which this lowering cannot place \
                     in the join's ON"
                        .to_owned(),
                );
            }
            Ok((table, *right, *left, false))
        }
        _ => Err("not a join M1 lowers".to_owned()),
    }
}

/// K1–K7 for the join at `join`, and the rendered table when they hold.
/// `Err` names the guard that failed, in the words the `declined` printout
/// uses.
pub fn key_facts(schema: &SchemaView, plan: &Plan, join: NodeId) -> Result<KeyFacts, String> {
    let (values, side, other, values_on_left) = join_sides(plan, join)?;
    let PlanOp::Values { variables, rows } = &plan.nodes[values].op else {
        unreachable!("join_sides returns a Values node");
    };
    let in_other = plan.variables_of(other);
    let exported = plan.variables_of(side);
    let shared: Vec<(usize, String)> = variables
        .iter()
        .enumerate()
        .filter(|(_, variable)| {
            in_other.contains(variable.as_str()) && exported.contains(variable.as_str())
        })
        .map(|(index, variable)| (index, variable.as_str().to_owned()))
        .collect();
    let (key_column, var) = match shared.as_slice() {
        [] => {
            return Err(
                "K3: the table shares no variable with n{other}, so the join is a cross product"
                    .replace("{other}", &other.to_string()),
            );
        }
        [one] => one.clone(),
        _ => {
            return Err(format!(
                "K3: the table shares {} variables with n{other}; one key column is lowered",
                shared.len()
            ));
        }
    };
    if exported.len() == 1 {
        return Err(format!(
            "the table adds no column to n{other}: a semi-join, which values_becomes_filter \
             takes"
        ));
    }
    if !plan.definitely_bound_of(other).contains(&var) {
        return Err(format!(
            "K1: ?{var} is in scope in n{other} but not bound in every solution (optional read)"
        ));
    }
    if rows
        .iter()
        .any(|row| !row.get(key_column).is_some_and(Option::is_some))
    {
        return Err(format!(
            "K2: a row leaves ?{var} UNDEF, and an UNDEF cell is compatible with every row"
        ));
    }

    // K4: the other side's column, and its inverse image.
    let term = crate::sparql_scopes::resolve_terms(plan.term_of(schema, other, &var));
    let (translation, key_descriptor, column_label) = match term {
        Some(TermOf::Identity { .. }) => (
            KeyTranslation::Identity,
            TermDescriptor::subject_iri(),
            "identity".to_owned(),
        ),
        // A record's direct type: the `asset_type` column holds the class's
        // IRI text, so the cell's own text is the key.
        Some(TermOf::DirectType { .. }) => (
            KeyTranslation::Identity,
            TermDescriptor::subject_iri(),
            "direct type".to_owned(),
        ),
        Some(TermOf::Slot {
            star_var,
            class_uri,
            path,
            reading: crate::sparql_refine::SlotReading::Column,
            ..
        }) => {
            let Some((descriptor, containers)) =
                crate::sparql_terms::resolve_column(schema, &class_uri, &path)
            else {
                return Err(format!(
                    "K4: ?{var}'s column on n{other} has no term descriptor"
                ));
            };
            let hop = element_hop(plan, other, &star_var, &path, &containers, &var)?;
            let plain = descriptor.lang.is_none()
                && descriptor
                    .datatype
                    .as_deref()
                    .is_none_or(|datatype| datatype == XSD_STRING);
            let translation = match descriptor.kind {
                TermKind::EnumIri => KeyTranslation::EnumCode,
                TermKind::Literal
                    if !descriptor.numeric
                        && (plain
                            || (descriptor.lang.is_some() && descriptor.datatype.is_none())) =>
                {
                    KeyTranslation::Lexical
                }
                _ => {
                    return Err(format!(
                        "K4: ?{var}'s column ({}) is not one whose stored text inverts to a \
                         term",
                        descriptor.shape()
                    ));
                }
            };
            let mut label = path.clone();
            if let Some(hop) = hop {
                label[hop].push_str("[each]");
            }
            (translation, descriptor, label.join("."))
        }
        Some(TermOf::Slot { .. }) => {
            return Err(format!(
                "K4: ?{var} is an unnested element on n{other}, not a column"
            ));
        }
        _ => {
            return Err(format!("K4: ?{var} is not one column on n{other}"));
        }
    };

    // K7: every other column is one kind of term.
    let mut descriptors: Vec<TermDescriptor> = Vec::with_capacity(variables.len());
    for (index, variable) in variables.iter().enumerate() {
        if index == key_column {
            descriptors.push(key_descriptor.clone());
            continue;
        }
        match uniform_descriptor(rows, index) {
            Ok(descriptor) => descriptors.push(descriptor),
            Err(why) => return Err(format!("K7: column {variable} is not uniform: {why}")),
        }
    }

    // The translation, row by row (K5: every row, duplicates included).
    let mut rendered: Vec<Vec<Option<String>>> = Vec::with_capacity(rows.len());
    let mut dropped = 0usize;
    for row in rows {
        let Some(Some(cell)) = row.get(key_column) else {
            unreachable!("K2 checked above");
        };
        let keys: Vec<String> = match (translation, cell) {
            (KeyTranslation::Identity, GroundTerm::NamedNode(node)) => {
                vec![node.as_str().to_owned()]
            }
            (KeyTranslation::Identity, GroundTerm::Literal(_)) => Vec::new(),
            (KeyTranslation::EnumCode, GroundTerm::NamedNode(node)) => key_descriptor
                .enum_map
                .iter()
                .filter(|(_, iri)| iri == node.as_str())
                .map(|(code, _)| code.clone())
                .collect(),
            (KeyTranslation::EnumCode, GroundTerm::Literal(_)) => {
                return Err(format!(
                    "K4: a literal key cell against the enum column of ?{var}: a stored code \
                     the enum does not permit reads as a literal, so it could match"
                ));
            }
            (KeyTranslation::Lexical, GroundTerm::Literal(literal)) => {
                let matches = match &key_descriptor.lang {
                    Some(lang) => literal
                        .language()
                        .is_some_and(|tag| tag.eq_ignore_ascii_case(lang)),
                    None => {
                        literal.language().is_none() && literal.datatype().as_str() == XSD_STRING
                    }
                };
                if matches {
                    vec![literal.value().to_owned()]
                } else {
                    Vec::new()
                }
            }
            (KeyTranslation::Lexical, GroundTerm::NamedNode(_)) => Vec::new(),
        };
        if keys.is_empty() {
            dropped += 1;
            continue;
        }
        for key in keys {
            rendered.push(
                row.iter()
                    .enumerate()
                    .map(|(index, cell)| {
                        if index == key_column {
                            Some(key.clone())
                        } else {
                            cell.as_ref().map(stored_text)
                        }
                    })
                    .collect(),
            );
        }
    }

    Ok(KeyFacts {
        values,
        side,
        other,
        values_on_left,
        var,
        key_column,
        translation,
        key_descriptor,
        column_label,
        descriptors,
        rows: rendered,
        dropped,
    })
}

/// Where the column of `path` crosses a collection, when it does: the index of
/// the one hop through a collection that `other` unnests, exactly once, as the
/// element the statement reads the column off (#440). `Ok(None)` for a column
/// on the record itself. Anything else -- two collections, a collection
/// `other` does not unnest, one it unnests twice, a collection that is the
/// column itself -- is refused: the statement would read the column off the
/// record, where a collection is not a value.
fn element_hop(
    plan: &Plan,
    other: NodeId,
    star_var: &str,
    path: &[String],
    containers: &[linkml_schemaview::slotview::SlotContainerMode],
    var: &str,
) -> Result<Option<usize>, String> {
    use linkml_schemaview::slotview::SlotContainerMode::SingleValue;
    let hops: Vec<usize> = containers
        .iter()
        .enumerate()
        .filter(|(_, container)| **container != SingleValue)
        .map(|(index, _)| index)
        .collect();
    let hop = match hops.as_slice() {
        [] => return Ok(None),
        [one] => *one,
        many => {
            return Err(format!(
                "K4: ?{var} is read through {} collections on n{other}",
                many.len()
            ));
        }
    };
    let unnests = plan
        .nodes
        .iter()
        .enumerate()
        .filter(|(id, node)| {
            node.executor == Executor::Sql
                && plan.feeds_visibly(*id, other)
                && matches!(&node.op, PlanOp::Unnest { star_var: s, slot_path, .. }
                    if s == star_var && slot_path.as_slice() == &path[..=hop])
        })
        .count();
    if hop + 1 >= path.len() || unnests != 1 {
        return Err(format!(
            "K4: ?{var} is read through a collection n{other} does not unnest (once)"
        ));
    }
    Ok(Some(hop))
}

/// The alias a lowered table renders under: `c{n}`, numbered by the
/// table's position among the plan's SQL tables. A function of the plan, so
/// the printout and the lowering name one table alike.
pub fn constant_alias(plan: &Plan, values: NodeId) -> String {
    let rank = (0..values)
        .filter(|id| {
            matches!(plan.nodes[*id].op, PlanOp::Values { .. })
                && plan.nodes[*id].executor == Executor::Sql
        })
        .count();
    format!("c{}", rank + 1)
}

/// The join a lowered `Values` is keyed by, when it is one.
pub fn keyed_consumer(plan: &Plan, values: NodeId) -> Option<NodeId> {
    plan.nodes.iter().position(|node| {
        matches!(&node.op,
            PlanOp::Join { left, right, key: Some(JoinKey::Value { .. }), .. }
            | PlanOp::LeftJoin { left, right, key: Some(JoinKey::Value { .. }), .. }
                if table_under(plan, *left) == Some(values)
                    || table_under(plan, *right) == Some(values))
    })
}

/// The printout's note on a lowered table: its alias, its key and what the
/// translation kept -- `lowered as c1, key ?t: enum→code, 1 kept, 2 outside
/// enum`. `None` for anything else.
pub fn lowered_note(plan: &Plan, id: NodeId) -> Option<String> {
    if !matches!(plan.nodes[id].op, PlanOp::Values { .. })
        || plan.nodes[id].executor != Executor::Sql
    {
        return None;
    }
    let join = keyed_consumer(plan, id)?;
    let (PlanOp::Join {
        key:
            Some(JoinKey::Value {
                var,
                translation,
                kept,
                dropped,
                ..
            }),
        ..
    }
    | PlanOp::LeftJoin {
        key:
            Some(JoinKey::Value {
                var,
                translation,
                kept,
                dropped,
                ..
            }),
        ..
    }) = &plan.nodes[join].op
    else {
        return None;
    };
    let mut note = format!(
        "lowered as {}, key ?{var}: {}, {kept} kept",
        constant_alias(plan, id),
        translation.as_str()
    );
    if *dropped > 0 {
        note.push_str(&format!(", {dropped} {}", translation.outside()));
    }
    Some(note)
}

/// The rule: an inline table whose join meets K1–K7 runs in SQL, keyed by
/// value.
///
/// **Match.** A `Values` node `[E]` whose one consumer is an `[E]` `Join` or
/// `LeftJoin` with no key and no reference, whose other input is `[S]`.
///
/// **Edit.** Flip the `Values` and the join to `[S]`; record
/// [`JoinKey::Value`] with the translation [`key_facts`] proved.
///
/// **Equivalence.** Under K1 and K2 every shared variable is bound on both
/// sides of every candidate pair, so compatibility is agreement on that
/// variable by `sameTerm`; under K4 the other column's stored text maps to
/// terms injectively and each key cell was translated into exactly the
/// texts that map to its term, so `sameTerm` is SQL equality of the texts.
/// Dropped rows are compatible with nothing (K1). K5 keeps the bag, K6
/// keeps a left join's `ON` empty, K7 lets each column carry one
/// descriptor. The merged mapping takes the shared variable from either
/// side (they are `sameTerm`-equal) and every other column from the table.
///
/// **Caught by** [`Plan::join_keys_agree`], which re-derives the facts.
pub struct LowerConstantRelation<'s> {
    schema: &'s SchemaView,
}

impl<'s> LowerConstantRelation<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self { schema }
    }

    /// The join a `Values` node is matched by, and its other input: the
    /// table's one consumer, an engine join with no key, over an SQL side.
    /// `None` when the match itself fails -- that is not a decline.
    fn matched(plan: &Plan, values: NodeId) -> Option<NodeId> {
        if plan.nodes[values].executor != Executor::Engine
            || !matches!(plan.nodes[values].op, PlanOp::Values { .. })
        {
            return None;
        }
        let consumers = crate::sparql_rules::consumers_of(plan, values);
        let [consumer] = consumers.as_slice() else {
            return None;
        };
        // Through an `OPTIONAL`-body barrier directly over the table.
        let (side, join) = match &plan.nodes[*consumer].op {
            PlanOp::SubSelect { domain: None, .. } => {
                let above = crate::sparql_rules::consumers_of(plan, *consumer);
                let [join] = above.as_slice() else {
                    return None;
                };
                if plan.nodes[*consumer].executor != Executor::Engine {
                    return None;
                }
                (*consumer, *join)
            }
            _ => (values, *consumer),
        };
        if plan.nodes[join].executor != Executor::Engine {
            return None;
        }
        let other = match &plan.nodes[join].op {
            PlanOp::Join {
                left,
                right,
                reference: None,
                key: None,
                ..
            }
            | PlanOp::LeftJoin {
                left,
                right,
                reference: None,
                key: None,
                ..
            } => {
                if *left == side {
                    *right
                } else {
                    *left
                }
            }
            _ => return None,
        };
        (plan.nodes[other].executor == Executor::Sql).then_some(join)
    }

    /// Every table the rule matched and a guard declined, with the guard:
    /// the `declined` section of a refined printout. Computed from the plan
    /// as it stands, so it never reports a decline a later rule undid.
    pub fn declined(&self, plan: &Plan) -> Vec<(NodeId, String)> {
        (0..plan.nodes.len())
            .filter_map(|values| {
                let join = Self::matched(plan, values)?;
                key_facts(self.schema, plan, join)
                    .err()
                    .map(|why| (values, why))
            })
            .collect()
    }
}

impl Rule for LowerConstantRelation<'_> {
    fn name(&self) -> &'static str {
        "lower_constant_relation"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for values in 0..plan.nodes.len() {
            let Some(join) = Self::matched(plan, values) else {
                continue;
            };
            let Ok(facts) = key_facts(self.schema, plan, join) else {
                continue;
            };
            plan.nodes[values].executor = Executor::Sql;
            plan.nodes[facts.side].executor = Executor::Sql;
            plan.nodes[join].executor = Executor::Sql;
            let key = JoinKey::Value {
                var: facts.var.clone(),
                translation: facts.translation,
                column: facts.column_label.clone(),
                kept: facts.kept(),
                dropped: facts.dropped,
            };
            match &mut plan.nodes[join].op {
                PlanOp::Join { key: slot, .. } | PlanOp::LeftJoin { key: slot, .. } => {
                    *slot = Some(key);
                }
                _ => unreachable!("matched a join"),
            }
            return true;
        }
        false
    }
}

#[cfg(all(test, feature = "sparql-endpoint"))]
pub(crate) mod tests {
    use linkml_meta::SchemaDefinition;
    use linkml_schemaview::schemaview::SchemaView;
    use std::path::Path;

    use crate::sparql_algebra::equivalence::each_rewrite_preserves_answers;
    use crate::sparql_oracle::{Oracle, probe};
    use crate::sparql_refine::{Executor, Plan, PlanOp};
    use crate::sparql_rules::{Rule, tier_one_rules};
    use crate::sparql_scoper::tests::{test_schema_view, test_schema_view_plus};

    const SCHEMA_GRAPH: &str = "https://data.infrabel.be/asset360/schema";
    const SIGNAL: &str = "https://data.infrabel.be/asset360/Signal";

    pub(crate) fn asset360_schema_view() -> SchemaView {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
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

    fn asset360_query(body: &str) -> String {
        format!(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \
             PREFIX skos: <http://www.w3.org/2004/02/skos/core#> {body}"
        )
    }

    /// The fixture schema's `SignalKind` concept IRIs, by code.
    pub(crate) fn kind_iri(code: &str) -> String {
        let schema = test_schema_view();
        let (descriptor, _) =
            crate::sparql_terms::resolve_column(&schema, SIGNAL, &["kind".to_owned()]).unwrap();
        descriptor
            .enum_map
            .iter()
            .find(|(stored, _)| stored == code)
            .map(|(_, iri)| iri.clone())
            .unwrap()
    }

    /// Signals with an enum-valued `kind`: two sharing a name, one without
    /// a kind, one whose kind no table below lists.
    pub(crate) fn kind_oracle(schema: &SchemaView) -> Oracle {
        Oracle::new(
            schema,
            &[
                (
                    "Signal",
                    r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/A", "name": "Alpha", "kind": "GSA", "length": 3}"#,
                ),
                (
                    "Signal",
                    r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/B", "name": "Alpha", "kind": "KSS", "length": 5}"#,
                ),
                (
                    "Signal",
                    r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/C", "length": 7}"#,
                ),
                (
                    "Signal",
                    r#"{"asset360_uri": "https://data.infrabel.be/asset360/signal/D", "name": "Delta", "kind": "REP_H_D", "length": 1}"#,
                ),
            ],
        )
    }

    fn rules(schema: &SchemaView) -> Vec<Box<dyn Rule + '_>> {
        tier_one_rules(schema, crate::sparql_dataset::default_dataset())
    }

    fn refined_text(query: &str, schema: &SchemaView) -> String {
        let text = format!("PREFIX asset360: <https://data.infrabel.be/asset360/> {query}");
        let mut plan = crate::sparql_refine::naive_plan(
            &crate::sparql_scoper::parse_query(&text, Some(schema)).unwrap(),
            crate::sparql_dataset::default_dataset(),
        )
        .unwrap();
        let rules = rules(schema);
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        crate::sparql_rules::refine(&mut plan, &borrowed).unwrap();
        crate::sparql_plan::with_declined(&plan, schema, crate::sparql_dataset::default_dataset())
    }

    fn lowered_tables(plan: &Plan) -> usize {
        plan.nodes
            .iter()
            .filter(|node| {
                matches!(node.op, PlanOp::Values { .. }) && node.executor == Executor::Sql
            })
            .count()
    }

    /// P2 of the design: the nested label lookup of #494 is one statement.
    #[test]
    fn p2_a_nested_label_lookup_is_one_statement() {
        let sv = asset360_schema_view();
        let q = asset360_query(&format!(
            "SELECT ?s ?name ?typeNl WHERE {{ ?s a asset360:Signal ; asset360:NationalUniqueID ?name . \
             OPTIONAL {{ ?s asset360:signalType ?t . \
             OPTIONAL {{ GRAPH <{SCHEMA_GRAPH}> {{ ?t skos:prefLabel ?typeNl FILTER(lang(?typeNl) = \"nl-be\") }} }} }} }}"
        ));
        let refined = crate::sparql_plan::plan_texts_of(
            &q,
            &sv,
            &crate::sparql_dataset::schema_dataset(SCHEMA_GRAPH),
        )
        .1
        .unwrap();
        assert!(
            refined.contains("lowered as c1, key ?t: enum→code, 1 kept, 2 outside enum [S]"),
            "{refined}"
        );
        assert!(
            refined.contains("by value ?t (signalType, enum→code)  [S]"),
            "{refined}"
        );
        let plan = crate::sparql_plan::plan_of(
            &q,
            &sv,
            &crate::sparql_dataset::schema_dataset(SCHEMA_GRAPH),
        )
        .unwrap();
        let printed = plan.to_string();
        assert_eq!(plan.refinement.as_str(), "used_alone", "{printed}");
        assert!(
            printed.contains(
                "constant  c1 (t, typeNl) × 1   t: enum→code of signalType, typeNl: literal@nl-be"
            ),
            "{printed}"
        );
        assert!(
            printed.contains("join      value s.signalType = c1.t   left"),
            "{printed}"
        );
        assert!(
            printed.contains("relation  q0 [?s:identity ?typeNl:constant]"),
            "{printed}"
        );
    }

    /// P3 of the design: the label joined where its key may be unbound is
    /// declined, and the printout says which guard stopped it. (Joined, not
    /// left-joined: a sibling `OPTIONAL` on a maybe-unbound key is refused by
    /// the scoper before any plan exists -- #460 -- so there is no plan to
    /// print for it.)
    #[test]
    fn p3_an_optional_key_declines_on_k1() {
        let sv = asset360_schema_view();
        let q = asset360_query(&format!(
            "SELECT ?s ?typeNl WHERE {{ ?s a asset360:Signal . \
             OPTIONAL {{ ?s asset360:signalType ?t }} \
             GRAPH <{SCHEMA_GRAPH}> {{ ?t skos:prefLabel ?typeNl FILTER(lang(?typeNl) = \"nl-be\") }} }}"
        ));
        let refined = crate::sparql_plan::plan_texts_of(
            &q,
            &sv,
            &crate::sparql_dataset::schema_dataset(SCHEMA_GRAPH),
        )
        .1
        .unwrap();
        assert!(refined.contains("  declined\n"), "{refined}");
        assert!(
            refined.contains(
                "lower_constant_relation  n2  K1: ?t is in scope in n1 but not bound in every \
                 solution (optional read)"
            ),
            "{refined}"
        );
    }

    /// Every shape M1 admits, one rewrite at a time against the oracle
    /// (test 2(d)): an enum key on an inner join with a row outside the
    /// enum, the #494 nesting as a client-written table, a string key with a
    /// duplicate row and a cell of another kind, an identity key with a
    /// literal cell. Each lowers, and each translation agrees with the
    /// engine.
    #[test]
    fn m1_fires_and_every_rewrite_preserves_the_answer() {
        let schema = test_schema_view();
        let oracle = kind_oracle(&schema);
        let (gsa, kss) = (kind_iri("GSA"), kind_iri("KSS"));
        let rules = rules(&schema);
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        for query in [
            format!(
                "SELECT ?s ?lbl WHERE {{ ?s a asset360:Signal ; asset360:kind ?k . \
                 VALUES (?k ?lbl) {{ (<{gsa}> \"gsa\") (<{kss}> \"kss\") (<urn:outside> \"none\") }} }}"
            ),
            format!(
                "SELECT ?s ?lbl WHERE {{ ?s a asset360:Signal . OPTIONAL {{ ?s asset360:kind ?k . \
                 OPTIONAL {{ VALUES (?k ?lbl) {{ (<{gsa}> \"gsa\") (<urn:outside> \"none\") }} }} }} }}"
            ),
            "SELECT ?s ?lbl WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             OPTIONAL { VALUES (?nm ?lbl) { (\"Alpha\" \"a\") (\"Alpha\" \"a2\") (\"Delta\"@en \"x\") } } }"
                .to_owned(),
            "SELECT ?s ?lbl WHERE { ?s a asset360:Signal . \
             VALUES (?s ?lbl) { (<https://data.infrabel.be/asset360/signal/A> \"a\") (\"A\" \"lit\") } }"
                .to_owned(),
        ] {
            let plan = each_rewrite_preserves_answers(&query, &schema, &oracle, &borrowed);
            assert_eq!(lowered_tables(&plan), 1, "{query}\n{plan}");
            crate::sparql_ops::lower_refined(&plan, &schema, None, None)
                .unwrap_or_else(|refusal| panic!("{refusal}\n{query}\n{plan}"));
        }
    }

    /// The key of a lowered table read through a structure, inside an
    /// `OPTIONAL` body: nothing but the scan's own premise says the record
    /// holds the value -- the left join to the table keeps the row either
    /// way -- so the answering statement states it as a presence check (the
    /// consolidator's guard refused the #494 superStructure material label
    /// for its absence, and bypassing the guard answered a year where the
    /// engine answers none).
    #[test]
    fn a_nested_key_is_a_stated_premise() {
        let schema = test_schema_view();
        let query = "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s ?lat ?lbl WHERE { ?s a asset360:Signal . \
             OPTIONAL { ?s asset360:location ?loc . ?loc asset360:latitude ?lat ; asset360:detail ?d . \
             ?d asset360:value ?v . OPTIONAL { VALUES (?v ?lbl) { (\"x\" \"X\") } } } }";
        let plan =
            crate::sparql_plan::plan_of(query, &schema, crate::sparql_dataset::default_dataset())
                .unwrap();
        assert_eq!(plan.refinement.as_str(), "used_alone", "{plan}");
        let printed = plan.to_string();
        assert!(
            printed.contains("present   location.detail.value"),
            "{printed}"
        );
    }

    /// **`m1_unbound_key_joins_every_row`** (design appendix): K1. The key
    /// is an optional read, so an unbound `?k` is compatible with every row
    /// of the table, and SQL's `NULL = v` matches none.
    #[test]
    fn m1_unbound_key_joins_every_row() {
        let schema = test_schema_view();
        let refined = refined_text(
            "SELECT ?s ?lbl WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:kind ?k } \
             OPTIONAL { VALUES (?k ?lbl) { (<urn:outside-enum> \"label\") } } }",
            &schema,
        );
        assert!(refined.contains("K1: ?k is in scope in"), "{refined}");
        assert!(!refined.contains("lowered as"), "{refined}");
        // The rewrite K1 refuses answers differently.
        let data = "<urn:s1> <urn:type> <urn:A> .";
        let original = probe(
            data,
            "SELECT * { ?s <urn:type> <urn:A> OPTIONAL { ?s <urn:j> ?j } \
             OPTIONAL { VALUES (?j ?label) { (<urn:outside-enum> \"label\") } } }",
        );
        let pruned = probe(
            data,
            "SELECT * { ?s <urn:type> <urn:A> OPTIONAL { ?s <urn:j> ?j } \
             OPTIONAL { VALUES (?j ?label) { } } }",
        );
        assert_ne!(original, pruned);
    }

    /// **`m1_undef_key_cell`** (design appendix): K2. An `UNDEF` key cell is
    /// compatible with every row; SQL's `NULL` matches none.
    #[test]
    fn m1_undef_key_cell() {
        let schema = test_schema_view();
        let refined = refined_text(
            "SELECT ?s ?lbl WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { VALUES (?k ?lbl) { (UNDEF \"any\") } } }",
            &schema,
        );
        assert!(refined.contains("K2: a row leaves ?k UNDEF"), "{refined}");
        let data = "<urn:s1> <urn:j> <urn:E1> .";
        let original = probe(
            data,
            "SELECT * { ?s <urn:j> ?j OPTIONAL { VALUES (?j ?label) { (UNDEF \"any\") } } }",
        );
        // SQL equality on a NULL key: the key never matches, so the row is
        // kept unextended.
        let sql_semantics = probe(data, "SELECT * { ?s <urn:j> ?j }");
        assert_ne!(original, sql_semantics);
    }

    /// The other guards, each named in the printout: a condition on the
    /// left join (K6), a column of two kinds of term (K7), and a numeric key
    /// whose stored text need not be the engine's canonical form (K4).
    #[test]
    fn the_other_guards_decline_by_name() {
        let schema = test_schema_view();
        let gsa = kind_iri("GSA");
        for (query, guard) in [
            (
                format!(
                    "SELECT ?s ?lbl WHERE {{ ?s a asset360:Signal ; asset360:kind ?k . \
                     VALUES (?k ?lbl) {{ (<{gsa}> \"gsa\") (<{gsa}> <urn:iri>) }} }}"
                ),
                "K7: column ?lbl is not uniform",
            ),
            (
                "SELECT ?s ?lbl WHERE { ?s a asset360:Signal ; asset360:length ?len . \
                 VALUES (?len ?lbl) { (3 \"three\") } }"
                    .to_owned(),
                "K4: ?len's column",
            ),
        ] {
            let refined = refined_text(&query, &schema);
            assert!(refined.contains(guard), "{guard}\n{refined}");
            assert!(!refined.contains("lowered as"), "{refined}");
        }

        // K6, on a left join given a condition by hand: spargebra keeps a
        // `FILTER` written beside a `VALUES` inside the body, and op 2 sinks
        // one that reads the body alone, so no query text reaches this shape
        // today -- and the guard still has to hold.
        let text = format!(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> \
             SELECT ?s ?lbl WHERE {{ ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL {{ VALUES (?k ?lbl) {{ (<{gsa}> \"gsa\") }} }} }}"
        );
        let mut plan = crate::sparql_refine::naive_plan(
            &crate::sparql_scoper::parse_query(&text, Some(&schema)).unwrap(),
            crate::sparql_dataset::default_dataset(),
        )
        .unwrap();
        let rules = rules(&schema);
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        let leftjoin = plan.find("leftjoin")[0];
        if let PlanOp::LeftJoin { condition, .. } = &mut plan.nodes[leftjoin].op {
            // One that reads the preserved side, so op 2 cannot sink it into
            // the body.
            *condition = Some(crate::sparql_refine::Expr::Var("s".to_owned()));
        }
        crate::sparql_rules::refine(&mut plan, &borrowed).unwrap();
        let refined = crate::sparql_plan::with_declined(
            &plan,
            &schema,
            crate::sparql_dataset::default_dataset(),
        );
        assert!(
            refined.contains("K6: the left join carries a condition"),
            "{refined}"
        );
        assert_eq!(lowered_tables(&plan), 0, "{refined}");
    }
    /// A holder of inlined plates, once as a list and once as a keyed
    /// mapping, each plate holding an enum: what asset360's
    /// `hasStructureElement.hasMaterial` is in miniature (#440).
    const PLATE_CLASSES: &str = r#"
  Plate:
    class_uri: asset360:Plate
    attributes:
      plateId:
        key: true
      plateKind:
        range: SignalKind
      plateKinds:
        range: SignalKind
        multivalued: true
      side:
        range: string
      nested:
        range: Plate
        multivalued: true
        inlined: true
  Rack:
    class_uri: asset360:Rack
    attributes:
      asset360_uri:
        identifier: true
      plates:
        range: Plate
        multivalued: true
        inlined: true
        inlined_as_list: true
      slots:
        range: Plate
        multivalued: true
        inlined: true
        inlined_as_list: false
"#;

    fn plate_query(holder_slot: &str, labels: &str) -> String {
        asset360_query(&format!(
            "SELECT ?r ?side ?kind ?nl ?fr WHERE {{ ?r a asset360:Rack . \
             OPTIONAL {{ ?r asset360:{holder_slot} ?p . ?p asset360:side ?side ; asset360:plateKind ?kind . \
             {labels} }} }}"
        ))
    }

    fn label_block(var: &str, gsa: &str, kss: &str) -> String {
        format!(
            "OPTIONAL {{ VALUES (?kind ?{var}) {{ (<{gsa}> \"{var}-gsa\") (<{kss}> \"{var}-kss\") \
             (<urn:outside> \"none\") }} }}"
        )
    }

    /// #440: a label lookup on an enum held by an element of an inlined
    /// collection (list or keyed mapping) is read off the element's own row,
    /// inside the element's relation.
    #[test]
    fn an_element_enum_label_is_one_statement() {
        let sv = test_schema_view_plus(PLATE_CLASSES);
        for slot in ["plates", "slots"] {
            let (gsa, kss) = (kind_iri("GSA"), kind_iri("KSS"));
            let q = plate_query(
                slot,
                &format!(
                    "{} {}",
                    label_block("nl", &gsa, &kss),
                    label_block("fr", &gsa, &kss)
                ),
            );
            let dataset = crate::sparql_dataset::default_dataset();
            let refined = crate::sparql_plan::plan_texts_of(&q, &sv, dataset)
                .1
                .unwrap();
            assert!(
                refined.contains("key ?kind: enum→code"),
                "{slot}\n{refined}"
            );
            assert!(!refined.contains("declined"), "{slot}\n{refined}");
            assert!(
                refined.contains(&format!("({slot}[each].plateKind, enum→code)")),
                "{slot}\n{refined}"
            );
            let plan = crate::sparql_plan::plan_of(&q, &sv, dataset).unwrap();
            let printed = plan.to_string();
            assert_eq!(plan.refinement.as_str(), "used_alone", "{slot}\n{printed}");
            assert!(
                printed.contains(&format!(
                    "join      value r.{slot}[each].plateKind = c1.kind   left"
                )),
                "{slot}\n{printed}"
            );
            assert!(
                printed.contains(&format!(
                    "join      value r.{slot}[each].plateKind = c2.kind   left"
                )),
                "{slot}\n{printed}"
            );
        }
    }
    /// Racks for the equivalence runs: no plates; a plate without a kind;
    /// two plates of one code; a code outside the table; a null kind; and
    /// the same plates as a keyed mapping.
    fn rack_oracle(schema: &SchemaView) -> Oracle {
        let base = "https://data.infrabel.be/asset360/rack";
        let records: Vec<(&str, String)> = [
            ("R0", r#""plates": [], "slots": {}"#),
            (
                "R1",
                r#""plates": [{"plateId": "a", "side": "left", "plateKind": "GSA"},
                              {"plateId": "b", "side": "right"},
                              {"plateId": "c", "side": "mid", "plateKind": "GSA"}],
                   "slots": {"a": {"side": "left", "plateKind": "GSA"},
                             "b": {"side": "right"},
                             "c": {"side": "mid", "plateKind": "GSA"}}"#,
            ),
            (
                "R2",
                r#""plates": [{"plateId": "d", "side": "x", "plateKind": "KSS"},
                              {"plateId": "e", "side": "y", "plateKind": "REP_H_D"},
                              {"plateId": "f", "side": "z", "plateKind": null}],
                   "slots": {"d": {"side": "x", "plateKind": "KSS"},
                             "e": {"side": "y", "plateKind": "REP_H_D"},
                             "f": {"side": "z", "plateKind": null}}"#,
            ),
            ("R3", r#""side": "none""#),
        ]
        .iter()
        .map(|(id, body)| {
            (
                "Rack",
                format!(r#"{{"asset360_uri": "{base}/{id}", {body}}}"#),
            )
        })
        .collect();
        let borrowed: Vec<(&str, &str)> = records.iter().map(|(c, j)| (*c, j.as_str())).collect();
        Oracle::new(schema, &borrowed)
    }

    /// #440 against the engine: every rewrite of an element-enum label keeps
    /// the answers -- siblings, a single label, a label outside any
    /// `OPTIONAL`, a required one -- on a list and on a keyed mapping, over
    /// holders with no plates, plates without the enum, repeated codes, a
    /// code outside the table, a null.
    #[test]
    fn an_element_enum_label_preserves_the_answer() {
        let schema = test_schema_view_plus(PLATE_CLASSES);
        let oracle = rack_oracle(&schema);
        let (gsa, kss) = (kind_iri("GSA"), kind_iri("KSS"));
        let rules = rules(&schema);
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        for slot in ["plates", "slots"] {
            let table = |var: &str| {
                format!(
                    "VALUES (?kind ?{var}) {{ (<{gsa}> \"{var}-gsa\") (<{gsa}> \"{var}-gsa2\") \
                     (<{kss}> \"{var}-kss\") (<urn:outside> \"none\") }}"
                )
            };
            for query in [
                // siblings under the element's OPTIONAL
                format!(
                    "SELECT ?r ?side ?kind ?nl ?fr WHERE {{ ?r a asset360:Rack . \
                     OPTIONAL {{ ?r asset360:{slot} ?p . ?p asset360:side ?side ; asset360:plateKind ?kind . \
                     OPTIONAL {{ {} }} OPTIONAL {{ {} }} }} }}",
                    table("nl"),
                    table("fr")
                ),
                // one label
                format!(
                    "SELECT ?r ?side ?nl WHERE {{ ?r a asset360:Rack . \
                     OPTIONAL {{ ?r asset360:{slot} ?p . ?p asset360:side ?side ; asset360:plateKind ?kind . \
                     OPTIONAL {{ {} }} }} }}",
                    table("nl")
                ),
                // the element read outside any OPTIONAL
                format!(
                    "SELECT ?r ?side ?nl WHERE {{ ?r a asset360:Rack ; asset360:{slot} ?p . \
                     ?p asset360:side ?side ; asset360:plateKind ?kind . OPTIONAL {{ {} }} }}",
                    table("nl")
                ),
                // a required label
                format!(
                    "SELECT ?r ?side ?nl WHERE {{ ?r a asset360:Rack ; asset360:{slot} ?p . \
                     ?p asset360:side ?side ; asset360:plateKind ?kind . {} }}",
                    table("nl")
                ),
            ] {
                let plan = each_rewrite_preserves_answers(&query, &schema, &oracle, &borrowed);
                assert!(lowered_tables(&plan) >= 1, "{query}\n{plan}");
                crate::sparql_ops::lower_refined(&plan, &schema, None, None)
                    .unwrap_or_else(|refusal| panic!("{refusal}\n{query}\n{plan}"));
            }
        }
    }

    /// What #440 still refuses, each by its own words: a multivalued enum on
    /// the element, and a key read through two collections.
    #[test]
    fn an_element_enum_label_refuses_what_it_cannot_place() {
        let schema = test_schema_view_plus(PLATE_CLASSES);
        let gsa = kind_iri("GSA");
        for (query, why) in [
            (
                format!(
                    "SELECT ?r ?nl WHERE {{ ?r a asset360:Rack ; asset360:plates ?p . ?p asset360:plateKinds ?kind . \
                     OPTIONAL {{ VALUES (?kind ?nl) {{ (<{gsa}> \"x\") }} }} }}"
                ),
                "K4: ?kind is an unnested element",
            ),
            (
                format!(
                    "SELECT ?r ?nl WHERE {{ ?r a asset360:Rack ; asset360:plates ?p . ?p asset360:nested ?q . \
                     ?q asset360:plateKind ?kind . \
                     OPTIONAL {{ VALUES (?kind ?nl) {{ (<{gsa}> \"x\") }} }} }}"
                ),
                "K4: ?kind is read through 2 collections",
            ),
        ] {
            let refined = refined_text(&query, &schema);
            assert!(refined.contains(why), "{why}\n{refined}");
            assert!(!refined.contains("lowered as"), "{refined}");
        }
    }
}
