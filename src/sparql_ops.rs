//! The SQL leaf as operators, so a rewrite can be a function on plans.
//!
//! A [`crate::sparql_plan::SqlPass`] today carries two composite structures --
//! the star decomposition and, when the pass groups, the whole solution spec.
//! Both are shaped for *rendering*: which classes to scan, which slots to
//! compare, what to select. That is enough to run a plan and not enough to
//! rewrite one. Pushing a filter from the engine into SQL means reaching into
//! a `HashMap<slot, Vec<FilterCondition>>`, moving an obligation id, and
//! remembering to recompute the engine pass's causes so they do not lie. Every
//! rule would re-derive the same surgery, and nothing would check the result.
//!
//! An operator tree makes each of those a local edit on a node, and the
//! ledger -- every obligation discharged exactly once -- becomes the
//! postcondition every rule must preserve. A rule that forgets to move an id,
//! or claims one twice, then fails a check instead of answering a different
//! question quietly.
//!
//! # Enforcing versus narrowing
//!
//! One distinction has to be in the data rather than in a convention, because
//! it is exactly what a pushdown rule needs to know.
//!
//! When the engine finishes a query, SQL may still apply a filter to *narrow*
//! the rows it returns while the engine remains authoritative for the answer --
//! SPARQL compares RDF terms, and a `->>` comparison on stored text is not
//! always the same test. Such a filter is real work but not a claim: the
//! obligation stays with the engine. When SQL answers alone, the same filter
//! *is* the answer and the obligation is its own.
//!
//! Today that difference lives in which pass claims the obligation, and can
//! only be inferred by comparing two structures. [`Enforcement`] states it, so
//! a rule can ask a node directly whether removing it would change the answer.

use std::collections::BTreeMap;
use std::fmt;

use crate::sparql_plan::ObligationId;
use crate::sparql_pushdown::{BindingSpec, HavingTerm, MeasureSpec, OrderTerm};
use crate::sparql_scoper::{FilterCondition, JoinType};

/// Index into an [`OpTree`]'s node list. Nodes refer to their inputs by index,
/// so a tree is a flat vector and a rewrite is an edit to it rather than a
/// rebuild of a nest of boxes.
pub type OpId = usize;

/// Whether an operator's result is the answer, or only a narrowing of what the
/// next pass will decide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Enforcement {
    /// The operator decides the obligation. Removing it changes the answer.
    Enforces,
    /// The operator only reduces rows; something later decides. Removing it
    /// leaves the answer intact and the query slower.
    ///
    /// The case this exists for: a comparison SQL can express approximately,
    /// where the engine still applies SPARQL's term semantics afterwards.
    Narrows,
}

/// How to read the value a `(star, slot_path)` address names.
///
/// The fact 28d's `Slot` was missing, and the reason a rule could not push a
/// condition on a multivalued slot at all: one address means three different
/// things, and they select different rows.
///
/// The lesson is stage 1's, with a second instance. [`crate::sparql_refine::ScanSlot`] carries
/// `multivalued` because a plan whose scan slots are bare names cannot see a
/// cardinality error; an address with no reading cannot see this one. A
/// consumer handed `(?s, [trafficKinds], = 'm')` and nothing else has to guess
/// between an equality that matches no array, a containment test over the
/// array, and a comparison against one unnested element -- and two of those
/// answer a different question than the query asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotReading {
    /// The column's own value: `object_data->>'name'`. Only correct for a
    /// single-valued slot -- on an array it compares the array's text and
    /// matches nothing.
    Column,
    /// Some element of the array the column holds. `?s :trafficKinds "m"` asks
    /// whether the record carries that triple at all, which is a containment
    /// test -- `EXISTS (SELECT 1 FROM jsonb_array_elements_text(...))`, what
    /// the star decomposition's `multivalued_fields` tells its renderer to do
    /// -- and matches a record once however many values it holds.
    AnyElement,
    /// The element a [`Op::Unnest`] below bound to a variable. One row per
    /// value, so a condition on it selects *rows* rather than records, which
    /// is what SPARQL means by `?s :trafficKinds ?k . FILTER(?k = "m")`: one
    /// solution per matching value, not every value of a matching record.
    BoundElement,
}

impl SlotReading {
    /// The name a consumer outside Rust reads. Stable: a renderer switches on
    /// it.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Column => "column",
            Self::AnyElement => "any_element",
            Self::BoundElement => "bound_element",
        }
    }
}

impl fmt::Display for SlotReading {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column => Ok(()),
            Self::AnyElement => f.write_str("[any]"),
            Self::BoundElement => f.write_str("[each]"),
        }
    }
}

/// One step of the SQL leaf.
///
/// Inputs are explicit so the shape is a DAG rather than a fixed pipeline: a
/// join has two. Every node carries the obligations it discharges, which is
/// what makes a rewrite checkable — move a node, move its claims, and the
/// ledger still has to balance.
#[derive(Debug, Clone)]
pub enum Op {
    /// Rows of one class, by `asset_type`, with the existence checks the
    /// scoper derived from the query's triple patterns.
    Scan {
        /// The SPARQL variable this star binds, which becomes a table alias.
        star_var: String,
        class_uri: String,
        /// Values bound against the class's identifier slot, compared against
        /// the indexed `asset360_uri` column rather than the payload.
        identifier_values: Vec<String>,
        /// Slots that must be present: `object_data ? 'slot'`.
        required_slots: Vec<String>,
        /// Slots that may be absent, so no existence check.
        optional_slots: Vec<String>,
        /// Whether the star itself appears only inside `OPTIONAL`.
        ///
        /// Decides both the join type above it and whether its own conditions
        /// have to tolerate a row the join did not match. A renderer that
        /// misses it turns an optional block into a required one, which drops
        /// rows the query keeps.
        is_optional: bool,
    },
    /// One row per element of a multivalued slot, so row count matches
    /// solution count. Without it a record with three values counts once.
    Unnest {
        input: OpId,
        star_var: String,
        slot_path: Vec<String>,
    },
    /// A value comparison on one slot of one star.
    Filter {
        input: OpId,
        star_var: String,
        slot_path: Vec<String>,
        condition: FilterCondition,
        enforcement: Enforcement,
        /// Which value at that path the condition holds of.
        ///
        /// Carried for the same reason as `numeric`, and it was missing: a
        /// condition on a multivalued slot is a test over the array's
        /// *elements*, and rendering it as an equality on the column compares
        /// the array's text and matches nothing. The star decomposition tells
        /// its renderer through `Star::multivalued_fields`; an operator tree
        /// had no way to say it, so the renderer reading operators rendered
        /// `object_data->>'trafficKinds' = 'm'` where the other path rendered
        /// an `EXISTS` over the elements -- no rows for a query with an
        /// answer.
        reading: SlotReading,
        /// Whether the value compares as a number rather than as text.
        ///
        /// Carried per node because it cannot be recovered from the path: the
        /// same slot name on two classes may differ, and a value inside a
        /// structure is not in the record's own slot list. Comparing a number
        /// as text is the `'9' >= '10'` answer the scoper tracks numeric-ness
        /// to prevent.
        numeric: bool,
    },
    /// A reference between two stars: the right side holds the foreign key.
    Join {
        left: OpId,
        right: OpId,
        /// The stars the two sides bind.
        ///
        /// Named as well as pointed at: an input may be a filter or another
        /// join, so recovering which star a side belongs to would mean walking
        /// down to its scan, and a renderer needs the variable to reach the
        /// right table alias.
        left_star: String,
        right_star: String,
        /// Slot on the right whose value is the left row's `asset360_uri`.
        right_slot: String,
        kind: JoinType,
    },
    /// Grouping and aggregation. `keys` are indices into `bindings`; empty
    /// means one row over the whole input, which is what SPARQL returns for a
    /// bare aggregate.
    Group {
        input: OpId,
        bindings: Vec<BindingSpec>,
        keys: Vec<usize>,
        measures: Vec<MeasureSpec>,
        /// Conditions on the grouped rows — SQL `HAVING`.
        ///
        /// On the grouping node rather than as an operator above it, because
        /// SQL has no operator there: a `HAVING` is a clause of the grouping,
        /// and its terms name that grouping's own measures. A separate node
        /// would have to point back into this one's measure list to be
        /// rendered at all.
        having: Vec<HavingTerm>,
    },
    Sort {
        input: OpId,
        terms: Vec<OrderTerm>,
    },
    Distinct {
        input: OpId,
    },
    Slice {
        input: OpId,
        limit: Option<usize>,
        offset: usize,
    },
    /// The variables the query asked for, in `SELECT` order. Anything not
    /// listed is machinery: a variable that exists only to be grouped by, or
    /// an aggregate spargebra named internally.
    Project {
        input: OpId,
        vars: Vec<String>,
    },
}

impl Op {
    /// The inputs this node consumes, for a walk that does not need to match
    /// on the variant.
    pub fn inputs(&self) -> Vec<OpId> {
        match self {
            Self::Scan { .. } => Vec::new(),
            Self::Unnest { input, .. }
            | Self::Filter { input, .. }
            | Self::Group { input, .. }
            | Self::Sort { input, .. }
            | Self::Distinct { input }
            | Self::Slice { input, .. }
            | Self::Project { input, .. } => vec![*input],
            Self::Join { left, right, .. } => vec![*left, *right],
        }
    }

    /// A short name, for a plan printout and for an executor that refuses a
    /// node kind it does not know.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Scan { .. } => "scan",
            Self::Unnest { .. } => "unnest",
            Self::Filter { .. } => "filter",
            Self::Join { .. } => "join",
            Self::Group { .. } => "group",
            Self::Sort { .. } => "sort",
            Self::Distinct { .. } => "distinct",
            Self::Slice { .. } => "slice",
            Self::Project { .. } => "project",
        }
    }
}

/// One node with its bookkeeping.
#[derive(Debug, Clone)]
pub struct OpNode {
    pub op: Op,
    /// Obligations this node discharges. A node that only narrows claims
    /// nothing — see [`Enforcement`].
    pub discharges: Vec<ObligationId>,
}

/// The SQL leaf's operators, flat, with the root last.
///
/// Flat rather than nested so a rule can replace or insert a node by index
/// without rebuilding the tree around it, and so a plan can be printed and
/// compared as data.
#[derive(Debug, Clone)]
pub struct OpTree {
    pub nodes: Vec<OpNode>,
}

/// Build the operator tree for an SQL pass.
///
/// Lowering, not planning: every decision was already made by the scoper and
/// the pushdown analyser. This restates their result as nodes so a rewrite has
/// something local to edit, and so the same result can be printed and compared
/// as data.
///
/// `enforcing` says whether this pass answers the query or only narrows what an
/// engine pass will decide, which fixes the [`Enforcement`] on every filter it
/// emits — a distinction the two composite structures could only express by
/// which pass claimed the obligation.
pub fn lower_sql_pass(
    plan: &crate::sparql_scoper::QueryPlan,
    solution: Option<&crate::sparql_pushdown::SolutionSpec>,
    discharges: &[ObligationId],
    enforcing: bool,
) -> OpTree {
    let mut nodes: Vec<OpNode> = Vec::new();
    let enforcement = if enforcing {
        Enforcement::Enforces
    } else {
        Enforcement::Narrows
    };

    // Scans, in the order the plan lists them, so a printed tree matches a
    // printed plan.
    let stars = plan.root.all_stars();
    let mut root_by_star: std::collections::HashMap<String, OpId> =
        std::collections::HashMap::new();

    for star in &stars {
        nodes.push(OpNode {
            op: Op::Scan {
                star_var: star.variable.clone(),
                class_uri: star.class_uri.clone(),
                identifier_values: star.identifier_values.clone(),
                required_slots: star.required_fields.clone(),
                optional_slots: star.optional_fields.clone(),
                is_optional: star.is_optional,
            },
            discharges: Vec::new(),
        });
        root_by_star.insert(star.variable.clone(), nodes.len() - 1);

        // A column's conditions, then the ones on a value inside a structure.
        // Both are comparisons on one star; the difference is only the depth of
        // the path, which the node carries.
        for (slot, conditions) in &star.filters {
            let numeric = star.numeric_fields.iter().any(|field| field == slot);
            // A condition the query wrote on a multivalued slot is a
            // containment test over the record's array: `?s :kinds "m"` and
            // `FILTER(?k = "m")` both ask whether the record carries the
            // triple, which matches it once.
            //
            // One of two places that decide a reading -- `lower_refined` is
            // the other, where it comes from the condition the rules built.
            // Both resolve it through the schema (here via
            // `Star::multivalued_fields`, there via the scan slot's own
            // multiplicity), so they agree; whoever changes one should look at
            // the other.
            let reading = if star.multivalued_fields.iter().any(|field| field == slot) {
                SlotReading::AnyElement
            } else {
                SlotReading::Column
            };
            for condition in conditions {
                let input = root_by_star[&star.variable];
                nodes.push(OpNode {
                    op: Op::Filter {
                        input,
                        star_var: star.variable.clone(),
                        slot_path: vec![slot.clone()],
                        condition: condition.clone(),
                        enforcement,
                        numeric,
                        reading,
                    },
                    discharges: Vec::new(),
                });
                root_by_star.insert(star.variable.clone(), nodes.len() - 1);
            }
        }
        for path_filter in &star.path_filters {
            for condition in &path_filter.conditions {
                let input = root_by_star[&star.variable];
                nodes.push(OpNode {
                    op: Op::Filter {
                        input,
                        star_var: star.variable.clone(),
                        slot_path: path_filter.slot_path.clone(),
                        condition: condition.clone(),
                        enforcement,
                        numeric: path_filter.numeric,
                        // Only single-valued hops become a path filter, and
                        // only a single-valued value at the end of one -- the
                        // scoper leaves an array to the engine, so a path
                        // condition names a column.
                        reading: SlotReading::Column,
                    },
                    discharges: Vec::new(),
                });
                root_by_star.insert(star.variable.clone(), nodes.len() - 1);
            }
        }
    }

    // Joins, left-deep in the plan's own order. `root_by_star` tracks where
    // each star's rows currently live, so a second join over the same star
    // builds on the first rather than on its scan.
    let mut current = stars.first().map(|star| root_by_star[&star.variable]);
    for join in plan.root.all_joins() {
        let left = root_by_star
            .get(&join.left)
            .copied()
            .or(current)
            .unwrap_or(0);
        let Some(&right) = root_by_star.get(&join.right) else {
            continue;
        };
        nodes.push(OpNode {
            op: Op::Join {
                left,
                right,
                left_star: join.left.clone(),
                right_star: join.right.clone(),
                right_slot: join.right_slot.clone(),
                kind: join.join_type,
            },
            discharges: Vec::new(),
        });
        let joined = nodes.len() - 1;
        root_by_star.insert(join.left.clone(), joined);
        root_by_star.insert(join.right.clone(), joined);
        current = Some(joined);
    }

    let mut input = match current {
        Some(id) => id,
        // No stars at all: nothing to lower, and the caller has already
        // refused a query with nothing scoped.
        None => return OpTree { nodes },
    };

    if let Some(solution) = solution {
        // Unnest before grouping: a multivalued binding must fan out first, or
        // the count is one per record instead of one per value.
        for binding in &solution.bindings {
            let multivalued = binding
                .containers
                .iter()
                .any(|container| *container != crate::sparql_pushdown::Container::Single);
            if !multivalued {
                continue;
            }
            nodes.push(OpNode {
                op: Op::Unnest {
                    input,
                    star_var: binding.star_var.clone(),
                    slot_path: binding.slot_path.clone(),
                },
                discharges: Vec::new(),
            });
            input = nodes.len() - 1;
        }

        nodes.push(OpNode {
            op: Op::Group {
                input,
                bindings: solution.bindings.clone(),
                keys: solution.group_keys.clone(),
                measures: solution.measures.clone(),
                having: solution.having.clone(),
            },
            // The claims sit here rather than spread over the nodes: the
            // analyser decides eligibility for the pass as a whole, so
            // attributing an obligation to a particular node would be a guess.
            // A rewrite that splits this node inherits the job of splitting the
            // claims with it.
            discharges: discharges.to_vec(),
        });
        input = nodes.len() - 1;

        if solution.distinct {
            nodes.push(OpNode {
                op: Op::Distinct { input },
                discharges: Vec::new(),
            });
            input = nodes.len() - 1;
        }
        if !solution.order_by.is_empty() {
            nodes.push(OpNode {
                op: Op::Sort {
                    input,
                    terms: solution.order_by.clone(),
                },
                discharges: Vec::new(),
            });
            input = nodes.len() - 1;
        }
        if solution.limit.is_some() || solution.offset > 0 {
            nodes.push(OpNode {
                op: Op::Slice {
                    input,
                    limit: solution.limit,
                    offset: solution.offset,
                },
                discharges: Vec::new(),
            });
            input = nodes.len() - 1;
        }
        nodes.push(OpNode {
            op: Op::Project {
                input,
                vars: solution.projected.clone(),
            },
            discharges: Vec::new(),
        });
    } else {
        // A scan feeding an engine pass: the rows are the product, and the
        // claims are the triples the scoper represented.
        if let Some(last) = nodes.last_mut() {
            last.discharges = discharges.to_vec();
        }

        // The fetch bound, when the scoper found one safe to apply. It is a
        // *narrowing* and claims nothing: the engine still applies the query's
        // own LIMIT and OFFSET, and this only spares the fetch from reading
        // rows no answer could use. Represented as an operator so the fetch
        // pass is fully described by its nodes -- otherwise a consumer reading
        // only operators would fetch unbounded where the scoper said it need
        // not.
        if let Some(limit) = plan.sql_limit {
            nodes.push(OpNode {
                op: Op::Slice {
                    input,
                    limit: Some(limit),
                    offset: 0,
                },
                discharges: Vec::new(),
            });
        }
    }

    OpTree { nodes }
}

// ---------------------------------------------------------------------------
// Lowering a refined plan
// ---------------------------------------------------------------------------

/// Why a refined plan could not be lowered into operators.
///
/// A reason rather than a bare `None`, because the caller logs it: a fallback
/// nobody can see is a fallback that becomes permanent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoweringRefusal {
    /// No node runs in SQL, so there is nothing to render.
    NothingPushed,
    /// The `Sql` nodes form more than one island. The frontier is a *cut*, so
    /// this is a legal plan -- an `OPTIONAL` over two stars is exactly it --
    /// and it is not a statement: today's pass is one SQL query, and rendering
    /// only one island would answer a narrower question than the pass claims.
    /// The rule that would fix it pushes the left join itself.
    SeveralIslands { islands: usize },
    /// An operator the refined plan pushed that this lowering does not
    /// translate. Every one of them is tier two, so a rule producing one is
    /// ahead of the renderer rather than wrong.
    UnknownOperator { kind: &'static str },
    /// A pushed filter that does not render, or a pushed join with no
    /// reference recorded. Both mean the plan claims SQL applies something SQL
    /// cannot state, so refusing is the only safe answer.
    Unrenderable { node: usize },
    /// A pushed left join. No rule pushes one today, and this lowering emits
    /// every scan with `is_optional: false` -- true only while that holds. A
    /// refusal rather than a comment, so the first rule that renders optional
    /// semantics in SQL fails here loudly instead of turning an optional block
    /// into a required one and dropping the rows the join exists to keep.
    PushedOptional { node: usize },
    /// A scan on the optional side of a left join. Same reason: the star is
    /// optional and this lowering would call it mandatory, which wraps its
    /// conditions the wrong way.
    OptionalScan { node: usize },
}

impl fmt::Display for LoweringRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NothingPushed => f.write_str("no operator runs in SQL"),
            Self::SeveralIslands { islands } => write!(
                f,
                "the SQL frontier is {islands} islands, and a pass is one statement"
            ),
            Self::UnknownOperator { kind } => {
                write!(f, "'{kind}' is pushed but this lowering cannot render it")
            }
            Self::Unrenderable { node } => {
                write!(f, "n{node} is pushed but does not render")
            }
            Self::PushedOptional { node } => write!(
                f,
                "n{node} pushes a left join, which this lowering cannot render"
            ),
            Self::OptionalScan { node } => write!(
                f,
                "n{node} scans the optional side of a left join, which this \
                 lowering would render as mandatory"
            ),
        }
    }
}

/// The operator tree for the `Sql` frontier of a refined plan.
///
/// Lowering, not planning -- the same contract as [`lower_sql_pass`]. Every
/// decision was made by the rules; this restates the result in the vocabulary
/// `sql_builder.py` already renders, so the refined planner reaches the
/// database through the path the differential oracle and the render tests
/// already cover. Two node vocabularies is the duplication to remove, not to
/// double.
///
/// The schema is needed for two facts the refined plan addresses *logically*
/// and a renderer needs *physically*: whether a value compares as a number,
/// and which slot is the class's identifier (whose values belong against the
/// indexed `asset360_uri` column rather than the JSONB payload). Both come
/// from the same `resolve_column` today's lowering uses, so the two cannot
/// disagree about a column.
pub fn lower_refined(
    plan: &crate::sparql_refine::Plan,
    schema: &linkml_schemaview::schemaview::SchemaView,
    fetch_bound: Option<usize>,
) -> Result<OpTree, LoweringRefusal> {
    use crate::sparql_refine::{Executor, PlanOp as RefinedOp, SlotPresence};

    let sql: Vec<usize> = (0..plan.nodes.len())
        .filter(|id| plan.nodes[*id].executor == Executor::Sql)
        .collect();
    if sql.is_empty() {
        return Err(LoweringRefusal::NothingPushed);
    }
    // An island root is an `Sql` node no `Sql` node reads. One island is a
    // statement; more than one is a plan whose frontier a single pass cannot
    // express.
    let roots: Vec<usize> = sql
        .iter()
        .copied()
        .filter(|id| {
            !plan
                .nodes
                .iter()
                .any(|node| node.executor == Executor::Sql && node.op.inputs().contains(id))
        })
        .collect();
    let [root] = roots.as_slice() else {
        return Err(LoweringRefusal::SeveralIslands {
            islands: roots.len(),
        });
    };
    if !sql.iter().all(|id| plan.feeds(*id, *root)) {
        return Err(LoweringRefusal::SeveralIslands { islands: 2 });
    }

    // Every scan below is emitted as mandatory, so a scan whose rows the query
    // only optionally wants has to be refused here. Both shapes are
    // unreachable today -- no rule pushes a left join, and a scan inside one's
    // optional side makes the frontier two islands -- and both are checked
    // rather than argued, because the argument stops holding the moment a
    // tier-two rule renders optional semantics in SQL.
    for &id in &sql {
        if matches!(plan.nodes[id].op, RefinedOp::LeftJoin { .. }) {
            return Err(LoweringRefusal::PushedOptional { node: id });
        }
    }
    for &id in &sql {
        if !matches!(plan.nodes[id].op, RefinedOp::Scan { .. }) {
            continue;
        }
        let optional_side = plan.nodes.iter().any(
            |node| matches!(&node.op, RefinedOp::LeftJoin { right, .. } if plan.feeds(id, *right)),
        );
        if optional_side {
            return Err(LoweringRefusal::OptionalScan { node: id });
        }
    }

    // A pass that answers the query enforces its filters; one that feeds the
    // engine only narrows. The refined plan says which by whether anything
    // above the frontier is left.
    let enforcement = if plan.nodes.len() == sql.len() {
        Enforcement::Enforces
    } else {
        Enforcement::Narrows
    };

    // Which class each star was scanned as, for the conditions and for the
    // identifier hoist.
    let classes: std::collections::HashMap<String, String> = plan
        .nodes
        .iter()
        .filter_map(|node| match &node.op {
            RefinedOp::Scan {
                star_var,
                class_uri,
                ..
            } => Some((star_var.clone(), class_uri.clone())),
            _ => None,
        })
        .collect();

    let mut nodes: Vec<OpNode> = Vec::with_capacity(sql.len());
    let mut remap: std::collections::HashMap<usize, OpId> = std::collections::HashMap::new();

    for id in sql {
        let node = &plan.nodes[id];
        match &node.op {
            RefinedOp::Scan {
                star_var,
                class_uri,
                slots,
            } => {
                // Only a slot of the record itself is an existence check.
                // A value further in is reached by walking into the column,
                // which is what the path conditions do, and today's star does
                // not check those either.
                let column_slots = |presence: SlotPresence| -> Vec<String> {
                    slots
                        .iter()
                        .filter(|slot| slot.presence == presence && slot.path.len() == 1)
                        .map(|slot| slot.path[0].clone())
                        .collect()
                };
                let identifier = identifier_slot_of(schema, class_uri);
                nodes.push(OpNode {
                    op: Op::Scan {
                        star_var: star_var.clone(),
                        class_uri: class_uri.clone(),
                        // Filled by the filters below: a constraint on the
                        // identifier slot belongs against the indexed column.
                        identifier_values: Vec::new(),
                        required_slots: column_slots(SlotPresence::Required)
                            .into_iter()
                            // The identifier's existence check is structurally
                            // always true, and today's star leaves it out for
                            // the same reason.
                            .filter(|slot| Some(slot.as_str()) != identifier.as_deref())
                            .collect(),
                        optional_slots: column_slots(SlotPresence::Delivered),
                        // Mandatory, and checked rather than assumed:
                        // `LoweringRefusal::OptionalScan` above refuses a scan
                        // the query only optionally wants, and
                        // `PushedOptional` refuses a left join in SQL.
                        is_optional: false,
                    },
                    discharges: node.discharges.clone(),
                });
            }
            RefinedOp::Unnest {
                star_var,
                slot_path,
                ..
            } => {
                let input = remap[&node.op.inputs()[0]];
                if enforcement == Enforcement::Narrows {
                    // A fan-out makes row count equal *solution* count, which
                    // only a pass that counts needs. This one hands rows to an
                    // engine that re-runs the whole query, so the fan-out buys
                    // nothing and costs the fetch a copy of the record per
                    // element -- fifty traffic kinds, fifty identical records
                    // triplified into the same graph. Dropping it is what
                    // makes the conditions above weaken to a containment test;
                    // see the filter arm.
                    remap.insert(id, input);
                    continue;
                }
                nodes.push(OpNode {
                    op: Op::Unnest {
                        input,
                        star_var: star_var.clone(),
                        slot_path: slot_path.clone(),
                    },
                    discharges: node.discharges.clone(),
                });
            }
            RefinedOp::Filter { condition, .. } => {
                let Some(conditions) = condition.to_sql(schema, &classes) else {
                    return Err(LoweringRefusal::Unrenderable { node: id });
                };
                let mut input = remap[&node.op.inputs()[0]];
                // One node per condition, with the claim on the last of them:
                // a conjunction is one obligation, and splitting the claim
                // would leave the ledger describing conditions rather than
                // demands.
                let last = conditions.len() - 1;
                for (index, condition) in conditions.into_iter().enumerate() {
                    let discharges = if index == last {
                        node.discharges.clone()
                    } else {
                        Vec::new()
                    };
                    let class_uri = classes.get(&condition.star_var);
                    // A constraint on the identifier slot is hoisted onto the
                    // scan, against the indexed column. Rendering it as a
                    // JSONB comparison would miss the B-tree, and today's star
                    // hoists it for the same reason.
                    let identifier = class_uri
                        .and_then(|class_uri| identifier_slot_of(schema, class_uri))
                        .is_some_and(|slot| condition.slot_path.as_slice() == [slot]);
                    if identifier {
                        let scan = scan_of_star(&mut nodes, &condition.star_var);
                        if let Some(Op::Scan {
                            identifier_values, ..
                        }) = scan.map(|scan| &mut nodes[scan].op)
                        {
                            match &condition.condition {
                                FilterCondition::Eq(value) => {
                                    identifier_values.push(value.clone());
                                }
                                FilterCondition::In(values) => {
                                    identifier_values.extend(values.iter().cloned());
                                }
                                // An ordering comparison on an identifier is
                                // not a set of values, so it stays a filter --
                                // the renderer collates the column itself.
                                FilterCondition::Cmp { .. } => {
                                    push_filter(
                                        &mut nodes,
                                        input,
                                        &condition,
                                        schema,
                                        class_uri,
                                        enforcement,
                                        discharges,
                                    );
                                    input = nodes.len() - 1;
                                    continue;
                                }
                            }
                            if !discharges.is_empty() {
                                nodes[scan.unwrap()].discharges.extend(discharges);
                                nodes[scan.unwrap()].discharges.sort_unstable();
                            }
                            continue;
                        }
                    }
                    push_filter(
                        &mut nodes,
                        input,
                        &condition,
                        schema,
                        class_uri,
                        enforcement,
                        discharges,
                    );
                    input = nodes.len() - 1;
                }
            }
            RefinedOp::Join {
                left,
                right,
                reference,
                ..
            } => {
                let Some(edge) = reference else {
                    // A join the rules pushed without recording its edge
                    // cannot be rendered: the renderer needs the slot that
                    // holds the other row's identifier.
                    return Err(LoweringRefusal::Unrenderable { node: id });
                };
                nodes.push(OpNode {
                    op: Op::Join {
                        left: remap[left],
                        right: remap[right],
                        left_star: edge.referenced.clone(),
                        right_star: edge.holder.clone(),
                        right_slot: edge.slot.clone(),
                        // No rule pushes a left join, so a pushed join is
                        // inner. The refusal above is what keeps that true.
                        kind: JoinType::Inner,
                    },
                    discharges: node.discharges.clone(),
                });
            }
            RefinedOp::Group { keys, measures, .. } => {
                let input = remap[&node.op.inputs()[0]];
                // One binding per group key, resolved against the schema by
                // the same function the single-pass planner uses -- the
                // renderer needs the term descriptor and the containers, and
                // deriving them here a second way is how two planners come to
                // disagree about a column.
                let mut bindings = Vec::with_capacity(keys.len());
                for key in keys {
                    let Some((star_var, class_uri, path)) = scanned_column(plan, key) else {
                        return Err(LoweringRefusal::Unrenderable { node: id });
                    };
                    let Some(spec) = crate::sparql_pushdown::binding_spec(
                        schema, &star_var, &class_uri, key, path,
                    ) else {
                        return Err(LoweringRefusal::Unrenderable { node: id });
                    };
                    bindings.push(spec);
                }
                let mut lowered = Vec::with_capacity(measures.len());
                for measure in measures {
                    // `COUNT(*)` only, which is what the rule pushes. Anything
                    // else reaching here is a rule ahead of this lowering, and
                    // a refusal is how it finds out.
                    if !matches!(
                        measure.aggregate,
                        spargebra::algebra::AggregateExpression::CountSolutions { distinct: false }
                    ) {
                        return Err(LoweringRefusal::Unrenderable { node: id });
                    }
                    lowered.push(MeasureSpec {
                        var: measure.var.clone(),
                        func: crate::sparql_pushdown::Measure::Count {
                            arg: None,
                            distinct: false,
                        },
                    });
                }
                nodes.push(OpNode {
                    op: Op::Group {
                        input,
                        keys: (0..bindings.len()).collect(),
                        bindings,
                        measures: lowered,
                        // No rule pushes a condition on grouped rows into a
                        // refined plan; the single-pass planner renders those.
                        having: Vec::new(),
                    },
                    discharges: node.discharges.clone(),
                });
            }
            RefinedOp::Project { vars, .. } => {
                let input = remap[&node.op.inputs()[0]];
                nodes.push(OpNode {
                    op: Op::Project {
                        input,
                        vars: vars.clone(),
                    },
                    discharges: node.discharges.clone(),
                });
            }
            other => {
                return Err(LoweringRefusal::UnknownOperator { kind: other.kind() });
            }
        }
        remap.insert(id, nodes.len() - 1);
    }

    // The fetch bound: a row cap the scoper found safe to apply, which claims
    // nothing -- the engine still applies the query's own `LIMIT` and this
    // only spares the fetch rows no answer could use.
    //
    // Borrowed from the single-pass planner rather than re-derived, and that
    // is a decision rather than a shortcut: whether a limit may reach the
    // fetch is the scoper's analysis (a dropped filter makes it unsafe, and
    // `test_limit_is_not_pushed_past_a_dropped_filter` is why), and a second
    // derivation of one decision is how two planners come to disagree about
    // it. Duplicating that reasoning here would be worse than depending on
    // it, so this dependency stays until a rule owns the question outright.
    //
    // Losing it is not a wrong answer but it is a real regression:
    // `LIMIT 1` fetched every row of the class, which is what
    // `test_single_star_limit_1_returns_exactly_one` caught the last time a
    // planner mislaid it.
    if enforcement == Enforcement::Narrows
        && let Some(limit) = fetch_bound
        && let Some(input) = nodes.len().checked_sub(1)
    {
        nodes.push(OpNode {
            op: Op::Slice {
                input,
                limit: Some(limit),
                offset: 0,
            },
            discharges: Vec::new(),
        });
    }

    Ok(OpTree { nodes })
}

/// The row cap a lowered pass carries, when it is the fetch bound.
///
/// A claim-free `Slice` on a pass that does not group is the scoper's bound;
/// the same reading `sql_builder.fetch_limit_from_ops` performs, from the same
/// nodes, so the two cannot disagree about what the fetch may skip.
pub fn fetch_bound_of(tree: &OpTree) -> Option<usize> {
    if !tree.find("group").is_empty() {
        return None;
    }
    tree.nodes.iter().rev().find_map(|node| match &node.op {
        Op::Slice { limit, offset, .. } if node.discharges.is_empty() && *offset == 0 => *limit,
        _ => None,
    })
}

/// A scan's facts, for a comparison that does not depend on node order.
struct ScanFacts {
    class_uri: String,
    identifier_values: Vec<String>,
    required_slots: Vec<String>,
    is_optional: bool,
}

fn scans_by_star(tree: &OpTree) -> BTreeMap<String, ScanFacts> {
    tree.nodes
        .iter()
        .filter_map(|node| match &node.op {
            Op::Scan {
                star_var,
                class_uri,
                identifier_values,
                required_slots,
                is_optional,
                ..
            } => {
                let mut identifier_values = identifier_values.clone();
                identifier_values.sort();
                let mut required_slots = required_slots.clone();
                required_slots.sort();
                Some((
                    star_var.clone(),
                    ScanFacts {
                        class_uri: class_uri.clone(),
                        identifier_values,
                        required_slots,
                        is_optional: *is_optional,
                    },
                ))
            }
            _ => None,
        })
        .collect()
}

/// Every condition a tree applies, as text a comparison can sort.
///
/// The reading and the numeric-ness are part of the identity: the same path
/// and the same value read two different ways select different rows, which is
/// the whole reason both facts are on the node.
fn conditions_of(tree: &OpTree) -> Vec<String> {
    let mut out: Vec<String> = tree
        .nodes
        .iter()
        .filter_map(|node| match &node.op {
            Op::Filter {
                star_var,
                slot_path,
                condition,
                numeric,
                reading,
                ..
            } => Some(format!(
                "?{star_var}.{} {condition} numeric={numeric} reading={reading}",
                slot_path.join(".")
            )),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

fn joins_of(tree: &OpTree) -> Vec<String> {
    let mut out: Vec<String> = tree
        .nodes
        .iter()
        .filter_map(|node| match &node.op {
            Op::Join {
                left_star,
                right_star,
                right_slot,
                kind,
                ..
            } => Some(format!("?{left_star} ?{right_star}.{right_slot} {kind:?}")),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

fn fanouts_of(tree: &OpTree) -> Vec<String> {
    let mut out: Vec<String> = tree
        .nodes
        .iter()
        .filter_map(|node| match &node.op {
            Op::Unnest {
                star_var,
                slot_path,
                ..
            } => Some(format!("?{star_var}.{}", slot_path.join("."))),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

/// The operators that collapse or reorder rows, which is the work a fallback
/// would hand back to the engine.
///
/// A slice is here only when it claims an obligation: the query's own `LIMIT`
/// is work, and the scoper's fetch bound is a narrowing compared separately.
fn collapsing_of(tree: &OpTree) -> Vec<String> {
    let mut out: Vec<String> = tree
        .nodes
        .iter()
        .filter_map(|node| match &node.op {
            // A grouping's `HAVING` is work the grouping does, so it is part
            // of what a fallback would hand back. Rendered into the key rather
            // than counted, so a *different* condition is a difference too.
            Op::Group { having, .. } => Some(format!(
                "group having=[{}]",
                having
                    .iter()
                    .map(|term| format!(
                        "{}{} {} numeric={}",
                        match term.key {
                            crate::sparql_pushdown::OrderKey::Binding(_) => "c",
                            crate::sparql_pushdown::OrderKey::Measure(_) => "m",
                        },
                        match term.key {
                            crate::sparql_pushdown::OrderKey::Binding(index)
                            | crate::sparql_pushdown::OrderKey::Measure(index) => index,
                        },
                        term.condition,
                        term.numeric
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            Op::Sort { .. } | Op::Distinct { .. } | Op::Project { .. } => {
                Some(node.op.kind().to_owned())
            }
            Op::Slice { .. } if !node.discharges.is_empty() => Some("slice".to_owned()),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

/// The first element of `wanted` that `have` does not cover, counting
/// duplicates.
fn missing_from(wanted: &[String], have: &[String]) -> Option<String> {
    let mut left: Vec<&String> = have.iter().collect();
    for item in wanted {
        match left.iter().position(|candidate| *candidate == item) {
            Some(index) => {
                left.remove(index);
            }
            None => return Some(item.clone()),
        }
    }
    None
}

/// The star, class and path an `Sql` scan binds a variable to, when one binds
/// it as a column of its own record.
///
/// Deliberately narrow: a variable bound to an element of an array, or read
/// without being required, is not a column a grouping can key on, and the
/// rules decline those before the lowering sees them.
fn scanned_column(
    plan: &crate::sparql_refine::Plan,
    var: &str,
) -> Option<(String, String, Vec<String>)> {
    use crate::sparql_refine::{Executor, PlanOp as RefinedOp, SlotPresence};
    plan.nodes.iter().find_map(|node| {
        let RefinedOp::Scan {
            star_var,
            class_uri,
            slots,
        } = &node.op
        else {
            return None;
        };
        if node.executor != Executor::Sql {
            return None;
        }
        slots
            .iter()
            .find(|slot| {
                slot.var.as_deref() == Some(var)
                    && !slot.multivalued
                    && slot.presence == SlotPresence::Required
            })
            .map(|slot| (star_var.clone(), class_uri.clone(), slot.path.clone()))
    })
}

/// The scan node for a star, in a tree being built.
fn scan_of_star(nodes: &mut [OpNode], star_var: &str) -> Option<OpId> {
    nodes.iter().position(
        |node| matches!(&node.op, Op::Scan { star_var: scanned, .. } if scanned == star_var),
    )
}

/// Append one filter operator, resolving the physical facts the condition does
/// not carry.
fn push_filter(
    nodes: &mut Vec<OpNode>,
    input: OpId,
    condition: &crate::sparql_refine::SqlCondition,
    schema: &linkml_schemaview::schemaview::SchemaView,
    class_uri: Option<&String>,
    enforcement: Enforcement,
    discharges: Vec<ObligationId>,
) {
    // Numeric-ness is a property of the column, not of the condition, which is
    // why the condition names a slot rather than carrying SQL: the renderer
    // has to cast, or `'9' >= '10'` is true. Resolved through the same
    // `resolve_column` today's lowering uses.
    let numeric = class_uri.is_some_and(|class_uri| {
        crate::sparql_scoper::numeric_at_path(schema, class_uri, &condition.slot_path)
    });
    // A condition on the element an unnest bound cannot name that element
    // where the unnest was dropped, so it becomes the containment test that
    // *narrows* to the same records -- one row per record instead of one per
    // matching value, which is the shape a fetch wants and the shape today's
    // star renders. Sound only in the narrowing direction: a pass that
    // answers alone keeps its fan-out and its element condition, because
    // "some element matches" counts a record once where SPARQL counts its
    // matching values.
    let reading = match (enforcement, condition.reading) {
        (Enforcement::Narrows, SlotReading::BoundElement) => SlotReading::AnyElement,
        (_, reading) => reading,
    };
    nodes.push(OpNode {
        op: Op::Filter {
            input,
            star_var: condition.star_var.clone(),
            slot_path: condition.slot_path.clone(),
            condition: condition.condition.clone(),
            enforcement,
            numeric,
            reading,
        },
        discharges,
    });
}

/// The name of a class's identifier slot, when it has one.
fn identifier_slot_of(
    schema: &linkml_schemaview::schemaview::SchemaView,
    class_uri: &str,
) -> Option<String> {
    let class = schema.get_class_by_uri(class_uri).ok().flatten()?;
    class.identifier_slot().map(|slot| slot.name.clone())
}

impl OpTree {
    pub fn root(&self) -> Option<&OpNode> {
        self.nodes.last()
    }

    /// Every obligation any node claims, in ascending order, with duplicates
    /// kept — a caller checking the ledger wants to see a double claim rather
    /// than have it silently collapsed.
    pub fn claims(&self) -> Vec<ObligationId> {
        let mut claimed: Vec<ObligationId> = self
            .nodes
            .iter()
            .flat_map(|node| node.discharges.iter().copied())
            .collect();
        claimed.sort_unstable();
        claimed
    }

    /// Whether every node's inputs exist and precede it.
    ///
    /// The invariant a rewrite is most likely to break: inserting a node
    /// without renumbering the ones after it leaves an input pointing at the
    /// wrong operator, which no type checks.
    pub fn is_well_formed(&self) -> bool {
        self.nodes.iter().enumerate().all(|(index, node)| {
            node.op
                .inputs()
                .iter()
                .all(|input| *input < index && *input < self.nodes.len())
        })
    }

    /// Whether this tree fetches a row set no larger than `today`'s and leaves
    /// no more work to the engine.
    ///
    /// **The question the switchover gate actually cares about.** The gate was
    /// first written to compare *claims*, and for every tier-one rule the two
    /// coincide -- a rule that pushes work claims what it pushed -- so the
    /// difference did not show. They come apart on an `OPTIONAL` read: both
    /// statements deliver the same column, the SQL is identical, nothing moved
    /// to the engine, and the ledgers differ only because a narrowing scan
    /// declines to claim optionality it does not render. Comparing claims
    /// there rejects the *more truthful* plan for a difference that costs
    /// nothing, so the comparand is the row source and the ledger stays out of
    /// the decision.
    ///
    /// Decidable and conservative: every clause below either shows a
    /// difference cannot make things worse, or refuses. `Ok(())` means it is
    /// safe to switch; `Err` carries the reason for the caller's log.
    ///
    /// What "no worse" is made of, and why each direction:
    ///
    /// * **the same scans** -- same stars, same classes, same optionality.
    ///   Scanning a different set of records is not a comparison this can
    ///   reason about, so it refuses rather than guessing which is narrower.
    /// * **at least today's existence checks.** More `object_data ? 'slot'`
    ///   means fewer rows. Fewer means more, so today's must all be there.
    /// * **at least today's conditions**, matched exactly -- path, condition,
    ///   reading and numeric-ness. An extra condition narrows further, which
    ///   is safe under the over-fetch contract; a missing one fetches rows
    ///   today's statement excluded.
    /// * **the same joins.** An inner join both narrows and adds a class to
    ///   read, so neither direction is safely "no worse".
    /// * **no new fan-out.** An `unnest` multiplies rows, so having one today
    ///   lacks is worse by definition.
    /// * **at least today's collapsing work** -- group, sort, distinct,
    ///   project, and any slice that claims an obligation. Missing one means
    ///   the engine does it instead, which is exactly the regression this
    ///   refuses.
    /// * **a fetch bound no looser than today's.** Losing it fetches every row
    ///   of the class; `LIMIT 1` returning three rows is how that showed up
    ///   the last time.
    ///
    /// `optional_slots` are deliberately not compared: they emit nothing. A
    /// slot listed there suppresses an existence check it would not otherwise
    /// have, so a difference in that list alone cannot change a row.
    pub fn is_no_worse_than(&self, today: &OpTree) -> Result<(), String> {
        let mine = scans_by_star(self);
        let theirs = scans_by_star(today);
        let stars = |scans: &BTreeMap<String, ScanFacts>| -> Vec<String> {
            scans.keys().cloned().collect()
        };
        if stars(&mine) != stars(&theirs) {
            return Err(format!(
                "the two plans scan different stars: {:?} against {:?}",
                stars(&mine),
                stars(&theirs)
            ));
        }
        for (star, today_scan) in &theirs {
            let scan = &mine[star];
            if scan.class_uri != today_scan.class_uri {
                return Err(format!(
                    "?{star} is scanned as {} rather than {}",
                    scan.class_uri, today_scan.class_uri
                ));
            }
            if scan.is_optional != today_scan.is_optional {
                return Err(format!("?{star} disagrees about being optional"));
            }
            if scan.identifier_values != today_scan.identifier_values {
                return Err(format!(
                    "?{star} disagrees about identifier values: {:?} against {:?}",
                    scan.identifier_values, today_scan.identifier_values
                ));
            }
            if let Some(missing) = today_scan
                .required_slots
                .iter()
                .find(|slot| !scan.required_slots.contains(slot))
            {
                return Err(format!(
                    "?{star} would not require '{missing}', so the fetch is wider"
                ));
            }
        }

        if let Some(missing) = missing_from(&conditions_of(today), &conditions_of(self)) {
            return Err(format!("the refined plan does not apply {missing}"));
        }
        if joins_of(self) != joins_of(today) {
            return Err(format!(
                "the two plans join differently: {:?} against {:?}",
                joins_of(self),
                joins_of(today)
            ));
        }
        if let Some(extra) = missing_from(&fanouts_of(self), &fanouts_of(today)) {
            return Err(format!(
                "the refined plan fans out {extra}, which today does not"
            ));
        }
        if let Some(missing) = missing_from(&collapsing_of(today), &collapsing_of(self)) {
            return Err(format!("the refined plan leaves '{missing}' to the engine"));
        }
        match (fetch_bound_of(self), fetch_bound_of(today)) {
            (_, None) => {}
            (Some(mine), Some(theirs)) if mine <= theirs => {}
            (mine, Some(theirs)) => {
                return Err(format!(
                    "the fetch bound is {mine:?} rather than {theirs:?}, so the fetch is wider"
                ));
            }
        }
        Ok(())
    }

    /// Nodes of one kind, for a rule that looks for its own shape.
    pub fn find(&self, kind: &str) -> Vec<OpId> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.op.kind() == kind)
            .map(|(id, _)| id)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparql_plan::{PassKind, plan_query};
    use crate::sparql_scoper::tests::test_schema_view;
    use linkml_schemaview::schemaview::SchemaView;

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

    /// Every SQL pass of a planned query, as (pass claims, operator tree).
    fn sql_passes(query: &str) -> Vec<(Vec<ObligationId>, OpTree)> {
        let sv = test_schema_view();
        let plan = plan_query(&format!("{PREFIX}{query}"), &sv).expect("should plan");
        plan.passes
            .iter()
            .filter_map(|pass| match &pass.kind {
                PassKind::Sql(sql) => Some((pass.discharges.clone(), sql.ops.clone())),
                PassKind::Engine(_) => None,
            })
            .collect()
    }

    /// Every refined plan the rules produce for these queries lowers, and the
    /// tree it lowers to is well formed -- the invariant a renderer walking
    /// the list in order depends on.
    #[test]
    fn a_refined_plan_lowers_to_a_well_formed_tree() {
        let sv = test_schema_view();
        for query in [
            "SELECT ?s WHERE { ?s a asset360:Signal }",
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             FILTER(?nm > \"A\") }",
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name \"BX517\" ; \
             asset360:length 3 }",
            "SELECT ?k WHERE { ?s a asset360:Signal ; asset360:trafficKinds ?k . \
             FILTER(?k = \"m\") }",
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t a asset360:Track ; asset360:hasName ?tn }",
            "SELECT ?lon WHERE { ?s a asset360:Signal ; asset360:location ?loc . \
             ?loc asset360:longitude ?lon . FILTER(?lon > 3) }",
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { ?s asset360:name ?nm } }",
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             VALUES ?nm { \"a\" \"b\" } }",
        ] {
            let tree = lowered(query, &sv).unwrap_or_else(|refusal| {
                panic!("{query} did not lower: {refusal}");
            });
            assert!(tree.is_well_formed(), "{query}");
            assert!(!tree.nodes.is_empty(), "{query}");
        }
    }

    /// The refined plan and today's planner render the same statement.
    ///
    /// The gate that matters at this level: the operators are what
    /// `sql_builder.py` reads, so two trees that agree node for node produce
    /// the same SQL. Compared as *data* rather than as SQL text because the
    /// renderer lives in Python -- `tests/test_ops_render.py` closes that half
    /// by rendering both.
    #[test]
    fn the_two_planners_lower_to_the_same_row_set() {
        let sv = test_schema_view();
        for query in [
            "SELECT ?s WHERE { ?s a asset360:Signal }",
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm }",
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             FILTER(?nm > \"A\") }",
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name \"BX517\" }",
            "SELECT ?len WHERE { ?s a asset360:Signal ; asset360:length ?len . \
             FILTER(?len >= 10) }",
            "SELECT ?k WHERE { ?s a asset360:Signal ; asset360:trafficKinds ?k . \
             FILTER(?k = \"m\") }",
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:trafficKinds \"m\" }",
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t a asset360:Track ; asset360:hasName ?tn }",
            "SELECT ?lon WHERE { ?s a asset360:Signal ; asset360:location ?loc . \
             ?loc asset360:longitude ?lon . FILTER(?lon > 3) }",
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { ?s asset360:name ?nm } }",
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:asset360_uri \"u\" }",
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             VALUES ?nm { \"a\" \"b\" } }",
            // The first collapsing shape: a scalar key and a count. Its
            // lowered statement has to be today's, node for node, because
            // today's planner already pushes this grouping -- which is exactly
            // why it is the shape to start with.
            "SELECT ?nm (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:name ?nm } GROUP BY ?nm",
        ] {
            let refined = lowered(query, &sv)
                .unwrap_or_else(|refusal| panic!("{query} did not lower: {refusal}"));
            let passes = sql_passes(query);
            let [(_, today)] = passes.as_slice() else {
                panic!("{query} does not have exactly one SQL pass today");
            };
            assert_eq!(
                row_set(&refined),
                row_set(today),
                "the two planners disagree about the statement for {query}"
            );
        }
    }

    /// The row set as data: what a renderer reads out of the operators, with
    /// the order of a star's own conditions normalised away.
    ///
    /// `Star::filters` is a `HashMap`, so today's lowering emits one star's
    /// conditions in whatever order it iterates; the refined plan emits them
    /// in the order the query wrote them. A conjunction of `WHERE` clauses
    /// means the same thing either way, so the difference is not one to hold a
    /// planner to.
    fn row_set(tree: &OpTree) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let mut conditions: Vec<String> = Vec::new();
        for node in &tree.nodes {
            match &node.op {
                Op::Scan {
                    star_var,
                    class_uri,
                    identifier_values,
                    required_slots,
                    optional_slots,
                    is_optional,
                } => {
                    let mut identifiers = identifier_values.clone();
                    identifiers.sort();
                    let mut required = required_slots.clone();
                    required.sort();
                    let mut optional = optional_slots.clone();
                    optional.sort();
                    out.push(format!(
                        "scan ?{star_var} {class_uri} ids={identifiers:?} \
                         req={required:?} opt={optional:?} optional={is_optional}"
                    ));
                }
                Op::Filter {
                    star_var,
                    slot_path,
                    condition,
                    numeric,
                    reading,
                    ..
                } => conditions.push(format!(
                    "filter ?{star_var}.{} {condition} numeric={numeric} reading={reading}",
                    slot_path.join(".")
                )),
                Op::Join {
                    left_star,
                    right_star,
                    right_slot,
                    kind,
                    ..
                } => out.push(format!(
                    "join ?{left_star} ?{right_star}.{right_slot} {kind:?}"
                )),
                Op::Unnest {
                    star_var,
                    slot_path,
                    ..
                } => out.push(format!("unnest ?{star_var}.{}", slot_path.join("."))),
                other => out.push(other.kind().to_owned()),
            }
        }
        conditions.sort();
        out.extend(conditions);
        out
    }

    /// A narrowing pass drops the fan-out and weakens the element condition to
    /// a containment test.
    ///
    /// Both halves are one decision. The fan-out makes row count equal
    /// *solution* count, which only a pass that counts needs; this one hands
    /// rows to an engine that re-runs the query, so keeping it would fetch a
    /// copy of the record per element for nothing. And once the unnest is
    /// gone, a condition naming the element it bound has nothing to name, so
    /// it becomes the test that narrows to the same records.
    ///
    /// Sound only in this direction. A pass that answers alone keeps both:
    /// "some element matches" counts a record once where SPARQL counts its
    /// matching values, which is the D11-shaped error the fan-out exists to
    /// prevent.
    #[test]
    fn a_narrowing_pass_reads_the_record_and_not_the_element() {
        let sv = test_schema_view();
        let query = "SELECT ?k WHERE { ?s a asset360:Signal ; asset360:trafficKinds ?k . \
                     FILTER(?k = \"m\") }";
        let plan = refined_plan(query, &sv);
        // The refined plan itself says element: the unnest is there and the
        // condition names what it bound.
        assert_eq!(plan.find("unnest").len(), 1, "{plan}");
        assert!(
            plan.nodes[plan.find("filter")[0]]
                .op
                .describe()
                .contains("[each]"),
            "{plan}"
        );

        let tree = lower_refined(&plan, &sv, None).expect("should lower");
        assert!(
            tree.find("unnest").is_empty(),
            "a narrowing fetch does not fan out"
        );
        let Op::Filter {
            reading,
            enforcement,
            ..
        } = &tree.nodes[tree.find("filter")[0]].op
        else {
            panic!("the second node is the filter");
        };
        assert_eq!(*reading, SlotReading::AnyElement);
        assert_eq!(*enforcement, Enforcement::Narrows);
    }

    /// The comparator, in both directions, on trees built by hand -- so what
    /// counts as "worse" is stated rather than inferred from whichever
    /// difference two planners happen to produce.
    #[test]
    fn the_gate_refuses_every_way_a_statement_can_be_worse() {
        let scan = |required: &[&str], identifier: &[&str], optional: bool| OpNode {
            op: Op::Scan {
                star_var: "s".to_owned(),
                class_uri: "https://data.infrabel.be/asset360/Signal".to_owned(),
                identifier_values: identifier.iter().map(|v| (*v).to_owned()).collect(),
                required_slots: required.iter().map(|v| (*v).to_owned()).collect(),
                optional_slots: Vec::new(),
                is_optional: optional,
            },
            discharges: Vec::new(),
        };
        let filter = |reading: SlotReading| OpNode {
            op: Op::Filter {
                input: 0,
                star_var: "s".to_owned(),
                slot_path: vec!["name".to_owned()],
                condition: FilterCondition::Eq("BX".to_owned()),
                enforcement: Enforcement::Narrows,
                numeric: false,
                reading,
            },
            discharges: Vec::new(),
        };
        let slice = |limit: Option<usize>, claims: Vec<ObligationId>| OpNode {
            op: Op::Slice {
                input: 0,
                limit,
                offset: 0,
            },
            discharges: claims,
        };
        let tree = |nodes: Vec<OpNode>| OpTree { nodes };

        let today = tree(vec![
            scan(&["name"], &[], false),
            filter(SlotReading::Column),
        ]);
        // The same statement is trivially no worse than itself.
        today.is_no_worse_than(&today).unwrap();
        // Narrowing further is fine: an extra condition costs rows the engine
        // would have discarded, which is the over-fetch contract.
        tree(vec![
            scan(&["name", "kind"], &[], false),
            filter(SlotReading::Column),
            filter(SlotReading::Column),
        ])
        .is_no_worse_than(&today)
        .unwrap();

        for (worse, why) in [
            (
                tree(vec![scan(&["name"], &[], false)]),
                "a condition today applies is missing",
            ),
            (
                tree(vec![scan(&[], &[], false), filter(SlotReading::Column)]),
                "an existence check today applies is missing",
            ),
            (
                tree(vec![
                    scan(&["name"], &["u"], false),
                    filter(SlotReading::Column),
                ]),
                "the identifier values disagree",
            ),
            (
                tree(vec![
                    scan(&["name"], &[], true),
                    filter(SlotReading::Column),
                ]),
                "the star's optionality disagrees",
            ),
            (
                tree(vec![
                    scan(&["name"], &[], false),
                    filter(SlotReading::AnyElement),
                ]),
                "the same path read a different way selects different rows",
            ),
            (
                tree(vec![
                    scan(&["name"], &[], false),
                    filter(SlotReading::Column),
                    OpNode {
                        op: Op::Unnest {
                            input: 1,
                            star_var: "s".to_owned(),
                            slot_path: vec!["trafficKinds".to_owned()],
                        },
                        discharges: Vec::new(),
                    },
                ]),
                "a fan-out today does not have multiplies rows",
            ),
        ] {
            worse
                .is_no_worse_than(&today)
                .expect_err(&format!("should have refused: {why}"));
        }

        // Work handed back to the engine, and a fetch bound loosened: two
        // shapes with their own clause, so they get their own baseline.
        let grouping = tree(vec![
            scan(&["name"], &[], false),
            OpNode {
                op: Op::Group {
                    input: 0,
                    bindings: Vec::new(),
                    keys: Vec::new(),
                    measures: Vec::new(),
                    having: Vec::new(),
                },
                discharges: vec![0],
            },
        ]);
        tree(vec![scan(&["name"], &[], false)])
            .is_no_worse_than(&grouping)
            .expect_err("dropping the grouping hands it back to the engine");

        let bounded = tree(vec![scan(&[], &[], false), slice(Some(1), Vec::new())]);
        tree(vec![scan(&[], &[], false)])
            .is_no_worse_than(&bounded)
            .expect_err("losing the fetch bound reads every row of the class");
        tree(vec![scan(&[], &[], false), slice(Some(1), Vec::new())])
            .is_no_worse_than(&bounded)
            .unwrap();
        // A tighter bound is narrower, which is not worse.
        tree(vec![scan(&[], &[], false), slice(Some(1), Vec::new())])
            .is_no_worse_than(&tree(vec![
                scan(&[], &[], false),
                slice(Some(5), Vec::new()),
            ]))
            .unwrap();
    }

    /// A refined plan whose `Sql` nodes are two islands is a legal plan and
    /// not a statement, so the lowering refuses rather than rendering one of
    /// them and answering a narrower question.
    ///
    /// The shape is an `OPTIONAL` over a second star: nothing pushes a left
    /// join, so the two scans have no `Sql` node joining them.
    #[test]
    fn two_islands_do_not_lower() {
        let sv = test_schema_view();
        let refusal = lowered(
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { ?t a asset360:Track ; asset360:hasName ?tn } }",
            &sv,
        )
        .expect_err("two islands must not lower");
        assert!(
            matches!(refusal, LoweringRefusal::SeveralIslands { .. }),
            "{refusal}"
        );
    }

    /// A scan the query only optionally wants is refused, rather than lowered
    /// as mandatory.
    ///
    /// Unreachable through the rules today -- nothing pushes a left join, and
    /// a scan inside one's optional side makes the frontier two islands -- so
    /// the plan is edited by hand to reach the check. That is the point: the
    /// check exists so the first tier-two rule that renders optional semantics
    /// in SQL fails loudly here instead of turning an optional block into a
    /// required one.
    #[test]
    fn a_scan_the_query_only_optionally_wants_does_not_lower() {
        let sv = test_schema_view();
        let mut plan = refined_plan(
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal ; asset360:kind ?k . \
             OPTIONAL { ?s asset360:name ?nm } }",
            &sv,
        );
        // As it stands, this lowers: the optional read is a delivered slot on
        // a mandatory scan.
        lower_refined(&plan, &sv, None).expect("should lower as it is");

        // Push the left join, as a tier-two rule eventually will.
        let leftjoin = plan.find("leftjoin")[0];
        plan.nodes[leftjoin].executor = crate::sparql_refine::Executor::Sql;
        for input in plan.nodes[leftjoin].op.inputs() {
            plan.nodes[input].executor = crate::sparql_refine::Executor::Sql;
        }
        let refusal = lower_refined(&plan, &sv, None)
            .expect_err("a pushed left join must not lower as an inner one");
        assert!(
            matches!(refusal, LoweringRefusal::PushedOptional { .. }),
            "{refusal}"
        );

        // And the other half: the scan on the optional side, without the left
        // join itself being pushed.
        let mut sided = refined_plan(
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             OPTIONAL { ?t a asset360:Track ; asset360:hasName ?tn } }",
            &sv,
        );
        // Two islands as it stands, which is its own refusal; make the
        // mandatory side engine-run so the optional scan is the only island
        // and the reason has to be the optionality.
        let mandatory = sided
            .find("scan")
            .into_iter()
            .find(|id| {
                matches!(&sided.nodes[*id].op,
                    crate::sparql_refine::PlanOp::Scan { star_var, .. } if star_var == "s")
            })
            .expect("the mandatory star is scanned");
        sided.nodes[mandatory].executor = crate::sparql_refine::Executor::Engine;
        let refusal = lower_refined(&sided, &sv, None)
            .expect_err("a scan on the optional side must not lower as mandatory");
        assert!(
            matches!(refusal, LoweringRefusal::OptionalScan { .. }),
            "{refusal}"
        );
    }

    /// A plan with nothing pushed has nothing to render, which is a refusal
    /// and not an empty statement -- an empty statement would fetch every row
    /// of every class.
    #[test]
    fn a_plan_with_nothing_pushed_does_not_lower() {
        let sv = test_schema_view();
        let refusal = lowered("SELECT ?s WHERE { VALUES ?s { \"a\" \"b\" } }", &sv)
            .expect_err("nothing pushed must not lower");
        assert_eq!(refusal, LoweringRefusal::NothingPushed);
    }

    /// The identifier slot's values are hoisted onto the scan, against the
    /// indexed column, exactly as today's star hoists them -- a JSONB
    /// comparison would miss the B-tree.
    #[test]
    fn an_identifier_constraint_is_hoisted_onto_the_scan() {
        let sv = test_schema_view();
        let tree = lowered(
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:asset360_uri \"u\" }",
            &sv,
        )
        .expect("should lower");
        let Op::Scan {
            identifier_values,
            required_slots,
            ..
        } = &tree.nodes[0].op
        else {
            panic!("the first node is the scan");
        };
        assert_eq!(identifier_values, &vec!["u".to_owned()]);
        assert!(
            required_slots.is_empty(),
            "every row has an identifier, so the existence check is structurally true"
        );
        assert!(
            tree.find("filter").is_empty(),
            "hoisted, not rendered twice"
        );
        // The claim moves with it: the triple is discharged by the scan, so
        // the ledger still accounts for it.
        assert_eq!(tree.claims().len(), 2, "the type and the identifier value");
    }

    /// Refinement moves claims, never invents them: what the lowered tree
    /// claims is what the refined plan's `Sql` nodes claimed.
    #[test]
    fn lowering_carries_the_claims_across() {
        let sv = test_schema_view();
        for query in [
            "SELECT ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             FILTER(?nm > \"A\") }",
            "SELECT ?tn WHERE { ?s a asset360:Signal ; asset360:locatedOnTrack ?t . \
             ?t a asset360:Track ; asset360:hasName ?tn }",
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name \"BX517\" ; \
             asset360:length 3 }",
        ] {
            let plan = refined_plan(query, &sv);
            let tree = lowered(query, &sv).expect("should lower");
            let mut expected: Vec<ObligationId> = plan
                .nodes
                .iter()
                .filter(|node| node.executor == crate::sparql_refine::Executor::Sql)
                .flat_map(|node| node.discharges.iter().copied())
                .collect();
            expected.sort_unstable();
            assert_eq!(tree.claims(), expected, "{query}\n{plan}");
        }
    }

    /// A refined plan, to fixpoint.
    fn refined_plan(query: &str, sv: &SchemaView) -> crate::sparql_refine::Plan {
        let rules = crate::sparql_rules::tier_one_rules(sv);
        let borrowed: Vec<&dyn crate::sparql_rules::Rule> =
            rules.iter().map(|rule| rule.as_ref()).collect();
        let mut plan = crate::sparql_refine::naive_plan_of(&format!("{PREFIX}{query}"))
            .expect("should build a naive plan");
        crate::sparql_rules::refine(&mut plan, &borrowed).expect("every invariant holds");
        plan
    }

    fn lowered(query: &str, sv: &SchemaView) -> Result<OpTree, LoweringRefusal> {
        lower_refined(&refined_plan(query, sv), sv, None)
    }

    /// A condition on a multivalued slot is a test over the array's elements,
    /// and the operator has to say so.
    ///
    /// The bug this fixes was live: a renderer reading operators had no way to
    /// know, so it rendered `object_data->>'trafficKinds' = 'm'` where the
    /// star decomposition rendered an `EXISTS` over the elements. That returns
    /// no rows for a query with an answer -- and it is a *fetch* for an engine
    /// that re-runs the query, so the endpoint answered nothing rather than
    /// answering slowly.
    #[test]
    fn a_condition_on_an_array_says_it_reads_the_elements() {
        let readings = |query: &str| -> Vec<(Vec<String>, SlotReading)> {
            sql_passes(query)
                .into_iter()
                .flat_map(|(_, tree)| tree.nodes)
                .filter_map(|node| match node.op {
                    Op::Filter {
                        slot_path, reading, ..
                    } => Some((slot_path, reading)),
                    _ => None,
                })
                .collect()
        };

        assert_eq!(
            readings("SELECT ?s WHERE { ?s a asset360:Signal ; asset360:trafficKinds \"m\" }"),
            vec![(vec!["trafficKinds".to_owned()], SlotReading::AnyElement)],
        );
        assert_eq!(
            readings(
                "SELECT ?k WHERE { ?s a asset360:Signal ; asset360:trafficKinds ?k . \
                 FILTER(?k = \"m\") }"
            ),
            vec![(vec!["trafficKinds".to_owned()], SlotReading::AnyElement)],
        );
        // A single-valued slot names a column, and so does a value inside a
        // structure: the scoper walks single-valued hops only.
        assert_eq!(
            readings("SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name \"BX517\" }"),
            vec![(vec!["name".to_owned()], SlotReading::Column)],
        );
        assert_eq!(
            readings(
                "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:location ?loc . \
                 ?loc asset360:longitude 5 }"
            ),
            vec![(
                vec!["location".to_owned(), "longitude".to_owned()],
                SlotReading::Column
            )],
        );
    }

    /// The invariant every rewrite has to preserve, checked on what the planner
    /// itself emits: a node's inputs exist and come before it. A rule that
    /// inserts a node without renumbering breaks this, and nothing in the type
    /// system would.
    #[test]
    fn lowered_trees_are_well_formed() {
        for query in [
            "SELECT ?s WHERE { ?s a asset360:Signal }",
            "SELECT ?n (COUNT(*) AS ?c) WHERE { ?s a asset360:Signal ; asset360:name ?n } \
             GROUP BY ?n ORDER BY DESC(?c) LIMIT 5",
            "SELECT ?sig ?bg WHERE { ?bg a asset360:BaliseGroup ; asset360:refersToSignal ?sig . \
             ?sig a asset360:Signal }",
            "SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name \"BX517\" }",
        ] {
            for (_claims, ops) in sql_passes(query) {
                assert!(ops.is_well_formed(), "{query}\n{:#?}", ops.nodes);
                assert!(!ops.nodes.is_empty(), "{query} lowered to nothing");
            }
        }
    }

    /// The operators must account for exactly what the pass says it
    /// discharges. If lowering dropped or invented a claim, a rewrite checked
    /// against the tree would be checking against a different plan than the one
    /// the executor runs.
    #[test]
    fn operator_claims_match_the_pass() {
        for query in [
            "SELECT ?n (COUNT(*) AS ?c) WHERE { ?s a asset360:Signal ; asset360:name ?n } \
             GROUP BY ?n",
            "SELECT ?s ?n WHERE { ?s a asset360:Signal ; asset360:name ?n } LIMIT 3",
        ] {
            for (claims, ops) in sql_passes(query) {
                let mut expected = claims.clone();
                expected.sort_unstable();
                assert_eq!(ops.claims(), expected, "{query}\n{:#?}", ops.nodes);
            }
        }
    }

    /// A filter's enforcement is the thing a pushdown rule reads, so it has to
    /// follow the pass rather than the syntax: the same comparison enforces
    /// when SQL answers alone and only narrows when an engine pass decides.
    #[test]
    fn enforcement_follows_who_answers() {
        // SQL answers alone: the constant is applied and claimed.
        let (_claims, ops) = sql_passes(
            "SELECT ?n (COUNT(*) AS ?c) WHERE { \
             ?s a asset360:Signal ; asset360:name ?n ; asset360:kind \"main\" } GROUP BY ?n",
        )
        .into_iter()
        .next()
        .expect("one SQL pass");

        let filters: Vec<Enforcement> = ops
            .nodes
            .iter()
            .filter_map(|node| match &node.op {
                Op::Filter { enforcement, .. } => Some(*enforcement),
                _ => None,
            })
            .collect();
        assert!(
            !filters.is_empty() && filters.iter().all(|e| *e == Enforcement::Enforces),
            "an all-SQL pass enforces its filters, got {filters:?}"
        );

        // The engine finishes: the same shape of filter narrows only, because
        // the engine reapplies it with SPARQL's term semantics.
        let (_claims, ops) = sql_passes(
            "SELECT ?s ?n WHERE { ?s a asset360:Signal ; asset360:name ?n ; \
             asset360:kind \"main\" } LIMIT 3",
        )
        .into_iter()
        .next()
        .expect("one SQL pass");

        let filters: Vec<Enforcement> = ops
            .nodes
            .iter()
            .filter_map(|node| match &node.op {
                Op::Filter { enforcement, .. } => Some(*enforcement),
                _ => None,
            })
            .collect();
        assert!(
            !filters.is_empty() && filters.iter().all(|e| *e == Enforcement::Narrows),
            "a narrowing scan does not enforce, got {filters:?}"
        );
    }

    /// A grouping pass lowers to the operators the renderer will walk, in an
    /// order that respects what each step needs: fan out before counting, and
    /// project last.
    #[test]
    fn a_grouping_pass_lowers_in_execution_order() {
        let (_claims, ops) = sql_passes(
            "SELECT ?n (COUNT(*) AS ?c) WHERE { ?s a asset360:Signal ; asset360:name ?n } \
             GROUP BY ?n ORDER BY DESC(?c) LIMIT 5",
        )
        .into_iter()
        .next()
        .expect("one SQL pass");

        let kinds: Vec<&str> = ops.nodes.iter().map(|node| node.op.kind()).collect();
        assert_eq!(
            kinds,
            vec!["scan", "group", "sort", "slice", "project"],
            "{:#?}",
            ops.nodes
        );
        assert_eq!(ops.root().map(|node| node.op.kind()), Some("project"));
    }

    /// Lowering must carry the two facts a renderer cannot recover from the
    /// node's shape, because dropping either is a wrong answer rather than an
    /// error: whether a comparison is numeric (text comparison makes
    /// `'9' >= '10'` true) and whether a star is optional (a required render
    /// of an optional block drops rows the query keeps).
    #[test]
    fn lowering_carries_numeric_and_optional() {
        let (_claims, ops) = sql_passes(
            "SELECT ?s ?len WHERE { ?s a asset360:Signal ; asset360:length ?len . \
             FILTER(?len >= 10) }",
        )
        .into_iter()
        .next()
        .expect("one SQL pass");

        let numeric_filters: Vec<bool> = ops
            .nodes
            .iter()
            .filter_map(|node| match &node.op {
                Op::Filter { numeric, .. } => Some(*numeric),
                _ => None,
            })
            .collect();
        assert!(
            numeric_filters.iter().any(|numeric| *numeric),
            "a comparison on an integer slot compares as a number, got {numeric_filters:?}"
        );

        // An optional star is marked, so the renderer knows the join above it
        // is a LEFT JOIN and its own conditions have to be null-tolerant.
        let (_claims, ops) = sql_passes(
            "SELECT ?sig ?bg WHERE { ?sig a asset360:Signal . \
             OPTIONAL { ?bg a asset360:BaliseGroup ; asset360:refersToSignal ?sig } }",
        )
        .into_iter()
        .next()
        .expect("one SQL pass");

        let optionality: Vec<(String, bool)> = ops
            .nodes
            .iter()
            .filter_map(|node| match &node.op {
                Op::Scan {
                    star_var,
                    is_optional,
                    ..
                } => Some((star_var.clone(), *is_optional)),
                _ => None,
            })
            .collect();
        assert!(
            optionality.iter().any(|(_, optional)| *optional),
            "the OPTIONAL block's star is optional, got {optionality:?}"
        );
        assert!(
            optionality.iter().any(|(_, optional)| !*optional),
            "the mandatory star is not, got {optionality:?}"
        );
    }

    /// A join lowers to a join node over two scans, which is what makes join
    /// order a rewrite rather than surgery inside one structure.
    #[test]
    fn a_reference_join_lowers_to_a_join_node() {
        let (_claims, ops) = sql_passes(
            "SELECT ?sig ?bg WHERE { ?bg a asset360:BaliseGroup ; asset360:refersToSignal ?sig . \
             ?sig a asset360:Signal }",
        )
        .into_iter()
        .next()
        .expect("one SQL pass");

        assert_eq!(ops.find("scan").len(), 2, "{:#?}", ops.nodes);
        let joins = ops.find("join");
        assert_eq!(joins.len(), 1, "{:#?}", ops.nodes);

        // Both sides of the join are scans of the two classes, reached through
        // the node's own inputs rather than by position.
        let inputs = ops.nodes[joins[0]].op.inputs();
        assert_eq!(inputs.len(), 2);
        for input in inputs {
            assert_eq!(ops.nodes[input].op.kind(), "scan");
        }
    }
}
