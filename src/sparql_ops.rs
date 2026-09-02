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

use crate::sparql_plan::ObligationId;
use crate::sparql_pushdown::{BindingSpec, MeasureSpec, OrderTerm};
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
    }

    OpTree { nodes }
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
