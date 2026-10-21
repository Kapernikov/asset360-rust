//! **A `BIND` of a string cut, run in SQL.**
//!
//! `BIND(STRAFTER(STR(?e), "/track/") AS ?tail)` was an engine node: no tier-one
//! rule ever flipped a [`PlanOp::Bind`], so a query with one needed the engine
//! to finish over rows (or, with two such nodes, declined the statement as
//! several islands). [`PushStringBind`] flips the smallest useful subset:
//!
//! * `STR(?x)` of a scanned star's identity (its IRI) or of a plain string
//!   column;
//! * `STRAFTER(arg, "const")` and `STRBEFORE(arg, "const")` where `arg` is
//!   one of those, or a plain string column read directly.
//!
//! Everything else -- `SUBSTR`, `STRLEN`, `CONCAT`, case folding, a language
//! tagged, enum, reference or numeric column, a needle that is not a simple
//! string constant, a leaf that is a relation's export -- stays the engine's.
//!
//! **Why exactly this subset is exact.** spareval's `STRAFTER`/`STRBEFORE`
//! take a string literal (simple, `xsd:string` or language tagged) and a
//! needle; an argument that is not a string is an error, which leaves the
//! variable unbound. `STR` of a literal or an IRI is a simple literal. So
//! over a column whose values are simple strings, or an IRI under `STR`:
//!
//! * no match yields the simple literal `""` (not unbound);
//! * an empty needle matches at 0, so `STRAFTER(x, "") = x` and
//!   `STRBEFORE(x, "") = ""`;
//! * an unbound input is an error, hence unbound -- NULL in SQL, and every
//!   function here is NULL-strict;
//! * positions are code points, which PostgreSQL's text functions count too.
//!
//! A language tagged argument makes the result carry the tag, and a tagged
//! needle must agree with it; both are declined by only admitting columns with
//! no language, and needles that are simple strings.
//!
//! The expression stays the query's own (`Expr::Var` leaves) in the plan so the
//! oracle harness can write it back; leaves are resolved to columns at
//! lowering, by [`computed_of`].

use linkml_schemaview::schemaview::SchemaView;
use spargebra::term::Term;

use crate::sparql_pushdown::Computed;
use crate::sparql_refine::{Executor, Expr, NodeId, Plan, PlanOp, SlotReading};
use crate::sparql_rules::Rule;
use crate::sparql_scopes::{TermOf, resolve_terms};
use crate::sparql_terms::{TermDescriptor, TermKind};

const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

/// The expression with `STR` erased -- both leaf kinds are text already --
/// and its leaves named by variable. The lowering resolves a leaf to the
/// column the statement reads it from; this only says it can.
pub(crate) enum Shape {
    Leaf(String),
    After(Box<Shape>, String),
    Before(Box<Shape>, String),
}

impl Shape {
    /// The shape as a [`Computed`], each leaf variable resolved by `column`.
    pub(crate) fn computed(
        &self,
        column: &impl Fn(&str) -> Option<crate::sparql_pushdown::BindingSpec>,
    ) -> Option<Computed> {
        Some(match self {
            Self::Leaf(var) => Computed::Column(Box::new(column(var)?)),
            Self::After(arg, needle) => {
                Computed::StrAfter(Box::new(arg.computed(column)?), needle.clone())
            }
            Self::Before(arg, needle) => {
                Computed::StrBefore(Box::new(arg.computed(column)?), needle.clone())
            }
        })
    }
}

/// The term a computed string is: a simple literal.
pub(crate) fn result_descriptor() -> TermDescriptor {
    TermDescriptor {
        kind: TermKind::Literal,
        datatype: None,
        lang: None,
        enum_map: Vec::new(),
        numeric: false,
    }
}

/// Whether `expr`, as the extension of the plan at `input`, is a string
/// computation SQL can run, and over which variables.
pub(crate) fn shape_of(
    schema: &SchemaView,
    plan: &Plan,
    input: NodeId,
    expr: &Expr,
) -> Option<Shape> {
    let Expr::Function { name, args } = expr else {
        return None;
    };
    let leaf = |var: &str, identity_ok: bool| -> Option<Shape> {
        is_text_column(schema, plan, input, var, identity_ok).then(|| Shape::Leaf(var.to_owned()))
    };
    match (name.as_str(), args.as_slice()) {
        ("STR", [Expr::Var(var)]) => leaf(var, true),
        ("STRAFTER" | "STRBEFORE", [arg, Expr::Literal(Term::Literal(needle))]) => {
            if needle.language().is_some()
                || needle.datatype().as_str() != XSD_STRING
                || needle.value().contains('\0')
            {
                return None;
            }
            let arg = match arg {
                Expr::Var(var) => leaf(var, false)?,
                Expr::Function { name, args } if name == "STR" => match args.as_slice() {
                    [Expr::Var(var)] => leaf(var, true)?,
                    _ => return None,
                },
                _ => return None,
            };
            let (arg, needle) = (Box::new(arg), needle.value().to_owned());
            Some(if name == "STRAFTER" {
                Shape::After(arg, needle)
            } else {
                Shape::Before(arg, needle)
            })
        }
        _ => None,
    }
}

/// Whether `var` at `input` is text a statement reads from a scan: a star's
/// identity (`identity_ok`), or a plain string column.
///
/// Every producer is a scan the statement runs and can see -- a relation's
/// export, an engine node or a constant table is not a column of this
/// statement -- and exactly one of them binds it. A reference join binds the
/// variable twice (a slot and the referenced star's identity) and is refused:
/// the stored form may be a CURIE, and the lowering would read the preserved
/// side's slot text rather than the joined star's `asset360_uri`.
///
/// A column is plain when its values are simple strings: no language (the
/// result would carry the tag, and a needle must agree with it), no datatype
/// but `xsd:string` (anything else is an error to `STRAFTER`), not an enum,
/// not a number, one value per record read as a column (a fan-out would make
/// the leaf an element).
fn is_text_column(
    schema: &SchemaView,
    plan: &Plan,
    input: NodeId,
    var: &str,
    identity_ok: bool,
) -> bool {
    let producers = plan.producers_of(input, var);
    if producers.len() != 1
        || !producers.iter().all(|producer| {
            plan.nodes[*producer].executor == Executor::Sql
                && matches!(plan.nodes[*producer].op, PlanOp::Scan { .. })
                && plan.feeds_visibly(*producer, input)
        })
    {
        return false;
    }
    match resolve_terms(plan.term_of(schema, input, var)) {
        Some(TermOf::Identity { .. }) => identity_ok,
        Some(TermOf::Slot {
            class_uri,
            path,
            reading: SlotReading::Column,
            ..
        }) => {
            let Some((descriptor, containers)) =
                crate::sparql_terms::resolve_column(schema, &class_uri, &path)
            else {
                return false;
            };
            containers.iter().all(|mode| {
                matches!(
                    mode,
                    linkml_schemaview::slotview::SlotContainerMode::SingleValue
                )
            }) && descriptor.kind == TermKind::Literal
                && descriptor.lang.is_none()
                && descriptor
                    .datatype
                    .as_deref()
                    .is_none_or(|datatype| datatype == XSD_STRING)
                && !descriptor.numeric
                && descriptor.enum_map.is_empty()
        }
        _ => false,
    }
}

/// Whether the `Bind` at `node` is a computation SQL can run.
pub(crate) fn is_pushable(schema: &SchemaView, plan: &Plan, node: NodeId) -> bool {
    matches!(&plan.nodes[node].op, PlanOp::Bind { input, expr, .. }
        if shape_of(schema, plan, *input, expr).is_some())
}

/// A `Bind` over a `Sql` input whose expression is a string computation of
/// [`shape_of`]'s vocabulary becomes `Sql`.
///
/// Preconditions, each with the wrong answer it prevents:
///
/// * **The input runs in SQL.** The frontier is a cut; a bind over an engine
///   node cannot be SQL however well it renders.
/// * **Every leaf is one visible scan's column** and nothing else binds the
///   name: a variable two stars bind is a value join, and naming either
///   column answers a different question.
/// * **The variable the bind introduces is new** to its input. SPARQL forbids
///   the other case, but a lifted plan can reach it, and SQL has no way to
///   say which of two columns of one name is meant.
///
/// The bind keeps its own expression; its leaves are resolved at lowering. The
/// result is not a slot, so no filter, join or grouping above it can name it
/// and those stay with the engine -- which reads the statement's column.
pub struct PushStringBind<'s> {
    schema: &'s SchemaView,
    /// Whether the lifting rules run beside this one, so a join above the
    /// bind can be waited for.
    lifts: bool,
}

impl<'s> PushStringBind<'s> {
    pub fn new(schema: &'s SchemaView) -> Self {
        Self {
            schema,
            lifts: false,
        }
    }

    /// The rule for a schedule that has R4: a bind it is about to lift over a
    /// join is left to it.
    pub fn with_lifts(schema: &'s SchemaView) -> Self {
        Self {
            schema,
            lifts: true,
        }
    }
}

impl Rule for PushStringBind<'_> {
    fn name(&self) -> &'static str {
        "push_string_bind"
    }

    fn apply(&self, plan: &mut Plan) -> bool {
        for id in 0..plan.nodes.len() {
            if plan.nodes[id].executor != Executor::Engine {
                continue;
            }
            let PlanOp::Bind { input, var, expr } = &plan.nodes[id].op else {
                continue;
            };
            if plan.nodes[*input].executor != Executor::Sql
                || plan.variables_of(*input).contains(var)
                || shape_of(self.schema, plan, *input, expr).is_none()
            {
                continue;
            }
            // A join above the bind that R4 lifts it over is waiting for the
            // bind to get out of the way: the join may then be pushed, or an
            // optional read absorbed into the scan, which a bind between the
            // two prevents. The bind is pushed once it sits above whatever
            // the join becomes; left where it is, it costs the plan the very
            // statement the join was about to join.
            if self.lifts
                && (0..plan.nodes.len()).any(|join| {
                    matches!(
                    crate::sparql_lift::LiftExtensionOverJoin::candidate(plan, join),
                        Some(Ok(found)) if found.bind == id
                    )
                })
            {
                continue;
            }
            plan.nodes[id].executor = Executor::Sql;
            return true;
        }
        false
    }
}
#[cfg(all(test, feature = "sparql-endpoint"))]
mod tests {
    use crate::sparql_algebra::equivalence::each_rewrite_preserves_answers;
    use crate::sparql_lift::tests::{PREFIX, lifted_plan};
    use crate::sparql_ops::Op;
    use crate::sparql_oracle::fixture;
    use crate::sparql_plan::{PassKind, Refinement};
    use crate::sparql_pushdown::Computed;
    use crate::sparql_refine::{Executor, Plan, PlanOp};
    use crate::sparql_rules::{Rule, tier_one_rules};
    use crate::sparql_scoper::tests::{test_schema_view, test_schema_view_plus};
    use linkml_schemaview::schemaview::SchemaView;

    /// A language-tagged string column, to be refused.
    const TAGGED: &str = r#"
  Tagged:
    class_uri: asset360:Tagged
    attributes:
      asset360_uri:
        identifier: true
      label:
        range: string
        in_language: en
"#;

    fn plan_of(query: &str, schema: &SchemaView) -> crate::sparql_plan::ExecutionPlan {
        crate::sparql_plan::plan_of(
            &format!("{PREFIX}{query}"),
            schema,
            crate::sparql_dataset::default_dataset(),
        )
        .unwrap()
    }

    /// Every operator of the first SQL pass, relation bodies included.
    fn ops_of(plan: &crate::sparql_plan::ExecutionPlan) -> Vec<&Op> {
        fn walk<'a>(tree: &'a crate::sparql_ops::OpTree, out: &mut Vec<&'a Op>) {
            for node in &tree.nodes {
                out.push(&node.op);
                if let Op::Relation { body, .. } = &node.op {
                    walk(body, out);
                }
            }
        }
        let Some(PassKind::Sql(sql)) = plan.passes.first().map(|pass| &pass.kind) else {
            panic!("{plan}");
        };
        let mut out = Vec::new();
        walk(&sql.ops, &mut out);
        out
    }

    /// The computed bindings of a plan's statement, as `(var, expression)`.
    fn computed_columns(plan: &crate::sparql_plan::ExecutionPlan) -> Vec<(String, String)> {
        ops_of(plan)
            .into_iter()
            .filter_map(|op| match op {
                Op::Project { bindings, .. } => Some(bindings),
                _ => None,
            })
            .flatten()
            .filter_map(|binding| {
                binding
                    .computed
                    .as_ref()
                    .map(|computed| (binding.var.clone(), computed.render()))
            })
            .collect()
    }

    fn sql_binds(plan: &Plan) -> usize {
        plan.nodes
            .iter()
            .filter(|node| matches!(node.op, PlanOp::Bind { .. }) && node.executor == Executor::Sql)
            .count()
    }

    /// **The A1 shape**: a `BIND` over a plain string column inside an
    /// `OPTIONAL`, with another `OPTIONAL` and an `ORDER BY` beside it, is one
    /// statement. The bind is a computed column of the optional body's
    /// relation, which exports it as `derived`, and the outside reads that
    /// column.
    #[test]
    fn a_bind_over_a_string_column_in_an_optional_is_one_statement() {
        let schema = test_schema_view();
        let plan = plan_of(
            "SELECT ?s ?nm ?t ?len WHERE { ?s a asset360:Signal . \
             OPTIONAL { ?s asset360:name ?nm . BIND(STRAFTER(?nm, \"l\") AS ?t) } \
             OPTIONAL { ?s asset360:length ?len } } ORDER BY ?s",
            &schema,
        );
        assert!(
            matches!(plan.refinement, Refinement::UsedAlone(_)),
            "{plan}"
        );
        assert_eq!(
            computed_columns(&plan),
            vec![("t".to_owned(), "STRAFTER(?s.name, \"l\")".to_owned())],
            "{plan}"
        );
        let relation = ops_of(&plan)
            .into_iter()
            .find_map(|op| match op {
                Op::Relation { columns, .. } => Some(columns),
                _ => None,
            })
            .expect("the optional body is a relation");
        let t = relation.iter().find(|column| column.var == "t").unwrap();
        assert_eq!(t.kind.as_str(), "derived");
        assert_eq!(
            t.descriptor.as_ref().map(|descriptor| descriptor.shape()),
            Some("literal".to_owned())
        );
        // The outside reads the relation's column, not the expression.
        let outer = ops_of(&plan)
            .into_iter()
            .filter_map(|op| match op {
                Op::Project { bindings, .. } => Some(bindings),
                _ => None,
            })
            .next_back()
            .unwrap();
        let outer_t = outer.iter().find(|binding| binding.var == "t").unwrap();
        assert!(outer_t.computed.is_none() && outer_t.relation.is_some());
    }

    /// A top-level `STR`/`STRBEFORE` over the subject IRI and over a column,
    /// each a computed column.
    #[test]
    fn str_strafter_and_strbefore_are_computed_columns() {
        let schema = test_schema_view();
        let plan = plan_of(
            "SELECT ?s ?a ?b ?c WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             BIND(STR(?s) AS ?a) BIND(STRBEFORE(STR(?s), \"/\") AS ?b) \
             BIND(STR(?nm) AS ?c) }",
            &schema,
        );
        assert!(
            matches!(plan.refinement, Refinement::UsedAlone(_)),
            "{plan}"
        );
        let computed = computed_columns(&plan);
        assert_eq!(
            computed,
            vec![
                ("a".to_owned(), "?s".to_owned()),
                ("b".to_owned(), "STRBEFORE(?s, \"/\")".to_owned()),
                ("c".to_owned(), "?s.name".to_owned()),
            ],
            "{plan}"
        );
        // The shape the Python renderer reads.
        let Some(Op::Project { bindings, .. }) = ops_of(&plan)
            .into_iter()
            .rfind(|op| matches!(op, Op::Project { .. }))
        else {
            panic!("{plan}");
        };
        let b = bindings.iter().find(|binding| binding.var == "b").unwrap();
        let Some(Computed::StrBefore(arg, needle)) = &b.computed else {
            panic!("{plan}");
        };
        assert_eq!(needle, "/");
        let Computed::Column(leaf) = arg.as_ref() else {
            panic!("{plan}");
        };
        assert_eq!((leaf.star_var.as_str(), leaf.slot_path.len()), ("s", 0));
        assert!(b.star_var.is_empty() && b.slot_path.is_empty() && b.class_uri.is_empty());
        assert_eq!(b.descriptor.shape(), "literal");
    }

    /// What stays the engine's, exactly as before: every one of these keeps
    /// its bind out of SQL.
    #[test]
    fn everything_outside_the_vocabulary_stays_with_the_engine() {
        let schema = test_schema_view_plus(TAGGED);
        for (what, bind) in [
            (
                "a language-tagged column",
                "?l a asset360:Tagged ; asset360:label ?x . BIND(STRAFTER(?x, \"a\") AS ?t)",
            ),
            (
                "an enum column",
                "?s asset360:kind ?x . BIND(STRAFTER(STR(?x), \"G\") AS ?t)",
            ),
            (
                "a reference",
                "?s asset360:locatedOnTrack ?x . BIND(STRAFTER(STR(?x), \"t\") AS ?t)",
            ),
            (
                "a number",
                "?s asset360:length ?x . BIND(STRAFTER(STR(?x), \"3\") AS ?t)",
            ),
            ("a bare identity", "BIND(STRAFTER(?s, \"a\") AS ?t)"),
            ("UCASE", "?s asset360:name ?x . BIND(UCASE(?x) AS ?t)"),
            ("SUBSTR", "?s asset360:name ?x . BIND(SUBSTR(?x, 2) AS ?t)"),
            (
                "CONCAT",
                "?s asset360:name ?x . BIND(CONCAT(?x, \"a\") AS ?t)",
            ),
            ("STRLEN", "?s asset360:name ?x . BIND(STRLEN(?x) AS ?t)"),
            (
                "nested cuts",
                "?s asset360:name ?x . BIND(STRAFTER(STRBEFORE(?x, \"p\"), \"l\") AS ?t)",
            ),
            (
                "a tagged needle",
                "?s asset360:name ?x . BIND(STRAFTER(?x, \"a\"@en) AS ?t)",
            ),
            (
                "a typed needle",
                "?s asset360:name ?x . BIND(STRAFTER(?x, \"a\"^^<http://www.w3.org/2001/XMLSchema#token>) AS ?t)",
            ),
            (
                "a variable needle",
                "?s asset360:name ?x . BIND(STRAFTER(?x, ?x) AS ?t)",
            ),
            (
                "a multivalued column",
                "?s asset360:trafficKinds ?x . BIND(STRAFTER(?x, \"m\") AS ?t)",
            ),
            (
                "a sub-select's export",
                "{ SELECT ?s ?x WHERE { ?s a asset360:Signal ; asset360:name ?x } } BIND(STRAFTER(?x, \"l\") AS ?t)",
            ),
            (
                "a variable of an enclosing body",
                "?s asset360:name ?x . OPTIONAL { ?s asset360:length ?len . BIND(STRAFTER(?x, \"l\") AS ?t) }",
            ),
        ] {
            let query = format!("{PREFIX}SELECT * WHERE {{ {{ ?s a asset360:Signal . {bind} }} }}");
            let query = if what == "a language-tagged column" {
                format!("{PREFIX}SELECT * WHERE {{ {bind} }}")
            } else {
                query
            };
            let plan = lifted_plan(&query, &schema);
            assert_eq!(sql_binds(&plan), 0, "{what}\n{plan}");
        }
        // The control: the same wrapper, a bind that is in the vocabulary.
        let plan = lifted_plan(
            &format!(
                "{PREFIX}SELECT * WHERE {{ {{ ?s a asset360:Signal . ?s asset360:name ?x . \
                 BIND(STRAFTER(?x, \"l\") AS ?t) }} }}"
            ),
            &schema,
        );
        assert_eq!(sql_binds(&plan), 1, "{plan}");
    }

    /// **G2f**: R2 does not lift a bind SQL runs -- not even before the
    /// rule that makes it SQL has fired. The plan is refined without the
    /// rules that push the body, so the left join is still the engine's and
    /// R2 would otherwise take the bind; the decline names why.
    #[test]
    fn the_lift_leaves_a_bind_the_statement_runs() {
        let schema = test_schema_view();
        let declined = |bind: &str| {
            let query = format!(
                "{PREFIX}SELECT ?s ?t WHERE {{ ?s a asset360:Signal . \
                 OPTIONAL {{ ?s asset360:name ?nm . BIND({bind} AS ?t) }} }}"
            );
            let mut plan = crate::sparql_refine::naive_plan(
                &crate::sparql_scoper::parse_query(&query, Some(&schema)).unwrap(),
                crate::sparql_dataset::default_dataset(),
            )
            .unwrap();
            let rules = tier_one_rules(&schema, crate::sparql_dataset::default_dataset());
            let borrowed: Vec<&dyn Rule> = rules
                .iter()
                .filter(|rule| {
                    !matches!(
                        rule.name(),
                        "push_string_bind"
                            | "push_left_join"
                            | "push_barrier"
                            | "push_join_on_identity"
                            | "push_projection"
                    )
                })
                .map(|rule| rule.as_ref())
                .collect();
            crate::sparql_rules::refine(&mut plan, &borrowed).unwrap();
            crate::sparql_lift::LiftOptionalExtension::new(&schema).declined(&plan)
        };
        let pushable = declined("STRAFTER(?nm, \"l\")");
        assert!(
            pushable.iter().any(|(_, why)| why.starts_with("G2f")),
            "{pushable:?}"
        );
        // The control: R2 still takes a bind SQL does not run.
        assert!(declined("STRLEN(?nm)").is_empty());
    }

    /// **A bind between a scan and an optional read does not stop the read
    /// being absorbed.** `BIND(..) OPTIONAL { ?s :length ?len }` is lifted by
    /// R4 above the left join so the join can be pushed (here the optional
    /// read is absorbed into the scan); the rule that pushes the bind waits for
    /// that, and the result is one statement with the bind computed in it --
    /// not a plan that stops at the bind and hands the engine the optional
    /// read (a real-schema `UsedRows` that became a records-route `Used`).
    #[test]
    fn a_bind_waits_for_the_lift_over_the_join_above_it() {
        let schema = test_schema_view();
        let query = "SELECT ?s ?t ?len WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
                     BIND(STRAFTER(STR(?s), \"signal/\") AS ?t) \
                     OPTIONAL { ?s asset360:length ?len } }";
        let printed = crate::sparql_lift::tests::printout(&format!("{PREFIX}{query}"), &schema);
        assert!(printed.contains("R4  "), "{printed}");

        let plan = plan_of(query, &schema);
        assert!(
            matches!(plan.refinement, Refinement::UsedAlone(_)),
            "{plan}"
        );
        assert_eq!(
            computed_columns(&plan),
            vec![("t".to_owned(), "STRAFTER(?s, \"signal/\")".to_owned())],
            "{plan}"
        );
    }

    /// Every rewrite of queries with these binds keeps the answer -- the
    /// oracle holds the plan before and after each rule to the original --
    /// over names that are absent (`signal/C`), repeated, and cut at the
    /// start, the middle, the end, nowhere and by the empty needle. The last
    /// two read a variable of an enclosing body, which the rule must leave
    /// alone (the oracle's own nested-`OPTIONAL` scoping is not asked about).
    #[test]
    fn every_rewrite_of_a_string_bind_preserves_the_answer() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let rules = tier_one_rules(&schema, crate::sparql_dataset::default_dataset());
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        for query in [
            "SELECT ?s ?t WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:name ?nm . BIND(STRAFTER(?nm, \"l\") AS ?t) } }",
            "SELECT ?s ?t WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:name ?nm . BIND(STRBEFORE(?nm, \"h\") AS ?t) } }",
            "SELECT ?s ?t WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:name ?nm . BIND(STRAFTER(?nm, \"zz\") AS ?t) } }",
            "SELECT ?s ?t ?u WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:name ?nm . BIND(STRAFTER(?nm, \"\") AS ?t) BIND(STRBEFORE(?nm, \"\") AS ?u) } }",
            "SELECT ?s ?t WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:name ?nm . BIND(STRAFTER(?nm, \"Alpha\") AS ?t) } }",
            "SELECT ?s ?t WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:name ?nm . BIND(STRBEFORE(?nm, \"A\") AS ?t) } }",
            "SELECT ?s ?t WHERE { ?s a asset360:Signal . BIND(STRAFTER(STR(?s), \"signal/\") AS ?t) } ORDER BY ?t ?s",
            "SELECT ?s ?t WHERE { ?s a asset360:Signal ; asset360:name ?nm . BIND(STR(?nm) AS ?t) }",
            "SELECT ?s ?t WHERE { ?s a asset360:Signal . BIND(STR(?s) AS ?t) }",
            "SELECT ?s ?t WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:locatedOnTrack ?tr . ?tr a asset360:Track . BIND(STRAFTER(STR(?tr), \"track/\") AS ?t) } }",
            "SELECT ?s ?t ?len WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:name ?nm . BIND(STRAFTER(?nm, \"l\") AS ?t) } OPTIONAL { ?s asset360:length ?len } } ORDER BY ?s",
            "SELECT ?s ?t ?ti WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:name ?nm . BIND(STRAFTER(?nm, \"l\") AS ?t) OPTIONAL { ?s asset360:documents ?d . ?d asset360:title ?ti } } }",
            "SELECT ?s ?nm ?t WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:name ?nm OPTIONAL { ?s asset360:length ?len . BIND(STRAFTER(?nm, \"l\") AS ?t) } } }",
            "SELECT ?s ?t WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:name ?nm . BIND(STRAFTER(?nm, \"l\") AS ?t) } FILTER(?t != \"\") }",
        ] {
            let plan = each_rewrite_preserves_answers(query, &schema, &oracle, &borrowed);
            // Not vacuous: the bind is SQL's, except where it reads a
            // variable of an enclosing body.
            let enclosing = query
                .contains("OPTIONAL { ?s asset360:length ?len . BIND(STRAFTER(?nm")
                || query.contains("?tr a asset360:Track");
            let binds = query.matches("BIND(").count();
            assert_eq!(
                sql_binds(&plan),
                if enclosing { 0 } else { binds },
                "{query}\n{plan}"
            );
        }
    }
}
