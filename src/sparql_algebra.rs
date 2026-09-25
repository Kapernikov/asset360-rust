//! `plan_to_algebra`: a refined plan back as SPARQL algebra, so a rewrite
//! can be *evaluated* rather than trusted.
//!
//! The test instrument of `docs/design/sparql-scopes-as-relations.md`, test
//! 2(d): every logical rewrite, taken alone, must be answer-preserving, and
//! that is checked by translating the plan before and after each rule
//! application and evaluating both on the in-memory oracle over the full
//! fixture. A rewrite that is only right in combination with a later one is
//! a rewrite whose precondition lies, and this is what finds it.
//!
//! The translation covers the *logical* subset -- what a plan means, not how
//! it is rendered:
//!
//! * the naive node kinds (`Match`, `Join`, `LeftJoin`, `Filter`, `Bind`,
//!   `Group`, `Sort`, `Distinct`, `Slice`, `Project`, `Union`, `Minus`,
//!   `Values`, `AntiJoin`) go back to the algebra they came from, so the
//!   translation of a naive plan is the query it was built from
//!   (`the_naive_plan_round_trips`);
//! * a `SubSelect` barrier is the identity when it exports everything its
//!   input's projection does, and a projection otherwise -- a pruned barrier
//!   is a bag projection;
//! * a `Scan` is `?v a C` plus one triple per read (a required read is the
//!   triple, an absorbed optional read is `OPTIONAL`, a read that binds no
//!   variable is an existence test, an identity restriction is `VALUES`);
//! * an `Unnest` is the multivalued slot's triple, required or `OPTIONAL`;
//! * a pushed condition names slots rather than variables
//!   ([`crate::sparql_refine::Expr::Slot`]), and is written back over the
//!   variable that reads the slot -- the scan's own, or one this translation
//!   introduces and the root projection drops.
//!
//! Nodes the naive plan does not have -- `Scan`, `Unnest`, a slot condition
//! -- get direct tests of their own, evaluated against the matches they
//! replaced, since the round trip never exercises them.

use std::collections::{BTreeMap, HashMap};

use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::SchemaView;

use spargebra::Query;
use spargebra::algebra::{Expression, GraphPattern, OrderExpression};
use spargebra::term::{
    BlankNode, GroundTerm, NamedNode, NamedNodePattern, Term, TermPattern, TriplePattern, Variable,
};

use crate::sparql_refine::{CompareOp, Expr, NodeId, Plan, PlanOp, SlotPresence, SlotReading};
use crate::sparql_scopes::class_at_path_of;

/// The query a plan means: its root translated, as a `SELECT`.
///
/// `None` when a node has no algebra (a `Service`, a query form other than
/// `SELECT`, an opaque expression), which a test treats as a failure to
/// translate rather than as "nothing to check".
pub fn plan_to_query(plan: &Plan, schema: &SchemaView) -> Option<Query> {
    let root = plan.nodes.len().checked_sub(1)?;
    let pattern = plan_to_algebra(plan, schema, root)?;
    Some(Query::Select {
        dataset: None,
        pattern,
        base_iri: None,
    })
}

/// A left join's right side with its match witness: `BIND(true AS ?m)`
/// appended, so `?m` is bound exactly where the right side matched.
pub fn with_witness(right: GraphPattern, witness: &str) -> GraphPattern {
    GraphPattern::Extend {
        inner: Box::new(right),
        variable: Variable::new_unchecked(witness.to_owned()),
        expression: Expression::Literal(spargebra::term::Literal::from(true)),
    }
}

/// The algebra of the subtree rooted at `node`.
pub fn plan_to_algebra(plan: &Plan, schema: &SchemaView, node: NodeId) -> Option<GraphPattern> {
    let mut translation = Translation {
        plan,
        schema,
        bindings: HashMap::new(),
        fresh: 0,
    };
    translation.pattern(node)
}

/// One translation: the plan, the schema (for slot IRIs), and the variable
/// each slot address was written back as.
struct Translation<'a> {
    plan: &'a Plan,
    schema: &'a SchemaView,
    /// `(scan, path)` → the variable that reads it, once the scan or unnest
    /// that binds it has been translated. Filled bottom-up: a scan is
    /// translated before any condition above it. Keyed by the scan node and
    /// not the star's name: two scopes scan one variable as two scans, and
    /// a condition in one must read that scope's own binding -- a name
    /// shared across a sub-select's barrier is unbound inside it.
    bindings: HashMap<(NodeId, Vec<String>), String>,
    fresh: usize,
}

impl Translation<'_> {
    fn fresh_var(&mut self, hint: &str) -> String {
        self.fresh += 1;
        format!("_{}_{}", hint, self.fresh)
    }

    fn predicate(&self, class_uri: &str, slot: &str) -> Option<NamedNode> {
        let class = self.schema.get_class_by_uri(class_uri).ok().flatten()?;
        let slot = class.slot(&Identifier::Name(slot.to_owned()))?;
        let conv = self.schema.converter();
        let iri = slot.canonical_uri().to_uri(&conv).ok()?;
        NamedNode::new(iri.0).ok()
    }

    /// The class at the end of a path of inlined hops, for the predicate of
    /// the next hop.
    fn class_at(&self, class_uri: &str, path: &[String]) -> Option<String> {
        crate::sparql_scopes::class_at_path_of(self.schema, class_uri, path)
    }

    /// The triples that read `path` off `?star` of `class_uri`, binding the
    /// value at its end to `object`. One triple per hop, through blank nodes.
    fn read(
        &self,
        class_uri: &str,
        star: &str,
        path: &[String],
        object: TermPattern,
        counter: &mut usize,
    ) -> Option<Vec<TriplePattern>> {
        let mut triples = Vec::new();
        let mut subject = TermPattern::Variable(Variable::new_unchecked(star.to_owned()));
        let mut class = class_uri.to_owned();
        for (index, hop) in path.iter().enumerate() {
            let predicate = self.predicate(&class, hop)?;
            let last = index + 1 == path.len();
            let next = if last {
                object.clone()
            } else {
                *counter += 1;
                TermPattern::BlankNode(BlankNode::new_unchecked(format!("hop{counter}")))
            };
            triples.push(TriplePattern {
                subject: subject.clone(),
                predicate: NamedNodePattern::NamedNode(predicate),
                object: next.clone(),
            });
            if !last {
                class = self.class_at(&class, &path[..=index])?;
                subject = next;
            }
        }
        Some(triples)
    }

    fn pattern(&mut self, node: NodeId) -> Option<GraphPattern> {
        let plan = self.plan;
        Some(match &plan.nodes[node].op {
            PlanOp::Unit => GraphPattern::Bgp {
                patterns: Vec::new(),
            },
            PlanOp::Match { pattern } => GraphPattern::Bgp {
                patterns: vec![(**pattern).clone()],
            },
            PlanOp::Path {
                subject,
                path,
                object,
            } => GraphPattern::Path {
                subject: subject.clone(),
                path: (**path).clone(),
                object: object.clone(),
            },
            PlanOp::Values { variables, rows } => GraphPattern::Values {
                variables: variables.clone(),
                bindings: rows.clone(),
            },
            PlanOp::Join {
                left,
                right,
                reference,
                ..
            } => {
                let left = self.pattern(*left)?;
                let right = self.pattern(*right)?;
                let right = self.with_edge(right, reference.as_ref(), node)?;
                GraphPattern::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                }
            }
            PlanOp::LeftJoin {
                left,
                right,
                condition,
                reference,
                witness,
                ..
            } => {
                let left = self.pattern(*left)?;
                let right = self.pattern(*right)?;
                let right = self.with_edge(right, reference.as_ref(), node)?;
                let right = match witness {
                    Some(witness) => with_witness(right, witness),
                    None => right,
                };
                let expression = match condition {
                    Some(condition) => Some(self.expression(condition, node)?),
                    None => None,
                };
                GraphPattern::LeftJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    expression,
                }
            }
            PlanOp::AntiJoin { left, right, .. } => {
                let inner = self.pattern(*left)?;
                let block = self.pattern(*right)?;
                GraphPattern::Filter {
                    expr: Expression::Not(Box::new(Expression::Exists(Box::new(block)))),
                    inner: Box::new(inner),
                }
            }
            PlanOp::Union { left, right } => GraphPattern::Union {
                left: Box::new(self.pattern(*left)?),
                right: Box::new(self.pattern(*right)?),
            },
            PlanOp::Minus { left, right } => GraphPattern::Minus {
                left: Box::new(self.pattern(*left)?),
                right: Box::new(self.pattern(*right)?),
            },
            PlanOp::Filter { input, condition } => {
                let inner = self.pattern(*input)?;
                let (inner, expr) = self.condition_over(condition, inner, node)?;
                GraphPattern::Filter {
                    expr,
                    inner: Box::new(inner),
                }
            }
            PlanOp::Bind { input, var, expr } => {
                let inner = self.pattern(*input)?;
                let (inner, expression) = self.condition_over(expr, inner, node)?;
                GraphPattern::Extend {
                    inner: Box::new(inner),
                    variable: Variable::new_unchecked(var.clone()),
                    expression,
                }
            }
            PlanOp::Group {
                input,
                keys,
                measures,
                having,
            } => {
                let inner = self.pattern(*input)?;
                let mut pattern = GraphPattern::Group {
                    inner: Box::new(inner),
                    variables: keys
                        .iter()
                        .map(|k| Variable::new_unchecked(k.clone()))
                        .collect(),
                    aggregates: measures
                        .iter()
                        .map(|m| (Variable::new_unchecked(m.var.clone()), m.aggregate.clone()))
                        .collect(),
                };
                for condition in having {
                    pattern = GraphPattern::Filter {
                        expr: self.expression(condition, node)?,
                        inner: Box::new(pattern),
                    };
                }
                pattern
            }
            PlanOp::Sort { input, terms, .. } => {
                let inner = self.pattern(*input)?;
                let mut expression = Vec::with_capacity(terms.len());
                for term in terms {
                    let expr = self.expression(&term.expr, node)?;
                    expression.push(if term.desc {
                        OrderExpression::Desc(expr)
                    } else {
                        OrderExpression::Asc(expr)
                    });
                }
                GraphPattern::OrderBy {
                    inner: Box::new(inner),
                    expression,
                }
            }
            PlanOp::Distinct { input } => GraphPattern::Distinct {
                inner: Box::new(self.pattern(*input)?),
            },
            PlanOp::Reduced { input } => GraphPattern::Reduced {
                inner: Box::new(self.pattern(*input)?),
            },
            PlanOp::Slice {
                input,
                limit,
                offset,
            } => GraphPattern::Slice {
                inner: Box::new(self.pattern(*input)?),
                start: *offset,
                length: *limit,
            },
            PlanOp::Project { input, vars } => GraphPattern::Project {
                inner: Box::new(self.pattern(*input)?),
                variables: vars
                    .iter()
                    .map(|v| Variable::new_unchecked(v.clone()))
                    .collect(),
            },
            // The barrier: the identity when it exports everything its
            // input can bind, a projection otherwise.
            PlanOp::SubSelect { input, vars, .. } => {
                let inner = self.pattern(*input)?;
                let outputs = plan.variables_of(*input);
                if outputs.iter().all(|var| vars.contains(var)) {
                    inner
                } else {
                    GraphPattern::Project {
                        inner: Box::new(inner),
                        variables: vars
                            .iter()
                            .map(|v| Variable::new_unchecked(v.clone()))
                            .collect(),
                    }
                }
            }
            PlanOp::Graph { input, name } => GraphPattern::Graph {
                name: graph_name(name)?,
                inner: Box::new(self.pattern(*input)?),
            },
            PlanOp::Scan {
                star_var,
                class_uri,
                slots,
                identifier_values,
            } => {
                let type_iri = NamedNode::new(crate::sparql_scoper::RDF_TYPE).ok()?;
                let mut required: Vec<TriplePattern> = vec![TriplePattern {
                    subject: TermPattern::Variable(Variable::new_unchecked(star_var.clone())),
                    predicate: NamedNodePattern::NamedNode(type_iri),
                    object: TermPattern::NamedNode(NamedNode::new(class_uri.clone()).ok()?),
                }];
                let mut optional: Vec<Vec<TriplePattern>> = Vec::new();
                let mut exists: Vec<Vec<TriplePattern>> = Vec::new();
                let mut counter = 0usize;
                // A read *through* an unnested element -- a path whose
                // prefix is a multivalued slot this scan fans out -- is read
                // off the element's variable at the fan-out, not here: a
                // fresh blank node here would be some element, not that one.
                let through_element = |slot: &crate::sparql_refine::ScanSlot| {
                    slot.path.len() > 1
                        && slots.iter().any(|other| {
                            other.multivalued
                                && other.var.is_some()
                                && slot.path.starts_with(&other.path)
                        })
                };
                for slot in slots {
                    if through_element(slot) {
                        continue;
                    }
                    match (&slot.var, slot.presence, slot.multivalued) {
                        // A delivered read binds nothing and requires nothing,
                        // and an absorbed optional collection is bound by its
                        // optional fan-out alone.
                        (None, SlotPresence::Optional, _)
                        | (Some(_), SlotPresence::Optional, true) => {}
                        // The fan-out binds a multivalued variable; the scan
                        // itself only requires the collection to be there.
                        (Some(_), SlotPresence::Required, true)
                        | (None, SlotPresence::Required, true) => {
                            counter += 1;
                            let object = TermPattern::BlankNode(BlankNode::new_unchecked(format!(
                                "exists{counter}"
                            )));
                            exists.push(self.read(
                                class_uri,
                                star_var,
                                &slot.path,
                                object,
                                &mut counter,
                            )?);
                        }
                        (Some(var), SlotPresence::Required, false) => {
                            self.bindings.insert((node, slot.path.clone()), var.clone());
                            required.extend(self.read(
                                class_uri,
                                star_var,
                                &slot.path,
                                TermPattern::Variable(Variable::new_unchecked(var.clone())),
                                &mut counter,
                            )?);
                        }
                        (Some(var), SlotPresence::Optional, false) => {
                            self.bindings.insert((node, slot.path.clone()), var.clone());
                            optional.push(self.read(
                                class_uri,
                                star_var,
                                &slot.path,
                                TermPattern::Variable(Variable::new_unchecked(var.clone())),
                                &mut counter,
                            )?);
                        }
                        // The existence half of `?s :name "X"`: read into a
                        // variable of this translation's own, which a
                        // condition above may name and the root projection
                        // drops.
                        (None, SlotPresence::Required, false) => {
                            let var =
                                self.fresh_var(&format!("{star_var}_{}", slot.path.join("_")));
                            self.bindings.insert((node, slot.path.clone()), var.clone());
                            required.extend(self.read(
                                class_uri,
                                star_var,
                                &slot.path,
                                TermPattern::Variable(Variable::new_unchecked(var)),
                                &mut counter,
                            )?);
                        }
                    }
                }
                let mut pattern = GraphPattern::Bgp { patterns: required };
                for triples in exists {
                    pattern = GraphPattern::Filter {
                        expr: Expression::Exists(Box::new(GraphPattern::Bgp { patterns: triples })),
                        inner: Box::new(pattern),
                    };
                }
                for triples in optional {
                    pattern = GraphPattern::LeftJoin {
                        left: Box::new(pattern),
                        right: Box::new(GraphPattern::Bgp { patterns: triples }),
                        expression: None,
                    };
                }
                if !identifier_values.is_empty() {
                    let variable = Variable::new_unchecked(star_var.clone());
                    let bindings = identifier_values
                        .iter()
                        .map(|value| {
                            vec![Some(GroundTerm::NamedNode(NamedNode::new_unchecked(
                                value.clone(),
                            )))]
                        })
                        .collect();
                    pattern = GraphPattern::Join {
                        left: Box::new(pattern),
                        right: Box::new(GraphPattern::Values {
                            variables: vec![variable],
                            bindings,
                        }),
                    };
                }
                pattern
            }
            PlanOp::Unnest {
                input,
                star_var,
                slot_path,
                var,
                presence,
            } => {
                let inner = self.pattern(*input)?;
                let scan = self.scan_of(star_var, node)?;
                let class_uri = self.class_of_scan(star_var, node)?;
                let mut counter = 100;
                // A nested fan-out reads off the outer element's variable,
                // not off the record through a fresh blank node.
                let (from_var, from_class, rest) = (1..slot_path.len())
                    .rev()
                    .find_map(|cut| {
                        self.bindings
                            .get(&(scan, slot_path[..cut].to_vec()))
                            .cloned()
                            .and_then(|bound| {
                                class_at_path_of(self.schema, &class_uri, &slot_path[..cut])
                                    .map(|class| (bound, class, slot_path[cut..].to_vec()))
                            })
                    })
                    .unwrap_or((star_var.clone(), class_uri.clone(), slot_path.clone()));
                let triples = self.read(
                    &from_class,
                    &from_var,
                    &rest,
                    TermPattern::Variable(Variable::new_unchecked(var.clone())),
                    &mut counter,
                )?;
                self.bindings.insert((scan, slot_path.clone()), var.clone());
                let read = GraphPattern::Bgp { patterns: triples };
                let mut pattern = match presence {
                    SlotPresence::Required => GraphPattern::Join {
                        left: Box::new(inner),
                        right: Box::new(read),
                    },
                    SlotPresence::Optional => GraphPattern::LeftJoin {
                        left: Box::new(inner),
                        right: Box::new(read),
                        expression: None,
                    },
                };
                // The scan's reads through this element, off its variable.
                let element_class = class_at_path_of(self.schema, &class_uri, slot_path);
                let element_reads: Vec<(Vec<String>, Option<String>, SlotPresence)> = self
                    .scan_slots_of(star_var, node)
                    .into_iter()
                    .filter(|(path, _, _)| {
                        path.len() > slot_path.len() && path.starts_with(slot_path)
                    })
                    .collect();
                for (path, bound, read_presence) in element_reads {
                    let Some(element_class) = &element_class else {
                        return None;
                    };
                    let target = match &bound {
                        Some(bound) => bound.clone(),
                        None => {
                            self.fresh_var(&format!("{var}_{}", path[slot_path.len()..].join("_")))
                        }
                    };
                    self.bindings.insert((scan, path.clone()), target.clone());
                    let mut counter = 400 + self.fresh;
                    let triples = self.read(
                        element_class,
                        var,
                        &path[slot_path.len()..],
                        TermPattern::Variable(Variable::new_unchecked(target)),
                        &mut counter,
                    )?;
                    let read = GraphPattern::Bgp { patterns: triples };
                    pattern = match read_presence {
                        SlotPresence::Required => GraphPattern::Join {
                            left: Box::new(pattern),
                            right: Box::new(read),
                        },
                        SlotPresence::Optional => GraphPattern::LeftJoin {
                            left: Box::new(pattern),
                            right: Box::new(read),
                            expression: None,
                        },
                    };
                }
                pattern
            }
            // Row numbering has no algebra: the finish query reads the
            // ordinal as a column of the rows it is handed, never computes
            // it (see `crate::sparql_lift`).
            PlanOp::Service { .. }
            | PlanOp::Number { .. }
            | PlanOp::Construct { .. }
            | PlanOp::Describe { .. }
            | PlanOp::Ask { .. } => return None,
        })
    }

    /// A join's recorded reference edge, as the triple it stands for:
    /// `?holder <slot> ?referenced`, joined into the side the edge
    /// correlates. The holder's read is a delivered slot of its scan, which
    /// binds nothing, so without the triple the join would be a product.
    fn with_edge(
        &self,
        side: GraphPattern,
        edge: Option<&crate::sparql_refine::ReferenceEdge>,
        at: NodeId,
    ) -> Option<GraphPattern> {
        let Some(edge) = edge else {
            return Some(side);
        };
        let class_uri = self.class_of_scan(&edge.holder, at)?;
        // An element-held edge reads the key off the element the holder's
        // unnest binds: `?element <slot> ?referenced`, the element's own
        // triple, where a record's is `?holder <slot> ?referenced`.
        let (subject, class_uri) = if edge.path.is_empty() {
            (edge.holder.clone(), class_uri)
        } else {
            let element =
                self.plan
                    .nodes
                    .iter()
                    .enumerate()
                    .find_map(|(id, node)| match &node.op {
                        PlanOp::Unnest {
                            star_var,
                            slot_path,
                            var,
                            ..
                        } if *star_var == edge.holder
                            && *slot_path == edge.path
                            && self.plan.feeds(id, at) =>
                        {
                            Some(var.clone())
                        }
                        _ => None,
                    })?;
            (element, self.class_at(&class_uri, &edge.path)?)
        };
        let predicate = self.predicate(&class_uri, &edge.slot)?;
        let triple = TriplePattern {
            subject: TermPattern::Variable(Variable::new_unchecked(subject)),
            predicate: NamedNodePattern::NamedNode(predicate),
            object: TermPattern::Variable(Variable::new_unchecked(edge.referenced.clone())),
        };
        Some(GraphPattern::Join {
            left: Box::new(side),
            right: Box::new(GraphPattern::Bgp {
                patterns: vec![triple],
            }),
        })
    }

    /// The slots of the scan of `star` visible from `at`: `(path, variable,
    /// presence)`.
    fn scan_slots_of(
        &self,
        star: &str,
        at: NodeId,
    ) -> Vec<(Vec<String>, Option<String>, SlotPresence)> {
        self.plan
            .nodes
            .iter()
            .enumerate()
            .find_map(|(id, node)| match &node.op {
                PlanOp::Scan {
                    star_var, slots, ..
                } if star_var == star && self.plan.feeds_visibly(id, at) => Some(
                    slots
                        .iter()
                        .map(|slot| (slot.path.clone(), slot.var.clone(), slot.presence))
                        .collect(),
                ),
                _ => None,
            })
            .unwrap_or_default()
    }

    /// The scan of `star` visible from `at`.
    fn scan_of(&self, star: &str, at: NodeId) -> Option<NodeId> {
        self.plan
            .nodes
            .iter()
            .enumerate()
            .find_map(|(id, node)| match &node.op {
                PlanOp::Scan { star_var, .. }
                    if star_var == star && self.plan.feeds_visibly(id, at) =>
                {
                    Some(id)
                }
                _ => None,
            })
    }

    /// The class the scan of `star` visible from `at` reads.
    fn class_of_scan(&self, star: &str, at: NodeId) -> Option<String> {
        self.plan
            .nodes
            .iter()
            .enumerate()
            .find_map(|(id, node)| match &node.op {
                PlanOp::Scan {
                    star_var,
                    class_uri,
                    ..
                } if star_var == star && self.plan.feeds_visibly(id, at) => Some(class_uri.clone()),
                _ => None,
            })
    }

    /// A condition over `inner`: the expression with every slot address
    /// written back as a variable, and `inner` extended with whatever reads
    /// those variables need (an element's slot, an any-element test).
    fn condition_over(
        &mut self,
        condition: &Expr,
        inner: GraphPattern,
        at: NodeId,
    ) -> Option<(GraphPattern, Expression)> {
        let mut reads: Vec<TriplePattern> = Vec::new();
        let expr = self.rewrite(condition, at, &mut reads)?;
        let inner = if reads.is_empty() {
            inner
        } else {
            GraphPattern::Join {
                left: Box::new(inner),
                right: Box::new(GraphPattern::Bgp { patterns: reads }),
            }
        };
        Some((inner, expr))
    }

    fn expression(&mut self, condition: &Expr, at: NodeId) -> Option<Expression> {
        let mut reads = Vec::new();
        let expr = self.rewrite(condition, at, &mut reads)?;
        reads.is_empty().then_some(expr)
    }

    /// `condition` with each `Expr::Slot` replaced by the variable that
    /// reads it, adding the triples that bind a variable nothing bound yet.
    fn rewrite(
        &mut self,
        condition: &Expr,
        at: NodeId,
        reads: &mut Vec<TriplePattern>,
    ) -> Option<Expression> {
        let rewritten = self.substitute(condition, at, reads)?;
        rewritten.try_as_expression_lossy()
    }

    fn substitute(
        &mut self,
        condition: &Expr,
        at: NodeId,
        reads: &mut Vec<TriplePattern>,
    ) -> Option<Expr> {
        let all = |this: &mut Self,
                   parts: &[Expr],
                   reads: &mut Vec<TriplePattern>|
         -> Option<Vec<Expr>> {
            parts
                .iter()
                .map(|part| this.substitute(part, at, reads))
                .collect()
        };
        Some(match condition {
            Expr::Slot {
                star_var,
                slot_path,
                reading,
                ..
            } => {
                let var = self.variable_for(star_var, slot_path, *reading, at, reads)?;
                Expr::Var(var)
            }
            Expr::Var(_) | Expr::Literal(_) | Expr::Opaque(_) | Expr::InClass { .. } => {
                condition.clone()
            }
            Expr::Compare { op, left, right } => Expr::Compare {
                op: *op,
                left: Box::new(self.substitute(left, at, reads)?),
                right: Box::new(self.substitute(right, at, reads)?),
            },
            Expr::In { value, candidates } => Expr::In {
                value: Box::new(self.substitute(value, at, reads)?),
                candidates: all(self, candidates, reads)?,
            },
            Expr::And(parts) => Expr::And(all(self, parts, reads)?),
            Expr::Or(parts) => Expr::Or(all(self, parts, reads)?),
            Expr::Not(inner) => Expr::Not(Box::new(self.substitute(inner, at, reads)?)),
            Expr::Function { name, args } => Expr::Function {
                name: name.clone(),
                args: all(self, args, reads)?,
            },
        })
    }

    /// The variable that reads `(star, path)` with `reading`, binding one
    /// through `reads` when nothing has.
    fn variable_for(
        &mut self,
        star: &str,
        path: &[String],
        reading: SlotReading,
        at: NodeId,
        reads: &mut Vec<TriplePattern>,
    ) -> Option<String> {
        let scan = self.scan_of(star, at)?;
        if let Some(var) = self.bindings.get(&(scan, path.to_vec())) {
            // A binding is reused only where it is still in scope: a
            // projection or a grouping between the scan and `at` drops it,
            // and a condition written on the dropped name tests unbound. A
            // single-valued slot read again is the same triple joined
            // again, so a fresh read is the same answer; a bound element
            // has no second read (it is the fan-out's row) and is reused
            // where it was bound.
            let visible = self.plan.nodes[at]
                .op
                .inputs()
                .iter()
                .any(|input| self.plan.variables_of(*input).contains(var));
            if visible || reading != SlotReading::Column || at == scan {
                return Some(var.clone());
            }
        }
        let class_uri = self.class_of_scan(star, at)?;
        match reading {
            SlotReading::Column => {
                let var = self.fresh_var(&format!("{star}_{}", path.join("_")));
                let mut counter = 200 + self.fresh;
                reads.extend(self.read(
                    &class_uri,
                    star,
                    path,
                    TermPattern::Variable(Variable::new_unchecked(var.clone())),
                    &mut counter,
                )?);
                self.bindings.insert((scan, path.to_vec()), var.clone());
                Some(var)
            }
            // A read *through* the element the fan-out bound: the longest
            // bound prefix is the element, and the rest is read off it.
            SlotReading::BoundElement => {
                let (prefix, element) = (0..path.len()).rev().find_map(|cut| {
                    self.bindings
                        .get(&(scan, path[..cut].to_vec()))
                        .map(|var| (cut, var.clone()))
                })?;
                let element_class = self.class_at(&class_uri, &path[..prefix])?;
                let var = self.fresh_var(&format!("{element}_{}", path[prefix..].join("_")));
                let mut counter = 300 + self.fresh;
                reads.extend(self.read(
                    &element_class,
                    &element,
                    &path[prefix..],
                    TermPattern::Variable(Variable::new_unchecked(var.clone())),
                    &mut counter,
                )?);
                self.bindings.insert((scan, path.to_vec()), var.clone());
                Some(var)
            }
            // Some element of the collection: a test the statement makes
            // with `EXISTS`, and which this translation cannot spell as a
            // variable. Declined; the grammar keeps such conditions where
            // the fan-out is below them.
            SlotReading::AnyElement => None,
        }
    }
}

impl Expr {
    /// [`Expr::try_as_expression`] for a translation that has already
    /// replaced every slot: identical, with no feature gate on the caller.
    fn try_as_expression_lossy(&self) -> Option<Expression> {
        let pair = |left: &Expr, right: &Expr| -> Option<(Box<Expression>, Box<Expression>)> {
            Some((
                Box::new(left.try_as_expression_lossy()?),
                Box::new(right.try_as_expression_lossy()?),
            ))
        };
        let fold = |parts: &[Expr], join: fn(Box<Expression>, Box<Expression>) -> Expression| {
            let mut parts = parts.iter();
            let mut out = parts.next()?.try_as_expression_lossy()?;
            for part in parts {
                out = join(Box::new(out), Box::new(part.try_as_expression_lossy()?));
            }
            Some(out)
        };
        Some(match self {
            Self::Var(name) => Expression::Variable(Variable::new_unchecked(name.clone())),
            Self::Literal(term) => match term {
                Term::NamedNode(node) => Expression::NamedNode(node.clone()),
                Term::Literal(literal) => Expression::Literal(literal.clone()),
                _ => return None,
            },
            Self::Compare { op, left, right } => {
                let (left, right) = pair(left, right)?;
                match op {
                    CompareOp::Eq => Expression::Equal(left, right),
                    CompareOp::Ne => Expression::Not(Box::new(Expression::Equal(left, right))),
                    CompareOp::Gt => Expression::Greater(left, right),
                    CompareOp::Gte => Expression::GreaterOrEqual(left, right),
                    CompareOp::Lt => Expression::Less(left, right),
                    CompareOp::Lte => Expression::LessOrEqual(left, right),
                }
            }
            Self::In { value, candidates } => Expression::In(
                Box::new(value.try_as_expression_lossy()?),
                candidates
                    .iter()
                    .map(Self::try_as_expression_lossy)
                    .collect::<Option<Vec<_>>>()?,
            ),
            Self::And(parts) => fold(parts, Expression::And)?,
            Self::Or(parts) => fold(parts, Expression::Or)?,
            Self::Not(inner) => Expression::Not(Box::new(inner.try_as_expression_lossy()?)),
            Self::Function { name, args } => {
                let arguments = args
                    .iter()
                    .map(Self::try_as_expression_lossy)
                    .collect::<Option<Vec<_>>>()?;
                crate::sparql_refine::function_expression_of(name, arguments)?
            }
            Self::InClass { .. } => self.try_as_expression()?,
            Self::Slot { .. } | Self::Opaque(_) => return None,
        })
    }
}

/// A `PlanOp::Graph`'s name, back as the pattern it was rendered from.
fn graph_name(name: &str) -> Option<NamedNodePattern> {
    match name.strip_prefix('?') {
        Some(variable) => Some(NamedNodePattern::Variable(Variable::new(variable).ok()?)),
        None => {
            let iri = name.strip_prefix('<')?.strip_suffix('>')?;
            Some(NamedNodePattern::NamedNode(NamedNode::new(iri).ok()?))
        }
    }
}

/// A bag of solution mappings over RDF terms: what two evaluations are
/// compared as. Unbound is absence, multiplicity counts, `"1"` and `"1.0"`
/// differ.
pub type Bag = BTreeMap<BTreeMap<String, String>, usize>;

/// The variables a bag binds anywhere.
pub fn bag_variables(bag: &Bag) -> std::collections::BTreeSet<String> {
    bag.keys().flat_map(|row| row.keys().cloned()).collect()
}

/// A bag with the variables of this translation's own making dropped, for a
/// comparison at a node below the root projection.
pub fn without_synthetic(bag: &Bag) -> Bag {
    let mut out = Bag::new();
    for (row, count) in bag {
        let row: BTreeMap<String, String> = row
            .iter()
            .filter(|(var, _)| !var.starts_with('_'))
            .map(|(var, term)| (var.clone(), term.clone()))
            .collect();
        *out.entry(row).or_insert(0) += count;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparql_scoper::tests::test_schema_view;

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

    /// The naive plan is the algebra faithfully, so its translation is the
    /// query it came from -- the translation's own test, for the nodes a
    /// naive plan has.
    #[test]
    fn the_naive_plan_round_trips() {
        let schema = test_schema_view();
        for query in [
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm }",
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:name ?nm } }",
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:name ?nm . \
             FILTER(?nm > \"A\") } }",
            "SELECT ?nm ?n WHERE { ?s a asset360:Signal ; asset360:name ?nm . \
             { SELECT ?s (COUNT(?d) AS ?n) WHERE { ?s a asset360:Signal ; \
             asset360:documents ?d } GROUP BY ?s } }",
            "SELECT ?s WHERE { { SELECT DISTINCT ?s WHERE { ?s a asset360:Signal } \
             ORDER BY ?s LIMIT 3 OFFSET 1 } }",
            "SELECT ?s WHERE { { ?s a asset360:Signal } UNION { ?s a asset360:Track } }",
            "SELECT ?s WHERE { ?s a asset360:Signal . FILTER NOT EXISTS { ?s asset360:name \"X\" } }",
            "SELECT ?s ?n WHERE { ?s a asset360:Signal . VALUES ?n { \"a\" \"b\" } }",
            "SELECT ?s (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal ; asset360:trafficKinds ?k } \
             GROUP BY ?s HAVING (COUNT(*) > 1) ORDER BY DESC(?n)",
            "SELECT ?s ?d WHERE { ?s a asset360:Signal . BIND(?s AS ?d) }",
        ] {
            let parsed = crate::sparql_scoper::parse_query(&format!("{PREFIX}{query}")).unwrap();
            let plan = crate::sparql_refine::naive_plan(&parsed).unwrap();
            let translated =
                plan_to_query(&plan, &schema).unwrap_or_else(|| panic!("{query}\n{plan}"));
            // Structurally the same up to how a basic graph pattern is
            // split into matches and a conjunction into filters -- which
            // is the naive plan's own spelling -- and identical in what it
            // answers.
            assert_eq!(
                normalise(pattern_of(&translated)),
                normalise(pattern_of(&parsed)),
                "{query}\n{plan}"
            );
            #[cfg(feature = "sparql-endpoint")]
            {
                let oracle = crate::sparql_oracle::fixture(&schema);
                assert_eq!(
                    oracle.answers(translated),
                    oracle.answers(parsed),
                    "{query}\n{plan}"
                );
            }
        }
    }

    fn pattern_of(query: &Query) -> &GraphPattern {
        match query {
            Query::Select { pattern, .. }
            | Query::Construct { pattern, .. }
            | Query::Describe { pattern, .. }
            | Query::Ask { pattern, .. } => pattern,
        }
    }

    /// Basic graph patterns joined are one basic graph pattern, and nested
    /// filters are one conjunction.
    fn normalise(pattern: &GraphPattern) -> GraphPattern {
        match pattern {
            GraphPattern::Join { left, right } => match (normalise(left), normalise(right)) {
                (GraphPattern::Bgp { patterns: mut a }, GraphPattern::Bgp { patterns: b }) => {
                    a.extend(b);
                    GraphPattern::Bgp { patterns: a }
                }
                (left, right) => GraphPattern::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                },
            },
            GraphPattern::Filter { expr, inner } => match normalise(inner) {
                GraphPattern::Filter { expr: below, inner } => GraphPattern::Filter {
                    expr: Expression::And(Box::new(below), Box::new(expr.clone())),
                    inner,
                },
                inner => GraphPattern::Filter {
                    expr: expr.clone(),
                    inner: Box::new(inner),
                },
            },
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => GraphPattern::LeftJoin {
                left: Box::new(normalise(left)),
                right: Box::new(normalise(right)),
                expression: expression.clone(),
            },
            GraphPattern::Union { left, right } => GraphPattern::Union {
                left: Box::new(normalise(left)),
                right: Box::new(normalise(right)),
            },
            GraphPattern::Minus { left, right } => GraphPattern::Minus {
                left: Box::new(normalise(left)),
                right: Box::new(normalise(right)),
            },
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => GraphPattern::Extend {
                inner: Box::new(normalise(inner)),
                variable: variable.clone(),
                expression: expression.clone(),
            },
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => GraphPattern::Group {
                inner: Box::new(normalise(inner)),
                variables: variables.clone(),
                aggregates: aggregates.clone(),
            },
            GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
                inner: Box::new(normalise(inner)),
                expression: expression.clone(),
            },
            GraphPattern::Project { inner, variables } => GraphPattern::Project {
                inner: Box::new(normalise(inner)),
                variables: variables.clone(),
            },
            GraphPattern::Distinct { inner } => GraphPattern::Distinct {
                inner: Box::new(normalise(inner)),
            },
            GraphPattern::Reduced { inner } => GraphPattern::Reduced {
                inner: Box::new(normalise(inner)),
            },
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => GraphPattern::Slice {
                inner: Box::new(normalise(inner)),
                start: *start,
                length: *length,
            },
            other => other.clone(),
        }
    }
}

/// **Test 2 of the design: every rewrite, evaluated.** The property grammar,
/// the per-rewrite oracle, and the direct tests for the nodes a naive plan
/// does not have.
#[cfg(all(test, feature = "sparql-endpoint"))]
pub(crate) mod equivalence {
    use super::*;
    use crate::sparql_oracle::{Oracle, fixture};
    use crate::sparql_rules::{Rule, tier_one_rules};
    use crate::sparql_scoper::tests::test_schema_view;

    const PREFIX: &str = "PREFIX asset360: <https://data.infrabel.be/asset360/> ";

    /// Bags compared with a blank node standing for "some blank node" -- the
    /// oracle already spells one so -- and the rows of the query's own
    /// variables only.
    fn answers(oracle: &Oracle, query: &Query) -> Bag {
        without_synthetic(&oracle.answers(query.clone()))
    }

    /// Refine one query one application at a time, translating the plan
    /// before and after each and holding both to the oracle. Returns the
    /// refined plan.
    pub(crate) fn each_rewrite_preserves_answers(
        query: &str,
        schema: &SchemaView,
        oracle: &Oracle,
        rules: &[&dyn Rule],
    ) -> Plan {
        let text = format!("{PREFIX}{query}");
        let parsed = crate::sparql_scoper::parse_query(&text).unwrap();
        let expected = answers(oracle, &parsed);
        let mut plan =
            crate::sparql_refine::naive_plan(&parsed).unwrap_or_else(|e| panic!("{query}: {e}"));
        let naive = plan_to_query(&plan, schema).unwrap_or_else(|| panic!("{query}\n{plan}"));
        assert_eq!(answers(oracle, &naive), expected, "naive: {query}\n{plan}");

        let mut applications = 0;
        let mut changed = true;
        while changed {
            changed = false;
            for rule in rules {
                let before = plan.clone();
                let kept = plan.kept_exports();
                if !rule.apply(&mut plan) {
                    continue;
                }
                changed = true;
                applications += 1;
                assert!(applications < 64, "no fixpoint: {query}\n{plan}");
                plan.check_transition(&kept)
                    .unwrap_or_else(|defect| panic!("{}: {defect}\n{query}\n{plan}", rule.name()));
                plan.check_with(schema)
                    .unwrap_or_else(|defect| panic!("{}: {defect}\n{query}\n{plan}", rule.name()));
                let before_q = plan_to_query(&before, schema).unwrap_or_else(|| {
                    panic!("untranslatable before {}: {query}\n{before}", rule.name())
                });
                let after_q = plan_to_query(&plan, schema).unwrap_or_else(|| {
                    panic!("untranslatable after {}: {query}\n{plan}", rule.name())
                });
                let (before_a, after_a) = (answers(oracle, &before_q), answers(oracle, &after_q));
                assert_eq!(
                    before_a,
                    after_a,
                    "{} changed the answer\n{query}\nbefore:\n{before}\nafter:\n{plan}\n{before_q}\n{after_q}",
                    rule.name()
                );
                assert_eq!(
                    after_a,
                    expected,
                    "{}: drifted from the original\n{query}\n{plan}",
                    rule.name()
                );
            }
        }
        plan
    }

    /// The grammar of the design's test 2: bodies from a driving class, its
    /// reads, an array hop (scalar or structure), a reference hop with an
    /// optional nested read, a grouping on the identity, an optionally bound
    /// variable or nothing, an ordering with a slice, a shadowing variable,
    /// one or two shared variables, an export nothing names under the
    /// whole-mapping observers, a two-level array hop, and one level of
    /// nesting; each placed as a mandatory sub-select, an `OPTIONAL` body,
    /// or `OPTIONAL { { SELECT … } }`. Small enough to enumerate.
    fn grammar() -> Vec<String> {
        let mut out: Vec<String> = Vec::new();

        // The bodies a sub-select may have, each with its projection list.
        // `(select list, where body)`.
        let bodies: Vec<(&str, &str)> = vec![
            (
                "?s (COUNT(?k) AS ?n)",
                "?s a asset360:Signal ; asset360:trafficKinds ?k } GROUP BY ?s",
            ),
            (
                "?s (COUNT(?d) AS ?n)",
                "?s a asset360:Signal ; asset360:documents ?d } GROUP BY ?s",
            ),
            (
                "?s (MIN(?len) AS ?n)",
                "?s a asset360:Signal ; asset360:length ?len } GROUP BY ?s",
            ),
            (
                "?s (MAX(?len) AS ?n)",
                "?s a asset360:Signal ; asset360:length ?len } GROUP BY ?s",
            ),
            ("?s ?nm", "?s a asset360:Signal ; asset360:name ?nm }"),
            ("?s", "?s a asset360:Signal } ORDER BY ?s LIMIT 2"),
            (
                "?s",
                "?s a asset360:Signal ; asset360:length ?len } ORDER BY ?len ?s LIMIT 2 OFFSET 1",
            ),
            (
                "?s ?tn",
                "?s a asset360:Signal ; asset360:locatedOnTrack ?tr . ?tr a asset360:Track ; asset360:hasName ?tn }",
            ),
            (
                "?s ?tn",
                "?s a asset360:Signal . OPTIONAL { ?s asset360:locatedOnTrack ?tr . ?tr a asset360:Track ; asset360:hasName ?tn } }",
            ),
            (
                "?tn (COUNT(*) AS ?n)",
                "?s a asset360:Signal . OPTIONAL { ?s asset360:locatedOnTrack ?tr . ?tr a asset360:Track ; asset360:hasName ?tn } } GROUP BY ?tn",
            ),
            ("(COUNT(*) AS ?n)", "?s a asset360:Signal }"),
            // A shadowing variable: `?nm` inside is not the outer `?nm`.
            (
                "?s",
                "?s a asset360:Signal ; asset360:name ?nm . FILTER(?nm = \"Alpha\") }",
            ),
            // The structure hop with element reads.
            (
                "?s ?t",
                "?s a asset360:Signal ; asset360:documents ?d . ?d asset360:title ?t }",
            ),
            (
                "?s (COUNT(?d) AS ?n)",
                "?s a asset360:Signal ; asset360:documents ?d . ?d asset360:title \"One\" } GROUP BY ?s",
            ),
            // A two-level array hop.
            (
                "?a (COUNT(?c) AS ?n)",
                "?a a asset360:Assembly ; asset360:parts ?p . ?p asset360:children ?c } GROUP BY ?a",
            ),
            (
                "?a ?l",
                "?a a asset360:Assembly ; asset360:parts ?p . ?p asset360:children ?c . ?c asset360:label ?l }",
            ),
            (
                "?s ?nm ?len",
                "?s a asset360:Signal ; asset360:name ?nm . OPTIONAL { ?s asset360:length ?len } }",
            ),
            (
                "?s ?k",
                "?s a asset360:Signal . OPTIONAL { ?s asset360:trafficKinds ?k } }",
            ),
            (
                "DISTINCT ?s",
                "?s a asset360:Signal ; asset360:trafficKinds ?k }",
            ),
        ];
        // How the outside uses the sub-select's exports.
        let outers: Vec<(&str, &str, &str)> = vec![
            // (select, before, after) — placed as a mandatory sub-select.
            ("?s ?n", "?s a asset360:Signal ; asset360:name ?nm . ", ""),
            ("?nm ?n", "?s a asset360:Signal ; asset360:name ?nm . ", ""),
            ("?s ?n ?tn", "?s a asset360:Signal . ", ""),
            ("*", "?s a asset360:Signal . ", ""),
            ("(COUNT(DISTINCT *) AS ?c)", "", ""),
            ("DISTINCT ?s", "?s a asset360:Signal . ", ""),
            ("REDUCED ?n", "?s a asset360:Signal . ", ""),
            ("?s ?n", "?s a asset360:Signal . ", " FILTER(?n > 1)"),
            ("?s ?nm", "", " ?s asset360:name ?nm ."),
            // An outer row test on the shared identity, as a constant and
            // as a `FILTER`: op 3a carries it into a typed body.
            ("?s ?n", "?s a asset360:Signal ; asset360:length 3 . ", ""),
            (
                "?s ?n",
                "?s a asset360:Signal ; asset360:length ?l . FILTER(?l > 2) ",
                "",
            ),
        ];
        for (select, body) in &bodies {
            for (outer_select, before, after) in &outers {
                // Mandatory.
                out.push(format!(
                    "SELECT {outer_select} WHERE {{ {before}{{ SELECT {select} WHERE {{ {body} }}{after} }}"
                ));
                // Optional.
                if !before.is_empty() {
                    out.push(format!(
                        "SELECT {outer_select} WHERE {{ {before}OPTIONAL {{ {{ SELECT {select} WHERE {{ {body} }} }}{after} }}"
                    ));
                }
            }
        }
        // Optional bodies without a sub-select: the shapes the absorb rules
        // serve and the ones beyond them.
        for body in [
            "?s asset360:name ?nm",
            "?s asset360:trafficKinds ?k",
            "?s asset360:locatedOnTrack ?tr . ?tr a asset360:Track ; asset360:hasName ?tn",
            "?s asset360:locatedOnTrack ?tr . ?tr a asset360:Track ; asset360:hasName ?tn . OPTIONAL { ?tr asset360:rsmName ?rn }",
            "?s asset360:locatedOnTrack ?tr . ?tr a asset360:Track ; asset360:hasName \"Main\"",
            "?s asset360:documents ?d . ?d asset360:title ?t",
            // A constant on the unnested element, alone and beside a read.
            "?s asset360:documents ?d . ?d asset360:title \"One\"",
            "?s asset360:documents ?d . ?d asset360:title \"One\" ; asset360:docId ?id",
            "?s a asset360:Signal ; asset360:length ?len . FILTER(?len > 2)",
            "?s asset360:name ?nm . FILTER(?nm = \"Alpha\")",
        ] {
            for select in ["*", "?s", "?s ?nm"] {
                out.push(format!(
                    "SELECT {select} WHERE {{ ?s a asset360:Signal ; asset360:length ?len . OPTIONAL {{ {body} }} }}"
                ));
            }
        }
        // One level of nesting: a generated body inside another.
        out.push(
            "SELECT ?s ?nx ?ny WHERE { { SELECT ?s ?nx ?ny WHERE { \
             { SELECT ?s (COUNT(?k) AS ?nx) WHERE { ?s a asset360:Signal ; asset360:trafficKinds ?k } GROUP BY ?s } \
             { SELECT ?s (COUNT(?d) AS ?ny) WHERE { ?s a asset360:Signal ; asset360:documents ?d } GROUP BY ?s } } } \
             ?s a asset360:Signal }"
                .to_owned(),
        );
        out.push(
            "SELECT ?s ?n WHERE { ?s a asset360:Signal . \
             { SELECT ?s (COUNT(*) AS ?n) WHERE { { SELECT ?s ?k WHERE { ?s a asset360:Signal ; asset360:trafficKinds ?k } } } GROUP BY ?s } }"
                .to_owned(),
        );
        // Whole-mapping observers over an exported column nothing names.
        out.push(
            "SELECT (COUNT(DISTINCT *) AS ?n) WHERE { { SELECT ?x ?y WHERE { VALUES (?x ?y) { (1 10) (1 20) } } } }"
                .to_owned(),
        );
        out.push(
            "SELECT DISTINCT ?x WHERE { { SELECT ?x ?y WHERE { VALUES (?x ?y) { (1 10) (1 20) } } } }"
                .to_owned(),
        );
        out.push(
            "SELECT REDUCED ?x WHERE { { SELECT ?x ?y WHERE { VALUES (?x ?y) { (1 10) (1 20) } } } }"
                .to_owned(),
        );
        // The table's rows the design pins.
        out.push(
            "SELECT ?nm ?total WHERE { { SELECT (COUNT(*) AS ?total) WHERE { ?t a asset360:Track } } ?s a asset360:Signal ; asset360:name ?nm }"
                .to_owned(),
        );
        out.push(
            "SELECT ?nm WHERE { { SELECT ?s WHERE { ?s a asset360:Signal } ORDER BY ?s LIMIT 3 } ?s a asset360:Signal ; asset360:name ?nm }"
                .to_owned(),
        );
        out
    }

    /// (a) a fixpoint with every invariant holding, (b) lowers or names the
    /// node that stopped it, (d) each rewrite alone answer-preserving --
    /// over the whole grammar, exhaustively.
    #[test]
    fn every_rewrite_of_every_grammar_query_preserves_the_answer() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let rules = tier_one_rules(&schema, None);
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        let grammar = grammar();
        assert!(grammar.len() > 100, "{}", grammar.len());
        let mut lowered = 0;
        for query in &grammar {
            let plan = each_rewrite_preserves_answers(query, &schema, &oracle, &borrowed);
            match crate::sparql_ops::lower_refined(&plan, &schema, None, None) {
                Ok(_) => lowered += 1,
                Err(refusal) => {
                    // Names the node, or the shape: never a panic.
                    let _ = refusal.to_string();
                }
            }
        }
        assert!(lowered > 0);
    }

    /// The same, under the three legal schedules: the list, the list
    /// reversed, and one fixed permutation. Two schedules may produce two
    /// plans; both must answer the query.
    #[test]
    fn every_schedule_answers_the_same() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let rules = tier_one_rules(&schema, None);
        let forwards: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        let backwards: Vec<&dyn Rule> = rules.iter().rev().map(|rule| rule.as_ref()).collect();
        let mut permuted: Vec<&dyn Rule> = forwards.clone();
        // A fixed permutation: rotate by a third.
        let third = permuted.len() / 3;
        permuted.rotate_left(third);
        for query in grammar().iter().step_by(3) {
            for schedule in [&forwards, &backwards, &permuted] {
                each_rewrite_preserves_answers(query, &schema, &oracle, schedule);
            }
        }
    }

    /// `MAX_ROUNDS = 1` over the grammar: every plan passes every invariant,
    /// lowers or names the node, and agrees with the oracle -- a budget hit
    /// leaves a correct, less refined plan (design test 11(v)).
    #[test]
    fn one_round_leaves_a_correct_plan() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let rules = tier_one_rules(&schema, None);
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        for query in grammar().iter().step_by(5) {
            let text = format!("{PREFIX}{query}");
            let parsed = crate::sparql_scoper::parse_query(&text).unwrap();
            let expected = answers(&oracle, &parsed);
            let mut plan = crate::sparql_refine::naive_plan(&parsed).unwrap();
            for rule in &borrowed {
                rule.apply(&mut plan);
            }
            plan.check_with(&schema)
                .unwrap_or_else(|d| panic!("{d}\n{query}\n{plan}"));
            let translated = plan_to_query(&plan, &schema).unwrap();
            assert_eq!(answers(&oracle, &translated), expected, "{query}\n{plan}");
            let _ = crate::sparql_ops::lower_refined(&plan, &schema, None, None);
        }
    }

    /// **Direct tests for the nodes the round trip never exercises**: a
    /// scan with a required read and one with an optional read, each
    /// against the matches it replaced; an unnest over an array with a
    /// duplicate entry and over an absent key, asserting multiplicity --
    /// one row per distinct value for a scalar slot, one per occurrence for
    /// a structure; a nested unnest whose composed occurrence tells
    /// `parts[0].children[1]` from `parts[1].children[1]`.
    #[test]
    fn the_scan_and_unnest_translations_are_the_matches_they_replaced() {
        let schema = test_schema_view();
        let oracle = fixture(&schema);
        let rules = tier_one_rules(&schema, None);
        let borrowed: Vec<&dyn Rule> = rules.iter().map(|rule| rule.as_ref()).collect();
        for (query, expected_rows) in [
            // A required read: the record without a name is not a row.
            (
                "SELECT ?s ?nm WHERE { ?s a asset360:Signal ; asset360:name ?nm }",
                4,
            ),
            // An optional read: every record, name or not.
            (
                "SELECT ?s ?nm WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:name ?nm } }",
                5,
            ),
            // A duplicated scalar is one triple: A has m, p -- two rows.
            (
                "SELECT ?k WHERE { <https://data.infrabel.be/asset360/signal/A> a asset360:Signal ; asset360:trafficKinds ?k }",
                2,
            ),
            // An absent array: no rows.
            (
                "SELECT ?k WHERE { <https://data.infrabel.be/asset360/signal/C> a asset360:Signal ; asset360:trafficKinds ?k }",
                0,
            ),
            // A duplicated structure is two blank nodes: parts[2] and
            // parts[3] both count. Four parts.
            (
                "SELECT (COUNT(?p) AS ?n) WHERE { ?a a asset360:Assembly ; asset360:parts ?p }",
                1,
            ),
            // The nested occurrence: six children, two of them identical.
            (
                "SELECT (COUNT(?c) AS ?n) WHERE { ?a a asset360:Assembly ; asset360:parts ?p . ?p asset360:children ?c }",
                1,
            ),
        ] {
            let plan = each_rewrite_preserves_answers(query, &schema, &oracle, &borrowed);
            let translated = plan_to_query(&plan, &schema).unwrap();
            let bag = answers(&oracle, &translated);
            let rows: usize = bag.values().sum();
            assert_eq!(rows, expected_rows, "{query}\n{plan}\n{bag:?}");
        }
        // And the counts themselves: four parts, six children.
        assert_eq!(
            oracle
                .answers_to(&format!(
                    "{PREFIX}SELECT (COUNT(?p) AS ?n) WHERE {{ ?a a asset360:Assembly ; asset360:parts ?p }}"
                ))
                .keys()
                .next()
                .unwrap()["n"],
            "\"4\"^^<http://www.w3.org/2001/XMLSchema#integer>"
        );
        assert_eq!(
            oracle
                .answers_to(&format!(
                    "{PREFIX}SELECT (COUNT(?c) AS ?n) WHERE {{ ?a a asset360:Assembly ; asset360:parts ?p . ?p asset360:children ?c }}"
                ))
                .keys()
                .next()
                .unwrap()["n"],
            "\"6\"^^<http://www.w3.org/2001/XMLSchema#integer>"
        );
    }
}
