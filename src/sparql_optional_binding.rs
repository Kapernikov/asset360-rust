//! An `OPTIONAL` whose only link to what precedes it may be unbound is refused.
//!
//! `OPTIONAL { ?a :hasTrackFastening ?f } OPTIONAL { GRAPH <schema> { ?f
//! skos:prefLabel ?l } }` is what a SPARQL-literate person writes first for a
//! label, and per the algebra it is a cartesian product: for every row where
//! the first `OPTIONAL` left `?f` unbound, the second is joined against *every*
//! label in the graph, because an unbound variable is compatible with anything.
//! One label block turns 130 tunnels into 2 098 rows; sixteen of them pinned a
//! worker at 100 % CPU and 5 GB for seven minutes before it was killed
//! ([#460](https://gitlab.pp.kapernikov.com/asset360/consolidator-server/-/issues/460)).
//! Correct per spec, and never what the author meant — they meant the nested
//! form, `OPTIONAL { ?a :hasTrackFastening ?f . OPTIONAL { … ?f … } }`, where
//! the label is asked only of a fastening that exists.
//!
//! So the shape is refused, by name, with the nested spelling in the message.
//! The rule is narrow on purpose: a `LeftJoin` whose right side shares
//! variables with its left side, **none of which the left side certainly
//! binds**. A shared variable the left side certainly binds (`?a` in `OPTIONAL
//! { ?a :q ?y } OPTIONAL { ?a :r ?y }`, the fallback-value idiom) constrains
//! the optional to that row and the product never happens, so that is left
//! alone. An `OPTIONAL` sharing nothing with what precedes it is a product by
//! construction and is also left alone: it is at least what it says.
//!
//! "Certainly bound" is the textbook notion: a variable every solution of the
//! pattern binds. A basic graph pattern binds all of its variables; a join
//! binds both sides'; a left join binds its preserved side's; a union binds
//! what both arms bind; a projection keeps the projected ones. A `BIND` is
//! counted as binding its variable — strictly it does not when the expression
//! errors, but refusing every `BIND … OPTIONAL` for that would refuse far more
//! than it protects.
//!
//! This is a refusal on the *query*, checked by the scoper before either route
//! plans it, so the two routes refuse the same thing. The engine leg's
//! wall-clock ceiling ([`crate::sparql_executor::ExecuteLimits::max_eval_millis`])
//! is the backstop for whatever this rule does not name.

use std::collections::BTreeSet;

use spargebra::Query;
use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern, Variable};

/// An `OPTIONAL` this module refuses, with what to write instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionalOnMaybeUnbound {
    /// The variables the `OPTIONAL` shares with the pattern before it, none of
    /// which that pattern certainly binds. Sorted.
    pub variables: Vec<String>,
    /// The optional pattern, rendered.
    pub optional: String,
    /// The pattern that binds the shared variable optionally -- the `OPTIONAL`
    /// the refused one should be nested under -- when there is exactly one.
    pub binder: Option<String>,
}

impl std::fmt::Display for OptionalOnMaybeUnbound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let vars = self
            .variables
            .iter()
            .map(|v| format!("?{v}"))
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "OPTIONAL {{ {} }} is joined to the pattern before it only through {vars}, \
             which that pattern may leave unbound (it is bound inside an OPTIONAL). Where it \
             is unbound, SPARQL joins this OPTIONAL against every match in the graph -- a \
             cartesian product. Nest it under the OPTIONAL that binds {vars} instead",
            self.optional
        )?;
        match &self.binder {
            Some(binder) => write!(
                f,
                ": OPTIONAL {{ {binder} OPTIONAL {{ {} }} }}",
                self.optional
            ),
            None => write!(f, "."),
        }
    }
}

/// The first `OPTIONAL` in the query this module refuses, if any.
pub fn optional_on_a_maybe_unbound_variable(query: &Query) -> Option<OptionalOnMaybeUnbound> {
    let pattern = match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => pattern,
    };
    walk(pattern)
}

fn walk(pattern: &GraphPattern) -> Option<OptionalOnMaybeUnbound> {
    match pattern {
        GraphPattern::LeftJoin { left, right, .. } => {
            if let Some(found) = walk(left).or_else(|| walk(right)) {
                return Some(found);
            }
            let shared: BTreeSet<&Variable> = in_scope(left)
                .intersection(&in_scope(right))
                .copied()
                .collect();
            if shared.is_empty() {
                return None;
            }
            let bound = certainly_bound(left);
            if shared.iter().any(|v| bound.contains(*v)) {
                return None;
            }
            let variables: Vec<String> = shared.iter().map(|v| v.as_str().to_owned()).collect();
            let binders: Vec<&GraphPattern> = optionals_binding_any(left, &shared);
            Some(OptionalOnMaybeUnbound {
                variables,
                optional: right.to_string(),
                binder: match binders.as_slice() {
                    [one] => Some(one.to_string()),
                    _ => None,
                },
            })
        }
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => None,
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => walk(left).or_else(|| walk(right)),
        GraphPattern::Lateral { left, right } => walk(left).or_else(|| walk(right)),
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Service { inner, .. } => walk(inner),
    }
}

/// The optional sides, anywhere in `pattern`, that bind one of `vars`.
fn optionals_binding_any<'a>(
    pattern: &'a GraphPattern,
    vars: &BTreeSet<&Variable>,
) -> Vec<&'a GraphPattern> {
    let mut out = Vec::new();
    fn go<'a>(p: &'a GraphPattern, vars: &BTreeSet<&Variable>, out: &mut Vec<&'a GraphPattern>) {
        match p {
            GraphPattern::LeftJoin { left, right, .. } => {
                go(left, vars, out);
                if in_scope(right).iter().any(|v| vars.contains(v)) {
                    out.push(right);
                } else {
                    go(right, vars, out);
                }
            }
            GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => {}
            GraphPattern::Join { left, right }
            | GraphPattern::Union { left, right }
            | GraphPattern::Minus { left, right } => {
                go(left, vars, out);
                go(right, vars, out);
            }
            GraphPattern::Lateral { left, right } => {
                go(left, vars, out);
                go(right, vars, out);
            }
            GraphPattern::Filter { inner, .. }
            | GraphPattern::Graph { inner, .. }
            | GraphPattern::Extend { inner, .. }
            | GraphPattern::OrderBy { inner, .. }
            | GraphPattern::Project { inner, .. }
            | GraphPattern::Distinct { inner }
            | GraphPattern::Reduced { inner }
            | GraphPattern::Slice { inner, .. }
            | GraphPattern::Group { inner, .. }
            | GraphPattern::Service { inner, .. } => go(inner, vars, out),
        }
    }
    go(pattern, vars, &mut out);
    out
}

/// Every variable a pattern may bind.
fn in_scope(pattern: &GraphPattern) -> BTreeSet<&Variable> {
    let mut out = BTreeSet::new();
    pattern.on_in_scope_variable(|v| {
        out.insert(v);
    });
    out
}

fn triple_variables<'a>(pattern: &'a TriplePattern, out: &mut BTreeSet<&'a Variable>) {
    if let TermPattern::Variable(v) = &pattern.subject {
        out.insert(v);
    }
    if let NamedNodePattern::Variable(v) = &pattern.predicate {
        out.insert(v);
    }
    if let TermPattern::Variable(v) = &pattern.object {
        out.insert(v);
    }
}

/// Every variable a pattern binds in each of its solutions.
fn certainly_bound(pattern: &GraphPattern) -> BTreeSet<&Variable> {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            let mut out = BTreeSet::new();
            for triple in patterns {
                triple_variables(triple, &mut out);
            }
            out
        }
        GraphPattern::Path {
            subject, object, ..
        } => {
            let mut out = BTreeSet::new();
            if let TermPattern::Variable(v) = subject {
                out.insert(v);
            }
            if let TermPattern::Variable(v) = object {
                out.insert(v);
            }
            out
        }
        GraphPattern::Join { left, right } => {
            let mut out = certainly_bound(left);
            out.extend(certainly_bound(right));
            out
        }
        GraphPattern::Lateral { left, right } => {
            let mut out = certainly_bound(left);
            out.extend(certainly_bound(right));
            out
        }
        GraphPattern::LeftJoin { left, .. } | GraphPattern::Minus { left, .. } => {
            certainly_bound(left)
        }
        GraphPattern::Union { left, right } => certainly_bound(left)
            .intersection(&certainly_bound(right))
            .copied()
            .collect(),
        GraphPattern::Graph { name, inner } => {
            let mut out = certainly_bound(inner);
            if let NamedNodePattern::Variable(v) = name {
                out.insert(v);
            }
            out
        }
        GraphPattern::Extend {
            inner, variable, ..
        } => {
            let mut out = certainly_bound(inner);
            out.insert(variable);
            out
        }
        GraphPattern::Values {
            variables,
            bindings,
        } => variables
            .iter()
            .enumerate()
            .filter(|(i, _)| {
                bindings
                    .iter()
                    .all(|row| row.get(*i).is_some_and(Option::is_some))
            })
            .map(|(_, v)| v)
            .collect(),
        GraphPattern::Filter { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => certainly_bound(inner),
        GraphPattern::Project { inner, variables } => {
            let inner = certainly_bound(inner);
            variables.iter().filter(|v| inner.contains(v)).collect()
        }
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            let inner = certainly_bound(inner);
            variables
                .iter()
                .filter(|v| inner.contains(v))
                .chain(aggregates.iter().map(|(v, _)| v))
                .collect()
        }
        // Not served by this endpoint; nothing it binds is relied on.
        GraphPattern::Service { .. } => BTreeSet::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(body: &str) -> Query {
        crate::sparql_scoper::parse_query(&format!(
            "PREFIX : <https://data.infrabel.be/asset360/> \
             PREFIX skos: <http://www.w3.org/2004/02/skos/core#> {body}"
        ))
        .expect("parses")
    }

    fn refused(body: &str) -> Option<OptionalOnMaybeUnbound> {
        optional_on_a_maybe_unbound_variable(&parse(body))
    }

    #[test]
    fn a_sibling_label_optional_is_refused_naming_the_nested_form() {
        let found = refused(
            "SELECT * WHERE { ?a a :CivilEngineeringAsset . \
             OPTIONAL { ?a :hasTrackFastening ?f } \
             OPTIONAL { GRAPH <https://data.infrabel.be/asset360/schema> { ?f skos:prefLabel ?l } } }",
        )
        .expect("the sibling shape is refused");
        assert_eq!(found.variables, vec!["f"]);
        let message = found.to_string();
        assert!(message.contains("?f"), "{message}");
        assert!(
            message.contains("Nest it under the OPTIONAL that binds ?f"),
            "{message}"
        );
        assert!(
            message.contains("OPTIONAL { ?a <https://data.infrabel.be/asset360/hasTrackFastening> ?f . OPTIONAL { GRAPH"),
            "the rewrite is spelled out: {message}"
        );
    }

    #[test]
    fn the_nested_form_and_the_bound_forms_pass() {
        // Nested under the binder: the label is asked of a fastening that exists.
        assert_eq!(
            refused(
                "SELECT * WHERE { ?a a :CivilEngineeringAsset . \
                 OPTIONAL { ?a :hasTrackFastening ?f . \
                 OPTIONAL { GRAPH <https://data.infrabel.be/asset360/schema> { ?f skos:prefLabel ?l } } } }"
            ),
            None
        );
        // The subject is certainly bound: an ordinary optional read.
        assert_eq!(
            refused(
                "SELECT * WHERE { ?a a :Signal ; :hasTrackFastening ?f . OPTIONAL { ?f skos:prefLabel ?l } }"
            ),
            None
        );
        // The fallback-value idiom shares `?a`, which is certainly bound.
        assert_eq!(
            refused(
                "SELECT * WHERE { ?a a :Signal . OPTIONAL { ?a :name ?n } OPTIONAL { ?a :label ?n } }"
            ),
            None
        );
        // Sharing nothing is a product by construction, and left alone.
        assert_eq!(
            refused("SELECT * WHERE { ?a a :Signal . OPTIONAL { ?z a :Track } }"),
            None
        );
        // A UNION binds only what both arms bind.
        assert!(
            refused(
                "SELECT * WHERE { ?a a :Signal . { ?a :x ?f } UNION { ?a :y ?g } OPTIONAL { ?f :p ?l } }"
            )
            .is_some()
        );
        // Bound by a BIND counts as bound.
        assert_eq!(
            refused("SELECT * WHERE { ?a a :Signal . BIND(?a AS ?f) OPTIONAL { ?f :p ?l } }"),
            None
        );
    }

    #[test]
    fn the_refusal_is_found_inside_a_nested_optional_too() {
        let found = refused(
            "SELECT * WHERE { ?a a :Signal . OPTIONAL { ?a :ref ?i . \
             OPTIONAL { ?i :q ?y } OPTIONAL { ?y :p ?l } } }",
        )
        .expect("refused inside the outer OPTIONAL");
        assert_eq!(found.variables, vec!["y"]);
    }
}
