//! Naming domains, for the scoper: a variable inside a sub-`SELECT` is a
//! variable of that sub-query, whatever it is spelled.
//!
//! SPARQL solution mappings are per query, and only a sub-select's
//! projection makes a variable private (§18.2.4): the `?s` of `{ SELECT ?x
//! WHERE { ?s :p ?x } }` is not the outer `?s`, while the `?a` inside an
//! `OPTIONAL` body or a `UNION` arm *is* the outer `?a`. The star
//! decomposition keys stars by variable name, so a triple written inside a
//! sub-query joined the star of any outer variable spelled the same -- and
//! the fetch of `?s a :C . { SELECT ?x WHERE { ?s :p ?x } }` held `:C`
//! records only, where the query reads every record with `:p`
//! (`docs/design/sparql-scopes-as-relations.md`, *The scoper infers per
//! scope*).
//!
//! The contract is **a star is a `(naming domain, variable)`**, and this
//! module is how the scoper gets it without a second star construction:
//! [`qualify`] renames every variable occurrence inside a sub-select to
//! `<name>__d<n>`, `n` the domain's number in the walk order the plan
//! builder numbers its barriers in ([`crate::sparql_refine::PlanOp::SubSelect`]'s
//! `domain`), and leaves the query's own domain unsuffixed. Everything
//! downstream -- star construction, the path walk, filters, the fetch --
//! then keys by qualified name and is per domain by construction, the way
//! `scope_union` already keys a second arm's star `?s__u1`. A scoper star
//! named `s__d1` is the plan's `?s` in domain 1, which is what `resolve`
//! reads back ([`split`]).
//!
//! The renaming is of the scoper's *reading* of the query, never of the
//! query the engine runs: the engine leg is handed the original text.

use spargebra::Query;
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern, Variable};

/// The query with every variable inside a sub-select qualified by its
/// naming domain. See the module docs.
pub fn qualify(query: &Query) -> Query {
    let mut counter = 0usize;
    let mut query = query.clone();
    match &mut query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => walk(pattern, true, None, &mut counter),
    }
    query
}

/// A qualified name back to `(variable, domain)`: `s__d2` is `("s", 2)`,
/// and a name with no suffix is domain `0`, the query's own.
pub fn split(name: &str) -> (String, usize) {
    match name.rsplit_once("__d") {
        Some((base, digits)) => match digits.parse::<usize>() {
            Ok(domain) => (base.to_owned(), domain),
            Err(_) => (name.to_owned(), 0),
        },
        None => (name.to_owned(), 0),
    }
}

/// The qualified name of `var` in domain `domain` (`0` unsuffixed).
pub fn qualified(var: &str, domain: usize) -> String {
    if domain == 0 {
        var.to_owned()
    } else {
        format!("{var}__d{domain}")
    }
}

/// What each sub-select of a *qualified* query exports, as `(inner name,
/// outer name)`: `("s__d2", "s__d1")` for a `SELECT ?s` in domain 2 nested
/// in domain 1, `("s__d1", "s")` at the top. The link a consumer needs to
/// know that the inner star and the outer one are one variable across the
/// barrier. Numbered by the same walk as [`qualify`].
pub fn exports(query: &Query) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut counter = 0usize;
    let pattern = match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => pattern,
    };
    collect_exports(pattern, true, 0, &mut counter, &mut out);
    out
}

fn collect_exports(
    pattern: &GraphPattern,
    on_spine: bool,
    enclosing: usize,
    counter: &mut usize,
    out: &mut Vec<(String, String)>,
) {
    if !on_spine
        && matches!(
            pattern,
            GraphPattern::Slice { .. }
                | GraphPattern::Distinct { .. }
                | GraphPattern::Reduced { .. }
                | GraphPattern::Project { .. }
        )
    {
        *counter += 1;
        let opened = *counter;
        // The projection at the top of the fresh spine, under its
        // modifiers, is what this sub-select exports.
        let mut top = pattern;
        loop {
            match top {
                GraphPattern::Slice { inner, .. }
                | GraphPattern::Distinct { inner }
                | GraphPattern::Reduced { inner } => top = inner,
                GraphPattern::Project { variables, .. } => {
                    for variable in variables {
                        let (base, _) = split(variable.as_str());
                        out.push((variable.as_str().to_owned(), qualified(&base, enclosing)));
                    }
                    break;
                }
                _ => break,
            }
        }
        collect_exports(pattern, true, opened, counter, out);
        return;
    }
    match pattern {
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => {}
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_exports(left, false, enclosing, counter, out);
            collect_exports(right, false, enclosing, counter, out);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            collect_exports(left, false, enclosing, counter, out);
            collect_exports(right, false, enclosing, counter, out);
            if let Some(expression) = expression {
                exports_in_expression(expression, enclosing, counter, out);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            collect_exports(inner, on_spine, enclosing, counter, out);
            exports_in_expression(expr, enclosing, counter, out);
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            collect_exports(inner, on_spine, enclosing, counter, out);
            exports_in_expression(expression, enclosing, counter, out);
        }
        GraphPattern::Group { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => {
            collect_exports(inner, on_spine, enclosing, counter, out)
        }
        GraphPattern::Project { inner, .. } => {
            collect_exports(inner, false, enclosing, counter, out)
        }
    }
}

fn exports_in_expression(
    expr: &Expression,
    enclosing: usize,
    counter: &mut usize,
    out: &mut Vec<(String, String)>,
) {
    let mut blocks = Vec::new();
    crate::sparql_scoper::exists_patterns_of(expr, &mut blocks);
    for block in blocks {
        collect_exports(block, false, enclosing, counter, out);
    }
}

/// Rename inside one pattern. `on_spine` and the domain counter mirror
/// `Builder::pattern` in `sparql_refine.rs` exactly: a modifier met off the
/// spine opens the next domain, and the walk order -- left before right,
/// inner before the `EXISTS` blocks of a condition -- is the builder's.
fn walk(pattern: &mut GraphPattern, on_spine: bool, domain: Option<usize>, counter: &mut usize) {
    if !on_spine
        && matches!(
            pattern,
            GraphPattern::Slice { .. }
                | GraphPattern::Distinct { .. }
                | GraphPattern::Reduced { .. }
                | GraphPattern::Project { .. }
        )
    {
        *counter += 1;
        let opened = *counter;
        walk(pattern, true, Some(opened), counter);
        return;
    }
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for triple in patterns {
                rename_triple(triple, domain);
            }
        }
        GraphPattern::Path {
            subject, object, ..
        } => {
            rename_term(subject, domain);
            rename_term(object, domain);
        }
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Minus { left, right } => {
            walk(left, false, domain, counter);
            walk(right, false, domain, counter);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            walk(left, false, domain, counter);
            walk(right, false, domain, counter);
            if let Some(expression) = expression {
                rename_expression(expression, domain, counter);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            walk(inner, on_spine, domain, counter);
            rename_expression(expr, domain, counter);
        }
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            walk(inner, on_spine, domain, counter);
            rename_expression(expression, domain, counter);
            rename_variable(variable, domain);
        }
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            walk(inner, on_spine, domain, counter);
            for variable in variables {
                rename_variable(variable, domain);
            }
            for (variable, aggregate) in aggregates {
                rename_variable(variable, domain);
                if let AggregateExpression::FunctionCall { expr, .. } = aggregate {
                    rename_expression(expr, domain, counter);
                }
            }
        }
        GraphPattern::OrderBy { inner, expression } => {
            walk(inner, on_spine, domain, counter);
            for term in expression {
                let (OrderExpression::Asc(expr) | OrderExpression::Desc(expr)) = term;
                rename_expression(expr, domain, counter);
            }
        }
        GraphPattern::Project { inner, variables } => {
            walk(inner, false, domain, counter);
            for variable in variables {
                rename_variable(variable, domain);
            }
        }
        GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => walk(inner, on_spine, domain, counter),
        GraphPattern::Graph { name, inner } => {
            if let NamedNodePattern::Variable(variable) = name {
                rename_variable(variable, domain);
            }
            walk(inner, on_spine, domain, counter);
        }
        GraphPattern::Service { name, inner, .. } => {
            if let NamedNodePattern::Variable(variable) = name {
                rename_variable(variable, domain);
            }
            walk(inner, on_spine, domain, counter);
        }
        GraphPattern::Values { variables, .. } => {
            for variable in variables {
                rename_variable(variable, domain);
            }
        }
    }
}

fn rename_variable(variable: &mut Variable, domain: Option<usize>) {
    if let Some(domain) = domain {
        *variable = Variable::new_unchecked(qualified(variable.as_str(), domain));
    }
}

fn rename_term(term: &mut TermPattern, domain: Option<usize>) {
    if let TermPattern::Variable(variable) = term {
        rename_variable(variable, domain);
    }
}

fn rename_triple(triple: &mut TriplePattern, domain: Option<usize>) {
    rename_term(&mut triple.subject, domain);
    if let NamedNodePattern::Variable(variable) = &mut triple.predicate {
        rename_variable(variable, domain);
    }
    rename_term(&mut triple.object, domain);
}

/// Rename inside an expression, and walk into its `EXISTS` blocks in the
/// order `exists_patterns_of` lists them, which is the order the builder
/// reads them in.
fn rename_expression(expr: &mut Expression, domain: Option<usize>, counter: &mut usize) {
    match expr {
        Expression::Variable(variable) | Expression::Bound(variable) => {
            rename_variable(variable, domain)
        }
        Expression::Exists(pattern) => walk(pattern, false, domain, counter),
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
            rename_expression(left, domain, counter);
            rename_expression(right, domain, counter);
        }
        Expression::In(value, candidates) => {
            rename_expression(value, domain, counter);
            for candidate in candidates.iter_mut() {
                rename_expression(candidate, domain, counter);
            }
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            rename_expression(inner, domain, counter)
        }
        Expression::If(condition, then, otherwise) => {
            rename_expression(condition, domain, counter);
            rename_expression(then, domain, counter);
            rename_expression(otherwise, domain, counter);
        }
        Expression::Coalesce(parts) | Expression::FunctionCall(_, parts) => {
            for part in parts.iter_mut() {
                rename_expression(part, domain, counter);
            }
        }
        Expression::NamedNode(_) | Expression::Literal(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(query: &str) -> Query {
        crate::sparql_scoper::parse_query(&format!(
            "PREFIX asset360: <https://data.infrabel.be/asset360/> {query}"
        ))
        .unwrap()
    }

    /// The private inner `?s` is qualified, the outer one is not, and the
    /// numbering is the plan builder's: two sub-selects, left before right.
    #[test]
    fn a_variable_inside_a_sub_select_is_of_that_domain() {
        let query = parse(
            "SELECT ?s ?x ?n WHERE { ?s a asset360:Signal . \
             { SELECT ?x WHERE { ?s asset360:name ?x } } \
             { SELECT ?s (COUNT(*) AS ?n) WHERE { ?s a asset360:Signal } GROUP BY ?s } }",
        );
        let qualified = qualify(&query).to_string();
        assert!(
            qualified.contains("?s__d1 <https://data.infrabel.be/asset360/name> ?x__d1"),
            "{qualified}"
        );
        assert!(qualified.contains("SELECT ?x__d1 WHERE"), "{qualified}");
        assert!(
            qualified.contains(
                "?s__d2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://data.infrabel.be/asset360/Signal>"
            ),
            "{qualified}"
        );
        assert!(qualified.contains("GROUP BY ?s__d2"), "{qualified}");
        // The outer pattern and projection are untouched.
        assert!(qualified.contains("SELECT ?s ?x ?n WHERE"), "{qualified}");
        assert!(
            qualified.contains(
                "{ ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://data.infrabel.be/asset360/Signal> ."
            ),
            "{qualified}"
        );

        let plan = crate::sparql_refine::naive_plan(&query).unwrap();
        let domains: Vec<usize> = plan
            .barriers()
            .into_iter()
            .filter_map(|barrier| match &plan.nodes[barrier].op {
                crate::sparql_refine::PlanOp::SubSelect { domain, .. } => *domain,
                _ => None,
            })
            .collect();
        assert_eq!(domains, vec![1, 2], "{plan}");
        assert_eq!(split("s__d2"), ("s".to_owned(), 2));
        assert_eq!(split("s"), ("s".to_owned(), 0));
        assert_eq!(qualified_name_round_trip("kpFromMeter", 3), "kpFromMeter");
    }

    fn qualified_name_round_trip(var: &str, domain: usize) -> String {
        split(&qualified(var, domain)).0
    }

    /// The export links: the inner projection's names to the enclosing
    /// domain's spelling of them, one level at a time.
    #[test]
    fn a_sub_select_exports_its_projection_to_the_enclosing_domain() {
        let query = parse(
            "SELECT ?s ?n WHERE { ?s a asset360:Signal . OPTIONAL { \
             { SELECT ?s (COUNT(*) AS ?n) WHERE { { SELECT ?s WHERE { ?s a asset360:Signal } } } \
             GROUP BY ?s } } }",
        );
        let qualified = qualify(&query);
        let mut links = exports(&qualified);
        links.sort();
        assert_eq!(
            links,
            vec![
                ("n__d1".to_owned(), "n".to_owned()),
                ("s__d1".to_owned(), "s".to_owned()),
                ("s__d2".to_owned(), "s__d1".to_owned()),
            ]
        );
    }

    /// An `OPTIONAL` body opens no domain: its variables are the outer ones.
    #[test]
    fn an_optional_body_is_the_outer_domain() {
        let query = parse(
            "SELECT ?s ?nm WHERE { ?s a asset360:Signal . OPTIONAL { ?s asset360:name ?nm } }",
        );
        assert_eq!(qualify(&query).to_string(), query.to_string());
    }

    /// A sub-select nested in a sub-select: the inner occurrence carries the
    /// innermost domain only.
    #[test]
    fn nesting_qualifies_by_the_innermost_domain() {
        let query = parse(
            "SELECT ?x WHERE { { SELECT ?x WHERE { { SELECT ?x WHERE { ?s asset360:name ?x } } } } }",
        );
        let qualified = qualify(&query).to_string();
        assert!(
            qualified.contains("?s__d2 <https://data.infrabel.be/asset360/name> ?x__d2"),
            "{qualified}"
        );
        assert!(qualified.contains("SELECT ?x__d1 WHERE"), "{qualified}");
    }
}
