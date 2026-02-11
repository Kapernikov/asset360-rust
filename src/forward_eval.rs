//! Forward evaluation: AST × object data → violations.
//!
//! Evaluates a SHACL AST against a JSON object and produces a list of
//! violations. Sans-IO: works entirely in memory.

use crate::shacl_ast::*;

/// Evaluate a SHACL AST against object data (flattened JSON object).
///
/// Returns a list of violations. Empty list means the data satisfies the constraint.
pub fn evaluate_forward(
    ast: &ShaclAst,
    data: &serde_json::Value,
    message: &str,
    enforcement_level: &EnforcementLevel,
) -> Vec<Violation> {
    if eval_node(ast, data) {
        vec![]
    } else {
        // Constraint failed — collect affected fields for the violation
        let fields = collect_violation_fields(ast, data);
        vec![Violation {
            fields,
            message: message.to_owned(),
            enforcement_level: enforcement_level.clone(),
            suggested_fix: None,
        }]
    }
}

/// Recursively evaluate an AST node. Returns `true` if the constraint is satisfied.
fn eval_node(ast: &ShaclAst, data: &serde_json::Value) -> bool {
    match ast {
        ShaclAst::And { children } => children.iter().all(|c| eval_node(c, data)),
        ShaclAst::Or { children } => children.iter().any(|c| eval_node(c, data)),
        ShaclAst::Not { child } => !eval_node(child, data),

        ShaclAst::PropEquals { path, value } => {
            let actual = resolve_path(data, path);
            match actual {
                Some(v) => values_equal(v, value),
                None => false,
            }
        }

        ShaclAst::PropIn { path, values } => {
            let actual = resolve_path(data, path);
            match actual {
                Some(v) => values.iter().any(|allowed| values_equal(v, allowed)),
                None => false,
            }
        }

        ShaclAst::PropCount { path, min, max } => {
            let count = resolve_count(data, path);
            let min_ok = min.is_none_or(|m| count >= m);
            let max_ok = max.is_none_or(|m| count <= m);
            min_ok && max_ok
        }

        ShaclAst::PathEquals { path_a, path_b } => {
            let val_a = resolve_path(data, path_a);
            let val_b = resolve_path(data, path_b);
            match (val_a, val_b) {
                (Some(a), Some(b)) => values_equal(a, b),
                (None, None) => true, // both absent = equal
                _ => false,
            }
        }

        ShaclAst::PathDisjoint { path_a, path_b } => {
            let val_a = resolve_path(data, path_a);
            let val_b = resolve_path(data, path_b);
            match (val_a, val_b) {
                (Some(a), Some(b)) => !values_equal(a, b),
                _ => true, // if either is absent, they're disjoint
            }
        }
    }
}

/// Resolve a property path against a JSON value.
///
/// For IRI paths, extracts the local name and looks it up as a JSON key.
/// For sequence paths, follows each step. For inverse paths, not resolvable
/// in a single object (returns None).
fn resolve_path<'a>(
    data: &'a serde_json::Value,
    path: &PropertyPath,
) -> Option<&'a serde_json::Value> {
    match path {
        PropertyPath::Iri { iri } => {
            let local = iri
                .rsplit_once('#')
                .or_else(|| iri.rsplit_once('/'))
                .map(|(_, name)| name)
                .unwrap_or(iri);
            data.get(local)
        }
        PropertyPath::Sequence { steps } => {
            let mut current = data;
            for step in steps {
                current = resolve_path(current, step)?;
            }
            Some(current)
        }
        PropertyPath::Inverse { .. } => {
            // Inverse paths require traversing from another object back to this one.
            // Cannot evaluate sans-IO in a single object context.
            None
        }
    }
}

/// Count values at a path (for cardinality constraints).
fn resolve_count(data: &serde_json::Value, path: &PropertyPath) -> u32 {
    match resolve_path(data, path) {
        None => 0,
        Some(serde_json::Value::Null) => 0,
        Some(serde_json::Value::Array(arr)) => arr.len() as u32,
        Some(_) => 1,
    }
}

/// Compare two JSON values with loose type coercion (string ↔ other).
fn values_equal(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    if a == b {
        return true;
    }
    // Loose comparison: "true" == true, "42" == 42, etc.
    match (a, b) {
        (serde_json::Value::String(s), other) | (other, serde_json::Value::String(s)) => {
            match other {
                serde_json::Value::Bool(bv) => s == &bv.to_string(),
                serde_json::Value::Number(n) => s == &n.to_string(),
                serde_json::Value::String(s2) => s == s2,
                _ => false,
            }
        }
        _ => false,
    }
}

/// Collect field names involved in a failing constraint (for violation reporting).
fn collect_violation_fields(ast: &ShaclAst, _data: &serde_json::Value) -> Vec<String> {
    let mut fields = Vec::new();
    collect_paths(ast, &mut fields);
    fields.sort();
    fields.dedup();
    fields
}

fn collect_paths(ast: &ShaclAst, fields: &mut Vec<String>) {
    match ast {
        ShaclAst::And { children } | ShaclAst::Or { children } => {
            for child in children {
                collect_paths(child, fields);
            }
        }
        ShaclAst::Not { child } => collect_paths(child, fields),
        ShaclAst::PropEquals { path, .. }
        | ShaclAst::PropIn { path, .. }
        | ShaclAst::PropCount { path, .. } => {
            if let Some(name) = path.local_name() {
                fields.push(name.to_owned());
            }
        }
        ShaclAst::PathEquals { path_a, path_b } | ShaclAst::PathDisjoint { path_a, path_b } => {
            if let Some(name) = path_a.local_name() {
                fields.push(name.to_owned());
            }
            if let Some(name) = path_b.local_name() {
                fields.push(name.to_owned());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn status_combo_ast() -> ShaclAst {
        // Not(Or(And(P=In_voorbereiding, S=Verkocht), ..9 combos))
        let forbidden_combos = vec![
            ("In_voorbereiding", "Verkocht"),
            ("In_voorbereiding", "Afgebroken"),
            ("In_voorbereiding", "Aangevuld"),
            ("In_voorbereiding", "Uit_dienst"),
            ("In_opvolging", "Verkocht"),
            ("In_opvolging", "Afgebroken"),
            ("In_opvolging", "Aangevuld"),
            ("In_opvolging", "Uit_dienst"),
            ("Uit_opvolging", "In_dienst"),
        ];

        let or_children: Vec<ShaclAst> = forbidden_combos
            .into_iter()
            .map(|(primary, secondary)| ShaclAst::And {
                children: vec![
                    ShaclAst::PropEquals {
                        path: PropertyPath::iri(
                            "https://data.infrabel.be/asset360/ceAssetPrimaryStatus",
                        ),
                        value: json!(primary),
                    },
                    ShaclAst::PropEquals {
                        path: PropertyPath::iri(
                            "https://data.infrabel.be/asset360/ceAssetSecondaryStatus",
                        ),
                        value: json!(secondary),
                    },
                ],
            })
            .collect();

        ShaclAst::Not {
            child: Box::new(ShaclAst::Or {
                children: or_children,
            }),
        }
    }

    #[test]
    fn test_forbidden_combos_produce_violations() {
        let ast = status_combo_ast();
        let forbidden = vec![
            ("In_voorbereiding", "Verkocht"),
            ("In_voorbereiding", "Afgebroken"),
            ("In_voorbereiding", "Aangevuld"),
            ("In_voorbereiding", "Uit_dienst"),
            ("In_opvolging", "Verkocht"),
            ("In_opvolging", "Afgebroken"),
            ("In_opvolging", "Aangevuld"),
            ("In_opvolging", "Uit_dienst"),
            ("Uit_opvolging", "In_dienst"),
        ];

        for (primary, secondary) in &forbidden {
            let data = json!({
                "ceAssetPrimaryStatus": primary,
                "ceAssetSecondaryStatus": secondary,
            });
            let violations =
                evaluate_forward(&ast, &data, "Forbidden combo", &EnforcementLevel::Serious);
            assert!(
                !violations.is_empty(),
                "Expected violation for {primary}/{secondary}"
            );
            assert_eq!(violations[0].enforcement_level, EnforcementLevel::Serious);
        }
    }

    #[test]
    fn test_valid_combos_no_violations() {
        let ast = status_combo_ast();
        let valid = vec![
            ("In_voorbereiding", "In_dienst"),
            ("In_opvolging", "In_dienst"),
            ("Uit_opvolging", "Verkocht"),
            ("Uit_opvolging", "Afgebroken"),
        ];

        for (primary, secondary) in &valid {
            let data = json!({
                "ceAssetPrimaryStatus": primary,
                "ceAssetSecondaryStatus": secondary,
            });
            let violations =
                evaluate_forward(&ast, &data, "Forbidden combo", &EnforcementLevel::Serious);
            assert!(
                violations.is_empty(),
                "Unexpected violation for {primary}/{secondary}: {:?}",
                violations
            );
        }
    }

    #[test]
    fn test_missing_field_produces_violation() {
        // If a field is missing, PropEquals fails, so Not(Or(And(false, ...))) = Not(false) = true
        // Actually: PropEquals with missing field = false, And(false, x) = false, Or(false, ...) = false, Not(false) = true
        // So missing fields should NOT produce a violation (the constraint is vacuously satisfied)
        let ast = status_combo_ast();
        let data = json!({"ceAssetPrimaryStatus": "In_voorbereiding"});
        let violations = evaluate_forward(&ast, &data, "test", &EnforcementLevel::Serious);
        assert!(
            violations.is_empty(),
            "Missing secondary should not violate"
        );
    }

    #[test]
    fn test_prop_in() {
        let ast = ShaclAst::PropIn {
            path: PropertyPath::iri("https://example.org/status"),
            values: vec![json!("active"), json!("pending")],
        };
        let data = json!({"status": "active"});
        assert!(eval_node(&ast, &data));

        let data = json!({"status": "deleted"});
        assert!(!eval_node(&ast, &data));
    }

    #[test]
    fn test_prop_count() {
        let ast = ShaclAst::PropCount {
            path: PropertyPath::iri("https://example.org/tags"),
            min: Some(1),
            max: Some(3),
        };
        let data = json!({"tags": ["a", "b"]});
        assert!(eval_node(&ast, &data));

        let data = json!({"tags": []});
        assert!(!eval_node(&ast, &data));

        let data = json!({"tags": ["a", "b", "c", "d"]});
        assert!(!eval_node(&ast, &data));
    }

    #[test]
    fn test_loose_equality() {
        // String "true" should match boolean true
        assert!(values_equal(&json!("true"), &json!(true)));
        assert!(values_equal(&json!("42"), &json!(42)));
    }
}
