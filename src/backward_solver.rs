//! Backward solver: AST × known field values × target field → Predicate.
//!
//! Three-step algorithm:
//! 1. **Substitute** known field values into the AST (PropEquals with known field → Bool)
//! 2. **Simplify** the boolean formula (constant folding)
//! 3. **Extract** remaining constraints on the target field as a Predicate

use crate::predicate::Predicate;
use crate::shacl_ast::*;

// ── Intermediate representation ──────────────────────────────────────

/// Simplified AST with boolean constants for the solver.
#[derive(Clone, Debug)]
enum Simplified {
    Bool(bool),
    And(Vec<Simplified>),
    Or(Vec<Simplified>),
    Not(Box<Simplified>),
    /// A constraint on a single field (kept as-is for extraction).
    FieldConstraint {
        field: String,
        kind: FieldConstraintKind,
    },
}

#[derive(Clone, Debug)]
enum FieldConstraintKind {
    Equals(serde_json::Value),
    In(Vec<serde_json::Value>),
    NotEquals(serde_json::Value),
}

// ── Public API ───────────────────────────────────────────────────────

/// Given a SHACL AST, known field values, and a target field, produce a
/// `Predicate` describing allowed values for the target field.
///
/// Returns `None` if:
/// - The constraint is fully satisfied (all values allowed for target)
/// - The constraint doesn't reference the target field
/// - The simplified AST has unresolvable dependencies on unknown fields
pub fn solve_backward(
    ast: &ShaclAst,
    known_fields: &serde_json::Map<String, serde_json::Value>,
    target_field: &str,
) -> Option<Predicate> {
    let substituted = substitute(ast, known_fields, target_field);
    let simplified = simplify(substituted);
    extract_predicate(&simplified, target_field)
}

// ── Step 1: Substitute ───────────────────────────────────────────────

fn substitute(
    ast: &ShaclAst,
    known: &serde_json::Map<String, serde_json::Value>,
    target_field: &str,
) -> Simplified {
    match ast {
        ShaclAst::And { children } => {
            let subs: Vec<_> = children
                .iter()
                .map(|c| substitute(c, known, target_field))
                .collect();
            Simplified::And(subs)
        }
        ShaclAst::Or { children } => {
            let subs: Vec<_> = children
                .iter()
                .map(|c| substitute(c, known, target_field))
                .collect();
            Simplified::Or(subs)
        }
        ShaclAst::Not { child } => {
            Simplified::Not(Box::new(substitute(child, known, target_field)))
        }

        ShaclAst::PropEquals { path, value } => {
            if let Some(field_name) = path.local_name() {
                if let Some(known_val) = known.get(field_name) {
                    // Known field: substitute with boolean
                    Simplified::Bool(values_equal_json(known_val, value))
                } else if field_name == target_field {
                    // Target field: keep as constraint
                    Simplified::FieldConstraint {
                        field: field_name.to_owned(),
                        kind: FieldConstraintKind::Equals(value.clone()),
                    }
                } else {
                    // Unknown field that's not the target: can't resolve
                    // Treat as unconstrained (true) to be conservative
                    Simplified::Bool(true)
                }
            } else {
                Simplified::Bool(true)
            }
        }

        ShaclAst::PropIn { path, values } => {
            if let Some(field_name) = path.local_name() {
                if let Some(known_val) = known.get(field_name) {
                    Simplified::Bool(values.iter().any(|v| values_equal_json(known_val, v)))
                } else if field_name == target_field {
                    Simplified::FieldConstraint {
                        field: field_name.to_owned(),
                        kind: FieldConstraintKind::In(values.clone()),
                    }
                } else {
                    Simplified::Bool(true)
                }
            } else {
                Simplified::Bool(true)
            }
        }

        ShaclAst::PropCount { path, min, max } => {
            // Cardinality constraints are hard to invert symbolically.
            // If the field is known, evaluate directly. Otherwise, pass through.
            if let Some(field_name) = path.local_name() {
                if let Some(known_val) = known.get(field_name) {
                    let count = match known_val {
                        serde_json::Value::Array(arr) => arr.len() as u32,
                        serde_json::Value::Null => 0,
                        _ => 1,
                    };
                    let ok = min.map_or(true, |m| count >= m) && max.map_or(true, |m| count <= m);
                    Simplified::Bool(ok)
                } else {
                    // Can't produce a meaningful predicate for cardinality
                    Simplified::Bool(true)
                }
            } else {
                Simplified::Bool(true)
            }
        }

        ShaclAst::PathEquals { .. } | ShaclAst::PathDisjoint { .. } => {
            // Property pair constraints with paths are complex.
            // For now, treat as unconstrained (conservative).
            Simplified::Bool(true)
        }
    }
}

fn values_equal_json(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    if a == b {
        return true;
    }
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

// ── Step 2: Simplify ─────────────────────────────────────────────────

fn simplify(node: Simplified) -> Simplified {
    match node {
        Simplified::Bool(b) => Simplified::Bool(b),

        Simplified::Not(inner) => {
            let s = simplify(*inner);
            match s {
                Simplified::Bool(b) => Simplified::Bool(!b),
                Simplified::FieldConstraint {
                    field,
                    kind: FieldConstraintKind::Equals(v),
                } => Simplified::FieldConstraint {
                    field,
                    kind: FieldConstraintKind::NotEquals(v),
                },
                // De Morgan: Not(Or(a, b, c)) → And(Not(a), Not(b), Not(c))
                Simplified::Or(children) => {
                    let negated = children
                        .into_iter()
                        .map(|c| simplify(Simplified::Not(Box::new(c))))
                        .collect();
                    simplify(Simplified::And(negated))
                }
                // De Morgan: Not(And(a, b, c)) → Or(Not(a), Not(b), Not(c))
                Simplified::And(children) => {
                    let negated = children
                        .into_iter()
                        .map(|c| simplify(Simplified::Not(Box::new(c))))
                        .collect();
                    simplify(Simplified::Or(negated))
                }
                other => Simplified::Not(Box::new(other)),
            }
        }

        Simplified::And(children) => {
            let simplified: Vec<Simplified> = children.into_iter().map(simplify).collect();
            // Short-circuit on false
            if simplified
                .iter()
                .any(|c| matches!(c, Simplified::Bool(false)))
            {
                return Simplified::Bool(false);
            }
            // Remove true constants
            let filtered: Vec<Simplified> = simplified
                .into_iter()
                .filter(|c| !matches!(c, Simplified::Bool(true)))
                .collect();
            match filtered.len() {
                0 => Simplified::Bool(true),
                1 => filtered.into_iter().next().unwrap(),
                _ => Simplified::And(filtered),
            }
        }

        Simplified::Or(children) => {
            let simplified: Vec<Simplified> = children.into_iter().map(simplify).collect();
            // Short-circuit on true
            if simplified
                .iter()
                .any(|c| matches!(c, Simplified::Bool(true)))
            {
                return Simplified::Bool(true);
            }
            // Remove false constants
            let filtered: Vec<Simplified> = simplified
                .into_iter()
                .filter(|c| !matches!(c, Simplified::Bool(false)))
                .collect();
            match filtered.len() {
                0 => Simplified::Bool(false),
                1 => filtered.into_iter().next().unwrap(),
                _ => Simplified::Or(filtered),
            }
        }

        fc @ Simplified::FieldConstraint { .. } => fc,
    }
}

// ── Step 3: Extract Predicate ────────────────────────────────────────

fn extract_predicate(node: &Simplified, target_field: &str) -> Option<Predicate> {
    match node {
        Simplified::Bool(true) => None, // All values allowed
        Simplified::Bool(false) => {
            // No values allowed — return an impossible predicate
            Some(Predicate::simple(target_field, "in", serde_json::json!([])))
        }

        Simplified::FieldConstraint { field, kind } if field == target_field => Some(match kind {
            FieldConstraintKind::Equals(v) => Predicate::simple(field, "equals", v.clone()),
            FieldConstraintKind::In(values) => {
                Predicate::simple(field, "in", serde_json::Value::Array(values.clone()))
            }
            FieldConstraintKind::NotEquals(v) => {
                Predicate::not(Predicate::simple(field, "equals", v.clone()))
            }
        }),
        Simplified::FieldConstraint { .. } => None, // Different field, ignore

        Simplified::Not(inner) => {
            let inner_pred = extract_predicate(inner, target_field)?;
            Some(Predicate::not(inner_pred))
        }

        Simplified::And(children) => {
            let preds: Vec<Predicate> = children
                .iter()
                .filter_map(|c| extract_predicate(c, target_field))
                .collect();
            match preds.len() {
                0 => None,
                1 => Some(preds.into_iter().next().unwrap()),
                _ => Some(Predicate::and(preds)),
            }
        }

        Simplified::Or(children) => {
            let preds: Vec<Predicate> = children
                .iter()
                .filter_map(|c| extract_predicate(c, target_field))
                .collect();
            match preds.len() {
                0 => None,
                1 => Some(preds.into_iter().next().unwrap()),
                _ => Some(Predicate::or(preds)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn status_combo_ast() -> ShaclAst {
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

        let or_children: Vec<ShaclAst> = forbidden
            .into_iter()
            .map(|(p, s)| ShaclAst::And {
                children: vec![
                    ShaclAst::PropEquals {
                        path: PropertyPath::iri(
                            "https://data.infrabel.be/asset360/ceAssetPrimaryStatus",
                        ),
                        value: json!(p),
                    },
                    ShaclAst::PropEquals {
                        path: PropertyPath::iri(
                            "https://data.infrabel.be/asset360/ceAssetSecondaryStatus",
                        ),
                        value: json!(s),
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
    fn test_solve_in_voorbereiding() {
        let ast = status_combo_ast();
        let mut known = serde_json::Map::new();
        known.insert("ceAssetPrimaryStatus".into(), json!("In_voorbereiding"));

        let pred = solve_backward(&ast, &known, "ceAssetSecondaryStatus");
        assert!(pred.is_some(), "should produce a predicate");
        let pred = pred.unwrap();

        // Should be AND of 4 NOT-EQUALS (Verkocht, Afgebroken, Aangevuld, Uit_dienst)
        let json = serde_json::to_value(&pred).unwrap();
        assert_eq!(json["operator"], "AND");
        let predicates = json["predicates"].as_array().unwrap();
        assert_eq!(
            predicates.len(),
            4,
            "4 forbidden secondary statuses for In_voorbereiding"
        );

        // Each should be a Negated predicate (operator: "NOT")
        for p in predicates {
            assert_eq!(p["operator"], "NOT");
        }
    }

    #[test]
    fn test_solve_in_opvolging() {
        let ast = status_combo_ast();
        let mut known = serde_json::Map::new();
        known.insert("ceAssetPrimaryStatus".into(), json!("In_opvolging"));

        let pred = solve_backward(&ast, &known, "ceAssetSecondaryStatus");
        assert!(pred.is_some());
        let json = serde_json::to_value(&pred.unwrap()).unwrap();
        let predicates = json["predicates"].as_array().unwrap();
        assert_eq!(
            predicates.len(),
            4,
            "4 forbidden secondary statuses for In_opvolging"
        );
    }

    #[test]
    fn test_solve_uit_opvolging() {
        let ast = status_combo_ast();
        let mut known = serde_json::Map::new();
        known.insert("ceAssetPrimaryStatus".into(), json!("Uit_opvolging"));

        let pred = solve_backward(&ast, &known, "ceAssetSecondaryStatus");
        assert!(pred.is_some());
        let pred = pred.unwrap();

        // Only 1 forbidden: In_dienst
        let json = serde_json::to_value(&pred).unwrap();
        assert_eq!(json["operator"], "NOT", "single negation, not AND");
    }

    #[test]
    fn test_solve_no_restrictions() {
        let ast = status_combo_ast();
        let mut known = serde_json::Map::new();
        // Use a primary status that has no forbidden combos
        known.insert("ceAssetPrimaryStatus".into(), json!("In_dienst"));

        let pred = solve_backward(&ast, &known, "ceAssetSecondaryStatus");
        assert!(pred.is_none(), "no restrictions for In_dienst");
    }

    #[test]
    fn test_solve_target_is_primary_status() {
        let ast = status_combo_ast();
        let mut known = serde_json::Map::new();
        known.insert("ceAssetSecondaryStatus".into(), json!("Verkocht"));

        let pred = solve_backward(&ast, &known, "ceAssetPrimaryStatus");
        assert!(pred.is_some());
        let json = serde_json::to_value(&pred.unwrap()).unwrap();
        // Verkocht is forbidden with In_voorbereiding and In_opvolging
        let predicates = json["predicates"].as_array().unwrap();
        assert_eq!(predicates.len(), 2);
    }

    #[test]
    fn test_solve_both_known() {
        let ast = status_combo_ast();
        let mut known = serde_json::Map::new();
        known.insert("ceAssetPrimaryStatus".into(), json!("In_voorbereiding"));
        known.insert("ceAssetSecondaryStatus".into(), json!("In_dienst"));

        // Both fields known, target field also known → no predicate needed
        let pred = solve_backward(&ast, &known, "ceAssetSecondaryStatus");
        assert!(
            pred.is_none(),
            "both fields known, valid combo → no restrictions"
        );
    }

    #[test]
    fn test_solve_no_known_fields() {
        let ast = status_combo_ast();
        let known = serde_json::Map::new();

        // No fields known → primary status is unknown (not target), becomes Bool(true).
        // This means each Or branch simplifies to just the secondary constraint,
        // producing Not(Or(secondary=X, secondary=Y, ...)) → AND of NOT-EQUALS.
        // The solver CAN produce a predicate: all forbidden secondary values are excluded.
        let pred = solve_backward(&ast, &known, "ceAssetSecondaryStatus");
        assert!(
            pred.is_some(),
            "unknown primary → all secondary constraints survive"
        );
        let json = serde_json::to_value(&pred.unwrap()).unwrap();
        assert_eq!(json["operator"], "AND");
        // 9 Not-Equals (one per forbidden combo; duplicates not eliminated)
        let predicates = json["predicates"].as_array().unwrap();
        assert_eq!(predicates.len(), 9, "9 forbidden combos → 9 NOT-EQUALS");
    }
}
