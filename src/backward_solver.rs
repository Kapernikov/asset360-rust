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
    // Cross-reference path-equality is solved on the *tail* of `path_a`
    // (e.g. `refersToLine`), not on `target_field` (`belongsToTrack`). The
    // substitute/simplify/extract pipeline only extracts predicates keyed to
    // `target_field`, so it cannot carry this; handle it with a dedicated branch.
    if let ShaclAst::PathEquals { path_a, path_b } = ast {
        return solve_cross_ref_path_equals(path_a, path_b, known_fields, target_field);
    }

    let substituted = substitute(ast, known_fields, target_field);
    let simplified = simplify(substituted);
    extract_predicate(&simplified, target_field)
}

/// Solve a cross-reference path-equality (`path_a == path_b`) for the value a
/// reference `target_field` may take.
///
/// Recognized shape: `path_a` is a two-step sequence whose head is
/// `target_field` and whose single tail step is the slot on the referenced
/// object that must equal `path_b`'s value (a simple slot on the focus object,
/// read from `known_fields`). Returns a predicate keyed by the **tail** slot
/// (e.g. `refersToLine`), or `None` when the shape doesn't match or the peer
/// value is unbound. `path_b` keeps its value verbatim; an absent or JSON-null
/// peer yields `None` (caller then returns no constraint -> unfiltered dropdown).
fn solve_cross_ref_path_equals(
    path_a: &PropertyPath,
    path_b: &PropertyPath,
    known_fields: &serde_json::Map<String, serde_json::Value>,
    target_field: &str,
) -> Option<Predicate> {
    let PropertyPath::Sequence { steps } = path_a else {
        return None;
    };
    // Exactly: head (the target reference slot) + one tail step (the remote slot).
    if steps.len() != 2 {
        return None;
    }
    if steps[0].local_name()? != target_field {
        return None;
    }
    let tail_field = steps[1].local_name()?;
    let peer_field = path_b.local_name()?;
    let value = known_fields.get(peer_field)?;
    if value.is_null() {
        return None;
    }
    Some(Predicate::simple(tail_field, "equals", value.clone()))
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
            // If the field is known, evaluate directly. Otherwise, reason about
            // what concrete values can satisfy the constraint.
            if let Some(field_name) = path.local_name() {
                if let Some(known_val) = known.get(field_name) {
                    let count = match known_val {
                        serde_json::Value::Array(arr) => arr.len() as u32,
                        serde_json::Value::Null => 0,
                        _ => 1,
                    };
                    let ok = min.is_none_or(|m| count >= m) && max.is_none_or(|m| count <= m);
                    Simplified::Bool(ok)
                } else if field_name == target_field {
                    // Target field: reason about what concrete values can satisfy this.
                    // max=0 means "must be absent" → no concrete value works → false
                    // Otherwise (e.g. min≥1 "must be present") → any concrete value works → true
                    match max {
                        Some(0) => Simplified::Bool(false),
                        _ => Simplified::Bool(true),
                    }
                } else {
                    // Unknown non-target field: can't resolve, be conservative
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

        ShaclAst::UniqueByMemberField { .. } => {
            // Array-member uniqueness is not a scalar inter-field constraint;
            // it is solved by the dedicated `solve_member_field` path, not by
            // this scalar substitute/simplify/extract pipeline. Treat as
            // unconstrained here so it never interferes with scalar solving.
            Simplified::Bool(true)
        }
    }
}

/// Result of solving a multivalued-slot member field for the allowed values of
/// a new/edited member.
#[derive(Clone, Debug, PartialEq)]
pub struct MemberSolution {
    /// Allowed value set from the rule's `sh:in`, if any (before subtracting used).
    pub allowed_values: Option<Vec<serde_json::Value>>,
    /// Values already used up by the other members (capacity reached).
    pub excluded: Vec<serde_json::Value>,
}

/// Backward-solve a `UniqueByMemberField` rule for the value a new/edited member
/// may take in `member_field`. `used_values` is the multiset of `member_field`
/// values already present on the OTHER members (caller excludes the edited row).
///
/// Returns `None` when `ast` is not a `UniqueByMemberField` for this
/// `array_field` / `member_field`. Otherwise the rule's allowed set (if any) plus
/// the values whose per-value capacity is already reached and so must be excluded.
pub fn solve_member_field(
    ast: &ShaclAst,
    array_field: &str,
    member_field: &str,
    used_values: &[serde_json::Value],
) -> Option<MemberSolution> {
    match ast {
        // Only a conjunctive context yields an unconditional rule. Under `or` /
        // `not` the rule is conditional, so backward-solving it would over-restrict
        // the UI and disagree with forward evaluation — treat as unsolvable.
        ShaclAst::And { children } => children
            .iter()
            .find_map(|c| solve_member_field(c, array_field, member_field, used_values)),
        ShaclAst::UniqueByMemberField {
            array_path,
            member_field: mf,
            allowed_values,
            max_count_per_value,
        } => {
            if array_path.local_name() != Some(array_field) || mf.local_name() != Some(member_field)
            {
                return None;
            }
            // A value is excluded once the OTHER members already fill its capacity.
            let mut counts: std::collections::HashMap<String, (serde_json::Value, u32)> =
                std::collections::HashMap::new();
            for v in used_values {
                let key = match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                let entry = counts.entry(key).or_insert_with(|| (v.clone(), 0));
                entry.1 += 1;
            }
            let excluded = counts
                .into_values()
                .filter(|(_, n)| *n >= *max_count_per_value)
                .map(|(v, _)| v)
                .collect();
            Some(MemberSolution {
                allowed_values: allowed_values.clone(),
                excluded,
            })
        }
        _ => None,
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
                Predicate::negate(Predicate::simple(field, "equals", v.clone()))
            }
        }),
        Simplified::FieldConstraint { .. } => None, // Different field, ignore

        Simplified::Not(inner) => {
            let inner_pred = extract_predicate(inner, target_field)?;
            Some(Predicate::negate(inner_pred))
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

    fn member_rule() -> ShaclAst {
        ShaclAst::UniqueByMemberField {
            array_path: PropertyPath::iri("https://data.infrabel.be/asset360/fileLinksTyped"),
            member_field: PropertyPath::iri("https://data.infrabel.be/asset360/type"),
            allowed_values: Some(vec![json!("A"), json!("B")]),
            max_count_per_value: 1,
        }
    }

    #[test]
    fn test_solve_member_field_extracted_under_and() {
        let ast = ShaclAst::And {
            children: vec![member_rule()],
        };
        let sol = solve_member_field(&ast, "fileLinksTyped", "type", &[json!("A")]);
        assert!(sol.is_some());
        assert_eq!(sol.unwrap().excluded, vec![json!("A")]);
    }

    #[test]
    fn test_solve_member_field_not_extracted_under_or_or_not() {
        // Conditional contexts must be unsolvable (would over-restrict the UI).
        let under_or = ShaclAst::Or {
            children: vec![member_rule()],
        };
        let under_not = ShaclAst::Not {
            child: Box::new(member_rule()),
        };
        assert!(solve_member_field(&under_or, "fileLinksTyped", "type", &[]).is_none());
        assert!(solve_member_field(&under_not, "fileLinksTyped", "type", &[]).is_none());
    }

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
        let json = serde_json::to_value(pred.unwrap()).unwrap();
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
        let json = serde_json::to_value(pred.unwrap()).unwrap();
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
        let json = serde_json::to_value(pred.unwrap()).unwrap();
        assert_eq!(json["operator"], "AND");
        // 9 Not-Equals (one per forbidden combo; duplicates not eliminated)
        let predicates = json["predicates"].as_array().unwrap();
        assert_eq!(predicates.len(), 9, "9 forbidden combos → 9 NOT-EQUALS");
    }

    // ── Allowed-pattern tests ───────────────────────────────────────

    /// Build an allowed-pattern AST matching R1 from the MR:
    /// Or(
    ///   And(primary=="In_opvolging", Or(secondary=="In_dienst", secondary=="Uit_dienst")),
    ///   And(primary=="Uit_opvolging", Or(secondary=="Verkocht", secondary=="Afgebroken")),
    ///   PropCount(primary, max=0),    ← escape: primary absent
    ///   PropCount(secondary, max=0),  ← escape: secondary absent
    /// )
    fn allowed_pattern_ast() -> ShaclAst {
        let primary = |v: &str| ShaclAst::PropEquals {
            path: PropertyPath::iri("https://example.org/primaryStatus"),
            value: json!(v),
        };
        let secondary = |v: &str| ShaclAst::PropEquals {
            path: PropertyPath::iri("https://example.org/secondaryStatus"),
            value: json!(v),
        };

        ShaclAst::Or {
            children: vec![
                // Branch 1: In_opvolging → {In_dienst, Uit_dienst}
                ShaclAst::And {
                    children: vec![
                        primary("In_opvolging"),
                        ShaclAst::Or {
                            children: vec![secondary("In_dienst"), secondary("Uit_dienst")],
                        },
                    ],
                },
                // Branch 2: Uit_opvolging → {Verkocht, Afgebroken}
                ShaclAst::And {
                    children: vec![
                        primary("Uit_opvolging"),
                        ShaclAst::Or {
                            children: vec![secondary("Verkocht"), secondary("Afgebroken")],
                        },
                    ],
                },
                // Escape: primary absent
                ShaclAst::PropCount {
                    path: PropertyPath::iri("https://example.org/primaryStatus"),
                    min: None,
                    max: Some(0),
                },
                // Escape: secondary absent
                ShaclAst::PropCount {
                    path: PropertyPath::iri("https://example.org/secondaryStatus"),
                    min: None,
                    max: Some(0),
                },
            ],
        }
    }

    #[test]
    fn test_allowed_pattern_solve_primary_known() {
        let ast = allowed_pattern_ast();
        let mut known = serde_json::Map::new();
        known.insert("primaryStatus".into(), json!("In_opvolging"));

        let pred = solve_backward(&ast, &known, "secondaryStatus");
        assert!(pred.is_some(), "should produce a predicate");
        let json = serde_json::to_value(pred.unwrap()).unwrap();

        // Should be OR of Equals(In_dienst), Equals(Uit_dienst)
        assert_eq!(json["operator"], "OR");
        let predicates = json["predicates"].as_array().unwrap();
        assert_eq!(predicates.len(), 2, "2 allowed secondary statuses");
    }

    #[test]
    fn test_allowed_pattern_solve_bidirectional() {
        let ast = allowed_pattern_ast();
        let mut known = serde_json::Map::new();
        known.insert("secondaryStatus".into(), json!("Verkocht"));

        let pred = solve_backward(&ast, &known, "primaryStatus");
        assert!(pred.is_some(), "should produce a predicate");
        let json = serde_json::to_value(pred.unwrap()).unwrap();

        // Verkocht is only allowed with Uit_opvolging → Equals("Uit_opvolging")
        assert_eq!(json["fieldId"], "primaryStatus");
        assert_eq!(json["predicateTypeId"], "equals");
        assert_eq!(json["value"], "Uit_opvolging");
    }

    #[test]
    fn test_allowed_pattern_no_known_fields() {
        let ast = allowed_pattern_ast();
        let known = serde_json::Map::new();

        // No fields known → escape clause PropCount(primary, max=0) becomes
        // Bool(true) (unknown non-target), which short-circuits the Or → None.
        let pred = solve_backward(&ast, &known, "secondaryStatus");
        assert!(
            pred.is_none(),
            "no known fields → escape clause makes it unconstrained"
        );
    }

    #[test]
    fn test_allowed_pattern_wall_type_no_section() {
        // Simulate R3: wall type requires no sectionType.
        // Or(
        //   And(sectionType=="Rectangle", Or(concept=="Bridge", concept=="Tunnel")),
        //   And(Or(concept=="Gabion_Wall"), PropCount(sectionType, max=0)),
        //   PropCount(sectionType, max=0),   ← escape
        //   PropCount(concept, max=0),       ← escape
        // )
        let section = |v: &str| ShaclAst::PropEquals {
            path: PropertyPath::iri("https://example.org/sectionType"),
            value: json!(v),
        };
        let concept = |v: &str| ShaclAst::PropEquals {
            path: PropertyPath::iri("https://example.org/constructionConcept"),
            value: json!(v),
        };

        let ast = ShaclAst::Or {
            children: vec![
                ShaclAst::And {
                    children: vec![
                        section("Rectangle"),
                        ShaclAst::Or {
                            children: vec![concept("Bridge"), concept("Tunnel")],
                        },
                    ],
                },
                ShaclAst::And {
                    children: vec![
                        ShaclAst::Or {
                            children: vec![concept("Gabion_Wall")],
                        },
                        ShaclAst::PropCount {
                            path: PropertyPath::iri("https://example.org/sectionType"),
                            min: None,
                            max: Some(0),
                        },
                    ],
                },
                ShaclAst::PropCount {
                    path: PropertyPath::iri("https://example.org/sectionType"),
                    min: None,
                    max: Some(0),
                },
                ShaclAst::PropCount {
                    path: PropertyPath::iri("https://example.org/constructionConcept"),
                    min: None,
                    max: Some(0),
                },
            ],
        };

        let mut known = serde_json::Map::new();
        known.insert("constructionConcept".into(), json!("Gabion_Wall"));

        let pred = solve_backward(&ast, &known, "sectionType");
        assert!(pred.is_some(), "should produce a predicate");
        let json = serde_json::to_value(pred.unwrap()).unwrap();

        // Gabion_Wall requires no sectionType → impossible predicate (empty in)
        assert_eq!(json["fieldId"], "sectionType");
        assert_eq!(json["predicateTypeId"], "in");
        assert_eq!(json["value"], json!([]));
    }

    #[test]
    fn test_propcount_max0_target_field() {
        // PropCount(target, max=0) alone → should resolve to Bool(false)
        let ast = ShaclAst::PropCount {
            path: PropertyPath::iri("https://example.org/myField"),
            min: None,
            max: Some(0),
        };
        let known = serde_json::Map::new();
        let pred = solve_backward(&ast, &known, "myField");

        // Bool(false) → impossible predicate
        assert!(pred.is_some());
        let json = serde_json::to_value(pred.unwrap()).unwrap();
        assert_eq!(json["predicateTypeId"], "in");
        assert_eq!(json["value"], json!([]));
    }

    #[test]
    fn test_propcount_min1_target_field() {
        // PropCount(target, min=1) → should resolve to Bool(true) → None
        let ast = ShaclAst::PropCount {
            path: PropertyPath::iri("https://example.org/myField"),
            min: Some(1),
            max: None,
        };
        let known = serde_json::Map::new();
        let pred = solve_backward(&ast, &known, "myField");

        assert!(
            pred.is_none(),
            "min=1 on target → any concrete value works → None"
        );
    }

    fn track_line_path_equals() -> ShaclAst {
        ShaclAst::PathEquals {
            path_a: PropertyPath::sequence(vec![
                PropertyPath::iri("https://data.infrabel.be/asset360/belongsToTrack"),
                PropertyPath::iri("https://data.infrabel.be/asset360/refersToLine"),
            ]),
            path_b: PropertyPath::iri("https://data.infrabel.be/asset360/belongsToLine"),
        }
    }

    #[test]
    fn test_solve_cross_ref_path_equals() {
        let ast = track_line_path_equals();
        let known = json!({ "belongsToLine": "Line-9" })
            .as_object()
            .unwrap()
            .clone();
        let pred = solve_backward(&ast, &known, "belongsToTrack");
        assert_eq!(
            pred,
            Some(Predicate::simple("refersToLine", "equals", "Line-9"))
        );
    }

    #[test]
    fn test_solve_cross_ref_unsolvable_cases_are_none() {
        let ast = track_line_path_equals();

        // Peer field absent entirely.
        let empty = serde_json::Map::new();
        assert_eq!(solve_backward(&ast, &empty, "belongsToTrack"), None);

        // Peer field explicitly null.
        let null_known = json!({ "belongsToLine": null })
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(solve_backward(&ast, &null_known, "belongsToTrack"), None);

        // Target field is not the sequence head.
        let known = json!({ "belongsToLine": "Line-9" })
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(solve_backward(&ast, &known, "somethingElse"), None);
    }
}
