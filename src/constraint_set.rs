//! Unified constraint set: owns a set of SHACL shapes and exposes
//! evaluate, solve, scope, and affected_fields operations.

use serde::{Deserialize, Serialize};

use crate::predicate::Predicate;
use crate::shacl_ast::{ShapeResult, Violation};

#[cfg(feature = "shacl-parser")]
use crate::shacl_parser;

use linkml_schemaview::classview::ClassView;
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::SchemaView;

/// Describes the allowed values for a target field after backward solving.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum FieldConstraint {
    /// The target field's range is an enum with known permissible values;
    /// only the listed values satisfy all constraints.
    AllowedValues { values: Vec<String> },
    /// The constraint is expressed as a predicate (no enum information available).
    Query { predicate: Predicate },
}

/// A set of SHACL shapes that can be evaluated, solved, and scoped as a unit.
#[derive(Clone)]
pub struct ConstraintSet {
    shapes: Vec<ShapeResult>,
    schema_view: Option<SchemaView>,
    target_class: Option<ClassView>,
}

impl ConstraintSet {
    // ── Construction ─────────────────────────────────────────────────

    /// Deserialize a constraint set from a JSON array of `ShapeResult`s.
    pub fn from_json(json: &str) -> Result<Self, String> {
        let shapes: Vec<ShapeResult> =
            serde_json::from_str(json).map_err(|e| format!("invalid shapes JSON: {e}"))?;
        Ok(Self {
            shapes,
            schema_view: None,
            target_class: None,
        })
    }

    /// Serialize the shapes back to JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.shapes)
    }

    /// Parse SHACL Turtle text into a constraint set.
    #[cfg(feature = "shacl-parser")]
    pub fn from_shacl(
        ttl: &str,
        target_class: &str,
        language: &str,
        schema_view: Option<&SchemaView>,
    ) -> Result<Self, String> {
        let shapes = shacl_parser::parse_shacl(ttl, target_class, language)
            .map_err(|e| format!("SHACL parse error: {e}"))?;
        let mut cs = Self {
            shapes,
            schema_view: None,
            target_class: None,
        };
        if let Some(sv) = schema_view {
            cs = cs.with_schema_view(sv, target_class)?;
        }
        Ok(cs)
    }

    /// Attach a schema view and resolve the target class.
    pub fn with_schema_view(mut self, sv: &SchemaView, target_class: &str) -> Result<Self, String> {
        let conv = sv.converter();
        let class_view = sv
            .get_class(&Identifier::new(target_class), &conv)
            .map_err(|e| format!("error resolving class '{target_class}': {e:?}"))?
            .ok_or_else(|| format!("class '{target_class}' not found in schema"))?;
        self.schema_view = Some(sv.clone());
        self.target_class = Some(class_view);
        Ok(self)
    }

    // ── Operations ───────────────────────────────────────────────────

    /// Forward-evaluate all shapes against `object_data`, returning all violations.
    pub fn evaluate(&self, object_data: &serde_json::Value) -> Vec<Violation> {
        let mut violations = Vec::new();
        for shape in &self.shapes {
            if let Some(ref ast) = shape.ast {
                let vs = crate::forward_eval::evaluate_forward(
                    ast,
                    object_data,
                    &shape.message,
                    &shape.enforcement_level,
                );
                violations.extend(vs);
            }
        }
        violations
    }

    /// Backward-solve: determine the allowed values for `target_field` given `object_data`.
    pub fn solve(
        &self,
        object_data: &serde_json::Value,
        target_field: &str,
    ) -> Option<FieldConstraint> {
        let obj = object_data.as_object()?;

        // Build known fields = all object fields except the target
        let mut known = obj.clone();
        known.remove(target_field);

        // Collect predicates from all shapes that have an AST
        let mut predicates: Vec<Predicate> = Vec::new();
        for shape in &self.shapes {
            if let Some(ref ast) = shape.ast
                && let Some(pred) =
                    crate::backward_solver::solve_backward(ast, &known, target_field)
            {
                predicates.push(pred);
            }
        }

        if predicates.is_empty() {
            return None;
        }

        // AND-combine all predicates
        let combined = if predicates.len() == 1 {
            predicates.into_iter().next().unwrap()
        } else {
            Predicate::and(predicates)
        };

        // Try enum resolution if schema is available
        if let (Some(sv), Some(class_view)) = (&self.schema_view, &self.target_class) {
            // Find the slot matching target_field
            let _ = sv; // used indirectly via class_view
            for slot in class_view.slots() {
                if slot.name == target_field {
                    if let Some(enum_view) = slot.get_range_enum() {
                        // Slot has an enum range — filter permissible values
                        if let Ok(keys) = enum_view.permissible_value_keys() {
                            let passing: Vec<String> = keys
                                .iter()
                                .filter(|candidate| {
                                    evaluate_predicate_for_value(&combined, target_field, candidate)
                                })
                                .cloned()
                                .collect();
                            return Some(FieldConstraint::AllowedValues { values: passing });
                        }
                    }
                    break;
                }
            }
        }

        Some(FieldConstraint::Query {
            predicate: combined,
        })
    }

    /// Derive a scope predicate for fetching peer objects relevant to this constraint set.
    pub fn scope(
        &self,
        focus_data: &serde_json::Map<String, serde_json::Value>,
        uri_field: &str,
    ) -> Option<Predicate> {
        let mut predicates: Vec<Predicate> = Vec::new();
        for shape in &self.shapes {
            if let Some(pred) =
                crate::scope_predicate::derive_scope_predicate(shape, focus_data, uri_field)
            {
                predicates.push(pred);
            }
        }
        match predicates.len() {
            0 => None,
            1 => Some(predicates.into_iter().next().unwrap()),
            _ => Some(Predicate::or(predicates)),
        }
    }

    /// Return all field names referenced by any shape, sorted and deduplicated.
    pub fn affected_fields(&self) -> Vec<String> {
        let mut fields: Vec<String> = self
            .shapes
            .iter()
            .flat_map(|s| s.affected_fields.iter().cloned())
            .collect();
        fields.sort();
        fields.dedup();
        fields
    }

    /// Number of shapes in this constraint set.
    pub fn shape_count(&self) -> usize {
        self.shapes.len()
    }

    /// Whether a schema view has been attached.
    pub fn has_schema(&self) -> bool {
        self.schema_view.is_some()
    }
}

// ── Private helpers ──────────────────────────────────────────────────

/// Evaluate whether a candidate string value satisfies a predicate for a target field.
fn evaluate_predicate_for_value(pred: &Predicate, target_field: &str, candidate: &str) -> bool {
    match pred {
        Predicate::Simple {
            field_id,
            predicate_type_id,
            value,
        } => {
            if field_id != target_field {
                // Constraint on a different field — already resolved, treat as satisfied
                return true;
            }
            match predicate_type_id.as_str() {
                "equals" => match value {
                    Some(v) => values_equal_json_str(candidate, v),
                    None => false,
                },
                "notEquals" => match value {
                    Some(v) => !values_equal_json_str(candidate, v),
                    None => true,
                },
                "in" => match value {
                    Some(serde_json::Value::Array(arr)) => {
                        arr.iter().any(|v| values_equal_json_str(candidate, v))
                    }
                    _ => true, // Malformed, be permissive
                },
                _ => true, // Unknown operator, be permissive
            }
        }
        Predicate::Negated { predicate, .. } => {
            !evaluate_predicate_for_value(predicate, target_field, candidate)
        }
        Predicate::Expression {
            operator,
            predicates,
        } => {
            use crate::predicate::LogicalOperator;
            match operator {
                LogicalOperator::And => predicates
                    .iter()
                    .all(|p| evaluate_predicate_for_value(p, target_field, candidate)),
                LogicalOperator::Or => predicates
                    .iter()
                    .any(|p| evaluate_predicate_for_value(p, target_field, candidate)),
            }
        }
    }
}

/// Loose type coercion for comparing a string candidate against a JSON value.
fn values_equal_json_str(candidate: &str, value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(s) => candidate == s,
        serde_json::Value::Bool(b) => candidate == b.to_string(),
        serde_json::Value::Number(n) => candidate == n.to_string(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shacl_ast::{EnforcementLevel, PropertyPath, ShaclAst};
    use serde_json::json;

    fn status_combo_shape() -> ShapeResult {
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
        ShapeResult {
            shape_uri: "asset360:StatusComboShape".into(),
            target_class: "TunnelComponent".into(),
            enforcement_level: EnforcementLevel::Serious,
            message: "Forbidden status combination".into(),
            affected_fields: vec![
                "ceAssetPrimaryStatus".into(),
                "ceAssetSecondaryStatus".into(),
            ],
            introspectable: true,
            ast: Some(ShaclAst::Not {
                child: Box::new(ShaclAst::Or {
                    children: or_children,
                }),
            }),
            sparql: None,
        }
    }

    #[test]
    fn test_from_json_to_json_roundtrip() {
        let shapes = vec![status_combo_shape()];
        let json = serde_json::to_string(&shapes).unwrap();
        let cs = ConstraintSet::from_json(&json).unwrap();
        assert_eq!(cs.shape_count(), 1);
        let json2 = cs.to_json().unwrap();
        // Roundtrip should produce equivalent JSON
        let shapes2: Vec<ShapeResult> = serde_json::from_str(&json2).unwrap();
        assert_eq!(shapes2.len(), 1);
        assert_eq!(shapes2[0].shape_uri, "asset360:StatusComboShape");
    }

    #[test]
    fn test_evaluate_no_violations() {
        let cs = ConstraintSet {
            shapes: vec![status_combo_shape()],
            schema_view: None,
            target_class: None,
        };
        let data = json!({
            "ceAssetPrimaryStatus": "In_voorbereiding",
            "ceAssetSecondaryStatus": "In_dienst",
        });
        let violations = cs.evaluate(&data);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_evaluate_with_violation() {
        let cs = ConstraintSet {
            shapes: vec![status_combo_shape()],
            schema_view: None,
            target_class: None,
        };
        let data = json!({
            "ceAssetPrimaryStatus": "In_voorbereiding",
            "ceAssetSecondaryStatus": "Verkocht",
        });
        let violations = cs.evaluate(&data);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].message, "Forbidden status combination");
    }

    #[test]
    fn test_evaluate_two_shapes() {
        let shape1 = status_combo_shape();
        let shape2 = ShapeResult {
            shape_uri: "asset360:AnotherShape".into(),
            target_class: "TunnelComponent".into(),
            enforcement_level: EnforcementLevel::Error,
            message: "Another rule".into(),
            affected_fields: vec!["ceAssetPrimaryStatus".into()],
            introspectable: true,
            ast: Some(ShaclAst::PropIn {
                path: PropertyPath::iri("https://data.infrabel.be/asset360/ceAssetPrimaryStatus"),
                values: vec![json!("In_voorbereiding"), json!("In_opvolging")],
            }),
            sparql: None,
        };
        let cs = ConstraintSet {
            shapes: vec![shape1, shape2],
            schema_view: None,
            target_class: None,
        };
        // This data violates shape1 (forbidden combo) and passes shape2
        let data = json!({
            "ceAssetPrimaryStatus": "In_voorbereiding",
            "ceAssetSecondaryStatus": "Verkocht",
        });
        let violations = cs.evaluate(&data);
        assert_eq!(violations.len(), 1);

        // This data violates shape2 (primary not in allowed set) but passes shape1
        let data2 = json!({
            "ceAssetPrimaryStatus": "Uit_opvolging",
            "ceAssetSecondaryStatus": "Verkocht",
        });
        let violations2 = cs.evaluate(&data2);
        assert_eq!(violations2.len(), 1);
        assert_eq!(violations2[0].message, "Another rule");
    }

    #[test]
    fn test_solve_without_schema() {
        let cs = ConstraintSet {
            shapes: vec![status_combo_shape()],
            schema_view: None,
            target_class: None,
        };
        let data = json!({
            "ceAssetPrimaryStatus": "In_voorbereiding",
            "ceAssetSecondaryStatus": "In_dienst",
        });
        let result = cs.solve(&data, "ceAssetSecondaryStatus");
        assert!(result.is_some());
        match result.unwrap() {
            FieldConstraint::Query { predicate } => {
                let json = serde_json::to_value(&predicate).unwrap();
                // Should be AND of NOT-EQUALS for the 4 forbidden secondary statuses
                assert_eq!(json["operator"], "AND");
            }
            FieldConstraint::AllowedValues { .. } => {
                panic!("expected Query without schema");
            }
        }
    }

    #[test]
    fn test_solve_no_restrictions() {
        let cs = ConstraintSet {
            shapes: vec![status_combo_shape()],
            schema_view: None,
            target_class: None,
        };
        let data = json!({
            "ceAssetPrimaryStatus": "In_dienst",
            "ceAssetSecondaryStatus": "In_dienst",
        });
        let result = cs.solve(&data, "ceAssetSecondaryStatus");
        assert!(result.is_none(), "In_dienst has no forbidden combos");
    }

    #[test]
    fn test_affected_fields_dedup() {
        let shape1 = status_combo_shape();
        let shape2 = ShapeResult {
            shape_uri: "asset360:AnotherShape".into(),
            target_class: "TunnelComponent".into(),
            enforcement_level: EnforcementLevel::Error,
            message: "Another rule".into(),
            affected_fields: vec!["ceAssetPrimaryStatus".into(), "newField".into()],
            introspectable: true,
            ast: None,
            sparql: None,
        };
        let cs = ConstraintSet {
            shapes: vec![shape1, shape2],
            schema_view: None,
            target_class: None,
        };
        let fields = cs.affected_fields();
        assert_eq!(
            fields,
            vec![
                "ceAssetPrimaryStatus".to_string(),
                "ceAssetSecondaryStatus".to_string(),
                "newField".to_string(),
            ]
        );
    }

    #[test]
    fn test_scope_combining_multiple() {
        use crate::shacl_ast::EnforcementLevel;

        let shape = ShapeResult {
            shape_uri: "asset360:DelegateShape".into(),
            target_class: "TunnelComponent".into(),
            enforcement_level: EnforcementLevel::Serious,
            message: "Delegate uniqueness".into(),
            affected_fields: vec!["belongsToTunnelComplex".into(), "isTunnelDelegate".into()],
            introspectable: false,
            ast: None,
            sparql: Some(
                r#"
                SELECT $this ?path
                WHERE {
                    $this asset360:belongsToTunnelComplex ?complex ;
                          asset360:isTunnelDelegate true .
                    ?other asset360:belongsToTunnelComplex ?complex ;
                           asset360:isTunnelDelegate true .
                    FILTER(?other != $this)
                }
                "#
                .to_owned(),
            ),
        };
        let cs = ConstraintSet {
            shapes: vec![shape],
            schema_view: None,
            target_class: None,
        };

        let mut focus = serde_json::Map::new();
        focus.insert("asset360_uri".into(), json!("https://example.org/tc-42"));
        focus.insert("belongsToTunnelComplex".into(), json!("complex-7"));
        focus.insert("isTunnelDelegate".into(), json!(true));

        let pred = cs.scope(&focus, "asset360_uri");
        assert!(pred.is_some());
    }

    #[test]
    fn test_scope_no_scope_shapes() {
        let cs = ConstraintSet {
            shapes: vec![status_combo_shape()],
            schema_view: None,
            target_class: None,
        };
        let mut focus = serde_json::Map::new();
        focus.insert("asset360_uri".into(), json!("https://example.org/obj-1"));

        let pred = cs.scope(&focus, "asset360_uri");
        assert!(pred.is_none(), "single-object shape needs no scope");
    }

    // ── evaluate_predicate_for_value tests ───────────────────────────

    #[test]
    fn test_eval_pred_equals() {
        let pred = Predicate::simple("status", "equals", "Verkocht");
        assert!(evaluate_predicate_for_value(&pred, "status", "Verkocht"));
        assert!(!evaluate_predicate_for_value(&pred, "status", "In_dienst"));
    }

    #[test]
    fn test_eval_pred_not_equals() {
        let pred = Predicate::negate(Predicate::simple("status", "equals", "Verkocht"));
        assert!(!evaluate_predicate_for_value(&pred, "status", "Verkocht"));
        assert!(evaluate_predicate_for_value(&pred, "status", "In_dienst"));
    }

    #[test]
    fn test_eval_pred_in() {
        let pred = Predicate::simple("status", "in", json!(["A", "B", "C"]));
        assert!(evaluate_predicate_for_value(&pred, "status", "A"));
        assert!(evaluate_predicate_for_value(&pred, "status", "C"));
        assert!(!evaluate_predicate_for_value(&pred, "status", "D"));
    }

    #[test]
    fn test_eval_pred_different_field() {
        let pred = Predicate::simple("other_field", "equals", "X");
        // Constraint on a different field — should pass
        assert!(evaluate_predicate_for_value(&pred, "status", "anything"));
    }

    #[test]
    fn test_eval_pred_and() {
        let pred = Predicate::and(vec![
            Predicate::negate(Predicate::simple("status", "equals", "Verkocht")),
            Predicate::negate(Predicate::simple("status", "equals", "Afgebroken")),
        ]);
        assert!(evaluate_predicate_for_value(&pred, "status", "In_dienst"));
        assert!(!evaluate_predicate_for_value(&pred, "status", "Verkocht"));
        assert!(!evaluate_predicate_for_value(&pred, "status", "Afgebroken"));
    }

    #[test]
    fn test_eval_pred_or() {
        let pred = Predicate::or(vec![
            Predicate::simple("status", "equals", "A"),
            Predicate::simple("status", "equals", "B"),
        ]);
        assert!(evaluate_predicate_for_value(&pred, "status", "A"));
        assert!(evaluate_predicate_for_value(&pred, "status", "B"));
        assert!(!evaluate_predicate_for_value(&pred, "status", "C"));
    }
}
