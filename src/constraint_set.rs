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
            if !shape.introspectable {
                continue;
            }
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

        // Normalize: any affected peer the caller didn't supply is treated as
        // JSON null, not a wildcard. The inner solver's "missing == wildcard"
        // is correct for forward eval but wrong for edit-session backward
        // solving — fix at the API boundary (see MR 438).
        for field in self.affected_fields() {
            if field != target_field {
                known.entry(field).or_insert(serde_json::Value::Null);
            }
        }

        // Collect predicates from all introspectable shapes that have an AST
        let mut predicates: Vec<Predicate> = Vec::new();
        for shape in &self.shapes {
            if shape.introspectable
                && let Some(ref ast) = shape.ast
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

    /// Backward-solve the allowed values for the `member_field` of a member being
    /// added or edited in the multivalued slot `array_field`.
    ///
    /// `object_data` is the full parent object (contains `array_field`).
    /// `editing_index` is `Some(i)` when editing the i-th existing member (its own
    /// value is then excluded from "already used"); `None` for a new member.
    ///
    /// Returns `AllowedValues` = (rule's `sh:in` set, or the member field's range
    /// enum when no `sh:in`) minus the values whose per-value capacity is already
    /// filled by the OTHER members. `None` when no `UniqueByMemberField` rule
    /// matches or the allowed universe cannot be determined.
    pub fn solve_member(
        &self,
        object_data: &serde_json::Value,
        array_field: &str,
        member_field: &str,
        editing_index: Option<usize>,
    ) -> Option<FieldConstraint> {
        // Member values already present on the OTHER members.
        let used_values: Vec<serde_json::Value> = match object_data.get(array_field) {
            Some(serde_json::Value::Array(arr)) => arr
                .iter()
                .enumerate()
                .filter(|(i, _)| Some(*i) != editing_index)
                .filter_map(|(_, m)| member_value(m, member_field))
                .collect(),
            _ => Vec::new(),
        };

        // Find the matching rule and its allowed set + excluded (capacity-filled) values.
        let solution = self
            .shapes
            .iter()
            .filter(|s| s.introspectable)
            .find_map(|s| {
                s.ast.as_ref().and_then(|ast| {
                    crate::backward_solver::solve_member_field(
                        ast,
                        array_field,
                        member_field,
                        &used_values,
                    )
                })
            })?;

        // Determine the allowed universe: the rule's sh:in, else the member
        // field's range enum (via schema).
        let universe: Vec<serde_json::Value> = match &solution.allowed_values {
            Some(values) => values.clone(),
            None => self
                .member_enum_keys(array_field, member_field)?
                .into_iter()
                .map(serde_json::Value::String)
                .collect(),
        };

        let allowed: Vec<String> = universe
            .iter()
            .filter(|v| !solution.excluded.iter().any(|e| json_eq(e, v)))
            .map(value_to_key)
            .collect();
        Some(FieldConstraint::AllowedValues { values: allowed })
    }

    /// Permissible enum keys of `member_field` on the range class of `array_field`,
    /// when a schema view is attached.
    fn member_enum_keys(&self, array_field: &str, member_field: &str) -> Option<Vec<String>> {
        let class_view = self.target_class.as_ref()?;
        let array_slot = class_view.slots().iter().find(|s| s.name == array_field)?;
        let range_class = array_slot.get_range_class()?;
        let member_slot = range_class
            .slots()
            .iter()
            .find(|s| s.name == member_field)?;
        let enum_view = member_slot.get_range_enum()?;
        enum_view.permissible_value_keys().ok().cloned()
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

/// Resolve the `member_field` value within one array-member JSON object.
/// `member_field` is a local name (the dotted/sequence case is not used here).
fn member_value(member: &serde_json::Value, member_field: &str) -> Option<serde_json::Value> {
    member.get(member_field).cloned()
}

/// Loose JSON equality mirroring the solver's string coercion.
fn json_eq(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    value_to_key(a) == value_to_key(b)
}

/// Stable string key for a JSON value (strings as-is, others stringified).
fn value_to_key(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

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

    fn file_links_shape() -> ShapeResult {
        ShapeResult {
            shape_uri: "asset360:FileLinksTypedShape".into(),
            target_class: "TunnelComplex".into(),
            enforcement_level: EnforcementLevel::Serious,
            message: "Each document type must be allowed and unique.".into(),
            affected_fields: vec!["fileLinksTyped".into(), "type".into()],
            introspectable: true,
            ast: Some(ShaclAst::UniqueByMemberField {
                array_path: PropertyPath::iri("https://data.infrabel.be/asset360/fileLinksTyped"),
                member_field: PropertyPath::iri("https://data.infrabel.be/asset360/type"),
                allowed_values: Some(vec![
                    json!("NetMapExcerpt"),
                    json!("RoadMapExcerpt"),
                    json!("NGIMapExcerpt"),
                    json!("Sketch"),
                ]),
                max_count_per_value: 1,
            }),
            sparql: None,
        }
    }

    fn file_links_cs() -> ConstraintSet {
        ConstraintSet {
            shapes: vec![file_links_shape()],
            schema_view: None,
            target_class: None,
        }
    }

    fn allowed_set(fc: Option<FieldConstraint>) -> Vec<String> {
        match fc {
            Some(FieldConstraint::AllowedValues { mut values }) => {
                values.sort();
                values
            }
            other => panic!("expected AllowedValues, got {other:?}"),
        }
    }

    #[test]
    fn test_solve_member_add_excludes_used() {
        let cs = file_links_cs();
        // One member already uses NetMapExcerpt; adding a new member (index None).
        let data = json!({"fileLinksTyped": [{"type": "NetMapExcerpt", "url": "u"}]});
        let allowed = allowed_set(cs.solve_member(&data, "fileLinksTyped", "type", None));
        assert_eq!(allowed, vec!["NGIMapExcerpt", "RoadMapExcerpt", "Sketch"]);
    }

    #[test]
    fn test_solve_member_edit_keeps_own_value() {
        let cs = file_links_cs();
        let data = json!({"fileLinksTyped": [
            {"type": "NetMapExcerpt", "url": "a"},
            {"type": "Sketch", "url": "b"}
        ]});
        // Editing row 0 (NetMapExcerpt): its own value stays available, Sketch (row 1) excluded.
        let allowed = allowed_set(cs.solve_member(&data, "fileLinksTyped", "type", Some(0)));
        assert_eq!(
            allowed,
            vec!["NGIMapExcerpt", "NetMapExcerpt", "RoadMapExcerpt"]
        );
    }

    #[test]
    fn test_solve_member_exhausted_is_empty() {
        let cs = file_links_cs();
        let data = json!({"fileLinksTyped": [
            {"type": "NetMapExcerpt"}, {"type": "RoadMapExcerpt"},
            {"type": "NGIMapExcerpt"}, {"type": "Sketch"}
        ]});
        let allowed = allowed_set(cs.solve_member(&data, "fileLinksTyped", "type", None));
        assert!(allowed.is_empty());
    }

    #[test]
    fn test_solve_member_no_array_returns_full_allowed() {
        let cs = file_links_cs();
        let data = json!({});
        let allowed = allowed_set(cs.solve_member(&data, "fileLinksTyped", "type", None));
        assert_eq!(
            allowed,
            vec!["NGIMapExcerpt", "NetMapExcerpt", "RoadMapExcerpt", "Sketch"]
        );
    }

    #[test]
    fn test_evaluate_blocks_duplicate_and_disallowed() {
        let cs = file_links_cs();
        let dup = json!({"fileLinksTyped": [{"type": "Sketch"}, {"type": "Sketch"}]});
        assert_eq!(cs.evaluate(&dup).len(), 1);
        let wrong = json!({"fileLinksTyped": [{"type": "Cassandra"}]});
        assert_eq!(cs.evaluate(&wrong).len(), 1);
        let ok = json!({"fileLinksTyped": [{"type": "Sketch"}, {"type": "NetMapExcerpt"}]});
        assert!(cs.evaluate(&ok).is_empty());
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

    // ── Null-normalization at the solve boundary (MR 438 follow-up) ──
    //
    // `backward_solver::substitute` treats a missing non-target field as
    // `Bool(true)` — correct for forward eval, wrong for edit-session
    // backward solving. `ConstraintSet::solve` is the boundary that fixes
    // the contract: any affected peer the caller didn't supply is normalized
    // to JSON null before delegation.

    #[test]
    fn test_solve_missing_peer_treated_as_null() {
        let cs = ConstraintSet {
            shapes: vec![status_combo_shape()],
            schema_view: None,
            target_class: None,
        };
        // No primary supplied. After normalization, primary=null causes every
        // `And(primary==X, secondary==Y)` branch to short-circuit to false,
        // so the outer `Not(Or(...))` is true and target is free.
        let result = cs.solve(&json!({}), "ceAssetSecondaryStatus");
        assert!(
            result.is_none(),
            "missing peer must behave like null, not wildcard"
        );
    }

    #[test]
    fn test_solve_missing_peer_matches_explicit_null() {
        let cs = ConstraintSet {
            shapes: vec![status_combo_shape()],
            schema_view: None,
            target_class: None,
        };
        let from_empty = cs.solve(&json!({}), "ceAssetSecondaryStatus");
        let from_null = cs.solve(
            &json!({ "ceAssetPrimaryStatus": null }),
            "ceAssetSecondaryStatus",
        );
        let to_json = |fc: Option<FieldConstraint>| serde_json::to_value(&fc).unwrap();
        assert_eq!(to_json(from_empty), to_json(from_null));
    }

    #[test]
    fn test_solve_missing_peer_prop_in() {
        // Not(And(PropIn(primary, [A,B]), PropEquals(secondary, "X")))
        // Missing primary → null → PropIn false → And false → Not true → free.
        let shape = ShapeResult {
            shape_uri: "asset360:PropInPeerShape".into(),
            target_class: "Thing".into(),
            enforcement_level: EnforcementLevel::Serious,
            message: "Forbidden when primary in {A,B} and secondary=X".into(),
            affected_fields: vec!["primary".into(), "secondary".into()],
            introspectable: true,
            ast: Some(ShaclAst::Not {
                child: Box::new(ShaclAst::And {
                    children: vec![
                        ShaclAst::PropIn {
                            path: PropertyPath::iri("https://example.org/primary"),
                            values: vec![json!("A"), json!("B")],
                        },
                        ShaclAst::PropEquals {
                            path: PropertyPath::iri("https://example.org/secondary"),
                            value: json!("X"),
                        },
                    ],
                }),
            }),
            sparql: None,
        };
        let cs = ConstraintSet {
            shapes: vec![shape],
            schema_view: None,
            target_class: None,
        };
        let result = cs.solve(&json!({}), "secondary");
        assert!(
            result.is_none(),
            "missing peer with PropIn must not over-restrict target"
        );
    }

    #[test]
    fn test_solve_missing_peer_prop_count() {
        // Not(And(PropCount(primary, min=1), PropEquals(secondary, "X")))
        // Missing primary → null → count=0, fails min=1 → false → free.
        let shape = ShapeResult {
            shape_uri: "asset360:PropCountPeerShape".into(),
            target_class: "Thing".into(),
            enforcement_level: EnforcementLevel::Serious,
            message: "Forbidden when primary present and secondary=X".into(),
            affected_fields: vec!["primary".into(), "secondary".into()],
            introspectable: true,
            ast: Some(ShaclAst::Not {
                child: Box::new(ShaclAst::And {
                    children: vec![
                        ShaclAst::PropCount {
                            path: PropertyPath::iri("https://example.org/primary"),
                            min: Some(1),
                            max: None,
                        },
                        ShaclAst::PropEquals {
                            path: PropertyPath::iri("https://example.org/secondary"),
                            value: json!("X"),
                        },
                    ],
                }),
            }),
            sparql: None,
        };
        let cs = ConstraintSet {
            shapes: vec![shape],
            schema_view: None,
            target_class: None,
        };
        let result = cs.solve(&json!({}), "secondary");
        assert!(
            result.is_none(),
            "missing peer with PropCount(min=1) must not over-restrict target"
        );
    }

    #[test]
    fn test_solve_target_not_coerced_to_null() {
        // Target absent from object_data must still produce a meaningful
        // predicate — normalization must skip the target field.
        let cs = ConstraintSet {
            shapes: vec![status_combo_shape()],
            schema_view: None,
            target_class: None,
        };
        let result = cs.solve(
            &json!({ "ceAssetPrimaryStatus": "In_voorbereiding" }),
            "ceAssetSecondaryStatus",
        );
        assert!(result.is_some(), "target stays free; constraints survive");
        match result.unwrap() {
            FieldConstraint::Query { predicate } => {
                let j = serde_json::to_value(&predicate).unwrap();
                assert_eq!(j["operator"], "AND");
                assert_eq!(
                    j["predicates"].as_array().unwrap().len(),
                    4,
                    "4 forbidden secondaries for In_voorbereiding"
                );
            }
            _ => panic!("expected Query"),
        }
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
