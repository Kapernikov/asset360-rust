//! Unified constraint set: owns a set of SHACL shapes and exposes
//! evaluate, solve, scope, and affected_fields operations.

use serde::{Deserialize, Serialize};

use crate::predicate::Predicate;
use crate::shacl_ast::{PathSegment, ShapeResult, Violation};

#[cfg(feature = "shacl-parser")]
use crate::shacl_parser;

use linkml_schemaview::classview::ClassView;
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::SchemaView;
#[cfg(feature = "shacl-parser")]
use linkml_schemaview::slotview::SlotInlineMode;

use linkml_runtime::LinkMLInstance;

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
    schema_view: SchemaView,
    target_class: ClassView,
    /// `target_class` and its `is_a` ancestors, nearest first: a shape on any
    /// of them applies to the root, as `sh:targetClass` reaches instances of
    /// subclasses.
    root_lineage: Vec<String>,
}

impl ConstraintSet {
    // ── Construction ─────────────────────────────────────────────────

    /// Deserialize a constraint set for `target_class` from a JSON array of
    /// `ShapeResult`s (as written by [`to_json`](Self::to_json)).
    pub fn from_json(
        json: &str,
        schema_view: &SchemaView,
        target_class: &str,
    ) -> Result<Self, String> {
        let shapes: Vec<ShapeResult> =
            serde_json::from_str(json).map_err(|e| format!("invalid shapes JSON: {e}"))?;
        let root = resolve_class(schema_view, target_class)?;
        Ok(Self {
            shapes,
            schema_view: schema_view.clone(),
            root_lineage: class_lineage(&root)?,
            target_class: root,
        })
    }

    /// Serialize the shapes back to JSON: the root class's and the nested
    /// classes' alike, so [`from_json`](Self::from_json) rebuilds the same set.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.shapes)
    }

    /// Serialize only the shapes that target the root class itself.
    ///
    /// For a consumer that places violations on the root object's own fields
    /// (a form's live check): a nested shape's fields belong to the nested
    /// object, not to the root.
    pub fn root_shapes_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.root_shapes().collect::<Vec<_>>())
    }

    /// Parse SHACL Turtle text into a constraint set for `target_class`.
    ///
    /// The set also carries the introspectable shapes of every class inlined
    /// inside `target_class`, recursively (single-valued, list and mapping
    /// slots, and the descendants of each range class). SHACL's
    /// `sh:targetClass` applies to every node of the class, nested ones
    /// included; this is how forward evaluation honours that. Those shapes
    /// only take part in [`evaluate`](Self::evaluate). A shape on an `is_a`
    /// ancestor of a class applies to it as well, at the root and nested.
    #[cfg(feature = "shacl-parser")]
    pub fn from_shacl(
        ttl: &str,
        target_class: &str,
        language: &str,
        schema_view: &SchemaView,
    ) -> Result<Self, String> {
        let root = resolve_class(schema_view, target_class)?;
        let root_lineage = class_lineage(&root)?;
        // The nested classes and the ancestors of each, whose shapes reach them.
        let mut nested = std::collections::BTreeSet::new();
        for class_name in nested_class_names(&root)? {
            for ancestor in class_lineage(&resolve_class(schema_view, &class_name)?)? {
                if !root_lineage.contains(&ancestor) {
                    nested.insert(ancestor);
                }
            }
        }
        let wanted: Vec<&str> = std::iter::once(target_class)
            .chain(root_lineage.iter().map(String::as_str))
            .chain(nested.iter().map(String::as_str))
            .collect();
        let parsed = shacl_parser::parse_shacl_for_classes(ttl, &wanted, language)
            .map_err(|e| format!("SHACL parse error: {e}"))?;
        // Nested classes only contribute what the Rust engine can evaluate.
        let shapes = parsed
            .into_iter()
            .filter(|shape| root_lineage.contains(&shape.target_class) || shape.introspectable)
            .collect();
        Ok(Self {
            shapes,
            root_lineage,
            schema_view: schema_view.clone(),
            target_class: root,
        })
    }

    // ── Operations ───────────────────────────────────────────────────

    /// Forward-evaluate all shapes against `object_data`, returning all violations.
    ///
    /// Root shapes run on `object_data` itself. Nested shapes (see
    /// [`from_shacl`](Self::from_shacl)) run on every
    /// inlined object of their target class, and their violations carry the
    /// object's `path` and `element_label`.
    ///
    /// Fails when nested shapes exist and `object_data` cannot be loaded as the
    /// target class at all, since none of them could then be checked.
    /// Validation issues on a loadable object (a half-filled form) are not a
    /// failure: its nested objects are still evaluated.
    pub fn evaluate(&self, object_data: &serde_json::Value) -> Result<Vec<Violation>, String> {
        let mut violations = Vec::new();
        for shape in self.root_shapes() {
            if !shape.introspectable {
                continue;
            }
            violations.extend(crate::forward_eval::evaluate_forward(shape, object_data));
        }
        self.evaluate_nested(object_data, &mut violations)?;
        Ok(violations)
    }

    fn evaluate_nested(
        &self,
        object_data: &serde_json::Value,
        out: &mut Vec<Violation>,
    ) -> Result<(), String> {
        if !self
            .shapes
            .iter()
            .any(|s| s.introspectable && !self.is_root_shape(s))
        {
            return Ok(());
        }
        // Loading through the runtime rather than walking the raw JSON is what
        // resolves each nested object's concrete class (type designators,
        // descendants) and fills a mapping entry's key slot from its dict key.
        let sv = &self.schema_view;
        let root = &self.target_class;
        let conv = sv.converter();
        let loaded =
            LinkMLInstance::from_json(object_data.clone(), root.clone(), None, sv, &conv, false);
        let Some(instance) = loaded.instance else {
            let issues: Vec<String> = loaded
                .validation_issues
                .iter()
                .map(|issue| format!("{issue:?}"))
                .collect();
            return Err(format!(
                "cannot load the object as {} to evaluate its nested shapes: {}",
                root.name(),
                issues.join("; ")
            ));
        };
        self.walk_nested(&instance, &mut Vec::new(), None, out)
    }

    fn walk_nested(
        &self,
        node: &LinkMLInstance,
        path: &mut Vec<PathSegment>,
        label: Option<&str>,
        out: &mut Vec<Violation>,
    ) -> Result<(), String> {
        match node {
            LinkMLInstance::Object { values, class, .. } => {
                if !path.is_empty() {
                    // Matched by the object's class and its ancestors, so a
                    // subclass instance gets its parents' shapes, and a nested
                    // object of the root class gets the root shapes.
                    let lineage = class_lineage(class)?;
                    let applicable = self
                        .shapes
                        .iter()
                        .filter(|s| s.introspectable && lineage.contains(&s.target_class));
                    let mut data = None;
                    for shape in applicable {
                        let data = data.get_or_insert_with(|| node.to_json());
                        for mut violation in crate::forward_eval::evaluate_forward(shape, data) {
                            violation.path = path.clone();
                            violation.element_label = label.map(str::to_owned);
                            out.push(violation);
                        }
                    }
                }
                let mut keys: Vec<&String> = values.keys().collect();
                keys.sort();
                for key in keys {
                    path.push(PathSegment::Key(key.clone()));
                    self.walk_nested(&values[key], path, label, out)?;
                    path.pop();
                }
            }
            LinkMLInstance::List { values, .. } => {
                for (index, child) in values.iter().enumerate() {
                    let own = linkml_runtime::element_identity_label(child)
                        .unwrap_or_else(|| (index + 1).to_string());
                    path.push(PathSegment::Index(index));
                    self.walk_nested(child, path, Some(&own), out)?;
                    path.pop();
                }
            }
            LinkMLInstance::Mapping { values, .. } => {
                let mut keys: Vec<&String> = values.keys().collect();
                keys.sort();
                for key in keys {
                    let child = &values[key];
                    let own = linkml_runtime::element_identity_label(child)
                        .unwrap_or_else(|| key.clone());
                    path.push(PathSegment::Key(key.clone()));
                    self.walk_nested(child, path, Some(&own), out)?;
                    path.pop();
                }
            }
            LinkMLInstance::Scalar { .. } | LinkMLInstance::Null { .. } => {}
        }
        Ok(())
    }

    /// Whether `shape` targets the class this set was built for or one of its
    /// ancestors. Everything else in the set belongs to a class nested inside
    /// it. Decided against the set's own class, not stored on the shape: the
    /// same shape is a root shape in a set built for its own class.
    fn is_root_shape(&self, shape: &ShapeResult) -> bool {
        self.root_lineage.contains(&shape.target_class)
    }

    /// Shapes on the root class.
    fn root_shapes(&self) -> impl Iterator<Item = &ShapeResult> {
        self.shapes.iter().filter(|s| self.is_root_shape(s))
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
        for shape in self.root_shapes() {
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

        // Enum resolution: an enum-ranged target field gets its passing values
        if let Some(slot) = self
            .target_class
            .slots()
            .iter()
            .find(|slot| slot.name == target_field)
            && let Some(enum_view) = slot.get_range_enum()
            && let Ok(keys) = enum_view.permissible_value_keys()
        {
            let passing: Vec<String> = keys
                .iter()
                .filter(|candidate| {
                    evaluate_predicate_for_value(&combined, target_field, candidate)
                })
                .cloned()
                .collect();
            return Some(FieldConstraint::AllowedValues { values: passing });
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
            .root_shapes()
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

    /// Permissible enum keys of `member_field` on the range class of `array_field`.
    fn member_enum_keys(&self, array_field: &str, member_field: &str) -> Option<Vec<String>> {
        let class_view = &self.target_class;
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
        for shape in self.root_shapes() {
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
            .root_shapes()
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

    /// Name of the class this set was built for.
    pub fn target_class_name(&self) -> &str {
        self.target_class.name()
    }
}

// ── Private helpers ──────────────────────────────────────────────────

/// Names of the classes an instance of `root` can hold inlined, at any depth:
/// the range class of every inlined slot and that class's descendants, sorted.
/// `root` itself is left out; its shapes are already the set's root shapes.
#[cfg(feature = "shacl-parser")]
fn nested_class_names(root: &ClassView) -> Result<Vec<String>, String> {
    let mut found = std::collections::BTreeSet::new();
    let mut queue = vec![root.clone()];
    while let Some(class) = queue.pop() {
        for slot in class.slots() {
            if slot.determine_slot_inline_mode() != SlotInlineMode::Inline {
                continue;
            }
            let Some(range) = slot.get_range_class() else {
                continue;
            };
            let descendants = range
                .get_descendants(true, false)
                .map_err(|e| format!("error resolving descendants of '{}': {e:?}", range.name()))?;
            for candidate in std::iter::once(range).chain(descendants) {
                if candidate.name() != root.name() && found.insert(candidate.name().to_owned()) {
                    queue.push(candidate);
                }
            }
        }
    }
    Ok(found.into_iter().collect())
}

/// `class` and its `is_a` ancestors, nearest first.
fn class_lineage(class: &ClassView) -> Result<Vec<String>, String> {
    let mut lineage = vec![class.name().to_owned()];
    let mut current = class.clone();
    while let Some(parent) = current
        .parent_class()
        .map_err(|e| format!("error resolving the parent of '{}': {e:?}", current.name()))?
    {
        lineage.push(parent.name().to_owned());
        current = parent;
    }
    Ok(lineage)
}

fn resolve_class(schema_view: &SchemaView, target_class: &str) -> Result<ClassView, String> {
    let conv = schema_view.converter();
    schema_view
        .get_class(&Identifier::new(target_class), &conv)
        .map_err(|e| format!("error resolving class '{target_class}': {e:?}"))?
        .ok_or_else(|| format!("class '{target_class}' not found in schema"))
}

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

/// Schema of slot-less classes, one per class the hand-built test shapes
/// target, for tests that only care about the shapes. `class` picks the
/// set's root.
#[cfg(test)]
pub(crate) fn bare_schema(class: &str) -> (SchemaView, ClassView) {
    use linkml_meta::SchemaDefinition;
    use serde_path_to_error as p2e;
    use serde_yml as yml;

    const BARE: &str = "
id: https://example.org/bare
name: bare
prefixes:
  linkml: https://w3id.org/linkml/
  ex: https://example.org/bare/
default_prefix: ex
default_range: string
imports:
  - linkml:types
classes:
  TunnelComponent: {}
  TunnelComplex: {}
  CoveredSection: {}
  Thing: {}
";
    let mut sv = SchemaView::new();
    for raw in [include_str!("../tests/data/types.yaml"), BARE] {
        let schema: SchemaDefinition = p2e::deserialize(yml::Deserializer::from_str(raw)).unwrap();
        sv.add_schema(schema).unwrap();
    }
    let class = resolve_class(&sv, class).unwrap();
    (sv, class)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shacl_ast::{EnforcementLevel, PropertyPath, ShaclAst};
    use serde_json::json;

    /// A set rooted at the shapes' own (slot-less) class: every field a test
    /// shape names is outside the schema, so solving falls back to a `Query`
    /// and no enum or member lookup kicks in.
    fn bare(shapes: Vec<ShapeResult>) -> ConstraintSet {
        let class = shapes[0].target_class.clone();
        assert!(shapes.iter().all(|s| s.target_class == class));
        let (schema_view, target_class) = bare_schema(&class);
        ConstraintSet {
            shapes,
            schema_view,
            root_lineage: class_lineage(&target_class).unwrap(),
            target_class,
        }
    }

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
        bare(vec![file_links_shape()])
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
        assert_eq!(cs.evaluate(&dup).unwrap().len(), 1);
        let wrong = json!({"fileLinksTyped": [{"type": "Cassandra"}]});
        assert_eq!(cs.evaluate(&wrong).unwrap().len(), 1);
        let ok = json!({"fileLinksTyped": [{"type": "Sketch"}, {"type": "NetMapExcerpt"}]});
        assert!(cs.evaluate(&ok).unwrap().is_empty());
    }

    #[test]
    fn test_from_json_to_json_roundtrip() {
        let shapes = vec![status_combo_shape()];
        let json = serde_json::to_string(&shapes).unwrap();
        let (sv, _) = bare_schema("TunnelComponent");
        let cs = ConstraintSet::from_json(&json, &sv, "TunnelComponent").unwrap();
        assert_eq!(cs.shape_count(), 1);
        let json2 = cs.to_json().unwrap();
        // Roundtrip should produce equivalent JSON
        let shapes2: Vec<ShapeResult> = serde_json::from_str(&json2).unwrap();
        assert_eq!(shapes2.len(), 1);
        assert_eq!(shapes2[0].shape_uri, "asset360:StatusComboShape");
    }

    #[test]
    fn test_evaluate_no_violations() {
        let cs = bare(vec![status_combo_shape()]);
        let data = json!({
            "ceAssetPrimaryStatus": "In_voorbereiding",
            "ceAssetSecondaryStatus": "In_dienst",
        });
        let violations = cs.evaluate(&data).unwrap();
        assert!(violations.is_empty());
    }

    #[test]
    fn test_evaluate_with_violation() {
        let cs = bare(vec![status_combo_shape()]);
        let data = json!({
            "ceAssetPrimaryStatus": "In_voorbereiding",
            "ceAssetSecondaryStatus": "Verkocht",
        });
        let violations = cs.evaluate(&data).unwrap();
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].message, "Forbidden status combination");
        // The consumer groups findings per rule, so the shape identity has to
        // survive evaluation — the message is localized and cannot serve.
        assert_eq!(
            violations[0].shape_uri.as_deref(),
            Some("asset360:StatusComboShape")
        );

        // A blank-node shape has no identity worth propagating: renumbered on
        // every parse, so an id here would be worse than none.
        let mut anonymous = status_combo_shape();
        anonymous.shape_uri = "_:b7".into();
        let cs = bare(vec![anonymous]);
        let violations = cs.evaluate(&data).unwrap();
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].shape_uri, None);
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
        let cs = bare(vec![shape1, shape2]);
        // This data violates shape1 (forbidden combo) and passes shape2
        let data = json!({
            "ceAssetPrimaryStatus": "In_voorbereiding",
            "ceAssetSecondaryStatus": "Verkocht",
        });
        let violations = cs.evaluate(&data).unwrap();
        assert_eq!(violations.len(), 1);

        // This data violates shape2 (primary not in allowed set) but passes shape1
        let data2 = json!({
            "ceAssetPrimaryStatus": "Uit_opvolging",
            "ceAssetSecondaryStatus": "Verkocht",
        });
        let violations2 = cs.evaluate(&data2).unwrap();
        assert_eq!(violations2.len(), 1);
        assert_eq!(violations2[0].message, "Another rule");
        assert_eq!(
            violations2[0].shape_uri.as_deref(),
            Some("asset360:AnotherShape")
        );
    }

    #[test]
    fn test_solve_field_outside_schema_returns_query() {
        let cs = bare(vec![status_combo_shape()]);
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
        let cs = bare(vec![status_combo_shape()]);
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
        let cs = bare(vec![status_combo_shape()]);
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
        let cs = bare(vec![status_combo_shape()]);
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
        let cs = bare(vec![shape]);
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
        let cs = bare(vec![shape]);
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
        let cs = bare(vec![status_combo_shape()]);
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
        let cs = bare(vec![shape1, shape2]);
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
        let cs = bare(vec![shape]);

        let mut focus = serde_json::Map::new();
        focus.insert("asset360_uri".into(), json!("https://example.org/tc-42"));
        focus.insert("belongsToTunnelComplex".into(), json!("complex-7"));
        focus.insert("isTunnelDelegate".into(), json!(true));

        let pred = cs.scope(&focus, "asset360_uri");
        assert!(pred.is_some());
    }

    #[test]
    fn test_scope_no_scope_shapes() {
        let cs = bare(vec![status_combo_shape()]);
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

    // ── Cross-reference PathEquals end-to-end ────────────────────────

    fn track_line_cs() -> ConstraintSet {
        bare(vec![ShapeResult {
            shape_uri: "asset360:CoveredSection_TrackLineConsistencyShape".into(),
            target_class: "CoveredSection".into(),
            enforcement_level: EnforcementLevel::Serious,
            message: "Track must be on the section's line.".into(),
            affected_fields: vec!["belongsToLine".into()],
            introspectable: true,
            ast: Some(ShaclAst::PathEquals {
                path_a: PropertyPath::sequence(vec![
                    PropertyPath::iri("https://data.infrabel.be/asset360/belongsToTrack"),
                    PropertyPath::iri("https://data.infrabel.be/asset360/refersToLine"),
                ]),
                path_b: PropertyPath::iri("https://data.infrabel.be/asset360/belongsToLine"),
            }),
            sparql: None,
        }])
    }

    #[test]
    fn test_solve_cross_ref_returns_query() {
        let cs = track_line_cs();
        let data = json!({ "belongsToLine": "Line-9" });
        match cs.solve(&data, "belongsToTrack") {
            Some(FieldConstraint::Query { predicate }) => {
                assert_eq!(
                    predicate,
                    Predicate::simple("refersToLine", "equals", "Line-9")
                );
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn test_solve_cross_ref_no_line_is_none() {
        let cs = track_line_cs();
        // `belongsToLine` is an affected field, so solve() normalizes its
        // absence to JSON null -> no predicate -> no constraint -> unfiltered.
        assert!(cs.solve(&json!({}), "belongsToTrack").is_none());
    }

    #[test]
    fn test_solve_cross_ref_with_embedded_class_schema() {
        use linkml_meta::SchemaDefinition;
        use serde_path_to_error as p2e;
        use serde_yml as yml;

        let schema_sources = [
            include_str!("../tests/data/types.yaml"),
            include_str!("../tests/data/rsm.yaml"),
            include_str!("../tests/data/eulynx.yaml"),
            include_str!("../tests/data/asset360.yaml"),
        ];
        let mut sv = SchemaView::new();
        for raw in schema_sources {
            let schema: SchemaDefinition =
                p2e::deserialize(yml::Deserializer::from_str(raw)).unwrap();
            sv.add_schema(schema).unwrap();
        }

        // Attaching the schema resolves CoveredSection (an embedded-only class)
        // via get_class — this is the "small risk" the spec flags.
        let cs =
            ConstraintSet::from_json(&track_line_cs().to_json().unwrap(), &sv, "CoveredSection")
                .unwrap();

        let data = json!({ "belongsToLine": "https://data.infrabel.be/asset360/Line-9" });
        match cs.solve(&data, "belongsToTrack") {
            // belongsToTrack's range is the Track class (not an enum), so the
            // enum branch is skipped and we get a Query, not AllowedValues.
            Some(FieldConstraint::Query { predicate }) => {
                assert_eq!(
                    predicate,
                    Predicate::simple(
                        "refersToLine",
                        "equals",
                        "https://data.infrabel.be/asset360/Line-9"
                    )
                );
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    // ── Nested evaluation ────────────────────────────────────────────

    #[cfg(feature = "shacl-parser")]
    const NESTED_SCHEMA: &str = r#"
id: https://example.org/nested
name: nested
prefixes:
  linkml: https://w3id.org/linkml/
  ex: https://example.org/nested/
default_prefix: ex
default_range: string
imports:
  - linkml:types
classes:
  Holder:
    attributes:
      id:
        identifier: true
      name: {}
      elements:
        range: Element
        multivalued: true
        inlined: true
      sections:
        range: Section
        multivalued: true
        inlined_as_list: true
      notes:
        range: Note
        multivalued: true
        inlined_as_list: true
      holderLoad:
        range: Load
        inlined: true
  Element:
    attributes:
      elementType:
        key: true
      load:
        range: Load
        inlined: true
      parts:
        range: Part
        multivalued: true
        inlined_as_list: true
  Part:
    attributes:
      code: {}
  SpecialHolder:
    is_a: Holder
  Load:
    attributes:
      kind:
        designates_type: true
      standard: {}
      model: {}
  SpecialLoad:
    is_a: Load
  Section:
    unique_keys:
      section_key:
        unique_key_slots:
          - sequenceNumber
    attributes:
      sequenceNumber:
        range: integer
      kp: {}
      track: {}
  Note:
    attributes:
      text: {}
"#;

    #[cfg(feature = "shacl-parser")]
    const NESTED_SHAPES: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <https://example.org/nested/> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

ex:HolderNameShape a sh:NodeShape ;
  sh:targetClass ex:Holder ;
  asset360:introspectable true ;
  sh:message "Name must be Good."@en ;
  sh:or (
    [ sh:property [ sh:path ex:name ; sh:hasValue "Good" ] ]
    [ sh:property [ sh:path ex:name ; sh:maxCount 0 ] ]
  ) .

ex:LoadComboShape a sh:NodeShape ;
  sh:targetClass ex:Load ;
  asset360:introspectable true ;
  sh:message "Model not allowed for standard."@en ;
  sh:or (
    [ sh:and (
      [ sh:property [ sh:path ex:standard ; sh:hasValue "S1" ] ]
      [ sh:property [ sh:path ex:model ; sh:hasValue "M1" ] ]
    ) ]
    [ sh:property [ sh:path ex:standard ; sh:maxCount 0 ] ]
  ) .

ex:MainElementNeedsLoadShape a sh:NodeShape ;
  sh:targetClass ex:Element ;
  asset360:introspectable true ;
  sh:message "A main element needs a load."@en ;
  sh:or (
    [ sh:not [ sh:property [ sh:path ex:elementType ; sh:hasValue "Main" ] ] ]
    [ sh:property [ sh:path ex:load ; sh:minCount 1 ] ]
  ) .

ex:ElementPartCodeShape a sh:NodeShape ;
  sh:targetClass ex:Element ;
  asset360:introspectable true ;
  sh:message "Each part code is allowed once."@en ;
  sh:property [ sh:path ( ex:parts ex:code ) ; sh:in ( "P1" "P2" ) ] ;
  sh:property [
    sh:path ex:parts ;
    sh:qualifiedValueShape [ sh:path ex:code ; sh:hasValue "P1" ] ;
    sh:qualifiedMaxCount 1
  ] ;
  sh:property [
    sh:path ex:parts ;
    sh:qualifiedValueShape [ sh:path ex:code ; sh:hasValue "P2" ] ;
    sh:qualifiedMaxCount 1
  ] .

ex:SectionKpNeedsTrackShape a sh:NodeShape ;
  sh:targetClass ex:Section ;
  asset360:introspectable true ;
  sh:message "A KP needs a track."@en ;
  sh:or (
    [ sh:property [ sh:path ex:kp ; sh:maxCount 0 ] ]
    [ sh:property [ sh:path ex:track ; sh:minCount 1 ] ]
  ) .

ex:NoteTextShape a sh:NodeShape ;
  sh:targetClass ex:Note ;
  asset360:introspectable true ;
  sh:message "A note needs text."@en ;
  sh:property [ sh:path ex:text ; sh:minCount 1 ] .
"#;

    #[cfg(feature = "shacl-parser")]
    fn nested_schema_view() -> SchemaView {
        use linkml_meta::SchemaDefinition;
        use serde_path_to_error as p2e;
        use serde_yml as yml;

        let mut sv = SchemaView::new();
        for raw in [include_str!("../tests/data/types.yaml"), NESTED_SCHEMA] {
            let schema: SchemaDefinition =
                p2e::deserialize(yml::Deserializer::from_str(raw)).unwrap();
            sv.add_schema(schema).unwrap();
        }
        sv
    }

    /// Every rule broken once: the root, a mapping entry in compact form (its
    /// key only as the dict key), an object nested one level below a mapping
    /// entry, a single-valued inlined object, a keyed list item and an
    /// unkeyed list item.
    #[cfg(feature = "shacl-parser")]
    fn nested_invalid_data() -> serde_json::Value {
        json!({
            "id": "h1",
            "name": "Bad",
            "elements": {
                "Main": {},
                "Side": { "elementType": "Side", "load": { "standard": "S1", "model": "M2" } }
            },
            "sections": [
                { "sequenceNumber": 1, "kp": "k1", "track": "t1" },
                { "sequenceNumber": 7, "kp": "k2" }
            ],
            "notes": [ { "text": "fine" }, {} ],
            "holderLoad": { "standard": "S1", "model": "M9" }
        })
    }

    #[cfg(feature = "shacl-parser")]
    fn located(violations: &[Violation]) -> Vec<(String, serde_json::Value, Option<String>)> {
        violations
            .iter()
            .map(|v| {
                (
                    v.message.clone(),
                    serde_json::to_value(&v.path).unwrap(),
                    v.element_label.clone(),
                )
            })
            .collect()
    }

    #[cfg(feature = "shacl-parser")]
    #[test]
    fn test_evaluate_nested_reports_each_object_at_its_path() {
        let sv = nested_schema_view();
        let cs = ConstraintSet::from_shacl(NESTED_SHAPES, "Holder", "en", &sv).unwrap();

        let violations = cs.evaluate(&nested_invalid_data()).unwrap();
        let label = |s: &str| Some(s.to_owned());
        assert_eq!(
            located(&violations),
            vec![
                ("Name must be Good.".into(), json!([]), None),
                (
                    "A main element needs a load.".into(),
                    json!(["elements", "Main"]),
                    label("Main")
                ),
                (
                    "Model not allowed for standard.".into(),
                    json!(["elements", "Side", "load"]),
                    label("Side")
                ),
                (
                    "Model not allowed for standard.".into(),
                    json!(["holderLoad"]),
                    None
                ),
                ("A note needs text.".into(), json!(["notes", 1]), label("2")),
                (
                    "A KP needs a track.".into(),
                    json!(["sections", 1]),
                    label("7")
                ),
            ]
        );
        // The root violation serializes exactly as before: no path, no label.
        let root_json = serde_json::to_value(&violations[0]).unwrap();
        assert!(root_json.get("path").is_none());
        assert!(root_json.get("element_label").is_none());
    }

    /// The same set with its nested shapes taken out: what `from_shacl` gave
    /// before nested classes were collected.
    #[cfg(feature = "shacl-parser")]
    fn root_only(deep: &ConstraintSet, sv: &SchemaView) -> ConstraintSet {
        ConstraintSet::from_json(&deep.root_shapes_json().unwrap(), sv, "Holder").unwrap()
    }

    /// Nested shapes add violations and nothing else: every root-only
    /// operation gives the same answer with or without them.
    #[cfg(feature = "shacl-parser")]
    #[test]
    fn test_nested_shapes_leave_root_operations_alone() {
        let sv = nested_schema_view();
        let deep = ConstraintSet::from_shacl(NESTED_SHAPES, "Holder", "en", &sv).unwrap();
        let plain = root_only(&deep, &sv);
        let data = nested_invalid_data();

        let root_violations: Vec<Violation> = deep
            .evaluate(&data)
            .unwrap()
            .into_iter()
            .filter(|v| v.path.is_empty())
            .collect();
        assert_eq!(
            located(&root_violations),
            located(&plain.evaluate(&data).unwrap())
        );
        assert_eq!(deep.affected_fields(), plain.affected_fields());
        let solved = |cs: &ConstraintSet| {
            ["name", "standard", "model", "kp", "code"]
                .map(|field| serde_json::to_value(cs.solve(&data, field)).unwrap())
        };
        assert_eq!(solved(&deep), solved(&plain));
        // `parts`/`code` is a unique-by-member rule, but on the nested Element.
        let solved_member = |cs: &ConstraintSet| {
            serde_json::to_value(cs.solve_member(&data, "parts", "code", None)).unwrap()
        };
        assert_eq!(solved_member(&deep), solved_member(&plain));
        assert_eq!(
            serde_json::to_value(deep.scope(data.as_object().unwrap(), "id")).unwrap(),
            serde_json::to_value(plain.scope(data.as_object().unwrap(), "id")).unwrap()
        );
    }

    /// The frontend rebuilds a set from JSON: that path must evaluate the
    /// nested shapes exactly the same way.
    #[cfg(feature = "shacl-parser")]
    #[test]
    fn test_nested_shapes_survive_json_round_trip() {
        let sv = nested_schema_view();
        let deep = ConstraintSet::from_shacl(NESTED_SHAPES, "Holder", "en", &sv).unwrap();
        let data = nested_invalid_data();

        let rebuilt = ConstraintSet::from_json(&deep.to_json().unwrap(), &sv, "Holder").unwrap();
        assert_eq!(
            located(&rebuilt.evaluate(&data).unwrap()),
            located(&deep.evaluate(&data).unwrap())
        );
    }

    /// Whether a shape is nested depends on the set's root, not on the shape:
    /// a Holder set's JSON rebuilt for Element runs Element's shapes at the
    /// root, Load's below it, and leaves Holder's out.
    #[cfg(feature = "shacl-parser")]
    #[test]
    fn test_rebuilt_for_a_nested_class_roots_at_that_class() {
        let sv = nested_schema_view();
        let deep = ConstraintSet::from_shacl(NESTED_SHAPES, "Holder", "en", &sv).unwrap();
        let element = ConstraintSet::from_json(&deep.to_json().unwrap(), &sv, "Element").unwrap();
        let data = json!({
            "elementType": "Main",
            "load": { "standard": "S1", "model": "M2" },
            "parts": [ { "code": "P1" }, { "code": "P1" } ]
        });
        assert_eq!(
            located(&element.evaluate(&data).unwrap()),
            vec![
                ("Each part code is allowed once.".into(), json!([]), None),
                (
                    "Model not allowed for standard.".into(),
                    json!(["load"]),
                    None
                ),
            ]
        );
        assert!(element.affected_fields().contains(&"parts".to_owned()));
        assert!(!element.affected_fields().contains(&"name".to_owned()));
    }

    /// `sh:targetClass` reaches instances of subclasses too (SHACL 2.1.3.3):
    /// a `Load` shape must run on an inlined `SpecialLoad`.
    #[cfg(feature = "shacl-parser")]
    #[test]
    fn test_evaluate_nested_applies_shape_to_subclass_instances() {
        let sv = nested_schema_view();
        let cs = ConstraintSet::from_shacl(NESTED_SHAPES, "Holder", "en", &sv).unwrap();
        let data = json!({
            "id": "h1",
            "holderLoad": { "kind": "ex:SpecialLoad", "standard": "S1", "model": "M9" }
        });
        assert_eq!(
            located(&cs.evaluate(&data).unwrap()),
            vec![(
                "Model not allowed for standard.".into(),
                json!(["holderLoad"]),
                None
            )]
        );
    }

    /// The same at the root: a set built for a subclass carries its parent's
    /// shapes as root shapes, for evaluation and solving alike.
    #[cfg(feature = "shacl-parser")]
    #[test]
    fn test_subclass_root_gets_parent_shapes() {
        let sv = nested_schema_view();
        let special = ConstraintSet::from_shacl(NESTED_SHAPES, "SpecialHolder", "en", &sv).unwrap();
        let data = json!({ "id": "h1", "name": "Bad" });
        assert_eq!(
            located(&special.evaluate(&data).unwrap()),
            vec![("Name must be Good.".into(), json!([]), None)]
        );
        assert_eq!(special.affected_fields(), vec!["name".to_owned()]);
    }

    /// An object the runtime cannot load at all leaves no nested shape
    /// checkable, and says so instead of reporting nothing.
    #[cfg(feature = "shacl-parser")]
    #[test]
    fn test_evaluate_nested_refuses_unloadable_object() {
        let sv = nested_schema_view();
        let cs = ConstraintSet::from_shacl(NESTED_SHAPES, "Holder", "en", &sv).unwrap();
        let error = cs.evaluate(&json!(["not", "an", "object"])).unwrap_err();
        assert!(
            error.contains("cannot load the object as Holder"),
            "{error}"
        );
    }
}
