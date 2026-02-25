use linkml_runtime::LinkMLInstance;
use linkml_schemaview::schemaview::{ClassView, SlotInlineMode, SlotView};
use serde::Serialize;

const ASSET360ID_ANNOTATION: &str = "data.infrabel.be/linkml/asset360id";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum RefKind {
    Foreign,
    Primary,
}

impl RefKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            RefKind::Foreign => "foreign",
            RefKind::Primary => "primary",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ForeignReference {
    pub uri: String,
    pub object_type: String,
    pub object_type_uri: String,
    pub slot_name: String,
    pub slot_path: Vec<String>,
    pub kind: RefKind,
}

/// Check if a `serde_value::Value` (wrapped in an `Anything`) is truthy.
///
/// Mirrors the Python `_truthy` helper: booleans, strings like "true"/"1"/"yes"/"y"/"on",
/// and non-zero numbers are truthy.
fn is_annotation_truthy(ann: &linkml_meta::Annotation) -> bool {
    // Serialize the Anything value through serde_json to inspect it
    let json_val = match serde_json::to_value(&ann.extension_value) {
        Ok(v) => v,
        Err(_) => return false,
    };
    match json_val {
        serde_json::Value::Bool(b) => b,
        serde_json::Value::String(s) => {
            matches!(
                s.to_ascii_lowercase().as_str(),
                "true" | "1" | "yes" | "y" | "on"
            )
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i != 0
            } else if let Some(f) = n.as_f64() {
                f != 0.0
            } else {
                false
            }
        }
        serde_json::Value::Null => false,
        _ => true,
    }
}

/// Check if a slot is a reference slot (i.e. its range is a class with an identifier,
/// and it's not inlined).
fn is_reference_slot(slot: &SlotView) -> bool {
    slot.determine_slot_inline_mode() == SlotInlineMode::Reference
}

/// Check if a slot is an asset360 ID slot (marked with the asset360id annotation).
fn is_asset360id_slot(slot: &SlotView, class: Option<&ClassView>) -> bool {
    let def = slot.definition();

    // Check direct slot annotations
    if let Some(annotations) = &def.annotations
        && let Some(ann) = annotations.get(ASSET360ID_ANNOTATION)
    {
        return is_annotation_truthy(ann);
    }

    // Check class slot_usage annotations
    if let Some(cv) = class
        && let Some(slot_usage) = &cv.def().slot_usage
        && let Some(usage_def) = slot_usage.get(&def.name)
        && let Some(annotations) = &usage_def.annotations
        && let Some(ann) = annotations.get(ASSET360ID_ANNOTATION)
    {
        return is_annotation_truthy(ann);
    }

    false
}

/// The filter function applied to each child node during the tree walk.
/// Returns true if this node matches the criteria (is a reference slot,
/// or optionally an asset360 ID slot).
fn matches_filter(instance: &LinkMLInstance, also_include_id_slots: bool) -> bool {
    if let Some(slot) = instance.slot() {
        let is_ref = is_reference_slot(slot);
        let is_id = if also_include_id_slots {
            is_asset360id_slot(slot, instance.class())
        } else {
            false
        };
        is_ref || is_id
    } else {
        false
    }
}

/// Recursively walk a `LinkMLInstance` tree, collecting `(path, instance)` pairs
/// for all nodes that match the filter.
///
/// This mirrors the Python `get_rust_slot_paths_satisfying` function:
/// - For Object/Mapping: iterate keys, check filter on child. If match, collect; else recurse.
/// - For List (no keys): iterate indexed values, recurse into each.
/// - Scalar/Null: leaf nodes, nothing to iterate.
///
/// Uses a mutable path stack to avoid allocating a new Vec on every recursion level.
fn collect_matching_paths<'a>(
    instance: &'a LinkMLInstance,
    also_include_id_slots: bool,
    path: &mut Vec<String>,
    result: &mut Vec<(Vec<String>, &'a LinkMLInstance)>,
) {
    match instance {
        LinkMLInstance::Object { values, .. } | LinkMLInstance::Mapping { values, .. } => {
            for (key, child) in values {
                path.push(key.clone());
                if matches_filter(child, also_include_id_slots) {
                    result.push((path.clone(), child));
                } else {
                    collect_matching_paths(child, also_include_id_slots, path, result);
                }
                path.pop();
            }
        }
        LinkMLInstance::List { values, .. } => {
            for (ix, child) in values.iter().enumerate() {
                path.push(ix.to_string());
                collect_matching_paths(child, also_include_id_slots, path, result);
                path.pop();
            }
        }
        LinkMLInstance::Scalar { .. } | LinkMLInstance::Null { .. } => {
            // Leaf nodes — nothing to iterate
        }
    }
}

/// Extract a string URI from a `LinkMLInstance` scalar value.
/// Returns None for Null instances. Avoids building a full JSON tree.
fn instance_uri_string(instance: &LinkMLInstance) -> Option<String> {
    match instance {
        LinkMLInstance::Scalar { value, .. } => match value {
            serde_json::Value::Null => None,
            serde_json::Value::String(s) => Some(s.clone()),
            other => Some(other.to_string()),
        },
        LinkMLInstance::Null { .. } => None,
        // For non-scalar instances, fall back to JSON serialization
        other => {
            let json = other.to_json();
            match json {
                serde_json::Value::Null => None,
                serde_json::Value::String(s) => Some(s),
                v => Some(v.to_string()),
            }
        }
    }
}

/// Check if a `LinkMLInstance` represents a null value, without building JSON.
fn is_instance_null(instance: &LinkMLInstance) -> bool {
    matches!(instance, LinkMLInstance::Null { .. })
}

/// Transform collected `(path, instance)` pairs into `ForeignReference` structs.
///
/// Mirrors the Python `_transform_and_filter_ref` function.
fn transform_refs(ref_slot_paths: Vec<(Vec<String>, &LinkMLInstance)>) -> Vec<ForeignReference> {
    let mut result = Vec::new();

    for (path, instance) in ref_slot_paths {
        let slot = instance.slot();
        let slot_def = slot.map(|s| s.definition());

        let range = slot_def.and_then(|d| d.range.as_deref());
        let range_class = slot.and_then(|s| s.get_range_class());

        if let (Some(classview), Some(def), Some(range)) = (&range_class, slot_def, range) {
            // Check if the value is null — skip if so
            if is_instance_null(instance) {
                continue;
            }

            let object_type_uri = classview.canonical_uri().to_string();
            let slot_name = def.name.clone();

            // Check if the slot is multivalued by checking if the instance is a List
            let is_list = matches!(instance, LinkMLInstance::List { .. });
            let is_multivalued = is_list || def.multivalued.unwrap_or(false);

            if is_multivalued {
                if let LinkMLInstance::List { values, .. } = instance {
                    for (ix, child) in values.iter().enumerate() {
                        if let Some(uri) = instance_uri_string(child) {
                            let mut child_path = path.clone();
                            child_path.push(ix.to_string());
                            result.push(ForeignReference {
                                uri,
                                object_type: range.to_string(),
                                object_type_uri: object_type_uri.clone(),
                                slot_name: slot_name.clone(),
                                slot_path: child_path,
                                kind: RefKind::Foreign,
                            });
                        }
                    }
                }
            } else if let Some(uri) = instance_uri_string(instance) {
                result.push(ForeignReference {
                    uri,
                    object_type: range.to_string(),
                    object_type_uri,
                    slot_name,
                    slot_path: path,
                    kind: RefKind::Foreign,
                });
            }
        } else if let Some(cv) = instance.class() {
            // Fallback path for ID slots (Primary kind)
            let object_type_uri = cv.canonical_uri().to_string();
            let slot_name = slot_def.map(|d| d.name.clone()).unwrap_or_default();
            let object_type = range.unwrap_or("").to_string();

            if let Some(uri) = instance_uri_string(instance) {
                result.push(ForeignReference {
                    uri,
                    object_type,
                    object_type_uri,
                    slot_name,
                    slot_path: path,
                    kind: RefKind::Primary,
                });
            }
        }
    }

    result
}

/// Get all foreign (and optionally primary/ID) references from a `LinkMLInstance` tree.
///
/// This is the main entry point, equivalent to the Python `get_foreign_references` function.
/// It walks the instance tree, finds all reference slots (and optionally asset360 ID slots),
/// and returns structured `ForeignReference` entries.
pub fn get_foreign_references(
    instance: &LinkMLInstance,
    also_include_id_slots: bool,
) -> Vec<ForeignReference> {
    let mut matched = Vec::new();
    let mut path = Vec::new();
    collect_matching_paths(instance, also_include_id_slots, &mut path, &mut matched);
    transform_refs(matched)
}

#[cfg(test)]
mod tests {
    use super::*;
    use linkml_meta::SchemaDefinition;
    use linkml_schemaview::identifier::Identifier;
    use linkml_schemaview::schemaview::SchemaView;

    fn load_test_schema() -> SchemaView {
        let schema_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("data")
            .join("asset360.yaml");
        let yaml = std::fs::read_to_string(&schema_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", schema_path.display()));
        let deser = serde_yml::Deserializer::from_str(&yaml);
        let schema: SchemaDefinition = serde_path_to_error::deserialize(deser).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        sv
    }

    #[test]
    fn test_foreign_references_basic() {
        let sv = load_test_schema();
        let conv = sv.converter_for_primary_schema().unwrap();

        // Signal has signallingPost (range: SignallingPost, inlined: false) = Reference slot
        let class = sv
            .get_class(&Identifier::new("Signal"), &conv)
            .unwrap()
            .unwrap();

        let data = r#"
id: "urn:signal:1"
signallingPost: "urn:post:42"
signalType: "HOME"
"#;
        let value = linkml_runtime::load_yaml_str(data, &sv, &class, &conv)
            .unwrap()
            .into_instance_tolerate_errors()
            .unwrap();

        let refs = get_foreign_references(&value, false);
        assert!(
            !refs.is_empty(),
            "expected at least one foreign reference for Signal with signallingPost"
        );

        let post_ref = refs.iter().find(|r| r.slot_name == "signallingPost");
        assert!(
            post_ref.is_some(),
            "expected a foreign reference for signallingPost, got: {:?}",
            refs
        );
        let post_ref = post_ref.unwrap();
        assert_eq!(post_ref.uri, "urn:post:42");
        assert_eq!(post_ref.kind, RefKind::Foreign);
        assert_eq!(post_ref.slot_path, vec!["signallingPost"]);
    }

    #[test]
    fn test_foreign_references_with_id_slots() {
        let sv = load_test_schema();
        let conv = sv.converter_for_primary_schema().unwrap();

        let class = sv
            .get_class(&Identifier::new("Signal"), &conv)
            .unwrap()
            .unwrap();

        let data = r#"
id: "urn:signal:1"
signallingPost: "urn:post:42"
signalType: "HOME"
"#;
        let value = linkml_runtime::load_yaml_str(data, &sv, &class, &conv)
            .unwrap()
            .into_instance_tolerate_errors()
            .unwrap();

        let refs_without_id = get_foreign_references(&value, false);
        let refs_with_id = get_foreign_references(&value, true);

        // With id slots should have at least one more entry (the id slot itself)
        assert!(
            refs_with_id.len() > refs_without_id.len(),
            "expected more refs when including id slots: without={}, with={}",
            refs_without_id.len(),
            refs_with_id.len()
        );

        let id_ref = refs_with_id.iter().find(|r| r.kind == RefKind::Primary);
        assert!(
            id_ref.is_some(),
            "expected a primary (ID) reference, got: {:?}",
            refs_with_id
        );
    }

    #[test]
    fn test_foreign_references_null_values_skipped() {
        let sv = load_test_schema();
        let conv = sv.converter_for_primary_schema().unwrap();

        let class = sv
            .get_class(&Identifier::new("Signal"), &conv)
            .unwrap()
            .unwrap();

        // signallingPost not set → should be null/absent
        let data = r#"
id: "urn:signal:1"
signalType: "HOME"
"#;
        let value = linkml_runtime::load_yaml_str(data, &sv, &class, &conv)
            .unwrap()
            .into_instance_tolerate_errors()
            .unwrap();

        let refs = get_foreign_references(&value, false);
        let post_ref = refs.iter().find(|r| r.slot_name == "signallingPost");
        assert!(
            post_ref.is_none(),
            "null signallingPost should be excluded, got: {:?}",
            refs
        );
    }
}
