use std::collections::HashMap;

use linkml_runtime::diff::PatchOptions;
use linkml_runtime::{Delta, LinkMLInstance, NodeId, PatchTrace, patch};
use linkml_schemaview::schemaview::SchemaView;

/// Asset-specific metadata attached as blame.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Asset360ChangeMeta {
    pub author: String,
    pub timestamp: String,
    // Extend with more fields as needed
}

/// One stage of changes with associated metadata.
#[derive(Clone, Debug)]
pub struct ChangeStage<M> {
    pub meta: M,
    pub deltas: Vec<Delta>,
}

/// Apply a sequence of change stages, collecting blame (last-writer-wins) per NodeId.
pub fn apply_deltas(
    base: Option<LinkMLInstance>,
    stages: Vec<ChangeStage<Asset360ChangeMeta>>,
    sv: &SchemaView,
) -> (LinkMLInstance, HashMap<NodeId, Asset360ChangeMeta>) {
    // For now, require a base value with proper class context; creating a root value
    // from scratch requires a target class.
    let mut value = base.expect("base LinkMLInstance required (with class context)");
    let mut blame: HashMap<NodeId, Asset360ChangeMeta> = HashMap::new();

    for stage in stages.into_iter() {
        let (new_value, trace): (LinkMLInstance, PatchTrace) =
            patch(&value, &stage.deltas, sv, PatchOptions::default()).expect("patch failed");
        // Last-writer-wins on added and updated nodes
        for id in trace.added.iter().chain(trace.updated.iter()) {
            blame.insert(*id, stage.meta.clone());
        }
        value = new_value;
    }

    (value, blame)
}

/// Retrieve blame info for a given value from a blame map.
pub fn get_blame_info<'a>(
    value: &LinkMLInstance,
    blame_map: &'a HashMap<NodeId, Asset360ChangeMeta>,
) -> Option<&'a Asset360ChangeMeta> {
    let id = value.node_id();
    blame_map.get(&id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_blame_info_with_manual_map() {
        // Build a tiny manual blame map and a dummy value
        let mut blame = HashMap::new();
        let meta1 = Asset360ChangeMeta {
            author: "a".into(),
            timestamp: "t1".into(),
        };
        let meta2 = Asset360ChangeMeta {
            author: "b".into(),
            timestamp: "t2".into(),
        };

        // Create a minimal value by parsing an empty object for a dummy class
        use linkml_meta::SchemaDefinition;
        use serde_path_to_error as p2e;
        use serde_yml as yml;

        let schema_yaml = r#"
id: https://example.org/test
name: test
default_prefix: ex
prefixes:
  ex:
    prefix_reference: http://example.org/
classes:
  Root: {}
"#;
        let deser = yml::Deserializer::from_str(schema_yaml);
        let schema: SchemaDefinition = p2e::deserialize(deser).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        let conv = sv.converter_for_primary_schema().unwrap();
        let class = sv
            .get_class(
                &linkml_schemaview::identifier::Identifier::new("Root"),
                conv,
            )
            .unwrap()
            .unwrap();
        let v = linkml_runtime::load_yaml_str("{}", &sv, &class, conv).unwrap();
        let id = v.node_id();

        // First writer
        blame.insert(id, meta1.clone());
        // Last writer wins
        blame.insert(id, meta2.clone());

        assert_eq!(get_blame_info(&v, &blame), Some(&meta2));
    }

    #[test]
    fn test_apply_deltas_no_stages() {
        // When there are no stages, the base is returned and blame is empty.
        use linkml_meta::SchemaDefinition;
        use serde_path_to_error as p2e;
        use serde_yml as yml;

        let schema_yaml = r#"
id: https://example.org/test
name: test
default_prefix: ex
prefixes:
  ex:
    prefix_reference: http://example.org/
classes:
  Root: {}
"#;
        let deser = yml::Deserializer::from_str(schema_yaml);
        let schema: SchemaDefinition = p2e::deserialize(deser).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        let conv = sv.converter_for_primary_schema().unwrap();
        let class = sv
            .get_class(
                &linkml_schemaview::identifier::Identifier::new("Root"),
                conv,
            )
            .unwrap()
            .unwrap();
        let base = linkml_runtime::load_yaml_str("{}", &sv, &class, conv).unwrap();
        let (out, b) = apply_deltas(Some(base.clone()), vec![], &sv);
        // Can't assert structural equality without schema context, but Default base should be preserved.
        // We can at least ensure the map is empty.
        assert!(b.is_empty());
        // Ensure node_id remains the same when no stages are applied
        assert_eq!(out.node_id(), base.node_id());
    }
}
