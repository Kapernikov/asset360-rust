use std::collections::HashMap;

use linkml_runtime::{Delta, LinkMLValue, NodeId, PatchTrace, node_id_of, patch};
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
pub fn apply_changes_with_blame(
    base: Option<LinkMLValue>,
    stages: Vec<ChangeStage<Asset360ChangeMeta>>,
    sv: &SchemaView,
) -> (LinkMLValue, HashMap<NodeId, Asset360ChangeMeta>) {
    let mut value = base.unwrap_or_default();
    let mut blame: HashMap<NodeId, Asset360ChangeMeta> = HashMap::new();

    for stage in stages.into_iter() {
        let (new_value, trace): (LinkMLValue, PatchTrace) = patch(&value, &stage.deltas, sv);
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
    value: &LinkMLValue,
    blame_map: &'a HashMap<NodeId, Asset360ChangeMeta>,
) -> Option<&'a Asset360ChangeMeta> {
    let id = node_id_of(value);
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

        // Create a value and obtain its id
        let v: LinkMLValue = LinkMLValue::default();
        let id = node_id_of(&v);

        // First writer
        blame.insert(id, meta1.clone());
        // Last writer wins
        blame.insert(id, meta2.clone());

        assert_eq!(get_blame_info(&v, &blame), Some(&meta2));
    }

    #[test]
    fn test_apply_changes_with_blame_no_stages() {
        // When there are no stages, the base is returned and blame is empty.
        let base: LinkMLValue = LinkMLValue::default();
        let sv = SchemaView::new();
        let (out, b) = apply_changes_with_blame(Some(base.clone()), vec![], &sv);
        // Can't assert structural equality without schema context, but Default base should be preserved.
        // We can at least ensure the map is empty.
        assert!(b.is_empty());
        // Ensure node_id remains the same when no stages are applied
        assert_eq!(node_id_of(&out), node_id_of(&base));
    }
}
