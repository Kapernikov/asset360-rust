use std::collections::{BTreeMap, HashMap};

use linkml_runtime::diff::PatchOptions;
use linkml_runtime::{Delta, LinkMLInstance, NodeId, PatchTrace, patch};

pub(crate) fn format_path(segments: &[String]) -> String {
    if segments.is_empty() {
        return "<root>".to_string();
    }

    let mut out = String::new();
    for segment in segments {
        if out.is_empty() {
            out.push_str(segment);
        } else {
            out.push('.');
            out.push_str(segment);
        }
    }
    out
}

/// Asset-specific metadata attached as blame.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Asset360ChangeMeta {
    pub author: String,
    pub timestamp: String,
    pub source: String,
    pub change_id: u64,
    pub ics_id: u64,
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
) -> (LinkMLInstance, HashMap<NodeId, Asset360ChangeMeta>) {
    // For now, require a base value with proper class context; creating a root value
    // from scratch requires a target class.
    let mut value = base.expect("base LinkMLInstance required (with class context)");
    let mut blame: HashMap<NodeId, Asset360ChangeMeta> = HashMap::new();

    for stage in stages.into_iter() {
        let (new_value, trace): (LinkMLInstance, PatchTrace) =
            patch(&value, &stage.deltas, PatchOptions::default()).expect("patch failed");
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

/// Convert a blame map into a dictionary keyed by dot-separated paths.
///
/// Paths use `"<root>"` for the root node. List indices are expressed using
/// dot notation (for example `items.0.title`).
pub fn blame_map_to_path_stage_map(
    value: &LinkMLInstance,
    blame_map: &HashMap<NodeId, Asset360ChangeMeta>,
) -> BTreeMap<String, Asset360ChangeMeta> {
    fn collect(
        node: &LinkMLInstance,
        blame_map: &HashMap<NodeId, Asset360ChangeMeta>,
        path: &mut Vec<String>,
        out: &mut BTreeMap<String, Asset360ChangeMeta>,
    ) {
        if let Some(meta) = blame_map.get(&node.node_id()) {
            out.insert(format_path(path), meta.clone());
        }

        match node {
            LinkMLInstance::Object { values, .. } | LinkMLInstance::Mapping { values, .. } => {
                let mut entries: Vec<_> = values.iter().collect();
                entries.sort_by(|(ka, _), (kb, _)| ka.cmp(kb));
                for (key, child) in entries {
                    path.push(key.clone());
                    collect(child, blame_map, path, out);
                    path.pop();
                }
            }
            LinkMLInstance::List { values, .. } => {
                for (idx, child) in values.iter().enumerate() {
                    path.push(idx.to_string());
                    collect(child, blame_map, path, out);
                    path.pop();
                }
            }
            LinkMLInstance::Scalar { .. } | LinkMLInstance::Null { .. } => {}
        }
    }

    let mut entries: BTreeMap<String, Asset360ChangeMeta> = BTreeMap::new();
    let mut path = Vec::new();
    collect(value, blame_map, &mut path, &mut entries);
    entries
}

#[cfg(feature = "python-bindings")]
mod py_conversions {
    use super::Asset360ChangeMeta;
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use pyo3::types::PyDict;

    impl<'py> FromPyObject<'py> for Asset360ChangeMeta {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let dict = ob.downcast::<PyDict>()?;
            let require = |key: &str| {
                dict.get_item(key)?
                    .ok_or_else(|| PyValueError::new_err(format!("missing '{key}' in metadata")))
            };
            Ok(Asset360ChangeMeta {
                author: require("author")?.extract()?,
                timestamp: require("timestamp")?.extract()?,
                source: require("source")?.extract()?,
                change_id: require("change_id")?.extract()?,
                ics_id: require("ics_id")?.extract()?,
            })
        }
    }

    impl<'py> pyo3::IntoPyObject<'py> for Asset360ChangeMeta {
        type Target = PyAny;
        type Output = Bound<'py, PyAny>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
            let dict = PyDict::new(py);
            let Asset360ChangeMeta {
                author,
                timestamp,
                source,
                change_id,
                ics_id,
                ..
            } = self;
            dict.set_item("author", author)?;
            dict.set_item("timestamp", timestamp)?;
            dict.set_item("source", source)?;
            dict.set_item("change_id", change_id)?;
            dict.set_item("ics_id", ics_id)?;
            Ok(dict.into_any())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use linkml_schemaview::schemaview::SchemaView;
    use std::collections::BTreeMap;

    #[test]
    fn test_get_blame_info_with_manual_map() {
        // Build a tiny manual blame map and a dummy value
        let mut blame = HashMap::new();
        let meta1 = Asset360ChangeMeta {
            author: "a".into(),
            timestamp: "t1".into(),
            source: "manual".into(),
            change_id: 1,
            ics_id: 101,
        };
        let meta2 = Asset360ChangeMeta {
            author: "b".into(),
            timestamp: "t2".into(),
            source: "manual".into(),
            change_id: 2,
            ics_id: 102,
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
        let (out, b) = apply_deltas(Some(base.clone()), vec![]);
        // Can't assert structural equality without schema context, but Default base should be preserved.
        // We can at least ensure the map is empty.
        assert!(b.is_empty());
        // Ensure node_id remains the same when no stages are applied
        assert_eq!(out.node_id(), base.node_id());
    }

    #[test]
    fn test_blame_map_to_path_stage_map() {
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
slots:
  name:
    range: string
  child:
    range: Child
  items:
    range: Child
    multivalued: true
  title:
    range: string
classes:
  Root:
    slots:
      - name
      - child
      - items
  Child:
    slots:
      - title
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

        let data = r#"
name: Rooty
child:
  title: Kid
items:
  - title: First
  - title: Second
"#;
        let value = linkml_runtime::load_yaml_str(data, &sv, &class, conv).unwrap();

        let mut blame = HashMap::new();
        let root_meta = Asset360ChangeMeta {
            author: "root-author".into(),
            timestamp: "t0".into(),
            source: "import".into(),
            change_id: 1,
            ics_id: 10,
        };
        blame.insert(value.node_id(), root_meta.clone());

        let child_title_node = match &value {
            LinkMLInstance::Object { values, .. } => values
                .get("child")
                .and_then(|child| match child {
                    LinkMLInstance::Object { values, .. } => values.get("title"),
                    _ => None,
                })
                .expect("child.title node present"),
            _ => panic!("expected root object"),
        };
        let child_meta = Asset360ChangeMeta {
            author: "child-author".into(),
            timestamp: "t1".into(),
            source: "import".into(),
            change_id: 2,
            ics_id: 20,
        };
        blame.insert(child_title_node.node_id(), child_meta.clone());

        let items_title_nodes = match &value {
            LinkMLInstance::Object { values, .. } => values
                .get("items")
                .and_then(|items| match items {
                    LinkMLInstance::List { values, .. } => {
                        if values.len() == 2 {
                            let first = match &values[0] {
                                LinkMLInstance::Object { values, .. } => {
                                    values.get("title").expect("items[0].title")
                                }
                                _ => panic!("expected object for items[0]"),
                            };
                            let second = match &values[1] {
                                LinkMLInstance::Object { values, .. } => {
                                    values.get("title").expect("items[1].title")
                                }
                                _ => panic!("expected object for items[1]"),
                            };
                            Some((first, second))
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
                .expect("items list present"),
            _ => panic!("expected root object"),
        };

        let (item0_title_node, item1_title_node) = items_title_nodes;

        let item0_meta = Asset360ChangeMeta {
            author: "item0-author".into(),
            timestamp: "t2".into(),
            source: "import".into(),
            change_id: 3,
            ics_id: 30,
        };
        blame.insert(item0_title_node.node_id(), item0_meta.clone());

        let item1_meta = Asset360ChangeMeta {
            author: "item1-author".into(),
            timestamp: "t3".into(),
            source: "import".into(),
            change_id: 4,
            ics_id: 40,
        };
        blame.insert(item1_title_node.node_id(), item1_meta.clone());

        let entries = blame_map_to_path_stage_map(&value, &blame);

        let mut expected = BTreeMap::new();
        expected.insert("<root>".to_string(), root_meta);
        expected.insert("child.title".to_string(), child_meta);
        expected.insert("items.0.title".to_string(), item0_meta);
        expected.insert("items.1.title".to_string(), item1_meta);

        assert_eq!(entries, expected);
    }
}
