use std::collections::{BTreeMap, HashMap};

use linkml_runtime::diff::DiffOptions;
use linkml_runtime::diff::PatchOptions;
use linkml_runtime::{Delta, LinkMLInstance, NodeId, PatchTrace, diff};
use serde_json::Value as JsonValue;

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
///
/// Each stage represents the full LinkML value emitted by a change together with
/// any metadata supplied by Asset360. The `deltas` field can be empty for raw
/// stages; [`compute_history`] will derive normalized deltas when rebuilding the
/// timeline.
#[derive(Clone)]
pub struct ChangeStage<M> {
    pub meta: M,
    pub value: LinkMLInstance,
    pub deltas: Vec<Delta>,
    pub rejected_paths: Vec<Vec<String>>,
}

/// Rebuild a normalized change history from staged LinkML values.
///
/// The first stage seeds the cumulative value. Every subsequent stage is
/// diffed against the running value, rejected paths are filtered, and the
/// remaining deltas are applied before continuing. The updated per-stage deltas
/// are returned alongside the final LinkML value. The function panics if delta
/// application reports any failed paths or when no stages are provided.
pub fn compute_history(
    stages: Vec<ChangeStage<Asset360ChangeMeta>>,
) -> (LinkMLInstance, Vec<ChangeStage<Asset360ChangeMeta>>) {
    let mut iter = stages.into_iter();
    let mut history: Vec<ChangeStage<Asset360ChangeMeta>> = Vec::new();
    let first = iter
        .next()
        .expect("at least one stage required to compute history");
    let mut value = first.value.clone();
    history.push(first);

    for stage in iter {
        let deltas = diff::diff(&value, &stage.value, DiffOptions::default());
        let real_deltas: Vec<Delta> = deltas
            .iter()
            .filter(|d| !stage.rejected_paths.contains(&d.path))
            .cloned()
            .collect();
        let new_stage = ChangeStage {
            meta: stage.meta.clone(),
            value: stage.value.clone(),
            deltas: real_deltas.clone(),
            rejected_paths: stage.rejected_paths.clone(),
        };
        history.push(new_stage);
        let (new_value, trace) =
            diff::patch(&value, &real_deltas, PatchOptions::default()).expect("patch failed");
        if !trace.failed.is_empty() {
            panic!("patch reported failed paths: {:?}", trace.failed);
        }
        value = new_value;
    }

    (value, history)
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
            diff::patch(&value, &stage.deltas, PatchOptions::default()).expect("patch failed");
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

/// Convert a blame map into ordered `(path_segments, metadata)` pairs.
///
/// Each path is represented as the list of path components from the root to
/// the node. The root path is an empty list.
pub fn blame_map_to_path_stage_map(
    value: &LinkMLInstance,
    blame_map: &HashMap<NodeId, Asset360ChangeMeta>,
) -> Vec<(Vec<String>, Asset360ChangeMeta)> {
    fn collect(
        node: &LinkMLInstance,
        blame_map: &HashMap<NodeId, Asset360ChangeMeta>,
        path: &mut Vec<String>,
        out: &mut BTreeMap<Vec<String>, Asset360ChangeMeta>,
    ) {
        if let Some(meta) = blame_map.get(&node.node_id()) {
            out.insert(path.clone(), meta.clone());
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

    let mut entries: BTreeMap<Vec<String>, Asset360ChangeMeta> = BTreeMap::new();
    let mut path = Vec::new();
    collect(value, blame_map, &mut path, &mut entries);
    entries.into_iter().collect()
}

/// Produce a human-readable summary of blame metadata aligned with a YAML-style view.
pub fn format_blame_map(
    value: &LinkMLInstance,
    blame_map: &HashMap<NodeId, Asset360ChangeMeta>,
) -> String {
    const META_COL_WIDTH: usize = 72;
    fn meta_column(meta: Option<&Asset360ChangeMeta>) -> String {
        let mut text = meta
            .map(|m| {
                format!(
                    "cid={:>3} author={} ts={} src={} ics={}",
                    m.change_id, m.author, m.timestamp, m.source, m.ics_id
                )
            })
            .unwrap_or_default();
        if text.len() > META_COL_WIDTH {
            text.truncate(META_COL_WIDTH);
        }
        format!("{text:<width$}", width = META_COL_WIDTH)
    }

    enum Marker {
        None,
        Dash,
    }

    fn scalar_to_string(v: &JsonValue) -> String {
        serde_json::to_string(v).unwrap_or_else(|_| "<unserializable>".into())
    }

    fn walk(
        node: &LinkMLInstance,
        blame_map: &HashMap<NodeId, Asset360ChangeMeta>,
        indent: usize,
        key: Option<&str>,
        marker: Marker,
        lines: &mut Vec<String>,
    ) {
        let meta = blame_map.get(&node.node_id());
        let meta_str = meta_column(meta);
        let indent_str = "  ".repeat(indent);
        match node {
            LinkMLInstance::Scalar { value, .. } => {
                let val = scalar_to_string(value);
                let yaml_line = match (&marker, key) {
                    (Marker::None, Some(k)) => format!("{indent_str}{k}: {val}"),
                    (Marker::None, None) => format!("{indent_str}{val}"),
                    (Marker::Dash, Some(k)) => format!("{indent_str}- {k}: {val}"),
                    (Marker::Dash, None) => format!("{indent_str}- {val}"),
                };
                lines.push(format!("{meta_str} | {yaml_line}"));
            }
            LinkMLInstance::Null { .. } => {
                let yaml_line = match (&marker, key) {
                    (Marker::None, Some(k)) => format!("{indent_str}{k}: null"),
                    (Marker::None, None) => format!("{indent_str}null"),
                    (Marker::Dash, Some(k)) => format!("{indent_str}- {k}: null"),
                    (Marker::Dash, None) => format!("{indent_str}- null"),
                };
                lines.push(format!("{meta_str} | {yaml_line}"));
            }
            LinkMLInstance::Object { values, class, .. } => {
                let type_hint = format!(" ({})", class.name());
                let header = match (&marker, key) {
                    (Marker::None, Some(k)) => format!("{indent_str}{k}:{type_hint}"),
                    (Marker::None, None) => format!("{indent_str}<root>{type_hint}"),
                    (Marker::Dash, Some(k)) => format!("{indent_str}- {k}:{type_hint}"),
                    (Marker::Dash, None) => format!("{indent_str}-{type_hint}"),
                };
                lines.push(format!("{meta_str} | {header}"));
                let mut entries: Vec<_> = values.iter().collect();
                entries.sort_by(|(a, _), (b, _)| a.cmp(b));
                for (child_key, child_value) in entries {
                    walk(
                        child_value,
                        blame_map,
                        indent + 1,
                        Some(child_key),
                        Marker::None,
                        lines,
                    );
                }
            }
            LinkMLInstance::Mapping { values, .. } => {
                let header = match (&marker, key) {
                    (Marker::None, Some(k)) => format!("{indent_str}{k}:",),
                    (Marker::None, None) => format!("{indent_str}<mapping>"),
                    (Marker::Dash, Some(k)) => format!("{indent_str}- {k}:",),
                    (Marker::Dash, None) => format!("{indent_str}-"),
                };
                lines.push(format!("{meta_str} | {header}"));
                let mut entries: Vec<_> = values.iter().collect();
                entries.sort_by(|(a, _), (b, _)| a.cmp(b));
                for (child_key, child_value) in entries {
                    walk(
                        child_value,
                        blame_map,
                        indent + 1,
                        Some(child_key),
                        Marker::None,
                        lines,
                    );
                }
            }
            LinkMLInstance::List { values, .. } => {
                let header = match (&marker, key) {
                    (Marker::None, Some(k)) => format!("{indent_str}{k}:",),
                    (Marker::None, None) => format!("{indent_str}<list>"),
                    (Marker::Dash, Some(k)) => format!("{indent_str}- {k}:",),
                    (Marker::Dash, None) => format!("{indent_str}-"),
                };
                lines.push(format!("{meta_str} | {header}"));
                for child in values {
                    walk(child, blame_map, indent + 1, None, Marker::Dash, lines);
                }
            }
        }
    }

    let mut lines = Vec::new();
    walk(value, blame_map, 0, None, Marker::None, &mut lines);
    if lines.is_empty() {
        "<empty blame map>".to_string()
    } else {
        lines.join("\n")
    }
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

    fn path_to_string(segments: &[String]) -> String {
        if segments.is_empty() {
            return "<root>".into();
        }
        let mut out = String::new();
        for segment in segments {
            if segment.chars().all(|c| c.is_ascii_digit()) {
                out.push_str(&format!("[{segment}]"));
            } else {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(segment);
            }
        }
        out
    }

    fn format_stage_entries(entries: &[(Vec<String>, Asset360ChangeMeta)]) -> String {
        if entries.is_empty() {
            return "<empty stage map>".to_string();
        }
        let mut lines = Vec::with_capacity(entries.len());
        for (path, meta) in entries {
            lines.push(format!(
                "{} => change_id={} author={} timestamp={} source={} ics_id={}",
                path_to_string(path),
                meta.change_id,
                meta.author,
                meta.timestamp,
                meta.source,
                meta.ics_id
            ));
        }
        lines.join("\n")
    }
    
    #[test]
    #[should_panic(expected = "at least one stage required to compute history")]
    fn test_compute_history_panics_without_stages() {
        let stages: Vec<ChangeStage<Asset360ChangeMeta>> = Vec::new();
        let _ = compute_history(stages);
    }

    fn setup_schema() -> (SchemaView, linkml_schemaview::schemaview::ClassView) {
        use linkml_meta::SchemaDefinition;
        use serde_path_to_error as p2e;
        use serde_yml as yml;

        let schema_yaml = r#"
id: https://example.org/person
name: person
default_prefix: ex
prefixes:
  ex:
    prefix_reference: http://example.org/
slots:
  name:
    range: string
  age:
    range: integer
  city:
    range: string
classes:
  Person:
    slots:
      - name
      - age
      - city
"#;
        let schema: SchemaDefinition =
            p2e::deserialize(yml::Deserializer::from_str(schema_yaml)).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        let conv = sv.converter_for_primary_schema().unwrap();
        let class = sv
            .get_class(
                &linkml_schemaview::identifier::Identifier::new("Person"),
                conv,
            )
            .unwrap()
            .unwrap();
        (sv, class)
    }

    #[test]
    fn test_compute_history_single_stage_returns_value() {
        let (sv, class) = setup_schema();
        let conv = sv.converter_for_primary_schema().unwrap();
        let stage_yaml = r#"
name: Alice
age: 30
"#;
        let stage_value = linkml_runtime::load_yaml_str(stage_yaml, &sv, &class, conv).unwrap();

        let meta = Asset360ChangeMeta {
            author: "author0".into(),
            timestamp: "t0".into(),
            source: "src0".into(),
            change_id: 0,
            ics_id: 0,
        };

        let stages = vec![ChangeStage {
            meta: meta.clone(),
            value: stage_value.clone(),
            deltas: Vec::new(),
            rejected_paths: Vec::new(),
        }];

        let (final_value, history) = compute_history(stages);

        assert_eq!(history.len(), 1);
        assert_eq!(history[0].meta.author, meta.author);
        assert!(history[0].deltas.is_empty());
        assert_eq!(final_value.to_json(), stage_value.to_json());
    }

    #[test]
    fn test_compute_history_filters_rejected_paths() {
        let (sv, class) = setup_schema();
        let conv = sv.converter_for_primary_schema().unwrap();

        let stage_one_yaml = r#"
name: Alice
age: 30
"#;
        let stage_one_value =
            linkml_runtime::load_yaml_str(stage_one_yaml, &sv, &class, conv).unwrap();

        let stage_two_yaml = r#"
name: Alicia
age: 31
city: Paris
"#;
        let stage_two_value =
            linkml_runtime::load_yaml_str(stage_two_yaml, &sv, &class, conv).unwrap();

        let meta1 = Asset360ChangeMeta {
            author: "author1".into(),
            timestamp: "t1".into(),
            source: "src1".into(),
            change_id: 1,
            ics_id: 11,
        };
        let meta2 = Asset360ChangeMeta {
            author: "author2".into(),
            timestamp: "t2".into(),
            source: "src2".into(),
            change_id: 2,
            ics_id: 22,
        };

        let stages = vec![
            ChangeStage {
                meta: meta1,
                value: stage_one_value.clone(),
                deltas: Vec::new(),
                rejected_paths: Vec::new(),
            },
            ChangeStage {
                meta: meta2,
                value: stage_two_value.clone(),
                deltas: Vec::new(),
                rejected_paths: vec![vec!["name".to_string()]],
            },
        ];

        let (final_value, history) = compute_history(stages);

        assert_eq!(history.len(), 2);
        assert!(
            history[1]
                .deltas
                .iter()
                .any(|delta| delta.path == vec!["age".to_string()])
        );
        assert!(
            history[1]
                .deltas
                .iter()
                .any(|delta| delta.path == vec!["city".to_string()])
        );
        assert!(
            !history[1]
                .deltas
                .iter()
                .any(|delta| delta.path == vec!["name".to_string()])
        );

        let final_obj = match &final_value {
            LinkMLInstance::Object { values, .. } => values,
            _ => panic!("expected object"),
        };
        let name_value = final_obj
            .get("name")
            .and_then(|v| match v {
                LinkMLInstance::Scalar { value, .. } => Some(value.clone()),
                _ => None,
            })
            .expect("name present");
        let city_value = final_obj
            .get("city")
            .and_then(|v| match v {
                LinkMLInstance::Scalar { value, .. } => Some(value.clone()),
                _ => None,
            })
            .expect("city present");
        assert_eq!(name_value.as_str(), Some("Alice"));
        assert_eq!(city_value.as_str(), Some("Paris"));
    }

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
    fn test_apply_deltas_with_asset360_stages_panics() {
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
        let conv = sv.converter_for_primary_schema().unwrap();
        let signal_class = sv
            .get_class(
                &linkml_schemaview::identifier::Identifier::new(
                    "https://data.infrabel.be/asset360/Signal",
                ),
                conv,
            )
            .unwrap()
            .unwrap();

        let stages_json = include_str!("../tests/data/asset360_stages.json");
        let mut parsed: Vec<serde_json::Value> = serde_json::from_str(stages_json).unwrap();

        let mut stages: Vec<ChangeStage<Asset360ChangeMeta>> = Vec::new();
        for entry in parsed.drain(..) {
            let meta: Asset360ChangeMeta =
                serde_json::from_value(entry.get("meta").cloned().expect("meta present")).unwrap();
            let value_json = entry.get("value").cloned().expect("value present");
            let value_str = serde_json::to_string(&value_json).unwrap();
            let value = linkml_runtime::load_json_str(&value_str, &sv, &signal_class, conv)
                .expect("stage value conversion");
            let deltas: Vec<Delta> =
                serde_json::from_value(entry.get("deltas").cloned().expect("deltas present"))
                    .unwrap();

            stages.push(ChangeStage {
                meta,
                value,
                deltas,
                rejected_paths: Vec::new(),
            });
        }

        assert!(stages.len() >= 2);
        let base_stage = stages.remove(0);
        let _ = apply_deltas(Some(base_stage.value), stages);
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

        let expected = vec![
            (vec![], root_meta),
            (vec!["child".to_string(), "title".to_string()], child_meta),
            (
                vec!["items".to_string(), "0".to_string(), "title".to_string()],
                item0_meta,
            ),
            (
                vec!["items".to_string(), "1".to_string(), "title".to_string()],
                item1_meta,
            ),
        ];

        assert_eq!(entries, expected);
    }

    #[test]
    fn test_apply_multiple_stages_preserves_blame_history() {
        use linkml_meta::SchemaDefinition;
        use serde_path_to_error as p2e;
        use serde_yml as yml;

        const SCHEMA: &str = include_str!("../tests/data/blame_series/schema.yaml");
        const GENERATIONS: [&str; 4] = [
            include_str!("../tests/data/blame_series/project_v0.yaml"),
            include_str!("../tests/data/blame_series/project_v1.yaml"),
            include_str!("../tests/data/blame_series/project_v2.yaml"),
            include_str!("../tests/data/blame_series/project_v3.yaml"),
        ];

        let schema_deser = yml::Deserializer::from_str(SCHEMA);
        let schema: SchemaDefinition = p2e::deserialize(schema_deser).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        let conv = sv.converter_for_primary_schema().unwrap();
        let project_class = sv
            .get_class(
                &linkml_schemaview::identifier::Identifier::new("Project"),
                conv,
            )
            .unwrap()
            .unwrap();

        let generations: Vec<LinkMLInstance> = GENERATIONS
            .iter()
            .map(|data| linkml_runtime::load_yaml_str(data, &sv, &project_class, conv).unwrap())
            .collect();

        let base = generations
            .first()
            .cloned()
            .expect("base generation present");
        let expected_final = generations
            .last()
            .cloned()
            .expect("final generation present");

        let stage_metadata = vec![
            Asset360ChangeMeta {
                author: "planner.one".into(),
                timestamp: "2024-01-01T09:00:00Z".into(),
                source: "ingest".into(),
                change_id: 1,
                ics_id: 1001,
            },
            Asset360ChangeMeta {
                author: "planner.two".into(),
                timestamp: "2024-01-03T14:00:00Z".into(),
                source: "ingest".into(),
                change_id: 2,
                ics_id: 1002,
            },
            Asset360ChangeMeta {
                author: "planner.three".into(),
                timestamp: "2024-01-05T08:30:00Z".into(),
                source: "ingest".into(),
                change_id: 3,
                ics_id: 1003,
            },
        ];

        let stages: Vec<ChangeStage<Asset360ChangeMeta>> = stage_metadata
            .into_iter()
            .enumerate()
            .map(|(idx, meta)| ChangeStage {
                meta,
                deltas: linkml_runtime::diff::diff(
                    &generations[idx],
                    &generations[idx + 1],
                    DiffOptions {
                        treat_missing_as_null: true,
                        ..DiffOptions::default()
                    },
                ),
            })
            .collect();

        let (updated, blame_map) = apply_deltas(Some(base), stages);

        assert_eq!(updated.to_json(), expected_final.to_json());

        let blame_dump = format_blame_map(&updated, &blame_map);
        println!("Multi-stage blame map:\n{}", blame_dump);
        let entries = blame_map_to_path_stage_map(&updated, &blame_map);
        let stage_dump = format_stage_entries(&entries);
        println!("Stage map entries:\n{}", stage_dump);
        let mut path_meta: std::collections::BTreeMap<Vec<String>, Asset360ChangeMeta> =
            entries.into_iter().collect();

        let root_meta = path_meta
            .remove(&Vec::new())
            .unwrap_or_else(|| panic!("root blame present\n{blame_dump}"));
        assert_eq!(root_meta.change_id, 2);

        // Ensure we recorded blame for each stage by checking representative paths
        let role_meta = path_meta
            .remove(&vec!["owner".to_string(), "role".to_string()])
            .unwrap_or_else(|| panic!("owner.role blame present\n{blame_dump}"));
        assert_eq!(role_meta.change_id, 2);

        let task_title_meta = path_meta
            .remove(&vec![
                "tasks".to_string(),
                "1".to_string(),
                "title".to_string(),
            ])
            .unwrap_or_else(|| panic!("tasks[1].title blame present\n{blame_dump}"));
        assert_eq!(task_title_meta.change_id, 2);

        let description_meta = path_meta
            .remove(&vec!["description".to_string()])
            .unwrap_or_else(|| panic!("description blame present\n{blame_dump}"));
        assert_eq!(description_meta.change_id, 3);

        // Also ensure a node touched twice ends up with the latest stage metadata
        let task_status_meta = path_meta
            .remove(&vec![
                "tasks".to_string(),
                "0".to_string(),
                "status".to_string(),
            ])
            .unwrap_or_else(|| panic!("tasks[0].status blame present\n{blame_dump}"));
        assert_eq!(task_status_meta.change_id, 3);

        // Sanity check: we should see metadata from all stages in the blame map
        let mut seen_changes: std::collections::BTreeSet<u64> =
            path_meta.values().map(|meta| meta.change_id).collect();
        seen_changes.insert(root_meta.change_id);
        seen_changes.insert(role_meta.change_id);
        seen_changes.insert(task_title_meta.change_id);
        seen_changes.insert(description_meta.change_id);
        seen_changes.insert(task_status_meta.change_id);
        assert!(seen_changes.contains(&1));
        assert!(seen_changes.contains(&2));
        assert!(seen_changes.contains(&3));
    }
}
