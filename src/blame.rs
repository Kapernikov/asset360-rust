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
mod tests;
