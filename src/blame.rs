use std::collections::HashMap;

use linkml_runtime::blame::{
    blame_map_to_paths, format_blame_map_with, patch_with_blame as core_patch_with_blame,
};
use linkml_runtime::diff::{self, DiffOptions, PatchOptions};
use linkml_runtime::{Delta, LinkMLInstance, NodeId, PatchTrace};

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
        let deltas = diff::diff(
            &value,
            &stage.value,
            DiffOptions {
                treat_changed_identifier_as_new_object: false,
                ..Default::default()
            },
        );
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
        let (new_value, trace): (LinkMLInstance, PatchTrace) = core_patch_with_blame(
            &value,
            &stage.deltas,
            PatchOptions::default(),
            stage.meta.clone(),
            &mut blame,
        )
        .expect("patch failed");

        if !trace.failed.is_empty() {
            panic!("patch reported failed paths: {:?}", trace.failed);
        }

        value = new_value;
    }

    (value, blame)
}

/// Convert a blame map into ordered `(path_segments, metadata)` pairs.
///
/// Each path is represented as the list of path components from the root to
/// the node. The root path is an empty list.
pub fn blame_map_to_path_stage_map(
    value: &LinkMLInstance,
    blame_map: &HashMap<NodeId, Asset360ChangeMeta>,
) -> Vec<(Vec<String>, Asset360ChangeMeta)> {
    blame_map_to_paths(value, blame_map)
}

/// Produce a human-readable summary of blame metadata aligned with a YAML-style view.
pub fn format_blame_map(
    value: &LinkMLInstance,
    blame_map: &HashMap<NodeId, Asset360ChangeMeta>,
) -> String {
    const META_COL_WIDTH: usize = 72;
    format_blame_map_with(value, blame_map, |meta| {
        let mut text = format!(
            "cid={:>3} author={} ts={} src={} ics={}",
            meta.change_id, meta.author, meta.timestamp, meta.source, meta.ics_id
        );
        if text.len() > META_COL_WIDTH {
            text.truncate(META_COL_WIDTH);
        }
        format!("{text:<width$}", width = META_COL_WIDTH)
    })
}

pub use linkml_runtime::blame::get_blame_info;

#[cfg(feature = "python-bindings")]
mod py_conversions {
    use super::Asset360ChangeMeta;
    use crate::PyAsset360ChangeMeta;
    use pyo3::PyRef;
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use pyo3::types::PyDict;

    impl<'py> FromPyObject<'py> for Asset360ChangeMeta {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            if let Ok(meta_obj) = ob.extract::<PyRef<PyAsset360ChangeMeta>>() {
                return Ok(meta_obj.clone_inner());
            }

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
            let meta = PyAsset360ChangeMeta::from(self);
            let bound = meta.into_pyobject(py)?;
            Ok(bound.into_any())
        }
    }
}

#[cfg(test)]
mod tests;
