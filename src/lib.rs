#[cfg(feature = "python-bindings")]
use pyo3::prelude::*;
#[cfg(all(feature = "python-bindings", feature = "stubgen"))]
use pyo3_stub_gen::{
    define_stub_info_gatherer,
    derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods},
};

#[cfg(feature = "python-bindings")]
use std::collections::HashMap;

#[cfg(feature = "python-bindings")]
use linkml_meta::{Annotation, ClassDefinition};
#[cfg(feature = "python-bindings")]
use linkml_runtime::{NodeId, diff::Delta};
#[cfg(feature = "python-bindings")]
use linkml_runtime_python::{PyDelta, PyLinkMLInstance, PySchemaView, node_map_into_pydict};
#[cfg(feature = "python-bindings")]
use linkml_schemaview::{Converter, schemaview::SchemaView};
#[cfg(feature = "python-bindings")]
use pyo3::Bound;
#[cfg(feature = "python-bindings")]
use pyo3::types::{PyDict, PyModule};

#[cfg(feature = "python-bindings")]
use crate::blame::{Asset360ChangeMeta, ChangeStage};

pub mod blame;

#[cfg(feature = "wasm-bindings")]
pub mod wasm;

#[cfg(feature = "python-bindings")]
/// Python bindings entrypoint mirroring the dependency's module.
/// Name is different to avoid symbol clashes with the dependency.
#[cfg(feature = "python-bindings")]
#[pymodule(name = "_native2")]
pub fn runtime_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    linkml_runtime_python::runtime_module(m)?;
    m.add_class::<PyAsset360ChangeMeta>()?;
    m.add_class::<PyChangeStage>()?;
    m.add_function(wrap_pyfunction!(
        get_all_classes_by_type_designator_and_schema,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(apply_deltas_py, m)?)?;
    m.add_function(wrap_pyfunction!(blame_map_to_path_stage_map, m)?)?;
    m.add_function(wrap_pyfunction!(get_blame_info_py, m)?)?;
    Ok(())
}

#[cfg(feature = "python-bindings")]
fn is_truthy(py: Python<'_>, ann: &Annotation) -> bool {
    // Try python-level truthiness for annotation values
    let Ok(obj) = ann.extension_value.clone().into_pyobject(py) else {
        return false;
    };
    let any = obj.as_any();
    if let Ok(b) = any.extract::<bool>() {
        return b;
    }
    if let Ok(s) = any.extract::<String>() {
        let sl = s.to_ascii_lowercase();
        return matches!(sl.as_str(), "true" | "1" | "yes" | "y" | "on");
    }
    any.is_truthy().unwrap_or(false)
}

#[cfg(feature = "python-bindings")]
fn compute_classes_by_type_designator(
    sv: &SchemaView,
    only_registered: bool,
    only_default: bool,
    py: Option<Python<'_>>,
) -> HashMap<String, ClassDefinition> {
    let mut out: HashMap<String, ClassDefinition> = HashMap::new();

    for (schema_id, schema) in sv.all_schema_definitions() {
        let mut process_classes = |conv: &Converter| {
            if let Some(classes) = &schema.classes {
                for (class_name, class_def) in classes {
                    if only_registered {
                        let managed = class_def
                            .annotations
                            .as_ref()
                            .and_then(|m| m.get("data.infrabel.be/asset360/managed"));
                        let managed_truthy = managed.map(|ann| match py {
                            Some(py) => is_truthy(py, ann),
                            None => true,
                        });
                        if !managed_truthy.unwrap_or(false) {
                            out.remove(class_name);
                            continue;
                        }
                    }

                    if let Ok(Some(cv)) = sv.get_class_by_schema(schema_id, class_name)
                        && let Some(td_slot) = cv.get_type_designator_slot()
                    {
                        if only_default {
                            if let Ok(id) = cv.get_type_designator_value(td_slot, conv) {
                                out.insert(id.to_string(), class_def.clone());
                            }
                        } else if let Ok(ids) =
                            cv.get_accepted_type_designator_values(td_slot, conv)
                        {
                            for id in ids {
                                out.insert(id.to_string(), class_def.clone());
                            }
                        }
                    }
                }
            }
        };

        if let Some(conv) = sv.converter_for_schema(schema_id) {
            process_classes(conv);
        } else {
            let conv_owned = sv.converter();
            process_classes(&conv_owned);
        }
    }
    out
}

/// Return every class keyed by its resolved type designator.
///
/// * `schemaview` – existing [`SchemaView`] instance to inspect.
/// * `only_registered` – require the ``data.infrabel.be/asset360/managed``
///   annotation to be truthy.
/// * `only_default` – restrict to each class' primary type designator instead of
///   all accepted aliases.
#[cfg(feature = "python-bindings")]
fn get_all_classes_by_type_designator_and_schema_impl(
    py: Python<'_>,
    schemaview: Py<PySchemaView>,
    only_registered: bool,
    only_default: bool,
) -> PyResult<HashMap<String, ClassDefinition>> {
    let bound = schemaview.bind(py);
    let sv_ref = bound.borrow();
    Ok(compute_classes_by_type_designator(
        sv_ref.as_rust(),
        only_registered,
        only_default,
        Some(py),
    ))
}

#[cfg(all(feature = "python-bindings", feature = "stubgen"))]
/// Return every class keyed by its resolved type designator.
///
/// * `schemaview` – existing [`SchemaView`] instance to inspect.
/// * `only_registered` – require the ``data.infrabel.be/asset360/managed``
///   annotation to be truthy.
/// * `only_default` – restrict to each class' primary type designator instead of
///   all accepted aliases.
#[gen_stub_pyfunction]
#[gen_stub(
    override_return_type(
        type_repr = "dict[str, linkml_meta.ClassDefinition]",
        imports = ("linkml_meta",)
    )
)]
#[pyfunction(
    name = "get_all_classes_by_type_designator_and_schema",
    signature = (schemaview, only_registered=true, only_default=true)
)]
fn get_all_classes_by_type_designator_and_schema(
    py: Python<'_>,
    #[gen_stub(
        override_type(
            type_repr = "asset360_rust.SchemaView",
            imports = ("asset360_rust",)
        )
    )]
    schemaview: Py<PySchemaView>,
    only_registered: bool,
    only_default: bool,
) -> PyResult<HashMap<String, ClassDefinition>> {
    get_all_classes_by_type_designator_and_schema_impl(
        py,
        schemaview,
        only_registered,
        only_default,
    )
}

#[cfg(all(feature = "python-bindings", not(feature = "stubgen")))]
/// Return every class keyed by its resolved type designator.
///
/// * `schemaview` – existing [`SchemaView`] instance to inspect.
/// * `only_registered` – require the ``data.infrabel.be/asset360/managed``
///   annotation to be truthy.
/// * `only_default` – restrict to each class' primary type designator instead of
///   all accepted aliases.
#[pyfunction(
    name = "get_all_classes_by_type_designator_and_schema",
    signature = (schemaview, only_registered=true, only_default=true)
)]
fn get_all_classes_by_type_designator_and_schema(
    py: Python<'_>,
    schemaview: Py<PySchemaView>,
    only_registered: bool,
    only_default: bool,
) -> PyResult<HashMap<String, ClassDefinition>> {
    get_all_classes_by_type_designator_and_schema_impl(
        py,
        schemaview,
        only_registered,
        only_default,
    )
}

#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[cfg(feature = "stubgen")]
#[gen_stub(
    override_return_type(
        type_repr = "list[tuple[list[str], asset360_rust.Asset360ChangeMeta]]",
        imports = ("typing", "asset360_rust")
    )
)]
#[pyfunction(
    name = "blame_map_to_path_stage_map",
    signature = (value, blame_map)
)]
fn blame_map_to_path_stage_map(
    py: Python<'_>,
    #[cfg(feature = "stubgen")]
    #[gen_stub(
        override_type(
            type_repr = "asset360_rust.LinkMLInstance",
            imports = ("asset360_rust",)
        )
    )]
    value: Py<PyLinkMLInstance>,
    #[cfg(feature = "stubgen")]
    #[gen_stub(
        override_type(
            type_repr = "dict[int, dict[str, typing.Any]]",
            imports = ("typing",)
        )
    )]
    blame_map: HashMap<NodeId, Asset360ChangeMeta>,
) -> PyResult<Vec<(Vec<String>, Asset360ChangeMeta)>> {
    let bound = value.bind(py);
    let rust_value = bound.borrow().value.clone();
    Ok(crate::blame::blame_map_to_path_stage_map(
        &rust_value,
        &blame_map,
    ))
}

#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "Asset360ChangeMeta")]
#[derive(Clone)]
struct PyAsset360ChangeMeta {
    inner: Asset360ChangeMeta,
}

#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PyAsset360ChangeMeta {
    #[new]
    #[pyo3(signature = (author, timestamp, source, change_id, ics_id))]
    fn new(author: String, timestamp: String, source: String, change_id: u64, ics_id: u64) -> Self {
        Self {
            inner: Asset360ChangeMeta {
                author,
                timestamp,
                source,
                change_id,
                ics_id,
            },
        }
    }

    #[getter]
    fn author(&self) -> &str {
        &self.inner.author
    }

    #[getter]
    fn timestamp(&self) -> &str {
        &self.inner.timestamp
    }

    #[getter]
    fn source(&self) -> &str {
        &self.inner.source
    }

    #[getter]
    fn change_id(&self) -> u64 {
        self.inner.change_id
    }

    #[getter]
    fn ics_id(&self) -> u64 {
        self.inner.ics_id
    }

    fn __repr__(&self) -> String {
        format!(
            "Asset360ChangeMeta(author='{}', timestamp='{}', source='{}', change_id={}, ics_id={})",
            self.inner.author,
            self.inner.timestamp,
            self.inner.source,
            self.inner.change_id,
            self.inner.ics_id
        )
    }
}

impl From<Asset360ChangeMeta> for PyAsset360ChangeMeta {
    fn from(inner: Asset360ChangeMeta) -> Self {
        Self { inner }
    }
}

impl PyAsset360ChangeMeta {
    fn clone_inner(&self) -> Asset360ChangeMeta {
        self.inner.clone()
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "ChangeStage")]
struct PyChangeStage {
    inner: ChangeStage<Asset360ChangeMeta>,
}

#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PyChangeStage {
    #[new]
    fn new(py: Python<'_>, meta: PyAsset360ChangeMeta, deltas: Vec<Py<PyDelta>>) -> PyResult<Self> {
        let mut rust_deltas: Vec<Delta> = Vec::with_capacity(deltas.len());
        for delta in deltas {
            let bound = delta.bind(py);
            rust_deltas.push(bound.borrow().inner.clone());
        }
        Ok(Self {
            inner: ChangeStage {
                meta: meta.clone_inner(),
                deltas: rust_deltas,
            },
        })
    }

    #[getter]
    fn meta(&self) -> PyAsset360ChangeMeta {
        PyAsset360ChangeMeta::from(self.inner.meta.clone())
    }

    #[getter]
    fn deltas<'py>(&self, py: Python<'py>) -> PyResult<Vec<Py<PyDelta>>> {
        PyDelta::from_deltas(py, self.inner.deltas.clone())
    }

    fn __repr__(&self) -> String {
        format!(
            "ChangeStage(meta={}, deltas_len={})",
            PyAsset360ChangeMeta::from(self.inner.meta.clone()).__repr__(),
            self.inner.deltas.len()
        )
    }
}

impl PyChangeStage {
    fn clone_inner(&self) -> ChangeStage<Asset360ChangeMeta> {
        self.inner.clone()
    }
}

#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(
    name = "apply_deltas",
    signature = (base, stages)
)]
fn apply_deltas_py(
    py: Python<'_>,
    base: Py<PyLinkMLInstance>,
    stages: Vec<Py<PyChangeStage>>,
) -> PyResult<(Py<PyLinkMLInstance>, Py<PyDict>)> {
    let base_bound = base.bind(py);
    let base_instance = base_bound.borrow();
    let base_value = base_instance.value.clone();

    let rust_stages: Vec<_> = stages
        .into_iter()
        .map(|stage| {
            let bound = stage.bind(py);
            bound.borrow().clone_inner()
        })
        .collect();

    let (updated, blame_map) = crate::blame::apply_deltas(Some(base_value), rust_stages);
    let py_instance = Py::new(
        py,
        PyLinkMLInstance::new(updated, base_instance.sv.clone_ref(py)),
    )?;
    drop(base_instance);

    let blame_entries = blame_map
        .into_iter()
        .map(|(node_id, meta)| {
            Py::new(py, PyAsset360ChangeMeta::from(meta)).map(|py_meta| (node_id, py_meta))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let blame_dict = node_map_into_pydict(py, blame_entries)?;

    Ok((py_instance, blame_dict))
}

#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(
    name = "get_blame_info",
    signature = (value, blame_map)
)]
#[cfg(feature = "stubgen")]
#[gen_stub(
    override_return_type(
        type_repr = "typing.Optional[asset360_rust.Asset360ChangeMeta]",
        imports = ("typing", "asset360_rust")
    )
)]
fn get_blame_info_py(
    py: Python<'_>,
    value: Py<PyLinkMLInstance>,
    #[cfg(feature = "stubgen")]
    #[gen_stub(
        override_type(
            type_repr = "dict[int, asset360_rust.Asset360ChangeMeta]",
            imports = ("typing", "asset360_rust")
        )
    )]
    blame_map: HashMap<NodeId, Asset360ChangeMeta>,
) -> PyResult<Option<Py<PyAsset360ChangeMeta>>> {
    let bound = value.bind(py);
    let rust_value = bound.borrow().value.clone();

    if let Some(meta) = crate::blame::get_blame_info(&rust_value, &blame_map) {
        Ok(Some(Py::new(py, PyAsset360ChangeMeta::from(meta.clone()))?))
    } else {
        Ok(None)
    }
}

#[cfg(all(feature = "python-bindings", feature = "stubgen"))]
define_stub_info_gatherer!(stub_info);

#[cfg(all(test, feature = "python-bindings"))]
mod tests {
    use super::*;
    use linkml_meta::SchemaDefinition;

    #[test]
    fn test_compute_classes_by_type_designator_basic() {
        let yaml = r#"
id: https://example.org/test
name: test
default_prefix: ex
prefixes:
  ex:
    prefix_reference: http://example.org/
classes:
  A:
    annotations:
      data.infrabel.be/asset360/managed: true
    slots:
      - type
  B:
    slots:
      - type
slot_definitions:
  type:
    designates_type: true
    range: string
"#;
        let deser = serde_yml::Deserializer::from_str(yaml);
        let schema: SchemaDefinition = serde_path_to_error::deserialize(deser).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        let m = compute_classes_by_type_designator(&sv, false, true, None);
        assert!(m.contains_key("A"));
        assert!(m.get("A").unwrap().name == "A");
        assert!(m.contains_key("B"));
    }
}
