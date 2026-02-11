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
use linkml_meta::Annotation;
#[cfg(feature = "python-bindings")]
use linkml_runtime::{LinkMLInstance, NodeId, diff::Delta};
#[cfg(feature = "python-bindings")]
use linkml_runtime_python::{
    PyClassView, PyDelta, PyLinkMLInstance, PySchemaView, node_map_into_pydict,
};
#[cfg(feature = "python-bindings")]
use linkml_schemaview::classview::ClassView;
#[cfg(feature = "python-bindings")]
use linkml_schemaview::{Converter, identifier::Identifier, schemaview::SchemaView};
#[cfg(feature = "python-bindings")]
use pyo3::Bound;
#[cfg(feature = "python-bindings")]
use pyo3::types::{PyDict, PyModule};

#[cfg(feature = "python-bindings")]
use crate::blame::{Asset360ChangeMeta, ChangeStage};

pub mod blame;
pub mod predicate;
pub mod shacl_ast;
pub mod forward_eval;
pub mod backward_solver;
pub mod scope_predicate;

#[cfg(feature = "shacl-parser")]
pub mod shacl_parser;

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
    {
        let py = m.py();
        let meta_type = py.get_type::<PyAsset360ChangeMeta>();
        meta_type.setattr("__asset360_original_name__", "Asset360ChangeMeta")?;
        let hint = "Asset360ChangeMeta (use Asset360ChangeMeta.to_dict() before JSON encoding)";
        meta_type.setattr("__name__", hint)?;
        meta_type.setattr("__qualname__", hint)?;
    }
    m.add_function(wrap_pyfunction!(
        get_all_classes_by_type_designator_and_schema,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(apply_deltas_py, m)?)?;
    m.add_function(wrap_pyfunction!(compute_history_py, m)?)?;
    m.add_function(wrap_pyfunction!(blame_map_to_path_stage_map, m)?)?;
    m.add_function(wrap_pyfunction!(format_blame_map_py, m)?)?;
    m.add_function(wrap_pyfunction!(get_blame_info_py, m)?)?;
    m.add_function(wrap_pyfunction!(evaluate_forward_py, m)?)?;
    m.add_function(wrap_pyfunction!(solve_backward_py, m)?)?;
    m.add_function(wrap_pyfunction!(derive_scope_predicate_py, m)?)?;
    #[cfg(feature = "shacl-parser")]
    m.add_function(wrap_pyfunction!(parse_shacl_py, m)?)?;
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

/// Default annotation key used to mark a class as "managed" by asset360.
#[cfg(feature = "python-bindings")]
pub const DEFAULT_MANAGED_ANNOTATION: &str = "data.infrabel.be/asset360/managed";

#[cfg(feature = "python-bindings")]
fn compute_classes_by_type_designator(
    sv: &SchemaView,
    only_registered: bool,
    only_default: bool,
    py: Option<Python<'_>>,
    managed_annotation: &str,
) -> HashMap<String, ClassView> {
    let mut out: HashMap<String, ClassView> = HashMap::new();

    sv.with_schema_definitions(|schemas| {
        for (schema_id, schema) in schemas {
            let mut process_classes = |conv: &Converter| {
                if let Some(classes) = &schema.classes {
                    for (class_name, class_def) in classes {
                        if only_registered {
                            let managed = class_def
                                .annotations
                                .as_ref()
                                .and_then(|m| m.get(managed_annotation));
                            let managed_truthy = managed.map(|ann| match py {
                                Some(py) => is_truthy(py, ann),
                                None => true,
                            });
                            if !managed_truthy.unwrap_or(false) {
                                out.remove(class_name);
                                continue;
                            }
                        }

                        if let Ok(Some(cv)) = sv.get_class_by_schema(schema_id.as_str(), class_name)
                            && let Some(td_slot) = cv.get_type_designator_slot()
                        {
                            if only_default {
                                if let Ok(id) = cv.get_type_designator_value(td_slot, conv) {
                                    out.insert(id.to_string(), cv.clone());
                                }
                            } else if let Ok(ids) =
                                cv.get_accepted_type_designator_values(td_slot, conv)
                            {
                                for id in ids {
                                    out.insert(id.to_string(), cv.clone());
                                }
                            }
                        }
                    }
                }
            };

            if let Some(conv) = sv.converter_for_schema(schema_id) {
                process_classes(&conv);
            } else {
                let conv_owned = sv.converter();
                process_classes(&conv_owned);
            }
        }
    });
    out
}

/// Return every class keyed by its resolved type designator.
///
/// * `schemaview` – existing [`SchemaView`] instance to inspect.
/// * `only_registered` – require the annotation named by `managed_annotation`
///   to be truthy.
/// * `only_default` – restrict to each class' primary type designator instead of
///   all accepted aliases.
/// * `managed_annotation` – annotation key that marks a class as managed
///   (default: `data.infrabel.be/asset360/managed`).
#[cfg(feature = "python-bindings")]
fn get_all_classes_by_type_designator_and_schema_impl(
    py: Python<'_>,
    schemaview: Py<PySchemaView>,
    only_registered: bool,
    only_default: bool,
    managed_annotation: &str,
) -> PyResult<HashMap<String, Py<PyClassView>>> {
    let bound = schemaview.bind(py);
    let sv_ref = bound.borrow();
    let raw = compute_classes_by_type_designator(
        sv_ref.as_rust(),
        only_registered,
        only_default,
        Some(py),
        managed_annotation,
    );
    raw.into_iter()
        .map(|(designator, view)| {
            Py::new(py, PyClassView::from(view)).map(|py_view| (designator, py_view))
        })
        .collect()
}

#[cfg(all(feature = "python-bindings", feature = "stubgen"))]
/// Return every class keyed by its resolved type designator.
///
/// * `schemaview` – existing [`SchemaView`] instance to inspect.
/// * `only_registered` – require the annotation named by `managed_annotation`
///   to be truthy.
/// * `only_default` – restrict to each class' primary type designator instead of
///   all accepted aliases.
/// * `managed_annotation` – annotation key that marks a class as managed
///   (default: `"data.infrabel.be/asset360/managed"`).
#[gen_stub_pyfunction]
#[gen_stub(
    override_return_type(
        type_repr = "dict[str, asset360_rust.ClassView]",
        imports = ("asset360_rust",)
    )
)]
#[pyfunction(
    name = "get_all_classes_by_type_designator_and_schema",
    signature = (schemaview, only_registered=true, only_default=true, managed_annotation="data.infrabel.be/asset360/managed")
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
    managed_annotation: &str,
) -> PyResult<HashMap<String, Py<PyClassView>>> {
    get_all_classes_by_type_designator_and_schema_impl(
        py,
        schemaview,
        only_registered,
        only_default,
        managed_annotation,
    )
}

#[cfg(all(feature = "python-bindings", not(feature = "stubgen")))]
/// Return every class keyed by its resolved type designator.
///
/// * `schemaview` – existing [`SchemaView`] instance to inspect.
/// * `only_registered` – require the annotation named by `managed_annotation`
///   to be truthy.
/// * `only_default` – restrict to each class' primary type designator instead of
///   all accepted aliases.
/// * `managed_annotation` – annotation key that marks a class as managed
///   (default: `"data.infrabel.be/asset360/managed"`).
#[pyfunction(
    name = "get_all_classes_by_type_designator_and_schema",
    signature = (schemaview, only_registered=true, only_default=true, managed_annotation="data.infrabel.be/asset360/managed")
)]
fn get_all_classes_by_type_designator_and_schema(
    py: Python<'_>,
    schemaview: Py<PySchemaView>,
    only_registered: bool,
    only_default: bool,
    managed_annotation: &str,
) -> PyResult<HashMap<String, Py<PyClassView>>> {
    get_all_classes_by_type_designator_and_schema_impl(
        py,
        schemaview,
        only_registered,
        only_default,
        managed_annotation,
    )
}

#[cfg(feature = "python-bindings")]
fn blame_map_to_path_stage_map_impl(
    py: Python<'_>,
    value: Py<PyLinkMLInstance>,
    blame_map: HashMap<NodeId, Asset360ChangeMeta>,
) -> PyResult<Vec<(Vec<String>, Asset360ChangeMeta)>> {
    let bound = value.bind(py);
    let rust_value = bound.borrow().value.clone();
    Ok(crate::blame::blame_map_to_path_stage_map(
        &rust_value,
        &blame_map,
    ))
}

#[cfg(feature = "python-bindings")]
fn format_blame_map_impl(
    py: Python<'_>,
    value: Py<PyLinkMLInstance>,
    blame_map: HashMap<NodeId, Asset360ChangeMeta>,
) -> PyResult<String> {
    let bound = value.bind(py);
    let rust_value = bound.borrow().value.clone();
    Ok(crate::blame::format_blame_map(&rust_value, &blame_map))
}

#[cfg(all(feature = "python-bindings", feature = "stubgen"))]
#[gen_stub_pyfunction]
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
    #[gen_stub(
        override_type(
            type_repr = "asset360_rust.LinkMLInstance",
            imports = ("asset360_rust",)
        )
    )]
    value: Py<PyLinkMLInstance>,
    #[gen_stub(
        override_type(
            type_repr = "dict[int, asset360_rust.Asset360ChangeMeta]",
            imports = ("asset360_rust",)
        )
    )]
    blame_map: HashMap<NodeId, Asset360ChangeMeta>,
) -> PyResult<Vec<(Vec<String>, Asset360ChangeMeta)>> {
    blame_map_to_path_stage_map_impl(py, value, blame_map)
}

#[cfg(all(feature = "python-bindings", not(feature = "stubgen")))]
#[pyfunction(
    name = "blame_map_to_path_stage_map",
    signature = (value, blame_map)
)]
fn blame_map_to_path_stage_map(
    py: Python<'_>,
    value: Py<PyLinkMLInstance>,
    blame_map: HashMap<NodeId, Asset360ChangeMeta>,
) -> PyResult<Vec<(Vec<String>, Asset360ChangeMeta)>> {
    blame_map_to_path_stage_map_impl(py, value, blame_map)
}

#[cfg(all(feature = "python-bindings", feature = "stubgen"))]
#[gen_stub_pyfunction]
#[pyfunction(
    name = "format_blame_map",
    signature = (value, blame_map)
)]
fn format_blame_map_py(
    py: Python<'_>,
    #[gen_stub(
        override_type(
            type_repr = "asset360_rust.LinkMLInstance",
            imports = ("asset360_rust",)
        )
    )]
    value: Py<PyLinkMLInstance>,
    #[gen_stub(
        override_type(
            type_repr = "dict[int, asset360_rust.Asset360ChangeMeta]",
            imports = ("asset360_rust",)
        )
    )]
    blame_map: HashMap<NodeId, Asset360ChangeMeta>,
) -> PyResult<String> {
    format_blame_map_impl(py, value, blame_map)
}

#[cfg(all(feature = "python-bindings", not(feature = "stubgen")))]
#[pyfunction(
    name = "format_blame_map",
    signature = (value, blame_map)
)]
fn format_blame_map_py(
    py: Python<'_>,
    value: Py<PyLinkMLInstance>,
    blame_map: HashMap<NodeId, Asset360ChangeMeta>,
) -> PyResult<String> {
    format_blame_map_impl(py, value, blame_map)
}

#[cfg(feature = "python-bindings")]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "Asset360ChangeMeta")]
#[derive(Clone)]
struct PyAsset360ChangeMeta {
    inner: Asset360ChangeMeta,
}

#[cfg(feature = "python-bindings")]
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

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("author", &self.inner.author)?;
        dict.set_item("timestamp", &self.inner.timestamp)?;
        dict.set_item("source", &self.inner.source)?;
        dict.set_item("change_id", self.inner.change_id)?;
        dict.set_item("ics_id", self.inner.ics_id)?;
        Ok(dict.into())
    }
}

#[cfg(feature = "python-bindings")]
impl From<Asset360ChangeMeta> for PyAsset360ChangeMeta {
    fn from(inner: Asset360ChangeMeta) -> Self {
        Self { inner }
    }
}

#[cfg(feature = "python-bindings")]
impl PyAsset360ChangeMeta {
    fn clone_inner(&self) -> Asset360ChangeMeta {
        self.inner.clone()
    }
}

#[cfg(feature = "python-bindings")]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "ChangeStage")]
struct PyChangeStage {
    inner: ChangeStage<Asset360ChangeMeta>,
    sv: Py<PySchemaView>,
    class_id: String,
}

#[cfg(feature = "python-bindings")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PyChangeStage {
    #[new]
    #[pyo3(signature = (meta, value, deltas, rejected_paths=None))]
    fn new(
        py: Python<'_>,
        meta: PyAsset360ChangeMeta,
        value: Py<PyLinkMLInstance>,
        deltas: Vec<Py<PyDelta>>,
        rejected_paths: Option<Vec<Vec<String>>>,
    ) -> PyResult<Self> {
        let stage_value: LinkMLInstance;
        let schema_view: Py<PySchemaView>;
        let class_id: String;
        {
            let bound = value.bind(py);
            let borrowed = bound.borrow();
            let bound_sv = borrowed.sv.bind(py);
            let borrowed_sv = bound_sv.borrow();
            let conv = borrowed_sv.as_rust().converter();
            class_id = Self::value_class_identifier(&borrowed.value, &conv).ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(
                    "ChangeStage value missing class context; cannot serialize",
                )
            })?;
            drop(borrowed_sv);
            stage_value = borrowed.value.clone();
            schema_view = borrowed.sv.clone_ref(py);
        }
        let mut rust_deltas: Vec<Delta> = Vec::with_capacity(deltas.len());
        for delta in deltas {
            let bound = delta.bind(py);
            rust_deltas.push(bound.borrow().inner.clone());
        }
        Ok(Self {
            inner: ChangeStage {
                meta: meta.clone_inner(),
                value: stage_value,
                deltas: rust_deltas,
                rejected_paths: rejected_paths.unwrap_or_default(),
            },
            sv: schema_view,
            class_id,
        })
    }

    #[getter]
    fn meta(&self) -> PyAsset360ChangeMeta {
        PyAsset360ChangeMeta::from(self.inner.meta.clone())
    }

    #[getter]
    fn value(&self, py: Python<'_>) -> PyResult<Py<PyLinkMLInstance>> {
        Py::new(
            py,
            PyLinkMLInstance::new(self.inner.value.clone(), self.sv.clone_ref(py)),
        )
    }

    #[getter]
    fn deltas<'py>(&self, py: Python<'py>) -> PyResult<Vec<Py<PyDelta>>> {
        PyDelta::from_deltas(py, self.inner.deltas.clone())
    }

    #[getter]
    fn rejected_paths(&self) -> Vec<Vec<String>> {
        self.inner.rejected_paths.clone()
    }

    fn to_json(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("class_id", &self.class_id)?;
        dict.set_item(
            "meta",
            PyAsset360ChangeMeta::from(self.inner.meta.clone()).to_dict(py)?,
        )?;
        let json_mod = PyModule::import(py, "json")?;

        let value_json = self.inner.value.to_json();
        let value_str = serde_json::to_string(&value_json).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to encode LinkML value as JSON string: {e}"
            ))
        })?;
        let value_py = json_mod.call_method1("loads", (value_str.as_str(),))?;
        dict.set_item("value", value_py)?;

        let deltas_str = serde_json::to_string(&self.inner.deltas).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to encode deltas as JSON string: {e}"
            ))
        })?;
        let deltas_py = json_mod.call_method1("loads", (deltas_str.as_str(),))?;
        dict.set_item("deltas", deltas_py)?;

        dict.set_item("rejected_paths", &self.inner.rejected_paths)?;
        Ok(dict.into())
    }

    #[staticmethod]
    #[pyo3(signature = (schemaview, data))]
    fn from_json(
        py: Python<'_>,
        schemaview: Py<PySchemaView>,
        data: &Bound<'_, PyDict>,
    ) -> PyResult<Self> {
        let json_mod = PyModule::import(py, "json")?;

        let class_id_obj = data.get_item("class_id")?.ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("missing 'class_id' in ChangeStage JSON")
        })?;
        let class_id: String = class_id_obj.extract()?;
        let meta_obj = data.get_item("meta")?.ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("missing 'meta' in ChangeStage JSON")
        })?;
        let meta = match meta_obj.extract::<PyAsset360ChangeMeta>() {
            Ok(py_meta) => py_meta.clone_inner(),
            Err(_) => {
                let meta_str: String = json_mod
                    .call_method1("dumps", (&meta_obj,))?
                    .extract()
                    .map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "failed to serialize 'meta' payload: {e}"
                        ))
                    })?;
                serde_json::from_str::<Asset360ChangeMeta>(&meta_str).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!("invalid 'meta' payload: {e}"))
                })?
            }
        };

        let value_obj = data.get_item("value")?.ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("missing 'value' in ChangeStage JSON")
        })?;
        let value_str: String = json_mod
            .call_method1("dumps", (&value_obj,))?
            .extract()
            .map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "failed to serialize 'value' payload: {e}"
                ))
            })?;
        let value_json: serde_json::Value = serde_json::from_str(&value_str).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid 'value' payload: {e}"))
        })?;

        let deltas: Vec<Delta> = data
            .get_item("deltas")?
            .map(|obj| {
                let deltas_str: String = json_mod
                    .call_method1("dumps", (&obj,))?
                    .extract()
                    .map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "failed to serialize 'deltas' payload: {e}"
                        ))
                    })?;
                serde_json::from_str(&deltas_str).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "invalid 'deltas' payload: {e}"
                    ))
                })
            })
            .transpose()? // PyResult<Option<Vec<Delta>>>
            .unwrap_or_default();

        let rejected_paths = data
            .get_item("rejected_paths")?
            .map(|obj| obj.extract::<Vec<Vec<String>>>())
            .transpose()?
            .unwrap_or_default();
        let bound_sv = schemaview.bind(py);
        let borrowed_sv = bound_sv.borrow();
        let rust_sv = borrowed_sv.as_rust();
        let conv = rust_sv.converter();
        let class_view = rust_sv
            .get_class(&Identifier::new(&class_id), &conv)
            .map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "error resolving class '{class_id}': {:?}",
                    e
                ))
            })?
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "class '{class_id}' not found in provided SchemaView"
                ))
            })?;
        let value_str = serde_json::to_string(&value_json).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to encode LinkML value as JSON string: {e}"
            ))
        })?;
        let linkml_value = linkml_runtime::load_json_str(&value_str, rust_sv, &class_view, &conv)
            .map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("failed to load LinkML value: {e}"))
            })?
            .into_instance_tolerate_errors()?;

        Ok(Self {
            inner: ChangeStage {
                meta,
                value: linkml_value,
                deltas,
                rejected_paths,
            },
            sv: schemaview.clone_ref(py),
            class_id,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "ChangeStage(meta={}, deltas_len={}, rejected_paths_len={})",
            PyAsset360ChangeMeta::from(self.inner.meta.clone()).__repr__(),
            self.inner.deltas.len(),
            self.inner.rejected_paths.len()
        )
    }
}

#[cfg(feature = "python-bindings")]
impl PyChangeStage {
    fn clone_inner(&self) -> ChangeStage<Asset360ChangeMeta> {
        self.inner.clone()
    }

    fn class_identifier_from_view(class: &ClassView, conv: &Converter) -> String {
        match class.get_uri(conv, false, true) {
            Ok(identifier) => match identifier {
                Identifier::Name(_) => class.canonical_uri().to_string(),
                other => other.to_string(),
            },
            Err(_) => class
                .def()
                .class_uri
                .as_ref()
                .map(|uri| uri.to_string())
                .unwrap_or_else(|| class.canonical_uri().to_string()),
        }
    }

    fn value_class_identifier(value: &LinkMLInstance, conv: &Converter) -> Option<String> {
        match value {
            LinkMLInstance::Object { class, .. } => {
                Some(Self::class_identifier_from_view(class, conv))
            }
            LinkMLInstance::Scalar {
                class: Some(class), ..
            }
            | LinkMLInstance::List {
                class: Some(class), ..
            }
            | LinkMLInstance::Mapping {
                class: Some(class), ..
            }
            | LinkMLInstance::Null {
                class: Some(class), ..
            } => Some(Self::class_identifier_from_view(class, conv)),
            _ => None,
        }
    }

    fn from_inner_py(
        py: Python<'_>,
        inner: ChangeStage<Asset360ChangeMeta>,
        sv: &Py<PySchemaView>,
    ) -> PyResult<Py<PyChangeStage>> {
        let bound_sv = sv.bind(py);
        let borrowed_sv = bound_sv.borrow();
        let conv = borrowed_sv.as_rust().converter();
        let class_id = Self::value_class_identifier(&inner.value, &conv).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "ChangeStage value missing class context; cannot serialize",
            )
        })?;
        drop(borrowed_sv);

        Py::new(
            py,
            PyChangeStage {
                inner,
                sv: sv.clone_ref(py),
                class_id,
            },
        )
    }
}

#[cfg(feature = "python-bindings")]
fn py_change_stage_to_rust(
    py: Python<'_>,
    stage: &Py<PyChangeStage>,
) -> PyResult<(ChangeStage<Asset360ChangeMeta>, Py<PySchemaView>)> {
    let bound = stage.bind(py);
    let borrowed = bound.borrow();
    Ok((borrowed.clone_inner(), borrowed.sv.clone_ref(py)))
}

#[cfg(feature = "python-bindings")]
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

#[cfg(feature = "python-bindings")]
#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(
    name = "compute_history",
    signature = (stages,)
)]
/// Python wrapper for [`crate::blame::compute_history`].
///
/// Accepts a sequence of `ChangeStage` objects, recomputes their semantic
/// deltas while respecting rejected paths, and returns the final
/// `LinkMLInstance` together with updated stages.
fn compute_history_py(
    py: Python<'_>,
    stages: Vec<Py<PyChangeStage>>,
) -> PyResult<(Py<PyLinkMLInstance>, Vec<Py<PyChangeStage>>)> {
    use pyo3::exceptions::PyValueError;

    if stages.is_empty() {
        return Err(PyValueError::new_err(
            "compute_history requires at least one stage",
        ));
    }

    let mut schema_view: Option<Py<PySchemaView>> = None;
    let mut rust_stages: Vec<ChangeStage<Asset360ChangeMeta>> = Vec::with_capacity(stages.len());

    for stage in stages.iter() {
        let (rust_stage, sv) = py_change_stage_to_rust(py, stage)?;
        if let Some(existing) = &schema_view {
            if existing.as_ptr() != sv.as_ptr() {
                return Err(PyValueError::new_err(
                    "all stages must share the same SchemaView",
                ));
            }
        } else {
            schema_view = Some(sv.clone_ref(py));
        }
        rust_stages.push(rust_stage);
    }

    let schema_view = schema_view.expect("non-empty stages validated above");
    let (final_value, history) = crate::blame::compute_history(rust_stages);

    let py_value = Py::new(
        py,
        PyLinkMLInstance::new(final_value, schema_view.clone_ref(py)),
    )?;
    let py_history = history
        .into_iter()
        .map(|stage| PyChangeStage::from_inner_py(py, stage, &schema_view))
        .collect::<PyResult<Vec<_>>>()?;

    Ok((py_value, py_history))
}

#[cfg(feature = "python-bindings")]
fn get_blame_info_py_impl(
    py: Python<'_>,
    value: Py<PyLinkMLInstance>,
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
#[gen_stub_pyfunction]
#[gen_stub(
    override_return_type(
        type_repr = "typing.Optional[asset360_rust.Asset360ChangeMeta]",
        imports = ("typing", "asset360_rust")
    )
)]
#[pyfunction(
    name = "get_blame_info",
    signature = (value, blame_map)
)]
fn get_blame_info_py(
    py: Python<'_>,
    value: Py<PyLinkMLInstance>,
    #[gen_stub(
        override_type(
            type_repr = "dict[int, asset360_rust.Asset360ChangeMeta]",
            imports = ("typing", "asset360_rust")
        )
    )]
    blame_map: HashMap<NodeId, Asset360ChangeMeta>,
) -> PyResult<Option<Py<PyAsset360ChangeMeta>>> {
    get_blame_info_py_impl(py, value, blame_map)
}

#[cfg(all(feature = "python-bindings", not(feature = "stubgen")))]
#[pyfunction(
    name = "get_blame_info",
    signature = (value, blame_map)
)]
fn get_blame_info_py(
    py: Python<'_>,
    value: Py<PyLinkMLInstance>,
    blame_map: HashMap<NodeId, Asset360ChangeMeta>,
) -> PyResult<Option<Py<PyAsset360ChangeMeta>>> {
    get_blame_info_py_impl(py, value, blame_map)
}

// ── SHACL / Business Rules Python bindings ──────────────────────────

/// Evaluate a SHACL AST against object data (forward validation).
///
/// * `ast_json` – JSON-serialized `ShaclAst`
/// * `object_data_json` – JSON object with field values
/// * `message` – violation message
/// * `enforcement_level` – one of "critical", "serious", "error", "unlikely"
///
/// Returns JSON array of violations (empty = valid).
#[cfg(feature = "python-bindings")]
#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(name = "evaluate_forward", signature = (ast_json, object_data_json, message, enforcement_level))]
fn evaluate_forward_py(
    ast_json: &str,
    object_data_json: &str,
    message: &str,
    enforcement_level: &str,
) -> PyResult<String> {
    let ast: crate::shacl_ast::ShaclAst = serde_json::from_str(ast_json)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("invalid AST JSON: {e}")))?;
    let data: serde_json::Value = serde_json::from_str(object_data_json)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("invalid data JSON: {e}")))?;
    let level: crate::shacl_ast::EnforcementLevel = serde_json::from_value(
        serde_json::Value::String(enforcement_level.to_owned()),
    )
    .unwrap_or(crate::shacl_ast::EnforcementLevel::Error);
    let violations = crate::forward_eval::evaluate_forward(&ast, &data, message, &level);
    serde_json::to_string(&violations)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("serialize error: {e}")))
}

/// Solve backward: given an AST and known field values, produce a Predicate
/// for the target field describing its allowed values.
///
/// * `ast_json` – JSON-serialized `ShaclAst`
/// * `known_fields_json` – JSON object of known field name → value
/// * `target_field` – field name to solve for
///
/// Returns JSON-serialized `Predicate` or `None`.
#[cfg(feature = "python-bindings")]
#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(name = "solve_backward", signature = (ast_json, known_fields_json, target_field))]
fn solve_backward_py(
    ast_json: &str,
    known_fields_json: &str,
    target_field: &str,
) -> PyResult<Option<String>> {
    let ast: crate::shacl_ast::ShaclAst = serde_json::from_str(ast_json)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("invalid AST JSON: {e}")))?;
    let known: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(known_fields_json).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid known fields JSON: {e}"))
        })?;
    match crate::backward_solver::solve_backward(&ast, &known, target_field) {
        Some(pred) => {
            let json = serde_json::to_string(&pred).map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("serialize error: {e}"))
            })?;
            Ok(Some(json))
        }
        None => Ok(None),
    }
}

/// Derive a scope predicate for fetching peer objects relevant to a constraint.
///
/// * `shape_json` – JSON-serialized `ShapeResult`
/// * `focus_data_json` – JSON object of the focus object's field values
/// * `uri_field` – field name holding the object URI (default: "asset360_uri")
///
/// Returns JSON-serialized `Predicate` or `None`.
#[cfg(feature = "python-bindings")]
#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(name = "derive_scope_predicate", signature = (shape_json, focus_data_json, uri_field="asset360_uri"))]
fn derive_scope_predicate_py(
    shape_json: &str,
    focus_data_json: &str,
    uri_field: &str,
) -> PyResult<Option<String>> {
    let shape: crate::shacl_ast::ShapeResult = serde_json::from_str(shape_json).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid shape JSON: {e}"))
    })?;
    let focus: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(focus_data_json).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid focus data JSON: {e}"))
        })?;
    match crate::scope_predicate::derive_scope_predicate(&shape, &focus, uri_field) {
        Some(pred) => {
            let json = serde_json::to_string(&pred).map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("serialize error: {e}"))
            })?;
            Ok(Some(json))
        }
        None => Ok(None),
    }
}

/// Parse a SHACL Turtle file and extract shapes targeting a class.
///
/// * `ttl` – SHACL Turtle text
/// * `target_class` – class name to filter (empty string = all shapes)
///
/// Returns JSON array of `ShapeResult` objects.
#[cfg(all(feature = "python-bindings", feature = "shacl-parser"))]
#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(name = "parse_shacl", signature = (ttl, target_class))]
fn parse_shacl_py(ttl: &str, target_class: &str) -> PyResult<String> {
    let results = crate::shacl_parser::parse_shacl(ttl, target_class).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("SHACL parse error: {e}"))
    })?;
    serde_json::to_string(&results)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("serialize error: {e}")))
}

#[cfg(all(feature = "python-bindings", feature = "stubgen"))]
define_stub_info_gatherer!(stub_info);

#[cfg(all(test, feature = "python-bindings"))]
mod tests {
    use std::hint::black_box;
    use std::path::Path;
    use std::time::Instant;

    use super::*;
    use linkml_meta::SchemaDefinition;

    #[test]
    fn test_compute_classes_by_type_designator_basic() {
        let schema_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("data")
            .join("asset360.yaml");
        let yaml = std::fs::read_to_string(&schema_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", schema_path.display()));
        let deser = serde_yml::Deserializer::from_str(&yaml);
        let schema: SchemaDefinition = serde_path_to_error::deserialize(deser).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        let baseline =
            compute_classes_by_type_designator(&sv, true, true, None, DEFAULT_MANAGED_ANNOTATION);
        assert!(
            !baseline.is_empty(),
            "expected managed classes with designator entries"
        );
        let sample = baseline
            .values()
            .next()
            .expect("at least one managed class available");
        assert!(sample.name().contains(':') || !sample.name().is_empty());

        let iterations = std::env::var("TYPE_DESIGNATOR_BENCH_ITERS")
            .ok()
            .and_then(|raw| raw.parse::<u32>().ok())
            .filter(|iters| *iters > 0)
            .unwrap_or(10_000u32);
        let start = Instant::now();
        for _ in 0..iterations {
            let result = compute_classes_by_type_designator(
                &sv,
                true,
                true,
                None,
                DEFAULT_MANAGED_ANNOTATION,
            );
            black_box(result);
        }
        let elapsed = start.elapsed();
        let per_iter = elapsed.as_secs_f64() / f64::from(iterations);
        println!(
            "compute_classes_by_type_designator: {:.6} s/iter over {iterations} iterations",
            per_iter
        );
    }
}
