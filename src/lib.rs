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

pub mod backward_solver;
pub mod blame;
pub mod constraint_set;
pub mod foreign_references;
pub mod forward_eval;
pub mod predicate;
pub mod scope_predicate;
pub mod shacl_ast;

#[cfg(feature = "sparql-endpoint")]
pub mod sparql_executor;
pub mod sparql_pushdown;
pub mod sparql_scoper;
pub mod sparql_terms;

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
    m.add_function(wrap_pyfunction!(get_foreign_references_py, m)?)?;
    m.add_class::<PyForeignReference>()?;
    m.add_class::<PyConstraintSet>()?;
    #[cfg(feature = "sparql-endpoint")]
    {
        m.add_function(wrap_pyfunction!(sparql_scope, m)?)?;
        m.add_function(wrap_pyfunction!(py_sparql_pushdown, m)?)?;
        m.add_function(wrap_pyfunction!(sparql_execute, m)?)?;
        m.add_class::<QueryPlan>()?;
        m.add_class::<PlanNode>()?;
        m.add_class::<Star>()?;
        m.add_class::<JoinEdge>()?;
        m.add_class::<FilterCondition>()?;
        m.add_class::<PushdownVerdict>()?;
        m.add_class::<PushdownSolution>()?;
        m.add_class::<PushdownBinding>()?;
        m.add_class::<PushdownMeasure>()?;
        m.add_class::<PushdownOrder>()?;
    }
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

/// Default annotation keys used to mark a class as "managed" by asset360.
/// Both the short and fully-qualified forms are tried so that models using
/// either convention (e.g. RINF vs asset360) work out of the box.
#[cfg(feature = "python-bindings")]
pub const DEFAULT_MANAGED_ANNOTATIONS: &[&str] = &[
    "data.infrabel.be/asset360/managed",
    "asset360/managed",
    "infraventory/managed",
    "rinf/managed",
];

#[cfg(feature = "python-bindings")]
fn compute_classes_by_type_designator(
    sv: &SchemaView,
    only_registered: bool,
    only_default: bool,
    py: Option<Python<'_>>,
    managed_annotations: &[&str],
) -> HashMap<String, ClassView> {
    let mut out: HashMap<String, ClassView> = HashMap::new();

    sv.with_schema_definitions(|schemas| {
        for (schema_id, schema) in schemas {
            let mut process_classes = |conv: &Converter| {
                if let Some(classes) = &schema.classes {
                    for (class_name, class_def) in classes {
                        if only_registered {
                            let managed = class_def.annotations.as_ref().and_then(|m| {
                                managed_annotations.iter().find_map(|key| m.get(*key))
                            });
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
/// * `managed_annotations` – annotation keys to try when checking whether a
///   class is managed.  When `None`, both `"data.infrabel.be/asset360/managed"`
///   and `"asset360/managed"` are tried so that every known datamodel works out
///   of the box.
#[cfg(feature = "python-bindings")]
fn get_all_classes_by_type_designator_and_schema_impl(
    py: Python<'_>,
    schemaview: Py<PySchemaView>,
    only_registered: bool,
    only_default: bool,
    managed_annotations: Option<Vec<String>>,
) -> PyResult<HashMap<String, Py<PyClassView>>> {
    let bound = schemaview.bind(py);
    let sv_ref = bound.borrow();
    let keys: Vec<&str> = match &managed_annotations {
        Some(v) => v.iter().map(|s| s.as_str()).collect(),
        None => DEFAULT_MANAGED_ANNOTATIONS.to_vec(),
    };
    let raw = compute_classes_by_type_designator(
        sv_ref.as_rust(),
        only_registered,
        only_default,
        Some(py),
        &keys,
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
/// * `only_registered` – require the annotation named by one of the
///   `managed_annotations` keys to be truthy.
/// * `only_default` – restrict to each class' primary type designator instead of
///   all accepted aliases.
/// * `managed_annotations` – annotation keys to try (default: `None`, which
///   tries both `"data.infrabel.be/asset360/managed"` and `"asset360/managed"`).
#[gen_stub_pyfunction]
#[gen_stub(
    override_return_type(
        type_repr = "dict[str, asset360_rust.ClassView]",
        imports = ("asset360_rust",)
    )
)]
#[pyfunction(
    name = "get_all_classes_by_type_designator_and_schema",
    signature = (schemaview, only_registered=true, only_default=true, managed_annotations=None)
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
    managed_annotations: Option<Vec<String>>,
) -> PyResult<HashMap<String, Py<PyClassView>>> {
    get_all_classes_by_type_designator_and_schema_impl(
        py,
        schemaview,
        only_registered,
        only_default,
        managed_annotations,
    )
}

#[cfg(all(feature = "python-bindings", not(feature = "stubgen")))]
/// Return every class keyed by its resolved type designator.
///
/// * `schemaview` – existing [`SchemaView`] instance to inspect.
/// * `only_registered` – require the annotation named by one of the
///   `managed_annotations` keys to be truthy.
/// * `only_default` – restrict to each class' primary type designator instead of
///   all accepted aliases.
/// * `managed_annotations` – annotation keys to try (default: `None`, which
///   tries both `"data.infrabel.be/asset360/managed"` and `"asset360/managed"`).
#[pyfunction(
    name = "get_all_classes_by_type_designator_and_schema",
    signature = (schemaview, only_registered=true, only_default=true, managed_annotations=None)
)]
fn get_all_classes_by_type_designator_and_schema(
    py: Python<'_>,
    schemaview: Py<PySchemaView>,
    only_registered: bool,
    only_default: bool,
    managed_annotations: Option<Vec<String>>,
) -> PyResult<HashMap<String, Py<PyClassView>>> {
    get_all_classes_by_type_designator_and_schema_impl(
        py,
        schemaview,
        only_registered,
        only_default,
        managed_annotations,
    )
}

#[cfg(feature = "python-bindings")]
fn blame_map_to_path_stage_map_impl(
    py: Python<'_>,
    value: Py<PyLinkMLInstance>,
    blame_map: HashMap<NodeId, Asset360ChangeMeta>,
) -> PyResult<Vec<(Vec<String>, Asset360ChangeMeta)>> {
    let bound = value.bind(py);
    let borrowed = bound.borrow();
    Ok(crate::blame::blame_map_to_path_stage_map(
        &borrowed.value,
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
    let borrowed = bound.borrow();
    Ok(crate::blame::format_blame_map(&borrowed.value, &blame_map))
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
    let borrowed = bound.borrow();

    if let Some(meta) = crate::blame::get_blame_info(&borrowed.value, &blame_map) {
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

// ── Foreign references Python bindings ────────────────────────────────

#[cfg(feature = "python-bindings")]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "ForeignReference")]
#[derive(Clone)]
struct PyForeignReference {
    inner: crate::foreign_references::ForeignReference,
}

#[cfg(feature = "python-bindings")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PyForeignReference {
    #[getter]
    fn uri(&self) -> &str {
        &self.inner.uri
    }

    #[getter]
    fn object_type(&self) -> &str {
        &self.inner.object_type
    }

    #[getter]
    fn object_type_uri(&self) -> &str {
        &self.inner.object_type_uri
    }

    #[getter]
    fn slot_name(&self) -> &str {
        &self.inner.slot_name
    }

    #[getter]
    fn slot_path(&self) -> Vec<String> {
        self.inner.slot_path.clone()
    }

    #[getter]
    fn kind(&self) -> &str {
        self.inner.kind.as_str()
    }

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("uri", &self.inner.uri)?;
        dict.set_item("object_type", &self.inner.object_type)?;
        dict.set_item("object_type_uri", &self.inner.object_type_uri)?;
        dict.set_item("slot_name", &self.inner.slot_name)?;
        dict.set_item("slot_path", &self.inner.slot_path)?;
        dict.set_item("kind", self.inner.kind.as_str())?;
        Ok(dict.into())
    }

    fn __repr__(&self) -> String {
        format!(
            "ForeignReference(uri='{}', object_type='{}', slot_name='{}', kind='{}')",
            self.inner.uri,
            self.inner.object_type,
            self.inner.slot_name,
            self.inner.kind.as_str()
        )
    }
}

#[cfg(feature = "python-bindings")]
fn get_foreign_references_impl(
    py: Python<'_>,
    instance: Py<PyLinkMLInstance>,
    also_include_id_slots: bool,
) -> PyResult<Vec<Py<PyForeignReference>>> {
    let bound = instance.bind(py);
    let borrowed = bound.borrow();
    // Borrow the instance rather than cloning the entire tree — this is a hot path.
    let refs =
        crate::foreign_references::get_foreign_references(&borrowed.value, also_include_id_slots);

    refs.into_iter()
        .map(|r| Py::new(py, PyForeignReference { inner: r }))
        .collect()
}

#[cfg(all(feature = "python-bindings", feature = "stubgen"))]
#[gen_stub_pyfunction]
#[gen_stub(
    override_return_type(
        type_repr = "list[asset360_rust.ForeignReference]",
        imports = ("asset360_rust",)
    )
)]
#[pyfunction(
    name = "get_foreign_references",
    signature = (instance, also_include_id_slots=false)
)]
fn get_foreign_references_py(
    py: Python<'_>,
    #[gen_stub(
        override_type(
            type_repr = "asset360_rust.LinkMLInstance",
            imports = ("asset360_rust",)
        )
    )]
    instance: Py<PyLinkMLInstance>,
    also_include_id_slots: bool,
) -> PyResult<Vec<Py<PyForeignReference>>> {
    get_foreign_references_impl(py, instance, also_include_id_slots)
}

#[cfg(all(feature = "python-bindings", not(feature = "stubgen")))]
#[pyfunction(
    name = "get_foreign_references",
    signature = (instance, also_include_id_slots=false)
)]
fn get_foreign_references_py(
    py: Python<'_>,
    instance: Py<PyLinkMLInstance>,
    also_include_id_slots: bool,
) -> PyResult<Vec<Py<PyForeignReference>>> {
    get_foreign_references_impl(py, instance, also_include_id_slots)
}

// ── ConstraintSet Python bindings ────────────────────────────────────

#[cfg(feature = "python-bindings")]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "ConstraintSet")]
struct PyConstraintSet {
    inner: crate::constraint_set::ConstraintSet,
}

#[cfg(feature = "python-bindings")]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PyConstraintSet {
    /// Create a ConstraintSet from a JSON array of ShapeResult objects.
    #[staticmethod]
    #[pyo3(signature = (json,))]
    fn from_json(json: &str) -> PyResult<Self> {
        let inner = crate::constraint_set::ConstraintSet::from_json(json)
            .map_err(pyo3::exceptions::PyValueError::new_err)?;
        Ok(Self { inner })
    }

    /// Parse SHACL Turtle text into a ConstraintSet.
    #[cfg(feature = "shacl-parser")]
    #[staticmethod]
    #[pyo3(signature = (ttl, target_class, language="", schema_view=None))]
    fn from_shacl(
        py: Python<'_>,
        ttl: &str,
        target_class: &str,
        language: &str,
        schema_view: Option<Py<PySchemaView>>,
    ) -> PyResult<Self> {
        let sv_option = schema_view.as_ref().map(|sv| {
            let bound = sv.bind(py);
            let borrowed = bound.borrow();
            borrowed.as_rust().clone()
        });
        let inner = crate::constraint_set::ConstraintSet::from_shacl(
            ttl,
            target_class,
            language,
            sv_option.as_ref(),
        )
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
        Ok(Self { inner })
    }

    /// Attach a schema view (returns a new ConstraintSet with schema awareness).
    #[pyo3(signature = (schema_view, target_class))]
    fn with_schema_view(
        &self,
        py: Python<'_>,
        schema_view: Py<PySchemaView>,
        target_class: &str,
    ) -> PyResult<Self> {
        let bound = schema_view.bind(py);
        let borrowed = bound.borrow();
        let sv = borrowed.as_rust();
        let new_inner = self
            .inner
            .clone()
            .with_schema_view(sv, target_class)
            .map_err(pyo3::exceptions::PyValueError::new_err)?;
        Ok(Self { inner: new_inner })
    }

    /// Serialize the shapes to JSON.
    fn to_json(&self) -> PyResult<String> {
        self.inner
            .to_json()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("serialize error: {e}")))
    }

    /// Evaluate all shapes against object data, returning JSON array of violations.
    #[pyo3(signature = (object_data_json,))]
    fn evaluate(&self, object_data_json: &str) -> PyResult<String> {
        let data: serde_json::Value = serde_json::from_str(object_data_json).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid data JSON: {e}"))
        })?;
        let violations = self.inner.evaluate(&data);
        serde_json::to_string(&violations)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("serialize error: {e}")))
    }

    /// Solve backward for a target field, returning JSON FieldConstraint or None.
    #[pyo3(signature = (object_data_json, target_field))]
    fn solve(&self, object_data_json: &str, target_field: &str) -> PyResult<Option<String>> {
        let data: serde_json::Value = serde_json::from_str(object_data_json).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid data JSON: {e}"))
        })?;
        match self.inner.solve(&data, target_field) {
            Some(fc) => {
                let json = serde_json::to_string(&fc).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!("serialize error: {e}"))
                })?;
                Ok(Some(json))
            }
            None => Ok(None),
        }
    }

    /// Solve allowed values for an array-member field, returning JSON
    /// FieldConstraint or None. `editing_index` excludes the edited member's own
    /// value from "already used" (None for a new member).
    #[pyo3(signature = (object_data_json, array_field, member_field, editing_index=None))]
    fn solve_member(
        &self,
        object_data_json: &str,
        array_field: &str,
        member_field: &str,
        editing_index: Option<usize>,
    ) -> PyResult<Option<String>> {
        let data: serde_json::Value = serde_json::from_str(object_data_json).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid data JSON: {e}"))
        })?;
        match self
            .inner
            .solve_member(&data, array_field, member_field, editing_index)
        {
            Some(fc) => {
                let json = serde_json::to_string(&fc).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!("serialize error: {e}"))
                })?;
                Ok(Some(json))
            }
            None => Ok(None),
        }
    }

    /// Derive scope predicate, returning JSON Predicate or None.
    #[pyo3(signature = (focus_data_json, uri_field="asset360_uri"))]
    fn scope(&self, focus_data_json: &str, uri_field: &str) -> PyResult<Option<String>> {
        let focus: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(focus_data_json).map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("invalid focus data JSON: {e}"))
            })?;
        match self.inner.scope(&focus, uri_field) {
            Some(pred) => {
                let json = serde_json::to_string(&pred).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!("serialize error: {e}"))
                })?;
                Ok(Some(json))
            }
            None => Ok(None),
        }
    }

    /// Return all field names referenced by any shape.
    fn affected_fields(&self) -> Vec<String> {
        self.inner.affected_fields()
    }

    fn __repr__(&self) -> String {
        format!(
            "ConstraintSet(shapes={}, has_schema={})",
            self.inner.shape_count(),
            self.inner.has_schema()
        )
    }
}

// ---- SPARQL endpoint PyO3 bindings ----

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[pyclass]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[derive(Clone)]
/// A filter condition extracted from the SPARQL query, pushable to SQL.
///
/// Each condition has an `operator` and one or more string `values`:
///
/// | operator | from | values |
/// |---|---|---|
/// | `"eq"` | `FILTER(?v = "x")`, `?s :slot "x"` | one |
/// | `"in"` | `VALUES ?v { "a" "b" }` | one or more |
/// | `"gt"` / `"gte"` / `"lt"` / `"lte"` | `FILTER(?v > 10)` and friends | one |
///
/// A comparison compares the way SPARQL does, not the way text does: a numeric
/// slot casts (as text, `"9" > "10"`) and a string slot needs codepoint
/// collation. The consumer decides from the slot's type, which is why the
/// operator alone is carried here.
///
/// **Unknown operators must be refused, not ignored.** A newer planner can emit
/// one this consumer does not know, and silently dropping it widens the fetch
/// instead of narrowing it — which stays correct only while something else
/// re-applies the filter.
///
/// Python usage:
///
/// ```python
/// for field, conditions in scope.predicate_filters.items():
///     for cond in conditions:
///         if cond.operator == "eq":
///             qs = qs.filter(**{f"object_data__{field}": cond.value})
///         elif cond.operator == "in":
///             qs = qs.filter(**{f"object_data__{field}__in": cond.values})
///         elif cond.operator in ("gt", "gte", "lt", "lte"):
///             qs = qs.filter(**{f"object_data__{field}__{cond.operator}": cond.value})
///         else:
///             raise ValueError(f"unsupported filter operator: {cond.operator}")
/// ```
pub struct FilterCondition {
    operator: String,
    values: Vec<String>,
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl FilterCondition {
    /// The filter operator: ``"eq"`` (equality), ``"in"`` (set membership), or
    /// ``"gt"`` / ``"gte"`` / ``"lt"`` / ``"lte"`` (ordering comparison).
    ///
    /// ``"ne"`` is deliberately absent: SPARQL's inequality is false for an
    /// unbound variable, where SQL's ``<>`` against NULL is unknown and would
    /// drop rows the query keeps.
    #[getter]
    fn operator(&self) -> &str {
        &self.operator
    }

    /// The single filter value, for every operator except ``"in"``.
    ///
    /// Shorthand for ``self.values[0]``. Raises ``IndexError`` if called on
    /// an empty condition (should not happen in practice).
    #[getter]
    fn value(&self) -> PyResult<&str> {
        self.values
            .first()
            .map(|s| s.as_str())
            .ok_or_else(|| pyo3::exceptions::PyIndexError::new_err("empty filter condition"))
    }

    /// All filter values as a list.
    ///
    /// One element for every operator except ``"in"``, which may have several.
    #[getter]
    fn values(&self) -> Vec<String> {
        self.values.clone()
    }

    fn __repr__(&self) -> String {
        // Print the operator rather than branching on one known value and
        // labelling everything else "in" — which is how a `gt` condition came
        // out as `FilterCondition(in=["10"])`.
        if self.operator == "in" {
            format!("FilterCondition(in={:?})", self.values)
        } else {
            format!(
                "FilterCondition({}={:?})",
                self.operator,
                self.values.first().map_or("", String::as_str)
            )
        }
    }
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
impl FilterCondition {
    fn from_rust(cond: &crate::sparql_scoper::FilterCondition) -> Self {
        match cond {
            crate::sparql_scoper::FilterCondition::Eq(v) => Self {
                operator: "eq".to_owned(),
                values: vec![v.clone()],
            },
            crate::sparql_scoper::FilterCondition::In(vs) => Self {
                operator: "in".to_owned(),
                values: vs.clone(),
            },
            crate::sparql_scoper::FilterCondition::Cmp { op, value } => Self {
                operator: op.as_str().to_owned(),
                values: vec![value.clone()],
            },
        }
    }
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[pyclass]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[derive(Clone)]
/// A star in the query plan — one LinkML class with its constraints.
///
/// Named after the SPARQL algebra concept of "star-shaped sub-pattern":
/// all triple patterns sharing the same subject variable.
pub struct Star {
    inner: crate::sparql_scoper::Star,
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl Star {
    /// The SPARQL variable name (without ``?``), e.g. ``"complex"``.
    #[getter]
    fn variable(&self) -> &str {
        &self.inner.variable
    }

    /// The full RDF class IRI, e.g.
    /// ``"https://data.infrabel.be/asset360/TunnelComplex"``.
    /// Compare against the indexed ``asset_type`` column with ``=``.
    #[getter]
    fn class_uri(&self) -> &str {
        &self.inner.class_uri
    }

    /// Values bound to the LinkML identifier slot of this class.
    /// Schema-resolved (``identifier: true``), never assumed to be
    /// named ``"id"``. Empty when the query has no identifier predicate.
    /// Python should translate to ``WHERE asset360_uri IN (...)``
    /// against the indexed column — NOT JSONB extraction. The
    /// identifier slot is also absent from ``filters`` and
    /// ``required_fields``.
    #[getter]
    fn identifier_values(&self) -> Vec<String> {
        self.inner.identifier_values.clone()
    }

    /// Slots referenced in mandatory triple patterns. Python uses for
    /// existence checks: ``WHERE object_data ? 'hasName'``.
    #[getter]
    fn required_fields(&self) -> Vec<String> {
        self.inner.required_fields.clone()
    }

    /// Slots referenced only inside OPTIONAL blocks. Python fetches
    /// them without an existence check so rows missing the slot still
    /// reach oxigraph.
    #[getter]
    fn optional_fields(&self) -> Vec<String> {
        self.inner.optional_fields.clone()
    }

    /// ``True`` if this star itself appears only inside an OPTIONAL
    /// block — its ``WHERE`` conditions must be null-guarded by Python.
    #[getter]
    fn is_optional(&self) -> bool {
        self.inner.is_optional
    }

    /// Value-level filter conditions per slot, pushable to SQL.
    #[getter]
    fn filters(&self) -> HashMap<String, Vec<FilterCondition>> {
        self.inner
            .filters
            .iter()
            .map(|(field, conds)| {
                let py_conds = conds.iter().map(FilterCondition::from_rust).collect();
                (field.clone(), py_conds)
            })
            .collect()
    }

    /// Which SPARQL variable each slot binds to, as ``{slot: variable}``.
    ///
    /// Answers "where does ``?name`` come from" without re-parsing the
    /// query: the star decomposition already worked it out to find join
    /// edges. Only object *variables* appear — a constant object is a
    /// filter, not a binding.
    #[getter]
    fn slot_variables(&self) -> HashMap<String, String> {
        self.inner.slot_variables.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "Star(variable={:?}, class_uri={:?}, fields={:?})",
            self.inner.variable, self.inner.class_uri, self.inner.required_fields
        )
    }
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[pyclass]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[derive(Clone)]
/// A join between two stars, pushable to a SQL JOIN.
///
/// The ``right`` star has a slot (``right_slot``) whose value equals
/// the ``left`` star's ``asset360_uri``.
pub struct JoinEdge {
    inner: crate::sparql_scoper::JoinEdge,
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl JoinEdge {
    /// Variable of the referenced star (join target).
    #[getter]
    fn left(&self) -> &str {
        &self.inner.left
    }

    /// Variable of the star holding the foreign key.
    #[getter]
    fn right(&self) -> &str {
        &self.inner.right
    }

    /// Slot on the right star whose value = left's ``asset360_uri``.
    #[getter]
    fn right_slot(&self) -> &str {
        &self.inner.right_slot
    }

    /// Join type: ``"inner"`` or ``"left"``.
    #[getter]
    fn join_type(&self) -> &str {
        match self.inner.join_type {
            crate::sparql_scoper::JoinType::Inner => "inner",
            crate::sparql_scoper::JoinType::Left => "left",
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "JoinEdge(left={:?}, right={:?}, slot={:?}, type={:?})",
            self.inner.left,
            self.inner.right,
            self.inner.right_slot,
            self.join_type()
        )
    }
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[pyclass]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[derive(Clone)]
/// One node in the query plan algebra tree.
///
/// Today only two kinds are produced: ``"bgp"`` (a Basic Graph Pattern
/// — a group of stars with inner joins) and ``"left_join"`` (SPARQL
/// ``OPTIONAL``: left-join semantics). Future SPARQL constructs
/// (``UNION``, ``MINUS``, ``NOT EXISTS``, property paths) will be
/// added as new kinds — Python consumers MUST pattern-match on
/// ``kind`` and raise ``ValueError("unsupported_plan_node: <kind>")``
/// for anything unknown, so that older clients fail loudly rather
/// than silently miscomputing.
pub struct PlanNode {
    inner: crate::sparql_scoper::PlanNode,
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PlanNode {
    /// Node kind discriminator: ``"bgp"`` or ``"left_join"``.
    #[getter]
    fn kind(&self) -> &str {
        match &self.inner {
            crate::sparql_scoper::PlanNode::Bgp { .. } => "bgp",
            crate::sparql_scoper::PlanNode::LeftJoin { .. } => "left_join",
        }
    }

    /// For ``kind=="bgp"``: the stars in this basic graph pattern.
    /// For ``kind=="left_join"``: an empty list (use ``.left``/``.right``).
    #[getter]
    fn stars(&self) -> Vec<Star> {
        match &self.inner {
            crate::sparql_scoper::PlanNode::Bgp { stars, .. } => {
                stars.iter().map(|s| Star { inner: s.clone() }).collect()
            }
            crate::sparql_scoper::PlanNode::LeftJoin { .. } => Vec::new(),
        }
    }

    /// For ``kind=="bgp"``: the join edges in this basic graph
    /// pattern. For ``kind=="left_join"``: an empty list.
    #[getter]
    fn joins(&self) -> Vec<JoinEdge> {
        match &self.inner {
            crate::sparql_scoper::PlanNode::Bgp { joins, .. } => joins
                .iter()
                .map(|j| JoinEdge { inner: j.clone() })
                .collect(),
            crate::sparql_scoper::PlanNode::LeftJoin { .. } => Vec::new(),
        }
    }

    /// For ``kind=="left_join"``: the left (mandatory) sub-plan.
    /// For ``kind=="bgp"``: returns ``None``.
    #[getter]
    fn left(&self) -> Option<PlanNode> {
        match &self.inner {
            crate::sparql_scoper::PlanNode::LeftJoin { left, .. } => Some(PlanNode {
                inner: (**left).clone(),
            }),
            _ => None,
        }
    }

    /// For ``kind=="left_join"``: the right (optional) sub-plan.
    /// For ``kind=="bgp"``: returns ``None``.
    #[getter]
    fn right(&self) -> Option<PlanNode> {
        match &self.inner {
            crate::sparql_scoper::PlanNode::LeftJoin { right, .. } => Some(PlanNode {
                inner: (**right).clone(),
            }),
            _ => None,
        }
    }

    fn __repr__(&self) -> String {
        format!("PlanNode(kind={:?})", self.kind())
    }
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[pyclass]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[derive(Clone)]
/// Structured plan for fetching data from PostgreSQL.
///
/// Shaped as an algebra tree rooted at :class:`PlanNode`. Legacy flat
/// accessors ``.stars`` and ``.joins`` walk the tree and return all
/// stars / joins pre-order for call-sites that don't need the tree
/// structure.
pub struct QueryPlan {
    root: PlanNode,
    sql_limit: Option<usize>,
    path_bindings: HashMap<String, (String, Vec<String>)>,
    exact: bool,
    inexact_reason: Option<&'static str>,
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl QueryPlan {
    /// Variables reached by walking into a star's nested structures, as
    /// ``{variable: (star_variable, [slot, ...])}``.
    ///
    /// ``?s :location ?l . ?l :longitude ?v`` binds ``?v`` two slots down from
    /// ``?s``, which no star can describe — ``?l`` has no ``rdf:type`` and is
    /// part of ``?s``'s JSON rather than an object of its own. Read the value
    /// with ``object_data->'location'->>'longitude'``.
    ///
    /// Only scalar leaves appear. A variable standing for the nested structure
    /// itself serialises as a blank node, which nothing can reproduce, so it is
    /// traversable but never a value.
    #[getter]
    fn path_bindings(&self) -> HashMap<String, (String, Vec<String>)> {
        self.path_bindings.clone()
    }

    /// Whether this plan describes the query *completely*.
    ///
    /// Extraction is deliberately lossy in the safe direction: a constraint
    /// that cannot be expressed is dropped, the fetch widens, and oxigraph
    /// re-applies the real query to what came back. ``False`` means something
    /// was dropped — a ``FILTER`` this cannot express, a triple whose subject
    /// is not a scoped class, a sub-``SELECT``, a ``FILTER`` inside
    /// ``OPTIONAL``.
    ///
    /// A consumer that only needs a superset (fetch objects, let oxigraph
    /// answer) may ignore this. A consumer that answers *from* the plan must
    /// refuse when it is ``False``, or it answers a weaker question and reports
    /// a plausible wrong number.
    #[getter]
    fn exact(&self) -> bool {
        self.exact
    }

    /// Why the plan is not exact, or ``None`` when it is: one of
    /// ``filter_expression``, ``filter_in_optional``, ``variable_predicate``,
    /// ``unknown_predicate``, ``unscoped_subject``, ``untyped_subject``,
    /// ``constant_in_optional``, ``unbound_values``, ``subquery``.
    ///
    /// Recorded at the point the planner dropped something, so this names a
    /// real cause rather than an inference about what survived.
    #[getter]
    fn inexact_reason(&self) -> Option<&'static str> {
        self.inexact_reason
    }

    /// Root of the algebra tree.
    #[getter]
    fn root(&self) -> PlanNode {
        self.root.clone()
    }

    /// All stars (type-scoped subject groups) in the query, flattened
    /// from the algebra tree in pre-order. Legacy accessor.
    #[getter]
    fn stars(&self) -> Vec<Star> {
        self.root
            .inner
            .all_stars()
            .into_iter()
            .map(|s| Star { inner: s.clone() })
            .collect()
    }

    /// All join edges in the query, flattened from the algebra tree
    /// in pre-order. Legacy accessor.
    #[getter]
    fn joins(&self) -> Vec<JoinEdge> {
        self.root
            .inner
            .all_joins()
            .into_iter()
            .map(|j| JoinEdge { inner: j.clone() })
            .collect()
    }

    /// SQL LIMIT — only for single-star, zero-join, zero-OPTIONAL
    /// queries with a top-level SPARQL LIMIT.
    #[getter]
    fn sql_limit(&self) -> Option<usize> {
        self.sql_limit
    }

    fn __repr__(&self) -> String {
        format!(
            "QueryPlan(root={:?}, limit={:?})",
            self.root.kind(),
            self.sql_limit
        )
    }
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[pyclass]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[derive(Clone)]
/// One projected value in a pushdown solution: which slot of which star it
/// reads, and where that sits in the schema.
///
/// Result columns are addressed by *position* in
/// :attr:`PushdownSolution.bindings`, never by SPARQL variable name — those
/// come from the request, and unquoted SQL identifiers case-fold and truncate
/// at 63 bytes, so two distinct variables could silently collide into one
/// column. ``var`` is for labelling the response only.
pub struct PushdownBinding {
    inner: crate::sparql_pushdown::BindingSpec,
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PushdownBinding {
    /// SPARQL variable name (without ``?``) — the result column label.
    #[getter]
    fn var(&self) -> &str {
        &self.inner.var
    }

    /// The star's variable, which the SQL builder maps to a table alias.
    #[getter]
    fn star_var(&self) -> &str {
        &self.inner.star_var
    }

    /// Slot path from the object root to the value. Empty means the object's
    /// own IRI (the indexed ``asset360_uri`` column, not JSONB).
    #[getter]
    fn slot_path(&self) -> Vec<String> {
        self.inner.slot_path.clone()
    }

    /// Class IRI this binding's slot path is resolved against — the schema
    /// position to ask for a term descriptor.
    #[getter]
    fn class_uri(&self) -> &str {
        &self.inner.term_ref.class_uri
    }

    /// How each step of ``slot_path`` is stored: ``"single"``, ``"list"`` or
    /// ``"mapping"``, one entry per step.
    ///
    /// RDF gives one triple per element of a collection, so a query over a
    /// multivalued slot has one solution per element. A consumer must unnest
    /// those (``jsonb_array_elements`` for a list, ``jsonb_each`` for a
    /// mapping, whose keys are not part of the graph) or the counts come out as
    /// one per record. Any hop along a path may be a collection, and each one
    /// multiplies the rows.
    #[getter]
    fn containers(&self) -> Vec<&'static str> {
        self.inner
            .containers
            .iter()
            .map(|container| container.as_str())
            .collect()
    }

    /// ``True`` when the slot's values are numbers.
    ///
    /// The renderer needs this twice over: a numeric column is cast before
    /// aggregation, while a text column must be compared and sorted under
    /// ``COLLATE "C"`` — SPARQL orders simple literals by Unicode codepoint,
    /// which the database's default collation does not.
    ///
    /// Resolved from the datatype the schema derives (so a custom type with
    /// ``typeof: integer`` counts), not from the range's spelling.
    #[getter]
    fn numeric(&self) -> bool {
        self.inner.descriptor.numeric
    }

    /// How this column's stored text becomes an RDF term: ``"iri"``,
    /// ``"literal"`` or ``"enum_iri"``.
    ///
    /// An unrecognised value must be rejected rather than guessed — it means
    /// the analyser knows a term shape this serialiser does not, and guessing
    /// would answer differently from the oxigraph route.
    #[getter]
    fn term_kind(&self) -> &'static str {
        use crate::sparql_terms::TermKind;
        match self.inner.descriptor.kind {
            TermKind::Iri => "iri",
            TermKind::Literal => "literal",
            TermKind::EnumIri => "enum_iri",
        }
    }

    /// Datatype IRI for a typed literal, or ``None`` for a plain one.
    #[getter]
    fn datatype(&self) -> Option<String> {
        self.inner.descriptor.datatype.clone()
    }

    /// Language tag, mutually exclusive with ``datatype`` per RDF.
    #[getter]
    fn lang(&self) -> Option<String> {
        self.inner.descriptor.lang.clone()
    }

    /// For ``term_kind == "enum_iri"``: stored value → IRI. A value absent
    /// from this map has no ``meaning`` in the schema and stays a literal,
    /// which is what the turtle writer does.
    #[getter]
    fn enum_map(&self) -> HashMap<String, String> {
        self.inner.descriptor.enum_map.iter().cloned().collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "PushdownBinding(var={:?}, slot_path={:?}, term_kind={:?})",
            self.inner.var,
            self.inner.slot_path,
            self.term_kind()
        )
    }
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[pyclass]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[derive(Clone)]
/// One aggregate in the SELECT list.
pub struct PushdownMeasure {
    inner: crate::sparql_pushdown::MeasureSpec,
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PushdownMeasure {
    /// The result variable (the ``AS`` name), used to label the column.
    #[getter]
    fn var(&self) -> &str {
        &self.inner.var
    }

    /// One of ``"count"``, ``"sum"``, ``"avg"``, ``"min"``, ``"max"``.
    /// Unknown values MUST be rejected rather than guessed: a new aggregate
    /// appearing here means the Rust side supports something this Python
    /// does not yet render.
    #[getter]
    fn func(&self) -> &str {
        use crate::sparql_pushdown::Measure;
        match self.inner.func {
            Measure::Count { .. } => "count",
            Measure::Sum { .. } => "sum",
            Measure::Avg { .. } => "avg",
            Measure::Min { .. } => "min",
            Measure::Max { .. } => "max",
        }
    }

    /// Index into ``bindings`` of the aggregated value, or ``None`` for
    /// ``COUNT(*)`` — which counts solutions and has no argument.
    #[getter]
    fn arg(&self) -> Option<usize> {
        use crate::sparql_pushdown::Measure;
        match self.inner.func {
            Measure::Count { arg, .. } => arg,
            Measure::Sum { arg }
            | Measure::Avg { arg }
            | Measure::Min { arg }
            | Measure::Max { arg } => Some(arg),
        }
    }

    /// ``True`` for ``COUNT(DISTINCT ...)``. Always ``False`` for the other
    /// functions, where SPARQL has no DISTINCT form this subset accepts.
    #[getter]
    fn distinct(&self) -> bool {
        use crate::sparql_pushdown::Measure;
        matches!(self.inner.func, Measure::Count { distinct: true, .. })
    }

    fn __repr__(&self) -> String {
        format!(
            "PushdownMeasure(var={:?}, func={:?})",
            self.inner.var,
            self.func()
        )
    }
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[pyclass]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[derive(Clone)]
/// One ``ORDER BY`` term.
///
/// ``kind`` says whether it sorts on a projected value or on an aggregate
/// result, which decides whether the SQL builder can place it inside the
/// binding subquery or must apply it outside the grouping.
pub struct PushdownOrder {
    inner: crate::sparql_pushdown::OrderTerm,
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PushdownOrder {
    /// ``"binding"`` or ``"measure"``.
    #[getter]
    fn kind(&self) -> &str {
        use crate::sparql_pushdown::OrderKey;
        match self.inner.key {
            OrderKey::Binding(_) => "binding",
            OrderKey::Measure(_) => "measure",
        }
    }

    /// Index into ``bindings`` or ``measures``, per ``kind``.
    #[getter]
    fn index(&self) -> usize {
        use crate::sparql_pushdown::OrderKey;
        match self.inner.key {
            OrderKey::Binding(i) | OrderKey::Measure(i) => i,
        }
    }

    /// ``True`` for ``DESC``.
    ///
    /// Note for the renderer: SPARQL sorts unbound *before* every bound value
    /// ascending, where Postgres defaults to NULLS LAST for ASC — so the
    /// generated SQL must say ``NULLS FIRST`` / ``NULLS LAST`` explicitly.
    #[getter]
    fn desc(&self) -> bool {
        self.inner.desc
    }

    fn __repr__(&self) -> String {
        format!(
            "PushdownOrder(kind={:?}, index={}, desc={})",
            self.kind(),
            self.index(),
            self.inner.desc
        )
    }
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[pyclass]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[derive(Clone)]
/// What SQL must produce for an eligible query: one row per solution, then
/// grouping on top.
pub struct PushdownSolution {
    inner: crate::sparql_pushdown::SolutionSpec,
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PushdownSolution {
    #[getter]
    fn bindings(&self) -> Vec<PushdownBinding> {
        self.inner
            .bindings
            .iter()
            .map(|b| PushdownBinding { inner: b.clone() })
            .collect()
    }

    /// Indices into ``bindings`` forming the ``GROUP BY``. Empty is legal and
    /// means one row over the whole input — SPARQL returns exactly one
    /// solution for a bare aggregate, where a SQL ``GROUP BY`` over no rows
    /// would return none, so the renderer must omit ``GROUP BY`` entirely.
    #[getter]
    fn group_keys(&self) -> Vec<usize> {
        self.inner.group_keys.clone()
    }

    #[getter]
    fn measures(&self) -> Vec<PushdownMeasure> {
        self.inner
            .measures
            .iter()
            .map(|m| PushdownMeasure { inner: m.clone() })
            .collect()
    }

    #[getter]
    fn order_by(&self) -> Vec<PushdownOrder> {
        self.inner
            .order_by
            .iter()
            .map(|o| PushdownOrder { inner: o.clone() })
            .collect()
    }

    #[getter]
    fn distinct(&self) -> bool {
        self.inner.distinct
    }

    #[getter]
    fn limit(&self) -> Option<usize> {
        self.inner.limit
    }

    #[getter]
    fn offset(&self) -> usize {
        self.inner.offset
    }

    /// The variables the query asks for, in ``SELECT`` order.
    ///
    /// A measure or binding absent from this list is machinery: a value grouped
    /// by but not selected, or an aggregate that exists only to order by — the
    /// latter has no ``AS`` name, so emitting it would produce a column named
    /// after an internal identifier.
    #[getter]
    fn projected(&self) -> Vec<String> {
        self.inner.projected.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "PushdownSolution(bindings={}, group_keys={:?}, measures={})",
            self.inner.bindings.len(),
            self.inner.group_keys,
            self.inner.measures.len()
        )
    }
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[pyclass]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[derive(Clone)]
/// Verdict on whether a query's grouping and aggregation can be answered in
/// SQL.
///
/// Three-way on purpose. ``"not_applicable"`` (not an aggregate at all) and
/// ``"blocked"`` (an aggregate outside the supported subset) must not collapse
/// into one falsy value: the first keeps the existing route silently, the
/// second is reportable to whoever wrote the query.
pub struct PushdownVerdict {
    inner: crate::sparql_pushdown::Pushdown,
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[cfg_attr(feature = "stubgen", gen_stub_pymethods)]
#[pymethods]
impl PushdownVerdict {
    /// ``"not_applicable"``, ``"blocked"`` or ``"eligible"``.
    #[getter]
    fn kind(&self) -> &str {
        use crate::sparql_pushdown::Pushdown;
        match &self.inner {
            Pushdown::NotApplicable => "not_applicable",
            Pushdown::Blocked(_) => "blocked",
            Pushdown::Eligible { .. } => "eligible",
        }
    }

    /// Stable machine-readable refusal code, or ``None`` when not blocked.
    /// Branch on this, never on ``detail``.
    #[getter]
    fn code(&self) -> Option<&'static str> {
        use crate::sparql_pushdown::Pushdown;
        match &self.inner {
            Pushdown::Blocked(b) => Some(b.code.as_str()),
            _ => None,
        }
    }

    /// What blocked, in terms of the query and the data model.
    #[getter]
    fn detail(&self) -> Option<String> {
        use crate::sparql_pushdown::Pushdown;
        match &self.inner {
            Pushdown::Blocked(b) => Some(b.detail.clone()),
            _ => None,
        }
    }

    /// Where in the query, when locatable — a variable or an operator.
    #[getter]
    fn at(&self) -> Option<String> {
        use crate::sparql_pushdown::Pushdown;
        match &self.inner {
            Pushdown::Blocked(b) => b.at.clone(),
            _ => None,
        }
    }

    /// A supported shape to use instead. This is the field that turns a
    /// refusal into a repair, so it is meant to be shown verbatim.
    #[getter]
    fn instead(&self) -> Option<&'static str> {
        use crate::sparql_pushdown::Pushdown;
        match &self.inner {
            Pushdown::Blocked(b) => Some(b.instead()),
            _ => None,
        }
    }

    /// The plan the verdict was derived from, when ``kind == "eligible"``.
    ///
    /// A consumer needs **both**: the solution says what to project, group and
    /// aggregate, and the plan says which rows — the classes, their filters and
    /// the join edges. Reading only the solution silently drops every
    /// constraint: ``FILTER(?l > 5)`` on a ``COUNT(*)`` yields a solution with
    /// no bindings, and a bare aggregate's solution does not even name the
    /// class.
    ///
    /// Use this rather than calling ``sparql_scope`` again — a second call
    /// re-parses and could in principle disagree with the one behind the
    /// verdict.
    #[getter]
    fn plan(&self) -> Option<QueryPlan> {
        use crate::sparql_pushdown::Pushdown;
        match &self.inner {
            Pushdown::Eligible { plan, .. } => Some(QueryPlan {
                root: PlanNode {
                    inner: plan.root.clone(),
                },
                sql_limit: plan.sql_limit,
                path_bindings: plan
                    .path_bindings
                    .iter()
                    .map(|(var, binding)| {
                        (
                            var.clone(),
                            (binding.star_var.clone(), binding.slot_path.clone()),
                        )
                    })
                    .collect(),
                exact: plan.inexact.is_none(),
                inexact_reason: plan.inexact.map(|cause| cause.as_str()),
            }),
            _ => None,
        }
    }

    /// The solution spec when ``kind == "eligible"``, else ``None``.
    #[getter]
    fn solution(&self) -> Option<PushdownSolution> {
        use crate::sparql_pushdown::Pushdown;
        match &self.inner {
            Pushdown::Eligible { solution, .. } => Some(PushdownSolution {
                inner: solution.clone(),
            }),
            _ => None,
        }
    }

    fn __repr__(&self) -> String {
        match self.code() {
            Some(code) => format!("PushdownVerdict(kind={:?}, code={:?})", self.kind(), code),
            None => format!("PushdownVerdict(kind={:?})", self.kind()),
        }
    }
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[pyfunction]
#[pyo3(name = "sparql_pushdown")]
#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
/// Classify a SPARQL query for aggregate pushdown.
///
/// Unlike :func:`sparql_scope`, which decides what to *load* for oxigraph to
/// query, this decides whether the query's grouping and aggregation can be
/// answered by SQL without loading anything — the only shape that works for an
/// aggregate, which by definition touches every object of its class.
///
/// Args:
///     query: SPARQL query string.
///     schema_view: The LinkML schema, for resolving slots and ranges.
///
/// Returns:
///     PushdownVerdict — inspect ``.kind`` first.
///
/// Raises:
///     ValueError: the query does not parse or cannot be scoped at all. A
///         query that parses but cannot be pushed down is a ``"blocked"``
///         verdict, not an exception.
fn py_sparql_pushdown(
    py: Python<'_>,
    query: &str,
    schema_view: Py<PySchemaView>,
) -> PyResult<PushdownVerdict> {
    let bound = schema_view.bind(py);
    let sv_ref = bound.borrow();
    let sv = sv_ref.as_rust();

    match crate::sparql_pushdown::analyse_pushdown(query, sv) {
        Ok(verdict) => Ok(PushdownVerdict { inner: verdict }),
        Err(crate::sparql_scoper::ScopeError::UpdateRejected) => {
            Err(pyo3::exceptions::PyValueError::new_err(
                "SPARQL Update (INSERT/DELETE) is not supported. This endpoint is read-only.",
            ))
        }
        Err(crate::sparql_scoper::ScopeError::ParseError(msg)) => Err(
            pyo3::exceptions::PyValueError::new_err(format!("SPARQL parse error: {msg}")),
        ),
        Err(crate::sparql_scoper::ScopeError::Unscoped(msg)) => Err(
            pyo3::exceptions::PyValueError::new_err(format!("Query is unscoped: {msg}")),
        ),
        Err(crate::sparql_scoper::ScopeError::UnsupportedConstruct(msg)) => Err(
            pyo3::exceptions::PyValueError::new_err(format!("unsupported_construct: {msg}")),
        ),
    }
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[pyfunction]
#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
/// Analyse a SPARQL query and produce a structured fetch plan.
///
/// Decomposes the query into stars (one per ``rdf:type``), detects
/// join edges between stars (reference properties), and collects
/// filter conditions. Python translates the plan to SQL.
///
/// Args:
///     query: SPARQL query string (SELECT, ASK, CONSTRUCT, or DESCRIBE).
///     schema_view: The LinkML schema, used to resolve predicate IRIs to
///         slot names and class IRIs to class names.
///
/// Returns:
///     QueryPlan with stars, joins, and optional sql_limit.
///
/// Raises:
///     ValueError: SPARQL parse error, unscoped query, or SPARQL Update.
fn sparql_scope(py: Python<'_>, query: &str, schema_view: Py<PySchemaView>) -> PyResult<QueryPlan> {
    let bound = schema_view.bind(py);
    let sv_ref = bound.borrow();
    let sv = sv_ref.as_rust();

    match crate::sparql_scoper::sparql_scope(query, sv) {
        Ok(plan) => Ok(QueryPlan {
            root: PlanNode { inner: plan.root },
            sql_limit: plan.sql_limit,
            path_bindings: plan
                .path_bindings
                .into_iter()
                .map(|(var, binding)| (var, (binding.star_var, binding.slot_path)))
                .collect(),
            exact: plan.inexact.is_none(),
            inexact_reason: plan.inexact.map(|cause| cause.as_str()),
        }),
        Err(crate::sparql_scoper::ScopeError::UpdateRejected) => {
            Err(pyo3::exceptions::PyValueError::new_err(
                "SPARQL Update (INSERT/DELETE) is not supported. This endpoint is read-only.",
            ))
        }
        Err(crate::sparql_scoper::ScopeError::ParseError(msg)) => Err(
            pyo3::exceptions::PyValueError::new_err(format!("SPARQL parse error: {msg}")),
        ),
        Err(crate::sparql_scoper::ScopeError::Unscoped(msg)) => Err(
            pyo3::exceptions::PyValueError::new_err(format!("Query is unscoped: {msg}")),
        ),
        Err(crate::sparql_scoper::ScopeError::UnsupportedConstruct(msg)) => Err(
            pyo3::exceptions::PyValueError::new_err(format!("unsupported_construct: {msg}")),
        ),
    }
}

#[cfg(all(feature = "python-bindings", feature = "sparql-endpoint"))]
#[pyfunction]
#[pyo3(signature = (query, instances, schema_view, format="json", max_triples=500_000, max_result_rows=10_000))]
#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
/// Execute a SPARQL query against a list of LinkML instances.
///
/// Converts each instance to RDF, loads into an in-memory store (with
/// pre-loaded schema triples), executes the query, and returns the
/// serialised result.
///
/// Args:
///     query: SPARQL query string.
///     instances: List of LinkMLInstance objects to query against.
///     schema_view: The LinkML schema (for RDF conversion).
///     format: Output format — ``"json"`` for SELECT/ASK (SPARQL JSON Results),
///         ``"turtle"`` for CONSTRUCT/DESCRIBE (N-Triples).
///     max_triples: Maximum triples in the store (default 500,000).
///     max_result_rows: Maximum result rows (default 10,000).
///
/// Returns:
///     JSON string (for SELECT/ASK) or Turtle string (for CONSTRUCT/DESCRIBE).
///
/// Raises:
///     RuntimeError: Conversion failure (with object URI), limit exceeded,
///         or query execution error.
fn sparql_execute(
    py: Python<'_>,
    query: &str,
    instances: Vec<Py<PyLinkMLInstance>>,
    schema_view: Py<PySchemaView>,
    format: &str,
    max_triples: usize,
    max_result_rows: usize,
) -> PyResult<String> {
    let bound_sv = schema_view.bind(py);
    let sv_ref = bound_sv.borrow();
    let sv = sv_ref.as_rust();

    // Borrow all instances and collect references
    let bound_instances: Vec<_> = instances.iter().map(|i| i.bind(py).borrow()).collect();
    let instance_refs: Vec<&LinkMLInstance> = bound_instances.iter().map(|b| &b.value).collect();

    match crate::sparql_executor::sparql_execute(
        query,
        &instance_refs,
        sv,
        format,
        crate::sparql_executor::ExecuteLimits {
            max_triples,
            max_result_rows,
        },
    ) {
        Ok(result) => Ok(result),
        Err(crate::sparql_executor::ExecuteError::ConversionError {
            object_uri,
            message,
        }) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "Conversion error for {object_uri}: {message}"
        ))),
        Err(crate::sparql_executor::ExecuteError::TripleLimitExceeded { count, limit }) => {
            Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Triple limit exceeded: {count} > {limit}"
            )))
        }
        Err(crate::sparql_executor::ExecuteError::ResultLimitExceeded { count, limit }) => {
            Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Result row limit exceeded: {count} > {limit}"
            )))
        }
        Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
    }
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
            compute_classes_by_type_designator(&sv, true, true, None, DEFAULT_MANAGED_ANNOTATIONS);
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
                DEFAULT_MANAGED_ANNOTATIONS,
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
