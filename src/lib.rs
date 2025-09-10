use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::Bound;

/// Python bindings entrypoint mirroring the dependency's module.
/// Name is different to avoid symbol clashes with the dependency.
#[pymodule(name = "asset360_native")]
pub fn runtime_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    linkml_runtime_python::runtime_module(m)
}
