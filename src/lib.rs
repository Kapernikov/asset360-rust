use pyo3::Bound;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use std::collections::HashMap;

use linkml_meta::{Annotation, ClassDefinition};
use linkml_runtime_python::PySchemaView;
use linkml_schemaview::schemaview::SchemaView;

/// Python bindings entrypoint mirroring the dependency's module.
/// Name is different to avoid symbol clashes with the dependency.
#[pymodule(name = "_native2")]
pub fn runtime_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    linkml_runtime_python::runtime_module(m)?;
    m.add_function(wrap_pyfunction!(get_all_classes_by_type_designator_and_schema, m)?)?;
    Ok(())
}

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

#[pyfunction(
    name = "get_all_classes_by_type_designator_and_schema",
    signature = (schemaview=None, only_registered=true, only_default=true)
)]
fn get_all_classes_by_type_designator_and_schema(
    py: Python<'_>,
    schemaview: Option<Py<PySchemaView>>,
    only_registered: bool,
    only_default: bool,
) -> PyResult<HashMap<String, ClassDefinition>> {
    // Use provided schemaview or an empty one
    let sv_arc: std::sync::Arc<SchemaView> = if let Some(py_sv) = schemaview {
        let bound = py_sv.bind(py);
        std::sync::Arc::new(bound.borrow().as_rust().clone())
    } else {
        std::sync::Arc::new(SchemaView::new())
    };

    let sv: &SchemaView = &sv_arc;

    let mut out: HashMap<String, ClassDefinition> = HashMap::new();

    // Restrict to primary schema to mirror imports=False behavior
    let Some(primary) = sv.primary_schema() else {
        return Ok(out);
    };
    let schema_id = primary.id.clone();
    let conv = sv
        .converter_for_schema(&schema_id)
        .unwrap_or_else(|| sv.converter_for_primary_schema().expect("no converter"));

    if let Some(classes) = &primary.classes {
        for (class_name, class_def) in classes.iter() {
            // Filter by managed annotation if requested
            if only_registered {
                let managed_truthy = class_def
                    .annotations
                    .as_ref()
                    .and_then(|m| m.get("data.infrabel.be/asset360/managed"))
                    .map(|ann| is_truthy(py, ann))
                    .unwrap_or(false);
                if !managed_truthy {
                    continue;
                }
            }

            // Build ClassView to discover type designator slot
            let Ok(Some(cv)) = sv.get_class_by_schema(&schema_id, class_name) else {
                continue;
            };
            let Some(td_slot) = cv.get_type_designator_slot() else {
                continue;
            };

            if only_default {
                match cv.get_type_designator_value(td_slot, conv) {
                    Ok(id) => {
                        out.insert(id.to_string(), class_def.clone());
                    }
                    Err(_) => continue,
                }
            } else {
                match cv.get_accepted_type_designator_values(td_slot, conv) {
                    Ok(ids) => {
                        for id in ids {
                            out.insert(id.to_string(), class_def.clone());
                        }
                    }
                    Err(_) => continue,
                }
            }
        }
    }

    Ok(out)
}
