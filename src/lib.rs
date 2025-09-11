use pyo3::Bound;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use std::collections::HashMap;

use linkml_meta::{Annotation, ClassDefinition};
use linkml_runtime_python::PySchemaView;
use linkml_schemaview::schemaview::SchemaView;

pub mod blame;

/// Python bindings entrypoint mirroring the dependency's module.
/// Name is different to avoid symbol clashes with the dependency.
#[pymodule(name = "_native2")]
pub fn runtime_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    linkml_runtime_python::runtime_module(m)?;
    m.add_function(wrap_pyfunction!(
        get_all_classes_by_type_designator_and_schema,
        m
    )?)?;
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

fn compute_classes_by_type_designator(
    sv: &SchemaView,
    only_registered: bool,
    only_default: bool,
    py: Option<Python<'_>>,
) -> HashMap<String, ClassDefinition> {
    let mut out: HashMap<String, ClassDefinition> = HashMap::new();
    let Some(primary) = sv.primary_schema() else {
        return out;
    };
    let schema_id = primary.id.clone();
    let conv = sv
        .converter_for_schema(&schema_id)
        .unwrap_or_else(|| sv.converter_for_primary_schema().expect("no converter"));

    if let Some(classes) = &primary.classes {
        for (class_name, class_def) in classes.iter() {
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
                    continue;
                }
            }

            let Ok(Some(cv)) = sv.get_class_by_schema(&schema_id, class_name) else {
                continue;
            };
            let Some(td_slot) = cv.get_type_designator_slot() else {
                continue;
            };

            if only_default {
                if let Ok(id) = cv.get_type_designator_value(td_slot, conv) {
                    out.insert(id.to_string(), class_def.clone());
                }
            } else if let Ok(ids) = cv.get_accepted_type_designator_values(td_slot, conv) {
                for id in ids {
                    out.insert(id.to_string(), class_def.clone());
                }
            }
        }
    }
    out
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
    let sv_arc: std::sync::Arc<SchemaView> = if let Some(py_sv) = schemaview {
        let bound = py_sv.bind(py);
        std::sync::Arc::new(bound.borrow().as_rust().clone())
    } else {
        std::sync::Arc::new(SchemaView::new())
    };
    Ok(compute_classes_by_type_designator(
        &sv_arc,
        only_registered,
        only_default,
        Some(py),
    ))
}

#[cfg(test)]
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
