//! WebAssembly bindings exposed through `wasm-bindgen`.
//! This module currently offers a minimal handle for loading LinkML schemas
//! from YAML text so that higher-level APIs can be layered on gradually.

#![cfg(feature = "wasm-bindings")]

use serde::Serialize;
use serde_wasm_bindgen::to_value;
use wasm_bindgen::prelude::*;

use linkml_meta::SchemaDefinition;
use linkml_schemaview::schemaview::SchemaView;

/// Wrapper around [`SchemaView`] that can be owned from JavaScript.
#[wasm_bindgen]
pub struct SchemaViewHandle {
    inner: SchemaView,
}

#[wasm_bindgen]
impl SchemaViewHandle {
    /// Resolve a schema's identifier if one was declared.
    #[wasm_bindgen(js_name = primarySchemaId)]
    pub fn primary_schema_id(&self) -> Option<String> {
        self.inner.primary_schema().map(|schema| schema.id.clone())
    }

    /// Return the schema definition for the provided identifier.
    #[wasm_bindgen(js_name = schemaDefinition)]
    pub fn schema_definition(&self, schema_id: &str) -> Result<Option<JsValue>, JsValue> {
        option_to_js(self.inner.get_schema_definition(schema_id))
    }

    /// Return the primary schema definition, if one was registered.
    #[wasm_bindgen(js_name = primarySchemaDefinition)]
    pub fn primary_schema_definition(&self) -> Result<Option<JsValue>, JsValue> {
        option_to_js(self.inner.primary_schema())
    }
}

/// Load a [`SchemaView`] from a YAML schema definition.
#[wasm_bindgen(js_name = loadSchemaView)]
pub fn load_schema_view(yaml: &str) -> Result<SchemaViewHandle, JsValue> {
    let schema = parse_schema_definition(yaml)?;
    let mut view = SchemaView::new();
    view.add_schema(schema)
        .map_err(|err| JsValue::from_str(&err))?;
    Ok(SchemaViewHandle { inner: view })
}

fn parse_schema_definition(yaml: &str) -> Result<SchemaDefinition, JsValue> {
    let deserializer = serde_yml::Deserializer::from_str(yaml);
    let schema: SchemaDefinition = serde_path_to_error::deserialize(deserializer)
        .map_err(|err| JsValue::from_str(&err.to_string()))?;
    Ok(schema)
}

fn to_js<T: Serialize>(value: &T) -> Result<JsValue, JsValue> {
    to_value(value).map_err(format_err)
}

fn option_to_js<T>(value: Option<T>) -> Result<Option<JsValue>, JsValue>
where
    T: Serialize,
{
    value.map(|inner| to_js(&inner)).transpose()
}

fn format_err<E: ToString>(err: E) -> JsValue {
    JsValue::from_str(&err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_basic_schema() {
        let yaml = r#"
id: https://example.org/test
name: test
default_prefix: ex
prefixes:
  ex:
    prefix_reference: http://example.org/
classes:
  Person: {}
"#;
        let handle = load_schema_view(yaml).expect("schema loads");
        assert_eq!(
            handle.primary_schema_id().as_deref(),
            Some("https://example.org/test")
        );
        assert!(
            handle
                .primary_schema_definition()
                .expect("primary schema definition")
                .is_some()
        );
        assert!(
            handle
                .schema_definition("https://example.org/test")
                .expect("schema definition")
                .is_some()
        );
        assert!(
            handle
                .schema_definition("missing")
                .expect("missing schema should map to null")
                .is_none()
        );
    }
}
