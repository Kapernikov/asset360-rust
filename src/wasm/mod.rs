//! WebAssembly bindings exposed through `wasm-bindgen`.
//! This module currently offers a minimal handle for loading LinkML schemas
//! from YAML text so that higher-level APIs can be layered on gradually.

#![cfg(feature = "wasm-bindings")]

use wasm_bindgen::prelude::*;

use linkml_meta::SchemaDefinition;
use linkml_schemaview::schemaview::SchemaView;
use serde_wasm_bindgen::to_value;

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
    pub fn schema_definition(&self, schema_id: &str) -> Result<JsValue, JsValue> {
        let schema: &SchemaDefinition = self
            .inner
            .get_schema_definition(schema_id)
            .ok_or_else(|| JsValue::from_str(&format!("schema '{schema_id}' not found")))?;
        to_value(schema).map_err(|err| JsValue::from_str(&err.to_string()))
    }

    /// Return the primary schema definition, if one was registered.
    #[wasm_bindgen(js_name = primarySchemaDefinition)]
    pub fn primary_schema_definition(&self) -> Result<JsValue, JsValue> {
        let schema = self
            .inner
            .primary_schema()
            .ok_or_else(|| JsValue::from_str("no primary schema registered"))?;
        to_value(schema).map_err(|err| JsValue::from_str(&err.to_string()))
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
        assert!(handle.primary_schema_definition().is_ok());
        assert!(handle.schema_definition("https://example.org/test").is_ok());
        assert!(handle.schema_definition("missing").is_err());
    }
}
