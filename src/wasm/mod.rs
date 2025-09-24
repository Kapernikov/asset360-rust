//! WebAssembly bindings exposed through `wasm-bindgen`.
//! This module currently offers a minimal handle for loading LinkML schemas
//! from YAML text so that higher-level APIs can be layered on gradually.

#![cfg(feature = "wasm-bindings")]

use serde::Serialize;
use serde_wasm_bindgen::to_value;
use wasm_bindgen::prelude::*;

use linkml_meta::SchemaDefinition;
use linkml_schemaview::classview::ClassView;
use linkml_schemaview::enumview::EnumView;
use linkml_schemaview::schemaview::{SchemaView, SchemaViewError};
use linkml_schemaview::slotview::{RangeInfo, SlotContainerMode, SlotInlineMode, SlotView};

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
        self.inner
            .get_schema_definition(schema_id)
            .map(|schema| to_js(schema))
            .transpose()
    }

    /// Return the primary schema definition, if one was registered.
    #[wasm_bindgen(js_name = primarySchemaDefinition)]
    pub fn primary_schema_definition(&self) -> Result<Option<JsValue>, JsValue> {
        self.inner
            .primary_schema()
            .map(|schema| to_js(schema))
            .transpose()
    }

    /// Return every schema identifier loaded in this view.
    #[wasm_bindgen(js_name = schemaIds)]
    pub fn schema_ids(&self) -> Vec<String> {
        self.inner
            .all_schema_definitions()
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Return every class identifier indexed in this view.
    #[wasm_bindgen(js_name = classIds)]
    pub fn class_ids(&self) -> Vec<String> {
        self.inner.get_class_ids()
    }

    /// Return every slot identifier indexed in this view.
    #[wasm_bindgen(js_name = slotIds)]
    pub fn slot_ids(&self) -> Vec<String> {
        self.inner.get_slot_ids()
    }

    /// Return every enum identifier discovered across all schemas.
    #[wasm_bindgen(js_name = enumIds)]
    pub fn enum_ids(&self) -> Vec<String> {
        let mut ids = Vec::new();
        for (_, schema) in self.inner.all_schema_definitions() {
            if let Some(enums) = &schema.enums {
                ids.extend(enums.keys().cloned());
            }
        }
        ids
    }

    /// Retrieve a [`ClassView`] scoped to a specific schema by name.
    #[wasm_bindgen(js_name = classView)]
    pub fn class_view(
        &self,
        schema_id: &str,
        class_name: &str,
    ) -> Result<Option<ClassViewHandle>, JsValue> {
        self.inner
            .get_class_by_schema(schema_id, class_name)
            .map(|opt| opt.map(ClassViewHandle::from_inner))
            .map_err(map_schema_error)
    }

    /// Retrieve a [`SlotView`] scoped to a specific schema by name.
    #[wasm_bindgen(js_name = slotView)]
    pub fn slot_view(
        &self,
        schema_id: &str,
        slot_name: &str,
    ) -> Result<Option<SlotViewHandle>, JsValue> {
        match self.inner.get_schema(schema_id) {
            Some(schema) => {
                let slot_def = schema
                    .slot_definitions
                    .as_ref()
                    .and_then(|defs| defs.get(slot_name));
                if let Some(def) = slot_def {
                    let slot_view = SlotView::new(
                        slot_name.to_string(),
                        vec![def.clone()],
                        schema_id,
                        &self.inner,
                    );
                    Ok(Some(SlotViewHandle::from_inner(slot_view)))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Retrieve an [`EnumView`] scoped to a specific schema by name.
    #[wasm_bindgen(js_name = enumView)]
    pub fn enum_view(
        &self,
        schema_id: &str,
        enum_name: &str,
    ) -> Result<Option<EnumViewHandle>, JsValue> {
        match self.inner.get_schema(schema_id) {
            Some(schema) => {
                let enum_def = schema.enums.as_ref().and_then(|defs| defs.get(enum_name));
                if let Some(def) = enum_def {
                    let enum_view = EnumView::new(def, &self.inner, schema_id);
                    Ok(Some(EnumViewHandle::from_inner(enum_view)))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
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

fn format_err<E: ToString>(err: E) -> JsValue {
    JsValue::from_str(&err.to_string())
}

fn map_schema_error(err: SchemaViewError) -> JsValue {
    format_err(err)
}

#[wasm_bindgen]
pub struct ClassViewHandle {
    inner: ClassView,
}

#[wasm_bindgen]
impl ClassViewHandle {
    fn from_inner(inner: ClassView) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen]
impl ClassViewHandle {
    #[wasm_bindgen(js_name = name)]
    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }

    #[wasm_bindgen(js_name = schemaId)]
    pub fn schema_id(&self) -> String {
        self.inner.schema_id().to_string()
    }

    #[wasm_bindgen(js_name = definition)]
    pub fn definition(&self) -> Result<JsValue, JsValue> {
        to_js(self.inner.def())
    }

    #[wasm_bindgen(js_name = slotViews)]
    pub fn slot_views(&self) -> Vec<SlotViewHandle> {
        self.inner
            .slots()
            .iter()
            .cloned()
            .map(SlotViewHandle::from_inner)
            .collect()
    }

    #[wasm_bindgen(js_name = typeDesignatorSlot)]
    pub fn type_designator_slot(&self) -> Result<Option<JsValue>, JsValue> {
        self.inner
            .get_type_designator_slot()
            .map(|slot| to_js(slot))
            .transpose()
    }

    #[wasm_bindgen(js_name = canonicalIdentifier)]
    pub fn canonical_identifier(&self) -> Result<JsValue, JsValue> {
        to_js(&self.inner.canonical_uri())
    }

    #[wasm_bindgen(js_name = parentClass)]
    pub fn parent_class(&self) -> Result<Option<ClassViewHandle>, JsValue> {
        self.inner
            .parent_class()
            .map(|opt| opt.map(ClassViewHandle::from_inner))
            .map_err(map_schema_error)
    }

    #[wasm_bindgen(js_name = keyOrIdentifierSlot)]
    pub fn key_or_identifier_slot(&self) -> Result<Option<JsValue>, JsValue> {
        self.inner
            .key_or_identifier_slot()
            .cloned()
            .map(|slot| to_js(&slot))
            .transpose()
    }

    #[wasm_bindgen(js_name = identifierSlot)]
    pub fn identifier_slot(&self) -> Result<Option<JsValue>, JsValue> {
        self.inner
            .identifier_slot()
            .cloned()
            .map(|slot| to_js(&slot))
            .transpose()
    }
}

#[wasm_bindgen]
pub struct SlotViewHandle {
    inner: SlotView,
}

impl SlotViewHandle {
    fn from_inner(inner: SlotView) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen]
impl SlotViewHandle {
    #[wasm_bindgen(js_name = name)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    #[wasm_bindgen(js_name = schemaId)]
    pub fn schema_id(&self) -> String {
        self.inner.schema_uri.clone()
    }

    #[wasm_bindgen(js_name = definition)]
    pub fn definition(&self) -> Result<JsValue, JsValue> {
        to_js(self.inner.definition())
    }

    #[wasm_bindgen(js_name = permissibleValueKeys)]
    pub fn permissible_value_keys(&self) -> Result<Vec<String>, JsValue> {
        self.inner
            .permissible_value_keys()
            .map(|keys| keys.clone())
            .map_err(map_schema_error)
    }

    #[wasm_bindgen(js_name = definitions)]
    pub fn definitions(&self) -> Result<JsValue, JsValue> {
        to_js(self.inner.definitions())
    }

    #[wasm_bindgen(js_name = rangeInfos)]
    pub fn range_infos(&self) -> Vec<RangeInfoHandle> {
        self.inner
            .get_range_info()
            .clone()
            .into_iter()
            .map(RangeInfoHandle::from_inner)
            .collect()
    }

    #[wasm_bindgen(js_name = rangeClass)]
    pub fn range_class(&self) -> Option<ClassViewHandle> {
        self.inner
            .get_range_class()
            .map(ClassViewHandle::from_inner)
    }

    #[wasm_bindgen(js_name = rangeEnum)]
    pub fn range_enum(&self) -> Option<EnumViewHandle> {
        self.inner.get_range_enum().map(EnumViewHandle::from_inner)
    }
}

#[wasm_bindgen]
pub struct EnumViewHandle {
    inner: EnumView,
}

impl EnumViewHandle {
    fn from_inner(inner: EnumView) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen]
pub struct RangeInfoHandle {
    inner: RangeInfo,
}

impl RangeInfoHandle {
    fn from_inner(inner: RangeInfo) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen]
impl RangeInfoHandle {
    #[wasm_bindgen(js_name = slotExpression)]
    pub fn slot_expression(&self) -> Result<JsValue, JsValue> {
        to_js(&self.inner.e)
    }

    #[wasm_bindgen(js_name = slotView)]
    pub fn slot_view(&self) -> SlotViewHandle {
        SlotViewHandle::from_inner(self.inner.slotview.clone())
    }

    #[wasm_bindgen(js_name = rangeClass)]
    pub fn range_class(&self) -> Option<ClassViewHandle> {
        self.inner
            .range_class
            .clone()
            .map(ClassViewHandle::from_inner)
    }

    #[wasm_bindgen(js_name = rangeEnum)]
    pub fn range_enum(&self) -> Option<EnumViewHandle> {
        self.inner
            .range_enum
            .clone()
            .map(EnumViewHandle::from_inner)
    }

    #[wasm_bindgen(js_name = isRangeScalar)]
    pub fn is_range_scalar(&self) -> bool {
        self.inner.is_range_scalar
    }

    #[wasm_bindgen(js_name = slotContainerMode)]
    pub fn slot_container_mode(&self) -> String {
        match self.inner.slot_container_mode {
            SlotContainerMode::SingleValue => "single".to_string(),
            SlotContainerMode::Mapping => "mapping".to_string(),
            SlotContainerMode::List => "list".to_string(),
        }
    }

    #[wasm_bindgen(js_name = slotInlineMode)]
    pub fn slot_inline_mode(&self) -> String {
        match self.inner.slot_inline_mode {
            SlotInlineMode::Inline => "inline".to_string(),
            SlotInlineMode::Primitive => "primitive".to_string(),
            SlotInlineMode::Reference => "reference".to_string(),
        }
    }
}

#[wasm_bindgen]
impl EnumViewHandle {
    #[wasm_bindgen(js_name = name)]
    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }

    #[wasm_bindgen(js_name = schemaId)]
    pub fn schema_id(&self) -> String {
        self.inner.schema_id().to_string()
    }

    #[wasm_bindgen(js_name = definition)]
    pub fn definition(&self) -> Result<JsValue, JsValue> {
        to_js(self.inner.definition())
    }

    #[wasm_bindgen(js_name = definitions)]
    pub fn definitions(&self) -> Result<JsValue, JsValue> {
        to_js(self.inner.definitions())
    }

    #[wasm_bindgen(js_name = rangeInfos)]
    pub fn range_infos(&self) -> Vec<RangeInfoHandle> {
        self.inner
            .get_range_info()
            .clone()
            .into_iter()
            .map(RangeInfoHandle::from_inner)
            .collect()
    }

    #[wasm_bindgen(js_name = rangeClass)]
    pub fn range_class(&self) -> Option<ClassViewHandle> {
        self.inner
            .get_range_class()
            .map(ClassViewHandle::from_inner)
    }

    #[wasm_bindgen(js_name = rangeEnum)]
    pub fn range_enum(&self) -> Option<EnumViewHandle> {
        self.inner.get_range_enum().map(EnumViewHandle::from_inner)
    }
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
        assert_eq!(
            handle.schema_ids(),
            vec!["https://example.org/test".to_string()]
        );
        assert_eq!(handle.class_ids().len(), 1);
        assert!(handle.slot_ids().is_empty());
        assert!(handle.enum_ids().is_empty());
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

        let class_handle = handle
            .class_view("https://example.org/test", "Person")
            .expect("class lookup")
            .expect("class exists");
        assert_eq!(class_handle.name(), "Person");
        assert_eq!(class_handle.schema_id(), "https://example.org/test");
        assert!(class_handle.definition().is_ok());
        assert!(class_handle.slot_views().is_empty());

        assert!(
            handle
                .slot_view("https://example.org/test", "unknown")
                .unwrap()
                .is_none()
        );
        assert!(
            handle
                .enum_view("https://example.org/test", "unknown")
                .unwrap()
                .is_none()
        );
    }
}
