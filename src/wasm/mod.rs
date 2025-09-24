//! WebAssembly bindings exposed through `wasm-bindgen`.
//! This module currently offers a minimal handle for loading LinkML schemas
//! from YAML text so that higher-level APIs can be layered on gradually.

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
    pub fn schema_definition(&self, schema_id: &str) -> Result<JsValue, JsValue> {
        match self.inner.get_schema_definition(schema_id) {
            Some(schema) => to_js(schema),
            None => Ok(JsValue::NULL),
        }
    }

    /// Return the primary schema definition, if one was registered.
    #[wasm_bindgen(js_name = primarySchemaDefinition)]
    pub fn primary_schema_definition(&self) -> Result<JsValue, JsValue> {
        match self.inner.primary_schema() {
            Some(schema) => to_js(schema),
            None => Ok(JsValue::NULL),
        }
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
        let mut ids = Vec::new();
        for (_, schema) in self.inner.iter_schemas() {
            if let Some(classes) = &schema.classes {
                ids.extend(classes.keys().cloned());
            }
        }
        ids
    }

    /// Return every slot identifier indexed in this view.
    #[wasm_bindgen(js_name = slotIds)]
    pub fn slot_ids(&self) -> Vec<String> {
        let mut ids = Vec::new();
        for (_, schema) in self.inner.iter_schemas() {
            if let Some(slots) = &schema.slot_definitions {
                ids.extend(slots.keys().cloned());
            }
        }
        ids
    }

    /// Return every enum identifier discovered across all schemas.
    #[wasm_bindgen(js_name = enumIds)]
    pub fn enum_ids(&self) -> Vec<String> {
        let mut ids = Vec::new();
        for (_, schema) in self.inner.iter_schemas() {
            if let Some(enums) = &schema.enums {
                ids.extend(enums.keys().cloned());
            }
        }
        ids
    }

    /// Return all classes as handles across every schema.
    #[wasm_bindgen(js_name = classViews)]
    pub fn class_views(&self) -> Result<Vec<ClassViewHandle>, JsValue> {
        self.inner
            .class_views()
            .map(|views| views.into_iter().map(ClassViewHandle::from_inner).collect())
            .map_err(map_schema_error)
    }

    /// Return all slots as handles across every schema.
    #[wasm_bindgen(js_name = slotViews)]
    pub fn slot_views(&self) -> Result<Vec<SlotViewHandle>, JsValue> {
        self.inner
            .slot_views()
            .map(|views| views.into_iter().map(SlotViewHandle::from_inner).collect())
            .map_err(map_schema_error)
    }

    /// Return all enums as handles across every schema.
    #[wasm_bindgen(js_name = enumViews)]
    pub fn enum_views(&self) -> Result<Vec<EnumViewHandle>, JsValue> {
        self.inner
            .enum_views()
            .map(|views| views.into_iter().map(EnumViewHandle::from_inner).collect())
            .map_err(map_schema_error)
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
    pub fn slot_view(&self, schema_id: &str, slot_name: &str) -> Option<SlotViewHandle> {
        self.inner
            .get_schema(schema_id)
            .and_then(|schema| schema.slot_definitions.as_ref())
            .and_then(|defs| defs.get(slot_name))
            .map(|def| {
                SlotViewHandle::from_inner_with_schema(
                    SlotView::new(
                        slot_name.to_string(),
                        vec![def.clone()],
                        schema_id,
                        &self.inner,
                    ),
                    schema_id.to_string(),
                )
            })
    }

    /// Retrieve an [`EnumView`] scoped to a specific schema by name.
    #[wasm_bindgen(js_name = enumView)]
    pub fn enum_view(&self, schema_id: &str, enum_name: &str) -> Option<EnumViewHandle> {
        self.inner
            .get_schema(schema_id)
            .and_then(|schema| schema.enums.as_ref())
            .and_then(|defs| defs.get(enum_name))
            .map(|def| EnumView::new(def, &self.inner, schema_id))
            .map(EnumViewHandle::from_inner)
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
    to_value(value).map_err(|err| format_err(&err))
}

fn format_err<E: std::fmt::Debug>(err: E) -> JsValue {
    JsValue::from_str(&format!("{err:?}"))
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
        let schema_id = self.schema_id().to_string();
        self.inner
            .slots()
            .iter()
            .cloned()
            .map(|slot| SlotViewHandle::from_inner_with_schema(slot, schema_id.clone()))
            .collect()
    }

    #[wasm_bindgen(js_name = typeDesignatorSlot)]
    pub fn type_designator_slot(&self) -> Result<JsValue, JsValue> {
        match self.inner.get_type_designator_slot() {
            Some(slot) => to_js(slot),
            None => Ok(JsValue::NULL),
        }
    }

    #[wasm_bindgen(js_name = canonicalIdentifier)]
    pub fn canonical_identifier(&self) -> String {
        self.inner.canonical_uri().to_string()
    }

    #[wasm_bindgen(js_name = parentClass)]
    pub fn parent_class(&self) -> Result<Option<ClassViewHandle>, JsValue> {
        self.inner
            .parent_class()
            .map(|opt| opt.map(ClassViewHandle::from_inner))
            .map_err(map_schema_error)
    }

    #[wasm_bindgen(js_name = keyOrIdentifierSlot)]
    pub fn key_or_identifier_slot(&self) -> Option<SlotViewHandle> {
        let schema_id = self.schema_id().to_string();
        self.inner
            .key_or_identifier_slot()
            .map(|slot| SlotViewHandle::from_inner_with_schema(slot.clone(), schema_id))
    }

    #[wasm_bindgen(js_name = identifierSlot)]
    pub fn identifier_slot(&self) -> Option<SlotViewHandle> {
        let schema_id = self.schema_id().to_string();
        self.inner
            .identifier_slot()
            .map(|slot| SlotViewHandle::from_inner_with_schema(slot.clone(), schema_id))
    }
}

#[wasm_bindgen]
pub struct SlotViewHandle {
    inner: SlotView,
    schema_id: Option<String>,
}

impl SlotViewHandle {
    fn from_inner(inner: SlotView) -> Self {
        Self {
            inner,
            schema_id: None,
        }
    }

    fn from_inner_with_schema(inner: SlotView, schema_id: String) -> Self {
        Self {
            inner,
            schema_id: Some(schema_id),
        }
    }
}

#[wasm_bindgen]
impl SlotViewHandle {
    #[wasm_bindgen(js_name = name)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    #[wasm_bindgen(js_name = schemaId)]
    pub fn schema_id(&self) -> Option<String> {
        self.schema_id
            .clone()
            .or_else(|| Some(self.inner.schema_id().to_string()))
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

    #[wasm_bindgen(js_name = permissibleValueKeys)]
    pub fn permissible_value_keys(&self) -> Result<Vec<String>, JsValue> {
        self.inner
            .permissible_value_keys()
            .map(|keys| keys.to_vec())
            .map_err(map_schema_error)
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
        let schema_id = self.inner.slotview.schema_id().to_string();
        SlotViewHandle::from_inner_with_schema(self.inner.slotview.clone(), schema_id)
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
        assert_eq!(handle.class_views().unwrap().len(), 1);
        assert!(handle.slot_views().unwrap().is_empty());
        assert!(handle.enum_views().unwrap().is_empty());
        let primary_schema = handle
            .primary_schema_definition()
            .expect("primary schema definition");
        assert!(!primary_schema.is_null());
        let schema_def = handle
            .schema_definition("https://example.org/test")
            .expect("schema definition");
        assert!(!schema_def.is_null());
        let missing_schema = handle
            .schema_definition("missing")
            .expect("missing schema should map to null");
        assert!(missing_schema.is_null());

        let class_handle = handle
            .class_view("https://example.org/test", "Person")
            .expect("class lookup")
            .expect("class exists");
        assert_eq!(class_handle.name(), "Person");
        assert_eq!(class_handle.schema_id(), "https://example.org/test");
        assert!(class_handle.definition().is_ok());
        assert!(class_handle.slot_views().is_empty());
        assert!(class_handle.type_designator_slot().unwrap().is_null());
        assert!(!class_handle.canonical_identifier().is_empty());
        assert!(class_handle.parent_class().unwrap().is_none());
        assert!(class_handle.key_or_identifier_slot().is_none());
        assert!(class_handle.identifier_slot().is_none());

        assert!(
            handle
                .slot_view("https://example.org/test", "unknown")
                .is_none()
        );
        assert!(
            handle
                .enum_view("https://example.org/test", "unknown")
                .is_none()
        );
    }
}
