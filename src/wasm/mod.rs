//! WebAssembly bindings exposed through `wasm-bindgen`.
//! This module currently offers a minimal handle for loading LinkML schemas
//! from YAML text so that higher-level APIs can be layered on gradually.

use js_sys::{Array, JSON};
use serde::Serialize;
use serde_wasm_bindgen::to_value;
use wasm_bindgen::prelude::*;

#[cfg(feature = "minijinja-wasm")]
pub mod minijinja;
#[cfg(feature = "minijinja-wasm")]
pub use minijinja::*;

use linkml_meta::SchemaDefinition;
use linkml_runtime::{LinkMLInstance, load_json_str};
use linkml_schemaview::classview::ClassView;
use linkml_schemaview::enumview::EnumView;
use linkml_schemaview::identifier::Identifier;
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
            Some(schema) => to_js(&schema),
            None => Ok(JsValue::NULL),
        }
    }

    /// Return the primary schema definition, if one was registered.
    #[wasm_bindgen(js_name = primarySchemaDefinition)]
    pub fn primary_schema_definition(&self) -> Result<JsValue, JsValue> {
        match self.inner.primary_schema() {
            Some(schema) => to_js(&schema),
            None => Ok(JsValue::NULL),
        }
    }

    /// Serialize this view into snapshot YAML.
    #[wasm_bindgen(js_name = toSnapshotYaml)]
    pub fn to_snapshot_yaml(&self) -> Result<String, JsValue> {
        self.inner.to_snapshot_yaml().map_err(map_schema_error)
    }

    /// Return every schema identifier loaded in this view.
    #[wasm_bindgen(js_name = schemaIds)]
    pub fn schema_ids(&self) -> Vec<String> {
        let mut ids = Vec::new();
        self.inner.with_schema_definitions(|schemas| {
            ids.extend(schemas.keys().cloned());
        });
        ids
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

    /// Add a schema payload that fulfills an unresolved import reference.
    #[wasm_bindgen(js_name = addSchemaStrWithImportRef)]
    pub fn add_schema_str_with_import_ref(
        &mut self,
        data: &str,
        schema_id: &str,
        uri: &str,
    ) -> Result<bool, JsValue> {
        let schema = parse_schema_definition(data)?;
        self.inner
            .add_schema_with_import_ref(schema, Some((schema_id.to_string(), uri.to_string())))
            .map_err(|err| JsValue::from_str(&err))
    }

    /// List unresolved import references as `(schema_id, uri)` tuples.
    #[wasm_bindgen(js_name = getUnresolvedSchemaRefs)]
    pub fn get_unresolved_schema_refs(&self) -> Result<JsValue, JsValue> {
        to_js(&self.inner.get_unresolved_schemas())
    }

    /// Return the resolution URI for a schema identifier, if known.
    #[wasm_bindgen(js_name = getResolutionUriOfSchema)]
    pub fn get_resolution_uri_of_schema(&self, schema_id: &str) -> Option<String> {
        self.inner.get_resolution_uri_of_schema(schema_id)
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
            .and_then(|schema| schema.slot_definitions)
            .and_then(|mut defs| defs.remove(slot_name))
            .map(|def| {
                SlotViewHandle::from_inner_with_schema(
                    SlotView::new(slot_name.to_string(), vec![def], schema_id, &self.inner),
                    schema_id.to_string(),
                )
            })
    }

    /// Retrieve an [`EnumView`] scoped to a specific schema by name.
    #[wasm_bindgen(js_name = enumView)]
    pub fn enum_view(&self, schema_id: &str, enum_name: &str) -> Option<EnumViewHandle> {
        self.inner
            .get_schema(schema_id)
            .and_then(|schema| schema.enums)
            .and_then(|mut defs| defs.remove(enum_name))
            .map(|def| EnumView::new(&def, &self.inner, schema_id))
            .map(EnumViewHandle::from_inner)
    }

    #[wasm_bindgen(js_name = toString)]
    pub fn to_string_js(&self) -> String {
        let schema_count = self.inner.iter_schemas().len();
        let class_count = self.class_ids().len();
        let slot_count = self.slot_ids().len();
        let enum_count = self.enum_ids().len();
        match self.primary_schema_id() {
            Some(primary) => format!(
                "SchemaViewHandle(primary={primary}, schemas={schema_count}, classes={class_count}, slots={slot_count}, enums={enum_count})"
            ),
            None => format!(
                "SchemaViewHandle(schemas={schema_count}, classes={class_count}, slots={slot_count}, enums={enum_count})"
            ),
        }
    }

    /// Create a [`LinkMLInstance`] from JSON text for the given class.
    #[wasm_bindgen(js_name = loadInstanceFromJson)]
    pub fn load_instance_from_json(
        &self,
        class_name: &str,
        json: &str,
    ) -> Result<LinkMLInstanceHandle, JsValue> {
        let converter = self.inner.converter();
        let identifier = Identifier::new(class_name);
        let class_view = self
            .inner
            .get_class(&identifier, &converter)
            .map_err(map_schema_error)?
            .ok_or_else(|| JsValue::from_str(&format!("class `{class_name}` not found")))?;
        let instance = load_json_str(json, &self.inner, &class_view, &converter)
            .map_err(|err| JsValue::from_str(&err.to_string()))?
            .into_instance_tolerate_errors()
            .map_err(|err| JsValue::from_str(&err.to_string()))?;
        Ok(LinkMLInstanceHandle::from_inner(instance))
    }

    /// Create a [`LinkMLInstance`] from a JavaScript value for the given class.
    #[wasm_bindgen(js_name = createInstance)]
    pub fn create_instance(
        &self,
        class_name: &str,
        value: JsValue,
    ) -> Result<LinkMLInstanceHandle, JsValue> {
        if let Some(text) = value.as_string() {
            return self.load_instance_from_json(class_name, &text);
        }
        if value.is_undefined() {
            return Err(JsValue::from_str(
                "cannot create LinkMLInstance from undefined value",
            ));
        }
        let json_text: String = JSON::stringify(&value)?.into();
        self.load_instance_from_json(class_name, &json_text)
    }

    /// Retrieve a [`ClassView`] by name or CURIE (without requiring a schema id).
    #[wasm_bindgen(js_name = classViewByName)]
    pub fn class_view_by_name(&self, name: &str) -> Result<Option<ClassViewHandle>, JsValue> {
        let conv = self.inner.converter();
        self.inner
            .get_class(&Identifier::new(name), &conv)
            .map(|opt| opt.map(ClassViewHandle::from_inner))
            .map_err(map_schema_error)
    }

    /// Retrieve a [`ClassView`] by its canonical URI.
    #[wasm_bindgen(js_name = classViewByUri)]
    pub fn class_view_by_uri(&self, uri: &str) -> Result<Option<ClassViewHandle>, JsValue> {
        self.inner
            .get_class_by_uri(uri)
            .map(|opt| opt.map(ClassViewHandle::from_inner))
            .map_err(map_schema_error)
    }

    /// Retrieve a [`SlotView`] by name or CURIE (without requiring a schema id).
    #[wasm_bindgen(js_name = slotViewByName)]
    pub fn slot_view_by_name(&self, name: &str) -> Result<Option<SlotViewHandle>, JsValue> {
        let conv = self.inner.converter();
        self.inner
            .get_slot(&Identifier::new(name), &conv)
            .map(|opt| opt.map(SlotViewHandle::from_inner))
            .map_err(|e| map_schema_error(SchemaViewError::from(e)))
    }

    /// Retrieve a [`SlotView`] by its canonical URI.
    #[wasm_bindgen(js_name = slotViewByUri)]
    pub fn slot_view_by_uri(&self, uri: &str) -> Result<Option<SlotViewHandle>, JsValue> {
        self.inner
            .get_slot_by_uri(uri)
            .map(|opt| opt.map(SlotViewHandle::from_inner))
            .map_err(map_schema_error)
    }

    /// Retrieve an [`EnumView`] by name or CURIE (without requiring a schema id).
    #[wasm_bindgen(js_name = enumViewByName)]
    pub fn enum_view_by_name(&self, name: &str) -> Result<Option<EnumViewHandle>, JsValue> {
        let conv = self.inner.converter();
        self.inner
            .get_enum(&Identifier::new(name), &conv)
            .map(|opt| opt.map(EnumViewHandle::from_inner))
            .map_err(|e| map_schema_error(SchemaViewError::from(e)))
    }

    /// Check whether a class with the given name or CURIE exists.
    #[wasm_bindgen(js_name = existsClass)]
    pub fn exists_class(&self, name: &str) -> Result<bool, JsValue> {
        let conv = self.inner.converter();
        self.inner
            .exists_class(&Identifier::new(name), &conv)
            .map_err(map_schema_error)
    }

    /// Return the tree root class, optionally overridden by name.
    #[wasm_bindgen(js_name = getTreeRoot)]
    pub fn get_tree_root(&self, class_name: Option<String>) -> Option<ClassViewHandle> {
        self.inner
            .get_tree_root_or(class_name.as_deref())
            .map(ClassViewHandle::from_inner)
    }

    /// Resolve a path of slot names starting from a class, returning the
    /// [`SlotView`] handles reachable at the terminal segment.
    #[wasm_bindgen(js_name = slotsForPath)]
    pub fn slots_for_path(
        &self,
        class_id: &str,
        path: JsValue,
    ) -> Result<Vec<SlotViewHandle>, JsValue> {
        let segments: Vec<String> = if path.is_undefined() || path.is_null() {
            Vec::new()
        } else if !Array::is_array(&path) {
            return Err(JsValue::from_str("path must be an array of strings"));
        } else {
            let array = Array::from(&path);
            let mut segs = Vec::with_capacity(array.length() as usize);
            for entry in array.iter() {
                match entry.as_string() {
                    Some(s) => segs.push(s),
                    None => {
                        return Err(JsValue::from_str("path entries must be strings"));
                    }
                }
            }
            segs
        };
        let id = Identifier::new(class_id);
        self.inner
            .slots_for_path(&id, segments.iter().map(|s| s.as_str()))
            .map(|slots| slots.into_iter().map(SlotViewHandle::from_inner).collect())
            .map_err(map_schema_error)
    }

    /// Add a schema from a YAML string (without an import reference).
    #[wasm_bindgen(js_name = addSchemaStr)]
    pub fn add_schema_str(&mut self, data: &str) -> Result<bool, JsValue> {
        let schema = parse_schema_definition(data)?;
        self.inner
            .add_schema(schema)
            .map_err(|err| JsValue::from_str(&err))
    }

    /// Return the default prefix for a schema, optionally expanded to a URI.
    #[wasm_bindgen(js_name = getDefaultPrefix)]
    pub fn get_default_prefix(&self, schema_id: &str, expand: bool) -> Option<String> {
        self.inner.get_default_prefix_for_schema(schema_id, expand)
    }

    /// Check whether two views reference the same underlying schema data.
    #[wasm_bindgen(js_name = isSame)]
    pub fn is_same(&self, other: &SchemaViewHandle) -> bool {
        self.inner.is_same(&other.inner)
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

/// Load a [`SchemaView`] from snapshot YAML.
#[wasm_bindgen(js_name = loadSchemaViewFromSnapshot)]
pub fn load_schema_view_from_snapshot(yaml: &str) -> Result<SchemaViewHandle, JsValue> {
    let view = SchemaView::from_snapshot_yaml(yaml).map_err(map_schema_error)?;
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

    /// Returns classes that inherit from this class.
    #[wasm_bindgen(js_name = descendants)]
    pub fn descendants(
        &self,
        recurse: bool,
        include_mixins: bool,
    ) -> Result<Vec<ClassViewHandle>, JsValue> {
        self.inner
            .get_descendants(recurse, include_mixins)
            .map(|views| views.into_iter().map(ClassViewHandle::from_inner).collect())
            .map_err(map_schema_error)
    }

    #[wasm_bindgen(js_name = toString)]
    pub fn to_string_js(&self) -> String {
        format!(
            "ClassViewHandle(name={}, schema={})",
            self.name(),
            self.schema_id()
        )
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

    /// Returns the canonical URI for this slot.
    #[wasm_bindgen(js_name = canonicalUri)]
    pub fn canonical_uri(&self) -> String {
        self.inner.canonical_uri().to_string()
    }

    /// Returns `true` when the range is a scalar type rather than a class.
    #[wasm_bindgen(js_name = isRangeScalar)]
    pub fn is_range_scalar(&self) -> bool {
        self.inner.is_range_scalar()
    }

    /// Returns the container mode: `"single"`, `"list"`, or `"mapping"`.
    #[wasm_bindgen(js_name = slotContainerMode)]
    pub fn slot_container_mode(&self) -> String {
        match self.inner.determine_slot_container_mode() {
            SlotContainerMode::SingleValue => "single".to_string(),
            SlotContainerMode::List => "list".to_string(),
            SlotContainerMode::Mapping => "mapping".to_string(),
        }
    }

    /// Returns the inline mode: `"inline"`, `"primitive"`, or `"reference"`.
    #[wasm_bindgen(js_name = slotInlineMode)]
    pub fn slot_inline_mode(&self) -> String {
        match self.inner.determine_slot_inline_mode() {
            SlotInlineMode::Inline => "inline".to_string(),
            SlotInlineMode::Primitive => "primitive".to_string(),
            SlotInlineMode::Reference => "reference".to_string(),
        }
    }

    #[wasm_bindgen(js_name = toString)]
    pub fn to_string_js(&self) -> String {
        let schema = self.schema_id().unwrap_or_else(|| "<unknown>".to_string());
        format!("SlotViewHandle(name={}, schema={schema})", self.name())
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

    /// Returns the canonical URI for this enum.
    #[wasm_bindgen(js_name = canonicalUri)]
    pub fn canonical_uri(&self) -> String {
        self.inner.canonical_uri().to_string()
    }

    #[wasm_bindgen(js_name = permissibleValueKeys)]
    pub fn permissible_value_keys(&self) -> Result<Vec<String>, JsValue> {
        self.inner
            .permissible_value_keys()
            .map(|keys| keys.to_vec())
            .map_err(map_schema_error)
    }

    #[wasm_bindgen(js_name = toString)]
    pub fn to_string_js(&self) -> String {
        format!(
            "EnumViewHandle(name={}, schema={})",
            self.name(),
            self.schema_id()
        )
    }
}

#[wasm_bindgen]
pub struct LinkMLInstanceHandle {
    inner: LinkMLInstance,
}

impl LinkMLInstanceHandle {
    fn from_inner(inner: LinkMLInstance) -> Self {
        Self { inner }
    }

    #[cfg(test)]
    fn as_inner(&self) -> &LinkMLInstance {
        &self.inner
    }
}

#[wasm_bindgen]
impl LinkMLInstanceHandle {
    #[wasm_bindgen(js_name = kind)]
    pub fn kind(&self) -> String {
        match &self.inner {
            LinkMLInstance::Scalar { .. } => "scalar".to_string(),
            LinkMLInstance::Null { .. } => "null".to_string(),
            LinkMLInstance::List { .. } => "list".to_string(),
            LinkMLInstance::Mapping { .. } => "mapping".to_string(),
            LinkMLInstance::Object { .. } => "object".to_string(),
        }
    }

    #[wasm_bindgen(js_name = nodeId)]
    pub fn node_id(&self) -> u64 {
        self.inner.node_id()
    }

    #[wasm_bindgen(js_name = slotName)]
    pub fn slot_name(&self) -> Option<String> {
        match &self.inner {
            LinkMLInstance::Scalar { slot, .. }
            | LinkMLInstance::List { slot, .. }
            | LinkMLInstance::Null { slot, .. }
            | LinkMLInstance::Mapping { slot, .. } => Some(slot.name.clone()),
            LinkMLInstance::Object { .. } => None,
        }
    }

    #[wasm_bindgen(js_name = slotView)]
    pub fn slot_view(&self) -> Option<SlotViewHandle> {
        match &self.inner {
            LinkMLInstance::Scalar { slot, .. }
            | LinkMLInstance::List { slot, .. }
            | LinkMLInstance::Null { slot, .. }
            | LinkMLInstance::Mapping { slot, .. } => Some(SlotViewHandle::from_inner_with_schema(
                slot.clone(),
                slot.schema_id().to_string(),
            )),
            LinkMLInstance::Object { .. } => None,
        }
    }

    #[wasm_bindgen(js_name = className)]
    pub fn class_name(&self) -> Option<String> {
        match &self.inner {
            LinkMLInstance::Object { class, .. } => Some(class.def().name.clone()),
            LinkMLInstance::Scalar { class: Some(c), .. }
            | LinkMLInstance::List { class: Some(c), .. }
            | LinkMLInstance::Mapping { class: Some(c), .. }
            | LinkMLInstance::Null { class: Some(c), .. } => Some(c.def().name.clone()),
            _ => None,
        }
    }

    #[wasm_bindgen(js_name = classView)]
    pub fn class_view(&self) -> Option<ClassViewHandle> {
        match &self.inner {
            LinkMLInstance::Object { class, .. } => {
                Some(ClassViewHandle::from_inner(class.clone()))
            }
            LinkMLInstance::Scalar { class: Some(c), .. }
            | LinkMLInstance::List { class: Some(c), .. }
            | LinkMLInstance::Mapping { class: Some(c), .. }
            | LinkMLInstance::Null { class: Some(c), .. } => {
                Some(ClassViewHandle::from_inner(c.clone()))
            }
            _ => None,
        }
    }

    #[wasm_bindgen(js_name = length)]
    pub fn length(&self) -> usize {
        match &self.inner {
            LinkMLInstance::Scalar { .. } | LinkMLInstance::Null { .. } => 0,
            LinkMLInstance::List { values, .. } => values.len(),
            LinkMLInstance::Mapping { values, .. } | LinkMLInstance::Object { values, .. } => {
                values.len()
            }
        }
    }

    #[wasm_bindgen(js_name = keys)]
    pub fn keys(&self) -> Vec<String> {
        match &self.inner {
            LinkMLInstance::Object { values, .. } | LinkMLInstance::Mapping { values, .. } => {
                values.keys().cloned().collect()
            }
            _ => Vec::new(),
        }
    }

    #[wasm_bindgen(js_name = values)]
    pub fn values(&self) -> Vec<LinkMLInstanceHandle> {
        match &self.inner {
            LinkMLInstance::Object { values, .. } | LinkMLInstance::Mapping { values, .. } => {
                values
                    .values()
                    .cloned()
                    .map(LinkMLInstanceHandle::from_inner)
                    .collect()
            }
            LinkMLInstance::List { values, .. } => values
                .iter()
                .cloned()
                .map(LinkMLInstanceHandle::from_inner)
                .collect(),
            _ => Vec::new(),
        }
    }

    #[wasm_bindgen(js_name = get)]
    pub fn get(&self, key: &str) -> Option<LinkMLInstanceHandle> {
        match &self.inner {
            LinkMLInstance::Object { values, .. } | LinkMLInstance::Mapping { values, .. } => {
                values
                    .get(key)
                    .cloned()
                    .map(LinkMLInstanceHandle::from_inner)
            }
            _ => None,
        }
    }

    #[wasm_bindgen(js_name = at)]
    pub fn at(&self, index: usize) -> Option<LinkMLInstanceHandle> {
        match &self.inner {
            LinkMLInstance::List { values, .. } => values
                .get(index)
                .cloned()
                .map(LinkMLInstanceHandle::from_inner),
            _ => None,
        }
    }

    #[wasm_bindgen(js_name = navigate)]
    pub fn navigate(&self, path: JsValue) -> Result<Option<LinkMLInstanceHandle>, JsValue> {
        let segments: Vec<String> = if path.is_undefined() || path.is_null() {
            Vec::new()
        } else {
            if !Array::is_array(&path) {
                return Err(JsValue::from_str("path must be an array"));
            }
            let array = Array::from(&path);
            let mut segs = Vec::with_capacity(array.length() as usize);
            for entry in array.iter() {
                if let Some(seg) = entry.as_string() {
                    segs.push(seg);
                } else if let Some(idx) = entry.as_f64() {
                    if !idx.is_finite() || idx.fract() != 0.0 || idx < 0.0 {
                        return Err(JsValue::from_str(
                            "numeric path segments must be finite, non-negative integers",
                        ));
                    }
                    if idx > (usize::MAX as f64) {
                        return Err(JsValue::from_str("path index out of range"));
                    }
                    segs.push(format!("{}", idx as usize));
                } else {
                    return Err(JsValue::from_str(
                        "path entries must be strings or integers",
                    ));
                }
            }
            segs
        };

        Ok(self
            .inner
            .navigate_path(segments.iter().map(|s| s.as_str()))
            .map(|value| LinkMLInstanceHandle::from_inner(value.clone())))
    }

    #[wasm_bindgen(js_name = scalarValue)]
    pub fn scalar_value(&self) -> Result<JsValue, JsValue> {
        match &self.inner {
            LinkMLInstance::Scalar { value, .. } => to_js(value),
            LinkMLInstance::Null { .. } => Ok(JsValue::NULL),
            _ => Err(JsValue::from_str("value is not a scalar")),
        }
    }

    #[wasm_bindgen(js_name = toPlainJson)]
    pub fn to_plain_json(&self) -> Result<JsValue, JsValue> {
        let json = self.inner.to_json();
        to_js(&json)
    }

    #[wasm_bindgen(js_name = cloneHandle)]
    pub fn clone_handle(&self) -> LinkMLInstanceHandle {
        LinkMLInstanceHandle::from_inner(self.inner.clone())
    }

    #[wasm_bindgen(js_name = toString)]
    pub fn to_string_js(&self) -> String {
        let kind = self.kind();
        let mut details = Vec::new();
        if let Some(class) = self.class_name() {
            details.push(format!("class={class}"));
        }
        if let Some(slot) = self.slot_name() {
            details.push(format!("slot={slot}"));
        }
        details.push(format!("node={}", self.node_id()));
        format!("LinkMLInstanceHandle(kind={kind}, {})", details.join(", "))
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

    #[wasm_bindgen(js_name = toString)]
    pub fn to_string_js(&self) -> String {
        let slot_name = self.inner.slotview.name.clone();
        let schema_id = self.inner.slotview.schema_id().to_string();
        let range_desc = if let Some(class) = &self.inner.range_class {
            format!("class={}", class.name())
        } else if let Some(enum_view) = &self.inner.range_enum {
            format!("enum={}", enum_view.name())
        } else if self.inner.is_range_scalar {
            "scalar".to_string()
        } else {
            "unknown".to_string()
        };
        format!(
            "RangeInfoHandle(slot={}, schema={}, range={range_desc})",
            slot_name, schema_id
        )
    }
}

// ── ConstraintSet WASM bindings ──────────────────────────────────────
// Per decision D2: parse_shacl and scope are server-side only.

/// Handle wrapping a [`ConstraintSet`] for JavaScript usage.
#[wasm_bindgen]
pub struct ConstraintSetHandle {
    inner: crate::constraint_set::ConstraintSet,
}

#[wasm_bindgen]
impl ConstraintSetHandle {
    /// Create a ConstraintSet from a JSON array of ShapeResult objects.
    #[wasm_bindgen(js_name = fromJson)]
    pub fn from_json(json: &str) -> Result<ConstraintSetHandle, JsValue> {
        let inner = crate::constraint_set::ConstraintSet::from_json(json)
            .map_err(|e| JsValue::from_str(&e))?;
        Ok(Self { inner })
    }

    /// Attach a schema view and target class (returns a new handle).
    #[wasm_bindgen(js_name = withSchemaView)]
    pub fn with_schema_view(
        self,
        sv: &SchemaViewHandle,
        target_class: &str,
    ) -> Result<ConstraintSetHandle, JsValue> {
        let new_inner = self
            .inner
            .with_schema_view(&sv.inner, target_class)
            .map_err(|e| JsValue::from_str(&e))?;
        Ok(Self { inner: new_inner })
    }

    /// Forward-evaluate all shapes against object data.
    /// Returns a JS array of violation objects.
    #[wasm_bindgen(js_name = evaluate)]
    pub fn evaluate(&self, object_data_json: &str) -> Result<JsValue, JsValue> {
        let data: serde_json::Value = serde_json::from_str(object_data_json)
            .map_err(|e| JsValue::from_str(&format!("invalid data JSON: {e}")))?;
        let violations = self.inner.evaluate(&data);
        to_js(&violations)
    }

    /// Backward-solve for a target field.
    /// Returns a FieldConstraint JS object, or null if no constraints apply.
    #[wasm_bindgen(js_name = solve)]
    pub fn solve(&self, object_data_json: &str, target_field: &str) -> Result<JsValue, JsValue> {
        let data: serde_json::Value = serde_json::from_str(object_data_json)
            .map_err(|e| JsValue::from_str(&format!("invalid data JSON: {e}")))?;
        match self.inner.solve(&data, target_field) {
            Some(fc) => to_js(&fc),
            None => Ok(JsValue::NULL),
        }
    }

    /// Return all field names referenced by any shape.
    #[wasm_bindgen(js_name = affectedFields)]
    pub fn affected_fields(&self) -> Vec<String> {
        self.inner.affected_fields()
    }

    /// Serialize the shapes to JSON.
    #[wasm_bindgen(js_name = toJson)]
    pub fn to_json(&self) -> Result<String, JsValue> {
        self.inner
            .to_json()
            .map_err(|e| JsValue::from_str(&format!("serialize error: {e}")))
    }

    #[wasm_bindgen(js_name = toString)]
    pub fn to_string_js(&self) -> String {
        format!(
            "ConstraintSetHandle(shapes={}, has_schema={})",
            self.inner.shape_count(),
            self.inner.has_schema()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_arch = "wasm32")]
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

    #[test]
    fn creates_instances_and_navigates() {
        let yaml = r#"
id: https://example.org/test
name: test
default_prefix: ex
prefixes:
  ex:
    prefix_reference: http://example.org/
classes:
  Person:
    slots:
      - name
      - aliases
slots:
  name:
    range: string
  aliases:
    range: string
    multivalued: true
"#;
        let view = load_schema_view(yaml).expect("schema loads");
        let json_data = r#"{"name": "Alice", "aliases": ["Al"]}"#;
        let instance = view
            .load_instance_from_json("Person", json_data)
            .expect("instance loads");
        assert_eq!(instance.kind(), "object");
        assert_eq!(instance.class_name().as_deref(), Some("Person"));

        let mut keys = instance.keys();
        keys.sort();
        assert_eq!(keys, vec!["aliases".to_string(), "name".to_string()]);

        let alias_list = instance.get("aliases").expect("aliases slot");
        assert_eq!(alias_list.kind(), "list");
        assert_eq!(alias_list.length(), 1);
        match alias_list.as_inner() {
            LinkMLInstance::List { values, .. } => {
                assert_eq!(values.len(), 1);
                match &values[0] {
                    LinkMLInstance::Scalar { value, .. } => {
                        assert_eq!(value.as_str(), Some("Al"));
                    }
                    _ => panic!("expected scalar list entry"),
                }
            }
            _ => panic!("expected list variant"),
        }

        let plain = instance.as_inner().to_json();
        assert_eq!(plain["name"], "Alice");
        assert_eq!(plain["aliases"][0], "Al");

        let navigated = instance
            .as_inner()
            .navigate_path(["aliases", "0"])
            .expect("navigate inner");
        match navigated {
            LinkMLInstance::Scalar { value, .. } => {
                assert_eq!(value.as_str(), Some("Al"));
            }
            _ => panic!("expected scalar result"),
        }
    }

    #[test]
    fn roundtrips_snapshot_yaml() {
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
        let view = load_schema_view(yaml).expect("schema loads");
        let snapshot_yaml = view.to_snapshot_yaml().expect("snapshot serializes");

        let restored = load_schema_view_from_snapshot(&snapshot_yaml).expect("snapshot loads");

        let mut original_ids = view.schema_ids();
        original_ids.sort();
        let mut restored_ids = restored.schema_ids();
        restored_ids.sort();
        assert_eq!(restored_ids, original_ids);

        let mut original_classes = view.class_ids();
        original_classes.sort();
        let mut restored_classes = restored.class_ids();
        restored_classes.sort();
        assert_eq!(restored_classes, original_classes);

        assert_eq!(restored.primary_schema_id(), view.primary_schema_id());
    }

    #[test]
    fn to_string_handles_are_informative() {
        let yaml = r#"
id: https://example.org/test
name: test
default_prefix: ex
prefixes:
  ex:
    prefix_reference: http://example.org/
classes:
  Person:
    slots:
      - name
      - aliases
slots:
  name:
    range: string
  aliases:
    range: string
    multivalued: true
"#;
        let view = load_schema_view(yaml).expect("schema loads");

        let view_summary = view.to_string_js();
        assert!(view_summary.contains("SchemaViewHandle"));
        assert!(view_summary.contains("classes=1"));
        assert!(view_summary.contains("slots=2"));

        let class_handle = view
            .class_view("https://example.org/test", "Person")
            .expect("class lookup")
            .expect("class exists");
        assert_eq!(
            class_handle.to_string_js(),
            "ClassViewHandle(name=Person, schema=https://example.org/test)"
        );

        let slot_handles = class_handle.slot_views();
        let mut slot_summaries: Vec<String> = slot_handles
            .iter()
            .map(|slot| slot.to_string_js())
            .collect();
        slot_summaries.sort();
        assert_eq!(
            slot_summaries,
            vec![
                "SlotViewHandle(name=aliases, schema=https://example.org/test)".to_string(),
                "SlotViewHandle(name=name, schema=https://example.org/test)".to_string()
            ]
        );

        let name_slot = slot_handles
            .iter()
            .find(|slot| slot.name() == "name")
            .expect("name slot present");
        let range_summaries: Vec<String> = name_slot
            .range_infos()
            .into_iter()
            .map(|info| info.to_string_js())
            .collect();
        assert!(
            range_summaries
                .iter()
                .any(|summary| summary.contains("range=scalar"))
        );

        let instance = view
            .load_instance_from_json("Person", r#"{"name": "Alice", "aliases": ["Al"]}"#)
            .expect("instance loads");
        let instance_summary = instance.to_string_js();
        assert!(instance_summary.contains("LinkMLInstanceHandle"));
        assert!(instance_summary.contains("kind=object"));
        assert!(instance_summary.contains("class=Person"));
    }
}
