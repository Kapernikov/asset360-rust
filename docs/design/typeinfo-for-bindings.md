# Type Information for Bindings

## Context
- Several high-level view/definition APIs (e.g., `SlotViewHandle::definition`, class/enum lookups, diff outputs) currently serialise with `serde_wasm_bindgen::to_value`, so the generated TypeScript declarations collapse to `any`.
- `SlotView` owns a fully merged `SlotDefinition`; upstream structs already derive Serde/PyO3 metadata.
- Python bindings usually return concrete `#[pyclass]` wrappers, but any `.into_py(py)` calls degrade stub detail to `Any` unless patched.

## Goals and Non-Goals
- Provide first-class typings for shared schema objects in both Wasm/TypeScript and PyO3/Python bindings.
- Avoid hand-maintaining `.d.ts` or `.pyi` files; drive definitions from the Rust types.
- Keep the existing public APIs intact; consumers still call `SlotView.definition()`.
- Defer long-tail structs until the pattern is proven.

## Baseline Snapshot
- `SlotView` (excerpt): `name: String`, `schema_uri: String`, `data: Arc<SlotViewData>` with cached `SlotDefinition`.
- `SlotDefinition` (excerpt): optional fields like `singular_name`, `domain`, `slot_uri`; already derives `Serialize`, `Deserialize`, `pyclass`, etc.
- `ClassDefinition` (excerpt): includes collections of `SlotDefinition` (e.g., `attributes`, `slot_usage`), so any typing strategy must keep nested definitions in sync.
- Wasm export signature: `pub fn definition(&self) -> Result<JsValue, JsValue>`.
- Python getter signature: `pub fn definition(&self) -> SlotDefinition` (already good).

## Step 1 – Add Cross-Binding Metadata Upstream
- Introduce a `bindings-types` feature in `rust-linkml-core` that brings in `tsify` with `wasm-bindgen` support.
- Update the generator that produces `src/metamodel/src/lib.rs` so every exported struct/enum (e.g., `SlotDefinition`, `ClassDefinition`, `UniqueKey`) receives the new conditional derives while preserving the existing `gen_stub_pyclass`/`pyclass` annotations and helper trait impls that PyO3 relies on; manual edits would be overwritten otherwise.
- Decorate key structs/enums with conditional derives:
  ```rust
  #[cfg_attr(feature = "bindings-types", derive(tsify::Tsify))]
  #[cfg_attr(feature = "bindings-types", tsify(from_wasm_abi, into_wasm_abi))]
  pub struct SlotDefinition {
      pub singular_name: Option<String>,
      pub domain: Option<String>,
      // …
  }
  ```
- Extend the same treatment to nested enums/structs referenced by the public API (`ClassDefinition`, `RangeInfo`, literals, etc.), making sure transitive fields such as `ClassDefinition.attributes: Vec<SlotDefinition>` remain strongly typed end-to-end. Because the derives come from codegen, the annotations stay intact on every regeneration.

### Generator Rules for Field Shapes
The metamodel exposes a limited number of field “shapes”; the generator can stamp the right `tsify` hints for each pattern while leaving the runtime data model untouched.

- **Plain scalars / aliases** (`String`, `bool`, `uriorcurie`, …): no extra attribute required—`tsify` maps Rust primitives and type aliases to their natural TypeScript counterparts.
- **`Option<T>`**: default `tsify` emits optional properties. No additional directive needed unless you want nullable fields; if so, use `#[cfg_attr(feature = "bindings-types", tsify(optional, nullable))]` instead.
- **`Vec<T>`** / **`Option<Vec<T>>`**: generates `T[]` (optionally optional). Ensure `T` is also `Tsify`-derived if it is a custom struct or enum.
- **`HashMap<String, Box<T>>`** (e.g., `ClassDefinition.attributes`/`slot_usage`/`slot_conditions`): surface this as a `Record<string, T>` by emitting `#[cfg_attr(feature = "bindings-types", tsify(type = "Record<string, TType>")])`. The generator can resolve `TType` from `T`’s Rust path, stripping `Box<>`.
- **`HashMap<String, U>` where `U` is already `Tsify`** (e.g., `extensions: HashMap<String, ExtensionOrSubtype>`): same rule as above; reuse the resolved `TType` name.
- **`Box<T>` fields** (e.g., `Vec<Box<AnonymousClassExpression>>`): translate to the underlying `T`. Generator can omit `Box` in the emitted type string because the conversion traits flatten it at the Wasm boundary.
- **`Vec<Box<T>>` / `Option<Vec<Box<T>>>`**: treat as arrays of `T`, mirroring the previous item, with `tsify(type = "TType[]")` when `T` is not primitive.
- **Enums**: ensure the enums themselves derive `Tsify`. For string-literal enums (like `DeltaOp`), add `#[cfg_attr(feature = "bindings-types", tsify(into_wasm_abi, from_wasm_abi))]` so wasm-bindgen can move them across the boundary without extra glue.
- **Newtypes** (tuple structs) generated for specific slots: stamp the derives on the wrapper and rely on existing Serde behaviour; no per-field attributes needed.

The generator logic boils down to: derive `Tsify` everywhere, then emit `tsify(type = ...)` overrides whenever the Rust type would otherwise leak internal wrappers (`Box`, `HashMap`) into the `.d.ts` surface. This keeps code changes declarative and future re-generations deterministic.

### Example: Generated `ClassDefinition`
Once the generator applies the rules above, the emitted struct will look like:

```rust
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[cfg_attr(feature = "pyo3", pyclass(subclass, get_all, set_all))]
#[cfg_attr(feature = "bindings-types", derive(tsify::Tsify))]
#[cfg_attr(feature = "bindings-types", tsify(from_wasm_abi, into_wasm_abi))]
pub struct ClassDefinition {
    #[cfg_attr(feature = "serde", serde(default))]
    pub slots: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(
        feature = "bindings-types",
        tsify(type = "Record<string, SlotDefinition>")
    )]
    pub slot_usage: Option<HashMap<String, Box<SlotDefinition>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_utils::deserialize_inlined_dict_map_optional")
    )]
    #[cfg_attr(
        feature = "bindings-types",
        tsify(type = "Record<string, SlotDefinition>")
    )]
    pub attributes: Option<HashMap<String, Box<SlotDefinition>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub class_uri: Option<uriorcurie>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub union_of: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(
        feature = "bindings-types",
        tsify(type = "Record<string, SlotDefinition>")
    )]
    pub slot_conditions: Option<HashMap<String, Box<SlotDefinition>>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub rules: Option<Vec<Box<ClassRule>>>,
    // …
}
```

Here the generator drops the `Box<>` layer in the TypeScript view, while regular Serde attributes stay untouched. Other fields follow the same pattern—arrays remain arrays, optional scalars remain optional, and nested structs/enums pick up their own `Tsify` derives.

## Step 2 – Register TypeScript Shapes Once
- Provide a helper macro upstream (e.g., `register_types!`) wrapping `tsify::declare::declare_types!`.
- In the wasm crate, add a custom section so the `.d.ts` includes the generated aliases:
  ```rust
  #[wasm_bindgen(typescript_custom_section)]
  const _: &'static str = register_types![SlotDefinition, RangeInfoData /* … */];
  ```
- Run this once per module; downstream crates (like `asset360-rust`) reuse the same helper by enabling the feature.

## Step 3 – Return Concrete Structs from Wasm Exports
- Update `SlotViewHandle::definition` to emit the typed struct:
  ```rust
  #[wasm_bindgen(js_name = definition)]
  pub fn definition(&self) -> SlotDefinition {
      self.inner.definition().clone()
  }
  ```
- Convert other getters from `Result<JsValue, JsValue>` to `Result<ConcreteType, JsValue>` while reusing existing error mapping.
- Ensure the wasm crate depends on `rust-linkml-core` with `features = ["bindings-types"]` so the derives are available.

## Step 4 – Align Python Bindings
- Treat Python exactly the same: every exported property or method should surface a concrete `#[pyclass]`, enum, or primitive instead of a generic `PyObject`.
- For property-style access, keep using `#[getter]`/`#[setter]` methods that return owning Rust values (`SlotDefinition`, `Vec<PySlotView>`, etc.). Cloning into the owned struct is acceptable—the focus is on preserving type identity at the boundary so stub-gen emits accurate annotations.
- For call-style helpers (`fn range_class(&self) -> Option<PyClassView>`), return `Option<PyClassView>`/`Vec<PySlotView>` rather than `PyResult<PyObject>`; PyO3 derives the necessary conversions automatically.
- When a Python-visible enum is required, replicate the `PyDeltaOp` recipe: expose it as a `#[pyclass]`/`#[pyenum]` or implement both `IntoPyObject` and `PyStubType` so stub-gen records a literal union instead of `Any`.
- Identify existing `IntoPyObject` usages (search for `into_pyobject`/`.into_py(py)`) and either remove them or accompany them with an explicit `PyStubType` implementation. This keeps direct property access (`py.slot.definition`) just as ergonomic while preserving type hints in the generated `.pyi` signature.

### Example: Python `ClassView.definition`
The generator already emits a PyO3 wrapper similar to `PySlotView`. After applying the rules, the relevant portion would look like:

```rust
#[pyclass(name = "ClassView")]
pub struct PyClassView {
    inner: ClassView,
}

#[pymethods]
impl PyClassView {
    #[getter]
    pub fn definition(&self) -> ClassDefinition {
        self.inner.definition().clone()
    }

    pub fn attributes(&self) -> Vec<PySlotView> {
        self.inner
            .attributes()
            .into_iter()
            .map(|sv| PySlotView { inner: sv.clone() })
            .collect()
    }

    pub fn slot_usage(&self) -> HashMap<String, SlotDefinition> {
        self.inner
            .slot_usage()
            .iter()
            .map(|(k, v)| (k.clone(), v.as_ref().clone()))
            .collect()
    }
}
```

Stub generation now records `ClassView.definition -> ClassDefinition`, `ClassView.attributes -> list[SlotView]`, and `ClassView.slot_usage -> dict[str, SlotDefinition]`. No raw `PyObject` escapes, so the `.pyi` mirrors the concrete types.

### Example: Python Enums
For enums such as `DeltaOp`, keep the combination of `IntoPyObject` and `PyStubType`:

```rust
#[pyclass(name = "DeltaOp")]
#[derive(Clone, Copy)]
pub enum PyDeltaOp {
    Add,
    Remove,
    Update,
}

impl<'py> IntoPyObject<'py> for PyDeltaOp {
    // returns a Python string, but stub-gen still sees the enum name
}

#[cfg(feature = "stubgen")]
impl PyStubType for PyDeltaOp {
    fn type_output() -> TypeInfo {
        TypeInfo::with_module("typing.Literal['add', 'remove', 'update']", "typing".into())
    }
}
```

This ensures literal unions appear in type hints even though the runtime representation is a string.

## Step 6 – Summary Example
Combining the steps for both bindings, the generator produces:

```rust
// Shared metamodel (autogenerated)
#[cfg_attr(feature = "bindings-types", derive(tsify::Tsify))]
#[cfg_attr(feature = "bindings-types", tsify(from_wasm_abi, into_wasm_abi))]
pub struct ClassDefinition {
    #[cfg_attr(feature = "bindings-types", tsify(type = "Record<string, SlotDefinition>"))]
    pub attributes: Option<HashMap<String, Box<SlotDefinition>>>,
    // …
}

// Wasm wrapper
#[wasm_bindgen]
impl ClassViewHandle {
    #[wasm_bindgen(js_name = definition)]
    pub fn definition(&self) -> ClassDefinition {
        self.inner.definition().clone()
    }

    #[wasm_bindgen(js_name = slotUsage)]
    pub fn slot_usage(&self) -> Option<HashMap<String, SlotDefinition>> {
        self.inner
            .slot_usage()
            .map(|map| map.into_iter().map(|(k, v)| (k, (*v).clone())).collect())
    }
}

#[wasm_bindgen(typescript_custom_section)]
const _: &'static str = register_types![ClassDefinition, SlotDefinition];

// Python wrapper
#[pymethods]
impl PyClassView {
    #[getter]
    pub fn definition(&self) -> ClassDefinition {
        self.inner.definition().clone()
    }

    pub fn slot_usage(&self) -> HashMap<String, SlotDefinition> {
        self.inner
            .slot_usage()
            .iter()
            .map(|(k, v)| (k.clone(), v.as_ref().clone()))
            .collect()
    }
}
```

Remember that the generator must also emit the supporting `IntoPyObject`/`FromPyObject` implementations for boxed types (as it already does today for `Box<AnonymousClassExpression>`). Those impls bridge the gap between `Box<T>` in the data model and PyO3’s expectations, allowing the `slot_usage`/`attributes` code above to clone into bare `SlotDefinition` without breaking the orphan rules.

The shared derive strategy feeds both bindings: wasm gains concrete `.d.ts` signatures, while PyO3’s stub-gen emits precise `.pyi` entries and retains existing `pyclass` behaviour. Downstream packages only need to enable the feature flag and re-export the helpers.

## Step 5 – Wire Up Consumers and Validate
- Enable the new feature in `asset360-rust` dependencies and pull in the upstream helper macro.
- Mirror the wasm signature changes locally (`src/wasm/mod.rs`) so `definition()` returns `SlotDefinition`.
- Regenerate artifacts:
  - `cargo run --bin stub_gen -- --check`
  - `wasm-pack build` (or existing wasm build pipeline)
- Inspect the generated `.pyi` and `.d.ts` to confirm `definition(): SlotDefinition` with structured fields.

## Unsolved Problems

- **Untagged unions remain opaque to TypeScript tooling.** The metamodel relies on constructs such as:

  ```rust
  #[derive(Serialize, Deserialize, Clone)]
  #[serde(untagged)]
  pub enum AnonymousClassExpression {
      SlotUsage(SlotUsageExpression),
      InlineClass(Box<ClassDefinition>),
  }
  ```

  `tsify` rejects `serde(untagged)` enums, so the design currently falls back to `JsValue`/`PyObject`. *Possible mitigation:* introduce binding-specific wrappers that inject an explicit tag while converting from the untagged core type:

  ```rust
  #[cfg_attr(feature = "bindings-types", derive(tsify::Tsify, Serialize, Deserialize))]
  #[cfg_attr(feature = "bindings-types", tsify(from_wasm_abi, into_wasm_abi))]
  #[serde(tag = "kind", rename_all = "camelCase")]
  pub enum BindingAnonymousClassExpression {
      SlotUsage(SlotUsageExpression),
      InlineClass(ClassDefinition),
  }

  impl From<&AnonymousClassExpression> for BindingAnonymousClassExpression {
      fn from(expr: &AnonymousClassExpression) -> Self {
          match expr {
              AnonymousClassExpression::SlotUsage(value) => Self::SlotUsage(value.clone()),
              AnonymousClassExpression::InlineClass(value) => {
                  Self::InlineClass((**value).clone())
              }
          }
      }
  }
  ```

- **Generated `tsify(type = ...)` annotations need stable type names.** The generator still has to map nested paths like `Box<AnonymousClassExpression>` to exported identifiers (and fail fast if the name is missing from the `register_types!` list).

- **Returning concrete structs from wasm hides existing error paths.** Replacing `Result<JsValue, JsValue>` with `ClassDefinition` changes failure handling; we need to confirm these APIs are truly infallible or keep a fallible signature.

- **`register_types!` must be gated to wasm builds.** Without a `cfg(target_arch = "wasm32")`, enabling the feature for Python-only targets drags wasm-only dependencies into every build.

- **Python bindings still deep-clone definitions.** The proposed getters allocate full copies of `SlotDefinition`/`ClassDefinition`; heavier schemas will amplify the cost unless we add borrowed or iterator-based accessors.

- **Feature plumbing across crates is unresolved.** Enabling `bindings-types` in `asset360-rust` requires matching feature propagation through `rust-linkml-core` and any downstream crates; the design does not yet define how cargo features stay in sync.

## Risks and Mitigations
- **Large surface area**: start with high-value structs; introduce lightweight view structs if exposing full metamodel is undesirable.
- **Derive gaps**: some enums may need manual `#[tsify]` annotations; fallback wrappers keep progress unblocked.
- **Binary size**: evaluate `wasm-opt` output after adding type info; remove unused exports if necessary.

## Next Steps Checklist
1. Prototype the `SlotDefinition` flow end-to-end and review generated typings.
2. Extend derives to `ClassDefinition`, `EnumDefinition`, `RangeInfo`, and other frequently returned types.
3. Document the “no raw `JsValue`/`PyObject` in public bindings” guideline for contributors.
