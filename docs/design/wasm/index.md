# Asset360 Wasm/TypeScript Support

This document sketches the long-term Wasm export plan for the Asset360 stack. The goal is to make the Rust implementations usable directly from JavaScript/TypeScript through `wasm-bindgen`, while keeping the existing Python integration intact.

## Design Snapshot

- **Compilation targets** – build the `asset360-rust` crate (and later its LinkML dependencies) for `wasm32-unknown-unknown`, producing lightweight `.wasm` artifacts that can be consumed in browsers and Node runtimes.
- **Feature gating** – keep Python (`pyo3`) bindings behind a `python-bindings` feature and bundle Wasm-specific code under a `wasm-bindings` feature so both stacks stay isolated and optional.
- **API surface** – start with YAML schema ingestion (`loadSchemaView`) and progressively mirror the `SchemaView`, runtime, diff/patch, and CLI capabilities once the upstream crates expose Wasm-friendly wrappers.
- **TypeScript usability** – rely on `wasm-bindgen` (or `wasm-pack`) to emit `.d.ts` definitions, complementing the generated JS shim with hand-authored docs/snippets as the API grows.
- **Binding strategy** – default to `serde_wasm_bindgen` conversions so existing codegen remains untouched; revisit generated field getters only if real-world benchmarks show the eager serialization cost is unacceptable.
- **Build tooling** – `build_wasm.sh` now encapsulates the cargo build, runs `wasm-bindgen`, and, when Binaryen is available, shrinks the artifact with `wasm-opt -Oz` (current release build ~2.9 MiB).
- **Testing strategy** – keep Rust unit tests for core logic and add JavaScript-based smoke tests (via `wasm-bindgen-test` or Vitest) once the exported API expands beyond schema loading.

## TODO Backlog

### Current crate (`asset360-rust`)
- [x] Provide a reproducible wasm build via `build_wasm.sh` (includes `wasm-opt` when available and emits JS/TS bindings).
- [ ] Flesh out `SchemaViewHandle` to expose essential inspection helpers (class lookup, slot metadata, type designators).
- [ ] Provide an async-friendly loading path for schemas fetched over HTTP (to be wired to upstream resolve support).
- [ ] Add basic JS/TS smoke tests (e.g., `wasm-pack test --headless --chrome`).
- [ ] Document the public Wasm API with examples and versioning guidance.
- [ ] Benchmark the serde-based bridge on large, nested schema payloads; fall back to per-field getters/hybrid exports only if it proves too costly.

### Upstream LinkML crates
- [ ] `linkml_schemaview`: add `wasm-bindgen` wrappers for the `SchemaView` type and dependent structs so that the JS surface can return richer objects without cloning into ad hoc types.
- [ ] `linkml_runtime`: expose validated `LinkMLInstance` handling, diff/patch, and serialization helpers under Wasm feature gates.
- [ ] `linkml_runtime`: add feature gating for heavy RDF dependencies (e.g., `oxrdf`, `sophia`) so wasm builds can opt out of RNG/TLS-heavy stacks (current builds rely on manually disabling defaults).
- [ ] `linkml_meta`: ensure the metamodel types derive/implement the traits needed for cross-boundary serialization (e.g., `wasm_bindgen` compatible `JsValue` conversions).
- [x] Audit feature flags so Python- and Wasm-specific dependencies remain optional and mutually exclusive where necessary (`asset360-rust` consumes LinkML crates with defaults off by default).
- [ ] Evaluate the `curies` dependency footprint; either introduce feature gating for its network stack (`reqwest`, TLS) or replace it with a lighter mapping approach if the wasm surface only needs offline prefix resolution (upstream feature now exists; decide on default).

### Tooling & Packaging
- [ ] Decide on distribution strategy (`wasm-pack` npm package vs. manual bundling) and set up CI outputs.
- [ ] Provide minimal starter examples (browser and Node) that exercise the exported API.
- [ ] Align versioning between the Rust crate, npm package, and the Python package to keep consumers in sync.

This backlog should evolve as each upstream crate gains Wasm support; update the checklist once milestones land.
