asset360-rust
=============

Python bindings and package wrapper around the Asset360 Rust module.

Develop with maturin
--------------------

- Prerequisites: Python 3.8+, Rust toolchain.
- Create a virtualenv (recommended):
  - `python -m venv .venv && . .venv/bin/activate`
- Install maturin: `pip install -U maturin`
- Build and install in the active environment:
  - `maturin develop`
- Test import:
  - `python -c "import asset360_rust as a; print(a.SchemaView)"`

Build wheels
------------

- `maturin build` (artifacts in `./target/wheels` or `./dist`)

Notes
-----

- The native extension module is named `asset360_native` to avoid symbol
  clashes with the dependency’s own `_native` module. The Python package
  `asset360_rust` re-exports the same API.

Experimental Wasm bindings
--------------------------

- Install the Wasm target once: `rustup target add wasm32-unknown-unknown`.
- Build the crate with the new bindings enabled (Python features off):
  - `cargo build --target wasm32-unknown-unknown --no-default-features --features wasm-bindings`
- Run `wasm-bindgen` (or `wasm-pack`) to emit the `.js` shim and `.d.ts` typings, for example:
  - `wasm-bindgen --target bundler --typescript --out-dir pkg target/wasm32-unknown-unknown/debug/asset360_rust.wasm`
- The generated `pkg/asset360_rust.d.ts` exposes `loadSchemaView`, returning a `SchemaViewHandle` with trivial inspection helpers for now.
