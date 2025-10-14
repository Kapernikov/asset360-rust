asset360-rust
=============

Python bindings and package wrapper around the Asset360 Rust module.

Install from GitHub release
---------------------------

> ⚠️ Releases are staged entirely on GitHub. The examples below assume you are
> installing version `vX.Y.Z`. Replace the tag and the filename that matches
> your platform/interpreter.

### Python (wheel)

```bash
pip install \
  https://github.com/Kapernikov/asset360-rust/releases/download/vX.Y.Z/asset360_rust-X.Y.Z-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
```

Pick the wheel whose `cpXYZ`/`manylinux` tags line up with your Python version
and OS. The release page lists one wheel per supported interpreter/ABI.

### Node / bundler (npm tarball)

```bash
npm install \
  https://github.com/Kapernikov/asset360-rust/releases/download/vX.Y.Z/asset360-rust-X.Y.Z.tgz
```

You can also reference the tarball in `package.json`:

```json
{
  "dependencies": {
    "asset360-rust": "https://github.com/Kapernikov/asset360-rust/releases/download/vX.Y.Z/asset360-rust-X.Y.Z.tgz"
  }
}
```

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

Makefile targets
----------------

- `make wheel` builds the Python wheel via `scripts/build_py.sh` (optional `PYTHON_VERSION=x.y`).
- `make npm` builds the Wasm bundle under `pkg/`.
- `make test-ts` runs the TypeScript tests, rebuilding the Wasm bundle first.

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
