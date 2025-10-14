asset360-rust
=============

Python bindings and package wrapper around the Asset360 Rust module.

Install from GitHub release
---------------------------

### Python (wheel)

```bash
pip install "$(
  curl -sfL https://api.github.com/repos/Kapernikov/asset360-rust/releases/latest |
    jq -r --arg py_tag "cp$(python -c 'import sys; print(sys.version_info.major, sys.version_info.minor, sep="")')" '
      .assets
      | map(select(.name | endswith(".whl")))
      | map(select(.name | test($py_tag)))
      | (map(select(.name | test("manylinux"))) + map(select(.name | test("musllinux"))))
      | first
      | .browser_download_url
    '
)"
```

The command fetches the latest release metadata from GitHub, filters for wheels
matching the active Python `cpXY` tag, prefers manylinux builds, and falls back
to musllinux if that’s all that’s available. Requires `curl`, `jq`, and `python`
on the PATH.

### Node / bundler (npm tarball)

```bash
npm install "$(
  curl -sfL https://api.github.com/repos/Kapernikov/asset360-rust/releases/latest |
    jq -r '
      .assets
      | map(select(.name | startswith("asset360-rust-") and endswith(".tgz")))
      | first
      | .browser_download_url
    '
)"
```

`npm install` receives the tarball URL for the newest published release
automatically. Requires `curl` and `jq`.

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
