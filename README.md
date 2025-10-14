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
pip install "$(
  python - <<'PY'
import json
import sys
import urllib.request

OWNER = "Kapernikov"
REPO = "asset360-rust"

with urllib.request.urlopen(f"https://api.github.com/repos/{OWNER}/{REPO}/releases/latest") as resp:
    release = json.load(resp)

py_tag = f"cp{sys.version_info.major}{sys.version_info.minor}"
abi_tag = py_tag
preferred_platforms = ["manylinux", "musllinux"]

for platform in preferred_platforms:
    for asset in release.get("assets", []):
        name = asset.get("name", "")
        if name.endswith(".whl") and py_tag in name and abi_tag in name and platform in name:
            print(asset["browser_download_url"])
            raise SystemExit

raise SystemExit("No matching wheel found in the latest release.")
PY
)"
```

The script above queries the latest GitHub release, picks the wheel matching the
current interpreter (`cpXY`) and prefers manylinux builds, falling back to
musllinux if needed.

### Node / bundler (npm tarball)

```bash
npm install "$(
  python - <<'PY'
import json
import urllib.request

OWNER = "Kapernikov"
REPO = "asset360-rust"

with urllib.request.urlopen(f"https://api.github.com/repos/{OWNER}/{REPO}/releases/latest") as resp:
    release = json.load(resp)

for asset in release.get("assets", []):
    name = asset.get("name", "")
    if name.endswith(".tgz") and name.startswith("asset360-rust-"):
        print(asset["browser_download_url"])
        raise SystemExit

raise SystemExit("No npm tarball found in the latest release.")
PY
)"
```

This resolves the tarball URL from the latest release and hands it to `npm
install`, so you always grab the newest published package.

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
