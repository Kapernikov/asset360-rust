#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

PROFILE=release
OUT_DIR="target/wasm/asset360_rust"
BINDGEN_TARGETS=()
FEATURES="wasm-bindings,minijinja-wasm"
SCOPE=""
EXTRA_ARGS=()
CARGO_EXTRA_ARGS=()
BINARYEN_VERSION="${BINARYEN_VERSION:-124}"
BINARYEN_BASE_URL="${BINARYEN_BASE_URL:-https://github.com/WebAssembly/binaryen/releases/download}"
BINARYEN_CACHE_DIR="${BINARYEN_CACHE_DIR:-${REPO_ROOT}/.cache/binaryen}"
USE_SYSTEM_BINARYEN="${USE_SYSTEM_BINARYEN:-0}"

DEFAULT_CARGO_HOME="${REPO_ROOT}/target/wasm/.cargo"
DEFAULT_CARGO_INSTALL_ROOT="${REPO_ROOT}/target/wasm/cargo-install"

if [[ -z "${CARGO_HOME:-}" ]]; then
  export CARGO_HOME="${DEFAULT_CARGO_HOME}"
fi

if [[ -z "${CARGO_INSTALL_ROOT:-}" ]]; then
  export CARGO_INSTALL_ROOT="${DEFAULT_CARGO_INSTALL_ROOT}"
fi

mkdir -p "${CARGO_HOME}" "${CARGO_HOME}/bin" "${CARGO_INSTALL_ROOT}" "${CARGO_INSTALL_ROOT}/bin"
export PATH="${CARGO_INSTALL_ROOT}/bin:${CARGO_HOME}/bin:${PATH}"

usage() {
  cat <<'USAGE'
Usage: build_wasm.sh [options]

Build the wasm32 artifact for asset360-rust using wasm-pack and emit npm-ready JS/TS bindings.

Options:
  --release           Build with --release (default)
  --features <list>   Comma-separated feature list to pass to Cargo (default: wasm-bindings,minijinja-wasm)
  --profile <name>    Cargo profile (debug or release; default debug unless --release)
  --target-dir <dir>  Base output directory for generated packages (default: target/wasm/asset360_rust)
  --bindgen-target <t>  wasm-pack target (repeatable; default: bundler, web, nodejs)
  --scope <scope>     npm scope for the generated package (e.g. kapernikov)
  --extra <args>      Extra arguments to pass directly to wasm-pack (repeatable)
  -h, --help          Show this help message

Environment:
  BINARYEN_VERSION      Binaryen release version (default: 124)
  BINARYEN_BASE_URL     Base URL for Binaryen downloads
  BINARYEN_CACHE_DIR    Directory to cache downloaded Binaryen (default: .cache/binaryen)
  USE_SYSTEM_BINARYEN   Set to 1 to use system wasm-opt if available
USAGE
}

download_binaryen() {
  local archive="binaryen-version_${BINARYEN_VERSION}-x86_64-linux.tar.gz"
  local url="${BINARYEN_BASE_URL}/version_${BINARYEN_VERSION}/${archive}"
  local archive_path="${BINARYEN_CACHE_DIR}/${archive}"

  mkdir -p "${BINARYEN_CACHE_DIR}"

  if [[ ! -f "${archive_path}" ]]; then
    echo "Downloading Binaryen ${BINARYEN_VERSION} from ${url}" >&2
    if command -v curl >/dev/null 2>&1; then
      if ! curl -Lsf -o "${archive_path}" "${url}"; then
        rm -f "${archive_path}"
        echo "Failed to download Binaryen" >&2
        exit 1
      fi
    elif command -v wget >/dev/null 2>&1; then
      if ! wget -q -O "${archive_path}" "${url}"; then
        rm -f "${archive_path}"
        echo "Failed to download Binaryen" >&2
        exit 1
      fi
    else
      echo "curl or wget required to download Binaryen" >&2
      exit 1
    fi
  fi

  echo "${archive_path}"
}

ensure_binaryen() {
  if [[ "${USE_SYSTEM_BINARYEN}" == "1" ]]; then
    if ! command -v wasm-opt >/dev/null 2>&1; then
      echo "USE_SYSTEM_BINARYEN=1 but wasm-opt not found on PATH" >&2
      exit 1
    fi
    command -v wasm-opt
    return
  fi

  local install_dir="${BINARYEN_CACHE_DIR}/binaryen-version_${BINARYEN_VERSION}"
  local wasm_opt_path="${install_dir}/bin/wasm-opt"

  if [[ ! -x "${wasm_opt_path}" ]]; then
    local archive_path
    archive_path="$(download_binaryen)"
    echo "Extracting Binaryen to ${BINARYEN_CACHE_DIR}" >&2
    if ! tar -xf "${archive_path}" -C "${BINARYEN_CACHE_DIR}"; then
      echo "Failed to extract Binaryen" >&2
      exit 1
    fi
  fi

  if [[ ! -x "${wasm_opt_path}" ]]; then
    echo "Binaryen wasm-opt not found after extraction" >&2
    exit 1
  fi

  echo "${wasm_opt_path}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --release)
      PROFILE=release
      shift
      ;;
    --features)
      [[ $# -ge 2 ]] || { echo "--features requires an argument" >&2; exit 1; }
      FEATURES="$2"
      shift 2
      ;;
    --profile)
      [[ $# -ge 2 ]] || { echo "--profile requires an argument" >&2; exit 1; }
      PROFILE="$2"
      shift 2
      ;;
    --bindgen-target)
      [[ $# -ge 2 ]] || { echo "--bindgen-target requires an argument" >&2; exit 1; }
      BINDGEN_TARGETS+=("$2")
      shift 2
      ;;
    --target-dir)
      [[ $# -ge 2 ]] || { echo "--target-dir requires an argument" >&2; exit 1; }
      OUT_DIR="$2"
      shift 2
      ;;
    --scope)
      [[ $# -ge 2 ]] || { echo "--scope requires an argument" >&2; exit 1; }
      SCOPE="$2"
      shift 2
      ;;
    --extra)
      [[ $# -ge 2 ]] || { echo "--extra requires an argument" >&2; exit 1; }
      EXTRA_ARGS+=("$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! command -v wasm-pack >/dev/null 2>&1; then
  echo "wasm-pack not found in PATH. Install it with 'cargo install wasm-pack' or use the official installer." >&2
  exit 1
fi

if [[ "$PROFILE" != "release" && "$PROFILE" != "debug" ]]; then
  echo "Unsupported profile '$PROFILE'. Use --release or omit for debug." >&2
  exit 1
fi

if [[ ${#BINDGEN_TARGETS[@]} -eq 0 ]]; then
  BINDGEN_TARGETS=("bundler" "web" "nodejs")
fi

VALID_TARGETS=("bundler" "web" "nodejs")
for target in "${BINDGEN_TARGETS[@]}"; do
  case "$target" in
    bundler|web|nodejs)
      ;;
    *)
      echo "Unsupported wasm-pack target '$target'. Supported targets: bundler, web, nodejs" >&2
      exit 1
      ;;
  esac
done

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

CARGO_EXTRA_ARGS=("--no-default-features" "--features" "$FEATURES")

if [[ -n "${CARGO_TARGET_DIR-}" ]]; then
  CARGO_EXTRA_ARGS+=("--target-dir" "$CARGO_TARGET_DIR")
fi

WASM_OPT_BIN="$(ensure_binaryen)"

build_target() {
  local target="$1"
  local out_dir="$2"

  mkdir -p "$out_dir"

  local pack_args=("build" "--target" "$target" "--out-dir" "$out_dir" "--no-opt")

  if [[ "$PROFILE" == "release" ]]; then
    pack_args+=("--release")
  else
    pack_args+=("--dev")
  fi

  if [[ -n "$SCOPE" ]]; then
    pack_args+=("--scope" "$SCOPE")
  fi

  : "${WASM_BINDGEN_NO_EXTERNREF:=1}"

  if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
    WASM_BINDGEN_NO_EXTERNREF="$WASM_BINDGEN_NO_EXTERNREF" \
    wasm-pack "${pack_args[@]}" "${EXTRA_ARGS[@]}" -- "${CARGO_EXTRA_ARGS[@]}"
  else
    WASM_BINDGEN_NO_EXTERNREF="$WASM_BINDGEN_NO_EXTERNREF" \
    wasm-pack "${pack_args[@]}" -- "${CARGO_EXTRA_ARGS[@]}"
  fi

  shopt -s nullglob
  local wasm_files=("${out_dir}"/*.wasm)
  shopt -u nullglob

  if [[ ${#wasm_files[@]} -eq 0 ]]; then
    echo "error: wasm-pack did not produce any .wasm artifacts in '$out_dir'" >&2
    exit 1
  fi

  for wasm_path in "${wasm_files[@]}"; do
    local wasm_dir="$(dirname "$wasm_path")"
    local wasm_file="$(basename "$wasm_path")"
    local tmp_opt="${wasm_dir}/${wasm_file}.opt"
    "${WASM_OPT_BIN}" -Oz "$wasm_path" -o "$tmp_opt"
    mv "$tmp_opt" "$wasm_path"
  done
}

for target in "${BINDGEN_TARGETS[@]}"; do
  build_target "$target" "${OUT_DIR}/${target}"
done

find "${OUT_DIR}" -name '.gitignore' -delete

PRIMARY_TARGET="${BINDGEN_TARGETS[0]}"
PRIMARY_DIR="${OUT_DIR}/${PRIMARY_TARGET}"
PRIMARY_PACKAGE_JSON="${PRIMARY_DIR}/package.json"

if [[ ! -f "$PRIMARY_PACKAGE_JSON" ]]; then
  echo "error: expected '$PRIMARY_PACKAGE_JSON' to exist after wasm-pack build" >&2
  exit 1
fi

readarray -t PACKAGE_META < <(python3 - "$PRIMARY_PACKAGE_JSON" <<'PY'
import json
import sys

pkg_path = sys.argv[1]
with open(pkg_path, 'r', encoding='utf-8') as fh:
    data = json.load(fh)

print(data.get('name', 'asset360-rust'))
print(data.get('version', '0.0.0'))
PY
)

PACKAGE_NAME="${PACKAGE_META[0]}"
PACKAGE_VERSION="${PACKAGE_META[1]}"

for doc in README.md LICENSE COPYING; do
  if [[ -f "${PRIMARY_DIR}/${doc}" ]]; then
    cp "${PRIMARY_DIR}/${doc}" "${OUT_DIR}/${doc}"
  fi
done

cat >"${OUT_DIR}/index.mjs" <<'INDEX_MJS'
import initWasm, * as wasmBindings from './web/asset360_rust.js';

// Auto-initialize WASM with configurable path resolution
let initPromise = null;
let wasmPathResolver = null;

// Default resolver tries multiple common locations
async function defaultWasmResolver() {
  // Try 1: Bundler-friendly ?url import (esbuild, Vite, Webpack 5+)
  // This tells bundlers to emit the WASM file as an asset and return its URL
  try {
    const wasmUrlModule = await import('./web/asset360_rust_bg.wasm?url');
    // Return the URL string which can be passed to fetch or used directly
    return wasmUrlModule.default || wasmUrlModule;
  } catch (e) {
    // Not a bundler context or ?url not supported (e.g., Node.js)
    // Fall through to other resolution methods
  }

  // Try 2: import.meta.url relative (works in Node.js with --experimental-wasm-modules)
  const locations = [
    new URL('./web/asset360_rust_bg.wasm', import.meta.url).href,
    // Try 3: Common asset paths for different bundlers (browser only)
    ...(typeof document !== 'undefined' ? [
      new URL('/assets/asset360/asset360_rust_bg.wasm', document.baseURI).href,
      new URL('/web/asset360_rust_bg.wasm', document.baseURI).href,
    ] : [])
  ];

  for (const url of locations) {
    try {
      const resp = await fetch(url, { credentials: 'same-origin' });
      if (resp.ok) return resp;
    } catch (e) {
      // Try next location
    }
  }

  throw new Error(
    `Failed to load WASM from any location: ${locations.join(', ')}. ` +
    `Configure with: import { setWasmPath } from 'asset360-rust'`
  );
}

async function ensureInit() {
  if (initPromise === null) {
    initPromise = (async () => {
      const resolver = wasmPathResolver || defaultWasmResolver;
      const wasmOrUrl = typeof resolver === 'function' ? await resolver() : resolver;
      await initWasm(wasmOrUrl);
    })();
  }
  return initPromise;
}

// Allow users to configure WASM path
export function setWasmPath(pathOrResolver) {
  if (initPromise !== null) {
    throw new Error('WASM already initialized. Call setWasmPath() before any other imports.');
  }
  wasmPathResolver = pathOrResolver;
}

// Export classes that require explicit initialization
export class MiniJinjaEnvironment {
  constructor() {
    if (initPromise === null) {
      throw new Error(
        'WASM not initialized. Call await init() or await ready() before creating MiniJinjaEnvironment'
      );
    }
    this._inner = new wasmBindings.MiniJinjaEnvironment();
  }

  renderStr(template, context) {
    return this._inner.renderStr(template, context);
  }
}

// Export other bindings for advanced use
export { initWasm as init };
export * from './web/asset360_rust.js';

export function ready() {
  return ensureInit();
}

export default ensureInit;
INDEX_MJS

cat >"${OUT_DIR}/index.cjs" <<'INDEX_CJS'
'use strict';

const bindings = require('./nodejs/asset360_rust.js');

let readyPromise = null;

async function init() {
  if (readyPromise === null) {
    readyPromise = Promise.resolve(bindings);
  }
  return readyPromise;
}

function ready() {
  return init();
}

const exported = Object.assign({}, bindings, { init, ready });
exported.default = init;

module.exports = exported;
INDEX_CJS

cat >"${OUT_DIR}/index.d.ts" <<'INDEX_DTS'
// MiniJinjaEnvironment - requires WASM to be initialized first
export declare class MiniJinjaEnvironment {
  constructor();
  renderStr(template: string, context: Record<string, unknown>): string;
}

// Configure WASM path (optional - auto-detects by default)
export declare function setWasmPath(pathOrResolver: string | (() => Promise<Response | string>)): void;

// Re-export all bindings from web build for advanced usage
export * from './web/asset360_rust.js';

// Initialization functions - call before creating MiniJinjaEnvironment
export declare function init(): Promise<void>;
export declare function ready(): Promise<void>;

declare const _default: typeof init;
export default _default;
INDEX_DTS

FILES_ENTRIES=()
for target in "${BINDGEN_TARGETS[@]}"; do
  FILES_ENTRIES+=("\"${target}\"")
done
if [[ -f "${OUT_DIR}/README.md" ]]; then
  FILES_ENTRIES+=("\"README.md\"")
fi
if [[ -f "${OUT_DIR}/LICENSE" ]]; then
  FILES_ENTRIES+=("\"LICENSE\"")
fi
FILES_ENTRIES+=("\"index.mjs\"")
FILES_ENTRIES+=("\"index.cjs\"")
FILES_ENTRIES+=("\"index.d.ts\"")
FILES_JSON="["
for entry in "${FILES_ENTRIES[@]}"; do
  FILES_JSON+="${entry},"
done
FILES_JSON="${FILES_JSON%,}]"

cat >"${OUT_DIR}/package.json" <<PACKAGE_JSON
{
  "name": "${PACKAGE_NAME}",
  "version": "${PACKAGE_VERSION}",
  "files": ${FILES_JSON},
  "main": "./index.cjs",
  "module": "./index.mjs",
  "types": "./index.d.ts",
  "exports": {
    ".": {
      "types": "./index.d.ts",
      "browser": {
        "import": "./index.mjs",
        "require": "./index.cjs"
      },
      "node": "./index.cjs",
      "import": "./index.mjs",
      "require": "./index.cjs",
      "default": "./index.cjs"
    },
    "./node": {
      "types": "./nodejs/asset360_rust.d.ts",
      "default": "./nodejs/asset360_rust.js"
    },
    "./nodejs/asset360_rust.js": {
      "types": "./nodejs/asset360_rust.d.ts",
      "default": "./nodejs/asset360_rust.js"
    },
    "./web": "./web/asset360_rust.js"
  }
}
PACKAGE_JSON

OUT_PARENT="$(dirname "$OUT_DIR")"
mkdir -p "$OUT_PARENT"

if command -v npm >/dev/null 2>&1; then
  PACK_BASENAME=$(printf '%s\n' "${PACKAGE_NAME}-${PACKAGE_VERSION}" | sed 's/[^A-Za-z0-9._-]/_/g')
  if [[ -z "$PACK_BASENAME" ]]; then
    PACK_BASENAME="asset360-rust-${PACKAGE_VERSION}"
  fi
  rm -f "${OUT_PARENT}/${PACK_BASENAME}.tgz"
  PACK_RESULT=$(cd "$OUT_PARENT" && npm pack "./$(basename "$OUT_DIR")")
  if [[ -n "${PACK_RESULT-}" ]]; then
    PACK_FILE="${PACK_RESULT##*$'\n'}"
    if [[ ! -f "${OUT_PARENT}/${PACK_FILE}" ]]; then
      echo "warning: npm pack reported '${PACK_FILE}' but file not found" >&2
    fi
  fi
else
  echo "warning: npm not found; skipping npm pack tarball creation" >&2
fi
