#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

PROFILE=release
OUT_DIR="pkg"
BINDGEN_TARGET="web"
FEATURES="wasm-bindings"
SCOPE=""
EXTRA_ARGS=()
CARGO_EXTRA_ARGS=()
BINARYEN_VERSION="${BINARYEN_VERSION:-124}"
BINARYEN_BASE_URL="${BINARYEN_BASE_URL:-https://github.com/WebAssembly/binaryen/releases/download}"
BINARYEN_CACHE_DIR="${BINARYEN_CACHE_DIR:-${REPO_ROOT}/.cache/binaryen}"
USE_SYSTEM_BINARYEN="${USE_SYSTEM_BINARYEN:-0}"

usage() {
  cat <<'USAGE'
Usage: build_wasm.sh [options]

Build the wasm32 artifact for asset360-rust using wasm-pack and emit npm-ready JS/TS bindings.

Options:
  --release           Build with --release (default)
  --features <list>   Comma-separated feature list to pass to Cargo (default: wasm-bindings)
  --profile <name>    Cargo profile (debug or release; default debug unless --release)
  --target-dir <dir>  Output directory passed to wasm-pack (default: pkg)
  --bindgen-target <t>  wasm-pack target (default: web)
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
      BINDGEN_TARGET="$2"
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

PACK_ARGS=("build")
PACK_ARGS+=("--target" "$BINDGEN_TARGET")
PACK_ARGS+=("--out-dir" "$OUT_DIR")
PACK_ARGS+=("--no-opt")

if [[ "$PROFILE" == "release" ]]; then
  PACK_ARGS+=("--release")
else
  PACK_ARGS+=("--dev")
fi

if [[ -n "$SCOPE" ]]; then
  PACK_ARGS+=("--scope" "$SCOPE")
fi

CARGO_EXTRA_ARGS=("--no-default-features" "--features" "$FEATURES")

if [[ -n "${CARGO_TARGET_DIR-}" ]]; then
  CARGO_EXTRA_ARGS+=("--target-dir" "$CARGO_TARGET_DIR")
fi

if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
  : "${WASM_BINDGEN_NO_EXTERNREF:=1}"
  WASM_BINDGEN_NO_EXTERNREF="$WASM_BINDGEN_NO_EXTERNREF" wasm-pack "${PACK_ARGS[@]}" "${EXTRA_ARGS[@]}" -- "${CARGO_EXTRA_ARGS[@]}"
else
  : "${WASM_BINDGEN_NO_EXTERNREF:=1}"
  WASM_BINDGEN_NO_EXTERNREF="$WASM_BINDGEN_NO_EXTERNREF" wasm-pack "${PACK_ARGS[@]}" -- "${CARGO_EXTRA_ARGS[@]}"
fi

shopt -s nullglob
WASM_FILES=("$OUT_DIR"/*.wasm)
shopt -u nullglob

if [[ ${#WASM_FILES[@]} -eq 0 ]]; then
  echo "error: wasm-pack did not produce any .wasm artifacts in '$OUT_DIR'" >&2
  exit 1
fi

WASM_OPT_BIN="$(ensure_binaryen)"

for wasm_path in "${WASM_FILES[@]}"; do
  wasm_dir=$(dirname "$wasm_path")
  wasm_file=$(basename "$wasm_path")
  tmp_opt="${wasm_dir}/${wasm_file}.opt"
  "${WASM_OPT_BIN}" -Oz "$wasm_path" -o "$tmp_opt"
  mv "$tmp_opt" "$wasm_path"
done

if command -v zip >/dev/null 2>&1; then
  ZIP_OUTPUT="${OUT_DIR%/}.zip"
  rm -f "$ZIP_OUTPUT"
  zip -qr "$ZIP_OUTPUT" "$OUT_DIR"
else
  echo "warning: zip not found; skipping archive creation" >&2
fi
