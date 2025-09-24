#!/bin/bash
set -euo pipefail

PROFILE=debug
OUT_DIR="pkg"
TARGET="wasm32-unknown-unknown"
BINDGEN_TARGET="bundler"
FEATURES="wasm-bindings"
EXTRA_ARGS=()

usage() {
  cat <<'USAGE'
Usage: build_wasm.sh [options]

Build the wasm32 artifact for asset360-rust and run wasm-bindgen to emit JS/TS bindings.

Options:
  --release           Build with --release (default builds debug)
  --features <list>   Comma-separated feature list to use (default: wasm-bindings)
  --profile <name>    Cargo profile (default: debug or release when --release is set)
  --target-dir <dir>  Output directory passed to wasm-bindgen (default: pkg)
  --bindgen-target <t>  wasm-bindgen target (default: bundler)
  --extra <args>      Extra arguments to pass to wasm-bindgen (repeatable)
  -h, --help          Show this help message
USAGE
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
    --target-dir)
      [[ $# -ge 2 ]] || { echo "--target-dir requires an argument" >&2; exit 1; }
      OUT_DIR="$2"
      shift 2
      ;;
    --bindgen-target)
      [[ $# -ge 2 ]] || { echo "--bindgen-target requires an argument" >&2; exit 1; }
      BINDGEN_TARGET="$2"
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

if ! command -v wasm-bindgen >/dev/null 2>&1; then
  echo "wasm-bindgen not found in PATH. Install it with 'cargo install wasm-bindgen-cli' or use wasm-pack." >&2
  exit 1
fi

BUILD_FLAGS=("--target" "$TARGET" "--no-default-features" "--features" "$FEATURES")
if [[ "$PROFILE" == "release" ]]; then
  BUILD_FLAGS+=("--release")
fi

if [[ -n "${CARGO_TARGET_DIR-}" ]]; then
  cargo_target_dir_opt=("--target-dir" "$CARGO_TARGET_DIR")
else
  cargo_target_dir_opt=()
fi

cargo build "${BUILD_FLAGS[@]}" "${cargo_target_dir_opt[@]}"

if [[ -n "${CARGO_TARGET_DIR-}" ]]; then
  ARTIFACT_BASE="$CARGO_TARGET_DIR"
else
  ARTIFACT_BASE="target"
fi

ARTIFACT="${ARTIFACT_BASE}/${TARGET}/${PROFILE}/asset360_rust.wasm"
if [[ ! -f "$ARTIFACT" ]]; then
  echo "Expected wasm artifact not found at $ARTIFACT" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

if command -v wasm-opt >/dev/null 2>&1; then
  OPT_ARTIFACT="${ARTIFACT%.wasm}-opt.wasm"
  wasm-opt -Oz -o "$OPT_ARTIFACT" "$ARTIFACT"
  ARTIFACT="$OPT_ARTIFACT"
else
  echo "warning: wasm-opt not found; skipping size optimization" >&2
fi

wasm-bindgen "$ARTIFACT" \
  --target "$BINDGEN_TARGET" \
  --out-dir "$OUT_DIR" \
  --typescript \
  "${EXTRA_ARGS[@]}"
