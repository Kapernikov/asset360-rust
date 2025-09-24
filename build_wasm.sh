#!/bin/bash
set -euo pipefail

PROFILE=debug
OUT_DIR="pkg"
BINDGEN_TARGET="bundler"
FEATURES="wasm-bindings"
SCOPE=""
EXTRA_ARGS=()
CARGO_EXTRA_ARGS=()

usage() {
  cat <<'USAGE'
Usage: build_wasm.sh [options]

Build the wasm32 artifact for asset360-rust using wasm-pack and emit npm-ready JS/TS bindings.

Options:
  --release           Build with --release (default builds debug)
  --features <list>   Comma-separated feature list to pass to Cargo (default: wasm-bindings)
  --profile <name>    Cargo profile (debug or release; default debug unless --release)
  --target-dir <dir>  Output directory passed to wasm-pack (default: pkg)
  --bindgen-target <t>  wasm-pack target (default: bundler)
  --scope <scope>     npm scope for the generated package (e.g. kapernikov)
  --extra <args>      Extra arguments to pass directly to wasm-pack (repeatable)
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
  wasm-pack "${PACK_ARGS[@]}" "${EXTRA_ARGS[@]}" -- "${CARGO_EXTRA_ARGS[@]}"
else
  wasm-pack "${PACK_ARGS[@]}" -- "${CARGO_EXTRA_ARGS[@]}"
fi

if command -v zip >/dev/null 2>&1; then
  ZIP_OUTPUT="${OUT_DIR%/}.zip"
  rm -f "$ZIP_OUTPUT"
  zip -qr "$ZIP_OUTPUT" "$OUT_DIR"
else
  echo "warning: zip not found; skipping archive creation" >&2
fi
