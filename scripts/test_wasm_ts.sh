#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TS_DIR="${REPO_ROOT}/tests/wasm/ts"
WASM_OUT_DIR="${WASM_OUT_DIR:-target/wasm/asset360_rust}"
PKG_DIR="${REPO_ROOT}/${WASM_OUT_DIR}"
NODE_IMAGE="${NODE_IMAGE:-node:20}"

if [[ ! -d "${PKG_DIR}" ]]; then
  echo "error: wasm package dir '${PKG_DIR}' not found. run 'make wasm-build' first." >&2
  exit 1
fi

if [[ ! -d "${TS_DIR}" ]]; then
  echo "error: TypeScript test directory '${TS_DIR}' not found" >&2
  exit 1
fi

PACKAGE_JSON_PATH="${PKG_DIR}/package.json"
if [[ ! -f "${PACKAGE_JSON_PATH}" ]]; then
  echo "error: wasm package.json '${PACKAGE_JSON_PATH}' not found" >&2
  exit 1
fi

readarray -t PKG_META < <(python3 - "$PACKAGE_JSON_PATH" <<'PY'
import json, sys
with open(sys.argv[1], 'r', encoding='utf-8') as fh:
    data = json.load(fh)
print(data.get('name', 'asset360-rust'))
print(data.get('version', '0.0.0'))
PY
)

PKG_NAME="${PKG_META[0]}"
PKG_VERSION="${PKG_META[1]}"

TARBALL_PATH="${REPO_ROOT}/target/wasm/${PKG_NAME}-${PKG_VERSION}.tgz"

if [[ ! -f "${TARBALL_PATH}" ]]; then
  echo "error: npm tarball '${TARBALL_PATH}' not found. run 'scripts/build_wasm.sh' first." >&2
  exit 1
fi

TARBALL_CONTAINER_PATH="/work/target/wasm/${PKG_NAME}-${PKG_VERSION}.tgz"

DOCKER_CMD="set -euo pipefail; if [[ -f package-lock.json ]]; then npm ci --no-audit --no-fund; else npm install --no-audit --no-fund; fi; npm install --no-save --no-audit --no-fund '${TARBALL_CONTAINER_PATH}'; npm test"

docker run --rm -t \
  -u "$(id -u):$(id -g)" \
  -v "${REPO_ROOT}:/work" \
  -e "WASM_OUT_DIR=${WASM_OUT_DIR}" \
  -w /work/tests/wasm/ts \
  "${NODE_IMAGE}" \
  bash -lc "${DOCKER_CMD}"

rm -rf "${TS_DIR}/node_modules" "${TS_DIR}/dist"
