#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TS_DIR="${REPO_ROOT}/tests/wasm/ts"
WASM_OUT_DIR="${WASM_OUT_DIR:-pkg}"
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

DOCKER_CMD="set -euo pipefail; if [[ -f package-lock.json ]]; then npm ci --no-audit --no-fund; else npm install --no-audit --no-fund; fi; npm test"

docker run --rm -t \
  -u "$(id -u):$(id -g)" \
  -v "${REPO_ROOT}:/work" \
  -e "WASM_OUT_DIR=${WASM_OUT_DIR}" \
  -w /work/tests/wasm/ts \
  "${NODE_IMAGE}" \
  bash -lc "${DOCKER_CMD}"

rm -rf "${TS_DIR}/node_modules" "${TS_DIR}/dist"
