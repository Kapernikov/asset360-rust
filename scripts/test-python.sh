#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
VENV_DIR="${ROOT_DIR}/.env"

if [[ ! -d "${VENV_DIR}" ]]; then
    python3 -m venv "${VENV_DIR}"
fi

source "${VENV_DIR}/bin/activate"

python -m pip install --upgrade pip >/dev/null
python -m pip install --quiet maturin pytest >/dev/null

pushd "${ROOT_DIR}" >/dev/null
maturin develop
PYTHONPATH=python python -m pytest python/tests "$@"
popd >/dev/null
