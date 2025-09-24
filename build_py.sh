#!/bin/bash
set -euo pipefail

PYTHON_VERSION="3.13"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python-version)
      [[ $# -ge 2 ]] || { echo "--python-version requires an argument" >&2; exit 1; }
      PYTHON_VERSION="$2"
      shift 2
      ;;
    --help|-h)
      cat <<'USAGE'
Usage: build_py.sh [--python-version <major.minor>]

Build the Python wheel inside the maturin manylinux container.

Options:
  --python-version  CPython version to target (default: 3.13)
USAGE
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

py_tag="cp${PYTHON_VERSION//./}"
interpreter="/opt/python/${py_tag}-${py_tag}/bin/python"

cargo run --bin stub_gen
docker run --rm -v "$(pwd)":/io ghcr.io/pyo3/maturin:latest \
  build --release --strip --manylinux 2014 \
  --interpreter "${interpreter}"
