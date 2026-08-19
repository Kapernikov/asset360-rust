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

# `stub_gen` builds on the host, so it needs rustc >= 1.85 (edition 2024) and,
# via `pyo3-stub-gen`, CPython >= 3.10. Check up front: a late stub-build
# failure (or a silently skipped one) ships stale .pyi files in the wheel.
rustc_version="$(rustc --version | awk '{print $2}')"
if [[ "$(printf '%s\n' 1.85.0 "$rustc_version" | sort -V | head -1)" != 1.85.0 ]]; then
  echo "ERROR: rustc >= 1.85 required, found ${rustc_version}." >&2
  exit 1
fi
stub_python="${PYO3_PYTHON:-python3}"
if ! "$stub_python" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null; then
  echo "ERROR: CPython >= 3.10 required for stub generation, checked '${stub_python}'." >&2
  echo "       Set PYO3_PYTHON to a suitable interpreter." >&2
  exit 1
fi

cargo run --bin stub_gen
docker run --rm -v "$(pwd)":/io ghcr.io/pyo3/maturin:latest \
  build --release --strip --manylinux 2014 \
  --interpreter "${interpreter}"
