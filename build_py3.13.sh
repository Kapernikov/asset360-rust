#!/bin/bash
cargo run --bin stub_gen
docker run --rm -v $(pwd):/io ghcr.io/pyo3/maturin:latest   build --release --manylinux 2014 --interpreter /opt/python/cp313-cp313/bin/python