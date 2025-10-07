SHELL := /bin/bash

WASM_BINDGEN_TARGET ?= nodejs
WASM_OUT_DIR ?= pkg
BUILD_WASM_ARGS ?=
NODE_IMAGE ?= node:22

.PHONY: wheel npm test-ts test-py

wheel:
	@bash scripts/build_py.sh $(if $(PYTHON_VERSION),--python-version $(PYTHON_VERSION))

npm:
	@bash scripts/build_wasm.sh --bindgen-target $(WASM_BINDGEN_TARGET) --target-dir $(WASM_OUT_DIR) $(BUILD_WASM_ARGS)

test-ts: npm
	@NODE_IMAGE=$(NODE_IMAGE) bash scripts/test_wasm_ts.sh

test-py:
	@bash scripts/test-python.sh
