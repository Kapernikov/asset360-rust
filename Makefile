SHELL := /bin/bash

WASM_BINDGEN_TARGET ?= nodejs
WASM_OUT_DIR ?= pkg
BUILD_WASM_ARGS ?=
NODE_IMAGE ?= node:22

.PHONY: wasm-build wasm-test wasm-test-only

wasm-build:
	@bash scripts/build_wasm.sh --bindgen-target $(WASM_BINDGEN_TARGET) --target-dir $(WASM_OUT_DIR) $(BUILD_WASM_ARGS)

wasm-test: wasm-build
	@NODE_IMAGE=$(NODE_IMAGE) bash scripts/test_wasm_ts.sh

wasm-test-only:
	@NODE_IMAGE=$(NODE_IMAGE) bash scripts/test_wasm_ts.sh
