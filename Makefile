SHELL := /bin/bash

WASM_BINDGEN_TARGETS ?= bundler web nodejs
WASM_OUT_DIR ?= pkg
BUILD_WASM_ARGS ?=
NODE_IMAGE ?= node:22

ifdef WASM_BINDGEN_TARGET
WASM_BINDGEN_TARGETS := $(WASM_BINDGEN_TARGET)
endif

WASM_BINDGEN_FLAGS := $(if $(strip $(WASM_BINDGEN_TARGETS)),$(foreach target,$(WASM_BINDGEN_TARGETS),--bindgen-target $(target)),)

.PHONY: wheel npm test-ts test-py

wheel:
	@bash scripts/build_py.sh $(if $(PYTHON_VERSION),--python-version $(PYTHON_VERSION))

npm:
	@bash scripts/build_wasm.sh $(WASM_BINDGEN_FLAGS) --target-dir $(WASM_OUT_DIR) $(BUILD_WASM_ARGS)

test-ts: npm
	@NODE_IMAGE=$(NODE_IMAGE) bash scripts/test_wasm_ts.sh

test-py:
	@bash scripts/test-python.sh
