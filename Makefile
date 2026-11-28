CARGO_TARGET_DIR ?= /tmp/cowrie-cargo-target
BUILD_DIR        ?= /tmp/cowrie-build
WHEEL_DIR        ?= /tmp/cowrie-dist
LLAMA_ROOT       ?= /home/omen/prism-llama.cpp

.PHONY: build wasm formal test conformance connectors adversarial wheel adapter bench clean

build:
	CARGO_TARGET_DIR=$(CARGO_TARGET_DIR) cargo build --release
	@chmod 0700 $(CARGO_TARGET_DIR) 2>/dev/null || true
	@chmod 0755 $(CARGO_TARGET_DIR)/release/libcowrie_core.so $(CARGO_TARGET_DIR)/release/cowrie 2>/dev/null || true

wasm:
	CARGO_TARGET_DIR=$(CARGO_TARGET_DIR) RUSTFLAGS="-C strip=symbols" cargo build -p cowrie-core --target wasm32-unknown-unknown --release
	node wasm/test_wasm.mjs

connectors: build wasm
	COWRIE_LIB_PATH=$(CARGO_TARGET_DIR)/release/libcowrie_core.so python3 tests/test_ecosystem_connectors.py
	COWRIE_LIB_PATH=$(CARGO_TARGET_DIR)/release/libcowrie_core.so python3 adapters/lancedb/cookbook_lancedb_mcp.py
	node adapters/cloudflare-workers/test_worker.mjs

adversarial: build wasm
	COWRIE_LIB_PATH=$(CARGO_TARGET_DIR)/release/libcowrie_core.so python3 tests/test_adversarial_and_legacy.py
	node wasm/test_adversarial.mjs

wheel: build
	mkdir -p $(WHEEL_DIR) $(BUILD_DIR)/pybuild
	CARGO_TARGET_DIR=$(CARGO_TARGET_DIR) python3 setup.py egg_info --egg-base $(BUILD_DIR)/pybuild build --build-base $(BUILD_DIR)/pybuild bdist_wheel --dist-dir $(WHEEL_DIR) --bdist-dir $(BUILD_DIR)/bdist

formal: build wasm
	CARGO_TARGET_DIR=$(CARGO_TARGET_DIR) COWRIE_LIB_PATH=$(CARGO_TARGET_DIR)/release/libcowrie_core.so python3 formal/verify_formal.py

test:
	CARGO_TARGET_DIR=$(CARGO_TARGET_DIR) cargo test --release

conformance: build
	COWRIE_LIB_PATH=$(CARGO_TARGET_DIR)/release/libcowrie_core.so python3 python/cowrie/conformance.py

adapter: build
	mkdir -p $(BUILD_DIR)
	g++ -O3 -std=c++17 \
		-I$(LLAMA_ROOT)/include -I$(LLAMA_ROOT)/ggml/include \
		-Icrates/cowrie-core/include \
		adapters/llama-statepage/demo_qwen9b_statepage.cpp \
		-L$(LLAMA_ROOT)/build/bin -L$(CARGO_TARGET_DIR)/release \
		-Wl,-rpath,$(LLAMA_ROOT)/build/bin:$(CARGO_TARGET_DIR)/release \
		-lllama -lggml -lcowrie_core \
		-o $(BUILD_DIR)/demo_qwen9b_statepage

bench: build
	mkdir -p $(BUILD_DIR)
	g++ -O3 -std=c++17 \
		-I$(LLAMA_ROOT)/include -I$(LLAMA_ROOT)/ggml/include \
		-Icrates/cowrie-core/include \
		adapters/llama-statepage/bench_shani_kvquant.cpp \
		-L$(LLAMA_ROOT)/build/bin -L$(CARGO_TARGET_DIR)/release \
		-Wl,-rpath,$(LLAMA_ROOT)/build/bin:$(CARGO_TARGET_DIR)/release \
		-lllama -lggml -lcowrie_core \
		-o $(BUILD_DIR)/bench_shani_kvquant

clean:
	rm -rf $(CARGO_TARGET_DIR) $(BUILD_DIR)
