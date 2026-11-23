# Cowrie Runtime Adapters

Production adapters connecting the single audited Rust core (`libcowrie_core.so` / `cowrie.h`) to three local AI runtimes:

1. **Eureka Engine (`engine_gpu` + `llb-serve`, branch `feat/cowrie`):**
   - `host/llb_cowrie.h`: Native persistent 16 KB-aligned SHA-NI Merkle `StatePages` over Eureka's hybrid Gated DeltaNet SSM + KV VRAM state (`LLB_COWRIE_STATE_DIR`), surviving cold process restarts with `model_sha`, `kv_quant`, and parallel 4 MiB Merkle verification.
   - `desktop/engine/metal/src/cowrie_route.h`: Native HTTP Agent Memory + Causal Trust Membrane (`/v1/memory/store`, `/v1/memory/triage`, `/v1/memory/lens`, `/v1/trust/gate`, plus `cowrie_memory_handles` & `cowrie_trust_gate` directly inside `/v1/chat/completions`).
2. **llama.cpp (`adapters/llama-statepage/`):**
   - `cowrie_llama_statepage.hpp`: Header-only drop-in adapter for split-precision hybrid `StatePages` (`FP16` / `Q8_0` / `Q4_0` KV + exact recurrent SSM) with strict `ERR_MODEL_MISMATCH`, `ERR_KV_QUANT_MISMATCH`, and `ERR_STATEPAGE_HASH_MISMATCH` guards.
   - `bench_shani_kvquant.cpp`: Verified live on `Qwen3.5-9B-Q4_K_M` (`13,800` tokens: `64.9 ms` cold NVMe verified / `25.8 ms` warm verified `Q4_0` KV, `50.7×–128.9×`, `KL = 3.99e-04`, `33/33` tokens, `5/5` needles).
3. **vLLM (`adapters/vllm/`):**
   - `cowrie_vllm.py`: `CowrieMemoryStore` + `CowrieVllmRuntime` wrapping `vllm.LLM` with `Alpha32 R0` triage, adaptive `R2` lenses, canonical `CoreHash` prefix keying (`enable_prefix_caching=True`), and `BIT_TAINTED` causal tool gating.
