# Cowrie Runtime & WebAssembly Adapters (`v1.2.0`)

Production adapters connecting the single zero-dependency Rust core (`libcowrie_core.so` / `cowrie.h` / `cowrie_core.wasm`) symmetrically across **four local/cloud AI runtimes**, **stdio MCP servers**, and **Browser/Node.js WebAssembly**:

## Symmetric `v1.2.0` Feature Matrix Across All Runtimes
Every runtime adapter below implements the same five core `v1.2` capabilities over `libcowrie_core`:
1. **Longest-Prefix `0600` HMAC `StatePages` (`CWSP`):** Cross-restart KV / hybrid SSM+KV offloading bound to 64-stride `cowrie_model_fingerprint`, `runtime_id`, `kv_quant`, `ssm_dtype`, parallel 4 MiB `SHA-NI`/`ARMv8 SHA2` Merkle leaves, and optional `HMAC-SHA256` (`COWRIE_HMAC_KEY`).
2. **Crash-Safe `.cwwal` Audit & Memory Persistence:** Every stored frame, tombstone revocation, and tool audit event appends to an `0600` SHA-256 hash-chained `.cwwal` write-ahead log (`cowrie_wal_open` / `cowrie_wal_append_frame` / `cowrie_wal_append_tombstone`) with automatic torn-tail recovery and sequential re-insertion replay.
3. **10-Letter 47-Bit `Alpha32` Handles & Breakout-Escaped `R0–R2` Lenses:** Zero `\p{N}` digit splitting, `~12M` birthday bound, fail-closed collision detection, and automatic `<\<<` / `>\>>` fence escaping.
4. **Sticky Causal Taint (`BIT_TAINTED = 0x80`) & Scoped RFC 8032 `Ed25519` Declassification:** Whole-context lineage tracking that blocks direct `0x04` and multi-hop `0x80` tool calls unless accompanied by a valid, non-expired `COWRIE_DECLASSIFY_V1` `Ed25519` signature bound to `(target_cid_34, action_u8, expiry_u64)`.
5. **16 KB `CWZM` Zone-Map & `CWIV` Co-Located Vector Access:** Direct C-ABI access to `0.51 ms` cold NVMe point/range shards and `AVX2`/`NEON` INT8 vector search.

---

## 1. Eureka LLM (`adapters/eureka/llb_cowrie.h` + `cowrie_route.h`)
- **Upstream branches:** `eureka-engine` (`feat/cowrie`, `host/llb_cowrie.h`) and `eureka-linux-parity` (`feat/cowrie`, `desktop/engine/metal/src/cowrie_route.h` + `serve.c`).
- **`llb_cowrie.h` (`engine_gpu`):** Persistent 16 KB-aligned hybrid Gated DeltaNet SSM + KV `StatePages` (`LLB_COWRIE_STATE_DIR`, `0700` dir / `0600` files), 64-stride `cowrie_model_fingerprint`, `runtime_id="eureka-llm"`, `HMAC-SHA256`, and longest-prefix chunk restore (`5.00×` E2E cold process-kill restart on `Qwen3.5-9B`).
- **`cowrie_route.h` (`llb-serve`):** Native HTTP Agent Memory + `.cwwal` WAL + `Ed25519` Trust Gate (`/v1/memory/store`, `/v1/memory/triage`, `/v1/memory/lens`, `/v1/trust/gate`, plus inline `cowrie_memory_handles` & `cowrie_declassify` inside `/v1/chat/completions`).

## 2. Eureka Creative Engine v7 (`adapters/eureka/ec_cowrie.{h,c}`)
- **Live path:** `/home/omen/worktrees/eureka-engine/creative/ec_cowrie.{h,c}`.
- Cross-restart multimodal text-encoder `StatePages` (`151.7×` Wan 2.2 UMT5-XXL; `200×–2,970×` FLUX.2 Qwen3-4B) plus `ec_cowrie_sp_save_prefix` / `ec_cowrie_sp_load_longest_prefix` chunked prefix scanning.
- Canonical `CoreHash` ComfyUI 8-node DAG cache (`1.0–9.6 ms` across kills), Studio Vault `R0–R2` lenses (`−99.8%` at `R0`), `creative_audit.cwwal` hash-chained logging, and `ec_cowrie_trust_gate_signed_json` (`Ed25519` owner declassification).

## 3. llama.cpp (`adapters/llama-statepage/cowrie_llama_statepage.hpp`)
- Header-only C++17 drop-in providing:
  - `cowrie::save_llama_statepage` / `cowrie::restore_llama_statepage` for split-precision hybrid `Q4_0`/`Q8_0`/`FP16` KV + exact `F32` DeltaNet SSM (`49.7×` cold NVMe / `123.2×` warm on `Qwen3.5-9B`, `KL = 3.99e-04`, plus `3.38 GB/s` cold `O_DIRECT` dual-stream CUDA DMA).
  - `cowrie::LlamaPrefixStateCache`: Chunked longest-prefix KV/SSM offloader (`13.72×` speedup on brand-new suffix questions: `9.01 ms vs 123.63 ms` cold prefill, `100%` greedy token parity, explicit multi-slot `seq_id`, thread-safe LRU eviction).
  - `cowrie::LlamaCowrieMemory` (`.cwwal` live replay + 47-bit handles + `Ed25519` `DeclassifyToken` gate) and `cowrie::LlamaZonedShard` (`CWZM`).

## 4. vLLM 0.15+ (`adapters/vllm/cowrie_kv_connector.py` + `cowrie_vllm.py`)
- **`CowrieKVConnectorV1`:** Native `KVConnectorBase_V1` packing all layers' paged KV blocks into a single `0600` HMAC-authenticated `.state.cowrie` shard (`80.53 ms vs 453.88 ms` cold prefill = `5.64×` cross-process speedup on `11,755` tokens, chunked-prefill safe, `100%` greedy parity).
- **`CowrieMemoryStore` + `CowrieVllmRuntime`:** Whole-context causal `0x80` lineage, `.cwwal` WAL persistence, `R0/R2` lenses (`−69.7%` prompt tokens), and `Ed25519` `declassify_tokens` tool gating.

## 5. WebAssembly SDK (`wasm/cowrie.mjs` + `cowrie_core.wasm`)
- Build via `make wasm` -> `/tmp/cowrie-cargo-target/wasm32-unknown-unknown/release/cowrie_core.wasm` (`193,278 B` raw / `82,668 B` (`80.7 KiB`) gzipped, zero external imports).
- Verified in both **Node.js v22** and **Headless Chrome 135 (`WebAssembly.instantiate`)**: `0.57 ms` instantiation, `1.72 µs` zero-alloc canonical verification + CID, `R0/R1/R2` lenses, 16 KB `CWRF` dual-tree `0x04` Merkle verification, `CWSP` in-RAM StatePage verification, and RFC 8032 `Ed25519` signing/verification, plus `2,000` lockstep differential vectors against compiled `formal/CowrieKernel.bend`.
