# Cowrie Engineering Guide (`v1.2.0`)

Cowrie (`v1.2.0`) is a 16 KB-aligned, content-addressed, provenance-tagged binary container and runtime library for AI inference, retrieval, and tool execution. It serves three consumers from one container layout: LLM context windows (`Plane B` lenses), GPU/NVMe DMA pipelines (`Plane C` 16 KB arenas), and CPU integrity and policy verifiers (`Plane A` canonical wire + `Plane D` causal provenance and Merkle roots).

Every metric in this guide is backed by a reproducible receipt in [`receipts/`](../receipts/) or `~/tessera-lab/`.

---

## 1. Benchmark Scoreboard

### A. Inference, KV Offloading & Prompt Compression
| Capability | Baseline | Cowrie (`v1.2.0`) | Gain | Verification | Receipt |
|---|---:|---:|---:|---|---|
| **13.8K-tok Hybrid SSM+KV Resume** (`llama.cpp`, `Qwen3.5-9B`, `Q4_0` KV + `F32` SSM) | `3,391.5 ms` (`505.2 MB` FP16) | `68.2 ms` cold / `27.2 ms` warm (`180.15 MB`) | `49.7×` cold / `123.2×` warm (`2.80×` smaller) | `KL = 3.99e-04`, `33/33` tokens, `5/5` needles, `HMAC-SHA256` + `SHA-NI` Merkle | [`receipt_shani_kvquant.json`](../receipts/receipt_shani_kvquant.json) |
| **13.8K-tok Exact FP16 Resume** (`llama.cpp`, `FP16` KV + `F32` SSM) | `3,366.4 ms` | `177.7 ms` cold / `70.9 ms` warm (`505.17 MB`) | `18.9×` cold / `47.5×` warm | `KL = 0.00e+00`, `max_diff = 0.00`, `33/33` tokens | [`receipt_shani_kvquant.json`](../receipts/receipt_shani_kvquant.json) |
| **`vLLM 0.15.0` Cross-Restart KV Offload** (`CowrieKVConnectorV1`, `11,755` tok, 12L) | `453.88 ms` cold prefill | `80.53 ms` (`144.13 MB` `.state.cowrie`) | `5.64×` across engine restart | `100%` greedy parity, `runtime_id="vllm-v1"`, chunked-prefill safe | [`receipt_v1_2_complete.json`](../receipts/receipt_v1_2_complete.json) |
| **`llama.cpp` `LlamaPrefixStateCache`** (`1,503` tok shared prefix, `Qwen3.5-0.8B`) | `123.63 ms` cold prefill | `46.46 ms` full / `9.01 ms` new suffix (`29.95 MB`) | `2.66×` full / `13.72×` suffix | `100%` greedy parity across process restarts with `COWRIE_HMAC_KEY` | [`receipt_v1_2_wasm_and_parity.json`](../receipts/receipt_v1_2_wasm_and_parity.json) |
| **16 KB `O_DIRECT` + Dual-Stream CUDA DMA** (`192 MiB` StatePage $\to$ `RTX 3090`) | `77.88 ms` (`2.59 GB/s` pageable) | `59.62 ms` cold (`3.38 GB/s`) / `7.51 ms` warm (`26.79 GB/s`) | `1.31×` cold NVMe-to-VRAM | Double-buffered `cudaHostAlloc` + 2 CUDA streams on `/dev/nvme1n1p3` | [`receipt_v1_2_complete.json`](../receipts/receipt_v1_2_complete.json) |
| **Eureka Creative v7 Encoder & DAG Cache** (`UMT5-XXL`, `FLUX Qwen3-4B`, 8-node DAG) | `3.23 s` / `91.8 s` / `98.7 s` | `21.3 ms` / `5.9–30.9 ms` / `1.0–9.6 ms` | `151.7×` Wan / `200×–2,970×` FLUX | `8/8` DAG hits (`6/8` on key-shuffled edits); `R0` Vault `−99.8%` bytes | `c0-v7-cowrie-receipt.txt` |
| **Eureka LLM Cold Process Restart** (`Qwen3.5-9B`, `Q8_0`, `2,800` tok HTTP) | `1,541.6 ms` E2E HTTP | `96.3 ms` state / `308.5 ms` E2E (`100.3 MB`) | `5.00×` E2E HTTP | Exact needle `"0x80"` restored; `.cwwal` + `Ed25519` HTTP gates active | [`receipt_live_all.json`](../receipts/receipt_live_all.json) |
| **Paired QA with `R2` Lenses** (`Qwen3.5-9B` + `vLLM 0.15.0`, 24 tasks) | `500–1,311 ms` JSON; `3,176` tok | `138–353 ms` (`−65.2%` tok; `961` tok / `−69.7%` vLLM) | `1.93×–4.43×` `llama.cpp` / `1.15×` vLLM | `24/24 (100%)` QA parity; canonical CID hits vLLM prefix cache | `~/tessera-lab/receipt_u1.json` · [`receipt_vllm_cowrie.json`](../receipts/receipt_vllm_cowrie.json) |

### B. Indexing, WAL Compaction, Provenance, WebAssembly & Formal Proofs
| Capability | Baseline | Cowrie (`v1.2.0`) | Gain | Verification | Receipt |
|---|---:|---:|---:|---|---|
| **16 KB Zone-Map Shards (`CWZM`)** (`100,000` rows on cold NVMe) | `1.031 ms` `SQLite3` PK (`1.562 ms` `ts`; `8.91 MB`) | `0.514 ms` point (`0.001%`) / `0.542 ms` `ts` (`7.57 MB`) | `1.58×–2.88×` cold NVMe, `−15.0%` disk | 64 B Merkle-authenticated Zone-Map fences + per-page `SHA-NI` check | [`receipt_v1_2_complete.json`](../receipts/receipt_v1_2_complete.json) |
| **Hash-Chained `.cwwal` + `LiveIvfStore`** (`5K` frames; `50.5K` INT8×128 vectors) | `12.9 ms` full shard build | `4.45 µs` append (`1.88 ms` `fsync`); `87.5 µs` hybrid search | Sub-`5 µs` ingest + `3.14 ms` recovery | Sequential tombstone & re-insertion replay; `12.91 ms` LSM compaction | [`receipt_v1_2_complete.json`](../receipts/receipt_v1_2_complete.json) |
| **`cowrie mcp-proxy` + `Ed25519` (`COWRIE_DECLASSIFY_V1`)** | Stdio JSON-RPC pass-through | `13.15 µs` wire gate; `810.4 µs` sign / `808.2 µs` verify | Wire-level causal taint isolation | Pure-Rust RFC 8032 §7.1; binds `(target_cid_34, action_u8, expiry_u64)` | [`receipt_v1_2_complete.json`](../receipts/receipt_v1_2_complete.json) |
| **Zero-Alloc Wire Verifier + `TAG_TENSOR_REF (0x0A)`** | `1,261.5 ns` full decode; `78 B` sentinel | `169.8 ns` verify (`278.7 ns` w/ CID); `41 B` `0x0A` tag | `7.43×` faster verify, `−47.4%` bytes | `0` heap allocations; dual-tree `0x04` Merkle over frames and tensors | [`receipt_v1_1_hardening.json`](../receipts/receipt_v1_1_hardening.json) |
| **10M × 512d Cold NVMe Search (`CWIV`)** (`5.30 GB` shard, `K=4,096`, `nprobe=16`) | `64.81 ms` serial `pread` | `7.07 ms` cold NVMe (`0.155 ms` warm VRAM) | `9.17×` cold (`0.390%` disk read) | `100.0%` Recall@20, `AVX2`/`NEON` INT8 kernels, `0x04`/`0x80` filtering | `~/tessera-lab/receipt_u3_v03.json` |
| **64-Stride Model Fingerprinter + `SHA-NI`/`ARMv8 SHA2`** (`5.59 GiB` GGUF) | `~3.0 s` full-file hash | `1.86 ms` (`64 × 64 KiB` strides); `1.92 GiB/s` 1-thread | `> 1,500×` faster model identity check | Samples 64 evenly spaced weight windows across all model layers | [`receipt_v1_1_hardening.json`](../receipts/receipt_v1_1_hardening.json) |
| **WebAssembly (`cowrie_core.wasm` + `wasm/cowrie.mjs`)** | Native C-ABI | `193,278 B` raw / `82,668 B` (`80.7 KiB`) gzipped | `0.57 ms` load, `1.72 µs` verify+CID | Zero host imports; verified in Node v22 and Headless Chrome 135 | [`receipt_v1_2_wasm_and_parity.json`](../receipts/receipt_v1_2_wasm_and_parity.json) |
| **`Bend 2.0.35 --verdict` Formal Suite (`formal/`)** | Unit & fuzz testing | `26/26` laws (`218.3 ms`) + `2,000` lockstep vectors | Machine-checked kernel invariants | Lean-proven BendTT kernel (`0 @unsafe`) + `.mjs` $\leftrightarrow$ `.wasm` parity | [`receipt_v1_2_formal_bend.json`](../receipts/receipt_v1_2_formal_bend.json) |

---

## 2. Core Use Cases

### 1. Cross-Restart KV & Hybrid SSM+KV Checkpointing (`CWSP`)
Checkpoints live model state (`Q4_0`/`Q8_0`/`FP16` GQA KV + `F32` recurrent SSM in `llama.cpp` and `Eureka`, or multi-layer paged KV blocks in `vLLM 0.15+` via `CowrieKVConnectorV1`) into `0600` 16 KB-aligned `.state.cowrie` shards. Each shard pins `(model_sha, runtime_id, kv_quant, ssm_dtype)` with optional RFC 2104 `HMAC-SHA256` and parallel 4 MiB `SHA-NI`/`ARMv8 SHA2` Merkle verification, enabling `27–96 ms` workspace restores and `9.01 ms` (`13.72×`) longest-prefix reuse on new suffix queries.

### 2. Multi-Resolution Prompt Projections (`R0 → R1 → R2`)
Projects canonical `COWR` frames deterministically at three resolutions:
- **`R0` (`@abcdefghij`):** 10-letter lowercase Alpha32 handle (`47.0` bits, `~9–12` tokens) for triaging thousands of records in context.
- **`R1` (Atlas):** Field names, types, array counts, and `<T:dtype[dims]@space>` tensor descriptors.
- **`R2` (Attention Lens):** Hoists uniform tables (`len >= 3`, key density `>= 0.70`), keeps tensors as compact handles, and wraps output in CID-bound escaped fences (`−65.2%` to `−98.7%` tokens at `100%` QA parity).

### 3. Causal Provenance & Scoped `Ed25519` Tool Gating (`0x80` + `mcp-proxy`)
Assigns out-of-band authority (`0x01 SYS_POLICY`, `0x02 VERIFIED_TOOL`, `0x04 UNTRUSTED_EXT`, `0x08 MODEL_SYNTH`) in the 8-byte frame header and propagates `BIT_TAINTED (0x80)` transitively across derived records. `cowrie mcp-proxy` wraps stdio MCP servers (`13.15 µs` check), blocking tainted calls to `shell_exec`, `cred_read`, `fs_write`, and `net_exfil` unless authorized by a non-expired RFC 8032 `Ed25519` `COWRIE_DECLASSIFY_V1` token for that exact `(cid, action)` pair.

### 4. Out-of-Core Vector Search & Live Streaming Ingest (`CWIV` + `.cwwal`)
Stores `[embeddings || metadata || 0x00 -> 16 KB]` inside contiguous cluster blocks queried with `QD=16` `O_DIRECT` reads and `AVX2`/`NEON` INT8 kernels (`7.07 ms` over `10M × 512d` on cold NVMe). `LiveIvfStore` pairs immutable `CWIV` shards with a SHA-256 hash-chained `.cwwal` delta log (`4.45 µs` buffered append, `87.5 µs` hybrid search, `12.91 ms` compaction).

### 5. Cold NVMe Point & Range Indexing (`CWZM`)
Indexes sorted canonical frames in 16 KB leaf pages using 64-byte Plane D Zone-Map entries `(min_key, max_key, min_sec, max_sec, leaf_sha256)`. Queries prune non-overlapping pages in L1 cache and read only matching 16 KB pages (`0.514 ms` point lookup and `0.542 ms` secondary range on 100K cold NVMe rows), verifying each touched page's `SHA-NI` leaf digest.

### 6. Cross-Language Canonical Identity (`COWR` + `0x0A TensorRef`)
Produces identical bytes and identical 34-byte SHA-256 CIDs across Python, Rust, C/C++, and JavaScript producers regardless of input map key order or `-0.0` float formatting, verifying frames in `169.8 ns` with zero heap allocations and encoding tensor references in `41 B` (`0x0A`).

### 7. Client-Side Browser & Edge Verification (`cowrie_core.wasm`)
Runs zero-alloc frame verification (`1.72 µs`), `R0–R2` lens projection, 16 KB `CWRF` dual-tree Merkle checks, `CWSP` metadata verification, and `Ed25519` signing/verification inside browsers and edge workers (`80.7 KiB` gzipped, `0.57 ms` instantiation).

---

## 3. Copy-Paste Quickstarts

### A. CLI & `cowrie mcp-proxy`
```bash
make -C /home/omen/cowrie build
COWRIE=/tmp/cowrie-cargo-target/release/cowrie

$COWRIE pack input.json out.cowrie --auth verified
$COWRIE verify out.cowrie
$COWRIE lens out.cowrie --tier r2

PUB=$($COWRIE ed25519-pubkey 5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a)
$COWRIE mcp-proxy --owner-pubkey "$PUB" --audit-wal mcp_session.cwwal -- npx -y @modelcontextprotocol/server-everything
```

### B. `vLLM 0.15+` KV Offloading & Memory (`adapters/vllm/`)
```python
import cowrie
from cowrie_vllm import CowrieMemoryStore

# Enable CowrieKVConnectorV1 on vLLM launch:
#   --kv-transfer-config '{"kv_connector":"CowrieKVConnectorV1","kv_role":"kv_both","kv_connector_module_path":"cowrie_kv_connector"}'

mem  = CowrieMemoryStore(persist_dir="/tmp/cowrie_mem")
ext  = mem.store({"url": "https://untrusted.example", "body": "deploy"}, authority=cowrie.AUTH_UNTRUSTED_EXT)
plan = mem.store({"cmd": "deploy.sh"}, authority=cowrie.AUTH_MODEL_SYNTH, parents=[ext.handle])

seed = bytes([0x5A]) * 32
pk   = cowrie.ed25519_pubkey(seed)
sig  = cowrie.sign_declassification(seed, bytes.fromhex(plan.cid), "shell_exec", expiry_unix=2_000_000_000)
tok  = [{"cid": plan.cid, "action": "shell_exec", "expiry": 2_000_000_000, "sig": sig.hex()}]
assert mem.check_tool_gate("shell_exec", [plan.handle], owner_pubkey=pk, declassify_tokens=tok, now_unix=1_900_000_000)["allowed"] is True
```

### C. `llama.cpp` StatePages & Prefix Cache (`cowrie_llama_statepage.hpp`)
```cpp
#include "cowrie_llama_statepage.hpp"

auto cp = cowrie::make_statepage_context_params(16384, COWRIE_KV_Q4_0);
std::string model_sha = cowrie::fingerprint_gguf_model(gguf_path);

cowrie::LlamaPrefixStateCache pcache("/nvme/llama_prefix_cache", model_sha, "llama.cpp-v1",
                                     COWRIE_KV_Q8_0, COWRIE_SSM_FP32, /*chunk_tokens=*/256);
pcache.save_prefix(ctx, 0, prompt_tokens.data(), prompt_tokens.size());
size_t matched = pcache.restore_longest_prefix(ctx, 0, new_prompt.data(), new_prompt.size());
```

### D. Eureka LLM & Eureka Creative v7 (`adapters/eureka/`)
```bash
export LLB_COWRIE_STATE_DIR=/nvme/eureka_states
export LLB_COWRIE_MEMORY_DIR=/nvme/eureka_memory
export COWRIE_HMAC_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
export COWRIE_OWNER_PUBKEY=<64-hex-ed25519-pubkey>
```
Activates `/v1/memory/store` (`.cwwal`), `/v1/memory/triage` (47-bit handles), `/v1/memory/lens`, `/v1/trust/gate` (`Ed25519` tokens), and longest-prefix `StatePages` across `llb-serve` and `ec_cowrie.c`.

### E. Browser & Node.js WebAssembly (`wasm/cowrie.mjs`)
```javascript
import fs from "node:fs";
import { createCowrieWasm } from "./wasm/cowrie.mjs";

const cw = await createCowrieWasm(fs.readFileSync("/tmp/cowrie-cargo-target/wasm32-unknown-unknown/release/cowrie_core.wasm"));
const frame = cw.encodeJson({ b: 1, a: [10, 20, 30] }, 0x02, 0x01);
const info  = cw.verifyFrame(frame);
const r2    = cw.projectLens(frame, "r2");
```

### F. Python `CWZM` Zone-Maps & `.cwwal` Compaction (`python/cowrie/`)
```python
import cowrie

f0 = cowrie.encode_json({"id": 1, "ts": 1000, "msg": "alpha"})
f1 = cowrie.encode_json({"id": 2, "ts": 2000, "msg": "beta"})
cowrie.write_zoned_shard("/tmp/events.cwzm", [(1, 1000, f0), (2, 2000, f1)])
with cowrie.ZonedShard("/tmp/events.cwzm") as zs:
    hits, bytes_read = zs.query_keys(2, 2)

with cowrie.Wal("/tmp/live.cwwal") as wal:
    wal.append_frame(f0)
    _, cid1 = wal.append_frame(f1)
    wal.append_tombstone(cid1)
    wal.compact_to_shard("/tmp/compacted.cowrie")
```

---

## 4. Workload Fit & Format Selection

| Workload | Recommended Choice | Physical Basis |
|---|---|---|
| **KV / Hybrid SSM+KV checkpointing, multimodal records, agent memory, filtered IVF & point shards** | **Cowrie (`CWSP`, `COWR`, `R0–R2`, `CWIV`, `CWZM`, `CWAL`)** | 16 KB alignment, canonical CIDs, token-compact lenses, and out-of-band provenance. |
| **Autoregressive decode throughput (`tok/s` after token 1)** | **Weight quantization & GEMM/attention kernels** | Decode speed is governed by GPU memory bandwidth and compute kernels (`0%` container impact). |
| **Cold archival compression of flat prose logs** | **`zstd + JSONL`** | Repeated ASCII keys give LZ77 lower compressed size (`1,323 B` vs `2,257 B` on `W3`). |
| **Multi-table relational joins & wide columnar OLAP** | **`DuckDB` / `Parquet` / `SQLite`** | Dedicated relational join planners and column-striped encodings target ad-hoc SQL analytics. |

---

## 5. Module Status (`v1.2.0`)

| Component | Status | Verification Coverage |
|---|---|---|
| **Core (`cowrie-core`, `cowrie.h`, Python)** | Shipped | `12/12` Rust unit suites + standalone C-ABI conformance (`vectors.json`, Merkle, `CWSP`, `Ed25519`, `CWAL`, `CWZM`) |
| **WebAssembly (`cowrie_core.wasm`, `wasm/cowrie.mjs`)** | Shipped | `150` golden vectors + `20` negative/DoS + `14` Merkle bitflips + `Ed25519` in Node v22 & Chrome 135 |
| **MCP Firewall (`cowrie mcp-proxy`)** | Shipped | End-to-end stdio JSON-RPC proxy, `.cwwal` logging, and `Ed25519` declassification verification |
| **Runtime Adapters (`vLLM`, `llama.cpp`, `Eureka LLM`, `Eureka Creative v7`)** | Shipped | Live RTX 3090 + NVMe receipts across all four engines (`receipts/receipt_v1_2_*.json`) |
| **Formal Proofs (`formal/PROOF.bend`)** | Shipped | `26/26` laws checked by Bend 2.0.35 `--verdict` (`218.3 ms`) + `2,000` Bend-to-WASM lockstep vectors |

## SUMMARY
Engineering guide for Cowrie `v1.2.0` covering measured benchmarks, core use cases, copy-paste quickstarts across `CLI`, `vLLM 0.15+`, `llama.cpp`, `Eureka`, `cowrie mcp-proxy`, `wasm32`, and Python, workload selection guidelines, and verification coverage.
