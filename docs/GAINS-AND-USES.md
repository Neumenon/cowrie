# Cowrie — Measured Gains, Use Cases & Engineering Guide (`v1.2.0`)

> **Core is physics, profiles are chemistry, lenses are optics.**

Cowrie (`v1.2.0`) is a **16 KB-aligned, content-addressed, trust-tagged binary container and runtime substrate** designed for the tri-consumer collision of AI systems: an **LLM attention engine** (paying per token), a **GPU/NPU arithmetic & NVMe DMA engine** (requiring `16,384 B` page-aligned zero-copy `mmap` and `O_DIRECT` transfers), and a **CPU security/cache verifier** (requiring zero-allocation bijective hashing, unforgeable origin tags, and asymmetric `Ed25519` auditability).

This engineering guide documents **every measured gain**, **eight concrete production use cases**, **copy-paste usage across all four runtimes + MCP + WebAssembly + Bend formal proofs**, **honest physical boundaries where other formats win**, and the **complete `v1.2.0` verification ledger** — backed strictly by committed receipts in [`receipts/`](../receipts/) and `~/tessera-lab/`.

---

## 1. Executive Scoreboard — All Measured Gains

### A. Inference Latency, KV Offloading & Context Efficiency (`Qwen3.5`, `Creative v7` & `vLLM 0.15.0`)
| Capability | Baseline | Cowrie (`v1.2.0`) | Measured Gain | Quality / Safety Proof | Receipt |
|---|---:|---:|---:|---|---|
| **13.8K-tok Hybrid SSM+KV Workspace Resume** (`llama.cpp`, `Q4_0` KV + Exact `F32` DeltaNet SSM) | `3,391.5 ms` cold prefill (`505.2 MB` FP16 state) | **`68.2 ms`** cold NVMe verified<br>**`27.2 ms`** warm verified (`180.15 MB`) | **`49.7×`** cold NVMe<br>**`123.2×`** warm mmap<br>**`2.80×`** smaller state | `KL = 3.99e-04`, `33/33` exact tokens, `5/5` needles, `model_sha` + `runtime_id` + `kv_quant` + `ssm_dtype` + HMAC | [`receipt_shani_kvquant.json`](../receipts/receipt_shani_kvquant.json) |
| **13.8K-tok Exact FP16 Workspace Resume** (`llama.cpp`, FP16 KV + Exact SSM) | `3,366.4 ms` cold prefill | **`177.7 ms`** cold NVMe verified<br>**`70.9 ms`** warm verified (`505.17 MB`) | **`18.9×`** cold NVMe<br>**`45.5×–47.5×`** warm | `KL = 0.00e+00`, `max_diff = 0.00`, `33/33` exact tokens, `5/5` needles | [`receipt_shani_kvquant.json`](../receipts/receipt_shani_kvquant.json) |
| **`vLLM 0.15.0` V1 Cross-Restart `CowrieKVConnectorV1`** (`11,755` tok, 12-layer `Qwen3ForCausalLM`, `FLASH_ATTN`) | `453.88 ms` pure cold prefill (12 per-layer `.safetensors` files) | **`80.53 ms`** cross-process NVMe restore (1 × `144.13 MB` `.state.cowrie`) | **`5.64×`** faster across full engine kill | `100%` greedy token parity, `HMAC-SHA256` + 4 MiB SHA-NI Merkle + `runtime_id="vllm-v1"` + chunked-prefill safe | [`receipt_v1_2_complete.json`](../receipts/receipt_v1_2_complete.json) |
| **`llama.cpp` `LlamaPrefixStateCache` Cross-Process Prefix/Suffix Reuse** (`1,503` tok shared prefix, `Qwen3.5-0.8B`, `Q8_0` KV + `F32` SSM) | `123.63 ms` cold prefill | **`46.46 ms`** exact prefix restore<br>**`9.01 ms`** brand-new suffix question (`1,488` tok hit, `29.95 MB`) | **`2.66×`** full restore<br>**`13.72×`** new-suffix reuse | `100%` greedy token parity across OS process restarts with `COWRIE_HMAC_KEY` | [`receipt_v1_2_wasm_and_parity.json`](../receipts/receipt_v1_2_wasm_and_parity.json) |
| **Cold NVMe `O_DIRECT` + Pinned Dual-Stream CUDA DMA** (`192 MiB` StatePage $\to$ `RTX 3090`) | `77.88 ms` (`2.59 GB/s` pageable `pread + cudaMemcpy`) | **`59.62 ms`** cold (`3.38 GB/s` `O_DIRECT` + 2 streams)<br>**`7.51 ms`** warm (`26.79 GB/s` PCIe Gen4) | **`1.31×`** faster cold NVMe-to-VRAM | 16 KB alignment enables `O_DIRECT` on `/dev/nvme1n1p3` without consumer GeForce P2P BAR1 GDS | [`receipt_v1_2_complete.json`](../receipts/receipt_v1_2_complete.json) |
| **Eureka Creative v7 Multimodal Restarts & ComfyUI DAG** (`UMT5-XXL`, `FLUX Qwen3-4B`, 8-node DAG) | `3,230.8 ms` Wan / `91.8 s` cold FLUX / `98.7 s` DAG | **`21.3 ms`** Wan / **`5.9–30.9 ms`** FLUX / **`1.0–9.6 ms`** 8-node DAG | **`151.7×`** Wan<br>**`200×–2,970×`** FLUX<br>**`8/8`** DAG hits | Canonical `CoreHash` survives key-shuffled edits (`6/8` upstream hits); `0x84` URLs blocked `403` | `c0-v7-cowrie-receipt.txt` |
| **Eureka LLM Cold Process Restart** (`engine_gpu` + `llb-serve`, `Qwen3.5-9B`, `Q8_0`, `2,800` tok) | `1,541.6 ms` E2E HTTP cold prefill | **`96.3 ms`** cold NVMe state restore<br>**`308.5 ms`** E2E HTTP (`100.3 MB`) | **`5.00×`** E2E HTTP across full process kill | Exact needle `"0x80"` restored; `.cwwal` + `Ed25519` HTTP 403/200 gates verified | [`receipt_live_all.json`](../receipts/receipt_live_all.json) · [`receipt_v1_2_wasm_and_parity.json`](../receipts/receipt_v1_2_wasm_and_parity.json) |
| **Fresh Prefill / Paired QA (Zero Cache)** (`Qwen3.5-9B` + `vLLM 0.15.0`) | `500–1,311 ms` JSON; `3,176` tok vLLM | **`138–353 ms`** (`−65.2%` tok; **`961` tok / `−69.7%`** on vLLM) | **`1.93×–4.43×`** `llama.cpp`<br>**`1.13×–1.15×`** vLLM | **`24/24 (100%)`** QA parity (`0` flips); key-shuffle CID hits vLLM prefix cache | `~/tessera-lab/receipt_u1.json` · [`receipt_vllm_cowrie.json`](../receipts/receipt_vllm_cowrie.json) |

### B. Storage Indexing, WAL Compaction, Security, WebAssembly & Formal Verification (`v1.2.0`)
| Capability | Baseline | Cowrie (`v1.2.0`) | Measured Gain | Integrity / Security Proof | Receipt |
|---|---:|---:|---:|---|---|
| **Plane D 16 KB Zone-Map Shards (`CWZM`) vs Cold `SQLite3`** (`100,000` rows on `/dev/nvme1n1p3` post-`FADV_DONTNEED`) | `0.988–2.753 ms` `SQLite3` PK (`1.562 ms` secondary `ts` index; `8.91 MB`) | **`0.514 ms`** point (`0.001%`)<br>**`0.526–1.291 ms`** (`0.1%–10%`)<br>**`0.542 ms`** secondary `ts` (`7.57 MB`) | **`1.58×–2.13×`** PK cold<br>**`2.88×`** secondary cold<br>**`−15.0%`** disk size | Eliminates the `< 9%` cold SQLite gap; binds full 64 B Zone-Map fences + leaf SHA-NI Merkle & filters `0x04` taint | [`receipt_v1_2_complete.json`](../receipts/receipt_v1_2_complete.json) |
| **Crash-Safe `.cwwal` + `LiveIvfStore` Delta Compaction** (`5K` frames; `50K + 500` INT8×128 vectors) | Full-shard rewrite on every mutation | **`4.45 µs`** buffered append (`1.88 ms` NVMe `fsync`); **`87.5 µs`** hybrid search; **`12.91 ms`** 50.5K `CWIV` compact | **Live sub-`5 µs` ingest** + **`3.14 ms`** torn-tail recovery | SHA-256 hash chain recovers torn tails, rejects mid-log edits, and supports sequential tombstone & re-insertion replay | [`receipt_v1_2_complete.json`](../receipts/receipt_v1_2_complete.json) |
| **`cowrie mcp-proxy` Wire Firewall + Pure-Rust `Ed25519` (`COWRIE_DECLASSIFY_V1`)** | Unfiltered stdio MCP JSON-RPC or shared symmetric HMAC secret | **`13.15 µs`** wire block (`-32025`); **`810.4 µs`** sign / **`808.2 µs`** strict verify | **`100%` wire-level block** before MCP server sees call | RFC 8032 §7.1 verified; rejects expired or wrong-action (`shell_exec` $\to$ `fs_write`) token replays | [`receipt_v1_2_complete.json`](../receipts/receipt_v1_2_complete.json) |
| **Zero-Allocation Canonical Wire Verifier + `TAG_TENSOR_REF (0x0A)`** | `1,261.5 ns` decode-to-JSON; `78 B` tensor map sentinel | **`169.8 ns`** zero-alloc verify (`278.7 ns` w/ SHA-NI CID); **`41 B`** `0x0A` tag | **`7.43×`** faster verify<br>**`−47.4%`** tensor-ref bytes | Zero heap allocations; contiguous `CWRF` Dual-Tree `0x04` Merkle catches `10/10` frame + `4/4` tensor bitflips | [`receipt_v1_1_hardening.json`](../receipts/receipt_v1_1_hardening.json) |
| **10M × 512d Cold NVMe Out-of-Core Search (`CWIV`)** | `64.81 ms` (split-seek serial `QD=1` `pread`) | **`7.07 ms`** median (`5.50 ms` I/O @ `3.77 GB/s`, `0.390%` of `5.30 GB`) | **`9.17×`** faster cold NVMe (`256×` less disk read) | `100.0%` Recall@20, `0` leaked `0x04`, `AVX2`/`NEON` + C-ABI `cowrie_ivf_*` (`0.155 ms` warm VRAM) | `~/tessera-lab/receipt_u3_v03.json` |
| **64-Stride Model Fingerprinter + `SHA-NI`/`ARMv8 SHA2`** (`5.59 GiB` `Qwen3.5-9B` GGUF) | ~`3 s` full-file hash or head-only blind to LoRA | **`1.86 ms`** (`64 × 64 KiB` strided SHA-NI); **`1.92 GiB/s`** 1-thread | **`> 1,500×`** faster than full scan | Catches mid-layer weight/LoRA edits across all transformer layers | [`receipt_v1_1_hardening.json`](../receipts/receipt_v1_1_hardening.json) |
| **Browser & Node.js `wasm32-unknown-unknown` (`cowrie_core.wasm` + `wasm/cowrie.mjs`)** | Multi-MB JS/WASM bundle or unverified JSON in browser | **`193,278 B`** raw / **`82,668 B` (`80.7 KiB`)** gzipped; **`0.57 ms`** load; **`1.72 µs`** verify+CID | **Zero imports**, `< 1 ms` startup | `150/150` golden vectors + `20` DoS/negatives + `14` dual-Merkle bitflips + RFC 8032 `Ed25519` verified in Node v22 & Chrome 135 | [`receipt_v1_2_wasm_and_parity.json`](../receipts/receipt_v1_2_wasm_and_parity.json) |
| **Non-Bolt-On `Bend 2.0.35 --verdict` Formal Proofs + Lockstep Gate (`formal/`)** | One-sided specs & subtle edge bugs (`<<<<` bypass, raw `R2` table cols, `TensorRef` `>>`+`>`, `CWZM` fences, `CWRF` slack, zombie `.cwwal` tombstones, empty `runtime_id`) | **`26/26` biconditional/liveness laws** checked by Lean-proven BendTT kernel in **`218.3 ms`** + **`2,000` lockstep vectors** | **`8` real security/wire bugs (`F1`–`F8`) caught & closed** | `100%` `.bendtt` coverage (`0` `@unsafe`), **`13/13` fail-open & anti-underspecification mutants rejected**, compiled `.mjs` locked against `.wasm` & `.so` | [`receipt_v1_2_formal_bend.json`](../receipts/receipt_v1_2_formal_bend.json) |

---

## 2. Eight Real-World Use Cases (When & Why to Use Cowrie `v1.2`)

### Use 1 — Cross-Restart Hybrid SSM+KV & Paged KV Offloading (`CWSP`)
- **The pain:** Restarting an inference daemon, autoscaling a cloud pod, or switching between chat, coding, and FLUX/Wan creative models discards gigabytes of prefilled GPU state, forcing seconds of quadratic re-prefill. Worse, loading mismatched weights, runtime layouts, or uniformly quantized SSM states silently corrupts outputs.
- **How Cowrie solves it:** Packs split-precision hybrid state (`Q4_0`/`Q8_0`/`FP16` GQA KV + exact `F32` recurrent DeltaNet SSM in `llama.cpp` and `Eureka`, or multi-layer paged KV blocks in `vLLM 0.15+` via `CowrieKVConnectorV1`) into a single `0600` `16 KB`-aligned `.state.cowrie` satellite bound to `(model_sha, runtime_id, kv_quant, ssm_dtype, hmac_sha256)` with parallel 4 MiB `SHA-NI`/`ARMv8 SHA2` Merkle verification and double-buffered `O_DIRECT` CUDA DMA.
- **Result:** `27–96 ms` full workspace restore (`5.0×–123.2×` faster; `151.7×–2,970×` on creative text encoders) and `9.01 ms` (`13.72×`) longest-prefix reuse on brand-new suffix questions, while failing closed on any model, runtime, quant, or bitflip mismatch.

### Use 2 — Multi-Resolution Agent Memory (`47-Bit Alpha32 R0 → R1 → Adaptive R2`)
- **The pain:** Naive RAG shreds structured tables into arbitrary 512-token chunks, prints hex IDs that Qwen3.5/Llama-3 `\p{N}` tokenizers split digit-by-digit, and burns context budgets on repeated JSON keys or inline float arrays.
- **How Cowrie solves it:** Stores canonical binary `COWR` frames in Plane A and projects zero-inference views in Plane B:
  - **`R0` (`@abcdefghij`, `~9–12 tok/doc`):** 10-letter lowercase Alpha32 handle (`47.0` bits, `~12M` birthday bound, zero digit fragmentation).
  - **`R1` (Structural Atlas):** Schema keys, table dimensions, and typed `<T:dtype[dims]@space>` tensor pointers.
  - **`R2` (Adaptive Attention Lens):** Hoists uniform tables (`len >= 3`, key density `>= 0.70`), falls back to compact JSON on sparse ASTs, and escapes `<<<`/`>>>` via a formally proven 3-state Mealy transducer across values, column headers, and `<T:...>` pointers.
- **Result:** `−65.2%` to `−98.7%` prompt tokens (`−99.8%` `R0` Studio Vault bytes) and `1.93×–4.43×` faster live QA at `24/24 (100%)` accuracy parity.

### Use 3 — Cryptographically Scoped Prompt-Injection Firewall (`0x80` + `Ed25519` + `mcp-proxy`)
- **The pain:** Prompt-level delimiters live in the attacker's alphabet, and naive per-message tags lose track of untrusted web/email data once an LLM summarizes it across two or three reasoning hops. Meanwhile, boolean "admin override" flags are too coarse for multi-tenant fleets.
- **How Cowrie solves it:** Tracks provenance out-of-band (`0x04 UNTRUSTED_EXT` $\to$ sticky `0x80 BIT_TAINTED`) across the full causal parent DAG, wraps any unmodified stdio MCP server via `cowrie mcp-proxy` (`13.15 µs` wire block), and supports pure-Rust RFC 8032 **`Ed25519` `COWRIE_DECLASSIFY_V1` tokens** approving one exact 34-byte CID for one exact `ToolAction` before `expiry_unix_secs` without unblocking any other tainted argument in context.

### Use 4 — Out-of-Core 10M-Vector Search + Live Streaming Compaction (`CWIV` + `.cwwal`)
- **The pain:** Keeping 10M vectors in RAM/VRAM costs `5.3–20+ GB`, split-file disk indexes stall on serial `pread` seeks (`64.8 ms`), and static shards cannot absorb live streaming inserts without full rewrites.
- **How Cowrie solves it:** Co-locates `[embs_c || meta_c || 0x00 -> 16 KB]` per IVF cluster for `QD=16` `O_DIRECT` + `AVX2`/`NEON` INT8 search (`7.07 ms` on `10M × 512d`) and pairs it with `LiveIvfStore` (`.cwwal` hash-chained appends in `4.45 µs`, `87.5 µs` hybrid search, and `12.91 ms` LSM compaction).

### Use 5 — Sub-Millisecond Cold NVMe Point & Range Queries (`CWZM` Zone-Maps)
- **The pain:** Full-shard scans lose to `SQLite3` B-trees on cold `< 9%` point/range lookups (`1.03–2.75 ms`).
- **How Cowrie solves it:** Packs sorted canonical frames into self-verifying 16 KB pages indexed by 64-byte Merkle-authenticated `(min_key, max_key, min_sec, max_sec, leaf_sha256)` Zone-Maps in Plane D (`zonemap.rs`), jumping directly to matching 16 KB pages (`0.514 ms` vs `1.031 ms` cold `SQLite3` at `0.001%`, `2.88×` faster on secondary timestamp ranges, `−15.0%` smaller on disk).

### Use 6 — Cross-Service Canonical Identity & Prefix-Cache Hits (`COWR` + `0x0A`)
- **The pain:** Microservices in Python, Rust, C++, and JS serialize logically identical JSON with different key order or `-0.0` / float formatting, busting content-addressed caches and vLLM/Eureka KV prefix caches.
- **How Cowrie solves it:** Enforces a bijective canonical wire representation verified in `169.8 ns` with zero heap allocations, plus native `TAG_TENSOR_REF (0x0A)` (`−47.4%` bytes). Key-shuffled inputs produce byte-identical CIDs and live KV prefix-cache hits.

### Use 7 — Zero-Import Browser & Edge Client Verification (`cowrie_core.wasm`)
- **The pain:** Browser and edge clients normally trust server JSON blindly or pull multi-megabyte JS/WASM bundles.
- **How Cowrie solves it:** `cowrie_core.wasm` (`80.7 KiB` gzipped, zero host imports) instantiates in `0.57 ms` in Chrome 135 and Node v22, running canonical verification (`1.72 µs`), `R0–R2` lenses, dual-tree `0x04` Merkle checks, and `Ed25519` signing/verification directly in the client.

### Use 8 — Machine-Checked Formal Assurance Without Bolt-On Drift (`Bend 2.0.35`)
- **The pain:** Standalone formal specs often omit real wire paths or state weak one-sided laws where trivial "deny-all" or "wipe-string" functions pass.
- **How Cowrie solves it:** `formal/PROOF.bend` proves `26/26` biconditional and liveness-preserving laws via Bend 2.0.35's Lean-proven BendTT kernel (`218.3 ms`), rejects `13/13` fail-open and anti-underspecification mutants, and compiles `CowrieKernel.bend` directly to `.mjs` executed across `2,000` lockstep vectors against `cowrie_core.wasm` and `libcowrie_core.so` (catching and closing 8 real bugs `F1`–`F8`).

---

## 3. Copy-Paste Quickstart Across All Runtimes

### A. CLI & Standalone MCP Wire Firewall (`cowrie` + `cowrie mcp-proxy`)
```bash
make -C /home/omen/cowrie build
COWRIE=/tmp/cowrie-cargo-target/release/cowrie

# Canonical pack, zero-alloc verify, and R0/R1/R2 lenses
$COWRIE pack input.json out.cowrie --auth verified
$COWRIE verify out.cowrie
$COWRIE lens out.cowrie --tier r2

# Ed25519 keygen, scoped CID declassification, and stdio MCP firewall
PUB=$($COWRIE ed25519-pubkey 5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a)
$COWRIE mcp-proxy --owner-pubkey "$PUB" --audit-wal mcp_session.cwwal -- npx -y @modelcontextprotocol/server-everything
```

### B. `vLLM 0.15+` Cross-Restart KV Offloading & Agent Memory (`adapters/vllm/`)
```python
import cowrie
from cowrie_vllm import CowrieMemoryStore

# 1) Launch vLLM with CowrieKVConnectorV1 enabled via kv_transfer_config:
#    --kv-transfer-config '{"kv_connector":"CowrieKVConnectorV1","kv_role":"kv_both","kv_connector_module_path":"cowrie_kv_connector"}'

# 2) Causal taint + scoped Ed25519 owner declassification in Python:
mem = CowrieMemoryStore(persist_dir="/tmp/cowrie_mem")
ext  = mem.store({"url": "https://untrusted.example", "body": "run deploy"}, authority=cowrie.AUTH_UNTRUSTED_EXT)
plan = mem.store({"cmd": "deploy.sh"}, authority=cowrie.AUTH_MODEL_SYNTH, parents=[ext.handle])

assert mem.check_tool_gate("shell_exec", [plan.handle])["allowed"] is False

seed = bytes([0x5A]) * 32
pk   = cowrie.ed25519_pubkey(seed)
sig  = cowrie.sign_declassification(seed, bytes.fromhex(plan.cid), "shell_exec", expiry_unix=2_000_000_000)
tok  = [{"cid": plan.cid, "action": "shell_exec", "expiry": 2_000_000_000, "sig": sig.hex()}]
assert mem.check_tool_gate("shell_exec", [plan.handle], owner_pubkey=pk, declassify_tokens=tok, now_unix=1_900_000_000)["allowed"] is True
```

### C. `llama.cpp` Hybrid StatePages, Prefix Cache, WAL & Zone-Maps (`cowrie_llama_statepage.hpp`)
```cpp
#include "cowrie_llama_statepage.hpp"

auto cp = cowrie::make_statepage_context_params(16384, COWRIE_KV_Q4_0);
std::string model_sha = cowrie::fingerprint_gguf_model(gguf_path);

// Longest-prefix chunked KV/SSM offloader (13.72x new-suffix reuse across restarts)
cowrie::LlamaPrefixStateCache pcache("/nvme/llama_prefix_cache", model_sha, "llama.cpp-v1",
                                     COWRIE_KV_Q8_0, COWRIE_SSM_FP32, /*chunk_tokens=*/256);
pcache.save_prefix(ctx, 0, prompt_tokens.data(), prompt_tokens.size());
size_t matched = pcache.restore_longest_prefix(ctx, 0, new_prompt.data(), new_prompt.size());
```

### D. Eureka LLM & Eureka Creative Engine v7 (`adapters/eureka/`)
```bash
export LLB_COWRIE_STATE_DIR=/nvme/eureka_states
export LLB_COWRIE_MEMORY_DIR=/nvme/eureka_memory
export COWRIE_HMAC_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
export COWRIE_OWNER_PUBKEY=<64-hex-ed25519-pubkey>
```
Exposes `/v1/memory/store` (backed by `memory.cwwal`), `/v1/memory/triage` (47-bit 10-letter handles), `/v1/memory/lens`, `/v1/trust/gate` (supporting signed `declassify_tokens`), and longest-prefix `StatePages` across both `llb-serve` and `ec_cowrie.c`.

### E. Browser & Node.js WebAssembly (`wasm/cowrie.mjs`)
```javascript
import fs from "node:fs";
import { createCowrieWasm } from "./wasm/cowrie.mjs";

const cw = await createCowrieWasm(fs.readFileSync("/tmp/cowrie-cargo-target/wasm32-unknown-unknown/release/cowrie_core.wasm"));
const frame = cw.encodeJson({ b: 1, a: [10, 20, 30] }, 0x02, 0x01);
const info  = cw.verifyFrame(frame);       // 1.72 us zero-alloc verify + 34B SHA-256 CID
const r2    = cw.projectLens(frame, "r2"); // CID-fenced R2 projection in browser/worker
```

### F. Python `CWZM` 16 KB Zone-Maps & `.cwwal` Tombstone Compaction (`python/cowrie/`)
```python
import cowrie

f0 = cowrie.encode_json({"id": 1, "ts": 1000, "msg": "alpha"})
f1 = cowrie.encode_json({"id": 2, "ts": 2000, "msg": "beta"})
cowrie.write_zoned_shard("/tmp/events.cwzm", [(1, 1000, f0), (2, 2000, f1)])
with cowrie.ZonedShard("/tmp/events.cwzm") as zs:
    hits, bytes_read = zs.query_keys(2, 2)  # reads & SHA-NI verifies only 1 x 16 KB page

with cowrie.Wal("/tmp/live.cwwal") as wal:
    wal.append_frame(f0)
    _, cid1 = wal.append_frame(f1)
    wal.append_tombstone(cid1)              # revokes f1 without touching f0; re-appending f1 revives it
    wal.compact_to_shard("/tmp/compacted.cowrie")
```

### G. Non-Bolt-On `Bend 2.0.35 --verdict` Formal Verification (`formal/`)
```bash
make -C /home/omen/cowrie formal
# Runs all 4 stages (receipts/receipt_v1_2_formal_bend.json):
#   1) bend formal/PROOF.bend --verdict -> 26/26 laws ALL PROOFS CHECK (218.3 ms)
#   2) .bendtt kernel coverage & 0 @unsafe / 0 ? / 0 ~ hygiene audit
#   3) 13/13 fail-open & anti-underspecification mutant controls REJECTED
#   4) 2,000 lockstep vectors (Bend .mjs <-> cowrie_core.wasm) + F1..F8 C-ABI checks
```

---

## 4. Honest Boundaries — Where Cowrie Defers (and Which v1.0 Limits `v1.2` Closed)

1. **Streaming decode speed (`tok/s` after token 1):** `0%` change. Autoregressive decode throughput is bound by GPU weight bandwidth and GEMM/attention kernels, never by container format.
2. **Cold archival compression of flat prose logs (`zstd + JSONL` wins):** On flat prose tables (`W3`), `zstd-3` over minified JSONL (`1,323 B`) beats `zstd-3` over binary Cowrie (`2,257 B`) because binary varints and dictionary indices reduce byte-level LZ77 repetition.
3. **Ad-hoc multi-table relational joins & wide OLAP aggregations (`SQLite` / `Parquet` win):** Cowrie indexes primary/secondary keys (`CWZM`) and vectors (`CWIV`) per shard without embedding a SQL join planner.
4. **Closed in `v1.2` — Cold `< 9%` point/range selectivity:** Previously deferred to `SQLite3`; now closed by `CWZM` 16 KB Zone-Map shards (`0.514 ms vs 1.031 ms` cold NVMe at `0.001%`, `1.58×–2.88×` faster).
5. **Closed in `v1.2` — Live sub-`5 µs` streaming mutations:** Previously required batch shard packing; now closed by hash-chained `.cwwal` + `LiveIvfStore` (`4.45 µs` append, `12.91 ms` LSM compaction).

---

## 5. Complete `v1.2.0` Integration & Verification Ledger

| Workstream | Status | What Is Built & Verified in `v1.2.0` |
|---|---|---|
| **Core Library (`cowrie-core`, `cowrie.h`, Python `cowrie`)** | **100% DONE** | Zero-dep Rust (`799 KB` `.so`), `169.8 ns` zero-alloc verifier, `TAG_TENSOR_REF (0x0A)`, `CWZM` 16 KB Zone-Maps, `.cwwal` + `LiveIvfStore` + C-ABI tombstones, pure-Rust RFC 8032 `Ed25519`, `SHA-NI` + `ARMv8 SHA2`, `HMAC-SHA256`, `12/12` Rust tests + `100%` C-ABI conformance |
| **Browser & Node WebAssembly (`wasm/cowrie.mjs` + `cowrie_core.wasm`)** | **100% DONE** | `188.7 KiB` raw / `80.7 KiB` gzipped `wasm32-unknown-unknown` (`0.57 ms` load, `1.72 µs` verify+CID), full `R0–R2` lenses, 16 KB `CWRF` + `CWSP` byte verification, and `Ed25519` signing/verification in V8 & Headless Chrome 135 |
| **Standalone MCP Firewall (`cowrie mcp-proxy`)** | **100% DONE** | Transparent stdio JSON-RPC proxy auto-tainting external tool responses (`0x04`), logging `.cwwal` audit chains, blocking tainted privileged calls in `13.2 µs`, and verifying `Ed25519` declassification tokens |
| **`vLLM 0.15.0` V1 Offloader (`cowrie_kv_connector.py` + `cowrie_vllm.py`)** | **100% DONE** | Native `KVConnectorBase_V1` (`5.64×` cross-restart speedup, `100%` greedy parity) + `CowrieVllmRuntime` with `.cwwal` WAL, 47-bit handles, `R0/R2` lenses, and `Ed25519` `declassify_tokens` |
| **`llama.cpp` + CUDA DMA (`cowrie_llama_statepage.hpp` + `O_DIRECT`)** | **100% DONE** | Split-precision `Q4_0` KV + `F32` SSM (`49.7×` cold / `123.2×` warm), `LlamaPrefixStateCache` (`13.72×` suffix reuse), `LlamaCowrieMemory` (`.cwwal` + `Ed25519`), `LlamaZonedShard` (`CWZM`), and `O_DIRECT` dual-stream DMA (`3.38 GB/s` cold) |
| **Eureka Creative v7 & Eureka LLM (`ec_cowrie.c`, `llb_cowrie.h`, `cowrie_route.h`)** | **100% DONE** | Full `v1.2` symmetry: `0700/0600` HMAC `StatePages` + longest-prefix scanning (`151.7×–2,970×` encoders, `5.00×` E2E LLM), `1.0–9.6 ms` ComfyUI DAG hits, `.cwwal` WAL logging, and `Ed25519` HTTP/C trust gates (`feat/cowrie`) |
| **Formal Verification & Lockstep Gate (`formal/PROOF.bend` + `verify_formal.py`)** | **100% DONE** | `26/26` biconditional/liveness laws proven via Bend 2.0.35 `--verdict` (`218.3 ms`), `13/13` fail-open & anti-underspecification mutants rejected, `2,000` lockstep differential vectors (`Bend .mjs` == `cowrie_core.wasm` == `libcowrie_core.so`), and `8` subtle bugs (`F1`–`F8`) caught & fixed |

## SUMMARY
Complete `v1.2.0` engineering guide covering measured receipts, 8 production use cases, copy-paste quickstarts across `CLI`, `vLLM 0.15+`, `llama.cpp`, `Eureka LLM`, `Eureka Creative v7`, `cowrie mcp-proxy`, `wasm32`, and `Bend 2.0.35 --verdict` (`26/26` laws), honest crossover limits, and full integration status.
