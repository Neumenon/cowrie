# Cowrie — AI-Native Binary Container & Runtime Substrate (`v1.2.0`)

> **Core is physics, profiles are chemistry, lenses are optics.**

Cowrie (`v1.2.0`) is a **16 KB-aligned, content-addressed, trust-tagged binary container and runtime substrate** built for the three hardware consumers that collide on modern AI data: **LLM attention engines** (paying per token), **GPU/NPU tensor & NVMe DMA pipelines** (requiring `16,384 B` page alignment and zero-copy `mmap`/`O_DIRECT`), and **CPU security & cache verifiers** (requiring zero-allocation canonical hashing, causal taint tracking, and `Ed25519` auditability).

- **Zero external Rust crate dependencies:** compiles offline in `< 4 s` into `libcowrie_core.so` (`799 KB`), `cowrie` CLI, and zero-import `wasm32-unknown-unknown` (`cowrie_core.wasm` = `193,278 B` raw / `82,668 B` (`80.7 KiB`) gzipped).
- **Symmetric 4-runtime `v1.2` parity:** live and receipted across **vLLM 0.15.0**, **llama.cpp**, **Eureka LLM**, and **Eureka Creative Engine v7**, plus standalone **`cowrie mcp-proxy`** and **Browser/Node WebAssembly**.
- **Non-bolt-on formal verification (`formal/`):** `26/26` biconditional, liveness-preserving, end-to-end laws machine-checked by **Bend 2.0.35 `--verdict`** (Lean-proven BendTT kernel) in `218.3 ms`, `13/13` fail-open and anti-underspecification mutants rejected, and `2,000` differential lockstep vectors executed directly from compiled `CowrieKernel.bend` (`.mjs`) against `cowrie_core.wasm` and `libcowrie_core.so`.

## Documentation Map
- **Engineering Guide, Use Cases & Copy-Paste Playbook:** [`docs/GAINS-AND-USES.md`](docs/GAINS-AND-USES.md)
- **Frozen Wire & Container Specification (`v1.2.0`):** [`SPEC.md`](SPEC.md)
- **Runtime & WebAssembly Adapters (`vLLM`, `llama.cpp`, `Eureka`, `wasm32`):** [`adapters/README.md`](adapters/README.md)
- **Formal Proofs & Lockstep Differential Gate (`Bend 2.0.35`):** [`formal/README.md`](formal/README.md)
- **Legacy v0 Codec Archive:** [`Neumenon/cowrie-legacy`](https://github.com/Neumenon/cowrie-legacy)

---

## Headline Measured Results (RTX 3090 + Ryzen 7 7700X + NVMe)

| Subsystem | Baseline | Cowrie `v1.2.0` | Measured Gain | Receipt |
|---|---:|---:|---:|---|
| **`llama.cpp` Hybrid SSM+KV Resume** (`13.8K` tok `Qwen3.5-9B`, `Q4_0` KV + `F32` SSM) | `3,391.5 ms` (`505 MB` FP16) | **`68.2 ms`** cold / **`27.2 ms`** warm (`180 MB`) | **`49.7×` cold / `123.2×` warm** (`KL = 3.99e-04`) | [`receipt_shani_kvquant.json`](receipts/receipt_shani_kvquant.json) |
| **`llama.cpp` Cross-Process Prefix/Suffix Reuse** (`LlamaPrefixStateCache`, `1,488` tok) | `123.63 ms` cold prefill | **`9.01 ms`** new suffix (`46.46 ms` full) | **`13.72×` suffix / `2.66×` full** (`100%` parity) | [`receipt_v1_2_wasm_and_parity.json`](receipts/receipt_v1_2_wasm_and_parity.json) |
| **`vLLM 0.15.0` Cross-Restart `CowrieKVConnectorV1`** (`11,755` tok, 12-layer paged KV) | `453.88 ms` cold prefill | **`80.53 ms`** single HMAC shard | **`5.64×`** across engine kill (`100%` parity) | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **Eureka Creative v7 Multimodal & ComfyUI DAG** (`UMT5-XXL`, `FLUX Qwen3-4B`, 8-node DAG) | `3.23 s` / `91.8 s` / `98.7 s` | **`21.3 ms`** / **`5.9–30.9 ms`** / **`1.0–9.6 ms`** | **`151.7×–2,970×`** (`8/8` DAG hits) | `c0-v7-cowrie-receipt.txt` |
| **Eureka LLM Cold Process Restart** (`Qwen3.5-9B`, `2,800` tok E2E HTTP + `.cwwal` + `Ed25519`) | `1,541.6 ms` E2E HTTP | **`96.3 ms`** state / **`308.5 ms`** E2E | **`5.00×`** E2E across process kill | [`receipt_live_all.json`](receipts/receipt_live_all.json) |
| **16 KB `O_DIRECT` + Dual-Stream CUDA DMA** (`192 MiB` StatePage $\to$ RTX 3090) | `77.88 ms` (`2.59 GB/s`) | **`59.62 ms`** cold (`3.38 GB/s`) / **`26.79 GB/s`** warm | **`1.31×`** faster cold NVMe-to-VRAM | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **16 KB Zone-Map Shards (`CWZM`) vs Cold `SQLite3`** (`100K` rows on NVMe post-`FADV_DONTNEED`) | `1.031 ms` PK / `1.562 ms` `ts` | **`0.514 ms`** PK (`0.001%`) / **`0.542 ms`** `ts` | **`1.58×–2.88×` faster**, `−15.0%` disk | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **10M × 512d Cold NVMe Search (`CWIV`) + `LiveIvfStore`** (`5.30 GB` + `.cwwal` delta) | `64.81 ms` split-seek | **`7.07 ms`** 10M cold; **`87.5 µs`** live hybrid | **`9.17×` cold** (`4.45 µs` WAL, `12.9 ms` compact) | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **Adaptive `R2` Lenses & Zero-Alloc Verifier** (`COWR` + `0x0A TensorRef`) | `1,261.5 ns` decode; JSON | **`169.8 ns`** 0-alloc verify; **`−65%` to `−98.7%`** tok | **`7.43×` verify**; **`1.93×–4.43×` QA** (`24/24`) | [`receipt_v1_1_hardening.json`](receipts/receipt_v1_1_hardening.json) |
| **`cowrie mcp-proxy` + Pure-Rust `Ed25519` (`COWRIE_DECLASSIFY_V1`)** | Unfiltered stdio MCP | **`13.15 µs`** wire block (`-32025`); **`808 µs`** verify | **`100%`** direct & 2-hop taint blocked | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **Browser & Node `wasm32` (`cowrie_core.wasm`)** | Multi-MB JS/WASM bundle | **`80.7 KiB` gzipped** (`0.57 ms` load, `1.72 µs` verify) | **Zero imports**, verified in Chrome 135 & Node v22 | [`receipt_v1_2_wasm_and_parity.json`](receipts/receipt_v1_2_wasm_and_parity.json) |
| **`Bend 2.0.35 --verdict` Formal Proofs + Lockstep Gate (`formal/`)** | Unproved edge paths | **`26/26` laws** (`218.3 ms`) + **`2,000` lockstep vectors** | **`8` bugs (`F1`–`F8`) closed**, `13/13` mutants rejected | [`receipt_v1_2_formal_bend.json`](receipts/receipt_v1_2_formal_bend.json) |

---

## Architecture (Four Orthogonal Planes)

1. **Plane A — Bijective Canonical Core (`canonical.rs`, `COWR`):**
   UTF-8 byte-sorted key dictionaries, stack-inline `[u64; 4]` unused-key bitset (`ERR_UNUSED_DICT_KEY`), zero-allocation wire verifier (`verify_frame_zero_alloc`, `169.8 ns`), native typed `TAG_TENSOR_REF (0x0A)` Coordinate Contracts (`−47.4%` bytes), strict RFC 8259 / UTF-16 surrogate-safe JSON bridge (failing closed on partial `$tensor`/`$bytes` sentinels), `MAX_DEPTH = 64`, byte-bounded allocations, out-of-band `authority`/`epistemic` bytes, and 34-byte SHA-256 multihash CIDs.
2. **Plane B — Progressive Attention Lenses (`lens.rs`, `R0–R3`):**
   Zero-inference deterministic views: 10-letter lowercase `Alpha32 R0` handles (`47.0` bits, `~12M` birthday bound, zero `\p{N}` digit fragmentation), `R1` structural atlas, and adaptive `R2` (`len >= 3`, key density `>= 0.70`, `<T:dtype[shape]@space>` pointers, CID-bound trust fences, and a formally verified 3-state Mealy transducer escaping `<<<`/`>>>` across scalar values, hoisted table column headers, and `<T:...>` boundaries).
3. **Plane C — 16 KB Hardware Arena (`shard.rs`, `ivf.rs`, `wal.rs`):**
   Universal `16,384 B` (`align_log2 = 14`) contiguous pages for:
   - **`CWSP` StatePages:** Split-precision `Q4_0`/`Q8_0`/`FP16` KV + exact recurrent SSM, `O_CREAT|O_EXCL` `0600` atomic writes + parent `fsync`, strict `model_sha` (`64`-stride `1.86 ms` fingerprint) + `runtime_id` + `kv_quant` + `ssm_dtype` + optional RFC 2104 `HMAC-SHA256`, parallel 4 MiB `SHA-NI`/`ARMv8 SHA2` Merkle over `WorkerPool`, `O_DIRECT` dual-stream CUDA DMA, and native `vLLM` `CowrieKVConnectorV1` / `LlamaPrefixStateCache`.
   - **`CWIV` Co-Located IVF-INT8 + `LiveIvfStore`:** Single-read `O_DIRECT` QD=16 cold NVMe search (`AVX2`/`NEON` INT8 dot products) paired with a crash-safe hash-chained **`.cwwal` write-ahead log** (sequential tombstone & re-insertion replay + `12.9 ms` LSM compaction).
4. **Plane D — Trust Membrane, `Ed25519`, `CWZM` Zone-Maps & Dual-Tree Merkle (`trust.rs`, `ed25519.rs`, `zonemap.rs`):**
   Out-of-band authority bitmask (`0x01/0x02/0x04/0x08`), sticky causal taint (`BIT_TAINTED = 0x80`), pure-Rust RFC 8032 `Ed25519` owner-declassification signatures (`COWRIE_DECLASSIFY_V1`), `cowrie mcp-proxy` stdio JSON-RPC firewall, 64 B Merkle-authenticated `CWZM` Zone-Maps with monotonic sorted fences, and contiguous dual-tree `0x02`/`0x04` RFC 6962 Merkle footers.

---

## Repository Layout

```text
cowrie/
├── crates/
│   ├── cowrie-core/      # Zero-dep Rust core (canonical, lens, shard, ivf, wal, zonemap, trust, ed25519, sha256, ffi, wasm)
│   └── cowrie-cli/       # `cowrie` CLI + `cowrie mcp-proxy` stdio JSON-RPC firewall
├── python/cowrie/        # Zero-dep ctypes bindings + standalone C-ABI conformance suite
├── wasm/                 # Zero-import `cowrie.mjs` ES module wrapper + `test_wasm.mjs` (Node v22 & Chrome 135)
├── adapters/             # Symmetric v1.2 adapters: vLLM 0.15+, llama.cpp, Eureka LLM, Eureka Creative v7
├── formal/               # Bend 2.0.35 `CowrieKernel.bend`, `LAWS.bend` (26 laws), `PROOF.bend`, 13 mutants & 2,000-vector lockstep
├── docs/GAINS-AND-USES.md# Full engineering guide, 8 use cases, copy-paste quickstarts & honest boundaries
├── receipts/             # Hardware, security, WASM, 4-runtime parity & Bend formal verification receipts
└── SPEC.md               # Frozen Cowrie v1.2.0 wire & container specification
```

---

## Build, Test, WebAssembly & Formal Verification

All builds default to scratch `/tmp/cowrie-cargo-target` so the source tree stays clean:

```bash
make test          # 12/12 Rust unit & security regression tests (release)
make conformance   # Standalone C-ABI golden vectors, DoS bombs, bitflips, HMAC, Ed25519, CWAL & CWZM
make wasm          # Build stripped cowrie_core.wasm (80.7 KiB gzip) + run Node v22 WASM conformance
make formal        # Run Bend 2.0.35 --verdict (26/26 laws), 13/13 mutants, 2,000 lockstep vectors & F1-F8 C-ABI checks
```

## CLI Quickstart (`cowrie`)

```bash
COWRIE=/tmp/cowrie-cargo-target/release/cowrie
$COWRIE pack input.json out.cowrie --auth verified
$COWRIE inspect out.cowrie
$COWRIE verify out.cowrie
$COWRIE lens out.cowrie --tier r0        # 10-letter 47-bit Alpha32 handle (or --tier r1 / r2)
$COWRIE state-inspect ws.state.cowrie --verify-hash
$COWRIE model-fp weights.gguf            # 1.86 ms 64-stride SHA-NI model fingerprint
$COWRIE ed25519-pubkey <64-hex-seed>
$COWRIE declassify-sign <64-hex-seed> <68-hex-cid> shell_exec <expiry_unix>
$COWRIE mcp-proxy --owner-pubkey <64-hex-pk> --audit-wal session.cwwal -- <mcp-server-cmd>
```
