# Cowrie (`v1.2.0`)

Cowrie is a 16 KB-aligned, content-addressed, provenance-tagged binary container and runtime library for AI systems. It stores canonical structured records, multi-resolution prompt projections, page-aligned GPU state checkpoints, out-of-core vector and scalar indexes, and append-only audit logs in one zero-dependency Rust core.

- **Native & WebAssembly artifacts:** Compiles offline in `< 4 s` with zero external Rust crates into `libcowrie_core.so` (`799 KB`), the `cowrie` CLI, and `cowrie_core.wasm` (`193,278 B` raw / `82,668 B` (`80.7 KiB`) gzipped, zero host imports).
- **Runtime adapters:** Ships with symmetric `v1.2.0` integrations for **vLLM 0.15+**, **llama.cpp**, **Eureka LLM**, **Eureka Creative Engine v7**, **stdio MCP servers** (`cowrie mcp-proxy`), **Python**, and **JavaScript / WebAssembly**.
- **Formal specification:** Core taint, wire bijectivity, fence escaping, zone-map pruning, Merkle domain separation, and WAL replay invariants are proved in `Bend 2.0.35` (`formal/PROOF.bend`, `26/26` laws checked in `218.3 ms`) and tested in 2,000-vector lockstep against `cowrie_core.wasm` and `libcowrie_core.so`.

## Documentation
- **Engineering Guide & Use Cases:** [`docs/GAINS-AND-USES.md`](docs/GAINS-AND-USES.md)
- **Wire & Container Specification (`v1.2.0`):** [`SPEC.md`](SPEC.md)
- **Runtime & WebAssembly Adapters:** [`adapters/README.md`](adapters/README.md)
- **Formal Verification Suite:** [`formal/README.md`](formal/README.md)
- **Archived v0 Codec:** [`Neumenon/cowrie-legacy`](https://github.com/Neumenon/cowrie-legacy)

---

## Measured Benchmarks (RTX 3090 · Ryzen 7 7700X · NVMe)

| Workload | Cowrie `v1.2.0` | Speedup / Reduction | Receipt |
|---|---:|---:|---|
| **`llama.cpp` Hybrid SSM+KV Resume** (`13.8K` tok `Qwen3.5-9B`, `Q4_0` KV + `F32` SSM) | `68.2 ms` cold / `27.2 ms` warm (`180.2 MB`) | `49.7×` cold / `123.2×` warm (`KL = 3.99e-04`) | [`receipt_shani_kvquant.json`](receipts/receipt_shani_kvquant.json) |
| **`llama.cpp` Prefix State Cache** (`1,488` tok shared prefix, new suffix query) | `9.01 ms` suffix / `46.46 ms` full | `13.72×` suffix / `2.66×` full (`100%` parity) | [`receipt_v1_2_wasm_and_parity.json`](receipts/receipt_v1_2_wasm_and_parity.json) |
| **`vLLM 0.15.0` Cross-Restart KV Offload** (`CowrieKVConnectorV1`, `11,755` tok, 12L) | `80.53 ms` (`144.1 MB` HMAC shard) | `5.64×` vs cold prefill (`100%` parity) | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **Eureka Creative v7 Encoder & DAG Cache** (`UMT5-XXL`, `FLUX Qwen3-4B`, 8-node DAG) | `21.3 ms` / `5.9–30.9 ms` / `1.0–9.6 ms` | `151.7×–2,970×` (`8/8` DAG hits) | `c0-v7-cowrie-receipt.txt` |
| **Eureka LLM Cold Restart** (`Qwen3.5-9B`, `2,800` tok E2E HTTP) | `96.3 ms` state / `308.5 ms` E2E | `5.00×` E2E across process restart | [`receipt_live_all.json`](receipts/receipt_live_all.json) |
| **16 KB `O_DIRECT` + Dual-Stream CUDA DMA** (`192 MiB` StatePage $\to$ VRAM) | `59.62 ms` cold (`3.38 GB/s`) / `26.79 GB/s` warm | `1.31×` cold NVMe-to-VRAM throughput | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **16 KB Zone-Map Shards (`CWZM`)** (`100K` rows on cold NVMe) | `0.514 ms` point (`0.001%`) / `0.542 ms` secondary | `1.58×–2.88×` vs cold `SQLite3`, `−15.0%` disk | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **10M × 512d Cold NVMe IVF (`CWIV`) + `LiveIvfStore`** (`5.30 GB` + `.cwwal`) | `7.07 ms` 10M cold / `87.5 µs` live hybrid | `9.17×` cold (`4.45 µs` WAL, `12.9 ms` compact) | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **Canonical Verifier & `R2` Lenses** (`COWR` + `0x0A TensorRef`) | `169.8 ns` 0-alloc verify (`278.7 ns` w/ CID) | `7.43×` verify; `−65%` to `−98.7%` prompt tokens | [`receipt_v1_1_hardening.json`](receipts/receipt_v1_1_hardening.json) |
| **`cowrie mcp-proxy` & `Ed25519` Gate** (`COWRIE_DECLASSIFY_V1`) | `13.15 µs` wire check / `808 µs` `Ed25519` verify | `100%` causal taint enforcement | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **WebAssembly (`cowrie_core.wasm`)** (Node v22 & Chrome 135) | `0.57 ms` load / `1.72 µs` verify + CID | `80.7 KiB` gzipped, `0` imports | [`receipt_v1_2_wasm_and_parity.json`](receipts/receipt_v1_2_wasm_and_parity.json) |

---

## Architecture

1. **Plane A — Canonical Wire Core (`canonical.rs`, `COWR`):**
   Bijective binary frames with UTF-8 byte-sorted key dictionaries, stack-inline unused-key tracking, zero-allocation wire verification (`verify_frame_zero_alloc`), typed `TAG_TENSOR_REF (0x0A)` descriptors, strict RFC 8259 JSON conversion, bounded nesting (`MAX_DEPTH = 64`), out-of-band `authority` and `epistemic` header bytes, and 34-byte SHA-256 multihash CIDs.
2. **Plane B — Progressive Attention Lenses (`lens.rs`, `R0–R3`):**
   Deterministic prompt projections computed directly from Plane A: 10-letter lowercase `Alpha32 R0` handles (`47.0` bits), `R1` structural summaries, `R2` adaptive table hoisting (`len >= 3`, key density `>= 0.70`) with `<T:dtype[shape]@space>` tensor handles and CID-bound escaped fences, and `R3` lossless extraction.
3. **Plane C — 16 KB Hardware Arena (`shard.rs`, `ivf.rs`, `wal.rs`):**
   `16,384 B`-aligned binary regions for:
   - **`CWSP` StatePages:** Split-precision `Q4_0`/`Q8_0`/`FP16` KV + `F32` recurrent SSM checkpoints with `0600` atomic writes, 64-stride `cowrie_model_fingerprint` (`1.86 ms`), `runtime_id` and dtype contract checks, optional RFC 2104 `HMAC-SHA256`, parallel 4 MiB `SHA-NI`/`ARMv8 SHA2` Merkle hashing, `mmap` / `O_DIRECT` CUDA DMA, `CowrieKVConnectorV1` (`vLLM`), and `LlamaPrefixStateCache` (`llama.cpp`).
   - **`CWIV` Co-Located IVF-INT8 & `LiveIvfStore`:** Contiguous 16 KB cluster blocks (`AVX2`/`NEON` INT8 dot products + `O_DIRECT`) paired with a SHA-256 hash-chained `.cwwal` write-ahead log and LSM compaction.
4. **Plane D — Provenance, `Ed25519`, `CWZM` Zone-Maps & Merkle (`trust.rs`, `ed25519.rs`, `zonemap.rs`):**
   Out-of-band authority flags (`0x01/0x02/0x04/0x08`), transitive causal taint (`BIT_TAINTED = 0x80`), RFC 8032 `Ed25519` scoped declassification tokens (`COWRIE_DECLASSIFY_V1`), `cowrie mcp-proxy` JSON-RPC gating, 64 B Merkle-authenticated `CWZM` page zone-maps, and dual-tree RFC 6962 Merkle footers.

---

## Quickstart

### Build & Verify
Builds default to `/tmp/cowrie-cargo-target`:

```bash
make test          # Release unit test suite
make conformance   # C-ABI conformance suite (vectors, Merkle, StatePage, Ed25519, CWAL, CWZM)
make wasm          # Build stripped cowrie_core.wasm + run Node.js WASM suite
make formal        # Run Bend 2.0.35 --verdict proofs + 2,000-vector differential lockstep
```

### CLI (`cowrie`)
```bash
COWRIE=/tmp/cowrie-cargo-target/release/cowrie
$COWRIE pack input.json out.cowrie --auth verified
$COWRIE inspect out.cowrie
$COWRIE verify out.cowrie
$COWRIE lens out.cowrie --tier r2
$COWRIE state-inspect session.state.cowrie --verify-hash
$COWRIE model-fp weights.gguf
$COWRIE ed25519-pubkey <64-hex-seed>
$COWRIE declassify-sign <64-hex-seed> <68-hex-cid> shell_exec <expiry_unix>
$COWRIE mcp-proxy --owner-pubkey <64-hex-pk> --audit-wal session.cwwal -- <mcp-server-cmd>
```

---

## Repository Layout

```text
cowrie/
├── crates/cowrie-core/   # Core library (canonical, lens, shard, ivf, wal, zonemap, trust, ed25519, sha256, ffi, wasm)
├── crates/cowrie-cli/    # CLI binary and `cowrie mcp-proxy`
├── python/cowrie/        # ctypes bindings and standalone conformance runner
├── wasm/                 # Zero-import ES module wrapper (`cowrie.mjs`) and WASM test suite
├── adapters/             # vLLM 0.15+, llama.cpp, Eureka LLM, and Eureka Creative v7 adapters
├── formal/               # Bend 2.0.35 specification, machine-checked proofs, and differential runner
├── docs/GAINS-AND-USES.md# Engineering guide, use cases, runtime quickstarts, and workload fit
├── receipts/             # Benchmark and verification receipts
└── SPEC.md               # Cowrie v1.2.0 wire and container specification
```
