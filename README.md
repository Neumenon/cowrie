# Cowrie (`v1.2.0`)

Cowrie is a 16 KB-aligned, content-addressed, provenance-tagged binary container and runtime library for AI systems. It stores canonical structured records, multi-resolution prompt projections, page-aligned GPU state checkpoints, out-of-core vector and scalar indexes, and append-only audit logs in one zero-dependency Rust core.

- **Native & WebAssembly artifacts:** Compiles offline in `< 5 s` with zero external Rust crates into `libcowrie_core.so`, the `cowrie` CLI, and `cowrie_core.wasm` (`230,085 B` raw / `93,702 B` (`91.5 KiB`) gzipped, zero host imports).
- **Runtime adapters:** Ships with symmetric `v1.2.0` integrations for **vLLM 0.15+**, **llama.cpp**, **Eureka LLM**, **Eureka Creative Engine v7**, **stdio MCP servers** (`cowrie mcp-proxy`), **Python**, and **JavaScript / WebAssembly**.
- **Formal specification & independent oracle:** Core taint, wire bijectivity, fence escaping, zone-map pruning, Merkle domain separation, and WAL replay invariants are proved in `Bend 2.0.35` (`formal/PROOF.bend`, `26/26` laws checked in `259.3 ms` median), tested in 2,000-vector lockstep against `cowrie_core.wasm` + 100 native C-ABI trials, and cross-checked against an independent pure-Python `COWR v1` verifier ([`tests/independent_cowr_oracle.py`](tests/independent_cowr_oracle.py)).

## Documentation
- **Interactive Systems Monograph (`Making Cowrie`):** [`docs/making-cowrie.html`](docs/making-cowrie.html)
- **Engineering Guide & Use Cases:** [`docs/GAINS-AND-USES.md`](docs/GAINS-AND-USES.md)
- **Wire & Container Specification (`v1.2.0`):** [`SPEC.md`](SPEC.md)
- **Systems Architecture Brief:** [`docs/ai-data-format.html`](docs/ai-data-format.html)
- **Runtime & WebAssembly Adapters:** [`adapters/README.md`](adapters/README.md)
- **Formal Verification Suite:** [`formal/README.md`](formal/README.md)
- **Archived v0 Codec:** [`Neumenon/cowrie-legacy`](https://github.com/Neumenon/cowrie-legacy)

---

## Measured Benchmarks (RTX 3090 · Ryzen 7 7700X · NVMe)

| Workload | Cowrie `v1.2.0` | Speedup / Reduction | Receipt |
|---|---:|---:|---|
| **`llama.cpp` Hybrid SSM+KV Resume** (`13.8K` tok `Qwen3.5-9B`, `Q4_0` KV + `F32` recurrent state*) | `68.2 ms` cold / `27.2 ms` warm verified (`180.2 MB`) | `49.7×` cold / `124.5×` warm verified (`123.2×` trusted `27.5 ms`, `KL = 3.99e-04`) | [`receipt_shani_kvquant.json`](receipts/receipt_shani_kvquant.json) |
| **`llama.cpp` Prefix State Cache** (`1,488` tok shared prefix, new suffix query) | `9.01 ms` suffix / `46.46 ms` full | `13.72×` suffix / `2.66×` full (`100%` parity) | [`receipt_v1_2_wasm_and_parity.json`](receipts/receipt_v1_2_wasm_and_parity.json) |
| **`vLLM 0.15.0` Cross-Restart KV Offload** (`CowrieKVConnectorV1`, `11,755` tok, 12L) | `80.53 ms` (`144.1 MB` HMAC shard) | `5.64×` vs cold prefill (`100%` parity) | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **Eureka Creative v7 Encoder & DAG Cache** (`UMT5-XXL`, `FLUX Qwen3-4B`, 8-node DAG) | `21.3 ms` / `5.9–30.9 ms` / `1.0–9.6 ms` | `151.7×–2,970×` (`8/8` DAG hits) | [`c0-v7-cowrie-receipt.txt`](receipts/c0-v7-cowrie-receipt.txt) |
| **Eureka LLM Cold Restart** (`Qwen3.5-9B`, `2,800` tok E2E HTTP) | `96.3 ms` state / `308.5 ms` E2E | `5.00×` E2E across process restart | [`receipt_live_all.json`](receipts/receipt_live_all.json) |
| **16 KB `O_DIRECT` + Dual-Stream CUDA DMA** (`192 MiB` StatePage $\to$ VRAM) | `59.62 ms` cold (`3.38 GB/s`) / `26.79 GB/s` warm | `1.31×` cold NVMe-to-VRAM throughput | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **16 KB Zone-Map Shards (`CWZM`)** (`100K` rows on cold NVMe, clustered fields) | `0.514 ms` point (`0.001%`) / `0.542 ms` secondary | `1.58×–2.88×` vs cold `SQLite3`, `−15.0%` disk | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **10M × 512d Cold NVMe IVF (`CWIV`) + `LiveIvfStore`** (`5.30 GB` + `.cwwal`†) | `7.07 ms` 10M CUDA+`O_DIRECT` prototype / `87.5 µs` Rust live hybrid | `4.45 µs` WAL append, `12.9 ms` Rust compact | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **Canonical Verifier & `R2` Lenses** (`COWR` + `0x0A TensorRef`) | `169.8 ns` 0-alloc verify (`278.7 ns` w/ CID) | `7.43×` verify; `−65%` to `−98.7%` prompt tokens | [`receipt_v1_1_hardening.json`](receipts/receipt_v1_1_hardening.json) |
| **`cowrie mcp-proxy` & `Ed25519` Gate** (`COWRIE_DECLASSIFY_V1`) | `13.15 µs` wire check / `808 µs` `Ed25519` verify | `100%` causal taint enforcement | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **`mcp-proxy --virtualize` + `R2-Compact`** (`Qwen3.5-9B`, 16 real `15–68 KB` payloads + 4 `<1 KB`) | `16/16` vs `11/16` inline (`17.9 s` vs `58.5 s`); `−89.1%` gross tok (`−94.4%` net new) | **`3.27×` E2E speedup**, `+5` paired accuracy wins, `0` hijacks, `−31.2%` `<1 KB` payload tok | [`receipt_v1_2_b2_live_paired.json`](receipts/receipt_v1_2_b2_live_paired.json) |
| **WebAssembly (`cowrie_core.wasm`)** (Node v22) | `0.68 ms` load / `1.85 µs` verify + CID | `91.5 KiB` gzipped, `0` imports | [`receipt_v1_2_wasm_and_parity.json`](receipts/receipt_v1_2_wasm_and_parity.json) |

> *Notes on historical receipts:* `\*` In the historical `receipt_shani_kvquant.json` capture, `bench_shani_kvquant.cpp` stamped the header enum `COWRIE_SSM_FP16` onto `llama.cpp`'s internal F32 recurrent payload; the harness now passes `COWRIE_SSM_FP32`. `†` The `7.07 ms` 10M-vector cold NVMe IVF result comes from the CUDA + `O_DIRECT` prototype benchmark (`receipt_u3_v03.json`, 20 synthetic centroid-adjacent queries); the shipped Rust `CWIV` reader uses buffered `pread` + AVX2/NEON CPU scoring.

---

## Architecture

1. **Plane A — Canonical Wire Core (`canonical.rs`, `COWR`):**
   Bijective binary frames with UTF-8 byte-sorted key dictionaries, stack-inline unused-key tracking, zero-allocation wire verification (`verify_frame_zero_alloc`), typed `TAG_TENSOR_REF (0x0A)` descriptors, repack-stable `resolved_frame_cid` committing to frame + referenced Plane C tensor Merkle digests, strict lossless RFC 8259 JSON conversion (`$bytes`, `$tensor`, `$float`, `$map`), bounded nesting (`MAX_DEPTH = 64`), out-of-band `authority` and `epistemic` header bytes, and 34-byte SHA-256 multihash CIDs.
2. **Plane B — Progressive Attention Lenses (`lens.rs`, `R0–R3`) & Field Extraction:**
   Deterministic projections computed directly from Plane A (rejecting `tier > 3` uniformly across Rust/C/Python/WASM/CLI): 10-letter `Alpha32 R0` handles (`47.0` bits) with exact `authority_label`, `R1` structural summaries, `R2` adaptive table hoisting (`len >= 3`, density `>= 0.70`, distinct `null`/`<missing>`/`""` cells, exact inline integer arrays, `<T:dtype[shape]@space>` tensor handles, and CID-bound escaped fences), `R3` 100% lossless canonical typed JSON extraction, and `extract_field` / `cowrie get` exact path lookup (`/a/0/b` or `a[0].b`).
3. **Plane C — 16 KB Hardware Arena (`shard.rs`, `ivf.rs`, `wal.rs`):**
   `16,384 B`-aligned binary regions for:
   - **`CWSP` StatePages:** Split-precision `Q4_0`/`Q8_0`/`FP16` KV + `F32` recurrent SSM checkpoints with `0600` atomic writes, 64-stride `cowrie_model_fingerprint` (`1.86 ms`), `runtime_id`/`doc_cid`/dtype contract checks, authenticate-first RFC 2104 `HMAC-SHA256`, parallel 4 MiB `SHA-NI`/`ARMv8 SHA2` Merkle hashing, `mmap` / `O_DIRECT` CUDA DMA, `CowrieKVConnectorV1` (`vLLM`, multi-process index sync, LoRA/tenant/TP/PP scoping), and `LlamaPrefixStateCache` (`llama.cpp`).
   - **`CWIV` Co-Located IVF-INT8 & `LiveIvfStore`:** Contiguous 16 KB cluster blocks (`AVX2`/`NEON` INT8 dot products) paired with a SHA-256 hash-chained `.cwwal` write-ahead log (`ver=1/2` monotonic `base_seq`), bounded-memory streaming compaction, and an atomic `.cwgen` generation manifest preventing crash-window double-counting.
4. **Plane D — Provenance, `Ed25519`, `CWZM` Zone-Maps & Merkle (`trust.rs`, `ed25519.rs`, `zonemap.rs`):**
   Out-of-band authority flags (`0x01/0x02/0x04/0x08`), transitive causal taint (`BIT_TAINTED = 0x80`), RFC 8032 `Ed25519` scoped declassification tokens (`COWRIE_DECLASSIFY_V1`), fail-closed `cowrie mcp-proxy` with host `--policy`, `_cowrie_parents`, and durable audit WAL, 64 B Merkle-authenticated `CWZM` page zone-maps, and dual-tree RFC 6962 Merkle footers.

---

## Quickstart

### Build & Verify
Builds default to `/tmp/cowrie-cargo-target`:

```bash
make test          # Release unit test suite (14/14)
make conformance   # C-ABI + independent pure-Python COWR oracle conformance suite
make wasm          # Build stripped cowrie_core.wasm + run Node.js WASM suite
make formal        # Run Bend 2.0.35 --verdict proofs + 2,000-vector WASM + 100 native C-ABI lockstep
```

### CLI (`cowrie`)
```bash
COWRIE=/tmp/cowrie-cargo-target/release/cowrie
$COWRIE pack input.json out.cowrie --auth verified       # 16 KB aligned CWRF shard (add --raw for compact COWR frame)
$COWRIE pack-batch batch.cowrie --jsonl records.jsonl    # Pack multiple records into one 16 KB shard
$COWRIE inspect out.cowrie
$COWRIE verify out.cowrie
$COWRIE lens out.cowrie --tier r2                        # r0 | r1 | r2 | r2-compact | r3 (lossless)
$COWRIE get out.cowrie rows[0].id                        # Exact field extraction (dot-bracket or /rows/0/id)
$COWRIE state-inspect session.state.cowrie --verify-hash
$COWRIE model-fp weights.gguf
$COWRIE keygen --out-seed-file owner.seed                # 0600 secret seed file
$COWRIE ed25519-pubkey --seed-file owner.seed
$COWRIE declassify-sign --seed-file owner.seed <68-hex-cid> shell_exec <expiry_unix>
$COWRIE mcp-proxy --owner-pubkey <64-hex-pk> --policy policy.json --audit-wal session.cwwal --virtualize -- <mcp-server-cmd>
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
