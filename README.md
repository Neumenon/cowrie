# Cowrie — AI-Native Binary Container (v1.2.0)

> **Core is physics, profiles are chemistry, lenses are optics.**

Cowrie (`v1.2.0`) is a **16 KB-aligned, content-addressed, trust-tagged binary container and runtime substrate** for local, cloud, and browser AI stacks. Zero external Rust crate dependencies (`libcowrie_core.so` = `799 KB`; `cowrie_core.wasm` = `188.9 KiB` raw / `80.4 KiB` gzipped); compiles offline in `< 4 s` across native and `wasm32-unknown-unknown`.

- **Measured Gains, Use Cases & Integration Playbook:** [`docs/GAINS-AND-USES.md`](docs/GAINS-AND-USES.md)
- **Frozen Wire & Container Specification (v1.2):** [`SPEC.md`](SPEC.md)
- **Symmetric Runtime & Browser Adapters (`vLLM`, `llama.cpp`, `Eureka`, `wasm32`):** [`adapters/README.md`](adapters/README.md)
- **Formal Proofs & Lockstep Differential Gate (`Bend 2.0.35 --verdict`):** [`formal/README.md`](formal/README.md)
- **Legacy v0 Codec Archive:** [`Neumenon/cowrie-legacy`](https://github.com/Neumenon/cowrie-legacy)

## Architecture (Four Planes + v1.2 Live Stack)
- **Plane A (`canonical.rs`):** Bijective canonical binary frames (`COWR`) with UTF-8 byte-sorted key dictionaries, stack-bitset unused-key verification, zero-allocation wire verifier (`verify_frame_zero_alloc`, `169.8 ns`), native typed `TAG_TENSOR_REF (0x0A)` Coordinate Contracts (`−47.4%` bytes), strict RFC 8259 / UTF-16 surrogate-safe JSON bridge, hard `MAX_DEPTH = 64`, byte-bounded allocations, out-of-band `authority`/`epistemic` header bytes, and 34-byte SHA-256 multihash CIDs.
- **Plane B (`lens.rs`):** Pure-function progressive attention views — 10-letter `Alpha32 R0` handles (`47-bit` collision space, zero `\p{N}` digit fragmentation), `R1` structural atlas, and adaptive `R2` (`len >= 3`, density `>= 0.70`, `<T:dtype[shape]@space>` tensor handles, CID-bound trust fences with `<<<`/`>>>` breakout escaping).
- **Plane C (`shard.rs`, `ivf.rs`, `wal.rs`):** Strictly `16,384 B` aligned contiguous hardware arena (`CWRF`, `CWIV`, `CWAL`) for:
  - **Hybrid SSM+KV `StatePages` (`CWSP`):** Split-precision `Q4_0`/`Q8_0`/`FP16` KV + exact recurrent SSM, `O_CREAT|O_EXCL` `0600` atomic save + parent `fsync`, zero-copy `mmap` + `O_DIRECT` dual-stream `cudaHostAlloc` DMA (`3.38 GB/s` cold NVMe, `26.79 GB/s` warm PCIe Gen4), 64-stride `cowrie_model_fingerprint` (`1.86 ms`), RFC 2104 `HMAC-SHA256`, x86_64 `SHA-NI` + ARMv8 `SHA2` parallel 4 MiB Merkle over `WorkerPool`, and native **`vLLM 0.15+` `CowrieKVConnectorV1`** (`5.64×` cross-restart restore).
  - **Co-Located 16 KB IVF-INT8 (`CWIV`) + `LiveIvfStore`:** Single-read `O_DIRECT` QD=16 cold NVMe search (`7.07 ms` on `5.30 GB` / `10M x 512d`, `AVX2`/`NEON` INT8 SIMD) paired with a crash-safe hash-chained **`.cwwal` delta tail** (`4.45 µs` append, `87.5 µs` hybrid search, `12.9 ms` LSM compaction).
- **Plane D (`trust.rs`, `ed25519.rs`, `zonemap.rs`):** Out-of-band authority bitmask (`0x01/0x02/0x04/0x08`), sticky transitive causal taint (`BIT_TAINTED = 0x80`), pure-Rust RFC 8032 **`Ed25519` owner-declassification tokens** (`COWRIE_DECLASSIFY_V1`), transparent **`cowrie mcp-proxy`** stdio JSON-RPC wire firewall (`13.2 µs` taint block), **16 KB Zone-Map shards (`CWZM`)** (`1.58×–2.88×` faster than cold `SQLite3` B-trees down to `0.001%` point lookups), and dual-tree RFC 6962 count-bound Merkle roots.

## Build, Test, WebAssembly & Standalone Conformance (Scratch Target Dir)

```bash
CARGO_TARGET_DIR=/tmp/cowrie-cargo-target cargo test --release
COWRIE_LIB_PATH=/tmp/cowrie-cargo-target/release/libcowrie_core.so python3 python/cowrie/conformance.py
make wasm && make formal
```

## CLI (`cowrie`)

```bash
COWRIE=/tmp/cowrie-cargo-target/release/cowrie
$COWRIE pack input.json out.cowrie --auth verified
$COWRIE inspect out.cowrie
$COWRIE verify out.cowrie
$COWRIE lens out.cowrie --tier r0   # or r1 / r2
$COWRIE state-inspect workspace.state.cowrie --verify-hash
$COWRIE model-fp weights.gguf
$COWRIE ed25519-pubkey <64-hex-seed>
$COWRIE declassify-sign <64-hex-seed> <68-hex-cid> shell_exec <expiry_unix>
$COWRIE mcp-proxy --owner-pubkey <64-hex-pk> --audit-wal session.cwwal -- <mcp-server-cmd>
```
