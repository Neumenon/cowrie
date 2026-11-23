# Cowrie — AI-Native Binary Container

> **Core is physics, profiles are chemistry, lenses are optics.**

Cowrie is a **16 KB-aligned, content-addressed, trust-tagged binary container** for local AI runtimes. Zero external Rust crate dependencies; compiles offline in `< 3 s`.

- **Measured Gains, Use Cases & Integration Playbook:** [`docs/GAINS-AND-USES.md`](docs/GAINS-AND-USES.md)
- **Frozen Wire & Container Specification:** [`SPEC.md`](SPEC.md)
- **Legacy v0 Codec Archive:** [`Neumenon/cowrie-legacy`](https://github.com/Neumenon/cowrie-legacy)

## Architecture (Four Planes)
- **Plane A (`canonical.rs`):** Bijective canonical binary frames (`COWR`) with UTF-8 byte-sorted key dictionaries, minimal ints/varints, canonical `+0.0`/qNaN, hard `MAX_DEPTH = 64`, byte-bounded allocations, out-of-band `authority`/`epistemic` header bytes, and 34-byte SHA-256 multihash CIDs.
- **Plane B (`lens.rs`):** Pure-function progressive attention views — `Alpha32 R0` handles, `R1` structural atlas, and adaptive `R2` (`len >= 3`, density `>= 0.70`, `<T:dtype[shape]@space>` tensor handles, CID-bound trust fences).
- **Plane C (`shard.rs`, `ivf.rs`):** Strictly `16,384 B` aligned contiguous hardware arena (`CWRF`) for:
  - **Hybrid SSM+KV `StatePages` (`CWSP`):** Split-precision `Q4_0`/`Q8_0`/`FP16` KV + exact recurrent SSM, atomic save (`tmp -> fsync -> rename`), zero-copy `mmap` restore, hardware x86_64 SHA-NI parallel 4 MiB RFC 6962 Merkle verification (`1.92 GiB/s` single-thread, `< 0.6 ms` warm overhead on `505 MB`), and strict `ERR_MODEL_MISMATCH` / `ERR_KV_QUANT_MISMATCH` / `ERR_STATEPAGE_HASH_MISMATCH` enforcement.
  - **Co-Located 16 KB IVF-INT8 (`CWIV`):** Packs `[embs_c || meta_c || 0x00 pad -> 16 KB]` per cluster for single-read `O_DIRECT` QD=16 cold NVMe search (`7.07 ms` median on `5.30 GB` / `10M x 512d`, `0.390%` of disk, `100%` Recall@20, `0` leaked `0x04`).
- **Plane D (`trust.rs`):** Out-of-band authority bitmask (`0x01/0x02/0x04/0x08`), sticky transitive causal taint (`BIT_TAINTED = 0x80`), RFC 6962 count-bound Merkle root, and 12-byte seek-from-end trailer (`u64 footer_off || "CWRF"`).

## Build, Test & Standalone Conformance (Scratch Target Dir)

```bash
make test
make conformance
```

## Runtime Adapters (`adapters/`)
- **Eureka Engine (`feat/cowrie`):** Native persistent `StatePages` (`host/llb_cowrie.h`) in `engine_gpu` + native HTTP Agent Memory & Causal Trust Membrane (`desktop/engine/metal/src/cowrie_route.h`) in `llb-serve`.
- **llama.cpp (`adapters/llama-statepage/`):** Drop-in C++ `cowrie_llama_statepage.hpp` (`Q4_0` KV + Exact SSM on `13.8K`-token `Qwen3.5-9B`).
- **vLLM (`adapters/vllm/`):** `CowrieMemoryStore` + `CowrieVllmRuntime` (`Alpha32 R0->R2`, canonical `CoreHash` prefix caching, causal `BIT_TAINTED` gate).

## CLI (`cowrie`)

```bash
COWRIE=/tmp/cowrie-cargo-target/release/cowrie
$COWRIE pack input.json out.cowrie --auth verified
$COWRIE inspect out.cowrie
$COWRIE verify out.cowrie
$COWRIE lens out.cowrie --tier r0   # or r1 / r2
$COWRIE state-inspect workspace.state.cowrie --verify-hash
```
