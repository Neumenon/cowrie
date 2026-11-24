# Designing a Data Format for the Age of AI

**Date:** 2026-11-23  
**Interactive Editorial Monograph:** `/home/omen/agent-lib/ai-data-format.html` (`http://100.108.6.13:8795/ai-data-format.html`)  
**Production Reference Repo:** `/home/omen/cowrie` (`Cowrie` — AI-Native Container Extension over `Cowrie v1`)  
**Benchmark Script & Receipts:** `/home/omen/agent-lib/ai-data-format-assets/bench_format.py` · `/home/omen/agent-lib/ai-data-format-assets/bench_receipt.json` · `/home/omen/cowrie/receipts/`

---

## 01 · The Tri-Consumer Collision

Every enduring data format was built for a single scarce machine: CSV and XML for human eyes and Unix pipelines; JSON and Protocol Buffers for HTTP APIs and microservice CPUs; Parquet, Arrow, Safetensors, and GGUF for columnar SIMD scans and memory-mapped GPU weight loads.

AI systems place three incompatible machines on the exact same artifact at once:
1. **An attention engine (LLM / agent)** whose scarce resource is context tokens and attention budget, not disk bytes.
2. **An arithmetic engine (GPU / NPU)** requiring 16 KB page-aligned raw buffers (`f32`, `bf16`, `fp8`, `i8`, packed `i4`, hybrid SSM+KV `StatePages`, and co-located IVF clusters) mapped straight into memory without parsing or copying.
3. **A deterministic verifier (CPU / cache / security runtime)** requiring bijective content hashing, O(1) seek, zero-allocation borrows, and non-forgeable authority boundaries.

Compact text formats (such as TOON) trim 25%–50% of tokens on flat tabular JSON, yet leave tensors, hashing, progressive zoom, and prompt-injection security unsolved. Storage and identity belong to one canonical binary core (**Cowrie**); human text and LLM-lean syntax belong to deterministic projection lenses.

## 02 · Six First-Principles Invariants (Cowrie)

Governing rule: **Core is physics, profiles are chemistry, lenses are optics.**

1. **Bijective Canonical Identity (`COWR`):** `encode: Value -> Bytes` is a total function with an exact inverse (`decode(encode(v)) == v` and `encode(decode(b)) == b`). UTF-8 byte-sorted key dictionaries, minimal varints, `-0.0 -> +0.0`, single quiet NaN, hard `MAX_DEPTH = 64`, byte-bounded allocations, and strict decoders (`ERR_*` ABI) guarantee identical 34-byte multihashes across every language.
2. **16 KB-Aligned Zero-Copy Tensors, `StatePages` (`CWSP`), and Co-Located IVF (`CWIV`):** Contiguous payloads sit in a strictly `16,384 B` aligned arena (`align_log2 = 14`, aligned simultaneously to Apple Silicon Unified Memory, Linux NVMe DMA, and Android ARM64) with strict `0x00` padding and mandatory Coordinate Contracts `(model_sha, kv_quant, layer, metric)`.
3. **Semantic Mipmaps (`R0`–`R3`):** Progressive attention levels of detail: `Alpha32 R0` handle (`~8–10` tokens), `R1` mechanical structural atlas with child offsets, adaptive `R2` Attention Lens (hoisting pipe tables only when `len >= 3` and schema density `>= 0.70`, eliding raw tensors to `<T:dtype[shape]@space>`), and `R3` full lossless binary core plus aligned tensors. Learned synopses and `StatePages` live outside `CoreHash` as attested satellites.
4. **The Structural Trust Membrane (`BIT_TAINTED = 0x80`):** Binary length-prefixed `COWR` frames carry an out-of-band 1-byte Authority and Epistemic bitfield (`0x01 SYS_POLICY`, `0x02 VERIFIED_TOOL`, `0x04 UNTRUSTED_EXT`, `0x08 MODEL_SYNTH`) plus sticky transitive taint (`BIT_TAINTED = 0x80`) propagated across causal parent DAGs, blocking direct and multi-hop laundered tool calls deterministically.
5. **Epistemic Uncertainty & Merkle Lineage:** Standardized profiles over Core represent calibrated confidence intervals, evidence `(cid, byte_span)` citations, and derivation DAGs so retracted or poisoned sources surgically invalidate downstream artifacts.
6. **Seek-From-End Containers & Hardware SHA-NI Merkle Seals (`CWRF`):** Compression (`zstd`, `lz4`) lives strictly at transport, never inside identity. Sealed shards end with a 12-byte trailer (`u64 footer_offset || "CWRF"`) pointing to a byte-range TOC and an RFC 6962 domain-separated, promote-odd, count-bound Merkle root, accelerated with x86_64 SHA-NI parallel 4 MiB chunked verification.

## 03 · Measured Evidence

Reproducible benchmark on four deterministic AI workloads (`bench_format.py`, 2026-11-23) plus 16 live hardware and runtime verification suites on RTX 3090:

| Workload | JSON-min B | Cowrie Core B | zstd-3 (JSON / Core) | JSON-min tok | R2 Lens tok | R1 / R0 tok |
|---|---:|---:|---:|---:|---:|---:|
| W1 · Agent Trace (12 steps) | 2,880 | **2,113** (−26.6%) | 749 / 858 | 972 | **733** (−24.6%) | 95 / **25** |
| W2 · Multimodal (1536d f32 + 64×64 i8) | 20,509 | **11,008** (−46.3%) | 10,250 / 10,632 | 10,024 | **255** (−97.5%) | 164 / **26** |
| W3 · Retrieval Table (100 rows) | 19,789 | **14,122** (−28.6%) | 1,323 / 2,257 | 5,863 | **4,364** (−25.6%) | 51 / **25** |
| W4 · Epistemic DAG (40 claims) | 8,281 | **5,155** (−37.7%) | 1,645 / 1,674 | 3,221 | **2,012** (−37.5%) | 66 / **24** |

- **Live Qwen3.5-9B Paired QA (`U1`):** `24/24 (100%)` vs `24/24` on minified JSON (`0` flips), `−65.2%` tokens (`−82.5%` on Sets B+C), `1.93×` median (`2.43×` mean; up to `4.43×`) GPU latency speedup.
- **Live Hybrid SSM+KV `StatePages` (`U2` + `v2`):** `13,800`-token `Qwen3.5-9B` workspace shrinks `505.2 MB -> 180.2 MB` (`2.80×`) under split-precision `Q4_0` KV + exact DeltaNet SSM, resuming in `64.9 ms` cold NVMe verified (`50.7×`) and `25.8 ms` warm verified (`127.6×`) with SHA-NI parallel 4 MiB Merkle verification, `KL = 3.99e-04`, `33/33` exact tokens, and `5/5` needles (`FP16` exact `KL = 0.00e+00`).
- **10M-Vector (5.30 GB) Co-Located 16 KB IVF-INT8 (`U3-v0.3`):** `0.155 ms` warm VRAM (`16.3×` vs 10M brute force) and `7.07 ms` median cold `O_DIRECT` `QD=16` NVMe (`0.390%` of disk read, `100.0%` Recall@20, `0` leaked `0x04`).
- **Cross-Runtime Live Wiring (4 Production Runtimes):** Verified via the single zero-dependency Rust C-ABI (`libcowrie_core.so`, `665 KB`, `cowrie_has_shani() == 1`) across **Eureka LLM Engine** (`feat/cowrie`: persistent `Q8_0` `StatePages` across cold process restart in `98.6 ms` + HTTP `/v1/memory/*` & `/v1/trust/gate`), **Eureka Creative Engine v7** (`creative/c0-foundation` `55d0cca` + `creative/backend-wiring` `9e344166`, optional `dlopen` bridge `creative/ec_cowrie.c`), **llama.cpp** (`prism-llama.cpp`), and **vLLM 0.15.0** (`Qwen3ForCausalLM`).

### Live Multimodal Generative Wins — Eureka Creative Engine v7 (RTX 3090 Paired Cold-Process Restart)

Receipts: `/home/omen/agent-lib/creative-proofs/c0-v7-cowrie-receipt.txt` · `c0-v7-e2e-receipt.txt` (`18/18 PASS`) · `c0-v7-supervisor-receipt.txt` (`PASS`) · `c0-v7-bench-receipt.txt` (`G-C6 PASS: 1 MB StatePage sv=0.83 ms, ld=0.52 ms, ERR_MODEL_MISMATCH=-23`) · Companion Monograph: `/home/omen/agent-lib/eureka-creative-qa-monograph.html`.

| Creative Subsystem (Pure C11/CUDA + `dlopen` `libcowrie_core.so`) | Cold Process 1 (No Cache) | Cold-Restart Process 2 (0 RAM Cache, SHA-NI `StatePage`) | Measured Gain & Contract Proof |
|---|---:|---:|---|
| **FLUX.2-klein-4B Qwen3-4B Text State (`7.86 MB` BF16)** | `91,771 ms` (HDD-streamed) / `~1,200 ms` (warm OS) | **`5.9 ms`** (`E2E.18`) / **`30.9 ms`** (cold mmap) | **`~200×` vs warm / `2,970×` vs cold HDD**; bypasses 8 GB weight stream on new seeds |
| **Wan 2.2 TI2V-5B UMT5-XXL FP8 (`2 × 4.19 MB` pos+neg)** | `3,230.8 ms` (24L FP8 × 2) | **`21.3 ms`** (verified pos+neg) | **`151.7×` faster** across cold server restart (new seed `7010`) |
| **8-Node ComfyUI FLUX.2 + ColorGrade DAG** | `98,704 ms` (`8/8` exec) | **`1.0 ms`** (`E2E.18`) / **`9.6 ms`** (cold restart) | **`8/8` hits, `0` exec** across process exit (`12` `.state.cowrie` pages, `23.31 MiB`) |
| **Canonical Key-Shuffled Downstream Tweak (Cold Restart)** | Full FLUX.2 rerun | **`9.0–11.3 ms`** (`6/8` hits + `2/8` Post-FX) | Lexicographic SHA-256 `CoreHash` + `StatePage` bypasses FLUX.2 `KSampler` |
| **Trust Membrane on External ComfyUI URLs (`/v1/flows/import`)** | Unframed JSON | **`0x84` (`UNTRUSTED_EXT\|BIT_TAINTED`) + `403`** | `shell_exec` & `fs_write` blocked (`ERR_TAINTED_EXECUTION_BLOCKED`), `read_only` & `owner_override` `200`, identical JSON/HTML URLs deduplicated to 1 CID |
| **Live Director LLM Lenses (`/v1/creative/lens`)** | `vault` `12,525 B` · `recipes` `6,964 B` · `models` `888 B` · `templates` `5,658 B` | **`R2`: `−27.0%` / `−22.6%` / `−30.4%`** · **`R1`: `−97.5%` / `−97.1%` / `−98.6%`** · **`R0`: `−99.5%` to `−99.8%`** (`28 B`) | Exact live token/byte reduction for Qwen3.5 Director routing (zero-copy `COWR` −20.6% to −42.9%) |

- **Honest ledger:** On zstd-3 compressed pure text (`W3`: `1,323 B` vs `2,257 B`), minified JSON beats binary core; below `9%` selectivity SQLite B-trees beat cold full-shard GPU scans (`0.32–2.81 ms` vs `25.4 ms`); and in Creative Studio Director lenses, `R2` expands deeply nested heterogeneous DAG graphs (`templates`: `+1.5%`) and flat string lists (`prompts`: `+6.9%`) where pipe-table hoisting does not apply—meaning Director LLMs should call `R1` (`−98.6%` / `−82.7%`) or `R0` (`−99.5%` / `−97.8%`) for discovery and `R2` (`−22.6%` to `−30.4%`) on structured tabular inventories (`vault`, `recipes`, `models`).

## SUMMARY

- AI systems break single-consumer serialization because one artifact must simultaneously serve LLM attention, GPU DMA, and strict cryptographic verification.
- **Cowrie** combines a bijective canonical core (`COWR` / `CWRF`) with 16 KB alignment, split-precision hybrid `StatePages` (`CWSP`), co-located 16 KB IVF-INT8 (`CWIV`), deterministic `Alpha32 R0->R2` lenses, and a causal-DAG `BIT_TAINTED (0x80)` Trust Membrane.
- Verified live across **four** production runtimes—**Eureka LLM Engine** (`feat/cowrie`), **Eureka Creative Engine v7** (`creative/c0-foundation` `55d0cca`, `18/18` E2E + `151.7×–2,970×` cold-restart encoder/DAG `StatePage` acceleration + `403` external ComfyUI taint blocking + `−97.5%` R1 / `−99.8%` R0 Director lenses), **llama.cpp**, and **vLLM 0.15.0**—through one zero-dependency Rust core (`/home/omen/cowrie`, `libcowrie_core.so`, `cowrie` CLI), while honestly documenting zstd pure-text, `< 9%` cold-selectivity, and non-tabular `R2` boundaries.
