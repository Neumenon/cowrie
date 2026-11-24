# Making Cowrie — Format Divergence, Complete v1.2 Architecture, 15 Wins, 4-Runtime Parity & WebAssembly

**Interactive Monograph:** [`making-cowrie.html`](making-cowrie.html) (`http://100.108.6.13:8795/making-cowrie.html`)  
**Repository & Spec:** `https://github.com/Neumenon/cowrie` (`v1.2.0`, legacy archived at `Neumenon/cowrie-legacy`)  
**Updated:** 2026-11-23

## Structure (`00`–`07`)
- **00 · Intro & Motivation** — Six incompatible formats per AI request; one `799 KB` zero-dependency Rust core (`libcowrie_core.so`), an `80.4 KiB` gzipped `wasm32` build (`cowrie_core.wasm`), and `cowrie mcp-proxy` running symmetrically across four live engines.
- **01 · Format Divergence (`FIG 01`, Table 1, `FIG 02`)** — 50-year history of how human text (JSON), RPC wire (Protobuf), content hashing (DAG-CBOR), columnar OLAP (Arrow/Parquet), and static weights (Safetensors/GGUF) diverged, and how `v1.2` closes even the `< 9%` cold SQLite point-lookup gap (`0.51 ms vs 1.03 ms`).
- **02 · Architecture Layered With Wins (`Plane A`–`Plane D`, `FIG 03`–`FIG 10`, Table 2)** — `169.8 ns` zero-alloc canonical `COWR` + native `TAG_TENSOR_REF (0x0A)` + satellite boundary; 47-bit 10-letter `R0–R3` lenses + fence breakout escaping; 16 KB `0600` HMAC `CWSP` StatePages + symmetric longest-prefix KV offloading + `O_DIRECT` dual-stream CUDA DMA (`3.38 GB/s` cold / `26.79 GB/s` warm) + `LiveIvfStore` (`.cwwal` + `CWIV`); dual-tree `0x04` Merkle + `CWZM` 16 KB Zone-Maps + `Ed25519` (`COWRIE_DECLASSIFY_V1`) + `cowrie mcp-proxy`.
- **03 · Fifteen Major Wins Across Four Live Runtimes, MCP & WebAssembly (`FIG 11`, Table 3)** — 15 receipted wins across `llama.cpp` (`13.72×` suffix reuse), `vLLM 0.15.0` (`5.64×` KV thaw), `Eureka Creative v7`, `Eureka LLM`, `cowrie mcp-proxy`, and Browser/Node `wasm32` (`80.4 KiB` gzipped, `0.59 ms` load, `1.74 µs` verify).
- **04 · Eleven Troubles, Security Audit & Bend 2.0.35 Formal Proofs (11 Postmortems + Table 4)** — Engineering postmortems, `v1.1` security hardening catches, `v1.2` non-bolt-on Bend 2.0.35 `--verdict` formal verification (`26/26` biconditional/liveness laws in `218.3 ms`, `13/13` fail-open & anti-underspecification mutants rejected, `2,000` lockstep vectors catching 8 real bugs `F1`–`F8`), and physical crossover table.
- **05 · What This Means for Local LLMs vs. Cloud Fleets (`FIG 12`, Table 5)** — `0600` NVMe warm virtual-memory swap tier & zero-daemon local scale vs. `CowrieKVConnectorV1` stateless pod elasticity and `Ed25519`-audited MCP fleets.
- **06 · Future Work (4 Next-Horizon Milestones)** — Datacenter GPUDirect Storage (`nvidia-fs` P2P BAR1 on H100/B200) & Apple Metal `MTLBuffer` zero-copy, upstream `Qwen3_5ForConditionalGeneration` hybrid parity in `vLLM`/`SGLang`, 128K+ attention-sink mixed precision, and WebGPU `GPUBuffer` zero-copy views + hardware `FIDO2` signing.
- **07 · Summary**

## SUMMARY
Catalog source for `v1.2.0` [making-cowrie.html](file:///home/omen/agent-lib/making-cowrie.html), incorporating all High-to-Medium ROI roadmap completions, zero-import `wasm32-unknown-unknown` (`80.7 KiB` gzipped), symmetric 4-runtime `v1.2` parity across `vLLM`, `llama.cpp`, `Eureka LLM`, and `Eureka Creative Engine v7`, and non-bolt-on `Bend 2.0.35 --verdict` formal verification (`26/26` laws, `13/13` mutants, `2,000` lockstep vectors, `8` bugs `F1`–`F8` closed) backed by `/home/omen/cowrie/receipts/receipt_v1_2_complete.json`, `/home/omen/cowrie/receipts/receipt_v1_2_wasm_and_parity.json`, and `/home/omen/cowrie/receipts/receipt_v1_2_formal_bend.json`.
