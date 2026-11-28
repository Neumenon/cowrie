# Cowrie (`v1.2.0`)

**Drop-in MCP virtualization & taint firewall, vLLM/llama.cpp cross-restart KV offloader, and 16 KB AI binary container.**

Wrap any existing MCP server in front of **Claude Code / Cursor**, persist **vLLM V1** KV caches across restarts, or guard **LangGraph / PydanticAI / OpenAI Agents / ComfyUI / Diffusers / Cloudflare Workers / LanceDB** — zero servers, zero migration, zero external Rust crates.

> **Package name note:** Install as **`cowrie-ai`** on PyPI and npm (`Neumenon/cowrie`, Apache-2.0). Bare `cowrie` on PyPI belongs to the unrelated SSH/Telnet honeypot.

---

## 30-Second Start (Zero Infra, Zero Migration)

### 1. Claude Code / Cursor / Any MCP Server — Virtualize & Firewall in 1 Command
Wrap any existing `stdio` MCP server to cut oversized tool-output tokens **`−89.1%` gross (`−94.4%` net new)**, speed up multi-tool agent loops **`3.27×`**, lift accuracy **`11/16 → 16/16`**, and block multi-hop indirect prompt injection in **`13.15 µs`** (`-32025` JSON-RPC wire gate):

```bash
# Option A: Python wheel (bundles native Rust `cowrie` CLI + `libcowrie_core` + all Python adapters)
pip install cowrie-ai
cowrie mcp-proxy --virtualize -- <existing-mcp-server>

# Option B: Zero-install Node/WASM CLI (works on macOS, Linux, Windows; auto-uses native binary if present)
npx -y cowrie-ai mcp-proxy --virtualize -- <existing-mcp-server>

# Day-1 ramp option: get −89% token savings immediately + log taint blocks without hard-blocking yet
cowrie mcp-proxy --mode observe --virtualize -- <existing-mcp-server>
```

**Drop directly into Cursor (`.cursor/mcp.json`) or Claude Desktop (`claude_desktop_config.json`):**
```json
{
  "mcpServers": {
    "filesystem": {
      "command": "cowrie",
      "args": [
        "mcp-proxy", "--virtualize", "--trust-server-annotations", "--",
        "npx", "-y", "@modelcontextprotocol/server-filesystem", "."
      ]
    }
  }
}
```

Or generate a ready-to-paste config for **Claude Code**, **Cursor**, or **Claude Desktop**:
```bash
cowrie mcp-config --client claude-code --virtualize -- github npx -y @modelcontextprotocol/server-github
```

*What happens automatically:* Any tool response `≥ 16 KB` (`--virtualize-bytes` tunable) is sealed in a canonical `COWR` frame, replaced with a bounded Mealy-fenced structural atlas, and paired with an auto-injected local `cowrie_get` tool for RFC 6901 pointer / line / grep hydration. External/web/issue/search outputs stamp `0x04 UNTRUSTED_EXT -> 0x80 BIT_TAINTED` and block unsigned privileged tool calls (`shell_exec`, `cred_read`, `fs_write`, `net_exfil`) unless authorized by a scoped RFC 8032 `Ed25519` token (`cowrie declassify-sign`).

### 2. vLLM 0.15+ — Cross-Restart KV Offload in 1 Flag
`pip install cowrie-ai` auto-registers **`CowrieKVConnectorV1`** via `vllm.general_plugins` (`SupportsHMA`-compatible, lazy-loaded):

```bash
pip install cowrie-ai

vllm serve Qwen/Qwen3-8B \
  --enable-prefix-caching \
  --kv-transfer-config '{"kv_connector":"CowrieKVConnectorV1","kv_role":"kv_both"}'
```

Multi-layer paged KV blocks persist into `0600`, 16 KB-aligned, HMAC/SHA-NI-authenticated `.state.cowrie` shards on local NVMe (`80.53 ms` vs `453.88 ms` cold prefill = **`5.64×`** across OS process restarts at `100%` greedy parity). Run the portable 2-process demo in [`adapters/vllm/demo_restart_kv.py`](adapters/vllm/demo_restart_kv.py).

---

## Drop-In Ecosystem Connectors

Cowrie meets teams inside the Layer 3 engines and frameworks they already run — zero mandatory extra dependencies:

| Ecosystem | Drop-In Entrypoint | What You Get Immediately | Guide / Source |
|---|---|---|---|
| **MCP / Claude Code / Cursor** | `cowrie mcp-proxy --virtualize -- <cmd>` · `npx cowrie-ai` | `−89.1%` / `−94.4%` tool tokens, `3.27×` speedup, `13.15 µs` `0x80` taint gate, `.cwwal` audit | [`crates/cowrie-cli/`](crates/cowrie-cli/src/main.rs) · [`wasm/`](wasm/mcp-proxy.mjs) |
| **vLLM 0.15+ (V1 Engine)** | `CowrieKVConnectorV1` (auto-registered plugin) | `5.64×` cross-process KV restore, `0600` HMAC/SHA-NI `.state.cowrie`, `SupportsHMA` | [`adapters/vllm/`](adapters/vllm/cowrie_kv_connector.py) · [`demo_restart_kv.py`](adapters/vllm/demo_restart_kv.py) |
| **`llama.cpp`** | `LlamaPrefixStateCache` (header-only C++17) | `49.7×` cold / `124.5×` warm hybrid `Q4_0` KV + `F32` SSM resume; `13.72×` new-suffix reuse | [`adapters/llama-statepage/`](adapters/llama-statepage/cowrie_llama_statepage.hpp) |
| **LangGraph / LangChain** | `from cowrie.integrations.langgraph import CowrieLangGraphGuard` | `wrap_tools()`, `ToolNode(wrap_tool_call=...)`, `cowrie_get`, `.cwwal` `CowrieAuditRecorder` | [`langgraph.py`](python/cowrie/integrations/langgraph.py) |
| **PydanticAI** | `from cowrie.integrations.pydantic_ai import CowriePydanticAIGuard` | `@guard.tool` preserving `RunContext` signatures, `guard.register_cowrie_get(agent)` | [`pydantic_ai.py`](python/cowrie/integrations/pydantic_ai.py) |
| **OpenAI Agents SDK** | `from cowrie.integrations.openai_agents import CowrieOpenAIAgentsGuard` | `wrap_tools()` for `FunctionTool`, `cowrie_get`, `CowrieRunHooks` (`on_tool_start`/`end`) | [`openai_agents.py`](python/cowrie/integrations/openai_agents.py) |
| **ComfyUI** | `adapters/comfyui/` (`custom_nodes/cowrie-comfyui`) | Canonical `CWSP` conditioning cache (key-order invariant), `0x80` workflow gate, `R0–R2` lens | [`adapters/comfyui/`](adapters/comfyui/README.md) |
| **Hugging Face Diffusers** | `from cowrie.integrations.diffusers import wrap_pipeline_encode_prompt` | 1-line `CWSP` NVMe cache for `encode_prompt` (`FLUX` / `SD3` / `Wan`), HMAC + `mmap` | [`diffusers.py`](python/cowrie/integrations/diffusers.py) · [`adapters/diffusers/`](adapters/diffusers/README.md) |
| **Cloudflare Workers** | `adapters/cloudflare-workers/` (`cowrie_core.wasm`, `0` imports) | Edge `/mcp` virtualization + `0x80` taint proxy, `/v1/verify`, `/v1/lens`, `/v1/declassify/verify` | [`adapters/cloudflare-workers/`](adapters/cloudflare-workers/README.md) |
| **LanceDB → Sealed MCP** | `from cowrie.integrations.lancedb import query_lancedb_sealed` | Hoists float vectors to `$tensor`, packs 16 KB Merkle `CWRF`, Mealy-fenced MCP result | [`adapters/lancedb/`](adapters/lancedb/README.md) · [`cookbook_lancedb_mcp.py`](adapters/lancedb/cookbook_lancedb_mcp.py) |
| **Eureka LLM & Creative v7** | `llb_cowrie.h` · `cowrie_route.h` · `ec_cowrie.{h,c}` | `5.00×` E2E restart, `151.7×–2,970×` encoder cache, `8/8` ComfyUI DAG hits | [`adapters/eureka/`](adapters/eureka/) |

### 5-Line Python Agent Drop-Ins (LangGraph · PydanticAI · OpenAI Agents · LanceDB)
```python
# LangGraph / LangChain
from cowrie.integrations.langgraph import CowrieLangGraphGuard
guard = CowrieLangGraphGuard(virtualize_bytes=16_384, audit_wal_path="agent.cwwal")
tools = guard.wrap_tools([web_search, read_file, run_shell])  # auto-adds `cowrie_get`

# PydanticAI
from cowrie.integrations.pydantic_ai import CowriePydanticAIGuard
guard = CowriePydanticAIGuard()
guard.register_cowrie_get(agent)
# Decorate tools with `@guard.tool(untrusted=True)` or `@guard.tool(action="shell_exec")`

# OpenAI Agents SDK
from cowrie.integrations.openai_agents import CowrieOpenAIAgentsGuard
guard = CowrieOpenAIAgentsGuard()
agent = Agent(name="Ops", tools=guard.wrap_tools([web_fetch, run_shell]))

# LanceDB -> Cowrie Sealed MCP Frame (runnable: `python3 adapters/lancedb/cookbook_lancedb_mcp.py`)
from cowrie.integrations.lancedb import query_lancedb_sealed
sealed, mcp_result = query_lancedb_sealed(table, query_vec, query_text="invoice", limit=10)
```

---

## Measured Benchmarks (RTX 3090 · Ryzen 7 7700X · NVMe)

| Workload | Cowrie `v1.2.0` | Speedup / Reduction | Receipt |
|---|---:|---:|---|
| **`mcp-proxy --virtualize` + `R2-Compact`** (`Qwen3.5-9B`, 16 real `15–68 KB` payloads + 4 `<1 KB`) | `16/16` vs `11/16` inline (`17.9 s` vs `58.5 s`); `−89.1%` gross tok (`−94.4%` net new) | **`3.27×` E2E speedup**, `+5` paired accuracy wins, `0` hijacks, `−31.2%` `<1 KB` payload tok | [`receipt_v1_2_b2_live_paired.json`](receipts/receipt_v1_2_b2_live_paired.json) |
| **`vLLM 0.15.0` Cross-Restart KV Offload** (`CowrieKVConnectorV1`, `11,755` tok, 12L) | `80.53 ms` (`144.1 MB` HMAC shard) | **`5.64×`** vs cold prefill (`100%` parity) | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **`llama.cpp` Hybrid SSM+KV Resume** (`13.8K` tok `Qwen3.5-9B`, `Q4_0` KV + `F32` recurrent state*) | `68.2 ms` cold / `27.2 ms` warm verified (`180.2 MB`) | **`49.7×`** cold / **`124.5×`** warm verified (`KL = 3.99e-04`) | [`receipt_shani_kvquant.json`](receipts/receipt_shani_kvquant.json) |
| **`llama.cpp` Prefix State Cache** (`1,488` tok shared prefix, new suffix query) | `9.01 ms` suffix / `46.46 ms` full | **`13.72×`** suffix / `2.66×` full (`100%` parity) | [`receipt_v1_2_wasm_and_parity.json`](receipts/receipt_v1_2_wasm_and_parity.json) |
| **Eureka Creative v7 Encoder & DAG Cache** (`UMT5-XXL`, `FLUX Qwen3-4B`, 8-node DAG) | `21.3 ms` / `5.9–30.9 ms` / `1.0–9.6 ms` | **`151.7×–2,970×`** (`8/8` DAG hits) | [`c0-v7-cowrie-receipt.txt`](receipts/c0-v7-cowrie-receipt.txt) |
| **Eureka LLM Cold Restart** (`Qwen3.5-9B`, `2,800` tok E2E HTTP) | `96.3 ms` state / `308.5 ms` E2E | **`5.00×`** E2E across process restart | [`receipt_live_all.json`](receipts/receipt_live_all.json) |
| **16 KB `O_DIRECT` + Dual-Stream CUDA DMA** (`192 MiB` StatePage → VRAM) | `59.62 ms` cold (`3.38 GB/s`) / `26.79 GB/s` warm | **`1.31×`** cold NVMe-to-VRAM throughput | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **16 KB Zone-Map Shards (`CWZM`)** (`100K` rows on cold NVMe, clustered fields) | `0.514 ms` point (`0.001%`) / `0.542 ms` secondary | **`1.58×–2.88×`** vs cold `SQLite3`, `−15.0%` disk | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **10M × 512d Cold NVMe IVF (`CWIV`) + `LiveIvfStore`** (`5.30 GB` + `.cwwal`†) | `7.07 ms` 10M CUDA+`O_DIRECT` prototype / `87.5 µs` Rust live hybrid | `4.45 µs` WAL append, `12.9 ms` Rust compact | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **Canonical Verifier & `R2` Lenses** (`COWR` + `0x0A TensorRef`) | `169.8 ns` 0-alloc verify (`278.7 ns` w/ CID) | **`7.43×`** verify; `−65%` to `−98.7%` prompt tokens | [`receipt_v1_1_hardening.json`](receipts/receipt_v1_1_hardening.json) |
| **`cowrie mcp-proxy` & `Ed25519` Gate** (`COWRIE_DECLASSIFY_V1`) | `13.15 µs` wire check / `808 µs` `Ed25519` verify | `100%` causal taint enforcement | [`receipt_v1_2_complete.json`](receipts/receipt_v1_2_complete.json) |
| **WebAssembly (`cowrie_core.wasm`)** (Node v22 & Chrome 135) | `0.66 ms` load / `1.91 µs` verify + CID | `102.2 KiB` gzipped (`80.4 KiB` at `v1.2.0` tag), `0` host imports | [`receipt_v1_2_wasm_and_parity.json`](receipts/receipt_v1_2_wasm_and_parity.json) |

> *Notes on historical receipts:* `\*` Historical `receipt_shani_kvquant.json` stamped `COWRIE_SSM_FP16` onto `llama.cpp`'s F32 recurrent payload; fixed to `COWRIE_SSM_FP32`. `†` The `7.07 ms` 10M-vector cold NVMe IVF result comes from the CUDA + `O_DIRECT` prototype (`receipt_u3_v03.json`); the shipped Rust `CWIV` reader uses buffered `pread` + AVX2/NEON CPU scoring.

---

## Under the Hood: 4-Plane Container & Formal Proofs

All connectors share one zero-dependency Rust core (`libcowrie_core.so`, `cowrie` CLI, `cowrie_core.wasm`) compiling offline in `< 5 s`:

1. **Plane A — Canonical Wire Core (`canonical.rs`, `COWR`):** Bijective binary frames with UTF-8 byte-sorted key dictionaries, zero-allocation wire verification (`169.8 ns`), `TAG_TENSOR_REF (0x0A)` descriptors, repack-stable `resolved_frame_cid`, strict RFC 8259 JSON conversion, bounded nesting (`MAX_DEPTH = 64`), and 34-byte SHA-256 multihash CIDs.
2. **Plane B — Progressive Attention Lenses (`lens.rs`, `R0–R3` & Virtual Atlas):** 10-letter `Alpha32` handles (`47.0` bits), `R1` structural summaries, `R2` / `R2-Compact` adaptive table hoisting with 3-state Mealy fence escaping, bounded `project_virtual_atlas` + `query_frame_slice` (`cowrie_get`), and lossless `R3` / `extract_field`.
3. **Plane C — 16 KB Hardware Arena (`shard.rs`, `ivf.rs`, `wal.rs`):** `16,384 B`-aligned `CWSP` KV/SSM StatePages (`0600` atomic writes, 64-stride `cowrie_model_fingerprint`, `HMAC-SHA256`, parallel 4 MiB `SHA-NI`/`ARMv8 SHA2` Merkle), co-located `CWIV` IVF-INT8 shards, and hash-chained `.cwwal` write-ahead logs.
4. **Plane D — Provenance, `Ed25519`, `CWZM` Zone-Maps & Merkle (`trust.rs`, `ed25519.rs`, `zonemap.rs`):** Out-of-band authority flags (`0x01/0x02/0x04/0x08`), sticky causal taint (`BIT_TAINTED = 0x80`), RFC 8032 `Ed25519` scoped declassification tokens (`COWRIE_DECLASSIFY_V1`), 64 B `CWZM` zone-maps, and dual-tree RFC 6962 Merkle footers.
5. **Formal Verification:** `26/26` laws proved in `Bend 2.0.35` (`formal/PROOF.bend`), `2,000`-vector WASM + `100` native C-ABI lockstep checks, and an independent pure-Python verifier ([`tests/independent_cowr_oracle.py`](tests/independent_cowr_oracle.py)).

---

## Build From Source & Verify

Builds default to `/tmp/cowrie-cargo-target` (keeping the source tree clean):

```bash
make test          # Release Rust core unit tests
make conformance   # C-ABI + 150 golden vectors + pure-Python oracle conformance
make wasm          # Build stripped cowrie_core.wasm (0 imports) + Node.js WASM & npx proxy tests
make connectors    # Verify LangGraph, PydanticAI, OpenAI Agents, ComfyUI, Diffusers, LanceDB, Cloudflare Workers
make formal        # Bend 2.0.35 26/26 proofs + 2,000 WASM & 100 C-ABI lockstep vectors
make wheel         # Build self-contained platform wheel in /tmp/cowrie-dist
```

## Documentation

- **Connectors Ledger & Launch Roadmap:** [`ROADMAP.md`](ROADMAP.md)
- **Engineering Guide & Copy-Paste Quickstarts:** [`docs/GAINS-AND-USES.md`](docs/GAINS-AND-USES.md)
- **Ecosystem Adapters Matrix:** [`adapters/README.md`](adapters/README.md)
- **Interactive Systems Monograph (`Making Cowrie`):** [`docs/making-cowrie.html`](docs/making-cowrie.html)
- **Wire & Container Specification (`v1.2.0`):** [`SPEC.md`](SPEC.md)
- **Formal Verification Suite:** [`formal/README.md`](formal/README.md)
- **Archived v0 Codec:** [`Neumenon/cowrie-legacy`](https://github.com/Neumenon/cowrie-legacy)

## Repository Layout

```text
cowrie/
├── crates/cowrie-core/          # Zero-dep Rust core (canonical, lens, shard, ivf, wal, zonemap, trust, ed25519, ffi/wasm)
├── crates/cowrie-cli/           # Native `cowrie` CLI (`mcp-proxy --virtualize`, `mcp-config`, `pack`, `verify`, `lens`, `get`)
├── python/cowrie/               # Python ctypes SDK, `cowrie` console script, and `cowrie.integrations` (LangGraph, PydanticAI, OpenAI Agents, Diffusers, LanceDB)
├── wasm/                        # `cowrie-ai` npm package (`npx cowrie-ai mcp-proxy`, `cowrie.mjs`, `cowrie.d.ts`)
├── adapters/
│   ├── vllm/                    # `CowrieKVConnectorV1` (`vllm.general_plugins`, `SupportsHMA`), `demo_restart_kv.py`
│   ├── llama-statepage/         # `llama.cpp` header-only `LlamaPrefixStateCache` & `CWSP`
│   ├── comfyui/                 # ComfyUI custom nodes (`CWSP` conditioning cache, `0x80` trust gate, lens)
│   ├── diffusers/               # Hugging Face Diffusers `encode_prompt` `CWSP` cache docs
│   ├── cloudflare-workers/      # Cloudflare Workers edge MCP gateway & verifier (`cowrie_core.wasm`)
│   ├── lancedb/                 # LanceDB -> Cowrie sealed MCP frame runnable cookbook
│   └── eureka/                  # Eureka LLM & Creative Engine v7 native C adapters
├── formal/                      # Bend 2.0.35 specification, 26 machine-checked laws, differential lockstep
├── tests/                       # Golden vectors, independent Python oracle, and ecosystem connector test suite
└── receipts/                    # Hardware-measured benchmark & verification receipts
```
