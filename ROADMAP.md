# Cowrie Ecosystem Connectors Ledger & Go-To-Market Roadmap

**Repo:** `Neumenon/cowrie` (`v1.2.0`) · **Packages:** `cowrie-ai` (PyPI & npm) · **Last Updated:** 2026-11-28  
**Core Thesis:** *Zero-Infra 30-Second Wedge + Ecosystem Connectors Over Standalone Religion.* Meet engineers inside Claude Code, Cursor, vLLM, llama.cpp, LangGraph, PydanticAI, OpenAI Agents, ComfyUI, Diffusers, Cloudflare Workers, and LanceDB — zero migration, zero standalone daemons.

---

## 1. Connectors Inventory & Live Verification Status

| # | Connector / Surface | Install / Entrypoint | Source | Tested Against Real Upstream? | Day-1 Unlock Features | Status |
|---:|---|---|---|---|---|---|
| 1 | **Python Wheel + Native CLI** | `pip install cowrie-ai` → `cowrie` | [`pyproject.toml`](pyproject.toml), [`setup.py`](setup.py), [`python/cowrie/_cli.py`](python/cowrie/_cli.py) | Clean-room venv (no `COWRIE_LIB`, no `CARGO_TARGET_DIR`) | Bundles `libcowrie_core` + native `cowrie` CLI + all Python integrations in `py3-none-<plat>` wheel (`967 KB`) | **Built & Verified** (needs PyPI publish) |
| 2 | **`npx` CLI & WASM SDK** | `npx -y cowrie-ai mcp-proxy --virtualize -- <cmd>` | [`wasm/package.json`](wasm/package.json), [`wasm/bin/cowrie.mjs`](wasm/bin/cowrie.mjs), [`wasm/mcp-proxy.mjs`](wasm/mcp-proxy.mjs) | Real official **MCP 2.x SDK** (`MCPServer` + `ClientSession`) | Auto-uses native `cowrie` if installed, falls back to zero-import `cowrie_core.wasm` (`117 KB` tgz); `--mode observe\|enforce` + stderr savings meter | **Built & Verified** (needs npm publish) |
| 3 | **MCP / Claude Code / Cursor** | `cowrie mcp-proxy --virtualize -- <cmd>` · `cowrie mcp-config` | [`crates/cowrie-cli/src/main.rs`](crates/cowrie-cli/src/main.rs) | Real official **MCP 2.x SDK** + live paired Qwen3.5-9B (`16/16` vs `11/16`, `3.27×`, `−89.1%` tok) | `--mode observe\|enforce`, `--quiet`, live stderr savings meter (`KB -> KB, ~tok saved`), `mcp-config` generator, `.cwwal` audit | **Built & Verified** |
| 4 | **vLLM 0.15+ (V1 Engine)** | `vllm serve ... --kv-transfer-config '{"kv_connector":"CowrieKVConnectorV1","kv_role":"kv_both"}'` | [`adapters/vllm/cowrie_kv_connector.py`](adapters/vllm/cowrie_kv_connector.py), [`_plugin.py`](adapters/vllm/_plugin.py), [`demo_restart_kv.py`](adapters/vllm/demo_restart_kv.py) | Live **2-process `vLLM 0.15.0` GPU restart** on RTX 3090 (`SupportsHMA=True`, `HMAC-SHA256`, `100%` greedy parity) | Lazy idempotent `vllm.general_plugins` auto-discovery; `SupportsHMA` (`request_finished_all_groups`); portable 2-process demo | **Built & Verified** (next: `vllm serve` HTTP kill-9 recipe + upstream PR) |
| 5 | **`llama.cpp`** | Header-only `#include "cowrie_llama_statepage.hpp"` | [`adapters/llama-statepage/`](adapters/llama-statepage/cowrie_llama_statepage.hpp) | Compiled & linked against local `llama.cpp`; live RTX 3090 receipts (`49.7×` cold / `124.5×` warm; `13.72×` suffix) | `LlamaPrefixStateCache`, `LlamaCowrieMemory`, `LlamaZonedShard` | **Built & Verified** |
| 6 | **LangGraph / LangChain** | `from cowrie.integrations.langgraph import CowrieLangGraphGuard` | [`python/cowrie/integrations/langgraph.py`](python/cowrie/integrations/langgraph.py) | Real **`langgraph` + `langchain-core`** (`StateGraph`, sync `wrap_tool_call` & async `awrap_tool_call`, `StructuredTool`) | `wrap_tools()`, `cowrie_get`, `CowrieAuditRecorder` (`.cwwal`), `mode="observe"`, **`make_langgraph_interrupt_approver`** (`interrupt()` HITL + Ed25519) | **Built & Verified** |
| 7 | **PydanticAI** | `from cowrie.integrations.pydantic_ai import CowriePydanticAIGuard` | [`python/cowrie/integrations/pydantic_ai.py`](python/cowrie/integrations/pydantic_ai.py) | Real **`pydantic-ai-slim 2.77.0`** (`Agent(TestModel)`, `RunContext[str]`, `@agent.tool`) | `@guard.tool` preserving `RunContext` signatures, `register_cowrie_get(agent)`, `mode="observe"`, `on_taint_block` HITL | **Built & Verified** |
| 8 | **OpenAI Agents SDK** | `from cowrie.integrations.openai_agents import CowrieOpenAIAgentsGuard` | [`python/cowrie/integrations/openai_agents.py`](python/cowrie/integrations/openai_agents.py) | Real **`openai-agents 0.26.0`** (`@function_tool`, `FunctionTool`, `ToolContext`) | `wrap_tools()`, `FunctionTool` `cowrie_get`, `_cowrie_declassify` stripping, `CowrieRunHooks`, async `on_taint_block` HITL | **Built & Verified** |
| 9 | **Hugging Face Diffusers** | `from cowrie.integrations.diffusers import wrap_pipeline_encode_prompt` | [`python/cowrie/integrations/diffusers.py`](python/cowrie/integrations/diffusers.py), [`adapters/diffusers/README.md`](adapters/diffusers/README.md) | Real **`torch 2.9.1+cu128`** (`bf16`, `fp16`, `fp32` bit-exact HMAC `CWSP` round-trip) | Key-order-invariant canonical `COWR` key → `0600` HMAC/SHA-NI 16 KB `CWSP` shard | **Built & Verified** |
| 10 | **ComfyUI** | Symlink [`adapters/comfyui/`](adapters/comfyui/) → `custom_nodes/cowrie-comfyui` | [`adapters/comfyui/nodes.py`](adapters/comfyui/nodes.py) | Real **`torch 2.9.1+cu128`** `bf16`+`fp16` `CONDITIONING` round-trip + `Ed25519` gate | `CowrieCanonicalConditioningCache`, `CowrieWorkflowTrustGate`, `CowriePromptLens` | **Built & Verified** (needs ComfyUI Registry submission) |
| 11 | **Cloudflare Workers** | `adapters/cloudflare-workers/` (`wrangler deploy`) | [`adapters/cloudflare-workers/src/worker.mjs`](adapters/cloudflare-workers/src/worker.mjs), [`wrangler.toml`](adapters/cloudflare-workers/wrangler.toml) | Node v22 + real **`WebAssembly.Module`** + simulated **Durable Object cold eviction survival** (`10/10` pass) | Zero-import `cowrie_core.wasm`, **`CowrieMcpSessionDO` Durable Objects** (handles + `0x80` taint survive eviction), `/mcp`, `/v1/verify`, `/v1/lens` | **Built & Verified** (next: live `wrangler deploy` demo URL) |
| 12 | **LanceDB → Sealed MCP** | `from cowrie.integrations.lancedb import query_lancedb_sealed` | [`python/cowrie/integrations/lancedb.py`](python/cowrie/integrations/lancedb.py), [`adapters/lancedb/cookbook_lancedb_mcp.py`](adapters/lancedb/cookbook_lancedb_mcp.py) | Real **`lancedb 0.41.0` + `pyarrow 26.0.1`** (`live-lancedb`: `97.5 KB → 2.5 KB`, **`−97.38%`**) | Hoists float vectors to SHA-256-pinned handles, packs 16 KB Merkle `CWRF`, enforces `0x04 -> 0x80` RAG taint + Ed25519 | **Built & Verified** (next: PR to `lancedb/vectordb-recipes`) |
| 13 | **Eureka LLM & Creative v7** | Native C headers | [`adapters/eureka/`](adapters/eureka/) | Live RTX 3090 (`5.00×` E2E restart, `151.7×–2,970×` encoders, `8/8` DAG hits) | Longest-prefix `CWSP`, HTTP `/v1/memory/*` & `/v1/trust/gate`, `.cwwal` | **Built & Verified** |

---

## 2. Release & Distribution Checklist (Phase 0 — Ship First)

- [ ] **Commit & push** current working tree (`v1.2.1` wedge + ecosystem connectors + CI/Release workflows) to `Neumenon/cowrie`.
- [ ] **Tag & run Release CI (`v1.2.1`):** Confirm [.github/workflows/release.yml](.github/workflows/release.yml) builds all 4 platform wheels (`manylinux_2_28_x86_64`, `manylinux_2_28_aarch64`, `macosx_11_0_arm64`, `win_amd64`), standalone CLI tarballs, and `cowrie-ai-1.2.1.tgz`.
- [ ] **Publish `cowrie-ai` to PyPI** (`uv publish` or OIDC Trusted Publishing) — verify `pip install cowrie-ai` on clean Linux & macOS Apple Silicon.
- [ ] **Publish `cowrie-ai` to npm** (`npm login` + `npm publish`) — verify `npx -y cowrie-ai --version` & `npx -y cowrie-ai mcp-proxy --mode observe --virtualize -- ...`.
- [ ] **Publish `cowrie-core` & `cowrie-cli` to crates.io** (optional Rust ecosystem lane).

---

## 3. Awareness & Upstream Distribution Roadmap (Phases 1–4)

### Phase 1 — Ecosystem Directories & 5 Surgical Upstream PRs (Compounding Organic Traffic)
- [ ] **MCP Directories:** List `cowrie-ai` (`cowrie mcp-proxy --virtualize`) on Official MCP Registry, Smithery, Glama, Cursor Directory, and `awesome-mcp-servers`.
- [ ] **PR #1 (`vllm-project/vllm`):** Add `examples/others/cowrie/` cross-restart KV offload example + `vllm serve` HTTP kill-9 recipe (modeled on `examples/others/lmcache/`).
- [ ] **PR #2 (`lancedb/vectordb-recipes`):** Submit *"LanceDB → Cowrie Sealed MCP Frame (−97% RAG JSON bloat + deterministic taint)"*.
- [ ] **PR #3 (ComfyUI Registry / Manager):** Publish `cowrie-comfyui` custom node pack for 1-click install.
- [ ] **PR #4 (LangGraph / PydanticAI / OpenAI Agents docs):** Submit community integration guides highlighting `−89%` tool-output virtualization + `Ed25519` HITL `interrupt()` approval.
- [ ] **PR #5 (Cloudflare Workers MCP templates):** Deploy live demo worker (`cowrie-edge-mcp-gateway`) with `CowrieMcpSessionDO` + submit template reference.

### Phase 2 — High-Signal 45-Second Visual Proof Artifacts
- [ ] **Recording #1 (MCP Split-Screen GIF/Video):** Unmodified MCP server dumping 55 KB JSON + prompt injection vs `npx -y cowrie-ai mcp-proxy --virtualize --` printing `[cowrie] 55.2 KB -> 1.3 KB (-97.6%)`, hydrating via `cowrie_get`, and blocking `shell_exec` in `13.15 µs` (`-32025`).
- [ ] **Recording #2 (vLLM Kill-9 Restart GIF/Video):** Cold 11.7K-tok prefill (`453 ms`) → `kill -9` → restart → `80.5 ms` restore (`5.64×`), `100%` greedy parity, `cowrie state-inspect --verify-hash`.
- [ ] **Zero-Install Browser Playground:** Embed `cowrie_core.wasm` into `cowrie-editorial/site/` so visitors paste raw MCP JSON / prompt injections and watch live `R2-Compact`, Mealy escaping, and `< 2 µs` Merkle/Ed25519 verification in-browser.

### Phase 3 — Three Staggered Problem-First Launches (1–2 Weeks Apart)
- [ ] **Wave 1 — MCP Wedge** (Show HN, X, r/ClaudeAI, r/cursor, r/LocalLLaMA): *"Wrap any MCP server to cut tool-output tokens 89% and block indirect prompt injection in 13 µs (`npx -y cowrie-ai`)"*.
- [ ] **Wave 2 — vLLM / Inference Wedge** (vLLM/SGLang Discord, X, HN): *"Persist vLLM V1 KV caches across restarts in one flag (`CowrieKVConnectorV1`)"* + `02-vllm-llamacpp-prefix-restart`.
- [ ] **Wave 3 — Deterministic Security & Formal Proofs** (AgentDojo eval + AI Security researchers): Run `CowrieToolGuard` on **AgentDojo** suites and publish *"Stopping multi-hop MCP prompt injection in 13 µs without an LLM judge (26 Bend proofs)"*.

### Phase 4 — Direct Technical Notes to Top-20 Maintainers (Tailored 3-Line Stack Demos)
Draft-first before every external send:
- [ ] **vLLM / KV Infra:** Woosuk Kwon (vLLM), Robert Shaw (Red Hat `llm-d`), Junchen Jiang (LMCache), Lianmin Zheng (SGLang), Moein Khazraee (NVIDIA NIXL), Ruoyu Qin (Mooncake).
- [ ] **MCP & Security:** David Soria Parra (Anthropic MCP), Luca Beurer-Kellner (Invariant Labs), Florian Tramèr (ETH CaMeL / AgentDojo), Simon Willison.
- [ ] **Storage, Edge & Local Runtimes:** Chang She (LanceDB), Rita Kozlov (Cloudflare Workers), Georgi Gerganov (`llama.cpp`), Nicolas Patry (`Safetensors`), Awni Hannun (MLX), Michael Truell (Cursor).

---

## 4. Next Engineering Unlocks (Backlog)

- [ ] **Local HTTP/SSE MCP proxy mode** (`cowrie mcp-proxy --listen 127.0.0.1:8787 --upstream https://...`) for remote Streamable HTTP MCP servers in Cursor / Claude Desktop.
- [ ] **Interactive local CLI approver (`cowrie approve`)** for signing single-use Ed25519 tokens from terminal notifications during `--mode enforce`.
- [ ] **vLLM cluster/object-storage tier** (async S3 / R2 / NIXL persistence underneath `CowrieKVConnectorV1` for ephemeral multi-node Kubernetes pods).
- [ ] **AgentDojo benchmark harness** reproducing public indirect prompt-injection suites under `CowrieToolGuard`.
- [ ] **ComfyUI intermediate latent/VAE DAG cache** extending `CWSP` beyond `CLIPTextEncode` to full graph caching.

## SUMMARY
Canonical ledger of all 13 Cowrie v1.2 ecosystem connectors (all built and verified against real upstream SDKs/hardware), the Phase 0 PyPI/npm/GitHub Release checklist, 5 upstream PR targets, visual proof artifacts, 3 staggered launches, Top-20 maintainer outreach, and next engineering unlocks.
