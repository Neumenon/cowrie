# Cowrie Ecosystem Connectors & Runtime Adapters (`v1.2.0`)

Drop-in adapters connecting one zero-dependency Rust/WASM core (`libcowrie_core`, `cowrie` CLI, `cowrie_core.wasm`) into the Layer 3 inference engines, agent frameworks, creative pipelines, vector stores, and edge platforms teams already run — zero standalone daemons, zero migration.

## Quick Matrix

| Target | Install / Path | Entrypoint | Core Capability |
|---|---|---|---|
| **MCP / Claude Code / Cursor** | `pip install cowrie-ai` or `npx -y cowrie-ai` | `cowrie mcp-proxy --virtualize -- <cmd>` · `cowrie mcp-config` | `−89.1%` / `−94.4%` tool tokens, `3.27×` speedup, `13.15 µs` `0x80` taint firewall, `.cwwal` audit |
| **vLLM 0.15+ (V1 Engine)** | `pip install cowrie-ai` (auto plugin) · [`vllm/`](vllm/) | `CowrieKVConnectorV1` · [`demo_restart_kv.py`](vllm/demo_restart_kv.py) | `5.64×` cross-restart KV offload (`80.53 ms` vs `453.88 ms`), `SupportsHMA`, `0600` HMAC/SHA-NI |
| **`llama.cpp`** | Header-only C++17 · [`llama-statepage/`](llama-statepage/) | `LlamaPrefixStateCache` · `LlamaCowrieMemory` | `49.7×` cold / `124.5×` warm hybrid `Q4_0` KV + `F32` SSM resume; `13.72×` new-suffix reuse |
| **LangGraph / LangChain** | `pip install cowrie-ai` · [`langgraph.py`](../python/cowrie/integrations/langgraph.py) | `CowrieLangGraphGuard`, `wrap_tools()`, `wrap_tool_call()`, `CowrieAuditRecorder` | Virtual atlas + `cowrie_get`, `R2-Compact`, causal `0x80` + `Ed25519` gate, `.cwwal` step log |
| **PydanticAI** | `pip install cowrie-ai` · [`pydantic_ai.py`](../python/cowrie/integrations/pydantic_ai.py) | `CowriePydanticAIGuard`, `@guard.tool`, `register_cowrie_get(agent)` | Preserves `RunContext` signatures; virtualizes oversized tool results & blocks tainted exec |
| **OpenAI Agents SDK** | `pip install cowrie-ai` · [`openai_agents.py`](../python/cowrie/integrations/openai_agents.py) | `CowrieOpenAIAgentsGuard`, `wrap_tools()`, `CowrieRunHooks` | Wraps `FunctionTool.on_invoke_tool`, auto-injects `cowrie_get`, strips `_cowrie_declassify` |
| **ComfyUI** | Symlink [`comfyui/`](comfyui/) to `custom_nodes/` | `CowrieCanonicalConditioningCache`, `CowrieWorkflowTrustGate`, `CowriePromptLens` | Key-order-invariant `CWSP` conditioning cache, `0x80` + `Ed25519` workflow security gate |
| **Hugging Face Diffusers** | `pip install cowrie-ai` · [`diffusers/`](diffusers/README.md) | `wrap_pipeline_encode_prompt(pipe)` · `CowrieDiffusersCache` | 1-line `CWSP` NVMe cache for `encode_prompt` (`FLUX` / `SD3` / `Wan`), HMAC + `mmap` |
| **Cloudflare Workers** | [`cloudflare-workers/`](cloudflare-workers/README.md) | `createCowrieWorker(wasmModule)` (`POST /mcp`, `/v1/verify`, `/v1/lens`) | `~92 KiB` gzipped zero-import V8 Isolate MCP virtualization & taint gateway |
| **LanceDB → Sealed MCP** | [`lancedb/`](lancedb/README.md) | `query_lancedb_sealed()`, `seal_lancedb_hits()`, [`cookbook_lancedb_mcp.py`](lancedb/cookbook_lancedb_mcp.py) | Hoists float vectors to `$tensor`, seals 16 KB Merkle `CWRF`, emits Mealy-fenced MCP result |
| **Eureka LLM & Creative v7** | [`eureka/`](eureka/) | `llb_cowrie.h`, `cowrie_route.h`, `ec_cowrie.{h,c}` | `5.00×` E2E restart, `151.7×–2,970×` encoder cache, `8/8` ComfyUI DAG hits |
| **Browser / Node WASM** | `npm install cowrie-ai` · [`wasm/`](../wasm/) | `createCowrieWasm` · `CowrieMcpMediator` · `npx cowrie-ai` | Zero host imports, verified in Node v22 & Chrome 135, `2,000` Bend lockstep vectors |

---

## Symmetric `v1.2.0` Core Guarantees Across Every Adapter

1. **Longest-Prefix `0600` HMAC `StatePages` (`CWSP`):** Cross-restart KV / hybrid SSM+KV / text-encoder tensor offloading bound to 64-stride `cowrie_model_fingerprint`, `runtime_id`, `kv_quant`, `ssm_dtype`, parallel 4 MiB `SHA-NI`/`ARMv8 SHA2` Merkle leaves, and optional RFC 2104 `HMAC-SHA256` (`COWRIE_HMAC_KEY`).
2. **Pre-Append Virtualization + `R2-Compact` Lenses:** Large (`≥ 16 KB`) tool outputs seal into canonical `COWR` frames and render as bounded Mealy-fenced atlases paired with `cowrie_get` (RFC 6901 pointer / line / grep hydration). Small clean structured records render as `R2-Compact` (`−31.2%` payload tokens).
3. **Sticky Causal Taint (`BIT_TAINTED = 0x80`) & Scoped RFC 8032 `Ed25519` Declassification:** Untrusted inputs stamp `0x04 -> 0x80` and block privileged calls (`shell_exec`, `cred_read`, `fs_write`, `net_exfil`) unless authorized by a valid, non-expired `COWRIE_DECLASSIFY_V1` signature bound to `(target_cid_34, action_u8, expiry_u64)`.
4. **Crash-Safe `.cwwal` Audit & Memory Persistence:** Optional `0600` SHA-256 hash-chained write-ahead log (`cowrie.Wal`) with torn-tail recovery and compaction to 16 KB `CWRF` shards.
