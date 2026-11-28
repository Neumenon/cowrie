# Cowrie Edge MCP Gateway for Cloudflare Workers

Deploy Cowrie's zero-import `cowrie_core.wasm` (`~92 KiB` gzipped, `~0.6 ms` isolate load) as a Cloudflare Worker in front of any Streamable HTTP MCP server or edge AI API:

- **`POST /mcp`** — Virtualizes oversized (`>= 16 KB`) MCP responses into Mealy-fenced atlases, injects `cowrie_get` for bounded RFC 6901 / grep / line slicing, and blocks unsigned tainted privileged calls with `-32025` at the edge. Backed by **`CowrieMcpSessionDO` (Cloudflare Durable Objects)** in [`wrangler.toml`](wrangler.toml) so virtualized handles and causal `0x80` taint survive global V8 isolate evictions.
- **`POST /v1/verify`** — Zero-alloc `COWR` / `CWRF` Merkle verification.
- **`POST /v1/lens`** — `R0` / `R1` / `R2` / `R2-Compact` / `virtual-atlas` prompt projection.
- **`POST /v1/declassify/verify`** — Strict RFC 8032 `Ed25519` capability verification.

## Quickstart

```bash
make -C ../.. wasm
cp /tmp/cowrie-cargo-target/wasm32-unknown-unknown/release/cowrie_core.wasm ./src/cowrie_core.wasm
npm test
npx wrangler deploy
```
