# Cookbook: LanceDB → Cowrie Sealed MCP Frame

Keep **LanceDB** as your multimodal vector lakehouse, and use **Cowrie** at the boundary between retrieval and the LLM/MCP wire:

1. **Eliminates JSON vector bloat (`−90%+` bytes):** Hoists raw `384–1536d` float lists into compact typed `$tensor` handles (`dtype`, `shape`, `sha256`, `space`).
2. **Proves integrity end-to-end:** Seals the retrieved slice in a 16 KB-aligned Merkle-authenticated `CWRF` shard (`verify_shard` catches any 1-bit modification).
3. **Stops RAG prompt injection deterministically (`0x04 -> 0x80`):** Stamps external KB rows `AUTH_UNTRUSTED_EXT`, wraps prompt output in Mealy-escaped fences, and blocks tainted `shell_exec` / `fs_write` / `net_exfil` tool calls unless approved by an RFC 8032 `Ed25519` token.

## 6-Line Usage Inside an MCP Tool

```python
import lancedb
import cowrie
from cowrie.integrations.lancedb import query_lancedb_sealed

db = lancedb.connect("./my_lancedb")
table = db.open_table("external_kb")

sealed, mcp_result = query_lancedb_sealed(
    table,
    query_embedding,
    query_text="vendor invoice alignment",
    limit=10,
    authority=cowrie.AUTH_UNTRUSTED_EXT,
)
# Return `mcp_result` directly from your MCP server (pairs natively with `cowrie mcp-proxy --virtualize`)
```

## Run the Self-Contained Verification Demo

```bash
python3 adapters/lancedb/cookbook_lancedb_mcp.py
```
