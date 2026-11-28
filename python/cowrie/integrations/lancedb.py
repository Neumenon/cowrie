"""LanceDB -> Cowrie Sealed MCP Frame adapter & cookbook helpers.

Turns LanceDB vector/hybrid search results into:
  1. Compact canonical `COWR` frames where raw float embeddings become typed `$tensor` / `0x0A`
     handles (eliminating JSON float bloat) and every record carries out-of-band provenance
     (`AUTH_UNTRUSTED_EXT` for external corpora, `AUTH_VERIFIED_TOOL` for curated internal tables).
  2. 16 KB-aligned Merkle-authenticated `CWRF` shards (`pack_single_shard`) verifiable in `169.8 ns`.
  3. Ready-to-return MCP `tools/call` responses (`R2-Compact` or Virtual Atlas + `cowrie_get` when >= 16 KB)
     with `_meta.cowrie` CID + taint lineage for Claude Code, Cursor, or LangGraph/PydanticAI.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import struct
from typing import Any, Iterable, Optional, Sequence

import cowrie
from cowrie.integrations.guard import CowrieToolGuard, DEFAULT_VIRTUALIZE_BYTES


@dataclass(frozen=True)
class SealedLanceResult:
    table_name: str
    query: str
    frame: bytes
    shard: bytes
    cid_hex: str
    handle: str
    merkle_root_hex: str
    authority: int
    tainted: bool
    hit_count: int
    raw_json_bytes: int


def _to_rows(hits: Any) -> list[dict[str, Any]]:
    if hasattr(hits, "to_list"):
        return list(hits.to_list())
    if hasattr(hits, "to_pylist"):
        return list(hits.to_pylist())
    if hasattr(hits, "to_dict"):
        return list(hits.to_dict(orient="records"))
    return [dict(r) for r in hits]


def _vector_to_external_ref(vec: Sequence[float], space: str) -> str:
    """Summarize a LanceDB float32 vector into a compact SHA-256-pinned external handle (`<T:f32[dim]@space#sha12>`)."""
    floats = [float(x) for x in vec]
    raw = struct.pack(f"<{len(floats)}f", *floats)
    digest_hex = cowrie.sha256_hex(raw)
    return f"<T:f32[{len(floats)}]@{space}#{digest_hex[:12]}>"


_RESERVED_SENTINELS = frozenset({"$bytes", "$tensor", "$float", "$map"})


def _sanitize_value(v: Any, depth: int = 0) -> Any:
    import math

    if depth > 56:
        return str(v)
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        if math.isnan(v):
            return {"$float": "NaN"}
        if math.isinf(v):
            return {"$float": "Infinity" if v > 0 else "-Infinity"}
        return 0.0 if v == 0.0 else v
    if isinstance(v, (bytes, bytearray)):
        return {"$bytes": bytes(v).hex()}
    if hasattr(v, "tolist"):
        return _sanitize_value(v.tolist(), depth + 1)
    if isinstance(v, (list, tuple)):
        return [_sanitize_value(x, depth + 1) for x in v]
    if isinstance(v, dict):
        out = {str(k): _sanitize_value(val, depth + 1) for k, val in v.items()}
        if any(k in _RESERVED_SENTINELS for k in out):
            return {"$map": out}
        return out
    return str(v)


def seal_lancedb_hits(
    hits: Any,
    *,
    table_name: str,
    query: str = "",
    authority: int = cowrie.AUTH_UNTRUSTED_EXT,
    vector_columns: Iterable[str] = ("vector", "embedding"),
    tensor_space: str = "lancedb",
) -> SealedLanceResult:
    """Seal LanceDB query rows into a canonical `COWR` frame + 16 KB Merkle `CWRF` shard."""
    rows = _to_rows(hits)
    raw_json_bytes = len(json.dumps({"table": table_name, "query": query, "hits": rows}, default=str).encode("utf-8"))
    vec_cols = set(vector_columns)

    compact_rows: list[dict[str, Any]] = []
    for row in rows:
        out_row: dict[str, Any] = {}
        for k, v in row.items():
            if k in vec_cols and v is not None:
                seq = v.tolist() if hasattr(v, "tolist") else v
                if isinstance(seq, (list, tuple)) and seq and isinstance(seq[0], (int, float)):
                    out_row[str(k)] = _vector_to_external_ref(seq, f"{tensor_space}.{table_name}")
                    continue
            out_row[str(k)] = _sanitize_value(v)
        if any(k in _RESERVED_SENTINELS for k in out_row):
            out_row = {"$map": out_row}
        compact_rows.append(out_row)

    envelope = {
        "source": "lancedb",
        "table": table_name,
        "query": query,
        "hit_count": len(compact_rows),
        "hits": compact_rows,
    }
    frame = cowrie.encode_json(envelope, authority=authority, epistemic=cowrie.EPI_OBSERVED)
    ver = cowrie.verify_frame(frame)
    cid_bytes: bytes = ver["cid"]
    shard = cowrie.pack_single_shard(frame)
    merkle_root = cowrie.verify_shard(shard)

    return SealedLanceResult(
        table_name=table_name,
        query=query,
        frame=frame,
        shard=shard,
        cid_hex=cid_bytes.hex(),
        handle=cowrie.alpha_handle(cid_bytes),
        merkle_root_hex=merkle_root.hex(),
        authority=ver["authority"],
        tainted=cowrie.is_tainted(ver["authority"]),
        hit_count=len(compact_rows),
        raw_json_bytes=raw_json_bytes,
    )


def lancedb_hits_to_mcp_result(
    sealed: SealedLanceResult,
    *,
    virtualize_bytes: int = DEFAULT_VIRTUALIZE_BYTES,
    atlas_max_chars: int = 1800,
    guard: Optional[CowrieToolGuard] = None,
) -> dict[str, Any]:
    """Format a `SealedLanceResult` into an MCP `tools/call` `result` object with `R2-Compact` or Virtual Atlas."""
    virtualized = virtualize_bytes > 0 and len(sealed.frame) >= virtualize_bytes
    if virtualized:
        rendered = cowrie.project_virtual_atlas(sealed.frame, max_chars=atlas_max_chars)
    else:
        rendered = cowrie.project_r2_compact(sealed.frame)

    if guard is not None:
        # Register handle & causal taint inside the caller's CowrieToolGuard if provided
        guard.seal_output(
            f"lancedb:{sealed.table_name}",
            json.loads(cowrie.decode_to_json(sealed.frame)),
            authority=sealed.authority,
        )

    return {
        "content": [{"type": "text", "text": rendered}],
        "_meta": {
            "cowrie": {
                "source": "lancedb",
                "table": sealed.table_name,
                "cid": sealed.cid_hex,
                "handle": sealed.handle,
                "merkle_root": sealed.merkle_root_hex,
                "shard_bytes": len(sealed.shard),
                "raw_json_bytes": sealed.raw_json_bytes,
                "rendered_bytes": len(rendered.encode("utf-8")),
                "tainted": sealed.tainted,
                "virtualized": virtualized,
            }
        },
    }


def query_lancedb_sealed(
    table: Any,
    query_vector_or_text: Any,
    *,
    query_text: str = "",
    limit: int = 10,
    where: Optional[str] = None,
    authority: int = cowrie.AUTH_UNTRUSTED_EXT,
    virtualize_bytes: int = DEFAULT_VIRTUALIZE_BYTES,
) -> tuple[SealedLanceResult, dict[str, Any]]:
    """One-call bridge: run `table.search(...)` on a live LanceDB table and return `(SealedLanceResult, mcp_result)`."""
    q = table.search(query_vector_or_text)
    if where:
        q = q.where(where)
    hits = q.limit(limit).to_list()
    tname = getattr(table, "name", "lancedb_table")
    sealed = seal_lancedb_hits(hits, table_name=tname, query=query_text, authority=authority)
    mcp_res = lancedb_hits_to_mcp_result(sealed, virtualize_bytes=virtualize_bytes)
    return sealed, mcp_res


__all__ = [
    "SealedLanceResult",
    "lancedb_hits_to_mcp_result",
    "query_lancedb_sealed",
    "seal_lancedb_hits",
]
