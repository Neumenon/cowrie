"""Profiles — conventions over the LOCKED Core (§6). A profile is an agreed Object/Array/Tensor
shape with named fields and carries **no new wire tag**. It therefore inherits, for free, everything
Core proves: one canonical byte-string, content address, structural fingerprint, file identity,
zero-copy aligned tensors, and 4-language byte-identity.

The §6.4 release gate is: every profile must be fully expressible here over the frozen Core with no
Core change. If a profile cannot be, that is a signal to fix Core *before* 1.0 — not to grow the wire.
"""
from __future__ import annotations

from . import model as m


def _numel(shape: tuple[int, ...]) -> int:
    n = 1
    for d in shape:
        n *= d
    return n


def embedding(model: str, vector: m.Tensor, *, id: str | None = None, meta: dict | None = None) -> dict:
    """An Embedding record as a plain Core Object (no new wire tag): the dense vector is a Core
    ``Tensor`` (64-byte aligned, zero-copy), wrapped with provenance. Field set is fixed so all
    records of a given ``(model, dtype, dim)`` share a structural fingerprint (route/dedup/drift)."""
    if not isinstance(vector, m.Tensor):
        raise TypeError("embedding vector must be a Core Tensor")
    if len(vector.shape) != 1:
        raise ValueError("embedding vector must be rank-1")
    obj: dict = {
        "model": model,
        "dim": vector.shape[0],
        "dtype": m.DTYPE_NAME[vector.dtype],
        "vector": vector,
    }
    if id is not None:
        obj["id"] = id
    if meta is not None:
        obj["meta"] = meta
    return obj


def media(kind: str, fmt: str, data: bytes, **meta) -> dict:
    """Image/Audio/Video as an OPAQUE blob envelope (§6.3) — Core models metadata only, never the codec.

    REVISIT LATER (not blessed): currently this is thin — Core's Bytes already gives the content address,
    so today it adds only a named-metadata convention. Worth revisiting if/when it earns a real capability
    (e.g. perceptual/near-duplicate fingerprinting). Kept for investigation, not part of the proven set."""
    if kind not in ("image", "audio", "video"):
        raise ValueError("media kind must be image|audio|video")
    obj = {"kind": kind, "format": fmt, "bytes": bytes(data)}
    obj.update(meta)
    return obj


def trace(trace_id: str, spans: list[dict]) -> dict:
    """An LLM/agent execution trace as Core Object/Array (OpenTelemetry-shaped spans). Tensors
    (logits/embeddings) may appear inline as Core Tensors. Hash-addressed -> reproducible eval."""
    return {"trace_id": trace_id, "spans": list(spans)}


def graph(nodes: list[dict], edges: list[dict]) -> dict:
    """A knowledge/agent-memory graph snapshot as Core objects/arrays (no graph wire variant).

    CANONICAL by construction: graphs have no inherent node/edge order, but Core arrays do — so nodes
    and edges are sorted by their canonical Cowrie encoding. Any permutation of the same node/edge set
    therefore yields the SAME content address (the promise now holds; verified by test_profiles)."""
    from .encode import encode as _canon
    return {"nodes": sorted(nodes, key=_canon), "edges": sorted(edges, key=_canon)}


def dataset_manifest(shards: list[dict]) -> dict:
    """The §7 stream/dataset layer as a profile: a manifest over content-addressed shards whose
    identity is a Merkle root OVER the per-file Merkle roots — one hash for a whole sharded dataset.
    Each shard is ``{uri, merkle_root: Bytes(34 multihash), count}``."""
    roots = [bytes(s["merkle_root"]) for s in shards]
    return {"shards": list(shards), "root": dataset_root(roots)}


def dataset_root(file_roots: list[bytes]) -> bytes:
    """Dataset identity = a Merkle root over the ordered per-file roots (a DAG: dataset -> files ->
    frames). Reuses the §7 Merkle construction so the whole tree is one consistent multihash."""
    from . import file as _file
    # Treat each 34-byte file multihash as a frame; merkle_root binds count + domain-separates.
    return _file.merkle_root(file_roots)

