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
