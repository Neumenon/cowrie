"""§6.4 profile-simulation gate: a profile MUST be fully expressible over the LOCKED Core with no new
wire tag, and inherit the Core guarantees (canonical round-trip, content address, structural
fingerprint, zero-copy tensor). Passing this for the shipped profiles is a 1.0 release gate.
"""
from __future__ import annotations

import struct

import cowrie_ref as c
from cowrie_ref import Bitmask, Datetime, Decimal128, Extension, Tensor, Uuid
from cowrie_ref import profiles as p

# The Core value types. "No new wire tag" == a profile value is built ONLY from these, so the canonical
# encoder (which raises on any non-Core type) accepts it with zero Core change.
CORE_TYPES = (type(None), bool, int, float, str, bytes,
              Decimal128, Datetime, Uuid, Tensor, Bitmask, Extension, list, dict)


def _is_core(v) -> bool:
    if isinstance(v, list):
        return all(_is_core(x) for x in v)
    if isinstance(v, dict):
        return all(isinstance(k, str) and _is_core(x) for k, x in v.items())
    return isinstance(v, CORE_TYPES)


def _f32(vals):
    return Tensor(0x01, (len(vals),), b"".join(struct.pack("<f", x) for x in vals))


def test_embedding_expressible_over_core_no_new_tag() -> None:
    rec = p.embedding("text-embed-3", _f32([0.1, 0.2, 0.3, 0.4]), id="doc-1", meta={"src": "kb"})
    assert _is_core(rec)                               # built only from Core types -> no new wire tag
    blob = c.encode(rec)                               # ... so the locked Core encoder accepts it
    assert c.value_eq(c.decode(blob), rec)             # canonical round-trip (bijectivity)
    assert c.encode(c.decode(blob)) == blob            # byte-stable
    # the dense vector inherits Core's zero-copy 64-byte alignment
    spans = c.tensor_spans(blob)
    assert len(spans) == 1 and spans[0][2] % 64 == 0


def test_embedding_inherits_identity_and_fingerprint() -> None:
    a = p.embedding("m", _f32([1.0, 2.0]), id="x")
    b = p.embedding("m", _f32([3.0, 4.0]), id="y")     # same shape/field-set, different vector + id
    assert c.fingerprint(a) == c.fingerprint(b)        # route/dedup/drift a homogeneous stream by shape
    assert c.content_address(a) != c.content_address(b)  # per-value identity
    assert c.fingerprint(a) != c.fingerprint(p.embedding("m", _f32([1.0, 2.0, 3.0])))  # different dim
