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


def test_media_is_opaque_envelope_over_core() -> None:
    img = p.media("image", "png", b"\x89PNG\r\n", width=2, height=2)
    assert _is_core(img)
    blob = c.encode(img)
    assert c.encode(c.decode(blob)) == blob
    # same media bytes -> same content address (dedup across a dataset)
    assert c.content_address(img) == c.content_address(p.media("image", "png", b"\x89PNG\r\n", width=2, height=2))


def test_trace_and_graph_expressible_and_addressed() -> None:
    tr = p.trace("t1", [{"name": "llm", "start": 0, "end": 5, "tokens": 12},
                        {"name": "tool", "start": 5, "end": 9}])
    g = p.graph([{"id": "a", "labels": ["X"], "props": {"k": 1}}],
                [{"src": "a", "dst": "a", "type": "self", "props": {}}])
    for v in (tr, g):
        assert _is_core(v)
        assert c.encode(c.decode(c.encode(v))) == c.encode(v)  # byte-stable round-trip
    # same graph content -> same address regardless of construction order (Core sorts keys)
    g2 = p.graph([{"props": {"k": 1}, "labels": ["X"], "id": "a"}],
                 [{"props": {}, "type": "self", "dst": "a", "src": "a"}])
    assert c.content_address(g) == c.content_address(g2)


def test_training_batch_tensors_zero_copy_aligned() -> None:
    import struct
    feats = Tensor(0x01, (2, 3), b"".join(struct.pack("<f", i) for i in range(6)))
    labels = Tensor(0x04, (2,), b"\x00\x01")
    batch = p.training_batch(feats, labels, meta={"epoch": 1})
    blob = c.encode(batch)
    assert all(off % 64 == 0 for (_d, _s, off, _l) in c.tensor_spans(blob))  # both tensors aligned


def test_dataset_manifest_is_merkle_dag_over_file_roots() -> None:
    from cowrie_ref import file as F
    r1 = F.merkle_root([c.encode({"a": 1})])
    r2 = F.merkle_root([c.encode([1, 2, 3])])
    man = p.dataset_manifest([{"uri": "s0.cwrf", "merkle_root": r1, "count": 1},
                              {"uri": "s1.cwrf", "merkle_root": r2, "count": 1}])
    assert _is_core(man) and c.encode(c.decode(c.encode(man))) == c.encode(man)
    # dataset identity composes the file roots: change/reorder a shard -> different dataset root
    assert p.dataset_root([r1, r2]) == man["root"]
    assert p.dataset_root([r1, r2]) != p.dataset_root([r2, r1])           # order matters
    assert p.dataset_root([r1, r2]) != p.dataset_root([r1, r1])           # content matters
