"""Graph JSON encode-after-roundtrip parity.

Mirrors go/graph_json_roundtrip_test.go (review gap #2). The JSON bridge
reconstructs graph values whose numeric props must be normalized to codec-safe
types; this asserts the reconstructed value also ENCODES — the Go bug was
encode-after-JSON failing, which a pure to_json/from_json round-trip would not
catch. Confirms the Python bridge has no analogous bug.
"""
from cowrie.gen2 import (
    Value, Type, to_json, from_json, encode, decode, NodeData, EdgeData,
)


def _json_then_encode(v: Value) -> Value:
    """to_json -> from_json -> encode -> decode: the full bridge + binary path."""
    return decode(encode(from_json(to_json(v))))


def test_node_encode_after_json():
    n = Value.node("n1", ["Person"], {
        "age": Value.int64(42),
        "score": Value.float64(3.5),
        "name": Value.string("Alice"),
    })
    d = _json_then_encode(n)
    assert d.type == Type.NODE
    assert d.data.props["age"].data == 42
    assert d.data.props["score"].data == 3.5
    assert d.data.props["name"].data == "Alice"


def test_edge_encode_after_json():
    e = Value.edge("a", "b", "KNOWS", {
        "weight": Value.int64(7),
        "w2": Value.float64(1.5),
    })
    d = _json_then_encode(e)
    assert d.type == Type.EDGE
    assert d.data.props["weight"].data == 7


def test_node_batch_encode_after_json():
    nb = Value.node_batch([
        NodeData(id="n1", labels=["P"], props={"age": Value.int64(1), "f": Value.float64(2.5)}),
    ])
    d = _json_then_encode(nb)
    assert d.type == Type.NODE_BATCH


def test_edge_batch_encode_after_json():
    eb = Value.edge_batch([
        EdgeData(from_id="a", to_id="b", edge_type="R", props={"w": Value.int64(2)}),
    ])
    d = _json_then_encode(eb)
    assert d.type == Type.EDGE_BATCH
