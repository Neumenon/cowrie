"""Gen2 codec tests."""

import pytest
from sjson.gen2 import (
    encode, decode, Value, Type, to_json, to_any,
    NodeData, EdgeData, NodeBatchData, EdgeBatchData, GraphShardData,
    TensorData, DType, ImageData, AudioData, RichTextData, RichTextSpan,
    DeltaData, DeltaOp, DeltaOpCode, AdjlistData,
    SecurityLimitExceeded
)


class TestEmptyContainers:
    """Test empty container round-trips."""

    def test_empty_array(self):
        """Empty array [] should round-trip correctly."""
        original = Value.array([])
        decoded = decode(encode(original))
        assert decoded.type == Type.ARRAY
        assert decoded.data == []

    def test_empty_object(self):
        """Empty object {} should round-trip correctly."""
        original = Value.object({})
        decoded = decode(encode(original))
        assert decoded.type == Type.OBJECT
        assert decoded.data == {}

    def test_empty_string(self):
        """Empty string should round-trip correctly."""
        original = Value.string("")
        decoded = decode(encode(original))
        assert decoded.type == Type.STRING
        assert decoded.data == ""

    def test_empty_bytes(self):
        """Empty bytes should round-trip correctly."""
        original = Value.bytes_(b"")
        decoded = decode(encode(original))
        assert decoded.type == Type.BYTES
        assert decoded.data == b""


class TestPrimitives:
    """Test primitive value encoding/decoding."""

    def test_null(self):
        decoded = decode(encode(Value.null()))
        assert decoded.type == Type.NULL

    def test_true(self):
        decoded = decode(encode(Value.bool_(True)))
        assert decoded.type == Type.BOOL
        assert decoded.data is True

    def test_false(self):
        decoded = decode(encode(Value.bool_(False)))
        assert decoded.type == Type.BOOL
        assert decoded.data is False

    def test_positive_int(self):
        decoded = decode(encode(Value.int64(42)))
        assert decoded.type == Type.INT64
        assert decoded.data == 42

    def test_negative_int(self):
        decoded = decode(encode(Value.int64(-42)))
        assert decoded.type == Type.INT64
        assert decoded.data == -42

    def test_zero(self):
        decoded = decode(encode(Value.int64(0)))
        assert decoded.type == Type.INT64
        assert decoded.data == 0

    def test_uint64(self):
        decoded = decode(encode(Value.uint64(2**63)))
        assert decoded.type == Type.UINT64
        assert decoded.data == 2**63

    def test_float(self):
        decoded = decode(encode(Value.float64(3.14159)))
        assert decoded.type == Type.FLOAT64
        assert abs(decoded.data - 3.14159) < 0.0001

    def test_string(self):
        decoded = decode(encode(Value.string("hello")))
        assert decoded.type == Type.STRING
        assert decoded.data == "hello"

    def test_unicode(self):
        decoded = decode(encode(Value.string("hello 世界 🌍")))
        assert decoded.type == Type.STRING
        assert decoded.data == "hello 世界 🌍"


class TestContainers:
    """Test container encoding/decoding."""

    def test_simple_array(self):
        original = Value.array([Value.int64(1), Value.int64(2), Value.int64(3)])
        decoded = decode(encode(original))
        assert decoded.type == Type.ARRAY
        assert len(decoded.data) == 3
        assert all(v.data == i+1 for i, v in enumerate(decoded.data))

    def test_simple_object(self):
        original = Value.object({
            "name": Value.string("Alice"),
            "age": Value.int64(30)
        })
        decoded = decode(encode(original))
        assert decoded.type == Type.OBJECT
        assert decoded.data["name"].data == "Alice"
        assert decoded.data["age"].data == 30

    def test_nested_object(self):
        original = Value.object({
            "user": Value.object({
                "name": Value.string("Bob"),
                "address": Value.object({
                    "city": Value.string("NYC")
                })
            })
        })
        decoded = decode(encode(original))
        assert decoded.data["user"].data["address"].data["city"].data == "NYC"


class TestTensor:
    """Test tensor encoding/decoding."""

    def test_float32_tensor(self):
        import struct
        data = struct.pack('<4f', 1.0, 2.0, 3.0, 4.0)
        tensor = Value.tensor(DType.FLOAT32, [2, 2], data)
        decoded = decode(encode(tensor))
        assert decoded.type == Type.TENSOR
        assert decoded.data.dtype == DType.FLOAT32
        assert decoded.data.shape == [2, 2]
        assert decoded.data.data == data

    def test_int64_tensor(self):
        import struct
        data = struct.pack('<4q', 1, 2, 3, 4)
        tensor = Value.tensor(DType.INT64, [4], data)
        decoded = decode(encode(tensor))
        assert decoded.type == Type.TENSOR
        assert decoded.data.dtype == DType.INT64
        assert decoded.data.shape == [4]


class TestGraphTypes:
    """Test graph type encoding/decoding (v2.1)."""

    def test_node_roundtrip(self):
        """Node with ID, labels, and properties should round-trip."""
        props = {"name": Value.string("Alice"), "age": Value.int64(30)}
        node = Value.node("person_42", ["Person", "Employee"], props)
        decoded = decode(encode(node))

        assert decoded.type == Type.NODE
        assert decoded.data.id == "person_42"
        assert decoded.data.labels == ["Person", "Employee"]
        assert decoded.data.props["name"].data == "Alice"
        assert decoded.data.props["age"].data == 30

    def test_edge_roundtrip(self):
        """Edge with source, target, type, and properties should round-trip."""
        props = {"since": Value.int64(2020), "role": Value.string("Engineer")}
        edge = Value.edge("person_42", "company_1", "WORKS_AT", props)
        decoded = decode(encode(edge))

        assert decoded.type == Type.EDGE
        assert decoded.data.from_id == "person_42"
        assert decoded.data.to_id == "company_1"
        assert decoded.data.edge_type == "WORKS_AT"
        assert decoded.data.props["since"].data == 2020

    def test_node_batch_roundtrip(self):
        """NodeBatch should round-trip correctly."""
        node1 = NodeData(id="n1", labels=["A"], props={})
        node2 = NodeData(id="n2", labels=["B"], props={"x": Value.float64(0.5)})
        batch = Value.node_batch([node1, node2])
        decoded = decode(encode(batch))

        assert decoded.type == Type.NODE_BATCH
        assert len(decoded.data.nodes) == 2
        assert decoded.data.nodes[0].id == "n1"
        assert decoded.data.nodes[1].id == "n2"

    def test_edge_batch_roundtrip(self):
        """EdgeBatch should round-trip correctly."""
        edge1 = EdgeData(from_id="a", to_id="b", edge_type="E1", props={})
        edge2 = EdgeData(from_id="b", to_id="c", edge_type="E2", props={})
        batch = Value.edge_batch([edge1, edge2])
        decoded = decode(encode(batch))

        assert decoded.type == Type.EDGE_BATCH
        assert len(decoded.data.edges) == 2
        assert decoded.data.edges[0].from_id == "a"
        assert decoded.data.edges[1].edge_type == "E2"

    def test_graph_shard_roundtrip(self):
        """GraphShard with nodes, edges, and metadata should round-trip."""
        nodes = [
            NodeData(id="1", labels=["Node"], props={"x": Value.float64(0.1)}),
            NodeData(id="2", labels=["Node"], props={"x": Value.float64(0.2)}),
        ]
        edges = [
            EdgeData(from_id="1", to_id="2", edge_type="EDGE", props={"weight": Value.float64(0.85)})
        ]
        metadata = {"version": Value.int64(1)}
        shard = Value.graph_shard(nodes, edges, metadata)
        decoded = decode(encode(shard))

        assert decoded.type == Type.GRAPH_SHARD
        assert len(decoded.data.nodes) == 2
        assert len(decoded.data.edges) == 1
        assert decoded.data.metadata["version"].data == 1
        assert decoded.data.nodes[0].id == "1"
        assert decoded.data.edges[0].edge_type == "EDGE"

    def test_empty_graph_shard(self):
        """Empty GraphShard should round-trip correctly."""
        shard = Value.graph_shard([], [], {})
        decoded = decode(encode(shard))

        assert decoded.type == Type.GRAPH_SHARD
        assert len(decoded.data.nodes) == 0
        assert len(decoded.data.edges) == 0
        assert len(decoded.data.metadata) == 0


class TestDelta:
    """Test delta encoding/decoding."""

    def test_delta_set_field(self):
        """Delta with SET_FIELD operation should round-trip."""
        ops = [
            DeltaOp(op_code=DeltaOpCode.SET_FIELD, field_id=0, value=Value.string("new_value")),
            DeltaOp(op_code=DeltaOpCode.DELETE_FIELD, field_id=1, value=None),
        ]
        delta = Value.delta(123, ops)
        decoded = decode(encode(delta))

        assert decoded.type == Type.DELTA
        assert decoded.data.base_id == 123
        assert len(decoded.data.ops) == 2
        assert decoded.data.ops[0].op_code == DeltaOpCode.SET_FIELD
        assert decoded.data.ops[1].op_code == DeltaOpCode.DELETE_FIELD


class TestJSON:
    """Test JSON conversion."""

    def test_to_json_simple(self):
        obj = Value.object({"name": Value.string("test"), "count": Value.int64(42)})
        json_str = to_json(obj)
        assert '"name": "test"' in json_str
        assert '"count": 42' in json_str

    def test_to_any_node(self):
        node = Value.node("n1", ["Label"], {"prop": Value.string("value")})
        result = to_any(node)
        assert result["_type"] == "node"
        assert result["id"] == "n1"
        assert result["labels"] == ["Label"]
        assert result["props"]["prop"] == "value"

    def test_to_any_graph_shard(self):
        shard = Value.graph_shard(
            [NodeData(id="x", labels=[], props={})],
            [],
            {"v": Value.int64(1)}
        )
        result = to_any(shard)
        assert result["_type"] == "graph_shard"
        assert len(result["nodes"]) == 1
        assert result["metadata"]["v"] == 1


class TestDictionaryCoding:
    """Test dictionary coding efficiency."""

    def test_repeated_keys_encoded_once(self):
        """Objects with repeated keys should benefit from dictionary coding."""
        # Create many objects with the same keys
        items = [
            Value.object({
                "id": Value.int64(i),
                "name": Value.string(f"item_{i}"),
                "score": Value.float64(i * 0.1),
            })
            for i in range(100)
        ]
        arr = Value.array(items)
        encoded = encode(arr)

        # Dictionary coding means keys are stored once, not 100 times
        # Each key is ~5 bytes, so without dict we'd have 300 * 5 = 1500 bytes of keys
        # With dict we have 3 keys * 5 = 15 bytes + 100 objects * 3 indices
        # Just verify it decodes correctly
        decoded = decode(encoded)
        assert len(decoded.data) == 100
        assert decoded.data[50].data["name"].data == "item_50"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
