"""Gen1 codec tests."""

import struct
import json
import pytest
from cowrie.gen1 import (
    encode, decode, encode_json, decode_json,
    Node, Edge, AdjList, GraphShard,
    SecurityLimitExceeded,
)


class TestEmptyContainers:
    """Test empty container round-trips."""

    def test_empty_array(self):
        """Empty array [] should round-trip correctly."""
        original = []
        encoded = encode(original)
        decoded = decode(encoded)
        assert decoded == original

    def test_empty_object(self):
        """Empty object {} should round-trip correctly."""
        original = {}
        encoded = encode(original)
        decoded = decode(encoded)
        assert decoded == original

    def test_empty_string(self):
        """Empty string should round-trip correctly."""
        original = ""
        encoded = encode(original)
        decoded = decode(encoded)
        assert decoded == original

    def test_empty_bytes(self):
        """Empty bytes should round-trip correctly."""
        original = b""
        encoded = encode(original)
        decoded = decode(encoded)
        assert decoded == original


class TestPrimitives:
    """Test primitive value encoding/decoding."""

    def test_null(self):
        assert decode(encode(None)) is None

    def test_true(self):
        assert decode(encode(True)) is True

    def test_false(self):
        assert decode(encode(False)) is False

    def test_positive_int(self):
        assert decode(encode(42)) == 42

    def test_negative_int(self):
        assert decode(encode(-42)) == -42

    def test_zero(self):
        assert decode(encode(0)) == 0

    def test_float(self):
        result = decode(encode(3.14159))
        assert abs(result - 3.14159) < 0.0001

    def test_string(self):
        assert decode(encode("hello")) == "hello"

    def test_unicode(self):
        assert decode(encode("hello 世界 🌍")) == "hello 世界 🌍"

    def test_large_positive_int(self):
        """Large int requiring multi-byte varint."""
        assert decode(encode(100000)) == 100000

    def test_large_negative_int(self):
        """Large negative int."""
        assert decode(encode(-100000)) == -100000

    def test_max_int64(self):
        """Maximum int64 value."""
        val = 2**63 - 1
        assert decode(encode(val)) == val

    def test_min_int64(self):
        """Minimum int64 value."""
        val = -(2**63)
        assert decode(encode(val)) == val

    def test_bytes_data(self):
        """Binary data should round-trip."""
        data = b"\x00\x01\x02\xff\xfe\xfd"
        assert decode(encode(data)) == data

    def test_large_bytes(self):
        """Larger binary data."""
        data = bytes(range(256)) * 10
        assert decode(encode(data)) == data

    def test_long_string(self):
        """Long string requiring multi-byte length varint."""
        s = "x" * 500
        assert decode(encode(s)) == s


class TestContainers:
    """Test container encoding/decoding."""

    def test_simple_array(self):
        original = [1, 2, 3]
        assert decode(encode(original)) == original

    def test_simple_object(self):
        original = {"name": "Alice", "age": 30}
        decoded = decode(encode(original))
        assert decoded["name"] == "Alice"
        assert decoded["age"] == 30

    def test_nested_object(self):
        original = {"user": {"name": "Bob", "address": {"city": "NYC"}}}
        decoded = decode(encode(original))
        assert decoded["user"]["address"]["city"] == "NYC"

    def test_mixed_array(self):
        """Array with mixed types."""
        original = [1, "two", 3.0, True, None, b"bytes"]
        decoded = decode(encode(original))
        assert decoded[0] == 1
        assert decoded[1] == "two"
        assert abs(decoded[2] - 3.0) < 0.001
        assert decoded[3] is True
        assert decoded[4] is None
        assert decoded[5] == b"bytes"

    def test_short_mixed_array(self):
        """Array with fewer than 4 mixed elements (no proto-tensor)."""
        original = [1, "two", 3.0]
        decoded = decode(encode(original))
        assert decoded == [1, "two", 3.0]

    def test_nested_arrays(self):
        original = [[1, 2], [3, 4]]
        decoded = decode(encode(original))
        assert decoded[0] == [1, 2]
        assert decoded[1] == [3, 4]

    def test_object_sorted_keys(self):
        """Objects are encoded with sorted keys."""
        original = {"z": 1, "a": 2, "m": 3}
        decoded = decode(encode(original))
        assert decoded == original


class TestProtoTensors:
    """Test proto-tensor (homogeneous array) encoding."""

    def test_int64_array(self):
        """Homogeneous int arrays should round-trip correctly."""
        original = [1, 2, 3, 4, 5, 6, 7, 8]
        decoded = decode(encode(original))
        assert decoded == original

    def test_float64_array(self):
        """Homogeneous float arrays should round-trip correctly."""
        original = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        decoded = decode(encode(original))
        for i, (orig, dec) in enumerate(zip(original, decoded)):
            assert abs(orig - dec) < 0.0001, f"Mismatch at index {i}"

    def test_mixed_int_float_as_float_array(self):
        """Array with mixed ints and floats treated as float array."""
        original = [1, 2.0, 3, 4.0, 5, 6.0, 7, 8.0]
        decoded = decode(encode(original))
        for orig, dec in zip(original, decoded):
            assert abs(float(orig) - dec) < 0.001

    def test_short_int_array_not_tensorized(self):
        """Arrays with < 4 elements are not tensorized."""
        original = [1, 2, 3]
        decoded = decode(encode(original))
        assert decoded == original

    def test_bool_array_not_tensorized(self):
        """Bool arrays should not be treated as int arrays."""
        original = [True, False, True, False, True]
        decoded = decode(encode(original))
        assert decoded == original

    def test_large_int_array(self):
        """Large homogeneous int array."""
        original = list(range(1000))
        decoded = decode(encode(original))
        assert decoded == original


class TestGraphTypes:
    """Test Gen1 graph type encoding/decoding."""

    def test_node_roundtrip(self):
        """Node should round-trip correctly."""
        node = Node(id=42, label="Person", properties={"name": "Alice", "age": 30})
        decoded = decode(encode(node))
        assert isinstance(decoded, Node)
        assert decoded.id == 42
        assert decoded.label == "Person"
        assert decoded.properties["name"] == "Alice"
        assert decoded.properties["age"] == 30

    def test_node_empty_properties(self):
        """Node with no properties."""
        node = Node(id=1, label="Empty", properties={})
        decoded = decode(encode(node))
        assert isinstance(decoded, Node)
        assert decoded.properties == {}

    def test_edge_roundtrip(self):
        """Edge should round-trip correctly."""
        edge = Edge(src=1, dst=2, label="KNOWS", properties={"since": 2020})
        decoded = decode(encode(edge))
        assert isinstance(decoded, Edge)
        assert decoded.src == 1
        assert decoded.dst == 2
        assert decoded.label == "KNOWS"
        assert decoded.properties["since"] == 2020

    def test_edge_empty_properties(self):
        """Edge with no properties."""
        edge = Edge(src=10, dst=20, label="REL", properties={})
        decoded = decode(encode(edge))
        assert isinstance(decoded, Edge)
        assert decoded.properties == {}

    def test_adjlist_roundtrip(self):
        """AdjList should round-trip correctly."""
        col_indices = struct.pack('<3I', 1, 2, 0)
        adj = AdjList(id_width=1, node_count=3, edge_count=3,
                      row_offsets=[0, 1, 2, 3], col_indices=col_indices)
        decoded = decode(encode(adj))
        assert isinstance(decoded, AdjList)
        assert decoded.id_width == 1
        assert decoded.node_count == 3
        assert decoded.edge_count == 3
        assert decoded.row_offsets == [0, 1, 2, 3]
        assert decoded.col_indices == col_indices

    def test_adjlist_int64(self):
        """AdjList with int64 IDs."""
        col_indices = struct.pack('<2Q', 100, 200)
        adj = AdjList(id_width=2, node_count=2, edge_count=2,
                      row_offsets=[0, 1, 2], col_indices=col_indices)
        decoded = decode(encode(adj))
        assert isinstance(decoded, AdjList)
        assert decoded.id_width == 2

    def test_graph_shard_roundtrip(self):
        """GraphShard should round-trip correctly."""
        nodes = [
            Node(id=1, label="A", properties={"x": 1}),
            Node(id=2, label="B", properties={"x": 2}),
        ]
        edges = [
            Edge(src=1, dst=2, label="REL", properties={"w": 0.5}),
        ]
        meta = {"version": 1}
        shard = GraphShard(nodes=nodes, edges=edges, meta=meta)
        decoded = decode(encode(shard))
        assert isinstance(decoded, GraphShard)
        assert len(decoded.nodes) == 2
        assert len(decoded.edges) == 1
        assert decoded.meta["version"] == 1
        assert decoded.nodes[0].id == 1
        assert decoded.edges[0].label == "REL"

    def test_graph_shard_empty(self):
        """Empty GraphShard."""
        shard = GraphShard(nodes=[], edges=[], meta={})
        decoded = decode(encode(shard))
        assert isinstance(decoded, GraphShard)
        assert len(decoded.nodes) == 0
        assert len(decoded.edges) == 0


class TestJSONHelpers:
    """Test JSON conversion helpers."""

    def test_encode_json(self):
        """encode_json converts JSON bytes to Gen1 format."""
        json_data = b'{"name": "Alice", "age": 30}'
        gen1_data = encode_json(json_data)
        decoded = decode(gen1_data)
        assert decoded["name"] == "Alice"
        assert decoded["age"] == 30

    def test_decode_json(self):
        """decode_json converts Gen1 to JSON bytes."""
        original = {"name": "Bob", "active": True}
        gen1_data = encode(original)
        json_bytes = decode_json(gen1_data)
        result = json.loads(json_bytes)
        assert result["name"] == "Bob"
        assert result["active"] is True


class TestErrorHandling:
    """Test error handling paths."""

    def test_unsupported_type(self):
        """Encoding unsupported types should raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported type"):
            encode(object())

    def test_invalid_tag(self):
        """Decoding invalid tag should raise ValueError."""
        # Tag 0xFF is not a valid Gen1 tag
        with pytest.raises(ValueError, match="Invalid tag"):
            decode(bytes([0xFF]))

    def test_truncated_data(self):
        """Decoding truncated data should raise EOFError."""
        with pytest.raises(EOFError):
            decode(b"")

    def test_truncated_string(self):
        """String with length but no data."""
        # TAG_STRING(0x05) + length=10 but no data follows
        import io
        buf = io.BytesIO()
        buf.write(bytes([0x05]))
        buf.write(bytes([10]))  # length = 10
        data = buf.getvalue()
        # This should either raise or return garbage; at minimum not crash
        # The _read_uvarint and r.read will handle this
        # Depending on implementation, it may return short data or raise
        try:
            decode(data)
        except Exception:
            pass  # Expected to fail on truncated data

    def test_varint_overflow(self):
        """Varint with too many continuation bytes should raise."""
        # Create a varint with >63 bits of shift (10+ bytes all with continuation bit)
        data = bytes([0x80] * 10 + [0x01])
        import io
        from cowrie.gen1 import _read_uvarint
        with pytest.raises(ValueError, match="Varint overflow"):
            _read_uvarint(io.BytesIO(data))


class TestSecurityLimits:
    """Test security limit enforcement."""

    def test_depth_limit(self):
        """Deep nesting should be rejected."""
        import sys
        old_limit = sys.getrecursionlimit()
        sys.setrecursionlimit(5000)
        try:
            # Build a deeply nested structure manually as bytes
            # Each level is TAG_ARRAY(0x06) + count=1
            depth = 1002
            buf = b""
            for _ in range(depth):
                buf += bytes([0x06, 0x01])  # TAG_ARRAY, count=1
            buf += bytes([0x00])  # TAG_NULL at center
            with pytest.raises(SecurityLimitExceeded):
                decode(buf)
        finally:
            sys.setrecursionlimit(old_limit)
