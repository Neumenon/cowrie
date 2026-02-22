"""Gen1: Lightweight binary JSON codec with proto-tensor support.

Gen1 provides a compact binary JSON format that's simpler than Gen2:
- 11 core types (null, bool, int64, float64, string, bytes, object, arrays)
- Proto-tensor support for efficient numeric arrays
- 6 graph types (Node, Edge, AdjList, batches)
- No dictionary coding (simpler implementation)

Example
-------
>>> from cowrie.gen1 import encode, decode
>>> data = encode({"name": "Alice", "scores": [1.0, 2.0, 3.0]})
>>> decoded = decode(data)
>>> decoded
{'name': 'Alice', 'scores': [1.0, 2.0, 3.0]}
"""

import struct
import io
from typing import Any, Dict, List, Union, Tuple, Optional
from dataclasses import dataclass

# Type tags
TAG_NULL = 0x00
TAG_FALSE = 0x01
TAG_TRUE = 0x02
TAG_INT64 = 0x03
TAG_FLOAT64 = 0x04
TAG_STRING = 0x05
TAG_BYTES = 0x06
TAG_ARRAY = 0x07
TAG_OBJECT = 0x08
# Proto-tensor types
TAG_INT64_ARRAY = 0x09
TAG_FLOAT64_ARRAY = 0x0A
TAG_STRING_ARRAY = 0x0B
# Graph types
TAG_NODE = 0x10
TAG_EDGE = 0x11
TAG_ADJLIST = 0x12
TAG_NODE_BATCH = 0x13
TAG_EDGE_BATCH = 0x14
TAG_GRAPH_SHARD = 0x15

# Security limits - prevent DoS from malicious input
MAX_DEPTH = 1000
MAX_ARRAY_LEN = 100_000_000  # 100M elements
MAX_OBJECT_LEN = 10_000_000  # 10M fields
MAX_STRING_LEN = 500_000_000  # 500MB
MAX_BYTES_LEN = 1_000_000_000  # 1GB


class SecurityLimitExceeded(ValueError):
    """Raised when a security limit is exceeded during decode."""
    pass


@dataclass
class Node:
    """Graph node with id, label, and properties."""
    id: int
    label: str
    properties: Dict[str, Any]


@dataclass
class Edge:
    """Graph edge with src, dst, label, and properties."""
    src: int
    dst: int
    label: str
    properties: Dict[str, Any]


@dataclass
class AdjList:
    """CSR adjacency list representation."""
    id_width: int  # 1=int32, 2=int64
    node_count: int
    edge_count: int
    row_offsets: List[int]
    col_indices: bytes


@dataclass
class GraphShard:
    """Graph shard with nodes, edges, and metadata."""
    nodes: List[Node]
    edges: List[Edge]
    meta: Dict[str, Any]


def _write_uvarint(w: io.BytesIO, n: int) -> None:
    """Write unsigned varint.

    Optimized: uses bytearray to avoid creating intermediate bytes objects.
    """
    if n < 0x80:
        # Fast path for single-byte varints (0-127)
        w.write(bytes((n,)))
        return

    buf = bytearray()
    while n >= 0x80:
        buf.append((n & 0x7F) | 0x80)
        n >>= 7
    buf.append(n)
    w.write(bytes(buf))


def _read_uvarint(r: io.BytesIO) -> int:
    """Read unsigned varint.

    Optimized: fast path for single-byte varints (most common case).
    """
    b = r.read(1)
    if not b:
        raise EOFError("Unexpected end of data")
    byte = b[0]

    # Fast path: single-byte varint (0-127) - most common case
    if byte < 0x80:
        return byte

    # Multi-byte varint
    result = byte & 0x7F
    shift = 7
    while True:
        b = r.read(1)
        if not b:
            raise EOFError("Unexpected end of data")
        byte = b[0]
        result |= (byte & 0x7F) << shift
        if byte < 0x80:
            return result
        shift += 7
        if shift > 63:
            raise ValueError("Varint overflow")


def _zigzag_encode(n: int) -> int:
    """Zigzag encode signed int to unsigned.

    Apply 64-bit masking consistently to handle Python's arbitrary precision integers.
    """
    return ((n << 1) ^ (n >> 63)) & 0xFFFFFFFFFFFFFFFF


def _zigzag_decode(n: int) -> int:
    """Zigzag decode to signed int."""
    return (n >> 1) ^ -(n & 1)


def _is_homogeneous_int_array(arr: list) -> bool:
    """Check if array is homogeneous ints (for proto-tensor)."""
    if len(arr) < 4:  # Only tensorize if 4+ elements
        return False
    return all(isinstance(x, int) and not isinstance(x, bool) for x in arr)


def _is_homogeneous_float_array(arr: list) -> bool:
    """Check if array is homogeneous floats (for proto-tensor)."""
    if len(arr) < 4:  # Only tensorize if 4+ elements
        return False
    return all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in arr)


def encode(value: Any) -> bytes:
    """Encode a Python value to Gen1 binary format.

    Args:
        value: Any JSON-compatible Python value

    Returns:
        bytes: Encoded binary data
    """
    buf = io.BytesIO()
    _encode_value(buf, value)
    return buf.getvalue()


def _encode_value(w: io.BytesIO, value: Any) -> None:
    """Encode a single value."""
    if value is None:
        w.write(bytes([TAG_NULL]))
    elif isinstance(value, bool):
        w.write(bytes([TAG_TRUE if value else TAG_FALSE]))
    elif isinstance(value, int):
        w.write(bytes([TAG_INT64]))
        _write_uvarint(w, _zigzag_encode(value))
    elif isinstance(value, float):
        w.write(bytes([TAG_FLOAT64]))
        w.write(struct.pack('<d', value))
    elif isinstance(value, str):
        w.write(bytes([TAG_STRING]))
        encoded = value.encode('utf-8')
        _write_uvarint(w, len(encoded))
        w.write(encoded)
    elif isinstance(value, bytes):
        w.write(bytes([TAG_BYTES]))
        _write_uvarint(w, len(value))
        w.write(value)
    elif isinstance(value, list):
        # Check for proto-tensor opportunities
        if _is_homogeneous_float_array(value):
            w.write(bytes([TAG_FLOAT64_ARRAY]))
            _write_uvarint(w, len(value))
            for v in value:
                w.write(struct.pack('<d', float(v)))
        elif _is_homogeneous_int_array(value):
            w.write(bytes([TAG_INT64_ARRAY]))
            _write_uvarint(w, len(value))
            for v in value:
                w.write(struct.pack('<q', v))
        else:
            w.write(bytes([TAG_ARRAY]))
            _write_uvarint(w, len(value))
            for item in value:
                _encode_value(w, item)
    elif isinstance(value, dict):
        w.write(bytes([TAG_OBJECT]))
        _write_uvarint(w, len(value))
        # Sort keys for deterministic output
        for key in sorted(value.keys()):
            encoded_key = key.encode('utf-8')
            _write_uvarint(w, len(encoded_key))
            w.write(encoded_key)
            _encode_value(w, value[key])
    elif isinstance(value, Node):
        w.write(bytes([TAG_NODE]))
        _write_uvarint(w, _zigzag_encode(value.id))
        encoded_label = value.label.encode('utf-8')
        _write_uvarint(w, len(encoded_label))
        w.write(encoded_label)
        _write_uvarint(w, len(value.properties))
        for key, val in value.properties.items():
            encoded_key = key.encode('utf-8')
            _write_uvarint(w, len(encoded_key))
            w.write(encoded_key)
            _encode_value(w, val)
    elif isinstance(value, Edge):
        w.write(bytes([TAG_EDGE]))
        _write_uvarint(w, _zigzag_encode(value.src))
        _write_uvarint(w, _zigzag_encode(value.dst))
        encoded_label = value.label.encode('utf-8')
        _write_uvarint(w, len(encoded_label))
        w.write(encoded_label)
        _write_uvarint(w, len(value.properties))
        for key, val in value.properties.items():
            encoded_key = key.encode('utf-8')
            _write_uvarint(w, len(encoded_key))
            w.write(encoded_key)
            _encode_value(w, val)
    elif isinstance(value, AdjList):
        w.write(bytes([TAG_ADJLIST]))
        w.write(bytes([value.id_width]))
        _write_uvarint(w, value.node_count)
        _write_uvarint(w, value.edge_count)
        for offset in value.row_offsets:
            _write_uvarint(w, offset)
        w.write(value.col_indices)
    elif isinstance(value, GraphShard):
        w.write(bytes([TAG_GRAPH_SHARD]))
        _write_uvarint(w, len(value.nodes))
        for node in value.nodes:
            _encode_value(w, node)
        _write_uvarint(w, len(value.edges))
        for edge in value.edges:
            _encode_value(w, edge)
        _write_uvarint(w, len(value.meta))
        for key, val in value.meta.items():
            encoded_key = key.encode('utf-8')
            _write_uvarint(w, len(encoded_key))
            w.write(encoded_key)
            _encode_value(w, val)
    else:
        raise ValueError(f"Unsupported type: {type(value)}")


def decode(data: bytes) -> Any:
    """Decode Gen1 binary data to a Python value.

    Args:
        data: Binary data to decode

    Returns:
        Decoded Python value

    Raises:
        SecurityLimitExceeded: If security limits are exceeded
    """
    r = io.BytesIO(data)
    return _decode_value(r, depth=0)


def _decode_value(r: io.BytesIO, depth: int = 0) -> Any:
    """Decode a single value."""
    # Security: check depth limit
    if depth > MAX_DEPTH:
        raise SecurityLimitExceeded(f"Maximum nesting depth exceeded: {MAX_DEPTH}")

    tag = r.read(1)
    if not tag:
        raise EOFError("Unexpected end of data")
    tag = tag[0]

    if tag == TAG_NULL:
        return None
    elif tag == TAG_FALSE:
        return False
    elif tag == TAG_TRUE:
        return True
    elif tag == TAG_INT64:
        return _zigzag_decode(_read_uvarint(r))
    elif tag == TAG_FLOAT64:
        return struct.unpack('<d', r.read(8))[0]
    elif tag == TAG_STRING:
        length = _read_uvarint(r)
        if length > MAX_STRING_LEN:
            raise SecurityLimitExceeded(f"String too long: {length} > {MAX_STRING_LEN}")
        return r.read(length).decode('utf-8')
    elif tag == TAG_BYTES:
        length = _read_uvarint(r)
        if length > MAX_BYTES_LEN:
            raise SecurityLimitExceeded(f"Bytes too long: {length} > {MAX_BYTES_LEN}")
        return r.read(length)
    elif tag == TAG_ARRAY:
        count = _read_uvarint(r)
        if count > MAX_ARRAY_LEN:
            raise SecurityLimitExceeded(f"Array too large: {count} > {MAX_ARRAY_LEN}")
        return [_decode_value(r, depth + 1) for _ in range(count)]
    elif tag == TAG_OBJECT:
        count = _read_uvarint(r)
        if count > MAX_OBJECT_LEN:
            raise SecurityLimitExceeded(f"Object too large: {count} > {MAX_OBJECT_LEN}")
        result = {}
        for _ in range(count):
            key_len = _read_uvarint(r)
            if key_len > MAX_STRING_LEN:
                raise SecurityLimitExceeded(f"String too long: {key_len} > {MAX_STRING_LEN}")
            key = r.read(key_len).decode('utf-8')
            result[key] = _decode_value(r, depth + 1)
        return result
    elif tag == TAG_INT64_ARRAY:
        count = _read_uvarint(r)
        return [struct.unpack('<q', r.read(8))[0] for _ in range(count)]
    elif tag == TAG_FLOAT64_ARRAY:
        count = _read_uvarint(r)
        return [struct.unpack('<d', r.read(8))[0] for _ in range(count)]
    elif tag == TAG_STRING_ARRAY:
        count = _read_uvarint(r)
        result = []
        for _ in range(count):
            length = _read_uvarint(r)
            result.append(r.read(length).decode('utf-8'))
        return result
    elif tag == TAG_NODE:
        node_id = _zigzag_decode(_read_uvarint(r))
        label_len = _read_uvarint(r)
        label = r.read(label_len).decode('utf-8')
        prop_count = _read_uvarint(r)
        if prop_count > MAX_OBJECT_LEN:
            raise SecurityLimitExceeded(f"Properties too large: {prop_count} > {MAX_OBJECT_LEN}")
        properties = {}
        for _ in range(prop_count):
            key_len = _read_uvarint(r)
            key = r.read(key_len).decode('utf-8')
            properties[key] = _decode_value(r, depth + 1)
        return Node(id=node_id, label=label, properties=properties)
    elif tag == TAG_EDGE:
        src = _zigzag_decode(_read_uvarint(r))
        dst = _zigzag_decode(_read_uvarint(r))
        label_len = _read_uvarint(r)
        label = r.read(label_len).decode('utf-8')
        prop_count = _read_uvarint(r)
        if prop_count > MAX_OBJECT_LEN:
            raise SecurityLimitExceeded(f"Properties too large: {prop_count} > {MAX_OBJECT_LEN}")
        properties = {}
        for _ in range(prop_count):
            key_len = _read_uvarint(r)
            key = r.read(key_len).decode('utf-8')
            properties[key] = _decode_value(r, depth + 1)
        return Edge(src=src, dst=dst, label=label, properties=properties)
    elif tag == TAG_ADJLIST:
        id_width = r.read(1)[0]
        node_count = _read_uvarint(r)
        edge_count = _read_uvarint(r)
        row_offsets = [_read_uvarint(r) for _ in range(node_count + 1)]
        col_bytes_len = edge_count * (4 if id_width == 1 else 8)
        col_indices = r.read(col_bytes_len)
        return AdjList(
            id_width=id_width,
            node_count=node_count,
            edge_count=edge_count,
            row_offsets=row_offsets,
            col_indices=col_indices
        )
    elif tag == TAG_NODE_BATCH:
        count = _read_uvarint(r)
        if count > MAX_ARRAY_LEN:
            raise SecurityLimitExceeded(f"Batch too large: {count} > {MAX_ARRAY_LEN}")
        return [_decode_value(r, depth + 1) for _ in range(count)]
    elif tag == TAG_EDGE_BATCH:
        count = _read_uvarint(r)
        if count > MAX_ARRAY_LEN:
            raise SecurityLimitExceeded(f"Batch too large: {count} > {MAX_ARRAY_LEN}")
        return [_decode_value(r, depth + 1) for _ in range(count)]
    elif tag == TAG_GRAPH_SHARD:
        node_count = _read_uvarint(r)
        if node_count > MAX_ARRAY_LEN:
            raise SecurityLimitExceeded(f"Nodes too large: {node_count} > {MAX_ARRAY_LEN}")
        nodes = [_decode_value(r, depth + 1) for _ in range(node_count)]
        edge_count = _read_uvarint(r)
        if edge_count > MAX_ARRAY_LEN:
            raise SecurityLimitExceeded(f"Edges too large: {edge_count} > {MAX_ARRAY_LEN}")
        edges = [_decode_value(r, depth + 1) for _ in range(edge_count)]
        meta_count = _read_uvarint(r)
        if meta_count > MAX_OBJECT_LEN:
            raise SecurityLimitExceeded(f"Metadata too large: {meta_count} > {MAX_OBJECT_LEN}")
        meta = {}
        for _ in range(meta_count):
            key_len = _read_uvarint(r)
            key = r.read(key_len).decode('utf-8')
            meta[key] = _decode_value(r, depth + 1)
        return GraphShard(nodes=nodes, edges=edges, meta=meta)
    else:
        raise ValueError(f"Invalid tag: 0x{tag:02x}")


# JSON conversion helpers
def encode_json(json_data: bytes) -> bytes:
    """Encode JSON bytes to Gen1 format.

    Args:
        json_data: JSON bytes to encode

    Returns:
        Gen1 encoded bytes
    """
    import json
    value = json.loads(json_data)
    return encode(value)


def decode_json(data: bytes) -> bytes:
    """Decode Gen1 bytes to JSON format.

    Args:
        data: Gen1 encoded bytes

    Returns:
        JSON bytes
    """
    import json
    value = decode(data)
    return json.dumps(value).encode('utf-8')
