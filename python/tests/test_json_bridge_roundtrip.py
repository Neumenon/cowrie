"""JSON bridge round-trip tests for typed values.

Tests that value -> to_json -> from_json -> value produces equal typed values,
using the canonical Go projection schema (_type fields).
"""

import base64
import json
import pytest

from cowrie.gen2 import (
    Value, Type, to_json, to_any, from_json, from_any,
    DType, ImageFormat, AudioEncoding,
    TensorData, TensorRefData, ImageData, AudioData,
    NodeData, EdgeData, NodeBatchData, EdgeBatchData,
    BitmaskData, UnknownExtData,
)


def json_roundtrip(v: Value) -> Value:
    """Serialize v to JSON and deserialize back."""
    return from_json(to_json(v))


class TestTensorRoundtrip:
    def test_tensor_basic(self):
        """tensor: shape key must be 'dims' (Go canonical)."""
        import struct
        data = struct.pack('<4f', 1.0, 2.0, 3.0, 4.0)
        original = Value.tensor(DType.FLOAT32, [2, 2], data)

        # Verify to_any produces canonical Go schema
        d = to_any(original)
        assert d["_type"] == "tensor"
        assert "dims" in d, "must use 'dims' not 'shape'"
        assert d["dims"] == [2, 2]
        assert d["dtype"] == "float32"
        assert isinstance(d["data"], str)  # base64

        # Round-trip
        result = json_roundtrip(original)
        assert result.type == Type.TENSOR
        t = result.data
        assert t.dtype == DType.FLOAT32
        assert t.shape == [2, 2]
        assert t.data == data

    def test_tensor_dtype_strings(self):
        """All dtype strings must match Go's dtypeToString spellings."""
        dtype_cases = [
            (DType.FLOAT32, "float32", 4),
            (DType.FLOAT64, "float64", 8),
            (DType.INT8, "int8", 1),
            (DType.INT16, "int16", 2),
            (DType.INT32, "int32", 4),
            (DType.INT64, "int64", 8),
            (DType.UINT8, "uint8", 1),
            (DType.UINT16, "uint16", 2),
            (DType.UINT32, "uint32", 4),
            (DType.UINT64, "uint64", 8),
        ]
        for dtype, expected_str, elem_size in dtype_cases:
            data = bytes(elem_size)  # single zero element, shape [1]
            v = Value.tensor(dtype, [1], data)
            d = to_any(v)
            assert d["dtype"] == expected_str, f"{dtype.name} -> expected '{expected_str}', got '{d['dtype']}'"
            # Round-trip
            result = json_roundtrip(v)
            assert result.type == Type.TENSOR
            assert result.data.dtype == dtype

    def test_tensor_scalar(self):
        """Rank-0 tensor (empty dims) with one element round-trips."""
        import struct
        data = struct.pack('<f', 3.14)  # rank-0 = single scalar element
        original = Value.tensor(DType.FLOAT32, [], data)
        d = to_any(original)
        assert d["dims"] == []
        result = json_roundtrip(original)
        assert result.type == Type.TENSOR
        assert result.data.shape == []
        assert result.data.data == data


class TestTensorRefRoundtrip:
    def test_tensor_ref_basic(self):
        """tensor_ref: 'store' must be int (not 'store_id')."""
        key = b"\x01\x02\x03\x04"
        original = Value.tensor_ref(3, key)

        d = to_any(original)
        assert d["_type"] == "tensor_ref"
        assert "store" in d, "must use 'store' not 'store_id'"
        assert d["store"] == 3
        assert isinstance(d["key"], str)  # base64

        result = json_roundtrip(original)
        assert result.type == Type.TENSOR_REF
        ref = result.data
        assert ref.store_id == 3
        assert ref.key == key

    def test_tensor_ref_store_zero(self):
        original = Value.tensor_ref(0, b"\xff")
        result = json_roundtrip(original)
        assert result.data.store_id == 0
        assert result.data.key == b"\xff"


class TestImageRoundtrip:
    def test_image_basic(self):
        """image: format/width/height/data fields, format as lowercase string."""
        pixel = b"\xff\x00\x00"  # single red pixel
        original = Value.image(ImageFormat.PNG, 1, 1, pixel)

        d = to_any(original)
        assert d["_type"] == "image"
        assert d["format"] == "png"
        assert d["width"] == 1
        assert d["height"] == 1
        assert isinstance(d["data"], str)

        result = json_roundtrip(original)
        assert result.type == Type.IMAGE
        img = result.data
        assert img.format == ImageFormat.PNG
        assert img.width == 1
        assert img.height == 1
        assert img.data == pixel

    def test_image_formats(self):
        """All image format strings match Go's imageFormatToString."""
        cases = [
            (ImageFormat.JPEG, "jpeg"),
            (ImageFormat.PNG, "png"),
            (ImageFormat.WEBP, "webp"),
            (ImageFormat.AVIF, "avif"),
            (ImageFormat.BMP, "bmp"),
        ]
        for fmt, expected_str in cases:
            v = Value.image(fmt, 1, 1, b"\x00")
            d = to_any(v)
            assert d["format"] == expected_str
            result = json_roundtrip(v)
            assert result.data.format == fmt


class TestAudioRoundtrip:
    def test_audio_basic(self):
        """audio: 'rate' must be used (not 'sample_rate')."""
        data = b"\x00" * 16
        original = Value.audio(AudioEncoding.PCM_INT16, 44100, 2, data)

        d = to_any(original)
        assert d["_type"] == "audio"
        assert "rate" in d, "must use 'rate' not 'sample_rate'"
        assert d["rate"] == 44100
        assert d["channels"] == 2
        assert d["encoding"] == "pcm_int16"
        assert isinstance(d["data"], str)

        result = json_roundtrip(original)
        assert result.type == Type.AUDIO
        aud = result.data
        assert aud.encoding == AudioEncoding.PCM_INT16
        assert aud.sample_rate == 44100
        assert aud.channels == 2
        assert aud.data == data

    def test_audio_encoding_strings(self):
        """All audio encoding strings match Go's audioEncodingToString."""
        cases = [
            (AudioEncoding.PCM_INT16, "pcm_int16"),
            (AudioEncoding.PCM_FLOAT32, "pcm_float32"),
            (AudioEncoding.OPUS, "opus"),
            (AudioEncoding.AAC, "aac"),
        ]
        for enc, expected_str in cases:
            v = Value.audio(enc, 8000, 1, b"\x00\x00")
            d = to_any(v)
            assert d["encoding"] == expected_str
            result = json_roundtrip(v)
            assert result.data.encoding == enc


class TestUnknownExtRoundtrip:
    def test_unknown_ext_basic(self):
        """unknown_ext: ext_type as int, payload as base64."""
        payload = b"\xde\xad\xbe\xef"
        original = Value.unknown_ext(42, payload)

        d = to_any(original)
        assert d["_type"] == "unknown_ext"
        assert d["ext_type"] == 42
        assert isinstance(d["payload"], str)

        result = json_roundtrip(original)
        assert result.type == Type.UNKNOWN_EXT
        ext = result.data
        assert ext.ext_type == 42
        assert ext.payload == payload

    def test_unknown_ext_zero(self):
        original = Value.unknown_ext(0, b"")
        result = json_roundtrip(original)
        assert result.data.ext_type == 0
        assert result.data.payload == b""


class TestNodeRoundtrip:
    def test_node_basic(self):
        """node: id/labels/props fields."""
        original = Value.node("n1", ["Person", "User"], {"age": Value.int64(30)})

        d = to_any(original)
        assert d["_type"] == "node"
        assert d["id"] == "n1"
        assert d["labels"] == ["Person", "User"]
        assert "props" in d

        result = json_roundtrip(original)
        assert result.type == Type.NODE
        node = result.data
        assert node.id == "n1"
        assert node.labels == ["Person", "User"]
        assert "age" in node.props

    def test_node_empty(self):
        """node with empty labels and props."""
        original = Value.node("x", [], {})
        result = json_roundtrip(original)
        assert result.type == Type.NODE
        assert result.data.id == "x"
        assert result.data.labels == []
        assert result.data.props == {}


class TestEdgeRoundtrip:
    def test_edge_basic(self):
        """edge: 'fromId'/'toId' field names (not 'from'/'to')."""
        original = Value.edge("n1", "n2", "KNOWS", {"since": Value.int64(2020)})

        d = to_any(original)
        assert d["_type"] == "edge"
        assert "fromId" in d, "must use 'fromId' not 'from'"
        assert "toId" in d, "must use 'toId' not 'to'"
        assert d["fromId"] == "n1"
        assert d["toId"] == "n2"
        assert d["type"] == "KNOWS"

        result = json_roundtrip(original)
        assert result.type == Type.EDGE
        edge = result.data
        assert edge.from_id == "n1"
        assert edge.to_id == "n2"
        assert edge.edge_type == "KNOWS"

    def test_edge_empty_props(self):
        original = Value.edge("a", "b", "LINK", {})
        result = json_roundtrip(original)
        assert result.data.from_id == "a"
        assert result.data.to_id == "b"
        assert result.data.edge_type == "LINK"


class TestNodeBatchRoundtrip:
    def test_node_batch_basic(self):
        """node_batch: nodes array; each node object must have '_type':'node'."""
        nodes = [
            NodeData(id="n1", labels=["A"], props={}),
            NodeData(id="n2", labels=["B"], props={"x": Value.int64(1)}),
        ]
        original = Value.node_batch(nodes)

        d = to_any(original)
        assert d["_type"] == "node_batch"
        assert isinstance(d["nodes"], list)
        assert len(d["nodes"]) == 2
        # Each node object must have _type field (Go schema)
        for node_obj in d["nodes"]:
            assert node_obj.get("_type") == "node", \
                f"node_batch node items must have '_type':'node', got: {node_obj}"

        result = json_roundtrip(original)
        assert result.type == Type.NODE_BATCH
        batch = result.data
        assert len(batch.nodes) == 2
        assert batch.nodes[0].id == "n1"
        assert batch.nodes[1].id == "n2"

    def test_node_batch_empty(self):
        original = Value.node_batch([])
        result = json_roundtrip(original)
        assert result.type == Type.NODE_BATCH
        assert result.data.nodes == []


class TestEdgeBatchRoundtrip:
    def test_edge_batch_basic(self):
        """edge_batch: edges array; each edge object must have '_type':'edge'."""
        edges = [
            EdgeData(from_id="n1", to_id="n2", edge_type="KNOWS", props={}),
            EdgeData(from_id="n2", to_id="n3", edge_type="LIKES", props={}),
        ]
        original = Value.edge_batch(edges)

        d = to_any(original)
        assert d["_type"] == "edge_batch"
        assert isinstance(d["edges"], list)
        assert len(d["edges"]) == 2
        for edge_obj in d["edges"]:
            assert edge_obj.get("_type") == "edge"
            assert "fromId" in edge_obj
            assert "toId" in edge_obj

        result = json_roundtrip(original)
        assert result.type == Type.EDGE_BATCH
        batch = result.data
        assert len(batch.edges) == 2
        assert batch.edges[0].from_id == "n1"
        assert batch.edges[0].to_id == "n2"

    def test_edge_batch_empty(self):
        original = Value.edge_batch([])
        result = json_roundtrip(original)
        assert result.type == Type.EDGE_BATCH
        assert result.data.edges == []


class TestBitmaskRoundtrip:
    def test_bitmask_basic(self):
        """bitmask: 'bits' must be base64 string (not list of bools)."""
        # [True, False, True, True] = 0b1101 = 0x0D
        original = Value.bitmask_from_bools([True, False, True, True])

        d = to_any(original)
        assert d["_type"] == "bitmask"
        assert d["count"] == 4
        assert isinstance(d["bits"], str), "bits must be a base64 string (not list of bools)"
        # Verify it's valid base64
        bits_decoded = base64.b64decode(d["bits"])
        assert len(bits_decoded) == 1
        assert bits_decoded[0] == 0x0D  # 0b00001101

        result = json_roundtrip(original)
        assert result.type == Type.BITMASK
        bm = result.data
        assert bm.count == 4
        assert bm.to_bools() == [True, False, True, True]

    def test_bitmask_empty(self):
        original = Value.bitmask(0, b"")
        d = to_any(original)
        assert isinstance(d["bits"], str)
        assert d["count"] == 0
        result = json_roundtrip(original)
        assert result.data.count == 0

    def test_bitmask_large(self):
        """16-bit bitmask (2 bytes) round-trips."""
        bools = [i % 3 == 0 for i in range(16)]
        original = Value.bitmask_from_bools(bools)
        result = json_roundtrip(original)
        assert result.data.to_bools() == bools


class TestJsonSchemaFieldNames:
    """Verify exact canonical Go field names are produced by to_any."""

    def test_tensor_has_dims_not_shape(self):
        v = Value.tensor(DType.FLOAT32, [3], bytes(12))
        d = to_any(v)
        assert "dims" in d
        assert "shape" not in d

    def test_tensor_ref_has_store_not_store_id(self):
        v = Value.tensor_ref(0, b"\x00")
        d = to_any(v)
        assert "store" in d
        assert "store_id" not in d

    def test_audio_has_rate_not_sample_rate(self):
        v = Value.audio(AudioEncoding.PCM_INT16, 44100, 1, b"\x00\x00")
        d = to_any(v)
        assert "rate" in d
        assert "sample_rate" not in d

    def test_edge_has_fromId_toId(self):
        v = Value.edge("a", "b", "T", {})
        d = to_any(v)
        assert "fromId" in d
        assert "toId" in d
        assert "from" not in d
        assert "to" not in d

    def test_bitmask_bits_is_string(self):
        v = Value.bitmask(1, b"\x01")
        d = to_any(v)
        assert isinstance(d["bits"], str)
