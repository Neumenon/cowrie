"""Gen2 codec tests."""

import struct
import json
import pytest
from datetime import datetime, timezone
from uuid import UUID

from cowrie.gen2 import (
    encode, decode, Value, Type, to_json, to_any, from_json, from_any,
    NodeData, EdgeData, NodeBatchData, EdgeBatchData, GraphShardData,
    TensorData, DType, ImageData, ImageFormat, AudioData, AudioEncoding,
    TensorRefData, AdjlistData, IDWidth,
    RichTextData, RichTextSpan,
    DeltaData, DeltaOp, DeltaOpCode,
    BitmaskData, UnknownExtData,
    Decimal128, DecodeOptions, UnknownExtBehavior,
    SecurityLimitExceeded,
    encode_framed, decode_framed, Compression,
    dumps, loads,
    encode_with_opts, EncodeOptions,
    schema_fingerprint64, schema_fingerprint32, schema_equals,
    write_master_frame, read_master_frame, MasterWriterOptions,
    is_master_stream, is_cowrie_document,
    encode_uvarint, decode_uvarint, zigzag_encode, zigzag_decode,
    crc32,
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

    def test_fixint_range(self):
        """FIXINT covers 0-127 inline."""
        for val in [0, 1, 42, 127]:
            decoded = decode(encode(Value.int64(val)))
            assert decoded.type == Type.INT64
            assert decoded.data == val

    def test_fixneg_range(self):
        """FIXNEG covers -1 to -16 inline."""
        for val in [-1, -5, -16]:
            decoded = decode(encode(Value.int64(val)))
            assert decoded.type == Type.INT64
            assert decoded.data == val

    def test_large_negative_int(self):
        """Negative int outside FIXNEG range uses full encoding."""
        decoded = decode(encode(Value.int64(-1000)))
        assert decoded.type == Type.INT64
        assert decoded.data == -1000

    def test_large_positive_int(self):
        """Positive int outside FIXINT range uses full encoding."""
        decoded = decode(encode(Value.int64(200)))
        assert decoded.type == Type.INT64
        assert decoded.data == 200

    def test_uint64_max(self):
        """Maximum uint64 value."""
        decoded = decode(encode(Value.uint64(2**64 - 1)))
        assert decoded.type == Type.UINT64
        assert decoded.data == 2**64 - 1

    def test_uint64_range_error(self):
        """UINT64 rejects negative values."""
        with pytest.raises(ValueError):
            Value.uint64(-1)


class TestExtendedTypes:
    """Test extended type encoding/decoding."""

    def test_decimal128_roundtrip(self):
        """Decimal128 should round-trip correctly."""
        # coef = 12345 as 16-byte big-endian two's complement
        coef = (12345).to_bytes(16, 'big', signed=True)
        original = Value.decimal128(2, coef)
        decoded = decode(encode(original))
        assert decoded.type == Type.DECIMAL128
        assert decoded.data.scale == 2
        assert decoded.data.coef == coef

    def test_datetime64_roundtrip(self):
        """Datetime64 should round-trip correctly."""
        nanos = 1700000000_000_000_000  # some timestamp in nanos
        original = Value.datetime64(nanos)
        decoded = decode(encode(original))
        assert decoded.type == Type.DATETIME64
        assert decoded.data == nanos

    def test_datetime_from_python(self):
        """Create datetime from Python datetime object."""
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        original = Value.datetime(dt)
        decoded = decode(encode(original))
        assert decoded.type == Type.DATETIME64
        # Should be close to the expected nanos
        expected_nanos = int(dt.timestamp() * 1_000_000_000)
        assert decoded.data == expected_nanos

    def test_uuid128_roundtrip(self):
        """UUID128 should round-trip correctly."""
        u = UUID("550e8400-e29b-41d4-a716-446655440000")
        original = Value.uuid128(u)
        decoded = decode(encode(original))
        assert decoded.type == Type.UUID128
        assert decoded.data == u

    def test_bigint_roundtrip(self):
        """BigInt should round-trip correctly."""
        # Encode 123456789 as big-endian two's complement
        big_val = (123456789).to_bytes(8, 'big', signed=True)
        original = Value.bigint(big_val)
        decoded = decode(encode(original))
        assert decoded.type == Type.BIGINT
        assert decoded.data == big_val

    def test_bytes_roundtrip(self):
        """Bytes should round-trip correctly."""
        data = b"\x00\x01\x02\xff\xfe"
        original = Value.bytes_(data)
        decoded = decode(encode(original))
        assert decoded.type == Type.BYTES
        assert decoded.data == data


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

    def test_fixarray_inline(self):
        """Arrays of length 0-15 use FIXARRAY inline encoding."""
        for length in [0, 1, 5, 15]:
            items = [Value.int64(i) for i in range(length)]
            decoded = decode(encode(Value.array(items)))
            assert decoded.type == Type.ARRAY
            assert len(decoded.data) == length

    def test_large_array(self):
        """Arrays larger than 15 use full ARRAY tag."""
        items = [Value.int64(i) for i in range(20)]
        decoded = decode(encode(Value.array(items)))
        assert decoded.type == Type.ARRAY
        assert len(decoded.data) == 20

    def test_fixmap_inline(self):
        """Objects with 0-15 fields use FIXMAP inline encoding."""
        members = {f"k{i}": Value.int64(i) for i in range(5)}
        decoded = decode(encode(Value.object(members)))
        assert decoded.type == Type.OBJECT
        assert len(decoded.data) == 5

    def test_large_object(self):
        """Objects with more than 15 fields use full OBJECT tag."""
        members = {f"key_{i}": Value.int64(i) for i in range(20)}
        decoded = decode(encode(Value.object(members)))
        assert decoded.type == Type.OBJECT
        assert len(decoded.data) == 20


class TestTensor:
    """Test tensor encoding/decoding."""

    def test_float32_tensor(self):
        data = struct.pack('<4f', 1.0, 2.0, 3.0, 4.0)
        tensor = Value.tensor(DType.FLOAT32, [2, 2], data)
        decoded = decode(encode(tensor))
        assert decoded.type == Type.TENSOR
        assert decoded.data.dtype == DType.FLOAT32
        assert decoded.data.shape == [2, 2]
        assert decoded.data.data == data

    def test_int64_tensor(self):
        data = struct.pack('<4q', 1, 2, 3, 4)
        tensor = Value.tensor(DType.INT64, [4], data)
        decoded = decode(encode(tensor))
        assert decoded.type == Type.TENSOR
        assert decoded.data.dtype == DType.INT64
        assert decoded.data.shape == [4]

    def test_float64_tensor(self):
        data = struct.pack('<3d', 1.5, 2.5, 3.5)
        tensor = Value.tensor(DType.FLOAT64, [3], data)
        decoded = decode(encode(tensor))
        assert decoded.data.dtype == DType.FLOAT64
        assert decoded.data.shape == [3]

    def test_uint8_tensor(self):
        data = bytes([10, 20, 30, 40, 50, 60])
        tensor = Value.tensor(DType.UINT8, [2, 3], data)
        decoded = decode(encode(tensor))
        assert decoded.data.dtype == DType.UINT8
        assert decoded.data.shape == [2, 3]
        assert decoded.data.data == data

    def test_tensor_data_size_mismatch(self):
        """TensorData rejects data that doesn't match shape."""
        with pytest.raises(ValueError, match="Data size mismatch"):
            TensorData(dtype=DType.FLOAT32, shape=[2, 2], data=b"\x00" * 8)

    def test_tensor_properties(self):
        """TensorData ndim and size properties."""
        data = struct.pack('<6f', *range(6))
        t = TensorData(dtype=DType.FLOAT32, shape=[2, 3], data=data)
        assert t.ndim == 2
        assert t.size == 6


class TestTensorViews:
    """Test TensorData view and copy methods."""

    def test_copy_float32(self):
        data = struct.pack('<3f', 1.0, 2.0, 3.0)
        t = TensorData(dtype=DType.FLOAT32, shape=[3], data=data)
        result = t.copy_float32()
        assert result is not None
        assert len(result) == 3
        assert abs(result[0] - 1.0) < 0.001

    def test_copy_float32_wrong_dtype(self):
        data = struct.pack('<3d', 1.0, 2.0, 3.0)
        t = TensorData(dtype=DType.FLOAT64, shape=[3], data=data)
        assert t.copy_float32() is None

    def test_copy_float64(self):
        data = struct.pack('<2d', 1.5, 2.5)
        t = TensorData(dtype=DType.FLOAT64, shape=[2], data=data)
        result = t.copy_float64()
        assert result is not None
        assert len(result) == 2

    def test_copy_float64_wrong_dtype(self):
        data = struct.pack('<2f', 1.0, 2.0)
        t = TensorData(dtype=DType.FLOAT32, shape=[2], data=data)
        assert t.copy_float64() is None

    def test_copy_int32(self):
        data = struct.pack('<3i', 10, 20, 30)
        t = TensorData(dtype=DType.INT32, shape=[3], data=data)
        result = t.copy_int32()
        assert result == [10, 20, 30]

    def test_copy_int32_wrong_dtype(self):
        data = struct.pack('<3q', 1, 2, 3)
        t = TensorData(dtype=DType.INT64, shape=[3], data=data)
        assert t.copy_int32() is None

    def test_copy_int64(self):
        data = struct.pack('<2q', 100, 200)
        t = TensorData(dtype=DType.INT64, shape=[2], data=data)
        result = t.copy_int64()
        assert result == [100, 200]

    def test_copy_int64_wrong_dtype(self):
        data = struct.pack('<2i', 1, 2)
        t = TensorData(dtype=DType.INT32, shape=[2], data=data)
        assert t.copy_int64() is None

    def test_copy_empty_float32(self):
        t = TensorData(dtype=DType.FLOAT32, shape=[0], data=b"")
        assert t.copy_float32() == []

    def test_copy_empty_float64(self):
        t = TensorData(dtype=DType.FLOAT64, shape=[0], data=b"")
        assert t.copy_float64() == []

    def test_copy_empty_int32(self):
        t = TensorData(dtype=DType.INT32, shape=[0], data=b"")
        assert t.copy_int32() == []

    def test_copy_empty_int64(self):
        t = TensorData(dtype=DType.INT64, shape=[0], data=b"")
        assert t.copy_int64() == []


class TestTensorNumpy:
    """Test TensorData NumPy integration."""

    def test_view_float32(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        data = struct.pack('<4f', 1.0, 2.0, 3.0, 4.0)
        t = TensorData(dtype=DType.FLOAT32, shape=[2, 2], data=data)
        view = t.view_float32()
        assert view is not None
        assert view.shape == (2, 2)
        assert view[0, 0] == pytest.approx(1.0)

    def test_view_float64(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        data = struct.pack('<2d', 1.5, 2.5)
        t = TensorData(dtype=DType.FLOAT64, shape=[2], data=data)
        view = t.view_float64()
        assert view is not None
        assert len(view) == 2

    def test_view_int32(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        data = struct.pack('<3i', 10, 20, 30)
        t = TensorData(dtype=DType.INT32, shape=[3], data=data)
        view = t.view_int32()
        assert view is not None
        assert list(view) == [10, 20, 30]

    def test_view_int64(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        data = struct.pack('<2q', 100, 200)
        t = TensorData(dtype=DType.INT64, shape=[2], data=data)
        view = t.view_int64()
        assert view is not None
        assert list(view) == [100, 200]

    def test_view_uint8(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        data = bytes([1, 2, 3, 4])
        t = TensorData(dtype=DType.UINT8, shape=[4], data=data)
        view = t.view_uint8()
        assert view is not None
        assert list(view) == [1, 2, 3, 4]

    def test_view_wrong_dtype_returns_none(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        data = struct.pack('<2d', 1.0, 2.0)
        t = TensorData(dtype=DType.FLOAT64, shape=[2], data=data)
        assert t.view_float32() is None
        assert t.view_int32() is None
        assert t.view_int64() is None
        assert t.view_uint8() is None

    def test_view_empty_float32(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        t = TensorData(dtype=DType.FLOAT32, shape=[0], data=b"")
        view = t.view_float32()
        assert view is not None
        assert len(view) == 0

    def test_view_empty_float64(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        t = TensorData(dtype=DType.FLOAT64, shape=[0], data=b"")
        view = t.view_float64()
        assert view is not None
        assert len(view) == 0

    def test_view_empty_int32(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        t = TensorData(dtype=DType.INT32, shape=[0], data=b"")
        view = t.view_int32()
        assert view is not None
        assert len(view) == 0

    def test_view_empty_int64(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        t = TensorData(dtype=DType.INT64, shape=[0], data=b"")
        view = t.view_int64()
        assert view is not None
        assert len(view) == 0

    def test_view_empty_uint8(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        t = TensorData(dtype=DType.UINT8, shape=[0], data=b"")
        view = t.view_uint8()
        assert view is not None
        assert len(view) == 0

    def test_float32_array_convenience(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        data = struct.pack('<4f', 1.0, 2.0, 3.0, 4.0)
        t = TensorData(dtype=DType.FLOAT32, shape=[2, 2], data=data)
        arr = t.float32_array()
        assert arr is not None
        assert arr.shape == (2, 2)

    def test_float64_array_convenience(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        data = struct.pack('<2d', 1.5, 2.5)
        t = TensorData(dtype=DType.FLOAT64, shape=[2], data=data)
        arr = t.float64_array()
        assert arr is not None
        assert len(arr) == 2

    def test_from_numpy_float32(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        arr = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
        t = TensorData.from_numpy(arr)
        assert t.dtype == DType.FLOAT32
        assert t.shape == [2, 2]
        # Roundtrip
        v = Value.tensor(t.dtype, t.shape, t.data)
        decoded = decode(encode(v))
        assert decoded.data.shape == [2, 2]

    def test_from_numpy_int64(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        arr = np.array([10, 20, 30], dtype=np.int64)
        t = TensorData.from_numpy(arr)
        assert t.dtype == DType.INT64
        assert t.shape == [3]

    def test_from_numpy_bool(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        arr = np.array([True, False, True], dtype=np.bool_)
        t = TensorData.from_numpy(arr)
        assert t.dtype == DType.BOOL
        assert t.shape == [3]

    def test_from_numpy_unsupported_dtype(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        arr = np.array([1+2j, 3+4j], dtype=np.complex128)
        with pytest.raises(ValueError, match="Unsupported NumPy dtype"):
            TensorData.from_numpy(arr)

    def test_tensor_from_numpy_value(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        arr = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        v = Value.tensor_from_numpy(arr)
        decoded = decode(encode(v))
        assert decoded.type == Type.TENSOR
        assert decoded.data.dtype == DType.FLOAT32


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


class TestImage:
    """Test image encoding/decoding."""

    def test_image_roundtrip(self):
        """Image with format, dimensions, and data should round-trip."""
        img_data = b"\xff\xd8\xff\xe0" + b"\x00" * 100  # fake JPEG header
        img = Value.image(ImageFormat.JPEG, 640, 480, img_data)
        decoded = decode(encode(img))

        assert decoded.type == Type.IMAGE
        assert decoded.data.format == ImageFormat.JPEG
        assert decoded.data.width == 640
        assert decoded.data.height == 480
        assert decoded.data.data == img_data

    def test_image_properties(self):
        """ImageData properties."""
        img = ImageData(format=ImageFormat.PNG, width=100, height=200, data=b"png_data")
        assert img.size == (100, 200)
        assert img.format_name == "png"

    def test_image_validation(self):
        """ImageData rejects invalid dimensions."""
        with pytest.raises(ValueError):
            ImageData(format=ImageFormat.JPEG, width=-1, height=100, data=b"")
        with pytest.raises(ValueError):
            ImageData(format=ImageFormat.JPEG, width=100, height=70000, data=b"")


class TestAudio:
    """Test audio encoding/decoding."""

    def test_audio_roundtrip(self):
        """Audio data should round-trip correctly."""
        audio_data = b"\x00" * 1000
        aud = Value.audio(AudioEncoding.PCM_INT16, 44100, 2, audio_data)
        decoded = decode(encode(aud))

        assert decoded.type == Type.AUDIO
        assert decoded.data.encoding == AudioEncoding.PCM_INT16
        assert decoded.data.sample_rate == 44100
        assert decoded.data.channels == 2
        assert decoded.data.data == audio_data

    def test_audio_properties(self):
        """AudioData properties."""
        aud = AudioData(encoding=AudioEncoding.PCM_INT16, sample_rate=44100, channels=2,
                        data=b"\x00" * 176400)
        assert aud.encoding_name == "pcm_int16"
        duration = aud.duration_seconds
        assert duration is not None
        assert abs(duration - 1.0) < 0.01  # 44100 samples * 2 bytes * 2 channels = 176400

    def test_audio_pcm_float32_duration(self):
        """PCM float32 duration estimation."""
        # 1 second at 48000 Hz, mono, float32 = 48000 * 4 = 192000 bytes
        aud = AudioData(encoding=AudioEncoding.PCM_FLOAT32, sample_rate=48000, channels=1,
                        data=b"\x00" * 192000)
        duration = aud.duration_seconds
        assert duration is not None
        assert abs(duration - 1.0) < 0.01

    def test_audio_compressed_no_duration(self):
        """Compressed audio returns None for duration."""
        aud = AudioData(encoding=AudioEncoding.OPUS, sample_rate=48000, channels=1,
                        data=b"\x00" * 100)
        assert aud.duration_seconds is None

    def test_audio_validation(self):
        """AudioData rejects invalid parameters."""
        with pytest.raises(ValueError):
            AudioData(encoding=AudioEncoding.PCM_INT16, sample_rate=-1, channels=1, data=b"")
        with pytest.raises(ValueError):
            AudioData(encoding=AudioEncoding.PCM_INT16, sample_rate=44100, channels=0, data=b"")


class TestTensorRef:
    """Test tensor reference encoding/decoding."""

    def test_tensor_ref_roundtrip(self):
        """TensorRef should round-trip correctly."""
        key = b"\x01\x02\x03\x04\x05\x06\x07\x08"
        ref = Value.tensor_ref(42, key)
        decoded = decode(encode(ref))

        assert decoded.type == Type.TENSOR_REF
        assert decoded.data.store_id == 42
        assert decoded.data.key == key

    def test_tensor_ref_validation(self):
        """TensorRefData rejects invalid store_id."""
        with pytest.raises(ValueError):
            TensorRefData(store_id=256, key=b"test")
        with pytest.raises(ValueError):
            TensorRefData(store_id=-1, key=b"test")


class TestAdjlist:
    """Test adjacency list encoding/decoding."""

    def test_adjlist_int32_roundtrip(self):
        """Adjlist with int32 IDs should round-trip."""
        col_indices = struct.pack('<3I', 1, 2, 0)  # 3 edges
        adj = Value.adjlist(IDWidth.INT32, 3, 3, [0, 1, 2, 3], col_indices)
        decoded = decode(encode(adj))

        assert decoded.type == Type.ADJLIST
        assert decoded.data.id_width == IDWidth.INT32
        assert decoded.data.node_count == 3
        assert decoded.data.edge_count == 3
        assert decoded.data.row_offsets == [0, 1, 2, 3]
        assert decoded.data.col_indices == col_indices

    def test_adjlist_int64_roundtrip(self):
        """Adjlist with int64 IDs should round-trip."""
        col_indices = struct.pack('<2Q', 100, 200)
        adj = Value.adjlist(IDWidth.INT64, 2, 2, [0, 1, 2], col_indices)
        decoded = decode(encode(adj))

        assert decoded.type == Type.ADJLIST
        assert decoded.data.id_width == IDWidth.INT64
        assert decoded.data.id_size == 8

    def test_adjlist_id_size(self):
        """AdjlistData id_size property."""
        adj32 = AdjlistData(id_width=IDWidth.INT32, node_count=0, edge_count=0,
                            row_offsets=[0], col_indices=b"")
        assert adj32.id_size == 4
        adj64 = AdjlistData(id_width=IDWidth.INT64, node_count=0, edge_count=0,
                            row_offsets=[0], col_indices=b"")
        assert adj64.id_size == 8


class TestRichText:
    """Test rich text encoding/decoding."""

    def test_richtext_plain(self):
        """Plain richtext (no tokens or spans) should round-trip."""
        rt = Value.richtext("Hello, world!")
        decoded = decode(encode(rt))

        assert decoded.type == Type.RICHTEXT
        assert decoded.data.text == "Hello, world!"
        assert decoded.data.tokens is None
        assert decoded.data.spans is None

    def test_richtext_with_tokens(self):
        """Richtext with tokens should round-trip."""
        rt = Value.richtext("Hello world", tokens=[101, 7592, 2088])
        decoded = decode(encode(rt))

        assert decoded.type == Type.RICHTEXT
        assert decoded.data.text == "Hello world"
        assert decoded.data.tokens == [101, 7592, 2088]
        assert decoded.data.spans is None

    def test_richtext_with_spans(self):
        """Richtext with spans should round-trip."""
        spans = [RichTextSpan(start=0, end=5, kind_id=1),
                 RichTextSpan(start=6, end=11, kind_id=2)]
        rt = Value.richtext("Hello world", spans=spans)
        decoded = decode(encode(rt))

        assert decoded.type == Type.RICHTEXT
        assert decoded.data.text == "Hello world"
        assert decoded.data.tokens is None
        assert len(decoded.data.spans) == 2
        assert decoded.data.spans[0].start == 0
        assert decoded.data.spans[0].end == 5
        assert decoded.data.spans[0].kind_id == 1

    def test_richtext_with_tokens_and_spans(self):
        """Richtext with both tokens and spans should round-trip."""
        spans = [RichTextSpan(start=0, end=5, kind_id=1)]
        rt = Value.richtext("Hello world", tokens=[101, 7592], spans=spans)
        decoded = decode(encode(rt))

        assert decoded.type == Type.RICHTEXT
        assert decoded.data.tokens == [101, 7592]
        assert len(decoded.data.spans) == 1


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

    def test_delta_append_array(self):
        """Delta with APPEND_ARRAY operation should round-trip."""
        ops = [
            DeltaOp(op_code=DeltaOpCode.APPEND_ARRAY, field_id=0, value=Value.int64(42)),
        ]
        delta = Value.delta(456, ops)
        decoded = decode(encode(delta))

        assert decoded.type == Type.DELTA
        assert decoded.data.ops[0].op_code == DeltaOpCode.APPEND_ARRAY
        assert decoded.data.ops[0].value.data == 42


class TestBitmask:
    """Test bitmask encoding/decoding."""

    def test_bitmask_roundtrip(self):
        """Bitmask should round-trip correctly."""
        bm = Value.bitmask(8, bytes([0b10110101]))
        decoded = decode(encode(bm))

        assert decoded.type == Type.BITMASK
        assert decoded.data.count == 8
        assert decoded.data.bits == bytes([0b10110101])

    def test_bitmask_from_bools(self):
        """Bitmask from list of bools should round-trip."""
        bools = [True, False, True, True, False, False, True, False, True]
        bm = Value.bitmask_from_bools(bools)
        decoded = decode(encode(bm))

        assert decoded.type == Type.BITMASK
        assert decoded.data.count == 9
        assert decoded.data.to_bools() == bools

    def test_bitmask_get(self):
        """BitmaskData.get should return correct values."""
        bm = BitmaskData(count=4, bits=bytes([0b1010]))
        assert bm.get(0) is False
        assert bm.get(1) is True
        assert bm.get(2) is False
        assert bm.get(3) is True

    def test_bitmask_get_out_of_bounds(self):
        """BitmaskData.get should raise IndexError for out-of-bounds."""
        bm = BitmaskData(count=4, bits=bytes([0b1010]))
        with pytest.raises(IndexError):
            bm.get(4)
        with pytest.raises(IndexError):
            bm.get(-1)


class TestUnknownExt:
    """Test unknown extension handling."""

    def test_unknown_ext_roundtrip(self):
        """Unknown extension should round-trip with KEEP behavior."""
        ext = Value.unknown_ext(42, b"payload_data")
        decoded = decode(encode(ext))

        assert decoded.type == Type.UNKNOWN_EXT
        assert decoded.data.ext_type == 42
        assert decoded.data.payload == b"payload_data"

    def test_unknown_ext_skip_as_null(self):
        """Unknown extension with SKIP_AS_NULL returns null."""
        ext = Value.unknown_ext(42, b"payload_data")
        encoded = encode(ext)
        decoded = decode(encoded, on_unknown_ext=UnknownExtBehavior.SKIP_AS_NULL)
        assert decoded.type == Type.NULL

    def test_unknown_ext_error(self):
        """Unknown extension with ERROR raises."""
        ext = Value.unknown_ext(42, b"payload_data")
        encoded = encode(ext)
        with pytest.raises(ValueError, match="Unknown extension type"):
            decode(encoded, on_unknown_ext=UnknownExtBehavior.ERROR)


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

    def test_to_any_edge(self):
        edge = Value.edge("a", "b", "REL", {"w": Value.float64(0.5)})
        result = to_any(edge)
        assert result["_type"] == "edge"
        assert result["from"] == "a"
        assert result["to"] == "b"
        assert result["type"] == "REL"

    def test_to_any_node_batch(self):
        batch = Value.node_batch([NodeData(id="n1", labels=["L"], props={})])
        result = to_any(batch)
        assert result["_type"] == "node_batch"
        assert len(result["nodes"]) == 1

    def test_to_any_edge_batch(self):
        batch = Value.edge_batch([EdgeData(from_id="a", to_id="b", edge_type="E", props={})])
        result = to_any(batch)
        assert result["_type"] == "edge_batch"
        assert len(result["edges"]) == 1

    def test_to_any_tensor(self):
        data = struct.pack('<2f', 1.0, 2.0)
        v = Value.tensor(DType.FLOAT32, [2], data)
        result = to_any(v)
        assert result["_type"] == "tensor"
        assert result["dtype"] == "float32"
        assert result["shape"] == [2]

    def test_to_any_image(self):
        v = Value.image(ImageFormat.PNG, 100, 200, b"png_data")
        result = to_any(v)
        assert result["_type"] == "image"
        assert result["format"] == "png"
        assert result["width"] == 100

    def test_to_any_audio(self):
        v = Value.audio(AudioEncoding.OPUS, 48000, 1, b"opus_data")
        result = to_any(v)
        assert result["_type"] == "audio"
        assert result["encoding"] == "opus"
        assert result["sample_rate"] == 48000

    def test_to_any_tensor_ref(self):
        v = Value.tensor_ref(1, b"key123")
        result = to_any(v)
        assert result["_type"] == "tensor_ref"
        assert result["store_id"] == 1

    def test_to_any_adjlist(self):
        col_indices = struct.pack('<2I', 1, 0)
        v = Value.adjlist(IDWidth.INT32, 2, 2, [0, 1, 2], col_indices)
        result = to_any(v)
        assert result["_type"] == "adjlist"
        assert result["id_width"] == "int32"
        assert result["node_count"] == 2

    def test_to_any_richtext(self):
        spans = [RichTextSpan(start=0, end=5, kind_id=1)]
        v = Value.richtext("hello world", tokens=[1, 2, 3], spans=spans)
        result = to_any(v)
        assert result["_type"] == "richtext"
        assert result["text"] == "hello world"
        assert result["tokens"] == [1, 2, 3]
        assert len(result["spans"]) == 1

    def test_to_any_delta(self):
        ops = [DeltaOp(op_code=DeltaOpCode.SET_FIELD, field_id=0, value=Value.int64(99))]
        v = Value.delta(100, ops)
        result = to_any(v)
        assert result["_type"] == "delta"
        assert result["base_id"] == 100

    def test_to_any_bitmask(self):
        v = Value.bitmask_from_bools([True, False, True])
        result = to_any(v)
        assert result["_type"] == "bitmask"
        assert result["bits"] == [True, False, True]

    def test_to_any_unknown_ext(self):
        v = Value.unknown_ext(42, b"data")
        result = to_any(v)
        assert result["_type"] == "unknown_ext"
        assert result["ext_type"] == 42

    def test_to_any_decimal128(self):
        coef = (12345).to_bytes(16, 'big', signed=True)
        v = Value.decimal128(2, coef)
        result = to_any(v)
        assert result == "123.45"

    def test_to_any_datetime64(self):
        # 2024-01-01T00:00:00Z in nanos
        nanos = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000_000)
        v = Value.datetime64(nanos)
        result = to_any(v)
        assert "2024-01-01" in result

    def test_to_any_uuid128(self):
        u = UUID("550e8400-e29b-41d4-a716-446655440000")
        v = Value.uuid128(u)
        result = to_any(v)
        assert result == "550e8400-e29b-41d4-a716-446655440000"

    def test_to_any_bigint(self):
        big_val = (123456789).to_bytes(8, 'big', signed=True)
        v = Value.bigint(big_val)
        result = to_any(v)
        assert result == "123456789"

    def test_to_any_bytes(self):
        v = Value.bytes_(b"\x01\x02\x03")
        result = to_any(v)
        import base64
        assert base64.b64decode(result) == b"\x01\x02\x03"

    def test_to_any_large_int(self):
        """Int64 outside JS safe range serializes as string."""
        v = Value.int64(9007199254740992)  # MAX_SAFE_INT + 1
        result = to_any(v)
        assert isinstance(result, str)

    def test_to_any_large_uint64(self):
        """Uint64 outside JS safe range serializes as string."""
        v = Value.uint64(9007199254740992)
        result = to_any(v)
        assert isinstance(result, str)

    def test_to_any_adjlist_int64(self):
        col_indices = struct.pack('<2Q', 100, 200)
        v = Value.adjlist(IDWidth.INT64, 2, 2, [0, 1, 2], col_indices)
        result = to_any(v)
        assert result["id_width"] == "int64"


class TestFromJson:
    """Test JSON to Value conversion."""

    def test_from_json_basic(self):
        v = from_json('{"name": "Alice", "age": 30}')
        assert v.type == Type.OBJECT
        assert v.data["name"].data == "Alice"
        assert v.data["age"].data == 30

    def test_from_json_bytes(self):
        v = from_json(b'{"x": 1}')
        assert v.type == Type.OBJECT
        assert v.data["x"].data == 1

    def test_from_any_none(self):
        v = from_any(None)
        assert v.type == Type.NULL

    def test_from_any_bool(self):
        assert from_any(True).type == Type.BOOL
        assert from_any(False).type == Type.BOOL

    def test_from_any_int(self):
        v = from_any(42)
        assert v.type == Type.INT64
        assert v.data == 42

    def test_from_any_large_int(self):
        """Ints above MAX_SAFE_INT become UINT64."""
        v = from_any(9007199254740992)
        assert v.type == Type.UINT64

    def test_from_any_negative_int(self):
        v = from_any(-42)
        assert v.type == Type.INT64

    def test_from_any_float(self):
        v = from_any(3.14)
        assert v.type == Type.FLOAT64

    def test_from_any_string(self):
        v = from_any("hello")
        assert v.type == Type.STRING

    def test_from_any_bytes(self):
        v = from_any(b"data")
        assert v.type == Type.BYTES

    def test_from_any_list(self):
        v = from_any([1, 2, 3])
        assert v.type == Type.ARRAY
        assert len(v.data) == 3

    def test_from_any_dict(self):
        v = from_any({"key": "value"})
        assert v.type == Type.OBJECT

    def test_from_any_datetime(self):
        dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        v = from_any(dt)
        assert v.type == Type.DATETIME64

    def test_from_any_uuid(self):
        u = UUID("550e8400-e29b-41d4-a716-446655440000")
        v = from_any(u)
        assert v.type == Type.UUID128

    def test_from_any_iso_string_datetime_field(self):
        """ISO8601 string with time-related field name infers datetime."""
        v = from_any("2024-01-15T12:00:00Z", field_name="created_at")
        assert v.type == Type.DATETIME64

    def test_from_any_iso_string_pattern(self):
        """ISO8601 string inferred as datetime even without field hint."""
        v = from_any("2024-01-15T12:00:00Z", field_name="")
        assert v.type == Type.DATETIME64

    def test_from_any_uuid_string(self):
        """UUID string is inferred as UUID128."""
        v = from_any("550e8400-e29b-41d4-a716-446655440000")
        assert v.type == Type.UUID128

    def test_from_any_ext_dict(self):
        """Dict with _type=ext round-trips as unknown_ext."""
        import base64
        v = from_any({"_type": "ext", "ext_type": 42, "payload": base64.b64encode(b"data").decode()})
        assert v.type == Type.UNKNOWN_EXT
        assert v.data.ext_type == 42

    def test_from_any_unknown_ext_dict(self):
        """Dict with _type=unknown_ext round-trips."""
        import base64
        v = from_any({"_type": "unknown_ext", "ext_type": 7, "payload": base64.b64encode(b"xyz").decode()})
        assert v.type == Type.UNKNOWN_EXT

    def test_from_any_fallback_to_string(self):
        """Unsupported types fall back to string."""
        v = from_any(object())
        assert v.type == Type.STRING

    def test_from_any_numpy_array(self):
        try:
            import numpy as np
        except ImportError:
            pytest.skip("numpy not available")
        arr = np.array([1.0, 2.0], dtype=np.float32)
        v = from_any(arr)
        assert v.type == Type.TENSOR


class TestDictionaryCoding:
    """Test dictionary coding efficiency."""

    def test_repeated_keys_encoded_once(self):
        """Objects with repeated keys should benefit from dictionary coding."""
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

        decoded = decode(encoded)
        assert len(decoded.data) == 100
        assert decoded.data[50].data["name"].data == "item_50"


class TestValueMethods:
    """Test Value convenience methods."""

    def test_get_object(self):
        obj = Value.object({"key": Value.int64(42)})
        assert obj.get("key").data == 42
        assert obj.get("missing") is None

    def test_get_non_object_raises(self):
        v = Value.int64(1)
        with pytest.raises(TypeError, match="Not an object"):
            v.get("key")

    def test_getitem_object(self):
        obj = Value.object({"key": Value.string("val")})
        assert obj["key"].data == "val"

    def test_getitem_array(self):
        arr = Value.array([Value.int64(10), Value.int64(20)])
        assert arr[0].data == 10
        assert arr[1].data == 20

    def test_getitem_invalid_type(self):
        v = Value.int64(1)
        with pytest.raises(TypeError):
            v[0]

    def test_len_array(self):
        arr = Value.array([Value.int64(1), Value.int64(2), Value.int64(3)])
        assert len(arr) == 3

    def test_len_object(self):
        obj = Value.object({"a": Value.int64(1), "b": Value.int64(2)})
        assert len(obj) == 2

    def test_len_invalid_type(self):
        v = Value.int64(1)
        with pytest.raises(TypeError):
            len(v)


class TestFramedCompression:
    """Test framed compression (gzip/zstd)."""

    def _make_large_value(self):
        """Create a value large enough to trigger compression."""
        members = {f"field_{i}": Value.string("This is test data " * 5) for i in range(50)}
        return Value.object(members)

    def test_gzip_roundtrip(self):
        """Gzip-compressed data should decode correctly."""
        v = self._make_large_value()
        framed = encode_framed(v, Compression.GZIP)
        decoded = decode_framed(framed)
        # Verify some data made it through
        assert decoded.type == Type.OBJECT
        assert len(decoded.data) == 50

    def test_zstd_roundtrip(self):
        """Zstd-compressed data should decode correctly (if available)."""
        try:
            import zstandard
        except ImportError:
            pytest.skip("zstandard not available")
        v = self._make_large_value()
        framed = encode_framed(v, Compression.ZSTD)
        decoded = decode_framed(framed)
        assert decoded.type == Type.OBJECT
        assert len(decoded.data) == 50

    def test_no_compression_passthrough(self):
        """Small data shouldn't be compressed."""
        v = Value.int64(42)
        framed = encode_framed(v, Compression.GZIP)
        # Should be same as raw because under threshold
        assert framed == encode(v)

    def test_compression_none(self):
        """Compression.NONE returns raw."""
        v = self._make_large_value()
        framed = encode_framed(v, Compression.NONE)
        assert framed == encode(v)

    def test_decode_framed_uncompressed(self):
        """decode_framed handles uncompressed data."""
        v = Value.int64(42)
        raw = encode(v)
        decoded = decode_framed(raw)
        assert decoded.data == 42

    def test_decode_framed_too_short(self):
        with pytest.raises(ValueError, match="Data too short"):
            decode_framed(b"\x00")

    def test_decode_framed_bad_magic(self):
        with pytest.raises(ValueError, match="Invalid magic"):
            decode_framed(b"XX\x02\x00")

    def test_decode_framed_bad_version(self):
        with pytest.raises(ValueError, match="Unsupported version"):
            decode_framed(b"SJ\x99\x00")

    def test_decode_framed_decompression_bomb_protection(self):
        """decode_framed should reject oversized decompressed data."""
        v = self._make_large_value()
        framed = encode_framed(v, Compression.GZIP)
        # Only test if it was actually compressed
        if framed[3] & 0x01:
            opts = DecodeOptions(max_decompressed_size=10)
            with pytest.raises(SecurityLimitExceeded):
                decode_framed(framed, opts=opts)


class TestDumpsLoads:
    """Test convenience dumps/loads functions."""

    def test_dumps_loads_basic(self):
        obj = {"name": "Alice", "age": 30}
        data = dumps(obj)
        result = loads(data)
        assert result["name"] == "Alice"
        assert result["age"] == 30

    def test_dumps_with_gzip(self):
        obj = {f"field_{i}": "data " * 20 for i in range(50)}
        data = dumps(obj, Compression.GZIP)
        result = loads(data)
        assert len(result) == 50

    def test_dumps_no_compression(self):
        obj = {"x": 1}
        data = dumps(obj, Compression.NONE)
        result = loads(data)
        assert result["x"] == 1


class TestDeterministicEncoder:
    """Test deterministic encoding."""

    def test_deterministic_sorted_keys(self):
        """Deterministic encoding should produce sorted keys."""
        obj = Value.object({"z": Value.int64(1), "a": Value.int64(2), "m": Value.int64(3)})
        opts = EncodeOptions(deterministic=True)
        data1 = encode_with_opts(obj, opts)
        data2 = encode_with_opts(obj, opts)
        assert data1 == data2

    def test_deterministic_false_uses_regular(self):
        """Non-deterministic falls back to regular encoding."""
        obj = Value.object({"a": Value.int64(1)})
        opts = EncodeOptions(deterministic=False)
        data = encode_with_opts(obj, opts)
        decoded = decode(data)
        assert decoded.data["a"].data == 1

    def test_omit_null(self):
        """omit_null should skip null values."""
        obj = Value.object({"a": Value.int64(1), "b": Value.null(), "c": Value.string("x")})
        opts = EncodeOptions(deterministic=True, omit_null=True)
        data = encode_with_opts(obj, opts)
        decoded = decode(data)
        assert "b" not in decoded.data
        assert decoded.data["a"].data == 1
        assert decoded.data["c"].data == "x"

    def test_encode_with_opts_defaults(self):
        """encode_with_opts with no opts uses defaults."""
        obj = Value.object({"key": Value.int64(42)})
        data = encode_with_opts(obj)
        decoded = decode(data)
        assert decoded.data["key"].data == 42

    def test_deterministic_nested(self):
        """Deterministic encoding handles nested structures."""
        obj = Value.object({
            "arr": Value.array([Value.object({"z": Value.int64(1), "a": Value.int64(2)})]),
            "nested": Value.object({"b": Value.int64(3), "a": Value.int64(4)}),
        })
        opts = EncodeOptions(deterministic=True)
        data = encode_with_opts(obj, opts)
        decoded = decode(data)
        assert decoded.data["nested"].data["a"].data == 4

    def test_deterministic_bitmask(self):
        """Deterministic encoder handles bitmask type."""
        bm = Value.bitmask_from_bools([True, False, True])
        opts = EncodeOptions(deterministic=True)
        data = encode_with_opts(bm, opts)
        decoded = decode(data)
        assert decoded.type == Type.BITMASK

    def test_deterministic_tensor(self):
        """Deterministic encoder handles tensor type."""
        tensor_data = struct.pack('<2f', 1.0, 2.0)
        v = Value.tensor(DType.FLOAT32, [2], tensor_data)
        opts = EncodeOptions(deterministic=True)
        data = encode_with_opts(v, opts)
        decoded = decode(data)
        assert decoded.type == Type.TENSOR

    def test_deterministic_image(self):
        """Deterministic encoder handles image type."""
        v = Value.image(ImageFormat.JPEG, 10, 10, b"jpeg_data")
        opts = EncodeOptions(deterministic=True)
        data = encode_with_opts(v, opts)
        decoded = decode(data)
        assert decoded.type == Type.IMAGE

    def test_deterministic_audio(self):
        """Deterministic encoder handles audio type."""
        v = Value.audio(AudioEncoding.PCM_INT16, 44100, 1, b"\x00" * 10)
        opts = EncodeOptions(deterministic=True)
        data = encode_with_opts(v, opts)
        decoded = decode(data)
        assert decoded.type == Type.AUDIO

    def test_deterministic_tensor_ref(self):
        """Deterministic encoder handles tensor ref type."""
        v = Value.tensor_ref(1, b"key")
        opts = EncodeOptions(deterministic=True)
        data = encode_with_opts(v, opts)
        decoded = decode(data)
        assert decoded.type == Type.TENSOR_REF

    def test_deterministic_adjlist(self):
        """Deterministic encoder handles adjlist type."""
        col = struct.pack('<2I', 1, 0)
        v = Value.adjlist(IDWidth.INT32, 2, 2, [0, 1, 2], col)
        opts = EncodeOptions(deterministic=True)
        data = encode_with_opts(v, opts)
        decoded = decode(data)
        assert decoded.type == Type.ADJLIST

    def test_deterministic_richtext(self):
        """Deterministic encoder handles richtext type."""
        spans = [RichTextSpan(start=0, end=3, kind_id=1)]
        v = Value.richtext("abc", tokens=[1, 2], spans=spans)
        opts = EncodeOptions(deterministic=True)
        data = encode_with_opts(v, opts)
        decoded = decode(data)
        assert decoded.type == Type.RICHTEXT

    def test_deterministic_delta(self):
        """Deterministic encoder handles delta type."""
        ops = [
            DeltaOp(op_code=DeltaOpCode.SET_FIELD, field_id=0, value=Value.int64(1)),
            DeltaOp(op_code=DeltaOpCode.APPEND_ARRAY, field_id=1, value=Value.string("x")),
        ]
        v = Value.delta(10, ops)
        opts = EncodeOptions(deterministic=True)
        data = encode_with_opts(v, opts)
        decoded = decode(data)
        assert decoded.type == Type.DELTA

    def test_deterministic_all_primitives(self):
        """Deterministic encoder handles all primitive types."""
        u = UUID("550e8400-e29b-41d4-a716-446655440000")
        coef = (100).to_bytes(16, 'big', signed=True)
        obj = Value.object({
            "null": Value.null(),
            "bool": Value.bool_(True),
            "int": Value.int64(42),
            "uint": Value.uint64(999),
            "float": Value.float64(3.14),
            "decimal": Value.decimal128(2, coef),
            "string": Value.string("hello"),
            "bytes": Value.bytes_(b"data"),
            "datetime": Value.datetime64(1000000000),
            "uuid": Value.uuid128(u),
            "bigint": Value.bigint(b"\x01\x00"),
        })
        opts = EncodeOptions(deterministic=True)
        data = encode_with_opts(obj, opts)
        decoded = decode(data)
        assert decoded.type == Type.OBJECT
        assert len(decoded.data) == 11


class TestSchemaFingerprint:
    """Test schema fingerprinting."""

    def test_same_schema_same_fingerprint(self):
        """Two values with same structure should have same fingerprint."""
        v1 = Value.object({"name": Value.string("Alice"), "age": Value.int64(30)})
        v2 = Value.object({"name": Value.string("Bob"), "age": Value.int64(25)})
        assert schema_fingerprint64(v1) == schema_fingerprint64(v2)

    def test_different_schema_different_fingerprint(self):
        """Values with different structure should have different fingerprints."""
        v1 = Value.object({"name": Value.string("Alice")})
        v2 = Value.object({"id": Value.int64(1)})
        assert schema_fingerprint64(v1) != schema_fingerprint64(v2)

    def test_schema_fingerprint32(self):
        """32-bit fingerprint is lower 32 bits of 64-bit."""
        v = Value.object({"x": Value.int64(1)})
        fp64 = schema_fingerprint64(v)
        fp32 = schema_fingerprint32(v)
        assert fp32 == fp64 & 0xFFFFFFFF

    def test_schema_equals(self):
        v1 = Value.object({"x": Value.int64(1)})
        v2 = Value.object({"x": Value.int64(999)})
        assert schema_equals(v1, v2)

    def test_schema_fingerprint_scalar_types(self):
        """Different scalar types have different fingerprints."""
        fp_int = schema_fingerprint64(Value.int64(1))
        fp_str = schema_fingerprint64(Value.string("x"))
        fp_null = schema_fingerprint64(Value.null())
        fp_bool = schema_fingerprint64(Value.bool_(True))
        fp_float = schema_fingerprint64(Value.float64(1.0))
        assert len({fp_int, fp_str, fp_null, fp_bool, fp_float}) == 5

    def test_schema_fingerprint_tensor(self):
        """Tensor schema includes dtype and rank."""
        data2 = struct.pack('<2f', 1.0, 2.0)
        data4 = struct.pack('<4f', 1.0, 2.0, 3.0, 4.0)
        t1 = Value.tensor(DType.FLOAT32, [2], data2)
        t2 = Value.tensor(DType.FLOAT32, [4], data4)
        t3 = Value.tensor(DType.INT64, [1], struct.pack('<q', 1))
        # Same dtype and rank -> same fingerprint
        assert schema_fingerprint64(t1) == schema_fingerprint64(t2)
        # Different dtype -> different fingerprint
        assert schema_fingerprint64(t1) != schema_fingerprint64(t3)

    def test_schema_fingerprint_image(self):
        """Image schema includes format."""
        i1 = Value.image(ImageFormat.JPEG, 100, 100, b"a")
        i2 = Value.image(ImageFormat.PNG, 100, 100, b"a")
        assert schema_fingerprint64(i1) != schema_fingerprint64(i2)

    def test_schema_fingerprint_audio(self):
        """Audio schema includes encoding."""
        a1 = Value.audio(AudioEncoding.PCM_INT16, 44100, 1, b"x")
        a2 = Value.audio(AudioEncoding.OPUS, 44100, 1, b"x")
        assert schema_fingerprint64(a1) != schema_fingerprint64(a2)

    def test_schema_fingerprint_tensor_ref(self):
        """TensorRef schema includes store_id."""
        r1 = Value.tensor_ref(1, b"key")
        r2 = Value.tensor_ref(2, b"key")
        assert schema_fingerprint64(r1) != schema_fingerprint64(r2)

    def test_schema_fingerprint_adjlist(self):
        """Adjlist schema includes id_width."""
        a1 = Value.adjlist(IDWidth.INT32, 0, 0, [0], b"")
        a2 = Value.adjlist(IDWidth.INT64, 0, 0, [0], b"")
        assert schema_fingerprint64(a1) != schema_fingerprint64(a2)

    def test_schema_fingerprint_richtext(self):
        """RichText schema is consistent."""
        r1 = Value.richtext("hello")
        r2 = Value.richtext("world")
        assert schema_fingerprint64(r1) == schema_fingerprint64(r2)

    def test_schema_fingerprint_delta(self):
        """Delta schema includes op codes."""
        ops1 = [DeltaOp(op_code=DeltaOpCode.SET_FIELD, field_id=0, value=Value.int64(1))]
        ops2 = [DeltaOp(op_code=DeltaOpCode.DELETE_FIELD, field_id=0, value=None)]
        d1 = Value.delta(0, ops1)
        d2 = Value.delta(0, ops2)
        assert schema_fingerprint64(d1) != schema_fingerprint64(d2)


class TestMasterStream:
    """Test master stream frame write/read."""

    def test_write_read_roundtrip(self):
        """Master stream frame should round-trip."""
        v = Value.object({"key": Value.string("value")})
        frame_bytes = write_master_frame(v)
        frame, consumed = read_master_frame(frame_bytes)

        assert frame.payload.type == Type.OBJECT
        assert frame.payload.data["key"].data == "value"
        assert consumed == len(frame_bytes)

    def test_write_read_with_metadata(self):
        """Master frame with metadata should round-trip."""
        v = Value.int64(42)
        meta = Value.object({"source": Value.string("test")})
        frame_bytes = write_master_frame(v, meta=meta)
        frame, consumed = read_master_frame(frame_bytes)

        assert frame.payload.data == 42
        assert frame.meta is not None
        assert frame.meta.data["source"].data == "test"

    def test_master_frame_crc(self):
        """Master frame with CRC should verify on read."""
        v = Value.string("test")
        opts = MasterWriterOptions(enable_crc=True)
        frame_bytes = write_master_frame(v, opts=opts)
        frame, _ = read_master_frame(frame_bytes)
        assert frame.payload.data == "test"

    def test_master_frame_no_crc(self):
        """Master frame without CRC."""
        v = Value.string("test")
        opts = MasterWriterOptions(enable_crc=False)
        frame_bytes = write_master_frame(v, opts=opts)
        frame, _ = read_master_frame(frame_bytes)
        assert frame.payload.data == "test"

    def test_is_master_stream(self):
        assert is_master_stream(b"SJST\x00\x00")
        assert not is_master_stream(b"SJ\x02\x00")
        assert not is_master_stream(b"XX")

    def test_is_cowrie_document(self):
        assert is_cowrie_document(b"SJ\x02\x00")
        assert not is_cowrie_document(b"SJST")
        assert not is_cowrie_document(b"XX\x00\x00")
        assert not is_cowrie_document(b"SJ")  # too short

    def test_master_frame_type_id(self):
        """Master frame type_id matches schema fingerprint."""
        v = Value.object({"x": Value.int64(1)})
        expected_type_id = schema_fingerprint32(v)
        frame_bytes = write_master_frame(v)
        frame, _ = read_master_frame(frame_bytes)
        assert frame.type_id == expected_type_id

    def test_master_frame_truncated(self):
        with pytest.raises(ValueError, match="Truncated"):
            read_master_frame(b"SJST" + b"\x00" * 10)

    def test_master_frame_bad_magic(self):
        with pytest.raises(ValueError, match="Invalid master stream magic"):
            read_master_frame(b"XXXX" + b"\x00" * 20)

    def test_master_frame_bad_version(self):
        data = bytearray(b"SJST" + b"\x00" * 20)
        data[4] = 0xFF  # bad version
        with pytest.raises(ValueError, match="Invalid master stream version"):
            read_master_frame(bytes(data))


class TestVarintHelpers:
    """Test varint encoding/decoding helpers."""

    def test_encode_decode_uvarint(self):
        for val in [0, 1, 127, 128, 255, 16384, 2**32, 2**63]:
            encoded = encode_uvarint(val)
            decoded, pos = decode_uvarint(encoded, 0)
            assert decoded == val
            assert pos == len(encoded)

    def test_uvarint_out_of_range(self):
        with pytest.raises(ValueError):
            encode_uvarint(-1)
        with pytest.raises(ValueError):
            encode_uvarint(2**64 + 1)

    def test_zigzag(self):
        for val in [0, 1, -1, 42, -42, 127, -128, 2**31, -2**31]:
            assert zigzag_decode(zigzag_encode(val)) == val

    def test_decode_uvarint_incomplete(self):
        with pytest.raises(ValueError, match="Incomplete varint"):
            decode_uvarint(b"", 0)

    def test_decode_uvarint_multi_byte_incomplete(self):
        with pytest.raises(ValueError, match="Incomplete varint"):
            decode_uvarint(bytes([0x80]), 0)  # continuation bit set but no more bytes


class TestCRC32:
    """Test CRC32 implementation."""

    def test_crc32_known_value(self):
        """CRC32 of empty data."""
        assert crc32(b"") == 0x00000000

    def test_crc32_hello(self):
        """CRC32 should produce consistent results."""
        import zlib
        data = b"hello world"
        assert crc32(data) == zlib.crc32(data) & 0xFFFFFFFF


class TestSecurityLimits:
    """Test security limit enforcement."""

    def test_decode_with_custom_opts(self):
        """Custom DecodeOptions should be respected."""
        opts = DecodeOptions(max_array_len=5)
        # Create an array with 10 elements
        arr = Value.array([Value.int64(i) for i in range(10)])
        encoded = encode(arr)
        with pytest.raises(SecurityLimitExceeded):
            decode(encoded, opts=opts)

    def test_decode_max_depth(self):
        """Deep nesting should be rejected."""
        opts = DecodeOptions(max_depth=2)
        nested = Value.array([Value.array([Value.array([Value.int64(1)])])])
        encoded = encode(nested)
        with pytest.raises(SecurityLimitExceeded):
            decode(encoded, opts=opts)

    def test_decode_invalid_magic(self):
        with pytest.raises(ValueError, match="Invalid magic"):
            decode(b"XX\x02\x00\x00")

    def test_decode_invalid_version(self):
        with pytest.raises(ValueError, match="Unsupported version"):
            decode(b"SJ\x99\x00\x00")

    def test_decode_trailing_data(self):
        """Trailing data after root value should be rejected."""
        v = Value.int64(42)
        encoded = encode(v) + b"\x00\x00"
        with pytest.raises(ValueError, match="trailing data"):
            decode(encoded)


class TestCanonicalRoundtrip:
    """Invariant #2: encode(decode(bytes)) == bytes."""

    def test_encode_decode_encode_canonical(self):
        """For canonical values, encode(decode(bytes)) must produce identical bytes."""
        test_values = [
            Value.int64(42),
            Value.string("hello"),
            Value.array([Value.int64(1), Value.int64(2), Value.int64(3)]),
            Value.object({"a": Value.int64(1), "b": Value.int64(2)}),
            Value.object({
                "x": Value.array([Value.int64(10), Value.int64(20)]),
                "y": Value.array([Value.string("nested"), Value.string("map")]),
            }),
        ]
        for original in test_values:
            bytes1 = encode(original)
            decoded = decode(bytes1)
            bytes2 = encode(decoded)
            assert bytes1 == bytes2, (
                f"Canonical roundtrip failed for {original}: "
                f"bytes1={bytes1.hex()} != bytes2={bytes2.hex()}"
            )


class TestBinaryNaNInfRoundtrip:
    """NaN and Inf must roundtrip through cowrie binary encoding."""

    def test_nan_roundtrip(self):
        import math
        original = Value.float64(math.nan)
        decoded = decode(encode(original))
        assert decoded.type == Type.FLOAT64
        assert math.isnan(decoded.data)

    def test_positive_inf_roundtrip(self):
        import math
        original = Value.float64(math.inf)
        decoded = decode(encode(original))
        assert decoded.type == Type.FLOAT64
        assert math.isinf(decoded.data) and decoded.data > 0

    def test_negative_inf_roundtrip(self):
        import math
        original = Value.float64(-math.inf)
        decoded = decode(encode(original))
        assert decoded.type == Type.FLOAT64
        assert math.isinf(decoded.data) and decoded.data < 0


class TestInvariantTrailingGarbage:
    """Invariant #4: Trailing garbage after a valid root value must be rejected."""

    def test_decode_rejects_trailing_garbage(self):
        """Encode map {"a": 42}, append 0xFF, assert decode raises ValueError."""
        original = Value.object({"a": Value.int64(42)})
        data = encode(original)
        corrupted = data + b"\xff"
        with pytest.raises(ValueError, match="trailing"):
            decode(corrupted)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
