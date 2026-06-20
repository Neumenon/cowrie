"""Tests for the four correctness fixes from the deep-review action.

FIX 1 — BigInt sign (two's-complement round-trip via gen1 and gen2):
    Already correct in Python — test confirms expected behaviour.

FIX 2 — Float64 precision default:
    N/A — Python from_any never auto-tensorizes lists, so no downcast exists.

FIX 3 — Tensor decode validation:
    Real bug fixed: decoder now rejects tensors where dataLen != product(dims)*elemSize.

FIX 4 — schema_equals structural:
    Real bug fixed: schema_equals now does recursive structural comparison;
    schema_fingerprint_equal preserved as the fast probabilistic path.
"""

import struct
import pytest

from cowrie.gen2 import (
    encode, decode, Value, Type, DType,
    TensorData, ImageFormat, AudioEncoding,
    schema_equals, schema_fingerprint_equal,
    MAGIC, VERSION,
    encode_uvarint,
    Tag,
)
import cowrie.gen1 as gen1


# ============================================================
# FIX 1 — BigInt sign (two's-complement)
# ============================================================

class TestBigIntSign:
    """
    WHY: BigInt (tag 0x0D) must use minimal two's-complement big-endian bytes.
    Negative values encode with the top bit set; decoding must restore sign.
    Python's int.from_bytes / int.to_bytes with signed=True gives this
    automatically if the minimal byte length is used.  These tests verify
    round-trip fidelity and byte-level encoding for canonical values.
    """

    def _bigint_bytes(self, n: int) -> bytes:
        """Produce minimal two's-complement big-endian bytes for n (matches Go).

        Algorithm (mirrors Go's bigIntToTwosComplement):
        - zero          → single 0x00 byte
        - positive n    → minimal big-endian; prepend 0x00 if top bit is set
        - negative n    → use (-n-1).bit_length() to find minimal byte count
        """
        if n == 0:
            return b'\x00'
        if n > 0:
            byte_len = (n.bit_length() + 8) // 8
        else:
            # For negative: the complement magnitude is (-n-1)
            byte_len = ((-n - 1).bit_length() + 8) // 8
        return n.to_bytes(byte_len, byteorder='big', signed=True)

    def _roundtrip_gen2(self, n: int) -> int:
        """Encode n as gen2 BigInt, decode, interpret as signed integer."""
        raw = self._bigint_bytes(n)
        v = Value.bigint(raw)
        out = decode(encode(v))
        assert out.type == Type.BIGINT
        return int.from_bytes(out.data, 'big', signed=True)

    def test_zero(self):
        assert self._roundtrip_gen2(0) == 0

    def test_positive_small(self):
        assert self._roundtrip_gen2(1) == 1

    def test_positive_255(self):
        # 255 needs the 0x00 sign-extension prefix in two's-complement
        raw = self._bigint_bytes(255)
        assert raw == b'\x00\xff', f"Expected 0x00ff, got {raw.hex()}"
        assert self._roundtrip_gen2(255) == 255

    def test_negative_one(self):
        raw = self._bigint_bytes(-1)
        assert raw == b'\xff', f"Expected 0xff, got {raw.hex()}"
        assert self._roundtrip_gen2(-1) == -1

    def test_negative_128(self):
        # -128 in minimal two's-complement big-endian is 0x80 (1 byte).
        # Top bit is set → the value is correctly interpreted as negative.
        # (-(-128)-1).bit_length() = 127.bit_length() = 7 → byte_len = (7+8)//8 = 1
        raw = self._bigint_bytes(-128)
        assert raw == b'\x80', f"Expected 0x80 for -128 in two's-complement, got {raw.hex()}"
        assert self._roundtrip_gen2(-128) == -128

    def test_negative_256(self):
        raw = self._bigint_bytes(-256)
        assert raw == b'\xff\x00', f"Expected 0xff00, got {raw.hex()}"
        assert self._roundtrip_gen2(-256) == -256

    def test_large_negative(self):
        n = -(2 ** 100)
        assert self._roundtrip_gen2(n) == n

    def test_gen1_bigint_decode_signed(self):
        """
        WHY: gen1's decode path must interpret BigInt bytes as signed.
        Craft a raw gen1 packet with tag 0x0D and bytes 0xff (= -1).
        """
        import io
        # Build gen1 wire bytes manually: tag | varint(len) | payload
        tag = gen1.TAG_BIGINT          # 0x0D
        payload = b'\xff'              # two's-complement -1
        buf = bytes([tag]) + bytes([len(payload)]) + payload
        result = gen1.decode(buf)
        assert result == -1, f"Expected -1 from gen1 bigint decode, got {result}"

    def test_gen1_bigint_decode_positive(self):
        """
        WHY: positive values whose top bit is clear must be decoded unsigned.
        """
        payload = b'\x01'
        tag = gen1.TAG_BIGINT
        buf = bytes([tag]) + bytes([len(payload)]) + payload
        assert gen1.decode(buf) == 1


# ============================================================
# FIX 2 — Float64 precision (N/A)
# ============================================================

class TestFloat64PrecisionNA:
    """
    WHY: Python's from_any wraps each float item as Value.float64() individually.
    There is no auto-tensorization of float lists, so no silent float32 downcast
    can occur. This test documents that behaviour explicitly so a future change
    cannot silently introduce the bug.
    """

    def test_float_list_stays_array_not_tensor(self):
        """A list of floats through from_any must NOT become a tensor."""
        from cowrie.gen2 import from_any
        v = from_any([1.0, 2.0, 3.0])
        assert v.type == Type.ARRAY, (
            "from_any([float, ...]) must produce ARRAY, not TENSOR — "
            "Python has no auto-tensorization; float64 precision is preserved"
        )

    def test_float_values_are_float64(self):
        """Individual floats must be encoded as FLOAT64 (not FLOAT32)."""
        from cowrie.gen2 import from_any
        v = from_any(3.141592653589793)
        assert v.type == Type.FLOAT64
        out = decode(encode(v))
        assert out.type == Type.FLOAT64
        # float64 round-trip should be exact
        assert out.data == 3.141592653589793


# ============================================================
# FIX 3 — Tensor decode validation
# ============================================================

def _build_malformed_tensor(dtype_byte: int, dims: list[int], wrong_data_len: int, data: bytes) -> bytes:
    """
    Build a raw Cowrie v2 packet containing a tensor with a deliberately
    wrong dataLen field (so dataLen != product(dims)*elemSize).

    Wire layout:
        Magic(2) Version(1) Flags(1) DictLen(varint=0)
        TagTensor(1) dtype(1) rank(1) dim*(varint each)
        dataLen(varint=wrong_data_len) data
    """
    buf = bytearray()
    buf += MAGIC
    buf += bytes([VERSION, 0x00])  # flags
    buf += encode_uvarint(0)       # empty dict
    buf.append(Tag.TENSOR)
    buf.append(dtype_byte)
    buf.append(len(dims))
    for d in dims:
        buf += encode_uvarint(d)
    buf += encode_uvarint(wrong_data_len)
    buf += data
    return bytes(buf)


class TestTensorDecodeValidation:
    """
    WHY: Without dataLen validation a malformed tensor (wrong byte count for
    the declared shape/dtype) silently corrupts callers who iterate over
    elements. The decoder must reject shape/length mismatches.
    """

    def test_valid_tensor_decodes(self):
        """A well-formed 2×3 float32 tensor must decode without error."""
        # 6 elements × 4 bytes = 24 bytes
        data = struct.pack('<6f', 1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
        v = Value.tensor(DType.FLOAT32, [2, 3], data)
        out = decode(encode(v))
        assert out.type == Type.TENSOR
        td = out.data
        assert td.dtype == DType.FLOAT32
        assert td.shape == [2, 3]
        assert td.data == data

    def test_malformed_tensor_raises(self):
        """
        WHY: A 2×3 float32 tensor should have 24 bytes but we supply dataLen=8.
        The decoder MUST raise ValueError so callers are not silently misled.
        """
        # Claim dataLen=8 but shape is [2,3] → expected 24 bytes for float32
        payload = bytes(8)
        raw = _build_malformed_tensor(
            dtype_byte=DType.FLOAT32,
            dims=[2, 3],
            wrong_data_len=8,  # WRONG: should be 24
            data=payload,
        )
        with pytest.raises(ValueError, match="dataLen"):
            decode(raw)

    def test_malformed_float64_tensor_raises(self):
        """
        WHY: A [3] float64 tensor expects 24 bytes; supplying 8 must be rejected.
        """
        payload = bytes(8)
        raw = _build_malformed_tensor(
            dtype_byte=DType.FLOAT64,
            dims=[3],
            wrong_data_len=8,  # WRONG: should be 24
            data=payload,
        )
        with pytest.raises(ValueError, match="dataLen"):
            decode(raw)

    def test_zero_dim_tensor_decodes(self):
        """A tensor with a zero dimension has 0 data bytes — must decode cleanly."""
        v = Value.tensor(DType.FLOAT32, [0, 4], b"")
        out = decode(encode(v))
        assert out.type == Type.TENSOR
        assert out.data.shape == [0, 4]

    def test_correct_float32_tensor_passes_validation(self):
        """
        WHY: The new validation must NOT reject tensors with the correct byte count.
        A 1×4 float32 tensor has 16 bytes; this must decode successfully.
        """
        data = struct.pack('<4f', 1.0, 2.0, 3.0, 4.0)
        v = Value.tensor(DType.FLOAT32, [4], data)
        out = decode(encode(v))
        assert out.type == Type.TENSOR
        assert out.data.shape == [4]
        assert struct.unpack('<4f', out.data.data) == (1.0, 2.0, 3.0, 4.0)

    def test_correct_int64_tensor_passes_validation(self):
        """
        WHY: int64 elements are 8 bytes each; shape [3] → 24 bytes total.
        Validation must accept this.
        """
        data = struct.pack('<3q', 10, 20, 30)
        v = Value.tensor(DType.INT64, [3], data)
        out = decode(encode(v))
        assert out.type == Type.TENSOR
        assert struct.unpack('<3q', out.data.data) == (10, 20, 30)

    def test_decoder_rejects_wrong_datalen_not_constructor(self):
        """
        WHY: The validation lives in the decoder, not only the TensorData
        constructor.  A raw packet with a lying dataLen field must be rejected
        even though TensorData itself would accept whatever bytes are given.
        This confirms the fix is in the decode path, not just the constructor.
        """
        # Build a [1,1] float32 tensor whose wire dataLen claims 100 bytes
        # but the actual expected count is 4. We give 100 bytes so the reader
        # doesn't hit EOF — the shape/dtype check must catch it first.
        payload = bytes(100)
        raw = _build_malformed_tensor(
            dtype_byte=DType.FLOAT32,
            dims=[1, 1],
            wrong_data_len=100,   # WRONG: should be 4
            data=payload,
        )
        with pytest.raises(ValueError, match="dataLen"):
            decode(raw)


# ============================================================
# FIX 4 — schema_equals structural
# ============================================================

class TestSchemaEqualsStructural:
    """
    WHY: schema_equals based on fingerprint comparison is probabilistic
    (1-in-2^64 collision risk) and can silently accept mismatched schemas
    in rare cases.  The correct implementation must be deterministic structural
    recursion.  schema_fingerprint_equal is preserved as the fast-path alias.
    """

    def _obj(self, **fields) -> Value:
        """Build a Value.object from keyword=Value pairs."""
        return Value.object(fields)

    def test_same_schema_same_values_true(self):
        """Identical schema and type → True."""
        a = self._obj(x=Value.int64(1), y=Value.string("hello"))
        b = self._obj(x=Value.int64(99), y=Value.string("world"))
        assert schema_equals(a, b) is True

    def test_renamed_field_false(self):
        """
        WHY: If a field is renamed the schemas differ structurally even if
        the fingerprints of the old and new names happened to collide.
        schema_equals must catch this deterministically.
        """
        a = self._obj(x=Value.int64(1))
        c = self._obj(y=Value.int64(1))   # different field name
        assert schema_equals(a, c) is False

    def test_changed_field_type_false(self):
        """Changing a field's type must make schema_equals return False."""
        a = self._obj(x=Value.int64(1))
        d = self._obj(x=Value.string("1"))  # same name, different type
        assert schema_equals(a, d) is False

    def test_different_field_count_false(self):
        """Different number of fields → False."""
        a = self._obj(x=Value.int64(1), y=Value.int64(2))
        b = self._obj(x=Value.int64(1))
        assert schema_equals(a, b) is False

    def test_scalars_same_type_true(self):
        """Two int64 values have the same schema regardless of value."""
        assert schema_equals(Value.int64(0), Value.int64(999)) is True

    def test_scalars_different_type_false(self):
        """int64 vs float64 → different schema."""
        assert schema_equals(Value.int64(0), Value.float64(0.0)) is False

    def test_arrays_same_element_schema_true(self):
        """Arrays with the same element count and element schemas → True."""
        a = Value.array([Value.int64(1), Value.int64(2)])
        b = Value.array([Value.int64(10), Value.int64(20)])
        assert schema_equals(a, b) is True

    def test_arrays_different_element_schema_false(self):
        """Arrays with different element schemas → False."""
        a = Value.array([Value.int64(1)])
        b = Value.array([Value.string("x")])
        assert schema_equals(a, b) is False

    def test_arrays_different_length_false(self):
        """Arrays of different lengths → False."""
        a = Value.array([Value.int64(1), Value.int64(2)])
        b = Value.array([Value.int64(1)])
        assert schema_equals(a, b) is False

    def test_tensor_same_dtype_rank_true(self):
        """Tensors with same dtype and rank (different dims) have same schema."""
        a = Value.tensor(DType.FLOAT32, [2, 3], bytes(24))
        b = Value.tensor(DType.FLOAT32, [4, 5], bytes(80))
        assert schema_equals(a, b) is True

    def test_tensor_different_dtype_false(self):
        """Tensors with different dtype → different schema."""
        a = Value.tensor(DType.FLOAT32, [3], bytes(12))
        b = Value.tensor(DType.FLOAT64, [3], bytes(24))
        assert schema_equals(a, b) is False

    def test_tensor_different_rank_false(self):
        """Tensors with different rank → different schema."""
        a = Value.tensor(DType.FLOAT32, [6], bytes(24))
        b = Value.tensor(DType.FLOAT32, [2, 3], bytes(24))
        assert schema_equals(a, b) is False

    def test_nested_objects_structural(self):
        """Nested objects must be compared structurally."""
        inner_a = self._obj(z=Value.int64(0))
        inner_b = self._obj(z=Value.int64(999))
        inner_c = self._obj(w=Value.int64(0))  # different field name

        a = self._obj(inner=inner_a)
        b = self._obj(inner=inner_b)   # same schema
        c = self._obj(inner=inner_c)   # different nested schema

        assert schema_equals(a, b) is True
        assert schema_equals(a, c) is False

    def test_schema_fingerprint_equal_fast_path(self):
        """schema_fingerprint_equal agrees with schema_equals on identical schemas."""
        a = self._obj(x=Value.int64(1), y=Value.string("a"))
        b = self._obj(x=Value.int64(2), y=Value.string("b"))
        assert schema_fingerprint_equal(a, b) is True
        assert schema_equals(a, b) is True

    def test_schema_fingerprint_equal_exists(self):
        """schema_fingerprint_equal must be importable from cowrie.gen2."""
        from cowrie.gen2 import schema_fingerprint_equal as sfe
        assert callable(sfe)
