"""
Cowrie v2 Types - Extended type system for binary JSON.

This module provides:
  - Type enum for all Cowrie value types
  - Value class for representing Cowrie values
  - Constructor functions for each type
  - Conversion to/from Python native types
"""

from enum import IntEnum
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime, timezone
from decimal import Decimal as PyDecimal
import uuid as uuid_module


class Type(IntEnum):
    """Cowrie value types."""
    # Core types (0x00-0x1F)
    NULL = 0
    BOOL = 1
    INT64 = 2
    UINT64 = 3
    FLOAT64 = 4
    DECIMAL128 = 5
    STRING = 6
    BYTES = 7
    DATETIME64 = 8
    UUID128 = 9
    BIGINT = 10
    ARRAY = 11
    OBJECT = 12
    # v2.1 ML/Multimodal extensions (0x20-0x2F)
    TENSOR = 13
    TENSOR_REF = 14
    IMAGE = 15
    AUDIO = 16
    # v2.1 Graph/Delta extensions (0x30-0x3F)
    ADJLIST = 17
    RICH_TEXT = 18
    DELTA = 19


# Wire format type tags - Core (0x00-0x1F)
TAG_NULL = 0x00
TAG_FALSE = 0x01
TAG_TRUE = 0x02
TAG_INT64 = 0x03
TAG_FLOAT64 = 0x04
TAG_STRING = 0x05
TAG_ARRAY = 0x06
TAG_OBJECT = 0x07
TAG_BYTES = 0x08
TAG_UINT64 = 0x09
TAG_DECIMAL128 = 0x0A
TAG_DATETIME64 = 0x0B
TAG_UUID128 = 0x0C
TAG_BIGINT = 0x0D

# Wire format type tags - v2.1 ML/Multimodal (0x20-0x2F)
TAG_TENSOR = 0x20
TAG_TENSOR_REF = 0x21
TAG_IMAGE = 0x22
TAG_AUDIO = 0x23

# Wire format type tags - v2.1 Graph/Delta (0x30-0x3F)
TAG_ADJLIST = 0x30
TAG_RICH_TEXT = 0x31
TAG_DELTA = 0x32

# DType enum for TENSOR
class DType(IntEnum):
    FLOAT32 = 0x01
    FLOAT16 = 0x02
    BFLOAT16 = 0x03
    INT8 = 0x04
    INT16 = 0x05
    INT32 = 0x06
    INT64 = 0x07
    UINT8 = 0x08
    UINT16 = 0x09
    UINT32 = 0x0A
    UINT64 = 0x0B

# Image format enum
class ImageFormat(IntEnum):
    JPEG = 0x01
    PNG = 0x02
    WEBP = 0x03
    AVIF = 0x04
    BMP = 0x05

# Audio encoding enum
class AudioEncoding(IntEnum):
    PCM_INT16 = 0x01
    PCM_FLOAT32 = 0x02
    OPUS = 0x03
    AAC = 0x04

# ADJLIST id_width enum
class IdWidth(IntEnum):
    INT32 = 0x01
    INT64 = 0x02

# DELTA op codes
class DeltaOp(IntEnum):
    SET_FIELD = 0x01
    DELETE_FIELD = 0x02
    APPEND_ARRAY = 0x03

# Magic bytes and version
MAGIC = b'SJ'
VERSION = 2


class Decimal128Data:
    """128-bit decimal representation. Value = Coef * 10^(-Scale)"""
    __slots__ = ('scale', 'coef')

    def __init__(self, scale: int, coef: bytes):
        self.scale = scale  # -127 to +127
        self.coef = coef    # 16 bytes, two's complement big-endian

    def to_decimal(self) -> PyDecimal:
        """Convert to Python Decimal."""
        # Parse two's complement big-endian coefficient
        coef_int = int.from_bytes(self.coef, byteorder='big', signed=True)
        return PyDecimal(coef_int) * (PyDecimal(10) ** (-self.scale))

    @classmethod
    def from_decimal(cls, d: PyDecimal) -> 'Decimal128Data':
        """Create from Python Decimal."""
        sign, digits, exponent = d.as_tuple()

        # Build coefficient
        coef_int = int(''.join(str(d) for d in digits)) if digits else 0
        if sign:
            coef_int = -coef_int

        # Scale is negative exponent
        scale = -exponent if exponent else 0

        # Convert to 16-byte two's complement
        try:
            coef_bytes = coef_int.to_bytes(16, byteorder='big', signed=True)
        except OverflowError:
            raise ValueError(f"Decimal coefficient too large for Decimal128: {d}")

        return cls(scale, coef_bytes)


# ============================================================
# v2.1 Extension Type Data Classes
# ============================================================

class TensorData:
    """Tensor data: embeddings, feature vectors, model I/O."""
    __slots__ = ('dtype', 'dims', 'data')

    def __init__(self, dtype: DType, dims: Tuple[int, ...], data: bytes):
        self.dtype = dtype
        self.dims = dims  # shape tuple
        self.data = data  # raw bytes, row-major

    @property
    def rank(self) -> int:
        return len(self.dims)

    @property
    def numel(self) -> int:
        """Total number of elements."""
        result = 1
        for d in self.dims:
            result *= d
        return result


class TensorRefData:
    """Reference to a stored tensor (vector DB, KV cache)."""
    __slots__ = ('store_id', 'key')

    def __init__(self, store_id: int, key: bytes):
        self.store_id = store_id  # u8: which store/shard
        self.key = key  # lookup key (UUID, hash, etc.)


class ImageData:
    """Image data: raw images without base64."""
    __slots__ = ('format', 'width', 'height', 'data')

    def __init__(self, fmt: ImageFormat, width: int, height: int, data: bytes):
        self.format = fmt
        self.width = width    # u16
        self.height = height  # u16
        self.data = data      # encoded image bytes


class AudioData:
    """Audio data: waveforms, voice clips."""
    __slots__ = ('encoding', 'sample_rate', 'channels', 'data')

    def __init__(self, encoding: AudioEncoding, sample_rate: int, channels: int, data: bytes):
        self.encoding = encoding
        self.sample_rate = sample_rate  # u32
        self.channels = channels        # u8
        self.data = data


class AdjlistData:
    """CSR adjacency list for graphs/GNNs."""
    __slots__ = ('id_width', 'node_count', 'edge_count', 'row_offsets', 'col_indices')

    def __init__(self, id_width: IdWidth, row_offsets: List[int], col_indices: List[int]):
        self.id_width = id_width
        self.row_offsets = row_offsets  # len = node_count + 1
        self.col_indices = col_indices  # len = edge_count
        self.node_count = len(row_offsets) - 1
        self.edge_count = len(col_indices)


class RichTextSpan:
    """Annotated span within rich text."""
    __slots__ = ('start', 'end', 'kind_id')

    def __init__(self, start: int, end: int, kind_id: int):
        self.start = start    # byte offset
        self.end = end        # byte offset
        self.kind_id = kind_id  # application-defined


class RichTextData:
    """Text with optional token IDs and annotated spans."""
    __slots__ = ('text', 'tokens', 'spans')

    def __init__(self, text: str, tokens: Optional[List[int]] = None,
                 spans: Optional[List[RichTextSpan]] = None):
        self.text = text
        self.tokens = tokens  # int32 token IDs
        self.spans = spans    # annotated spans


class DeltaOpData:
    """Single operation in a delta patch."""
    __slots__ = ('op_code', 'field_id', 'value')

    def __init__(self, op_code: DeltaOp, field_id: int, value: Optional['Value'] = None):
        self.op_code = op_code
        self.field_id = field_id  # dictionary-coded field ID
        self.value = value  # for SET_FIELD and APPEND_ARRAY


class DeltaData:
    """Semantic diff/patch vs a previous object/state."""
    __slots__ = ('base_id', 'ops')

    def __init__(self, base_id: int, ops: List[DeltaOpData]):
        self.base_id = base_id  # reference to base object
        self.ops = ops


class Value:
    """
    Represents a Cowrie value of any type.

    Use constructor functions like Null(), Int64(), String(), etc.
    """
    __slots__ = ('_type', '_data')

    def __init__(self, typ: Type, data: Any = None):
        self._type = typ
        self._data = data

    @property
    def type(self) -> Type:
        """Get the type of this value."""
        return self._type

    def is_null(self) -> bool:
        return self._type == Type.NULL

    def is_bool(self) -> bool:
        return self._type == Type.BOOL

    def is_int64(self) -> bool:
        return self._type == Type.INT64

    def is_uint64(self) -> bool:
        return self._type == Type.UINT64

    def is_float64(self) -> bool:
        return self._type == Type.FLOAT64

    def is_string(self) -> bool:
        return self._type == Type.STRING

    def is_bytes(self) -> bool:
        return self._type == Type.BYTES

    def is_array(self) -> bool:
        return self._type == Type.ARRAY

    def is_object(self) -> bool:
        return self._type == Type.OBJECT

    def is_decimal128(self) -> bool:
        return self._type == Type.DECIMAL128

    def is_datetime64(self) -> bool:
        return self._type == Type.DATETIME64

    def is_uuid128(self) -> bool:
        return self._type == Type.UUID128

    def is_bigint(self) -> bool:
        return self._type == Type.BIGINT

    # v2.1 extension type checks
    def is_tensor(self) -> bool:
        return self._type == Type.TENSOR

    def is_tensor_ref(self) -> bool:
        return self._type == Type.TENSOR_REF

    def is_image(self) -> bool:
        return self._type == Type.IMAGE

    def is_audio(self) -> bool:
        return self._type == Type.AUDIO

    def is_adjlist(self) -> bool:
        return self._type == Type.ADJLIST

    def is_rich_text(self) -> bool:
        return self._type == Type.RICH_TEXT

    def is_delta(self) -> bool:
        return self._type == Type.DELTA

    # Accessor methods

    def bool_value(self) -> bool:
        """Get boolean value. Raises TypeError if not a bool."""
        if self._type != Type.BOOL:
            raise TypeError(f"Expected BOOL, got {self._type.name}")
        return self._data

    def int64_value(self) -> int:
        """Get int64 value. Raises TypeError if not an int64."""
        if self._type != Type.INT64:
            raise TypeError(f"Expected INT64, got {self._type.name}")
        return self._data

    def uint64_value(self) -> int:
        """Get uint64 value. Raises TypeError if not a uint64."""
        if self._type != Type.UINT64:
            raise TypeError(f"Expected UINT64, got {self._type.name}")
        return self._data

    def float64_value(self) -> float:
        """Get float64 value. Raises TypeError if not a float64."""
        if self._type != Type.FLOAT64:
            raise TypeError(f"Expected FLOAT64, got {self._type.name}")
        return self._data

    def string_value(self) -> str:
        """Get string value. Raises TypeError if not a string."""
        if self._type != Type.STRING:
            raise TypeError(f"Expected STRING, got {self._type.name}")
        return self._data

    def bytes_value(self) -> bytes:
        """Get bytes value. Raises TypeError if not bytes."""
        if self._type != Type.BYTES:
            raise TypeError(f"Expected BYTES, got {self._type.name}")
        return self._data

    def array_value(self) -> List['Value']:
        """Get array value. Raises TypeError if not an array."""
        if self._type != Type.ARRAY:
            raise TypeError(f"Expected ARRAY, got {self._type.name}")
        return self._data

    def object_value(self) -> List[Tuple[str, 'Value']]:
        """Get object value as list of (key, value) pairs. Raises TypeError if not an object."""
        if self._type != Type.OBJECT:
            raise TypeError(f"Expected OBJECT, got {self._type.name}")
        return self._data

    def decimal128_value(self) -> Decimal128Data:
        """Get Decimal128 value. Raises TypeError if not a decimal128."""
        if self._type != Type.DECIMAL128:
            raise TypeError(f"Expected DECIMAL128, got {self._type.name}")
        return self._data

    def datetime64_value(self) -> int:
        """Get datetime value as nanoseconds since Unix epoch. Raises TypeError if not a datetime64."""
        if self._type != Type.DATETIME64:
            raise TypeError(f"Expected DATETIME64, got {self._type.name}")
        return self._data

    def uuid128_value(self) -> bytes:
        """Get UUID value as 16 bytes. Raises TypeError if not a uuid128."""
        if self._type != Type.UUID128:
            raise TypeError(f"Expected UUID128, got {self._type.name}")
        return self._data

    def bigint_value(self) -> bytes:
        """Get BigInt value as two's complement big-endian bytes. Raises TypeError if not a bigint."""
        if self._type != Type.BIGINT:
            raise TypeError(f"Expected BIGINT, got {self._type.name}")
        return self._data

    # v2.1 extension type accessors

    def tensor_value(self) -> TensorData:
        """Get Tensor value. Raises TypeError if not a tensor."""
        if self._type != Type.TENSOR:
            raise TypeError(f"Expected TENSOR, got {self._type.name}")
        return self._data

    def tensor_ref_value(self) -> TensorRefData:
        """Get TensorRef value. Raises TypeError if not a tensor_ref."""
        if self._type != Type.TENSOR_REF:
            raise TypeError(f"Expected TENSOR_REF, got {self._type.name}")
        return self._data

    def image_value(self) -> ImageData:
        """Get Image value. Raises TypeError if not an image."""
        if self._type != Type.IMAGE:
            raise TypeError(f"Expected IMAGE, got {self._type.name}")
        return self._data

    def audio_value(self) -> AudioData:
        """Get Audio value. Raises TypeError if not audio."""
        if self._type != Type.AUDIO:
            raise TypeError(f"Expected AUDIO, got {self._type.name}")
        return self._data

    def adjlist_value(self) -> AdjlistData:
        """Get Adjlist value. Raises TypeError if not an adjlist."""
        if self._type != Type.ADJLIST:
            raise TypeError(f"Expected ADJLIST, got {self._type.name}")
        return self._data

    def rich_text_value(self) -> RichTextData:
        """Get RichText value. Raises TypeError if not rich_text."""
        if self._type != Type.RICH_TEXT:
            raise TypeError(f"Expected RICH_TEXT, got {self._type.name}")
        return self._data

    def delta_value(self) -> DeltaData:
        """Get Delta value. Raises TypeError if not a delta."""
        if self._type != Type.DELTA:
            raise TypeError(f"Expected DELTA, got {self._type.name}")
        return self._data

    # Object/Array helpers

    def __len__(self) -> int:
        """Length of array or object."""
        if self._type == Type.ARRAY:
            return len(self._data)
        elif self._type == Type.OBJECT:
            return len(self._data)
        return 0

    def __getitem__(self, key: Union[int, str]) -> 'Value':
        """Index into array or object."""
        if self._type == Type.ARRAY:
            return self._data[key]
        elif self._type == Type.OBJECT:
            for k, v in self._data:
                if k == key:
                    return v
            raise KeyError(key)
        raise TypeError(f"Cannot index into {self._type.name}")

    def get(self, key: str, default: Optional['Value'] = None) -> Optional['Value']:
        """Get value from object by key."""
        if self._type != Type.OBJECT:
            raise TypeError(f"Expected OBJECT, got {self._type.name}")
        for k, v in self._data:
            if k == key:
                return v
        return default

    def keys(self) -> List[str]:
        """Get all keys from object."""
        if self._type != Type.OBJECT:
            raise TypeError(f"Expected OBJECT, got {self._type.name}")
        return [k for k, v in self._data]

    def values(self) -> List['Value']:
        """Get all values from object."""
        if self._type != Type.OBJECT:
            raise TypeError(f"Expected OBJECT, got {self._type.name}")
        return [v for k, v in self._data]

    def items(self) -> List[Tuple[str, 'Value']]:
        """Get all (key, value) pairs from object."""
        if self._type != Type.OBJECT:
            raise TypeError(f"Expected OBJECT, got {self._type.name}")
        return self._data

    def __repr__(self) -> str:
        if self._type == Type.NULL:
            return "Null()"
        elif self._type == Type.BOOL:
            return f"Bool({self._data})"
        elif self._type == Type.INT64:
            return f"Int64({self._data})"
        elif self._type == Type.UINT64:
            return f"Uint64({self._data})"
        elif self._type == Type.FLOAT64:
            return f"Float64({self._data})"
        elif self._type == Type.STRING:
            return f"String({self._data!r})"
        elif self._type == Type.BYTES:
            return f"Bytes({self._data!r})"
        elif self._type == Type.ARRAY:
            return f"Array({self._data!r})"
        elif self._type == Type.OBJECT:
            return f"Object({self._data!r})"
        elif self._type == Type.DECIMAL128:
            return f"Decimal128({self._data.to_decimal()})"
        elif self._type == Type.DATETIME64:
            return f"Datetime64({self._data})"
        elif self._type == Type.UUID128:
            return f"UUID128({self._data.hex()})"
        elif self._type == Type.BIGINT:
            return f"BigInt({int.from_bytes(self._data, byteorder='big', signed=True)})"
        # v2.1 extension types
        elif self._type == Type.TENSOR:
            t = self._data
            return f"Tensor({t.dtype.name}, {t.dims}, {len(t.data)} bytes)"
        elif self._type == Type.TENSOR_REF:
            t = self._data
            return f"TensorRef(store={t.store_id}, key={t.key.hex()})"
        elif self._type == Type.IMAGE:
            img = self._data
            return f"Image({img.format.name}, {img.width}x{img.height}, {len(img.data)} bytes)"
        elif self._type == Type.AUDIO:
            a = self._data
            return f"Audio({a.encoding.name}, {a.sample_rate}Hz, {a.channels}ch, {len(a.data)} bytes)"
        elif self._type == Type.ADJLIST:
            adj = self._data
            return f"Adjlist({adj.node_count} nodes, {adj.edge_count} edges)"
        elif self._type == Type.RICH_TEXT:
            rt = self._data
            tokens = len(rt.tokens) if rt.tokens else 0
            spans = len(rt.spans) if rt.spans else 0
            return f"RichText({rt.text!r}, {tokens} tokens, {spans} spans)"
        elif self._type == Type.DELTA:
            d = self._data
            return f"Delta(base={d.base_id}, {len(d.ops)} ops)"
        return f"Value({self._type}, {self._data})"


# === Constructor Functions ===

def Null() -> Value:
    """Create a null value."""
    return Value(Type.NULL)


def Bool(v: bool) -> Value:
    """Create a boolean value."""
    return Value(Type.BOOL, v)


def Int64(v: int) -> Value:
    """Create a signed 64-bit integer value."""
    if not (-2**63 <= v < 2**63):
        raise ValueError(f"Value {v} out of int64 range")
    return Value(Type.INT64, v)


def Uint64(v: int) -> Value:
    """Create an unsigned 64-bit integer value."""
    if not (0 <= v < 2**64):
        raise ValueError(f"Value {v} out of uint64 range")
    return Value(Type.UINT64, v)


def Float64(v: float) -> Value:
    """Create a 64-bit floating point value."""
    return Value(Type.FLOAT64, v)


def String(v: str) -> Value:
    """Create a string value."""
    return Value(Type.STRING, v)


def Bytes(v: bytes) -> Value:
    """Create a bytes value."""
    return Value(Type.BYTES, v)


def Array(*items: Value) -> Value:
    """Create an array value."""
    return Value(Type.ARRAY, list(items))


def Object(*members: Tuple[str, Value]) -> Value:
    """Create an object value from (key, value) pairs."""
    return Value(Type.OBJECT, list(members))


def Decimal128(v: Union[PyDecimal, str, float]) -> Value:
    """Create a 128-bit decimal value."""
    if isinstance(v, str):
        v = PyDecimal(v)
    elif isinstance(v, float):
        v = PyDecimal(str(v))
    data = Decimal128Data.from_decimal(v)
    return Value(Type.DECIMAL128, data)


def Datetime64(nanos: int) -> Value:
    """Create a datetime value from nanoseconds since Unix epoch."""
    return Value(Type.DATETIME64, nanos)


def Datetime(dt: datetime) -> Value:
    """Create a datetime value from a Python datetime."""
    # Convert to nanoseconds since epoch
    ts = dt.timestamp()
    nanos = int(ts * 1_000_000_000)
    return Value(Type.DATETIME64, nanos)


def UUID128(v: bytes) -> Value:
    """Create a UUID value from 16 bytes."""
    if len(v) != 16:
        raise ValueError(f"UUID must be 16 bytes, got {len(v)}")
    return Value(Type.UUID128, v)


def UUID(v: Union[bytes, str, uuid_module.UUID]) -> Value:
    """Create a UUID value from bytes, string, or uuid.UUID."""
    if isinstance(v, uuid_module.UUID):
        return Value(Type.UUID128, v.bytes)
    elif isinstance(v, str):
        return Value(Type.UUID128, uuid_module.UUID(v).bytes)
    elif isinstance(v, bytes) and len(v) == 16:
        return Value(Type.UUID128, v)
    raise ValueError(f"Invalid UUID: {v}")


def BigInt(v: Union[int, bytes]) -> Value:
    """Create a BigInt value from an integer or bytes."""
    if isinstance(v, int):
        # Determine byte length needed
        byte_len = (v.bit_length() + 8) // 8  # +8 for sign bit
        if byte_len == 0:
            byte_len = 1
        v = v.to_bytes(byte_len, byteorder='big', signed=True)
    return Value(Type.BIGINT, v)


# === v2.1 Extension Constructor Functions ===

def Tensor(dtype: DType, dims: Tuple[int, ...], data: bytes) -> Value:
    """Create a tensor value (embeddings, feature vectors, model I/O)."""
    return Value(Type.TENSOR, TensorData(dtype, dims, data))


def TensorRef(store_id: int, key: bytes) -> Value:
    """Create a tensor reference (pointer to stored embedding/tensor)."""
    if not (0 <= store_id <= 255):
        raise ValueError(f"store_id must be 0-255, got {store_id}")
    return Value(Type.TENSOR_REF, TensorRefData(store_id, key))


def Image(fmt: ImageFormat, width: int, height: int, data: bytes) -> Value:
    """Create an image value (raw images without base64)."""
    if not (0 <= width <= 65535):
        raise ValueError(f"width must be 0-65535, got {width}")
    if not (0 <= height <= 65535):
        raise ValueError(f"height must be 0-65535, got {height}")
    return Value(Type.IMAGE, ImageData(fmt, width, height, data))


def Audio(encoding: AudioEncoding, sample_rate: int, channels: int, data: bytes) -> Value:
    """Create an audio value (waveforms, voice clips)."""
    if not (0 <= sample_rate <= 0xFFFFFFFF):
        raise ValueError(f"sample_rate must be u32, got {sample_rate}")
    if not (0 <= channels <= 255):
        raise ValueError(f"channels must be 0-255, got {channels}")
    return Value(Type.AUDIO, AudioData(encoding, sample_rate, channels, data))


def Adjlist(id_width: IdWidth, row_offsets: List[int], col_indices: List[int]) -> Value:
    """Create a CSR adjacency list for graphs/GNNs."""
    return Value(Type.ADJLIST, AdjlistData(id_width, row_offsets, col_indices))


def RichText(text: str, tokens: Optional[List[int]] = None,
             spans: Optional[List[RichTextSpan]] = None) -> Value:
    """Create rich text with optional token IDs and annotated spans."""
    return Value(Type.RICH_TEXT, RichTextData(text, tokens, spans))


def Delta(base_id: int, ops: List[DeltaOpData]) -> Value:
    """Create a delta patch (semantic diff vs previous state)."""
    return Value(Type.DELTA, DeltaData(base_id, ops))


# === Python Conversion ===

def from_python(obj: Any) -> Value:
    """
    Convert a Python object to a Cowrie Value.

    Supports: None, bool, int, float, str, bytes, list, dict,
              datetime, uuid.UUID, Decimal
    """
    if obj is None:
        return Null()
    elif isinstance(obj, bool):
        return Bool(obj)
    elif isinstance(obj, int):
        if obj < 0:
            return Int64(obj)
        elif obj < 2**63:
            return Int64(obj)
        elif obj < 2**64:
            return Uint64(obj)
        else:
            return BigInt(obj)
    elif isinstance(obj, float):
        return Float64(obj)
    elif isinstance(obj, str):
        return String(obj)
    elif isinstance(obj, bytes):
        return Bytes(obj)
    elif isinstance(obj, list):
        return Array(*[from_python(item) for item in obj])
    elif isinstance(obj, dict):
        return Object(*[(k, from_python(v)) for k, v in obj.items()])
    elif isinstance(obj, datetime):
        return Datetime(obj)
    elif isinstance(obj, uuid_module.UUID):
        return UUID(obj)
    elif isinstance(obj, PyDecimal):
        return Decimal128(obj)
    elif isinstance(obj, Value):
        return obj
    else:
        raise TypeError(f"Cannot convert {type(obj).__name__} to Cowrie Value")


def to_python(v: Value) -> Any:
    """
    Convert a Cowrie Value to a Python object.

    Returns native Python types where possible.
    """
    if v.is_null():
        return None
    elif v.is_bool():
        return v.bool_value()
    elif v.is_int64():
        return v.int64_value()
    elif v.is_uint64():
        return v.uint64_value()
    elif v.is_float64():
        return v.float64_value()
    elif v.is_string():
        return v.string_value()
    elif v.is_bytes():
        return v.bytes_value()
    elif v.is_array():
        return [to_python(item) for item in v.array_value()]
    elif v.is_object():
        return {k: to_python(val) for k, val in v.object_value()}
    elif v.is_decimal128():
        return v.decimal128_value().to_decimal()
    elif v.is_datetime64():
        nanos = v.datetime64_value()
        return datetime.fromtimestamp(nanos / 1_000_000_000, tz=timezone.utc)
    elif v.is_uuid128():
        return uuid_module.UUID(bytes=v.uuid128_value())
    elif v.is_bigint():
        data = v.bigint_value()
        return int.from_bytes(data, byteorder='big', signed=True)
    else:
        raise ValueError(f"Unknown Cowrie type: {v.type}")
