"""
Cowrie v2 - "JSON++" Binary Codec for Python

A binary format that extends JSON with:
- Explicit integer types (int64, uint64)
- Decimal128 for high-precision decimals
- Native binary data (no base64)
- Datetime64 (nanosecond timestamps)
- UUID128 (native UUIDs)
- BigInt (arbitrary precision)
- Dictionary-coded object keys
"""

from __future__ import annotations
import struct
import io
import json
import base64
import re
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
from enum import IntEnum

try:
    import zstandard as zstd
    HAS_ZSTD = True
except ImportError:
    HAS_ZSTD = False

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    np = None
    HAS_NUMPY = False

import gzip

# Wire format constants
MAGIC = b'SJ'
VERSION = 2

# Compression
class Compression(IntEnum):
    NONE = 0
    GZIP = 1
    ZSTD = 2

FLAG_COMPRESSED = 0x01
FLAG_HAS_COLUMN_HINTS = 0x08
COMPRESS_THRESHOLD = 256

# Security limits - aligned with Go reference implementation
MAX_DEPTH = 1000               # Maximum nesting depth
MAX_ARRAY_LEN = 100_000_000    # 100M elements
MAX_OBJECT_LEN = 10_000_000    # 10M fields
MAX_STRING_LEN = 500_000_000   # 500MB
MAX_BYTES_LEN = 1_000_000_000  # 1GB
MAX_EXT_LEN = 100_000_000      # 100MB extension payload
MAX_RANK = 32                  # Maximum tensor rank (dimensions)
MAX_HINT_COUNT = 10_000        # Maximum column hints
MAX_DECOMPRESSED_SIZE = 256 * 1024 * 1024  # 256MB
MAX_DICT_LEN = 10_000_000     # 10M dictionary entries


class SecurityLimitExceeded(ValueError):
    """Raised when a security limit is exceeded during decode."""
    pass


@dataclass
class DecodeOptions:
    """Configurable security limits for decoding.

    All fields have safe defaults matching the module-level constants.
    Pass an instance to decode() or decode_framed() to override limits.
    """
    max_depth: int = 1000
    max_array_len: int = 100_000_000
    max_object_len: int = 10_000_000
    max_string_len: int = 500_000_000
    max_bytes_len: int = 1_000_000_000
    max_ext_len: int = 100_000_000
    max_dict_len: int = 10_000_000
    max_rank: int = 32
    max_hint_count: int = 10_000
    max_decompressed_size: int = 256 * 1024 * 1024


# Type tags - aligned with Go reference implementation
class Tag(IntEnum):
    NULL = 0x00
    FALSE = 0x01
    TRUE = 0x02
    INT64 = 0x03
    FLOAT64 = 0x04
    STRING = 0x05
    ARRAY = 0x06
    OBJECT = 0x07
    BYTES = 0x08
    UINT64 = 0x09
    DECIMAL128 = 0x0A
    DATETIME64 = 0x0B
    UUID128 = 0x0C
    BIGINT = 0x0D
    EXT = 0x0E
    # ML/Multimodal extensions (0x20-0x2F)
    TENSOR = 0x20
    TENSOR_REF = 0x21
    IMAGE = 0x22
    AUDIO = 0x23
    # Graph/Delta extensions (0x30-0x3F)
    ADJLIST = 0x30
    RICHTEXT = 0x31
    DELTA = 0x32
    # Graph types (v2.1)
    NODE = 0x35
    EDGE = 0x36
    NODE_BATCH = 0x37
    EDGE_BATCH = 0x38
    GRAPH_SHARD = 0x39


# Tensor data types - aligned with Go reference implementation
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
    FLOAT64 = 0x0C
    BOOL = 0x0D
    # Quantized types
    QINT4 = 0x10    # 4-bit quantized integer
    QINT2 = 0x11    # 2-bit quantized integer
    QINT3 = 0x12    # 3-bit quantized integer
    TERNARY = 0x13  # Ternary (-1, 0, 1)
    BINARY = 0x14   # Binary (0, 1)


# Image format identifiers - aligned with Go reference implementation
class ImageFormat(IntEnum):
    JPEG = 0x01
    PNG = 0x02
    WEBP = 0x03
    AVIF = 0x04
    BMP = 0x05


# Audio encoding identifiers - aligned with Go reference implementation
class AudioEncoding(IntEnum):
    PCM_INT16 = 0x01
    PCM_FLOAT32 = 0x02
    OPUS = 0x03
    AAC = 0x04


# ID width for adjacency lists - aligned with Go reference implementation
class IDWidth(IntEnum):
    INT32 = 0x01
    INT64 = 0x02


# Delta operation codes - aligned with Go reference implementation
class DeltaOpCode(IntEnum):
    SET_FIELD = 0x01
    DELETE_FIELD = 0x02
    APPEND_ARRAY = 0x03


class Type(IntEnum):
    """Cowrie value types."""
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
    TENSOR = 13
    TENSOR_REF = 14
    IMAGE = 15
    AUDIO = 16
    ADJLIST = 17
    RICHTEXT = 18
    DELTA = 19
    # Graph types (v2.1)
    NODE = 20
    EDGE = 21
    NODE_BATCH = 22
    EDGE_BATCH = 23
    GRAPH_SHARD = 24
    UNKNOWN_EXT = 25


class UnknownExtBehavior(IntEnum):
    KEEP = 0
    SKIP_AS_NULL = 1
    ERROR = 2


@dataclass
class Decimal128:
    """128-bit decimal: value = coef * 10^(-scale)"""
    scale: int  # -127 to 127
    coef: bytes  # 16 bytes, two's complement big-endian


@dataclass
class TensorData:
    """
    Tensor data with shape, dtype, and raw bytes.

    Supports zero-copy views via NumPy when available and alignment permits.
    """
    dtype: DType
    shape: List[int]
    data: bytes  # Raw tensor data in little-endian format

    def __post_init__(self):
        """Validate tensor data."""
        expected_size = self._element_size() * self._numel()
        if len(self.data) != expected_size:
            raise ValueError(f"Data size mismatch: expected {expected_size}, got {len(self.data)}")

    def _element_size(self) -> int:
        """Get size in bytes of one element."""
        sizes = {
            DType.FLOAT32: 4,
            DType.FLOAT64: 8,
            DType.INT8: 1,
            DType.INT16: 2,
            DType.INT32: 4,
            DType.INT64: 8,
            DType.UINT8: 1,
            DType.UINT16: 2,
            DType.UINT32: 4,
            DType.UINT64: 8,
            DType.BOOL: 1,
            DType.BFLOAT16: 2,
            DType.FLOAT16: 2,
        }
        return sizes.get(self.dtype, 1)

    def _numel(self) -> int:
        """Get total number of elements.

        Checks for overflow during multiplication.
        """
        # Limit to prevent overflow attacks (max ~1 billion elements)
        MAX_NUMEL = 1_000_000_000
        result = 1
        for dim in self.shape:
            if dim < 0:
                raise ValueError(f"Invalid negative dimension: {dim}")
            # Check for potential overflow before multiplication
            if dim > 0 and result > MAX_NUMEL // dim:
                raise ValueError(f"Tensor dimensions overflow: shape={self.shape}")
            result *= dim
        return result

    @property
    def ndim(self) -> int:
        """Number of dimensions."""
        return len(self.shape)

    @property
    def size(self) -> int:
        """Total number of elements."""
        return self._numel()

    # Zero-copy view methods (return NumPy arrays backed by same memory)

    def view_float32(self) -> Optional['np.ndarray']:
        """
        Return a zero-copy NumPy view of the data as float32.

        Returns None if:
        - NumPy is not available
        - dtype is not FLOAT32
        - Data is not properly aligned
        """
        if not HAS_NUMPY:
            return None
        if self.dtype != DType.FLOAT32:
            return None
        if len(self.data) == 0:
            return np.array([], dtype=np.float32).reshape(self.shape if self.shape else (0,))
        # Check alignment (float32 requires 4-byte alignment)
        # For bytes objects, we need to use frombuffer which handles alignment
        try:
            arr = np.frombuffer(self.data, dtype='<f4')  # little-endian float32
            return arr.reshape(self.shape)
        except (ValueError, TypeError):
            return None

    def view_float64(self) -> Optional['np.ndarray']:
        """
        Return a zero-copy NumPy view of the data as float64.

        Returns None if:
        - NumPy is not available
        - dtype is not FLOAT64
        - Data is not properly aligned
        """
        if not HAS_NUMPY:
            return None
        if self.dtype != DType.FLOAT64:
            return None
        if len(self.data) == 0:
            return np.array([], dtype=np.float64).reshape(self.shape if self.shape else (0,))
        try:
            arr = np.frombuffer(self.data, dtype='<f8')  # little-endian float64
            return arr.reshape(self.shape)
        except (ValueError, TypeError):
            return None

    def view_int32(self) -> Optional['np.ndarray']:
        """Return a zero-copy NumPy view of the data as int32."""
        if not HAS_NUMPY:
            return None
        if self.dtype != DType.INT32:
            return None
        if len(self.data) == 0:
            return np.array([], dtype=np.int32).reshape(self.shape if self.shape else (0,))
        try:
            arr = np.frombuffer(self.data, dtype='<i4')
            return arr.reshape(self.shape)
        except (ValueError, TypeError):
            return None

    def view_int64(self) -> Optional['np.ndarray']:
        """Return a zero-copy NumPy view of the data as int64."""
        if not HAS_NUMPY:
            return None
        if self.dtype != DType.INT64:
            return None
        if len(self.data) == 0:
            return np.array([], dtype=np.int64).reshape(self.shape if self.shape else (0,))
        try:
            arr = np.frombuffer(self.data, dtype='<i8')
            return arr.reshape(self.shape)
        except (ValueError, TypeError):
            return None

    def view_uint8(self) -> Optional['np.ndarray']:
        """Return a zero-copy NumPy view of the data as uint8."""
        if not HAS_NUMPY:
            return None
        if self.dtype != DType.UINT8:
            return None
        if len(self.data) == 0:
            return np.array([], dtype=np.uint8).reshape(self.shape if self.shape else (0,))
        try:
            arr = np.frombuffer(self.data, dtype=np.uint8)
            return arr.reshape(self.shape)
        except (ValueError, TypeError):
            return None

    # Copy methods (always work, return new arrays)

    def copy_float32(self) -> Optional[List[float]]:
        """Copy data to a new list of float32 values."""
        if self.dtype != DType.FLOAT32:
            return None
        if len(self.data) == 0:
            return []
        count = len(self.data) // 4
        return list(struct.unpack(f'<{count}f', self.data))

    def copy_float64(self) -> Optional[List[float]]:
        """Copy data to a new list of float64 values."""
        if self.dtype != DType.FLOAT64:
            return None
        if len(self.data) == 0:
            return []
        count = len(self.data) // 8
        return list(struct.unpack(f'<{count}d', self.data))

    def copy_int32(self) -> Optional[List[int]]:
        """Copy data to a new list of int32 values."""
        if self.dtype != DType.INT32:
            return None
        if len(self.data) == 0:
            return []
        count = len(self.data) // 4
        return list(struct.unpack(f'<{count}i', self.data))

    def copy_int64(self) -> Optional[List[int]]:
        """Copy data to a new list of int64 values."""
        if self.dtype != DType.INT64:
            return None
        if len(self.data) == 0:
            return []
        count = len(self.data) // 8
        return list(struct.unpack(f'<{count}q', self.data))

    # Convenience methods that prefer view, fall back to copy

    def float32_array(self) -> Optional['np.ndarray']:
        """Get float32 data as NumPy array (zero-copy if possible, otherwise copy)."""
        view = self.view_float32()
        if view is not None:
            return view
        data = self.copy_float32()
        if data is None:
            return None
        if HAS_NUMPY:
            return np.array(data, dtype=np.float32).reshape(self.shape)
        return None

    def float64_array(self) -> Optional['np.ndarray']:
        """Get float64 data as NumPy array (zero-copy if possible, otherwise copy)."""
        view = self.view_float64()
        if view is not None:
            return view
        data = self.copy_float64()
        if data is None:
            return None
        if HAS_NUMPY:
            return np.array(data, dtype=np.float64).reshape(self.shape)
        return None

    @staticmethod
    def from_numpy(arr: 'np.ndarray') -> 'TensorData':
        """
        Create TensorData from a NumPy array.

        The array data is copied to ensure consistent little-endian byte order.
        """
        if not HAS_NUMPY:
            raise RuntimeError("NumPy is required for from_numpy()")

        # Map NumPy dtype to Cowrie DType
        dtype_map = {
            np.dtype('float32'): DType.FLOAT32,
            np.dtype('float64'): DType.FLOAT64,
            np.dtype('int8'): DType.INT8,
            np.dtype('int16'): DType.INT16,
            np.dtype('int32'): DType.INT32,
            np.dtype('int64'): DType.INT64,
            np.dtype('uint8'): DType.UINT8,
            np.dtype('uint16'): DType.UINT16,
            np.dtype('uint32'): DType.UINT32,
            np.dtype('uint64'): DType.UINT64,
            np.dtype('bool'): DType.BOOL,
        }

        if arr.dtype not in dtype_map:
            raise ValueError(f"Unsupported NumPy dtype: {arr.dtype}")

        dtype = dtype_map[arr.dtype]
        shape = list(arr.shape)

        # Ensure little-endian and contiguous
        arr_le = arr.astype(arr.dtype.newbyteorder('<'), copy=False)
        if not arr_le.flags['C_CONTIGUOUS']:
            arr_le = np.ascontiguousarray(arr_le)

        return TensorData(dtype=dtype, shape=shape, data=arr_le.tobytes())


@dataclass
class ImageData:
    """
    Image data with format and dimensions.

    Wire format: Tag(0x22) | format:u8 | width:u16 LE | height:u16 LE | dataLen:varint | data
    """
    format: ImageFormat
    width: int
    height: int
    data: bytes

    def __post_init__(self):
        """Validate image data."""
        if self.width < 0 or self.width > 65535:
            raise ValueError(f"Invalid image width: {self.width}")
        if self.height < 0 or self.height > 65535:
            raise ValueError(f"Invalid image height: {self.height}")

    @property
    def size(self) -> tuple[int, int]:
        """Return (width, height) tuple."""
        return (self.width, self.height)

    @property
    def format_name(self) -> str:
        """Return format name as lowercase string."""
        return self.format.name.lower()


@dataclass
class AudioData:
    """
    Audio data with encoding, sample rate, and channel info.

    Wire format: Tag(0x23) | encoding:u8 | sampleRate:u32 LE | channels:u8 | dataLen:varint | data
    """
    encoding: AudioEncoding
    sample_rate: int
    channels: int
    data: bytes

    def __post_init__(self):
        """Validate audio data."""
        if self.sample_rate < 0:
            raise ValueError(f"Invalid sample rate: {self.sample_rate}")
        if self.channels < 1 or self.channels > 255:
            raise ValueError(f"Invalid channel count: {self.channels}")

    @property
    def encoding_name(self) -> str:
        """Return encoding name as lowercase string."""
        return self.encoding.name.lower()

    @property
    def duration_seconds(self) -> Optional[float]:
        """
        Estimate duration for PCM formats.

        Returns None for compressed formats.
        """
        if self.encoding == AudioEncoding.PCM_INT16:
            # 2 bytes per sample per channel
            bytes_per_frame = 2 * self.channels
            if bytes_per_frame > 0 and self.sample_rate > 0:
                num_frames = len(self.data) // bytes_per_frame
                return num_frames / self.sample_rate
        elif self.encoding == AudioEncoding.PCM_FLOAT32:
            # 4 bytes per sample per channel
            bytes_per_frame = 4 * self.channels
            if bytes_per_frame > 0 and self.sample_rate > 0:
                num_frames = len(self.data) // bytes_per_frame
                return num_frames / self.sample_rate
        return None


@dataclass
class TensorRefData:
    """
    Reference to an externally stored tensor.

    Wire format: Tag(0x21) | storeID:u8 | keyLen:varint | key
    """
    store_id: int  # Which store/shard (0-255)
    key: bytes     # Lookup key (UUID, hash, etc.)

    def __post_init__(self):
        if self.store_id < 0 or self.store_id > 255:
            raise ValueError(f"Invalid store_id: {self.store_id}")


@dataclass
class AdjlistData:
    """
    CSR (Compressed Sparse Row) adjacency list for graphs.

    Wire format: Tag(0x30) | id_width:u8 | node_count:varint | edge_count:varint |
                 row_offsets:[(node_count+1) x varint] | col_indices:[edge_count x (4 or 8 bytes)]
    """
    id_width: IDWidth    # 1=int32, 2=int64
    node_count: int
    edge_count: int
    row_offsets: List[int]  # [node_count + 1] offsets into col_indices
    col_indices: bytes      # Edge destinations (int32/int64 LE based on id_width)

    @property
    def id_size(self) -> int:
        """Return byte size of node IDs."""
        return 4 if self.id_width == IDWidth.INT32 else 8


@dataclass
class RichTextSpan:
    """Annotated span within rich text."""
    start: int   # Byte offset start
    end: int     # Byte offset end
    kind_id: int # Application-defined kind


@dataclass
class RichTextData:
    """
    Text with optional tokens and spans for NLP/annotation.

    Wire format: Tag(0x31) | textLen:varint | text |
                 tokenCount:varint | tokens:[int32 LE...] |
                 spanCount:varint | spans:[(start:varint, end:varint, kind:varint)...]
    """
    text: str
    tokens: Optional[List[int]] = None  # Token IDs (e.g., BPE tokens)
    spans: Optional[List[RichTextSpan]] = None  # Annotated spans


@dataclass
class DeltaOp:
    """Single delta operation."""
    op_code: DeltaOpCode
    field_id: int  # Dictionary-coded field ID
    value: Optional['Value'] = None  # For SET_FIELD and APPEND_ARRAY


@dataclass
class DeltaData:
    """
    Semantic diff/patch representing changes to an object.

    Wire format: Tag(0x32) | baseID:varint | opCount:varint |
                 ops:[(opCode:u8, fieldID:varint, [value])...]
    """
    base_id: int       # Reference to base object
    ops: List[DeltaOp] # Operations


# ============================================================
# Graph Types (v2.1)
# ============================================================

@dataclass
class NodeData:
    """
    Graph node with ID, labels, and properties.
    Properties use dictionary-coded keys for efficient encoding.

    Wire format: Tag(0x35) | id:string | labelCount:varint | labels:string* |
                 propCount:varint | (dictIdx:varint + value)*
    """
    id: str
    labels: List[str]
    props: Dict[str, Any]  # Keys are dictionary-coded


@dataclass
class EdgeData:
    """
    Graph edge with source, destination, type, and properties.
    Properties use dictionary-coded keys for efficient encoding.

    Wire format: Tag(0x36) | srcId:string | dstId:string | type:string |
                 propCount:varint | (dictIdx:varint + value)*
    """
    from_id: str
    to_id: str
    edge_type: str
    props: Dict[str, Any]  # Keys are dictionary-coded


@dataclass
class NodeBatchData:
    """
    Batch of nodes for streaming/bulk operations.

    Wire format: Tag(0x37) | count:varint | Node[count]
    """
    nodes: List[NodeData]


@dataclass
class EdgeBatchData:
    """
    Batch of edges for streaming/bulk operations.

    Wire format: Tag(0x38) | count:varint | Edge[count]
    """
    edges: List[EdgeData]


@dataclass
class GraphShardData:
    """
    Self-contained subgraph with nodes, edges, and metadata.
    Useful for distributed graph processing and checkpointing.

    Wire format: Tag(0x39) | nodeCount:varint | Node* |
                 edgeCount:varint | Edge* |
                 metaCount:varint | (dictIdx:varint + value)*
    """
    nodes: List[NodeData]
    edges: List[EdgeData]
    metadata: Dict[str, Any]  # Keys are dictionary-coded


@dataclass
class UnknownExtData:
    """Unknown extension payload preserved for round-trip."""
    ext_type: int
    payload: bytes


@dataclass
class Value:
    """Cowrie value container."""
    type: Type
    data: Any  # The actual value based on type

    @staticmethod
    def null() -> Value:
        return Value(Type.NULL, None)

    @staticmethod
    def bool_(b: bool) -> Value:
        return Value(Type.BOOL, b)

    @staticmethod
    def int64(i: int) -> Value:
        return Value(Type.INT64, i)

    @staticmethod
    def uint64(u: int) -> Value:
        if u < 0 or u > 0xFFFFFFFFFFFFFFFF:
            raise ValueError(f"UINT64 value out of range: {u} (must be 0..2^64-1)")
        return Value(Type.UINT64, u)

    @staticmethod
    def float64(f: float) -> Value:
        return Value(Type.FLOAT64, f)

    @staticmethod
    def decimal128(scale: int, coef: bytes) -> Value:
        return Value(Type.DECIMAL128, Decimal128(scale, coef))

    @staticmethod
    def string(s: str) -> Value:
        return Value(Type.STRING, s)

    @staticmethod
    def bytes_(b: bytes) -> Value:
        return Value(Type.BYTES, b)

    @staticmethod
    def datetime64(nanos: int) -> Value:
        """Create from nanoseconds since Unix epoch."""
        return Value(Type.DATETIME64, nanos)

    @staticmethod
    def datetime(dt: datetime) -> Value:
        """Create from Python datetime."""
        ts = dt.timestamp()
        nanos = int(ts * 1_000_000_000)
        return Value(Type.DATETIME64, nanos)

    @staticmethod
    def uuid128(u: UUID) -> Value:
        return Value(Type.UUID128, u)

    @staticmethod
    def bigint(b: bytes) -> Value:
        """Create from two's complement big-endian bytes."""
        return Value(Type.BIGINT, b)

    @staticmethod
    def array(items: List[Value]) -> Value:
        return Value(Type.ARRAY, items)

    @staticmethod
    def object(members: Dict[str, Value]) -> Value:
        return Value(Type.OBJECT, members)

    @staticmethod
    def tensor(dtype: DType, shape: List[int], data: bytes) -> Value:
        """Create a tensor value."""
        return Value(Type.TENSOR, TensorData(dtype=dtype, shape=shape, data=data))

    @staticmethod
    def tensor_from_numpy(arr: 'np.ndarray') -> Value:
        """Create a tensor value from a NumPy array."""
        return Value(Type.TENSOR, TensorData.from_numpy(arr))

    @staticmethod
    def image(format: ImageFormat, width: int, height: int, data: bytes) -> Value:
        """Create an image value."""
        return Value(Type.IMAGE, ImageData(format=format, width=width, height=height, data=data))

    @staticmethod
    def audio(encoding: AudioEncoding, sample_rate: int, channels: int, data: bytes) -> Value:
        """Create an audio value."""
        return Value(Type.AUDIO, AudioData(encoding=encoding, sample_rate=sample_rate, channels=channels, data=data))

    @staticmethod
    def tensor_ref(store_id: int, key: bytes) -> Value:
        """Create a tensor reference value (reference to externally stored tensor)."""
        return Value(Type.TENSOR_REF, TensorRefData(store_id=store_id, key=key))

    @staticmethod
    def adjlist(id_width: IDWidth, node_count: int, edge_count: int,
                row_offsets: List[int], col_indices: bytes) -> Value:
        """Create an adjacency list value for graph data (CSR format)."""
        return Value(Type.ADJLIST, AdjlistData(
            id_width=id_width,
            node_count=node_count,
            edge_count=edge_count,
            row_offsets=row_offsets,
            col_indices=col_indices
        ))

    @staticmethod
    def richtext(text: str, tokens: Optional[List[int]] = None,
                 spans: Optional[List[RichTextSpan]] = None) -> Value:
        """Create a rich text value with optional tokens and spans."""
        return Value(Type.RICHTEXT, RichTextData(text=text, tokens=tokens, spans=spans))

    @staticmethod
    def delta(base_id: int, ops: List[DeltaOp]) -> Value:
        """Create a delta value representing changes to an object."""
        return Value(Type.DELTA, DeltaData(base_id=base_id, ops=ops))

    @staticmethod
    def node(id: str, labels: List[str], props: Dict[str, 'Value']) -> 'Value':
        """Create a graph node value."""
        return Value(Type.NODE, NodeData(id=id, labels=labels, props=props))

    @staticmethod
    def edge(from_id: str, to_id: str, edge_type: str, props: Dict[str, 'Value']) -> 'Value':
        """Create a graph edge value."""
        return Value(Type.EDGE, EdgeData(from_id=from_id, to_id=to_id, edge_type=edge_type, props=props))

    @staticmethod
    def node_batch(nodes: List[NodeData]) -> 'Value':
        """Create a batch of nodes."""
        return Value(Type.NODE_BATCH, NodeBatchData(nodes=nodes))

    @staticmethod
    def edge_batch(edges: List[EdgeData]) -> 'Value':
        """Create a batch of edges."""
        return Value(Type.EDGE_BATCH, EdgeBatchData(edges=edges))

    @staticmethod
    def graph_shard(nodes: List[NodeData], edges: List[EdgeData], metadata: Dict[str, 'Value']) -> 'Value':
        """Create a graph shard containing nodes, edges, and metadata."""
        return Value(Type.GRAPH_SHARD, GraphShardData(nodes=nodes, edges=edges, metadata=metadata))

    @staticmethod
    def unknown_ext(ext_type: int, payload: bytes) -> 'Value':
        """Create an unknown extension value (TagExt)."""
        return Value(Type.UNKNOWN_EXT, UnknownExtData(ext_type=ext_type, payload=payload))

    def get(self, key: str) -> Optional[Value]:
        """Get value by key for objects."""
        if self.type != Type.OBJECT:
            raise TypeError("Not an object")
        return self.data.get(key)

    def __getitem__(self, key: Union[str, int]) -> Value:
        if self.type == Type.OBJECT:
            return self.data[key]
        elif self.type == Type.ARRAY:
            return self.data[key]
        raise TypeError(f"Cannot index {self.type}")

    def __len__(self) -> int:
        if self.type == Type.ARRAY:
            return len(self.data)
        elif self.type == Type.OBJECT:
            return len(self.data)
        raise TypeError(f"Cannot get length of {self.type}")


# ============================================================
# Varint encoding/decoding
# ============================================================

def encode_uvarint(n: int) -> bytes:
    """Encode unsigned integer as varint."""
    if n < 0 or n > 0xFFFFFFFFFFFFFFFF:
        raise ValueError(f"uvarint value out of range: {n} (must be 0..2^64-1)")
    result = []
    while n >= 0x80:
        result.append((n & 0x7F) | 0x80)
        n >>= 7
    result.append(n)
    return bytes(result)


def decode_uvarint(data: bytes, pos: int) -> tuple[int, int]:
    """Decode varint, return (value, new_pos).

    Hot path: ~99% of varints are single-byte (0-127), so we inline that case.
    """
    if pos >= len(data):
        raise ValueError("Incomplete varint")

    # Fast path: single-byte varint (most common case)
    b = data[pos]
    if b < 0x80:
        return b, pos + 1

    # Multi-byte varint
    result = b & 0x7F
    shift = 7
    pos += 1
    while pos < len(data):
        b = data[pos]
        result |= (b & 0x7F) << shift
        pos += 1
        if (b & 0x80) == 0:
            return result, pos
        shift += 7
    raise ValueError("Incomplete varint")


def zigzag_encode(n: int) -> int:
    """Encode signed int using zigzag encoding."""
    return (n << 1) ^ (n >> 63)


def zigzag_decode(n: int) -> int:
    """Decode zigzag-encoded value."""
    return (n >> 1) ^ -(n & 1)


# ============================================================
# Encoder
# ============================================================

class Encoder:
    def __init__(self):
        self.buf = io.BytesIO()
        self.dict_keys: List[str] = []
        self.dict_lookup: Dict[str, int] = {}

    def _add_key(self, key: str) -> int:
        if key in self.dict_lookup:
            return self.dict_lookup[key]
        idx = len(self.dict_keys)
        self.dict_keys.append(key)
        self.dict_lookup[key] = idx
        return idx

    def _collect_keys(self, v: Value):
        """Recursively collect all object keys."""
        if v.type == Type.ARRAY:
            for item in v.data:
                self._collect_keys(item)
        elif v.type == Type.OBJECT:
            for key, val in v.data.items():
                self._add_key(key)
                self._collect_keys(val)
        elif v.type == Type.DELTA:
            # Delta ops may contain nested values
            for op in v.data.ops:
                if op.value is not None:
                    self._collect_keys(op.value)
        # Graph types - collect property keys
        elif v.type == Type.NODE:
            self._collect_props_keys(v.data.props)
        elif v.type == Type.EDGE:
            self._collect_props_keys(v.data.props)
        elif v.type == Type.NODE_BATCH:
            for node in v.data.nodes:
                self._collect_props_keys(node.props)
        elif v.type == Type.EDGE_BATCH:
            for edge in v.data.edges:
                self._collect_props_keys(edge.props)
        elif v.type == Type.GRAPH_SHARD:
            for node in v.data.nodes:
                self._collect_props_keys(node.props)
            for edge in v.data.edges:
                self._collect_props_keys(edge.props)
            self._collect_props_keys(v.data.metadata)

    def _collect_props_keys(self, props: Dict[str, Value]):
        """Collect keys from a properties dict."""
        for key, val in props.items():
            self._add_key(key)
            self._collect_keys(val)

    def _write(self, data: bytes):
        self.buf.write(data)

    def _write_byte(self, b: int):
        self.buf.write(bytes([b]))

    def _write_uvarint(self, n: int):
        self._write(encode_uvarint(n))

    def _write_string(self, s: str):
        b = s.encode('utf-8')
        self._write_uvarint(len(b))
        self._write(b)

    def _encode_value(self, v: Value):
        if v is None or v.type == Type.NULL:
            self._write_byte(Tag.NULL)
        elif v.type == Type.BOOL:
            self._write_byte(Tag.TRUE if v.data else Tag.FALSE)
        elif v.type == Type.INT64:
            self._write_byte(Tag.INT64)
            self._write_uvarint(zigzag_encode(v.data))
        elif v.type == Type.UINT64:
            if v.data < 0 or v.data > 0xFFFFFFFFFFFFFFFF:
                raise ValueError(f"UINT64 value out of range: {v.data} (must be 0..2^64-1)")
            self._write_byte(Tag.UINT64)
            self._write_uvarint(v.data)
        elif v.type == Type.FLOAT64:
            self._write_byte(Tag.FLOAT64)
            self._write(struct.pack('<d', v.data))
        elif v.type == Type.DECIMAL128:
            self._write_byte(Tag.DECIMAL128)
            self._write_byte(v.data.scale & 0xFF)
            self._write(v.data.coef)
        elif v.type == Type.STRING:
            self._write_byte(Tag.STRING)
            self._write_string(v.data)
        elif v.type == Type.BYTES:
            self._write_byte(Tag.BYTES)
            self._write_uvarint(len(v.data))
            self._write(v.data)
        elif v.type == Type.DATETIME64:
            self._write_byte(Tag.DATETIME64)
            self._write(struct.pack('<q', v.data))
        elif v.type == Type.UUID128:
            self._write_byte(Tag.UUID128)
            self._write(v.data.bytes)
        elif v.type == Type.BIGINT:
            self._write_byte(Tag.BIGINT)
            self._write_uvarint(len(v.data))
            self._write(v.data)
        elif v.type == Type.UNKNOWN_EXT:
            ext = v.data  # UnknownExtData
            self._write_byte(Tag.EXT)
            self._write_uvarint(ext.ext_type)
            self._write_uvarint(len(ext.payload))
            self._write(ext.payload)
        elif v.type == Type.ARRAY:
            self._write_byte(Tag.ARRAY)
            self._write_uvarint(len(v.data))
            for item in v.data:
                self._encode_value(item)
        elif v.type == Type.OBJECT:
            self._write_byte(Tag.OBJECT)
            self._write_uvarint(len(v.data))
            for key, val in v.data.items():
                idx = self.dict_lookup[key]
                self._write_uvarint(idx)
                self._encode_value(val)
        elif v.type == Type.TENSOR:
            t = v.data  # TensorData
            rank = len(t.shape)
            if rank > MAX_RANK or rank > 255:
                raise SecurityLimitExceeded(f"Tensor rank too large: {rank} > {MAX_RANK}")
            self._write_byte(Tag.TENSOR)
            self._write_byte(t.dtype)
            self._write_byte(rank)
            for dim in t.shape:
                self._write_uvarint(dim)
            self._write_uvarint(len(t.data))
            self._write(t.data)
        elif v.type == Type.IMAGE:
            img = v.data  # ImageData
            self._write_byte(Tag.IMAGE)
            self._write_byte(img.format)
            self._write(struct.pack('<H', img.width))
            self._write(struct.pack('<H', img.height))
            self._write_uvarint(len(img.data))
            self._write(img.data)
        elif v.type == Type.AUDIO:
            aud = v.data  # AudioData
            self._write_byte(Tag.AUDIO)
            self._write_byte(aud.encoding)
            self._write(struct.pack('<I', aud.sample_rate))
            self._write_byte(aud.channels)
            self._write_uvarint(len(aud.data))
            self._write(aud.data)
        elif v.type == Type.TENSOR_REF:
            ref = v.data  # TensorRefData
            self._write_byte(Tag.TENSOR_REF)
            self._write_byte(ref.store_id)
            self._write_uvarint(len(ref.key))
            self._write(ref.key)
        elif v.type == Type.ADJLIST:
            adj = v.data  # AdjlistData
            self._write_byte(Tag.ADJLIST)
            self._write_byte(adj.id_width)
            self._write_uvarint(adj.node_count)
            self._write_uvarint(adj.edge_count)
            # Write row_offsets as varints
            for offset in adj.row_offsets:
                self._write_uvarint(offset)
            # Write col_indices as raw bytes
            self._write(adj.col_indices)
        elif v.type == Type.RICHTEXT:
            rt = v.data  # RichTextData
            self._write_byte(Tag.RICHTEXT)
            # Text (writeString format: len:varint + bytes)
            text_bytes = rt.text.encode('utf-8')
            self._write_uvarint(len(text_bytes))
            self._write(text_bytes)
            # Calculate and write flags byte
            tokens = rt.tokens or []
            spans = rt.spans or []
            flags = 0
            if len(tokens) > 0:
                flags |= 0x01
            if len(spans) > 0:
                flags |= 0x02
            self._write_byte(flags)
            # Write tokens if present
            if flags & 0x01:
                self._write_uvarint(len(tokens))
                for tok in tokens:
                    self._write(struct.pack('<i', tok))
            # Write spans if present
            if flags & 0x02:
                self._write_uvarint(len(spans))
                for span in spans:
                    self._write_uvarint(span.start)
                    self._write_uvarint(span.end)
                    self._write_uvarint(span.kind_id)
        elif v.type == Type.DELTA:
            delta = v.data  # DeltaData
            self._write_byte(Tag.DELTA)
            self._write_uvarint(delta.base_id)
            self._write_uvarint(len(delta.ops))
            for op in delta.ops:
                self._write_byte(op.op_code)
                self._write_uvarint(op.field_id)
                if op.op_code in (DeltaOpCode.SET_FIELD, DeltaOpCode.APPEND_ARRAY):
                    if op.value is not None:
                        self._encode_value(op.value)
        # Graph types
        elif v.type == Type.NODE:
            node = v.data  # NodeData
            self._write_byte(Tag.NODE)
            self._encode_node(node)
        elif v.type == Type.EDGE:
            edge = v.data  # EdgeData
            self._write_byte(Tag.EDGE)
            self._encode_edge(edge)
        elif v.type == Type.NODE_BATCH:
            batch = v.data  # NodeBatchData
            self._write_byte(Tag.NODE_BATCH)
            self._write_uvarint(len(batch.nodes))
            for node in batch.nodes:
                self._encode_node(node)
        elif v.type == Type.EDGE_BATCH:
            batch = v.data  # EdgeBatchData
            self._write_byte(Tag.EDGE_BATCH)
            self._write_uvarint(len(batch.edges))
            for edge in batch.edges:
                self._encode_edge(edge)
        elif v.type == Type.GRAPH_SHARD:
            shard = v.data  # GraphShardData
            self._write_byte(Tag.GRAPH_SHARD)
            # Encode nodes
            self._write_uvarint(len(shard.nodes))
            for node in shard.nodes:
                self._encode_node(node)
            # Encode edges
            self._write_uvarint(len(shard.edges))
            for edge in shard.edges:
                self._encode_edge(edge)
            # Encode metadata
            self._encode_props(shard.metadata)

    def _encode_node(self, node: NodeData):
        """Encode a node without tag byte."""
        self._write_string(node.id)
        self._write_uvarint(len(node.labels))
        for label in node.labels:
            self._write_string(label)
        self._encode_props(node.props)

    def _encode_edge(self, edge: EdgeData):
        """Encode an edge without tag byte."""
        self._write_string(edge.from_id)
        self._write_string(edge.to_id)
        self._write_string(edge.edge_type)
        self._encode_props(edge.props)

    def _encode_props(self, props: Dict[str, Value]):
        """Encode dictionary-coded properties."""
        self._write_uvarint(len(props))
        for key, val in props.items():
            idx = self.dict_lookup[key]
            self._write_uvarint(idx)
            self._encode_value(val)

    def encode(self, v: Value) -> bytes:
        # Collect all keys
        self._collect_keys(v)

        # Write header
        self._write(MAGIC)
        self._write_byte(VERSION)
        self._write_byte(0)  # flags

        # Write dictionary
        self._write_uvarint(len(self.dict_keys))
        for key in self.dict_keys:
            self._write_string(key)

        # Write root value
        self._encode_value(v)

        return self.buf.getvalue()


def encode(v: Value) -> bytes:
    """Encode a value to Cowrie v2 binary format."""
    return Encoder().encode(v)


# ============================================================
# Decoder
# ============================================================

class Decoder:
    def __init__(self, data: bytes, on_unknown_ext: UnknownExtBehavior = UnknownExtBehavior.KEEP,
                 opts: Optional[DecodeOptions] = None):
        self.data = data
        self.pos = 0
        self.dict: List[str] = []
        self.depth = 0
        self.on_unknown_ext = on_unknown_ext
        self.opts = opts if opts is not None else DecodeOptions()

    def _read(self, n: int) -> bytes:
        if self.pos + n > len(self.data):
            raise ValueError("Unexpected end of data")
        result = self.data[self.pos:self.pos + n]
        self.pos += n
        return result

    def _read_byte(self) -> int:
        return self._read(1)[0]

    def _read_uvarint(self) -> int:
        val, new_pos = decode_uvarint(self.data, self.pos)
        self.pos = new_pos
        return val

    def _read_string(self) -> str:
        length = self._read_uvarint()
        if length > self.opts.max_string_len:
            raise SecurityLimitExceeded(f"String too long: {length} > {self.opts.max_string_len}")
        return self._read(length).decode('utf-8')

    def _enter_nested(self) -> None:
        self.depth += 1
        if self.depth > self.opts.max_depth:
            raise SecurityLimitExceeded(f"Maximum nesting depth exceeded: {self.opts.max_depth}")

    def _exit_nested(self) -> None:
        self.depth -= 1

    def _decode_value(self) -> Value:
        tag = self._read_byte()

        if tag == Tag.NULL:
            return Value.null()
        elif tag == Tag.FALSE:
            return Value.bool_(False)
        elif tag == Tag.TRUE:
            return Value.bool_(True)
        elif tag == Tag.INT64:
            return Value.int64(zigzag_decode(self._read_uvarint()))
        elif tag == Tag.UINT64:
            return Value.uint64(self._read_uvarint())
        elif tag == Tag.FLOAT64:
            return Value.float64(struct.unpack('<d', self._read(8))[0])
        elif tag == Tag.DECIMAL128:
            scale = self._read_byte()
            if scale > 127:
                scale -= 256  # Convert to signed
            coef = self._read(16)
            return Value.decimal128(scale, coef)
        elif tag == Tag.STRING:
            return Value.string(self._read_string())
        elif tag == Tag.BYTES:
            length = self._read_uvarint()
            if length > self.opts.max_bytes_len:
                raise SecurityLimitExceeded(f"Bytes too long: {length} > {self.opts.max_bytes_len}")
            return Value.bytes_(self._read(length))
        elif tag == Tag.DATETIME64:
            nanos = struct.unpack('<q', self._read(8))[0]
            return Value.datetime64(nanos)
        elif tag == Tag.UUID128:
            return Value.uuid128(UUID(bytes=self._read(16)))
        elif tag == Tag.BIGINT:
            length = self._read_uvarint()
            return Value.bigint(self._read(length))
        elif tag == Tag.EXT:
            ext_type = self._read_uvarint()
            length = self._read_uvarint()
            if length > self.opts.max_ext_len:
                raise SecurityLimitExceeded(f"Extension payload too large: {length} > {self.opts.max_ext_len}")
            payload = self._read(length)
            if self.on_unknown_ext == UnknownExtBehavior.ERROR:
                raise ValueError("Unknown extension type")
            if self.on_unknown_ext == UnknownExtBehavior.SKIP_AS_NULL:
                return Value.null()
            return Value.unknown_ext(ext_type, payload)
        elif tag == Tag.ARRAY:
            count = self._read_uvarint()
            if count > self.opts.max_array_len:
                raise SecurityLimitExceeded(f"Array too large: {count} > {self.opts.max_array_len}")
            self._enter_nested()
            items = [self._decode_value() for _ in range(count)]
            self._exit_nested()
            return Value.array(items)
        elif tag == Tag.OBJECT:
            count = self._read_uvarint()
            if count > self.opts.max_object_len:
                raise SecurityLimitExceeded(f"Object too large: {count} > {self.opts.max_object_len}")
            self._enter_nested()
            members = {}
            for _ in range(count):
                field_id = self._read_uvarint()
                if field_id >= len(self.dict):
                    raise ValueError(f"Invalid dictionary index: {field_id} >= {len(self.dict)}")
                key = self.dict[field_id]
                val = self._decode_value()
                members[key] = val
            self._exit_nested()
            return Value.object(members)
        elif tag == Tag.TENSOR:
            dtype = DType(self._read_byte())
            rank = self._read_byte()
            if rank > self.opts.max_rank:
                raise SecurityLimitExceeded(f"Tensor rank too large: {rank} > {self.opts.max_rank}")
            shape = [self._read_uvarint() for _ in range(rank)]
            data_len = self._read_uvarint()
            if data_len > self.opts.max_bytes_len:
                raise SecurityLimitExceeded(f"Tensor data too large: {data_len} > {self.opts.max_bytes_len}")
            data = self._read(data_len)
            return Value.tensor(dtype, shape, data)
        elif tag == Tag.IMAGE:
            format_byte = self._read_byte()
            width = struct.unpack('<H', self._read(2))[0]
            height = struct.unpack('<H', self._read(2))[0]
            data_len = self._read_uvarint()
            if data_len > self.opts.max_bytes_len:
                raise SecurityLimitExceeded(f"Image data too large: {data_len} > {self.opts.max_bytes_len}")
            data = self._read(data_len)
            return Value.image(ImageFormat(format_byte), width, height, data)
        elif tag == Tag.AUDIO:
            encoding_byte = self._read_byte()
            sample_rate = struct.unpack('<I', self._read(4))[0]
            channels = self._read_byte()
            data_len = self._read_uvarint()
            if data_len > self.opts.max_bytes_len:
                raise SecurityLimitExceeded(f"Audio data too large: {data_len} > {self.opts.max_bytes_len}")
            data = self._read(data_len)
            return Value.audio(AudioEncoding(encoding_byte), sample_rate, channels, data)
        elif tag == Tag.TENSOR_REF:
            store_id = self._read_byte()
            key_len = self._read_uvarint()
            if key_len > self.opts.max_string_len:
                raise SecurityLimitExceeded(f"TensorRef key too long: {key_len} > {self.opts.max_string_len}")
            key = self._read(key_len)
            return Value.tensor_ref(store_id, key)
        elif tag == Tag.ADJLIST:
            id_width = IDWidth(self._read_byte())
            node_count = self._read_uvarint()
            if node_count > self.opts.max_array_len:
                raise SecurityLimitExceeded(f"Adjlist node count too large: {node_count} > {self.opts.max_array_len}")
            edge_count = self._read_uvarint()
            if edge_count > self.opts.max_array_len:
                raise SecurityLimitExceeded(f"Adjlist edge count too large: {edge_count} > {self.opts.max_array_len}")
            # Read row_offsets (node_count + 1 varints)
            row_offsets = [self._read_uvarint() for _ in range(node_count + 1)]
            # Read col_indices (edge_count * id_size bytes)
            id_size = 4 if id_width == IDWidth.INT32 else 8
            col_indices = self._read(edge_count * id_size)
            return Value.adjlist(id_width, node_count, edge_count, row_offsets, col_indices)
        elif tag == Tag.RICHTEXT:
            # Text (readString format: len:varint + bytes)
            text_len = self._read_uvarint()
            if text_len > self.opts.max_string_len:
                raise SecurityLimitExceeded(f"RichText text too long: {text_len} > {self.opts.max_string_len}")
            text = self._read(text_len).decode('utf-8')
            # Read flags byte
            flags = self._read_byte()
            # Read tokens if present (flags & 0x01)
            tokens = None
            if flags & 0x01:
                token_count = self._read_uvarint()
                if token_count > self.opts.max_array_len:
                    raise SecurityLimitExceeded(f"RichText token count too large: {token_count} > {self.opts.max_array_len}")
                tokens = [struct.unpack('<i', self._read(4))[0] for _ in range(token_count)]
            # Read spans if present (flags & 0x02)
            spans = None
            if flags & 0x02:
                span_count = self._read_uvarint()
                if span_count > self.opts.max_array_len:
                    raise SecurityLimitExceeded(f"RichText span count too large: {span_count} > {self.opts.max_array_len}")
                spans = []
                for _ in range(span_count):
                    start = self._read_uvarint()
                    end = self._read_uvarint()
                    kind_id = self._read_uvarint()
                    spans.append(RichTextSpan(start=start, end=end, kind_id=kind_id))
            return Value.richtext(text, tokens, spans)
        elif tag == Tag.DELTA:
            base_id = self._read_uvarint()
            op_count = self._read_uvarint()
            if op_count > self.opts.max_array_len:
                raise SecurityLimitExceeded(f"Delta op count too large: {op_count} > {self.opts.max_array_len}")
            ops = []
            for _ in range(op_count):
                op_code = DeltaOpCode(self._read_byte())
                field_id = self._read_uvarint()
                value = None
                if op_code in (DeltaOpCode.SET_FIELD, DeltaOpCode.APPEND_ARRAY):
                    value = self._decode_value()
                ops.append(DeltaOp(op_code=op_code, field_id=field_id, value=value))
            return Value.delta(base_id, ops)
        # Graph types
        elif tag == Tag.NODE:
            node = self._decode_node()
            return Value.node(node.id, node.labels, node.props)
        elif tag == Tag.EDGE:
            edge = self._decode_edge()
            return Value.edge(edge.from_id, edge.to_id, edge.edge_type, edge.props)
        elif tag == Tag.NODE_BATCH:
            count = self._read_uvarint()
            if count > self.opts.max_array_len:
                raise SecurityLimitExceeded(f"Node batch count too large: {count} > {self.opts.max_array_len}")
            nodes = [self._decode_node() for _ in range(count)]
            return Value.node_batch(nodes)
        elif tag == Tag.EDGE_BATCH:
            count = self._read_uvarint()
            if count > self.opts.max_array_len:
                raise SecurityLimitExceeded(f"Edge batch count too large: {count} > {self.opts.max_array_len}")
            edges = [self._decode_edge() for _ in range(count)]
            return Value.edge_batch(edges)
        elif tag == Tag.GRAPH_SHARD:
            # Decode nodes
            node_count = self._read_uvarint()
            if node_count > self.opts.max_array_len:
                raise SecurityLimitExceeded(f"Graph shard node count too large: {node_count} > {self.opts.max_array_len}")
            nodes = [self._decode_node() for _ in range(node_count)]
            # Decode edges
            edge_count = self._read_uvarint()
            if edge_count > self.opts.max_array_len:
                raise SecurityLimitExceeded(f"Graph shard edge count too large: {edge_count} > {self.opts.max_array_len}")
            edges = [self._decode_edge() for _ in range(edge_count)]
            # Decode metadata
            metadata = self._decode_props()
            return Value.graph_shard(nodes, edges, metadata)
        else:
            raise ValueError(f"Invalid tag: {tag}")

    def _decode_node(self) -> NodeData:
        """Decode a node without tag byte."""
        id = self._read_string()
        label_count = self._read_uvarint()
        if label_count > self.opts.max_array_len:
            raise SecurityLimitExceeded(f"Node label count too large: {label_count} > {self.opts.max_array_len}")
        labels = [self._read_string() for _ in range(label_count)]
        props = self._decode_props()
        return NodeData(id=id, labels=labels, props=props)

    def _decode_edge(self) -> EdgeData:
        """Decode an edge without tag byte."""
        from_id = self._read_string()
        to_id = self._read_string()
        edge_type = self._read_string()
        props = self._decode_props()
        return EdgeData(from_id=from_id, to_id=to_id, edge_type=edge_type, props=props)

    def _decode_props(self) -> Dict[str, Value]:
        """Decode dictionary-coded properties."""
        prop_count = self._read_uvarint()
        if prop_count > self.opts.max_object_len:
            raise SecurityLimitExceeded(f"Props count too large: {prop_count} > {self.opts.max_object_len}")
        props = {}
        for _ in range(prop_count):
            field_id = self._read_uvarint()
            if field_id >= len(self.dict):
                raise ValueError(f"Invalid dictionary index: {field_id} >= {len(self.dict)}")
            key = self.dict[field_id]
            val = self._decode_value()
            props[key] = val
        return props

    def _skip_hints(self) -> None:
        count = self._read_uvarint()
        if count > self.opts.max_hint_count:
            raise SecurityLimitExceeded(f"Too many hints: {count} > {self.opts.max_hint_count}")
        for _ in range(count):
            _ = self._read_string()  # field name
            _ = self._read_byte()    # type
            shape_len = self._read_uvarint()
            if shape_len > self.opts.max_rank:
                raise SecurityLimitExceeded(f"Hint shape too large: {shape_len} > {self.opts.max_rank}")
            for _ in range(shape_len):
                _ = self._read_uvarint()
            _ = self._read_byte()    # flags

    def decode(self) -> Value:
        # Read header
        magic = self._read(2)
        if magic != MAGIC:
            raise ValueError("Invalid magic bytes")

        version = self._read_byte()
        if version != VERSION:
            raise ValueError(f"Unsupported version: {version}")

        flags = self._read_byte()
        # For now, ignore compression flag (use decode_framed for compressed)
        if flags & FLAG_HAS_COLUMN_HINTS:
            self._skip_hints()

        # Read dictionary
        dict_len = self._read_uvarint()
        if dict_len > self.opts.max_dict_len:
            raise SecurityLimitExceeded(f"Dictionary too large: {dict_len} > {self.opts.max_dict_len}")
        self.dict = [self._read_string() for _ in range(dict_len)]

        # Decode root value
        result = self._decode_value()

        # Verify all input consumed — trailing bytes indicate corruption or concatenated data
        if self.pos < len(self.data):
            remaining = len(self.data) - self.pos
            raise ValueError(
                f"cowrie: trailing data after root value: {remaining} unconsumed bytes at position {self.pos}"
            )

        return result


def decode(data: bytes, on_unknown_ext: UnknownExtBehavior = UnknownExtBehavior.KEEP,
           opts: Optional[DecodeOptions] = None) -> Value:
    """Decode Cowrie v2 binary data into a Value.

    Args:
        data: Raw Cowrie v2 binary data.
        on_unknown_ext: Behavior for unknown extension types.
        opts: Optional decode security limits. Uses safe defaults if None.
    """
    return Decoder(data, on_unknown_ext=on_unknown_ext, opts=opts).decode()


# ============================================================
# Compression
# ============================================================

def encode_framed(v: Value, compression: Compression = Compression.ZSTD) -> bytes:
    """Encode with optional compression."""
    raw = encode(v)

    if compression == Compression.NONE or len(raw) < COMPRESS_THRESHOLD:
        return raw

    payload = raw[4:]  # Skip header

    if compression == Compression.GZIP:
        compressed = gzip.compress(payload)
    elif compression == Compression.ZSTD:
        if not HAS_ZSTD:
            return raw  # Fall back to raw
        cctx = zstd.ZstdCompressor()
        compressed = cctx.compress(payload)
    else:
        return raw

    if len(compressed) >= len(payload):
        return raw  # Compression didn't help

    # Build framed output
    buf = io.BytesIO()
    flags = FLAG_COMPRESSED | ((compression & 0x03) << 1)
    buf.write(MAGIC)
    buf.write(bytes([VERSION, flags]))
    buf.write(encode_uvarint(len(payload)))
    buf.write(compressed)

    return buf.getvalue()


def decode_framed(data: bytes, max_size: int = 0,
                  opts: Optional[DecodeOptions] = None) -> Value:
    """Decode with automatic decompression and decompression bomb protection.

    Args:
        data: Framed Cowrie v2 binary data (possibly compressed).
        max_size: Legacy max decompressed size override. If 0, uses
                  opts.max_decompressed_size instead.
        opts: Optional decode security limits. Uses safe defaults if None.
    """
    if opts is None:
        opts = DecodeOptions()
    effective_max = max_size if max_size > 0 else opts.max_decompressed_size

    if len(data) < 4:
        raise ValueError("Data too short")

    if data[0:2] != MAGIC:
        raise ValueError("Invalid magic bytes")

    if data[2] != VERSION:
        raise ValueError(f"Unsupported version: {data[2]}")

    flags = data[3]

    if not (flags & FLAG_COMPRESSED):
        return decode(data, opts=opts)

    comp_type = Compression((flags >> 1) & 0x03)

    orig_len, pos = decode_uvarint(data, 4)
    if effective_max > 0 and orig_len > effective_max:
        raise SecurityLimitExceeded(f"Decompressed size too large: {orig_len} > {effective_max}")
    compressed = data[pos:]

    if comp_type == Compression.GZIP:
        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as gz:
            decompressed = gz.read(effective_max + 1 if effective_max > 0 else -1)
    elif comp_type == Compression.ZSTD:
        if not HAS_ZSTD:
            raise ValueError("zstd not available")
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(io.BytesIO(compressed)) as reader:
            decompressed = reader.read(effective_max + 1 if effective_max > 0 else -1)
    else:
        raise ValueError(f"Unsupported compression: {comp_type}")

    if effective_max > 0 and len(decompressed) > effective_max:
        raise SecurityLimitExceeded(f"cowrie: decompressed data exceeds size limit"
                                    f" ({len(decompressed)} > {effective_max})")
    if len(decompressed) != orig_len:
        raise ValueError("Decompressed length mismatch")

    # Reconstruct full message
    full = MAGIC + bytes([VERSION, 0]) + decompressed
    return decode(full, opts=opts)


# ============================================================
# JSON Bridge
# ============================================================

# Patterns for type inference
ISO8601_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')
UUID_PATTERN = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')

# JavaScript safe integer range
MAX_SAFE_INT = 9007199254740991
MIN_SAFE_INT = -9007199254740991


def from_json(data: Union[str, bytes]) -> Value:
    """Parse JSON into a Cowrie value with type inference."""
    if isinstance(data, bytes):
        data = data.decode('utf-8')
    return from_any(json.loads(data))


def from_any(v: Any, field_name: str = "") -> Value:
    """Convert a Python value to a Cowrie value with type inference."""
    if v is None:
        return Value.null()
    elif isinstance(v, bool):
        return Value.bool_(v)
    elif isinstance(v, int):
        if v >= 0:
            return Value.uint64(v) if v > MAX_SAFE_INT else Value.int64(v)
        else:
            return Value.int64(v)
    elif isinstance(v, float):
        return Value.float64(v)
    elif isinstance(v, str):
        return _infer_string_type(v, field_name)
    elif isinstance(v, bytes):
        return Value.bytes_(v)
    elif isinstance(v, datetime):
        return Value.datetime(v)
    elif isinstance(v, UUID):
        return Value.uuid128(v)
    elif isinstance(v, list):
        return Value.array([from_any(item) for item in v])
    elif isinstance(v, dict):
        if v.get("_type") in ("ext", "unknown_ext") and "ext_type" in v and "payload" in v:
            try:
                ext_type = int(v.get("ext_type"))
                payload = base64.b64decode(v.get("payload"))
                return Value.unknown_ext(ext_type, payload)
            except Exception:
                pass
        members = {}
        for key, val in v.items():
            members[key] = from_any(val, key)
        return Value.object(members)
    elif HAS_NUMPY and isinstance(v, np.ndarray):
        # Convert NumPy arrays to tensors
        return Value.tensor_from_numpy(v)
    else:
        return Value.string(str(v))


def _infer_string_type(s: str, field_name: str) -> Value:
    """Infer a more specific type from a string value."""
    # Check field name hints
    lower_field = field_name.lower()

    if any(x in lower_field for x in ['time', 'date', '_at']) or lower_field in ['created', 'updated']:
        if ISO8601_PATTERN.match(s):
            try:
                dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
                return Value.datetime(dt)
            except ValueError:
                pass

    # Pattern-based inference
    if ISO8601_PATTERN.match(s):
        try:
            dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
            return Value.datetime(dt)
        except ValueError:
            pass

    if UUID_PATTERN.match(s):
        try:
            return Value.uuid128(UUID(s))
        except ValueError:
            pass

    return Value.string(s)


def to_json(v: Value, indent: Optional[int] = None) -> str:
    """Convert a Cowrie value to JSON string with canonical projections."""
    return json.dumps(to_any(v), indent=indent)


def to_any(v: Value) -> Any:
    """Convert a Cowrie value to a Python value."""
    if v.type == Type.NULL:
        return None
    elif v.type == Type.BOOL:
        return v.data
    elif v.type == Type.INT64:
        if MIN_SAFE_INT <= v.data <= MAX_SAFE_INT:
            return v.data
        return str(v.data)
    elif v.type == Type.UINT64:
        if v.data <= MAX_SAFE_INT:
            return v.data
        return str(v.data)
    elif v.type == Type.FLOAT64:
        return v.data
    elif v.type == Type.DECIMAL128:
        # Convert to decimal string
        coef_int = int.from_bytes(v.data.coef, 'big', signed=True)
        d = Decimal(coef_int) / (10 ** v.data.scale)
        return str(d)
    elif v.type == Type.STRING:
        return v.data
    elif v.type == Type.BYTES:
        return base64.b64encode(v.data).decode('ascii')
    elif v.type == Type.DATETIME64:
        dt = datetime.fromtimestamp(v.data / 1_000_000_000, tz=timezone.utc)
        return dt.isoformat().replace('+00:00', 'Z')
    elif v.type == Type.UUID128:
        return str(v.data)
    elif v.type == Type.BIGINT:
        return str(int.from_bytes(v.data, 'big', signed=True))
    elif v.type == Type.ARRAY:
        return [to_any(item) for item in v.data]
    elif v.type == Type.OBJECT:
        return {k: to_any(val) for k, val in v.data.items()}
    elif v.type == Type.TENSOR:
        t = v.data  # TensorData
        # Return as a dict with tensor metadata and base64-encoded data
        return {
            "_type": "tensor",
            "dtype": t.dtype.name.lower(),
            "shape": t.shape,
            "data": base64.b64encode(t.data).decode('ascii'),
        }
    elif v.type == Type.IMAGE:
        img = v.data  # ImageData
        return {
            "_type": "image",
            "format": img.format.name.lower(),
            "width": img.width,
            "height": img.height,
            "data": base64.b64encode(img.data).decode('ascii'),
        }
    elif v.type == Type.AUDIO:
        aud = v.data  # AudioData
        return {
            "_type": "audio",
            "encoding": aud.encoding.name.lower(),
            "sample_rate": aud.sample_rate,
            "channels": aud.channels,
            "data": base64.b64encode(aud.data).decode('ascii'),
        }
    elif v.type == Type.TENSOR_REF:
        ref = v.data  # TensorRefData
        return {
            "_type": "tensor_ref",
            "store_id": ref.store_id,
            "key": base64.b64encode(ref.key).decode('ascii'),
        }
    elif v.type == Type.ADJLIST:
        adj = v.data  # AdjlistData
        return {
            "_type": "adjlist",
            "id_width": "int32" if adj.id_width == IDWidth.INT32 else "int64",
            "node_count": adj.node_count,
            "edge_count": adj.edge_count,
            "row_offsets": adj.row_offsets,
            "col_indices": base64.b64encode(adj.col_indices).decode('ascii'),
        }
    elif v.type == Type.RICHTEXT:
        rt = v.data  # RichTextData
        result = {
            "_type": "richtext",
            "text": rt.text,
        }
        if rt.tokens:
            result["tokens"] = rt.tokens
        if rt.spans:
            result["spans"] = [
                {"start": s.start, "end": s.end, "kind_id": s.kind_id}
                for s in rt.spans
            ]
        return result
    elif v.type == Type.DELTA:
        delta = v.data  # DeltaData
        ops_json = []
        for op in delta.ops:
            op_dict = {
                "op_code": op.op_code.name.lower(),
                "field_id": op.field_id,
            }
            if op.value is not None:
                op_dict["value"] = to_any(op.value)
            ops_json.append(op_dict)
        return {
            "_type": "delta",
            "base_id": delta.base_id,
            "ops": ops_json,
        }
    # Graph types
    elif v.type == Type.NODE:
        node = v.data  # NodeData
        return {
            "_type": "node",
            "id": node.id,
            "labels": node.labels,
            "props": {k: to_any(val) for k, val in node.props.items()},
        }
    elif v.type == Type.EDGE:
        edge = v.data  # EdgeData
        return {
            "_type": "edge",
            "from": edge.from_id,
            "to": edge.to_id,
            "type": edge.edge_type,
            "props": {k: to_any(val) for k, val in edge.props.items()},
        }
    elif v.type == Type.NODE_BATCH:
        batch = v.data  # NodeBatchData
        return {
            "_type": "node_batch",
            "nodes": [
                {
                    "id": n.id,
                    "labels": n.labels,
                    "props": {k: to_any(val) for k, val in n.props.items()},
                }
                for n in batch.nodes
            ],
        }
    elif v.type == Type.EDGE_BATCH:
        batch = v.data  # EdgeBatchData
        return {
            "_type": "edge_batch",
            "edges": [
                {
                    "from": e.from_id,
                    "to": e.to_id,
                    "type": e.edge_type,
                    "props": {k: to_any(val) for k, val in e.props.items()},
                }
                for e in batch.edges
            ],
        }
    elif v.type == Type.GRAPH_SHARD:
        shard = v.data  # GraphShardData
        return {
            "_type": "graph_shard",
            "nodes": [
                {
                    "id": n.id,
                    "labels": n.labels,
                    "props": {k: to_any(val) for k, val in n.props.items()},
                }
                for n in shard.nodes
            ],
            "edges": [
                {
                    "from": e.from_id,
                    "to": e.to_id,
                    "type": e.edge_type,
                    "props": {k: to_any(val) for k, val in e.props.items()},
                }
                for e in shard.edges
            ],
            "metadata": {k: to_any(val) for k, val in shard.metadata.items()},
        }
    elif v.type == Type.UNKNOWN_EXT:
        ext = v.data  # UnknownExtData
        return {
            "_type": "unknown_ext",
            "ext_type": ext.ext_type,
            "payload": base64.b64encode(ext.payload).decode("ascii"),
        }
    else:
        return None


# ============================================================
# Convenience
# ============================================================

def dumps(obj: Any, compression: Compression = Compression.NONE) -> bytes:
    """Encode a Python object to Cowrie bytes."""
    v = from_any(obj)
    if compression == Compression.NONE:
        return encode(v)
    return encode_framed(v, compression)


def loads(data: bytes) -> Any:
    """Decode Cowrie bytes to a Python object."""
    # Check if compressed
    if len(data) >= 4 and data[3] & FLAG_COMPRESSED:
        v = decode_framed(data)
    else:
        v = decode(data)
    return to_any(v)


# ============================================================
# Deterministic Encoding
# ============================================================

@dataclass
class EncodeOptions:
    """Options for encoding."""
    deterministic: bool = False
    omit_null: bool = False


class DeterministicEncoder(Encoder):
    """Encoder that sorts object keys for deterministic output."""

    def __init__(self, opts: Optional[EncodeOptions] = None):
        super().__init__()
        self.opts = opts or EncodeOptions()

    def _collect_keys_sorted(self, v: Value):
        """Recursively collect all object keys in sorted order."""
        if v.type == Type.ARRAY:
            for item in v.data:
                self._collect_keys_sorted(item)
        elif v.type == Type.OBJECT:
            for key in sorted(v.data.keys()):
                val = v.data[key]
                # Skip null values if omit_null is set
                if self.opts.omit_null and val.type == Type.NULL:
                    continue
                self._add_key(key)
                self._collect_keys_sorted(val)
        elif v.type == Type.DELTA:
            # Delta ops may contain nested values
            for op in v.data.ops:
                if op.value is not None:
                    self._collect_keys_sorted(op.value)

    def _encode_value_sorted(self, v: Value):
        """Encode value with sorted object keys."""
        if v is None or v.type == Type.NULL:
            self._write_byte(Tag.NULL)
        elif v.type == Type.BOOL:
            self._write_byte(Tag.TRUE if v.data else Tag.FALSE)
        elif v.type == Type.INT64:
            self._write_byte(Tag.INT64)
            self._write_uvarint(zigzag_encode(v.data))
        elif v.type == Type.UINT64:
            if v.data < 0 or v.data > 0xFFFFFFFFFFFFFFFF:
                raise ValueError(f"UINT64 value out of range: {v.data} (must be 0..2^64-1)")
            self._write_byte(Tag.UINT64)
            self._write_uvarint(v.data)
        elif v.type == Type.FLOAT64:
            self._write_byte(Tag.FLOAT64)
            self._write(struct.pack('<d', v.data))
        elif v.type == Type.DECIMAL128:
            self._write_byte(Tag.DECIMAL128)
            self._write_byte(v.data.scale & 0xFF)
            self._write(v.data.coef)
        elif v.type == Type.STRING:
            self._write_byte(Tag.STRING)
            self._write_string(v.data)
        elif v.type == Type.BYTES:
            self._write_byte(Tag.BYTES)
            self._write_uvarint(len(v.data))
            self._write(v.data)
        elif v.type == Type.DATETIME64:
            self._write_byte(Tag.DATETIME64)
            self._write(struct.pack('<q', v.data))
        elif v.type == Type.UUID128:
            self._write_byte(Tag.UUID128)
            self._write(v.data.bytes)
        elif v.type == Type.BIGINT:
            self._write_byte(Tag.BIGINT)
            self._write_uvarint(len(v.data))
            self._write(v.data)
        elif v.type == Type.ARRAY:
            self._write_byte(Tag.ARRAY)
            self._write_uvarint(len(v.data))
            for item in v.data:
                self._encode_value_sorted(item)
        elif v.type == Type.OBJECT:
            # Filter null values if omit_null is set
            if self.opts.omit_null:
                items = [(k, v.data[k]) for k in sorted(v.data.keys()) if v.data[k].type != Type.NULL]
            else:
                items = [(k, v.data[k]) for k in sorted(v.data.keys())]
            self._write_byte(Tag.OBJECT)
            self._write_uvarint(len(items))
            for key, val in items:
                idx = self.dict_lookup[key]
                self._write_uvarint(idx)
                self._encode_value_sorted(val)
        elif v.type == Type.TENSOR:
            t = v.data  # TensorData
            self._write_byte(Tag.TENSOR)
            self._write_byte(t.dtype)
            self._write_uvarint(len(t.shape))
            for dim in t.shape:
                self._write_uvarint(dim)
            self._write_uvarint(len(t.data))
            self._write(t.data)
        elif v.type == Type.IMAGE:
            img = v.data  # ImageData
            self._write_byte(Tag.IMAGE)
            self._write_byte(img.format)
            self._write(struct.pack('<H', img.width))
            self._write(struct.pack('<H', img.height))
            self._write_uvarint(len(img.data))
            self._write(img.data)
        elif v.type == Type.AUDIO:
            aud = v.data  # AudioData
            self._write_byte(Tag.AUDIO)
            self._write_byte(aud.encoding)
            self._write(struct.pack('<I', aud.sample_rate))
            self._write_byte(aud.channels)
            self._write_uvarint(len(aud.data))
            self._write(aud.data)
        elif v.type == Type.TENSOR_REF:
            ref = v.data  # TensorRefData
            self._write_byte(Tag.TENSOR_REF)
            self._write_byte(ref.store_id)
            self._write_uvarint(len(ref.key))
            self._write(ref.key)
        elif v.type == Type.ADJLIST:
            adj = v.data  # AdjlistData
            self._write_byte(Tag.ADJLIST)
            self._write_byte(adj.id_width)
            self._write_uvarint(adj.node_count)
            self._write_uvarint(adj.edge_count)
            for offset in adj.row_offsets:
                self._write_uvarint(offset)
            self._write(adj.col_indices)
        elif v.type == Type.RICHTEXT:
            rt = v.data  # RichTextData
            self._write_byte(Tag.RICHTEXT)
            text_bytes = rt.text.encode('utf-8')
            self._write_uvarint(len(text_bytes))
            self._write(text_bytes)
            # Calculate and write flags byte
            tokens = rt.tokens or []
            spans = rt.spans or []
            flags = 0
            if len(tokens) > 0:
                flags |= 0x01
            if len(spans) > 0:
                flags |= 0x02
            self._write_byte(flags)
            # Write tokens if present
            if flags & 0x01:
                self._write_uvarint(len(tokens))
                for tok in tokens:
                    self._write(struct.pack('<i', tok))
            # Write spans if present
            if flags & 0x02:
                self._write_uvarint(len(spans))
                for span in spans:
                    self._write_uvarint(span.start)
                    self._write_uvarint(span.end)
                    self._write_uvarint(span.kind_id)
        elif v.type == Type.DELTA:
            delta = v.data  # DeltaData
            self._write_byte(Tag.DELTA)
            self._write_uvarint(delta.base_id)
            self._write_uvarint(len(delta.ops))
            for op in delta.ops:
                self._write_byte(op.op_code)
                self._write_uvarint(op.field_id)
                if op.op_code in (DeltaOpCode.SET_FIELD, DeltaOpCode.APPEND_ARRAY):
                    if op.value is not None:
                        self._encode_value_sorted(op.value)

    def encode_with_opts(self, v: Value) -> bytes:
        """Encode with options."""
        if not self.opts.deterministic:
            return self.encode(v)

        # Collect keys in sorted order
        self._collect_keys_sorted(v)

        # Write header
        self._write(MAGIC)
        self._write_byte(VERSION)
        self._write_byte(0)  # flags

        # Write dictionary
        self._write_uvarint(len(self.dict_keys))
        for key in self.dict_keys:
            self._write_string(key)

        # Write root value with sorted keys
        self._encode_value_sorted(v)

        return self.buf.getvalue()


def encode_with_opts(v: Value, opts: Optional[EncodeOptions] = None) -> bytes:
    """Encode a value with options."""
    if opts is None:
        opts = EncodeOptions()
    return DeterministicEncoder(opts).encode_with_opts(v)


# ============================================================
# Schema Fingerprinting (FNV-1a)
# ============================================================

FNV_OFFSET_BASIS = 14695981039346656037
FNV_PRIME = 1099511628211
FNV_MASK = (1 << 64) - 1


def _fnv_hash_byte(h: int, b: int) -> int:
    """FNV-1a hash a single byte."""
    h ^= b
    h = (h * FNV_PRIME) & FNV_MASK
    return h


def _fnv_hash_u64(h: int, v: int) -> int:
    """FNV-1a hash a 64-bit integer."""
    for i in range(8):
        h ^= (v >> (i * 8)) & 0xFF
        h = (h * FNV_PRIME) & FNV_MASK
    return h


def _fnv_hash_string(h: int, s: str) -> int:
    """FNV-1a hash a string."""
    h = _fnv_hash_u64(h, len(s))
    for b in s.encode('utf-8'):
        h ^= b
        h = (h * FNV_PRIME) & FNV_MASK
    return h


def _type_to_ord(t: Type) -> int:
    """Map type to ordinal for cross-language fingerprint compatibility.

    Matches Go's Type enum: Null=0, Bool=1, Int64=2, Uint64=3, Float64=4, Decimal128=5,
    String=6, Bytes=7, Datetime64=8, UUID128=9, BigInt=10, Array=11, Object=12, Tensor=13,
    Image=14, Audio=15, TensorRef=16, Adjlist=17, RichText=18, Delta=19
    """
    mapping = {
        Type.NULL: 0,
        Type.BOOL: 1,
        Type.INT64: 2,
        Type.UINT64: 3,
        Type.FLOAT64: 4,
        Type.DECIMAL128: 5,
        Type.STRING: 6,
        Type.BYTES: 7,
        Type.DATETIME64: 8,
        Type.UUID128: 9,
        Type.BIGINT: 10,
        Type.ARRAY: 11,
        Type.OBJECT: 12,
        Type.TENSOR: 13,
        Type.IMAGE: 14,
        Type.AUDIO: 15,
        Type.TENSOR_REF: 16,
        Type.ADJLIST: 17,
        Type.RICHTEXT: 18,
        Type.DELTA: 19,
    }
    return mapping.get(t, 0xFF)


def _hash_schema(v: Value, h: int) -> int:
    """Hash the schema of a value."""
    h = _fnv_hash_byte(h, _type_to_ord(v.type))

    if v.type in (Type.NULL, Type.BOOL, Type.INT64, Type.UINT64, Type.FLOAT64,
                  Type.STRING, Type.BYTES, Type.DECIMAL128, Type.DATETIME64,
                  Type.UUID128, Type.BIGINT):
        # Scalar types: type tag is sufficient
        pass
    elif v.type == Type.ARRAY:
        arr = v.data
        h = _fnv_hash_u64(h, len(arr))
        for item in arr:
            h = _hash_schema(item, h)
    elif v.type == Type.OBJECT:
        obj = v.data
        h = _fnv_hash_u64(h, len(obj))
        # Sort keys for canonical ordering
        for key in sorted(obj.keys()):
            h = _fnv_hash_string(h, key)
            h = _hash_schema(obj[key], h)
    elif v.type == Type.TENSOR:
        # Include dtype and rank (dims are data, not schema)
        t = v.data  # TensorData
        h = _fnv_hash_byte(h, t.dtype)
        h = _fnv_hash_u64(h, len(t.shape))
    elif v.type == Type.IMAGE:
        # Include format in schema (dimensions are data, not schema)
        img = v.data  # ImageData
        h = _fnv_hash_byte(h, img.format)
    elif v.type == Type.AUDIO:
        # Include encoding in schema (sample_rate, channels are data)
        aud = v.data  # AudioData
        h = _fnv_hash_byte(h, aud.encoding)
    elif v.type == Type.TENSOR_REF:
        # Include store_id in schema (key is data)
        ref = v.data  # TensorRefData
        h = _fnv_hash_byte(h, ref.store_id)
    elif v.type == Type.ADJLIST:
        # Include id_width in schema (counts are data)
        adj = v.data  # AdjlistData
        h = _fnv_hash_byte(h, adj.id_width)
    elif v.type == Type.RICHTEXT:
        # Type tag is sufficient for schema (text/tokens/spans are data)
        pass
    elif v.type == Type.DELTA:
        # Include op codes in schema
        delta = v.data  # DeltaData
        h = _fnv_hash_u64(h, len(delta.ops))
        for op in delta.ops:
            h = _fnv_hash_byte(h, op.op_code)
            if op.value is not None:
                h = _hash_schema(op.value, h)

    return h


def schema_fingerprint64(v: Value) -> int:
    """
    Compute a 64-bit FNV-1a fingerprint of the value's schema.

    The fingerprint captures type structure (field names, types)
    but not actual values. Two values with identical structure
    produce the same fingerprint.
    """
    return _hash_schema(v, FNV_OFFSET_BASIS)


def schema_fingerprint32(v: Value) -> int:
    """
    Returns the low 32 bits of the 64-bit schema fingerprint.
    Suitable for use as a type ID in stream frames.
    """
    return schema_fingerprint64(v) & 0xFFFFFFFF


def schema_equals(a: Value, b: Value) -> bool:
    """Check if two values have the same schema."""
    return schema_fingerprint64(a) == schema_fingerprint64(b)


# ============================================================
# CRC32-IEEE
# ============================================================

CRC32_POLYNOMIAL = 0xEDB88320


def _build_crc32_table() -> List[int]:
    """Build CRC32 lookup table."""
    table = []
    for i in range(256):
        c = i
        for _ in range(8):
            if c & 1:
                c = CRC32_POLYNOMIAL ^ (c >> 1)
            else:
                c >>= 1
        table.append(c)
    return table


_CRC32_TABLE = _build_crc32_table()


def crc32(data: bytes) -> int:
    """Calculate CRC32-IEEE checksum."""
    crc = 0xFFFFFFFF
    for b in data:
        crc = _CRC32_TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return (~crc) & 0xFFFFFFFF


# ============================================================
# Master Stream Implementation
# ============================================================

MASTER_MAGIC = b'SJST'
MASTER_VERSION = 0x02

# Master stream frame flags
MFLAG_COMPRESSED = 0x01
MFLAG_CRC = 0x02
MFLAG_DETERMINISTIC = 0x04
MFLAG_META = 0x08
MFLAG_COMP_GZIP = 0x10
MFLAG_COMP_ZSTD = 0x20


@dataclass
class MasterFrameHeader:
    """Master stream frame header."""
    version: int
    flags: int
    header_len: int
    type_id: int
    payload_len: int
    raw_len: int
    meta_len: int


@dataclass
class MasterFrame:
    """Decoded master stream frame."""
    header: MasterFrameHeader
    meta: Optional[Value]
    payload: Value
    type_id: int


@dataclass
class MasterWriterOptions:
    """Options for master stream writer."""
    deterministic: bool = True
    enable_crc: bool = True
    compress: int = 0  # 0=none, 1=gzip, 2=zstd


def is_master_stream(data: bytes) -> bool:
    """Check if data starts with master stream magic 'SJST'."""
    return len(data) >= 4 and data[:4] == MASTER_MAGIC


def is_cowrie_document(data: bytes) -> bool:
    """Check if data starts with Cowrie document magic but not master stream."""
    if len(data) < 4:
        return False
    if data[:2] != MAGIC:
        return False
    # Exclude master stream format
    if data[2:4] == b'ST':
        return False
    return True


def write_master_frame(
    value: Value,
    meta: Optional[Value] = None,
    opts: Optional[MasterWriterOptions] = None
) -> bytes:
    """Write a master stream frame."""
    if opts is None:
        opts = MasterWriterOptions()

    enc_opts = EncodeOptions(deterministic=opts.deterministic)

    # Encode payload
    payload_bytes = encode_with_opts(value, enc_opts)

    # Encode metadata if present
    meta_bytes = b''
    if meta is not None:
        meta_bytes = encode_with_opts(meta, enc_opts)

    # Compute type ID from schema
    type_id = schema_fingerprint32(value)

    # Build flags
    frame_flags = 0
    if opts.deterministic:
        frame_flags |= MFLAG_DETERMINISTIC
    if opts.enable_crc:
        frame_flags |= MFLAG_CRC
    if len(meta_bytes) > 0:
        frame_flags |= MFLAG_META

    # Header length (fixed at 24 bytes for v2)
    header_len = 24

    # Build frame
    buf = io.BytesIO()

    # Magic
    buf.write(MASTER_MAGIC)

    # Version
    buf.write(bytes([MASTER_VERSION]))

    # Flags
    buf.write(bytes([frame_flags]))

    # Header length (LE)
    buf.write(struct.pack('<H', header_len))

    # Type ID (LE)
    buf.write(struct.pack('<I', type_id))

    # Payload length (LE)
    buf.write(struct.pack('<I', len(payload_bytes)))

    # Raw length (0 = not compressed)
    buf.write(struct.pack('<I', 0))

    # Meta length (LE)
    buf.write(struct.pack('<I', len(meta_bytes)))

    # Metadata
    buf.write(meta_bytes)

    # Payload
    buf.write(payload_bytes)

    # CRC32 if enabled
    if opts.enable_crc:
        frame_data = buf.getvalue()
        crc_value = crc32(frame_data)
        buf.write(struct.pack('<I', crc_value))

    return buf.getvalue()


def read_master_frame(data: bytes) -> tuple[MasterFrame, int]:
    """
    Read a master stream frame.

    Returns (frame, bytes_consumed).
    """
    if len(data) < 24:
        raise ValueError("Truncated master frame")

    # Check magic
    if not is_master_stream(data):
        raise ValueError("Invalid master stream magic")

    version = data[4]
    if version != MASTER_VERSION:
        raise ValueError(f"Invalid master stream version: {version}")

    frame_flags = data[5]
    header_len = struct.unpack('<H', data[6:8])[0]
    type_id = struct.unpack('<I', data[8:12])[0]
    payload_len = struct.unpack('<I', data[12:16])[0]
    raw_len = struct.unpack('<I', data[16:20])[0]
    meta_len = struct.unpack('<I', data[20:24])[0]

    pos = max(header_len, 24)

    # Read metadata
    meta = None
    if (frame_flags & MFLAG_META) and meta_len > 0:
        if pos + meta_len > len(data):
            raise ValueError("Truncated master frame (metadata)")
        meta = decode(data[pos:pos + meta_len])
        pos += meta_len
    else:
        pos += meta_len

    # Read payload
    if pos + payload_len > len(data):
        raise ValueError("Truncated master frame (payload)")
    payload = decode(data[pos:pos + payload_len])
    pos += payload_len

    # Verify CRC
    if frame_flags & MFLAG_CRC:
        if pos + 4 > len(data):
            raise ValueError("Truncated master frame (CRC)")
        expected_crc = struct.unpack('<I', data[pos:pos + 4])[0]
        actual_crc = crc32(data[:pos])
        if actual_crc != expected_crc:
            raise ValueError("CRC mismatch")
        pos += 4

    header = MasterFrameHeader(
        version=version,
        flags=frame_flags,
        header_len=header_len,
        type_id=type_id,
        payload_len=payload_len,
        raw_len=raw_len,
        meta_len=meta_len,
    )

    return MasterFrame(header=header, meta=meta, payload=payload, type_id=type_id), pos


if __name__ == '__main__':
    # Quick test
    import time

    obj = {
        "user_id": 12345,
        "user_name": "alice",
        "created_at": "2025-11-28T12:34:56Z",
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "active": True,
        "tags": ["admin", "user"]
    }

    # Encode
    data = dumps(obj)
    json_data = json.dumps(obj).encode()

    print(f"Cowrie size: {len(data)} bytes")
    print(f"JSON size:  {len(json_data)} bytes")
    print(f"Savings:    {(1 - len(data)/len(json_data))*100:.1f}%")

    # Decode
    result = loads(data)
    print(f"\nDecoded: {result}")

    # Compression test
    big_obj = {"field_" + str(i): "This is test value " * 5 for i in range(100)}

    raw = dumps(big_obj)
    compressed = dumps(big_obj, Compression.GZIP)

    print(f"\nLarge object:")
    print(f"Raw size:        {len(raw)} bytes")
    print(f"Compressed size: {len(compressed)} bytes")
    print(f"Ratio:           {len(compressed)/len(raw)*100:.1f}%")

    # Deterministic encoding test
    value = from_any(obj)
    det_data = encode_with_opts(value, EncodeOptions(deterministic=True))
    print(f"\nDeterministic encode: {len(det_data)} bytes")

    # Schema fingerprinting test
    fp = schema_fingerprint32(value)
    print(f"Schema fingerprint: 0x{fp:08x}")

    # Master stream test
    master_data = write_master_frame(value, None, MasterWriterOptions(deterministic=True, enable_crc=True))
    print(f"Master stream size: {len(master_data)} bytes")

    frame, consumed = read_master_frame(master_data)
    print(f"Master frame typeId: 0x{frame.type_id:08x}")
    print(f"Master frame consumed: {consumed} bytes")
