"""
Cowrie v2 Codec - Binary encode/decode for Cowrie values.

Wire format:
    Magic:   'S' 'J'     (2 bytes)
    Version: 0x02        (1 byte)
    Flags:   0x00        (1 byte)
    DictLen: uvarint
    Dict:    DictLen x [len:uvarint][utf8 bytes]
    RootVal: encoded value
"""

import struct
from io import BytesIO
from typing import Any, BinaryIO, List, Tuple

from .types import (
    Value, Type, from_python,
    MAGIC, VERSION,
    TAG_NULL, TAG_FALSE, TAG_TRUE,
    TAG_INT64, TAG_UINT64, TAG_FLOAT64,
    TAG_STRING, TAG_BYTES,
    TAG_ARRAY, TAG_OBJECT,
    TAG_DECIMAL128, TAG_DATETIME64, TAG_UUID128, TAG_BIGINT,
    Null, Bool, Int64, Uint64, Float64, String, Bytes, Array, Object,
    Decimal128Data,
)


# === Encoding ===

def zigzag_encode(n: int) -> int:
    """Encode signed int to unsigned using zigzag encoding."""
    return (n << 1) ^ (n >> 63)


def zigzag_decode(n: int) -> int:
    """Decode zigzag-encoded unsigned int to signed."""
    return (n >> 1) ^ -(n & 1)


class BinaryWriter:
    """Low-level binary writer with uvarint support."""

    def __init__(self):
        self.buf = BytesIO()

    def write(self, data: bytes):
        self.buf.write(data)

    def write_byte(self, b: int):
        self.buf.write(bytes([b]))

    def write_uvarint(self, v: int):
        """Write unsigned variable-length integer."""
        while v >= 0x80:
            self.buf.write(bytes([(v & 0x7F) | 0x80]))
            v >>= 7
        self.buf.write(bytes([v]))

    def write_string(self, s: str):
        encoded = s.encode('utf-8')
        self.write_uvarint(len(encoded))
        self.buf.write(encoded)

    def write_u64_le(self, v: int):
        self.buf.write(struct.pack('<Q', v))

    def write_i64_le(self, v: int):
        self.buf.write(struct.pack('<q', v))

    def write_f64_le(self, v: float):
        self.buf.write(struct.pack('<d', v))

    def getvalue(self) -> bytes:
        return self.buf.getvalue()


class BinaryReader:
    """Low-level binary reader with uvarint support."""

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def read_byte(self) -> int:
        if self.pos >= len(self.data):
            raise EOFError("Unexpected end of data")
        b = self.data[self.pos]
        self.pos += 1
        return b

    def read(self, n: int) -> bytes:
        if self.pos + n > len(self.data):
            raise EOFError("Unexpected end of data")
        result = self.data[self.pos:self.pos + n]
        self.pos += n
        return result

    def read_uvarint(self) -> int:
        """Read unsigned variable-length integer."""
        result = 0
        shift = 0
        while True:
            b = self.read_byte()
            result |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                return result
            shift += 7
            if shift > 63:
                raise ValueError("uvarint too long")

    def read_string(self) -> str:
        length = self.read_uvarint()
        data = self.read(length)
        return data.decode('utf-8')

    def read_u64_le(self) -> int:
        data = self.read(8)
        return struct.unpack('<Q', data)[0]

    def read_i64_le(self) -> int:
        data = self.read(8)
        return struct.unpack('<q', data)[0]

    def read_f64_le(self) -> float:
        data = self.read(8)
        return struct.unpack('<d', data)[0]


def _collect_keys(v: Value, keys: dict):
    """Recursively collect all object keys into dictionary."""
    if v.type == Type.ARRAY:
        for item in v.array_value():
            _collect_keys(item, keys)
    elif v.type == Type.OBJECT:
        for key, val in v.object_value():
            if key not in keys:
                keys[key] = len(keys)
            _collect_keys(val, keys)


def _encode_value(w: BinaryWriter, v: Value, key_dict: dict):
    """Encode a single value to the writer."""
    if v.type == Type.NULL:
        w.write_byte(TAG_NULL)

    elif v.type == Type.BOOL:
        w.write_byte(TAG_TRUE if v.bool_value() else TAG_FALSE)

    elif v.type == Type.INT64:
        w.write_byte(TAG_INT64)
        w.write_uvarint(zigzag_encode(v.int64_value()))

    elif v.type == Type.UINT64:
        w.write_byte(TAG_UINT64)
        w.write_uvarint(v.uint64_value())

    elif v.type == Type.FLOAT64:
        w.write_byte(TAG_FLOAT64)
        w.write_f64_le(v.float64_value())

    elif v.type == Type.STRING:
        w.write_byte(TAG_STRING)
        w.write_string(v.string_value())

    elif v.type == Type.BYTES:
        w.write_byte(TAG_BYTES)
        data = v.bytes_value()
        w.write_uvarint(len(data))
        w.write(data)

    elif v.type == Type.ARRAY:
        w.write_byte(TAG_ARRAY)
        items = v.array_value()
        w.write_uvarint(len(items))
        for item in items:
            _encode_value(w, item, key_dict)

    elif v.type == Type.OBJECT:
        w.write_byte(TAG_OBJECT)
        members = v.object_value()
        w.write_uvarint(len(members))
        for key, val in members:
            key_idx = key_dict[key]
            w.write_uvarint(key_idx)
            _encode_value(w, val, key_dict)

    elif v.type == Type.DECIMAL128:
        w.write_byte(TAG_DECIMAL128)
        dec = v.decimal128_value()
        w.write_byte(dec.scale & 0xFF)  # signed byte as unsigned
        w.write(dec.coef)

    elif v.type == Type.DATETIME64:
        w.write_byte(TAG_DATETIME64)
        w.write_u64_le(v.datetime64_value())

    elif v.type == Type.UUID128:
        w.write_byte(TAG_UUID128)
        w.write(v.uuid128_value())

    elif v.type == Type.BIGINT:
        w.write_byte(TAG_BIGINT)
        data = v.bigint_value()
        w.write_uvarint(len(data))
        w.write(data)

    else:
        raise ValueError(f"Unknown type: {v.type}")


def encode(v: Any) -> bytes:
    """
    Encode a value to Cowrie v2 binary format.

    Args:
        v: A Cowrie Value, or a Python object (dict, list, str, int, etc.)

    Returns:
        Cowrie binary data as bytes
    """
    if not isinstance(v, Value):
        v = from_python(v)

    # Build key dictionary
    key_dict = {}
    _collect_keys(v, key_dict)
    keys = sorted(key_dict.keys(), key=lambda k: key_dict[k])

    w = BinaryWriter()

    # Write header
    w.write(MAGIC)
    w.write_byte(VERSION)
    w.write_byte(0)  # flags = 0

    # Write dictionary
    w.write_uvarint(len(keys))
    for key in keys:
        w.write_string(key)

    # Write root value
    _encode_value(w, v, key_dict)

    return w.getvalue()


def encode_to(f: BinaryIO, v: Any):
    """Encode a value and write to a file-like object."""
    data = encode(v)
    f.write(data)


# === Decoding ===

def _decode_value(r: BinaryReader, key_dict: List[str]) -> Value:
    """Decode a single value from the reader."""
    tag = r.read_byte()

    if tag == TAG_NULL:
        return Null()

    elif tag == TAG_FALSE:
        return Bool(False)

    elif tag == TAG_TRUE:
        return Bool(True)

    elif tag == TAG_INT64:
        v = r.read_uvarint()
        return Int64(zigzag_decode(v))

    elif tag == TAG_UINT64:
        v = r.read_uvarint()
        return Uint64(v)

    elif tag == TAG_FLOAT64:
        v = r.read_f64_le()
        return Float64(v)

    elif tag == TAG_STRING:
        s = r.read_string()
        return String(s)

    elif tag == TAG_BYTES:
        length = r.read_uvarint()
        data = r.read(length)
        return Bytes(data)

    elif tag == TAG_ARRAY:
        count = r.read_uvarint()
        items = [_decode_value(r, key_dict) for _ in range(count)]
        return Array(*items)

    elif tag == TAG_OBJECT:
        count = r.read_uvarint()
        members = []
        for _ in range(count):
            key_idx = r.read_uvarint()
            if key_idx >= len(key_dict):
                raise ValueError(f"Invalid key index: {key_idx}")
            key = key_dict[key_idx]
            val = _decode_value(r, key_dict)
            members.append((key, val))
        return Object(*members)

    elif tag == TAG_DECIMAL128:
        scale = r.read_byte()
        # Convert unsigned byte to signed
        if scale > 127:
            scale = scale - 256
        coef = r.read(16)
        dec = Decimal128Data(scale, coef)
        return Value(Type.DECIMAL128, dec)

    elif tag == TAG_DATETIME64:
        nanos = r.read_u64_le()
        return Value(Type.DATETIME64, nanos)

    elif tag == TAG_UUID128:
        uuid_bytes = r.read(16)
        return Value(Type.UUID128, uuid_bytes)

    elif tag == TAG_BIGINT:
        length = r.read_uvarint()
        data = r.read(length)
        return Value(Type.BIGINT, data)

    else:
        raise ValueError(f"Unknown tag: 0x{tag:02x}")


def decode(data: bytes) -> Value:
    """
    Decode Cowrie v2 binary data into a Value.

    Args:
        data: Cowrie binary data

    Returns:
        Cowrie Value object
    """
    r = BinaryReader(data)

    # Read and verify magic
    magic = r.read(2)
    if magic != MAGIC:
        raise ValueError(f"Invalid magic bytes: {magic!r}")

    # Read and verify version
    version = r.read_byte()
    if version != VERSION:
        raise ValueError(f"Unsupported version: {version}")

    # Read flags
    flags = r.read_byte()

    # Skip column hints if present
    if flags & 0x08:
        count = r.read_uvarint()
        for _ in range(count):
            r.read_string()  # field name
            r.read_byte()     # type
            shape_len = r.read_uvarint()
            for _ in range(shape_len):
                r.read_uvarint()
            r.read_byte()     # flags

    # Read dictionary
    dict_len = r.read_uvarint()
    key_dict = [r.read_string() for _ in range(dict_len)]

    # Decode root value
    return _decode_value(r, key_dict)


def decode_from(f: BinaryIO) -> Value:
    """Decode from a file-like object."""
    data = f.read()
    return decode(data)


# === Compression ===

# Optional zstd import
try:
    import zstandard as zstd
    HAS_ZSTD = True
except ImportError:
    try:
        import zstd
        HAS_ZSTD = True
    except ImportError:
        HAS_ZSTD = False

MAGIC_COMPRESSED = b'SJZ'  # Compressed Cowrie


def byte_shuffle(data: bytes, element_size: int = 4) -> bytes:
    """
    Shuffle bytes to group similar bytes together for better compression.

    Lossless transformation that improves zstd compression by 10-30%.

    Example (element_size=4):
        [A1 B1 C1 D1] [A2 B2 C2 D2] → [A1 A2] [B1 B2] [C1 C2] [D1 D2]
    """
    if len(data) % element_size != 0:
        return data  # Can't shuffle, return as-is

    n_elements = len(data) // element_size
    result = bytearray(len(data))

    for i in range(n_elements):
        for j in range(element_size):
            result[j * n_elements + i] = data[i * element_size + j]

    return bytes(result)


def byte_unshuffle(data: bytes, element_size: int = 4) -> bytes:
    """Reverse byte shuffle."""
    if len(data) % element_size != 0:
        return data

    n_elements = len(data) // element_size
    result = bytearray(len(data))

    for i in range(n_elements):
        for j in range(element_size):
            result[i * element_size + j] = data[j * n_elements + i]

    return bytes(result)


MAGIC_SHUFFLED = b'SJS'  # Shuffled + compressed


def encode_compressed(v: Any, level: int = 19, shuffle: bool = True) -> bytes:
    """
    Encode and compress with zstd.

    Args:
        v: Value to encode
        level: Compression level (1-22, default 19 for best compression)
        shuffle: Apply byte shuffling for better compression (lossless)

    Returns:
        Compressed bytes with 'SJZ' or 'SJS' magic
    """
    if not HAS_ZSTD:
        raise ImportError("zstandard required. Install with: pip install zstandard")

    raw = encode(v)

    # Apply byte shuffling (lossless, improves compression)
    if shuffle:
        shuffled = byte_shuffle(raw, 4)
        magic = MAGIC_SHUFFLED
    else:
        shuffled = raw
        magic = MAGIC_COMPRESSED

    # Compress
    if hasattr(zstd, 'ZstdCompressor'):
        cctx = zstd.ZstdCompressor(level=level)
        compressed = cctx.compress(shuffled)
    else:
        compressed = zstd.compress(shuffled, level)

    # Build output: magic + original_size(u32) + compressed
    result = bytearray()
    result.extend(magic)
    result.extend(struct.pack('<I', len(raw)))
    result.extend(compressed)

    return bytes(result)


def decode_compressed(data: bytes) -> Value:
    """
    Decode compressed Cowrie data.

    Auto-detects format:
      - 'SJ'  = uncompressed
      - 'SJZ' = zstd compressed
      - 'SJS' = byte-shuffled + zstd compressed
    """
    if not HAS_ZSTD and data[:3] in (MAGIC_COMPRESSED, MAGIC_SHUFFLED):
        raise ImportError("zstandard required. Install with: pip install zstandard")

    # Check magic
    if data[:3] == MAGIC_SHUFFLED:
        # Shuffled + compressed
        original_size = struct.unpack('<I', data[3:7])[0]

        if hasattr(zstd, 'ZstdDecompressor'):
            dctx = zstd.ZstdDecompressor()
            shuffled = dctx.decompress(data[7:], max_output_size=original_size)
        else:
            shuffled = zstd.decompress(data[7:])

        # Unshuffle
        raw = byte_unshuffle(shuffled, 4)
        return decode(raw)

    elif data[:3] == MAGIC_COMPRESSED:
        # Just compressed, no shuffle
        original_size = struct.unpack('<I', data[3:7])[0]

        if hasattr(zstd, 'ZstdDecompressor'):
            dctx = zstd.ZstdDecompressor()
            raw = dctx.decompress(data[7:], max_output_size=original_size)
        else:
            raw = zstd.decompress(data[7:])

        return decode(raw)

    elif data[:2] == MAGIC:
        # Uncompressed
        return decode(data)
    else:
        raise ValueError(f"Invalid magic: {data[:4]!r}")


def save(v: Any, path: str, compressed: bool = False):
    """Save value to file. Uncompressed by default to preserve streaming capability."""
    if compressed:
        data = encode_compressed(v)
    else:
        data = encode(v)
    with open(path, 'wb') as f:
        f.write(data)


def load(path: str) -> Value:
    """Load value from file (auto-detects compression)."""
    with open(path, 'rb') as f:
        data = f.read()
    return decode_compressed(data)
