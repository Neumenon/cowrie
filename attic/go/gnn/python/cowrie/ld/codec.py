"""
Cowrie-LD Codec - Binary encode/decode for Cowrie-LD documents.

Wire format:
    Magic:     'S' 'J' 'L' 'D'  (4 bytes)
    Version:   0x01             (1 byte)
    Flags:     0x00             (1 byte)
    FieldDict: [count:uvarint][entries...]
    Terms:     [count:uvarint][TermEntry...]
    IRIs:      [count:uvarint][entries...]
    Datatypes: [count:uvarint][entries...]
    RootValue: Cowrie v2 value tree (with extended tags for IRI/BNode)

Compressed format (SJLZ):
    Magic:     'S' 'J' 'L' 'Z'  (4 bytes)
    OrigSize:  u32              (original size)
    Data:      zstd-compressed document
"""

import struct
from io import BytesIO
from typing import List, Optional
import sys

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

MAGIC_COMPRESSED = b'SJLZ'
MAGIC_SHUFFLED = b'SJLS'  # Shuffled + compressed


def byte_shuffle(data: bytes, element_size: int = 4) -> bytes:
    """
    Shuffle bytes to group similar bytes together for better compression.
    Lossless transformation that improves zstd compression.
    """
    if len(data) % element_size != 0:
        return data
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


sys.path.insert(0, str(__file__).rsplit('/', 2)[0])

from .types import (
    MAGIC, VERSION,
    TAG_IRI, TAG_BNODE,
    IRI, TermEntry, TermFlags,
    LDDocument, LDValue,
)
from cowrie.types import (
    Value, Type,
    TAG_NULL, TAG_FALSE, TAG_TRUE,
    TAG_INT64, TAG_UINT64, TAG_FLOAT64,
    TAG_STRING, TAG_BYTES,
    TAG_ARRAY, TAG_OBJECT,
    TAG_DECIMAL128, TAG_DATETIME64, TAG_UUID128, TAG_BIGINT,
    Null, Bool, Int64, Uint64, Float64, String, Bytes, Array, Object,
    Decimal128Data,
)
from cowrie.codec import zigzag_encode, zigzag_decode


class BinaryWriter:
    """Low-level binary writer."""

    def __init__(self):
        self.buf = BytesIO()

    def write(self, data: bytes):
        self.buf.write(data)

    def write_byte(self, b: int):
        self.buf.write(bytes([b]))

    def write_uvarint(self, v: int):
        while v >= 0x80:
            self.buf.write(bytes([(v & 0x7F) | 0x80]))
            v >>= 7
        self.buf.write(bytes([v]))

    def write_string(self, s: str):
        encoded = s.encode('utf-8')
        self.write_uvarint(len(encoded))
        self.buf.write(encoded)

    def write_f64_le(self, v: float):
        self.buf.write(struct.pack('<d', v))

    def write_u64_le(self, v: int):
        self.buf.write(struct.pack('<Q', v))

    def getvalue(self) -> bytes:
        return self.buf.getvalue()


class BinaryReader:
    """Low-level binary reader."""

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
        result = 0
        shift = 0
        while True:
            b = self.read_byte()
            result |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                return result
            shift += 7

    def read_string(self) -> str:
        length = self.read_uvarint()
        data = self.read(length)
        return data.decode('utf-8')

    def read_f64_le(self) -> float:
        data = self.read(8)
        return struct.unpack('<d', data)[0]

    def read_u64_le(self) -> int:
        data = self.read(8)
        return struct.unpack('<Q', data)[0]


def _collect_keys(v: Value, doc: LDDocument):
    """Recursively collect all object keys into the document's field dictionary."""
    # Handle LDValue wrapper
    if isinstance(v, LDValue):
        if v.is_iri or v.is_bnode:
            return  # IRI/BNode references don't have nested keys
        v = v.value

    if v.type == Type.ARRAY:
        for item in v.array_value():
            _collect_keys(item, doc)
    elif v.type == Type.OBJECT:
        for key, val in v.object_value():
            doc.add_field(key)
            _collect_keys(val, doc)


def _encode_value(w: BinaryWriter, v: Value, doc: LDDocument):
    """Encode a single value with Cowrie-LD extensions."""
    # Check if this is an LDValue with special semantics
    if isinstance(v, LDValue):
        if v.is_iri:
            w.write_byte(TAG_IRI)
            w.write_uvarint(v.iri_id)
            return
        elif v.is_bnode:
            w.write_byte(TAG_BNODE)
            w.write_string(v.bnode_id)
            return
        # Otherwise encode the underlying value
        v = v.value

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
            _encode_value(w, item, doc)

    elif v.type == Type.OBJECT:
        w.write_byte(TAG_OBJECT)
        members = v.object_value()
        w.write_uvarint(len(members))
        for key, val in members:
            key_idx = doc._field_lookup.get(key, doc.add_field(key))
            w.write_uvarint(key_idx)
            _encode_value(w, val, doc)

    elif v.type == Type.DECIMAL128:
        w.write_byte(TAG_DECIMAL128)
        dec = v.decimal128_value()
        w.write_byte(dec.scale & 0xFF)
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


def encode(doc: LDDocument) -> bytes:
    """
    Encode a Cowrie-LD document to binary format.

    Args:
        doc: LDDocument with terms, IRIs, and root value

    Returns:
        Cowrie-LD binary data as bytes
    """
    if doc.root is None:
        raise ValueError("Document has no root value")

    # Collect all field keys
    _collect_keys(doc.root, doc)

    w = BinaryWriter()

    # Write header
    w.write(MAGIC)
    w.write_byte(VERSION)
    w.write_byte(0)  # flags

    # Write field dictionary
    w.write_uvarint(len(doc.field_dict))
    for field in doc.field_dict:
        w.write_string(field)

    # Write terms
    w.write_uvarint(len(doc.terms))
    for term in doc.terms:
        w.write_string(term.term)
        w.write_string(str(term.iri))
        w.write_byte(int(term.flags))

    # Write IRIs
    w.write_uvarint(len(doc.iris))
    for iri in doc.iris:
        w.write_string(str(iri))

    # Write datatypes
    w.write_uvarint(len(doc.datatypes))
    for dt in doc.datatypes:
        w.write_string(str(dt))

    # Write root value
    _encode_value(w, doc.root, doc)

    return w.getvalue()


def _decode_value(r: BinaryReader, doc: LDDocument) -> Value:
    """Decode a single value with Cowrie-LD extensions."""
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
        items = [_decode_value(r, doc) for _ in range(count)]
        return Array(*items)

    elif tag == TAG_OBJECT:
        count = r.read_uvarint()
        members = []
        for _ in range(count):
            key_idx = r.read_uvarint()
            key = doc.get_field(key_idx) or ""
            val = _decode_value(r, doc)
            members.append((key, val))
        return Object(*members)

    elif tag == TAG_DECIMAL128:
        scale = r.read_byte()
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

    elif tag == TAG_IRI:
        # IRI reference
        iri_id = r.read_uvarint()
        iri = doc.get_iri(iri_id)
        # Return as LDValue wrapper
        ld_val = LDValue(value=String(str(iri) if iri else ""), is_iri=True, iri_id=iri_id)
        return ld_val

    elif tag == TAG_BNODE:
        # Blank node
        bnode_id = r.read_string()
        ld_val = LDValue(value=String(bnode_id), is_bnode=True, bnode_id=bnode_id)
        return ld_val

    else:
        raise ValueError(f"Unknown tag: 0x{tag:02x}")


def decode(data: bytes) -> LDDocument:
    """
    Decode Cowrie-LD binary data into an LDDocument.

    Args:
        data: Cowrie-LD binary data

    Returns:
        LDDocument with terms, IRIs, and root value
    """
    r = BinaryReader(data)

    # Read and verify magic
    magic = r.read(4)
    if magic != MAGIC:
        raise ValueError(f"Invalid magic: {magic!r}")

    # Read and verify version
    version = r.read_byte()
    if version != VERSION:
        raise ValueError(f"Unsupported version: {version}")

    # Read flags
    flags = r.read_byte()

    doc = LDDocument()

    # Read field dictionary
    field_count = r.read_uvarint()
    for _ in range(field_count):
        doc.add_field(r.read_string())

    # Read terms
    term_count = r.read_uvarint()
    for _ in range(term_count):
        term = r.read_string()
        iri = IRI(r.read_string())
        flags = TermFlags(r.read_byte())
        doc.add_term(term, iri, flags)

    # Read IRIs
    iri_count = r.read_uvarint()
    for _ in range(iri_count):
        doc.add_iri(IRI(r.read_string()))

    # Read datatypes
    dt_count = r.read_uvarint()
    for _ in range(dt_count):
        doc.add_datatype(IRI(r.read_string()))

    # Decode root value
    doc.root = _decode_value(r, doc)

    return doc


# === Compression ===

def encode_compressed(doc: LDDocument, level: int = 19, shuffle: bool = True) -> bytes:
    """
    Encode and compress a Cowrie-LD document with zstd.

    Args:
        doc: LDDocument to encode
        level: Compression level (1-22, default 19)
        shuffle: Apply byte shuffling for better compression (lossless)
    """
    if not HAS_ZSTD:
        raise ImportError("zstandard required. Install with: pip install zstandard")

    raw = encode(doc)

    # Apply byte shuffling (lossless)
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

    # Build output: magic + original_size + compressed
    result = bytearray()
    result.extend(magic)
    result.extend(struct.pack('<I', len(raw)))
    result.extend(compressed)

    return bytes(result)


def decode_compressed(data: bytes) -> LDDocument:
    """
    Decode Cowrie-LD data with auto-detection of compression.

    Handles 'SJLD', 'SJLZ', and 'SJLS' (shuffled) magic.
    """
    if data[:4] == MAGIC_SHUFFLED:
        # Shuffled + compressed
        if not HAS_ZSTD:
            raise ImportError("zstandard required. Install with: pip install zstandard")

        original_size = struct.unpack('<I', data[4:8])[0]

        if hasattr(zstd, 'ZstdDecompressor'):
            dctx = zstd.ZstdDecompressor()
            shuffled = dctx.decompress(data[8:], max_output_size=original_size)
        else:
            shuffled = zstd.decompress(data[8:])

        raw = byte_unshuffle(shuffled, 4)
        return decode(raw)

    elif data[:4] == MAGIC_COMPRESSED:
        # Compressed only
        if not HAS_ZSTD:
            raise ImportError("zstandard required. Install with: pip install zstandard")

        original_size = struct.unpack('<I', data[4:8])[0]

        if hasattr(zstd, 'ZstdDecompressor'):
            dctx = zstd.ZstdDecompressor()
            raw = dctx.decompress(data[8:], max_output_size=original_size)
        else:
            raw = zstd.decompress(data[8:])

        return decode(raw)

    elif data[:4] == MAGIC:
        return decode(data)
    else:
        raise ValueError(f"Invalid magic: {data[:4]!r}")
