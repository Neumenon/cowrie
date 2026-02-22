"""
GraphCowrie-Stream Writer/Reader - Streaming graph event encoder/decoder.

Wire format:
    Magic:     'S' 'J' 'G' 'S'  (4 bytes)
    Version:   0x01             (1 byte)
    Flags:     bitfield         (1 byte)
    FieldDict: [count:uvarint][entries...]
    LabelDict: [count:uvarint][entries...]
    PredDict:  [count:uvarint][entries...]  (if FLAG_HAS_PRED_DICT)
    Frames:    [len:u32][frameBody]...

Compressed format (SJGZ):
    Magic:     'S' 'J' 'G' 'Z'  (4 bytes)
    OrigSize:  u32              (original size)
    Data:      zstd-compressed stream
"""

import struct
from io import BytesIO
from typing import Iterator, Any, Dict, List, Optional

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

MAGIC_COMPRESSED = b'SJGZ'
MAGIC_SHUFFLED = b'SJGH'  # Shuffled + compressed


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


from .types import (
    MAGIC, VERSION,
    FLAG_HAS_PRED_DICT, FLAG_COMPRESSED, FLAG_HAS_TIMESTAMP,
    EventKind, Op, TermKind,
    Event, NodeEvent, EdgeEvent, TripleEvent, RDFTerm,
    StreamHeader,
)


class BinaryWriter:
    """Low-level binary writer."""

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

    def write_u32_le(self, v: int):
        self.buf.write(struct.pack('<I', v))

    def write_u64_le(self, v: int):
        self.buf.write(struct.pack('<Q', v))

    def getvalue(self) -> bytes:
        return self.buf.getvalue()


class BinaryReader:
    """Low-level binary reader."""

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def remaining(self) -> int:
        return len(self.data) - self.pos

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

    def read_u32_le(self) -> int:
        data = self.read(4)
        return struct.unpack('<I', data)[0]

    def read_u64_le(self) -> int:
        data = self.read(8)
        return struct.unpack('<Q', data)[0]


class StreamWriter:
    """
    Write graph events to GraphCowrie-Stream format.

    Usage:
        writer = StreamWriter()
        writer.write_node(NodeEvent(op=Op.UPSERT, id="n1", labels=["Person"]))
        writer.write_edge(EdgeEvent(op=Op.UPSERT, label="KNOWS", from_id="n1", to_id="n2"))
        data = writer.getvalue()
    """

    def __init__(self, include_timestamps: bool = False):
        self.header = StreamHeader()
        self.frames: List[bytes] = []
        self.include_timestamps = include_timestamps
        if include_timestamps:
            self.header.flags |= FLAG_HAS_TIMESTAMP

    def write_node(self, event: NodeEvent):
        """Write a node event."""
        frame = self._encode_node_frame(event)
        self.frames.append(frame)

    def write_edge(self, event: EdgeEvent):
        """Write an edge event."""
        frame = self._encode_edge_frame(event)
        self.frames.append(frame)

    def write_triple(self, event: TripleEvent):
        """Write a triple event."""
        frame = self._encode_triple_frame(event)
        self.frames.append(frame)

    def write_event(self, event: Event):
        """Write any event."""
        if event.kind == EventKind.NODE and event.node:
            self.write_node(event.node)
        elif event.kind == EventKind.EDGE and event.edge:
            self.write_edge(event.edge)
        elif event.kind == EventKind.TRIPLE and event.triple:
            self.write_triple(event.triple)

    def _encode_node_frame(self, event: NodeEvent) -> bytes:
        """Encode a node event to frame bytes."""
        w = BinaryWriter()

        # Event kind + op
        w.write_byte((EventKind.NODE << 4) | event.op)

        # Node ID
        w.write_string(event.id)

        # Labels (as indices)
        w.write_uvarint(len(event.labels))
        for label in event.labels:
            idx = self.header.add_label(label)
            w.write_uvarint(idx)

        # Properties
        self._encode_props(w, event.props)

        # Timestamp (if enabled)
        if self.include_timestamps:
            w.write_u64_le(event.timestamp)

        return w.getvalue()

    def _encode_edge_frame(self, event: EdgeEvent) -> bytes:
        """Encode an edge event to frame bytes."""
        w = BinaryWriter()

        # Event kind + op
        w.write_byte((EventKind.EDGE << 4) | event.op)

        # Edge ID (optional)
        w.write_string(event.id)

        # Label (as index)
        idx = self.header.add_label(event.label)
        w.write_uvarint(idx)

        # From/To IDs
        w.write_string(event.from_id)
        w.write_string(event.to_id)

        # Properties
        self._encode_props(w, event.props)

        # Timestamp
        if self.include_timestamps:
            w.write_u64_le(event.timestamp)

        return w.getvalue()

    def _encode_triple_frame(self, event: TripleEvent) -> bytes:
        """Encode a triple event to frame bytes."""
        w = BinaryWriter()

        # Event kind + op
        w.write_byte((EventKind.TRIPLE << 4) | event.op)

        # Subject
        self._encode_term(w, event.subject)

        # Predicate (as index)
        idx = self.header.add_predicate(event.predicate)
        w.write_uvarint(idx)

        # Object
        self._encode_term(w, event.object)

        # Graph (optional)
        w.write_string(event.graph)

        # Timestamp
        if self.include_timestamps:
            w.write_u64_le(event.timestamp)

        return w.getvalue()

    def _encode_term(self, w: BinaryWriter, term: RDFTerm):
        """Encode an RDF term."""
        w.write_byte(term.kind)
        w.write_string(term.value)
        if term.kind == TermKind.LITERAL:
            w.write_string(term.datatype)
            w.write_string(term.lang)

    def _encode_props(self, w: BinaryWriter, props: Dict[str, Any]):
        """Encode properties as field_idx:value pairs."""
        w.write_uvarint(len(props))
        for key, value in props.items():
            field_idx = self.header.add_field(key)
            w.write_uvarint(field_idx)
            self._encode_value(w, value)

    def _encode_value(self, w: BinaryWriter, value: Any):
        """Encode a property value (simple types only)."""
        if value is None:
            w.write_byte(0)  # null
        elif isinstance(value, bool):
            w.write_byte(1 if value else 2)  # true/false
        elif isinstance(value, int):
            w.write_byte(3)  # int64
            # Zigzag encode
            zz = (value << 1) ^ (value >> 63)
            w.write_uvarint(zz)
        elif isinstance(value, float):
            w.write_byte(4)  # float64
            w.write(struct.pack('<d', value))
        elif isinstance(value, str):
            w.write_byte(5)  # string
            w.write_string(value)
        elif isinstance(value, bytes):
            w.write_byte(6)  # bytes
            w.write_uvarint(len(value))
            w.write(value)
        elif isinstance(value, list):
            w.write_byte(7)  # array
            w.write_uvarint(len(value))
            for item in value:
                self._encode_value(w, item)
        else:
            # Convert to string as fallback
            w.write_byte(5)
            w.write_string(str(value))

    def getvalue(self) -> bytes:
        """Get the complete stream as bytes."""
        w = BinaryWriter()

        # Magic
        w.write(MAGIC)

        # Version
        w.write_byte(VERSION)

        # Flags
        w.write_byte(self.header.flags)

        # Field dictionary
        w.write_uvarint(len(self.header.field_dict))
        for field in self.header.field_dict:
            w.write_string(field)

        # Label dictionary
        w.write_uvarint(len(self.header.label_dict))
        for label in self.header.label_dict:
            w.write_string(label)

        # Predicate dictionary (if present)
        if self.header.flags & FLAG_HAS_PRED_DICT:
            w.write_uvarint(len(self.header.pred_dict))
            for pred in self.header.pred_dict:
                w.write_string(pred)

        # Frames
        for frame in self.frames:
            w.write_u32_le(len(frame))
            w.write(frame)

        return w.getvalue()

    def getvalue_compressed(self, level: int = 19, shuffle: bool = True) -> bytes:
        """
        Get the complete stream as zstd-compressed bytes.

        Args:
            level: Compression level (1-22, default 19)
            shuffle: Apply byte shuffling for better compression (lossless)
        """
        if not HAS_ZSTD:
            raise ImportError("zstandard required. Install with: pip install zstandard")

        raw = self.getvalue()

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


class StreamReader:
    """
    Read graph events from GraphCowrie-Stream format.

    Auto-detects compression: handles both 'SJGS' and 'SJGZ' magic.

    Usage:
        for event in StreamReader(data):
            if event.kind == EventKind.NODE:
                print(event.node)
            elif event.kind == EventKind.EDGE:
                print(event.edge)
    """

    def __init__(self, data: bytes):
        # Auto-detect and decompress
        data = self._maybe_decompress(data)
        self.r = BinaryReader(data)
        self.header = StreamHeader()
        self._read_header()

    def _maybe_decompress(self, data: bytes) -> bytes:
        """Decompress if needed. Auto-detects compression and shuffling."""
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

            return byte_unshuffle(shuffled, 4)

        elif data[:4] == MAGIC_COMPRESSED:
            # Compressed only
            if not HAS_ZSTD:
                raise ImportError("zstandard required. Install with: pip install zstandard")

            original_size = struct.unpack('<I', data[4:8])[0]

            if hasattr(zstd, 'ZstdDecompressor'):
                dctx = zstd.ZstdDecompressor()
                return dctx.decompress(data[8:], max_output_size=original_size)
            else:
                return zstd.decompress(data[8:])

        return data

    def _read_header(self):
        """Read and validate stream header."""
        # Magic
        magic = self.r.read(4)
        if magic != MAGIC:
            raise ValueError(f"Invalid magic: {magic!r}")

        # Version
        version = self.r.read_byte()
        if version != VERSION:
            raise ValueError(f"Unsupported version: {version}")

        # Flags
        self.header.flags = self.r.read_byte()

        # Field dictionary
        field_count = self.r.read_uvarint()
        for _ in range(field_count):
            self.header.add_field(self.r.read_string())

        # Label dictionary
        label_count = self.r.read_uvarint()
        for _ in range(label_count):
            self.header.add_label(self.r.read_string())

        # Predicate dictionary
        if self.header.flags & FLAG_HAS_PRED_DICT:
            pred_count = self.r.read_uvarint()
            for _ in range(pred_count):
                self.header.add_predicate(self.r.read_string())

    def __iter__(self) -> Iterator[Event]:
        """Iterate over all events in the stream."""
        while self.r.remaining() >= 4:
            frame_len = self.r.read_u32_le()
            if frame_len == 0:
                break
            frame_data = self.r.read(frame_len)
            yield self._decode_frame(frame_data)

    def _decode_frame(self, data: bytes) -> Event:
        """Decode a single frame."""
        r = BinaryReader(data)

        # Event kind + op
        kind_op = r.read_byte()
        kind = EventKind(kind_op >> 4)
        op = Op(kind_op & 0x0F)

        if kind == EventKind.NODE:
            return Event.from_node(self._decode_node(r, op))
        elif kind == EventKind.EDGE:
            return Event.from_edge(self._decode_edge(r, op))
        elif kind == EventKind.TRIPLE:
            return Event.from_triple(self._decode_triple(r, op))
        else:
            raise ValueError(f"Unknown event kind: {kind}")

    def _decode_node(self, r: BinaryReader, op: Op) -> NodeEvent:
        """Decode a node event."""
        # Node ID
        node_id = r.read_string()

        # Labels
        label_count = r.read_uvarint()
        labels = []
        for _ in range(label_count):
            idx = r.read_uvarint()
            labels.append(self.header.get_label(idx))

        # Properties
        props = self._decode_props(r)

        # Timestamp
        timestamp = 0
        if self.header.flags & FLAG_HAS_TIMESTAMP:
            timestamp = r.read_u64_le()

        return NodeEvent(op=op, id=node_id, labels=labels, props=props, timestamp=timestamp)

    def _decode_edge(self, r: BinaryReader, op: Op) -> EdgeEvent:
        """Decode an edge event."""
        # Edge ID
        edge_id = r.read_string()

        # Label
        label_idx = r.read_uvarint()
        label = self.header.get_label(label_idx)

        # From/To IDs
        from_id = r.read_string()
        to_id = r.read_string()

        # Properties
        props = self._decode_props(r)

        # Timestamp
        timestamp = 0
        if self.header.flags & FLAG_HAS_TIMESTAMP:
            timestamp = r.read_u64_le()

        return EdgeEvent(op=op, id=edge_id, label=label, from_id=from_id,
                        to_id=to_id, props=props, timestamp=timestamp)

    def _decode_triple(self, r: BinaryReader, op: Op) -> TripleEvent:
        """Decode a triple event."""
        # Subject
        subject = self._decode_term(r)

        # Predicate
        pred_idx = r.read_uvarint()
        predicate = self.header.get_predicate(pred_idx)

        # Object
        obj = self._decode_term(r)

        # Graph
        graph = r.read_string()

        # Timestamp
        timestamp = 0
        if self.header.flags & FLAG_HAS_TIMESTAMP:
            timestamp = r.read_u64_le()

        return TripleEvent(op=op, subject=subject, predicate=predicate,
                          object=obj, graph=graph, timestamp=timestamp)

    def _decode_term(self, r: BinaryReader) -> RDFTerm:
        """Decode an RDF term."""
        kind = TermKind(r.read_byte())
        value = r.read_string()
        datatype = ""
        lang = ""
        if kind == TermKind.LITERAL:
            datatype = r.read_string()
            lang = r.read_string()
        return RDFTerm(kind=kind, value=value, datatype=datatype, lang=lang)

    def _decode_props(self, r: BinaryReader) -> Dict[str, Any]:
        """Decode properties."""
        count = r.read_uvarint()
        props = {}
        for _ in range(count):
            field_idx = r.read_uvarint()
            key = self.header.get_field(field_idx)
            value = self._decode_value(r)
            props[key] = value
        return props

    def _decode_value(self, r: BinaryReader) -> Any:
        """Decode a property value."""
        tag = r.read_byte()
        if tag == 0:  # null
            return None
        elif tag == 1:  # true
            return True
        elif tag == 2:  # false
            return False
        elif tag == 3:  # int64
            zz = r.read_uvarint()
            return (zz >> 1) ^ -(zz & 1)
        elif tag == 4:  # float64
            return struct.unpack('<d', r.read(8))[0]
        elif tag == 5:  # string
            return r.read_string()
        elif tag == 6:  # bytes
            length = r.read_uvarint()
            return r.read(length)
        elif tag == 7:  # array
            count = r.read_uvarint()
            return [self._decode_value(r) for _ in range(count)]
        else:
            raise ValueError(f"Unknown value tag: {tag}")
