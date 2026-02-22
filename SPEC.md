# Cowrie Wire Format Specification

## Overview

Cowrie is a binary serialization format for JSON-like data structures. Two variants exist:

- **Gen1**: Simple binary JSON with proto-tensors
- **Gen2**: Full Cowrie v2 with dictionary coding and ML extensions

## Gen1 Wire Format

Gen1 uses a simple tag-length-value encoding without a header.

### Type Tags

| Tag | Type | Encoding |
|-----|------|----------|
| 0x00 | Null | Tag only |
| 0x01 | False | Tag only |
| 0x02 | True | Tag only |
| 0x03 | Int64 | Tag + zigzag-encoded varint |
| 0x04 | Float64 | Tag + 8 bytes LE |
| 0x05 | String | Tag + length:varint + UTF-8 bytes |
| 0x06 | Bytes | Tag + length:varint + raw bytes |
| 0x07 | Array | Tag + count:varint + elements |
| 0x08 | Object | Tag + count:varint + (key + value) pairs |
| 0x09 | Int64Array | Tag + count:varint + count*8 bytes LE |
| 0x0A | Float64Array | Tag + count:varint + count*8 bytes LE |
| 0x0B | StringArray | Tag + count:varint + (length:varint + UTF-8)* |
| 0x10 | Node | Graph node (see below) |
| 0x11 | Edge | Graph edge (see below) |
| 0x12 | AdjList | CSR adjacency list (see below) |
| 0x13 | NodeBatch | Batch of nodes |
| 0x14 | EdgeBatch | Batch of edges |
| 0x15 | GraphShard | Graph shard with nodes, edges, meta |

### Graph Type Layouts (Gen1)

#### Node (0x10)
```
Tag(0x10) | id:zigzag-varint | labelLen:varint | labelBytes | propCount:varint | (keyLen:varint | keyBytes | value)*
```

#### Edge (0x11)
```
Tag(0x11) | src:zigzag-varint | dst:zigzag-varint | labelLen:varint | labelBytes | propCount:varint | (keyLen:varint | keyBytes | value)*
```

#### AdjList (0x12)
```
Tag(0x12) | idWidth:u8 (1=int32, 2=int64) | nodeCount:varint | edgeCount:varint | rowOffsets:(nodeCount+1)*varint | colIndices:edgeCount*(4|8 bytes based on idWidth)
```

#### NodeBatch (0x13)
```
Tag(0x13) | count:varint | Node*
```

#### EdgeBatch (0x14)
```
Tag(0x14) | count:varint | Edge*
```

#### GraphShard (0x15)
```
Tag(0x15) | nodeCount:varint | Node* | edgeCount:varint | Edge* | metaCount:varint | (keyLen:varint | keyBytes | value)*
```

### Varint Encoding

Unsigned varints use the same encoding as Protocol Buffers:
- Each byte uses 7 bits for data and 1 bit (MSB) as continuation flag
- MSB=1 means more bytes follow, MSB=0 means this is the last byte

### Zigzag Encoding

Signed integers use zigzag encoding:
```
encode(n) = (n << 1) ^ (n >> 63)
decode(z) = (z >> 1) ^ -(z & 1)
```

### Object Keys

In Gen1, object keys are encoded inline with each field:
```
Object: Tag(0x08) | count:varint | (keyLen:varint | keyBytes | value)*
```

## Gen1 vs Gen2 Compatibility

**IMPORTANT**: Gen1 and Gen2 use different tag assignments for several types.
A decoder MUST check for the Gen2 magic header ("SJ") before decoding.

| Tag | Gen1 Type | Gen2 Type |
|-----|-----------|-----------|
| 0x00-0x05 | Same | Same |
| 0x06 | **Bytes** | **Array** |
| 0x07 | **Array** | **Object** (dict-coded) |
| 0x08 | **Object** | **Bytes** |
| 0x09 | **Int64Array** | Uint64 |
| 0x0A | **Float64Array** | Decimal128 |
| 0x0B | **StringArray** | Datetime64 |

To distinguish between formats:
1. Check first two bytes for "SJ" (0x53 0x4A)
2. If present → Gen2 format (version in byte 3)
3. If absent → Gen1 format (first byte is root value tag)

## Gen2 Wire Format

Gen2 adds a header, dictionary coding, and extended types.

### Header

```
Bytes 0-1: Magic "SJ" (0x53, 0x4A)
Byte 2:    Version (0x02)
Byte 3:    Flags
           - Bit 0: Compressed
           - Bits 1-2: Compression type (0=none, 1=gzip, 2=zstd)
           - Bit 3: Has column hints
Bytes 4+:  [ColumnHints] (if FlagHasColumnHints set)
           DictLen:varint
           Dict: DictLen * (len:varint + UTF-8 bytes)
           RootValue: encoded value
```

### Header Flags

Flags are bit-packed in byte 3 of the header:

| Bit(s) | Name | Meaning |
|--------|------|---------|
| 0 | Compressed | If set, payload is compressed and framed (see Compression Framing). |
| 1-2 | Compression type | 0=none, 1=gzip, 2=zstd. Only valid if Compressed=1. |
| 3 | Has column hints | If set, a ColumnHints block appears before DictLen. |

### Column Hints

Column hints are optional metadata for columnar readers. They appear immediately
after the header **only** when FlagHasColumnHints is set, and **before** DictLen.

```
ColumnHints:
  HintCount: varint
  Repeat HintCount times:
    Field: [len:varint][utf8 bytes]
    Type:  u8
    ShapeLen: varint
    ShapeDims: ShapeLen * varint
    Flags: u8
```

If the flag is set, decoders MUST skip or parse this block before reading DictLen.
Malformed hints MUST result in a decode error.

### Type Tags

| Tag | Type | Encoding |
|-----|------|----------|
| 0x00 | Null | Tag only |
| 0x01 | False | Tag only |
| 0x02 | True | Tag only |
| 0x03 | Int64 | Tag + zigzag-encoded varint |
| 0x04 | Float64 | Tag + 8 bytes LE |
| 0x05 | String | Tag + length:varint + UTF-8 bytes |
| 0x06 | Array | Tag + count:varint + elements |
| 0x07 | Object | Tag + count:varint + (dictIndex:varint + value)* |
| 0x08 | Bytes | Tag + length:varint + raw bytes |
| 0x09 | Uint64 | Tag + varint |
| 0x0A | Decimal128 | Tag + scale:int8 + coef:16 bytes |
| 0x0B | Datetime64 | Tag + nanos:int64 LE |
| 0x0C | UUID128 | Tag + 16 bytes |
| 0x0D | BigInt | Tag + length:varint + two's complement bytes |
| 0x0E | Extension | Tag + extType:varint + length:varint + payload |

### ML Extension Tags (0x20-0x2F)

| Tag | Type | Encoding |
|-----|------|----------|
| 0x20 | Tensor | dtype:u8 + rank:u8 + dims:varint* + dataLen:varint + data |
| 0x21 | TensorRef | storeId:u8 + keyLen:varint + key |
| 0x22 | Image | format:u8 + width:u16 LE + height:u16 LE + dataLen:varint + data |
| 0x23 | Audio | encoding:u8 + sampleRate:u32 LE + channels:u8 + dataLen:varint + data |

### Delta/RichText Tags (0x30-0x34)

| Tag | Type | Encoding |
|-----|------|----------|
| 0x30 | AdjList | idWidth:u8 + nodeCount:varint + edgeCount:varint + rowOffsets + colIndices |
| 0x31 | RichText | text:string + flags:u8 + [tokens:count + int32*] + [spans:count + (start,end,kind)*] |
| 0x32 | Delta | baseId:varint + opCount:varint + ops |

### Graph Extension Tags (0x35-0x39)

| Tag | Type | Encoding |
|-----|------|----------|
| 0x35 | Node | id:string + labelCount:varint + labels:string* + propCount:varint + (dictIdx:varint + value)* |
| 0x36 | Edge | srcId:string + dstId:string + type:string + propCount:varint + (dictIdx:varint + value)* |
| 0x37 | NodeBatch | count:varint + Node[count] |
| 0x38 | EdgeBatch | count:varint + Edge[count] |
| 0x39 | GraphShard | nodeCount:varint + Node* + edgeCount:varint + Edge* + metaCount:varint + (dictIdx:varint + value)* |

### TagExt (Extension Envelope)

TagExt (0x0E) is a forward-compatibility envelope:

```
TagExt | ExtType:uvarint | Len:uvarint | Payload:Len bytes
```

Unknown ExtType handling is implementation-defined but must be one of:
1) Keep (preserve payload and type for round-trip),
2) Skip (return Null),
3) Error (strict mode).

### Compression Framing Rules

When the Compressed flag is set, the payload after the 4-byte header is:

```
OrigLen: uvarint
CompressedPayload: bytes
```

Rules:
1. Decode must reject if OrigLen exceeds MaxDecompressedSize.
2. Decompression must be size-bounded (do not allocate unbounded buffers).
3. If decompressed length != OrigLen, return ERR_DECOMPRESSED_MISMATCH.
4. If compression type is unknown, return ERR_UNSUPPORTED_COMPRESSION.

### Security Limits and Decode Options

Default limits are designed to prevent memory exhaustion while allowing ML payloads.
Implementations may allow overrides; “unlimited” must be explicit (not default).

| Limit | Default | Meaning |
|-------|---------|---------|
| MaxDepth | 1000 | Maximum nested depth (arrays/objects) |
| MaxArrayLen | 100,000,000 | Maximum array element count |
| MaxObjectLen | 10,000,000 | Maximum object field count |
| MaxStringLen | 500,000,000 | Maximum string length (bytes) |
| MaxBytesLen | 1,000,000,000 | Maximum bytes length (also tensor/image/audio) |
| MaxDictLen | 10,000,000 | Maximum dictionary entries |
| MaxExtLen | 100,000,000 | Maximum TagExt payload length |
| MaxHintCount | 10,000 | Maximum column hints |
| MaxRank | 32 | Maximum tensor rank (dims count) |

### Tensor Rank Limits

Tensor rank is encoded as **u8** in the wire format. Decoders MUST:
1) Reject rank > MaxRank (default 32), and
2) Reject rank > 255 (wire limit).

### Error Codes (Canonical for Fixtures)

Fixture tests use the following canonical error codes. Implementations may map
their internal errors to these codes for fixture validation:

- ERR_INVALID_MAGIC
- ERR_INVALID_VERSION
- ERR_TRUNCATED
- ERR_INVALID_TAG
- ERR_INVALID_FIELD_ID
- ERR_INVALID_UTF8
- ERR_INVALID_VARINT
- ERR_TOO_DEEP
- ERR_TOO_LARGE
- ERR_DICT_TOO_LARGE
- ERR_STRING_TOO_LARGE
- ERR_BYTES_TOO_LARGE
- ERR_EXT_TOO_LARGE
- ERR_RANK_TOO_LARGE
- ERR_UNSUPPORTED_COMPRESSION
- ERR_DECOMPRESSED_TOO_LARGE
- ERR_DECOMPRESSED_MISMATCH
- ERR_UNKNOWN_EXTENSION

### Graph Type Layouts (Gen2)

Graph types in Gen2 use dictionary-coded property keys for efficient encoding.

#### Node (0x35)
```
Tag(0x35) | idLen:varint | idBytes | labelCount:varint | (labelLen:varint | labelBytes)* | propCount:varint | (dictIdx:varint | value)*
```

**Properties**: Use dictionary indices (same as Object type), enabling 70-80% size reduction for repeated property keys.

**Example**:
```json
{
  "id": "person_42",
  "labels": ["Person", "Employee"],
  "props": {"name": "Alice", "age": 30, "salary": 50000}
}
```
If dictionary = ["name", "age", "salary"], encoded props use indices 0, 1, 2 instead of string keys.

#### Edge (0x36)
```
Tag(0x36) | srcLen:varint | srcBytes | dstLen:varint | dstBytes | typeLen:varint | typeBytes | propCount:varint | (dictIdx:varint | value)*
```

**Example**:
```json
{
  "from": "person_42",
  "to": "company_1",
  "type": "WORKS_AT",
  "props": {"since": 2020, "role": "Engineer"}
}
```

#### NodeBatch (0x37)
```
Tag(0x37) | count:varint | Node[0] | Node[1] | ... | Node[count-1]
```

Used for streaming GNN mini-batches. Dictionary is shared across all nodes in the batch.

#### EdgeBatch (0x38)
```
Tag(0x38) | count:varint | Edge[0] | Edge[1] | ... | Edge[count-1]
```

Used for bulk edge loading. Edges encoded in COO (Coordinate) format.

#### GraphShard (0x39)
```
Tag(0x39) | nodeCount:varint | Node* | edgeCount:varint | Edge* | metaCount:varint | (dictIdx:varint | value)*
```

Self-contained subgraph for:
- GNN mini-batch checkpointing
- Distributed graph processing
- Graph database snapshots
- Streaming graph partitions

**Example**:
```json
{
  "nodes": [
    {"id": "1", "labels": ["Node"], "props": {"x": 0.1}},
    {"id": "2", "labels": ["Node"], "props": {"x": 0.2}}
  ],
  "edges": [
    {"from": "1", "to": "2", "type": "EDGE", "props": {"weight": 0.85}}
  ],
  "metadata": {"version": 1, "partitionId": 42}
}
```

### Dictionary Integration for Graph Types

Graph property keys are collected into the shared dictionary during the first encoding pass:

1. Traverse all Node.props keys
2. Traverse all Edge.props keys
3. Traverse all GraphShard.metadata keys
4. Add to dictionary (same as Object keys)

This enables significant size savings when graphs have many nodes/edges with repeated property schemas.

### Dictionary Coding

In Gen2, object keys are dictionary-coded:
1. All unique keys are collected before encoding
2. Keys are stored in the header dictionary
3. Objects reference keys by dictionary index (varint)

This significantly reduces size for objects with repeated key patterns.

### Compression

When the compressed flag is set:
1. Everything after the header is compressed
2. Original uncompressed length is stored as varint after flags
3. Compression types: 1=gzip, 2=zstd

### Tensor DTypes

| Code | Type | Size |
|------|------|------|
| 0x01 | float32 | 4 |
| 0x02 | float16 | 2 |
| 0x03 | bfloat16 | 2 |
| 0x04 | int8 | 1 |
| 0x05 | int16 | 2 |
| 0x06 | int32 | 4 |
| 0x07 | int64 | 8 |
| 0x08 | uint8 | 1 |
| 0x09 | uint16 | 2 |
| 0x0A | uint32 | 4 |
| 0x0B | uint64 | 8 |
| 0x0C | float64 | 8 |

### Image Formats

| Code | Format |
|------|--------|
| 0x01 | JPEG |
| 0x02 | PNG |
| 0x03 | WebP |
| 0x04 | AVIF |
| 0x05 | BMP |

### Audio Encodings

| Code | Encoding |
|------|----------|
| 0x01 | PCM Int16 |
| 0x02 | PCM Float32 |
| 0x03 | Opus |
| 0x04 | AAC |

### Array Promotion Threshold

When encoding, implementations may automatically promote homogeneous numeric arrays
to proto-tensor types (Int64Array, Float64Array) for efficiency. The recommended
threshold is **4 elements** - arrays with 4 or more elements of the same numeric type
should be encoded as proto-tensors. Arrays with fewer elements should use the generic
Array type.

This is a recommendation, not a requirement. Implementations may choose different
thresholds based on their use case.

## Security Limits

Decoders should enforce limits to prevent denial-of-service:

| Limit | Default |
|-------|---------|
| Max nesting depth | 1000 |
| Max array length | 100M |
| Max object fields | 10M |
| Max string length | 500MB |
| Max bytes length | 1GB |
| Max extension length | 100MB |
| Max dictionary size | 10M |
| Max tensor rank | 32 |

## Schema Fingerprinting

Gen2 supports schema fingerprinting using FNV-1a (64-bit):
- Fingerprint captures type structure but not values
- Keys are sorted for deterministic output
- Useful for type routing and schema drift detection

```
fingerprint = FNV-1a(type_structure)
fingerprint32 = fingerprint & 0xFFFFFFFF
```
