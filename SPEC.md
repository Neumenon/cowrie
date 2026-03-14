# Cowrie Wire Format Specification

## Overview

Cowrie is a binary serialization format for JSON-like data structures. Two variants exist:

- **Gen1**: Simple binary JSON with proto-tensors
- **Gen2**: Full Cowrie v2 with dictionary coding and ML extensions

## Gen1 Wire Format

Gen1 uses a simple tag-length-value encoding without a header.

### Unified Type Tags (0x00-0x0F)

These tags are shared between Gen1 and Gen2. Both formats use the same tag numbers
for the same types.

| Tag | Type | Encoding |
|-----|------|----------|
| 0x00 | Null | Tag only |
| 0x01 | False | Tag only |
| 0x02 | True | Tag only |
| 0x03 | Int64 | Tag + zigzag-encoded varint |
| 0x04 | Float64 | Tag + 8 bytes LE |
| 0x05 | String | Tag + length:varint + UTF-8 bytes |
| 0x06 | Array | Tag + count:varint + elements |
| 0x07 | Object | Tag + count:varint + (key + value) pairs |
| 0x08 | Bytes | Tag + length:varint + raw bytes |
| 0x09 | Uint64 | Tag + varint (unsigned, no zigzag) |
| 0x0A | Decimal128 | Tag + scale:int8 + coefficient:16 bytes |
| 0x0B | Datetime64 | Tag + nanos:int64 LE |
| 0x0C | UUID128 | Tag + 16 bytes |
| 0x0D | BigInt | Tag + length:varint + two's complement bytes |
| 0x0E | Extension | Tag + extType:varint + length:varint + payload |
| 0x0F | Float32 | Tag + 4 bytes LE (compact float, decodes as float64) |

### Proto-Tensor Tags (0x16-0x19)

Gen1-specific compact encoding for homogeneous arrays. Gen2 uses Tensor (0x20) instead.

| Tag | Type | Encoding |
|-----|------|----------|
| 0x16 | Int64Array | Tag + count:varint + count×8 bytes LE |
| 0x17 | Float64Array | Tag + count:varint + count×8 bytes LE |
| 0x18 | Float32Array | Tag + count:varint + count×4 bytes LE |
| 0x19 | StringArray | Tag + count:varint + (length:varint + UTF-8)* |

### Graph Type Layouts (Gen1)

Gen1 graph types use inline keys (not dictionary-coded). Tags are shared with Gen2.

#### Node (0x35)
```
Tag(0x35) | id:zigzag-varint | labelLen:varint | labelBytes | propCount:varint | (keyLen:varint | keyBytes | value)*
```

#### Edge (0x36)
```
Tag(0x36) | src:zigzag-varint | dst:zigzag-varint | labelLen:varint | labelBytes | propCount:varint | (keyLen:varint | keyBytes | value)*
```

#### AdjList (0x30)
```
Tag(0x30) | idWidth:u8 (1=int32, 2=int64) | nodeCount:varint | edgeCount:varint | rowOffsets:(nodeCount+1)*varint | colIndices:edgeCount*(4|8 bytes based on idWidth)
```

#### NodeBatch (0x37)
```
Tag(0x37) | count:varint | Node*
```

#### EdgeBatch (0x38)
```
Tag(0x38) | count:varint | Edge*
```

#### GraphShard (0x39)
```
Tag(0x39) | nodeCount:varint | Node* | edgeCount:varint | Edge* | metaCount:varint | (keyLen:varint | keyBytes | value)*
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
Object: Tag(0x07) | count:varint | (keyLen:varint | keyBytes | value)*
```

## Format Detection

Gen2 starts with a 4-byte header. Gen1 has no header — the first byte is the root value's tag.

```
if bytes[0] == 0x53 && bytes[1] == 0x4A && bytes[2] == 0x02:
    → Gen2 (magic "SJ", version 0x02)
else:
    → Gen1 (first byte is root tag 0x00-0x19 or FIXINT 0x40+)
```

Three bytes are needed because FIXINT can produce 0x53 ('S') and 0x4A ('J') for integers 19 and 10.

## Gen1 vs Gen2 Tag Usage

Tags 0x00-0x0F are unified — both formats use the same tag numbers for the same types.
The formats differ only in header presence and which tag ranges they use:

| Range | Gen1 | Gen2 |
|-------|------|------|
| 0x00-0x0F | Core types (unified) | Core types (unified) |
| 0x16-0x19 | Proto-tensor arrays | — (use Tensor 0x20 instead) |
| 0x20-0x24 | — | ML extensions (Tensor, Image, Audio, Bitmask) |
| 0x30-0x39 | Graph types (inline keys) | Graph types (dict-coded keys) + Delta/RichText |
| 0x40-0xEF | FIXINT/FIXARRAY/FIXMAP/FIXNEG | FIXINT/FIXARRAY/FIXMAP/FIXNEG |

Gen2 adds dictionary coding (header contains string dictionary, Object/Graph keys use dictionary indices)
and a header with magic bytes, version, and compression flags. Gen1 has none of this overhead.

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
| 0x0F | Float32 | Tag + 4 bytes LE (compact float, decodes as float64) |

### ML Extension Tags (0x20-0x2F)

| Tag | Type | Encoding |
|-----|------|----------|
| 0x20 | Tensor | dtype:u8 + rank:u8 + dims:varint* + dataLen:varint + data |
| 0x21 | TensorRef | storeId:u8 + keyLen:varint + key |
| 0x22 | Image | format:u8 + width:u16 LE + height:u16 LE + dataLen:varint + data |
| 0x23 | Audio | encoding:u8 + sampleRate:u32 LE + channels:u8 + dataLen:varint + data |
| 0x24 | Bitmask | count:uvarint + ceil(count/8) packed bytes (v3) |

#### Bitmask (0x24)

```
Tag(0x24) | count:uvarint | packed_bytes:ceil(count/8) bytes
```

Bit ordering: LSB-first within each byte. Bit `i` is at `bytes[i/8] & (1 << (i%8))`.

Use cases: attention masks, padding masks, train/val/test splits, boolean selections.
A 2048-element mask encodes in 259 bytes vs ~4000+ bytes as an int array.

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

### v3 Inline Types (0x40-0xEF)

v3 adds inline encoding for small integers, arrays, objects, and negative integers.
These tags encode the value directly in the tag byte, saving 1-2 bytes per value.

| Tag Range | Type | Decoding |
|-----------|------|----------|
| 0x40-0xBF | FIXINT | value = tag - 0x40 (0 to 127) |
| 0xC0-0xCF | FIXARRAY | count = tag - 0xC0 (0 to 15), followed by count values |
| 0xD0-0xDF | FIXMAP | count = tag - 0xD0 (0 to 15), followed by count (dictIdx + value) pairs |
| 0xE0-0xEF | FIXNEG | value = -1 - (tag - 0xE0) (-1 to -16) |
| 0xF0-0xFF | RESERVED | Must be rejected as invalid |

#### Encoding Rules

- **Integers 0-127**: MUST use FIXINT (single byte) instead of TagInt64
- **Integers -1 to -16**: MUST use FIXNEG (single byte) instead of TagInt64
- **Integers outside [-16, 127]**: Use TagInt64 with zigzag varint as before
- **Arrays with 0-15 elements**: MUST use FIXARRAY instead of TagArray
- **Arrays with 16+ elements**: Use TagArray with uvarint count as before
- **Objects with 0-15 fields**: MUST use FIXMAP instead of TagObject
- **Objects with 16+ fields**: Use TagObject with uvarint count as before

#### Wire Savings

| Scenario | Before (v2) | After (v3) | Savings |
|----------|------------|------------|---------|
| Int 42 | 2 bytes (tag + varint) | 1 byte (fixint) | 50% |
| Int -1 | 2 bytes (tag + varint) | 1 byte (fixneg) | 50% |
| Array [1,2,3] | 5 bytes (tag + count + 3×2) | 4 bytes (fixarray + 3×1) | 20% |
| Object {"a":1} | 4 bytes (tag + count + idx + 2) | 3 bytes (fixmap + idx + 1) | 25% |

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

When encoding, Gen1 implementations may automatically promote homogeneous arrays
to proto-tensor types (Int64Array 0x16, Float64Array 0x17, Float32Array 0x18,
StringArray 0x19) for efficiency. The recommended threshold is **4 elements** —
arrays with 4 or more elements of the same type should be encoded as proto-tensors.
Arrays with fewer elements should use the generic Array type (0x06).

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
