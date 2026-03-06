# Cowrie: A Binary JSON Codec for 5 Languages, No Code Generation Required

*TL;DR: Cowrie is a binary codec for JSON-like data with native ML types (tensors, images, audio), dictionary coding for ~50% size reduction, and graph types for GNN workloads. Implementations in Go, Rust, Python, C, and TypeScript. Apache 2.0 licensed, no code generation, no schema files.*

---

## The Problem

If you've worked on a system where a Python ML service talks to a Go API server, you've probably hit this wall:

**Repeated keys eat your bandwidth.** A log pipeline sending 10,000 events per second with `{"timestamp": ..., "level": ..., "service": ..., "message": ...}` repeats those four keys 10,000 times. JSON doesn't care.

**ML types don't exist.** You want to send a 768-dimensional float32 embedding from Python to Go. Your options: base64-encode the raw bytes and wrap it in a JSON string (losing type information), or serialize to a nested JSON array of numbers (3-4x size overhead). Neither is good.

**Code generation adds friction.** Protobuf and FlatBuffers solve the size problem, but they require `.proto` files, generated code, and build pipeline integration. When your schema changes, you regenerate, recompile, redeploy. For teams that iterate fast on data shapes, this overhead slows you down.

**Graph data is an afterthought.** GNN training pipelines need to move nodes, edges, and adjacency structures between services. The standard approach is to flatten everything into JSON arrays and reconstruct the graph on the other side. It works, but it's tedious and error-prone.

Cowrie was built to solve these four problems together.

---

## The Solution: Two Variants for Different Needs

Cowrie has two wire formats, both schemaless:

### Gen1: Simple and Fast

Gen1 is a straightforward binary encoding for JSON types with a few additions. No header, no dictionary, no compression. It maps directly to what you'd expect:

- Null, Boolean, Int64, Float64, String, Bytes, Array, Object
- Proto-tensors: `Int64Array` and `Float64Array` for homogeneous numeric data
- Graph types: Node, Edge, NodeBatch, EdgeBatch, GraphShard, AdjList

For a small object like `{"name": "Alice", "age": 30, "active": true}`:
- JSON: 46 bytes
- Gen1: 35 bytes (76% of JSON)

Gen1 is best when you want something simpler than JSON with predictable single-pass encoding. Think embedded systems, real-time pipelines, or anywhere you want low overhead.

### Gen2: Dictionary Coding and ML Extensions

Gen2 starts with a 4-byte header (`SJ` magic + version + flags) and adds three capabilities:

1. **Dictionary coding**: Object keys are collected into a shared dictionary in the header. Each field reference is a varint index instead of a repeated string. For payloads with repeated schemas (arrays of objects with the same keys), this is significant.

2. **Compression**: The payload after the header can be gzip or zstd compressed. Combined with dictionary coding, this compounds well.

3. **ML extension types**: Tensor (with dtype, shape, and raw data), Image (format + dimensions + bytes), Audio (encoding + sample rate + channels + bytes).

For 1,000 objects with the same schema:
- JSON: 48KB
- Gen1: 34KB (70%)
- Gen2: 23KB (47%)

The 47% figure is from dictionary coding alone, without compression. With zstd, it goes lower.

---

## Code Examples

Cowrie works the same way in every language: encode a value, get bytes; decode bytes, get a value. No schema files, no generated code.

### Go

```go
import (
    "github.com/Neumenon/cowrie/gen1"
    "github.com/Neumenon/cowrie/gen2"
)

// Gen1
data, _ := gen1.Encode(map[string]any{
    "name": "Alice",
    "embedding": []float64{0.1, 0.2, 0.3},
})
result, _ := gen1.Decode(data)

// Gen2
val := gen2.Object(
    gen2.Member{Key: "name", Value: gen2.String("Alice")},
)
data, _ = gen2.Encode(val)
```

### Rust

```rust
use cowrie::{gen1, gen2};

// Gen1
let val = gen1::Value::Object(vec![
    ("name".to_string(), gen1::Value::String("test".to_string())),
]);
let encoded = gen1::encode(&val)?;

// Gen2
let val = gen2::Value::object(vec![
    ("name", gen2::Value::String("test".into())),
]);
let encoded = gen2::encode(&val)?;
```

### Python

```python
from cowrie import gen1, gen2

# Gen1
data = gen1.encode({"name": "Alice", "scores": [1.0, 2.0, 3.0]})
result = gen1.decode(data)

# Gen2
val = gen2.from_any({"name": "Alice"})
data = gen2.encode(val)
```

### C

```c
#include "cowrie_gen1.h"

cowrie_g1_value_t *obj = cowrie_g1_object(2);
cowrie_g1_object_set(obj, "name", cowrie_g1_string("Alice", 5));
cowrie_g1_object_set(obj, "count", cowrie_g1_int64(42));

cowrie_g1_buf_t buf;
cowrie_g1_encode(obj, &buf);

cowrie_g1_value_t *decoded;
cowrie_g1_decode(buf.data, buf.len, &decoded);
```

### TypeScript

```typescript
import { gen1, gen2 } from 'cowrie-codec';

// Gen1
const data = gen1.encode({ name: 'Alice', scores: [1.0, 2.0, 3.0] });
const result = gen1.decode(data);

// Gen2
const val = gen2.SJ.object([
  ['name', gen2.SJ.string('Alice')],
]);
const encoded = gen2.encode(val);
```

### CLI Tool

There's also a Go-based CLI for quick encoding and inspection:

```bash
# Encode JSON to Gen2
echo '{"name":"Alice","age":30}' | cowrie encode --gen2 > data.cowrie

# Decode back to JSON
cowrie decode < data.cowrie

# Inspect a Cowrie file
cowrie info < data.cowrie
```

---

## Deep Dive: Dictionary Coding

This is Gen2's most impactful feature, and it's worth understanding how it works at the wire level.

Consider an array of 1,000 user objects, each with keys `name`, `age`, `email`, `role`. In JSON, those 4 strings appear 1,000 times each. In Gen2, they appear once.

### Wire Format

Gen2's header contains a dictionary section:

```
Bytes 0-1: "SJ" (magic)
Byte 2:    Version (0x02)
Byte 3:    Flags
Then:      DictLen (varint) — number of dictionary entries
           Dict entries: (len:varint + UTF-8 bytes) * DictLen
           Root value: the actual payload
```

When encoding, the encoder makes a first pass to collect all unique object keys. These go into the dictionary. In the payload, each object field stores a varint dictionary index instead of the full key string.

For our 1,000-user example:
- JSON encodes `"name"` (6 bytes with quotes and colon) 1,000 times = 6,000 bytes just for that one key
- Gen2 encodes `"name"` once in the dictionary (5 bytes), then uses index `0` (1 byte) 1,000 times = 1,005 bytes

Multiply across 4 keys and 1,000 objects, and you get the 47% size reduction. The dictionary itself is small overhead — a few dozen bytes for typical schemas.

### When It Helps Most

Dictionary coding helps proportionally to how many times keys repeat:
- 10 objects with 5 keys: modest savings (~15-20%)
- 1,000 objects with 5 keys: large savings (~45-55%)
- Single object: slight overhead (dictionary adds ~10 bytes)

This is why Gen2 shows 93% of JSON size for a single small object — the header and dictionary overhead exceed the savings. For single objects, Gen1 (76%) is the better choice.

---

## Deep Dive: ML Types

Gen2 defines three ML extension types that eliminate the base64-in-JSON workaround:

### Tensor (tag 0x20)

```
Tag(0x20) | dtype:u8 | rank:u8 | dims:varint* | dataLen:varint | data
```

A tensor carries its dtype (float32, float16, bfloat16, int8, etc. — 12 supported dtypes), shape, and raw data. No base64 encoding, no JSON array-of-numbers overhead.

A 768-dimensional float32 embedding:
- JSON array: ~5,400 bytes (numbers as text)
- Base64 in JSON: ~4,100 bytes
- Cowrie tensor: ~3,080 bytes (4 bytes * 768 + small header)

### Image (tag 0x22)

```
Tag(0x22) | format:u8 | width:u16 LE | height:u16 LE | dataLen:varint | data
```

Carries format (JPEG, PNG, WebP, AVIF, BMP), dimensions, and raw image bytes. The receiving service knows the format and dimensions without parsing the image header.

### Audio (tag 0x23)

```
Tag(0x23) | encoding:u8 | sampleRate:u32 LE | channels:u8 | dataLen:varint | data
```

Carries encoding (PCM Int16, PCM Float32, Opus, AAC), sample rate, channel count, and raw audio data. Metadata and payload travel together.

These types mean an ML pipeline can pass tensors, images, and audio clips between services without serialization hacks. The type information is in the wire format, so the receiving side knows what it's getting without out-of-band metadata.

---

## Deep Dive: Graph Types

Both Gen1 and Gen2 support six graph types designed for GNN workloads and graph databases:

| Type | Tag (Gen2) | Purpose |
|------|-----------|---------|
| Node | 0x35 | Single node with ID, labels, properties |
| Edge | 0x36 | Single edge with src, dst, type, properties |
| NodeBatch | 0x37 | Batch of nodes for streaming |
| EdgeBatch | 0x38 | Batch of edges for bulk loading |
| GraphShard | 0x39 | Self-contained subgraph (nodes + edges + metadata) |
| AdjList | 0x30 | CSR adjacency list (compressed sparse row) |

### Why Wire-Level Graph Types?

The standard approach to serializing graphs is to flatten them into arrays:

```json
{
  "nodes": [{"id": "1", "label": "Person", "name": "Alice"}, ...],
  "edges": [{"src": "1", "dst": "2", "type": "KNOWS"}, ...]
}
```

This works but loses type information. The decoder doesn't know it's looking at a graph until application code interprets the structure. Cowrie's graph types make the structure explicit at the wire level.

In Gen2, graph property keys are dictionary-coded (shared with the main dictionary), so graphs with many nodes sharing the same property schema get the same size benefits as regular objects.

### GraphShard for GNN Mini-Batches

A `GraphShard` is a self-contained subgraph with nodes, edges, and metadata. It's designed for:
- GNN mini-batch checkpointing
- Distributed graph processing (partition shards)
- Graph database snapshots
- Streaming graph partitions

```go
shard := gen2.GraphShard(nodes, edges, metadata)
data, _ := gen2.Encode(shard)
// Send to another service, another language
```

### AdjList for CSR Adjacency

The `AdjList` type encodes a compressed sparse row adjacency matrix directly:

```
Tag(0x30) | idWidth:u8 | nodeCount:varint | edgeCount:varint
         | rowOffsets:(nodeCount+1)*varint | colIndices:edgeCount*(4|8 bytes)
```

This is the native format used by most graph libraries (PyG, DGL, NetworkX internals), so no conversion is needed on either end.

---

## Comparison Matrix

| Feature | JSON | MsgPack | CBOR | Protobuf | FlatBuffers | Cowrie Gen1 | Cowrie Gen2 |
|---------|------|---------|------|----------|-------------|-------------|-------------|
| Schema required | No | No | No | Yes | Yes | No | No |
| Code generation | No | No | No | Yes | Yes | No | No |
| Human readable | Yes | No | No | No | No | No | No |
| Binary | No | Yes | Yes | Yes | Yes | Yes | Yes |
| Dictionary coding | No | No | No | N/A | N/A | No | Yes |
| ML types (tensor) | No | No | No | Manual | Manual | Proto-tensor | Native |
| Graph types | No | No | No | Manual | Manual | Yes | Yes |
| Compression | No | No | No | No | No | No | gzip/zstd |
| Streaming | No | Yes | Yes | Yes | No | Yes | Yes |
| Language support | All | Many | Many | Many | Many | 5 | 5 |

### Where Competitors Win

- **MessagePack/CBOR**: More mature, wider language support, more battle-tested in production. If you don't need ML types or dictionary coding, they're solid choices.
- **Protobuf/FlatBuffers**: Stronger type safety through schemas, better tooling ecosystem, zero-copy reads (FlatBuffers). If you have stable schemas and want compile-time type checking, they're the right choice.
- **JSON**: Universal. Every language, every tool, every service understands it. Human-readable. Debuggable with `curl | jq`. If you're not bandwidth-constrained, JSON is hard to beat for simplicity.

Cowrie's niche is the intersection of: schemaless + binary + ML types + graph types. If you need all four, there isn't a direct alternative.

---

## Honest Trade-offs

Cowrie is not the right choice in every situation. Here's when you should use something else:

**Use JSON when** you need human readability, universal tooling support, or you're not bandwidth-constrained. JSON's ubiquity is a feature.

**Use Protobuf when** you have stable schemas, want compile-time type safety, or need the mature ecosystem of generated clients, documentation, and validation tools.

**Use MessagePack/CBOR when** you need a mature, battle-tested binary format with broad language support and don't need ML types or dictionary coding.

**Use FlatBuffers when** you need zero-copy reads and are willing to trade schema management for access speed.

**Gen2 overhead on small objects**: A single 3-field object is 43 bytes in Gen2 vs 46 bytes in JSON (93%). The header and dictionary add overhead that only pays off with repeated schemas. For single small messages, Gen1 (35 bytes, 76%) or even JSON may be more appropriate.

**Python performance**: The Python implementation is pure Python with no C extensions. It's correct and compatible, but it won't match the throughput of the Go or Rust implementations for large payloads. If you're in a hot path, use the Go or Rust implementation and call it from Python via FFI, or accept the throughput trade-off.

**Not battle-tested**: Cowrie is a new project. It has not been deployed in large-scale production systems. The implementations are well-tested (23 cross-language fixtures, 16 security regression tests), but they haven't seen the variety of real-world edge cases that mature projects have.

---

## Security

Binary decoders are attack surfaces. Cowrie addresses this directly:

- **7 critical/high vulnerability fixes** in the Gen2 decoder, covering buffer overflows, allocation bombs, and infinite loops
- **16 security regression tests** ensure these vulnerabilities stay fixed
- **9 configurable limits** prevent resource exhaustion: max depth (1,000), max array length (100M), max string length (500MB), max tensor rank (32), and more
- Compression decompression is size-bounded — decoders reject payloads that exceed `MaxDecompressedSize` before allocating memory
- All limits have safe defaults; "unlimited" must be explicitly opted into

---

## Getting Started

### Install

**Go:**
```bash
go get github.com/Neumenon/cowrie@v0.1.1
```

**Rust:**
```bash
cargo add cowrie-rs@0.1.1
```

**Python:**
```bash
pip install cowrie-py==0.1.1
```

**TypeScript:**
```bash
npm install cowrie-codec@0.1.1
```

**C:** Clone the repo and build with CMake:
```bash
git clone https://github.com/Neumenon/cowrie.git
cd cowrie/c && mkdir build && cd build && cmake .. && make
```

### Links

- **GitHub**: [github.com/Neumenon/cowrie](https://github.com/Neumenon/cowrie)
- **Spec**: [SPEC.md](https://github.com/Neumenon/cowrie/blob/main/SPEC.md)
- **Changelog**: [CHANGELOG.md](https://github.com/Neumenon/cowrie/blob/main/CHANGELOG.md)
- **License**: Apache 2.0

---

*Cowrie is maintained by [Neumenon](https://github.com/Neumenon). Contributions, bug reports, and feedback are welcome.*
