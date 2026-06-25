# Cowrie — Structured Binary Codec

A multi-language binary codec for structured data with native ML extensions:

- **Gen1**: Lightweight codec — 16 core types + 4 proto-tensor types
- **Gen2**: Full Cowrie v2 — 20+ types, dictionary coding, compression, schema fingerprint

Cowrie's lead value is **deterministic encoding with native bytes (0x08), tensors
(0x20), tensor refs (0x21), and a stable schema fingerprint** — useful where
JSON's text shape and lack of binary primitives are the bottleneck.

> **Reserved tags (Gen2).** In Gen2, tags `0x30` (AdjList), `0x31` (RichText),
> `0x32` (Delta), and `0x39` (GraphShard) are deprecated. Decoders MUST skip
> their length-prefixed payloads silently. Encoders MUST NOT emit them. The
> original Gen2 graph / rich-text / delta implementations live under `attic/`.
> (Gen1 retains AdjList and GraphShard as active graph types.)

## Features

| Feature | Gen1 | Gen2 |
|---------|------|------|
| Core types (0x00-0x0F) | 16 (unified with Gen2) | 16 (unified with Gen1) |
| ML types | proto-tensors (0x16-0x19) | Tensor, Image, Audio, etc. |
| Dictionary coding | No | Yes |
| Compression | No | gzip/zstd |
| Schema fingerprint | No | Yes |

## Language Implementations

| Language | Gen1 | Gen2 | Status |
|----------|------|------|--------|
| Go | Yes | Yes | Supported |
| Rust | Yes | Yes | Supported |
| Python | Yes | Yes | Supported |
| TypeScript | Yes | Yes | Supported |

## Cross-Language Parity

Cowrie is **beta** software: the wire format is intentionally small and pinned by
fixtures, but the project is new and not yet production-proven at large scale.

Cowrie's cross-language parity is **machine-enforced**: the Python reference (the executable spec /
oracle), Go, Rust, and TypeScript must agree byte-for-byte. CI runs `tools/run_all_gates.sh` on every
push, and a red gate blocks merge. The 11 standing gates:

- **count-guard** — the conformance corpus may only grow (no silent coverage shrink)
- **conformance** — every golden vector encodes/decodes to identical canonical bytes in all 4 langs
- **content-address** — all 4 agree on the multihash-SHA-256 content address (§3)
- **file-identity** — all 4 agree on the Merkle file identity (§7)
- **tensor-view** — all 4 agree on zero-copy tensor data spans (§2.5, 64-byte aligned)
- **fingerprint** — all 4 agree on the §4 structural fingerprint
- **dataset-identity** — all 4 agree on the Merkle dataset root (stream layer)
- **negative** (lenient + **strict**) — non-canonical/malformed input is rejected with the exact `ERR_*` code
- **differential fuzz** — thousands of random values, zero cross-language divergence
- **canary** — proves a 1-byte corruption turns the gate red

Deterministic, content-addressed encoding (global byte-sorted keys, 64-byte-aligned tensors) makes the
output suitable for content-addressable storage, dedup, and cross-service caching.

## Install

```bash
# Go
go get github.com/Neumenon/cowrie/go/v2@latest

# Python
pip install cowrie-py

# JavaScript / TypeScript
npm install cowrie-codec

# Rust
cargo add cowrie-rs
```

## Benchmarks

- [vLLM serialization benchmark](go/vllm_bench_test.go) — Cowrie vs JSON for inference payloads (~4.8x compression on a 1536-dim float32 embedding, zero-copy tensor decode)

## Quick Start

### Go

```go
import (
    cowrie "github.com/Neumenon/cowrie/go/v2"
    "github.com/Neumenon/cowrie/go/v2/gen1"
)

// Gen1
data, _ := gen1.Encode(map[string]any{
    "name": "Alice",
    "embedding": []float64{0.1, 0.2, 0.3},
})
result, _ := gen1.Decode(data)

// Gen2
val := cowrie.Object(
    cowrie.Member{Key: "name", Value: cowrie.String("Alice")},
)
data, _ = cowrie.Encode(val)
```

### Rust

```rust
use cowrie_rs::{gen1, gen2};

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

### TypeScript

```typescript
import { gen1, gen2 } from 'cowrie-codec';

// Gen1
const data = gen1.encode({ name: 'Alice', scores: [1.0, 2.0, 3.0] });
const result = gen1.decode(data);

// Gen2
const val = gen2.SJ.object({ name: gen2.SJ.string('Alice') });
const encoded = gen2.encode(val);
```

## Building

### Go

```bash
cd go
go build ./...
go test ./...
```

### Rust

```bash
cd rust
cargo build
cargo test
```

### Python

```bash
cd python
pip install -e ".[dev]"
pytest tests/
```

The optional Cython fast-path extension needs `zlib` development headers (and
`libzstd` + `pkg-config` for zstd support); without them, the pure-Python path is
used automatically. Published v2.1.2 wheels require and smoke-test the native
accelerator on Linux/macOS; Windows installs use the pure-Python sdist fallback
until native MSVC/zlib wheels are validated.

### TypeScript

```bash
cd typescript
npm install
npm run build
npm test
```

## CLI Tool

A command-line tool is available for encoding/decoding:

```bash
cd go
go build -o cowrie ./cmd/cowrie

# Encode JSON to Cowrie
echo '{"name":"Alice","age":30}' | ./cowrie encode --gen2 > data.cowrie

# Decode Cowrie to JSON
./cowrie decode < data.cowrie

# Get info about Cowrie file
./cowrie info < data.cowrie
```

## Performance

### Payload Size Comparison

Measured with `cowrie encode` on the described inputs (the embedding row uses the
typed float32 proto-tensor / `Tensor` type, the ML use case):

| Payload | JSON | Gen1 | Gen2 |
|---------|-----:|-----:|-----:|
| Small object (3 fields) | 39 B | 35 B | 34 B |
| Array of 1,000 repeated-schema objects | 45,149 B | 40,003 B | **18,680 B** |
| 1536-dim float32 embedding (typed) | 29,648 B | 6,185 B | 6,195 B |

**Key insights:** Gen2's dictionary coding roughly halves repeated-schema payloads
(18.7 KB vs 40 KB Gen1, ~2.4× vs JSON). For float32 tensors/embeddings, both Gen1's
proto-tensor and Gen2's `Tensor` give ~4.8× over JSON (binary float32 vs float64
text) — use the typed tensor for ML payloads; an untyped JSON-style float array
compresses less.

### When to Use Gen1 vs Gen2

| Use Case | Recommended | Why |
|----------|-------------|-----|
| Simple JSON APIs | Gen1 | Faster, simpler |
| Repeated schemas (logs, events) | Gen2 | Dictionary coding saves ~50% |
| ML pipelines (tensors, images) | Gen2 | Native ML type support |
| Graph data (GNN) | Gen2 | Node, Edge, NodeBatch, EdgeBatch types |
| Embedded/IoT | Gen1 | Smaller code footprint |
| Real-time systems | Gen1 | Single-pass, predictable latency |

## Graph Types (v2.1)

Gen2 graph data structures (Node/Edge/NodeBatch/EdgeBatch — 0x35-0x38):

```go
// Go - Gen2 Graph Types
node := cowrie.Node("person_42", []string{"Person", "Employee"}, map[string]any{
    "name": "Alice",
    "age":  int64(30),
})

edge := cowrie.Edge("person_42", "company_1", "WORKS_AT", map[string]any{
    "since": int64(2020),
})

batch := cowrie.NodeBatch([]cowrie.NodeData{node.Node()})
```

> **Note:** In Gen2, tag 0x39 (GraphShard) is reserved and no longer emitted —
> use NodeBatch (0x37) + EdgeBatch (0x38) to transport graph data. (Gen1 still
> supports GraphShard and AdjList directly.)

## Schema Fingerprint

A stable 64-bit FNV-1a fingerprint of a value's **schema** — its type structure and
field names, not the data — identical across all implementations. Useful as a
type ID for stream frames or for detecting schema changes.

```go
v := cowrie.Object(
    cowrie.Member{Key: "name", Value: cowrie.String("Alice")},
    cowrie.Member{Key: "age", Value: cowrie.Int64(30)},
)
fp := cowrie.SchemaFingerprint64(v) // same fingerprint in Go/Rust/Python/TS
// 32-bit variant for compact frame headers:
id := cowrie.SchemaFingerprint32(v)
```

Two values with the same field names and types share a fingerprint regardless of
their actual values; changing a field's name or type changes it.

## Streaming Support

Cowrie supports streaming for large payloads:

### Gen1: Record-by-Record Streaming

```go
// Go - Stream decode from io.Reader
dec := gen1.NewStreamDecoder(conn)
for {
    val, err := dec.Decode()
    if err == io.EOF {
        break
    }
    process(val)
}
```

### Gen2: Framed Master Stream

```go
// Go - Master stream with metadata
import (
    cowrie "github.com/Neumenon/cowrie/go/v2"
    "github.com/Neumenon/cowrie/go/v2/codec"
)

mw := codec.NewMasterWriter(writer, codec.DefaultMasterWriterOptions())
_ = mw.WriteWithMeta(
    map[string]any{"name": "Alice"},
    cowrie.Object(cowrie.Member{Key: "version", Value: cowrie.String("1.0")}),
)

// Read frame
mr := codec.NewMasterReader(streamBytes, codec.DefaultMasterReaderOptions())
frame, _ := mr.Next()
val := frame.Payload
meta := frame.Meta
```

```python
# Python - Master stream
from cowrie.gen2 import Value, write_master_frame, read_master_frame

payload = write_master_frame(
    Value.object({"name": Value.string("Alice")}),
    Value.object({"version": Value.int64(1)}),
)
frame, _ = read_master_frame(payload)
```


## Wire Format

See [SPEC.md](SPEC.md) for the complete wire format specification.

## Glyph (text format)

**Glyph** is a sibling *text* serialization that encodes the same values as
token-efficient, human-readable text (a JSON bridge for LLM payloads), with a
bridge to and from the cowrie binary format. It is maintained as a standalone
project at [github.com/Neumenon/glyph](https://github.com/Neumenon/glyph) — its
guide, quickstart, and specs live there.

## Benchmarks

Run benchmarks:

```bash
# Go (includes the vLLM serialization benchmarks)
cd go && go test -bench=. -benchmem ./...

# Rust
cd rust && cargo bench
```

## License

MIT
