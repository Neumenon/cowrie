# Cowrie - Structured JSON Binary Codec

A multi-language binary JSON codec with two variants:

- **Gen1**: Lightweight codec with proto-tensor support (11 core types + graph types)
- **Gen2**: Full Cowrie v2 with ML extensions (18+ types, dictionary coding, compression)

## Features

| Feature | Gen1 | Gen2 |
|---------|------|------|
| Core types | 11 | 13+ |
| ML types | proto-tensors | Tensor, Image, Audio, etc. |
| Dictionary coding | No | Yes |
| Compression | No | gzip/zstd |
| Schema fingerprint | No | Yes |
| Graph types | 6 | 6 (Node, Edge, NodeBatch, EdgeBatch, GraphShard, Adjlist) |

## Language Implementations

| Language | Gen1 | Gen2 | Status |
|----------|------|------|--------|
| Go | Yes | Yes | Complete |
| Rust | Yes | Yes | Complete |
| Python | Yes | Yes | Complete |
| C | Yes (core + proto-tensor) | Yes | Partial (Gen1 graph types pending) |
| TypeScript | Yes (core + proto-tensor) | Yes | Partial (Gen1 graph types pending) |

## Quick Start

### Go

```go
import (
    cowrie "github.com/Neumenon/cowrie"
    "github.com/Neumenon/cowrie/gen1"
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

### C

```bash
cd c
cmake -S . -B build
cmake --build build
ctest --test-dir build
```

Requirements:
- `zlib` development headers (required)
- `libzstd` + `pkg-config` (optional, for zstd support)

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

| Payload Type | JSON | Gen1 | Gen2 |
|--------------|------|------|------|
| Small object (3 fields) | 46 bytes | 35 bytes (76%) | 43 bytes (93%) |
| Large array (1000 objects) | 48KB | 34KB (70%) | 23KB (47%) |
| Float array (10K floats) | 86KB | 80KB (93%) | - |
| Graph shard (100 nodes) | - | - | 10KB |

**Key insight**: Gen2 dictionary coding provides ~50% size reduction for repeated schemas.

### When to Use Gen1 vs Gen2

| Use Case | Recommended | Why |
|----------|-------------|-----|
| Simple JSON APIs | Gen1 | Faster, simpler |
| Repeated schemas (logs, events) | Gen2 | Dictionary coding saves ~50% |
| ML pipelines (tensors, images) | Gen2 | Native ML type support |
| Graph data (GNN) | Gen2 | Node, Edge, GraphShard types |
| Embedded/IoT | Gen1 | Smaller code footprint |
| Real-time systems | Gen1 | Single-pass, predictable latency |

## Graph Types (v2.1)

Gen2 graph data structures:

```go
// Go - Gen2 Graph Types
node := cowrie.NodeData{
    ID:     "person_42",
    Labels: []string{"Person", "Employee"},
    Props:  map[string]any{"name": "Alice", "age": int64(30)},
}

edge := cowrie.EdgeData{
    From:  "person_42",
    To:    "company_1",
    Type:  "WORKS_AT",
    Props: map[string]any{"since": int64(2020)},
}

shard := cowrie.GraphShard(
    []cowrie.NodeData{node},
    []cowrie.EdgeData{edge},
    map[string]any{"source": "example"},
)
```

```python
# Python - Gen2 Graph Types
from cowrie.gen2 import Value, NodeData, EdgeData

node = NodeData(id="person_42", labels=["Person"], props={"name": Value.string("Alice")})
edge = EdgeData(from_id="person_42", to_id="company_1", edge_type="WORKS_AT", props={})
shard = Value.graph_shard([node], [edge], {"source": Value.string("example")})
```

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
    cowrie "github.com/Neumenon/cowrie"
    "github.com/Neumenon/cowrie/codec"
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

### Gen2: Column-wise Access

```go
// Go - Read specific columns without full decode
cr, _ := cowrie.NewColumnReader(data)
names, _ := cr.ReadColumn("name")   // Only decode "name" field
ages, _, _ := cr.ReadInt64Column("age")
```

## Wire Format

See [SPEC.md](SPEC.md) for the complete wire format specification.

## Benchmarks

Run benchmarks:

```bash
# Go
cd go && go test -bench=. -benchmem ./...

# Python
cd python && python ../benchmarks/bench_python.py

# Rust
cd rust && cargo bench
```

## License

MIT
