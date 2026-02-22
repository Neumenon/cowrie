# SJSON - Structured JSON Binary Codec

A multi-language binary JSON codec with two variants:

- **Gen1**: Lightweight codec with proto-tensor support (11 core types + graph types)
- **Gen2**: Full SJSON v2 with ML extensions (18+ types, dictionary coding, compression)

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
| C | Yes | Yes | Complete |
| TypeScript | Yes | Yes | Complete |

## Quick Start

### Go

```go
import (
    "github.com/phenomenon0/sjson-final/go/gen1"
    "github.com/phenomenon0/sjson-final/go/gen2"
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
use sjson::{gen1, gen2};

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
from sjson import gen1, gen2

# Gen1
data = gen1.encode({"name": "Alice", "scores": [1.0, 2.0, 3.0]})
result = gen1.decode(data)

# Gen2
val = gen2.from_any({"name": "Alice"})
data = gen2.encode(val)
```

### C

```c
#include "sjson_gen1.h"

sjson_g1_value_t *obj = sjson_g1_object(2);
sjson_g1_object_set(obj, "name", sjson_g1_string("Alice", 5));
sjson_g1_object_set(obj, "count", sjson_g1_int64(42));

sjson_g1_buf_t buf;
sjson_g1_encode(obj, &buf);

sjson_g1_value_t *decoded;
sjson_g1_decode(buf.data, buf.len, &decoded);
```

### TypeScript

```typescript
import { gen1, gen2 } from 'sjson';

// Gen1
const data = gen1.encode({ name: 'Alice', scores: [1.0, 2.0, 3.0] });
const result = gen1.decode(data);

// Gen2
const val = gen2.SJ.object([
  ['name', gen2.SJ.string('Alice')],
]);
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
pip install -e .
pytest tests/
```

### C

```bash
cd c
mkdir build && cd build
cmake ..
make
ctest
```

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
go build -o sjson ./cmd/sjson

# Encode JSON to SJSON
echo '{"name":"Alice","age":30}' | ./sjson encode --gen2 > data.sjson

# Decode SJSON to JSON
./sjson decode < data.sjson

# Get info about SJSON file
./sjson info < data.sjson
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

Both Gen1 and Gen2 support graph data structures:

```go
// Go - Gen2 Graph Types
node := gen2.Node(gen2.NodeConfig{
    ID:     "person_42",
    Labels: []string{"Person", "Employee"},
    Props:  map[string]any{"name": "Alice", "age": 30},
})

edge := gen2.Edge(gen2.EdgeConfig{
    From:     "person_42",
    To:       "company_1",
    EdgeType: "WORKS_AT",
    Props:    map[string]any{"since": 2020},
})

shard := gen2.GraphShard(nodes, edges, metadata)
```

```python
# Python - Gen2 Graph Types
from sjson.gen2 import Value, NodeData, EdgeData

node = Value.node("person_42", ["Person"], {"name": Value.string("Alice")})
edge = Value.edge("person_42", "company_1", "WORKS_AT", {})
shard = Value.graph_shard(nodes, edges, metadata)
```

## Streaming Support

SJSON supports streaming for large payloads:

### Gen1: Record-by-Record Streaming

```go
// Go - Stream decode from io.Reader
dec := gen1.NewStreamDecoder(conn)
for {
    val, err := dec.Next()
    if err == io.EOF {
        break
    }
    process(val)
}
```

### Gen2: Framed Master Stream

```go
// Go - Master stream with metadata
frame := gen2.NewMasterFrame(data)
frame.SetMetadata("version", "1.0")
frame.WriteTo(writer)

// Read frame
frame, _ := gen2.ReadMasterFrame(reader)
val := frame.Value()
meta := frame.Metadata()
```

```python
# Python - Master stream
from sjson.gen2 import write_master_frame, read_master_frame

write_master_frame(writer, value, metadata={"version": 1})
frame = read_master_frame(reader)
```

### Gen2: Column-wise Access

```go
// Go - Read specific columns without full decode
cr := gen2.NewColumnReader(data)
names := cr.Column("name")  // Only decode "name" field
ages := cr.Column("age")    // Only decode "age" field
```

## Wire Format

See [SPEC.md](SPEC.md) for the complete wire format specification.

## Benchmarks

Run benchmarks:

```bash
# Go
cd go && go test -bench=. -benchmem ../benchmarks/

# Python
cd python && python ../benchmarks/bench_python.py

# Rust
cd rust && cargo bench
```

## License

MIT
