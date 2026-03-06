# Reddit Posts (3 Subreddits)

---

## r/golang

**Title:** I built a binary JSON codec in Go with dictionary coding and ML types — single dependency (klauspost/compress)

**Body:**

I've been working on Cowrie, a binary codec for JSON-like data. There are implementations in 5 languages, but I wanted to share the Go specifics here since it's the most complete implementation and where the CLI tool lives.

**What it does:**

Two wire formats. Gen1 is a straightforward binary JSON (null, bool, int64, float64, string, bytes, array, object) — a 46-byte JSON object becomes 35 bytes (76%). Gen2 adds a header with dictionary-coded keys and optional gzip/zstd compression — 1,000 objects with repeated schemas go from 48KB JSON to 23KB (47%).

**Go-specific things:**

- **Single dependency**: `github.com/klauspost/compress` for zstd. Everything else is stdlib.
- **Streaming decoder**: `gen1.NewStreamDecoder(reader)` for record-by-record decode from an `io.Reader`. Gen2 has `MasterFrame` with framed streaming and metadata.
- **Column reader**: `gen2.NewColumnReader(data)` lets you read specific columns without full decode — useful for log processing.
- **CLI tool**: `go install github.com/Neumenon/cowrie/cmd/cowrie@latest` gives you `cowrie encode/decode/info` for command-line use.
- **Graph types**: Node, Edge, GraphShard, AdjList (CSR format) — first-class wire types, not application conventions.

**Quick example:**

```go
import (
    "github.com/Neumenon/cowrie/gen1"
    "github.com/Neumenon/cowrie/gen2"
)

// Gen1 — simple
data, _ := gen1.Encode(map[string]any{
    "name": "Alice",
    "embedding": []float64{0.1, 0.2, 0.3},
})

// Gen2 — dictionary coding
val := gen2.Object(
    gen2.Member{Key: "name", Value: gen2.String("Alice")},
)
data, _ = gen2.Encode(val)
```

**Trade-offs:**

- Gen2 has header overhead on small objects (93% of JSON for a single 3-field object). Dictionary coding pays off with repeated schemas, not single messages.
- New project — well-tested (23 cross-language test fixtures, 16 security regression tests) but not production-proven at scale.
- 5 language implementations vs MessagePack's dozens.

**Install:**

```bash
go get github.com/Neumenon/cowrie@v0.1.1
```

Source: https://github.com/Neumenon/cowrie

Spec: https://github.com/Neumenon/cowrie/blob/main/SPEC.md

Interested in feedback on the Go API, especially around the Gen2 value construction (the `gen2.Object(gen2.Member{...})` pattern). Does it feel idiomatic, or would you prefer something else?

---

## r/rust

**Title:** cowrie-rs: schemaless binary JSON codec with dictionary coding, ML types, and graph types

**Body:**

I'm releasing `cowrie-rs`, a Rust implementation of the Cowrie binary codec. It's part of a multi-language project (Go, Rust, Python, C, TypeScript), but I wanted to get Rust-specific feedback.

**What Cowrie is:**

A binary codec for JSON-like data in two variants:
- **Gen1**: Simple binary encoding, no header. `{"name":"Alice","age":30,"active":true}` goes from 46 bytes JSON to 35 bytes.
- **Gen2**: 4-byte header with dictionary-coded keys and optional compression. 1,000 repeated-schema objects go from 48KB to 23KB (47% of JSON).

Gen2 also adds native ML types (Tensor with 12 dtypes, Image, Audio) and graph types (Node, Edge, GraphShard, CSR AdjList).

**Dependency footprint:**

```toml
[dependencies]
flate2 = "1.0"
serde_json = "1.0"
base64 = "0.22"
zstd = { version = "0.13", optional = true }

[features]
default = []
zstd = ["dep:zstd"]
```

`zstd` is behind a feature flag. Without it, you get gzip compression only.

**Usage:**

```rust
use cowrie::{gen1, gen2};

// Gen1
let val = gen1::Value::Object(vec![
    ("name".to_string(), gen1::Value::String("test".to_string())),
]);
let encoded = gen1::encode(&val)?;
let decoded = gen1::decode(&encoded)?;

// Gen2
let val = gen2::Value::object(vec![
    ("name", gen2::Value::String("test".into())),
]);
let encoded = gen2::encode(&val)?;
let decoded = gen2::decode(&encoded)?;
```

**Things I'd like feedback on:**

1. **serde derive**: The current API uses its own `Value` enum. Would a `#[derive(Cowrie)]` macro be useful, or is the `Value` API sufficient for schemaless use?
2. **Error types**: Currently uses a single error enum. Should it be more granular?
3. **The dependency on `serde_json`**: Used for JSON bridge (convert between Cowrie values and serde_json::Value). If you're not using the JSON bridge, this is dead weight. Should it be behind a feature flag?

**Trade-offs to be aware of:**

- Gen2 header overhead: a single small object is 93% of JSON size (Gen1 is 76%). Dictionary coding pays off at scale, not for individual messages.
- New crate, not battle-tested. 23 cross-language test fixtures and 16 security regression tests, but no large-scale production use yet.
- 9 configurable decode limits for security (max depth, max array length, max string length, etc.), all with safe defaults.

**Install:**

```bash
cargo add cowrie-rs@0.1.1
```

Source: https://github.com/Neumenon/cowrie

Wire format spec: https://github.com/Neumenon/cowrie/blob/main/SPEC.md

---

## r/programming

**Title:** Cowrie: a binary JSON codec in 5 languages (Go, Rust, Python, C, TypeScript) with dictionary coding and ML types

**Body:**

I built a binary codec for JSON-like data that works identically across Go, Rust, Python, C, and TypeScript. No code generation, no schema files — you pass a data structure and get bytes.

**Why another binary format?**

I was building a system where Python ML services produce embeddings and Go API services consume them. The options were: base64-encode float arrays in JSON (ugly, ~33% overhead), use Protobuf (requires .proto files and codegen for both languages), or use MessagePack (no native tensor type, so you're back to encoding conventions).

Cowrie has two variants:

**Gen1** is a simple binary JSON encoding. 11 types: null, bool, int64, float64, string, bytes, array, object, plus proto-tensors (Int64Array, Float64Array) and graph types. A 46-byte JSON object becomes 35 bytes.

**Gen2** adds three things:
1. **Dictionary-coded keys** — object keys are deduplicated into a shared dictionary. 1,000 objects with the same schema: 48KB JSON becomes 23KB (47%).
2. **Compression** — gzip or zstd on the payload, compounds with dictionary coding.
3. **ML types** — Tensor (12 dtypes including float16 and bfloat16, with shape), Image (format + dimensions + bytes), Audio (encoding + sample rate + channels + bytes).
4. **Graph types** — Node, Edge, NodeBatch, EdgeBatch, GraphShard, CSR AdjList.

**Cross-language parity:**

All 5 implementations pass the same 23 test fixtures (7 core, 7 ML, 5 graph, 4 negative). The fixture suite covers encode/decode round-trips, ML type handling, graph types, and error cases (invalid magic, bad version, truncated input, invalid tags).

**Comparison with alternatives:**

| | JSON | MsgPack | CBOR | Protobuf | Cowrie Gen2 |
|---|---|---|---|---|---|
| Schema required | No | No | No | Yes | No |
| Code generation | No | No | No | Yes | No |
| Dictionary coding | No | No | No | N/A | Yes |
| ML types | No | No | No | Manual | Native |
| Graph types | No | No | No | Manual | Native |
| Compression | No | No | No | No | gzip/zstd |

**When NOT to use Cowrie:**

- **Need human readability?** Use JSON.
- **Have stable schemas and want type safety?** Use Protobuf or FlatBuffers.
- **Need maximum language support?** MessagePack and CBOR have implementations in 50+ languages. Cowrie has 5.
- **Single small messages?** Gen2's header overhead makes it 93% of JSON for one object. Gen1 is 76%, but if you're sending single small messages, JSON is probably fine.
- **Need production-proven maturity?** Cowrie is new. It has security tests (7 vulnerability fixes, 16 regression tests, 9 configurable limits) but hasn't been deployed at scale.
- **Python hot paths?** The Python implementation is pure Python. It's correct but not fast.

**Quick demo (Python):**

```python
from cowrie import gen1, gen2

# One line to encode, one to decode
data = gen1.encode({"name": "Alice", "scores": [1.0, 2.0, 3.0]})
result = gen1.decode(data)
# result == {"name": "Alice", "scores": [1.0, 2.0, 3.0]}
```

**Install:**

```
Go:         go get github.com/Neumenon/cowrie@v0.1.1
Rust:       cargo add cowrie-rs@0.1.1
Python:     pip install cowrie-py==0.1.1
TypeScript: npm install cowrie-codec@0.1.1
C:          cmake (see repo)
```

Source and spec: https://github.com/Neumenon/cowrie

Happy to discuss design decisions, trade-offs, or anything about the wire format. The full spec is in the repo.
