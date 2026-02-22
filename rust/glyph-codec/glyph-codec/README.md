# GLYPH Codec - Rust

Token-efficient serialization for AI agents.

## Installation

```toml
[dependencies]
glyph-codec = "0.1"
```

## Quick Start

```rust
use glyph_codec::{from_json, to_json, canonicalize_loose, GValue};
use serde_json::json;

// JSON to GLYPH
let data = json!({"action": "search", "query": "weather", "max_results": 10});
let gvalue = from_json(&data);
let glyph_str = canonicalize_loose(&gvalue);
// {action=search max_results=10 query=weather}

// GLYPH value to JSON
let json_str = to_json(&gvalue);

// Build values directly
let value = GValue::Map(vec![
    MapEntry { key: "name".into(), value: GValue::Str("Alice".into()) },
    MapEntry { key: "age".into(), value: GValue::Int(30) },
]);
```

## API Reference

### Types

```rust
pub enum GValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    Time(DateTime<Utc>),
    Id(RefId),
    List(Vec<GValue>),
    Map(Vec<MapEntry>),
    Struct(StructValue),
    Sum(SumValue),
}
```

### Schema + Streaming Field Values

Both `schema_evolution` and `stream_validator` define a `FieldValue` type. The crate root
re-exports the streaming one as `StreamFieldValue` to avoid name collisions.

```rust
use glyph_codec::{FieldValue, StreamFieldValue};
// FieldValue = schema_evolution::FieldValue
// StreamFieldValue = stream_validator::FieldValue
```

### Functions

| Function | Description |
|----------|-------------|
| `from_json(&JsonValue) -> GValue` | Convert serde_json Value to GValue |
| `to_json(&GValue) -> String` | Convert GValue to JSON string |
| `canonicalize_loose(&GValue) -> String` | Canonical GLYPH representation |
| `canonicalize_loose_no_tabular(&GValue) -> String` | Canonical without auto-tabular |
| `fingerprint_loose(&GValue) -> String` | Same as canonicalize_loose |
| `hash_loose(&GValue) -> String` | SHA-256 hash (first 16 hex chars) |
| `equal_loose(&GValue, &GValue) -> bool` | Semantic equality check |

### Options

```rust
let opts = LooseCanonOpts {
    auto_tabular: true,     // Enable tabular mode for arrays
    min_rows: 3,            // Min rows for tabular
    max_cols: 64,           // Max columns for tabular
    allow_missing: true,    // Allow missing keys in tabular
    null_style: NullStyle::Underscore,  // _ or ∅
};
let result = canonicalize_loose_with_opts(&value, &opts);
```

## Examples

### Token Savings

```rust
// JSON: 67 chars
// {"action":"search","query":"weather in NYC","max_results":10}

// GLYPH: 52 chars (22% smaller)
// {action=search max_results=10 query="weather in NYC"}
```

### Auto-Tabular Mode

```rust
let data = json!([
    {"id": "doc_1", "score": 0.95},
    {"id": "doc_2", "score": 0.89},
    {"id": "doc_3", "score": 0.84},
]);
let gvalue = from_json(&data);
println!("{}", canonicalize_loose(&gvalue));
// @tab _ rows=3 cols=2 [id score]
// |doc_1|0.95|
// |doc_2|0.89|
// |doc_3|0.84|
// @end
```

### Reference IDs

```rust
let ref_id = GValue::Id(RefId {
    prefix: "user".into(),
    value: "abc123".into(),
});
// ^user:abc123
```

## License

MIT
