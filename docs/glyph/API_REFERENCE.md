# GLYPH API Reference

**Per-language API documentation and examples.**

---

## Quick Navigation

| Language | Installation | API Docs |
|----------|--------------|----------|
| [Python](#python-api) | `pip install glyph-serial` | [Full Docs →](../py/README.md) |
| [Go](#go-api) | `go get github.com/Neumenon/glyph` | [Full Docs →](../go/README.md) |
| [JavaScript](#javascript-api) | `npm install glyph-js` | [Full Docs →](../js/README.md) |
| [Rust](#rust-api) | `cargo add glyph-codec` | [Full Docs →](../rust/README.md) |
| [C](#c-api) | `make` | [Full Docs →](../c/README.md) |

---

## Python API

### Installation

```bash
pip install glyph-serial
```

### Core Functions

#### `json_to_glyph(data: Any) -> str`
Convert JSON-compatible Python data to GLYPH text.

```python
import glyph

data = {"action": "search", "query": "test", "limit": 5}
text = glyph.json_to_glyph(data)
# Result: {action=search limit=5 query=test}
```

#### `glyph_to_json(text: str) -> Any`
Parse GLYPH text to Python dict/list.

```python
text = '{name=Alice age=30 active=t}'
data = glyph.glyph_to_json(text)
# Result: {"name": "Alice", "age": 30, "active": True}
```

#### `parse(text: str) -> GValue`
Parse GLYPH text to GValue object.

```python
result = glyph.parse('{name=Alice scores=[95 87 92]}')
print(result.get("name").as_str())  # "Alice"
print(result.get("scores").as_list())  # [95, 87, 92]
```

#### `emit(value: GValue, mode: str = "loose") -> str`
Emit GValue as GLYPH text.

```python
from glyph import g, field

match = g.struct("Match",
    field("home", g.str("Arsenal")),
    field("away", g.str("Liverpool")),
    field("score", g.list(g.int(2), g.int(1)))
)

text = glyph.emit(match)
# Match{away=Liverpool home=Arsenal score=[2 1]}
```

### Fingerprinting

#### `fingerprint_loose(data: Any) -> str`
Compute SHA-256 hash of canonical form.

```python
data = {"user": "alice", "count": 42}
hash = glyph.fingerprint_loose(data)
# sha256:a1b2c3d4e5f6...
```

### Streaming Validation

#### `StreamingValidator(registry: ToolRegistry)`
Validate tool calls during token streaming.

```python
from glyph import StreamingValidator, ToolRegistry

registry = ToolRegistry()
registry.register("search", args={"query": "str", "limit": "int<1,100>"})

validator = StreamingValidator(registry)

for token in llm_stream:
    result = validator.push(token)
    if result.has_errors():
        cancel_generation()
        break
```

**[Full Python Documentation →](../py/README.md)**

---

## Go API

### Installation

```bash
go get github.com/Neumenon/glyph
```

### Core Functions

#### `Parse(text []byte) (Value, error)`
Parse GLYPH text to Value.

```go
import "github.com/Neumenon/glyph/glyph"

text := `{action=search query=weather limit=10}`
val, err := glyph.Parse([]byte(text))
if err != nil {
    log.Fatal(err)
}

action := val.Get("action").String()  // "search"
limit := val.Get("limit").Int()       // 10
```

#### `FromJSONLoose(jsonData interface{}) Value`
Convert Go data to GLYPH Value.

```go
data := map[string]interface{}{
    "name":   "Alice",
    "active": true,
    "scores": []int{95, 87, 92},
}

v := glyph.FromJSONLoose(data)
text := glyph.CanonicalizeLoose(v)
// {active=t name=Alice scores=[95 87 92]}
```

#### `ToJSONLoose(v Value) (interface{}, error)`
Convert GLYPH Value to Go data.

```go
text := `{name=Alice age=30}`
val, _ := glyph.Parse([]byte(text))

data, err := glyph.ToJSONLoose(val)
// map[string]interface{}{"name": "Alice", "age": 30}
```

### Fingerprinting

#### `FingerprintLoose(v Value) string`
Compute SHA-256 hash.

```go
val := glyph.FromJSONLoose(data)
hash := glyph.FingerprintLoose(val)
// sha256:a1b2c3d4e5f6...
```

### Streaming

#### `NewStreamingValidator(registry *ToolRegistry) *Validator`

```go
registry := glyph.NewToolRegistry()
registry.Register("search", glyph.Args{
    "query": {Type: "str", Required: true},
    "limit": {Type: "int", Min: 1, Max: 100},
})

validator := glyph.NewStreamingValidator(registry)

for token := range llmStream {
    result := validator.Push(token)
    if result.HasErrors() {
        cancelGeneration()
        break
    }
}
```

**[Full Go Documentation →](../go/README.md)**

---

## JavaScript API

### Installation

```bash
npm install glyph-js
```

### Core Functions

#### `parse(text: string): GValue`
Parse GLYPH text.

```typescript
import { parse } from 'glyph-js';

const value = parse('{action=search query=test}');
console.log(value.get('action'));  // 'search'
```

#### `emit(value: GValue): string`
Emit GLYPH text.

```typescript
import { emit, g } from 'glyph-js';

const data = g.map({
    name: g.str('Alice'),
    age: g.int(30),
    active: g.bool(true)
});

const text = emit(data);
// {active=t age=30 name=Alice}
```

#### `fromJSON(data: any): GValue`
Convert JS data to GValue.

```typescript
import { fromJSON, emit } from 'glyph-js';

const data = { name: 'Alice', scores: [95, 87, 92] };
const gv = fromJSON(data);
const text = emit(gv);
// {name=Alice scores=[95 87 92]}
```

#### `toJSON(value: GValue): any`
Convert GValue to JS data.

```typescript
import { parse, toJSON } from 'glyph-js';

const value = parse('{name=Alice age=30}');
const data = toJSON(value);
// { name: 'Alice', age: 30 }
```

### Fingerprinting

#### `fingerprintLoose(data: any): string`

```typescript
import { fingerprintLoose } from 'glyph-js';

const data = { user: 'alice', count: 42 };
const hash = fingerprintLoose(data);
// sha256:a1b2c3d4e5f6...
```

**[Full JavaScript Documentation →](../js/README.md)**

---

## Rust API

### Installation

```bash
cargo add glyph-codec
```

### Core Functions

#### `from_json(json: &Value) -> GValue`

```rust
use glyph_codec::{from_json, canonicalize_loose};
use serde_json::json;

let data = json!({"action": "search", "query": "weather"});
let gvalue = from_json(&data);
let glyph = canonicalize_loose(&gvalue);
// {action=search query=weather}
```

#### `to_json(gvalue: &GValue) -> Value`

```rust
use glyph_codec::{parse, to_json};

let gvalue = parse("{name=Alice age=30}").unwrap();
let json = to_json(&gvalue);
// {"name": "Alice", "age": 30}
```

**[Full Rust Documentation →](../rust/README.md)**

---

## C API

### Installation

```bash
cd c/glyph-codec
make
```

### Core Functions

#### `glyph_from_json(const char *json) -> glyph_value_t*`

```c
#include "glyph.h"

glyph_value_t *v = glyph_from_json("{\"action\": \"search\"}");
char *glyph = glyph_canonicalize_loose(v);
// {action=search}

glyph_free(glyph);
glyph_value_free(v);
```

#### `glyph_to_json(glyph_value_t *v) -> char*`

```c
glyph_value_t *v = glyph_parse("{name=Alice}");
char *json = glyph_to_json(v);
// {"name":"Alice"}

glyph_free(json);
glyph_value_free(v);
```

**[Full C Documentation →](../c/README.md)**

---

## Common Patterns Across Languages

### Pattern 1: JSON Replacement

**Python:**
```python
text = glyph.json_to_glyph(data)
```

**Go:**
```go
text := glyph.CanonicalizeLoose(glyph.FromJSONLoose(data))
```

**JavaScript:**
```typescript
const text = emit(fromJSON(data));
```

### Pattern 2: Fingerprinting

**Python:**
```python
hash = glyph.fingerprint_loose(data)
```

**Go:**
```go
hash := glyph.FingerprintLoose(glyph.FromJSONLoose(data))
```

**JavaScript:**
```typescript
const hash = fingerprintLoose(data);
```

### Pattern 3: Streaming Validation

**Python:**
```python
validator = StreamingValidator(registry)
for token in stream:
    result = validator.push(token)
```

**Go:**
```go
validator := glyph.NewStreamingValidator(registry)
for token := range stream {
    result := validator.Push(token)
}
```

**JavaScript:**
```typescript
const validator = new StreamingValidator(registry);
for (const token of stream) {
    const result = validator.push(token);
}
```

---

## Related Documentation

- [Quickstart](QUICKSTART.md) - Get started in 5 minutes
- [Complete Guide](GUIDE.md) - Features and patterns
- [Specifications](SPECIFICATIONS.md) - Technical specs

---

**Questions?** Open an [issue](https://github.com/Neumenon/glyph/issues) or check [discussions](https://github.com/Neumenon/glyph/discussions).
