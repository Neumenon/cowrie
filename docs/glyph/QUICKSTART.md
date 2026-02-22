# GLYPH Quickstart

**Get running with GLYPH in 5 minutes.**

---

## What is GLYPH?

Token-efficient serialization for AI agents. **30-56% fewer tokens** than JSON with **immediate streaming validation**.

```json
JSON:  {"action": "search", "query": "weather in NYC", "max_results": 10}
```
**140 tokens**

```glyph
GLYPH: {action=search query="weather in NYC" max_results=10}
```
**84 tokens** (40% reduction)

---

## Why Use It?

**Token Reduction**: No quotes on keys, no colons, no commas
**Streaming Validation**: Detect errors as they stream, cancel immediately. Saves tokens, time, and reduces failures
**Human-Readable**: Text format, easy to debug

---

## Install

| Language | Command |
|----------|---------|
| Python | `pip install glyph-serial` |
| Go | `go get github.com/Neumenon/glyph` |
| JavaScript | `npm install glyph-js` |
| Rust | `cargo add glyph-codec` |
| C | See [c/README.md](../c/README.md) |

---

## Basic Usage

### Python

```python
import glyph

# JSON to GLYPH (30-50% smaller)
data = {"action": "search", "query": "AI agents", "limit": 5}
glyph_str = glyph.json_to_glyph(data)
# Result: {action=search limit=5 query="AI agents"}

# GLYPH to JSON
restored = glyph.glyph_to_json(glyph_str)

# Parse GLYPH directly
result = glyph.parse('{name=Alice age=30 active=t}')
print(result.get("name").as_str())  # Alice
print(result.get("age").as_int())   # 30
```

### Go

```go
import "github.com/Neumenon/glyph/glyph"

// Parse GLYPH
text := `{action=search query=weather limit=10}`
val, err := glyph.Parse([]byte(text))
action := val.Get("action").String()  // "search"

// JSON to GLYPH
jsonData := map[string]interface{}{
    "status": "active",
    "count":  42,
}
v := glyph.FromJSONLoose(jsonData)
glyphText := glyph.CanonicalizeLoose(v)
// {count=42 status=active}
```

### JavaScript

```typescript
import { parse, emit, fromJSON, toJSON } from 'glyph-js';

// Parse GLYPH
const value = parse('{action=search query=test}');
console.log(value.get('action'));  // 'search'

// JSON to GLYPH
const data = { name: 'Alice', scores: [95, 87, 92] };
const glyphText = emit(fromJSON(data));
// {name=Alice scores=[95 87 92]}

// GLYPH to JSON
const jsonData = toJSON(value);
```

---

## Streaming Validation Example

Validate tool calls **during generation**, not after:

```python
from glyph import StreamingValidator

# Define tools
tools = {
    "search": {"query": "str", "limit": "int<1,100>"},
    "fetch": {"url": "str"}
}

validator = StreamingValidator(tools)

# Feed tokens as they arrive from LLM
tokens = ["{", "tool", "=", "unknown", "..."]
for token in tokens:
    result = validator.feed(token)
    if result.error:
        print(f"Invalid at token {result.token_count}: {result.error}")
        break  # Cancel immediately!
```

**Result**: Catch bad tool names, missing params, constraint violations **as they appear**. Saves tokens, time, and reduces failures.

---

## Auto-Tabular Mode

Homogeneous lists compress automatically:

```python
data = [
    {"id": "doc1", "score": 0.95},
    {"id": "doc2", "score": 0.89},
    {"id": "doc3", "score": 0.84}
]

print(glyph.json_to_glyph(data))
```

Output:
```glyph
@tab _ [id score]
|doc1|0.95|
|doc2|0.89|
|doc3|0.84|
@end
```

**50-70% smaller** than JSON arrays.

---

## When to Use GLYPH

### ✅ Use GLYPH:
- LLMs **reading** structured data (tool responses, state, batch data)
- Streaming validation needed
- Token budgets are tight
- Multi-agent systems

### ⚠️ Use JSON:
- LLMs **generating** output (they're trained on JSON)
- Existing JSON-only integrations

**Best Practice**: LLMs generate JSON → serialize to GLYPH for storage/transmission.

---

## Format Quick Reference

```
Null:    ∅ or _          List:    [1 2 3]
Bool:    t / f           Map:     {a=1 b=2}
Int:     42              String:  hello or "hello world"
Float:   3.14            Struct:  User{name=Alice age=30}
```

**vs JSON**: No commas · `=` not `:` · bare strings · `t`/`f` bools · `∅` null

---

## Next Steps

**Learn More**:
- [Complete Guide](GUIDE.md) - Features, patterns, best practices
- [AI Agent Patterns](AGENTS.md) - Tool calling, state management, ReAct loops
- [Technical Specs](SPECIFICATIONS.md) - Loose Mode, GS1 streaming protocol

**Examples**:
- [Cookbook](archive/COOKBOOK.md) - 10 practical recipes
- [Language-Specific Docs](../README.md#implementations) - Go, Python, JS, Rust, C

**Reports**:
- [Performance Benchmarks](reports/CODEC_BENCHMARK_REPORT.md)
- [LLM Accuracy](reports/LLM_ACCURACY_REPORT.md)

---

**Questions?** Open an [issue](https://github.com/Neumenon/glyph/issues) or check [discussions](https://github.com/Neumenon/glyph/discussions).
