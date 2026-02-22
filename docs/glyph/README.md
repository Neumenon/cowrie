# GLYPH

**Token-efficient serialization and streaming protocol for AI agents.**

```python
# JSON: 58 tokens
{"action": "search", "query": "weather in NYC", "max_results": 10, "filters": {"type": "forecast"}}

# GLYPH: 34 tokens
{action=search query="weather in NYC" max_results=10 filters={type=forecast}}
```

40% fewer tokens. Human-readable. Schema-optional. Streaming validation.

---

## Why GLYPH?

**The problem:** LLM context windows are expensive. JSON wastes tokens on quotes, colons, commas, and repeated keys. Validating tool calls requires waiting for complete responses.

**GLYPH solves this:**

| Capability | What it means |
|------------|---------------|
| **Token efficiency** | 30-50% smaller than JSON for structured data |
| **Streaming validation** | Detect errors as they stream, cancel immediately—not after full generation |
| **State-verified patches** | Cryptographic proof you're updating what you think you're updating |
| **Human-readable** | Debug without tools — it's just text |
| **Schema-optional** | Works without coordination; add schemas when you need them |

---

## Install

```bash
pip install glyph-serial
```

<details>
<summary>Other languages</summary>

```bash
# Go
go get github.com/anthropics/glyph

# JavaScript / TypeScript
npm install @anthropics/glyph
```

</details>

---

## Quick Start

### Encode & Decode

```python
import glyph

# Build a value
match = glyph.struct("Match",
    home="Arsenal",
    away="Liverpool", 
    score=[2, 1]
)

# Emit as GLYPH text
text = glyph.emit(match)
print(text)
# Output: Match{away=Liverpool home=Arsenal score=[2 1]}

# Parse it back
result = glyph.parse(text)
print(result.value["home"])  # Arsenal
```

### Drop-in JSON Replacement

```python
import glyph

# Your existing JSON
data = {"name": "Alice", "scores": [95, 87, 92], "active": True}

# Convert to GLYPH (40% fewer tokens)
text = glyph.from_json(data)
print(text)
# Output: {active=t name=Alice scores=[95 87 92]}

# Parse back to Python dict
result = glyph.to_json(text)
assert result == data
```

### With LLM APIs

```python
import glyph
from anthropic import Anthropic

client = Anthropic()

# Define tools in GLYPH (more compact in system prompt)
tools_glyph = """
Tool{name=search args={query:str max_results:int}}
Tool{name=calculate args={expression:str}}
"""

# Parse tool calls from model output
response = client.messages.create(
    model="claude-sonnet-4-20250514",
    messages=[{"role": "user", "content": "Search for weather in NYC"}],
    # ... 
)

# GLYPH tool call is smaller than JSON
tool_call = glyph.parse(response.content)
print(tool_call["action"])  # search
```

---

## Streaming Validation

Validate LLM tool calls **as tokens arrive**. Reject bad calls before generation completes.

```python
import glyph

# Define allowed tools with constraints
registry = glyph.ToolRegistry()
registry.register(
    name="search",
    args={
        "query": {"type": "str", "required": True, "min_len": 1},
        "max_results": {"type": "int", "min": 1, "max": 100},
    }
)
registry.register(
    name="calculate",
    args={
        "expression": {"type": "str", "required": True},
    }
)

# Validate incrementally as tokens stream
validator = glyph.StreamingValidator(registry)

async for token in llm_stream:
    result = validator.push(token)
    
    # Tool detected early (before response complete)
    if result.tool_name:
        print(f"Tool: {result.tool_name} (detected at token {result.tool_detected_at_token})")
    
    # Unknown tool? Stop generation immediately
    if result.tool_name and not result.tool_allowed:
        await cancel_generation()
        raise ValueError(f"Unknown tool: {result.tool_name}")
    
    # Constraint violation? Stop early
    if result.should_stop():
        await cancel_generation()
        raise ValueError(f"Validation error: {result.errors}")

# After stream completes
if result.complete and result.valid:
    execute_tool(result.tool_name, result.fields)
```

**Why this matters:** If the model hallucinates a tool name or violates constraints, you detect it as it streams and cancel immediately. Saves tokens, time, and reduces failures.

---

## Encoding Modes

GLYPH has three encoding modes. The encoder picks automatically, or you can specify.

### Struct Mode (default, human-friendly)

```
Match{home=Arsenal away=Liverpool score=[2 1]}
```

### Packed Mode (minimal tokens)

```
Match@(Arsenal Liverpool [2 1])
```

Fields encoded positionally by schema order. 20-30% smaller than struct mode.

```python
schema = glyph.Schema()
schema.add_packed("Match", fields=["home", "away", "score"])

text = glyph.emit(match, schema=schema, mode="packed")
# Output: Match@(Arsenal Liverpool [2 1])
```

### Tabular Mode (bulk data)

```
@tab Match [home away score]
Arsenal Liverpool [2 1]
Chelsea "Man City" [1 1]
Everton Newcastle [0 3]
@end
```

Column headers once, then rows. **50-70% smaller than JSON arrays** for homogeneous lists.

```python
matches = [match1, match2, match3]  # List of Match objects

# Emit as table
text = glyph.emit_tabular(matches, schema)

# Or stream rows incrementally
writer = glyph.TabularWriter(schema, "Match")
for match in match_stream:
    writer.write_row(match)
output = writer.finish()
```

**Use case:** Streaming embeddings, batch inference results, dataset rows, metrics.

---

## Schemas

Schemas are optional. Add them for:
- Wire key compression (`home` → `h`)
- Validation with constraints
- Packed/tabular encoding

```python
import glyph

schema = glyph.Schema()

# Define types with short wire keys
schema.add_struct("Team",
    fields={
        "id": {"type": "id", "wire_key": "t"},
        "name": {"type": "str", "wire_key": "n"},
        "league": {"type": "str", "wire_key": "l", "optional": True},
    }
)

schema.add_struct("Match",
    fields={
        "id": {"type": "id", "wire_key": "m"},
        "home": {"type": "Team", "wire_key": "H"},
        "away": {"type": "Team", "wire_key": "A"},
        "score": {"type": "list[int]", "wire_key": "s", "optional": True},
    },
    packed=True,  # Enable packed mode
    tabular=True,  # Enable tabular mode
)

# Emit with wire keys (maximum compression)
text = glyph.emit(match, schema=schema, use_wire_keys=True)
# Output: Match{m=^m:ARS-LIV H=Team{t=^t:ARS n=Arsenal} A=Team{...}}
```

### Constraints

```python
schema.add_struct("Player",
    fields={
        "name": {"type": "str", "min_len": 1, "max_len": 100},
        "age": {"type": "int", "min": 16, "max": 50},
        "email": {"type": "str", "pattern": r"^[^@]+@[^@]+\.[^@]+$"},
        "positions": {"type": "list[str]", "non_empty": True, "unique": True},
        "rating": {"type": "float", "min": 0.0, "max": 100.0},
    }
)

# Validate
result = schema.validate(player, "Player")
if not result.valid:
    print(result.errors)  # [ValidationError(path="age", message="value 15 < min 16")]
```

---

## GS1: Streaming Transport

GS1 is a framing protocol for streaming GLYPH over connections. It provides:

- **Multiplexing**: Multiple streams over one connection (via stream ID)
- **Ordering**: Sequence numbers per stream
- **Integrity**: CRC-32 checksums
- **State verification**: SHA-256 base hash for patches
- **Frame types**: doc, patch, row, ui, ack, err, ping, pong

### Wire Format

```
@frame{v=1 sid=1 seq=0 kind=doc len=42 crc=a1b2c3d4}
Match{home=Arsenal away=Liverpool score=[2 1]}

@frame{v=1 sid=1 seq=1 kind=patch len=18 base=sha256:abc123...}
@patch
= score [3 1]
@end

@frame{v=1 sid=1 seq=2 kind=ui len=28}
Progress{pct=0.75 msg="processing"}
```

### Writing Frames

```python
from glyph import stream

writer = stream.Writer(connection)

# Send document
writer.write_frame(
    sid=1,
    seq=0,
    kind="doc",
    payload=glyph.emit(match),
)

# Send progress update
writer.write_frame(
    sid=1,
    seq=1, 
    kind="ui",
    payload=stream.progress(0.5, "processing step 2 of 4"),
)

# Send with integrity check
writer.write_frame(
    sid=1,
    seq=2,
    kind="doc",
    payload=data,
    crc=True,  # Auto-compute CRC-32
)
```

### Reading with State Tracking

```python
from glyph import stream

handler = stream.FrameHandler()

@handler.on_doc
def handle_doc(sid: int, seq: int, payload: bytes, state: stream.SIDState):
    result = glyph.parse(payload)
    # Update tracked state (for patch verification)
    handler.cursor.set_state(sid, result.value)
    return process_document(result.value)

@handler.on_patch
def handle_patch(sid: int, seq: int, payload: bytes, state: stream.SIDState):
    # Base hash already verified by handler
    patch = glyph.parse_patch(payload)
    new_state = apply_patch(state.value, patch)
    handler.cursor.set_state(sid, new_state)
    return new_state

@handler.on_ui
def handle_ui(sid: int, seq: int, payload: bytes, state: stream.SIDState):
    event = stream.parse_ui_event(payload)
    if event.type == "Progress":
        update_progress_bar(event.fields["pct"], event.fields["msg"])

@handler.on_error
def handle_error(sid: int, seq: int, payload: bytes, state: stream.SIDState):
    error = glyph.parse(payload)
    logger.error(f"Stream {sid} error: {error['code']} - {error['msg']}")

# Process incoming frames
reader = stream.Reader(connection)
async for frame in reader:
    await handler.handle(frame)
```

### State-Verified Patches

Patches include the SHA-256 hash of the expected base state. Receivers reject patches that don't match.

```python
# Sender: include base hash for safety
current_hash = stream.state_hash(current_state)
writer.write_frame(
    sid=1,
    seq=5,
    kind="patch",
    payload=patch_bytes,
    base=current_hash,  # SHA-256 of current state
)

# Receiver: verification is automatic
@handler.on_base_mismatch
def handle_mismatch(sid: int, frame: stream.Frame):
    # State diverged — request full resync
    logger.warning(f"State mismatch on stream {sid}, requesting resync")
    request_resync(sid)
```

**Why this matters:** In distributed agents, state can diverge. Base hashes ensure patches apply cleanly or fail fast.

---

## Type Reference

### Scalar Types

| Type | GLYPH | Python |
|------|-------|--------|
| null | `_` (default) or `∅` | `None` |
| bool | `t`, `f` | `True`, `False` |
| int | `42`, `-100` | `int` |
| float | `3.14`, `1e-10` | `float` |
| str | `hello`, `"with spaces"` | `str` |
| bytes | `b64"SGVsbG8="` | `bytes` |
| time | `2025-12-19T20:00:00Z` | `datetime` |
| id | `^prefix:value` | `glyph.RefID` |

### Container Types

| Type | GLYPH | Python |
|------|-------|--------|
| list | `[1 2 3]` | `list` |
| map | `{a=1 b=2}` | `dict` |
| struct | `Type{field=value}` | `glyph.Struct` or `dict` |
| sum | `Success(42)` | `glyph.Sum` |

### Syntax Flexibility

GLYPH accepts multiple separator styles (parsed identically):

```python
# All equivalent:
glyph.parse("{a=1 b=2}")      # Space-separated, = 
glyph.parse("{a:1, b:2}")     # Comma-separated, :
glyph.parse("{a=1, b=2}")     # Mixed
glyph.parse("[1 2 3]")        # Space-separated list
glyph.parse("[1, 2, 3]")      # Comma-separated list
```

---

## Comparison

| Feature | GLYPH | JSON | Protobuf | MsgPack |
|---------|-------|------|----------|---------|
| Human-readable | ✅ | ✅ | ❌ | ❌ |
| Token-efficient | ✅ | ❌ | ✅ | ✅ |
| Schema-optional | ✅ | ✅ | ❌ | ✅ |
| Streaming validation | ✅ | ❌ | ❌ | ❌ |
| State-verified patches | ✅ | ❌ | ❌ | ❌ |
| No code generation | ✅ | ✅ | ❌ | ✅ |
| Tabular mode | ✅ | ❌ | ❌ | ❌ |
| Drop-in JSON replacement | ✅ | — | ❌ | ❌ |

---

## Use Cases

### Agent Tool Calling

```python
# Compact tool definitions in system prompt
tools = """
SearchTool{query:str max_results:int[1..100]}
CalculateTool{expression:str}
BrowseTool{url:str}
"""

# Validate tool calls as they stream
validator = glyph.StreamingValidator.from_schema(tools)
```

### Streaming Inference Results

```python
# Stream embeddings in tabular format
writer = glyph.TabularWriter(schema, "Embedding")
for batch in model.embed_batches(texts):
    for i, vec in enumerate(batch):
        writer.write_row({"id": i, "vector": vec.tolist()})

# 60% smaller than JSON arrays
```

### Agent State Sync

```python
# Sync agent state across processes with verified patches
writer.write_frame(
    kind="patch",
    payload=glyph.emit_patch([
        ("=", "memory.last_query", "weather in NYC"),
        ("+", "memory.context", new_context),
        ("~", "memory.turn_count", 1),  # Increment
    ]),
    base=current_state_hash,
)
```

### Checkpoint/Resume

```python
# Save agent state
with open("checkpoint.glyph", "w") as f:
    f.write(glyph.emit(agent_state))

# Restore
with open("checkpoint.glyph") as f:
    agent_state = glyph.parse(f.read()).value
```

---

## Examples

See [`examples/`](./examples/) for complete runnable code:

| Example | Description |
|---------|-------------|
| [tool-calling](./examples/tool-calling/) | Streaming validation for LLM tool calls |
| [langchain-integration](./examples/langchain/) | GLYPH with LangChain agents |
| [data-pipeline](./examples/data-pipeline/) | Tabular mode for batch processing |
| [agent-streaming](./examples/agent-streaming/) | GS1 with progress events |
| [json-migration](./examples/json-migration/) | Gradual migration from JSON |

---

## Performance

Token count comparison (cl100k_base tokenizer):

| Payload | JSON | GLYPH | Reduction |
|---------|------|-------|-----------|
| Simple tool call | 42 | 28 | 33% |
| Nested response | 156 | 98 | 37% |
| Tabular (10 rows) | 320 | 145 | 55% |
| Agent trace | 890 | 520 | 42% |

Python throughput (M3 MacBook Pro):

```
parse (small):    450k ops/sec
parse (medium):    85k ops/sec
emit (small):     620k ops/sec
emit_tabular:     180k rows/sec
```

---

## API Reference

Full documentation: [glyph-serial.readthedocs.io](https://glyph-serial.readthedocs.io/)

### Core

```python
glyph.parse(text: str) -> ParseResult
glyph.emit(value) -> str
glyph.from_json(data: dict) -> str
glyph.to_json(text: str) -> dict
```

### Streaming

```python
glyph.StreamingValidator(registry)
glyph.TabularWriter(schema, type_name)
```

### Transport

```python
glyph.stream.Writer(conn)
glyph.stream.Reader(conn)
glyph.stream.FrameHandler()
```

---

## Contributing

```bash
git clone https://github.com/anthropics/glyph
cd glyph
pip install -e ".[dev]"
pytest
```

---

## License

MIT

---

<p align="center">
  <b>Built for the age of AI agents.</b><br>
  <sub>Less tokens. More context. Safer streaming.</sub>
</p># GLYPH

**Token-efficient serialization and streaming protocol for AI agents.**

```
JSON:   {"action":"search","query":"weather NYC","max_results":10}
GLYPH:  {action=search query="weather NYC" max_results=10}
```

40% fewer tokens. Human-readable. Schema-optional. Streaming validation.

---

## Why GLYPH?

**The problem:** LLM context windows are expensive. JSON wastes tokens on quotes, colons, and repeated keys. Validating tool calls requires waiting for complete responses.

**GLYPH solves this:**

| Capability | What it means |
|------------|---------------|
| **Token efficiency** | 30-50% smaller than JSON for structured data |
| **Streaming validation** | Detect errors as they stream, cancel immediately—not after full generation |
| **State-verified patches** | Cryptographic proof you're updating what you think you're updating |
| **Human-readable** | Debug without tools — it's just text |
| **Schema-optional** | Works without coordination; add schemas when you need them |

---

## Install

```bash
# Go
go get github.com/anthropics/glyph

# Python
pip install glyph-serial

# JavaScript
npm install @anthropics/glyph
```

---

## Quick Start

### Encode & Decode

```go
package main

import (
    "fmt"
    "github.com/anthropics/glyph"
)

func main() {
    // Build a value
    match := glyph.Struct("Match",
        glyph.FieldVal("home", glyph.Str("Arsenal")),
        glyph.FieldVal("away", glyph.Str("Liverpool")),
        glyph.FieldVal("score", glyph.List(glyph.Int(2), glyph.Int(1))),
    )

    // Emit as GLYPH text
    text := glyph.Emit(match)
    fmt.Println(text)
    // Output: Match{away=Liverpool home=Arsenal score=[2 1]}

    // Parse it back
    result, _ := glyph.Parse(text)
    home := result.Value.Get("home").AsStr()
    fmt.Println(home) // Arsenal
}
```

### Drop-in JSON Replacement

```go
// Convert JSON to GLYPH
jsonData := []byte(`{"name": "Alice", "scores": [95, 87, 92]}`)
gval, _ := glyph.FromJSONLoose(jsonData)

// Canonical GLYPH output
fmt.Println(glyph.CanonicalizeLoose(gval))
// Output: {name=Alice scores=[95 87 92]}
// Note: auto-tabular is enabled by default (eligible lists of objects with >=50% shared keys).
//       Use CanonicalizeLooseNoTabular for non-tabular output.

// Convert back to JSON
jsonOut, _ := glyph.ToJSONLoose(gval)
```

---

## Streaming Validation

Validate LLM tool calls **as tokens arrive**. Reject bad calls before generation completes.

```go
// Define allowed tools
registry := glyph.NewToolRegistry()
registry.Register(&glyph.ToolSchema{
    Name: "search",
    Args: map[string]glyph.ArgSchema{
        "query":       {Type: "string", Required: true, MinLen: glyph.MinInt(1)},
        "max_results": {Type: "int", Min: glyph.MinFloat64(1), Max: glyph.MaxFloat64(100)},
    },
})

// Validate incrementally
validator := glyph.NewStreamingValidator(registry)

for token := range llmOutputStream {
    result := validator.PushToken(token)
    
    // Tool detected before response complete
    if result.ToolName != "" {
        fmt.Printf("Tool: %s (token %d)\n", result.ToolName, result.ToolDetectedAtToken)
    }
    
    // Unknown tool? Stop generation immediately
    if result.ToolAllowed != nil && !*result.ToolAllowed {
        cancelGeneration()
        break
    }
    
    // Constraint violation? Stop early
    if result.ShouldStop() {
        cancelGeneration()
        break
    }
}

// Timeline shows exactly when events occurred
fmt.Printf("Tool detected at token %d, char %d\n", 
    result.ToolDetectedAtToken, result.ToolDetectedAtChar)
```

---

## Encoding Modes

GLYPH has three encoding modes. Use `ModeAuto` and let the encoder pick.

### Struct Mode (default, human-friendly)

```
Match{home=Arsenal away=Liverpool score=[2 1]}
```

### Packed Mode (minimal tokens)

```
Match@(Arsenal Liverpool [2 1])
```

Fields encoded positionally by schema order. 20-30% smaller than struct mode.

### Tabular Mode (bulk data)

```
@tab Match [home away score]
Arsenal Liverpool [2 1]
Chelsea "Man City" [1 1]
Everton Newcastle [0 3]
@end
```

Column headers once, then rows. 50-70% smaller than JSON arrays for homogeneous lists.

```go
// Encode as tabular
matches := glyph.List(match1, match2, match3)
output, _ := glyph.EmitTabular(matches, schema)

// Stream rows incrementally  
writer := glyph.NewTabularWriter(typeDef, opts)
for _, row := range rows {
    writer.WriteRow(row)
}
result, _ := writer.Finish()
```

---

## Schemas

Schemas are optional. Add them for:
- Wire key compression (`home` → `h`)
- Validation with constraints
- Packed/tabular encoding

```go
schema := glyph.NewSchemaBuilder().
    AddPackedStruct("Match", "v1",
        glyph.Field("id", glyph.PrimitiveType("id"), glyph.WithFID(1), glyph.WithWireKey("m")),
        glyph.Field("home", glyph.RefType("Team"), glyph.WithFID(2), glyph.WithWireKey("h")),
        glyph.Field("away", glyph.RefType("Team"), glyph.WithFID(3), glyph.WithWireKey("a")),
        glyph.Field("score", glyph.ListType(glyph.PrimitiveType("int")), glyph.WithFID(4), glyph.WithOptional()),
    ).
    AddPackedStruct("Team", "v1",
        glyph.Field("id", glyph.PrimitiveType("id"), glyph.WithFID(1)),
        glyph.Field("name", glyph.PrimitiveType("str"), glyph.WithFID(2)),
    ).
    WithPack("Match").
    WithTab("Match").
    Build()

// Emit with wire keys
opts := glyph.EmitOptions{Schema: schema, UseWireKeys: true}
output := glyph.EmitWithOptions(match, opts)
// Output: Match{m=^m:ARS-LIV h=Team{...} a=Team{...}}
```

### Constraints

```go
glyph.Field("score", glyph.PrimitiveType("int"),
    glyph.WithConstraint(glyph.MinConstraint(0)),
    glyph.WithConstraint(glyph.MaxConstraint(100)),
)

glyph.Field("email", glyph.PrimitiveType("str"),
    glyph.WithConstraint(glyph.RegexConstraint(`^[^@]+@[^@]+$`)),
)

glyph.Field("tags", glyph.ListType(glyph.PrimitiveType("str")),
    glyph.WithConstraint(glyph.NonEmptyConstraint()),
    glyph.WithConstraint(glyph.UniqueConstraint()),
)
```

---

## GS1: Streaming Transport

GS1 is a framing protocol for streaming GLYPH payloads. It provides:

- **Multiplexing**: Multiple streams over one connection (via SID)
- **Ordering**: Sequence numbers per stream
- **Integrity**: CRC-32 checksums
- **State verification**: SHA-256 base hash for patches
- **Frame types**: doc, patch, row, ui, ack, err, ping, pong

### Wire Format

```
@frame{v=1 sid=1 seq=0 kind=doc len=42 crc=a1b2c3d4}
Match{home=Arsenal away=Liverpool score=[2 1]}

@frame{v=1 sid=1 seq=1 kind=patch len=18 base=sha256:abc123...}
@patch
= score [3 1]
@end

@frame{v=1 sid=1 seq=2 kind=ui len=28}
Progress{pct=0.75 msg="loading"}
```

### Writing Frames

```go
import "github.com/anthropics/glyph/stream"

w := stream.NewWriter(conn)

// Send document
w.WriteFrame(&stream.Frame{
    SID:     1,
    Seq:     0,
    Kind:    stream.KindDoc,
    Payload: []byte(glyph.Emit(match)),
})

// Send progress update
w.WriteFrame(&stream.Frame{
    SID:     1,
    Seq:     1,
    Kind:    stream.KindUI,
    Payload: stream.EmitProgress(0.5, "processing"),
})
```

### Reading with State Tracking

```go
handler := stream.NewFrameHandler()

handler.OnDoc = func(sid, seq uint64, payload []byte, state *stream.SIDState) error {
    result, _ := glyph.Parse(string(payload))
    // Update local state
    handler.Cursor.SetState(sid, result.Value)
    return nil
}

handler.OnPatch = func(sid, seq uint64, payload []byte, state *stream.SIDState) error {
    // Base hash already verified by handler
    // Apply patch to state.State
    return nil
}

handler.OnUI = func(sid, seq uint64, payload []byte, state *stream.SIDState) error {
    typeName, fields, _ := stream.ParseUIEvent(payload)
    if typeName == "Progress" {
        fmt.Printf("Progress: %.0f%% - %s\n", fields["pct"], fields["msg"])
    }
    return nil
}

handler.OnSeqGap = func(sid, expected, got uint64) error {
    // Request resync
    return fmt.Errorf("gap detected: expected %d, got %d", expected, got)
}

// Process frames
for {
    frame, err := reader.Next()
    if err == io.EOF {
        break
    }
    handler.Handle(frame)
}
```

### State-Verified Patches

Patches include the SHA-256 hash of the expected base state. Receivers reject patches that don't match.

```go
// Sender: include base hash
baseHash := stream.StateHashLoose(currentState)
w.WriteFrame(&stream.Frame{
    SID:     1,
    Seq:     5,
    Kind:    stream.KindPatch,
    Payload: patchBytes,
    Base:    &baseHash,
})

// Receiver: verification is automatic
handler.OnBaseMismatch = func(sid uint64, frame *stream.Frame) error {
    // State diverged — request full resync
    return requestResync(sid)
}
```

---

## Type Reference

### Scalar Types

| Type | Example | Notes |
|------|---------|-------|
| `null` | `_` (default) or `∅` | Underscore is the canonical default |
| `bool` | `t`, `f` | Single character |
| `int` | `42`, `-100` | 64-bit signed |
| `float` | `3.14`, `1e-10` | 64-bit IEEE 754 |
| `str` | `hello`, `"with spaces"` | Bare if safe, quoted otherwise |
| `bytes` | `b64"SGVsbG8="` | Base64 encoded |
| `time` | `2025-12-19T20:00:00Z` | ISO 8601 |
| `id` | `^prefix:value` | Reference ID with optional prefix |

### Container Types

| Type | Example |
|------|---------|
| `list` | `[1 2 3]` or `[1, 2, 3]` |
| `map` | `{a=1 b=2}` or `{a:1, b:2}` |
| `struct` | `Type{field=value}` |
| `sum` | `Success(42)` or `Error{code=404}` |

---

## Comparison

| Feature | GLYPH | JSON | Protobuf | MessagePack |
|---------|-------|------|----------|-------------|
| Human-readable | ✅ | ✅ | ❌ | ❌ |
| Token-efficient | ✅ | ❌ | ✅ | ✅ |
| Schema-optional | ✅ | ✅ | ❌ | ✅ |
| Streaming validation | ✅ | ❌ | ❌ | ❌ |
| State-verified patches | ✅ (GS1) | ❌ | ❌ | ❌ |
| No code generation | ✅ | ✅ | ❌ | ✅ |
| Tabular mode | ✅ | ❌ | ❌ | ❌ |

---

## Project Structure

```
glyph/
├── types.go          # Core value types
├── token.go          # Lexer
├── parse.go          # Parser
├── emit.go           # Struct-mode encoder
├── emit_packed.go    # Packed-mode encoder
├── emit_tabular.go   # Tabular-mode encoder
├── validate.go       # Schema validation
├── stream_validator.go # Streaming validation
├── json_bridge.go    # JSON interop
└── schema.go         # Schema builder

stream/
├── types.go          # GS1 frame types
├── gs1t_reader.go    # Frame reader
├── gs1t_writer.go    # Frame writer
├── cursor.go         # State tracking
├── hash.go           # State hashing
└── ui_events.go      # Standard UI events
```

---

## Examples

See [`examples/`](./examples/) for complete examples:

- **[tool-calling](./examples/tool-calling/)** — Streaming validation for LLM tool calls
- **[data-pipeline](./examples/data-pipeline/)** — Tabular mode for bulk data
- **[agent-streaming](./examples/agent-streaming/)** — GS1 with progress events
- **[json-migration](./examples/json-migration/)** — Drop-in JSON replacement

---

## Performance

Benchmarks on M3 MacBook Pro (see `loose_bench_test.go`):

```
BenchmarkCanonicalizeLoose_Map_Small-8       2000000     580 ns/op      312 B/op     8 allocs/op
BenchmarkCanonicalizeLoose_Map_Medium-8       300000    4200 ns/op     2048 B/op    52 allocs/op
BenchmarkCanonicalizeLoose_Nested_Deep-8      500000    2800 ns/op     1536 B/op    40 allocs/op
BenchmarkCanonicalizeLoose_LLMToolCall-8      400000    3100 ns/op     1792 B/op    45 allocs/op
BenchmarkCanonicalizeLoose_AgentTrace-8       150000    8500 ns/op     4096 B/op   102 allocs/op
```

Token count comparison (GPT-4 tokenizer, representative payloads):

| Payload | JSON tokens | GLYPH tokens | Reduction |
|---------|-------------|--------------|-----------|
| Tool call | 42 | 28 | 33% |
| API response | 156 | 98 | 37% |
| Tabular (10 rows) | 320 | 145 | 55% |
| Agent trace | 890 | 520 | 42% |

---

## Contributing

Contributions welcome. Please:

1. Run tests: `go test ./...`
2. Run benchmarks: `go test -bench=. -benchmem ./...`
3. Format code: `go fmt ./...`
4. Update golden tests if changing output: `go test -update-golden ./...`

### Cross-language parity

Changes to encoding rules must pass triple-implementation tests:

```bash
# Run Go tests
go test ./...

# Run Python tests  
cd python && pytest

# Run JS tests
cd js && npm test
```

---

## License

MIT License. See [LICENSE](./LICENSE).

---

## Links

- [Specification](./SPEC.md) — Formal grammar and encoding rules
- [API Reference](https://pkg.go.dev/github.com/anthropics/glyph) — Go documentation
- [Python Package](https://pypi.org/project/glyph-serial/) — PyPI
- [npm Package](https://www.npmjs.com/package/@anthropics/glyph) — npm

---

<p align="center">
  <sub>Built for the age of AI agents.</sub>
</p>
