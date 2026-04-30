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

### JSON Bridge

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
| JSON bridge (round-trips) | ✅ | — | ❌ | ❌ |

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

This Cowrie-local copy is legacy documentation. Current examples live in the
standalone GLYPH docs:

- [Quickstart](../../../glyph/docs/QUICKSTART.md)
- [Cookbook](../../../glyph/docs/COOKBOOK.md)
- [Agent Patterns](../../../glyph/docs/AGENTS.md)

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
</p>
