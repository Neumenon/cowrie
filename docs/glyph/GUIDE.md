# GLYPH Complete Guide

**Comprehensive features, patterns, and best practices for GLYPH.**

---

## Table of Contents

1. [Core Concepts](#core-concepts)
2. [Token Efficiency](#token-efficiency)
3. [Streaming Validation](#streaming-validation)
4. [Auto-Tabular Mode](#auto-tabular-mode)
5. [State Management & Fingerprinting](#state-management--fingerprinting)
6. [JSON Interoperability](#json-interoperability)
7. [Common Patterns](#common-patterns)
8. [Performance Optimization](#performance-optimization)
9. [Migration from JSON](#migration-from-json)

---

## Core Concepts

### Why GLYPH Exists

**Problem 1: Token Waste**
JSON requires quotes around keys, colons, commas. This adds up:
- `{"action": "search"}` = 22 tokens
- `{action=search}` = 13 tokens
- **41% reduction** on a 2-field object

**Problem 2: Late Validation**
Traditional flow: LLM generates → you parse → validate → discover error
GLYPH flow: LLM generates → validate as tokens stream → cancel immediately if bad

**Problem 3: State Conflicts**
Two agents update the same state → conflicts → data loss
GLYPH: Base hash verification prevents applying patches to wrong version

### Format Basics

```glyph
Null:    ∅ or _
Bool:    t / f
Int:     42, -7
Float:   3.14, 1e-10
String:  hello or "hello world"
List:    [1 2 3]
Map:     {a=1 b=2}
Struct:  User{name=Alice age=30}
Ref:     ^user:abc123
Time:    2025-01-13T12:00:00Z
```

**Key differences from JSON:**
- No commas between elements
- `=` instead of `:`
- Bare strings (no quotes) when safe
- `t`/`f` instead of `true`/`false`
- `∅` or `_` instead of `null`

---

## Token Efficiency

### How It Works

GLYPH eliminates redundant syntax:

```json
JSON (58 tokens):
{"action": "search", "query": "weather in NYC", "max_results": 10}
```

```glyph
GLYPH (34 tokens):
{action=search query="weather in NYC" max_results=10}
```

**Savings breakdown:**
- No quotes on keys: 12 tokens
- No colons: 3 tokens
- No commas: 2 tokens
- Bare string values: 7 tokens

### Token Savings by Data Shape

| Structure | JSON | GLYPH | Savings |
|-----------|------|-------|---------|
| Flat object (5 fields) | 45 | 30 | 33% |
| Nested (3 levels) | 120 | 75 | 38% |
| Array of objects (10) | 300 | 160 | 47% |
| Tabular (20 rows) | 500 | 220 | 56% |

### When Token Savings Matter Most

**High-value scenarios:**
- System prompts with tool definitions (sent every request)
- Conversation history (grows over time)
- Batch operations (thousands of records)
- Multi-turn agents (state persisted across calls)

**Example: Tool Definition**

```json
JSON (180 tokens):
{
  "name": "search",
  "description": "Search the web",
  "parameters": {
    "query": {"type": "string", "required": true},
    "limit": {"type": "integer", "minimum": 1, "maximum": 100}
  }
}
```

```glyph
GLYPH (98 tokens):
{name=search description="Search the web" parameters={query={type=string required=t} limit={type=integer minimum=1 maximum=100}}}
```

**46% reduction** → Fits more tools in system prompt.

---

## Streaming Validation

### Why Validate While Streaming?

**Traditional approach:**
1. Wait for full response (50-150 tokens)
2. Parse complete JSON
3. Validate against schema
4. Discover tool doesn't exist or params invalid
5. **Wasted**: All those tokens, all that latency

**GLYPH approach:**
1. Tokens stream: `{tool=unknown...`
2. Error detected as soon as "unknown" appears
3. **Cancel immediately**
4. **Saved**: Tokens, time, reduced failures

### How It Works

```python
from glyph import StreamingValidator, ToolRegistry

# Define tools
registry = ToolRegistry()
registry.register(
    name="search",
    args={"query": "str", "limit": "int<1,100>"}
)

validator = StreamingValidator(registry)

# Feed tokens as they arrive
for token in llm_stream:
    result = validator.push(token)

    if result.tool_name and not result.tool_allowed:
        # Cancel immediately
        cancel_generation()
        break

    if result.has_errors():
        # Constraint violation - cancel immediately
        cancel_generation()
        break
```

### What Gets Validated

**Early in stream:**
- Tool name exists in registry
- Required fields present
- Field names are valid

**Mid-stream:**
- Type constraints (int, str, bool)
- Range constraints (min, max)
- Enum values

**Late in stream:**
- Complete structure
- All required fields have values

### Real-World Impact

**Example: Bad tool call**
- Traditional: Wait for full generation (150 tokens, 2 seconds), then discover error
- GLYPH streaming: Detect error as it appears, cancel immediately
- **Savings: Tokens, time, reduced failure rate**

---

## Auto-Tabular Mode

### Why Tables?

Homogeneous lists (same structure repeated) waste tokens on repeated keys:

```json
JSON (120 tokens):
[
  {"id": "doc1", "score": 0.95, "title": "GLYPH Guide"},
  {"id": "doc2", "score": 0.89, "title": "API Docs"},
  {"id": "doc3", "score": 0.84, "title": "Tutorial"}
]
```

```glyph
GLYPH table (45 tokens):
@tab _ [id score title]
|doc1|0.95|GLYPH Guide|
|doc2|0.89|API Docs|
|doc3|0.84|Tutorial|
@end
```

**62% reduction** → Keys appear once, not per row.

### When Auto-Tabular Triggers

GLYPH automatically detects homogeneous lists:
- All elements are objects
- All have the same keys
- At least 2 elements

```python
import glyph

data = [
    {"name": "Alice", "age": 28},
    {"name": "Bob", "age": 32},
    {"name": "Carol", "age": 25}
]

text = glyph.json_to_glyph(data)
# Automatically becomes table format
```

### Best Use Cases

**High-value:**
- Search results (10-100 rows)
- Embeddings with metadata
- Log entries
- Batch API responses
- Database query results

**Not ideal for:**
- Heterogeneous data (different structures)
- Single-item lists
- Deeply nested objects

### Manual Control

```python
# Force tabular mode
text = glyph.emit(data, mode="tabular")

# Disable tabular mode
text = glyph.emit(data, mode="loose")
```

---

## State Management & Fingerprinting

### The State Conflict Problem

**Scenario:**
1. Agent A reads state (version 1)
2. Agent B reads state (version 1)
3. Agent A writes update → version 2
4. Agent B writes update (thinks it's updating v1) → **conflict!**

### Solution: Base Fingerprints

```python
import glyph

# Agent A reads state
state = {" current": "processing", "count": 5}
base_hash = glyph.fingerprint_loose(state)
# base_hash: "sha256:a1b2c3..."

# Agent A creates patch
patch = {"count": 6}
update = glyph.create_patch(patch, base=base_hash)

# Server validates before applying
if glyph.verify_patch(current_state, update):
    apply(update)  # Safe - base matches
else:
    reject(update)  # Conflict - state changed
```

### How Fingerprinting Works

SHA-256 hash of canonical representation:

```python
state = {"user": "alice", "count": 42}
canonical = "{count=42 user=alice}"  # Keys sorted
hash = sha256(canonical)
```

**Properties:**
- Deterministic (same data → same hash)
- Collision-resistant (different data → different hash)
- Cross-language compatible (same hash in Go/Python/JS)

### Checkpoint & Resume

```python
# Save checkpoint
checkpoint = {
    "state": current_state,
    "hash": glyph.fingerprint_loose(current_state),
    "timestamp": now()
}
save(checkpoint)

# Resume later
loaded = load_checkpoint()
if glyph.fingerprint_loose(loaded["state"]) == loaded["hash"]:
    resume(loaded["state"])  # Integrity verified
else:
    error("Checkpoint corrupted")
```

---

## JSON Interoperability

### Drop-In Replacement

```python
import glyph

# Your existing code
data = {"action": "search", "query": "test"}

# One-line change
text = glyph.json_to_glyph(data)  # Instead of json.dumps()
restored = glyph.glyph_to_json(text)  # Instead of json.loads()

assert restored == data  # Perfect round-trip
```

### Gradual Migration

**Phase 1:** Generate JSON, store as GLYPH
```python
# LLM generates JSON (what it's trained on)
llm_output = '{"action": "search"}'
parsed = json.loads(llm_output)

# Convert to GLYPH for storage (40% smaller)
stored = glyph.json_to_glyph(parsed)
save_to_db(stored)
```

**Phase 2:** Retrieve GLYPH, send to LLM as JSON
```python
# Load from storage
stored = load_from_db()

# Convert to JSON for LLM
as_json = glyph.glyph_to_json(stored)
send_to_llm(as_json)
```

**Phase 3:** Ask LLM to generate GLYPH directly
```python
system_prompt = """
You are an AI assistant. When calling tools, use GLYPH format:
{tool_name=search query="example"}
"""
```

### Compatibility Notes

**Works perfectly:**
- Primitive types (string, number, boolean, null)
- Objects and arrays
- Nested structures
- Unicode strings

**GLYPH extensions (no JSON equivalent):**
- Typed references: `^user:abc123`
- Struct types: `User{name=Alice}`
- Explicit null: `∅`

---

## Common Patterns

### Pattern 1: Tool Calling

**Define tools in GLYPH (smaller system prompts):**

```glyph
Tools available:
- search{query=str max_results=int<1,100>}
- calculate{expression=str precision=int<0,15>}
- get_weather{location=str units=enum[celsius,fahrenheit]}
```

**LLM responds:**
```glyph
{tool=search query="GLYPH documentation" max_results=5}
```

**Validate during streaming**, execute if valid.

### Pattern 2: Agent Memory

```python
# Turn 1
conversation = [
    {turn=1 role=user msg="What's the weather in NYC?"}
]

# Turn 2 - append, don't recreate
conversation.append(
    {turn=2 role=assistant thought="Need weather data" action={tool=get_weather location=NYC}}
)

# Store efficiently (40% smaller than JSON)
stored = glyph.emit(conversation, mode="tabular")
```

### Pattern 3: Batch Operations

```python
# Process 100 embeddings
embeddings = [
    {"doc_id": f"doc_{i}", "vector": [...], "score": random()}
    for i in range(100)
]

# Auto-tabular saves 50-70%
glyph_text = glyph.json_to_glyph(embeddings)
send_to_vector_db(glyph_text)
```

### Pattern 4: Multi-Agent Coordination

```python
# Agent A sends message
msg = {
    from="agent_a"
    to="agent_b"
    type=request
    payload={action=search query="documentation"}
    correlation_id=msg_123
}

# Serialize with fingerprint for verification
text = glyph.emit(msg)
hash = glyph.fingerprint_loose(msg)

bus.publish(text, hash=hash)
```

---

## Performance Optimization

### When to Use GLYPH vs JSON

**Use GLYPH:**
- LLMs reading data (system prompts, context, state)
- Storage/transmission (databases, message queues, logs)
- Streaming scenarios (real-time validation needed)
- Tabular data (repeated structures)

**Use JSON:**
- LLMs generating output (trained on JSON)
- External API integrations (most APIs expect JSON)
- Browser/web contexts (native JSON support)

**Hybrid approach (best):**
```python
# LLM generates JSON
llm_output = generate(prompt)
parsed = json.loads(llm_output)

# Store as GLYPH (40% smaller)
glyph_text = glyph.json_to_glyph(parsed)
save_to_db(glyph_text)

# Load and send to next LLM as JSON
loaded = load_from_db()
as_json = glyph.glyph_to_json(loaded)
next_llm_call(as_json)
```

### Codec Performance

**Go implementation:**
- Canonicalization: 2M+ ops/sec
- Parsing: 1.5M+ ops/sec
- Fingerprinting: 500K+ ops/sec

**Python implementation:**
- Parsing: 50K+ ops/sec
- JSON conversion: 100K+ ops/sec

**Overhead:** <1ms for typical payloads (< 10KB)

---

## Migration from JSON

### Step 1: Identify High-Value Targets

**Priority areas:**
1. System prompts (sent every request)
2. Conversation history (grows over time)
3. Tool definitions (large schemas)
4. Batch operations (many records)

### Step 2: Convert Storage Layer

```python
# Before
def save_state(state):
    json_text = json.dumps(state)
    db.set("state", json_text)

# After
def save_state(state):
    glyph_text = glyph.json_to_glyph(state)
    db.set("state", glyph_text)

def load_state():
    glyph_text = db.get("state")
    return glyph.glyph_to_json(glyph_text)
```

### Step 3: Update LLM System Prompts

```python
# Before
system = """
Tools available (JSON format):
{"name": "search", "parameters": {"query": "string"}}
"""

# After
system = """
Tools available (GLYPH format - 40% fewer tokens):
{name=search parameters={query=string}}
"""
```

### Step 4: Measure Impact

```python
# Before
json_size = len(json.dumps(data))

# After
glyph_size = len(glyph.json_to_glyph(data))

savings = (json_size - glyph_size) / json_size * 100
print(f"Token savings: {savings:.1f}%")
```

---

## Next Steps

**Deep Dives:**
- [AI Agent Patterns](AGENTS.md) - LLM integration patterns
- [Technical Specifications](SPECIFICATIONS.md) - Format specs
- [API Reference](API_REFERENCE.md) - Per-language APIs

**Examples:**
- [Cookbook](archive/COOKBOOK.md) - 10 detailed recipes
- [Visual Guide](visual-guide.html) - Interactive examples

**Research:**
- [LLM Accuracy](reports/LLM_ACCURACY_REPORT.md) - Performance with language models
- [Benchmarks](reports/CODEC_BENCHMARK_REPORT.md) - Speed and efficiency metrics

---

**Questions?** Open an [issue](https://github.com/Neumenon/glyph/issues) or check [discussions](https://github.com/Neumenon/glyph/discussions).
