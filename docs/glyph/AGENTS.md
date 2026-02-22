# GLYPH for AI Agents

Quick patterns for using GLYPH in agent systems.

**TL;DR:**
1. Define tools in GLYPH (40% fewer tokens in system prompt)
2. Validate tool calls as tokens stream (detect errors and cancel immediately—not after full generation)
3. Sync state with verified patches (cryptographic proof of consistency)

---

## Tool Definitions

### Minimal (no schema)

```python
# System prompt
TOOLS = """
Available tools:
- search{query:str max_results:int} - Search the web
- calculate{expression:str} - Evaluate math
- browse{url:str} - Fetch webpage content
"""

# Parse tool call from model output
tool_call = glyph.parse(model_output)
name = tool_call.type_name  # "search"
args = tool_call.fields     # {"query": "weather NYC", "max_results": 10}
```

### With Validation

```python
registry = glyph.ToolRegistry()

registry.register("search", {
    "query": {"type": "str", "required": True, "min_len": 1},
    "max_results": {"type": "int", "min": 1, "max": 100, "default": 10},
})

registry.register("calculate", {
    "expression": {"type": "str", "required": True},
})

registry.register("browse", {
    "url": {"type": "str", "required": True, "pattern": r"^https?://"},
})

# Validate before execution
result = registry.validate(tool_call)
if not result.valid:
    return f"Invalid tool call: {result.errors}"
```

### System Prompt Pattern

```
You have access to these tools:

search{query:str max_results:int[1..100]=10}
  Search the web. Returns list of results.

calculate{expression:str}
  Evaluate a mathematical expression. Returns number.

browse{url:str}
  Fetch and summarize a webpage. Returns text.

To use a tool, output:
ToolName{arg1=value arg2=value}

Example:
search{query="python async tutorial" max_results=5}
```

---

## Streaming Validation

Detect invalid tool calls **before generation completes**. Save tokens and latency.

```python
validator = glyph.StreamingValidator(registry)

async for token in llm_stream:
    result = validator.push(token)

    # Tool name detected early
    if result.tool_name:
        print(f"Tool: {result.tool_name} at token {result.token_index}")

        # Unknown tool? Cancel immediately
        if not result.tool_allowed:
            await cancel_generation()
            raise ToolNotFoundError(result.tool_name)

    # Constraint violation? Cancel
    if result.should_stop():
        await cancel_generation()
        raise ValidationError(result.errors)

# Generation complete - execute if valid
if result.complete and result.valid:
    return await execute_tool(result.tool_name, result.fields)
```

### When to Cancel

| Condition | Action |
|-----------|--------|
| Unknown tool name | Cancel at token ~3-5 |
| Wrong argument type | Cancel when type detected |
| Constraint violation | Cancel when value complete |
| Missing required arg | Wait until `}` then error |

---

## State Management

### Simple: Full State Per Message

```python
# Agent state as GLYPH
state = glyph.struct("AgentState",
    goal="Find weather in NYC",
    memory=[
        {"query": "NYC weather", "result": "72F sunny"},
    ],
    turn=3,
)

# Include in context
context = f"""
Current state:
{glyph.emit(state)}

Continue toward the goal.
"""
```

### Advanced: Patches with Verification

For long-running agents, send patches instead of full state.

```python
from glyph import stream

# Initial state
writer.write_frame(sid=1, seq=0, kind="doc", payload=glyph.emit(state))

# After each action, send patch
patch = glyph.patch([
    ("=", "turn", 4),                           # Set value
    ("+", "memory", new_memory_entry),          # Append
    ("~", "token_count", tokens_used),          # Increment
])

# Include base hash for safety
base_hash = stream.state_hash(current_state)
writer.write_frame(
    sid=1,
    seq=1,
    kind="patch",
    payload=patch,
    base=base_hash,  # Receiver rejects if state diverged
)
```

### Receiver Side

```python
handler = stream.FrameHandler()

@handler.on_patch
def handle_patch(sid, seq, payload, state):
    # Base hash already verified
    patch = glyph.parse_patch(payload)
    new_state = apply_patch(state.value, patch)
    handler.cursor.set_state(sid, new_state)
    return new_state

@handler.on_base_mismatch
def handle_mismatch(sid, frame):
    # State diverged - request full resync
    logger.warning(f"State mismatch on {sid}")
    request_full_state(sid)
```

---

## Common Patterns

### ReAct Loop

```python
async def react_loop(goal: str, max_turns: int = 10):
    state = {"goal": goal, "observations": [], "turn": 0}

    for turn in range(max_turns):
        # Format state as GLYPH (compact)
        state_glyph = glyph.from_json(state)

        prompt = f"""
State: {state_glyph}

Think step by step, then either:
1. Use a tool: ToolName{{args}}
2. Return final answer: Answer{{result="..."}}
"""

        response = await llm.generate(prompt)
        parsed = glyph.parse(response)

        if parsed.type_name == "Answer":
            return parsed.fields["result"]

        # Execute tool
        result = await execute_tool(parsed.type_name, parsed.fields)
        state["observations"].append({
            "tool": parsed.type_name,
            "args": parsed.fields,
            "result": result,
        })
        state["turn"] += 1

    raise MaxTurnsExceeded()
```

### Multi-Agent Coordination

Use stream IDs (SID) to multiplex agent communication.

```python
# Coordinator assigns SIDs
PLANNER_SID = 1
EXECUTOR_SID = 2
CRITIC_SID = 3

# Planner sends task
writer.write_frame(
    sid=EXECUTOR_SID,
    seq=0,
    kind="doc",
    payload=glyph.emit(glyph.struct("Task",
        action="search",
        query="latest AI news",
    ))
)

# Executor sends result
writer.write_frame(
    sid=PLANNER_SID,
    seq=0,
    kind="doc",
    payload=glyph.emit(glyph.struct("Result",
        task_id=1,
        status="complete",
        data=search_results,
    ))
)

# Critic sends feedback
writer.write_frame(
    sid=EXECUTOR_SID,
    seq=1,
    kind="doc",
    payload=glyph.emit(glyph.struct("Feedback",
        task_id=1,
        score=0.8,
        suggestion="Include source URLs",
    ))
)
```

### Checkpoint / Resume

```python
def save_checkpoint(agent_state, path: str):
    with open(path, "w") as f:
        f.write(glyph.emit(agent_state))

def load_checkpoint(path: str):
    with open(path) as f:
        return glyph.parse(f.read()).value

# Save periodically
if turn % 5 == 0:
    save_checkpoint(state, f"checkpoint_{turn}.glyph")

# Resume from crash
state = load_checkpoint("checkpoint_latest.glyph")
```

### Progress Reporting

```python
# Send progress during long operations
writer.write_frame(
    sid=1,
    seq=seq,
    kind="ui",
    payload=glyph.emit(glyph.struct("Progress",
        pct=0.45,
        msg="Processing batch 9 of 20",
        eta_seconds=120,
    ))
)

# Client handles UI updates
@handler.on_ui
def handle_ui(sid, seq, payload, state):
    event = glyph.parse(payload)
    if event.type_name == "Progress":
        update_progress_bar(event.fields["pct"], event.fields["msg"])
```

---

## Anti-Patterns

### Don't: Parse with Regex

```python
# BAD - breaks on nested structures
match = re.search(r'search\{query="([^"]+)"', response)

# GOOD - use the parser
result = glyph.parse(response)
```

### Don't: Validate After Full Generation

```python
# BAD - wastes tokens on invalid calls
response = await llm.generate(prompt)  # 50 tokens
result = glyph.parse(response)
if result.type_name not in allowed_tools:  # Discovered too late
    raise Error()

# GOOD - validate as tokens arrive
async for token in llm.stream(prompt):
    result = validator.push(token)
    if result.tool_name and not result.tool_allowed:
        await cancel()  # Stop at token 5
        break
```

### Don't: Send Full State Every Turn

```python
# BAD - O(n) tokens per turn for n observations
state["observations"].append(new_obs)
send_full_state(state)  # Gets bigger every turn

# GOOD - O(1) patches
patch = glyph.patch([("+", "observations", new_obs)])
send_patch(patch, base_hash=current_hash)
```

### Don't: Inline Large Data

```python
# BAD - bloats context
state = {"embeddings": [[0.1, 0.2, ...] * 1536] * 100}  # Huge

# GOOD - use blob references
state = {
    "embeddings_ref": glyph.blob(
        cid="sha256:abc123...",
        mime="application/octet-stream",
        bytes=614400,
        caption="100 embeddings, 1536-dim each",
    )
}
```

---

## Quick Reference

### GLYPH vs JSON Token Counts

| Pattern | JSON | GLYPH | Savings |
|---------|------|-------|---------|
| Tool call | 42 | 28 | 33% |
| Tool list (5 tools) | 180 | 95 | 47% |
| Agent state (small) | 120 | 75 | 38% |
| Agent state (large) | 500 | 290 | 42% |
| Tabular data (10 rows) | 320 | 145 | 55% |

### Type Cheatsheet

```
null:    _
bool:    t / f
int:     42
float:   3.14
string:  hello  or  "with spaces"
list:    [1 2 3]
map:     {a=1 b=2}
struct:  Type{field=value}
id:      ^prefix:value
```

### Frame Types (GS1)

| Kind | Use |
|------|-----|
| `doc` | Full state snapshot |
| `patch` | Incremental update |
| `row` | Single record (streaming tabular) |
| `ui` | Progress, logs, artifacts |
| `ack` | Acknowledgement |
| `err` | Error event |

---

## Links

- [README](./README.md) - Full documentation
- [LOOSE_MODE_SPEC](./LOOSE_MODE_SPEC.md) - Canonical encoding rules
- [GS1_SPEC](./stream/GS1_SPEC.md) - Streaming protocol
