# GLYPH Cookbook

Practical recipes for common use cases. All examples are copy-paste ready.

---

## Table of Contents

1. [Tool Calling with Streaming Validation](#1-tool-calling-with-streaming-validation)
2. [Drop-in JSON Replacement](#2-drop-in-json-replacement)
3. [Agent Memory & State Management](#3-agent-memory--state-management)
4. [Batch Data with Tabular Mode](#4-batch-data-with-tabular-mode)
5. [Real-time Progress Streaming](#5-real-time-progress-streaming)
6. [Multi-Agent Communication](#6-multi-agent-communication)
7. [LangChain Integration](#7-langchain-integration)
8. [Checkpoint & Resume](#8-checkpoint--resume)
9. [Schema Validation](#9-schema-validation)
10. [Custom Wire Protocol](#10-custom-wire-protocol)

---

## 1. Tool Calling with Streaming Validation

**Problem:** You're streaming LLM output and want to validate tool calls before generation completes. If the model hallucinates a tool, you want to cancel immediately.

**Solution:** Use `StreamingValidator` to check tokens incrementally.

```python
import glyph
import asyncio
from anthropic import AsyncAnthropic

# Define your tools with constraints
registry = glyph.ToolRegistry()

registry.register(
    name="search",
    description="Search the web",
    args={
        "query": {"type": "str", "required": True, "min_len": 1, "max_len": 500},
        "max_results": {"type": "int", "min": 1, "max": 20, "default": 10},
    }
)

registry.register(
    name="calculate",
    description="Evaluate a math expression",
    args={
        "expression": {"type": "str", "required": True},
        "precision": {"type": "int", "min": 0, "max": 15, "default": 2},
    }
)

registry.register(
    name="get_weather",
    description="Get weather for a location",
    args={
        "location": {"type": "str", "required": True},
        "units": {"type": "str", "enum": ["celsius", "fahrenheit"], "default": "celsius"},
    }
)


async def stream_with_validation(prompt: str):
    client = AsyncAnthropic()
    validator = glyph.StreamingValidator(registry)

    collected_tokens = []

    async with client.messages.stream(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
        system="Respond with tool calls in GLYPH format: ToolName{arg=value ...}"
    ) as stream:
        async for token in stream.text_stream:
            collected_tokens.append(token)
            result = validator.push(token)

            # Tool name detected
            if result.tool_name and result.tool_detected_at_token:
                print(f"[Token {result.tool_detected_at_token}] Tool detected: {result.tool_name}")

                # Unknown tool - cancel immediately
                if not result.tool_allowed:
                    print(f"[CANCEL] Unknown tool: {result.tool_name}")
                    await stream.close()
                    return None

            # Validation error - cancel
            if result.should_stop():
                print(f"[CANCEL] Validation error: {result.errors}")
                await stream.close()
                return None

    # Stream complete - execute if valid
    final = validator.finalize()
    if final.valid:
        print(f"[OK] Executing: {final.tool_name}")
        print(f"     Args: {final.fields}")
        return execute_tool(final.tool_name, final.fields)
    else:
        print(f"[INVALID] {final.errors}")
        return None


def execute_tool(name: str, args: dict):
    """Your tool execution logic here."""
    print(f"Executing {name} with {args}")
    # ... actual implementation
    return {"status": "ok", "tool": name}


# Usage
asyncio.run(stream_with_validation("What's the weather in Tokyo?"))
```

**Why this matters:**
- Unknown tool detected at token 3-5, not token 50+
- Constraint violations caught mid-stream
- Save 80%+ latency on bad requests

---

## 2. Drop-in JSON Replacement

**Problem:** You have existing JSON-based code but want token savings without a rewrite.

**Solution:** Use `from_json` and `to_json` for seamless conversion.

```python
import glyph
import json

# Your existing data structures work unchanged
user_data = {
    "id": "user_123",
    "name": "Alice Chen",
    "email": "alice@example.com",
    "preferences": {
        "theme": "dark",
        "notifications": True,
        "language": "en"
    },
    "tags": ["premium", "beta-tester", "early-adopter"],
    "metadata": {
        "created_at": "2024-01-15T10:30:00Z",
        "last_login": "2025-01-10T14:22:00Z",
        "login_count": 847
    }
}

# Convert to GLYPH (one line change)
glyph_text = glyph.json_to_glyph(user_data)
print("GLYPH output:")
print(glyph_text)
# {email=alice@example.com id=user_123 metadata={created_at=2024-01-15T10:30:00Z ...} ...}

# Compare sizes
json_text = json.dumps(user_data)
print(f"\nJSON:  {len(json_text)} chars")
print(f"GLYPH: {len(glyph_text)} chars")
print(f"Reduction: {100 * (1 - len(glyph_text)/len(json_text)):.1f}%")

# Convert back to Python dict (identical to original)
restored = glyph.glyph_to_json(glyph_text)
assert restored == user_data


# === Gradual Migration Pattern ===

def serialize(data: dict, use_glyph: bool = False) -> str:
    """Wrapper for gradual migration."""
    if use_glyph:
        return glyph.json_to_glyph(data)
    return json.dumps(data)


def deserialize(text: str) -> dict:
    """Auto-detect format and parse."""
    text = text.strip()
    if text.startswith("{") and "=" in text.split("\n")[0]:
        # Looks like GLYPH
        return glyph.glyph_to_json(text)
    return json.loads(text)


# Works with both formats
data1 = deserialize('{"name": "Bob"}')
data2 = deserialize('{name=Bob}')
assert data1 == data2


# === With LLM Context ===

def build_context(user: dict, history: list[dict]) -> str:
    """Build LLM context with GLYPH for token savings."""
    return f"""User: {glyph.json_to_glyph(user)}

History:
{chr(10).join(glyph.json_to_glyph(h) for h in history)}
"""

# 35-45% fewer tokens in your context window
```

**Token savings by data type:**

| Data Shape | JSON | GLYPH | Savings |
|------------|------|-------|---------|
| Flat object (5 fields) | ~45 tokens | ~30 tokens | 33% |
| Nested object (3 levels) | ~120 tokens | ~75 tokens | 38% |
| Array of objects (10 items) | ~300 tokens | ~160 tokens | 47% |
| API response (typical) | ~200 tokens | ~120 tokens | 40% |

---

## 3. Agent Memory & State Management

**Problem:** You're building a multi-step agent and need to track conversation history, tool results, and working memory efficiently.

**Solution:** Use GLYPH structs with state hashing for verified updates.

```python
import glyph
from glyph import g, field
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime
from typing import Optional
import hashlib


@dataclass
class AgentMemory:
    """Structured agent memory with GLYPH serialization."""

    conversation: list[dict] = dataclass_field(default_factory=list)
    tool_results: dict[str, any] = dataclass_field(default_factory=dict)
    working_memory: dict[str, any] = dataclass_field(default_factory=dict)
    plan: list[str] = dataclass_field(default_factory=list)
    current_step: int = 0

    def to_glyph(self) -> str:
        """Serialize to GLYPH format."""
        return glyph.emit(g.struct("AgentMemory",
            field("conversation", glyph.from_json(self.conversation)),
            field("tool_results", glyph.from_json(self.tool_results)),
            field("working_memory", glyph.from_json(self.working_memory)),
            field("plan", glyph.from_json(self.plan)),
            field("current_step", g.int(self.current_step)),
        ))

    @classmethod
    def from_glyph(cls, text: str) -> "AgentMemory":
        """Deserialize from GLYPH format."""
        v = glyph.parse(text)
        return cls(
            conversation=glyph.to_json(v.get("conversation")) or [],
            tool_results=glyph.to_json(v.get("tool_results")) or {},
            working_memory=glyph.to_json(v.get("working_memory")) or {},
            plan=glyph.to_json(v.get("plan")) or [],
            current_step=v.get("current_step").as_int() if v.get("current_step") else 0,
        )

    def state_hash(self) -> str:
        """Compute hash for state verification."""
        canonical = glyph.emit(glyph.from_json({
            "conversation": self.conversation,
            "tool_results": self.tool_results,
            "working_memory": self.working_memory,
            "plan": self.plan,
            "current_step": self.current_step,
        }))
        return hashlib.sha256(canonical.encode()).hexdigest()

    def add_message(self, role: str, content: str):
        self.conversation.append({
            "role": role,
            "content": content,
            "ts": datetime.utcnow().isoformat() + "Z"
        })

    def add_tool_result(self, tool: str, call_id: str, result: any):
        self.tool_results[call_id] = {
            "tool": tool,
            "result": result,
            "ts": datetime.utcnow().isoformat() + "Z"
        }

    def set_working(self, key: str, value: any):
        self.working_memory[key] = value

    def context_window(self, max_messages: int = 10) -> str:
        """Get recent context for LLM, GLYPH-formatted."""
        recent = self.conversation[-max_messages:]

        # Compact format for context window
        lines = []
        for msg in recent:
            role = "U" if msg["role"] == "user" else "A"
            lines.append(f"{role}: {msg['content']}")

        working = glyph.json_to_glyph(self.working_memory) if self.working_memory else "{}"

        return f"""Memory{{
  recent=[{chr(10).join(lines)}]
  working={working}
  step={self.current_step}
  plan={self.plan}
}}"""


class StatefulAgent:
    """Agent with verified state updates."""

    def __init__(self):
        self.memory = AgentMemory()
        self._last_hash: Optional[str] = None

    def checkpoint(self) -> tuple[str, str]:
        """Create a checkpoint with state hash."""
        state = self.memory.to_glyph()
        hash = self.memory.state_hash()
        self._last_hash = hash
        return state, hash

    def apply_update(self, update_fn, expected_base: Optional[str] = None):
        """Apply an update with optional base verification."""
        if expected_base and self._last_hash:
            if expected_base != self._last_hash:
                raise ValueError("State mismatch - concurrent modification detected")

        update_fn(self.memory)
        self._last_hash = self.memory.state_hash()

    def restore(self, state: str, expected_hash: Optional[str] = None):
        """Restore from checkpoint with optional verification."""
        self.memory = AgentMemory.from_glyph(state)
        actual_hash = self.memory.state_hash()

        if expected_hash and actual_hash != expected_hash:
            raise ValueError("Checkpoint corrupted - hash mismatch")

        self._last_hash = actual_hash


# === Usage ===

agent = StatefulAgent()

# Add conversation
agent.memory.add_message("user", "Find restaurants in SF")
agent.memory.add_message("assistant", "I'll search for restaurants in San Francisco.")
agent.memory.plan = ["search_restaurants", "filter_by_rating", "format_results"]
agent.memory.current_step = 0

# Checkpoint before tool execution
state, hash = agent.checkpoint()
print(f"Checkpoint hash: {hash[:16]}...")

# Execute step with verified update
def step_update(mem: AgentMemory):
    mem.add_tool_result("search", "call_1", {
        "restaurants": [
            {"name": "Flour + Water", "rating": 4.5},
            {"name": "State Bird", "rating": 4.7},
        ]
    })
    mem.current_step = 1

agent.apply_update(step_update, expected_base=hash)

# Get context for next LLM call
context = agent.memory.context_window()
print(context)

# Save to disk
with open("agent_state.glyph", "w") as f:
    f.write(agent.memory.to_glyph())
```

---

## 4. Batch Data with Tabular Mode

**Problem:** You're sending large datasets to/from LLMs (embeddings, search results, structured outputs) and JSON arrays are token-expensive.

**Solution:** Use tabular mode for 50-70% token savings on homogeneous lists.

```python
import glyph
from glyph import GValue, MapEntry


# === Basic Tabular Encoding ===

# Define your data
search_results = [
    {"id": "doc_1", "title": "Introduction to GLYPH", "score": 0.95},
    {"id": "doc_2", "title": "Streaming Validation", "score": 0.89},
    {"id": "doc_3", "title": "Agent State Management", "score": 0.84},
    {"id": "doc_4", "title": "Tabular Mode Guide", "score": 0.82},
    {"id": "doc_5", "title": "JSON Migration Path", "score": 0.78},
]

# Convert to GValue list of maps (auto-tabular kicks in for 3+ homogeneous items)
rows = GValue.list_(*[
    GValue.map_(
        MapEntry("id", GValue.str_(r["id"])),
        MapEntry("title", GValue.str_(r["title"])),
        MapEntry("score", GValue.float_(r["score"])),
    )
    for r in search_results
])

# Emit - will automatically use tabular format
table_text = glyph.emit(rows)
print(table_text)
# Output:
# @tab _ [id score title]
# |doc_1|0.95|Introduction to GLYPH|
# |doc_2|0.89|Streaming Validation|
# |doc_3|0.84|Agent State Management|
# |doc_4|0.82|Tabular Mode Guide|
# |doc_5|0.78|JSON Migration Path|
# @end

# Compare to JSON
import json
json_text = json.dumps(search_results)
print(f"\nJSON:    {len(json_text)} chars")
print(f"Tabular: {len(table_text)} chars")
print(f"Savings: {100 * (1 - len(table_text)/len(json_text)):.0f}%")

# Parse back
parsed = glyph.parse(table_text)
assert len(parsed) == 5
assert parsed.index(0).get("id").as_str() == "doc_1"


# === LLM Context with Tabular Data ===

def build_rag_context(query: str, results: list[dict], max_results: int = 5) -> str:
    """Build RAG context with tabular search results."""

    top_results = results[:max_results]

    # Build as GValue list
    rows = GValue.list_(*[
        GValue.map_(
            MapEntry("id", GValue.str_(r["id"])),
            MapEntry("title", GValue.str_(r["title"])),
            MapEntry("content", GValue.str_(r.get("content", "")[:200])),
        )
        for r in top_results
    ])

    docs_table = glyph.emit(rows)

    return f"""Query: {query}

Relevant documents:
{docs_table}

Based on these documents, provide a comprehensive answer."""


# === Structured Output Parsing ===

def parse_llm_table_output(output: str) -> list[dict]:
    """Parse LLM output that contains a GLYPH table."""

    # Find table boundaries
    start = output.find("@tab")
    end = output.find("@end", start) + 4

    if start == -1 or end == 3:
        raise ValueError("No table found in output")

    table_text = output[start:end]
    parsed = glyph.parse(table_text)

    # Convert to list of dicts
    result = []
    for i in range(len(parsed)):
        row = parsed.index(i)
        row_dict = {}
        for entry in row.as_map():
            row_dict[entry.key] = glyph.to_json(entry.value)
        result.append(row_dict)

    return result


# Example: Ask LLM to return structured data
prompt = """Analyze these companies and return a table with columns: [name sector market_cap growth_rate]

Companies: Apple, Microsoft, Google, Amazon, Meta

Return your analysis as a GLYPH table starting with @tab"""

# LLM returns:
llm_output = """Based on my analysis:

@tab _ [growth_rate market_cap name sector]
|0.08|3.0T|Apple|Technology|
|0.12|2.8T|Microsoft|Technology|
|0.10|1.9T|Google|Technology|
|0.15|1.8T|Amazon|Consumer/Tech|
|0.18|1.2T|Meta|Technology|
@end

Apple leads in market cap while Meta shows highest growth rate."""

companies = parse_llm_table_output(llm_output)
print(f"Parsed {len(companies)} companies")
for c in companies:
    print(f"  {c['name']}: {c['market_cap']} market cap, {float(c['growth_rate'])*100:.0f}% growth")
```

---

## 5. Real-time Progress Streaming

**Problem:** Long-running agent tasks need progress updates, but you don't want to mix progress with data in your protocol.

**Solution:** Use structured progress messages with GLYPH.

```python
import glyph
from glyph import g, field, GValue, MapEntry
import asyncio
from typing import AsyncIterator, Callable
from datetime import datetime


def emit_progress(pct: float, message: str) -> str:
    """Emit a progress update."""
    return glyph.emit(g.struct("Progress",
        field("pct", g.float(pct)),
        field("msg", g.str(message)),
        field("ts", g.str(datetime.utcnow().isoformat() + "Z")),
    ))


def emit_log(level: str, message: str) -> str:
    """Emit a log message."""
    return glyph.emit(g.struct("Log",
        field("level", g.str(level)),
        field("msg", g.str(message)),
        field("ts", g.str(datetime.utcnow().isoformat() + "Z")),
    ))


def emit_metric(name: str, value: float, unit: str = "") -> str:
    """Emit a metric."""
    return glyph.emit(g.struct("Metric",
        field("name", g.str(name)),
        field("value", g.float(value)),
        field("unit", g.str(unit)),
    ))


class ProgressReporter:
    """Stream progress events alongside data."""

    def __init__(self):
        self.events = []

    async def send_progress(self, pct: float, message: str):
        """Send progress update."""
        self.events.append(("progress", emit_progress(pct, message)))

    async def send_log(self, level: str, message: str):
        """Send log message."""
        self.events.append(("log", emit_log(level, message)))

    async def send_metric(self, name: str, value: float, unit: str = ""):
        """Send metric."""
        self.events.append(("metric", emit_metric(name, value, unit)))

    async def send_data(self, data: any, final: bool = False):
        """Send data."""
        self.events.append(("data", glyph.json_to_glyph(data)))

    async def send_row(self, row: dict):
        """Send a single row."""
        self.events.append(("row", glyph.json_to_glyph(row)))


async def process_documents_with_progress(
    docs: list[str],
    reporter: ProgressReporter,
    processor: Callable[[str], dict]
):
    """Process documents with real-time progress updates."""

    total = len(docs)
    await reporter.send_log("info", f"Starting processing of {total} documents")

    results = []
    start_time = asyncio.get_event_loop().time()

    for i, doc in enumerate(docs):
        # Progress update
        pct = (i + 1) / total
        await reporter.send_progress(pct, f"Processing document {i+1}/{total}")

        # Process document
        try:
            result = processor(doc)
            results.append(result)

            # Stream result immediately
            await reporter.send_row(result)

        except Exception as e:
            await reporter.send_log("error", f"Failed to process doc {i}: {e}")

        # Periodic metrics
        if (i + 1) % 10 == 0:
            elapsed = asyncio.get_event_loop().time() - start_time
            rate = (i + 1) / elapsed
            await reporter.send_metric("docs_per_sec", rate, "docs/s")

    # Final summary
    elapsed = asyncio.get_event_loop().time() - start_time
    await reporter.send_log("info", f"Completed {total} documents in {elapsed:.1f}s")
    await reporter.send_metric("total_time", elapsed, "seconds")

    # Send final result
    await reporter.send_data({"total": total, "successful": len(results)}, final=True)

    return results


# === Client-side progress handling ===

class ProgressUI:
    """Handle progress events on the client side."""

    def __init__(self):
        self.current_progress = 0.0
        self.current_message = ""
        self.logs = []
        self.metrics = {}

    def handle_event(self, event_type: str, payload: str):
        """Process event."""
        parsed = glyph.parse(payload)

        if parsed.as_struct().type_name == "Progress":
            self.current_progress = parsed.get("pct").as_float()
            self.current_message = parsed.get("msg").as_str() if parsed.get("msg") else ""
            self.render_progress()

        elif parsed.as_struct().type_name == "Log":
            level = parsed.get("level").as_str()
            msg = parsed.get("msg").as_str()
            self.logs.append((level, msg))
            self.render_log(level, msg)

        elif parsed.as_struct().type_name == "Metric":
            name = parsed.get("name").as_str()
            value = parsed.get("value").as_float()
            self.metrics[name] = value
            unit = parsed.get("unit").as_str() if parsed.get("unit") else ""
            self.render_metric(name, value, unit)

    def render_progress(self):
        """Render progress bar."""
        bar_width = 40
        filled = int(bar_width * self.current_progress)
        bar = "#" * filled + "-" * (bar_width - filled)
        print(f"\r[{bar}] {self.current_progress*100:.1f}% {self.current_message}", end="", flush=True)

    def render_log(self, level: str, msg: str):
        """Render log message."""
        print(f"\n[{level.upper()}] {msg}")

    def render_metric(self, name: str, value: float, unit: str):
        """Render metric."""
        print(f"\n* {name}: {value:.2f} {unit}")


# === Demo ===

async def demo():
    reporter = ProgressReporter()

    # Simulate document processing
    docs = [f"Document {i} content here" for i in range(25)]

    def mock_processor(doc: str) -> dict:
        import time
        time.sleep(0.02)  # Simulate work
        return {"doc": doc[:20], "tokens": len(doc.split())}

    results = await process_documents_with_progress(docs, reporter, mock_processor)

    # Show events
    print("\n\n=== Events generated ===")
    for event_type, payload in reporter.events[:10]:
        print(f"{event_type}: {payload[:60]}...")
    print(f"... and {len(reporter.events) - 10} more events")


if __name__ == "__main__":
    asyncio.run(demo())
```

---

## 6. Multi-Agent Communication

**Problem:** You have multiple agents that need to communicate structured messages efficiently.

**Solution:** Use GLYPH with typed message schemas.

```python
import glyph
from glyph import g, field
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Any
import asyncio


class MessageType(Enum):
    TASK = "task"
    RESULT = "result"
    QUERY = "query"
    RESPONSE = "response"
    ERROR = "error"
    HEARTBEAT = "heartbeat"


@dataclass
class AgentMessage:
    """Typed message for inter-agent communication."""

    type: MessageType
    from_agent: str
    to_agent: str
    payload: Any
    correlation_id: Optional[str] = None
    timestamp: Optional[str] = None

    def to_glyph(self) -> str:
        return glyph.emit(g.struct("Msg",
            field("type", g.str(self.type.value)),
            field("from", g.str(self.from_agent)),
            field("to", g.str(self.to_agent)),
            field("payload", glyph.from_json(self.payload)),
            field("cid", g.str(self.correlation_id) if self.correlation_id else g.null()),
            field("ts", g.str(self.timestamp) if self.timestamp else g.null()),
        ))

    @classmethod
    def from_glyph(cls, text: str) -> "AgentMessage":
        v = glyph.parse(text)
        return cls(
            type=MessageType(v.get("type").as_str()),
            from_agent=v.get("from").as_str(),
            to_agent=v.get("to").as_str(),
            payload=glyph.to_json(v.get("payload")),
            correlation_id=v.get("cid").as_str() if v.get("cid") and not v.get("cid").is_null() else None,
            timestamp=v.get("ts").as_str() if v.get("ts") and not v.get("ts").is_null() else None,
        )


class AgentBus:
    """Message bus for multi-agent communication."""

    def __init__(self):
        self.agents: dict[str, asyncio.Queue] = {}
        self.handlers: dict[str, Any] = {}

    def register(self, agent_id: str, handler):
        """Register an agent with its message handler."""
        self.agents[agent_id] = asyncio.Queue()
        self.handlers[agent_id] = handler

    async def send(self, msg: AgentMessage):
        """Send message to target agent."""
        if msg.to_agent not in self.agents:
            raise ValueError(f"Unknown agent: {msg.to_agent}")

        await self.agents[msg.to_agent].put(msg)

    async def broadcast(self, from_agent: str, payload: Any, msg_type: MessageType = MessageType.QUERY):
        """Broadcast to all other agents."""
        for agent_id in self.agents:
            if agent_id != from_agent:
                msg = AgentMessage(
                    type=msg_type,
                    from_agent=from_agent,
                    to_agent=agent_id,
                    payload=payload,
                )
                await self.send(msg)

    async def run_agent(self, agent_id: str):
        """Run agent message loop."""
        queue = self.agents[agent_id]
        handler = self.handlers[agent_id]

        while True:
            msg = await queue.get()
            try:
                response = await handler(msg)
                if response:
                    await self.send(response)
            except Exception as e:
                error_msg = AgentMessage(
                    type=MessageType.ERROR,
                    from_agent=agent_id,
                    to_agent=msg.from_agent,
                    payload={"error": str(e)},
                    correlation_id=msg.correlation_id,
                )
                await self.send(error_msg)


# === Example Agents ===

async def researcher_agent(msg: AgentMessage) -> Optional[AgentMessage]:
    """Agent that handles research tasks."""

    if msg.type == MessageType.TASK:
        query = msg.payload.get("query", "")
        # Simulate research
        results = [
            {"title": f"Result 1 for {query}", "relevance": 0.95},
            {"title": f"Result 2 for {query}", "relevance": 0.87},
        ]

        return AgentMessage(
            type=MessageType.RESULT,
            from_agent="researcher",
            to_agent=msg.from_agent,
            payload={"results": results},
            correlation_id=msg.correlation_id,
        )

    return None


async def writer_agent(msg: AgentMessage) -> Optional[AgentMessage]:
    """Agent that handles writing tasks."""

    if msg.type == MessageType.TASK:
        topic = msg.payload.get("topic", "")
        # Simulate writing
        content = f"Article about {topic}..."

        return AgentMessage(
            type=MessageType.RESULT,
            from_agent="writer",
            to_agent=msg.from_agent,
            payload={"content": content, "word_count": len(content.split())},
            correlation_id=msg.correlation_id,
        )

    return None


async def coordinator_agent(msg: AgentMessage) -> Optional[AgentMessage]:
    """Agent that coordinates other agents."""

    if msg.type == MessageType.RESULT:
        print(f"[Coordinator] Received result from {msg.from_agent}")
        print(f"  Payload: {glyph.json_to_glyph(msg.payload)}")

    return None


# === Run the system ===

async def demo_multi_agent():
    bus = AgentBus()

    # Register agents
    bus.register("researcher", researcher_agent)
    bus.register("writer", writer_agent)
    bus.register("coordinator", coordinator_agent)

    # Start agent loops
    tasks = [
        asyncio.create_task(bus.run_agent("researcher")),
        asyncio.create_task(bus.run_agent("writer")),
        asyncio.create_task(bus.run_agent("coordinator")),
    ]

    # Coordinator sends tasks
    await bus.send(AgentMessage(
        type=MessageType.TASK,
        from_agent="coordinator",
        to_agent="researcher",
        payload={"query": "GLYPH serialization"},
        correlation_id="task_1",
    ))

    await bus.send(AgentMessage(
        type=MessageType.TASK,
        from_agent="coordinator",
        to_agent="writer",
        payload={"topic": "AI agents"},
        correlation_id="task_2",
    ))

    # Let messages process
    await asyncio.sleep(0.1)

    # Cleanup
    for task in tasks:
        task.cancel()


if __name__ == "__main__":
    asyncio.run(demo_multi_agent())
```

---

## 7. LangChain Integration

**Problem:** You're using LangChain and want to use GLYPH for more efficient structured outputs.

**Solution:** Create custom output parsers and tools that use GLYPH.

```python
import glyph
from langchain.schema import BaseOutputParser, OutputParserException
from langchain.tools import BaseTool
from langchain.callbacks.manager import CallbackManagerForToolRun
from typing import Optional, Type, Any
from pydantic import BaseModel, Field


# === GLYPH Output Parser ===

class GlyphOutputParser(BaseOutputParser[dict]):
    """Parse GLYPH-formatted LLM output."""

    def parse(self, text: str) -> dict:
        """Parse GLYPH text to dictionary."""
        try:
            # Find GLYPH content (may be wrapped in markdown)
            content = text
            if "```glyph" in text:
                start = text.find("```glyph") + 8
                end = text.find("```", start)
                content = text[start:end].strip()
            elif "```" in text:
                start = text.find("```") + 3
                end = text.find("```", start)
                content = text[start:end].strip()

            parsed = glyph.parse(content)
            return glyph.to_json(parsed)

        except Exception as e:
            raise OutputParserException(f"Failed to parse GLYPH: {e}")

    def get_format_instructions(self) -> str:
        return """Return your response in GLYPH format:
- Use {key=value} for objects
- Use [item1 item2] for arrays
- Strings with spaces need quotes: "hello world"
- Booleans are t/f, null is _

Example: {name=Alice age=30 hobbies=[reading coding]}"""

    @property
    def _type(self) -> str:
        return "glyph"


class GlyphStructuredParser(BaseOutputParser[BaseModel]):
    """Parse GLYPH output into a Pydantic model."""

    pydantic_model: Type[BaseModel]

    def parse(self, text: str) -> BaseModel:
        parser = GlyphOutputParser()
        data = parser.parse(text)
        return self.pydantic_model(**data)

    def get_format_instructions(self) -> str:
        schema = self.pydantic_model.model_json_schema()
        fields = schema.get("properties", {})

        field_strs = []
        for name, info in fields.items():
            type_str = info.get("type", "any")
            field_strs.append(f"{name}:{type_str}")

        return f"""Return a GLYPH object with these fields:
{{{' '.join(field_strs)}}}

Example: {{{' '.join(f'{k}=...' for k in fields.keys())}}}"""

    @property
    def _type(self) -> str:
        return "glyph_structured"


# === GLYPH-based Tools ===

class GlyphTool(BaseTool):
    """Base class for tools that accept GLYPH input."""

    def _parse_input(self, tool_input: str) -> dict:
        """Parse GLYPH-formatted tool input."""
        try:
            parsed = glyph.parse(tool_input)
            return glyph.to_json(parsed)
        except:
            # Fall back to treating as simple string
            return {"input": tool_input}


class SearchTool(GlyphTool):
    """Search tool that accepts GLYPH input."""

    name: str = "search"
    description: str = """Search for information.
Input format: {query="your search" max_results=10}"""

    def _run(
        self,
        tool_input: str,
        run_manager: Optional[CallbackManagerForToolRun] = None
    ) -> str:
        args = self._parse_input(tool_input)
        query = args.get("query", args.get("input", ""))
        max_results = args.get("max_results", 5)

        # Your search implementation
        results = [
            {"title": f"Result {i}", "snippet": f"Content for {query}..."}
            for i in range(max_results)
        ]

        # Return as GLYPH (token-efficient)
        return glyph.json_to_glyph({"results": results})


# === Usage with LangChain ===

from langchain_anthropic import ChatAnthropic
from langchain.prompts import ChatPromptTemplate
from langchain.chains import LLMChain


def create_glyph_chain():
    """Create a chain that outputs GLYPH."""

    llm = ChatAnthropic(model="claude-sonnet-4-20250514")
    parser = GlyphOutputParser()

    prompt = ChatPromptTemplate.from_messages([
        ("system", """You are a helpful assistant that returns structured data.
{format_instructions}"""),
        ("human", "{query}")
    ])

    chain = LLMChain(
        llm=llm,
        prompt=prompt.partial(format_instructions=parser.get_format_instructions()),
        output_parser=parser,
    )

    return chain


# Structured output example
class MovieRecommendation(BaseModel):
    title: str = Field(description="Movie title")
    year: int = Field(description="Release year")
    genre: str = Field(description="Primary genre")
    reason: str = Field(description="Why this movie is recommended")


def create_structured_chain():
    """Create a chain with structured Pydantic output."""

    llm = ChatAnthropic(model="claude-sonnet-4-20250514")
    parser = GlyphStructuredParser(pydantic_model=MovieRecommendation)

    prompt = ChatPromptTemplate.from_messages([
        ("system", """Recommend movies based on user preferences.
{format_instructions}"""),
        ("human", "I like: {preferences}")
    ])

    chain = LLMChain(
        llm=llm,
        prompt=prompt.partial(format_instructions=parser.get_format_instructions()),
        output_parser=parser,
    )

    return chain


# Run example
if __name__ == "__main__":
    chain = create_glyph_chain()
    result = chain.run(query="List 3 programming languages with their main use cases")
    print(f"Result type: {type(result)}")
    print(f"Result: {result}")
```

---

## 8. Checkpoint & Resume

**Problem:** Long-running agent tasks need to be resumable after failures or restarts.

**Solution:** GLYPH checkpoints with integrity verification.

```python
import glyph
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Any
from datetime import datetime
import hashlib


@dataclass
class TaskCheckpoint:
    """Checkpoint for a resumable task."""

    task_id: str
    task_type: str
    created_at: str
    updated_at: str

    # Progress tracking
    total_steps: int
    completed_steps: int
    current_step: int

    # State
    input_data: Any
    intermediate_results: List[Any] = field(default_factory=list)
    final_result: Optional[Any] = None

    # Error handling
    last_error: Optional[str] = None
    retry_count: int = 0

    # Integrity
    state_hash: Optional[str] = None

    def to_glyph(self) -> str:
        """Serialize checkpoint to GLYPH."""
        return glyph.json_to_glyph(asdict(self))

    @classmethod
    def from_glyph(cls, text: str) -> "TaskCheckpoint":
        """Deserialize checkpoint from GLYPH."""
        data = glyph.glyph_to_json(text)
        return cls(**data)

    def compute_hash(self) -> str:
        """Compute state hash for integrity verification."""
        # Hash without the hash field itself
        temp_hash = self.state_hash
        self.state_hash = None
        canonical = glyph.json_to_glyph(asdict(self))
        self.state_hash = temp_hash
        return hashlib.sha256(canonical.encode()).hexdigest()

    def update_hash(self):
        """Update the state hash."""
        self.state_hash = self.compute_hash()

    def verify_integrity(self) -> bool:
        """Verify checkpoint integrity."""
        if not self.state_hash:
            return True
        return self.compute_hash() == self.state_hash


class CheckpointManager:
    """Manage task checkpoints."""

    def __init__(self, checkpoint_dir: str = "./checkpoints"):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, task_id: str) -> Path:
        return self.checkpoint_dir / f"{task_id}.glyph"

    def save(self, checkpoint: TaskCheckpoint):
        """Save checkpoint to disk."""
        checkpoint.updated_at = datetime.utcnow().isoformat() + "Z"
        checkpoint.update_hash()

        path = self._path(checkpoint.task_id)
        with open(path, "w") as f:
            f.write(checkpoint.to_glyph())

    def load(self, task_id: str) -> Optional[TaskCheckpoint]:
        """Load checkpoint from disk."""
        path = self._path(task_id)
        if not path.exists():
            return None

        with open(path) as f:
            checkpoint = TaskCheckpoint.from_glyph(f.read())

        if not checkpoint.verify_integrity():
            raise ValueError(f"Checkpoint integrity check failed for {task_id}")

        return checkpoint

    def delete(self, task_id: str):
        """Delete checkpoint."""
        path = self._path(task_id)
        if path.exists():
            path.unlink()

    def list_tasks(self) -> List[str]:
        """List all checkpointed tasks."""
        return [p.stem for p in self.checkpoint_dir.glob("*.glyph")]


class ResumableTask:
    """Base class for resumable tasks."""

    def __init__(self, manager: CheckpointManager):
        self.manager = manager
        self.checkpoint: Optional[TaskCheckpoint] = None

    def start(self, task_id: str, task_type: str, input_data: Any, total_steps: int):
        """Start a new task."""
        now = datetime.utcnow().isoformat() + "Z"
        self.checkpoint = TaskCheckpoint(
            task_id=task_id,
            task_type=task_type,
            created_at=now,
            updated_at=now,
            total_steps=total_steps,
            completed_steps=0,
            current_step=0,
            input_data=input_data,
        )
        self.manager.save(self.checkpoint)

    def resume(self, task_id: str) -> bool:
        """Resume an existing task. Returns True if checkpoint exists."""
        self.checkpoint = self.manager.load(task_id)
        return self.checkpoint is not None

    def step_complete(self, result: Any):
        """Mark a step as complete."""
        self.checkpoint.intermediate_results.append(result)
        self.checkpoint.completed_steps += 1
        self.checkpoint.current_step += 1
        self.checkpoint.last_error = None
        self.manager.save(self.checkpoint)

    def step_failed(self, error: str):
        """Record a step failure."""
        self.checkpoint.last_error = error
        self.checkpoint.retry_count += 1
        self.manager.save(self.checkpoint)

    def finish(self, result: Any):
        """Mark task as complete."""
        self.checkpoint.final_result = result
        self.checkpoint.completed_steps = self.checkpoint.total_steps
        self.manager.save(self.checkpoint)

    @property
    def is_complete(self) -> bool:
        return self.checkpoint.final_result is not None

    @property
    def progress(self) -> float:
        return self.checkpoint.completed_steps / self.checkpoint.total_steps


# === Example Usage ===

class DocumentProcessingTask(ResumableTask):
    """Example: Process a batch of documents."""

    def run(self, docs: List[str]):
        """Run or resume document processing."""

        task_id = f"doc_process_{hash(tuple(docs)) % 10000}"

        # Try to resume
        if self.resume(task_id):
            print(f"Resuming from step {self.checkpoint.current_step}")
            start_idx = self.checkpoint.current_step
        else:
            print("Starting new task")
            self.start(task_id, "doc_processing", docs, len(docs))
            start_idx = 0

        # Process remaining documents
        for i in range(start_idx, len(docs)):
            doc = docs[i]
            print(f"Processing document {i+1}/{len(docs)}: {doc[:30]}...")

            try:
                # Simulate processing
                result = self._process_doc(doc)
                self.step_complete(result)
                print(f"  Done ({self.progress*100:.0f}%)")

            except Exception as e:
                self.step_failed(str(e))
                print(f"  Failed: {e}")

                if self.checkpoint.retry_count > 3:
                    raise RuntimeError(f"Too many retries for doc {i}")

                raise

        # Finalize
        summary = {
            "total": len(docs),
            "processed": len(self.checkpoint.intermediate_results),
        }
        self.finish(summary)
        print(f"Task complete: {summary}")

        # Clean up checkpoint
        self.manager.delete(task_id)

        return self.checkpoint.intermediate_results

    def _process_doc(self, doc: str) -> dict:
        """Process a single document."""
        import time
        time.sleep(0.05)  # Simulate work

        # Simulate occasional failures
        import random
        if random.random() < 0.1:
            raise ValueError("Random processing error")

        return {
            "doc": doc[:50],
            "word_count": len(doc.split()),
            "processed_at": datetime.utcnow().isoformat() + "Z",
        }


# Demo
if __name__ == "__main__":
    manager = CheckpointManager("./checkpoints")
    task = DocumentProcessingTask(manager)

    docs = [
        "First document about AI and machine learning.",
        "Second document about natural language processing.",
        "Third document about computer vision techniques.",
        "Fourth document about reinforcement learning.",
        "Fifth document about transformer architectures.",
    ]

    try:
        results = task.run(docs)
        print(f"\nProcessed {len(results)} documents")
    except Exception as e:
        print(f"\nTask interrupted: {e}")
        print("Run again to resume from checkpoint")
```

---

## 9. Schema Validation

**Problem:** You need to validate complex data structures with custom constraints.

**Solution:** Use validation helpers with GLYPH parsing.

```python
import glyph
from typing import List, Optional, Dict, Any
import re


class ValidationResult:
    """Result of schema validation."""

    def __init__(self):
        self.valid = True
        self.errors: List[str] = []

    def add_error(self, path: str, message: str):
        self.valid = False
        self.errors.append(f"{path}: {message}")

    def __bool__(self):
        return self.valid

    def __str__(self):
        if self.valid:
            return "Valid"
        return f"Invalid: {'; '.join(self.errors)}"


def validate_field(value: Any, field_def: dict, path: str, result: ValidationResult):
    """Validate a single field against its definition."""

    field_type = field_def.get("type", "any")

    # Type checking
    if field_type == "str":
        if not isinstance(value, str):
            result.add_error(path, f"expected string, got {type(value).__name__}")
            return

        # String constraints
        if "min_len" in field_def and len(value) < field_def["min_len"]:
            result.add_error(path, f"length {len(value)} < min {field_def['min_len']}")
        if "max_len" in field_def and len(value) > field_def["max_len"]:
            result.add_error(path, f"length {len(value)} > max {field_def['max_len']}")
        if "pattern" in field_def and not re.match(field_def["pattern"], value):
            result.add_error(path, f"does not match pattern {field_def['pattern']}")
        if "enum" in field_def and value not in field_def["enum"]:
            result.add_error(path, f"value '{value}' not in enum {field_def['enum']}")

    elif field_type == "int":
        if not isinstance(value, int) or isinstance(value, bool):
            result.add_error(path, f"expected int, got {type(value).__name__}")
            return

        if "min" in field_def and value < field_def["min"]:
            result.add_error(path, f"value {value} < min {field_def['min']}")
        if "max" in field_def and value > field_def["max"]:
            result.add_error(path, f"value {value} > max {field_def['max']}")

    elif field_type == "float":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            result.add_error(path, f"expected float, got {type(value).__name__}")
            return

        if "min" in field_def and value < field_def["min"]:
            result.add_error(path, f"value {value} < min {field_def['min']}")
        if "max" in field_def and value > field_def["max"]:
            result.add_error(path, f"value {value} > max {field_def['max']}")

    elif field_type == "bool":
        if not isinstance(value, bool):
            result.add_error(path, f"expected bool, got {type(value).__name__}")

    elif field_type.startswith("list"):
        if not isinstance(value, list):
            result.add_error(path, f"expected list, got {type(value).__name__}")
            return

        if "max_items" in field_def and len(value) > field_def["max_items"]:
            result.add_error(path, f"list length {len(value)} > max {field_def['max_items']}")
        if "unique" in field_def and field_def["unique"] and len(value) != len(set(str(v) for v in value)):
            result.add_error(path, "list items must be unique")


def validate_struct(data: dict, schema: Dict[str, dict]) -> ValidationResult:
    """Validate data against a schema."""

    result = ValidationResult()

    # Check required fields
    for field_name, field_def in schema.items():
        field_path = field_name

        if field_def.get("required", False) and field_name not in data:
            result.add_error(field_path, "required field missing")
            continue

        if field_name not in data:
            continue

        value = data[field_name]
        validate_field(value, field_def, field_path, result)

    return result


# === Define schemas ===

USER_SCHEMA = {
    "id": {
        "type": "str",
        "required": True,
        "pattern": r"^usr_[a-z0-9]{8,}$",
    },
    "email": {
        "type": "str",
        "required": True,
        "pattern": r"^[^@]+@[^@]+\.[^@]+$",
    },
    "name": {
        "type": "str",
        "required": True,
        "min_len": 1,
        "max_len": 100,
    },
    "age": {
        "type": "int",
        "min": 0,
        "max": 150,
    },
    "role": {
        "type": "str",
        "enum": ["admin", "user", "guest"],
    },
    "tags": {
        "type": "list[str]",
        "max_items": 10,
        "unique": True,
    },
}

API_REQUEST_SCHEMA = {
    "method": {
        "type": "str",
        "required": True,
        "enum": ["GET", "POST", "PUT", "DELETE", "PATCH"],
    },
    "path": {
        "type": "str",
        "required": True,
        "pattern": r"^/[a-z0-9/_-]*$",
    },
    "timeout_ms": {
        "type": "int",
        "min": 100,
        "max": 30000,
    },
}


# === Usage ===

def demo_validation():
    # Valid user
    valid_user = {
        "id": "usr_abc12345",
        "email": "alice@example.com",
        "name": "Alice",
        "age": 30,
        "role": "admin",
        "tags": ["premium", "beta"],
    }

    result = validate_struct(valid_user, USER_SCHEMA)
    print(f"Valid user: {result}")

    # Invalid user
    invalid_user = {
        "id": "bad_id",  # Wrong pattern
        "email": "not-an-email",  # Wrong pattern
        "name": "",  # Too short
        "age": 200,  # Too high
        "role": "superadmin",  # Not in enum
        "tags": ["a", "a", "b"],  # Not unique
    }

    result = validate_struct(invalid_user, USER_SCHEMA)
    print(f"\nInvalid user: {result}")
    for error in result.errors:
        print(f"  - {error}")

    # Validate GLYPH input
    glyph_text = '{id=usr_xyz98765 email=bob@test.com name=Bob age=25 role=user}'
    parsed = glyph.glyph_to_json(glyph_text)
    result = validate_struct(parsed, USER_SCHEMA)
    print(f"\nGLYPH user: {result}")


if __name__ == "__main__":
    demo_validation()
```

---

## 10. Custom Wire Protocol

**Problem:** You need a custom protocol for agent-to-server communication with specific requirements.

**Solution:** Build structured message types with GLYPH.

```python
import glyph
from glyph import g, field
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional, Callable, Any, Dict
import asyncio


class MessageKind(IntEnum):
    """Message kinds for custom protocol."""

    # Standard kinds
    DATA = 0
    ACK = 1
    ERROR = 2
    PING = 3
    PONG = 4

    # Custom kinds
    AUTH = 100
    AUTH_OK = 101
    AUTH_FAIL = 102
    SUBSCRIBE = 110
    UNSUBSCRIBE = 111
    PUBLISH = 112
    RPC_REQUEST = 120
    RPC_RESPONSE = 121
    RPC_ERROR = 122


@dataclass
class ProtocolConfig:
    """Configuration for custom protocol."""

    version: int = 1
    heartbeat_interval: float = 30.0
    auth_timeout: float = 10.0
    max_subscriptions: int = 100


class CustomProtocol:
    """Custom protocol built on GLYPH messages."""

    def __init__(self, config: Optional[ProtocolConfig] = None):
        self.config = config or ProtocolConfig()
        self.authenticated = False
        self.subscriptions: set[str] = set()
        self.rpc_handlers: Dict[str, Callable] = {}
        self.pending_rpcs: Dict[str, asyncio.Future] = {}
        self._seq = 0
        self._rpc_id = 0
        self.outbox: list[str] = []

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _next_rpc_id(self) -> str:
        self._rpc_id += 1
        return f"rpc_{self._rpc_id}"

    def _send(self, kind: MessageKind, payload: Any):
        """Queue a message to send."""
        msg = glyph.emit(g.struct("Msg",
            field("seq", g.int(self._next_seq())),
            field("kind", g.int(kind)),
            field("payload", glyph.from_json(payload) if payload else g.null()),
        ))
        self.outbox.append(msg)

    # === Authentication ===

    def authenticate(self, credentials: dict):
        """Send authentication request."""
        self._send(MessageKind.AUTH, credentials)

    def handle_auth_response(self, kind: MessageKind) -> bool:
        """Handle authentication response."""
        if kind == MessageKind.AUTH_OK:
            self.authenticated = True
            return True
        elif kind == MessageKind.AUTH_FAIL:
            self.authenticated = False
            return False
        return False

    # === Pub/Sub ===

    def subscribe(self, topic: str):
        """Subscribe to a topic."""
        if len(self.subscriptions) >= self.config.max_subscriptions:
            raise ValueError("Max subscriptions reached")

        self._send(MessageKind.SUBSCRIBE, {"topic": topic})
        self.subscriptions.add(topic)

    def unsubscribe(self, topic: str):
        """Unsubscribe from a topic."""
        self._send(MessageKind.UNSUBSCRIBE, {"topic": topic})
        self.subscriptions.discard(topic)

    def publish(self, topic: str, message: Any):
        """Publish message to topic."""
        self._send(MessageKind.PUBLISH, {"topic": topic, "msg": message})

    # === RPC ===

    def register_rpc(self, method: str, handler: Callable):
        """Register an RPC handler."""
        self.rpc_handlers[method] = handler

    async def call_rpc(self, method: str, params: dict, timeout: float = 30.0) -> Any:
        """Call a remote procedure."""
        rpc_id = self._next_rpc_id()

        # Create future for response
        future = asyncio.get_event_loop().create_future()
        self.pending_rpcs[rpc_id] = future

        # Send request
        self._send(MessageKind.RPC_REQUEST, {
            "id": rpc_id,
            "method": method,
            "params": params,
        })

        try:
            result = await asyncio.wait_for(future, timeout)
            return result
        finally:
            self.pending_rpcs.pop(rpc_id, None)

    async def handle_rpc_request(self, payload: dict):
        """Handle incoming RPC request."""
        rpc_id = payload["id"]
        method = payload["method"]
        params = payload.get("params", {})

        try:
            if method not in self.rpc_handlers:
                raise ValueError(f"Unknown method: {method}")

            result = await self.rpc_handlers[method](params)

            self._send(MessageKind.RPC_RESPONSE, {
                "id": rpc_id,
                "result": result,
            })
        except Exception as e:
            self._send(MessageKind.RPC_ERROR, {
                "id": rpc_id,
                "error": str(e),
            })

    def handle_rpc_response(self, kind: MessageKind, payload: dict):
        """Handle RPC response."""
        rpc_id = payload["id"]

        if rpc_id in self.pending_rpcs:
            future = self.pending_rpcs[rpc_id]

            if kind == MessageKind.RPC_RESPONSE:
                future.set_result(payload.get("result"))
            elif kind == MessageKind.RPC_ERROR:
                future.set_exception(RuntimeError(payload.get("error")))

    # === Message handling ===

    async def handle_message(self, msg_text: str, on_publish: Optional[Callable] = None):
        """Handle incoming message."""

        parsed = glyph.parse(msg_text)
        kind = MessageKind(parsed.get("kind").as_int())
        payload = glyph.to_json(parsed.get("payload")) if parsed.get("payload") and not parsed.get("payload").is_null() else None

        # Authentication
        if kind in (MessageKind.AUTH_OK, MessageKind.AUTH_FAIL):
            self.handle_auth_response(kind)

        # Pub/Sub
        elif kind == MessageKind.PUBLISH and on_publish and payload:
            await on_publish(payload["topic"], payload["msg"])

        # RPC
        elif kind == MessageKind.RPC_REQUEST and payload:
            await self.handle_rpc_request(payload)
        elif kind in (MessageKind.RPC_RESPONSE, MessageKind.RPC_ERROR) and payload:
            self.handle_rpc_response(kind, payload)

        # Ping/Pong
        elif kind == MessageKind.PING:
            self._send(MessageKind.PONG, None)


# === Example Usage ===

async def demo_custom_protocol():
    """Demo the custom protocol."""

    # Create protocol instance
    proto = CustomProtocol(ProtocolConfig(
        heartbeat_interval=10.0,
    ))

    # Register RPC handlers
    async def handle_echo(params: dict) -> dict:
        return {"echo": params.get("message", "")}

    async def handle_add(params: dict) -> dict:
        a = params.get("a", 0)
        b = params.get("b", 0)
        return {"sum": a + b}

    proto.register_rpc("echo", handle_echo)
    proto.register_rpc("add", handle_add)

    # Simulate authentication
    proto.authenticate({
        "token": "secret_token_123",
        "client_id": "agent_1",
    })

    # Simulate subscription
    proto.subscribe("events.user.*")
    proto.subscribe("events.system.alerts")

    # Simulate publish
    proto.publish("events.user.created", {
        "user_id": "usr_123",
        "name": "Alice",
    })

    # Show messages
    print("=== Messages generated ===")
    for msg in proto.outbox:
        parsed = glyph.parse(msg)
        kind = MessageKind(parsed.get("kind").as_int())
        print(f"KIND={kind.name} SEQ={parsed.get('seq').as_int()}")
        payload = parsed.get("payload")
        if payload and not payload.is_null():
            print(f"  Payload: {glyph.emit(payload)[:60]}...")


if __name__ == "__main__":
    asyncio.run(demo_custom_protocol())
```

---

## Next Steps

- **[SPEC.md](./SPEC.md)** - Formal grammar specification
- **[GitHub Issues](https://github.com/Neumenon/glyph/issues)** - Report bugs, request features

---

<p align="center">
  <sub>Questions? Open an issue or discussion on GitHub.</sub>
</p>
