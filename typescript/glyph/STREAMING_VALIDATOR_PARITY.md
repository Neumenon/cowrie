# GLYPH Streaming Validator - Cross-Language Feature Parity

This document tracks feature parity between the Go, Python, and JavaScript implementations of the GLYPH Streaming Validator.

## Overview

The streaming validator enables incremental validation of GLYPH tool calls as tokens arrive from an LLM, providing:
- **Early tool detection**: Know the tool name before full response
- **Early rejection**: Stop on unknown tools mid-stream
- **Incremental validation**: Check constraints as tokens arrive
- **Latency savings**: Reject bad payloads without waiting for completion

## Feature Parity Matrix

| Feature | Go | Python | JS | Notes |
|---------|:--:|:------:|:--:|-------|
| **ToolRegistry** | | | | |
| Thread-safe (mutex) | ✓ | - | - | Not needed in JS/Python (single-threaded) |
| register/add_tool | ✓ | ✓ | ✓ | |
| isAllowed | ✓ | ✓ | ✓ | |
| get | ✓ | ✓ | ✓ | |
| **ArgSchema** | | | | |
| type | ✓ | ✓ | ✓ | Python uses enum, JS/Go use string |
| required | ✓ | ✓ | ✓ | |
| min/max | ✓ | ✓ | ✓ | |
| minLen/maxLen | ✓ | ✓ | ✓ | |
| pattern (regex) | ✓ | ✓ | ✓ | |
| enum | ✓ | ✓ | ✓ | |
| default | - | ✓ | - | Python-only |
| **Validator State** | | | | |
| Waiting | ✓ | ✓ | ✓ | |
| InObject | ✓ | ✓ | ✓ | |
| Complete | ✓ | ✓ | ✓ | |
| Error | ✓ | ✓ | ✓ | |
| **Value Parsing** | | | | |
| Boolean (t/true/f/false) | ✓ | ✓ | ✓ | |
| Null (_/null) | ✓ | ✓ | ✓ | |
| Null (∅ Unicode) | - | ✓ | ✓ | |
| Integer | ✓ | ✓ | ✓ | |
| Float | ✓ | ✓ | ✓ | |
| Scientific notation (1e5) | ✓ | ✓ | ✓ | |
| **Constraint Validation** | | | | |
| min/max | ✓ | ✓ | ✓ | |
| minLen/maxLen | ✓ | ✓ | ✓ | |
| pattern | ✓ | ✓ | ✓ | |
| enum | ✓ | ✓ | ✓ | |
| Type checking | - | ✓ | - | Python validates types strictly |
| **Error Codes** | | | | |
| UNKNOWN_TOOL | ✓ | ✓ | ✓ | |
| MISSING_REQUIRED | ✓ | ✓ | ✓ | |
| MISSING_TOOL | ✓ | ✓ | ✓ | |
| CONSTRAINT_MIN | ✓ | ✓ | ✓ | |
| CONSTRAINT_MAX | ✓ | ✓ | ✓ | |
| CONSTRAINT_LEN | ✓ | ✓ | ✓ | |
| CONSTRAINT_PATTERN | ✓ | ✓ | ✓ | |
| CONSTRAINT_ENUM | ✓ | ✓ | ✓ | |
| INVALID_TYPE | ✓ | - | ✓ | |
| LIMIT_EXCEEDED | ✓ | - | ✓ | |
| **DoS Protection** | | | | |
| maxBufferSize | ✓ | - | ✓ | 1MB default |
| maxFieldCount | ✓ | - | ✓ | 1000 default |
| maxErrorCount | ✓ | - | ✓ | 100 default |
| withLimits() | ✓ | - | ✓ | |
| **Timing Instrumentation** | | | | |
| tokenCount | ✓ | ✓ | ✓ | |
| charCount | ✓ | ✓ | ✓ | |
| toolDetectedAtToken | ✓ | ✓ | ✓ | |
| toolDetectedAtChar | ✓ | ✓ | ✓ | |
| toolDetectedAtTime | ✓ | ✓ | ✓ | |
| firstErrorAtToken | ✓ | ✓ | ✓ | |
| firstErrorAtTime | ✓ | ✓ | ✓ | |
| completeAtToken | ✓ | ✓ | ✓ | |
| completeAtTime | ✓ | ✓ | ✓ | |
| **Timeline Events** | ✓ | ✓ | ✓ | TOOL_DETECTED, ERROR, COMPLETE |
| **Methods** | | | | |
| pushToken | ✓ | ✓ | ✓ | |
| reset | ✓ | ✓ | ✓ | |
| start | ✓ | ✓ | ✓ | |
| getResult | ✓ | ✓ | ✓ | |
| shouldStop | ✓ | - | ✓ | Python uses should_cancel property |
| isToolAllowed | ✓ | - | ✓ | Python uses tool_allowed property |
| getParsed | - | ✓ | ✓ | Returns fields if complete & valid |
| **Result Object** | | | | |
| state (enum) | - | ✓ | ✓ | |
| should_cancel property | - | ✓ | - | Python-only |
| **Syntax Support** | | | | |
| action=tool inside braces | ✓ | ✓ | ✓ | `{action=search query=...}` |
| tool name before brace | - | ✓ | ✓ | `search{query=...}` |
| **Default Tools** | | | | |
| search | ✓ | - | ✓ | Python has no default registry |
| calculate | ✓ | - | ✓ | |
| browse | ✓ | - | ✓ | |
| execute | ✓ | - | ✓ | |
| read_file | ✓ | - | ✓ | |
| write_file | ✓ | - | ✓ | |

## Language-Specific Notes

### Go
- Uses sync.RWMutex for thread-safe registry access
- Pointer helpers (MinFloat64, MaxFloat64, MinInt, MaxInt) for optional constraint values
- Duration type for time measurements

### Python
- Uses dataclasses for structured data
- ArgType enum for type validation
- Validates types strictly (e.g., int must be int, not bool)
- Supports `default` field in ArgSchema
- `should_cancel` and `tool_allowed` are properties on result object
- Captures tool name before opening brace (e.g., `search{...}`)

### JavaScript/TypeScript
- Full TypeScript type definitions
- Uses Map for tool storage
- Millisecond timestamps (Date.now())
- Supports both `{action=tool ...}` and `tool{...}` syntax
- DoS protection with configurable limits
- `getParsed()` convenience method

## Usage Examples

### JavaScript
```typescript
import { StreamingValidator, ToolRegistry, defaultToolRegistry } from 'glyph-codec';

// Use default tools
const registry = defaultToolRegistry();

// Or create custom registry
const customRegistry = new ToolRegistry();
customRegistry.register({
  name: 'my_tool',
  description: 'My custom tool',
  args: {
    query: { type: 'string', required: true, minLen: 1 },
    limit: { type: 'int', min: 1, max: 100 },
  },
});

// Create validator with optional limits
const validator = new StreamingValidator(registry, {
  maxBufferSize: 1024 * 1024,  // 1MB
  maxFieldCount: 100,
  maxErrorCount: 10,
});

// Process streaming tokens
for await (const token of llmStream) {
  const result = validator.pushToken(token);

  // Early rejection
  if (validator.shouldStop()) {
    await llmStream.cancel();
    console.error('Invalid tool:', result.errors);
    break;
  }

  // Complete and valid
  if (result.complete && result.valid) {
    const fields = validator.getParsed();
    console.log('Tool:', result.toolName, 'Args:', fields);
  }
}
```

### Go
```go
registry := glyph.DefaultToolRegistry()
validator := glyph.NewStreamingValidator(registry).
    WithLimits(1<<20, 100, 10)

for token := range llmStream {
    result := validator.PushToken(token)

    if validator.ShouldStop() {
        llmStream.Cancel()
        break
    }

    if result.Complete && result.Valid {
        fmt.Printf("Tool: %s, Args: %v\n", result.ToolName, result.Fields)
    }
}
```

### Python
```python
registry = ToolRegistry()
registry.add_tool("search", {
    "query": {"type": "str", "required": True},
    "max_results": {"type": "int", "min": 1, "max": 100},
})

validator = StreamingValidator(registry)

for token in llm_stream:
    result = validator.push_token(token)

    if result.should_cancel:
        await llm.cancel()
        break

    if result.complete and result.valid:
        print(f"Tool: {result.tool_name}, Args: {result.fields}")
```

## Test Coverage

All implementations have comprehensive test coverage including:
- Basic parsing (strings, integers, floats, booleans, null)
- Tool detection and validation
- Constraint validation (min, max, length, pattern, enum)
- Required field checking
- Buffer limits and DoS protection (Go, JS)
- Timing instrumentation
- Timeline events
- Reset and reuse
- Edge cases (nested structures, whitespace, streaming tokens)
- Feature parity tests (scientific notation, Unicode null, pre-brace syntax)

## Version History

- **v0.3.0** (JS): Full feature parity with Go implementation
  - Added DoS protection (buffer/field/error limits)
  - Added `toolDetectedAtChar` timing field
  - Added `state` to ValidationResult
  - Added scientific notation parsing
  - Added Unicode null (∅) support
  - Added `getParsed()` method
  - Added tool name before brace syntax support
  - Added comprehensive test suite (59 tests)
