# GLYPH-Loose Mode Specification

**Spec ID:** `glyph-loose-1.0.0`
**Date:** 2026-01-13
**Status:** Stable

Canonical output for any input in the test corpus is frozen and will not change.

GLYPH-Loose is the schema-optional subset of GLYPH. It provides a deterministic canonical representation for JSON-like data, suitable for hashing, caching, and cross-language interoperability.

## Design Goals

1. **Drop-in JSON replacement** - Any valid JSON is valid GLYPH-Loose input
2. **Deterministic canonical form** - Same data always produces same output
3. **Cross-language parity** - Go, JS, and Python implementations produce identical output
4. **Compact** - More token-efficient than JSON for LLM contexts

---

## Canonical Rules

### Scalars

| Type | Canonical Form | Examples |
|------|----------------|----------|
| null | `_` | `_` (accepts `∅`, `null` on input) |
| bool | `t` / `f` | `t`, `f` |
| int | Decimal, no leading zeros | `0`, `42`, `-100` |
| float | Shortest roundtrip, `e` (not `E`) | `3.14`, `1e-06`, `1e+15` |
| string | Bare if safe, else quoted | `hello`, `"hello world"` |

### Float Formatting

- **Zero:** Always `0` (not `-0`, not `0.0`)
- **Negative zero:** Canonicalizes to `0`
- **Exponent threshold:** Use exponential when `exp < -4` or `exp >= 15`
- **Exponent format:** 2-digit minimum (`1e-06`, not `1e-6`)
- **NaN/Infinity:** Rejected with error (not JSON-compatible)

### String Bare-Safe Rule

A string is "bare-safe" (unquoted) if:
1. Non-empty
2. First character: Unicode letter or `_`
3. Remaining characters: Unicode letter, digit, `_`, `-`, `.`, `/`
4. Not a reserved word: `t`, `f`, `true`, `false`, `null`, `none`, `nil`

Otherwise, the string is quoted with minimal escapes.

### Containers

| Type | Canonical Form |
|------|----------------|
| list | `[` + space-separated elements + `]` |
| map | `{` + sorted key=value pairs + `}` |

**Examples:**
```
[]
[1 2 3]
[_ t 42 hello]
{}
{a=1}
{a=1 b=2 c=3}
```

### Key Ordering

Map keys are sorted by **bytewise UTF-8 comparison** of their canonical string form.

```
Input:  {"b":1,"a":2,"aa":3,"A":4,"_":5}
Output: {A=4 _=5 a=2 aa=3 b=1}
```

UTF-8 byte order: `A` (0x41) < `_` (0x5F) < `a` (0x61) < ...

### Duplicate Keys

**Last-wins policy:** When a JSON object has duplicate keys, the last value is used.

```
Input:  {"k":1,"k":2,"k":3}
Output: {k=3}
```

---

## JSON Bridge

### Input (JSON → GLYPH)

```go
gv, err := glyph.FromJSONLoose(jsonBytes)
```

- Accepts any valid JSON
- Rejects NaN/Infinity (returns error)
- Integers within ±2^53 become `int`, others become `float`

### Output (GLYPH → JSON)

```go
jsonBytes, err := glyph.ToJSONLoose(gv)
```

- Produces valid JSON
- IDs become `"^prefix:value"` strings
- Times become ISO-8601 strings
- Bytes become base64 strings

### Extended Mode

With `BridgeOpts{Extended: true}`:
- Times become `{"$glyph":"time","value":"..."}`
- IDs become `{"$glyph":"id","value":"^..."}`
- Bytes become `{"$glyph":"bytes","base64":"..."}`

---

## CLI Usage

```bash
# Format JSON as canonical GLYPH-Loose
echo '{"b":1,"a":2}' | glyph fmt-loose
# Output: {a=2 b=1}

# Convert to pretty JSON
echo '{"b":1,"a":2}' | glyph to-json
# Output:
# {
#   "a": 2,
#   "b": 1
# }

# File input
glyph fmt-loose data.json

# LLM mode (ASCII-safe nulls)
echo '{"value":null}' | glyph fmt-loose --llm
# Output: {value=_}

# Compact mode with schema header
echo '{"action":"search","query":"test"}' | glyph fmt-loose --compact
# Output:
# @schema#<hash> keys=[action query]
# {#0=search #1=test}
```

---

## Conformance Testing

The test corpus at `testdata/loose_json/` contains 50 cases covering:
- Deep nesting (10-20 levels)
- Unicode (surrogates, CJK, emoji)
- Edge numbers (boundaries, precision)
- Key ordering (stability, unicode)
- Duplicate keys
- Reserved words
- Control characters

Golden files at `testdata/loose_json/golden/` anchor expected canonical output.

Cross-implementation tests verify Go, JS, and Python produce byte-identical canonical forms.

---

## Schema Extensions

GLYPH-Loose is the foundation. Schema features enable additional capabilities.

### @open Structs

The `@open` annotation allows a struct type to accept fields not defined in the schema. This is useful for forward compatibility and dynamic payloads (e.g., Kubernetes-style metadata).

**Schema Definition (Go):**
```go
schema := NewSchemaBuilder().
    AddOpenStruct("Config", "v1",
        Field("name", PrimitiveType("str")),
        Field("port", PrimitiveType("int")),
    ).
    Build()
```

**Schema Definition (TypeScript):**
```typescript
const schema = new SchemaBuilder()
    .addOpenStruct('Config', 'v1')
    .field('name', t.str())
    .field('port', t.int())
    .build();
```

**Canonical Schema Text:**
```
Config:v1 @open struct{
    name: str
    port: int
}
```

**Behavior:**

| Struct Type | Unknown Field | Validation Result |
|-------------|---------------|-------------------|
| Closed (default) | Present | Error: `unknown_field` |
| `@open` | Present | Pass (warning logged) |
| `@open` + Strict | Present | Error: `unknown_field` |

**Strict Validation:**

Use `ValidateStrict()` to reject unknown fields even for `@open` structs:

```go
// Normal validation - unknown fields accepted with warning
result := ValidateWithSchema(value, schema)

// Strict validation - unknown fields always rejected
result := ValidateStrict(value, schema)
```

### map<K,V> Validation

Map values are validated against the specified value type:

```go
schema := NewSchemaBuilder().
    AddStruct("Config", "v1",
        Field("settings", MapType(PrimitiveType("str"), PrimitiveType("int"))),
    ).
    Build()

// This passes - all values are ints
value := Map(
    MapEntry{Key: "timeout", Value: Int(30)},
    MapEntry{Key: "port", Value: Int(8080)},
)

// This fails - "name" value is string, not int
value := Map(
    MapEntry{Key: "timeout", Value: Int(30)},
    MapEntry{Key: "name", Value: Str("myapp")}, // type_mismatch error
)
```

Nested type references are also validated:

```go
schema := NewSchemaBuilder().
    AddStruct("Address", "v1",
        Field("host", PrimitiveType("str")),
        Field("port", PrimitiveType("int")),
    ).
    AddStruct("Registry", "v1",
        Field("services", MapType(PrimitiveType("str"), RefType("Address"))),
    ).
    Build()
```

---

## Auto-Tabular Mode

Auto-Tabular mode provides compact representation for homogeneous lists of objects (arrays of records). This is common in API responses and database results.

### Syntax

```
@tab _ [col1 col2 col3]
|val1|val2|val3|
|val4|val5|val6|
@end
```

- **Header:** `@tab _` followed by sorted column names in brackets
- **Rows:** Pipe-delimited cells, one row per line
- **Footer:** `@end` marker

### Enabling Auto-Tabular

Auto-tabular is **enabled by default** in canonicalization outputs. Disable it if you need v2.2.x-compatible output or strict non-tabular formatting.

**Go:**
```go
// Default: auto-tabular enabled
canonical := glyph.CanonicalizeLoose(value)

// Disable auto-tabular
canonical := glyph.CanonicalizeLooseNoTabular(value)

// Custom options
opts := glyph.DefaultLooseCanonOpts()
opts.AutoTabular = false
opts.MinRows = 5  // Only tabularize 5+ rows (if enabled)
canonical := glyph.CanonicalizeLooseWithOpts(value, opts)
```

**TypeScript:**
```typescript
// Default: auto-tabular enabled
const canonical = canonicalizeLoose(value);

// Disable auto-tabular
const canonical = canonicalizeLooseWithOpts(value, { autoTabular: false });

// Custom options
const canonical = canonicalizeLooseWithOpts(value, {
  autoTabular: true,
  minRows: 5
});
```

**CLI:**
```bash
echo '[{"id":1,"name":"a"},{"id":2,"name":"b"},{"id":3,"name":"c"}]' | glyph fmt-loose
# Output:
# @tab _ [id name]
# |1|a|
# |2|b|
# |3|c|
# @end

# Disable tabular
echo '[{"id":1},{"id":2},{"id":3}]' | glyph fmt-loose --no-tabular
```

### Eligibility Criteria

A list qualifies for tabular emission when:

1. Contains ≥ `MinRows` elements (default: 3)
2. All elements are maps or structs
3. No row is an empty object (must have at least one key)
4. Total column count ≤ `MaxCols` (default: 20)
5. When `AllowMissing=false`, all rows must have identical keys
6. When `AllowMissing=true`, the shared key ratio must be ≥ 50%:
   `|intersection(keys)| / |union(keys)| >= 0.5`

### Column Ordering

Columns are sorted by bytewise UTF-8 comparison of their canonical key form (same as map key ordering).

### Missing Values

When a row is missing a key present in other rows, the cell contains `_`:

```
Input:  [{"id":1,"name":"a"},{"id":2},{"id":3,"name":"c"}]
Output:
@tab _ [id name]
|1|a|
|2|_|
|3|c|
@end
```

### Escaping

Pipes in cell values are escaped as `\|`:

```
Input:  [{"val":"a|b"},{"val":"c|d"},{"val":"e|f"}]
Output:
@tab _ [val]
|"a\|b"|
|"c\|d"|
|"e\|f"|
@end
```

### Nested Values

Nested maps and lists are emitted inline:

```
Input:  [{"id":1,"meta":{"x":10}},{"id":2,"meta":{"x":20}},{"id":3,"meta":{"x":30}}]
Output:
@tab _ [id meta]
|1|{x=10}|
|2|{x=20}|
|3|{x=30}|
@end
```

### Parsing

**Go:**
```go
result, err := glyph.ParseTabularLoose(input)
// result is []map[string]any
```

**TypeScript:**
```typescript
const result = parseTabularLoose(input);
// result.columns: string[]
// result.rows: Array<Record<string, unknown>>
```

### Tabular Resync Metadata

Row/column counts can be added to tabular headers for streaming resync:

```
@tab _ rows=120 cols=6 [id name score status created updated]
|1|Alice|0.95|active|2025-01-01|2025-06-15|
...
@end
```

This enables:
- Detection of truncated streams
- Verification of complete transmission
- Progress tracking for large tables

### Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `AutoTabular` | bool | true | Enable auto-tabular detection |
| `MinRows` | int | 3 | Minimum rows to trigger tabular |
| `MaxCols` | int | 20 | Maximum columns allowed |
| `AllowMissing` | bool | true | Allow rows with missing keys |
| `NullStyle` | NullStyle | underscore | `symbol` for ∅, `underscore` for _ |
| `SchemaRef` | string | "" | Schema hash/id for @schema header |
| `KeyDict` | []string | nil | Key dictionary for compact keys |
| `UseCompactKeys` | bool | false | Emit #N instead of field names |

### Byte Savings

Auto-tabular reduces output size by eliminating repeated key names:

| Format | Size (55 test cases) |
|--------|---------------------|
| JSON-min | 5,109 bytes |
| GLYPH-Loose | 4,224 bytes |
| GLYPH-Tabular | ~3,900 bytes |

Tabular mode is most effective for wide, shallow tables.

---

## Schema Context & Key Dictionaries

For repeated structured outputs (tool calls, agent traces, topK results),
schema contexts enable significant token savings through compact key encoding.

### Schema Directive Format

**Inline schema (self-contained):**
```
@schema#abc123 @keys=[action query confidence sources]
{#0=search #1="weather NYC" #2=0.95 #3=[web news]}
```

**External schema ref (compact, receiver must have schema cached):**
```
@schema#abc123
{#0=search #1="weather NYC" #2=0.95 #3=[web news]}
```

**Clear active schema:**
```
@schema.clear
{action=search query="weather NYC"}
```

### Schema ID Format

- **Hash-based (default):** SHA-256 of keys, first 5 bytes as base32 (8 chars)
- **Session-based:** Short IDs like `S1`, `S2` for compact streaming

### Key Rules

1. Keys are positional indices in the `@keys=[...]` list
2. Objects may mix numeric keys (`#N=`) and string keys
3. Known keys → numeric; unknown/rare keys → string literal
4. Nested objects/lists also resolve numeric keys via the active schema

### Go Usage

```go
// Create schema context
schema := glyph.NewSchemaContext([]string{"role", "content", "tool_calls"})

// Emit with schema
opts := glyph.SchemaLooseCanonOpts(schema)
output := glyph.CanonicalizeLooseWithSchema(value, opts)
// Output: @schema#jka43dvv @keys=[role content tool_calls]
//         {#0=user #1="Hello"}

// Parse with registry
registry := glyph.NewSchemaRegistry()
parsed, ctx, err := glyph.ParseLoosePayload(input, registry)
```

### Python Usage

```python
from glyph import new_schema_context, SchemaRegistry, parse_loose_payload

# Create schema context
schema = new_schema_context(["role", "content", "tool_calls"])

# Parse with registry
registry = SchemaRegistry()
parsed, ctx = parse_loose_payload(input_str, registry)
role = parsed.get("role").as_str()  # "user"
```

### TypeScript Usage

```typescript
const keyDict = buildKeyDictFromValue(sampleValue);
const opts: LooseCanonOpts = {
    schemaRef: 'abc123',
    keyDict,
    useCompactKeys: true,
};
const output = canonicalizeLooseWithSchema(value, opts);
```

---

## Patch Base Fingerprint

Optional base state fingerprinting for patch validation:

```
@patch @schema#abc123 @keys=wire @target=m:123 @base=1a2b3c4d5e6f7890
= score 5
+ events "Goal!"
@end
```

The `@base=` attribute contains the first 16 characters of the SHA-256 hash
of the canonical loose form of the base state. This enables:

- **Optimistic concurrency**: Reject patches applied to stale state
- **Streaming validation**: Verify state consistency without full doc transfer
- **Debugging**: Trace state divergence in distributed systems

**Go:**
```go
// Create patch with base fingerprint from state
patch := glyph.NewPatchBuilder(target).
    WithBaseValue(baseState).  // Computes SHA-256, uses first 16 hex chars
    Set("score", glyph.Int(5)).
    Build()

// Or with explicit fingerprint
patch := glyph.NewPatchBuilder(target).
    WithBaseFingerprint("1a2b3c4d5e6f7890").
    Set("score", glyph.Int(5)).
    Build()
```

**TypeScript:**
```typescript
const patch = new PatchBuilder(target)
    .withBaseValue(baseState)
    .set('score', g.int(5))
    .build();
```

**Python:**
```python
patch = PatchBuilder(target) \
    .with_base_value(base_state) \
    .set("score", Int(5)) \
    .build()
```

---

## Upgrade Path

GLYPH-Loose is the foundation. When you need schema features:

1. **Add a schema** → enables packed encoding, FID-based parsing
2. **Use `@open` structs** → collect unknown fields safely
3. **Use `map<K,V>`** → validate dynamic keys
4. **Use patches** → efficient incremental updates

The canonical form remains stable across modes.
