# GLYPH Technical Specifications

**Complete technical reference for implementers and advanced users.**

---

## Table of Contents

1. [Overview](#overview)
2. [Loose Mode Specification](#loose-mode-specification)
3. [GS1 Streaming Protocol](#gs1-streaming-protocol)
4. [Type System](#type-system)
5. [Conformance Requirements](#conformance-requirements)

---

## Overview

GLYPH has two main specifications:

**1. GLYPH-Loose** - Schema-optional JSON-compatible mode
- Drop-in JSON replacement
- Deterministic canonical form
- Cross-language parity

**2. GS1 Stream** - Frame protocol for multiplexed streaming
- Transport-agnostic (TCP, WebSocket, SSE, pipes)
- CRC-32 integrity checking
- SHA-256 state verification

---

## Loose Mode Specification

**Spec ID:** `glyph-loose-1.0.0`
**Status:** Stable (frozen)

### Design Goals

1. **Drop-in JSON replacement** - Any valid JSON is valid input
2. **Deterministic canonical form** - Same data always produces same output
3. **Cross-language parity** - Go, JS, Python produce identical output
4. **Token efficiency** - More compact than JSON

### Canonical Rules

#### Scalars

| Type | Canonical Form | Examples |
|------|----------------|----------|
| null | `_` | `_` (accepts `∅`, `null` on input) |
| bool | `t` / `f` | `t`, `f` |
| int | Decimal, no leading zeros | `0`, `42`, `-100` |
| float | Shortest roundtrip, `e` not `E` | `3.14`, `1e-06`, `1e+15` |
| string | Bare if safe, else quoted | `hello`, `"hello world"` |

#### Float Formatting

- **Zero:** Always `0` (not `-0`, not `0.0`)
- **Negative zero:** Canonicalizes to `0`
- **Exponent threshold:** Use exponential when `exp < -4` or `exp >= 15`
- **Exponent format:** 2-digit minimum (`1e-06`, not `1e-6`)
- **NaN/Infinity:** Rejected with error (not JSON-compatible)

**Examples:**
```
0.000001  →  1e-06
100000000000000  →  1e+14
0.0001  →  0.0001  (no exponent)
10000000000000000  →  1e+16  (exponent)
```

#### String Bare-Safe Rule

A string is "bare-safe" (unquoted) if:
1. Non-empty
2. First character: Unicode letter or `_`
3. Remaining characters: Unicode letter, digit, `_`, `-`, `.`, `/`
4. Not a reserved word: `t`, `f`, `true`, `false`, `null`, `none`, `nil`

Otherwise, the string is quoted with minimal escapes.

**Examples:**
```
hello       → hello
hello_world → hello_world
hello-2.0   → hello-2.0
file.txt    → file.txt
src/main.go → src/main.go

hello world → "hello world"  (space)
123abc      → "123abc"       (starts with digit)
true        → "true"         (reserved word)
```

#### Containers

| Type | Canonical Form |
|------|----------------|
| list | `[` + space-separated elements + `]` |
| map | `{` + sorted key=value pairs + `}` |

**Examples:**
```glyph
[]
[1 2 3]
[_ t 42 hello]
{}
{a=1}
{a=1 b=2 c=3}
{name=Alice age=30 active=t}
```

#### Key Ordering

Map keys are sorted by **bytewise UTF-8 comparison** of their canonical string form.

```glyph
Input:  {"b":1,"a":2,"aa":3,"A":4,"_":5}
Output: {A=4 _=5 a=2 aa=3 b=1}
```

UTF-8 byte order: `A` (0x41) < `_` (0x5F) < `a` (0x61) < ...

#### Duplicate Keys

**Last-wins policy:** When a JSON object has duplicate keys, the last value is used.

```glyph
Input:  {"k":1,"k":2,"k":3}
Output: {k=3}
```

### JSON Bridge

#### Input (JSON → GLYPH)

```go
gv, err := glyph.FromJSONLoose(jsonBytes)
```

```python
import glyph
gv = glyph.json_to_glyph(json_str)
```

```typescript
import { fromJSON } from 'glyph-js';
const gv = fromJSON(jsonData);
```

- Accepts any valid JSON
- Rejects NaN/Infinity (returns error)
- Integers within ±2^53 become `int`, others become `float`

#### Output (GLYPH → JSON)

```go
jsonBytes, err := glyph.ToJSONLoose(gv)
```

```python
json_str = glyph.glyph_to_json(glyph_text)
```

```typescript
const jsonData = toJSON(glyphValue);
```

- Produces valid JSON
- Extended types become strings (see below)

#### Extended Types

**Default mode** (simple strings):
- IDs: `"^prefix:value"`
- Times: `"2025-01-13T12:00:00Z"`
- Bytes: `"<base64>"`

**Extended mode** (typed objects):
```json
{"$glyph": "time", "value": "2025-01-13T12:00:00Z"}
{"$glyph": "id", "value": "^user:abc123"}
{"$glyph": "bytes", "base64": "SGVsbG8="}
```

### Fingerprinting

Deterministic SHA-256 hash of canonical form:

```python
import glyph

data = {"user": "alice", "count": 42}
hash = glyph.fingerprint_loose(data)
# sha256:a1b2c3d4e5f6...
```

**Properties:**
- Same data → same hash (across languages)
- Different data → different hash (collision-resistant)
- Used for state verification and patch safety

---

## GS1 Streaming Protocol

**Spec ID:** `gs1-1.0.0`
**Status:** Frozen (v1.0)

### Purpose

**GS1** is a stream framing protocol for transporting GLYPH payloads over streaming transports.

**Key features:**
- Multiplexed streams (multiple logical streams on one connection)
- Sequence tracking (monotonic seq numbers per stream)
- Integrity checking (CRC-32)
- State verification (SHA-256 base hash)

**Transports supported:**
- TCP
- WebSocket (text and binary frames)
- Server-Sent Events (SSE)
- Unix pipes
- Files

### Frame Structure (GS1-T Text Format)

```
@frame{key=value key=value ...}\n
<exactly len bytes of payload>
\n
```

**Required fields:**

| Key | Type | Description |
|-----|------|-------------|
| `v` | uint8 | Protocol version (MUST be 1) |
| `sid` | uint64 | Stream identifier |
| `seq` | uint64 | Sequence number (per-SID, monotonic) |
| `kind` | string/uint8 | Frame kind (see below) |
| `len` | uint32 | Payload length in bytes |

**Optional fields:**

| Key | Type | Description |
|-----|------|-------------|
| `crc` | string | CRC-32: `crc32:<8hex>` |
| `base` | string | SHA-256: `sha256:<64hex>` |
| `final` | bool | End-of-stream marker |
| `flags` | uint8 | Bitmask (hex) |

### Frame Kinds

| Value | Name | Meaning |
|------:|------|---------|
| 0 | `doc` | Snapshot or general GLYPH document |
| 1 | `patch` | GLYPH patch document |
| 2 | `row` | Single row (streaming tabular data) |
| 3 | `ui` | UI event (progress/log/artifact refs) |
| 4 | `ack` | Acknowledgement |
| 5 | `err` | Error event |
| 6 | `ping` | Keepalive / liveness check |
| 7 | `pong` | Ping response |

### Example Frame

```
@frame{v=1 sid=1 seq=12 kind=patch len=32 crc=89abcdef base=sha256:0123456789abcdef...}
@patch
set .foo 42
@end

```

### Integrity: CRC-32

When `crc` is present:

- **Algorithm:** CRC-32 IEEE (polynomial 0xEDB88320)
- **Input:** Payload bytes as transmitted
- **Format:** `crc=<8 lowercase hex digits>`

**Receiver MUST:**
- Verify CRC if present
- Reject frame on mismatch

**Example:**
```python
import glyph
import zlib

payload = b'{action=search query=test}'
crc = zlib.crc32(payload) & 0xffffffff
print(f"crc={crc:08x}")
```

### Patch Safety: BASE Hash

When `base` is present:

- **Algorithm:** SHA-256
- **Input:** Canonical form of state document
- **Format:** `base=sha256:<64 lowercase hex digits>`

**State hash definition:**
```
base = sha256(Canonicalize(stateDoc))
```

**Patch application rule:**
Receiver MUST NOT apply patch if `receiverStateHash != base`.

**On mismatch:**
- Request a `doc` snapshot, OR
- Emit an `err` frame, OR
- Emit an `ack` with failure payload

**Example:**
```python
import glyph

# Sender side
current_state = {"user": "alice", "count": 5}
base_hash = glyph.fingerprint_loose(current_state)

patch = {"count": 6}
frame = {
    "v": 1,
    "sid": 1,
    "seq": 10,
    "kind": "patch",
    "base": base_hash,
    "payload": glyph.emit(patch)
}

# Receiver side
if glyph.fingerprint_loose(receiver_state) == frame["base"]:
    apply_patch(frame["payload"])
else:
    request_snapshot()  # State diverged
```

### Sequence Tracking

**Per-SID monotonicity:**
- `seq` MUST be monotonically increasing by 1
- Receivers SHOULD detect gaps and handle appropriately

**Gap handling:**
- Request retransmission
- Request snapshot
- Emit error

### ACK Frames

- `kind=ack` acknowledges receipt of `(sid, seq)`
- `ack` frames typically have `len=0`
- `ack` with payload may carry error/status details

**Example:**
```
@frame{v=1 sid=1 seq=13 kind=ack len=0}

```

### FINAL Flag

- `final=true` indicates no more frames for this `sid`
- Receiver may clean up per-SID state
- Useful for multiplexed connections

---

## Type System

### Primitive Types

| Type | Description | Example |
|------|-------------|---------|
| `str` | UTF-8 string | `hello`, `"hello world"` |
| `int` | Signed 64-bit integer | `42`, `-100` |
| `float` | IEEE 754 double | `3.14`, `1e-06` |
| `bool` | Boolean | `t`, `f` |
| `null` | Null value | `_`, `∅` |
| `bytes` | Binary data (base64) | `b64"SGVsbG8="` |
| `time` | ISO-8601 timestamp | `2025-01-13T12:00:00Z` |
| `id` | Typed reference | `^user:abc123` |

### Collection Types

| Type | Description | Example |
|------|-------------|---------|
| `list` | Ordered collection | `[1 2 3]` |
| `map` | Key-value pairs | `{a=1 b=2}` |
| `struct` | Named structure | `User{name=Alice age=30}` |

### Constraints

Constraints can be specified in schemas:

**Integer constraints:**
```glyph
limit int<1,100>      # min=1, max=100
port int<1024,65535>
```

**String constraints:**
```glyph
query str<1,500>      # min_len=1, max_len=500
username str<3,20>
```

**Enum constraints:**
```glyph
units enum[celsius,fahrenheit]
status enum[pending,active,complete]
```

---

## Conformance Requirements

### Implementations MUST:

**Loose Mode:**
1. Produce identical canonical output for same input (deterministic)
2. Sort map keys by UTF-8 byte order
3. Format floats according to exponent thresholds
4. Apply bare-safe rules consistently
5. Use last-wins for duplicate keys
6. Accept any valid JSON as input
7. Produce valid JSON as output

**GS1 Streaming:**
1. Parse frame headers correctly
2. Read payload using `len`, not delimiters
3. Verify CRC-32 if present
4. Reject patches with mismatched base hash
5. Enforce seq monotonicity per SID
6. Accept unknown frame kinds

### Cross-Language Compatibility

All implementations MUST produce byte-identical canonical forms.

**Test corpus:** `testdata/loose_json/` contains 50+ test cases:
- Deep nesting (10-20 levels)
- Unicode (surrogates, CJK, emoji)
- Edge numbers (boundaries, precision)
- Key ordering (stability, unicode)
- Duplicate keys
- Reserved words
- Control characters

**Golden files:** `testdata/loose_json/golden/` anchor expected output.

### Performance Targets

**Go implementation (reference):**
- Canonicalization: 2M+ ops/sec
- Parsing: 1.5M+ ops/sec
- Fingerprinting: 500K+ ops/sec

**Other implementations:**
- Python: 50K+ ops/sec parsing
- JavaScript: 100K+ ops/sec parsing

---

## Related Documentation

**Implementation Guides:**
- [Quickstart](QUICKSTART.md) - Get started in 5 minutes
- [Complete Guide](GUIDE.md) - Features and patterns
- [Agent Patterns](AGENTS.md) - AI integration

**Research:**
- [LLM Accuracy Report](reports/LLM_ACCURACY_REPORT.md)
- [Performance Benchmarks](reports/CODEC_BENCHMARK_REPORT.md)

---

**Questions?** Open an [issue](https://github.com/Neumenon/glyph/issues) or check [discussions](https://github.com/Neumenon/glyph/discussions).
