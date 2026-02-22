# GLYPH Blob References & Value Pools

**Spec ID:** `glyph-blob-pool-1.0.0`
**Date:** 2026-01-13
**Status:** Stable

## Overview

This extension adds two built-in primitives for bandwidth-efficient streaming:

1. **Blob References** - Content-addressed external artifacts
2. **Value Pools** - Cross-message automatic deduplication

Both are **in-format** mechanisms with defined fallback semantics, not external plugins.

---

## 1. Blob References

### Motivation

LLM interactions often involve large artifacts (images, documents, code files) that:
- Are expensive to inline (base64 bloat)
- May not need to be fetched for every consumer
- Benefit from content-addressing (dedup, caching, integrity)

### Syntax

```
@blob cid=<hash> mime=<type> bytes=<n> [name=<filename>] [caption=<text>] [preview=<text>]
```

**Fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `cid` | Yes | Content ID: `sha256:<hex>` or `blake3:<hex>` |
| `mime` | Yes | MIME type: `image/png`, `text/plain`, etc. |
| `bytes` | Yes | Size in bytes |
| `name` | No | Original filename |
| `caption` | No | Short description (≤100 chars, for LLM context) |
| `preview` | No | Tiny inline preview (≤500 chars) |

### Examples

```
# Image with caption
@blob cid=sha256:a1b2c3d4... mime=image/png bytes=45230 name=chart.png caption="Q4 revenue by region"

# Document with preview
@blob cid=sha256:e5f6g7h8... mime=text/markdown bytes=12400 name=README.md preview="# Project\n\nThis is..."

# Code file
@blob cid=blake3:9a8b7c6d... mime=text/x-python bytes=8900 name=model.py caption="Training script v2"
```

### Inline Value Syntax

Blobs can appear as values in structs/maps:

```
Message{
  role=assistant
  content="Here's the analysis"
  attachments=[
    @blob cid=sha256:abc... mime=image/png bytes=1024 caption="Chart 1"
    @blob cid=sha256:def... mime=image/png bytes=2048 caption="Chart 2"
  ]
}
```

### Fallback Rules

**MUST:** Decoders MUST be able to process streams with unresolved blobs:

1. **Metadata-only mode**: Treat blob as a value with `{cid, mime, bytes, caption}` fields
2. **Caption/preview**: If present, LLMs can operate without fetching
3. **Fetch-on-demand**: Resolver can be called lazily when content needed

**SHOULD:** Encoders SHOULD include `caption` for blobs likely to be processed by LLMs.

**MAY:** For legacy receivers, encoder MAY inline small blobs as base64:

```
# Legacy inline form (discouraged, for compatibility only)
@blob.inline mime=image/png bytes=1024 data=b64"iVBORw0KGgo..."
```

### CID Computation

```
cid = algorithm ":" hex(hash(content))

algorithm = "sha256" | "blake3"
```

Default algorithm: `sha256`

### Blob Registry Interface

```go
type BlobRegistry interface {
    // Store content, returns CID
    Put(content []byte, mime string) (cid string, err error)
    
    // Fetch content by CID
    Get(cid string) (content []byte, mime string, err error)
    
    // Check if blob exists
    Has(cid string) bool
    
    // Metadata only (no content fetch)
    Meta(cid string) (mime string, bytes int64, err error)
}
```

### Policy: Inline vs Blob

| Size | Policy |
|------|--------|
| ≤ 4KB | MAY inline as base64 |
| 4KB - 1MB | SHOULD use blob ref |
| > 1MB | MUST use blob ref |

---

## 2. Value Pools

### Motivation

LLM conversations have massive repetition:
- System prompts repeated every turn
- Tool names (`tool:search`, `tool:calculate`)
- Status strings (`pending`, `completed`, `error`)
- Recurring structured payloads

Interning these into a pool and using references saves significant bandwidth.

### String Pool

#### Definition

```
@pool.str id=<pool-id> [<entry0> <entry1> <entry2> ...]
```

**Examples:**

```
@pool.str id=S1 [
  "You are a helpful assistant that..."
  "tool:web_search"
  "tool:calculator"
  pending
  completed
  error
]
```

#### Reference Syntax

```
^<pool-id>:<index>
```

**Examples:**

```
# Reference to S1[0] (system prompt)
{role=system content=^S1:0}

# Reference to S1[1] (tool name)  
{type=function name=^S1:1}

# Mixed with literal values
{status=^S1:4 message="Task completed"}
```

### Object Pool

For repeated structured values:

```
@pool.obj id=O1 [
  ErrorResponse{code=400 message="Bad Request"}
  ErrorResponse{code=401 message="Unauthorized"}
  ErrorResponse{code=500 message="Internal Server Error"}
]
```

```
# Reference
{error=^O1:2}
```

### Pool Scope and Lifetime

Pools are valid from definition until:
1. End of stream/session
2. Explicit invalidation: `@pool.clear id=S1`
3. Redefinition with same ID (replaces)

### Automatic Interning

Encoders MAY automatically intern values:

```go
type AutoInternOpts struct {
    MinLength    int  // Minimum string length to consider (default: 50)
    MinOccurs    int  // Minimum occurrences to intern (default: 2)
    MaxPoolSize  int  // Maximum entries per pool (default: 256)
}
```

**Heuristics:**
1. Strings ≥ MinLength appearing ≥ MinOccurs times → intern
2. LRU eviction when pool exceeds MaxPoolSize
3. Tool names, role strings → always intern

### Fallback Rules

**MUST:** If a pool reference cannot be resolved:
1. Decoder MAY request pool resync (interactive mode)
2. Decoder MAY treat as error (strict mode)
3. Encoder MAY inline the value instead (safe fallback)

**SHOULD:** Encoders SHOULD periodically emit pool sync shards:

```
# Resync pool S1 (re-emit for late joiners or after eviction)
@pool.sync id=S1 entries=[0 1 2] [
  "You are a helpful..."
  "tool:web_search"
  "tool:calculator"
]
```

### Wire Format Efficiency

| Content | JSON | GLYPH | GLYPH+Pool |
|---------|------|-------|------------|
| System prompt (500 chars) × 10 turns | 5000 | 5000 | 500 + 9×6 = 554 |
| Tool name "tool:web_search" × 20 | 300 | 300 | 15 + 19×5 = 110 |

---

## 3. Reference Syntax Summary

GLYPH now has three reference types:

| Syntax | Type | Scope | Example |
|--------|------|-------|---------|
| `^prefix:value` | Entity ID | Semantic (domain) | `^m:ARS-LIV` |
| `^pool:index` | Pool ref | Session | `^S1:0` |
| `@blob cid=...` | Blob ref | Content-addressed | `@blob cid=sha256:abc...` |

### Disambiguation

- Pool IDs are uppercase letter + digit: `S1`, `O1`, `P42`
- Entity prefixes are lowercase: `m:`, `t:`, `u:`
- Blobs use `@blob` directive, not `^` syntax

---

## 4. Frame Integration (GS1)

### New Frame Types

| ID | Name | Description |
|----|------|-------------|
| 5 | `pool` | Pool definition/sync |
| 6 | `blob_meta` | Blob metadata (no content) |
| 7 | `blob_data` | Blob content chunk |

### Blob Streaming

Large blobs can be streamed in chunks:

```
Frame 6: blob_meta cid=sha256:abc... mime=image/png bytes=1000000
Frame 7: blob_data cid=sha256:abc... offset=0 chunk=<first 64KB>
Frame 7: blob_data cid=sha256:abc... offset=65536 chunk=<next 64KB>
...
```

---

## 5. Implementation Checklist

### Core

- [ ] `@blob` parsing and emission
- [ ] CID computation (sha256)
- [ ] Local blob registry (in-memory map)
- [ ] Caption/preview fields
- [ ] Decoder: metadata-only mode for unresolved blobs

### String Pools

- [ ] `@pool.str` parsing and emission
- [ ] `^pool:index` reference resolution
- [ ] Encoder: manual pool definition
- [ ] Fallback: inline on missing ref

### Advanced

- [ ] Auto-interning heuristics
- [ ] Pool resync shards
- [ ] Object pools (`@pool.obj`)
- [ ] LRU eviction policies

---

## 6. Examples

### Complete Conversation with Pools and Blobs

```
# Session start - define pools
@pool.str id=S1 [
  "You are a helpful AI assistant. You have access to tools..."
  "tool:web_search"
  "tool:code_execute"
  user
  assistant
  system
]

# Turn 1
Message{
  role=^S1:5
  content=^S1:0
}

Message{
  role=^S1:3
  content="Analyze this chart and summarize the trends"
  attachments=[
    @blob cid=sha256:a1b2... mime=image/png bytes=45000 caption="Q4 Sales Chart"
  ]
}

Message{
  role=^S1:4
  content="Based on the chart, Q4 sales show..."
  tool_calls=[
    {type=function name=^S1:1 args={query="Q4 sales analysis"}}
  ]
}
```

### Bandwidth Comparison

| Format | Size |
|--------|------|
| JSON (inline image) | 62,000 bytes |
| JSON (no image) | 1,200 bytes |
| GLYPH + blob ref | 800 bytes |
| GLYPH + pools + blob | 450 bytes |

---

## 7. Conformance

### MUST

1. Decoders MUST handle streams with unresolved blob refs (metadata-only)
2. Decoders MUST handle unknown pool refs gracefully (error or request resync)
3. Encoders MUST include `cid`, `mime`, `bytes` in blob refs

### SHOULD

1. Encoders SHOULD include `caption` for LLM-facing blobs
2. Encoders SHOULD use pools for strings appearing ≥2 times
3. Encoders SHOULD resync pools periodically in long streams

### MAY

1. Encoders MAY auto-intern based on heuristics
2. Decoders MAY cache blob content by CID
3. Implementations MAY support additional hash algorithms
