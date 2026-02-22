# GLYPH Substrate Comparison: Agent Workflow Benchmarks

**Date:** December 25, 2025
**Version:** GLYPH 1.0 with Auto-Pool

---

## Executive Summary

This report compares GLYPH against JSON, ZON, and TOON as an **agent substrate** - not just compression, but semantic capabilities critical for LLM agent workflows.

### Verdict

| Role | Winner | Notes |
|------|--------|-------|
| **Compression Champ** | ZON | -74% vs JSON, but limited semantics |
| **Agent Substrate Champ** | **GLYPH+Pool** | -62% vs JSON + dedup + refs + round-trip |
| **Readability** | TOON/GLYPH | Human-friendly, grep-able |
| **Round-Trip Safety** | JSON/TOON/GLYPH+Pool | ZON fails on some nulls |

### Key Metrics (100-Step Agent Trace)

| Codec | Size vs JSON | Tokens vs JSON | Pool Refs | Round-Trip |
|-------|--------------|----------------|-----------|------------|
| JSON | baseline | baseline | 0 | 100% |
| ZON | **-74.3%** | -63.2% | 0 | FAIL |
| TOON | +11.6% | +16.7% | 0 | 100% |
| GLYPH | -13.1% | -5.6% | 0 | FAIL* |
| **GLYPH+Pool** | **-61.6%** | **-48.8%** | **265** | 100%** |

*GLYPH without pools has tabular parsing issues for to-json
**GLYPH+Pool round-trip works with pool resolution

---

## Test Methodology

### Datasets

| Dataset | Description | Key Stress |
|---------|-------------|------------|
| Agent Trace (100 steps) | Full agent execution with tool calls | Repeated schemas, system prompts |
| Agent Trace (50 steps) | Smaller agent execution | Same patterns, 50% size |
| Conversation (10/20 turns) | Multi-turn chat with tools | Repeated system prompt per turn |
| Tool Calls (100 calls) | Batch of tool invocations | 4 schemas repeated 25x each |
| Event Log (500/1000 events) | High-frequency event stream | Repeated message patterns |

### Metrics Measured

1. **Size** - Raw bytes, gzip bytes, LLM tokens (cl100k)
2. **Round-Trip Safety** - encode -> decode -> compare
3. **Incremental Updates** - Diff size for single field change
4. **Developer Usability** - grep-ability, line count, readable keys
5. **Dedup Efficiency** - Pool refs created, actual vs theoretical savings

---

## Detailed Results

### 1. Size & Token Efficiency

#### Agent Trace (100 steps) - Dedup Stress Test

```
+------------+--------+---------+--------+---------+------+
| Codec      | Bytes  | vs JSON | Tokens | vs JSON | Gzip |
+------------+--------+---------+--------+---------+------+
| JSON       | 129924 | 0.0%    | 30489  | 0.0%    | 5140 |
| ZON        | 33388  | -74.3%  | 11209  | -63.2%  | 4032 |
| TOON       | 144970 | +11.6%  | 35595  | +16.7%  | 5698 |
| GLYPH      | 112848 | -13.1%  | 28773  | -5.6%   | 5160 |
| GLYPH+Pool | 49941  | -61.6%  | 15601  | -48.8%  | 4230 |
+------------+--------+---------+--------+---------+------+
```

**Analysis:**
- ZON achieves best raw compression but fails round-trip tests
- GLYPH+Pool achieves **55.7% internal savings** from pooling alone
- 265 pool references replaced inline strings
- Token savings (-48.8%) directly reduce LLM API costs

#### Conversation (20 turns) - System Prompt Repetition

```
+------------+-------+---------+--------+---------+------+
| Codec      | Bytes | vs JSON | Tokens | vs JSON | Gzip |
+------------+-------+---------+--------+---------+------+
| JSON       | 13330 | 0.0%    | 3328   | 0.0%    | 1399 |
| ZON        | 10032 | -24.7%  | 2747   | -17.5%  | 1369 |
| TOON       | 16700 | +25.3%  | 4042   | +21.5%  | 1460 |
| GLYPH      | 10863 | -18.5%  | 3279   | -1.5%   | 1424 |
| GLYPH+Pool | 9992  | -25.0%  | 2922   | -12.2%  | 1406 |
+------------+-------+---------+--------+---------+------+
```

**Analysis:**
- GLYPH+Pool beats ZON on conversations (-25.0% vs -24.7%)
- System prompt (500+ chars) appears once in pool, referenced 20x

---

### 2. Round-Trip Safety

```
+------------+--------+--------+-------+------------+
| Codec      | Encode | Decode | Match | Notes      |
+------------+--------+--------+-------+------------+
| JSON       | OK     | OK     | OK    | Reference  |
| ZON        | OK     | FAIL   | -     | Null handling bug |
| TOON       | OK     | OK     | OK    | Good       |
| GLYPH      | OK     | FAIL   | -     | Tabular parsing* |
| GLYPH+Pool | OK     | OK     | OK**  | Good       |
+------------+--------+--------+-------+------------+
```

*GLYPH without pools uses @tab format that has parsing edge cases
**GLYPH+Pool resolves pool refs during decode, producing valid JSON

**Implications:**
- ZON cannot be trusted for data that may contain nulls in certain structures
- GLYPH+Pool is production-safe for round-trip workflows

---

### 3. Incremental Update Cost

When changing a single field (`status: "pending" -> "modified"`):

```
+------------+----------+----------+------------+
| Codec      | Original | Modified | Diff Lines |
+------------+----------+----------+------------+
| JSON       | 129924   | 129932   | 1          |
| ZON        | 33388    | 33404    | 67         |
| TOON       | 144970   | 144978   | 1          |
| GLYPH      | 112848   | 112856   | 1          |
| GLYPH+Pool | 49941    | 49949    | 1          |
+------------+----------+----------+------------+
```

**Analysis:**
- ZON has poor diff locality (67 lines changed for 1 field)
- GLYPH/TOON/JSON all have excellent diff locality (1 line)
- This matters for git history and human review

---

### 4. Dedup Efficiency

```
+------------+-----------+------------------+----------+
| Codec      | Pool Refs | Theoretical Save | Actual % |
+------------+-----------+------------------+----------+
| JSON       | 0         | 61354            | 0.0%     |
| ZON        | 0         | 61354            | 74.3%    |
| TOON       | 0         | 61354            | -11.6%   |
| GLYPH      | 0         | 61354            | 13.1%    |
| GLYPH+Pool | 265       | 61354            | 61.6%    |
+------------+-----------+------------------+----------+
```

**GLYPH Pool Savings Breakdown:**
- 12 unique long strings identified
- 265 references replaced inline occurrences
- Theoretical savings: 61,354 bytes (repeated strings)
- Actual savings: 55.7% from pooling mechanism

---

### 5. Developer Usability

```
+------------+------------+-----------+-------+---------------+
| Codec      | Grep:error | Grep:tool | Lines | Readable Keys |
+------------+------------+-----------+-------+---------------+
| JSON       | YES        | YES       | 1     | YES           |
| ZON        | YES        | YES       | 123   | YES           |
| TOON       | YES        | YES       | 3595  | YES           |
| GLYPH      | YES        | YES       | 414   | YES           |
| GLYPH+Pool | YES        | YES       | 104   | YES           |
+------------+------------+-----------+-------+---------------+
```

**Analysis:**
- All formats are grep-able for debugging
- GLYPH+Pool has fewest lines (most compact) while maintaining readability
- ZON uses special syntax that may be unfamiliar but still searchable

---

### 6. Streaming Growth (Event Log)

Bytes per event as event count increases:

```
+------------+-------+-------+-------+--------+---------+
| Codec      | @50   | @100  | @200  | @500   | @1000   |
+------------+-------+-------+-------+--------+---------+
| JSON       | 188.6 | 188.4 | 189.3 | 190.0  | 190.2   |
| ZON        | 86.0  | 83.7  | 83.7  | 83.8   | 83.8    |
| TOON       | 211.2 | 211.0 | 212.0 | 212.7  | 212.9   |
| GLYPH      | 123.7 | 122.9 | 123.6 | 124.2  | N/A*    |
| GLYPH+Pool | 109.2 | 107.1 | 107.1 | 107.1  | N/A*    |
+------------+-------+-------+-------+--------+---------+
```

*Large event logs hit timeout for shell-based encoding

**Analysis:**
- ZON has best bytes/event but no dedup semantics
- GLYPH+Pool shows slight efficiency gain at scale (dedup kicks in)
- All formats are roughly O(n) - no format achieves sublinear growth without external dedup

---

## GLYPH Auto-Pool Feature

### Usage

```bash
# Enable automatic string pooling
echo '{"data": [...]}' | glyph fmt-loose --llm --auto-pool

# Configure pooling thresholds
glyph fmt-loose --auto-pool --min-occurs=2 --min-length=20
```

### How It Works

1. **Pass 1:** Walk the JSON tree, count string occurrences
2. **Filter:** Strings with length >= 20 and occurrences >= 2
3. **Pool:** Create `@pool.str` definition with candidates
4. **Pass 2:** Emit value with `^S1:N` references replacing strings

### Example Output

**Input:**
```json
{
  "system": "You are a helpful assistant...",
  "messages": [
    {"role": "user", "content": "Hello"},
    {"role": "assistant", "content": "You are a helpful assistant..."}
  ]
}
```

**Output (GLYPH+Pool):**
```
@pool.str id=S1 ["You are a helpful assistant..."]

{messages=[{content=Hello role=user} {content=^S1:0 role=assistant}] system=^S1:0}
```

**Savings:** System prompt stored once, referenced twice.

---

## Recommendations

### When to Use Each Format

| Use Case | Recommended | Why |
|----------|-------------|-----|
| One-shot JSON encoding | ZON | Best compression |
| Agent traces with repeated schemas | **GLYPH+Pool** | Dedup saves 50%+ |
| Human-readable config | TOON | Most readable |
| Streaming with dedup | **GLYPH+Pool** | Pool refs work cross-message |
| Round-trip critical | JSON/TOON/GLYPH+Pool | ZON has null bugs |
| Line-based diffing | GLYPH/TOON | Good locality |

### GLYPH Roadmap

1. **Implemented:** Auto-pool for strings (this release)
2. **Next:** Object pools for repeated structures
3. **Future:** Automatic schema inference and cross-session pool persistence

---

## Appendix: Running the Benchmarks

```bash
# Quick mode (2 datasets)
cd sjson/benchmark/comparison/js
node codec_substrate_bench.mjs --quick

# Full mode (all datasets)
node codec_substrate_bench.mjs

# Single dataset
node codec_substrate_bench.mjs --dataset=agent_trace_100
```

### Requirements

- Node.js 18+
- npm packages: `@toon-format/toon`, `zon-format`, `gpt-tokenizer`
- GLYPH binary at `sjson/glyph/cmd/glyph/glyph`

---

## Conclusion

**ZON** remains the compression champion for one-shot encoding where round-trip isn't critical.

**GLYPH+Pool** is the agent substrate champion:
- **61.6% size reduction** on agent traces (vs JSON)
- **48.8% token savings** (direct LLM cost reduction)
- **265 pool refs** for automatic deduplication
- **Round-trip safe** (unlike ZON)
- **Grep-able and diff-friendly** (unlike minified JSON)

For agent workflows with repeated schemas, system prompts, and tool definitions, GLYPH+Pool delivers the best balance of compression, semantics, and developer experience.
