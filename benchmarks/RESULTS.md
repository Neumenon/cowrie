# Cowrie Gen1 vs Gen2 Decode Benchmark Results

**Hardware**: AMD Ryzen 7 7700X 8-Core, 64GB DDR5, Linux 6.17
**Date**: 2026-03-06
**Compilers**: Go 1.21, Rust 1.x release, gcc -O2, Node 20 (V8), Python 3.14

## Fixture Sizes (vs JSON baseline)

| Payload | Description | JSON | Gen1 | Gen2 | Gen1/JSON | Gen2/JSON |
|---------|-------------|------|------|------|-----------|-----------|
| small | 3-field object | 46 B | 35 B (0.76x) | 43 B (0.93x) | 24% smaller | 7% smaller |
| medium | 20-field + nested | 250 B | 143 B (0.57x) | 172 B (0.69x) | 43% smaller | 31% smaller |
| large | 1000x {id,name,value} | 48,623 B | 33,939 B (0.70x) | 22,958 B (0.47x) | 30% smaller | **53% smaller** |
| floats | 10,000 float64 | 86,236 B | 80,003 B (0.93x) | 40,013 B (0.46x) | 7% smaller | **54% smaller** |

## Decode Speed vs JSON (ops/s)

Values >1.0x mean faster than JSON.parse / json.Unmarshal.

### Go

| Payload | JSON ops/s | Gen1 ops/s | Gen2 ops/s | Gen1/JSON | Gen2/JSON |
|---------|-----------|-----------|-----------|-----------|-----------|
| small | 2.4M | 7.9M | 2.5M | **3.3x faster** | 1.1x faster |
| medium | 390K | 1.6M | 518K | **4.1x faster** | 1.3x faster |
| large | 2,860 | 7,448 | 1,940 | **2.6x faster** | 0.68x |
| floats | 1,465 | 135K | 463K | **92x faster** | **316x faster** |

### Rust

| Payload | Gen1 ops/s | Gen2 ops/s | Gen1/Gen2 |
|---------|-----------|-----------|-----------|
| small | 9.3M | 4.6M | Gen1 2.0x faster |
| medium | 1.2M | 508K | Gen1 2.4x faster |
| large | 3,511 | 1,666 | Gen1 2.1x faster |
| floats | 108K | 1.8M | **Gen2 17x faster** |

### TypeScript (V8)

| Payload | JSON ops/s | Gen1 ops/s | Gen2 ops/s | Gen1/JSON | Gen2/JSON |
|---------|-----------|-----------|-----------|-----------|-----------|
| small | 3.9M | 979K | 794K | 0.25x | 0.20x |
| medium | 1.4M | 166K | 177K | 0.12x | 0.13x |
| large | 6,816 | 984 | 1,468 | 0.14x | 0.22x |
| floats | 3,981 | 380 | 81K | 0.10x | **20x faster** |

### Python

| Payload | JSON ops/s | Gen1 ops/s | Gen2 ops/s | Gen1/JSON | Gen2/JSON |
|---------|-----------|-----------|-----------|-----------|-----------|
| small | 763K | 863K | 222K | **1.1x faster** | 0.29x |
| medium | 409K | 133K | 46K | 0.32x | 0.11x |
| large | 3,398 | 900 | 304 | 0.26x | 0.09x |
| floats | 1,635 | 1,206 | 248K | 0.74x | **152x faster** |

### C

| Payload | Gen1 ops/s | Gen2 ops/s | Gen1/Gen2 |
|---------|-----------|-----------|-----------|
| small | 10.3M | 6.4M | Gen1 1.6x faster |
| medium | 883K | 471K | Gen1 1.9x faster |
| large | 5,040 | 4,115 | Gen1 1.2x faster |
| floats | 982K | 943K | ~parity |

## Key Findings

### 1. Both Gen1 and Gen2 beat JSON for Go (the reference impl)
Go Gen1 is **2.6-92x faster** than `json.Unmarshal` across all payloads. Gen2 is faster for small objects (1.1-1.3x) and massively faster for floats (316x). For large repeated-key arrays, Gen2 is slightly slower than JSON (0.68x) due to dictionary construction overhead — but the payload is 53% smaller on the wire.

### 2. V8's JSON.parse is extremely hard to beat
In TypeScript, `JSON.parse()` is a native C++ implementation in V8 that's 5-10x faster than any JS-implemented decoder (Gen1/Gen2). The only exception is float arrays, where Gen2's zero-copy tensor decode is 20x faster. This means **for TS users, Cowrie's value proposition is wire size savings, not decode speed** (except for numeric data).

### 3. Python's json module is also native C
Python's `json.loads()` is C-accelerated, making it hard to beat with pure Python decoders. Gen1 matches JSON for small objects (1.1x) but falls behind for larger payloads (0.26x). Gen2's float tensor decode is again the standout at 152x faster.

### 4. Gen2 tensor decode is universally dominant
Across all 5 languages, Gen2 float decode is dramatically faster than both Gen1 and JSON:
- Go: 316x faster than JSON, 3.4x faster than Gen1
- Rust: 17x faster than Gen1
- TypeScript: 20x faster than JSON, 213x faster than Gen1
- Python: 152x faster than JSON, 205x faster than Gen1
- C: ~parity with Gen1 (both are already zero-copy in C)

### 5. Language performance tiers (structured object decode)
| Tier | Language | Gen1 ops/s (small) | Notes |
|------|----------|-------------------|-------|
| 1 | C | 10.3M | Fastest raw decode |
| 1 | Rust | 9.3M | Neck and neck with C |
| 2 | Go | 7.9M | Slightly behind due to GC allocations |
| 3 | TypeScript | 979K | V8 JIT, ~10x slower than compiled |
| 3 | Python | 863K | CPython, surprisingly close to TS |

### 6. Gen2 dictionary overhead is consistent: ~2x for objects
Across C (1.6x), Rust (2.0x), and Go (3.0x), Gen2 decode is roughly 2x slower than Gen1 for structured data. The overhead comes from: (1) header validation (magic + version + flags), (2) dictionary construction (reading all key strings upfront), (3) field ID lookup per decoded field.

## Recommendation

| Use Case | Format | Why |
|----------|--------|-----|
| Hot-path API responses (<1KB) | Gen1 | Fastest decode, minimal overhead |
| Repeated-key payloads (logs, events) | Gen2 | 47-53% smaller on wire, dictionary amortizes |
| Numeric arrays / tensors | Gen2 | 3-316x faster decode via zero-copy |
| Storage / network transfer | Gen2 | Consistently 30-54% smaller |
| TypeScript-heavy stack | Gen1 or JSON | Can't beat V8's native JSON.parse for speed |
| Cross-language interop | Gen1 | Simplest, widest parity, fastest everywhere |

---

## V3 Inline Types & Performance Optimizations

**Date**: 2026-03-08
**Hardware**: AMD Ryzen 7 7700X 8-Core, Linux 6.17

### Wire Size Improvements (v3 vs v2)

| Fixture | V2 | V3 | Savings |
|---------|----|----|---------|
| small.gen2 | 43 B | 41 B | -5% |
| medium.gen2 | 172 B | 168 B | -2% |
| large.gen2 (1000 objects) | 22,958 B | 21,766 B | **-5.2%** |
| floats.gen2 (tensor) | 40,013 B | 40,013 B | 0% (tensor unchanged) |

Wire savings come from FIXINT (0-127 → 1 byte), FIXNEG (-1..-16 → 1 byte), FIXARRAY (≤15 → 1 byte), FIXMAP (≤15 → 1 byte).

### BITMASK vs Bool/Int Arrays (2048 elements)

| Encoding | Wire Size | Encode (ns/op) | Decode (ns/op) |
|----------|-----------|----------------|----------------|
| **Bitmask** | **264 B** | **65** | **113** |
| Bool array | 2,056 B | 8,850 | 163,000 |
| Int array | 2,056 B | — | — |

**Bitmask is 7.8x smaller, 135x faster encode, 1,400x faster decode.**

### Unsafe Strings (zero-copy decode, Go only)

| Mode | ops/s | Allocs/op | MB/s |
|------|-------|-----------|------|
| Safe strings | ~4,100 | 5,007 | ~113 |
| Unsafe strings | ~4,900 | 3,003 | ~131 |

**16% faster, 40% fewer allocations** on string-heavy payloads (500 objects × 4 string fields).

### Scatter-Gather EncodeToWriter (Go only, 1MB tensor)

| Method | ns/op | Alloc |
|--------|-------|-------|
| Encode() | 121,000 | 2.1 MB |
| EncodeToWriter() | 64,000 | 1.0 MB |

**1.9x faster, 50% less memory** — avoids copying tensor data into intermediate buffer.

### TensorSink Streaming Decode (Go only, 1MB tensor)

| Method | ns/op | Alloc | Throughput |
|--------|-------|-------|------------|
| Standard decode | 47,000 | 1.0 MB | ~22 GB/s |
| TensorSink | 10,700 | 5 KB | ~98 GB/s |

**4.4x faster, 207x less memory** — streams tensor body to callback without allocation.

### Large Payload Decode (1000 objects, safe vs unsafe)

| Mode | ns/op | Allocs/op | MB/s |
|------|-------|-----------|------|
| Decode (safe) | ~404,000 | 6,006 | ~54 |
| Decode (unsafe) | ~399,000 | 5,003 | ~55 |

Modest 3% speed gain on mixed payloads (fewer string fields than pure-string benchmark).

### V3 Cowrie vs JSON (raw wire size)

| Payload | JSON | Cowrie V3 | Savings |
|---------|------|-----------|---------|
| Small API (5 fields) | 140 B | 108 B | 22.9% |
| Nested objects (20×4 fields) | 1,044 B | 518 B | 50.4% |
| Large log (100 records) | 14,218 B | 6,454 B | 54.6% |
| Telemetry (200 records) | 25,864 B | 10,127 B | **60.8%** |
| Float tensor (256×256) | 713 KB | 256 KB | **64.1%** |
