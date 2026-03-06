# Show HN Post

## Title Options

1. **Show HN: Cowrie -- Binary JSON codec with ML types, dictionary coding, 5 languages**
2. **Show HN: Cowrie -- Schemaless binary codec for JSON with native tensors and graph types**
3. **Show HN: Cowrie -- Binary JSON in Go, Rust, Python, C, TypeScript. No codegen.**

**Recommended:** Option 1 — most informative, hits the key differentiators, under 80 chars.

## URL

```
https://github.com/Neumenon/cowrie
```

## Backstory Comment

Post this as the first comment immediately after submitting:

---

Hi HN, I built Cowrie to solve a specific problem: moving tensors and structured data between a Python ML pipeline and Go API services without base64 hacks or Protobuf codegen.

**What it is:** A binary codec for JSON-like data in two variants. Gen1 is a simple binary encoding (35 bytes vs 46 bytes JSON for a small object). Gen2 adds a header with dictionary-coded keys and optional gzip/zstd compression — for 1,000 objects with repeated schemas, it's 23KB vs 48KB JSON (47%).

**What's different from MessagePack/CBOR:** Three things. (1) Dictionary coding — Gen2 deduplicates object keys into a shared dictionary, which is where most of the size savings come from on repeated schemas. MsgPack and CBOR encode keys inline every time. (2) Native ML types — Tensor (12 dtypes, shaped), Image (JPEG/PNG/WebP/AVIF/BMP with dimensions), Audio (PCM/Opus/AAC with sample rate and channels). These are wire-level types, not application conventions. (3) Graph types — Node, Edge, GraphShard, CSR adjacency lists. Designed for GNN mini-batch transfer.

**What's different from Protobuf/FlatBuffers:** No schema files, no code generation. You pass a map/dict/object and get bytes. The trade-off is you lose compile-time type safety.

**Caveats I want to be upfront about:**
- Gen2 has header overhead. A single small object is 93% of JSON size (worse than Gen1's 76%). Dictionary coding only pays off with repeated schemas.
- The Python implementation is pure Python, no C extension. It's correct but not fast.
- This is a new project. Well-tested (23 cross-language fixtures, 16 security regression tests), but not battle-tested in production.
- Language support is 5 implementations. MsgPack and Protobuf support dozens.

**Try it:**
```bash
go install github.com/Neumenon/cowrie/cmd/cowrie@latest
echo '{"name":"Alice","scores":[1.0,2.0,3.0]}' | cowrie encode --gen2 | cowrie decode
```

Happy to answer questions about the wire format, the dictionary coding approach, or any of the implementation trade-offs.

---

## Engagement Strategy

### Anticipated Questions and Response Notes

**Q: "Why not just use MessagePack/CBOR?"**
A: Acknowledge they're more mature and widely supported. Cowrie's value is dictionary coding for repeated schemas, native ML types, and graph types. If you don't need those, MsgPack/CBOR are great choices. Don't be defensive — frame it as different design targets.

**Q: "Why not Protobuf/FlatBuffers?"**
A: Different trade-off. Protobuf gives you type safety and a mature ecosystem but requires schema management and codegen. Cowrie is schemaless — the encoder infers structure from the data. Useful when schemas change frequently or when you want to avoid build pipeline integration.

**Q: "What are the benchmarks? Encode/decode speed?"**
A: Be honest. Size numbers are in the README. Throughput benchmarks exist (Go: `go test -bench=.`, Rust: `cargo bench`) but haven't been published as formatted comparisons yet. Offer to share raw numbers if asked.

**Q: "How do you handle schema evolution?"**
A: Cowrie is schemaless — there's no schema to evolve. If you add a field, it just appears in the encoded data. If you remove one, it's gone. The trade-off is no validation that the sender and receiver agree on structure. Schema fingerprinting (FNV-1a) in Gen2 can detect drift but not enforce compatibility.

**Q: "Pure Python is going to be slow."**
A: Agree. Acknowledge it directly. The Python impl is for correctness and interoperability, not throughput. For hot paths, use the Go or Rust implementation and call via FFI, or accept the trade-off.

**Q: "Why not just compress JSON with gzip?"**
A: For repeated schemas, gzip on JSON gets you to roughly 60-70% depending on content. Gen2 dictionary coding alone gets to 47%, and you can add gzip/zstd on top for further reduction. The bigger advantage is the ML types — gzip doesn't solve the base64-tensor problem.

### Timing

- Post between 8-10 AM ET on a weekday (Tuesday-Thursday preferred)
- Be available to respond for the first 2-3 hours, then check periodically for 12-24 hours
- Don't bump or ask for upvotes

### Tone

- Technical, factual, understated
- Lead with what the project does, not why it's great
- Acknowledge trade-offs before anyone asks
- Thank people for specific technical feedback
- If someone points out a real issue, acknowledge it and file it
