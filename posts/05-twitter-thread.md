# Twitter/X Thread

Post as a thread (reply to each previous tweet). Include the image/code screenshot suggestions where noted.

---

**Tweet 1 (Hook)**

Shipped Cowrie — a binary JSON codec that works in Go, Rust, Python, C, and TypeScript.

No schema files. No code generation. Encode a map, get bytes, decode in any of the 5 languages.

github.com/Neumenon/cowrie

---

**Tweet 2 (Problem)**

The problem: Python ML services produce float32 embeddings. Go API services consume them.

Your options with JSON:
- base64-encode the raw bytes (lose type info)
- JSON array of numbers (3-4x size overhead)
- Protobuf (codegen in both languages)

None felt right.

---

**Tweet 3 (Gen1)**

Cowrie Gen1: simple binary encoding for JSON types.

{"name":"Alice","age":30,"active":true}
- JSON: 46 bytes
- Gen1: 35 bytes (76%)

Single-pass encode/decode. One dependency in Go (klauspost/compress). Good for real-time systems and embedded use.

---

**Tweet 4 (Gen2 — dictionary coding)**

Gen2 adds dictionary-coded keys.

1,000 objects with the same 4 keys:
- JSON encodes those keys 4,000 times
- Gen2 encodes them once in a shared dictionary, references by index

Result: 48KB JSON becomes 23KB (47%).

Add zstd compression on top and it goes lower.

---

**Tweet 5 (Code example)**

Python, one line each:

```python
from cowrie import gen1

data = gen1.encode({"name": "Alice", "scores": [1.0, 2.0, 3.0]})
result = gen1.decode(data)
```

Same API in every language. No .proto files, no generated code, no build pipeline changes.

[Suggest: screenshot of code or terminal output]

---

**Tweet 6 (ML types)**

Gen2 has native ML types at the wire level:

- Tensor: 12 dtypes (float32, float16, bfloat16...), shaped, raw binary
- Image: format (JPEG/PNG/WebP) + dimensions + bytes
- Audio: encoding (PCM/Opus) + sample rate + channels + bytes

No more base64 hacks.

---

**Tweet 7 (Graph types)**

Also: first-class graph types.

Node, Edge, GraphShard, CSR AdjList — built into the wire format.

Designed for GNN mini-batch transfer and graph database snapshots. Dictionary-coded property keys in Gen2.

---

**Tweet 8 (Cross-language parity)**

Cross-language parity: 23 shared test fixtures.

7 core types, 7 ML types, 5 graph types, 4 negative cases.

All 5 implementations must produce and consume identical bytes. If Go encodes it, Rust, Python, C, and TypeScript decode it the same way.

---

**Tweet 9 (Honesty + CTA)**

Honest trade-offs:
- Gen2 overhead on single small objects (93% of JSON)
- Python impl is pure Python — correct, not fast
- New project, not production-proven yet
- MsgPack/CBOR have broader language support

If you need schemaless binary + ML types + graph types, give it a try:

go get github.com/Neumenon/cowrie@v0.1.1
pip install cowrie-py
cargo add cowrie-rs

Apache 2.0 licensed. Feedback welcome.
