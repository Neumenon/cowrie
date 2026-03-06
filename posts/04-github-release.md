# Cowrie v2.0.0

Initial public release of Cowrie — a multi-language binary codec for structured data with ML extensions.

## Highlights

### Two Wire Formats

- **Gen1**: Simple binary JSON encoding — 11 core types + graph types. No header, single-pass encode/decode. A 46-byte JSON object encodes to 35 bytes (76%).
- **Gen2**: Dictionary-coded keys, gzip/zstd compression, ML extensions, graph types. 1,000 repeated-schema objects go from 48KB JSON to 23KB (47%).

### 5 Language Implementations

All implementations are feature-complete and pass the same 23 cross-language test fixtures.

| Language | Package | Version |
|----------|---------|---------|
| Go | `github.com/Neumenon/cowrie` | v0.1.1 |
| Rust | `cowrie-rs` | 0.1.1 |
| Python | `cowrie-py` | 0.1.1 |
| TypeScript | `cowrie-codec` | 0.1.1 |
| C | Source (CMake) | v0.1.1 |

### ML Extension Types (Gen2)

- **Tensor**: 12 dtypes (float32, float16, bfloat16, int8, ...), shaped, raw binary data
- **Image**: Format (JPEG/PNG/WebP/AVIF/BMP) + dimensions + raw bytes
- **Audio**: Encoding (PCM/Opus/AAC) + sample rate + channels + raw bytes

### Graph Types (Gen1 & Gen2)

- Node, Edge, NodeBatch, EdgeBatch, GraphShard, AdjList (CSR)
- Dictionary-coded property keys in Gen2 for size-efficient graph encoding
- Designed for GNN mini-batch transfer, graph database snapshots, streaming partitions

### CLI Tool

```bash
go install github.com/Neumenon/cowrie/cmd/cowrie@latest

echo '{"name":"Alice","age":30}' | cowrie encode --gen2 > data.cowrie
cowrie decode < data.cowrie
cowrie info < data.cowrie
```

### Security

- 7 critical/high vulnerability fixes in Gen2 decoder (buffer overflows, allocation bombs, infinite loops)
- 16 security regression tests
- 9 configurable decode limits with safe defaults (max depth: 1,000, max array length: 100M, max string length: 500MB, max tensor rank: 32)
- Size-bounded decompression — rejects oversized payloads before allocation

## Performance

| Payload | JSON | Gen1 | Gen2 |
|---------|------|------|------|
| Small object (3 fields) | 46 B | 35 B (76%) | 43 B (93%) |
| Large array (1000 objects) | 48 KB | 34 KB (70%) | 23 KB (47%) |
| Float array (10K floats) | 86 KB | 80 KB (93%) | — |
| Graph shard (100 nodes) | — | — | 10 KB |

## Install

```bash
# Go
go get github.com/Neumenon/cowrie@v0.1.1

# Rust
cargo add cowrie-rs@0.1.1

# Python
pip install cowrie-py==0.1.1

# TypeScript
npm install cowrie-codec@0.1.1

# C
git clone https://github.com/Neumenon/cowrie.git
cd cowrie/c && mkdir build && cd build && cmake .. && make
```

## Cross-Language Test Fixtures

23 shared fixtures ensure identical behavior across all implementations:

- **7 core**: null, true, int, float, string, array, object
- **7 ML**: tensor, tensor_ref, image, audio, adjlist, richtext, delta
- **5 graph**: node, edge, node_batch, edge_batch, graph_shard
- **4 negative**: bad_magic, bad_version, truncated, invalid_tag

## Links

- [Wire Format Specification (SPEC.md)](https://github.com/Neumenon/cowrie/blob/main/SPEC.md)
- [Full Changelog (CHANGELOG.md)](https://github.com/Neumenon/cowrie/blob/main/CHANGELOG.md)
- License: Apache 2.0
