# Changelog

## v2.0.0 (2026-02-26)

Initial public release of Cowrie — a multi-language binary codec for structured data with ML extensions.

### Features

- **Gen1 codec**: Lightweight binary JSON with 11 core types + graph types (Node, Edge, NodeBatch, EdgeBatch, GraphShard, Adjlist)
- **Gen2 codec**: Full v2 with ML extensions — 18+ wire types, dictionary coding, gzip/zstd compression, schema fingerprinting
- **GLYPH text format**: Human-readable structured output with streaming validation, tabular mode, and lossless JSON round-trip
- **5 language implementations**: Go, Python, Rust, C, TypeScript — all feature-complete with cross-language compatibility
- **Graph types**: CSR adjacency, heterogeneous graphs, temporal edges
- **Streaming**: Record-by-record (Gen1) and framed master stream (Gen2)
- **CLI tool**: `cowrie encode/decode/info` for command-line usage (Go)

### Security

- 7 critical/high vulnerability fixes in Gen2 decoder (buffer overflows, allocation bombs, infinite loops)
- 16 security regression tests

### Cross-Language Parity

- Configurable limits, zstd encode, hints skip across all implementations
- Cross-language compatibility test fixtures
- Deterministic encoding for content-addressable storage
