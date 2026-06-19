# Changelog

## Unreleased

### Dropped — C implementation; Glyph consolidated to Go-only

The standalone C implementation (`c/`, incl. `c/glyph/`) is removed. It published
to no registry, used a `SJFR` compression-framing envelope incompatible with the
wire spec, and gated releases without shipping anything. The Python package is
unaffected: its Cython `_cext` fast path builds from the self-contained `python/csrc/`
sources (not `c/`), and pure-Python remains the fallback. The dead ctypes path
(`_native.py`/`_fast.py`, which loaded a `.so` built from `c/`) is removed.

Glyph is now maintained Go-only: the diverged, unpublished `rust/glyph-codec/`
copy is removed (use the standalone `glyph` repo for any future Rust/JS glyph
publishing). The stale `benchmarks/` tree and `docs/glyph/archive/` are removed.

### Scope cut — graph types, RichText, Delta, ColumnHints parked

In **Gen2**, wire-format tags `0x30` (AdjList), `0x31` (RichText), `0x32`
(Delta), and `0x39` (GraphShard) are now reserved (deprecated). Gen2 encoders no
longer emit them; decoders skip the length-prefixed payload silently. The
dedicated Gen2 packages (`go/graph/`, `go/gnn/`, `go/ld/`, `go/delta/`,
hints/column helpers) moved to `attic/` — revivable but not built or tested by
default. **Gen1 retains AdjList (0x30) and GraphShard (0x39)** as active graph
types (implemented in `go/gen1`).

The `FlagHasColumnHints = 0x08` header bit remains listed but is reserved;
decoders MUST skip it.

**Fingerprint impact**: schema fingerprints of any historical objects that
contained Adjlist, RichText, Delta, or GraphShard values will change after
this cut, since their switch arms are gone. Acceptable because those types
were experimental and not in production use.

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
