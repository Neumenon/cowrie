# Changelog

## 0.9.0 — Cowrie v1 (COWR) clean-room (2026-06-25)

Clean-room reimplementation of Cowrie as a deterministic, content-addressable,
AI-native binary codec. Magic `COWR`, format version 1. Authoritative spec:
`docs/SPEC-v1.md`. This is a pre-1.0 release candidate — the wire format is
**not yet frozen**.

### Added — deterministic content-addressable v1 format

The core invariant: **equal value => equal canonical bytes => equal hash**, and
those bytes are **byte-identical across Python, Go, Rust, and TypeScript**. The
content address is a multihash SHA-256 (spec §3). Verified: all four languages
emit the same content address
`122091f7d42a00c157c37f0929b15e90d8c785dbe50581cc651aafc338f6e5e8aad1`
for `{"name":"Alice","scores":[1,2,3]}`.

### Added — 11 cross-language conformance gates

Eleven standing conformance gates run via `bash tools/run_all_gates.sh`, wired
into CI at `.github/workflows/conformance.yml`. Green is law: no merge while red.

### Added — Python as a first-class CLI peer and installable package

The Python reference (built from `tools/`) is now a first-class peer of the other
implementations: it ships both an importable library (`import cowrie_ref`) and a
`cowrie` console script. PyPI package name: `cowrie-ref`.

- Library: `from cowrie_ref import encode, decode, content_address, fingerprint`
  — plain objects go in directly.
- CLI: `cowrie recode [--strict|--addr|--file-id|--file-recode|--tensor-spans|--fingerprint|--dataset-root] < wire`

### Added — publish workflow

Tag-triggered publishing via `.github/workflows/publish.yml` (fires on `v*`),
gate-gated against the 11 conformance gates. One-time setup before first publish:
PyPI Trusted Publisher for `cowrie-ref`, plus `NPM_TOKEN` and
`CARGO_REGISTRY_TOKEN` secrets.

### Changed — all implementations aligned to 0.9.0

The repository is now a monorepo (`go/`, `rust/`, `typescript/`, `tools/` for
Python), with every implementation pinned to version **0.9.0**. Registry
mappings: PyPI `cowrie-ref`, npm `cowrie-codec`, crates.io `cowrie-rs`, Go module
`github.com/Neumenon/cowrie/go`.

### Changed — Go module path `/v2` suffix dropped

The Go module is now `github.com/Neumenon/cowrie/go` (a subdir module; release
tags take the form `go/vX.Y.Z`). The legacy `/v2` major-version path suffix was
dropped.

### Install (from the `phase0-spec` branch — nothing published yet)

- Python: `pip install "git+https://github.com/Neumenon/cowrie.git@phase0-spec#subdirectory=tools"`
- Go: `go get github.com/Neumenon/cowrie/go@phase0-spec`
- Rust: `cowrie-rs = { git = "https://github.com/Neumenon/cowrie.git", branch = "phase0-spec" }`
- JS: npm cannot install a git subdir — `git clone -b phase0-spec <repo>; cd cowrie/typescript;
  npm install && npm run build && npm pack` -> `cowrie-codec-0.9.0.tgz`, then `npm install <path>.tgz`.

After publish: `pip install cowrie-ref` | `npm install cowrie-codec` |
`cargo add cowrie-rs` | `go get github.com/Neumenon/cowrie/go@v0.9.0`.

### Retired — legacy v2 implementation moved to `attic/`

The legacy pre-v1 implementation and its format are retired to `attic/`. The
entries below document that pre-v1 history and remain for the record.

---

## Legacy (pre-v1, archived)

The entries in this section are a verbatim historical record of the retired
pre-v1 implementation. They are preserved unchanged for provenance and may
reference retired terminology that no longer applies to the v1 format above.

## Unreleased

## v2.1.2 (2026-06-20)

### Changed — native Cython accelerator revived for the PyPI wheel

Regenerated `cowrie/_cext.c` with Cython 3.1 so it builds against the NumPy 2.x
C API (the bundled C used a pre-2.0 API and failed to compile, silently falling
back to pure Python), and fixed an incomplete dtype map in the raw-tensor encode
path that mis-encoded BOOL/INT16/UINT16/UINT32/UINT64 tensors as FLOAT32. The
native path now passes the full test suite plus a 13-dtype conformance check and
is ~10x faster than pure Python (measured: 11.7x encode, 9.2x decode). PyPI
Linux/macOS wheels again require and ship it (`COWRIE_REQUIRE_NATIVE=1`), built
against NumPy 2.x. Windows installs use the pure-Python sdist fallback until
native MSVC/zlib wheels are validated; Linux aarch64 wheels are deferred until
QEMU/binfmt wheel builds are explicitly wired into CI.

### Removed — Glyph pulled out into its own repository

The embedded Glyph text format (`go/glyph/`, the `glyph` and `bench` CLIs, and
`docs/glyph/`) is removed from cowrie. Glyph is maintained as a standalone project
at github.com/Neumenon/glyph, which carries the full source, its own docs, and the
glyph↔cowrie bridge. Cowrie's embedded copy had diverged from the canonical one
(57 of 74 files differed), so keeping it risked silent skew. The cowrie binary
codec has no dependency on Glyph — only the two removed CLIs and a
`//go:build cogs`-tagged cross-format test did. Go source shrinks ~30.6k → ~11.4k LOC.

To port: cowrie's copy carried a **BLOB_POOL** feature (`blob.go`, `pool.go`,
`auto_pool.go`, `document.ResolvePoolRefs`, and `types.go` additions, wired into the
old glyph CLI) that the standalone does not yet have. It is recoverable from git
history at the pre-removal commit and should be ported to github.com/Neumenon/glyph
if still wanted.

### Fixed — graph-type cross-language determinism; added Gen1 fixtures

Graph types (Node/Edge/NodeBatch/EdgeBatch, 0x35-0x38) now encode prop keys in
UTF-8 byte order in **all** implementations, so deterministic encoding is
byte-identical across Go, Rust, Python, and TypeScript. Previously Go assigned
dictionary indices and emitted node/edge props in random map order; Python's and
TypeScript's deterministic encoders had no graph arms and silently produced
empty/zero-byte payloads. Each non-Go impl now pins a regression test to the Go
canonical bytes. (Graph tags 0x35-0x38 remain active; the old review notes live under `attic/docs/review-notes/`.)

Added Gen1 cross-language fixtures (`testdata/fixtures/gen1/`) for core
object/array/int/string plus Float64Array (0x17) and Float32Array (0x18)
proto-tensors. The fixture manifest now has 55 cases, including JSON-bridge
`from_json` fixtures and framed gzip/zstd decode fixtures.

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
