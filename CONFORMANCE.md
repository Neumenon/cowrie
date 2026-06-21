# Cowrie Cross-Language Conformance

This document is the canonical reference for how cross-language correctness is
guaranteed across the Go, Rust, Python, and TypeScript implementations.

---

## Overview

Correctness guarantees are built in four layers:

1. **Shared fixture manifest** — binary `.cowrie` files + expected JSON, decoded
   by the Go reference and Python decoders under the shared harness; Rust and
   TypeScript are gated by their own pinned parity tests.
2. **Per-language pinned parity tests** — decode invariants, schema fingerprint
   stability, and JSON round-trip tests pinned inside each language's own test
   suite.
3. **Fuzz testing** — Go and Rust each run structured fuzzing in CI weekly; Python
   runs Hypothesis-based property tests.
4. **Mutation / truth-table tests** — TypeScript and Go verify that decoder
   behaviour on corrupted or edge-case inputs matches a pinned truth table.

---

## Layer 1 — Fixture manifest (`testdata/fixtures/`)

### What is covered

| Group | Count | Kind | Notes |
|---|---|---|---|
| `core/` | 7 | decode + JSON check | null, bool, int, float, string, array, object |
| `ml/` | 9 | decode (ok-only or ERR_TRAILING_DATA) | tensor, rank0_scalar, bool_tensor, tensor_ref, image, audio; adjlist/richtext/delta reserved |
| `graph/` | 5 | decode (4 ok-only, 1 ERR_TRAILING_DATA) | node, edge, node_batch, edge_batch; graph_shard reserved |
| `neg/` | 5 | decode + error check | bad_magic, bad_version, truncated, invalid_tag, invalid audio channels |
| `v3/` | 11 | decode (JSON or ok-only) | fixint, fixneg, fixarray, fixmap, bitmask variants |
| `gen1/` | 6 | decode (4 JSON, 2 ok-only) | gen1 core types + proto-tensor float64/float32 |
| `bigint/` | 3 | decode + JSON check | pos_large, neg_large, u64plus1 — all round-trip as JSON strings |
| `compressed/` | 2 | decode_framed + JSON check | gzip_framed, zstd_framed — verify `EncodeFramed` / `DecodeFramed` path |
| `fromjson/` | 7 | from_json bridge | primitives, tensor, image, audio, and invalid audio bounds |

**Total: 55 cases** as of 2026-06-21.

### Fixture format

Each fixture is a raw `.cowrie` binary file. The manifest (`manifest.json`) maps
each case to:

```json
{
  "id":     "gen2_bigint_neg_large",
  "gen":    2,
  "kind":   "decode",
  "input":  "bigint/neg_large.cowrie",
  "expect": {"ok": true, "json": "bigint/neg_large.json"}
}
```

- `expect.ok == true` + `expect.json` — decode must succeed and the JSON
  projection must equal the expected file.
- `expect.ok == true` (no `json`) — decode must succeed; no canonical JSON
  projection (ML / graph / bitmask types).
- `expect.ok == false` — decode or JSON-bridge conversion must fail (negative / error cases).
- `kind == "decode"` — decode a raw Gen1/Gen2 `.cowrie` binary.
- `kind == "decode_framed"` — decode a framed/compressed Gen2 `.cowrie` binary.
- `kind == "from_json"` — convert JSON projection to Cowrie (`FromJSON`/`from_json`) and round-trip accepted cases.
- `python_skip` (string) — optional; if present, the Python cross-check is
  skipped for this case only. Currently used for `gen1_proto_float32`
  (tagArrayFloat32 / 0x18 is not implemented in the Python gen1 decoder).

### Generating fixtures

The Go CLI (`go/cmd/cowrie`) is the oracle. Build it with:

```sh
cd go && go build -o /tmp/cowrie-cli ./cmd/cowrie
```

Generate a binary fixture:

```sh
echo '123456789012345678901234567890' | /tmp/cowrie-cli encode --gen2 > testdata/fixtures/bigint/pos_large.cowrie
/tmp/cowrie-cli decode < testdata/fixtures/bigint/pos_large.cowrie > testdata/fixtures/bigint/pos_large.json
```

Compressed fixtures require a payload larger than 256 bytes (the Go
`compressThreshold`). If the compressed output is no smaller than the plain
output, `EncodeFramed` silently falls back to uncompressed. The flag byte in the
header reveals the actual encoding: `0x00` = uncompressed, `0x03` = gzip,
`0x05` = zstd.

---

## Layer 2 — Harness (`testdata/fixtures/validate_fixtures.py`)

### Decoders

| Decoder | When used | How |
|---|---|---|
| Go (primary) | All 55 cases | subprocess `cowrie decode [--gen1]` or `cowrie encode` for `from_json` cases |
| Python (secondary) | All gen2 cases + gen1 where tag is implemented | in-process `COWRIE_PUREPYTHON=1` |

The Python decoder is loaded from the sibling `python/` source tree (or
`PYTHON_PKG_DIR` env) using `COWRIE_PUREPYTHON=1` to bypass the Cython
extension (which may have ABI incompatibilities with the installed NumPy). The
gen2 decode path uses `decode_framed()` so compressed fixtures are handled
transparently.

Rust and TypeScript are not run by this harness — they are gated by their own
pinned tests (see Layer 2b).

### Running the harness

```sh
cd go && go build -o /tmp/cowrie-cli ./cmd/cowrie
GO_CLI=/tmp/cowrie-cli python3 testdata/fixtures/validate_fixtures.py
```

Expected output:

```
  Python decoder: loaded from .../python (PUREPYTHON)
  OK   gen2_core_null
  ...
  OK   gen1_proto_float32 [py:skip]
  OK   gen2_bigint_pos_large
  OK   gen2_bigint_neg_large
  OK   gen2_bigint_u64plus1
  OK   gen2_compressed_gzip
  OK   gen2_compressed_zstd

Results: 55 passed, 0 skipped, 0 failed, 1 py-skipped
```

Exit code is non-zero on any failure.

### Failure semantics

- A case fails if either decoder disagrees with the manifest — both decoders
  must accept what `ok=true` mandates and reject what `ok=false` mandates.
- JSON comparison uses `json.loads()` on both sides so whitespace differences
  are normalised. Python gen1 returns floats for integers (e.g. `1.0`) which
  are equal to `1` under `json.loads` comparison.

---

## Layer 2b — Per-language pinned parity tests

Each implementation carries its own suite of pinned tests that verify decode
invariants independently of the shared harness.

### Go (`go/`)

| File | What it pins |
|---|---|
| `invariant_test.go` | 7 codec invariants (encode→decode roundtrip, etc.) |
| `schema_fingerprint_test.go` | Schema fingerprint stability across Go runs |
| `deterministic_test.go` | `EncodeWithOptions(Deterministic=true)` produces identical bytes across calls |
| `truth_table_test.go` | Encoder/decoder behaviour against a pinned truth-case JSON manifest |
| `gen2_test.go`, `spec_test.go` | Wire-format spec compliance |
| `v3_test.go` | v3 inline types (fixint, fixneg, fixarray, fixmap) |

### Rust (`rust/`)

`cargo test --test coverage_boost` runs 173 tests covering:

- Encode/decode roundtrip for all value types.
- `encode_framed` / `decode_framed` for gzip and zstd.
- `schema_fingerprint64` stability.
- `from_json` / `to_json` round-trip for gen2 types.
- All negative error paths (bad magic, bad version, truncated, invalid tag).

### Python (`python/`)

`pytest tests/` covers:

- `test_gen2.py` — encode/decode for all type tags.
- `test_hypothesis.py` — Hypothesis property tests for encode→decode identity.
- `test_gen1.py` — gen1 codec round-trips.

### TypeScript (`typescript/`)

`node --import tsx --test` runs:

| File | What it pins |
|---|---|
| `fixtures_core.test.ts` | Decodes shared `testdata/fixtures/` binaries (gen2 subset) |
| `truth_table.test.ts` | Decoder behaviour against the pinned truth-table manifest |
| `mutation.test.ts` | Decoder never panics on corrupted/truncated input |
| `skip_reserved_tag.test.ts` | Reserved tags (0x30-0x39, 0xF0-0xFF) are skipped or rejected cleanly |
| `gen2.test.ts` | Full gen2 encode/decode round-trips |

---

## Layer 3 — Fuzz testing (`.github/workflows/fuzz.yml`)

The fuzz workflow runs weekly (Sunday 04:00 UTC) and on manual dispatch.

| Target | Tool | Duration |
|---|---|---|
| Go `FuzzMasterStreamReader_Next` | `go test -fuzz` | 5 minutes |
| Go `FuzzDecodeBytes` | `go test -fuzz` | 5 minutes |
| Go `FuzzFastEncode` | `go test -fuzz` | 5 minutes |
| Rust `fuzz_decode` | `cargo fuzz` | 5 minutes |
| Rust `fuzz_roundtrip` | `cargo fuzz` | 5 minutes |
| Python | Hypothesis (`test_hypothesis.py`) | per-run budget |

Crash artifacts are uploaded to GitHub Actions on failure. A crash in any
fuzzer is treated as a P0 bug — it must be reproduced and fixed before the next
release.

---

## Layer 4 — Mutation / truth-table tests

### TypeScript `mutation.test.ts`

Systematically corrupts each byte of a valid fixture and asserts the decoder
either returns an error or produces a value (never panics / throws). This
catches crash-on-corrupt regressions that fuzzing may miss on short inputs.

### Go and TypeScript `truth_table.test.ts` / `truth_table_test.go`

Both languages consume a shared `testdata/truth_cases.json` manifest that
encodes exact (input → expected behaviour) pairs for edge cases: integer
overflow boundaries, BigInt encoding, float precision, and reserved tag
handling. Any change to the manifest is a deliberate spec decision and requires
updating all language tests that consume it.

---

## Adding a new fixture

1. Generate the binary with the Go CLI (oracle):
   ```sh
   echo '<json>' | /tmp/cowrie-cli encode --gen2 > testdata/fixtures/<group>/<name>.cowrie
   /tmp/cowrie-cli decode < testdata/fixtures/<group>/<name>.cowrie > testdata/fixtures/<group>/<name>.json
   ```
2. Add an entry to `testdata/fixtures/manifest.json` matching the schema above.
3. Run the harness to confirm both Go and Python decode correctly:
   ```sh
   GO_CLI=/tmp/cowrie-cli python3 testdata/fixtures/validate_fixtures.py
   ```
4. If the new type is not yet supported in Python gen1, set `"python_skip":
   "<reason>"` on the manifest entry.
5. Add the corresponding case to the TypeScript `fixtures_core.test.ts` and
   Rust `coverage_boost.rs` pin tests.

---

## What is NOT covered here

- **Encoder conformance** — the harness only runs the decode path. Encoder
  correctness is verified by encode→decode roundtrip tests inside each
  language's own suite.
- **Schema fingerprint cross-language parity** — fingerprint values are pinned
  per-language but not yet cross-checked between Go, Rust, and TypeScript in a
  single test. This is a known gap.
- **glyph text format** — the glyph bridge (JSON ↔ `Value`) is tested in Go
  and TS independently. A cross-language glyph fixture suite is not yet built.
