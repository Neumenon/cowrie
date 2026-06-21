# Cowrie Final Sprint Plan (Atomic, Testable Tickets)

Each sprint ends with a demoable, runnable, and testable increment. Every ticket is atomic and commit-able with explicit validation.

## Sprint 0 — Spec + Fixture Contract
**Goal:** Lock shared rules (limits, framing, TagExt, hints, rank) and tooling to validate spec + fixtures.
**Demo:** Run spec/fixture validators and see "OK".

Tickets:
- S0-1 Add `tools/spec/verify_sections.py` to assert required SPEC.md sections exist (TagExt, column hints, limits table, framing error behavior, max rank).
  Validation: `python tools/spec/verify_sections.py`
- S0-2 Update `SPEC.md` with the required sections + a canonical error-code table (e.g., `ERR_INVALID_MAGIC`, `ERR_TOO_LARGE`, `ERR_UNSUPPORTED_COMPRESSION`).
  Validation: `python tools/spec/verify_sections.py`
- S0-3 Define fixture manifest schema in `testdata/fixtures/manifest.json` (cases list, expected result or error code).
  Validation: `python tools/fixtures/validate_manifest.py`
- S0-4 Add `tools/fixtures/validate_manifest.py` to validate schema + allowed error codes.
  Validation: `python tools/fixtures/validate_manifest.py`

## Sprint 1 — Core Fixtures + Harness
**Goal:** Cross-language decode/encode parity for core types (null/bool/int/float/string/bytes/array/object).
**Demo:** Run fixture tests in all languages; all pass for core fixtures.

Tickets:
- S1-1 Create core fixtures in `testdata/fixtures/core/` (JSON + Cowrie bytes + manifest entries).
  Validation: `python tools/fixtures/validate_manifest.py`
- S1-2 Go fixture runner for core cases in `go/gen2/fixture_test.go`.
  Validation: `go test ./go/gen2 -run TestFixturesCore`
- S1-3 Rust fixture runner for core cases in `rust/tests/fixtures_core.rs`.
  Validation: `cargo test -p cowrie fixtures_core`
- S1-4 Python pytest runner for core cases in `python/tests/test_fixtures_core.py`.
  Validation: `pytest python/tests/test_fixtures_core.py`
- S1-5 TS Jest runner for core cases in `typescript/src/gen2/fixtures_core.test.ts`.
  Validation: `npm test -- gen2 fixtures_core`
- S1-6 C ctest runner for core cases (new test binary).
  Validation: `ctest -R fixtures_core`

## Sprint 2 — ML + Graph Parity + Tensor Rank
**Goal:** Parity for ML/graph types and rank encoding (u8) across all languages.
**Demo:** Run ML/graph fixtures; all pass.

Tickets:
- S2-1 Add ML fixtures (tensor/tensor_ref/image/audio/adjlist/richtext/delta).
  Validation: `python tools/fixtures/validate_manifest.py`
- S2-2 Add graph fixtures (node/edge/batches/graph_shard).
  Validation: `python tools/fixtures/validate_manifest.py`
- S2-3 Python: change tensor rank encoding/decoding to **u8**; reject rank >255 (or spec max).
  Validation: `pytest python/tests/test_tensor_rank.py`
- S2-4 TS: change tensor rank encoding/decoding to **u8**; reject rank >255 (or spec max).
  Validation: `npm test -- gen2 tensor_rank`
- S2-5 Go/Rust/C: add rank boundary tests (0, 1, max, max+1) and map to canonical error codes.
  Validation: language-specific tests.
- S2-6 Rust: ensure encode/decode covers ML + graph tags (if gaps exist).
  Validation: `cargo test -p cowrie fixtures_ml_graph`
- S2-7 Python: ensure encode/decode covers ML + graph tags (if gaps exist).
  Validation: `pytest python/tests/test_fixtures_ml_graph.py`
- S2-8 TS: ensure encode/decode covers ML + graph tags (if gaps exist).
  Validation: `npm test -- gen2 fixtures_ml_graph`
- S2-9 C: ensure encode/decode covers ML + graph tags (if gaps exist).
  Validation: `ctest -R fixtures_ml_graph`

## Sprint 3 — Limits Parity + Options
**Goal:** Uniform limits and predictable errors for oversized inputs.
**Demo:** Run limit-negative fixtures; all languages reject with canonical error codes.

Tickets:
- S3-1 Add limit-negative fixtures (bytes, bigint, string, array, object, dict, rank, ext).
  Validation: `python tools/fixtures/validate_manifest.py`
- S3-2 Go: add canonical error-mapping helper for fixture tests (limit errors -> codes).
  Validation: `go test ./go/gen2 -run TestFixturesLimits`
- S3-3 Rust: add `DecodeOptions` with defaults matching Go; enforce per-type limits.
  Validation: `cargo test -p cowrie fixtures_limits`
- S3-4 Python: add `MAX_DICT_LEN`, `MAX_EXT_LEN`, `MAX_RANK`; enforce BIGINT length; optional `DecodeOptions`.
  Validation: `pytest python/tests/test_limits.py`
- S3-5 TS: enforce limits for BIGINT, dict length, rank, ext; add `DecodeOptions`.
  Validation: `npm test -- gen2 limits`
- S3-6 C: enforce `max_bytes_len` for BYTES/BIGINT/TENSOR/IMAGE/AUDIO; add `max_dict_len`.
  Validation: `ctest -R limits`

## Sprint 4 — Column Hints
**Goal:** Correct handling when `FlagHasColumnHints` is set (skip or parse per spec).
**Demo:** Decode a hinted file and verify dictionary/root parse correctly.

Tickets:
- S4-1 Add hinted fixtures (valid + malformed hints).
  Validation: `python tools/fixtures/validate_manifest.py`
- S4-2 Python: implement `skip_hints` (or parse hints) + tests.
  Validation: `pytest python/tests/test_hints.py`
- S4-3 TS: implement `skip_hints` + tests.
  Validation: `npm test -- gen2 hints`
- S4-4 Rust: implement `skip_hints` + tests.
  Validation: `cargo test -p cowrie hints`
- S4-5 C: implement `skip_hints` **or** explicit error code if unsupported.
  Validation: `ctest -R hints`

## Sprint 5 — TagExt Forward Compatibility
**Goal:** Unknown extensions are handled consistently (keep/skip/error) across languages.
**Demo:** Unknown TagExt round-trips without data loss in "keep" mode.

Tickets:
- S5-1 Add TagExt fixtures (keep/skip/error) with canonical error codes.
  Validation: `python tools/fixtures/validate_manifest.py`
- S5-2 Python: add Tag.EXT + ExtData value, policy handling, encode/decode.
  Validation: `pytest python/tests/test_tagext.py`
- S5-3 TS: add Tag.EXT + ExtValue, policy handling, encode/decode.
  Validation: `npm test -- gen2 tagext`
- S5-4 C: add `COWRIE_EXT` type + encode/decode/free.
  Validation: `ctest -R tagext`
- S5-5 Rust: map TagExt errors to canonical codes (if needed).
  Validation: `cargo test -p cowrie tagext`

## Sprint 6 — Compression Framing Safety (All Languages)
**Goal:** Safe framed decoding with size checks and consistent error handling.
**Demo:** Framed decode rejects bombs; framed roundtrip works.

Tickets:
- S6-1 Add framed fixtures (good + bad: orig_len mismatch, truncated header, unsupported codec).
  Validation: `python tools/fixtures/validate_manifest.py`
- S6-2 Go: cap decompression via `io.LimitedReader`; verify `len == orig_len`; map errors.
  Validation: `go test ./go/gen2 -run TestFramedSafety`
- S6-3 Rust: size-limited decompress; verify `len == orig_len`; map errors.
  Validation: `cargo test -p cowrie framed`
- S6-4 Python: `decode_framed(data, max_size)` + size check + tests.
  Validation: `pytest python/tests/test_framed.py`
- S6-5 TS: `decodeFramed(data, maxSize)` + size check + tests.
  Validation: `npm test -- gen2 framed`
- S6-6 C: implement `cowrie_decode_framed` with size limit + tests.
  Validation: `ctest -R framed_decode`

## Sprint 7 — C Framed Encode
**Goal:** C has full framed encode/decode parity with optional gzip/zstd.
**Demo:** C framed roundtrip via ctest.

Tickets:
- S7-1 Implement `cowrie_encode_framed` with gzip/zstd optional builds + raw fallback.
  Validation: `ctest -R framed_encode`
- S7-2 Add build-flag docs and tests for missing codecs.
  Validation: `ctest -R framed_codec_fallback`

## Sprint 8 — Fuzzing + CI Runner
**Goal:** Continuous regression coverage for decoders.
**Demo:** Run fuzzers briefly and a single `run_all` script for fixtures.

Tickets:
- S8-1 Add Go fuzz target for Gen2 decode seeded with fixtures.
  Validation: `go test ./go/gen2 -run Fuzz -fuzztime=5s`
- S8-2 Add Rust fuzz target (cargo-fuzz) seeded with fixtures.
  Validation: `cargo fuzz run decode -- -runs=1000`
- S8-3 Add Python Hypothesis or TS fast-check roundtrip property test (one minimal target).
  Validation: `pytest python/tests/test_property.py` **or** `npm test -- gen2 property`
- S8-4 Add `tools/ci/run_all.sh` to execute fixture tests across languages.
  Validation: `bash tools/ci/run_all.sh`
