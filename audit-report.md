# Cowrie Codebase Cleanup — Audit Report

**Scope:** Go (`/go/`) + Python (`/python/cowrie/`, `/python/tests/`) only. Rust, C, TypeScript deferred.
**Mode:** Audit-only. Proposed diffs below are NOT applied.
**Baselines (at commit `b2e2b3e`):** Go `go test ./...` all green; Python `pytest` 319 passed.

---

## 0. Pre-step Applied This Session

- Deleted untracked `python/c/` (byte-identical duplicate of `python/csrc/src/`).
- Committed removal of stale `python/dist/*.whl` and `*.tar.gz` artifacts (`python/dist/` already in `.gitignore`).
- Commit: `b2e2b3e chore: remove stale dist/ wheel artifacts from repo`.

---

## Subagent 4: Circular Dependencies

### Critical Assessment
Go graph is acyclic (compiler-enforced), but `cowrie(root)` is a universal dependency sink (in-degree 8). Python has two **hidden cycles** broken only by deferred function-level imports: `gen2 ↔ _fast` and `gen2 ↔ _native`. Four Go packages (`intern`, `dicts`, `ucodec`, `codec`) have zero internal consumers — orphaned or external-only.

### Go DAG
```
cmd/cowrie ──► cowrie(root) ◄── glyph, codec, delta, gnn, graph, ld  (in-deg 8)
cmd/bench  ──► glyph ──► glyph/stream ──► glyph
cmd/glyph  ──► glyph
cmd/cowrie ──► gen1
graph/loader ──► graph ──► cowrie(root), ld ──► cowrie(root)
gnn ──► cowrie(root), graph
gnn/onnx ──► gnn/algo
# Orphans (in-deg 0 internally): intern, dicts, ucodec, codec
```

### High-Confidence Findings

**[C-1] Python hidden cycle: `gen2 ↔ _fast` and `gen2 ↔ _native`**
- `python/cowrie/_fast.py` imports `Value, Type, DType` from `.gen2` inside `_pack_value()` (deferred).
- `python/cowrie/_native.py` defers the same import inside `_python_value_to_c()` and `_c_value_to_python()`.
- `gen2.py` imports `_fast` and `_native` at module level.
- Any refactor that moves either deferred import to module level causes `ImportError` at load.
- **Proposed:** extract shared types to `python/cowrie/_types.py`:
  ```python
  # python/cowrie/_types.py
  from dataclasses import dataclass
  from enum import IntEnum
  class Type(IntEnum): ...
  class DType(IntEnum): ...
  @dataclass
  class Value: ...
  @dataclass
  class TensorData: ...
  ```
  Then `gen2.py`, `_fast.py`, `_native.py` all import from `_types` — cycle eliminated.
- **Confidence:** High (deferred-import pattern is the smoking gun).

### Medium-Confidence Findings

**[C-2] Go root `cowrie` is a universal sink (in-deg 8, out-deg 0).**
- Architectural smell: root package is implicit `commons`. Any change forces rebuild of 8 dependents; no sub-package extractable as standalone module.
- **Proposed:** audit exports of `cowrie(root)`; move package-specific types down; keep only true shared primitives (possibly in a new `cowrie/core` or `cowrie/types`).
- **Confidence:** Medium (refactor scope large; requires careful export audit).

**[C-3] Orphan packages: `intern`, `dicts`, `ucodec`, `codec` have 0 internal importers.**
- Could be external-only (public API), dead, or misplaced.
- `dicts` (dictionary-coding for Gen2 string interning) would logically belong as a dependency of `codec`. If it isn't, check whether `codec` reimplements dictionary logic inline.
- **Proposed:** verify each package's intended consumer; remove if truly dead (coordinate with Subagent 3).
- **Confidence:** Medium (needs cross-check with unused-code audit).

**[C-4] Root external test package `cowrie_test` imports `gen1` — logical inversion.**
- Cross-codec round-trip test in the root's `_test` layer reaches into `gen1`.
- **Proposed:** move cross-codec tests to `go/cmd/` or `go/internal/testutil`.
- **Confidence:** Medium (low urgency, but improves per-package test independence).

**[C-5] `glyph/stream → glyph` — parent-import-from-child risk.**
- Currently clean, but if `glyph` ever needs a stream type, cycle is introduced.
- **Proposed:** document invariant "`glyph` must not back-import `glyph/stream`" in package comment.
- **Confidence:** Medium (preventive).

### Low-Confidence / Deferred
- **[C-6]** `gnn` bypasses `codec` for serialization — verify no duplicated serialization logic (low urgency).

---

## Subagent 1: Duplication/DRY

### Critical Assessment
Most severe: the `_NP_TO_COWRIE_DTYPE` numpy dtype map is defined in **three independent places** across `_native.py`, `_cext.pyx`, `_fast.py` — and `_fast.py`'s inline 7-entry version is narrower than the 12-entry versions elsewhere (latent correctness bug: INT16/UINT16/UINT32/UINT64/BFLOAT16/BOOL dropped on that path). **IMPORTANT clarification** from audit: `_fast.py` opcodes (`_ARRAY=0x10, _OBJECT=0x11`) and `_cext.pyx` tags (`TAG_ARRAY=0x06, TAG_OBJECT=0x07`) are NOT inconsistent — they serve **different protocols** (descriptor-ring vs. direct wire). Merging these would break encoding. Go: `zigzagEncode/Decode`, `readUvarint`, and five security-limit constants are duplicated across `go/`, `go/gen1/`, `go/ld/`, `go/ucodec/` — clean-up via new `go/internal/varint` + `go/internal/limits` packages.

### High-Confidence Findings

**[D-1] Triplicated `_NP_TO_COWRIE_DTYPE` numpy→wire dtype map (with narrowing bug in `_fast.py`).**
- `_native.py:39-52` (12 entries) ≡ `_cext.pyx:55-62` (12 entries) character-for-character.
- `_fast.py:93-97` has inline 7-entry subset — **missing INT16, UINT16, UINT32, UINT64, BFLOAT16, BOOL**. Silent narrowing bug if `_fast.py` is reached with those dtypes.
- Propose `python/cowrie/_wire_constants.py` with single `NP_TO_WIRE_DTYPE` dict. Cython `.pyx` can import Python dicts with zero overhead (no `cdef` constant involved). Hot-path `cdef uint8_t DTYPE_*` constants in `_cext.pyx` **must stay** (compile-inline).

**[D-2] Duplicate `zigzagEncode/zigzagDecode` across Go packages.**
- `go/encode.go:113-115` ≡ `go/ld/encode.go:82-84` (identical body).
- `go/decode.go:423-425` vs `go/ld/decode.go:205-211` — different expressions, same semantics, `ld` has `#nosec` annotation (intentional).
- Propose new `go/internal/varint` package; both packages import. Verify decode equivalence via property test before consolidating.

**[D-3] Duplicate `readUvarint` in `gen1` and `ucodec`.**
- `go/gen1/gen1.go:1318-1327` ≡ `go/ucodec/sparse.go:514-523` (body identical, `ucodec` uses `errors.New(...)` inline while `gen1` uses sentinel errors).
- Propose same `go/internal/varint` destination as D-2.

**[D-4] Triplicated security-limit constants.**
- `go/decode.go:46-54`, `go/gen1/gen1.go:67-71`, `go/ld/decode.go:31` all define `DefaultMaxDepth=1000`, `DefaultMaxArrayLen=1_000_000`, etc.
- Cross-references with finding [T-2]: **`gen1` actually disagrees** on `MaxObjectLen` (10_000_000 vs 1_000_000). Root and `ld` agree; `gen1` is 10× looser.
- Propose `go/internal/limits` package (single source of truth) OR add `TestDefaultLimitsConsistency` cross-package drift test.

**[D-5] `_fast.py` descriptor-opcodes ≠ `_cext.pyx` wire-tags — ADD DOCUMENTATION, DO NOT MERGE.**
- `_fast.py:17-27` (_ARRAY=0x10, _OBJECT=0x11, _BYTES=0x06, _UINT64=0x07) → fed to C `cowrie_encode_from_descriptor`.
- `_cext.pyx:26-40` (TAG_ARRAY=0x06, TAG_OBJECT=0x07, TAG_BYTES=0x08, TAG_UINT64=0x09) → written directly to cowrie wire.
- Values legitimately differ. Add cross-reference comment in each file so future consolidation attempts don't break encoding.

### Medium-Confidence Findings

**[D-6] `DType`-enum-to-wire-code map defined 4× across Python**
- `_native.py:313-322`, `_native.py:418-427`, `_cext.pyx:248-253`, `_fast.py:93-97`. Not all agree. Fold into `_wire_constants.py`.
- Confidence medium because `_cext.pyx:248` is inside a hot `cdef` function; profile import overhead before changing.

**[D-7] Numeric-slice→tensor promotion logic duplicated** — `go/gen1/gen1.go:859-884` (`asFloatSlice`/`asIntSlice`) vs `go/codec/tensor.go:93-122` (`TryEncodeNumericSliceWithOpts`). Serve gen1 vs gen2 value trees — may be intentional divergence. Flag only.

### Low-Confidence / Deferred
- **[D-8]** `cdef uint8_t DTYPE_*` to `.pxd` header — only one `.pyx` consumer; defer.
- **[D-9]** Go `glyph/glyph_test.go` `mustAs*` helpers — standard Go test idiom, no real DRY violation.
- **[D-10]** `mapErrorCode`/`_map_error_code` in Go vs Python fixture tests — cross-language, consolidation impossible; canonical table should live in `testdata/fixtures/manifest.json` (already does).

## Subagent 2: Shared Types Consolidation

### Critical Assessment
Root `go/` package correctly owns canonical wire-format types. Breakdown in three spots: `gen1`, `gnn`, `ucodec` each define their own `DType` enums with **incompatible numeric values** (e.g. `cowrie.DTypeFloat32=0x01` but `gnn.DTypeFloat32=0` and `ucodec.DTypeFloat32=0`). Security-limit constants have silently drifted between root and `gen1` (10× divergence on `MaxObjectLen`). Python `typing.Dict/List` imports are an easy PEP 585 migration — Python floor is 3.9.

### High-Confidence Findings

**[T-1] `ucodec.DType` values diverge from canonical `cowrie.DType`** — `go/ucodec/sparse.go:43` uses iota-based 0–9, while `cowrie.DType` (`go/types.go:219`) uses 0x01–0x14. Nothing structurally justifies this. Propose `type DType = cowrie.DType` alias in `ucodec` + remap the constants. (`gnn.DType` is wire-boundary-justified; keep with conversion function.)

**[T-2] Security-limit constants diverged between root and gen1** — `go/gen1/gen1.go:66-72` sets `MaxObjectLen = 10_000_000`, root sets `1_000_000`. 10× skew is a silent security-posture drift. Propose promoting shared limits to a shared const block.

**[T-3] Graph `Node`/`Edge` types near-identically duplicated** across `go/types.go:473` (NodeData/EdgeData), `go/gen1/gen1.go:160` (Node/Edge), `go/graph/types.go:82` (NodeEvent/EdgeEvent). Propose `type Node = cowrie.NodeData` alias in gen1; `graph` streaming variants are justified-separate (adds Op+Timestamp).

**[T-4] Python: migrate `typing.Dict/List/Optional` → PEP 585 built-ins** — `gen1.py:20`, `gen2.py:24`. `gen2.py` already has `from __future__ import annotations` (line 14). Plan: `gen2.py` safe for `dict`/`list`/`X | None`; `gen1.py` needs `from __future__ import annotations` added first for PEP 604, else only `Dict→dict`/`List→list` (safe on 3.9+).

### Medium-Confidence Findings

**[T-5] `gnn.CompressionType`** at `go/gnn/types.go:76` redefines `None=0,Zstd=1` while `cowrie.Compression` has `None=0,Gzip=1,Zstd=2`. Values incompatible. Propose conversion function rather than full consolidation (wire boundary).

**[T-6] `codec/test_helpers_test.go:14` has `type Compression = cowrie.Compression`** — test-only alias. If `codec` ever exposes compression publicly, move to non-test file.

**[T-7] `_fast.py` / `_cext.pyx` encode full envelope; `_native.py` encodes bare tensor.** Asymmetric semantics not enforced by type. Propose docstring note or `_ENCODES_FULL_FRAME: bool` module constant.

### Low-Confidence / Deferred
- **[T-8]** `NewType('TagByte', int)` in Python — already have `IntEnum`, adds nothing.
- **[T-9]** Aliasing gen1 Node→cowrie.NodeData creates new import edge gen1→root. Architecturally acceptable but prevents future gen1 extraction. Owner decision.
- **[T-10]** Python `Any` usages are all at codec boundaries or property bags — justified, no action.

## Subagent 3: Unused Code Removal

### Critical Assessment
Tree compiles and passes `go vet`. Three entire Go subpackages are orphaned: `intern/`, `ucodec/`, `dicts/` — zero internal importers, zero advertised API references. `_native.py`'s three public functions (`native_encode`, `native_decode`, `native_encode_tensor`) are dead (~130 lines), but `_fast.py` depends on `_native`'s ctypes structs, so only the public API trio can be trimmed — not the file. Experimental `gnn/onnx/` is unreachable from the main codec but actively developed per explainer.html.

### High-Confidence Findings

**[U-1] Delete `go/intern/`** — `pool.go` + `pool_test.go`. Zero internal importers (grepped `cowrie/go/v2/intern`). Not in README or explainer.html. `glyph/pool.go:481-552` implements its own `AutoInterner` (superseded).

**[U-2] Delete `go/ucodec/`** — `delta_pred.go`, `sparse.go` + tests (4 files). Zero internal importers. Live delta/tensor path is in `codec/` and `delta/`, which don't depend on `ucodec`.

**[U-3] Delete `go/dicts/`** — `ml_dict.go` + test. Zero internal importers. Live dict-coding path goes through C (`cowrie_encode_with_dict`) and Go root, not this package.

**[U-4] Delete dead public functions from `python/cowrie/_native.py`** — `native_encode()`, `native_decode()`, `native_encode_tensor()` (~130 lines, approx lines 239–465). Zero callers. Keep the file (ctypes structs + `_lib` loader at lines 1-237 are used by `_fast.py`).

### Medium-Confidence Findings

**[U-5] `go/gnn/onnx/` ONNX-enabled variants are unreachable from main codec** but are active development (explainer.html + worklog). Keep, but add `// NOTE: experimental, not wired into main codec` doc comment to `gnn/onnx/types.go`.

**[U-6] `go/cmd/bench/` + `go/cmd/generate_golden/`** — dev tools, not in CI (only `cmd/cowrie` is). Keep; flag for Makefile entry so they don't rot silently.

**[U-7] `_fast.py` has no `fast_decode` counterpart** — asymmetric dispatch tier (encode uses C, decode falls back to pure Python when `_cext.so` unavailable). Not dead code but a maintenance hazard; add `# NOTE: no fast decode in this tier` comment.

### Low-Confidence / Deferred
- **[U-8]** `_cext.c` committed alongside `_cext.pyx` — intentional for sdist. No CI step enforces regeneration when `.pyx` changes.
- **[U-9]** Python test files show no redundancy; six files cover distinct concerns.

### Cross-references
- [U-1/U-2/U-3] confirm orphan finding [C-3] from Subagent 4.

## Subagent 5: Weak Type Elimination

### Critical Assessment
**Python floor is 3.9** (per pyproject.toml) → PEP 585 built-ins (`list[T]`, `dict[K,V]`) usable but PEP 604 `|` unions NOT (need 3.10). Three tiers of `any`/`Any`: (1) justified codec boundary (encode/decode, Value.data, FromAny/ToAny — keep), (2) graph property bags `map[string]any` (inherent to prop-bag design — keep), (3) unnecessary internal any that can be tightened (several concrete wins).

### High-Confidence Findings

**[W-1] Go: `readObject` returns `any` but every caller asserts `map[string]any`.** `go/gen1/gen1.go:1533` + 7 call sites (1780, 1837, 1901, 2054, 2104, 2400). Change return type to `map[string]any`. Eliminates 7 panic opportunities.

**[W-2] Go: `interface{}` → `any` mechanical modernisation** in `go/glyph/json_bridge.go`, `schema_evolution.go`, `schema.go`, `stream_validator.go`, `loose.go`. All 66 `interface{}` occurrences in codebase concentrated here. Zero-semantic-change Go 1.18 alias.

**[W-3] Python: `Dict[...]` / `List[...]` → `dict[...]` / `list[...]`** in `gen1.py:20`, `gen2.py:24` and throughout (~15 occurrences). Safe on 3.9+. `gen2.py` already has `from __future__ import annotations`; `gen1.py` doesn't (add if also converting `Optional[X]` → `X | None`, otherwise skip that).

**[W-4] Python: `_get_case(case_id) -> dict` should be `TypedDict`** at `python/tests/test_truth_table.py:207`. Test uses fixed keys `case["id"]`, `case["action"]`, `case["input"]`. Define `TruthCase(TypedDict, total=False)`.

### Medium-Confidence Findings

**[W-5] `go/glyph/schema.go:137 Constraint.Value interface{}`** is a discriminated union by `Kind`. Minimal fix: rename `interface{}` → `any`. Larger fix (sealed interface or `Constraint[T]`) requires redesign — deferred.

**[W-6] Go `valueToAny` (`decode.go:1393`) / `ToGoAny` / `ToAny` — three variants.** Not a type-weakness but a clarity issue — document which to use when, or consolidate.

**[W-7] Python tag constants (`gen1.py:23-51`) bare `int`.** `NewType('TagByte', int)` adds static-typing safety with zero runtime cost. Medium value.

### Low-Confidence / Deferred
- **[W-8]** `stream_validator.go:156 parsedFields map[string]interface{}` — 5-case bounded set (`bool`, `int64`, `float64`, `string`, `nil`). Sealed interface viable but requires touching validateField+toFloat64. Defer unless panics observed.
- **[W-9]** Generic `toFloat64[T numeric]` in `glyph/stream_validator.go:581` needs `x/exp/constraints` dependency.

### Justified `any`/`Any` (keep — document why)
- `sync.Pool.New() any` in `gen1.go:132` — forced by Go stdlib API.
- `encodeAny`, `collectKeysFromAnyValue`, `readArrayGeneric` internal recursive walkers over prop bags.
- Python `Value.data: Any` — tagged union with ~22 concrete types discriminated by `Value.type`.
- All codec boundary functions.

### Remaining Weak Types Requiring Product Decision
- **Graph prop-bag type parity:** Go uses `map[string]any`; Python uses `Dict[str, Value]`. **Implementations inconsistent.** Choose: Go moves to `map[string]*Value` (typed, API break) or Python relaxes to `Dict[str, Any]` (loose, matches Go). Cross-language codec parity argument.
- **Constraint.Value** — discriminated union vs. generic.

## Subagent 6: Defensive Error Handling Cleanup

### Critical Assessment
Codebase is broadly well-disciplined — **all codec-invariant enforcement intact**. Three high-severity issues: (1) a silent data-corruption path in prod Python (`gen2.py:1832`), (2) six mutation-test handlers that swallow all exceptions, defeating the mutation-test suite's purpose, (3) a `var err error + _ = err` suppressor in Go that hides a latent bug. All 8 Go `recover()` calls and ~53 `panic()` calls are justified (programmer-error contracts, init() failures, boundary-testing recovers).

### High-Confidence Findings

**[E-1] `python/cowrie/gen2.py:1828-1833` — silent data-corruption path.**
```python
try:
    ext_type = int(v.get("ext_type"))
    payload = base64.b64decode(v.get("payload"))
    return Value.unknown_ext(ext_type, payload)
except Exception:
    pass  # falls through to re-encode as plain object — SILENT CORRUPTION
```
If `Value.unknown_ext` raises an internal codec error, it's swallowed and the dict is re-encoded as plain object. **Narrow to `except (ValueError, TypeError, binascii.Error):`** (intended cases from `int()` and `base64.b64decode()`).

**[E-2] `python/tests/test_mutation.py` — 6× `except Exception: pass`** at lines 47, 62, 78, 93, 99, 105. All of the form "decode truncated/mutated bytes; swallow any error." Undermines TESTING_STANDARDS.md rule 4 ("wrong error type passing silently is worse than no test"). Must narrow to `(cowrie.DecodeError, struct.error, ValueError, OverflowError)` so `MemoryError`/`RecursionError`/unexpected `AttributeError` propagate as test failures.

**[E-3] `python/tests/test_gen1.py:338-341` — same anti-pattern, unit-test version.**
```python
try: decode(data)
except Exception: pass
```
No assertion that an error actually raised — test passes even if decoder silently succeeds on truncated data. **Use `with pytest.raises((ValueError, cowrie.DecodeError, struct.error)):`**.

### Medium-Confidence Findings

**[E-4] `go/gen1/gen1.go:2072 + 2336` — dead `var err error` + `_ = err` suppressor.**
Outer `var err error` at 2072 never assigned via `=`; all inner uses are `:=` (new scope). Line 2336 `_ = err` silences the resulting "declared and not used" error. Antipattern — if a future developer adds `if err != nil` against the outer variable, it never triggers. Remove both lines; function compiles cleanly.

**[E-5] Test helpers using `panic(err)` should use `t.Fatal`** — `go/glyph/decimal128_test.go:281-284` (`mustParseDecimal`), `go/codec/test_helpers_test.go:28-33` (`MustEncodeBytes`). Panic aborts entire test binary vs `t.Fatal` which fails current test gracefully. Low severity, convention issue.

### Low-Confidence / Deferred — Justified handlers (KEEP)

- All 8 `recover()` calls: `codec/fuzz_test.go:210,234` (boundary-protection asserts), `codec/unmarshal_test.go:82,103,124` (typed-target panic safety), `accessor_coverage_test.go:132` + `dicts/ml_dict_test.go:365` + `glyph/coverage_boost_test.go:40` (programmer-error contract tests).
- All `go/types.go:572-1042` typed-accessor panics (`Bool()` on non-bool, etc.) — documented programmer-error contracts, stdlib pattern.
- `go/compress.go:198`, `codec/compress.go:27,31` — `init()` panics on zstd construction. Correct: process must not start silently broken.
- `python/tests/test_hypothesis.py:63,151,166` — `except Exception: pass` correctly scoped. Property being tested is "decoder is robust / never crashes" — broad handler is the correct formulation for this specific property. Correctness (right error type) is tested elsewhere.
- `gnn/algo/louvain.go:222 communities, _ = renumberCommunities(...)` — discarded value is `int` count, not error. Fine.

## Subagent 7: Legacy/Deprecated/Fallback

### Critical Assessment
Four categories found: (1) active wire-format compat layers (gen1, GNN v1.0, master stream legacy reads) — **all KEEP**; (2) deprecated public API shims with only test callers — deprecation-cycle removals; (3) one stale no-op CLI flag (`--auto-tabular`); (4) several stale comments. No gen3 scaffold found. No permanently-false feature flags. Python three-tier extension loading is real tested fallback — KEEP.

### High-Confidence Findings

**[L-1] `go/glyph/loose.go:95 CanonicalizeLooseTabular`** — exported `// Deprecated` wrapper, body is `return CanonicalizeLoose(v)`. Only 12 call-sites, all in `loose_test.go`. Propose removal after one release cycle. Medium urgency (public API break).

**[L-2] `go/glyph/loose.go:387 TabularLooseCanonOpts`** — same pattern, tests don't even reference it directly. Propose removal after one release cycle.

**[L-3] `go/glyph/loose.go:122 base64Encode`** — `// Deprecated` comment is STALE; function is still called at `go/glyph/auto_pool.go:276`. Inline the one-liner at call site and delete wrapper. **Internal-only, high priority, low risk.**

**[L-4] `go/cmd/glyph/main.go:91 --auto-tabular` case** — genuine dead branch (flag silently ignored). Options: (a) keep parsing but `fmt.Fprintf(os.Stderr, "--auto-tabular is now default\n")` deprecation warning, (b) remove entirely (silently breaks user scripts). Recommend (a).

**[L-5] `go/codec/compat_test.go:18 TestReader_MixedStream_BackCompat`** — permanently `t.Skip(...)` with stale comment "requires complex frame detection logic" — but the underlying `readLegacyDocument`/`readLegacyStream` IS implemented (`cowrie_master_stream.go:411-469`) and tested elsewhere (19 non-skipped refs). Either remove the skip and verify, or delete the test file.

### Medium-Confidence Findings (Stale comments / Keep-with-note)

**[L-6] `go/codec/json.go:9`** — "default codec for backward compatibility" — stale; `JSONCodec` is load-bearing current default. Remove the phrase.

**[L-7] `go/gen1/gen1.go:4` + `:612`** — "legacy" language in package doc and "backward compatible" annotation are stale. gen1 is an active supported codec. Reword.

### Low-Confidence / Deferred — KEEP
- **[L-KEEP-1]** `go/codec/cowrie_master_stream.go` `AllowLegacy=true` — real parse path for existing data.
- **[L-KEEP-2]** `go/gnn/container.go` v1.0 decoder — existing GNN datasets may be v1.0.
- **[L-KEEP-3]** `go/gnn/graphrag.go` `blendResultsAlpha` + `FusionAlpha` default — active code path.
- **[L-KEEP-4]** `go/gnn/onnx/*_stub.go` — build-tag gated fallback stubs, intentional.
- **[L-KEEP-5]** Python `COWRIE_PUREPYTHON` + three-tier `_cext` / `_fast.py` / pure-Python fallback — tested intentional deployment model.
- **[L-KEEP-6]** `python/cowrie/_native.py` — imported by `_fast.py` as second-tier bridge. Overlaps with [U-4] (remove dead public functions but keep ctypes infrastructure).

### No gen3/pre-gen1 scaffolds found — clean baseline.

## Subagent 8: AI Slop / Stubs / Larp / Comments

### Critical Assessment
Significant volume of **assertion-free "tests"** that log marketing claims (emoji bullet dashboards, hardcoded "savings" percentages, fake `"(measured)"` strings) and always pass. Concentrated in `go/glyph/savings_benchmark_test.go` and `go/gnn/benchmark_comparison_test.go`. Regressions would be invisible. Also two ONNX stub tests that self-admit they "pass either way." Spacer-comment abuse in `go/v3_test.go` and `go/coverage_boost2_test.go` (~38 `// ===` dividers).

### False Positives (DO NOT TOUCH)
- **`XXXX` hex test vectors** — 10 hits across `go/codec/safety_test.go:171`, `go/glyph/canon.go:232`, `go/glyph/parse_packed.go:59`, etc. — legitimate 4-hex-char test fixtures.
- **`gat_stub.go`/`gcn_stub.go`** — `//go:build !onnx` build-tag stubs with real `FallbackClassify`/`FallbackGATClassify` algorithmic implementations (PageRank-weighted). KEEP.

### High-Confidence Findings

**[S-1] `go/glyph/savings_benchmark_test.go:10-41 TestFullSavingsSummary`** — entire body is hardcoded `t.Log` strings printing a "95% savings" marketing table. Zero computation, zero assertions. **Delete.** `TestComprehensiveSavings` (same file, line 44) does the real work.

**[S-2] `go/gnn/benchmark_comparison_test.go:965-1028 TestMLImpact`** — writes `"~386ms (measured)"` but measures nothing; strings are literal. Emoji bullet dashboard. **Delete.** If summary wanted, move to `cmd/bench` binary.

**[S-3] `go/gnn/onnx/gcn_test.go:82-90 TestIsONNXEnabled`** — self-documented `// This test passes either way, just logs the state`. **Delete.**

**[S-4] `go/gnn/onnx/gat_test.go:36-41 TestIsGATEnabled`** — same pattern, **delete**.

**[S-5] `go/gnn/benchmark_comparison_test.go TestSizeComparison_*`** (lines 265-320) + `TestOptimizationStrategies` (481+) + `TestCompressionBattle` (326) — compute real sizes but only `t.Logf` ratios. A regression where GNN is 10× larger passes silently. Either add ratio-floor assertions (e.g., `if float64(jsonLen)/float64(gnnLen) < 1.5 { t.Errorf(...) }`) or delete strategy-exploration tests.

**[S-6] `go/glyph/savings_benchmark_test.go:321-350 TestStreamingDictSavings`** — real measurement, no assertion. Add `if s.Dict().Len() == 0` check + savings floor.

**[S-7] Spacer comment abuse** — `go/v3_test.go` has 22 instances of `// ===...===` triple-dividers around individual test functions; `go/coverage_boost2_test.go` has 16. Keep one-line label, drop outer `===`.

**[S-8] Decorative `t.Log("✅ ...")`** in `go/gnn/benchmark_comparison_test.go:686-729 TestCompressionAPI` closing — remove. Test output should be silent on success.

**[S-9] Section-banner `===` comments** in `go/gnn/benchmark_comparison_test.go` (6 instances) — same pattern as [S-7].

### Medium-Confidence Findings

**[S-10] Add sanity assertions** to `go/codec/bench_test.go:200-249`, `go/gen2_test.go:377-402`, `go/gnn/gnn_test.go:472-520` — all compute real data, log only. Low-cost addition of `if cowrieSize >= jsonSize { t.Errorf(...) }`.

**[S-11] `go/codec/unmarshal_test.go:392`** `// NOTE:` prefix non-standard Go comment — rewrite as plain `//`.

### Low-Confidence / Deferred
- **[S-12]** Python `gen2.py:1-12` module docstring duplicates `go/doc.go` feature list. Could drift. Monitor only.
- **[S-13]** `go/glyph/loose.go:74-82` version annotation `// ... (v2.3.0+)` — borderline; explains observable behavior change for callers. KEEP.

---

## Cross-Cutting Themes

### Overlapping findings across subagents
- **[C-3/U-1/U-2/U-3]** — `go/intern/`, `go/ucodec/`, `go/dicts/` orphaning found by both Subagent 3 (unused) and Subagent 4 (circular). Same action: delete 8 files total. High confidence.
- **[T-2/D-4]** — security-limit constants duplicated AND drifted between `go/`, `go/gen1/`, `go/ld/`. Subagent 1 (DRY) and Subagent 2 (types) both flagged. Solution: new `go/internal/limits` package OR cross-package drift test.
- **[D-2/D-3/D-4]** — `zigzagEncode/Decode`, `readUvarint`, security limits all want to live in a shared `go/internal/` package. Propose single new package `go/internal/` with subpackages `varint`, `limits`.
- **[C-1/T-3/D-1/U-4]** — Python `_native.py` / `_fast.py` / `_cext.pyx` — circular deps (C-1), type inconsistency, dtype map duplication (D-1), and dead public functions (U-4) all point to the same refactor: extract `_types.py` + `_wire_constants.py`, delete 3 dead public functions from `_native.py`.
- **[L-KEEP-5/L-KEEP-6/U-4/M-3]** — `_native.py` status: not fully dead (backs `_fast.py`); but its public API is dead. Single coherent action: trim public API, keep infra.

---

## Ordered Apply List (recommended for follow-up session)

Ranked by confidence × impact × risk-freeness. Each item is atomic and independently commit-able.

### Tier A — High Confidence, Zero Risk (do first)
1. **Delete `go/intern/`** (2 files) — [U-1]
2. **Delete `go/ucodec/`** (4 files) — [U-2]
3. **Delete `go/dicts/`** (2 files) — [U-3]
4. **Delete `go/glyph/savings_benchmark_test.go::TestFullSavingsSummary`** — [S-1]
5. **Delete `go/gnn/benchmark_comparison_test.go::TestMLImpact`** — [S-2]
6. **Delete `go/gnn/onnx/gcn_test.go::TestIsONNXEnabled`, `gat_test.go::TestIsGATEnabled`** — [S-3/S-4]
7. **Mechanical `interface{}` → `any`** across `go/glyph/` non-test files — [W-2]
8. **Python `Dict[...]`/`List[...]` → `dict[...]`/`list[...]`** in `gen1.py`, `gen2.py` — [W-3]
9. **Strip `===` spacer comments** in `go/v3_test.go`, `coverage_boost2_test.go`, `go/gnn/benchmark_comparison_test.go` — [S-7/S-9]
10. **Narrow `except Exception` at `python/cowrie/gen2.py:1832`** to `(ValueError, TypeError, binascii.Error)` — [E-1]
11. **Narrow 6× `except Exception: pass` in `python/tests/test_mutation.py`** to named exception set — [E-2]
12. **Replace `except Exception: pass` at `python/tests/test_gen1.py:340`** with `pytest.raises` — [E-3]
13. **Remove `var err error + _ = err` suppressor at `go/gen1/gen1.go:2072+2336`** — [E-4]
14. **Inline `base64Encode` one-liner at `go/glyph/auto_pool.go:276`; delete wrapper** — [L-3]
15. **Delete dead public functions from `python/cowrie/_native.py`** (`native_encode`, `native_decode`, `native_encode_tensor` — ~130 LOC) — [U-4]

### Tier B — High Confidence, Refactor (do next)
16. **Create `python/cowrie/_types.py`** with `Value, Type, DType, TensorData` shared dataclasses — eliminates hidden cycles [C-1].
17. **Create `python/cowrie/_wire_constants.py`** with `NP_TO_WIRE_DTYPE` and `DTYPE_ENUM_TO_WIRE` maps. Fixes `_fast.py` dtype-narrowing bug [D-1].
18. **Create `go/internal/varint`** package with `ZigzagEncode`/`ZigzagDecode`/`ReadUvarint`; wire up `go/`, `go/ld/`, `go/gen1/` — [D-2/D-3]
19. **Create `go/internal/limits`** package OR add `TestDefaultLimitsConsistency` cross-package drift test — fix 10× divergence on `MaxObjectLen` — [T-2/D-4]
20. **Change `readObject` return type** from `any` to `map[string]any` at `go/gen1/gen1.go:1533` — [W-1]
21. **Add sanity assertions** to 3 `TestSizeComparison` variants — [S-10/S-5]
22. **Rename `ucodec.DType` values** to match canonical `cowrie.DType` (coordinate with U-2 deletion — if ucodec is deleted, finding moot) — [T-1]

### Tier C — Medium Confidence (needs owner decision)
23. **`go/glyph/loose.go:95,387` deprecated public API** (`CanonicalizeLooseTabular`, `TabularLooseCanonOpts`) — remove after one release cycle — [L-1/L-2]
24. **`--auto-tabular` CLI flag at `go/cmd/glyph/main.go:91`** — add stderr deprecation warning — [L-4]
25. **`go/codec/compat_test.go:18`** — remove `t.Skip`, complete the test, or delete file — [L-5]
26. **Stale "legacy"/"backward compat" comments** — reword `go/gen1/gen1.go:4,612`, `go/codec/json.go:9` — [L-6/L-7]
27. **`Constraint.Value interface{}`** discriminated union — rename to `any` (minimal); larger typed design deferred — [W-5]
28. **`_fast.py` asymmetric no-decode tier** — add `# NOTE: no fast decode in this tier` — [U-7]
29. **Add `// NOTE: experimental, not wired into main codec` to `go/gnn/onnx/types.go`** — [U-5]

### Tier D — Product Decisions (do not implement blindly)
30. **Go `map[string]any` vs Python `Dict[str, Value]`** graph prop-bag parity — cross-language API decision.
31. **`cowrie(root)` in-degree 8** — refactor to `cowrie/types` or `cowrie/core`; requires careful export audit — [C-2]
32. **Move cross-codec tests out of root `_test`** — [C-4]
33. **NewType('TagByte', int)**, TypedDict adoption for truth-table cases — [W-4/W-7]

### Tier E — Monitor / Preventive (document invariants, don't refactor)
- **`glyph/stream → glyph`** back-import risk — add invariant comment to `glyph` doc.
- **`_cext.c` regeneration** — add CI check that it's in sync with `_cext.pyx`.
- **Python module docstring** potential drift from `go/doc.go` — monitor.
- **`go/cmd/bench`, `go/cmd/generate_golden`** — add to Makefile/docs to prevent silent rot.

---

## Validation Plan for Follow-up Session

**After each Tier A item, run:**
```
cd go && go test ./... && go vet ./...
cd python && pytest -q
```
Both must stay green. If any test fails, stop and investigate.

**Before Tier B items:**
- Run `go build ./...` after package extractions to confirm import paths.
- For Python `_types.py` extraction: check for circular imports via `python -c "import cowrie.gen2"`.

**Cross-language parity check** (Tier B and beyond):
- For any change touching encode/decode paths, verify cross-language golden fixtures at `testdata/fixtures/` still decode identically in Go and Python.

**Estimated source-file count for full apply:**
- Tier A: ~20 files modified/deleted, 8 files deleted outright.
- Tier B: ~12 files modified, 2–3 new files created.
- Tier C: ~8 files modified.
- Totals: ~40 files touched, ~10 files deleted, ~3 files created.

---

## Remaining Medium/Low-Confidence Items Not Implemented (Deferred)

See Tier C + D + E above. Notable deferrals:
- No type-system redesign of `go/glyph/schema.go Constraint.Value`.
- No Go root-package refactor.
- No cross-language prop-bag parity decision.
- No NewType/TypedDict adoption in Python — low marginal value without mypy CI.
- No removal of experimental `gnn/onnx/` subtree — still in active development per `explainer.html`.

