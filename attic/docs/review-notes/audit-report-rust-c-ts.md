# Cowrie Audit Report — Rust, C, TypeScript

> ⚠️ **SUPERSEDED / PARTIALLY STALE (2026-06-19).** All Tier A items in this
> report were applied before it was committed. The schema-fingerprint ordinal
> bug it documents was fixed in commit `3855080` and no longer exists (its
> evidence cited the *correct* current values as the bug). Tier B items B3–B6
> may still be actionable. See `CUTLIST.md` for the current decision set.

**Mode:** Audit-only (no code changes applied).
**Scope:** `rust/`, `c/`, `typescript/`. Go + Python already cleaned in prior session (commits 302966c..5d3ca3f).
**Source:** 8 parallel subagent reports (DRY, types, unused, cycles, weak-types, defensive-errors, legacy, AI-slop).

---

## TL;DR — Cross-Language Bug Surface

One real cross-impl correctness bug, three latent ones, and the usual hygiene:

1. **Schema fingerprint `typeToOrd` is inconsistent across Rust / TS / C.** Fingerprints for `TensorRef`, `Image`, `Audio` hash differently in each language. Any fingerprint-based parity check will miss these three types.
   - Rust `gen2/schema.rs:43-72` → TensorRef=14, Image=15, Audio=16
   - TS `gen2/index.ts:2224-2237` → Image=14, Audio=15, TensorRef=16
   - C `gen2.c:3064` → TENSOR_REF=15, IMAGE=16, AUDIO=17
2. **Rust `mod tags` duplicated between encode + decode** with `FLOAT32 = 0x0F` present in decode only. Silent future drift risk.
3. **C `dict_add` double-realloc leak** (`gen2.c:973-976`) — second `realloc` overwrites first on failure.
4. **C JSON bridge missing 5 quantized dtypes** (`json.c:413-424`) — qint2/3/4, ternary, binary silently unsupported.
5. **TS gen1 security limits are 100× looser than gen2 within the same language** (100M array / 10M object / 500MB string vs. 1M / 1M / 10MB). No drift test guards this.

Everything else is dead code, stale aliases, orphan crates, and TypeScript `any`.

---

## Tier A — Zero-Risk (apply first)

These are internal-only, zero external callers, or equivalence-preserving. Each has file:line evidence.

### A1. Fix schema fingerprint ordinal mismatch — **CRITICAL**
- `rust/src/gen2/schema.rs:43-72`
- `typescript/src/gen2/index.ts:2224-2237`
- `c/src/gen2.c:3064` (and wherever the C helper lives)

Unify the ordering. Recommend canonical order from SPEC.md; if none, freeze whichever order Go uses (since Go is primary and already tested in prior session). Add a cross-lang parity test that computes the fingerprint for a reference schema and asserts equality across impls.

### A2. Deduplicate Rust `mod tags`
- `rust/src/gen2/encode.rs:8-47` + `rust/src/gen2/decode.rs:8-48`

Move `mod tags` to a shared `gen2/tags.rs`, re-export from both. Add missing `FLOAT32 = 0x0F` to the consolidated copy.

### A3. Fix C `dict_add` double-realloc leak
- `c/src/gen2.c:973-976`

Classic pattern: `p = realloc(p, ...)` without capturing failure. Use a temporary, check, then assign.

### A4. C JSON bridge — add 5 missing quantized dtypes
- `c/src/json.c:413-424`

Add qint2, qint3, qint4, ternary, binary handlers. Currently they silently fall through (or error?).

### A5. Remove Rust `gen1`/`gen2` feature flags (dead declarations)
- `rust/Cargo.toml:24-25`

Neither flag gates any `#[cfg]` in the tree. Modules unconditionally compile. Delete the two lines.

### A6. Remove TS `readLegacyDocument` + `readLegacyStream`
- `typescript/src/gen2/index.ts:2627-2675` (~48 LOC) + fallback branch at 2557-2563

Module-private, zero external callers. Master-stream v2.0.0 is initial public release — no legacy producers exist. Make `readMasterFrame` throw `InvalidMagic` when `SJST` magic is absent.

### A7. C broken tool binary — `decode_stdin.c`
- `c/tools/decode_stdin.c:26,64`

Wrong-arity call + `feof()` anti-pattern (classic off-by-one in read loops). Either fix or delete; this is build-broken and not exercised by CTest.

### A8. TS `Value.data: unknown` → discriminated union
- `typescript/src/gen2/index.ts:319`

Pure type tightening. The runtime shape is already discriminated by `tag`; TypeScript just doesn't know. Low-churn, strong payoff.

### A9. Remove TS non-null assertions `!`
- `typescript/src/gen2/index.ts:739, 938`

Replace with explicit guards. These are lurking `TypeError`s on unexpected input.

### A10. Narrow 3 bare `catch` blocks in TS
- `typescript/src/gen2/index.ts:1531-1554`

Narrow to the specific error types (mirrors what we did for Python `except Exception` at gen2.py:1832).

### A11. Delete TS stub test masquerading as gen1 coverage
- `typescript/src/gen1/gen1_coverage.test.ts:38-46`

Test named `"Uint8Array"` asserts null roundtrip. Either fix or delete.

### A12. Fix TS `gen2.test.ts` — test harness invisible to node:test
- `typescript/src/gen2/gen2.test.ts`

Custom `test()` harness registers **0 tests** with `node:test`. Either convert to `node:test` API or delete (prior migration likely left stub behind).

### A13. TS `bench_vllm.ts` — exclude from dist
- `typescript/src/gen2/bench_vllm.ts`

Compiled into shipped `dist/` with hardcoded relative path `../../../rust/cowrie-wasm/pkg-node/`. Belongs in a `scripts/` or `bench/` tree excluded from `tsconfig.json` `include`.

### A14. Add cross-package `DefaultMax*` drift test for TypeScript
Mirror what we added in Go (`go/limits_drift_test.go`): TS gen1 limits currently 100× looser than TS gen2 in the same package. Add a test that fails if they diverge without justification, or unify them.

---

## Tier B — Structural (review before applying)

### B1. Narrow weak types in TS `wasm-bindgen` output
- `rust/cowrie-wasm/pkg-node/*.d.ts` — all `any`-typed

Regenerate with `tsify` attributes on the Rust side to emit typed TS. Requires a WASM rebuild step; small but non-trivial churn. Blocked by: `rust/cowrie-wasm/` source missing (only `Cargo.lock` tracked — needs investigation).

### B2. C header type sharpening — `cowrie_gen2.h`
- `c/include/cowrie_gen2.h`

5 struct fields typed `uint8_t` that should be their defined enum. `void *col_indices` should be typed. `int boolean` inside a union should be `bool` (C99+). These are API-visible; coordinate with any C FFI consumers.

### B3. Orphan crates / libs
- `rust/glyph-codec/glyph-codec/` — 3022 LOC orphan crate + 381MB `target/`
- `c/glyph/` — 4957 LOC orphan C lib, not in `CMakeLists.txt`
- `rust/glyph-codec/glyph-codec/src/schema_evolution.rs` (620 LOC) — 0 external callers, publicly re-exported
- `c/glyph/src/schema_evolution.c` (710 LOC) — same

**Decision needed from user:** are these parked work-in-progress or truly dead? If dead, deleting `rust/glyph-codec/` and `c/glyph/` recovers ~8K LOC + 381MB. If parked, at minimum:
- Add to repo root's `.gitignore` for `target/`
- Add a README in each explaining status
- Remove `pub use schema_evolution::*;` from `glyph-codec/src/lib.rs` so it doesn't pollute the public surface

### B4. Rust `encode.rs` panic paths
- `rust/src/gen2/encode.rs:259-261, 450-452` — `.expect("key should be in dictionary")`

The "should be" suggests internal invariants. If truly internal, acceptable; if reachable from user input, convert to `Result` error variants.

### B5. Rust `_ => {}` swallows future variants
- `rust/src/gen2/encode.rs:148`

Replace with explicit match arms or `unreachable!()` so new tag variants force a compile error rather than silent no-op encode.

### B6. Missing Rust WASM source
- `rust/cowrie-wasm/` — only `Cargo.lock` + `pkg-node/` + `pkg-web/` tracked. Source crate not in tree.

Either restore the source from history or declare the `pkg-*` directories as released binary artifacts and move them to CI-built releases. Currently opaque to reviewers.

---

## Tier C — Deferred / Out of Scope

- **Gen1 itself** across all 3 languages — intentional parallel codec, not legacy. No action.
- **Rust zigzag duplicated 5×** — nice to consolidate but low value; each copy is 4 lines.
- **dtype name table duplicated 6×** — same; consolidate when touching for another reason.
- **`docs/glyph/archive/BLOB_POOL_SPEC.md`** — stale spec, not code.
- **`rust/target/package/`** — cargo artifacts, not source. Add to `.gitignore`.

---

## Apply Order

1. **Tier A1–A4 first** (real bugs). One commit each; each paired with a test.
2. **Tier A5–A14** (hygiene). Can bundle into per-language commits: one Rust, one C, one TS.
3. **Tier B** only after user confirms scope — especially B3 (orphan crates) which deletes thousands of LOC.
4. Full cross-lang fixture parity run after every Tier A commit: `cd go && go test ./...` + `cd python && python -m pytest` + `cd rust && cargo test` + `cd c/build && ctest --output-on-failure` + `cd typescript && node --import tsx --test src/**/*.test.ts`.

---

## Files Estimate (if all Tier A + B applied)

- Modified: ~18 (4 Rust, 5 C, 7 TS, 2 config)
- Deleted: 0 Tier A; ~50 files Tier B3 if orphan crates removed
- New: 2 (cross-lang fingerprint parity test, TS limits drift test)

Recommend Tier A only in the next apply session; surface Tier B3 as a separate scope decision.
