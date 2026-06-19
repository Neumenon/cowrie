# Cowrie Cut List & Maintenance Decisions

> **Status: PROPOSED — awaiting maintainer ratification.**
> This document captures the findings of two adversarial multi-agent review
> passes (2026-06-19). Each verdict is tagged with a confidence level and
> provenance. **No code has been changed on the strength of this document
> alone.** Items in §6 require an explicit maintainer decision before any
> code or spec change is made.

## 0. How this was produced

Two review passes ran over the whole repo. Each pass: ~6–8 deep-readers map a
component → a synthesis lead drafts recommendations → one adversarial skeptic
per recommendation independently re-opens the code to *refute* it → a final
editor keeps only what survives.

- **Pass 1** (formats & dead weight): 6 of 14 readers completed; 8 were lost to
  transient server rate-limiting. 15 recommendations drafted, 14 upheld, 1 refuted.
- **Pass 2** (gap-fill: per-language + glyph verdicts): all 8 readers completed.
  18 recommendations drafted, 18 upheld, 0 refuted.

**The adversarial pass refuted two claims that pass 1 had treated as settled.**
Those are flagged below and moved to §6 (needs ratification), not §3 (safe).

---

## 1. Language implementations

| Lang | Verdict | Confidence | Notes |
|------|---------|-----------|-------|
| **Go** | **keep** | high | Reference impl; generates fixtures; publishes via git tag. Non-negotiable. |
| **Python** | **keep** | high | PyPI `cowrie-py`; strongest non-Go test coverage; passes all 34 fixtures. |
| **Rust** | **keep** | high | `cowrie-rs` Gen2; correct `SJ` framing; 157+ tests; pre-built WASM artifacts. |
| **TypeScript** | **lean-keep** | high | Only JS/web entry (`cowrie-codec`@2.0.0); 341 tests / 87.6% cov. **Conditional on 3 fixes (§2); if not fixed before next release → lean-drop** (only ~37 npm downloads/mo, no external dependents). |
| **C** | **DROP** | high | Two independently sufficient grounds (below). |

### C drop — grounds
1. **Wire incompatibility.** `c/src/gen2.c` uses a 4-byte `SJFR` framing magic
   (v0x01, fixed u32 LE sizes); Go/Rust/Python/SPEC all use 2-byte `SJ` + v0x02
   + uvarint sizes. Any C-produced *framed* message is silently undecodable by
   every other implementation. (`c/src/gen2.c:2900-2953`)
2. **Inverted publish gate.** `ci.yml:318` lists `c` in the publish-gate `needs`
   array — so a C failure **blocks PyPI/npm/crates.io releases** — yet C
   publishes to no registry itself (`README.md:52` "C (source-only)"). Pure drag.

Supporting (not independently decisive): ~18,096 LOC (excl. build), only
memory-unsafe impl, niche redundant with Rust.

> ⚠️ **C drop is NOT a clean `rm -rf c/`.** Python's Cython `_cext` extension
> compiles a *diverged fork* of the C sources (`python/csrc/`, 4,083 LOC) and
> calls ~20 FFI entry points (`cowrie_direct_encode_tensor`,
> `cowrie_encode_from_descriptor`, …). Dropping C requires restructuring the
> Python build to inline/replace those call sites, or the binary-wheel build
> breaks. See Phase 4 in the action plan.

---

## 2. Live bugs in *published* packages (fix regardless of any drop)

These affect shipped artifacts **now** and are independent of every drop decision.

| # | Severity | Bug | Location |
|---|----------|-----|----------|
| B1 | high | **Dual-PyPI-publish race** — `ci.yml` *and* `publish-pypi.yml` both trigger on `v*` tags → HTTP 400 duplicate-upload on the next release tag. Keep the hardened `publish-pypi.yml`; delete the `ci.yml` publish job. | `ci.yml:359-378` + `publish-pypi.yml` |
| B2 | high | **TS Bitmask fingerprint = `0xff`** — `typeToOrd()` has no case for `Type.BITMASK` (wire tag 0x24, active). Live correctness bug in `cowrie-codec@2.0.0`. | `typescript/src/gen2/index.ts` `typeToOrd()` (~1766-1804) |
| B3 | high | **CI "Cross-Language Fixture Validation" is a no-op** — never invokes a decoder; just `json.load()`s the expected files (`subprocess` imported, never called). This is why C's framing incompatibility went undetected. | `ci.yml:193-265` |
| B4 | med | **Schema fingerprint ordinal divergence** Go↔Rust (off-by-1 for Node→Bitmask) + Go/Rust differ on structural recursion for graph/bitmask. No fingerprint fixture exists. | `go/types.go:111-138`, `rust/src/gen2/schema.rs:62-67`, `go/schema.go:45-96` |
| B5 | med | **Published PyPI wheel (`csrc/`) regressions** — `python/csrc/src/gen2.c` lacks `type_to_go_ord()` (fingerprints incompatible with Go for non-scalars) and drops the reserved-tag 0x30-0x3F forward-compat skip. No CI test catches it (`cibuildwheel` only asserts `_HAS_NATIVE`). | `python/csrc/src/gen2.c`, `publish-pypi.yml` CIBW_TEST_COMMAND |
| B6 | med | **3 silenced CI lint steps** (`|| true` / `2>/dev/null`) masking real findings on every push: mypy (14 errs incl. a bytes/str confusion `gen2.py:1257-1258`), tsc `--noEmit` (44 errs in `src/gen1`), clippy (17 errs). | `ci.yml:95, 118, 142` |

> Note: B2 and B4 partially intersect the graph/bitmask fingerprint story.
> Bitmask (0x24) is **active**, so B2 is a real shipped bug irrespective of the
> graph-tag decision in §6.

---

## 3. Safe cleanups (low controversy, verified — execute on branch)

### 3a. Delete (dead / actively misleading)
- **`benchmarks/`** — stale `.gen1` fixtures cause silent type-misparse (a
  renumbered tag means `BenchmarkDecodeGen1Floats` decodes a DECIMAL128 value);
  `RESULTS.md` is ~150× off; `bench_go.go` calls a non-existent API; all 19
  notebooks crash at import. Real perf claims survive in `go/vllm_bench_test.go`
  + `go/view.go`. **No CI hooks into `benchmarks/`.**
- **`docs/glyph/archive/`** — `COOKBOOK.md` is a 1,853-line near-dup (differs by
  one broken path); `SUBSTRATE_COMPARISON.md` benchmarks a non-existent script;
  `BLOB_POOL_SPEC.md` says "Stable" with an all-unchecked checklist.
  **Precondition:** repoint `docs/glyph/GUIDE.md:585` and
  `docs/glyph/QUICKSTART.md:196` to `docs/glyph/COOKBOOK.md` *before* deleting.

### 3b. Glyph → **keep Go-only** (high confidence)
- **Drop `rust/glyph-codec/`** (3,022 LOC) — diverged cowrie-local copy;
  encodes NaN as the string `"NaN"` (SPEC says reject); still includes `none`/`nil`
  reserved words; unpublished; **not a dependency of `cowrie-rs`** (workspace
  member only). Remove from `rust/Cargo.toml` members.
  - *Conflict to resolve:* `posts/00-release-checklist.md:19` plans to publish it
    as `glyph-rs 0.1.0`. If Rust glyph is ever published, publish the
    **standalone** `glyph/rust/glyph-codec` (authoritative, better tested), not
    this copy.
- **Drop `c/glyph/`** (5,615 LOC) — automatic with the C drop.
  - *Before deletion:* upstream the cowrie copy's safer `strbuf_grow`
    (integer-returning, two `SIZE_MAX` overflow checks) to the standalone, which
    currently lacks those checks.
- **Prune 5 dead `go/glyph/` submodules** (~3,093 prod LOC + ~2,316 test LOC, no
  external callers): `streaming.go`, `stream_validator.go`, `incremental.go`,
  `schema_evolution.go`, `blob.go`.
- **Delete 14 permanently-skipping cross-impl tests** (8 in `cross_impl_test.go`
  + 6 `TestCrossImpl_*` in `loose_test.go`) — they wait on `glyph-js/dist/index.js`,
  which does not exist in this repo.
- **Keep:** `parse.go, emit.go, loose.go, canon.go, pool.go, auto_pool.go,
  document.go, bridge.go, decimal128.go, types.go, doc.go`, and the `stream/`
  subpackage (used by the CLI).

### 3c. Archive stale audit reports
- **`audit-report.md`** → `attic/docs/` (or add a SUPERSEDED banner): its DAG
  references `gnn/graph/delta/ld` as active `go/` packages — all four are in
  `attic/go/`; finding U-5 ("gnn/onnx — KEEP") is wrong (it's attic'd).
- **`audit-report-rust-c-ts.md`** → add a staleness note (Tier A all applied;
  Tier B items B3-B6 remain open). The schema-fingerprint bug it documents was
  fixed in commit `3855080` (this was Pass 1's single refuted recommendation —
  the bug no longer exists).

---

## 4. Documentation fixes (verified, execute on branch)

| Path | Action | Fix |
|------|--------|-----|
| `README.md` | update | Wrong `go get`/import paths (need `/go/v2`); nonexistent APIs `NewNode/NewEdge/NewNodeBatch` (actual: `Node/Edge/NodeBatch`); stale GraphShard perf row; **no mention of Glyph** despite it being a v2.0.0 feature. |
| `SPEC.md` | update | Gen1 Node/Edge layouts wrong (id is length-prefixed string not zigzag-varint; Edge writes 4 strings not src/dst zigzag). *(Defer/delete if Gen1 dropped — see §6.)* |
| `CLAUDE.md` | update | `TESTING_PHILOSOPHY.md` → `TESTING_STANDARDS.md`; stale `173`→`157` test count; `MEMORY.md` does not exist. |
| `docs/glyph/README.md` | update | Wrong install commands: `glyph-serial`→`glyph-py`; `github.com/anthropics/glyph`→`github.com/Neumenon/glyph`; `@anthropics/glyph`→`cowrie-glyph`; dead readthedocs/org links (5 lines). |
| `docs/glyph/QUICKSTART.md` | update | Same wrong package names (3 more lines); `cargo add glyph-codec`→`glyph-rs`. |
| `posts/01-blog-devto.md`, `02-show-hn.md`, `03-reddit.md`, `04-github-release.md` | update | `23`→`34` cross-language fixtures. |
| `posts/00-ideal-customer-profile.md` | update | Line 148 "GraphShard/AdjList on the roadmap" contradicts CHANGELOG ("reserved (deprecated)"). Change to deprecated. |
| `posts/00-market-analysis.md` | update | Edge/IoT section leans on Gen1+C (C being dropped); reframe around WASM. `AdjList … planned extension` → deprecated. |
| `posts/00-elevator-pitches.md` | update | GNN pitch misleading (`go/gnn/` is attic'd) — add caveat that wire-level graph types ship but the GNN convenience package does not. |
| `posts/00-release-checklist.md` | update | `cowrie-final/` paths don't exist → `cowrie/`; remove stale `typescript/glyph/`,`python/glyph/` delete-note (already absent); update line 19 if `glyph-codec` not published. |

> **Do NOT scrub Gen1 content from posts** — there is no recorded Gen1 drop
> decision (§6). Only the fixture-count and GNN/graph-framing fixes are safe now.

---

## 5. Parity gaps (informational — fold into B3/B4 fixes)
- Gen1 has **zero** cross-language fixture coverage (all 34 cases are gen2/v3).
- No compressed/framed fixture exists — the gap that hid C's `SJFR` bug.
- Python `csrc/` fingerprint + reserved-tag regressions ship in the PyPI wheel (B5).

---

## 6. ⚠️ Decisions requiring maintainer sign-off (DO NOT auto-execute)

The adversarial pass **refuted** these as "already-decided." They are defensible
proposals but are *product/spec decisions you have not recorded*:

### D1 — Drop Gen1 entirely (~9,500 LOC across 5 langs)
- **For:** live spec violation (every Gen1 encoder emits `0x30`/`0x39` that SPEC
  says "MUST NOT emit"); `Float32Array` (0x18) missing in 4 of 5 impls; zero
  fixture coverage; no-header design conflicts with the Gen2 fingerprint/routing
  direction.
- **Against / reality:** SPEC documents Gen1 as active (lines 7, 10-131); all 5
  impls ship it; **no CHANGELOG/planning record of a drop**; unknown external
  consumers; Go/Rust/C retain a real 2–2.4× structured-decode advantage.
- **Decision needed:** retire Gen1 (then delete `go/gen1/`, `rust/src/gen1/`,
  `python/cowrie/gen1.py`, `typescript/src/gen1/`, `c/src/gen1.c`, the SPEC Gen1
  sections, and the post Gen1 content) **or** keep it and *fix* the spec
  violation instead.

### D2 — Reserve graph wire tags `0x35-0x38` (Node/Edge/NodeBatch/EdgeBatch)
- **For:** the app layer (`go/graph`, `go/gnn`, 8,875 LOC) is already attic'd;
  two soundness bugs while active — Python `DeterministicEncoder` silently emits
  zero bytes for these types; Go `encodeNodeData`/`encodeEdgeData` iterate props
  by map-range (non-deterministic for ≥2 props).
- **Against / reality:** SPEC still lists `0x35-0x38` as **active**; only
  `0x30-0x32` and `0x39` are reserved. This is a wire-format breaking change (4
  currently-passing fixtures would become `ERR_TRAILING_DATA`).
- **Decision needed:** reserve them (then stub encoder+decoder arms in all impls,
  update SPEC + CHANGELOG + `attic/README.md`) **or** keep them and *fix* the two
  soundness bugs (sort props; add the missing deterministic-encoder arms).

---

## 7. Proposed CHANGELOG entry (DRAFT — only if §6 ratified)

```markdown
## Unreleased

### Dropped — C implementation
The C implementation (`c/`, incl. `c/glyph/`) is removed. It published to no
registry, used a `SJFR` compression-framing envelope incompatible with the wire
spec, and blocked the publish gate without shipping anything. The Python build no
longer compiles the bundled C fork (`python/csrc/`); the ctypes fast-path
(`_native.py`) is removed. The Cython `_cext` extension is retained, built from
inlined sources.

### Glyph — consolidated to a single implementation
Glyph is now maintained Go-only. `rust/glyph-codec/` and `c/glyph/` are removed
(diverged, unpublished copies). Dead `go/glyph/` submodules (streaming,
stream_validator, incremental, schema_evolution, blob) pruned.

### (If D1 ratified) Dropped — Gen1 wire format
Gen1 is retired across all implementations. Decoders no longer accept the Gen1
format. Use Gen2 for all new payloads.

### (If D2 ratified) Reserved — graph wire tags 0x35-0x38
Tags 0x35 (Node), 0x36 (Edge), 0x37 (NodeBatch), 0x38 (EdgeBatch) are now
reserved (deprecated); decoders skip their length-prefixed payloads.
```

## 8. Proposed SPEC deprecation note (DRAFT — only if D2 ratified)

```markdown
### Reserved Tags (0x30-0x32, 0x35-0x39)
Tags 0x30 (AdjList), 0x31 (RichText), 0x32 (Delta), 0x35 (Node), 0x36 (Edge),
0x37 (NodeBatch), 0x38 (EdgeBatch), and 0x39 (GraphShard) are reserved
(deprecated). Encoders MUST NOT emit them. Decoders MUST skip their
length-prefixed payloads silently.
```
