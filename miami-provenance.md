# Miami — Cowrie Provenance & Build Plan

> Single-file record of the full decision chain: the adoption-view verdict, the clean-room engineering verdict, and the phased build plan. Produced via the cross-model **quorum** (Claude Opus + GPT-5.5 + Gemini + DeepSeek) over 2026-06-23.

---

# PART 1 — North Star (adoption view)

# Cowrie — North Star (engineering verdict)

> Adjudicated by a cross-model **quorum** (Claude/Opus orchestrating + GPT‑5.5, Gemini, DeepSeek as independent voices), grounded in the actual repo — not the aspirational doc. Valuation/market claims out of scope. Run `wkcodd0zn`, 8 agents.

## Verdict (6/6 models, no dissent)

**The codebase's deliberate minimal core IS the north star. The maximalist "AI data plane" document is second‑system overreach** — it proposes re‑adding almost exactly what the team already, correctly, sent to `attic/` (Graph, GNN, RichText, Delta, columnar), plus first‑class Video, a 5–6 product platform (Lake/Eval/Observe/Gateway/Train/Vector‑Ingest), and 8 SDKs.

**The attic decision was correct** — the most mature engineering judgment in the picture.

## One‑sentence north star

> **Cowrie is the deterministic, cross‑language, typed _envelope_ for AI data _in flight_ — embeddings, tensors, tensor‑refs + provenance — that hashes identically everywhere (caching, dedup, dataset identity, reproducible replay) and _interoperates with_ Arrow / Parquet / safetensors rather than replacing them.**

## Is the document's north star good design + feasible? No — the repo proves it

Every model independently cited `attic/docs/audit-report.md` as evidence that breadth already caused rot:

- `DType` enums with **incompatible numeric values** across `gnn` / `ucodec` / root (`0x01` vs `0`)
- `MaxObjectLen` security limit **silently drifted 10×** between packages
- orphaned packages (`intern` / `ucodec` / `dicts`) with zero importers; `gnn/onnx` unreachable from the codec
- triplicated NumPy dtype map with a narrowing bug
- assertion‑free **"95% savings" tests that always pass**

The team can't keep *four* languages' constants in agreement today → 8 SDKs of a multimodal platform is infeasible. Honest signal: *too much surface, already drifting.* The document's answer — "add a platform" — is the wrong direction.

## Claims adjudicated

| Claim | Verdict | Note |
|---|---|---|
| **C1** JSON poor substrate for AI/binary | **TRUE** | the real, defensible core (float32 text‑bloat ~4.8×; no bytes/decimal/uuid/ns‑time without ceremony) |
| **C2** deterministic → content‑addressable | **TRUE but hard** | sound in design, *not yet proven in fleet*; cross‑language byte‑determinism (NaN, key sort, padding) is the actual hard problem |
| **C3** fingerprint routing without decode | **HALF‑TRUE** | real for routing/drift; it's a 64‑bit FNV‑1a *shape* hash — **not** validation, **not** tamper‑detection |
| **C4** one format for everything | **OVERREACH** | row vs columnar are different memory models. Formally drop it. |
| **C5** embeddings are the beachhead | **TRUE‑ish** | the one *measured* win (open decision below) |
| **C6** replayability is the differentiator | **HALF‑TRUE** | an event‑sourcing/systems choice; Cowrie is the enabler, not the engine |
| **C7** interop, don't compete | **TRUE — elevate to policy** | the only sane path |

## Real engineering value (not $)

Killing the **"JSON + base64 + `.npy` sidecar"** nightmare: one deterministic, hashable, typed envelope carrying metadata + tensors + raw bytes that decodes identically in Go/Rust/Py/TS — plus **schema‑fingerprint routing/caching at a proxy without decoding the payload**. `TensorRef (0x21)` is the strategic linchpin: *point at the heavy bytes, don't own them.*

Where Cowrie adds **zero** value (and loses): tensor‑on‑disk → **safetensors**; columnar/lake → **Arrow/Parquet**; schema'd RPC → **Protobuf/Avro**; generic binary‑JSON → **CBOR/MessagePack**; telemetry → **OpenTelemetry**; RichText/Delta → **CRDTs/ProseMirror**.

## Keep / Cut

**Keep:** core `0x00–0x0F` + the scalars JSON lacks (`bytes 0x08`, int64/uint64, `decimal128`, `UUID128`, ns‑`datetime64`, float32) · `Tensor 0x20` · `TensorRef 0x21` · `Bitmask 0x24` · schema fingerprint (scoped to routing/drift) · `TagExt 0x0E` as the **only** sanctioned growth path · adapters · a sharp CLI (inspect/diff/fingerprint).

**Cut / keep‑cut:** Graph/GNN · RichText · Delta · JSON‑LD · columnar/ColumnHints · Video · the entire platform layer (Lake/Eval/Observe/Gateway/Train/Vector‑Ingest).

## Gating work (close these before anything new)

1. Fix the cross‑language **DType / limit drift**.
2. Finish the **full encode‑parity matrix** (today: only "pinned tests").
3. Delete the **assertion‑free savings tests**.
4. Specify **content hashes over uncompressed canonical bytes only** (push compression to transport/storage).

## The one open decision

The wedge:
- **Embeddings‑first** (Claude + GPT‑5.5) — smallest, hottest, only measured win.
- **End‑to‑end multimodal AI request/response payload** (Gemini + DeepSeek) — prompt + image bytes + embedding + output in one row; VectorDBs already own embedding *storage*.

Both agree it's the *in‑flight typed‑AI‑payload* seam. The split is: lead with the **vector**, or the whole **transaction**. That's the call left to make.

---

*Confidence: high — four independent models + the codebase's own audit converged on the same verdict.*

---

# PART 2 — North Star (clean-room engineering view)

# Cowrie — Clean-Room Engineering North Star

> **Framing:** incumbents (Arrow/Parquet/safetensors/Protobuf/CBOR/OTel/vector-DBs) assumed **not to exist**; adoption/market/team-size **out of scope**; 2-year from-scratch horizon. Judged only by the laws of engineering: memory layout, alignment, determinism, information theory, type-system soundness, cross-language reproducibility.
>
> This run deliberately **un-does the framing bias** of the first review (which evaluated *"should you ship this vs incumbents"* and leaned on the team's `attic` focus-cuts as if they were proof against the ideal). The cuts in `attic/CUTLIST.md` were made for **focus / maintenance / positioning — not** proven engineering impossibility.

---

## Headline

The ideal is **engineeringly coherent** — *if* reframed as **"one deterministic typed container with multiple physical layouts,"** not "one magic byte layout that is simultaneously row-store, column-store, tensor-store, codec system, and mutable-state engine." The merits are largely **TRUE**. The work isn't "trim ambition" — it's **canonical-identity discipline**.

## The jewel & the crux (Q4)

The defensible jewel is **not breadth of types** — it is **deterministic, content-addressable canonical identity across languages.**

> **Breadth does NOT defeat determinism. Ambiguity and per-type hand-written hash code defeats it.**

Proof is in the repo (bug **B4**): fingerprint divergence appears **only in compound/recursive types** (UNKNOWN_EXT recursion, Audio `encoding+channels`, bitmask/node ordinals) — **never in scalars**. Root cause located in code: `go/schema.go` hashes `byte(v.typ)` — a **raw Go `iota` ordinal** — so the fingerprint is bound to *host enum declaration order*, and **zero fingerprint fixtures** exist.

**Answer:** you do not need a frozen-*small* type set; you need a **frozen canonical-identity surface** — every type ships a pinned canonical-form contribution + **golden cross-language fixtures as a release gate**, and extensions stay **opaque** (fixed sentinel in the hash) unless formally promoted.

## Conflict map

| | Conflict | Verdict | Mechanism / Cost |
|---|---|---|---|
| K1 | canonical bytes vs zero-copy tensor views | **reconcilable** | make **alignment padding part of the canonical form** (zero-filled, hashed). *Single most important fix the spec is missing* — today zero-copy fails closed on `ptr%align≠0`. |
| K2 | one container: row + columnar + tensor | **represent-yes / optimal-no** | TLV can *hold* all three (columnar = Object{col→Tensor}); cannot be layout-optimal at all three. Admit columnar is hosted-as-tensor. |
| K3 | content-hash vs compression vs unknown-ext | **reconcilable, one rule** | identity = hash of **canonical, uncompressed, decoded** bytes; compression is transport-only, below the identity line. |
| K5 | lossy quantization vs "lossless" | **false** | lossless about *whatever dtype you're handed* (qint4 bytes preserved exactly). |
| K6 | determinism vs open type system | **reconcilable** | freeze the *identity surface*, not the type count (see crux). |
| K7 | append-only vs whole-hash vs random-access | **reconcilable by layering** | frame (streamable+hashable) → sealed file + **Merkle footer** → file identity = Merkle root. |
| K8 | dict-coded keys vs zero-copy | **false** | disjoint regions (object keys vs tensor buffer). |
| **K4** | **Delta-as-value-type vs immutability** | **FATAL** | mutable-state primitive contradicts content-addressing. Serialize *patch records* (Objects), never a Delta tag. |
| **K9** | **first-class Video** (codecs/keyframe/object-tracks) | **FATAL — category error** | codecs/keyframe indices are codec-internal or application semantics. Keep an **honest blob envelope** only. |

**Conflicts the panel found unprompted:**
- **K10 — Float32 (0x0F) "decodes as float64"** → two canonical byte-strings for one decoded value → **identity ambiguity**. Pick exactly one wire-type-per-value.
- **K11 — configurable decode limits** are part of the acceptance set → two conformant decoders accept different bytes → **conformance non-determinism**. Make limits normative constants.
- **K12 — unknown-ext participates in the fingerprint** → forward-compat fights fingerprint stability. Unknown-ext must contribute a **fixed sentinel**, never its ExtType/structure.

## Cut taxonomy (why a cut is made matters)

- **Cut on INCOHERENCE** (applies even clean-room): Delta-as-value-type (K4), first-class Video/Image/Audio *modeling* (K9), Float32 dual-encoding (K10), configurable-limits-as-conformance (K11).
- **Cut for DETERMINISM-COST** (coherent, but net-negative to the jewel): Graph/GNN/RichText **as wire tags** — pure sugar over Object/Array/Tensor, most fingerprint surface for least layout gain. Keep as **schemas/conventions**, not core tags.
- **NOT engineering-mandated** (the team's focus/adoption cuts): the platform layer, SDK count — runway decisions, *not* coherence ones.

## The defensible 2-year core (unanimous)

1. **Strict canonical profile (MANDATORY):** UTF-8 byte-sorted unique keys, minimal varints (FIXINT/FIXARRAY/FIXMAP), one float rule (NaN / −0.0 / subnormal pinned), LE everywhere, normalized decimal128/bigint, duplicate-key rejection.
2. **Content-addressing** = hash of canonical, **uncompressed, decoded** bytes (per-value/per-frame; file identity = **Merkle root** over frame hashes). Compression is transport-only.
3. **Scalar core** (the jewel) + **Tensor with mandatory deterministic alignment padding** (zero-copy *guaranteed*, not best-effort) + **Bitmask** + **TensorRef** (documented opaque pointer) + **dictionary-coded keys**.
4. **Image/Audio/Video** ONLY as opaque typed-blob envelopes (encoding/dims + bytes + optional annotation Object). No codec-internal modeling.
5. **Unknown-ext:** byte-exact round-trip, hashed verbatim, **never recursively interpreted** (TLV opaque-skip), fixed fingerprint sentinel.
6. **Append-only frames + sealed Merkle footer** for random access.
7. Express **Embedding / RichText / columnar as schemas over the core**, not as new wire tags.

**Immediate determinism-hardening work (kills B4-class drift):** fix the `iota`-ordinal fingerprint (use a spec-pinned contribution table), add golden cross-language fingerprint fixtures as a **release gate**, resolve K10/K11/K12, and write the alignment-padding rule into the canonical form.

---

## Provenance — how this was produced

Produced by the **quorum** harness (Claude/Opus orchestrating; external models consulted as independent voices), grounded in the real repo (`SPEC.md`, `go/types.go`, `go/schema.go`, `attic/docs/review-notes/CUTLIST.md`).

**Two cross-model runs on Cowrie:**

| Run | Framing | Agents | Models that answered | Subagent tokens | Wall-clock |
|---|---|---|---|---|---|
| `wkcodd0zn` | adoption / vs-incumbents (later judged *biased*) | 8 | Opus 4.8 ×5, GPT‑5.5, Gemini 3.1 Pro, DeepSeek V3 | ~359k | ~5 min |
| `wlioi1xm5` | **clean-room engineering merit** (this doc) | 8 | Opus 4.8 ×5, GPT‑5.5, Gemini 3.1 Pro *(DeepSeek dropped out)* | ~352k | ~15.6 min |

**The 4 distinct models (per run):**
- **Claude Opus 4.8 (1M)** — orchestrator + **4 parallel "angle" subagents** + **1 synthesis** agent (reads real code, drives the analysis).
- **GPT‑5.5** — via `oracle-codex` (Codex / ChatGPT Pro login, `medium` effort).
- **Gemini 3.1 Pro** — `gemini-3.1-pro-preview` (direct Google key).
- **DeepSeek V3** — `deepseek-chat` (direct `api.deepseek.com`). *Returned in `wkcodd0zn`; its agent dropped on a slow-command bug in `wlioi1xm5`.*

**Per run:** 5 Opus agents + 3 external voices = **8 agents**. Across both Cowrie runs: **16 agent invocations, ~711k subagent tokens, 4 distinct models.**

> Method note: the first run's near-unanimity was partly an artifact of a leading prompt (it pre-labeled the maximalist doc "overreach" and fed the panel the team's own self-critical audit). This clean-room run re-ran with the bias removed; the engineering-merit verdict (above) is the one to trust.

---

# PART 3 — Phased Build Plan (rev 2)

# Cowrie — Phased Build Plan to the Defensible Core (rev 2)

## Context
Cowrie (`github.com/Neumenon/cowrie`) is a real polyglot binary codec (Go reference ~34k LOC + Python/Rust/TS; C slated to drop). Two clean-room cross-model reviews established the defensible north star: **a deterministic, content-addressable, typed envelope for AI data in flight** — the product is **cross-language deterministic identity**, so the work is **canonical-identity discipline, sequenced determinism-first**. This plan takes the repo from its current state to a frozen, conformance-gated 1.0 of that core. ~2 years, milestone-driven.

> **Provenance:** grounded in clean-room quorum `wlioi1xm5` (merit truth-table + K1–K12 conflict map), a build-mode draft (`waa9izt9x`), and a cross-model critique pass (`bgayyylvj`: GPT‑5.5 + Gemini + DeepSeek) whose corrections are folded into **rev 2** below.

## Critique corrections folded into rev 2 (what changed from rev 1)
- **Content address = hash of canonical, uncompressed *WIRE* bytes** — *not* "decoded bytes" (rev-1 error; decoded/in-memory leaks host endianness/padding).
- **Spec is the reference, not Go.** `SPEC.md` becomes executable normative pseudocode (canonical-encode + fingerprint traversal); fixtures derive from the spec. Go passes/fails like every impl. (Using Go-as-oracle would just prove everyone matches Go's bugs.)
- **Drop C early** (new Phase 1b) — a diverged C fork shipping non-conformant fingerprints poisons the parity gate.
- **One identity-hardening window:** fingerprint table + K10/K11/K12 + tensor padding all change canonical form → bundle under a single **`CANONICAL_VERSION`/epoch**; pre-epoch artifacts rejected with `ERR_LEGACY_*`. `FINGERPRINT_VERSION` alone is insufficient.
- **Zero-copy is a performance property, not an identity property** — all impls emit identical LE canonical bytes; zero-copy is an optimization layered on top. Post-C, Python's zero-copy path is **Rust via PyO3**, with pure-Python as the correctness fallback.
- **Phase-0 reality:** B3 is already fixed (`testdata/fixtures/validate_fixtures.py` really decodes Go+Python across 55 cases); silenced lints are down to two benign ones. The real Phase-0 gap is **byte-level checks + Rust/TS in the matrix + spec-as-oracle**, not "de-no-op B3."

## Principle
Determinism/identity is the product. Build the **proof instrument** (spec-anchored, byte-level, cross-language) before touching identity; **drop the poisoning impl**; **pin the identity surface in one epoch**; only then widen. Every phase has an objective, testable EXIT gate.

---

## Phase 0 — A real, spec-anchored, byte-level parity gate  *(M0; blocks everything)*
**Goal:** a proof instrument that catches `-0`/NaN/f32-vs-f64/ordering/padding/unknown-ext bugs — which a JSON-equality matrix hides.
**Work:** extend `testdata/fixtures/validate_fixtures.py` (currently Go+Python only) to a `lang × fixture` matrix that **also runs Rust + TS** via thin `decode <fixture> → canonical-JSON` CLIs (none exist yet — write them: `rust/src/bin/`, `typescript/bin/`). Gate on **bytes, not JSON**: `decode(wire) → IR`, `canonical_encode(IR) == original wire bytes`, `fingerprint(IR.schema) == expected`, and `decode→encode→decode` round-trip byte-match. Negative fixtures MUST fail with the exact error class. Smoke-test the **pure-Python fallback** (no `_cext`) and keep it green from here on. The hard deliverable is a **lossless, language-neutral IR/manifest** preserving every identity-relevant distinction (GPT‑5.5's top risk) — start it here.
**Exit gate:** 4×N green cells (go/py/rust/ts) on byte-match + fingerprint + round-trip + negative-reject; a `testdata/_canary/` meta-test that turns a cell red on a 1-byte flip; CI grep-guard: no `|| true`/`2>/dev/null` in workflow logic.
**Risk:** the IR, not the CLIs, is the hard part — if the IR drops a distinction, later phases look green while identity is broken. Mitigate by deriving the IR from the spec's value model and asserting wire-byte equality, never just IR equality.

## Phase 1a — Make SPEC.md the executable reference  *(M0→M1)*
**Goal:** stop proving "matches Go"; prove "matches spec."
**Work:** write `SPEC.md` canonical-encode + fingerprint-traversal as literate, deterministic pseudocode; generate fixtures from the spec model; Go becomes one more impl under test.
**Exit gate:** every fixture's expected bytes/fingerprint trace to a spec clause; a spec-conformance doc lists each rule → fixture.

## Phase 1b — Drop C before locking identity  *(M1)*
**Goal:** remove the wire-incompatible (`SJFR` vs `SJ`), non-conformant C fork that poisons the gate.
**Work:** re-home the ~20 `_cext` FFI entry points off `python/csrc/`; choose Python's native path = **Rust via PyO3** (for real zero-copy) with pure-Python as correctness fallback; delete `c/`; remove C from the publish gate (`ci.yml`).
**Exit gate:** wheel builds AND passes the full Phase-0 byte-level suite; PyO3 path byte-identical to pure-Python; no C in publish `needs`.
**Risk:** wheel regressions (Win/aarch64) → keep pure-Python sdist fallback, gate native behind `COWRIE_REQUIRE_NATIVE`.

## Phase 2 — The identity-hardening epoch  *(M1; one `CANONICAL_VERSION` bump)*
**Goal:** exactly one canonical wire-byte-string per value, fingerprint stable cross-lang. **Ship as ONE incompatible version**, because these all move the canonical form together:
- **Fingerprint root cause (B4):** replace `go/schema.go`'s `byte(v.typ)` (raw `iota`) with a **spec-pinned contribution table**, mirrored in all impls; define normative compound/recursive traversal. Includes **K12** (unknown-ext contributes a **fixed sentinel**, never ExtType/payload/recursion, while round-tripping bytes verbatim).
- **K10:** Float32(0x0F) one-wire-type-per-value; identity over wire+bits; single canonical encoder.
- **K11:** decode limits → normative constants; overrides = out-of-conformance.
- **Tensor alignment padding** as canonical form: zero-filled, **hashed**, strictly **LE** wire bytes, frame-relative offset with exact alignment base + nested-container rule; `ERR_INVALID_PADDING`. Decoders **hard-fail `ERR_MALLEABLE`** on non-canonical bytes (overlong, un-normalized NaN, alt padding).
**Exit gate:** byte-identical fingerprint (`fp64`/`fp32`) + canonical bytes across go/py/rust/ts for every type incl. compound/recursive; "reorder canary" proves fingerprint is enum-order-independent; non-canonical inputs rejected; pre-epoch artifacts rejected with `ERR_LEGACY_*`. Append-only fixture table per type.
**Risk:** back-compat — version bumps don't fix shipped data → explicit epoch transition + legacy-reject fixtures.

## Phase 3 — Content-addressing + frames + random access  *(M2)*
**Goal:** the identity architecture, consistent with Phase 2's canonical bytes.
**Work:** content address = hash of canonical **uncompressed WIRE** bytes; compression strictly **below** the identity line **and block/frame-aligned** (continuous stream compression breaks random access); file identity = **Merkle root** over frame hashes. Resolve the append-vs-random-access fork explicitly: **fixed frames + sealed footer index** (no append after seal) OR **append-only + unsealed rolling accumulator** (no random access until seal) — pick one in SPEC, don't conflate.
**Exit gate:** identical content-address + Merkle root cross-lang AND across `none`/`gzip`/`zstd`; independent **block-decompression** + random-access-by-footer + tamper-localization fixtures pass on every decoder.

## Phase 4 — Honest envelopes + schemas-over-core → freeze 1.0  *(M3)*
**Goal:** the frozen, honest core.
**Work:** Image/Audio/Video as **opaque blob envelopes only** (no Video tag, no codec modeling); resolve the Gen1 `0x30/0x39` doc-vs-code conflict (D1); express Embedding/RichText/columnar as `docs/schemas/` **conventions over core types**, never new wire tags.
**Exit gate:** envelope identity depends only on pinned fields; reserved-tag skip uniform; "no new wire tag" subset check; conformance count frozen → tag **v1.0**.

---

## Cross-cutting — standing gates (every PR + release)
Spec-anchored byte-match · `canonical_encode(decode(x)) == x` round-trip · fingerprint **release gate** · content-address/Merkle parity · **identity over wire bytes only** (never decoded/in-memory) · compression-invariance of identity · negative/legacy-reject fixtures with exact error class · tri-language **differential fuzzing** · monotonic conformance count · no-silenced-lint guard · no-new-wire-tag guard.

## Cross-language parity matrix
**SPEC = reference/generator.** Go / Python(+wheel via PyO3) / Rust / TS must byte-match on: decode, canonical encode, fingerprint, K10/K11/K12, tensor canonical bytes (+ zero-copy as a *perf* property on the Rust/Go/PyO3 paths), content address, Merkle root, compression-invariance, envelope identity, reserved-tag handling, differential fuzz. C is dropped (Phase 1b).

## Milestones
M0 spec-anchored byte-level gate (incl. Rust/TS) → M1 C dropped + identity epoch frozen *(one incompatible `CANONICAL_VERSION`)* → M2 identity architecture (frames/Merkle/block-compression) → M3 honest core frozen = **1.0**.

## Verification
Per phase: regenerate fixtures **from the spec model**, run each language's decode CLI, assert wire-byte equality + fingerprint + round-trip + negative-reject; fuzz targets (go/rust/py) find no determinism/zero-copy/malleability violation; `CONFORMANCE.md` count rises monotonically.

## Files this touches (representative)
`SPEC.md` (executable canonical/fingerprint/identity rules) · `go/schema.go` + `go/types.go` (contribution table, padding) · `testdata/fixtures/validate_fixtures.py` + `testdata/_canary/` · new `rust/src/bin/` + `typescript/bin/` decode CLIs · `.github/workflows/ci.yml` + `fuzz.yml` · `python/csrc/` + `publish-pypi.yml` (C-drop → PyO3) · `CONFORMANCE.md` · `docs/schemas/`.
