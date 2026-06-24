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
