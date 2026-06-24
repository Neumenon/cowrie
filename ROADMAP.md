# Cowrie — Roadmap & Status

**Cowrie** is a deterministic, content-addressable, AI-native binary envelope: *equal value ⇒ equal
canonical bytes ⇒ equal hash, in any language* — with tamper-evident files and zero-copy tensors.
Branch: `phase0-spec`. Everything below is **machine-verified by CI gates**, not aspirational.

---

## Standing gates (the proof instrument) — all GREEN in CI
Every push/PR runs `tools/run_all_gates.sh`; a red gate blocks merge.

| Gate | What it proves |
| --- | --- |
| `pytest` (199) | the Python reference (the executable spec) is correct |
| **conformance** (69 × 4 langs) | Python/Go/Rust/TS produce byte-identical canonical output |
| **content-address** (69 × 4) | all four agree on the multihash-SHA-256 per-value identity |
| **file-identity** (7 × 4) | all four agree on Merkle file identity + canonical re-encode |
| **tensor-view** (8 × 4) | all four agree on tensor data spans (64-byte aligned) |
| **negative** (lenient + **strict**) | malformed rejected everywhere; strict rejects non-canonical |
| **differential fuzz** | thousands of random values, zero cross-language divergence |
| **canary** | a 1-byte corruption turns the gate red (the gate has teeth) |

Four implementations: **Python** (reference/oracle), **Go**, **Rust**, **TypeScript**.

---

## Phase 1 — Trust  ✅ COMPLETE
The deterministic spine, sealed and self-defending.
- ✅ Frozen, self-contained **SPEC-v1** (buildable from the doc alone) + Python reference oracle
- ✅ 4-language **byte-identical** canonical conformance (68→69 golden vectors)
- ✅ **Strict decoders** reject non-canonical input (anti-malleability, §5.3) — all four languages
- ✅ Negative + differential-fuzz + canary gates, **wired into CI** (gates are law)
- 🐛 Bugs the gates caught & fixed: DFS-vs-global dict sort (Go/Rust/TS), Rust uvarint overflow,
  missing strict mode, TS object-field integer-key reorder, TS `1<<32` wrap

## Phase 2 — Identity & Tensor Advantage  ✅ COMPLETE
What the format has *beyond* determinism.
- ✅ **Content addressing** — multihash SHA-256 of canonical bytes; native API + gate in all four langs
- ✅ **Files + Merkle identity** (§7) — frames + sealed footer (random access) + count-bound RFC-6962
  Merkle root; tamper-evident; cross-language parity gate
- ✅ **Tensor zero-copy** — data-locator (option A) **and** canonical **64-byte alignment** (option B,
  one canonical epoch): every tensor's data lands at a 64-byte absolute file offset under `mmap`
  ⇒ safetensors-grade zero-copy **inside** the deterministic, content-addressed envelope

## Now — Backlog cleanup (to a clean 1.0 candidate)  🚧 IN PROGRESS
- 🚧 **Strict error-code precision** — negative gate verifies the exact `ERR_*` code cross-language
- 🚧 **Legacy `SJ` test cleanup** — green native test suites (`go test` / `cargo test` / `npm test`)
- 🚧 **CI action bump** — clear the Node-20 deprecation warning

## Phase 3 — Profiles & stream layer  ⏳ NEXT
The last capabilities that must exist *before* Core can be frozen.
- ⏳ **Richer profiles** as conventions-over-Core (no new wire tags): Embedding, Media (opaque
  envelopes), Graph, Trace
- ⏳ **§6.4 profile-simulation release gate** — prove Embedding/Media/Trace are fully expressible over
  the locked Core with **no Core change** (if a profile needs a new Core capability, add it *before* freeze)
- ⏳ **Stream / dataset layer** — file-level identity across many files (deferred from §7)
- ⏳ **Packed tensor-file profile** (optional) — relocated aligned tensor section, a pure safetensors
  replacement (the Gemini design in `docs/PHASE2-TENSOR.md`); only if a flat tensor-file is wanted

---

## 🏁 Phase 4 — Freeze v1.0 — THE FINISH LINE (where we intend to stop)
1.0 is **the honest, frozen, conformance-gated Core** — not "every feature," but "the determinism +
identity + zero-copy spine, proven and locked." We ship here and stop.

**Definition of done (all must hold):**
- ✅ All standing gates green in CI across Python/Go/Rust/TS (conformance · content-address ·
  file-identity · tensor-view · negative+strict with exact codes · fuzz · canary)
- ⏳ Core **frozen** — no new wire tags; the §6.4 profile-simulation gate passes
- ⏳ Conformance vector count **frozen and monotonic**; `CONFORMANCE.md` is the locked record
- ⏳ All four native test suites green; spec is the sole oracle (no impl-as-oracle anywhere)
- ⏳ Tag **`v1.0`**

**Where we deliberately STOP (explicit non-goals for 1.0):**
- ❌ No more Core wire types after freeze — new needs become **profiles over Core**, never new tags
- ❌ Performance tuning beyond zero-copy (threading, GPU kernels, SIMD codecs) — **post-1.0**
- ❌ Ecosystem/bindings/package publishing, language ports beyond the 4 — **post-1.0**
- ❌ The append-only / mutable-stream variant — sealed files only for v1
- ❌ Compression schemes beyond the framing already specced — **post-1.0**

> **Net:** from a sprawling, Go-as-oracle, unprovable codec → a small, spec-anchored core where
> determinism, content-addressing, file identity, and zero-copy tensors are each a **gated fact** in
> four languages. Phase 4 locks that and we're done. Everything past the line is opt-in growth on a
> frozen, trustworthy foundation.

---

## How it compares (beyond determinism)
- **vs JSON** — different category: JSON has no binary, no typed tensors, no canonical form, no identity.
- **vs CBOR** — Cowrie is CBOR-canonical lineage with three things CBOR leaves optional/absent:
  *enforced* (not optional) canonicality + strict anti-malleable decoders; **AI-native Core**
  (tensors/Decimal128/UUID/Datetime with pinned canonical rules + structural fingerprint); and
  **built-in content-addressing + Merkle file identity** — all proven byte-identical across 4 languages.
- **vs safetensors** — Cowrie gives the same aligned zero-copy tensor reads, but inside a deterministic,
  content-addressed, tamper-evident, arbitrarily-nested envelope (safetensors is a flat tensor map).
