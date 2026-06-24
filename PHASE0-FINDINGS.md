# Phase 0 — Findings (cross-language identity gate)

## What was built
A cross-language **round-trip identity gate**: for each fixture, compute `canonical = encode(decode(fixture))` per language and compare **byte-for-byte across languages** (JSON-projection-independent, so it tests identity not display).
- Go: new `recode` subcommand — `go/cmd/cowrie/main.go` (`DecodeFramed → Encode → stdout raw`).
- Rust: `rust/examples/recode.rs` (`decode_framed → encode → stdout`). Also `rust/examples/decode_cli.rs` (framed decode→JSON) for the JSON-projection matrix.
- Baseline: existing `testdata/fixtures/validate_fixtures.py` is green (57 passed, Go+Python decode).

## Finding F1 (high) — canonical OBJECT KEY ORDERING is unspecified → cross-language identity divergence
Round-trip over all `testdata/**/*.cowrie`: **Go vs Rust canonical bytes — 35 same, 9 different.** The 9 differ on **object key ordering**:
- **Go** preserves insertion/encounter order in the dictionary.
- **Rust** byte-sorts keys.
- `SPEC.md` is **silent** on canonical key order (dictionary-coding section describes index references, never ordering). So neither impl is "wrong" — the spec under-specifies the canonical form, and the determinism *product* is therefore not actually pinned.

Concrete (`testdata/gen2/floats.cowrie`, keys e/pi/zero):
- orig fixture used `TagObject (0x07)`; both impls correctly canonicalize to `FIXMAP (0xd3)` (mandatory for ≤15 fields) — so the FIXMAP rule IS shared.
- Go dict order: `zero, pi, e` (insertion). Rust dict order: `e, pi, zero` (sorted). → different bytes.

Same class as known bug **B4** (schema-fingerprint divergence) — both stem from canonical degrees of freedom not being normatively specified.

## Implication for the plan
- The round-trip gate works and earns its place as a standing CI gate (Phase 0 deliverable ✓).
- **Phase 1a must come first and is bigger than "make Go the oracle":** the SPEC itself must *define* the canonical form (key order + any other free DOFs) before fixtures or any identity work can be trusted. You cannot make the spec the oracle until the spec is complete.
- JSON-projection divergence also exists (int64-as-string vs number, float `0` vs `0.0`, bigint object vs decimal string) — lower priority (display, not identity), file separately.

## Cross-model review verdict (run `bvwlhpiql` — GPT‑5.5 + Gemini + DeepSeek, UNANIMOUS)
**1. Canonical key order = byte-sorted lexicographic on raw UTF-8 key bytes.** Neutralizes language hash-map iteration order; aligns with the existing fingerprint "keys sorted" rule; Gen2 already collects all keys before encoding (buffering already required) so sorting is near-zero extra cost. Header **dictionary** byte-sorted; **object fields** emitted in sorted/dict-index order; **duplicate keys rejected** in canonical mode.

**2. Phase-1a = write a normative "Canonical Encoding Profile" into SPEC.md FIRST** (do NOT bless an impl's accidental behavior — the Protobuf trap). Then make Go + Rust implement a strict `MarshalCanonical()` mode that complies. Implementations are evidence, not authority.

**3. Other canonical DOFs to pin in the Profile (key order alone is NOT enough):**
- Floats: normalize `-0.0`→`+0.0`; single canonical NaN bit pattern (reject/normalize signaling+payload); decide infinities.
- Integers: mandate minimal form (FIXINT/FIXNEG over TagInt64; Uint64 vs positive Int64); forbid overlong varints; BigInt minimal two's-complement.
- **Compression: canonical form is UNCOMPRESSED** (gzip/zstd vary by version/level) — or one exact algo+config. Identity is over uncompressed bytes.
- Dictionary: global lexicographic construction order across the whole doc; forbid unused entries.
- Strings: require valid UTF-8; do NOT Unicode-normalize.
- Decimal128 coefficient/scale canonicalization.
- Tensor/Image/Audio: LE raw bytes, no implicit padding, dtype-size consistency; Bitmask trailing bits zeroed.
- Reject trailing data / reserved tags / non-round-trippable input.

**4. Gate enhancements (beyond recode byte-equality):**
- Semantic AST equality *before* byte comparison (don't corrupt data to force byte-equality).
- Cross-language symmetry: `GoEncode(RustDecode(x)) == RustEncode(GoDecode(x))`.
- Idempotence: `encode(decode(canonical)) == canonical`.
- **Anti-malleability negative tests:** feed valid-but-non-canonical bytes (overlong varint, unsorted keys, TagInt64 of a small value, duplicate keys) → strict mode MUST reject with `ERR_NON_CANONICAL`, not silently fix.
- Float edge fixtures (`-0`, NaN, inf); fingerprint equality from both decoded value and canonical bytes.

## Phase-1a — spec-completeness panel verdict (run `bnj4i2468`, clean-room implementers, UNANIMOUS)
The Canonical Encoding Profile was a good start but the SPEC is **not yet clean-room-implementable**. The panel (acting as new-language implementers with zero source access) found the spec needs a coherent greenfield consolidation, not more patches (my Appendix C contradicts the main spec in ~6 places).

**Top fixes (consensus):**
- **[DONE] C.2 float rule** — "source type is f32" was undecidable from bytes & broke bijectivity. Fixed to value-based: Float32 iff `widen(narrow(v))==v`, else Float64.
- **Add missing error codes** to the canonical table + a condition→error **precedence** table: `ERR_NON_CANONICAL`, `ERR_DUPLICATE_KEY`, `ERR_RESERVED_TAG`, plus enum/length cases.
- **Fingerprint grammar undefined** — `FNV-1a(type_structure)` never defines the byte serialization fed to FNV-1a. Needs a normative per-type contribution table + test vectors (this is the B4 root).
- **Pin primitive formats** (clean-room blockers): BigInt endianness, UUID128 (RFC-4122 BE vs LE), Decimal128 coefficient endianness+sign + canonicalization algorithm ("divide coeff/scale by 10 while coeff%10==0 && scale>0"), Datetime64 epoch (Unix UTC?), **varint max length (10 bytes / DoS cutoff)**, FIXMAP `dictIdx:uvarint`, DictLen = count vs bytes, TensorRef store semantics.
- **Resolve contradictions:** reserved tags (main "skip" vs C.10 "reject" → reject in canonical), TagExt (main "implementation-defined" vs C.9 "KEEP" → KEEP), compression header byte-4 meaning, version `0x02` vs "v3 inline types" label, MaxStringLen applies to decoded bytes.
- **Define a formal value algebra/AST first**, then derive canonical encoding from it (the single biggest risk — bijectivity/AST-equality/fingerprint all depend on a precise value model the spec lacks).

**Greenfield amputations (no users — clean cuts, user-approved):**
- **Rename magic `SJ` (0x534A)** → `COWR` (0x434F5752, 4-byte) + explicit version byte (GPT-5.5's pick; avoids the FIXINT collision the spec itself notes). (Gemini/DeepSeek suggested 2-byte `CW`.)
- **Delete Gen1 entirely** — one "Cowrie v1 canonical" format; unify the tag space; free 0x16–0x19; drop the Gen1/Gen2 reserved-tag split.
- **Single compression-type byte** (0=none/1=gzip/2=zstd) instead of bit-flags.
- **Move deprecated tags to 0xF0+ and reject**; drop the "v3 inline types" label (no versioning — it's just the format).
- Consider **deleting scalar Float32** (always Float64) and **TensorRef** (no external-store spec) unless fully specified.

→ Phase 1a is therefore a deliberate **SPEC rewrite to one clean-room-complete greenfield canonical format**, ideally panel-vetted, not piecemeal patches.

## Status
Go+Rust round-trip gate built & run (Phase-0 core ✓). Finding F1 + review captured. **Next (Phase 1a): author the Canonical Encoding Profile in SPEC.md, then conform Go/Rust + add strict-mode negative fixtures.** TS + Python recode and CI wiring still pending. Held before drop-C (correct — canonical form must be pinned first).
