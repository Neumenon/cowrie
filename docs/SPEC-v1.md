# Cowrie v1 — Wire Format Specification (clean-room rewrite)

> **Status: spine complete (§0–§6), panel-vetted, reference-executable.** Greenfield (no shipped users
> → no back-compat). The completeness bar: a new-language implementation must be buildable, and pass
> every conformance fixture, from **this document alone** — never by reading any existing implementation.
> Any ambiguity here is a spec bug to fix, not a thing to resolve by copying code.
>
> This supersedes the legacy two-format (Gen1/Gen2) `SPEC.md`. Order: **(0) decisions → (1) value model →
> (2) wire encoding → (3) canonical profile → (4) fingerprint grammar → (5) conformance → (6) profiles.**
> The Python reference `tools/cowrie_ref/` implements the full Core from this document alone and generates
> the golden vectors (`testdata/v1_golden.json`); it is the conformance oracle. Remaining: conform Go/Rust/TS
> to the golden vectors (in progress), then the cross-language gate goes green.

## 0. Greenfield decisions (settled)
- **One format.** No Gen1, no proto-tensors, no "v3 inline" label, no Gen1/Gen2 reserved-tag split.
  It is just "Cowrie v1."
- **Magic + version.** Stream begins with `COWR` (`0x43 0x4F 0x57 0x52`) + 1 version byte `0x01`.
  (Replaces the legacy `SJ`/`0x534A` magic, which collided with FIXINT values.)
- **Compression is a single header byte**, not bit-flags: `0x00` none · `0x01` gzip · `0x02` zstd ·
  `0x03–0xFF` reserved (reject). Compression is transport only; **identity is over uncompressed bytes.**
- **Deprecated/unknown core tags → reject** (`ERR_RESERVED_TAG`). Forward compatibility is **only**
  via the Extension envelope (§1.3), never via unknown core tags.
- **Always headered.** Every stream has the header; there is no headerless mode.

### 0.1 Core-admission rule (standing law)
> **A type may enter Core only if its value domain, equality, canonical encoding, error behavior, and
> conformance fixtures are fully specified WITHOUT external state.** Otherwise it is a Profile, Stream-layer,
> Dataset-layer, Tool, or Integration concern — never a Core wire variant.

This is why `TensorRef` (needs an external store), graph/richtext/delta (application semantics), and
media codecs (external codec ecosystems) are **not** Core. It is the rule that keeps Core deterministic
and prevents a second source of spec entropy.

### 0.2 Doctrine (organizing principle)
**Core is physics · Profiles are chemistry · Applications are biology.** Core = a small set of strongly
specified, composable primitives. Everything else is a **Profile** — a recommended *schema over Core
types*, carrying no new wire tag. Build determinism-first: a layer (Core → Tensor → Stream → Profiles →
Manifest → Tools) is "done" only when its canonical spec is frozen **and** the cross-language fixture
gate is green. New use cases should *fall out of* strong primitives, not be predicted as new Core types.

## 1. The value model (the algebra)
A Cowrie *value* is exactly one of the variants below, each with a precise **value domain**. Canonical
encoding (§3) is a total function from this domain to bytes; decoding is its inverse. Two values are
**equal** iff same variant and equal per the domain rule here — this is the equality the conformance
gate's "semantic AST equality" uses.

### 1.1 Scalars
| Variant | Domain | Notes |
|---|---|---|
| **Null** | unit | — |
| **Bool** | {false, true} | — |
| **Int** | ℤ ∩ [−2⁶³, 2⁶³−1] | signed 64-bit |
| **Uint** | ℤ ∩ [2⁶³, 2⁶⁴−1] | ONLY values above Int's range; values ≤ 2⁶³−1 are **Int**, never Uint (canonical disjointness) |
| **BigInt** | ℤ outside [−2⁶³, 2⁶⁴−1] | arbitrary-precision; smaller integers are Int/Uint, never BigInt |
| **Float** | IEEE-754 binary64 | `−0.0`≡`+0.0`; exactly one canonical quiet NaN; ±∞ allowed. **One scalar float type only** — no scalar f32 (bulk f32 lives in Tensor, §1.2). This deletes the K10 dual-encode ambiguity at the source: one wire form per float value. |
| **Decimal128** | `value = coefficient × 10^(−scale)`; coefficient ∈ ℤ∩[−2¹²⁷,2¹²⁷−1], **scale ∈ int64** (svarint) | **Canonical = lowest terms:** while coefficient ≠ 0 and `coefficient mod 10 == 0`, divide coefficient by 10 and **decrement scale**; `0 ≡ (coeff 0, scale 0)`. Two values equal iff equal canonical `(coeff, scale)`. **NB:** this is a `(coeff, 10^−scale)` decimal, **not** IEEE-754 decimal128 (BID/DPD) — different format despite the name. |
| **String** | finite sequences of Unicode scalar values, valid UTF-8 | NOT Unicode-normalized (byte-preserving) |
| **Bytes** | finite byte sequences | — |
| **UUID** | 128-bit | byte order per §2 (RFC-4122 big-endian) |
| **Datetime** | ℤ ∩ int64 nanoseconds since **Unix epoch 1970-01-01T00:00:00Z** | — |

### 1.2 Composites
| Variant | Domain |
|---|---|
| **Array** | ordered finite sequence of values (order is semantic, preserved) |
| **Object** | finite **map** from String keys to values; keys unique. Key order is NOT semantic — two Objects are equal iff same key→value set. (Canonical encoding emits keys byte-sorted; §3 C.4.) |
| **Tensor** | (dtype ∈ DType enum §2, shape ∈ finite seq of non-neg ints, data = `product(shape)×dtype_size` little-endian elements) |
| **Bitmask** | (count ∈ ℕ, count packed bits LSB-first; trailing bits of final byte are not part of the value and MUST be zero on the wire) |

### 1.3 Extension (forward-compat boundary)
- **Extension** = (extType ∈ uvarint, payload = opaque bytes). The ONLY sanctioned growth path.
- Decoders preserve unknown extensions byte-for-byte (KEEP). Two Extensions are equal iff equal
  (extType, payload). Fingerprint contribution is `uvarint(extType)` (§4) — structural and
  decoder-independent; the payload is never hashed/recursed.

> **Deliberately excluded** from the v1 value model (were in the legacy spec; cut on coherence
> grounds — see clean-room verdict): Graph/Node/Edge/GraphShard/AdjList, GNN batches, RichText,
> Delta, columnar/ColumnHints, TensorRef (no external-store semantics specified). Express these as
> schemas over the core (Object/Array/Tensor) if needed, never as new wire variants.

## 2. Wire encoding
Encodes exactly the §1 value model — no other variants. (Image/Audio/Video as opaque envelopes is a
deferred decision, §2.9.)

### 2.1 Varints
- **uvarint** = unsigned LEB128, little-endian groups, 7 bits/byte, MSB=continuation. MUST be
  **minimal** (no trailing all-zero continuation), **≤ 10 bytes**, AND **MUST NOT overflow 64 bits**:
  a 10-byte uvarint whose final group sets any bit above bit 63 ⇒ `ERR_INVALID_VARINT` (minimality alone
  does not catch overflow). Overlong/over-length/overflow ⇒ `ERR_INVALID_VARINT`. Used for all lengths,
  counts, dict indices, dims.
- **svarint** = zigzag(`(n<<1) ^ (n>>63)`) then uvarint. Used by Int64 (0x03).

### 2.2 Stream header
```
COWR (0x43 0x4F 0x57 0x52) | version:u8 (0x01) | compression:u8 | DictLen:uvarint
| Dict[DictLen] = (keyLen:uvarint + keyLen UTF-8 bytes)   | RootValue
```
- `version != 0x01` ⇒ `ERR_INVALID_VERSION`; bad magic ⇒ `ERR_INVALID_MAGIC`.
- **compression:** `0x00` none · `0x01` gzip · `0x02` zstd · else `ERR_UNSUPPORTED_COMPRESSION`.
  If non-zero, everything after the `compression` byte (i.e. `DictLen … RootValue`) is replaced by
  `OrigLen:uvarint + compressed bytes`; decoders decompress (size-bounded, `ERR_DECOMPRESSED_*`)
  then parse the inner `DictLen … RootValue`. **Compression is transport only; canonical = `0x00`.**
- **Dict** = the deduplicated set of all Object keys in the document. `DictLen` is an **entry count**
  (not a byte length). Canonical order is byte-sorted (§3 / Appendix C.4). Exactly one Root value
  follows; bytes after it ⇒ `ERR_TRAILING_DATA`.

### 2.3 Tag space (one unified 0x00–0xFF)
| Tag | Variant | Body |
|---|---|---|
| 0x00 | Null | — |
| 0x01 | Bool false | — |
| 0x02 | Bool true | — |
| 0x03 | Int | svarint |
| 0x04 | Float | 8 bytes IEEE-754 binary64 **LE** — the only scalar float |
| 0x05 | String | len:uvarint + UTF-8 |
| 0x06 | Array | count:uvarint + count values |
| 0x07 | Object | count:uvarint + count × (dictIdx:uvarint + value) |
| 0x08 | Bytes | len:uvarint + raw bytes |
| 0x09 | Uint | uvarint (no zigzag) |
| 0x0A | Decimal128 | scale:svarint + coefficient:16-byte two's-complement **LE** |
| 0x0B | Datetime | int64 nanos since Unix epoch UTC, **LE** (range ±2⁶³ ns ≈ ±292 yr; out of int64 ⇒ `ERR_TOO_LARGE`) |
| 0x0C | UUID | 16 bytes, RFC-4122 field order (**big-endian**) — the sole BE field |
| 0x0D | BigInt | len:uvarint + two's-complement bytes **LE**, minimal length |
| 0x0E | Extension | extType:uvarint + len:uvarint + payload |
| 0x20 | Tensor | dtype:u8 + rank:u8 + dims:uvarint×rank + dataLen:uvarint + **align-pad** + data (LE elements) |

**Tensor data alignment (§2.5, NORMATIVE).** A tensor's `data` MUST begin at a **64-byte boundary
relative to byte 0 of the message** (the `COWR` magic). After `dataLen`, the encoder writes exactly
`pad = (−offset) mod 64` zero bytes, where `offset` is the absolute byte position just after `dataLen`;
`data` then starts at the aligned position. The padding MUST be zero (non-zero ⇒ `ERR_NON_CANONICAL`)
and its length is fully determined by position, so there is still exactly one canonical byte-string.
This makes the contiguous `data` run mmap/SIMD/GPU zero-copy-friendly (see §7 for file-level alignment).
The content address (§3) hashes the padded canonical bytes.
| 0x24 | Bitmask | count:uvarint + ⌈count/8⌉ bytes, LSB-first; trailing bits of last byte = 0 |
| 0x40–0xBF | FIXINT | value = tag − 0x40 (0..127); single byte |
| 0xC0–0xCF | FIXARRAY | count = tag − 0xC0 (0..15), then count values |
| 0xD0–0xDF | FIXMAP | count = tag − 0xD0 (0..15), then count × (dictIdx:uvarint + value) |
| 0xE0–0xEF | FIXNEG | value = −1 − (tag − 0xE0) (−1..−16); single byte |
| all other tags | **reject** | `ERR_RESERVED_TAG` (incl. legacy 0x0F, 0x16–0x19, 0x21–0x23, 0x30–0x39, 0xF0–0xFF) |

> Deleted vs legacy: **scalar Float32 (0x0F)** — see §1.1; bulk f32 → Tensor dtype Float32. Also
> proto-tensor arrays (0x16–0x19), TensorRef (0x21), Image/Audio (0x22/0x23 — see §2.9), all
> graph/RichText/Delta tags (0x30–0x39). All are `ERR_RESERVED_TAG` in v1.

### 2.3.1 Endianness & canonical number form
- **All multi-byte numeric fields are little-endian** (Float, Datetime, Decimal128 coefficient,
  BigInt, Tensor elements). **UUID is the sole exception** (RFC-4122 big-endian field order). This
  removes the LE/BE mix that breeds bugs.
- **Canonical integer choice (governs which tag):** smallest valid form — `0..127` → FIXINT,
  `−1..−16` → FIXNEG, values fitting Int64 → Int (0x03), non-negative > Int64::MAX → Uint (0x09),
  else BigInt (0x0D). A decoder MUST reject (`ERR_NON_CANONICAL`) any BigInt/Uint/Int that encodes
  a value representable in a smaller form. Likewise FIXARRAY/FIXMAP for ≤15 elements/fields.
- **Float canonical bytes:** `+0.0` = `00 00 00 00 00 00 00 00`; `−0.0` MUST normalize to it; the one
  canonical quiet NaN is `0x7FF8000000000000` (LE on the wire); any other NaN/`−0` ⇒ `ERR_NON_CANONICAL`.

### 2.4 Object & dictionary
- An Object body (0x07 / FIXMAP) is `count` pairs `(dictIdx:uvarint, value)`. **`dictIdx` is the
  0-based positional index** of the key in the header Dict sequence; out-of-range ⇒ `ERR_INVALID_FIELD_ID`.
  Canonical emits pairs in ascending dictIdx (== byte-sorted key, Appendix C.4) with unique keys;
  duplicate ⇒ `ERR_DUPLICATE_KEY`.

### 2.5 DType enum (Tensor) — explicit values + element size
| DType | val | bytes/elem | | DType | val | bytes/elem |
|---|---|---|---|---|---|---|
| Float32 | 0x01 | 4 | | Uint8 | 0x08 | 1 |
| Float16 | 0x02 | 2 | | Uint16 | 0x09 | 2 |
| BFloat16 | 0x03 | 2 | | Uint32 | 0x0A | 4 |
| Int8 | 0x04 | 1 | | Uint64 | 0x0B | 8 |
| Int16 | 0x05 | 2 | | Float64 | 0x0C | 8 |
| Int32 | 0x06 | 4 | | Bool | 0x0D | 1 |
| Int64 | 0x07 | 8 | | | | |
| QINT4 | 0x10 | ½ (packed) | | Ternary | 0x13 | packed |
| QINT2 | 0x11 | ¼ (packed) | | Binary | 0x14 | ⅛ (1 bit) |
| QINT3 | 0x12 | ⅜ (packed) | | | | |
- **Shape:** `rank` = number of dims. **rank 0 = scalar** (exactly 1 element, no dims; `product = 1`).
  A dim MAY be 0 (empty tensor, 0 elements). `[1]` and `[1,1]` are distinct shapes (different rank).
- **`bits_per_elem`:** full-byte dtypes per §2.5 table; sub-byte: Binary=1, QINT2=2, Ternary=2,
  QINT3=3, QINT4=4. `dataLen` MUST equal `ceil(product(shape) × bits_per_elem / 8)`; mismatch ⇒ length error.
- **Sub-byte packing:** elements are packed in row-major order, **LSB-first within each byte** — element
  e₀ occupies the lowest `bits_per_elem` bits of byte 0, e₁ the next, crossing byte boundaries with no
  per-element padding. The final byte's unused high bits MUST be 0 (non-zero ⇒ `ERR_NON_CANONICAL`).
  Unknown dtype byte ⇒ `ERR_INVALID_TAG`.
- **dtype names are NOMINAL.** Core defines only bit-width + packing + dataLen for each dtype; the
  *numeric interpretation* of element bits (e.g. what QINT2 quantization means, or the meaning of
  Ternary's unused 4th 2-bit code) is a **Profile** concern, not Core-observable (§0.1). Two impls
  may interpret a dtype differently and both pass Core conformance — Core only fixes the byte layout.

### 2.6 Error codes (canonical) + precedence
Existing: `ERR_INVALID_MAGIC, ERR_INVALID_VERSION, ERR_TRUNCATED, ERR_INVALID_TAG, ERR_TRAILING_DATA,
ERR_INVALID_UTF8, ERR_INVALID_VARINT, ERR_INVALID_FIELD_ID, ERR_TOO_DEEP, ERR_TOO_LARGE,
ERR_DICT_TOO_LARGE, ERR_STRING_TOO_LARGE, ERR_BYTES_TOO_LARGE, ERR_EXT_TOO_LARGE, ERR_RANK_TOO_LARGE,
ERR_UNSUPPORTED_COMPRESSION, ERR_DECOMPRESSED_TOO_LARGE, ERR_DECOMPRESSED_MISMATCH, ERR_UNKNOWN_EXTENSION`.
*(Audio-specific codes removed — audio is deferred, §2.9.)*
**Added (normative):** `ERR_RESERVED_TAG`, `ERR_DUPLICATE_KEY`, `ERR_NON_CANONICAL` (strict mode), `ERR_INVALID_PADDING`.
**Precedence** (first applicable wins): magic/version → truncation → varint validity → tag validity
(reserved/unknown) → limit checks (depth/size) → UTF-8/field-id/dtype validity → duplicate-key →
canonical-form violations (`ERR_NON_CANONICAL`) → trailing data.

### 2.7 Limits (normative constants for conformance)
MaxDepth 1000 · MaxArray/MaxObject 1,000,000 · MaxString 10MB · MaxBytes 50MB · MaxExt 1MB ·
MaxDict 1,000,000 · MaxRank 32 · **MaxTensorBytes 16 GiB** (`dataLen` ≤ 2³⁴; the format exists for
model-scale tensors, so this is large but MUST be bounded — an unbounded `dataLen` is a DoS surface).
Sizes count **decoded** bytes. Overrides are out-of-conformance.

### 2.8 Streaming note (acknowledged constraint)
The header dictionary forces a buffering pre-pass on encode (collect+sort all keys before byte 0 of
the body). This is accepted for v1 (identity > single-pass streaming). A future frame/stream layer may
relax it without changing per-value identity.

### 2.9 Image / Audio / Video — DEFERRED decision
Clean-room verdict: keep only as *opaque blob envelopes* (no codec modeling) or drop. Not in v1 core
yet — expressible as `Extension` or `Object{format, bytes}` until a decision is made. Resolve before 1.0.

## 3. Canonical Encoding Profile (NORMATIVE)
Exactly **one** canonical byte-string per value (§0.1). A strict decoder MUST `ERR_NON_CANONICAL`-reject
well-formed-but-non-canonical input (it MUST NOT silently re-canonicalize). The rules live where each
field is defined; this is the complete index:
- **Integers / varints** (§2.1, §2.3.1): smallest form — FIXINT/FIXNEG → Int → Uint → BigInt; minimal LEB128.
- **Float** (§2.3.1): single scalar float = Float64; `−0.0`→`+0.0`; canonical quiet NaN `0x7FF8000000000000`
  (LE). **No scalar Float32** (§1.1) — f32 is a Tensor dtype only.
- **Object keys & dictionary** (§2.4): header dict byte-sorted, unique, no unused entries; fields in
  ascending dictIdx; duplicate ⇒ `ERR_DUPLICATE_KEY`.
- **Decimal128** (§2.3): coefficient in lowest terms (no trailing-zero coefficient foldable into a smaller
  scale); `0` = (coeff 0, scale 0).
- **Strings** (§1.1): valid UTF-8, not Unicode-normalized.
- **Tensor / Bitmask** (§2.5): little-endian; exact `dataLen`; sub-byte LSB-first; trailing bits zero.
- **Compression** (§2.2): canonical form is **uncompressed** (flag `0x00`).
- **Extensions** (§1.3): KEEP byte-exact.
- **Framing** (§2.6): trailing data ⇒ `ERR_TRAILING_DATA`; reserved/unknown tags ⇒ `ERR_RESERVED_TAG`.
- **Identity (per value):** content address = **SHA-256** of the canonical, **uncompressed** value bytes,
  emitted as a **multihash** (`0x12 0x20` prefix + the 32 hash bytes) so the algorithm can rotate post-1.0.
  This is the *only* identity v1 defines. **File-level identity (a Merkle root over many values/frames) is
  DEFERRED** to the not-yet-specified stream/dataset layer — v1 makes no file-identity claim.
- **Bijectivity invariant (MUST):** `canonical_encode(decode(canonical)) == canonical`.

*(Supersedes the legacy `SPEC.md` Appendix C, which still carries the now-removed scalar-Float32 rule.)*

## 4. Structural fingerprint (NORMATIVE grammar)
This is a **structural (shape) fingerprint**, NOT a value-independent declaration-level schema identity —
be precise about what equality means. Two values share a fingerprint iff they have **identical shape up to
scalar values within a variant**: same array lengths, same tensor shapes, same Bitmask counts, same Object
key sets, the same integer **magnitude class** (Int vs Uint vs BigInt, decided by magnitude per §1.1), and
the same variant otherwise. Consequences to know: `{n:5}` and `{n:2⁶³}` differ (Int vs Uint); a 1000-element
array differs from a 1001-element one. It is excellent for routing/drift on **homogeneous** streams; it is
**not** a column-schema hash. — It is `FNV-1a-64` over the bytes produced by `fp(value)` below, using a
**spec-pinned fingerprint-code table** — NOT the wire tag and NOT any host enum/`iota` (the B4 root bug).
All `uvarint(...)` below use the §2.1 minimal-LEB128 encoding. `fp` operates on the **abstract value model
of §1.1**, never the wire form: a value encoded as FIXINT, `Int`, or `Uint` all route to the FPC of their
§1.1 domain (`5` is always `Int`/0x02). FPC bytes are a **separate namespace** from wire tags — any numeric
coincidence (e.g. Null=0x00) is incidental. Object keys are byte-lexicographically sorted *before* emitting
their element fps.

### 4.1 Fingerprint codes (FPC) — frozen, append-only
```
Null 0x00  Bool 0x01  Int 0x02  Uint 0x03  BigInt 0x04  Float 0x05  Decimal128 0x06
String 0x07  Bytes 0x08  UUID 0x09  Datetime 0x0A  Array 0x0B  Object 0x0C  Tensor 0x0D
Bitmask 0x0E  Extension 0x0F
```
Note: `Int`/`Uint`/`BigInt` are **distinct** FPCs — but the boundary is **magnitude** (§1.1), so this
deliberately leaks value into the fingerprint (see the §4 framing on shape-not-schema).

### 4.2 `fp(value)` → bytes
- **Scalars** (Null…Datetime): the single FPC byte. (No value bytes — structure only.)
- **Array:** `0x0B` + `uvarint(len)` + `fp(eᵢ)` for each element in order. (So `[Int,String]` ≠ `[Int,Int]`.)
- **Object:** `0x0C` + `uvarint(nKeys)` + for each key in **byte-sorted** order: `uvarint(keyLen)` +
  key UTF-8 bytes + `fp(value)`. (Keys + value-types; order canonical.)
- **Tensor:** `0x0D` + `dtype:u8` + `rank:u8` + `uvarint(dimᵢ)` for each dim in order. (dtype, rank,
  **and shape** are all type structure for a tensor — `[3]` ≠ `[3,4]` ≠ `[2,3]`. This is the one place a
  "shape" is structural, unlike scalar values elsewhere.)
- **Bitmask:** `0x0E` + `uvarint(count)`. (Collection sizes are uniformly structural — consistent with
  Array length and Tensor shape; previously Bitmask omitted its count, an inconsistency now fixed.)
- **Extension:** `0x0F` + `uvarint(extType)`. The `extType` is stable type structure — read identically by
  every decoder whether or not it *understands* the extension — and keeps extension routing/type-checking.
  The **payload is excluded** (never recursed). (This also fixes B4's Go-includes-extType / TS-excludes split
  by making inclusion normative.)

### 4.3 Computation
```
fingerprint64 = FNV1a64(concat of fp(root))     # offset basis 0xcbf29ce484222325, prime 0x100000001b3
fingerprint32 = fingerprint64 & 0xFFFFFFFF
```
A `FINGERPRINT_VERSION` (currently 1) covers this grammar + the FPC table; bumping it is the only way
the table changes, and the table is append-only.

### 4.4 Conformance
Golden fingerprint vectors live in `testdata/**/*.fingerprint`; every implementation MUST reproduce
`fingerprint64`/`fingerprint32` **byte-identically** for every fixture, computed from the decoded value
(structure) — this is the release gate that kills B4. *(Vectors to be generated from this grammar, not
from any implementation.)*

## 5. Conformance (NORMATIVE)
A `manifest.json` lists cases. Fixtures are **generated from this spec**, never from an implementation.

### 5.1 Case shape
```
{ id, kind: "decode"|"from_json", file: "<path>.cow",
  expect: { ok: bool, json?: <value>, error?: "ERR_*", fingerprint64?: <u64>, canonical?: <hex> } }
```

### 5.2 Obligations a conformant implementation MUST satisfy (every positive fixture)
1. **Round-trip identity:** `canonical_encode(decode(bytes)) == bytes` (byte-exact).
2. **Cross-language byte-identity:** every language's canonical encoding of a value is identical.
3. **Cross-language symmetry:** `EncodeₐReference == Encodeᵦ ∘ Decodeₐ` for all language pairs.
4. **Idempotence:** `encode(decode(canonical)) == canonical`.
5. **Semantic AST equality:** decoded values compare equal (per §1 domains) before byte comparison.
6. **Fingerprint equality:** §4 `fingerprint64` matches the golden value across languages.

### 5.3 Negative / anti-malleability fixtures (MUST reject in strict mode)
Overlong varint, unsorted dict/keys, non-minimal int (e.g. `TagInt64` for a FIXINT value), duplicate
key, `−0`/non-canonical NaN, compressed-as-identity, BigInt/Uint for a smaller-fitting value, non-zero
sub-byte/bitmask trailing bits, trailing data, reserved/unknown core tag — each ⇒ the specific `ERR_*`
of §2.6 (or `ERR_NON_CANONICAL`). The strict decoder MUST NOT silently re-canonicalize.

### 5.4 Definition of "done"
**A new-language implementation written from this document alone (no source reference) passes 5.2 + 5.3
on the full fixture suite.** That, and only that, is conformance — and it is the meaning of "v1 done."

## 6. Profiles (chemistry over physics)
A **Profile** is a recommended **schema over Core types** (Object/Array/Tensor/Bytes/…) — it adds NO
wire tag and needs NO new decoder code; any v1 implementation already decodes it. Profiles carry
*meaning*; Core carries *bytes*. A Profile may define its own equality/identity rule layered on Core's,
but MUST NOT depend on external state to *decode* (per §0.1). Profiles live in `docs/profiles/`.

### 6.1 TensorRef Profile (pin carefully — it is the dangerous one)
A pointer to a tensor stored elsewhere, expressed as an Object:
```
TensorRef = Object{
  hash:    Bytes,        // multihash SHA-256 of the referenced tensor's CANONICAL bytes (§3)  ← the identity
  dtype:   String,       // canonical §2.5 dtype name, LOWERCASE only: "float32","float16","bfloat16",
                         //   "int8".."uint64","float64","bool","qint4","qint2","qint3","ternary","binary"
  shape:   Array<Int>,
  byteLen: Int,
  store:   String?       // OPTIONAL URI HINT only
}
```
Normative rules (these prevent the nondeterminism a naive ref reintroduces):
- **Identity = `hash`** (= hash of the referenced tensor's canonical bytes), never a hash of the ref
  Object and never the fetched bytes at decode time.
- `store` is a **hint, not semantics** — two refs with the same `hash` and different `store` are equal.
- **No existence/resolution check at decode.** Resolution (fetch + verify `hash`) is an application step.

### 6.2 Embedding Profile (the first killer profile)
```
EmbeddingRecord = Object{ id, vector: Tensor<f32>[D], model, modelVersion, metric, normalized: Bool,
                          sourceRef?, createdAt: Datetime }
```
Vector is a real Tensor (not a JSON float array). Identity is the Core identity of the Object.

### 6.3 Other profiles (stubs — schemas over Core, defined as needed)
Media (Image/Audio/Video as `Object{format, bytes, ...}` opaque envelopes), Graph (Node/Edge as Object
schemas), RichText (text + token-span arrays), Eval, Trace, TrainingBatch, DatasetManifest/Shards.

### 6.4 Profile-simulation gate (de-risks the "emergence" bet)
**Before Core is frozen at 1.0**, the Embedding, Media, and Trace profiles MUST be fully expressible as
schemas over the locked Core with no Core change required. If any profile needs a new Core capability,
add it *before* freeze — thawing Core after 1.0 breaks determinism. This is a release gate, not advice.

## 7. Files & Merkle file identity (NORMATIVE)
A **Cowrie file** packages an ordered list of **frames** — each frame is one complete, canonical §2.2
COWR value's wire bytes (uncompressed) — with a sealed footer index for O(1) random access and a
**Merkle root** that is the file's content identity. Sealed (no append); growth = a new file. Compression,
if any, sits *above* this layer (it changes file bytes, hence identity) — frames are always uncompressed.

**Layout** (LE ints; uvarint = §2.1):
```
"CWRF" (0x43 0x57 0x52 0x46) · version u8(0x01) · reserved u8(0x00) · uvarint frame_count
repeat frame_count:  uvarint frame_len · align-pad · frame_bytes   (canonical COWR value, 64B-aligned start)
FOOTER @ footer_offset:
    uvarint frame_count                                     (MUST equal the header count)
    repeat:  uvarint frame_offset · uvarint frame_len       (MUST mirror the body layout exactly)
    merkle_root  34 bytes                                   (multihash SHA-256, §3)
u64 LE footer_offset · "CWRF"                               (seek-from-end trailer)
```
Each frame's bytes begin at a **64-byte file offset**: after `frame_len`, write `(−offset) mod 64` zero
bytes (verified zero on decode). Combined with §2.5's in-message tensor alignment, this puts every tensor's
`data` at a 64-byte **absolute file offset**, so `mmap(file)` yields aligned zero-copy tensor views.
The **body is the source of truth**: a decoder reads frames sequentially via their length prefixes and
MUST reject (`ERR_NON_CANONICAL`) any file whose footer count/offsets do not exactly mirror that layout,
whose reserved byte ≠ 0, or whose body does not end exactly at `footer_offset`. Thus there is **exactly one
canonical file byte-string per ordered value list**. `frame_count` MUST be ≤ 2³². Bad magic/version ⇒
`ERR_INVALID_MAGIC`; truncation ⇒ `ERR_TRUNCATED`.

**Merkle root (RFC 6962 domain separation, promote-odd — never duplicate; count-bound):**
```
leaf(f)     = SHA-256(0x00 || f)
node(a, b)  = SHA-256(0x01 || a || b)
tree_digest = promote-odd reduction of the leaves           (0 frames ⇒ SHA-256(""))
root_digest = SHA-256(0x02 || uvarint(frame_count) || tree_digest)
merkle_root = 0x12 0x20 || root_digest                      (multihash sha2-256)
```
Promote-odd (carry a lone node up unchanged) avoids the duplicate-leaf ambiguity (CVE-2012-2459); the
`0x00/0x01/0x02` domain tags plus the bound `frame_count` make it impossible for two different frame lists
to share a root. **File identity = `merkle_root`.** A verifying decoder recomputes it from the frames and
rejects a mismatch (tamper detection). The Merkle *leaf* (`0x00`-prefixed) is deliberately distinct from a
frame's per-value content address (§3, un-prefixed).
