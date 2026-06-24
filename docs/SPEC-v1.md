# Cowrie v1 — Wire Format Specification (clean-room rewrite, draft)

> **Status: DRAFT foundation.** Greenfield (no shipped users → no back-compat). The completeness
> bar: a new-language implementation must be buildable, and pass every conformance fixture, from
> **this document alone** — never by reading any existing implementation. Any ambiguity here is a
> spec bug to fix, not a thing to resolve by copying code.
>
> This rewrite supersedes the legacy two-format (Gen1/Gen2) `SPEC.md`. It is built in the order the
> clean-room panel prescribed: **(1) value model → (2) wire encoding → (3) canonical profile →
> (4) fingerprint grammar → (5) conformance**. This draft lands §1–§2; §3 reuses `SPEC.md` Appendix C
> (already authored); §4–§5 are the remaining backlog (see `PHASE0-FINDINGS.md`).

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
| **Float** | IEEE-754 binary64 values, **plus** the marker that the value is exactly representable in binary32 | `−0.0`≡`+0.0`; exactly one canonical NaN; ±∞ allowed. Wire width is value-decidable (§3, C.2), not a separate type — so equality is over the normalized real/NaN/∞, and width follows from it. |
| **Decimal128** | (coefficient ∈ ℤ∩[−2¹²⁷,2¹²⁷−1], scale ∈ ℤ) in lowest terms | canonical: no trailing-zero coefficient foldable into a smaller scale; 0 ≡ (coeff 0, scale 0) |
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
  (extType, payload). Fingerprint contribution is a **fixed sentinel** (§4), never extType/payload.

> **Deliberately excluded** from the v1 value model (were in the legacy spec; cut on coherence
> grounds — see clean-room verdict): Graph/Node/Edge/GraphShard/AdjList, GNN batches, RichText,
> Delta, columnar/ColumnHints, TensorRef (no external-store semantics specified). Express these as
> schemas over the core (Object/Array/Tensor) if needed, never as new wire variants.

## 2. Wire encoding
Encodes exactly the §1 value model — no other variants. (Image/Audio/Video as opaque envelopes is a
deferred decision, §2.9.)

### 2.1 Varints
- **uvarint** = unsigned LEB128, little-endian groups, 7 bits/byte, MSB=continuation. MUST be
  **minimal** (no trailing all-zero continuation) and **≤ 10 bytes** (a 64-bit value). Overlong or
  >10 bytes ⇒ `ERR_INVALID_VARINT`. Used for all lengths, counts, dict indices, dims.
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
| 0x04 | Float (f64) | 8 bytes IEEE-754 binary64 **LE** |
| 0x05 | String | len:uvarint + UTF-8 |
| 0x06 | Array | count:uvarint + count values |
| 0x07 | Object | count:uvarint + count × (dictIdx:uvarint + value) |
| 0x08 | Bytes | len:uvarint + raw bytes |
| 0x09 | Uint | uvarint (no zigzag) |
| 0x0A | Decimal128 | scale:**s**int8 + coefficient:16-byte two's-complement **big-endian** |
| 0x0B | Datetime | int64 nanos since Unix epoch UTC, **LE** |
| 0x0C | UUID | 16 bytes, RFC-4122 field order (**big-endian**) |
| 0x0D | BigInt | len:uvarint + two's-complement bytes **big-endian**, minimal length |
| 0x0E | Extension | extType:uvarint + len:uvarint + payload |
| 0x0F | Float (f32) | 4 bytes IEEE-754 binary32 **LE** |
| 0x20 | Tensor | dtype:u8 + rank:u8 + dims:uvarint×rank + dataLen:uvarint + data (LE elements) |
| 0x24 | Bitmask | count:uvarint + ⌈count/8⌉ bytes, LSB-first; trailing bits of last byte = 0 |
| 0x40–0xBF | FIXINT | value = tag − 0x40 (0..127); single byte |
| 0xC0–0xCF | FIXARRAY | count = tag − 0xC0 (0..15), then count values |
| 0xD0–0xDF | FIXMAP | count = tag − 0xD0 (0..15), then count × (dictIdx:uvarint + value) |
| 0xE0–0xEF | FIXNEG | value = −1 − (tag − 0xE0) (−1..−16); single byte |
| all other tags | **reject** | `ERR_RESERVED_TAG` (incl. legacy 0x16–0x19, 0x21–0x23, 0x30–0x39, 0xF0–0xFF) |

> Deleted vs legacy: proto-tensor arrays (0x16–0x19), TensorRef (0x21), Image/Audio (0x22/0x23 —
> see §2.9), all graph/RichText/Delta tags (0x30–0x39). They are `ERR_RESERVED_TAG` in v1.

### 2.4 Object & dictionary
- An Object body (0x07 / FIXMAP) is `count` pairs `(dictIdx:uvarint, value)`. `dictIdx` indexes the
  header Dict; out-of-range ⇒ `ERR_INVALID_FIELD_ID`. Canonical emits pairs in ascending dictIdx
  (== byte-sorted key) order with unique keys (Appendix C.4); duplicate ⇒ `ERR_DUPLICATE_KEY`.

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
- `dataLen` MUST equal `ceil(product(shape) × bits_per_elem / 8)`; mismatch ⇒ `ERR_TOO_LARGE`/length error.
  Sub-byte dtypes (QINT*/Ternary/Binary) pack LSB-first; trailing bits of the final byte = 0. Unknown
  dtype byte ⇒ `ERR_INVALID_TAG`. *(Sub-byte packing layout to be pinned with fixtures.)*

### 2.6 Error codes (canonical) + precedence
Existing: `ERR_INVALID_MAGIC, ERR_INVALID_VERSION, ERR_TRUNCATED, ERR_INVALID_TAG, ERR_TRAILING_DATA,
ERR_INVALID_UTF8, ERR_INVALID_VARINT, ERR_INVALID_FIELD_ID, ERR_TOO_DEEP, ERR_TOO_LARGE,
ERR_DICT_TOO_LARGE, ERR_STRING_TOO_LARGE, ERR_BYTES_TOO_LARGE, ERR_EXT_TOO_LARGE, ERR_RANK_TOO_LARGE,
ERR_UNSUPPORTED_COMPRESSION, ERR_DECOMPRESSED_TOO_LARGE, ERR_DECOMPRESSED_MISMATCH, ERR_UNKNOWN_EXTENSION,
ERR_INVALID_AUDIO_RATE, ERR_INVALID_AUDIO_CHANNELS`.
**Added (normative):** `ERR_RESERVED_TAG`, `ERR_DUPLICATE_KEY`, `ERR_NON_CANONICAL` (strict mode), `ERR_INVALID_PADDING`.
**Precedence** (first applicable wins): magic/version → truncation → varint validity → tag validity
(reserved/unknown) → limit checks (depth/size) → UTF-8/field-id/dtype validity → duplicate-key →
canonical-form violations (`ERR_NON_CANONICAL`) → trailing data.

### 2.7 Limits (normative constants for conformance)
MaxDepth 1000 · MaxArray/MaxObject 1,000,000 · MaxString 10MB · MaxBytes 50MB · MaxExt 1MB ·
MaxDict 1,000,000 · MaxRank 32. Sizes count **decoded** bytes. Overrides are out-of-conformance.

### 2.8 Streaming note (acknowledged constraint)
The header dictionary forces a buffering pre-pass on encode (collect+sort all keys before byte 0 of
the body). This is accepted for v1 (identity > single-pass streaming). A future frame/stream layer may
relax it without changing per-value identity.

### 2.9 Image / Audio / Video — DEFERRED decision
Clean-room verdict: keep only as *opaque blob envelopes* (no codec modeling) or drop. Not in v1 core
yet — expressible as `Extension` or `Object{format, bytes}` until a decision is made. Resolve before 1.0.

## 3. Canonical Encoding Profile
Use `SPEC.md` Appendix C verbatim (already authored: integers, value-decidable floats, strings,
byte-sorted keys/dictionary, decimal, tensor/bitmask, uncompressed identity, extensions, framing
hygiene, conformance obligations). To be folded in here once §2 is settled.

## 4. Fingerprint grammar — TODO
Replace `FNV-1a(type_structure)` hand-waving with a normative **per-variant contribution table**
(decoupled from any host enum/`iota`; the B4 root) + golden test vectors. Unknown-extension
contributes the fixed sentinel from §1.3.

## 5. Conformance — TODO
Fixture manifest format; the cross-language gate obligations (C.11): byte-identical canonical
re-encode, cross-language symmetry, idempotence, semantic AST equality, anti-malleability negative
fixtures, fingerprint equality. A new-language port passing this suite is the definition of "done."
