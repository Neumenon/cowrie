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

## 2. Wire encoding — TODO (next draft section)
Tag table (one unified 0x00–0xFF space), header byte layout, dictionary wire format
(DictLen = entry **count**; entries `len:uvarint + UTF-8`), FIXMAP `dictIdx:uvarint + value`,
varint definition (LEB128, **max 10 bytes**, minimal), per-primitive byte layout + **endianness**
(BigInt/Decimal128 coefficient/UUID/Datetime all pinned), DType enum **with explicit numeric values
and sizes**, error-code table + **condition→error precedence**.

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
