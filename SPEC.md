# Cowrie Wire & AI Container Specification (v1.0)

**Status:** Frozen v1.0 (`2026-11-23`)  
**Lineage:** Successor to legacy `Neumenon/cowrie-legacy` canonical codec, re-architected for tri-consumer AI runtimes.  
**Scope:** Bijective canonical binary frames (`Plane A`), deterministic progressive attention lenses (`Plane B`), 16 KB-aligned zero-copy tensor / `StatePage` / co-located IVF arenas (`Plane C`), and out-of-band authority + causal taint + RFC 6962 Merkle seals (`Plane D`).

---

## 1. Design Law

> **Core is physics, profiles are chemistry, lenses are optics.**

- **CoreHash** commits strictly to `Plane A` canonical bytes, raw `Plane C` data payloads, and `Plane D` trust metadata.
- **Satellites** (`StatePages`, model embeddings, learned `R0` synopses) are bound to `(core_hash, model_sha, ctx_cfg)` and live outside `CoreHash` so model churn never mutates eternal archive identity.
- **Compression** (`zstd`, `lz4`) is transport-only and never part of canonical identity.
- **Universal Page Alignment:** `PAGE_ALIGN = 16,384` bytes (`align_log2 = 14`), aligned simultaneously to Apple Silicon 16 KB Unified Memory pages, Linux 4×4 KB NVMe/DMA pages, Android ARM64 16 KB pages, and 256×64 B SIMD cache lines.

---

## 2. Plane A — Bijective Canonical Frame (`COWR`)

### 2.1 Frame Header (8 bytes)
Fixed 8-byte canonical frame header carrying out-of-band trust and epistemic bytes:

| Offset | Size | Field | Rule |
|---:|---:|---|---|
| `0` | 4 | `magic` | `"COWR"` (`43 4F 57 52`) |
| `4` | 1 | `version` | `0x01` (reject any other with `ERR_UNSUPPORTED_VER`) |
| `5` | 1 | `authority` | Out-of-band authority + taint bitfield (§5) |
| `6` | 1 | `epistemic` | Epistemic class (`0x01` observed, `0x02` derived, `0x03` estimated) |
| `7` | 1 | `flags` | Must be `0x00` (`ERR_NON_CANONICAL_FLAGS`; compression is never identity) |

### 2.2 Header Key Dictionary
- `nkeys: uvarint` (minimal LEB128; trailing `0x00` on multi-byte varint -> `ERR_NON_CANONICAL_VARINT`).
- `nkeys` entries of `(klen: uvarint, utf8_bytes)`.
- Keys MUST be strictly ascending in **UTF-8 byte lexicographical order** (`kb[i] > kb[i-1]`; violations -> `ERR_UNSORTED_DICT` / `ERR_DUPLICATE_KEY`), valid UTF-8 (`ERR_INVALID_UTF8`), and **every dictionary key must be referenced at least once** (`ERR_UNUSED_DICT_KEY`).

### 2.3 Canonical Value Tags
| Tag | Type | Canonical Rule |
|---|---|---|
| `0x00` | `Null` | 1 byte |
| `0x01` | `False` | 1 byte |
| `0x02` | `True` | 1 byte |
| `0x40..0xBF` | `FIXINT` | Inline integer `0..=127` (`tag - 0x40`) |
| `0x03` | `Int` | Minimal zigzag `uvarint` for `i64` strictly outside `0..=127` (if `0..=127` -> `ERR_NON_CANONICAL_INT`) |
| `0x04` | `Float64` | IEEE-754 LE 8 bytes. `-0.0` forbidden (`+0.0` only). All NaNs canonicalize to `0x7FF8000000000000` (`ERR_NON_CANONICAL_FLOAT`) |
| `0x05` | `Str` | `uvarint(len)` + valid UTF-8 bytes |
| `0x06` | `Bytes` | `uvarint(len)` + raw bytes |
| `0x07` | `Array` | `uvarint(count)` + `count` values |
| `0x08` | `Map` | `uvarint(count)` + `count` pairs `(dict_idx: uvarint, value)` with strictly ascending `dict_idx` (`ERR_UNSORTED_MAP_KEYS` / `ERR_DUPLICATE_KEY`) |
| `0x09` | `Uint` | `uvarint` strictly `> i64::MAX` (`<= i64::MAX` MUST encode as `FIXINT`/`Int` -> `ERR_NON_CANONICAL_UINT`) |

### 2.4 Hardening Bounds (Mandatory for All Compliant Decoders)
- **`MAX_DEPTH = 64`:** Any nested container at depth `> 64` MUST reject immediately with `ERR_MAX_DEPTH`.
- **Byte-Bounded Allocation:** Wire counts (`nkeys`, array length, map length) MUST be validated against remaining buffer bytes before allocation (`ERR_ALLOC_LIMIT`).
- **No Trailing Bytes:** Decoder MUST consume exact frame length (`ERR_TRAILING_BYTES`).
- **Content Address (`CID`):** 34-byte multihash `0x12 0x20 || SHA-256(frame_bytes)`.

---

## 3. Shard Container (`CWRF`) & Plane C Layouts

### 3.1 Fixed 64-Byte Shard Header
```
[0..4)    magic        = "CWRF"
[4]       version      = 1
[5]       align_log2   = 14          // 2^14 = 16,384 B strictly
[6..8)    flags        = uint16 LE   // bit 0: StatePage shard; bit 1: Co-located IVF-INT8 shard; reserved bits = 0
[8..12)   frame_count  = uint32 LE
[12..16)  tensor_count = uint32 LE
[16..24)  plane_a_off  = uint64 LE   // = 64
[24..32)  plane_a_len  = uint64 LE
[32..40)  plane_c_off  = uint64 LE   // % 16,384 == 0
[40..48)  plane_c_len  = uint64 LE
[48..56)  footer_off   = uint64 LE
[56..64)  reserved     = 0           // must be 0
```
- All alignment padding bytes between `Plane A` and `Plane C`, and between consecutive `Plane C` regions, MUST be `0x00` (`ERR_NON_ZERO_PAD`).

### 3.2 Plane D — Merkle Footer & Seek-From-End Trailer
At `footer_off`:
1. `frame_count` entries of `(off: u64, len: u32, leaf_sha256: [u8; 32])` (44 bytes each; `leaf = SHA-256(0x00 || frame)`).
2. `tensor_count` entries of `(off: u64, len: u64)` (16 bytes each; `off % 16384 == 0`).
3. `merkle_root`: 34-byte RFC 6962 domain-separated, promote-odd (CVE-2012-2459 safe), count-bound multihash:
   - `leaf(f) = SHA-256(0x00 || f)`
   - `node(a, b) = SHA-256(0x01 || a || b)` (odd leaf promoted, never duplicated)
   - `root = 0x12 0x20 || SHA-256(0x02 || uvarint(N) || tree_digest)`
4. 12-byte EOF trailer: `footer_off: u64 LE || "CWRF"`.

### 3.3 Plane C Profile A — Hybrid SSM+KV `StatePage` (`flags & 0x01`)
Stores live recurrent SSM + attention KV state at `offset % 16384 == 0`, paired with a 256-byte `CWSP` header block inside Page 0 carrying:
- `model_sha` (64 hex chars / SHA-256 fingerprint of exact weights + file size)
- `doc_cid` (eternal `CoreHash` of source workspace)
- `ctx_tokens`, `kv_quant` (`0 = FP16`, `1 = Q4_0`, `2 = Q8_0`, `3 = F32`), `ssm_dtype` (`0 = FP16`, `1 = BF16`, `2 = FP32`)
- `hash_mode` (`0 = flat SHA-256`, `1 = parallel 4 MiB chunked RFC 6962 Merkle SHA-256`, deterministic across thread counts and hardware SHA-NI accelerated)
- `state_digest` (64 hex chars verified before calling runtime restore)
- Mandatory rejections: wrong model -> `ERR_MODEL_MISMATCH`; wrong KV quant mode -> `ERR_KV_QUANT_MISMATCH`; corrupt payload -> `ERR_STATEPAGE_HASH_MISMATCH`.

### 3.4 Plane C Profile B — Co-Located 16 KB IVF-INT8 Index (`CWIV`, `flags & 0x02`)
To eliminate serial split-seek NVMe stalls on multi-gigabyte corpora:
1. `centroids[K, D]` (`int8`, 16 KB-aligned)
2. `cluster_toc[K]` (`(disk_off: u64, disk_len: u64, vec_count: u32, reserved: u32)`, 16 KB-aligned)
3. `K` **co-located 16 KB-aligned cluster blocks**: each cluster block `c` packs `[embs_c (vec_count * D bytes) || meta_c (vec_count * 16 bytes) || 0x00 pad -> 16 KB]` contiguously so each `nprobe` read is **one** `O_DIRECT` (`QD=16`) submission directly into page-locked DMA memory.

---

## 4. Plane B — Deterministic Progressive Lenses (`R0`, `R1`, `R2`)

Lenses are pure functions of `Plane A` (zero model calls):
- **`R0` (Alpha32 Handle):** `@<6-char Alpha32 ID>` (lowercase ASCII letters `a-z`, avoiding BPE single-digit `\p{N}` splitting) + coarse size bucket (`xs`, `sm`, `md`, `lg`) + trust tag + title/topic. Target: `<= 12` tokens per doc.
- **`R1` (Structural Atlas):** Schema, authority, field types/lengths, array counts, tensor coordinate contracts (`<T:dtype[shape]@space>`).
- **`R2` (Adaptive Attention Lens):**
  - Uniform object arrays with **`len >= 3` AND schema key density `>= 0.70`** render as header-hoisted pipe tables (`name[N]{k1,k2,...}: ...`).
  - Sparse, deeply nested, or small arrays (`len < 3` or density `< 0.70`) fall back automatically to compact JSON (preventing negative crossover).
  - Raw tensors always project as compact Coordinate Contract handles (`<T:f32[256]@space>`).
  - Output wraps mixed-trust payloads inside per-session non-colliding boundary fences derived from `CID`: `<<<COWRIE:R2 ...>>> ... <<<END:COWRIE ...>>>`.

---

## 5. Plane D — Authority Bitfield & Causal Taint Membrane

| Bit / Value | Symbol | Meaning |
|---|---|---|
| `0x01` | `SYS_POLICY` | Owner/system instructions |
| `0x02` | `VERIFIED_TOOL` | Trusted local workspace / deterministic tool output |
| `0x04` | `UNTRUSTED_EXT` | External web, email, MCP, or unverified third-party bytes |
| `0x08` | `MODEL_SYNTH` | Model-synthesized summary or derived claim |
| `0x80` | `BIT_TAINTED` | Sticky transitive taint flag: set automatically on any frame derived from any parent having `(auth & 0x04 != 0)` OR `(auth & 0x80 != 0)` |

- Privileged execution gates (`shell_exec`, `cred_read`, `net_exfil`, `fs_write`) MUST reject any argument whose frame carries `(auth & (0x04 | 0x80)) != 0` unless an explicit owner-signed override capability is present.
