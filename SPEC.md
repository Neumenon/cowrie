# Cowrie Wire & AI Container Specification (v1.2.0)

**Status:** Frozen v1.2 (`2026-11-23`) — 100% Plane A `COWR v1` golden-CID compatible; adds 16 KB Zone-Map shards (`CWZM`), crash-safe hash-chained `.cwwal` + `LiveIvfStore`, symmetric 4-runtime KV/StatePage + WAL + `Ed25519` parity (`vLLM` `CowrieKVConnectorV1`, `llama.cpp`, `Eureka LLM`, `Eureka Creative v7`), pure-Rust RFC 8032 `Ed25519` owner-declassification tokens (`COWRIE_DECLASSIFY_V1`), transparent stdio JSON-RPC `cowrie mcp-proxy`, zero-import `wasm32-unknown-unknown` (`80.7 KiB` gzipped), and `26/26` machine-checked `Bend 2.0.35 --verdict` formal invariants (`formal/PROOF.bend`) on top of all `v1.1` hardening.  
**Lineage:** Successor to legacy `Neumenon/cowrie-legacy` canonical codec, re-architected for tri-consumer AI runtimes.  
**Scope:** Bijective canonical binary frames (`Plane A`), deterministic progressive attention lenses (`Plane B`), 16 KB-aligned zero-copy tensor / `StatePage` / co-located IVF arenas (`Plane C`), and out-of-band authority + causal taint + `Ed25519` declassification + 16 KB Zone-Maps + RFC 6962 Merkle seals (`Plane D`).

---

## 1. Architectural Principles

- **CoreHash** commits strictly to `Plane A` canonical bytes, raw `Plane C` data payloads, and `Plane D` trust metadata.
- **Satellites** (`StatePages`, model embeddings, learned `R0` synopses, `CWIV` vector clusters) are bound to `(core_hash, model_sha, runtime_id, kv_quant, ssm_dtype)` outside `CoreHash`, preserving permanent document CIDs across model upgrades.
- **Transport Compression** (`zstd`, `lz4`) operates at the transport layer outside canonical frame identity (`flags == 0x00`).
- **Universal Page Alignment:** `PAGE_ALIGN = 16,384` bytes (`align_log2 = 14`), matching Apple Silicon 16 KB Unified Memory pages, Linux 4×4 KB NVMe/`O_DIRECT` DMA pages, Android ARM64 16 KB pages, and 256×64 B SIMD cache lines.

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
- Keys MUST be strictly ascending in **UTF-8 byte lexicographical order** (`kb[i] > kb[i-1]`; violations -> `ERR_UNSORTED_DICT` / `ERR_DUPLICATE_KEY`), valid UTF-8 (`ERR_INVALID_UTF8`), and **every dictionary key must be referenced at least once** (`ERR_UNUSED_DICT_KEY`, tracked via stack-inline bitset).

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
| `0x06` | `Bytes` | `uvarint(len)` + raw bytes (lossless JSON sentinel: `{"$bytes":"<hex>"}`) |
| `0x07` | `Array` | `uvarint(count)` + `count` values |
| `0x08` | `Map` | `uvarint(count)` + `count` pairs `(dict_idx: uvarint, value)` with strictly ascending `dict_idx` (`ERR_UNSORTED_MAP_KEYS` / `ERR_DUPLICATE_KEY`) |
| `0x09` | `Uint` | `uvarint` strictly `> i64::MAX` (`<= i64::MAX` MUST encode as `FIXINT`/`Int` -> `ERR_NON_CANONICAL_UINT`) |
| `0x0A` | `TensorRef` | Native Coordinate Contract: `arena_idx: uvarint (u32)` + `dtype: u8 (0..=8)` + `ndim: u8 (0..=8)` + `dims: uvarint[ndim]` + `space_len: uvarint (1..=256)` + UTF-8 `space`. Checked against `Plane C` tensor count and byte footprint (`ERR_INVALID_CONTRACT` / `ERR_ALLOC_LIMIT`). |

### 2.4 Hardening Bounds (Mandatory for All Compliant Decoders)
- **`MAX_DEPTH = 64`:** Any nested container at depth `> 64` MUST reject immediately with `ERR_MAX_DEPTH`.
- **Zero-Allocation Wire Verification:** `verify_frame_zero_alloc` validates every canonical rule with zero heap allocations (`[u64; 4]` stack bitmask for `<= 256` keys).
- **Byte-Bounded Allocation:** Wire counts (`nkeys`, array length, map length, tensor dims) MUST be validated against remaining buffer bytes and arithmetic overflow before allocation (`ERR_ALLOC_LIMIT`).
- **Strict RFC 8259 JSON Bridge:** Rejects lone/mispaired UTF-16 surrogates (`\uD800\u0041`, `\uDC00`), unescaped `< 0x20` control bytes, duplicate object keys (`ERR_DUPLICATE_KEY`), and leading-zero numbers (`01`, `-01`); preserves whole floats (`1.0`), non-finite floats (`{"$float":"NaN"|"Infinity"|"-Infinity"}`), `Bytes` (`{"$bytes":"..."}`), `TensorRef` (`{"$tensor":...}`), and literal maps containing reserved `$` keys (`{"$map":{...}}`) losslessly.
- **No Trailing Bytes:** Decoder MUST consume exact frame length (`ERR_TRAILING_BYTES`).
- **Identity Scopes (`frame_cid` vs `resolved_frame_cid` vs `shard_root`):**
  - `frame_cid`: 34-byte multihash `0x12 0x20 || SHA-256(frame_bytes)` — commits strictly to the canonical Plane A frame (including `TAG_TENSOR_REF` descriptors, NOT the Plane C arena bytes).
  - `resolved_frame_cid(shard, frame_idx)`: repack-stable 34-byte multihash `0x12 0x20 || SHA-256("COWRIE_RESOLVED_FRAME_V1\0" || frame_cid || ref_count || (arena_idx, dtype, shape, tensor_len, tensor_merkle_4m_sha256)*)` — commits to both the descriptor AND every referenced Plane C tensor's bytes; use this as cache/dedup identity for tensor-bearing frames.
  - `shard_root`: 34-byte dual-tree RFC 6962 Merkle root over all Plane A frames and Plane C tensors in a specific `CWRF` shard.
- **Legacy v0 Header Compatibility Note:** The archived v0 polyglot codec (`Neumenon/cowrie-legacy`) also prefixed frames with `"COWR\x01"` followed immediately by `flags` and `dict_count` (without `authority`/`epistemic` header bytes). Do not auto-dispatch v0 and v1.x frames by magic alone; any future breaking wire revision MUST bump `version` to `0x02`.

---

## 3. Shard Containers (`CWRF`, `CWZM`, `CWAL`) & Plane C Layouts

### 3.1 Fixed 64-Byte `CWRF` Shard Header
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
- All alignment padding bytes between `Plane A` and `Plane C`, and between consecutive `Plane C` regions, MUST be `0x00` (`ERR_NON_ZERO_PAD`), and `FrameEntry` / `TensorEntry` slices MUST partition `[64 .. 64 + plane_a_len)` and `[plane_c_off .. plane_c_off + plane_c_len)` contiguously with zero unhashed slack bytes.

### 3.2 Plane D — Dual-Tree Merkle Footer & Seek-From-End Trailer
At `footer_off`:
1. `frame_count` entries of `(off: u64, len: u32, leaf_sha256: [u8; 32])` (`44` bytes each; `leaf = SHA-256(0x00 || frame)`).
2. `tensor_count` entries of `(off: u64, len: u64, merkle_4m_sha256: [u8; 32])` (`48` bytes each; `off % 16384 == 0`; `tensor_leaf = SHA-256(0x03 || uvarint(len) || merkle_4m_sha256)`).
3. `merkle_root`: 34-byte RFC 6962 domain-separated, promote-odd (CVE-2012-2459 safe), count-bound multihash:
   - Frame-only shards (`T == 0`): `0x12 0x20 || SHA-256(0x02 || uvarint(F) || frame_tree)`
   - Tensor-bearing shards (`T > 0`): `0x12 0x20 || SHA-256(0x04 || uvarint(F) || frame_tree || uvarint(T) || tensor_tree)`
4. 12-byte EOF trailer: `footer_off: u64 LE || "CWRF"`.

### 3.3 Plane C Profile A — Hybrid SSM+KV `StatePage` (`CWSP`, `flags & 0x01`)
Stores live recurrent SSM + attention KV state at `offset % 16384 == 0`, paired with a `276`-byte `CWSP` descriptor inside Page 0 (`[64..340)`):
- `model_sha` (64 lowercase hex chars via `cowrie_model_fingerprint`: full-file SHA-NI for `<= 16 MiB`; `file_size + 64 × 64 KiB` strided windows for `> 16 MiB`)
- `doc_cid` (eternal `CoreHash` or block-aligned prefix digest, 64 lowercase hex chars)
- `runtime_id` (`[288..304)`, up to 16 ASCII bytes: `"llama.cpp"`, `"eureka-llm"`, `"eureka-creative"`, `"vllm-v1"`; when a caller specifies a non-empty `expected_runtime_id`, an empty or mismatched file `runtime_id` MUST fail closed with `ERR_RUNTIME_MISMATCH`) + `layout_ver: u16` (`[304..306)`)
- `ctx_tokens`, `kv_quant` (`0 = FP16/BF16`, `1 = Q4_0`, `2 = Q8_0/FP8`, `3 = F32`), `ssm_dtype` (`0 = FP16`, `1 = BF16`, `2 = FP32`)
- `hash_mode` (`0 = flat SHA-256`, `1 = parallel 4 MiB chunked RFC 6962 Merkle SHA-256` over bounded `WorkerPool`, accelerated by x86_64 `SHA-NI` and ARMv8 `SHA2`)
- Optional **RFC 2104 HMAC-SHA256** tag (`[308..340)`, `hmac_flag = 1` at `page0[75]` when `COWRIE_HMAC_KEY` is configured)
- **HMAC Error Precedence:** When `hmac_flag == 1` (or caller provides an HMAC key), RFC 2104 `HMAC-SHA256` verification over Page-0 `[0..308)` MUST execute on a stack-snapshotted copy BEFORE semantic header parsing (`ERR_AUTH_MAC_MISMATCH` precedes `ERR_INVALID_CONTRACT` / `ERR_MODEL_MISMATCH`).
- Supports both zero-copy `mmap` (`MappedStatePage`) and 16 KB-aligned `O_DIRECT` into double-buffered `cudaHostAlloc` pinned memory overlapped with dual-stream `cudaMemcpyAsync`.

### 3.4 Plane C Profile B — Co-Located 16 KB IVF-INT8 Index (`CWIV`, `flags & 0x02`) & `LiveIvfStore`
1. `centroids[K, D]` (`int8`, 16 KB-aligned)
2. `cluster_toc[K]` (`(disk_off: u64, disk_len: u64, vec_count: u32, reserved: u32)`, 16 KB-aligned, bounds-checked against `file_len`)
3. `K` **co-located 16 KB-aligned cluster blocks**: `[embs_c || meta_c || 0x00 pad -> 16 KB]` queried over `WorkerPool` with `AVX2`/`NEON` signed INT8 dot-product kernels and `0x04 | 0x80` trust filtering.
4. **`LiveIvfStore` (`v1.2`):** Pairs an immutable `CWIV` base generation with a `.cwwal` SIMD delta tail (`WAL_KIND_IVF_VEC = 2`) and bounded-memory streaming compaction (`compact_to_cwiv()`). Compaction writes immutable `<base>.gen.<N>.cwiv` one cluster at a time, atomically publishes an 88-byte SHA-256-authenticated generation manifest `<base>.cwgen` (`"CWG1"`, `gen_id`, `applied_wal_seq_exclusive`, `base_gen_sha256`), and then atomically resets `.cwwal` to `CWAL v2` starting at monotonic `base_seq = applied_wal_seq_exclusive`. Recovery skips WAL entries with `seq < applied_wal_seq_exclusive`, guaranteeing exact multiplicity across crashes at any step.

### 3.5 Plane D Profile C — 16 KB Page-Targeted Scalar Zone-Map Shards (`CWZM`, `v1.2`)
Packs sorted canonical `COWR` frames into self-verifying `16,384`-byte leaf pages (`[count: u16, res: u16, slots[count] * 20B, frames...]`) and appends a `16 KB`-aligned Plane D Zone-Map directory of **64-byte cache-line entries**:
- `(min_key: i64, max_key: i64, min_sec: i64, max_sec: i64, leaf_sha256: [u8; 32])` (`64 B` per 16 KB leaf page), enforcing monotonic primary-key fences (`z[p].min_key <= z[p].max_key` and `z[p].max_key <= z[p+1].min_key`).
- Header commits to the RFC 6962 Merkle root over full 64-byte Zone-Map entries (`SHA-256(0x00 || zm_entry_64B)`, `hdr[48..82)`), authenticating both fence bounds and leaf digests.
- Point and narrow-range queries prune non-overlapping pages in L1 cache and `pread` (`read_exact_at`) matching 16 KB pages, verifying each touched page's `leaf_sha256` via hardware SHA-NI and filtering `(auth & (0x04 | 0x80)) != 0` in `O(1)`.

### 3.6 Crash-Safe Hash-Chained Framed WAL (`CWAL` / `.cwwal`, `v1.2`)
Append-only `0600` mutation and audit log protected by `flock(LOCK_EX)`, beginning with a 32-byte `"CWAL"` genesis header (`ver=1` starting at `seq=0`, or `ver=2` carrying monotonic `base_seq: u64 LE` across compactions) followed by `"CWLR"` records:
- `[0..4) "CWLR" || [4] kind (0=Frame, 1=Tombstone, 2=IvfVec) || [5..8) 0 || [8..16) seq: u64 LE || [16..20) len: u32 LE || [20..52) prev_sha256 || payload || record_sha256`
- `open(..., recover_torn_tail = true)` truncates incomplete or zero-filled power-loss tail writes cleanly and rejects any mid-log tampering, unknown `kind > 2`, or sequence gap (`ERR_ROOT_MISMATCH` / `ERR_CORRUPT_TRAILER` / `ERR_INVALID_CONTRACT`).
- Live-frame replay (`live_frames`) is sequential: `Tombstone(cid)` revokes prior `Frame(cid)` records while preserving other frames, and a subsequent `Frame(cid)` at a higher `seq` restores `cid` to the live set.

---

## 4. Plane B — Deterministic Progressive Lenses (`R0`–`R3`) & Exact Field Extraction

Lenses are pure functions of `Plane A` (zero model calls); any `tier > 3` fails closed (`ERR_INVALID_CONTRACT`):
- **`R0` (Alpha32 Handle):** `@<10-char Alpha32 ID>` (lowercase ASCII letters `a-z`, $26^{10} \approx 2^{47}$ space, birthday bound `~12M` records) + coarse size bucket + exact `authority_label` (`SYS_POLICY`, `VERIFIED_TOOL`, `MODEL_SYNTH`, `UNTRUSTED_EXT`, `MIXED|UNTRUSTED`) + title/topic.
- **`R1` (Structural Atlas):** Schema, authority, field types/lengths, array counts, typed tensor Coordinate Contracts (`<T:dtype[shape]@space>`), and `<T:f64[N]>` for dense float64 vectors (`len >= 16`).
- **`R2` (Adaptive Attention Lens) & `R2-Compact`:** Hoists uniform object tables when `len >= 3` and key density `>= 0.70`, rendering explicit distinct markers for `null`, `<missing>` absent columns, `""` empty strings, and quoted literal `"null"`/`"<missing>"` strings; keeps integer ID arrays exact inline (eliding only float64 vectors as `<T:f64[N]@inline_elided>`); renders `TAG_TENSOR_REF (0x0A)` as `<T:dtype[shape]@space>`; wraps output in CID-bound fences with 3-state Mealy transducer escaping plus zero-width/Bidi/Tag/ANSI stripping and `<|`/`|>` neutralization. `project_r2_compact` (`cowrie_project_r2_compact` / `--tier r2-compact`) uses a compact single-line header `[@handle|AUTHORITY]` for clean `SYS_POLICY`/`VERIFIED_TOOL` frames and fails closed to the full Mealy fence for `UNTRUSTED_EXT`, `MODEL_SYNTH`, or `BIT_TAINTED` frames.
- **`R3` (Lossless Extraction) & `extract_field`:** `tier = 3` (`project_r3`) returns 100% round-trip-lossless canonical typed JSON (preserving `Bytes`, `TensorRef`, `NaN`/`±Infinity`, whole floats, and literal `$` map keys via `$map`). `extract_field(frame, path)` (`cowrie_extract_field` / `cowrie get`) resolves exact typed values by RFC 6901 JSON Pointer (`/rows/2/id`) or dot-bracket selector (`rows[2].id`), failing closed with `ERR_INVALID_CONTRACT` on absent keys or out-of-bounds indices.
- **Pre-Append MCP Virtualization (`mcp-proxy --virtualize` / `--virtualize-bytes <N>`):** Seals oversized MCP responses (`>= 16,384 B` default) into `.cwwal`, replaces `result.content` with a bounded `VIRTUALIZED_TOOL_OUTPUT` structural/text atlas (`project_virtual_atlas`, Mealy-fenced when untrusted), injects read-only `cowrie_get(handle, pointer, grep, lines, offset, max_chars)` (`query_frame_slice`) served locally in `<1 ms`, and preserves `BIT_TAINTED (0x80)` across every slice read.

---

## 5. Plane D — Authority Bitfield, Causal Taint, `Ed25519` Declassification & MCP Wire Proxy

| Bit / Value | Symbol | Meaning |
|---|---|---|
| `0x01` | `SYS_POLICY` | Owner/system instructions |
| `0x02` | `VERIFIED_TOOL` | Trusted local workspace / deterministic tool output |
| `0x04` | `UNTRUSTED_EXT` | External web, email, search, browser, or unverified MCP bytes |
| `0x08` | `MODEL_SYNTH` | Model-synthesized summary or derived claim |
| `0x80` | `BIT_TAINTED` | Sticky transitive taint flag: set automatically on any frame derived from any parent having `(auth & (0x04 | 0x80)) != 0` |

- **Fail-Closed Privileged Gates:** `shell_exec`, `cred_read`, `net_exfil`, and `fs_write` fail closed (`ERR_TAINTED_EXECUTION_BLOCKED`) on tainted, unknown, empty, or colliding argument IDs.
- **Scoped Asymmetric Declassification (`COWRIE_DECLASSIFY_V1`, `v1.2`):** Pure-Rust RFC 8032 `Ed25519` signatures over `"COWRIE_DECLASSIFY_V1\0" || target_cid_34 || action_u8 || expiry_u64_le` (`64 B` message, strict `S < L` and canonical curve-point verification) allow an operator's public key to approve one specific reviewed 34-byte CID for one specific `ToolAction` before `expiry_unix_secs` without sharing symmetric keys or approving any other tainted CID in context.
- **Transparent MCP Wire Firewall (`cowrie mcp-proxy`, `v1.2`):** Wraps unmodified stdio JSON-RPC MCP servers, logging tool responses into `.cwwal`, sealing external fetch/search/browser/email results as `0x04 UNTRUSTED_EXT`, and blocking tainted privileged `tools/call` messages at the wire (`-32025 ERR_TAINTED_EXECUTION_BLOCKED`) unless covered by valid `Ed25519` `_cowrie_declassify` tokens.

---

## 6. Deterministic Status ABI (`ERR_*` Codes `0..=33`)

`0 = OK`, `1..=29` unchanged from v1.0, plus v1.1/v1.2 security extensions:
- `30 = ERR_SSM_DTYPE_MISMATCH`
- `31 = ERR_RUNTIME_MISMATCH`
- `32 = ERR_AUTH_MAC_MISMATCH` (covers both `CWSP` `HMAC-SHA256` and `Ed25519` `COWRIE_DECLASSIFY_V1` signature/expiry failures)
- `33 = ERR_INTERNAL` (FFI panic-guard trap; never unwinds across `extern "C"`)
