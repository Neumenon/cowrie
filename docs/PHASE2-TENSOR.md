# Phase 2 — Tensor Advantage (design brief, pre-decision)

## Goal
Make Cowrie the format you'd choose over **safetensors / msgpack / raw .npy** for AI tensor data —
by enabling **zero-copy, mmap-friendly, random-access** tensor reads **without** giving up the
one-canonical-byte-string identity (content address §3, Merkle file identity §7) we just proved
across four languages. The differentiator vs safetensors is: *the same zero-copy tensor access, but
inside a deterministic, content-addressed, tamper-evident, arbitrarily-nested envelope.*

## Where we are
A `Tensor` is a Core primitive: `dtype` + `shape` + a **contiguous little-endian `data` byte run**.
The data is already contiguous, BUT within a message its byte offset is unpredictable (it follows the
`COWR` header + dictionary + tag + `uvarint` shape dims) and is **unaligned**. So "zero-copy" today is
weak: you can slice the bytes, but not at a SIMD/page-aligned boundary, and not at a predictable place.

## Options
- **A — Read-side views (NON-breaking).** Add an API that locates each tensor's
  `(dtype, shape, data_offset, data_len)` within the canonical bytes, so a caller can `mmap` the
  buffer and wrap the data as a numpy/torch view with no copy. No wire change. Offsets stay
  unaligned/unpredictable. Cheap, immediately useful, keeps every existing golden/address/fingerprint.
- **B — Canonical alignment padding (WIRE CHANGE).** Pad so each tensor's `data` begins at a fixed
  alignment (e.g. 64 B) relative to a defined base; padding is zero-filled, **hashed**, and
  **verified** (exactly one canonical padding ⇒ determinism preserved). Enables true aligned mmap
  (safetensors-grade). BUT changes every tensor's canonical bytes ⇒ new content addresses + fingerprints.
  Pre-1.0, so permissible, but it is a real canonical-form epoch.
- **C — Staged.** Ship A now (views over the current contiguous layout); add B as an opt-in/again-canonical
  alignment before the 1.0 freeze, bundled with any other canonical-form changes (one epoch, not drip).

## Questions for the panel
1. Is alignment (B) actually worth the determinism complexity + wire change, or is contiguous-but-unaligned
   (A) enough for real-world zero-copy? (numpy `frombuffer` / torch `frombuffer` tolerate unaligned; SIMD
   kernels and some GPU upload paths prefer alignment.)
2. If B: alignment boundary (8 / 32 / 64 B?), relative to **what** base (frame start? file start? a dedicated
   tensor-data section?), and the exact canonical rule so there is exactly ONE valid padding.
3. In a FILE (§7), should tensors be **relocated into a packed, aligned tensor-data section** (safetensors-style:
   index + contiguous aligned blobs) instead of inline in frames? That gives the best mmap story but adds a
   second layout. Or keep tensors inline and just align in place?
4. Interaction with identity: does the content address hash the **padded** canonical bytes (yes, if padding is
   canonical)? Does padding break "exactly one canonical byte-string"? (No, if deterministic + verified.)
5. Do this **pre-1.0 now**, or ship A and defer B to the freeze epoch?

## DECISION (panel-reviewed: Gemini + DeepSeek, both converged on C)
**Option C — staged.** Ship **A now**; schedule **B (alignment)** as a deliberate pre-1.0 canonical epoch
with a firm deadline (not open-ended). Pinned by the panel:

- **A (now, non-breaking):** a gated, cross-language **tensor data-locator** — every implementation reports
  the same `(dtype, shape, data_offset, data_len)` for every tensor in a canonical message/file, and the
  bytes at that range equal the decoded tensor data. This is the zero-copy *read* foundation and a clean gate;
  it changes no bytes, addresses, or fingerprints.
- **B (pre-freeze epoch):** canonical alignment padding. Decisions locked by the panel: **64-byte boundary**;
  padding = `(64 − (offset mod 64)) mod 64` zero bytes immediately before each tensor's `data`, **verified**
  (non-zero or wrong-length ⇒ reject) so the padded form *is* the one canonical form (address hashes the
  padded bytes); **each tensor's data MUST fit in a single frame** (no multi-frame tensors — split large
  tensors into a chunk convention). **Open B-question to resolve at epoch time:** align *inline* (DeepSeek —
  base = frame-payload start, simplest identity) vs *relocate* tensors to a packed aligned data section
  (Gemini — best mmap/GPU story + Merkle-DAG lazy verify, but two-level layout and the "composition alignment
  trap" under nesting). Both agree unaligned (A alone) is **insufficient** for the GPU/SIMD zero-copy claim,
  so B is required before 1.0 — it just shouldn't block A.

This file is the standing record of that decision; A is built first.
