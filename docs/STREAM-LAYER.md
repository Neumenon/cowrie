# Cowrie Stream / Dataset Layer — design brief (pre-build, HIGH RIGOR)

## Why this needs care
This is the top of the identity DAG: **value → frame → file (§7) → dataset**. Today only the bottom three
are cross-language-gated; the dataset layer exists as a *Python-only* `dataset_root` helper with no parity
gate and no adversarial review. Streaming is where complexity hides (incremental production, lazy/partial
verification, ordering, location-vs-identity), so it gets the same treatment as Core: design → panel →
reference → cross-language → gate → adversarial review.

## What we already have (and its limits)
- §7 **files are SEALED** (footer written once; no append). Random access *within* a file via the footer.
- `profiles.dataset_manifest(shards)` → `Object{shards:[{uri, merkle_root, count}], root}` where
  `root = dataset_root(shard_roots) = file.merkle_root([34-byte file multihashes])`. **Python only, ungated.**

## Decisions to pin (this brief proposes; the panel + review stress-test)
1. **No new append-able format.** A "stream" of data = an **ordered sequence of sealed shard files** plus a
   manifest. Streaming *write* = seal each shard as it completes and (re)issue the manifest; each manifest
   version is itself an immutable, content-addressed artifact. (Matches §7's deliberate sealed choice; avoids
   an unsealed-stream format with weaker identity.)
2. **Dataset identity = `dataset_root`**, the Merkle root over the **ordered** shard file-roots — NOT the
   manifest's content address. Rationale: `dataset_root` is **location-independent** (same data at a
   different `uri` ⇒ same dataset identity), whereas the manifest's content address binds `uri` too. The
   manifest *commits to* `dataset_root` and the shard list; verifying the manifest is integrity, identifying
   the data is `dataset_root`.
3. **Merkle construction reused, not reinvented.** `dataset_root` = the §7 `merkle_root` over the list of
   34-byte file multihashes as leaves (RFC-6962 `0x00/0x01` domains, promote-odd, **count-bound** `0x02`).
   Leaves are themselves multihashes — fine; domains + count-binding still prevent ambiguity. **Ordered**
   (datasets have shard order); reorder ⇒ different dataset.
4. **Lazy / partial verification protocol (the streaming payoff).** Given the manifest + ONE shard you can
   verify, without fetching the rest: (a) the shard file decodes and its §7 Merkle root equals the manifest's
   listed `merkle_root` for that shard; (b) `dataset_root(listed roots) == manifest.root`. So a consumer
   streams shard-by-shard, verifying each against a single trusted `dataset_root`.
5. **Large tensors that exceed a file** are a **chunk convention** (a profile), NOT a core change: an ordered
   list of tensor-chunks across shards with a `{chunks:[...], shape, dtype}` header. (Multi-frame tensors stay
   forbidden, §2.5.) Flagged here; built only if demand — do not block the dataset layer on it.

## The hard questions for the panel (stress these)
1. Is "sealed shards + immutable manifest versions" genuinely sufficient for *streaming* (incremental
   produce/consume of multi-GB datasets), or is a real append-able stream format actually needed? What
   breaks at scale (millions of shards; a manifest too big to hold; updating one shard re-hashing the root)?
2. Identity = `dataset_root` (location-independent) vs `content_address(manifest)` — is that the right split,
   and does anything force the opposite?
3. Merkle-of-multihashes: any second-preimage / cross-shard ambiguity from leaves being 34-byte hashes rather
   than raw bytes? Is binding `count` enough, or must we also bind per-shard `count`/`uri`/order index?
4. The lazy-verification protocol — any way a malicious producer makes a shard verify against the manifest
   while the manifest verifies against a `dataset_root` the consumer trusts, yet the data is wrong?
5. Cross-language risk: `dataset_root` must be byte-identical in Go/Rust/TS (it's new Merkle computation like
   §7 was). What's the most likely divergence, and what golden fixtures pin it?
6. What would we **regret freezing** here? (manifest schema rigidity, no manifest-of-manifests for huge
   datasets, ordering semantics, updatability.)

## DECISIONS (panel-reviewed: Gemini + DeepSeek)
- **Sealed-shards + immutable-manifest is sufficient for streaming** (streaming ≠ append-able format). Kept.
- **The "regret freezing" concerns (manifest-of-manifests, versioning, insert/remove/reorder) are deferred
  safely** because the manifest is a **PROFILE over Core, and profiles are NOT frozen at 1.0** — only Core
  wire tags are. So a hierarchical/versioned manifest is a *post-1.0 profile evolution*, not a freeze blocker.
  The dataset-layer code is marked evolvable; we only harden the identity primitive + verify protocol now.
- **`dataset_root` reuses the §7 Merkle** (RFC-6962 `0x00/0x01`, promote-odd, **count-bound `0x02 || uvarint(n)`**)
  over the **ordered** 34-byte file-roots. It is order-sensitive (reorder ⇒ different root) and count-bound, so
  the panel's reorder/position concern is covered by the *order-sensitive tree + the consumer recomputing the
  full root* — no new per-leaf position field needed (and the §7 Merkle is already cross-language gated).
- **LAZY-VERIFY PROTOCOL (normative):** a consumer (1) downloads the manifest's list of 34-byte shard roots,
  recomputes `dataset_root(roots)` and checks it equals the trusted `dataset_root` — this binds **order + count**;
  then (2) for each shard it actually wants, downloads that shard file and checks its §7 Merkle root equals the
  listed root. Tampering or reordering any shard ⇒ step (1) or (2) fails. Adversarially tested.
- **Identity = `dataset_root`** (location-independent); the manifest *commits to* it and carries `uri`/`count`
  metadata (so `content_address(manifest)` is the location-specific record, `dataset_root` is the data identity).

## Build plan (after panel)
spec §8 stream/dataset → rigorous reference (incl. a `verify_shard` lazy-check) + golden dataset fixtures →
cross-language `dataset_root` in Go/Rust/TS → a `dataset_identity_gate.py` (all four agree on `dataset_root`;
lazy verification accepts good shards and rejects tampered ones) → adversarial review → wire into CI.
