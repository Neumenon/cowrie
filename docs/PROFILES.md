# Cowrie Profiles — what each addresses & where it fits

**Ground rule (from the lean doctrine):** a profile is a **convention over the locked Core** — an
agreed `Object`/`Array`/`Tensor` shape with named fields — and carries **no new wire tag**. Core is
physics; profiles are chemistry. Every profile therefore inherits, for free, the things Core already
proves: one canonical byte-string, a content address (multihash SHA-256), a structural fingerprint
(route/dedup by shape), file-level Merkle identity, zero-copy aligned tensors, and byte-identical
behavior across Python/Go/Rust/TS.

**Honest framing of "market":** these are *positioning hypotheses*, not validated demand. The value
is engineering — **deterministic identity for AI data** — so each section names the category, the real
incumbents, and the specific **wedge** where Cowrie’s identity/determinism beats "just use the incumbent."

---

## Verdict — measured demos + adversarial panel (judged on usefulness, NOT adoption)
Validated with `tools/demos/profile_value.py` (real numbers) and an adversarial Gemini+DeepSeek panel.
**Adoption is uphill for every format — so adoption difficulty is discounted; the only question is "is it
extremely useful."** Size is *not* the pitch and is not optimized yet (1.4× vs JSON) — correctness first,
optimize later.

| Profile | Verdict | Why (effectiveness, not adoption) |
| --- | --- | --- |
| **Embedding** | ✅ **proven, high** | canonical dedup JSON can't do (700/1000 unique), O(1) drift, cross-lang cache |
| **Dataset manifest** | ✅ **proven, high** | one verifiable Merkle hash for a whole sharded dataset; verify a shard without the rest |
| **Trace** | ✅ **kept** | byte-exact eval-CI reproducibility is real (OTLP "exists" is an *adoption* point, discounted) |
| **Graph** | ✅ **kept — FIXED** | had a real correctness bug (reordered nodes → different hash); now nodes/edges are sorted by canonical encoding so the order-independent identity promise actually holds (verified) |
| **Packed tensor-file** | ⏳ deferred (useful on merit) | content-identity + Merkle over aligned weights = real supply-chain integrity; not yet built |
| **Media** | 🔬 **revisit later** | today thin (Core `Bytes` already content-addresses); kept for investigation, not blessed |
| **TrainingBatch / Eval** | ➖ ignored (low) | batches are produced-once/consumed-once — content-addressing solves a non-problem |

The one item that needed a fix regardless of keep/cut was **Graph** — an effectiveness bug, now closed.
No invented dollar figures.

---

## 1. Embedding
**What it is.** `Object{model, dim, dtype, vector: Tensor, id?, meta?}` — and a stream of them as a
Cowrie file of frames. The vector is a Core `Tensor`, so it’s 64-byte aligned and zero-copy.

**Addresses.** Vector storage/transport for RAG and semantic search: today embeddings move as ad-hoc
JSON arrays (huge, lossy on floats), bespoke `.npy`/parquet, or DB-specific blobs — none content-addressed,
none portable with their `(model, dim, dtype)` provenance attached and verifiable.

**Cowrie’s wedge.** (a) **Dedup & cache by content address** — identical embedding ⇒ identical hash, so
a vector computed twice is stored once and a cache hit is a hash compare. (b) **Drift/route by structural
fingerprint** — every record of a given `(model, dim, dtype)` shares a fingerprint; a stream whose
fingerprint changes flags a model swap. (c) **Zero-copy** load of the aligned vector tensor.

**Market.** Vector-DB / RAG infrastructure (Pinecone, Weaviate, Qdrant, Milvus, pgvector) and the
embedding pipelines feeding them. Cowrie isn’t the database — it’s the **deterministic interchange &
cache layer** between embedder and store, where content-addressed dedup and provenance are the wedge.

## 2. Media (Image / Audio / Video)
**What it is.** Opaque-blob envelopes only: `Object{kind, format, bytes: Bytes, width?, height?,
sample_rate?, ...}`. Cowrie does **not** model codecs — the pixels/samples stay an opaque `Bytes` (or
`Tensor` for decoded frames); only the metadata is structured.

**Addresses.** Multimodal training/eval data: images, audio, video clips that need to travel with their
metadata and a stable identity, deduplicated across a dataset, without a codec dependency in the format.

**Cowrie’s wedge.** **Content-addressed media** — the same image is one hash no matter how many samples
reference it, so a multimodal dataset dedups for free and every sample’s media is tamper-evident. The
envelope is honest (no fake video-codec semantics to drift on), which is exactly what the spec’s §0.1
admission test enforces.

**Market.** Multimodal dataset tooling and training-data pipelines (the world around HuggingFace
`datasets`, WebDataset, MosaicML Streaming, LAION-style corpora). Wedge: dedup + verifiable provenance
of media inside a deterministic envelope, vs tar/parquet shards with no identity.

## 3. Graph (Node / Edge)
**What it is.** `Object{nodes: Array<Object{id, labels, props}>, edges: Array<Object{src, dst, type,
props}>}` — graphs as Core objects/arrays, no graph wire variant (those were deliberately deleted).

**Addresses.** Knowledge graphs, GraphRAG, and **agent memory graphs** that need a stable, comparable
identity and reproducible snapshots — today these live in property-graph DBs or ad-hoc JSON with no
canonical form, so "same graph?" is undecidable byte-wise.

**Cowrie’s wedge.** A **canonical, content-addressed graph snapshot**: two graphs with the same content
hash to the same address regardless of construction order (keys are globally byte-sorted in Core). Diff,
cache, and version graphs by hash; sub-graphs (frames) get their own addresses for partial verification.

**Market.** GraphRAG / knowledge-graph tooling (Neo4j, the LangChain/LlamaIndex graph layers) and
agent-memory systems. Wedge: deterministic, hashable graph snapshots for caching and reproducibility —
the interchange/snapshot format, not the query engine.

## 4. Trace (agent / LLM execution)
**What it is.** `Object{trace_id, spans: Array<Object{name, start, end, kind, input, output,
tool_calls, tokens, ...}>}` — OpenTelemetry-shaped spans expressed over Core, with tensors (logits,
embeddings) inline as aligned `Tensor`s where present.

**Addresses.** LLM/agent **observability and reproducible eval**: traces today are JSON in vendor SaaS,
not portable, not content-addressed, hard to diff or replay deterministically.

**Cowrie’s wedge.** **Content-addressed, replayable traces** — a trace has one canonical byte-string and
one hash, so eval runs are reproducible and comparable by identity, traces dedup, and tamper-evidence is
free via the file Merkle root. The structural fingerprint lets you bucket traces by shape.

**Market.** LLM observability / eval (LangSmith, Langfuse, Arize Phoenix, Braintrust, W&B Weave). Wedge:
a vendor-neutral, deterministic, hashable trace artifact for reproducible eval and cross-tool portability.

## 5. Eval & TrainingBatch (lightweight schemas)
**What it is.** `Eval`: `Object{suite, cases: Array<Object{input, expected, got, score}>}`. `TrainingBatch`:
`Object{features: Tensor, labels: Tensor, meta}` — aligned tensors, zero-copy into the trainer.

**Addresses.** Reproducible eval suites and deterministic training-batch interchange/caching.

**Cowrie’s wedge.** Hash-addressed eval suites (same suite ⇒ same hash ⇒ trustworthy regression compares)
and zero-copy batches whose identity is stable, so a batch cache is keyed by content.

**Market.** Eval frameworks and training data loaders (PyTorch `DataLoader`, MosaicML Streaming, Ray Data).
Wedge: deterministic, content-addressed batch/eval artifacts.

## 6. Dataset / Stream layer (DatasetManifest + Shards)
**What it is.** The deferred multi-file layer: a `DatasetManifest` `Object{shards: Array<Object{uri,
merkle_root, count}>, root}` whose identity is a Merkle root **over the per-file Merkle roots** (§7).

**Addresses.** Large sharded datasets (millions of records across thousands of files) that need a single
verifiable dataset identity, content-addressed shards, and reproducible versioning.

**Cowrie’s wedge.** **One hash for an entire dataset**, composed from file roots composed from frame
roots — a true Merkle DAG. Verify a dataset, dedup shards, and pin a dataset *version* by a single
address; lazily verify any shard/frame without reading the rest.

**Market.** ML dataset versioning & data-lake tooling (HuggingFace Hub, DVC, LakeFS, Pachyderm, Iceberg
for ML). Wedge: native, cross-language, deterministic dataset identity — content-addressing as a
first-class property rather than bolted on.

## 7. Packed tensor-file profile (optional — safetensors replacement)
**What it is.** A file layout that **relocates** tensors into a contiguous 64-byte-aligned data section
with a fat-pointer index (the Gemini design in `docs/PHASE2-TENSOR.md`) — a pure model-weights container.

**Addresses.** Model weight storage/distribution where you want safetensors’ flat mmap-everything layout
*plus* Cowrie identity.

**Cowrie’s wedge.** safetensors gives aligned zero-copy mmap but **no content identity, no nesting, no
cross-format determinism**. This profile gives the same zero-copy access **plus** a content address and
Merkle identity per tensor and per file — verifiable, dedupable model weights.

**Market.** Model hubs and weight distribution (HuggingFace `safetensors`, GGUF). Wedge: drop-in zero-copy
weights with built-in content-addressed identity & tamper-evidence — useful for supply-chain integrity of
model weights, a real and growing concern.

---

## The through-line
Every profile sells the **same** Core property in a different vertical: *deterministic, content-addressed
identity for AI data, with zero-copy tensors, identical in four languages.* Cowrie is rarely the database,
the trainer, or the observability SaaS — it’s the **deterministic interchange & identity layer** beneath
them. The honest bet is **engineering value** (dedup, caching, reproducibility, tamper-evidence,
portability), earned by the gated guarantees — not a TAM story. Profiles are added only when they pass the
§6.4 simulation gate (expressible over the frozen Core with **no new wire tag**); anything that can’t is a
signal to fix Core *before* 1.0, not to grow the wire format after.
