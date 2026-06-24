# Cowrie — "lean version" design doctrine (proposed new approach)

> The AIM is unchanged: Cowrie = the deterministic AI-native data rail — typed artifacts, tensors,
> embeddings, streams, replay records moving through ML systems without JSON loss. The APPROACH below
> is a NEW suggestion for how to structure/sequence the build. Review it against the current state.

## Thesis
Build a small set of primitives so strong that future use cases fall out of them. Infrastructure
before platform. Emergence over prediction.

## Five principles
1. **Speed** = remove waste (float-as-text, base64, untyped media/embeddings, JSON between stages).
2. **Simplicity** = keep Core small; everything else is a Profile, not core bloat.
   *"Core is physics. Profiles are chemistry. Applications are biology."*
3. **Power** = a few composing primitives (Tensor, TensorRef, Object, StreamFrame, SchemaFingerprint,
   UnknownExtension, Bitmask, Bytes, Datetime, Dictionary) express embeddings/training/eval/trace/media/graph.
4. **Infrastructure before platform** = spec, test vectors, fuzzing, CLI, Go/Py/Rust, stream r/w,
   inspector, JSON converter, validator, bench. Files: `.cow` / `.cows` / `.cowset`.
5. **Emergence** = clean lower layer makes the upper layer obvious (tensors→embeddings, streams→observability,
   fingerprints→routing, determinism→content-addressing, TensorRef→sharding, JSON projection→adoption).

## Proposed Core type list
null, bool, int64/uint64, **float32/float64**, decimal, string, bytes, datetime, uuid, bigint,
array, object, **tensor, tensor ref, bitmask, extension**. *(Everything else = profile.)*

## The lean stack
1. Cowrie Core (stable typed binary values) → 2. Tensor (dtype+shape+bytes/ref) → 3. Stream (framed
records: schema id, compression, checksum, metadata) → 4. Profiles (embeddings, media, graph, richtext,
evals, traces, training) → 5. Tools (inspect/validate/convert/bench/visualize) → 6. Integrations
(NumPy/PyTorch/Rust/Go/Arrow/Parquet/vectorDB/object-store).

## Proposed sequence
- **Phase 1 Trust:** spec, tests, fuzzing, safe decode, canonical mode, CLI inspect.
- **Phase 2 Tensor advantage:** tensor validation, zero-copy/copy APIs, NumPy/PyTorch/Rust, **embedding profile**.
- **Phase 3 Stream advantage:** master-stream hardening, incremental read, schema routing, compression, CRC, metadata.
- **Phase 4 Dataset shape:** manifest, shards, indexes, tensor refs, training-batch + eval profiles.
- **Phase 5 Emergent platform:** vector ingest, AI traces, dataset lake, eval replay, observability adapters.

## Two engineering "passes"
- **Tesla (reduce resistance):** every conversion/base64/JSON-float/lost-dtype/tensor-copy/blocked-stream is
  resistance; Cowrie's job = move typed AI data with less loss. (float arrays→tensors, large tensors→refs,
  booleans→bitmasks, repeated keys→dicts, records→frames, schema checks→fingerprints, binary→native bytes.)
- **von Neumann (clean machine):** minimal stable machine = Value, Dictionary, Encoder, Decoder, Tensor, Ref,
  Frame, Profile, Manifest. No hidden magic, no silent precision loss, no unbounded decode,
  **"no 'the Go implementation is the spec'."**

## Doctrine
Speed over ceremony · Simplicity over ontology · Power through primitives · Infrastructure before platform ·
Emergence over prediction. → *Make the bytes right. Make tensors native. Make streams safe. Make tools obvious.
Let the ecosystem emerge.*
