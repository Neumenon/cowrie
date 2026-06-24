# Cowrie — North Star (engineering verdict)

> Adjudicated by a cross-model **quorum** (Claude/Opus orchestrating + GPT‑5.5, Gemini, DeepSeek as independent voices), grounded in the actual repo — not the aspirational doc. Valuation/market claims out of scope. Run `wkcodd0zn`, 8 agents.

## Verdict (6/6 models, no dissent)

**The codebase's deliberate minimal core IS the north star. The maximalist "AI data plane" document is second‑system overreach** — it proposes re‑adding almost exactly what the team already, correctly, sent to `attic/` (Graph, GNN, RichText, Delta, columnar), plus first‑class Video, a 5–6 product platform (Lake/Eval/Observe/Gateway/Train/Vector‑Ingest), and 8 SDKs.

**The attic decision was correct** — the most mature engineering judgment in the picture.

## One‑sentence north star

> **Cowrie is the deterministic, cross‑language, typed _envelope_ for AI data _in flight_ — embeddings, tensors, tensor‑refs + provenance — that hashes identically everywhere (caching, dedup, dataset identity, reproducible replay) and _interoperates with_ Arrow / Parquet / safetensors rather than replacing them.**

## Is the document's north star good design + feasible? No — the repo proves it

Every model independently cited `attic/docs/audit-report.md` as evidence that breadth already caused rot:

- `DType` enums with **incompatible numeric values** across `gnn` / `ucodec` / root (`0x01` vs `0`)
- `MaxObjectLen` security limit **silently drifted 10×** between packages
- orphaned packages (`intern` / `ucodec` / `dicts`) with zero importers; `gnn/onnx` unreachable from the codec
- triplicated NumPy dtype map with a narrowing bug
- assertion‑free **"95% savings" tests that always pass**

The team can't keep *four* languages' constants in agreement today → 8 SDKs of a multimodal platform is infeasible. Honest signal: *too much surface, already drifting.* The document's answer — "add a platform" — is the wrong direction.

## Claims adjudicated

| Claim | Verdict | Note |
|---|---|---|
| **C1** JSON poor substrate for AI/binary | **TRUE** | the real, defensible core (float32 text‑bloat ~4.8×; no bytes/decimal/uuid/ns‑time without ceremony) |
| **C2** deterministic → content‑addressable | **TRUE but hard** | sound in design, *not yet proven in fleet*; cross‑language byte‑determinism (NaN, key sort, padding) is the actual hard problem |
| **C3** fingerprint routing without decode | **HALF‑TRUE** | real for routing/drift; it's a 64‑bit FNV‑1a *shape* hash — **not** validation, **not** tamper‑detection |
| **C4** one format for everything | **OVERREACH** | row vs columnar are different memory models. Formally drop it. |
| **C5** embeddings are the beachhead | **TRUE‑ish** | the one *measured* win (open decision below) |
| **C6** replayability is the differentiator | **HALF‑TRUE** | an event‑sourcing/systems choice; Cowrie is the enabler, not the engine |
| **C7** interop, don't compete | **TRUE — elevate to policy** | the only sane path |

## Real engineering value (not $)

Killing the **"JSON + base64 + `.npy` sidecar"** nightmare: one deterministic, hashable, typed envelope carrying metadata + tensors + raw bytes that decodes identically in Go/Rust/Py/TS — plus **schema‑fingerprint routing/caching at a proxy without decoding the payload**. `TensorRef (0x21)` is the strategic linchpin: *point at the heavy bytes, don't own them.*

Where Cowrie adds **zero** value (and loses): tensor‑on‑disk → **safetensors**; columnar/lake → **Arrow/Parquet**; schema'd RPC → **Protobuf/Avro**; generic binary‑JSON → **CBOR/MessagePack**; telemetry → **OpenTelemetry**; RichText/Delta → **CRDTs/ProseMirror**.

## Keep / Cut

**Keep:** core `0x00–0x0F` + the scalars JSON lacks (`bytes 0x08`, int64/uint64, `decimal128`, `UUID128`, ns‑`datetime64`, float32) · `Tensor 0x20` · `TensorRef 0x21` · `Bitmask 0x24` · schema fingerprint (scoped to routing/drift) · `TagExt 0x0E` as the **only** sanctioned growth path · adapters · a sharp CLI (inspect/diff/fingerprint).

**Cut / keep‑cut:** Graph/GNN · RichText · Delta · JSON‑LD · columnar/ColumnHints · Video · the entire platform layer (Lake/Eval/Observe/Gateway/Train/Vector‑Ingest).

## Gating work (close these before anything new)

1. Fix the cross‑language **DType / limit drift**.
2. Finish the **full encode‑parity matrix** (today: only "pinned tests").
3. Delete the **assertion‑free savings tests**.
4. Specify **content hashes over uncompressed canonical bytes only** (push compression to transport/storage).

## The one open decision

The wedge:
- **Embeddings‑first** (Claude + GPT‑5.5) — smallest, hottest, only measured win.
- **End‑to‑end multimodal AI request/response payload** (Gemini + DeepSeek) — prompt + image bytes + embedding + output in one row; VectorDBs already own embedding *storage*.

Both agree it's the *in‑flight typed‑AI‑payload* seam. The split is: lead with the **vector**, or the whole **transaction**. That's the call left to make.

---

*Confidence: high — four independent models + the codebase's own audit converged on the same verdict.*
