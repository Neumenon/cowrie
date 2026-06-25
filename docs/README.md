# Cowrie v1 — Documentation

Cowrie v1 (`COWR`, version 1) is a **deterministic, content-addressable, AI-native binary
codec**. There is exactly one canonical byte-string per value, it is byte-identical across
Python, Go, Rust, and TypeScript, and its content address is a multihash SHA-256 over those
bytes (spec §3):

> **equal value ⇒ equal canonical bytes ⇒ equal hash.**

This page is the map of the documentation. Start with the spec if you want the format; start
with [QUICKSTART](QUICKSTART.md) if you want to encode something.

> **Status:** all implementations are **0.9.0** — pre-1.0 release candidate. The wire format
> is not yet frozen.

## Specification & conformance

| Document | What it is |
| --- | --- |
| **[SPEC-v1.md](SPEC-v1.md)** | The authoritative Cowrie v1 wire format — canonical encoding, type system, and the content-address definition. The single normative source; everything else defers to it. |
| [CONFORMANCE.md](../CONFORMANCE.md) | Current conformance state of the four implementations, the standing gates, and how each count was verified. |
| [PROFILES.md](PROFILES.md) | Optional layered profiles built on the core codec (file identity, dataset Merkle roots, fingerprints) and how they compose. |
| [STREAM-LAYER.md](STREAM-LAYER.md) | The framed streaming layer over Cowrie values — record framing for append/scan use. |
| [PHASE2-TENSOR.md](PHASE2-TENSOR.md) | Phase 2 tensor-span design — addressing tensor payloads within Cowrie values. |

## Getting started & operations

| Document | What it is |
| --- | --- |
| [INSTALL.md](../INSTALL.md) | Per-language install (pre-publish from the `phase0-spec` branch, and the registry forms after publish). |
| [QUICKSTART.md](QUICKSTART.md) | Minimal encode / decode / content-address walkthrough in each language, plus the `cowrie` CLI. |
| [RELEASING.md](../RELEASING.md) | How releases are cut: tag-triggered, gate-gated publishing to PyPI, npm, crates.io, and Go. |
| [ROADMAP.md](../ROADMAP.md) | What is done, what is planned, and the path to a frozen 1.0. |

## Historical (point-in-time records, not maintained)

These captured a moment and are kept for provenance only. They are not updated and may
describe retired approaches; trust [SPEC-v1.md](SPEC-v1.md) over anything here.

- [miami-provenance.md](../miami-provenance.md) — provenance record of the clean-room v1 work.
- [PHASE0-FINDINGS.md](../PHASE0-FINDINGS.md) — findings from the Phase 0 spec investigation.
- [posts/](../posts/) — dated write-ups and checklists (e.g. the release checklist).
