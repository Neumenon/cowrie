# Cowrie Custom Nodes for ComfyUI

Drop-in ComfyUI nodes bringing canonical `CWSP` conditioning caching, `R0–R2-Compact` prompt lenses, and `0x80` + `Ed25519` community-workflow security directly into ComfyUI graphs.

## Install

```bash
pip install cowrie-ai
ln -s "$(pwd)/adapters/comfyui" /path/to/ComfyUI/custom_nodes/cowrie-comfyui
```

## Nodes Included

1. **`Cowrie Canonical Conditioning Cache (CWSP)`** — Replaces `CLIPTextEncode` with a canonical `COWR` CID + 16 KB `CWSP` NVMe shard cache so ComfyUI restarts and seed/sampler iterations reuse text-encoder outputs immediately (key-order invariant).
2. **`Cowrie Workflow Trust Gate (0x80 + Ed25519)`** — Blocks tainted `net_exfil` / `fs_write` / `shell_exec` actions from untrusted community workflows unless accompanied by an offline RFC 8032 `Ed25519` signature (`cowrie declassify-sign`).
3. **`Cowrie Prompt Lens (R0–R2-Compact)`** — Projects large JSON metadata/annotations into compact, Mealy-fenced LLM/VLM prompt blocks.
