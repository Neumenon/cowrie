"""ComfyUI drop-in nodes backed by Cowrie v1.2 (`cowrie-ai`).

Install into ComfyUI:
    pip install cowrie-ai
    ln -s /path/to/cowrie/adapters/comfyui /path/to/ComfyUI/custom_nodes/cowrie-comfyui
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import time
from typing import Any

import cowrie
from cowrie.integrations.diffusers import CowrieDiffusersCache


class CowrieCanonicalConditioningCache:
    """Encode CLIP/T5/Qwen prompt text once and persist `CONDITIONING` in a 16 KB `CWSP` NVMe shard.

    Because the cache key is a canonical `COWR` SHA-256 CID, key-shuffled workflow metadata or
    restarting ComfyUI hits the exact same `0600` HMAC/SHA-NI-verified shard on disk.
    """

    @classmethod
    def INPUT_TYPES(cls) -> dict[str, Any]:
        return {
            "required": {
                "clip": ("CLIP",),
                "text": ("STRING", {"multiline": True, "dynamicPrompts": True}),
            },
            "optional": {
                "cache_dir": ("STRING", {"default": "~/.cache/cowrie/comfyui_cond"}),
                "model_tag": ("STRING", {"default": "comfyui-clip"}),
                "extra_json": ("STRING", {"default": "{}"}),
            },
        }

    RETURN_TYPES = ("CONDITIONING", "STRING")
    RETURN_NAMES = ("conditioning", "cowrie_cid")
    FUNCTION = "encode_cached"
    CATEGORY = "Cowrie/Cache"

    def encode_cached(
        self,
        clip: Any,
        text: str,
        cache_dir: str = "~/.cache/cowrie/comfyui_cond",
        model_tag: str = "comfyui-clip",
        extra_json: str = "{}",
    ) -> tuple[Any, str]:
        extra = json.loads(extra_json) if extra_json.strip() else {}
        spec = {"text": text, "model_tag": model_tag, "extra": extra}
        cache = CowrieDiffusersCache(cache_dir=Path(cache_dir).expanduser(), model_id=model_tag)
        cid_hex = cache.canonical_key(spec)

        cached = cache.load(spec)
        if isinstance(cached, dict) and "count" in cached:
            n = int(cached["count"])
            rebuilt = []
            for idx in range(n):
                t = cached.get(f"t_{idx}")
                meta = dict(cached.get(f"m_{idx}") or {})
                pooled = cached.get(f"p_{idx}")
                if pooled is not None:
                    meta["pooled_output"] = pooled
                rebuilt.append([t, meta])
            return (rebuilt, cid_hex)

        tokens = clip.tokenize(text)
        if hasattr(clip, "encode_from_tokens_scheduled"):
            cond = clip.encode_from_tokens_scheduled(tokens)
        else:
            tensor, pooled = clip.encode_from_tokens(tokens, return_pooled=True)
            cond = [[tensor, {"pooled_output": pooled}]]

        # Store tensors at top-level dict keys so CowrieDiffusersCache packs them as raw binary tensors
        flat: dict[str, Any] = {"count": len(cond)}
        for idx, pair in enumerate(cond):
            t, meta = pair[0], dict(pair[1] or {})
            pooled = meta.pop("pooled_output", None)
            flat[f"t_{idx}"] = t
            flat[f"p_{idx}"] = pooled
            flat[f"m_{idx}"] = meta
        cache.save(spec, flat)
        return (cond, cid_hex)


class CowrieWorkflowTrustGate:
    """Enforce Cowrie `0x04 UNTRUSTED_EXT -> 0x80 BIT_TAINTED` + RFC 8032 `Ed25519` gate on workflow actions.

    Drop in front of custom nodes that fetch remote URLs, write files, or run shell commands
    when opening untrusted community workflows.
    """

    @classmethod
    def INPUT_TYPES(cls) -> dict[str, Any]:
        return {
            "required": {
                "payload_json": ("STRING", {"multiline": True, "default": "{}"}),
                "source_trust": (["untrusted_ext", "verified_tool", "sys_policy"],),
                "action": (["read_only", "net_exfil", "fs_write", "shell_exec", "cred_read"],),
            },
            "optional": {
                "owner_pubkey_hex": ("STRING", {"default": ""}),
                "declassify_sig_hex": ("STRING", {"default": ""}),
                "expiry_unix": ("INT", {"default": 0, "min": 0, "max": 2**63 - 1}),
            },
        }

    RETURN_TYPES = ("BOOLEAN", "STRING", "STRING")
    RETURN_NAMES = ("allowed", "cid_hex", "reason")
    FUNCTION = "check_gate"
    CATEGORY = "Cowrie/Security"

    def check_gate(
        self,
        payload_json: str,
        source_trust: str,
        action: str,
        owner_pubkey_hex: str = "",
        declassify_sig_hex: str = "",
        expiry_unix: int = 0,
    ) -> tuple[bool, str, str]:
        auth_map = {
            "sys_policy": cowrie.AUTH_SYS_POLICY,
            "verified_tool": cowrie.AUTH_VERIFIED_TOOL,
            "untrusted_ext": cowrie.AUTH_UNTRUSTED_EXT,
        }
        auth = auth_map[source_trust]
        frame = cowrie.encode_json(payload_json, authority=auth, epistemic=cowrie.EPI_OBSERVED)
        ver = cowrie.verify_frame(frame)
        cid_bytes: bytes = ver["cid"]
        cid_hex = cid_bytes.hex()

        if action == "read_only" or not cowrie.is_tainted(ver["authority"]):
            return (True, cid_hex, "ALLOWED_CLEAN")

        pk_hex = owner_pubkey_hex.strip() or os.environ.get("COWRIE_OWNER_PUBKEY", "").strip()
        sig_hex = declassify_sig_hex.strip()
        if len(pk_hex) == 64 and len(sig_hex) == 128 and expiry_unix > 0:
            ok = cowrie.verify_declassification(
                bytes.fromhex(pk_hex),
                cid_bytes,
                action,
                int(expiry_unix),
                bytes.fromhex(sig_hex),
                int(time.time()),
            )
            if ok:
                return (True, cid_hex, "ALLOWED_ED25519_DECLASSIFIED")

        return (False, cid_hex, "ERR_TAINTED_EXECUTION_BLOCKED")


class CowriePromptLens:
    """Project structured JSON metadata or workflow parameters into compact `R0`/`R1`/`R2-Compact` prompt text."""

    @classmethod
    def INPUT_TYPES(cls) -> dict[str, Any]:
        return {
            "required": {
                "json_text": ("STRING", {"multiline": True, "default": "{}"}),
                "tier": (["r2-compact", "r2", "r1", "r0"],),
                "source_trust": (["verified_tool", "untrusted_ext"],),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("lens_text", "handle")
    FUNCTION = "project"
    CATEGORY = "Cowrie/Lens"

    def project(self, json_text: str, tier: str, source_trust: str) -> tuple[str, str]:
        auth = cowrie.AUTH_UNTRUSTED_EXT if source_trust == "untrusted_ext" else cowrie.AUTH_VERIFIED_TOOL
        frame = cowrie.encode_json(json_text, authority=auth, epistemic=cowrie.EPI_OBSERVED)
        cid = cowrie.verify_frame(frame)["cid"]
        handle = cowrie.alpha_handle(cid)
        text = cowrie.project_r2_compact(frame) if tier == "r2-compact" else cowrie.project_lens(frame, tier)
        return (text, handle)


NODE_CLASS_MAPPINGS = {
    "CowrieCanonicalConditioningCache": CowrieCanonicalConditioningCache,
    "CowrieWorkflowTrustGate": CowrieWorkflowTrustGate,
    "CowriePromptLens": CowriePromptLens,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "CowrieCanonicalConditioningCache": "Cowrie Canonical Conditioning Cache (CWSP)",
    "CowrieWorkflowTrustGate": "Cowrie Workflow Trust Gate (0x80 + Ed25519)",
    "CowriePromptLens": "Cowrie Prompt Lens (R0–R2-Compact)",
}
