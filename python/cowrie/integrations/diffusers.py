"""Drop-in Cowrie `CWSP` canonical prompt-embedding cache for Hugging Face `diffusers` pipelines.

Caches slow text-encoder outputs (`encode_prompt`: UMT5-XXL, T5-XXL, Qwen3, CLIP) in 16 KB-aligned,
0600 HMAC/SHA-NI-verified `.state.cowrie` shards keyed by canonical `COWR` prompt+parameter CID
(100% immune to dict key ordering or JSON formatting drift).

Zero-migration usage:
    from diffusers import FluxPipeline
    from cowrie.integrations.diffusers import CowrieDiffusersCache

    pipe = FluxPipeline.from_pretrained("black-forest-labs/FLUX.1-dev", torch_dtype=torch.bfloat16)
    cache = CowrieDiffusersCache(cache_dir="~/.cache/cowrie/diffusers", model_id="FLUX.1-dev")
    cache.wrap_pipeline(pipe)
    # Repeated or seed/CFG-tweaked calls reuse cached text embeddings in milliseconds!
"""

from __future__ import annotations

import functools
import json
import os
from pathlib import Path
import struct
from typing import Any, Callable, Optional

import cowrie

DIFFUSERS_RUNTIME_ID = "diffusers-v1"
DIFFUSERS_LAYOUT_VER = 1
_HEADER_FMT = "<I"


def _default_cache_dir() -> Path:
    xdg = os.environ.get("XDG_CACHE_HOME")
    base = Path(xdg) if xdg else (Path.home() / ".cache")
    return base / "cowrie" / "diffusers"


def _normalize_for_cid(obj: Any) -> Any:
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, (list, tuple)):
        return [_normalize_for_cid(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _normalize_for_cid(v) for k, v in obj.items()}
    return str(obj)


def _pack_tensors(tensors: list[tuple[str, Any]]) -> bytes:
    """Pack a list of `(name, tensor_or_bytes)` into a self-describing binary payload for `CWSP`."""
    meta_entries = []
    chunks: list[bytes] = []
    offset = 0
    for name, obj in tensors:
        if obj is None:
            meta_entries.append({"name": name, "kind": "none"})
            continue
        if hasattr(obj, "detach") and hasattr(obj, "cpu") and hasattr(obj, "numpy"):
            # PyTorch Tensor (works for float16/bfloat16/float32/int64 without bfloat16 numpy issues via uint8 view)
            import torch
            t = obj.detach().contiguous().cpu()
            raw = t.view(torch.uint8).numpy().tobytes()
            meta_entries.append(
                {
                    "name": name,
                    "kind": "torch",
                    "dtype": str(t.dtype).replace("torch.", ""),
                    "shape": list(t.shape),
                    "offset": offset,
                    "nbytes": len(raw),
                }
            )
            chunks.append(raw)
            offset += len(raw)
        elif isinstance(obj, (bytes, bytearray, memoryview)):
            raw = bytes(obj)
            meta_entries.append(
                {
                    "name": name,
                    "kind": "bytes",
                    "offset": offset,
                    "nbytes": len(raw),
                }
            )
            chunks.append(raw)
            offset += len(raw)
        else:
            raw = json.dumps(_normalize_for_cid(obj), separators=(",", ":")).encode("utf-8")
            meta_entries.append(
                {
                    "name": name,
                    "kind": "json",
                    "offset": offset,
                    "nbytes": len(raw),
                }
            )
            chunks.append(raw)
            offset += len(raw)

    header_bytes = json.dumps({"v": 1, "items": meta_entries}, separators=(",", ":")).encode("utf-8")
    return struct.pack(_HEADER_FMT, len(header_bytes)) + header_bytes + b"".join(chunks)


def _unpack_tensors(payload: memoryview | bytes, device: Any = None) -> list[tuple[str, Any]]:
    mv = memoryview(payload)
    if len(mv) < 4:
        raise ValueError("Truncated Diffusers CWSP payload header")
    (hdr_len,) = struct.unpack_from(_HEADER_FMT, mv, 0)
    if hdr_len > len(mv) - 4 or hdr_len > 16 * 1024 * 1024:
        raise ValueError(f"Invalid Diffusers CWSP header length: {hdr_len}")
    header = json.loads(bytes(mv[4 : 4 + hdr_len]).decode("utf-8"))
    data_start = 4 + hdr_len
    data_avail = len(mv) - data_start
    out: list[tuple[str, Any]] = []
    for item in header["items"]:
        kind = item["kind"]
        name = item["name"]
        if kind == "none":
            out.append((name, None))
            continue
        off = int(item["offset"])
        nbytes = int(item["nbytes"])
        if off < 0 or nbytes < 0 or off + nbytes > data_avail:
            raise ValueError(f"Out-of-bounds tensor slice for {name!r}")
        raw = mv[data_start + off : data_start + off + nbytes]
        if kind == "torch":
            import torch
            dtype = getattr(torch, item["dtype"])
            arr = torch.frombuffer(bytearray(raw), dtype=torch.uint8).view(dtype).reshape(item["shape"])
            if device is not None:
                arr = arr.to(device=device)
            out.append((name, arr))
        elif kind == "bytes":
            out.append((name, bytes(raw)))
        elif kind == "json":
            out.append((name, json.loads(bytes(raw).decode("utf-8"))))
    return out


class CowrieDiffusersCache:
    """Canonical `COWR` + 16 KB `CWSP` StatePage cache for Diffusers text-encoder embeddings."""

    def __init__(
        self,
        cache_dir: Optional[str | Path] = None,
        *,
        model_id: str = "diffusers-pipeline",
        model_weights_path: Optional[str | Path] = None,
        verify_hash: bool = True,
    ):
        self.cache_dir = Path(cache_dir).expanduser() if cache_dir else _default_cache_dir()
        self.cache_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.cache_dir, 0o700)
        if model_weights_path and Path(model_weights_path).is_file():
            self.model_sha = cowrie.fingerprint_model(str(model_weights_path))
        else:
            self.model_sha = cowrie.sha256_hex(f"diffusers:{model_id}".encode("utf-8"))
        self.verify_hash = bool(verify_hash)
        self.hits = 0
        self.misses = 0

    def canonical_key(self, prompt_spec: dict[str, Any]) -> str:
        """Compute canonical 64-hex SHA-256 digest of the `COWR` frame for `prompt_spec` (invariant to key order)."""
        frame = cowrie.encode_json(
            _normalize_for_cid(prompt_spec),
            authority=cowrie.AUTH_VERIFIED_TOOL,
            epistemic=cowrie.EPI_OBSERVED,
        )
        # Strip the 2-byte `0x12 0x20` multihash prefix to match CWSP's strict 64-hex `doc_cid` contract.
        return cowrie.verify_frame(frame)["cid"][2:].hex()

    def shard_path(self, key_cid_hex: str) -> Path:
        return self.cache_dir / f"diffusers_{self.model_sha[:12]}_{key_cid_hex[:32]}.state.cowrie"

    def save(self, prompt_spec: dict[str, Any], outputs: Any, *, ctx_tokens: int = 1) -> tuple[str, Path]:
        key_cid = self.canonical_key(prompt_spec)
        if isinstance(outputs, tuple):
            items = [(f"out_{i}", v) for i, v in enumerate(outputs)]
            is_tuple = True
        elif isinstance(outputs, dict):
            items = sorted((str(k), v) for k, v in outputs.items())
            is_tuple = False
        else:
            items = [("single", outputs)]
            is_tuple = False
        items.append(("__container__", "tuple" if is_tuple else ("dict" if isinstance(outputs, dict) else "single")))
        blob = _pack_tensors(items)
        sp = self.shard_path(key_cid)
        cowrie.save_statepage(
            str(sp),
            self.model_sha,
            key_cid,
            max(1, int(ctx_tokens)),
            blob,
            kv_quant=0,
            ssm_dtype=0,
            runtime_id=DIFFUSERS_RUNTIME_ID,
            layout_ver=DIFFUSERS_LAYOUT_VER,
        )
        return key_cid, sp

    def load(self, prompt_spec: dict[str, Any], *, device: Any = None) -> Optional[Any]:
        key_cid = self.canonical_key(prompt_spec)
        sp = self.shard_path(key_cid)
        if not sp.is_file():
            self.misses += 1
            return None
        try:
            with cowrie.MappedStatePageView(
                str(sp),
                expected_model_sha=self.model_sha,
                expected_doc_cid=key_cid,
                expected_runtime_id=DIFFUSERS_RUNTIME_ID,
                verify_hash=self.verify_hash,
            ) as mp:
                items = _unpack_tensors(mp.buffer, device=device)
        except (cowrie.CowrieError, ValueError, OSError):
            self.misses += 1
            return None

        self.hits += 1
        kind = "tuple"
        if items and items[-1][0] == "__container__":
            kind = items[-1][1]
            items = items[:-1]
        if kind == "tuple":
            return tuple(v for _, v in items)
        if kind == "dict":
            return {k: v for k, v in items}
        return items[0][1] if items else None

    def wrap_pipeline(self, pipe: Any) -> Any:
        """Intercept `pipe.encode_prompt(...)` in place with canonical `CWSP` caching."""
        orig_encode = pipe.encode_prompt

        @functools.wraps(orig_encode)
        def _cached_encode_prompt(*args: Any, **kwargs: Any) -> Any:
            device = kwargs.get("device") or getattr(pipe, "_execution_device", None)
            spec = {
                "args": _normalize_for_cid(list(args)),
                "kwargs": {k: _normalize_for_cid(v) for k, v in kwargs.items() if k != "device"},
            }
            cached = self.load(spec, device=device)
            if cached is not None:
                return cached
            result = orig_encode(*args, **kwargs)
            self.save(spec, result)
            return result

        pipe.encode_prompt = _cached_encode_prompt
        return pipe


def wrap_pipeline_encode_prompt(
    pipe: Any,
    cache_dir: Optional[str | Path] = None,
    *,
    model_id: Optional[str] = None,
) -> CowrieDiffusersCache:
    """One-line helper to attach a `CowrieDiffusersCache` to any Hugging Face `DiffusionPipeline`."""
    mid = model_id or getattr(pipe, "name_or_path", None) or type(pipe).__name__
    cache = CowrieDiffusersCache(cache_dir=cache_dir, model_id=mid)
    cache.wrap_pipeline(pipe)
    return cache


__all__ = [
    "CowrieDiffusersCache",
    "wrap_pipeline_encode_prompt",
]
