"""Native `vLLM 0.15+` V1 Engine `KVConnectorBase_V1` Offloader backed by Cowrie (`libcowrie_core.so`).

Instead of scattering 28–80 unverified per-layer `.safetensors` files per request:
  1. Packs ALL attention layers' paged KV blocks for a request into ONE 16 KB-aligned `.state.cowrie`
     (`CWSP`) shard on NVMe via `cowrie_statepage_save_ext`.
  2. Binds every cached shard to `(model_sha, runtime_id="vllm-v1", kv_quant, doc_cid)` + optional
     RFC 2104 `HMAC-SHA256` (`COWRIE_HMAC_KEY`) + 4 MiB hardware SHA-NI parallel Merkle root.
  3. Supports longest-prefix matching across distinct follow-up questions (`vllm_{ntok}_{cid}.state.cowrie`)
     and restores across full vLLM engine restarts via zero-copy `mmap` (`MappedStatePageView`)
     -> single `torch.frombuffer` view -> single async H2D transfer -> scatter into `kv_caches`.
"""

import os
import struct
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import torch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "python") not in sys.path:
    sys.path.insert(0, str(ROOT / "python"))

import cowrie

from vllm.config import VllmConfig
from vllm.distributed.kv_transfer.kv_connector.v1.base import (
    KVConnectorBase_V1,
    KVConnectorMetadata,
    KVConnectorRole,
)
from vllm.logger import init_logger
from vllm.model_executor.layers.attention.mla_attention import MLACommonMetadata
from vllm.v1.attention.backend import AttentionMetadata
from vllm.v1.core.sched.output import SchedulerOutput

if TYPE_CHECKING:
    from vllm.forward_context import ForwardContext
    from vllm.v1.core.kv_cache_manager import KVCacheBlocks
    from vllm.v1.kv_cache_interface import KVCacheConfig
    from vllm.v1.request import Request

logger = init_logger(__name__)

VLLM_RUNTIME_ID = "vllm-v1"
VLLM_LAYOUT_VER = 1


def aligned_prefix_tokens(num_tokens: int, block_size: int) -> int:
    """Return block-aligned prefix token count leaving >= 1 final token for next-token logit prefill."""
    if num_tokens <= 1:
        return 0
    return ((num_tokens - 1) // block_size) * block_size


def _compute_vllm_model_sha(model_name_or_path: str) -> str:
    p = Path(model_name_or_path)
    if p.is_file():
        return cowrie.fingerprint_model(str(p))
    if p.is_dir():
        parts = []
        for f in sorted(p.glob("*.safetensors")) + sorted(p.glob("config.json")):
            if f.is_file():
                parts.append(f"{f.name}:{cowrie.fingerprint_model(str(f))}")
        if parts:
            return cowrie.sha256_hex("|".join(parts).encode("utf-8"))
    return cowrie.sha256_hex(f"vllm-model:{model_name_or_path}".encode("utf-8"))


def _kv_dtype_tag(dtype: torch.dtype) -> int:
    if dtype in (torch.float16, torch.bfloat16):
        return 0
    if dtype == torch.float32:
        return 3
    return 2  # fp8 / int8


def _hash_prefix(token_ids: list[int], num_tokens: int, mm_hashes: list[str]) -> str:
    prefix = token_ids[:num_tokens]
    raw_key = struct.pack(f"<{num_tokens}i", *prefix)
    if mm_hashes:
        raw_key += ("|" + "|".join(mm_hashes)).encode("utf-8")
    return cowrie.sha256_hex(raw_key)


@dataclass
class CowrieReqMeta:
    req_id: str
    doc_cid: str
    num_tokens: int
    slot_mapping: torch.Tensor
    is_store: bool

    @staticmethod
    def build(
        req_id: str,
        token_ids: list[int],
        block_ids: list[int],
        block_size: int,
        num_tokens: int,
        is_store: bool,
        mm_hashes: list[str],
    ) -> Optional["CowrieReqMeta"]:
        alloc_cap = len(block_ids) * block_size
        actual_tokens = min(num_tokens, alloc_cap)
        actual_tokens = (actual_tokens // block_size) * block_size
        if actual_tokens <= 0:
            return None
        doc_cid = _hash_prefix(token_ids, actual_tokens, mm_hashes)
        num_blocks = actual_tokens // block_size
        blk_t = torch.tensor(block_ids[:num_blocks], dtype=torch.long)
        offs = torch.arange(0, block_size, dtype=torch.long)
        slots = (offs.unsqueeze(0) + blk_t.unsqueeze(1) * block_size).flatten()[:actual_tokens]
        return CowrieReqMeta(
            req_id=req_id,
            doc_cid=doc_cid,
            num_tokens=actual_tokens,
            slot_mapping=slots,
            is_store=is_store,
        )


@dataclass
class CowrieConnectorMetadata(KVConnectorMetadata):
    requests: list[CowrieReqMeta] = field(default_factory=list)


class CowrieKVConnectorV1(KVConnectorBase_V1):
    """vLLM V1 KVConnector storing/restoring multi-layer KV blocks as single 16 KB `.state.cowrie` shards."""

    def __init__(
        self,
        vllm_config: VllmConfig,
        role: KVConnectorRole,
        kv_cache_config: Optional["KVCacheConfig"] = None,
    ):
        super().__init__(
            vllm_config=vllm_config,
            role=role,
            kv_cache_config=kv_cache_config,
        )
        self._block_size = vllm_config.cache_config.block_size
        self._requests_need_load: dict[str, tuple["Request", int]] = {}
        extra = self._kv_transfer_config.kv_connector_extra_config or {}
        self._storage_dir = Path(extra.get("cowrie_state_dir", "/tmp/cowrie_vllm_kv"))
        self._storage_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self._storage_dir, 0o700)
        self._model_sha = _compute_vllm_model_sha(vllm_config.model_config.model)
        self._staging: dict[tuple[int, str], list[tuple[str, torch.Tensor]]] = {}

    def _shard_path(self, num_tokens: int, doc_cid: str) -> Path:
        return self._storage_dir / f"vllm_{num_tokens}_{doc_cid[:32]}.state.cowrie"

    def _candidate_lengths(self, max_tokens: int) -> list[int]:
        lengths: set[int] = set()
        for f in self._storage_dir.glob("vllm_*_*.state.cowrie"):
            parts = f.name.split("_", 2)
            if len(parts) == 3 and parts[1].isdigit():
                nt = int(parts[1])
                if 0 < nt <= max_tokens and nt % self._block_size == 0:
                    lengths.add(nt)
        return sorted(lengths, reverse=True)

    def _find_longest_match(self, token_ids: list[int], mm_hashes: list[str]) -> Optional[tuple[str, int, Path]]:
        max_valid = aligned_prefix_tokens(len(token_ids), self._block_size)
        if max_valid <= 0:
            return None
        for nt in self._candidate_lengths(max_valid):
            cid = _hash_prefix(token_ids, nt, mm_hashes)
            sp = self._shard_path(nt, cid)
            if not sp.is_file():
                continue
            try:
                cowrie.verify_statepage_file(
                    str(sp),
                    expected_model_sha=self._model_sha,
                    verify_hash=False,
                    expected_runtime_id=VLLM_RUNTIME_ID,
                )
                return cid, nt, sp
            except cowrie.CowrieError:
                continue
        return None

    # ==============================
    # Scheduler-side V1 protocol
    # ==============================

    def get_num_new_matched_tokens(
        self,
        request: "Request",
        num_computed_tokens: int,
    ) -> tuple[int | None, bool]:
        hit = self._find_longest_match(
            list(request.prompt_token_ids or []),
            [f.identifier for f in request.mm_features],
        )
        if hit is None:
            return 0, False
        _, matched_tokens, _ = hit
        if matched_tokens <= num_computed_tokens:
            return 0, False
        return matched_tokens - num_computed_tokens, False

    def update_state_after_alloc(
        self,
        request: "Request",
        blocks: "KVCacheBlocks",
        num_external_tokens: int,
    ) -> None:
        if num_external_tokens > 0:
            hit = self._find_longest_match(
                list(request.prompt_token_ids or []),
                [f.identifier for f in request.mm_features],
            )
            matched = hit[1] if hit is not None else num_external_tokens
            self._requests_need_load[request.request_id] = (request, matched)

    def build_connector_meta(
        self,
        scheduler_output: SchedulerOutput,
    ) -> KVConnectorMetadata:
        meta = CowrieConnectorMetadata()
        for new_req in scheduler_output.scheduled_new_reqs:
            token_ids = list(new_req.prompt_token_ids or [])
            mm_hashes = [f.identifier for f in new_req.mm_features]
            if new_req.req_id in self._requests_need_load:
                _, matched_toks = self._requests_need_load[new_req.req_id]
                rm = CowrieReqMeta.build(
                    new_req.req_id,
                    token_ids,
                    new_req.block_ids[0],
                    self._block_size,
                    matched_toks,
                    False,
                    mm_hashes,
                )
                if rm is not None:
                    meta.requests.append(rm)
            else:
                alloc_cap = len(new_req.block_ids[0]) * self._block_size
                store_toks = min(aligned_prefix_tokens(len(token_ids), self._block_size), alloc_cap)
                store_toks = (store_toks // self._block_size) * self._block_size
                if store_toks > 0:
                    cid = _hash_prefix(token_ids, store_toks, mm_hashes)
                    if not self._shard_path(store_toks, cid).is_file():
                        rm = CowrieReqMeta.build(
                            new_req.req_id,
                            token_ids,
                            new_req.block_ids[0],
                            self._block_size,
                            store_toks,
                            True,
                            mm_hashes,
                        )
                        if rm is not None:
                            meta.requests.append(rm)

        cached_reqs = scheduler_output.scheduled_cached_reqs
        for i, req_id in enumerate(cached_reqs.req_ids):
            if req_id not in cached_reqs.resumed_req_ids or req_id not in self._requests_need_load:
                continue
            request, matched_toks = self._requests_need_load[req_id]
            total_tokens = cached_reqs.num_computed_tokens[i] + scheduler_output.num_scheduled_tokens[req_id]
            new_block_ids = cached_reqs.new_block_ids[i]
            if new_block_ids is not None:
                rm = CowrieReqMeta.build(
                    req_id,
                    list(request.all_token_ids[:total_tokens]),
                    new_block_ids[0],
                    self._block_size,
                    min(matched_toks, aligned_prefix_tokens(total_tokens, self._block_size)),
                    False,
                    [f.identifier for f in request.mm_features],
                )
                if rm is not None:
                    meta.requests.append(rm)

        self._requests_need_load.clear()
        return meta

    # ==============================
    # Worker-side V1 protocol
    # ==============================

    def start_load_kv(self, forward_context: "ForwardContext", **kwargs: Any) -> None:
        metadata = self._get_connector_metadata()
        if not isinstance(metadata, CowrieConnectorMetadata) or not metadata.requests:
            return
        attn_metadata = forward_context.attn_metadata
        if attn_metadata is None:
            return
        is_mla = isinstance(attn_metadata, MLACommonMetadata)

        attn_layers: list[tuple[str, torch.Tensor]] = []
        for layer_name in sorted(forward_context.no_compile_layers.keys()):
            layer = forward_context.no_compile_layers[layer_name]
            kv_attr = getattr(layer, "kv_cache", None)
            if kv_attr is not None:
                attn_layers.append((layer_name, kv_attr[forward_context.virtual_engine]))
        if not attn_layers:
            return

        sample_layer = attn_layers[0][1]
        kv_quant = _kv_dtype_tag(sample_layer.dtype)

        for req in metadata.requests:
            if req.is_store:
                continue
            sp = self._shard_path(req.num_tokens, req.doc_cid)
            with cowrie.MappedStatePageView(
                str(sp),
                expected_model_sha=self._model_sha,
                expected_runtime_id=VLLM_RUNTIME_ID,
                expected_kv_quant=kv_quant,
                verify_hash=True,
            ) as mp:
                raw_u8 = torch.frombuffer(mp.buffer, dtype=torch.uint8)
                stacked_cpu = raw_u8.view(sample_layer.dtype)
                stacked_gpu = stacked_cpu.to(device=sample_layer.device, non_blocking=True)
                slots = req.slot_mapping.to(device=sample_layer.device, non_blocking=True)
                n_layers = len(attn_layers)

                if is_mla:
                    stacked_gpu = stacked_gpu.reshape(n_layers, req.num_tokens, -1)
                    for idx, (_, dst_layer) in enumerate(attn_layers):
                        orig_shape = dst_layer.shape
                        flat = dst_layer.reshape(orig_shape[0] * orig_shape[1], -1)
                        flat[slots, ...] = stacked_gpu[idx]
                else:
                    stacked_gpu = stacked_gpu.reshape(n_layers, 2, req.num_tokens, -1)
                    for idx, (_, dst_layer) in enumerate(attn_layers):
                        orig_shape = dst_layer.shape
                        flat = dst_layer.reshape(2, orig_shape[1] * orig_shape[2], -1)
                        flat[:, slots, ...] = stacked_gpu[idx]

    def wait_for_layer_load(self, layer_name: str) -> None:
        return

    def save_kv_layer(
        self,
        layer_name: str,
        kv_layer: torch.Tensor,
        attn_metadata: AttentionMetadata,
        **kwargs: Any,
    ) -> None:
        metadata = self._get_connector_metadata()
        if not isinstance(metadata, CowrieConnectorMetadata) or not metadata.requests:
            return
        is_mla = isinstance(attn_metadata, MLACommonMetadata)

        for req in metadata.requests:
            if not req.is_store:
                continue
            slots = req.slot_mapping.to(device=kv_layer.device)
            if is_mla:
                num_pages, page_size = kv_layer.shape[0], kv_layer.shape[1]
                sliced = kv_layer.reshape(num_pages * page_size, -1)[slots, ...].contiguous().cpu()
            else:
                num_pages, page_size = kv_layer.shape[1], kv_layer.shape[2]
                sliced = kv_layer.reshape(2, num_pages * page_size, -1)[:, slots, ...].contiguous().cpu()
            self._staging.setdefault((req.num_tokens, req.doc_cid), []).append((layer_name, sliced))

    def wait_for_save(self) -> None:
        metadata = self._get_connector_metadata()
        if not isinstance(metadata, CowrieConnectorMetadata) or not self._staging:
            return
        for req in metadata.requests:
            key = (req.num_tokens, req.doc_cid)
            if not req.is_store or key not in self._staging:
                continue
            items = self._staging.pop(key)
            items.sort(key=lambda kv: kv[0])
            stacked = torch.stack([t for _, t in items], dim=0).contiguous()
            kv_quant = _kv_dtype_tag(stacked.dtype)
            raw_bytes = stacked.view(torch.uint8).numpy().tobytes()
            sp = self._shard_path(req.num_tokens, req.doc_cid)
            cowrie.save_statepage(
                str(sp),
                self._model_sha,
                req.doc_cid,
                req.num_tokens,
                raw_bytes,
                kv_quant=kv_quant,
                ssm_dtype=0,
                runtime_id=VLLM_RUNTIME_ID,
                layout_ver=VLLM_LAYOUT_VER,
            )
        self._staging.clear()
