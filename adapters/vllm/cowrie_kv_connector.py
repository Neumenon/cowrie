"""Native `vLLM 0.15+` V1 Engine `KVConnectorBase_V1` Offloader backed by Cowrie (`libcowrie_core.so`).

Packs ALL attention layers' paged KV blocks for a request into ONE 16 KB-aligned `.state.cowrie`
(`CWSP`) shard on NVMe via `cowrie_statepage_save_ext`, scoped to `(tp_rank, tp_size, pp_rank,
pp_size, kv_dtype, layout)`, with:
  1. Strict `(model_sha, doc_cid, runtime_id="vllm-v1", kv_quant, layout_ver)` + optional RFC 2104
     `HMAC-SHA256` (`COWRIE_HMAC_KEY`) + parallel 4 MiB hardware `SHA-NI`/`ARMv8 SHA2` Merkle seal.
  2. O(1) in-memory prefix length index + bounded `max_cache_bytes` LRU eviction (default `8 GiB`).
  3. Non-blocking background `ThreadPoolExecutor` NVMe persistence so GPU workers do not stall on `fsync`.
  4. Chunked-prefill save scheduling, lazy MLA detection, `register_kv_caches()`, and `vllm.general_plugins` hook.
"""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import itertools
import json
import os
from pathlib import Path
import struct
import sys
import threading
import time
from typing import TYPE_CHECKING, Any, Optional
import warnings

import torch

import importlib.util
if importlib.util.find_spec("cowrie") is None:
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
try:
    from vllm.distributed.kv_transfer.kv_connector.v1.base import SupportsHMA as _SupportsHMA
    SUPPORTS_HMA_NATIVE = True
except ImportError:  # Older vLLM V1 builds without SupportsHMA
    SUPPORTS_HMA_NATIVE = False

    class _SupportsHMA:  # type: ignore[no-redef]
        pass
from vllm.logger import init_logger
from vllm.v1.attention.backend import AttentionMetadata
from vllm.v1.core.sched.output import SchedulerOutput

if TYPE_CHECKING:
    from vllm.forward_context import ForwardContext
    from vllm.v1.core.kv_cache_manager import KVCacheBlocks
    from vllm.v1.kv_cache_interface import KVCacheConfig
    from vllm.v1.request import Request

logger = init_logger(__name__)

VLLM_RUNTIME_ID = "vllm-v1"
VLLM_LAYOUT_VER = 3
DEFAULT_MAX_CACHE_BYTES = 8 * 1024 * 1024 * 1024  # 8 GiB
DEFAULT_MAX_INFLIGHT_BYTES = 512 * 1024 * 1024    # 512 MiB host-RAM backpressure cap
_STATS_FILE_PREFIX = ".cowrie_stats_"
_INSTANCE_COUNTER = itertools.count(1)


def _extract_mm_hashes(obj: Any) -> list[str]:
    feats = getattr(obj, "mm_features", None) or []
    return [str(getattr(f, "identifier", getattr(f, "mm_hash", ""))) for f in feats]


def _detect_kv_layout(kv_layer: torch.Tensor, is_mla: bool) -> str:
    """Detect KV cache tensor layout: MLA, FlashAttention `(2, blocks, ...)`, or FlashInfer `(blocks, 2, ...)`."""
    if is_mla:
        return "mla"
    if len(kv_layer.shape) >= 3 and kv_layer.shape[0] == 2:
        return "2_blocks_first"
    if len(kv_layer.shape) >= 3 and kv_layer.shape[1] == 2:
        return "blocks_2_first"
    raise ValueError(f"Unsupported KV cache tensor shape for CowrieKVConnectorV1: {tuple(kv_layer.shape)}")


def reset_connector_stats(storage_dir: str | Path) -> int:
    """Remove persisted `.cowrie_stats_*.json` snapshots in `storage_dir` (leaves `.state.cowrie` shards intact)."""
    d = Path(storage_dir)
    if not d.is_dir():
        return 0
    removed = 0
    for f in d.glob(f"{_STATS_FILE_PREFIX}*.json"):
        try:
            f.unlink()
            removed += 1
        except OSError:
            continue
    return removed


def read_connector_stats(storage_dir: str | Path) -> dict[str, Any]:
    """Read and aggregate all persisted `CowrieKVConnectorV1` counter snapshots from `storage_dir`."""
    d = Path(storage_dir)
    instances: list[dict[str, Any]] = []
    if d.is_dir():
        for f in sorted(d.glob(f"{_STATS_FILE_PREFIX}*.json")):
            if ".tmp." in f.name:
                continue
            try:
                instances.append(json.loads(f.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError):
                continue

    sum_int_keys = (
        "register_kv_caches_calls",
        "get_num_new_matched_tokens_calls",
        "scheduler_hits",
        "scheduler_misses",
        "scheduler_matched_tokens",
        "update_state_after_alloc_load_calls",
        "update_state_after_alloc_store_calls",
        "build_connector_meta_calls",
        "meta_load_requests",
        "meta_store_requests",
        "request_finished_calls",
        "request_finished_all_groups_calls",
        "start_load_kv_calls",
        "loads_completed",
        "loads_failed",
        "loaded_tokens",
        "loaded_bytes",
        "save_kv_layer_calls",
        "wait_for_save_calls",
        "saves_submitted",
        "saves_completed",
        "saves_failed",
        "saves_skipped_inflight_cap",
        "saved_tokens",
        "saved_bytes",
        "inflight_saves",
        "inflight_bytes",
    )
    totals: dict[str, Any] = {k: sum(int(inst.get(k, 0)) for inst in instances) for k in sum_int_keys}
    totals["load_ms_total"] = round(sum(float(inst.get("load_ms_total", 0.0)) for inst in instances), 3)
    totals["save_ms_total"] = round(sum(float(inst.get("save_ms_total", 0.0)) for inst in instances), 3)
    totals["instance_count"] = len(instances)
    totals["roles_seen"] = sorted({str(inst.get("role", "")) for inst in instances if inst.get("role")})
    totals["kv_layouts"] = sorted({str(inst.get("kv_layout", "")) for inst in instances if inst.get("kv_layout")})
    totals["any_supports_hma"] = any(bool(inst.get("supports_hma", False)) for inst in instances)
    totals["all_supports_hma"] = bool(instances) and all(bool(inst.get("supports_hma", False)) for inst in instances)
    return {"instances": instances, "totals": totals}


def _request_lora_tag(request: Any) -> str:
    lr = getattr(request, "lora_request", None)
    if lr is None:
        return "base"
    name = getattr(lr, "lora_name", None) or getattr(lr, "name", None) or ""
    lid = getattr(lr, "lora_int_id", None)
    path = getattr(lr, "lora_path", None) or getattr(lr, "local_path", None) or ""
    if path and Path(str(path)).exists():
        try:
            p = Path(str(path))
            if p.is_file():
                return f"lora:{name}:{cowrie.fingerprint_model(str(p))[:16]}"
            if p.is_dir():
                parts = [
                    f"{f.name}:{cowrie.fingerprint_model(str(f))[:16]}"
                    for f in sorted(p.glob("*.safetensors")) + sorted(p.glob("adapter_config.json"))
                    if f.is_file()
                ]
                if parts:
                    return f"lora:{name}:{cowrie.sha256_hex('|'.join(parts).encode('utf-8'))[:16]}"
        except Exception:
            pass
    return f"lora:{name}:{lid}:{path}"


def _default_cache_dir() -> Path:
    xdg = os.environ.get("XDG_CACHE_HOME")
    base = Path(xdg) if xdg else (Path.home() / ".cache")
    return base / "cowrie" / "vllm_kv"


def _is_mla_metadata(attn_metadata: Any) -> bool:
    if attn_metadata is None:
        return False
    try:
        from vllm.model_executor.layers.attention.mla_attention import MLACommonMetadata
        return isinstance(attn_metadata, MLACommonMetadata)
    except Exception:
        return type(attn_metadata).__name__ == "MLACommonMetadata"


def _get_parallel_topology(vllm_config: VllmConfig) -> tuple[int, int, int, int]:
    p_cfg = getattr(vllm_config, "parallel_config", None)
    tp_size = int(getattr(p_cfg, "tensor_parallel_size", 1) or 1)
    pp_size = int(getattr(p_cfg, "pipeline_parallel_size", 1) or 1)
    tp_rank = 0
    pp_rank = 0
    try:
        from vllm.distributed import (
            get_pp_group,
            get_tensor_model_parallel_rank,
        )
        tp_rank = int(get_tensor_model_parallel_rank())
        pp_rank = int(get_pp_group().rank_in_group)
    except Exception:
        tp_rank = int(os.environ.get("VLLM_TP_RANK", "0"))
        pp_rank = int(os.environ.get("VLLM_PP_RANK", "0"))
    return tp_rank, tp_size, pp_rank, pp_size


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


def _kv_dtype_tag(dtype: torch.dtype) -> tuple[int, str]:
    """Return `(cowrie_kv_quant_u8, distinct_dtype_slug)` preserving fp16 vs bf16 and fp8 vs int8 distinction."""
    if dtype == torch.float16:
        return 0, "f16"
    if dtype == torch.bfloat16:
        return 10, "bf16"
    if dtype == torch.float32:
        return 3, "f32"
    if dtype == torch.int8:
        return 2, "i8"
    if dtype == torch.uint8:
        return 11, "u8"
    name = str(dtype).replace("torch.", "").replace(".", "_")
    if "e4m3" in name:
        return 12, "fp8e4m3"
    if "e5m2" in name:
        return 13, "fp8e5m2"
    return 14, name[:12]


def _hash_prefix(token_ids: list[int], num_tokens: int, mm_hashes: list[str], extra_salt: str = "") -> str:
    prefix = token_ids[:num_tokens]
    raw_key = struct.pack(f"<{num_tokens}i", *prefix)
    if mm_hashes:
        raw_key += ("|" + "|".join(mm_hashes)).encode("utf-8")
    if extra_salt:
        raw_key += ("|" + extra_salt).encode("utf-8")
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
        topology_salt: str,
    ) -> Optional["CowrieReqMeta"]:
        alloc_cap = len(block_ids) * block_size
        actual_tokens = min(num_tokens, alloc_cap)
        actual_tokens = (actual_tokens // block_size) * block_size
        if actual_tokens <= 0:
            return None
        doc_cid = _hash_prefix(token_ids, actual_tokens, mm_hashes, topology_salt)
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


class CowrieKVConnectorV1(KVConnectorBase_V1, _SupportsHMA):
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
        self._requests_pending_store: dict[str, "Request"] = {}
        extra = self._kv_transfer_config.kv_connector_extra_config or {}
        raw_dir = extra.get("cowrie_state_dir")
        self._storage_dir = Path(raw_dir) if raw_dir else _default_cache_dir()
        self._storage_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self._storage_dir, 0o700)

        self._max_cache_bytes = int(extra.get("cowrie_max_cache_bytes", DEFAULT_MAX_CACHE_BYTES))
        self._max_inflight_bytes = int(extra.get("cowrie_max_inflight_bytes", DEFAULT_MAX_INFLIGHT_BYTES))
        self._tenant_id = str(extra.get("cowrie_tenant_id") or os.environ.get("COWRIE_TENANT_ID", ""))
        self._tp_rank, self._tp_size, self._pp_rank, self._pp_size = _get_parallel_topology(vllm_config)
        self._cache_dtype = str(getattr(vllm_config.cache_config, "cache_dtype", "auto"))
        self._model_dtype = str(getattr(vllm_config.model_config, "dtype", "auto"))
        self._model_sha = _compute_vllm_model_sha(vllm_config.model_config.model)
        self._model_tag = self._model_sha[:12]
        self._topo_tag = self._make_topo_tag(self._tp_rank, self._pp_rank)
        self._base_salt = self._make_topo_salt(self._tp_rank, self._pp_rank)

        self._lock = threading.Lock()
        self._dir_mtime_ns: int = -1
        self._index_by_len: dict[int, set[str]] = {}
        self._staging: dict[tuple[int, str], list[tuple[str, torch.Tensor]]] = {}
        self._kv_caches: dict[str, torch.Tensor] = {}
        self._io_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="cowrie-kv-io")
        self._inflight_saves: set[tuple[int, str]] = set()
        self._inflight_bytes: int = 0
        self._role_name = getattr(role, "name", str(role)).lower()
        self._pid = os.getpid()
        self._instance_id = next(_INSTANCE_COUNTER)
        self._stats_path = (
            self._storage_dir
            / f"{_STATS_FILE_PREFIX}{self._role_name}_{self._topo_tag}_{self._pid}_{self._instance_id}.json"
        )
        self._stats: dict[str, Any] = {
            "role": self._role_name,
            "pid": self._pid,
            "instance_id": self._instance_id,
            "tp_rank": self._tp_rank,
            "tp_size": self._tp_size,
            "pp_rank": self._pp_rank,
            "pp_size": self._pp_size,
            "topo_tag": self._topo_tag,
            "model_sha": self._model_sha,
            "block_size": self._block_size,
            "runtime_id": VLLM_RUNTIME_ID,
            "layout_ver": VLLM_LAYOUT_VER,
            "supports_hma": bool(SUPPORTS_HMA_NATIVE and isinstance(self, _SupportsHMA)),
            "supports_hma_native_import": bool(SUPPORTS_HMA_NATIVE),
            "max_cache_bytes": self._max_cache_bytes,
            "max_inflight_bytes": self._max_inflight_bytes,
            "kv_layout": "",
            "register_kv_caches_calls": 0,
            "registered_layers": 0,
            "get_num_new_matched_tokens_calls": 0,
            "scheduler_hits": 0,
            "scheduler_misses": 0,
            "scheduler_matched_tokens": 0,
            "update_state_after_alloc_load_calls": 0,
            "update_state_after_alloc_store_calls": 0,
            "build_connector_meta_calls": 0,
            "meta_load_requests": 0,
            "meta_store_requests": 0,
            "request_finished_calls": 0,
            "request_finished_all_groups_calls": 0,
            "start_load_kv_calls": 0,
            "loads_completed": 0,
            "loads_failed": 0,
            "loaded_tokens": 0,
            "loaded_bytes": 0,
            "load_ms_total": 0.0,
            "save_kv_layer_calls": 0,
            "wait_for_save_calls": 0,
            "saves_submitted": 0,
            "saves_completed": 0,
            "saves_failed": 0,
            "saves_skipped_inflight_cap": 0,
            "saved_tokens": 0,
            "saved_bytes": 0,
            "save_ms_total": 0.0,
            "shutdown_called": False,
        }
        self._scan_existing_index(force=True)
        self._flush_stats()

    def get_stats(self) -> dict[str, Any]:
        """Return a thread-safe snapshot of this connector instance's runtime counters."""
        with self._lock:
            snap = dict(self._stats)
            snap["inflight_saves"] = len(self._inflight_saves)
            snap["inflight_bytes"] = self._inflight_bytes
            snap["indexed_shards"] = sum(len(v) for v in self._index_by_len.values())
            snap["updated_at_unix"] = round(time.time(), 6)
        return snap

    def _flush_stats(self) -> None:
        snap = self.get_stats()
        tmp_path = (
            self._storage_dir
            / f"{_STATS_FILE_PREFIX}tmp.{self._pid}.{threading.get_ident()}.json"
        )
        try:
            payload = json.dumps(snap, sort_keys=True).encode("utf-8")
            fd = os.open(str(tmp_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                os.write(fd, payload)
            finally:
                os.close(fd)
            os.replace(str(tmp_path), str(self._stats_path))
        except OSError:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _make_topo_tag(self, tp_rank: int, pp_rank: int) -> str:
        return f"tp{tp_rank}of{self._tp_size}_pp{pp_rank}of{self._pp_size}"

    def _make_topo_salt(self, tp_rank: int, pp_rank: int, lora_tag: str = "base") -> str:
        topo = self._make_topo_tag(tp_rank, pp_rank)
        return (
            f"m={self._model_sha}|{topo}|cd={self._cache_dtype}|md={self._model_dtype}"
            f"|lora={lora_tag}|tenant={self._tenant_id}|v={VLLM_LAYOUT_VER}"
        )

    def register_kv_caches(self, kv_caches: dict[str, torch.Tensor]) -> None:
        self._kv_caches = dict(kv_caches)
        with self._lock:
            self._stats["register_kv_caches_calls"] += 1
            self._stats["registered_layers"] = len(self._kv_caches)
        self._flush_stats()

    def request_finished(
        self,
        request: "Request",
        block_ids: list[int],
    ) -> tuple[bool, Optional[dict[str, Any]]]:
        self._requests_need_load.pop(request.request_id, None)
        self._requests_pending_store.pop(request.request_id, None)
        with self._lock:
            self._stats["request_finished_calls"] += 1
        self._flush_stats()
        return False, None

    def request_finished_all_groups(
        self,
        request: "Request",
        block_ids: tuple[list[int], ...],
    ) -> tuple[bool, Optional[dict[str, Any]]]:
        self._requests_need_load.pop(request.request_id, None)
        self._requests_pending_store.pop(request.request_id, None)
        with self._lock:
            self._stats["request_finished_all_groups_calls"] += 1
        self._flush_stats()
        return False, None

    def shutdown(self) -> None:
        self._io_pool.shutdown(wait=True)
        with self._lock:
            self._stats["shutdown_called"] = True
        self._flush_stats()

    def _shard_path(self, num_tokens: int, doc_cid: str, topo_tag: Optional[str] = None) -> Path:
        tt = topo_tag or self._topo_tag
        return self._storage_dir / f"vllm_{self._model_tag}_{tt}_{num_tokens}_{doc_cid[:32]}.state.cowrie"

    def _scan_existing_index(self, force: bool = False) -> None:
        try:
            cur_mtime = os.stat(self._storage_dir).st_mtime_ns
        except OSError:
            return
        prefix = f"vllm_{self._model_tag}_{self._topo_tag}_"
        with self._lock:
            if not force and cur_mtime == self._dir_mtime_ns:
                return
            self._dir_mtime_ns = cur_mtime
            self._index_by_len.clear()
            for f in self._storage_dir.glob(f"{prefix}*.state.cowrie"):
                rest = f.name[len(prefix):]
                parts = rest.split("_", 1)
                if len(parts) == 2 and parts[0].isdigit():
                    nt = int(parts[0])
                    if nt > 0 and nt % self._block_size == 0:
                        self._index_by_len.setdefault(nt, set()).add(f.name)

    def _evict_lru_if_needed(self) -> None:
        if self._max_cache_bytes <= 0:
            return
        entries: list[tuple[float, int, Path]] = []
        total = 0
        for f in self._storage_dir.glob(f"vllm_{self._model_tag}_{self._topo_tag}_*.state.cowrie"):
            try:
                st = f.stat()
            except OSError:
                continue
            total += st.st_size
            entries.append((st.st_mtime, st.st_size, f))
        if total <= self._max_cache_bytes:
            return
        entries.sort(key=lambda x: x[0])  # oldest mtime first
        for _, sz, f in entries:
            if total <= self._max_cache_bytes:
                break
            try:
                f.unlink()
                total -= sz
                with self._lock:
                    for s in self._index_by_len.values():
                        s.discard(f.name)
            except OSError:
                continue

    def _candidate_lengths(self, max_tokens: int) -> list[int]:
        # Multi-process vLLM V1: refresh directory index when worker processes persist new shards.
        self._scan_existing_index(force=False)
        with self._lock:
            lengths = [nt for nt, files in self._index_by_len.items() if files and 0 < nt <= max_tokens]
        lengths.sort(reverse=True)
        return lengths

    def _all_ranks_ready(
        self, token_ids: list[int], num_tokens: int, mm_hashes: list[str], lora_tag: str
    ) -> bool:
        if self._tp_size <= 1 and self._pp_size <= 1:
            return True
        for r_tp in range(self._tp_size):
            for r_pp in range(self._pp_size):
                rsalt = self._make_topo_salt(r_tp, r_pp, lora_tag)
                rcid = _hash_prefix(token_ids, num_tokens, mm_hashes, rsalt)
                rsp = self._shard_path(num_tokens, rcid, self._make_topo_tag(r_tp, r_pp))
                if not rsp.is_file():
                    return False
        return True

    def _find_longest_match(
        self, token_ids: list[int], mm_hashes: list[str], lora_tag: str = "base"
    ) -> Optional[tuple[str, int, Path]]:
        max_valid = aligned_prefix_tokens(len(token_ids), self._block_size)
        if max_valid <= 0:
            return None
        salt = self._make_topo_salt(self._tp_rank, self._pp_rank, lora_tag)
        for nt in self._candidate_lengths(max_valid):
            cid = _hash_prefix(token_ids, nt, mm_hashes, salt)
            sp = self._shard_path(nt, cid)
            if not sp.is_file():
                continue
            if not self._all_ranks_ready(token_ids, nt, mm_hashes, lora_tag):
                continue
            try:
                cowrie.verify_statepage_file(
                    str(sp),
                    expected_model_sha=self._model_sha,
                    expected_doc_cid=cid,
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
            _extract_mm_hashes(request),
            _request_lora_tag(request),
        )
        if hit is None:
            with self._lock:
                self._stats["get_num_new_matched_tokens_calls"] += 1
                self._stats["scheduler_misses"] += 1
            self._flush_stats()
            return 0, False
        _, matched_tokens, _ = hit
        if matched_tokens <= num_computed_tokens:
            with self._lock:
                self._stats["get_num_new_matched_tokens_calls"] += 1
                self._stats["scheduler_misses"] += 1
            self._flush_stats()
            return 0, False
        new_matched = matched_tokens - num_computed_tokens
        with self._lock:
            self._stats["get_num_new_matched_tokens_calls"] += 1
            self._stats["scheduler_hits"] += 1
            self._stats["scheduler_matched_tokens"] += int(new_matched)
        self._flush_stats()
        return new_matched, False

    def update_state_after_alloc(
        self,
        request: "Request",
        blocks: "KVCacheBlocks",
        num_external_tokens: int,
    ) -> None:
        if num_external_tokens > 0:
            hit = self._find_longest_match(
                list(request.prompt_token_ids or []),
                _extract_mm_hashes(request),
                _request_lora_tag(request),
            )
            matched = hit[1] if hit is not None else num_external_tokens
            self._requests_need_load[request.request_id] = (request, matched)
            with self._lock:
                self._stats["update_state_after_alloc_load_calls"] += 1
        else:
            self._requests_pending_store[request.request_id] = request
            with self._lock:
                self._stats["update_state_after_alloc_store_calls"] += 1

    def build_connector_meta(
        self,
        scheduler_output: SchedulerOutput,
    ) -> KVConnectorMetadata:
        meta = CowrieConnectorMetadata()
        for new_req in scheduler_output.scheduled_new_reqs:
            token_ids = list(new_req.prompt_token_ids or [])
            mm_hashes = _extract_mm_hashes(new_req)
            req_lora = _request_lora_tag(new_req)
            req_salt = self._make_topo_salt(self._tp_rank, self._pp_rank, req_lora)
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
                    req_salt,
                )
                if rm is not None:
                    meta.requests.append(rm)
            else:
                num_sched = int(scheduler_output.num_scheduled_tokens.get(new_req.req_id, len(token_ids)))
                alloc_cap = len(new_req.block_ids[0]) * self._block_size
                ready_toks = min(num_sched, len(token_ids))
                store_toks = min(aligned_prefix_tokens(ready_toks, self._block_size), alloc_cap)
                store_toks = (store_toks // self._block_size) * self._block_size
                if store_toks > 0:
                    cid = _hash_prefix(token_ids, store_toks, mm_hashes, req_salt)
                    if not self._shard_path(store_toks, cid).is_file():
                        rm = CowrieReqMeta.build(
                            new_req.req_id,
                            token_ids,
                            new_req.block_ids[0],
                            self._block_size,
                            store_toks,
                            True,
                            mm_hashes,
                            req_salt,
                        )
                        if rm is not None:
                            meta.requests.append(rm)
                            if store_toks >= aligned_prefix_tokens(len(token_ids), self._block_size):
                                self._requests_pending_store.pop(new_req.req_id, None)

        cached_reqs = scheduler_output.scheduled_cached_reqs
        for i, req_id in enumerate(cached_reqs.req_ids):
            total_tokens = cached_reqs.num_computed_tokens[i] + scheduler_output.num_scheduled_tokens[req_id]
            new_block_ids = cached_reqs.new_block_ids[i]
            if req_id in cached_reqs.resumed_req_ids and req_id in self._requests_need_load:
                request, matched_toks = self._requests_need_load[req_id]
                req_salt = self._make_topo_salt(self._tp_rank, self._pp_rank, _request_lora_tag(request))
                if new_block_ids is not None:
                    rm = CowrieReqMeta.build(
                        req_id,
                        list(request.all_token_ids[:total_tokens]),
                        new_block_ids[0],
                        self._block_size,
                        min(matched_toks, aligned_prefix_tokens(total_tokens, self._block_size)),
                        False,
                        _extract_mm_hashes(request),
                        req_salt,
                    )
                    if rm is not None:
                        meta.requests.append(rm)
            elif req_id in self._requests_pending_store:
                # Chunked prefill completion: save full block-aligned prompt prefix once computed.
                request = self._requests_pending_store[req_id]
                prompt_toks = list(request.prompt_token_ids or [])
                target_store = aligned_prefix_tokens(len(prompt_toks), self._block_size)
                if target_store > 0 and total_tokens >= target_store and new_block_ids is not None:
                    mm_hashes = _extract_mm_hashes(request)
                    req_salt = self._make_topo_salt(self._tp_rank, self._pp_rank, _request_lora_tag(request))
                    cid = _hash_prefix(prompt_toks, target_store, mm_hashes, req_salt)
                    if not self._shard_path(target_store, cid).is_file():
                        rm = CowrieReqMeta.build(
                            req_id,
                            prompt_toks,
                            new_block_ids[0],
                            self._block_size,
                            target_store,
                            True,
                            mm_hashes,
                            req_salt,
                        )
                        if rm is not None:
                            meta.requests.append(rm)
                    self._requests_pending_store.pop(req_id, None)

        self._requests_need_load.clear()
        n_load = sum(1 for r in meta.requests if not r.is_store)
        n_store = sum(1 for r in meta.requests if r.is_store)
        with self._lock:
            self._stats["build_connector_meta_calls"] += 1
            self._stats["meta_load_requests"] += n_load
            self._stats["meta_store_requests"] += n_store
            should_flush = bool(meta.requests) or self._stats["build_connector_meta_calls"] == 1
        if should_flush:
            self._flush_stats()
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
        is_mla = _is_mla_metadata(attn_metadata)

        attn_layers: list[tuple[str, torch.Tensor]] = []
        if self._kv_caches:
            for layer_name in sorted(self._kv_caches.keys()):
                attn_layers.append((layer_name, self._kv_caches[layer_name]))
        else:
            for layer_name in sorted(forward_context.no_compile_layers.keys()):
                layer = forward_context.no_compile_layers[layer_name]
                kv_attr = getattr(layer, "kv_cache", None)
                if kv_attr is not None:
                    attn_layers.append((layer_name, kv_attr[forward_context.virtual_engine]))
        if not attn_layers:
            return

        sample_layer = attn_layers[0][1]
        layout = _detect_kv_layout(sample_layer, is_mla)
        with self._lock:
            self._stats["start_load_kv_calls"] += 1
            if not self._stats["kv_layout"]:
                self._stats["kv_layout"] = layout
        kv_quant, _ = _kv_dtype_tag(sample_layer.dtype)
        any_load = False

        for req in metadata.requests:
            if req.is_store:
                continue
            any_load = True
            sp = self._shard_path(req.num_tokens, req.doc_cid)
            t_load0 = time.perf_counter()
            try:
                with cowrie.MappedStatePageView(
                    str(sp),
                    expected_model_sha=self._model_sha,
                    expected_doc_cid=req.doc_cid,
                    expected_runtime_id=VLLM_RUNTIME_ID,
                    expected_kv_quant=kv_quant,
                    verify_hash=True,
                ) as mp:
                    n_bytes = len(mp.buffer)
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        raw_u8 = torch.frombuffer(mp.buffer, dtype=torch.uint8)
                    stacked_cpu = raw_u8.view(sample_layer.dtype)
                    stacked_gpu = stacked_cpu.to(device=sample_layer.device, non_blocking=True)
                    slots = req.slot_mapping.to(device=sample_layer.device, non_blocking=True)
                    n_layers = len(attn_layers)

                    if layout == "mla":
                        stacked_gpu = stacked_gpu.reshape(n_layers, req.num_tokens, -1)
                        for idx, (_, dst_layer) in enumerate(attn_layers):
                            orig_shape = dst_layer.shape
                            flat = dst_layer.reshape(orig_shape[0] * orig_shape[1], -1)
                            flat[slots, ...] = stacked_gpu[idx]
                    elif layout == "2_blocks_first":
                        stacked_gpu = stacked_gpu.reshape(n_layers, 2, req.num_tokens, -1)
                        for idx, (_, dst_layer) in enumerate(attn_layers):
                            orig_shape = dst_layer.shape
                            flat = dst_layer.reshape(2, orig_shape[1] * orig_shape[2], -1)
                            flat[:, slots, ...] = stacked_gpu[idx]
                    else:  # "blocks_2_first": (num_blocks, 2, block_size, ...)
                        stacked_gpu = stacked_gpu.reshape(n_layers, 2, req.num_tokens, -1)
                        for idx, (_, dst_layer) in enumerate(attn_layers):
                            page_size = dst_layer.shape[2]
                            tail_shape = dst_layer.shape[3:]
                            blk_idx = slots // page_size
                            blk_off = slots % page_size
                            dst_layer[blk_idx, 0, blk_off, ...] = stacked_gpu[idx, 0].reshape(
                                req.num_tokens, *tail_shape
                            )
                            dst_layer[blk_idx, 1, blk_off, ...] = stacked_gpu[idx, 1].reshape(
                                req.num_tokens, *tail_shape
                            )
                    try:
                        os.utime(sp, None)
                    except OSError:
                        pass
                load_ms = (time.perf_counter() - t_load0) * 1000.0
                with self._lock:
                    self._stats["loads_completed"] += 1
                    self._stats["loaded_tokens"] += int(req.num_tokens)
                    self._stats["loaded_bytes"] += int(n_bytes)
                    self._stats["load_ms_total"] = round(self._stats["load_ms_total"] + load_ms, 3)
            except (cowrie.CowrieError, RuntimeError, ValueError, OSError) as exc:
                # Fail CLOSED: scheduler already credited external prefix tokens for `req`;
                # proceeding on uninitialized GPU KV blocks would silently corrupt model output.
                with self._lock:
                    self._stats["loads_failed"] += 1
                self._flush_stats()
                logger.error("Cowrie StatePage verified load failed for %s: %s", sp.name, exc)
                raise RuntimeError(
                    f"Cowrie StatePage verified restore failed for {sp.name} (refusing uninitialized KV reuse): {exc}"
                ) from exc
        if any_load:
            self._flush_stats()

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
        is_mla = _is_mla_metadata(attn_metadata)
        layout = _detect_kv_layout(kv_layer, is_mla)
        with self._lock:
            self._stats["save_kv_layer_calls"] += 1
            if not self._stats["kv_layout"]:
                self._stats["kv_layout"] = layout

        for req in metadata.requests:
            if not req.is_store:
                continue
            slots = req.slot_mapping.to(device=kv_layer.device)
            if layout == "mla":
                num_pages, page_size = kv_layer.shape[0], kv_layer.shape[1]
                sliced = kv_layer.reshape(num_pages * page_size, -1)[slots, ...].contiguous().cpu()
            elif layout == "2_blocks_first":
                num_pages, page_size = kv_layer.shape[1], kv_layer.shape[2]
                sliced = kv_layer.reshape(2, num_pages * page_size, -1)[:, slots, ...].contiguous().cpu()
            else:  # "blocks_2_first": (num_blocks, 2, block_size, ...)
                page_size = kv_layer.shape[2]
                blk_idx = slots // page_size
                blk_off = slots % page_size
                k_sl = kv_layer[blk_idx, 0, blk_off, ...].reshape(req.num_tokens, -1)
                v_sl = kv_layer[blk_idx, 1, blk_off, ...].reshape(req.num_tokens, -1)
                sliced = torch.stack((k_sl, v_sl), dim=0).contiguous().cpu()
            self._staging.setdefault((req.num_tokens, req.doc_cid), []).append((layer_name, sliced))

    def _persist_shard_bg(
        self,
        sp: Path,
        num_tokens: int,
        doc_cid: str,
        kv_quant: int,
        raw_bytes: bytes,
    ) -> None:
        key = (num_tokens, doc_cid)
        t_save0 = time.perf_counter()
        try:
            cowrie.save_statepage(
                str(sp),
                self._model_sha,
                doc_cid,
                num_tokens,
                raw_bytes,
                kv_quant=kv_quant,
                ssm_dtype=0,
                runtime_id=VLLM_RUNTIME_ID,
                layout_ver=VLLM_LAYOUT_VER,
            )
            save_ms = (time.perf_counter() - t_save0) * 1000.0
            with self._lock:
                self._index_by_len.setdefault(num_tokens, set()).add(sp.name)
                self._stats["saves_completed"] += 1
                self._stats["saved_tokens"] += int(num_tokens)
                self._stats["saved_bytes"] += len(raw_bytes)
                self._stats["save_ms_total"] = round(self._stats["save_ms_total"] + save_ms, 3)
            self._evict_lru_if_needed()
        except Exception as exc:
            with self._lock:
                self._stats["saves_failed"] += 1
            logger.warning("Cowrie background StatePage save failed for %s: %s", sp.name, exc)
        finally:
            with self._lock:
                self._inflight_saves.discard(key)
                self._inflight_bytes = max(0, self._inflight_bytes - len(raw_bytes))
            self._flush_stats()

    def wait_for_save(self) -> None:
        metadata = self._get_connector_metadata()
        if not isinstance(metadata, CowrieConnectorMetadata) or not self._staging:
            return
        with self._lock:
            self._stats["wait_for_save_calls"] += 1
        any_change = False
        for req in metadata.requests:
            key = (req.num_tokens, req.doc_cid)
            if not req.is_store or key not in self._staging:
                continue
            items = self._staging.pop(key)
            items.sort(key=lambda kv: kv[0])
            stacked = torch.stack([t for _, t in items], dim=0).contiguous()
            kv_quant, _ = _kv_dtype_tag(stacked.dtype)
            raw_bytes = stacked.view(torch.uint8).numpy().tobytes()
            nbytes = len(raw_bytes)
            with self._lock:
                if key in self._inflight_saves:
                    continue
                if self._max_inflight_bytes > 0 and (self._inflight_bytes + nbytes) > self._max_inflight_bytes:
                    self._stats["saves_skipped_inflight_cap"] += 1
                    any_change = True
                    logger.warning(
                        "Skipping Cowrie KV save (%d B): host-RAM inflight cap %d B reached",
                        nbytes,
                        self._max_inflight_bytes,
                    )
                    continue
                self._inflight_saves.add(key)
                self._inflight_bytes += nbytes
                self._stats["saves_submitted"] += 1
                any_change = True
            sp = self._shard_path(req.num_tokens, req.doc_cid)
            self._io_pool.submit(
                self._persist_shard_bg, sp, req.num_tokens, req.doc_cid, kv_quant, raw_bytes
            )
        self._staging.clear()
        if any_change:
            self._flush_stats()


def register_vllm_plugin() -> None:
    """Entry-point hook for `vllm.general_plugins` so CowrieKVConnectorV1 is auto-discoverable."""
    from cowrie_vllm_adapters._plugin import register_vllm_plugin as _reg
    _reg()
