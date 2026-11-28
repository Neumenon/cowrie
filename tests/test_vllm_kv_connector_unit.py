#!/usr/bin/env python3
"""Local unit tests for `adapters/vllm/cowrie_kv_connector.py` and `adapters/vllm/_plugin.py`.

Exercises the real Rust `libcowrie_core.so` StatePage save/verify/mmap-restore path,
`SupportsHMA` inheritance, `KVConnectorFactory` registration, runtime counters &
persisted `.cowrie_stats_*.json` aggregation, both FlashAttention `(2, blocks, ...)`
and FlashInfer `(blocks, 2, ...)` tensor layouts, inflight backpressure accounting,
and fail-closed tamper rejection — without requiring a GPU or an installed `vllm`/`torch`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import importlib
import os
from pathlib import Path
import sys
import tempfile
import types

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "python") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "python"))

import cowrie


class _FakeTensor:
    """Minimal NumPy-backed tensor shim implementing only the ops used by CowrieKVConnectorV1."""

    def __init__(self, arr: np.ndarray, device: str = "cpu"):
        self._arr = np.asarray(arr)
        self.device = device

    @property
    def shape(self) -> tuple[int, ...]:
        return self._arr.shape

    @property
    def dtype(self):
        return self._arr.dtype

    @property
    def ndim(self) -> int:
        return self._arr.ndim

    def numel(self) -> int:
        return int(self._arr.size)

    def to(self, device: str = "cpu", non_blocking: bool = False) -> "_FakeTensor":
        return _FakeTensor(self._arr, device=device)

    def cpu(self) -> "_FakeTensor":
        return _FakeTensor(self._arr.copy(), device="cpu")

    def contiguous(self) -> "_FakeTensor":
        return _FakeTensor(np.ascontiguousarray(self._arr), device=self.device)

    def reshape(self, *shape) -> "_FakeTensor":
        if len(shape) == 1 and isinstance(shape[0], (tuple, list)):
            shape = tuple(shape[0])
        return _FakeTensor(self._arr.reshape(shape), device=self.device)

    def view(self, dtype) -> "_FakeTensor":
        return _FakeTensor(np.ascontiguousarray(self._arr).view(dtype), device=self.device)

    def numpy(self) -> np.ndarray:
        return self._arr

    def flatten(self) -> "_FakeTensor":
        return _FakeTensor(self._arr.reshape(-1), device=self.device)

    def unsqueeze(self, axis: int) -> "_FakeTensor":
        return _FakeTensor(np.expand_dims(self._arr, axis=axis), device=self.device)

    def __add__(self, other) -> "_FakeTensor":
        rhs = other._arr if isinstance(other, _FakeTensor) else other
        return _FakeTensor(self._arr + rhs, device=self.device)

    def __mul__(self, other) -> "_FakeTensor":
        rhs = other._arr if isinstance(other, _FakeTensor) else other
        return _FakeTensor(self._arr * rhs, device=self.device)

    def __floordiv__(self, other) -> "_FakeTensor":
        rhs = other._arr if isinstance(other, _FakeTensor) else other
        return _FakeTensor(self._arr // rhs, device=self.device)

    def __mod__(self, other) -> "_FakeTensor":
        rhs = other._arr if isinstance(other, _FakeTensor) else other
        return _FakeTensor(self._arr % rhs, device=self.device)

    def __getitem__(self, idx) -> "_FakeTensor":
        idx_np = _unwrap_idx(idx)
        return _FakeTensor(self._arr[idx_np], device=self.device)

    def __setitem__(self, idx, value) -> None:
        idx_np = _unwrap_idx(idx)
        val_np = value._arr if isinstance(value, _FakeTensor) else value
        self._arr[idx_np] = val_np


def _unwrap_idx(idx):
    if isinstance(idx, _FakeTensor):
        return idx._arr
    if isinstance(idx, tuple):
        return tuple(x._arr if isinstance(x, _FakeTensor) else x for x in idx)
    return idx


def _install_mock_torch_and_vllm():
    """Install minimal in-memory mock `torch` and `vllm` packages for isolated unit testing."""
    torch_mod = types.ModuleType("torch")
    torch_mod.Tensor = _FakeTensor
    torch_mod.dtype = np.dtype
    torch_mod.float16 = np.dtype(np.float16)
    torch_mod.bfloat16 = np.dtype(np.uint16)  # 2-byte stand-in for layout tests
    torch_mod.float32 = np.dtype(np.float32)
    torch_mod.int8 = np.dtype(np.int8)
    torch_mod.uint8 = np.dtype(np.uint8)
    torch_mod.long = np.dtype(np.int64)
    torch_mod.tensor = lambda data, dtype=None: _FakeTensor(np.array(data, dtype=dtype))
    torch_mod.arange = lambda start, stop, dtype=None: _FakeTensor(np.arange(start, stop, dtype=dtype))
    torch_mod.frombuffer = lambda buf, dtype=np.uint8: _FakeTensor(np.frombuffer(buf, dtype=dtype))
    torch_mod.stack = lambda seq, dim=0: _FakeTensor(np.stack([t._arr for t in seq], axis=dim))
    sys.modules["torch"] = torch_mod

    class KVConnectorRole(Enum):
        SCHEDULER = 1
        WORKER = 2

    class KVConnectorMetadata:
        pass

    class SupportsHMA:
        pass

    class KVConnectorBase_V1:
        def __init__(self, vllm_config, role, kv_cache_config=None):
            self._vllm_config = vllm_config
            self._role = role
            self._kv_cache_config = kv_cache_config
            self._kv_transfer_config = vllm_config.kv_transfer_config
            self._bound_meta = None

        def bind_connector_metadata(self, meta):
            self._bound_meta = meta

        def _get_connector_metadata(self):
            return self._bound_meta

    class KVConnectorFactory:
        _registry: dict[str, object] = {}

        @classmethod
        def register_connector(cls, name: str, module_path: str, class_name: str) -> None:
            if name in cls._registry:
                raise ValueError(f"Already registered: {name}")

            def _loader():
                mod = importlib.import_module(module_path)
                return getattr(mod, class_name)

            cls._registry[name] = _loader

        @classmethod
        def get_connector_class(cls, kv_transfer_config):
            return cls._registry[kv_transfer_config.kv_connector]()

    class VllmConfig:
        pass

    class AttentionMetadata:
        pass

    class SchedulerOutput:
        pass

    class _DummyLogger:
        def warning(self, *args, **kwargs):
            pass

        def error(self, *args, **kwargs):
            pass

        def info(self, *args, **kwargs):
            pass

    def _pkg(name: str):
        m = types.ModuleType(name)
        m.__path__ = []
        sys.modules[name] = m
        return m

    _pkg("vllm")
    vllm_cfg = _pkg("vllm.config")
    vllm_cfg.VllmConfig = VllmConfig
    _pkg("vllm.distributed")
    _pkg("vllm.distributed.kv_transfer")
    _pkg("vllm.distributed.kv_transfer.kv_connector")
    v1_pkg = _pkg("vllm.distributed.kv_transfer.kv_connector.v1")
    v1_base = types.ModuleType("vllm.distributed.kv_transfer.kv_connector.v1.base")
    v1_base.KVConnectorBase_V1 = KVConnectorBase_V1
    v1_base.KVConnectorMetadata = KVConnectorMetadata
    v1_base.KVConnectorRole = KVConnectorRole
    v1_base.SupportsHMA = SupportsHMA
    sys.modules["vllm.distributed.kv_transfer.kv_connector.v1.base"] = v1_base
    v1_pkg.base = v1_base

    fac_mod = types.ModuleType("vllm.distributed.kv_transfer.kv_connector.factory")
    fac_mod.KVConnectorFactory = KVConnectorFactory
    sys.modules["vllm.distributed.kv_transfer.kv_connector.factory"] = fac_mod

    logger_mod = types.ModuleType("vllm.logger")
    logger_mod.init_logger = lambda _name: _DummyLogger()
    sys.modules["vllm.logger"] = logger_mod

    _pkg("vllm.v1")
    _pkg("vllm.v1.attention")
    attn_mod = types.ModuleType("vllm.v1.attention.backend")
    attn_mod.AttentionMetadata = AttentionMetadata
    sys.modules["vllm.v1.attention.backend"] = attn_mod

    _pkg("vllm.v1.core")
    _pkg("vllm.v1.core.sched")
    sched_mod = types.ModuleType("vllm.v1.core.sched.output")
    sched_mod.SchedulerOutput = SchedulerOutput
    sys.modules["vllm.v1.core.sched.output"] = sched_mod

    # Alias `cowrie_vllm_adapters` to `adapters/vllm` just as pyproject.toml maps it.
    pkg_dir = REPO_ROOT / "adapters" / "vllm"
    spec = importlib.util.spec_from_file_location(
        "cowrie_vllm_adapters",
        pkg_dir / "__init__.py",
        submodule_search_locations=[str(pkg_dir)],
    )
    assert spec is not None and spec.loader is not None
    cva_mod = importlib.util.module_from_spec(spec)
    sys.modules["cowrie_vllm_adapters"] = cva_mod
    spec.loader.exec_module(cva_mod)

    return KVConnectorFactory, KVConnectorRole, SupportsHMA


@dataclass
class _FakeReq:
    request_id: str
    prompt_token_ids: list[int]
    mm_features: list = field(default_factory=list)


@dataclass
class _FakeNewReq:
    req_id: str
    prompt_token_ids: list[int]
    block_ids: tuple[list[int], ...]
    mm_features: list = field(default_factory=list)


@dataclass
class _FakeCachedReqs:
    req_ids: list[str] = field(default_factory=list)
    num_computed_tokens: list[int] = field(default_factory=list)
    new_block_ids: list = field(default_factory=list)
    resumed_req_ids: set[str] = field(default_factory=set)


@dataclass
class _FakeSchedOut:
    scheduled_new_reqs: list[_FakeNewReq]
    num_scheduled_tokens: dict[str, int]
    scheduled_cached_reqs: _FakeCachedReqs = field(default_factory=_FakeCachedReqs)


@dataclass
class _FakeForwardCtx:
    attn_metadata: object = field(default_factory=object)
    no_compile_layers: dict = field(default_factory=dict)
    virtual_engine: int = 0


def _make_vllm_config(state_dir: Path, max_inflight_bytes: int = 64 * 1024 * 1024):
    return types.SimpleNamespace(
        cache_config=types.SimpleNamespace(block_size=16, cache_dtype="auto"),
        model_config=types.SimpleNamespace(model="Qwen/Qwen3-32B", dtype="bfloat16"),
        parallel_config=types.SimpleNamespace(tensor_parallel_size=1, pipeline_parallel_size=1),
        kv_transfer_config=types.SimpleNamespace(
            kv_connector="CowrieKVConnectorV1",
            kv_role="kv_both",
            kv_connector_extra_config={
                "cowrie_state_dir": str(state_dir),
                "cowrie_max_inflight_bytes": max_inflight_bytes,
            },
        ),
    )


def _run_roundtrip_for_layout(
    connector_cls,
    KVConnectorRole,
    read_connector_stats,
    reset_connector_stats,
    layout_name: str,
) -> None:
    rng = np.random.default_rng(20261128)
    n_layers = 4
    num_blocks = 8
    block_size = 16
    n_kv = 2
    head_dim = 8
    prompt_tokens = list(range(100, 100 + 49))  # aligned prefix = 48 tokens = 3 blocks
    block_ids = [1, 3, 5]

    with tempfile.TemporaryDirectory(prefix=f"cowrie_vllm_unit_{layout_name}_") as td:
        state_dir = Path(td)
        cfg = _make_vllm_config(state_dir)

        # --- Phase 1: Store ---
        sched1 = connector_cls(cfg, KVConnectorRole.SCHEDULER)
        work1 = connector_cls(cfg, KVConnectorRole.WORKER)

        req1 = _FakeReq("req-store-1", prompt_tokens)
        matched1, async1 = sched1.get_num_new_matched_tokens(req1, 0)
        assert (matched1, async1) == (0, False)
        sched1.update_state_after_alloc(req1, None, 0)
        meta1 = sched1.build_connector_meta(
            _FakeSchedOut(
                scheduled_new_reqs=[_FakeNewReq("req-store-1", prompt_tokens, (block_ids,))],
                num_scheduled_tokens={"req-store-1": len(prompt_tokens)},
            )
        )
        assert len(meta1.requests) == 1 and meta1.requests[0].is_store is True
        assert meta1.requests[0].num_tokens == 48

        kv_src = {}
        for l_idx in range(n_layers):
            if layout_name == "2_blocks_first":
                arr = rng.standard_normal((2, num_blocks, block_size, n_kv, head_dim)).astype(np.float16)
            else:
                arr = rng.standard_normal((num_blocks, 2, block_size, n_kv, head_dim)).astype(np.float16)
            kv_src[f"layer_{l_idx:02d}"] = _FakeTensor(arr)

        work1.register_kv_caches(kv_src)
        work1.bind_connector_metadata(meta1)
        work1.start_load_kv(_FakeForwardCtx())
        for lname, tensor in kv_src.items():
            work1.save_kv_layer(lname, tensor, object())
        work1.wait_for_save()
        sched1.request_finished_all_groups(req1, (block_ids,))
        work1.shutdown()
        sched1.shutdown()

        store_stats = read_connector_stats(state_dir)
        t1 = store_stats["totals"]
        assert t1["all_supports_hma"] is True
        assert t1["saves_completed"] == 1
        assert t1["saved_tokens"] == 48
        assert t1["saved_bytes"] > 0
        assert t1["save_kv_layer_calls"] == n_layers
        assert t1["request_finished_all_groups_calls"] == 1
        assert t1["kv_layouts"] == [layout_name]
        shards = list(state_dir.glob("*.state.cowrie"))
        assert len(shards) == 1

        # Verify shard independently via libcowrie_core.so
        meta_verified = cowrie.verify_statepage_file(
            str(shards[0]),
            expected_model_sha=work1._model_sha,
            expected_doc_cid=meta1.requests[0].doc_cid,
            expected_runtime_id="vllm-v1",
            verify_hash=True,
        )
        assert meta_verified["ctx_tokens"] == 48

        # --- Phase 2: Restore in fresh connector instances ---
        removed = reset_connector_stats(state_dir)
        assert removed >= 2

        sched2 = connector_cls(cfg, KVConnectorRole.SCHEDULER)
        work2 = connector_cls(cfg, KVConnectorRole.WORKER)

        req2 = _FakeReq("req-restore-1", prompt_tokens + [999, 1000])  # new suffix!
        matched2, async2 = sched2.get_num_new_matched_tokens(req2, 0)
        assert (matched2, async2) == (48, False), f"Expected 48 matched tokens, got {(matched2, async2)}"
        sched2.update_state_after_alloc(req2, None, matched2)
        restore_blocks = [2, 4, 6]
        meta2 = sched2.build_connector_meta(
            _FakeSchedOut(
                scheduled_new_reqs=[_FakeNewReq("req-restore-1", req2.prompt_token_ids, (restore_blocks,))],
                num_scheduled_tokens={"req-restore-1": len(req2.prompt_token_ids)},
            )
        )
        assert len(meta2.requests) == 1 and meta2.requests[0].is_store is False

        kv_dst = {}
        for l_idx in range(n_layers):
            shape = (
                (2, num_blocks, block_size, n_kv, head_dim)
                if layout_name == "2_blocks_first"
                else (num_blocks, 2, block_size, n_kv, head_dim)
            )
            kv_dst[f"layer_{l_idx:02d}"] = _FakeTensor(np.zeros(shape, dtype=np.float16))

        work2.register_kv_caches(kv_dst)
        work2.bind_connector_metadata(meta2)
        work2.start_load_kv(_FakeForwardCtx())
        sched2.request_finished_all_groups(req2, (restore_blocks,))
        work2.shutdown()
        sched2.shutdown()

        # Bit-for-bit equality check on restored blocks!
        for l_idx in range(n_layers):
            src_arr = kv_src[f"layer_{l_idx:02d}"].numpy()
            dst_arr = kv_dst[f"layer_{l_idx:02d}"].numpy()
            for src_b, dst_b in zip(block_ids, restore_blocks):
                if layout_name == "2_blocks_first":
                    assert np.array_equal(src_arr[:, src_b], dst_arr[:, dst_b]), (
                        f"Layer {l_idx} block {src_b}->{dst_b} mismatch in {layout_name}"
                    )
                else:
                    assert np.array_equal(src_arr[src_b], dst_arr[dst_b]), (
                        f"Layer {l_idx} block {src_b}->{dst_b} mismatch in {layout_name}"
                    )

        restore_stats = read_connector_stats(state_dir)
        t2 = restore_stats["totals"]
        assert t2["all_supports_hma"] is True
        assert t2["scheduler_hits"] == 1
        assert t2["scheduler_matched_tokens"] == 48
        assert t2["loads_completed"] == 1
        assert t2["loaded_tokens"] == 48
        assert t2["loaded_bytes"] == t1["saved_bytes"]
        assert t2["loads_failed"] == 0
        assert t2["request_finished_all_groups_calls"] == 1

        # --- Phase 3: Tampered shard MUST fail closed and increment loads_failed ---
        raw = bytearray(shards[0].read_bytes())
        raw[16384 + 7] ^= 0x01
        shards[0].write_bytes(bytes(raw))
        work3 = connector_cls(cfg, KVConnectorRole.WORKER)
        work3.register_kv_caches(kv_dst)
        work3.bind_connector_metadata(meta2)
        failed = False
        try:
            work3.start_load_kv(_FakeForwardCtx())
        except RuntimeError:
            failed = True
        assert failed, "1-bit tampered .state.cowrie shard MUST fail closed!"
        assert work3.get_stats()["loads_failed"] == 1
        work3.shutdown()


def main() -> int:
    old_hmac = os.environ.get("COWRIE_HMAC_KEY")
    os.environ["COWRIE_HMAC_KEY"] = "3c" * 32
    try:
        KVConnectorFactory, KVConnectorRole, SupportsHMA = _install_mock_torch_and_vllm()
        from cowrie_vllm_adapters._plugin import register_vllm_plugin
        from cowrie_vllm_adapters.cowrie_kv_connector import (
            CowrieKVConnectorV1,
            SUPPORTS_HMA_NATIVE,
            read_connector_stats,
            reset_connector_stats,
        )

        # 1. Idempotent plugin registration + factory resolution + SupportsHMA
        register_vllm_plugin()
        register_vllm_plugin()  # idempotent
        cfg_stub = types.SimpleNamespace(kv_connector="CowrieKVConnectorV1")
        resolved = KVConnectorFactory.get_connector_class(cfg_stub)
        assert resolved is CowrieKVConnectorV1
        assert SUPPORTS_HMA_NATIVE is True
        assert issubclass(resolved, SupportsHMA)
        print("PASS 1: KVConnectorFactory registration & SupportsHMA inheritance verified")

        # 2. FlashAttention (2, blocks, block_size, n_kv, head_dim) roundtrip + tamper fail-closed
        _run_roundtrip_for_layout(
            resolved, KVConnectorRole, read_connector_stats, reset_connector_stats, "2_blocks_first"
        )
        print("PASS 2: FlashAttention (2, blocks, ...) store/restore bit-exact parity, counters & tamper rejection")

        # 3. FlashInfer (blocks, 2, block_size, n_kv, head_dim) roundtrip + tamper fail-closed
        _run_roundtrip_for_layout(
            resolved, KVConnectorRole, read_connector_stats, reset_connector_stats, "blocks_2_first"
        )
        print("PASS 3: FlashInfer (blocks, 2, ...) store/restore bit-exact parity, counters & tamper rejection")

        # 4. Inflight byte-cap skip accounting (prevents silent skips!)
        with tempfile.TemporaryDirectory(prefix="cowrie_vllm_cap_") as td:
            cfg_tiny_cap = _make_vllm_config(Path(td), max_inflight_bytes=64)
            work_cap = resolved(cfg_tiny_cap, KVConnectorRole.WORKER)
            sched_cap = resolved(cfg_tiny_cap, KVConnectorRole.SCHEDULER)
            toks = list(range(1, 34))  # 32 aligned tokens
            sched_cap.update_state_after_alloc(_FakeReq("r-cap", toks), None, 0)
            meta_cap = sched_cap.build_connector_meta(
                _FakeSchedOut(
                    scheduled_new_reqs=[_FakeNewReq("r-cap", toks, ([0, 1],))],
                    num_scheduled_tokens={"r-cap": len(toks)},
                )
            )
            kv = {"layer_00": _FakeTensor(np.zeros((2, 4, 16, 2, 8), dtype=np.float16))}
            work_cap.register_kv_caches(kv)
            work_cap.bind_connector_metadata(meta_cap)
            work_cap.save_kv_layer("layer_00", kv["layer_00"], object())
            work_cap.wait_for_save()
            st = work_cap.get_stats()
            assert st["saves_skipped_inflight_cap"] == 1 and st["saves_submitted"] == 0
            work_cap.shutdown()
            sched_cap.shutdown()
        print("PASS 4: Inflight backpressure skip counter verified")
        print("VLLM KV CONNECTOR UNIT SUITE: 4/4 PASSED")
        return 0
    finally:
        if old_hmac is None:
            os.environ.pop("COWRIE_HMAC_KEY", None)
        else:
            os.environ["COWRIE_HMAC_KEY"] = old_hmac


if __name__ == "__main__":
    raise SystemExit(main())
