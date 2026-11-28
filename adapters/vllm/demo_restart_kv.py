#!/usr/bin/env python3
"""Portable cross-process vLLM V1 + `CowrieKVConnectorV1` restart demo.

Demonstrates zero-migration cross-process KV prefix persistence on local NVMe using two
completely separate OS processes (`--phase store`, full VRAM teardown, then `--phase restore`).

Quickstart:
    pip install cowrie-ai
    python adapters/vllm/demo_restart_kv.py --model Qwen/Qwen3-0.6B --state-dir /tmp/cowrie_vllm_demo
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


def _make_long_prompt(repeats: int = 120) -> str:
    rows = [
        f"node=gpu-{i:03d} rack=r{i % 8} temp_c={54 + (i % 19)} util_pct={78 + (i % 20)} status=healthy"
        for i in range(repeats)
    ]
    return (
        "You are a systems diagnostics assistant. Analyze the following verified cluster ledger.\n"
        + "\n".join(rows)
        + "\n\nQuestion: Which node index is the last entry in the ledger?\nAnswer:"
    )


def _run_phase(args: argparse.Namespace) -> dict:
    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig

    # Ensure CowrieKVConnectorV1 is registered even if running from a source checkout
    try:
        from cowrie_vllm_adapters._plugin import register_vllm_plugin
        register_vllm_plugin()
    except ImportError:
        repo_root = Path(__file__).resolve().parents[2]
        sys.path.insert(0, str(repo_root / "python"))
        sys.path.insert(0, str(repo_root / "adapters"))
        from vllm._plugin import register_vllm_plugin  # type: ignore
        register_vllm_plugin()

    state_dir = Path(args.state_dir)
    state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    prompt = _make_long_prompt(args.prompt_rows)

    kv_cfg = KVTransferConfig(
        kv_connector="CowrieKVConnectorV1",
        kv_role="kv_both",
        kv_connector_extra_config={"cowrie_state_dir": str(state_dir)},
    )
    llm = LLM(
        model=args.model,
        dtype=args.dtype,
        max_model_len=args.max_model_len,
        gpu_memory_utilization=args.gpu_memory_utilization,
        enable_prefix_caching=True,
        enforce_eager=True,
        kv_transfer_config=kv_cfg,
    )
    sp = SamplingParams(temperature=0.0, max_tokens=args.max_tokens)
    llm.generate(["warmup"], sp, use_tqdm=False)

    t0 = time.perf_counter()
    out = llm.generate([prompt], sp, use_tqdm=False)[0]
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    shards = sorted(state_dir.glob("*.state.cowrie"))

    # Flush async background StatePage saves before process exit
    if hasattr(llm.llm_engine, "engine_core"):
        time.sleep(0.25)

    return {
        "phase": args.phase,
        "model": args.model,
        "prompt_tokens": len(out.prompt_token_ids),
        "generated_text": out.outputs[0].text,
        "generated_token_ids": list(out.outputs[0].token_ids),
        "latency_ms": round(elapsed_ms, 2),
        "state_shards": [s.name for s in shards],
        "state_bytes": sum(s.stat().st_size for s in shards),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Two-process vLLM V1 CowrieKVConnectorV1 restart demo")
    parser.add_argument("--model", required=True, help="HF repo ID or local model directory")
    parser.add_argument("--state-dir", default=str(Path(tempfile.gettempdir()) / "cowrie_vllm_demo"))
    parser.add_argument("--dtype", default="auto")
    parser.add_argument("--max-model-len", type=int, default=8192)
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.45)
    parser.add_argument("--prompt-rows", type=int, default=160)
    parser.add_argument("--max-tokens", type=int, default=16)
    parser.add_argument("--phase", choices=["both", "store", "restore"], default="both")
    args = parser.parse_args()

    if args.phase != "both":
        print(json.dumps(_run_phase(args)))
        return 0

    state_dir = Path(args.state_dir)
    if state_dir.exists():
        for old in state_dir.glob("*.state.cowrie"):
            old.unlink()

    base_cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--model", args.model,
        "--state-dir", str(state_dir),
        "--dtype", args.dtype,
        "--max-model-len", str(args.max_model_len),
        "--gpu-memory-utilization", str(args.gpu_memory_utilization),
        "--prompt-rows", str(args.prompt_rows),
        "--max-tokens", str(args.max_tokens),
    ]

    p_store = subprocess.run([*base_cmd, "--phase", "store"], capture_output=True, text=True, check=True)
    store_res = json.loads(p_store.stdout.strip().splitlines()[-1])

    p_restore = subprocess.run([*base_cmd, "--phase", "restore"], capture_output=True, text=True, check=True)
    restore_res = json.loads(p_restore.stdout.strip().splitlines()[-1])

    parity = store_res["generated_token_ids"] == restore_res["generated_token_ids"]
    speedup = round(store_res["latency_ms"] / max(restore_res["latency_ms"], 0.01), 2)
    summary = {
        "connector": "CowrieKVConnectorV1",
        "store": store_res,
        "restore": restore_res,
        "greedy_token_parity": parity,
        "restore_speedup_x": speedup,
    }
    print(json.dumps(summary, indent=2))
    return 0 if parity else 1


if __name__ == "__main__":
    raise SystemExit(main())
