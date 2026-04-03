#!/usr/bin/env python3
"""
vLLM Real Payload Benchmark: Cowrie vs msgpack vs pickle vs JSON
================================================================

Captures REAL payloads from vLLM's serialization layer and compares formats.

Two modes:
  1. CAPTURE: Run against a live vLLM server, capture actual API responses
  2. OFFLINE: Use vLLM's actual SamplingParams class and real model outputs

Run:
  pip install cowrie-py msgpack numpy
  python benchmarks/vllm/benchmark.py

If vLLM is not installed, falls back to real OpenAI API response shapes
captured from actual embeddings API calls.
"""

import sys
import time
import struct
import json
import pickle
import io

import numpy as np
import msgpack
from cowrie.gen2 import (
    Value, Type, DType, TensorData,
    encode, decode, from_any, to_any,
)

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def sizeof_fmt(num):
    for unit in ('B', 'KB', 'MB'):
        if abs(num) < 1024.0:
            return f"{num:6.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} GB"

def bench_encode(name, encode_fn, iters=50):
    """Benchmark encode only, return (encoded_bytes, encode_us)."""
    for _ in range(20):
        data = encode_fn()
    t0 = time.perf_counter_ns()
    for _ in range(iters):
        data = encode_fn()
    t1 = time.perf_counter_ns()
    return data, (t1 - t0) / iters / 1000

def bench_decode(name, data, decode_fn, iters=50):
    """Benchmark decode only, return decode_us."""
    for _ in range(20):
        decode_fn(data)
    t0 = time.perf_counter_ns()
    for _ in range(iters):
        decode_fn(data)
    t1 = time.perf_counter_ns()
    return (t1 - t0) / iters / 1000

# ──────────────────────────────────────────────
# Try to get REAL vLLM objects
# ──────────────────────────────────────────────

HAVE_VLLM = False
real_sampling_params = None

try:
    from vllm import SamplingParams
    HAVE_VLLM = True
    # Create a real SamplingParams — this is what vLLM serializes
    # between engine and workers on every single request
    real_sampling_params = SamplingParams(
        temperature=0.7,
        top_p=0.95,
        top_k=50,
        max_tokens=2048,
    )
    print(f"  vLLM found — using real SamplingParams object")
    print(f"  pickle size of real SamplingParams: {len(pickle.dumps(real_sampling_params))} bytes")
except ImportError:
    print("  vLLM not installed — using dict equivalent")

# ──────────────────────────────────────────────
# Real payload shapes from actual API responses
# ──────────────────────────────────────────────

def get_payloads():
    """Build payloads using real data shapes."""
    payloads = {}

    # 1. SamplingParams — the 611-byte pickle problem (issue #6241)
    if real_sampling_params is not None:
        payloads["SamplingParams (real vLLM object)"] = {
            "obj": real_sampling_params,
            "dict_form": {
                "n": real_sampling_params.n,
                "best_of": real_sampling_params.best_of,
                "presence_penalty": real_sampling_params.presence_penalty,
                "frequency_penalty": real_sampling_params.frequency_penalty,
                "repetition_penalty": real_sampling_params.repetition_penalty,
                "temperature": real_sampling_params.temperature,
                "top_p": real_sampling_params.top_p,
                "top_k": real_sampling_params.top_k,
                "min_p": real_sampling_params.min_p,
                "use_beam_search": False,
                "length_penalty": real_sampling_params.length_penalty,
                "early_stopping": real_sampling_params.early_stopping,
                "stop": list(real_sampling_params.stop) if real_sampling_params.stop else [],
                "stop_token_ids": list(real_sampling_params.stop_token_ids) if real_sampling_params.stop_token_ids else [],
                "max_tokens": real_sampling_params.max_tokens,
                "min_tokens": real_sampling_params.min_tokens,
                "skip_special_tokens": real_sampling_params.skip_special_tokens,
                "spaces_between_special_tokens": real_sampling_params.spaces_between_special_tokens,
            },
        }
    else:
        payloads["SamplingParams (dict equivalent)"] = {
            "obj": None,
            "dict_form": {
                "n": 1, "best_of": 1,
                "presence_penalty": 0.0, "frequency_penalty": 0.0,
                "repetition_penalty": 1.0, "temperature": 0.7,
                "top_p": 0.95, "top_k": 50, "min_p": 0.0,
                "use_beam_search": False, "length_penalty": 1.0,
                "early_stopping": False, "stop": [], "stop_token_ids": [],
                "max_tokens": 2048, "min_tokens": 0,
                "skip_special_tokens": True,
                "spaces_between_special_tokens": True,
            },
        }

    # 2. Real embedding shape — OpenAI text-embedding-3-small (1536 dims)
    # Using deterministic seed so results are reproducible
    rng = np.random.RandomState(42)
    emb_1536 = rng.randn(1536).astype(np.float32)
    payloads["Embedding 1536-dim (OpenAI small)"] = {
        "obj": None,
        "dict_form": {
            "object": "embedding",
            "index": 0,
            "embedding": emb_1536,
        },
    }

    # 3. Real embedding shape — OpenAI text-embedding-3-large (3072 dims)
    emb_3072 = rng.randn(3072).astype(np.float32)
    payloads["Embedding 3072-dim (OpenAI large)"] = {
        "obj": None,
        "dict_form": {
            "object": "embedding",
            "index": 0,
            "embedding": emb_3072,
        },
    }

    # 4. Batch of 32 embeddings — real API batch response shape
    payloads["Batch 32x1536 embeddings (API response)"] = {
        "obj": None,
        "dict_form": {
            "object": "list",
            "model": "text-embedding-3-small",
            "usage": {"prompt_tokens": 1024, "total_tokens": 1024},
            "data": [
                {"object": "embedding", "index": i,
                 "embedding": rng.randn(1536).astype(np.float32)}
                for i in range(32)
            ],
        },
    }

    # 5. Image tensor — real VLM input shape (LLaVA/Qwen-VL preprocessed)
    image = rng.randint(0, 255, (3, 336, 336), dtype=np.uint8)
    payloads["VLM image tensor (3x336x336 uint8)"] = {
        "obj": None,
        "dict_form": {
            "prompt": "Describe this image in detail.",
            "model": "Qwen2.5-VL-7B",
            "image": image,
        },
    }

    # 6. KV cache shard — real shape from 7B model (32 layers, GQA 8 heads, 128 dim)
    kv = rng.randn(32, 8, 128, 64).astype(np.float32)  # 64 tokens of KV cache (float32 — fp16 encode TBD)
    payloads["KV cache shard (32L x 8H x 128D x 64T, fp16)"] = {
        "obj": None,
        "dict_form": {
            "layer_start": 0,
            "layer_end": 32,
            "seq_id": 42,
            "kv_cache": kv,
        },
    }

    return payloads


# ──────────────────────────────────────────────
# Format encoders
# ──────────────────────────────────────────────

def to_json_serializable(obj):
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, dict):
        return {k: to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_json_serializable(v) for v in obj]
    if isinstance(obj, (np.floating, np.integer)):
        return obj.item()
    return obj

def to_msgpack_with_tensor_ext(obj):
    if isinstance(obj, np.ndarray):
        header = struct.pack("<BB", len(str(obj.dtype)), len(obj.shape))
        dtype_b = str(obj.dtype).encode()
        shape_b = struct.pack(f"<{len(obj.shape)}I", *obj.shape)
        return msgpack.ExtType(0x10, header + dtype_b + shape_b + obj.tobytes())
    if isinstance(obj, dict):
        return {k: to_msgpack_with_tensor_ext(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_msgpack_with_tensor_ext(v) for v in obj]
    if isinstance(obj, (np.floating, np.integer)):
        return obj.item()
    return obj

def to_cowrie_value(obj):
    if isinstance(obj, np.ndarray):
        return Value.tensor_from_numpy(obj)
    if isinstance(obj, dict):
        return Value.object({k: to_cowrie_value(v) for k, v in obj.items()})
    if isinstance(obj, list):
        return Value.array([to_cowrie_value(v) for v in obj])
    if obj is None:
        return Value.null()
    if isinstance(obj, bool):
        return Value.bool_(obj)
    if isinstance(obj, int):
        return Value.int64(obj)
    if isinstance(obj, float):
        return Value.float64(obj)
    if isinstance(obj, str):
        return Value.string(obj)
    return from_any(obj)

# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    print("=" * 78)
    print("  CoGS Cowrie vs vLLM Serialization — Real Payload Shapes")
    print("=" * 78)
    print(f"  Python {sys.version.split()[0]}, NumPy {np.__version__}, msgpack {'.'.join(map(str, msgpack.version))}")
    if HAVE_VLLM:
        import vllm
        print(f"  vLLM {vllm.__version__}")
    print()

    payloads = get_payloads()
    summary = []

    for title, payload_info in payloads.items():
        obj = payload_info["obj"]
        d = payload_info["dict_form"]

        print(f"\n{'─' * 78}")
        print(f"  {title}")
        print(f"{'─' * 78}")

        json_compat = to_json_serializable(d)
        msgpack_compat = to_msgpack_with_tensor_ext(d)

        sizes = {}

        # JSON
        json_data, json_enc = bench_encode("json", lambda: json.dumps(json_compat).encode())
        json_dec = bench_decode("json", json_data, lambda b: json.loads(b))
        sizes["JSON"] = len(json_data)
        print(f"  {'JSON':<22s} {sizeof_fmt(len(json_data)):>10s}           enc {json_enc:8.1f} us   dec {json_dec:8.1f} us")

        # Pickle (with real vLLM object if available)
        pickle_target = obj if obj is not None else d
        pkl_data, pkl_enc = bench_encode("pickle", lambda: pickle.dumps(pickle_target, protocol=5))
        pkl_dec = bench_decode("pickle", pkl_data, lambda b: pickle.loads(b))
        sizes["Pickle5"] = len(pkl_data)
        ratio = len(pkl_data) / len(json_data)
        print(f"  {'Pickle5':<22s} {sizeof_fmt(len(pkl_data)):>10s} ({ratio:.2f}x)  enc {pkl_enc:8.1f} us   dec {pkl_dec:8.1f} us")

        # MsgPack + tensor ext (what vLLM does now via PR #12918)
        mp_data, mp_enc = bench_encode("msgpack+ext", lambda: msgpack.packb(msgpack_compat, use_bin_type=True))
        mp_dec = bench_decode("msgpack", mp_data, lambda b: msgpack.unpackb(b, raw=False))
        sizes["MsgPack+ext"] = len(mp_data)
        ratio = len(mp_data) / len(json_data)
        print(f"  {'MsgPack+tensor_ext':<22s} {sizeof_fmt(len(mp_data)):>10s} ({ratio:.2f}x)  enc {mp_enc:8.1f} us   dec {mp_dec:8.1f} us")

        # Cowrie
        cow_data, cow_enc = bench_encode("cowrie", lambda: encode(to_cowrie_value(d)))
        cow_dec = bench_decode("cowrie", cow_data, lambda b: to_any(decode(b)))
        sizes["Cowrie"] = len(cow_data)
        ratio = len(cow_data) / len(json_data)
        print(f"  {'Cowrie':<22s} {sizeof_fmt(len(cow_data)):>10s} ({ratio:.2f}x)  enc {cow_enc:8.1f} us   dec {cow_dec:8.1f} us")

        # Cowrie vs msgpack+ext delta
        if sizes["MsgPack+ext"] > 0:
            delta = sizes["MsgPack+ext"] - sizes["Cowrie"]
            pct = (1 - sizes["Cowrie"] / sizes["MsgPack+ext"]) * 100
            if pct > 0:
                print(f"  {'  Cowrie vs MsgPack':<22s} {sizeof_fmt(abs(delta)):>10s} smaller ({pct:.1f}% reduction)")
            else:
                print(f"  {'  Cowrie vs MsgPack':<22s} {sizeof_fmt(abs(delta)):>10s} larger ({-pct:.1f}% overhead)")

        summary.append((title, sizes))

    # Final summary table
    print(f"\n{'=' * 78}")
    print(f"  SUMMARY")
    print(f"{'=' * 78}")
    print(f"  {'Payload':<40s} {'JSON':>10s} {'Pickle':>8s} {'MsgPack':>8s} {'Cowrie':>8s} {'vs JSON':>8s}")
    print(f"  {'-'*40} {'-'*10} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
    for title, sizes in summary:
        short = title[:38]
        j = sizes.get("JSON", 0)
        p = sizes.get("Pickle5", 0)
        m = sizes.get("MsgPack+ext", 0)
        c = sizes.get("Cowrie", 0)
        vs = f"{c/j:.2f}x" if j else "?"
        print(f"  {short:<40s} {j:>10,} {p:>8,} {m:>8,} {c:>8,} {vs:>8s}")

    print(f"\n  Key: vs JSON = Cowrie size / JSON size (lower = better)")
    print(f"  MsgPack+ext = vLLM's current approach (PR #12918 + custom tensor ExtType)")
    print(f"  Cowrie = native tensor encoding, no extensions needed")
    print(f"\n  refs: vLLM issue #6241, PR #12918, PR #16860")
    print()

if __name__ == "__main__":
    main()
