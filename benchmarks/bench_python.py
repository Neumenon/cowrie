#!/usr/bin/env python3
"""SJSON Python Benchmark Suite

Run: python benchmarks/bench_python.py
"""

import json
import time
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sjson import gen1
from sjson.gen2 import (
    encode, decode, Value, NodeData, EdgeData,
    TensorData, DType
)

# Test payloads
SMALL_OBJECT = {
    "name": "Alice",
    "age": 30,
    "score": 3.14159,
}

MEDIUM_OBJECT = {chr(ord('a') + i): i * 100 for i in range(20)}
MEDIUM_OBJECT["nested"] = {"x": 1.0, "y": 2.0, "z": 3.0}

LARGE_ARRAY = [
    {"id": i, "name": "item", "value": i * 0.1}
    for i in range(1000)
]

FLOAT_ARRAY = [i * 0.001 for i in range(10000)]


def benchmark(name: str, func, iterations: int = 1000):
    """Run a benchmark and return results."""
    # Warmup
    for _ in range(10):
        func()

    # Timed run
    start = time.perf_counter()
    for _ in range(iterations):
        func()
    elapsed = time.perf_counter() - start

    ops_per_sec = iterations / elapsed
    us_per_op = (elapsed / iterations) * 1_000_000

    return {
        "name": name,
        "iterations": iterations,
        "total_ms": elapsed * 1000,
        "us_per_op": us_per_op,
        "ops_per_sec": ops_per_sec,
    }


def run_benchmarks():
    """Run all benchmarks and print results."""
    results = []

    print("=" * 60)
    print("SJSON Python Benchmarks")
    print("=" * 60)

    # Gen1 Benchmarks
    print("\n--- Gen1 Encode ---")

    results.append(benchmark("Gen1 Encode Small", lambda: gen1.encode(SMALL_OBJECT)))
    results.append(benchmark("Gen1 Encode Medium", lambda: gen1.encode(MEDIUM_OBJECT)))
    results.append(benchmark("Gen1 Encode Large", lambda: gen1.encode(LARGE_ARRAY), iterations=100))
    results.append(benchmark("Gen1 Encode FloatArray", lambda: gen1.encode(FLOAT_ARRAY), iterations=100))

    print("\n--- Gen1 Decode ---")

    small_gen1 = gen1.encode(SMALL_OBJECT)
    results.append(benchmark("Gen1 Decode Small", lambda: gen1.decode(small_gen1)))

    large_gen1 = gen1.encode(LARGE_ARRAY)
    results.append(benchmark("Gen1 Decode Large", lambda: gen1.decode(large_gen1), iterations=100))

    # Gen2 Benchmarks
    print("\n--- Gen2 Encode ---")

    small_val = Value.object({
        "name": Value.string("Alice"),
        "age": Value.int64(30),
        "score": Value.float64(3.14159),
    })
    results.append(benchmark("Gen2 Encode Small", lambda: encode(small_val)))

    large_val = Value.array([
        Value.object({
            "id": Value.int64(i),
            "name": Value.string("item"),
            "value": Value.float64(i * 0.1),
        })
        for i in range(1000)
    ])
    results.append(benchmark("Gen2 Encode Large", lambda: encode(large_val), iterations=100))

    print("\n--- Gen2 Decode ---")

    small_gen2 = encode(small_val)
    results.append(benchmark("Gen2 Decode Small", lambda: decode(small_gen2)))

    large_gen2 = encode(large_val)
    results.append(benchmark("Gen2 Decode Large", lambda: decode(large_gen2), iterations=100))

    # Gen2 Graph Benchmarks
    print("\n--- Gen2 Graph Types ---")

    nodes = [
        NodeData(
            id=f"node_{i}",
            labels=["Person"],
            props={"name": Value.string("node"), "x": Value.float64(i * 0.1)}
        )
        for i in range(100)
    ]
    edges = [
        EdgeData(
            from_id=f"node_{i}",
            to_id=f"node_{(i+1) % 100}",
            edge_type="KNOWS",
            props={"weight": Value.float64(0.5)}
        )
        for i in range(200)
    ]
    shard = Value.graph_shard(nodes, edges, {"version": Value.int64(1)})
    results.append(benchmark("Gen2 Encode GraphShard", lambda: encode(shard), iterations=100))

    shard_data = encode(shard)
    results.append(benchmark("Gen2 Decode GraphShard", lambda: decode(shard_data), iterations=100))

    # JSON Comparison
    print("\n--- JSON Comparison ---")

    results.append(benchmark("JSON Encode Small", lambda: json.dumps(SMALL_OBJECT)))
    results.append(benchmark("JSON Encode Large", lambda: json.dumps(LARGE_ARRAY), iterations=100))

    small_json = json.dumps(SMALL_OBJECT)
    results.append(benchmark("JSON Decode Small", lambda: json.loads(small_json)))

    large_json = json.dumps(LARGE_ARRAY)
    results.append(benchmark("JSON Decode Large", lambda: json.loads(large_json), iterations=100))

    # Print results table
    print("\n" + "=" * 60)
    print("Results Summary")
    print("=" * 60)
    print(f"{'Benchmark':<35} {'us/op':>10} {'ops/sec':>12}")
    print("-" * 60)

    for r in results:
        print(f"{r['name']:<35} {r['us_per_op']:>10.1f} {r['ops_per_sec']:>12.0f}")

    # Size comparison
    print("\n" + "=" * 60)
    print("Payload Size Comparison")
    print("=" * 60)

    small_json_data = json.dumps(SMALL_OBJECT).encode()
    small_gen1_data = gen1.encode(SMALL_OBJECT)
    small_gen2_data = encode(small_val)

    print(f"\nSmall Object:")
    print(f"  JSON:  {len(small_json_data):>6} bytes")
    print(f"  Gen1:  {len(small_gen1_data):>6} bytes ({len(small_gen1_data)/len(small_json_data)*100:.1f}% of JSON)")
    print(f"  Gen2:  {len(small_gen2_data):>6} bytes ({len(small_gen2_data)/len(small_json_data)*100:.1f}% of JSON)")

    large_json_data = json.dumps(LARGE_ARRAY).encode()
    large_gen1_data = gen1.encode(LARGE_ARRAY)
    large_gen2_data = encode(large_val)

    print(f"\nLarge Array (1000 objects):")
    print(f"  JSON:  {len(large_json_data):>6} bytes")
    print(f"  Gen1:  {len(large_gen1_data):>6} bytes ({len(large_gen1_data)/len(large_json_data)*100:.1f}% of JSON)")
    print(f"  Gen2:  {len(large_gen2_data):>6} bytes ({len(large_gen2_data)/len(large_json_data)*100:.1f}% of JSON)")

    float_json_data = json.dumps(FLOAT_ARRAY).encode()
    float_gen1_data = gen1.encode(FLOAT_ARRAY)

    print(f"\nFloat Array (10000 floats):")
    print(f"  JSON:  {len(float_json_data):>6} bytes")
    print(f"  Gen1:  {len(float_gen1_data):>6} bytes ({len(float_gen1_data)/len(float_json_data)*100:.1f}% of JSON)")

    print(f"\nGraph Shard (100 nodes, 200 edges):")
    print(f"  Gen2:  {len(shard_data):>6} bytes")


if __name__ == "__main__":
    run_benchmarks()
