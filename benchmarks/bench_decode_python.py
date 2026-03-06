#!/usr/bin/env python3
"""Cowrie Gen1 vs Gen2 Decode Benchmark (Python)"""
import time
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))
from cowrie import gen1
from cowrie.gen2 import decode as gen2_decode

FIXTURE_DIR = Path(__file__).parent / "fixtures"

FIXTURES = {
    "small":  (35, 43),
    "medium": (143, 172),
    "large":  (33939, 22958),
    "floats": (80003, 40013),
}

def load(name):
    g1 = (FIXTURE_DIR / f"{name}.gen1").read_bytes()
    g2 = (FIXTURE_DIR / f"{name}.gen2").read_bytes()
    return g1, g2

def bench(label, func, data, iterations):
    # warmup
    for _ in range(min(iterations, 10)):
        func(data)
    start = time.perf_counter()
    for _ in range(iterations):
        func(data)
    elapsed = time.perf_counter() - start
    ops = iterations / elapsed
    us = (elapsed / iterations) * 1e6
    mb_s = (len(data) * iterations / elapsed) / 1e6
    return ops, us, mb_s

def main():
    print("=" * 72)
    print("Cowrie Decode Benchmark — Python")
    print("=" * 72)
    print(f"{'Payload':<10} {'Gen':>4} {'Size':>8} {'ops/s':>10} {'us/op':>10} {'MB/s':>10}")
    print("-" * 72)

    results = {}
    for name in ["small", "medium", "large", "floats"]:
        g1_data, g2_data = load(name)
        iters = 10000 if len(g1_data) < 1000 else 500

        ops1, us1, mb1 = bench(f"{name}/gen1", gen1.decode, g1_data, iters)
        print(f"{name:<10} {'g1':>4} {len(g1_data):>7}B {ops1:>10.0f} {us1:>10.1f} {mb1:>10.1f}")

        ops2, us2, mb2 = bench(f"{name}/gen2", gen2_decode, g2_data, iters)
        print(f"{'':<10} {'g2':>4} {len(g2_data):>7}B {ops2:>10.0f} {us2:>10.1f} {mb2:>10.1f}")

        ratio = ops2 / ops1 if ops1 > 0 else 0
        print(f"{'':<10} {'':>4} {'':>8} {'ratio':>10} {ratio:>10.2f}x")
        print()
        results[name] = (ops1, ops2, ratio)

    print("=" * 72)
    print("Gen2/Gen1 decode speed ratios (>1 = Gen2 faster):")
    for name, (_, _, ratio) in results.items():
        bar = "#" * int(ratio * 20)
        print(f"  {name:<8}: {ratio:.2f}x  {bar}")

if __name__ == "__main__":
    main()
