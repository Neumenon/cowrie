#!/usr/bin/env python3
"""
Benchmark: Cowrie vs JSON vs MessagePack vs CBOR

Compares size and speed across formats for different data types.

Run: python benchmark_formats.py
"""

import json
import gzip
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import cowrie
from cowrie.graph import StreamWriter, NodeEvent, EdgeEvent, Op

# Optional imports
try:
    import zstandard as zstd
    HAS_ZSTD = True
except ImportError:
    HAS_ZSTD = False

try:
    import msgpack
    HAS_MSGPACK = True
except ImportError:
    HAS_MSGPACK = False

try:
    import cbor2
    HAS_CBOR = True
except ImportError:
    HAS_CBOR = False


def generate_test_data(num_records: int = 1000) -> dict:
    """Generate realistic test data."""
    import random
    random.seed(42)

    users = []
    for i in range(num_records):
        users.append({
            "id": i,
            "name": f"User_{i}",
            "email": f"user{i}@example.com",
            "age": random.randint(18, 80),
            "active": random.random() > 0.3,
            "score": round(random.random() * 100, 2),
            "tags": [f"tag_{j}" for j in range(random.randint(1, 5))],
            "metadata": {
                "created": 1700000000 + i * 86400,
                "updated": 1700000000 + i * 86400 + random.randint(0, 86400),
                "version": random.randint(1, 10),
            },
        })

    return {
        "version": "1.0.0",
        "total": num_records,
        "users": users,
    }


def generate_graph_data(num_nodes: int = 500, edges_per_node: int = 3) -> tuple:
    """Generate graph event data for GraphCowrie-Stream testing."""
    import random
    random.seed(42)

    nodes = []
    edges = []

    for i in range(num_nodes):
        nodes.append({
            "id": f"node_{i}",
            "labels": ["Person"] if random.random() > 0.3 else ["Person", "Employee"],
            "props": {
                "name": f"Person_{i}",
                "age": random.randint(20, 60),
            },
        })

        # Add random edges
        for _ in range(random.randint(1, edges_per_node)):
            target = random.randint(0, num_nodes - 1)
            if target != i:
                edges.append({
                    "from": f"node_{i}",
                    "to": f"node_{target}",
                    "label": random.choice(["KNOWS", "WORKS_WITH", "FOLLOWS"]),
                    "weight": round(random.random(), 2),
                })

    return nodes, edges


def benchmark_json(data: dict) -> dict:
    """Benchmark JSON formats."""
    results = {}

    # JSON raw
    start = time.perf_counter()
    json_bytes = json.dumps(data).encode('utf-8')
    encode_time = time.perf_counter() - start

    start = time.perf_counter()
    _ = json.loads(json_bytes)
    decode_time = time.perf_counter() - start

    results['JSON'] = {
        'size': len(json_bytes),
        'encode_ms': encode_time * 1000,
        'decode_ms': decode_time * 1000,
    }

    # JSON + gzip
    start = time.perf_counter()
    gzip_bytes = gzip.compress(json_bytes, compresslevel=9)
    encode_time = time.perf_counter() - start

    start = time.perf_counter()
    _ = json.loads(gzip.decompress(gzip_bytes))
    decode_time = time.perf_counter() - start

    results['JSON+gzip'] = {
        'size': len(gzip_bytes),
        'encode_ms': encode_time * 1000,
        'decode_ms': decode_time * 1000,
    }

    # JSON + zstd
    if HAS_ZSTD:
        cctx = zstd.ZstdCompressor(level=19)
        dctx = zstd.ZstdDecompressor()

        start = time.perf_counter()
        zstd_bytes = cctx.compress(json_bytes)
        encode_time = time.perf_counter() - start

        start = time.perf_counter()
        _ = json.loads(dctx.decompress(zstd_bytes))
        decode_time = time.perf_counter() - start

        results['JSON+zstd'] = {
            'size': len(zstd_bytes),
            'encode_ms': encode_time * 1000,
            'decode_ms': decode_time * 1000,
        }

    return results


def benchmark_cowrie(data: dict) -> dict:
    """Benchmark Cowrie formats."""
    results = {}

    # Cowrie raw
    start = time.perf_counter()
    cowrie_bytes = cowrie.encode(data)
    encode_time = time.perf_counter() - start

    start = time.perf_counter()
    _ = cowrie.to_python(cowrie.decode(cowrie_bytes))
    decode_time = time.perf_counter() - start

    results['Cowrie'] = {
        'size': len(cowrie_bytes),
        'encode_ms': encode_time * 1000,
        'decode_ms': decode_time * 1000,
    }

    # Cowrie + zstd
    if HAS_ZSTD:
        start = time.perf_counter()
        cowrie_zstd = cowrie.encode_compressed(data)
        encode_time = time.perf_counter() - start

        start = time.perf_counter()
        _ = cowrie.to_python(cowrie.decode_compressed(cowrie_zstd))
        decode_time = time.perf_counter() - start

        results['Cowrie+zstd'] = {
            'size': len(cowrie_zstd),
            'encode_ms': encode_time * 1000,
            'decode_ms': decode_time * 1000,
        }

    return results


def benchmark_msgpack(data: dict) -> dict:
    """Benchmark MessagePack formats."""
    if not HAS_MSGPACK:
        return {}

    results = {}

    # msgpack raw
    start = time.perf_counter()
    mp_bytes = msgpack.packb(data)
    encode_time = time.perf_counter() - start

    start = time.perf_counter()
    _ = msgpack.unpackb(mp_bytes)
    decode_time = time.perf_counter() - start

    results['MessagePack'] = {
        'size': len(mp_bytes),
        'encode_ms': encode_time * 1000,
        'decode_ms': decode_time * 1000,
    }

    # msgpack + zstd
    if HAS_ZSTD:
        cctx = zstd.ZstdCompressor(level=19)
        dctx = zstd.ZstdDecompressor()

        start = time.perf_counter()
        mp_zstd = cctx.compress(mp_bytes)
        encode_time = time.perf_counter() - start

        start = time.perf_counter()
        _ = msgpack.unpackb(dctx.decompress(mp_zstd))
        decode_time = time.perf_counter() - start

        results['MessagePack+zstd'] = {
            'size': len(mp_zstd),
            'encode_ms': encode_time * 1000,
            'decode_ms': decode_time * 1000,
        }

    return results


def benchmark_cbor(data: dict) -> dict:
    """Benchmark CBOR formats."""
    if not HAS_CBOR:
        return {}

    results = {}

    # CBOR raw
    start = time.perf_counter()
    cbor_bytes = cbor2.dumps(data)
    encode_time = time.perf_counter() - start

    start = time.perf_counter()
    _ = cbor2.loads(cbor_bytes)
    decode_time = time.perf_counter() - start

    results['CBOR'] = {
        'size': len(cbor_bytes),
        'encode_ms': encode_time * 1000,
        'decode_ms': decode_time * 1000,
    }

    # CBOR + zstd
    if HAS_ZSTD:
        cctx = zstd.ZstdCompressor(level=19)
        dctx = zstd.ZstdDecompressor()

        start = time.perf_counter()
        cbor_zstd = cctx.compress(cbor_bytes)
        encode_time = time.perf_counter() - start

        start = time.perf_counter()
        _ = cbor2.loads(dctx.decompress(cbor_zstd))
        decode_time = time.perf_counter() - start

        results['CBOR+zstd'] = {
            'size': len(cbor_zstd),
            'encode_ms': encode_time * 1000,
            'decode_ms': decode_time * 1000,
        }

    return results


def benchmark_graph_stream(nodes: list, edges: list) -> dict:
    """Benchmark GraphCowrie-Stream vs JSON for graph data."""
    results = {}

    # Build JSON representation
    json_data = {"nodes": nodes, "edges": edges}

    # JSON
    json_bytes = json.dumps(json_data).encode('utf-8')
    results['JSON (graph)'] = {'size': len(json_bytes)}

    if HAS_ZSTD:
        cctx = zstd.ZstdCompressor(level=19)
        results['JSON+zstd (graph)'] = {'size': len(cctx.compress(json_bytes))}

    # GraphCowrie-Stream
    writer = StreamWriter()
    for node in nodes:
        writer.write_node(NodeEvent(
            op=Op.UPSERT,
            id=node['id'],
            labels=node['labels'],
            props=node['props'],
        ))
    for edge in edges:
        writer.write_edge(EdgeEvent(
            op=Op.UPSERT,
            label=edge['label'],
            from_id=edge['from'],
            to_id=edge['to'],
            props={'weight': edge['weight']},
        ))

    stream_bytes = writer.getvalue()
    results['GraphCowrie-Stream'] = {'size': len(stream_bytes)}

    if HAS_ZSTD:
        stream_zstd = writer.getvalue_compressed()
        results['GraphCowrie-Stream+zstd'] = {'size': len(stream_zstd)}

    return results


def print_results(title: str, results: dict, baseline: str = 'JSON'):
    """Print benchmark results as a table."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")

    if not results:
        print("  No results (missing dependencies)")
        return

    baseline_size = results.get(baseline, {}).get('size', 0)

    # Sort by size
    sorted_results = sorted(results.items(), key=lambda x: x[1].get('size', 0))

    print(f"  {'Format':<22} {'Size':>10} {'Ratio':>8} {'Encode':>10} {'Decode':>10}")
    print(f"  {'-' * 22} {'-' * 10} {'-' * 8} {'-' * 10} {'-' * 10}")

    for name, data in sorted_results:
        size = data.get('size', 0)
        ratio = f"{size / baseline_size:.2f}x" if baseline_size else "N/A"
        encode_ms = data.get('encode_ms', 0)
        decode_ms = data.get('decode_ms', 0)

        encode_str = f"{encode_ms:.2f}ms" if encode_ms else "-"
        decode_str = f"{decode_ms:.2f}ms" if decode_ms else "-"

        print(f"  {name:<22} {size:>10,} {ratio:>8} {encode_str:>10} {decode_str:>10}")


def run_benchmarks():
    """Run all benchmarks."""
    print("=" * 60)
    print("  Cowrie Format Benchmark")
    print("=" * 60)
    print()
    print("Available formats:")
    print(f"  - JSON: Yes")
    print(f"  - Cowrie: Yes")
    print(f"  - zstd compression: {'Yes' if HAS_ZSTD else 'No (pip install zstandard)'}")
    print(f"  - MessagePack: {'Yes' if HAS_MSGPACK else 'No (pip install msgpack)'}")
    print(f"  - CBOR: {'Yes' if HAS_CBOR else 'No (pip install cbor2)'}")

    # Generate test data
    print("\nGenerating test data (1000 records)...")
    data = generate_test_data(1000)

    # Run benchmarks
    all_results = {}
    all_results.update(benchmark_json(data))
    all_results.update(benchmark_cowrie(data))
    all_results.update(benchmark_msgpack(data))
    all_results.update(benchmark_cbor(data))

    print_results("Document Data (1000 records)", all_results, 'JSON')

    # Graph benchmark
    print("\nGenerating graph data (500 nodes)...")
    nodes, edges = generate_graph_data(500, 3)

    graph_results = benchmark_graph_stream(nodes, edges)
    print_results("Graph Events (500 nodes, ~1500 edges)", graph_results, 'JSON (graph)')

    # Summary
    print("\n" + "=" * 60)
    print("  Summary")
    print("=" * 60)

    if 'Cowrie+zstd' in all_results and 'JSON+zstd' in all_results:
        cowrie_size = all_results['Cowrie+zstd']['size']
        json_size = all_results['JSON+zstd']['size']
        improvement = json_size / cowrie_size
        print(f"\n  Cowrie+zstd vs JSON+zstd: {improvement:.2f}x smaller")

    if 'GraphCowrie-Stream+zstd' in graph_results and 'JSON+zstd (graph)' in graph_results:
        stream_size = graph_results['GraphCowrie-Stream+zstd']['size']
        json_size = graph_results['JSON+zstd (graph)']['size']
        improvement = json_size / stream_size
        print(f"  GraphCowrie-Stream+zstd vs JSON+zstd: {improvement:.2f}x smaller")

    print()
    print("  GraphCowrie-Stream compares to:")
    print("    - Neo4j binary import format")
    print("    - RDF N-Triples/N-Quads streaming")
    print("    - Apache Arrow Flight for graphs")
    print("    - Protocol Buffers (graph events)")
    print()


if __name__ == "__main__":
    run_benchmarks()
