#!/usr/bin/env python3
"""
Industry Comparison Benchmark

Compares Cowrie formats against industry standards:
- GraphCowrie-Stream vs graph serialization formats
- Cowrie-LD vs JSON-LD and RDF formats
"""

import json
import gzip
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import cowrie
from cowrie.graph import StreamWriter, NodeEvent, EdgeEvent, Op
from cowrie.ld import LDDocument, IRI, encode as ld_encode, encode_compressed as ld_encode_compressed

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


def generate_graph_data(num_nodes=1000, edges_per_node=5):
    """Generate realistic graph data."""
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
                "age": random.randint(20, 65),
                "email": f"person{i}@example.com",
                "score": round(random.random() * 100, 2),
            },
        })

        for _ in range(random.randint(1, edges_per_node)):
            target = random.randint(0, num_nodes - 1)
            if target != i:
                edges.append({
                    "from": f"node_{i}",
                    "to": f"node_{target}",
                    "label": random.choice(["KNOWS", "WORKS_WITH", "FOLLOWS", "MANAGES"]),
                    "props": {
                        "weight": round(random.random(), 3),
                        "since": 2010 + random.randint(0, 14),
                    },
                })

    return nodes, edges


def generate_ld_data(num_entities=500):
    """Generate JSON-LD style data."""
    import random
    random.seed(42)

    entities = []
    for i in range(num_entities):
        entity = {
            "@id": f"http://example.org/person/{i}",
            "@type": "http://schema.org/Person",
            "http://schema.org/name": f"Person {i}",
            "http://schema.org/email": f"person{i}@example.org",
            "http://schema.org/age": random.randint(18, 80),
            "http://schema.org/knows": [
                {"@id": f"http://example.org/person/{random.randint(0, num_entities-1)}"}
                for _ in range(random.randint(1, 5))
            ],
        }
        entities.append(entity)

    return {
        "@context": {
            "name": "http://schema.org/name",
            "email": "http://schema.org/email",
            "age": "http://schema.org/age",
            "knows": "http://schema.org/knows",
            "Person": "http://schema.org/Person",
        },
        "@graph": entities,
    }


def benchmark_graph_formats(nodes, edges):
    """Benchmark graph serialization formats."""
    results = {}

    # === JSON-based formats ===

    # 1. Plain JSON (like GraphSON)
    json_data = {"nodes": nodes, "edges": edges}
    start = time.perf_counter()
    json_bytes = json.dumps(json_data).encode('utf-8')
    json_time = time.perf_counter() - start
    results['JSON (GraphSON-style)'] = {
        'size': len(json_bytes),
        'encode_ms': json_time * 1000,
    }

    # 2. JSON + gzip
    start = time.perf_counter()
    json_gzip = gzip.compress(json_bytes, compresslevel=9)
    gzip_time = time.perf_counter() - start
    results['JSON+gzip'] = {
        'size': len(json_gzip),
        'encode_ms': gzip_time * 1000,
    }

    # 3. JSON + zstd
    if HAS_ZSTD:
        cctx = zstd.ZstdCompressor(level=19)
        start = time.perf_counter()
        json_zstd = cctx.compress(json_bytes)
        zstd_time = time.perf_counter() - start
        results['JSON+zstd'] = {
            'size': len(json_zstd),
            'encode_ms': zstd_time * 1000,
        }

    # 4. JSON Lines (streaming format)
    start = time.perf_counter()
    lines = []
    for node in nodes:
        lines.append(json.dumps({"type": "node", **node}))
    for edge in edges:
        lines.append(json.dumps({"type": "edge", **edge}))
    jsonl_bytes = '\n'.join(lines).encode('utf-8')
    jsonl_time = time.perf_counter() - start
    results['JSON Lines'] = {
        'size': len(jsonl_bytes),
        'encode_ms': jsonl_time * 1000,
    }

    if HAS_ZSTD:
        jsonl_zstd = cctx.compress(jsonl_bytes)
        results['JSON Lines+zstd'] = {
            'size': len(jsonl_zstd),
            'encode_ms': jsonl_time * 1000,
        }

    # === Binary formats ===

    # 5. MessagePack
    if HAS_MSGPACK:
        start = time.perf_counter()
        mp_bytes = msgpack.packb(json_data)
        mp_time = time.perf_counter() - start
        results['MessagePack'] = {
            'size': len(mp_bytes),
            'encode_ms': mp_time * 1000,
        }

        if HAS_ZSTD:
            mp_zstd = cctx.compress(mp_bytes)
            results['MessagePack+zstd'] = {
                'size': len(mp_zstd),
                'encode_ms': mp_time * 1000,
            }

    # 6. CBOR
    if HAS_CBOR:
        start = time.perf_counter()
        cbor_bytes = cbor2.dumps(json_data)
        cbor_time = time.perf_counter() - start
        results['CBOR'] = {
            'size': len(cbor_bytes),
            'encode_ms': cbor_time * 1000,
        }

        if HAS_ZSTD:
            cbor_zstd = cctx.compress(cbor_bytes)
            results['CBOR+zstd'] = {
                'size': len(cbor_zstd),
                'encode_ms': cbor_time * 1000,
            }

    # === GraphCowrie-Stream ===

    # 7. GraphCowrie-Stream (our format)
    start = time.perf_counter()
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
            props=edge['props'],
        ))
    stream_bytes = writer.getvalue()
    stream_time = time.perf_counter() - start
    results['GraphCowrie-Stream'] = {
        'size': len(stream_bytes),
        'encode_ms': stream_time * 1000,
    }

    if HAS_ZSTD:
        start = time.perf_counter()
        stream_zstd = writer.getvalue_compressed(shuffle=True)
        stream_zstd_time = time.perf_counter() - start
        results['GraphCowrie-Stream+zstd'] = {
            'size': len(stream_zstd),
            'encode_ms': stream_zstd_time * 1000,
        }

    # === RDF-style formats (simulated) ===

    # 8. N-Triples style (text RDF)
    start = time.perf_counter()
    triples = []
    for node in nodes:
        subj = f"<http://example.org/{node['id']}>"
        for label in node['labels']:
            triples.append(f'{subj} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/{label}> .')
        for key, val in node['props'].items():
            if isinstance(val, str):
                triples.append(f'{subj} <http://example.org/{key}> "{val}" .')
            else:
                triples.append(f'{subj} <http://example.org/{key}> "{val}"^^<http://www.w3.org/2001/XMLSchema#integer> .')
    for edge in edges:
        subj = f"<http://example.org/{edge['from']}>"
        obj = f"<http://example.org/{edge['to']}>"
        triples.append(f'{subj} <http://example.org/{edge["label"]}> {obj} .')
    nt_bytes = '\n'.join(triples).encode('utf-8')
    nt_time = time.perf_counter() - start
    results['N-Triples (RDF)'] = {
        'size': len(nt_bytes),
        'encode_ms': nt_time * 1000,
    }

    if HAS_ZSTD:
        nt_zstd = cctx.compress(nt_bytes)
        results['N-Triples+zstd'] = {
            'size': len(nt_zstd),
            'encode_ms': nt_time * 1000,
        }

    return results


def benchmark_ld_formats(ld_data):
    """Benchmark JSON-LD style formats."""
    results = {}

    # === JSON-LD formats ===

    # 1. JSON-LD (standard)
    start = time.perf_counter()
    jsonld_bytes = json.dumps(ld_data).encode('utf-8')
    jsonld_time = time.perf_counter() - start
    results['JSON-LD'] = {
        'size': len(jsonld_bytes),
        'encode_ms': jsonld_time * 1000,
    }

    # 2. JSON-LD + gzip
    start = time.perf_counter()
    jsonld_gzip = gzip.compress(jsonld_bytes, compresslevel=9)
    gzip_time = time.perf_counter() - start
    results['JSON-LD+gzip'] = {
        'size': len(jsonld_gzip),
        'encode_ms': gzip_time * 1000,
    }

    # 3. JSON-LD + zstd
    if HAS_ZSTD:
        cctx = zstd.ZstdCompressor(level=19)
        start = time.perf_counter()
        jsonld_zstd = cctx.compress(jsonld_bytes)
        zstd_time = time.perf_counter() - start
        results['JSON-LD+zstd'] = {
            'size': len(jsonld_zstd),
            'encode_ms': zstd_time * 1000,
        }

    # === Binary formats ===

    # 4. CBOR (CBOR-LD style)
    if HAS_CBOR:
        start = time.perf_counter()
        cbor_bytes = cbor2.dumps(ld_data)
        cbor_time = time.perf_counter() - start
        results['CBOR-LD'] = {
            'size': len(cbor_bytes),
            'encode_ms': cbor_time * 1000,
        }

        if HAS_ZSTD:
            cbor_zstd = cctx.compress(cbor_bytes)
            results['CBOR-LD+zstd'] = {
                'size': len(cbor_zstd),
                'encode_ms': cbor_time * 1000,
            }

    # === Cowrie-LD ===

    # 5. Build Cowrie-LD document
    start = time.perf_counter()
    doc = LDDocument()

    # Add context terms
    doc.add_term("name", IRI("http://schema.org/name"))
    doc.add_term("email", IRI("http://schema.org/email"))
    doc.add_term("age", IRI("http://schema.org/age"))
    doc.add_term("knows", IRI("http://schema.org/knows"))
    doc.add_term("Person", IRI("http://schema.org/Person"))
    doc.add_term("@type", IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))

    # Add IRIs for all entities
    for entity in ld_data["@graph"]:
        doc.add_iri(IRI(entity["@id"]))
        for knows in entity.get("http://schema.org/knows", []):
            doc.add_iri(IRI(knows["@id"]))

    # Build root value
    entities = []
    for entity in ld_data["@graph"]:
        knows_refs = []
        for knows in entity.get("http://schema.org/knows", []):
            knows_refs.append(doc.iri_value(IRI(knows["@id"])))

        entities.append(cowrie.Object(
            ("@id", doc.iri_value(IRI(entity["@id"]))),
            ("@type", doc.iri_value(IRI("http://schema.org/Person"))),
            ("name", cowrie.String(entity["http://schema.org/name"])),
            ("email", cowrie.String(entity["http://schema.org/email"])),
            ("age", cowrie.Int64(entity["http://schema.org/age"])),
            ("knows", cowrie.Array(*knows_refs)),
        ))

    doc.root = cowrie.Object(
        ("@graph", cowrie.Array(*entities)),
    )

    cowrield_bytes = ld_encode(doc)
    cowrield_time = time.perf_counter() - start
    results['Cowrie-LD'] = {
        'size': len(cowrield_bytes),
        'encode_ms': cowrield_time * 1000,
    }

    if HAS_ZSTD:
        start = time.perf_counter()
        cowrield_zstd = ld_encode_compressed(doc, shuffle=True)
        cowrield_zstd_time = time.perf_counter() - start
        results['Cowrie-LD+zstd'] = {
            'size': len(cowrield_zstd),
            'encode_ms': cowrield_zstd_time * 1000,
        }

    return results


def print_results(title, results, baseline_key):
    """Print benchmark results."""
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")

    baseline_size = results.get(baseline_key, {}).get('size', 1)

    # Sort by size
    sorted_results = sorted(results.items(), key=lambda x: x[1]['size'])

    print(f"  {'Format':<28} {'Size':>12} {'Ratio':>10} {'Encode':>12}")
    print(f"  {'-' * 28} {'-' * 12} {'-' * 10} {'-' * 12}")

    for name, data in sorted_results:
        size = data['size']
        ratio = size / baseline_size
        encode_ms = data.get('encode_ms', 0)

        ratio_str = f"{ratio:.2f}x"
        encode_str = f"{encode_ms:.1f}ms" if encode_ms else "-"

        # Highlight our formats
        marker = " <--" if name.startswith('GraphCowrie') or name.startswith('Cowrie-LD') else ""

        print(f"  {name:<28} {size:>12,} {ratio_str:>10} {encode_str:>12}{marker}")


def main():
    print("=" * 70)
    print("  Cowrie Industry Comparison Benchmark")
    print("=" * 70)
    print()
    print("Available formats:")
    print(f"  - JSON/JSON-LD: Yes")
    print(f"  - zstd compression: {'Yes' if HAS_ZSTD else 'No (pip install zstandard)'}")
    print(f"  - MessagePack: {'Yes' if HAS_MSGPACK else 'No (pip install msgpack)'}")
    print(f"  - CBOR: {'Yes' if HAS_CBOR else 'No (pip install cbor2)'}")

    # === Graph Benchmark ===
    print("\n" + "-" * 70)
    print("Generating graph data (1000 nodes, ~5000 edges)...")
    nodes, edges = generate_graph_data(1000, 5)
    print(f"  Nodes: {len(nodes)}, Edges: {len(edges)}")

    graph_results = benchmark_graph_formats(nodes, edges)
    print_results(
        "Graph Serialization: GraphCowrie-Stream vs Industry",
        graph_results,
        'JSON (GraphSON-style)'
    )

    # === JSON-LD Benchmark ===
    print("\n" + "-" * 70)
    print("Generating JSON-LD data (500 entities)...")
    ld_data = generate_ld_data(500)
    print(f"  Entities: {len(ld_data['@graph'])}")

    ld_results = benchmark_ld_formats(ld_data)
    print_results(
        "Linked Data: Cowrie-LD vs Industry",
        ld_results,
        'JSON-LD'
    )

    # === Summary ===
    print("\n" + "=" * 70)
    print("  Summary & Industry Comparison")
    print("=" * 70)

    print("""
  GraphCowrie-Stream compares to:
  ┌──────────────────────────────────────────────────────────────────────┐
  │ Format              │ Use Case           │ vs GraphCowrie-Stream    │
  ├──────────────────────────────────────────────────────────────────────┤
  │ Apache TinkerPop    │ Gremlin graphs     │ Smaller, streaming       │
  │ GraphSON            │                    │                          │
  ├──────────────────────────────────────────────────────────────────────┤
  │ Neo4j CSV Import    │ Bulk loading       │ Binary, typed            │
  ├──────────────────────────────────────────────────────────────────────┤
  │ RDF N-Triples       │ Triple stores      │ Much smaller             │
  ├──────────────────────────────────────────────────────────────────────┤
  │ Apache Arrow Flight │ Columnar graphs    │ Row-based streaming      │
  ├──────────────────────────────────────────────────────────────────────┤
  │ Protocol Buffers    │ Custom schemas     │ Schema-free              │
  └──────────────────────────────────────────────────────────────────────┘

  Cowrie-LD compares to:
  ┌──────────────────────────────────────────────────────────────────────┐
  │ Format              │ Use Case           │ vs Cowrie-LD             │
  ├──────────────────────────────────────────────────────────────────────┤
  │ JSON-LD             │ Web semantics      │ Smaller, binary          │
  ├──────────────────────────────────────────────────────────────────────┤
  │ CBOR-LD             │ IoT, constrained   │ IRI tables, smaller      │
  ├──────────────────────────────────────────────────────────────────────┤
  │ HDT (RDF)           │ RDF archival       │ Simpler, JSON-native     │
  ├──────────────────────────────────────────────────────────────────────┤
  │ RDF/XML             │ Legacy systems     │ Much smaller             │
  ├──────────────────────────────────────────────────────────────────────┤
  │ Turtle/N3           │ Human-readable RDF │ Binary, faster           │
  └──────────────────────────────────────────────────────────────────────┘

  Key advantages of Cowrie formats:
  • Dictionary-coded keys/labels (smaller wire size)
  • Native streaming support (no buffering needed)
  • Auto-compression detection (transparent to apps)
  • IRI reference tables (Cowrie-LD specific)
  • Compatible with standard JSON data models
""")


if __name__ == "__main__":
    main()
