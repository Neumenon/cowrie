#!/usr/bin/env python3
"""
Tests for Cowrie Python packages.

Run with: python -m pytest test_cowrie.py -v
Or simply: python test_cowrie.py
"""

import sys
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
import uuid as uuid_module

# Add package to path for testing
sys.path.insert(0, str(Path(__file__).parent))


# === Cowrie Core Tests ===

def test_cowrie_primitives():
    """Test basic Cowrie types."""
    import cowrie

    # Null
    v = cowrie.Null()
    assert v.is_null()

    # Bool
    v = cowrie.Bool(True)
    assert v.bool_value() == True
    v = cowrie.Bool(False)
    assert v.bool_value() == False

    # Int64
    v = cowrie.Int64(-123456)
    assert v.int64_value() == -123456

    # Uint64
    v = cowrie.Uint64(2**63 + 100)
    assert v.uint64_value() == 2**63 + 100

    # Float64
    v = cowrie.Float64(3.14159)
    assert abs(v.float64_value() - 3.14159) < 1e-10

    # String
    v = cowrie.String("Hello, World!")
    assert v.string_value() == "Hello, World!"

    # Bytes
    v = cowrie.Bytes(b"\x00\x01\x02\x03")
    assert v.bytes_value() == b"\x00\x01\x02\x03"

    print("  Cowrie primitives work")


def test_cowrie_extended_types():
    """Test extended Cowrie types."""
    import cowrie

    # Decimal128
    v = cowrie.Decimal128("3.14159265358979323846")
    dec = v.decimal128_value()
    restored = dec.to_decimal()
    assert str(restored).startswith("3.14159")

    # Datetime64
    now = datetime.now(tz=timezone.utc)
    v = cowrie.Datetime(now)
    nanos = v.datetime64_value()
    assert nanos > 0

    # UUID128
    u = uuid_module.uuid4()
    v = cowrie.UUID(u)
    assert v.uuid128_value() == u.bytes

    # BigInt
    big = 2**200 + 12345
    v = cowrie.BigInt(big)
    data = v.bigint_value()
    restored = int.from_bytes(data, byteorder='big', signed=True)
    assert restored == big

    print("  Cowrie extended types work")


def test_cowrie_containers():
    """Test array and object types."""
    import cowrie

    # Array
    arr = cowrie.Array(
        cowrie.Int64(1),
        cowrie.Int64(2),
        cowrie.Int64(3),
    )
    assert len(arr) == 3
    assert arr[0].int64_value() == 1
    assert arr[2].int64_value() == 3

    # Object
    obj = cowrie.Object(
        ("name", cowrie.String("Alice")),
        ("age", cowrie.Int64(30)),
        ("active", cowrie.Bool(True)),
    )
    assert len(obj) == 3
    assert obj["name"].string_value() == "Alice"
    assert obj.get("age").int64_value() == 30
    assert "active" in obj.keys()

    print("  Cowrie containers work")


def test_cowrie_roundtrip():
    """Test encode/decode roundtrip."""
    import cowrie

    # Complex nested structure
    data = {
        "users": [
            {"name": "Alice", "age": 30, "admin": True},
            {"name": "Bob", "age": 25, "admin": False},
        ],
        "count": 2,
        "active": True,
        "metadata": None,
    }

    # Encode
    encoded = cowrie.encode(data)
    assert encoded[:2] == b'SJ'
    assert encoded[2] == 2  # version

    # Decode
    decoded = cowrie.decode(encoded)
    result = cowrie.to_python(decoded)

    assert result["count"] == 2
    assert result["active"] == True
    assert result["metadata"] is None
    assert len(result["users"]) == 2
    assert result["users"][0]["name"] == "Alice"
    assert result["users"][1]["age"] == 25

    print(f"  Encoded size: {len(encoded)} bytes")
    print("  Cowrie roundtrip works")


def test_cowrie_from_python():
    """Test Python object conversion."""
    import cowrie

    # Various Python types
    data = {
        "int": 42,
        "float": 3.14,
        "str": "hello",
        "bytes": b"world",
        "list": [1, 2, 3],
        "bool": True,
        "none": None,
    }

    v = cowrie.from_python(data)
    assert v.is_object()

    result = cowrie.to_python(v)
    assert result["int"] == 42
    assert result["str"] == "hello"
    assert result["list"] == [1, 2, 3]

    print("  Cowrie from_python works")


# === GraphCowrie-Stream Tests ===

def test_graph_node_events():
    """Test node event encoding/decoding."""
    from cowrie.graph import StreamWriter, StreamReader, NodeEvent, Op, EventKind

    writer = StreamWriter()

    # Write nodes
    writer.write_node(NodeEvent(
        op=Op.UPSERT,
        id="node1",
        labels=["Person", "Employee"],
        props={"name": "Alice", "age": 30},
    ))
    writer.write_node(NodeEvent(
        op=Op.UPSERT,
        id="node2",
        labels=["Person"],
        props={"name": "Bob"},
    ))
    writer.write_node(NodeEvent(
        op=Op.DELETE,
        id="node3",
        labels=[],
    ))

    data = writer.getvalue()
    assert data[:4] == b'SJGS'

    # Read back
    events = list(StreamReader(data))
    assert len(events) == 3

    assert events[0].kind == EventKind.NODE
    assert events[0].node.id == "node1"
    assert events[0].node.labels == ["Person", "Employee"]
    assert events[0].node.props["name"] == "Alice"

    assert events[1].node.id == "node2"
    assert events[2].node.op == Op.DELETE

    print(f"  Stream size: {len(data)} bytes")
    print("  Graph node events work")


def test_graph_edge_events():
    """Test edge event encoding/decoding."""
    from cowrie.graph import StreamWriter, StreamReader, EdgeEvent, Op, EventKind

    writer = StreamWriter()

    # Write edges
    writer.write_edge(EdgeEvent(
        op=Op.UPSERT,
        label="KNOWS",
        from_id="n1",
        to_id="n2",
        props={"since": 2020},
    ))
    writer.write_edge(EdgeEvent(
        op=Op.UPSERT,
        id="e2",
        label="WORKS_FOR",
        from_id="n1",
        to_id="company1",
    ))

    data = writer.getvalue()
    events = list(StreamReader(data))
    assert len(events) == 2

    assert events[0].kind == EventKind.EDGE
    assert events[0].edge.label == "KNOWS"
    assert events[0].edge.from_id == "n1"
    assert events[0].edge.to_id == "n2"
    assert events[0].edge.props["since"] == 2020

    print("  Graph edge events work")


def test_graph_triple_events():
    """Test RDF triple event encoding/decoding."""
    from cowrie.graph import (
        StreamWriter, StreamReader,
        TripleEvent, RDFTerm, Op, EventKind, TermKind,
    )

    writer = StreamWriter()

    # Write triples
    writer.write_triple(TripleEvent(
        op=Op.UPSERT,
        subject=RDFTerm.iri("http://example.org/alice"),
        predicate="http://schema.org/name",
        object=RDFTerm.literal("Alice", lang="en"),
    ))
    writer.write_triple(TripleEvent(
        op=Op.UPSERT,
        subject=RDFTerm.bnode("_:b1"),
        predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        object=RDFTerm.iri("http://schema.org/Person"),
    ))

    data = writer.getvalue()
    events = list(StreamReader(data))
    assert len(events) == 2

    assert events[0].kind == EventKind.TRIPLE
    assert events[0].triple.subject.kind == TermKind.IRI
    assert events[0].triple.subject.value == "http://example.org/alice"
    assert events[0].triple.object.kind == TermKind.LITERAL
    assert events[0].triple.object.value == "Alice"
    assert events[0].triple.object.lang == "en"

    print("  Graph triple events work")


# === Cowrie-LD Tests ===

def test_ld_document():
    """Test Cowrie-LD document creation."""
    from cowrie.ld import LDDocument, IRI, TermFlags
    import cowrie

    doc = LDDocument()

    # Add terms (JSON-LD context)
    doc.add_term("name", IRI("http://schema.org/name"))
    doc.add_term("knows", IRI("http://schema.org/knows"), TermFlags.ID)

    # Create IRI values
    alice_iri = doc.iri_value(IRI("http://example.org/alice"))
    bob_iri = doc.iri_value(IRI("http://example.org/bob"))

    assert alice_iri.is_iri
    assert alice_iri.iri_id == 0
    assert bob_iri.iri_id == 1

    # Check terms
    term = doc.lookup_term("name")
    assert term is not None
    assert term.iri == IRI("http://schema.org/name")

    print("  Cowrie-LD document creation works")


def test_ld_roundtrip():
    """Test Cowrie-LD encode/decode roundtrip."""
    from cowrie.ld import LDDocument, IRI, encode, decode
    import cowrie

    doc = LDDocument()

    # Add terms
    doc.add_term("name", IRI("http://schema.org/name"))
    doc.add_term("age", IRI("http://schema.org/age"))

    # Build root value
    doc.root = cowrie.Object(
        ("@id", cowrie.String("http://example.org/alice")),
        ("name", cowrie.String("Alice")),
        ("age", cowrie.Int64(30)),
    )

    # Encode
    data = encode(doc)
    assert data[:4] == b'SJLD'

    # Decode
    doc2 = decode(data)

    assert len(doc2.terms) == 2
    assert doc2.lookup_term("name") is not None
    assert doc2.root is not None

    print(f"  LD document size: {len(data)} bytes")
    print("  Cowrie-LD roundtrip works")


# === Main ===

def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("Cowrie Python Package Tests")
    print("=" * 60)

    # Cowrie Core
    print("\n--- Cowrie Core ---")
    test_cowrie_primitives()
    test_cowrie_extended_types()
    test_cowrie_containers()
    test_cowrie_roundtrip()
    test_cowrie_from_python()

    # GraphCowrie-Stream
    print("\n--- GraphCowrie-Stream ---")
    test_graph_node_events()
    test_graph_edge_events()
    test_graph_triple_events()

    # Cowrie-LD
    print("\n--- Cowrie-LD ---")
    test_ld_document()
    test_ld_roundtrip()

    print()
    print("=" * 60)
    print("All tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    run_all_tests()
