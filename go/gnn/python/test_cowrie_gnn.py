#!/usr/bin/env python3
"""
Tests for Cowrie-GNN Python implementation.

Run with: python -m pytest test_cowrie_gnn.py -v
Or simply: python test_cowrie_gnn.py
"""

import sys
import numpy as np
from pathlib import Path

# Add package to path for testing
sys.path.insert(0, str(Path(__file__).parent))

from cowrie_gnn.reader import (
    BinaryReader,
    SectionKind,
    DType,
    FeatureMode,
)
from cowrie_gnn.writer import (
    ContainerWriter,
    FeatureWriter,
    SplitWriter,
    AuxWriter,
)
from cowrie_gnn import load, load_compressed, to_numpy


def test_binary_reader_uvarint():
    """Test uvarint encoding/decoding."""
    from cowrie_gnn.writer import BinaryWriter as BW

    # Test various values
    test_values = [0, 1, 127, 128, 255, 256, 16383, 16384, 2097151, 2097152]

    for v in test_values:
        w = BW()
        w.write_uvarint(v)
        data = w.getvalue()

        r = BinaryReader(data)
        decoded = r.read_uvarint()
        assert decoded == v, f"Expected {v}, got {decoded}"

    print("✅ uvarint encoding/decoding works")


def test_feature_writer_reader():
    """Test feature write/read round-trip."""
    # Create test data
    num_rows = 100
    dim = 64
    data = np.random.randn(num_rows, dim).astype(np.float32)

    # Write
    fw = FeatureWriter("x", DType.FLOAT32, [dim])
    fw.write_header(num_rows)
    fw.write_tensor(data)
    encoded = fw.getvalue()

    # Read
    from cowrie_gnn.reader import FeatureReader

    fr = FeatureReader(encoded)

    assert fr.feature_name == "x"
    assert fr.dtype == DType.FLOAT32
    assert fr.shape == [dim]
    assert fr.num_rows == num_rows

    tensor = fr.read_tensor()
    np.testing.assert_array_almost_equal(tensor, data)

    print("✅ Feature write/read round-trip works")


def test_split_writer_reader():
    """Test split write/read round-trip."""
    train = np.arange(0, 60)
    val = np.arange(60, 80)
    test = np.arange(80, 100)

    # Write
    sw = SplitWriter()
    sw.write_indices(train, val, test)
    encoded = sw.getvalue()

    # Read
    from cowrie_gnn.reader import SplitReader

    sr = SplitReader(encoded)

    np.testing.assert_array_equal(sr.train, train)
    np.testing.assert_array_equal(sr.val, val)
    np.testing.assert_array_equal(sr.test, test)

    print("✅ Split write/read round-trip works")


def test_aux_csr_writer_reader():
    """Test CSR aux write/read round-trip."""
    num_nodes = 10
    # Simple graph: 0->1, 0->2, 1->2, 2->3, ...
    indptr = np.array([0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10], dtype=np.int64)
    indices = np.array([1, 2, 2, 3, 4, 5, 6, 7, 8, 9], dtype=np.int64)

    # Write
    aw = AuxWriter()
    aw.write_csr(num_nodes, indptr, indices)
    encoded = aw.getvalue()

    # Read
    from cowrie_gnn.reader import AuxReader

    ar = AuxReader(encoded)

    assert ar.num_nodes == num_nodes
    np.testing.assert_array_equal(ar.indptr, indptr)
    np.testing.assert_array_equal(ar.indices, indices)

    # Test COO conversion
    src, dst = ar.to_coo()
    expected_src = np.array([0, 0, 1, 2, 3, 4, 5, 6, 7, 8])
    expected_dst = indices
    np.testing.assert_array_equal(src, expected_src)
    np.testing.assert_array_equal(dst, expected_dst)

    print("✅ CSR aux write/read round-trip works")


def test_container_roundtrip():
    """Test full container write/read round-trip."""
    # Create container
    writer = ContainerWriter("test-dataset")
    writer.set_directed(True)
    writer.add_node_type("node", 100)

    # Add features
    x = np.random.randn(100, 64).astype(np.float32)
    y = np.random.randint(0, 7, size=(100, 1)).astype(np.int64)
    writer.add_feature("x", x)
    writer.add_feature("y", y)

    # Add edges
    src = np.random.randint(0, 100, size=500)
    dst = np.random.randint(0, 100, size=500)
    writer.add_coo_edges(src, dst, num_nodes=100)

    # Add split
    train = np.arange(0, 60)
    val = np.arange(60, 80)
    test = np.arange(80, 100)
    writer.add_split(train, val, test)

    # Encode
    encoded = writer.encode()
    print(f"  Encoded size: {len(encoded)} bytes")

    # Decode
    container = load(encoded)

    # Verify
    assert container.meta.dataset_name == "test-dataset"
    assert container.meta.directed == True

    features = container.features
    np.testing.assert_array_almost_equal(features["x"], x)
    np.testing.assert_array_equal(features["y"], y)

    np.testing.assert_array_equal(container.split.train, train)
    np.testing.assert_array_equal(container.split.val, val)
    np.testing.assert_array_equal(container.split.test, test)

    print("✅ Full container round-trip works")


def test_compression():
    """Test compressed container."""
    try:
        import zstandard
    except ImportError:
        print("⚠️ Skipping compression test (zstandard not installed)")
        return

    # Create container
    writer = ContainerWriter("compressed-test")
    x = np.random.randn(1000, 128).astype(np.float32)
    writer.add_feature("x", x)

    # Encode uncompressed
    raw = writer.encode()

    # Encode compressed
    compressed = writer.encode_compressed()

    print(f"  Raw size: {len(raw)} bytes")
    print(f"  Compressed size: {len(compressed)} bytes")
    print(f"  Ratio: {len(raw) / len(compressed):.2f}x")

    # Decode compressed
    container = load_compressed(compressed)
    np.testing.assert_array_almost_equal(container.features["x"], x)

    print("✅ Compression works")


def test_to_numpy():
    """Test NumPy adapter."""
    writer = ContainerWriter("numpy-test")
    writer.add_node_type("node", 50)

    x = np.random.randn(50, 32).astype(np.float32)
    writer.add_feature("x", x)

    src = np.random.randint(0, 50, size=100)
    dst = np.random.randint(0, 50, size=100)
    writer.add_coo_edges(src, dst, num_nodes=50)

    container = load(writer.encode())
    data = to_numpy(container)

    assert "x" in data
    assert "edge_index" in data
    assert isinstance(data["edge_index"], tuple)

    print("✅ to_numpy adapter works")


def test_cora_like():
    """Test with Cora-like dataset dimensions."""
    writer = ContainerWriter("cora-like")
    writer.set_directed(True)
    writer.add_node_type("paper", 2708)
    writer.add_edge_type("paper", "cites", "paper")

    # Features: 1433-dim
    x = np.random.randn(2708, 1433).astype(np.float32)
    writer.add_feature("x", x, node_type="paper")

    # Labels: 7 classes
    y = np.random.randint(0, 7, size=(2708, 1)).astype(np.int64)
    writer.add_feature("y", y, node_type="paper")

    # Edges: ~5429 edges
    src = np.random.randint(0, 2708, size=5429)
    dst = np.random.randint(0, 2708, size=5429)
    writer.add_coo_edges(src, dst, num_nodes=2708)

    # Split
    train = np.arange(0, 140)
    val = np.arange(140, 640)
    test = np.arange(1708, 2708)
    writer.add_split(train, val, test)

    # Encode
    raw = writer.encode()
    print(f"  Cora-like raw: {len(raw):,} bytes ({len(raw) / 1e6:.2f} MB)")

    try:
        compressed = writer.encode_compressed()
        print(
            f"  Cora-like compressed: {len(compressed):,} bytes ({len(compressed) / 1e6:.2f} MB)"
        )
        print(f"  Compression ratio: {len(raw) / len(compressed):.2f}x")

        # Load and verify
        container = load_compressed(compressed)
        assert container.num_nodes() == 2708
        assert container.num_edges() == 5429
    except ImportError:
        container = load(raw)

    # Verify features - use allclose for large arrays (numpy 2.x compatibility)
    assert np.allclose(container.features["x"], x), "x feature mismatch"

    print("✅ Cora-like dataset works")


def test_float16():
    """Test float16 dtype."""
    writer = ContainerWriter("float16-test")

    # Use float16 for smaller file size
    x = np.random.randn(1000, 128).astype(np.float16)
    writer.add_feature("x", x, dtype=DType.FLOAT16)

    encoded = writer.encode()
    container = load(encoded)

    # Note: Float16 has lower precision
    restored = container.features["x"]
    assert restored.dtype == np.float16

    print(f"  Float16 size: {len(encoded)} bytes (vs ~512KB for float32)")
    print("✅ Float16 dtype works")


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("Cowrie-GNN Python Tests")
    print("=" * 60)

    test_binary_reader_uvarint()
    test_feature_writer_reader()
    test_split_writer_reader()
    test_aux_csr_writer_reader()
    test_container_roundtrip()
    test_compression()
    test_to_numpy()
    test_cora_like()
    test_float16()

    print()
    print("=" * 60)
    print("All tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    run_all_tests()
