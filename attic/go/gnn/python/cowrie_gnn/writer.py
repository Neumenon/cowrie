"""
Cowrie-GNN Writer - Create and save Cowrie-GNN containers.

Usage:
    from cowrie_gnn import ContainerWriter, FeatureWriter, save

    # Create container
    writer = ContainerWriter("my-dataset")
    writer.set_directed(True)
    writer.add_node_type("paper", num_nodes=2708)

    # Add features
    features = np.random.randn(2708, 1433).astype(np.float32)
    writer.add_feature("x", features)

    # Add labels
    labels = np.random.randint(0, 7, size=(2708,)).astype(np.int64)
    writer.add_feature("y", labels.reshape(-1, 1))

    # Add edges (CSR format)
    writer.add_csr_edges(indptr, indices)

    # Add split
    writer.add_split(train_idx, val_idx, test_idx)

    # Save
    save(writer, "dataset.sjgn")
    save_compressed(writer, "dataset.sjgz")
"""

import struct
import json
from typing import List, Dict, Any, Optional, Tuple, Union
from io import BytesIO

import numpy as np

# Optional imports
try:
    import zstandard as zstd
    HAS_ZSTD = True
except ImportError:
    try:
        import zstd
        HAS_ZSTD = True
    except ImportError:
        HAS_ZSTD = False

from .reader import (
    MAGIC_SJGN, MAGIC_SJGZ, VERSION,
    FLAG_HETEROGENEOUS, FLAG_TEMPORAL, FLAG_HAS_CSR,
    SectionKind, DType, FeatureMode, SplitMode, AuxFormat,
)


# === Writer Utilities ===

class BinaryWriter:
    """Low-level binary writer with uvarint support."""

    def __init__(self):
        self.buf = BytesIO()

    def write(self, data: bytes):
        self.buf.write(data)

    def write_byte(self, b: int):
        self.buf.write(bytes([b]))

    def write_uvarint(self, v: int):
        """Write unsigned variable-length integer."""
        while v >= 0x80:
            self.buf.write(bytes([(v & 0x7F) | 0x80]))
            v >>= 7
        self.buf.write(bytes([v]))

    def write_string(self, s: str):
        encoded = s.encode('utf-8')
        self.write_uvarint(len(encoded))
        self.buf.write(encoded)

    def write_u64_le(self, v: int):
        self.buf.write(struct.pack('<Q', v))

    def write_i64_le(self, v: int):
        self.buf.write(struct.pack('<q', v))

    def write_f32_le(self, v: float):
        self.buf.write(struct.pack('<f', v))

    def write_f64_le(self, v: float):
        self.buf.write(struct.pack('<d', v))

    def getvalue(self) -> bytes:
        return self.buf.getvalue()


# === Section Writers ===

class FeatureWriter:
    """Write feature tensors."""

    def __init__(self, name: str, dtype: DType, shape: List[int],
                 mode: FeatureMode = FeatureMode.BLOCKED):
        self.name = name
        self.dtype = dtype
        self.shape = shape
        self.mode = mode
        self.w = BinaryWriter()

    def write_header(self, num_rows: int):
        """Write feature header."""
        self.w.write_byte(self.mode)
        self.w.write_string(self.name)
        self.w.write_uvarint(self.dtype)
        self.w.write_uvarint(len(self.shape))
        for dim in self.shape:
            self.w.write_uvarint(dim)
        if self.mode == FeatureMode.BLOCKED:
            self.w.write_uvarint(num_rows)

    def write_tensor(self, data: np.ndarray):
        """Write tensor data (blocked mode)."""
        # Ensure correct dtype
        expected = self.dtype
        if expected == DType.FLOAT32:
            data = data.astype(np.float32)
        elif expected == DType.FLOAT64:
            data = data.astype(np.float64)
        elif expected == DType.INT32:
            data = data.astype(np.int32)
        elif expected == DType.INT64:
            data = data.astype(np.int64)
        elif expected == DType.UINT32:
            data = data.astype(np.uint32)
        elif expected == DType.UINT64:
            data = data.astype(np.uint64)
        elif expected == DType.BOOL:
            data = data.astype(np.bool_)
        elif expected == DType.FLOAT16:
            data = data.astype(np.float16)

        self.w.write(data.tobytes())

    def getvalue(self) -> bytes:
        return self.w.getvalue()


class SplitWriter:
    """Write train/val/test splits."""

    def __init__(self, mode: SplitMode = SplitMode.INDICES):
        self.mode = mode
        self.w = BinaryWriter()

    def write_indices(self, train: np.ndarray, val: np.ndarray, test: np.ndarray):
        """Write split as index arrays."""
        self.w.write_byte(SplitMode.INDICES)

        for arr in [train, val, test]:
            self.w.write_uvarint(len(arr))
            for idx in arr:
                self.w.write_uvarint(int(idx))

    def write_masks(self, train_mask: np.ndarray, val_mask: np.ndarray,
                   test_mask: np.ndarray, num_nodes: int):
        """Write split as bitmasks."""
        self.w.write_byte(SplitMode.MASK)
        self.w.write_uvarint(num_nodes)

        for mask in [train_mask, val_mask, test_mask]:
            mask_bytes = self._mask_to_bytes(mask, num_nodes)
            self.w.write(mask_bytes)

    def _mask_to_bytes(self, mask: np.ndarray, num_nodes: int) -> bytes:
        """Convert boolean mask to bytes."""
        byte_count = (num_nodes + 7) // 8
        result = bytearray(byte_count)
        for i, val in enumerate(mask):
            if val:
                result[i // 8] |= (1 << (i % 8))
        return bytes(result)

    def getvalue(self) -> bytes:
        return self.w.getvalue()


class AuxWriter:
    """Write auxiliary data (CSR/CSC)."""

    def __init__(self):
        self.w = BinaryWriter()

    def write_csr(self, num_nodes: int, indptr: np.ndarray, indices: np.ndarray):
        """Write CSR format."""
        self.w.write_byte(AuxFormat.CSR)
        self.w.write_uvarint(num_nodes)
        self.w.write(indptr.astype(np.int64).tobytes())
        self.w.write(indices.astype(np.int64).tobytes())

    def write_csc(self, num_nodes: int, indptr: np.ndarray, indices: np.ndarray):
        """Write CSC format."""
        self.w.write_byte(AuxFormat.CSC)
        self.w.write_uvarint(num_nodes)
        self.w.write(indptr.astype(np.int64).tobytes())
        self.w.write(indices.astype(np.int64).tobytes())

    def getvalue(self) -> bytes:
        return self.w.getvalue()


# === Container Writer ===

class ContainerWriter:
    """Build Cowrie-GNN containers."""

    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name
        self.flags = 0
        self.directed = True
        self.temporal = False
        self.heterogeneous = False

        self.node_types: List[str] = []
        self.edge_types: List[Dict[str, str]] = []
        self.id_spaces: Dict[str, Dict[str, int]] = {}
        self.feature_specs: Dict[str, Dict[str, Any]] = {}
        self.label_specs: Dict[str, Dict[str, Any]] = {}

        self.sections: List[Tuple[SectionKind, str, bytes]] = []

    def set_directed(self, v: bool):
        self.directed = v

    def set_temporal(self, v: bool):
        self.temporal = v
        if v:
            self.flags |= FLAG_TEMPORAL
        else:
            self.flags &= ~FLAG_TEMPORAL

    def set_heterogeneous(self, v: bool):
        self.heterogeneous = v
        if v:
            self.flags |= FLAG_HETEROGENEOUS
        else:
            self.flags &= ~FLAG_HETEROGENEOUS

    def add_node_type(self, node_type: str, num_nodes: int):
        """Add a node type with count."""
        start = 0
        for space in self.id_spaces.values():
            end = space["start"] + space["count"]
            if end > start:
                start = end

        self.node_types.append(node_type)
        self.id_spaces[node_type] = {"start": start, "count": num_nodes}

    def add_edge_type(self, src_type: str, edge_type: str, dst_type: str):
        """Add an edge type."""
        self.edge_types.append({
            "src_type": src_type,
            "edge_type": edge_type,
            "dst_type": dst_type,
        })

    def add_feature(self, name: str, data: np.ndarray,
                   node_type: str = "", dtype: Optional[DType] = None):
        """
        Add a feature tensor.

        Args:
            name: Feature name (e.g., "x", "y")
            data: NumPy array with shape [N, ...] where N is num_nodes
            node_type: Optional node type for heterogeneous graphs
            dtype: Optional dtype override
        """
        if dtype is None:
            # Infer from numpy dtype
            np_to_dtype = {
                np.float32: DType.FLOAT32,
                np.float64: DType.FLOAT64,
                np.int32: DType.INT32,
                np.int64: DType.INT64,
                np.uint32: DType.UINT32,
                np.uint64: DType.UINT64,
                np.bool_: DType.BOOL,
                np.float16: DType.FLOAT16,
            }
            dtype = np_to_dtype.get(data.dtype.type, DType.FLOAT32)

        num_rows = data.shape[0]
        shape = list(data.shape[1:]) if len(data.shape) > 1 else [1]

        # Create feature writer
        fw = FeatureWriter(name, dtype, shape)
        fw.write_header(num_rows)
        fw.write_tensor(data.flatten())

        # Add section
        section_name = f"feature:{node_type}:{name}" if node_type else f"feature:{name}"
        self.sections.append((SectionKind.FEATURE, section_name, fw.getvalue()))

        # Track in meta
        key = f"{node_type}:{name}" if node_type else name
        self.feature_specs[key] = {
            "shape": shape,
            "dtype": dtype.name.lower(),
        }

    def add_csr_edges(self, indptr: np.ndarray, indices: np.ndarray,
                      name: str = "csr"):
        """Add edges in CSR format."""
        num_nodes = len(indptr) - 1
        aw = AuxWriter()
        aw.write_csr(num_nodes, indptr, indices)
        self.sections.append((SectionKind.AUX, f"aux:{name}", aw.getvalue()))
        self.flags |= FLAG_HAS_CSR

    def add_coo_edges(self, src: np.ndarray, dst: np.ndarray,
                      num_nodes: Optional[int] = None, name: str = "csr"):
        """Add edges in COO format (converted to CSR internally)."""
        if num_nodes is None:
            num_nodes = max(src.max(), dst.max()) + 1

        # Convert COO to CSR
        indptr, indices = self._coo_to_csr(num_nodes, src, dst)
        self.add_csr_edges(indptr, indices, name)

    def _coo_to_csr(self, num_nodes: int, src: np.ndarray,
                    dst: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Convert COO to CSR."""
        # Count edges per node
        counts = np.zeros(num_nodes, dtype=np.int64)
        for s in src:
            counts[s] += 1

        # Build indptr
        indptr = np.zeros(num_nodes + 1, dtype=np.int64)
        indptr[1:] = np.cumsum(counts)

        # Build indices
        indices = np.zeros(len(src), dtype=np.int64)
        current = np.zeros(num_nodes, dtype=np.int64)
        for s, d in zip(src, dst):
            pos = indptr[s] + current[s]
            indices[pos] = d
            current[s] += 1

        return indptr, indices

    def add_split(self, train: np.ndarray, val: np.ndarray, test: np.ndarray,
                  name: str = "default"):
        """Add train/val/test split indices."""
        sw = SplitWriter()
        sw.write_indices(train.astype(np.int64), val.astype(np.int64),
                        test.astype(np.int64))
        self.sections.append((SectionKind.SPLIT, f"split:{name}", sw.getvalue()))

    def add_split_masks(self, train_mask: np.ndarray, val_mask: np.ndarray,
                       test_mask: np.ndarray, name: str = "default"):
        """Add train/val/test split as masks."""
        num_nodes = len(train_mask)
        sw = SplitWriter(mode=SplitMode.MASK)
        sw.write_masks(train_mask, val_mask, test_mask, num_nodes)
        self.sections.append((SectionKind.SPLIT, f"split:{name}", sw.getvalue()))

    def _build_meta(self) -> bytes:
        """Build meta section."""
        meta = {
            "dataset_name": self.dataset_name,
            "directed": self.directed,
            "temporal": self.temporal,
            "heterogeneous": self.heterogeneous,
            "node_types": self.node_types,
            "edge_types": self.edge_types,
            "id_spaces": self.id_spaces,
            "features": self.feature_specs,
            "labels": self.label_specs,
        }
        return json.dumps(meta).encode('utf-8')

    def encode(self) -> bytes:
        """Encode container to bytes."""
        w = BinaryWriter()

        # Magic
        w.write(MAGIC_SJGN)

        # Version
        w.write_byte(VERSION)

        # Flags
        w.write_byte(self.flags)

        # Build all sections (meta first)
        all_sections = []
        meta_body = self._build_meta()
        all_sections.append((SectionKind.META, "meta", meta_body))
        all_sections.extend(self.sections)

        # Section count
        w.write_uvarint(len(all_sections))

        # Write sections
        for kind, name, body in all_sections:
            w.write_uvarint(kind)
            w.write_string(name)
            w.write_u64_le(len(body))
            w.write(body)

        return w.getvalue()

    def encode_compressed(self) -> bytes:
        """Encode and compress with zstd."""
        if not HAS_ZSTD:
            raise ImportError("zstandard package required. Install with: pip install zstandard")

        raw = self.encode()

        # Compress
        if hasattr(zstd, 'ZstdCompressor'):
            # zstandard package
            cctx = zstd.ZstdCompressor(level=19)  # Best compression
            compressed = cctx.compress(raw)
        else:
            # zstd package
            compressed = zstd.compress(raw, 19)

        # Build output: SJGZ + original_size + compressed
        result = bytearray()
        result.extend(MAGIC_SJGZ)
        result.extend(struct.pack('<I', len(raw)))
        result.extend(compressed)

        return bytes(result)


# === Convenience Functions ===

def save(container: ContainerWriter, path: str):
    """Save container to file."""
    data = container.encode()
    with open(path, 'wb') as f:
        f.write(data)


def save_compressed(container: ContainerWriter, path: str):
    """Save compressed container to file."""
    data = container.encode_compressed()
    with open(path, 'wb') as f:
        f.write(data)
