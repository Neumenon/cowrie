"""
Cowrie-GNN Reader - Core container and section parsing.

Wire format:
    Magic: 'SJGN' (4 bytes) or 'SJGZ' (compressed)
    Version: 0x01 (1 byte)
    Flags: bitfield (1 byte)
    SectionCount: uvarint
    Sections: [Kind, Name, BodyLen, Body]...
"""

import struct
import json
from enum import IntEnum
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Any, BinaryIO
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


# === Constants ===

MAGIC_SJGN = b'SJGN'
MAGIC_SJGZ = b'SJGZ'
VERSION = 0x01

# Flags
FLAG_HETEROGENEOUS = 0x01
FLAG_TEMPORAL = 0x02
FLAG_HAS_CSR = 0x04
FLAG_COMPRESSED_ZSTD = 0x08


class SectionKind(IntEnum):
    """Section types in Cowrie-GNN container."""
    META = 0
    NODE_TABLE = 1
    EDGE_TABLE = 2
    FEATURE = 3
    SPLIT = 4
    AUX = 5


class DType(IntEnum):
    """Data types for tensors."""
    FLOAT32 = 0
    FLOAT64 = 1
    INT32 = 2
    INT64 = 3
    UINT32 = 4
    UINT64 = 5
    BOOL = 6
    FLOAT16 = 7

    @property
    def numpy_dtype(self) -> np.dtype:
        """Convert to NumPy dtype."""
        mapping = {
            DType.FLOAT32: np.float32,
            DType.FLOAT64: np.float64,
            DType.INT32: np.int32,
            DType.INT64: np.int64,
            DType.UINT32: np.uint32,
            DType.UINT64: np.uint64,
            DType.BOOL: np.bool_,
            DType.FLOAT16: np.float16,
        }
        return np.dtype(mapping[self])

    @property
    def byte_size(self) -> int:
        """Bytes per element."""
        sizes = {
            DType.FLOAT32: 4, DType.FLOAT64: 8,
            DType.INT32: 4, DType.INT64: 8,
            DType.UINT32: 4, DType.UINT64: 8,
            DType.BOOL: 1, DType.FLOAT16: 2,
        }
        return sizes[self]


class FeatureMode(IntEnum):
    """Feature storage mode."""
    ROW_WISE = 0  # Each row is separate
    BLOCKED = 1   # Contiguous tensor


class SplitMode(IntEnum):
    """Split storage mode."""
    INDICES = 0  # Array of indices
    MASK = 1     # Bitmask


class AuxFormat(IntEnum):
    """Auxiliary data format."""
    CSR = 0
    CSC = 1


# === Reader Utilities ===

class BinaryReader:
    """Low-level binary reader with uvarint support."""

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def read(self, n: int) -> bytes:
        if self.pos + n > len(self.data):
            raise EOFError("Unexpected end of data")
        result = self.data[self.pos:self.pos + n]
        self.pos += n
        return result

    def read_byte(self) -> int:
        return self.read(1)[0]

    def read_uvarint(self) -> int:
        """Read unsigned variable-length integer."""
        result = 0
        shift = 0
        while True:
            b = self.read_byte()
            result |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                break
            shift += 7
        return result

    def read_string(self) -> str:
        length = self.read_uvarint()
        return self.read(length).decode('utf-8')

    def read_u64_le(self) -> int:
        return struct.unpack('<Q', self.read(8))[0]

    def read_i64_le(self) -> int:
        return struct.unpack('<q', self.read(8))[0]

    def read_f32_le(self) -> float:
        return struct.unpack('<f', self.read(4))[0]

    def read_f64_le(self) -> float:
        return struct.unpack('<d', self.read(8))[0]

    def remaining(self) -> bytes:
        return self.data[self.pos:]


# === Section Structures ===

@dataclass
class Section:
    """A section in the container."""
    kind: SectionKind
    name: str
    body: bytes


@dataclass
class Meta:
    """Dataset metadata."""
    dataset_name: str = ""
    version: str = ""
    directed: bool = True
    temporal: bool = False
    heterogeneous: bool = False
    node_types: List[str] = field(default_factory=list)
    edge_types: List[Dict[str, str]] = field(default_factory=list)
    id_spaces: Dict[str, Dict[str, int]] = field(default_factory=dict)
    features: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    labels: Dict[str, Dict[str, Any]] = field(default_factory=dict)


# === Feature Reader ===

class FeatureReader:
    """Reads feature tensors from a section."""

    def __init__(self, data: bytes):
        self.r = BinaryReader(data)

        # Read header
        self.mode = FeatureMode(self.r.read_byte())
        self.feature_name = self.r.read_string()
        self.dtype = DType(self.r.read_uvarint())

        # Read shape
        shape_len = self.r.read_uvarint()
        self.shape = [self.r.read_uvarint() for _ in range(shape_len)]

        # Read num_rows for blocked mode
        self.num_rows = 0
        if self.mode == FeatureMode.BLOCKED:
            self.num_rows = self.r.read_uvarint()

    def read_tensor(self) -> np.ndarray:
        """Read the feature tensor as NumPy array."""
        if self.mode == FeatureMode.BLOCKED:
            # Calculate total elements
            elements_per_row = 1
            for dim in self.shape:
                elements_per_row *= dim
            total_elements = self.num_rows * elements_per_row

            # Read raw bytes and convert to numpy
            byte_size = self.dtype.byte_size
            raw = self.r.read(total_elements * byte_size)
            arr = np.frombuffer(raw, dtype=self.dtype.numpy_dtype)

            # Reshape
            full_shape = [self.num_rows] + list(self.shape)
            return arr.reshape(full_shape)
        else:
            # Row-wise mode - read row by row
            rows = []
            while self.r.pos < len(self.r.data):
                row_id = self.r.read_uvarint()
                elements = 1
                for dim in self.shape:
                    elements *= dim
                byte_size = self.dtype.byte_size
                raw = self.r.read(elements * byte_size)
                row = np.frombuffer(raw, dtype=self.dtype.numpy_dtype)
                rows.append((row_id, row.reshape(self.shape)))
            return rows  # List of (id, array) tuples


# === Split Reader ===

class SplitReader:
    """Reads train/val/test splits."""

    def __init__(self, data: bytes):
        self.r = BinaryReader(data)

        # Read mode
        self.mode = SplitMode(self.r.read_byte())

        if self.mode == SplitMode.INDICES:
            # Read index arrays
            train_len = self.r.read_uvarint()
            self.train = np.array([self.r.read_uvarint() for _ in range(train_len)], dtype=np.int64)

            val_len = self.r.read_uvarint()
            self.val = np.array([self.r.read_uvarint() for _ in range(val_len)], dtype=np.int64)

            test_len = self.r.read_uvarint()
            self.test = np.array([self.r.read_uvarint() for _ in range(test_len)], dtype=np.int64)
        else:
            # Mask mode
            num_nodes = self.r.read_uvarint()
            mask_bytes = (num_nodes + 7) // 8

            train_mask = self.r.read(mask_bytes)
            val_mask = self.r.read(mask_bytes)
            test_mask = self.r.read(mask_bytes)

            # Convert to boolean arrays
            self.train = self._mask_to_indices(train_mask, num_nodes)
            self.val = self._mask_to_indices(val_mask, num_nodes)
            self.test = self._mask_to_indices(test_mask, num_nodes)

    def _mask_to_indices(self, mask: bytes, num_nodes: int) -> np.ndarray:
        """Convert bitmask to indices."""
        indices = []
        for i in range(num_nodes):
            byte_idx = i // 8
            bit_idx = i % 8
            if mask[byte_idx] & (1 << bit_idx):
                indices.append(i)
        return np.array(indices, dtype=np.int64)

    def get_masks(self, num_nodes: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Get boolean masks for train/val/test."""
        train_mask = np.zeros(num_nodes, dtype=bool)
        val_mask = np.zeros(num_nodes, dtype=bool)
        test_mask = np.zeros(num_nodes, dtype=bool)

        train_mask[self.train] = True
        val_mask[self.val] = True
        test_mask[self.test] = True

        return train_mask, val_mask, test_mask


# === Aux Reader (CSR/CSC) ===

class AuxReader:
    """Reads auxiliary data (CSR/CSC indices)."""

    def __init__(self, data: bytes):
        self.r = BinaryReader(data)

        # Read format
        self.format = AuxFormat(self.r.read_byte())

        # Read num_nodes
        self.num_nodes = self.r.read_uvarint()

        # Read indptr (N+1 elements)
        indptr_bytes = self.r.read((self.num_nodes + 1) * 8)
        self.indptr = np.frombuffer(indptr_bytes, dtype=np.int64)

        # Read indices (remaining bytes)
        num_edges = self.indptr[-1]
        indices_bytes = self.r.read(int(num_edges) * 8)
        self.indices = np.frombuffer(indices_bytes, dtype=np.int64)

    def to_coo(self) -> Tuple[np.ndarray, np.ndarray]:
        """Convert CSR/CSC to COO format (src, dst arrays)."""
        src = []
        dst = []
        for node in range(self.num_nodes):
            start = self.indptr[node]
            end = self.indptr[node + 1]
            neighbors = self.indices[start:end]
            if self.format == AuxFormat.CSR:
                # CSR: indptr indexes source, indices are destinations
                src.extend([node] * len(neighbors))
                dst.extend(neighbors)
            else:
                # CSC: indptr indexes destination, indices are sources
                src.extend(neighbors)
                dst.extend([node] * len(neighbors))
        return np.array(src, dtype=np.int64), np.array(dst, dtype=np.int64)


# === Container ===

@dataclass
class Container:
    """Cowrie-GNN container with all sections."""
    flags: int = 0
    meta: Optional[Meta] = None
    sections: List[Section] = field(default_factory=list)

    # Cached parsed data
    _features: Dict[str, np.ndarray] = field(default_factory=dict, repr=False)
    _split: Optional[SplitReader] = field(default=None, repr=False)
    _aux: Optional[AuxReader] = field(default=None, repr=False)

    @property
    def is_heterogeneous(self) -> bool:
        return bool(self.flags & FLAG_HETEROGENEOUS)

    @property
    def is_temporal(self) -> bool:
        return bool(self.flags & FLAG_TEMPORAL)

    @property
    def has_csr(self) -> bool:
        return bool(self.flags & FLAG_HAS_CSR)

    def get_section(self, name: str) -> Optional[Section]:
        """Get section by name."""
        for s in self.sections:
            if s.name == name:
                return s
        return None

    def get_sections_by_kind(self, kind: SectionKind) -> List[Section]:
        """Get all sections of a kind."""
        return [s for s in self.sections if s.kind == kind]

    @property
    def features(self) -> Dict[str, np.ndarray]:
        """Get all features as NumPy arrays (cached)."""
        if not self._features:
            for section in self.get_sections_by_kind(SectionKind.FEATURE):
                reader = FeatureReader(section.body)
                tensor = reader.read_tensor()
                # Use feature name from reader, or section name
                name = reader.feature_name or section.name.split(":")[-1]
                self._features[name] = tensor
        return self._features

    @property
    def split(self) -> Optional[SplitReader]:
        """Get train/val/test split."""
        if self._split is None:
            split_sections = self.get_sections_by_kind(SectionKind.SPLIT)
            if split_sections:
                self._split = SplitReader(split_sections[0].body)
        return self._split

    @property
    def aux(self) -> Optional[AuxReader]:
        """Get auxiliary data (CSR/CSC)."""
        if self._aux is None:
            aux_sections = self.get_sections_by_kind(SectionKind.AUX)
            if aux_sections:
                self._aux = AuxReader(aux_sections[0].body)
        return self._aux

    def edge_index(self) -> Tuple[np.ndarray, np.ndarray]:
        """Get edge indices as (src, dst) arrays."""
        if self.aux:
            return self.aux.to_coo()
        raise ValueError("No edge data found (no Aux section)")

    def num_nodes(self) -> int:
        """Get number of nodes."""
        if self.aux:
            return self.aux.num_nodes
        # Try to infer from features
        for name, feat in self.features.items():
            if isinstance(feat, np.ndarray):
                return feat.shape[0]
        raise ValueError("Cannot determine number of nodes")

    def num_edges(self) -> int:
        """Get number of edges."""
        if self.aux:
            return int(self.aux.indptr[-1])
        raise ValueError("No edge data found")


# === Loading Functions ===

def load(path_or_data) -> Container:
    """
    Load a Cowrie-GNN container from file or bytes.

    Args:
        path_or_data: File path (str) or bytes

    Returns:
        Container with all sections parsed
    """
    if isinstance(path_or_data, (str, bytes)) and not isinstance(path_or_data, bytes):
        with open(path_or_data, 'rb') as f:
            data = f.read()
    else:
        data = path_or_data

    return _decode(data)


def load_compressed(path_or_data) -> Container:
    """
    Load a potentially compressed Cowrie-GNN container.
    Auto-detects SJGN (uncompressed) vs SJGZ (zstd compressed).
    """
    if isinstance(path_or_data, str):
        with open(path_or_data, 'rb') as f:
            data = f.read()
    else:
        data = path_or_data

    # Check magic
    if data[:4] == MAGIC_SJGZ:
        if not HAS_ZSTD:
            raise ImportError("zstandard or zstd package required for compressed files. "
                            "Install with: pip install zstandard")
        # Decompress: skip magic (4) + original_size (4)
        compressed = data[8:]
        if hasattr(zstd, 'ZstdDecompressor'):
            # zstandard package
            dctx = zstd.ZstdDecompressor()
            data = dctx.decompress(compressed)
        else:
            # zstd package
            data = zstd.decompress(compressed)

    return _decode(data)


def _decode(data: bytes) -> Container:
    """Decode Cowrie-GNN container from bytes."""
    r = BinaryReader(data)

    # Read magic
    magic = r.read(4)
    if magic != MAGIC_SJGN:
        raise ValueError(f"Invalid magic: {magic!r}, expected {MAGIC_SJGN!r}")

    # Read version
    version = r.read_byte()
    if version != VERSION:
        raise ValueError(f"Unsupported version: {version}, expected {VERSION}")

    # Read flags
    flags = r.read_byte()

    # Read section count
    section_count = r.read_uvarint()

    container = Container(flags=flags)
    container.sections = []

    # Read sections
    for _ in range(section_count):
        kind = SectionKind(r.read_uvarint())
        name = r.read_string()
        body_len = r.read_u64_le()
        body = r.read(body_len)

        section = Section(kind=kind, name=name, body=body)

        # Parse meta section
        if kind == SectionKind.META and name == "meta":
            meta_dict = json.loads(body.decode('utf-8'))
            container.meta = Meta(
                dataset_name=meta_dict.get("dataset_name", ""),
                version=meta_dict.get("version", ""),
                directed=meta_dict.get("directed", True),
                temporal=meta_dict.get("temporal", False),
                heterogeneous=meta_dict.get("heterogeneous", False),
                node_types=meta_dict.get("node_types", []),
                edge_types=meta_dict.get("edge_types", []),
                id_spaces=meta_dict.get("id_spaces", {}),
                features=meta_dict.get("features", {}),
                labels=meta_dict.get("labels", {}),
            )
        else:
            container.sections.append(section)

    return container
