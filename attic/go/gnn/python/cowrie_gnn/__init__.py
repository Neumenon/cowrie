"""
Cowrie-GNN: Binary container format for Graph Neural Network datasets.

Fast, compact, and graph-native format for GNN workflows.
Replaces JSON/CSV/Parquet with direct NumPy/PyTorch integration.

Usage:
    import cowrie_gnn

    # Load a dataset
    dataset = cowrie_gnn.load("cora.sjgn")

    # Access features as NumPy arrays
    features = dataset.features["x"]  # np.ndarray
    labels = dataset.features["y"]

    # Get edge indices
    src, dst = dataset.edge_index()

    # Convert to PyTorch
    data = dataset.to_torch()

    # Convert to PyTorch Geometric
    pyg_data = dataset.to_pyg()
"""

from .reader import (
    load,
    load_compressed,
    Container,
    Section,
    SectionKind,
    DType,
    FeatureReader,
    SplitReader,
    AuxReader,
)

from .writer import (
    save,
    save_compressed,
    ContainerWriter,
    FeatureWriter,
    SplitWriter,
    AuxWriter,
)

from .adapters import (
    to_numpy,
    to_torch,
    to_pyg,
)

__version__ = "0.1.0"
__all__ = [
    # Reader
    "load",
    "load_compressed",
    "Container",
    "Section",
    "SectionKind",
    "DType",
    "FeatureReader",
    "SplitReader",
    "AuxReader",
    # Writer
    "save",
    "save_compressed",
    "ContainerWriter",
    "FeatureWriter",
    "SplitWriter",
    "AuxWriter",
    # Adapters
    "to_numpy",
    "to_torch",
    "to_pyg",
]
