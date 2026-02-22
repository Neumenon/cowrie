# Cowrie-GNN Python

Fast binary format for Graph Neural Network datasets. 2.6x smaller than JSON+zstd, with direct NumPy/PyTorch/PyG integration.

## Installation

```bash
pip install cowrie-gnn

# With compression support
pip install cowrie-gnn[compression]

# With PyTorch Geometric
pip install cowrie-gnn[pyg]
```

## Quick Start

### Loading Data

```python
import cowrie_gnn

# Load dataset (auto-detects compression)
dataset = cowrie_gnn.load_compressed("cora.sjgn")

# Access features as NumPy arrays
x = dataset.features["x"]  # [2708, 1433] float32
y = dataset.features["y"]  # [2708, 1] int64

# Get edge indices
src, dst = dataset.edge_index()  # CSR -> COO conversion

# Get train/val/test split
train_idx = dataset.split.train
val_idx = dataset.split.val
test_idx = dataset.split.test
```

### PyTorch Integration

```python
from cowrie_gnn import load_compressed, to_torch

dataset = load_compressed("cora.sjgn")
data = to_torch(dataset, device="cuda")

# data['x'] -> torch.Tensor [2708, 1433]
# data['y'] -> torch.Tensor [2708, 1]
# data['edge_index'] -> torch.Tensor [2, 10556]
# data['train_idx'] -> torch.Tensor
```

### PyTorch Geometric Integration

```python
from cowrie_gnn import load_compressed, to_pyg

dataset = load_compressed("cora.sjgn")
data = to_pyg(dataset)

# Data(x=[2708, 1433], edge_index=[2, 10556], y=[2708],
#      train_mask=[2708], val_mask=[2708], test_mask=[2708])

# Use directly with PyG models
from torch_geometric.nn import GCNConv

conv = GCNConv(1433, 128)
out = conv(data.x, data.edge_index)
```

### DGL Integration

```python
from cowrie_gnn import load_compressed
from cowrie_gnn.adapters import to_dgl

dataset = load_compressed("cora.sjgn")
g = to_dgl(dataset)

# g.ndata['x'] -> features
# g.ndata['y'] -> labels
# g.ndata['train_mask'], g.ndata['val_mask'], g.ndata['test_mask']
```

## Creating Datasets

```python
from cowrie_gnn import ContainerWriter, save_compressed
import numpy as np

# Create container
writer = ContainerWriter("my-dataset")
writer.set_directed(True)
writer.add_node_type("paper", num_nodes=2708)

# Add features (NumPy arrays)
features = np.random.randn(2708, 1433).astype(np.float32)
writer.add_feature("x", features)

# Add labels
labels = np.random.randint(0, 7, size=(2708, 1)).astype(np.int64)
writer.add_feature("y", labels)

# Add edges (COO format -> auto-converted to CSR)
src = np.array([0, 0, 1, 2, ...], dtype=np.int64)
dst = np.array([1, 2, 0, 3, ...], dtype=np.int64)
writer.add_coo_edges(src, dst, num_nodes=2708)

# Add train/val/test split
train_idx = np.arange(0, 1624)
val_idx = np.arange(1624, 2166)
test_idx = np.arange(2166, 2708)
writer.add_split(train_idx, val_idx, test_idx)

# Save (with zstd compression)
save_compressed(writer, "my-dataset.sjgz")
```

## Performance

Comparison on Cora-like dataset (2708 nodes, 5429 edges, 1433-dim features):

| Format | Size | Load Time |
|--------|------|-----------|
| JSON | 43.5 MB | ~400ms |
| JSON + gzip | 17.6 MB | ~500ms |
| JSON + zstd | 16.8 MB | ~300ms |
| **Cowrie-GNN + zstd** | **6.4 MB** | **~3ms** |

**2.6x smaller** than JSON+zstd, **100x faster** to load.

## Wire Format

```
Magic: 'SJGN' (4 bytes) or 'SJGZ' (compressed)
Version: 0x01
Flags: bitfield (heterogeneous, temporal, hasCSR)
SectionCount: uvarint
Sections: [Kind, Name, BodyLen, Body]...
```

Section kinds: Meta, NodeTable, EdgeTable, Feature, Split, Aux

## API Reference

### Reading

- `load(path)` - Load uncompressed .sjgn file
- `load_compressed(path)` - Load .sjgn or .sjgz file (auto-detect)
- `Container` - Main container class with sections
- `FeatureReader` - Read feature tensors
- `SplitReader` - Read train/val/test splits
- `AuxReader` - Read CSR/CSC indices

### Writing

- `ContainerWriter` - Build containers
- `FeatureWriter` - Write feature tensors
- `SplitWriter` - Write splits
- `AuxWriter` - Write CSR/CSC
- `save(writer, path)` - Save uncompressed
- `save_compressed(writer, path)` - Save with zstd

### Adapters

- `to_numpy(container)` - Convert to NumPy dict
- `to_torch(container, device=None)` - Convert to PyTorch tensors
- `to_pyg(container, device=None)` - Convert to PyG Data
- `to_dgl(container, device=None)` - Convert to DGL graph

## License

MIT
