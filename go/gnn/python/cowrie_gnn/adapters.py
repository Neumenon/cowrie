"""
Cowrie-GNN Adapters - Convert to NumPy, PyTorch, and PyTorch Geometric.

Usage:
    from cowrie_gnn import load, to_torch, to_pyg

    # Load dataset
    container = load("cora.sjgn")

    # Get as NumPy dict
    data = to_numpy(container)
    # {'x': np.ndarray, 'y': np.ndarray, 'edge_index': (src, dst), ...}

    # Get as PyTorch tensors
    data = to_torch(container)
    # {'x': torch.Tensor, 'y': torch.Tensor, 'edge_index': torch.Tensor, ...}

    # Get as PyTorch Geometric Data object
    pyg_data = to_pyg(container)
    # Data(x=[2708, 1433], edge_index=[2, 10556], y=[2708], ...)
"""

from typing import Dict, Any, Tuple, Optional
import numpy as np

from .reader import Container


def to_numpy(container: Container) -> Dict[str, Any]:
    """
    Convert Cowrie-GNN container to NumPy arrays.

    Returns:
        Dict with:
        - Feature names → np.ndarray
        - 'edge_index' → tuple of (src, dst) arrays
        - 'train_idx', 'val_idx', 'test_idx' → np.ndarray
        - 'num_nodes', 'num_edges' → int
    """
    result = {}

    # Features
    for name, arr in container.features.items():
        result[name] = arr

    # Edge index
    if container.aux:
        src, dst = container.edge_index()
        result['edge_index'] = (src, dst)
        result['num_edges'] = len(src)

    # Split
    if container.split:
        result['train_idx'] = container.split.train
        result['val_idx'] = container.split.val
        result['test_idx'] = container.split.test

    # Num nodes
    try:
        result['num_nodes'] = container.num_nodes()
    except ValueError:
        pass

    # Metadata
    if container.meta:
        result['_meta'] = {
            'dataset_name': container.meta.dataset_name,
            'directed': container.meta.directed,
            'node_types': container.meta.node_types,
            'edge_types': container.meta.edge_types,
        }

    return result


def to_torch(container: Container, device: Optional[str] = None) -> Dict[str, Any]:
    """
    Convert Cowrie-GNN container to PyTorch tensors.

    Args:
        container: Cowrie-GNN container
        device: Optional device ('cpu', 'cuda', 'cuda:0', etc.)

    Returns:
        Dict with PyTorch tensors
    """
    try:
        import torch
    except ImportError:
        raise ImportError("PyTorch required. Install with: pip install torch")

    numpy_data = to_numpy(container)
    result = {}

    for key, value in numpy_data.items():
        if key.startswith('_'):
            # Keep metadata as-is
            result[key] = value
        elif isinstance(value, np.ndarray):
            tensor = torch.from_numpy(value)
            if device:
                tensor = tensor.to(device)
            result[key] = tensor
        elif isinstance(value, tuple) and key == 'edge_index':
            # Stack edge_index to [2, E] format
            src, dst = value
            edge_index = torch.stack([
                torch.from_numpy(src),
                torch.from_numpy(dst)
            ], dim=0)
            if device:
                edge_index = edge_index.to(device)
            result['edge_index'] = edge_index
        elif isinstance(value, (int, float)):
            result[key] = value

    return result


def to_pyg(container: Container, device: Optional[str] = None):
    """
    Convert Cowrie-GNN container to PyTorch Geometric Data object.

    Args:
        container: Cowrie-GNN container
        device: Optional device

    Returns:
        torch_geometric.data.Data object

    Example:
        data = to_pyg(container)
        # Data(x=[2708, 1433], edge_index=[2, 10556], y=[2708],
        #      train_mask=[2708], val_mask=[2708], test_mask=[2708])
    """
    try:
        import torch
        from torch_geometric.data import Data
    except ImportError:
        raise ImportError(
            "PyTorch Geometric required. Install with:\n"
            "  pip install torch torch_geometric"
        )

    torch_data = to_torch(container, device=device)

    # Build Data object
    data_kwargs = {}

    # Features (x, y, etc.)
    for key, value in torch_data.items():
        if key.startswith('_'):
            continue
        if isinstance(value, torch.Tensor):
            if key in ['x', 'y', 'edge_index', 'edge_attr', 'pos']:
                data_kwargs[key] = value
            elif key.endswith('_idx'):
                # Convert indices to masks
                pass  # Handle below
            else:
                data_kwargs[key] = value

    # Edge index
    if 'edge_index' in torch_data:
        data_kwargs['edge_index'] = torch_data['edge_index']

    # Convert split indices to masks (PyG convention)
    num_nodes = torch_data.get('num_nodes', 0)
    if num_nodes and container.split:
        train_mask = torch.zeros(num_nodes, dtype=torch.bool)
        val_mask = torch.zeros(num_nodes, dtype=torch.bool)
        test_mask = torch.zeros(num_nodes, dtype=torch.bool)

        train_mask[torch_data['train_idx'].long()] = True
        val_mask[torch_data['val_idx'].long()] = True
        test_mask[torch_data['test_idx'].long()] = True

        if device:
            train_mask = train_mask.to(device)
            val_mask = val_mask.to(device)
            test_mask = test_mask.to(device)

        data_kwargs['train_mask'] = train_mask
        data_kwargs['val_mask'] = val_mask
        data_kwargs['test_mask'] = test_mask

    # Create Data object
    data = Data(**data_kwargs)

    # Add metadata as attributes
    if container.meta:
        data.dataset_name = container.meta.dataset_name
        data.directed = container.meta.directed

    return data


def to_dgl(container: Container, device: Optional[str] = None):
    """
    Convert Cowrie-GNN container to DGL graph.

    Args:
        container: Cowrie-GNN container
        device: Optional device

    Returns:
        dgl.DGLGraph object
    """
    try:
        import dgl
        import torch
    except ImportError:
        raise ImportError(
            "DGL required. Install with:\n"
            "  pip install dgl torch"
        )

    # Get edge index
    src, dst = container.edge_index()

    # Create DGL graph
    g = dgl.graph((src, dst))

    # Add node features
    for name, arr in container.features.items():
        tensor = torch.from_numpy(arr)
        if device:
            tensor = tensor.to(device)
        g.ndata[name] = tensor

    # Add split masks
    if container.split:
        num_nodes = container.num_nodes()
        train_mask, val_mask, test_mask = container.split.get_masks(num_nodes)

        g.ndata['train_mask'] = torch.from_numpy(train_mask)
        g.ndata['val_mask'] = torch.from_numpy(val_mask)
        g.ndata['test_mask'] = torch.from_numpy(test_mask)

    if device:
        g = g.to(device)

    return g


# === Helper for heterogeneous graphs ===

def to_pyg_hetero(container: Container, device: Optional[str] = None):
    """
    Convert heterogeneous Cowrie-GNN container to PyTorch Geometric HeteroData.

    Args:
        container: Cowrie-GNN container with heterogeneous=True

    Returns:
        torch_geometric.data.HeteroData object
    """
    try:
        import torch
        from torch_geometric.data import HeteroData
    except ImportError:
        raise ImportError(
            "PyTorch Geometric required. Install with:\n"
            "  pip install torch torch_geometric"
        )

    if not container.is_heterogeneous:
        raise ValueError("Container is not heterogeneous. Use to_pyg() instead.")

    data = HeteroData()

    # Add node features by type
    if container.meta:
        for node_type in container.meta.node_types:
            for key, spec in container.meta.features.items():
                if key.startswith(f"{node_type}:"):
                    feat_name = key.split(":")[-1]
                    section = container.get_section(f"feature:{key}")
                    if section:
                        from .reader import FeatureReader
                        fr = FeatureReader(section.body)
                        arr = fr.read_tensor()
                        tensor = torch.from_numpy(arr)
                        if device:
                            tensor = tensor.to(device)
                        data[node_type][feat_name] = tensor

    # Add edge indices by type
    # (Would need edge table sections parsed by edge type)

    return data


# === Batch utilities ===

class CowrieGNNDataset:
    """
    PyTorch Dataset wrapper for Cowrie-GNN files.

    Usage:
        dataset = CowrieGNNDataset(["train.sjgn", "val.sjgn", "test.sjgn"])
        loader = DataLoader(dataset, batch_size=1)

        for data in loader:
            # data is PyG Data object
            out = model(data.x, data.edge_index)
    """

    def __init__(self, paths, transform=None):
        """
        Args:
            paths: List of .sjgn file paths
            transform: Optional transform function
        """
        try:
            import torch
            from torch.utils.data import Dataset
        except ImportError:
            raise ImportError("PyTorch required")

        self.paths = paths
        self.transform = transform

    def __len__(self):
        return len(self.paths)

    def __getitem__(self, idx):
        from .reader import load_compressed
        container = load_compressed(self.paths[idx])
        data = to_pyg(container)

        if self.transform:
            data = self.transform(data)

        return data
