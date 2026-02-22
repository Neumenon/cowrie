"""
Cowrie Python Ecosystem - Binary codecs for JSON, Graph, and LD data.

This package includes:
  - cowrie: Cowrie v2 core - Binary JSON++ with 13 extended types
  - cowrie.graph: GraphCowrie-Stream - Streaming graph event format
  - cowrie.ld: Cowrie-LD - Binary JSON-LD codec
  - cowrie_gnn: GNN dataset container with PyTorch/PyG adapters

Installation:
    pip install cowrie-gnn

    # With compression support:
    pip install cowrie-gnn[compression]

    # With PyTorch support:
    pip install cowrie-gnn[torch]

    # With PyTorch Geometric support:
    pip install cowrie-gnn[pyg]

    # All extras:
    pip install cowrie-gnn[all]
"""

from setuptools import setup, find_packages

setup(
    name="cowrie-gnn",
    version="0.1.0",
    description="Cowrie binary codecs: JSON++, GraphStream, JSON-LD, and GNN datasets",
    long_description=open("README.md").read() if __import__("os").path.exists("README.md") else "",
    long_description_content_type="text/markdown",
    author="AgentScope-Go Team",
    url="https://github.com/your-org/agentscope-go",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "numpy>=1.20.0",
    ],
    extras_require={
        "compression": ["zstandard>=0.18.0"],
        "torch": ["torch>=1.9.0"],
        "pyg": ["torch>=1.9.0", "torch_geometric>=2.0.0"],
        "dgl": ["torch>=1.9.0", "dgl>=0.9.0"],
        "all": [
            "zstandard>=0.18.0",
            "torch>=1.9.0",
            "torch_geometric>=2.0.0",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: File Formats :: JSON",
        "Topic :: Database :: Database Engines/Servers",
    ],
    keywords="cowrie json binary graph rdf json-ld gnn neural network pytorch pyg",
)
