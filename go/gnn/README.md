# Cowrie-GNN - Binary Graph Neural Network Container

A binary container format for Graph Neural Network datasets, providing compact storage for heterogeneous graphs, feature tensors, train/val/test splits, and CSR indices.

## Why Cowrie-GNN?

| Problem | Cowrie-GNN Solution |
|---------|-------------------|
| JSON/CSV overhead | Binary section format |
| Separate feature files | Single container with sections |
| No split info | Built-in train/val/test splits |
| Slow graph loading | Pre-computed CSR indices |
| Large embedding files | Float16 support + byte shuffling |

## Quick Start

```go
import "agentscope/cowrie/gnn"

// Create a container
c := gnn.NewContainer("cora")
c.SetDirected(true)
c.AddNodeType("paper", 2708)
c.AddEdgeType("paper", "cites", "paper")
c.AddFeature("paper", "x", gnn.DTypeFloat32, []int{1433})
c.AddLabel("paper", "y", gnn.DTypeInt64, []int{1})

// Add sections
c.AddSection(gnn.SectionFeature, "features:x", featureData)
c.AddSection(gnn.SectionSplit, "split:train_val_test", splitData)

// Encode
data, _ := c.Encode()

// Encode with compression (2-3x smaller)
compressed, _ := c.EncodeCompressed()

// Decode (auto-detects compression)
decoded, _ := gnn.DecodeCompressed(data)
```

## Wire Format

### Container Header

```
Magic:     'S' 'J' 'G' 'N'  (4 bytes)
Version:   0x01              (1 byte)
Flags:     bitfield          (1 byte)
           bit0: heterogeneous (multiple node/edge types)
           bit1: temporal (timestamps on events)
           bit2: has CSR indices
           bit3: zstd compressed

SectionCount: uvarint
Sections:     Section...
```

### Compressed Container Header

When compressed with `EncodeCompressed()`:

```
Magic:          'S' 'J' 'G' 'Z'  (4 bytes)
OriginalSize:   u32 LE           (4 bytes)
CompressedData: zstd bytes...
```

### Section Format

Each section:

```
Kind:    uvarint
         0 = Meta (dataset metadata as JSON)
         1 = NodeTable (node records)
         2 = EdgeTable (edge records)
         3 = Feature (tensor data)
         4 = Split (train/val/test indices)
         5 = Aux (CSR/CSC indices)

NameLen: uvarint
Name:    utf8 bytes (e.g., "nodes:paper", "features:x")
BodyLen: u64 LE
Body:    bytes
```

## Section Types

### Meta Section (Kind 0)

JSON-encoded dataset metadata:

```json
{
  "dataset_name": "cora",
  "version": "1.0",
  "directed": true,
  "heterogeneous": false,
  "temporal": false,
  "node_types": ["paper"],
  "edge_types": [
    {"src_type": "paper", "edge_type": "cites", "dst_type": "paper"}
  ],
  "id_spaces": {
    "paper": {"start": 0, "count": 2708}
  },
  "features": {
    "paper:x": {"shape": [1433], "dtype": "float32"}
  },
  "labels": {
    "paper:y": {"shape": [1], "dtype": "int64"}
  }
}
```

### Feature Section (Kind 3)

```
FeatureHeader:
  NameLen: uvarint
  Name:    utf8 (e.g., "x")
  Mode:    1 byte (0=row-wise, 1=blocked)
  DType:   1 byte
  Rank:    uvarint
  Shape:   uvarint × Rank

TensorData: raw bytes (row-major order)
```

### Split Section (Kind 4)

```
Mode: 1 byte (0=indices, 1=mask)

If indices mode:
  TrainCount: uvarint
  TrainIdx:   int64 × TrainCount
  ValCount:   uvarint
  ValIdx:     int64 × ValCount
  TestCount:  uvarint
  TestIdx:    int64 × TestCount

If mask mode:
  NodeCount: uvarint
  TrainMask: ceil(NodeCount/8) bytes
  ValMask:   ceil(NodeCount/8) bytes
  TestMask:  ceil(NodeCount/8) bytes
```

### Aux Section (Kind 5)

CSR/CSC indices for fast graph operations:

```
Format:   1 byte (0=CSR, 1=CSC)
NumNodes: u64 LE
IndPtr:   (NumNodes+1) × u64 LE
Indices:  NumEdges × u64 LE
```

## Data Types

| DType | Value | Size | Description |
|-------|-------|------|-------------|
| Float32 | 0 | 4 | 32-bit IEEE 754 |
| Float64 | 1 | 8 | 64-bit IEEE 754 |
| Int32 | 2 | 4 | Signed 32-bit |
| Int64 | 3 | 8 | Signed 64-bit |
| Uint32 | 4 | 4 | Unsigned 32-bit |
| Uint64 | 5 | 8 | Unsigned 64-bit |
| Bool | 6 | 1 | Boolean |
| Float16 | 7 | 2 | Half-precision |

## Optimization Techniques

### Float16 Conversion

Reduce embedding storage by 50% with minimal precision loss:

```go
// Convert float32 to float16
f16Data := gnn.Float32ToFloat16(embeddings)

// Convert back
f32Data := gnn.Float16ToFloat32(f16Data)
```

### Byte Shuffling

Improve zstd compression 2-10x for numeric data:

```go
// Group similar bytes together
shuffled := gnn.ByteShuffle(data, 4)  // 4 bytes per float32

// Reverse after decompression
original := gnn.ByteUnshuffle(shuffled, 4)
```

### Combined Optimization

For best compression:

```go
// 1. Convert to float16
f16 := gnn.Float32ToFloat16(features)

// 2. Byte shuffle
shuffled := gnn.ByteShuffle(f16, 2)  // 2 bytes per float16

// 3. Compress
// (handled by EncodeCompressed)
```

## File Operations

```go
// Save to file
container.Save("dataset.sjgn")

// Load from file
loaded, _ := gnn.Load("dataset.sjgn")

// Stream to io.Writer
container.EncodeTo(writer)

// Stream from io.Reader
decoded, _ := gnn.DecodeFrom(reader)
```

## Size Comparison

Typical compression ratios for GNN datasets:

| Dataset | JSON | SJGN | SJGN+zstd |
|---------|------|------|-----------|
| Cora | 100% | 35% | 8% |
| CiteSeer | 100% | 38% | 9% |
| OGB-Products | 100% | 32% | 6% |

## Heterogeneous Graphs

```go
c := gnn.NewContainer("mag")
c.SetHeterogeneous(true)

// Multiple node types
c.AddNodeType("paper", 736389)
c.AddNodeType("author", 1134649)
c.AddNodeType("institution", 8740)

// Multiple edge types
c.AddEdgeType("author", "writes", "paper")
c.AddEdgeType("paper", "cites", "paper")
c.AddEdgeType("author", "affiliated_with", "institution")
```

## Temporal Graphs

```go
c := gnn.NewContainer("reddit")
c.SetTemporal(true)

// Edge timestamps stored in EdgeTable sections
// Use for dynamic graph learning
```

## Building

```bash
go test ./cowrie/gnn/...
```

## License

MIT
