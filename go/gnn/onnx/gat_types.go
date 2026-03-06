// Package onnx provides ONNX-based GNN inference.
// This file defines GAT (Graph Attention Network) types and interface.
package onnx

import (
	"github.com/Neumenon/cowrie/go/gnn/algo"
)

// GATConfig configures the GAT inference model.
type GATConfig struct {
	ModelPath   string  // Path to ONNX model file
	NumLayers   int     // Number of GAT layers (default: 2)
	NumHeads    int     // Number of attention heads (default: 8)
	HiddenDim   int     // Hidden dimension per head (default: 8)
	NumClasses  int     // Number of output classes
	Dropout     float32 // Dropout rate (default: 0.6)
	AttnDropout float32 // Attention dropout rate (default: 0.6)
	Negative    float32 // LeakyReLU negative slope (default: 0.2)
}

// DefaultGATConfig returns sensible defaults matching PyTorch Geometric GAT.
func DefaultGATConfig() GATConfig {
	return GATConfig{
		NumLayers:   2,
		NumHeads:    8,
		HiddenDim:   8, // 8 heads * 8 dim = 64 total
		NumClasses:  7,
		Dropout:     0.6,
		AttnDropout: 0.6,
		Negative:    0.2,
	}
}

// GATInference defines the interface for GAT-based inference.
type GATInference interface {
	// ClassifyNodes performs node classification with attention.
	ClassifyNodes(features [][]float32, csr *algo.CSR) (*NodeClassificationResult, error)

	// ClassifyNodesBatch classifies specific nodes.
	ClassifyNodesBatch(features [][]float32, csr *algo.CSR, nodeIDs []int64) (*NodeClassificationResult, error)

	// GetAttentionWeights returns attention weights for edges.
	// Useful for interpretability - see which neighbors are most important.
	GetAttentionWeights(features [][]float32, csr *algo.CSR) (*AttentionResult, error)

	// GetEmbeddings extracts learned node embeddings.
	GetEmbeddings(features [][]float32, csr *algo.CSR) (*NodeEmbeddingResult, error)

	// Close releases ONNX resources.
	Close() error
}

// AttentionResult contains edge attention weights.
type AttentionResult struct {
	// Weights contains attention weights for each edge [numEdges][numHeads].
	// Higher weight = more important neighbor.
	Weights [][]float32

	// SourceNodes and TargetNodes define the edges.
	SourceNodes []int64
	TargetNodes []int64

	// NumHeads is the number of attention heads.
	NumHeads int
}

// TopAttentionEdges returns the top-k edges with highest attention (averaged across heads).
func (ar *AttentionResult) TopAttentionEdges(k int) []AttentionEdge {
	if ar == nil || len(ar.Weights) == 0 {
		return nil
	}

	// Compute average attention per edge
	edges := make([]AttentionEdge, len(ar.Weights))
	for i := range ar.Weights {
		var avg float32
		for _, w := range ar.Weights[i] {
			avg += w
		}
		avg /= float32(len(ar.Weights[i]))
		edges[i] = AttentionEdge{
			Source:    ar.SourceNodes[i],
			Target:    ar.TargetNodes[i],
			Attention: avg,
		}
	}

	// Sort by attention (descending) - simple bubble sort for small k
	for i := 0; i < k && i < len(edges); i++ {
		maxIdx := i
		for j := i + 1; j < len(edges); j++ {
			if edges[j].Attention > edges[maxIdx].Attention {
				maxIdx = j
			}
		}
		edges[i], edges[maxIdx] = edges[maxIdx], edges[i]
	}

	if k > len(edges) {
		k = len(edges)
	}
	return edges[:k]
}

// AttentionEdge represents an edge with its attention weight.
type AttentionEdge struct {
	Source    int64
	Target    int64
	Attention float32
}

// SupportedGATModels lists known pre-trained GAT models.
var SupportedGATModels = map[string]GATConfig{
	// Cora dataset (citation network)
	"cora-gat-2layer": {
		NumLayers:  2,
		NumHeads:   8,
		HiddenDim:  8,
		NumClasses: 7,
	},

	// CiteSeer dataset
	"citeseer-gat-2layer": {
		NumLayers:  2,
		NumHeads:   8,
		HiddenDim:  8,
		NumClasses: 6,
	},

	// Agent topology (custom) - multi-head attention for complex relationships
	"agent-gat-2layer": {
		NumLayers:  2,
		NumHeads:   4, // Fewer heads for smaller graphs
		HiddenDim:  16,
		NumClasses: 4,
	},
}
