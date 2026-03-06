// Package onnx provides ONNX-based GNN inference for node classification and link prediction.
//
// Tier 2 in the three-tier architecture:
//
//	Tier 1 (Pure Go, <5ms):  PageRank, BFS/DFS, Louvain - always available
//	Tier 2 (ONNX, <20ms):    GCN inference, node classification - requires ONNX model
//	Tier 3 (Python, <100ms): Training, fine-tuning - external service
//
// Build with -tags onnx to enable ONNX runtime.
package onnx

import (
	"github.com/Neumenon/cowrie/go/gnn/algo"
)

// GCNConfig configures the GCN inference model.
type GCNConfig struct {
	ModelPath  string  // Path to ONNX model file
	NumLayers  int     // Number of GCN layers (default: 2)
	HiddenDim  int     // Hidden dimension (default: 64)
	NumClasses int     // Number of output classes
	Dropout    float32 // Dropout rate (default: 0.5, only for training)
}

// DefaultGCNConfig returns sensible defaults.
func DefaultGCNConfig() GCNConfig {
	return GCNConfig{
		NumLayers:  2,
		HiddenDim:  64,
		NumClasses: 7, // Common for Cora-like datasets
		Dropout:    0.5,
	}
}

// NodeClassificationResult contains node classification results.
type NodeClassificationResult struct {
	// Predictions contains the predicted class for each node.
	Predictions []int

	// Probabilities contains softmax probabilities [numNodes][numClasses].
	// May be nil if not requested.
	Probabilities [][]float32

	// Confidence is the max probability for each prediction.
	Confidence []float32
}

// LinkPredictionResult contains link prediction results.
type LinkPredictionResult struct {
	// Scores contains the link probability for each queried pair.
	Scores []float32

	// SourceNodes and TargetNodes are the queried node pairs.
	SourceNodes []int64
	TargetNodes []int64
}

// NodeEmbeddingResult contains learned node embeddings.
type NodeEmbeddingResult struct {
	// Embeddings contains the learned representations [numNodes][embeddingDim].
	Embeddings [][]float32

	// Dim is the embedding dimension.
	Dim int
}

// GCNInference defines the interface for GCN-based inference.
type GCNInference interface {
	// ClassifyNodes performs node classification.
	// features: [numNodes][featureDim] input features
	// csr: graph structure in CSR format
	// Returns predicted class for each node.
	ClassifyNodes(features [][]float32, csr *algo.CSR) (*NodeClassificationResult, error)

	// ClassifyNodesBatch classifies specific nodes (more efficient for large graphs).
	ClassifyNodesBatch(features [][]float32, csr *algo.CSR, nodeIDs []int64) (*NodeClassificationResult, error)

	// PredictLinks scores potential edges between node pairs.
	PredictLinks(features [][]float32, csr *algo.CSR, srcNodes, dstNodes []int64) (*LinkPredictionResult, error)

	// GetEmbeddings extracts learned node embeddings (from GCN hidden layer).
	GetEmbeddings(features [][]float32, csr *algo.CSR) (*NodeEmbeddingResult, error)

	// Close releases ONNX resources.
	Close() error
}

// SupportedModels lists known pre-trained GCN models.
var SupportedModels = map[string]GCNConfig{
	// Cora dataset (citation network)
	"cora-gcn-2layer": {
		NumLayers:  2,
		HiddenDim:  16,
		NumClasses: 7, // 7 paper categories
	},

	// CiteSeer dataset
	"citeseer-gcn-2layer": {
		NumLayers:  2,
		HiddenDim:  16,
		NumClasses: 6,
	},

	// PubMed dataset
	"pubmed-gcn-2layer": {
		NumLayers:  2,
		HiddenDim:  16,
		NumClasses: 3,
	},

	// Agent topology (custom)
	"agent-gcn-2layer": {
		NumLayers:  2,
		HiddenDim:  64,
		NumClasses: 4, // e.g., coordinator, worker, tool, observer
	},
}

// FeatureNormalization options for input features.
type FeatureNormalization int

const (
	NormNone      FeatureNormalization = iota
	NormRow                            // L2 normalize each row (node)
	NormSymmetric                      // Symmetric normalization (D^-0.5 * A * D^-0.5)
)

// PrepareFeatures normalizes features for GCN input.
func PrepareFeatures(features [][]float32, norm FeatureNormalization) [][]float32 {
	if norm == NormNone || len(features) == 0 {
		return features
	}

	result := make([][]float32, len(features))
	for i, row := range features {
		result[i] = make([]float32, len(row))
		copy(result[i], row)
	}

	switch norm {
	case NormRow:
		// L2 normalize each row
		for i := range result {
			var sum float32
			for _, v := range result[i] {
				sum += v * v
			}
			if sum > 0 {
				invNorm := 1.0 / sqrt32(sum)
				for j := range result[i] {
					result[i][j] *= invNorm
				}
			}
		}
	}

	return result
}

func sqrt32(x float32) float32 {
	// Fast inverse square root approximation
	if x <= 0 {
		return 0
	}
	// Use standard library via float64 conversion
	return float32(sqrtFloat64(float64(x)))
}

func sqrtFloat64(x float64) float64 {
	// Newton-Raphson
	z := x / 2
	for i := 0; i < 10; i++ {
		z -= (z*z - x) / (2 * z)
	}
	return z
}
