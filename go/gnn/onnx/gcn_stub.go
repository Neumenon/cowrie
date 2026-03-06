//go:build !onnx

package onnx

import (
	"fmt"

	"github.com/Neumenon/cowrie/go/gnn/algo"
)

// GCN is a placeholder when built without the "onnx" tag.
// Use pure Go algorithms from cowrie/gnn/algo instead.
type GCN struct{}

// NewGCN returns an error when ONNX is not enabled.
func NewGCN(cfg GCNConfig) (GCNInference, error) {
	return nil, fmt.Errorf("onnx GCN not enabled (build with -tags onnx); use algo.PageRank for importance ranking")
}

// LoadGCN returns an error when ONNX is not enabled.
func LoadGCN(modelPath string) (GCNInference, error) {
	return nil, fmt.Errorf("onnx GCN not enabled (build with -tags onnx)")
}

// ClassifyNodes is not available without ONNX.
func (g *GCN) ClassifyNodes(features [][]float32, csr *algo.CSR) (*NodeClassificationResult, error) {
	return nil, fmt.Errorf("onnx GCN not enabled")
}

// ClassifyNodesBatch is not available without ONNX.
func (g *GCN) ClassifyNodesBatch(features [][]float32, csr *algo.CSR, nodeIDs []int64) (*NodeClassificationResult, error) {
	return nil, fmt.Errorf("onnx GCN not enabled")
}

// PredictLinks is not available without ONNX.
func (g *GCN) PredictLinks(features [][]float32, csr *algo.CSR, srcNodes, dstNodes []int64) (*LinkPredictionResult, error) {
	return nil, fmt.Errorf("onnx GCN not enabled")
}

// GetEmbeddings is not available without ONNX.
func (g *GCN) GetEmbeddings(features [][]float32, csr *algo.CSR) (*NodeEmbeddingResult, error) {
	return nil, fmt.Errorf("onnx GCN not enabled")
}

// Close is a no-op for the stub.
func (g *GCN) Close() error {
	return nil
}

// IsONNXEnabled returns false when built without ONNX support.
func IsONNXEnabled() bool {
	return false
}

// FallbackClassify uses PageRank + degree for simple "importance" classification.
// This provides a pure Go fallback when ONNX is not available.
func FallbackClassify(csr *algo.CSR, numClasses int) (*NodeClassificationResult, error) {
	if csr == nil || csr.NumNodes == 0 {
		return nil, fmt.Errorf("empty graph")
	}

	// Use PageRank as a proxy for "importance"
	prResult := algo.PageRank(csr, algo.DefaultPageRankConfig)
	if prResult == nil {
		return nil, fmt.Errorf("pagerank failed")
	}

	// Classify nodes into buckets based on PageRank score
	predictions := make([]int, csr.NumNodes)
	confidence := make([]float32, csr.NumNodes)

	// Find min/max for normalization
	var minPR, maxPR float32 = prResult.Scores[0], prResult.Scores[0]
	for _, s := range prResult.Scores {
		if s < minPR {
			minPR = s
		}
		if s > maxPR {
			maxPR = s
		}
	}

	prRange := maxPR - minPR
	if prRange == 0 {
		prRange = 1
	}

	// Assign classes based on PageRank quantiles
	for i, score := range prResult.Scores {
		normalized := (score - minPR) / prRange
		classIdx := int(normalized * float32(numClasses-1))
		if classIdx >= numClasses {
			classIdx = numClasses - 1
		}
		predictions[i] = classIdx
		confidence[i] = normalized
	}

	return &NodeClassificationResult{
		Predictions: predictions,
		Confidence:  confidence,
	}, nil
}
