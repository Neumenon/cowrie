//go:build !onnx

package onnx

import (
	"fmt"

	"github.com/Neumenon/cowrie/go/gnn/algo"
)

// GAT is a placeholder when built without the "onnx" tag.
type GAT struct{}

// NewGAT returns an error when ONNX is not enabled.
func NewGAT(cfg GATConfig) (GATInference, error) {
	return nil, fmt.Errorf("onnx GAT not enabled (build with -tags onnx); use FallbackGATClassify for heuristic approach")
}

// LoadGAT returns an error when ONNX is not enabled.
func LoadGAT(modelPath string) (GATInference, error) {
	return nil, fmt.Errorf("onnx GAT not enabled (build with -tags onnx)")
}

// ClassifyNodes is not available without ONNX.
func (g *GAT) ClassifyNodes(features [][]float32, csr *algo.CSR) (*NodeClassificationResult, error) {
	return nil, fmt.Errorf("onnx GAT not enabled")
}

// ClassifyNodesBatch is not available without ONNX.
func (g *GAT) ClassifyNodesBatch(features [][]float32, csr *algo.CSR, nodeIDs []int64) (*NodeClassificationResult, error) {
	return nil, fmt.Errorf("onnx GAT not enabled")
}

// GetAttentionWeights is not available without ONNX.
func (g *GAT) GetAttentionWeights(features [][]float32, csr *algo.CSR) (*AttentionResult, error) {
	return nil, fmt.Errorf("onnx GAT not enabled")
}

// GetEmbeddings is not available without ONNX.
func (g *GAT) GetEmbeddings(features [][]float32, csr *algo.CSR) (*NodeEmbeddingResult, error) {
	return nil, fmt.Errorf("onnx GAT not enabled")
}

// Close is a no-op for the stub.
func (g *GAT) Close() error {
	return nil
}

// IsGATEnabled returns false when built without ONNX support.
func IsGATEnabled() bool {
	return false
}

// FallbackGATClassify uses PageRank + neighbor similarity for attention-like classification.
// This provides a pure Go fallback that mimics attention behavior.
func FallbackGATClassify(features [][]float32, csr *algo.CSR, numClasses int) (*NodeClassificationResult, error) {
	if csr == nil || csr.NumNodes == 0 {
		return nil, fmt.Errorf("empty graph")
	}
	if len(features) != int(csr.NumNodes) {
		return nil, fmt.Errorf("features length (%d) != nodes (%d)", len(features), csr.NumNodes)
	}

	// Step 1: Compute PageRank for base importance
	prResult := algo.PageRank(csr, algo.DefaultPageRankConfig)
	if prResult == nil {
		return nil, fmt.Errorf("pagerank failed")
	}

	// Step 2: Compute "attention-weighted" features using neighbor similarity
	// For each node, weight neighbors by feature cosine similarity
	numNodes := int(csr.NumNodes)
	featureDim := len(features[0])

	aggregated := make([][]float32, numNodes)
	for i := range aggregated {
		aggregated[i] = make([]float32, featureDim)
	}

	for node := int64(0); node < csr.NumNodes; node++ {
		nodeFeats := features[node]

		// Get neighbors
		start := csr.IndPtr[node]
		end := csr.IndPtr[node+1]

		if start == end {
			// No neighbors, use own features
			copy(aggregated[node], nodeFeats)
			continue
		}

		// Compute attention weights (cosine similarity)
		weights := make([]float32, end-start)
		var weightSum float32
		for i := start; i < end; i++ {
			neighbor := csr.Indices[i]
			weights[i-start] = cosineSimilarity(nodeFeats, features[neighbor])
			if weights[i-start] < 0 {
				weights[i-start] = 0 // ReLU-like
			}
			weightSum += weights[i-start]
		}

		// Normalize weights
		if weightSum > 0 {
			for i := range weights {
				weights[i] /= weightSum
			}
		} else {
			// Uniform weights
			uniform := 1.0 / float32(len(weights))
			for i := range weights {
				weights[i] = uniform
			}
		}

		// Aggregate neighbor features
		for i := start; i < end; i++ {
			neighbor := csr.Indices[i]
			w := weights[i-start]
			for j := range features[neighbor] {
				aggregated[node][j] += w * features[neighbor][j]
			}
		}

		// Add self-loop contribution
		for j := range nodeFeats {
			aggregated[node][j] = 0.5*aggregated[node][j] + 0.5*nodeFeats[j]
		}
	}

	// Step 3: Classify based on aggregated features + PageRank
	predictions := make([]int, numNodes)
	confidence := make([]float32, numNodes)

	// Simple classification: hash features into classes weighted by PageRank
	for i := 0; i < numNodes; i++ {
		// Sum features as simple hash
		var featureSum float32
		for _, f := range aggregated[i] {
			featureSum += f
		}

		// Combine with PageRank
		score := 0.7*featureSum + 0.3*prResult.Scores[i]*100

		// Map to class
		classIdx := int(abs32(score)*1000) % numClasses
		predictions[i] = classIdx
		confidence[i] = prResult.Scores[i] // Use PageRank as confidence proxy
	}

	return &NodeClassificationResult{
		Predictions: predictions,
		Confidence:  confidence,
	}, nil
}

// FallbackAttention computes approximate attention weights using feature similarity.
func FallbackAttention(features [][]float32, csr *algo.CSR) (*AttentionResult, error) {
	if csr == nil || csr.NumNodes == 0 {
		return nil, fmt.Errorf("empty graph")
	}

	numEdges := int(csr.NumEdges)
	weights := make([][]float32, numEdges)
	sourceNodes := make([]int64, numEdges)
	targetNodes := make([]int64, numEdges)

	edgeIdx := 0
	for node := int64(0); node < csr.NumNodes; node++ {
		start := csr.IndPtr[node]
		end := csr.IndPtr[node+1]
		nodeFeats := features[node]

		for i := start; i < end; i++ {
			neighbor := csr.Indices[i]
			sim := cosineSimilarity(nodeFeats, features[neighbor])
			if sim < 0 {
				sim = 0
			}

			// Simulate 4 heads with slightly different weights
			weights[edgeIdx] = []float32{
				sim,
				sim * 0.9,
				sim * 1.1,
				sim * 0.95,
			}
			sourceNodes[edgeIdx] = node
			targetNodes[edgeIdx] = neighbor
			edgeIdx++
		}
	}

	return &AttentionResult{
		Weights:     weights,
		SourceNodes: sourceNodes,
		TargetNodes: targetNodes,
		NumHeads:    4,
	}, nil
}

// abs32 returns the absolute value of a float32.
func abs32(x float32) float32 {
	if x < 0 {
		return -x
	}
	return x
}

// cosineSimilarity computes cosine similarity between two vectors.
func cosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var dot, normA, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dot / (sqrt32(normA) * sqrt32(normB))
}
