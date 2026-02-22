package onnx

import (
	"testing"

	"github.com/Neumenon/cowrie/gnn/algo"
)

func TestDefaultGATConfig(t *testing.T) {
	cfg := DefaultGATConfig()

	if cfg.NumLayers != 2 {
		t.Errorf("Expected 2 layers, got %d", cfg.NumLayers)
	}
	if cfg.NumHeads != 8 {
		t.Errorf("Expected 8 heads, got %d", cfg.NumHeads)
	}
	if cfg.HiddenDim != 8 {
		t.Errorf("Expected 8 hidden dim, got %d", cfg.HiddenDim)
	}
	if cfg.NumClasses != 7 {
		t.Errorf("Expected 7 classes, got %d", cfg.NumClasses)
	}
}

func TestSupportedGATModels(t *testing.T) {
	models := []string{"cora-gat-2layer", "citeseer-gat-2layer", "agent-gat-2layer"}

	for _, name := range models {
		if _, ok := SupportedGATModels[name]; !ok {
			t.Errorf("Model %s should be in SupportedGATModels", name)
		}
	}
}

func TestIsGATEnabled(t *testing.T) {
	if IsGATEnabled() {
		t.Log("GAT is enabled (built with -tags onnx)")
	} else {
		t.Log("GAT is not enabled (stub)")
	}
}

func TestFallbackGATClassify(t *testing.T) {
	// Build a simple graph: 0 <-> 1 <-> 2
	indPtr := []int64{0, 1, 3, 4}
	indices := []int64{1, 0, 2, 1}
	csr := algo.NewCSR(3, indPtr, indices)

	// Simple features
	features := [][]float32{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
		{0.0, 0.0, 1.0},
	}

	result, err := FallbackGATClassify(features, csr, 3)
	if err != nil {
		t.Fatalf("FallbackGATClassify failed: %v", err)
	}

	if len(result.Predictions) != 3 {
		t.Errorf("Expected 3 predictions, got %d", len(result.Predictions))
	}

	for i, pred := range result.Predictions {
		if pred < 0 || pred >= 3 {
			t.Errorf("Prediction %d at index %d out of range [0, 3)", pred, i)
		}
	}
}

func TestFallbackGATClassify_FeatureMismatch(t *testing.T) {
	indPtr := []int64{0, 1, 2, 2}
	indices := []int64{1, 2}
	csr := algo.NewCSR(3, indPtr, indices)

	// Wrong number of features
	features := [][]float32{
		{1.0, 0.0},
		{0.0, 1.0},
	}

	_, err := FallbackGATClassify(features, csr, 3)
	if err == nil {
		t.Error("Expected error for feature mismatch")
	}
}

func TestFallbackAttention(t *testing.T) {
	// 0 -> 1, 0 -> 2, 1 -> 2
	indPtr := []int64{0, 2, 3, 3}
	indices := []int64{1, 2, 2}
	csr := algo.NewCSR(3, indPtr, indices)

	features := [][]float32{
		{1.0, 0.5},
		{0.8, 0.6},
		{0.2, 0.9},
	}

	result, err := FallbackAttention(features, csr)
	if err != nil {
		t.Fatalf("FallbackAttention failed: %v", err)
	}

	if len(result.Weights) != 3 {
		t.Errorf("Expected 3 edges, got %d", len(result.Weights))
	}

	if result.NumHeads != 4 {
		t.Errorf("Expected 4 heads, got %d", result.NumHeads)
	}

	// Check each weight has correct number of heads
	for i, w := range result.Weights {
		if len(w) != 4 {
			t.Errorf("Edge %d: expected 4 attention values, got %d", i, len(w))
		}
	}
}

func TestAttentionResult_TopAttentionEdges(t *testing.T) {
	ar := &AttentionResult{
		Weights: [][]float32{
			{0.1, 0.2, 0.1, 0.2}, // avg = 0.15
			{0.8, 0.9, 0.7, 0.8}, // avg = 0.8
			{0.3, 0.4, 0.3, 0.4}, // avg = 0.35
		},
		SourceNodes: []int64{0, 0, 1},
		TargetNodes: []int64{1, 2, 2},
		NumHeads:    4,
	}

	top := ar.TopAttentionEdges(2)
	if len(top) != 2 {
		t.Fatalf("Expected 2 edges, got %d", len(top))
	}

	// Highest attention should be first
	if top[0].Source != 0 || top[0].Target != 2 {
		t.Errorf("Expected edge 0->2 first (highest attention), got %d->%d",
			top[0].Source, top[0].Target)
	}
}

func TestAttentionResult_TopAttentionEdges_Empty(t *testing.T) {
	var ar *AttentionResult
	top := ar.TopAttentionEdges(5)
	if top != nil {
		t.Error("Expected nil for nil AttentionResult")
	}

	ar = &AttentionResult{}
	top = ar.TopAttentionEdges(5)
	if top != nil {
		t.Error("Expected nil for empty AttentionResult")
	}
}

func TestCosineSimilarity(t *testing.T) {
	tests := []struct {
		a, b     []float32
		expected float32
	}{
		{[]float32{1, 0}, []float32{1, 0}, 1.0},
		{[]float32{1, 0}, []float32{0, 1}, 0.0},
		{[]float32{1, 0}, []float32{-1, 0}, -1.0},
		{[]float32{3, 4}, []float32{3, 4}, 1.0},
	}

	for i, tc := range tests {
		got := cosineSimilarity(tc.a, tc.b)
		if abs32(got-tc.expected) > 0.001 {
			t.Errorf("Test %d: expected %f, got %f", i, tc.expected, got)
		}
	}
}

func TestCosineSimilarity_EdgeCases(t *testing.T) {
	// Empty
	if cosineSimilarity(nil, nil) != 0 {
		t.Error("Empty should return 0")
	}

	// Different lengths
	if cosineSimilarity([]float32{1}, []float32{1, 2}) != 0 {
		t.Error("Different lengths should return 0")
	}

	// Zero vector
	if cosineSimilarity([]float32{0, 0}, []float32{1, 1}) != 0 {
		t.Error("Zero vector should return 0")
	}
}
