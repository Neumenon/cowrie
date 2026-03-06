package onnx

import (
	"testing"

	"github.com/Neumenon/cowrie/go/gnn/algo"
)

func TestPrepareFeatures_NormNone(t *testing.T) {
	features := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
	}

	result := PrepareFeatures(features, NormNone)

	// Should return same data
	if len(result) != len(features) {
		t.Fatalf("Expected %d rows, got %d", len(features), len(result))
	}

	for i := range features {
		for j := range features[i] {
			if result[i][j] != features[i][j] {
				t.Errorf("Expected %f, got %f", features[i][j], result[i][j])
			}
		}
	}
}

func TestPrepareFeatures_NormRow(t *testing.T) {
	features := [][]float32{
		{3.0, 4.0}, // L2 norm = 5
	}

	result := PrepareFeatures(features, NormRow)

	// After L2 norm: [0.6, 0.8]
	expected := []float32{0.6, 0.8}
	tolerance := float32(0.001)

	for i, exp := range expected {
		if abs32(result[0][i]-exp) > tolerance {
			t.Errorf("Expected ~%f, got %f", exp, result[0][i])
		}
	}
}

func TestPrepareFeatures_Empty(t *testing.T) {
	var features [][]float32
	result := PrepareFeatures(features, NormRow)
	if len(result) != 0 {
		t.Error("Empty input should return empty output")
	}
}

func TestDefaultGCNConfig(t *testing.T) {
	cfg := DefaultGCNConfig()

	if cfg.NumLayers != 2 {
		t.Errorf("Expected 2 layers, got %d", cfg.NumLayers)
	}
	if cfg.HiddenDim != 64 {
		t.Errorf("Expected 64 hidden dim, got %d", cfg.HiddenDim)
	}
	if cfg.NumClasses != 7 {
		t.Errorf("Expected 7 classes, got %d", cfg.NumClasses)
	}
}

func TestSupportedModels(t *testing.T) {
	// Check that standard models are defined
	models := []string{"cora-gcn-2layer", "citeseer-gcn-2layer", "pubmed-gcn-2layer", "agent-gcn-2layer"}

	for _, name := range models {
		if _, ok := SupportedModels[name]; !ok {
			t.Errorf("Model %s should be in SupportedModels", name)
		}
	}
}

func TestIsONNXEnabled(t *testing.T) {
	// When built without -tags onnx, this should be false
	if IsONNXEnabled() {
		t.Log("ONNX is enabled (built with -tags onnx)")
	} else {
		t.Log("ONNX is not enabled (stub)")
	}
	// This test passes either way, just logs the state
}

func TestFallbackClassify(t *testing.T) {
	// Build a simple graph: 0 -> 1 -> 2
	indPtr := []int64{0, 1, 2, 2}
	indices := []int64{1, 2}
	csr := algo.NewCSR(3, indPtr, indices)

	result, err := FallbackClassify(csr, 3)
	if err != nil {
		t.Fatalf("FallbackClassify failed: %v", err)
	}

	if len(result.Predictions) != 3 {
		t.Errorf("Expected 3 predictions, got %d", len(result.Predictions))
	}

	// All predictions should be in valid range [0, numClasses)
	for i, pred := range result.Predictions {
		if pred < 0 || pred >= 3 {
			t.Errorf("Prediction %d at index %d out of range [0, 3)", pred, i)
		}
	}

	if len(result.Confidence) != 3 {
		t.Errorf("Expected 3 confidence scores, got %d", len(result.Confidence))
	}
}

func TestFallbackClassify_EmptyGraph(t *testing.T) {
	_, err := FallbackClassify(nil, 3)
	if err == nil {
		t.Error("Expected error for nil CSR")
	}

	emptyCsr := algo.NewCSR(0, []int64{0}, []int64{})
	_, err = FallbackClassify(emptyCsr, 3)
	if err == nil {
		t.Error("Expected error for empty CSR")
	}
}

func TestNodeClassificationResult(t *testing.T) {
	result := NodeClassificationResult{
		Predictions: []int{0, 1, 2},
		Confidence:  []float32{0.9, 0.8, 0.7},
		Probabilities: [][]float32{
			{0.9, 0.05, 0.05},
			{0.1, 0.8, 0.1},
			{0.1, 0.2, 0.7},
		},
	}

	if len(result.Predictions) != 3 {
		t.Error("Expected 3 predictions")
	}
	if result.Predictions[0] != 0 {
		t.Error("First prediction should be 0")
	}
}

func TestLinkPredictionResult(t *testing.T) {
	result := LinkPredictionResult{
		Scores:      []float32{0.9, 0.1},
		SourceNodes: []int64{0, 1},
		TargetNodes: []int64{1, 2},
	}

	if len(result.Scores) != 2 {
		t.Error("Expected 2 scores")
	}
	if result.SourceNodes[0] != 0 {
		t.Error("First source should be 0")
	}
}

func TestNodeEmbeddingResult(t *testing.T) {
	result := NodeEmbeddingResult{
		Embeddings: [][]float32{
			{0.1, 0.2, 0.3},
			{0.4, 0.5, 0.6},
		},
		Dim: 3,
	}

	if len(result.Embeddings) != 2 {
		t.Error("Expected 2 embeddings")
	}
	if result.Dim != 3 {
		t.Error("Dim should be 3")
	}
}

// abs32 is defined in gat_stub.go
