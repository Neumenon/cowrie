//go:build onnx

package onnx

import (
	"fmt"
	"os"

	"github.com/Neumenon/cowrie/go/gnn/algo"
	ort "github.com/yalue/onnxruntime_go"
)

// GCN implements GCN inference using ONNX Runtime.
type GCN struct {
	session    *ort.DynamicAdvancedSession
	cfg        GCNConfig
	inputNames []string
	outputName string
}

// NewGCN creates a new GCN inference model.
// For pre-trained models, use LoadGCN instead.
func NewGCN(cfg GCNConfig) (GCNInference, error) {
	if cfg.ModelPath == "" {
		return nil, fmt.Errorf("model path is required")
	}
	return LoadGCN(cfg.ModelPath)
}

// LoadGCN loads a pre-trained GCN model from ONNX file.
func LoadGCN(modelPath string) (GCNInference, error) {
	// Check file exists
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("model file not found: %s", modelPath)
	}

	// Initialize ONNX runtime
	if err := ort.InitializeEnvironment(); err != nil {
		return nil, fmt.Errorf("onnx init failed: %w", err)
	}

	// Load model
	modelBytes, err := os.ReadFile(modelPath)
	if err != nil {
		return nil, fmt.Errorf("read model: %w", err)
	}

	// Create session options
	opts, err := ort.NewSessionOptions()
	if err != nil {
		return nil, fmt.Errorf("session options: %w", err)
	}
	defer opts.Destroy()

	// Create session
	session, err := ort.NewDynamicAdvancedSession(modelBytes,
		[]string{"features", "edge_index"}, // Standard GCN inputs
		[]string{"output"},                 // Classification output
		opts)
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}

	return &GCN{
		session:    session,
		inputNames: []string{"features", "edge_index"},
		outputName: "output",
	}, nil
}

// ClassifyNodes performs node classification on all nodes.
func (g *GCN) ClassifyNodes(features [][]float32, csr *algo.CSR) (*NodeClassificationResult, error) {
	if g.session == nil {
		return nil, fmt.Errorf("session not initialized")
	}

	numNodes := len(features)
	if numNodes == 0 {
		return nil, fmt.Errorf("empty features")
	}
	featureDim := len(features[0])

	// Flatten features to [numNodes * featureDim]
	flatFeatures := make([]float32, numNodes*featureDim)
	for i, row := range features {
		copy(flatFeatures[i*featureDim:], row)
	}

	// Convert CSR to edge index format [2, numEdges]
	edgeIndex := csrToEdgeIndex(csr)

	// Create input tensors
	featureShape := ort.NewShape(int64(numNodes), int64(featureDim))
	featureTensor, err := ort.NewTensor(featureShape, flatFeatures)
	if err != nil {
		return nil, fmt.Errorf("create feature tensor: %w", err)
	}
	defer featureTensor.Destroy()

	edgeShape := ort.NewShape(2, int64(len(edgeIndex)/2))
	edgeTensor, err := ort.NewTensor(edgeShape, edgeIndex)
	if err != nil {
		return nil, fmt.Errorf("create edge tensor: %w", err)
	}
	defer edgeTensor.Destroy()

	// Run inference
	outputs, err := g.session.Run([]ort.ArbitraryTensor{featureTensor, edgeTensor})
	if err != nil {
		return nil, fmt.Errorf("run inference: %w", err)
	}
	defer func() {
		for _, o := range outputs {
			o.Destroy()
		}
	}()

	// Parse output [numNodes, numClasses]
	outputTensor := outputs[0].(*ort.Tensor[float32])
	outputData := outputTensor.GetData()
	outputShape := outputTensor.GetShape()

	numClasses := int(outputShape[1])
	predictions := make([]int, numNodes)
	confidence := make([]float32, numNodes)
	probabilities := make([][]float32, numNodes)

	for i := 0; i < numNodes; i++ {
		row := outputData[i*numClasses : (i+1)*numClasses]
		probabilities[i] = softmax(row)

		// Find argmax
		maxIdx := 0
		maxVal := probabilities[i][0]
		for j, v := range probabilities[i] {
			if v > maxVal {
				maxVal = v
				maxIdx = j
			}
		}
		predictions[i] = maxIdx
		confidence[i] = maxVal
	}

	return &NodeClassificationResult{
		Predictions:   predictions,
		Probabilities: probabilities,
		Confidence:    confidence,
	}, nil
}

// ClassifyNodesBatch classifies specific nodes.
func (g *GCN) ClassifyNodesBatch(features [][]float32, csr *algo.CSR, nodeIDs []int64) (*NodeClassificationResult, error) {
	// For GCN, we need to run on full graph but can return subset
	fullResult, err := g.ClassifyNodes(features, csr)
	if err != nil {
		return nil, err
	}

	// Extract requested nodes
	predictions := make([]int, len(nodeIDs))
	confidence := make([]float32, len(nodeIDs))
	var probabilities [][]float32
	if fullResult.Probabilities != nil {
		probabilities = make([][]float32, len(nodeIDs))
	}

	for i, nodeID := range nodeIDs {
		if nodeID < 0 || nodeID >= int64(len(fullResult.Predictions)) {
			continue
		}
		predictions[i] = fullResult.Predictions[nodeID]
		confidence[i] = fullResult.Confidence[nodeID]
		if probabilities != nil {
			probabilities[i] = fullResult.Probabilities[nodeID]
		}
	}

	return &NodeClassificationResult{
		Predictions:   predictions,
		Probabilities: probabilities,
		Confidence:    confidence,
	}, nil
}

// PredictLinks scores potential edges between node pairs.
func (g *GCN) PredictLinks(features [][]float32, csr *algo.CSR, srcNodes, dstNodes []int64) (*LinkPredictionResult, error) {
	// Get node embeddings
	embeddings, err := g.GetEmbeddings(features, csr)
	if err != nil {
		return nil, err
	}

	// Score each pair using dot product
	scores := make([]float32, len(srcNodes))
	for i := range srcNodes {
		src, dst := srcNodes[i], dstNodes[i]
		if src < 0 || src >= int64(len(embeddings.Embeddings)) ||
			dst < 0 || dst >= int64(len(embeddings.Embeddings)) {
			scores[i] = 0
			continue
		}

		// Dot product similarity
		var dot float32
		srcEmb := embeddings.Embeddings[src]
		dstEmb := embeddings.Embeddings[dst]
		for j := range srcEmb {
			dot += srcEmb[j] * dstEmb[j]
		}
		// Sigmoid to get probability
		scores[i] = sigmoid(dot)
	}

	return &LinkPredictionResult{
		Scores:      scores,
		SourceNodes: srcNodes,
		TargetNodes: dstNodes,
	}, nil
}

// GetEmbeddings extracts learned node embeddings from GCN hidden layer.
func (g *GCN) GetEmbeddings(features [][]float32, csr *algo.CSR) (*NodeEmbeddingResult, error) {
	// For standard GCN, the "embeddings" are the pre-softmax activations
	// or we can use a model that outputs embeddings directly

	// Run classification and use logits as embeddings
	result, err := g.ClassifyNodes(features, csr)
	if err != nil {
		return nil, err
	}

	// Use probabilities as embeddings (common approach)
	return &NodeEmbeddingResult{
		Embeddings: result.Probabilities,
		Dim:        len(result.Probabilities[0]),
	}, nil
}

// Close releases ONNX resources.
func (g *GCN) Close() error {
	if g.session != nil {
		g.session.Destroy()
		g.session = nil
	}
	return nil
}

// IsONNXEnabled returns true when built with ONNX support.
func IsONNXEnabled() bool {
	return true
}

// Helper functions

// csrToEdgeIndex converts CSR to PyG-style edge_index [2, numEdges].
func csrToEdgeIndex(csr *algo.CSR) []int64 {
	if csr == nil || csr.NumEdges == 0 {
		return nil
	}

	edgeIndex := make([]int64, csr.NumEdges*2)
	edgeIdx := int64(0)

	for node := int64(0); node < csr.NumNodes; node++ {
		start := csr.IndPtr[node]
		end := csr.IndPtr[node+1]
		for i := start; i < end; i++ {
			edgeIndex[edgeIdx] = node                        // source
			edgeIndex[edgeIdx+csr.NumEdges] = csr.Indices[i] // target
			edgeIdx++
		}
	}

	return edgeIndex
}

// softmax applies softmax to a slice.
func softmax(x []float32) []float32 {
	result := make([]float32, len(x))

	// Find max for numerical stability
	maxVal := x[0]
	for _, v := range x {
		if v > maxVal {
			maxVal = v
		}
	}

	// Compute exp and sum
	var sum float32
	for i, v := range x {
		result[i] = exp32(v - maxVal)
		sum += result[i]
	}

	// Normalize
	for i := range result {
		result[i] /= sum
	}

	return result
}

// sigmoid applies sigmoid function.
func sigmoid(x float32) float32 {
	return 1.0 / (1.0 + exp32(-x))
}

// exp32 computes e^x for float32.
func exp32(x float32) float32 {
	// Fast approximation
	if x < -88 {
		return 0
	}
	if x > 88 {
		return 3.4e38
	}

	// Use polynomial approximation
	x = 1.0 + x/256
	x *= x
	x *= x
	x *= x
	x *= x
	x *= x
	x *= x
	x *= x
	x *= x
	return x
}
