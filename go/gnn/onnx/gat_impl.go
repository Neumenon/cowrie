//go:build onnx

package onnx

import (
	"fmt"
	"os"

	"github.com/Neumenon/cowrie/gnn/algo"
	ort "github.com/yalue/onnxruntime_go"
)

// GAT implements GAT inference using ONNX Runtime.
type GAT struct {
	session    *ort.DynamicAdvancedSession
	cfg        GATConfig
	inputNames []string
	outputName string
}

// NewGAT creates a new GAT inference model.
func NewGAT(cfg GATConfig) (GATInference, error) {
	if cfg.ModelPath == "" {
		return nil, fmt.Errorf("model path is required")
	}
	return LoadGAT(cfg.ModelPath)
}

// LoadGAT loads a pre-trained GAT model from ONNX file.
func LoadGAT(modelPath string) (GATInference, error) {
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("model file not found: %s", modelPath)
	}

	if err := ort.InitializeEnvironment(); err != nil {
		return nil, fmt.Errorf("onnx init failed: %w", err)
	}

	modelBytes, err := os.ReadFile(modelPath)
	if err != nil {
		return nil, fmt.Errorf("read model: %w", err)
	}

	opts, err := ort.NewSessionOptions()
	if err != nil {
		return nil, fmt.Errorf("session options: %w", err)
	}
	defer opts.Destroy()

	// GAT models typically have features, edge_index, and optionally return attention
	session, err := ort.NewDynamicAdvancedSession(modelBytes,
		[]string{"features", "edge_index"},
		[]string{"output", "attention"}, // GAT can output attention weights
		opts)
	if err != nil {
		// Try without attention output
		session, err = ort.NewDynamicAdvancedSession(modelBytes,
			[]string{"features", "edge_index"},
			[]string{"output"},
			opts)
		if err != nil {
			return nil, fmt.Errorf("create session: %w", err)
		}
	}

	return &GAT{
		session:    session,
		inputNames: []string{"features", "edge_index"},
		outputName: "output",
	}, nil
}

// ClassifyNodes performs node classification with attention.
func (g *GAT) ClassifyNodes(features [][]float32, csr *algo.CSR) (*NodeClassificationResult, error) {
	if g.session == nil {
		return nil, fmt.Errorf("session not initialized")
	}

	numNodes := len(features)
	if numNodes == 0 {
		return nil, fmt.Errorf("empty features")
	}
	featureDim := len(features[0])

	// Flatten features
	flatFeatures := make([]float32, numNodes*featureDim)
	for i, row := range features {
		copy(flatFeatures[i*featureDim:], row)
	}

	// Convert CSR to edge index
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

	// Parse output
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
func (g *GAT) ClassifyNodesBatch(features [][]float32, csr *algo.CSR, nodeIDs []int64) (*NodeClassificationResult, error) {
	fullResult, err := g.ClassifyNodes(features, csr)
	if err != nil {
		return nil, err
	}

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

// GetAttentionWeights returns attention weights for edges.
func (g *GAT) GetAttentionWeights(features [][]float32, csr *algo.CSR) (*AttentionResult, error) {
	if g.session == nil {
		return nil, fmt.Errorf("session not initialized")
	}

	numNodes := len(features)
	if numNodes == 0 {
		return nil, fmt.Errorf("empty features")
	}
	featureDim := len(features[0])

	flatFeatures := make([]float32, numNodes*featureDim)
	for i, row := range features {
		copy(flatFeatures[i*featureDim:], row)
	}

	edgeIndex := csrToEdgeIndex(csr)

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

	outputs, err := g.session.Run([]ort.ArbitraryTensor{featureTensor, edgeTensor})
	if err != nil {
		return nil, fmt.Errorf("run inference: %w", err)
	}
	defer func() {
		for _, o := range outputs {
			o.Destroy()
		}
	}()

	// Check if attention output exists
	if len(outputs) < 2 {
		// Model doesn't output attention, use fallback
		return FallbackAttention(features, csr)
	}

	// Parse attention output [numEdges, numHeads] or [numHeads, numEdges]
	attnTensor := outputs[1].(*ort.Tensor[float32])
	attnData := attnTensor.GetData()
	attnShape := attnTensor.GetShape()

	numEdges := int(csr.NumEdges)
	numHeads := int(attnShape[1])

	weights := make([][]float32, numEdges)
	for i := 0; i < numEdges; i++ {
		weights[i] = make([]float32, numHeads)
		for h := 0; h < numHeads; h++ {
			weights[i][h] = attnData[i*numHeads+h]
		}
	}

	// Build source/target node lists
	sourceNodes := make([]int64, numEdges)
	targetNodes := make([]int64, numEdges)
	edgeIdx := 0
	for node := int64(0); node < csr.NumNodes; node++ {
		start := csr.IndPtr[node]
		end := csr.IndPtr[node+1]
		for i := start; i < end; i++ {
			sourceNodes[edgeIdx] = node
			targetNodes[edgeIdx] = csr.Indices[i]
			edgeIdx++
		}
	}

	return &AttentionResult{
		Weights:     weights,
		SourceNodes: sourceNodes,
		TargetNodes: targetNodes,
		NumHeads:    numHeads,
	}, nil
}

// GetEmbeddings extracts learned node embeddings.
func (g *GAT) GetEmbeddings(features [][]float32, csr *algo.CSR) (*NodeEmbeddingResult, error) {
	result, err := g.ClassifyNodes(features, csr)
	if err != nil {
		return nil, err
	}

	return &NodeEmbeddingResult{
		Embeddings: result.Probabilities,
		Dim:        len(result.Probabilities[0]),
	}, nil
}

// Close releases ONNX resources.
func (g *GAT) Close() error {
	if g.session != nil {
		g.session.Destroy()
		g.session = nil
	}
	return nil
}

// IsGATEnabled returns true when built with ONNX support.
func IsGATEnabled() bool {
	return true
}
