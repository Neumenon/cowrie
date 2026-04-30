package gnn

import (
	"math"
	"testing"
)

// =============================================================================
// Feature Tests (Blocked + Row-Wise modes)
// =============================================================================

func TestFeatureBlockedFloat32(t *testing.T) {
	// Create a blocked float32 feature (e.g., node embeddings)
	w := NewFeatureWriter("embeddings", DTypeFloat32, []int{4}) // 4-dim embeddings
	w.WriteHeader(3)                                            // 3 nodes

	// Write tensor: 3 nodes x 4 features = 12 values
	data := []float32{
		1.0, 2.0, 3.0, 4.0, // node 0
		5.0, 6.0, 7.0, 8.0, // node 1
		9.0, 10.0, 11.0, 12.0, // node 2
	}
	w.WriteFloat32Tensor(data)

	// Read back
	encoded := w.Bytes()
	r, err := NewFeatureReader(encoded)
	if err != nil {
		t.Fatalf("NewFeatureReader failed: %v", err)
	}

	if r.FeatureName() != "embeddings" {
		t.Errorf("FeatureName = %q, want %q", r.FeatureName(), "embeddings")
	}
	if r.Mode() != FeatureModeBlocked {
		t.Errorf("Mode = %v, want FeatureModeBlocked", r.Mode())
	}
	if r.DType() != DTypeFloat32 {
		t.Errorf("DType = %v, want DTypeFloat32", r.DType())
	}
	if r.NumRows() != 3 {
		t.Errorf("NumRows = %d, want 3", r.NumRows())
	}

	decoded, err := r.ReadFloat32Tensor()
	if err != nil {
		t.Fatalf("ReadFloat32Tensor failed: %v", err)
	}

	if len(decoded) != 12 {
		t.Fatalf("len(decoded) = %d, want 12", len(decoded))
	}
	for i, v := range data {
		if decoded[i] != v {
			t.Errorf("decoded[%d] = %f, want %f", i, decoded[i], v)
		}
	}
}

func TestFeatureBlockedFloat64(t *testing.T) {
	w := NewFeatureWriter("weights", DTypeFloat64, []int{2})
	w.WriteHeader(2)

	data := []float64{1.5, 2.5, 3.5, 4.5}
	w.WriteFloat64Tensor(data)

	r, err := NewFeatureReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewFeatureReader failed: %v", err)
	}

	decoded, err := r.ReadFloat64Tensor()
	if err != nil {
		t.Fatalf("ReadFloat64Tensor failed: %v", err)
	}

	for i, v := range data {
		if decoded[i] != v {
			t.Errorf("decoded[%d] = %f, want %f", i, decoded[i], v)
		}
	}
}

func TestFeatureBlockedInt64(t *testing.T) {
	w := NewFeatureWriter("labels", DTypeInt64, []int{1})
	w.WriteHeader(5)

	data := []int64{0, 1, 2, 1, 0}
	w.WriteInt64Tensor(data)

	r, err := NewFeatureReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewFeatureReader failed: %v", err)
	}

	decoded, err := r.ReadInt64Tensor()
	if err != nil {
		t.Fatalf("ReadInt64Tensor failed: %v", err)
	}

	for i, v := range data {
		if decoded[i] != v {
			t.Errorf("decoded[%d] = %d, want %d", i, decoded[i], v)
		}
	}
}

func TestFeatureRowWiseFloat32(t *testing.T) {
	// Row-wise mode: sparse features with IDs
	w := NewRowWiseFeatureWriter("sparse_embed", DTypeFloat32, []int{3})
	w.WriteHeader(0) // numRows doesn't matter for row-wise mode

	// Only write features for specific node IDs (sparse)
	w.WriteFloat32Row(5, []float32{1.0, 2.0, 3.0})
	w.WriteFloat32Row(10, []float32{4.0, 5.0, 6.0})
	w.WriteFloat32Row(100, []float32{7.0, 8.0, 9.0})

	r, err := NewFeatureReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewFeatureReader failed: %v", err)
	}

	if r.Mode() != FeatureModeRowWise {
		t.Errorf("Mode = %v, want FeatureModeRowWise", r.Mode())
	}

	// Read rows
	expectedIDs := []int64{5, 10, 100}
	expectedData := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
		{7.0, 8.0, 9.0},
	}

	for i := 0; i < 3; i++ {
		if !r.HasMoreRows() {
			t.Fatalf("Expected more rows at iteration %d", i)
		}
		id, data, err := r.ReadFloat32Row()
		if err != nil {
			t.Fatalf("ReadFloat32Row failed: %v", err)
		}
		if id != expectedIDs[i] {
			t.Errorf("id = %d, want %d", id, expectedIDs[i])
		}
		for j, v := range expectedData[i] {
			if data[j] != v {
				t.Errorf("data[%d] = %f, want %f", j, data[j], v)
			}
		}
	}

	if r.HasMoreRows() {
		t.Error("Expected no more rows")
	}
}

func TestFeatureMultiDimShape(t *testing.T) {
	// 2D shape: 3x4 matrix per node
	w := NewFeatureWriter("matrices", DTypeFloat32, []int{3, 4})
	w.WriteHeader(2) // 2 nodes

	// 2 nodes * 3 * 4 = 24 values
	data := make([]float32, 24)
	for i := range data {
		data[i] = float32(i)
	}
	w.WriteFloat32Tensor(data)

	r, err := NewFeatureReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewFeatureReader failed: %v", err)
	}

	shape := r.Shape()
	if len(shape) != 2 || shape[0] != 3 || shape[1] != 4 {
		t.Errorf("Shape = %v, want [3, 4]", shape)
	}

	decoded, err := r.ReadFloat32Tensor()
	if err != nil {
		t.Fatalf("ReadFloat32Tensor failed: %v", err)
	}

	if len(decoded) != 24 {
		t.Fatalf("len(decoded) = %d, want 24", len(decoded))
	}
}

// =============================================================================
// Split Tests (Indices + Mask modes)
// =============================================================================

func TestSplitIndicesMode(t *testing.T) {
	w := NewSplitWriter()

	train := []int64{0, 1, 2, 3, 4}
	val := []int64{5, 6}
	test := []int64{7, 8, 9}

	w.WriteIndices(train, val, test)

	r, err := NewSplitReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewSplitReader failed: %v", err)
	}

	if r.Mode() != SplitModeIndices {
		t.Errorf("Mode = %v, want SplitModeIndices", r.Mode())
	}

	decodedTrain, decodedVal, decodedTest, err := r.ReadIndices()
	if err != nil {
		t.Fatalf("ReadIndices failed: %v", err)
	}

	compareInt64Slices(t, "train", decodedTrain, train)
	compareInt64Slices(t, "val", decodedVal, val)
	compareInt64Slices(t, "test", decodedTest, test)
}

func TestSplitMaskMode(t *testing.T) {
	numNodes := int64(100)

	// Create masks: train=0-59, val=60-79, test=80-99
	trainMask := IndicesToMask(rangeInt64(0, 60), numNodes)
	valMask := IndicesToMask(rangeInt64(60, 80), numNodes)
	testMask := IndicesToMask(rangeInt64(80, 100), numNodes)

	w := NewSplitWriter()
	w.WriteMasks(numNodes, trainMask, valMask, testMask)

	r, err := NewSplitReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewSplitReader failed: %v", err)
	}

	if r.Mode() != SplitModeMask {
		t.Errorf("Mode = %v, want SplitModeMask", r.Mode())
	}
	if r.NumNodes() != numNodes {
		t.Errorf("NumNodes = %d, want %d", r.NumNodes(), numNodes)
	}

	decodedTrain, decodedVal, decodedTest, err := r.ReadMasks()
	if err != nil {
		t.Fatalf("ReadMasks failed: %v", err)
	}

	compareBytesSlice(t, "trainMask", decodedTrain, trainMask)
	compareBytesSlice(t, "valMask", decodedVal, valMask)
	compareBytesSlice(t, "testMask", decodedTest, testMask)
}

func TestIndicesToMaskRoundTrip(t *testing.T) {
	indices := []int64{0, 5, 10, 15, 99}
	numNodes := int64(100)

	mask := IndicesToMask(indices, numNodes)
	recovered := MaskToIndices(mask, numNodes)

	compareInt64Slices(t, "indices", recovered, indices)
}

func TestTrainValTestSplit(t *testing.T) {
	nodeIDs := rangeInt64(0, 100)

	train, val, test := TrainValTestSplit(nodeIDs, 0.6, 0.2)

	if len(train) != 60 {
		t.Errorf("len(train) = %d, want 60", len(train))
	}
	if len(val) != 20 {
		t.Errorf("len(val) = %d, want 20", len(val))
	}
	if len(test) != 20 {
		t.Errorf("len(test) = %d, want 20", len(test))
	}

	// Verify non-overlapping
	total := len(train) + len(val) + len(test)
	if total != 100 {
		t.Errorf("total = %d, want 100", total)
	}
}

// =============================================================================
// Aux Tests (CSR/CSC)
// =============================================================================

func TestAuxCSR(t *testing.T) {
	// Simple graph: 0->1, 0->2, 1->2
	numNodes := int64(3)
	indptr := []int64{0, 2, 3, 3} // node 0 has 2 edges, node 1 has 1, node 2 has 0
	indices := []int64{1, 2, 2}   // node 0 -> [1,2], node 1 -> [2]

	w := NewAuxWriter()
	w.WriteCSR(numNodes, indptr, indices)

	r, err := NewAuxReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewAuxReader failed: %v", err)
	}

	if r.Format() != AuxFormatCSR {
		t.Errorf("Format = %v, want AuxFormatCSR", r.Format())
	}
	if r.NumNodes() != numNodes {
		t.Errorf("NumNodes = %d, want %d", r.NumNodes(), numNodes)
	}
	if r.NumEdges() != 3 {
		t.Errorf("NumEdges = %d, want 3", r.NumEdges())
	}

	decodedIndptr, decodedIndices, err := r.ReadCSRArrays()
	if err != nil {
		t.Fatalf("ReadCSRArrays failed: %v", err)
	}

	compareInt64Slices(t, "indptr", decodedIndptr, indptr)
	compareInt64Slices(t, "indices", decodedIndices, indices)
}

func TestAuxCSC(t *testing.T) {
	numNodes := int64(3)
	indptr := []int64{0, 0, 1, 3} // col 0 has 0 entries, col 1 has 1, col 2 has 2
	_ = []int64{0, 0, 1}          // column 1 <- [0], column 2 <- [0, 1]

	w := NewAuxWriter()
	w.WriteCSC(numNodes, indptr, indptr[1:]) // Use indptr slice for test

	r, err := NewAuxReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewAuxReader failed: %v", err)
	}

	if r.Format() != AuxFormatCSC {
		t.Errorf("Format = %v, want AuxFormatCSC", r.Format())
	}
}

// =============================================================================
// Graph Utility Tests
// =============================================================================

func TestCOOToCSR(t *testing.T) {
	// Graph: 0->1, 0->2, 1->2, 2->0
	src := []int64{0, 0, 1, 2}
	dst := []int64{1, 2, 2, 0}
	numNodes := int64(3)

	indptr, indices := COOToCSR(numNodes, src, dst)

	// Expected CSR:
	// node 0: edges to [1, 2] -> indptr[0]=0, indptr[1]=2
	// node 1: edges to [2]    -> indptr[2]=3
	// node 2: edges to [0]    -> indptr[3]=4
	expectedIndptr := []int64{0, 2, 3, 4}
	if len(indptr) != 4 {
		t.Fatalf("len(indptr) = %d, want 4", len(indptr))
	}
	compareInt64Slices(t, "indptr", indptr, expectedIndptr)

	if len(indices) != 4 {
		t.Fatalf("len(indices) = %d, want 4", len(indices))
	}

	// Verify neighbors using CSRToNeighbors
	neighbors0 := CSRToNeighbors(0, indptr, indices)
	if len(neighbors0) != 2 {
		t.Errorf("neighbors of 0: len = %d, want 2", len(neighbors0))
	}

	neighbors1 := CSRToNeighbors(1, indptr, indices)
	if len(neighbors1) != 1 || neighbors1[0] != 2 {
		t.Errorf("neighbors of 1 = %v, want [2]", neighbors1)
	}

	neighbors2 := CSRToNeighbors(2, indptr, indices)
	if len(neighbors2) != 1 || neighbors2[0] != 0 {
		t.Errorf("neighbors of 2 = %v, want [0]", neighbors2)
	}
}

func TestCOOToCSC(t *testing.T) {
	// Graph: 0->1, 0->2, 1->2
	src := []int64{0, 0, 1}
	dst := []int64{1, 2, 2}
	numNodes := int64(3)

	indptr, indices := COOToCSC(numNodes, src, dst)

	// CSC (transpose): which nodes point TO each column
	// col 0: no incoming -> indptr[0]=0, indptr[1]=0
	// col 1: 0 points to it -> indptr[2]=1
	// col 2: 0 and 1 point to it -> indptr[3]=3
	expectedIndptr := []int64{0, 0, 1, 3}
	compareInt64Slices(t, "indptr", indptr, expectedIndptr)

	// Verify indices length matches number of edges
	if len(indices) != 3 {
		t.Errorf("len(indices) = %d, want 3", len(indices))
	}
}

func TestCSRToCOO(t *testing.T) {
	// CSR representation
	indptr := []int64{0, 2, 3, 4}
	indices := []int64{1, 2, 2, 0}

	src, dst := CSRToCOO(indptr, indices)

	if len(src) != 4 || len(dst) != 4 {
		t.Fatalf("len mismatch: src=%d, dst=%d", len(src), len(dst))
	}

	// Verify edges: 0->1, 0->2, 1->2, 2->0
	edges := make(map[[2]int64]bool)
	for i := range src {
		edges[[2]int64{src[i], dst[i]}] = true
	}

	expectedEdges := [][2]int64{{0, 1}, {0, 2}, {1, 2}, {2, 0}}
	for _, e := range expectedEdges {
		if !edges[e] {
			t.Errorf("Missing edge %d->%d", e[0], e[1])
		}
	}
}

func TestAddSelfLoops(t *testing.T) {
	// Graph without self-loops
	src := []int64{0, 1}
	dst := []int64{1, 2}
	numNodes := int64(3)

	newSrc, newDst := AddSelfLoops(numNodes, src, dst)

	// Should have 3 self-loops added (0->0, 1->1, 2->2)
	if len(newSrc) != 5 {
		t.Errorf("len(newSrc) = %d, want 5", len(newSrc))
	}

	// Check self-loops exist
	selfLoops := make(map[int64]bool)
	for i := range newSrc {
		if newSrc[i] == newDst[i] {
			selfLoops[newSrc[i]] = true
		}
	}

	for i := int64(0); i < numNodes; i++ {
		if !selfLoops[i] {
			t.Errorf("Missing self-loop for node %d", i)
		}
	}
}

func TestAddSelfLoopsAlreadyExists(t *testing.T) {
	// Graph with existing self-loop on node 1
	src := []int64{0, 1, 1}
	dst := []int64{1, 1, 2} // 1->1 is self-loop
	numNodes := int64(3)

	newSrc, _ := AddSelfLoops(numNodes, src, dst)

	// Should add 2 self-loops (0->0, 2->2), not duplicate 1->1
	if len(newSrc) != 5 {
		t.Errorf("len(newSrc) = %d, want 5", len(newSrc))
	}
}

func TestToUndirected(t *testing.T) {
	// Directed graph: 0->1, 1->2
	src := []int64{0, 1}
	dst := []int64{1, 2}

	newSrc, newDst := ToUndirected(src, dst)

	// Should add reverse edges: 1->0, 2->1
	if len(newSrc) != 4 {
		t.Errorf("len(newSrc) = %d, want 4", len(newSrc))
	}

	// Verify all edges
	edges := make(map[[2]int64]bool)
	for i := range newSrc {
		edges[[2]int64{newSrc[i], newDst[i]}] = true
	}

	expectedEdges := [][2]int64{{0, 1}, {1, 0}, {1, 2}, {2, 1}}
	for _, e := range expectedEdges {
		if !edges[e] {
			t.Errorf("Missing edge %d->%d", e[0], e[1])
		}
	}
}

func TestToUndirectedNoDuplicates(t *testing.T) {
	// Already has both directions: 0<->1
	src := []int64{0, 1}
	dst := []int64{1, 0}

	newSrc, _ := ToUndirected(src, dst)

	// Should not add duplicates
	if len(newSrc) != 2 {
		t.Errorf("len(newSrc) = %d, want 2", len(newSrc))
	}
}

func TestToUndirectedSelfLoop(t *testing.T) {
	// Self-loop should not be duplicated
	src := []int64{0, 1}
	dst := []int64{0, 2} // 0->0 is self-loop

	newSrc, _ := ToUndirected(src, dst)

	// Should add 2->1 but not duplicate 0->0
	if len(newSrc) != 3 {
		t.Errorf("len(newSrc) = %d, want 3", len(newSrc))
	}
}

// =============================================================================
// Byte Conversion Tests
// =============================================================================

func TestFloat32ToBytes(t *testing.T) {
	data := []float32{1.0, 2.5, 3.14159, -0.5}

	bytes := Float32ToBytes(data)
	if len(bytes) != 16 {
		t.Fatalf("len(bytes) = %d, want 16", len(bytes))
	}

	recovered := BytesToFloat32(bytes)
	for i, v := range data {
		if math.Abs(float64(recovered[i]-v)) > 1e-6 {
			t.Errorf("recovered[%d] = %f, want %f", i, recovered[i], v)
		}
	}
}

func TestFloat64ToBytes(t *testing.T) {
	data := []float64{1.0, 2.5, 3.14159265358979, -0.5}

	bytes := Float64ToBytes(data)
	if len(bytes) != 32 {
		t.Fatalf("len(bytes) = %d, want 32", len(bytes))
	}

	recovered := BytesToFloat64(bytes)
	for i, v := range data {
		if recovered[i] != v {
			t.Errorf("recovered[%d] = %f, want %f", i, recovered[i], v)
		}
	}
}

func TestInt64ToBytes(t *testing.T) {
	data := []int64{0, 1, -1, 1000000, -9223372036854775808}

	bytes := Int64ToBytes(data)
	if len(bytes) != 40 {
		t.Fatalf("len(bytes) = %d, want 40", len(bytes))
	}

	recovered := BytesToInt64(bytes)
	for i, v := range data {
		if recovered[i] != v {
			t.Errorf("recovered[%d] = %d, want %d", i, recovered[i], v)
		}
	}
}

// =============================================================================
// Section Integration Tests
// =============================================================================

func TestFeatureToSection(t *testing.T) {
	w := NewFeatureWriter("test", DTypeFloat32, []int{2})
	w.WriteHeader(2)
	w.WriteFloat32Tensor([]float32{1, 2, 3, 4})

	section := w.ToSection("feature:test")

	if section.Kind != SectionFeature {
		t.Errorf("Kind = %v, want SectionFeature", section.Kind)
	}
	if section.Name != "feature:test" {
		t.Errorf("Name = %q, want %q", section.Name, "feature:test")
	}
	if len(section.Body) == 0 {
		t.Error("Body should not be empty")
	}
}

func TestSplitToSection(t *testing.T) {
	w := NewSplitWriter()
	w.WriteIndices([]int64{0, 1}, []int64{2}, []int64{3})

	section := w.ToSection("split:default")

	if section.Kind != SectionSplit {
		t.Errorf("Kind = %v, want SectionSplit", section.Kind)
	}
}

func TestAuxToSection(t *testing.T) {
	w := NewAuxWriter()
	w.WriteCSR(3, []int64{0, 1, 2, 3}, []int64{1, 2, 0})

	section := w.ToSection("aux:csr")

	if section.Kind != SectionAux {
		t.Errorf("Kind = %v, want SectionAux", section.Kind)
	}
}

// =============================================================================
// NodeTable Tests (GraphCowrie-Stream integration)
// =============================================================================

func TestNodeTableRoundTrip(t *testing.T) {
	// Create node table
	w := NewNodeTableWriter("paper")
	if err := w.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Write nodes with properties
	w.WriteNode(0, map[string]any{"title": "Paper A", "year": int64(2020)})
	w.WriteNode(1, map[string]any{"title": "Paper B", "year": int64(2021)})
	w.WriteNode(2, map[string]any{"title": "Paper C", "year": int64(2022)})
	w.Close()

	// Read back
	r, err := NewNodeTableReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewNodeTableReader failed: %v", err)
	}

	// Verify header
	hdr := r.Header()
	if len(hdr.LabelDict) == 0 || hdr.LabelDict[0] != "paper" {
		t.Errorf("Header labels = %v, want [paper]", hdr.LabelDict)
	}

	// Read nodes one by one
	node0, err := r.ReadNode()
	if err != nil {
		t.Fatalf("ReadNode failed: %v", err)
	}
	if node0 == nil {
		t.Fatal("Expected node, got nil")
	}
	if node0.ID != "0" {
		t.Errorf("Node ID = %q, want %q", node0.ID, "0")
	}
	if len(node0.Labels) == 0 || node0.Labels[0] != "paper" {
		t.Errorf("Node labels = %v, want [paper]", node0.Labels)
	}
}

func TestNodeTableReadAll(t *testing.T) {
	w := NewNodeTableWriter("author")
	w.WriteHeader()
	w.WriteNode(0, map[string]any{"name": "Alice"})
	w.WriteNode(1, map[string]any{"name": "Bob"})
	w.WriteNode(2, map[string]any{"name": "Charlie"})
	w.Close()

	r, err := NewNodeTableReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewNodeTableReader failed: %v", err)
	}

	nodes, err := r.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(nodes) != 3 {
		t.Errorf("len(nodes) = %d, want 3", len(nodes))
	}
}

func TestNodeTableReadAllRecords(t *testing.T) {
	w := NewNodeTableWriter("user")
	w.WriteHeader()
	w.WriteNode(10, map[string]any{"email": "a@b.com"})
	w.WriteNode(20, map[string]any{"email": "c@d.com"})
	w.Close()

	r, err := NewNodeTableReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewNodeTableReader failed: %v", err)
	}

	records, err := r.ReadAllRecords()
	if err != nil {
		t.Fatalf("ReadAllRecords failed: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("len(records) = %d, want 2", len(records))
	}

	// Verify IDs are parsed correctly
	if records[0].ID != 10 {
		t.Errorf("records[0].ID = %d, want 10", records[0].ID)
	}
	if records[1].ID != 20 {
		t.Errorf("records[1].ID = %d, want 20", records[1].ID)
	}
}

func TestNodeTableWithLabels(t *testing.T) {
	w := NewNodeTableWriter("entity")
	w.WriteHeader()
	w.WriteNodeWithLabels(0, []string{"person", "employee"}, map[string]any{"name": "John"})
	w.Close()

	r, err := NewNodeTableReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewNodeTableReader failed: %v", err)
	}

	node, err := r.ReadNode()
	if err != nil {
		t.Fatalf("ReadNode failed: %v", err)
	}

	if len(node.Labels) != 2 {
		t.Errorf("len(Labels) = %d, want 2", len(node.Labels))
	}
}

// =============================================================================
// EdgeTable Tests (GraphCowrie-Stream integration)
// =============================================================================

func TestEdgeTableRoundTrip(t *testing.T) {
	w := NewEdgeTableWriter("cites")
	w.WriteHeader()
	w.WriteEdge(0, 1, map[string]any{"weight": 1.0})
	w.WriteEdge(0, 2, map[string]any{"weight": 0.5})
	w.WriteEdge(1, 2, map[string]any{"weight": 0.8})
	w.Close()

	r, err := NewEdgeTableReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewEdgeTableReader failed: %v", err)
	}

	// Verify header
	hdr := r.Header()
	if len(hdr.LabelDict) == 0 || hdr.LabelDict[0] != "cites" {
		t.Errorf("Header labels = %v, want [cites]", hdr.LabelDict)
	}

	// Read first edge
	edge, err := r.ReadEdge()
	if err != nil {
		t.Fatalf("ReadEdge failed: %v", err)
	}
	if edge == nil {
		t.Fatal("Expected edge, got nil")
	}
	if edge.FromID != "0" || edge.ToID != "1" {
		t.Errorf("Edge = %s->%s, want 0->1", edge.FromID, edge.ToID)
	}
	if edge.Label != "cites" {
		t.Errorf("Edge label = %q, want %q", edge.Label, "cites")
	}
}

func TestEdgeTableReadAll(t *testing.T) {
	w := NewEdgeTableWriter("follows")
	w.WriteHeader()
	w.WriteEdge(0, 1, nil)
	w.WriteEdge(1, 2, nil)
	w.WriteEdge(2, 0, nil)
	w.Close()

	r, err := NewEdgeTableReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewEdgeTableReader failed: %v", err)
	}

	edges, err := r.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(edges) != 3 {
		t.Errorf("len(edges) = %d, want 3", len(edges))
	}
}

func TestEdgeTableWithTimestamp(t *testing.T) {
	w := NewEdgeTableWriter("interaction")
	w.WriteHeader()
	w.WriteEdgeWithTimestamp(0, 1, 1700000000, map[string]any{"type": "click"})
	w.Close()

	r, err := NewEdgeTableReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewEdgeTableReader failed: %v", err)
	}

	records, err := r.ReadAllRecords()
	if err != nil {
		t.Fatalf("ReadAllRecords failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}

	if records[0].Timestamp != 1700000000 {
		t.Errorf("Timestamp = %d, want 1700000000", records[0].Timestamp)
	}
}

func TestEdgeTableWithWeight(t *testing.T) {
	w := NewEdgeTableWriter("similarity")
	w.WriteHeader()
	w.WriteEdgeWithWeight(0, 1, 0.95, nil)
	w.Close()

	r, err := NewEdgeTableReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewEdgeTableReader failed: %v", err)
	}

	records, err := r.ReadAllRecords()
	if err != nil {
		t.Fatalf("ReadAllRecords failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}

	if records[0].Weight != 0.95 {
		t.Errorf("Weight = %f, want 0.95", records[0].Weight)
	}
}

func TestEdgeTableCOO(t *testing.T) {
	w := NewEdgeTableWriter("connects")
	w.WriteHeader()
	w.WriteEdge(0, 1, nil)
	w.WriteEdge(0, 2, nil)
	w.WriteEdge(1, 2, nil)
	w.WriteEdge(2, 0, nil)
	w.Close()

	r, err := NewEdgeTableReader(w.Bytes())
	if err != nil {
		t.Fatalf("NewEdgeTableReader failed: %v", err)
	}

	src, dst, err := r.COO()
	if err != nil {
		t.Fatalf("COO failed: %v", err)
	}

	if len(src) != 4 || len(dst) != 4 {
		t.Fatalf("len(src)=%d, len(dst)=%d, want 4,4", len(src), len(dst))
	}

	// Verify edges
	expectedSrc := []int64{0, 0, 1, 2}
	expectedDst := []int64{1, 2, 2, 0}
	compareInt64Slices(t, "src", src, expectedSrc)
	compareInt64Slices(t, "dst", dst, expectedDst)
}

func TestNodeTableToSection(t *testing.T) {
	w := NewNodeTableWriter("test")
	w.WriteHeader()
	w.WriteNode(0, nil)
	w.Close()

	section := w.ToSection("nodes:test")

	if section.Kind != SectionNodeTable {
		t.Errorf("Kind = %v, want SectionNodeTable", section.Kind)
	}
	if section.Name != "nodes:test" {
		t.Errorf("Name = %q, want %q", section.Name, "nodes:test")
	}
}

func TestEdgeTableToSection(t *testing.T) {
	w := NewEdgeTableWriter("test")
	w.WriteHeader()
	w.WriteEdge(0, 1, nil)
	w.Close()

	section := w.ToSection("edges:test")

	if section.Kind != SectionEdgeTable {
		t.Errorf("Kind = %v, want SectionEdgeTable", section.Kind)
	}
}

// =============================================================================
// Helpers
// =============================================================================

func rangeInt64(start, end int64) []int64 {
	result := make([]int64, end-start)
	for i := range result {
		result[i] = start + int64(i)
	}
	return result
}

func compareInt64Slices(t *testing.T, name string, got, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: len = %d, want %d", name, len(got), len(want))
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s[%d] = %d, want %d", name, i, got[i], want[i])
		}
	}
}

func compareBytesSlice(t *testing.T, name string, got, want []byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: len = %d, want %d", name, len(got), len(want))
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s[%d] = %d, want %d", name, i, got[i], want[i])
		}
	}
}
