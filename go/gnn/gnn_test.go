package gnn

import (
	"bytes"
	"testing"
)

func TestContainerRoundTrip(t *testing.T) {
	// Create a container
	c := NewContainer("test-dataset")
	c.SetDirected(true)
	c.SetHeterogeneous(true)
	c.SetTemporal(false)

	// Add node types
	c.AddNodeType("paper", 1000)
	c.AddNodeType("author", 500)

	// Add edge type
	c.AddEdgeType("paper", "cites", "paper")
	c.AddEdgeType("author", "writes", "paper")

	// Add feature spec
	c.AddFeature("paper", "x", DTypeFloat32, []int{128})
	c.AddLabel("paper", "y", DTypeInt64, []int{1})

	// Add a custom section
	c.AddSection(SectionAux, "test:aux", []byte("test data"))

	// Encode
	data, err := c.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Verify magic bytes
	if len(data) < 4 {
		t.Fatal("Data too short")
	}
	if data[0] != 'S' || data[1] != 'J' || data[2] != 'G' || data[3] != 'N' {
		t.Error("Magic bytes incorrect")
	}

	// Decode
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify flags
	if !decoded.IsHeterogeneous() {
		t.Error("Expected heterogeneous flag")
	}
	if decoded.IsTemporal() {
		t.Error("Expected temporal flag to be false")
	}

	// Verify meta
	meta := decoded.Meta()
	if meta == nil {
		t.Fatal("Meta should not be nil")
	}
	if meta.DatasetName != "test-dataset" {
		t.Errorf("DatasetName = %q, want %q", meta.DatasetName, "test-dataset")
	}
	if !meta.Directed {
		t.Error("Expected Directed = true")
	}
	if len(meta.NodeTypes) != 2 {
		t.Errorf("len(NodeTypes) = %d, want 2", len(meta.NodeTypes))
	}
	if len(meta.EdgeTypes) != 2 {
		t.Errorf("len(EdgeTypes) = %d, want 2", len(meta.EdgeTypes))
	}

	// Verify ID spaces
	paperSpace, ok := meta.IDSpaces["paper"]
	if !ok {
		t.Fatal("Missing paper ID space")
	}
	if paperSpace.Start != 0 || paperSpace.Count != 1000 {
		t.Errorf("Paper ID space = {%d, %d}, want {0, 1000}", paperSpace.Start, paperSpace.Count)
	}
	authorSpace, ok := meta.IDSpaces["author"]
	if !ok {
		t.Fatal("Missing author ID space")
	}
	if authorSpace.Start != 1000 || authorSpace.Count != 500 {
		t.Errorf("Author ID space = {%d, %d}, want {1000, 500}", authorSpace.Start, authorSpace.Count)
	}

	// Verify custom section
	auxSections := decoded.GetSectionsByKind(SectionAux)
	if len(auxSections) != 1 {
		t.Fatalf("Expected 1 aux section, got %d", len(auxSections))
	}
	if string(auxSections[0].Body) != "test data" {
		t.Errorf("Aux body = %q, want %q", string(auxSections[0].Body), "test data")
	}
}

func TestContainerWithSections(t *testing.T) {
	c := NewContainer("multi-section")

	// Add multiple sections
	c.AddSection(SectionNodeTable, "nodes:paper", []byte("node data 1"))
	c.AddSection(SectionNodeTable, "nodes:author", []byte("node data 2"))
	c.AddSection(SectionEdgeTable, "edges:cites", []byte("edge data"))
	c.AddSection(SectionFeature, "features:paper:x", []byte("feature data"))
	c.AddSection(SectionSplit, "splits:paper:y", []byte("split data"))

	data, _ := c.Encode()
	decoded, _ := Decode(data)

	// Verify section counts by kind
	nodeTables := decoded.GetSectionsByKind(SectionNodeTable)
	if len(nodeTables) != 2 {
		t.Errorf("Expected 2 node tables, got %d", len(nodeTables))
	}

	edgeTables := decoded.GetSectionsByKind(SectionEdgeTable)
	if len(edgeTables) != 1 {
		t.Errorf("Expected 1 edge table, got %d", len(edgeTables))
	}

	features := decoded.GetSectionsByKind(SectionFeature)
	if len(features) != 1 {
		t.Errorf("Expected 1 feature section, got %d", len(features))
	}

	splits := decoded.GetSectionsByKind(SectionSplit)
	if len(splits) != 1 {
		t.Errorf("Expected 1 split section, got %d", len(splits))
	}

	// Verify GetSection by name
	paperNodes := decoded.GetSection("nodes:paper")
	if paperNodes == nil {
		t.Fatal("Expected to find nodes:paper section")
	}
	if string(paperNodes.Body) != "node data 1" {
		t.Errorf("Body = %q, want %q", string(paperNodes.Body), "node data 1")
	}
}

func TestSectionKindString(t *testing.T) {
	tests := []struct {
		kind SectionKind
		want string
	}{
		{SectionMeta, "meta"},
		{SectionNodeTable, "node_table"},
		{SectionEdgeTable, "edge_table"},
		{SectionFeature, "feature"},
		{SectionSplit, "split"},
		{SectionAux, "aux"},
		{SectionKind(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.kind.String(); got != tt.want {
			t.Errorf("SectionKind(%d).String() = %q, want %q", tt.kind, got, tt.want)
		}
	}
}

func TestDTypeString(t *testing.T) {
	tests := []struct {
		dtype DType
		want  string
	}{
		{DTypeFloat32, "float32"},
		{DTypeFloat64, "float64"},
		{DTypeInt32, "int32"},
		{DTypeInt64, "int64"},
		{DTypeUint32, "uint32"},
		{DTypeUint64, "uint64"},
		{DTypeBool, "bool"},
		{DType(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.dtype.String(); got != tt.want {
			t.Errorf("DType(%d).String() = %q, want %q", tt.dtype, got, tt.want)
		}
	}
}

func TestDTypeByteSize(t *testing.T) {
	tests := []struct {
		dtype DType
		want  int
	}{
		{DTypeFloat32, 4},
		{DTypeFloat64, 8},
		{DTypeInt32, 4},
		{DTypeInt64, 8},
		{DTypeUint32, 4},
		{DTypeUint64, 8},
		{DTypeBool, 1},
		{DType(99), 0},
	}
	for _, tt := range tests {
		if got := tt.dtype.ByteSize(); got != tt.want {
			t.Errorf("DType(%d).ByteSize() = %d, want %d", tt.dtype, got, tt.want)
		}
	}
}

func TestContainerFlags(t *testing.T) {
	c := NewContainer("flags-test")

	// Initially all false
	if c.IsHeterogeneous() {
		t.Error("Expected IsHeterogeneous = false initially")
	}
	if c.IsTemporal() {
		t.Error("Expected IsTemporal = false initially")
	}
	if c.HasCSR() {
		t.Error("Expected HasCSR = false initially")
	}

	// Set heterogeneous
	c.SetHeterogeneous(true)
	if !c.IsHeterogeneous() {
		t.Error("Expected IsHeterogeneous = true after setting")
	}

	// Set temporal
	c.SetTemporal(true)
	if !c.IsTemporal() {
		t.Error("Expected IsTemporal = true after setting")
	}

	// Toggle back
	c.SetHeterogeneous(false)
	if c.IsHeterogeneous() {
		t.Error("Expected IsHeterogeneous = false after unsetting")
	}
}

func TestContainerEncodeDecodeWithIO(t *testing.T) {
	c := NewContainer("io-test")
	c.AddSection(SectionNodeTable, "nodes", []byte("node bytes"))

	// Encode to buffer
	var buf bytes.Buffer
	if err := c.EncodeTo(&buf); err != nil {
		t.Fatalf("EncodeTo failed: %v", err)
	}

	// Decode from buffer
	decoded, err := DecodeFrom(&buf)
	if err != nil {
		t.Fatalf("DecodeFrom failed: %v", err)
	}

	if decoded.Meta().DatasetName != "io-test" {
		t.Error("DatasetName mismatch")
	}
}

func TestEmptyContainer(t *testing.T) {
	c := NewContainer("empty")

	data, err := c.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Meta().DatasetName != "empty" {
		t.Error("DatasetName mismatch")
	}
	if len(decoded.Sections) != 0 {
		t.Errorf("Expected 0 sections, got %d", len(decoded.Sections))
	}
}

func TestInvalidMagic(t *testing.T) {
	// Invalid magic bytes
	data := []byte{'X', 'Y', 'Z', 'W', 0x01, 0x00, 0x00}

	_, err := Decode(data)
	if err != ErrInvalidMagic {
		t.Errorf("Expected ErrInvalidMagic, got %v", err)
	}
}

func TestInvalidVersion(t *testing.T) {
	// Valid magic but invalid version
	data := []byte{'S', 'J', 'G', 'N', 0xFF, 0x00, 0x00}

	_, err := Decode(data)
	if err != ErrInvalidVersion {
		t.Errorf("Expected ErrInvalidVersion, got %v", err)
	}
}

func TestTruncatedData(t *testing.T) {
	c := NewContainer("truncated")
	c.AddSection(SectionNodeTable, "nodes", []byte("data"))

	data, _ := c.Encode()

	// Truncate data
	_, err := Decode(data[:len(data)/2])
	if err == nil {
		t.Error("Expected error for truncated data")
	}
}

func BenchmarkContainerEncode(b *testing.B) {
	c := NewContainer("benchmark")
	c.SetHeterogeneous(true)
	c.AddNodeType("paper", 100000)
	c.AddNodeType("author", 50000)
	c.AddEdgeType("paper", "cites", "paper")
	c.AddFeature("paper", "x", DTypeFloat32, []int{128})

	// Add some sections with realistic sizes
	nodeData := make([]byte, 10000)
	c.AddSection(SectionNodeTable, "nodes:paper", nodeData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Encode()
	}
}

func BenchmarkContainerDecode(b *testing.B) {
	c := NewContainer("benchmark")
	c.SetHeterogeneous(true)
	c.AddNodeType("paper", 100000)
	c.AddSection(SectionNodeTable, "nodes:paper", make([]byte, 10000))

	data, _ := c.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Decode(data)
	}
}

// Realistic GNN benchmarks

func BenchmarkFeatureWrite1000x128(b *testing.B) {
	// Simulate writing 1000 node embeddings of dimension 128
	data := make([]float32, 1000*128)
	for i := range data {
		data[i] = float32(i) * 0.001
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := NewFeatureWriter("embeddings", DTypeFloat32, []int{128})
		w.WriteHeader(1000)
		w.WriteFloat32Tensor(data)
		_ = w.Bytes()
	}
}

func BenchmarkFeatureRead1000x128(b *testing.B) {
	// Prepare data
	data := make([]float32, 1000*128)
	for i := range data {
		data[i] = float32(i) * 0.001
	}

	w := NewFeatureWriter("embeddings", DTypeFloat32, []int{128})
	w.WriteHeader(1000)
	w.WriteFloat32Tensor(data)
	encoded := w.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := NewFeatureReader(encoded)
		r.ReadFloat32Tensor()
	}
}

func BenchmarkCOOToCSR10000Edges(b *testing.B) {
	// Simulate converting 10000 edges from COO to CSR
	numNodes := int64(1000)
	numEdges := 10000
	src := make([]int64, numEdges)
	dst := make([]int64, numEdges)
	for i := 0; i < numEdges; i++ {
		src[i] = int64(i % int(numNodes))
		dst[i] = int64((i + 1) % int(numNodes))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		COOToCSR(numNodes, src, dst)
	}
}

func BenchmarkAddSelfLoops1000Nodes(b *testing.B) {
	numNodes := int64(1000)
	src := make([]int64, 5000)
	dst := make([]int64, 5000)
	for i := 0; i < 5000; i++ {
		src[i] = int64(i % int(numNodes))
		dst[i] = int64((i + 1) % int(numNodes))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AddSelfLoops(numNodes, src, dst)
	}
}

func BenchmarkToUndirected5000Edges(b *testing.B) {
	src := make([]int64, 5000)
	dst := make([]int64, 5000)
	for i := 0; i < 5000; i++ {
		src[i] = int64(i % 1000)
		dst[i] = int64((i + 1) % 1000)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ToUndirected(src, dst)
	}
}

func BenchmarkSplitIndices(b *testing.B) {
	train := make([]int64, 6000)
	val := make([]int64, 2000)
	test := make([]int64, 2000)
	for i := range train {
		train[i] = int64(i)
	}
	for i := range val {
		val[i] = int64(6000 + i)
	}
	for i := range test {
		test[i] = int64(8000 + i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := NewSplitWriter()
		w.WriteIndices(train, val, test)
		_ = w.Bytes()
	}
}

func BenchmarkAuxCSR1000Nodes(b *testing.B) {
	numNodes := int64(1000)
	src := make([]int64, 10000)
	dst := make([]int64, 10000)
	for i := 0; i < 10000; i++ {
		src[i] = int64(i % int(numNodes))
		dst[i] = int64((i + 1) % int(numNodes))
	}
	indptr, indices := COOToCSR(numNodes, src, dst)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := NewAuxWriter()
		w.WriteCSR(numNodes, indptr, indices)
		_ = w.Bytes()
	}
}

// Size comparison - output sizes for same data
func TestFeatureSizeComparison(t *testing.T) {
	// 1000 nodes x 128-dim embeddings
	data := make([]float32, 1000*128)
	for i := range data {
		data[i] = float32(i) * 0.001
	}

	// GNN format
	w := NewFeatureWriter("embeddings", DTypeFloat32, []int{128})
	w.WriteHeader(1000)
	w.WriteFloat32Tensor(data)
	gnnSize := len(w.Bytes())

	// Raw binary (just float32 bytes, no metadata)
	rawSize := len(data) * 4

	// Calculate overhead
	overhead := float64(gnnSize-rawSize) / float64(rawSize) * 100

	t.Logf("Feature tensor 1000x128 float32:")
	t.Logf("  Raw binary:  %d bytes", rawSize)
	t.Logf("  GNN format:  %d bytes", gnnSize)
	t.Logf("  Overhead:    %.2f%% (for metadata: name, dtype, shape, numRows)", overhead)
}

func TestEdgeListSizeComparison(t *testing.T) {
	// 10000 edges
	numEdges := 10000
	numNodes := int64(1000)

	// Build edge list
	src := make([]int64, numEdges)
	dst := make([]int64, numEdges)
	for i := 0; i < numEdges; i++ {
		src[i] = int64(i % int(numNodes))
		dst[i] = int64((i + 1) % int(numNodes))
	}

	// GNN EdgeTable format (streaming, supports properties)
	ew := NewEdgeTableWriter("connects")
	ew.WriteHeader()
	for i := 0; i < numEdges; i++ {
		ew.WriteEdge(src[i], dst[i], nil)
	}
	ew.Close()
	edgeTableSize := len(ew.Bytes())

	// GNN Aux CSR format (compact, structure only)
	indptr, indices := COOToCSR(numNodes, src, dst)
	aw := NewAuxWriter()
	aw.WriteCSR(numNodes, indptr, indices)
	auxCSRSize := len(aw.Bytes())

	// Raw COO as int64 pairs
	rawCOOSize := numEdges * 16 // 2 x int64 per edge

	// Raw CSR
	rawCSRSize := len(indptr)*8 + len(indices)*8

	t.Logf("Edge list with %d edges, %d nodes:", numEdges, numNodes)
	t.Logf("  Raw COO (2x int64):     %6d bytes", rawCOOSize)
	t.Logf("  Raw CSR:                %6d bytes", rawCSRSize)
	t.Logf("  GNN Aux (CSR+metadata): %6d bytes (%.1f%% overhead)", auxCSRSize, float64(auxCSRSize-rawCSRSize)/float64(rawCSRSize)*100)
	t.Logf("  GNN EdgeTable:          %6d bytes (streaming format, supports edge properties)", edgeTableSize)
	t.Logf("")
	t.Logf("Use Aux for compact structure, EdgeTable for rich edge data with properties")
}

func TestCSRConversionThroughput(t *testing.T) {
	sizes := []int{1000, 10000, 100000}

	for _, numEdges := range sizes {
		numNodes := int64(numEdges / 10)
		src := make([]int64, numEdges)
		dst := make([]int64, numEdges)
		for i := 0; i < numEdges; i++ {
			src[i] = int64(i % int(numNodes))
			dst[i] = int64((i + 1) % int(numNodes))
		}

		// Time conversion
		start := testing.Benchmark(func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				COOToCSR(numNodes, src, dst)
			}
		})

		edgesPerSec := float64(numEdges) / (float64(start.T.Nanoseconds()) / float64(start.N) / 1e9)
		t.Logf("COOToCSR %d edges: %.0f edges/sec (%.2f µs)", numEdges, edgesPerSec, float64(start.T.Nanoseconds())/float64(start.N)/1000)
	}
}
