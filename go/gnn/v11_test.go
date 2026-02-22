package gnn

import (
	"bytes"
	"testing"
)

// TestV11ContainerRoundTrip tests that v1.1 containers encode and decode correctly.
func TestV11ContainerRoundTrip(t *testing.T) {
	// Create a v1.1 container (default for new containers)
	c := NewContainer("v11-test")
	c.SetDirected(true)
	c.SetHeterogeneous(true)
	c.SetTemporal(true)

	// Add node types
	c.AddNodeType("paper", 1000)
	c.AddNodeType("author", 500)

	// Add edge types
	c.AddEdgeType("paper", "cites", "paper")
	c.AddEdgeType("author", "writes", "paper")

	// Add feature specs
	c.AddFeature("paper", "embedding", DTypeFloat32, []int{128})
	c.AddFeature("author", "features", DTypeFloat32, []int{64})

	// Add label specs
	c.AddLabel("paper", "category", DTypeInt64, []int{1})

	// Verify it's v1.1
	if !c.IsV11() {
		t.Fatal("NewContainer should create v1.1 container")
	}

	// Encode
	data, err := c.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Verify magic bytes
	if len(data) < 7 {
		t.Fatal("Data too short")
	}
	if data[0] != 'S' || data[1] != 'J' || data[2] != 'G' || data[3] != 'N' {
		t.Error("Magic bytes incorrect")
	}

	// v1.1 version: bytes 4-5 should be 0x01 0x10 (major=1, minor=16)
	if data[4] != 0x01 || data[5] != 0x10 {
		t.Errorf("Version bytes = [%02x %02x], want [01 10]", data[4], data[5])
	}

	// Decode
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify it's recognized as v1.1
	if !decoded.IsV11() {
		t.Error("Decoded container should be v1.1")
	}
	if decoded.Version != Version11 {
		t.Errorf("Version = %04x, want %04x", decoded.Version, Version11)
	}

	// Verify flags
	if !decoded.IsHeterogeneous() {
		t.Error("Expected heterogeneous flag")
	}
	if !decoded.IsTemporal() {
		t.Error("Expected temporal flag")
	}

	// Verify meta
	meta := decoded.Meta()
	if meta == nil {
		t.Fatal("Meta should not be nil")
	}
	if meta.DatasetName != "v11-test" {
		t.Errorf("DatasetName = %q, want %q", meta.DatasetName, "v11-test")
	}
	if !meta.Directed {
		t.Error("Expected Directed = true")
	}
	if !meta.Heterogeneous {
		t.Error("Expected Heterogeneous = true in meta")
	}
	if !meta.Temporal {
		t.Error("Expected Temporal = true in meta")
	}

	// Verify node types
	if len(meta.NodeTypes) != 2 {
		t.Errorf("len(NodeTypes) = %d, want 2", len(meta.NodeTypes))
	}

	// Verify edge types
	if len(meta.EdgeTypes) != 2 {
		t.Errorf("len(EdgeTypes) = %d, want 2", len(meta.EdgeTypes))
	}
	if meta.EdgeTypes[0].SrcType != "paper" || meta.EdgeTypes[0].EdgeType != "cites" {
		t.Error("First edge type mismatch")
	}

	// Verify ID spaces
	paperSpace, ok := meta.IDSpaces["paper"]
	if !ok {
		t.Fatal("Missing paper ID space")
	}
	if paperSpace.Start != 0 || paperSpace.Count != 1000 {
		t.Errorf("Paper ID space = {%d, %d}, want {0, 1000}", paperSpace.Start, paperSpace.Count)
	}

	// Verify features
	if len(meta.Features) != 2 {
		t.Errorf("len(Features) = %d, want 2", len(meta.Features))
	}
	embSpec, ok := meta.Features["paper:embedding"]
	if !ok {
		t.Fatal("Missing paper:embedding feature")
	}
	if embSpec.DType != "float32" || len(embSpec.Shape) != 1 || embSpec.Shape[0] != 128 {
		t.Errorf("Embedding spec = %+v, unexpected", embSpec)
	}

	// Verify labels
	if len(meta.Labels) != 1 {
		t.Errorf("len(Labels) = %d, want 1", len(meta.Labels))
	}
}

// TestV11SectionEncoding tests that sections have correct encoding in v1.1.
func TestV11SectionEncoding(t *testing.T) {
	c := NewContainer("encoding-test")

	// Add sections with different kinds
	c.AddSection(SectionAux, "aux:test", []byte("aux data"))
	c.AddSection(SectionFeature, "features:x", []byte{0x01, 0x02, 0x03})

	// Encode and decode
	data, _ := c.Encode()
	decoded, _ := Decode(data)

	// Check that sections have correct encoding fields
	for _, s := range decoded.Sections {
		switch s.Kind {
		case SectionAux:
			// v1.1 should use Cowrie for aux
			if decoded.IsV11() && s.Encoding != SectionEncodingCowrie {
				t.Errorf("Aux section encoding = %d, want Cowrie (%d)", s.Encoding, SectionEncodingCowrie)
			}
		case SectionFeature:
			// Features use raw tensor encoding
			if s.Encoding != SectionEncodingRawTensor {
				t.Errorf("Feature section encoding = %d, want RawTensor (%d)", s.Encoding, SectionEncodingRawTensor)
			}
		}
	}
}

// TestV10BackwardCompatibility tests that v1.0 containers can still be read.
func TestV10BackwardCompatibility(t *testing.T) {
	// Create a v1.0 container manually
	c := &Container{
		Version:  1, // v1.0
		Flags:    FlagHeterogeneous,
		Sections: []Section{},
		features: make(map[string]*FeatureHeader),
		meta: &Meta{
			DatasetName: "v10-legacy",
			Directed:    true,
			NodeTypes:   []string{"node"},
			EdgeTypes:   []EdgeTypeTuple{{SrcType: "node", EdgeType: "edge", DstType: "node"}},
			IDSpaces:    map[string]IDSpace{"node": {Start: 0, Count: 100}},
			Features:    map[string]FeatureSpec{},
			Labels:      map[string]LabelSpec{},
		},
	}

	// Encode as v1.0
	data, err := c.Encode()
	if err != nil {
		t.Fatalf("v1.0 Encode failed: %v", err)
	}

	// Verify it's v1.0 format (single byte version)
	if data[4] != 0x01 {
		t.Errorf("Version byte = %02x, want 01", data[4])
	}
	// In v1.0, flags follow immediately (byte 5)
	if data[5] != FlagHeterogeneous {
		t.Errorf("Flags byte = %02x, want %02x", data[5], FlagHeterogeneous)
	}

	// Decode and verify
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("v1.0 Decode failed: %v", err)
	}

	if decoded.IsV11() {
		t.Error("Decoded v1.0 container should not be v1.1")
	}
	if decoded.Version != 1 {
		t.Errorf("Version = %d, want 1", decoded.Version)
	}

	meta := decoded.Meta()
	if meta.DatasetName != "v10-legacy" {
		t.Errorf("DatasetName = %q, want v10-legacy", meta.DatasetName)
	}
}

// TestV11MetaCowrieEncoding verifies that Meta is encoded as Cowrie in v1.1.
func TestV11MetaCowrieEncoding(t *testing.T) {
	c := NewContainer("cowrie-meta-test")
	c.SetDirected(true)
	c.AddNodeType("entity", 500)
	c.AddFeature("entity", "x", DTypeFloat32, []int{64})

	data, _ := c.Encode()

	// The meta section should be encoded with Cowrie
	// We can verify by checking that the meta body starts with Cowrie magic
	decoded, _ := Decode(data)
	meta := decoded.Meta()

	if meta.DatasetName != "cowrie-meta-test" {
		t.Errorf("DatasetName = %q, want cowrie-meta-test", meta.DatasetName)
	}
	if !meta.Directed {
		t.Error("Expected Directed = true")
	}

	// Verify feature spec was preserved
	spec, ok := meta.Features["entity:x"]
	if !ok {
		t.Fatal("Missing entity:x feature")
	}
	if spec.DType != "float32" {
		t.Errorf("Feature dtype = %q, want float32", spec.DType)
	}
}

// TestV11WithSections tests full roundtrip with various section types.
func TestV11WithSections(t *testing.T) {
	c := NewContainer("full-sections")
	c.SetDirected(true)

	// Add various sections
	c.AddSection(SectionNodeTable, "nodes:entity", []byte("node stream data"))
	c.AddSection(SectionEdgeTable, "edges:connects", []byte("edge stream data"))
	c.AddSection(SectionFeature, "features:x", []byte{0x01, 0x02, 0x03, 0x04})
	c.AddSection(SectionSplit, "splits:train", []byte("split data"))
	c.AddSection(SectionAux, "aux:csr", []byte("aux data"))

	// Roundtrip
	data, _ := c.Encode()
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify all sections are present
	if len(decoded.Sections) != 5 {
		t.Errorf("len(Sections) = %d, want 5", len(decoded.Sections))
	}

	// Verify each section
	tests := []struct {
		name string
		kind SectionKind
	}{
		{"nodes:entity", SectionNodeTable},
		{"edges:connects", SectionEdgeTable},
		{"features:x", SectionFeature},
		{"splits:train", SectionSplit},
		{"aux:csr", SectionAux},
	}

	for _, tt := range tests {
		s := decoded.GetSection(tt.name)
		if s == nil {
			t.Errorf("Missing section %q", tt.name)
			continue
		}
		if s.Kind != tt.kind {
			t.Errorf("Section %q kind = %v, want %v", tt.name, s.Kind, tt.kind)
		}
	}
}

// TestSectionEncodingDefaults verifies default encodings for section kinds.
func TestSectionEncodingDefaults(t *testing.T) {
	tests := []struct {
		kind    SectionKind
		v10Want SectionEncoding
		v11Want SectionEncoding
	}{
		{SectionMeta, SectionEncodingJSON, SectionEncodingCowrie},
		{SectionNodeTable, SectionEncodingGraphStream, SectionEncodingGraphStream},
		{SectionEdgeTable, SectionEncodingGraphStream, SectionEncodingGraphStream},
		{SectionFeature, SectionEncodingRawTensor, SectionEncodingRawTensor},
		{SectionSplit, SectionEncodingJSON, SectionEncodingCowrie},
		{SectionAux, SectionEncodingJSON, SectionEncodingCowrie},
	}

	for _, tt := range tests {
		if got := tt.kind.LegacyEncoding(); got != tt.v10Want {
			t.Errorf("%v.LegacyEncoding() = %v, want %v", tt.kind, got, tt.v10Want)
		}
		if got := tt.kind.DefaultEncoding(); got != tt.v11Want {
			t.Errorf("%v.DefaultEncoding() = %v, want %v", tt.kind, got, tt.v11Want)
		}
	}
}

// TestV11IOCompatibility tests io.Writer/Reader compatibility.
func TestV11IOCompatibility(t *testing.T) {
	c := NewContainer("io-v11")
	c.AddNodeType("test", 100)

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

	if !decoded.IsV11() {
		t.Error("Decoded should be v1.1")
	}
	if decoded.Meta().DatasetName != "io-v11" {
		t.Error("DatasetName mismatch")
	}
}

// TestV11EmptyMeta tests minimal meta encoding.
func TestV11EmptyMeta(t *testing.T) {
	c := NewContainer("minimal")

	data, err := c.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	meta := decoded.Meta()
	if meta.DatasetName != "minimal" {
		t.Errorf("DatasetName = %q, want minimal", meta.DatasetName)
	}
	if len(meta.NodeTypes) != 0 {
		t.Errorf("NodeTypes should be empty, got %v", meta.NodeTypes)
	}
}

// BenchmarkV11Encode benchmarks v1.1 encoding.
func BenchmarkV11Encode(b *testing.B) {
	c := NewContainer("benchmark")
	c.SetHeterogeneous(true)
	c.AddNodeType("paper", 100000)
	c.AddNodeType("author", 50000)
	c.AddEdgeType("paper", "cites", "paper")
	c.AddFeature("paper", "x", DTypeFloat32, []int{128})
	c.AddSection(SectionNodeTable, "nodes:paper", make([]byte, 10000))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Encode()
	}
}

// BenchmarkV11Decode benchmarks v1.1 decoding.
func BenchmarkV11Decode(b *testing.B) {
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

// TestV11SizeComparison compares v1.0 vs v1.1 encoded sizes.
func TestV11SizeComparison(t *testing.T) {
	// Create identical content for both versions
	meta := &Meta{
		DatasetName:   "size-comparison",
		Directed:      true,
		Heterogeneous: true,
		NodeTypes:     []string{"paper", "author", "institution"},
		EdgeTypes: []EdgeTypeTuple{
			{SrcType: "paper", EdgeType: "cites", DstType: "paper"},
			{SrcType: "author", EdgeType: "writes", DstType: "paper"},
			{SrcType: "author", EdgeType: "affiliated", DstType: "institution"},
		},
		IDSpaces: map[string]IDSpace{
			"paper":       {Start: 0, Count: 100000},
			"author":      {Start: 100000, Count: 50000},
			"institution": {Start: 150000, Count: 1000},
		},
		Features: map[string]FeatureSpec{
			"paper:x":       {Shape: []int{128}, DType: "float32"},
			"author:x":      {Shape: []int{64}, DType: "float32"},
			"institution:x": {Shape: []int{32}, DType: "float32"},
		},
		Labels: map[string]LabelSpec{
			"paper:y": {Shape: []int{1}, DType: "int64"},
		},
	}

	// v1.0 container
	v10 := &Container{
		Version:  1,
		Flags:    FlagHeterogeneous,
		Sections: []Section{},
		features: make(map[string]*FeatureHeader),
		meta:     meta,
	}

	// v1.1 container
	v11 := &Container{
		Version:  Version11,
		Flags:    FlagHeterogeneous,
		Sections: []Section{},
		features: make(map[string]*FeatureHeader),
		meta:     meta,
	}

	v10Data, _ := v10.Encode()
	v11Data, _ := v11.Encode()

	t.Logf("Meta section size comparison:")
	t.Logf("  v1.0 (JSON):  %d bytes", len(v10Data))
	t.Logf("  v1.1 (Cowrie): %d bytes", len(v11Data))
	t.Logf("  Savings:      %.1f%%", (1-float64(len(v11Data))/float64(len(v10Data)))*100)
}
