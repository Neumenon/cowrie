package codec

import (
	"bytes"
	"fmt"
	"testing"
)

// TestStruct for benchmarking struct encoding
type TestDocument struct {
	ID       string            `json:"id"`
	Title    string            `json:"title"`
	Content  string            `json:"content"`
	Score    float64           `json:"score"`
	Vector   []float32         `json:"vector,omitempty"`
	Tags     []string          `json:"tags"`
	Metadata map[string]string `json:"metadata"`
}

type TestQueryResponse struct {
	Documents []TestDocument `json:"documents"`
	Total     int            `json:"total"`
	NextPage  string         `json:"next_page,omitempty"`
}

func generateStructResponse(n int) TestQueryResponse {
	docs := make([]TestDocument, n)
	for i := 0; i < n; i++ {
		docs[i] = TestDocument{
			ID:      fmt.Sprintf("doc_%d", i),
			Title:   fmt.Sprintf("Document Title %d", i),
			Content: fmt.Sprintf("This is document number %d with some realistic content that might appear in a search result.", i),
			Score:   float64(n-i) / float64(n),
			Vector:  make([]float32, 128), // 128-dim embedding
			Tags:    []string{"tag1", "tag2", "tag3"},
			Metadata: map[string]string{
				"author":   fmt.Sprintf("author_%d", i%10),
				"category": fmt.Sprintf("cat_%d", i%5),
			},
		}
		// Initialize vector
		for j := range docs[i].Vector {
			docs[i].Vector[j] = float32(j) * 0.01
		}
	}
	return TestQueryResponse{
		Documents: docs,
		Total:     n,
		NextPage:  "",
	}
}

func TestFastEncode_Basic(t *testing.T) {
	doc := TestDocument{
		ID:      "test-1",
		Title:   "Test Document",
		Content: "Some content here",
		Score:   0.95,
		Vector:  []float32{0.1, 0.2, 0.3},
		Tags:    []string{"a", "b"},
		Metadata: map[string]string{
			"key": "value",
		},
	}

	data, err := FastEncode(doc)
	if err != nil {
		t.Fatalf("FastEncode failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty output")
	}

	// Verify we can decode it back
	var decoded TestDocument
	if err := DecodeBytes(data, &decoded); err != nil {
		t.Fatalf("DecodeBytes failed: %v", err)
	}

	if decoded.ID != doc.ID {
		t.Errorf("ID mismatch: got %q, want %q", decoded.ID, doc.ID)
	}
	if decoded.Title != doc.Title {
		t.Errorf("Title mismatch: got %q, want %q", decoded.Title, doc.Title)
	}
	if decoded.Score != doc.Score {
		t.Errorf("Score mismatch: got %v, want %v", decoded.Score, doc.Score)
	}
}

func TestFastEncode_NestedStruct(t *testing.T) {
	resp := generateStructResponse(10)

	data, err := FastEncode(resp)
	if err != nil {
		t.Fatalf("FastEncode failed: %v", err)
	}

	var decoded TestQueryResponse
	if err := DecodeBytes(data, &decoded); err != nil {
		t.Fatalf("DecodeBytes failed: %v", err)
	}

	if decoded.Total != resp.Total {
		t.Errorf("Total mismatch: got %d, want %d", decoded.Total, resp.Total)
	}
	if len(decoded.Documents) != len(resp.Documents) {
		t.Errorf("Documents length mismatch: got %d, want %d", len(decoded.Documents), len(resp.Documents))
	}
}

// TestFastEncode_EmbeddedStruct verifies that embedded struct fields are flattened
// like the decoder does, ensuring encode/decode symmetry.
func TestFastEncode_EmbeddedStruct(t *testing.T) {
	// Define base struct
	type BaseFields struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}

	// Define struct with embedded base
	type WithEmbedded struct {
		BaseFields        // Embedded - fields should be flattened
		Score      int64  `json:"score"`
		Tag        string `json:"tag"`
	}

	original := WithEmbedded{
		BaseFields: BaseFields{
			ID:   42,
			Name: "test",
		},
		Score: 100,
		Tag:   "example",
	}

	// Encode
	data, err := FastEncode(original)
	if err != nil {
		t.Fatalf("FastEncode failed: %v", err)
	}

	// Decode back
	var decoded WithEmbedded
	if err := DecodeBytes(data, &decoded); err != nil {
		t.Fatalf("DecodeBytes failed: %v", err)
	}

	// Verify all fields round-trip correctly
	if decoded.ID != original.ID {
		t.Errorf("ID mismatch: got %d, want %d", decoded.ID, original.ID)
	}
	if decoded.Name != original.Name {
		t.Errorf("Name mismatch: got %q, want %q", decoded.Name, original.Name)
	}
	if decoded.Score != original.Score {
		t.Errorf("Score mismatch: got %d, want %d", decoded.Score, original.Score)
	}
	if decoded.Tag != original.Tag {
		t.Errorf("Tag mismatch: got %q, want %q", decoded.Tag, original.Tag)
	}
}

// TestFastEncode_DeepEmbeddedStruct verifies nested embedded structs are flattened.
func TestFastEncode_DeepEmbeddedStruct(t *testing.T) {
	type Level1 struct {
		A int64 `json:"a"`
	}

	type Level2 struct {
		Level1
		B int64 `json:"b"`
	}

	type Level3 struct {
		Level2
		C int64 `json:"c"`
	}

	original := Level3{
		Level2: Level2{
			Level1: Level1{A: 1},
			B:      2,
		},
		C: 3,
	}

	data, err := FastEncode(original)
	if err != nil {
		t.Fatalf("FastEncode failed: %v", err)
	}

	var decoded Level3
	if err := DecodeBytes(data, &decoded); err != nil {
		t.Fatalf("DecodeBytes failed: %v", err)
	}

	if decoded.A != 1 || decoded.B != 2 || decoded.C != 3 {
		t.Errorf("Deep embedded fields mismatch: got A=%d B=%d C=%d, want 1 2 3", decoded.A, decoded.B, decoded.C)
	}
}

// BenchmarkCowrieEncode_Struct benchmarks struct encoding with generic encoder
func BenchmarkCowrieEncode_Struct(b *testing.B) {
	sizes := []int{10, 100, 500}
	cowrieCodec := CowrieCodec{}

	for _, size := range sizes {
		data := generateStructResponse(size)
		b.Run(fmt.Sprintf("generic/n=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var buf bytes.Buffer
				if err := cowrieCodec.Encode(&buf, data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFastEncode_Struct benchmarks struct encoding with fast encoder
func BenchmarkFastEncode_Struct(b *testing.B) {
	sizes := []int{10, 100, 500}

	for _, size := range sizes {
		data := generateStructResponse(size)
		b.Run(fmt.Sprintf("fast/n=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := FastEncode(data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkJSONEncode_Struct benchmarks JSON struct encoding for comparison
func BenchmarkJSONEncode_Struct(b *testing.B) {
	sizes := []int{10, 100, 500}
	jsonCodec := JSONCodec{}

	for _, size := range sizes {
		data := generateStructResponse(size)
		b.Run(fmt.Sprintf("json/n=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var buf bytes.Buffer
				if err := jsonCodec.Encode(&buf, data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
