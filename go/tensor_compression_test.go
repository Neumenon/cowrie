package cowrie

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// TestTensorCompressionRealWorld demonstrates Cowrie's compression advantage over JSON
// for real-world tensor data where the benefits are most visible.
//
// Expected results:
//   Test 1: 1024 float array      - JSON: ~10KB,  Cowrie: ~4KB    (~60% smaller)
//   Test 2: 4096×1 float vector   - JSON: ~40KB,  Cowrie: ~16KB   (~60% smaller)
//   Test 3: 256×256 float matrix  - JSON: ~800KB, Cowrie: ~260KB  (~68% smaller)
//   Test 4: PTShard embeddings    - JSON: ~3-6MB, Cowrie: ~1-2MB  (~67% smaller)

func TestTensorCompression_1024FloatArray(t *testing.T) {
	// Test 1: 1024 float array
	// JSON represents each float as text: "0.12345678" = ~10-12 chars
	// Cowrie stores raw binary: 4 bytes per float32

	data := make([]float32, 1024)
	for i := range data {
		data[i] = rand.Float32()
	}

	// Create JSON representation (array of numbers)
	jsonData, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	// Create Cowrie tensor
	tensorBytes := make([]byte, len(data)*4)
	for i, v := range data {
		binary.LittleEndian.PutUint32(tensorBytes[i*4:], math.Float32bits(v))
	}
	cowrieValue := Tensor(DTypeFloat32, []uint64{1024}, tensorBytes)
	cowrieData, err := Encode(cowrieValue)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Calculate compression ratio
	jsonSize := len(jsonData)
	cowrieSize := len(cowrieData)
	savings := float64(jsonSize-cowrieSize) / float64(jsonSize) * 100

	t.Logf("Test 1: 1024 float array")
	t.Logf("  JSON size:  %d bytes (~%.1f KB)", jsonSize, float64(jsonSize)/1024)
	t.Logf("  Cowrie size: %d bytes (~%.1f KB)", cowrieSize, float64(cowrieSize)/1024)
	t.Logf("  Savings:    %.1f%%", savings)

	// Cowrie should be at least 50% smaller
	if savings < 50 {
		t.Errorf("Expected at least 50%% savings, got %.1f%%", savings)
	}
}

func TestTensorCompression_4096Vector(t *testing.T) {
	// Test 2: 4096×1 float vector
	// Common size for word embeddings (e.g., GPT-2 hidden dimension)

	data := make([]float32, 4096)
	for i := range data {
		data[i] = rand.Float32()*2 - 1 // Range [-1, 1]
	}

	// JSON representation
	jsonData, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	// Cowrie tensor (4096×1 vector shape)
	tensorBytes := make([]byte, len(data)*4)
	for i, v := range data {
		binary.LittleEndian.PutUint32(tensorBytes[i*4:], math.Float32bits(v))
	}
	cowrieValue := Tensor(DTypeFloat32, []uint64{4096, 1}, tensorBytes)
	cowrieData, err := Encode(cowrieValue)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	jsonSize := len(jsonData)
	cowrieSize := len(cowrieData)
	savings := float64(jsonSize-cowrieSize) / float64(jsonSize) * 100

	t.Logf("Test 2: 4096×1 float vector")
	t.Logf("  JSON size:  %d bytes (~%.1f KB)", jsonSize, float64(jsonSize)/1024)
	t.Logf("  Cowrie size: %d bytes (~%.1f KB)", cowrieSize, float64(cowrieSize)/1024)
	t.Logf("  Savings:    %.1f%%", savings)

	if savings < 55 {
		t.Errorf("Expected at least 55%% savings, got %.1f%%", savings)
	}
}

func TestTensorCompression_256x256Matrix(t *testing.T) {
	// Test 3: 256×256 float32 matrix
	// Common size for attention weight matrices

	rows, cols := 256, 256
	data := make([]float32, rows*cols)
	for i := range data {
		data[i] = rand.Float32()*2 - 1
	}

	// JSON representation (nested arrays)
	matrix := make([][]float32, rows)
	for i := 0; i < rows; i++ {
		matrix[i] = data[i*cols : (i+1)*cols]
	}
	jsonData, err := json.Marshal(matrix)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	// Cowrie tensor (256×256 matrix)
	tensorBytes := make([]byte, len(data)*4)
	for i, v := range data {
		binary.LittleEndian.PutUint32(tensorBytes[i*4:], math.Float32bits(v))
	}
	cowrieValue := Tensor(DTypeFloat32, []uint64{256, 256}, tensorBytes)
	cowrieData, err := Encode(cowrieValue)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	jsonSize := len(jsonData)
	cowrieSize := len(cowrieData)
	savings := float64(jsonSize-cowrieSize) / float64(jsonSize) * 100

	t.Logf("Test 3: 256×256 float32 matrix")
	t.Logf("  JSON size:  %d bytes (~%.1f KB)", jsonSize, float64(jsonSize)/1024)
	t.Logf("  Cowrie size: %d bytes (~%.1f KB)", cowrieSize, float64(cowrieSize)/1024)
	t.Logf("  Savings:    %.1f%%", savings)

	if savings < 60 {
		t.Errorf("Expected at least 60%% savings, got %.1f%%", savings)
	}
}

func TestTensorCompression_PTShardEmbeddings(t *testing.T) {
	// Test 4: PTShard-style training samples
	// 100 samples, each with a 512-dim embedding + metadata
	// This simulates a training shard with embedding data

	type Sample struct {
		ID        int       `json:"id"`
		Label     int       `json:"label"`
		Embedding []float32 `json:"embedding"`
	}

	samples := make([]Sample, 100)
	for i := range samples {
		embedding := make([]float32, 512)
		for j := range embedding {
			embedding[j] = rand.Float32()*2 - 1
		}
		samples[i] = Sample{
			ID:        i,
			Label:     i % 10,
			Embedding: embedding,
		}
	}

	// JSON representation
	jsonData, err := json.Marshal(samples)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	// Cowrie representation using structured objects with tensor embeddings
	cowrieSamples := make([]*Value, len(samples))
	for i, s := range samples {
		// Encode embedding as tensor
		tensorBytes := make([]byte, len(s.Embedding)*4)
		for j, v := range s.Embedding {
			binary.LittleEndian.PutUint32(tensorBytes[j*4:], math.Float32bits(v))
		}

		cowrieSamples[i] = Object(
			Member{Key: "id", Value: Int64(int64(s.ID))},
			Member{Key: "label", Value: Int64(int64(s.Label))},
			Member{Key: "embedding", Value: Tensor(DTypeFloat32, []uint64{512}, tensorBytes)},
		)
	}
	cowrieData, err := Encode(Array(cowrieSamples...))
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	jsonSize := len(jsonData)
	cowrieSize := len(cowrieData)
	savings := float64(jsonSize-cowrieSize) / float64(jsonSize) * 100

	t.Logf("Test 4: PTShard (100 samples × 512-dim embeddings)")
	t.Logf("  JSON size:  %d bytes (~%.2f MB)", jsonSize, float64(jsonSize)/1024/1024)
	t.Logf("  Cowrie size: %d bytes (~%.2f MB)", cowrieSize, float64(cowrieSize)/1024/1024)
	t.Logf("  Savings:    %.1f%%", savings)

	if savings < 60 {
		t.Errorf("Expected at least 60%% savings, got %.1f%%", savings)
	}
}

// TestTensorCompressionSummary runs all tests and produces a summary table
func TestTensorCompressionSummary(t *testing.T) {
	fmt.Println()
	fmt.Println("╔═════════════════════════════════════════════════════════════════╗")
	fmt.Println("║        Cowrie vs JSON Tensor Compression Comparison              ║")
	fmt.Println("╠═════════════════════════════════════════════════════════════════╣")
	fmt.Println("║  Test                      │  JSON       │  Cowrie     │ Savings ║")
	fmt.Println("╠════════════════════════════╪═════════════╪════════════╪═════════╣")

	tests := []struct {
		name  string
		genFn func() (jsonBytes, cowrieBytes []byte, err error)
	}{
		{
			name: "1024 float array",
			genFn: func() ([]byte, []byte, error) {
				data := make([]float32, 1024)
				for i := range data {
					data[i] = rand.Float32()
				}
				jsonBytes, _ := json.Marshal(data)
				tensorBytes := float32ToBytes(data)
				cowrieVal := Tensor(DTypeFloat32, []uint64{1024}, tensorBytes)
				cowrieBytes, err := Encode(cowrieVal)
				return jsonBytes, cowrieBytes, err
			},
		},
		{
			name: "4096×1 float vector",
			genFn: func() ([]byte, []byte, error) {
				data := make([]float32, 4096)
				for i := range data {
					data[i] = rand.Float32()*2 - 1
				}
				jsonBytes, _ := json.Marshal(data)
				tensorBytes := float32ToBytes(data)
				cowrieVal := Tensor(DTypeFloat32, []uint64{4096, 1}, tensorBytes)
				cowrieBytes, err := Encode(cowrieVal)
				return jsonBytes, cowrieBytes, err
			},
		},
		{
			name: "256×256 float matrix",
			genFn: func() ([]byte, []byte, error) {
				data := make([]float32, 256*256)
				for i := range data {
					data[i] = rand.Float32()*2 - 1
				}
				matrix := make([][]float32, 256)
				for i := 0; i < 256; i++ {
					matrix[i] = data[i*256 : (i+1)*256]
				}
				jsonBytes, _ := json.Marshal(matrix)
				tensorBytes := float32ToBytes(data)
				cowrieVal := Tensor(DTypeFloat32, []uint64{256, 256}, tensorBytes)
				cowrieBytes, err := Encode(cowrieVal)
				return jsonBytes, cowrieBytes, err
			},
		},
		{
			name: "100×512 embeddings",
			genFn: func() ([]byte, []byte, error) {
				type Sample struct {
					ID        int       `json:"id"`
					Embedding []float32 `json:"embedding"`
				}
				samples := make([]Sample, 100)
				for i := range samples {
					embedding := make([]float32, 512)
					for j := range embedding {
						embedding[j] = rand.Float32()*2 - 1
					}
					samples[i] = Sample{ID: i, Embedding: embedding}
				}
				jsonBytes, _ := json.Marshal(samples)

				cowrieSamples := make([]*Value, 100)
				for i, s := range samples {
					tensorBytes := float32ToBytes(s.Embedding)
					cowrieSamples[i] = Object(
						Member{Key: "id", Value: Int64(int64(s.ID))},
						Member{Key: "embedding", Value: Tensor(DTypeFloat32, []uint64{512}, tensorBytes)},
					)
				}
				cowrieBytes, err := Encode(Array(cowrieSamples...))
				return jsonBytes, cowrieBytes, err
			},
		},
	}

	for _, tc := range tests {
		jsonBytes, cowrieBytes, err := tc.genFn()
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}

		jsonSize := len(jsonBytes)
		cowrieSize := len(cowrieBytes)
		savings := float64(jsonSize-cowrieSize) / float64(jsonSize) * 100

		fmt.Printf("║  %-24s │ %7s     │ %7s    │ %5.1f%%  ║\n",
			tc.name,
			formatBytes(jsonSize),
			formatBytes(cowrieSize),
			savings)
	}

	fmt.Println("╚═════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Key insight: JSON encodes each float as ~10-12 ASCII characters,")
	fmt.Println("while Cowrie uses native binary (4 bytes for float32).")
	fmt.Println()
}

// Helper function to convert float32 slice to bytes
func float32ToBytes(data []float32) []byte {
	result := make([]byte, len(data)*4)
	for i, v := range data {
		binary.LittleEndian.PutUint32(result[i*4:], math.Float32bits(v))
	}
	return result
}

// Helper function to format bytes in human-readable form
func formatBytes(b int) string {
	const (
		KB = 1024
		MB = 1024 * KB
	)
	switch {
	case b >= MB:
		return fmt.Sprintf("%.1fMB", float64(b)/MB)
	case b >= KB:
		return fmt.Sprintf("%.1fKB", float64(b)/KB)
	default:
		return fmt.Sprintf("%dB", b)
	}
}

// TestTensorRoundtripVerification ensures encoded tensors can be decoded correctly
func TestTensorRoundtripVerification(t *testing.T) {
	original := make([]float32, 1024)
	for i := range original {
		original[i] = rand.Float32()
	}

	// Encode
	tensorBytes := float32ToBytes(original)
	cowrieValue := Tensor(DTypeFloat32, []uint64{1024}, tensorBytes)
	encoded, err := Encode(cowrieValue)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Type() != TypeTensor {
		t.Fatalf("Expected TypeTensor, got %v", decoded.Type())
	}

	tensorData := decoded.Tensor()
	if tensorData.DType != DTypeFloat32 {
		t.Fatalf("Expected DTypeFloat32, got %v", tensorData.DType)
	}

	// Verify dimensions
	if len(tensorData.Dims) != 1 || tensorData.Dims[0] != 1024 {
		t.Fatalf("Expected dims [1024], got %v", tensorData.Dims)
	}

	// Verify data
	for i := 0; i < 1024; i++ {
		bits := binary.LittleEndian.Uint32(tensorData.Data[i*4:])
		recovered := math.Float32frombits(bits)
		if recovered != original[i] {
			t.Errorf("Mismatch at index %d: expected %v, got %v", i, original[i], recovered)
		}
	}

	t.Logf("Roundtrip verification passed: 1024 floats encoded/decoded losslessly")
}

// BenchmarkTensorEncodingSpeed compares encoding performance
func BenchmarkTensorEncodingSpeed(b *testing.B) {
	data := make([]float32, 65536) // 64K floats = 256KB tensor
	for i := range data {
		data[i] = rand.Float32()
	}
	tensorBytes := float32ToBytes(data)

	b.Run("JSON_Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			json.Marshal(data)
		}
	})

	b.Run("Cowrie_Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			v := Tensor(DTypeFloat32, []uint64{65536}, tensorBytes)
			Encode(v)
		}
	})
}

// BenchmarkTensorDecodingSpeed compares decoding performance
func BenchmarkTensorDecodingSpeed(b *testing.B) {
	data := make([]float32, 65536)
	for i := range data {
		data[i] = rand.Float32()
	}

	jsonBytes, _ := json.Marshal(data)
	tensorBytes := float32ToBytes(data)
	cowrieVal := Tensor(DTypeFloat32, []uint64{65536}, tensorBytes)
	cowrieBytes, _ := Encode(cowrieVal)

	b.Run("JSON_Decode", func(b *testing.B) {
		var result []float32
		for i := 0; i < b.N; i++ {
			json.Unmarshal(jsonBytes, &result)
		}
	})

	b.Run("Cowrie_Decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Decode(cowrieBytes)
		}
	})
}

// TestTensorFileSizes writes actual files to show real-world size differences
func TestTensorFileSizes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping file write test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "tensor_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a 512×512 matrix (1MB of float32 data)
	data := make([]float32, 512*512)
	for i := range data {
		data[i] = rand.Float32()*2 - 1
	}

	// Write as JSON
	matrix := make([][]float32, 512)
	for i := 0; i < 512; i++ {
		matrix[i] = data[i*512 : (i+1)*512]
	}
	jsonPath := filepath.Join(tmpDir, "matrix.json")
	jsonData, _ := json.Marshal(matrix)
	os.WriteFile(jsonPath, jsonData, 0644)

	// Write as Cowrie
	cowriePath := filepath.Join(tmpDir, "matrix.cowrie")
	tensorBytes := float32ToBytes(data)
	cowrieVal := Tensor(DTypeFloat32, []uint64{512, 512}, tensorBytes)
	cowrieData, _ := Encode(cowrieVal)
	os.WriteFile(cowriePath, cowrieData, 0644)

	jsonInfo, _ := os.Stat(jsonPath)
	cowrieInfo, _ := os.Stat(cowriePath)

	t.Logf("512×512 matrix file sizes:")
	t.Logf("  JSON:  %.2f MB", float64(jsonInfo.Size())/1024/1024)
	t.Logf("  Cowrie: %.2f MB", float64(cowrieInfo.Size())/1024/1024)
	t.Logf("  Ratio: %.2fx smaller", float64(jsonInfo.Size())/float64(cowrieInfo.Size()))
}
