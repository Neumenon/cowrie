package cowrie_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	cowrie "github.com/Neumenon/cowrie/go"
)

var (
	smallGen2  []byte
	mediumGen2 []byte
	largeGen2  []byte
	floatsGen2 []byte
	smallJSON  []byte
	mediumJSON []byte
	largeJSON  []byte
	floatsJSON []byte
)

func init() {
	dir := filepath.Join("..", "benchmarks", "fixtures")
	smallGen2, _ = os.ReadFile(filepath.Join(dir, "small.gen2"))
	mediumGen2, _ = os.ReadFile(filepath.Join(dir, "medium.gen2"))
	largeGen2, _ = os.ReadFile(filepath.Join(dir, "large.gen2"))
	floatsGen2, _ = os.ReadFile(filepath.Join(dir, "floats.gen2"))
	smallJSON, _ = os.ReadFile(filepath.Join(dir, "small.json"))
	mediumJSON, _ = os.ReadFile(filepath.Join(dir, "medium.json"))
	largeJSON, _ = os.ReadFile(filepath.Join(dir, "large.json"))
	floatsJSON, _ = os.ReadFile(filepath.Join(dir, "floats.json"))
}

// Gen2 Decode
func BenchmarkDecodeGen2Small(b *testing.B) {
	b.SetBytes(int64(len(smallGen2)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cowrie.Decode(smallGen2)
	}
}

func BenchmarkDecodeGen2Medium(b *testing.B) {
	b.SetBytes(int64(len(mediumGen2)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cowrie.Decode(mediumGen2)
	}
}

func BenchmarkDecodeGen2Large(b *testing.B) {
	b.SetBytes(int64(len(largeGen2)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cowrie.Decode(largeGen2)
	}
}

func BenchmarkDecodeGen2Floats(b *testing.B) {
	b.SetBytes(int64(len(floatsGen2)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cowrie.Decode(floatsGen2)
	}
}

// JSON Decode (baseline)
func BenchmarkDecodeJSONSmall(b *testing.B) {
	b.SetBytes(int64(len(smallJSON)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var v any
		json.Unmarshal(smallJSON, &v)
	}
}

func BenchmarkDecodeJSONMedium(b *testing.B) {
	b.SetBytes(int64(len(mediumJSON)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var v any
		json.Unmarshal(mediumJSON, &v)
	}
}

func BenchmarkDecodeJSONLarge(b *testing.B) {
	b.SetBytes(int64(len(largeJSON)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var v any
		json.Unmarshal(largeJSON, &v)
	}
}

func BenchmarkDecodeJSONFloats(b *testing.B) {
	b.SetBytes(int64(len(floatsJSON)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var v any
		json.Unmarshal(floatsJSON, &v)
	}
}
