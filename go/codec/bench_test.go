package codec_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Neumenon/cowrie/go/codec"
)

// Realistic query response sizes for benchmarking
var benchSizes = []int{10, 100, 500, 1000}

// generateQueryResponse creates a realistic vectordb query response
func generateQueryResponse(n int) map[string]any {
	ids := make([]any, n)
	docs := make([]any, n)
	scores := make([]float32, n)
	meta := make([]map[string]string, n)

	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("doc_%d", i)
		docs[i] = fmt.Sprintf("This is document number %d with some realistic content that might appear in a search result.", i)
		scores[i] = float32(n-i) / float32(n) // Decreasing scores
		meta[i] = map[string]string{
			"author":   fmt.Sprintf("author_%d", i%10),
			"category": fmt.Sprintf("cat_%d", i%5),
		}
	}

	return map[string]any{
		"ids":    ids,
		"docs":   docs,
		"scores": scores,
		"meta":   meta,
		"stats":  fmt.Sprintf("%d results in 12ms", n),
		"next":   "",
	}
}

// BenchmarkJSONEncode benchmarks JSON encoding at various sizes
func BenchmarkJSONEncode(b *testing.B) {
	jsonCodec := codec.JSONCodec{}

	for _, size := range benchSizes {
		data := generateQueryResponse(size)
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
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

// BenchmarkCowrieEncode benchmarks Cowrie encoding at various sizes
func BenchmarkCowrieEncode(b *testing.B) {
	cowrieCodec := codec.CowrieCodec{}

	for _, size := range benchSizes {
		data := generateQueryResponse(size)
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
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

// BenchmarkJSONDecode benchmarks JSON decoding at various sizes
func BenchmarkJSONDecode(b *testing.B) {
	jsonCodec := codec.JSONCodec{}

	for _, size := range benchSizes {
		data := generateQueryResponse(size)
		var buf bytes.Buffer
		if err := jsonCodec.Encode(&buf, data); err != nil {
			b.Fatal(err)
		}
		encoded := buf.Bytes()

		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(encoded)))
			for i := 0; i < b.N; i++ {
				var result map[string]any
				if err := jsonCodec.Decode(bytes.NewReader(encoded), &result); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCowrieDecode benchmarks Cowrie decoding at various sizes
func BenchmarkCowrieDecode(b *testing.B) {
	cowrieCodec := codec.CowrieCodec{}

	for _, size := range benchSizes {
		data := generateQueryResponse(size)
		var buf bytes.Buffer
		if err := cowrieCodec.Encode(&buf, data); err != nil {
			b.Fatal(err)
		}
		encoded := buf.Bytes()

		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(encoded)))
			for i := 0; i < b.N; i++ {
				var result map[string]any
				if err := cowrieCodec.Decode(bytes.NewReader(encoded), &result); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRoundTrip benchmarks full encode+decode cycle
func BenchmarkRoundTrip(b *testing.B) {
	jsonCodec := codec.JSONCodec{}
	cowrieCodec := codec.CowrieCodec{}

	for _, size := range benchSizes {
		data := generateQueryResponse(size)

		b.Run(fmt.Sprintf("JSON/n=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var buf bytes.Buffer
				if err := jsonCodec.Encode(&buf, data); err != nil {
					b.Fatal(err)
				}
				var result map[string]any
				if err := jsonCodec.Decode(&buf, &result); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("Cowrie/n=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var buf bytes.Buffer
				if err := cowrieCodec.Encode(&buf, data); err != nil {
					b.Fatal(err)
				}
				var result map[string]any
				if err := cowrieCodec.Decode(bytes.NewReader(buf.Bytes()), &result); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkScoresOnly benchmarks encoding/decoding of just float32 scores
// This isolates the tensor optimization benefit
func BenchmarkScoresOnly(b *testing.B) {
	jsonCodec := codec.JSONCodec{}
	cowrieCodec := codec.CowrieCodec{}

	for _, size := range benchSizes {
		scores := make([]float32, size)
		for i := 0; i < size; i++ {
			scores[i] = float32(i) * 0.001
		}
		data := map[string]any{"scores": scores}

		b.Run(fmt.Sprintf("JSON/n=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var buf bytes.Buffer
				if err := jsonCodec.Encode(&buf, data); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("Cowrie/n=%d", size), func(b *testing.B) {
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

// TestSizeComparison prints a detailed size comparison table
func TestSizeComparison(t *testing.T) {
	jsonCodec := codec.JSONCodec{}
	cowrieCodec := codec.CowrieCodec{}

	t.Log("\n=== Size Comparison (Query Responses) ===")
	t.Log("Results | JSON Size | Cowrie Size | Savings | Savings %")
	t.Log("--------|-----------|------------|---------|----------")

	for _, size := range benchSizes {
		data := generateQueryResponse(size)

		var jsonBuf, cowrieBuf bytes.Buffer
		jsonCodec.Encode(&jsonBuf, data)
		cowrieCodec.Encode(&cowrieBuf, data)

		jsonSize := jsonBuf.Len()
		cowrieSize := cowrieBuf.Len()
		savings := jsonSize - cowrieSize
		pct := float64(savings) / float64(jsonSize) * 100

		t.Logf("%7d | %9d | %10d | %7d | %7.1f%%",
			size, jsonSize, cowrieSize, savings, pct)
	}

	// Also test scores-only to show tensor optimization
	t.Log("\n=== Size Comparison (Scores Only - Tensor Optimization) ===")
	t.Log("Scores  | JSON Size | Cowrie Size | Savings | Savings %")
	t.Log("--------|-----------|------------|---------|----------")

	for _, size := range benchSizes {
		scores := make([]float32, size)
		for i := 0; i < size; i++ {
			scores[i] = float32(i) * 0.001
		}
		data := map[string]any{"scores": scores}

		var jsonBuf, cowrieBuf bytes.Buffer
		jsonCodec.Encode(&jsonBuf, data)
		cowrieCodec.Encode(&cowrieBuf, data)

		jsonSize := jsonBuf.Len()
		cowrieSize := cowrieBuf.Len()
		savings := jsonSize - cowrieSize
		pct := float64(savings) / float64(jsonSize) * 100

		t.Logf("%7d | %9d | %10d | %7d | %7.1f%%",
			size, jsonSize, cowrieSize, savings, pct)
	}
}
