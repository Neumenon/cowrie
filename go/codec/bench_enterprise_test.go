package codec

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"testing"

	"github.com/Neumenon/cowrie"
)

// Enterprise Benchmark Suite
// These benchmarks provide receipts for procurement conversations.
// Run with: go test -bench=. -benchmem ./codec/...

// BenchmarkEncode_Struct_Cached benchmarks struct encoding with cached encoder.
func BenchmarkEncode_Struct_Cached(b *testing.B) {
	type QueryResult struct {
		ID       string   `json:"id"`
		Score    float32  `json:"score"`
		Document string   `json:"document"`
		Metadata map[string]string `json:"metadata"`
	}

	type QueryResponse struct {
		Results []QueryResult `json:"results"`
		Total   int64         `json:"total"`
		Took    int64         `json:"took_ms"`
	}

	// Generate test data
	resp := QueryResponse{
		Total: 100,
		Took:  15,
		Results: make([]QueryResult, 100),
	}
	for i := range resp.Results {
		resp.Results[i] = QueryResult{
			ID:       "doc_" + string(rune('0'+i%10)),
			Score:    float32(rand.Float64()),
			Document: "This is document content for result " + string(rune('0'+i%10)),
			Metadata: map[string]string{"key": "value"},
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := FastEncode(resp)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecode_Struct_Cached benchmarks struct decoding with cached decoder.
func BenchmarkDecode_Struct_Cached(b *testing.B) {
	type QueryResult struct {
		ID       string   `json:"id"`
		Score    float32  `json:"score"`
		Document string   `json:"document"`
	}

	type QueryResponse struct {
		Results []QueryResult `json:"results"`
		Total   int64         `json:"total"`
		Took    int64         `json:"took_ms"`
	}

	resp := QueryResponse{
		Total: 100,
		Took:  15,
		Results: make([]QueryResult, 100),
	}
	for i := range resp.Results {
		resp.Results[i] = QueryResult{
			ID:       "doc_" + string(rune('0'+i%10)),
			Score:    float32(rand.Float64()),
			Document: "This is document content",
		}
	}

	encoded, _ := FastEncode(resp)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var decoded QueryResponse
		if err := DecodeBytes(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMasterStream_WriteRead benchmarks master stream write/read.
func BenchmarkMasterStream_WriteRead(b *testing.B) {
	payload := MustValueFromAny(map[string]any{
		"id":      int64(42),
		"name":    "benchmark_payload",
		"score":   float64(0.95),
		"tags":    []any{"a", "b", "c"},
		"enabled": true,
	})

	b.Run("write", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			mw := NewMasterWriter(&buf, MasterWriterOptions{})
			mw.Write(payload)
		}
	})

	// Pre-encode for read benchmark
	var buf bytes.Buffer
	mw := NewMasterWriter(&buf, MasterWriterOptions{})
	mw.Write(payload)
	encoded := buf.Bytes()

	b.Run("read", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			mr := NewMasterReader(encoded, MasterReaderOptions{})
			mr.Next()
		}
	})

	b.Run("write_with_crc", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			mw := NewMasterWriter(&buf, MasterWriterOptions{EnableCRC: true})
			mw.Write(payload)
		}
	})

	b.Run("write_deterministic", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			mw := NewMasterWriter(&buf, MasterWriterOptions{Deterministic: true})
			mw.Write(payload)
		}
	})
}

// BenchmarkTensor_EncodeDecode benchmarks tensor encode/decode.
func BenchmarkTensor_EncodeDecode(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		f32 := make([]float32, size)
		f64 := make([]float64, size)
		for i := range f32 {
			f32[i] = rand.Float32()
			f64[i] = rand.Float64()
		}

		b.Run("float32_encode_"+formatSize(size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				EncodeFloat32Tensor(f32)
			}
		})

		t32 := EncodeFloat32Tensor(f32)
		b.Run("float32_decode_"+formatSize(size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				DecodeFloat32Tensor(t32)
			}
		})

		b.Run("float64_encode_"+formatSize(size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				EncodeFloat64Tensor(f64)
			}
		})

		t64 := EncodeFloat64Tensor(f64)
		b.Run("float64_decode_"+formatSize(size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				DecodeFloat64Tensor(t64)
			}
		})
	}
}

// BenchmarkVsJSON compares Cowrie vs standard JSON.
func BenchmarkVsJSON(b *testing.B) {
	type TestStruct struct {
		ID     int64     `json:"id"`
		Name   string    `json:"name"`
		Score  float64   `json:"score"`
		Tags   []string  `json:"tags"`
		Values []float32 `json:"values"`
	}

	data := TestStruct{
		ID:     42,
		Name:   "benchmark_struct",
		Score:  3.14159265358979,
		Tags:   []string{"tag1", "tag2", "tag3", "tag4", "tag5"},
		Values: make([]float32, 100),
	}
	for i := range data.Values {
		data.Values[i] = rand.Float32()
	}

	b.Run("json_encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			json.Marshal(data)
		}
	})

	b.Run("cowrie_encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			FastEncode(data)
		}
	})

	jsonEncoded, _ := json.Marshal(data)
	cowrieEncoded, _ := FastEncode(data)

	b.Run("json_decode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var decoded TestStruct
			json.Unmarshal(jsonEncoded, &decoded)
		}
	})

	b.Run("cowrie_decode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var decoded TestStruct
			DecodeBytes(cowrieEncoded, &decoded)
		}
	})

	b.Logf("JSON size: %d bytes", len(jsonEncoded))
	b.Logf("Cowrie size: %d bytes", len(cowrieEncoded))
	b.Logf("Size savings: %.1f%%", 100*(1-float64(len(cowrieEncoded))/float64(len(jsonEncoded))))
}

// BenchmarkSchemaFingerprint benchmarks fingerprint computation.
func BenchmarkSchemaFingerprint(b *testing.B) {
	small := MustValueFromAny(map[string]any{
		"id":   int64(42),
		"name": "test",
	})

	medium := MustValueFromAny(map[string]any{
		"id":    int64(42),
		"name":  "test",
		"score": float64(3.14),
		"tags":  []any{"a", "b", "c"},
		"nested": map[string]any{
			"x": int64(1),
			"y": int64(2),
		},
	})

	large := func() *cowrie.Value {
		items := make([]any, 100)
		for i := range items {
			items[i] = map[string]any{
				"id":    int64(i),
				"name":  "item_" + string(rune('0'+i%10)),
				"value": float64(i) * 0.01,
			}
		}
		return MustValueFromAny(map[string]any{
			"items": items,
			"total": int64(100),
		})
	}()

	b.Run("small", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cowrie.SchemaFingerprint32(small)
		}
	})

	b.Run("medium", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cowrie.SchemaFingerprint32(medium)
		}
	})

	b.Run("large", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cowrie.SchemaFingerprint32(large)
		}
	})
}

// BenchmarkDeterministicEncode benchmarks deterministic vs regular encoding.
func BenchmarkDeterministicEncode(b *testing.B) {
	// Object with many keys (to stress sorting)
	obj := make(map[string]any)
	for i := 0; i < 50; i++ {
		obj["key_"+string(rune('A'+i%26))+string(rune('0'+i%10))] = int64(i)
	}
	v := MustValueFromAny(obj)

	b.Run("regular", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cowrie.Encode(v)
		}
	})

	b.Run("deterministic", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cowrie.EncodeWithOptions(v, cowrie.EncodeOptions{Deterministic: true})
		}
	})
}

// BenchmarkCompression benchmarks compression overhead.
func BenchmarkCompression(b *testing.B) {
	// Large payload to benefit from compression
	items := make([]any, 1000)
	for i := range items {
		items[i] = map[string]any{
			"id":      int64(i),
			"name":    "item_" + string(rune('0'+i%10)),
			"content": "This is some repeated content that compresses well. Lorem ipsum dolor sit amet.",
		}
	}
	payload := MustValueFromAny(map[string]any{"items": items})

	b.Run("uncompressed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			mw := NewMasterWriter(&buf, MasterWriterOptions{})
			mw.Write(payload)
		}
	})

	b.Run("gzip", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			mw := NewMasterWriter(&buf, MasterWriterOptions{Compression: CompressionGzip})
			mw.Write(payload)
		}
	})

	b.Run("zstd", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			mw := NewMasterWriter(&buf, MasterWriterOptions{Compression: CompressionZstd})
			mw.Write(payload)
		}
	})

	// Report sizes
	var uncompressed, gzipped, zstd bytes.Buffer
	mwU := NewMasterWriter(&uncompressed, MasterWriterOptions{})
	mwG := NewMasterWriter(&gzipped, MasterWriterOptions{Compression: CompressionGzip})
	mwZ := NewMasterWriter(&zstd, MasterWriterOptions{Compression: CompressionZstd})
	mwU.Write(payload)
	mwG.Write(payload)
	mwZ.Write(payload)

	b.Logf("Uncompressed: %d bytes", uncompressed.Len())
	b.Logf("Gzip: %d bytes (%.1f%% savings)", gzipped.Len(), 100*(1-float64(gzipped.Len())/float64(uncompressed.Len())))
	b.Logf("Zstd: %d bytes (%.1f%% savings)", zstd.Len(), 100*(1-float64(zstd.Len())/float64(uncompressed.Len())))
}

func formatSize(n int) string {
	if n >= 1000 {
		return string(rune('0'+n/1000)) + "k"
	}
	return string(rune('0' + n/100))
}
