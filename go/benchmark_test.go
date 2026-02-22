package cowrie

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Compress JSON with gzip
func compressJSONGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Compress JSON with zstd
func compressJSONZstd(data []byte) ([]byte, error) {
	enc, _ := zstd.NewWriter(nil)
	return enc.EncodeAll(data, nil), nil
}

// TestCompressionComparison compares JSON vs Cowrie compression ratios
func TestCompressionComparison(t *testing.T) {
	testCases := []struct {
		name string
		data func() *Value
	}{
		{
			name: "Small API Response",
			data: func() *Value {
				return Object(
					Member{Key: "user_id", Value: Int64(12345)},
					Member{Key: "user_name", Value: String("alice_wonder")},
					Member{Key: "user_email", Value: String("alice@example.com")},
					Member{Key: "created_at", Value: Datetime(time.Now())},
					Member{Key: "is_active", Value: Bool(true)},
				)
			},
		},
		{
			name: "Many Fields Object (dict-coded keys)",
			data: func() *Value {
				members := make([]Member, 50)
				// Same key patterns repeated across multiple sub-objects to show dict benefit
				for i := 0; i < 50; i++ {
					members[i] = Member{
						Key:   fmt.Sprintf("field_%d", i),
						Value: String(fmt.Sprintf("value_%d", i)),
					}
				}
				return Object(members...)
			},
		},
		{
			name: "Nested Objects (dict reuse)",
			data: func() *Value {
				// Multiple objects with same keys - dictionary really shines here
				items := make([]*Value, 20)
				for i := 0; i < 20; i++ {
					items[i] = Object(
						Member{Key: "id", Value: Int64(int64(i))},
						Member{Key: "name", Value: String(fmt.Sprintf("item_%d", i))},
						Member{Key: "active", Value: Bool(i%2 == 0)},
						Member{Key: "score", Value: Float64(float64(i) * 1.5)},
					)
				}
				return Array(items...)
			},
		},
		{
			name: "Large Log-like Data",
			data: func() *Value {
				items := make([]*Value, 100)
				for i := 0; i < 100; i++ {
					items[i] = Object(
						Member{Key: "timestamp", Value: Datetime(time.Now().Add(time.Duration(i) * time.Second))},
						Member{Key: "level", Value: String("INFO")},
						Member{Key: "message", Value: String("Processing request from user")},
						Member{Key: "request_id", Value: Int64(int64(1000000 + i))},
						Member{Key: "duration_ms", Value: Float64(float64(i) * 1.5)},
					)
				}
				return Array(items...)
			},
		},
		{
			name: "Telemetry Data",
			data: func() *Value {
				items := make([]*Value, 200)
				for i := 0; i < 200; i++ {
					items[i] = Object(
						Member{Key: "ts", Value: Datetime64(time.Now().UnixNano() + int64(i*1000000))},
						Member{Key: "cpu", Value: Float64(float64(i%100) / 100.0)},
						Member{Key: "mem", Value: Float64(float64((i+50)%100) / 100.0)},
						Member{Key: "disk_read", Value: Int64(int64(i * 1024))},
						Member{Key: "disk_write", Value: Int64(int64(i * 512))},
						Member{Key: "net_in", Value: Int64(int64(i * 256))},
						Member{Key: "net_out", Value: Int64(int64(i * 128))},
					)
				}
				return Array(items...)
			},
		},
		{
			name: "Mixed Binary Data",
			data: func() *Value {
				// Simulate an object with binary blobs
				blob1 := make([]byte, 500)
				for i := range blob1 {
					blob1[i] = byte(i % 256)
				}
				blob2 := make([]byte, 300)
				for i := range blob2 {
					blob2[i] = byte((i * 7) % 256)
				}
				return Object(
					Member{Key: "name", Value: String("binary_record")},
					Member{Key: "data1", Value: Bytes(blob1)},
					Member{Key: "data2", Value: Bytes(blob2)},
					Member{Key: "checksum", Value: Int64(12345678)},
				)
			},
		},
	}

	fmt.Println("\n=== Cowrie vs JSON Compression Comparison ===")
	fmt.Println()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v := tc.data()

			// Cowrie raw
			cowrieRaw, _ := Encode(v)

			// Cowrie + gzip
			cowrieGzip, _ := EncodeFramed(v, CompressionGzip)

			// Cowrie + zstd
			cowrieZstd, _ := EncodeFramed(v, CompressionZstd)

			// JSON raw
			jsonRaw, _ := ToJSON(v)

			// JSON + gzip
			jsonGzip, _ := compressJSONGzip(jsonRaw)

			// JSON + zstd
			jsonZstd, _ := compressJSONZstd(jsonRaw)

			fmt.Printf("=== %s ===\n", tc.name)
			fmt.Printf("%-20s %8s %8s %8s\n", "", "Raw", "Gzip", "Zstd")
			fmt.Printf("%-20s %8d %8d %8d\n", "JSON (bytes)", len(jsonRaw), len(jsonGzip), len(jsonZstd))
			fmt.Printf("%-20s %8d %8d %8d\n", "Cowrie (bytes)", len(cowrieRaw), len(cowrieGzip), len(cowrieZstd))
			fmt.Printf("%-20s %7.1f%% %7.1f%% %7.1f%%\n", "Cowrie savings",
				(1-float64(len(cowrieRaw))/float64(len(jsonRaw)))*100,
				(1-float64(len(cowrieGzip))/float64(len(jsonGzip)))*100,
				(1-float64(len(cowrieZstd))/float64(len(jsonZstd)))*100)
			fmt.Println()
		})
	}
}

// BenchmarkCompressionSpeed compares encoding speeds
func BenchmarkCompressionSpeed(b *testing.B) {
	// Create test data
	items := make([]*Value, 100)
	for i := 0; i < 100; i++ {
		items[i] = Object(
			Member{Key: "timestamp", Value: Datetime(time.Now())},
			Member{Key: "level", Value: String("INFO")},
			Member{Key: "message", Value: String("Processing request")},
			Member{Key: "request_id", Value: Int64(int64(1000000 + i))},
		)
	}
	v := Array(items...)

	// Equivalent JSON data
	jsonData, _ := ToJSON(v)

	b.Run("Cowrie_Raw", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Encode(v)
		}
	})

	b.Run("Cowrie_Zstd", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			EncodeFramed(v, CompressionZstd)
		}
	})

	b.Run("Cowrie_Gzip", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			EncodeFramed(v, CompressionGzip)
		}
	})

	b.Run("JSON_Raw", func(b *testing.B) {
		obj := make([]map[string]any, 100)
		for i := 0; i < 100; i++ {
			obj[i] = map[string]any{
				"timestamp":  time.Now().Format(time.RFC3339Nano),
				"level":      "INFO",
				"message":    "Processing request",
				"request_id": 1000000 + i,
			}
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			json.Marshal(obj)
		}
	})

	b.Run("JSON_Zstd", func(b *testing.B) {
		enc, _ := zstd.NewWriter(nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			enc.EncodeAll(jsonData, nil)
		}
	})

	b.Run("JSON_Gzip", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			w := gzip.NewWriter(&buf)
			w.Write(jsonData)
			w.Close()
		}
	})
}

// BenchmarkDecompressionSpeed compares decoding speeds
func BenchmarkDecompressionSpeed(b *testing.B) {
	items := make([]*Value, 100)
	for i := 0; i < 100; i++ {
		items[i] = Object(
			Member{Key: "timestamp", Value: Datetime(time.Now())},
			Member{Key: "level", Value: String("INFO")},
			Member{Key: "message", Value: String("Processing request")},
			Member{Key: "request_id", Value: Int64(int64(1000000 + i))},
		)
	}
	v := Array(items...)

	cowrieRaw, _ := Encode(v)
	cowrieZstd, _ := EncodeFramed(v, CompressionZstd)
	cowrieGzip, _ := EncodeFramed(v, CompressionGzip)
	jsonRaw, _ := ToJSON(v)
	jsonZstd, _ := compressJSONZstd(jsonRaw)
	jsonGzip, _ := compressJSONGzip(jsonRaw)

	b.Run("Cowrie_Raw", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Decode(cowrieRaw)
		}
	})

	b.Run("Cowrie_Zstd", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			DecodeFramed(cowrieZstd)
		}
	})

	b.Run("Cowrie_Gzip", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			DecodeFramed(cowrieGzip)
		}
	})

	b.Run("JSON_Raw", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var v []map[string]any
			json.Unmarshal(jsonRaw, &v)
		}
	})

	dec, _ := zstd.NewReader(nil)
	b.Run("JSON_Zstd", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			decompressed, _ := dec.DecodeAll(jsonZstd, nil)
			var v []map[string]any
			json.Unmarshal(decompressed, &v)
		}
	})

	b.Run("JSON_Gzip", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := gzip.NewReader(bytes.NewReader(jsonGzip))
			decompressed, _ := io.ReadAll(r)
			r.Close()
			var v []map[string]any
			json.Unmarshal(decompressed, &v)
		}
	})
}
