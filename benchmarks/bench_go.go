// Cowrie Go Benchmark Suite
// Run: go test -bench=. -benchmem ./benchmarks/

package benchmarks

import (
	"encoding/json"
	"testing"

	"github.com/phenomenon0/cowrie-final/go/gen1"
	"github.com/phenomenon0/cowrie-final/go/gen2"
)

// Test payloads
var (
	smallObject = map[string]any{
		"name":  "Alice",
		"age":   30,
		"score": 3.14159,
	}

	mediumObject = func() map[string]any {
		obj := make(map[string]any)
		for i := 0; i < 20; i++ {
			obj[string(rune('a'+i))] = i * 100
		}
		obj["nested"] = map[string]any{
			"x": 1.0,
			"y": 2.0,
			"z": 3.0,
		}
		return obj
	}()

	largeArray = func() []any {
		arr := make([]any, 1000)
		for i := range arr {
			arr[i] = map[string]any{
				"id":    i,
				"name":  "item",
				"value": float64(i) * 0.1,
			}
		}
		return arr
	}()

	floatArray = func() []float64 {
		arr := make([]float64, 10000)
		for i := range arr {
			arr[i] = float64(i) * 0.001
		}
		return arr
	}()
)

// Gen1 Benchmarks

func BenchmarkGen1EncodeSmall(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		gen1.Encode(smallObject)
	}
}

func BenchmarkGen1EncodeMedium(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		gen1.Encode(mediumObject)
	}
}

func BenchmarkGen1EncodeLarge(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		gen1.Encode(largeArray)
	}
}

func BenchmarkGen1EncodeFloatArray(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		gen1.Encode(floatArray)
	}
}

func BenchmarkGen1DecodeSmall(b *testing.B) {
	data, _ := gen1.Encode(smallObject)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		gen1.Decode(data)
	}
}

func BenchmarkGen1DecodeLarge(b *testing.B) {
	data, _ := gen1.Encode(largeArray)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		gen1.Decode(data)
	}
}

// Gen2 Benchmarks

func BenchmarkGen2EncodeSmall(b *testing.B) {
	val := gen2.Object(
		gen2.Member{Key: "name", Value: gen2.String("Alice")},
		gen2.Member{Key: "age", Value: gen2.Int64(30)},
		gen2.Member{Key: "score", Value: gen2.Float64(3.14159)},
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		gen2.Encode(val)
	}
}

func BenchmarkGen2EncodeLargeArray(b *testing.B) {
	items := make([]*gen2.Value, 1000)
	for i := range items {
		items[i] = gen2.Object(
			gen2.Member{Key: "id", Value: gen2.Int64(int64(i))},
			gen2.Member{Key: "name", Value: gen2.String("item")},
			gen2.Member{Key: "value", Value: gen2.Float64(float64(i) * 0.1)},
		)
	}
	val := gen2.Array(items...)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		gen2.Encode(val)
	}
}

func BenchmarkGen2DecodeSmall(b *testing.B) {
	val := gen2.Object(
		gen2.Member{Key: "name", Value: gen2.String("Alice")},
		gen2.Member{Key: "age", Value: gen2.Int64(30)},
		gen2.Member{Key: "score", Value: gen2.Float64(3.14159)},
	)
	data, _ := gen2.Encode(val)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		gen2.Decode(data)
	}
}

func BenchmarkGen2DecodeLargeArray(b *testing.B) {
	items := make([]*gen2.Value, 1000)
	for i := range items {
		items[i] = gen2.Object(
			gen2.Member{Key: "id", Value: gen2.Int64(int64(i))},
			gen2.Member{Key: "name", Value: gen2.String("item")},
			gen2.Member{Key: "value", Value: gen2.Float64(float64(i) * 0.1)},
		)
	}
	val := gen2.Array(items...)
	data, _ := gen2.Encode(val)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		gen2.Decode(data)
	}
}

// Graph Type Benchmarks

func BenchmarkGen2EncodeGraphShard(b *testing.B) {
	nodes := make([]*gen2.Value, 100)
	for i := range nodes {
		nodes[i] = gen2.Node(gen2.NodeConfig{
			ID:     string(rune('a' + i%26)),
			Labels: []string{"Person"},
			Props: map[string]any{
				"name": "node",
				"x":    float64(i) * 0.1,
			},
		})
	}
	edges := make([]*gen2.Value, 200)
	for i := range edges {
		edges[i] = gen2.Edge(gen2.EdgeConfig{
			From:     string(rune('a' + i%26)),
			To:       string(rune('a' + (i+1)%26)),
			EdgeType: "KNOWS",
			Props: map[string]any{
				"weight": 0.5,
			},
		})
	}
	shard := gen2.GraphShard(nodes, edges, map[string]any{"version": int64(1)})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		gen2.Encode(shard)
	}
}

// JSON Comparison Benchmarks

func BenchmarkJSONEncodeSmall(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		json.Marshal(smallObject)
	}
}

func BenchmarkJSONEncodeLarge(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		json.Marshal(largeArray)
	}
}

func BenchmarkJSONDecodeSmall(b *testing.B) {
	data, _ := json.Marshal(smallObject)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result map[string]any
		json.Unmarshal(data, &result)
	}
}

func BenchmarkJSONDecodeLarge(b *testing.B) {
	data, _ := json.Marshal(largeArray)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result []any
		json.Unmarshal(data, &result)
	}
}

// Size Comparison

func TestPayloadSizes(t *testing.T) {
	// Small object
	smallJSON, _ := json.Marshal(smallObject)
	smallGen1, _ := gen1.Encode(smallObject)
	smallGen2Val := gen2.Object(
		gen2.Member{Key: "name", Value: gen2.String("Alice")},
		gen2.Member{Key: "age", Value: gen2.Int64(30)},
		gen2.Member{Key: "score", Value: gen2.Float64(3.14159)},
	)
	smallGen2, _ := gen2.Encode(smallGen2Val)

	t.Logf("Small Object Sizes:")
	t.Logf("  JSON:  %d bytes", len(smallJSON))
	t.Logf("  Gen1:  %d bytes (%.1f%% of JSON)", len(smallGen1), float64(len(smallGen1))/float64(len(smallJSON))*100)
	t.Logf("  Gen2:  %d bytes (%.1f%% of JSON)", len(smallGen2), float64(len(smallGen2))/float64(len(smallJSON))*100)

	// Large array with repeated keys
	largeJSON, _ := json.Marshal(largeArray)
	largeGen1, _ := gen1.Encode(largeArray)
	items := make([]*gen2.Value, 1000)
	for i := range items {
		items[i] = gen2.Object(
			gen2.Member{Key: "id", Value: gen2.Int64(int64(i))},
			gen2.Member{Key: "name", Value: gen2.String("item")},
			gen2.Member{Key: "value", Value: gen2.Float64(float64(i) * 0.1)},
		)
	}
	largeGen2Val := gen2.Array(items...)
	largeGen2, _ := gen2.Encode(largeGen2Val)

	t.Logf("\nLarge Array (1000 objects) Sizes:")
	t.Logf("  JSON:  %d bytes", len(largeJSON))
	t.Logf("  Gen1:  %d bytes (%.1f%% of JSON)", len(largeGen1), float64(len(largeGen1))/float64(len(largeJSON))*100)
	t.Logf("  Gen2:  %d bytes (%.1f%% of JSON)", len(largeGen2), float64(len(largeGen2))/float64(len(largeJSON))*100)

	// Float array
	floatJSON, _ := json.Marshal(floatArray)
	floatGen1, _ := gen1.Encode(floatArray)

	t.Logf("\nFloat Array (10000 floats) Sizes:")
	t.Logf("  JSON:  %d bytes", len(floatJSON))
	t.Logf("  Gen1:  %d bytes (%.1f%% of JSON)", len(floatGen1), float64(len(floatGen1))/float64(len(floatJSON))*100)
}
