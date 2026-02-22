package gen1

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"reflect"
	"testing"
)

func TestPrimitives(t *testing.T) {
	tests := []struct {
		name  string
		input any
		check func(any) bool
	}{
		{"null", nil, func(v any) bool { return v == nil }},
		{"true", true, func(v any) bool { return v == true }},
		{"false", false, func(v any) bool { return v == false }},
		{"int64 positive", int64(42), func(v any) bool { return v == int64(42) }},
		{"int64 negative", int64(-42), func(v any) bool { return v == int64(-42) }},
		{"int64 zero", int64(0), func(v any) bool { return v == int64(0) }},
		{"int64 max", int64(math.MaxInt64), func(v any) bool { return v == int64(math.MaxInt64) }},
		{"int64 min", int64(math.MinInt64), func(v any) bool { return v == int64(math.MinInt64) }},
		{"float64", 3.14159, func(v any) bool { return v == 3.14159 }},
		{"float64 zero", 0.0, func(v any) bool { return v == 0.0 }},
		{"float64 negative", -1.5, func(v any) bool { return v == -1.5 }},
		{"string empty", "", func(v any) bool { return v == "" }},
		{"string hello", "hello", func(v any) bool { return v == "hello" }},
		{"string unicode", "hello 世界 🌍", func(v any) bool { return v == "hello 世界 🌍" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := Encode(tt.input)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			result, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			if !tt.check(result) {
				t.Errorf("Check failed: got %v (%T), want match for %v", result, result, tt.input)
			}
		})
	}
}

func TestIntTypes(t *testing.T) {
	// All int types should encode as int64
	tests := []struct {
		name  string
		input any
		want  int64
	}{
		{"int", 42, 42},
		{"int8", int8(42), 42},
		{"int16", int16(42), 42},
		{"int32", int32(42), 42},
		{"int64", int64(42), 42},
		{"uint", uint(42), 42},
		{"uint8", uint8(42), 42},
		{"uint16", uint16(42), 42},
		{"uint32", uint32(42), 42},
		{"uint64", uint64(42), 42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := Encode(tt.input)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			result, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			if result != tt.want {
				t.Errorf("got %v, want %v", result, tt.want)
			}
		})
	}
}

func TestObject(t *testing.T) {
	input := map[string]any{
		"name":   "Alice",
		"age":    int64(30),
		"active": true,
	}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	obj, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("Expected map, got %T", result)
	}

	if obj["name"] != "Alice" {
		t.Errorf("name: got %v, want Alice", obj["name"])
	}
	if obj["age"] != int64(30) {
		t.Errorf("age: got %v, want 30", obj["age"])
	}
	if obj["active"] != true {
		t.Errorf("active: got %v, want true", obj["active"])
	}
}

func TestNestedObject(t *testing.T) {
	input := map[string]any{
		"user": map[string]any{
			"name": "Bob",
			"address": map[string]any{
				"city": "NYC",
			},
		},
	}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	obj := result.(map[string]any)
	user := obj["user"].(map[string]any)
	address := user["address"].(map[string]any)

	if address["city"] != "NYC" {
		t.Errorf("city: got %v, want NYC", address["city"])
	}
}

func TestArrayGeneric(t *testing.T) {
	// Small array (< NumericArrayMin) uses generic encoding
	input := []any{"a", "b", "c"}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	arr, ok := result.([]any)
	if !ok {
		t.Fatalf("Expected []any, got %T", result)
	}

	if len(arr) != 3 {
		t.Fatalf("Expected 3 elements, got %d", len(arr))
	}

	for i, want := range []string{"a", "b", "c"} {
		if arr[i] != want {
			t.Errorf("arr[%d]: got %v, want %v", i, arr[i], want)
		}
	}
}

func TestProtoTensorFloat64(t *testing.T) {
	// Large float array (>= NumericArrayMin) uses proto-tensor
	input := []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Verify it uses float64 encoding (tag 0x0A) - default is HighPrecision: true
	if data[0] != tagArrayFloat64 {
		t.Errorf("Expected tagArrayFloat64 (0x0A), got 0x%02X", data[0])
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	arr, ok := result.([]float64)
	if !ok {
		t.Fatalf("Expected []float64, got %T", result)
	}

	if len(arr) != 8 {
		t.Fatalf("Expected 8 elements, got %d", len(arr))
	}

	// Check values (with float32 precision loss tolerance)
	for i, want := range input {
		got := arr[i]
		if math.Abs(got-want) > 0.001 {
			t.Errorf("arr[%d]: got %v, want ~%v", i, got, want)
		}
	}
}

func TestProtoTensorFloat32(t *testing.T) {
	input := []float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if data[0] != tagArrayFloat32 {
		t.Errorf("Expected tagArrayFloat32 (0x0A), got 0x%02X", data[0])
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	arr, ok := result.([]float64)
	if !ok {
		t.Fatalf("Expected []float64, got %T", result)
	}
	if len(arr) != 8 {
		t.Fatalf("Expected 8 elements, got %d", len(arr))
	}
}

func TestProtoTensorInt64(t *testing.T) {
	input := []int64{1, 2, 3, 4, 5, 6, 7, 8}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if data[0] != tagArrayInt64 {
		t.Errorf("Expected tagArrayInt64 (0x09), got 0x%02X", data[0])
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Int64Array now decodes to []int64 for type safety
	arr, ok := result.([]int64)
	if !ok {
		t.Fatalf("Expected []int64, got %T", result)
	}
	for i, want := range input {
		if arr[i] != want {
			t.Errorf("arr[%d]: got %v, want %v", i, arr[i], want)
		}
	}
}

func TestProtoTensorIntSlice(t *testing.T) {
	input := []int{1, 2, 3, 4, 5, 6, 7, 8}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if data[0] != tagArrayInt64 {
		t.Errorf("Expected tagArrayInt64, got 0x%02X", data[0])
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Int64Array now decodes to []int64 for type safety
	arr, ok := result.([]int64)
	if !ok {
		t.Fatalf("Expected []int64, got %T", result)
	}
	for i, want := range input {
		if arr[i] != int64(want) {
			t.Errorf("arr[%d]: got %v, want %v", i, arr[i], want)
		}
	}
}

func TestProtoTensorPromotion(t *testing.T) {
	// []any with homogeneous floats should promote to proto-tensor
	input := []any{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Should use float64 encoding (default is HighPrecision: true)
	if data[0] != tagArrayFloat64 {
		t.Errorf("Expected promotion to tagArrayFloat64, got 0x%02X", data[0])
	}
}

func TestMatrix(t *testing.T) {
	input := [][]float64{
		{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0},
		{9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0},
	}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	arr, ok := result.([]any)
	if !ok {
		t.Fatalf("Expected []any, got %T", result)
	}
	if len(arr) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(arr))
	}

	row0, ok := arr[0].([]float64)
	if !ok {
		t.Fatalf("Expected []float64, got %T", arr[0])
	}
	if len(row0) != 8 {
		t.Fatalf("Expected 8 columns, got %d", len(row0))
	}

	// Check first element with tolerance
	if math.Abs(row0[0]-1.0) > 0.001 {
		t.Errorf("row0[0]: got %v, want 1.0", row0[0])
	}
}

func TestJSONRoundtrip(t *testing.T) {
	jsonInput := []byte(`{"name":"Alice","scores":[1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8]}`)

	gen1Data, err := EncodeJSON(jsonInput)
	if err != nil {
		t.Fatalf("EncodeJSON failed: %v", err)
	}

	jsonOutput, err := DecodeJSON(gen1Data)
	if err != nil {
		t.Fatalf("DecodeJSON failed: %v", err)
	}

	// Parse both to compare
	var original, result map[string]any
	json.Unmarshal(jsonInput, &original)
	json.Unmarshal(jsonOutput, &result)

	if result["name"] != original["name"] {
		t.Errorf("name mismatch: %v vs %v", result["name"], original["name"])
	}
}

func TestDeterministicOrder(t *testing.T) {
	input := map[string]any{
		"z": 1,
		"a": 2,
		"m": 3,
	}

	// Encode multiple times - should get identical bytes
	data1, _ := Encode(input)
	data2, _ := Encode(input)
	data3, _ := Encode(input)

	if !reflect.DeepEqual(data1, data2) || !reflect.DeepEqual(data2, data3) {
		t.Error("Encoding is not deterministic")
	}
}

func TestSizeComparison(t *testing.T) {
	// 768-float embedding (typical for text-embedding models)
	embedding := make([]float64, 768)
	for i := range embedding {
		embedding[i] = float64(i) * 0.001
	}

	input := map[string]any{
		"id":        "doc-12345",
		"embedding": embedding,
	}

	// Use HighPrecision: false for size comparison (4 bytes/float vs 8 bytes)
	// Default is now HighPrecision: true for cross-language compatibility
	gen1Data, _ := EncodeWithOptions(input, EncodeOptions{HighPrecision: false})
	jsonData, _ := json.Marshal(input)

	t.Logf("JSON size: %d bytes", len(jsonData))
	t.Logf("Gen1 size (HighPrecision=false): %d bytes", len(gen1Data))
	t.Logf("Compression ratio: %.1f%%", float64(len(gen1Data))/float64(len(jsonData))*100)

	// Gen 1 with float32 should be significantly smaller
	if len(gen1Data) >= len(jsonData) {
		t.Errorf("Gen1 should be smaller than JSON: %d >= %d", len(gen1Data), len(jsonData))
	}

	// Expect at least 40% reduction for float arrays (actual: ~48-52%)
	if float64(len(gen1Data)) > float64(len(jsonData))*0.60 {
		t.Errorf("Expected at least 40%% reduction, got %.1f%%",
			float64(len(gen1Data))/float64(len(jsonData))*100)
	}
}

func TestEmptyContainers(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{"empty object", map[string]any{}},
		{"empty array", []any{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := Encode(tt.input)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			result, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			switch v := result.(type) {
			case map[string]any:
				if len(v) != 0 {
					t.Errorf("Expected empty map, got %d elements", len(v))
				}
			case []any:
				if len(v) != 0 {
					t.Errorf("Expected empty array, got %d elements", len(v))
				}
			default:
				t.Errorf("Unexpected type: %T", result)
			}
		})
	}
}

func TestJSONNumber(t *testing.T) {
	// json.Number handling
	jsonInput := []byte(`{"int": 42, "float": 3.14}`)

	var obj map[string]any
	dec := json.NewDecoder(bytes.NewReader(jsonInput))
	dec.UseNumber()
	dec.Decode(&obj)

	data, err := Encode(obj)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	m := result.(map[string]any)
	if m["int"] != int64(42) {
		t.Errorf("int: got %v (%T), want int64(42)", m["int"], m["int"])
	}
	if m["float"] != 3.14 {
		t.Errorf("float: got %v, want 3.14", m["float"])
	}
}

func TestDecodeErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"truncated varint", []byte{tagInt64}},
		{"truncated float", []byte{tagFloat64, 0x00, 0x00}},
		{"truncated string", []byte{tagString, 0x05, 'h', 'e'}},
		{"unknown tag", []byte{0xFF}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Decode(tt.data)
			if err == nil {
				t.Error("Expected error, got nil")
			}
		})
	}
}

func BenchmarkEncode(b *testing.B) {
	embedding := make([]float64, 768)
	for i := range embedding {
		embedding[i] = float64(i) * 0.001
	}

	input := map[string]any{
		"id":        "doc-12345",
		"embedding": embedding,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Encode(input)
	}
}

func BenchmarkDecode(b *testing.B) {
	embedding := make([]float64, 768)
	for i := range embedding {
		embedding[i] = float64(i) * 0.001
	}

	input := map[string]any{
		"id":        "doc-12345",
		"embedding": embedding,
	}

	data, _ := Encode(input)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Decode(data)
	}
}

func TestStreamingEncodeDecode(t *testing.T) {
	// Simulate log file: multiple records written sequentially
	records := []map[string]any{
		{"ts": int64(1732825200), "level": "info", "msg": "started"},
		{"ts": int64(1732825201), "level": "warn", "msg": "slow query", "latency": 150.5},
		{"ts": int64(1732825202), "level": "error", "msg": "connection failed"},
	}

	// Write to buffer
	var buf bytes.Buffer
	for _, r := range records {
		if err := EncodeTo(&buf, r); err != nil {
			t.Fatalf("EncodeTo failed: %v", err)
		}
	}

	// Read back using StreamDecoder
	reader := bytes.NewReader(buf.Bytes())
	dec := NewStreamDecoder(reader)
	var decoded []map[string]any

	for {
		v, err := dec.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("StreamDecoder.Decode failed: %v", err)
		}
		decoded = append(decoded, v.(map[string]any))
	}

	if len(decoded) != len(records) {
		t.Fatalf("Expected %d records, got %d", len(records), len(decoded))
	}

	// Verify content
	for i, r := range decoded {
		if r["level"] != records[i]["level"] {
			t.Errorf("Record %d level mismatch: %v vs %v", i, r["level"], records[i]["level"])
		}
		if r["msg"] != records[i]["msg"] {
			t.Errorf("Record %d msg mismatch: %v vs %v", i, r["msg"], records[i]["msg"])
		}
	}
}

func TestLogSizeComparison(t *testing.T) {
	// Typical structured log entry
	logEntry := map[string]any{
		"ts":      int64(1732825200000000000),
		"level":   "info",
		"service": "api",
		"method":  "GET",
		"path":    "/users",
		"status":  int64(200),
		"latency": 42.5,
		"bytes":   int64(1234),
	}

	gen1Data, _ := Encode(logEntry)
	jsonData, _ := json.Marshal(logEntry)

	t.Logf("JSON log size: %d bytes", len(jsonData))
	t.Logf("Gen1 log size: %d bytes", len(gen1Data))
	t.Logf("Savings: %.1f%%", (1-float64(len(gen1Data))/float64(len(jsonData)))*100)

	// Gen 1 should be smaller
	if len(gen1Data) >= len(jsonData) {
		t.Errorf("Gen1 should be smaller: %d >= %d", len(gen1Data), len(jsonData))
	}
}

func BenchmarkVsJSON(b *testing.B) {
	embedding := make([]float64, 768)
	for i := range embedding {
		embedding[i] = float64(i) * 0.001
	}

	input := map[string]any{
		"id":        "doc-12345",
		"embedding": embedding,
	}

	b.Run("json_encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			json.Marshal(input)
		}
	})

	b.Run("gen1_encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Encode(input)
		}
	})

	jsonData, _ := json.Marshal(input)
	gen1Data, _ := Encode(input)

	b.Run("json_decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var v any
			json.Unmarshal(jsonData, &v)
		}
	})

	b.Run("gen1_decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Decode(gen1Data)
		}
	})
}

// ---------- Graph Type Tests ----------

func TestNode(t *testing.T) {
	input := Node{
		ID:     "user-123",
		Labels: []string{"Person", "Employee"},
		Props: map[string]any{
			"name": "Alice",
			"age":  int64(30),
		},
	}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Verify tag
	if data[0] != tagNode {
		t.Errorf("Expected tagNode (0x10), got 0x%02X", data[0])
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	node, ok := result.(Node)
	if !ok {
		t.Fatalf("Expected Node, got %T", result)
	}

	if node.ID != "user-123" {
		t.Errorf("ID: got %v, want user-123", node.ID)
	}
	if len(node.Labels) != 2 || node.Labels[0] != "Person" || node.Labels[1] != "Employee" {
		t.Errorf("Labels: got %v, want [Person Employee]", node.Labels)
	}
	if node.Props["name"] != "Alice" {
		t.Errorf("Props[name]: got %v, want Alice", node.Props["name"])
	}
	if node.Props["age"] != int64(30) {
		t.Errorf("Props[age]: got %v, want 30", node.Props["age"])
	}
}

func TestNodeMinimal(t *testing.T) {
	// Node with no labels and no props
	input := Node{
		ID: "minimal-node",
	}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	node := result.(Node)
	if node.ID != "minimal-node" {
		t.Errorf("ID: got %v, want minimal-node", node.ID)
	}
	if len(node.Labels) != 0 {
		t.Errorf("Labels: got %v, want empty", node.Labels)
	}
}

func TestEdge(t *testing.T) {
	input := Edge{
		ID:   "rel-456",
		Type: "KNOWS",
		From: "user-1",
		To:   "user-2",
		Props: map[string]any{
			"since": int64(2020),
			"trust": 0.95,
		},
	}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if data[0] != tagEdge {
		t.Errorf("Expected tagEdge (0x11), got 0x%02X", data[0])
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	edge, ok := result.(Edge)
	if !ok {
		t.Fatalf("Expected Edge, got %T", result)
	}

	if edge.ID != "rel-456" {
		t.Errorf("ID: got %v, want rel-456", edge.ID)
	}
	if edge.Type != "KNOWS" {
		t.Errorf("Type: got %v, want KNOWS", edge.Type)
	}
	if edge.From != "user-1" {
		t.Errorf("From: got %v, want user-1", edge.From)
	}
	if edge.To != "user-2" {
		t.Errorf("To: got %v, want user-2", edge.To)
	}
	if edge.Props["since"] != int64(2020) {
		t.Errorf("Props[since]: got %v, want 2020", edge.Props["since"])
	}
}

func TestAdjList(t *testing.T) {
	input := AdjList{
		NodeID:    42,
		Neighbors: []int64{1, 2, 3, 10, 20, 30, 100, 200},
	}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if data[0] != tagAdjList {
		t.Errorf("Expected tagAdjList (0x12), got 0x%02X", data[0])
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	adj, ok := result.(AdjList)
	if !ok {
		t.Fatalf("Expected AdjList, got %T", result)
	}

	if adj.NodeID != 42 {
		t.Errorf("NodeID: got %v, want 42", adj.NodeID)
	}
	if len(adj.Neighbors) != 8 {
		t.Errorf("Neighbors len: got %v, want 8", len(adj.Neighbors))
	}
	if adj.Neighbors[0] != 1 || adj.Neighbors[7] != 200 {
		t.Errorf("Neighbors: got %v", adj.Neighbors)
	}
}

func TestNodeBatch(t *testing.T) {
	input := NodeBatch{
		Nodes: []Node{
			{ID: "n1", Labels: []string{"A"}, Props: map[string]any{"x": int64(1)}},
			{ID: "n2", Labels: []string{"B"}, Props: map[string]any{"x": int64(2)}},
			{ID: "n3", Labels: []string{"C"}, Props: map[string]any{"x": int64(3)}},
		},
	}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if data[0] != tagNodeBatch {
		t.Errorf("Expected tagNodeBatch (0x13), got 0x%02X", data[0])
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	batch, ok := result.(NodeBatch)
	if !ok {
		t.Fatalf("Expected NodeBatch, got %T", result)
	}

	if len(batch.Nodes) != 3 {
		t.Fatalf("Nodes len: got %v, want 3", len(batch.Nodes))
	}

	if batch.Nodes[0].ID != "n1" {
		t.Errorf("Nodes[0].ID: got %v, want n1", batch.Nodes[0].ID)
	}
	if batch.Nodes[1].Labels[0] != "B" {
		t.Errorf("Nodes[1].Labels[0]: got %v, want B", batch.Nodes[1].Labels[0])
	}
	if batch.Nodes[2].Props["x"] != int64(3) {
		t.Errorf("Nodes[2].Props[x]: got %v, want 3", batch.Nodes[2].Props["x"])
	}
}

func TestEdgeBatchMinimal(t *testing.T) {
	// EdgeBatch without types or props
	input := EdgeBatch{
		Sources: []int64{1, 2, 3},
		Targets: []int64{4, 5, 6},
	}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if data[0] != tagEdgeBatch {
		t.Errorf("Expected tagEdgeBatch (0x14), got 0x%02X", data[0])
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	batch, ok := result.(EdgeBatch)
	if !ok {
		t.Fatalf("Expected EdgeBatch, got %T", result)
	}

	if len(batch.Sources) != 3 {
		t.Fatalf("Sources len: got %v, want 3", len(batch.Sources))
	}
	if batch.Sources[0] != 1 || batch.Targets[2] != 6 {
		t.Errorf("Sources/Targets mismatch")
	}
	if batch.Types != nil {
		t.Errorf("Types: got %v, want nil", batch.Types)
	}
	if batch.Props != nil {
		t.Errorf("Props: got %v, want nil", batch.Props)
	}
}

func TestEdgeBatchFull(t *testing.T) {
	// EdgeBatch with types and props
	input := EdgeBatch{
		Sources: []int64{1, 2, 3},
		Targets: []int64{4, 5, 6},
		Types:   []string{"KNOWS", "FOLLOWS", "LIKES"},
		Props: []map[string]any{
			{"weight": 0.9},
			{"weight": 0.8},
			{"weight": 0.7},
		},
	}

	data, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	batch := result.(EdgeBatch)

	if len(batch.Types) != 3 {
		t.Fatalf("Types len: got %v, want 3", len(batch.Types))
	}
	if batch.Types[0] != "KNOWS" || batch.Types[2] != "LIKES" {
		t.Errorf("Types: got %v", batch.Types)
	}

	if len(batch.Props) != 3 {
		t.Fatalf("Props len: got %v, want 3", len(batch.Props))
	}
	// Check with float tolerance (float32 encoding)
	w := batch.Props[0]["weight"].(float64)
	if w < 0.89 || w > 0.91 {
		t.Errorf("Props[0][weight]: got %v, want ~0.9", w)
	}
}

func TestGraphPointerEncode(t *testing.T) {
	// Test that pointer types also work
	node := &Node{ID: "ptr-node", Labels: []string{"Test"}}
	edge := &Edge{ID: "ptr-edge", Type: "REL", From: "a", To: "b"}
	adj := &AdjList{NodeID: 99, Neighbors: []int64{1, 2}}

	// Node pointer
	data, err := Encode(node)
	if err != nil {
		t.Fatalf("Encode *Node failed: %v", err)
	}
	result, _ := Decode(data)
	if result.(Node).ID != "ptr-node" {
		t.Error("*Node roundtrip failed")
	}

	// Edge pointer
	data, err = Encode(edge)
	if err != nil {
		t.Fatalf("Encode *Edge failed: %v", err)
	}
	result, _ = Decode(data)
	if result.(Edge).Type != "REL" {
		t.Error("*Edge roundtrip failed")
	}

	// AdjList pointer
	data, err = Encode(adj)
	if err != nil {
		t.Fatalf("Encode *AdjList failed: %v", err)
	}
	result, _ = Decode(data)
	if result.(AdjList).NodeID != 99 {
		t.Error("*AdjList roundtrip failed")
	}
}

func TestGraphStreamingRoundtrip(t *testing.T) {
	// Stream graph elements
	elements := []any{
		Node{ID: "n1", Labels: []string{"Person"}},
		Node{ID: "n2", Labels: []string{"Person"}},
		Edge{ID: "e1", Type: "KNOWS", From: "n1", To: "n2"},
		AdjList{NodeID: 1, Neighbors: []int64{2, 3, 4}},
	}

	var buf bytes.Buffer
	for _, e := range elements {
		if err := EncodeTo(&buf, e); err != nil {
			t.Fatalf("EncodeTo failed: %v", err)
		}
	}

	reader := bytes.NewReader(buf.Bytes())
	dec := NewStreamDecoder(reader)
	var decoded []any
	for {
		v, err := dec.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("StreamDecoder.Decode failed: %v", err)
		}
		decoded = append(decoded, v)
	}

	if len(decoded) != 4 {
		t.Fatalf("Expected 4 elements, got %d", len(decoded))
	}

	// Verify types
	if _, ok := decoded[0].(Node); !ok {
		t.Errorf("decoded[0]: expected Node, got %T", decoded[0])
	}
	if _, ok := decoded[2].(Edge); !ok {
		t.Errorf("decoded[2]: expected Edge, got %T", decoded[2])
	}
	if _, ok := decoded[3].(AdjList); !ok {
		t.Errorf("decoded[3]: expected AdjList, got %T", decoded[3])
	}
}

func BenchmarkNodeEncode(b *testing.B) {
	node := Node{
		ID:     "bench-node-12345",
		Labels: []string{"Person", "Employee", "Manager"},
		Props: map[string]any{
			"name":       "Alice",
			"age":        int64(35),
			"salary":     75000.50,
			"department": "Engineering",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Encode(node)
	}
}

func BenchmarkEdgeBatchEncode(b *testing.B) {
	batch := EdgeBatch{
		Sources: make([]int64, 1000),
		Targets: make([]int64, 1000),
		Types:   make([]string, 1000),
	}
	for i := 0; i < 1000; i++ {
		batch.Sources[i] = int64(i)
		batch.Targets[i] = int64(i + 1000)
		batch.Types[i] = "CONNECTED"
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Encode(batch)
	}
}

// ---------- GraphShard Tests ----------

func TestGraphShardMinimal(t *testing.T) {
	// Minimal shard with just a name
	gs := NewGraphShard("test-shard")

	data, err := Encode(gs)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if data[0] != tagGraphShard {
		t.Errorf("Expected tagGraphShard (0x15), got 0x%02X", data[0])
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	decoded, ok := result.(GraphShard)
	if !ok {
		t.Fatalf("Expected GraphShard, got %T", result)
	}

	if decoded.Name != "test-shard" {
		t.Errorf("Name: got %v, want test-shard", decoded.Name)
	}
}

func TestGraphShardWithNodes(t *testing.T) {
	gs := NewGraphShard("node-shard")
	gs.AddNode(Node{ID: "n1", Labels: []string{"Person"}, Props: map[string]any{"name": "Alice"}})
	gs.AddNode(Node{ID: "n2", Labels: []string{"Person"}, Props: map[string]any{"name": "Bob"}})

	data, err := Encode(gs)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	decoded := result.(GraphShard)
	if len(decoded.Nodes) != 2 {
		t.Fatalf("Nodes len: got %d, want 2", len(decoded.Nodes))
	}
	if decoded.Nodes[0].ID != "n1" {
		t.Errorf("Nodes[0].ID: got %v, want n1", decoded.Nodes[0].ID)
	}
	if decoded.Nodes[1].Props["name"] != "Bob" {
		t.Errorf("Nodes[1].Props[name]: got %v, want Bob", decoded.Nodes[1].Props["name"])
	}
}

func TestGraphShardWithEdges(t *testing.T) {
	gs := NewGraphShard("edge-shard")
	gs.AddNode(Node{ID: "n1"})
	gs.AddNode(Node{ID: "n2"})
	gs.AddEdge(Edge{ID: "e1", Type: "KNOWS", From: "n1", To: "n2"})

	data, err := Encode(gs)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	decoded := result.(GraphShard)
	if len(decoded.Edges) != 1 {
		t.Fatalf("Edges len: got %d, want 1", len(decoded.Edges))
	}
	if decoded.Edges[0].Type != "KNOWS" {
		t.Errorf("Edges[0].Type: got %v, want KNOWS", decoded.Edges[0].Type)
	}
}

func TestGraphShardWithEdgeIndex(t *testing.T) {
	gs := NewGraphShard("coo-shard")
	gs.SetEdgeIndex(
		[]int64{0, 0, 1, 2}, // sources
		[]int64{1, 2, 2, 0}, // targets
	)

	data, err := Encode(gs)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	decoded := result.(GraphShard)
	if len(decoded.EdgeIndex) != 2 {
		t.Fatalf("EdgeIndex len: got %d, want 2", len(decoded.EdgeIndex))
	}
	if len(decoded.EdgeIndex[0]) != 4 {
		t.Fatalf("EdgeIndex[0] len: got %d, want 4", len(decoded.EdgeIndex[0]))
	}
	if decoded.EdgeIndex[0][0] != 0 || decoded.EdgeIndex[1][0] != 1 {
		t.Errorf("EdgeIndex mismatch: got [%v, %v], want [0, 1]",
			decoded.EdgeIndex[0][0], decoded.EdgeIndex[1][0])
	}
}

func TestGraphShardWithFeatures(t *testing.T) {
	gs := NewGraphShard("feature-shard")

	// 3 nodes, 4-dimensional features
	features := [][]float64{
		{1.0, 2.0, 3.0, 4.0},
		{5.0, 6.0, 7.0, 8.0},
		{9.0, 10.0, 11.0, 12.0},
	}
	gs.SetNodeFeatures(features)

	data, err := Encode(gs)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	decoded := result.(GraphShard)
	if len(decoded.NodeFeatures) != 3 {
		t.Fatalf("NodeFeatures len: got %d, want 3", len(decoded.NodeFeatures))
	}
	if len(decoded.NodeFeatures[0]) != 4 {
		t.Fatalf("NodeFeatures[0] len: got %d, want 4", len(decoded.NodeFeatures[0]))
	}

	// Check with float32 tolerance
	if decoded.NodeFeatures[0][0] < 0.99 || decoded.NodeFeatures[0][0] > 1.01 {
		t.Errorf("NodeFeatures[0][0]: got %v, want ~1.0", decoded.NodeFeatures[0][0])
	}
}

func TestGraphShardWithLabels(t *testing.T) {
	gs := NewGraphShard("label-shard")
	gs.NodeLabels = []int64{0, 1, 0, 2, 1}
	gs.EdgeLabels = []int64{0, 0, 1}

	data, err := Encode(gs)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	decoded := result.(GraphShard)
	if len(decoded.NodeLabels) != 5 {
		t.Fatalf("NodeLabels len: got %d, want 5", len(decoded.NodeLabels))
	}
	if decoded.NodeLabels[3] != 2 {
		t.Errorf("NodeLabels[3]: got %v, want 2", decoded.NodeLabels[3])
	}
	if len(decoded.EdgeLabels) != 3 {
		t.Fatalf("EdgeLabels len: got %d, want 3", len(decoded.EdgeLabels))
	}
}

func TestGraphShardFull(t *testing.T) {
	// Create a complete GraphShard with all fields populated
	gs := NewGraphShard("full-shard")
	gs.Metadata = map[string]any{
		"schema_version": int64(1),
		"partition":      int64(3),
	}

	// Add nodes
	gs.AddNode(Node{ID: "alice", Labels: []string{"Person"}, Props: map[string]any{"age": int64(30)}})
	gs.AddNode(Node{ID: "bob", Labels: []string{"Person"}, Props: map[string]any{"age": int64(25)}})
	gs.AddNode(Node{ID: "carol", Labels: []string{"Person"}, Props: map[string]any{"age": int64(35)}})

	// Add edges
	gs.AddEdge(Edge{ID: "e1", Type: "KNOWS", From: "alice", To: "bob"})
	gs.AddEdge(Edge{ID: "e2", Type: "KNOWS", From: "alice", To: "carol"})

	// Set COO edge index
	gs.SetEdgeIndex([]int64{0, 0}, []int64{1, 2})

	// Build adjacency lists
	gs.BuildAdjLists()

	// Node features (3 nodes, 4 dims)
	gs.NodeFeatures = [][]float64{
		{0.1, 0.2, 0.3, 0.4},
		{0.5, 0.6, 0.7, 0.8},
		{0.9, 1.0, 1.1, 1.2},
	}

	// Labels
	gs.NodeLabels = []int64{0, 1, 0}

	data, err := Encode(gs)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	t.Logf("Full GraphShard size: %d bytes", len(data))

	result, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	decoded := result.(GraphShard)

	// Verify all fields
	if decoded.Name != "full-shard" {
		t.Errorf("Name: got %v, want full-shard", decoded.Name)
	}
	if decoded.Metadata["schema_version"] != int64(1) {
		t.Errorf("Metadata[schema_version]: got %v, want 1", decoded.Metadata["schema_version"])
	}
	if len(decoded.Nodes) != 3 {
		t.Errorf("Nodes len: got %d, want 3", len(decoded.Nodes))
	}
	if len(decoded.Edges) != 2 {
		t.Errorf("Edges len: got %d, want 2", len(decoded.Edges))
	}
	if len(decoded.EdgeIndex) != 2 || len(decoded.EdgeIndex[0]) != 2 {
		t.Errorf("EdgeIndex: unexpected shape")
	}
	if len(decoded.AdjLists) != 1 { // Only node 0 has outgoing edges
		t.Errorf("AdjLists len: got %d, want 1", len(decoded.AdjLists))
	}
	if len(decoded.NodeFeatures) != 3 {
		t.Errorf("NodeFeatures len: got %d, want 3", len(decoded.NodeFeatures))
	}
	if len(decoded.NodeLabels) != 3 {
		t.Errorf("NodeLabels len: got %d, want 3", len(decoded.NodeLabels))
	}
}

func TestGraphShardHelperMethods(t *testing.T) {
	gs := NewGraphShard("test")

	// Test NumNodes/NumEdges
	if gs.NumNodes() != 0 {
		t.Errorf("NumNodes: got %d, want 0", gs.NumNodes())
	}
	if gs.NumEdges() != 0 {
		t.Errorf("NumEdges: got %d, want 0", gs.NumEdges())
	}

	// Add some data
	gs.AddNode(Node{ID: "n1"})
	gs.AddNode(Node{ID: "n2"})
	gs.SetEdgeIndex([]int64{0, 0}, []int64{1, 1})

	if gs.NumNodes() != 2 {
		t.Errorf("NumNodes: got %d, want 2", gs.NumNodes())
	}
	if gs.NumEdges() != 2 {
		t.Errorf("NumEdges: got %d, want 2", gs.NumEdges())
	}
}

func BenchmarkGraphShardEncode(b *testing.B) {
	gs := NewGraphShard("bench-shard")

	// 100 nodes with features
	for i := 0; i < 100; i++ {
		gs.AddNode(Node{
			ID:     string(rune('a'+i%26)) + string(rune('0'+i)),
			Labels: []string{"Node"},
			Props:  map[string]any{"idx": int64(i)},
		})
	}

	// 500 edges
	sources := make([]int64, 500)
	targets := make([]int64, 500)
	for i := 0; i < 500; i++ {
		sources[i] = int64(i % 100)
		targets[i] = int64((i + 1) % 100)
	}
	gs.SetEdgeIndex(sources, targets)

	// 100 nodes × 64-dim features
	features := make([][]float64, 100)
	for i := range features {
		features[i] = make([]float64, 64)
		for j := range features[i] {
			features[i][j] = float64(i*64 + j)
		}
	}
	gs.NodeFeatures = features

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Encode(gs)
	}
}

func BenchmarkGraphShardDecode(b *testing.B) {
	gs := NewGraphShard("bench-shard")

	// Build same shard as encode benchmark
	for i := 0; i < 100; i++ {
		gs.AddNode(Node{ID: string(rune('a'+i%26)) + string(rune('0'+i)), Labels: []string{"Node"}})
	}
	sources := make([]int64, 500)
	targets := make([]int64, 500)
	for i := 0; i < 500; i++ {
		sources[i] = int64(i % 100)
		targets[i] = int64((i + 1) % 100)
	}
	gs.SetEdgeIndex(sources, targets)
	features := make([][]float64, 100)
	for i := range features {
		features[i] = make([]float64, 64)
	}
	gs.NodeFeatures = features

	data, _ := Encode(gs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Decode(data)
	}
}

// =============================================================================
// StreamDecoder Tests - Safe streaming with non-seekable readers
// =============================================================================

func TestStreamDecoderWithPipe(t *testing.T) {
	// io.Pipe creates a non-seekable reader - tests streaming decode
	pr, pw := io.Pipe()

	records := []any{
		map[string]any{"id": int64(1), "name": "first"},
		map[string]any{"id": int64(2), "name": "second"},
		map[string]any{"id": int64(3), "name": "third"},
	}

	// Writer goroutine
	go func() {
		defer pw.Close()
		for _, r := range records {
			if err := EncodeTo(pw, r); err != nil {
				t.Errorf("EncodeTo failed: %v", err)
				return
			}
		}
	}()

	// Reader using StreamDecoder
	dec := NewStreamDecoder(pr)
	var decoded []any
	for {
		v, err := dec.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("StreamDecoder.Decode failed: %v", err)
		}
		decoded = append(decoded, v)
	}

	if len(decoded) != len(records) {
		t.Fatalf("Expected %d records, got %d", len(records), len(decoded))
	}

	// Verify content
	for i, r := range decoded {
		m, ok := r.(map[string]any)
		if !ok {
			t.Errorf("decoded[%d]: expected map, got %T", i, r)
			continue
		}
		if m["id"] != int64(i+1) {
			t.Errorf("decoded[%d].id: got %v, want %d", i, m["id"], i+1)
		}
	}
}

func TestStreamDecoderMultipleRecordTypes(t *testing.T) {
	// Test streaming with different record types
	pr, pw := io.Pipe()

	records := []any{
		"hello",
		int64(42),
		[]float64{1.0, 2.0, 3.0},
		map[string]any{"key": "value"},
		true,
		nil,
	}

	go func() {
		defer pw.Close()
		for _, r := range records {
			if err := EncodeTo(pw, r); err != nil {
				t.Errorf("EncodeTo failed: %v", err)
				return
			}
		}
	}()

	dec := NewStreamDecoder(pr)
	var decoded []any
	for {
		v, err := dec.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		decoded = append(decoded, v)
	}

	if len(decoded) != len(records) {
		t.Fatalf("Expected %d records, got %d", len(records), len(decoded))
	}

	// Verify types
	if _, ok := decoded[0].(string); !ok {
		t.Errorf("decoded[0]: expected string, got %T", decoded[0])
	}
	if _, ok := decoded[1].(int64); !ok {
		t.Errorf("decoded[1]: expected int64, got %T", decoded[1])
	}
	if _, ok := decoded[3].(map[string]any); !ok {
		t.Errorf("decoded[3]: expected map, got %T", decoded[3])
	}
}

func TestStreamDecoderLargeRecords(t *testing.T) {
	// Test with records larger than default buffer size
	pr, pw := io.Pipe()

	// Create a large float array (larger than 4KB default buffer)
	largeArray := make([]float64, 2000) // ~16KB when encoded
	for i := range largeArray {
		largeArray[i] = float64(i) * 0.001
	}

	records := []any{
		map[string]any{"data": largeArray},
		map[string]any{"small": "record"},
		map[string]any{"data": largeArray},
	}

	go func() {
		defer pw.Close()
		for _, r := range records {
			if err := EncodeTo(pw, r); err != nil {
				t.Errorf("EncodeTo failed: %v", err)
				return
			}
		}
	}()

	dec := NewStreamDecoder(pr)
	count := 0
	for {
		_, err := dec.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Decode failed on record %d: %v", count, err)
		}
		count++
	}

	if count != 3 {
		t.Fatalf("Expected 3 records, got %d", count)
	}
}

// =============================================================================
// EncodeOptions / HighPrecision Tests
// =============================================================================

func TestHighPrecisionFloat64(t *testing.T) {
	// Test that HighPrecision: true preserves float64 precision
	// These values lose precision when converted to float32
	highPrecisionValues := []float64{
		1.0000000000001,         // Needs more than float32 precision
		123456789.123456789,     // Large with fractional
		0.123456789012345,       // Many decimal places
		1e-40,                   // Very small (float32 denormal)
		1.7976931348623157e+308, // Near float64 max
	}

	// Lossy encoding (float32) - uses HighPrecision: false
	lossyData, err := EncodeWithOptions(highPrecisionValues, EncodeOptions{HighPrecision: false})
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// High precision encoding (lossless float64) - default is now HighPrecision: true
	hpData, err := Encode(highPrecisionValues)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// High precision should be larger (8 bytes/float vs 4 bytes/float)
	if len(hpData) <= len(lossyData) {
		t.Errorf("HighPrecision data should be larger: hp=%d, lossy=%d", len(hpData), len(lossyData))
	}

	// Decode high precision and verify exact values
	decoded, err := Decode(hpData)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	decodedSlice, ok := decoded.([]float64)
	if !ok {
		t.Fatalf("Expected []float64, got %T", decoded)
	}

	for i, expected := range highPrecisionValues {
		if decodedSlice[i] != expected {
			t.Errorf("Value %d: got %v, want %v (exact match required)", i, decodedSlice[i], expected)
		}
	}
}

func TestHighPrecisionInMap(t *testing.T) {
	// Test HighPrecision with nested structures
	data := map[string]any{
		"embeddings": []float64{1.123456789012345, 2.234567890123456},
		"config": map[string]any{
			"threshold": []float64{0.999999999999999},
		},
	}

	hpData, err := EncodeWithOptions(data, EncodeOptions{HighPrecision: true})
	if err != nil {
		t.Fatalf("EncodeWithOptions failed: %v", err)
	}

	decoded, err := Decode(hpData)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	m := decoded.(map[string]any)
	embeddings, ok := m["embeddings"].([]float64)
	if !ok {
		t.Fatalf("embeddings: expected []float64, got %T", m["embeddings"])
	}

	if embeddings[0] != 1.123456789012345 {
		t.Errorf("embeddings[0]: got %v, want exact 1.123456789012345", embeddings[0])
	}
}

func TestDefaultVsHighPrecision(t *testing.T) {
	// Verify default behavior is now lossless (HighPrecision: true for cross-language compatibility)
	values := []float64{1.123456789012345}

	// Default (HighPrecision: true) should preserve precision
	defaultData, _ := Encode(values)
	decoded, _ := Decode(defaultData)
	decodedSlice := decoded.([]float64)

	// Default should now preserve precision (float64)
	if decodedSlice[0] != 1.123456789012345 {
		t.Errorf("Default encoding should preserve precision (HighPrecision: true), got %v", decodedSlice[0])
	}

	// Test that HighPrecision: false gives lossy behavior
	lossyData, _ := EncodeWithOptions(values, EncodeOptions{HighPrecision: false})
	decodedLossy, _ := Decode(lossyData)
	decodedLossySlice := decodedLossy.([]float64)

	// HighPrecision: false should lose precision (converted to float32)
	if decodedLossySlice[0] == 1.123456789012345 {
		t.Error("HighPrecision: false should lose precision (use float32)")
	}

	// But should be close
	diff := decodedLossySlice[0] - 1.123456789012345
	if diff < -0.0001 || diff > 0.0001 {
		t.Errorf("HighPrecision: false too lossy: got %v", decodedLossySlice[0])
	}
}

func TestHighPrecisionMatrix(t *testing.T) {
	// Test with 2D matrix
	matrix := [][]float64{
		{1.123456789012345, 2.234567890123456},
		{3.345678901234567, 4.456789012345678},
	}

	hpData, err := EncodeWithOptions(matrix, EncodeOptions{HighPrecision: true})
	if err != nil {
		t.Fatalf("EncodeWithOptions failed: %v", err)
	}

	decoded, err := Decode(hpData)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Decode as []any containing []float64
	decodedMatrix, ok := decoded.([]any)
	if !ok {
		t.Fatalf("Expected []any, got %T", decoded)
	}
	for i, row := range decodedMatrix {
		rowSlice, ok := row.([]float64)
		if !ok {
			t.Fatalf("row %d: expected []float64, got %T", i, row)
		}
		for j, val := range rowSlice {
			if val != matrix[i][j] {
				t.Errorf("matrix[%d][%d]: got %v, want %v", i, j, val, matrix[i][j])
			}
		}
	}
}

func TestSetDefaultEncodeOptions(t *testing.T) {
	// Save original
	original := globalEncodeOptions

	// Set high precision as default
	SetDefaultEncodeOptions(EncodeOptions{HighPrecision: true})

	values := []float64{1.123456789012345}
	data, _ := Encode(values)
	decoded, _ := Decode(data)
	decodedSlice := decoded.([]float64)

	// Should preserve precision with new default
	if decodedSlice[0] != 1.123456789012345 {
		t.Errorf("SetDefaultEncodeOptions not respected: got %v", decodedSlice[0])
	}

	// Restore original
	SetDefaultEncodeOptions(original)
}
