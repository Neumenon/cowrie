package cowrie

import (
	"math/big"
	"testing"
)

// TestBigIntTwosComplementHelpers verifies the low-level helpers for a range
// of values including zero, positive, negative, exact power-of-two boundaries,
// and large multi-byte negatives.
func TestBigIntTwosComplementHelpers(t *testing.T) {
	cases := []string{
		"0",
		"1",
		"-1",
		"127",
		"-127",
		"128",
		"-128",
		"255",
		"-255",
		"256",
		"-256",
		"9007199254740991",  // JS maxSafeInt
		"-9007199254740991", // JS minSafeInt
		"123456789012345678901234567890",
		"-123456789012345678901234567890",
	}

	for _, s := range cases {
		n := new(big.Int)
		n.SetString(s, 10)

		encoded := bigIntToTwosComplement(n)
		decoded := bigIntFromTwosComplement(encoded)

		if decoded.Cmp(n) != 0 {
			t.Errorf("round-trip failed for %s: got %s (encoded bytes: %x)", s, decoded.String(), encoded)
		}
	}
}

// TestNegativeBigIntRoundTrip confirms that a negative BigInt stored in a
// Cowrie Value survives a full ToAny → string → FromAny round-trip.
// This is the primary regression test for the sign-loss bug.
func TestNegativeBigIntRoundTrip(t *testing.T) {
	// -1 is the simplest negative that exposed the bug (was returning "1").
	negOne := new(big.Int).SetInt64(-1)
	v := BigInt(bigIntToTwosComplement(negOne))

	// ToAny must produce the string "-1"
	got := ToAny(v)
	if got != "-1" {
		t.Fatalf("ToAny(-1 BigInt) = %v, want \"-1\"", got)
	}

	// A larger negative
	large := new(big.Int)
	large.SetString("-123456789012345678901234567890", 10)
	v2 := BigInt(bigIntToTwosComplement(large))
	got2 := ToAny(v2)
	if got2 != "-123456789012345678901234567890" {
		t.Fatalf("ToAny(large negative BigInt) = %v, want \"-123456789012345678901234567890\"", got2)
	}
}

// TestToAnyNode verifies that a Node value projects to a map with "_type":"node"
// and the expected fields.
func TestToAnyNode(t *testing.T) {
	v := Node("n1", []string{"Person", "Employee"}, map[string]any{"age": 42})
	got, ok := ToAny(v).(map[string]any)
	if !ok {
		t.Fatalf("ToAny(Node) did not return map[string]any, got %T", ToAny(v))
	}
	if got["_type"] != "node" {
		t.Errorf("_type = %v, want \"node\"", got["_type"])
	}
	if got["id"] != "n1" {
		t.Errorf("id = %v, want \"n1\"", got["id"])
	}
	labels, ok := got["labels"].([]any)
	if !ok || len(labels) != 2 {
		t.Errorf("labels = %v, want [Person Employee]", got["labels"])
	}
	props, ok := got["props"].(map[string]any)
	if !ok {
		t.Errorf("props = %v, want map", got["props"])
	}
	if props["age"] != 42 {
		t.Errorf("props[age] = %v, want 42", props["age"])
	}
}

// TestToAnyEdge verifies that an Edge value projects correctly.
func TestToAnyEdge(t *testing.T) {
	v := Edge("n1", "n2", "KNOWS", map[string]any{"since": 2020})
	got, ok := ToAny(v).(map[string]any)
	if !ok {
		t.Fatalf("ToAny(Edge) did not return map[string]any, got %T", ToAny(v))
	}
	if got["_type"] != "edge" {
		t.Errorf("_type = %v, want \"edge\"", got["_type"])
	}
	if got["fromId"] != "n1" {
		t.Errorf("fromId = %v, want \"n1\"", got["fromId"])
	}
	if got["toId"] != "n2" {
		t.Errorf("toId = %v, want \"n2\"", got["toId"])
	}
	if got["type"] != "KNOWS" {
		t.Errorf("type = %v, want \"KNOWS\"", got["type"])
	}
}

// TestToAnyBitmask verifies that a Bitmask value projects to a map with
// "_type":"bitmask" and the expected count field.
func TestToAnyBitmask(t *testing.T) {
	v := BitmaskFromBools([]bool{true, false, true, true, false})
	got, ok := ToAny(v).(map[string]any)
	if !ok {
		t.Fatalf("ToAny(Bitmask) did not return map[string]any, got %T", ToAny(v))
	}
	if got["_type"] != "bitmask" {
		t.Errorf("_type = %v, want \"bitmask\"", got["_type"])
	}
	if got["count"] != uint64(5) {
		t.Errorf("count = %v (%T), want uint64(5)", got["count"], got["count"])
	}
	if _, ok := got["bits"].(string); !ok {
		t.Errorf("bits should be a base64 string, got %T", got["bits"])
	}
}

// TestToAnyNodeBatch verifies node_batch projection.
func TestToAnyNodeBatch(t *testing.T) {
	nodes := []NodeData{
		{ID: "a", Labels: []string{"X"}, Props: nil},
		{ID: "b", Labels: []string{"Y"}, Props: nil},
	}
	v := NodeBatch(nodes)
	got, ok := ToAny(v).(map[string]any)
	if !ok {
		t.Fatalf("ToAny(NodeBatch) did not return map[string]any, got %T", ToAny(v))
	}
	if got["_type"] != "node_batch" {
		t.Errorf("_type = %v, want \"node_batch\"", got["_type"])
	}
	arr, ok := got["nodes"].([]any)
	if !ok || len(arr) != 2 {
		t.Errorf("nodes = %v, want 2-element slice", got["nodes"])
	}
}

// TestToAnyEdgeBatch verifies edge_batch projection.
func TestToAnyEdgeBatch(t *testing.T) {
	edges := []EdgeData{
		{From: "a", To: "b", Type: "REL", Props: nil},
	}
	v := EdgeBatch(edges)
	got, ok := ToAny(v).(map[string]any)
	if !ok {
		t.Fatalf("ToAny(EdgeBatch) did not return map[string]any, got %T", ToAny(v))
	}
	if got["_type"] != "edge_batch" {
		t.Errorf("_type = %v, want \"edge_batch\"", got["_type"])
	}
	arr, ok := got["edges"].([]any)
	if !ok || len(arr) != 1 {
		t.Errorf("edges = %v, want 1-element slice", got["edges"])
	}
}
