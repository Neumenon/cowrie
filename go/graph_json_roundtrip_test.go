package cowrie

import (
	"testing"
)

// TestGraphJSONRoundtrip verifies that Encode(FromJSON(ToJSON(x))) succeeds for
// all four graph types (node, edge, node_batch, edge_batch) and that the
// props survive the round-trip with correct Go types.
//
// Before the fix, FromJSON left json.Number values in props maps, causing
// Encode to fail with "unsupported property type json.Number".
func TestGraphJSONRoundtrip(t *testing.T) {
	t.Run("node with int float string nested props", func(t *testing.T) {
		orig := Node("n1", []string{"Person"}, map[string]any{
			"age":    int64(42),
			"score":  float64(3.14),
			"name":   "Alice",
			"nested": map[string]any{"x": int64(1)},
		})

		js, err := ToJSON(orig)
		if err != nil {
			t.Fatalf("ToJSON: %v", err)
		}

		back, err := FromJSON(js)
		if err != nil {
			t.Fatalf("FromJSON: %v", err)
		}

		_, err = Encode(back)
		if err != nil {
			t.Fatalf("Encode after round-trip: %v", err)
		}

		// Verify the props survived
		if back.typ != TypeNode {
			t.Fatalf("expected TypeNode, got %v", back.typ)
		}
		props := back.nodeVal.Props
		if age, ok := props["age"].(int64); !ok || age != 42 {
			t.Errorf("props[age]: got %T(%v), want int64(42)", props["age"], props["age"])
		}
		if score, ok := props["score"].(float64); !ok || score != 3.14 {
			t.Errorf("props[score]: got %T(%v), want float64(3.14)", props["score"], props["score"])
		}
		if name, ok := props["name"].(string); !ok || name != "Alice" {
			t.Errorf("props[name]: got %T(%v), want string(Alice)", props["name"], props["name"])
		}
	})

	t.Run("edge with int props", func(t *testing.T) {
		orig := Edge("n1", "n2", "KNOWS", map[string]any{
			"since":  int64(2020),
			"weight": float64(0.75),
		})

		js, err := ToJSON(orig)
		if err != nil {
			t.Fatalf("ToJSON: %v", err)
		}

		back, err := FromJSON(js)
		if err != nil {
			t.Fatalf("FromJSON: %v", err)
		}

		_, err = Encode(back)
		if err != nil {
			t.Fatalf("Encode after round-trip: %v", err)
		}

		props := back.edgeVal.Props
		if since, ok := props["since"].(int64); !ok || since != 2020 {
			t.Errorf("props[since]: got %T(%v), want int64(2020)", props["since"], props["since"])
		}
	})

	t.Run("node_batch", func(t *testing.T) {
		nodes := []NodeData{
			{ID: "n1", Labels: []string{"Person"}, Props: map[string]any{"age": int64(30)}},
			{ID: "n2", Labels: []string{"Person"}, Props: map[string]any{"age": int64(25)}},
		}
		orig := NodeBatch(nodes)

		js, err := ToJSON(orig)
		if err != nil {
			t.Fatalf("ToJSON: %v", err)
		}

		back, err := FromJSON(js)
		if err != nil {
			t.Fatalf("FromJSON: %v", err)
		}

		_, err = Encode(back)
		if err != nil {
			t.Fatalf("Encode after round-trip: %v", err)
		}
	})

	t.Run("edge_batch", func(t *testing.T) {
		edges := []EdgeData{
			{From: "n1", To: "n2", Type: "KNOWS", Props: map[string]any{"since": int64(2019)}},
			{From: "n2", To: "n3", Type: "FOLLOWS", Props: map[string]any{"weight": float64(1.5)}},
		}
		orig := EdgeBatch(edges)

		js, err := ToJSON(orig)
		if err != nil {
			t.Fatalf("ToJSON: %v", err)
		}

		back, err := FromJSON(js)
		if err != nil {
			t.Fatalf("FromJSON: %v", err)
		}

		_, err = Encode(back)
		if err != nil {
			t.Fatalf("Encode after round-trip: %v", err)
		}
	})
}
