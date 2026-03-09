package graph

import (
	"testing"
)

func TestEncodeDecodeAllPropertyTypes(t *testing.T) {
	// Create a node event with various property types to cover encodeAny/decodeAny
	node := &NodeEvent{
		ID:     "n1",
		Labels: []string{"Person"},
		Props: map[string]any{
			"name":    "Alice",
			"age":     42,
			"age64":   int64(42),
			"count":   uint64(100),
			"score":   3.14,
			"active":  true,
			"nothing": nil,
			"data":    []byte{1, 2, 3},
			"tags":    []any{"a", "b", "c"},
			"nested":  map[string]any{"x": 1, "y": "z"},
		},
	}

	header := NewStreamHeader()
	encoded := encodeEvent(Event{Kind: EventNode, Node: node}, header)

	decoded, err := decodeEvent(encoded, header)
	if err != nil {
		t.Fatalf("decodeEvent: %v", err)
	}
	if decoded.Kind != EventNode {
		t.Error("expected node event")
	}
	if decoded.Node.ID != "n1" {
		t.Error("node ID mismatch")
	}
}

func TestEncodeDecodeEdgeWithProps(t *testing.T) {
	edge := &EdgeEvent{
		FromID: "n1",
		ToID:   "n2",
		Label:  "KNOWS",
		Props: map[string]any{
			"since":  "2024",
			"weight": 0.5,
		},
	}

	header := NewStreamHeader()
	encoded := encodeEvent(Event{Kind: EventEdge, Edge: edge}, header)

	decoded, err := decodeEvent(encoded, header)
	if err != nil {
		t.Fatalf("decodeEvent: %v", err)
	}
	if decoded.Kind != EventEdge {
		t.Error("expected edge event")
	}
}

func TestEncodeDecodeTripleWithProps(t *testing.T) {
	triple := &TripleEvent{
		Subject:   RDFTerm{Kind: TermIRI, Value: "http://example.org/n1"},
		Predicate: "http://example.org/knows",
		Object:    RDFTerm{Kind: TermIRI, Value: "http://example.org/n2"},
	}

	header := NewStreamHeader()
	encoded := encodeEvent(Event{Kind: EventTriple, Triple: triple}, header)

	decoded, err := decodeEvent(encoded, header)
	if err != nil {
		t.Fatalf("decodeEvent: %v", err)
	}
	if decoded.Kind != EventTriple {
		t.Error("expected triple event")
	}
}
