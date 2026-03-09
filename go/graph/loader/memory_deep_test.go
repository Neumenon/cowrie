package loader

import (
	"testing"

	"github.com/Neumenon/cowrie/go/graph"
)

func TestMemoryGraph_GetEdge(t *testing.T) {
	mg := NewMemoryGraph()

	mg.WriteEdge(&graph.EdgeEvent{
		Op:     graph.OpUpsert,
		ID:     "e1",
		FromID: "n1",
		ToID:   "n2",
		Label:  "knows",
		Props:  map[string]any{"since": "2024"},
	})

	e := mg.GetEdge("e1")
	if e == nil {
		t.Fatal("nil edge")
	}
	if e.Label != "knows" {
		t.Error("wrong label")
	}

	// Non-existent edge
	e2 := mg.GetEdge("nonexistent")
	if e2 != nil {
		t.Error("should be nil")
	}
}

func TestMemoryGraph_AllNodes(t *testing.T) {
	mg := NewMemoryGraph()

	mg.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "n1", Labels: []string{"A"}})
	mg.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "n2", Labels: []string{"B"}})

	nodes := mg.AllNodes()
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}
}

func TestMemoryGraph_AllEdges(t *testing.T) {
	mg := NewMemoryGraph()

	mg.WriteEdge(&graph.EdgeEvent{Op: graph.OpUpsert, ID: "e1", FromID: "n1", ToID: "n2", Label: "R"})
	mg.WriteEdge(&graph.EdgeEvent{Op: graph.OpUpsert, ID: "e2", FromID: "n2", ToID: "n3", Label: "S"})

	edges := mg.AllEdges()
	if len(edges) != 2 {
		t.Errorf("expected 2 edges, got %d", len(edges))
	}
}

func TestMemoryGraph_AllTriples(t *testing.T) {
	mg := NewMemoryGraph()

	mg.WriteTriple(&graph.TripleEvent{
		Subject:   graph.NewIRITerm("s1"),
		Predicate: "p1",
		Object:    graph.NewIRITerm("o1"),
	})
	mg.WriteTriple(&graph.TripleEvent{
		Subject:   graph.NewIRITerm("s2"),
		Predicate: "p2",
		Object:    graph.NewLiteralTerm("lit"),
	})

	triples := mg.AllTriples()
	if len(triples) != 2 {
		t.Errorf("expected 2 triples, got %d", len(triples))
	}
}

func TestMemoryGraph_DeleteNode(t *testing.T) {
	mg := NewMemoryGraph()
	mg.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "n1", Labels: []string{"A"}})
	mg.WriteNode(&graph.NodeEvent{Op: graph.OpDelete, ID: "n1"})

	n := mg.GetNode("n1")
	if n != nil {
		t.Error("expected node to be deleted")
	}
}

func TestMemoryGraph_DeleteEdge(t *testing.T) {
	mg := NewMemoryGraph()
	mg.WriteEdge(&graph.EdgeEvent{Op: graph.OpUpsert, ID: "e1", FromID: "n1", ToID: "n2", Label: "R"})
	mg.WriteEdge(&graph.EdgeEvent{Op: graph.OpDelete, ID: "e1"})

	e := mg.GetEdge("e1")
	if e != nil {
		t.Error("expected edge to be deleted")
	}
}

func TestMemoryGraph_UpsertMerge(t *testing.T) {
	mg := NewMemoryGraph()

	// First write
	mg.WriteNode(&graph.NodeEvent{
		Op:     graph.OpUpsert,
		ID:     "n1",
		Labels: []string{"A"},
		Props:  map[string]any{"name": "Alice"},
	})

	// Second write merges props
	mg.WriteNode(&graph.NodeEvent{
		Op:    graph.OpUpsert,
		ID:    "n1",
		Props: map[string]any{"age": 30},
	})

	n := mg.GetNode("n1")
	if n == nil {
		t.Fatal("nil node")
	}
	if n.Props["name"] != "Alice" {
		t.Error("original prop missing")
	}
	if n.Props["age"] != 30 {
		t.Error("merged prop missing")
	}
}
