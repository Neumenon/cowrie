package loader

import (
	"bytes"
	"testing"

	"github.com/Neumenon/cowrie/go/graph"
	"github.com/Neumenon/cowrie/go/ld"
)

func createTestStream(t *testing.T) []byte {
	var buf bytes.Buffer
	sw := graph.NewStreamWriter(&buf)

	sw.Header().AddLabel("Person")
	sw.Header().AddLabel("KNOWS")

	if err := sw.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	sw.WriteNode(&graph.NodeEvent{
		Op:     graph.OpUpsert,
		ID:     "alice",
		Labels: []string{"Person"},
		Props:  map[string]any{"name": "Alice", "age": int64(30)},
	})

	sw.WriteNode(&graph.NodeEvent{
		Op:     graph.OpUpsert,
		ID:     "bob",
		Labels: []string{"Person"},
		Props:  map[string]any{"name": "Bob", "age": int64(25)},
	})

	sw.WriteEdge(&graph.EdgeEvent{
		Op:     graph.OpUpsert,
		Label:  "KNOWS",
		FromID: "alice",
		ToID:   "bob",
		Props:  map[string]any{"since": int64(2020)},
	})

	sw.Close()
	return buf.Bytes()
}

func TestLoadFromStreamToMemory(t *testing.T) {
	data := createTestStream(t)

	mg := NewMemoryGraph()
	if err := LoadFromStream(data, mg); err != nil {
		t.Fatalf("LoadFromStream failed: %v", err)
	}

	// Verify nodes
	if mg.NodeCount() != 2 {
		t.Errorf("Expected 2 nodes, got %d", mg.NodeCount())
	}

	alice := mg.GetNode("alice")
	if alice == nil {
		t.Fatal("Node 'alice' not found")
	}
	if alice.Props["name"] != "Alice" {
		t.Errorf("Expected name=Alice, got %v", alice.Props["name"])
	}

	// Verify edges
	if mg.EdgeCount() != 1 {
		t.Errorf("Expected 1 edge, got %d", mg.EdgeCount())
	}

	edges := mg.FindEdgesByLabel("KNOWS")
	if len(edges) != 1 {
		t.Fatalf("Expected 1 KNOWS edge, got %d", len(edges))
	}
	if edges[0].FromID != "alice" || edges[0].ToID != "bob" {
		t.Error("Edge endpoints incorrect")
	}
}

func TestMemoryGraphUpsert(t *testing.T) {
	mg := NewMemoryGraph()

	// Initial write
	mg.WriteNode(&graph.NodeEvent{
		Op:     graph.OpUpsert,
		ID:     "node1",
		Labels: []string{"Label1"},
		Props:  map[string]any{"key": "value1"},
	})

	// Update with same ID
	mg.WriteNode(&graph.NodeEvent{
		Op:     graph.OpUpsert,
		ID:     "node1",
		Labels: []string{"Label2"},
		Props:  map[string]any{"key": "value2", "extra": "prop"},
	})

	if mg.NodeCount() != 1 {
		t.Errorf("Expected 1 node (upsert), got %d", mg.NodeCount())
	}

	node := mg.GetNode("node1")
	if node.Props["key"] != "value2" {
		t.Error("Property should be updated")
	}
	if node.Props["extra"] != "prop" {
		t.Error("New property should be added")
	}
	if len(node.Labels) != 1 || node.Labels[0] != "Label2" {
		t.Error("Labels should be updated")
	}
}

func TestMemoryGraphDelete(t *testing.T) {
	mg := NewMemoryGraph()

	mg.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "node1"})
	mg.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "node2"})

	if mg.NodeCount() != 2 {
		t.Fatal("Setup failed")
	}

	mg.WriteNode(&graph.NodeEvent{Op: graph.OpDelete, ID: "node1"})

	if mg.NodeCount() != 1 {
		t.Errorf("Expected 1 node after delete, got %d", mg.NodeCount())
	}
	if mg.GetNode("node1") != nil {
		t.Error("Deleted node should not exist")
	}
	if mg.GetNode("node2") == nil {
		t.Error("node2 should still exist")
	}
}

func TestMemoryGraphTriples(t *testing.T) {
	mg := NewMemoryGraph()

	mg.WriteTriple(&graph.TripleEvent{
		Op:        graph.OpUpsert,
		Subject:   graph.NewIRITerm("http://example.org/alice"),
		Predicate: "http://schema.org/name",
		Object:    graph.NewLiteralTerm("Alice"),
	})

	if mg.TripleCount() != 1 {
		t.Errorf("Expected 1 triple, got %d", mg.TripleCount())
	}

	// Duplicate should not be added
	mg.WriteTriple(&graph.TripleEvent{
		Op:        graph.OpUpsert,
		Subject:   graph.NewIRITerm("http://example.org/alice"),
		Predicate: "http://schema.org/name",
		Object:    graph.NewLiteralTerm("Alice"),
	})

	if mg.TripleCount() != 1 {
		t.Errorf("Duplicate triple should not be added, got %d", mg.TripleCount())
	}

	// Delete
	mg.WriteTriple(&graph.TripleEvent{
		Op:        graph.OpDelete,
		Subject:   graph.NewIRITerm("http://example.org/alice"),
		Predicate: "http://schema.org/name",
		Object:    graph.NewLiteralTerm("Alice"),
	})

	if mg.TripleCount() != 0 {
		t.Errorf("Expected 0 triples after delete, got %d", mg.TripleCount())
	}
}

func TestMemoryGraphQueries(t *testing.T) {
	mg := NewMemoryGraph()

	mg.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "a", Labels: []string{"Person"}})
	mg.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "b", Labels: []string{"Person"}})
	mg.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "c", Labels: []string{"Company"}})
	mg.WriteEdge(&graph.EdgeEvent{Op: graph.OpUpsert, Label: "WORKS_FOR", FromID: "a", ToID: "c"})
	mg.WriteEdge(&graph.EdgeEvent{Op: graph.OpUpsert, Label: "KNOWS", FromID: "a", ToID: "b"})

	// FindNodesByLabel
	persons := mg.FindNodesByLabel("Person")
	if len(persons) != 2 {
		t.Errorf("Expected 2 Person nodes, got %d", len(persons))
	}

	// FindEdgesByLabel
	knows := mg.FindEdgesByLabel("KNOWS")
	if len(knows) != 1 {
		t.Errorf("Expected 1 KNOWS edge, got %d", len(knows))
	}

	// OutgoingEdges
	outgoing := mg.OutgoingEdges("a")
	if len(outgoing) != 2 {
		t.Errorf("Expected 2 outgoing edges from 'a', got %d", len(outgoing))
	}

	// IncomingEdges
	incoming := mg.IncomingEdges("c")
	if len(incoming) != 1 {
		t.Errorf("Expected 1 incoming edge to 'c', got %d", len(incoming))
	}
}

func TestBatchingWriter(t *testing.T) {
	mg := NewMemoryGraph()
	bw := NewBatchingWriter(mg, 2) // Small batch size for testing

	// Write 3 nodes - should trigger one flush
	bw.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "n1"})
	bw.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "n2"})
	// At this point, batch should flush (size=2)
	if mg.NodeCount() != 2 {
		t.Errorf("Expected 2 nodes after auto-flush, got %d", mg.NodeCount())
	}

	bw.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "n3"})
	// Not flushed yet
	if mg.NodeCount() != 2 {
		t.Errorf("Expected 2 nodes before manual flush, got %d", mg.NodeCount())
	}

	bw.Flush()
	if mg.NodeCount() != 3 {
		t.Errorf("Expected 3 nodes after flush, got %d", mg.NodeCount())
	}
}

func TestStatsWriter(t *testing.T) {
	mg := NewMemoryGraph()
	sw := NewStatsWriter(mg)

	sw.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "n1"})
	sw.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "n2"})
	sw.WriteNode(&graph.NodeEvent{Op: graph.OpDelete, ID: "n1"})
	sw.WriteEdge(&graph.EdgeEvent{Op: graph.OpUpsert, Label: "REL", FromID: "a", ToID: "b"})

	stats := sw.Stats()
	if stats.NodesWritten != 2 {
		t.Errorf("Expected NodesWritten=2, got %d", stats.NodesWritten)
	}
	if stats.NodesDeleted != 1 {
		t.Errorf("Expected NodesDeleted=1, got %d", stats.NodesDeleted)
	}
	if stats.EdgesWritten != 1 {
		t.Errorf("Expected EdgesWritten=1, got %d", stats.EdgesWritten)
	}
}

func TestNeo4jWriterMock(t *testing.T) {
	mock := &MockNeo4jExecutor{}
	nw := NewNeo4jWriter(mock, 100)

	nw.WriteNode(&graph.NodeEvent{
		Op:     graph.OpUpsert,
		ID:     "alice",
		Labels: []string{"Person"},
		Props:  map[string]any{"name": "Alice"},
	})

	nw.WriteEdge(&graph.EdgeEvent{
		Op:     graph.OpUpsert,
		Label:  "KNOWS",
		FromID: "alice",
		ToID:   "bob",
	})

	nw.Flush()

	if len(mock.Queries) != 2 {
		t.Errorf("Expected 2 queries, got %d", len(mock.Queries))
	}

	// Verify node query has proper structure
	nodeQuery := mock.Queries[0]
	if nodeQuery.Params["id"] != "alice" {
		t.Error("Node query should have id=alice")
	}

	// Verify edge query
	edgeQuery := mock.Queries[1]
	if edgeQuery.Params["from"] != "alice" || edgeQuery.Params["to"] != "bob" {
		t.Error("Edge query should have from=alice, to=bob")
	}
}

func TestNeo4jWriterDelete(t *testing.T) {
	mock := &MockNeo4jExecutor{}
	nw := NewNeo4jWriter(mock, 100)

	nw.WriteNode(&graph.NodeEvent{Op: graph.OpDelete, ID: "node1"})
	nw.WriteEdge(&graph.EdgeEvent{Op: graph.OpDelete, Label: "REL", FromID: "a", ToID: "b"})
	nw.Flush()

	if len(mock.Queries) != 2 {
		t.Fatalf("Expected 2 queries, got %d", len(mock.Queries))
	}

	// Node delete query should contain DETACH DELETE
	if mock.Queries[0].Params["id"] != "node1" {
		t.Error("Delete query should have id=node1")
	}
}

func TestNeo4jWriterTriples(t *testing.T) {
	mock := &MockNeo4jExecutor{}
	nw := NewNeo4jWriter(mock, 100)

	nw.WriteTriple(&graph.TripleEvent{
		Op:        graph.OpUpsert,
		Subject:   graph.NewIRITerm("http://example.org/alice"),
		Predicate: "http://schema.org/name",
		Object:    graph.NewTypedLiteral("Alice", ld.XSDString),
	})
	nw.Flush()

	if len(mock.Queries) != 1 {
		t.Fatalf("Expected 1 query, got %d", len(mock.Queries))
	}
}

func TestSanitizeLabel(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Person", "Person"},
		{"my-label", "my_label"},
		{"123label", "_23label"},
		{"", "_"},
		{"WORKS_FOR", "WORKS_FOR"},
	}

	for _, tt := range tests {
		result := sanitizeLabel(tt.input)
		if result != tt.expected {
			t.Errorf("sanitizeLabel(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestPredicateToRelType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"http://schema.org/knows", "knows"},
		{"http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "type"},
		{"http://xmlns.com/foaf/0.1/name", "name"},
		{"simple", "simple"},
	}

	for _, tt := range tests {
		result := predicateToRelType(tt.input)
		if result != tt.expected {
			t.Errorf("predicateToRelType(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestMemoryGraphClear(t *testing.T) {
	mg := NewMemoryGraph()

	mg.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "n1"})
	mg.WriteEdge(&graph.EdgeEvent{Op: graph.OpUpsert, Label: "R", FromID: "a", ToID: "b"})
	mg.WriteTriple(&graph.TripleEvent{Op: graph.OpUpsert, Subject: graph.NewIRITerm("s"), Predicate: "p", Object: graph.NewIRITerm("o")})

	mg.Clear()

	if mg.NodeCount() != 0 {
		t.Error("Clear should remove all nodes")
	}
	if mg.EdgeCount() != 0 {
		t.Error("Clear should remove all edges")
	}
	if mg.TripleCount() != 0 {
		t.Error("Clear should remove all triples")
	}
}

func BenchmarkLoadToMemory(b *testing.B) {
	// Create a larger stream
	var buf bytes.Buffer
	sw := graph.NewStreamWriter(&buf)
	sw.Header().AddLabel("Node")
	sw.Header().AddLabel("LINK")
	sw.WriteHeader()

	for i := 0; i < 1000; i++ {
		sw.WriteNode(&graph.NodeEvent{
			Op:     graph.OpUpsert,
			ID:     string(rune(i)),
			Labels: []string{"Node"},
			Props:  map[string]any{"value": int64(i)},
		})
	}
	sw.Close()

	data := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mg := NewMemoryGraph()
		LoadFromStream(data, mg)
	}
}
