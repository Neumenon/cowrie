package loader

import (
	"testing"

	"github.com/Neumenon/cowrie/go/graph"
)

func TestBatchingWriter_EdgesAndTriples(t *testing.T) {
	mg := NewMemoryGraph()
	bw := NewBatchingWriter(mg, 2) // Small batch size for testing

	// Write edges
	for i := 0; i < 5; i++ {
		err := bw.WriteEdge(&graph.EdgeEvent{
			Op:     graph.OpUpsert,
			FromID: "n1",
			ToID:   "n2",
			Label:  "knows",
		})
		if err != nil {
			t.Fatalf("WriteEdge failed: %v", err)
		}
	}

	// Write triples
	for i := 0; i < 5; i++ {
		err := bw.WriteTriple(&graph.TripleEvent{
			Subject:   graph.NewIRITerm("n1"),
			Predicate: "knows",
			Object:    graph.NewIRITerm("n2"),
		})
		if err != nil {
			t.Fatalf("WriteTriple failed: %v", err)
		}
	}

	// Close
	if err := bw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestBatchingWriter_Flush(t *testing.T) {
	mg := NewMemoryGraph()
	bw := NewBatchingWriter(mg, 100) // Large batch size

	// Write a few nodes that won't trigger auto-flush
	bw.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "n1", Labels: []string{"A"}})
	bw.WriteEdge(&graph.EdgeEvent{Op: graph.OpUpsert, FromID: "n1", ToID: "n2", Label: "R"})
	bw.WriteTriple(&graph.TripleEvent{Subject: graph.NewIRITerm("s"), Predicate: "p", Object: graph.NewIRITerm("o")})

	// Manual flush
	if err := bw.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Check memory graph received the events (nodes is unexported, so just verify no error)
	mg.mu.RLock()
	nodeCount := len(mg.nodes)
	mg.mu.RUnlock()
	if nodeCount != 1 {
		t.Errorf("expected 1 node, got %d", nodeCount)
	}
}

func TestStatsWriter_Coverage(t *testing.T) {
	mg := NewMemoryGraph()
	sw := NewStatsWriter(mg)

	sw.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "n1", Labels: []string{"Person"}})
	sw.WriteNode(&graph.NodeEvent{Op: graph.OpUpsert, ID: "n2", Labels: []string{"Person"}})
	sw.WriteEdge(&graph.EdgeEvent{Op: graph.OpUpsert, FromID: "n1", ToID: "n2", Label: "knows"})
	sw.WriteTriple(&graph.TripleEvent{Subject: graph.NewIRITerm("n1"), Predicate: "has", Object: graph.NewLiteralTerm("property")})

	sw.Flush()
	sw.Close()

	stats := sw.Stats()
	if stats.NodesWritten != 2 {
		t.Errorf("stats.NodesWritten: got %d, want 2", stats.NodesWritten)
	}
	if stats.EdgesWritten != 1 {
		t.Errorf("stats.EdgesWritten: got %d, want 1", stats.EdgesWritten)
	}
	if stats.TriplesWritten != 1 {
		t.Errorf("stats.TriplesWritten: got %d, want 1", stats.TriplesWritten)
	}
}
