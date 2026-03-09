package algo

import (
	"testing"
)

func TestCriticalPath_Coverage(t *testing.T) {
	// DAG: 0->1 (w=3), 0->2 (w=2), 1->3 (w=4), 2->3 (w=5)
	csr := NewCSR(4, []int64{0, 2, 3, 4, 4}, []int64{1, 2, 3, 3})
	weights := []float32{3, 2, 4, 5}

	result, err := CriticalPath(csr, weights)
	if err != nil {
		t.Fatalf("CriticalPath error: %v", err)
	}
	if len(result.Path) == 0 {
		t.Error("expected non-empty critical path")
	}
}

func TestCriticalNodes_Coverage(t *testing.T) {
	csr := NewCSR(4, []int64{0, 2, 3, 4, 4}, []int64{1, 2, 3, 3})
	weights := []float32{3, 2, 4, 5}

	cpResult, err := CriticalPath(csr, weights)
	if err != nil {
		t.Fatalf("CriticalPath error: %v", err)
	}

	nodes := CriticalNodes(cpResult, 0.0)
	if len(nodes) == 0 {
		t.Error("expected critical nodes")
	}
}

func TestLongestPath_Coverage(t *testing.T) {
	csr := NewCSR(4, []int64{0, 2, 3, 4, 4}, []int64{1, 2, 3, 3})

	path, length, err := LongestPath(csr)
	if err != nil {
		t.Fatalf("LongestPath error: %v", err)
	}
	if length <= 0 {
		t.Error("expected positive path length")
	}
	if len(path) == 0 {
		t.Error("expected non-empty path")
	}
}

func TestShortestPathDAG_Coverage(t *testing.T) {
	csr := NewCSR(4, []int64{0, 2, 3, 4, 4}, []int64{1, 2, 3, 3})
	weights := []float32{3, 2, 4, 5}

	dist, pred, err := ShortestPathDAG(csr, weights, 0)
	if err != nil {
		t.Fatalf("ShortestPathDAG error: %v", err)
	}
	if len(dist) != 4 {
		t.Errorf("expected 4 distances, got %d", len(dist))
	}
	if len(pred) != 4 {
		t.Errorf("expected 4 predecessors, got %d", len(pred))
	}
}

func TestMultiBFS_Coverage(t *testing.T) {
	// Graph: 0->1, 0->2, 1->3, 2->3, 3->4
	csr := NewCSR(5, []int64{0, 2, 3, 4, 5, 5}, []int64{1, 2, 3, 3, 4})

	distances, closestSource := MultiBFS(csr, []int64{0, 4})
	if len(distances) != 5 {
		t.Errorf("expected 5 distances, got %d", len(distances))
	}
	if len(closestSource) != 5 {
		t.Errorf("expected 5 closest sources, got %d", len(closestSource))
	}
	// Node 0 should be closest to source 0
	if closestSource[0] != 0 {
		t.Errorf("node 0 closest source: got %d, want 0", closestSource[0])
	}
}

func TestDFSFull_Coverage(t *testing.T) {
	csr := NewCSR(4, []int64{0, 2, 3, 3, 3}, []int64{1, 2, 2})
	result := DFSFull(csr)
	if result == nil {
		t.Fatal("nil DFS result")
	}
	if len(result.DiscoveryTime) != 4 {
		t.Errorf("expected 4 discovery times, got %d", len(result.DiscoveryTime))
	}
}

func TestReachable_Coverage(t *testing.T) {
	csr := NewCSR(4, []int64{0, 2, 3, 3, 3}, []int64{1, 2, 2})
	reachable := Reachable(csr, 0)
	if len(reachable) < 2 {
		t.Errorf("expected at least 2 reachable nodes from 0, got %d", len(reachable))
	}
}

func TestReachableFrom_Coverage(t *testing.T) {
	csr := NewCSR(4, []int64{0, 2, 3, 3, 3}, []int64{1, 2, 2})
	reachable := ReachableFrom(csr, []int64{0, 3})
	if len(reachable) < 2 {
		t.Errorf("expected at least 2 reachable nodes, got %d", len(reachable))
	}
}

func TestPersonalizedPageRank_Coverage(t *testing.T) {
	csr := NewCSR(4, []int64{0, 2, 3, 4, 4}, []int64{1, 2, 3, 3})
	cfg := PageRankConfig{
		Damping:    0.85,
		Iterations: 20,
		Tolerance:  1e-6,
	}
	result := PersonalizedPageRank(csr, cfg, []int64{0})
	if result == nil {
		t.Fatal("nil result")
	}
	if len(result.Scores) != 4 {
		t.Errorf("expected 4 scores, got %d", len(result.Scores))
	}
}

func TestLouvainUnweighted_Coverage(t *testing.T) {
	// Undirected graph with symmetric edges
	csr := NewCSR(6, []int64{0, 2, 3, 6, 9, 11, 12},
		[]int64{1, 2, 0, 0, 1, 3, 2, 4, 5, 3, 5, 3})

	result := LouvainUnweighted(csr, DefaultLouvainConfig)
	if result == nil {
		t.Fatal("nil Louvain result")
	}
	if len(result.Communities) != 6 {
		t.Errorf("expected 6 community assignments, got %d", len(result.Communities))
	}
	if result.NumComms < 1 {
		t.Error("expected at least 1 community")
	}
}
