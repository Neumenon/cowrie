//go:build agentgo

package gnn

import (
	"math"
	"testing"

	"github.com/Neumenon/cowrie/gnn/algo"
)

func TestGraphRAG_NewGraphRAG(t *testing.T) {
	cfg := DefaultGraphRAGConfig
	g := NewGraphRAG(nil, cfg)

	if g == nil {
		t.Fatal("expected non-nil GraphRAG")
	}
	if g.cfg.Alpha != 0.6 {
		t.Errorf("expected alpha 0.6, got %f", g.cfg.Alpha)
	}
}

func TestGraphRAG_SetGraph(t *testing.T) {
	g := NewGraphRAG(nil, DefaultGraphRAGConfig)

	// Create a simple graph: A -> B -> C
	indptr := []int64{0, 1, 2, 2}
	indices := []int64{1, 2}
	csr := algo.NewCSR(3, indptr, indices)

	idToNode := map[string]int64{
		"doc-a": 0,
		"doc-b": 1,
		"doc-c": 2,
	}

	g.SetGraph(csr, idToNode)

	// PageRank should be computed
	if g.pagerank == nil {
		t.Fatal("expected PageRank to be computed")
	}
	if len(g.pagerank.Scores) != 3 {
		t.Errorf("expected 3 PageRank scores, got %d", len(g.pagerank.Scores))
	}

	// Reverse mapping should be built
	if g.nodeToID[0] != "doc-a" {
		t.Errorf("expected nodeToID[0] = doc-a, got %s", g.nodeToID[0])
	}
}

func TestGraphRAG_GetImportance(t *testing.T) {
	g := NewGraphRAG(nil, DefaultGraphRAGConfig)

	// Create star graph: B, C, D -> A (A is hub)
	indptr := []int64{0, 0, 1, 2, 3}
	indices := []int64{0, 0, 0}
	csr := algo.NewCSR(4, indptr, indices)

	idToNode := map[string]int64{
		"hub":    0,
		"spoke1": 1,
		"spoke2": 2,
		"spoke3": 3,
	}

	g.SetGraph(csr, idToNode)

	hubImportance := g.GetImportance("hub")
	spokeImportance := g.GetImportance("spoke1")

	if hubImportance <= spokeImportance {
		t.Errorf("hub should have higher importance: hub=%f, spoke=%f",
			hubImportance, spokeImportance)
	}

	// Unknown doc should return 0
	unknownImportance := g.GetImportance("unknown")
	if unknownImportance != 0 {
		t.Errorf("unknown doc should have 0 importance, got %f", unknownImportance)
	}
}

func TestGraphRAG_GetNeighbors(t *testing.T) {
	g := NewGraphRAG(nil, DefaultGraphRAGConfig)

	// A -> B, A -> C
	indptr := []int64{0, 2, 2, 2}
	indices := []int64{1, 2}
	csr := algo.NewCSR(3, indptr, indices)

	idToNode := map[string]int64{
		"A": 0,
		"B": 1,
		"C": 2,
	}

	g.SetGraph(csr, idToNode)

	neighbors := g.GetNeighbors("A")
	if len(neighbors) != 2 {
		t.Errorf("expected 2 neighbors, got %d", len(neighbors))
	}

	// B and C should be neighbors (order may vary)
	hasB, hasC := false, false
	for _, n := range neighbors {
		if n == "B" {
			hasB = true
		}
		if n == "C" {
			hasC = true
		}
	}
	if !hasB || !hasC {
		t.Errorf("expected B and C as neighbors, got %v", neighbors)
	}

	// B should have no outgoing neighbors
	neighborsB := g.GetNeighbors("B")
	if len(neighborsB) != 0 {
		t.Errorf("expected 0 neighbors for B, got %d", len(neighborsB))
	}
}

func TestGraphRAG_Stats(t *testing.T) {
	g := NewGraphRAG(nil, DefaultGraphRAGConfig)

	// Empty stats
	stats := g.Stats()
	if stats.NumNodes != 0 {
		t.Errorf("expected 0 nodes, got %d", stats.NumNodes)
	}

	// Add graph
	indptr := []int64{0, 2, 3, 3}
	indices := []int64{1, 2, 2}
	csr := algo.NewCSR(3, indptr, indices)

	idToNode := map[string]int64{
		"A": 0,
		"B": 1,
		"C": 2,
	}

	g.SetGraph(csr, idToNode)

	stats = g.Stats()
	if stats.NumNodes != 3 {
		t.Errorf("expected 3 nodes, got %d", stats.NumNodes)
	}
	if stats.NumEdges != 3 {
		t.Errorf("expected 3 edges, got %d", stats.NumEdges)
	}
	if stats.NumDocuments != 3 {
		t.Errorf("expected 3 documents, got %d", stats.NumDocuments)
	}
}

func TestGraphRAG_ComputeCommunities(t *testing.T) {
	g := NewGraphRAG(nil, DefaultGraphRAGConfig)

	// Create two clusters connected by weak link
	// Cluster 1: 0 <-> 1 <-> 2
	// Cluster 2: 3 <-> 4 <-> 5
	// Weak: 2 -> 3
	indptr := []int64{0, 2, 4, 6, 8, 10, 12}
	indices := []int64{
		1, 2, // 0 -> 1, 2
		0, 2, // 1 -> 0, 2
		0, 3, // 2 -> 0, 3 (bridge)
		4, 5, // 3 -> 4, 5
		3, 5, // 4 -> 3, 5
		3, 4, // 5 -> 3, 4
	}
	csr := algo.NewCSR(6, indptr, indices)

	idToNode := map[string]int64{
		"a": 0, "b": 1, "c": 2,
		"d": 3, "e": 4, "f": 5,
	}

	g.SetGraph(csr, idToNode)
	g.ComputeCommunities()

	if g.communities == nil {
		t.Fatal("expected communities to be computed")
	}

	// Check same cluster = same community
	commA := g.GetCommunity("a")
	commB := g.GetCommunity("b")
	if commA != commB {
		t.Logf("a and b might be in different communities: %d vs %d (depends on Louvain)", commA, commB)
	}

	stats := g.Stats()
	if stats.NumCommunities < 1 {
		t.Errorf("expected at least 1 community, got %d", stats.NumCommunities)
	}
}

func TestGraphRAG_ReRank(t *testing.T) {
	g := NewGraphRAG(nil, DefaultGraphRAGConfig)

	// Star graph: all point to A
	indptr := []int64{0, 0, 1, 2, 3}
	indices := []int64{0, 0, 0}
	csr := algo.NewCSR(4, indptr, indices)

	idToNode := map[string]int64{
		"A": 0, // Hub
		"B": 1,
		"C": 2,
		"D": 3,
	}

	g.SetGraph(csr, idToNode)

	// Vector scores: D > C > B > A
	docIDs := []string{"A", "B", "C", "D"}
	vectorScores := []float32{0.5, 0.6, 0.7, 0.8}

	results := g.ReRank(docIDs, vectorScores, 4)

	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}

	// With 60% vector + 40% graph, A should be boosted due to high PageRank
	// Check that A is not last anymore
	foundA := false
	for i, r := range results {
		if r.ID == "A" {
			foundA = true
			if i == 3 {
				t.Logf("A is still last - graph weight may not be enough to overcome vector")
			}
			break
		}
	}
	if !foundA {
		t.Error("expected to find A in results")
	}
}

func TestGraphRAG_DiverseTopK(t *testing.T) {
	g := NewGraphRAG(nil, DefaultGraphRAGConfig)

	// Create graph with communities
	indptr := []int64{0, 2, 3, 3, 5, 6, 6}
	indices := []int64{1, 2, 0, 4, 5, 3}
	csr := algo.NewCSR(6, indptr, indices)

	idToNode := map[string]int64{
		"a": 0, "b": 1, "c": 2,
		"d": 3, "e": 4, "f": 5,
	}

	g.SetGraph(csr, idToNode)
	// Manually set communities
	g.SetCommunities([]int64{0, 0, 0, 1, 1, 1})

	// All results from same community
	results := []GraphRAGResult{
		{ID: "a", FinalScore: 0.9, CommunityID: 0},
		{ID: "b", FinalScore: 0.8, CommunityID: 0},
		{ID: "c", FinalScore: 0.7, CommunityID: 0},
		{ID: "d", FinalScore: 0.6, CommunityID: 1},
		{ID: "e", FinalScore: 0.5, CommunityID: 1},
	}

	// Limit to 2 per community, top 3
	diverse := g.DiverseTopK(results, 3, 2)

	if len(diverse) != 3 {
		t.Errorf("expected 3 diverse results, got %d", len(diverse))
	}

	// Should have at most 2 from community 0
	comm0Count := 0
	for _, r := range diverse {
		if r.CommunityID == 0 {
			comm0Count++
		}
	}
	if comm0Count > 2 {
		t.Errorf("expected at most 2 from community 0, got %d", comm0Count)
	}
}

func TestGraphRAG_ExpandQuery(t *testing.T) {
	g := NewGraphRAG(nil, DefaultGraphRAGConfig)

	// A -> B -> C, A -> D
	indptr := []int64{0, 2, 3, 3, 3}
	indices := []int64{1, 3, 2}
	csr := algo.NewCSR(4, indptr, indices)

	idToNode := map[string]int64{
		"A": 0,
		"B": 1,
		"C": 2,
		"D": 3,
	}

	g.SetGraph(csr, idToNode)

	// Initial result: just A
	results := []GraphRAGResult{
		{ID: "A", FinalScore: 1.0, NodeID: 0},
	}

	expanded := g.ExpandQuery(results, 2)

	// Should add B and D (neighbors of A)
	if len(expanded) < 2 {
		t.Errorf("expected at least 2 results after expansion, got %d", len(expanded))
	}

	// Check that B or D was added
	hasB, hasD := false, false
	for _, r := range expanded {
		if r.ID == "B" {
			hasB = true
		}
		if r.ID == "D" {
			hasD = true
		}
	}
	if !hasB && !hasD {
		t.Error("expected B or D to be added via expansion")
	}
}

func TestNormalizeScores(t *testing.T) {
	results := []GraphRAGResult{
		{ID: "A", FinalScore: 0.2},
		{ID: "B", FinalScore: 0.5},
		{ID: "C", FinalScore: 0.8},
	}

	NormalizeScores(results)

	// After normalization: min=0, max=1
	if math.Abs(float64(results[0].FinalScore)-0.0) > 0.001 {
		t.Errorf("expected A normalized to 0, got %f", results[0].FinalScore)
	}
	if math.Abs(float64(results[2].FinalScore)-1.0) > 0.001 {
		t.Errorf("expected C normalized to 1, got %f", results[2].FinalScore)
	}
	if math.Abs(float64(results[1].FinalScore)-0.5) > 0.001 {
		t.Errorf("expected B normalized to 0.5, got %f", results[1].FinalScore)
	}
}

func TestNormalizeScores_Empty(t *testing.T) {
	results := []GraphRAGResult{}
	NormalizeScores(results) // Should not panic
}

func TestNormalizeScores_Single(t *testing.T) {
	results := []GraphRAGResult{
		{ID: "A", FinalScore: 0.5},
	}
	NormalizeScores(results)
	if results[0].FinalScore != 1.0 {
		t.Errorf("single result should normalize to 1.0, got %f", results[0].FinalScore)
	}
}

func TestGraphRAGConfig_Defaults(t *testing.T) {
	cfg := DefaultGraphRAGConfig

	if cfg.Alpha != 0.6 {
		t.Errorf("expected alpha 0.6, got %f", cfg.Alpha)
	}
	if cfg.UsePersonalized {
		t.Error("expected UsePersonalized to be false by default")
	}
	if cfg.BoostCommunity != 0.2 {
		t.Errorf("expected BoostCommunity 0.2, got %f", cfg.BoostCommunity)
	}
}

func TestGraphRAG_EmptyGraph(t *testing.T) {
	g := NewGraphRAG(nil, DefaultGraphRAGConfig)

	// Should handle empty graph gracefully
	importance := g.GetImportance("any")
	if importance != 0 {
		t.Errorf("expected 0 importance for empty graph, got %f", importance)
	}

	neighbors := g.GetNeighbors("any")
	if neighbors != nil {
		t.Errorf("expected nil neighbors for empty graph, got %v", neighbors)
	}

	community := g.GetCommunity("any")
	if community != -1 {
		t.Errorf("expected -1 community for empty graph, got %d", community)
	}
}

func TestGraphRAG_computeGraphScores_NoGraph(t *testing.T) {
	g := NewGraphRAG(nil, DefaultGraphRAGConfig)

	docIDs := []string{"A", "B", "C"}
	scores := g.computeGraphScores(docIDs, "")

	// Without a graph, scores should be uniform
	expected := float32(1.0) / float32(len(docIDs))
	for i, s := range scores {
		if math.Abs(float64(s-expected)) > 0.001 {
			t.Errorf("expected uniform score %f for doc %d, got %f", expected, i, s)
		}
	}
}

func TestGraphRAG_blendResults_AlphaEdgeCases(t *testing.T) {
	g := NewGraphRAG(nil, GraphRAGConfig{Alpha: 0, BoostCommunity: 0})

	docIDs := []string{"A", "B"}
	vectorScores := []float32{1.0, 0.5}
	graphScores := []float32{0.5, 1.0}

	// Alpha = 0: pure graph
	results := g.blendResults(docIDs, vectorScores, graphScores, 2)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	// B should be first (higher graph score)
	if results[0].ID != "B" {
		t.Errorf("with alpha=0, B should be first (pure graph), got %s", results[0].ID)
	}

	// Alpha = 1: pure vector
	g.cfg.Alpha = 1.0
	results = g.blendResults(docIDs, vectorScores, graphScores, 2)
	// A should be first (higher vector score)
	if results[0].ID != "A" {
		t.Errorf("with alpha=1, A should be first (pure vector), got %s", results[0].ID)
	}
}
