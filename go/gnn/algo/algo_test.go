package algo

import (
	"testing"
)

// buildTestGraph creates a CSR from edge list for testing.
func buildTestGraph(numNodes int64, edges [][2]int64) *CSR {
	// Count outgoing edges per node
	outCount := make([]int64, numNodes)
	for _, e := range edges {
		if e[0] >= 0 && e[0] < numNodes {
			outCount[e[0]]++
		}
	}

	// Build indptr
	indptr := make([]int64, numNodes+1)
	for i := int64(0); i < numNodes; i++ {
		indptr[i+1] = indptr[i] + outCount[i]
	}

	// Build indices
	indices := make([]int64, indptr[numNodes])
	pos := make([]int64, numNodes)
	for _, e := range edges {
		src, dst := e[0], e[1]
		if src >= 0 && src < numNodes {
			idx := indptr[src] + pos[src]
			indices[idx] = dst
			pos[src]++
		}
	}

	return NewCSR(numNodes, indptr, indices)
}

// --- PageRank Tests ---

func TestPageRank(t *testing.T) {
	// Simple triangle graph: 0 -> 1 -> 2 -> 0
	edges := [][2]int64{{0, 1}, {1, 2}, {2, 0}}
	csr := buildTestGraph(3, edges)

	result := PageRank(csr, DefaultPageRankConfig)

	if result == nil || len(result.Scores) != 3 {
		t.Fatal("PageRank should return 3 scores")
	}

	// All nodes should have roughly equal PageRank in a symmetric cycle
	avg := (result.Scores[0] + result.Scores[1] + result.Scores[2]) / 3.0
	for i := 0; i < 3; i++ {
		if result.Scores[i] < avg*0.9 || result.Scores[i] > avg*1.1 {
			t.Errorf("Node %d has unbalanced PageRank: %.4f (avg: %.4f)", i, result.Scores[i], avg)
		}
	}
}

func TestPageRankTopK(t *testing.T) {
	// Star graph: 0 <- 1, 0 <- 2, 0 <- 3 (node 0 receives all links)
	edges := [][2]int64{{1, 0}, {2, 0}, {3, 0}}
	csr := buildTestGraph(4, edges)

	result := PageRank(csr, DefaultPageRankConfig)
	topK := PageRankTopK(result, 2)

	if len(topK) != 2 {
		t.Fatalf("Expected 2 top nodes, got %d", len(topK))
	}
	if topK[0] != 0 {
		t.Errorf("Node 0 should be top ranked, got %d", topK[0])
	}
}

// --- BFS Tests ---

func TestBFS(t *testing.T) {
	// Linear graph: 0 -> 1 -> 2 -> 3
	edges := [][2]int64{{0, 1}, {1, 2}, {2, 3}}
	csr := buildTestGraph(4, edges)

	result := BFS(csr, 0)

	if result.Distances[0] != 0 {
		t.Errorf("Distance to source should be 0, got %d", result.Distances[0])
	}
	if result.Distances[3] != 3 {
		t.Errorf("Distance to node 3 should be 3, got %d", result.Distances[3])
	}
}

func TestBFSPath(t *testing.T) {
	edges := [][2]int64{{0, 1}, {1, 2}, {2, 3}}
	csr := buildTestGraph(4, edges)

	path := BFSPath(csr, 0, 3)

	expected := []int64{0, 1, 2, 3}
	if len(path) != len(expected) {
		t.Fatalf("Path length mismatch: got %v", path)
	}
	for i, v := range expected {
		if path[i] != v {
			t.Errorf("Path[%d] = %d, want %d", i, path[i], v)
		}
	}
}

// --- DFS Tests ---

func TestDFS(t *testing.T) {
	edges := [][2]int64{{0, 1}, {1, 2}, {0, 3}}
	csr := buildTestGraph(4, edges)

	result := DFS(csr, 0)

	if len(result.PreOrder) != 4 {
		t.Errorf("DFS should visit 4 nodes, visited %d", len(result.PreOrder))
	}
	if result.PreOrder[0] != 0 {
		t.Error("DFS should start from source")
	}
}

func TestConnectedComponents(t *testing.T) {
	// Two disconnected components: 0-1 and 2-3
	edges := [][2]int64{{0, 1}, {1, 0}, {2, 3}, {3, 2}}
	csr := buildTestGraph(4, edges)

	compID, numComps := ConnectedComponents(csr)

	if numComps != 2 {
		t.Errorf("Expected 2 components, got %d", numComps)
	}
	if compID[0] != compID[1] {
		t.Error("Nodes 0 and 1 should be in same component")
	}
	if compID[2] != compID[3] {
		t.Error("Nodes 2 and 3 should be in same component")
	}
	if compID[0] == compID[2] {
		t.Error("Components should be different")
	}
}

// --- Louvain Tests ---

func TestLouvainSingleNode(t *testing.T) {
	csr := buildTestGraph(1, nil)
	result := Louvain(csr, nil, DefaultLouvainConfig)

	if result.NumComms != 1 {
		t.Errorf("Single node should have 1 community, got %d", result.NumComms)
	}
}

func TestLouvainDisconnected(t *testing.T) {
	// 4 isolated nodes - each should be its own community
	csr := buildTestGraph(4, nil)
	result := Louvain(csr, nil, DefaultLouvainConfig)

	if result.NumComms != 4 {
		t.Errorf("4 isolated nodes should have 4 communities, got %d", result.NumComms)
	}
}

func TestLouvainClique(t *testing.T) {
	// Complete graph K4 - all nodes should be in one community
	edges := [][2]int64{
		{0, 1}, {0, 2}, {0, 3},
		{1, 0}, {1, 2}, {1, 3},
		{2, 0}, {2, 1}, {2, 3},
		{3, 0}, {3, 1}, {3, 2},
	}
	csr := buildTestGraph(4, edges)
	result := Louvain(csr, nil, DefaultLouvainConfig)

	if result.NumComms != 1 {
		t.Errorf("Complete graph should have 1 community, got %d", result.NumComms)
	}
	if result.Modularity < 0 {
		t.Errorf("Modularity should be >= 0, got %.4f", result.Modularity)
	}
}

func TestLouvainTwoCommunities(t *testing.T) {
	// Two dense clusters connected by single edge
	// Cluster 1: nodes 0,1,2 fully connected
	// Cluster 2: nodes 3,4,5 fully connected
	// Bridge: 2-3
	edges := [][2]int64{
		// Cluster 1
		{0, 1}, {1, 0}, {0, 2}, {2, 0}, {1, 2}, {2, 1},
		// Cluster 2
		{3, 4}, {4, 3}, {3, 5}, {5, 3}, {4, 5}, {5, 4},
		// Bridge
		{2, 3}, {3, 2},
	}
	csr := buildTestGraph(6, edges)
	result := Louvain(csr, nil, DefaultLouvainConfig)

	// Should find 2 communities
	if result.NumComms != 2 {
		t.Errorf("Expected 2 communities, got %d", result.NumComms)
	}

	// Verify cluster integrity
	if result.Communities[0] != result.Communities[1] || result.Communities[0] != result.Communities[2] {
		t.Error("Nodes 0,1,2 should be in same community")
	}
	if result.Communities[3] != result.Communities[4] || result.Communities[3] != result.Communities[5] {
		t.Error("Nodes 3,4,5 should be in same community")
	}
}

func TestLouvainWithWeights(t *testing.T) {
	// Triangle with different weights
	edges := [][2]int64{{0, 1}, {1, 2}, {2, 0}}
	weights := []float64{1.0, 1.0, 10.0} // Strong edge between 0-2

	csr := buildTestGraph(3, edges)
	result := Louvain(csr, weights, DefaultLouvainConfig)

	// Should find communities
	if result.NumComms < 1 {
		t.Error("Should find at least 1 community")
	}
}

func TestGetCommunityNodes(t *testing.T) {
	result := &LouvainResult{
		Communities: []int64{0, 0, 1, 1, 0},
		NumComms:    2,
	}

	comm0 := GetCommunityNodes(result, 0)
	comm1 := GetCommunityNodes(result, 1)

	if len(comm0) != 3 {
		t.Errorf("Community 0 should have 3 nodes, got %d", len(comm0))
	}
	if len(comm1) != 2 {
		t.Errorf("Community 1 should have 2 nodes, got %d", len(comm1))
	}
}

func TestGetCommunityStats(t *testing.T) {
	// Two triangles
	edges := [][2]int64{
		{0, 1}, {1, 0}, {0, 2}, {2, 0}, {1, 2}, {2, 1},
		{3, 4}, {4, 3}, {3, 5}, {5, 3}, {4, 5}, {5, 4},
	}
	csr := buildTestGraph(6, edges)

	// Manually set communities
	result := &LouvainResult{
		Communities: []int64{0, 0, 0, 1, 1, 1},
		NumComms:    2,
	}

	stats := GetCommunityStats(csr, nil, result)

	if len(stats) != 2 {
		t.Fatalf("Expected 2 community stats, got %d", len(stats))
	}

	for i, s := range stats {
		if s.Size != 3 {
			t.Errorf("Community %d size should be 3, got %d", i, s.Size)
		}
		// Each triangle has 3 internal edges (6 directed edges / 2)
		if s.Internal != 3.0 {
			t.Errorf("Community %d internal should be 3.0, got %.2f", i, s.Internal)
		}
	}
}

func TestCommunityDensity(t *testing.T) {
	stats := []CommunityStats{
		{ID: 0, Size: 3, Internal: 3.0}, // Complete triangle: 3/(3*2/2) = 1.0
		{ID: 1, Size: 4, Internal: 3.0}, // Sparse: 3/(4*3/2) = 0.5
	}

	densities := CommunityDensity(stats)

	if densities[0] != 1.0 {
		t.Errorf("Complete triangle density should be 1.0, got %.2f", densities[0])
	}
	if densities[1] != 0.5 {
		t.Errorf("Sparse community density should be 0.5, got %.2f", densities[1])
	}
}

func TestInterCommunityEdges(t *testing.T) {
	edges := [][2]int64{
		{0, 1}, {1, 0}, // Internal to comm 0
		{2, 3}, {3, 2}, // Internal to comm 1
		{1, 2}, {2, 1}, // Between communities
	}
	csr := buildTestGraph(4, edges)
	communities := []int64{0, 0, 1, 1}

	weight := InterCommunityEdges(csr, nil, communities, 0, 1)

	// Edge 1->2 connects community 0 to community 1
	if weight != 1.0 {
		t.Errorf("Inter-community weight should be 1.0, got %.2f", weight)
	}
}

// --- Anomaly Detection Tests ---

func TestComputeGraphMetrics(t *testing.T) {
	// Simple complete graph K4
	edges := [][2]int64{
		{0, 1}, {0, 2}, {0, 3},
		{1, 0}, {1, 2}, {1, 3},
		{2, 0}, {2, 1}, {2, 3},
		{3, 0}, {3, 1}, {3, 2},
	}
	csr := buildTestGraph(4, edges)

	metrics := ComputeGraphMetrics(csr)

	if metrics.NumNodes != 4 {
		t.Errorf("Expected 4 nodes, got %d", metrics.NumNodes)
	}
	if metrics.NumEdges != 12 {
		t.Errorf("Expected 12 edges, got %d", metrics.NumEdges)
	}
	// K4: density = 12 / (4*3) = 1.0
	if metrics.Density < 0.99 || metrics.Density > 1.01 {
		t.Errorf("K4 density should be 1.0, got %.2f", metrics.Density)
	}
	// Each node has degree 3
	if metrics.AvgDegree != 3.0 {
		t.Errorf("K4 avg degree should be 3.0, got %.2f", metrics.AvgDegree)
	}
}

func TestComputeGraphMetricsWithIsolates(t *testing.T) {
	// Graph with one isolated node
	edges := [][2]int64{{0, 1}, {1, 0}}
	csr := buildTestGraph(3, edges) // Node 2 is isolated

	metrics := ComputeGraphMetrics(csr)

	if metrics.NumIsolated != 1 {
		t.Errorf("Expected 1 isolated node, got %d", metrics.NumIsolated)
	}
}

func TestDetectAnomalies(t *testing.T) {
	// Star graph: node 0 is hub, nodes 1-5 are spokes
	edges := [][2]int64{
		{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5},
		{1, 0}, {2, 0}, {3, 0}, {4, 0}, {5, 0},
	}
	csr := buildTestGraph(6, edges)

	result := DetectAnomalies(csr, 1.5) // Lower threshold to catch hub

	// Node 0 should be detected as anomaly (high degree hub)
	foundHub := false
	for _, a := range result.Anomalies {
		if a.NodeID == 0 && (a.AnomalyType == "hub" || a.AnomalyType == "high_out_degree") {
			foundHub = true
			break
		}
	}
	if !foundHub {
		t.Error("Node 0 should be detected as hub/high_degree anomaly")
	}
}

func TestDetectDegreeAnomalies(t *testing.T) {
	// Star graph
	edges := [][2]int64{
		{0, 1}, {0, 2}, {0, 3}, {0, 4},
		{1, 0}, {2, 0}, {3, 0}, {4, 0},
	}
	csr := buildTestGraph(5, edges)

	anomalies := DetectDegreeAnomalies(csr, 1.5)

	// Hub should be detected
	if len(anomalies) == 0 {
		t.Error("Should detect at least one anomaly (hub)")
	}
}

func TestDegreeHistogram(t *testing.T) {
	edges := [][2]int64{{0, 1}, {0, 2}, {1, 2}}
	csr := buildTestGraph(3, edges)

	bins, counts := DegreeHistogram(csr, 3)

	if bins == nil || counts == nil {
		t.Fatal("Histogram should not be nil")
	}
	// Total counts should equal number of nodes
	total := 0
	for _, c := range counts {
		total += c
	}
	if total != 3 {
		t.Errorf("Total histogram count should be 3, got %d", total)
	}
}

func TestPowerLawExponent(t *testing.T) {
	// Scale-free-like graph
	edges := [][2]int64{
		{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5}, {0, 6}, {0, 7}, // Hub
		{1, 2}, {3, 4}, // Some connections
	}
	csr := buildTestGraph(8, edges)

	alpha := PowerLawExponent(csr)

	// Should return some positive exponent
	if alpha < 0 {
		t.Errorf("Power law exponent should be positive, got %.2f", alpha)
	}
}

func TestLocalClusteringCoefficient(t *testing.T) {
	// Complete triangle
	edges := [][2]int64{
		{0, 1}, {0, 2},
		{1, 0}, {1, 2},
		{2, 0}, {2, 1},
	}
	csr := buildTestGraph(3, edges)

	// Each node in K3 has clustering = 1.0
	for i := int64(0); i < 3; i++ {
		cc := LocalClusteringCoefficient(csr, i)
		if cc < 0.99 {
			t.Errorf("Node %d in K3 should have clustering ~1.0, got %.2f", i, cc)
		}
	}
}

func TestAllLocalClustering(t *testing.T) {
	edges := [][2]int64{{0, 1}, {1, 0}, {1, 2}, {2, 1}}
	csr := buildTestGraph(3, edges)

	coeffs := AllLocalClustering(csr)

	if len(coeffs) != 3 {
		t.Errorf("Should return 3 coefficients, got %d", len(coeffs))
	}
}

func TestFindHubs(t *testing.T) {
	// Node 0 is hub with degree 5, others have degree 1
	edges := [][2]int64{
		{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5},
	}
	csr := buildTestGraph(6, edges)

	hubs := FindHubs(csr, 1.5)

	// Node 0 should be identified as hub
	found := false
	for _, h := range hubs {
		if h == 0 {
			found = true
			break
		}
	}
	if !found {
		t.Error("Node 0 should be identified as hub")
	}
}

func TestFindPeripheral(t *testing.T) {
	// Node 0 is hub, nodes 1-5 are peripheral (degree 1)
	edges := [][2]int64{
		{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5},
		{1, 0}, {2, 0}, {3, 0}, {4, 0}, {5, 0},
	}
	csr := buildTestGraph(6, edges)

	peripheral := FindPeripheral(csr, 1)

	// Should find 5 peripheral nodes (all except hub)
	if len(peripheral) != 5 {
		t.Errorf("Expected 5 peripheral nodes, got %d", len(peripheral))
	}
}

// --- Tarjan SCC Tests ---

func TestTarjanSCCLinear(t *testing.T) {
	// DAG: 0 -> 1 -> 2 -> 3
	edges := [][2]int64{{0, 1}, {1, 2}, {2, 3}}
	csr := buildTestGraph(4, edges)

	result := TarjanSCC(csr)
	if result.NumComponents != 4 {
		t.Errorf("DAG should have 4 SCCs, got %d", result.NumComponents)
	}
	if result.HasCycles {
		t.Error("DAG should not have cycles")
	}
}

func TestTarjanSCCCycle(t *testing.T) {
	// Cycle: 0 -> 1 -> 2 -> 0
	edges := [][2]int64{{0, 1}, {1, 2}, {2, 0}}
	csr := buildTestGraph(3, edges)

	result := TarjanSCC(csr)
	if result.NumComponents != 1 {
		t.Errorf("Cycle should have 1 SCC, got %d", result.NumComponents)
	}
	if !result.HasCycles {
		t.Error("Should detect cycle")
	}
}

func TestTarjanTwoSCCs(t *testing.T) {
	// Two cycles: 0<->1, 2<->3
	edges := [][2]int64{{0, 1}, {1, 0}, {2, 3}, {3, 2}}
	csr := buildTestGraph(4, edges)

	result := TarjanSCC(csr)
	if result.NumComponents != 2 {
		t.Errorf("Should have 2 SCCs, got %d", result.NumComponents)
	}
}

func TestHasCycle(t *testing.T) {
	dag := buildTestGraph(3, [][2]int64{{0, 1}, {1, 2}})
	if HasCycle(dag) {
		t.Error("DAG should not have cycle")
	}

	cyclic := buildTestGraph(3, [][2]int64{{0, 1}, {1, 2}, {2, 0}})
	if !HasCycle(cyclic) {
		t.Error("Should detect cycle")
	}
}

func TestIsDAG(t *testing.T) {
	dag := buildTestGraph(3, [][2]int64{{0, 1}, {1, 2}})
	if !IsDAG(dag) {
		t.Error("Should be DAG")
	}

	cyclic := buildTestGraph(3, [][2]int64{{0, 1}, {1, 2}, {2, 0}})
	if IsDAG(cyclic) {
		t.Error("Cyclic graph is not DAG")
	}
}

func TestFindCycles(t *testing.T) {
	edges := [][2]int64{{0, 1}, {1, 2}, {2, 0}, {3, 4}}
	csr := buildTestGraph(5, edges)

	cycles := FindCycles(csr)
	if len(cycles) != 1 {
		t.Errorf("Should find 1 cycle, got %d", len(cycles))
	}
}

func TestGetSCCGraph(t *testing.T) {
	// Two SCCs connected: {0,1} -> {2,3}
	edges := [][2]int64{{0, 1}, {1, 0}, {1, 2}, {2, 3}, {3, 2}}
	csr := buildTestGraph(4, edges)

	sccCSR, _ := GetSCCGraph(csr)
	if sccCSR == nil {
		t.Fatal("SCC graph should not be nil")
	}
	if sccCSR.NumNodes != 2 {
		t.Errorf("Should have 2 SCC nodes, got %d", sccCSR.NumNodes)
	}
	if HasCycle(sccCSR) {
		t.Error("Condensation should be DAG")
	}
}

func TestCycleNodes(t *testing.T) {
	edges := [][2]int64{{0, 1}, {1, 2}, {2, 0}, {0, 3}}
	csr := buildTestGraph(4, edges)

	nodes := CycleNodes(csr)
	if len(nodes) != 3 {
		t.Errorf("Should find 3 cycle nodes, got %d", len(nodes))
	}
}

func TestBreakCycles(t *testing.T) {
	edges := [][2]int64{{0, 1}, {1, 2}, {2, 0}}
	csr := buildTestGraph(3, edges)

	toRemove := BreakCycles(csr)
	if len(toRemove) != 1 {
		t.Errorf("Should suggest 1 edge removal, got %d", len(toRemove))
	}
}
