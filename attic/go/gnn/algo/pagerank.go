package algo

import (
	"math"
)

// PageRankConfig configures the PageRank algorithm.
type PageRankConfig struct {
	Damping    float32 // Damping factor (default 0.85)
	Iterations int     // Max iterations (default 20)
	Tolerance  float32 // Convergence tolerance (default 1e-6)
}

// DefaultPageRankConfig returns default PageRank configuration.
var DefaultPageRankConfig = PageRankConfig{
	Damping:    0.85,
	Iterations: 20,
	Tolerance:  1e-6,
}

// PageRankResult contains PageRank computation results.
type PageRankResult struct {
	Scores     []float32 // PageRank score for each node
	Iterations int       // Actual iterations performed
	Converged  bool      // Whether tolerance was reached
}

// PageRank computes PageRank scores on a CSR adjacency matrix.
// Uses power iteration directly on CSR format for maximum performance.
//
// Time complexity: O(E × iterations)
// Space complexity: O(N) for rank vectors
//
// For a 10K node graph with 50K edges:
// - Expected latency: <1ms
// - Memory: ~80KB (2 float32 arrays)
func PageRank(csr *CSR, cfg PageRankConfig) *PageRankResult {
	if csr == nil || csr.NumNodes == 0 {
		return &PageRankResult{}
	}

	n := csr.NumNodes
	damping := cfg.Damping
	if damping <= 0 || damping >= 1 {
		damping = 0.85
	}
	maxIter := cfg.Iterations
	if maxIter <= 0 {
		maxIter = 20
	}
	tol := cfg.Tolerance
	if tol <= 0 {
		tol = 1e-6
	}

	// Initialize uniform distribution
	rank := make([]float32, n)
	newRank := make([]float32, n)
	initial := float32(1.0) / float32(n)
	for i := range rank {
		rank[i] = initial
	}

	// Precompute out-degrees for efficiency
	outDeg := make([]float32, n)
	for i := int64(0); i < n; i++ {
		outDeg[i] = float32(csr.IndPtr[i+1] - csr.IndPtr[i])
	}

	// Count dangling nodes (nodes with no outgoing edges)
	// Their PageRank is distributed evenly to all nodes
	teleport := (1 - damping) / float32(n)

	var iter int
	var converged bool

	for iter = 0; iter < maxIter; iter++ {
		// Reset new ranks with teleport probability
		for i := range newRank {
			newRank[i] = teleport
		}

		// Handle dangling nodes: sum their rank and distribute evenly
		var danglingSum float32
		for i := int64(0); i < n; i++ {
			if outDeg[i] == 0 {
				danglingSum += rank[i]
			}
		}
		danglingContrib := damping * danglingSum / float32(n)
		for i := range newRank {
			newRank[i] += danglingContrib
		}

		// Main PageRank iteration: propagate rank through edges
		// Direct CSR access for zero-copy performance
		for src := int64(0); src < n; src++ {
			if outDeg[src] == 0 {
				continue // Dangling node, already handled
			}
			contribution := damping * rank[src] / outDeg[src]

			// Iterate over outgoing edges using CSR offsets
			start := csr.IndPtr[src]
			end := csr.IndPtr[src+1]
			for idx := start; idx < end; idx++ {
				dst := csr.Indices[idx]
				if dst >= 0 && dst < n {
					newRank[dst] += contribution
				}
			}
		}

		// Check convergence (L1 norm of difference)
		var diff float32
		for i := range rank {
			diff += float32(math.Abs(float64(newRank[i] - rank[i])))
		}

		// Swap buffers
		rank, newRank = newRank, rank

		if diff < tol {
			converged = true
			iter++ // Count final iteration
			break
		}
	}

	return &PageRankResult{
		Scores:     rank,
		Iterations: iter,
		Converged:  converged,
	}
}

// PageRankTopK returns the top-K nodes by PageRank score.
// Returns node indices sorted by descending score.
func PageRankTopK(result *PageRankResult, k int) []int64 {
	if result == nil || len(result.Scores) == 0 {
		return nil
	}

	n := len(result.Scores)
	if k <= 0 || k > n {
		k = n
	}

	// Build index-score pairs
	type scoredNode struct {
		idx   int64
		score float32
	}
	nodes := make([]scoredNode, n)
	for i := range nodes {
		nodes[i] = scoredNode{idx: int64(i), score: result.Scores[i]}
	}

	// Partial sort: find top-K using selection
	// For small K, this is more efficient than full sort
	for i := 0; i < k; i++ {
		maxIdx := i
		for j := i + 1; j < n; j++ {
			if nodes[j].score > nodes[maxIdx].score {
				maxIdx = j
			}
		}
		nodes[i], nodes[maxIdx] = nodes[maxIdx], nodes[i]
	}

	// Extract indices
	topK := make([]int64, k)
	for i := 0; i < k; i++ {
		topK[i] = nodes[i].idx
	}

	return topK
}

// PersonalizedPageRank computes PageRank with a personalization vector.
// personalNodes are the nodes to bias towards (restart probability).
func PersonalizedPageRank(csr *CSR, cfg PageRankConfig, personalNodes []int64) *PageRankResult {
	if csr == nil || csr.NumNodes == 0 {
		return &PageRankResult{}
	}

	n := csr.NumNodes
	damping := cfg.Damping
	if damping <= 0 || damping >= 1 {
		damping = 0.85
	}
	maxIter := cfg.Iterations
	if maxIter <= 0 {
		maxIter = 20
	}
	tol := cfg.Tolerance
	if tol <= 0 {
		tol = 1e-6
	}

	// Build personalization vector
	personal := make([]float32, n)
	if len(personalNodes) == 0 {
		// Uniform if no personalization specified
		val := float32(1.0) / float32(n)
		for i := range personal {
			personal[i] = val
		}
	} else {
		val := float32(1.0) / float32(len(personalNodes))
		for _, nodeID := range personalNodes {
			if nodeID >= 0 && nodeID < n {
				personal[nodeID] = val
			}
		}
	}

	// Initialize with personalization
	rank := make([]float32, n)
	newRank := make([]float32, n)
	copy(rank, personal)

	// Precompute out-degrees
	outDeg := make([]float32, n)
	for i := int64(0); i < n; i++ {
		outDeg[i] = float32(csr.IndPtr[i+1] - csr.IndPtr[i])
	}

	var iter int
	var converged bool

	for iter = 0; iter < maxIter; iter++ {
		// Reset with personalized teleport
		for i := range newRank {
			newRank[i] = (1 - damping) * personal[i]
		}

		// Dangling node handling with personalization
		var danglingSum float32
		for i := int64(0); i < n; i++ {
			if outDeg[i] == 0 {
				danglingSum += rank[i]
			}
		}
		for i := range newRank {
			newRank[i] += damping * danglingSum * personal[i]
		}

		// Propagate through edges
		for src := int64(0); src < n; src++ {
			if outDeg[src] == 0 {
				continue
			}
			contribution := damping * rank[src] / outDeg[src]
			start := csr.IndPtr[src]
			end := csr.IndPtr[src+1]
			for idx := start; idx < end; idx++ {
				dst := csr.Indices[idx]
				if dst >= 0 && dst < n {
					newRank[dst] += contribution
				}
			}
		}

		// Check convergence
		var diff float32
		for i := range rank {
			diff += float32(math.Abs(float64(newRank[i] - rank[i])))
		}
		rank, newRank = newRank, rank

		if diff < tol {
			converged = true
			iter++
			break
		}
	}

	return &PageRankResult{
		Scores:     rank,
		Iterations: iter,
		Converged:  converged,
	}
}
