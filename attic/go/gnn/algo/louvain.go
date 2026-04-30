package algo

// LouvainConfig configures the Louvain community detection algorithm.
type LouvainConfig struct {
	MaxIterations int     // Max passes over nodes per phase (default 10)
	MaxPasses     int     // Max phase iterations (default 10)
	MinModGain    float64 // Minimum modularity gain to continue (default 0.0001)
	Resolution    float64 // Resolution parameter (default 1.0, higher = smaller communities)
}

// DefaultLouvainConfig returns sensible defaults.
var DefaultLouvainConfig = LouvainConfig{
	MaxIterations: 10,
	MaxPasses:     10,
	MinModGain:    0.0001,
	Resolution:    1.0,
}

// LouvainResult contains the community detection results.
type LouvainResult struct {
	Communities []int64 // Community ID for each node
	NumComms    int     // Number of communities found
	Modularity  float64 // Final modularity score
	Passes      int     // Number of phases executed
	Converged   bool    // Whether algorithm converged
}

// CommunityStats returns statistics for each community.
type CommunityStats struct {
	ID       int64   // Community ID
	Size     int     // Number of nodes
	Internal float64 // Sum of internal edge weights
	Total    float64 // Sum of all edge weights (internal + external)
}

// Louvain performs community detection using the Louvain algorithm.
// Works on CSR adjacency with optional edge weights.
//
// The Louvain algorithm is a greedy modularity optimization method:
// 1. Local phase: Move nodes to neighboring communities for max modularity gain
// 2. Aggregation phase: Merge communities into super-nodes and repeat
//
// Use cases in agent systems:
// - Cluster agents with high interaction frequency
// - Identify work locality for task scheduling
// - Group related agents for co-location
//
// Time complexity: O(V log V) for sparse graphs
// Space complexity: O(V + E)
func Louvain(csr *CSR, weights []float64, cfg LouvainConfig) *LouvainResult {
	if csr == nil || csr.NumNodes == 0 {
		return &LouvainResult{}
	}

	// Apply defaults
	if cfg.MaxIterations <= 0 {
		cfg.MaxIterations = 10
	}
	if cfg.MaxPasses <= 0 {
		cfg.MaxPasses = 10
	}
	if cfg.MinModGain <= 0 {
		cfg.MinModGain = 0.0001
	}
	if cfg.Resolution <= 0 {
		cfg.Resolution = 1.0
	}

	n := int(csr.NumNodes)

	// If no weights provided, use 1.0 for all edges
	if weights == nil {
		weights = make([]float64, csr.NumEdges)
		for i := range weights {
			weights[i] = 1.0
		}
	}

	// Initialize: each node is its own community
	communities := make([]int64, n)
	for i := range communities {
		communities[i] = int64(i)
	}

	// Compute total edge weight (m = sum of all weights / 2 for undirected)
	var totalWeight float64
	for _, w := range weights {
		totalWeight += w
	}
	m := totalWeight / 2.0 // Each edge counted once
	if m == 0 {
		// No edges, each node is its own community
		return &LouvainResult{
			Communities: communities,
			NumComms:    n,
			Modularity:  0,
			Passes:      0,
			Converged:   true,
		}
	}

	// Compute weighted degree for each node (k_i)
	nodeDegree := make([]float64, n)
	for i := 0; i < n; i++ {
		start := csr.IndPtr[i]
		end := csr.IndPtr[i+1]
		for idx := start; idx < end; idx++ {
			nodeDegree[i] += weights[idx]
		}
	}

	// Community internal weights and total weights
	commInternal := make([]float64, n) // Sum of weights inside community
	commTotal := make([]float64, n)    // Sum of weighted degrees in community

	for i := 0; i < n; i++ {
		commTotal[i] = nodeDegree[i]
		// Self-loops count as internal
		start := csr.IndPtr[i]
		end := csr.IndPtr[i+1]
		for idx := start; idx < end; idx++ {
			if csr.Indices[idx] == int64(i) {
				commInternal[i] += weights[idx]
			}
		}
	}

	var passes int
	var converged bool
	var currentMod float64

	// Main Louvain loop
	for pass := 0; pass < cfg.MaxPasses; pass++ {
		passes = pass + 1
		improvement := false

		// Phase 1: Local moving
		for iter := 0; iter < cfg.MaxIterations; iter++ {
			moved := false

			for i := 0; i < n; i++ {
				currentComm := communities[i]
				ki := nodeDegree[i]

				// Remove node from its current community
				commTotal[currentComm] -= ki

				// Compute weights to neighboring communities
				neighborComms := make(map[int64]float64)
				start := csr.IndPtr[i]
				end := csr.IndPtr[i+1]
				for idx := start; idx < end; idx++ {
					neighbor := csr.Indices[idx]
					if neighbor >= 0 && neighbor < int64(n) {
						neighborComm := communities[neighbor]
						neighborComms[neighborComm] += weights[idx]
					}
				}

				// Self-loop weight to current community
				selfWeight := neighborComms[currentComm]

				// Update internal weight (remove self-contribution)
				commInternal[currentComm] -= 2.0*selfWeight + selfWeight // Approximate removal

				// Find best community
				bestComm := currentComm
				bestGain := 0.0

				for comm, wToComm := range neighborComms {
					// Modularity gain for moving to this community
					// ΔQ = [Σin + 2*ki,in / 2m - ((Σtot + ki) / 2m)^2]
					//    - [Σin / 2m - (Σtot / 2m)^2 - (ki / 2m)^2]
					// Simplified: ΔQ = wToComm/m - ki*commTotal[comm]*resolution / (2*m^2)

					gain := wToComm - cfg.Resolution*ki*commTotal[comm]/(2.0*m)

					if gain > bestGain {
						bestGain = gain
						bestComm = comm
					}
				}

				// Also consider staying in current community (re-add self)
				if _, ok := neighborComms[currentComm]; !ok {
					neighborComms[currentComm] = 0
				}
				currentGain := neighborComms[currentComm] - cfg.Resolution*ki*commTotal[currentComm]/(2.0*m)
				if currentGain >= bestGain {
					bestComm = currentComm
				}

				// Move to best community
				communities[i] = bestComm
				commTotal[bestComm] += ki

				// Update internal weights
				wToBest := neighborComms[bestComm]
				commInternal[bestComm] += 2.0 * wToBest

				if bestComm != currentComm {
					moved = true
				}
			}

			if !moved {
				break
			}
			improvement = true
		}

		// Compute current modularity
		currentMod = computeModularity(csr, weights, communities, cfg.Resolution)

		if !improvement {
			converged = true
			break
		}

		// Phase 2: Aggregate (renumber communities and potentially contract graph)
		// For simplicity, we just renumber communities here
		communities, _ = renumberCommunities(communities)
	}

	// Final renumbering
	finalComms, numComms := renumberCommunities(communities)

	return &LouvainResult{
		Communities: finalComms,
		NumComms:    numComms,
		Modularity:  currentMod,
		Passes:      passes,
		Converged:   converged,
	}
}

// LouvainUnweighted is a convenience function for unweighted graphs.
func LouvainUnweighted(csr *CSR, cfg LouvainConfig) *LouvainResult {
	return Louvain(csr, nil, cfg)
}

// computeModularity calculates the modularity score Q.
// Q = (1/2m) * Σij [Aij - (ki*kj)/(2m)] * δ(ci, cj)
func computeModularity(csr *CSR, weights []float64, communities []int64, resolution float64) float64 {
	n := int(csr.NumNodes)
	var totalWeight float64
	for _, w := range weights {
		totalWeight += w
	}
	m := totalWeight / 2.0
	if m == 0 {
		return 0
	}

	// Compute weighted degrees
	nodeDegree := make([]float64, n)
	for i := 0; i < n; i++ {
		start := csr.IndPtr[i]
		end := csr.IndPtr[i+1]
		for idx := start; idx < end; idx++ {
			nodeDegree[i] += weights[idx]
		}
	}

	// Sum internal edges and degree products within communities
	var q float64
	for i := 0; i < n; i++ {
		ci := communities[i]
		ki := nodeDegree[i]

		start := csr.IndPtr[i]
		end := csr.IndPtr[i+1]
		for idx := start; idx < end; idx++ {
			j := csr.Indices[idx]
			if j >= 0 && j < int64(n) {
				cj := communities[j]
				if ci == cj {
					q += weights[idx] - resolution*ki*nodeDegree[j]/(2.0*m)
				}
			}
		}
	}

	return q / (2.0 * m)
}

// renumberCommunities compresses community IDs to be contiguous starting from 0.
func renumberCommunities(communities []int64) ([]int64, int) {
	seen := make(map[int64]int64)
	result := make([]int64, len(communities))
	nextID := int64(0)

	for i, c := range communities {
		if newID, ok := seen[c]; ok {
			result[i] = newID
		} else {
			seen[c] = nextID
			result[i] = nextID
			nextID++
		}
	}

	return result, int(nextID)
}

// GetCommunityNodes returns all node IDs belonging to a specific community.
func GetCommunityNodes(result *LouvainResult, communityID int64) []int64 {
	if result == nil {
		return nil
	}

	nodes := make([]int64, 0)
	for i, c := range result.Communities {
		if c == communityID {
			nodes = append(nodes, int64(i))
		}
	}
	return nodes
}

// GetCommunityStats computes statistics for each community.
func GetCommunityStats(csr *CSR, weights []float64, result *LouvainResult) []CommunityStats {
	if csr == nil || result == nil {
		return nil
	}

	n := int(csr.NumNodes)

	// If no weights, use 1.0
	if weights == nil {
		weights = make([]float64, csr.NumEdges)
		for i := range weights {
			weights[i] = 1.0
		}
	}

	stats := make([]CommunityStats, result.NumComms)
	for i := range stats {
		stats[i].ID = int64(i)
	}

	// Count sizes and compute edge weights
	for i := 0; i < n; i++ {
		ci := result.Communities[i]
		stats[ci].Size++

		start := csr.IndPtr[i]
		end := csr.IndPtr[i+1]
		for idx := start; idx < end; idx++ {
			j := csr.Indices[idx]
			if j >= 0 && j < int64(n) {
				w := weights[idx]
				stats[ci].Total += w
				if result.Communities[j] == ci {
					stats[ci].Internal += w
				}
			}
		}
	}

	// Internal edges are counted twice, divide by 2
	for i := range stats {
		stats[i].Internal /= 2.0
	}

	return stats
}

// CommunityDensity computes the density of each community.
// Density = 2 * internal_edges / (size * (size - 1))
func CommunityDensity(stats []CommunityStats) []float64 {
	densities := make([]float64, len(stats))
	for i, s := range stats {
		if s.Size <= 1 {
			densities[i] = 1.0 // Single node is maximally dense
		} else {
			maxEdges := float64(s.Size * (s.Size - 1) / 2)
			densities[i] = s.Internal / maxEdges
		}
	}
	return densities
}

// InterCommunityEdges returns the weight of edges between two communities.
func InterCommunityEdges(csr *CSR, weights []float64, communities []int64, c1, c2 int64) float64 {
	if csr == nil {
		return 0
	}

	n := int(csr.NumNodes)
	if weights == nil {
		weights = make([]float64, csr.NumEdges)
		for i := range weights {
			weights[i] = 1.0
		}
	}

	var total float64
	for i := 0; i < n; i++ {
		if communities[i] != c1 {
			continue
		}
		start := csr.IndPtr[i]
		end := csr.IndPtr[i+1]
		for idx := start; idx < end; idx++ {
			j := csr.Indices[idx]
			if j >= 0 && j < int64(n) && communities[j] == c2 {
				total += weights[idx]
			}
		}
	}
	return total
}
