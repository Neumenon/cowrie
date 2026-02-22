package algo

import (
	"errors"
)

// ErrCycleDetected indicates the graph contains a cycle (not a DAG).
var ErrCycleDetected = errors.New("algo: cycle detected, graph is not a DAG")

// CriticalPathResult contains critical path analysis results.
type CriticalPathResult struct {
	Path             []int64   // Nodes on the critical path
	TotalTime        float32   // Total time along critical path
	EarliestStart    []float32 // Earliest start time for each node
	LatestStart      []float32 // Latest start time without delaying project
	Slack            []float32 // Slack time for each node (LS - ES)
	TopologicalOrder []int64   // Topological order of all nodes
}

// TopologicalSort returns nodes in topological order for a DAG.
// Returns error if the graph contains a cycle.
//
// Time complexity: O(V + E)
// Space complexity: O(V)
func TopologicalSort(csr *CSR) ([]int64, error) {
	if csr == nil || csr.NumNodes == 0 {
		return nil, nil
	}

	n := csr.NumNodes

	// Calculate in-degrees
	inDegree := make([]int64, n)
	for i := int64(0); i < n; i++ {
		start := csr.IndPtr[i]
		end := csr.IndPtr[i+1]
		for idx := start; idx < end; idx++ {
			dst := csr.Indices[idx]
			if dst >= 0 && dst < n {
				inDegree[dst]++
			}
		}
	}

	// Initialize queue with zero in-degree nodes
	queue := make([]int64, 0, n)
	for i := int64(0); i < n; i++ {
		if inDegree[i] == 0 {
			queue = append(queue, i)
		}
	}

	// Kahn's algorithm
	order := make([]int64, 0, n)
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		order = append(order, node)

		// Reduce in-degree of neighbors
		start := csr.IndPtr[node]
		end := csr.IndPtr[node+1]
		for idx := start; idx < end; idx++ {
			neighbor := csr.Indices[idx]
			if neighbor >= 0 && neighbor < n {
				inDegree[neighbor]--
				if inDegree[neighbor] == 0 {
					queue = append(queue, neighbor)
				}
			}
		}
	}

	// Check for cycle
	if int64(len(order)) != n {
		return nil, ErrCycleDetected
	}

	return order, nil
}

// CriticalPath finds the critical path in a weighted DAG.
// weights[i] is the time/cost for node i.
// This is useful for workflow optimization - identifies the longest path
// that determines the minimum completion time.
//
// Time complexity: O(V + E)
// Space complexity: O(V)
func CriticalPath(csr *CSR, weights []float32) (*CriticalPathResult, error) {
	if csr == nil || csr.NumNodes == 0 {
		return &CriticalPathResult{}, nil
	}

	n := csr.NumNodes

	// Default weights to 1 if not provided
	if weights == nil || int64(len(weights)) != n {
		weights = make([]float32, n)
		for i := range weights {
			weights[i] = 1.0
		}
	}

	// Get topological order
	topoOrder, err := TopologicalSort(csr)
	if err != nil {
		return nil, err
	}

	// Forward pass: compute earliest start times
	earliestStart := make([]float32, n)
	parent := make([]int64, n)
	for i := range parent {
		parent[i] = -1
	}

	for _, u := range topoOrder {
		start := csr.IndPtr[u]
		end := csr.IndPtr[u+1]
		for idx := start; idx < end; idx++ {
			v := csr.Indices[idx]
			if v >= 0 && v < n {
				newTime := earliestStart[u] + weights[u]
				if newTime > earliestStart[v] {
					earliestStart[v] = newTime
					parent[v] = u
				}
			}
		}
	}

	// Find the end node (maximum earliest start + weight)
	endNode := int64(-1)
	var totalTime float32
	for i := int64(0); i < n; i++ {
		finishTime := earliestStart[i] + weights[i]
		if finishTime > totalTime {
			totalTime = finishTime
			endNode = i
		}
	}

	// Backward pass: compute latest start times
	latestStart := make([]float32, n)
	for i := range latestStart {
		latestStart[i] = totalTime // Initialize to project end time
	}

	// Process in reverse topological order
	csrT := csr.Transpose() // Need incoming edges
	for i := len(topoOrder) - 1; i >= 0; i-- {
		u := topoOrder[i]
		// Latest finish time is constrained by successors
		start := csr.IndPtr[u]
		end := csr.IndPtr[u+1]
		for idx := start; idx < end; idx++ {
			v := csr.Indices[idx]
			if v >= 0 && v < n {
				// u must finish before v starts
				if latestStart[v]-weights[u] < latestStart[u] {
					latestStart[u] = latestStart[v] - weights[u]
				}
			}
		}
		// If no successors, latest start is project end - own duration
		if start == end {
			latestStart[u] = totalTime - weights[u]
		}
	}

	// Compute slack
	slack := make([]float32, n)
	for i := range slack {
		slack[i] = latestStart[i] - earliestStart[i]
	}

	// Backtrack to find critical path
	path := make([]int64, 0)
	if endNode >= 0 {
		node := endNode
		for node != -1 {
			path = append(path, node)
			node = parent[node]
		}
		// Reverse to get start -> end order
		for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
			path[i], path[j] = path[j], path[i]
		}
	}

	// Use csrT to suppress unused variable warning
	_ = csrT

	return &CriticalPathResult{
		Path:             path,
		TotalTime:        totalTime,
		EarliestStart:    earliestStart,
		LatestStart:      latestStart,
		Slack:            slack,
		TopologicalOrder: topoOrder,
	}, nil
}

// CriticalNodes returns nodes with zero slack (on critical path).
func CriticalNodes(result *CriticalPathResult, tolerance float32) []int64 {
	if result == nil {
		return nil
	}

	critical := make([]int64, 0)
	for i, s := range result.Slack {
		if s <= tolerance {
			critical = append(critical, int64(i))
		}
	}
	return critical
}

// LongestPath finds the longest path in a DAG from any source to any sink.
// This is equivalent to critical path with unit weights.
func LongestPath(csr *CSR) ([]int64, int64, error) {
	if csr == nil || csr.NumNodes == 0 {
		return nil, 0, nil
	}

	n := csr.NumNodes

	topoOrder, err := TopologicalSort(csr)
	if err != nil {
		return nil, 0, err
	}

	// Distance and parent tracking
	dist := make([]int64, n)
	parent := make([]int64, n)
	for i := range parent {
		parent[i] = -1
	}

	// DP: longest path to each node
	for _, u := range topoOrder {
		start := csr.IndPtr[u]
		end := csr.IndPtr[u+1]
		for idx := start; idx < end; idx++ {
			v := csr.Indices[idx]
			if v >= 0 && v < n {
				if dist[u]+1 > dist[v] {
					dist[v] = dist[u] + 1
					parent[v] = u
				}
			}
		}
	}

	// Find node with maximum distance
	endNode := int64(0)
	maxDist := int64(0)
	for i := int64(0); i < n; i++ {
		if dist[i] > maxDist {
			maxDist = dist[i]
			endNode = i
		}
	}

	// Backtrack path
	path := make([]int64, 0, maxDist+1)
	node := endNode
	for node != -1 {
		path = append(path, node)
		node = parent[node]
	}
	// Reverse
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}

	return path, maxDist, nil
}

// ShortestPathDAG finds shortest path in a DAG using topological sort.
// This is more efficient than Dijkstra for DAGs: O(V + E) vs O((V+E) log V).
func ShortestPathDAG(csr *CSR, weights []float32, source int64) ([]float32, []int64, error) {
	if csr == nil || csr.NumNodes == 0 {
		return nil, nil, nil
	}

	n := csr.NumNodes
	if source < 0 || source >= n {
		return nil, nil, nil
	}

	// Default unit weights
	if weights == nil || int64(len(weights)) != n {
		weights = make([]float32, n)
		for i := range weights {
			weights[i] = 1.0
		}
	}

	topoOrder, err := TopologicalSort(csr)
	if err != nil {
		return nil, nil, err
	}

	// Initialize distances
	const inf = float32(1e30)
	dist := make([]float32, n)
	parent := make([]int64, n)
	for i := range dist {
		dist[i] = inf
		parent[i] = -1
	}
	dist[source] = 0

	// Process in topological order starting from source
	started := false
	for _, u := range topoOrder {
		if u == source {
			started = true
		}
		if !started || dist[u] == inf {
			continue
		}

		start := csr.IndPtr[u]
		end := csr.IndPtr[u+1]
		for idx := start; idx < end; idx++ {
			v := csr.Indices[idx]
			if v >= 0 && v < n {
				newDist := dist[u] + weights[v]
				if newDist < dist[v] {
					dist[v] = newDist
					parent[v] = u
				}
			}
		}
	}

	return dist, parent, nil
}
