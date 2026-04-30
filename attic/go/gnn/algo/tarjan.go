package algo

// SCCResult contains the results of strongly connected component detection.
type SCCResult struct {
	Components    [][]int64 // List of SCCs, each containing node IDs
	ComponentID   []int64   // SCC ID for each node
	NumComponents int       // Total number of SCCs
	HasCycles     bool      // True if any SCC has more than one node
	LargestSCC    int       // Index of largest SCC
	LargestSize   int       // Size of largest SCC
}

// TarjanSCC finds all strongly connected components using Tarjan's algorithm.
// An SCC is a maximal set of nodes where every node is reachable from every other.
//
// Use cases in agent systems:
// - Detect circular dependencies in workflows
// - Find feedback loops in agent communication
// - Identify tightly coupled agent groups
//
// Time complexity: O(V + E)
// Space complexity: O(V)
func TarjanSCC(csr *CSR) *SCCResult {
	if csr == nil || csr.NumNodes == 0 {
		return &SCCResult{}
	}

	n := int(csr.NumNodes)

	// Tarjan state
	index := make([]int, n)
	lowlink := make([]int, n)
	onStack := make([]bool, n)
	componentID := make([]int64, n)

	for i := range index {
		index[i] = -1
		lowlink[i] = -1
		componentID[i] = -1
	}

	var stack []int64
	var components [][]int64
	currentIndex := 0

	// Iterative Tarjan (avoids stack overflow on deep graphs)
	type frame struct {
		node     int64
		neighbor int64
		phase    int
	}

	for startNode := int64(0); startNode < int64(n); startNode++ {
		if index[startNode] != -1 {
			continue
		}

		callStack := []frame{{node: startNode, neighbor: csr.IndPtr[startNode], phase: 0}}

		for len(callStack) > 0 {
			f := &callStack[len(callStack)-1]
			node := f.node

			switch f.phase {
			case 0:
				index[node] = currentIndex
				lowlink[node] = currentIndex
				currentIndex++
				stack = append(stack, node)
				onStack[node] = true
				f.phase = 1

			case 1:
				found := false
				for f.neighbor < csr.IndPtr[node+1] {
					neighbor := csr.Indices[f.neighbor]
					f.neighbor++

					if neighbor < 0 || neighbor >= int64(n) {
						continue
					}

					if index[neighbor] == -1 {
						callStack = append(callStack, frame{
							node:     neighbor,
							neighbor: csr.IndPtr[neighbor],
							phase:    0,
						})
						found = true
						break
					} else if onStack[neighbor] {
						if lowlink[neighbor] < lowlink[node] {
							lowlink[node] = lowlink[neighbor]
						}
					}
				}

				if !found {
					f.phase = 2
				}

			case 2:
				if len(callStack) > 1 {
					parent := &callStack[len(callStack)-2]
					if lowlink[node] < lowlink[parent.node] {
						lowlink[parent.node] = lowlink[node]
					}
				}

				if lowlink[node] == index[node] {
					var component []int64
					for {
						top := stack[len(stack)-1]
						stack = stack[:len(stack)-1]
						onStack[top] = false
						component = append(component, top)
						if top == node {
							break
						}
					}

					compID := int64(len(components))
					for _, n := range component {
						componentID[n] = compID
					}
					components = append(components, component)
				}

				callStack = callStack[:len(callStack)-1]
			}
		}
	}

	hasCycles := false
	largestIdx := 0
	largestSize := 0

	for i, comp := range components {
		if len(comp) > largestSize {
			largestSize = len(comp)
			largestIdx = i
		}
		if len(comp) > 1 {
			hasCycles = true
		}
	}

	return &SCCResult{
		Components:    components,
		ComponentID:   componentID,
		NumComponents: len(components),
		HasCycles:     hasCycles,
		LargestSCC:    largestIdx,
		LargestSize:   largestSize,
	}
}

// FindCycles returns all SCCs that contain cycles.
func FindCycles(csr *CSR) [][]int64 {
	result := TarjanSCC(csr)
	if result == nil {
		return nil
	}

	cycles := make([][]int64, 0)
	for _, comp := range result.Components {
		if len(comp) > 1 {
			cycles = append(cycles, comp)
		}
	}

	// Check for self-loops
	for i := int64(0); i < csr.NumNodes; i++ {
		start := csr.IndPtr[i]
		end := csr.IndPtr[i+1]
		for idx := start; idx < end; idx++ {
			if csr.Indices[idx] == i {
				cycles = append(cycles, []int64{i})
				break
			}
		}
	}

	return cycles
}

// HasCycle returns true if the graph contains any cycles.
func HasCycle(csr *CSR) bool {
	result := TarjanSCC(csr)
	if result == nil {
		return false
	}

	if result.HasCycles {
		return true
	}

	// Check for self-loops
	for i := int64(0); i < csr.NumNodes; i++ {
		start := csr.IndPtr[i]
		end := csr.IndPtr[i+1]
		for idx := start; idx < end; idx++ {
			if csr.Indices[idx] == i {
				return true
			}
		}
	}

	return false
}

// IsDAG returns true if the graph is a directed acyclic graph.
func IsDAG(csr *CSR) bool {
	return !HasCycle(csr)
}

// GetSCCGraph creates a condensation graph where each SCC is a single node.
func GetSCCGraph(csr *CSR) (*CSR, []int64) {
	result := TarjanSCC(csr)
	if result == nil || result.NumComponents == 0 {
		return nil, nil
	}

	numSCCs := int64(result.NumComponents)
	sccEdges := make(map[int64]map[int64]bool)
	for i := int64(0); i < numSCCs; i++ {
		sccEdges[i] = make(map[int64]bool)
	}

	for src := int64(0); src < csr.NumNodes; src++ {
		srcSCC := result.ComponentID[src]
		start := csr.IndPtr[src]
		end := csr.IndPtr[src+1]
		for idx := start; idx < end; idx++ {
			dst := csr.Indices[idx]
			if dst >= 0 && dst < csr.NumNodes {
				dstSCC := result.ComponentID[dst]
				if srcSCC != dstSCC {
					sccEdges[srcSCC][dstSCC] = true
				}
			}
		}
	}

	indptr := make([]int64, numSCCs+1)
	var indices []int64

	for i := int64(0); i < numSCCs; i++ {
		indptr[i] = int64(len(indices))
		for dst := range sccEdges[i] {
			indices = append(indices, dst)
		}
	}
	indptr[numSCCs] = int64(len(indices))

	return NewCSR(numSCCs, indptr, indices), result.ComponentID
}

// CycleNodes returns all nodes that are part of any cycle.
func CycleNodes(csr *CSR) []int64 {
	result := TarjanSCC(csr)
	if result == nil {
		return nil
	}

	var cycleNodes []int64

	for _, comp := range result.Components {
		if len(comp) > 1 {
			cycleNodes = append(cycleNodes, comp...)
		}
	}

	// Add self-loop nodes
	for i := int64(0); i < csr.NumNodes; i++ {
		start := csr.IndPtr[i]
		end := csr.IndPtr[i+1]
		for idx := start; idx < end; idx++ {
			if csr.Indices[idx] == i {
				found := false
				for _, n := range cycleNodes {
					if n == i {
						found = true
						break
					}
				}
				if !found {
					cycleNodes = append(cycleNodes, i)
				}
				break
			}
		}
	}

	return cycleNodes
}

// BreakCycles suggests edges to remove to make the graph acyclic.
func BreakCycles(csr *CSR) [][2]int64 {
	result := TarjanSCC(csr)
	if result == nil || !result.HasCycles {
		return nil
	}

	var edgesToRemove [][2]int64

	for _, comp := range result.Components {
		if len(comp) <= 1 {
			continue
		}

		compSet := make(map[int64]bool)
		for _, n := range comp {
			compSet[n] = true
		}

		maxDeg := int64(0)
		var maxNode int64 = -1

		for _, node := range comp {
			deg := int64(0)
			start := csr.IndPtr[node]
			end := csr.IndPtr[node+1]
			for idx := start; idx < end; idx++ {
				if compSet[csr.Indices[idx]] {
					deg++
				}
			}
			if deg > maxDeg {
				maxDeg = deg
				maxNode = node
			}
		}

		if maxNode >= 0 {
			start := csr.IndPtr[maxNode]
			end := csr.IndPtr[maxNode+1]
			for idx := start; idx < end; idx++ {
				dst := csr.Indices[idx]
				if compSet[dst] {
					edgesToRemove = append(edgesToRemove, [2]int64{maxNode, dst})
					break
				}
			}
		}
	}

	return edgesToRemove
}
