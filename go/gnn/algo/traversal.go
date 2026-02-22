package algo

// BFSResult contains the result of a BFS traversal.
type BFSResult struct {
	Distances []int64 // Distance from source to each node (-1 if unreachable)
	Parents   []int64 // Parent node in BFS tree (-1 for source/unreachable)
	Order     []int64 // Nodes in BFS visit order
}

// BFS performs breadth-first search from a source node.
// Returns distances, parent pointers, and visit order.
//
// Time complexity: O(V + E)
// Space complexity: O(V)
func BFS(csr *CSR, source int64) *BFSResult {
	if csr == nil || csr.NumNodes == 0 {
		return &BFSResult{}
	}

	n := csr.NumNodes
	if source < 0 || source >= n {
		return &BFSResult{}
	}

	distances := make([]int64, n)
	parents := make([]int64, n)
	for i := range distances {
		distances[i] = -1
		parents[i] = -1
	}

	order := make([]int64, 0, n)
	queue := make([]int64, 0, n)

	// Start from source
	distances[source] = 0
	queue = append(queue, source)
	order = append(order, source)

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		dist := distances[node]

		// Visit neighbors using CSR
		start := csr.IndPtr[node]
		end := csr.IndPtr[node+1]
		for idx := start; idx < end; idx++ {
			neighbor := csr.Indices[idx]
			if neighbor >= 0 && neighbor < n && distances[neighbor] == -1 {
				distances[neighbor] = dist + 1
				parents[neighbor] = node
				queue = append(queue, neighbor)
				order = append(order, neighbor)
			}
		}
	}

	return &BFSResult{
		Distances: distances,
		Parents:   parents,
		Order:     order,
	}
}

// BFSPath returns the shortest path from source to target.
// Returns nil if no path exists.
func BFSPath(csr *CSR, source, target int64) []int64 {
	result := BFS(csr, source)
	if result.Distances[target] == -1 {
		return nil
	}

	// Backtrack from target to source
	path := make([]int64, 0)
	node := target
	for node != -1 {
		path = append(path, node)
		node = result.Parents[node]
	}

	// Reverse to get source -> target order
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}

	return path
}

// MultiBFS performs BFS from multiple sources simultaneously.
// Returns the closest source for each reachable node.
func MultiBFS(csr *CSR, sources []int64) (distances []int64, closestSource []int64) {
	if csr == nil || csr.NumNodes == 0 || len(sources) == 0 {
		return nil, nil
	}

	n := csr.NumNodes
	distances = make([]int64, n)
	closestSource = make([]int64, n)
	for i := range distances {
		distances[i] = -1
		closestSource[i] = -1
	}

	queue := make([]int64, 0, n)

	// Initialize all sources
	for _, src := range sources {
		if src >= 0 && src < n && distances[src] == -1 {
			distances[src] = 0
			closestSource[src] = src
			queue = append(queue, src)
		}
	}

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		dist := distances[node]
		srcNode := closestSource[node]

		start := csr.IndPtr[node]
		end := csr.IndPtr[node+1]
		for idx := start; idx < end; idx++ {
			neighbor := csr.Indices[idx]
			if neighbor >= 0 && neighbor < n && distances[neighbor] == -1 {
				distances[neighbor] = dist + 1
				closestSource[neighbor] = srcNode
				queue = append(queue, neighbor)
			}
		}
	}

	return distances, closestSource
}

// DFSResult contains the result of a DFS traversal.
type DFSResult struct {
	DiscoveryTime []int64 // When each node was first visited
	FinishTime    []int64 // When each node's subtree was fully explored
	Parents       []int64 // Parent node in DFS tree
	PreOrder      []int64 // Nodes in pre-order (discovery order)
	PostOrder     []int64 // Nodes in post-order (finish order)
}

// DFS performs depth-first search from a source node.
// Uses iterative implementation to avoid stack overflow on deep graphs.
//
// Time complexity: O(V + E)
// Space complexity: O(V)
func DFS(csr *CSR, source int64) *DFSResult {
	if csr == nil || csr.NumNodes == 0 {
		return &DFSResult{}
	}

	n := csr.NumNodes
	if source < 0 || source >= n {
		return &DFSResult{}
	}

	discovery := make([]int64, n)
	finish := make([]int64, n)
	parents := make([]int64, n)
	for i := range discovery {
		discovery[i] = -1
		finish[i] = -1
		parents[i] = -1
	}

	preOrder := make([]int64, 0, n)
	postOrder := make([]int64, 0, n)

	// Stack entry: (node, neighborIndex, isBacktrack)
	type stackEntry struct {
		node        int64
		neighborIdx int64
		backtrack   bool
	}

	stack := make([]stackEntry, 0, n)
	stack = append(stack, stackEntry{node: source, neighborIdx: csr.IndPtr[source], backtrack: false})
	time := int64(0)

	for len(stack) > 0 {
		top := &stack[len(stack)-1]
		node := top.node

		if !top.backtrack {
			// First visit
			if discovery[node] == -1 {
				discovery[node] = time
				time++
				preOrder = append(preOrder, node)
			}
			top.backtrack = true
		}

		// Find next unvisited neighbor
		found := false
		start := top.neighborIdx
		end := csr.IndPtr[node+1]
		for idx := start; idx < end; idx++ {
			neighbor := csr.Indices[idx]
			if neighbor >= 0 && neighbor < n && discovery[neighbor] == -1 {
				top.neighborIdx = idx + 1
				parents[neighbor] = node
				stack = append(stack, stackEntry{node: neighbor, neighborIdx: csr.IndPtr[neighbor], backtrack: false})
				found = true
				break
			}
		}

		if !found {
			// All neighbors explored, finish this node
			finish[node] = time
			time++
			postOrder = append(postOrder, node)
			stack = stack[:len(stack)-1]
		}
	}

	return &DFSResult{
		DiscoveryTime: discovery,
		FinishTime:    finish,
		Parents:       parents,
		PreOrder:      preOrder,
		PostOrder:     postOrder,
	}
}

// DFSFull performs DFS from all nodes to discover the entire graph.
// Useful for finding all connected components and detecting cycles.
func DFSFull(csr *CSR) *DFSResult {
	if csr == nil || csr.NumNodes == 0 {
		return &DFSResult{}
	}

	n := csr.NumNodes
	discovery := make([]int64, n)
	finish := make([]int64, n)
	parents := make([]int64, n)
	for i := range discovery {
		discovery[i] = -1
		finish[i] = -1
		parents[i] = -1
	}

	preOrder := make([]int64, 0, n)
	postOrder := make([]int64, 0, n)
	time := int64(0)

	type stackEntry struct {
		node        int64
		neighborIdx int64
		backtrack   bool
	}

	for root := int64(0); root < n; root++ {
		if discovery[root] != -1 {
			continue
		}

		stack := make([]stackEntry, 0, n)
		stack = append(stack, stackEntry{node: root, neighborIdx: csr.IndPtr[root], backtrack: false})

		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			node := top.node

			if !top.backtrack {
				if discovery[node] == -1 {
					discovery[node] = time
					time++
					preOrder = append(preOrder, node)
				}
				top.backtrack = true
			}

			found := false
			start := top.neighborIdx
			end := csr.IndPtr[node+1]
			for idx := start; idx < end; idx++ {
				neighbor := csr.Indices[idx]
				if neighbor >= 0 && neighbor < n && discovery[neighbor] == -1 {
					top.neighborIdx = idx + 1
					parents[neighbor] = node
					stack = append(stack, stackEntry{node: neighbor, neighborIdx: csr.IndPtr[neighbor], backtrack: false})
					found = true
					break
				}
			}

			if !found {
				finish[node] = time
				time++
				postOrder = append(postOrder, node)
				stack = stack[:len(stack)-1]
			}
		}
	}

	return &DFSResult{
		DiscoveryTime: discovery,
		FinishTime:    finish,
		Parents:       parents,
		PreOrder:      preOrder,
		PostOrder:     postOrder,
	}
}

// ConnectedComponents finds connected components in an undirected graph.
// Returns component ID for each node (-1 if unreachable from any visited node).
func ConnectedComponents(csr *CSR) (componentID []int64, numComponents int) {
	if csr == nil || csr.NumNodes == 0 {
		return nil, 0
	}

	n := csr.NumNodes
	componentID = make([]int64, n)
	for i := range componentID {
		componentID[i] = -1
	}

	currentComponent := int64(0)
	queue := make([]int64, 0, n)

	for start := int64(0); start < n; start++ {
		if componentID[start] != -1 {
			continue
		}

		// BFS to find all nodes in this component
		queue = queue[:0]
		queue = append(queue, start)
		componentID[start] = currentComponent

		for len(queue) > 0 {
			node := queue[0]
			queue = queue[1:]

			startIdx := csr.IndPtr[node]
			endIdx := csr.IndPtr[node+1]
			for idx := startIdx; idx < endIdx; idx++ {
				neighbor := csr.Indices[idx]
				if neighbor >= 0 && neighbor < n && componentID[neighbor] == -1 {
					componentID[neighbor] = currentComponent
					queue = append(queue, neighbor)
				}
			}
		}

		currentComponent++
	}

	return componentID, int(currentComponent)
}

// Reachable returns all nodes reachable from the source.
func Reachable(csr *CSR, source int64) []int64 {
	result := BFS(csr, source)
	return result.Order
}

// ReachableFrom returns all nodes reachable from any of the sources.
func ReachableFrom(csr *CSR, sources []int64) []int64 {
	if csr == nil || len(sources) == 0 {
		return nil
	}

	n := csr.NumNodes
	visited := make([]bool, n)
	order := make([]int64, 0, n)
	queue := make([]int64, 0, n)

	for _, src := range sources {
		if src >= 0 && src < n && !visited[src] {
			visited[src] = true
			queue = append(queue, src)
			order = append(order, src)
		}
	}

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		start := csr.IndPtr[node]
		end := csr.IndPtr[node+1]
		for idx := start; idx < end; idx++ {
			neighbor := csr.Indices[idx]
			if neighbor >= 0 && neighbor < n && !visited[neighbor] {
				visited[neighbor] = true
				queue = append(queue, neighbor)
				order = append(order, neighbor)
			}
		}
	}

	return order
}
