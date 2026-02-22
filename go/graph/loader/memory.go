package loader

import (
	"sync"

	"github.com/Neumenon/cowrie/graph"
)

// MemoryNode represents a node in the in-memory graph.
type MemoryNode struct {
	ID     string
	Labels []string
	Props  map[string]any
}

// MemoryEdge represents an edge in the in-memory graph.
type MemoryEdge struct {
	ID     string
	Label  string
	FromID string
	ToID   string
	Props  map[string]any
}

// MemoryTriple represents an RDF triple in the in-memory graph.
type MemoryTriple struct {
	Subject   graph.RDFTerm
	Predicate string
	Object    graph.RDFTerm
	Graph     string
}

// MemoryGraph is an in-memory graph database for testing.
type MemoryGraph struct {
	mu      sync.RWMutex
	nodes   map[string]*MemoryNode
	edges   map[string]*MemoryEdge
	triples []MemoryTriple
}

// NewMemoryGraph creates a new in-memory graph.
func NewMemoryGraph() *MemoryGraph {
	return &MemoryGraph{
		nodes:   make(map[string]*MemoryNode),
		edges:   make(map[string]*MemoryEdge),
		triples: make([]MemoryTriple, 0),
	}
}

// WriteNode writes a node to the in-memory graph.
func (mg *MemoryGraph) WriteNode(evt *graph.NodeEvent) error {
	mg.mu.Lock()
	defer mg.mu.Unlock()

	if evt.Op == graph.OpDelete {
		delete(mg.nodes, evt.ID)
		return nil
	}

	// Upsert: create or update
	node, exists := mg.nodes[evt.ID]
	if !exists {
		node = &MemoryNode{
			ID:    evt.ID,
			Props: make(map[string]any),
		}
		mg.nodes[evt.ID] = node
	}

	// Update labels
	if len(evt.Labels) > 0 {
		node.Labels = make([]string, len(evt.Labels))
		copy(node.Labels, evt.Labels)
	}

	// Merge properties
	for k, v := range evt.Props {
		node.Props[k] = v
	}

	return nil
}

// WriteEdge writes an edge to the in-memory graph.
func (mg *MemoryGraph) WriteEdge(evt *graph.EdgeEvent) error {
	mg.mu.Lock()
	defer mg.mu.Unlock()

	// Generate ID if not provided
	edgeID := evt.ID
	if edgeID == "" {
		edgeID = evt.FromID + "-" + evt.Label + "-" + evt.ToID
	}

	if evt.Op == graph.OpDelete {
		delete(mg.edges, edgeID)
		return nil
	}

	// Upsert: create or update
	edge, exists := mg.edges[edgeID]
	if !exists {
		edge = &MemoryEdge{
			ID:     edgeID,
			Label:  evt.Label,
			FromID: evt.FromID,
			ToID:   evt.ToID,
			Props:  make(map[string]any),
		}
		mg.edges[edgeID] = edge
	}

	// Merge properties
	for k, v := range evt.Props {
		edge.Props[k] = v
	}

	return nil
}

// WriteTriple writes an RDF triple to the in-memory graph.
func (mg *MemoryGraph) WriteTriple(evt *graph.TripleEvent) error {
	mg.mu.Lock()
	defer mg.mu.Unlock()

	if evt.Op == graph.OpDelete {
		// Remove matching triple
		for i, t := range mg.triples {
			if tripleMatches(t, evt) {
				mg.triples = append(mg.triples[:i], mg.triples[i+1:]...)
				break
			}
		}
		return nil
	}

	// Check for duplicate before adding
	for _, t := range mg.triples {
		if tripleMatches(t, evt) {
			return nil // Already exists
		}
	}

	mg.triples = append(mg.triples, MemoryTriple{
		Subject:   evt.Subject,
		Predicate: evt.Predicate,
		Object:    evt.Object,
		Graph:     evt.Graph,
	})

	return nil
}

// tripleMatches checks if a stored triple matches an event.
func tripleMatches(t MemoryTriple, evt *graph.TripleEvent) bool {
	return t.Subject.Kind == evt.Subject.Kind &&
		t.Subject.Value == evt.Subject.Value &&
		t.Predicate == evt.Predicate &&
		t.Object.Kind == evt.Object.Kind &&
		t.Object.Value == evt.Object.Value &&
		t.Graph == evt.Graph
}

// Flush is a no-op for the in-memory graph.
func (mg *MemoryGraph) Flush() error {
	return nil
}

// Close is a no-op for the in-memory graph.
func (mg *MemoryGraph) Close() error {
	return nil
}

// GetNode returns a node by ID.
func (mg *MemoryGraph) GetNode(id string) *MemoryNode {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	return mg.nodes[id]
}

// GetEdge returns an edge by ID.
func (mg *MemoryGraph) GetEdge(id string) *MemoryEdge {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	return mg.edges[id]
}

// AllNodes returns all nodes.
func (mg *MemoryGraph) AllNodes() []*MemoryNode {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	nodes := make([]*MemoryNode, 0, len(mg.nodes))
	for _, n := range mg.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// AllEdges returns all edges.
func (mg *MemoryGraph) AllEdges() []*MemoryEdge {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	edges := make([]*MemoryEdge, 0, len(mg.edges))
	for _, e := range mg.edges {
		edges = append(edges, e)
	}
	return edges
}

// AllTriples returns all triples.
func (mg *MemoryGraph) AllTriples() []MemoryTriple {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	triples := make([]MemoryTriple, len(mg.triples))
	copy(triples, mg.triples)
	return triples
}

// NodeCount returns the number of nodes.
func (mg *MemoryGraph) NodeCount() int {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	return len(mg.nodes)
}

// EdgeCount returns the number of edges.
func (mg *MemoryGraph) EdgeCount() int {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	return len(mg.edges)
}

// TripleCount returns the number of triples.
func (mg *MemoryGraph) TripleCount() int {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	return len(mg.triples)
}

// FindNodesByLabel returns all nodes with a given label.
func (mg *MemoryGraph) FindNodesByLabel(label string) []*MemoryNode {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	var result []*MemoryNode
	for _, n := range mg.nodes {
		for _, l := range n.Labels {
			if l == label {
				result = append(result, n)
				break
			}
		}
	}
	return result
}

// FindEdgesByLabel returns all edges with a given label.
func (mg *MemoryGraph) FindEdgesByLabel(label string) []*MemoryEdge {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	var result []*MemoryEdge
	for _, e := range mg.edges {
		if e.Label == label {
			result = append(result, e)
		}
	}
	return result
}

// OutgoingEdges returns edges starting from a node.
func (mg *MemoryGraph) OutgoingEdges(nodeID string) []*MemoryEdge {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	var result []*MemoryEdge
	for _, e := range mg.edges {
		if e.FromID == nodeID {
			result = append(result, e)
		}
	}
	return result
}

// IncomingEdges returns edges ending at a node.
func (mg *MemoryGraph) IncomingEdges(nodeID string) []*MemoryEdge {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	var result []*MemoryEdge
	for _, e := range mg.edges {
		if e.ToID == nodeID {
			result = append(result, e)
		}
	}
	return result
}

// Clear removes all data from the graph.
func (mg *MemoryGraph) Clear() {
	mg.mu.Lock()
	defer mg.mu.Unlock()
	mg.nodes = make(map[string]*MemoryNode)
	mg.edges = make(map[string]*MemoryEdge)
	mg.triples = mg.triples[:0]
}
