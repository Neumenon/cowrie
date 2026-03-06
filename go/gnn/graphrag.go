//go:build agentgo

// Package gnn provides GraphRAG: hybrid retrieval combining vector similarity
// with graph-based importance (PageRank, community structure).
//
// GraphRAG improves upon pure vector search by incorporating structural
// knowledge about how documents/entities relate to each other in a graph.
//
// Typical use cases:
//   - Document retrieval with citation importance
//   - Agent memory with communication pattern weighting
//   - Knowledge graph queries with entity centrality

package gnn

import (
	"context"
	"math"
	"sort"

	"github.com/Neumenon/cowrie/go"
	"github.com/Neumenon/cowrie/go/gnn/algo"
	"github.com/phenomenon0/Agent-GO/vectordb/client"
	"github.com/phenomenon0/Agent-GO/vectordb/hybrid"
)

// FusionStrategy defines how vector and graph scores are combined.
type FusionStrategy int

const (
	// FusionAlpha uses simple alpha blending: final = alpha*vector + (1-alpha)*graph
	FusionAlpha FusionStrategy = iota

	// FusionRRF uses Reciprocal Rank Fusion (parameter-free, robust)
	FusionRRF

	// FusionWeighted uses weighted combination with normalization
	FusionWeighted
)

// GraphRAGConfig configures the hybrid retrieval.
type GraphRAGConfig struct {
	// Alpha controls the blend: final = alpha*vector + (1-alpha)*graph
	// Default 0.6 (60% vector, 40% graph)
	// Only used when FusionStrategy is FusionAlpha
	Alpha float32

	// FusionStrategy determines how vector and graph scores are combined
	// Default: FusionAlpha (backward compatible)
	FusionStrategy FusionStrategy

	// UsePersonalized enables query-specific PageRank
	// When true, seeds PageRank from query matches
	UsePersonalized bool

	// PageRankConfig for importance computation
	PageRank algo.PageRankConfig

	// MinVectorScore filters results below this threshold
	MinVectorScore float32

	// MinGraphScore filters results below this threshold (after normalization)
	MinGraphScore float32

	// BoostCommunity adds bonus for same-community matches
	// 0 = disabled, 1.0 = 100% boost for same community
	BoostCommunity float32

	// VectorWeight for FusionWeighted strategy (default 0.7)
	VectorWeight float32

	// GraphWeight for FusionWeighted strategy (default 0.3)
	GraphWeight float32
}

// DefaultGraphRAGConfig returns sensible defaults.
var DefaultGraphRAGConfig = GraphRAGConfig{
	Alpha:           0.6,
	FusionStrategy:  FusionAlpha, // Backward compatible
	UsePersonalized: false,
	PageRank:        algo.DefaultPageRankConfig,
	MinVectorScore:  0.0,
	MinGraphScore:   0.0,
	BoostCommunity:  0.2, // 20% boost for same community
	VectorWeight:    0.7,
	GraphWeight:     0.3,
}

// RRFGraphRAGConfig returns config optimized for RRF fusion.
var RRFGraphRAGConfig = GraphRAGConfig{
	FusionStrategy:  FusionRRF,
	UsePersonalized: false,
	PageRank:        algo.DefaultPageRankConfig,
	MinVectorScore:  0.0,
	MinGraphScore:   0.0,
	BoostCommunity:  0.2,
	VectorWeight:    0.7,
	GraphWeight:     0.3,
}

// GraphRAGResult is a single result with hybrid scoring.
type GraphRAGResult struct {
	ID          string  // Document/node ID
	Doc         string  // Document content (if available)
	VectorScore float32 // Raw vector similarity score
	GraphScore  float32 // Normalized PageRank score
	FinalScore  float32 // Blended final score
	CommunityID int64   // Community ID (-1 if unknown)
	NodeID      int64   // Graph node ID (-1 if not in graph)
}

// GraphRAG provides hybrid vector+graph retrieval.
type GraphRAG struct {
	vectorClient *client.Client
	vectorBridge *client.TensorRefBridge

	// Graph structure
	csr         *algo.CSR
	idToNode    map[string]int64 // Document ID → graph node
	nodeToID    map[int64]string // Graph node → document ID
	pagerank    *algo.PageRankResult
	communities []int64 // Node → community ID

	// Edge weights for weighted PageRank
	weights []float32

	cfg GraphRAGConfig
}

// NewGraphRAG creates a hybrid retrieval system.
func NewGraphRAG(vectorClient *client.Client, cfg GraphRAGConfig) *GraphRAG {
	g := &GraphRAG{
		vectorClient: vectorClient,
		idToNode:     make(map[string]int64),
		nodeToID:     make(map[int64]string),
		cfg:          cfg,
	}
	if vectorClient != nil {
		g.vectorBridge = client.NewTensorRefBridge(vectorClient)
	}
	return g
}

// SetGraph configures the graph structure.
// Call this after loading/building your document graph.
func (g *GraphRAG) SetGraph(csr *algo.CSR, idToNode map[string]int64) {
	g.csr = csr
	g.idToNode = idToNode

	// Build reverse mapping
	g.nodeToID = make(map[int64]string, len(idToNode))
	for id, node := range idToNode {
		g.nodeToID[node] = id
	}

	// Precompute PageRank
	if csr != nil && csr.NumNodes > 0 {
		g.pagerank = algo.PageRank(csr, g.cfg.PageRank)
	}
}

// SetWeights sets edge weights for weighted graph analysis.
func (g *GraphRAG) SetWeights(weights []float32) {
	g.weights = weights
}

// SetCommunities sets community assignments for community boosting.
func (g *GraphRAG) SetCommunities(communities []int64) {
	g.communities = communities
}

// ComputeCommunities uses Louvain to detect communities.
func (g *GraphRAG) ComputeCommunities() {
	if g.csr == nil || g.csr.NumNodes == 0 {
		return
	}
	result := algo.LouvainUnweighted(g.csr, algo.DefaultLouvainConfig)
	g.communities = result.Communities
}

// Query performs hybrid retrieval.
func (g *GraphRAG) Query(ctx context.Context, query string, topK int) ([]GraphRAGResult, error) {
	// Step 1: Vector search
	vectorResults, vectorScores, err := g.vectorSearch(ctx, query, topK*2) // Over-fetch for re-ranking
	if err != nil {
		return nil, err
	}

	if len(vectorResults) == 0 {
		return []GraphRAGResult{}, nil
	}

	// Step 2: Compute graph scores
	graphScores := g.computeGraphScores(vectorResults, query)

	// Step 3: Blend scores
	results := g.blendResults(vectorResults, vectorScores, graphScores, topK)

	return results, nil
}

// QueryWithContext performs hybrid retrieval with context nodes.
// Context nodes seed the personalized PageRank for better relevance.
func (g *GraphRAG) QueryWithContext(ctx context.Context, query string, contextIDs []string, topK int) ([]GraphRAGResult, error) {
	// Convert context IDs to node IDs for personalized PageRank
	var contextNodes []int64
	for _, id := range contextIDs {
		if node, ok := g.idToNode[id]; ok {
			contextNodes = append(contextNodes, node)
		}
	}

	// Step 1: Vector search
	vectorResults, vectorScores, err := g.vectorSearch(ctx, query, topK*2)
	if err != nil {
		return nil, err
	}

	if len(vectorResults) == 0 {
		return []GraphRAGResult{}, nil
	}

	// Step 2: Compute personalized graph scores
	graphScores := g.computePersonalizedGraphScores(vectorResults, contextNodes)

	// Step 3: Blend scores
	results := g.blendResults(vectorResults, vectorScores, graphScores, topK)

	return results, nil
}

// vectorSearch performs the vector similarity search.
func (g *GraphRAG) vectorSearch(ctx context.Context, query string, topK int) ([]string, []float32, error) {
	if g.vectorClient == nil {
		return nil, nil, nil
	}

	req := client.QueryRequest{
		Query: query,
		TopK:  topK,
	}

	resp, err := g.vectorClient.Query(ctx, req)
	if err != nil {
		return nil, nil, err
	}

	return resp.IDs, resp.Scores, nil
}

// computeGraphScores computes PageRank-based importance for results.
func (g *GraphRAG) computeGraphScores(docIDs []string, query string) []float32 {
	scores := make([]float32, len(docIDs))

	if g.pagerank == nil || len(g.pagerank.Scores) == 0 {
		// No graph - return uniform scores
		for i := range scores {
			scores[i] = 1.0 / float32(len(docIDs))
		}
		return scores
	}

	// Get PageRank scores for each document
	var maxScore float32
	for i, id := range docIDs {
		if node, ok := g.idToNode[id]; ok && node >= 0 && node < int64(len(g.pagerank.Scores)) {
			scores[i] = g.pagerank.Scores[node]
			if scores[i] > maxScore {
				maxScore = scores[i]
			}
		}
	}

	// Normalize to [0, 1]
	if maxScore > 0 {
		for i := range scores {
			scores[i] /= maxScore
		}
	}

	return scores
}

// computePersonalizedGraphScores uses personalized PageRank.
func (g *GraphRAG) computePersonalizedGraphScores(docIDs []string, seedNodes []int64) []float32 {
	scores := make([]float32, len(docIDs))

	if g.csr == nil || g.csr.NumNodes == 0 {
		for i := range scores {
			scores[i] = 1.0 / float32(len(docIDs))
		}
		return scores
	}

	// Compute personalized PageRank from seed nodes
	ppr := algo.PersonalizedPageRank(g.csr, g.cfg.PageRank, seedNodes)
	if ppr == nil || len(ppr.Scores) == 0 {
		return g.computeGraphScores(docIDs, "")
	}

	// Get scores for each document
	var maxScore float32
	for i, id := range docIDs {
		if node, ok := g.idToNode[id]; ok && node >= 0 && node < int64(len(ppr.Scores)) {
			scores[i] = ppr.Scores[node]
			if scores[i] > maxScore {
				maxScore = scores[i]
			}
		}
	}

	// Normalize to [0, 1]
	if maxScore > 0 {
		for i := range scores {
			scores[i] /= maxScore
		}
	}

	return scores
}

// blendResults combines vector and graph scores using the configured fusion strategy.
func (g *GraphRAG) blendResults(docIDs []string, vectorScores, graphScores []float32, topK int) []GraphRAGResult {
	// Route to appropriate fusion method based on strategy
	switch g.cfg.FusionStrategy {
	case FusionRRF:
		return g.blendResultsRRF(docIDs, vectorScores, graphScores, topK)
	case FusionWeighted:
		return g.blendResultsWeighted(docIDs, vectorScores, graphScores, topK)
	default:
		return g.blendResultsAlpha(docIDs, vectorScores, graphScores, topK)
	}
}

// blendResultsAlpha uses simple alpha blending (original algorithm, backward compatible)
func (g *GraphRAG) blendResultsAlpha(docIDs []string, vectorScores, graphScores []float32, topK int) []GraphRAGResult {
	results := make([]GraphRAGResult, 0, len(docIDs))

	alpha := g.cfg.Alpha
	if alpha < 0 {
		alpha = 0
	}
	if alpha > 1 {
		alpha = 1
	}

	for i, id := range docIDs {
		vs := float32(0)
		if i < len(vectorScores) {
			vs = vectorScores[i]
		}
		gs := float32(0)
		if i < len(graphScores) {
			gs = graphScores[i]
		}

		// Apply minimum thresholds
		if vs < g.cfg.MinVectorScore || gs < g.cfg.MinGraphScore {
			continue
		}

		// Blend scores
		finalScore := alpha*vs + (1-alpha)*gs

		// Community boost
		nodeID := int64(-1)
		communityID := int64(-1)
		if node, ok := g.idToNode[id]; ok {
			nodeID = node
			if g.communities != nil && node >= 0 && node < int64(len(g.communities)) {
				communityID = g.communities[node]
			}
		}

		// Apply community boost if we have context
		if g.cfg.BoostCommunity > 0 && communityID >= 0 {
			finalScore *= (1 + g.cfg.BoostCommunity)
		}

		results = append(results, GraphRAGResult{
			ID:          id,
			VectorScore: vs,
			GraphScore:  gs,
			FinalScore:  finalScore,
			CommunityID: communityID,
			NodeID:      nodeID,
		})
	}

	// Sort by final score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].FinalScore > results[j].FinalScore
	})

	// Apply community boost based on top result's community
	if len(results) > 0 && g.cfg.BoostCommunity > 0 {
		topCommunity := results[0].CommunityID
		if topCommunity >= 0 {
			for i := 1; i < len(results); i++ {
				if results[i].CommunityID == topCommunity {
					// Already boosted in initial calculation
				} else {
					// Remove boost for non-matching communities
					results[i].FinalScore /= (1 + g.cfg.BoostCommunity)
				}
			}
			// Re-sort after adjustment
			sort.Slice(results, func(i, j int) bool {
				return results[i].FinalScore > results[j].FinalScore
			})
		}
	}

	// Limit to topK
	if len(results) > topK {
		results = results[:topK]
	}

	return results
}

// blendResultsRRF uses Reciprocal Rank Fusion from the hybrid package.
// RRF is parameter-free and works well for combining heterogeneous score sources.
func (g *GraphRAG) blendResultsRRF(docIDs []string, vectorScores, graphScores []float32, topK int) []GraphRAGResult {
	if len(docIDs) == 0 {
		return []GraphRAGResult{}
	}

	// Build result lookup for community/node info
	resultInfo := g.buildResultInfo(docIDs, vectorScores, graphScores)

	// Create ranked lists for vector and graph scores
	vectorResults := make([]hybrid.SearchResult, 0, len(docIDs))
	graphResults := make([]hybrid.SearchResult, 0, len(docIDs))

	for i, id := range docIDs {
		vs := float32(0)
		gs := float32(0)
		if i < len(vectorScores) {
			vs = vectorScores[i]
		}
		if i < len(graphScores) {
			gs = graphScores[i]
		}

		// Apply minimum thresholds
		if vs < g.cfg.MinVectorScore || gs < g.cfg.MinGraphScore {
			continue
		}

		docID := hashDocID(id)
		vectorResults = append(vectorResults, hybrid.SearchResult{DocID: docID, Score: vs})
		graphResults = append(graphResults, hybrid.SearchResult{DocID: docID, Score: gs})
	}

	// Sort by score to establish proper ranks for RRF
	sort.Slice(vectorResults, func(i, j int) bool {
		return vectorResults[i].Score > vectorResults[j].Score
	})
	sort.Slice(graphResults, func(i, j int) bool {
		return graphResults[i].Score > graphResults[j].Score
	})

	// Apply RRF fusion
	params := hybrid.DefaultFusionParams()
	params.Strategy = hybrid.FusionRRF
	fusedResults, err := hybrid.HybridSearch(vectorResults, graphResults, params, topK*2)
	if err != nil || len(fusedResults) == 0 {
		// Fall back to alpha blending on error
		return g.blendResultsAlpha(docIDs, vectorScores, graphScores, topK)
	}

	// Convert back to GraphRAGResult with community boost
	results := g.convertFusedResults(fusedResults, resultInfo, topK)
	return results
}

// blendResultsWeighted uses weighted fusion from the hybrid package.
func (g *GraphRAG) blendResultsWeighted(docIDs []string, vectorScores, graphScores []float32, topK int) []GraphRAGResult {
	if len(docIDs) == 0 {
		return []GraphRAGResult{}
	}

	// Build result lookup for community/node info
	resultInfo := g.buildResultInfo(docIDs, vectorScores, graphScores)

	// Create result sets with scores
	vectorResults := make([]hybrid.SearchResult, 0, len(docIDs))
	graphResults := make([]hybrid.SearchResult, 0, len(docIDs))

	for i, id := range docIDs {
		vs := float32(0)
		gs := float32(0)
		if i < len(vectorScores) {
			vs = vectorScores[i]
		}
		if i < len(graphScores) {
			gs = graphScores[i]
		}

		// Apply minimum thresholds
		if vs < g.cfg.MinVectorScore || gs < g.cfg.MinGraphScore {
			continue
		}

		docID := hashDocID(id)
		vectorResults = append(vectorResults, hybrid.SearchResult{DocID: docID, Score: vs})
		graphResults = append(graphResults, hybrid.SearchResult{DocID: docID, Score: gs})
	}

	// Apply weighted fusion
	params := hybrid.FusionParams{
		Strategy:     hybrid.FusionWeighted,
		DenseWeight:  g.cfg.VectorWeight,
		SparseWeight: g.cfg.GraphWeight,
	}
	fusedResults, err := hybrid.HybridSearch(vectorResults, graphResults, params, topK*2)
	if err != nil || len(fusedResults) == 0 {
		// Fall back to alpha blending on error
		return g.blendResultsAlpha(docIDs, vectorScores, graphScores, topK)
	}

	// Convert back to GraphRAGResult with community boost
	results := g.convertFusedResults(fusedResults, resultInfo, topK)
	return results
}

// resultInfoEntry stores info needed to reconstruct GraphRAGResult
type resultInfoEntry struct {
	ID          string
	VectorScore float32
	GraphScore  float32
	CommunityID int64
	NodeID      int64
}

// buildResultInfo creates a lookup map for result metadata
func (g *GraphRAG) buildResultInfo(docIDs []string, vectorScores, graphScores []float32) map[uint64]resultInfoEntry {
	info := make(map[uint64]resultInfoEntry, len(docIDs))
	for i, id := range docIDs {
		vs := float32(0)
		gs := float32(0)
		if i < len(vectorScores) {
			vs = vectorScores[i]
		}
		if i < len(graphScores) {
			gs = graphScores[i]
		}

		nodeID := int64(-1)
		communityID := int64(-1)
		if node, ok := g.idToNode[id]; ok {
			nodeID = node
			if g.communities != nil && node >= 0 && node < int64(len(g.communities)) {
				communityID = g.communities[node]
			}
		}

		info[hashDocID(id)] = resultInfoEntry{
			ID:          id,
			VectorScore: vs,
			GraphScore:  gs,
			CommunityID: communityID,
			NodeID:      nodeID,
		}
	}
	return info
}

// convertFusedResults converts hybrid.SearchResult back to GraphRAGResult
func (g *GraphRAG) convertFusedResults(fusedResults []hybrid.SearchResult, info map[uint64]resultInfoEntry, topK int) []GraphRAGResult {
	results := make([]GraphRAGResult, 0, len(fusedResults))

	for _, fr := range fusedResults {
		entry, ok := info[fr.DocID]
		if !ok {
			continue
		}

		finalScore := fr.Score

		// Apply community boost
		if g.cfg.BoostCommunity > 0 && entry.CommunityID >= 0 {
			finalScore *= (1 + g.cfg.BoostCommunity)
		}

		results = append(results, GraphRAGResult{
			ID:          entry.ID,
			VectorScore: entry.VectorScore,
			GraphScore:  entry.GraphScore,
			FinalScore:  finalScore,
			CommunityID: entry.CommunityID,
			NodeID:      entry.NodeID,
		})
	}

	// Re-sort after community boost
	sort.Slice(results, func(i, j int) bool {
		return results[i].FinalScore > results[j].FinalScore
	})

	// Apply community boost adjustment based on top result
	if len(results) > 0 && g.cfg.BoostCommunity > 0 {
		topCommunity := results[0].CommunityID
		if topCommunity >= 0 {
			for i := 1; i < len(results); i++ {
				if results[i].CommunityID != topCommunity {
					results[i].FinalScore /= (1 + g.cfg.BoostCommunity)
				}
			}
			sort.Slice(results, func(i, j int) bool {
				return results[i].FinalScore > results[j].FinalScore
			})
		}
	}

	// Limit to topK
	if len(results) > topK {
		results = results[:topK]
	}

	return results
}

// hashDocID creates a uint64 hash from a string ID for hybrid.SearchResult
func hashDocID(id string) uint64 {
	h := uint64(14695981039346656037) // FNV-1a offset basis
	for i := 0; i < len(id); i++ {
		h ^= uint64(id[i])
		h *= 1099511628211 // FNV-1a prime
	}
	return h
}

// AddDocument adds a document to both vector store and graph.
func (g *GraphRAG) AddDocument(ctx context.Context, id, content string, meta map[string]string) (*cowrie.Value, error) {
	if g.vectorBridge == nil {
		return nil, nil
	}
	return g.vectorBridge.Store(ctx, content, meta, id)
}

// AddEdge adds an edge between documents (for graph construction).
// This should be called before SetGraph to build the graph structure.
func (g *GraphRAG) AddEdge(fromID, toID string, weight float32) {
	// Implementation note: edges are collected and then SetGraph is called
	// with the constructed CSR. See GraphBuilder in scheduler/ for full example.
}

// GetImportance returns the PageRank importance of a document.
func (g *GraphRAG) GetImportance(id string) float32 {
	if g.pagerank == nil {
		return 0
	}
	node, ok := g.idToNode[id]
	if !ok || node < 0 || node >= int64(len(g.pagerank.Scores)) {
		return 0
	}
	return g.pagerank.Scores[node]
}

// GetCommunity returns the community ID of a document.
func (g *GraphRAG) GetCommunity(id string) int64 {
	if g.communities == nil {
		return -1
	}
	node, ok := g.idToNode[id]
	if !ok || node < 0 || node >= int64(len(g.communities)) {
		return -1
	}
	return g.communities[node]
}

// GetNeighbors returns documents connected to the given document.
func (g *GraphRAG) GetNeighbors(id string) []string {
	if g.csr == nil {
		return nil
	}
	node, ok := g.idToNode[id]
	if !ok || node < 0 || node >= g.csr.NumNodes {
		return nil
	}

	neighbors := g.csr.Neighbors(node)
	result := make([]string, 0, len(neighbors))
	for _, n := range neighbors {
		if docID, ok := g.nodeToID[n]; ok {
			result = append(result, docID)
		}
	}
	return result
}

// Stats returns statistics about the graph.
func (g *GraphRAG) Stats() GraphRAGStats {
	stats := GraphRAGStats{}
	if g.csr != nil {
		stats.NumNodes = int(g.csr.NumNodes)
		stats.NumEdges = int(g.csr.NumEdges)
	}
	stats.NumDocuments = len(g.idToNode)
	if g.pagerank != nil {
		stats.PageRankConverged = g.pagerank.Converged
		stats.PageRankIterations = g.pagerank.Iterations
	}
	if g.communities != nil {
		// Count unique communities
		seen := make(map[int64]bool)
		for _, c := range g.communities {
			seen[c] = true
		}
		stats.NumCommunities = len(seen)
	}
	return stats
}

// GraphRAGStats contains statistics about the GraphRAG system.
type GraphRAGStats struct {
	NumNodes           int
	NumEdges           int
	NumDocuments       int
	NumCommunities     int
	PageRankConverged  bool
	PageRankIterations int
}

// ReRank re-ranks existing results using graph information.
// Useful when you have vector results from another source.
func (g *GraphRAG) ReRank(docIDs []string, vectorScores []float32, topK int) []GraphRAGResult {
	graphScores := g.computeGraphScores(docIDs, "")
	return g.blendResults(docIDs, vectorScores, graphScores, topK)
}

// ExpandQuery adds related documents to a query result.
// Uses graph neighbors to find potentially relevant documents.
func (g *GraphRAG) ExpandQuery(results []GraphRAGResult, maxExpansion int) []GraphRAGResult {
	if g.csr == nil || maxExpansion <= 0 {
		return results
	}

	// Collect existing IDs
	existing := make(map[string]bool, len(results))
	for _, r := range results {
		existing[r.ID] = true
	}

	// Expand from top results
	expanded := make([]GraphRAGResult, len(results))
	copy(expanded, results)

	added := 0
	for _, r := range results {
		if added >= maxExpansion {
			break
		}
		neighbors := g.GetNeighbors(r.ID)
		for _, nID := range neighbors {
			if added >= maxExpansion {
				break
			}
			if existing[nID] {
				continue
			}
			existing[nID] = true

			// Add neighbor with reduced score (as it's expanded, not directly matched)
			expanded = append(expanded, GraphRAGResult{
				ID:          nID,
				VectorScore: r.VectorScore * 0.5, // Decay
				GraphScore:  g.GetImportance(nID),
				FinalScore:  r.FinalScore * 0.7,
				CommunityID: g.GetCommunity(nID),
				NodeID:      g.idToNode[nID],
			})
			added++
		}
	}

	return expanded
}

// DiverseTopK returns diverse results by limiting per-community count.
func (g *GraphRAG) DiverseTopK(results []GraphRAGResult, topK, maxPerCommunity int) []GraphRAGResult {
	if g.communities == nil || maxPerCommunity <= 0 {
		if len(results) > topK {
			return results[:topK]
		}
		return results
	}

	communityCount := make(map[int64]int)
	diverse := make([]GraphRAGResult, 0, topK)

	for _, r := range results {
		if len(diverse) >= topK {
			break
		}
		comm := r.CommunityID
		if comm >= 0 && communityCount[comm] >= maxPerCommunity {
			continue // Skip over-represented community
		}
		diverse = append(diverse, r)
		if comm >= 0 {
			communityCount[comm]++
		}
	}

	return diverse
}

// NormalizeScores normalizes final scores to [0, 1] range.
func NormalizeScores(results []GraphRAGResult) {
	if len(results) == 0 {
		return
	}

	minScore := float32(math.MaxFloat32)
	maxScore := float32(-math.MaxFloat32)

	for _, r := range results {
		if r.FinalScore < minScore {
			minScore = r.FinalScore
		}
		if r.FinalScore > maxScore {
			maxScore = r.FinalScore
		}
	}

	rang := maxScore - minScore
	if rang > 0 {
		for i := range results {
			results[i].FinalScore = (results[i].FinalScore - minScore) / rang
		}
	} else {
		for i := range results {
			results[i].FinalScore = 1.0
		}
	}
}
