package algo

import (
	"math"
	"sort"
)

// GraphMetrics contains computed graph-level statistics.
type GraphMetrics struct {
	NumNodes        int64   // Total number of nodes
	NumEdges        int64   // Total number of edges
	Density         float64 // Edge density: E / (V*(V-1)) for directed
	AvgDegree       float64 // Average out-degree
	MaxDegree       int64   // Maximum out-degree
	MinDegree       int64   // Minimum out-degree
	MedianDegree    float64 // Median out-degree
	DegreeVariance  float64 // Variance of degree distribution
	ClusteringCoeff float64 // Global clustering coefficient
	NumIsolated     int64   // Nodes with no edges
	NumSources      int64   // Nodes with only outgoing edges
	NumSinks        int64   // Nodes with only incoming edges
}

// NodeAnomalyResult contains anomaly scores for individual nodes.
type NodeAnomalyResult struct {
	NodeID         int64   // Node identifier
	DegreeZScore   float64 // Z-score for out-degree
	InDegreeZScore float64 // Z-score for in-degree (if available)
	IsAnomaly      bool    // True if exceeds threshold
	AnomalyType    string  // Type of anomaly (high_degree, low_degree, isolated, hub)
}

// AnomalyResult contains the full anomaly detection results.
type AnomalyResult struct {
	Metrics    GraphMetrics        // Graph-level metrics
	NodeScores []NodeAnomalyResult // Per-node anomaly scores
	Anomalies  []NodeAnomalyResult // Filtered anomalous nodes
	Threshold  float64             // Z-score threshold used
}

// ComputeGraphMetrics calculates graph-level statistics.
//
// Time complexity: O(V + E)
// Space complexity: O(V)
func ComputeGraphMetrics(csr *CSR) *GraphMetrics {
	if csr == nil || csr.NumNodes == 0 {
		return &GraphMetrics{}
	}

	n := csr.NumNodes
	e := csr.NumEdges

	// Compute out-degrees
	outDeg := make([]int64, n)
	var sumDeg int64
	var maxDeg int64
	minDeg := int64(math.MaxInt64)
	var numIsolated int64

	for i := int64(0); i < n; i++ {
		deg := csr.IndPtr[i+1] - csr.IndPtr[i]
		outDeg[i] = deg
		sumDeg += deg

		if deg > maxDeg {
			maxDeg = deg
		}
		if deg < minDeg {
			minDeg = deg
		}
		if deg == 0 {
			numIsolated++
		}
	}

	avgDeg := float64(sumDeg) / float64(n)

	// Compute variance
	var variance float64
	for _, deg := range outDeg {
		diff := float64(deg) - avgDeg
		variance += diff * diff
	}
	variance /= float64(n)

	// Compute median
	sortedDeg := make([]int64, n)
	copy(sortedDeg, outDeg)
	sort.Slice(sortedDeg, func(i, j int) bool { return sortedDeg[i] < sortedDeg[j] })
	var medianDeg float64
	if n%2 == 0 {
		medianDeg = float64(sortedDeg[n/2-1]+sortedDeg[n/2]) / 2.0
	} else {
		medianDeg = float64(sortedDeg[n/2])
	}

	// Compute in-degrees for sources/sinks
	inDeg := csr.InDegree()
	var numSources, numSinks int64
	for i := int64(0); i < n; i++ {
		if outDeg[i] > 0 && inDeg[i] == 0 {
			numSources++
		}
		if outDeg[i] == 0 && inDeg[i] > 0 {
			numSinks++
		}
	}

	// Density: E / (V*(V-1)) for directed graphs
	var density float64
	if n > 1 {
		density = float64(e) / (float64(n) * float64(n-1))
	}

	// Global clustering coefficient
	clustering := computeGlobalClustering(csr)

	return &GraphMetrics{
		NumNodes:        n,
		NumEdges:        e,
		Density:         density,
		AvgDegree:       avgDeg,
		MaxDegree:       maxDeg,
		MinDegree:       minDeg,
		MedianDegree:    medianDeg,
		DegreeVariance:  variance,
		ClusteringCoeff: clustering,
		NumIsolated:     numIsolated,
		NumSources:      numSources,
		NumSinks:        numSinks,
	}
}

// computeGlobalClustering calculates the global clustering coefficient.
// C = 3 * triangles / connected_triplets
func computeGlobalClustering(csr *CSR) float64 {
	if csr == nil || csr.NumNodes == 0 {
		return 0
	}

	n := csr.NumNodes
	var triangles int64
	var triplets int64

	// Build neighbor sets for fast lookup
	neighborSets := make([]map[int64]struct{}, n)
	for i := int64(0); i < n; i++ {
		neighborSets[i] = make(map[int64]struct{})
		start := csr.IndPtr[i]
		end := csr.IndPtr[i+1]
		for idx := start; idx < end; idx++ {
			neighbor := csr.Indices[idx]
			if neighbor >= 0 && neighbor < n {
				neighborSets[i][neighbor] = struct{}{}
			}
		}
	}

	// Count triangles and triplets
	for i := int64(0); i < n; i++ {
		neighbors := neighborSets[i]
		deg := int64(len(neighbors))

		// Connected triplets centered on i
		if deg >= 2 {
			triplets += deg * (deg - 1) / 2
		}

		// Count triangles
		for j := range neighbors {
			if j <= i {
				continue // Avoid double counting
			}
			for k := range neighborSets[j] {
				if k <= j {
					continue
				}
				if _, exists := neighbors[k]; exists {
					triangles++
				}
			}
		}
	}

	if triplets == 0 {
		return 0
	}

	return float64(triangles) / float64(triplets)
}

// DetectAnomalies identifies anomalous nodes based on degree distribution.
//
// Parameters:
// - csr: The graph in CSR format
// - threshold: Z-score threshold for anomaly detection (default 2.0)
//
// A node is considered anomalous if its degree Z-score exceeds the threshold.
//
// Time complexity: O(V + E)
// Space complexity: O(V)
func DetectAnomalies(csr *CSR, threshold float64) *AnomalyResult {
	if csr == nil || csr.NumNodes == 0 {
		return &AnomalyResult{Threshold: threshold}
	}

	if threshold <= 0 {
		threshold = 2.0
	}

	n := csr.NumNodes
	metrics := ComputeGraphMetrics(csr)

	// Compute Z-scores for out-degree
	stdDev := math.Sqrt(metrics.DegreeVariance)

	// Compute in-degree stats
	inDeg := csr.InDegree()
	var sumInDeg int64
	for _, d := range inDeg {
		sumInDeg += d
	}
	avgInDeg := float64(sumInDeg) / float64(n)

	var inDegVariance float64
	for _, d := range inDeg {
		diff := float64(d) - avgInDeg
		inDegVariance += diff * diff
	}
	inDegVariance /= float64(n)
	inDegStdDev := math.Sqrt(inDegVariance)

	nodeScores := make([]NodeAnomalyResult, n)
	anomalies := make([]NodeAnomalyResult, 0)

	for i := int64(0); i < n; i++ {
		outDeg := float64(csr.IndPtr[i+1] - csr.IndPtr[i])

		// Calculate Z-scores
		var outZScore float64
		if stdDev > 0 {
			outZScore = (outDeg - metrics.AvgDegree) / stdDev
		}

		var inZScore float64
		if inDegStdDev > 0 {
			inZScore = (float64(inDeg[i]) - avgInDeg) / inDegStdDev
		}

		// Determine anomaly type
		isAnomaly := false
		anomalyType := ""

		if outDeg == 0 && inDeg[i] == 0 {
			isAnomaly = true
			anomalyType = "isolated"
		} else if math.Abs(outZScore) > threshold {
			isAnomaly = true
			if outZScore > 0 {
				anomalyType = "high_out_degree"
			} else {
				anomalyType = "low_out_degree"
			}
		} else if math.Abs(inZScore) > threshold {
			isAnomaly = true
			if inZScore > 0 {
				anomalyType = "high_in_degree"
			} else {
				anomalyType = "low_in_degree"
			}
		}

		// Hub detection: high both in and out degree
		if outZScore > threshold && inZScore > threshold {
			anomalyType = "hub"
		}

		nodeScore := NodeAnomalyResult{
			NodeID:         i,
			DegreeZScore:   outZScore,
			InDegreeZScore: inZScore,
			IsAnomaly:      isAnomaly,
			AnomalyType:    anomalyType,
		}
		nodeScores[i] = nodeScore

		if isAnomaly {
			anomalies = append(anomalies, nodeScore)
		}
	}

	return &AnomalyResult{
		Metrics:    *metrics,
		NodeScores: nodeScores,
		Anomalies:  anomalies,
		Threshold:  threshold,
	}
}

// DetectDegreeAnomalies is a simpler version that only checks out-degree.
func DetectDegreeAnomalies(csr *CSR, threshold float64) []int64 {
	result := DetectAnomalies(csr, threshold)
	anomalousNodes := make([]int64, 0, len(result.Anomalies))
	for _, a := range result.Anomalies {
		anomalousNodes = append(anomalousNodes, a.NodeID)
	}
	return anomalousNodes
}

// DegreeHistogram computes a histogram of node degrees.
func DegreeHistogram(csr *CSR, numBins int) ([]int64, []int) {
	if csr == nil || csr.NumNodes == 0 {
		return nil, nil
	}

	if numBins <= 0 {
		numBins = 10
	}

	n := csr.NumNodes

	// Find min and max degree
	var maxDeg int64
	for i := int64(0); i < n; i++ {
		deg := csr.IndPtr[i+1] - csr.IndPtr[i]
		if deg > maxDeg {
			maxDeg = deg
		}
	}

	if maxDeg == 0 {
		return []int64{0}, []int{int(n)}
	}

	// Create bins
	binWidth := float64(maxDeg+1) / float64(numBins)
	bins := make([]int64, numBins)
	counts := make([]int, numBins)

	for i := 0; i < numBins; i++ {
		bins[i] = int64(float64(i) * binWidth)
	}

	// Count
	for i := int64(0); i < n; i++ {
		deg := csr.IndPtr[i+1] - csr.IndPtr[i]
		binIdx := int(float64(deg) / binWidth)
		if binIdx >= numBins {
			binIdx = numBins - 1
		}
		counts[binIdx]++
	}

	return bins, counts
}

// PowerLawExponent estimates the power-law exponent of the degree distribution.
// Uses maximum likelihood estimation: α = 1 + n / Σ ln(k_i / k_min)
func PowerLawExponent(csr *CSR) float64 {
	if csr == nil || csr.NumNodes == 0 {
		return 0
	}

	n := csr.NumNodes

	// Find minimum non-zero degree
	kMin := int64(math.MaxInt64)
	for i := int64(0); i < n; i++ {
		deg := csr.IndPtr[i+1] - csr.IndPtr[i]
		if deg > 0 && deg < kMin {
			kMin = deg
		}
	}

	if kMin == int64(math.MaxInt64) || kMin == 0 {
		return 0
	}

	// MLE for power-law exponent
	var sumLog float64
	var count int64
	for i := int64(0); i < n; i++ {
		deg := csr.IndPtr[i+1] - csr.IndPtr[i]
		if deg >= kMin {
			sumLog += math.Log(float64(deg) / float64(kMin))
			count++
		}
	}

	if sumLog == 0 {
		return 0
	}

	return 1.0 + float64(count)/sumLog
}

// LocalClusteringCoefficient computes the local clustering coefficient for a node.
// C_i = 2 * triangles_i / (k_i * (k_i - 1))
func LocalClusteringCoefficient(csr *CSR, node int64) float64 {
	if csr == nil || node < 0 || node >= csr.NumNodes {
		return 0
	}

	// Get neighbors
	start := csr.IndPtr[node]
	end := csr.IndPtr[node+1]
	deg := end - start

	if deg < 2 {
		return 0
	}

	// Build neighbor set
	neighbors := make(map[int64]struct{})
	for idx := start; idx < end; idx++ {
		neighbor := csr.Indices[idx]
		if neighbor >= 0 && neighbor < csr.NumNodes {
			neighbors[neighbor] = struct{}{}
		}
	}

	// Count edges between neighbors (triangles)
	var triangles int64
	for neighbor := range neighbors {
		nStart := csr.IndPtr[neighbor]
		nEnd := csr.IndPtr[neighbor+1]
		for idx := nStart; idx < nEnd; idx++ {
			if _, exists := neighbors[csr.Indices[idx]]; exists {
				triangles++
			}
		}
	}

	// Each triangle is counted twice
	triangles /= 2

	return float64(2*triangles) / float64(deg*(deg-1))
}

// AllLocalClustering computes local clustering coefficients for all nodes.
func AllLocalClustering(csr *CSR) []float64 {
	if csr == nil {
		return nil
	}

	n := csr.NumNodes
	coeffs := make([]float64, n)

	for i := int64(0); i < n; i++ {
		coeffs[i] = LocalClusteringCoefficient(csr, i)
	}

	return coeffs
}

// FindHubs returns nodes with degree significantly above average.
// Hubs are nodes where degree > avgDegree + k*stdDev
func FindHubs(csr *CSR, k float64) []int64 {
	if csr == nil || csr.NumNodes == 0 {
		return nil
	}

	if k <= 0 {
		k = 2.0
	}

	metrics := ComputeGraphMetrics(csr)
	threshold := metrics.AvgDegree + k*math.Sqrt(metrics.DegreeVariance)

	hubs := make([]int64, 0)
	for i := int64(0); i < csr.NumNodes; i++ {
		deg := float64(csr.IndPtr[i+1] - csr.IndPtr[i])
		if deg > threshold {
			hubs = append(hubs, i)
		}
	}

	return hubs
}

// FindPeripheral returns nodes with very low connectivity.
func FindPeripheral(csr *CSR, maxDegree int64) []int64 {
	if csr == nil || csr.NumNodes == 0 {
		return nil
	}

	if maxDegree < 0 {
		maxDegree = 1
	}

	peripheral := make([]int64, 0)
	for i := int64(0); i < csr.NumNodes; i++ {
		deg := csr.IndPtr[i+1] - csr.IndPtr[i]
		if deg <= maxDegree {
			peripheral = append(peripheral, i)
		}
	}

	return peripheral
}
