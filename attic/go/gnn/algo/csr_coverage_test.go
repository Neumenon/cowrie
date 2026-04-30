package algo

import (
	"encoding/binary"
	"testing"
)

func TestCSR_Neighbors(t *testing.T) {
	// Graph: 0->1, 0->2, 1->2
	csr := NewCSR(3, []int64{0, 2, 3, 3}, []int64{1, 2, 2})

	n := csr.Neighbors(0)
	if len(n) != 2 || n[0] != 1 || n[1] != 2 {
		t.Errorf("Neighbors(0): got %v", n)
	}

	n = csr.Neighbors(1)
	if len(n) != 1 || n[0] != 2 {
		t.Errorf("Neighbors(1): got %v", n)
	}

	n = csr.Neighbors(2)
	if len(n) != 0 {
		t.Errorf("Neighbors(2): expected empty, got %v", n)
	}

	// Out of bounds
	n = csr.Neighbors(-1)
	if n != nil {
		t.Error("Neighbors(-1) should return nil")
	}
	n = csr.Neighbors(5)
	if n != nil {
		t.Error("Neighbors(5) should return nil")
	}
}

func TestCSR_OutDegree(t *testing.T) {
	csr := NewCSR(3, []int64{0, 2, 3, 3}, []int64{1, 2, 2})

	if csr.OutDegree(0) != 2 {
		t.Errorf("OutDegree(0): got %d", csr.OutDegree(0))
	}
	if csr.OutDegree(1) != 1 {
		t.Errorf("OutDegree(1): got %d", csr.OutDegree(1))
	}
	if csr.OutDegree(2) != 0 {
		t.Errorf("OutDegree(2): got %d", csr.OutDegree(2))
	}
	if csr.OutDegree(-1) != 0 {
		t.Error("OutDegree(-1) should be 0")
	}
	if csr.OutDegree(10) != 0 {
		t.Error("OutDegree(10) should be 0")
	}
}

func TestCSR_OutDegrees(t *testing.T) {
	csr := NewCSR(3, []int64{0, 2, 3, 3}, []int64{1, 2, 2})
	degrees := csr.OutDegrees()
	if len(degrees) != 3 {
		t.Fatalf("expected 3 degrees, got %d", len(degrees))
	}
	if degrees[0] != 2 || degrees[1] != 1 || degrees[2] != 0 {
		t.Errorf("OutDegrees: got %v", degrees)
	}
}

func TestCSR_HasEdge(t *testing.T) {
	csr := NewCSR(3, []int64{0, 2, 3, 3}, []int64{1, 2, 2})

	if !csr.HasEdge(0, 1) {
		t.Error("should have edge 0->1")
	}
	if !csr.HasEdge(0, 2) {
		t.Error("should have edge 0->2")
	}
	if !csr.HasEdge(1, 2) {
		t.Error("should have edge 1->2")
	}
	if csr.HasEdge(2, 0) {
		t.Error("should not have edge 2->0")
	}
	if csr.HasEdge(-1, 0) {
		t.Error("should not have edge from -1")
	}
	if csr.HasEdge(10, 0) {
		t.Error("should not have edge from 10")
	}
}

func TestCSR_Transpose(t *testing.T) {
	// 0->1, 0->2, 1->2
	csr := NewCSR(3, []int64{0, 2, 3, 3}, []int64{1, 2, 2})
	trans := csr.Transpose()

	// In transposed: 1->0, 2->0, 2->1
	if trans.NumNodes != 3 {
		t.Error("transposed NumNodes")
	}

	// Node 0: no incoming in original -> no outgoing in transpose
	if trans.OutDegree(0) != 0 {
		t.Errorf("transposed OutDegree(0): got %d", trans.OutDegree(0))
	}

	// Node 1: edge from 0 in original -> outgoing to 0 in transpose
	if trans.OutDegree(1) != 1 {
		t.Errorf("transposed OutDegree(1): got %d", trans.OutDegree(1))
	}

	// Node 2: edges from 0,1 in original -> outgoing to 0,1 in transpose
	if trans.OutDegree(2) != 2 {
		t.Errorf("transposed OutDegree(2): got %d", trans.OutDegree(2))
	}
}

func TestNewCSRFromBytes(t *testing.T) {
	// Build byte arrays for indptr and indices
	indptr := []int64{0, 2, 3, 3}
	indices := []int64{1, 2, 2}

	indptrBytes := make([]byte, len(indptr)*8)
	for i, v := range indptr {
		binary.LittleEndian.PutUint64(indptrBytes[i*8:], uint64(v))
	}
	indicesBytes := make([]byte, len(indices)*8)
	for i, v := range indices {
		binary.LittleEndian.PutUint64(indicesBytes[i*8:], uint64(v))
	}

	csr := NewCSRFromBytes(3, 3, indptrBytes, indicesBytes)
	if csr.NumNodes != 3 {
		t.Errorf("NumNodes: got %d", csr.NumNodes)
	}
	if csr.NumEdges != 3 {
		t.Errorf("NumEdges: got %d", csr.NumEdges)
	}

	n := csr.Neighbors(0)
	if len(n) != 2 {
		t.Errorf("Neighbors(0) from bytes: got %v", n)
	}
}

func TestTopologicalSort(t *testing.T) {
	// DAG: 0->1, 0->2, 1->3, 2->3
	csr := NewCSR(4, []int64{0, 2, 3, 4, 4}, []int64{1, 2, 3, 3})

	order, err := TopologicalSort(csr)
	if err != nil {
		t.Fatalf("TopologicalSort failed: %v", err)
	}
	if len(order) != 4 {
		t.Fatalf("expected 4 nodes in order, got %d", len(order))
	}

	// Verify 0 comes before 1 and 2, which come before 3
	pos := make(map[int64]int)
	for i, v := range order {
		pos[v] = i
	}
	if pos[0] > pos[1] || pos[0] > pos[2] {
		t.Error("0 should come before 1 and 2")
	}
	if pos[1] > pos[3] || pos[2] > pos[3] {
		t.Error("1 and 2 should come before 3")
	}
}
