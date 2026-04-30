package gnn

import (
	"encoding/binary"
)

// AuxWriter writes auxiliary data like CSR/CSC indices.
type AuxWriter struct {
	enc *SectionEncoder
}

// NewAuxWriter creates a new aux writer.
func NewAuxWriter() *AuxWriter {
	return &AuxWriter{
		enc: NewSectionEncoder(),
	}
}

// WriteCSR writes CSR (Compressed Sparse Row) indices.
// indptr has length numNodes+1, indices has length numEdges.
func (w *AuxWriter) WriteCSR(numNodes int64, indptr, indices []int64) {
	// Format
	w.enc.buf.writeByte(byte(AuxFormatCSR))

	// Num nodes
	w.enc.WriteUvarint(uint64(numNodes))

	// Num edges
	numEdges := int64(len(indices))
	w.enc.WriteUvarint(uint64(numEdges))

	// IndPtr as raw bytes
	indptrBytes := Int64ToBytes(indptr)
	w.enc.WriteBytes(indptrBytes)

	// Indices as raw bytes
	indicesBytes := Int64ToBytes(indices)
	w.enc.WriteBytes(indicesBytes)
}

// WriteCSC writes CSC (Compressed Sparse Column) indices.
// indptr has length numNodes+1, indices has length numEdges.
func (w *AuxWriter) WriteCSC(numNodes int64, indptr, indices []int64) {
	// Format
	w.enc.buf.writeByte(byte(AuxFormatCSC))

	// Num nodes
	w.enc.WriteUvarint(uint64(numNodes))

	// Num edges
	numEdges := int64(len(indices))
	w.enc.WriteUvarint(uint64(numEdges))

	// IndPtr as raw bytes
	indptrBytes := Int64ToBytes(indptr)
	w.enc.WriteBytes(indptrBytes)

	// Indices as raw bytes
	indicesBytes := Int64ToBytes(indices)
	w.enc.WriteBytes(indicesBytes)
}

// Bytes returns the encoded section body.
func (w *AuxWriter) Bytes() []byte {
	return w.enc.Bytes()
}

// ToSection returns this as a Section.
func (w *AuxWriter) ToSection(name string) Section {
	return Section{
		Kind: SectionAux,
		Name: name,
		Body: w.Bytes(),
	}
}

// AuxReader reads auxiliary data.
type AuxReader struct {
	dec      *SectionDecoder
	format   AuxFormat
	numNodes int64
	numEdges int64
}

// NewAuxReader creates a reader for an aux section.
func NewAuxReader(data []byte) (*AuxReader, error) {
	dec := NewSectionDecoder(data)

	// Read format
	formatByte, err := dec.r.readByte()
	if err != nil {
		return nil, err
	}

	// Read num nodes
	numNodes, err := dec.ReadUvarint()
	if err != nil {
		return nil, err
	}

	// Read num edges
	numEdges, err := dec.ReadUvarint()
	if err != nil {
		return nil, err
	}

	return &AuxReader{
		dec:      dec,
		format:   AuxFormat(formatByte),
		numNodes: int64(numNodes),
		numEdges: int64(numEdges),
	}, nil
}

// Format returns the aux format.
func (r *AuxReader) Format() AuxFormat {
	return r.format
}

// NumNodes returns the number of nodes.
func (r *AuxReader) NumNodes() int64 {
	return r.numNodes
}

// NumEdges returns the number of edges.
func (r *AuxReader) NumEdges() int64 {
	return r.numEdges
}

// ReadCSRArrays reads CSR data as int64 slices.
func (r *AuxReader) ReadCSRArrays() (indptr, indices []int64, err error) {
	// Read indptr bytes
	indptrBytes, err := r.dec.ReadBytes()
	if err != nil {
		return nil, nil, err
	}
	indptr = BytesToInt64(indptrBytes)

	// Read indices bytes
	indicesBytes, err := r.dec.ReadBytes()
	if err != nil {
		return nil, nil, err
	}
	indices = BytesToInt64(indicesBytes)

	return indptr, indices, nil
}

// ReadAuxData reads the full aux data.
func (r *AuxReader) ReadAuxData() (*AuxData, error) {
	indptr, indices, err := r.ReadCSRArrays()
	if err != nil {
		return nil, err
	}

	return &AuxData{
		Format:   r.format,
		NumNodes: r.numNodes,
		IndPtr:   Int64ToBytes(indptr),
		Indices:  Int64ToBytes(indices),
	}, nil
}

// COOToCSR converts COO format to CSR.
// src, dst are the edge endpoints (length E).
// Returns indptr (length N+1) and indices (length E).
func COOToCSR(numNodes int64, src, dst []int64) (indptr, indices []int64) {
	numEdges := len(src)

	// Count edges per node
	counts := make([]int, numNodes)
	for _, s := range src {
		if s >= 0 && s < numNodes {
			counts[s]++
		}
	}

	// Build indptr
	indptr = make([]int64, numNodes+1)
	for i := int64(0); i < numNodes; i++ {
		indptr[i+1] = indptr[i] + int64(counts[i])
	}

	// Build indices (sorted by source)
	indices = make([]int64, numEdges)
	pos := make([]int64, numNodes)
	for i := 0; i < numEdges; i++ {
		s := src[i]
		d := dst[i]
		if s >= 0 && s < numNodes {
			idx := indptr[s] + pos[s]
			indices[idx] = d
			pos[s]++
		}
	}

	return indptr, indices
}

// COOToCSC converts COO format to CSC.
// src, dst are the edge endpoints (length E).
// Returns indptr (length N+1) and indices (length E).
func COOToCSC(numNodes int64, src, dst []int64) (indptr, indices []int64) {
	// CSC is CSR on the transposed graph
	return COOToCSR(numNodes, dst, src)
}

// CSRToNeighbors returns the neighbors of a given node from CSR data.
func CSRToNeighbors(nodeID int64, indptr, indices []int64) []int64 {
	if nodeID < 0 || nodeID >= int64(len(indptr)-1) {
		return nil
	}
	start := indptr[nodeID]
	end := indptr[nodeID+1]
	return indices[start:end]
}

// CSRToCOO converts CSR format back to COO.
func CSRToCOO(indptr, indices []int64) (src, dst []int64) {
	numEdges := len(indices)
	src = make([]int64, numEdges)
	dst = make([]int64, numEdges)

	numNodes := len(indptr) - 1
	edgeIdx := 0
	for nodeID := 0; nodeID < numNodes; nodeID++ {
		start := indptr[nodeID]
		end := indptr[nodeID+1]
		for j := start; j < end; j++ {
			src[edgeIdx] = int64(nodeID)
			dst[edgeIdx] = indices[j]
			edgeIdx++
		}
	}

	return src, dst
}

// AddSelfLoops adds self-loops to an edge list (in COO format).
func AddSelfLoops(numNodes int64, src, dst []int64) (newSrc, newDst []int64) {
	// Check which nodes already have self-loops
	hasSelfLoop := make(map[int64]bool)
	for i := range src {
		if src[i] == dst[i] {
			hasSelfLoop[src[i]] = true
		}
	}

	// Add missing self-loops
	numMissing := 0
	for i := int64(0); i < numNodes; i++ {
		if !hasSelfLoop[i] {
			numMissing++
		}
	}

	newSrc = make([]int64, len(src)+numMissing)
	newDst = make([]int64, len(dst)+numMissing)

	copy(newSrc, src)
	copy(newDst, dst)

	idx := len(src)
	for i := int64(0); i < numNodes; i++ {
		if !hasSelfLoop[i] {
			newSrc[idx] = i
			newDst[idx] = i
			idx++
		}
	}

	return newSrc, newDst
}

// ToUndirected converts a directed graph to undirected by adding reverse edges.
func ToUndirected(src, dst []int64) (newSrc, newDst []int64) {
	// Track existing edges
	existing := make(map[int64]map[int64]bool)
	for i := range src {
		s, d := src[i], dst[i]
		if existing[s] == nil {
			existing[s] = make(map[int64]bool)
		}
		existing[s][d] = true
	}

	// Count edges to add
	toAdd := 0
	for i := range src {
		s, d := src[i], dst[i]
		if s != d { // Don't double self-loops
			if existing[d] == nil || !existing[d][s] {
				toAdd++
			}
		}
	}

	newSrc = make([]int64, len(src)+toAdd)
	newDst = make([]int64, len(dst)+toAdd)

	copy(newSrc, src)
	copy(newDst, dst)

	idx := len(src)
	for i := range src {
		s, d := src[i], dst[i]
		if s != d {
			if existing[d] == nil || !existing[d][s] {
				newSrc[idx] = d
				newDst[idx] = s
				idx++
				// Mark as added
				if existing[d] == nil {
					existing[d] = make(map[int64]bool)
				}
				existing[d][s] = true
			}
		}
	}

	return newSrc, newDst
}

// Helper already defined in feature.go, using import-safe version
func int64ToBytes(data []int64) []byte {
	result := make([]byte, len(data)*8)
	for i, v := range data {
		binary.LittleEndian.PutUint64(result[i*8:], uint64(v))
	}
	return result
}

func bytesToInt64(data []byte) []int64 {
	count := len(data) / 8
	result := make([]int64, count)
	for i := 0; i < count; i++ {
		result[i] = int64(binary.LittleEndian.Uint64(data[i*8:]))
	}
	return result
}
