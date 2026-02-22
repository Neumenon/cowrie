package gnn

import (
	"bytes"
	"strconv"

	"github.com/Neumenon/cowrie/graph"
)

// EdgeTableWriter writes edge records as a GraphCowrie-Stream section.
type EdgeTableWriter struct {
	buf      bytes.Buffer
	sw       *graph.StreamWriter
	edgeType string
}

// NewEdgeTableWriter creates a writer for an edge table section.
func NewEdgeTableWriter(edgeType string) *EdgeTableWriter {
	w := &EdgeTableWriter{
		edgeType: edgeType,
	}
	w.sw = graph.NewStreamWriter(&w.buf)
	w.sw.Header().AddLabel(edgeType)
	return w
}

// WriteHeader writes the stream header. Must be called before any WriteEdge calls.
func (w *EdgeTableWriter) WriteHeader() error {
	return w.sw.WriteHeader()
}

// WriteEdge writes an edge record in COO format.
func (w *EdgeTableWriter) WriteEdge(src, dst int64, props map[string]any) error {
	evt := &graph.EdgeEvent{
		Op:     graph.OpUpsert,
		Label:  w.edgeType,
		FromID: formatEdgeID(src),
		ToID:   formatEdgeID(dst),
		Props:  props,
	}
	return w.sw.WriteEdge(evt)
}

// WriteEdgeWithTimestamp writes an edge with a timestamp for temporal graphs.
func (w *EdgeTableWriter) WriteEdgeWithTimestamp(src, dst int64, ts int64, props map[string]any) error {
	if props == nil {
		props = make(map[string]any)
	}
	props["_ts"] = ts

	evt := &graph.EdgeEvent{
		Op:     graph.OpUpsert,
		Label:  w.edgeType,
		FromID: formatEdgeID(src),
		ToID:   formatEdgeID(dst),
		Props:  props,
	}
	return w.sw.WriteEdge(evt)
}

// WriteEdgeWithWeight writes an edge with weight.
func (w *EdgeTableWriter) WriteEdgeWithWeight(src, dst int64, weight float64, props map[string]any) error {
	if props == nil {
		props = make(map[string]any)
	}
	props["weight"] = weight

	evt := &graph.EdgeEvent{
		Op:     graph.OpUpsert,
		Label:  w.edgeType,
		FromID: formatEdgeID(src),
		ToID:   formatEdgeID(dst),
		Props:  props,
	}
	return w.sw.WriteEdge(evt)
}

// Close finalizes the stream.
func (w *EdgeTableWriter) Close() error {
	return w.sw.Close()
}

// Bytes returns the encoded section body.
func (w *EdgeTableWriter) Bytes() []byte {
	return w.buf.Bytes()
}

// ToSection returns this as a Section.
func (w *EdgeTableWriter) ToSection(name string) Section {
	return Section{
		Kind: SectionEdgeTable,
		Name: name,
		Body: w.Bytes(),
	}
}

// EdgeTableReader reads edge records from a GraphCowrie-Stream section.
type EdgeTableReader struct {
	sr *graph.StreamReader
}

// NewEdgeTableReader creates a reader for an edge table section.
func NewEdgeTableReader(data []byte) (*EdgeTableReader, error) {
	sr, err := graph.NewStreamReader(data)
	if err != nil {
		return nil, err
	}
	return &EdgeTableReader{sr: sr}, nil
}

// Header returns the stream header.
func (r *EdgeTableReader) Header() *graph.StreamHeader {
	return r.sr.Header()
}

// ReadEdge reads the next edge event.
func (r *EdgeTableReader) ReadEdge() (*graph.EdgeEvent, error) {
	evt, err := r.sr.Next()
	if err != nil {
		return nil, err
	}
	if evt == nil {
		return nil, nil // End of stream
	}
	if evt.Kind != graph.EventEdge {
		// Skip non-edge events and try again
		return r.ReadEdge()
	}
	return evt.Edge, nil
}

// ReadAll reads all edge events.
func (r *EdgeTableReader) ReadAll() ([]*graph.EdgeEvent, error) {
	var edges []*graph.EdgeEvent
	for {
		evt, err := r.sr.Next()
		if err != nil {
			return nil, err
		}
		if evt == nil {
			break // End of stream
		}
		if evt.Kind == graph.EventEdge {
			edges = append(edges, evt.Edge)
		}
	}
	return edges, nil
}

// EdgeRecord is a simplified edge representation for GNN use.
type EdgeRecord struct {
	Src       int64
	Dst       int64
	Label     string
	Timestamp int64   // Optional, from _ts prop
	Weight    float64 // Optional, from weight prop
	Props     map[string]any
}

// ReadAllRecords reads all edges as EdgeRecords.
func (r *EdgeTableReader) ReadAllRecords() ([]EdgeRecord, error) {
	events, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	records := make([]EdgeRecord, len(events))
	for i, evt := range events {
		rec := EdgeRecord{
			Src:   parseEdgeID(evt.FromID),
			Dst:   parseEdgeID(evt.ToID),
			Label: evt.Label,
			Props: evt.Props,
		}

		// Extract special properties
		if ts, ok := evt.Props["_ts"]; ok {
			if tsInt, ok := ts.(int64); ok {
				rec.Timestamp = tsInt
			}
		}
		if w, ok := evt.Props["weight"]; ok {
			if wFloat, ok := w.(float64); ok {
				rec.Weight = wFloat
			}
		}

		records[i] = rec
	}
	return records, nil
}

// COO returns the edges as coordinate (COO) format arrays.
func (r *EdgeTableReader) COO() (src, dst []int64, err error) {
	records, err := r.ReadAllRecords()
	if err != nil {
		return nil, nil, err
	}

	src = make([]int64, len(records))
	dst = make([]int64, len(records))
	for i, rec := range records {
		src[i] = rec.Src
		dst[i] = rec.Dst
	}
	return src, dst, nil
}

// formatEdgeID converts an int64 ID to string.
func formatEdgeID(id int64) string {
	return strconv.FormatInt(id, 10)
}

// parseEdgeID parses a string ID to int64.
func parseEdgeID(idStr string) int64 {
	id, _ := strconv.ParseInt(idStr, 10, 64)
	return id
}
