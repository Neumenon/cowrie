package gnn

import (
	"bytes"
	"strconv"

	"github.com/Neumenon/cowrie/graph"
)

// Note: graph.StreamReader takes []byte, not io.Reader

// NodeTableWriter writes node records as a GraphCowrie-Stream section.
type NodeTableWriter struct {
	buf      bytes.Buffer
	sw       *graph.StreamWriter
	nodeType string
}

// NewNodeTableWriter creates a writer for a node table section.
func NewNodeTableWriter(nodeType string) *NodeTableWriter {
	w := &NodeTableWriter{
		nodeType: nodeType,
	}
	w.sw = graph.NewStreamWriter(&w.buf)
	w.sw.Header().AddLabel(nodeType)
	return w
}

// WriteHeader writes the stream header. Must be called before any WriteNode calls.
func (w *NodeTableWriter) WriteHeader() error {
	return w.sw.WriteHeader()
}

// WriteNode writes a node record.
func (w *NodeTableWriter) WriteNode(id int64, props map[string]any) error {
	evt := &graph.NodeEvent{
		Op:     graph.OpUpsert,
		ID:     formatNodeID(id),
		Labels: []string{w.nodeType},
		Props:  props,
	}
	return w.sw.WriteNode(evt)
}

// WriteNodeWithLabels writes a node with custom labels.
func (w *NodeTableWriter) WriteNodeWithLabels(id int64, labels []string, props map[string]any) error {
	evt := &graph.NodeEvent{
		Op:     graph.OpUpsert,
		ID:     formatNodeID(id),
		Labels: labels,
		Props:  props,
	}
	return w.sw.WriteNode(evt)
}

// Close finalizes the stream.
func (w *NodeTableWriter) Close() error {
	return w.sw.Close()
}

// Bytes returns the encoded section body.
func (w *NodeTableWriter) Bytes() []byte {
	return w.buf.Bytes()
}

// ToSection returns this as a Section.
func (w *NodeTableWriter) ToSection(name string) Section {
	return Section{
		Kind: SectionNodeTable,
		Name: name,
		Body: w.Bytes(),
	}
}

// NodeTableReader reads node records from a GraphCowrie-Stream section.
type NodeTableReader struct {
	sr *graph.StreamReader
}

// NewNodeTableReader creates a reader for a node table section.
func NewNodeTableReader(data []byte) (*NodeTableReader, error) {
	sr, err := graph.NewStreamReader(data)
	if err != nil {
		return nil, err
	}
	return &NodeTableReader{sr: sr}, nil
}

// Header returns the stream header.
func (r *NodeTableReader) Header() *graph.StreamHeader {
	return r.sr.Header()
}

// ReadNode reads the next node event.
func (r *NodeTableReader) ReadNode() (*graph.NodeEvent, error) {
	evt, err := r.sr.Next()
	if err != nil {
		return nil, err
	}
	if evt == nil {
		return nil, nil // End of stream
	}
	if evt.Kind != graph.EventNode {
		// Skip non-node events and try again
		return r.ReadNode()
	}
	return evt.Node, nil
}

// ReadAll reads all node events.
func (r *NodeTableReader) ReadAll() ([]*graph.NodeEvent, error) {
	var nodes []*graph.NodeEvent
	for {
		evt, err := r.sr.Next()
		if err != nil {
			return nil, err
		}
		if evt == nil {
			break // End of stream
		}
		if evt.Kind == graph.EventNode {
			nodes = append(nodes, evt.Node)
		}
	}
	return nodes, nil
}

// NodeRecord is a simplified node representation for GNN use.
type NodeRecord struct {
	ID     int64
	Labels []string
	Props  map[string]any
}

// ReadAllRecords reads all nodes as NodeRecords.
func (r *NodeTableReader) ReadAllRecords() ([]NodeRecord, error) {
	events, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	records := make([]NodeRecord, len(events))
	for i, evt := range events {
		records[i] = NodeRecord{
			ID:     parseNodeID(evt.ID),
			Labels: evt.Labels,
			Props:  evt.Props,
		}
	}
	return records, nil
}

// formatNodeID converts an int64 ID to string.
func formatNodeID(id int64) string {
	return strconv.FormatInt(id, 10)
}

// parseNodeID parses a string ID to int64.
func parseNodeID(idStr string) int64 {
	id, _ := strconv.ParseInt(idStr, 10, 64)
	return id
}
