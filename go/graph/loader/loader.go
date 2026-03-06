// Package loader provides graph database writers for GraphCowrie streams.
//
// The loader package reads GraphCowrie-Stream data and writes it to various
// graph database backends. It supports batched writes for performance and
// provides a common interface for different graph databases.
//
// Example usage:
//
//	// Load from stream to Neo4j
//	writer := loader.NewNeo4jWriter(driver, 1000)
//	err := loader.LoadFromStream(streamData, writer)
//
//	// Load from stream to in-memory graph
//	memGraph := loader.NewMemoryGraph()
//	err := loader.LoadFromStream(streamData, memGraph)
package loader

import (
	"github.com/Neumenon/cowrie/go/graph"
)

// GraphWriter is the interface for writing graph events to a backend.
type GraphWriter interface {
	// WriteNode writes a node event (upsert or delete).
	WriteNode(evt *graph.NodeEvent) error

	// WriteEdge writes an edge event (upsert or delete).
	WriteEdge(evt *graph.EdgeEvent) error

	// WriteTriple writes an RDF triple event (assert or retract).
	// This is optional - writers that don't support RDF can return nil.
	WriteTriple(evt *graph.TripleEvent) error

	// Flush forces any buffered writes to the backend.
	Flush() error

	// Close closes the writer and releases resources.
	Close() error
}

// BatchingWriter wraps a GraphWriter to provide batching functionality.
type BatchingWriter struct {
	writer    GraphWriter
	batchSize int
	nodes     []*graph.NodeEvent
	edges     []*graph.EdgeEvent
	triples   []*graph.TripleEvent
}

// NewBatchingWriter creates a new batching writer.
func NewBatchingWriter(writer GraphWriter, batchSize int) *BatchingWriter {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &BatchingWriter{
		writer:    writer,
		batchSize: batchSize,
		nodes:     make([]*graph.NodeEvent, 0, batchSize),
		edges:     make([]*graph.EdgeEvent, 0, batchSize),
		triples:   make([]*graph.TripleEvent, 0, batchSize),
	}
}

// WriteNode buffers a node event.
func (bw *BatchingWriter) WriteNode(evt *graph.NodeEvent) error {
	bw.nodes = append(bw.nodes, evt)
	if len(bw.nodes) >= bw.batchSize {
		return bw.flushNodes()
	}
	return nil
}

// WriteEdge buffers an edge event.
func (bw *BatchingWriter) WriteEdge(evt *graph.EdgeEvent) error {
	bw.edges = append(bw.edges, evt)
	if len(bw.edges) >= bw.batchSize {
		return bw.flushEdges()
	}
	return nil
}

// WriteTriple buffers a triple event.
func (bw *BatchingWriter) WriteTriple(evt *graph.TripleEvent) error {
	bw.triples = append(bw.triples, evt)
	if len(bw.triples) >= bw.batchSize {
		return bw.flushTriples()
	}
	return nil
}

func (bw *BatchingWriter) flushNodes() error {
	for _, evt := range bw.nodes {
		if err := bw.writer.WriteNode(evt); err != nil {
			return err
		}
	}
	bw.nodes = bw.nodes[:0]
	return nil
}

func (bw *BatchingWriter) flushEdges() error {
	for _, evt := range bw.edges {
		if err := bw.writer.WriteEdge(evt); err != nil {
			return err
		}
	}
	bw.edges = bw.edges[:0]
	return nil
}

func (bw *BatchingWriter) flushTriples() error {
	for _, evt := range bw.triples {
		if err := bw.writer.WriteTriple(evt); err != nil {
			return err
		}
	}
	bw.triples = bw.triples[:0]
	return nil
}

// Flush flushes all buffered events.
func (bw *BatchingWriter) Flush() error {
	if err := bw.flushNodes(); err != nil {
		return err
	}
	if err := bw.flushEdges(); err != nil {
		return err
	}
	if err := bw.flushTriples(); err != nil {
		return err
	}
	return bw.writer.Flush()
}

// Close flushes and closes the underlying writer.
func (bw *BatchingWriter) Close() error {
	if err := bw.Flush(); err != nil {
		return err
	}
	return bw.writer.Close()
}

// LoadFromStream reads a GraphCowrie stream and writes events to the writer.
func LoadFromStream(data []byte, writer GraphWriter) error {
	sr, err := graph.NewStreamReader(data)
	if err != nil {
		return err
	}

	for {
		evt, err := sr.Next()
		if err != nil {
			return err
		}
		if evt == nil {
			break
		}

		switch evt.Kind {
		case graph.EventNode:
			if err := writer.WriteNode(evt.Node); err != nil {
				return err
			}
		case graph.EventEdge:
			if err := writer.WriteEdge(evt.Edge); err != nil {
				return err
			}
		case graph.EventTriple:
			if err := writer.WriteTriple(evt.Triple); err != nil {
				return err
			}
		}
	}

	return writer.Flush()
}

// LoadFromReader reads from a StreamReader and writes events to the writer.
func LoadFromReader(sr *graph.StreamReader, writer GraphWriter) error {
	for {
		evt, err := sr.Next()
		if err != nil {
			return err
		}
		if evt == nil {
			break
		}

		switch evt.Kind {
		case graph.EventNode:
			if err := writer.WriteNode(evt.Node); err != nil {
				return err
			}
		case graph.EventEdge:
			if err := writer.WriteEdge(evt.Edge); err != nil {
				return err
			}
		case graph.EventTriple:
			if err := writer.WriteTriple(evt.Triple); err != nil {
				return err
			}
		}
	}

	return writer.Flush()
}

// Stats tracks loading statistics.
type Stats struct {
	NodesWritten   int64
	EdgesWritten   int64
	TriplesWritten int64
	NodesDeleted   int64
	EdgesDeleted   int64
	TriplesDeleted int64
}

// StatsWriter wraps a writer to collect statistics.
type StatsWriter struct {
	writer GraphWriter
	stats  Stats
}

// NewStatsWriter creates a new stats-collecting writer.
func NewStatsWriter(writer GraphWriter) *StatsWriter {
	return &StatsWriter{writer: writer}
}

// WriteNode writes and tracks stats.
func (sw *StatsWriter) WriteNode(evt *graph.NodeEvent) error {
	if err := sw.writer.WriteNode(evt); err != nil {
		return err
	}
	if evt.Op == graph.OpDelete {
		sw.stats.NodesDeleted++
	} else {
		sw.stats.NodesWritten++
	}
	return nil
}

// WriteEdge writes and tracks stats.
func (sw *StatsWriter) WriteEdge(evt *graph.EdgeEvent) error {
	if err := sw.writer.WriteEdge(evt); err != nil {
		return err
	}
	if evt.Op == graph.OpDelete {
		sw.stats.EdgesDeleted++
	} else {
		sw.stats.EdgesWritten++
	}
	return nil
}

// WriteTriple writes and tracks stats.
func (sw *StatsWriter) WriteTriple(evt *graph.TripleEvent) error {
	if err := sw.writer.WriteTriple(evt); err != nil {
		return err
	}
	if evt.Op == graph.OpDelete {
		sw.stats.TriplesDeleted++
	} else {
		sw.stats.TriplesWritten++
	}
	return nil
}

// Flush flushes the underlying writer.
func (sw *StatsWriter) Flush() error {
	return sw.writer.Flush()
}

// Close closes the underlying writer.
func (sw *StatsWriter) Close() error {
	return sw.writer.Close()
}

// Stats returns the collected statistics.
func (sw *StatsWriter) Stats() Stats {
	return sw.stats
}
