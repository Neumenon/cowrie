// Package graph implements GraphCowrie-Stream, a streaming graph event format.
//
// GraphCowrie-Stream encodes graph mutations (nodes, edges, triples) as a sequence
// of frames, where each frame is a complete Cowrie value. This allows efficient
// streaming, incremental processing, and compression.
//
// Wire format:
//
//	Magic:   'S' 'J' 'G' 'S'  (4 bytes)
//	Version: 0x01             (1 byte)
//	Flags:   bitfield         (1 byte)
//	FieldDict: [count:uvarint][entries...]
//	LabelDict: [count:uvarint][entries...]  # Node labels, edge types
//	PredDict:  [count:uvarint][entries...]  # RDF predicates (optional)
//	Frames:    [len:u32][frameBody:Cowrie]...
package graph

import (
	"github.com/Neumenon/cowrie/go/ld"
)

// Wire format constants
const (
	Magic0  = 'S'
	Magic1  = 'J'
	Magic2  = 'G'
	Magic3  = 'S'
	Version = 1
)

// Stream flags
const (
	FlagHasPredDict  = 0x01 // Has RDF predicate dictionary
	FlagCompressed   = 0x02 // Frames are zstd compressed
	FlagHasTimestamp = 0x04 // Events include timestamps
)

// EventKind identifies the type of graph event.
type EventKind int

const (
	EventNode   EventKind = 0 // Node upsert/delete
	EventEdge   EventKind = 1 // Edge upsert/delete
	EventTriple EventKind = 2 // RDF triple assert/retract
)

// String returns the event kind name.
func (k EventKind) String() string {
	switch k {
	case EventNode:
		return "node"
	case EventEdge:
		return "edge"
	case EventTriple:
		return "triple"
	default:
		return "unknown"
	}
}

// Op represents the operation type.
type Op int

const (
	OpUpsert Op = 0 // Create or update (assert for triples)
	OpDelete Op = 1 // Delete (retract for triples)
)

// String returns the operation name.
func (o Op) String() string {
	switch o {
	case OpUpsert:
		return "upsert"
	case OpDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// NodeEvent represents a node creation, update, or deletion.
type NodeEvent struct {
	Op        Op             // Operation type
	ID        string         // Node identifier (may be UUID, integer, or string)
	Labels    []string       // Node labels (e.g., ["Person", "Employee"])
	Props     map[string]any // Node properties
	Timestamp int64          // Optional: nanoseconds since epoch
}

// EdgeEvent represents an edge creation, update, or deletion.
type EdgeEvent struct {
	Op        Op             // Operation type
	ID        string         // Edge identifier (optional)
	Label     string         // Edge type/label (e.g., "KNOWS", "WORKS_FOR")
	FromID    string         // Source node ID
	ToID      string         // Target node ID
	Props     map[string]any // Edge properties
	Timestamp int64          // Optional: nanoseconds since epoch
}

// TermKind identifies the type of RDF term.
type TermKind int

const (
	TermIRI     TermKind = 0 // IRI reference
	TermBNode   TermKind = 1 // Blank node
	TermLiteral TermKind = 2 // Literal value
)

// String returns the term kind name.
func (k TermKind) String() string {
	switch k {
	case TermIRI:
		return "iri"
	case TermBNode:
		return "bnode"
	case TermLiteral:
		return "literal"
	default:
		return "unknown"
	}
}

// RDFTerm represents a subject, predicate, or object in an RDF triple.
type RDFTerm struct {
	Kind     TermKind // Type of term
	Value    string   // IRI string, blank node ID, or literal value
	Datatype ld.IRI   // Datatype IRI for literals (empty = plain literal)
	Lang     string   // Language tag for literals (e.g., "en", "fr")
}

// NewIRITerm creates an IRI term.
func NewIRITerm(iri string) RDFTerm {
	return RDFTerm{Kind: TermIRI, Value: iri}
}

// NewBNodeTerm creates a blank node term.
func NewBNodeTerm(id string) RDFTerm {
	return RDFTerm{Kind: TermBNode, Value: id}
}

// NewLiteralTerm creates a plain literal term.
func NewLiteralTerm(value string) RDFTerm {
	return RDFTerm{Kind: TermLiteral, Value: value}
}

// NewTypedLiteral creates a typed literal term.
func NewTypedLiteral(value string, datatype ld.IRI) RDFTerm {
	return RDFTerm{Kind: TermLiteral, Value: value, Datatype: datatype}
}

// NewLangLiteral creates a language-tagged literal term.
func NewLangLiteral(value, lang string) RDFTerm {
	return RDFTerm{Kind: TermLiteral, Value: value, Lang: lang}
}

// TripleEvent represents an RDF triple assertion or retraction.
type TripleEvent struct {
	Op        Op      // OpUpsert = assert, OpDelete = retract
	Subject   RDFTerm // Subject (IRI or BNode)
	Predicate string  // Predicate IRI
	Object    RDFTerm // Object (IRI, BNode, or Literal)
	Graph     string  // Named graph IRI (empty = default graph)
	Timestamp int64   // Optional: nanoseconds since epoch
}

// Event is a union type for all event kinds.
type Event struct {
	Kind   EventKind
	Node   *NodeEvent
	Edge   *EdgeEvent
	Triple *TripleEvent
}

// NewNodeEvent creates a node event wrapper.
func NewNodeEvent(evt *NodeEvent) Event {
	return Event{Kind: EventNode, Node: evt}
}

// NewEdgeEvent creates an edge event wrapper.
func NewEdgeEvent(evt *EdgeEvent) Event {
	return Event{Kind: EventEdge, Edge: evt}
}

// NewTripleEvent creates a triple event wrapper.
func NewTripleEvent(evt *TripleEvent) Event {
	return Event{Kind: EventTriple, Triple: evt}
}

// StreamHeader contains metadata for a GraphCowrie stream.
type StreamHeader struct {
	Flags     byte     // Stream flags
	FieldDict []string // Object field names
	LabelDict []string // Node labels and edge types
	PredDict  []string // RDF predicates (if FlagHasPredDict)
}

// NewStreamHeader creates a new stream header.
func NewStreamHeader() *StreamHeader {
	return &StreamHeader{
		FieldDict: make([]string, 0),
		LabelDict: make([]string, 0),
		PredDict:  make([]string, 0),
	}
}

// AddField adds a field to the dictionary and returns its index.
func (h *StreamHeader) AddField(field string) int {
	for i, f := range h.FieldDict {
		if f == field {
			return i
		}
	}
	h.FieldDict = append(h.FieldDict, field)
	return len(h.FieldDict) - 1
}

// AddLabel adds a label to the dictionary and returns its index.
func (h *StreamHeader) AddLabel(label string) int {
	for i, l := range h.LabelDict {
		if l == label {
			return i
		}
	}
	h.LabelDict = append(h.LabelDict, label)
	return len(h.LabelDict) - 1
}

// AddPredicate adds a predicate to the dictionary and returns its index.
func (h *StreamHeader) AddPredicate(pred string) int {
	h.Flags |= FlagHasPredDict
	for i, p := range h.PredDict {
		if p == pred {
			return i
		}
	}
	h.PredDict = append(h.PredDict, pred)
	return len(h.PredDict) - 1
}

// GetField returns the field name at the given index.
func (h *StreamHeader) GetField(idx int) string {
	if idx < 0 || idx >= len(h.FieldDict) {
		return ""
	}
	return h.FieldDict[idx]
}

// GetLabel returns the label at the given index.
func (h *StreamHeader) GetLabel(idx int) string {
	if idx < 0 || idx >= len(h.LabelDict) {
		return ""
	}
	return h.LabelDict[idx]
}

// GetPredicate returns the predicate at the given index.
func (h *StreamHeader) GetPredicate(idx int) string {
	if idx < 0 || idx >= len(h.PredDict) {
		return ""
	}
	return h.PredDict[idx]
}
