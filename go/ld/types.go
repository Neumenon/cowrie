// Package ld implements Cowrie-LD, a binary JSON-LD codec on top of Cowrie v2.
//
// Cowrie-LD extends Cowrie v2 with:
//   - IRI references (0x11 tag)
//   - Blank nodes (0x12 tag)
//   - Term tables for JSON-LD context mapping
//   - Datatype tables for typed literals
//
// Wire format:
//
//	Magic:   'S' 'J' 'L' 'D'  (4 bytes)
//	Version: 0x01             (1 byte)
//	Flags:   0x00             (1 byte)
//	FieldDict: [count:uvarint][entries...]
//	Terms:     [count:uvarint][TermEntry...]
//	IRIs:      [count:uvarint][entries...]
//	Datatypes: [count:uvarint][entries...]
//	RootValue: Cowrie v2 value tree
package ld

import (
	"github.com/Neumenon/cowrie/go"
)

// Wire format constants
const (
	Magic0  = 'S'
	Magic1  = 'J'
	Magic2  = 'L'
	Magic3  = 'D'
	Version = 1
)

// Extended type tags for Cowrie-LD
const (
	TagIRI   = 0x11 // [iriID:uvarint] → IRIs[iriID]
	TagBNode = 0x12 // [len:uvarint][utf8]
)

// TermFlags defines container and type semantics for JSON-LD terms
type TermFlags byte

const (
	TermFlagNone     TermFlags = 0x00
	TermFlagList     TermFlags = 0x01 // @container: @list
	TermFlagSet      TermFlags = 0x02 // @container: @set
	TermFlagLanguage TermFlags = 0x04 // @container: @language
	TermFlagIndex    TermFlags = 0x08 // @container: @index
	TermFlagID       TermFlags = 0x10 // @type: @id
	TermFlagVocab    TermFlags = 0x20 // @type: @vocab
)

// IRI represents an Internationalized Resource Identifier.
// In RDF/JSON-LD, IRIs identify resources and predicates.
type IRI string

// String returns the IRI as a string.
func (i IRI) String() string {
	return string(i)
}

// BNode represents a blank node (anonymous resource) in RDF.
// Blank nodes are local identifiers that don't have global meaning.
type BNode string

// String returns the blank node identifier.
func (b BNode) String() string {
	return string(b)
}

// Literal represents an RDF literal with optional datatype and language tag.
type Literal struct {
	Value    *cowrie.Value // The underlying Cowrie value
	Datatype IRI          // xsd:string, xsd:dateTime, etc. (empty = plain literal)
	Lang     string       // Language tag (e.g., "en", "fr-CA"), empty if typed
}

// TermEntry maps a JSON-LD term to its IRI and semantics.
type TermEntry struct {
	Term  string    // The short name used in JSON (e.g., "name")
	IRI   IRI       // The full IRI (e.g., "http://schema.org/name")
	Flags TermFlags // Container type, default type, etc.
}

// LDDocument represents a complete Cowrie-LD document.
type LDDocument struct {
	FieldDict []string    // Object key dictionary (inherited from Cowrie)
	Terms     []TermEntry // JSON-LD term definitions
	IRIs      []IRI       // IRI table
	Datatypes []IRI       // Datatype IRI table
	Root      *cowrie.Value
}

// NewDocument creates an empty Cowrie-LD document.
func NewDocument() *LDDocument {
	return &LDDocument{
		FieldDict: make([]string, 0),
		Terms:     make([]TermEntry, 0),
		IRIs:      make([]IRI, 0),
		Datatypes: make([]IRI, 0),
	}
}

// AddTerm adds a term mapping to the document.
func (d *LDDocument) AddTerm(term string, iri IRI, flags TermFlags) {
	d.Terms = append(d.Terms, TermEntry{
		Term:  term,
		IRI:   iri,
		Flags: flags,
	})
}

// AddIRI adds an IRI to the document's IRI table and returns its index.
func (d *LDDocument) AddIRI(iri IRI) int {
	// Check if already exists
	for i, existing := range d.IRIs {
		if existing == iri {
			return i
		}
	}
	d.IRIs = append(d.IRIs, iri)
	return len(d.IRIs) - 1
}

// AddDatatype adds a datatype IRI to the table and returns its index.
func (d *LDDocument) AddDatatype(dt IRI) int {
	// Check if already exists
	for i, existing := range d.Datatypes {
		if existing == dt {
			return i
		}
	}
	d.Datatypes = append(d.Datatypes, dt)
	return len(d.Datatypes) - 1
}

// GetIRI returns the IRI at the given index, or empty string if out of bounds.
func (d *LDDocument) GetIRI(idx int) IRI {
	if idx < 0 || idx >= len(d.IRIs) {
		return ""
	}
	return d.IRIs[idx]
}

// GetDatatype returns the datatype IRI at the given index, or empty string if out of bounds.
func (d *LDDocument) GetDatatype(idx int) IRI {
	if idx < 0 || idx >= len(d.Datatypes) {
		return ""
	}
	return d.Datatypes[idx]
}

// LookupTerm finds a term entry by its short name.
func (d *LDDocument) LookupTerm(term string) *TermEntry {
	for i := range d.Terms {
		if d.Terms[i].Term == term {
			return &d.Terms[i]
		}
	}
	return nil
}

// LookupTermByIRI finds a term entry by its IRI.
func (d *LDDocument) LookupTermByIRI(iri IRI) *TermEntry {
	for i := range d.Terms {
		if d.Terms[i].IRI == iri {
			return &d.Terms[i]
		}
	}
	return nil
}

// Common XSD datatypes
const (
	XSDString   IRI = "http://www.w3.org/2001/XMLSchema#string"
	XSDBoolean  IRI = "http://www.w3.org/2001/XMLSchema#boolean"
	XSDInteger  IRI = "http://www.w3.org/2001/XMLSchema#integer"
	XSDDouble   IRI = "http://www.w3.org/2001/XMLSchema#double"
	XSDDecimal  IRI = "http://www.w3.org/2001/XMLSchema#decimal"
	XSDDateTime IRI = "http://www.w3.org/2001/XMLSchema#dateTime"
	XSDDate     IRI = "http://www.w3.org/2001/XMLSchema#date"
	XSDTime     IRI = "http://www.w3.org/2001/XMLSchema#time"
	XSDAnyURI   IRI = "http://www.w3.org/2001/XMLSchema#anyURI"
	XSDBase64   IRI = "http://www.w3.org/2001/XMLSchema#base64Binary"
)

// Common RDF namespace IRIs
const (
	RDFType    IRI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
	RDFFirst   IRI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first"
	RDFRest    IRI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest"
	RDFNil     IRI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil"
	RDFSLabel  IRI = "http://www.w3.org/2000/01/rdf-schema#label"
	RDFSRange  IRI = "http://www.w3.org/2000/01/rdf-schema#range"
	RDFSDomain IRI = "http://www.w3.org/2000/01/rdf-schema#domain"
)

// LDValue wraps an Cowrie value with optional LD metadata.
// This is used during encoding/decoding to track IRI references and blank nodes.
type LDValue struct {
	*cowrie.Value
	IsIRI   bool   // True if this value is an IRI reference
	IRIID   int    // Index into IRIs table (if IsIRI)
	IsBNode bool   // True if this value is a blank node
	BNodeID string // Blank node identifier (if IsBNode)
}

// NewIRIValue creates an LDValue representing an IRI reference.
func NewIRIValue(iri IRI, doc *LDDocument) *LDValue {
	idx := doc.AddIRI(iri)
	return &LDValue{
		Value: cowrie.String(string(iri)),
		IsIRI: true,
		IRIID: idx,
	}
}

// NewBNodeValue creates an LDValue representing a blank node.
func NewBNodeValue(id string) *LDValue {
	return &LDValue{
		Value:   cowrie.String(id),
		IsBNode: true,
		BNodeID: id,
	}
}

// NewLiteralValue creates an LDValue from a literal with optional datatype.
func NewLiteralValue(v *cowrie.Value, datatype IRI, doc *LDDocument) *LDValue {
	if datatype != "" {
		doc.AddDatatype(datatype)
	}
	return &LDValue{
		Value: v,
	}
}
