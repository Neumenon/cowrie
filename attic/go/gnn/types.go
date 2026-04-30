// Package gnn implements Cowrie-GNN, a binary container format for Graph Neural Network datasets.
//
// Cowrie-GNN provides a compact, compressed, and graph-native format that replaces
// JSON/CSV/Parquet for GNN workflows. It supports heterogeneous graphs, temporal data,
// blocked tensors for zero-copy access, and train/val/test splits.
//
// Wire format:
//
//	Magic:   'S' 'J' 'G' 'N'  (4 bytes)
//	Version: 0x01             (1 byte)
//	Flags:   bitfield         (1 byte)
//	         bit0: heterogeneous
//	         bit1: temporal
//	         bit2: has CSR indices
//
//	SectionCount: uvarint
//	Sections:     Section...
//
// Each section:
//
//	Kind:    uvarint (0=Meta, 1=NodeTable, 2=EdgeTable, 3=Feature, 4=Split, 5=Aux)
//	NameLen: uvarint
//	Name:    utf8 bytes
//	BodyLen: u64
//	Body:    bytes
package gnn

import (
	"io"
)

// Wire format constants
const (
	Magic0  = 'S'
	Magic1  = 'J'
	Magic2  = 'G'
	Magic3  = 'N'
	Version = 1

	// v1.1 adds per-section encoding field and uses Cowrie for metadata sections
	// Uses minor version 0x10 (16) to distinguish from v1.0 flags (0x00-0x0F)
	Version11 = 0x0110 // Major=1, Minor=16
)

// SectionEncoding indicates how section body is encoded.
// v1.0 used implicit encoding (JSON for Meta/Aux, binary for Features).
// v1.1 allows Cowrie for metadata sections.
type SectionEncoding uint8

const (
	// SectionEncodingJSON indicates the section uses JSON encoding (v1.0 default for Meta/Aux)
	SectionEncodingJSON SectionEncoding = 0

	// SectionEncodingCowrie indicates the section uses Cowrie binary encoding
	// Recommended for v1.1: Meta, Splits, Aux sections benefit from Cowrie
	SectionEncodingCowrie SectionEncoding = 1

	// SectionEncodingRawTensor indicates raw binary tensor data (zero-copy)
	// Used for Feature sections - no change from v1.0
	SectionEncodingRawTensor SectionEncoding = 2

	// SectionEncodingGraphStream indicates GraphCowrie-Stream encoding
	// Used for NodeTable and EdgeTable sections - no change from v1.0
	SectionEncodingGraphStream SectionEncoding = 3
)

// Container flags
const (
	FlagHeterogeneous  = 0x01 // Multiple node/edge types
	FlagTemporal       = 0x02 // Temporal graph (timestamps on events)
	FlagHasCSR         = 0x04 // CSR indices in Aux section
	FlagCompressedZstd = 0x08 // Container is zstd compressed
)

// CompressionType identifies the compression algorithm.
type CompressionType uint8

const (
	CompressionNone CompressionType = 0
	CompressionZstd CompressionType = 1
)

// SectionKind identifies the type of a container section.
type SectionKind uint8

const (
	SectionMeta      SectionKind = 0 // Dataset metadata
	SectionNodeTable SectionKind = 1 // Node records (GraphCowrie-Stream)
	SectionEdgeTable SectionKind = 2 // Edge records (GraphCowrie-Stream)
	SectionFeature   SectionKind = 3 // Feature tensors
	SectionSplit     SectionKind = 4 // Train/val/test splits
	SectionAux       SectionKind = 5 // Auxiliary data (CSR, etc.)
)

// String returns the section kind name.
func (k SectionKind) String() string {
	switch k {
	case SectionMeta:
		return "meta"
	case SectionNodeTable:
		return "node_table"
	case SectionEdgeTable:
		return "edge_table"
	case SectionFeature:
		return "feature"
	case SectionSplit:
		return "split"
	case SectionAux:
		return "aux"
	default:
		return "unknown"
	}
}

// DType represents a data type for tensors.
type DType uint8

const (
	DTypeFloat32 DType = 0
	DTypeFloat64 DType = 1
	DTypeInt32   DType = 2
	DTypeInt64   DType = 3
	DTypeUint32  DType = 4
	DTypeUint64  DType = 5
	DTypeBool    DType = 6
	DTypeFloat16 DType = 7 // Half-precision for smaller file sizes
)

// String returns the dtype name.
func (d DType) String() string {
	switch d {
	case DTypeFloat32:
		return "float32"
	case DTypeFloat64:
		return "float64"
	case DTypeInt32:
		return "int32"
	case DTypeInt64:
		return "int64"
	case DTypeUint32:
		return "uint32"
	case DTypeUint64:
		return "uint64"
	case DTypeBool:
		return "bool"
	case DTypeFloat16:
		return "float16"
	default:
		return "unknown"
	}
}

// ByteSize returns the byte size for this dtype.
func (d DType) ByteSize() int {
	switch d {
	case DTypeFloat16:
		return 2
	case DTypeFloat32, DTypeInt32, DTypeUint32:
		return 4
	case DTypeFloat64, DTypeInt64, DTypeUint64:
		return 8
	case DTypeBool:
		return 1
	default:
		return 0
	}
}

// Section represents a section in the container.
type Section struct {
	Kind     SectionKind
	Name     string          // e.g., "nodes:paper", "edges:cites", "features:x"
	Encoding SectionEncoding // v1.1: explicit encoding for this section
	Body     []byte
}

// DefaultEncoding returns the recommended encoding for a section kind.
// Used for v1.0 backward compatibility and v1.1 defaults.
func (k SectionKind) DefaultEncoding() SectionEncoding {
	switch k {
	case SectionMeta, SectionSplit, SectionAux:
		return SectionEncodingCowrie // v1.1 recommends Cowrie for metadata
	case SectionNodeTable, SectionEdgeTable:
		return SectionEncodingGraphStream
	case SectionFeature:
		return SectionEncodingRawTensor
	default:
		return SectionEncodingJSON
	}
}

// LegacyEncoding returns the v1.0 encoding for a section kind.
// Used when reading v1.0 containers.
func (k SectionKind) LegacyEncoding() SectionEncoding {
	switch k {
	case SectionMeta, SectionSplit, SectionAux:
		return SectionEncodingJSON // v1.0 used JSON for metadata
	case SectionNodeTable, SectionEdgeTable:
		return SectionEncodingGraphStream
	case SectionFeature:
		return SectionEncodingRawTensor
	default:
		return SectionEncodingJSON
	}
}

// Meta holds dataset metadata.
type Meta struct {
	DatasetName   string                 `json:"dataset_name"`
	Version       string                 `json:"version,omitempty"`
	Directed      bool                   `json:"directed"`
	Temporal      bool                   `json:"temporal,omitempty"`
	Heterogeneous bool                   `json:"heterogeneous,omitempty"`
	NodeTypes     []string               `json:"node_types,omitempty"`
	EdgeTypes     []EdgeTypeTuple        `json:"edge_types,omitempty"`
	IDSpaces      map[string]IDSpace     `json:"id_spaces,omitempty"`
	Features      map[string]FeatureSpec `json:"features,omitempty"`
	Labels        map[string]LabelSpec   `json:"labels,omitempty"`
}

// EdgeTypeTuple represents a (src_type, edge_type, dst_type) tuple.
type EdgeTypeTuple struct {
	SrcType  string `json:"src_type"`
	EdgeType string `json:"edge_type"`
	DstType  string `json:"dst_type"`
}

// IDSpace defines the ID range for a node type.
type IDSpace struct {
	Start int64 `json:"start"`
	Count int64 `json:"count"`
}

// FeatureSpec describes a feature tensor.
type FeatureSpec struct {
	Shape []int  `json:"shape"`
	DType string `json:"dtype"`
}

// LabelSpec describes labels for nodes/edges.
type LabelSpec struct {
	Shape []int  `json:"shape"`
	DType string `json:"dtype"`
}

// FeatureMode indicates how features are stored.
type FeatureMode uint8

const (
	FeatureModeRowWise FeatureMode = 0 // Each row is an Cowrie object
	FeatureModeBlocked FeatureMode = 1 // Contiguous tensor block
)

// FeatureHeader is the header for a feature section.
type FeatureHeader struct {
	FeatureName string
	Mode        FeatureMode
	DType       DType
	Shape       []int // For blocked: [N, D...]; for row-wise: [D...]
}

// SplitMode indicates how splits are stored.
type SplitMode uint8

const (
	SplitModeIndices SplitMode = 0 // Array of indices
	SplitModeMask    SplitMode = 1 // Bitmask
)

// SplitData holds train/val/test split information.
type SplitData struct {
	Mode      SplitMode
	TrainIdx  []int64 // If mode=indices
	ValIdx    []int64
	TestIdx   []int64
	TrainMask []byte // If mode=mask (ceil(N/8) bytes)
	ValMask   []byte
	TestMask  []byte
}

// AuxFormat identifies the auxiliary data format.
type AuxFormat uint8

const (
	AuxFormatCSR AuxFormat = 0
	AuxFormatCSC AuxFormat = 1
)

// AuxData holds auxiliary data like CSR/CSC indices.
type AuxData struct {
	Format   AuxFormat
	NumNodes int64
	IndPtr   []byte // (N+1) * 8 bytes for int64
	Indices  []byte // E * 8 bytes for int64
}

// Container represents an Cowrie-GNN container.
type Container struct {
	Flags    byte
	Version  uint16 // Wire format version (1 for v1.0, 0x0101 for v1.1)
	Sections []Section

	// Cached parsed data
	meta     *Meta
	features map[string]*FeatureHeader
}

// IsV11 returns true if this container uses v1.1 format.
func (c *Container) IsV11() bool {
	return c.Version >= Version11
}

// NewContainer creates a new empty container using v1.1 format.
func NewContainer(datasetName string) *Container {
	c := &Container{
		Version:  Version11, // Default to v1.1 for new containers
		Sections: make([]Section, 0),
		features: make(map[string]*FeatureHeader),
	}
	// Initialize meta
	c.meta = &Meta{
		DatasetName: datasetName,
		NodeTypes:   make([]string, 0),
		EdgeTypes:   make([]EdgeTypeTuple, 0),
		IDSpaces:    make(map[string]IDSpace),
		Features:    make(map[string]FeatureSpec),
		Labels:      make(map[string]LabelSpec),
	}
	return c
}

// SetHeterogeneous marks the container as heterogeneous.
func (c *Container) SetHeterogeneous(v bool) {
	if v {
		c.Flags |= FlagHeterogeneous
		c.meta.Heterogeneous = true
	} else {
		c.Flags &^= FlagHeterogeneous
		c.meta.Heterogeneous = false
	}
}

// SetTemporal marks the container as temporal.
func (c *Container) SetTemporal(v bool) {
	if v {
		c.Flags |= FlagTemporal
		c.meta.Temporal = true
	} else {
		c.Flags &^= FlagTemporal
		c.meta.Temporal = false
	}
}

// SetDirected sets whether the graph is directed.
func (c *Container) SetDirected(v bool) {
	c.meta.Directed = v
}

// AddNodeType adds a node type with its ID space.
func (c *Container) AddNodeType(nodeType string, count int64) {
	// Calculate start based on existing ID spaces
	var start int64
	for _, space := range c.meta.IDSpaces {
		end := space.Start + space.Count
		if end > start {
			start = end
		}
	}

	c.meta.NodeTypes = append(c.meta.NodeTypes, nodeType)
	c.meta.IDSpaces[nodeType] = IDSpace{Start: start, Count: count}
}

// AddEdgeType adds an edge type.
func (c *Container) AddEdgeType(srcType, edgeType, dstType string) {
	c.meta.EdgeTypes = append(c.meta.EdgeTypes, EdgeTypeTuple{
		SrcType:  srcType,
		EdgeType: edgeType,
		DstType:  dstType,
	})
}

// AddFeature defines a feature for a node type.
func (c *Container) AddFeature(nodeType, featureName string, dtype DType, shape []int) {
	key := nodeType + ":" + featureName
	c.meta.Features[key] = FeatureSpec{
		Shape: shape,
		DType: dtype.String(),
	}
}

// AddLabel defines a label for a node type.
func (c *Container) AddLabel(nodeType, labelName string, dtype DType, shape []int) {
	key := nodeType + ":" + labelName
	c.meta.Labels[key] = LabelSpec{
		Shape: shape,
		DType: dtype.String(),
	}
}

// Meta returns the container metadata.
func (c *Container) Meta() *Meta {
	return c.meta
}

// IsHeterogeneous returns true if the container has multiple node/edge types.
func (c *Container) IsHeterogeneous() bool {
	return c.Flags&FlagHeterogeneous != 0
}

// IsTemporal returns true if the container has temporal data.
func (c *Container) IsTemporal() bool {
	return c.Flags&FlagTemporal != 0
}

// HasCSR returns true if the container has CSR indices.
func (c *Container) HasCSR() bool {
	return c.Flags&FlagHasCSR != 0
}

// AddSection adds a section to the container with default encoding.
func (c *Container) AddSection(kind SectionKind, name string, body []byte) {
	encoding := kind.LegacyEncoding() // v1.0 default
	if c.IsV11() {
		encoding = kind.DefaultEncoding() // v1.1 default
	}
	c.Sections = append(c.Sections, Section{
		Kind:     kind,
		Name:     name,
		Encoding: encoding,
		Body:     body,
	})
}

// AddSectionWithEncoding adds a section with explicit encoding.
func (c *Container) AddSectionWithEncoding(kind SectionKind, name string, encoding SectionEncoding, body []byte) {
	c.Sections = append(c.Sections, Section{
		Kind:     kind,
		Name:     name,
		Encoding: encoding,
		Body:     body,
	})
}

// GetSection returns the first section with the given name, or nil.
func (c *Container) GetSection(name string) *Section {
	for i := range c.Sections {
		if c.Sections[i].Name == name {
			return &c.Sections[i]
		}
	}
	return nil
}

// GetSectionsByKind returns all sections of a given kind.
func (c *Container) GetSectionsByKind(kind SectionKind) []Section {
	result := make([]Section, 0)
	for _, s := range c.Sections {
		if s.Kind == kind {
			result = append(result, s)
		}
	}
	return result
}

// ContainerWriter writes Cowrie-GNN containers.
type ContainerWriter struct {
	w         io.Writer
	container *Container
}

// ContainerReader reads Cowrie-GNN containers.
type ContainerReader struct {
	data []byte
	pos  int
}
