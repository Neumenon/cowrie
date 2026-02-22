package glyph

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ============================================================
// LYPH v2 Patch Mode Encoder
// ============================================================
//
// Patch mode encodes a set of changes (delta) to a document:
//
//   @patch @schema#abc123 @keys=wire @target=M-123
//   = home.ft_h 2
//   = away.ft_a 1
//   + events 90' Goal{scorer=^p:smith assist=∅}
//   - odds
//   ~ home.rating +0.15
//   @end
//
// Operations:
//   =  Set (replace value at path)
//   +  Append (add to list, or add field)
//   -  Delete (remove field or list element)
//   ~  Delta (numeric increment/decrement)
//
// FID paths (v2):
//   @keys=fid -> paths use .#<fid> instead of .fieldName
//   Example: .#3.#2 instead of .home.score

// PathSeg represents a single segment in a patch path.
type PathSeg struct {
	Kind    PathSegKind
	Field   string // For PathSegField: field name (canonical internal key)
	FID     int    // For PathSegField: resolved FID (0 if unknown)
	ListIdx int    // For PathSegListIdx: list index
	MapKey  string // For PathSegMapKey: map key
}

// PathSegKind indicates the type of path segment.
type PathSegKind uint8

const (
	PathSegField   PathSegKind = iota // Struct field (.name or .#fid)
	PathSegListIdx                    // List index ([N])
	PathSegMapKey                     // Map key (["key"])
)

// FieldSeg creates a field path segment.
func FieldSeg(name string, fid int) PathSeg {
	return PathSeg{Kind: PathSegField, Field: name, FID: fid}
}

// ListIdxSeg creates a list index path segment.
func ListIdxSeg(idx int) PathSeg {
	return PathSeg{Kind: PathSegListIdx, ListIdx: idx}
}

// MapKeySeg creates a map key path segment.
func MapKeySeg(key string) PathSeg {
	return PathSeg{Kind: PathSegMapKey, MapKey: key}
}

// String returns the canonical string form of the path segment.
func (ps PathSeg) String() string {
	switch ps.Kind {
	case PathSegField:
		return ps.Field
	case PathSegListIdx:
		return fmt.Sprintf("[%d]", ps.ListIdx)
	case PathSegMapKey:
		return fmt.Sprintf("[%q]", ps.MapKey)
	default:
		return "?"
	}
}

// PatchOp represents a single patch operation.
type PatchOp struct {
	Op    PatchOpKind // =, +, -, ~
	Path  []PathSeg   // Path segments (struct field, list index, map key)
	Value *GValue     // The value (for =, +) or delta amount (for ~)
	Index int         // For list operations: -1 = append, >= 0 = specific index
}

// PatchOpKind is the type of patch operation.
type PatchOpKind rune

const (
	OpSet    PatchOpKind = '=' // Set/replace value
	OpAppend PatchOpKind = '+' // Append to list or add field
	OpDelete PatchOpKind = '-' // Delete field or list element
	OpDelta  PatchOpKind = '~' // Numeric delta
)

// String returns the operation symbol.
func (k PatchOpKind) String() string {
	return string(k)
}

// Patch represents a set of patches to apply to a target.
type Patch struct {
	Target          RefID      // Target document reference
	SchemaID        string     // Schema hash for validation
	BaseFingerprint string     // Base state fingerprint for validation (v2.4.0)
	Ops             []*PatchOp // Ordered list of operations
	TargetType      string     // Root type name for FID resolution (optional)
}

// NewPatch creates a new patch set for a target.
func NewPatch(target RefID, schemaID string) *Patch {
	return &Patch{
		Target:   target,
		SchemaID: schemaID,
		Ops:      make([]*PatchOp, 0),
	}
}

// Set adds a set operation.
func (p *Patch) Set(path string, value *GValue) *Patch {
	p.Ops = append(p.Ops, &PatchOp{
		Op:    OpSet,
		Path:  parsePathToSegs(path),
		Value: value,
	})
	return p
}

// SetWithSegs adds a set operation with pre-parsed path segments.
func (p *Patch) SetWithSegs(path []PathSeg, value *GValue) *Patch {
	p.Ops = append(p.Ops, &PatchOp{
		Op:    OpSet,
		Path:  path,
		Value: value,
	})
	return p
}

// Append adds an append operation.
func (p *Patch) Append(path string, value *GValue) *Patch {
	p.Ops = append(p.Ops, &PatchOp{
		Op:    OpAppend,
		Path:  parsePathToSegs(path),
		Value: value,
		Index: -1, // Append to end
	})
	return p
}

// Delete adds a delete operation.
func (p *Patch) Delete(path string) *Patch {
	p.Ops = append(p.Ops, &PatchOp{
		Op:   OpDelete,
		Path: parsePathToSegs(path),
	})
	return p
}

// Delta adds a numeric delta operation.
func (p *Patch) Delta(path string, amount float64) *Patch {
	p.Ops = append(p.Ops, &PatchOp{
		Op:    OpDelta,
		Path:  parsePathToSegs(path),
		Value: Float(amount),
	})
	return p
}

// InsertAt adds an insert operation at a specific index.
func (p *Patch) InsertAt(path string, index int, value *GValue) *Patch {
	p.Ops = append(p.Ops, &PatchOp{
		Op:    OpAppend,
		Path:  parsePathToSegs(path),
		Value: value,
		Index: index,
	})
	return p
}

// parsePathToSegs parses a dot-separated path into PathSeg slice.
// Supports: .fieldName, .#fid, [N], ["key"]
func parsePathToSegs(path string) []PathSeg {
	if path == "" {
		return nil
	}

	var segs []PathSeg
	i := 0
	n := len(path)

	for i < n {
		// Skip leading dots
		if path[i] == '.' {
			i++
			continue
		}

		// List index: [N]
		if path[i] == '[' {
			end := strings.IndexByte(path[i:], ']')
			if end == -1 {
				// Malformed, treat rest as field
				segs = append(segs, FieldSeg(path[i:], 0))
				break
			}
			inner := path[i+1 : i+end]
			if len(inner) > 0 && inner[0] == '"' {
				// Map key: ["key"]
				key := strings.Trim(inner, "\"")
				segs = append(segs, MapKeySeg(key))
			} else {
				// List index
				idx, _ := strconv.Atoi(inner)
				segs = append(segs, ListIdxSeg(idx))
			}
			i += end + 1
			continue
		}

		// FID reference: #N
		if path[i] == '#' {
			j := i + 1
			for j < n && path[j] >= '0' && path[j] <= '9' {
				j++
			}
			if j > i+1 {
				fid, _ := strconv.Atoi(path[i+1 : j])
				segs = append(segs, PathSeg{Kind: PathSegField, FID: fid})
			}
			i = j
			continue
		}

		// Field name: until . or [ or end
		j := i
		inQuote := false
		for j < n {
			c := path[j]
			if c == '"' {
				inQuote = !inQuote
			} else if !inQuote && (c == '.' || c == '[') {
				break
			}
			j++
		}

		if j > i {
			field := path[i:j]
			// Remove quotes if present
			if len(field) >= 2 && field[0] == '"' && field[len(field)-1] == '"' {
				field = field[1 : len(field)-1]
			}
			segs = append(segs, FieldSeg(field, 0))
		}
		i = j
	}

	return segs
}

// PatchOptions configures patch encoding.
type PatchOptions struct {
	Schema       *Schema
	KeyMode      KeyMode // How to encode path segments
	SortOps      bool    // Sort operations by path for determinism
	IndentPrefix string  // Prefix for each operation line
}

// DefaultPatchOptions returns default patch encoding options.
func DefaultPatchOptions(schema *Schema) PatchOptions {
	return PatchOptions{
		Schema:       schema,
		KeyMode:      KeyModeWire,
		SortOps:      true,
		IndentPrefix: "",
	}
}

// EmitPatch encodes a patch set.
func EmitPatch(p *Patch, schema *Schema) (string, error) {
	return EmitPatchWithOptions(p, DefaultPatchOptions(schema))
}

// EmitPatchWithOptions encodes a patch set with custom options.
func EmitPatchWithOptions(p *Patch, opts PatchOptions) (string, error) {
	if p == nil {
		return "", fmt.Errorf("nil patch set")
	}

	var buf bytes.Buffer

	// Header
	buf.WriteString("@patch")

	if p.SchemaID != "" {
		buf.WriteString(" @schema#")
		buf.WriteString(p.SchemaID)
	}

	buf.WriteString(" @keys=")
	switch opts.KeyMode {
	case KeyModeName:
		buf.WriteString("name")
	case KeyModeFID:
		buf.WriteString("fid")
	default:
		buf.WriteString("wire")
	}

	buf.WriteString(" @target=")
	buf.WriteString(canonRef(p.Target)[1:]) // Remove ^ prefix for target

	// v2.4.0: Base fingerprint for state validation
	if p.BaseFingerprint != "" {
		buf.WriteString(" @base=")
		buf.WriteString(p.BaseFingerprint)
	}

	buf.WriteByte('\n')

	// Operations
	ops := p.Ops
	if opts.SortOps {
		ops = sortPatchOps(ops, opts.KeyMode)
	}

	packOpts := PackedOptions{
		Schema:    opts.Schema,
		UseBitmap: true,
		KeyMode:   opts.KeyMode,
	}

	for _, op := range ops {
		buf.WriteString(opts.IndentPrefix)
		if err := emitPatchOp(&buf, op, opts, packOpts); err != nil {
			return "", err
		}
		buf.WriteByte('\n')
	}

	// Footer
	buf.WriteString("@end")

	return buf.String(), nil
}

// sortPatchOps returns a copy of ops sorted by path for determinism.
func sortPatchOps(ops []*PatchOp, keyMode KeyMode) []*PatchOp {
	sorted := make([]*PatchOp, len(ops))
	copy(sorted, ops)

	sort.Slice(sorted, func(i, j int) bool {
		// Compare paths using canonical string form
		pi := pathSegsToString(sorted[i].Path, keyMode)
		pj := pathSegsToString(sorted[j].Path, keyMode)
		if pi != pj {
			return pi < pj
		}
		// Same path, sort by op kind
		return sorted[i].Op < sorted[j].Op
	})

	return sorted
}

// emitPatchOp writes a single patch operation.
func emitPatchOp(out *bytes.Buffer, op *PatchOp, patchOpts PatchOptions, packOpts PackedOptions) error {
	// Operation symbol
	out.WriteRune(rune(op.Op))
	out.WriteByte(' ')

	// Path - emit according to KeyMode
	emitPathSegs(out, op.Path, patchOpts.KeyMode, patchOpts.Schema)

	// Value (for =, +, ~)
	switch op.Op {
	case OpSet, OpAppend:
		if op.Value != nil {
			out.WriteByte(' ')
			if err := emitPackedValue(out, op.Value, nil, packOpts); err != nil {
				return err
			}
		}
		// For append with index
		if op.Op == OpAppend && op.Index >= 0 {
			out.WriteString(fmt.Sprintf(" @idx=%d", op.Index))
		}

	case OpDelta:
		out.WriteByte(' ')
		if op.Value != nil && op.Value.typ == TypeFloat {
			f := op.Value.floatVal
			if f >= 0 {
				out.WriteByte('+')
			}
			out.WriteString(canonFloat(f))
		} else if op.Value != nil && op.Value.typ == TypeInt {
			n := op.Value.intVal
			if n >= 0 {
				out.WriteByte('+')
			}
			out.WriteString(canonInt(n))
		}

	case OpDelete:
		// No value needed
	}

	return nil
}

// emitPathSegs writes path segments according to KeyMode.
func emitPathSegs(out *bytes.Buffer, path []PathSeg, keyMode KeyMode, schema *Schema) {
	for i, seg := range path {
		switch seg.Kind {
		case PathSegField:
			if i > 0 {
				out.WriteByte('.')
			}
			if keyMode == KeyModeFID && seg.FID > 0 {
				// FID mode: emit .#<fid>
				out.WriteByte('#')
				out.WriteString(strconv.Itoa(seg.FID))
			} else if keyMode == KeyModeWire && schema != nil && seg.Field != "" {
				// Wire mode: use wire key if available
				// Need type context to resolve - for now just use field name
				if needsQuoting(seg.Field) {
					out.WriteString(quoteString(seg.Field))
				} else {
					out.WriteString(seg.Field)
				}
			} else {
				// Name mode or fallback
				if needsQuoting(seg.Field) {
					out.WriteString(quoteString(seg.Field))
				} else {
					out.WriteString(seg.Field)
				}
			}

		case PathSegListIdx:
			out.WriteByte('[')
			out.WriteString(strconv.Itoa(seg.ListIdx))
			out.WriteByte(']')

		case PathSegMapKey:
			out.WriteString("[\"")
			out.WriteString(seg.MapKey)
			out.WriteString("\"]")
		}
	}
}

// pathSegsToString converts path segments to canonical string for sorting.
func pathSegsToString(path []PathSeg, keyMode KeyMode) string {
	var buf bytes.Buffer
	for i, seg := range path {
		switch seg.Kind {
		case PathSegField:
			if i > 0 {
				buf.WriteByte('.')
			}
			if keyMode == KeyModeFID && seg.FID > 0 {
				buf.WriteByte('#')
				buf.WriteString(strconv.Itoa(seg.FID))
			} else {
				buf.WriteString(seg.Field)
			}
		case PathSegListIdx:
			buf.WriteByte('[')
			buf.WriteString(strconv.Itoa(seg.ListIdx))
			buf.WriteByte(']')
		case PathSegMapKey:
			buf.WriteString("[\"")
			buf.WriteString(seg.MapKey)
			buf.WriteString("\"]")
		}
	}
	return buf.String()
}

// pathSegsStr returns path as string for error messages.
func pathSegsStr(path []PathSeg) string {
	var buf bytes.Buffer
	for i, seg := range path {
		if i > 0 && seg.Kind == PathSegField {
			buf.WriteByte('.')
		}
		buf.WriteString(seg.String())
	}
	return buf.String()
}

// needsQuoting checks if a path component needs quoting.
func needsQuoting(s string) bool {
	if len(s) == 0 {
		return true
	}
	for i, r := range s {
		if i == 0 {
			if !isLetterPatch(r) && r != '_' {
				return true
			}
		} else {
			if !isLetterPatch(r) && !isDigitPatch(r) && r != '_' && r != '-' {
				return true
			}
		}
	}
	return false
}

func isLetterPatch(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

func isDigitPatch(r rune) bool {
	return r >= '0' && r <= '9'
}

// ============================================================
// Path Resolution with Schema
// ============================================================

// ResolvePathFIDs resolves FIDs for all field segments using the schema.
// This populates the FID field for each PathSegField that has a matching field.
func ResolvePathFIDs(path []PathSeg, rootType string, schema *Schema) error {
	if schema == nil || rootType == "" {
		return nil
	}

	currentType := rootType

	for i := range path {
		seg := &path[i]

		switch seg.Kind {
		case PathSegField:
			td := schema.GetType(currentType)
			if td == nil {
				return fmt.Errorf("unknown type: %s", currentType)
			}

			var fd *FieldDef
			if seg.FID > 0 {
				// Already have FID, resolve to field name
				fd = td.GetFieldByFID(seg.FID)
				if fd != nil {
					seg.Field = fd.Name
				}
			} else if seg.Field != "" {
				// Have field name, resolve to FID
				fd = td.FieldByKey(seg.Field)
				if fd != nil {
					seg.FID = fd.FID
					seg.Field = fd.Name // Normalize to canonical name
				}
			}

			if fd == nil {
				return fmt.Errorf("unknown field in %s: %s (fid=%d)", currentType, seg.Field, seg.FID)
			}

			// Update current type for next segment
			if fd.Type.Kind == TypeSpecRef {
				currentType = fd.Type.Name
			} else {
				currentType = "" // Can't navigate further into primitives
			}

		case PathSegListIdx:
			// For list, get element type
			td := schema.GetType(currentType)
			if td != nil {
				for _, fd := range td.Struct.Fields {
					if fd.Type.Kind == TypeSpecList && fd.Type.Elem != nil {
						if fd.Type.Elem.Kind == TypeSpecRef {
							currentType = fd.Type.Elem.Name
						}
					}
				}
			}

		case PathSegMapKey:
			// For maps, type context is lost for now
			currentType = ""
		}
	}

	return nil
}

// ============================================================
// Patch Application
// ============================================================

// ApplyPatch applies a patch set to a value and returns the modified copy.
func ApplyPatch(v *GValue, p *Patch) (*GValue, error) {
	if v == nil {
		return nil, fmt.Errorf("cannot apply patch to nil value")
	}

	// Deep copy the value
	result := deepCopy(v)

	for _, op := range p.Ops {
		var err error
		result, err = applyOp(result, op)
		if err != nil {
			return nil, fmt.Errorf("patch op %s %s: %w", op.Op, pathSegsStr(op.Path), err)
		}
	}

	return result, nil
}

// applyOp applies a single operation to a value.
func applyOp(v *GValue, op *PatchOp) (*GValue, error) {
	if len(op.Path) == 0 {
		// Root-level operation
		switch op.Op {
		case OpSet:
			return op.Value, nil
		default:
			return nil, fmt.Errorf("cannot apply %s to root", op.Op)
		}
	}

	// Navigate to parent, apply at leaf
	return applyAtPathSegs(v, op.Path, op)
}

// applyAtPathSegs navigates to a path and applies the operation.
func applyAtPathSegs(v *GValue, path []PathSeg, op *PatchOp) (*GValue, error) {
	if len(path) == 1 {
		// We're at the parent, apply to this level
		return applyToParentSeg(v, path[0], op)
	}

	// Navigate deeper
	seg := path[0]
	rest := path[1:]

	switch seg.Kind {
	case PathSegField:
		key := seg.Field
		if v.typ != TypeStruct {
			return nil, fmt.Errorf("cannot navigate into %s with field", v.typ)
		}
		for i, f := range v.structVal.Fields {
			if f.Key == key {
				newChild, err := applyAtPathSegs(f.Value, rest, op)
				if err != nil {
					return nil, err
				}
				v.structVal.Fields[i].Value = newChild
				return v, nil
			}
		}
		return nil, fmt.Errorf("field not found: %s", key)

	case PathSegListIdx:
		if v.typ != TypeList {
			return nil, fmt.Errorf("cannot index into %s", v.typ)
		}
		idx := seg.ListIdx
		if idx < 0 || idx >= len(v.listVal) {
			return nil, fmt.Errorf("index out of bounds: %d", idx)
		}
		newChild, err := applyAtPathSegs(v.listVal[idx], rest, op)
		if err != nil {
			return nil, err
		}
		v.listVal[idx] = newChild
		return v, nil

	case PathSegMapKey:
		if v.typ != TypeMap {
			return nil, fmt.Errorf("cannot access map key in %s", v.typ)
		}
		key := seg.MapKey
		for i, e := range v.mapVal {
			if e.Key == key {
				newChild, err := applyAtPathSegs(e.Value, rest, op)
				if err != nil {
					return nil, err
				}
				v.mapVal[i].Value = newChild
				return v, nil
			}
		}
		return nil, fmt.Errorf("key not found: %s", key)

	default:
		return nil, fmt.Errorf("unknown path segment kind")
	}
}

// applyToParentSeg applies an operation to a field/key of the parent value.
func applyToParentSeg(v *GValue, seg PathSeg, op *PatchOp) (*GValue, error) {
	key := seg.Field
	if seg.Kind == PathSegMapKey {
		key = seg.MapKey
	}

	switch op.Op {
	case OpSet:
		v.Set(key, op.Value)
		return v, nil

	case OpAppend:
		existing := v.Get(key)
		if existing == nil {
			// Create new list with the value
			v.Set(key, List(op.Value))
		} else if existing.typ == TypeList {
			if op.Index >= 0 && op.Index <= len(existing.listVal) {
				// Insert at index
				newList := make([]*GValue, 0, len(existing.listVal)+1)
				newList = append(newList, existing.listVal[:op.Index]...)
				newList = append(newList, op.Value)
				newList = append(newList, existing.listVal[op.Index:]...)
				existing.listVal = newList
			} else {
				// Append
				existing.Append(op.Value)
			}
		} else {
			return nil, fmt.Errorf("cannot append to %s", existing.typ)
		}
		return v, nil

	case OpDelete:
		switch v.typ {
		case TypeStruct:
			newFields := make([]MapEntry, 0, len(v.structVal.Fields))
			for _, f := range v.structVal.Fields {
				if f.Key != key {
					newFields = append(newFields, f)
				}
			}
			v.structVal.Fields = newFields
		case TypeMap:
			newEntries := make([]MapEntry, 0, len(v.mapVal))
			for _, e := range v.mapVal {
				if e.Key != key {
					newEntries = append(newEntries, e)
				}
			}
			v.mapVal = newEntries
		default:
			return nil, fmt.Errorf("cannot delete from %s", v.typ)
		}
		return v, nil

	case OpDelta:
		existing := v.Get(key)
		if existing == nil {
			return nil, fmt.Errorf("field not found for delta: %s", key)
		}

		delta, ok := op.Value.Number()
		if !ok {
			return nil, fmt.Errorf("delta value must be numeric")
		}

		switch existing.typ {
		case TypeInt:
			existing.intVal += int64(delta)
		case TypeFloat:
			existing.floatVal += delta
		default:
			return nil, fmt.Errorf("cannot apply delta to %s", existing.typ)
		}
		return v, nil

	default:
		return nil, fmt.Errorf("unknown operation: %s", op.Op)
	}
}

// deepCopy creates a deep copy of a GValue.
func deepCopy(v *GValue) *GValue {
	if v == nil {
		return nil
	}

	cp := &GValue{
		typ:      v.typ,
		boolVal:  v.boolVal,
		intVal:   v.intVal,
		floatVal: v.floatVal,
		strVal:   v.strVal,
		idVal:    v.idVal,
		timeVal:  v.timeVal,
		pos:      v.pos,
	}

	// Deep copy bytes
	if v.bytesVal != nil {
		cp.bytesVal = make([]byte, len(v.bytesVal))
		copy(cp.bytesVal, v.bytesVal)
	}

	// Deep copy list
	if v.listVal != nil {
		cp.listVal = make([]*GValue, len(v.listVal))
		for i, elem := range v.listVal {
			cp.listVal[i] = deepCopy(elem)
		}
	}

	// Deep copy map
	if v.mapVal != nil {
		cp.mapVal = make([]MapEntry, len(v.mapVal))
		for i, e := range v.mapVal {
			cp.mapVal[i] = MapEntry{
				Key:   e.Key,
				Value: deepCopy(e.Value),
			}
		}
	}

	// Deep copy struct
	if v.structVal != nil {
		cp.structVal = &StructValue{
			TypeName: v.structVal.TypeName,
			Fields:   make([]MapEntry, len(v.structVal.Fields)),
		}
		for i, f := range v.structVal.Fields {
			cp.structVal.Fields[i] = MapEntry{
				Key:   f.Key,
				Value: deepCopy(f.Value),
			}
		}
	}

	// Deep copy sum
	if v.sumVal != nil {
		cp.sumVal = &SumValue{
			Tag:   v.sumVal.Tag,
			Value: deepCopy(v.sumVal.Value),
		}
	}

	return cp
}

// ============================================================
// Patch Builder (Fluent API)
// ============================================================

// PatchBuilder provides a fluent API for building patches.
type PatchBuilder struct {
	patch  *Patch
	schema *Schema
}

// NewPatchBuilder creates a new patch builder.
func NewPatchBuilder(target RefID) *PatchBuilder {
	return &PatchBuilder{
		patch: NewPatch(target, ""),
	}
}

// WithSchema sets the schema for FID resolution.
func (pb *PatchBuilder) WithSchema(schema *Schema) *PatchBuilder {
	pb.schema = schema
	if schema != nil && schema.Hash != "" {
		pb.patch.SchemaID = schema.Hash
	}
	return pb
}

// WithSchemaID sets the schema ID for validation.
func (pb *PatchBuilder) WithSchemaID(schemaID string) *PatchBuilder {
	pb.patch.SchemaID = schemaID
	return pb
}

// WithTargetType sets the root type for FID resolution.
func (pb *PatchBuilder) WithTargetType(typeName string) *PatchBuilder {
	pb.patch.TargetType = typeName
	return pb
}

// WithBaseFingerprint sets the base state fingerprint for validation.
// The fingerprint should be the first 16 chars of the SHA-256 hash
// of the canonical form of the base state.
func (pb *PatchBuilder) WithBaseFingerprint(fingerprint string) *PatchBuilder {
	pb.patch.BaseFingerprint = fingerprint
	return pb
}

// WithBaseValue computes and sets the base fingerprint from a GValue.
// Uses the SHA-256 hash of the loose canonical form.
func (pb *PatchBuilder) WithBaseValue(base *GValue) *PatchBuilder {
	// Compute hash of canonical form
	canonical := CanonicalizeLoose(base)
	hash := sha256.Sum256([]byte(canonical))
	// Use first 16 chars of hex
	pb.patch.BaseFingerprint = hex.EncodeToString(hash[:])[:16]
	return pb
}

// Set adds a set operation.
func (pb *PatchBuilder) Set(path string, value *GValue) *PatchBuilder {
	pb.patch.Set(path, value)
	return pb
}

// SetFID adds a set operation using FID path segments.
func (pb *PatchBuilder) SetFID(path []PathSeg, value *GValue) *PatchBuilder {
	pb.patch.SetWithSegs(path, value)
	return pb
}

// Append adds an append operation.
func (pb *PatchBuilder) Append(path string, value *GValue) *PatchBuilder {
	pb.patch.Append(path, value)
	return pb
}

// Delete adds a delete operation.
func (pb *PatchBuilder) Delete(path string) *PatchBuilder {
	pb.patch.Delete(path)
	return pb
}

// Delta adds a delta operation.
func (pb *PatchBuilder) Delta(path string, amount float64) *PatchBuilder {
	pb.patch.Delta(path, amount)
	return pb
}

// Build returns the completed patch set.
func (pb *PatchBuilder) Build() *Patch {
	return pb.patch
}

// ============================================================
// Diff Generation
// ============================================================

// Diff computes the patch set needed to transform 'from' into 'to'.
func Diff(from, to *GValue, typeName string) *Patch {
	p := NewPatch(RefID{}, "")
	p.TargetType = typeName
	diffValues(from, to, nil, p)
	return p
}

// diffValues recursively computes differences.
func diffValues(from, to *GValue, path []PathSeg, p *Patch) {
	// Handle nil cases
	if from == nil && to == nil {
		return
	}
	if from == nil {
		p.Ops = append(p.Ops, &PatchOp{
			Op:    OpSet,
			Path:  copyPath(path),
			Value: to,
		})
		return
	}
	if to == nil {
		if len(path) > 0 {
			p.Ops = append(p.Ops, &PatchOp{
				Op:   OpDelete,
				Path: copyPath(path),
			})
		}
		return
	}

	// Type mismatch: replace
	if from.typ != to.typ {
		p.Ops = append(p.Ops, &PatchOp{
			Op:    OpSet,
			Path:  copyPath(path),
			Value: to,
		})
		return
	}

	// Same type, compare values
	switch from.typ {
	case TypeNull:
		// Both null, no change

	case TypeBool:
		if from.boolVal != to.boolVal {
			p.Ops = append(p.Ops, &PatchOp{
				Op:    OpSet,
				Path:  copyPath(path),
				Value: to,
			})
		}

	case TypeInt:
		if from.intVal != to.intVal {
			p.Ops = append(p.Ops, &PatchOp{
				Op:    OpSet,
				Path:  copyPath(path),
				Value: to,
			})
		}

	case TypeFloat:
		if from.floatVal != to.floatVal {
			p.Ops = append(p.Ops, &PatchOp{
				Op:    OpSet,
				Path:  copyPath(path),
				Value: to,
			})
		}

	case TypeStr:
		if from.strVal != to.strVal {
			p.Ops = append(p.Ops, &PatchOp{
				Op:    OpSet,
				Path:  copyPath(path),
				Value: to,
			})
		}

	case TypeID:
		if from.idVal != to.idVal {
			p.Ops = append(p.Ops, &PatchOp{
				Op:    OpSet,
				Path:  copyPath(path),
				Value: to,
			})
		}

	case TypeStruct:
		diffStructValues(from, to, path, p)

	case TypeMap:
		diffMapValues(from, to, path, p)

	case TypeList:
		// For now, just replace if different
		if !listsEqual(from.listVal, to.listVal) {
			p.Ops = append(p.Ops, &PatchOp{
				Op:    OpSet,
				Path:  copyPath(path),
				Value: to,
			})
		}

	default:
		// Other types: replace if not equal
		p.Ops = append(p.Ops, &PatchOp{
			Op:    OpSet,
			Path:  copyPath(path),
			Value: to,
		})
	}
}

// copyPath creates a copy of the path slice.
func copyPath(path []PathSeg) []PathSeg {
	if path == nil {
		return nil
	}
	cp := make([]PathSeg, len(path))
	copy(cp, path)
	return cp
}

// diffStructValues computes differences between two structs.
func diffStructValues(from, to *GValue, path []PathSeg, p *Patch) {
	fromFields := make(map[string]*GValue)
	for _, f := range from.structVal.Fields {
		fromFields[f.Key] = f.Value
	}

	toFields := make(map[string]*GValue)
	for _, f := range to.structVal.Fields {
		toFields[f.Key] = f.Value
	}

	// Check for changed/added fields
	for key, toVal := range toFields {
		fromVal := fromFields[key]
		childPath := append(copyPath(path), FieldSeg(key, 0))
		diffValues(fromVal, toVal, childPath, p)
	}

	// Check for deleted fields
	for key := range fromFields {
		if _, exists := toFields[key]; !exists {
			childPath := append(copyPath(path), FieldSeg(key, 0))
			p.Ops = append(p.Ops, &PatchOp{
				Op:   OpDelete,
				Path: childPath,
			})
		}
	}
}

// diffMapValues computes differences between two maps.
func diffMapValues(from, to *GValue, path []PathSeg, p *Patch) {
	fromMap := make(map[string]*GValue)
	for _, e := range from.mapVal {
		fromMap[e.Key] = e.Value
	}

	toMap := make(map[string]*GValue)
	for _, e := range to.mapVal {
		toMap[e.Key] = e.Value
	}

	for key, toVal := range toMap {
		fromVal := fromMap[key]
		childPath := append(copyPath(path), MapKeySeg(key))
		diffValues(fromVal, toVal, childPath, p)
	}

	for key := range fromMap {
		if _, exists := toMap[key]; !exists {
			childPath := append(copyPath(path), MapKeySeg(key))
			p.Ops = append(p.Ops, &PatchOp{
				Op:   OpDelete,
				Path: childPath,
			})
		}
	}
}

// listsEqual checks if two lists are deeply equal.
func listsEqual(a, b []*GValue) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !valuesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

// valuesEqual checks if two values are deeply equal.
func valuesEqual(a, b *GValue) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.typ != b.typ {
		return false
	}

	switch a.typ {
	case TypeNull:
		return true
	case TypeBool:
		return a.boolVal == b.boolVal
	case TypeInt:
		return a.intVal == b.intVal
	case TypeFloat:
		return a.floatVal == b.floatVal
	case TypeStr:
		return a.strVal == b.strVal
	case TypeID:
		return a.idVal == b.idVal
	case TypeList:
		return listsEqual(a.listVal, b.listVal)
	case TypeStruct:
		if a.structVal.TypeName != b.structVal.TypeName {
			return false
		}
		if len(a.structVal.Fields) != len(b.structVal.Fields) {
			return false
		}
		aFields := make(map[string]*GValue)
		for _, f := range a.structVal.Fields {
			aFields[f.Key] = f.Value
		}
		for _, f := range b.structVal.Fields {
			if !valuesEqual(aFields[f.Key], f.Value) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
