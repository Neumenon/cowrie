package glyph

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// Schema represents a GLYPH schema definition.
type Schema struct {
	Types map[string]*TypeDef // Type name → definition
	Hash  string              // SHA256 hash of canonical schema text
}

// TypeDef represents a type definition in a schema.
type TypeDef struct {
	Name    string // Type name (e.g., "Match", "Team")
	Version string // Optional version (e.g., "v1", "v1.2")
	Kind    TypeDefKind
	Struct  *StructDef // For Kind == TypeDefStruct
	Sum     *SumDef    // For Kind == TypeDefSum

	// v2 extensions
	PackEnabled bool // @pack: enable packed encoding for this struct
	TabEnabled  bool // @tab: enable tabular encoding for list<this> (default true)
	Open        bool // @open: accept unknown fields (stored in @unknown map)
}

// TypeDefKind indicates whether a type is a struct or sum.
type TypeDefKind uint8

const (
	TypeDefStruct TypeDefKind = iota
	TypeDefSum
)

// StructDef represents a struct type definition.
type StructDef struct {
	Fields []*FieldDef
}

// SumDef represents a sum (tagged union) type definition.
type SumDef struct {
	Variants []*VariantDef
}

// VariantDef represents a variant in a sum type.
type VariantDef struct {
	Tag  string   // Variant tag name
	Type TypeSpec // The wrapped type
}

// FieldDef represents a field in a struct.
type FieldDef struct {
	Name        string       // Field name
	Type        TypeSpec     // Field type
	Constraints []Constraint // Validation constraints
	WireKey     string       // Short key for wire format (@k annotation)
	Optional    bool         // Whether field is optional
	Default     *GValue      // Default value if any

	// v2 extensions
	FID      int    // Stable field ID for packed encoding (1+, never reuse)
	KeepNull bool   // Emit null in packed even if optional
	Codec    string // Encoding hint: "dict", "enum", "int", "f32", etc.
}

// TypeSpec represents a type reference.
type TypeSpec struct {
	Kind    TypeSpecKind
	Name    string     // For Kind == TypeSpecRef (reference to named type)
	Elem    *TypeSpec  // For Kind == TypeSpecList
	KeyType *TypeSpec  // For Kind == TypeSpecMap
	ValType *TypeSpec  // For Kind == TypeSpecMap
	Struct  *StructDef // For Kind == TypeSpecInlineStruct
}

// TypeSpecKind indicates the kind of type specification.
type TypeSpecKind uint8

const (
	TypeSpecNull TypeSpecKind = iota
	TypeSpecBool
	TypeSpecInt
	TypeSpecFloat
	TypeSpecStr
	TypeSpecBytes
	TypeSpecTime
	TypeSpecID
	TypeSpecList         // list<T>
	TypeSpecMap          // map<K, V>
	TypeSpecRef          // Reference to named type
	TypeSpecInlineStruct // Inline struct{...}
)

// String returns the type spec as a string.
func (ts TypeSpec) String() string {
	switch ts.Kind {
	case TypeSpecNull:
		return "null"
	case TypeSpecBool:
		return "bool"
	case TypeSpecInt:
		return "int"
	case TypeSpecFloat:
		return "float"
	case TypeSpecStr:
		return "str"
	case TypeSpecBytes:
		return "bytes"
	case TypeSpecTime:
		return "time"
	case TypeSpecID:
		return "id"
	case TypeSpecList:
		return "list<" + ts.Elem.String() + ">"
	case TypeSpecMap:
		return "map<" + ts.KeyType.String() + "," + ts.ValType.String() + ">"
	case TypeSpecRef:
		return ts.Name
	case TypeSpecInlineStruct:
		return "struct{...}"
	default:
		return "unknown"
	}
}

// ============================================================
// Constraints
// ============================================================

// Constraint represents a validation constraint.
type Constraint struct {
	Kind  ConstraintKind
	Value interface{} // Type depends on Kind
}

// ConstraintKind indicates the type of constraint.
type ConstraintKind uint8

const (
	ConstraintMin      ConstraintKind = iota // min=N (numeric)
	ConstraintMax                            // max=N (numeric)
	ConstraintMinLen                         // len>=N (string/list/bytes)
	ConstraintMaxLen                         // len<=N (string/list/bytes)
	ConstraintLen                            // len=N (exact length)
	ConstraintRegex                          // regex="pattern"
	ConstraintEnum                           // enum=["a","b","c"]
	ConstraintNonEmpty                       // nonempty
	ConstraintUnique                         // unique (list elements)
	ConstraintRange                          // range=[min,max]
	ConstraintOptional                       // optional (field may be omitted)
)

// String returns the constraint as a string.
func (c Constraint) String() string {
	switch c.Kind {
	case ConstraintMin:
		return fmt.Sprintf("min=%v", c.Value)
	case ConstraintMax:
		return fmt.Sprintf("max=%v", c.Value)
	case ConstraintMinLen:
		return fmt.Sprintf("len>=%v", c.Value)
	case ConstraintMaxLen:
		return fmt.Sprintf("len<=%v", c.Value)
	case ConstraintLen:
		return fmt.Sprintf("len=%v", c.Value)
	case ConstraintRegex:
		return fmt.Sprintf("regex=%q", c.Value)
	case ConstraintEnum:
		return fmt.Sprintf("enum=%v", c.Value)
	case ConstraintNonEmpty:
		return "nonempty"
	case ConstraintUnique:
		return "unique"
	case ConstraintRange:
		r := c.Value.([2]float64)
		return fmt.Sprintf("[%v..%v]", r[0], r[1])
	case ConstraintOptional:
		return "optional"
	default:
		return "unknown"
	}
}

// ============================================================
// Schema Methods
// ============================================================

// GetType returns a type definition by name.
func (s *Schema) GetType(name string) *TypeDef {
	if s == nil || s.Types == nil {
		return nil
	}
	return s.Types[name]
}

// GetField returns a field definition from a struct type.
func (s *Schema) GetField(typeName, fieldName string) *FieldDef {
	td := s.GetType(typeName)
	if td == nil || td.Kind != TypeDefStruct || td.Struct == nil {
		return nil
	}
	for _, f := range td.Struct.Fields {
		if f.Name == fieldName || f.WireKey == fieldName {
			return f
		}
	}
	return nil
}

// ResolveWireKey resolves a wire key to the full field name.
func (s *Schema) ResolveWireKey(typeName, wireKey string) string {
	td := s.GetType(typeName)
	if td == nil || td.Kind != TypeDefStruct || td.Struct == nil {
		return wireKey
	}
	for _, f := range td.Struct.Fields {
		if f.WireKey == wireKey {
			return f.Name
		}
	}
	return wireKey
}

// GetWireKey returns the wire key for a field, or the field name if no wire key.
func (s *Schema) GetWireKey(typeName, fieldName string) string {
	f := s.GetField(typeName, fieldName)
	if f != nil && f.WireKey != "" {
		return f.WireKey
	}
	return fieldName
}

// ComputeHash computes and sets the schema hash.
func (s *Schema) ComputeHash() string {
	canonical := s.Canonical()
	hash := sha256.Sum256([]byte(canonical))
	s.Hash = hex.EncodeToString(hash[:16]) // First 16 bytes = 32 hex chars
	return s.Hash
}

// Canonical returns the canonical schema text.
func (s *Schema) Canonical() string {
	var sb strings.Builder
	sb.WriteString("@schema{\n")

	// Sort type names for deterministic output
	names := make([]string, 0, len(s.Types))
	for name := range s.Types {
		names = append(names, name)
	}
	sortStrings(names)

	for _, name := range names {
		td := s.Types[name]
		writeTypeDef(&sb, td)
	}

	sb.WriteString("}")
	return sb.String()
}

func writeTypeDef(sb *strings.Builder, td *TypeDef) {
	sb.WriteString("  ")
	sb.WriteString(td.Name)
	if td.Version != "" {
		sb.WriteString(":")
		sb.WriteString(td.Version)
	}
	sb.WriteString(" ")

	switch td.Kind {
	case TypeDefStruct:
		if td.Open {
			sb.WriteString("@open ")
		}
		sb.WriteString("struct{\n")
		for _, f := range td.Struct.Fields {
			sb.WriteString("    ")
			sb.WriteString(f.Name)
			sb.WriteString(": ")
			sb.WriteString(f.Type.String())
			for _, c := range f.Constraints {
				sb.WriteString(" [")
				sb.WriteString(c.String())
				sb.WriteString("]")
			}
			if f.WireKey != "" {
				sb.WriteString(" @k(")
				sb.WriteString(f.WireKey)
				sb.WriteString(")")
			}
			if f.Optional {
				sb.WriteString(" [optional]")
			}
			sb.WriteString("\n")
		}
		sb.WriteString("  }\n")
	case TypeDefSum:
		sb.WriteString("sum{\n")
		for i, v := range td.Sum.Variants {
			sb.WriteString("    ")
			sb.WriteString(v.Tag)
			sb.WriteString(": ")
			sb.WriteString(v.Type.String())
			if i < len(td.Sum.Variants)-1 {
				sb.WriteString(" |")
			}
			sb.WriteString("\n")
		}
		sb.WriteString("  }\n")
	}
}

// Simple string sort
func sortStrings(s []string) {
	for i := 0; i < len(s); i++ {
		for j := i + 1; j < len(s); j++ {
			if s[j] < s[i] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

// ============================================================
// v2 Schema Helpers
// ============================================================

// FieldsByFID returns struct fields sorted by FID ascending.
// Fields with FID=0 are sorted after all FID > 0 fields, by name.
func (td *TypeDef) FieldsByFID() []*FieldDef {
	if td.Kind != TypeDefStruct || td.Struct == nil {
		return nil
	}

	fields := make([]*FieldDef, len(td.Struct.Fields))
	copy(fields, td.Struct.Fields)

	// Sort by FID, then by name for FID=0
	for i := 0; i < len(fields); i++ {
		for j := i + 1; j < len(fields); j++ {
			fi, fj := fields[i], fields[j]
			swap := false

			if fi.FID == 0 && fj.FID > 0 {
				swap = true // FID > 0 comes first
			} else if fi.FID > 0 && fj.FID == 0 {
				swap = false
			} else if fi.FID == 0 && fj.FID == 0 {
				swap = fi.Name > fj.Name // alphabetical for FID=0
			} else {
				swap = fi.FID > fj.FID // ascending FID
			}

			if swap {
				fields[i], fields[j] = fields[j], fields[i]
			}
		}
	}

	return fields
}

// RequiredFieldsByFID returns required (non-optional) fields sorted by FID.
func (td *TypeDef) RequiredFieldsByFID() []*FieldDef {
	all := td.FieldsByFID()
	result := make([]*FieldDef, 0, len(all))
	for _, f := range all {
		if !f.Optional {
			result = append(result, f)
		}
	}
	return result
}

// OptionalFieldsByFID returns optional fields sorted by FID.
// Used for bitmap mask computation.
func (td *TypeDef) OptionalFieldsByFID() []*FieldDef {
	all := td.FieldsByFID()
	result := make([]*FieldDef, 0, len(all))
	for _, f := range all {
		if f.Optional {
			result = append(result, f)
		}
	}
	return result
}

// AssignFIDs assigns FIDs to all fields that don't have one.
// FIDs are assigned sequentially starting from 1 in field order.
func (td *TypeDef) AssignFIDs() {
	if td.Kind != TypeDefStruct || td.Struct == nil {
		return
	}

	nextFID := 1
	// First pass: find max existing FID
	for _, f := range td.Struct.Fields {
		if f.FID >= nextFID {
			nextFID = f.FID + 1
		}
	}

	// Second pass: assign to fields without FID
	for _, f := range td.Struct.Fields {
		if f.FID == 0 {
			f.FID = nextFID
			nextFID++
		}
	}
}

// GetFieldByFID returns a field by its FID.
func (td *TypeDef) GetFieldByFID(fid int) *FieldDef {
	if td.Kind != TypeDefStruct || td.Struct == nil {
		return nil
	}
	for _, f := range td.Struct.Fields {
		if f.FID == fid {
			return f
		}
	}
	return nil
}

// FieldByKey resolves a field by wire key or field name.
// Wire keys are checked first, then field names.
// This is used for parsing paths that may use either format.
func (td *TypeDef) FieldByKey(key string) *FieldDef {
	if td.Kind != TypeDefStruct || td.Struct == nil {
		return nil
	}
	// First pass: check wire keys
	for _, f := range td.Struct.Fields {
		if f.WireKey != "" && f.WireKey == key {
			return f
		}
	}
	// Second pass: check field names
	for _, f := range td.Struct.Fields {
		if f.Name == key {
			return f
		}
	}
	return nil
}

// GetFIDForField returns the FID for a field by name or wire key.
// Returns 0 if the field is not found or has no FID.
func (td *TypeDef) GetFIDForField(key string) int {
	fd := td.FieldByKey(key)
	if fd == nil {
		return 0
	}
	return fd.FID
}

// ============================================================
// Constraint Helpers
// ============================================================

// MinConstraint creates a min constraint.
func MinConstraint(v float64) Constraint {
	return Constraint{Kind: ConstraintMin, Value: v}
}

// MaxConstraint creates a max constraint.
func MaxConstraint(v float64) Constraint {
	return Constraint{Kind: ConstraintMax, Value: v}
}

// RangeConstraint creates a range constraint.
func RangeConstraint(min, max float64) Constraint {
	return Constraint{Kind: ConstraintRange, Value: [2]float64{min, max}}
}

// LenConstraint creates an exact length constraint.
func LenConstraint(n int) Constraint {
	return Constraint{Kind: ConstraintLen, Value: n}
}

// MaxLenConstraint creates a max length constraint.
func MaxLenConstraint(n int) Constraint {
	return Constraint{Kind: ConstraintMaxLen, Value: n}
}

// MinLenConstraint creates a min length constraint.
func MinLenConstraint(n int) Constraint {
	return Constraint{Kind: ConstraintMinLen, Value: n}
}

// RegexConstraint creates a regex constraint.
func RegexConstraint(pattern string) Constraint {
	return Constraint{Kind: ConstraintRegex, Value: pattern}
}

// EnumConstraint creates an enum constraint.
func EnumConstraint(values []string) Constraint {
	return Constraint{Kind: ConstraintEnum, Value: values}
}

// NonEmptyConstraint creates a non-empty constraint.
func NonEmptyConstraint() Constraint {
	return Constraint{Kind: ConstraintNonEmpty}
}

// OptionalConstraint creates an optional constraint.
func OptionalConstraint() Constraint {
	return Constraint{Kind: ConstraintOptional}
}

// ============================================================
// Type Spec Helpers
// ============================================================

// PrimitiveType returns a TypeSpec for a primitive type name.
func PrimitiveType(name string) TypeSpec {
	switch name {
	case "null":
		return TypeSpec{Kind: TypeSpecNull}
	case "bool":
		return TypeSpec{Kind: TypeSpecBool}
	case "int":
		return TypeSpec{Kind: TypeSpecInt}
	case "float":
		return TypeSpec{Kind: TypeSpecFloat}
	case "str", "string":
		return TypeSpec{Kind: TypeSpecStr}
	case "bytes":
		return TypeSpec{Kind: TypeSpecBytes}
	case "time":
		return TypeSpec{Kind: TypeSpecTime}
	case "id":
		return TypeSpec{Kind: TypeSpecID}
	default:
		return TypeSpec{Kind: TypeSpecRef, Name: name}
	}
}

// ListType returns a list type spec.
func ListType(elem TypeSpec) TypeSpec {
	return TypeSpec{Kind: TypeSpecList, Elem: &elem}
}

// MapType returns a map type spec.
func MapType(key, val TypeSpec) TypeSpec {
	return TypeSpec{Kind: TypeSpecMap, KeyType: &key, ValType: &val}
}

// RefType returns a reference to a named type.
func RefType(name string) TypeSpec {
	return TypeSpec{Kind: TypeSpecRef, Name: name}
}

// ============================================================
// Schema Builder
// ============================================================

// SchemaBuilder helps construct schemas programmatically.
type SchemaBuilder struct {
	schema *Schema
}

// NewSchemaBuilder creates a new schema builder.
func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		schema: &Schema{Types: make(map[string]*TypeDef)},
	}
}

// TypeOption is a function that modifies a type definition.
type TypeOption func(*TypeDef)

// WithPack enables packed encoding for a struct type.
func WithPack() TypeOption {
	return func(td *TypeDef) {
		td.PackEnabled = true
	}
}

// WithTab enables tabular encoding for lists of this type.
func WithTab() TypeOption {
	return func(td *TypeDef) {
		td.TabEnabled = true
	}
}

// WithOpen marks a struct as open (accepts unknown fields).
func WithOpen() TypeOption {
	return func(td *TypeDef) {
		td.Open = true
	}
}

// AddStruct adds a struct type definition.
func (b *SchemaBuilder) AddStruct(name string, version string, fields ...*FieldDef) *SchemaBuilder {
	td := &TypeDef{
		Name:       name,
		Version:    version,
		Kind:       TypeDefStruct,
		Struct:     &StructDef{Fields: fields},
		TabEnabled: true, // default true for tabular
	}
	b.schema.Types[name] = td
	return b
}

// AddPackedStruct adds a struct type definition with packed encoding enabled.
func (b *SchemaBuilder) AddPackedStruct(name string, version string, fields ...*FieldDef) *SchemaBuilder {
	td := &TypeDef{
		Name:        name,
		Version:     version,
		Kind:        TypeDefStruct,
		Struct:      &StructDef{Fields: fields},
		PackEnabled: true,
		TabEnabled:  true,
	}
	// Auto-assign FIDs if not set
	td.AssignFIDs()
	b.schema.Types[name] = td
	return b
}

// AddOpenStruct adds a struct type definition that accepts unknown fields.
func (b *SchemaBuilder) AddOpenStruct(name string, version string, fields ...*FieldDef) *SchemaBuilder {
	td := &TypeDef{
		Name:       name,
		Version:    version,
		Kind:       TypeDefStruct,
		Struct:     &StructDef{Fields: fields},
		Open:       true,
		TabEnabled: true,
	}
	b.schema.Types[name] = td
	return b
}

// AddOpenPackedStruct adds a struct with both packed encoding and open fields.
func (b *SchemaBuilder) AddOpenPackedStruct(name string, version string, fields ...*FieldDef) *SchemaBuilder {
	td := &TypeDef{
		Name:        name,
		Version:     version,
		Kind:        TypeDefStruct,
		Struct:      &StructDef{Fields: fields},
		PackEnabled: true,
		TabEnabled:  true,
		Open:        true,
	}
	td.AssignFIDs()
	b.schema.Types[name] = td
	return b
}

// AddSum adds a sum type definition.
func (b *SchemaBuilder) AddSum(name string, version string, variants ...*VariantDef) *SchemaBuilder {
	b.schema.Types[name] = &TypeDef{
		Name:    name,
		Version: version,
		Kind:    TypeDefSum,
		Sum:     &SumDef{Variants: variants},
	}
	return b
}

// Build finalizes and returns the schema.
func (b *SchemaBuilder) Build() *Schema {
	b.schema.ComputeHash()
	return b.schema
}

// WithPack enables packed encoding for a type by name.
func (b *SchemaBuilder) WithPack(typeName string) *SchemaBuilder {
	if td, ok := b.schema.Types[typeName]; ok {
		td.PackEnabled = true
	}
	return b
}

// WithTab enables tabular encoding for lists of a type by name.
func (b *SchemaBuilder) WithTab(typeName string) *SchemaBuilder {
	if td, ok := b.schema.Types[typeName]; ok {
		td.TabEnabled = true
	}
	return b
}

// WithOpen marks a type as open (accepts unknown fields) by name.
func (b *SchemaBuilder) WithOpen(typeName string) *SchemaBuilder {
	if td, ok := b.schema.Types[typeName]; ok {
		td.Open = true
	}
	return b
}

// Field creates a field definition.
func Field(name string, typ TypeSpec, opts ...FieldOption) *FieldDef {
	f := &FieldDef{Name: name, Type: typ}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// FieldOption is a function that modifies a field definition.
type FieldOption func(*FieldDef)

// WithWireKey sets the wire key for a field.
func WithWireKey(key string) FieldOption {
	return func(f *FieldDef) {
		f.WireKey = key
	}
}

// WithOptional marks a field as optional.
func WithOptional() FieldOption {
	return func(f *FieldDef) {
		f.Optional = true
	}
}

// WithConstraint adds a constraint to a field.
func WithConstraint(c Constraint) FieldOption {
	return func(f *FieldDef) {
		f.Constraints = append(f.Constraints, c)
	}
}

// WithDefault sets a default value for a field.
func WithDefault(v *GValue) FieldOption {
	return func(f *FieldDef) {
		f.Default = v
	}
}

// WithFID sets the stable field ID for packed encoding.
func WithFID(fid int) FieldOption {
	return func(f *FieldDef) {
		f.FID = fid
	}
}

// WithKeepNull marks a field to emit null even if optional.
func WithKeepNull() FieldOption {
	return func(f *FieldDef) {
		f.KeepNull = true
	}
}

// WithCodec sets the encoding hint for a field.
func WithCodec(codec string) FieldOption {
	return func(f *FieldDef) {
		f.Codec = codec
	}
}

// Variant creates a variant definition for a sum type.
func Variant(tag string, typ TypeSpec) *VariantDef {
	return &VariantDef{Tag: tag, Type: typ}
}

// ============================================================
// Schema Validation Helpers
// ============================================================

// CompiledConstraint holds a pre-compiled constraint for fast validation.
type CompiledConstraint struct {
	Constraint Constraint
	Regex      *regexp.Regexp  // Compiled regex if Kind == ConstraintRegex
	EnumSet    map[string]bool // Enum lookup if Kind == ConstraintEnum
}

// Compile pre-compiles constraints for fast validation.
func (c *Constraint) Compile() (*CompiledConstraint, error) {
	cc := &CompiledConstraint{Constraint: *c}

	switch c.Kind {
	case ConstraintRegex:
		pattern, ok := c.Value.(string)
		if !ok {
			return nil, fmt.Errorf("regex constraint value must be string")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern: %w", err)
		}
		cc.Regex = re
	case ConstraintEnum:
		values, ok := c.Value.([]string)
		if !ok {
			return nil, fmt.Errorf("enum constraint value must be []string")
		}
		cc.EnumSet = make(map[string]bool, len(values))
		for _, v := range values {
			cc.EnumSet[v] = true
		}
	}

	return cc, nil
}
