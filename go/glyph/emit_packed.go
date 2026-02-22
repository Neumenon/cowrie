package glyph

import (
	"bytes"
	"fmt"
)

// ============================================================
// LYPH v2 Packed Mode Encoder
// ============================================================

// PackedOptions configures packed encoding behavior.
type PackedOptions struct {
	Schema    *Schema
	UseBitmap bool    // Use bitmap for sparse optionals (default true)
	KeyMode   KeyMode // For nested struct emission (Wire/Name/Fid)
}

// KeyMode specifies how field keys are encoded.
type KeyMode int

const (
	KeyModeWire KeyMode = iota // Use wire key if available, else name
	KeyModeName                // Always use full name
	KeyModeFID                 // Use #<fid> format
)

// DefaultPackedOptions returns default packed encoding options.
func DefaultPackedOptions(schema *Schema) PackedOptions {
	return PackedOptions{
		Schema:    schema,
		UseBitmap: true,
		KeyMode:   KeyModeWire,
	}
}

// EmitPacked encodes a struct value in packed format.
// Returns the packed representation as a string.
func EmitPacked(v *GValue, schema *Schema) (string, error) {
	return EmitPackedWithOptions(v, DefaultPackedOptions(schema))
}

// EmitPackedWithOptions encodes a struct value with custom options.
func EmitPackedWithOptions(v *GValue, opts PackedOptions) (string, error) {
	if v == nil || v.typ != TypeStruct {
		return "", fmt.Errorf("packed encoding requires struct value")
	}

	td := opts.Schema.GetType(v.structVal.TypeName)
	if td == nil {
		return "", fmt.Errorf("unknown type: %s", v.structVal.TypeName)
	}
	if td.Kind != TypeDefStruct || td.Struct == nil {
		return "", fmt.Errorf("type %s is not a struct", v.structVal.TypeName)
	}

	var buf bytes.Buffer
	if err := emitPackedStruct(&buf, v, td, opts); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// emitPackedStruct writes a packed struct to the buffer.
func emitPackedStruct(out *bytes.Buffer, v *GValue, td *TypeDef, opts PackedOptions) error {
	// Decide: dense or bitmap?
	useBitmap := shouldUseBitmap(td, v, opts)

	if useBitmap {
		return emitPackedBitmap(out, v, td, opts)
	}
	return emitPackedDense(out, v, td, opts)
}

// shouldUseBitmap determines if bitmap form should be used.
// Rule: UseBitmap enabled AND at least 1 optional field missing.
func shouldUseBitmap(td *TypeDef, v *GValue, opts PackedOptions) bool {
	if !opts.UseBitmap {
		return false
	}

	optFields := td.OptionalFieldsByFID()
	if len(optFields) == 0 {
		return false // No optional fields, always dense
	}

	// Check if any optional is missing
	for _, fd := range optFields {
		val := getFieldValue(v, fd)
		if !isFieldPresent(val, fd) {
			return true // At least one missing, use bitmap
		}
	}

	return false // All optionals present, use dense
}

// emitPackedDense writes dense packed format: Type@(v1 v2 ∅ v4)
func emitPackedDense(out *bytes.Buffer, v *GValue, td *TypeDef, opts PackedOptions) error {
	out.WriteString(td.Name)
	out.WriteString("@(")

	fields := td.FieldsByFID()
	for i, fd := range fields {
		if i > 0 {
			out.WriteByte(' ')
		}

		val := getFieldValue(v, fd)

		// For missing optional fields, emit ∅
		if fd.Optional && !isFieldPresent(val, fd) {
			out.WriteString(canonNull())
			continue
		}

		// For missing required fields, error
		if !fd.Optional && val == nil {
			return fmt.Errorf("missing required field: %s.%s", td.Name, fd.Name)
		}

		// Emit the value
		if err := emitPackedValue(out, val, fd, opts); err != nil {
			return err
		}
	}

	out.WriteByte(')')
	return nil
}

// emitPackedBitmap writes bitmap packed format: Type@{bm=0b101}(v1 v3 v5)
func emitPackedBitmap(out *bytes.Buffer, v *GValue, td *TypeDef, opts PackedOptions) error {
	reqFields := td.RequiredFieldsByFID()
	optFields := td.OptionalFieldsByFID()

	// Compute bitmap mask for optional fields
	mask := computeOptionalMask(td, v, opts)

	out.WriteString(td.Name)
	out.WriteString("@{bm=")
	out.WriteString(maskToBinary(mask))
	out.WriteString("}(")

	first := true

	// Emit required fields first (always present)
	for _, fd := range reqFields {
		if !first {
			out.WriteByte(' ')
		}
		first = false

		val := getFieldValue(v, fd)
		if val == nil {
			return fmt.Errorf("missing required field: %s.%s", td.Name, fd.Name)
		}

		if err := emitPackedValue(out, val, fd, opts); err != nil {
			return err
		}
	}

	// Emit only present optional fields
	for i, fd := range optFields {
		if !mask[i] {
			continue // Not present, skip
		}

		if !first {
			out.WriteByte(' ')
		}
		first = false

		val := getFieldValue(v, fd)
		if err := emitPackedValue(out, val, fd, opts); err != nil {
			return err
		}
	}

	out.WriteByte(')')
	return nil
}

// computeOptionalMask computes the presence mask for optional fields.
// Returns a boolean slice where mask[i] = true if optFields[i] is present.
func computeOptionalMask(td *TypeDef, v *GValue, opts PackedOptions) []bool {
	optFields := td.OptionalFieldsByFID()
	mask := make([]bool, len(optFields))

	for i, fd := range optFields {
		val := getFieldValue(v, fd)
		mask[i] = isFieldPresent(val, fd)
	}

	return mask
}

// isFieldPresent checks if a field value should be considered "present".
// A field is present if:
// - Value is not nil, OR
// - Value is null but KeepNull is set
func isFieldPresent(val *GValue, fd *FieldDef) bool {
	if val == nil {
		return false
	}
	if val.typ == TypeNull && fd.Optional && !fd.KeepNull {
		return false
	}
	return true
}

// getFieldValue retrieves a field value from a struct by name or wire key.
func getFieldValue(v *GValue, fd *FieldDef) *GValue {
	if v.typ != TypeStruct {
		return nil
	}

	// Try by name first
	for _, f := range v.structVal.Fields {
		if f.Key == fd.Name {
			return f.Value
		}
	}

	// Try by wire key
	if fd.WireKey != "" {
		for _, f := range v.structVal.Fields {
			if f.Key == fd.WireKey {
				return f.Value
			}
		}
	}

	return nil
}

// emitPackedValue writes a single value in packed format.
func emitPackedValue(out *bytes.Buffer, val *GValue, fd *FieldDef, opts PackedOptions) error {
	if val == nil {
		out.WriteString(canonNull())
		return nil
	}

	switch val.typ {
	case TypeNull:
		out.WriteString(canonNull())

	case TypeBool:
		out.WriteString(canonBool(val.boolVal))

	case TypeInt:
		out.WriteString(canonInt(val.intVal))

	case TypeFloat:
		out.WriteString(canonFloat(val.floatVal))

	case TypeStr:
		out.WriteString(canonString(val.strVal))

	case TypeID:
		out.WriteString(canonRef(val.idVal))

	case TypeTime:
		out.WriteString(val.timeVal.UTC().Format("2006-01-02T15:04:05Z"))

	case TypeBytes:
		out.WriteString("b64")
		out.WriteString(quoteString(string(val.bytesVal)))

	case TypeList:
		out.WriteByte('[')
		for i, elem := range val.listVal {
			if i > 0 {
				out.WriteByte(' ')
			}
			if err := emitPackedValue(out, elem, nil, opts); err != nil {
				return err
			}
		}
		out.WriteByte(']')

	case TypeMap:
		out.WriteByte('{')
		for i, entry := range val.mapVal {
			if i > 0 {
				out.WriteByte(' ')
			}
			out.WriteString(canonString(entry.Key))
			out.WriteByte(':')
			if err := emitPackedValue(out, entry.Value, nil, opts); err != nil {
				return err
			}
		}
		out.WriteByte('}')

	case TypeStruct:
		// Nested struct: use packed if packable, else struct mode
		nestedTD := opts.Schema.GetType(val.structVal.TypeName)
		if nestedTD != nil && nestedTD.PackEnabled {
			if err := emitPackedStruct(out, val, nestedTD, opts); err != nil {
				return err
			}
		} else if nestedTD != nil {
			// Fallback to struct mode for non-packable types
			if err := emitStructMode(out, val, nestedTD, opts); err != nil {
				return err
			}
		} else {
			// Unknown type, emit as generic struct
			if err := emitGenericStruct(out, val, opts); err != nil {
				return err
			}
		}

	case TypeSum:
		// Sum type: Tag(value) or Tag{...}
		out.WriteString(val.sumVal.Tag)
		if val.sumVal.Value == nil || val.sumVal.Value.typ == TypeNull {
			out.WriteString("()")
		} else if val.sumVal.Value.typ == TypeStruct {
			out.WriteByte('{')
			for i, entry := range val.sumVal.Value.structVal.Fields {
				if i > 0 {
					out.WriteByte(' ')
				}
				out.WriteString(canonString(entry.Key))
				out.WriteByte('=')
				if err := emitPackedValue(out, entry.Value, nil, opts); err != nil {
					return err
				}
			}
			out.WriteByte('}')
		} else {
			out.WriteByte('(')
			if err := emitPackedValue(out, val.sumVal.Value, nil, opts); err != nil {
				return err
			}
			out.WriteByte(')')
		}

	default:
		return fmt.Errorf("unsupported value type in packed mode: %s", val.typ)
	}

	return nil
}

// emitStructMode writes a value in struct mode (for non-packable nested structs).
func emitStructMode(out *bytes.Buffer, v *GValue, td *TypeDef, opts PackedOptions) error {
	out.WriteString(td.Name)
	out.WriteByte('{')

	fields := td.FieldsByFID()
	first := true

	for _, fd := range fields {
		val := getFieldValue(v, fd)

		// Skip missing optionals
		if fd.Optional && !isFieldPresent(val, fd) {
			continue
		}

		// Required field missing is an error
		if !fd.Optional && val == nil {
			return fmt.Errorf("missing required field: %s.%s", td.Name, fd.Name)
		}

		if !first {
			out.WriteByte(' ')
		}
		first = false

		// Use wire key if available
		key := fd.Name
		if opts.KeyMode == KeyModeWire && fd.WireKey != "" {
			key = fd.WireKey
		} else if opts.KeyMode == KeyModeFID {
			key = fmt.Sprintf("#%d", fd.FID)
		}

		out.WriteString(key)
		out.WriteByte('=')
		if err := emitPackedValue(out, val, fd, opts); err != nil {
			return err
		}
	}

	out.WriteByte('}')
	return nil
}

// emitGenericStruct writes a struct without schema info.
func emitGenericStruct(out *bytes.Buffer, v *GValue, opts PackedOptions) error {
	out.WriteString(v.structVal.TypeName)
	out.WriteByte('{')

	for i, entry := range v.structVal.Fields {
		if i > 0 {
			out.WriteByte(' ')
		}
		out.WriteString(canonString(entry.Key))
		out.WriteByte('=')
		if err := emitPackedValue(out, entry.Value, nil, opts); err != nil {
			return err
		}
	}

	out.WriteByte('}')
	return nil
}

// ============================================================
// Mode Detection
// ============================================================

// Mode represents the encoding mode for LYPH v2.
type Mode int

const (
	ModeAuto    Mode = iota // Auto-select best mode
	ModeStruct              // Type{a=1 b=2}
	ModePacked              // Type@(v1 v2 v3)
	ModeTabular             // @tab Type [...] ... @end
	ModePatch               // @patch ... @end
)

// String returns the mode name.
func (m Mode) String() string {
	switch m {
	case ModeAuto:
		return "auto"
	case ModeStruct:
		return "struct"
	case ModePacked:
		return "packed"
	case ModeTabular:
		return "tabular"
	case ModePatch:
		return "patch"
	default:
		return "unknown"
	}
}

// SelectMode determines the best encoding mode for a value.
func SelectMode(v *GValue, schema *Schema, tabThreshold int) Mode {
	if v == nil {
		return ModeStruct
	}

	// Patch mode is selected explicitly, not auto
	// (PatchSet is a different type, not handled here)

	// Tabular for list<struct> with enough elements
	if v.typ == TypeList && len(v.listVal) >= tabThreshold {
		// Check if all elements are structs of the same type
		if len(v.listVal) > 0 && v.listVal[0].typ == TypeStruct {
			typeName := v.listVal[0].structVal.TypeName
			allSame := true
			for _, elem := range v.listVal[1:] {
				if elem.typ != TypeStruct || elem.structVal.TypeName != typeName {
					allSame = false
					break
				}
			}
			if allSame {
				td := schema.GetType(typeName)
				if td != nil && td.TabEnabled {
					return ModeTabular
				}
			}
		}
	}

	// Packed for structs with PackEnabled
	if v.typ == TypeStruct {
		td := schema.GetType(v.structVal.TypeName)
		if td != nil && td.PackEnabled {
			return ModePacked
		}
	}

	return ModeStruct
}
