package glyph

import (
	"bytes"
	"fmt"
)

// ============================================================
// LYPH v2 Tabular Mode Encoder
// ============================================================
//
// Tabular mode efficiently encodes list<struct> as a table:
//
//   @tab Type [col1 col2 col3]
//   val1a val1b val1c
//   val2a val2b val2c
//   @end
//
// For packed structs, columns are ordered by FID.
// Nested packed structs remain inline.

// TabularOptions configures tabular encoding behavior.
type TabularOptions struct {
	Schema       *Schema
	Threshold    int     // Minimum list length for tabular mode (default 3)
	KeyMode      KeyMode // Column header format
	UseBitmap    bool    // Use bitmap for sparse optionals in cells
	IndentPrefix string  // Prefix for each row (e.g., "  ")
}

// DefaultTabularOptions returns default tabular encoding options.
func DefaultTabularOptions(schema *Schema) TabularOptions {
	return TabularOptions{
		Schema:       schema,
		Threshold:    3,
		KeyMode:      KeyModeWire,
		UseBitmap:    true,
		IndentPrefix: "",
	}
}

// EmitTabular encodes a list of structs in tabular format.
// Returns error if the value is not a suitable list<struct>.
func EmitTabular(v *GValue, schema *Schema) (string, error) {
	return EmitTabularWithOptions(v, DefaultTabularOptions(schema))
}

// EmitTabularWithOptions encodes a list of structs with custom options.
func EmitTabularWithOptions(v *GValue, opts TabularOptions) (string, error) {
	if v == nil || v.typ != TypeList {
		return "", fmt.Errorf("tabular encoding requires list value")
	}

	if len(v.listVal) == 0 {
		return "[]", nil // Empty list
	}

	// Verify all elements are structs of the same type
	first := v.listVal[0]
	if first.typ != TypeStruct {
		return "", fmt.Errorf("tabular encoding requires list of structs")
	}

	typeName := first.structVal.TypeName
	for i, elem := range v.listVal[1:] {
		if elem.typ != TypeStruct {
			return "", fmt.Errorf("element %d is not a struct", i+1)
		}
		if elem.structVal.TypeName != typeName {
			return "", fmt.Errorf("element %d has different type %s (expected %s)",
				i+1, elem.structVal.TypeName, typeName)
		}
	}

	td := opts.Schema.GetType(typeName)
	if td == nil {
		return "", fmt.Errorf("unknown type: %s", typeName)
	}
	if td.Kind != TypeDefStruct || td.Struct == nil {
		return "", fmt.Errorf("type %s is not a struct", typeName)
	}

	var buf bytes.Buffer
	if err := emitTabularTable(&buf, v.listVal, td, opts); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// emitTabularTable writes the full tabular structure.
func emitTabularTable(out *bytes.Buffer, rows []*GValue, td *TypeDef, opts TabularOptions) error {
	fields := td.FieldsByFID()

	// Header: @tab Type [col1 col2 ...]
	out.WriteString("@tab ")
	out.WriteString(td.Name)
	out.WriteString(" [")

	for i, fd := range fields {
		if i > 0 {
			out.WriteByte(' ')
		}
		out.WriteString(getColumnName(fd, opts.KeyMode))
	}
	out.WriteString("]\n")

	// Data rows
	for _, row := range rows {
		out.WriteString(opts.IndentPrefix)
		if err := emitTabularRow(out, row, fields, opts); err != nil {
			return err
		}
		out.WriteByte('\n')
	}

	// Footer
	out.WriteString("@end")

	return nil
}

// getColumnName returns the column header name based on KeyMode.
func getColumnName(fd *FieldDef, mode KeyMode) string {
	switch mode {
	case KeyModeWire:
		if fd.WireKey != "" {
			return fd.WireKey
		}
		return fd.Name
	case KeyModeFID:
		return fmt.Sprintf("#%d", fd.FID)
	default:
		return fd.Name
	}
}

// emitTabularRow writes a single data row.
func emitTabularRow(out *bytes.Buffer, row *GValue, fields []*FieldDef, opts TabularOptions) error {
	packOpts := PackedOptions{
		Schema:    opts.Schema,
		UseBitmap: opts.UseBitmap,
		KeyMode:   opts.KeyMode,
	}

	for i, fd := range fields {
		if i > 0 {
			out.WriteByte(' ')
		}

		val := getFieldValue(row, fd)

		// Handle missing values
		if !isFieldPresent(val, fd) {
			if fd.Optional {
				out.WriteString(canonNull())
			} else {
				return fmt.Errorf("missing required field: %s", fd.Name)
			}
			continue
		}

		// Emit cell value
		if err := emitTabularCell(out, val, fd, packOpts); err != nil {
			return err
		}
	}

	return nil
}

// emitTabularCell writes a single cell value in tabular format.
func emitTabularCell(out *bytes.Buffer, val *GValue, fd *FieldDef, opts PackedOptions) error {
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
			if err := emitTabularCell(out, elem, nil, opts); err != nil {
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
			if err := emitTabularCell(out, entry.Value, nil, opts); err != nil {
				return err
			}
		}
		out.WriteByte('}')

	case TypeStruct:
		// Nested struct in cell: use packed if packable
		nestedTD := opts.Schema.GetType(val.structVal.TypeName)
		if nestedTD != nil && nestedTD.PackEnabled {
			if err := emitPackedStruct(out, val, nestedTD, opts); err != nil {
				return err
			}
		} else if nestedTD != nil {
			if err := emitStructMode(out, val, nestedTD, opts); err != nil {
				return err
			}
		} else {
			if err := emitGenericStruct(out, val, opts); err != nil {
				return err
			}
		}

	case TypeSum:
		// Sum type in cell
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
				if err := emitTabularCell(out, entry.Value, nil, opts); err != nil {
					return err
				}
			}
			out.WriteByte('}')
		} else {
			out.WriteByte('(')
			if err := emitTabularCell(out, val.sumVal.Value, nil, opts); err != nil {
				return err
			}
			out.WriteByte(')')
		}

	default:
		return fmt.Errorf("unsupported value type in tabular cell: %s", val.typ)
	}

	return nil
}

// ============================================================
// Inline Tabular for Nested Lists
// ============================================================

// EmitInlineTabular encodes a list<struct> inline (no newlines).
// Format: @tab Type [cols] v1a v1b | v2a v2b | ... @end
// This is used when a packed struct contains a list<struct> field.
func EmitInlineTabular(v *GValue, schema *Schema) (string, error) {
	opts := DefaultTabularOptions(schema)
	return emitInlineTabularWithOptions(v, opts)
}

func emitInlineTabularWithOptions(v *GValue, opts TabularOptions) (string, error) {
	if v == nil || v.typ != TypeList {
		return "", fmt.Errorf("inline tabular requires list value")
	}

	if len(v.listVal) == 0 {
		return "[]", nil
	}

	first := v.listVal[0]
	if first.typ != TypeStruct {
		return "", fmt.Errorf("inline tabular requires list of structs")
	}

	typeName := first.structVal.TypeName
	td := opts.Schema.GetType(typeName)
	if td == nil {
		return "", fmt.Errorf("unknown type: %s", typeName)
	}

	var buf bytes.Buffer
	fields := td.FieldsByFID()

	// Header
	buf.WriteString("@tab ")
	buf.WriteString(td.Name)
	buf.WriteString(" [")
	for i, fd := range fields {
		if i > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(getColumnName(fd, opts.KeyMode))
	}
	buf.WriteString("]")

	packOpts := PackedOptions{
		Schema:    opts.Schema,
		UseBitmap: opts.UseBitmap,
		KeyMode:   opts.KeyMode,
	}

	// Rows separated by |
	for i, row := range v.listVal {
		if i > 0 {
			buf.WriteString(" |")
		}
		buf.WriteByte(' ')

		for j, fd := range fields {
			if j > 0 {
				buf.WriteByte(' ')
			}
			val := getFieldValue(row, fd)
			if !isFieldPresent(val, fd) {
				buf.WriteString(canonNull())
			} else if err := emitTabularCell(&buf, val, fd, packOpts); err != nil {
				return "", err
			}
		}
	}

	buf.WriteString(" @end")
	return buf.String(), nil
}

// ============================================================
// Streaming Tabular Writer
// ============================================================

// TabularWriter provides a streaming interface for tabular encoding.
// Useful for large datasets or incremental encoding.
type TabularWriter struct {
	out      *bytes.Buffer
	td       *TypeDef
	fields   []*FieldDef
	opts     TabularOptions
	rowCount int
	started  bool
	finished bool
}

// NewTabularWriter creates a new streaming tabular writer.
func NewTabularWriter(td *TypeDef, opts TabularOptions) *TabularWriter {
	return &TabularWriter{
		out:    &bytes.Buffer{},
		td:     td,
		fields: td.FieldsByFID(),
		opts:   opts,
	}
}

// WriteHeader writes the table header. Must be called before WriteRow.
func (tw *TabularWriter) WriteHeader() error {
	if tw.started {
		return fmt.Errorf("header already written")
	}

	tw.out.WriteString("@tab ")
	tw.out.WriteString(tw.td.Name)
	tw.out.WriteString(" [")

	for i, fd := range tw.fields {
		if i > 0 {
			tw.out.WriteByte(' ')
		}
		tw.out.WriteString(getColumnName(fd, tw.opts.KeyMode))
	}
	tw.out.WriteString("]\n")

	tw.started = true
	return nil
}

// WriteRow writes a single data row.
func (tw *TabularWriter) WriteRow(row *GValue) error {
	if !tw.started {
		if err := tw.WriteHeader(); err != nil {
			return err
		}
	}
	if tw.finished {
		return fmt.Errorf("writer already finished")
	}

	if row.typ != TypeStruct || row.structVal.TypeName != tw.td.Name {
		return fmt.Errorf("row type mismatch: expected %s", tw.td.Name)
	}

	tw.out.WriteString(tw.opts.IndentPrefix)
	if err := emitTabularRow(tw.out, row, tw.fields, tw.opts); err != nil {
		return err
	}
	tw.out.WriteByte('\n')

	tw.rowCount++
	return nil
}

// Finish writes the footer and returns the complete output.
func (tw *TabularWriter) Finish() (string, error) {
	if !tw.started {
		return "[]", nil // No rows written
	}
	if tw.finished {
		return tw.out.String(), nil
	}

	tw.out.WriteString("@end")
	tw.finished = true
	return tw.out.String(), nil
}

// RowCount returns the number of rows written.
func (tw *TabularWriter) RowCount() int {
	return tw.rowCount
}

// ============================================================
// Token Counting
// ============================================================

// EstimateTabularTokens estimates token count for tabular vs packed encoding.
// Returns (tabTokens, packedTokens) for comparison.
func EstimateTabularTokens(rows []*GValue, td *TypeDef, schema *Schema) (int, int) {
	if len(rows) == 0 {
		return 0, 0
	}

	fields := td.FieldsByFID()

	// Tabular overhead: @tab Type [cols]\n ... @end
	// Header: @tab (1) + Type (1) + [ (1) + cols + ] (1) + \n (0)
	// Footer: @end (1)
	tabTokens := 4 + len(fields) // @tab, Type, [, cols..., ], @end

	// Per-row: values separated by space + newline
	// Each row adds: numFields values
	tabTokens += len(rows) * len(fields)

	// Packed overhead per row: Type@( ... )
	// Each row: Type (1) + @ (counted with Type) + ( (1) + values + ) (1)
	packedTokens := len(rows) * (2 + len(fields)) // Type@(), plus values

	return tabTokens, packedTokens
}
