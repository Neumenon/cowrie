package glyph

import (
	"time"

	"github.com/Neumenon/cowrie"
)

// ToCowrie converts a GValue to an cowrie.Value.
func ToCowrie(v *GValue) *cowrie.Value {
	if v == nil {
		return cowrie.Null()
	}

	switch v.typ {
	case TypeNull:
		return cowrie.Null()

	case TypeBool:
		return cowrie.Bool(v.boolVal)

	case TypeInt:
		return cowrie.Int64(v.intVal)

	case TypeFloat:
		return cowrie.Float64(v.floatVal)

	case TypeStr:
		return cowrie.String(v.strVal)

	case TypeBytes:
		return cowrie.Bytes(v.bytesVal)

	case TypeTime:
		return cowrie.Datetime64(v.timeVal.UnixNano())

	case TypeID:
		// Represent as string with ^ prefix
		return cowrie.String(v.idVal.String())

	case TypeList:
		items := make([]*cowrie.Value, len(v.listVal))
		for i, elem := range v.listVal {
			items[i] = ToCowrie(elem)
		}
		return cowrie.Array(items...)

	case TypeMap:
		members := make([]cowrie.Member, len(v.mapVal))
		for i, entry := range v.mapVal {
			members[i] = cowrie.Member{Key: entry.Key, Value: ToCowrie(entry.Value)}
		}
		return cowrie.Object(members...)

	case TypeStruct:
		// Represent as object with _type field
		members := make([]cowrie.Member, 0, len(v.structVal.Fields)+1)
		members = append(members, cowrie.Member{Key: "_type", Value: cowrie.String(v.structVal.TypeName)})
		for _, field := range v.structVal.Fields {
			members = append(members, cowrie.Member{Key: field.Key, Value: ToCowrie(field.Value)})
		}
		return cowrie.Object(members...)

	case TypeSum:
		// Represent as object with _tag and _value
		return cowrie.Object(
			cowrie.Member{Key: "_tag", Value: cowrie.String(v.sumVal.Tag)},
			cowrie.Member{Key: "_value", Value: ToCowrie(v.sumVal.Value)},
		)

	default:
		return cowrie.Null()
	}
}

// FromCowrie converts an cowrie.Value to a GValue.
func FromCowrie(v *cowrie.Value) *GValue {
	if v == nil || v.IsNull() {
		return Null()
	}

	switch v.Type() {
	case cowrie.TypeNull:
		return Null()

	case cowrie.TypeBool:
		return Bool(v.Bool())

	case cowrie.TypeInt64:
		return Int(v.Int64())

	case cowrie.TypeUint64:
		return Int(int64(v.Uint64()))

	case cowrie.TypeFloat64:
		return Float(v.Float64())

	case cowrie.TypeString:
		s := v.String()
		// Check for ID reference
		if len(s) > 0 && s[0] == '^' {
			return parseRefFromString(s[1:])
		}
		return Str(s)

	case cowrie.TypeBytes:
		return Bytes(v.Bytes())

	case cowrie.TypeDatetime64:
		return Time(time.Unix(0, v.Datetime64()))

	case cowrie.TypeArray:
		items := v.Array()
		elements := make([]*GValue, len(items))
		for i, item := range items {
			elements[i] = FromCowrie(item)
		}
		return List(elements...)

	case cowrie.TypeObject:
		members := v.Members()

		// Check for struct marker
		typeVal := v.Get("_type")
		if typeVal != nil && typeVal.Type() == cowrie.TypeString {
			typeName := typeVal.String()
			fields := make([]MapEntry, 0, len(members)-1)
			for _, m := range members {
				if m.Key == "_type" {
					continue
				}
				fields = append(fields, MapEntry{Key: m.Key, Value: FromCowrie(m.Value)})
			}
			return Struct(typeName, fields...)
		}

		// Check for sum marker
		tagVal := v.Get("_tag")
		if tagVal != nil && tagVal.Type() == cowrie.TypeString {
			tag := tagVal.String()
			valueVal := v.Get("_value")
			return Sum(tag, FromCowrie(valueVal))
		}

		// Regular map
		entries := make([]MapEntry, len(members))
		for i, m := range members {
			entries[i] = MapEntry{Key: m.Key, Value: FromCowrie(m.Value)}
		}
		return Map(entries...)

	default:
		return Null()
	}
}

// parseRefFromString parses a ref ID from its string representation.
func parseRefFromString(s string) *GValue {
	// Split on first colon
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			return ID(s[:i], s[i+1:])
		}
	}
	return ID("", s)
}

// ============================================================
// Batch Conversion
// ============================================================

// ToCowrieList converts a list of GValues to cowrie.Values.
func ToCowrieList(values []*GValue) []*cowrie.Value {
	result := make([]*cowrie.Value, len(values))
	for i, v := range values {
		result[i] = ToCowrie(v)
	}
	return result
}

// FromCowrieList converts a list of cowrie.Values to GValues.
func FromCowrieList(values []*cowrie.Value) []*GValue {
	result := make([]*GValue, len(values))
	for i, v := range values {
		result[i] = FromCowrie(v)
	}
	return result
}

// ============================================================
// JSON Interop
// ============================================================

// ToJSON converts a GValue to JSON bytes via cowrie.
func ToJSON(v *GValue) ([]byte, error) {
	sv := ToCowrie(v)
	return cowrie.ToJSON(sv)
}

// FromJSON parses JSON bytes into a GValue.
func FromJSON(data []byte) (*GValue, error) {
	sv, err := cowrie.FromJSON(data)
	if err != nil {
		return nil, err
	}
	return FromCowrie(sv), nil
}

// ToJSONString converts a GValue to a JSON string.
func ToJSONString(v *GValue) (string, error) {
	data, err := ToJSON(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ============================================================
// Go Native Interop (any)
// ============================================================

// ToAny converts a GValue to a Go any value.
// This enables direct interop with Gen1 and JSON:
//
//	gv := glyph.Parse(text)
//	data, _ := gen1.Encode(glyph.ToAny(gv))  // GLYPH → Gen1
//	jsonBytes, _ := json.Marshal(glyph.ToAny(gv))  // GLYPH → JSON
func ToAny(v *GValue) any {
	return cowrie.ToAny(ToCowrie(v))
}

// FromAny converts a Go any value to a GValue.
// This enables direct interop from Gen1 and JSON:
//
//	native, _ := gen1.Decode(data)
//	gv := glyph.FromAny(native)  // Gen1 → GLYPH
//	text := glyph.Canonicalize(gv)
//
//	var obj any
//	json.Unmarshal(jsonBytes, &obj)
//	gv := glyph.FromAny(obj)  // JSON → GLYPH
func FromAny(v any) *GValue {
	return FromCowrie(cowrie.FromAny(v))
}

// ============================================================
// Binary Interop (via Cowrie)
// ============================================================

// EncodeBinary encodes a GValue to Cowrie binary format.
func EncodeBinary(v *GValue) ([]byte, error) {
	sv := ToCowrie(v)
	return cowrie.Encode(sv)
}

// DecodeBinary decodes Cowrie binary format to a GValue.
func DecodeBinary(data []byte) (*GValue, error) {
	sv, err := cowrie.Decode(data)
	if err != nil {
		return nil, err
	}
	return FromCowrie(sv), nil
}

// ============================================================
// Round-Trip Verification
// ============================================================

// RoundTrips checks if a GValue survives round-trip through Cowrie.
func RoundTrips(v *GValue) bool {
	sv := ToCowrie(v)
	roundTripped := FromCowrie(sv)

	// Compare canonical forms
	original := Emit(v)
	afterRT := Emit(roundTripped)

	return original == afterRT
}

// ============================================================
// Type-Aware Conversion
// ============================================================

// FromCowrieWithSchema converts an cowrie.Value using schema hints.
func FromCowrieWithSchema(v *cowrie.Value, schema *Schema, typeName string) *GValue {
	if v == nil || v.IsNull() {
		return Null()
	}

	td := schema.GetType(typeName)
	if td == nil {
		return FromCowrie(v)
	}

	if td.Kind == TypeDefStruct && v.Type() == cowrie.TypeObject {
		return fromCowrieStruct(v, schema, td)
	}

	return FromCowrie(v)
}

func fromCowrieStruct(v *cowrie.Value, schema *Schema, td *TypeDef) *GValue {
	members := v.Members()
	fields := make([]MapEntry, 0, len(members))

	// Build field lookup from schema
	fieldDefs := make(map[string]*FieldDef)
	wireKeyMap := make(map[string]string) // wireKey -> fieldName
	for _, fd := range td.Struct.Fields {
		fieldDefs[fd.Name] = fd
		if fd.WireKey != "" {
			wireKeyMap[fd.WireKey] = fd.Name
		}
	}

	for _, m := range members {
		if m.Key == "_type" {
			continue
		}

		// Resolve wire key to field name
		fieldName := m.Key
		if fullName, ok := wireKeyMap[m.Key]; ok {
			fieldName = fullName
		}

		// Get field def for type-aware conversion
		fd := fieldDefs[fieldName]
		var fieldValue *GValue
		if fd != nil && fd.Type.Kind == TypeSpecRef {
			fieldValue = FromCowrieWithSchema(m.Value, schema, fd.Type.Name)
		} else {
			fieldValue = FromCowrie(m.Value)
		}

		fields = append(fields, MapEntry{Key: fieldName, Value: fieldValue})
	}

	return Struct(td.Name, fields...)
}
