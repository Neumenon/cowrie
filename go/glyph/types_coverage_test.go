package glyph

import (
	"testing"
	"time"
)

func TestGType_String(t *testing.T) {
	tests := []struct {
		typ  GType
		want string
	}{
		{TypeNull, "null"},
		{TypeBool, "bool"},
		{TypeInt, "int"},
		{TypeFloat, "float"},
		{TypeStr, "str"},
		{TypeBytes, "bytes"},
		{TypeTime, "time"},
		{TypeID, "id"},
		{TypeList, "list"},
		{TypeMap, "map"},
		{TypeStruct, "struct"},
		{TypeSum, "sum"},
		{TypeBlob, "blob"},
		{TypePoolRef, "poolref"},
		{GType(255), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.typ.String(); got != tt.want {
				t.Errorf("GType(%d).String() = %q, want %q", tt.typ, got, tt.want)
			}
		})
	}
}

func TestConstructorsAndAccessors(t *testing.T) {
	// Null
	n := Null()
	if n.Type() != TypeNull {
		t.Error("Null type")
	}
	if !n.IsNull() {
		t.Error("expected IsNull")
	}

	// Bool
	b := Bool(true)
	if b.Type() != TypeBool {
		t.Error("Bool type")
	}
	bv, err := b.AsBool()
	if err != nil || bv != true {
		t.Errorf("AsBool: %v %v", bv, err)
	}

	// Int
	i := Int(42)
	if i.Type() != TypeInt {
		t.Error("Int type")
	}
	iv, err := i.AsInt()
	if err != nil || iv != 42 {
		t.Errorf("AsInt: %v %v", iv, err)
	}

	// Float
	f := Float(3.14)
	if f.Type() != TypeFloat {
		t.Error("Float type")
	}
	fv, err := f.AsFloat()
	if err != nil || fv != 3.14 {
		t.Errorf("AsFloat: %v %v", fv, err)
	}

	// Str
	s := Str("hello")
	if s.Type() != TypeStr {
		t.Error("Str type")
	}
	sv, err := s.AsStr()
	if err != nil || sv != "hello" {
		t.Errorf("AsStr: %v %v", sv, err)
	}

	// Bytes
	by := Bytes([]byte{1, 2, 3})
	if by.Type() != TypeBytes {
		t.Error("Bytes type")
	}
	bsv, err := by.AsBytes()
	if err != nil || len(bsv) != 3 {
		t.Errorf("AsBytes: %v %v", bsv, err)
	}

	// Time
	now := time.Now().UTC()
	tm := Time(now)
	if tm.Type() != TypeTime {
		t.Error("Time type")
	}
	tv, err := tm.AsTime()
	if err != nil || !tv.Equal(now) {
		t.Errorf("AsTime: %v %v", tv, err)
	}

	// ID
	id := ID("m", "123")
	if id.Type() != TypeID {
		t.Error("ID type")
	}
	idv, err := id.AsID()
	if err != nil || idv.Prefix != "m" || idv.Value != "123" {
		t.Errorf("AsID: %v %v", idv, err)
	}

	// IDFromRef
	ref := RefID{Prefix: "t", Value: "456"}
	idr := IDFromRef(ref)
	ridv, err := idr.AsID()
	if err != nil || ridv != ref {
		t.Errorf("IDFromRef: %v %v", ridv, err)
	}

	// List
	l := List(Int(1), Int(2), Int(3))
	if l.Type() != TypeList {
		t.Error("List type")
	}
	lv, err := l.AsList()
	if err != nil || len(lv) != 3 {
		t.Errorf("AsList: %v %v", lv, err)
	}

	// Map
	m := Map(MapEntry{Key: "a", Value: Int(1)}, MapEntry{Key: "b", Value: Int(2)})
	if m.Type() != TypeMap {
		t.Error("Map type")
	}
	mv, err := m.AsMap()
	if err != nil || len(mv) != 2 {
		t.Errorf("AsMap: %v %v", mv, err)
	}

	// Struct
	st := Struct("Person",
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "age", Value: Int(30)},
	)
	if st.Type() != TypeStruct {
		t.Error("Struct type")
	}
	sv2, err := st.AsStruct()
	if err != nil || sv2.TypeName != "Person" || len(sv2.Fields) != 2 {
		t.Errorf("AsStruct: %v %v", sv2, err)
	}

	// Sum
	su := Sum("Ok", Int(42))
	if su.Type() != TypeSum {
		t.Error("Sum type")
	}
	suv, err := su.AsSum()
	if err != nil || suv.Tag != "Ok" {
		t.Errorf("AsSum: %v %v", suv, err)
	}
}

func TestAccessors_NilAndWrongType(t *testing.T) {
	var nilV *GValue

	// Nil value accessors
	if nilV.Type() != TypeNull {
		t.Error("nil Type")
	}
	if !nilV.IsNull() {
		t.Error("nil IsNull")
	}

	_, err := nilV.AsBool()
	if err == nil {
		t.Error("expected error from nil AsBool")
	}
	_, err = nilV.AsInt()
	if err == nil {
		t.Error("expected error from nil AsInt")
	}
	_, err = nilV.AsFloat()
	if err == nil {
		t.Error("expected error from nil AsFloat")
	}
	_, err = nilV.AsStr()
	if err == nil {
		t.Error("expected error from nil AsStr")
	}
	_, err = nilV.AsBytes()
	if err == nil {
		t.Error("expected error from nil AsBytes")
	}
	_, err = nilV.AsTime()
	if err == nil {
		t.Error("expected error from nil AsTime")
	}
	_, err = nilV.AsID()
	if err == nil {
		t.Error("expected error from nil AsID")
	}
	_, err = nilV.AsList()
	if err == nil {
		t.Error("expected error from nil AsList")
	}
	_, err = nilV.AsMap()
	if err == nil {
		t.Error("expected error from nil AsMap")
	}
	_, err = nilV.AsStruct()
	if err == nil {
		t.Error("expected error from nil AsStruct")
	}
	_, err = nilV.AsSum()
	if err == nil {
		t.Error("expected error from nil AsSum")
	}

	// Wrong type errors
	v := Int(42)
	_, err = v.AsBool()
	if err == nil {
		t.Error("expected error from Int AsBool")
	}
	_, err = v.AsStr()
	if err == nil {
		t.Error("expected error from Int AsStr")
	}
	_, err = v.AsFloat()
	if err == nil {
		t.Error("expected error from Int AsFloat")
	}
	_, err = v.AsBytes()
	if err == nil {
		t.Error("expected error from Int AsBytes")
	}
	_, err = v.AsTime()
	if err == nil {
		t.Error("expected error from Int AsTime")
	}
	_, err = v.AsID()
	if err == nil {
		t.Error("expected error from Int AsID")
	}
	_, err = v.AsList()
	if err == nil {
		t.Error("expected error from Int AsList")
	}
	_, err = v.AsMap()
	if err == nil {
		t.Error("expected error from Int AsMap")
	}
	_, err = v.AsStruct()
	if err == nil {
		t.Error("expected error from Int AsStruct")
	}
	_, err = v.AsSum()
	if err == nil {
		t.Error("expected error from Int AsSum")
	}
}

func TestLen(t *testing.T) {
	l := List(Int(1), Int(2))
	if l.Len() != 2 {
		t.Errorf("List Len: got %d", l.Len())
	}

	m := Map(MapEntry{Key: "a", Value: Int(1)})
	if m.Len() != 1 {
		t.Errorf("Map Len: got %d", m.Len())
	}

	st := Struct("T", MapEntry{Key: "x", Value: Int(1)}, MapEntry{Key: "y", Value: Int(2)})
	if st.Len() != 2 {
		t.Errorf("Struct Len: got %d", st.Len())
	}

	// Non-container returns 0
	if Int(42).Len() != 0 {
		t.Error("Int Len should be 0")
	}
}

func TestGet(t *testing.T) {
	m := Map(
		MapEntry{Key: "a", Value: Int(1)},
		MapEntry{Key: "b", Value: Str("hello")},
	)
	if m.Get("a").Type() != TypeInt {
		t.Error("Get 'a' should return Int")
	}
	if m.Get("missing") != nil {
		t.Error("Get missing key should return nil")
	}

	st := Struct("T",
		MapEntry{Key: "x", Value: Float(1.5)},
	)
	if st.Get("x").Type() != TypeFloat {
		t.Error("Struct Get 'x' should return Float")
	}
	if st.Get("missing") != nil {
		t.Error("Struct Get missing key should return nil")
	}
}

func TestIndex(t *testing.T) {
	l := List(Int(1), Int(2), Int(3))
	v, err := l.Index(0)
	if err != nil {
		t.Fatalf("Index(0) error: %v", err)
	}
	iv, _ := v.AsInt()
	if iv != 1 {
		t.Errorf("Index(0): got %d", iv)
	}

	_, err = l.Index(-1)
	if err == nil {
		t.Error("expected error for negative index")
	}

	_, err = l.Index(5)
	if err == nil {
		t.Error("expected error for out of bounds index")
	}

	// non-list
	_, err = Int(42).Index(0)
	if err == nil {
		t.Error("expected error for Index on non-list")
	}
}

func TestSet(t *testing.T) {
	// Map set - update existing
	m := Map(MapEntry{Key: "a", Value: Int(1)})
	m.Set("a", Int(99))
	iv, _ := m.Get("a").AsInt()
	if iv != 99 {
		t.Error("Set did not update map value")
	}

	// Map set - add new
	m.Set("b", Str("new"))
	if m.Len() != 2 {
		t.Error("Set did not add new map entry")
	}

	// Struct set - update existing
	st := Struct("T", MapEntry{Key: "x", Value: Int(1)})
	st.Set("x", Int(42))
	iv2, _ := st.Get("x").AsInt()
	if iv2 != 42 {
		t.Error("Set did not update struct field")
	}

	// Struct set - add new
	st.Set("y", Str("new"))
	if st.Len() != 2 {
		t.Error("Set did not add new struct field")
	}
}

func TestAppend(t *testing.T) {
	l := List(Int(1))
	l.Append(Int(2))
	if l.Len() != 2 {
		t.Errorf("Append: expected len 2, got %d", l.Len())
	}
}

func TestRefID_String(t *testing.T) {
	r1 := RefID{Prefix: "m", Value: "123"}
	if r1.String() != "^m:123" {
		t.Errorf("RefID.String(): got %q", r1.String())
	}

	r2 := RefID{Prefix: "", Value: "abc"}
	if r2.String() != "^abc" {
		t.Errorf("RefID.String() no prefix: got %q", r2.String())
	}
}

func TestPosition_String(t *testing.T) {
	p := Position{Line: 10, Column: 5}
	if p.String() != "10:5" {
		t.Errorf("Position.String(): got %q", p.String())
	}
}

func TestPos_SetPos(t *testing.T) {
	var nilV *GValue
	if nilV.Pos() != (Position{}) {
		t.Error("nil Pos should return zero Position")
	}

	v := Int(42)
	v.SetPos(Position{Line: 3, Column: 7})
	if v.Pos().Line != 3 || v.Pos().Column != 7 {
		t.Error("SetPos/Pos mismatch")
	}
}

func TestFieldVal(t *testing.T) {
	f := FieldVal("name", Str("Alice"))
	if f.Key != "name" {
		t.Error("FieldVal key")
	}
}

func TestNumber_IsNumeric(t *testing.T) {
	iv := Int(42)
	n, ok := iv.Number()
	if !ok || n != 42.0 {
		t.Error("Int Number")
	}
	if !iv.IsNumeric() {
		t.Error("Int IsNumeric")
	}

	fv := Float(3.14)
	n, ok = fv.Number()
	if !ok || n != 3.14 {
		t.Error("Float Number")
	}
	if !fv.IsNumeric() {
		t.Error("Float IsNumeric")
	}

	sv := Str("hello")
	_, ok = sv.Number()
	if ok {
		t.Error("Str should not be numeric")
	}
	if sv.IsNumeric() {
		t.Error("Str IsNumeric should be false")
	}
}
