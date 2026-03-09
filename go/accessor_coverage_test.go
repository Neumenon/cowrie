package cowrie

import (
	"testing"
)

func TestNilValueAccessors(t *testing.T) {
	var v *Value

	if v.Type() != TypeNull {
		t.Error("nil Type should be TypeNull")
	}
	if !v.IsNull() {
		t.Error("nil should be null")
	}
}

func TestValueTypeMethodOnNonMatchingType(t *testing.T) {
	// Test all Try accessors on wrong types to cover the false/zero branches
	sv := String("hello")

	// TryBool on non-bool
	if _, ok := sv.TryBool(); ok {
		t.Error("TryBool on string should fail")
	}
	if sv.BoolOr(true) != true {
		t.Error("BoolOr default")
	}

	// TryInt64 on non-int
	if _, ok := sv.TryInt64(); ok {
		t.Error("TryInt64 on string should fail")
	}
	if sv.Int64Or(99) != 99 {
		t.Error("Int64Or default")
	}

	// TryUint64 on non-uint
	if _, ok := sv.TryUint64(); ok {
		t.Error("TryUint64 on string should fail")
	}
	if sv.Uint64Or(99) != 99 {
		t.Error("Uint64Or default")
	}

	// TryFloat64 on non-float
	if _, ok := sv.TryFloat64(); ok {
		t.Error("TryFloat64 on string should fail")
	}
	if sv.Float64Or(1.5) != 1.5 {
		t.Error("Float64Or default")
	}

	// TryString on non-string
	iv := Int64(42)
	if _, ok := iv.TryString(); ok {
		t.Error("TryString on int should fail")
	}
	if iv.StringOr("default") != "default" {
		t.Error("StringOr default")
	}

	// TryBytes on non-bytes
	if _, ok := iv.TryBytes(); ok {
		t.Error("TryBytes on int should fail")
	}

	// TryDatetime64 on non-datetime
	if _, ok := iv.TryDatetime64(); ok {
		t.Error("TryDatetime64 on int should fail")
	}

	// TryUUID128 on non-uuid
	if _, ok := iv.TryUUID128(); ok {
		t.Error("TryUUID128 on int should fail")
	}

	// TryArray on non-array
	if _, ok := iv.TryArray(); ok {
		t.Error("TryArray on int should fail")
	}

	// TryObject on non-object
	if _, ok := iv.TryObject(); ok {
		t.Error("TryObject on int should fail")
	}

	// TryTensor on non-tensor
	if _, ok := iv.TryTensor(); ok {
		t.Error("TryTensor on int should fail")
	}

	// TryNode on non-node
	if _, ok := iv.TryNode(); ok {
		t.Error("TryNode on int should fail")
	}

	// TryEdge on non-edge
	if _, ok := iv.TryEdge(); ok {
		t.Error("TryEdge on int should fail")
	}

	// TryNodeBatch on non-nodebatch
	if _, ok := iv.TryNodeBatch(); ok {
		t.Error("TryNodeBatch on int should fail")
	}

	// TryEdgeBatch on non-edgebatch
	if _, ok := iv.TryEdgeBatch(); ok {
		t.Error("TryEdgeBatch on int should fail")
	}

	// TryGraphShard on non-graphshard
	if _, ok := iv.TryGraphShard(); ok {
		t.Error("TryGraphShard on int should fail")
	}

	// TryBitmask on non-bitmask
	if _, ok := iv.TryBitmask(); ok {
		t.Error("TryBitmask on int should fail")
	}

	// TryUnknownExt on non-ext
	if _, ok := iv.TryUnknownExt(); ok {
		t.Error("TryUnknownExt on int should fail")
	}
}

func assertPanics(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("%s: expected panic", name)
		}
	}()
	f()
}

func TestValuePanics_Coverage(t *testing.T) {
	iv := Int64(42)

	assertPanics(t, "Bool on int", func() { _ = iv.Bool() })
	assertPanics(t, "Get on int", func() { _ = iv.Get("x") })
	assertPanics(t, "Index on int", func() { _ = iv.Index(0) })
	assertPanics(t, "Members on int", func() { _ = iv.Members() })
	assertPanics(t, "Array on int", func() { _ = iv.Array() })
	assertPanics(t, "String on int", func() { _ = iv.String() })
	assertPanics(t, "Bytes on int", func() { _ = iv.Bytes() })
	assertPanics(t, "Float64 on int", func() { _ = iv.Float64() })
	assertPanics(t, "Uint64 on int", func() { _ = iv.Uint64() })
	assertPanics(t, "Tensor on int", func() { _ = iv.Tensor() })
	assertPanics(t, "TensorRef on int", func() { _ = iv.TensorRef() })
	assertPanics(t, "Image on int", func() { _ = iv.Image() })
	assertPanics(t, "Audio on int", func() { _ = iv.Audio() })
	assertPanics(t, "Adjlist on int", func() { _ = iv.Adjlist() })
	assertPanics(t, "RichText on int", func() { _ = iv.RichText() })
	assertPanics(t, "Delta on int", func() { _ = iv.Delta() })
	assertPanics(t, "UnknownExt on int", func() { _ = iv.UnknownExt() })
	assertPanics(t, "Decimal128 on int", func() { _ = iv.Decimal128() })
	assertPanics(t, "Datetime64 on int", func() { _ = iv.Datetime64() })
	assertPanics(t, "UUID128 on int", func() { _ = iv.UUID128() })
	assertPanics(t, "BigInt on int", func() { _ = iv.BigInt() })
	assertPanics(t, "Node on int", func() { _ = iv.Node() })
	assertPanics(t, "Edge on int", func() { _ = iv.Edge() })
	assertPanics(t, "NodeBatch on int", func() { _ = iv.NodeBatch() })
	assertPanics(t, "EdgeBatch on int", func() { _ = iv.EdgeBatch() })
	assertPanics(t, "GraphShard on int", func() { _ = iv.GraphShard() })
	assertPanics(t, "Bitmask on int", func() { _ = iv.Bitmask() })
}

func TestValueLen_Coverage(t *testing.T) {
	// Array len
	arr := Array(Int64(1), Int64(2))
	if arr.Len() != 2 {
		t.Error("array len")
	}

	// Object len
	obj := Object(Member{Key: "x", Value: Int64(1)})
	if obj.Len() != 1 {
		t.Error("object len")
	}

	// Other types return 0
	sv := String("hello")
	if sv.Len() != 0 {
		t.Error("string len should be 0")
	}
}

func TestViewSliceFunctions_Coverage(t *testing.T) {
	// Float32Slice
	td := &TensorData{
		DType: DTypeFloat32,
		Dims:  []uint64{3},
		Data:  encodeFloat32LE([]float32{1.0, 2.0, 3.0}),
	}
	fs := td.Float32Slice()
	if len(fs) != 3 {
		t.Errorf("Float32Slice: expected 3, got %d", len(fs))
	}

	// Float64Slice
	td64 := &TensorData{
		DType: DTypeFloat64,
		Dims:  []uint64{2},
		Data:  encodeFloat64LE([]float64{1.0, 2.0}),
	}
	f64s := td64.Float64Slice()
	if len(f64s) != 2 {
		t.Errorf("Float64Slice: expected 2, got %d", len(f64s))
	}

	// Int32Slice
	td32 := &TensorData{
		DType: DTypeInt32,
		Dims:  []uint64{3},
		Data:  encodeInt32LE([]int32{10, 20, 30}),
	}
	i32s := td32.Int32Slice()
	if len(i32s) != 3 {
		t.Errorf("Int32Slice: expected 3, got %d", len(i32s))
	}

	// Int64Slice
	td64i := &TensorData{
		DType: DTypeInt64,
		Dims:  []uint64{2},
		Data:  encodeInt64LE([]int64{100, 200}),
	}
	i64s := td64i.Int64Slice()
	if len(i64s) != 2 {
		t.Errorf("Int64Slice: expected 2, got %d", len(i64s))
	}

	// CopyFloat32
	cf32 := CopyFloat32(td)
	if len(cf32) != 3 {
		t.Errorf("CopyFloat32: expected 3, got %d", len(cf32))
	}

	// CopyFloat64
	cf64 := CopyFloat64(td64)
	if len(cf64) != 2 {
		t.Errorf("CopyFloat64: expected 2, got %d", len(cf64))
	}
}

func TestViewFunctions_WrongDtype(t *testing.T) {
	// viewFloat64 with float32 data
	td := &TensorData{
		DType: DTypeFloat32,
		Data:  encodeFloat32LE([]float32{1.0}),
	}
	_, ok := td.ViewFloat64()
	if ok {
		t.Error("ViewFloat64 on float32 should fail")
	}

	// viewInt32 with float32 data
	_, ok = td.ViewInt32()
	if ok {
		t.Error("ViewInt32 on float32 should fail")
	}

	// viewInt64 with float32 data
	_, ok = td.ViewInt64()
	if ok {
		t.Error("ViewInt64 on float32 should fail")
	}

	// ViewUint8 always succeeds (no dtype check)
	_, ok = td.ViewUint8()
	if !ok {
		t.Error("ViewUint8 should always succeed")
	}
}

func TestSchemaDescriptor_MoreTypes(t *testing.T) {
	// RichText
	rt := RichText("hello world", nil, nil)
	d := SchemaDescriptor(rt)
	if d != "rich_text" {
		t.Errorf("expected rich_text, got %s", d)
	}

	// Delta
	dt := Delta(0, nil)
	d = SchemaDescriptor(dt)
	if d != "delta" {
		t.Errorf("expected delta, got %s", d)
	}

	// Adjlist
	adj := Adjlist(IDWidthInt32, 2, 1, []uint64{0, 0, 1}, []byte{1, 0, 0, 0})
	d = SchemaDescriptor(adj)
	if d != "adjlist" {
		t.Errorf("expected adjlist, got %s", d)
	}

	// TensorRef
	tr := TensorRef(1, []byte{1, 2})
	d = SchemaDescriptor(tr)
	if d != "tensor_ref" {
		t.Errorf("expected tensor_ref, got %s", d)
	}
}

func TestHashSchema_AllTypes(t *testing.T) {
	// Test that hash covers various types
	types := []*Value{
		Bool(true),
		Uint64(42),
		Float64(3.14),
		String("hello"),
		Bytes([]byte{1}),
		Datetime64(12345),
		UUID128([16]byte{1}),
		RichText("hello", nil, nil),
		Delta(0, nil),
		UnknownExtension(42, []byte{1}),
	}

	for i, v := range types {
		h := SchemaFingerprint64(v)
		if h == 0 {
			t.Errorf("type %d: zero hash", i)
		}
	}
}

func TestToAny_MoreTypes(t *testing.T) {
	// Decimal128
	dec := NewDecimal128(2, [16]byte{1})
	da := ToAny(dec)
	if da == nil {
		t.Error("decimal128 ToAny nil")
	}

	// BigInt
	bi := BigInt([]byte{0x01, 0x00})
	ba := ToAny(bi)
	if ba == nil {
		t.Error("bigint ToAny nil")
	}

	// TensorRef
	tr := TensorRef(1, []byte{1, 2})
	ta := ToAny(tr)
	if ta == nil {
		t.Error("tensorref ToAny nil")
	}

	// RichText
	rt := RichText("hello", []int32{1, 2}, []RichTextSpan{{Start: 0, End: 5, KindID: 1}})
	ra := ToAny(rt)
	if ra == nil {
		t.Error("richtext ToAny nil")
	}

	// Delta
	dt := Delta(1, []DeltaOp{{OpCode: DeltaOpSetField, FieldID: 1, Value: Int64(42)}})
	dta := ToAny(dt)
	if dta == nil {
		t.Error("delta ToAny nil")
	}

	// Adjlist
	adj := Adjlist(IDWidthInt32, 2, 1, []uint64{0, 0, 1}, []byte{1, 0, 0, 0})
	aa := ToAny(adj)
	if aa == nil {
		t.Error("adjlist ToAny nil")
	}

	// Large int64 (out of JS safe range)
	largeInt := Int64(1 << 53)
	li := ToAny(largeInt)
	if li == nil {
		t.Error("large int64 ToAny nil")
	}

	// Large uint64
	largeUint := Uint64(1 << 53)
	lu := ToAny(largeUint)
	if lu == nil {
		t.Error("large uint64 ToAny nil")
	}
}
