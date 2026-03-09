package cowrie

import (
	"testing"
)

func TestTryAccessors(t *testing.T) {
	// TryBool
	bv := Bool(true)
	b, ok := bv.TryBool()
	if !ok || !b {
		t.Error("TryBool true")
	}
	_, ok = Int64(1).TryBool()
	if ok {
		t.Error("TryBool on int should fail")
	}

	// BoolOr
	if !bv.BoolOr(false) {
		t.Error("BoolOr true")
	}
	if Int64(1).BoolOr(true) != true {
		t.Error("BoolOr default")
	}

	// TryInt64
	iv := Int64(42)
	i, ok := iv.TryInt64()
	if !ok || i != 42 {
		t.Error("TryInt64")
	}
	_, ok = String("x").TryInt64()
	if ok {
		t.Error("TryInt64 on string should fail")
	}

	// Int64Or
	if iv.Int64Or(0) != 42 {
		t.Error("Int64Or")
	}
	if String("x").Int64Or(99) != 99 {
		t.Error("Int64Or default")
	}

	// TryUint64
	uv := Uint64(100)
	u, ok := uv.TryUint64()
	if !ok || u != 100 {
		t.Error("TryUint64")
	}

	// Uint64Or
	if uv.Uint64Or(0) != 100 {
		t.Error("Uint64Or")
	}
	if String("x").Uint64Or(5) != 5 {
		t.Error("Uint64Or default")
	}

	// TryFloat64
	fv := Float64(3.14)
	f, ok := fv.TryFloat64()
	if !ok || f != 3.14 {
		t.Error("TryFloat64")
	}

	// Float64Or
	if fv.Float64Or(0) != 3.14 {
		t.Error("Float64Or")
	}
	if String("x").Float64Or(1.5) != 1.5 {
		t.Error("Float64Or default")
	}

	// TryString
	sv := String("hello")
	s, ok := sv.TryString()
	if !ok || s != "hello" {
		t.Error("TryString")
	}

	// StringOr
	if sv.StringOr("") != "hello" {
		t.Error("StringOr")
	}
	if Int64(1).StringOr("default") != "default" {
		t.Error("StringOr default")
	}

	// TryBytes
	byv := Bytes([]byte{1, 2, 3})
	by, ok := byv.TryBytes()
	if !ok || len(by) != 3 {
		t.Error("TryBytes")
	}

	// TryDatetime64
	dtv := Datetime64(1234567890)
	dt, ok := dtv.TryDatetime64()
	if !ok || dt != 1234567890 {
		t.Error("TryDatetime64")
	}

	// TryArray
	arr := Array(Int64(1), Int64(2))
	items, ok := arr.TryArray()
	if !ok || len(items) != 2 {
		t.Error("TryArray")
	}

	// TryObject
	obj := Object(Member{Key: "x", Value: Int64(1)})
	mems, ok := obj.TryObject()
	if !ok || len(mems) != 1 {
		t.Error("TryObject")
	}

	// TryTensor
	tv := Tensor(DTypeFloat32, []uint64{2}, encodeFloat32LE([]float32{1.0, 2.0}))
	td, ok := tv.TryTensor()
	if !ok || td.DType != DTypeFloat32 {
		t.Error("TryTensor")
	}
}

func TestTryExtTypes(t *testing.T) {
	// TryNode
	nv := Node("n1", []string{"A"}, map[string]any{"x": "y"})
	nd, ok := nv.TryNode()
	if !ok || nd.ID != "n1" {
		t.Error("TryNode")
	}

	// TryEdge
	ev := Edge("n1", "n2", "T", nil)
	ed, ok := ev.TryEdge()
	if !ok || ed.From != "n1" {
		t.Error("TryEdge")
	}

	// TryNodeBatch
	nbv := NodeBatch([]NodeData{{ID: "n1", Labels: []string{"A"}}})
	nbd, ok := nbv.TryNodeBatch()
	if !ok || len(nbd.Nodes) != 1 {
		t.Error("TryNodeBatch")
	}

	// TryEdgeBatch
	ebv := EdgeBatch([]EdgeData{{From: "n1", To: "n2", Type: "T"}})
	ebd, ok := ebv.TryEdgeBatch()
	if !ok || len(ebd.Edges) != 1 {
		t.Error("TryEdgeBatch")
	}

	// TryGraphShard
	gsv := GraphShard([]NodeData{{ID: "n1"}}, []EdgeData{{From: "n1", To: "n2"}}, nil)
	gsd, ok := gsv.TryGraphShard()
	if !ok || len(gsd.Nodes) != 1 {
		t.Error("TryGraphShard")
	}

	// TryBitmask
	bmv := BitmaskFromBools([]bool{true, false, true})
	bmd, ok := bmv.TryBitmask()
	if !ok || bmd.Count != 3 {
		t.Error("TryBitmask")
	}

	// TryUnknownExt
	uev := UnknownExtension(42, []byte{0x01})
	ued, ok := uev.TryUnknownExt()
	if !ok || ued.ExtType != 42 {
		t.Error("TryUnknownExt")
	}
}

func TestSetAndAppend(t *testing.T) {
	// Set on object
	obj := Object(Member{Key: "x", Value: Int64(1)})
	obj.Set("x", Int64(99))
	if obj.Get("x").Int64() != 99 {
		t.Error("Set update")
	}
	obj.Set("y", String("new"))
	if obj.Get("y") == nil {
		t.Error("Set add")
	}

	// Append on array
	arr := Array(Int64(1))
	arr.Append(Int64(2))
	if len(arr.Array()) != 2 {
		t.Error("Append")
	}
}

func TestGetOr(t *testing.T) {
	obj := Object(Member{Key: "x", Value: Int64(42)})
	if obj.GetOr("x", String("default")).Int64() != 42 {
		t.Error("GetOr existing")
	}
	if obj.GetOr("missing", String("default")).String() != "default" {
		t.Error("GetOr missing")
	}
}

func TestIndexOr(t *testing.T) {
	arr := Array(Int64(1), Int64(2), Int64(3))
	if arr.IndexOr(0, Null()).Int64() != 1 {
		t.Error("IndexOr valid")
	}
	def := arr.IndexOr(99, String("default"))
	if def.String() != "default" {
		t.Error("IndexOr out of bounds")
	}
}

func TestTryUUID128(t *testing.T) {
	uv := UUID128([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	u, ok := uv.TryUUID128()
	if !ok {
		t.Error("TryUUID128")
	}
	if u[0] != 1 {
		t.Error("UUID value")
	}
}
