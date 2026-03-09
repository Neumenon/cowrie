package delta

import (
	"testing"

	"github.com/Neumenon/cowrie/go"
)

func TestApply_NilDelta(t *testing.T) {
	base := cowrie.Object(cowrie.Member{Key: "x", Value: cowrie.Int64(1)})
	result, err := Apply(base, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != base {
		t.Error("expected same base returned for nil delta")
	}
}

func TestApply_EmptyOps(t *testing.T) {
	base := cowrie.Object(cowrie.Member{Key: "x", Value: cowrie.Int64(1)})
	delta := &cowrie.DeltaData{BaseID: 0, Ops: []cowrie.DeltaOp{}}
	result, err := Apply(base, delta, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != base {
		t.Error("expected same base returned for empty ops")
	}
}

func TestApply_NilBase(t *testing.T) {
	delta := &cowrie.DeltaData{
		BaseID: 0,
		Ops:    []cowrie.DeltaOp{{OpCode: cowrie.DeltaOpSetField, FieldID: 0, Value: cowrie.Int64(1)}},
	}
	_, err := Apply(nil, delta, []string{"x"})
	if err == nil {
		t.Error("expected error for nil base")
	}
}

func TestApply_NonObjectBase(t *testing.T) {
	delta := &cowrie.DeltaData{
		BaseID: 0,
		Ops:    []cowrie.DeltaOp{{OpCode: cowrie.DeltaOpSetField, FieldID: 0, Value: cowrie.Int64(1)}},
	}
	_, err := Apply(cowrie.Array(cowrie.Int64(1)), delta, []string{"x"})
	if err == nil {
		t.Error("expected error for non-object base")
	}
}

func TestApply_AppendArray_NewField(t *testing.T) {
	base := cowrie.Object(cowrie.Member{Key: "x", Value: cowrie.Int64(1)})
	delta := &cowrie.DeltaData{
		BaseID: 0,
		Ops: []cowrie.DeltaOp{
			{OpCode: cowrie.DeltaOpAppendArray, FieldID: 0, Value: cowrie.String("item1")},
		},
	}
	result, err := Apply(base, delta, []string{"items"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should have created a new array field
	found := false
	for _, m := range result.Members() {
		if m.Key == "items" && m.Value.Type() == cowrie.TypeArray {
			found = true
			if m.Value.Len() != 1 {
				t.Errorf("expected 1 item in new array, got %d", m.Value.Len())
			}
		}
	}
	if !found {
		t.Error("expected 'items' array field")
	}
}

func TestApply_AppendArray_ExistingArray(t *testing.T) {
	base := cowrie.Object(
		cowrie.Member{Key: "items", Value: cowrie.Array(cowrie.String("a"))},
	)
	delta := &cowrie.DeltaData{
		BaseID: 0,
		Ops: []cowrie.DeltaOp{
			{OpCode: cowrie.DeltaOpAppendArray, FieldID: 0, Value: cowrie.String("b")},
		},
	}
	result, err := Apply(base, delta, []string{"items"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, m := range result.Members() {
		if m.Key == "items" {
			if m.Value.Len() != 2 {
				t.Errorf("expected 2 items, got %d", m.Value.Len())
			}
		}
	}
}

func TestApply_AppendArray_NonArray(t *testing.T) {
	base := cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(1)},
	)
	delta := &cowrie.DeltaData{
		BaseID: 0,
		Ops: []cowrie.DeltaOp{
			{OpCode: cowrie.DeltaOpAppendArray, FieldID: 0, Value: cowrie.String("b")},
		},
	}
	_, err := Apply(base, delta, []string{"x"})
	if err == nil {
		t.Error("expected error when appending to non-array field")
	}
}

func TestApply_UnknownOp(t *testing.T) {
	base := cowrie.Object(cowrie.Member{Key: "x", Value: cowrie.Int64(1)})
	delta := &cowrie.DeltaData{
		BaseID: 0,
		Ops: []cowrie.DeltaOp{
			{OpCode: 0xFF, FieldID: 0, Value: cowrie.Int64(1)},
		},
	}
	_, err := Apply(base, delta, []string{"x"})
	if err == nil {
		t.Error("expected error for unknown op code")
	}
}

func TestApply_FieldIDOutOfBounds(t *testing.T) {
	base := cowrie.Object(cowrie.Member{Key: "x", Value: cowrie.Int64(1)})
	delta := &cowrie.DeltaData{
		BaseID: 0,
		Ops: []cowrie.DeltaOp{
			{OpCode: cowrie.DeltaOpSetField, FieldID: 99, Value: cowrie.Int64(1)},
		},
	}
	_, err := Apply(base, delta, []string{"x"})
	if err == nil {
		t.Error("expected error for field ID out of bounds")
	}
}

func TestApply_EmptyFieldDict(t *testing.T) {
	base := cowrie.Object(cowrie.Member{Key: "x", Value: cowrie.Int64(1)})
	delta := &cowrie.DeltaData{
		BaseID: 0,
		Ops: []cowrie.DeltaOp{
			{OpCode: cowrie.DeltaOpSetField, FieldID: 0, Value: cowrie.Int64(42)},
		},
	}
	result, err := Apply(base, delta, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// With empty dict, field name becomes "field_0"
	found := false
	for _, m := range result.Members() {
		if m.Key == "field_0" {
			found = true
		}
	}
	if !found {
		t.Error("expected fallback field name 'field_0'")
	}
}

func TestDefaultApplyConfig(t *testing.T) {
	cfg := DefaultApplyConfig()
	if cfg.StrictMode {
		t.Error("StrictMode should default to false")
	}
	if cfg.PreserveOrder {
		t.Error("PreserveOrder should default to false")
	}
}

func TestApplyWithStore(t *testing.T) {
	store := NewBaseStore(DefaultStoreConfig())

	// Store a value
	base := cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(1)},
		cowrie.Member{Key: "y", Value: cowrie.String("hello")},
	)
	baseID := store.Store(base)

	// Build delta
	delta := &cowrie.DeltaData{
		BaseID: baseID,
		Ops: []cowrie.DeltaOp{
			{OpCode: cowrie.DeltaOpSetField, FieldID: 0, Value: cowrie.Int64(99)},
		},
	}

	result, err := ApplyWithStore(delta, store)
	if err != nil {
		t.Fatalf("ApplyWithStore failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestApplyWithStore_NilDelta(t *testing.T) {
	store := NewBaseStore(DefaultStoreConfig())
	_, err := ApplyWithStore(nil, store)
	if err == nil {
		t.Error("expected error for nil delta")
	}
}

func TestApplyWithStore_MissingBase(t *testing.T) {
	store := NewBaseStore(DefaultStoreConfig())
	delta := &cowrie.DeltaData{BaseID: 999, Ops: []cowrie.DeltaOp{}}
	_, err := ApplyWithStore(delta, store)
	if err == nil {
		t.Error("expected error for missing base in store")
	}
}

func TestBaseStore_TouchEvictClear(t *testing.T) {
	store := NewBaseStore(StoreConfig{MaxEntries: 5})

	id1 := store.Store(cowrie.Object(cowrie.Member{Key: "a", Value: cowrie.Int64(1)}))
	id2 := store.Store(cowrie.Object(cowrie.Member{Key: "b", Value: cowrie.Int64(2)}))
	id3 := store.Store(cowrie.Object(cowrie.Member{Key: "c", Value: cowrie.Int64(3)}))

	// Touch
	store.Touch(id1)
	store.Touch(999) // non-existent, should be no-op

	if store.Size() != 3 {
		t.Errorf("expected size 3, got %d", store.Size())
	}

	// Evict
	store.Evict(id2)
	if store.Size() != 2 {
		t.Errorf("expected size 2 after evict, got %d", store.Size())
	}
	_, ok := store.Get(id2)
	if ok {
		t.Error("id2 should be evicted")
	}

	// Clear
	store.Clear()
	if store.Size() != 0 {
		t.Errorf("expected size 0 after clear, got %d", store.Size())
	}
	_, ok = store.Get(id1)
	if ok {
		t.Error("id1 should be gone after clear")
	}
	_, ok = store.Get(id3)
	if ok {
		t.Error("id3 should be gone after clear")
	}
}

func TestEstimateValueSize(t *testing.T) {
	tests := []struct {
		name string
		val  *cowrie.Value
	}{
		{"nil", nil},
		{"null", cowrie.Null()},
		{"bool", cowrie.Bool(true)},
		{"int64", cowrie.Int64(42)},
		{"float64", cowrie.Float64(3.14)},
		{"string", cowrie.String("hello")},
		{"bytes", cowrie.Bytes([]byte{1, 2, 3})},
		{"datetime64", cowrie.Datetime64(12345)},
		{"array", cowrie.Array(cowrie.Int64(1), cowrie.Int64(2))},
		{"object", cowrie.Object(cowrie.Member{Key: "k", Value: cowrie.String("v")})},
		{"tensor", cowrie.Tensor(cowrie.DTypeFloat32, []uint64{2}, make([]byte, 8))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := estimateValueSize(tt.val)
			// Just ensure it returns a reasonable value
			if tt.val != nil && tt.val.Type() != cowrie.TypeNull && size <= 0 {
				t.Errorf("expected positive size for %s, got %d", tt.name, size)
			}
		})
	}
}

func TestCompute_BytesEqual(t *testing.T) {
	base := cowrie.Object(
		cowrie.Member{Key: "data", Value: cowrie.Bytes([]byte{1, 2, 3})},
	)
	target := cowrie.Object(
		cowrie.Member{Key: "data", Value: cowrie.Bytes([]byte{1, 2, 3})},
	)
	result := Compute(base, target, DefaultConfig())
	if result.Delta == nil {
		t.Fatal("expected delta")
	}
	if len(result.Delta.Ops) != 0 {
		t.Errorf("expected 0 ops for identical bytes, got %d", len(result.Delta.Ops))
	}
}

func TestCompute_BytesDifferent(t *testing.T) {
	base := cowrie.Object(
		cowrie.Member{Key: "data", Value: cowrie.Bytes([]byte{1, 2, 3})},
	)
	target := cowrie.Object(
		cowrie.Member{Key: "data", Value: cowrie.Bytes([]byte{4, 5, 6})},
	)
	result := Compute(base, target, DefaultConfig())
	if result.Delta == nil {
		t.Fatal("expected delta")
	}
	if len(result.Delta.Ops) != 1 {
		t.Errorf("expected 1 op for changed bytes, got %d", len(result.Delta.Ops))
	}
}

func TestValuesEqual_Extended(t *testing.T) {
	tests := []struct {
		name  string
		a     *cowrie.Value
		b     *cowrie.Value
		equal bool
	}{
		{"float same", cowrie.Float64(1.5), cowrie.Float64(1.5), true},
		{"float diff", cowrie.Float64(1.5), cowrie.Float64(2.5), false},
		{"uint same", cowrie.Uint64(42), cowrie.Uint64(42), true},
		{"uint diff", cowrie.Uint64(42), cowrie.Uint64(43), false},
		{"bytes same", cowrie.Bytes([]byte{1, 2}), cowrie.Bytes([]byte{1, 2}), true},
		{"bytes diff", cowrie.Bytes([]byte{1, 2}), cowrie.Bytes([]byte{3, 4}), false},
		{"datetime same", cowrie.Datetime64(100), cowrie.Datetime64(100), true},
		{"datetime diff", cowrie.Datetime64(100), cowrie.Datetime64(200), false},
		{
			"array len mismatch",
			cowrie.Array(cowrie.Int64(1)),
			cowrie.Array(cowrie.Int64(1), cowrie.Int64(2)),
			false,
		},
		{
			"object len mismatch",
			cowrie.Object(cowrie.Member{Key: "a", Value: cowrie.Int64(1)}),
			cowrie.Object(
				cowrie.Member{Key: "a", Value: cowrie.Int64(1)},
				cowrie.Member{Key: "b", Value: cowrie.Int64(2)},
			),
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := valuesEqual(tt.a, tt.b); got != tt.equal {
				t.Errorf("valuesEqual() = %v, want %v", got, tt.equal)
			}
		})
	}
}
