package delta

import (
	"testing"

	"github.com/Neumenon/cowrie/go"
)

func TestComputeDelta_AddedField(t *testing.T) {
	base := cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(1)},
	)
	target := cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(1)},
		cowrie.Member{Key: "y", Value: cowrie.Int64(2)},
	)

	result := Compute(base, target, DefaultConfig())

	if result.FullValue != nil {
		t.Error("expected delta, got full value")
	}
	if result.Delta == nil {
		t.Fatal("expected delta, got nil")
	}
	if len(result.Delta.Ops) != 1 {
		t.Errorf("expected 1 op, got %d", len(result.Delta.Ops))
	}
	if result.Stats.FieldsAdded != 1 {
		t.Errorf("expected 1 field added, got %d", result.Stats.FieldsAdded)
	}
}

func TestComputeDelta_ChangedField(t *testing.T) {
	base := cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(1)},
	)
	target := cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(99)},
	)

	result := Compute(base, target, DefaultConfig())

	if result.Delta == nil {
		t.Fatal("expected delta")
	}
	if len(result.Delta.Ops) != 1 {
		t.Errorf("expected 1 op, got %d", len(result.Delta.Ops))
	}
	if result.Stats.FieldsChanged != 1 {
		t.Errorf("expected 1 field changed, got %d", result.Stats.FieldsChanged)
	}
}

func TestComputeDelta_DeletedField(t *testing.T) {
	base := cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(1)},
		cowrie.Member{Key: "y", Value: cowrie.Int64(2)},
	)
	target := cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(1)},
	)

	result := Compute(base, target, DefaultConfig())

	if result.Delta == nil {
		t.Fatal("expected delta")
	}
	if len(result.Delta.Ops) != 1 {
		t.Errorf("expected 1 op, got %d", len(result.Delta.Ops))
	}
	if result.Delta.Ops[0].OpCode != cowrie.DeltaOpDeleteField {
		t.Errorf("expected DeleteField op, got %d", result.Delta.Ops[0].OpCode)
	}
	if result.Stats.FieldsRemoved != 1 {
		t.Errorf("expected 1 field removed, got %d", result.Stats.FieldsRemoved)
	}
}

func TestComputeDelta_NoChanges(t *testing.T) {
	base := cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(1)},
		cowrie.Member{Key: "y", Value: cowrie.String("hello")},
	)
	target := cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(1)},
		cowrie.Member{Key: "y", Value: cowrie.String("hello")},
	)

	result := Compute(base, target, DefaultConfig())

	if result.Delta == nil {
		t.Fatal("expected empty delta")
	}
	if len(result.Delta.Ops) != 0 {
		t.Errorf("expected 0 ops, got %d", len(result.Delta.Ops))
	}
}

func TestComputeDelta_NilBase(t *testing.T) {
	target := cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(1)},
	)

	result := Compute(nil, target, DefaultConfig())

	if result.FullValue == nil {
		t.Error("expected full value for nil base")
	}
	if result.Delta != nil {
		t.Error("expected nil delta for nil base")
	}
}

func TestApplyDelta(t *testing.T) {
	base := cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(1)},
	)

	delta := &cowrie.DeltaData{
		BaseID: 0,
		Ops: []cowrie.DeltaOp{
			{
				OpCode:  cowrie.DeltaOpSetField,
				FieldID: 0,
				Value:   cowrie.Int64(99),
			},
		},
	}

	fieldDict := []string{"x"}

	result, err := Apply(base, delta, fieldDict)
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}

	// Check result has changed field
	members := result.Members()
	found := false
	for _, m := range members {
		if m.Key == "x" && m.Value.Int64() == 99 {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected field x to be updated to 99")
	}
}

func TestBaseStore(t *testing.T) {
	store := NewBaseStore(StoreConfig{MaxEntries: 3})

	// Store some values
	id1 := store.Store(cowrie.Object(cowrie.Member{Key: "a", Value: cowrie.Int64(1)}))
	id2 := store.Store(cowrie.Object(cowrie.Member{Key: "b", Value: cowrie.Int64(2)}))
	id3 := store.Store(cowrie.Object(cowrie.Member{Key: "c", Value: cowrie.Int64(3)}))

	if store.Size() != 3 {
		t.Errorf("expected size 3, got %d", store.Size())
	}

	// Retrieve values
	e1, ok := store.Get(id1)
	if !ok {
		t.Error("expected to find id1")
	}
	if e1.Value.Members()[0].Value.Int64() != 1 {
		t.Error("wrong value for id1")
	}

	// Add one more, should evict oldest (id1)
	store.Store(cowrie.Object(cowrie.Member{Key: "d", Value: cowrie.Int64(4)}))

	_, ok = store.Get(id1)
	if ok {
		t.Error("id1 should have been evicted")
	}

	_, ok = store.Get(id2)
	if !ok {
		t.Error("id2 should still exist")
	}
	_, ok = store.Get(id3)
	if !ok {
		t.Error("id3 should still exist")
	}
}

func TestBaseStore_ComputeAndStore(t *testing.T) {
	store := NewBaseStore(DefaultStoreConfig())

	// Store initial value
	initial := cowrie.Object(
		cowrie.Member{Key: "count", Value: cowrie.Int64(0)},
		cowrie.Member{Key: "name", Value: cowrie.String("test")},
	)
	baseID := store.Store(initial)

	// Compute delta for update
	updated := cowrie.Object(
		cowrie.Member{Key: "count", Value: cowrie.Int64(1)},
		cowrie.Member{Key: "name", Value: cowrie.String("test")},
	)

	result, newID := store.ComputeAndStore(baseID, updated, DefaultConfig())

	if !result.Stats.UsedDelta {
		t.Error("expected delta to be used")
	}
	if result.Delta == nil {
		t.Error("expected delta in result")
	}
	if result.Delta.BaseID != baseID {
		t.Errorf("expected baseID %d, got %d", baseID, result.Delta.BaseID)
	}
	if newID <= baseID {
		t.Error("new ID should be greater than base ID")
	}
}

func TestValuesEqual(t *testing.T) {
	tests := []struct {
		name  string
		a     *cowrie.Value
		b     *cowrie.Value
		equal bool
	}{
		{"nil nil", nil, nil, true},
		{"nil value", nil, cowrie.Null(), false},
		{"null null", cowrie.Null(), cowrie.Null(), true},
		{"int same", cowrie.Int64(42), cowrie.Int64(42), true},
		{"int diff", cowrie.Int64(42), cowrie.Int64(43), false},
		{"string same", cowrie.String("hello"), cowrie.String("hello"), true},
		{"string diff", cowrie.String("hello"), cowrie.String("world"), false},
		{"bool same", cowrie.Bool(true), cowrie.Bool(true), true},
		{"bool diff", cowrie.Bool(true), cowrie.Bool(false), false},
		{"type mismatch", cowrie.Int64(1), cowrie.String("1"), false},
		{
			"array same",
			cowrie.Array(cowrie.Int64(1), cowrie.Int64(2)),
			cowrie.Array(cowrie.Int64(1), cowrie.Int64(2)),
			true,
		},
		{
			"array diff",
			cowrie.Array(cowrie.Int64(1), cowrie.Int64(2)),
			cowrie.Array(cowrie.Int64(1), cowrie.Int64(3)),
			false,
		},
		{
			"object same",
			cowrie.Object(cowrie.Member{Key: "a", Value: cowrie.Int64(1)}),
			cowrie.Object(cowrie.Member{Key: "a", Value: cowrie.Int64(1)}),
			true,
		},
		{
			"object diff",
			cowrie.Object(cowrie.Member{Key: "a", Value: cowrie.Int64(1)}),
			cowrie.Object(cowrie.Member{Key: "a", Value: cowrie.Int64(2)}),
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

// Benchmark delta computation
func BenchmarkComputeDelta_SmallObject(b *testing.B) {
	base := cowrie.Object(
		cowrie.Member{Key: "id", Value: cowrie.Int64(1)},
		cowrie.Member{Key: "name", Value: cowrie.String("test")},
		cowrie.Member{Key: "value", Value: cowrie.Float64(3.14)},
	)
	target := cowrie.Object(
		cowrie.Member{Key: "id", Value: cowrie.Int64(1)},
		cowrie.Member{Key: "name", Value: cowrie.String("test")},
		cowrie.Member{Key: "value", Value: cowrie.Float64(2.71)}, // Changed
	)
	cfg := DefaultConfig()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Compute(base, target, cfg)
	}
}

func BenchmarkComputeDelta_LargeObject(b *testing.B) {
	// Object with 50 fields
	baseMembers := make([]cowrie.Member, 50)
	targetMembers := make([]cowrie.Member, 50)
	for i := 0; i < 50; i++ {
		baseMembers[i] = cowrie.Member{
			Key:   string(rune('a' + i%26)),
			Value: cowrie.Int64(int64(i)),
		}
		targetMembers[i] = baseMembers[i]
	}
	// Change 3 fields
	targetMembers[10].Value = cowrie.Int64(999)
	targetMembers[25].Value = cowrie.Int64(888)
	targetMembers[40].Value = cowrie.Int64(777)

	base := cowrie.Object(baseMembers...)
	target := cowrie.Object(targetMembers...)
	cfg := DefaultConfig()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Compute(base, target, cfg)
	}
}
