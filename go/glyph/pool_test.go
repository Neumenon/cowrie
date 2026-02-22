package glyph

import (
	"fmt"
	"sync"
	"testing"
)

func TestIsPoolRef(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"S1", true},
		{"O1", true},
		{"P42", true},
		{"ABC123", true},
		{"m", false},       // lowercase - entity prefix
		{"t", false},       // lowercase
		{"S", false},       // no digit
		{"1S", false},      // starts with digit
		{"", false},        // empty
		{"ARS-LIV", false}, // contains dash
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := IsPoolRef(tt.input)
			if got != tt.want {
				t.Errorf("IsPoolRef(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParsePoolRef(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    PoolRef
		wantErr bool
	}{
		{
			name:  "simple",
			input: "^S1:0",
			want:  PoolRef{PoolID: "S1", Index: 0},
		},
		{
			name:  "higher_index",
			input: "^S1:42",
			want:  PoolRef{PoolID: "S1", Index: 42},
		},
		{
			name:  "object_pool",
			input: "^O1:5",
			want:  PoolRef{PoolID: "O1", Index: 5},
		},
		{
			name:    "missing_caret",
			input:   "S1:0",
			wantErr: true,
		},
		{
			name:    "entity_ref_not_pool",
			input:   "^m:ARS",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePoolRef(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePoolRef() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if got.PoolID != tt.want.PoolID {
				t.Errorf("PoolID = %q, want %q", got.PoolID, tt.want.PoolID)
			}
			if got.Index != tt.want.Index {
				t.Errorf("Index = %d, want %d", got.Index, tt.want.Index)
			}
		})
	}
}

func TestPoolRef_String(t *testing.T) {
	ref := PoolRef{PoolID: "S1", Index: 5}
	want := "^S1:5"
	got := ref.String()
	if got != want {
		t.Errorf("PoolRef.String() = %q, want %q", got, want)
	}
}

func TestPool(t *testing.T) {
	pool := &Pool{
		ID:   "S1",
		Kind: PoolKindString,
	}

	// Add entries
	idx0 := pool.Add(Str("Hello"))
	idx1 := pool.Add(Str("World"))
	idx2 := pool.Add(Int(42))

	if idx0 != 0 || idx1 != 1 || idx2 != 2 {
		t.Errorf("Indices should be 0, 1, 2, got %d, %d, %d", idx0, idx1, idx2)
	}

	// Get entries
	v0, err := pool.Get(0)
	if err != nil || v0 == nil || mustAsStr(t, v0) != "Hello" {
		t.Error("Get(0) should return Hello")
	}

	v2, err := pool.Get(2)
	if err != nil || v2 == nil || mustAsInt(t, v2) != 42 {
		t.Error("Get(2) should return 42")
	}

	// Out of range
	_, err = pool.Get(-1)
	if err == nil {
		t.Error("Get(-1) should return error")
	}
	_, err = pool.Get(100)
	if err == nil {
		t.Error("Get(100) should return error")
	}
}

func TestPool_String(t *testing.T) {
	pool := &Pool{
		ID:   "S1",
		Kind: PoolKindString,
		Entries: []*GValue{
			Str("hello"),
			Str("world"),
			Int(42),
		},
	}

	got := pool.String()
	want := "@pool.str id=S1 [hello world 42]"
	if got != want {
		t.Errorf("Pool.String() = %q, want %q", got, want)
	}
}

func TestPoolRegistry(t *testing.T) {
	registry := NewPoolRegistry()

	// Define pool
	pool := &Pool{
		ID:   "S1",
		Kind: PoolKindString,
		Entries: []*GValue{
			Str("system prompt"),
			Str("tool:search"),
		},
	}
	registry.Define(pool)

	// Get pool
	got := registry.Get("S1")
	if got == nil {
		t.Fatal("Get should return pool")
	}
	if got.ID != "S1" {
		t.Errorf("Pool ID = %q, want S1", got.ID)
	}

	// Non-existent pool
	if registry.Get("S2") != nil {
		t.Error("Get should return nil for non-existent pool")
	}

	// Resolve reference
	ref := PoolRef{PoolID: "S1", Index: 0}
	resolved, err := registry.Resolve(ref)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if resolved == nil || mustAsStr(t, resolved) != "system prompt" {
		t.Error("Resolve should return correct value")
	}

	// Resolve non-existent
	badRef := PoolRef{PoolID: "S2", Index: 0}
	_, err = registry.Resolve(badRef)
	if err == nil {
		t.Error("Resolve should return error for non-existent pool")
	}

	// Clear
	registry.Clear("S1")
	if registry.Get("S1") != nil {
		t.Error("Pool should be cleared")
	}
}

func TestParsePool(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantID  string
		wantLen int
		wantErr bool
	}{
		{
			name:    "string_pool",
			input:   `@pool.str id=S1 [hello world]`,
			wantID:  "S1",
			wantLen: 2,
		},
		{
			name:    "with_quoted",
			input:   `@pool.str id=S1 ["hello world" simple]`,
			wantID:  "S1",
			wantLen: 2,
		},
		{
			name:    "object_pool",
			input:   `@pool.obj id=O1 [42 t]`,
			wantID:  "O1",
			wantLen: 2,
		},
		{
			name:    "missing_id",
			input:   `@pool.str [hello]`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePool(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePool() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if got.ID != tt.wantID {
				t.Errorf("Pool ID = %q, want %q", got.ID, tt.wantID)
			}
			if len(got.Entries) != tt.wantLen {
				t.Errorf("Entries len = %d, want %d", len(got.Entries), tt.wantLen)
			}
		})
	}
}

func TestParsePool_FullGlyphValues(t *testing.T) {
	input := `@pool.obj id=O1 [ErrorResponse{code=500 message="Bad Request"} [1 2 3] {a=1 b=2} ^S1:0]`

	pool, err := ParsePool(input)
	if err != nil {
		t.Fatalf("ParsePool failed: %v", err)
	}
	if pool.ID != "O1" {
		t.Fatalf("Pool ID = %q, want %q", pool.ID, "O1")
	}
	if len(pool.Entries) != 4 {
		t.Fatalf("Entries len = %d, want 4", len(pool.Entries))
	}

	entry0Val := pool.Entries[0]
	entry0 := mustAsStruct(t, entry0Val)
	if entry0.TypeName != "ErrorResponse" {
		t.Fatalf("entry0 type = %q, want %q", entry0.TypeName, "ErrorResponse")
	}
	if mustAsInt(t, entry0Val.Get("code")) != 500 {
		t.Errorf("entry0.code = %d, want 500", mustAsInt(t, entry0Val.Get("code")))
	}
	if mustAsStr(t, entry0Val.Get("message")) != "Bad Request" {
		t.Errorf("entry0.message = %q, want %q", mustAsStr(t, entry0Val.Get("message")), "Bad Request")
	}

	list := mustAsList(t, pool.Entries[1])
	if len(list) != 3 || mustAsInt(t, list[0]) != 1 || mustAsInt(t, list[1]) != 2 || mustAsInt(t, list[2]) != 3 {
		t.Errorf("entry1 list mismatch: got %v", list)
	}

	m := pool.Entries[2]
	if mustAsInt(t, m.Get("a")) != 1 || mustAsInt(t, m.Get("b")) != 2 {
		t.Errorf("entry2 map mismatch: got %s", CanonicalizeLoose(m))
	}

	if _, err := pool.Entries[3].AsID(); err != nil {
		t.Errorf("entry3 expected ID, got error: %v", err)
	}
}

func TestParsePool_StringPoolTypeEnforced(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{name: "int", input: `@pool.str id=S1 [hello 42]`},
		{name: "null", input: `@pool.str id=S1 [null]`},
		{name: "struct", input: `@pool.str id=S1 [Error{code=1}]`},
		{name: "list", input: `@pool.str id=S1 [[1 2]]`},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParsePool(tt.input); err == nil {
				t.Fatalf("expected error for input: %s", tt.input)
			}
		})
	}
}

func TestParsePool_RoundTrip(t *testing.T) {
	pool := &Pool{
		ID:   "O9",
		Kind: PoolKindObject,
		Entries: []*GValue{
			Struct("ErrorResponse",
				MapEntry{Key: "code", Value: Int(500)},
				MapEntry{Key: "message", Value: Str("Bad Request")},
			),
			List(Int(1), Int(2), Int(3)),
			Map(
				MapEntry{Key: "a", Value: Int(1)},
				MapEntry{Key: "b", Value: Int(2)},
			),
			ID("S1", "0"),
		},
	}

	encoded := EmitPool(pool)
	parsed, err := ParsePool(encoded)
	if err != nil {
		t.Fatalf("ParsePool failed: %v", err)
	}
	if parsed.ID != pool.ID || parsed.Kind != pool.Kind {
		t.Fatalf("parsed pool mismatch: got id=%q kind=%v", parsed.ID, parsed.Kind)
	}
	if len(parsed.Entries) != len(pool.Entries) {
		t.Fatalf("entry count mismatch: got %d want %d", len(parsed.Entries), len(pool.Entries))
	}
	for i := range pool.Entries {
		if CanonicalizeLoose(parsed.Entries[i]) != CanonicalizeLoose(pool.Entries[i]) {
			t.Fatalf("entry %d mismatch: got %s want %s", i, CanonicalizeLoose(parsed.Entries[i]), CanonicalizeLoose(pool.Entries[i]))
		}
	}
}

func TestParsePool_Errors(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{name: "missing_entries", input: `@pool.str id=S1 `},
		{name: "unterminated", input: `@pool.obj id=O1 [1 2 3`},
		{name: "trailing_content", input: `@pool.obj id=O1 [1 2] extra`},
		{name: "unbalanced", input: `@pool.obj id=O1 [1 [2]`},
		{name: "bad_prefix", input: `@pool.x id=S1 [a]`},
		{name: "missing_id", input: `@pool.str [a]`},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParsePool(tt.input); err == nil {
				t.Fatalf("expected error for input: %s", tt.input)
			}
		})
	}
}

func TestPoolRefValue(t *testing.T) {
	v := PoolRefValue("S1", 5)

	if !v.IsPoolRef() {
		t.Error("IsPoolRef should return true")
	}

	ref, err := v.AsPoolRef()
	if err != nil {
		t.Fatalf("AsPoolRef error: %v", err)
	}
	if ref.PoolID != "S1" {
		t.Errorf("PoolID = %q, want S1", ref.PoolID)
	}
	if ref.Index != 5 {
		t.Errorf("Index = %d, want 5", ref.Index)
	}
}

func TestAutoInterner(t *testing.T) {
	registry := NewPoolRegistry()
	interner := NewAutoInterner(registry, AutoInternOpts{
		MinLength:   10,
		MinOccurs:   2,
		MaxPoolSize: 100,
	})

	shortString := "short"
	longString := "This is a longer string that should be interned"

	// Short string - never interned
	v1 := interner.Process(shortString)
	v2 := interner.Process(shortString)
	if v1.IsPoolRef() || v2.IsPoolRef() {
		t.Error("Short strings should not be interned")
	}

	// Long string - first occurrence
	v3 := interner.Process(longString)
	if v3.IsPoolRef() {
		t.Error("First occurrence should not be interned")
	}

	// Long string - second occurrence (should intern)
	v4 := interner.Process(longString)
	if !v4.IsPoolRef() {
		t.Error("Second occurrence should be interned")
	}

	// Verify pool was created
	pool := registry.Get("S1")
	if pool == nil {
		t.Error("Pool should be created")
	}

	// Verify reference resolves correctly
	ref, err := v4.AsPoolRef()
	if err != nil {
		t.Fatalf("AsPoolRef failed: %v", err)
	}
	resolved, err := registry.Resolve(ref)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if resolved == nil || mustAsStr(t, resolved) != longString {
		t.Error("Reference should resolve to original string")
	}
}

func TestPoolConcurrentAccess(t *testing.T) {
	registry := NewPoolRegistry()
	interner := NewAutoInterner(registry, AutoInternOpts{
		MinLength:   1,
		MinOccurs:   1,
		MaxPoolSize: 256,
	})

	const (
		workers    = 8
		iterations = 200
	)

	var wg sync.WaitGroup
	errCh := make(chan error, workers*iterations)
	value := "concurrent-string"

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				v := interner.Process(value)
				if v.IsPoolRef() {
					ref, err := v.AsPoolRef()
					if err != nil {
						errCh <- err
						continue
					}
					resolved, err := registry.Resolve(ref)
					if err != nil {
						errCh <- err
						continue
					}
					s, err := resolved.AsStr()
					if err != nil {
						errCh <- err
						continue
					}
					if s != value {
						errCh <- fmt.Errorf("resolved value mismatch: %q", s)
					}
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent pool access error: %v", err)
		}
	}
}

func TestParsePool_EscapedBackslash(t *testing.T) {
	// Test strings with escaped backslashes
	tests := []struct {
		name    string
		input   string
		want    []string
		wantLen int
	}{
		{
			name:    "string_ending_with_backslash",
			input:   `@pool.str id=S1 ["test\\" normal]`,
			want:    []string{`test\`, "normal"},
			wantLen: 2,
		},
		{
			name:    "escaped_quote",
			input:   `@pool.str id=S1 ["say \"hello\"" simple]`,
			want:    []string{`say "hello"`, "simple"},
			wantLen: 2,
		},
		{
			name:    "multiple_escapes",
			input:   `@pool.str id=S1 ["a\\b\\c" "d\"e"]`,
			want:    []string{`a\b\c`, `d"e`},
			wantLen: 2,
		},
		{
			name:    "backslash_then_quote",
			input:   `@pool.str id=S1 ["end\\\\" other]`,
			want:    []string{`end\\`, "other"},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := ParsePool(tt.input)
			if err != nil {
				t.Fatalf("ParsePool failed: %v", err)
			}
			if len(pool.Entries) != tt.wantLen {
				t.Fatalf("Expected %d entries, got %d", tt.wantLen, len(pool.Entries))
			}
			for i, want := range tt.want {
				got := mustAsStr(t, pool.Entries[i])
				if got != want {
					t.Errorf("Entry %d = %q, want %q", i, got, want)
				}
			}
		})
	}
}
