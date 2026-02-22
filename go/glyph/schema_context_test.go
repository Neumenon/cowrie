package glyph

import (
	"strings"
	"testing"
)

func TestNewSchemaContext(t *testing.T) {
	keys := []string{"role", "content", "tool_calls"}
	ctx := NewSchemaContext(keys)

	if ctx.Len() != 3 {
		t.Errorf("Len() = %d, want 3", ctx.Len())
	}

	// Check key lookup
	if id := ctx.LookupKey("role"); id != 0 {
		t.Errorf("LookupKey(role) = %d, want 0", id)
	}
	if id := ctx.LookupKey("content"); id != 1 {
		t.Errorf("LookupKey(content) = %d, want 1", id)
	}
	if id := ctx.LookupKey("tool_calls"); id != 2 {
		t.Errorf("LookupKey(tool_calls) = %d, want 2", id)
	}
	if id := ctx.LookupKey("unknown"); id != -1 {
		t.Errorf("LookupKey(unknown) = %d, want -1", id)
	}

	// Check ID lookup
	if key := ctx.LookupID(0); key != "role" {
		t.Errorf("LookupID(0) = %q, want role", key)
	}
	if key := ctx.LookupID(2); key != "tool_calls" {
		t.Errorf("LookupID(2) = %q, want tool_calls", key)
	}
	if key := ctx.LookupID(100); key != "" {
		t.Errorf("LookupID(100) = %q, want empty", key)
	}

	// Check HasKey
	if !ctx.HasKey("role") {
		t.Error("HasKey(role) should be true")
	}
	if ctx.HasKey("unknown") {
		t.Error("HasKey(unknown) should be false")
	}
}

func TestSchemaContext_ComputeID(t *testing.T) {
	keys := []string{"role", "content", "tool_calls"}
	ctx1 := NewSchemaContext(keys)
	ctx2 := NewSchemaContext(keys)

	// Same keys should produce same ID
	if ctx1.ID != ctx2.ID {
		t.Errorf("Same keys should produce same ID: %q != %q", ctx1.ID, ctx2.ID)
	}

	// Different keys should produce different ID
	ctx3 := NewSchemaContext([]string{"a", "b", "c"})
	if ctx1.ID == ctx3.ID {
		t.Error("Different keys should produce different ID")
	}

	// ID should be 8 chars
	if len(ctx1.ID) != 8 {
		t.Errorf("ID length = %d, want 8", len(ctx1.ID))
	}
}

func TestSchemaContext_EmitHeader(t *testing.T) {
	ctx := NewSchemaContextWithID("abc123", []string{"role", "content"})

	// Inline mode
	inline := ctx.EmitHeader(true)
	want := "@schema#abc123 @keys=[role content]"
	if inline != want {
		t.Errorf("EmitHeader(true) = %q, want %q", inline, want)
	}

	// Reference mode
	ref := ctx.EmitHeader(false)
	want = "@schema#abc123"
	if ref != want {
		t.Errorf("EmitHeader(false) = %q, want %q", ref, want)
	}
}

func TestSchemaContext_EmitHeader_QuotedKeys(t *testing.T) {
	ctx := NewSchemaContextWithID("x", []string{"simple", "with space", "has\"quote"})

	header := ctx.EmitHeader(true)
	// Keys with spaces/quotes should be quoted
	if header != `@schema#x @keys=[simple "with space" "has\"quote"]` {
		t.Errorf("EmitHeader with quoted keys = %q", header)
	}
}

func TestSchemaRegistry(t *testing.T) {
	reg := NewSchemaRegistry()

	ctx := NewSchemaContextWithID("S1", []string{"role", "content"})
	reg.Define(ctx)

	// Get existing
	got := reg.Get("S1")
	if got == nil {
		t.Fatal("Get(S1) should return context")
	}
	if got.ID != "S1" {
		t.Errorf("ID = %q, want S1", got.ID)
	}

	// Get non-existent
	if reg.Get("S2") != nil {
		t.Error("Get(S2) should return nil")
	}

	// Active schema
	if reg.Active() == nil || reg.Active().ID != "S1" {
		t.Error("Active should be S1")
	}

	// Define another and switch
	ctx2 := NewSchemaContextWithID("S2", []string{"a", "b"})
	reg.Define(ctx2)
	if reg.Active().ID != "S2" {
		t.Error("Active should be S2 after Define")
	}

	// SetActive back to S1
	if err := reg.SetActive("S1"); err != nil {
		t.Errorf("SetActive failed: %v", err)
	}
	if reg.Active().ID != "S1" {
		t.Error("Active should be S1 after SetActive")
	}

	// SetActive non-existent
	if err := reg.SetActive("S99"); err == nil {
		t.Error("SetActive(S99) should fail")
	}

	// Clear
	reg.Clear("S1")
	if reg.Get("S1") != nil {
		t.Error("S1 should be cleared")
	}
	if reg.Active() != nil {
		t.Error("Active should be nil after clearing active schema")
	}

	// ClearAll
	reg.Define(ctx)
	reg.Define(ctx2)
	reg.ClearAll()
	if reg.Get("S1") != nil || reg.Get("S2") != nil {
		t.Error("ClearAll should remove all schemas")
	}
}

func TestParseSchemaDirective(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantID   string
		wantKeys []string
		wantDef  bool
		wantErr  bool
	}{
		{
			name:     "inline_define",
			input:    "@schema#abc123 @keys=[role content tool_calls]",
			wantID:   "abc123",
			wantKeys: []string{"role", "content", "tool_calls"},
			wantDef:  true,
		},
		{
			name:     "reference_only",
			input:    "@schema#abc123",
			wantID:   "abc123",
			wantKeys: nil,
			wantDef:  false,
		},
		{
			name:     "quoted_keys",
			input:    `@schema#x @keys=["with space" simple]`,
			wantID:   "x",
			wantKeys: []string{"with space", "simple"},
			wantDef:  true,
		},
		{
			name:    "clear_directive",
			input:   "@schema.clear",
			wantID:  "",
			wantDef: false,
		},
		{
			name:    "invalid_prefix",
			input:   "@invalid",
			wantErr: true,
		},
		{
			name:    "unclosed_keys",
			input:   "@schema#x @keys=[a b",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, isDef, err := ParseSchemaDirective(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			if isDef != tt.wantDef {
				t.Errorf("isDefine = %v, want %v", isDef, tt.wantDef)
			}

			if tt.input == "@schema.clear" {
				if ctx != nil {
					t.Error("clear should return nil context")
				}
				return
			}

			if ctx.ID != tt.wantID {
				t.Errorf("ID = %q, want %q", ctx.ID, tt.wantID)
			}

			if tt.wantKeys != nil {
				if len(ctx.Keys) != len(tt.wantKeys) {
					t.Errorf("Keys len = %d, want %d", len(ctx.Keys), len(tt.wantKeys))
				} else {
					for i, k := range tt.wantKeys {
						if ctx.Keys[i] != k {
							t.Errorf("Keys[%d] = %q, want %q", i, ctx.Keys[i], k)
						}
					}
				}
			}
		})
	}
}

func TestIsNumericKey(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"0", true},
		{"1", true},
		{"42", true},
		{"123", true},
		{"", false},
		{"a", false},
		{"1a", false},
		{"a1", false},
		{"-1", false},
		{"1.5", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := IsNumericKey(tt.input)
			if got != tt.want {
				t.Errorf("IsNumericKey(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseNumericKey(t *testing.T) {
	tests := []struct {
		input   string
		want    int
		wantErr bool
	}{
		{"0", 0, false},
		{"42", 42, false},
		{"123", 123, false},
		{"abc", 0, true},
		{"", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseNumericKey(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && got != tt.want {
				t.Errorf("ParseNumericKey(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestSchemaRoundTrip(t *testing.T) {
	// Create a schema context
	schema := NewSchemaContext([]string{"role", "content", "tool_calls"})

	// Create a map value
	v := Map(
		MapEntry{Key: "role", Value: Str("user")},
		MapEntry{Key: "content", Value: Str("Hello, world!")},
	)

	// Encode with schema
	opts := SchemaLooseCanonOpts(schema)
	encoded := CanonicalizeLooseWithSchema(v, opts)

	// Should contain @schema header and compact keys
	if !strings.Contains(encoded, "@schema#") {
		t.Errorf("Expected @schema header, got:\n%s", encoded)
	}
	if !strings.Contains(encoded, "#0=") || !strings.Contains(encoded, "#1=") {
		t.Errorf("Expected compact keys #0 and #1, got:\n%s", encoded)
	}

	// Parse back with registry
	registry := NewSchemaRegistry()
	parsed, ctx, err := ParseLoosePayload(encoded, registry)
	if err != nil {
		t.Fatalf("ParseLoosePayload error: %v", err)
	}

	// Verify schema was parsed
	if ctx == nil {
		t.Fatal("Expected schema context")
	}
	if ctx.ID != schema.ID {
		t.Errorf("Schema ID = %q, want %q", ctx.ID, schema.ID)
	}

	// Verify value was parsed correctly
	if parsed == nil {
		t.Fatal("Expected parsed value")
	}
	if parsed.typ != TypeMap {
		t.Fatalf("Expected map, got %v", parsed.typ)
	}

	roleVal := parsed.Get("role")
	if roleVal == nil || mustAsStr(t, roleVal) != "user" {
		t.Errorf("role = %v, want 'user'", roleVal)
	}

	contentVal := parsed.Get("content")
	if contentVal == nil || mustAsStr(t, contentVal) != "Hello, world!" {
		t.Errorf("content = %v, want 'Hello, world!'", contentVal)
	}
}

func TestSchemaRoundTrip_MixedKeys(t *testing.T) {
	// Create a schema with only some keys
	schema := NewSchemaContext([]string{"role", "content"})

	// Create a map with extra keys not in schema
	v := Map(
		MapEntry{Key: "role", Value: Str("user")},
		MapEntry{Key: "content", Value: Str("Hello")},
		MapEntry{Key: "extra_field", Value: Int(42)},
	)

	// Encode with schema
	opts := SchemaLooseCanonOpts(schema)
	encoded := CanonicalizeLooseWithSchema(v, opts)

	// Should have compact keys for role/content but string key for extra_field
	if !strings.Contains(encoded, "#0=") {
		t.Errorf("Expected #0 for role")
	}
	if !strings.Contains(encoded, "extra_field=") {
		t.Errorf("Expected string key for extra_field, got:\n%s", encoded)
	}

	// Parse back
	registry := NewSchemaRegistry()
	parsed, _, err := ParseLoosePayload(encoded, registry)
	if err != nil {
		t.Fatalf("ParseLoosePayload error: %v", err)
	}

	// Verify all values
	if mustAsStr(t, parsed.Get("role")) != "user" {
		t.Error("role mismatch")
	}
	if mustAsStr(t, parsed.Get("content")) != "Hello" {
		t.Error("content mismatch")
	}
	if mustAsInt(t, parsed.Get("extra_field")) != 42 {
		t.Error("extra_field mismatch")
	}
}

func TestSchemaRoundTrip_NestedObjects(t *testing.T) {
	schema := NewSchemaContext([]string{"name", "value", "items"})

	v := Map(
		MapEntry{Key: "name", Value: Str("test")},
		MapEntry{Key: "items", Value: List(
			Map(MapEntry{Key: "name", Value: Str("item1")}, MapEntry{Key: "value", Value: Int(1)}),
			Map(MapEntry{Key: "name", Value: Str("item2")}, MapEntry{Key: "value", Value: Int(2)}),
		)},
	)

	opts := SchemaLooseCanonOpts(schema)
	encoded := CanonicalizeLooseWithSchema(v, opts)

	registry := NewSchemaRegistry()
	parsed, _, err := ParseLoosePayload(encoded, registry)
	if err != nil {
		t.Fatalf("ParseLoosePayload error: %v", err)
	}

	// Verify nested structure
	items := parsed.Get("items")
	if items == nil || items.typ != TypeList {
		t.Fatal("Expected items list")
	}
	if len(items.listVal) != 2 {
		t.Errorf("Expected 2 items, got %d", len(items.listVal))
	}

	item1 := items.listVal[0]
	if mustAsStr(t, item1.Get("name")) != "item1" {
		t.Error("item1 name mismatch")
	}
}

func TestSchemaReference(t *testing.T) {
	// Pre-register a schema
	registry := NewSchemaRegistry()
	schema := NewSchemaContextWithID("abc123", []string{"role", "content"})
	registry.Define(schema)

	// Parse a payload that references the schema by ID only
	input := "@schema#abc123\n{#0=user #1=Hello}"

	parsed, ctx, err := ParseLoosePayload(input, registry)
	if err != nil {
		t.Fatalf("ParseLoosePayload error: %v", err)
	}

	if ctx == nil || ctx.ID != "abc123" {
		t.Error("Expected schema context with ID abc123")
	}

	if mustAsStr(t, parsed.Get("role")) != "user" {
		t.Error("role should be 'user'")
	}
}

func TestSchemaClear(t *testing.T) {
	registry := NewSchemaRegistry()
	schema := NewSchemaContext([]string{"a", "b"})
	registry.Define(schema)

	if registry.Active() == nil {
		t.Fatal("Expected active schema")
	}

	// Clear with @schema.clear directive
	input := "@schema.clear\n{a=1 b=2}"
	parsed, ctx, err := ParseLoosePayload(input, registry)
	if err != nil {
		t.Fatalf("ParseLoosePayload error: %v", err)
	}

	if ctx != nil {
		t.Error("Expected nil context after clear")
	}
	if registry.Active() != nil {
		t.Error("Registry active should be nil after clear")
	}

	// Value should still parse correctly with string keys
	if mustAsInt(t, parsed.Get("a")) != 1 {
		t.Error("a should be 1")
	}
}

func TestSchemaRegistry_LRUEviction(t *testing.T) {
	// Create registry with small capacity
	reg := NewSchemaRegistryWithSize(3)

	// Add 3 schemas - fills capacity
	reg.Define(NewSchemaContextWithID("A", []string{"a"}))
	reg.Define(NewSchemaContextWithID("B", []string{"b"}))
	reg.Define(NewSchemaContextWithID("C", []string{"c"}))

	// Verify all 3 exist
	if reg.Get("A") == nil || reg.Get("B") == nil || reg.Get("C") == nil {
		t.Fatal("All 3 schemas should exist")
	}

	// Access A to make it recently used (moves to front of LRU)
	reg.Get("A")

	// Add D - should evict B (least recently used after A was accessed)
	// Order before D: B, C, A (B is LRU because A was accessed, C was accessed in verify)
	// Actually after the verify loop: A, B, C (A accessed last)
	// So B should be evicted as it's least recently used
	reg.Define(NewSchemaContextWithID("D", []string{"d"}))

	// Verify LRU eviction
	if reg.Get("A") == nil {
		t.Error("A should still exist (was accessed recently)")
	}
	if reg.Get("C") == nil {
		t.Error("C should still exist")
	}
	if reg.Get("D") == nil {
		t.Error("D should exist (just added)")
	}
	// B should be evicted as it was the least recently used
	// Note: because of the verify loop above, all were accessed.
	// Let's test more carefully with fresh registry
}

func TestSchemaRegistry_LRUEviction_Precise(t *testing.T) {
	// Create registry with capacity 3
	reg := NewSchemaRegistryWithSize(3)

	// Add A, B, C in order
	reg.Define(NewSchemaContextWithID("A", []string{"a"}))
	reg.Define(NewSchemaContextWithID("B", []string{"b"}))
	reg.Define(NewSchemaContextWithID("C", []string{"c"}))

	// At this point, LRU order is: A (oldest) -> B -> C (newest)

	// Access A - moves it to most recent
	_ = reg.Get("A")
	// LRU order is now: B (oldest) -> C -> A (newest)

	// Add D - should evict B (the LRU)
	reg.Define(NewSchemaContextWithID("D", []string{"d"}))

	// Check results without accessing (to not change LRU order)
	if reg.Len() != 3 {
		t.Errorf("Registry should have 3 schemas, got %d", reg.Len())
	}

	// Now verify which ones exist
	// A should exist (was accessed)
	if reg.Get("A") == nil {
		t.Error("A should exist - was accessed before D was added")
	}
	// B should be evicted (was LRU)
	// We need to check without Get() which would panic if nil
	// Use the internal check via Len comparison
	// Actually Get returns nil for missing, so:
	bCtx := reg.Get("B")
	if bCtx != nil {
		t.Error("B should be evicted - it was LRU")
	}
	// C should exist
	if reg.Get("C") == nil {
		t.Error("C should exist")
	}
	// D should exist (just added)
	if reg.Get("D") == nil {
		t.Error("D should exist - just added")
	}
}

func TestSchemaRegistry_LRUEviction_SetActive(t *testing.T) {
	reg := NewSchemaRegistryWithSize(2)

	reg.Define(NewSchemaContextWithID("A", []string{"a"}))
	reg.Define(NewSchemaContextWithID("B", []string{"b"}))

	// SetActive on A makes it recently used
	reg.SetActive("A")

	// Add C - should evict B (LRU)
	reg.Define(NewSchemaContextWithID("C", []string{"c"}))

	if reg.Get("A") == nil {
		t.Error("A should exist - SetActive made it recently used")
	}
	if reg.Get("B") != nil {
		t.Error("B should be evicted")
	}
	if reg.Get("C") == nil {
		t.Error("C should exist")
	}
}

func TestSchemaRegistry_UpdateExisting(t *testing.T) {
	reg := NewSchemaRegistryWithSize(2)

	reg.Define(NewSchemaContextWithID("A", []string{"a"}))
	reg.Define(NewSchemaContextWithID("B", []string{"b"}))

	// Update A with new keys - should not evict anything
	reg.Define(NewSchemaContextWithID("A", []string{"a", "a2"}))

	if reg.Len() != 2 {
		t.Errorf("Should still have 2 schemas, got %d", reg.Len())
	}

	// Verify A was updated
	ctx := reg.Get("A")
	if ctx == nil {
		t.Fatal("A should exist")
	}
	if len(ctx.Keys) != 2 {
		t.Errorf("A should have 2 keys, got %d", len(ctx.Keys))
	}
}
