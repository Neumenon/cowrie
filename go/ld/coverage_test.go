package ld

import (
	"bytes"
	"testing"

	"github.com/Neumenon/cowrie/go"
)

func TestContextBuilder_TypedTerms(t *testing.T) {
	doc := NewContextBuilder().
		AddTypedTerm("homepage", "http://schema.org/url", TermFlagID).
		AddListTerm("items", "http://schema.org/itemList").
		AddSetTerm("tags", "http://schema.org/keywords").
		WithRoot(cowrie.Object(
			cowrie.Member{Key: "name", Value: cowrie.String("test")},
		)).
		Build()

	if doc == nil {
		t.Fatal("expected non-nil document")
	}
	if doc.Root == nil {
		t.Fatal("expected non-nil root")
	}
	if doc.Root.Get("name").String() != "test" {
		t.Error("expected name=test")
	}

	// Verify terms were added
	term := doc.LookupTerm("homepage")
	if term == nil {
		t.Error("expected 'homepage' term")
	}
	term = doc.LookupTerm("items")
	if term == nil {
		t.Error("expected 'items' term")
	}
	term = doc.LookupTerm("tags")
	if term == nil {
		t.Error("expected 'tags' term")
	}
}

func TestServiceContext(t *testing.T) {
	ctx := ServiceContext()
	if ctx == nil {
		t.Fatal("ServiceContext returned nil")
	}
	doc := ctx.Build()
	if doc == nil {
		t.Fatal("Build returned nil")
	}
	// Check known terms
	term := doc.LookupTerm("name")
	if term == nil {
		t.Error("expected 'name' term in ServiceContext")
	}
}

func TestIRI_String(t *testing.T) {
	iri := IRI("http://example.com/thing")
	if iri.String() != "http://example.com/thing" {
		t.Errorf("IRI.String() = %q", iri.String())
	}
}

func TestBNode_String(t *testing.T) {
	bn := BNode("_:b0")
	if bn.String() != "_:b0" {
		t.Errorf("BNode.String() = %q", bn.String())
	}
}

func TestEncodeTo(t *testing.T) {
	doc := NewDocument()
	doc.Root = cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(42)},
	)

	var buf bytes.Buffer
	err := EncodeTo(&buf, doc)
	if err != nil {
		t.Fatalf("EncodeTo failed: %v", err)
	}

	// Verify we can decode
	decoded, err := Decode(buf.Bytes())
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if decoded.Root.Get("x").Int64() != 42 {
		t.Error("expected x=42")
	}
}

func TestDecodeFrom(t *testing.T) {
	doc := NewDocument()
	doc.Root = cowrie.Object(
		cowrie.Member{Key: "name", Value: cowrie.String("alice")},
	)

	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrom(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("DecodeFrom failed: %v", err)
	}
	if decoded.Root.Get("name").String() != "alice" {
		t.Error("expected name=alice")
	}
}

func TestEncodeValue(t *testing.T) {
	doc := NewDocument()
	doc.AddTerm("name", "http://schema.org/name", TermFlagNone)

	root := cowrie.Object(
		cowrie.Member{Key: "name", Value: cowrie.String("test")},
	)

	data, err := EncodeValue(root, doc)
	if err != nil {
		t.Fatalf("EncodeValue failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if decoded.Root.Get("name").String() != "test" {
		t.Error("expected name=test")
	}
}

func TestRoundTrip_WithTerms(t *testing.T) {
	doc := NewContextBuilder().
		AddPrefix("schema", "http://schema.org/").
		AddTerm("name", "http://schema.org/name").
		AddIDTerm("homepage", "http://schema.org/url").
		WithRoot(cowrie.Object(
			cowrie.Member{Key: "name", Value: cowrie.String("Alice")},
			cowrie.Member{Key: "homepage", Value: cowrie.String("http://example.com")},
		)).
		Build()

	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Root.Get("name").String() != "Alice" {
		t.Error("expected name=Alice")
	}
}

func TestToolMetadata_ToLDDocument_Coverage(t *testing.T) {
	tm := &ToolMetadata{
		IRI:          "http://example.com/tool/search",
		Name:         "Search Tool",
		Description:  "Searches things",
		Capabilities: []IRI{"http://example.com/cap/search"},
		Version:      "1.0",
	}

	doc := tm.ToLDDocument()
	if doc == nil {
		t.Fatal("expected non-nil document")
	}
	if doc.Root == nil {
		t.Fatal("expected non-nil root")
	}

	// Roundtrip
	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	_, err = Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
}

func TestAgentMetadata_ToLDDocument_Coverage(t *testing.T) {
	am := &AgentMetadata{
		IRI:          "http://example.com/agent/helper",
		Name:         "Helper Agent",
		Description:  "Helps with things",
		Capabilities: []IRI{"http://example.com/cap/help"},
		Tools:        []IRI{"http://example.com/tool/search"},
		Version:      "2.0",
	}

	doc := am.ToLDDocument()
	if doc == nil {
		t.Fatal("expected non-nil document")
	}

	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	_, err = Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
}

func TestRoundTrip_AllValueTypes(t *testing.T) {
	doc := NewDocument()
	doc.Root = cowrie.Object(
		cowrie.Member{Key: "str", Value: cowrie.String("hello")},
		cowrie.Member{Key: "int", Value: cowrie.Int64(-42)},
		cowrie.Member{Key: "uint", Value: cowrie.Uint64(42)},
		cowrie.Member{Key: "float", Value: cowrie.Float64(3.14)},
		cowrie.Member{Key: "bool", Value: cowrie.Bool(true)},
		cowrie.Member{Key: "null", Value: cowrie.Null()},
		cowrie.Member{Key: "bytes", Value: cowrie.Bytes([]byte{0xDE, 0xAD})},
		cowrie.Member{Key: "arr", Value: cowrie.Array(cowrie.Int64(1), cowrie.String("two"))},
		cowrie.Member{Key: "nested", Value: cowrie.Object(
			cowrie.Member{Key: "inner", Value: cowrie.String("value")},
		)},
	)

	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	root := decoded.Root
	if root.Get("str").String() != "hello" {
		t.Error("str mismatch")
	}
	if root.Get("int").Int64() != -42 {
		t.Error("int mismatch")
	}
	if root.Get("uint").Uint64() != 42 {
		t.Error("uint mismatch")
	}
	if root.Get("bool").Bool() != true {
		t.Error("bool mismatch")
	}
	if !root.Get("null").IsNull() {
		t.Error("null mismatch")
	}
}

func TestLookupTermByIRI(t *testing.T) {
	doc := NewContextBuilder().
		AddTerm("name", "http://schema.org/name").
		Build()

	term := doc.LookupTermByIRI("http://schema.org/name")
	if term == nil {
		t.Fatal("expected to find term by IRI")
	}
	if term.Term != "name" {
		t.Errorf("expected term 'name', got %q", term.Term)
	}

	// Not found
	term = doc.LookupTermByIRI("http://schema.org/nonexistent")
	if term != nil {
		t.Error("expected nil for unknown IRI")
	}
}
