package ld

import (
	"bytes"
	cowrie "github.com/Neumenon/cowrie/go"
	"testing"
)

func TestEncodeDecodeAllTypes(t *testing.T) {
	tests := []struct {
		name string
		v    *cowrie.Value
	}{
		{"null", cowrie.Null()},
		{"bool_true", cowrie.Bool(true)},
		{"bool_false", cowrie.Bool(false)},
		{"int64_pos", cowrie.Int64(42)},
		{"int64_neg", cowrie.Int64(-7)},
		{"int64_zero", cowrie.Int64(0)},
		{"uint64", cowrie.Uint64(100)},
		{"float64", cowrie.Float64(3.14)},
		{"string_simple", cowrie.String("hello")},
		{"string_empty", cowrie.String("")},
		{"bytes", cowrie.Bytes([]byte{1, 2, 3})},
		{"array", cowrie.Array(cowrie.Int64(1), cowrie.String("two"))},
		{"object", cowrie.Object(
			cowrie.Member{Key: "x", Value: cowrie.Int64(42)},
			cowrie.Member{Key: "y", Value: cowrie.String("hello")},
		)},
		{"nested_object", cowrie.Object(
			cowrie.Member{Key: "outer", Value: cowrie.Object(
				cowrie.Member{Key: "inner", Value: cowrie.Int64(1)},
			)},
		)},
		{"nested_array", cowrie.Array(
			cowrie.Array(cowrie.Int64(1), cowrie.Int64(2)),
			cowrie.Array(cowrie.Int64(3), cowrie.Int64(4)),
		)},
		{"uuid", cowrie.UUID128([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})},
		{"datetime", cowrie.Datetime64(1234567890)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := NewDocument()
			doc.Root = tt.v

			data, err := Encode(doc)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(data) == 0 {
				t.Error("empty output")
			}

			decoded, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if decoded == nil {
				t.Error("nil decoded")
			}
		})
	}
}

func TestDocumentTerms_Coverage(t *testing.T) {
	doc := NewDocument()
	doc.AddTerm("name", "http://schema.org/name", 0)
	doc.AddTerm("age", "http://schema.org/age", 0)

	doc.Root = cowrie.Object(
		cowrie.Member{Key: "name", Value: cowrie.String("Alice")},
		cowrie.Member{Key: "age", Value: cowrie.Int64(30)},
	)

	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.Root == nil {
		t.Error("nil root")
	}
}

func TestLookupTerm_Coverage(t *testing.T) {
	doc := NewDocument()
	doc.AddTerm("name", "http://schema.org/name", 0)

	entry := doc.LookupTerm("name")
	if entry == nil {
		t.Error("should find name")
	}
	if entry.IRI != "http://schema.org/name" {
		t.Error("wrong IRI")
	}

	entry2 := doc.LookupTerm("missing")
	if entry2 != nil {
		t.Error("should not find missing")
	}

	// LookupTermByIRI
	entry3 := doc.LookupTermByIRI("http://schema.org/name")
	if entry3 == nil {
		t.Error("should find by IRI")
	}

	entry4 := doc.LookupTermByIRI("http://schema.org/missing")
	if entry4 != nil {
		t.Error("should not find missing IRI")
	}
}

func TestAddIRI_Coverage(t *testing.T) {
	doc := NewDocument()
	idx1 := doc.AddIRI("http://example.org/1")
	idx2 := doc.AddIRI("http://example.org/2")
	idx3 := doc.AddIRI("http://example.org/1") // duplicate

	if idx1 == idx2 {
		t.Error("different IRIs should have different indices")
	}
	if idx1 != idx3 {
		t.Error("same IRI should return same index")
	}

	iri := doc.GetIRI(idx1)
	if iri != "http://example.org/1" {
		t.Error("wrong IRI")
	}

	iri2 := doc.GetIRI(-1)
	if iri2 != "" {
		t.Error("out of bounds should return empty")
	}
}

func TestAddDatatype_Coverage(t *testing.T) {
	doc := NewDocument()
	idx1 := doc.AddDatatype(XSDString)
	idx2 := doc.AddDatatype(XSDInteger)
	idx3 := doc.AddDatatype(XSDString) // duplicate

	if idx1 == idx2 {
		t.Error("different datatypes should have different indices")
	}
	if idx1 != idx3 {
		t.Error("same datatype should return same index")
	}

	dt := doc.GetDatatype(idx1)
	if dt != XSDString {
		t.Error("wrong datatype")
	}

	dt2 := doc.GetDatatype(-1)
	if dt2 != "" {
		t.Error("out of bounds should return empty")
	}
}

func TestDecodeFrom_Coverage(t *testing.T) {
	doc := NewDocument()
	doc.Root = cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(42)},
	)

	data, err := Encode(doc)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeFrom(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if decoded == nil || decoded.Root == nil {
		t.Error("nil")
	}
}

func TestEncodeTo_Coverage(t *testing.T) {
	doc := NewDocument()
	doc.Root = cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Int64(42)},
	)

	var buf bytes.Buffer
	err := EncodeTo(&buf, doc)
	if err != nil {
		t.Fatal(err)
	}
	if buf.Len() == 0 {
		t.Error("empty")
	}
}
