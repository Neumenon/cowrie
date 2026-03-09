package ld

import (
	"testing"

	"github.com/Neumenon/cowrie/go"
)

// ============================================================
// encode.go: EncodeIRI, EncodeBNode, EncodeLDValue, buffer.bytes
// ============================================================

func TestEncoderLDDocumentRoundtrip(t *testing.T) {
	doc := NewDocument()
	doc.IRIs = append(doc.IRIs, "http://example.org/alice", "http://example.org/bob")
	doc.Root = cowrie.Object(
		cowrie.Member{Key: "name", Value: cowrie.String("Alice")},
	)

	data, err := Encode(doc)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty data")
	}

	// Decode back
	doc2, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if doc2 == nil {
		t.Fatal("expected non-nil doc")
	}
}

func TestEncoderWithTerms(t *testing.T) {
	doc := NewDocument()
	doc.AddTerm("name", "http://schema.org/name", 0)
	doc.AddTerm("age", "http://schema.org/age", 0)
	doc.IRIs = append(doc.IRIs, "http://example.org/person")
	doc.Root = cowrie.Object(
		cowrie.Member{Key: "name", Value: cowrie.String("Alice")},
		cowrie.Member{Key: "age", Value: cowrie.Int64(30)},
	)

	data, err := Encode(doc)
	if err != nil {
		t.Fatal(err)
	}

	doc2, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if doc2.Root == nil {
		t.Fatal("expected non-nil root")
	}
}

func TestEncoderNested(t *testing.T) {
	doc := NewDocument()
	doc.Root = cowrie.Object(
		cowrie.Member{Key: "nested", Value: cowrie.Object(
			cowrie.Member{Key: "x", Value: cowrie.Int64(1)},
		)},
		cowrie.Member{Key: "arr", Value: cowrie.Array(
			cowrie.String("a"), cowrie.String("b"),
		)},
	)

	data, err := Encode(doc)
	if err != nil {
		t.Fatal(err)
	}

	doc2, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if doc2.Root.Type() != cowrie.TypeObject {
		t.Fatalf("expected object, got %s", doc2.Root.Type())
	}
}

// ============================================================
// encode.go: buffer.bytes()
// ============================================================

func TestBufferBytes(t *testing.T) {
	b := &buffer{}
	b.writeByte(0x01)
	b.write([]byte{0x02, 0x03})
	b.writeString("hi")
	b.writeUvarint(42)
	data := b.bytes()
	if len(data) == 0 {
		t.Fatal("expected non-empty data")
	}
}
