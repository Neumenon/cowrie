package ld

import (
	"testing"

	"github.com/Neumenon/cowrie"
)

func TestRoundTripBasic(t *testing.T) {
	doc := NewDocument()
	doc.Root = cowrie.Object(
		cowrie.Member{Key: "name", Value: cowrie.String("Alice")},
		cowrie.Member{Key: "age", Value: cowrie.Int64(30)},
		cowrie.Member{Key: "active", Value: cowrie.Bool(true)},
	)

	// Encode
	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Verify magic bytes
	if len(data) < 6 {
		t.Fatalf("Data too short: %d bytes", len(data))
	}
	if data[0] != 'S' || data[1] != 'J' || data[2] != 'L' || data[3] != 'D' {
		t.Errorf("Invalid magic: %c%c%c%c", data[0], data[1], data[2], data[3])
	}
	if data[4] != Version {
		t.Errorf("Invalid version: %d", data[4])
	}

	// Decode
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify values
	if decoded.Root.Type() != cowrie.TypeObject {
		t.Errorf("Expected object, got %v", decoded.Root.Type())
	}

	name := decoded.Root.Get("name")
	if name.String() != "Alice" {
		t.Errorf("Expected name=Alice, got %s", name.String())
	}

	age := decoded.Root.Get("age")
	if age.Int64() != 30 {
		t.Errorf("Expected age=30, got %d", age.Int64())
	}

	active := decoded.Root.Get("active")
	if !active.Bool() {
		t.Error("Expected active=true")
	}
}

func TestRoundTripWithTerms(t *testing.T) {
	doc := NewDocument()

	// Add JSON-LD terms
	doc.AddTerm("name", "http://schema.org/name", TermFlagNone)
	doc.AddTerm("email", "http://schema.org/email", TermFlagNone)
	doc.AddTerm("knows", "http://schema.org/knows", TermFlagSet)

	doc.Root = cowrie.Object(
		cowrie.Member{Key: "name", Value: cowrie.String("Bob")},
		cowrie.Member{Key: "email", Value: cowrie.String("bob@example.com")},
	)

	// Encode
	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify terms
	if len(decoded.Terms) != 3 {
		t.Errorf("Expected 3 terms, got %d", len(decoded.Terms))
	}

	nameTerm := decoded.LookupTerm("name")
	if nameTerm == nil {
		t.Fatal("Term 'name' not found")
	}
	if nameTerm.IRI != "http://schema.org/name" {
		t.Errorf("Expected IRI http://schema.org/name, got %s", nameTerm.IRI)
	}

	knowsTerm := decoded.LookupTerm("knows")
	if knowsTerm == nil {
		t.Fatal("Term 'knows' not found")
	}
	if knowsTerm.Flags != TermFlagSet {
		t.Errorf("Expected flags=%d, got %d", TermFlagSet, knowsTerm.Flags)
	}
}

func TestRoundTripWithIRIs(t *testing.T) {
	doc := NewDocument()

	// Add IRIs
	doc.AddIRI("http://example.org/person/1")
	doc.AddIRI("http://example.org/person/2")

	doc.Root = cowrie.Object(
		cowrie.Member{Key: "id", Value: cowrie.String("http://example.org/person/1")},
		cowrie.Member{Key: "name", Value: cowrie.String("Charlie")},
	)

	// Encode
	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify IRIs
	if len(decoded.IRIs) != 2 {
		t.Errorf("Expected 2 IRIs, got %d", len(decoded.IRIs))
	}
	if decoded.GetIRI(0) != "http://example.org/person/1" {
		t.Errorf("Expected IRI[0]=http://example.org/person/1, got %s", decoded.GetIRI(0))
	}
}

func TestRoundTripWithDatatypes(t *testing.T) {
	doc := NewDocument()

	// Add datatypes
	doc.AddDatatype(XSDString)
	doc.AddDatatype(XSDDateTime)
	doc.AddDatatype(XSDInteger)

	doc.Root = cowrie.Object(
		cowrie.Member{Key: "value", Value: cowrie.Int64(42)},
	)

	// Encode
	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify datatypes
	if len(decoded.Datatypes) != 3 {
		t.Errorf("Expected 3 datatypes, got %d", len(decoded.Datatypes))
	}
	if decoded.GetDatatype(0) != XSDString {
		t.Errorf("Expected datatype[0]=xsd:string, got %s", decoded.GetDatatype(0))
	}
	if decoded.GetDatatype(1) != XSDDateTime {
		t.Errorf("Expected datatype[1]=xsd:dateTime, got %s", decoded.GetDatatype(1))
	}
}

func TestRoundTripNestedObject(t *testing.T) {
	doc := NewDocument()
	doc.Root = cowrie.Object(
		cowrie.Member{Key: "person", Value: cowrie.Object(
			cowrie.Member{Key: "name", Value: cowrie.String("Dave")},
			cowrie.Member{Key: "address", Value: cowrie.Object(
				cowrie.Member{Key: "city", Value: cowrie.String("NYC")},
				cowrie.Member{Key: "zip", Value: cowrie.String("10001")},
			)},
		)},
	)

	// Encode
	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify nested values
	person := decoded.Root.Get("person")
	if person.Type() != cowrie.TypeObject {
		t.Fatal("Expected person to be object")
	}

	name := person.Get("name")
	if name.String() != "Dave" {
		t.Errorf("Expected name=Dave, got %s", name.String())
	}

	address := person.Get("address")
	city := address.Get("city")
	if city.String() != "NYC" {
		t.Errorf("Expected city=NYC, got %s", city.String())
	}
}

func TestRoundTripArray(t *testing.T) {
	doc := NewDocument()
	doc.Root = cowrie.Object(
		cowrie.Member{Key: "items", Value: cowrie.Array(
			cowrie.String("one"),
			cowrie.String("two"),
			cowrie.String("three"),
		)},
		cowrie.Member{Key: "numbers", Value: cowrie.Array(
			cowrie.Int64(1),
			cowrie.Int64(2),
			cowrie.Int64(3),
		)},
	)

	// Encode
	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify arrays
	items := decoded.Root.Get("items")
	if items.Len() != 3 {
		t.Errorf("Expected 3 items, got %d", items.Len())
	}
	if items.Index(1).String() != "two" {
		t.Errorf("Expected items[1]=two, got %s", items.Index(1).String())
	}

	numbers := decoded.Root.Get("numbers")
	if numbers.Index(2).Int64() != 3 {
		t.Errorf("Expected numbers[2]=3, got %d", numbers.Index(2).Int64())
	}
}

func TestRoundTripAllTypes(t *testing.T) {
	doc := NewDocument()
	doc.Root = cowrie.Object(
		cowrie.Member{Key: "null", Value: cowrie.Null()},
		cowrie.Member{Key: "true", Value: cowrie.Bool(true)},
		cowrie.Member{Key: "false", Value: cowrie.Bool(false)},
		cowrie.Member{Key: "int", Value: cowrie.Int64(-42)},
		cowrie.Member{Key: "uint", Value: cowrie.Uint64(18446744073709551615)},
		cowrie.Member{Key: "float", Value: cowrie.Float64(3.14159)},
		cowrie.Member{Key: "string", Value: cowrie.String("hello world")},
		cowrie.Member{Key: "bytes", Value: cowrie.Bytes([]byte{0xDE, 0xAD, 0xBE, 0xEF})},
		cowrie.Member{Key: "datetime", Value: cowrie.Datetime64(1732777200000000000)},
		cowrie.Member{Key: "uuid", Value: cowrie.UUID128([16]byte{
			0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
			0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
		})},
	)

	// Encode
	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify all types
	if decoded.Root.Get("null").Type() != cowrie.TypeNull {
		t.Error("Expected null type")
	}
	if decoded.Root.Get("true").Bool() != true {
		t.Error("Expected true")
	}
	if decoded.Root.Get("false").Bool() != false {
		t.Error("Expected false")
	}
	if decoded.Root.Get("int").Int64() != -42 {
		t.Errorf("Expected -42, got %d", decoded.Root.Get("int").Int64())
	}
	if decoded.Root.Get("uint").Uint64() != 18446744073709551615 {
		t.Errorf("Expected max uint64, got %d", decoded.Root.Get("uint").Uint64())
	}
	if decoded.Root.Get("float").Float64() != 3.14159 {
		t.Errorf("Expected 3.14159, got %f", decoded.Root.Get("float").Float64())
	}
	if decoded.Root.Get("string").String() != "hello world" {
		t.Errorf("Expected 'hello world', got %s", decoded.Root.Get("string").String())
	}
}

func TestDocumentHelpers(t *testing.T) {
	doc := NewDocument()

	// Test AddIRI deduplication
	idx1 := doc.AddIRI("http://example.org/foo")
	idx2 := doc.AddIRI("http://example.org/bar")
	idx3 := doc.AddIRI("http://example.org/foo") // duplicate

	if idx1 != 0 {
		t.Errorf("Expected idx1=0, got %d", idx1)
	}
	if idx2 != 1 {
		t.Errorf("Expected idx2=1, got %d", idx2)
	}
	if idx3 != 0 {
		t.Errorf("Expected idx3=0 (duplicate), got %d", idx3)
	}
	if len(doc.IRIs) != 2 {
		t.Errorf("Expected 2 IRIs, got %d", len(doc.IRIs))
	}

	// Test AddDatatype deduplication
	dt1 := doc.AddDatatype(XSDString)
	dt2 := doc.AddDatatype(XSDInteger)
	dt3 := doc.AddDatatype(XSDString) // duplicate

	if dt1 != 0 {
		t.Errorf("Expected dt1=0, got %d", dt1)
	}
	if dt2 != 1 {
		t.Errorf("Expected dt2=1, got %d", dt2)
	}
	if dt3 != 0 {
		t.Errorf("Expected dt3=0 (duplicate), got %d", dt3)
	}

	// Test LookupTermByIRI
	doc.AddTerm("name", "http://schema.org/name", TermFlagNone)
	term := doc.LookupTermByIRI("http://schema.org/name")
	if term == nil {
		t.Fatal("Expected to find term by IRI")
	}
	if term.Term != "name" {
		t.Errorf("Expected term='name', got %s", term.Term)
	}

	// Test out of bounds
	if doc.GetIRI(100) != "" {
		t.Error("Expected empty string for out of bounds IRI")
	}
	if doc.GetDatatype(100) != "" {
		t.Error("Expected empty string for out of bounds datatype")
	}
}

func TestLDValueHelpers(t *testing.T) {
	doc := NewDocument()

	// Test NewIRIValue
	iriVal := NewIRIValue("http://example.org/resource", doc)
	if !iriVal.IsIRI {
		t.Error("Expected IsIRI=true")
	}
	if iriVal.IRIID != 0 {
		t.Errorf("Expected IRIID=0, got %d", iriVal.IRIID)
	}
	if len(doc.IRIs) != 1 {
		t.Errorf("Expected 1 IRI in doc, got %d", len(doc.IRIs))
	}

	// Test NewBNodeValue
	bnodeVal := NewBNodeValue("b1")
	if !bnodeVal.IsBNode {
		t.Error("Expected IsBNode=true")
	}
	if bnodeVal.BNodeID != "b1" {
		t.Errorf("Expected BNodeID='b1', got %s", bnodeVal.BNodeID)
	}

	// Test NewLiteralValue with datatype
	litVal := NewLiteralValue(cowrie.String("hello"), XSDString, doc)
	if litVal.IsIRI || litVal.IsBNode {
		t.Error("Expected regular value (not IRI or BNode)")
	}
	if len(doc.Datatypes) != 1 {
		t.Errorf("Expected 1 datatype in doc, got %d", len(doc.Datatypes))
	}
}

func TestEmptyDocument(t *testing.T) {
	doc := NewDocument()
	doc.Root = cowrie.Null()

	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Root.Type() != cowrie.TypeNull {
		t.Errorf("Expected null root, got %v", decoded.Root.Type())
	}
	if len(decoded.Terms) != 0 {
		t.Errorf("Expected 0 terms, got %d", len(decoded.Terms))
	}
	if len(decoded.IRIs) != 0 {
		t.Errorf("Expected 0 IRIs, got %d", len(decoded.IRIs))
	}
}

func BenchmarkEncode(b *testing.B) {
	doc := NewDocument()
	doc.AddTerm("name", "http://schema.org/name", TermFlagNone)
	doc.AddTerm("age", "http://schema.org/age", TermFlagNone)
	doc.AddIRI("http://example.org/person/1")

	doc.Root = cowrie.Object(
		cowrie.Member{Key: "name", Value: cowrie.String("Alice")},
		cowrie.Member{Key: "age", Value: cowrie.Int64(30)},
		cowrie.Member{Key: "active", Value: cowrie.Bool(true)},
		cowrie.Member{Key: "scores", Value: cowrie.Array(
			cowrie.Int64(95),
			cowrie.Int64(87),
			cowrie.Int64(92),
		)},
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Encode(doc)
	}
}

func BenchmarkDecode(b *testing.B) {
	doc := NewDocument()
	doc.AddTerm("name", "http://schema.org/name", TermFlagNone)
	doc.AddTerm("age", "http://schema.org/age", TermFlagNone)

	doc.Root = cowrie.Object(
		cowrie.Member{Key: "name", Value: cowrie.String("Alice")},
		cowrie.Member{Key: "age", Value: cowrie.Int64(30)},
		cowrie.Member{Key: "active", Value: cowrie.Bool(true)},
	)

	data, _ := Encode(doc)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Decode(data)
	}
}
