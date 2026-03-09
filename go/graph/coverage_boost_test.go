package graph

import (
	"testing"

	"github.com/Neumenon/cowrie/go/ld"
)

// ============================================================
// types.go: NewLiteralTerm
// ============================================================

func TestNewLiteralTerm(t *testing.T) {
	term := NewLiteralTerm("hello")
	if term.Kind != TermLiteral {
		t.Fatalf("expected literal term, got %d", term.Kind)
	}
	if term.Value != "hello" {
		t.Fatalf("expected 'hello', got %q", term.Value)
	}
}

func TestNewTypedLiteral(t *testing.T) {
	term := NewTypedLiteral("42", ld.IRI("http://www.w3.org/2001/XMLSchema#integer"))
	if term.Kind != TermLiteral {
		t.Fatalf("expected literal term")
	}
	if term.Value != "42" {
		t.Fatalf("expected '42'")
	}
	if term.Datatype != ld.IRI("http://www.w3.org/2001/XMLSchema#integer") {
		t.Fatalf("unexpected datatype")
	}
}

func TestNewLangLiteral(t *testing.T) {
	term := NewLangLiteral("hello", "en")
	if term.Lang != "en" {
		t.Fatalf("expected lang 'en', got %q", term.Lang)
	}
}

func TestNewBNodeTerm(t *testing.T) {
	term := NewBNodeTerm("b0")
	if term.Kind != TermBNode {
		t.Fatalf("expected bnode term")
	}
	if term.Value != "b0" {
		t.Fatalf("expected 'b0', got %q", term.Value)
	}
}
