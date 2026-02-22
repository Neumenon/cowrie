// Package glyph implements GLYPH, a token-optimized LLM-friendly codec.
//
// GLYPH is designed to be:
//   - LLM-easy to emit and repair
//   - Token-cheap (short keys, no quotes, structural compression)
//   - Strongly typed + constrained (schema-driven validation)
//   - Streamable / partial (frame-based)
//   - Deterministic + canonical (stable hashing)
//   - Round-trippable to JSON
//   - Diff-friendly + patchable
//
// # Dual Encoding
//
// GLYPH has two equivalent encodings:
//   - GLYPH-T (text): What the LLM reads/writes (token-optimized)
//   - GLYPH-B (binary): What systems store/transport (via Cowrie)
//
// Both share the same abstract data model and schema language.
//
// # Data Model
//
// Scalars: null, bool, int, float, str, bytes, time, id
// Containers: list, map, struct (fixed fields)
// Special: ref, sum (tagged union)
//
// # GLYPH-T Syntax
//
// Struct:     Type{a=1 b=2}
// Map:        {k:v k2:v2}
// List:       [v1 v2 v3]
// Sum/union:  Tag(v) or Tag{...}
// Ref:        ^id or ^#hash
// Null:       ∅
// Bool:       t / f
// String:     bare_word or "quoted string"
//
// # Schema Language
//
//	@schema{
//	  Team:v1 struct{
//	    id: id        @k(t)
//	    name: str     @k(n)
//	    league: str   @k(l) [optional]
//	  }
//	}
//
// # Example
//
//	Match{
//	  m=^m:2025-12-19:ARS-LIV
//	  k=2025-12-19T20:00Z
//	  H=Team{t=^t:ARS n="Arsenal"}
//	  xH=1.72
//	  O=[2.10 3.40 3.25]
//	}
//
// # Error Tolerance
//
// GLYPH-T parsing is tolerant:
//   - Accepts both = and : for field assignment
//   - Accepts optional commas between elements
//   - Auto-corrects common LLM mistakes
//   - Uses schema for field name fuzzy matching
package glyph
