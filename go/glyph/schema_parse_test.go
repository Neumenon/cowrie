package glyph

import "testing"

func TestParseSchemaParameterizedTypes(t *testing.T) {
	s, err := ParseSchema(`@schema{
		Team:v1 struct{ name: str }
		Config:v1 struct{
			tags: list<str>
			settings: map<str,int>
			nested: list<map<str,int>>
			inline: list<struct{a:int b:str}>
		}
	}`)
	if err != nil {
		t.Fatalf("ParseSchema error: %v", err)
	}

	cfg := s.GetType("Config")
	if cfg == nil {
		t.Fatalf("missing type Config")
	}

	tags := s.GetField("Config", "tags")
	if tags == nil {
		t.Fatalf("missing field Config.tags")
	}
	if tags.Type.Kind != TypeSpecList || tags.Type.Elem == nil || tags.Type.Elem.Kind != TypeSpecStr {
		t.Fatalf("tags type mismatch: got %v", tags.Type.String())
	}

	settings := s.GetField("Config", "settings")
	if settings == nil {
		t.Fatalf("missing field Config.settings")
	}
	if settings.Type.Kind != TypeSpecMap || settings.Type.KeyType == nil || settings.Type.ValType == nil {
		t.Fatalf("settings type mismatch: got %v", settings.Type.String())
	}
	if settings.Type.KeyType.Kind != TypeSpecStr || settings.Type.ValType.Kind != TypeSpecInt {
		t.Fatalf("settings key/val mismatch: got %v", settings.Type.String())
	}

	nested := s.GetField("Config", "nested")
	if nested == nil {
		t.Fatalf("missing field Config.nested")
	}
	if nested.Type.Kind != TypeSpecList || nested.Type.Elem == nil || nested.Type.Elem.Kind != TypeSpecMap {
		t.Fatalf("nested type mismatch: got %v", nested.Type.String())
	}
	if nested.Type.Elem.KeyType == nil || nested.Type.Elem.ValType == nil {
		t.Fatalf("nested map key/val missing: got %v", nested.Type.String())
	}
	if nested.Type.Elem.KeyType.Kind != TypeSpecStr || nested.Type.Elem.ValType.Kind != TypeSpecInt {
		t.Fatalf("nested map key/val mismatch: got %v", nested.Type.String())
	}

	inline := s.GetField("Config", "inline")
	if inline == nil {
		t.Fatalf("missing field Config.inline")
	}
	if inline.Type.Kind != TypeSpecList || inline.Type.Elem == nil || inline.Type.Elem.Kind != TypeSpecInlineStruct {
		t.Fatalf("inline type mismatch: got %v", inline.Type.String())
	}
	if inline.Type.Elem.Struct == nil || len(inline.Type.Elem.Struct.Fields) != 2 {
		t.Fatalf("inline struct fields mismatch")
	}
}

func TestParseSchemaParameterizedTypesErrors(t *testing.T) {
	cases := []string{
		`@schema{ A:v1 struct{ x: list } }`,
		`@schema{ A:v1 struct{ x: map } }`,
		`@schema{ A:v1 struct{ x: list< } }`,
		`@schema{ A:v1 struct{ x: map<str> } }`,
		`@schema{ A:v1 struct{ x: map<str,> } }`,
	}

	for _, input := range cases {
		if _, err := ParseSchema(input); err == nil {
			t.Fatalf("expected error for input: %s", input)
		}
	}
}
