package cowrie

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"testing"
)

// truthManifest mirrors the structure of truth_cases.json.
type truthManifest struct {
	Cowrie struct {
		Cases []truthCase `json:"cases"`
	} `json:"cowrie"`
}

type truthCase struct {
	ID          string          `json:"id"`
	Description string          `json:"description"`
	Action      string          `json:"action"`
	Input       json.RawMessage `json:"input"`
	Expect      truthExpect     `json:"expect"`
}

type truthExpect struct {
	OK            bool            `json:"ok"`
	Value         json.RawMessage `json:"value"`
	IsNaN         bool            `json:"is_nan"`
	IsPositiveInf bool            `json:"is_positive_inf"`
	IsNegativeInf bool            `json:"is_negative_inf"`
	NegativeZero  bool            `json:"negative_zero"`
	ValueBase64   *string         `json:"value_base64"`
}

type truthInput struct {
	Type        string          `json:"type"`
	Value       json.RawMessage `json:"value"`
	ValueBase64 *string         `json:"value_base64"`
	Entries     json.RawMessage `json:"entries"`
	Depth       int             `json:"depth"`
}

func loadTruthCases(t *testing.T) []truthCase {
	t.Helper()
	data, err := os.ReadFile("../../testdata/robustness/truth_cases.json")
	if err != nil {
		t.Fatalf("failed to read truth_cases.json: %v", err)
	}
	var m truthManifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("failed to parse truth_cases.json: %v", err)
	}
	return m.Cowrie.Cases
}

// buildValue constructs a *Value from a truth case input spec.
func buildValue(t *testing.T, raw json.RawMessage) *Value {
	t.Helper()
	var inp truthInput
	if err := json.Unmarshal(raw, &inp); err != nil {
		t.Fatalf("failed to parse input: %v", err)
	}

	switch inp.Type {
	case "null":
		return Null()

	case "bool":
		var b bool
		if err := json.Unmarshal(inp.Value, &b); err != nil {
			t.Fatalf("bad bool value: %v", err)
		}
		return Bool(b)

	case "int64":
		var n int64
		if err := json.Unmarshal(inp.Value, &n); err != nil {
			t.Fatalf("bad int64 value: %v", err)
		}
		return Int64(n)

	case "float64":
		// Value can be a number or a special string like "NaN", "+Inf", "-Inf", "-0.0"
		var s string
		if err := json.Unmarshal(inp.Value, &s); err == nil {
			switch s {
			case "NaN":
				return Float64(math.NaN())
			case "+Inf":
				return Float64(math.Inf(1))
			case "-Inf":
				return Float64(math.Inf(-1))
			case "-0.0":
				return Float64(math.Copysign(0, -1))
			default:
				t.Fatalf("unknown float64 string: %s", s)
			}
		}
		var f float64
		if err := json.Unmarshal(inp.Value, &f); err != nil {
			t.Fatalf("bad float64 value: %v", err)
		}
		return Float64(f)

	case "string":
		var s string
		if err := json.Unmarshal(inp.Value, &s); err != nil {
			t.Fatalf("bad string value: %v", err)
		}
		return String(s)

	case "bytes":
		b64 := ""
		if inp.ValueBase64 != nil {
			b64 = *inp.ValueBase64
		}
		if b64 == "" {
			return Bytes([]byte{})
		}
		raw, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			t.Fatalf("bad base64: %v", err)
		}
		return Bytes(raw)

	case "array":
		var items []json.RawMessage
		if err := json.Unmarshal(inp.Value, &items); err != nil {
			t.Fatalf("bad array value: %v", err)
		}
		vals := make([]*Value, len(items))
		for i, item := range items {
			vals[i] = jsonToValue(t, item)
		}
		return Array(vals...)

	case "object":
		return buildObject(t, inp.Entries)

	case "nested_arrays":
		return buildNestedArrays(inp.Depth)

	case "raw_hex":
		// Not a Value — handled separately
		t.Fatalf("raw_hex should not be passed to buildValue")
		return nil

	default:
		t.Fatalf("unknown input type: %s", inp.Type)
		return nil
	}
}

// buildObject constructs an object from the "entries" field: [["key", value], ...]
// Preserves duplicate keys.
func buildObject(t *testing.T, entriesRaw json.RawMessage) *Value {
	t.Helper()
	var entries []json.RawMessage
	if err := json.Unmarshal(entriesRaw, &entries); err != nil {
		t.Fatalf("bad entries: %v", err)
	}
	members := make([]Member, len(entries))
	for i, entry := range entries {
		var pair []json.RawMessage
		if err := json.Unmarshal(entry, &pair); err != nil || len(pair) != 2 {
			t.Fatalf("bad entry %d: %v", i, err)
		}
		var key string
		if err := json.Unmarshal(pair[0], &key); err != nil {
			t.Fatalf("bad entry key %d: %v", i, err)
		}
		members[i] = Member{Key: key, Value: jsonToValue(t, pair[1])}
	}
	return Object(members...)
}

// jsonToValue converts a raw JSON value to a cowrie *Value.
func jsonToValue(t *testing.T, raw json.RawMessage) *Value {
	t.Helper()

	// Try null
	if string(raw) == "null" {
		return Null()
	}

	// Try bool
	var b bool
	if err := json.Unmarshal(raw, &b); err == nil {
		return Bool(b)
	}

	// Try number — use json.Number for precision
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		// Try int64 first
		if i, err := n.Int64(); err == nil {
			return Int64(i)
		}
		if f, err := n.Float64(); err == nil {
			return Float64(f)
		}
	}

	// Try string
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return String(s)
	}

	// Try array
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err == nil {
		vals := make([]*Value, len(arr))
		for i, item := range arr {
			vals[i] = jsonToValue(t, item)
		}
		return Array(vals...)
	}

	// Try object
	dec := json.NewDecoder(nil) // placeholder
	_ = dec
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err == nil {
		members := make([]Member, 0, len(obj))
		for k, v := range obj {
			members = append(members, Member{Key: k, Value: jsonToValue(t, v)})
		}
		return Object(members...)
	}

	t.Fatalf("cannot convert JSON to Value: %s", string(raw))
	return nil
}

// buildNestedArrays creates depth levels of nested single-element arrays wrapping null.
func buildNestedArrays(depth int) *Value {
	v := Null()
	for i := 0; i < depth; i++ {
		v = Array(v)
	}
	return v
}

// verifyNestedArrayDepth checks the nesting depth of arrays wrapping a final null.
func verifyNestedArrayDepth(t *testing.T, v *Value, expectedDepth int) {
	t.Helper()
	cur := v
	for d := 0; d < expectedDepth; d++ {
		if cur.Type() != TypeArray {
			t.Fatalf("expected array at depth %d, got %v", d, cur.Type())
		}
		if cur.Len() != 1 {
			t.Fatalf("expected single-element array at depth %d, got len %d", d, cur.Len())
		}
		cur = cur.Index(0)
	}
	if cur.Type() != TypeNull {
		t.Fatalf("expected null at leaf, got %v", cur.Type())
	}
}

// valuesEqualJSON checks that a decoded Value matches the expected JSON.
func valuesEqualJSON(t *testing.T, got *Value, expectRaw json.RawMessage) {
	t.Helper()
	compareValueToRawJSON(t, got, expectRaw)
}

// compareValueToRawJSON recursively compares a Value against raw JSON,
// using json.Number to avoid precision loss on large integers.
func compareValueToRawJSON(t *testing.T, got *Value, raw json.RawMessage) {
	t.Helper()

	trimmed := string(raw)

	switch got.Type() {
	case TypeNull:
		if trimmed != "null" {
			t.Fatalf("expected null in JSON, got %s", trimmed)
		}
	case TypeBool:
		var b bool
		if err := json.Unmarshal(raw, &b); err != nil {
			t.Fatalf("expected bool in JSON: %v", err)
		}
		if got.Bool() != b {
			t.Fatalf("bool mismatch: got %v, expect %v", got.Bool(), b)
		}
	case TypeInt64:
		var n json.Number
		if err := json.Unmarshal(raw, &n); err != nil {
			t.Fatalf("expected number in JSON: %v", err)
		}
		expectI, err := n.Int64()
		if err != nil {
			t.Fatalf("expected int64-compatible number: %v", err)
		}
		if got.Int64() != expectI {
			t.Fatalf("int64 mismatch: got %d, expect %d", got.Int64(), expectI)
		}
	case TypeFloat64:
		var n json.Number
		if err := json.Unmarshal(raw, &n); err != nil {
			t.Fatalf("expected number in JSON: %v", err)
		}
		expectF, err := n.Float64()
		if err != nil {
			t.Fatalf("expected float64-compatible number: %v", err)
		}
		if got.Float64() != expectF {
			t.Fatalf("float64 mismatch: got %v, expect %v", got.Float64(), expectF)
		}
	case TypeString:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			t.Fatalf("expected string in JSON: %v", err)
		}
		if got.String() != s {
			t.Fatalf("string mismatch: got %q, expect %q", got.String(), s)
		}
	case TypeArray:
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			t.Fatalf("expected array in JSON: %v", err)
		}
		if got.Len() != len(arr) {
			t.Fatalf("array length mismatch: got %d, expect %d", got.Len(), len(arr))
		}
		for i := 0; i < got.Len(); i++ {
			compareValueToRawJSON(t, got.Index(i), arr[i])
		}
	case TypeObject:
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			t.Fatalf("expected object in JSON: %v", err)
		}
		// Build last-writer-wins map from decoded members
		// (cowrie may preserve duplicate keys internally).
		lastVal := make(map[string]*Value)
		for _, m := range got.Members() {
			lastVal[m.Key] = m.Value
		}
		for key, expectVal := range obj {
			gotVal, ok := lastVal[key]
			if !ok {
				t.Fatalf("missing key %q in decoded object", key)
			}
			compareValueToRawJSON(t, gotVal, expectVal)
		}
		// Verify no unexpected unique keys
		for key := range lastVal {
			if _, ok := obj[key]; !ok {
				t.Fatalf("unexpected key in decoded object: %q", key)
			}
		}
	default:
		// Fall back to JSON marshaling comparison
		gotJSON := valueToJSON(t, got)
		gotBytes, _ := json.Marshal(gotJSON)
		if string(gotBytes) != trimmed {
			t.Fatalf("value mismatch: got %s, expect %s", gotBytes, trimmed)
		}
	}
}

// valueToJSON converts a *Value to a Go interface{} suitable for JSON marshaling.
func valueToJSON(t *testing.T, v *Value) interface{} {
	t.Helper()
	switch v.Type() {
	case TypeNull:
		return nil
	case TypeBool:
		return v.Bool()
	case TypeInt64:
		return v.Int64()
	case TypeFloat64:
		return v.Float64()
	case TypeString:
		return v.String()
	case TypeBytes:
		return base64.StdEncoding.EncodeToString(v.Bytes())
	case TypeArray:
		arr := make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			arr[i] = valueToJSON(t, v.Index(i))
		}
		return arr
	case TypeObject:
		m := make(map[string]interface{})
		for _, mem := range v.Members() {
			m[mem.Key] = valueToJSON(t, mem.Value)
		}
		return m
	default:
		t.Fatalf("unsupported type for JSON conversion: %v", v.Type())
		return nil
	}
}

func TestTruthTable(t *testing.T) {
	cases := loadTruthCases(t)
	if len(cases) == 0 {
		t.Fatal("no truth cases loaded")
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.ID, func(t *testing.T) {
			switch tc.Action {
			case "roundtrip":
				testRoundtrip(t, tc)
			case "encode_decode":
				testEncodeDecode(t, tc)
			case "decode_raw":
				testDecodeRaw(t, tc)
			case "trailing_garbage":
				testTrailingGarbage(t, tc)
			case "truncated":
				testTruncated(t, tc)
			case "roundtrip_depth":
				testRoundtripDepth(t, tc)
			default:
				t.Skipf("unsupported action: %s", tc.Action)
			}
		})
	}
}

func testRoundtrip(t *testing.T, tc truthCase) {
	t.Helper()
	v := buildValue(t, tc.Input)

	data, err := Encode(v)
	if tc.Expect.OK {
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
	} else {
		if err != nil {
			return // expected failure at encode
		}
	}

	decoded, err := Decode(data)
	if tc.Expect.OK {
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
	} else {
		if err == nil {
			t.Fatal("expected decode error, got nil")
		}
		return
	}

	// Check special float expectations
	if tc.Expect.IsNaN {
		if decoded.Type() != TypeFloat64 || !math.IsNaN(decoded.Float64()) {
			t.Fatalf("expected NaN, got type=%v val=%v", decoded.Type(), decoded.Float64())
		}
		return
	}
	if tc.Expect.IsPositiveInf {
		if decoded.Type() != TypeFloat64 || !math.IsInf(decoded.Float64(), 1) {
			t.Fatalf("expected +Inf, got type=%v val=%v", decoded.Type(), decoded.Float64())
		}
		return
	}
	if tc.Expect.IsNegativeInf {
		if decoded.Type() != TypeFloat64 || !math.IsInf(decoded.Float64(), -1) {
			t.Fatalf("expected -Inf, got type=%v val=%v", decoded.Type(), decoded.Float64())
		}
		return
	}
	if tc.Expect.NegativeZero {
		if decoded.Type() != TypeFloat64 {
			t.Fatalf("expected float64, got %v", decoded.Type())
		}
		f := decoded.Float64()
		if f != 0 || !math.Signbit(f) {
			t.Fatalf("expected -0.0, got %v (signbit=%v)", f, math.Signbit(f))
		}
		return
	}

	// Check base64 bytes expectation
	if tc.Expect.ValueBase64 != nil {
		if decoded.Type() != TypeBytes {
			t.Fatalf("expected bytes, got %v", decoded.Type())
		}
		got := base64.StdEncoding.EncodeToString(decoded.Bytes())
		if got != *tc.Expect.ValueBase64 {
			t.Fatalf("bytes mismatch: got %q, expect %q", got, *tc.Expect.ValueBase64)
		}
		return
	}

	// General value comparison via JSON
	if tc.Expect.Value != nil {
		valuesEqualJSON(t, decoded, tc.Expect.Value)
	}
}

func testEncodeDecode(t *testing.T, tc truthCase) {
	t.Helper()
	var inp truthInput
	if err := json.Unmarshal(tc.Input, &inp); err != nil {
		t.Fatalf("bad input: %v", err)
	}

	var v *Value
	if inp.Type == "object" && inp.Entries != nil {
		v = buildObject(t, inp.Entries)
	} else {
		v = buildValue(t, tc.Input)
	}

	data, err := Encode(v)
	if err != nil {
		if !tc.Expect.OK {
			return
		}
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(data)
	if tc.Expect.OK {
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		if tc.Expect.Value != nil {
			valuesEqualJSON(t, decoded, tc.Expect.Value)
		}
	} else {
		if err == nil {
			t.Fatal("expected decode error, got nil")
		}
	}
}

func testDecodeRaw(t *testing.T, tc truthCase) {
	t.Helper()

	var inp truthInput
	if tc.Input != nil {
		if err := json.Unmarshal(tc.Input, &inp); err != nil {
			t.Fatalf("bad input: %v", err)
		}
	}

	var hexStr string
	if inp.Value != nil {
		if err := json.Unmarshal(inp.Value, &hexStr); err != nil {
			t.Fatalf("bad hex value: %v", err)
		}
	}

	var data []byte
	if hexStr != "" {
		var err error
		data, err = hex.DecodeString(hexStr)
		if err != nil {
			t.Fatalf("bad hex: %v", err)
		}
	}
	// empty hexStr means zero-length input

	_, err := Decode(data)
	if tc.Expect.OK {
		if err != nil {
			t.Fatalf("expected success, got: %v", err)
		}
	} else {
		if err == nil {
			t.Fatal("expected error for raw decode, got nil")
		}
	}
}

func testTrailingGarbage(t *testing.T, tc truthCase) {
	t.Helper()
	// Encode a simple value, append garbage, decode must error
	v := Object(Member{Key: "a", Value: Int64(1)})
	data, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	corrupted := make([]byte, len(data)+1)
	copy(corrupted, data)
	corrupted[len(data)] = 0xFF

	_, err = Decode(corrupted)
	if tc.Expect.OK {
		if err != nil {
			t.Fatalf("expected success, got: %v", err)
		}
	} else {
		if err == nil {
			t.Fatal("expected error for trailing garbage, got nil")
		}
		if !errors.Is(err, ErrTrailingData) {
			t.Fatalf("expected ErrTrailingData, got: %v", err)
		}
	}
}

func testTruncated(t *testing.T, tc truthCase) {
	t.Helper()
	// Encode a value with enough bytes to truncate meaningfully
	v := Object(
		Member{Key: "key", Value: String("value")},
		Member{Key: "num", Value: Int64(12345)},
	)
	data, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if len(data) < 2 {
		t.Fatal("encoded data too short to truncate")
	}

	truncated := data[:len(data)/2]
	_, err = Decode(truncated)
	if tc.Expect.OK {
		if err != nil {
			t.Fatalf("expected success, got: %v", err)
		}
	} else {
		if err == nil {
			t.Fatal("expected error for truncated input, got nil")
		}
	}
}

func testRoundtripDepth(t *testing.T, tc truthCase) {
	t.Helper()
	var inp truthInput
	if err := json.Unmarshal(tc.Input, &inp); err != nil {
		t.Fatalf("bad input: %v", err)
	}

	v := buildNestedArrays(inp.Depth)

	data, err := Encode(v)
	if tc.Expect.OK {
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
	} else {
		if err != nil {
			return
		}
	}

	decoded, err := Decode(data)
	if tc.Expect.OK {
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		verifyNestedArrayDepth(t, decoded, inp.Depth)
	} else {
		if err == nil {
			t.Fatal("expected decode error, got nil")
		}
	}
}
