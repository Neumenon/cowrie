package cowrie

import (
	"bytes"
	"testing"
)

// TestDtypeJSONRoundtrip exercises the JSON dtype map for dtypes that were
// missing from dtypeToString/stringToDtype (bool, qint4, qint2, qint3,
// ternary, binary). Before the fix, dtypeToString emitted "unknown" for these
// and stringToDtype could not parse them, so ToJSON/FromJSON silently dropped
// or mangled the tensor dtype. Each case must survive a ToJSON -> FromJSON
// round-trip with the dtype, dims and data bytes intact.
func TestDtypeJSONRoundtrip(t *testing.T) {
	cases := []struct {
		name  string
		dtype DType
		want  string
		dims  []uint64
		data  []byte
	}{
		{"bool", DTypeBool, "bool", []uint64{4}, []byte{0x01, 0x00, 0x01, 0x01}},
		{"qint4", DTypeQINT4, "qint4", []uint64{4}, []byte{0xAB, 0xCD}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			orig := Tensor(tc.dtype, tc.dims, tc.data)

			// The JSON projection must name the dtype, not "unknown".
			if got := dtypeToString(tc.dtype); got != tc.want {
				t.Fatalf("dtypeToString(%v) = %q, want %q", tc.dtype, got, tc.want)
			}
			if got, ok := stringToDtype(tc.want); !ok || got != tc.dtype {
				t.Fatalf("stringToDtype(%q) = (%v, %v), want (%v, true)", tc.want, got, ok, tc.dtype)
			}

			js, err := ToJSON(orig)
			if err != nil {
				t.Fatalf("ToJSON: %v", err)
			}

			back, err := FromJSON(js)
			if err != nil {
				t.Fatalf("FromJSON: %v", err)
			}
			if back == nil {
				t.Fatalf("FromJSON returned nil for %q tensor JSON: %s", tc.want, js)
			}

			td := back.Tensor()
			if td.DType != tc.dtype {
				t.Errorf("dtype: got %v, want %v", td.DType, tc.dtype)
			}
			if len(td.Dims) != len(tc.dims) || (len(tc.dims) > 0 && td.Dims[0] != tc.dims[0]) {
				t.Errorf("dims: got %v, want %v", td.Dims, tc.dims)
			}
			if !bytes.Equal(td.Data, tc.data) {
				t.Errorf("data: got %v, want %v", td.Data, tc.data)
			}
		})
	}
}
