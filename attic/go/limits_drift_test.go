package cowrie

import (
	"testing"

	"github.com/Neumenon/cowrie/go/v2/gen1"
)

// TestDefaultLimitsDoNotDriftFromGen1 asserts the security defaults
// shared between the root cowrie package (gen2) and gen1 remain in
// lockstep. If either package tightens or relaxes a limit, both must
// move together.
func TestDefaultLimitsDoNotDriftFromGen1(t *testing.T) {
	cases := []struct {
		name     string
		gen2, g1 int64
	}{
		{"MaxDepth", DefaultMaxDepth, gen1.DefaultMaxDepth},
		{"MaxArrayLen", DefaultMaxArrayLen, gen1.DefaultMaxArrayLen},
		{"MaxObjectLen", int64(DefaultMaxObjectLen), gen1.DefaultMaxObjectLen},
		{"MaxStringLen", DefaultMaxStringLen, gen1.DefaultMaxStringLen},
		{"MaxBytesLen", DefaultMaxBytesLen, gen1.DefaultMaxBytesLen},
	}
	for _, c := range cases {
		if c.gen2 != c.g1 {
			t.Errorf("%s drift: root=%d gen1=%d — update both or justify the split",
				c.name, c.gen2, c.g1)
		}
	}
}
