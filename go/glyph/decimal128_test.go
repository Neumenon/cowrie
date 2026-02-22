package glyph

import (
	"math"
	"testing"
)

func TestNewDecimal128FromInt64(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		expected string
	}{
		{"zero", 0, "0"},
		{"positive", 123, "123"},
		{"negative", -456, "-456"},
		{"large", 999999999, "999999999"},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDecimal128FromInt64(tt.value)
			if d.String() != tt.expected {
				t.Errorf("got %q, want %q", d.String(), tt.expected)
			}
			if d.ToInt64() != tt.value {
				t.Errorf("ToInt64() got %d, want %d", d.ToInt64(), tt.value)
			}
		})
	}
}

func TestNewDecimal128FromString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"simple", "123", false},
		{"decimal", "123.45", false},
		{"negative", "-99.99", false},
		{"small", "0.0001", false},
		{"invalid", "abc", true},
		{"too many dots", "1.2.3", true},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDecimal128FromString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDecimal128String(t *testing.T) {
	tests := []struct {
		name string
		d    Decimal128
		want string
	}{
		{"from int", NewDecimal128FromInt64(100), "100"},
		{"from string", mustParseDecimal("99.99"), "99.99"},
		{"negative", mustParseDecimal("-50.25"), "-50.25"},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.d.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDecimal128Arithmetic(t *testing.T) {
	t.Run("add", func(t *testing.T) {
		d1 := mustParseDecimal("100.50")
		d2 := mustParseDecimal("50.25")
		result, err := d1.Add(d2)
		if err != nil {
			t.Fatalf("Add error: %v", err)
		}
		if result.String() != "150.75" {
			t.Errorf("Add: got %q, want %q", result.String(), "150.75")
		}
	})

	t.Run("subtract", func(t *testing.T) {
		d1 := mustParseDecimal("100.50")
		d2 := mustParseDecimal("30.25")
		result, err := d1.Sub(d2)
		if err != nil {
			t.Fatalf("Sub error: %v", err)
		}
		if result.String() != "70.25" {
			t.Errorf("Sub: got %q, want %q", result.String(), "70.25")
		}
	})

	t.Run("multiply", func(t *testing.T) {
		d1 := mustParseDecimal("12.5")
		d2 := mustParseDecimal("4")
		result, err := d1.Mul(d2)
		if err != nil {
			t.Fatalf("Mul error: %v", err)
		}
		if result.String() != "50.0" {
			t.Errorf("Mul: got %q, want %q", result.String(), "50.0")
		}
	})

	t.Run("divide", func(t *testing.T) {
		d1 := mustParseDecimal("100")
		d2 := mustParseDecimal("4")
		result, err := d1.Div(d2)
		if err != nil {
			t.Fatalf("Div error: %v", err)
		}
		// Division result will have scale = 0 - 0 = 0
		if result.String() != "25" {
			t.Errorf("Div: got %q, want %q", result.String(), "25")
		}
	})
}

func TestDecimal128Comparison(t *testing.T) {
	t.Run("equal", func(t *testing.T) {
		d1 := mustParseDecimal("100.50")
		d2 := mustParseDecimal("100.50")
		if !d1.Equal(d2) {
			t.Error("Equal: expected true")
		}
	})
	
	t.Run("less than", func(t *testing.T) {
		d1 := mustParseDecimal("50.00")
		d2 := mustParseDecimal("100.00")
		if !d1.LessThan(d2) {
			t.Error("LessThan: expected true")
		}
	})
	
	t.Run("greater than", func(t *testing.T) {
		d1 := mustParseDecimal("200.00")
		d2 := mustParseDecimal("100.00")
		if !d1.GreaterThan(d2) {
			t.Error("GreaterThan: expected true")
		}
	})
}

func TestDecimal128Predicates(t *testing.T) {
	t.Run("is zero", func(t *testing.T) {
		d := NewDecimal128FromInt64(0)
		if !d.IsZero() {
			t.Error("IsZero: expected true")
		}
	})
	
	t.Run("is negative", func(t *testing.T) {
		d := mustParseDecimal("-100.00")
		if !d.IsNegative() {
			t.Error("IsNegative: expected true")
		}
	})
	
	t.Run("is positive", func(t *testing.T) {
		d := mustParseDecimal("100.00")
		if !d.IsPositive() {
			t.Error("IsPositive: expected true")
		}
	})
}

func TestDecimal128Negate(t *testing.T) {
	d := mustParseDecimal("99.99")
	neg := d.Neg()
	if !neg.IsNegative() {
		t.Error("Neg: expected negative")
	}
	sum, err := neg.Add(d)
	if err != nil {
		t.Fatalf("Add error: %v", err)
	}
	if !sum.IsZero() {
		t.Errorf("Neg: d + neg(d) should be 0, got %s", sum.String())
	}
}

func TestDecimal128Abs(t *testing.T) {
	d := mustParseDecimal("-99.99")
	abs := d.Abs()
	if !abs.IsPositive() {
		t.Error("Abs: expected positive")
	}
}

func TestDecimal128FinancialUseCase(t *testing.T) {
	// Price tracking
	unitPrice := mustParseDecimal("19.99")
	qty := mustParseDecimal("5")
	total, err := unitPrice.Mul(qty)
	if err != nil {
		t.Fatalf("Mul error: %v", err)
	}

	if total.String() != "99.95" {
		t.Errorf("Price calc: got %q, want %q", total.String(), "99.95")
	}
}

func TestDecimal128CryptoUseCase(t *testing.T) {
	// Crypto amount
	btc := mustParseDecimal("0.00001234")
	if !btc.IsPositive() {
		t.Error("Crypto amount should be positive")
	}
}

func TestIsDecimalLiteral(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want bool
	}{
		{"valid", "99.99m", true},
		{"integer", "100m", true},
		{"negative", "-50.25m", true},
		{"no m suffix", "99.99", false},
		{"no decimal", "100", false},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsDecimalLiteral(tt.s); got != tt.want {
				t.Errorf("IsDecimalLiteral(%q) = %v, want %v", tt.s, got, tt.want)
			}
		})
	}
}

func TestParseDecimalLiteral(t *testing.T) {
	d, err := ParseDecimalLiteral("99.99m")
	if err != nil {
		t.Fatalf("ParseDecimalLiteral: %v", err)
	}
	if d.String() != "99.99" {
		t.Errorf("got %q, want %q", d.String(), "99.99")
	}
}

func TestDecimal128ToFloat64(t *testing.T) {
	d := mustParseDecimal("123.45")
	f := d.ToFloat64()
	
	// Allow for floating point precision errors
	if math.Abs(f-123.45) > 0.0001 {
		t.Errorf("ToFloat64: got %f, want 123.45", f)
	}
}

func TestDecimal128FromFloat64(t *testing.T) {
	d, err := NewDecimal128FromFloat64(123.45)
	if err != nil {
		t.Fatalf("NewDecimal128FromFloat64: %v", err)
	}
	
	f := d.ToFloat64()
	if math.Abs(f-123.45) > 0.0001 {
		t.Errorf("roundtrip: got %f, want 123.45", f)
	}
}

// ============================================================
// Helper Functions
// ============================================================

func mustParseDecimal(s string) Decimal128 {
	d, err := NewDecimal128FromString(s)
	if err != nil {
		panic(err)
	}
	return d
}

// Benchmark encoding operations
func BenchmarkDecimal128Add(b *testing.B) {
	d1 := mustParseDecimal("100.50")
	d2 := mustParseDecimal("50.25")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d1.Add(d2)
	}
}

func BenchmarkDecimal128Mul(b *testing.B) {
	d1 := mustParseDecimal("12.5")
	d2 := mustParseDecimal("4")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d1.Mul(d2)
	}
}

func BenchmarkDecimal128String(b *testing.B) {
	d := mustParseDecimal("99.99")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.String()
	}
}

func BenchmarkNewDecimal128FromString(b *testing.B) {
	s := "123.456789"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewDecimal128FromString(s)
	}
}
