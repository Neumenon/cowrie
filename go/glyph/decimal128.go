package glyph

import (
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

// Decimal128 arithmetic errors
var (
	ErrDivisionByZero = errors.New("decimal128: division by zero")
	ErrOverflow       = errors.New("decimal128: overflow")
)

// Decimal128 represents a 128-bit decimal number: value = coefficient * 10^(-scale)
// where scale is -127 to 127 (8-bit signed) and coefficient is 16 bytes.
//
// This provides high-precision decimal arithmetic for financial, scientific,
// and blockchain applications where rounding errors are unacceptable.
type Decimal128 struct {
	Scale int8   // Exponent: -127 to 127
	Coef  [16]byte // 128-bit coefficient (two's complement, big-endian)
}

// NewDecimal128FromInt64 creates a Decimal128 from an int64.
func NewDecimal128FromInt64(value int64) Decimal128 {
	d := Decimal128{Scale: 0}
	coefInt := big.NewInt(value)
	coefBytes := coefInt.Bytes()
	
	// Handle negative numbers (two's complement)
	if value < 0 {
		// Convert to two's complement in 16 bytes
		coefInt.Add(coefInt, new(big.Int).Lsh(big.NewInt(1), 128))
		coefBytes = coefInt.Bytes()
	}
	
	// Copy to fixed 16-byte array
	if len(coefBytes) <= 16 {
		copy(d.Coef[16-len(coefBytes):], coefBytes)
	}
	
	return d
}

// NewDecimal128FromString creates a Decimal128 from a string representation.
// Examples: "123.45", "99.99", "0.0001234"
func NewDecimal128FromString(s string) (Decimal128, error) {
	s = strings.TrimSpace(s)
	
	// Find decimal point
	parts := strings.Split(s, ".")
	if len(parts) > 2 {
		return Decimal128{}, fmt.Errorf("invalid decimal format: %s", s)
	}
	
	d := Decimal128{}
	
	if len(parts) == 2 {
		// Has decimal part
		intPart := parts[0]
		fracPart := parts[1]
		
		// Scale is the number of fractional digits
		d.Scale = int8(len(fracPart))
		if d.Scale < -127 || d.Scale > 127 {
			return Decimal128{}, fmt.Errorf("scale out of range: %d", d.Scale)
		}
		
		// Combine into single integer
		coefStr := intPart + fracPart
		coefInt := new(big.Int)
		_, ok := coefInt.SetString(coefStr, 10)
		if !ok {
			return Decimal128{}, fmt.Errorf("invalid number: %s", s)
		}

		// Convert to 16 bytes
		coefBytes := intToCoef(coefInt)
		copy(d.Coef[:], coefBytes[:])
	} else {
		// No decimal part
		d.Scale = 0
		coefInt := new(big.Int)
		_, ok := coefInt.SetString(parts[0], 10)
		if !ok {
			return Decimal128{}, fmt.Errorf("invalid number: %s", s)
		}
		
		coefBytes := intToCoef(coefInt)
		copy(d.Coef[:], coefBytes[:])
	}

	return d, nil
}

// NewDecimal128FromFloat64 creates a Decimal128 from a float64.
// Note: Precision may be lost.
func NewDecimal128FromFloat64(f float64) (Decimal128, error) {
	return NewDecimal128FromString(strconv.FormatFloat(f, 'f', -1, 64))
}

// ToInt64 converts the decimal to int64 (truncates fractional part).
func (d Decimal128) ToInt64() int64 {
	coefInt := coefToInt(d.Coef[:])
	return coefInt.Int64()
}

// ToFloat64 converts the decimal to float64.
// Note: Precision may be lost.
func (d Decimal128) ToFloat64() float64 {
	coefInt := coefToInt(d.Coef[:])
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.Scale)), nil)
	result := new(big.Float).SetInt(coefInt)
	scalef := new(big.Float).SetInt(scale)
	result.Quo(result, scalef)
	f, _ := result.Float64()
	return f
}

// String returns the string representation.
func (d Decimal128) String() string {
	coefInt := coefToInt(d.Coef[:])
	coefStr := coefInt.String()
	
	if d.Scale == 0 {
		return coefStr
	}
	
	// Handle negative
	negative := false
	if coefStr[0] == '-' {
		negative = true
		coefStr = coefStr[1:]
	}
	
	// Pad with zeros if needed
	for len(coefStr) < int(d.Scale)+1 {
		coefStr = "0" + coefStr
	}
	
	// Insert decimal point
	insertPos := len(coefStr) - int(d.Scale)
	result := coefStr[:insertPos] + "." + coefStr[insertPos:]
	
	if negative {
		result = "-" + result
	}
	
	return result
}

// Add returns d + other.
func (d Decimal128) Add(other Decimal128) (Decimal128, error) {
	coef1 := coefToInt(d.Coef[:])
	coef2 := coefToInt(other.Coef[:])

	targetScale := d.Scale
	if d.Scale != other.Scale {
		if d.Scale < other.Scale {
			scale := int64(other.Scale) - int64(d.Scale)
			coef1.Mul(coef1, new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil))
			targetScale = other.Scale
		} else {
			scale := int64(d.Scale) - int64(other.Scale)
			coef2.Mul(coef2, new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil))
		}
	}

	coef1.Add(coef1, coef2)

	if coef1.BitLen() > 127 {
		return Decimal128{}, ErrOverflow
	}

	result := Decimal128{Scale: targetScale}
	coefResult := intToCoef(coef1)
	copy(result.Coef[:], coefResult[:])
	return result, nil
}

// Sub returns d - other.
func (d Decimal128) Sub(other Decimal128) (Decimal128, error) {
	coef2 := coefToInt(other.Coef[:])
	coef2.Neg(coef2)
	negOther := Decimal128{Scale: other.Scale}
	negCoef := intToCoef(coef2)
	copy(negOther.Coef[:], negCoef[:])
	return d.Add(negOther)
}

// Mul returns d * other.
func (d Decimal128) Mul(other Decimal128) (Decimal128, error) {
	coef1 := coefToInt(d.Coef[:])
	coef2 := coefToInt(other.Coef[:])
	coef1.Mul(coef1, coef2)

	if coef1.BitLen() > 127 {
		return Decimal128{}, ErrOverflow
	}

	result := Decimal128{Scale: d.Scale + other.Scale}
	mulCoef := intToCoef(coef1)
	copy(result.Coef[:], mulCoef[:])
	return result, nil
}

// Div returns d / other.
func (d Decimal128) Div(other Decimal128) (Decimal128, error) {
	coef1 := coefToInt(d.Coef[:])
	coef2 := coefToInt(other.Coef[:])

	if coef2.Sign() == 0 {
		return Decimal128{}, ErrDivisionByZero
	}

	coef1.Div(coef1, coef2)

	result := Decimal128{Scale: d.Scale - other.Scale}
	divCoef := intToCoef(coef1)
	copy(result.Coef[:], divCoef[:])
	return result, nil
}

// Abs returns the absolute value.
func (d Decimal128) Abs() Decimal128 {
	coefInt := coefToInt(d.Coef[:])
	if coefInt.Sign() < 0 {
		coefInt.Neg(coefInt)
		coefBytes := intToCoef(coefInt)
		copy(d.Coef[:], coefBytes[:])
	}
	return d
}

// Neg returns the negation.
func (d Decimal128) Neg() Decimal128 {
	coefInt := coefToInt(d.Coef[:])
	coefInt.Neg(coefInt)
	coefBytes := intToCoef(coefInt)
	copy(d.Coef[:], coefBytes[:])
	return d
}

// Cmp compares two decimals. Returns -1 if d < other, 0 if d == other, 1 if d > other.
func (d Decimal128) Cmp(other Decimal128) int {
	d1 := coefToInt(d.Coef[:])
	d2 := coefToInt(other.Coef[:])
	
	// Align scales
	if d.Scale != other.Scale {
		if d.Scale < other.Scale {
			scale := int64(other.Scale) - int64(d.Scale)
			d1.Mul(d1, new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil))
		} else {
			scale := int64(d.Scale) - int64(other.Scale)
			d2.Mul(d2, new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil))
		}
	}
	
	return d1.Cmp(d2)
}

// Equal returns true if d == other.
func (d Decimal128) Equal(other Decimal128) bool {
	return d.Cmp(other) == 0
}

// LessThan returns true if d < other.
func (d Decimal128) LessThan(other Decimal128) bool {
	return d.Cmp(other) < 0
}

// GreaterThan returns true if d > other.
func (d Decimal128) GreaterThan(other Decimal128) bool {
	return d.Cmp(other) > 0
}

// IsZero returns true if d == 0.
func (d Decimal128) IsZero() bool {
	return d.Cmp(NewDecimal128FromInt64(0)) == 0
}

// IsNegative returns true if d < 0.
func (d Decimal128) IsNegative() bool {
	coefInt := coefToInt(d.Coef[:])
	return coefInt.Sign() < 0
}

// IsPositive returns true if d > 0.
func (d Decimal128) IsPositive() bool {
	coefInt := coefToInt(d.Coef[:])
	return coefInt.Sign() > 0
}

// ============================================================
// Helper Functions
// ============================================================

// intToCoef converts a big.Int to 16-byte two's complement representation.
func intToCoef(value *big.Int) [16]byte {
	var result [16]byte
	
	if value.Sign() >= 0 {
		// Positive or zero: direct conversion
		bytes := value.Bytes()
		if len(bytes) > 16 {
			// Truncate (loss of precision)
			bytes = bytes[len(bytes)-16:]
		}
		copy(result[16-len(bytes):], bytes)
	} else {
		// Negative: two's complement
		// value + 2^128 gives the two's complement representation
		temp := new(big.Int).Add(value, new(big.Int).Lsh(big.NewInt(1), 128))
		bytes := temp.Bytes()
		if len(bytes) <= 16 {
			copy(result[16-len(bytes):], bytes)
		}
	}
	
	return result
}

// coefToInt converts a 16-byte two's complement representation to big.Int.
func coefToInt(coef []byte) *big.Int {
	if len(coef) != 16 {
		return big.NewInt(0)
	}
	
	// Check sign bit (MSB of first byte)
	negative := coef[0]&0x80 != 0
	
	result := new(big.Int).SetBytes(coef)
	
	if negative {
		// Two's complement: subtract 2^128
		result.Sub(result, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	
	return result
}

// DecimalFromAny creates a Decimal128 from various types.
func DecimalFromAny(value interface{}) (Decimal128, error) {
	switch v := value.(type) {
	case Decimal128:
		return v, nil
	case string:
		return NewDecimal128FromString(v)
	case int:
		return NewDecimal128FromInt64(int64(v)), nil
	case int64:
		return NewDecimal128FromInt64(v), nil
	case float64:
		return NewDecimal128FromFloat64(v)
	default:
		return Decimal128{}, fmt.Errorf("cannot convert %T to Decimal128", value)
	}
}

// ParseDecimalRegex matches decimal literals with "m" suffix (e.g., "99.99m")
var ParseDecimalRegex = regexp.MustCompile(`^(-?\d+(?:\.\d+)?)m$`)

// IsDecimalLiteral checks if a string is a decimal literal with "m" suffix.
func IsDecimalLiteral(s string) bool {
	return ParseDecimalRegex.MatchString(s)
}

// ParseDecimalLiteral parses a decimal literal with "m" suffix.
func ParseDecimalLiteral(s string) (Decimal128, error) {
	matches := ParseDecimalRegex.FindStringSubmatch(s)
	if len(matches) != 2 {
		return Decimal128{}, fmt.Errorf("not a valid decimal literal: %s", s)
	}
	return NewDecimal128FromString(matches[1])
}
