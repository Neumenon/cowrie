"""
Tests for Decimal128 type

Tests high-precision decimal support for financial, scientific, and blockchain applications.
"""

import pytest
from decimal import Decimal as PyDecimal
from glyph.decimal import Decimal128, decimal, _int_to_coef, _coef_to_int


class TestDecimal128Creation:
    """Test creating Decimal128 values."""
    
    def test_from_decimal(self):
        """Create from Python Decimal."""
        d = Decimal128.from_decimal(PyDecimal("123.45"))
        assert d.scale == 2
        assert d.to_decimal() == PyDecimal("123.45")
    
    def test_from_string(self):
        """Create from string."""
        d = Decimal128.from_string("999.99")
        assert d.to_decimal() == PyDecimal("999.99")
    
    def test_from_int(self):
        """Create from integer."""
        d = Decimal128.from_int(100)
        assert d.scale == 0
        assert d.to_decimal() == PyDecimal("100")
    
    def test_from_float(self):
        """Create from float (with precision caveats)."""
        d = Decimal128.from_float(123.45)
        # Note: float precision loss means we check approximate equality
        assert abs(d.to_float() - 123.45) < 0.0001
    
    def test_convenience_function(self):
        """Test decimal() helper function."""
        d1 = decimal("123.45")
        d2 = decimal(123)
        d3 = decimal(PyDecimal("999.99"))
        
        assert isinstance(d1, Decimal128)
        assert isinstance(d2, Decimal128)
        assert isinstance(d3, Decimal128)
    
    def test_invalid_scale(self):
        """Test that invalid scale raises error."""
        with pytest.raises(ValueError):
            # Scale out of range
            Decimal128(scale=200, coef=b'\x00' * 16)
    
    def test_invalid_coef_length(self):
        """Test that invalid coefficient length raises error."""
        with pytest.raises(ValueError):
            Decimal128(scale=2, coef=b'\x00' * 15)  # Only 15 bytes


class TestDecimal128Arithmetic:
    """Test arithmetic operations."""
    
    def test_addition(self):
        """Test addition."""
        d1 = Decimal128.from_string("100.50")
        d2 = Decimal128.from_string("50.25")
        result = d1 + d2
        assert result.to_decimal() == PyDecimal("150.75")
    
    def test_subtraction(self):
        """Test subtraction."""
        d1 = Decimal128.from_string("100.50")
        d2 = Decimal128.from_string("30.25")
        result = d1 - d2
        assert result.to_decimal() == PyDecimal("70.25")
    
    def test_multiplication(self):
        """Test multiplication."""
        d1 = Decimal128.from_string("12.5")
        d2 = Decimal128.from_string("4")
        result = d1 * d2
        assert result.to_decimal() == PyDecimal("50")
    
    def test_division(self):
        """Test division."""
        d1 = Decimal128.from_string("100")
        d2 = Decimal128.from_string("4")
        result = d1 / d2
        assert result.to_decimal() == PyDecimal("25")
    
    def test_negate(self):
        """Test negation."""
        d = Decimal128.from_string("123.45")
        neg = d.negate()
        assert neg.to_decimal() == PyDecimal("-123.45")
    
    def test_abs(self):
        """Test absolute value."""
        d = Decimal128.from_string("-123.45")
        abs_d = d.abs()
        assert abs_d.to_decimal() == PyDecimal("123.45")


class TestDecimal128Comparison:
    """Test comparison operations."""
    
    def test_equality(self):
        """Test equality."""
        d1 = Decimal128.from_string("123.45")
        d2 = Decimal128.from_string("123.45")
        d3 = Decimal128.from_string("123.46")
        
        assert d1 == d2
        assert not (d1 == d3)
    
    def test_less_than(self):
        """Test less than."""
        d1 = Decimal128.from_string("100")
        d2 = Decimal128.from_string("200")
        
        assert d1 < d2
        assert not (d2 < d1)
    
    def test_greater_than(self):
        """Test greater than."""
        d1 = Decimal128.from_string("200")
        d2 = Decimal128.from_string("100")
        
        assert d1 > d2
        assert not (d2 > d1)
    
    def test_ordering(self):
        """Test ordering with multiple values."""
        values = [
            Decimal128.from_string("50"),
            Decimal128.from_string("100"),
            Decimal128.from_string("25"),
            Decimal128.from_string("75"),
        ]
        
        sorted_values = sorted(values)
        assert sorted_values[0].to_decimal() == PyDecimal("25")
        assert sorted_values[-1].to_decimal() == PyDecimal("100")


class TestDecimal128Predicates:
    """Test predicate methods."""
    
    def test_is_zero(self):
        """Test zero detection."""
        d_zero = Decimal128.from_int(0)
        d_nonzero = Decimal128.from_int(1)
        
        assert d_zero.is_zero()
        assert not d_nonzero.is_zero()
    
    def test_is_negative(self):
        """Test negative detection."""
        d_neg = Decimal128.from_string("-5.0")
        d_pos = Decimal128.from_string("5.0")
        d_zero = Decimal128.from_int(0)
        
        assert d_neg.is_negative()
        assert not d_pos.is_negative()
        assert not d_zero.is_negative()
    
    def test_is_positive(self):
        """Test positive detection."""
        d_pos = Decimal128.from_string("5.0")
        d_neg = Decimal128.from_string("-5.0")
        d_zero = Decimal128.from_int(0)
        
        assert d_pos.is_positive()
        assert not d_neg.is_positive()
        assert not d_zero.is_positive()


class TestDecimal128Conversions:
    """Test type conversions."""
    
    def test_to_float(self):
        """Test conversion to float."""
        d = Decimal128.from_string("123.45")
        f = d.to_float()
        assert isinstance(f, float)
        assert abs(f - 123.45) < 0.0001
    
    def test_to_int(self):
        """Test conversion to int (truncates)."""
        d = Decimal128.from_string("123.99")
        i = d.to_int()
        assert i == 123
    
    def test_to_string(self):
        """Test string representation."""
        d = Decimal128.from_string("123.45")
        s = str(d)
        assert "123.45" in s


class TestDecimal128FinancialUseCases:
    """Test realistic financial use cases."""
    
    def test_price_calculation(self):
        """Test price calculations."""
        unit_price = Decimal128.from_string("19.99")
        quantity = Decimal128.from_string("5")
        total = unit_price * quantity
        
        assert total.to_decimal() == PyDecimal("99.95")
    
    def test_balance_precision(self):
        """Test balance tracking precision."""
        balance = Decimal128.from_string("1000000.00")
        fee = Decimal128.from_string("0.01")
        
        new_balance = balance - fee
        assert new_balance.to_decimal() == PyDecimal("999999.99")
    
    def test_exchange_rate(self):
        """Test exchange rate calculations."""
        usd_amount = Decimal128.from_string("100.00")
        exchange_rate = Decimal128.from_string("0.92")  # EUR/USD
        
        eur_amount = usd_amount * exchange_rate
        assert eur_amount.to_decimal() == PyDecimal("92.00")
    
    def test_cryptocurrency_amount(self):
        """Test cryptocurrency precision."""
        btc_amount = Decimal128.from_string("0.00001234")
        btc_price = Decimal128.from_string("50000.00")
        
        usd_value = btc_amount * btc_price
        assert usd_value.to_decimal() == PyDecimal("0.617")


class TestDecimal128ScientificUseCases:
    """Test scientific/precise calculation use cases."""
    
    def test_very_small_numbers(self):
        """Test very small numbers (scientific notation)."""
        small = Decimal128.from_string("0.0000000001")
        assert small.to_decimal() == PyDecimal("0.0000000001")
    
    def test_very_large_numbers(self):
        """Test very large numbers."""
        large = Decimal128.from_string("999999999999999999999999999999999")
        assert large.is_positive()
    
    def test_precision_preservation(self):
        """Test that precision is preserved through conversions."""
        original = PyDecimal("1.23456789012345678901234567890")
        d = Decimal128.from_decimal(original)
        result = d.to_decimal()
        
        # Should preserve most digits (up to Decimal128 precision)
        assert str(result).startswith("1.234567890123456789")


class TestDecimal128Encoding:
    """Test encoding/decoding (for future GLYPH integration)."""
    
    def test_coef_round_trip(self):
        """Test coefficient encoding/decoding."""
        value = 123456789
        coef = _int_to_coef(value)
        assert len(coef) == 16
        
        decoded = _coef_to_int(coef)
        assert decoded == value
    
    def test_negative_coef(self):
        """Test negative coefficient handling."""
        value = -123456789
        coef = _int_to_coef(value)
        decoded = _coef_to_int(coef)
        assert decoded == value
    
    def test_zero_coef(self):
        """Test zero coefficient."""
        coef = _int_to_coef(0)
        assert coef == b'\x00' * 16
        assert _coef_to_int(coef) == 0


class TestDecimal128EdgeCases:
    """Test edge cases and error handling."""
    
    def test_nan_rejection(self):
        """Test that NaN is rejected."""
        with pytest.raises(ValueError):
            Decimal128.from_decimal(PyDecimal('NaN'))
    
    def test_infinity_rejection(self):
        """Test that Infinity is rejected."""
        with pytest.raises(ValueError):
            Decimal128.from_decimal(PyDecimal('Infinity'))
    
    def test_scale_clamping(self):
        """Test that scale is clamped to valid range."""
        # Create with very large exponent (should clamp to 127)
        d = Decimal128.from_string("0." + "0" * 150 + "1")
        assert -127 <= d.scale <= 127
    
    def test_type_error(self):
        """Test type errors in convenience function."""
        with pytest.raises(TypeError):
            decimal([1, 2, 3])  # Invalid type


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
