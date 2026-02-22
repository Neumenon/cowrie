"""
Decimal128 - High-precision decimal type for GLYPH

A 128-bit decimal for financial, scientific, and precise mathematical calculations.
Value = coefficient * 10^(-scale) where scale is -127 to 127.

This is critical for:
  - Financial data (prices, balances, transactions)
  - Cryptocurrency/blockchain (exact amounts)
  - Scientific calculations (precision-critical)
  - Accounting (no rounding errors)

Unlike float64, Decimal128:
  - Preserves exact decimal representation
  - No precision loss for large numbers (>2^53)
  - Compatible with standard decimal arithmetic
  - Suitable for financial/blockchain systems
"""

from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal as PyDecimal, getcontext, Context, ROUND_HALF_UP
import struct
from typing import Optional, Union


# Set default context for consistent rounding
getcontext().prec = 34  # Maximum precision for 128-bit decimal
getcontext().rounding = ROUND_HALF_UP


@dataclass(frozen=True)
class Decimal128:
    """
    128-bit decimal: value = coefficient * 10^(-scale)
    
    Where:
      - scale: exponent (-127 to 127)
      - coef: 128-bit coefficient (two's complement big-endian)
    
    Example:
        >>> d = Decimal128.from_decimal(Decimal("123.45"))
        >>> d.scale
        2
        >>> d.to_decimal()
        Decimal('123.45')
        
        >>> d = Decimal128(scale=2, coef=b'\\x00' * 15 + b'\\x64')  # 100
        >>> d.to_decimal()
        Decimal('1.00')
    """
    scale: int  # -127 to 127 (exponent)
    coef: bytes  # 16 bytes, two's complement representation
    
    def __post_init__(self):
        """Validate the decimal."""
        if not isinstance(self.coef, bytes):
            raise TypeError("coef must be bytes")
        
        if len(self.coef) != 16:
            raise ValueError(f"coef must be 16 bytes, got {len(self.coef)}")
        
        if not (-127 <= self.scale <= 127):
            raise ValueError(f"scale must be -127 to 127, got {self.scale}")
    
    @classmethod
    def from_decimal(cls, d: PyDecimal) -> Decimal128:
        """
        Create a Decimal128 from a Python Decimal.
        
        Args:
            d: Python Decimal value
            
        Returns:
            Decimal128 instance
            
        Example:
            >>> from decimal import Decimal
            >>> d128 = Decimal128.from_decimal(Decimal("123.45"))
        """
        # Handle special values
        if d.is_nan():
            raise ValueError("Cannot convert NaN to Decimal128")
        if d.is_infinite():
            raise ValueError("Cannot convert Infinity to Decimal128")
        
        # Get sign, digits, exponent from Decimal
        sign, digits, exponent = d.as_tuple()
        
        # Convert digits to integer coefficient
        coef_int = int(''.join(str(d) for d in digits))
        if sign:
            coef_int = -coef_int
        
        # Calculate scale: exponent is negative in Decimal representation
        # e.g., Decimal("123.45") has exponent=-2, scale=2
        scale = -exponent
        
        # Clamp scale to valid range
        if scale < -127:
            # Scale too small - shift left
            shift = -127 - scale
            coef_int *= 10 ** shift
            scale = -127
        elif scale > 127:
            # Scale too large - need to truncate
            shift = scale - 127
            # Lose precision here (acceptable for very small numbers)
            coef_int //= (10 ** shift)
            scale = 127
        
        # Convert coefficient to 16-byte big-endian two's complement
        coef_bytes = _int_to_coef(coef_int)
        
        return cls(scale=scale, coef=coef_bytes)
    
    @classmethod
    def from_float(cls, f: float) -> Decimal128:
        """Create from float (with precision loss)."""
        return cls.from_decimal(PyDecimal(str(f)))
    
    @classmethod
    def from_int(cls, i: int) -> Decimal128:
        """Create from integer (scale=0)."""
        return cls.from_decimal(PyDecimal(i))
    
    @classmethod
    def from_string(cls, s: str) -> Decimal128:
        """Create from string representation."""
        return cls.from_decimal(PyDecimal(s))
    
    def to_decimal(self) -> PyDecimal:
        """Convert to Python Decimal."""
        coef_int = _coef_to_int(self.coef)
        
        # value = coef_int * 10^(-scale)
        return PyDecimal(coef_int) * (PyDecimal(10) ** (-self.scale))
    
    def to_float(self) -> float:
        """Convert to float (with precision loss)."""
        return float(self.to_decimal())
    
    def to_int(self) -> int:
        """Convert to int (truncates decimal part)."""
        return int(self.to_decimal())
    
    def __str__(self) -> str:
        """String representation."""
        return str(self.to_decimal())
    
    def __repr__(self) -> str:
        """Debug representation."""
        return f"Decimal128(scale={self.scale}, coef={self.coef.hex()})"
    
    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, Decimal128):
            return False
        return self.to_decimal() == other.to_decimal()
    
    def __lt__(self, other: Decimal128) -> bool:
        """Less than comparison."""
        return self.to_decimal() < other.to_decimal()
    
    def __le__(self, other: Decimal128) -> bool:
        """Less than or equal comparison."""
        return self.to_decimal() <= other.to_decimal()
    
    def __gt__(self, other: Decimal128) -> bool:
        """Greater than comparison."""
        return self.to_decimal() > other.to_decimal()
    
    def __ge__(self, other: Decimal128) -> bool:
        """Greater than or equal comparison."""
        return self.to_decimal() >= other.to_decimal()
    
    def __add__(self, other: Decimal128) -> Decimal128:
        """Addition."""
        result = self.to_decimal() + other.to_decimal()
        return Decimal128.from_decimal(result)
    
    def __sub__(self, other: Decimal128) -> Decimal128:
        """Subtraction."""
        result = self.to_decimal() - other.to_decimal()
        return Decimal128.from_decimal(result)
    
    def __mul__(self, other: Decimal128) -> Decimal128:
        """Multiplication."""
        result = self.to_decimal() * other.to_decimal()
        return Decimal128.from_decimal(result)
    
    def __truediv__(self, other: Decimal128) -> Decimal128:
        """Division."""
        result = self.to_decimal() / other.to_decimal()
        return Decimal128.from_decimal(result)
    
    def is_zero(self) -> bool:
        """Check if value is zero."""
        return self.to_decimal() == 0
    
    def is_negative(self) -> bool:
        """Check if value is negative."""
        return self.to_decimal() < 0
    
    def is_positive(self) -> bool:
        """Check if value is positive."""
        return self.to_decimal() > 0
    
    def abs(self) -> Decimal128:
        """Absolute value."""
        return Decimal128.from_decimal(abs(self.to_decimal()))
    
    def negate(self) -> Decimal128:
        """Negate the value."""
        return Decimal128.from_decimal(-self.to_decimal())


def _int_to_coef(value: int) -> bytes:
    """Convert int to 16-byte two's complement big-endian."""
    # Handle negative numbers in two's complement
    if value < 0:
        # Two's complement: find the equivalent positive value in 128-bit space
        value = (1 << 128) + value  # Add 2^128 for two's complement
    
    # Convert to 16 bytes big-endian
    return value.to_bytes(16, byteorder='big', signed=False)


def _coef_to_int(coef: bytes) -> int:
    """Convert 16-byte two's complement to int."""
    # Interpret as signed 128-bit big-endian
    value = int.from_bytes(coef, byteorder='big', signed=True)
    return value


# Convenience function for creating decimals
def decimal(value: Union[str, int, float, PyDecimal]) -> Decimal128:
    """
    Create a Decimal128 from various types.
    
    Args:
        value: String, int, float, or Decimal
        
    Returns:
        Decimal128 instance
        
    Example:
        >>> d = decimal("123.45")
        >>> d = decimal(123)
        >>> d = decimal(123.45)  # Loss of precision
    """
    if isinstance(value, Decimal128):
        return value
    elif isinstance(value, PyDecimal):
        return Decimal128.from_decimal(value)
    elif isinstance(value, str):
        return Decimal128.from_string(value)
    elif isinstance(value, int):
        return Decimal128.from_int(value)
    elif isinstance(value, float):
        return Decimal128.from_float(value)
    else:
        raise TypeError(f"Cannot create Decimal128 from {type(value)}")
