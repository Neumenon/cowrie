"""
Extension to types.py - Decimal128 type support

This module extends the base types.py with Decimal128 support.
It should be integrated into types.py directly.
"""

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional

from .decimal import Decimal128


# Add to GType enum
class GTypeDecimal(Enum):
    """Extended GType values including Decimal."""
    # ... existing types 0-10 ...
    DECIMAL = 11  # 128-bit decimal


# Add to GValue class - new field
class GValueDecimalExtension:
    """Extension for Decimal128 support in GValue."""
    
    # In GValue.__init__ or as a field:
    # decimalVal: Optional[Decimal128] = None
    
    @staticmethod
    def decimal(value: Decimal128) -> 'GValue':
        """Create a Decimal128 value."""
        from .types import GValue
        gv = GValue()
        gv.typ = GTypeDecimal.DECIMAL.value
        gv.decimalVal = value
        return gv
    
    @staticmethod
    def to_decimal(gv: 'GValue') -> Decimal128:
        """Extract Decimal128 from GValue."""
        if gv.typ != GTypeDecimal.DECIMAL.value:
            raise TypeError(f"Expected Decimal, got {gv.typ}")
        return gv.decimalVal
    
    @staticmethod
    def is_decimal(gv: 'GValue') -> bool:
        """Check if GValue is a Decimal."""
        return gv.typ == GTypeDecimal.DECIMAL.value


# Integration notes:
# 1. Add to types.py GType constants:
#    TypeDecimal = 11
#
# 2. Add to GValue:
#    decimalVal: Optional[Decimal128] = None
#
# 3. Add to GValue.String() method:
#    case TypeDecimal:
#        return "decimal"
#
# 4. Add constructor method to GValue:
#    def Decimal(value: Decimal128) -> GValue:
#        return GValue(typ=TypeDecimal, decimalVal=value)
#
# 5. Add parsing in parse.py:
#    - Detect "123.45m" suffix
#    - Parse as Decimal128
#
# 6. Add emission in loose.py:
#    - Emit Decimal as "123.45m"
#    - Or use Decimal{scale=2 coef="..."}
#
# 7. Add JSON bridge:
#    - from_json: string -> Decimal128
#    - to_json: Decimal128 -> string


# Example GLYPH syntax:
# Decimal suffix: 123.45m
# Canonical form: 123.45m (not 123.450m)
# Struct form: Decimal{scale=2 coef=b64"..."}

# Example usage:
# result = glyph.parse('{price=99.99m balance=1000.50m}')
# data = glyph.emit({
#     'price': decimal('99.99'),
#     'balance': decimal('1000.50'),
# })
# # {balance=1000.50m price=99.99m}
