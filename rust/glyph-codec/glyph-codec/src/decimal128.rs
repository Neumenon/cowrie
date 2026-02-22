//! Decimal128 - High-precision decimal type for GLYPH
//!
//! A 128-bit decimal for financial, scientific, and precise mathematical calculations.
//! Value = coefficient * 10^(-scale) where scale is -127 to 127.
//!
//! Unlike f64, Decimal128:
//! - Preserves exact decimal representation
//! - No precision loss for large numbers (>2^53)
//! - Safe for financial calculations
//! - Compatible with blockchain/crypto systems

use std::cmp::Ordering;
use std::fmt;
use std::ops::{Add, Sub, Mul, Div, Neg};
use std::str::FromStr;

/// Decimal128 represents a 128-bit decimal number.
/// Value = coefficient * 10^(-scale)
#[derive(Clone, Copy, Eq)]
pub struct Decimal128 {
    /// Exponent: -127 to 127
    pub scale: i8,
    /// 128-bit coefficient (two's complement, big-endian)
    pub coef: [u8; 16],
}

impl Decimal128 {
    /// Create a new Decimal128 from scale and coefficient bytes.
    pub fn new(scale: i8, coef: [u8; 16]) -> Self {
        Self { scale, coef }
    }

    /// Create a Decimal128 from an i64.
    pub fn from_i64(value: i64) -> Self {
        let mut coef = [0u8; 16];
        let bytes = value.to_be_bytes();

        // Sign extend for negative numbers
        let fill = if value < 0 { 0xFF } else { 0x00 };
        for i in 0..8 {
            coef[i] = fill;
        }
        coef[8..16].copy_from_slice(&bytes);

        Self { scale: 0, coef }
    }

    /// Create a Decimal128 from a string representation.
    pub fn from_string(s: &str) -> Result<Self, DecimalError> {
        let s = s.trim();

        // Remove 'm' suffix if present
        let s = s.strip_suffix('m').unwrap_or(s);

        // Check for negative
        let (negative, s) = if s.starts_with('-') {
            (true, &s[1..])
        } else {
            (false, s)
        };

        // Split by decimal point
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() > 2 {
            return Err(DecimalError::InvalidFormat);
        }

        let (int_part, frac_part, scale_len) = if parts.len() == 2 {
            (parts[0], parts[1], parts[1].len())
        } else {
            (parts[0], "", 0usize)
        };

        if scale_len > 127 {
            return Err(DecimalError::ScaleOverflow);
        }
        let scale = scale_len as i8;

        // Parse coefficient
        let coef_str = format!("{}{}", int_part, frac_part);
        let mut coef_value = i128::from_str(&coef_str)
            .map_err(|_| DecimalError::InvalidFormat)?;

        if negative {
            coef_value = -coef_value;
        }

        Ok(Self {
            scale,
            coef: int_to_coef(coef_value),
        })
    }

    /// Create a Decimal128 from an f64 (with potential precision loss).
    pub fn from_f64(f: f64) -> Result<Self, DecimalError> {
        Self::from_string(&format!("{}", f))
    }

    /// Convert to i64 (truncates fractional part).
    pub fn to_i64(&self) -> i64 {
        let coef = coef_to_int(&self.coef);
        let divisor = 10i128.pow(self.scale as u32);
        (coef / divisor) as i64
    }

    /// Convert to f64 (with potential precision loss).
    pub fn to_f64(&self) -> f64 {
        let coef = coef_to_int(&self.coef);
        let divisor = 10f64.powi(self.scale as i32);
        (coef as f64) / divisor
    }

    /// Check if value is zero.
    pub fn is_zero(&self) -> bool {
        coef_to_int(&self.coef) == 0
    }

    /// Check if value is negative.
    pub fn is_negative(&self) -> bool {
        coef_to_int(&self.coef) < 0
    }

    /// Check if value is positive.
    pub fn is_positive(&self) -> bool {
        coef_to_int(&self.coef) > 0
    }

    /// Return the absolute value.
    pub fn abs(&self) -> Self {
        let coef = coef_to_int(&self.coef);
        Self {
            scale: self.scale,
            coef: int_to_coef(coef.abs()),
        }
    }

    /// Negate the value.
    pub fn negate(&self) -> Self {
        let coef = coef_to_int(&self.coef);
        Self {
            scale: self.scale,
            coef: int_to_coef(-coef),
        }
    }
}

impl fmt::Display for Decimal128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let coef = coef_to_int(&self.coef);

        if self.scale == 0 {
            return write!(f, "{}", coef);
        }

        let negative = coef < 0;
        let mut coef_str = coef.abs().to_string();

        // Pad with zeros if needed
        while coef_str.len() <= self.scale as usize {
            coef_str.insert(0, '0');
        }

        let insert_pos = coef_str.len() - self.scale as usize;
        coef_str.insert(insert_pos, '.');

        if negative {
            write!(f, "-{}", coef_str)
        } else {
            write!(f, "{}", coef_str)
        }
    }
}

impl fmt::Debug for Decimal128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Decimal128(scale={}, value={})", self.scale, self)
    }
}

impl PartialEq for Decimal128 {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for Decimal128 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Decimal128 {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut c1 = coef_to_int(&self.coef);
        let mut c2 = coef_to_int(&other.coef);

        // Align scales
        if self.scale < other.scale {
            let diff = (other.scale - self.scale) as u32;
            c1 *= 10i128.pow(diff);
        } else if self.scale > other.scale {
            let diff = (self.scale - other.scale) as u32;
            c2 *= 10i128.pow(diff);
        }

        c1.cmp(&c2)
    }
}

impl Add for Decimal128 {
    type Output = Result<Self, DecimalError>;

    fn add(self, other: Self) -> Self::Output {
        let mut c1 = coef_to_int(&self.coef);
        let mut c2 = coef_to_int(&other.coef);
        let target_scale;

        if self.scale < other.scale {
            let diff = (other.scale - self.scale) as u32;
            c1 *= 10i128.pow(diff);
            target_scale = other.scale;
        } else {
            let diff = (self.scale - other.scale) as u32;
            c2 *= 10i128.pow(diff);
            target_scale = self.scale;
        }

        let result = c1 + c2;

        // Check overflow (128-bit signed max)
        if result.leading_zeros() < 1 && result.leading_ones() < 1 {
            return Err(DecimalError::Overflow);
        }

        Ok(Self {
            scale: target_scale,
            coef: int_to_coef(result),
        })
    }
}

impl Sub for Decimal128 {
    type Output = Result<Self, DecimalError>;

    fn sub(self, other: Self) -> Self::Output {
        self + other.negate()
    }
}

impl Mul for Decimal128 {
    type Output = Result<Self, DecimalError>;

    fn mul(self, other: Self) -> Self::Output {
        let c1 = coef_to_int(&self.coef);
        let c2 = coef_to_int(&other.coef);
        let result = c1 * c2;

        let new_scale = self.scale as i16 + other.scale as i16;
        if new_scale > 127 || new_scale < -127 {
            return Err(DecimalError::ScaleOverflow);
        }

        Ok(Self {
            scale: new_scale as i8,
            coef: int_to_coef(result),
        })
    }
}

impl Div for Decimal128 {
    type Output = Result<Self, DecimalError>;

    fn div(self, other: Self) -> Self::Output {
        let c2 = coef_to_int(&other.coef);
        if c2 == 0 {
            return Err(DecimalError::DivisionByZero);
        }

        let c1 = coef_to_int(&self.coef);
        let result = c1 / c2;

        let new_scale = self.scale as i16 - other.scale as i16;
        if new_scale > 127 || new_scale < -127 {
            return Err(DecimalError::ScaleOverflow);
        }

        Ok(Self {
            scale: new_scale as i8,
            coef: int_to_coef(result),
        })
    }
}

impl Neg for Decimal128 {
    type Output = Self;

    fn neg(self) -> Self::Output {
        self.negate()
    }
}

impl FromStr for Decimal128 {
    type Err = DecimalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_string(s)
    }
}

// ============================================================
// Error Type
// ============================================================

/// Decimal128 error types.
#[derive(Debug, Clone, PartialEq)]
pub enum DecimalError {
    InvalidFormat,
    Overflow,
    ScaleOverflow,
    DivisionByZero,
}

impl fmt::Display for DecimalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecimalError::InvalidFormat => write!(f, "invalid decimal format"),
            DecimalError::Overflow => write!(f, "decimal overflow"),
            DecimalError::ScaleOverflow => write!(f, "scale overflow"),
            DecimalError::DivisionByZero => write!(f, "division by zero"),
        }
    }
}

impl std::error::Error for DecimalError {}

// ============================================================
// Helper Functions
// ============================================================

/// Convert i128 to 16-byte two's complement representation.
fn int_to_coef(value: i128) -> [u8; 16] {
    value.to_be_bytes()
}

/// Convert 16-byte two's complement to i128.
fn coef_to_int(coef: &[u8; 16]) -> i128 {
    i128::from_be_bytes(*coef)
}

/// Check if a string is a decimal literal (ends with 'm').
pub fn is_decimal_literal(s: &str) -> bool {
    let s = s.trim();
    if !s.ends_with('m') {
        return false;
    }
    let num_part = &s[..s.len()-1];
    Decimal128::from_string(num_part).is_ok()
}

/// Parse a decimal literal (with 'm' suffix).
pub fn parse_decimal_literal(s: &str) -> Result<Decimal128, DecimalError> {
    let s = s.trim();
    if !s.ends_with('m') {
        return Err(DecimalError::InvalidFormat);
    }
    Decimal128::from_string(&s[..s.len()-1])
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_i64() {
        let d = Decimal128::from_i64(123);
        assert_eq!(d.to_string(), "123");
        assert_eq!(d.to_i64(), 123);

        let d = Decimal128::from_i64(-456);
        assert_eq!(d.to_string(), "-456");
        assert_eq!(d.to_i64(), -456);
    }

    #[test]
    fn test_from_string() {
        let d = Decimal128::from_string("123.45").unwrap();
        assert_eq!(d.to_string(), "123.45");
        assert_eq!(d.scale, 2);

        let d = Decimal128::from_string("-99.99").unwrap();
        assert_eq!(d.to_string(), "-99.99");

        let d = Decimal128::from_string("0.0001").unwrap();
        assert_eq!(d.to_string(), "0.0001");
    }

    #[test]
    fn test_arithmetic() {
        let d1 = Decimal128::from_string("100.50").unwrap();
        let d2 = Decimal128::from_string("50.25").unwrap();

        let sum = (d1 + d2).unwrap();
        assert_eq!(sum.to_string(), "150.75");

        let diff = (d1 - d2).unwrap();
        assert_eq!(diff.to_string(), "50.25");
    }

    #[test]
    fn test_comparison() {
        let d1 = Decimal128::from_string("100.50").unwrap();
        let d2 = Decimal128::from_string("100.50").unwrap();
        let d3 = Decimal128::from_string("50.25").unwrap();

        assert_eq!(d1, d2);
        assert!(d1 > d3);
        assert!(d3 < d1);
    }

    #[test]
    fn test_predicates() {
        let zero = Decimal128::from_i64(0);
        assert!(zero.is_zero());

        let neg = Decimal128::from_string("-10.5").unwrap();
        assert!(neg.is_negative());

        let pos = Decimal128::from_string("10.5").unwrap();
        assert!(pos.is_positive());
    }

    #[test]
    fn test_decimal_literal() {
        assert!(is_decimal_literal("99.99m"));
        assert!(is_decimal_literal("100m"));
        assert!(!is_decimal_literal("99.99"));

        let d = parse_decimal_literal("99.99m").unwrap();
        assert_eq!(d.to_string(), "99.99");
    }
}
