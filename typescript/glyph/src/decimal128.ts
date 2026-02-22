/**
 * Decimal128 - High-precision decimal type for GLYPH
 *
 * A 128-bit decimal for financial, scientific, and precise mathematical calculations.
 * Value = coefficient * 10^(-scale) where scale is -127 to 127.
 *
 * Unlike JavaScript numbers (float64), Decimal128:
 * - Preserves exact decimal representation
 * - No precision loss for large numbers (>2^53)
 * - Safe for financial calculations
 * - Compatible with blockchain/crypto systems
 */

export class DecimalError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DecimalError';
  }
}

/**
 * Decimal128 represents a 128-bit decimal number.
 * Value = coefficient * 10^(-scale)
 */
export class Decimal128 {
  /** Exponent: -127 to 127 */
  readonly scale: number;
  /** 128-bit coefficient as BigInt */
  readonly coef: bigint;

  constructor(scale: number, coef: bigint) {
    if (scale < -127 || scale > 127) {
      throw new DecimalError(`scale must be -127 to 127, got ${scale}`);
    }
    this.scale = scale;
    this.coef = coef;
  }

  /**
   * Create a Decimal128 from an integer.
   */
  static fromInt(value: number | bigint): Decimal128 {
    return new Decimal128(0, BigInt(value));
  }

  /**
   * Create a Decimal128 from a string.
   * Examples: "123.45", "99.99", "-0.001"
   */
  static fromString(s: string): Decimal128 {
    s = s.trim();

    // Remove 'm' suffix if present
    if (s.endsWith('m')) {
      s = s.slice(0, -1);
    }

    const negative = s.startsWith('-');
    if (negative) {
      s = s.slice(1);
    }

    const parts = s.split('.');
    if (parts.length > 2) {
      throw new DecimalError(`invalid decimal format: ${s}`);
    }

    let scale = 0;
    let coefStr: string;

    if (parts.length === 2) {
      const intPart = parts[0] || '0';
      const fracPart = parts[1];
      scale = fracPart.length;
      coefStr = intPart + fracPart;
    } else {
      coefStr = parts[0];
    }

    if (scale > 127) {
      throw new DecimalError(`scale too large: ${scale}`);
    }

    let coef = BigInt(coefStr);
    if (negative) {
      coef = -coef;
    }

    return new Decimal128(scale, coef);
  }

  /**
   * Create a Decimal128 from a number (with potential precision loss).
   */
  static fromNumber(n: number): Decimal128 {
    return Decimal128.fromString(n.toString());
  }

  /**
   * Convert to integer (truncates fractional part).
   */
  toInt(): bigint {
    const divisor = 10n ** BigInt(this.scale);
    return this.coef / divisor;
  }

  /**
   * Convert to number (with potential precision loss).
   */
  toNumber(): number {
    const divisor = 10 ** this.scale;
    return Number(this.coef) / divisor;
  }

  /**
   * Convert to string.
   */
  toString(): string {
    if (this.scale === 0) {
      return this.coef.toString();
    }

    const negative = this.coef < 0n;
    let coefStr = (negative ? -this.coef : this.coef).toString();

    // Pad with zeros if needed
    while (coefStr.length <= this.scale) {
      coefStr = '0' + coefStr;
    }

    const insertPos = coefStr.length - this.scale;
    const result = coefStr.slice(0, insertPos) + '.' + coefStr.slice(insertPos);

    return negative ? '-' + result : result;
  }

  /**
   * Check if value is zero.
   */
  isZero(): boolean {
    return this.coef === 0n;
  }

  /**
   * Check if value is negative.
   */
  isNegative(): boolean {
    return this.coef < 0n;
  }

  /**
   * Check if value is positive.
   */
  isPositive(): boolean {
    return this.coef > 0n;
  }

  /**
   * Return the absolute value.
   */
  abs(): Decimal128 {
    return new Decimal128(this.scale, this.coef < 0n ? -this.coef : this.coef);
  }

  /**
   * Negate the value.
   */
  negate(): Decimal128 {
    return new Decimal128(this.scale, -this.coef);
  }

  /**
   * Add two decimals.
   */
  add(other: Decimal128): Decimal128 {
    let c1 = this.coef;
    let c2 = other.coef;
    let targetScale: number;

    if (this.scale < other.scale) {
      const diff = other.scale - this.scale;
      c1 = c1 * (10n ** BigInt(diff));
      targetScale = other.scale;
    } else {
      const diff = this.scale - other.scale;
      c2 = c2 * (10n ** BigInt(diff));
      targetScale = this.scale;
    }

    return new Decimal128(targetScale, c1 + c2);
  }

  /**
   * Subtract two decimals.
   */
  sub(other: Decimal128): Decimal128 {
    return this.add(other.negate());
  }

  /**
   * Multiply two decimals.
   */
  mul(other: Decimal128): Decimal128 {
    const result = this.coef * other.coef;
    const newScale = this.scale + other.scale;

    if (newScale > 127 || newScale < -127) {
      throw new DecimalError('scale overflow');
    }

    return new Decimal128(newScale, result);
  }

  /**
   * Divide two decimals.
   */
  div(other: Decimal128): Decimal128 {
    if (other.coef === 0n) {
      throw new DecimalError('division by zero');
    }

    const result = this.coef / other.coef;
    const newScale = this.scale - other.scale;

    if (newScale > 127 || newScale < -127) {
      throw new DecimalError('scale overflow');
    }

    return new Decimal128(newScale, result);
  }

  /**
   * Compare two decimals.
   * Returns -1 if this < other, 0 if equal, 1 if this > other.
   */
  cmp(other: Decimal128): number {
    let c1 = this.coef;
    let c2 = other.coef;

    if (this.scale < other.scale) {
      const diff = other.scale - this.scale;
      c1 = c1 * (10n ** BigInt(diff));
    } else if (this.scale > other.scale) {
      const diff = this.scale - other.scale;
      c2 = c2 * (10n ** BigInt(diff));
    }

    if (c1 < c2) return -1;
    if (c1 > c2) return 1;
    return 0;
  }

  /**
   * Check equality.
   */
  equals(other: Decimal128): boolean {
    return this.cmp(other) === 0;
  }

  /**
   * Less than comparison.
   */
  lt(other: Decimal128): boolean {
    return this.cmp(other) < 0;
  }

  /**
   * Greater than comparison.
   */
  gt(other: Decimal128): boolean {
    return this.cmp(other) > 0;
  }

  /**
   * Less than or equal comparison.
   */
  lte(other: Decimal128): boolean {
    return this.cmp(other) <= 0;
  }

  /**
   * Greater than or equal comparison.
   */
  gte(other: Decimal128): boolean {
    return this.cmp(other) >= 0;
  }
}

/**
 * Check if a string is a decimal literal (ends with 'm').
 */
export function isDecimalLiteral(s: string): boolean {
  s = s.trim();
  if (!s.endsWith('m')) {
    return false;
  }
  try {
    Decimal128.fromString(s.slice(0, -1));
    return true;
  } catch {
    return false;
  }
}

/**
 * Parse a decimal literal (with 'm' suffix).
 */
export function parseDecimalLiteral(s: string): Decimal128 {
  s = s.trim();
  if (!s.endsWith('m')) {
    throw new DecimalError('not a decimal literal');
  }
  return Decimal128.fromString(s.slice(0, -1));
}

/**
 * Convenience function to create a Decimal128.
 */
export function decimal(value: string | number | bigint): Decimal128 {
  if (typeof value === 'string') {
    return Decimal128.fromString(value);
  }
  if (typeof value === 'bigint') {
    return Decimal128.fromInt(value);
  }
  return Decimal128.fromNumber(value);
}
