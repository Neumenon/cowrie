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
export declare class DecimalError extends Error {
    constructor(message: string);
}
/**
 * Decimal128 represents a 128-bit decimal number.
 * Value = coefficient * 10^(-scale)
 */
export declare class Decimal128 {
    /** Exponent: -127 to 127 */
    readonly scale: number;
    /** 128-bit coefficient as BigInt */
    readonly coef: bigint;
    constructor(scale: number, coef: bigint);
    /**
     * Create a Decimal128 from an integer.
     */
    static fromInt(value: number | bigint): Decimal128;
    /**
     * Create a Decimal128 from a string.
     * Examples: "123.45", "99.99", "-0.001"
     */
    static fromString(s: string): Decimal128;
    /**
     * Create a Decimal128 from a number (with potential precision loss).
     */
    static fromNumber(n: number): Decimal128;
    /**
     * Convert to integer (truncates fractional part).
     */
    toInt(): bigint;
    /**
     * Convert to number (with potential precision loss).
     */
    toNumber(): number;
    /**
     * Convert to string.
     */
    toString(): string;
    /**
     * Check if value is zero.
     */
    isZero(): boolean;
    /**
     * Check if value is negative.
     */
    isNegative(): boolean;
    /**
     * Check if value is positive.
     */
    isPositive(): boolean;
    /**
     * Return the absolute value.
     */
    abs(): Decimal128;
    /**
     * Negate the value.
     */
    negate(): Decimal128;
    /**
     * Add two decimals.
     */
    add(other: Decimal128): Decimal128;
    /**
     * Subtract two decimals.
     */
    sub(other: Decimal128): Decimal128;
    /**
     * Multiply two decimals.
     */
    mul(other: Decimal128): Decimal128;
    /**
     * Divide two decimals.
     */
    div(other: Decimal128): Decimal128;
    /**
     * Compare two decimals.
     * Returns -1 if this < other, 0 if equal, 1 if this > other.
     */
    cmp(other: Decimal128): number;
    /**
     * Check equality.
     */
    equals(other: Decimal128): boolean;
    /**
     * Less than comparison.
     */
    lt(other: Decimal128): boolean;
    /**
     * Greater than comparison.
     */
    gt(other: Decimal128): boolean;
    /**
     * Less than or equal comparison.
     */
    lte(other: Decimal128): boolean;
    /**
     * Greater than or equal comparison.
     */
    gte(other: Decimal128): boolean;
}
/**
 * Check if a string is a decimal literal (ends with 'm').
 */
export declare function isDecimalLiteral(s: string): boolean;
/**
 * Parse a decimal literal (with 'm' suffix).
 */
export declare function parseDecimalLiteral(s: string): Decimal128;
/**
 * Convenience function to create a Decimal128.
 */
export declare function decimal(value: string | number | bigint): Decimal128;
//# sourceMappingURL=decimal128.d.ts.map