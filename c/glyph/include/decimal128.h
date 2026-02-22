/**
 * Decimal128 - High-precision decimal type for GLYPH
 *
 * A 128-bit decimal for financial, scientific, and precise mathematical calculations.
 * Value = coefficient * 10^(-scale) where scale is -127 to 127.
 *
 * Unlike float/double, Decimal128:
 * - Preserves exact decimal representation
 * - No precision loss for large numbers
 * - Safe for financial calculations
 * - Compatible with blockchain/crypto systems
 */

#ifndef GLYPH_DECIMAL128_H
#define GLYPH_DECIMAL128_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Types
 * ============================================================ */

/** Error codes for decimal operations */
typedef enum {
    DECIMAL_OK = 0,
    DECIMAL_ERR_SCALE_OVERFLOW,
    DECIMAL_ERR_DIVISION_BY_ZERO,
    DECIMAL_ERR_PARSE_FAILED,
    DECIMAL_ERR_OVERFLOW,
} decimal_error_t;

/**
 * Decimal128 represents a 128-bit decimal number.
 * Value = coefficient * 10^(-scale)
 *
 * The coefficient is stored as a sign bit and a 127-bit unsigned value
 * split across two 64-bit integers for portability.
 */
typedef struct {
    int8_t scale;       /* Exponent: -127 to 127 */
    bool negative;      /* Sign flag */
    uint64_t coef_high; /* High 64 bits of coefficient */
    uint64_t coef_low;  /* Low 64 bits of coefficient */
} decimal128_t;

/* ============================================================
 * Constructors
 * ============================================================ */

/** Create a zero decimal */
decimal128_t decimal128_zero(void);

/** Create a decimal from an int64 */
decimal128_t decimal128_from_int(int64_t value);

/** Create a decimal from a uint64 */
decimal128_t decimal128_from_uint(uint64_t value);

/**
 * Create a decimal from a string.
 * Examples: "123.45", "99.99", "-0.001", "123.45m"
 * Returns DECIMAL_OK on success, error code on failure.
 */
decimal_error_t decimal128_from_string(const char *s, decimal128_t *out);

/**
 * Create a decimal from a double (with potential precision loss).
 */
decimal128_t decimal128_from_double(double value);

/* ============================================================
 * Conversion
 * ============================================================ */

/**
 * Convert to int64 (truncates fractional part).
 * Returns 0 if the value doesn't fit in int64.
 */
int64_t decimal128_to_int(const decimal128_t *d);

/**
 * Convert to double (with potential precision loss).
 */
double decimal128_to_double(const decimal128_t *d);

/**
 * Convert to string. Returns malloc'd string.
 * Caller must free the result.
 */
char *decimal128_to_string(const decimal128_t *d);

/* ============================================================
 * Predicates
 * ============================================================ */

/** Check if value is zero */
bool decimal128_is_zero(const decimal128_t *d);

/** Check if value is negative */
bool decimal128_is_negative(const decimal128_t *d);

/** Check if value is positive */
bool decimal128_is_positive(const decimal128_t *d);

/* ============================================================
 * Unary Operations
 * ============================================================ */

/** Return the absolute value */
decimal128_t decimal128_abs(const decimal128_t *d);

/** Negate the value */
decimal128_t decimal128_negate(const decimal128_t *d);

/* ============================================================
 * Arithmetic Operations
 * ============================================================ */

/**
 * Add two decimals.
 * Returns error code; result in *out on success.
 */
decimal_error_t decimal128_add(const decimal128_t *a, const decimal128_t *b, decimal128_t *out);

/**
 * Subtract two decimals (a - b).
 * Returns error code; result in *out on success.
 */
decimal_error_t decimal128_sub(const decimal128_t *a, const decimal128_t *b, decimal128_t *out);

/**
 * Multiply two decimals.
 * Returns error code; result in *out on success.
 */
decimal_error_t decimal128_mul(const decimal128_t *a, const decimal128_t *b, decimal128_t *out);

/**
 * Divide two decimals (a / b).
 * Returns error code; result in *out on success.
 */
decimal_error_t decimal128_div(const decimal128_t *a, const decimal128_t *b, decimal128_t *out);

/* ============================================================
 * Comparison
 * ============================================================ */

/**
 * Compare two decimals.
 * Returns -1 if a < b, 0 if a == b, 1 if a > b.
 */
int decimal128_cmp(const decimal128_t *a, const decimal128_t *b);

/** Check equality */
bool decimal128_equals(const decimal128_t *a, const decimal128_t *b);

/** Less than */
bool decimal128_lt(const decimal128_t *a, const decimal128_t *b);

/** Greater than */
bool decimal128_gt(const decimal128_t *a, const decimal128_t *b);

/** Less than or equal */
bool decimal128_lte(const decimal128_t *a, const decimal128_t *b);

/** Greater than or equal */
bool decimal128_gte(const decimal128_t *a, const decimal128_t *b);

/* ============================================================
 * Utility Functions
 * ============================================================ */

/** Check if a string is a decimal literal (ends with 'm') */
bool decimal128_is_literal(const char *s);

/** Get error message for error code */
const char *decimal128_error_string(decimal_error_t err);

#ifdef __cplusplus
}
#endif

#endif /* GLYPH_DECIMAL128_H */
