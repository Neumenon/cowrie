/**
 * Decimal128 - C Implementation
 */

#include "decimal128.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>

/* ============================================================
 * Internal Helpers
 * ============================================================ */

static char *strdup_safe(const char *s) {
    if (!s) return NULL;
    size_t len = strlen(s);
    char *copy = malloc(len + 1);
    if (copy) memcpy(copy, s, len + 1);
    return copy;
}

/* 128-bit unsigned addition */
static void u128_add(uint64_t *high, uint64_t *low, uint64_t h2, uint64_t l2) {
    uint64_t new_low = *low + l2;
    uint64_t carry = (new_low < *low) ? 1 : 0;
    *low = new_low;
    *high = *high + h2 + carry;
}

/* 128-bit unsigned subtraction (assumes a >= b) */
static void u128_sub(uint64_t *high, uint64_t *low, uint64_t h2, uint64_t l2) {
    uint64_t borrow = (*low < l2) ? 1 : 0;
    *low = *low - l2;
    *high = *high - h2 - borrow;
}

/* Compare two 128-bit unsigned numbers */
static int u128_cmp(uint64_t h1, uint64_t l1, uint64_t h2, uint64_t l2) {
    if (h1 > h2) return 1;
    if (h1 < h2) return -1;
    if (l1 > l2) return 1;
    if (l1 < l2) return -1;
    return 0;
}

/* Check if 128-bit number is zero */
static bool u128_is_zero(uint64_t high, uint64_t low) {
    return high == 0 && low == 0;
}

/* Multiply 128-bit by 10 */
static void u128_mul10(uint64_t *high, uint64_t *low) {
    /* Split into 32-bit parts for overflow-safe multiplication */
    uint64_t l = *low;
    uint64_t h = *high;

    uint64_t new_low = l * 10;
    uint64_t overflow = 0;

    /* Check for overflow in low multiplication */
    if (l > UINT64_MAX / 10) {
        /* Calculate overflow using high part */
        overflow = (l >> 60) + ((l & 0x0FFFFFFFFFFFFFFFULL) * 10 >> 60);
    }

    *low = new_low;
    *high = h * 10 + overflow;
}

/* Divide 128-bit by 10, return remainder */
static uint64_t u128_div10(uint64_t *high, uint64_t *low) {
    uint64_t h = *high;
    uint64_t l = *low;

    uint64_t new_high = h / 10;
    uint64_t remainder = h % 10;

    /* Combine remainder with low part */
    uint64_t combined_high = (remainder << 60) | (l >> 4);
    uint64_t new_low = (combined_high / 10) << 4;
    remainder = combined_high % 10;

    combined_high = (remainder << 4) | (l & 0xF);
    new_low |= combined_high / 10;
    remainder = combined_high % 10;

    *high = new_high;
    *low = new_low;
    return remainder;
}

/* ============================================================
 * Constructors
 * ============================================================ */

decimal128_t decimal128_zero(void) {
    decimal128_t d = {0, false, 0, 0};
    return d;
}

decimal128_t decimal128_from_int(int64_t value) {
    decimal128_t d;
    d.scale = 0;
    if (value < 0) {
        d.negative = true;
        d.coef_high = 0;
        d.coef_low = (uint64_t)(-value);
    } else {
        d.negative = false;
        d.coef_high = 0;
        d.coef_low = (uint64_t)value;
    }
    return d;
}

decimal128_t decimal128_from_uint(uint64_t value) {
    decimal128_t d = {0, false, 0, value};
    return d;
}

decimal_error_t decimal128_from_string(const char *s, decimal128_t *out) {
    if (!s || !out) return DECIMAL_ERR_PARSE_FAILED;

    /* Skip whitespace */
    while (isspace(*s)) s++;

    /* Remove 'm' suffix if present */
    size_t len = strlen(s);
    char *buf = strdup_safe(s);
    if (!buf) return DECIMAL_ERR_PARSE_FAILED;

    if (len > 0 && buf[len-1] == 'm') {
        buf[len-1] = '\0';
        len--;
    }

    /* Parse sign */
    bool negative = false;
    char *p = buf;
    if (*p == '-') {
        negative = true;
        p++;
    } else if (*p == '+') {
        p++;
    }

    /* Find decimal point */
    char *dot = strchr(p, '.');
    int scale = 0;

    if (dot) {
        scale = strlen(dot + 1);
        /* Remove decimal point by shifting */
        memmove(dot, dot + 1, strlen(dot));
    }

    if (scale > 127) {
        free(buf);
        return DECIMAL_ERR_SCALE_OVERFLOW;
    }

    /* Parse coefficient */
    uint64_t coef_high = 0, coef_low = 0;
    for (char *c = p; *c; c++) {
        if (!isdigit(*c)) {
            free(buf);
            return DECIMAL_ERR_PARSE_FAILED;
        }
        u128_mul10(&coef_high, &coef_low);
        u128_add(&coef_high, &coef_low, 0, *c - '0');
    }

    out->scale = scale;
    out->negative = negative && !u128_is_zero(coef_high, coef_low);
    out->coef_high = coef_high;
    out->coef_low = coef_low;

    free(buf);
    return DECIMAL_OK;
}

decimal128_t decimal128_from_double(double value) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%.15g", value);
    decimal128_t d;
    if (decimal128_from_string(buf, &d) != DECIMAL_OK) {
        return decimal128_zero();
    }
    return d;
}

/* ============================================================
 * Conversion
 * ============================================================ */

int64_t decimal128_to_int(const decimal128_t *d) {
    if (!d) return 0;

    uint64_t h = d->coef_high;
    uint64_t l = d->coef_low;

    /* Divide by 10^scale */
    for (int i = 0; i < d->scale; i++) {
        u128_div10(&h, &l);
    }

    /* Check if fits in int64 */
    if (h != 0 || l > INT64_MAX) {
        return d->negative ? INT64_MIN : INT64_MAX;
    }

    return d->negative ? -(int64_t)l : (int64_t)l;
}

double decimal128_to_double(const decimal128_t *d) {
    if (!d) return 0.0;

    double result = (double)d->coef_high * 18446744073709551616.0 + (double)d->coef_low;
    for (int i = 0; i < d->scale; i++) {
        result /= 10.0;
    }

    return d->negative ? -result : result;
}

char *decimal128_to_string(const decimal128_t *d) {
    if (!d) return strdup_safe("0");

    /* Build digit string by repeated division */
    char digits[64];
    int pos = 63;
    digits[pos] = '\0';

    uint64_t h = d->coef_high;
    uint64_t l = d->coef_low;

    if (u128_is_zero(h, l)) {
        if (d->scale == 0) {
            return strdup_safe("0");
        }
        /* Build 0.000... */
        char *result = malloc(d->scale + 3);
        result[0] = '0';
        result[1] = '.';
        for (int i = 0; i < d->scale; i++) {
            result[2 + i] = '0';
        }
        result[2 + d->scale] = '\0';
        return result;
    }

    while (!u128_is_zero(h, l)) {
        uint64_t rem = u128_div10(&h, &l);
        digits[--pos] = '0' + rem;
    }

    int digit_count = 63 - pos;
    char *digit_str = digits + pos;

    /* No decimal point needed */
    if (d->scale == 0) {
        size_t result_len = digit_count + (d->negative ? 1 : 0) + 1;
        char *result = malloc(result_len);
        if (d->negative) {
            result[0] = '-';
            strcpy(result + 1, digit_str);
        } else {
            strcpy(result, digit_str);
        }
        return result;
    }

    /* Need decimal point */
    int int_digits = digit_count - d->scale;
    if (int_digits <= 0) {
        /* 0.00...XXX */
        size_t result_len = 2 + (-int_digits) + digit_count + (d->negative ? 1 : 0) + 1;
        char *result = malloc(result_len);
        char *p = result;
        if (d->negative) *p++ = '-';
        *p++ = '0';
        *p++ = '.';
        for (int i = 0; i < -int_digits; i++) {
            *p++ = '0';
        }
        strcpy(p, digit_str);
        return result;
    }

    /* Normal case: XXX.YYY */
    size_t result_len = digit_count + 1 + (d->negative ? 1 : 0) + 1;
    char *result = malloc(result_len);
    char *p = result;
    if (d->negative) *p++ = '-';
    memcpy(p, digit_str, int_digits);
    p += int_digits;
    *p++ = '.';
    strcpy(p, digit_str + int_digits);
    return result;
}

/* ============================================================
 * Predicates
 * ============================================================ */

bool decimal128_is_zero(const decimal128_t *d) {
    return d && u128_is_zero(d->coef_high, d->coef_low);
}

bool decimal128_is_negative(const decimal128_t *d) {
    return d && d->negative && !u128_is_zero(d->coef_high, d->coef_low);
}

bool decimal128_is_positive(const decimal128_t *d) {
    return d && !d->negative && !u128_is_zero(d->coef_high, d->coef_low);
}

/* ============================================================
 * Unary Operations
 * ============================================================ */

decimal128_t decimal128_abs(const decimal128_t *d) {
    if (!d) return decimal128_zero();
    decimal128_t result = *d;
    result.negative = false;
    return result;
}

decimal128_t decimal128_negate(const decimal128_t *d) {
    if (!d) return decimal128_zero();
    decimal128_t result = *d;
    if (!u128_is_zero(d->coef_high, d->coef_low)) {
        result.negative = !d->negative;
    }
    return result;
}

/* ============================================================
 * Arithmetic Operations
 * ============================================================ */

decimal_error_t decimal128_add(const decimal128_t *a, const decimal128_t *b, decimal128_t *out) {
    if (!a || !b || !out) return DECIMAL_ERR_PARSE_FAILED;

    /* Align scales */
    uint64_t ah = a->coef_high, al = a->coef_low;
    uint64_t bh = b->coef_high, bl = b->coef_low;
    int target_scale;

    if (a->scale < b->scale) {
        int diff = b->scale - a->scale;
        for (int i = 0; i < diff; i++) {
            u128_mul10(&ah, &al);
        }
        target_scale = b->scale;
    } else {
        int diff = a->scale - b->scale;
        for (int i = 0; i < diff; i++) {
            u128_mul10(&bh, &bl);
        }
        target_scale = a->scale;
    }

    if (target_scale > 127 || target_scale < -127) {
        return DECIMAL_ERR_SCALE_OVERFLOW;
    }

    /* Add or subtract based on signs */
    if (a->negative == b->negative) {
        /* Same sign: add magnitudes */
        u128_add(&ah, &al, bh, bl);
        out->scale = target_scale;
        out->negative = a->negative;
        out->coef_high = ah;
        out->coef_low = al;
    } else {
        /* Different signs: subtract smaller from larger */
        int cmp = u128_cmp(ah, al, bh, bl);
        if (cmp >= 0) {
            u128_sub(&ah, &al, bh, bl);
            out->scale = target_scale;
            out->negative = a->negative && !u128_is_zero(ah, al);
            out->coef_high = ah;
            out->coef_low = al;
        } else {
            u128_sub(&bh, &bl, ah, al);
            out->scale = target_scale;
            out->negative = b->negative && !u128_is_zero(bh, bl);
            out->coef_high = bh;
            out->coef_low = bl;
        }
    }

    return DECIMAL_OK;
}

decimal_error_t decimal128_sub(const decimal128_t *a, const decimal128_t *b, decimal128_t *out) {
    if (!b) return DECIMAL_ERR_PARSE_FAILED;
    decimal128_t neg_b = decimal128_negate(b);
    return decimal128_add(a, &neg_b, out);
}

decimal_error_t decimal128_mul(const decimal128_t *a, const decimal128_t *b, decimal128_t *out) {
    if (!a || !b || !out) return DECIMAL_ERR_PARSE_FAILED;

    int new_scale = a->scale + b->scale;
    if (new_scale > 127 || new_scale < -127) {
        return DECIMAL_ERR_SCALE_OVERFLOW;
    }

    /* Simple multiplication using 64-bit parts */
    /* For full 128x128 bit multiplication, we'd need more complex logic */
    /* This is a simplified version that works for most practical cases */
    uint64_t al = a->coef_low;
    uint64_t bl = b->coef_low;

    /* Multiply low parts */
    __uint128_t product = (__uint128_t)al * bl;

    out->scale = new_scale;
    out->negative = a->negative != b->negative && product != 0;
    out->coef_high = (uint64_t)(product >> 64);
    out->coef_low = (uint64_t)product;

    return DECIMAL_OK;
}

decimal_error_t decimal128_div(const decimal128_t *a, const decimal128_t *b, decimal128_t *out) {
    if (!a || !b || !out) return DECIMAL_ERR_PARSE_FAILED;

    if (u128_is_zero(b->coef_high, b->coef_low)) {
        return DECIMAL_ERR_DIVISION_BY_ZERO;
    }

    int new_scale = a->scale - b->scale;
    if (new_scale > 127 || new_scale < -127) {
        return DECIMAL_ERR_SCALE_OVERFLOW;
    }

    /* Simple integer division (truncates) */
    uint64_t al = a->coef_low;
    uint64_t bl = b->coef_low;

    if (bl == 0) {
        return DECIMAL_ERR_DIVISION_BY_ZERO;
    }

    out->scale = new_scale;
    out->negative = a->negative != b->negative && al / bl != 0;
    out->coef_high = 0;
    out->coef_low = al / bl;

    return DECIMAL_OK;
}

/* ============================================================
 * Comparison
 * ============================================================ */

int decimal128_cmp(const decimal128_t *a, const decimal128_t *b) {
    if (!a && !b) return 0;
    if (!a) return -1;
    if (!b) return 1;

    /* Handle signs */
    bool a_neg = a->negative && !u128_is_zero(a->coef_high, a->coef_low);
    bool b_neg = b->negative && !u128_is_zero(b->coef_high, b->coef_low);

    if (a_neg && !b_neg) return -1;
    if (!a_neg && b_neg) return 1;

    /* Align scales */
    uint64_t ah = a->coef_high, al = a->coef_low;
    uint64_t bh = b->coef_high, bl = b->coef_low;

    if (a->scale < b->scale) {
        int diff = b->scale - a->scale;
        for (int i = 0; i < diff; i++) {
            u128_mul10(&ah, &al);
        }
    } else if (a->scale > b->scale) {
        int diff = a->scale - b->scale;
        for (int i = 0; i < diff; i++) {
            u128_mul10(&bh, &bl);
        }
    }

    int cmp = u128_cmp(ah, al, bh, bl);
    return a_neg ? -cmp : cmp;
}

bool decimal128_equals(const decimal128_t *a, const decimal128_t *b) {
    return decimal128_cmp(a, b) == 0;
}

bool decimal128_lt(const decimal128_t *a, const decimal128_t *b) {
    return decimal128_cmp(a, b) < 0;
}

bool decimal128_gt(const decimal128_t *a, const decimal128_t *b) {
    return decimal128_cmp(a, b) > 0;
}

bool decimal128_lte(const decimal128_t *a, const decimal128_t *b) {
    return decimal128_cmp(a, b) <= 0;
}

bool decimal128_gte(const decimal128_t *a, const decimal128_t *b) {
    return decimal128_cmp(a, b) >= 0;
}

/* ============================================================
 * Utility Functions
 * ============================================================ */

bool decimal128_is_literal(const char *s) {
    if (!s) return false;
    size_t len = strlen(s);
    if (len < 2) return false;
    if (s[len-1] != 'm') return false;

    decimal128_t d;
    return decimal128_from_string(s, &d) == DECIMAL_OK;
}

const char *decimal128_error_string(decimal_error_t err) {
    switch (err) {
        case DECIMAL_OK: return "OK";
        case DECIMAL_ERR_SCALE_OVERFLOW: return "scale overflow";
        case DECIMAL_ERR_DIVISION_BY_ZERO: return "division by zero";
        case DECIMAL_ERR_PARSE_FAILED: return "parse failed";
        case DECIMAL_ERR_OVERFLOW: return "overflow";
        default: return "unknown error";
    }
}
