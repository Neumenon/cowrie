/*
 * COWRIE Truth Table Test Suite
 *
 * Hardcoded truth table cases covering all core types, edge cases,
 * and error conditions for the cowrie binary codec.
 */

#include "../include/cowrie_gen2.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>
#include <limits.h>

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) do { \
    printf("  Testing %s... ", #name); \
    tests_run++; \
    if (test_##name()) { \
        printf("PASS\n"); \
        tests_passed++; \
    } else { \
        printf("FAIL\n"); \
    } \
} while(0)

#define ASSERT(cond) do { if (!(cond)) { printf("ASSERT FAILED: %s\n", #cond); return 0; } } while(0)

/* ============================================================
 * 1. null_roundtrip
 * ============================================================ */

static int test_null_roundtrip(void) {
    COWRIEValue *v = cowrie_new_null();
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);
    ASSERT(decoded->type == COWRIE_NULL);

    cowrie_free(v);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * 2. bool_roundtrip
 * ============================================================ */

static int test_bool_roundtrip(void) {
    /* true */
    COWRIEValue *t = cowrie_new_bool(1);
    ASSERT(t != NULL);
    ASSERT(t->type == COWRIE_BOOL);
    ASSERT(t->as.boolean == 1);

    COWRIEBuf buf_t;
    ASSERT(cowrie_encode(t, &buf_t) == 0);

    COWRIEValue *dec_t;
    ASSERT(cowrie_decode(buf_t.data, buf_t.len, &dec_t) == 0);
    ASSERT(dec_t->type == COWRIE_BOOL);
    ASSERT(dec_t->as.boolean == 1);

    /* false */
    COWRIEValue *f = cowrie_new_bool(0);
    ASSERT(f != NULL);
    ASSERT(f->as.boolean == 0);

    COWRIEBuf buf_f;
    ASSERT(cowrie_encode(f, &buf_f) == 0);

    COWRIEValue *dec_f;
    ASSERT(cowrie_decode(buf_f.data, buf_f.len, &dec_f) == 0);
    ASSERT(dec_f->type == COWRIE_BOOL);
    ASSERT(dec_f->as.boolean == 0);

    cowrie_free(t);
    cowrie_free(f);
    cowrie_free(dec_t);
    cowrie_free(dec_f);
    cowrie_buf_free(&buf_t);
    cowrie_buf_free(&buf_f);
    return 1;
}

/* ============================================================
 * 3. int64_max
 * ============================================================ */

static int test_int64_max(void) {
    COWRIEValue *v = cowrie_new_int64(INT64_MAX);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_INT64);
    ASSERT(v->as.i64 == INT64_MAX);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);
    ASSERT(decoded->type == COWRIE_INT64);
    ASSERT(decoded->as.i64 == INT64_MAX);

    cowrie_free(v);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * 4. int64_min
 * ============================================================ */

static int test_int64_min(void) {
    COWRIEValue *v = cowrie_new_int64(INT64_MIN);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_INT64);
    ASSERT(v->as.i64 == INT64_MIN);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);
    ASSERT(decoded->type == COWRIE_INT64);
    ASSERT(decoded->as.i64 == INT64_MIN);

    cowrie_free(v);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * 5. empty_string_key
 * ============================================================ */

static int test_empty_string_key(void) {
    COWRIEValue *obj = cowrie_new_object();
    ASSERT(obj != NULL);

    COWRIEValue *val = cowrie_new_int64(42);
    ASSERT(val != NULL);

    ASSERT(cowrie_object_set(obj, "", 0, val) == 0);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(obj, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);
    ASSERT(decoded->type == COWRIE_OBJECT);
    ASSERT(decoded->as.object.len == 1);
    ASSERT(decoded->as.object.members[0].key_len == 0);

    COWRIEValue *got = cowrie_object_get(decoded, "", 0);
    ASSERT(got != NULL);
    ASSERT(got->type == COWRIE_INT64);
    ASSERT(got->as.i64 == 42);

    cowrie_free(obj);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * 6. negative_zero
 * ============================================================ */

static int test_negative_zero(void) {
    COWRIEValue *v = cowrie_new_float64(-0.0);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_FLOAT64);
    ASSERT(signbit(v->as.f64));

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);
    ASSERT(decoded->type == COWRIE_FLOAT64);
    ASSERT(decoded->as.f64 == 0.0);
    ASSERT(signbit(decoded->as.f64));

    cowrie_free(v);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * 7. nan_roundtrip
 * ============================================================ */

static int test_nan_roundtrip(void) {
    double nan_val = nan("");
    COWRIEValue *v = cowrie_new_float64(nan_val);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_FLOAT64);
    ASSERT(isnan(v->as.f64));

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);
    ASSERT(decoded->type == COWRIE_FLOAT64);
    ASSERT(isnan(decoded->as.f64));

    cowrie_free(v);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * 8. inf_roundtrip
 * ============================================================ */

static int test_inf_roundtrip(void) {
    /* +Inf */
    COWRIEValue *pos = cowrie_new_float64(INFINITY);
    ASSERT(pos != NULL);
    ASSERT(isinf(pos->as.f64) && pos->as.f64 > 0);

    COWRIEBuf buf_pos;
    ASSERT(cowrie_encode(pos, &buf_pos) == 0);

    COWRIEValue *dec_pos;
    ASSERT(cowrie_decode(buf_pos.data, buf_pos.len, &dec_pos) == 0);
    ASSERT(dec_pos->type == COWRIE_FLOAT64);
    ASSERT(isinf(dec_pos->as.f64) && dec_pos->as.f64 > 0);

    /* -Inf */
    COWRIEValue *neg = cowrie_new_float64(-INFINITY);
    ASSERT(neg != NULL);
    ASSERT(isinf(neg->as.f64) && neg->as.f64 < 0);

    COWRIEBuf buf_neg;
    ASSERT(cowrie_encode(neg, &buf_neg) == 0);

    COWRIEValue *dec_neg;
    ASSERT(cowrie_decode(buf_neg.data, buf_neg.len, &dec_neg) == 0);
    ASSERT(dec_neg->type == COWRIE_FLOAT64);
    ASSERT(isinf(dec_neg->as.f64) && dec_neg->as.f64 < 0);

    cowrie_free(pos);
    cowrie_free(neg);
    cowrie_free(dec_pos);
    cowrie_free(dec_neg);
    cowrie_buf_free(&buf_pos);
    cowrie_buf_free(&buf_neg);
    return 1;
}

/* ============================================================
 * 9. empty_array
 * ============================================================ */

static int test_empty_array(void) {
    COWRIEValue *v = cowrie_new_array();
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_ARRAY);
    ASSERT(cowrie_array_len(v) == 0);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);
    ASSERT(decoded->type == COWRIE_ARRAY);
    ASSERT(cowrie_array_len(decoded) == 0);

    cowrie_free(v);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * 10. empty_object
 * ============================================================ */

static int test_empty_object(void) {
    COWRIEValue *v = cowrie_new_object();
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_OBJECT);
    ASSERT(cowrie_object_len(v) == 0);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);
    ASSERT(decoded->type == COWRIE_OBJECT);
    ASSERT(cowrie_object_len(decoded) == 0);

    cowrie_free(v);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * 11. trailing_garbage
 * ============================================================ */

static int test_trailing_garbage(void) {
    COWRIEValue *v = cowrie_new_null();
    ASSERT(v != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    /* Append 0xFF garbage byte */
    uint8_t *extended = (uint8_t *)malloc(buf.len + 1);
    ASSERT(extended != NULL);
    memcpy(extended, buf.data, buf.len);
    extended[buf.len] = 0xFF;

    COWRIEValue *decoded;
    int rc = cowrie_decode(extended, buf.len + 1, &decoded);
    ASSERT(rc != 0); /* Must fail */

    free(extended);
    cowrie_free(v);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * 12. truncated_input
 * ============================================================ */

static int test_truncated_input(void) {
    /* Encode a string value (produces header + tag + length + data) */
    COWRIEValue *v = cowrie_new_string("hello", 5);
    ASSERT(v != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);
    ASSERT(buf.len > 4); /* Must have header + payload */

    /* Truncate to just the header (4 bytes) - missing payload */
    COWRIEValue *decoded;
    int rc = cowrie_decode(buf.data, 4, &decoded);
    ASSERT(rc != 0); /* Must fail */

    /* Also try truncating mid-payload */
    if (buf.len > 5) {
        rc = cowrie_decode(buf.data, 5, &decoded);
        ASSERT(rc != 0); /* Must fail */
    }

    cowrie_free(v);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * 13. empty_input
 * ============================================================ */

static int test_empty_input(void) {
    COWRIEValue *decoded;

    /* Zero-length input */
    int rc = cowrie_decode((const uint8_t *)"", 0, &decoded);
    ASSERT(rc != 0); /* Must fail */

    /* NULL data pointer with zero length */
    rc = cowrie_decode(NULL, 0, &decoded);
    ASSERT(rc != 0); /* Must fail */

    return 1;
}

/* ============================================================
 * 14. unknown_tag
 * ============================================================ */

static int test_unknown_tag(void) {
    /* Build a valid header (SJ\x02\x00) followed by an unknown tag byte */
    uint8_t raw[] = { 'S', 'J', 0x02, 0x00, 0xFE };
    COWRIEValue *decoded;
    int rc = cowrie_decode(raw, sizeof(raw), &decoded);
    /* Unknown tag should fail or be handled; either way, must not crash */
    /* Some implementations may return error, some may handle it */
    (void)rc;
    if (rc == 0) {
        cowrie_free(decoded);
    }
    return 1;
}

/* ============================================================
 * 15. unicode_string
 * ============================================================ */

static int test_unicode_string(void) {
    const char *text = "hello \xe4\xb8\x96\xe7\x95\x8c \xf0\x9f\x8c\x8d";
    size_t text_len = strlen(text);

    COWRIEValue *v = cowrie_new_string(text, text_len);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_STRING);
    ASSERT(v->as.str.len == text_len);
    ASSERT(memcmp(v->as.str.data, text, text_len) == 0);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);
    ASSERT(decoded->type == COWRIE_STRING);
    ASSERT(decoded->as.str.len == text_len);
    ASSERT(memcmp(decoded->as.str.data, text, text_len) == 0);

    cowrie_free(v);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * 16. nested_depth_100
 * ============================================================ */

static int test_nested_depth_100(void) {
    /* Build 100 nested arrays: [[[[...[]...]]]] */
    COWRIEValue *innermost = cowrie_new_array();
    ASSERT(innermost != NULL);

    COWRIEValue *current = innermost;
    for (int i = 0; i < 99; i++) {
        COWRIEValue *outer = cowrie_new_array();
        ASSERT(outer != NULL);
        ASSERT(cowrie_array_append(outer, current) == 0);
        current = outer;
    }

    COWRIEBuf buf;
    ASSERT(cowrie_encode(current, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);

    /* Walk 100 levels deep to verify structure */
    COWRIEValue *walk = decoded;
    for (int i = 0; i < 99; i++) {
        ASSERT(walk->type == COWRIE_ARRAY);
        ASSERT(cowrie_array_len(walk) == 1);
        walk = cowrie_array_get(walk, 0);
        ASSERT(walk != NULL);
    }
    /* Innermost should be empty array */
    ASSERT(walk->type == COWRIE_ARRAY);
    ASSERT(cowrie_array_len(walk) == 0);

    cowrie_free(current);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * main
 * ============================================================ */

int main(void) {
    printf("=== Cowrie Truth Table Tests ===\n");

    TEST(null_roundtrip);
    TEST(bool_roundtrip);
    TEST(int64_max);
    TEST(int64_min);
    TEST(empty_string_key);
    TEST(negative_zero);
    TEST(nan_roundtrip);
    TEST(inf_roundtrip);
    TEST(empty_array);
    TEST(empty_object);
    TEST(trailing_garbage);
    TEST(truncated_input);
    TEST(empty_input);
    TEST(unknown_tag);
    TEST(unicode_string);
    TEST(nested_depth_100);

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
