/**
 * GLYPH Codec C Tests
 */

#include "glyph.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) void test_##name(void)
#define RUN_TEST(name) do { \
    printf("  Running %s...", #name); \
    test_##name(); \
    printf(" PASSED\n"); \
    tests_passed++; \
} while(0)

#define ASSERT_STR_EQ(expected, actual) do { \
    if (strcmp(expected, actual) != 0) { \
        printf("\n    FAILED: expected '%s', got '%s'\n", expected, actual); \
        tests_failed++; \
        return; \
    } \
} while(0)

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        printf("\n    FAILED: expected true\n"); \
        tests_failed++; \
        return; \
    } \
} while(0)

/* ============================================================
 * Primitive Tests
 * ============================================================ */

TEST(null_canonical) {
    glyph_value_t *v = glyph_null();
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("_", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(bool_true) {
    glyph_value_t *v = glyph_bool(true);
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("t", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(bool_false) {
    glyph_value_t *v = glyph_bool(false);
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("f", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(int_positive) {
    glyph_value_t *v = glyph_int(42);
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("42", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(int_negative) {
    glyph_value_t *v = glyph_int(-123);
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("-123", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(int_zero) {
    glyph_value_t *v = glyph_int(0);
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("0", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(float_whole_number) {
    glyph_value_t *v = glyph_float(42.0);
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("42", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(float_decimal) {
    glyph_value_t *v = glyph_float(3.14);
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_TRUE(strncmp(canon, "3.14", 4) == 0);
    glyph_free(canon);
    glyph_value_free(v);
}

/* ============================================================
 * String Tests
 * ============================================================ */

TEST(string_bare_safe) {
    glyph_value_t *v = glyph_str("hello");
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("hello", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(string_needs_quotes) {
    glyph_value_t *v = glyph_str("hello world");
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("\"hello world\"", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(string_starts_with_digit) {
    glyph_value_t *v = glyph_str("123abc");
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("\"123abc\"", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(string_empty) {
    glyph_value_t *v = glyph_str("");
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("\"\"", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(string_reserved_t) {
    glyph_value_t *v = glyph_str("t");
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("\"t\"", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(string_reserved_f) {
    glyph_value_t *v = glyph_str("f");
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("\"f\"", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(string_with_escape) {
    glyph_value_t *v = glyph_str("line1\nline2");
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("\"line1\\nline2\"", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

/* ============================================================
 * List Tests
 * ============================================================ */

TEST(list_empty) {
    glyph_value_t *v = glyph_list_new();
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("[]", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(list_single) {
    glyph_value_t *v = glyph_list_new();
    glyph_list_append(v, glyph_int(1));
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("[1]", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(list_multiple) {
    glyph_value_t *v = glyph_list_new();
    glyph_list_append(v, glyph_int(1));
    glyph_list_append(v, glyph_int(2));
    glyph_list_append(v, glyph_int(3));
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("[1 2 3]", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

/* ============================================================
 * Map Tests
 * ============================================================ */

TEST(map_empty) {
    glyph_value_t *v = glyph_map_new();
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("{}", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(map_single) {
    glyph_value_t *v = glyph_map_new();
    glyph_map_set(v, "a", glyph_int(1));
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("{a=1}", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(map_sorted_keys) {
    glyph_value_t *v = glyph_map_new();
    glyph_map_set(v, "b", glyph_int(2));
    glyph_map_set(v, "a", glyph_int(1));
    glyph_map_set(v, "c", glyph_int(3));
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("{a=1 b=2 c=3}", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

/* ============================================================
 * Reference ID Tests
 * ============================================================ */

TEST(ref_id_simple) {
    glyph_value_t *v = glyph_id(NULL, "user123");
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("^user123", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(ref_id_with_prefix) {
    glyph_value_t *v = glyph_id("user", "123");
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("^user:123", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(ref_id_numeric) {
    glyph_value_t *v = glyph_id(NULL, "12345");
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("^12345", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(ref_id_needs_quotes) {
    glyph_value_t *v = glyph_id(NULL, "hello world");
    char *canon = glyph_canonicalize_loose(v);
    ASSERT_STR_EQ("^\"hello world\"", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

/* ============================================================
 * Tabular Mode Tests
 * ============================================================ */

TEST(tabular_homogeneous) {
    glyph_value_t *v = glyph_list_new();
    for (int i = 0; i < 3; i++) {
        glyph_value_t *m = glyph_map_new();
        glyph_map_set(m, "x", glyph_int(i));
        glyph_map_set(m, "y", glyph_int(i * 2));
        glyph_list_append(v, m);
    }
    char *canon = glyph_canonicalize_loose(v);
    /* Should produce tabular output */
    ASSERT_TRUE(strstr(canon, "@tab") != NULL);
    ASSERT_TRUE(strstr(canon, "@end") != NULL);
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(tabular_sparse_keys_no_tabular) {
    /* [{a:1}, {b:2}, {c:3}] - less than 50% common keys */
    glyph_value_t *v = glyph_list_new();

    glyph_value_t *m1 = glyph_map_new();
    glyph_map_set(m1, "a", glyph_int(1));
    glyph_list_append(v, m1);

    glyph_value_t *m2 = glyph_map_new();
    glyph_map_set(m2, "b", glyph_int(2));
    glyph_list_append(v, m2);

    glyph_value_t *m3 = glyph_map_new();
    glyph_map_set(m3, "c", glyph_int(3));
    glyph_list_append(v, m3);

    char *canon = glyph_canonicalize_loose(v);
    /* Should NOT produce tabular output due to sparse keys */
    ASSERT_TRUE(strstr(canon, "@tab") == NULL);
    ASSERT_TRUE(canon[0] == '[');
    glyph_free(canon);
    glyph_value_free(v);
}

TEST(tabular_empty_objects_no_tabular) {
    /* [{}, {}, {}] - empty objects should not become tabular */
    glyph_value_t *v = glyph_list_new();
    glyph_list_append(v, glyph_map_new());
    glyph_list_append(v, glyph_map_new());
    glyph_list_append(v, glyph_map_new());

    char *canon = glyph_canonicalize_loose(v);
    ASSERT_TRUE(strstr(canon, "@tab") == NULL);
    ASSERT_STR_EQ("[{} {} {}]", canon);
    glyph_free(canon);
    glyph_value_free(v);
}

/* ============================================================
 * JSON Bridge Tests
 * ============================================================ */

TEST(json_parse_null) {
    glyph_value_t *v = glyph_from_json("null");
    ASSERT_TRUE(v != NULL);
    ASSERT_TRUE(v->type == GLYPH_NULL);
    glyph_value_free(v);
}

TEST(json_parse_bool_true) {
    glyph_value_t *v = glyph_from_json("true");
    ASSERT_TRUE(v != NULL);
    ASSERT_TRUE(v->type == GLYPH_BOOL);
    ASSERT_TRUE(v->bool_val == true);
    glyph_value_free(v);
}

TEST(json_parse_int) {
    glyph_value_t *v = glyph_from_json("42");
    ASSERT_TRUE(v != NULL);
    ASSERT_TRUE(v->type == GLYPH_INT);
    ASSERT_TRUE(v->int_val == 42);
    glyph_value_free(v);
}

TEST(json_parse_string) {
    glyph_value_t *v = glyph_from_json("\"hello\"");
    ASSERT_TRUE(v != NULL);
    ASSERT_TRUE(v->type == GLYPH_STR);
    ASSERT_STR_EQ("hello", v->str_val);
    glyph_value_free(v);
}

TEST(json_parse_array) {
    glyph_value_t *v = glyph_from_json("[1, 2, 3]");
    ASSERT_TRUE(v != NULL);
    ASSERT_TRUE(v->type == GLYPH_LIST);
    ASSERT_TRUE(v->list_val.count == 3);
    glyph_value_free(v);
}

TEST(json_parse_object) {
    glyph_value_t *v = glyph_from_json("{\"a\": 1, \"b\": 2}");
    ASSERT_TRUE(v != NULL);
    ASSERT_TRUE(v->type == GLYPH_MAP);
    ASSERT_TRUE(v->map_val.count == 2);
    glyph_value_free(v);
}

TEST(json_roundtrip) {
    glyph_value_t *v = glyph_from_json("{\"name\": \"test\", \"value\": 42}");
    char *json = glyph_to_json(v);
    glyph_value_t *v2 = glyph_from_json(json);
    ASSERT_TRUE(glyph_equal_loose(v, v2));
    glyph_free(json);
    glyph_value_free(v);
    glyph_value_free(v2);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    printf("GLYPH Codec C Tests\n");
    printf("===================\n\n");

    printf("Primitive Tests:\n");
    RUN_TEST(null_canonical);
    RUN_TEST(bool_true);
    RUN_TEST(bool_false);
    RUN_TEST(int_positive);
    RUN_TEST(int_negative);
    RUN_TEST(int_zero);
    RUN_TEST(float_whole_number);
    RUN_TEST(float_decimal);

    printf("\nString Tests:\n");
    RUN_TEST(string_bare_safe);
    RUN_TEST(string_needs_quotes);
    RUN_TEST(string_starts_with_digit);
    RUN_TEST(string_empty);
    RUN_TEST(string_reserved_t);
    RUN_TEST(string_reserved_f);
    RUN_TEST(string_with_escape);

    printf("\nList Tests:\n");
    RUN_TEST(list_empty);
    RUN_TEST(list_single);
    RUN_TEST(list_multiple);

    printf("\nMap Tests:\n");
    RUN_TEST(map_empty);
    RUN_TEST(map_single);
    RUN_TEST(map_sorted_keys);

    printf("\nReference ID Tests:\n");
    RUN_TEST(ref_id_simple);
    RUN_TEST(ref_id_with_prefix);
    RUN_TEST(ref_id_numeric);
    RUN_TEST(ref_id_needs_quotes);

    printf("\nTabular Mode Tests:\n");
    RUN_TEST(tabular_homogeneous);
    RUN_TEST(tabular_sparse_keys_no_tabular);
    RUN_TEST(tabular_empty_objects_no_tabular);

    printf("\nJSON Bridge Tests:\n");
    RUN_TEST(json_parse_null);
    RUN_TEST(json_parse_bool_true);
    RUN_TEST(json_parse_int);
    RUN_TEST(json_parse_string);
    RUN_TEST(json_parse_array);
    RUN_TEST(json_parse_object);
    RUN_TEST(json_roundtrip);

    printf("\n===================\n");
    printf("Results: %d passed, %d failed\n", tests_passed, tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
