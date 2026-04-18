/**
 * GLYPH Codec C Tests
 */

#include "glyph.h"
#include "decimal128.h"
#include "schema_evolution.h"
#include "stream_validator.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>

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
 * Decimal128 Tests
 * ============================================================ */

TEST(decimal128_zero) {
    decimal128_t d = decimal128_zero();
    ASSERT_TRUE(decimal128_is_zero(&d));
    ASSERT_TRUE(!decimal128_is_negative(&d));
    ASSERT_TRUE(!decimal128_is_positive(&d));
    char *s = decimal128_to_string(&d);
    ASSERT_STR_EQ("0", s);
    free(s);
}

TEST(decimal128_from_int_positive) {
    decimal128_t d = decimal128_from_int(42);
    ASSERT_TRUE(!decimal128_is_zero(&d));
    ASSERT_TRUE(decimal128_is_positive(&d));
    ASSERT_TRUE(!decimal128_is_negative(&d));
    ASSERT_TRUE(decimal128_to_int(&d) == 42);
    char *s = decimal128_to_string(&d);
    ASSERT_STR_EQ("42", s);
    free(s);
}

TEST(decimal128_from_int_negative) {
    decimal128_t d = decimal128_from_int(-99);
    ASSERT_TRUE(decimal128_is_negative(&d));
    ASSERT_TRUE(decimal128_to_int(&d) == -99);
    char *s = decimal128_to_string(&d);
    ASSERT_STR_EQ("-99", s);
    free(s);
}

TEST(decimal128_from_uint) {
    decimal128_t d = decimal128_from_uint(1000);
    ASSERT_TRUE(decimal128_to_int(&d) == 1000);
}

TEST(decimal128_from_string) {
    decimal128_t d;
    ASSERT_TRUE(decimal128_from_string("123.45", &d) == DECIMAL_OK);
    ASSERT_TRUE(d.scale == 2);
    char *s = decimal128_to_string(&d);
    ASSERT_STR_EQ("123.45", s);
    free(s);
}

TEST(decimal128_from_string_negative) {
    decimal128_t d;
    ASSERT_TRUE(decimal128_from_string("-0.001", &d) == DECIMAL_OK);
    ASSERT_TRUE(decimal128_is_negative(&d));
    char *s = decimal128_to_string(&d);
    ASSERT_STR_EQ("-0.001", s);
    free(s);
}

TEST(decimal128_from_string_m_suffix) {
    decimal128_t d;
    ASSERT_TRUE(decimal128_from_string("99.99m", &d) == DECIMAL_OK);
    char *s = decimal128_to_string(&d);
    ASSERT_STR_EQ("99.99", s);
    free(s);
}

TEST(decimal128_from_string_invalid) {
    decimal128_t d;
    ASSERT_TRUE(decimal128_from_string("abc", &d) == DECIMAL_ERR_PARSE_FAILED);
    ASSERT_TRUE(decimal128_from_string(NULL, &d) == DECIMAL_ERR_PARSE_FAILED);
}

TEST(decimal128_is_literal) {
    ASSERT_TRUE(decimal128_is_literal("123.45m"));
    ASSERT_TRUE(decimal128_is_literal("0m"));
    ASSERT_TRUE(!decimal128_is_literal("123.45"));
    ASSERT_TRUE(!decimal128_is_literal("m"));
    ASSERT_TRUE(!decimal128_is_literal(NULL));
}

TEST(decimal128_add) {
    decimal128_t a = decimal128_from_int(100);
    decimal128_t b = decimal128_from_int(200);
    decimal128_t out;
    ASSERT_TRUE(decimal128_add(&a, &b, &out) == DECIMAL_OK);
    ASSERT_TRUE(decimal128_to_int(&out) == 300);
}

TEST(decimal128_add_decimals) {
    decimal128_t a, b, out;
    decimal128_from_string("1.25", &a);
    decimal128_from_string("2.75", &b);
    ASSERT_TRUE(decimal128_add(&a, &b, &out) == DECIMAL_OK);
    char *s = decimal128_to_string(&out);
    ASSERT_STR_EQ("4.00", s);
    free(s);
}

TEST(decimal128_sub) {
    decimal128_t a = decimal128_from_int(500);
    decimal128_t b = decimal128_from_int(300);
    decimal128_t out;
    ASSERT_TRUE(decimal128_sub(&a, &b, &out) == DECIMAL_OK);
    ASSERT_TRUE(decimal128_to_int(&out) == 200);
}

TEST(decimal128_mul) {
    decimal128_t a = decimal128_from_int(7);
    decimal128_t b = decimal128_from_int(6);
    decimal128_t out;
    ASSERT_TRUE(decimal128_mul(&a, &b, &out) == DECIMAL_OK);
    ASSERT_TRUE(decimal128_to_int(&out) == 42);
}

TEST(decimal128_div) {
    decimal128_t a = decimal128_from_int(100);
    decimal128_t b = decimal128_from_int(4);
    decimal128_t out;
    ASSERT_TRUE(decimal128_div(&a, &b, &out) == DECIMAL_OK);
    ASSERT_TRUE(decimal128_to_int(&out) == 25);
}

TEST(decimal128_div_by_zero) {
    decimal128_t a = decimal128_from_int(1);
    decimal128_t b = decimal128_zero();
    decimal128_t out;
    ASSERT_TRUE(decimal128_div(&a, &b, &out) == DECIMAL_ERR_DIVISION_BY_ZERO);
}

TEST(decimal128_cmp) {
    decimal128_t a = decimal128_from_int(10);
    decimal128_t b = decimal128_from_int(20);
    decimal128_t c = decimal128_from_int(10);
    ASSERT_TRUE(decimal128_cmp(&a, &b) < 0);
    ASSERT_TRUE(decimal128_cmp(&b, &a) > 0);
    ASSERT_TRUE(decimal128_cmp(&a, &c) == 0);
    ASSERT_TRUE(decimal128_equals(&a, &c));
    ASSERT_TRUE(decimal128_lt(&a, &b));
    ASSERT_TRUE(decimal128_gt(&b, &a));
    ASSERT_TRUE(decimal128_lte(&a, &c));
    ASSERT_TRUE(decimal128_gte(&a, &c));
}

TEST(decimal128_negate) {
    decimal128_t a = decimal128_from_int(42);
    decimal128_t neg = decimal128_negate(&a);
    ASSERT_TRUE(decimal128_is_negative(&neg));
    ASSERT_TRUE(decimal128_to_int(&neg) == -42);
}

TEST(decimal128_abs) {
    decimal128_t a = decimal128_from_int(-42);
    decimal128_t abs_val = decimal128_abs(&a);
    ASSERT_TRUE(!decimal128_is_negative(&abs_val));
    ASSERT_TRUE(decimal128_to_int(&abs_val) == 42);
}

TEST(decimal128_from_double) {
    decimal128_t d = decimal128_from_double(3.14);
    double val = decimal128_to_double(&d);
    ASSERT_TRUE(fabs(val - 3.14) < 0.001);
}

/* ============================================================
 * Schema Evolution Tests
 * ============================================================ */

TEST(compare_versions_basic) {
    ASSERT_TRUE(compare_versions("1.0", "2.0") < 0);
    ASSERT_TRUE(compare_versions("2.0", "1.0") > 0);
    ASSERT_TRUE(compare_versions("1.0", "1.0") == 0);
}

TEST(compare_versions_multi_part) {
    ASSERT_TRUE(compare_versions("1.2.3", "1.2.4") < 0);
    ASSERT_TRUE(compare_versions("1.3.0", "1.2.9") > 0);
    ASSERT_TRUE(compare_versions("1.0", "1.0.0") == 0);
}

TEST(version_header_roundtrip) {
    char *header = format_version_header("2.1");
    ASSERT_STR_EQ("@version 2.1", header);
    char *version = parse_version_header(header);
    ASSERT_STR_EQ("2.1", version);
    free(header);
    free(version);
}

TEST(parse_version_header_invalid) {
    ASSERT_TRUE(parse_version_header("not a header") == NULL);
    ASSERT_TRUE(parse_version_header(NULL) == NULL);
}

TEST(field_value_types) {
    field_value_t v_null = field_value_null();
    ASSERT_TRUE(v_null.type == FIELD_VALUE_NULL);

    field_value_t v_bool = field_value_bool(true);
    ASSERT_TRUE(v_bool.type == FIELD_VALUE_BOOL);
    ASSERT_TRUE(v_bool.bool_val == true);

    field_value_t v_int = field_value_int(42);
    ASSERT_TRUE(v_int.type == FIELD_VALUE_INT);
    ASSERT_TRUE(v_int.int_val == 42);

    field_value_t v_float = field_value_float(3.14);
    ASSERT_TRUE(v_float.type == FIELD_VALUE_FLOAT);

    field_value_t v_str = field_value_str("hello");
    ASSERT_TRUE(v_str.type == FIELD_VALUE_STR);
    ASSERT_STR_EQ("hello", v_str.str_val);
    field_value_free(&v_str);
}

TEST(evolving_field_availability) {
    evolving_field_config_t config = {
        .type = FIELD_TYPE_STR,
        .required = false,
        .default_value = field_value_null(),
        .added_in = "1.0",
        .deprecated_in = "3.0",
        .renamed_from = NULL,
        .validation = NULL,
    };
    evolving_field_t *f = evolving_field_new("name", &config);
    ASSERT_TRUE(f != NULL);

    /* Available in 1.0 through 2.x */
    ASSERT_TRUE(evolving_field_is_available_in(f, "1.0"));
    ASSERT_TRUE(evolving_field_is_available_in(f, "2.0"));
    ASSERT_TRUE(evolving_field_is_available_in(f, "2.5"));

    /* Not available before added_in */
    ASSERT_TRUE(!evolving_field_is_available_in(f, "0.9"));

    /* Deprecated at 3.0 */
    ASSERT_TRUE(!evolving_field_is_available_in(f, "3.0"));
    ASSERT_TRUE(evolving_field_is_deprecated_in(f, "3.0"));
    ASSERT_TRUE(!evolving_field_is_deprecated_in(f, "2.0"));

    evolving_field_free(f);
}

TEST(evolving_field_validate_required) {
    evolving_field_config_t config = {
        .type = FIELD_TYPE_STR,
        .required = true,
        .default_value = field_value_null(),
        .added_in = "1.0",
        .deprecated_in = NULL,
        .renamed_from = NULL,
        .validation = NULL,
    };
    evolving_field_t *f = evolving_field_new("email", &config);

    /* Missing required field should fail */
    char *err = evolving_field_validate(f, NULL);
    ASSERT_TRUE(err != NULL);
    free(err);

    /* Providing a value should pass */
    field_value_t val = field_value_str("test@example.com");
    err = evolving_field_validate(f, &val);
    ASSERT_TRUE(err == NULL);
    field_value_free(&val);

    evolving_field_free(f);
}

TEST(evolving_field_validate_type) {
    evolving_field_config_t config = {
        .type = FIELD_TYPE_INT,
        .required = false,
        .default_value = field_value_null(),
        .added_in = "1.0",
        .deprecated_in = NULL,
        .renamed_from = NULL,
        .validation = NULL,
    };
    evolving_field_t *f = evolving_field_new("age", &config);

    /* Wrong type should fail */
    field_value_t str_val = field_value_str("not a number");
    char *err = evolving_field_validate(f, &str_val);
    ASSERT_TRUE(err != NULL);
    free(err);
    field_value_free(&str_val);

    /* Correct type should pass */
    field_value_t int_val = field_value_int(25);
    err = evolving_field_validate(f, &int_val);
    ASSERT_TRUE(err == NULL);

    evolving_field_free(f);
}

TEST(version_schema_add_and_get) {
    /* Use versioned_schema which has correct cleanup for embedded fields.
     * version_schema_free has a known bug: it calls evolving_field_free
     * which does free(f) on array-embedded elements. */
    versioned_schema_t *vs = versioned_schema_new("test");

    const char *field_names[] = {"name"};
    evolving_field_config_t fields[] = {{
        .type = FIELD_TYPE_STR,
        .required = true,
        .default_value = field_value_null(),
        .added_in = "1.0",
        .deprecated_in = NULL,
        .renamed_from = NULL,
        .validation = NULL,
    }};
    versioned_schema_add_version(vs, "1.0", fields, field_names, 1);

    const version_schema_t *s = versioned_schema_get_version(vs, "1.0");
    ASSERT_TRUE(s != NULL);

    const evolving_field_t *got = version_schema_get_field(s, "name");
    ASSERT_TRUE(got != NULL);
    ASSERT_STR_EQ("name", got->name);

    ASSERT_TRUE(version_schema_get_field(s, "nonexistent") == NULL);

    versioned_schema_free(vs);
}

TEST(versioned_schema_migration) {
    versioned_schema_t *s = versioned_schema_new("user_tool");
    versioned_schema_with_mode(s, EVOLUTION_MODE_TOLERANT);

    /* v1.0: name (required) */
    const char *v1_names[] = {"name"};
    evolving_field_config_t v1_fields[] = {{
        .type = FIELD_TYPE_STR,
        .required = true,
        .default_value = field_value_null(),
        .added_in = "1.0",
        .deprecated_in = NULL,
        .renamed_from = NULL,
        .validation = NULL,
    }};
    versioned_schema_add_version(s, "1.0", v1_fields, v1_names, 1);

    /* v2.0: name (required) + email (optional, default "none") */
    const char *v2_names[] = {"name", "email"};
    field_value_t email_default = field_value_str("none");
    evolving_field_config_t v2_fields[] = {
        {
            .type = FIELD_TYPE_STR,
            .required = true,
            .default_value = field_value_null(),
            .added_in = "1.0",
            .deprecated_in = NULL,
            .renamed_from = NULL,
            .validation = NULL,
        },
        {
            .type = FIELD_TYPE_STR,
            .required = false,
            .default_value = email_default,
            .added_in = "2.0",
            .deprecated_in = NULL,
            .renamed_from = NULL,
            .validation = NULL,
        },
    };
    versioned_schema_add_version(s, "2.0", v2_fields, v2_names, 2);

    /* Parse v1.0 data — should migrate to v2.0, adding "email" with default */
    const char *keys[] = {"name"};
    field_value_t data[] = {field_value_str("Alice")};
    evolution_parse_result_t result = versioned_schema_parse(s, data, keys, 1, "1.0");

    ASSERT_TRUE(result.error == NULL);
    ASSERT_TRUE(result.data_count == 2);

    /* Find email field in result */
    bool found_email = false;
    for (size_t i = 0; i < result.data_count; i++) {
        if (strcmp(result.keys[i], "email") == 0) {
            found_email = true;
            ASSERT_TRUE(result.data[i].type == FIELD_VALUE_STR);
            ASSERT_STR_EQ("none", result.data[i].str_val);
        }
    }
    ASSERT_TRUE(found_email);

    evolution_parse_result_free(&result);
    field_value_free(&data[0]);
    field_value_free(&email_default);
    versioned_schema_free(s);
}

TEST(versioned_schema_changelog) {
    versioned_schema_t *s = versioned_schema_new("test");

    const char *v1_names[] = {"a"};
    evolving_field_config_t v1_fields[] = {{
        .type = FIELD_TYPE_STR,
        .required = false,
        .default_value = field_value_null(),
        .added_in = "1.0",
        .deprecated_in = NULL,
        .renamed_from = NULL,
        .validation = NULL,
    }};
    versioned_schema_add_version(s, "1.0", v1_fields, v1_names, 1);

    size_t count = 0;
    changelog_entry_t *log = versioned_schema_get_changelog(s, &count);
    ASSERT_TRUE(count == 1);
    ASSERT_STR_EQ("1.0", log[0].version);
    ASSERT_TRUE(log[0].added_count == 1);
    ASSERT_STR_EQ("a", log[0].added_fields[0]);

    changelog_free(log, count);
    versioned_schema_free(s);
}

/* ============================================================
 * Stream Validator Tests
 * ============================================================ */

TEST(stream_validator_known_tool) {
    tool_registry_t *reg = tool_registry_default();
    streaming_validator_t *v = streaming_validator_new(reg);

    /* Feed a complete GLYPH tool call token by token */
    validation_result_t *r;
    r = streaming_validator_push_token(v, "{action=search ");
    validation_result_free(r);
    r = streaming_validator_push_token(v, "query=hello}");

    ASSERT_TRUE(r->complete);
    ASSERT_TRUE(r->valid);
    ASSERT_TRUE(r->tool_allowed);
    ASSERT_STR_EQ("search", r->tool_name);

    validation_result_free(r);
    streaming_validator_free(v);
    tool_registry_free(reg);
}

TEST(stream_validator_unknown_tool) {
    tool_registry_t *reg = tool_registry_default();
    streaming_validator_t *v = streaming_validator_new(reg);

    validation_result_t *r;
    r = streaming_validator_push_token(v, "{action=destroy_all ");
    validation_result_free(r);
    r = streaming_validator_push_token(v, "target=everything}");

    ASSERT_TRUE(r->complete);
    ASSERT_TRUE(!r->valid);
    ASSERT_TRUE(!r->tool_allowed);

    /* Should have UNKNOWN_TOOL error */
    bool found_unknown = false;
    for (size_t i = 0; i < r->errors_count; i++) {
        if (r->errors[i].code == VERR_UNKNOWN_TOOL) {
            found_unknown = true;
            break;
        }
    }
    ASSERT_TRUE(found_unknown);
    ASSERT_TRUE(streaming_validator_should_stop(v));

    validation_result_free(r);
    streaming_validator_free(v);
    tool_registry_free(reg);
}

TEST(stream_validator_missing_required) {
    tool_registry_t *reg = tool_registry_default();
    streaming_validator_t *v = streaming_validator_new(reg);

    /* search tool requires "query" — omit it */
    validation_result_t *r;
    r = streaming_validator_push_token(v, "{action=search}");

    ASSERT_TRUE(r->complete);
    ASSERT_TRUE(!r->valid);

    /* Should have MISSING_REQUIRED error */
    bool found_missing = false;
    for (size_t i = 0; i < r->errors_count; i++) {
        if (r->errors[i].code == VERR_MISSING_REQUIRED) {
            found_missing = true;
            break;
        }
    }
    ASSERT_TRUE(found_missing);

    validation_result_free(r);
    streaming_validator_free(v);
    tool_registry_free(reg);
}

TEST(stream_validator_constraint_max) {
    tool_registry_t *reg = tool_registry_default();
    streaming_validator_t *v = streaming_validator_new(reg);

    /* search: max_results has range [1, 100] — exceed it */
    validation_result_t *r;
    r = streaming_validator_push_token(v, "{action=search query=test max_results=999}");

    ASSERT_TRUE(r->complete);
    ASSERT_TRUE(!r->valid);

    bool found_max = false;
    for (size_t i = 0; i < r->errors_count; i++) {
        if (r->errors[i].code == VERR_CONSTRAINT_MAX) {
            found_max = true;
            break;
        }
    }
    ASSERT_TRUE(found_max);

    validation_result_free(r);
    streaming_validator_free(v);
    tool_registry_free(reg);
}

TEST(stream_validator_reset) {
    tool_registry_t *reg = tool_registry_default();
    streaming_validator_t *v = streaming_validator_new(reg);

    validation_result_t *r;
    r = streaming_validator_push_token(v, "{action=search query=first}");
    ASSERT_TRUE(r->complete);
    ASSERT_TRUE(r->valid);
    validation_result_free(r);

    /* Reset and feed new input */
    streaming_validator_reset(v);
    r = streaming_validator_push_token(v, "{action=calculate expression=1+1}");
    ASSERT_TRUE(r->complete);
    ASSERT_TRUE(r->valid);
    ASSERT_STR_EQ("calculate", r->tool_name);

    validation_result_free(r);
    streaming_validator_free(v);
    tool_registry_free(reg);
}

TEST(stream_validator_incremental) {
    tool_registry_t *reg = tool_registry_default();
    streaming_validator_t *v = streaming_validator_new(reg);

    /* Feed character by character to test incremental parsing */
    const char *input = "{action=search query=hello}";
    validation_result_t *r = NULL;
    for (const char *c = input; *c; c++) {
        if (r) validation_result_free(r);
        char token[2] = {*c, '\0'};
        r = streaming_validator_push_token(v, token);
    }

    ASSERT_TRUE(r != NULL);
    ASSERT_TRUE(r->complete);
    ASSERT_TRUE(r->valid);
    ASSERT_STR_EQ("search", r->tool_name);

    validation_result_free(r);
    streaming_validator_free(v);
    tool_registry_free(reg);
}

TEST(tool_registry_custom) {
    tool_registry_t *reg = tool_registry_new();

    tool_schema_t *tool = tool_schema_new("greet", "Say hello");
    arg_schema_t *name_arg = arg_schema_new("name", "string");
    arg_schema_set_required(name_arg, true);
    arg_schema_set_length(name_arg, 1, 50);
    tool_schema_add_arg(tool, name_arg);

    const char *mood_values[] = {"happy", "sad", "neutral"};
    arg_schema_t *mood_arg = arg_schema_new("mood", "string");
    arg_schema_set_enum(mood_arg, mood_values, 3);
    tool_schema_add_arg(tool, mood_arg);

    tool_registry_register(reg, tool);

    ASSERT_TRUE(tool_registry_is_allowed(reg, "greet"));
    ASSERT_TRUE(!tool_registry_is_allowed(reg, "unknown"));

    const tool_schema_t *got = tool_registry_get(reg, "greet");
    ASSERT_TRUE(got != NULL);
    ASSERT_STR_EQ("greet", got->name);
    ASSERT_TRUE(got->args_count == 2);

    /* Test enum constraint violation */
    streaming_validator_t *v = streaming_validator_new(reg);
    validation_result_t *r = streaming_validator_push_token(v, "{action=greet name=World mood=angry}");
    ASSERT_TRUE(r->complete);
    ASSERT_TRUE(!r->valid);

    bool found_enum = false;
    for (size_t i = 0; i < r->errors_count; i++) {
        if (r->errors[i].code == VERR_CONSTRAINT_ENUM) {
            found_enum = true;
            break;
        }
    }
    ASSERT_TRUE(found_enum);

    validation_result_free(r);
    streaming_validator_free(v);
    tool_registry_free(reg);
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

    printf("\nDecimal128 Tests:\n");
    RUN_TEST(decimal128_zero);
    RUN_TEST(decimal128_from_int_positive);
    RUN_TEST(decimal128_from_int_negative);
    RUN_TEST(decimal128_from_uint);
    RUN_TEST(decimal128_from_string);
    RUN_TEST(decimal128_from_string_negative);
    RUN_TEST(decimal128_from_string_m_suffix);
    RUN_TEST(decimal128_from_string_invalid);
    RUN_TEST(decimal128_is_literal);
    RUN_TEST(decimal128_add);
    RUN_TEST(decimal128_add_decimals);
    RUN_TEST(decimal128_sub);
    RUN_TEST(decimal128_mul);
    RUN_TEST(decimal128_div);
    RUN_TEST(decimal128_div_by_zero);
    RUN_TEST(decimal128_cmp);
    RUN_TEST(decimal128_negate);
    RUN_TEST(decimal128_abs);
    RUN_TEST(decimal128_from_double);

    printf("\nSchema Evolution Tests:\n");
    RUN_TEST(compare_versions_basic);
    RUN_TEST(compare_versions_multi_part);
    RUN_TEST(version_header_roundtrip);
    RUN_TEST(parse_version_header_invalid);
    RUN_TEST(field_value_types);
    RUN_TEST(evolving_field_availability);
    RUN_TEST(evolving_field_validate_required);
    RUN_TEST(evolving_field_validate_type);
    RUN_TEST(version_schema_add_and_get);
    RUN_TEST(versioned_schema_migration);
    RUN_TEST(versioned_schema_changelog);

    printf("\nStream Validator Tests:\n");
    RUN_TEST(stream_validator_known_tool);
    RUN_TEST(stream_validator_unknown_tool);
    RUN_TEST(stream_validator_missing_required);
    RUN_TEST(stream_validator_constraint_max);
    RUN_TEST(stream_validator_reset);
    RUN_TEST(stream_validator_incremental);
    RUN_TEST(tool_registry_custom);

    printf("\n===================\n");
    printf("Results: %d passed, %d failed\n", tests_passed, tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
