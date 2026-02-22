/*
 * COWRIE Gen1 Tests
 */

#include "../include/cowrie_gen1.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) \
    static void test_##name(void); \
    static void run_test_##name(void) { \
        tests_run++; \
        printf("  Test %s... ", #name); \
        test_##name(); \
        tests_passed++; \
        printf("OK\n"); \
    } \
    static void test_##name(void)

TEST(null) {
    cowrie_g1_value_t *v = cowrie_g1_null();
    assert(v != NULL);
    assert(v->type == COWRIE_G1_TYPE_NULL);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(v, &buf);
    assert(err == COWRIE_G1_OK);
    assert(buf.len == 1);
    assert(buf.data[0] == COWRIE_G1_TAG_NULL);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_NULL);

    cowrie_g1_value_free(v);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(bool) {
    cowrie_g1_value_t *v_true = cowrie_g1_bool(true);
    cowrie_g1_value_t *v_false = cowrie_g1_bool(false);

    assert(v_true != NULL && v_true->type == COWRIE_G1_TYPE_BOOL && v_true->bool_val == true);
    assert(v_false != NULL && v_false->type == COWRIE_G1_TYPE_BOOL && v_false->bool_val == false);

    cowrie_g1_buf_t buf;

    cowrie_g1_encode(v_true, &buf);
    assert(buf.len == 1 && buf.data[0] == COWRIE_G1_TAG_TRUE);

    cowrie_g1_value_t *decoded;
    cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(decoded->type == COWRIE_G1_TYPE_BOOL && decoded->bool_val == true);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);

    cowrie_g1_encode(v_false, &buf);
    assert(buf.len == 1 && buf.data[0] == COWRIE_G1_TAG_FALSE);

    cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(decoded->type == COWRIE_G1_TYPE_BOOL && decoded->bool_val == false);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);

    cowrie_g1_value_free(v_true);
    cowrie_g1_value_free(v_false);
}

TEST(int64) {
    int64_t test_values[] = {0, 1, -1, 42, -42, 127, -128, 256, 1000000, -1000000};
    int num_values = sizeof(test_values) / sizeof(test_values[0]);

    for (int i = 0; i < num_values; i++) {
        cowrie_g1_value_t *v = cowrie_g1_int64(test_values[i]);
        assert(v != NULL);
        assert(v->type == COWRIE_G1_TYPE_INT64);
        assert(v->int64_val == test_values[i]);

        cowrie_g1_buf_t buf;
        cowrie_g1_encode(v, &buf);

        cowrie_g1_value_t *decoded;
        cowrie_g1_decode(buf.data, buf.len, &decoded);
        assert(decoded->type == COWRIE_G1_TYPE_INT64);
        assert(decoded->int64_val == test_values[i]);

        cowrie_g1_value_free(v);
        cowrie_g1_value_free(decoded);
        cowrie_g1_buf_free(&buf);
    }
}

TEST(string) {
    const char *test_str = "Hello, World!";
    cowrie_g1_value_t *v = cowrie_g1_string(test_str, strlen(test_str));
    assert(v != NULL);
    assert(v->type == COWRIE_G1_TYPE_STRING);
    assert(strcmp(v->string_val.data, test_str) == 0);

    cowrie_g1_buf_t buf;
    cowrie_g1_encode(v, &buf);

    cowrie_g1_value_t *decoded;
    cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(decoded->type == COWRIE_G1_TYPE_STRING);
    assert(strcmp(decoded->string_val.data, test_str) == 0);

    cowrie_g1_value_free(v);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(object) {
    cowrie_g1_value_t *obj = cowrie_g1_object(2);
    cowrie_g1_object_set(obj, "name", cowrie_g1_string("test", 4));
    cowrie_g1_object_set(obj, "count", cowrie_g1_int64(42));

    assert(obj->type == COWRIE_G1_TYPE_OBJECT);
    assert(obj->object_val.len == 2);

    cowrie_g1_buf_t buf;
    cowrie_g1_encode(obj, &buf);

    cowrie_g1_value_t *decoded;
    cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(decoded->type == COWRIE_G1_TYPE_OBJECT);
    assert(decoded->object_val.len == 2);

    cowrie_g1_value_free(obj);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(float64_array) {
    double data[] = {1.0, 2.5, 3.14159, -42.0};
    cowrie_g1_value_t *v = cowrie_g1_float64_array(data, 4);
    assert(v != NULL);
    assert(v->type == COWRIE_G1_TYPE_FLOAT64_ARRAY);
    assert(v->float64_array_val.len == 4);

    cowrie_g1_buf_t buf;
    cowrie_g1_encode(v, &buf);

    cowrie_g1_value_t *decoded;
    cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(decoded->type == COWRIE_G1_TYPE_FLOAT64_ARRAY);
    assert(decoded->float64_array_val.len == 4);

    for (int i = 0; i < 4; i++) {
        assert(decoded->float64_array_val.data[i] == data[i]);
    }

    cowrie_g1_value_free(v);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(empty_array) {
    cowrie_g1_value_t *arr = cowrie_g1_array(0);
    assert(arr != NULL);
    assert(arr->type == COWRIE_G1_TYPE_ARRAY);
    assert(arr->array_val.len == 0);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(arr, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_ARRAY);
    assert(decoded->array_val.len == 0);

    cowrie_g1_value_free(arr);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(empty_object) {
    cowrie_g1_value_t *obj = cowrie_g1_object(0);
    assert(obj != NULL);
    assert(obj->type == COWRIE_G1_TYPE_OBJECT);
    assert(obj->object_val.len == 0);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(obj, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_OBJECT);
    assert(decoded->object_val.len == 0);

    cowrie_g1_value_free(obj);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(empty_string) {
    cowrie_g1_value_t *v = cowrie_g1_string("", 0);
    assert(v != NULL);
    assert(v->type == COWRIE_G1_TYPE_STRING);
    assert(v->string_val.len == 0);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(v, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_STRING);
    assert(decoded->string_val.len == 0);

    cowrie_g1_value_free(v);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

int main(void) {
    printf("Running COWRIE Gen1 tests...\n");

    run_test_null();
    run_test_bool();
    run_test_int64();
    run_test_string();
    run_test_object();
    run_test_float64_array();
    run_test_empty_array();
    run_test_empty_object();
    run_test_empty_string();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
