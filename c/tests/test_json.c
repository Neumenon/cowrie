/*
 * COWRIE JSON Bridge Tests
 */

#include "../include/cowrie_gen2.h"
#include "../include/cowrie_json.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) do { \
    printf("  Testing %s...", name); \
    tests_run++; \
} while(0)

#define PASS() do { \
    printf(" PASSED\n"); \
    tests_passed++; \
} while(0)

#define FAIL(msg) do { \
    printf(" FAILED: %s\n", msg); \
    return; \
} while(0)

/* ============================================================
 * Base64 Tests
 * ============================================================ */

static void test_base64_encode(void) {
    TEST("base64 encode");

    const uint8_t data[] = { 0x00, 0x01, 0x02, 0x03 };
    COWRIEBuf buf;

    if (cowrie_base64_encode(data, 4, &buf) != 0) FAIL("encode failed");
    if (buf.len != 8) FAIL("wrong length");
    if (strcmp((char*)buf.data, "AAECAw==") != 0) FAIL("wrong output");

    free(buf.data);
    PASS();
}

static void test_base64_decode(void) {
    TEST("base64 decode");

    const char *b64 = "AAECAw==";
    COWRIEBuf buf;

    if (cowrie_base64_decode(b64, strlen(b64), &buf) != 0) FAIL("decode failed");
    if (buf.len != 4) FAIL("wrong length");
    if (buf.data[0] != 0x00 || buf.data[1] != 0x01 ||
        buf.data[2] != 0x02 || buf.data[3] != 0x03) FAIL("wrong data");

    free(buf.data);
    PASS();
}

static void test_base64_roundtrip(void) {
    TEST("base64 roundtrip");

    const uint8_t original[] = "Hello, World! This is a test of base64 encoding.";
    COWRIEBuf encoded, decoded;

    if (cowrie_base64_encode(original, sizeof(original) - 1, &encoded) != 0) FAIL("encode failed");
    if (cowrie_base64_decode((char*)encoded.data, encoded.len, &decoded) != 0) {
        free(encoded.data);
        FAIL("decode failed");
    }

    if (decoded.len != sizeof(original) - 1) {
        free(encoded.data);
        free(decoded.data);
        FAIL("wrong length");
    }
    if (memcmp(decoded.data, original, decoded.len) != 0) {
        free(encoded.data);
        free(decoded.data);
        FAIL("data mismatch");
    }

    free(encoded.data);
    free(decoded.data);
    PASS();
}

/* ============================================================
 * JSON Parsing Tests
 * ============================================================ */

static void test_parse_null(void) {
    TEST("parse null");

    COWRIEValue *v;
    if (cowrie_from_json("null", 4, &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_NULL) FAIL("wrong type");

    cowrie_free(v);
    PASS();
}

static void test_parse_bool(void) {
    TEST("parse bool");

    COWRIEValue *v;
    if (cowrie_from_json("true", 4, &v) != 0) FAIL("parse true failed");
    if (v->type != COWRIE_BOOL || !v->as.boolean) FAIL("wrong true value");
    cowrie_free(v);

    if (cowrie_from_json("false", 5, &v) != 0) FAIL("parse false failed");
    if (v->type != COWRIE_BOOL || v->as.boolean) FAIL("wrong false value");
    cowrie_free(v);

    PASS();
}

static void test_parse_int(void) {
    TEST("parse int");

    COWRIEValue *v;
    if (cowrie_from_json("42", 2, &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_INT64 || v->as.i64 != 42) FAIL("wrong value");
    cowrie_free(v);

    if (cowrie_from_json("-123", 4, &v) != 0) FAIL("parse negative failed");
    if (v->type != COWRIE_INT64 || v->as.i64 != -123) FAIL("wrong negative value");
    cowrie_free(v);

    PASS();
}

static void test_parse_float(void) {
    TEST("parse float");

    COWRIEValue *v;
    if (cowrie_from_json("3.14159", 7, &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_FLOAT64) FAIL("wrong type");
    if (fabs(v->as.f64 - 3.14159) > 0.00001) FAIL("wrong value");
    cowrie_free(v);

    if (cowrie_from_json("1.23e10", 7, &v) != 0) FAIL("parse exp failed");
    if (v->type != COWRIE_FLOAT64) FAIL("wrong exp type");
    if (fabs(v->as.f64 - 1.23e10) > 1e6) FAIL("wrong exp value");
    cowrie_free(v);

    PASS();
}

static void test_parse_string(void) {
    TEST("parse string");

    COWRIEValue *v;
    if (cowrie_from_json("\"hello\"", 7, &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_STRING) FAIL("wrong type");
    if (v->as.str.len != 5) FAIL("wrong length");
    if (memcmp(v->as.str.data, "hello", 5) != 0) FAIL("wrong content");
    cowrie_free(v);

    PASS();
}

static void test_parse_string_escape(void) {
    TEST("parse string with escapes");

    COWRIEValue *v;
    if (cowrie_from_json("\"hello\\nworld\"", 14, &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_STRING) FAIL("wrong type");
    if (v->as.str.len != 11) FAIL("wrong length");
    if (memcmp(v->as.str.data, "hello\nworld", 11) != 0) FAIL("wrong content");
    cowrie_free(v);

    PASS();
}

static void test_parse_array(void) {
    TEST("parse array");

    COWRIEValue *v;
    if (cowrie_from_json("[1, 2, 3]", 9, &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_ARRAY) FAIL("wrong type");
    if (v->as.array.len != 3) FAIL("wrong length");
    if (v->as.array.items[0]->as.i64 != 1) FAIL("wrong item 0");
    if (v->as.array.items[1]->as.i64 != 2) FAIL("wrong item 1");
    if (v->as.array.items[2]->as.i64 != 3) FAIL("wrong item 2");
    cowrie_free(v);

    PASS();
}

static void test_parse_object(void) {
    TEST("parse object");

    const char *json = "{\"name\": \"Alice\", \"age\": 30}";
    COWRIEValue *v;
    if (cowrie_from_json(json, strlen(json), &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_OBJECT) FAIL("wrong type");

    COWRIEValue *name = cowrie_object_get(v, "name", 4);
    if (!name || name->type != COWRIE_STRING) FAIL("missing name");
    if (memcmp(name->as.str.data, "Alice", 5) != 0) FAIL("wrong name");

    COWRIEValue *age = cowrie_object_get(v, "age", 3);
    if (!age || age->type != COWRIE_INT64 || age->as.i64 != 30) FAIL("wrong age");

    cowrie_free(v);
    PASS();
}

static void test_parse_nested(void) {
    TEST("parse nested structure");

    const char *json = "{\"users\": [{\"name\": \"Alice\"}, {\"name\": \"Bob\"}]}";
    COWRIEValue *v;
    if (cowrie_from_json(json, strlen(json), &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_OBJECT) FAIL("wrong root type");

    COWRIEValue *users = cowrie_object_get(v, "users", 5);
    if (!users || users->type != COWRIE_ARRAY) FAIL("missing users");
    if (users->as.array.len != 2) FAIL("wrong user count");

    COWRIEValue *user0 = users->as.array.items[0];
    COWRIEValue *name0 = cowrie_object_get(user0, "name", 4);
    if (!name0 || memcmp(name0->as.str.data, "Alice", 5) != 0) FAIL("wrong user 0");

    cowrie_free(v);
    PASS();
}

/* ============================================================
 * Special Type Parsing Tests
 * ============================================================ */

static void test_parse_bytes(void) {
    TEST("parse bytes (_type:bytes)");

    const char *json = "{\"_type\":\"bytes\",\"data\":\"SGVsbG8=\"}";
    COWRIEValue *v;
    if (cowrie_from_json(json, strlen(json), &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_BYTES) FAIL("wrong type");
    if (v->as.bytes.len != 5) FAIL("wrong length");
    if (memcmp(v->as.bytes.data, "Hello", 5) != 0) FAIL("wrong data");

    cowrie_free(v);
    PASS();
}

static void test_parse_datetime(void) {
    TEST("parse datetime (_type:datetime)");

    const char *json = "{\"_type\":\"datetime\",\"nanos\":1234567890123456789}";
    COWRIEValue *v;
    if (cowrie_from_json(json, strlen(json), &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_DATETIME64) FAIL("wrong type");
    if (v->as.datetime64 != 1234567890123456789LL) FAIL("wrong nanos");

    cowrie_free(v);
    PASS();
}

static void test_parse_uuid(void) {
    TEST("parse uuid (_type:uuid)");

    const char *json = "{\"_type\":\"uuid\",\"hex\":\"550e8400-e29b-41d4-a716-446655440000\"}";
    COWRIEValue *v;
    if (cowrie_from_json(json, strlen(json), &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_UUID128) FAIL("wrong type");

    /* Check UUID bytes */
    uint8_t expected[] = {0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
                          0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00};
    if (memcmp(v->as.uuid, expected, 16) != 0) FAIL("wrong uuid bytes");

    cowrie_free(v);
    PASS();
}

static void test_parse_tensor(void) {
    TEST("parse tensor (_type:tensor)");

    /* 2x2 float32 tensor with values [1.0, 2.0, 3.0, 4.0] */
    const char *json = "{\"_type\":\"tensor\",\"dtype\":\"float32\",\"dims\":[2,2],\"data\":\"AACAPwAAAEAAAEBAAACAQA==\"}";
    COWRIEValue *v;
    if (cowrie_from_json(json, strlen(json), &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_TENSOR) FAIL("wrong type");
    if (v->as.tensor.dtype != COWRIE_DTYPE_FLOAT32) FAIL("wrong dtype");
    if (v->as.tensor.rank != 2) FAIL("wrong rank");
    if (v->as.tensor.dims[0] != 2 || v->as.tensor.dims[1] != 2) FAIL("wrong dims");
    if (v->as.tensor.data_len != 16) FAIL("wrong data length");

    /* Verify data values */
    size_t count;
    const float *floats = cowrie_tensor_view_float32(&v->as.tensor, &count);
    if (!floats || count != 4) FAIL("view failed");
    if (fabs(floats[0] - 1.0f) > 0.001f) FAIL("wrong value 0");
    if (fabs(floats[1] - 2.0f) > 0.001f) FAIL("wrong value 1");
    if (fabs(floats[2] - 3.0f) > 0.001f) FAIL("wrong value 2");
    if (fabs(floats[3] - 4.0f) > 0.001f) FAIL("wrong value 3");

    cowrie_free(v);
    PASS();
}

/* ============================================================
 * JSON Serialization Tests
 * ============================================================ */

static void test_serialize_null(void) {
    TEST("serialize null");

    COWRIEValue *v = cowrie_new_null();
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (strcmp((char*)buf.data, "null") != 0) FAIL("wrong output");

    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_serialize_bool(void) {
    TEST("serialize bool");

    COWRIEValue *v = cowrie_new_bool(1);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize true failed");
    if (strcmp((char*)buf.data, "true") != 0) FAIL("wrong true output");
    cowrie_free(v);
    free(buf.data);

    v = cowrie_new_bool(0);
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize false failed");
    if (strcmp((char*)buf.data, "false") != 0) FAIL("wrong false output");
    cowrie_free(v);
    free(buf.data);

    PASS();
}

static void test_serialize_int(void) {
    TEST("serialize int");

    COWRIEValue *v = cowrie_new_int64(-42);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (strcmp((char*)buf.data, "-42") != 0) FAIL("wrong output");

    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_serialize_string(void) {
    TEST("serialize string");

    COWRIEValue *v = cowrie_new_string("hello\nworld", 11);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (strcmp((char*)buf.data, "\"hello\\nworld\"") != 0) FAIL("wrong output");

    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_serialize_array(void) {
    TEST("serialize array");

    COWRIEValue *arr = cowrie_new_array();
    cowrie_array_append(arr, cowrie_new_int64(1));
    cowrie_array_append(arr, cowrie_new_int64(2));
    cowrie_array_append(arr, cowrie_new_int64(3));

    COWRIEBuf buf;
    if (cowrie_to_json(arr, &buf) != 0) FAIL("serialize failed");
    if (strcmp((char*)buf.data, "[1,2,3]") != 0) FAIL("wrong output");

    cowrie_free(arr);
    free(buf.data);
    PASS();
}

static void test_serialize_object(void) {
    TEST("serialize object");

    COWRIEValue *obj = cowrie_new_object();
    cowrie_object_set(obj, "a", 1, cowrie_new_int64(1));

    COWRIEBuf buf;
    if (cowrie_to_json(obj, &buf) != 0) FAIL("serialize failed");
    if (strcmp((char*)buf.data, "{\"a\":1}") != 0) FAIL("wrong output");

    cowrie_free(obj);
    free(buf.data);
    PASS();
}

static void test_serialize_bytes(void) {
    TEST("serialize bytes");

    COWRIEValue *v = cowrie_new_bytes((const uint8_t*)"Hello", 5);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (strcmp((char*)buf.data, "{\"_type\":\"bytes\",\"data\":\"SGVsbG8=\"}") != 0) FAIL("wrong output");

    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_serialize_datetime(void) {
    TEST("serialize datetime");

    COWRIEValue *v = cowrie_new_datetime64(1234567890123456789LL);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (strcmp((char*)buf.data, "{\"_type\":\"datetime\",\"nanos\":1234567890123456789}") != 0) FAIL("wrong output");

    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_serialize_uuid(void) {
    TEST("serialize uuid");

    uint8_t uuid[] = {0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
                      0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00};
    COWRIEValue *v = cowrie_new_uuid128(uuid);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (strcmp((char*)buf.data, "{\"_type\":\"uuid\",\"hex\":\"550e8400-e29b-41d4-a716-446655440000\"}") != 0) {
        printf("\nGot: %s\n", (char*)buf.data);
        FAIL("wrong output");
    }

    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_serialize_tensor(void) {
    TEST("serialize tensor");

    size_t dims[] = {2, 2};
    float data[] = {1.0f, 2.0f, 3.0f, 4.0f};
    COWRIEValue *v = cowrie_new_tensor(COWRIE_DTYPE_FLOAT32, 2, dims, (uint8_t*)data, sizeof(data));

    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");

    /* Parse it back to verify */
    COWRIEValue *v2;
    if (cowrie_from_json((char*)buf.data, buf.len, &v2) != 0) FAIL("reparse failed");
    if (v2->type != COWRIE_TENSOR) FAIL("wrong type after reparse");
    if (v2->as.tensor.dtype != COWRIE_DTYPE_FLOAT32) FAIL("wrong dtype after reparse");
    if (v2->as.tensor.rank != 2) FAIL("wrong rank after reparse");

    cowrie_free(v);
    cowrie_free(v2);
    free(buf.data);
    PASS();
}

/* ============================================================
 * Roundtrip Tests
 * ============================================================ */

static void test_roundtrip_complex(void) {
    TEST("roundtrip complex structure");

    /* Build a complex value */
    COWRIEValue *root = cowrie_new_object();
    cowrie_object_set(root, "name", 4, cowrie_new_string("test", 4));
    cowrie_object_set(root, "count", 5, cowrie_new_int64(42));
    cowrie_object_set(root, "enabled", 7, cowrie_new_bool(1));
    cowrie_object_set(root, "ratio", 5, cowrie_new_float64(3.14159));

    COWRIEValue *arr = cowrie_new_array();
    cowrie_array_append(arr, cowrie_new_int64(1));
    cowrie_array_append(arr, cowrie_new_int64(2));
    cowrie_array_append(arr, cowrie_new_int64(3));
    cowrie_object_set(root, "items", 5, arr);

    /* Serialize to JSON */
    COWRIEBuf buf;
    if (cowrie_to_json(root, &buf) != 0) FAIL("serialize failed");

    /* Parse back */
    COWRIEValue *reparsed;
    if (cowrie_from_json((char*)buf.data, buf.len, &reparsed) != 0) FAIL("reparse failed");

    /* Verify */
    COWRIEValue *name = cowrie_object_get(reparsed, "name", 4);
    if (!name || name->type != COWRIE_STRING || memcmp(name->as.str.data, "test", 4) != 0) FAIL("name mismatch");

    COWRIEValue *count = cowrie_object_get(reparsed, "count", 5);
    if (!count || count->type != COWRIE_INT64 || count->as.i64 != 42) FAIL("count mismatch");

    COWRIEValue *items = cowrie_object_get(reparsed, "items", 5);
    if (!items || items->type != COWRIE_ARRAY || items->as.array.len != 3) FAIL("items mismatch");

    cowrie_free(root);
    cowrie_free(reparsed);
    free(buf.data);
    PASS();
}

static void test_pretty_print(void) {
    TEST("pretty print");

    COWRIEValue *obj = cowrie_new_object();
    cowrie_object_set(obj, "name", 4, cowrie_new_string("Alice", 5));
    cowrie_object_set(obj, "age", 3, cowrie_new_int64(30));

    COWRIEBuf buf;
    if (cowrie_to_json_pretty(obj, &buf) != 0) FAIL("pretty print failed");

    /* Should have newlines and indentation */
    if (strstr((char*)buf.data, "\n") == NULL) FAIL("no newlines");
    if (strstr((char*)buf.data, "  ") == NULL) FAIL("no indentation");

    cowrie_free(obj);
    free(buf.data);
    PASS();
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    printf("COWRIE JSON Bridge Tests\n");
    printf("=======================\n\n");

    printf("Base64 Tests:\n");
    test_base64_encode();
    test_base64_decode();
    test_base64_roundtrip();

    printf("\nJSON Parsing Tests:\n");
    test_parse_null();
    test_parse_bool();
    test_parse_int();
    test_parse_float();
    test_parse_string();
    test_parse_string_escape();
    test_parse_array();
    test_parse_object();
    test_parse_nested();

    printf("\nSpecial Type Parsing Tests:\n");
    test_parse_bytes();
    test_parse_datetime();
    test_parse_uuid();
    test_parse_tensor();

    printf("\nJSON Serialization Tests:\n");
    test_serialize_null();
    test_serialize_bool();
    test_serialize_int();
    test_serialize_string();
    test_serialize_array();
    test_serialize_object();
    test_serialize_bytes();
    test_serialize_datetime();
    test_serialize_uuid();
    test_serialize_tensor();

    printf("\nRoundtrip Tests:\n");
    test_roundtrip_complex();
    test_pretty_print();

    printf("\n=======================\n");
    printf("Results: %d/%d tests passed\n", tests_passed, tests_run);

    return tests_passed == tests_run ? 0 : 1;
}
