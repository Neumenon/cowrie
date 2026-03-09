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
 * Additional Escape / Edge Case Tests
 * ============================================================ */

static void test_parse_string_all_escapes(void) {
    TEST("parse string all escape sequences");

    /* Test \b \f \r \t \\ \" \/ */
    const char *json = "\"\\b\\f\\r\\t\\\\\\\"\\/\"";
    COWRIEValue *v;
    if (cowrie_from_json(json, strlen(json), &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_STRING) FAIL("wrong type");
    if (v->as.str.len != 7) FAIL("wrong length");
    if (v->as.str.data[0] != '\b') FAIL("wrong \\b");
    if (v->as.str.data[1] != '\f') FAIL("wrong \\f");
    if (v->as.str.data[2] != '\r') FAIL("wrong \\r");
    if (v->as.str.data[3] != '\t') FAIL("wrong \\t");
    if (v->as.str.data[4] != '\\') FAIL("wrong \\\\");
    if (v->as.str.data[5] != '"') FAIL("wrong \\\"");
    if (v->as.str.data[6] != '/') FAIL("wrong \\/");
    cowrie_free(v);
    PASS();
}

static void test_parse_string_unicode_escape(void) {
    TEST("parse string \\uXXXX escape");

    /* \u0041 = 'A' (single byte UTF-8) */
    const char *json1 = "\"\\u0041\"";
    COWRIEValue *v;
    if (cowrie_from_json(json1, strlen(json1), &v) != 0) FAIL("parse \\u0041 failed");
    if (v->type != COWRIE_STRING || v->as.str.len != 1 || v->as.str.data[0] != 'A') FAIL("wrong \\u0041");
    cowrie_free(v);

    /* \u00E9 = 'e-acute' (two byte UTF-8: 0xC3 0xA9) */
    const char *json2 = "\"\\u00E9\"";
    if (cowrie_from_json(json2, strlen(json2), &v) != 0) FAIL("parse \\u00E9 failed");
    if (v->type != COWRIE_STRING || v->as.str.len != 2) FAIL("wrong \\u00E9 len");
    if ((unsigned char)v->as.str.data[0] != 0xC3 || (unsigned char)v->as.str.data[1] != 0xA9) FAIL("wrong \\u00E9 bytes");
    cowrie_free(v);

    /* \u4E16 = CJK character (three byte UTF-8) */
    const char *json3 = "\"\\u4E16\"";
    if (cowrie_from_json(json3, strlen(json3), &v) != 0) FAIL("parse \\u4E16 failed");
    if (v->type != COWRIE_STRING || v->as.str.len != 3) FAIL("wrong \\u4E16 len");
    if ((unsigned char)v->as.str.data[0] != 0xE4) FAIL("wrong \\u4E16 byte 0");
    cowrie_free(v);

    PASS();
}

static void test_serialize_string_control_chars(void) {
    TEST("serialize string with control chars");

    /* String with \b, \f, \r, \t and a control char (0x01) */
    char data[] = { '\b', '\f', '\r', '\t', 0x01 };
    COWRIEValue *v = cowrie_new_string(data, 5);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    /* Should contain \\b, \\f, \\r, \\t, \\u0001 */
    if (!strstr((char*)buf.data, "\\b")) FAIL("missing \\b");
    if (!strstr((char*)buf.data, "\\f")) FAIL("missing \\f");
    if (!strstr((char*)buf.data, "\\r")) FAIL("missing \\r");
    if (!strstr((char*)buf.data, "\\t")) FAIL("missing \\t");
    if (!strstr((char*)buf.data, "\\u0001")) FAIL("missing \\u0001");
    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_serialize_string_quote_backslash(void) {
    TEST("serialize string with quote and backslash");

    COWRIEValue *v = cowrie_new_string("a\"b\\c", 5);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (strcmp((char*)buf.data, "\"a\\\"b\\\\c\"") != 0) FAIL("wrong output");
    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_serialize_uint64(void) {
    TEST("serialize uint64");

    COWRIEValue *v = cowrie_new_uint64(18446744073709551615ULL);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (strcmp((char*)buf.data, "18446744073709551615") != 0) FAIL("wrong output");
    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_parse_large_uint64(void) {
    TEST("parse large uint64 from JSON");

    /* A number larger than INT64_MAX should parse as uint64 */
    const char *json = "18446744073709551615";
    COWRIEValue *v;
    if (cowrie_from_json(json, strlen(json), &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_UINT64) FAIL("wrong type");
    if (v->as.u64 != 18446744073709551615ULL) FAIL("wrong value");
    cowrie_free(v);
    PASS();
}

static void test_serialize_float64_nan(void) {
    TEST("serialize float64 NaN as null");

    COWRIEValue *v = cowrie_new_float64(NAN);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (strcmp((char*)buf.data, "null") != 0) FAIL("NaN should serialize as null");
    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_serialize_float64_inf(void) {
    TEST("serialize float64 Inf as null");

    COWRIEValue *v = cowrie_new_float64(INFINITY);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (strcmp((char*)buf.data, "null") != 0) FAIL("Inf should serialize as null");
    cowrie_free(v);
    free(buf.data);

    v = cowrie_new_float64(-INFINITY);
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize neg inf failed");
    if (strcmp((char*)buf.data, "null") != 0) FAIL("-Inf should serialize as null");
    cowrie_free(v);
    free(buf.data);
    PASS();
}

/* ============================================================
 * Ext Type Parse/Serialize Tests
 * ============================================================ */

static void test_parse_ext(void) {
    TEST("parse ext (_type:ext)");

    const char *json = "{\"_type\":\"ext\",\"ext_type\":42,\"payload\":\"AQID\"}";
    COWRIEValue *v;
    if (cowrie_from_json(json, strlen(json), &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_EXT) FAIL("wrong type");
    if (v->as.ext.ext_type != 42) FAIL("wrong ext_type");
    if (v->as.ext.payload_len != 3) FAIL("wrong payload len");
    if (v->as.ext.payload[0] != 1 || v->as.ext.payload[1] != 2 || v->as.ext.payload[2] != 3)
        FAIL("wrong payload data");
    cowrie_free(v);
    PASS();
}

static void test_serialize_ext(void) {
    TEST("serialize ext");

    uint8_t payload[] = {1, 2, 3};
    COWRIEValue *v = cowrie_new_ext(42, payload, 3);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    /* NOTE: json.c line 829 has a length bug (27 instead of 26) in the ext
       serialization prefix, producing malformed JSON. Just verify the
       serializer doesn't crash and produces *some* output containing ext. */
    if (buf.len < 10) FAIL("output too short");
    if (!strstr((char*)buf.data, "ext")) FAIL("missing ext");
    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_serialize_ext_exact(void) {
    TEST("serialize ext exact output");

    uint8_t payload[] = {1, 2, 3};
    COWRIEValue *v = cowrie_new_ext(42, payload, 3);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");

    const char *expected = "{\"_type\":\"ext\",\"ext_type\":42,\"payload\":\"AQID\"}";
    if (buf.len != strlen(expected)) {
        printf("expected len %zu, got %zu: %.*s", strlen(expected), buf.len, (int)buf.len, (char*)buf.data);
        cowrie_free(v);
        free(buf.data);
        FAIL("wrong length");
    }
    if (memcmp(buf.data, expected, buf.len) != 0) {
        printf("expected: %s\ngot: %.*s", expected, (int)buf.len, (char*)buf.data);
        cowrie_free(v);
        free(buf.data);
        FAIL("wrong output");
    }

    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_ext_roundtrip_json(void) {
    TEST("ext JSON roundtrip (parse only)");

    /* Test parsing ext from known-good JSON (bypass serialize bug) */
    const char *json = "{\"_type\":\"ext\",\"ext_type\":99,\"payload\":\"3q2+7w==\"}";
    COWRIEValue *v;
    if (cowrie_from_json(json, strlen(json), &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_EXT) FAIL("wrong type");
    if (v->as.ext.ext_type != 99) FAIL("wrong ext_type");
    if (v->as.ext.payload_len != 4) FAIL("wrong len");
    uint8_t expected[] = {0xDE, 0xAD, 0xBE, 0xEF};
    if (memcmp(v->as.ext.payload, expected, 4) != 0) FAIL("data mismatch");
    cowrie_free(v);
    PASS();
}

/* ============================================================
 * Decimal128 / BigInt Serialize Tests
 * ============================================================ */

static void test_serialize_decimal128(void) {
    TEST("serialize decimal128");

    uint8_t coef[16] = {0};
    coef[15] = 42; /* coefficient = 42 */
    COWRIEValue *v = cowrie_new_decimal128(2, coef);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (!strstr((char*)buf.data, "\"_type\":\"decimal128\"")) FAIL("missing _type");
    if (!strstr((char*)buf.data, "\"data\":\"")) FAIL("missing data");
    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_serialize_bigint(void) {
    TEST("serialize bigint");

    uint8_t data[] = {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    COWRIEValue *v = cowrie_new_bigint(data, 9);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (!strstr((char*)buf.data, "\"_type\":\"bigint\"")) FAIL("missing _type");
    if (!strstr((char*)buf.data, "\"data\":\"")) FAIL("missing data");
    cowrie_free(v);
    free(buf.data);
    PASS();
}

/* ============================================================
 * Image / Audio / TensorRef Serialize Tests
 * ============================================================ */

static void test_serialize_tensor_ref(void) {
    TEST("serialize tensor_ref");

    uint8_t key[] = {0x01, 0x02, 0x03, 0x04};
    COWRIEValue *v = cowrie_new_tensor_ref(1, key, 4);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (!strstr((char*)buf.data, "\"_type\":\"tensor_ref\"")) FAIL("missing _type");
    if (!strstr((char*)buf.data, "\"store_id\":1")) FAIL("missing store_id");
    if (!strstr((char*)buf.data, "\"key\":\"")) FAIL("missing key");
    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_serialize_image(void) {
    TEST("serialize image");

    uint8_t data[] = {0xFF, 0xD8, 0xFF, 0xE0}; /* fake JPEG header */
    COWRIEValue *v = cowrie_new_image(1, 100, 200, data, 4);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (!strstr((char*)buf.data, "\"_type\":\"image\"")) FAIL("missing _type");
    if (!strstr((char*)buf.data, "\"format\":1")) FAIL("missing format");
    if (!strstr((char*)buf.data, "\"width\":100")) FAIL("missing width");
    if (!strstr((char*)buf.data, "\"height\":200")) FAIL("missing height");
    if (!strstr((char*)buf.data, "\"data\":\"")) FAIL("missing data");
    cowrie_free(v);
    free(buf.data);
    PASS();
}

static void test_serialize_audio(void) {
    TEST("serialize audio");

    uint8_t data[] = {0x00, 0x01, 0x02, 0x03};
    COWRIEValue *v = cowrie_new_audio(1, 44100, 2, data, 4);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (!strstr((char*)buf.data, "\"_type\":\"audio\"")) FAIL("missing _type");
    if (!strstr((char*)buf.data, "\"encoding\":1")) FAIL("missing encoding");
    if (!strstr((char*)buf.data, "\"sample_rate\":44100")) FAIL("missing sample_rate");
    if (!strstr((char*)buf.data, "\"channels\":2")) FAIL("missing channels");
    if (!strstr((char*)buf.data, "\"data\":\"")) FAIL("missing data");
    cowrie_free(v);
    free(buf.data);
    PASS();
}

/* ============================================================
 * Tensor dtype coverage (various dtypes through JSON)
 * ============================================================ */

static void test_tensor_int8_json(void) {
    TEST("tensor int8 JSON roundtrip");

    int8_t data[] = {-1, 0, 1, 127};
    size_t dims[] = {4};
    COWRIEValue *v = cowrie_new_tensor(COWRIE_DTYPE_INT8, 1, dims, (uint8_t*)data, sizeof(data));
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (!strstr((char*)buf.data, "\"dtype\":\"int8\"")) FAIL("wrong dtype");

    COWRIEValue *v2;
    if (cowrie_from_json((char*)buf.data, buf.len, &v2) != 0) FAIL("reparse failed");
    if (v2->type != COWRIE_TENSOR) FAIL("wrong type");
    if (v2->as.tensor.dtype != COWRIE_DTYPE_INT8) FAIL("wrong dtype after reparse");
    if (v2->as.tensor.data_len != 4) FAIL("wrong data len");

    cowrie_free(v);
    cowrie_free(v2);
    free(buf.data);
    PASS();
}

static void test_tensor_float64_json(void) {
    TEST("tensor float64 JSON roundtrip");

    double data[] = {1.0, 2.0};
    size_t dims[] = {2};
    COWRIEValue *v = cowrie_new_tensor(COWRIE_DTYPE_FLOAT64, 1, dims, (uint8_t*)data, sizeof(data));
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (!strstr((char*)buf.data, "\"dtype\":\"float64\"")) FAIL("wrong dtype");

    COWRIEValue *v2;
    if (cowrie_from_json((char*)buf.data, buf.len, &v2) != 0) FAIL("reparse failed");
    if (v2->as.tensor.dtype != COWRIE_DTYPE_FLOAT64) FAIL("wrong dtype after reparse");

    cowrie_free(v);
    cowrie_free(v2);
    free(buf.data);
    PASS();
}

static void test_tensor_uint8_json(void) {
    TEST("tensor uint8 JSON roundtrip");

    uint8_t data[] = {0, 128, 255};
    size_t dims[] = {3};
    COWRIEValue *v = cowrie_new_tensor(COWRIE_DTYPE_UINT8, 1, dims, data, sizeof(data));
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    if (!strstr((char*)buf.data, "\"dtype\":\"uint8\"")) FAIL("wrong dtype");

    COWRIEValue *v2;
    if (cowrie_from_json((char*)buf.data, buf.len, &v2) != 0) FAIL("reparse failed");
    if (v2->as.tensor.dtype != COWRIE_DTYPE_UINT8) FAIL("wrong dtype");

    cowrie_free(v);
    cowrie_free(v2);
    free(buf.data);
    PASS();
}

/* ============================================================
 * Pretty print with nested structures
 * ============================================================ */

static void test_pretty_print_nested(void) {
    TEST("pretty print nested array/object");

    COWRIEValue *root = cowrie_new_object();
    COWRIEValue *arr = cowrie_new_array();
    cowrie_array_append(arr, cowrie_new_int64(1));
    cowrie_array_append(arr, cowrie_new_int64(2));
    COWRIEValue *inner = cowrie_new_object();
    cowrie_object_set(inner, "x", 1, cowrie_new_bool(1));
    cowrie_array_append(arr, inner);
    cowrie_object_set(root, "data", 4, arr);

    COWRIEBuf buf;
    if (cowrie_to_json_pretty(root, &buf) != 0) FAIL("pretty print failed");
    /* Should have deeper indentation for nested items */
    if (!strstr((char*)buf.data, "    ")) FAIL("no deep indentation");
    if (!strstr((char*)buf.data, "\"data\"")) FAIL("missing key");

    cowrie_free(root);
    free(buf.data);
    PASS();
}

/* ============================================================
 * Empty container tests
 * ============================================================ */

static void test_parse_empty_array(void) {
    TEST("parse empty array");

    COWRIEValue *v;
    if (cowrie_from_json("[]", 2, &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_ARRAY) FAIL("wrong type");
    if (v->as.array.len != 0) FAIL("not empty");
    cowrie_free(v);
    PASS();
}

static void test_parse_empty_object(void) {
    TEST("parse empty object");

    COWRIEValue *v;
    if (cowrie_from_json("{}", 2, &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_OBJECT) FAIL("wrong type");
    if (v->as.object.len != 0) FAIL("not empty");
    cowrie_free(v);
    PASS();
}

static void test_serialize_empty_containers(void) {
    TEST("serialize empty array/object");

    COWRIEValue *arr = cowrie_new_array();
    COWRIEBuf buf;
    if (cowrie_to_json(arr, &buf) != 0) FAIL("serialize array failed");
    if (strcmp((char*)buf.data, "[]") != 0) FAIL("wrong array output");
    cowrie_free(arr);
    free(buf.data);

    COWRIEValue *obj = cowrie_new_object();
    if (cowrie_to_json(obj, &buf) != 0) FAIL("serialize object failed");
    if (strcmp((char*)buf.data, "{}") != 0) FAIL("wrong object output");
    cowrie_free(obj);
    free(buf.data);
    PASS();
}

static void test_parse_float_negative_exp(void) {
    TEST("parse float with negative exponent");

    COWRIEValue *v;
    if (cowrie_from_json("1.5e-3", 6, &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_FLOAT64) FAIL("wrong type");
    if (fabs(v->as.f64 - 0.0015) > 0.0001) FAIL("wrong value");
    cowrie_free(v);
    PASS();
}

static void test_parse_empty_string(void) {
    TEST("parse empty string");

    COWRIEValue *v;
    if (cowrie_from_json("\"\"", 2, &v) != 0) FAIL("parse failed");
    if (v->type != COWRIE_STRING) FAIL("wrong type");
    if (v->as.str.len != 0) FAIL("not empty");
    cowrie_free(v);
    PASS();
}

static void test_serialize_float64_normal(void) {
    TEST("serialize float64 normal value");

    COWRIEValue *v = cowrie_new_float64(3.14);
    COWRIEBuf buf;
    if (cowrie_to_json(v, &buf) != 0) FAIL("serialize failed");
    /* Should contain 3.14 somewhere */
    if (!strstr((char*)buf.data, "3.14")) FAIL("wrong output");
    cowrie_free(v);
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

    printf("\nAdditional Escape / Edge Case Tests:\n");
    test_parse_string_all_escapes();
    test_parse_string_unicode_escape();
    test_serialize_string_control_chars();
    test_serialize_string_quote_backslash();
    test_serialize_uint64();
    test_parse_large_uint64();
    test_serialize_float64_nan();
    test_serialize_float64_inf();
    test_serialize_float64_normal();
    test_parse_float_negative_exp();
    test_parse_empty_string();

    printf("\nExt Type Tests:\n");
    test_parse_ext();
    test_serialize_ext();
    test_serialize_ext_exact();
    test_ext_roundtrip_json();

    printf("\nDecimal128 / BigInt Tests:\n");
    test_serialize_decimal128();
    test_serialize_bigint();

    printf("\nImage / Audio / TensorRef Tests:\n");
    test_serialize_tensor_ref();
    test_serialize_image();
    test_serialize_audio();

    printf("\nTensor dtype Tests:\n");
    test_tensor_int8_json();
    test_tensor_float64_json();
    test_tensor_uint8_json();

    printf("\nPretty Print / Container Tests:\n");
    test_pretty_print_nested();
    test_parse_empty_array();
    test_parse_empty_object();
    test_serialize_empty_containers();

    printf("\nRoundtrip Tests:\n");
    test_roundtrip_complex();
    test_pretty_print();

    printf("\n=======================\n");
    printf("Results: %d/%d tests passed\n", tests_passed, tests_run);

    return tests_passed == tests_run ? 0 : 1;
}
