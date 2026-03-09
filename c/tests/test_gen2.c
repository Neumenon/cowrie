/*
 * COWRIE Gen2 Test Suite
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

static int read_file(const char *path, char **out, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return 0; }
    long len = ftell(f);
    if (len < 0) { fclose(f); return 0; }
    if (fseek(f, 0, SEEK_SET) != 0) { fclose(f); return 0; }

    char *buf = (char *)malloc((size_t)len + 1);
    if (!buf) { fclose(f); return 0; }
    size_t n = fread(buf, 1, (size_t)len, f);
    fclose(f);
    if (n != (size_t)len) { free(buf); return 0; }
    buf[len] = '\0';
    *out = buf;
    *out_len = (size_t)len;
    return 1;
}

static int build_repo_root(char *buf, size_t buf_len) {
    const char *file = __FILE__;
    const char *marker = "/c/tests/";
    const char *pos = strstr(file, marker);
    if (!pos) return 0;
    size_t len = (size_t)(pos - file);
    if (len + 1 > buf_len) return 0;
    memcpy(buf, file, len);
    buf[len] = '\0';
    return 1;
}

/* ============================================================
 * Basic Type Tests
 * ============================================================ */

static int test_null_roundtrip(void) {
    COWRIEValue *v = cowrie_new_null();
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);
    ASSERT(buf.len > 4);  /* Header + at least one byte */

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);
    ASSERT(decoded->type == COWRIE_NULL);

    cowrie_free(v);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_bool_roundtrip(void) {
    COWRIEValue *t = cowrie_new_bool(1);
    COWRIEValue *f = cowrie_new_bool(0);
    ASSERT(t != NULL && f != NULL);
    ASSERT(t->as.boolean == 1);
    ASSERT(f->as.boolean == 0);

    COWRIEBuf buf_t, buf_f;
    ASSERT(cowrie_encode(t, &buf_t) == 0);
    ASSERT(cowrie_encode(f, &buf_f) == 0);

    COWRIEValue *dec_t, *dec_f;
    ASSERT(cowrie_decode(buf_t.data, buf_t.len, &dec_t) == 0);
    ASSERT(cowrie_decode(buf_f.data, buf_f.len, &dec_f) == 0);
    ASSERT(dec_t->type == COWRIE_BOOL && dec_t->as.boolean == 1);
    ASSERT(dec_f->type == COWRIE_BOOL && dec_f->as.boolean == 0);

    cowrie_free(t);
    cowrie_free(f);
    cowrie_free(dec_t);
    cowrie_free(dec_f);
    cowrie_buf_free(&buf_t);
    cowrie_buf_free(&buf_f);
    return 1;
}

static int test_int64_roundtrip(void) {
    int64_t values[] = {0, 1, -1, 127, -128, 32767, -32768,
                        2147483647LL, -2147483648LL,
                        9223372036854775807LL, -9223372036854775807LL};
    int n = sizeof(values) / sizeof(values[0]);

    for (int i = 0; i < n; i++) {
        COWRIEValue *v = cowrie_new_int64(values[i]);
        ASSERT(v != NULL);
        ASSERT(v->as.i64 == values[i]);

        COWRIEBuf buf;
        ASSERT(cowrie_encode(v, &buf) == 0);

        COWRIEValue *dec;
        ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
        ASSERT(dec->type == COWRIE_INT64);
        ASSERT(dec->as.i64 == values[i]);

        cowrie_free(v);
        cowrie_free(dec);
        cowrie_buf_free(&buf);
    }
    return 1;
}

static int test_uint64_roundtrip(void) {
    uint64_t values[] = {0, 1, 255, 65535, 4294967295ULL, 18446744073709551615ULL};
    int n = sizeof(values) / sizeof(values[0]);

    for (int i = 0; i < n; i++) {
        COWRIEValue *v = cowrie_new_uint64(values[i]);
        ASSERT(v != NULL);
        ASSERT(v->as.u64 == values[i]);

        COWRIEBuf buf;
        ASSERT(cowrie_encode(v, &buf) == 0);

        COWRIEValue *dec;
        ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
        ASSERT(dec->type == COWRIE_UINT64);
        ASSERT(dec->as.u64 == values[i]);

        cowrie_free(v);
        cowrie_free(dec);
        cowrie_buf_free(&buf);
    }
    return 1;
}

static int test_float64_roundtrip(void) {
    double values[] = {0.0, 1.0, -1.0, 3.14159, -2.71828, 1e100, -1e-100};
    int n = sizeof(values) / sizeof(values[0]);

    for (int i = 0; i < n; i++) {
        COWRIEValue *v = cowrie_new_float64(values[i]);
        ASSERT(v != NULL);

        COWRIEBuf buf;
        ASSERT(cowrie_encode(v, &buf) == 0);

        COWRIEValue *dec;
        ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
        ASSERT(dec->type == COWRIE_FLOAT64);
        ASSERT(dec->as.f64 == values[i]);

        cowrie_free(v);
        cowrie_free(dec);
        cowrie_buf_free(&buf);
    }
    return 1;
}

static int test_string_roundtrip(void) {
    const char *test = "Hello, COWRIE!";
    COWRIEValue *v = cowrie_new_string(test, strlen(test));
    ASSERT(v != NULL);
    ASSERT(v->as.str.len == strlen(test));
    ASSERT(strcmp(v->as.str.data, test) == 0);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_STRING);
    ASSERT(dec->as.str.len == strlen(test));
    ASSERT(strcmp(dec->as.str.data, test) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_bytes_roundtrip(void) {
    uint8_t data[] = {0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD};
    COWRIEValue *v = cowrie_new_bytes(data, sizeof(data));
    ASSERT(v != NULL);
    ASSERT(v->as.bytes.len == sizeof(data));
    ASSERT(memcmp(v->as.bytes.data, data, sizeof(data)) == 0);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_BYTES);
    ASSERT(dec->as.bytes.len == sizeof(data));
    ASSERT(memcmp(dec->as.bytes.data, data, sizeof(data)) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * Composite Type Tests
 * ============================================================ */

static int test_array_roundtrip(void) {
    COWRIEValue *arr = cowrie_new_array();
    ASSERT(arr != NULL);

    cowrie_array_append(arr, cowrie_new_int64(1));
    cowrie_array_append(arr, cowrie_new_int64(2));
    cowrie_array_append(arr, cowrie_new_string("three", 5));
    ASSERT(cowrie_array_len(arr) == 3);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(arr, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_ARRAY);
    ASSERT(cowrie_array_len(dec) == 3);
    ASSERT(cowrie_array_get(dec, 0)->type == COWRIE_INT64);
    ASSERT(cowrie_array_get(dec, 0)->as.i64 == 1);
    ASSERT(cowrie_array_get(dec, 2)->type == COWRIE_STRING);

    cowrie_free(arr);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_object_roundtrip(void) {
    COWRIEValue *obj = cowrie_new_object();
    ASSERT(obj != NULL);

    cowrie_object_set(obj, "name", 4, cowrie_new_string("Alice", 5));
    cowrie_object_set(obj, "age", 3, cowrie_new_int64(30));
    cowrie_object_set(obj, "active", 6, cowrie_new_bool(1));
    ASSERT(cowrie_object_len(obj) == 3);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(obj, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_OBJECT);
    ASSERT(cowrie_object_len(dec) == 3);

    COWRIEValue *name = cowrie_object_get(dec, "name", 4);
    ASSERT(name != NULL);
    ASSERT(name->type == COWRIE_STRING);
    ASSERT(strcmp(name->as.str.data, "Alice") == 0);

    COWRIEValue *age = cowrie_object_get(dec, "age", 3);
    ASSERT(age != NULL);
    ASSERT(age->type == COWRIE_INT64);
    ASSERT(age->as.i64 == 30);

    cowrie_free(obj);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_nested_object(void) {
    COWRIEValue *inner = cowrie_new_object();
    cowrie_object_set(inner, "x", 1, cowrie_new_int64(10));
    cowrie_object_set(inner, "y", 1, cowrie_new_int64(20));

    COWRIEValue *outer = cowrie_new_object();
    cowrie_object_set(outer, "point", 5, inner);
    cowrie_object_set(outer, "label", 5, cowrie_new_string("origin", 6));

    COWRIEBuf buf;
    ASSERT(cowrie_encode(outer, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);

    COWRIEValue *point = cowrie_object_get(dec, "point", 5);
    ASSERT(point != NULL);
    ASSERT(point->type == COWRIE_OBJECT);

    COWRIEValue *x = cowrie_object_get(point, "x", 1);
    ASSERT(x != NULL);
    ASSERT(x->as.i64 == 10);

    cowrie_free(outer);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * ML Extension Type Tests
 * ============================================================ */

static int test_tensor_roundtrip(void) {
    /* Create a 2x3 float32 tensor */
    size_t dims[] = {2, 3};
    float data[] = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f};

    COWRIEValue *v = cowrie_new_tensor(
        COWRIE_DTYPE_FLOAT32, 2, dims,
        (const uint8_t *)data, sizeof(data)
    );
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_TENSOR);
    ASSERT(v->as.tensor.dtype == COWRIE_DTYPE_FLOAT32);
    ASSERT(v->as.tensor.rank == 2);
    ASSERT(v->as.tensor.dims[0] == 2);
    ASSERT(v->as.tensor.dims[1] == 3);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_TENSOR);
    ASSERT(dec->as.tensor.dtype == COWRIE_DTYPE_FLOAT32);
    ASSERT(dec->as.tensor.rank == 2);
    ASSERT(dec->as.tensor.dims[0] == 2);
    ASSERT(dec->as.tensor.dims[1] == 3);
    ASSERT(dec->as.tensor.data_len == sizeof(data));

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_tensor_ref_roundtrip(void) {
    uint8_t key[] = {0xDE, 0xAD, 0xBE, 0xEF};
    COWRIEValue *v = cowrie_new_tensor_ref(1, key, sizeof(key));
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_TENSOR_REF);
    ASSERT(v->as.tensor_ref.store_id == 1);
    ASSERT(v->as.tensor_ref.key_len == sizeof(key));

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_TENSOR_REF);
    ASSERT(dec->as.tensor_ref.store_id == 1);
    ASSERT(memcmp(dec->as.tensor_ref.key, key, sizeof(key)) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_image_roundtrip(void) {
    uint8_t data[] = {0xFF, 0xD8, 0xFF, 0xE0};  /* JPEG magic */
    COWRIEValue *v = cowrie_new_image(COWRIE_IMG_JPEG, 1920, 1080, data, sizeof(data));
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_IMAGE);
    ASSERT(v->as.image.format == COWRIE_IMG_JPEG);
    ASSERT(v->as.image.width == 1920);
    ASSERT(v->as.image.height == 1080);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_IMAGE);
    ASSERT(dec->as.image.format == COWRIE_IMG_JPEG);
    ASSERT(dec->as.image.width == 1920);
    ASSERT(dec->as.image.height == 1080);
    ASSERT(dec->as.image.data_len == sizeof(data));

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_audio_roundtrip(void) {
    uint8_t data[] = {0x00, 0x00, 0x00, 0x00};  /* Silent PCM */
    COWRIEValue *v = cowrie_new_audio(COWRIE_AUD_PCM_INT16, 44100, 2, data, sizeof(data));
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_AUDIO);
    ASSERT(v->as.audio.encoding == COWRIE_AUD_PCM_INT16);
    ASSERT(v->as.audio.sample_rate == 44100);
    ASSERT(v->as.audio.channels == 2);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_AUDIO);
    ASSERT(dec->as.audio.encoding == COWRIE_AUD_PCM_INT16);
    ASSERT(dec->as.audio.sample_rate == 44100);
    ASSERT(dec->as.audio.channels == 2);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_datetime64_roundtrip(void) {
    int64_t nanos = 1704067200000000000LL;  /* 2024-01-01 00:00:00 UTC */
    COWRIEValue *v = cowrie_new_datetime64(nanos);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_DATETIME64);
    ASSERT(v->as.datetime64 == nanos);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_DATETIME64);
    ASSERT(dec->as.datetime64 == nanos);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_uuid128_roundtrip(void) {
    uint8_t uuid[16] = {
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
        0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00
    };
    COWRIEValue *v = cowrie_new_uuid128(uuid);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_UUID128);
    ASSERT(memcmp(v->as.uuid, uuid, 16) == 0);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_UUID128);
    ASSERT(memcmp(dec->as.uuid, uuid, 16) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * Special Feature Tests
 * ============================================================ */

static int test_deterministic_encoding(void) {
    /* Create object with keys in different orders */
    COWRIEValue *obj1 = cowrie_new_object();
    cowrie_object_set(obj1, "zebra", 5, cowrie_new_int64(1));
    cowrie_object_set(obj1, "alpha", 5, cowrie_new_int64(2));
    cowrie_object_set(obj1, "beta", 4, cowrie_new_int64(3));

    COWRIEValue *obj2 = cowrie_new_object();
    cowrie_object_set(obj2, "alpha", 5, cowrie_new_int64(2));
    cowrie_object_set(obj2, "beta", 4, cowrie_new_int64(3));
    cowrie_object_set(obj2, "zebra", 5, cowrie_new_int64(1));

    COWRIEEncodeOpts opts;
    cowrie_encode_opts_init(&opts);
    opts.deterministic = 1;

    COWRIEBuf buf1, buf2;
    ASSERT(cowrie_encode_with_opts(obj1, &opts, &buf1) == 0);
    ASSERT(cowrie_encode_with_opts(obj2, &opts, &buf2) == 0);

    /* Deterministic encoding should produce identical output */
    ASSERT(buf1.len == buf2.len);
    ASSERT(memcmp(buf1.data, buf2.data, buf1.len) == 0);

    cowrie_free(obj1);
    cowrie_free(obj2);
    cowrie_buf_free(&buf1);
    cowrie_buf_free(&buf2);
    return 1;
}

static int test_schema_fingerprint(void) {
    /* Same structure should produce same fingerprint */
    COWRIEValue *obj1 = cowrie_new_object();
    cowrie_object_set(obj1, "x", 1, cowrie_new_int64(1));
    cowrie_object_set(obj1, "y", 1, cowrie_new_int64(2));

    COWRIEValue *obj2 = cowrie_new_object();
    cowrie_object_set(obj2, "x", 1, cowrie_new_int64(100));
    cowrie_object_set(obj2, "y", 1, cowrie_new_int64(200));

    uint32_t fp1 = cowrie_schema_fingerprint32(obj1);
    uint32_t fp2 = cowrie_schema_fingerprint32(obj2);
    ASSERT(fp1 == fp2);  /* Same structure, different values */

    /* Different structure should produce different fingerprint */
    COWRIEValue *obj3 = cowrie_new_object();
    cowrie_object_set(obj3, "x", 1, cowrie_new_string("hello", 5));

    uint32_t fp3 = cowrie_schema_fingerprint32(obj3);
    ASSERT(fp1 != fp3);

    cowrie_free(obj1);
    cowrie_free(obj2);
    cowrie_free(obj3);
    return 1;
}

static int test_crc32(void) {
    const char *test = "Hello, World!";
    uint32_t crc = cowrie_crc32((const uint8_t *)test, strlen(test));
    /* Known CRC32-IEEE value for "Hello, World!" */
    ASSERT(crc == 0xEC4AC3D0);
    return 1;
}

static int test_master_stream(void) {
    COWRIEValue *value = cowrie_new_object();
    cowrie_object_set(value, "message", 7, cowrie_new_string("test", 4));

    COWRIEValue *meta = cowrie_new_object();
    cowrie_object_set(meta, "timestamp", 9, cowrie_new_int64(1234567890));

    COWRIEMasterWriterOpts opts;
    cowrie_master_writer_opts_init(&opts);

    COWRIEBuf buf;
    ASSERT(cowrie_master_write_frame(value, meta, &opts, &buf) == 0);
    ASSERT(buf.len > 24);  /* Header + data */

    /* Check magic */
    ASSERT(cowrie_is_master_stream(buf.data, buf.len));

    /* Read back */
    COWRIEMasterFrame frame;
    int consumed = cowrie_master_read_frame(buf.data, buf.len, &frame);
    ASSERT(consumed > 0);
    ASSERT(frame.payload != NULL);
    ASSERT(frame.meta != NULL);

    COWRIEValue *msg = cowrie_object_get(frame.payload, "message", 7);
    ASSERT(msg != NULL);
    ASSERT(strcmp(msg->as.str.data, "test") == 0);

    cowrie_master_frame_free(&frame);
    cowrie_free(value);
    cowrie_free(meta);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * Graph Type Tests
 * ============================================================ */

static int test_node_roundtrip(void) {
    const char *labels[] = {"Person", "Employee"};
    size_t label_lens[] = {6, 8};

    COWRIEMember props[2];
    props[0].key = "name";
    props[0].key_len = 4;
    props[0].value = cowrie_new_string("Alice", 5);
    props[1].key = "age";
    props[1].key_len = 3;
    props[1].value = cowrie_new_int64(30);

    COWRIEValue *node = cowrie_new_node("node_42", 7, labels, label_lens, 2, props, 2);
    ASSERT(node != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(node, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);

    ASSERT(decoded->type == COWRIE_NODE);
    ASSERT(strcmp(decoded->as.node.id, "node_42") == 0);
    ASSERT(decoded->as.node.label_count == 2);
    ASSERT(strcmp(decoded->as.node.labels[0], "Person") == 0);
    ASSERT(strcmp(decoded->as.node.labels[1], "Employee") == 0);
    ASSERT(decoded->as.node.prop_count == 2);

    cowrie_free(node);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_edge_roundtrip(void) {
    COWRIEMember props[1];
    props[0].key = "weight";
    props[0].key_len = 6;
    props[0].value = cowrie_new_float64(0.75);

    COWRIEValue *edge = cowrie_new_edge("node_1", 6, "node_2", 6, "KNOWS", 5, props, 1);
    ASSERT(edge != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(edge, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);

    ASSERT(decoded->type == COWRIE_EDGE);
    ASSERT(strcmp(decoded->as.edge.from_id, "node_1") == 0);
    ASSERT(strcmp(decoded->as.edge.to_id, "node_2") == 0);
    ASSERT(strcmp(decoded->as.edge.edge_type, "KNOWS") == 0);
    ASSERT(decoded->as.edge.prop_count == 1);

    cowrie_free(edge);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_graph_shard_roundtrip(void) {
    /* Create a simple node */
    COWRIENode nodes[1];
    nodes[0].id = "n1";
    nodes[0].id_len = 2;
    nodes[0].labels = NULL;
    nodes[0].label_lens = NULL;
    nodes[0].label_count = 0;
    nodes[0].props = NULL;
    nodes[0].prop_count = 0;

    /* Create a simple edge */
    COWRIEEdge edges[1];
    edges[0].from_id = "n1";
    edges[0].from_id_len = 2;
    edges[0].to_id = "n1";
    edges[0].to_id_len = 2;
    edges[0].edge_type = "SELF";
    edges[0].edge_type_len = 4;
    edges[0].props = NULL;
    edges[0].prop_count = 0;

    /* Create metadata */
    COWRIEMember meta[1];
    meta[0].key = "version";
    meta[0].key_len = 7;
    meta[0].value = cowrie_new_int64(1);

    COWRIEValue *shard = cowrie_new_graph_shard(nodes, 1, edges, 1, meta, 1);
    ASSERT(shard != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(shard, &buf) == 0);

    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);

    ASSERT(decoded->type == COWRIE_GRAPH_SHARD);
    ASSERT(decoded->as.graph_shard.node_count == 1);
    ASSERT(decoded->as.graph_shard.edge_count == 1);
    ASSERT(decoded->as.graph_shard.meta_count == 1);

    cowrie_free(shard);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * Fixture Tests (Core)
 * ============================================================ */

static int test_fixtures_core(void) {
    char root[1024];
    if (!build_repo_root(root, sizeof(root))) return 0;

    const char *cases[][2] = {
        {"core/null.cowrie", "core/null.json"},
        {"core/true.cowrie", "core/true.json"},
        {"core/int.cowrie", "core/int.json"},
        {"core/float.cowrie", "core/float.json"},
        {"core/string.cowrie", "core/string.json"},
        {"core/array.cowrie", "core/array.json"},
        {"core/object.cowrie", "core/object.json"},
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char input_path[1200];
        char json_path[1200];
        snprintf(input_path, sizeof(input_path), "%s/testdata/fixtures/%s", root, cases[i][0]);
        snprintf(json_path, sizeof(json_path), "%s/testdata/fixtures/%s", root, cases[i][1]);

        char *input_data = NULL;
        size_t input_len = 0;
        if (!read_file(input_path, &input_data, &input_len)) return 0;

        COWRIEValue *decoded = NULL;
        if (cowrie_decode((const uint8_t *)input_data, input_len, &decoded) != 0) {
            free(input_data);
            return 0;
        }
        free(input_data);

        COWRIEBuf actual_json;
        cowrie_buf_init(&actual_json);
        if (cowrie_to_json(decoded, &actual_json) != 0) {
            cowrie_free(decoded);
            cowrie_buf_free(&actual_json);
            return 0;
        }

        char *expected_json = NULL;
        size_t expected_len = 0;
        if (!read_file(json_path, &expected_json, &expected_len)) {
            cowrie_free(decoded);
            cowrie_buf_free(&actual_json);
            return 0;
        }

        COWRIEValue *expected_val = NULL;
        if (cowrie_from_json(expected_json, expected_len, &expected_val) != 0) {
            free(expected_json);
            cowrie_free(decoded);
            cowrie_buf_free(&actual_json);
            return 0;
        }
        free(expected_json);

        COWRIEBuf expected_canon;
        cowrie_buf_init(&expected_canon);
        if (cowrie_to_json(expected_val, &expected_canon) != 0) {
            cowrie_free(expected_val);
            cowrie_free(decoded);
            cowrie_buf_free(&actual_json);
            cowrie_buf_free(&expected_canon);
            return 0;
        }

        int ok = (actual_json.len == expected_canon.len) &&
                 (memcmp(actual_json.data, expected_canon.data, actual_json.len) == 0);

        cowrie_free(expected_val);
        cowrie_free(decoded);
        cowrie_buf_free(&actual_json);
        cowrie_buf_free(&expected_canon);

        if (!ok) return 0;
    }

    return 1;
}

/* ============================================================
 * Overflow Protection Tests
 * ============================================================ */

/* Helper: encode a uint64 as a protobuf-style uvarint into buf, return bytes written */
static size_t encode_uvarint(uint8_t *buf, uint64_t v) {
    size_t i = 0;
    while (v >= 0x80) {
        buf[i++] = (uint8_t)(v | 0x80);
        v >>= 7;
    }
    buf[i++] = (uint8_t)v;
    return i;
}

/* Test: adjlist with node_count that would overflow (node_count+1)*sizeof(size_t) */
static int test_oversized_adjlist_rejects(void) {
    /* Build: header(4) + dict_count(1) + tag(1) + id_width(1) + node_count(varint) + edge_count(varint)
     * Set node_count to SIZE_MAX so (node_count+1) wraps to 0 */
    uint8_t payload[30];
    size_t pos = 0;
    payload[pos++] = 0x53; /* S */
    payload[pos++] = 0x4A; /* J */
    payload[pos++] = 0x02; /* version 2 */
    payload[pos++] = 0x00; /* flags */
    payload[pos++] = 0x00; /* dict count = 0 */
    payload[pos++] = 0x30; /* SJT_ADJLIST */
    payload[pos++] = 0x00; /* id_width = INT32 */
    /* node_count = 0xFFFFFFFFFFFFFFFF (max uint64) */
    pos += encode_uvarint(payload + pos, UINT64_MAX);
    /* edge_count = 0 */
    payload[pos++] = 0x00;

    COWRIEValue *result = NULL;
    int rc = cowrie_decode(payload, pos, &result);
    ASSERT(rc != 0);  /* Must reject */
    ASSERT(result == NULL);
    return 1;
}

/* Test: richtext with huge token_count that would overflow token_count*sizeof(int32_t) */
static int test_oversized_richtext_tokens_rejects(void) {
    uint8_t payload[30];
    size_t pos = 0;
    payload[pos++] = 0x53; /* S */
    payload[pos++] = 0x4A; /* J */
    payload[pos++] = 0x02; /* version 2 */
    payload[pos++] = 0x00; /* flags */
    payload[pos++] = 0x00; /* dict count = 0 */
    payload[pos++] = 0x31; /* SJT_RICH_TEXT */
    payload[pos++] = 0x00; /* text_len = 0 */
    payload[pos++] = 0x01; /* flags: has tokens */
    /* token_count = 0x4000000000000000 — would overflow when * 4 */
    pos += encode_uvarint(payload + pos, (uint64_t)0x4000000000000000ULL);

    COWRIEValue *result = NULL;
    int rc = cowrie_decode(payload, pos, &result);
    ASSERT(rc != 0);  /* Must reject */
    ASSERT(result == NULL);
    return 1;
}

/* Test: array with count exceeding remaining input */
static int test_oversized_array_rejects(void) {
    uint8_t payload[20];
    size_t pos = 0;
    payload[pos++] = 0x53; /* S */
    payload[pos++] = 0x4A; /* J */
    payload[pos++] = 0x02; /* version 2 */
    payload[pos++] = 0x00; /* flags */
    payload[pos++] = 0x00; /* dict count = 0 */
    payload[pos++] = 0x06; /* SJT_ARRAY */
    /* count = 999999999 but only 0 bytes of data follow */
    pos += encode_uvarint(payload + pos, 999999999ULL);

    COWRIEValue *result = NULL;
    int rc = cowrie_decode(payload, pos, &result);
    ASSERT(rc != 0);  /* Must reject */
    ASSERT(result == NULL);
    return 1;
}

/* ============================================================
 * Additional Type Tests
 * ============================================================ */

static int test_decimal128_roundtrip(void) {
    uint8_t coef[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x27, 0x10};
    COWRIEValue *v = cowrie_new_decimal128(2, coef);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_DECIMAL128);
    ASSERT(v->as.decimal128.scale == 2);
    ASSERT(memcmp(v->as.decimal128.coef, coef, 16) == 0);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_DECIMAL128);
    ASSERT(dec->as.decimal128.scale == 2);
    ASSERT(memcmp(dec->as.decimal128.coef, coef, 16) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_bigint_roundtrip(void) {
    uint8_t data[] = {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    COWRIEValue *v = cowrie_new_bigint(data, sizeof(data));
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_BIGINT);
    ASSERT(v->as.bigint.len == sizeof(data));

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_BIGINT);
    ASSERT(dec->as.bigint.len == sizeof(data));
    ASSERT(memcmp(dec->as.bigint.data, data, sizeof(data)) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_ext_roundtrip(void) {
    uint8_t payload[] = {0xCA, 0xFE, 0xBA, 0xBE};
    COWRIEValue *v = cowrie_new_ext(42, payload, sizeof(payload));
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_EXT);
    ASSERT(v->as.ext.ext_type == 42);
    ASSERT(v->as.ext.payload_len == sizeof(payload));

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_EXT);
    ASSERT(dec->as.ext.ext_type == 42);
    ASSERT(dec->as.ext.payload_len == sizeof(payload));
    ASSERT(memcmp(dec->as.ext.payload, payload, sizeof(payload)) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_bitmask_roundtrip(void) {
    /* 10 bits: 1010110011 -> bytes: 0b11001101=0xCD, 0b00000010=0x02 */
    uint8_t bits[] = {0xCD, 0x02};
    COWRIEValue *v = cowrie_new_bitmask(10, bits);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_BITMASK);
    ASSERT(v->as.bitmask.count == 10);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_BITMASK);
    ASSERT(dec->as.bitmask.count == 10);
    ASSERT(dec->as.bitmask.bits_len == 2);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_empty_bitmask(void) {
    COWRIEValue *v = cowrie_new_bitmask(0, NULL);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_BITMASK);
    ASSERT(v->as.bitmask.count == 0);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_BITMASK);
    ASSERT(dec->as.bitmask.count == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_adjlist_construct_encode(void) {
    /* Full roundtrip: construct → encode → decode → compare all fields */
    size_t row_offsets[] = {0, 2, 3};
    int32_t col_indices[] = {1, 2, 0};
    COWRIEValue *v = cowrie_new_adjlist(COWRIE_ID_INT32, 2, 3, row_offsets, col_indices);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_ADJLIST);
    ASSERT(v->as.adjlist.node_count == 2);
    ASSERT(v->as.adjlist.edge_count == 3);
    ASSERT(v->as.adjlist.id_width == COWRIE_ID_INT32);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);
    ASSERT(buf.len > 0);

    /* Decode and verify all fields match */
    COWRIEValue *decoded;
    ASSERT(cowrie_decode(buf.data, buf.len, &decoded) == 0);
    ASSERT(decoded->type == COWRIE_ADJLIST);
    ASSERT(decoded->as.adjlist.node_count == 2);
    ASSERT(decoded->as.adjlist.edge_count == 3);
    ASSERT(decoded->as.adjlist.id_width == COWRIE_ID_INT32);

    /* Compare row_offsets */
    for (size_t i = 0; i <= 2; i++) {
        ASSERT(decoded->as.adjlist.row_offsets[i] == row_offsets[i]);
    }

    /* Compare col_indices */
    int32_t *dec_cols = (int32_t *)decoded->as.adjlist.col_indices;
    for (size_t i = 0; i < 3; i++) {
        ASSERT(dec_cols[i] == col_indices[i]);
    }

    cowrie_free(v);
    cowrie_free(decoded);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_adjlist_int64_construct_encode(void) {
    size_t row_offsets[] = {0, 1, 2};
    int64_t col_indices[] = {100, 200};
    COWRIEValue *v = cowrie_new_adjlist(COWRIE_ID_INT64, 2, 2, row_offsets, col_indices);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_ADJLIST);
    ASSERT(v->as.adjlist.id_width == COWRIE_ID_INT64);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);
    ASSERT(buf.len > 0);

    cowrie_free(v);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_rich_text_roundtrip(void) {
    const char *text = "Hello, World!";
    int32_t tokens[] = {101, 102, 103};
    COWRIERichTextSpan spans[] = {{0, 5, 1}, {7, 12, 2}};

    COWRIEValue *v = cowrie_new_rich_text(text, strlen(text), tokens, 3, spans, 2);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_RICH_TEXT);
    ASSERT(v->as.rich_text.text_len == strlen(text));
    ASSERT(v->as.rich_text.token_count == 3);
    ASSERT(v->as.rich_text.span_count == 2);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_RICH_TEXT);
    ASSERT(strcmp(dec->as.rich_text.text, text) == 0);
    ASSERT(dec->as.rich_text.token_count == 3);
    ASSERT(dec->as.rich_text.tokens[0] == 101);
    ASSERT(dec->as.rich_text.tokens[2] == 103);
    ASSERT(dec->as.rich_text.span_count == 2);
    ASSERT(dec->as.rich_text.spans[0].start == 0);
    ASSERT(dec->as.rich_text.spans[0].end == 5);
    ASSERT(dec->as.rich_text.spans[1].kind_id == 2);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_rich_text_plain(void) {
    const char *text = "Just plain text";
    COWRIEValue *v = cowrie_new_rich_text(text, strlen(text), NULL, 0, NULL, 0);
    ASSERT(v != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_RICH_TEXT);
    ASSERT(strcmp(dec->as.rich_text.text, text) == 0);
    ASSERT(dec->as.rich_text.token_count == 0);
    ASSERT(dec->as.rich_text.span_count == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_delta_roundtrip(void) {
    COWRIEDeltaOp_t ops[3];
    ops[0].op_code = COWRIE_DELTA_SET_FIELD;
    ops[0].field_id = 0;
    ops[0].value = cowrie_new_int64(42);
    ops[1].op_code = COWRIE_DELTA_DELETE_FIELD;
    ops[1].field_id = 1;
    ops[1].value = NULL;
    ops[2].op_code = COWRIE_DELTA_APPEND_ARRAY;
    ops[2].field_id = 2;
    ops[2].value = cowrie_new_string("appended", 8);

    COWRIEValue *v = cowrie_new_delta(100, ops, 3);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_DELTA);
    ASSERT(v->as.delta.base_id == 100);
    ASSERT(v->as.delta.op_count == 3);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_DELTA);
    ASSERT(dec->as.delta.base_id == 100);
    ASSERT(dec->as.delta.op_count == 3);
    ASSERT(dec->as.delta.ops[0].op_code == COWRIE_DELTA_SET_FIELD);
    ASSERT(dec->as.delta.ops[0].value->as.i64 == 42);
    ASSERT(dec->as.delta.ops[1].op_code == COWRIE_DELTA_DELETE_FIELD);
    ASSERT(dec->as.delta.ops[2].op_code == COWRIE_DELTA_APPEND_ARRAY);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_fixint_encoding(void) {
    /* Values 0-127 should use FIXINT inline encoding */
    for (int i = 0; i <= 127; i++) {
        COWRIEValue *v = cowrie_new_int64(i);
        COWRIEBuf buf;
        ASSERT(cowrie_encode(v, &buf) == 0);

        COWRIEValue *dec;
        ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
        ASSERT(dec->type == COWRIE_INT64);
        ASSERT(dec->as.i64 == i);

        cowrie_free(v);
        cowrie_free(dec);
        cowrie_buf_free(&buf);
    }
    return 1;
}

static int test_fixneg_encoding(void) {
    /* Values -1 to -16 should use FIXNEG inline encoding */
    for (int i = -1; i >= -16; i--) {
        COWRIEValue *v = cowrie_new_int64(i);
        COWRIEBuf buf;
        ASSERT(cowrie_encode(v, &buf) == 0);

        COWRIEValue *dec;
        ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
        ASSERT(dec->type == COWRIE_INT64);
        ASSERT(dec->as.i64 == i);

        cowrie_free(v);
        cowrie_free(dec);
        cowrie_buf_free(&buf);
    }
    return 1;
}

static int test_large_int64_roundtrip(void) {
    /* Values outside fixint/fixneg range use standard INT64 tag */
    int64_t values[] = {128, 256, -17, -128, 32767, -32768, 1000000LL, -1000000LL};
    for (int i = 0; i < 8; i++) {
        COWRIEValue *v = cowrie_new_int64(values[i]);
        COWRIEBuf buf;
        ASSERT(cowrie_encode(v, &buf) == 0);

        COWRIEValue *dec;
        ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
        ASSERT(dec->type == COWRIE_INT64);
        ASSERT(dec->as.i64 == values[i]);

        cowrie_free(v);
        cowrie_free(dec);
        cowrie_buf_free(&buf);
    }
    return 1;
}

static int test_fixarray_encoding(void) {
    /* Arrays with 0-15 items should use FIXARRAY inline encoding */
    for (int n = 0; n <= 15; n++) {
        COWRIEValue *arr = cowrie_new_array();
        for (int i = 0; i < n; i++) {
            cowrie_array_append(arr, cowrie_new_int64(i));
        }

        COWRIEBuf buf;
        ASSERT(cowrie_encode(arr, &buf) == 0);

        COWRIEValue *dec;
        ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
        ASSERT(dec->type == COWRIE_ARRAY);
        ASSERT(cowrie_array_len(dec) == (size_t)n);

        cowrie_free(arr);
        cowrie_free(dec);
        cowrie_buf_free(&buf);
    }
    return 1;
}

static int test_fixmap_encoding(void) {
    /* Objects with 0-15 fields should use FIXMAP inline encoding */
    for (int n = 0; n <= 5; n++) {
        COWRIEValue *obj = cowrie_new_object();
        for (int i = 0; i < n; i++) {
            char key[16];
            snprintf(key, sizeof(key), "key%d", i);
            cowrie_object_set(obj, key, strlen(key), cowrie_new_int64(i));
        }

        COWRIEBuf buf;
        ASSERT(cowrie_encode(obj, &buf) == 0);

        COWRIEValue *dec;
        ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
        ASSERT(dec->type == COWRIE_OBJECT);
        ASSERT(cowrie_object_len(dec) == (size_t)n);

        cowrie_free(obj);
        cowrie_free(dec);
        cowrie_buf_free(&buf);
    }
    return 1;
}

static int test_large_array_roundtrip(void) {
    /* Array with > 15 items should use regular ARRAY tag */
    COWRIEValue *arr = cowrie_new_array();
    for (int i = 0; i < 20; i++) {
        cowrie_array_append(arr, cowrie_new_int64(i));
    }

    COWRIEBuf buf;
    ASSERT(cowrie_encode(arr, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_ARRAY);
    ASSERT(cowrie_array_len(dec) == 20);
    ASSERT(cowrie_array_get(dec, 19)->as.i64 == 19);

    cowrie_free(arr);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_large_object_roundtrip(void) {
    /* Object with > 15 fields should use regular OBJECT tag */
    COWRIEValue *obj = cowrie_new_object();
    for (int i = 0; i < 20; i++) {
        char key[16];
        snprintf(key, sizeof(key), "field_%d", i);
        cowrie_object_set(obj, key, strlen(key), cowrie_new_int64(i * 10));
    }

    COWRIEBuf buf;
    ASSERT(cowrie_encode(obj, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_OBJECT);
    ASSERT(cowrie_object_len(dec) == 20);

    COWRIEValue *f0 = cowrie_object_get(dec, "field_0", 7);
    ASSERT(f0 != NULL && f0->as.i64 == 0);
    COWRIEValue *f19 = cowrie_object_get(dec, "field_19", 8);
    ASSERT(f19 != NULL && f19->as.i64 == 190);

    cowrie_free(obj);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_omit_null_encoding(void) {
    COWRIEValue *obj = cowrie_new_object();
    cowrie_object_set(obj, "keep", 4, cowrie_new_int64(1));
    cowrie_object_set(obj, "drop", 4, cowrie_new_null());
    cowrie_object_set(obj, "also_keep", 9, cowrie_new_string("hi", 2));

    COWRIEEncodeOpts opts;
    cowrie_encode_opts_init(&opts);
    opts.deterministic = 1;
    opts.omit_null = 1;

    COWRIEBuf buf;
    ASSERT(cowrie_encode_with_opts(obj, &opts, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_OBJECT);
    ASSERT(cowrie_object_len(dec) == 2);
    ASSERT(cowrie_object_get(dec, "keep", 4) != NULL);
    ASSERT(cowrie_object_get(dec, "also_keep", 9) != NULL);
    ASSERT(cowrie_object_get(dec, "drop", 4) == NULL);

    cowrie_free(obj);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_framed_none(void) {
    COWRIEValue *v = cowrie_new_object();
    cowrie_object_set(v, "msg", 3, cowrie_new_string("test", 4));

    COWRIEBuf buf;
    ASSERT(cowrie_encode_framed(v, COWRIE_COMP_NONE, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode_framed(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_OBJECT);
    COWRIEValue *msg = cowrie_object_get(dec, "msg", 3);
    ASSERT(msg != NULL && strcmp(msg->as.str.data, "test") == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_framed_gzip(void) {
    COWRIEValue *v = cowrie_new_object();
    cowrie_object_set(v, "data", 4, cowrie_new_string("compressed content", 18));

    COWRIEBuf buf;
    ASSERT(cowrie_encode_framed(v, COWRIE_COMP_GZIP, &buf) == 0);
    ASSERT(buf.len > 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode_framed(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_OBJECT);
    COWRIEValue *data = cowrie_object_get(dec, "data", 4);
    ASSERT(data != NULL && strcmp(data->as.str.data, "compressed content") == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_decode_with_opts(void) {
    COWRIEValue *v = cowrie_new_object();
    cowrie_object_set(v, "test", 4, cowrie_new_int64(123));

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEDecodeOpts opts;
    cowrie_decode_opts_init(&opts);
    opts.max_depth = 10;
    opts.max_array_len = 100;

    COWRIEValue *dec;
    ASSERT(cowrie_decode_with_opts(buf.data, buf.len, &opts, &dec) == 0);
    ASSERT(dec->type == COWRIE_OBJECT);
    COWRIEValue *test = cowrie_object_get(dec, "test", 4);
    ASSERT(test != NULL && test->as.i64 == 123);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_empty_string_roundtrip(void) {
    COWRIEValue *v = cowrie_new_string("", 0);
    ASSERT(v != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_STRING);
    ASSERT(dec->as.str.len == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_empty_bytes_roundtrip(void) {
    COWRIEValue *v = cowrie_new_bytes(NULL, 0);
    ASSERT(v != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_BYTES);
    ASSERT(dec->as.bytes.len == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_empty_array_roundtrip(void) {
    COWRIEValue *v = cowrie_new_array();
    ASSERT(v != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_ARRAY);
    ASSERT(cowrie_array_len(dec) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_empty_object_roundtrip(void) {
    COWRIEValue *v = cowrie_new_object();
    ASSERT(v != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_OBJECT);
    ASSERT(cowrie_object_len(dec) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_node_batch_roundtrip(void) {
    const char *labels[] = {"Person"};
    size_t label_lens[] = {6};

    COWRIEMember props[1];
    props[0].key = "name";
    props[0].key_len = 4;
    props[0].value = cowrie_new_string("Alice", 5);

    COWRIENode nodes[1];
    nodes[0].id = "n1";
    nodes[0].id_len = 2;
    nodes[0].labels = (char **)labels;
    nodes[0].label_lens = label_lens;
    nodes[0].label_count = 1;
    nodes[0].props = props;
    nodes[0].prop_count = 1;

    COWRIEValue *v = cowrie_new_node_batch(nodes, 1);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_NODE_BATCH);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_NODE_BATCH);
    ASSERT(dec->as.node_batch.node_count == 1);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_edge_batch_roundtrip(void) {
    COWRIEEdge edges[1];
    edges[0].from_id = "n1";
    edges[0].from_id_len = 2;
    edges[0].to_id = "n2";
    edges[0].to_id_len = 2;
    edges[0].edge_type = "KNOWS";
    edges[0].edge_type_len = 5;
    edges[0].props = NULL;
    edges[0].prop_count = 0;

    COWRIEValue *v = cowrie_new_edge_batch(edges, 1);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_EDGE_BATCH);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_EDGE_BATCH);
    ASSERT(dec->as.edge_batch.edge_count == 1);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_master_stream_with_crc(void) {
    COWRIEValue *value = cowrie_new_object();
    cowrie_object_set(value, "key", 3, cowrie_new_int64(42));

    COWRIEMasterWriterOpts opts;
    cowrie_master_writer_opts_init(&opts);
    opts.enable_crc = 1;
    opts.deterministic = 1;

    COWRIEBuf buf;
    ASSERT(cowrie_master_write_frame(value, NULL, &opts, &buf) == 0);
    ASSERT(buf.len > 0);

    COWRIEMasterFrame frame;
    int consumed = cowrie_master_read_frame(buf.data, buf.len, &frame);
    ASSERT(consumed > 0);
    ASSERT(frame.payload != NULL);

    COWRIEValue *key = cowrie_object_get(frame.payload, "key", 3);
    ASSERT(key != NULL && key->as.i64 == 42);

    cowrie_master_frame_free(&frame);
    cowrie_free(value);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_invalid_magic_rejects(void) {
    uint8_t bad_data[] = {0xFF, 0xFF, 0x02, 0x00, 0x00, 0x00};
    COWRIEValue *result = NULL;
    int rc = cowrie_decode(bad_data, sizeof(bad_data), &result);
    ASSERT(rc != 0);
    ASSERT(result == NULL);
    return 1;
}

static int test_mixed_type_array(void) {
    COWRIEValue *arr = cowrie_new_array();
    cowrie_array_append(arr, cowrie_new_null());
    cowrie_array_append(arr, cowrie_new_bool(1));
    cowrie_array_append(arr, cowrie_new_int64(-42));
    cowrie_array_append(arr, cowrie_new_uint64(999));
    cowrie_array_append(arr, cowrie_new_float64(2.718));
    cowrie_array_append(arr, cowrie_new_string("hello", 5));
    uint8_t bytes_data[] = {0xAB, 0xCD};
    cowrie_array_append(arr, cowrie_new_bytes(bytes_data, 2));

    COWRIEBuf buf;
    ASSERT(cowrie_encode(arr, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(cowrie_array_len(dec) == 7);
    ASSERT(cowrie_array_get(dec, 0)->type == COWRIE_NULL);
    ASSERT(cowrie_array_get(dec, 1)->type == COWRIE_BOOL);
    ASSERT(cowrie_array_get(dec, 2)->type == COWRIE_INT64);
    ASSERT(cowrie_array_get(dec, 2)->as.i64 == -42);
    ASSERT(cowrie_array_get(dec, 3)->type == COWRIE_UINT64);
    ASSERT(cowrie_array_get(dec, 3)->as.u64 == 999);
    ASSERT(cowrie_array_get(dec, 4)->type == COWRIE_FLOAT64);
    ASSERT(cowrie_array_get(dec, 5)->type == COWRIE_STRING);
    ASSERT(cowrie_array_get(dec, 6)->type == COWRIE_BYTES);

    cowrie_free(arr);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_deeply_nested(void) {
    /* Create a deeply nested structure: [[[[42]]]] */
    COWRIEValue *inner = cowrie_new_array();
    cowrie_array_append(inner, cowrie_new_int64(42));
    for (int i = 0; i < 10; i++) {
        COWRIEValue *wrapper = cowrie_new_array();
        cowrie_array_append(wrapper, inner);
        inner = wrapper;
    }

    COWRIEBuf buf;
    ASSERT(cowrie_encode(inner, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_ARRAY);

    cowrie_free(inner);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

static int test_tensor_copy_float32(void) {
    size_t dims[] = {2, 2};
    float data[] = {1.0f, 2.0f, 3.0f, 4.0f};

    COWRIEValue *v = cowrie_new_tensor(
        COWRIE_DTYPE_FLOAT32, 2, dims,
        (const uint8_t *)data, sizeof(data)
    );
    ASSERT(v != NULL);

    size_t count;
    float *copied = cowrie_tensor_copy_float32(&v->as.tensor, &count);
    ASSERT(copied != NULL);
    ASSERT(count == 4);
    ASSERT(fabs(copied[0] - 1.0f) < 0.001f);
    ASSERT(fabs(copied[3] - 4.0f) < 0.001f);

    free(copied);
    cowrie_free(v);
    return 1;
}

static int test_tensor_copy_float64(void) {
    size_t dims[] = {3};
    double data[] = {1.0, 2.0, 3.0};

    COWRIEValue *v = cowrie_new_tensor(
        COWRIE_DTYPE_FLOAT64, 1, dims,
        (const uint8_t *)data, sizeof(data)
    );
    ASSERT(v != NULL);

    size_t count;
    double *copied = cowrie_tensor_copy_float64(&v->as.tensor, &count);
    ASSERT(copied != NULL);
    ASSERT(count == 3);
    ASSERT(fabs(copied[0] - 1.0) < 0.001);
    ASSERT(fabs(copied[2] - 3.0) < 0.001);

    free(copied);
    cowrie_free(v);
    return 1;
}

static int test_tensor_copy_int32(void) {
    size_t dims[] = {4};
    int32_t data[] = {-1, 0, 1, 2147483647};

    COWRIEValue *v = cowrie_new_tensor(
        COWRIE_DTYPE_INT32, 1, dims,
        (const uint8_t *)data, sizeof(data)
    );
    ASSERT(v != NULL);

    size_t count;
    int32_t *copied = cowrie_tensor_copy_int32(&v->as.tensor, &count);
    ASSERT(copied != NULL);
    ASSERT(count == 4);
    ASSERT(copied[0] == -1);
    ASSERT(copied[3] == 2147483647);

    free(copied);
    cowrie_free(v);
    return 1;
}

static int test_tensor_copy_int64(void) {
    size_t dims[] = {3};
    int64_t data[] = {-1LL, 0LL, 9223372036854775807LL};

    COWRIEValue *v = cowrie_new_tensor(
        COWRIE_DTYPE_INT64, 1, dims,
        (const uint8_t *)data, sizeof(data)
    );
    ASSERT(v != NULL);

    size_t count;
    int64_t *copied = cowrie_tensor_copy_int64(&v->as.tensor, &count);
    ASSERT(copied != NULL);
    ASSERT(count == 3);
    ASSERT(copied[0] == -1LL);
    ASSERT(copied[2] == 9223372036854775807LL);

    free(copied);
    cowrie_free(v);
    return 1;
}

/* Test framed gzip roundtrip (encode_framed + decode_framed) */
static int test_framed_gzip_roundtrip(void) {
    COWRIEValue *obj = cowrie_new_object();
    cowrie_object_set(obj, "key", 3, cowrie_new_string("value", 5));
    cowrie_object_set(obj, "num", 3, cowrie_new_int64(42));

    COWRIEBuf buf;
    ASSERT(cowrie_encode_framed(obj, COWRIE_COMP_GZIP, &buf) == 0);
    ASSERT(buf.len > 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode_framed(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_OBJECT);

    COWRIEValue *key_val = cowrie_object_get(dec, "key", 3);
    ASSERT(key_val != NULL);
    ASSERT(key_val->type == COWRIE_STRING);
    ASSERT(memcmp(key_val->as.str.data, "value", 5) == 0);

    cowrie_free(obj);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

/* Test framed none roundtrip */
static int test_framed_none_roundtrip(void) {
    COWRIEValue *arr = cowrie_new_array();
    cowrie_array_append(arr, cowrie_new_int64(1));
    cowrie_array_append(arr, cowrie_new_int64(2));

    COWRIEBuf buf;
    ASSERT(cowrie_encode_framed(arr, COWRIE_COMP_NONE, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode_framed(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_ARRAY);
    ASSERT(dec->as.array.len == 2);

    cowrie_free(arr);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

/* Test tensor view functions (inline, need coverage) */
static int test_tensor_view_int32(void) {
    size_t dims[] = {3};
    int32_t data[] = {10, 20, 30};
    COWRIEValue *v = cowrie_new_tensor(
        COWRIE_DTYPE_INT32, 1, dims,
        (const uint8_t *)data, sizeof(data)
    );
    ASSERT(v != NULL);

    size_t count;
    const int32_t *view = cowrie_tensor_view_int32(&v->as.tensor, &count);
    ASSERT(view != NULL);
    ASSERT(count == 3);
    ASSERT(view[0] == 10);
    ASSERT(view[2] == 30);

    cowrie_free(v);
    return 1;
}

static int test_tensor_view_int64(void) {
    size_t dims[] = {2};
    int64_t data[] = {100LL, 200LL};
    COWRIEValue *v = cowrie_new_tensor(
        COWRIE_DTYPE_INT64, 1, dims,
        (const uint8_t *)data, sizeof(data)
    );
    ASSERT(v != NULL);

    size_t count;
    const int64_t *view = cowrie_tensor_view_int64(&v->as.tensor, &count);
    ASSERT(view != NULL);
    ASSERT(count == 2);
    ASSERT(view[0] == 100LL);

    cowrie_free(v);
    return 1;
}

static int test_tensor_view_uint8(void) {
    size_t dims[] = {4};
    uint8_t data[] = {0, 128, 255, 1};
    COWRIEValue *v = cowrie_new_tensor(
        COWRIE_DTYPE_UINT8, 1, dims,
        data, sizeof(data)
    );
    ASSERT(v != NULL);

    size_t count;
    const uint8_t *view = cowrie_tensor_view_uint8(&v->as.tensor, &count);
    ASSERT(view != NULL);
    ASSERT(count == 4);
    ASSERT(view[2] == 255);

    cowrie_free(v);
    return 1;
}

/* Test encode_with_opts for deterministic + sorted keys with diverse types */
static int test_encode_sorted_deterministic(void) {
    COWRIEValue *obj = cowrie_new_object();
    cowrie_object_set(obj, "z_null", 6, cowrie_new_null());
    cowrie_object_set(obj, "a_bool", 6, cowrie_new_bool(1));
    cowrie_object_set(obj, "m_int", 5, cowrie_new_int64(-42));
    cowrie_object_set(obj, "b_uint", 6, cowrie_new_uint64(999));
    cowrie_object_set(obj, "c_float", 7, cowrie_new_float64(3.14));
    cowrie_object_set(obj, "d_str", 5, cowrie_new_string("hello", 5));
    cowrie_object_set(obj, "e_bytes", 7, cowrie_new_bytes((const uint8_t*)"data", 4));

    /* Add a nested array */
    COWRIEValue *arr = cowrie_new_array();
    cowrie_array_append(arr, cowrie_new_int64(1));
    cowrie_array_append(arr, cowrie_new_string("two", 3));
    cowrie_object_set(obj, "f_arr", 5, arr);

    /* Nested object */
    COWRIEValue *inner = cowrie_new_object();
    cowrie_object_set(inner, "x", 1, cowrie_new_int64(10));
    cowrie_object_set(obj, "g_obj", 5, inner);

    COWRIEEncodeOpts opts = {0};
    opts.deterministic = 1;

    COWRIEBuf buf;
    ASSERT(cowrie_encode_with_opts(obj, &opts, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_OBJECT);
    /* Keys should be sorted alphabetically */
    ASSERT(dec->as.object.len == 9);
    ASSERT(strcmp(dec->as.object.members[0].key, "a_bool") == 0);
    ASSERT(dec->as.object.members[0].value->type == COWRIE_BOOL);

    /* Check null is present */
    COWRIEValue *z_null = cowrie_object_get(dec, "z_null", 6);
    ASSERT(z_null != NULL && z_null->type == COWRIE_NULL);

    /* Check float */
    COWRIEValue *c_float = cowrie_object_get(dec, "c_float", 7);
    ASSERT(c_float != NULL && c_float->type == COWRIE_FLOAT64);

    /* Check uint */
    COWRIEValue *b_uint = cowrie_object_get(dec, "b_uint", 6);
    ASSERT(b_uint != NULL);

    cowrie_free(obj);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

/* Test deterministic encoding with omit_null */
static int test_deterministic_omit_null(void) {
    COWRIEValue *obj = cowrie_new_object();
    cowrie_object_set(obj, "keep", 4, cowrie_new_int64(42));
    cowrie_object_set(obj, "drop", 4, cowrie_new_null());
    cowrie_object_set(obj, "also_keep", 9, cowrie_new_string("yes", 3));

    COWRIEEncodeOpts opts = {0};
    opts.deterministic = 1;
    opts.omit_null = 1;

    COWRIEBuf buf;
    ASSERT(cowrie_encode_with_opts(obj, &opts, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_OBJECT);
    /* null should be omitted */
    ASSERT(dec->as.object.len == 2);

    cowrie_free(obj);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

/* Test schema_fingerprint for extension types to cover fingerprint switch cases */
static int test_schema_fingerprint_ext_types(void) {
    /* Tensor */
    size_t dims[] = {2};
    float fdata[] = {1.0f, 2.0f};
    COWRIEValue *tensor = cowrie_new_tensor(COWRIE_DTYPE_FLOAT32, 1, dims, (const uint8_t*)fdata, sizeof(fdata));
    ASSERT(tensor != NULL);
    uint32_t fp_tensor = cowrie_schema_fingerprint32(tensor);
    ASSERT(fp_tensor != 0);

    /* Different dtype tensor should have different fingerprint */
    double ddata[] = {1.0, 2.0};
    COWRIEValue *tensor2 = cowrie_new_tensor(COWRIE_DTYPE_FLOAT64, 1, dims, (const uint8_t*)ddata, sizeof(ddata));
    uint32_t fp_tensor2 = cowrie_schema_fingerprint32(tensor2);
    ASSERT(fp_tensor != fp_tensor2);

    /* TensorRef */
    uint8_t key[] = {1, 2};
    COWRIEValue *tref = cowrie_new_tensor_ref(1, key, 2);
    uint32_t fp_tref = cowrie_schema_fingerprint32(tref);
    ASSERT(fp_tref != 0);

    /* Ext */
    uint8_t payload[] = {0xAB};
    COWRIEValue *ext = cowrie_new_ext(42, payload, 1);
    uint32_t fp_ext = cowrie_schema_fingerprint32(ext);
    ASSERT(fp_ext != 0);

    /* Image */
    uint8_t img[] = {0xFF};
    COWRIEValue *image = cowrie_new_image(1, 10, 10, img, 1);
    uint32_t fp_img = cowrie_schema_fingerprint32(image);
    ASSERT(fp_img != 0);

    /* Audio */
    uint8_t aud[] = {0x00};
    COWRIEValue *audio = cowrie_new_audio(1, 44100, 2, aud, 1);
    uint32_t fp_aud = cowrie_schema_fingerprint32(audio);
    ASSERT(fp_aud != 0);

    /* Adjlist */
    size_t row_offsets[] = {0, 1};
    int32_t col_indices[] = {0};
    COWRIEValue *adj = cowrie_new_adjlist(COWRIE_ID_INT32, 1, 1, row_offsets, col_indices);
    uint32_t fp_adj = cowrie_schema_fingerprint32(adj);
    ASSERT(fp_adj != 0);

    /* RichText */
    int32_t tokens[] = {1};
    COWRIERichTextSpan spans[] = {{0, 2, 1}};
    COWRIEValue *rt = cowrie_new_rich_text("hi", 2, tokens, 1, spans, 1);
    uint32_t fp_rt = cowrie_schema_fingerprint32(rt);
    ASSERT(fp_rt != 0);

    /* Delta */
    COWRIEDeltaOp_t ops[1];
    ops[0].op_code = COWRIE_DELTA_SET_FIELD;
    ops[0].field_id = 1;
    ops[0].value = cowrie_new_int64(99);
    COWRIEValue *delta = cowrie_new_delta(0, ops, 1);
    uint32_t fp_delta = cowrie_schema_fingerprint32(delta);
    ASSERT(fp_delta != 0);

    /* All types should have distinct fingerprints */
    ASSERT(fp_tensor != fp_ext);
    ASSERT(fp_ext != fp_img);
    ASSERT(fp_img != fp_aud);

    cowrie_free(tensor);
    cowrie_free(tensor2);
    cowrie_free(tref);
    cowrie_free(ext);
    cowrie_free(image);
    cowrie_free(audio);
    cowrie_free(adj);
    cowrie_free(rt);
    cowrie_free(delta);
    return 1;
}

/* Test UTF-8 multi-byte strings (2-byte and 3-byte sequences) */
static int test_utf8_multibyte_string(void) {
    /* 2-byte UTF-8: e-acute (U+00E9) = 0xC3 0xA9 */
    /* 3-byte UTF-8: CJK char (U+4E16) = 0xE4 0xB8 0x96 */
    const char utf8[] = "\xC3\xA9\xE4\xB8\x96";
    COWRIEValue *v = cowrie_new_string(utf8, 5);
    ASSERT(v != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_STRING);
    ASSERT(dec->as.str.len == 5);
    ASSERT(memcmp(dec->as.str.data, utf8, 5) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

/* Test UTF-8 4-byte sequences (emoji) */
static int test_utf8_4byte_string(void) {
    /* 4-byte UTF-8: U+1F600 (grinning face) = 0xF0 0x9F 0x98 0x80 */
    const char utf8[] = "\xF0\x9F\x98\x80";
    COWRIEValue *v = cowrie_new_string(utf8, 4);
    ASSERT(v != NULL);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_STRING);
    ASSERT(dec->as.str.len == 4);
    ASSERT(memcmp(dec->as.str.data, utf8, 4) == 0);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

/* Test graph_shard with properties on nodes/edges (covers collect_keys/dict_find paths) */
static int test_graph_shard_with_props(void) {
    /* Create nodes with properties */
    const char *labels[] = {"Person"};
    size_t label_lens[] = {6};

    COWRIEMember props1[1];
    COWRIEValue *age1 = cowrie_new_int64(25);
    props1[0].key = "age";
    props1[0].key_len = 3;
    props1[0].value = age1;
    COWRIEValue *node1 = cowrie_new_node("n1", 2, labels, label_lens, 1, props1, 1);
    ASSERT(node1 != NULL);

    COWRIEMember props2[1];
    COWRIEValue *age2 = cowrie_new_int64(30);
    props2[0].key = "age";
    props2[0].key_len = 3;
    props2[0].value = age2;
    COWRIEValue *node2 = cowrie_new_node("n2", 2, labels, label_lens, 1, props2, 1);
    ASSERT(node2 != NULL);

    /* Create edge with properties */
    COWRIEMember eprops[1];
    COWRIEValue *weight = cowrie_new_float64(0.5);
    eprops[0].key = "weight";
    eprops[0].key_len = 6;
    eprops[0].value = weight;
    COWRIEValue *edge = cowrie_new_edge("n1", 2, "n2", 2, "knows", 5, eprops, 1);
    ASSERT(edge != NULL);

    /* Build graph shard */
    COWRIENode nodes[2];
    nodes[0] = node1->as.node;
    nodes[1] = node2->as.node;
    COWRIEEdge edges[1];
    edges[0] = edge->as.edge;

    COWRIEMember meta_props[1];
    COWRIEValue *version = cowrie_new_int64(1);
    meta_props[0].key = "version";
    meta_props[0].key_len = 7;
    meta_props[0].value = version;

    COWRIEValue *shard = cowrie_new_graph_shard(nodes, 2, edges, 1, meta_props, 1);
    ASSERT(shard != NULL);
    ASSERT(shard->type == COWRIE_GRAPH_SHARD);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(shard, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_GRAPH_SHARD);
    ASSERT(dec->as.graph_shard.node_count == 2);
    ASSERT(dec->as.graph_shard.edge_count == 1);

    /* Clean up - note: node1/node2/edge own the values, shard copies them */
    cowrie_free(shard);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    /* node1, node2, edge still own their original values */
    cowrie_free(node1);
    cowrie_free(node2);
    cowrie_free(edge);
    return 1;
}

/* Test node with multiple labels */
static int test_node_multi_label(void) {
    const char *labels[] = {"Person", "Employee"};
    size_t label_lens[] = {6, 8};
    const char *id = "n001";

    COWRIEMember props[1];
    COWRIEValue *age = cowrie_new_int64(30);
    props[0].key = "age";
    props[0].key_len = 3;
    props[0].value = age;

    COWRIEValue *v = cowrie_new_node(id, 4, labels, label_lens, 2, props, 1);
    ASSERT(v != NULL);
    ASSERT(v->type == COWRIE_NODE);
    ASSERT(v->as.node.label_count == 2);

    COWRIEBuf buf;
    ASSERT(cowrie_encode(v, &buf) == 0);

    COWRIEValue *dec;
    ASSERT(cowrie_decode(buf.data, buf.len, &dec) == 0);
    ASSERT(dec->type == COWRIE_NODE);
    ASSERT(dec->as.node.label_count == 2);
    ASSERT(dec->as.node.id_len == 4);

    cowrie_free(v);
    cowrie_free(dec);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * Invariant Tests
 * ============================================================ */

/* NaN/Inf policy: allowed in cowrie binary */
static int test_binary_nan_inf_roundtrip(void) {
    /* NaN */
    COWRIEValue *nan_val = cowrie_new_float64(NAN);
    ASSERT(nan_val != NULL);
    COWRIEBuf nan_buf;
    ASSERT(cowrie_encode(nan_val, &nan_buf) == 0);
    COWRIEValue *nan_dec;
    ASSERT(cowrie_decode(nan_buf.data, nan_buf.len, &nan_dec) == 0);
    ASSERT(nan_dec->type == COWRIE_FLOAT64);
    ASSERT(isnan(nan_dec->as.f64));
    cowrie_free(nan_val);
    cowrie_free(nan_dec);
    cowrie_buf_free(&nan_buf);

    /* +Inf */
    COWRIEValue *inf_val = cowrie_new_float64(INFINITY);
    ASSERT(inf_val != NULL);
    COWRIEBuf inf_buf;
    ASSERT(cowrie_encode(inf_val, &inf_buf) == 0);
    COWRIEValue *inf_dec;
    ASSERT(cowrie_decode(inf_buf.data, inf_buf.len, &inf_dec) == 0);
    ASSERT(inf_dec->type == COWRIE_FLOAT64);
    ASSERT(isinf(inf_dec->as.f64) && inf_dec->as.f64 > 0);
    cowrie_free(inf_val);
    cowrie_free(inf_dec);
    cowrie_buf_free(&inf_buf);

    /* -Inf */
    COWRIEValue *ninf_val = cowrie_new_float64(-INFINITY);
    ASSERT(ninf_val != NULL);
    COWRIEBuf ninf_buf;
    ASSERT(cowrie_encode(ninf_val, &ninf_buf) == 0);
    COWRIEValue *ninf_dec;
    ASSERT(cowrie_decode(ninf_buf.data, ninf_buf.len, &ninf_dec) == 0);
    ASSERT(ninf_dec->type == COWRIE_FLOAT64);
    ASSERT(isinf(ninf_dec->as.f64) && ninf_dec->as.f64 < 0);
    cowrie_free(ninf_val);
    cowrie_free(ninf_dec);
    cowrie_buf_free(&ninf_buf);

    return 1;
}

/* Invariant #4: Trailing garbage must be rejected */
static int test_decode_rejects_trailing_garbage(void) {
    /* Encode a simple map {"a": 1} */
    COWRIEValue *map = cowrie_new_object();
    cowrie_object_set(map, "a", 1, cowrie_new_int64(1));

    COWRIEBuf buf;
    ASSERT(cowrie_encode(map, &buf) == 0);

    /* Append trailing garbage byte */
    uint8_t *padded = malloc(buf.len + 1);
    ASSERT(padded != NULL);
    memcpy(padded, buf.data, buf.len);
    padded[buf.len] = 0xFF;

    COWRIEValue *decoded;
    int result = cowrie_decode(padded, buf.len + 1, &decoded);
    ASSERT(result == -1);

    free(padded);
    cowrie_free(map);
    cowrie_buf_free(&buf);
    return 1;
}

/* Invariant #3: Truncated input must be rejected */
static int test_decode_rejects_truncated(void) {
    /* Encode a map {"a": 1} */
    COWRIEValue *map = cowrie_new_object();
    cowrie_object_set(map, "a", 1, cowrie_new_int64(1));

    COWRIEBuf buf;
    ASSERT(cowrie_encode(map, &buf) == 0);
    ASSERT(buf.len > 4); /* Header is at least 4 bytes */

    /* Try decoding at every truncation point from len-1 down to header */
    for (size_t i = buf.len - 1; i > 4; i--) {
        COWRIEValue *decoded = NULL;
        int result = cowrie_decode(buf.data, i, &decoded);
        ASSERT(result == -1);
        /* decoded should be NULL on failure */
    }

    cowrie_free(map);
    cowrie_buf_free(&buf);
    return 1;
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    printf("COWRIE Gen2 Test Suite\n");
    printf("=====================\n\n");

    printf("Basic Types:\n");
    TEST(null_roundtrip);
    TEST(bool_roundtrip);
    TEST(int64_roundtrip);
    TEST(uint64_roundtrip);
    TEST(float64_roundtrip);
    TEST(string_roundtrip);
    TEST(bytes_roundtrip);

    printf("\nComposite Types:\n");
    TEST(array_roundtrip);
    TEST(object_roundtrip);
    TEST(nested_object);

    printf("\nML Extension Types:\n");
    TEST(tensor_roundtrip);
    TEST(tensor_ref_roundtrip);
    TEST(image_roundtrip);
    TEST(audio_roundtrip);
    TEST(datetime64_roundtrip);
    TEST(uuid128_roundtrip);

    printf("\nGraph Types:\n");
    TEST(node_roundtrip);
    TEST(edge_roundtrip);
    TEST(graph_shard_roundtrip);

    printf("\nSpecial Features:\n");
    TEST(deterministic_encoding);
    TEST(schema_fingerprint);
    TEST(crc32);
    TEST(master_stream);

    printf("\nFixtures:\n");
    TEST(fixtures_core);

    printf("\nOverflow Protection:\n");
    TEST(oversized_adjlist_rejects);
    TEST(oversized_richtext_tokens_rejects);
    TEST(oversized_array_rejects);

    printf("\nAdditional Type Tests:\n");
    TEST(decimal128_roundtrip);
    TEST(bigint_roundtrip);
    TEST(ext_roundtrip);
    TEST(bitmask_roundtrip);
    TEST(empty_bitmask);
    TEST(adjlist_construct_encode);
    TEST(adjlist_int64_construct_encode);
    TEST(rich_text_roundtrip);
    TEST(rich_text_plain);
    TEST(delta_roundtrip);

    printf("\nv3 Inline Encoding Tests:\n");
    TEST(fixint_encoding);
    TEST(fixneg_encoding);
    TEST(large_int64_roundtrip);
    TEST(fixarray_encoding);
    TEST(fixmap_encoding);
    TEST(large_array_roundtrip);
    TEST(large_object_roundtrip);

    printf("\nEncoding Options Tests:\n");
    TEST(omit_null_encoding);
    TEST(framed_none);
    TEST(framed_gzip);
    TEST(decode_with_opts);

    printf("\nEmpty Type Tests:\n");
    TEST(empty_string_roundtrip);
    TEST(empty_bytes_roundtrip);
    TEST(empty_array_roundtrip);
    TEST(empty_object_roundtrip);

    printf("\nBatch Graph Tests:\n");
    TEST(node_batch_roundtrip);
    TEST(edge_batch_roundtrip);

    printf("\nTensor Copy/View Tests:\n");
    TEST(tensor_copy_float32);
    TEST(tensor_copy_float64);
    TEST(tensor_copy_int32);
    TEST(tensor_copy_int64);
    TEST(tensor_view_int32);
    TEST(tensor_view_int64);
    TEST(tensor_view_uint8);

    printf("\nFramed Encode/Decode Tests:\n");
    TEST(framed_gzip_roundtrip);
    TEST(framed_none_roundtrip);

    printf("\nAdditional Tests:\n");
    TEST(encode_sorted_deterministic);
    TEST(deterministic_omit_null);
    TEST(node_multi_label);
    TEST(schema_fingerprint_ext_types);
    TEST(utf8_multibyte_string);
    TEST(utf8_4byte_string);
    TEST(graph_shard_with_props);
    TEST(master_stream_with_crc);
    TEST(invalid_magic_rejects);
    TEST(mixed_type_array);
    TEST(deeply_nested);

    printf("\nNaN/Inf Binary Tests:\n");
    TEST(binary_nan_inf_roundtrip);

    printf("\nInvariant Tests:\n");
    TEST(decode_rejects_trailing_garbage);
    TEST(decode_rejects_truncated);

    printf("\n=====================\n");
    printf("Results: %d/%d tests passed\n", tests_passed, tests_run);

    return (tests_passed == tests_run) ? 0 : 1;
}
