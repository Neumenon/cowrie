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

static const cowrie_g1_value_t *obj_get(const cowrie_g1_value_t *obj, const char *key) {
    assert(obj != NULL);
    assert(obj->type == COWRIE_G1_TYPE_OBJECT);
    for (size_t i = 0; i < obj->object_val.len; i++) {
        if (strcmp(obj->object_val.members[i].key, key) == 0) {
            return obj->object_val.members[i].value;
        }
    }
    return NULL;
}

static cowrie_g1_value_t *make_node(int64_t id, const char *label, int64_t weight) {
    cowrie_g1_value_t *node = cowrie_g1_object(3);
    cowrie_g1_value_t *props = cowrie_g1_object(1);
    assert(node != NULL);
    assert(props != NULL);
    assert(cowrie_g1_object_set(props, "weight", cowrie_g1_int64(weight)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(node, "id", cowrie_g1_int64(id)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(node, "label", cowrie_g1_string(label, strlen(label))) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(node, "properties", props) == COWRIE_G1_OK);
    return node;
}

static cowrie_g1_value_t *make_edge(int64_t src, int64_t dst, const char *label, int64_t weight) {
    cowrie_g1_value_t *edge = cowrie_g1_object(4);
    cowrie_g1_value_t *props = cowrie_g1_object(1);
    assert(edge != NULL);
    assert(props != NULL);
    assert(cowrie_g1_object_set(props, "weight", cowrie_g1_int64(weight)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(edge, "src", cowrie_g1_int64(src)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(edge, "dst", cowrie_g1_int64(dst)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(edge, "label", cowrie_g1_string(label, strlen(label))) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(edge, "properties", props) == COWRIE_G1_OK);
    return edge;
}

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

TEST(graph_node) {
    cowrie_g1_value_t *node = make_node(42, "user", 7);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(node, &buf);
    assert(err == COWRIE_G1_OK);
    assert(buf.len > 0);
    assert(buf.data[0] == COWRIE_G1_TAG_NODE);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_OBJECT);
    assert(decoded->object_val.len == 3);

    const cowrie_g1_value_t *id = obj_get(decoded, "id");
    const cowrie_g1_value_t *label = obj_get(decoded, "label");
    const cowrie_g1_value_t *properties = obj_get(decoded, "properties");
    assert(id != NULL && id->type == COWRIE_G1_TYPE_INT64 && id->int64_val == 42);
    assert(label != NULL && label->type == COWRIE_G1_TYPE_STRING && strcmp(label->string_val.data, "user") == 0);
    assert(properties != NULL && properties->type == COWRIE_G1_TYPE_OBJECT);
    const cowrie_g1_value_t *weight = obj_get(properties, "weight");
    assert(weight != NULL && weight->type == COWRIE_G1_TYPE_INT64 && weight->int64_val == 7);

    cowrie_g1_value_free(node);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(graph_edge) {
    cowrie_g1_value_t *edge = make_edge(1, 2, "follows", 3);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(edge, &buf);
    assert(err == COWRIE_G1_OK);
    assert(buf.len > 0);
    assert(buf.data[0] == COWRIE_G1_TAG_EDGE);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_OBJECT);
    assert(decoded->object_val.len == 4);

    const cowrie_g1_value_t *src = obj_get(decoded, "src");
    const cowrie_g1_value_t *dst = obj_get(decoded, "dst");
    const cowrie_g1_value_t *label = obj_get(decoded, "label");
    const cowrie_g1_value_t *properties = obj_get(decoded, "properties");
    assert(src != NULL && src->type == COWRIE_G1_TYPE_INT64 && src->int64_val == 1);
    assert(dst != NULL && dst->type == COWRIE_G1_TYPE_INT64 && dst->int64_val == 2);
    assert(label != NULL && label->type == COWRIE_G1_TYPE_STRING && strcmp(label->string_val.data, "follows") == 0);
    assert(properties != NULL && properties->type == COWRIE_G1_TYPE_OBJECT);
    const cowrie_g1_value_t *weight = obj_get(properties, "weight");
    assert(weight != NULL && weight->type == COWRIE_G1_TYPE_INT64 && weight->int64_val == 3);

    cowrie_g1_value_free(edge);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(graph_adjlist) {
    uint8_t col_indices[] = {
        1, 0, 0, 0,
        2, 0, 0, 0,
        3, 0, 0, 0
    };
    cowrie_g1_value_t *adj = cowrie_g1_object(5);
    cowrie_g1_value_t *row_offsets = cowrie_g1_array(3);
    assert(adj != NULL);
    assert(row_offsets != NULL);
    assert(cowrie_g1_array_append(row_offsets, cowrie_g1_int64(0)) == COWRIE_G1_OK);
    assert(cowrie_g1_array_append(row_offsets, cowrie_g1_int64(1)) == COWRIE_G1_OK);
    assert(cowrie_g1_array_append(row_offsets, cowrie_g1_int64(3)) == COWRIE_G1_OK);

    assert(cowrie_g1_object_set(adj, "id_width", cowrie_g1_int64(1)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(adj, "node_count", cowrie_g1_int64(2)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(adj, "edge_count", cowrie_g1_int64(3)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(adj, "row_offsets", row_offsets) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(adj, "col_indices", cowrie_g1_bytes(col_indices, sizeof(col_indices))) == COWRIE_G1_OK);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(adj, &buf);
    assert(err == COWRIE_G1_OK);
    assert(buf.len > 0);
    assert(buf.data[0] == COWRIE_G1_TAG_ADJLIST);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_OBJECT);
    assert(decoded->object_val.len == 5);

    const cowrie_g1_value_t *id_width = obj_get(decoded, "id_width");
    const cowrie_g1_value_t *node_count = obj_get(decoded, "node_count");
    const cowrie_g1_value_t *edge_count = obj_get(decoded, "edge_count");
    const cowrie_g1_value_t *decoded_row_offsets = obj_get(decoded, "row_offsets");
    const cowrie_g1_value_t *decoded_col_indices = obj_get(decoded, "col_indices");
    assert(id_width != NULL && id_width->type == COWRIE_G1_TYPE_INT64 && id_width->int64_val == 1);
    assert(node_count != NULL && node_count->type == COWRIE_G1_TYPE_INT64 && node_count->int64_val == 2);
    assert(edge_count != NULL && edge_count->type == COWRIE_G1_TYPE_INT64 && edge_count->int64_val == 3);
    assert(decoded_row_offsets != NULL && decoded_row_offsets->type == COWRIE_G1_TYPE_ARRAY);
    assert(decoded_row_offsets->array_val.len == 3);
    assert(decoded_row_offsets->array_val.items[0]->int64_val == 0);
    assert(decoded_row_offsets->array_val.items[1]->int64_val == 1);
    assert(decoded_row_offsets->array_val.items[2]->int64_val == 3);
    assert(decoded_col_indices != NULL && decoded_col_indices->type == COWRIE_G1_TYPE_BYTES);
    assert(decoded_col_indices->bytes_val.len == sizeof(col_indices));
    assert(memcmp(decoded_col_indices->bytes_val.data, col_indices, sizeof(col_indices)) == 0);

    cowrie_g1_value_free(adj);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(graph_node_batch) {
    cowrie_g1_value_t *batch = cowrie_g1_object(1);
    cowrie_g1_value_t *nodes = cowrie_g1_array(2);
    assert(batch != NULL);
    assert(nodes != NULL);
    assert(cowrie_g1_array_append(nodes, make_node(1, "a", 10)) == COWRIE_G1_OK);
    assert(cowrie_g1_array_append(nodes, make_node(2, "b", 20)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(batch, "nodes", nodes) == COWRIE_G1_OK);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(batch, &buf);
    assert(err == COWRIE_G1_OK);
    assert(buf.len > 0);
    assert(buf.data[0] == COWRIE_G1_TAG_NODE_BATCH);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_OBJECT);
    const cowrie_g1_value_t *decoded_nodes = obj_get(decoded, "nodes");
    assert(decoded_nodes != NULL && decoded_nodes->type == COWRIE_G1_TYPE_ARRAY);
    assert(decoded_nodes->array_val.len == 2);
    const cowrie_g1_value_t *first = decoded_nodes->array_val.items[0];
    assert(first != NULL && first->type == COWRIE_G1_TYPE_OBJECT);
    const cowrie_g1_value_t *first_id = obj_get(first, "id");
    assert(first_id != NULL && first_id->type == COWRIE_G1_TYPE_INT64 && first_id->int64_val == 1);

    cowrie_g1_value_free(batch);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(graph_edge_batch) {
    cowrie_g1_value_t *batch = cowrie_g1_object(1);
    cowrie_g1_value_t *edges = cowrie_g1_array(2);
    assert(batch != NULL);
    assert(edges != NULL);
    assert(cowrie_g1_array_append(edges, make_edge(1, 2, "x", 11)) == COWRIE_G1_OK);
    assert(cowrie_g1_array_append(edges, make_edge(2, 3, "y", 12)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(batch, "edges", edges) == COWRIE_G1_OK);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(batch, &buf);
    assert(err == COWRIE_G1_OK);
    assert(buf.len > 0);
    assert(buf.data[0] == COWRIE_G1_TAG_EDGE_BATCH);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_OBJECT);
    const cowrie_g1_value_t *decoded_edges = obj_get(decoded, "edges");
    assert(decoded_edges != NULL && decoded_edges->type == COWRIE_G1_TYPE_ARRAY);
    assert(decoded_edges->array_val.len == 2);
    const cowrie_g1_value_t *first = decoded_edges->array_val.items[0];
    assert(first != NULL && first->type == COWRIE_G1_TYPE_OBJECT);
    const cowrie_g1_value_t *first_src = obj_get(first, "src");
    assert(first_src != NULL && first_src->type == COWRIE_G1_TYPE_INT64 && first_src->int64_val == 1);

    cowrie_g1_value_free(batch);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(graph_shard) {
    cowrie_g1_value_t *shard = cowrie_g1_object(3);
    cowrie_g1_value_t *nodes = cowrie_g1_array(2);
    cowrie_g1_value_t *edges = cowrie_g1_array(1);
    cowrie_g1_value_t *meta = cowrie_g1_object(2);
    assert(shard != NULL);
    assert(nodes != NULL);
    assert(edges != NULL);
    assert(meta != NULL);
    assert(cowrie_g1_array_append(nodes, make_node(10, "n1", 1)) == COWRIE_G1_OK);
    assert(cowrie_g1_array_append(nodes, make_node(11, "n2", 2)) == COWRIE_G1_OK);
    assert(cowrie_g1_array_append(edges, make_edge(10, 11, "e1", 5)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(meta, "partition", cowrie_g1_int64(7)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(meta, "region", cowrie_g1_string("us", 2)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(shard, "nodes", nodes) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(shard, "edges", edges) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(shard, "meta", meta) == COWRIE_G1_OK);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(shard, &buf);
    assert(err == COWRIE_G1_OK);
    assert(buf.len > 0);
    assert(buf.data[0] == COWRIE_G1_TAG_GRAPH_SHARD);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_OBJECT);
    assert(decoded->object_val.len == 3);

    const cowrie_g1_value_t *decoded_nodes = obj_get(decoded, "nodes");
    const cowrie_g1_value_t *decoded_edges = obj_get(decoded, "edges");
    const cowrie_g1_value_t *decoded_meta = obj_get(decoded, "meta");
    assert(decoded_nodes != NULL && decoded_nodes->type == COWRIE_G1_TYPE_ARRAY);
    assert(decoded_nodes->array_val.len == 2);
    assert(decoded_edges != NULL && decoded_edges->type == COWRIE_G1_TYPE_ARRAY);
    assert(decoded_edges->array_val.len == 1);
    assert(decoded_meta != NULL && decoded_meta->type == COWRIE_G1_TYPE_OBJECT);
    const cowrie_g1_value_t *partition = obj_get(decoded_meta, "partition");
    const cowrie_g1_value_t *region = obj_get(decoded_meta, "region");
    assert(partition != NULL && partition->type == COWRIE_G1_TYPE_INT64 && partition->int64_val == 7);
    assert(region != NULL && region->type == COWRIE_G1_TYPE_STRING && strcmp(region->string_val.data, "us") == 0);

    cowrie_g1_value_free(shard);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(float64) {
    double test_values[] = {0.0, 1.0, -1.0, 3.14159, -2.71828, 1e100, -1e-100, 1e308, -1e308};
    int num_values = sizeof(test_values) / sizeof(test_values[0]);

    for (int i = 0; i < num_values; i++) {
        cowrie_g1_value_t *v = cowrie_g1_float64(test_values[i]);
        assert(v != NULL);
        assert(v->type == COWRIE_G1_TYPE_FLOAT64);
        assert(v->float64_val == test_values[i]);

        cowrie_g1_buf_t buf;
        cowrie_g1_encode(v, &buf);

        cowrie_g1_value_t *decoded;
        cowrie_g1_decode(buf.data, buf.len, &decoded);
        assert(decoded->type == COWRIE_G1_TYPE_FLOAT64);
        assert(decoded->float64_val == test_values[i]);

        cowrie_g1_value_free(v);
        cowrie_g1_value_free(decoded);
        cowrie_g1_buf_free(&buf);
    }
}

TEST(bytes) {
    uint8_t data[] = {0x00, 0x01, 0xFE, 0xFF, 0x42, 0x80};
    cowrie_g1_value_t *v = cowrie_g1_bytes(data, sizeof(data));
    assert(v != NULL);
    assert(v->type == COWRIE_G1_TYPE_BYTES);
    assert(v->bytes_val.len == sizeof(data));
    assert(memcmp(v->bytes_val.data, data, sizeof(data)) == 0);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(v, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_BYTES);
    assert(decoded->bytes_val.len == sizeof(data));
    assert(memcmp(decoded->bytes_val.data, data, sizeof(data)) == 0);

    cowrie_g1_value_free(v);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(int64_array) {
    int64_t data[] = {0, 1, -1, 42, -42, 1000000, -1000000, INT64_MAX, INT64_MIN};
    cowrie_g1_value_t *v = cowrie_g1_int64_array(data, 9);
    assert(v != NULL);
    assert(v->type == COWRIE_G1_TYPE_INT64_ARRAY);
    assert(v->int64_array_val.len == 9);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(v, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_INT64_ARRAY);
    assert(decoded->int64_array_val.len == 9);
    for (int i = 0; i < 9; i++) {
        assert(decoded->int64_array_val.data[i] == data[i]);
    }

    cowrie_g1_value_free(v);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(string_array) {
    const char *strings[] = {"hello", "world", "foo", "bar"};
    cowrie_g1_value_t *v = cowrie_g1_string_array(strings, 4);
    assert(v != NULL);
    assert(v->type == COWRIE_G1_TYPE_STRING_ARRAY);
    assert(v->string_array_val.len == 4);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(v, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_STRING_ARRAY);
    assert(decoded->string_array_val.len == 4);
    for (int i = 0; i < 4; i++) {
        assert(strcmp(decoded->string_array_val.data[i], strings[i]) == 0);
    }

    cowrie_g1_value_free(v);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(nested_array) {
    cowrie_g1_value_t *outer = cowrie_g1_array(2);
    cowrie_g1_value_t *inner1 = cowrie_g1_array(2);
    cowrie_g1_value_t *inner2 = cowrie_g1_array(1);
    assert(cowrie_g1_array_append(inner1, cowrie_g1_int64(1)) == COWRIE_G1_OK);
    assert(cowrie_g1_array_append(inner1, cowrie_g1_int64(2)) == COWRIE_G1_OK);
    assert(cowrie_g1_array_append(inner2, cowrie_g1_string("nested", 6)) == COWRIE_G1_OK);
    assert(cowrie_g1_array_append(outer, inner1) == COWRIE_G1_OK);
    assert(cowrie_g1_array_append(outer, inner2) == COWRIE_G1_OK);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(outer, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_ARRAY);
    assert(decoded->array_val.len == 2);
    assert(decoded->array_val.items[0]->type == COWRIE_G1_TYPE_ARRAY);
    assert(decoded->array_val.items[0]->array_val.len == 2);
    assert(decoded->array_val.items[0]->array_val.items[0]->int64_val == 1);
    assert(decoded->array_val.items[1]->type == COWRIE_G1_TYPE_ARRAY);
    assert(decoded->array_val.items[1]->array_val.len == 1);
    assert(decoded->array_val.items[1]->array_val.items[0]->type == COWRIE_G1_TYPE_STRING);

    cowrie_g1_value_free(outer);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(large_int_values) {
    int64_t test_values[] = {INT64_MAX, INT64_MIN, INT64_MAX - 1, INT64_MIN + 1,
                              32767, -32768, 2147483647LL, -2147483648LL};
    int num_values = sizeof(test_values) / sizeof(test_values[0]);

    for (int i = 0; i < num_values; i++) {
        cowrie_g1_value_t *v = cowrie_g1_int64(test_values[i]);
        assert(v != NULL);

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

TEST(mixed_array) {
    cowrie_g1_value_t *arr = cowrie_g1_array(0);
    assert(cowrie_g1_array_append(arr, cowrie_g1_null()) == COWRIE_G1_OK);
    assert(cowrie_g1_array_append(arr, cowrie_g1_bool(true)) == COWRIE_G1_OK);
    assert(cowrie_g1_array_append(arr, cowrie_g1_int64(42)) == COWRIE_G1_OK);
    assert(cowrie_g1_array_append(arr, cowrie_g1_float64(3.14)) == COWRIE_G1_OK);
    assert(cowrie_g1_array_append(arr, cowrie_g1_string("hello", 5)) == COWRIE_G1_OK);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(arr, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_ARRAY);
    assert(decoded->array_val.len == 5);
    assert(decoded->array_val.items[0]->type == COWRIE_G1_TYPE_NULL);
    assert(decoded->array_val.items[1]->type == COWRIE_G1_TYPE_BOOL);
    assert(decoded->array_val.items[1]->bool_val == true);
    assert(decoded->array_val.items[2]->type == COWRIE_G1_TYPE_INT64);
    assert(decoded->array_val.items[2]->int64_val == 42);
    assert(decoded->array_val.items[3]->type == COWRIE_G1_TYPE_FLOAT64);
    assert(decoded->array_val.items[4]->type == COWRIE_G1_TYPE_STRING);

    cowrie_g1_value_free(arr);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(nested_object) {
    cowrie_g1_value_t *outer = cowrie_g1_object(2);
    cowrie_g1_value_t *inner = cowrie_g1_object(2);
    assert(cowrie_g1_object_set(inner, "x", cowrie_g1_int64(10)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(inner, "y", cowrie_g1_int64(20)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(outer, "point", inner) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(outer, "label", cowrie_g1_string("test", 4)) == COWRIE_G1_OK);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(outer, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_OBJECT);
    assert(decoded->object_val.len == 2);

    const cowrie_g1_value_t *point = obj_get(decoded, "point");
    assert(point != NULL && point->type == COWRIE_G1_TYPE_OBJECT);
    assert(point->object_val.len == 2);
    const cowrie_g1_value_t *x = obj_get(point, "x");
    assert(x != NULL && x->type == COWRIE_G1_TYPE_INT64 && x->int64_val == 10);

    cowrie_g1_value_free(outer);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(object_key_update) {
    cowrie_g1_value_t *obj = cowrie_g1_object(2);
    assert(cowrie_g1_object_set(obj, "key", cowrie_g1_int64(1)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(obj, "key", cowrie_g1_int64(2)) == COWRIE_G1_OK);
    assert(obj->object_val.len == 1);

    const cowrie_g1_value_t *val = obj_get(obj, "key");
    assert(val != NULL && val->int64_val == 2);

    cowrie_g1_value_free(obj);
}

TEST(array_grow) {
    cowrie_g1_value_t *arr = cowrie_g1_array(0);
    for (int i = 0; i < 100; i++) {
        assert(cowrie_g1_array_append(arr, cowrie_g1_int64(i)) == COWRIE_G1_OK);
    }
    assert(arr->array_val.len == 100);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(arr, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->array_val.len == 100);
    assert(decoded->array_val.items[99]->int64_val == 99);

    cowrie_g1_value_free(arr);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

TEST(truncated_data_rejects) {
    /* Encode a valid value, then truncate it */
    cowrie_g1_value_t *v = cowrie_g1_string("hello world test data", 21);
    cowrie_g1_buf_t buf;
    cowrie_g1_encode(v, &buf);
    cowrie_g1_value_free(v);

    /* Truncate to just the tag byte */
    cowrie_g1_value_t *decoded;
    int err = cowrie_g1_decode(buf.data, 1, &decoded);
    assert(err != COWRIE_G1_OK);

    /* Empty data */
    err = cowrie_g1_decode(buf.data, 0, &decoded);
    assert(err != COWRIE_G1_OK);

    cowrie_g1_buf_free(&buf);
}

TEST(long_string) {
    /* Test a longer string to exercise varint encoding */
    char long_str[1024];
    memset(long_str, 'A', sizeof(long_str));
    long_str[sizeof(long_str) - 1] = '\0';
    cowrie_g1_value_t *v = cowrie_g1_string(long_str, sizeof(long_str) - 1);
    assert(v != NULL);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(v, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_STRING);
    assert(decoded->string_val.len == sizeof(long_str) - 1);
    assert(memcmp(decoded->string_val.data, long_str, sizeof(long_str) - 1) == 0);

    cowrie_g1_value_free(v);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

/* Test object_set capacity growth (start with 0 capacity, grow via set) */
TEST(object_grow) {
    cowrie_g1_value_t *obj = cowrie_g1_object(0);
    assert(obj != NULL);
    assert(obj->type == COWRIE_G1_TYPE_OBJECT);

    /* Add several fields to trigger growth */
    for (int i = 0; i < 10; i++) {
        char key[16];
        snprintf(key, sizeof(key), "key%d", i);
        assert(cowrie_g1_object_set(obj, key, cowrie_g1_int64(i)) == COWRIE_G1_OK);
    }
    assert(obj->object_val.len == 10);

    /* Encode and decode roundtrip */
    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(obj, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_OBJECT);
    assert(decoded->object_val.len == 10);

    cowrie_g1_value_free(obj);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

/* Test empty string array */
TEST(empty_string_array) {
    cowrie_g1_value_t *v = cowrie_g1_string_array(NULL, 0);
    assert(v != NULL);
    assert(v->type == COWRIE_G1_TYPE_STRING_ARRAY);
    assert(v->string_array_val.len == 0);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(v, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_STRING_ARRAY);
    assert(decoded->string_array_val.len == 0);

    cowrie_g1_value_free(v);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

/* Test encoding a NULL pointer value (should encode as null tag) */
TEST(null_pointer_encode) {
    cowrie_g1_buf_t buf;
    cowrie_g1_buf_init(&buf);

    /* Create an array containing a NULL element */
    cowrie_g1_value_t *arr = cowrie_g1_array(2);
    assert(arr != NULL);
    cowrie_g1_array_append(arr, cowrie_g1_int64(1));
    cowrie_g1_array_append(arr, NULL); /* NULL pointer */

    int err = cowrie_g1_encode(arr, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_ARRAY);
    assert(decoded->array_val.len == 2);
    assert(decoded->array_val.items[1]->type == COWRIE_G1_TYPE_NULL);

    cowrie_g1_value_free(arr);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

/* Test float64_array roundtrip (also covers the decode path more thoroughly) */
TEST(float64_array_roundtrip) {
    double data[] = {1.5, -2.5, 3.14159, 0.0, -0.0};
    cowrie_g1_value_t *v = cowrie_g1_float64_array(data, 5);
    assert(v != NULL);
    assert(v->type == COWRIE_G1_TYPE_FLOAT64_ARRAY);
    assert(v->float64_array_val.len == 5);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(v, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_FLOAT64_ARRAY);
    assert(decoded->float64_array_val.len == 5);
    for (int i = 0; i < 5; i++) {
        assert(decoded->float64_array_val.data[i] == data[i]);
    }

    cowrie_g1_value_free(v);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

/* Test empty bytes roundtrip */
TEST(empty_bytes) {
    cowrie_g1_value_t *v = cowrie_g1_bytes(NULL, 0);
    assert(v != NULL);
    assert(v->type == COWRIE_G1_TYPE_BYTES);
    assert(v->bytes_val.len == 0);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(v, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_BYTES);
    assert(decoded->bytes_val.len == 0);

    cowrie_g1_value_free(v);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

/* Test empty int64 array */
TEST(empty_int64_array) {
    cowrie_g1_value_t *v = cowrie_g1_int64_array(NULL, 0);
    assert(v != NULL);
    assert(v->type == COWRIE_G1_TYPE_INT64_ARRAY);
    assert(v->int64_array_val.len == 0);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(v, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_INT64_ARRAY);
    assert(decoded->int64_array_val.len == 0);

    cowrie_g1_value_free(v);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

/* Test empty float64 array */
TEST(empty_float64_array) {
    cowrie_g1_value_t *v = cowrie_g1_float64_array(NULL, 0);
    assert(v != NULL);
    assert(v->type == COWRIE_G1_TYPE_FLOAT64_ARRAY);
    assert(v->float64_array_val.len == 0);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(v, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_FLOAT64_ARRAY);
    assert(decoded->float64_array_val.len == 0);

    cowrie_g1_value_free(v);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

/* Test adjlist with int64_array row_offsets (covers int_sequence_get INT64_ARRAY path) */
TEST(graph_adjlist_int64arr_offsets) {
    /* col_indices as bytes (12 bytes = 3 x int32) */
    uint8_t col_indices[] = {
        1, 0, 0, 0,
        2, 0, 0, 0,
        0, 0, 0, 0
    };
    /* row_offsets as int64_array instead of regular array */
    int64_t offsets[] = {0, 2, 3};
    cowrie_g1_value_t *row_offsets = cowrie_g1_int64_array(offsets, 3);
    assert(row_offsets != NULL);

    cowrie_g1_value_t *adj = cowrie_g1_object(5);
    assert(adj != NULL);
    assert(cowrie_g1_object_set(adj, "id_width", cowrie_g1_int64(1)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(adj, "node_count", cowrie_g1_int64(2)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(adj, "edge_count", cowrie_g1_int64(3)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(adj, "row_offsets", row_offsets) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(adj, "col_indices", cowrie_g1_bytes(col_indices, sizeof(col_indices))) == COWRIE_G1_OK);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(adj, &buf);
    assert(err == COWRIE_G1_OK);
    assert(buf.len > 0);
    assert(buf.data[0] == COWRIE_G1_TAG_ADJLIST);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_OBJECT);

    cowrie_g1_value_free(adj);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

/* Test adjlist with array-based col_indices (byte_sequence via array of ints) */
TEST(graph_adjlist_array_col) {
    /* Use an array of int64 values (0-255) for col_indices instead of bytes */
    cowrie_g1_value_t *col_arr = cowrie_g1_array(12);
    assert(col_arr != NULL);
    /* 3 edges of int32 (4 bytes each) = 12 int values */
    /* Edge to node 1: little-endian int32 = {1, 0, 0, 0} */
    cowrie_g1_array_append(col_arr, cowrie_g1_int64(1));
    cowrie_g1_array_append(col_arr, cowrie_g1_int64(0));
    cowrie_g1_array_append(col_arr, cowrie_g1_int64(0));
    cowrie_g1_array_append(col_arr, cowrie_g1_int64(0));
    /* Edge to node 2: {2, 0, 0, 0} */
    cowrie_g1_array_append(col_arr, cowrie_g1_int64(2));
    cowrie_g1_array_append(col_arr, cowrie_g1_int64(0));
    cowrie_g1_array_append(col_arr, cowrie_g1_int64(0));
    cowrie_g1_array_append(col_arr, cowrie_g1_int64(0));
    /* Edge to node 0: {0, 0, 0, 0} */
    cowrie_g1_array_append(col_arr, cowrie_g1_int64(0));
    cowrie_g1_array_append(col_arr, cowrie_g1_int64(0));
    cowrie_g1_array_append(col_arr, cowrie_g1_int64(0));
    cowrie_g1_array_append(col_arr, cowrie_g1_int64(0));

    cowrie_g1_value_t *adj = cowrie_g1_object(5);
    cowrie_g1_value_t *row_offsets = cowrie_g1_array(3);
    cowrie_g1_array_append(row_offsets, cowrie_g1_int64(0));
    cowrie_g1_array_append(row_offsets, cowrie_g1_int64(1));
    cowrie_g1_array_append(row_offsets, cowrie_g1_int64(3));

    assert(cowrie_g1_object_set(adj, "id_width", cowrie_g1_int64(1)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(adj, "node_count", cowrie_g1_int64(2)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(adj, "edge_count", cowrie_g1_int64(3)) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(adj, "row_offsets", row_offsets) == COWRIE_G1_OK);
    assert(cowrie_g1_object_set(adj, "col_indices", col_arr) == COWRIE_G1_OK);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(adj, &buf);
    assert(err == COWRIE_G1_OK);
    assert(buf.len > 0);
    assert(buf.data[0] == COWRIE_G1_TAG_ADJLIST);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_free(adj);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

/* Test deeply nested objects to stress depth handling */
TEST(deep_nesting) {
    /* Create a chain of nested objects: {"a": {"a": {"a": ... }}} */
    cowrie_g1_value_t *inner = cowrie_g1_int64(42);
    for (int i = 0; i < 50; i++) {
        cowrie_g1_value_t *obj = cowrie_g1_object(1);
        cowrie_g1_object_set(obj, "a", inner);
        inner = obj;
    }

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(inner, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);

    /* Walk down to verify */
    cowrie_g1_value_t *cur = decoded;
    for (int i = 0; i < 50; i++) {
        assert(cur->type == COWRIE_G1_TYPE_OBJECT);
        cur = cur->object_val.members[0].value;
    }
    assert(cur->type == COWRIE_G1_TYPE_INT64);
    assert(cur->int64_val == 42);

    cowrie_g1_value_free(inner);
    cowrie_g1_value_free(decoded);
    cowrie_g1_buf_free(&buf);
}

/* Test invalid tag byte rejection */
TEST(invalid_tag_rejects) {
    uint8_t data[] = {0xFF}; /* invalid tag */
    cowrie_g1_value_t *decoded = NULL;
    int err = cowrie_g1_decode(data, 1, &decoded);
    assert(err != COWRIE_G1_OK);
    assert(decoded == NULL);
}

/* Test multiple string arrays to cover multi-element paths */
TEST(multi_string_array) {
    const char *strings[] = {"hello", "world", "foo", "bar", "baz"};
    cowrie_g1_value_t *v = cowrie_g1_string_array(strings, 5);
    assert(v != NULL);
    assert(v->type == COWRIE_G1_TYPE_STRING_ARRAY);
    assert(v->string_array_val.len == 5);

    cowrie_g1_buf_t buf;
    int err = cowrie_g1_encode(v, &buf);
    assert(err == COWRIE_G1_OK);

    cowrie_g1_value_t *decoded;
    err = cowrie_g1_decode(buf.data, buf.len, &decoded);
    assert(err == COWRIE_G1_OK);
    assert(decoded->type == COWRIE_G1_TYPE_STRING_ARRAY);
    assert(decoded->string_array_val.len == 5);
    assert(strcmp(decoded->string_array_val.data[0], "hello") == 0);
    assert(strcmp(decoded->string_array_val.data[4], "baz") == 0);

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
    run_test_graph_node();
    run_test_graph_edge();
    run_test_graph_adjlist();
    run_test_graph_node_batch();
    run_test_graph_edge_batch();
    run_test_graph_shard();
    run_test_float64();
    run_test_bytes();
    run_test_int64_array();
    run_test_string_array();
    run_test_nested_array();
    run_test_large_int_values();
    run_test_mixed_array();
    run_test_nested_object();
    run_test_object_key_update();
    run_test_array_grow();
    run_test_truncated_data_rejects();
    run_test_long_string();
    run_test_object_grow();
    run_test_empty_string_array();
    run_test_null_pointer_encode();
    run_test_float64_array_roundtrip();
    run_test_empty_bytes();
    run_test_empty_int64_array();
    run_test_empty_float64_array();
    run_test_graph_adjlist_int64arr_offsets();
    run_test_graph_adjlist_array_col();
    run_test_deep_nesting();
    run_test_invalid_tag_rejects();
    run_test_multi_string_array();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
