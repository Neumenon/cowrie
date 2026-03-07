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

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
