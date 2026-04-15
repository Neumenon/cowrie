/*
 * Decode cowrie binary from stdin, emit JSON to stdout.
 * Used by the differential testing runner.
 *
 * Build:
 *   gcc -o decode_stdin decode_stdin.c -I../include -L../build -lcowrie_gen2 -lm
 */

#include "../include/cowrie_gen2.h"
#include "../include/cowrie_json.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_INPUT (64 * 1024 * 1024) /* 64MB */

int main(void) {
    size_t capacity = 4096;
    size_t len = 0;
    uint8_t *buf = malloc(capacity);
    if (!buf) {
        fprintf(stderr, "allocation failed\n");
        return 1;
    }

    for (;;) {
        if (len + 4096 > capacity) {
            size_t new_cap = capacity * 2;
            if (new_cap > MAX_INPUT) {
                fprintf(stderr, "input too large\n");
                free(buf);
                return 1;
            }
            uint8_t *newbuf = realloc(buf, new_cap);
            if (!newbuf) {
                fprintf(stderr, "realloc failed\n");
                free(buf);
                return 1;
            }
            buf = newbuf;
            capacity = new_cap;
        }
        size_t n = fread(buf + len, 1, 4096, stdin);
        len += n;
        if (n < 4096) {
            if (ferror(stdin)) {
                fprintf(stderr, "stdin read error\n");
                free(buf);
                return 1;
            }
            break; /* EOF */
        }
    }

    if (len == 0) {
        fprintf(stderr, "empty input\n");
        free(buf);
        return 1;
    }

    COWRIEValue *value = NULL;
    int rc = cowrie_decode(buf, len, &value);
    free(buf);

    if (rc != 0 || !value) {
        fprintf(stderr, "decode error: %d\n", rc);
        return 1;
    }

    COWRIEBuf json;
    rc = cowrie_to_json(value, &json);
    cowrie_free(value);

    if (rc != 0) {
        fprintf(stderr, "JSON conversion failed: %d\n", rc);
        return 1;
    }

    fwrite(json.data, 1, json.len, stdout);
    fputc('\n', stdout);
    free(json.data);
    return 0;
}
