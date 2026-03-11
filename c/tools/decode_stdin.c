/*
 * Decode cowrie binary from stdin, emit JSON to stdout.
 * Used by the differential testing runner.
 *
 * Build:
 *   gcc -o decode_stdin decode_stdin.c -I../include -L../build -lcowrie_gen2 -lcowrie_json -lm
 */

#include "../include/cowrie_gen2.h"
#include "../include/cowrie_json.h"
#include <stdio.h>
#include <stdlib.h>

#define MAX_INPUT (64 * 1024 * 1024) /* 64MB */

int main(void) {
    /* Read all of stdin */
    size_t capacity = 4096;
    size_t len = 0;
    char *buf = malloc(capacity);
    if (!buf) {
        fprintf(stderr, "allocation failed\n");
        return 1;
    }

    while (!feof(stdin)) {
        if (len + 4096 > capacity) {
            capacity *= 2;
            if (capacity > MAX_INPUT) {
                fprintf(stderr, "input too large\n");
                free(buf);
                return 1;
            }
            char *newbuf = realloc(buf, capacity);
            if (!newbuf) {
                fprintf(stderr, "realloc failed\n");
                free(buf);
                return 1;
            }
            buf = newbuf;
        }
        size_t n = fread(buf + len, 1, 4096, stdin);
        if (n == 0) break;
        len += n;
    }

    if (len == 0) {
        fprintf(stderr, "empty input\n");
        free(buf);
        return 1;
    }

    /* Decode */
    COWRIEValue *value = NULL;
    int rc = cowrie_decode(buf, len, &value);
    free(buf);

    if (rc != 0 || !value) {
        fprintf(stderr, "decode error: %d\n", rc);
        return 1;
    }

    /* Convert to JSON */
    char *json = cowrie_to_json(value);
    cowrie_free(value);

    if (!json) {
        fprintf(stderr, "JSON conversion failed\n");
        return 1;
    }

    printf("%s\n", json);
    free(json);
    return 0;
}
