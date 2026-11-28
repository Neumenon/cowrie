// cowrie_route.h — Native Cowrie v1.2 Agent Memory, `.cwwal` WAL & Ed25519 Causal Trust Membrane
// for Eureka `llb-serve` (100% parity with vLLM CowrieVllmRuntime & llama.cpp LlamaCowrieMemory).
//
// HTTP Endpoints wired into Eureka `llb-serve`:
//   POST /v1/memory/store   {payload, authority?, epistemic?, parents?: [cid|handle,...]}
//                           -> canonical COWR frame + sticky BIT_TAINTED (0x80) + .cwwal WAL + 0600 .cwfr
//   GET  /v1/memory/triage  -> compact 10-letter 47-bit Alpha32 R0 handle list
//   POST /v1/memory/lens    {id: cid|handle, tier?: "r0"|"r1"|"r2"}
//                           -> CID-fenced deterministic R0/R1/R2 Attention Lens (`<<<` breakout escaped)
//   POST /v1/trust/gate     {action: "shell_exec"|"cred_read"|"fs_write"|"net_exfil"|"read_only",
//                            arg_ids: [cid|handle,...], declassify_tokens?: [{cid,expiry,sig}],
//                            owner_pubkey?: "<64-hex>"}
//                           -> 200 OK or 403 ERR_TAINTED_EXECUTION_BLOCKED (fail-closed on empty/unknown/colliding)
#ifndef COWRIE_ROUTE_H
#define COWRIE_ROUTE_H

#ifndef _WIN32
#include <dirent.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>

typedef struct {
    uint8_t *data;
    size_t   len;
    size_t   cap;
} CowrieSrvBuf;

typedef struct CowrieWalHandle CowrieWalHandle;

typedef struct {
    int loaded;
    void *dl;
    void        (*buf_free)(CowrieSrvBuf);
    const char *(*err_name)(int);
    int         (*frame_verify)(const uint8_t *, size_t, uint8_t *, uint8_t *, uint8_t *);
    int         (*encode_json)(const uint8_t *, size_t, uint8_t, uint8_t, CowrieSrvBuf *);
    int         (*project_lens)(const uint8_t *, size_t, uint8_t, size_t, CowrieSrvBuf *);
    uint8_t     (*propagate_authority)(uint8_t, const uint8_t *, size_t);
    uint8_t     (*is_tainted)(uint8_t);
    uint8_t     (*has_shani)(void);
    int         (*declassify_verify)(const uint8_t *, const uint8_t *, uint8_t, uint64_t, const uint8_t *, uint64_t);
    int         (*wal_open)(const char *, uint8_t, CowrieWalHandle **);
    int         (*wal_append)(CowrieWalHandle *, const uint8_t *, size_t, uint8_t, uint64_t *, uint8_t *);
    size_t      (*wal_len)(const CowrieWalHandle *);
    size_t      (*wal_live_len)(const CowrieWalHandle *);
    int         (*wal_live_frame_at)(const CowrieWalHandle *, size_t, CowrieSrvBuf *);
} CowrieSrvApi;

typedef struct {
    char     cid_hex[69];   // 34B multihash hex (68 chars + NUL)
    char     handle[24];    // 10-letter 47-bit "@abcdefghij"
    uint8_t  authority;     // out-of-band auth + BIT_TAINTED (0x80)
    uint8_t  epistemic;
    uint8_t *frame_bytes;
    size_t   frame_len;
    char    *r0;
} CowrieMemNode;

#define COWRIE_MAX_MEM 4096
static CowrieSrvApi    cowrie_srv;
static CowrieMemNode   cowrie_mem[COWRIE_MAX_MEM];
static size_t          cowrie_mem_n;
static CowrieWalHandle *cowrie_srv_wal;
static int             cowrie_disk_loaded;
static unsigned long   cowrie_tmp_seq;
static pthread_mutex_t cowrie_mem_lock = PTHREAD_MUTEX_INITIALIZER;

static void cowrie_hex34(const uint8_t cid[34], char out[69]) {
    static const char H[] = "0123456789abcdef";
    for (int i = 0; i < 34; i++) {
        out[2 * i]     = H[(cid[i] >> 4) & 0xF];
        out[2 * i + 1] = H[cid[i] & 0xF];
    }
    out[68] = 0;
}

static int cowrie_parse_hex_n(const char *hex, uint8_t *out, size_t nbytes) {
    if (!hex || strlen(hex) != 2 * nbytes) return 0;
    for (size_t i = 0; i < nbytes; i++) {
        int v[2];
        for (int k = 0; k < 2; k++) {
            char ch = hex[2 * i + k];
            if (ch >= '0' && ch <= '9') v[k] = ch - '0';
            else if (ch >= 'a' && ch <= 'f') v[k] = ch - 'a' + 10;
            else if (ch >= 'A' && ch <= 'F') v[k] = ch - 'A' + 10;
            else return 0;
        }
        out[i] = (uint8_t)((v[0] << 4) | v[1]);
    }
    return 1;
}

static CowrieMemNode *cowrie_find_strict_locked(const char *id, int *out_ambig) {
    if (out_ambig) *out_ambig = 0;
    if (!id || !*id) return NULL;
    size_t ilen = strlen(id);
    CowrieMemNode *found = NULL;
    for (size_t i = 0; i < cowrie_mem_n; i++) {
        if (!strcmp(cowrie_mem[i].cid_hex, id)) return &cowrie_mem[i];
        int match = 0;
        if (id[0] == '@' && !strcmp(cowrie_mem[i].handle, id)) match = 1;
        else if (id[0] != '@' && ilen >= 32 && !strncmp(cowrie_mem[i].cid_hex, id, ilen)) match = 1;
        if (match) {
            if (found && strcmp(found->cid_hex, cowrie_mem[i].cid_hex) != 0) {
                if (out_ambig) *out_ambig = 1;
                return NULL;
            }
            found = &cowrie_mem[i];
        }
    }
    return found;
}

static void cowrie_index_raw_frame_locked(const uint8_t *raw, size_t len) {
    uint8_t v_auth = 0, v_epi = 0, cid[34];
    if (cowrie_srv.frame_verify(raw, len, &v_auth, &v_epi, cid) != 0) return;
    char cid_hex[69];
    cowrie_hex34(cid, cid_hex);

    CowrieSrvBuf r0b = {0};
    int rc = cowrie_srv.project_lens(raw, len, 0, 0, &r0b);
    char *r0_str = (rc == 0 && r0b.len > 0) ? strndup((const char *)r0b.data, r0b.len) : strdup("@unknown");
    if (rc == 0) cowrie_srv.buf_free(r0b);

    char handle[24] = "@unknown";
    const char *sp = strchr(r0_str, ' ');
    size_t hlen = sp ? (size_t)(sp - r0_str) : strlen(r0_str);
    if (hlen > 0 && hlen < sizeof handle) {
        memcpy(handle, r0_str, hlen);
        handle[hlen] = 0;
    }

    CowrieMemNode *slot = cowrie_find_strict_locked(cid_hex, NULL);
    if (slot && cowrie_srv.is_tainted(slot->authority) && !cowrie_srv.is_tainted(v_auth)) {
        v_auth |= 0x80; // sticky taint rule!
    }
    if (!slot && cowrie_mem_n < COWRIE_MAX_MEM) {
        slot = &cowrie_mem[cowrie_mem_n++];
        memset(slot, 0, sizeof *slot);
    }
    if (slot) {
        uint8_t *copy = malloc(len > 0 ? len : 1);
        if (!copy) {
            free(r0_str);
            return;
        }
        if (len > 0) memcpy(copy, raw, len);
        free(slot->frame_bytes);
        free(slot->r0);
        strcpy(slot->cid_hex, cid_hex);
        strcpy(slot->handle, handle);
        slot->authority = v_auth;
        slot->epistemic = v_epi;
        slot->frame_bytes = copy;
        slot->frame_len = len;
        slot->r0 = r0_str;
    } else {
        free(r0_str);
    }
}

static int cowrie_srv_init(void) {
    if (cowrie_srv.loaded != 0) return cowrie_srv.loaded > 0;
    const char *cands[7] = {
        getenv("COWRIE_LIB"),
        getenv("COWRIE_LIB_PATH"),
        "/tmp/cowrie-cargo-target/release/libcowrie_core.so",
        "/tmp/cowrie-cargo-target/release/libcowrie_core.dylib",
        "/home/omen/.local/lib/libcowrie_core.so",
        "libcowrie_core.so",
        NULL
    };
    void *dl = NULL;
    for (int i = 0; i < 6; i++) {
        if (!cands[i] || !cands[i][0]) continue;
        dl = dlopen(cands[i], RTLD_LAZY | RTLD_LOCAL);
        if (dl) break;
    }
    if (!dl) {
        cowrie_srv.loaded = -1;
        return 0;
    }
    cowrie_srv.dl                  = dl;
    cowrie_srv.buf_free            = (void (*)(CowrieSrvBuf))dlsym(dl, "cowrie_buf_free");
    cowrie_srv.err_name            = (const char *(*)(int))dlsym(dl, "cowrie_err_name");
    cowrie_srv.frame_verify        = (int (*)(const uint8_t *, size_t, uint8_t *, uint8_t *, uint8_t *))dlsym(dl, "cowrie_frame_verify");
    cowrie_srv.encode_json         = (int (*)(const uint8_t *, size_t, uint8_t, uint8_t, CowrieSrvBuf *))dlsym(dl, "cowrie_encode_json");
    cowrie_srv.project_lens        = (int (*)(const uint8_t *, size_t, uint8_t, size_t, CowrieSrvBuf *))dlsym(dl, "cowrie_project_lens");
    cowrie_srv.propagate_authority = (uint8_t (*)(uint8_t, const uint8_t *, size_t))dlsym(dl, "cowrie_propagate_authority");
    cowrie_srv.is_tainted          = (uint8_t (*)(uint8_t))dlsym(dl, "cowrie_is_tainted");
    cowrie_srv.has_shani           = (uint8_t (*)(void))dlsym(dl, "cowrie_has_shani");
    cowrie_srv.declassify_verify   = (int (*)(const uint8_t *, const uint8_t *, uint8_t, uint64_t, const uint8_t *, uint64_t))dlsym(dl, "cowrie_declassify_verify");
    cowrie_srv.wal_open            = (int (*)(const char *, uint8_t, CowrieWalHandle **))dlsym(dl, "cowrie_wal_open");
    cowrie_srv.wal_append          = (int (*)(CowrieWalHandle *, const uint8_t *, size_t, uint8_t, uint64_t *, uint8_t *))dlsym(dl, "cowrie_wal_append_frame");
    cowrie_srv.wal_len             = (size_t (*)(const CowrieWalHandle *))dlsym(dl, "cowrie_wal_len");
    cowrie_srv.wal_live_len        = (size_t (*)(const CowrieWalHandle *))dlsym(dl, "cowrie_wal_live_len");
    cowrie_srv.wal_live_frame_at   = (int (*)(const CowrieWalHandle *, size_t, CowrieSrvBuf *))dlsym(dl, "cowrie_wal_live_frame_at");
    if (!cowrie_srv.buf_free || !cowrie_srv.err_name || !cowrie_srv.frame_verify || !cowrie_srv.encode_json
        || !cowrie_srv.project_lens || !cowrie_srv.propagate_authority || !cowrie_srv.is_tainted) {
        cowrie_srv.loaded = -1;
        return 0;
    }
    cowrie_srv.loaded = 1;

    pthread_mutex_lock(&cowrie_mem_lock);
    if (!cowrie_disk_loaded) {
        cowrie_disk_loaded = 1;
        const char *mdir = getenv("LLB_COWRIE_MEMORY_DIR");
        if (mdir && mdir[0]) {
            mkdir(mdir, 0700);
            chmod(mdir, 0700);
            if (cowrie_srv.wal_open) {
                char wpath[1024];
                snprintf(wpath, sizeof wpath, "%s/memory.cwwal", mdir);
                if (cowrie_srv.wal_open(wpath, 1, &cowrie_srv_wal) == 0 && cowrie_srv_wal
                    && cowrie_srv.wal_live_len && cowrie_srv.wal_live_frame_at) {
                    size_t nlive = cowrie_srv.wal_live_len(cowrie_srv_wal);
                    for (size_t i = 0; i < nlive; i++) {
                        CowrieSrvBuf fb = {0};
                        if (cowrie_srv.wal_live_frame_at(cowrie_srv_wal, i, &fb) == 0 && fb.data && fb.len > 0) {
                            cowrie_index_raw_frame_locked(fb.data, fb.len);
                            cowrie_srv.buf_free(fb);
                        }
                    }
                }
            }
            DIR *d = opendir(mdir);
            if (d) {
                struct dirent *de;
                while ((de = readdir(d)) != NULL) {
                    if (!strstr(de->d_name, ".cwfr")) continue;
                    char fp_s[1024];
                    snprintf(fp_s, sizeof fp_s, "%s/%s", mdir, de->d_name);
                    FILE *fp = fopen(fp_s, "rb");
                    if (!fp) continue;
                    fseek(fp, 0, SEEK_END);
                    long sz = ftell(fp);
                    fseek(fp, 0, SEEK_SET);
                    if (sz > 0 && sz <= 16 * 1024 * 1024) {
                        uint8_t *buf = malloc((size_t)sz);
                        if (buf && fread(buf, 1, (size_t)sz, fp) == (size_t)sz) {
                            cowrie_index_raw_frame_locked(buf, (size_t)sz);
                        }
                        free(buf);
                    }
                    fclose(fp);
                }
                closedir(d);
            }
        }
    }
    pthread_mutex_unlock(&cowrie_mem_lock);
    return 1;
}

static uint8_t cowrie_parse_auth_str(const char *s) {
    if (!s) return 0x02;
    if (!strcmp(s, "policy") || !strcmp(s, "sys_policy") || !strcmp(s, "0x01")) return 0x01;
    if (!strcmp(s, "verified") || !strcmp(s, "verified_tool") || !strcmp(s, "0x02")) return 0x02;
    if (!strcmp(s, "untrusted") || !strcmp(s, "untrusted_ext") || !strcmp(s, "external") || !strcmp(s, "0x04")) return 0x04;
    if (!strcmp(s, "synth") || !strcmp(s, "model_synth") || !strcmp(s, "0x08")) return 0x08;
    return 0x04;
}

static const char *cowrie_auth_name_buf(uint8_t a, char buf[32]) {
    uint8_t low = a & 0x0F;
    const char *base = low == 0x01 ? "SYS_POLICY"
        : low == 0x02 ? "VERIFIED_TOOL"
        : low == 0x04 ? "UNTRUSTED_EXT"
        : low == 0x08 ? "MODEL_SYNTH"
        : (low & 0x04) ? "MIXED|UNTRUSTED" : "MIXED";
    snprintf(buf, 32, "%s%s", base, (a & 0x80) ? "|TAINTED" : "");
    return buf;
}

static int cowrie_srv_action_code(const char *act) {
    if (!act) return -1;
    if (!strcmp(act, "read_only") || !strcmp(act, "read_only_safe") || !strcmp(act, "search")) return 0;
    if (!strcmp(act, "shell_exec")) return 1;
    if (!strcmp(act, "cred_read"))  return 2;
    if (!strcmp(act, "fs_write"))   return 3;
    if (!strcmp(act, "net_exfil"))  return 4;
    return -1;
}

// POST /v1/memory/store
static void handle_cowrie_store(Conn *c, const char *body_s) {
    if (!cowrie_srv_init()) return err_reply(c, 503, "unavailable_error", "libcowrie_core.so not available");
    const char *err = NULL;
    jv *body = json_parse(body_s, &err);
    if (!body || body->type != J_OBJ) {
        if (body) json_free(body);
        return err_reply(c, 400, "invalid_request_error", err ? err : "JSON object required");
    }
    const jv *payload = json_get(body, "payload");
    if (!payload) {
        json_free(body);
        return err_reply(c, 400, "invalid_request_error", "payload field required");
    }
    const jv *av = json_get(body, "authority");
    uint8_t base_auth = (av && av->type == J_STR) ? cowrie_parse_auth_str(av->str)
        : (av && av->type == J_NUM) ? (uint8_t)(int)av->num : 0x02;
    const jv *ev = json_get(body, "epistemic");
    uint8_t epi = (ev && ev->type == J_NUM) ? (uint8_t)(int)ev->num : 0x01;

    const jv *pv = json_get(body, "parents");
    size_t p_cap = (pv && pv->type == J_ARR && pv->n > 0) ? pv->n : 1;
    uint8_t *parent_auths = malloc(p_cap);
    if (!parent_auths) {
        json_free(body);
        return err_reply(c, 500, "server_error", "OOM allocating parent authority buffer");
    }
    size_t nparents = 0;
    pthread_mutex_lock(&cowrie_mem_lock);
    if (pv && pv->type == J_ARR) {
        for (size_t i = 0; i < pv->n; i++) {
            int ambig = 0;
            CowrieMemNode *pn = (pv->items[i]->type == J_STR)
                ? cowrie_find_strict_locked(pv->items[i]->str, &ambig) : NULL;
            parent_auths[nparents++] = (pn && !ambig) ? pn->authority : (0x04 | 0x80);
        }
    }
    uint8_t eff_auth = cowrie_srv.propagate_authority(base_auth, parent_auths, nparents);
    free(parent_auths);

    sb pjson = {0};
    json_dump(&pjson, payload);
    CowrieSrvBuf wire = {0};
    int rc = cowrie_srv.encode_json((const uint8_t *)pjson.p, pjson.n, eff_auth, epi, &wire);
    free(pjson.p);
    if (rc != 0) {
        pthread_mutex_unlock(&cowrie_mem_lock);
        json_free(body);
        return err_reply(c, 400, "invalid_request_error", cowrie_srv.err_name(rc));
    }

    cowrie_index_raw_frame_locked(wire.data, wire.len);

    uint8_t va = 0, ve = 0, cid[34];
    cowrie_srv.frame_verify(wire.data, wire.len, &va, &ve, cid);
    char cid_hex[69];
    cowrie_hex34(cid, cid_hex);
    CowrieMemNode *stored = cowrie_find_strict_locked(cid_hex, NULL);
    if (stored) eff_auth = stored->authority;

    if (cowrie_srv_wal && cowrie_srv.wal_append) {
        uint64_t seq = 0;
        uint8_t out_c[34];
        cowrie_srv.wal_append(cowrie_srv_wal, wire.data, wire.len, 1, &seq, out_c);
    }

    const char *mdir = getenv("LLB_COWRIE_MEMORY_DIR");
    if (mdir && mdir[0]) {
        mkdir(mdir, 0700);
        chmod(mdir, 0700);
        char fpath[1024], tpath[1024];
        snprintf(fpath, sizeof fpath, "%s/%s.cwfr", mdir, cid_hex);
        unsigned long seq_n = ++cowrie_tmp_seq;
        snprintf(tpath, sizeof tpath, "%s/.%s.%d.%lu.tmp", mdir, cid_hex, (int)getpid(), seq_n);
        int fd = open(tpath, O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (fd >= 0) {
            size_t written = 0;
            int write_ok = 1;
            while (written < wire.len) {
                ssize_t wr = write(fd, wire.data + written, wire.len - written);
                if (wr <= 0) { write_ok = 0; break; }
                written += (size_t)wr;
            }
            if (write_ok && fsync(fd) == 0) {
                close(fd);
                if (rename(tpath, fpath) != 0) unlink(tpath);
            } else {
                close(fd);
                unlink(tpath);
            }
        }
    }

    char handle[24] = "@unknown";
    char *r0_dup = strdup(stored && stored->r0 ? stored->r0 : "@unknown");
    if (stored) snprintf(handle, sizeof handle, "%s", stored->handle);
    size_t flen = wire.len;
    size_t wal_cnt = (cowrie_srv_wal && cowrie_srv.wal_len) ? cowrie_srv.wal_len(cowrie_srv_wal) : 0;
    cowrie_srv.buf_free(wire);
    pthread_mutex_unlock(&cowrie_mem_lock);

    char auth_buf[32];
    sb out = {0};
    sb_add(&out, "{\"cid\":"); json_quote(&out, cid_hex);
    sb_add(&out, ",\"handle\":"); json_quote(&out, handle);
    sb_fmt(&out, ",\"authority\":%u,\"authority_label\":", (unsigned)eff_auth);
    json_quote(&out, cowrie_auth_name_buf(eff_auth, auth_buf));
    sb_fmt(&out, ",\"tainted\":%s,\"bytes\":%zu,\"wal_entries\":%zu,\"r0\":",
        cowrie_srv.is_tainted(eff_auth) ? "true" : "false", flen, wal_cnt);
    json_quote(&out, r0_dup);
    sb_addc(&out, '}');
    free(r0_dup);
    json_free(body);
    json_reply(c, 200, out.p);
    free(out.p);
}

// GET /v1/memory/triage
static void handle_cowrie_triage(Conn *c) {
    if (!cowrie_srv_init()) return err_reply(c, 503, "unavailable_error", "libcowrie_core.so not available");
    sb out = {0}, r0txt = {0};
    pthread_mutex_lock(&cowrie_mem_lock);
    sb_fmt(&out, "{\"count\":%zu,\"handles\":[", cowrie_mem_n);
    for (size_t i = 0; i < cowrie_mem_n; i++) {
        if (i) sb_addc(&out, ',');
        sb_add(&out, "{\"handle\":"); json_quote(&out, cowrie_mem[i].handle);
        sb_add(&out, ",\"cid\":"); json_quote(&out, cowrie_mem[i].cid_hex);
        sb_fmt(&out, ",\"authority\":%u,\"tainted\":%s,\"r0\":",
            (unsigned)cowrie_mem[i].authority, cowrie_srv.is_tainted(cowrie_mem[i].authority) ? "true" : "false");
        json_quote(&out, cowrie_mem[i].r0);
        sb_addc(&out, '}');
        sb_add(&r0txt, cowrie_mem[i].r0);
        sb_addc(&r0txt, '\n');
    }
    pthread_mutex_unlock(&cowrie_mem_lock);
    sb_add(&out, "],\"r0_text\":");
    json_quote(&out, r0txt.p ? r0txt.p : "");
    sb_addc(&out, '}');
    free(r0txt.p);
    json_reply(c, 200, out.p);
    free(out.p);
}

// POST /v1/memory/lens {id, tier?: "r0"|"r1"|"r2"}
static void handle_cowrie_lens(Conn *c, const char *body_s) {
    if (!cowrie_srv_init()) return err_reply(c, 503, "unavailable_error", "libcowrie_core.so not available");
    const char *err = NULL;
    jv *body = json_parse(body_s, &err);
    if (!body || body->type != J_OBJ) {
        if (body) json_free(body);
        return err_reply(c, 400, "invalid_request_error", "JSON object required");
    }
    const jv *iv = json_get(body, "id");
    if (!iv || iv->type != J_STR) {
        json_free(body);
        return err_reply(c, 400, "invalid_request_error", "id (handle or cid) required");
    }
    const jv *tv = json_get(body, "tier");
    uint8_t tier = 2;
    const char *tname = "r2";
    if (tv && tv->type == J_STR) {
        if (!strcmp(tv->str, "r0")) { tier = 0; tname = "r0"; }
        else if (!strcmp(tv->str, "r1")) { tier = 1; tname = "r1"; }
    }
    pthread_mutex_lock(&cowrie_mem_lock);
    int ambig = 0;
    CowrieMemNode *n = cowrie_find_strict_locked(iv->str, &ambig);
    if (!n || ambig) {
        pthread_mutex_unlock(&cowrie_mem_lock);
        json_free(body);
        return err_reply(c, ambig ? 409 : 404, "not_found_error", ambig ? "ambiguous handle collision" : "memory handle or cid not found");
    }
    CowrieSrvBuf lb = {0};
    int rc = cowrie_srv.project_lens(n->frame_bytes, n->frame_len, tier, 0, &lb);
    uint8_t auth = n->authority;
    char cid[69], hdl[24];
    strcpy(cid, n->cid_hex);
    strcpy(hdl, n->handle);
    pthread_mutex_unlock(&cowrie_mem_lock);
    json_free(body);
    if (rc != 0) return err_reply(c, 500, "server_error", cowrie_srv.err_name(rc));

    char *lstr = strndup((const char *)lb.data, lb.len);
    cowrie_srv.buf_free(lb);
    sb out = {0};
    sb_add(&out, "{\"handle\":"); json_quote(&out, hdl);
    sb_add(&out, ",\"cid\":"); json_quote(&out, cid);
    sb_fmt(&out, ",\"tier\":\"%s\",\"authority\":%u,\"tainted\":%s,\"lens\":",
        tname, (unsigned)auth, cowrie_srv.is_tainted(auth) ? "true" : "false");
    json_quote(&out, lstr);
    sb_addc(&out, '}');
    free(lstr);
    json_reply(c, 200, out.p);
    free(out.p);
}

static int cowrie_verify_declassify_jv_locked(
    const char *cid_hex,
    int acode,
    const jv *toks,
    const uint8_t *pk_opt,
    uint64_t now_ts
) {
    if (!pk_opt || acode <= 0 || !toks || toks->type != J_ARR || !cowrie_srv.declassify_verify) return 0;
    uint8_t cid34[34];
    if (!cowrie_parse_hex_n(cid_hex, cid34, 34)) return 0;
    for (size_t i = 0; i < toks->n; i++) {
        const jv *it = toks->items[i];
        if (!it || it->type != J_OBJ) continue;
        const jv *jc = json_get(it, "cid");
        const jv *js = json_get(it, "sig");
        const jv *je = json_get(it, "expiry");
        if (!jc || jc->type != J_STR || strcmp(jc->str, cid_hex) != 0) continue;
        if (!js || js->type != J_STR || !je || je->type != J_NUM || je->num < 0) continue;
        uint8_t sig64[64];
        if (!cowrie_parse_hex_n(js->str, sig64, 64)) continue;
        if (cowrie_srv.declassify_verify(pk_opt, cid34, (uint8_t)acode, (uint64_t)je->num, sig64, now_ts) == 0) {
            return 1;
        }
    }
    return 0;
}

static int cowrie_eval_gate_jv(
    const char *action,
    const jv *args,
    const jv *declassify_toks,
    const char *req_pubkey_hex,
    int unsigned_override,
    char out_culprit[96],
    int *out_declassified
) {
    (void)req_pubkey_hex; // NEVER accept caller-supplied trust anchor over HTTP!
    out_culprit[0] = 0;
    if (out_declassified) *out_declassified = 0;
    int acode = cowrie_srv_action_code(action);
    int priv = (acode != 0);
    if (!priv) return 1;
    const char *allow_ov = getenv("COWRIE_ALLOW_UNSIGNED_OVERRIDE");
    if (unsigned_override && allow_ov && strcmp(allow_ov, "1") == 0) return 1;

    if (!args || args->type != J_ARR || args->n == 0) {
        snprintf(out_culprit, 96, "<empty_lineage>");
        return 0;
    }

    // Strictly pin verification to server-configured COWRIE_OWNER_PUBKEY.
    const char *pk_hex = getenv("COWRIE_OWNER_PUBKEY");
    uint8_t pk[32];
    const uint8_t *pk_ptr = (pk_hex && cowrie_parse_hex_n(pk_hex, pk, 32)) ? pk : NULL;
    time_t now_raw = time(NULL);
    uint64_t now_ts = (now_raw > 0) ? (uint64_t)now_raw : UINT64_MAX;

    int allowed = 1;
    pthread_mutex_lock(&cowrie_mem_lock);
    for (size_t i = 0; i < args->n; i++) {
        if (args->items[i]->type != J_STR) {
            snprintf(out_culprit, 96, "<invalid_arg>");
            allowed = 0;
            break;
        }
        int ambig = 0;
        CowrieMemNode *n = cowrie_find_strict_locked(args->items[i]->str, &ambig);
        if (!n || ambig) {
            snprintf(out_culprit, 96, "%s", args->items[i]->str);
            allowed = 0;
            break;
        }
        if (cowrie_srv.is_tainted(n->authority)) {
            if (cowrie_verify_declassify_jv_locked(n->cid_hex, acode, declassify_toks, pk_ptr, now_ts)) {
                if (out_declassified) (*out_declassified)++;
            } else {
                snprintf(out_culprit, 96, "%s", args->items[i]->str);
                allowed = 0;
                break;
            }
        }
    }
    pthread_mutex_unlock(&cowrie_mem_lock);
    return allowed;
}

// POST /v1/trust/gate
static void handle_cowrie_trust_gate(Conn *c, const char *body_s) {
    if (!cowrie_srv_init()) return err_reply(c, 503, "unavailable_error", "libcowrie_core.so not available");
    const char *err = NULL;
    jv *body = json_parse(body_s, &err);
    if (!body || body->type != J_OBJ) {
        if (body) json_free(body);
        return err_reply(c, 400, "invalid_request_error", "JSON object required");
    }
    const jv *act = json_get(body, "action");
    char action[64];
    snprintf(action, sizeof action, "%s", (act && act->type == J_STR) ? act->str : "shell_exec");
    const jv *ov = json_get(body, "owner_override");
    int unsigned_ov = (ov && ov->type == J_BOOL && ov->b);
    const jv *args = json_get(body, "arg_ids");
    if (!args) args = json_get(body, "arg_cids");
    const jv *dtoks = json_get(body, "declassify_tokens");
    if (!dtoks) dtoks = json_get(body, "_cowrie_declassify");
    const jv *pkv = json_get(body, "owner_pubkey");

    char culprit[96];
    int declassified = 0;
    int allowed = cowrie_eval_gate_jv(
        action, args, dtoks, (pkv && pkv->type == J_STR) ? pkv->str : NULL,
        unsigned_ov, culprit, &declassified
    );
    int priv = (cowrie_srv_action_code(action) != 0);
    json_free(body);

    sb out = {0};
    sb_fmt(&out, "{\"allowed\":%s,\"privileged\":%s,\"tainted\":%s,\"declassified_count\":%d,\"action\":",
        allowed ? "true" : "false", priv ? "true" : "false", (!allowed && priv) ? "true" : "false", declassified);
    json_quote(&out, action);
    if (!allowed) {
        sb_add(&out, ",\"error\":\"ERR_TAINTED_EXECUTION_BLOCKED\",\"tainted_source\":");
        json_quote(&out, culprit);
    }
    sb_addc(&out, '}');
    json_reply(c, allowed ? 200 : 403, out.p);
    free(out.p);
}

static int cowrie_chat_inject_memory(const jv *body, char **prompt_io) {
    const jv *mh = json_get(body, "cowrie_memory_handles");
    if (!mh || mh->type != J_ARR || mh->n == 0 || !cowrie_srv_init()) return 0;
    sb ctx = {0};
    int any_tainted = 0;
    pthread_mutex_lock(&cowrie_mem_lock);
    for (size_t i = 0; i < mh->n; i++) {
        if (mh->items[i]->type != J_STR) { any_tainted = 1; continue; }
        int ambig = 0;
        CowrieMemNode *n = cowrie_find_strict_locked(mh->items[i]->str, &ambig);
        if (!n || ambig) { any_tainted = 1; continue; }
        if (cowrie_srv.is_tainted(n->authority)) any_tainted = 1;
        CowrieSrvBuf lb = {0};
        if (cowrie_srv.project_lens(n->frame_bytes, n->frame_len, 2, 0, &lb) == 0) {
            sb_addn(&ctx, (const char *)lb.data, lb.len);
            sb_addc(&ctx, '\n');
            cowrie_srv.buf_free(lb);
        }
    }
    pthread_mutex_unlock(&cowrie_mem_lock);
    if (ctx.n > 0 && *prompt_io) {
        const char *u = strstr(*prompt_io, "<|im_start|>user\n");
        sb merged = {0};
        if (u) {
            size_t pref = (size_t)((u + 17) - *prompt_io);
            sb_addn(&merged, *prompt_io, pref);
            sb_addn(&merged, ctx.p, ctx.n);
            sb_add(&merged, u + 17);
        } else {
            sb_addn(&merged, ctx.p, ctx.n);
            sb_add(&merged, *prompt_io);
        }
        free(*prompt_io);
        *prompt_io = merged.p;
    }
    free(ctx.p);
    return any_tainted;
}

static int cowrie_chat_gate_allowed(const jv *body) {
    const jv *mh = json_get(body, "cowrie_memory_handles");
    const jv *tov = json_get(body, "cowrie_owner_override");
    int unsigned_ov = (tov && tov->type == J_BOOL && tov->b);
    const jv *dtoks = json_get(body, "cowrie_declassify");
    if (!dtoks) dtoks = json_get(body, "declassify_tokens");
    const jv *pkv = json_get(body, "cowrie_owner_pubkey");
    const jv *actv = json_get(body, "cowrie_action");
    const char *act = (actv && actv->type == J_STR) ? actv->str : "shell_exec";
    char culprit[96];
    return cowrie_eval_gate_jv(act, mh, dtoks, (pkv && pkv->type == J_STR) ? pkv->str : NULL, unsigned_ov, culprit, NULL);
}
#endif // !_WIN32
#endif // COWRIE_ROUTE_H
