#pragma once
// Drop-in C++17 Adapter connecting `llama.cpp` to Cowrie v1.2 (`libcowrie_core` / `cowrie.h`).
// Provides 100% feature symmetry with `vLLM` (`CowrieKVConnectorV1` + `CowrieVllmRuntime`):
//   1. Split-precision hybrid SSM+KV `StatePage` atomic save & zero-copy `mmap` restore (`Q4_0`/`Q8_0`/`FP16` KV
//      + exact recurrent SSM, 64-stride `cowrie_model_fingerprint`, `runtime_id="llama.cpp"`, `doc_cid` pin, `HMAC-SHA256`).
//   2. `LlamaPrefixStateCache`: thread-safe, portable (`<filesystem>`), LRU-bounded longest-prefix StatePage offloader
//      with explicit multi-slot `seq_id` batching and `llama_memory_seq_rm` pre-restore clearing.
//   3. `LlamaCowrieMemory`: thread-safe in-process + `.cwwal` WAL-backed canonical `COWR` memory store with
//      full startup `.cwwal` replay, 47-bit `@handle` collision-safe resolution, `R0`/`R1`/`R2` lenses,
//      sticky `0x80` causal taint, and RFC 8032 `Ed25519` owner-declassification tokens (`COWRIE_DECLASSIFY_V1`).
//   4. `LlamaZonedShard` (`CWZM`) RAII wrapper.

#include "llama.h"
#include "cowrie.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <sys/stat.h>
#endif

namespace cowrie {

inline constexpr const char* LLAMA_CPP_RUNTIME_ID = "llama.cpp";
inline constexpr uint16_t    LLAMA_CPP_LAYOUT_VER = 1;
inline constexpr uint64_t    DEFAULT_MAX_CACHE_BYTES = 8ULL * 1024ULL * 1024ULL * 1024ULL; // 8 GiB

inline llama_context_params make_statepage_context_params(
    uint32_t n_ctx,
    uint8_t kv_quant = COWRIE_KV_FP16,
    uint32_t n_batch = 1024,
    uint32_t n_ubatch = 512
) {
    llama_context_params cp = llama_context_default_params();
    cp.n_ctx = n_ctx;
    cp.n_batch = n_batch;
    cp.n_ubatch = n_ubatch;
    cp.flash_attn_type = LLAMA_FLASH_ATTN_TYPE_ENABLED;
    switch (kv_quant) {
        case COWRIE_KV_Q4_0:
            cp.type_k = GGML_TYPE_Q4_0;
            cp.type_v = GGML_TYPE_Q4_0;
            break;
        case COWRIE_KV_Q8_0:
            cp.type_k = GGML_TYPE_Q8_0;
            cp.type_v = GGML_TYPE_Q8_0;
            break;
        default:
            cp.type_k = GGML_TYPE_F16;
            cp.type_v = GGML_TYPE_F16;
            break;
    }
    return cp;
}

inline std::string fingerprint_gguf_model(const std::string& gguf_path) {
    char hex[65] = {0};
    int rc = cowrie_model_fingerprint(gguf_path.c_str(), hex);
    if (rc != 0) {
        throw std::runtime_error(
            std::string("cowrie: model fingerprint failed (") + cowrie_err_name(rc) + "): " + gguf_path
        );
    }
    return std::string(hex, 64);
}

inline std::string sha256_bytes(const void* data, size_t len) {
    char hex[65] = {0};
    static const uint8_t dummy = 0;
    const uint8_t* ptr = (data && len > 0) ? static_cast<const uint8_t*>(data) : &dummy;
    cowrie_sha256_hex(ptr, len, hex);
    return std::string(hex, 64);
}

inline std::string sha256_string(const std::string& s) {
    return sha256_bytes(s.data(), s.size());
}

inline std::string to_hex_bytes(const uint8_t* b, size_t n) {
    static const char H[] = "0123456789abcdef";
    std::string out(2 * n, '\0');
    for (size_t i = 0; i < n; ++i) {
        out[2 * i]     = H[(b[i] >> 4) & 0xFu];
        out[2 * i + 1] = H[b[i] & 0xFu];
    }
    return out;
}

inline bool from_hex_exact(const std::string& hex, uint8_t* out, size_t expected_bytes) {
    if (hex.size() != 2 * expected_bytes) return false;
    auto nib = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    for (size_t i = 0; i < expected_bytes; ++i) {
        int hi = nib(hex[2 * i]);
        int lo = nib(hex[2 * i + 1]);
        if (hi < 0 || lo < 0) return false;
        out[i] = static_cast<uint8_t>((hi << 4) | lo);
    }
    return true;
}

inline void ensure_private_dir_0700(const std::string& path) {
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
#ifndef _WIN32
    chmod(path.c_str(), 0700);
#endif
}

inline int save_llama_statepage(
    llama_context* ctx,
    llama_seq_id seq_id,
    const std::string& out_path,
    const std::string& model_sha,
    const std::string& doc_cid,
    uint32_t ctx_tokens,
    uint8_t kv_quant = COWRIE_KV_FP16,
    uint8_t ssm_dtype = COWRIE_SSM_FP16,
    size_t* out_state_bytes = nullptr
) {
    size_t sz = llama_state_seq_get_size(ctx, seq_id);
    if (sz == 0) return -1;
    std::vector<uint8_t> buf(sz);
    size_t got = llama_state_seq_get_data(ctx, buf.data(), sz, seq_id);
    if (got == 0) return -2;
    if (out_state_bytes) *out_state_bytes = got;
    return cowrie_statepage_save_ext(
        out_path.c_str(),
        model_sha.c_str(),
        doc_cid.c_str(),
        LLAMA_CPP_RUNTIME_ID,
        LLAMA_CPP_LAYOUT_VER,
        ctx_tokens,
        kv_quant,
        ssm_dtype,
        buf.data(),
        got
    );
}

inline int restore_llama_statepage(
    llama_context* ctx,
    llama_seq_id seq_id,
    const std::string& sp_path,
    const std::string& expected_model_sha,
    bool verify_hash = true,
    size_t* out_restored_bytes = nullptr,
    int expected_kv_quant = -1,
    int expected_ssm_dtype = -1,
    const std::string& expected_doc_cid = ""
) {
    CowrieStatePageHandle* h = nullptr;
    int rc = cowrie_statepage_open_strict(
        sp_path.c_str(),
        expected_model_sha.c_str(),
        expected_doc_cid.empty() ? nullptr : expected_doc_cid.c_str(),
        LLAMA_CPP_RUNTIME_ID,
        expected_kv_quant,
        expected_ssm_dtype,
        verify_hash ? 1 : 0,
        &h
    );
    if (rc != 0) return rc;

    if (llama_memory_t mem = llama_get_memory(ctx)) {
        llama_memory_seq_rm(mem, seq_id, -1, -1);
    }
    const uint8_t* ptr = cowrie_statepage_data(h);
    size_t len = cowrie_statepage_len(h);
    size_t loaded = llama_state_seq_set_data(ctx, ptr, len, seq_id);
    cowrie_statepage_close(h);
    if (loaded == 0) return -3;
    if (out_restored_bytes) *out_restored_bytes = loaded;
    return 0;
}

// =========================================================================
// 1. LlamaPrefixStateCache — Parity with vLLM `CowrieKVConnectorV1`
// =========================================================================

class LlamaPrefixStateCache {
public:
    LlamaPrefixStateCache(
        std::string cache_dir,
        std::string model_sha,
        uint32_t block_size = 16,
        uint8_t kv_quant = COWRIE_KV_FP16,
        uint8_t ssm_dtype = COWRIE_SSM_FP32,
        uint64_t max_cache_bytes = DEFAULT_MAX_CACHE_BYTES
    )
        : dir_(std::move(cache_dir)),
          model_sha_(std::move(model_sha)),
          block_size_(block_size ? block_size : 16),
          kv_quant_(kv_quant),
          ssm_dtype_(ssm_dtype),
          max_cache_bytes_(max_cache_bytes) {
        ensure_private_dir_0700(dir_);
    }

    static uint32_t aligned_prefix_tokens(size_t n_tokens, uint32_t block_size) {
        if (n_tokens <= 1 || block_size == 0) return 0;
        return static_cast<uint32_t>(((n_tokens - 1) / block_size) * block_size);
    }

    std::string prefix_cid(const llama_token* tokens, uint32_t n_tokens) const {
        return sha256_bytes(tokens, static_cast<size_t>(n_tokens) * sizeof(llama_token));
    }

    std::string shard_path(uint32_t n_tokens, const std::string& cid_hex) const {
        return dir_ + "/llama_" + std::to_string(n_tokens) + "_" + cid_hex.substr(0, 32) + ".state.cowrie";
    }

    std::vector<uint32_t> candidate_lengths(uint32_t max_tokens) const {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<uint32_t> out;
        std::error_code ec;
        for (const auto& entry : std::filesystem::directory_iterator(dir_, ec)) {
            if (!entry.is_regular_file(ec)) continue;
            std::string name = entry.path().filename().string();
            if (name.rfind("llama_", 0) != 0 || name.find(".state.cowrie") == std::string::npos) continue;
            size_t u2 = name.find('_', 6);
            if (u2 == std::string::npos || u2 <= 6) continue;
            uint32_t nt = static_cast<uint32_t>(std::strtoul(name.substr(6, u2 - 6).c_str(), nullptr, 10));
            if (nt > 0 && nt <= max_tokens && (nt % block_size_) == 0) {
                if (std::find(out.begin(), out.end(), nt) == out.end()) out.push_back(nt);
            }
        }
        std::sort(out.begin(), out.end(), std::greater<uint32_t>());
        return out;
    }

    /// Find and zero-copy restore the longest verified cached prefix (`< n_tokens`) into `ctx` slot `seq_id`.
    /// Returns number of restored prefix tokens (`0` on miss).
    uint32_t restore_longest_prefix(
        llama_context* ctx,
        llama_seq_id seq_id,
        const llama_token* tokens,
        size_t n_tokens,
        size_t* out_restored_bytes = nullptr
    ) const {
        uint32_t max_valid = aligned_prefix_tokens(n_tokens, block_size_);
        if (max_valid == 0) return 0;
        for (uint32_t nt : candidate_lengths(max_valid)) {
            std::string cid = prefix_cid(tokens, nt);
            std::string sp = shard_path(nt, cid);
            std::error_code ec;
            if (!std::filesystem::is_regular_file(sp, ec)) continue;
            int rc = restore_llama_statepage(
                ctx, seq_id, sp, model_sha_, true, out_restored_bytes,
                static_cast<int>(kv_quant_), static_cast<int>(ssm_dtype_), cid
            );
            if (rc == 0) {
                std::filesystem::last_write_time(sp, std::filesystem::file_time_type::clock::now(), ec);
                return nt;
            }
        }
        return 0;
    }

    /// Evaluate `tokens[0..n_tokens)` on explicit `seq_id` using longest-prefix StatePage restore +
    /// suffix `llama_decode` + automatic saving and LRU eviction of newly computed block-aligned prefixes.
    int eval_with_prefix_cache(
        llama_context* ctx,
        llama_seq_id seq_id,
        const llama_token* tokens,
        size_t n_tokens,
        uint32_t n_batch = 512,
        uint32_t* out_matched_tokens = nullptr,
        size_t* out_state_bytes = nullptr
    ) const {
        if (!ctx || !tokens || n_tokens == 0) return -1;
        uint32_t matched = restore_longest_prefix(ctx, seq_id, tokens, n_tokens, out_state_bytes);
        if (out_matched_tokens) *out_matched_tokens = matched;

        uint32_t target_save = aligned_prefix_tokens(n_tokens, block_size_);
        bool want_save = (target_save > matched);
        if (want_save) {
            std::string scid = prefix_cid(tokens, target_save);
            std::error_code ec;
            if (std::filesystem::is_regular_file(shard_path(target_save, scid), ec)) want_save = false;
        }

        // Explicit `llama_batch_init` preserves `seq_id` and exact positions on multi-slot servers!
        auto decode_span = [&](size_t start_pos, size_t end_pos) -> int {
            size_t pos = start_pos;
            while (pos < end_pos) {
                uint32_t chunk = static_cast<uint32_t>(std::min<size_t>(n_batch, end_pos - pos));
                llama_batch batch = llama_batch_init(static_cast<int32_t>(chunk), 0, 1);
                batch.n_tokens = static_cast<int32_t>(chunk);
                for (uint32_t i = 0; i < chunk; ++i) {
                    batch.token[i]     = tokens[pos + i];
                    batch.pos[i]       = static_cast<llama_pos>(pos + i);
                    batch.n_seq_id[i]  = 1;
                    batch.seq_id[i][0] = seq_id;
                    batch.logits[i]    = (pos + i + 1 == n_tokens) ? 1 : 0;
                }
                int drc = llama_decode(ctx, batch);
                llama_batch_free(batch);
                if (drc != 0) return -4;
                pos += chunk;
            }
            return 0;
        };

        if (want_save) {
            int rc = decode_span(matched, target_save);
            if (rc != 0) return rc;
            std::string scid = prefix_cid(tokens, target_save);
            size_t saved_b = 0;
            rc = save_llama_statepage(
                ctx, seq_id, shard_path(target_save, scid), model_sha_, scid,
                target_save, kv_quant_, ssm_dtype_, &saved_b
            );
            if (rc != 0) return rc;
            evict_lru_if_needed();
            if (out_state_bytes && *out_state_bytes == 0) *out_state_bytes = saved_b;
            return decode_span(target_save, n_tokens);
        }
        return decode_span(matched, n_tokens);
    }

private:
    void evict_lru_if_needed() const {
        if (max_cache_bytes_ == 0) return;
        std::lock_guard<std::mutex> lk(mu_);
        struct Item {
            std::filesystem::file_time_type mtime;
            uint64_t size;
            std::filesystem::path path;
        };
        std::vector<Item> items;
        uint64_t total = 0;
        std::error_code ec;
        for (const auto& entry : std::filesystem::directory_iterator(dir_, ec)) {
            if (!entry.is_regular_file(ec)) continue;
            std::string name = entry.path().filename().string();
            if (name.rfind("llama_", 0) != 0 || name.find(".state.cowrie") == std::string::npos) continue;
            uint64_t sz = entry.file_size(ec);
            auto mt = entry.last_write_time(ec);
            total += sz;
            items.push_back(Item{mt, sz, entry.path()});
        }
        if (total <= max_cache_bytes_) return;
        std::sort(items.begin(), items.end(), [](const Item& a, const Item& b) {
            return a.mtime < b.mtime;
        });
        for (const auto& it : items) {
            if (total <= max_cache_bytes_) break;
            if (std::filesystem::remove(it.path, ec)) {
                total = (total >= it.size) ? (total - it.size) : 0;
            }
        }
    }

    std::string        dir_;
    std::string        model_sha_;
    uint32_t           block_size_;
    uint8_t            kv_quant_;
    uint8_t            ssm_dtype_;
    uint64_t           max_cache_bytes_;
    mutable std::mutex mu_;
};

// =========================================================================
// 2. LlamaCowrieMemory — Parity with `CowrieMemoryStore` + `Ed25519` + `.cwwal`
// =========================================================================

struct DeclassifyToken {
    std::string cid_hex;
    uint64_t    expiry_unix = 0;
    std::string sig_hex;
};

struct LlamaMemoryRecord {
    std::string          cid_hex;
    std::string          handle;
    uint8_t              authority = 0;
    uint8_t              epistemic = 0;
    std::vector<uint8_t> frame_bytes;
    std::string          r0;
    bool                 tainted = false;
};

struct GateDecision {
    bool        allowed = false;
    bool        privileged = true;
    bool        tainted = false;
    size_t      declassified_count = 0;
    std::string error;
    std::string tainted_source;
};

class LlamaCowrieMemory {
public:
    explicit LlamaCowrieMemory(
        const std::string& persist_dir = "",
        const std::string& owner_pubkey_hex = ""
    ) {
        if (!owner_pubkey_hex.empty()) {
            has_owner_pk_ = from_hex_exact(owner_pubkey_hex, owner_pk_, 32);
        } else if (const char* env_pk = std::getenv("COWRIE_OWNER_PUBKEY")) {
            has_owner_pk_ = from_hex_exact(env_pk, owner_pk_, 32);
        }
        if (!persist_dir.empty()) {
            ensure_private_dir_0700(persist_dir);
            std::string wal_p = persist_dir + "/memory.cwwal";
            if (cowrie_wal_open(wal_p.c_str(), 1, &wal_) == 0 && wal_) {
                replay_wal_locked();
            } else {
                wal_ = nullptr;
            }
        }
    }

    ~LlamaCowrieMemory() {
        std::lock_guard<std::mutex> lk(mu_);
        if (wal_) {
            cowrie_wal_close(wal_);
            wal_ = nullptr;
        }
    }

    LlamaCowrieMemory(const LlamaCowrieMemory&) = delete;
    LlamaCowrieMemory& operator=(const LlamaCowrieMemory&) = delete;

    static int action_code(const std::string& action) {
        if (action == "read_only" || action == "read_only_safe" || action == "search") return COWRIE_ACT_READ_ONLY;
        if (action == "shell_exec") return COWRIE_ACT_SHELL_EXEC;
        if (action == "cred_read")  return COWRIE_ACT_CRED_READ;
        if (action == "fs_write")   return COWRIE_ACT_FS_WRITE;
        if (action == "net_exfil")  return COWRIE_ACT_NET_EXFIL;
        return -1;
    }

    const LlamaMemoryRecord* resolve(const std::string& key) const {
        std::lock_guard<std::mutex> lk(mu_);
        return resolve_locked(key);
    }

    LlamaMemoryRecord store_json(
        const std::string& json_utf8,
        uint8_t base_authority = COWRIE_AUTH_VERIFIED_TOOL,
        const std::vector<std::string>& parents = {},
        uint8_t epistemic = COWRIE_EPI_OBSERVED
    ) {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<uint8_t> pauths;
        pauths.reserve(parents.size());
        for (const auto& pid : parents) {
            const LlamaMemoryRecord* pr = resolve_locked(pid);
            pauths.push_back(pr ? pr->authority : static_cast<uint8_t>(COWRIE_AUTH_UNTRUSTED_EXT | COWRIE_BIT_TAINTED));
        }
        uint8_t eff_auth = cowrie_propagate_authority(
            base_authority, pauths.empty() ? nullptr : pauths.data(), pauths.size()
        );

        CowrieBuf wbuf{nullptr, 0, 0};
        int rc = cowrie_encode_json(
            reinterpret_cast<const uint8_t*>(json_utf8.data()), json_utf8.size(),
            eff_auth, epistemic, &wbuf
        );
        if (rc != 0) throw std::runtime_error(std::string("cowrie_encode_json: ") + cowrie_err_name(rc));
        std::vector<uint8_t> wire(wbuf.data, wbuf.data + wbuf.len);
        cowrie_buf_free(wbuf);

        LlamaMemoryRecord rec = index_wire_frame_locked(std::move(wire));
        if (wal_) {
            uint64_t seq = 0;
            uint8_t out_c[34];
            cowrie_wal_append_frame(wal_, rec.frame_bytes.data(), rec.frame_bytes.size(), 1, &seq, out_c);
        }
        return rec;
    }

    std::string project_lens(const std::string& key, uint8_t tier = COWRIE_LENS_R2) const {
        std::vector<uint8_t> frame_copy;
        {
            std::lock_guard<std::mutex> lk(mu_);
            const LlamaMemoryRecord* rec = resolve_locked(key);
            if (!rec) throw std::runtime_error("Unknown or ambiguous Cowrie handle/CID: " + key);
            frame_copy = rec->frame_bytes;
        }
        CowrieBuf lb{nullptr, 0, 0};
        int rc = cowrie_project_lens(frame_copy.data(), frame_copy.size(), tier, 0, &lb);
        if (rc != 0) throw std::runtime_error(std::string("cowrie_project_lens: ") + cowrie_err_name(rc));
        std::string s(reinterpret_cast<const char*>(lb.data), lb.len);
        cowrie_buf_free(lb);
        return s;
    }

    GateDecision check_tool_gate(
        const std::string& action,
        const std::vector<std::string>& arg_ids,
        const std::vector<DeclassifyToken>& tokens = {},
        uint64_t now_unix = 0
    ) const {
        std::lock_guard<std::mutex> lk(mu_);
        GateDecision dec;
        int acode = action_code(action);
        dec.privileged = (acode != COWRIE_ACT_READ_ONLY);
        std::time_t now_raw = std::time(nullptr);
        uint64_t now_ts = now_unix ? now_unix : (now_raw > 0 ? static_cast<uint64_t>(now_raw) : UINT64_MAX);

        if (dec.privileged && arg_ids.empty()) {
            dec.tainted = true;
            dec.tainted_source = "<empty_lineage>";
            dec.error = "ERR_TAINTED_EXECUTION_BLOCKED";
            return dec;
        }
        for (const auto& aid : arg_ids) {
            const LlamaMemoryRecord* rec = resolve_locked(aid);
            if (!rec) {
                dec.tainted = true;
                dec.tainted_source = aid;
                dec.error = "ERR_TAINTED_EXECUTION_BLOCKED";
                return dec;
            }
            if (rec->tainted) {
                bool ok_sig = false;
                uint8_t cid34[34];
                if (dec.privileged && has_owner_pk_ && acode > 0 && from_hex_exact(rec->cid_hex, cid34, 34)) {
                    for (const auto& t : tokens) {
                        uint8_t sig64[64];
                        if (t.cid_hex == rec->cid_hex && from_hex_exact(t.sig_hex, sig64, 64)) {
                            if (cowrie_declassify_verify(
                                    owner_pk_, cid34, static_cast<uint8_t>(acode),
                                    t.expiry_unix, sig64, now_ts) == 0) {
                                ok_sig = true;
                                break;
                            }
                        }
                    }
                }
                if (ok_sig) {
                    dec.declassified_count++;
                } else {
                    dec.tainted = true;
                    dec.tainted_source = aid;
                    if (dec.privileged) {
                        dec.error = "ERR_TAINTED_EXECUTION_BLOCKED";
                        return dec;
                    }
                }
            }
        }
        dec.allowed = true;
        return dec;
    }

    size_t wal_entries() const {
        std::lock_guard<std::mutex> lk(mu_);
        return wal_ ? cowrie_wal_len(wal_) : 0;
    }

private:
    const LlamaMemoryRecord* resolve_locked(const std::string& key) const {
        auto it = by_cid_.find(key);
        if (it != by_cid_.end()) return &it->second;
        auto hit = by_handle_.find(key);
        if (hit != by_handle_.end()) {
            return hit->second.size() == 1 ? &by_cid_.at(hit->second[0]) : nullptr;
        }
        if (key.size() >= 32 && key[0] != '@') {
            const LlamaMemoryRecord* found = nullptr;
            for (const auto& kv : by_cid_) {
                if (kv.first.compare(0, key.size(), key) == 0) {
                    if (found) return nullptr;
                    found = &kv.second;
                }
            }
            return found;
        }
        return nullptr;
    }

    LlamaMemoryRecord index_wire_frame_locked(std::vector<uint8_t> wire) {
        uint8_t va = 0, ve = 0, cid34[34] = {0};
        int rc = cowrie_frame_verify(wire.data(), wire.size(), &va, &ve, cid34);
        if (rc != 0) throw std::runtime_error(std::string("cowrie_frame_verify: ") + cowrie_err_name(rc));

        CowrieBuf r0b{nullptr, 0, 0};
        rc = cowrie_project_lens(wire.data(), wire.size(), COWRIE_LENS_R0, 0, &r0b);
        if (rc != 0) throw std::runtime_error("cowrie_project_lens R0 failed");
        std::string r0_str(reinterpret_cast<const char*>(r0b.data), r0b.len);
        cowrie_buf_free(r0b);

        std::string cid_hex = to_hex_bytes(cid34, 34);
        size_t sp = r0_str.find(' ');
        std::string handle = (sp == std::string::npos) ? r0_str : r0_str.substr(0, sp);

        auto ex = by_cid_.find(cid_hex);
        if (ex != by_cid_.end() && ex->second.tainted && !cowrie_is_tainted(va)) {
            va |= COWRIE_BIT_TAINTED;
        }

        LlamaMemoryRecord rec;
        rec.cid_hex = cid_hex;
        rec.handle = handle;
        rec.authority = va;
        rec.epistemic = ve;
        rec.frame_bytes = std::move(wire);
        rec.r0 = std::move(r0_str);
        rec.tainted = cowrie_is_tainted(va) != 0;

        by_cid_[cid_hex] = rec;
        auto& bucket = by_handle_[handle];
        if (std::find(bucket.begin(), bucket.end(), cid_hex) == bucket.end()) {
            bucket.push_back(cid_hex);
        }
        return rec;
    }

    void replay_wal_locked() {
        if (!wal_) return;
        size_t n = cowrie_wal_live_len(wal_);
        for (size_t i = 0; i < n; ++i) {
            CowrieBuf fb{nullptr, 0, 0};
            if (cowrie_wal_live_frame_at(wal_, i, &fb) == 0 && fb.data && fb.len > 0) {
                std::vector<uint8_t> wire(fb.data, fb.data + fb.len);
                cowrie_buf_free(fb);
                try {
                    index_wire_frame_locked(std::move(wire));
                } catch (...) {
                }
            }
        }
    }

    mutable std::mutex                                         mu_;
    std::unordered_map<std::string, LlamaMemoryRecord>         by_cid_;
    std::unordered_map<std::string, std::vector<std::string>>  by_handle_;
    CowrieWalHandle*                                           wal_ = nullptr;
    uint8_t                                                    owner_pk_[32] = {0};
    bool                                                       has_owner_pk_ = false;
};

// =========================================================================
// 3. LlamaZonedShard (`CWZM`) RAII Wrapper
// =========================================================================

class LlamaZonedShard {
public:
    explicit LlamaZonedShard(const std::string& path) {
        int rc = cowrie_zoned_open(path.c_str(), &h_);
        if (rc != 0) throw std::runtime_error(std::string("cowrie_zoned_open: ") + cowrie_err_name(rc));
    }
    ~LlamaZonedShard() { if (h_) cowrie_zoned_close(h_); }
    LlamaZonedShard(const LlamaZonedShard&) = delete;
    LlamaZonedShard& operator=(const LlamaZonedShard&) = delete;

    std::pair<size_t, size_t> query_keys(int64_t lo, int64_t hi, bool trusted_only = true, bool verify_merkle = true) const {
        size_t cnt = 0, bread = 0;
        int rc = cowrie_zoned_query_keys(h_, lo, hi, trusted_only ? 1 : 0, verify_merkle ? 1 : 0, &cnt, &bread);
        if (rc != 0) throw std::runtime_error(std::string("cowrie_zoned_query_keys: ") + cowrie_err_name(rc));
        return {cnt, bread};
    }

private:
    CowrieZonedHandle* h_ = nullptr;
};

} // namespace cowrie
