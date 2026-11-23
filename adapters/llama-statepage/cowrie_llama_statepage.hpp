#pragma once
// Drop-in C++ adapter connecting llama.cpp hybrid SSM+KV state to Cowrie
// (`libcowrie_core.so` / `cowrie.h`). Supports split-precision KV quantization
// (FP16 / Q8_0 / Q4_0 KV + exact recurrent SSM), zero-copy 16 KB-aligned mmap restore,
// parallel 4 MiB SHA-NI Merkle verification, and strict model_sha + kv_quant contracts.

#include "llama.h"
#include "cowrie.h"

#include <sys/stat.h>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace cowrie {

/// Construct llama_context_params configured for split-precision hybrid StatePages.
/// GQA Attention KV cache uses `kv_quant` (FP16 / Q8_0 / Q4_0) while recurrent SSM layers
/// (e.g., Qwen3.5 Gated DeltaNet) remain exact in llama.cpp.
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

/// Fast deterministic 64-hex model identity fingerprint (first 4 MB + exact file byte size).
inline std::string fingerprint_gguf_model(const std::string& gguf_path) {
    struct stat st{};
    if (stat(gguf_path.c_str(), &st) != 0) {
        throw std::runtime_error("cowrie: cannot stat model file: " + gguf_path);
    }
    const size_t head = 4 * 1024 * 1024;
    std::vector<uint8_t> buf(head + 8, 0);
    FILE* f = fopen(gguf_path.c_str(), "rb");
    if (!f) throw std::runtime_error("cowrie: cannot open model file: " + gguf_path);
    size_t nr = fread(buf.data(), 1, head, f);
    fclose(f);
    uint64_t fsz = (uint64_t)st.st_size;
    memcpy(buf.data() + nr, &fsz, 8);
    char hex[65] = {0};
    cowrie_sha256_hex(buf.data(), nr + 8, hex);
    return std::string(hex, 64);
}

inline std::string sha256_string(const std::string& s) {
    char hex[65] = {0};
    cowrie_sha256_hex(reinterpret_cast<const uint8_t*>(s.data()), s.size(), hex);
    return std::string(hex, 64);
}

/// Extract live hybrid SSM+KV state for `seq_id` and atomically save as a 16 KB-aligned StatePage.
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
    return cowrie_statepage_save(
        out_path.c_str(),
        model_sha.c_str(),
        doc_cid.c_str(),
        ctx_tokens,
        kv_quant,
        ssm_dtype,
        buf.data(),
        got
    );
}

/// Open a 16 KB-aligned StatePage via Rust `libcowrie_core.so`, strictly verify alignment,
/// `model_sha`, optional `expected_kv_quant` (-1 = any), and parallel SHA-NI Merkle hash,
/// then zero-copy restore directly into `llama_state_seq_set_data()`.
inline int restore_llama_statepage(
    llama_context* ctx,
    llama_seq_id seq_id,
    const std::string& sp_path,
    const std::string& expected_model_sha,
    bool verify_hash = true,
    size_t* out_restored_bytes = nullptr,
    int expected_kv_quant = -1
) {
    CowrieStatePageHandle* h = nullptr;
    int rc = cowrie_statepage_open_checked(
        sp_path.c_str(),
        expected_model_sha.c_str(),
        expected_kv_quant,
        verify_hash ? 1 : 0,
        &h
    );
    if (rc != 0) return rc;

    const uint8_t* ptr = cowrie_statepage_data(h);
    size_t len = cowrie_statepage_len(h);
    size_t loaded = llama_state_seq_set_data(ctx, ptr, len, seq_id);
    cowrie_statepage_close(h);
    if (loaded == 0) return -3;
    if (out_restored_bytes) *out_restored_bytes = loaded;
    return 0;
}

} // namespace cowrie
