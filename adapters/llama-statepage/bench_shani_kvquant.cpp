// Live v1.0.1 Proof on Qwen3.5-9B-Q4_K_M (13,800-Token Workspace + 5 Buried Needles):
// Compares Split-Precision Hybrid SSM+KV StatePages (FP16 vs Q8_0 vs Q4_0 KV + Exact DeltaNet SSM)
// with Hardware x86_64 SHA-NI Parallel 4 MiB Merkle Verification in Rust `libcowrie_core.so`.

#include "cowrie_llama_statepage.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <string>
#include <vector>

static std::vector<llama_token> tok_str(const llama_vocab* vocab, const std::string& s, bool bos) {
    std::vector<llama_token> out(s.size() + 64);
    int32_t n = llama_tokenize(vocab, s.c_str(), (int32_t)s.size(), out.data(), (int32_t)out.size(), bos, true);
    if (n < 0) {
        out.resize(-n + 16);
        n = llama_tokenize(vocab, s.c_str(), (int32_t)s.size(), out.data(), (int32_t)out.size(), bos, true);
    }
    out.resize(n);
    return out;
}

static int decode_all(llama_context* ctx, const std::vector<llama_token>& toks) {
    for (size_t off = 0; off < toks.size(); off += 1024) {
        int ch = (int)std::min<size_t>(1024, toks.size() - off);
        if (llama_decode(ctx, llama_batch_get_one((llama_token*)toks.data() + off, ch)) != 0) return -1;
    }
    return 0;
}

static llama_token argmax(const float* lg, int n) {
    int best = 0; float bv = lg[0];
    for (int i = 1; i < n; ++i) if (lg[i] > bv) { bv = lg[i]; best = i; }
    return (llama_token)best;
}

static double kl_div(const std::vector<float>& ref, const std::vector<float>& cand, double& max_d) {
    max_d = 0.0;
    double mr = -1e30, mc = -1e30;
    for (size_t i = 0; i < ref.size(); ++i) {
        double d = std::fabs((double)ref[i] - (double)cand[i]);
        if (d > max_d) max_d = d;
        if (ref[i] > mr) mr = ref[i];
        if (cand[i] > mc) mc = cand[i];
    }
    double sr = 0.0, sc = 0.0;
    for (size_t i = 0; i < ref.size(); ++i) { sr += std::exp((double)ref[i] - mr); sc += std::exp((double)cand[i] - mc); }
    double lsr = std::log(sr) + mr, lsc = std::log(sc) + mc, kl = 0.0;
    for (size_t i = 0; i < ref.size(); ++i) {
        double lpr = (double)ref[i] - lsr, lpc = (double)cand[i] - lsc, pr = std::exp(lpr);
        if (pr > 1e-12) kl += pr * (lpr - lpc);
    }
    return kl;
}

struct QOut {
    std::string text;
    std::vector<llama_token> toks;
    std::vector<float> logits;
};

static QOut ask_greedy(llama_context* ctx, const llama_vocab* vocab, const std::string& q) {
    QOut out{};
    auto qt = tok_str(vocab, q, false);
    if (decode_all(ctx, qt) != 0) exit(2);
    int nv = llama_vocab_n_tokens(vocab);
    const float* lg = llama_get_logits_ith(ctx, -1);
    out.logits.assign(lg, lg + nv);
    llama_token cur = argmax(lg, nv);
    for (int step = 0; step < 32; ++step) {
        if (llama_vocab_is_eog(vocab, cur)) break;
        out.toks.push_back(cur);
        char buf[256];
        int n = llama_token_to_piece(vocab, cur, buf, sizeof(buf), 0, false);
        if (n > 0) out.text.append(buf, n);
        if (llama_decode(ctx, llama_batch_get_one(&cur, 1)) != 0) break;
        cur = argmax(llama_get_logits_ith(ctx, -1), nv);
    }
    return out;
}

static void evict_file(const std::string& p) {
    int fd = open(p.c_str(), O_RDONLY);
    if (fd >= 0) {
        posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
        close(fd);
    }
}

int main() {
    using clk = std::chrono::steady_clock;
    const std::string model_9b  = "/home/omen/.local/share/models/Qwen3.5-9B-Q4_K_M.gguf";
    const std::string model_08b = "/home/omen/.local/share/models/Qwen3.5-0.8B-Q4_K_M.gguf";

    llama_log_set([](ggml_log_level, const char*, void*){}, nullptr);
    llama_backend_init();

    const bool has_shani = (cowrie_has_shani() != 0);
    std::string sha_9b  = cowrie::fingerprint_gguf_model(model_9b);
    std::string sha_08b = cowrie::fingerprint_gguf_model(model_08b);

    llama_model_params mp = llama_model_default_params();
    mp.n_gpu_layers = 99;
    llama_model* model = llama_model_load_from_file(model_9b.c_str(), mp);
    if (!model) { fprintf(stderr, "Failed to load Qwen3.5-9B\n"); return 1; }
    const llama_vocab* vocab = llama_model_get_vocab(model);

    // Exact same 13,800-token workspace + 5 buried needles from U2
    std::string ws = "<|im_start|>system\nYou are a precise systems analyst. Answer questions about the workspace below directly and concisely without extra commentary.<|im_end|>\n<|im_start|>user\n=== LOCAL WORKSPACE ARCHIVE ===\n";
    std::string para =
        "Module subsystem telemetry: aligned page buffers prevent page-fault storms across unified memory tiers "
        "while canonical byte-sorted dictionaries maintain deterministic cross-language content hashes across Rust, "
        "Go, TypeScript, and Python verifiers. Background indexers build model-versioned satellites on idle power. ";
    int reps = 225;
    for (int i = 0; i < reps; ++i) {
        ws += "[Log Block " + std::to_string(i) + "] " + para + "\n";
        if (i == reps / 6)     ws += "NEEDLE_ALPHA: The canonical release commit SHA for Bend-Engine v4 is 9f4c2a8e11b7.\n";
        if (i == reps * 2 / 6) ws += "NEEDLE_BETA: The tailnet metrics port for the local verifier daemon is 19472.\n";
        if (i == reps * 3 / 6) ws += "NEEDLE_GAMMA: The measured RTX 3090 INT8 TensorCore peak recorded in Gate C0 is 132.7 TOPS.\n";
        if (i == reps * 4 / 6) ws += "NEEDLE_DELTA: The required Apple Silicon Metal page alignment constant is 16384 bytes.\n";
        if (i == reps * 5 / 6) ws += "NEEDLE_EPSILON: The wire bitmask bit assigned to transitive causal taint is 0x80.\n";
    }
    ws += "=== END WORKSPACE ===\n";
    std::string doc_cid = cowrie::sha256_string(ws);
    auto ws_toks = tok_str(vocab, ws, true);

    const char* questions[5] = {
        "Question: What is the canonical release commit SHA in NEEDLE_ALPHA? Reply with ONLY the SHA.<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n",
        "Question: What port number is in NEEDLE_BETA? Reply with ONLY the port number.<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n",
        "Question: What TOPS value is in NEEDLE_GAMMA? Reply with ONLY the number and unit.<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n",
        "Question: What byte alignment constant is in NEEDLE_DELTA? Reply with ONLY the number.<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n",
        "Question: What hex bitmask is in NEEDLE_EPSILON? Reply with ONLY the hex value.<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n",
    };
    const char* expected_sub[5] = {"9f4c2a8e11b7", "19472", "132.7", "16384", "0x80"};

    struct ModeCfg { const char* name; uint8_t kvq; };
    ModeCfg modes[3] = {
        {"FP16_KV_exact_SSM", COWRIE_KV_FP16},
        {"Q8_0_KV_exact_SSM", COWRIE_KV_Q8_0},
        {"Q4_0_KV_exact_SSM", COWRIE_KV_Q4_0},
    };

    struct ModeRes {
        const char* name;
        uint8_t kvq;
        size_t state_bytes;
        double cold_prefill_ms;
        double save_ms;
        double cold_verified_restore_ms;
        double warm_verified_restore_ms;
        double warm_trusted_restore_ms;
        double mean_kl_vs_fp16;
        double max_logit_diff;
        int exact_toks;
        int total_toks;
        int needles_ok;
    };
    std::vector<ModeRes> results;
    std::vector<QOut> fp16_ref(5);
    bool ctrl_model_ok = false, ctrl_kvq_ok = false, ctrl_bitflip_ok = false;

    printf("COWRIE v1.0 LIVE BENCH | Qwen3.5-9B | %zu tokens | SHA-NI=%s\n",
           ws_toks.size(), has_shani ? "ENABLED" : "SCALAR");

    for (int m = 0; m < 3; ++m) {
        auto cp = cowrie::make_statepage_context_params(16384, modes[m].kvq);
        llama_context* ctx = llama_init_from_model(model, cp);
        if (!ctx) { fprintf(stderr, "Failed to init ctx for %s\n", modes[m].name); return 2; }

        std::string sp_path = std::string("/home/omen/cowrie/receipts/_nvme_sp_") + modes[m].name + ".state.cowrie";

        // 1. Cold full prefill
        llama_memory_clear(llama_get_memory(ctx), true);
        auto tp0 = clk::now();
        if (decode_all(ctx, ws_toks) != 0) return 3;
        double cold_prefill_ms = std::chrono::duration<double, std::milli>(clk::now() - tp0).count();

        // 2. Atomic save through Rust `libcowrie_core.so` (parallel 4 MiB SHA-NI Merkle!)
        size_t st_bytes = 0;
        auto ts0 = clk::now();
        int rc = cowrie::save_llama_statepage(
            ctx, 0, sp_path, sha_9b, doc_cid, (uint32_t)ws_toks.size(),
            modes[m].kvq, COWRIE_SSM_FP32, &st_bytes
        );
        double save_ms = std::chrono::duration<double, std::milli>(clk::now() - ts0).count();
        if (rc != 0) { fprintf(stderr, "save failed: %s\n", cowrie_err_name(rc)); return 4; }

        // If FP16 baseline, capture exact fresh-prefill reference answers on Q0..Q4 (using restored FP16 which U2 proved KL=0)
        // Even cleaner: Q0 runs directly on untouched cold prefill, Q1..Q4 run on verified FP16 StatePage restore!
        if (m == 0) {
            fp16_ref[0] = ask_greedy(ctx, vocab, questions[0]);
            for (int q = 1; q < 5; ++q) {
                llama_memory_clear(llama_get_memory(ctx), true);
                if (cowrie::restore_llama_statepage(ctx, 0, sp_path, sha_9b, false, nullptr, COWRIE_KV_FP16) != 0) return 5;
                fp16_ref[q] = ask_greedy(ctx, vocab, questions[q]);
            }
        }

        // 3. Cold NVMe restore WITH FULL PARALLEL SHA-NI MERKLE VERIFICATION (`verify_hash = true`)!
        evict_file(sp_path);
        llama_memory_clear(llama_get_memory(ctx), true);
        auto tc0 = clk::now();
        rc = cowrie::restore_llama_statepage(ctx, 0, sp_path, sha_9b, true, nullptr, modes[m].kvq);
        double cold_ver_ms = std::chrono::duration<double, std::milli>(clk::now() - tc0).count();
        if (rc != 0) { fprintf(stderr, "cold verified restore failed: %s\n", cowrie_err_name(rc)); return 6; }

        // 4. Warm mmap restore WITH verify_hash=true vs verify_hash=false (median of 3)
        std::vector<double> w_ver, w_tru;
        for (int it = 0; it < 3; ++it) {
            llama_memory_clear(llama_get_memory(ctx), true);
            auto tw0 = clk::now();
            rc = cowrie::restore_llama_statepage(ctx, 0, sp_path, sha_9b, true, nullptr, modes[m].kvq);
            w_ver.push_back(std::chrono::duration<double, std::milli>(clk::now() - tw0).count());
            if (rc != 0) return 7;

            llama_memory_clear(llama_get_memory(ctx), true);
            auto tw1 = clk::now();
            rc = cowrie::restore_llama_statepage(ctx, 0, sp_path, sha_9b, false, nullptr, modes[m].kvq);
            w_tru.push_back(std::chrono::duration<double, std::milli>(clk::now() - tw1).count());
            if (rc != 0) return 8;
        }
        std::sort(w_ver.begin(), w_ver.end());
        std::sort(w_tru.begin(), w_tru.end());

        // 5. Evaluate all 5 buried needle questions against FP16 reference!
        int exact_toks = 0, total_toks = 0, needles_ok = 0;
        double sum_kl = 0.0, max_ld = 0.0;
        for (int q = 0; q < 5; ++q) {
            llama_memory_clear(llama_get_memory(ctx), true);
            if (cowrie::restore_llama_statepage(ctx, 0, sp_path, sha_9b, false, nullptr, modes[m].kvq) != 0) return 9;
            QOut got = ask_greedy(ctx, vocab, questions[q]);
            double ld = 0.0;
            double kl = kl_div(fp16_ref[q].logits, got.logits, ld);
            sum_kl += kl;
            if (ld > max_ld) max_ld = ld;
            size_t nt = std::max(fp16_ref[q].toks.size(), got.toks.size());
            total_toks += (int)nt;
            for (size_t k = 0; k < std::min(fp16_ref[q].toks.size(), got.toks.size()); ++k) {
                if (fp16_ref[q].toks[k] == got.toks[k]) exact_toks++;
            }
            if (got.text.find(expected_sub[q]) != std::string::npos) needles_ok++;
        }

        // Run all 3 negative controls on Q4_0 shard before deleting!
        if (modes[m].kvq == COWRIE_KV_Q4_0) {
            int r_mod = cowrie::restore_llama_statepage(ctx, 0, sp_path, sha_08b, true, nullptr, COWRIE_KV_Q4_0);
            int r_kvq = cowrie::restore_llama_statepage(ctx, 0, sp_path, sha_9b,  true, nullptr, COWRIE_KV_FP16);
            int fd = open(sp_path.c_str(), O_RDWR);
            uint8_t b = 0;
            pread(fd, &b, 1, 16384 + (off_t)(st_bytes / 2));
            b ^= 0x01;
            pwrite(fd, &b, 1, 16384 + (off_t)(st_bytes / 2));
            fsync(fd);
            close(fd);
            int r_bit = cowrie::restore_llama_statepage(ctx, 0, sp_path, sha_9b, true, nullptr, COWRIE_KV_Q4_0);
            ctrl_model_ok   = (std::string(cowrie_err_name(r_mod)) == "ERR_MODEL_MISMATCH");
            ctrl_kvq_ok     = (std::string(cowrie_err_name(r_kvq)) == "ERR_KV_QUANT_MISMATCH");
            ctrl_bitflip_ok = (std::string(cowrie_err_name(r_bit)) == "ERR_STATEPAGE_HASH_MISMATCH");
        }

        unlink(sp_path.c_str());
        llama_free(ctx);

        results.push_back({
            modes[m].name, modes[m].kvq, st_bytes, cold_prefill_ms, save_ms,
            cold_ver_ms, w_ver[1], w_tru[1], sum_kl / 5.0, max_ld,
            exact_toks, total_toks, needles_ok
        });
        printf("  %-18s | State=%6.2f MB | Prefill=%6.1f ms | ColdNVMe(SHA-NI)=%5.1f ms (%4.1fx) | Warm(SHA-NI)=%4.1f ms | Warm(mmap)=%4.1f ms (%5.1fx) | KL=%.2e | Toks=%d/%d | Needles=%d/5\n",
               modes[m].name, st_bytes / 1e6, cold_prefill_ms, cold_ver_ms, cold_prefill_ms / cold_ver_ms,
               w_ver[1], w_tru[1], cold_prefill_ms / w_tru[1], sum_kl / 5.0, exact_toks, total_toks, needles_ok);
    }

    llama_model_free(model);
    llama_backend_free();

    double fp16_sz = (double)results[0].state_bytes;
    FILE* fout = fopen("/home/omen/cowrie/receipts/receipt_shani_kvquant.json", "w");
    fprintf(fout, "{\n");
    fprintf(fout, "  \"suite\": \"cowrie_shani_parallel_merkle_and_split_precision_kv\",\n");
    fprintf(fout, "  \"model\": \"%s\",\n", model_9b.c_str());
    fprintf(fout, "  \"ctx_tokens\": %zu,\n", ws_toks.size());
    fprintf(fout, "  \"x86_shani_enabled\": %s,\n", has_shani ? "true" : "false");
    fprintf(fout, "  \"hash_mode\": \"parallel_4mib_rfc6962_merkle_shani\",\n");
    fprintf(fout, "  \"modes\": [\n");
    for (size_t i = 0; i < results.size(); ++i) {
        const auto& r = results[i];
        fprintf(fout,
                "    {\"mode\": \"%s\", \"state_bytes\": %zu, \"size_reduction_vs_fp16_x\": %.2f, \"cold_prefill_ms\": %.2f, \"atomic_save_ms\": %.2f, \"cold_nvme_verified_ms\": %.2f, \"cold_verified_speedup_x\": %.2f, \"warm_verified_ms\": %.2f, \"warm_mmap_trusted_ms\": %.2f, \"warm_trusted_speedup_x\": %.2f, \"mean_kl_vs_fp16\": %.3e, \"max_logit_diff_vs_fp16\": %.3e, \"exact_tok_match\": \"%d/%d\", \"needles_ok\": %d}%s\n",
                r.name, r.state_bytes, fp16_sz / (double)r.state_bytes,
                r.cold_prefill_ms, r.save_ms,
                r.cold_verified_restore_ms, r.cold_prefill_ms / r.cold_verified_restore_ms,
                r.warm_verified_restore_ms, r.warm_trusted_restore_ms, r.cold_prefill_ms / r.warm_trusted_restore_ms,
                r.mean_kl_vs_fp16, r.max_logit_diff, r.exact_toks, r.total_toks, r.needles_ok,
                (i + 1 < results.size() ? "," : ""));
    }
    fprintf(fout, "  ],\n");
    fprintf(fout, "  \"controls\": {\"model_mismatch_blocked\": %s, \"kv_quant_mismatch_blocked\": %s, \"bitflip_parallel_merkle_blocked\": %s},\n",
            ctrl_model_ok?"true":"false", ctrl_kvq_ok?"true":"false", ctrl_bitflip_ok?"true":"false");
    bool pass = has_shani && ctrl_model_ok && ctrl_kvq_ok && ctrl_bitflip_ok
                && results[0].needles_ok == 5 && results[1].needles_ok == 5 && results[2].needles_ok == 5
                && results[2].state_bytes < results[0].state_bytes;
    fprintf(fout, "  \"verdict\": \"%s\"\n}\n", pass ? "PASS" : "FAIL");
    fclose(fout);
    printf("CONTROLS: model_mismatch=%d kv_quant_mismatch=%d bitflip_merkle=%d | VERDICT: %s\n",
           ctrl_model_ok, ctrl_kvq_ok, ctrl_bitflip_ok, pass ? "PASS" : "FAIL");
    return pass ? 0 : 10;
}
