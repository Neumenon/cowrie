# CLOUD-LANE-01 (`EXP-04`): Live `vLLM 0.15+` + `CowrieKVConnectorV1` Colab Runbook

**Notebook:** [`notebooks/EXP-04-cowrie-kv-connector.ipynb`](../notebooks/EXP-04-cowrie-kv-connector.ipynb)  
**Adapter:** [`adapters/vllm/cowrie_kv_connector.py`](../adapters/vllm/cowrie_kv_connector.py)  
**Audit gap addressed:** [`reports/cowrie-latest-claims-audit-2026-11-28.md`](../reports/cowrie-latest-claims-audit-2026-11-28.md) §4 (vLLM 0.15+ `vllm.general_plugins` auto-discovery, live `SupportsHMA` runtime verification, and cross-restart KV restore on datacenter HBM3 / Blackwell silicon).

---

## 1. Credit Pool & Zero-Paid-Spend Guardrails

| Item | Specification |
|---|---|
| **Paying credit pool** | **Google AI Ultra — Google Colab Compute Units** (`2,000 CU/mo`, 90-day rollover up to `6,000 CU`). |
| **Paid GCP / API spend** | **`$0.00`** — does **not** use the `$100/mo` Ultra GCP/API credit pool, Cloud Run, Compute Engine, or external GPU rentals. |
| **External credentials required** | **None** — all four runnable Colab profiles are ungated on Hugging Face, and `COWRIE_HMAC_KEY` is generated ephemerally per run. |
| **No-BF16 Colab rule** | **Zero runnable Colab profiles use BF16 weights** (enforced in code by `compute_memory_footprint`). |
| **Idle-CU stop rule** | Call `google.colab.runtime.unassign()` (or **Runtime → Disconnect and delete runtime**) immediately after downloading `/tmp/cowrie_exp04_receipt.json`. |

---

## 2. Final Model Roster & Honest VRAM Footprints

### A. Single-GPU Colab Ultra Profiles (Active in Notebook — All Quantized, Identical Benchmark)

Every profile runs the **exact same benchmark configuration** (`max_model_len=8192`, `gpu_memory_utilization=0.90`, `PROMPT_ROWS=120`, `DECODE_MAX_TOKENS=64`, identical 3-corpus Store→Teardown→Restore protocol) so TTFT, tokens/s, KV hit rate, and connector counters are directly comparable across models. Cell 1 prints every profile's parameter count and computed weight footprint; Cell 4 (`exp04-gpu-preflight`) queries `nvidia-smi` total/free VRAM + compute capability and **rejects any profile that cannot fit before downloading or loading weights**.

| # | Profile Key | Hugging Face Repo ID | Provenance | Exact / Total Params (Active) | Checkpoint Precision | Computed Raw Weight Footprint | Est. Loaded (Weights + Overhead) | Min Required VRAM (incl. `8 GiB` Min KV) | Projected KV Headroom (H100 `79.6 GiB` / Blackwell `96 GiB`) |
|---:|---|---|---|---:|---|---:|---:|---:|---|
| 1 | **`qwen3.8-27b-fp8`** *(DEFAULT)* | `Qwen/Qwen3.8-27B-FP8` | Official FP8 | `27,781,427,952` (`27.78B`) | `F8_E4M3` (`1.0 B/param`) | **`25.87 GiB`** (`27.78 GB`) | `30.87 GiB` (`+5.0 GiB`) | **`38.87 GiB`** | `40.77 GiB` / `55.53 GiB` |
| 2 | **`qwen3.6-27b-fp8`** | `Qwen/Qwen3.6-27B-FP8` | Official FP8 | `27,800,000,000` (`27.80B`) | `F8_E4M3` (`1.0 B/param`) | **`25.89 GiB`** (`27.80 GB`) | `30.89 GiB` (`+5.0 GiB`) | **`38.89 GiB`** | `40.75 GiB` / `55.51 GiB` |
| 3 | **`inkling-2-mini`** | `thinkingmachines/Inkling-2-Mini` | Official pre-quantized *(do not convert)* | `26,388,646,440` (`~3B` active, looped MoE) | Mixed `U8`/`F8_E4M3` (`24.49B`) + `1.9B` `BF16` | **`26.35 GiB`** (`28.29 GB` conservative upper bound) | `30.35 GiB` (`+4.0 GiB`) | **`38.35 GiB`** | `41.29 GiB` / `56.05 GiB` |
| 4 | **`glm-4.7-flash-awq`** | `cyankiwi/GLM-4.7-Flash-AWQ-4bit` | **Community 4-bit AWQ** *(NOT official)* | `31,000,000,000` (`~3B` active, MoE + MLA) | `AWQ_INT4` (`~29B` 4-bit + `~2B` FP16 embed/head; vLLM AWQ path) | **`17.23 GiB`** (`18.50 GB`; `14.44 GiB` pure 4-bit floor) | `22.73 GiB` (`+5.5 GiB` scales + runtime) | **`30.73 GiB`** | `48.91 GiB` / `63.67 GiB` |

> **Important Provenance & Compatibility Notes:**
> - **GLM-4.7-Flash provenance:** `cyankiwi/GLM-4.7-Flash-AWQ-4bit` is a **community 4-bit AWQ quantization**, not an official Z.ai (`zai-org`) build. The official `zai-org/GLM-4.7-Flash` checkpoint is **BF16-only (`~62 GB` / `~57.7 GiB`)** and is therefore excluded from runnable Colab profiles.
> - **Inkling-2-Mini quantization:** `thinkingmachines/Inkling-2-Mini` ships already quantized (mixed `U8`/`F8_E4M3` with only `1.9B` params in `BF16`). Load it natively with `dtype="auto"` and `quantization=None` — **do not re-quantize or convert**.
> - **Hardware FP8 gate (`sm_89+`):** Profiles 1–3 (`qwen3.8-27b-fp8`, `qwen3.6-27b-fp8`, `inkling-2-mini`) require native FP8 hardware (`sm_89+`: Hopper H100 `sm_90` or Blackwell RTX PRO 6000 `sm_120`); Cell 4 rejects older GPUs (`T4 sm_75`, `A100 sm_80`) before download.
> - **Hybrid / MLA KV cache note:** `Qwen3.8-27B-FP8` / `Qwen3.6-27B-FP8` use hybrid attention (`SupportsHMA`), and `GLM-4.7-Flash` uses Multi-head Latent Attention (MLA). `CowrieKVConnectorV1` supports FlashAttention `(2, blocks, ...)`, FlashInfer `(blocks, 2, ...)`, and MLA layouts, failing closed (`ValueError`) on unrecognized recurrent state shapes rather than silently corrupting KV cache state.

### B. Excluded / Post-EIN Multi-GPU Targets (**NOT** Colab Profiles)

These models are **never** included in `notebooks/EXP-04-cowrie-kv-connector.ipynb`:

| Hugging Face Repo ID | Exact / Total Params | Why Excluded from Colab | Target Environment |
|---|---:|---|---|
| **`zai-org/GLM-4.7-Flash`** | `~31,000,000,000` | Official checkpoint is **BF16-only (`~62 GB`)**; no official quantized build exists yet (Colab lane uses community `cyankiwi/GLM-4.7-Flash-AWQ-4bit` instead). | Excluded from Colab (BF16-only) |
| **`zai-org/GLM-5.3-Flash`** | `321,323,031,390` | **`299.26 GiB`** raw FP8 (`3.74×` single H100 80GB) / **`598.51 GiB`** raw BF16. | **`8× H100 80GB` post-EIN target only** |
| **`deepseek-ai/DeepSeek-V4.1-Flash-1017`** | `763,205,315,794` | **`710.79 GiB`** raw FP8 (`8.88×` single H100 80GB; exceeds even `8× H100 80GB` `640 GiB`) / **`1,421.58 GiB`** raw BF16. | **`8× H200 141GB` / multi-node H100 post-EIN target only** |

---

## 3. Colab Hardware Profiles, Expected Runtime & Cost in Compute Units

Burn rates come from the late-2026 Google AI Ultra resource audit (`2026-11-ultra-aeo-monetization-plan.md`). All wall times and CU totals are **pre-execution estimates** (actual runtimes are **not measured** until executed in Colab).

| Colab Ultra Profile | GPU / Arch / VRAM | Burn Rate (Est.) | Active Colab Models | Est. Wall Time (Fresh VM, 1 Profile) | Est. Cost per Profile (CU) | Share of 2,000 CU/mo Pool |
|---|---|---:|---|---:|---:|---:|
| **Primary** | NVIDIA **H100 80GB** HBM3 (`sm_90`) | `~18.0 CU/hr` | All 4 quantized profiles (`qwen3.8-27b-fp8` default) | `8 – 15 min` | **`2.4 – 4.5 CU`** | `0.12% – 0.23%` |
| **Blackwell Variant** | NVIDIA **RTX PRO 6000 Blackwell 96GB** (`sm_120`, G4) | `~8.9 CU/hr` | All 4 quantized profiles (`qwen3.8-27b-fp8` default) | `8 – 15 min` | **`1.2 – 2.2 CU`** | `0.06% – 0.11%` |

### Estimated Phase Breakdown (Fresh Colab VM, 1 Profile)
1. `pip install -U vllm>=0.15.0` + minimal `rustup` + `cowrie-ai` Rust release build: **~3–6 min** (`0–1 min` on subsequent profiles in the same VM).
2. Hugging Face weight download (`~18.5 – 28.3 GB` across the 4 quantized profiles): **~3–6 min** (`0 min` once cached).
3. `SupportsHMA` factory verification + Phase 1 (Cold Store subprocess) + full VRAM teardown + Phase 2 (Warm Restore subprocess): **~2–3 min**.

---

## 4. Exact Run Steps

### Prerequisites
- Push `feat/cloud-lane-01` to `https://github.com/Neumenon/cowrie` (or merge into `main`), or upload the checkout to `/content/cowrie` in Colab (Cell 5 auto-detects `/content/cowrie`).

### Step-by-Step (Colab Web UI or Official Colab VS Code Extension)
1. Open [`notebooks/EXP-04-cowrie-kv-connector.ipynb`](../notebooks/EXP-04-cowrie-kv-connector.ipynb) in Google Colab (Google AI Ultra account).
2. Select runtime: **Runtime → Change runtime type → H100** (or **RTX PRO 6000 Blackwell / G4**).
3. *(Optional profile switch — zero edits needed for default `qwen3.8-27b-fp8`):*
   - Set `MODEL_PROFILE` (or `EXP04_MODEL_PROFILE`) to one of: `"qwen3.8-27b-fp8"`, `"qwen3.6-27b-fp8"`, `"inkling-2-mini"`, `"glm-4.7-flash-awq"`.
   - Set `EXP04_ENFORCE_EAGER=0` to benchmark with `torch.compile` / CUDA graphs.
   - Set `AUTO_UNASSIGN_COLAB_RUNTIME = True` (or `EXP04_AUTO_UNASSIGN=1`) for unattended runs.
4. Click **Runtime → Run all**. Cell 4 verifies hardware and VRAM fit before any weights download.
5. Verify Cell 11 prints `VERDICT: PASS` and download `/tmp/cowrie_exp04_receipt.json` into `receipts/receipt_exp04_<gpu>_<profile>_<YYYY-MM-DD>.json`.
6. **Stop CU billing immediately:** run Cell 13 with `AUTO_UNASSIGN_COLAB_RUNTIME = True` or click **Runtime → Disconnect and delete runtime**.

---

## 5. Current Measurement Status (Pre-Execution)

No Colab GPU run has been executed yet; every live GPU metric is **`not measured`**:

| Metric | Local Workstation | `qwen3.8-27b-fp8` | `qwen3.6-27b-fp8` | `inkling-2-mini` | `glm-4.7-flash-awq` |
|---|---|---|---|---|---|
| `vLLM 0.15+` live plugin auto-discovery | not measured | not measured | not measured | not measured | not measured |
| `SupportsHMA` runtime (`issubclass` + live instances) | mock unit test only | not measured | not measured | not measured | not measured |
| Cold TTFT (`ms`) & Warm Cowrie TTFT (`ms`) | not measured | not measured | not measured | not measured | not measured |
| Decode throughput (`tokens/s`, E2E & decode-only) | not measured | not measured | not measured | not measured | not measured |
| KV cache hit rate (disk-loaded `%` & scheduler `%`) | not measured | not measured | not measured | not measured | not measured |
| Greedy token parity across OS process restart | not measured | not measured | not measured | not measured | not measured |
| Connector counters (`saves_completed`, `loads_completed`, `loaded_bytes`) | unit-tested via `libcowrie_core.so` | not measured | not measured | not measured | not measured |

---

## 6. Success Criteria (Preregistered Pass Gates)

An `EXP-04` run counts as **SUCCESS (`PASS`)** only if **all** of the following hold on live GPU silicon and are saved to `/tmp/cowrie_exp04_receipt.json`:

1. **Memory & hardware preflight:** Cell 4 (`check_model_fits_gpu`) passes (`fits == True`) on real `nvidia-smi` total/free VRAM and compute capability (`sm_89+` for FP8 profiles).
2. **Upstream version & registration:** `vllm.__version__ >= 0.15.0` and `KVConnectorFactory.get_connector_class(...)` resolves `cowrie_vllm_adapters.cowrie_kv_connector.CowrieKVConnectorV1`.
3. **`SupportsHMA` proven `True` at runtime:** `supports_hma_native_import == True`, `issubclass_supports_hma == True`, `has_request_finished_all_groups == True`, and both Phase 1 and Phase 2 live connector snapshots report `all_supports_hma == True`.
4. **Phase 1 (Cold Store) persistence & integrity:** `saves_completed >= 3`, `saved_tokens > 0`, `saved_bytes > 0`, `save_kv_layer_calls > 0`, `saves_failed == 0`, `saves_skipped_inflight_cap == 0`, and all `>= 3` `.state.cowrie` shards pass `cowrie.verify_statepage_file(..., verify_hash=True)` with `hmac_sha256_verified == True`.
5. **Phase 2 (Warm Restore across full process restart):** `scheduler_hits >= 3`, `loads_completed >= 3`, `start_load_kv_calls > 0`, `loaded_tokens > 0`, `loaded_bytes > 0`, `loads_failed == 0`, disk-loaded KV hit rate `>= 85%`, and **100% exact greedy token parity** (`decode_token_ids` identical across all 64 tokens).
6. **Latency win:** `warm_cowrie_ttft_ms < cold_ttft_ms` (`ttft_speedup_x > 1.0`, target `>= 1.5×` at `PROMPT_ROWS=120`).

---

## 7. Failure Criteria (What Counts as a Failed Experiment)

An `EXP-04` run counts as **FAILED** if **any** of the following occur:

1. **Memory fit / OOM failure:** Cell 4 rejects the GPU/profile or vLLM OOMs while loading weights or KV cache.
2. **Silent bypass (caught by `assert_connector_was_used`):** `instance_count == 0`, `save_kv_layer_calls == 0`, `saves_completed < 3`, `scheduler_hits < 3`, `loads_completed < 3`, or `loaded_tokens == 0`.
3. **`SupportsHMA` or hybrid-group failure:** `SupportsHMA` cannot be imported natively from vLLM, `issubclass` is `False`, vLLM disables HMA / rejects the connector, or a hybrid recurrent layer triggers `_detect_kv_layout`'s fail-closed `ValueError` (indicating multi-group recurrent state support must be added to `CowrieKVConnectorV1`).
4. **Greedy token parity mismatch:** `store_res["decode_token_ids"] != restore_res["decode_token_ids"]` under `temperature=0.0`.
5. **Storage / cryptographic integrity failure:** any `loads_failed > 0`, `saves_failed > 0`, `saves_skipped_inflight_cap > 0`, or `cowrie.verify_statepage_file` / HMAC error.
6. **Performance non-win (`FUNCTIONAL_PASS_NO_TTFT_SPEEDUP`):** correctness passes, but `warm_cowrie_ttft_ms >= cold_ttft_ms`. Log honestly and diagnose I/O / host-to-device bandwidth or prompt length before retrying.
