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
> - **Hybrid / MLA KV cache gate (live-verified on vLLM `0.34.0`):** `GLM-4.7-Flash` uses Multi-head Latent Attention (MLA) and is **live-verified PASS** on RTX PRO 6000 Blackwell (`vLLM 0.34.0`, 64/64 greedy parity). By contrast, `Qwen/Qwen3.8-27B-FP8` (`Qwen3_5ForConditionalGeneration`: 48 Gated Delta Net `linear_attention` layers + 16 `full_attention` layers) allocates a hybrid recurrent `MambaSpec` state tensor of shape `(1017, 4, 832, 512)` and hits `CowrieKVConnectorV1`'s **explicit fail-closed gate** (`ValueError` at `register_kv_caches` / `_detect_kv_layout`). See §5.1 below for the design decision.

### B. Excluded / Post-EIN Multi-GPU Targets (**NOT** Colab Profiles)

These models are **never** included in `notebooks/EXP-04-cowrie-kv-connector.ipynb`:

| Hugging Face Repo ID | Exact / Total Params | Why Excluded from Colab | Target Environment |
|---|---:|---|---|
| **`zai-org/GLM-4.7-Flash`** | `~31,000,000,000` | Official checkpoint is **BF16-only (`~62 GB`)**; no official quantized build exists yet (Colab lane uses community `cyankiwi/GLM-4.7-Flash-AWQ-4bit` instead). | Excluded from Colab (BF16-only) |
| **`zai-org/GLM-5.3-Flash`** | `321,323,031,390` | **`299.26 GiB`** raw FP8 (`3.74×` single H100 80GB) / **`598.51 GiB`** raw BF16. | **`8× H100 80GB` post-EIN target only** |
| **`deepseek-ai/DeepSeek-V4.1-Flash-1017`** | `763,205,315,794` | **`710.79 GiB`** raw FP8 (`8.88×` single H100 80GB; exceeds even `8× H100 80GB` `640 GiB`) / **`1,421.58 GiB`** raw BF16. | **`8× H200 141GB` / multi-node H100 post-EIN target only** |

---

## 3. Colab Hardware Profiles, Expected Runtime & Cost in Compute Units

Burn rates come from the late-2026 Google AI Ultra resource audit (`2026-11-ultra-aeo-monetization-plan.md`). H100 wall times and fresh-VM CU totals remain **pre-execution estimates** (H100 is **not measured**). On a warm RTX PRO 6000 Blackwell VM (`exp04`, weights cached), the verified `glm-4.7-flash-awq` run completed end-to-end (`SupportsHMA` proof + Store + teardown + Restore + report) in **`~75 s`**.

| Colab Ultra Profile | GPU / Arch / VRAM | Burn Rate (Est.) | Active Colab Models | Est. Wall Time (Fresh VM, 1 Profile) | Est. Cost per Profile (CU) | Share of 2,000 CU/mo Pool |
|---|---|---:|---|---:|---:|---:|
| **Primary** | NVIDIA **H100 80GB** HBM3 (`sm_90`) | `~18.0 CU/hr` | All 4 quantized profiles (`qwen3.8-27b-fp8` default) — **not measured** | `8 – 15 min` | **`2.4 – 4.5 CU`** | `0.12% – 0.23%` |
| **Blackwell Variant** | NVIDIA **RTX PRO 6000 Blackwell 96GB** (`sm_120`, G4) | `~8.9 CU/hr` | `glm-4.7-flash-awq` **VERIFIED PASS**; `qwen3.8-27b-fp8` **FAIL-CLOSED (hybrid GDN)**; others **not measured** | `8 – 15 min` fresh (`~1.25 min` warm cache) | **`1.2 – 2.2 CU`** fresh (`~0.19 CU` warm) | `0.06% – 0.11%` |

### Estimated Phase Breakdown (Fresh Colab VM, 1 Profile)
1. `pip install -U vllm>=0.15.0` + minimal `rustup` + `cowrie-ai` Rust release build: **~3–6 min** (`0–1 min` on subsequent profiles in the same VM).
2. Hugging Face weight download (`~18.5 – 28.3 GB` across the 4 quantized profiles): **~3–6 min** (`0 min` once cached).
3. `SupportsHMA` factory verification + Phase 1 (Cold Store subprocess) + full VRAM teardown + Phase 2 (Warm Restore subprocess): **~1.25–3 min** (`~75 s` measured on warm Blackwell `exp04`).

---

## 4. Exact Run Steps

### Prerequisites
- Push `feat/cloud-lane-01` to `https://github.com/Neumenon/cowrie` (or merge into `main`), or upload the checkout to `/content/cowrie` in Colab (Cell 5 auto-detects `/content/cowrie`).

### Step-by-Step (Colab Web UI or Official Colab VS Code Extension)
1. Open [`notebooks/EXP-04-cowrie-kv-connector.ipynb`](../notebooks/EXP-04-cowrie-kv-connector.ipynb) in Google Colab (Google AI Ultra account).
2. Select runtime: **Runtime → Change runtime type → H100** (or **RTX PRO 6000 Blackwell / G4**).
3. *(Optional profile switch — zero edits needed for default `qwen3.8-27b-fp8`):*
   - Set `MODEL_PROFILE` (or `EXP04_MODEL_PROFILE`) to one of: `"qwen3.8-27b-fp8"`, `"qwen3.6-27b-fp8"`, `"inkling-2-mini"`, `"glm-4.7-flash-awq"`.
   - Note: `glm-4.7-flash-awq` is the live-verified passing profile on vLLM `0.34.0` (`layout_ver=3` MLA); `qwen3.8-27b-fp8` hits the intentional fail-closed hybrid linear-attention gate (§5.1).
   - Set `EXP04_ENFORCE_EAGER=0` to benchmark with `torch.compile` / CUDA graphs.
   - Set `AUTO_UNASSIGN_COLAB_RUNTIME = True` (or `EXP04_AUTO_UNASSIGN=1`) for unattended runs.
4. Click **Runtime → Run all**. Cell 4 verifies hardware and VRAM fit before any weights download.
5. Verify Cell 11 prints `VERDICT: PASS` and download `/tmp/cowrie_exp04_receipt.json` into `receipts/receipt_exp04_<gpu>_<profile>_<YYYY-MM-DD>.json`.
6. **Stop CU billing immediately:** run Cell 13 with `AUTO_UNASSIGN_COLAB_RUNTIME = True` or click **Runtime → Disconnect and delete runtime**.

---

## 5. Measurement Status (Live RTX PRO 6000 Blackwell + Local)

**Verified Live Receipt (RTX PRO 6000 Blackwell `95.59 GiB`, `sm_120`, vLLM `0.34.0`, `enforce_eager=True`):** [`receipts/receipt_exp04_rtx_pro_6000_blackwell_glm47_awq_2026-11-28.json`](../receipts/receipt_exp04_rtx_pro_6000_blackwell_glm47_awq_2026-11-28.json).  
All H100 80GB metrics, `qwen3.6-27b-fp8`, and `inkling-2-mini` remain **`not measured`**.

| Metric | Local Workstation | `qwen3.8-27b-fp8` (Blackwell) | `qwen3.6-27b-fp8` | `inkling-2-mini` | `glm-4.7-flash-awq` (RTX PRO 6000 Blackwell, vLLM `0.34.0`) |
|---|---|---|---|---|---|
| `vLLM 0.15+` live plugin auto-discovery | not measured | verified (`vLLM 0.34.0`) | not measured | not measured | **PASS** (`vLLM 0.34.0`, `auto_registered_via_entry_point=true`) |
| `SupportsHMA` runtime (`issubclass` + live instances) | mock unit test (`6/6`) | verified (`True`) before KV register | not measured | not measured | **PASS** (`native=True`, `issubclass=True`, `all_supports_hma=True`, `request_finished_all_groups_calls=5/4`) |
| KV cache layout registration | unit-tested (`GQA`, `MLA`, hybrid fail-closed) | **FAIL-CLOSED** (`ValueError`: hybrid `(1017, 4, 832, 512)`) | not measured | not measured | **PASS** (`mla`, 47 layers, `block_size=64`) |
| Cold TTFT (`ms`) → Warm Cowrie TTFT (`ms`) | not measured | not measured | not measured | not measured | **`335.49 ms` → `98.32 ms` (`3.412×` speedup)** (`4,217` prompt tok) |
| Decode throughput (`tokens/s`, 64 out tok) | not measured | not measured | not measured | not measured | Cold E2E `24.36 tok/s` (`2,627.68 ms`); Warm E2E **`26.85 tok/s`** (`2,383.27 ms`); Warm decode-only **`27.57 tok/s`**; New-suffix E2E **`26.91 tok/s`** (`2,378.65 ms`) |
| KV cache hit rate (disk-loaded `%` / scheduler `%` / req `%`) | not measured | not measured | not measured | not measured | **`99.10%`** / **`99.10%`** (`12,544 / 12,658` tok; `vllm_num_cached_tokens_total=12,544`) / **`100.0%`** (`3/3`) |
| Greedy token parity across OS process restart (`temp=0.0`) | unit-tested bit-exact | not measured | not measured | not measured | **PASS (`64/64` exact)**: `greedy_token_parity=true` (`store_cold == restore_cowrie`) & `gpu_prefix_hit_parity=true` (`store_gpu_hit == restore_cowrie`) |
| Connector counters (`saves_completed`, `loads_completed`, payload, HMAC) | unit-tested via `libcowrie_core.so` | not measured | not measured | not measured | Store: `saves_completed=3`, `saved_tokens=12,544`, `saved_bytes=679,182,336`, `saves_failed=0`, `hmac_sha256_verified=true` (`3` shards, `679,248,154 B` on disk). Restore: `scheduler_hits=3`, `loads_completed=3`, `loaded_tokens=12,544`, `loaded_bytes=679,182,336`, `load_ms_total=97.261 ms`, `loads_failed=0` |

### 5.1 Decision: `Qwen/Qwen3.8-27B-FP8` Explicit Fail-Closed Gate (Hybrid Linear Attention)

Live inspection of `Qwen/Qwen3.8-27B-FP8` (`config.json` and vLLM `0.34.0` KV allocation on `exp04`) established that it uses `Qwen3_5ForConditionalGeneration` with **64 layers = 48 `linear_attention` (Gated Delta Net: `16` K heads, `48` V heads, `128` head dim, conv kernel `4`) + 16 `full_attention` (`full_attention_interval=4`)**. vLLM `0.34.0` allocates a hybrid recurrent `MambaSpec` state tensor with shape `(1017, 4, 832, 512)`.

**Architectural decision:** Keep `CowrieKVConnectorV1` **explicitly fail-closed** on hybrid recurrent/Mamba KV tensors in `VLLM_LAYOUT_VER = 3` (`register_kv_caches` and `_detect_kv_layout` raise `ValueError` immediately rather than silently skipping recurrent layers or writing corrupt KV pages). Correct persistence of GDN/Mamba hybrid state requires:
1. Heterogeneous multi-group `block_ids[group_idx]` tracking per KV cache group (currently `layout_ver=3` persists a single homogeneous attention block sequence),
2. `mamba_cache_mode="align"` checkpoint semantics (saving/restoring both conv1d and SSM recurrent state tensors per aligned boundary rather than per-token slices), and
3. A dedicated wire-format bump (`VLLM_LAYOUT_VER = 4`) with unit and live parity verification.

`Qwen/Qwen3.8-27B-FP8` stays in the notebook's owner-selected roster as an explicit fail-closed gate until `VLLM_LAYOUT_VER = 4` implements `MambaSpec` multi-group checkpointing. `Qwen/Qwen3.6-27B-FP8` is **not measured** and will trigger the same fail-closed gate if its checkpoint uses the same `Qwen3_5ForConditionalGeneration` hybrid linear-attention architecture.

### 5.2 vLLM `0.34.0` Portable Connector Fixes (Verified Against Installed vLLM Source)

All vLLM `0.34.0` fixes in [`adapters/vllm/cowrie_kv_connector.py`](../adapters/vllm/cowrie_kv_connector.py) are guarded by runtime capability rather than version strings so vLLM `0.15+` through `0.34+` remain supported:

1. **`CachedRequestData` without `resumed_req_ids`** (`vllm/v1/core/sched/output.py`): `build_connector_meta` checks `resumed_ids = getattr(cached_reqs, "resumed_req_ids", None)` and falls back to `req_id in self._requests_need_load` when `resumed_req_ids` is absent.
2. **Per-layer dict / speculative list `attn_metadata`** (`vllm/model_executor/layers/attention/attention.py`): `_is_mla_metadata` walks `dict[str, AttentionMetadata]`, `list`/`tuple`, and single metadata objects to detect `MLACommonMetadata` / `*MLA*` backends.
3. **Symmetric stride-safe 4-D / 3-D MLA & GQA block indexing** (`vllm/v1/kv_cache_interface.py` `create_kv_cache_views` + `vllm/model_executor/layers/attention/mla_attention.py` `bind_kv_cache`): vLLM `0.34.0` registers 4-D strided views `[B, 1, N, C]` (`(19541, 1, 64, 576)`, `LBNHC`) in `register_kv_caches` while passing 3-D squeezed `(19541, 64, 576)` views to `save_kv_layer`. `_detect_kv_layout`, `save_kv_layer`, and `start_load_kv` now use symmetric in-place `blk = slots // page_size; off = slots % page_size` indexing across `mla_4d`, `mla_3d`, `2_blocks_first`, and `blocks_2_first` (never `.view()`/`.reshape()` into a copy on non-contiguous `as_strided` tensors), with synchronous H2D transfer (`non_blocking=False`) so mmap pages stay pinned until the CUDA copy completes.
4. **Chat-templated instruct prompts in EXP-04 worker:** Live diagnostic verification on `exp04` proved `CowrieKVConnectorV1` restored all `47 × 4,160 × 576` MLA elements bit-identically (`0` tensor mismatches, `torch.equal == True` on every layer), while raw non-chat prompts caused `GLM-4.7-Flash-AWQ` to emit degenerate near-tied loops where vLLM's own native GPU prefix cache (absorbed MLA + online softmax merge) diverged from cold non-absorbed prefill at token `25`. Formatting prompts with `tok.apply_chat_template(..., add_generation_prompt=True)` yields coherent generation and exact **`64/64` greedy parity** across both `store_cold == restore_cowrie` and `store_gpu_hit == restore_cowrie` across two separate OS processes.

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
