# CLOUD-LANE-01 (`EXP-04`): Live `vLLM 0.15+` + `CowrieKVConnectorV1` Colab Runbook

**Notebook:** [`notebooks/EXP-04-cowrie-kv-connector.ipynb`](../notebooks/EXP-04-cowrie-kv-connector.ipynb)  
**Adapter:** [`adapters/vllm/cowrie_kv_connector.py`](../adapters/vllm/cowrie_kv_connector.py)  
**Audit gap addressed:** [`reports/cowrie-latest-claims-audit-2026-11-28.md`](../reports/cowrie-latest-claims-audit-2026-11-28.md) §4 (vLLM 0.15+ `vllm.general_plugins` auto-discovery, live `SupportsHMA` runtime verification on hybrid-attention models, and cross-restart KV restore on datacenter HBM3 / Blackwell silicon).

---

## 1. Credit Pool & Zero-Paid-Spend Guardrails

| Item | Specification |
|---|---|
| **Paying credit pool** | **Google AI Ultra — Google Colab Compute Units** (`2,000 CU/mo`, 90-day rollover up to `6,000 CU`). |
| **Paid GCP / API spend** | **`$0.00`** — does **not** use the `$100/mo` Ultra GCP/API credit pool, Cloud Run, Compute Engine, or external GPU rentals. |
| **External credentials required** | **None** — both Colab profiles (`Qwen/Qwen3.8-27B-FP8` and `Qwen/Qwen3.5-35B-A3B`) are ungated on Hugging Face, and `COWRIE_HMAC_KEY` is generated ephemerally per run. |
| **Idle-CU stop rule** | Call `google.colab.runtime.unassign()` (or **Runtime → Disconnect and delete runtime**) immediately after downloading `/tmp/cowrie_exp04_receipt.json`. |

---

## 2. Verified Model Matrix & Honest VRAM Footprints

### A. Single-GPU Colab Ultra Profiles (Active in Notebook)

Raw weight footprints are computed directly as `params × bytes_per_param / 1024³` (`GiB`) and `params × bytes_per_param / 1e9` (`GB`). Cell 4 (`exp04-gpu-preflight`) queries `nvidia-smi` for total & free VRAM and **asserts the selected profile fits before downloading or loading weights**.

| Profile Key | Hugging Face Repo ID | Exact / Total Params | Active Params | Checkpoint Dtype | Raw Weight Footprint | Est. Loaded (Weights + Runtime) | Min Required VRAM (incl. KV Headroom) | Single H100 80GB (`79.6 GiB`) | Single RTX PRO 6000 Blackwell (`96 GiB`) |
|---|---|---:|---:|---|---:|---:|---:|---|---|
| **`qwen3.8-27b-fp8`** *(DEFAULT)* | `Qwen/Qwen3.8-27B-FP8` | `27,781,427,952` | `27,781,427,952` | `F8_E4M3` (`1 B/param`) | **`25.87 GiB`** (`27.78 GB`) | `~30.87 GiB` (`+5.0 GiB` est.) | **`38.87 GiB`** (`+8.0 GiB` min KV) | **Fits with room** (`~40.8 GiB` KV headroom at `0.90` util) | **Fits with room** (`~55.5 GiB` KV headroom at `0.90` util) |
| **`qwen3.5-35b-a3b`** *(ALTERNATE, MoE)* | `Qwen/Qwen3.5-35B-A3B` | `35,000,000,000` | `3,000,000,000` | `BF16` (`2 B/param`) | **`65.19 GiB`** (`70.00 GB`) | `~69.19 GiB` (`+4.0 GiB` est.) | **`73.19 GiB`** (`+4.0 GiB` min KV) | **Fits tightly** (`~4.0 GiB` KV headroom at `0.92` util, `max_model_len=8192`; all 35B MoE weights reside in VRAM) | **Fits with room** (`~19.1 GiB` KV headroom at `0.92` util) |

> **Hybrid Attention / `SupportsHMA` Note:** Both `Qwen/Qwen3.8-27B-FP8` and `Qwen/Qwen3.5-35B-A3B` use hybrid attention (full attention + linear recurrent layers). Running `EXP-04` on them exercises vLLM's Hybrid Memory Allocator (`SupportsHMA`) for real. If vLLM exposes a second recurrent KV-cache group whose tensor shape differs from `(2, blocks, ...)` / `(blocks, 2, ...)` / MLA, `CowrieKVConnectorV1` fails closed (`ValueError`) instead of silently corrupting state — that outcome must be logged honestly as an HMA multi-group gap to implement next.

### B. Post-EIN Multi-GPU / Multi-Node Targets (**NOT** for Colab)

The following models **do not fit** any single-GPU Colab runtime (H100 80GB or RTX PRO 6000 Blackwell 96GB) and are **excluded** from `notebooks/EXP-04-cowrie-kv-connector.ipynb`. They are tracked here strictly as post-EIN multi-GPU / multi-node targets once OmenWorks LLC unlocks multi-GPU cluster credits:

| Hugging Face Repo ID | Exact Params | Raw FP8 Footprint (`1 B/param`) | Raw BF16 Footprint (`2 B/param`) | Why Excluded from Single-GPU Colab | Minimum Post-EIN Cluster Target |
|---|---:|---:|---:|---|---|
| **`zai-org/GLM-5.3-Flash`** | `321,323,031,390` | **`299.26 GiB`** (`321.32 GB`) | **`598.51 GiB`** (`642.65 GB`) | FP8 weights alone are **`3.74×`** a single H100 80GB (`3.12×` a single RTX PRO 6000 96GB) | `8× H100 80GB` (`640 GiB`) or `4× H200 141GB` (FP8); `8× H200` / multi-node (BF16) |
| **`deepseek-ai/DeepSeek-V4.1-Flash-1017`** | `763,205,315,794` | **`710.79 GiB`** (`763.21 GB`) | **`1,421.58 GiB`** (`1,526.41 GB`) | FP8 weights alone are **`8.88×`** a single H100 80GB and exceed even a single `8× H100 80GB` node (`640 GiB`) | `8× H200 141GB` (`1,128 GiB`) or `2× 8× H100` multi-node (FP8); multi-node H200/B200 (BF16) |

---

## 3. Colab Hardware Profiles, Expected Runtime & Cost in Compute Units

Burn rates come from the late-2026 Google AI Ultra resource audit (`2026-11-ultra-aeo-monetization-plan.md`). All wall times and CU totals are **pre-execution estimates** (actual runtimes are **not measured** until executed in Colab).

| Colab Ultra Profile | GPU / Arch / VRAM | Burn Rate (Est.) | Active Colab Models | Est. Wall Time (Fresh VM) | Est. Cost per Run (CU) | Share of 2,000 CU/mo Pool |
|---|---|---:|---|---:|---:|---:|
| **Primary** | NVIDIA **H100 80GB** HBM3 (`sm_90`) | `~18.0 CU/hr` | `qwen3.8-27b-fp8` (default), `qwen3.5-35b-a3b` (alternate) | `8 – 16 min` (`12 – 22 min` on 35B BF16) | **`2.4 – 4.8 CU`** (`3.6 – 6.6 CU` on 35B) | `0.12% – 0.33%` |
| **Blackwell Variant** | NVIDIA **RTX PRO 6000 Blackwell 96GB** (`sm_120`, G4) | `~8.9 CU/hr` | `qwen3.8-27b-fp8` (default), `qwen3.5-35b-a3b` (alternate) | `8 – 16 min` (`12 – 22 min` on 35B BF16) | **`1.2 – 2.4 CU`** (`1.8 – 3.3 CU` on 35B) | `0.06% – 0.17%` |

### Estimated Phase Breakdown (Fresh Colab VM, `qwen3.8-27b-fp8`)
1. `pip install -U vllm>=0.15.0` + minimal `rustup` + `cowrie-ai` Rust release build: **~3–6 min** (`0–1 min` on repeat runs in the same VM).
2. Hugging Face weight download (`Qwen/Qwen3.8-27B-FP8` `~27.8 GB` vs `Qwen/Qwen3.5-35B-A3B` `~70.0 GB`): **~3–7 min** for `27B-FP8` (**~6–12 min** for `35B-A3B` BF16); `0 min` once cached in `~/.cache/huggingface`.
3. `SupportsHMA` factory verification + Phase 1 (Cold Store subprocess) + full VRAM teardown + Phase 2 (Warm Restore subprocess): **~2–3 min**.

> **KV Layout Note (Hopper vs Blackwell):** `CowrieKVConnectorV1` supports both FlashAttention `(2, num_blocks, block_size, n_kv, head_dim)` (`2_blocks_first`) and FlashInfer `(num_blocks, 2, block_size, n_kv, head_dim)` (`blocks_2_first`), recording the observed runtime layout in `kv_layouts` and failing closed on unrecognized shapes.

---

## 4. Exact Run Steps

### Prerequisites
- Branch `feat/cloud-lane-01` pushed to `https://github.com/Neumenon/cowrie` (or merged into `main`). If running before pushing, upload the repo checkout to `/content/cowrie` in Colab — Cell 5 automatically detects `/content/cowrie` and skips `git clone`.

### Step-by-Step (Colab Web UI or Official Colab VS Code Extension)
1. Open [`notebooks/EXP-04-cowrie-kv-connector.ipynb`](../notebooks/EXP-04-cowrie-kv-connector.ipynb) in Google Colab (signed into the Google AI Ultra account).
2. Select runtime: **Runtime → Change runtime type → Hardware accelerator: H100** (or **RTX PRO 6000 Blackwell / G4** for the Blackwell variant).
3. *(Optional switches — zero edits needed for default `Qwen/Qwen3.8-27B-FP8`):*
   - For the **MoE alternate** (`Qwen/Qwen3.5-35B-A3B`), set `MODEL_PROFILE = "qwen3.5-35b-a3b"` (or `export EXP04_MODEL_PROFILE=qwen3.5-35b-a3b`).
   - For `torch.compile` + CUDA graph decode timing instead of eager mode, set `EXP04_ENFORCE_EAGER=0`.
   - For unattended runs, set `AUTO_UNASSIGN_COLAB_RUNTIME = True` (or `EXP04_AUTO_UNASSIGN=1`) in Cell 1.
4. Click **Runtime → Run all**. Do not skip cells. Cell 4 asserts hardware + VRAM fit before any weights download.
5. Verify the printed summary table in Cell 11 (`VERDICT: PASS`) and confirm zero fail-loud exceptions in Cell 10.
6. Download `/tmp/cowrie_exp04_receipt.json` and save it into this repo as `receipts/receipt_exp04_<gpu>_<model>_<YYYY-MM-DD>.json`.
7. **Immediately stop CU billing:** run Cell 13 with `AUTO_UNASSIGN_COLAB_RUNTIME = True` or click **Runtime → Disconnect and delete runtime**.

---

## 5. Current Measurement Status (Pre-Execution)

Local workstation checks verified the Rust core, WASM adversarial suite, connector unit tests (`tests/test_vllm_kv_connector_unit.py`), and notebook `nbformat` validation + non-GPU cells. **No Colab GPU run has been executed yet:**

| Metric | Local Workstation | Colab H100 80GB (`qwen3.8-27b-fp8`) | Colab H100 80GB (`qwen3.5-35b-a3b`) | Colab RTX PRO 6000 Blackwell (`qwen3.8-27b-fp8`) |
|---|---|---|---|---|
| `vLLM 0.15+` live plugin auto-discovery | not measured (`vllm` absent locally) | not measured | not measured | not measured |
| `SupportsHMA` runtime (`issubclass` + live instances) | unit-tested with mock `vllm` only | not measured | not measured | not measured |
| Cold TTFT (`ms`) | not measured | not measured | not measured | not measured |
| Warm Cowrie Restored TTFT (`ms`) & speedup (`×`) | not measured | not measured | not measured | not measured |
| Decode throughput (`tokens/s`, E2E & decode-only) | not measured | not measured | not measured | not measured |
| KV cache hit rate (disk-loaded `%` & scheduler `%`) | not measured | not measured | not measured | not measured |
| Greedy token parity across OS process restart | not measured | not measured | not measured | not measured |
| Connector counters (`saves_completed`, `loads_completed`, `loaded_bytes`) | unit-tested with real `libcowrie_core.so` | not measured | not measured | not measured |

---

## 6. Success Criteria (Preregistered Pass Gates)

An `EXP-04` run counts as **SUCCESS (`PASS`)** only if **all** of the following hold on live GPU silicon and are recorded in `/tmp/cowrie_exp04_receipt.json`:

1. **Memory & hardware preflight:** Cell 4 (`check_model_fits_gpu`) passes (`fits == True`) on real `nvidia-smi` total/free VRAM and compute capability (`sm_89+` for `F8_E4M3`).
2. **Upstream version & registration:** `vllm.__version__ >= 0.15.0` and `KVConnectorFactory.get_connector_class(...)` resolves `cowrie_vllm_adapters.cowrie_kv_connector.CowrieKVConnectorV1` (with `auto_registered_via_entry_point` recorded transparently).
3. **`SupportsHMA` proven `True` at runtime:**
   - `hma_proof.supports_hma_native_import == True` (imported from `vllm.distributed.kv_transfer.kv_connector.v1.base.SupportsHMA`, never the fallback stub),
   - `hma_proof.issubclass_supports_hma == True`,
   - `hma_proof.has_request_finished_all_groups == True`,
   - Both Phase 1 and Phase 2 live connector counter snapshots report `all_supports_hma == True`.
4. **Phase 1 (Cold Store) persistence & integrity:**
   - `saves_completed >= 3`, `saved_tokens > 0`, `saved_bytes > 0`, `save_kv_layer_calls > 0`,
   - `saves_failed == 0` and `saves_skipped_inflight_cap == 0`,
   - All `>= 3` `.state.cowrie` shards pass independent `cowrie.verify_statepage_file(..., verify_hash=True)` with `hmac_sha256_verified == True`.
5. **Phase 2 (Warm Restore across full process restart):**
   - `scheduler_hits >= 3`, `loads_completed >= 3`, `start_load_kv_calls > 0`, `loaded_tokens > 0`, `loaded_bytes > 0`, `loads_failed == 0`,
   - Disk-loaded KV cache hit rate `>= 85%` across the 3 prompts (2 identical + 1 brand-new suffix sharing the saved prefix),
   - **100% exact greedy token parity** (`decode_token_ids` identical between Process 1 and Process 2 over all 64 generated tokens).
6. **Latency win:** `warm_cowrie_ttft_ms < cold_ttft_ms` (`ttft_speedup_x > 1.0`, target `>= 1.5×` at `PROMPT_ROWS=120` / ~2.5K–3K prompt tokens; larger prompts increase prefill-vs-restore ratio).

---

## 7. Failure Criteria (What Counts as a Failed Experiment)

An `EXP-04` run counts as **FAILED** (and must be diagnosed before making any compatibility or speedup claim) if **any** of the following occur:

1. **Memory fit / OOM failure:** Cell 4 fails `check_model_fits_gpu` (e.g., wrong Colab GPU or insufficient free VRAM) or vLLM OOMs while loading `Qwen/Qwen3.8-27B-FP8` or `Qwen/Qwen3.5-35B-A3B`.
2. **Silent bypass (caught automatically by `assert_connector_was_used`):**
   - `instance_count == 0`, `save_kv_layer_calls == 0`, `saves_completed < 3`, `scheduler_hits < 3`, `loads_completed < 3`, or `loaded_tokens == 0`.
3. **`SupportsHMA` or hybrid-group failure:**
   - `SupportsHMA` cannot be imported from `vllm.distributed.kv_transfer.kv_connector.v1.base`, `issubclass(CowrieKVConnectorV1, SupportsHMA)` is `False`, vLLM rejects the connector / disables HMA, or a hybrid recurrent layer triggers `_detect_kv_layout`'s fail-closed `ValueError` (proving multi-group recurrent-state persistence is needed in `CowrieKVConnectorV1`).
4. **Greedy token parity mismatch:**
   - `store_res["decode_token_ids"] != restore_res["decode_token_ids"]` under `temperature=0.0` (indicates KV cache axis/layout corruption, missing recurrent-state restore on hybrid layers, slot-mapping bug, or dtype mismatch).
5. **Storage / cryptographic integrity failure:**
   - Any `loads_failed > 0`, `saves_failed > 0`, `saves_skipped_inflight_cap > 0`, or `cowrie.verify_statepage_file` / HMAC error.
6. **Performance non-win (`FUNCTIONAL_PASS_NO_TTFT_SPEEDUP`):**
   - All correctness and counter checks pass, but `warm_cowrie_ttft_ms >= cold_ttft_ms` (restore is no faster than cold prefill on Colab local storage at the configured prompt length). Log the receipt honestly and investigate I/O bandwidth, host-to-device transfer path, or prompt length before retrying.
