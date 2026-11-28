# CLOUD-LANE-01 (`EXP-04`): Live `vLLM 0.15+` + `CowrieKVConnectorV1` Colab Runbook

**Notebook:** [`notebooks/EXP-04-cowrie-kv-connector.ipynb`](../notebooks/EXP-04-cowrie-kv-connector.ipynb)  
**Adapter:** [`adapters/vllm/cowrie_kv_connector.py`](../adapters/vllm/cowrie_kv_connector.py)  
**Audit gap addressed:** [`reports/cowrie-latest-claims-audit-2026-11-28.md`](../reports/cowrie-latest-claims-audit-2026-11-28.md) §4 (vLLM 0.15+ `vllm.general_plugins` auto-discovery, live `SupportsHMA` runtime verification, and cross-restart KV restore on datacenter HBM3 / Blackwell hardware at 32B–70B scale).

---

## 1. Credit Pool & Zero-Paid-Spend Guardrails

| Item | Specification |
|---|---|
| **Paying credit pool** | **Google AI Ultra — Google Colab Compute Units** (`2,000 CU/mo`, 90-day rollover up to `6,000 CU`). |
| **Paid GCP / API spend** | **`$0.00`** — does **not** use the `$100/mo` Ultra GCP/API credit pool, Cloud Run, Compute Engine, or external GPU rentals. |
| **External credentials required** | **None** for default profiles (`Qwen/Qwen3-32B`, `Qwen/Qwen3-32B-FP8`, `Qwen/Qwen2.5-72B-Instruct-AWQ` are ungated on Hugging Face; `COWRIE_HMAC_KEY` is generated ephemerally per run). Only the optional `70b-fp8` (`nvidia/Llama-3.3-70B-Instruct-FP8`) profile may require `HF_TOKEN`. |
| **Idle-CU stop rule** | Call `google.colab.runtime.unassign()` (or **Runtime → Disconnect and delete runtime**) immediately after downloading `/tmp/cowrie_exp04_receipt.json`. |

---

## 2. Hardware Profiles, Expected Runtime & Cost in Colab Compute Units

Rates below come from the verified late-2026 Google AI Ultra resource audit (`2026-11-ultra-aeo-monetization-plan.md`). All runtimes and CU totals are **pre-execution estimates** (actual run wall-clock is **not measured** until executed in Colab).

| Colab Ultra Profile | GPU / Arch / VRAM | Burn Rate (Est.) | Model Profiles Supported | Est. Wall Time (Fresh VM) | Est. Cost per Run (CU) | Share of 2,000 CU/mo Pool |
|---|---|---:|---|---:|---:|---:|
| **Primary** | NVIDIA **H100 80GB** HBM3 (`sm_90`) | `~18.0 CU/hr` | `qwen3-32b` (default, BF16 ~64 GB), `qwen3-32b-fp8`, `70b-awq` (~41 GB), `70b-fp8` (~71 GB) | `10 – 20 min` (`12 – 25 min` on 70B) | **`3.0 – 6.0 CU`** (`3.6 – 7.5 CU` on 70B) | `0.15% – 0.38%` |
| **Blackwell Variant** | NVIDIA **RTX PRO 6000 Blackwell 96GB** (`sm_120`, G4) | `~8.9 CU/hr` | `qwen3-32b` (default, BF16 ~64 GB + ~28 GB KV headroom), `qwen3-32b-fp8`, `70b-awq`, `70b-fp8` | `10 – 20 min` (`12 – 25 min` on 70B) | **`1.5 – 3.0 CU`** (`1.8 – 3.7 CU` on 70B) | `0.08% – 0.19%` |

### Estimated Phase Breakdown (Fresh Colab VM, `qwen3-32b`)
1. `pip install vllm>=0.15.0` + minimal `rustup` + `cowrie-ai` Rust release build: **~3–6 min** (0–1 min on repeat runs in the same VM).
2. Hugging Face weight download (`Qwen/Qwen3-32B` ~64 GB or `Qwen/Qwen2.5-72B-Instruct-AWQ` ~41 GB): **~4–10 min** (0 min once cached in `~/.cache/huggingface`).
3. `SupportsHMA` factory verification + Phase 1 (Cold Store subprocess) + full VRAM teardown + Phase 2 (Warm Restore subprocess): **~2–4 min**.

> **KV Layout Note (Hopper vs Blackwell):** `CowrieKVConnectorV1` supports both FlashAttention `(2, num_blocks, block_size, n_kv, head_dim)` (`2_blocks_first`) and FlashInfer `(num_blocks, 2, block_size, n_kv, head_dim)` (`blocks_2_first`), recording the observed runtime layout in `kv_layouts` and failing closed on unrecognized shapes.

---

## 3. Exact Run Steps

### Prerequisites
- Branch `feat/cloud-lane-01` pushed to `https://github.com/Neumenon/cowrie` (or merged into `main`). If running before pushing, upload the repo checkout to `/content/cowrie` in Colab — Cell 5 automatically detects `/content/cowrie` and skips `git clone`.

### Step-by-Step (Colab Web UI or Official Colab VS Code Extension)
1. Open [`notebooks/EXP-04-cowrie-kv-connector.ipynb`](../notebooks/EXP-04-cowrie-kv-connector.ipynb) in Google Colab (signed into the Google AI Ultra account).
2. Select runtime: **Runtime → Change runtime type → Hardware accelerator: H100** (or **RTX PRO 6000 Blackwell / G4** for the Blackwell variant).
3. *(Optional model switch — zero edits needed for default `Qwen/Qwen3-32B`):*
   - For the **70B switch on H100 80GB** (ungated), set `MODEL_PROFILE = "70b-awq"` (or `export EXP04_MODEL_PROFILE=70b-awq` before running).
   - For native Hopper FP8 70B, set `MODEL_PROFILE = "70b-fp8"` (and set `HF_TOKEN` if Hugging Face gates access).
   - For `torch.compile` + CUDA graph decode timing instead of eager mode, set `EXP04_ENFORCE_EAGER=0`.
   - For unattended runs, set `AUTO_UNASSIGN_COLAB_RUNTIME = True` (or `EXP04_AUTO_UNASSIGN=1`) in Cell 1.
4. Click **Runtime → Run all**. Do not skip cells.
5. Verify the printed summary table in Cell 11 (`VERDICT: PASS`) and confirm zero fail-loud exceptions in Cell 10.
6. Download `/tmp/cowrie_exp04_receipt.json` and save it into this repo as `receipts/receipt_exp04_<gpu>_<model>_<YYYY-MM-DD>.json`.
7. **Immediately stop CU billing:** run Cell 13 with `AUTO_UNASSIGN_COLAB_RUNTIME = True` or click **Runtime → Disconnect and delete runtime**.

---

## 4. Current Measurement Status (Pre-Execution)

Local workstation checks verified the Rust core, WASM adversarial suite, connector unit tests (`tests/test_vllm_kv_connector_unit.py`), and notebook dry-parse/non-GPU cells. **No Colab GPU run has been executed yet:**

| Metric | Local Workstation | Colab H100 80GB (`qwen3-32b`) | Colab H100 80GB (`70b-awq`) | Colab RTX PRO 6000 Blackwell (`qwen3-32b`) |
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

## 5. Success Criteria (Preregistered Pass Gates)

An `EXP-04` run counts as **SUCCESS (`PASS`)** only if **all** of the following hold on live GPU silicon and are recorded in `/tmp/cowrie_exp04_receipt.json`:

1. **Upstream version & registration:** `vllm.__version__ >= 0.15.0` and `KVConnectorFactory.get_connector_class(...)` resolves `cowrie_vllm_adapters.cowrie_kv_connector.CowrieKVConnectorV1` (with `auto_registered_via_entry_point` recorded transparently).
2. **`SupportsHMA` proven `True` at runtime:**
   - `hma_proof.supports_hma_native_import == True` (imported from `vllm.distributed.kv_transfer.kv_connector.v1.base.SupportsHMA`, never the fallback stub),
   - `hma_proof.issubclass_supports_hma == True`,
   - `hma_proof.has_request_finished_all_groups == True`,
   - Both Phase 1 and Phase 2 live connector counter snapshots report `all_supports_hma == True`.
3. **Phase 1 (Cold Store) persistence & integrity:**
   - `saves_completed >= 3`, `saved_tokens > 0`, `saved_bytes > 0`, `save_kv_layer_calls > 0`,
   - `saves_failed == 0` and `saves_skipped_inflight_cap == 0`,
   - All `>= 3` `.state.cowrie` shards pass independent `cowrie.verify_statepage_file(..., verify_hash=True)` with `hmac_sha256_verified == True`.
4. **Phase 2 (Warm Restore across full process restart):**
   - `scheduler_hits >= 3`, `loads_completed >= 3`, `start_load_kv_calls > 0`, `loaded_tokens > 0`, `loaded_bytes > 0`, `loads_failed == 0`,
   - Disk-loaded KV cache hit rate `>= 85%` across the 3 prompts (2 identical + 1 brand-new suffix sharing the saved prefix),
   - **100% exact greedy token parity** (`decode_token_ids` identical between Process 1 and Process 2 over all 64 generated tokens).
5. **Latency win:** `warm_cowrie_ttft_ms < cold_ttft_ms` (`ttft_speedup_x > 1.0`, target `>= 1.5×` at `PROMPT_ROWS=120` / ~2.5K–3K prompt tokens; larger prompts increase prefill-vs-restore ratio).

---

## 6. Failure Criteria (What Counts as a Failed Experiment)

An `EXP-04` run counts as **FAILED** (and must be diagnosed before making any compatibility or speedup claim) if **any** of the following occur:

1. **Silent bypass (caught automatically by `assert_connector_was_used`):**
   - `instance_count == 0`, `save_kv_layer_calls == 0`, `saves_completed < 3`, `scheduler_hits < 3`, `loads_completed < 3`, or `loaded_tokens == 0`.
2. **`SupportsHMA` failure or stub fallback:**
   - `SupportsHMA` cannot be imported from `vllm.distributed.kv_transfer.kv_connector.v1.base`, `issubclass(CowrieKVConnectorV1, SupportsHMA)` is `False`, or vLLM rejects the connector / disables HMA on startup.
3. **Greedy token parity mismatch:**
   - `store_res["decode_token_ids"] != restore_res["decode_token_ids"]` under `temperature=0.0` (indicates KV cache axis/layout corruption, slot-mapping bug, or dtype mismatch).
4. **Storage / cryptographic integrity failure:**
   - Any `loads_failed > 0`, `saves_failed > 0`, `saves_skipped_inflight_cap > 0`, or `cowrie.verify_statepage_file` / HMAC error.
5. **Performance non-win (`FUNCTIONAL_PASS_NO_TTFT_SPEEDUP`):**
   - All correctness and counter checks pass, but `warm_cowrie_ttft_ms >= cold_ttft_ms` (restore is no faster than cold prefill on Colab local storage at the configured prompt length). This is a **falsified speedup hypothesis** — log the receipt honestly and investigate I/O bandwidth, host-to-device copy path, or prompt length before retrying.
6. **Hardware fit / runtime crash:**
   - CUDA OOM, unsupported `sm_120` kernel crash, or engine hang on the target H100 80GB / Blackwell 96GB profile.
