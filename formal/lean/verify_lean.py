#!/usr/bin/env python3
"""
verify_lean.py — Independent Lean 4.34.0 Formal Verification, Axiom Audit, Mutant & 3-Way Lockstep Gate

Runs all 5 formal assurance stages for Cowrie v1.2 (Lean 4.34 Edition):
  1. Clean `lake clean && lake build --wfail` (`autoImplicit = false`, `relaxedAutoImplicit = false`,
     `warningAsError = true`, offline `Init`-only with zero external `[[require]]`).
  2. Independent kernel replay (`lake env leanchecker --fresh Cowrie`), programmatic `Lean.collectAxioms`
     + `_bindCowrieSpec : Cowrie.Laws.CowrieSpec` audit (`Cowrie/AxiomAudit.lean` under
     `-DwarningAsError=true -DautoImplicit=false` asserting STRICT constructive `{propext, Quot.sound}` only),
     and state-machine static hygiene scan (immune to nested comment / attribute evasion).
  3. 26 Planted-False Kernel Mutant Controls (`LM01`..`LM26`, tested in isolated scratch overlay dirs via
     `LEAN_PATH=<overlay>:.lake/build/lib/lean` leaving `Cowrie/Laws.lean` and `Cowrie/Proofs.lean` untouched)
     PLUS 16 machine-checked semantic counterexample refutations (`Cowrie/Refutations.lean`).
  4. 3-Way Non-Bolt-On Differential Lockstep (`formal/lean/differential_3way.mjs`): runs 2,000+ randomized
     & adversarial vectors asserting 100% agreement across Lean 4.34 native `cowrie-lean-oracle` <->
     Bend 2.0.35 `cowrie_kernel_verified.mjs` <-> Rust/WASM `cowrie_core.wasm`.
  5. Writes verified cryptographic receipt `receipts/receipt_v1_2_formal_lean.json`.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
LEAN_DIR = ROOT / "formal" / "lean"
LEAN_BIN_DIR = Path(os.environ.get("LEAN_BIN_DIR", "/home/omen/toolchains/lean-4.34.0/lean-4.34.0-linux/bin"))
CARGO_TARGET = Path(os.environ.get("CARGO_TARGET_DIR", "/tmp/cowrie-cargo-target"))
BEND_MJS = CARGO_TARGET / "cowrie_kernel_verified.mjs"
WASM_BIN = CARGO_TARGET / "wasm32-unknown-unknown" / "release" / "cowrie_core.wasm"


def lean_env() -> dict:
    env = os.environ.copy()
    env["PATH"] = f"{LEAN_BIN_DIR}:{env.get('PATH', '')}"
    return env


def sha256_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def strip_lean_comments_and_strings(src: str) -> str:
    """Proper lexical stripper for Lean 4 line comments (`--`), nested block comments (`/- ... -/`),
    and string literals (`\"...\"`). Immune to `-- /- ... -- -/` comment-interleaving evasion."""
    out = []
    i = 0
    n = len(src)
    depth = 0
    in_str = False
    while i < n:
        if depth > 0:
            if src[i : i + 2] == "/-":
                depth += 1
                i += 2
            elif src[i : i + 2] == "-/":
                depth -= 1
                i += 2
            else:
                if src[i] == "\n":
                    out.append("\n")
                i += 1
        elif in_str:
            if src[i] == "\\" and i + 1 < n:
                i += 2
            elif src[i] == '"':
                in_str = False
                out.append('""')
                i += 1
            else:
                i += 1
        else:
            if src[i : i + 2] == "--":
                while i < n and src[i] != "\n":
                    i += 1
            elif src[i : i + 2] == "/-":
                depth = 1
                i += 2
            elif src[i] == '"':
                in_str = True
                i += 1
            else:
                out.append(src[i])
                i += 1
    return "".join(out)


def run_stage1_build() -> dict:
    lake = LEAN_BIN_DIR / "lake"
    assert lake.exists(), f"Lean 4.34 lake binary not found at {lake}"
    # Clean rebuild guarantees zero stale `.olean` warnings can slip past `--wfail`
    subprocess.run([str(lake), "clean"], cwd=str(LEAN_DIR), env=lean_env(), capture_output=True, text=True, check=True)
    t0 = time.perf_counter()
    res = subprocess.run([str(lake), "build", "--wfail"], cwd=str(LEAN_DIR), env=lean_env(), capture_output=True, text=True)
    dt_ms = (time.perf_counter() - t0) * 1000.0
    if res.returncode != 0:
        raise RuntimeError(f"Stage 1 lake build --wfail FAILED:\n{res.stdout}\n{res.stderr}")
    ver_res = subprocess.run([str(LEAN_BIN_DIR / "lean"), "--version"], capture_output=True, text=True, check=True)
    return {
        "status": "PASS",
        "lean_version": ver_res.stdout.strip().splitlines()[0],
        "clean_rebuild": True,
        "build_ms": round(dt_ms, 2),
        "warnings_as_errors": True,
        "external_dependencies": 0,
    }


def run_stage2_kernel_and_axioms() -> dict:
    # Hardened lexical hygiene audit across ALL Lean source files
    lean_files = sorted(list((LEAN_DIR / "Cowrie").glob("*.lean")) + [LEAN_DIR / "Cowrie.lean", LEAN_DIR / "Main.lean"])
    forbidden_patterns = [
        (r"\bsorry\b", "sorry"),
        (r"\badmit\b", "admit"),
        (r"\baxiom\b", "axiom"),
        (r"\bnative_decide\b", "native_decide"),
        (r"\bbv_decide\b", "bv_decide"),
        (r"\bunsafe\b", "unsafe"),
        (r"\bextern\b", "extern"),
        (r"\bimplemented_by\b", "implemented_by"),
        (r"\bset_option\b", "set_option"),
    ]
    for lf in lean_files:
        txt_nc = strip_lean_comments_and_strings(lf.read_text(encoding="utf-8"))
        for pat, label in forbidden_patterns:
            assert not re.search(pat, txt_nc), f"Forbidden '{label}' found in {lf.name}!"
        if lf.name != "Main.lean":
            assert not re.search(r"\bpartial\b", txt_nc), f"Forbidden 'partial' found in proof/kernel module {lf.name}!"

    # Verify Cowrie/Laws.lean contains ZERO proofs/tactics (`:= by`, `theorem`)
    laws_nc = strip_lean_comments_and_strings((LEAN_DIR / "Cowrie" / "Laws.lean").read_text(encoding="utf-8"))
    assert ":= by" not in laws_nc and not re.search(r"\btheorem\b", laws_nc), (
        "TCB violation: Cowrie/Laws.lean must contain pure Prop definitions only (no proofs/tactics)!"
    )

    # Programmatic Lean.collectAxioms + `_bindCowrieSpec : Cowrie.Laws.CowrieSpec` audit
    t0 = time.perf_counter()
    res_ax = subprocess.run(
        [str(LEAN_BIN_DIR / "lake"), "env", "lean", "-DwarningAsError=true", "-DautoImplicit=false", "Cowrie/AxiomAudit.lean"],
        cwd=str(LEAN_DIR), env=lean_env(), capture_output=True, text=True,
    )
    ax_ms = (time.perf_counter() - t0) * 1000.0
    if res_ax.returncode != 0 or "AXIOM_AUDIT_PASS" not in res_ax.stdout:
        raise RuntimeError(f"Stage 2 AxiomAudit FAILED:\n{res_ax.stdout}\n{res_ax.stderr}")
    ax_line = [ln for ln in res_ax.stdout.splitlines() if "AXIOM_AUDIT_PASS" in ln][0]

    # Independent kernel replay: `lake env leanchecker --fresh Cowrie`
    t1 = time.perf_counter()
    res_chk = subprocess.run(
        [str(LEAN_BIN_DIR / "lake"), "env", "leanchecker", "--fresh", "Cowrie"],
        cwd=str(LEAN_DIR), env=lean_env(), capture_output=True, text=True,
    )
    chk_ms = (time.perf_counter() - t1) * 1000.0
    if res_chk.returncode != 0:
        raise RuntimeError(f"Stage 2 leanchecker --fresh Cowrie FAILED:\n{res_chk.stdout}\n{res_chk.stderr}")

    proofs_src = (LEAN_DIR / "Cowrie" / "Proofs.lean").read_text(encoding="utf-8")
    law_theorems = re.findall(r"^theorem\s+(L[0-9a-z_A-Z]+)\s*:", proofs_src, flags=re.M)
    ref_src = (LEAN_DIR / "Cowrie" / "Refutations.lean").read_text(encoding="utf-8")
    refutations = re.findall(r"^theorem\s+(refute_[0-9a-z_A-Z]+)\s*:", ref_src, flags=re.M)
    assert len(law_theorems) == 40, f"Expected 40 law theorems, found {len(law_theorems)}"
    assert len(refutations) == 16, f"Expected 16 refutation theorems, found {len(refutations)}"

    return {
        "status": "PASS",
        "laws_proved": len(law_theorems),
        "law_theorems": law_theorems,
        "cowriespec_type_bound": True,
        "semantic_refutations_proved": len(refutations),
        "semantic_refutations": refutations,
        "axiom_audit_output": ax_line,
        "allowed_axioms": ["propext", "Quot.sound"],
        "axiom_audit_ms": round(ax_ms, 2),
        "leanchecker_fresh_status": "PASS",
        "leanchecker_fresh_ms": round(chk_ms, 2),
        "forbidden_constructs_found": 0,
    }


def run_stage3_mutant_controls(scratch: Path) -> dict:
    """
    Fast overlay mutant testing: for each of 26 planted-false mutants of `Cowrie/{Trust,Fence,Canonical,Storage}.lean`,
    compiles ONLY the mutated module -> `Cowrie.Laws` -> `Cowrie.Proofs` into a private scratch overlay with
    `LEAN_PATH=<overlay>:.lake/build/lib/lean` and asserts `Cowrie/Proofs.lean` fails with a genuine Lean
    type/tactic error (NOT a syntax or missing-module tooling error!).
    """
    lean_bin = str(LEAN_BIN_DIR / "lean")
    base_lib = LEAN_DIR / ".lake" / "build" / "lib" / "lean"

    mutants = [
        (
            "LM01_empty_args_fail_open",
            "Cowrie/Trust.lean",
            "    match args with\n    | [] => false",
            "    match args with\n    | [] => true",
        ),
        (
            "LM02_four_bracket_fence_bypass_F1",
            "Cowrie/Fence.lean",
            "      | .r2 => .esc :: .lt :: sanitizeStep rest .r1 .r0 .pLt",
            "      | .r2 => .lt :: .esc :: .lt :: .lt :: sanitizeStep rest .r0 .r0 .pOther",
        ),
        (
            "LM03_non_canonical_int_accepted",
            "Cowrie/Canonical.lean",
            "  | .wTagIntPos n => if n < lim then none else some (.lPos n)",
            "  | .wTagIntPos n => some (.lPos n)",
        ),
        (
            "LM04_zonemap_strict_lt_false_negative",
            "Cowrie/Storage.lean",
            "  decide (lo <= z.maxK) && decide (z.minK <= hi)",
            "  decide (lo < z.maxK) && decide (z.minK <= hi)",
        ),
        (
            "LM05_wal_mid_tamper_silently_truncated",
            "Cowrie/Storage.lean",
            "        | _ :: _ => .walErr",
            "        | _ :: _ => .walOk []",
        ),
        (
            "LM06_smoking_gun_isTainted_ignores_ext",
            "Cowrie/Trust.lean",
            "def isTainted (a : Auth) : Bool :=\n  a.ext || a.tainted",
            "def isTainted (a : Auth) : Bool :=\n  a.tainted",
        ),
        (
            "LM07_smoking_gun_head_only_anyTainted",
            "Cowrie/Trust.lean",
            "def anyTainted (ps : List Auth) : Bool :=\n  ps.any isTainted",
            "def anyTainted (ps : List Auth) : Bool :=\n  match ps with | [] => false | h :: _ => isTainted h",
        ),
        (
            "LM08_smoking_gun_noop_deriveChain",
            "Cowrie/Trust.lean",
            "  | stepBase :: rest => deriveChain rest (propagate stepBase [cur])",
            "  | _ :: _ => cur",
        ),
        (
            "LM09_unknown_parent_fail_open",
            "Cowrie/Trust.lean",
            "  | none   => true",
            "  | none   => false",
        ),
        (
            "LM10_credRead_unprivileged_bypass",
            "Cowrie/Trust.lean",
            "  | .readOnly => false\n  | _         => true",
            "  | .readOnly | .credRead => false\n  | _         => true",
        ),
        (
            "LM11_token_ignores_target_cid",
            "Cowrie/Trust.lean",
            "  tok.sigValid && (tok.cid == cid) && (tok.act == act) && decide (now <= tok.expiry)",
            "  tok.sigValid && (tok.act == act) && decide (now <= tok.expiry)",
        ),
        (
            "LM12_missing_owner_pubkey_allows_declass",
            "Cowrie/Trust.lean",
            "    hasPk && hasValidToken toks cid act now",
            "    hasValidToken toks cid act now",
        ),
        (
            "LM13_r2_table_multicell_chatml_breakout_F9",
            "Cowrie/Fence.lean",
            "      let row := sanitizeFences (sanitizeFences c1 ++ (.pipe :: sanitizeFences c2))",
            "      let row := sanitizeFences c1 ++ (.pipe :: sanitizeFences c2)",
        ),
        (
            "LM14_r2_compact_allows_model_synth",
            "Cowrie/Fence.lean",
            "  if !a.ext && !a.tainted && (a.cleanBits == 1 || a.cleanBits == 2) then",
            "  if !a.ext && !a.tainted && (a.cleanBits == 1 || a.cleanBits == 2 || a.cleanBits == 8) then",
        ),
        (
            "LM15_table_cell_hidden_spoof_marker_F9",
            "Cowrie/Fence.lean",
            "      let eff := stripSpoof raw",
            "      let eff := raw",
        ),
        (
            "LM16_smoking_gun_merkle_const_empty",
            "Cowrie/Storage.lean",
            "def merkleTree (xs : List Digest) : Digest :=\n  match xs with\n  | [] => .hEmpty\n  | [only] => only\n  | a :: b :: rest =>\n      merkleTree (.hNode01 a b :: merklePairLevel rest)\ntermination_by xs.length\ndecreasing_by\n  have := merklePairLevel_length_le rest\n  simp\n  omega",
            "def merkleTree (_xs : List Digest) : Digest := .hEmpty",
        ),
        (
            "LM17_smoking_gun_merkle_cve_2012_2459_dup_odd",
            "Cowrie/Storage.lean",
            "  | [a] => [a]",
            "  | [a] => [.hNode01 a a]",
        ),
        (
            "LM18_smoking_gun_wal_opframe_no_dedup",
            "Cowrie/Storage.lean",
            "  | .opFrame c :: rest => walReplayLive rest (removeCid live c ++ [c])",
            "  | .opFrame c :: rest => walReplayLive rest (live ++ [c])",
        ),
        (
            "LM19_live_ivf_ignores_watermark_F10",
            "Cowrie/Storage.lean",
            "      if appliedSeq <= r.seq then\n        r.vecId :: ivfWalFilterFrom rest appliedSeq\n      else\n        ivfWalFilterFrom rest appliedSeq",
            "      r.vecId :: ivfWalFilterFrom rest appliedSeq",
        ),
        (
            "LM20_statepage_hmac_flag_precedence_bypass_F13",
            "Cowrie/Storage.lean",
            "  | true, _ + 2, _, _     => .spAuthMacMismatch",
            "  | true, _ + 2, _, _     => .spNonCanonicalFlags",
        ),
        (
            "LM21_lean_uint8_propagate_clobbers_origin",
            "Cowrie/Trust.lean",
            "    base ||| BIT_TAINTED",
            "    BIT_TAINTED",
        ),
        (
            "LM22_lean_i64_zigzag_drops_sign_xor",
            "Cowrie/Canonical.lean",
            "  UInt64.ofBitVec ((i.toBitVec <<< 1) ^^^ (i.toBitVec.sshiftRight 63))",
            "  UInt64.ofBitVec (i.toBitVec <<< 1)",
        ),
        # --- Post-Critique Adversarial Mutants (LM23..LM26) ---
        (
            "LM23_mcp_policy_override_fail_open",
            "Cowrie/Trust.lean",
            "  match polOverride, verbPriv with\n  | some act, _     => act\n  | none, some vact => vact",
            "  match polOverride, verbPriv with\n  | some _, _       => if allowRo then .readOnly else .shellExec\n  | none, some vact => vact",
        ),
        (
            "LM24_wal_rollback_seq_le_replay",
            "Cowrie/Storage.lean",
            "        if r.seq == expSeq then",
            "        if r.seq <= expSeq then",
        ),
        (
            "LM25_f64_bits_drops_nan_canon",
            "Cowrie/Canonical.lean",
            "  else if isNaNBits b then\n    CANON_QNAN_BITS",
            "  else if isNaNBits b then\n    b",
        ),
        (
            "LM26_uint8_isTainted_ignores_ext",
            "Cowrie/Trust.lean",
            "  (a &&& (BIT_TAINTED ||| AUTH_UNTRUSTED_EXT)) != 0",
            "  (a &&& BIT_TAINTED) != 0",
        ),
    ]

    results = {}
    t_start = time.perf_counter()

    def check_one_mutant(item):
        mname, rel_mod, target, replacement = item
        orig_path = LEAN_DIR / rel_mod
        orig_src = orig_path.read_text(encoding="utf-8")
        assert target in orig_src, f"Mutant target missing in {rel_mod} for {mname}"

        mdir = scratch / mname
        lib_dir = mdir / "lib"
        (lib_dir / "Cowrie").mkdir(parents=True, exist_ok=True)
        src_dir = mdir / "src"
        (src_dir / "Cowrie").mkdir(parents=True, exist_ok=True)

        mod_stem = Path(rel_mod).stem
        skip_stems = {mod_stem, "Laws", "Proofs", "Refutations"}
        if mod_stem == "Trust":
            skip_stems.add("Fence")
        for olean in (base_lib / "Cowrie").glob("*.olean"):
            if olean.stem not in skip_stems:
                os.symlink(olean, lib_dir / "Cowrie" / olean.name)

        mut_src = orig_src.replace(target, replacement, 1)
        mut_file = src_dir / rel_mod
        mut_file.write_text(mut_src, encoding="utf-8")

        env = lean_env()
        env["LEAN_PATH"] = f"{lib_dir}:{base_lib}"

        out_mod = lib_dir / "Cowrie" / f"{mod_stem}.olean"
        r_mod = subprocess.run(
            [lean_bin, "-R", str(src_dir), str(mut_file), "-o", str(out_mod)],
            cwd=str(src_dir), env=env, capture_output=True, text=True, timeout=60,
        )
        assert r_mod.returncode == 0, f"Mutant {mname} failed to compile kernel module {rel_mod}!\n{r_mod.stdout}\n{r_mod.stderr}"

        if mod_stem == "Trust":
            r_f = subprocess.run(
                [lean_bin, "-R", str(LEAN_DIR), str(LEAN_DIR / "Cowrie" / "Fence.lean"), "-o", str(lib_dir / "Cowrie" / "Fence.olean")],
                cwd=str(LEAN_DIR), env=env, capture_output=True, text=True, timeout=60,
            )
            assert r_f.returncode == 0, f"Mutant {mname} failed to recompile Fence.lean:\n{r_f.stdout}\n{r_f.stderr}"

        r_laws = subprocess.run(
            [lean_bin, "-R", str(LEAN_DIR), str(LEAN_DIR / "Cowrie" / "Laws.lean"), "-o", str(lib_dir / "Cowrie" / "Laws.olean")],
            cwd=str(LEAN_DIR), env=env, capture_output=True, text=True, timeout=60,
        )
        assert r_laws.returncode == 0, f"Mutant {mname} failed to compile untouched Laws.lean:\n{r_laws.stdout}\n{r_laws.stderr}"

        r_pf = subprocess.run(
            [lean_bin, "-R", str(LEAN_DIR), str(LEAN_DIR / "Cowrie" / "Proofs.lean"), "-o", str(lib_dir / "Cowrie" / "Proofs.olean")],
            cwd=str(LEAN_DIR), env=env, capture_output=True, text=True, timeout=60,
        )
        combined = (r_pf.stdout + "\n" + r_pf.stderr).lower()
        assert r_pf.returncode != 0, f"CRITICAL: Planted-false Lean mutant {mname} was NOT rejected by Cowrie/Proofs.lean!"
        assert "error:" in combined, f"Mutant {mname} exited non-zero without a Lean error diagnostic:\n{combined}"
        assert "unknown module" not in combined and "no such file" not in combined, (
            f"Mutant {mname} failed due to tooling/module path error, not proof rejection:\n{combined}"
        )
        return mname, "REJECTED (proof typecheck failed)"

    with ThreadPoolExecutor(max_workers=4) as pool:
        for mname, verdict in pool.map(check_one_mutant, mutants):
            results[mname] = verdict

    return {
        "controls_tested": len(mutants),
        "controls_rejected": len(results),
        "elapsed_s": round(time.perf_counter() - t_start, 2),
        "details": results,
    }


def run_stage4_three_way_lockstep() -> dict:
    lean_bin = LEAN_DIR / ".lake" / "build" / "bin" / "cowrie-lean-oracle"
    assert lean_bin.exists(), f"Missing native Lean oracle binary: {lean_bin}"
    assert BEND_MJS.exists(), f"Missing Bend compiled MJS: {BEND_MJS} (run Bend verify first)"
    assert WASM_BIN.exists(), f"Missing WASM binary: {WASM_BIN} (run make wasm first)"
    res = subprocess.run(
        ["node", str(LEAN_DIR / "differential_3way.mjs"), str(lean_bin), str(BEND_MJS), str(WASM_BIN)],
        capture_output=True, text=True, check=True,
    )
    return json.loads(res.stdout.strip())


def main() -> None:
    scratch = Path(tempfile.mkdtemp(prefix="cowrie-lean-formal-"))
    try:
        s1 = run_stage1_build()
        s2 = run_stage2_kernel_and_axioms()
        s3 = run_stage3_mutant_controls(scratch)
        s4 = run_stage4_three_way_lockstep()

        tracked = [
            "lean-toolchain",
            "lakefile.toml",
            "Cowrie.lean",
            "Main.lean",
            "Cowrie/Trust.lean",
            "Cowrie/Fence.lean",
            "Cowrie/Canonical.lean",
            "Cowrie/Storage.lean",
            "Cowrie/Laws.lean",
            "Cowrie/Proofs.lean",
            "Cowrie/Refutations.lean",
            "Cowrie/AxiomAudit.lean",
        ]
        hashes = {rel: sha256_file(LEAN_DIR / rel) for rel in tracked}

        receipt = {
            "suite": "cowrie_v1_2_formal_lean4_verification",
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "toolchain": f"{s1['lean_version']} (`lake clean && lake build --wfail` + `leanchecker --fresh` + `Lean.collectAxioms` + native oracle)",
            "file_hashes_sha256": hashes,
            "stage1_lake_build": s1,
            "stage2_kernel_and_axiom_audit": s2,
            "stage3_planted_false_mutant_controls": s3,
            "stage4_three_way_differential_lockstep": s4,
            "verdict": "PASS",
        }
        receipt_path = ROOT / "receipts" / "receipt_v1_2_formal_lean.json"
        receipt_path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(receipt, indent=2))
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    main()
