/-
  Cowrie/AxiomAudit.lean — Metaprogram Auditing BOTH `CowrieSpec` Type Binding AND Axiom Footprints
  of All 40 Laws + 16 Semantic Refutations

  Uses:
    1. Kernel type binding `private def _bindCowrieSpec : Cowrie.Laws.CowrieSpec := Cowrie.Proofs.all_laws_proved`
       (guarantees `all_laws_proved` literally inhabits `Cowrie.Laws.CowrieSpec` — cannot be stubbed to `True`!).
    2. `Lean.collectAxioms` over `_bindCowrieSpec`, all 40 law theorems, and all 16 refutation theorems,
       asserting strictly constructive `{propext, Quot.sound}` only (ZERO `sorryAx`, ZERO `Classical.choice`,
       ZERO `Lean.ofReduceBool`, ZERO `Lean.trustCompiler`).
-/
import Lean
import Cowrie.Proofs
import Cowrie.Refutations

open Lean

namespace Cowrie.AxiomAudit

/-- Kernel-enforced witness binding `Cowrie.Proofs.all_laws_proved` to exact `Cowrie.Laws.CowrieSpec`. -/
private theorem _bindCowrieSpec : Cowrie.Laws.CowrieSpec :=
  Cowrie.Proofs.all_laws_proved

def allLawTheorems : List Name := [
  ``Cowrie.Proofs.L01_propagate_iff,
  ``Cowrie.Proofs.L02_taint_irrevocable_hops,
  ``Cowrie.Proofs.L03_sticky_reregister,
  ``Cowrie.Proofs.L04_unknown_parent_fails_closed,
  ``Cowrie.Proofs.L05_empty_args_fail_closed,
  ``Cowrie.Proofs.L06a_wrong_action_token_rejected,
  ``Cowrie.Proofs.L06b_partial_declass_blocks,
  ``Cowrie.Proofs.L07_fence_no_triple,
  ``Cowrie.Proofs.L08_fence_safe_identity,
  ``Cowrie.Proofs.L09_canon_roundtrip,
  ``Cowrie.Proofs.L10_verify_iff_decode,
  ``Cowrie.Proofs.L11_canon_wire_unique,
  ``Cowrie.Proofs.L12_zonemap_no_false_neg,
  ``Cowrie.Proofs.L13_merkle_domain_and_odd_safe,
  ``Cowrie.Proofs.L14_wal_torn_vs_mid_tamper,
  ``Cowrie.Proofs.L15_propagate_preserves_origin,
  ``Cowrie.Proofs.L16_sticky_deep_and_clean_liveness,
  ``Cowrie.Proofs.L17_gate_exact_iff_and_liveness,
  ``Cowrie.Proofs.L18_expired_and_forged_sig_rejected,
  ``Cowrie.Proofs.L19_nth_arg_taint_blocks,
  ``Cowrie.Proofs.L20_fence_preserves_all_non_esc_symbols,
  ``Cowrie.Proofs.L21_r2_structured_table_header_safe,
  ``Cowrie.Proofs.L22_map_keys_and_dict_all_used,
  ``Cowrie.Proofs.L23_sorted_zonemap_early_break_exact,
  ``Cowrie.Proofs.L24_wal_tombstone_reinsert_and_statepage_rt,
  ``Cowrie.Proofs.L25_ledger_record_exact_iff,
  ``Cowrie.Proofs.L26_header_meta_and_owner_pubkey,
  ``Cowrie.Proofs.L27_ed25519_action_code_and_mcp_classifier,
  ``Cowrie.Proofs.L28_r2_compact_and_virtual_atlas_fail_closed,
  ``Cowrie.Proofs.L29_r2_table_marker_injectivity,
  ``Cowrie.Proofs.L30_lens_tier_bounds_and_extract_field,
  ``Cowrie.Proofs.L31_canon_float64_bijective_and_idempotent,
  ``Cowrie.Proofs.L32_json_sentinel_bijectivity_and_fail_closed,
  ``Cowrie.Proofs.L33_resolved_frame_cid_repack_and_binding,
  ``Cowrie.Proofs.L34_merkle_leaf_and_tensor_sensitivity,
  ``Cowrie.Proofs.L35_live_ivf_compaction_crash_invariant,
  ``Cowrie.Proofs.L36_statepage_hmac_precedence,
  ``Cowrie.Proofs.L37_lean_u8_bitvec_and_i64_zigzag,
  ``Cowrie.Proofs.L38_lean_merkle_universal_injective,
  ``Cowrie.Proofs.L39_lean_fence_declarative_infix_and_idempotent,
  ``Cowrie.Proofs.all_laws_proved,
  ``Cowrie.AxiomAudit._bindCowrieSpec
]

def allRefutationTheorems : List Name := [
  ``Cowrie.Refutations.refute_M14_ignore_ext,
  ``Cowrie.Refutations.refute_M15_head_only_anyTainted,
  ``Cowrie.Refutations.refute_M16_noop_deriveChain,
  ``Cowrie.Refutations.refute_M19_token_ignores_cid,
  ``Cowrie.Refutations.refute_M20_F9_multicell_chatml_breakout,
  ``Cowrie.Refutations.refute_M28_F9_hidden_cell_marker_spoof,
  ``Cowrie.Refutations.refute_M21_r2_compact_allows_synth,
  ``Cowrie.Refutations.refute_M22_merkle_const_empty,
  ``Cowrie.Refutations.refute_M23_merkle_cve_2012_2459,
  ``Cowrie.Refutations.refute_M24_wal_opframe_no_dedup,
  ``Cowrie.Refutations.refute_M25_F10_live_ivf_watermark_ignored,
  ``Cowrie.Refutations.refute_M26_F13_statepage_hmac_flag_precedence,
  ``Cowrie.Refutations.refute_M27_missing_owner_pubkey,
  ``Cowrie.Refutations.refute_L23_unsorted_zonemap_needs_disjoint_precondition,
  ``Cowrie.Refutations.refute_M32_mcp_policy_override_bypass,
  ``Cowrie.Refutations.refute_M33_wal_rollback_seq_le_replay
]

def allowedAxioms : List Name := [`propext, `Quot.sound]

#eval show MetaM Unit from do
  let mut unionAxioms : List Name := []
  for decl in allLawTheorems ++ allRefutationTheorems do
    let axioms ← Lean.collectAxioms decl
    for ax in axioms do
      if !allowedAxioms.contains ax then
        throwError m!"FORBIDDEN AXIOM '{ax}' used in theorem '{decl}'! Allowed constructive set: {allowedAxioms}"
      if !unionAxioms.contains ax then
        unionAxioms := unionAxioms ++ [ax]
  IO.println s!"AXIOM_AUDIT_PASS laws={allLawTheorems.length - 2} refutations={allRefutationTheorems.length} axioms={unionAxioms}"

end Cowrie.AxiomAudit
