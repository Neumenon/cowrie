/-
  Cowrie/Proofs.lean — Constructive Machine-Checked Proofs of All 40 Cowrie v1.2 Laws

  Zero `sorry`, zero `admit`, zero `axiom`, zero `native_decide`, zero `bv_decide`.
  Targets constructive axiom footprint `{propext, Quot.sound}` only (verified by `AxiomAudit`
  and independent `leanchecker --fresh Cowrie`).
-/
import Cowrie.Laws

namespace Cowrie.Proofs

open Cowrie.Trust
open Cowrie.Fence
open Cowrie.Canonical
open Cowrie.Storage
open Cowrie.Laws

-- =============================================================================
-- ZERO-AXIOM CONSTRUCTIVE BEQ HELPERS (AVOIDS `LawfulBEq` -> `Classical.choice`)
-- =============================================================================

private theorem nat_beq_refl (n : Nat) : (n == n) = true :=
  decide_eq_true rfl

private theorem nat_eq_of_beq {a b : Nat} (h : (a == b) = true) : a = b :=
  of_decide_eq_true h

private theorem action_beq_refl (a : Action) : (a == a) = true := by
  cases a <;> rfl

private theorem action_eq_of_beq {a b : Action} (h : (a == b) = true) : a = b := by
  cases a <;> cases b <;> first | rfl | cases h

private theorem uint64_eq_of_beq {a b : UInt64} (h : (a == b) = true) : a = b :=
  of_decide_eq_true h

private theorem uint64_beq_of_eq {a b : UInt64} (h : a = b) : (a == b) = true :=
  decide_eq_true h

-- =============================================================================
-- GROUP 1 PROOFS: PLANE D TRUST, STICKY LEDGER, GATE, HEADER META & MCP
-- =============================================================================

private theorem isTainted_eq_spec (a : Auth) : isTainted a = specTainted a := rfl

private theorem anyTainted_eq_spec (ps : List Auth) : anyTainted ps = specAnyTainted ps := by
  induction ps with
  | nil => rfl
  | cons h t ih =>
      simp only [anyTainted, List.any_cons, specAnyTainted, isTainted_eq_spec]
      rw [← anyTainted, ih]

private theorem ledgerLookup_eq_spec (lg : List LedgerNode) (cid : Nat) :
    ledgerLookup lg cid = specLedgerLookup lg cid := by
  induction lg with
  | nil => rfl
  | cons n rest ih => simp [ledgerLookup, specLedgerLookup, ih]

private theorem optAuthTainted_eq_spec (ma : Option Auth) :
    optAuthTainted ma = specOptTainted ma := by
  cases ma <;> rfl

private theorem existingTainted_eq_spec (ma : Option Auth) :
    existingTainted ma = specExistingTainted ma := by
  cases ma <;> rfl

private theorem parentsTainted_eq_spec (ps : List Nat) (lg : List LedgerNode) :
    parentsTainted ps lg = specParentsTainted ps lg := by
  induction ps with
  | nil => rfl
  | cons p rest ih =>
      rw [parentsTainted, List.any_cons, specParentsTainted, ledgerLookup_eq_spec, optAuthTainted_eq_spec]
      exact congrArg (fun b => specOptTainted (specLedgerLookup lg p) || b) ih

private theorem isTainted_propApply (flag : Bool) (base : Auth) :
    isTainted (propApply (isTainted base || flag) base) = (isTainted base || flag) := by
  cases base with
  | mk cb e t => cases e <;> cases t <;> cases flag <;> rfl

private theorem tainted_propApply (flag : Bool) (base : Auth) :
    (propApply (isTainted base || flag) base).tainted = (isTainted base || flag) := by
  cases base with
  | mk cb e t => cases e <;> cases t <;> cases flag <;> rfl

theorem L01_propagate_iff : L01_Prop := by
  intro c e t base ps
  refine ⟨rfl, ?_, ?_⟩
  · rw [propagate, isTainted_propApply, isTainted_eq_spec, anyTainted_eq_spec]
  · rw [propagate, tainted_propApply, isTainted_eq_spec, anyTainted_eq_spec]

theorem L02_taint_irrevocable_hops : L02_Prop := by
  intro hops
  induction hops with
  | nil =>
      intro root
      simp [deriveChain, specAnyTainted, isTainted_eq_spec]
  | cons h rest ih =>
      intro root
      simp only [deriveChain, specAnyTainted]
      rw [ih (propagate h [root])]
      have hp : specTainted (propagate h [root]) = (specTainted h || specTainted root) := by
        have h1 := (L01_propagate_iff 0 false false h [root]).2.1
        simpa [isTainted_eq_spec, specAnyTainted] using h1
      rw [hp]
      cases specTainted root <;> cases specTainted h <;> cases specAnyTainted rest <;> rfl

theorem L03_sticky_reregister : L03_Prop := by
  intro cid oc old rest base ps
  have h1 : isTainted (ledgerRecord ({ cid := cid, auth := { cleanBits := oc, ext := true, tainted := false } } :: rest) cid base ps) = true := by
    simp only [ledgerRecord, ledgerLookup, nat_beq_refl, ↓reduceIte, existingTainted, isTainted,
      Bool.true_or, Bool.or_true, propApply, setTainted]
  have h2 : isTainted (ledgerRecord ({ cid := cid, auth := specSetTainted old } :: rest) cid base ps) = true := by
    simp only [ledgerRecord, ledgerLookup, nat_beq_refl, ↓reduceIte, existingTainted,
      Bool.or_true, Bool.true_or, propApply, isTainted, setTainted, specSetTainted]
  exact ⟨h1, h2⟩

private theorem not_hasCid_lookup_none (lg : List LedgerNode) (unk : Nat)
    (h : specLedgerHasCid lg unk = false) : specLedgerLookup lg unk = none := by
  induction lg with
  | nil => rfl
  | cons n rest ih =>
      simp [specLedgerHasCid] at h
      obtain ⟨hne, hrest⟩ := h
      simp [specLedgerLookup, hne, ih hrest]

private theorem parentsTainted_append_hit (pPre pTail : List Nat) (badP : Nat) (lg : List LedgerNode)
    (h : specOptTainted (specLedgerLookup lg badP) = true) :
    specParentsTainted (pPre ++ badP :: pTail) lg = true := by
  induction pPre with
  | nil => simp [specParentsTainted, h]
  | cons p rest ih => simp [specParentsTainted, ih]

theorem L04_unknown_parent_fails_closed : L04_Prop := by
  intro lg cid base pPre unk pTail hnot
  have hnone : specLedgerLookup lg unk = none := not_hasCid_lookup_none lg unk hnot
  have hpar : specParentsTainted (pPre ++ unk :: pTail) lg = true :=
    parentsTainted_append_hit pPre pTail unk lg (by simp [specOptTainted, hnone])
  have hrec : isTainted (ledgerRecord lg cid base (pPre ++ unk :: pTail)) =
      (specTainted base || specExistingTainted (specLedgerLookup lg cid) || specParentsTainted (pPre ++ unk :: pTail) lg) := by
    simp only [ledgerRecord]
    have hb : (isTainted base || existingTainted (ledgerLookup lg cid) || parentsTainted (pPre ++ unk :: pTail) lg) =
        (isTainted base || (existingTainted (ledgerLookup lg cid) || parentsTainted (pPre ++ unk :: pTail) lg)) :=
      Bool.or_assoc _ _ _
    rw [hb, isTainted_propApply, isTainted_eq_spec, ledgerLookup_eq_spec, existingTainted_eq_spec, parentsTainted_eq_spec]
    exact (Bool.or_assoc _ _ _).symm
  rw [hrec, hpar]
  simp

private theorem isPrivileged_eq_spec (act : Action) : isPrivileged act = specIsPrivileged act := by
  cases act <;> rfl

theorem L05_empty_args_fail_closed : L05_Prop := by
  intro act lg toks hasPk now
  constructor
  · exact isPrivileged_eq_spec act
  · intro hpriv
    cases act <;> simp [specIsPrivileged] at hpriv <;> simp [checkGateSignedPk, isPrivileged]

private theorem hasValidToken_eq_spec (toks : List DeclassToken) (cid : Nat) (act : Action) (now : Nat) :
    hasValidToken toks cid act now = specHasValidTok toks cid act now := by
  induction toks with
  | nil => rfl
  | cons tk rest ih =>
      rw [hasValidToken, List.any_cons, specHasValidTok, tokenMatches, specTokValid]
      exact congrArg (fun b => (tk.sigValid && (tk.cid == cid) && (tk.act == act) && decide (now <= tk.expiry)) || b) ih

private theorem argAuthorized_eq_spec (cid : Nat) (lg : List LedgerNode) (act : Action)
    (toks : List DeclassToken) (hasPk : Bool) (now : Nat) :
    argAuthorized cid lg act toks hasPk now = specArgOk cid lg act toks hasPk now := by
  simp [argAuthorized, specArgOk, ledgerLookup_eq_spec, optAuthTainted_eq_spec, hasValidToken_eq_spec]

private theorem allArgsAuthorized_eq_spec (args : List Nat) (lg : List LedgerNode) (act : Action)
    (toks : List DeclassToken) (hasPk : Bool) (now : Nat) :
    args.all (fun c => argAuthorized c lg act toks hasPk now) =
      specAllArgsOk args lg act toks hasPk now := by
  induction args with
  | nil => rfl
  | cons c rest ih =>
      rw [List.all_cons, specAllArgsOk, argAuthorized_eq_spec, ih]

private theorem gate_eq_spec (lg : List LedgerNode) (act : Action) (args : List Nat)
    (toks : List DeclassToken) (hasPk : Bool) (now : Nat) :
    checkGateSignedPk lg act args toks hasPk now = specGate lg act args toks hasPk now := by
  cases act <;> cases args with
  | nil => rfl
  | cons c rest =>
      simp only [checkGateSignedPk, specGate, isPrivileged, specIsPrivileged, List.isEmpty_cons,
        Bool.not_false, Bool.not_true, Bool.false_or, Bool.true_and, allArgsAuthorized_eq_spec]
      rfl

theorem L06a_wrong_action_token_rejected : L06a_Prop := by
  intro a1 a2 cid oc oe ot expiry now hpriv ht hne
  rw [checkGateSigned, gate_eq_spec]
  have hneq : (a2 == a1) = false := by
    cases h : (a2 == a1)
    · rfl
    · exact absurd (action_eq_of_beq h).symm hne
  simp only [specGate, hpriv, Bool.not_true, List.isEmpty_cons, Bool.not_false,
    specAllArgsOk, specArgOk, specLedgerLookup, nat_beq_refl, ↓reduceIte,
    specOptTainted, specTainted, ht, specHasValidTok, specTokValid, hneq,
    Bool.and_false, Bool.false_and, Bool.or_false, Bool.and_true]

theorem L06b_partial_declass_blocks : L06b_Prop := by
  intro act c1 c2 o1 oc2 oe2 ot2 exp now hpriv hne ht2
  have hneq : (c1 == c2) = false := by
    cases h : (c1 == c2)
    · rfl
    · exact absurd (nat_eq_of_beq h) hne
  constructor
  · rw [checkGateSigned, gate_eq_spec]
    simp only [specGate, hpriv, Bool.not_true, List.isEmpty_cons, Bool.not_false,
      Bool.true_and, specAllArgsOk, specArgOk, specLedgerLookup, hneq, Bool.false_eq_true, ↓reduceIte,
      nat_beq_refl, specOptTainted, specTainted, ht2, specHasValidTok, specTokValid,
      Bool.and_false, Bool.false_and, Bool.or_false, Bool.and_true]
  · rw [checkGateSigned, gate_eq_spec]
    simp only [specGate, hpriv, Bool.not_true, List.isEmpty_cons, Bool.not_false,
      specAllArgsOk, specArgOk, specLedgerLookup, hneq, Bool.false_eq_true, ↓reduceIte,
      nat_beq_refl, specOptTainted, specTainted, ht2, specHasValidTok, specTokValid,
      Bool.and_false, Bool.false_and, Bool.or_false, Bool.and_true]

private theorem propApply_preserves_origin (flag : Bool) (base : Auth) :
    (propApply flag base).cleanBits = base.cleanBits ∧ (propApply flag base).ext = base.ext := by
  cases flag <;> cases base <;> simp [propApply, setTainted]

theorem L15_propagate_preserves_origin : L15_Prop := by
  intro base ps lg cid pids
  have h1 := propApply_preserves_origin (isTainted base || anyTainted ps) base
  have h2 := propApply_preserves_origin (isTainted base || existingTainted (ledgerLookup lg cid) || parentsTainted pids lg) base
  exact ⟨h1.1, h1.2, h2.1, h2.2⟩

private theorem deep_lookup_hit (pfx rest : List LedgerNode) (cid : Nat) (a : Auth)
    (hnot : specLedgerHasCid pfx cid = false) :
    specLedgerLookup (pfx ++ { cid := cid, auth := a } :: rest) cid = some a := by
  induction pfx with
  | nil => simp only [List.nil_append, specLedgerLookup, nat_beq_refl, ↓reduceIte]
  | cons n tl ih =>
      simp only [specLedgerHasCid, Bool.or_eq_false_iff] at hnot
      obtain ⟨hne, hrest⟩ := hnot
      simp only [List.cons_append, specLedgerLookup, hne, Bool.false_eq_true, ↓reduceIte, ih hrest]

private theorem ledgerRecord_taint_iff (lg : List LedgerNode) (cid : Nat) (base : Auth) (ps : List Nat) :
    isTainted (ledgerRecord lg cid base ps) =
      (specTainted base || specExistingTainted (specLedgerLookup lg cid) || specParentsTainted ps lg) := by
  simp only [ledgerRecord]
  have hb : (isTainted base || existingTainted (ledgerLookup lg cid) || parentsTainted ps lg) =
      (isTainted base || (existingTainted (ledgerLookup lg cid) || parentsTainted ps lg)) :=
    Bool.or_assoc _ _ _
  rw [hb, isTainted_propApply, isTainted_eq_spec, ledgerLookup_eq_spec, existingTainted_eq_spec, parentsTainted_eq_spec]
  exact (Bool.or_assoc _ _ _).symm

private theorem ledgerRecord_tainted_iff (lg : List LedgerNode) (cid : Nat) (base : Auth) (ps : List Nat) :
    (ledgerRecord lg cid base ps).tainted =
      (specTainted base || specExistingTainted (specLedgerLookup lg cid) || specParentsTainted ps lg) := by
  simp only [ledgerRecord]
  have hb : (isTainted base || existingTainted (ledgerLookup lg cid) || parentsTainted ps lg) =
      (isTainted base || (existingTainted (ledgerLookup lg cid) || parentsTainted ps lg)) :=
    Bool.or_assoc _ _ _
  rw [hb, tainted_propApply, isTainted_eq_spec, ledgerLookup_eq_spec, existingTainted_eq_spec, parentsTainted_eq_spec]
  exact (Bool.or_assoc _ _ _).symm

theorem L16_sticky_deep_and_clean_liveness : L16_Prop := by
  intro prefixLg rest cid oc oe ot base ps lg bc cleanPs target
  refine ⟨?_, ?_, ?_⟩
  · intro hnot ht
    rw [ledgerRecord_taint_iff]
    rw [deep_lookup_hit prefixLg rest cid { cleanBits := oc, ext := oe, tainted := ot } hnot]
    simp [specExistingTainted, specTainted, ht]
  · intro hold hpar
    rw [ledgerRecord_taint_iff]
    simp [specTainted, hold, hpar]
  · intro htarget
    simp only [ledgerInsert, specLedgerLookup]
    cases heq : (cid == target)
    · simpa using htarget
    · have hct : cid = target := nat_eq_of_beq heq
      subst hct
      simp only [↓reduceIte, specExistingTainted, ← isTainted_eq_spec]
      rw [ledgerRecord_taint_iff, htarget]
      simp

private theorem deep_tok_valid (tpre ttail : List DeclassToken) (cid : Nat) (act : Action) (now d : Nat) :
    specHasValidTok (tpre ++ { cid := cid, act := act, expiry := now + d, sigValid := true } :: ttail) cid act now = true := by
  induction tpre with
  | nil =>
      have hle : now <= now + d := Nat.le_add_right now d
      simp only [List.nil_append, specHasValidTok, specTokValid, nat_beq_refl, action_beq_refl, hle,
        decide_true, Bool.and_self, Bool.true_or]
  | cons tk rest ih =>
      simp only [List.cons_append, specHasValidTok, ih, Bool.or_true]

theorem L17_gate_exact_iff_and_liveness : L17_Prop := by
  intro lg act args toks hasPk now cid old tpre ttail d
  refine ⟨gate_eq_spec lg act args toks hasPk now, gate_eq_spec lg act args toks true now, ?_, ?_⟩
  · rfl
  · rw [gate_eq_spec]
    have hs : specTainted (specSetTainted old) = true := by
      cases old with | mk _ e _ => cases e <;> rfl
    have ht := deep_tok_valid tpre ttail cid act now d
    cases act <;> simp only [specGate, specIsPrivileged, Bool.not_false, Bool.not_true,
      Bool.true_or, Bool.false_or, List.isEmpty_cons, specAllArgsOk, specArgOk,
      specLedgerLookup, nat_beq_refl, ↓reduceIte, specOptTainted, hs, ht, Bool.and_true]

theorem L18_expired_and_forged_sig_rejected : L18_Prop := by
  intro act cid old exp d now hpriv
  have hs : specTainted (specSetTainted old) = true := by
    cases old with | mk _ e _ => cases e <;> rfl
  have hgt : ¬ (exp + d + 1 <= exp) := by omega
  refine ⟨?_, ?_, ?_⟩
  · rw [checkGateSigned, gate_eq_spec]
    simp only [specGate, hpriv, Bool.not_true, List.isEmpty_cons, Bool.not_false,
      specAllArgsOk, specArgOk, specLedgerLookup, nat_beq_refl, ↓reduceIte,
      specOptTainted, hs, specHasValidTok, specTokValid, hgt, decide_false,
      Bool.and_false, Bool.or_false, Bool.and_true]
  · rw [checkGateSigned, gate_eq_spec]
    simp only [specGate, hpriv, Bool.not_true, List.isEmpty_cons, Bool.not_false,
      Bool.true_and, specAllArgsOk, specArgOk, specLedgerLookup, nat_beq_refl, ↓reduceIte,
      specOptTainted, hs, specHasValidTok, specTokValid,
      Bool.false_and, Bool.or_false, Bool.and_true]
  · rw [gate_eq_spec]
    simp only [specGate, hpriv, Bool.not_true, Bool.false_or, List.isEmpty_cons, Bool.not_false,
      Bool.true_and, specAllArgsOk, specArgOk, specLedgerLookup, nat_beq_refl, ↓reduceIte,
      specOptTainted, hs, Bool.false_and, Bool.and_true]

private theorem allArgsOk_append_false (pfx tl : List Nat) (evil : Nat)
    (lg : List LedgerNode) (act : Action) (toks : List DeclassToken) (hasPk : Bool) (now : Nat)
    (hbad : specArgOk evil lg act toks hasPk now = false) :
    specAllArgsOk (pfx ++ evil :: tl) lg act toks hasPk now = false := by
  induction pfx with
  | nil => simp [specAllArgsOk, hbad]
  | cons h rest ih => simp [specAllArgsOk, ih]

theorem L19_nth_arg_taint_blocks : L19_Prop := by
  intro act pfx evilCid tl lg toks hasPk now hpriv hbad
  rw [gate_eq_spec]
  have hfalse := allArgsOk_append_false pfx tl evilCid lg act toks hasPk now hbad
  simp [specGate, hpriv, hfalse]

theorem L25_ledger_record_exact_iff : L25_Prop := by
  intro lg cid base ps pPre badP pTail
  refine ⟨ledgerRecord_taint_iff lg cid base ps, ledgerRecord_tainted_iff lg cid base ps, ?_⟩
  intro hbad
  rw [ledgerRecord_taint_iff]
  rw [parentsTainted_append_hit pPre pTail badP lg hbad]
  simp

theorem L26_header_meta_and_owner_pubkey : L26_Prop := by
  intro low4 res3 epi flags r e f
  refine ⟨rfl, rfl, ?_, ?_, rfl, ?_, ?_, rfl, rfl⟩
  · have h : ¬ (16 + r <= 15) := by omega
    simp only [isValidHeaderMeta, h, decide_false, Bool.and_false, Bool.false_and]
  · have h : (r + 1 == 0) = false := by cases r <;> rfl
    simp only [isValidHeaderMeta, h, Bool.and_false, Bool.false_and]
  · have h : ¬ (4 + e <= 3) := by omega
    simp only [isValidHeaderMeta, h, decide_false, Bool.and_false]
  · have h : (f + 1 == 0) = false := by cases f <;> rfl
    simp only [isValidHeaderMeta, h, Bool.false_and]

theorem L27_ed25519_action_code_and_mcp_classifier : L27_Prop := by
  intro a b k pol verb allowRo builtinRo trustAnn hint
  refine ⟨?_, ?_, ?_, ?_, ?_, rfl, ?_⟩
  · cases a <;> rfl
  · cases a <;> rfl
  · intro hne
    cases a <;> cases b <;> first | exact absurd rfl hne | decide
  · match h : 5 + k with
    | 0 | 1 | 2 | 3 | 4 => omega
    | _ + 5 => rfl
  · cases pol <;> cases verb <;> rfl
  · cases pol <;> cases verb <;> cases allowRo <;> cases builtinRo <;> cases hint <;> rfl

-- =============================================================================
-- GROUP 2 PROOFS: PLANE B 6-SYMBOL MEALY TRANSDUCER, MULTI-CELL R2, EXTRACT
-- =============================================================================

private theorem hasForbiddenStep_sanitizeStep (xs : List Sym) (rLt rGt : Run2) (prev : PrevSym) :
    hasForbiddenStep (sanitizeStep xs rLt rGt prev) rLt rGt prev = false := by
  induction xs generalizing rLt rGt prev with
  | nil => rfl
  | cons x rest ih =>
      cases x with
      | spoof => exact ih rLt rGt prev
      | lt =>
          cases rLt with
          | r0 => exact ih .r1 .r0 .pLt
          | r1 => exact ih .r2 .r0 .pLt
          | r2 => exact ih .r1 .r0 .pLt
      | gt =>
          cases prev with
          | pPipe => exact ih .r0 .r1 .pOther
          | pLt | pOther =>
              cases rGt with
              | r0 => exact ih .r0 .r1 .pOther
              | r1 => exact ih .r0 .r2 .pOther
              | r2 => exact ih .r0 .r1 .pOther
      | pipe =>
          cases prev with
          | pLt => exact ih .r0 .r0 .pPipe
          | pPipe | pOther => exact ih .r0 .r0 .pPipe
      | esc => exact ih .r0 .r0 .pOther
      | other => exact ih .r0 .r0 .pOther

theorem L07_fence_no_triple : L07_Prop := by
  intro xs
  exact hasForbiddenStep_sanitizeStep xs .r0 .r0 .pOther

private theorem sanitizeStep_safe_id (xs : List Sym) (rLt rGt : Run2) (prev : PrevSym)
    (hsafe : hasForbiddenStep xs rLt rGt prev = false) :
    sanitizeStep xs rLt rGt prev = xs := by
  induction xs generalizing rLt rGt prev with
  | nil => rfl
  | cons x rest ih =>
      cases x with
      | spoof => simp [hasForbiddenStep] at hsafe
      | lt =>
          cases rLt with
          | r0 => simp only [sanitizeStep, ih .r1 .r0 .pLt hsafe]
          | r1 => simp only [sanitizeStep, ih .r2 .r0 .pLt hsafe]
          | r2 => simp [hasForbiddenStep] at hsafe
      | gt =>
          cases prev with
          | pPipe => simp [hasForbiddenStep] at hsafe
          | pLt | pOther =>
              cases rGt with
              | r0 => simp only [sanitizeStep, ih .r0 .r1 .pOther hsafe]
              | r1 => simp only [sanitizeStep, ih .r0 .r2 .pOther hsafe]
              | r2 => simp [hasForbiddenStep] at hsafe
      | pipe =>
          cases prev with
          | pLt => simp [hasForbiddenStep] at hsafe
          | pPipe | pOther => simp only [sanitizeStep, ih .r0 .r0 .pPipe hsafe]
      | esc => simp only [sanitizeStep, ih .r0 .r0 .pOther hsafe]
      | other => simp only [sanitizeStep, ih .r0 .r0 .pOther hsafe]

theorem L08_fence_safe_identity : L08_Prop := by
  intro xs hsafe
  exact sanitizeStep_safe_id xs .r0 .r0 .pOther hsafe

private theorem stripEsc_cons_ne (x : Sym) (xs : List Sym) (h : (x != .esc) = true) :
    stripEsc (x :: xs) = x :: stripEsc xs := by
  simp [stripEsc, h]

private theorem stripEsc_cons_esc (xs : List Sym) :
    stripEsc (.esc :: xs) = stripEsc xs := by
  simp [stripEsc]

private theorem stripSpoof_cons_ne (x : Sym) (xs : List Sym) (h : (x != .spoof) = true) :
    stripSpoof (x :: xs) = x :: stripSpoof xs := by
  simp [stripSpoof, h]

private theorem stripSpoof_cons_spoof (xs : List Sym) :
    stripSpoof (.spoof :: xs) = stripSpoof xs := by
  simp [stripSpoof]

private theorem stripEsc_sanitizeStep (xs : List Sym) (rLt rGt : Run2) (prev : PrevSym) :
    stripEsc (sanitizeStep xs rLt rGt prev) = stripEsc (stripSpoof xs) := by
  induction xs generalizing rLt rGt prev with
  | nil => rfl
  | cons x rest ih =>
      cases x with
      | spoof =>
          rw [sanitizeStep, stripSpoof_cons_spoof]
          exact ih rLt rGt prev
      | lt =>
          rw [stripSpoof_cons_ne .lt rest rfl, stripEsc_cons_ne .lt _ rfl]
          cases rLt with
          | r0 => rw [sanitizeStep, stripEsc_cons_ne .lt _ rfl, ih .r1 .r0 .pLt]
          | r1 => rw [sanitizeStep, stripEsc_cons_ne .lt _ rfl, ih .r2 .r0 .pLt]
          | r2 => rw [sanitizeStep, stripEsc_cons_esc, stripEsc_cons_ne .lt _ rfl, ih .r1 .r0 .pLt]
      | gt =>
          rw [stripSpoof_cons_ne .gt rest rfl, stripEsc_cons_ne .gt _ rfl]
          cases prev with
          | pPipe => simp only [sanitizeStep]; rw [stripEsc_cons_esc, stripEsc_cons_ne .gt _ rfl, ih .r0 .r1 .pOther]
          | pLt | pOther =>
              cases rGt with
              | r0 => simp only [sanitizeStep]; rw [stripEsc_cons_ne .gt _ rfl, ih .r0 .r1 .pOther]
              | r1 => simp only [sanitizeStep]; rw [stripEsc_cons_ne .gt _ rfl, ih .r0 .r2 .pOther]
              | r2 => simp only [sanitizeStep]; rw [stripEsc_cons_esc, stripEsc_cons_ne .gt _ rfl, ih .r0 .r1 .pOther]
      | pipe =>
          rw [stripSpoof_cons_ne .pipe rest rfl, stripEsc_cons_ne .pipe _ rfl]
          cases prev with
          | pLt => simp only [sanitizeStep]; rw [stripEsc_cons_esc, stripEsc_cons_ne .pipe _ rfl, ih .r0 .r0 .pPipe]
          | pPipe | pOther => simp only [sanitizeStep]; rw [stripEsc_cons_ne .pipe _ rfl, ih .r0 .r0 .pPipe]
      | esc =>
          rw [sanitizeStep, stripSpoof_cons_ne .esc rest rfl, stripEsc_cons_esc, stripEsc_cons_esc]
          exact ih .r0 .r0 .pOther
      | other =>
          rw [sanitizeStep, stripSpoof_cons_ne .other rest rfl, stripEsc_cons_ne .other _ rfl, stripEsc_cons_ne .other _ rfl, ih .r0 .r0 .pOther]

private theorem sublist_sanitizeStep (xs : List Sym) (rLt rGt : Run2) (prev : PrevSym) :
    List.Sublist (stripSpoof xs) (sanitizeStep xs rLt rGt prev) := by
  induction xs generalizing rLt rGt prev with
  | nil => exact List.Sublist.slnil
  | cons x rest ih =>
      cases x with
      | spoof =>
          rw [sanitizeStep, stripSpoof_cons_spoof]
          exact ih rLt rGt prev
      | lt =>
          rw [stripSpoof_cons_ne .lt rest rfl]
          cases rLt with
          | r0 => exact List.Sublist.cons_cons .lt (ih .r1 .r0 .pLt)
          | r1 => exact List.Sublist.cons_cons .lt (ih .r2 .r0 .pLt)
          | r2 => exact List.Sublist.cons .esc (List.Sublist.cons_cons .lt (ih .r1 .r0 .pLt))
      | gt =>
          rw [stripSpoof_cons_ne .gt rest rfl]
          cases prev with
          | pPipe => exact List.Sublist.cons .esc (List.Sublist.cons_cons .gt (ih .r0 .r1 .pOther))
          | pLt | pOther =>
              cases rGt with
              | r0 => exact List.Sublist.cons_cons .gt (ih .r0 .r1 .pOther)
              | r1 => exact List.Sublist.cons_cons .gt (ih .r0 .r2 .pOther)
              | r2 => exact List.Sublist.cons .esc (List.Sublist.cons_cons .gt (ih .r0 .r1 .pOther))
      | pipe =>
          rw [stripSpoof_cons_ne .pipe rest rfl]
          cases prev with
          | pLt => exact List.Sublist.cons .esc (List.Sublist.cons_cons .pipe (ih .r0 .r0 .pPipe))
          | pPipe | pOther => exact List.Sublist.cons_cons .pipe (ih .r0 .r0 .pPipe)
      | esc =>
          rw [stripSpoof_cons_ne .esc rest rfl]
          exact List.Sublist.cons_cons .esc (ih .r0 .r0 .pOther)
      | other =>
          rw [stripSpoof_cons_ne .other rest rfl]
          exact List.Sublist.cons_cons .other (ih .r0 .r0 .pOther)

private theorem sanitizeStep_length_le_two_mul (xs : List Sym) (rLt rGt : Run2) (prev : PrevSym) :
    (sanitizeStep xs rLt rGt prev).length <= 2 * (specStripSpoof xs).length := by
  induction xs generalizing rLt rGt prev with
  | nil => simp [sanitizeStep, specStripSpoof]
  | cons x rest ih =>
      cases x with
      | spoof =>
          rw [sanitizeStep, show specStripSpoof (.spoof :: rest) = specStripSpoof rest from stripSpoof_cons_spoof rest]
          exact ih rLt rGt prev
      | lt =>
          rw [show specStripSpoof (.lt :: rest) = .lt :: specStripSpoof rest from stripSpoof_cons_ne .lt rest rfl]
          cases rLt with
          | r0 => have h := ih .r1 .r0 .pLt; simp [sanitizeStep]; omega
          | r1 => have h := ih .r2 .r0 .pLt; simp [sanitizeStep]; omega
          | r2 => have h := ih .r1 .r0 .pLt; simp [sanitizeStep]; omega
      | gt =>
          rw [show specStripSpoof (.gt :: rest) = .gt :: specStripSpoof rest from stripSpoof_cons_ne .gt rest rfl]
          cases prev with
          | pPipe => have h := ih .r0 .r1 .pOther; simp [sanitizeStep]; omega
          | pLt | pOther =>
              cases rGt with
              | r0 => have h := ih .r0 .r1 .pOther; simp [sanitizeStep]; omega
              | r1 => have h := ih .r0 .r2 .pOther; simp [sanitizeStep]; omega
              | r2 => have h := ih .r0 .r1 .pOther; simp [sanitizeStep]; omega
      | pipe =>
          rw [show specStripSpoof (.pipe :: rest) = .pipe :: specStripSpoof rest from stripSpoof_cons_ne .pipe rest rfl]
          cases prev with
          | pLt => have h := ih .r0 .r0 .pPipe; simp [sanitizeStep]; omega
          | pPipe | pOther => have h := ih .r0 .r0 .pPipe; simp [sanitizeStep]; omega
      | esc =>
          rw [show specStripSpoof (.esc :: rest) = .esc :: specStripSpoof rest from stripSpoof_cons_ne .esc rest rfl]
          have h := ih .r0 .r0 .pOther; simp [sanitizeStep]; omega
      | other =>
          rw [show specStripSpoof (.other :: rest) = .other :: specStripSpoof rest from stripSpoof_cons_ne .other rest rfl]
          have h := ih .r0 .r0 .pOther; simp [sanitizeStep]; omega

theorem L20_fence_preserves_all_non_esc_symbols : L20_Prop := by
  intro xs
  exact ⟨stripEsc_sanitizeStep xs .r0 .r0 .pOther,
         sublist_sanitizeStep xs .r0 .r0 .pOther,
         sanitizeStep_length_le_two_mul xs .r0 .r0 .pOther⟩

private theorem hasForbiddenStep_append_other (a b : List Sym) (rLt rGt : Run2) (prev : PrevSym)
    (ha : hasForbiddenStep a rLt rGt prev = false)
    (hb : hasForbiddenStep b .r0 .r0 .pOther = false) :
    hasForbiddenStep (a ++ .other :: b) rLt rGt prev = false := by
  induction a generalizing rLt rGt prev with
  | nil => exact hb
  | cons x rest ih =>
      cases x with
      | spoof => simp [hasForbiddenStep] at ha
      | lt =>
          cases rLt with
          | r0 => exact ih .r1 .r0 .pLt ha
          | r1 => exact ih .r2 .r0 .pLt ha
          | r2 => simp [hasForbiddenStep] at ha
      | gt =>
          cases prev with
          | pPipe => simp [hasForbiddenStep] at ha
          | pLt | pOther =>
              cases rGt with
              | r0 => exact ih .r0 .r1 .pOther ha
              | r1 => exact ih .r0 .r2 .pOther ha
              | r2 => simp [hasForbiddenStep] at ha
      | pipe =>
          cases prev with
          | pLt => simp [hasForbiddenStep] at ha
          | pPipe | pOther => exact ih .r0 .r0 .pPipe ha
      | esc => exact ih .r0 .r0 .pOther ha
      | other => exact ih .r0 .r0 .pOther ha

private theorem stripSpoof_sanitizeStep (xs : List Sym) (rLt rGt : Run2) (prev : PrevSym) :
    stripSpoof (sanitizeStep xs rLt rGt prev) = sanitizeStep xs rLt rGt prev := by
  induction xs generalizing rLt rGt prev with
  | nil => rfl
  | cons x rest ih =>
      cases x with
      | spoof => exact ih rLt rGt prev
      | lt =>
          cases rLt with
          | r0 => rw [sanitizeStep, stripSpoof_cons_ne .lt _ rfl, ih .r1 .r0 .pLt]
          | r1 => rw [sanitizeStep, stripSpoof_cons_ne .lt _ rfl, ih .r2 .r0 .pLt]
          | r2 => rw [sanitizeStep, stripSpoof_cons_ne .esc _ rfl, stripSpoof_cons_ne .lt _ rfl, ih .r1 .r0 .pLt]
      | gt =>
          cases prev with
          | pPipe => simp only [sanitizeStep]; rw [stripSpoof_cons_ne .esc _ rfl, stripSpoof_cons_ne .gt _ rfl, ih .r0 .r1 .pOther]
          | pLt | pOther =>
              cases rGt with
              | r0 => simp only [sanitizeStep]; rw [stripSpoof_cons_ne .gt _ rfl, ih .r0 .r1 .pOther]
              | r1 => simp only [sanitizeStep]; rw [stripSpoof_cons_ne .gt _ rfl, ih .r0 .r2 .pOther]
              | r2 => simp only [sanitizeStep]; rw [stripSpoof_cons_ne .esc _ rfl, stripSpoof_cons_ne .gt _ rfl, ih .r0 .r1 .pOther]
      | pipe =>
          cases prev with
          | pLt => simp only [sanitizeStep]; rw [stripSpoof_cons_ne .esc _ rfl, stripSpoof_cons_ne .pipe _ rfl, ih .r0 .r0 .pPipe]
          | pPipe | pOther => simp only [sanitizeStep]; rw [stripSpoof_cons_ne .pipe _ rfl, ih .r0 .r0 .pPipe]
      | esc => simp only [sanitizeStep]; rw [stripSpoof_cons_ne .esc _ rfl, ih .r0 .r0 .pOther]
      | other => simp only [sanitizeStep]; rw [stripSpoof_cons_ne .other _ rfl, ih .r0 .r0 .pOther]

private theorem stripEsc_append (a b : List Sym) :
    stripEsc (a ++ b) = stripEsc a ++ stripEsc b :=
  List.filter_append a b

private theorem stripSpoof_append (a b : List Sym) :
    stripSpoof (a ++ b) = stripSpoof a ++ stripSpoof b :=
  List.filter_append a b

private theorem stripEsc_sanitizeFences (xs : List Sym) :
    stripEsc (sanitizeFences xs) = stripEsc (stripSpoof xs) :=
  stripEsc_sanitizeStep xs .r0 .r0 .pOther

private theorem stripSpoof_sanitizeFences (xs : List Sym) :
    stripSpoof (sanitizeFences xs) = sanitizeFences xs :=
  stripSpoof_sanitizeStep xs .r0 .r0 .pOther

private theorem specStripSpoof_idem (xs : List Sym) :
    specStripSpoof (specStripSpoof xs) = specStripSpoof xs := by
  induction xs with
  | nil => rfl
  | cons x rest ih =>
      cases x with
      | spoof =>
          rw [show specStripSpoof (.spoof :: rest) = specStripSpoof rest from stripSpoof_cons_spoof rest, ih]
      | lt =>
          rw [show specStripSpoof (.lt :: rest) = .lt :: specStripSpoof rest from stripSpoof_cons_ne .lt rest rfl,
              show specStripSpoof (.lt :: specStripSpoof rest) = .lt :: specStripSpoof (specStripSpoof rest) from stripSpoof_cons_ne .lt _ rfl, ih]
      | gt =>
          rw [show specStripSpoof (.gt :: rest) = .gt :: specStripSpoof rest from stripSpoof_cons_ne .gt rest rfl,
              show specStripSpoof (.gt :: specStripSpoof rest) = .gt :: specStripSpoof (specStripSpoof rest) from stripSpoof_cons_ne .gt _ rfl, ih]
      | pipe =>
          rw [show specStripSpoof (.pipe :: rest) = .pipe :: specStripSpoof rest from stripSpoof_cons_ne .pipe rest rfl,
              show specStripSpoof (.pipe :: specStripSpoof rest) = .pipe :: specStripSpoof (specStripSpoof rest) from stripSpoof_cons_ne .pipe _ rfl, ih]
      | esc =>
          rw [show specStripSpoof (.esc :: rest) = .esc :: specStripSpoof rest from stripSpoof_cons_ne .esc rest rfl,
              show specStripSpoof (.esc :: specStripSpoof rest) = .esc :: specStripSpoof (specStripSpoof rest) from stripSpoof_cons_ne .esc _ rfl, ih]
      | other =>
          rw [show specStripSpoof (.other :: rest) = .other :: specStripSpoof rest from stripSpoof_cons_ne .other rest rfl,
              show specStripSpoof (.other :: specStripSpoof rest) = .other :: specStripSpoof (specStripSpoof rest) from stripSpoof_cons_ne .other _ rfl, ih]

private theorem sublist_sanitizeFences (xs : List Sym) :
    List.Sublist (specStripSpoof xs) (sanitizeFences xs) :=
  sublist_sanitizeStep xs .r0 .r0 .pOther

theorem L21_r2_structured_table_header_safe : L21_Prop := by
  intro field sk sv tk tcol tc1 tc2 rk rsp
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · cases field with
    | scalar k v =>
        exact hasForbiddenStep_append_other (sanitizeFences k) (sanitizeFences v) .r0 .r0 .pOther
          (L07_fence_no_triple k) (L07_fence_no_triple v)
    | table k col c1 c2 =>
        have hrow := L07_fence_no_triple (sanitizeFences c1 ++ (.pipe :: sanitizeFences c2))
        have hcr := hasForbiddenStep_append_other (sanitizeFences col)
          (sanitizeFences (sanitizeFences c1 ++ (.pipe :: sanitizeFences c2))) .r0 .r0 .pOther
          (L07_fence_no_triple col) hrow
        exact hasForbiddenStep_append_other (sanitizeFences k)
          (sanitizeFences col ++ .other :: sanitizeFences (sanitizeFences c1 ++ (.pipe :: sanitizeFences c2)))
          .r0 .r0 .pOther (L07_fence_no_triple k) hcr
    | tensorRef k sp =>
        exact hasForbiddenStep_append_other (sanitizeFences k)
          (sanitizeFences (.lt :: .other :: sanitizeFences sp ++ [.gt])) .r0 .r0 .pOther
          (L07_fence_no_triple k) (L07_fence_no_triple _)
  · exact show stripEsc (renderR2Field (.scalar sk sv)) = stripEsc (stripSpoof sk) ++ (.other :: stripEsc (stripSpoof sv)) by
      rw [renderR2Field, stripEsc_append, stripEsc_cons_ne .other _ rfl, stripEsc_sanitizeFences, stripEsc_sanitizeFences]
  · exact (sublist_sanitizeFences sk).append (List.Sublist.cons_cons .other (sublist_sanitizeFences sv))
  · exact show stripEsc (renderR2Field (.table tk tcol tc1 tc2)) =
        stripEsc (stripSpoof tk) ++ (.other :: stripEsc (stripSpoof tcol) ++
          (.other :: stripEsc (stripSpoof tc1) ++ (.pipe :: stripEsc (stripSpoof tc2)))) by
      simp only [renderR2Field, List.cons_append, stripEsc_append,
        stripEsc_cons_ne .other _ rfl, stripEsc_sanitizeFences,
        stripSpoof_append, stripSpoof_cons_ne .pipe _ rfl, stripSpoof_sanitizeFences,
        stripEsc_cons_ne .pipe _ rfl]
  · have hrow1 : List.Sublist (specStripSpoof tc1 ++ (.pipe :: specStripSpoof tc2))
        (sanitizeFences tc1 ++ (.pipe :: sanitizeFences tc2)) :=
      (sublist_sanitizeFences tc1).append (List.Sublist.cons_cons .pipe (sublist_sanitizeFences tc2))
    have hrow2 : List.Sublist (sanitizeFences tc1 ++ (.pipe :: sanitizeFences tc2))
        (sanitizeFences (sanitizeFences tc1 ++ (.pipe :: sanitizeFences tc2))) := by
      have hss : specStripSpoof (sanitizeFences tc1 ++ (.pipe :: sanitizeFences tc2)) =
          sanitizeFences tc1 ++ (.pipe :: sanitizeFences tc2) := by
        change stripSpoof (sanitizeFences tc1 ++ (.pipe :: sanitizeFences tc2)) = _
        rw [stripSpoof_append, stripSpoof_cons_ne .pipe _ rfl, stripSpoof_sanitizeFences, stripSpoof_sanitizeFences]
      have hsub := sublist_sanitizeFences (sanitizeFences tc1 ++ (.pipe :: sanitizeFences tc2))
      rwa [hss] at hsub
    have hrow := hrow1.trans hrow2
    have hmid : List.Sublist
        (specStripSpoof tcol ++ (.other :: (specStripSpoof tc1 ++ (.pipe :: specStripSpoof tc2))))
        (sanitizeFences tcol ++ (.other :: sanitizeFences (sanitizeFences tc1 ++ (.pipe :: sanitizeFences tc2)))) :=
      (sublist_sanitizeFences tcol).append (List.Sublist.cons_cons .other hrow)
    have hall := (sublist_sanitizeFences tk).append (List.Sublist.cons_cons .other hmid)
    simpa only [renderR2Field, List.cons_append] using hall
  · exact show stripEsc (renderR2Field (.tensorRef rk rsp)) =
        stripEsc (stripSpoof rk) ++ (.other :: .lt :: .other :: stripEsc (stripSpoof rsp) ++ [.gt]) by
      simp only [renderR2Field, List.cons_append, stripEsc_append,
        stripEsc_cons_ne .other _ rfl, stripEsc_sanitizeFences,
        stripSpoof_cons_ne .lt _ rfl, stripSpoof_cons_ne .other _ rfl,
        stripSpoof_append, stripSpoof_sanitizeFences, stripSpoof_cons_ne .gt [] rfl,
        stripEsc_cons_ne .lt _ rfl]
      rfl
  · have htr1 : List.Sublist (.lt :: .other :: (specStripSpoof rsp ++ [.gt]))
        (.lt :: .other :: (sanitizeFences rsp ++ [.gt])) :=
      List.Sublist.cons_cons .lt (List.Sublist.cons_cons .other
        ((sublist_sanitizeFences rsp).append (List.Sublist.refl [.gt])))
    have htr2 : List.Sublist (.lt :: .other :: (sanitizeFences rsp ++ [.gt]))
        (sanitizeFences (.lt :: .other :: (sanitizeFences rsp ++ [.gt]))) := by
      have hss : specStripSpoof (.lt :: .other :: (sanitizeFences rsp ++ [.gt])) =
          .lt :: .other :: (sanitizeFences rsp ++ [.gt]) := by
        change stripSpoof (.lt :: .other :: (sanitizeFences rsp ++ [.gt])) = _
        simp only [stripSpoof_cons_ne .lt _ rfl, stripSpoof_cons_ne .other _ rfl,
          stripSpoof_append, stripSpoof_sanitizeFences, stripSpoof_cons_ne .gt [] rfl]
        rfl
      have hsub := sublist_sanitizeFences (.lt :: .other :: (sanitizeFences rsp ++ [.gt]))
      rwa [hss] at hsub
    have hall := (sublist_sanitizeFences rk).append (List.Sublist.cons_cons .other (htr1.trans htr2))
    simpa only [renderR2Field, List.cons_append] using hall

private theorem hasForbiddenStep_take (n : Nat) (ys : List Sym) (rLt rGt : Run2) (prev : PrevSym)
    (hsafe : hasForbiddenStep ys rLt rGt prev = false) :
    hasForbiddenStep (ys.take n) rLt rGt prev = false := by
  induction n generalizing ys rLt rGt prev with
  | zero => rfl
  | succ pn ih =>
      cases ys with
      | nil => rfl
      | cons y rest =>
          cases y with
          | spoof => simp [hasForbiddenStep] at hsafe
          | lt =>
              cases rLt with
              | r0 => exact ih rest .r1 .r0 .pLt hsafe
              | r1 => exact ih rest .r2 .r0 .pLt hsafe
              | r2 => simp [hasForbiddenStep] at hsafe
          | gt =>
              cases prev with
              | pPipe => simp [hasForbiddenStep] at hsafe
              | pLt | pOther =>
                  cases rGt with
                  | r0 => exact ih rest .r0 .r1 .pOther hsafe
                  | r1 => exact ih rest .r0 .r2 .pOther hsafe
                  | r2 => simp [hasForbiddenStep] at hsafe
          | pipe =>
              cases prev with
              | pLt => simp [hasForbiddenStep] at hsafe
              | pPipe | pOther => exact ih rest .r0 .r0 .pPipe hsafe
          | esc => exact ih rest .r0 .r0 .pOther hsafe
          | other => exact ih rest .r0 .r0 .pOther hsafe

theorem L28_r2_compact_and_virtual_atlas_fail_closed : L28_Prop := by
  intro a c k n e t xs
  refine ⟨rfl, rfl, ?_, ?_, ?_, ?_, rfl, ?_⟩
  · cases e <;> cases t <;> rfl
  · cases t <;> rfl
  · cases e <;> rfl
  · have h1 : (3 + k == 1) = false := by
      cases h : (3 + k == 1)
      · rfl
      · have := nat_eq_of_beq h; omega
    have h2 : (3 + k == 2) = false := by
      cases h : (3 + k == 2)
      · rfl
      · have := nat_eq_of_beq h; omega
    cases e <;> cases t <;> simp [r2CompactMode, h1, h2]
  · exact hasForbiddenStep_take n (sanitizeFences xs) .r0 .r0 .pOther (L07_fence_no_triple xs)

theorem L29_r2_table_marker_injectivity : L29_Prop := by
  intro c raw
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · cases c with
    | cellNull => rfl
    | cellMissing => rfl
    | cellStr raw0 =>
        simp only [encodeTableCell]
        cases h : stripSpoof raw0 with
        | nil => rfl
        | cons hd tl =>
            cases isNullOrMissingStr (hd :: tl) <;>
              exact L07_fence_no_triple (hd :: tl)
  · simp only [encodeTableCell]
    cases h : stripSpoof raw with
    | nil => decide
    | cons hd tl => cases isNullOrMissingStr (hd :: tl) <;> simp
  · simp only [encodeTableCell]
    cases h : stripSpoof raw with
    | nil => decide
    | cons hd tl => cases isNullOrMissingStr (hd :: tl) <;> simp
  · decide
  · simp only [encodeTableCell]
    have hi : stripSpoof (specStripSpoof raw) = stripSpoof raw := specStripSpoof_idem raw
    rw [hi]
  · intro h
    simp only [encodeTableCell]
    have h' : stripSpoof raw = [] := h
    rw [h']
  · intro h
    simp only [encodeTableCell]
    have h' : stripSpoof raw = [.other] := h
    rw [h']; rfl
  · intro h
    simp only [encodeTableCell]
    have h' : stripSpoof raw = [.lt, .other, .gt] := h
    rw [h']; rfl
  · simp only [encodeTableCell]
    cases h : stripSpoof raw with
    | nil =>
        have h' : specStripSpoof raw = [] := h
        rw [h']; rfl
    | cons hd tl =>
        have h' : specStripSpoof raw = hd :: tl := h
        have hi : specStripSpoof (hd :: tl) = hd :: tl := by
          rw [← h', specStripSpoof_idem]
        cases isNullOrMissingStr (hd :: tl) <;>
          (simp only [Bool.false_eq_true, ↓reduceIte, cellTagContent, h']
           rw [show specStripEsc (sanitizeFences (hd :: tl)) = specStripEsc (specStripSpoof (hd :: tl)) from stripEsc_sanitizeFences (hd :: tl), hi])
  · simp only [encodeTableCell]
    cases h : stripSpoof raw with
    | nil =>
        have h' : specStripSpoof raw = [] := h
        rw [h']; exact List.Sublist.slnil
    | cons hd tl =>
        have h' : specStripSpoof raw = hd :: tl := h
        have hi : specStripSpoof (hd :: tl) = hd :: tl := by
          rw [← h', specStripSpoof_idem]
        cases isNullOrMissingStr (hd :: tl) <;>
          (simp only [Bool.false_eq_true, ↓reduceIte, cellTagContent, h']
           have hs := sublist_sanitizeFences (hd :: tl)
           rwa [hi] at hs)

private theorem extractFieldOpt_none (rest : List PathSeg) :
    extractFieldOpt rest none = none := by
  cases rest <;> rfl

private theorem extractFieldOpt_append (p1 p2 : List PathSeg) (opt : Option ValTree) :
    extractFieldOpt (p1 ++ p2) opt = (extractFieldOpt p1 opt).bind (fun u => extractField u p2) := by
  induction p1 generalizing opt with
  | nil =>
      cases opt with
      | none => exact extractFieldOpt_none p2
      | some v => rfl
  | cons seg rest ih =>
      cases opt with
      | none =>
          simp only [List.cons_append, extractFieldOpt]
          rfl
      | some v => exact ih (extractStep v seg)

private theorem mapFindVal_append_hit (pre post : List (Nat × ValTree)) (k : Nat) (v : ValTree)
    (hnot : specMapHasKey pre k = false) :
    mapFindVal (pre ++ (k, v) :: post) k = some v := by
  induction pre with
  | nil => simp only [List.nil_append, mapFindVal, nat_beq_refl, ↓reduceIte]
  | cons kv rest ih =>
      rcases kv with ⟨ek, ev⟩
      simp only [specMapHasKey, Bool.or_eq_false_iff] at hnot
      obtain ⟨hne, hrest⟩ := hnot
      simp only [List.cons_append, mapFindVal, hne, Bool.false_eq_true, ↓reduceIte, ih hrest]

private theorem listNthVal_append_hit (pre post : List ValTree) (v : ValTree) :
    listNthVal (pre ++ v :: post) pre.length = some v := by
  induction pre with
  | nil => rfl
  | cons x rest ih => exact ih

private theorem listNthVal_oob (xs : List ValTree) (k : Nat) :
    listNthVal xs (xs.length + k) = none := by
  induction xs with
  | nil => rfl
  | cons x rest ih =>
      have h : (x :: rest).length + k = (rest.length + k) + 1 := by simp; omega
      rw [h]
      exact ih

theorem L30_lens_tier_bounds_and_extract_field : L30_Prop := by
  intro t k a idx v preMap postMap preArr postArr seg p1 p2
  refine ⟨rfl, ?_, rfl, extractFieldOpt_append p1 p2 (some v), ?_, ?_, ?_, rfl, rfl, ?_⟩
  · have h : ¬ (4 + k <= 3) := by omega
    simp only [isValidLensTier, h, decide_false]
  · intro hnot
    simp only [extractField, extractFieldOpt, extractStep, mapFindVal_append_hit preMap postMap k v hnot]
  · simp only [extractField, extractFieldOpt, extractStep, listNthVal_append_hit preArr postArr v]
  · simp only [extractField, extractFieldOpt, extractStep, listNthVal_oob preArr k]
  · cases seg <;> simp only [extractField, extractFieldOpt, extractStep, extractFieldOpt_none]

-- =============================================================================
-- GROUP 3 PROOFS: PLANE A CANONICAL WIRE, FLOAT64, MAP/DICT, SENTINELS, RCID
-- =============================================================================

theorem L09_canon_roundtrip : L09_Prop := by
  intro lim ulim v
  cases v with
  | lPos n =>
      by_cases h : n < lim
      · simp [encodeCanonInt, decodeCanonInt, h]
      · simp [encodeCanonInt, decodeCanonInt, h]
  | lNeg m => rfl
  | lBigUint d =>
      have hnot : ¬ (ulim + d < ulim) := by omega
      have hsub : ulim + d - ulim = d := by omega
      simp [encodeCanonInt, decodeCanonInt, hnot, hsub]

theorem L10_verify_iff_decode : L10_Prop := by
  intro lim ulim w
  constructor
  · cases w <;> rfl
  · cases w with
    | wFix n => by_cases h : n < lim <;> simp [verifyCanonIntZeroAlloc, decodeCanonInt, h]
    | wTagIntPos n => by_cases h : n < lim <;> simp [verifyCanonIntZeroAlloc, decodeCanonInt, h]
    | wTagIntNeg m => rfl
    | wTagUint u => by_cases h : u < ulim <;> simp [verifyCanonIntZeroAlloc, decodeCanonInt, h]

theorem L11_canon_wire_unique : L11_Prop := by
  intro lim ulim w v hdec
  cases w with
  | wFix n =>
      by_cases h : n < lim
      · simp [decodeCanonInt, h] at hdec; subst hdec; simp [encodeCanonInt, h]
      · simp [decodeCanonInt, h] at hdec
  | wTagIntPos n =>
      by_cases h : n < lim
      · simp [decodeCanonInt, h] at hdec
      · simp [decodeCanonInt, h] at hdec; subst hdec; simp [encodeCanonInt, h]
  | wTagIntNeg m =>
      simp [decodeCanonInt] at hdec; subst hdec; rfl
  | wTagUint u =>
      by_cases h : u < ulim
      · simp [decodeCanonInt, h] at hdec
      · simp [decodeCanonInt, h] at hdec; subst hdec
        have hadd : ulim + (u - ulim) = u := by omega
        simp [encodeCanonInt, hadd]

private theorem verifyMapKeysFrom_eq_spec (ks : List Nat) (minA nkeys : Nat) :
    verifyMapKeysFrom ks minA nkeys = specStrictlyAscFrom ks minA nkeys := by
  induction ks generalizing minA with
  | nil => rfl
  | cons k rest ih => simp [verifyMapKeysFrom, specStrictlyAscFrom, ih]

private theorem allDictKeysUsed_eq_spec (nkeys : Nat) (refs : List Nat) :
    allDictKeysUsed nkeys refs = specAllLtIn nkeys refs := by
  induction nkeys with
  | zero => rfl
  | succ pk ih => simp [allDictKeysUsed, specAllLtIn, ih]

private theorem verifyMapKeysFrom_oob (pfx tl : List Nat) (minA nkeys d : Nat) :
    verifyMapKeysFrom (pfx ++ (nkeys + d) :: tl) minA nkeys = false := by
  induction pfx generalizing minA with
  | nil =>
      have h : ¬ (nkeys + d < nkeys) := by omega
      simp [verifyMapKeysFrom, h]
  | cons p rest ih =>
      simp [verifyMapKeysFrom, ih (p + 1)]

private theorem verifyMapKeysFrom_unsorted (pfx tl : List Nat) (minA nkeys b d : Nat) :
    verifyMapKeysFrom (pfx ++ (b + d) :: b :: tl) minA nkeys = false := by
  induction pfx generalizing minA with
  | nil =>
      have h : ¬ (b + d + 1 <= b) := by omega
      simp [verifyMapKeysFrom, h]
  | cons p rest ih =>
      simp [verifyMapKeysFrom, ih (p + 1)]

private theorem allDictKeysUsed_missing (nkeys m : Nat) (refs : List Nat)
    (hlt : m < nkeys) (hnot : refs.contains m = false) :
    allDictKeysUsed nkeys refs = false := by
  induction nkeys with
  | zero => omega
  | succ pk ih =>
      by_cases heq : m = pk
      · subst heq; simp only [allDictKeysUsed, hnot]; rfl
      · have hlt' : m < pk := by omega
        simp only [allDictKeysUsed, ih hlt', Bool.and_false]

theorem L22_map_keys_and_dict_all_used : L22_Prop := by
  intro nkeys b d m ks pfx tl refs
  refine ⟨verifyMapKeysFrom_eq_spec ks 0 nkeys, allDictKeysUsed_eq_spec nkeys refs,
    verifyMapKeysFrom_oob pfx tl 0 nkeys d, verifyMapKeysFrom_unsorted pfx tl 0 nkeys b d, ?_⟩
  intro hlt hnot
  exact allDictKeysUsed_missing nkeys m refs hlt hnot

theorem L31_canon_float64_bijective_and_idempotent : L31_Prop := by
  intro f p
  refine ⟨?_, ?_, ?_, rfl, rfl⟩
  · cases f <;> rfl
  · cases f <;> rfl
  · cases f <;> rfl

theorem L32_json_sentinel_bijectivity_and_fail_closed : L32_Prop := by
  intro v r c a dt kk p
  refine ⟨?_, rfl, rfl, rfl, rfl⟩
  cases v <;> rfl

private theorem lookupTensor_eq_spec (ts : List TensorDesc) (i : Nat) :
    lookupTensor ts i = specLookupTensor ts i := by
  induction ts generalizing i with
  | nil => rfl
  | cons h t ih =>
      cases i with
      | zero => rfl
      | succ pi => exact ih pi

private theorem resolveTensorRefs_eq_spec (refs : List Nat) (ts : List TensorDesc) :
    resolveTensorRefs refs ts = specResolveTensorRefs refs ts := by
  induction refs with
  | nil => rfl
  | cons idx rest ih =>
      simp only [resolveTensorRefs, specResolveTensorRefs, lookupTensor_eq_spec, ih]
      rfl

private theorem lookupTensor_append (ts extra : List TensorDesc) (i : Nat) (td : TensorDesc)
    (h : lookupTensor ts i = some td) :
    lookupTensor (ts ++ extra) i = some td := by
  induction ts generalizing i with
  | nil => cases h
  | cons head tl ih =>
      cases i with
      | zero => exact h
      | succ pi => exact ih pi h

private theorem resolveTensorRefs_append (refs : List Nat) (ts extra : List TensorDesc)
    (bound : List BoundTensor) (h : resolveTensorRefs refs ts = some bound) :
    resolveTensorRefs refs (ts ++ extra) = some bound := by
  induction refs generalizing bound with
  | nil => exact h
  | cons idx rest ih =>
      simp only [resolveTensorRefs] at h ⊢
      cases hl : lookupTensor ts idx with
      | none => simp [hl] at h
      | some td =>
          cases hr : resolveTensorRefs rest ts with
          | none => simp [hl, hr] at h
          | some more =>
              simp [hl, hr] at h
              rw [lookupTensor_append ts extra idx td hl, ih more hr]
              simp [h]

private theorem lookupTensor_oob (ts : List TensorDesc) (d : Nat) :
    lookupTensor ts (ts.length + d) = none := by
  induction ts with
  | nil => rfl
  | cons hd tl ih =>
      have h : (hd :: tl).length + d = (tl.length + d) + 1 := by simp; omega
      rw [h]; exact ih

private theorem resolveTensorRefs_oob (rPre rTail : List Nat) (ts : List TensorDesc) (d : Nat) :
    resolveTensorRefs (rPre ++ (ts.length + d) :: rTail) ts = none := by
  induction rPre with
  | nil =>
      simp only [List.nil_append, resolveTensorRefs, lookupTensor_oob]
  | cons idx rest ih =>
      simp only [List.cons_append, resolveTensorRefs, ih]
      cases lookupTensor ts idx <;> rfl

private theorem lookupTensor_at_len (pre post : List TensorDesc) (td : TensorDesc) :
    lookupTensor (pre ++ td :: post) pre.length = some td := by
  induction pre with
  | nil => rfl
  | cons _ tl ih => exact ih

private theorem resolveTensorRefs_contains_idx (rPre rTail : List Nat) (idx : Nat)
    (ts : List TensorDesc) (td : TensorDesc) (bound : List BoundTensor)
    (hl : lookupTensor ts idx = some td)
    (hr : resolveTensorRefs (rPre ++ idx :: rTail) ts = some bound) :
    ∃ preB postB, bound = preB ++ { idx := idx, td := td } :: postB ∧ preB.length = rPre.length := by
  induction rPre generalizing bound with
  | nil =>
      simp only [List.nil_append, resolveTensorRefs, hl] at hr
      cases htail : resolveTensorRefs rTail ts with
      | none => simp [htail] at hr
      | some more =>
          simp [htail] at hr
          exact ⟨[], more, hr.symm, rfl⟩
  | cons x rest ih =>
      simp only [List.cons_append, resolveTensorRefs] at hr
      cases hx : lookupTensor ts x with
      | none => simp [hx] at hr
      | some xtd =>
          cases hrest : resolveTensorRefs (rest ++ idx :: rTail) ts with
          | none => simp [hx, hrest] at hr
          | some more =>
              simp [hx, hrest] at hr
              obtain ⟨preB, postB, heq, hlen⟩ := ih more hrest
              refine ⟨{ idx := x, td := xtd } :: preB, postB, ?_, by simp [hlen]⟩
              rw [← hr, heq]; rfl

theorem L33_resolved_frame_cid_repack_and_binding : L33_Prop := by
  intro fcid l m d refs rPre rTail ts pre post extra r
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · simp only [resolvedFrameCid, specResolvedFrameCid, resolveTensorRefs_eq_spec]
    rfl
  · intro h
    simp only [resolvedFrameCid] at h ⊢
    cases hr : resolveTensorRefs refs ts with
    | none => simp [hr] at h
    | some bound =>
        simp [hr] at h
        rw [resolveTensorRefs_append refs ts extra bound hr]
        simp [h]
  · simp only [resolvedFrameCid, resolveTensorRefs_oob]
  · intro t ht heq
    rw [ht] at heq
    simp only [resolvedFrameCid] at ht heq
    cases hr : resolveTensorRefs refs ts with
    | none => simp [hr] at ht
    | some bound =>
        simp [hr] at ht heq
        have hfc : t.frameCid = fcid := by rw [← ht]
        have hfc2 : t.frameCid = fcid + d + 1 := by rw [heq]
        omega
  · cases h1 : resolveTensorRefs (rPre ++ pre.length :: rTail) (pre ++ { byteLen := l, merkle4m := m } :: post) with
    | none =>
        right
        simp [resolvedFrameCid, h1]
    | some b1 =>
        left
        intro heq
        simp only [resolvedFrameCid, h1] at heq
        cases h2 : resolveTensorRefs (rPre ++ pre.length :: rTail) (pre ++ { byteLen := l + d + 1, merkle4m := m } :: post) with
        | none => simp [h2] at heq
        | some b2 =>
            simp [h2] at heq
            obtain ⟨p1, q1, hb1, hl1⟩ := resolveTensorRefs_contains_idx rPre rTail pre.length _ _ b1
              (lookupTensor_at_len pre post { byteLen := l, merkle4m := m }) h1
            obtain ⟨p2, q2, hb2, hl2⟩ := resolveTensorRefs_contains_idx rPre rTail pre.length _ _ b2
              (lookupTensor_at_len pre post { byteLen := l + d + 1, merkle4m := m }) h2
            have heq' : p1 ++ { idx := pre.length, td := { byteLen := l, merkle4m := m } } :: q1 =
                p2 ++ { idx := pre.length, td := { byteLen := l + d + 1, merkle4m := m } } :: q2 := by
              rw [← hb1, ← hb2, heq]
            have hlen : p1.length = p2.length := hl1.trans hl2.symm
            have htail : ({ idx := pre.length, td := { byteLen := l, merkle4m := m } } :: q1) =
                ({ idx := pre.length, td := { byteLen := l + d + 1, merkle4m := m } } :: q2) :=
              (List.append_inj heq' hlen).2
            injection htail with hhd _
            have hbl : l = l + d + 1 := congrArg (fun bt => bt.td.byteLen) hhd
            omega
  · intro h
    simp only [resolvedFrameCid, resolveTensorRefs, lookupTensor_at_len] at h
    injection h with h1
    have hbl : l = l + d + 1 := by
      have := congrArg (fun rc => match rc.boundTensors with | bt :: _ => bt.td.byteLen | [] => 0) h1
      simpa using this
    omega
  · intro h
    simp only [resolvedFrameCid, resolveTensorRefs, lookupTensor_at_len] at h
    injection h with h1
    have hm : m = m + d + 1 := by
      have := congrArg (fun rc => match rc.boundTensors with | bt :: _ => bt.td.merkle4m | [] => 0) h1
      simpa using this
    omega

-- =============================================================================
-- GROUP 4 PROOFS: CWZM BINARY-SKIP, MERKLE, WAL, LIVEIVFSTORE & CWSP
-- =============================================================================

private theorem int_le_max_left (a b : Int) : a <= max a b := by
  change a <= if a <= b then b else a
  split <;> omega

private theorem int_le_max_right (a b : Int) : b <= max a b := by
  change b <= if a <= b then b else a
  split <;> omega

private theorem int_min_le_left (a b : Int) : min a b <= a := by
  change (if a <= b then a else b) <= a
  split <;> omega

private theorem int_min_le_right (a b : Int) : min a b <= b := by
  change (if a <= b then a else b) <= b
  split <;> omega

private theorem zoneOverlaps_extend_pres (z : Zone) (k lo hi : Int)
    (h : zoneOverlaps z lo hi = true) :
    zoneOverlaps (zoneExtend z k) lo hi = true := by
  simp only [zoneOverlaps, zoneExtend, Bool.and_eq_true, decide_eq_true_eq] at h ⊢
  exact ⟨decide_eq_true (Int.le_trans h.1 (int_le_max_left z.maxK k)),
         decide_eq_true (Int.le_trans (int_min_le_left z.minK k) h.2)⟩

private theorem zoneOverlaps_extend_hit (z : Zone) (k lo hi : Int)
    (h : specInRange k lo hi = true) :
    zoneOverlaps (zoneExtend z k) lo hi = true := by
  simp only [specInRange, zoneOverlaps, zoneExtend, Bool.and_eq_true, decide_eq_true_eq] at h ⊢
  exact ⟨decide_eq_true (Int.le_trans h.1 (int_le_max_right z.maxK k)),
         decide_eq_true (Int.le_trans (int_min_le_right z.minK k) h.2)⟩

private theorem buildZoneLoop_mono (ks : List Int) (z : Zone) (lo hi : Int)
    (h : (zoneOverlaps z lo hi || ks.any (fun k => specInRange k lo hi)) = true) :
    zoneOverlaps (buildZoneLoop ks z) lo hi = true := by
  induction ks generalizing z with
  | nil => simpa [buildZoneLoop] using h
  | cons k rest ih =>
      simp only [buildZoneLoop, List.any_cons, Bool.or_eq_true] at h ⊢
      rcases h with hz | hk | hrest
      · exact ih (zoneExtend z k) (by simp [zoneOverlaps_extend_pres z k lo hi hz])
      · exact ih (zoneExtend z k) (by simp [zoneOverlaps_extend_hit z k lo hi hk])
      · exact ih (zoneExtend z k) (by simp [hrest])

theorem L12_zonemap_no_false_neg : L12_Prop := by
  intro mn mx k0 lo hi ks
  constructor
  · rfl
  · intro hhit
    apply buildZoneLoop_mono ks { minK := k0, maxK := k0 } lo hi
    simpa [specPageHasInRange, zoneOverlaps, specInRange] using hhit

private theorem specFilterZonesAt_above_hi_nil (zs : List Zone) (idx : Nat) (prevMax lo hi : Int)
    (hdj : specZonesDisjointFrom zs prevMax = true) (hgt : ¬ (prevMax <= hi)) :
    specFilterZonesAt zs idx lo hi = [] := by
  induction zs generalizing idx prevMax with
  | nil => rfl
  | cons z rest ih =>
      simp only [specZonesDisjointFrom, Bool.and_eq_true, decide_eq_true_eq] at hdj
      obtain ⟨⟨hpm, hmnmx⟩, hrest⟩ := hdj
      have hmn : ¬ (z.minK <= hi) := by omega
      have hmx : ¬ (z.maxK <= hi) := by omega
      simp only [specFilterZonesAt, hmn, decide_false, Bool.and_false, Bool.false_eq_true, ↓reduceIte]
      exact ih (idx + 1) z.maxK hrest hmx

private theorem takeUntilGtHi_eq_filter (zs : List Zone) (idx : Nat) (prevMax lo hi : Int)
    (hdj : specZonesDisjointFrom zs prevMax = true) (hle : lo <= prevMax) :
    takeUntilGtHi zs idx hi = specFilterZonesAt zs idx lo hi := by
  induction zs generalizing idx prevMax with
  | nil => rfl
  | cons z rest ih =>
      simp only [specZonesDisjointFrom, Bool.and_eq_true, decide_eq_true_eq] at hdj
      obtain ⟨⟨hpm, hmnmx⟩, hrest⟩ := hdj
      have hlomx : lo <= z.maxK := by omega
      by_cases hmnhi : z.minK <= hi
      · simp only [takeUntilGtHi, specFilterZonesAt, hmnhi, hlomx, decide_true, Bool.and_self, ↓reduceIte]
        congr 1
        exact ih (idx + 1) z.maxK hrest hlomx
      · have hmxhi : ¬ (z.maxK <= hi) := by omega
        simp only [takeUntilGtHi, specFilterZonesAt, hmnhi, decide_false, Bool.and_false, Bool.false_eq_true, ↓reduceIte]
        exact (specFilterZonesAt_above_hi_nil rest (idx + 1) z.maxK lo hi hrest hmxhi).symm

private theorem queryKeyRangeAt_eq_filter (zs : List Zone) (idx : Nat) (prevMax lo hi : Int)
    (hdj : specZonesDisjointFrom zs prevMax = true) :
    queryKeyRangeAt zs idx lo hi = specFilterZonesAt zs idx lo hi := by
  induction zs generalizing idx prevMax with
  | nil => rfl
  | cons z rest ih =>
      simp only [specZonesDisjointFrom, Bool.and_eq_true, decide_eq_true_eq] at hdj
      obtain ⟨⟨hpm, hmnmx⟩, hrest⟩ := hdj
      by_cases hlt : z.maxK < lo
      · have hnotle : ¬ (lo <= z.maxK) := by omega
        simp only [queryKeyRangeAt, specFilterZonesAt, hlt, hnotle, decide_false, Bool.false_and, Bool.false_eq_true, ↓reduceIte]
        exact ih (idx + 1) z.maxK hrest
      · have hlomx : lo <= z.maxK := by omega
        simp only [queryKeyRangeAt, hlt, ↓reduceIte]
        by_cases hmnhi : z.minK <= hi
        · simp only [takeUntilGtHi, specFilterZonesAt, hmnhi, hlomx, decide_true, Bool.and_self, ↓reduceIte]
          congr 1
          exact takeUntilGtHi_eq_filter rest (idx + 1) z.maxK lo hi hrest hlomx
        · have hmxhi : ¬ (z.maxK <= hi) := by omega
          simp only [takeUntilGtHi, specFilterZonesAt, hmnhi, decide_false, Bool.and_false, Bool.false_eq_true, ↓reduceIte]
          exact (specFilterZonesAt_above_hi_nil rest (idx + 1) z.maxK lo hi hrest hmxhi).symm

theorem L23_sorted_zonemap_early_break_exact : L23_Prop := by
  intro zs lo hi
  constructor
  · intro hlohi hdj
    simp only [queryKeyRangePages, hlohi, ↓reduceIte]
    cases zs with
    | nil => rfl
    | cons z rest =>
        simp only [specZonesDisjoint, Bool.and_eq_true, decide_eq_true_eq] at hdj
        obtain ⟨hmnmx, hrest⟩ := hdj
        have hdj' : specZonesDisjointFrom (z :: rest) z.minK = true := by
          simp [specZonesDisjointFrom, hmnmx, hrest]
        exact queryKeyRangeAt_eq_filter (z :: rest) 0 z.minK lo hi hdj'
  · intro hgt
    have hnot : ¬ (lo <= hi) := by omega
    simp only [queryKeyRangePages, hnot, ↓reduceIte]

theorem L13_merkle_domain_and_odd_safe : L13_Prop := by
  intro f1 f2 t0 trest a b c
  refine ⟨rfl, rfl, ?_, ?_, ?_, ?_, ?_⟩
  · simp [shardMerkleRoot]
  · simp [merkleTree, merklePairLevel]
  · simp [merkleTree]
  · simp [merkleTree]
  · simp [merkleTree, merklePairLevel]

theorem L34_merkle_leaf_and_tensor_sensitivity : L34_Prop := by
  intro a b d bl m4m fs
  refine ⟨?_, ?_, ?_, ?_⟩
  · intro h; simp [merkleTree, merklePairLevel] at h; omega
  · intro h; simp [merkleTree, merklePairLevel] at h; omega
  · intro h; simp [shardMerkleRoot, merkleTree] at h; omega
  · intro h; simp [shardMerkleRoot, merkleTree] at h; omega

private theorem walRecoverMode_eq_spec (recs : List WalRec) (s : Nat) (mode : Bool) :
    walRecoverMode recs s mode = specWalRecoverMode recs s mode := by
  induction recs generalizing s with
  | nil => rfl
  | cons r rest ih =>
      cases hv : r.valid
      · cases rest <;> simp [walRecoverMode, specWalRecoverMode, hv]
      · simp only [walRecoverMode, specWalRecoverMode, hv, ↓reduceIte, ih (s + 1)]
        rfl

private theorem walRecover_clean (pfx : List Nat) (s : Nat) (mode : Bool) :
    walRecoverMode (specWalValidPrefix pfx s []) s mode = .walOk pfx := by
  induction pfx generalizing s with
  | nil => rfl
  | cons p rest ih =>
      simp only [specWalValidPrefix, walRecoverMode, nat_beq_refl, ↓reduceIte, ih (s + 1)]

private theorem walRecover_torn_ok (pfx : List Nat) (s tornSeq tp : Nat) :
    walRecoverMode (specWalValidPrefix pfx s [{ valid := false, seq := tornSeq, payload := tp }]) s true = .walOk pfx := by
  induction pfx generalizing s with
  | nil => rfl
  | cons p rest ih =>
      simp only [specWalValidPrefix, walRecoverMode, nat_beq_refl, ↓reduceIte, ih (s + 1)]

private theorem walRecover_torn_strict (pfx : List Nat) (s tornSeq tp : Nat) :
    walRecoverMode (specWalValidPrefix pfx s [{ valid := false, seq := tornSeq, payload := tp }]) s false = .walErr := by
  induction pfx generalizing s with
  | nil => rfl
  | cons p rest ih =>
      simp only [specWalValidPrefix, walRecoverMode, nat_beq_refl, ↓reduceIte, ih (s + 1)]

private theorem walRecover_mid_err (pfx : List Nat) (s tornSeq bp : Nat) (after : WalRec) (more : List WalRec) (mode : Bool) :
    walRecoverMode (specWalValidPrefix pfx s ({ valid := false, seq := tornSeq, payload := bp } :: after :: more)) s mode = .walErr := by
  induction pfx generalizing s with
  | nil => rfl
  | cons p rest ih =>
      simp only [specWalValidPrefix, walRecoverMode, nat_beq_refl, ↓reduceIte, ih (s + 1)]

private theorem walRecover_bad_seq_err (pfx : List Nat) (s badSeq tp : Nat) (mode : Bool)
    (hne : badSeq ≠ s + pfx.length) :
    walRecoverMode (specWalValidPrefix pfx s [{ valid := true, seq := badSeq, payload := tp }]) s mode = .walErr := by
  induction pfx generalizing s with
  | nil =>
      have hneq : (badSeq == s) = false := by
        cases h : (badSeq == s)
        · rfl
        · have heq := nat_eq_of_beq h
          simp at hne
          exact absurd heq hne
      simp only [specWalValidPrefix, walRecoverMode, hneq, Bool.false_eq_true, ↓reduceIte]
  | cons p rest ih =>
      have hne' : badSeq ≠ (s + 1) + rest.length := by simp at hne; omega
      simp only [specWalValidPrefix, walRecoverMode, nat_beq_refl, ↓reduceIte, ih (s + 1) hne']

theorem L14_wal_torn_vs_mid_tamper : L14_Prop := by
  intro recs pfx startSeq tornSeq badSeq tp bp after more mode
  exact ⟨walRecoverMode_eq_spec recs startSeq mode,
         walRecover_clean pfx startSeq mode,
         walRecover_torn_ok pfx startSeq tornSeq tp,
         walRecover_torn_strict pfx startSeq tornSeq tp,
         walRecover_mid_err pfx startSeq tornSeq bp after more mode,
         fun hne => walRecover_bad_seq_err pfx startSeq badSeq tp mode hne⟩

private theorem walReplayLive_append (ops1 ops2 : List WalOp) (live : List Nat) :
    walReplayLive (ops1 ++ ops2) live = walReplayLive ops2 (walReplayLive ops1 live) := by
  induction ops1 generalizing live with
  | nil => rfl
  | cons op rest ih =>
      cases op with
      | opFrame c => exact ih (removeCid live c ++ [c])
      | opTomb c  => exact ih (removeCid live c)

private theorem mem_removeCid (xs : List Nat) (x c : Nat) :
    x ∈ removeCid xs c ↔ x ∈ xs ∧ x ≠ c := by
  simp [removeCid, List.mem_filter]

private theorem count_removeCid_self (xs : List Nat) (c : Nat) :
    (removeCid xs c).count c = 0 := by
  rw [List.count_eq_zero]
  intro hmem
  exact ((mem_removeCid xs c c).mp hmem).2 rfl

theorem L24_wal_tombstone_reinsert_and_statepage_rt : L24_Prop := by
  intro ops1 ops2 live c b expRt fileRt
  refine ⟨walReplayLive_append ops1 ops2 live, ?_, ?_, ?_, ?_, ?_⟩
  · rw [walReplayLive_append]
    simp only [walReplayLive]
    intro hmem
    exact ((mem_removeCid _ c c).mp hmem).2 rfl
  · intro hbc
    rw [walReplayLive_append]
    simp only [walReplayLive, mem_removeCid]
    exact ⟨fun h => h.1, fun h => ⟨h, hbc⟩⟩
  · rw [walReplayLive_append]
    simp only [walReplayLive, List.count_append, count_removeCid_self]
    simp
  · intro hbc
    rw [walReplayLive_append]
    simp only [walReplayLive, List.mem_append, mem_removeCid, List.mem_singleton]
    exact ⟨fun h => h.elim (fun h1 => h1.1) (fun heq => absurd heq hbc),
           fun h => Or.inl ⟨h, hbc⟩⟩
  · cases expRt <;> cases fileRt <;> rfl

private theorem ivfMakeSeqRecs_append (a b : List Nat) (s0 : Nat) :
    ivfMakeSeqRecs (a ++ b) s0 = ivfMakeSeqRecs a s0 ++ ivfMakeSeqRecs b (s0 + a.length) := by
  induction a generalizing s0 with
  | nil => simp [ivfMakeSeqRecs]
  | cons v rest ih =>
      have hlen : s0 + (v :: rest).length = (s0 + 1) + rest.length := by simp; omega
      rw [hlen]
      simp [ivfMakeSeqRecs, ih (s0 + 1)]

private theorem ivfWalFilterFrom_append (xs ys : List IvfWalRec) (w : Nat) :
    ivfWalFilterFrom (xs ++ ys) w = ivfWalFilterFrom xs w ++ ivfWalFilterFrom ys w := by
  induction xs with
  | nil => rfl
  | cons r rest ih =>
      by_cases h : w <= r.seq
      · simp [ivfWalFilterFrom, h, ih]
      · simp [ivfWalFilterFrom, h, ih]

private theorem ivfWalFilterFrom_all_ge (ids : List Nat) (s w : Nat) (h : w <= s) :
    ivfWalFilterFrom (ivfMakeSeqRecs ids s) w = ids := by
  induction ids generalizing s with
  | nil => rfl
  | cons v rest ih =>
      simp [ivfMakeSeqRecs, ivfWalFilterFrom, h, ih (s + 1) (by omega)]

private theorem ivfWalFilterFrom_all_lt (ids : List Nat) (s : Nat) :
    ivfWalFilterFrom (ivfMakeSeqRecs ids s) (s + ids.length) = [] := by
  induction ids generalizing s with
  | nil => rfl
  | cons v rest ih =>
      have hnot : ¬ (s + (v :: rest).length <= s) := by simp
      have hlen : s + (v :: rest).length = (s + 1) + rest.length := by simp; omega
      simp only [ivfMakeSeqRecs, ivfWalFilterFrom, hnot, ↓reduceIte]
      rw [hlen]
      exact ih (s + 1)

theorem L35_live_ivf_compaction_crash_invariant : L35_Prop := by
  intro base compIds postIds s0
  refine ⟨?_, ?_, ?_⟩
  · simp only [liveIvfRecover]
    rw [ivfWalFilterFrom_all_ge (compIds ++ postIds) s0 s0 (Nat.le_refl s0), List.append_assoc]
  · simp only [liveIvfRecover]
    rw [ivfMakeSeqRecs_append, ivfWalFilterFrom_append,
        ivfWalFilterFrom_all_lt compIds s0,
        ivfWalFilterFrom_all_ge postIds (s0 + compIds.length) (s0 + compIds.length) (Nat.le_refl _)]
    simp
  · simp only [liveIvfRecover]
    rw [ivfWalFilterFrom_all_ge postIds (s0 + compIds.length) (s0 + compIds.length) (Nat.le_refl _)]

theorem L36_statepage_hmac_precedence : L36_Prop := by
  intro k mv hv
  cases mv <;> cases hv <;> refine ⟨rfl, rfl, rfl, rfl, rfl, rfl, rfl, rfl, rfl⟩

-- =============================================================================
-- GROUP 5 PROOFS: LEAN-EXCLUSIVE STRENGTHENINGS (`L37`..`L39`)
-- =============================================================================

private theorem bitvec8_tainted_or_bit7 (b : BitVec 8) :
    ((b ||| 0x80#8) &&& 0x84#8) ≠ 0#8 := by
  intro h
  have h7 := congrArg (fun v => v.getLsbD 7) h
  simp at h7

private theorem uint8_isTainted_or_bit7 (b : UInt8) :
    isTaintedByte (b ||| BIT_TAINTED) = true := by
  simp only [isTaintedByte, bne]
  cases h : (((b ||| BIT_TAINTED) &&& (BIT_TAINTED ||| AUTH_UNTRUSTED_EXT)) == 0)
  · rfl
  · have heq : ((b ||| BIT_TAINTED) &&& (BIT_TAINTED ||| AUTH_UNTRUSTED_EXT)) = 0 :=
      of_decide_eq_true h
    have hbv : ((b.toBitVec ||| 0x80#8) &&& (0x80#8 ||| 0x04#8)) = 0#8 :=
      congrArg UInt8.toBitVec heq
    exact False.elim (bitvec8_tainted_or_bit7 b.toBitVec (by simpa using hbv))

private theorem uint8_or_bit7_and_7F (b : UInt8) :
    ((b ||| BIT_TAINTED) &&& 0x7F) = (b &&& 0x7F) := by
  apply UInt8.eq_of_toBitVec_eq
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  simp only [UInt8.toBitVec_and, UInt8.toBitVec_or, BIT_TAINTED,
    BitVec.getLsbD_and, BitVec.getLsbD_or]
  have h7 : (0x80#8 : BitVec 8).getLsbD i = false ∨ (0x7F#8 : BitVec 8).getLsbD i = false := by
    have : i = 0 ∨ i = 1 ∨ i = 2 ∨ i = 3 ∨ i = 4 ∨ i = 5 ∨ i = 6 ∨ i = 7 := by omega
    rcases this with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;> decide
  rcases h7 with h0 | h1
  · simp [h0]
  · simp [h1]

private theorem testBit_pow2_sub1 (k : Nat) : ∀ i, i < k → (2 ^ k - 1).testBit i = true := by
  induction k with
  | zero => intro i hi; omega
  | succ m ih =>
      intro i hi
      have hpos : 0 < 2 ^ m := Nat.pow_pos (by omega)
      have hval : 2 ^ (m + 1) - 1 = 2 * (2 ^ m - 1) + 1 := by
        rw [Nat.pow_succ]; omega
      rw [hval]
      cases i with
      | zero =>
          simp [Nat.testBit, Nat.shiftRight_zero]
      | succ j =>
          have hdiv : (2 * (2 ^ m - 1) + 1) / 2 = 2 ^ m - 1 := by omega
          rw [Nat.testBit_succ, hdiv]
          exact ih j (by omega)

private theorem bitvec64_allOnes_getLsbD (i : Nat) (hi : i < 64) :
    (0xFFFFFFFFFFFFFFFF#64 : BitVec 64).getLsbD i = true := by
  show Nat.testBit (2 ^ 64 - 1) i = true
  exact testBit_pow2_sub1 64 i hi

private theorem bitvec64_neg_lsb (u : BitVec 64) (i : Nat) (hi : i < 64) :
    (0#64 - (u &&& 1#64)).getLsbD i = u.getLsbD 0 := by
  have hcases : (u &&& 1#64) = 0#64 ∨ (u &&& 1#64) = 1#64 := by
    cases h0 : u.getLsbD 0
    · left
      apply BitVec.eq_of_getLsbD_eq
      intro k hk
      cases k with
      | zero =>
          simp only [BitVec.getLsbD_and, BitVec.getLsbD_zero]
          rw [h0]; rfl
      | succ pk =>
          have h1 : (1#64 : BitVec 64).getLsbD (pk + 1) = false := by
            simp [BitVec.getLsbD_one]
          simp [h1]
    · right
      apply BitVec.eq_of_getLsbD_eq
      intro k hk
      cases k with
      | zero =>
          simp only [BitVec.getLsbD_and]
          rw [h0]; decide
      | succ pk =>
          have h1 : (1#64 : BitVec 64).getLsbD (pk + 1) = false := by
            simp [BitVec.getLsbD_one]
          simp [h1]
  rcases hcases with hz | ho
  · have h0 : u.getLsbD 0 = false := by
      have := congrArg (fun v => v.getLsbD 0) hz
      simpa using this
    rw [hz, h0]; simp
  · have h0 : u.getLsbD 0 = true := by
      have := congrArg (fun v => v.getLsbD 0) ho
      simpa using this
    rw [ho, h0]
    have hall : (0#64 - 1#64 : BitVec 64) = 0xFFFFFFFFFFFFFFFF#64 := by decide
    rw [hall]
    exact bitvec64_allOnes_getLsbD i hi

private theorem nat_testBit_63_of_lt (n : Nat) (h : n < 2 ^ 63) : n.testBit 63 = false := by
  rw [Nat.testBit_eq_decide_div_mod_eq]
  have hd : n / 2 ^ 63 = 0 := Nat.div_eq_of_lt h
  simp [hd]

private theorem nat_testBit_63_of_ge (n : Nat) (h1 : 2 ^ 63 <= n) (h2 : n < 2 ^ 64) : n.testBit 63 = true := by
  rw [Nat.testBit_eq_decide_div_mod_eq]
  have hd : n / 2 ^ 63 = 1 := by
    have : 2 ^ 64 = 2 * 2 ^ 63 := by decide
    omega
  simp [hd]

private theorem bitvec64_sshift63 (x : BitVec 64) (i : Nat) (hi : i < 64) :
    (x.sshiftRight 63).getLsbD i = x.getLsbD 63 := by
  have hlt64 : x.toNat < 2 ^ 64 := x.isLt
  by_cases hlt : x.toNat < 2 ^ 63
  · have hmsb : x.getLsbD 63 = false := nat_testBit_63_of_lt x.toNat hlt
    have htoInt : x.toInt = Int.ofNat x.toNat := by
      simp only [BitVec.toInt]
      have hhalf : 2 * x.toNat < 2 ^ 64 := by
        have : 2 ^ 64 = 2 * 2 ^ 63 := by decide
        omega
      simp only [hhalf, ↓reduceIte]
      rfl
    have hsr : x.sshiftRight 63 = 0#64 := by
      simp only [BitVec.sshiftRight, htoInt]
      have hdiv : Int.ofNat x.toNat >>> 63 = 0 := by
        change Int.ofNat (x.toNat >>> 63) = 0
        rw [Nat.shiftRight_eq_div_pow, Nat.div_eq_of_lt hlt]
        rfl
      rw [hdiv]; rfl
    rw [hsr, hmsb]; simp
  · have hge : 2 ^ 63 <= x.toNat := by omega
    have hmsb : x.getLsbD 63 = true := nat_testBit_63_of_ge x.toNat hge hlt64
    have htoInt : x.toInt = Int.negSucc (2 ^ 64 - 1 - x.toNat) := by
      simp only [BitVec.toInt]
      have hnhalf : ¬ (2 * x.toNat < 2 ^ 64) := by
        have : 2 ^ 64 = 2 * 2 ^ 63 := by decide
        omega
      simp only [hnhalf, ↓reduceIte]
      omega
    have hsr : x.sshiftRight 63 = 0xFFFFFFFFFFFFFFFF#64 := by
      simp only [BitVec.sshiftRight, htoInt]
      have hrem : 2 ^ 64 - 1 - x.toNat < 2 ^ 63 := by
        have : 2 ^ 64 = 2 * 2 ^ 63 := by decide
        omega
      have hdiv : Int.negSucc (2 ^ 64 - 1 - x.toNat) >>> 63 = Int.negSucc 0 := by
        change Int.negSucc ((2 ^ 64 - 1 - x.toNat) >>> 63) = Int.negSucc 0
        rw [Nat.shiftRight_eq_div_pow, Nat.div_eq_of_lt hrem]
      rw [hdiv]; decide
    rw [hsr, hmsb]
    exact bitvec64_allOnes_getLsbD i hi

private theorem zigzag_roundtrip_i64 (i : Int64) :
    zigzagDecode64 (zigzagEncode64 i) = i := by
  rw [← Int64.ofBitVec_toBitVec i, zigzagDecode64]
  congr 1
  apply BitVec.eq_of_getLsbD_eq
  intro k hk
  simp only [zigzagEncode64, UInt64.toBitVec_ofBitVec]
  let x := i.toBitVec
  let e := (x <<< 1) ^^^ (x.sshiftRight 63)
  change ((e >>> 1) ^^^ (0#64 - (e &&& 1#64))).getLsbD k = x.getLsbD k
  rw [BitVec.getLsbD_xor, bitvec64_neg_lsb e k hk, BitVec.getLsbD_ushiftRight]
  have he0 : e.getLsbD 0 = x.getLsbD 63 := by
    simp only [e, BitVec.getLsbD_xor, BitVec.getLsbD_shiftLeft, bitvec64_sshift63 x 0 (by omega)]
    simp
  rw [he0]
  by_cases hlt : k < 63
  · have hsh : (1 + k) < 64 := by omega
    have hek : e.getLsbD (1 + k) = (x.getLsbD k ^^ x.getLsbD 63) := by
      simp only [e, BitVec.getLsbD_xor, BitVec.getLsbD_shiftLeft, bitvec64_sshift63 x (1 + k) hsh]
      have hsub : 1 + k - 1 = k := by omega
      simp [hsh, hsub]
    rw [hek]
    cases x.getLsbD k <;> cases x.getLsbD 63 <;> rfl
  · have hk63 : k = 63 := by omega
    subst hk63
    simp

private theorem zigzag_roundtrip_u64 (u : UInt64) :
    zigzagEncode64 (zigzagDecode64 u) = u := by
  apply UInt64.eq_of_toBitVec_eq
  apply BitVec.eq_of_getLsbD_eq
  intro k hk
  simp only [zigzagEncode64, zigzagDecode64, UInt64.toBitVec_ofBitVec, Int64.toBitVec_ofBitVec]
  let v := u.toBitVec
  let d := (v >>> 1) ^^^ (0#64 - (v &&& 1#64))
  change ((d <<< 1) ^^^ (d.sshiftRight 63)).getLsbD k = v.getLsbD k
  rw [BitVec.getLsbD_xor, BitVec.getLsbD_shiftLeft, bitvec64_sshift63 d k hk]
  have hd63 : d.getLsbD 63 = v.getLsbD 0 := by
    simp only [d, BitVec.getLsbD_xor, BitVec.getLsbD_ushiftRight, bitvec64_neg_lsb v 63 (by omega)]
    simp
  rw [hd63]
  cases k with
  | zero => simp
  | succ pk =>
      have hpk : pk < 64 := by omega
      have hdpk : d.getLsbD pk = (v.getLsbD (pk + 1) ^^ v.getLsbD 0) := by
        simp only [d, BitVec.getLsbD_xor, BitVec.getLsbD_ushiftRight, bitvec64_neg_lsb v pk hpk]
        rw [show 1 + pk = pk + 1 by omega]
      simp [hk, hdpk]

private theorem isNaNBits_zero : isNaNBits 0 = false := by decide
private theorem isNaNBits_canonQNaN : isNaNBits CANON_QNAN_BITS = true := by decide
private theorem canonQNaN_ne_negZero : (CANON_QNAN_BITS == NEG_ZERO_BITS) = false := by decide
private theorem zero_ne_negZero : ((0 : UInt64) == NEG_ZERO_BITS) = false := by decide

private theorem canonQNaN_beq_self : (CANON_QNAN_BITS == CANON_QNAN_BITS) = true := by decide

private theorem testBit_pow2_sub1_sub (k : Nat) :
    ∀ n i, n < 2 ^ k → i < k → (2 ^ k - 1 - n).testBit i = !n.testBit i := by
  induction k with
  | zero => intro _ _ _ hi; omega
  | succ m ih =>
      intro n i hn hi
      have hpos : 0 < 2 ^ m := Nat.pow_pos (by omega)
      have hpow : 2 ^ (m + 1) = 2 * 2 ^ m := by rw [Nat.pow_succ]; omega
      cases i with
      | zero =>
          simp only [Nat.testBit_zero]
          have hmod : (2 ^ (m + 1) - 1 - n) % 2 = 1 - n % 2 := by omega
          rw [hmod]
          cases hnm : n % 2 with
          | zero => rfl
          | succ r =>
              have hr : r = 0 := by omega
              subst hr; rfl
      | succ j =>
          rw [Nat.testBit_succ, Nat.testBit_succ]
          have hdiv : (2 ^ (m + 1) - 1 - n) / 2 = 2 ^ m - 1 - n / 2 := by omega
          rw [hdiv]
          exact ih (n / 2) j (by omega) (by omega)

private theorem bitvec64_xor_allOnes_eq_ofNat (y : BitVec 64) :
    (y ^^^ 0xFFFFFFFFFFFFFFFF#64) = BitVec.ofNat 64 (2 ^ 64 - 1 - y.toNat) := by
  apply BitVec.eq_of_getLsbD_eq
  intro k hk
  rw [BitVec.getLsbD_xor, bitvec64_allOnes_getLsbD k hk, BitVec.getLsbD_ofNat]
  simp only [hk, decide_true, Bool.true_and, Bool.xor_true]
  exact (testBit_pow2_sub1_sub 64 y.toNat k y.isLt hk).symm

private theorem bitvec64_xor_allOnes_toNat (y : BitVec 64) :
    (y ^^^ 0xFFFFFFFFFFFFFFFF#64).toNat = 2 ^ 64 - 1 - y.toNat := by
  rw [bitvec64_xor_allOnes_eq_ofNat, BitVec.toNat_ofNat]
  have hlt : 2 ^ 64 - 1 - y.toNat < 2 ^ 64 := by
    have : 0 < 2 ^ 64 := by decide
    omega
  exact Nat.mod_eq_of_lt hlt

private theorem zigzag_eq_spec (i : Int64) :
    zigzagEncode64 i = specZigzag64 i := by
  let x := i.toBitVec
  have hlt64 : x.toNat < 2 ^ 64 := x.isLt
  have hpow64 : (2 ^ 64 : Nat) = 2 * 2 ^ 63 := by decide
  apply UInt64.eq_of_toBitVec_eq
  apply BitVec.eq_of_toNat_eq
  by_cases hlt : x.toNat < 2 ^ 63
  · have htoInt : i.toInt = Int.ofNat x.toNat := by
      show x.toInt = Int.ofNat x.toNat
      simp only [BitVec.toInt]
      have hhalf : 2 * x.toNat < 2 ^ 64 := by omega
      simp only [hhalf, ↓reduceIte]; rfl
    have hsr : x.sshiftRight 63 = 0#64 := by
      simp only [BitVec.sshiftRight]
      have hxi : x.toInt = Int.ofNat x.toNat := htoInt
      rw [hxi]
      have hdiv : Int.ofNat x.toNat >>> 63 = 0 := by
        change Int.ofNat (x.toNat >>> 63) = 0
        rw [Nat.shiftRight_eq_div_pow, Nat.div_eq_of_lt hlt]; rfl
      rw [hdiv]; rfl
    have hge0 : i.toInt >= 0 := by rw [htoInt]; exact Int.natCast_nonneg x.toNat
    have htn : i.toInt.toNat = x.toNat := by rw [htoInt]; rfl
    simp only [zigzagEncode64, specZigzag64, hge0, ↓reduceIte, htn, UInt64.toBitVec_ofBitVec]
    change ((x <<< 1) ^^^ (x.sshiftRight 63)).toNat = (BitVec.ofNat 64 (2 * x.toNat)).toNat
    rw [hsr]
    simp only [BitVec.xor_zero, BitVec.toNat_shiftLeft, BitVec.toNat_ofNat]
    rw [Nat.shiftLeft_eq]
    have hmul : x.toNat * 2 ^ 1 = 2 * x.toNat := by omega
    rw [hmul]
  · have hge : 2 ^ 63 <= x.toNat := by omega
    have htoInt : i.toInt = Int.negSucc (2 ^ 64 - 1 - x.toNat) := by
      show x.toInt = Int.negSucc (2 ^ 64 - 1 - x.toNat)
      simp only [BitVec.toInt]
      have hnhalf : ¬ (2 * x.toNat < 2 ^ 64) := by omega
      simp only [hnhalf, ↓reduceIte]; omega
    have hsr : x.sshiftRight 63 = 0xFFFFFFFFFFFFFFFF#64 := by
      simp only [BitVec.sshiftRight]
      have hxi : x.toInt = Int.negSucc (2 ^ 64 - 1 - x.toNat) := htoInt
      rw [hxi]
      have hrem : 2 ^ 64 - 1 - x.toNat < 2 ^ 63 := by omega
      have hdiv : Int.negSucc (2 ^ 64 - 1 - x.toNat) >>> 63 = Int.negSucc 0 := by
        change Int.negSucc ((2 ^ 64 - 1 - x.toNat) >>> 63) = Int.negSucc 0
        rw [Nat.shiftRight_eq_div_pow, Nat.div_eq_of_lt hrem]
      rw [hdiv]; decide
    have hneg : ¬ (i.toInt >= 0) := by rw [htoInt]; omega
    have hmag : (-i.toInt - 1).toNat = 2 ^ 64 - 1 - x.toNat := by rw [htoInt]; omega
    simp only [zigzagEncode64, specZigzag64, hneg, ↓reduceIte, hmag, UInt64.toBitVec_ofBitVec]
    change ((x <<< 1) ^^^ (x.sshiftRight 63)).toNat = (BitVec.ofNat 64 (2 * (2 ^ 64 - 1 - x.toNat) + 1)).toNat
    rw [hsr, bitvec64_xor_allOnes_toNat, BitVec.toNat_shiftLeft, BitVec.toNat_ofNat]
    rw [Nat.shiftLeft_eq]
    have hsh : x.toNat * 2 ^ 1 % 2 ^ 64 = 2 * x.toNat - 2 ^ 64 := by omega
    have hlt_rhs : 2 * (2 ^ 64 - 1 - x.toNat) + 1 < 2 ^ 64 := by omega
    rw [hsh, Nat.mod_eq_of_lt hlt_rhs]
    omega

theorem L37_lean_u8_bitvec_and_i64_zigzag : L37_Prop := by
  refine ⟨?_, fun i => ⟨zigzag_eq_spec i, zigzag_roundtrip_i64 i⟩, zigzag_roundtrip_u64, ?_⟩
  · intro a base epi flags ps
    refine ⟨rfl, rfl, rfl, ?_, ?_⟩
    · simp only [propagateByte]
      cases h : (isTaintedByte base || ps.any isTaintedByte)
      · have h' : (specIsTaintedByte base || ps.any specIsTaintedByte) = false := h
        rw [h']
        cases hb : specIsTaintedByte base
        · simp only [Bool.false_eq_true, ↓reduceIte, hb]
        · rw [hb] at h'; cases h'
      · have h' : (specIsTaintedByte base || ps.any specIsTaintedByte) = true := h
        rw [h']
        exact uint8_isTainted_or_bit7 base
    · simp only [propagateByte]
      cases (isTaintedByte base || ps.any isTaintedByte)
      · rfl
      · exact uint8_or_bit7_and_7F base
  · intro b
    refine ⟨rfl, rfl, ?_, ?_, ?_, by decide⟩
    · cases hnz : (b == NEG_ZERO_BITS)
      · cases hnan : isNaNBits b
        · simp only [canonicalizeF64Bits, verifyCanonF64Bits, hnz, hnan, Bool.false_eq_true, ↓reduceIte]
        · simp only [canonicalizeF64Bits, verifyCanonF64Bits, hnz, hnan, Bool.false_eq_true, ↓reduceIte,
            canonQNaN_ne_negZero, isNaNBits_canonQNaN, canonQNaN_beq_self]
      · simp only [canonicalizeF64Bits, verifyCanonF64Bits, hnz, ↓reduceIte, zero_ne_negZero,
          isNaNBits_zero, Bool.false_eq_true]
    · cases hnz : (b == NEG_ZERO_BITS)
      · cases hnan : isNaNBits b
        · simp only [canonicalizeF64Bits, hnz, hnan, Bool.false_eq_true, ↓reduceIte]
        · simp only [canonicalizeF64Bits, hnz, hnan, Bool.false_eq_true, ↓reduceIte,
            canonQNaN_ne_negZero, isNaNBits_canonQNaN]
      · simp only [canonicalizeF64Bits, hnz, ↓reduceIte, zero_ne_negZero, isNaNBits_zero, Bool.false_eq_true]
    · cases hnz : (b == NEG_ZERO_BITS)
      · cases hnan : isNaNBits b
        · simp only [verifyCanonF64Bits, canonicalizeF64Bits, hnz, hnan, Bool.false_eq_true, ↓reduceIte]
        · simp only [verifyCanonF64Bits, canonicalizeF64Bits, hnz, hnan, Bool.false_eq_true, ↓reduceIte]
          exact ⟨fun h => (uint64_eq_of_beq h).symm, fun h => uint64_beq_of_eq h.symm⟩
      · have hbe : b = NEG_ZERO_BITS := uint64_eq_of_beq hnz
        subst hbe
        have hne : (0 : UInt64) ≠ NEG_ZERO_BITS := by decide
        simp only [verifyCanonF64Bits, canonicalizeF64Bits, hnz, ↓reduceIte, Bool.false_eq_true, hne]

-- L38: Fuel-Free Well-Founded RFC 6962 Universal Merkle Injectivity (`flattenLeaves` invariant!)
private def flattenLeaves : Digest → List Digest
  | .hEmpty        => []
  | .hNode01 l r   => flattenLeaves l ++ flattenLeaves r
  | d              => [d]

private theorem flattenLeaves_atomic (d : Digest) (h : isAtomicLeaf d) :
    flattenLeaves d = [d] := by
  rcases h with ⟨p, rfl⟩ | ⟨bl, m, rfl⟩ <;> rfl

private theorem flatMap_flattenLeaves_pairLevel (xs : List Digest) :
    (merklePairLevel xs).flatMap flattenLeaves = xs.flatMap flattenLeaves := by
  match xs with
  | [] => rfl
  | [a] => simp [merklePairLevel]
  | a :: b :: rest =>
      simp only [merklePairLevel, List.flatMap_cons, flattenLeaves, List.append_assoc]
      rw [flatMap_flattenLeaves_pairLevel rest]

private theorem merkleTree_cons_cons (a b : Digest) (rest : List Digest) :
    merkleTree (a :: b :: rest) = merkleTree (.hNode01 a b :: merklePairLevel rest) := by
  rw [merkleTree]

private theorem flattenLeaves_merkleTree (xs : List Digest) :
    flattenLeaves (merkleTree xs) = xs.flatMap flattenLeaves := by
  match xs with
  | [] => simp [merkleTree, flattenLeaves]
  | [only] => simp [merkleTree]
  | a :: b :: rest =>
      rw [merkleTree_cons_cons, flattenLeaves_merkleTree (.hNode01 a b :: merklePairLevel rest)]
      simp only [List.flatMap_cons, flattenLeaves, List.append_assoc]
      rw [flatMap_flattenLeaves_pairLevel rest]
termination_by xs.length
decreasing_by
  have := merklePairLevel_length_le rest
  simp
  omega

private theorem flatMap_flattenLeaves_of_atomic (xs : List Digest)
    (h : ∀ d ∈ xs, isAtomicLeaf d) :
    xs.flatMap flattenLeaves = xs := by
  induction xs with
  | nil => rfl
  | cons d rest ih =>
      have hd := flattenLeaves_atomic d (h d (by simp))
      have hrest := ih (fun x hx => h x (by simp [hx]))
      simp [hd, hrest]

private theorem merkleTree_injective_atomic (xs ys : List Digest)
    (hx : ∀ d ∈ xs, isAtomicLeaf d) (hy : ∀ d ∈ ys, isAtomicLeaf d)
    (heq : merkleTree xs = merkleTree ys) : xs = ys := by
  have h1 := flattenLeaves_merkleTree xs
  have h2 := flattenLeaves_merkleTree ys
  rw [flatMap_flattenLeaves_of_atomic xs hx] at h1
  rw [flatMap_flattenLeaves_of_atomic ys hy] at h2
  rw [← h1, heq, h2]

theorem L38_lean_merkle_universal_injective : L38_Prop := by
  refine ⟨merkleTree_injective_atomic, ?_⟩
  intro f1 t1 f2 t2 hf1 ht1 hf2 ht2 heq
  cases t1 with
  | nil =>
      cases t2 with
      | nil =>
          simp [shardMerkleRoot] at heq
          exact ⟨merkleTree_injective_atomic f1 f2 hf1 hf2 heq.2, rfl⟩
      | cons _ _ =>
          simp [shardMerkleRoot] at heq
  | cons _ _ =>
      cases t2 with
      | nil =>
          simp [shardMerkleRoot] at heq
      | cons _ _ =>
          simp [shardMerkleRoot] at heq
          exact ⟨merkleTree_injective_atomic f1 f2 hf1 hf2 heq.2.1,
                 merkleTree_injective_atomic _ _ ht1 ht2 heq.2.2.2⟩

-- L39: Declarative `List.Infix` non-occurrence + zero `spoof` + `List.Sublist` + idempotence!
private theorem hasForbiddenStep_prefix (pre rest : List Sym) (rLt rGt : Run2) (prev : PrevSym) :
    hasForbiddenStep (pre ++ rest) rLt rGt prev = true ∨
    ∃ rL' rG' p', hasForbiddenStep (pre ++ rest) rLt rGt prev = hasForbiddenStep rest rL' rG' p' := by
  induction pre generalizing rLt rGt prev with
  | nil => exact Or.inr ⟨rLt, rGt, prev, rfl⟩
  | cons x tl ih =>
      cases x with
      | spoof => exact Or.inl rfl
      | lt =>
          cases rLt with
          | r0 => exact ih .r1 .r0 .pLt
          | r1 => exact ih .r2 .r0 .pLt
          | r2 => exact Or.inl rfl
      | gt =>
          cases prev with
          | pPipe => exact Or.inl rfl
          | pLt | pOther =>
              cases rGt with
              | r0 => exact ih .r0 .r1 .pOther
              | r1 => exact ih .r0 .r2 .pOther
              | r2 => exact Or.inl rfl
      | pipe =>
          cases prev with
          | pLt => exact Or.inl rfl
          | pPipe | pOther => exact ih .r0 .r0 .pPipe
      | esc => exact ih .r0 .r0 .pOther
      | other => exact ih .r0 .r0 .pOther

private theorem hasForbidden_of_infix_pat (ys pat : List Sym)
    (hpat : ∀ rLt rGt prev suf, hasForbiddenStep (pat ++ suf) rLt rGt prev = true)
    (hinf : pat <:+: ys) : hasForbidden ys = true := by
  rcases hinf with ⟨pre, suf, rfl⟩
  rw [List.append_assoc]
  rcases hasForbiddenStep_prefix pre (pat ++ suf) .r0 .r0 .pOther with ht | ⟨rL', rG', p', heq⟩
  · exact ht
  · simp only [hasForbidden, heq, hpat]

private theorem mem_to_singleton_infix {α : Type} {x : α} {ys : List α} (h : x ∈ ys) :
    [x] <:+: ys := by
  induction ys with
  | nil => cases h
  | cons y tl ih =>
      cases h with
      | head => exact ⟨[], tl, rfl⟩
      | tail _ htl =>
          rcases ih htl with ⟨pre, suf, heq⟩
          exact ⟨y :: pre, suf, by simp [heq]⟩

private theorem not_mem_spoof_of_safe (ys : List Sym) (h : hasForbidden ys = false) :
    ∀ s ∈ ys, s ≠ .spoof := by
  intro s hs heq
  subst heq
  have hinf : [.spoof] <:+: ys := mem_to_singleton_infix hs
  have ht : hasForbidden ys = true :=
    hasForbidden_of_infix_pat ys [.spoof] (fun _ _ _ _ => rfl) hinf
  rw [h] at ht
  cases ht

theorem L39_lean_fence_declarative_infix_and_idempotent : L39_Prop := by
  intro xs n
  have hsafe := L07_fence_no_triple xs
  have htake : hasForbidden ((sanitizeFences xs).take n) = false :=
    hasForbiddenStep_take n (sanitizeFences xs) .r0 .r0 .pOther hsafe
  refine ⟨?_, ?_, ?_, ?_, not_mem_spoof_of_safe _ hsafe, sublist_sanitizeStep xs .r0 .r0 .pOther,
          sanitizeStep_length_le_two_mul xs .r0 .r0 .pOther,
          L08_fence_safe_identity (sanitizeFences xs) hsafe⟩
  · intro hinf
    have ht := hasForbidden_of_infix_pat _ [.lt, .lt, .lt]
      (fun rLt rGt prev suf => by cases rLt <;> cases rGt <;> cases prev <;> rfl) hinf
    rw [htake] at ht; cases ht
  · intro hinf
    have ht := hasForbidden_of_infix_pat _ [.gt, .gt, .gt]
      (fun rLt rGt prev suf => by cases rLt <;> cases rGt <;> cases prev <;> rfl) hinf
    rw [htake] at ht; cases ht
  · intro hinf
    have ht := hasForbidden_of_infix_pat _ [.lt, .pipe]
      (fun rLt rGt prev suf => by cases rLt <;> cases rGt <;> cases prev <;> rfl) hinf
    rw [htake] at ht; cases ht
  · intro hinf
    have ht := hasForbidden_of_infix_pat _ [.pipe, .gt]
      (fun rLt rGt prev suf => by cases rLt <;> cases rGt <;> cases prev <;> rfl) hinf
    rw [htake] at ht; cases ht

/-- Master bundled proof witness confirming all 40 Cowrie v1.2 formal laws simultaneously. -/
theorem all_laws_proved : CowrieSpec :=
  { l01  := L01_propagate_iff
    l02  := L02_taint_irrevocable_hops
    l03  := L03_sticky_reregister
    l04  := L04_unknown_parent_fails_closed
    l05  := L05_empty_args_fail_closed
    l06a := L06a_wrong_action_token_rejected
    l06b := L06b_partial_declass_blocks
    l07  := L07_fence_no_triple
    l08  := L08_fence_safe_identity
    l09  := L09_canon_roundtrip
    l10  := L10_verify_iff_decode
    l11  := L11_canon_wire_unique
    l12  := L12_zonemap_no_false_neg
    l13  := L13_merkle_domain_and_odd_safe
    l14  := L14_wal_torn_vs_mid_tamper
    l15  := L15_propagate_preserves_origin
    l16  := L16_sticky_deep_and_clean_liveness
    l17  := L17_gate_exact_iff_and_liveness
    l18  := L18_expired_and_forged_sig_rejected
    l19  := L19_nth_arg_taint_blocks
    l20  := L20_fence_preserves_all_non_esc_symbols
    l21  := L21_r2_structured_table_header_safe
    l22  := L22_map_keys_and_dict_all_used
    l23  := L23_sorted_zonemap_early_break_exact
    l24  := L24_wal_tombstone_reinsert_and_statepage_rt
    l25  := L25_ledger_record_exact_iff
    l26  := L26_header_meta_and_owner_pubkey
    l27  := L27_ed25519_action_code_and_mcp_classifier
    l28  := L28_r2_compact_and_virtual_atlas_fail_closed
    l29  := L29_r2_table_marker_injectivity
    l30  := L30_lens_tier_bounds_and_extract_field
    l31  := L31_canon_float64_bijective_and_idempotent
    l32  := L32_json_sentinel_bijectivity_and_fail_closed
    l33  := L33_resolved_frame_cid_repack_and_binding
    l34  := L34_merkle_leaf_and_tensor_sensitivity
    l35  := L35_live_ivf_compaction_crash_invariant
    l36  := L36_statepage_hmac_precedence
    l37  := L37_lean_u8_bitvec_and_i64_zigzag
    l38  := L38_lean_merkle_universal_injective
    l39  := L39_lean_fence_declarative_infix_and_idempotent }

end Cowrie.Proofs
