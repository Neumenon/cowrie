/-
  Cowrie/Laws.lean — Pure `Prop` Formal Specification of All 40 Cowrie v1.2 Laws
  (37 Exact Bend Counterparts `L01`..`L36` + `L06a`/`L06b`, PLUS 3 Lean-Exclusive Strengthenings `L37`..`L39`)

  TCB Separation Invariant (Audited by Post-Proof Multi-Agent Critique):
    - ZERO proofs, ZERO tactics, ZERO `sorry`, ZERO `axiom`.
    - ALL specification predicates (`specTainted`, `specSetTainted`, `specGate`, `specStripSpoof`,
      `specStripEsc`, `hasForbidden`, `specActionToCode`, `specClassifyMcpTool`, `specValidWire`,
      `specCanonicalizeF64`, `specVerifyCanonF64`, `specResolvedFrameCid`, `specExtractField`,
      `specWalRecoverMode`, `specIsTaintedByte`, `specPropagateByte`, `specIsValidHeaderMetaByte`,
      `specZigzag64`, `specCanonF64Bits`, `specVerifyCanonF64Bits`, etc.) live HERE — NEVER imported
      from the mutable executable kernel modules!
-/
import Cowrie.Trust
import Cowrie.Fence
import Cowrie.Canonical
import Cowrie.Storage

namespace Cowrie.Laws

open Cowrie.Trust
open Cowrie.Fence
open Cowrie.Canonical
open Cowrie.Storage

-- =============================================================================
-- 0. FROZEN INDEPENDENT SPECIFICATION PREDICATES (TCB BOUNDARY)
-- =============================================================================

def specTainted (a : Auth) : Bool := a.ext || a.tainted

def specSetTainted (a : Auth) : Auth := { a with tainted := true }

def specAnyTainted (ps : List Auth) : Bool :=
  match ps with
  | [] => false
  | h :: t => specTainted h || specAnyTainted t

def specLedgerHasCid (lg : List LedgerNode) (cid : Nat) : Bool :=
  match lg with
  | [] => false
  | n :: rest => (n.cid == cid) || specLedgerHasCid rest cid

def specLedgerLookup (lg : List LedgerNode) (cid : Nat) : Option Auth :=
  match lg with
  | [] => none
  | n :: rest => if n.cid == cid then some n.auth else specLedgerLookup rest cid

def specOptTainted (ma : Option Auth) : Bool :=
  match ma with
  | none => true
  | some a => specTainted a

def specExistingTainted (ma : Option Auth) : Bool :=
  match ma with
  | none => false
  | some a => specTainted a

def specParentsTainted (ps : List Nat) (lg : List LedgerNode) : Bool :=
  match ps with
  | [] => false
  | p :: rest => specOptTainted (specLedgerLookup lg p) || specParentsTainted rest lg

def specIsPrivileged (act : Action) : Bool :=
  match act with
  | .readOnly => false
  | .shellExec | .credRead | .fsWrite | .netExfil => true

def specTokValid (tok : DeclassToken) (cid : Nat) (act : Action) (now : Nat) : Bool :=
  tok.sigValid && (tok.cid == cid) && (tok.act == act) && decide (now <= tok.expiry)

def specHasValidTok (toks : List DeclassToken) (cid : Nat) (act : Action) (now : Nat) : Bool :=
  match toks with
  | [] => false
  | tk :: rest => specTokValid tk cid act now || specHasValidTok rest cid act now

def specArgOk (cid : Nat) (lg : List LedgerNode) (act : Action)
    (toks : List DeclassToken) (hasPk : Bool) (now : Nat) : Bool :=
  if specOptTainted (specLedgerLookup lg cid) then
    hasPk && specHasValidTok toks cid act now
  else
    true

def specAllArgsOk (args : List Nat) (lg : List LedgerNode) (act : Action)
    (toks : List DeclassToken) (hasPk : Bool) (now : Nat) : Bool :=
  match args with
  | [] => true
  | c :: rest => specArgOk c lg act toks hasPk now && specAllArgsOk rest lg act toks hasPk now

def specGate (lg : List LedgerNode) (act : Action) (args : List Nat)
    (toks : List DeclassToken) (hasPk : Bool) (now : Nat) : Bool :=
  (!specIsPrivileged act) || (!args.isEmpty && specAllArgsOk args lg act toks hasPk now)

def specValidHeaderMeta (low4 res3 epi flags : Nat) : Bool :=
  (flags == 0) && (res3 == 0) && decide (1 <= low4) && decide (low4 <= 15) &&
    decide (1 <= epi) && decide (epi <= 3)

def specActionToCode (act : Action) : Nat :=
  match act with
  | .readOnly  => 0
  | .shellExec => 1
  | .credRead  => 2
  | .fsWrite   => 3
  | .netExfil  => 4

def specClassifyMcpTool (polOverride verbPriv : Option Action)
    (allowRo builtinRo trustAnn serverRoHint : Bool) : Action :=
  match polOverride, verbPriv with
  | some act, _     => act
  | none, some vact => vact
  | none, none =>
      if allowRo || builtinRo || (trustAnn && serverRoHint) then
        .readOnly
      else
        .shellExec

-- Frozen Plane B specs (moved out of mutable Cowrie/Fence.lean!)
def specStripSpoof (xs : List Sym) : List Sym :=
  xs.filter (fun s => s != .spoof)

def specStripEsc (xs : List Sym) : List Sym :=
  xs.filter (fun s => s != .esc)

def hasForbiddenStep (ys : List Sym) (rLt rGt : Run2) (prev : PrevSym) : Bool :=
  match ys with
  | [] => false
  | .spoof :: _ => true
  | .lt :: rest =>
      match rLt with
      | .r0 => hasForbiddenStep rest .r1 .r0 .pLt
      | .r1 => hasForbiddenStep rest .r2 .r0 .pLt
      | .r2 => true
  | .gt :: rest =>
      match prev with
      | .pPipe => true
      | _ =>
          match rGt with
          | .r0 => hasForbiddenStep rest .r0 .r1 .pOther
          | .r1 => hasForbiddenStep rest .r0 .r2 .pOther
          | .r2 => true
  | .pipe :: rest =>
      match prev with
      | .pLt => true
      | _    => hasForbiddenStep rest .r0 .r0 .pPipe
  | .esc :: rest =>
      hasForbiddenStep rest .r0 .r0 .pOther
  | .other :: rest =>
      hasForbiddenStep rest .r0 .r0 .pOther

def hasForbidden (ys : List Sym) : Bool :=
  hasForbiddenStep ys .r0 .r0 .pOther

def cellTagHasForbidden (ct : CellTag) : Bool :=
  match ct with
  | .outNull | .outMissing | .outEmptyQuoted => false
  | .outQuotedStr s | .outPlainStr s => hasForbidden s

def cellTagContent (ct : CellTag) : List Sym :=
  match ct with
  | .outNull | .outMissing | .outEmptyQuoted => []
  | .outQuotedStr s | .outPlainStr s => s

def specMapHasKey (entries : List (Nat × ValTree)) (key : Nat) : Bool :=
  match entries with
  | [] => false
  | (ek, _) :: rest => (ek == key) || specMapHasKey rest key

-- Frozen Plane A specs (de-circularized `L31`, `L33`, `L37`)
def specValidWire (lim ulim : Nat) (w : WireInt) : Bool :=
  match w with
  | .wFix n       => decide (n < lim)
  | .wTagIntPos n => decide (!(n < lim))
  | .wTagIntNeg _ => true
  | .wTagUint u   => decide (!(u < ulim))

def specStrictlyAscFrom (ks : List Nat) (minA nkeys : Nat) : Bool :=
  match ks with
  | [] => true
  | k :: rest => decide (minA <= k) && decide (k < nkeys) && specStrictlyAscFrom rest (k + 1) nkeys

def specAllLtIn (nkeys : Nat) (refs : List Nat) : Bool :=
  match nkeys with
  | 0 => true
  | pk + 1 => refs.contains pk && specAllLtIn pk refs

def specCanonicalizeF64 (f : FloatKind) : FloatKind :=
  match f with
  | .fPosZero       => .fPosZero
  | .fNegZero       => .fPosZero
  | .fFinite b      => .fFinite b
  | .fInf p         => .fInf p
  | .fCanonQNaN     => .fCanonQNaN
  | .fNonCanonNaN _ => .fCanonQNaN

def specVerifyCanonF64 (f : FloatKind) : Bool :=
  match f with
  | .fPosZero       => true
  | .fNegZero       => false
  | .fFinite _      => true
  | .fInf _         => true
  | .fCanonQNaN     => true
  | .fNonCanonNaN _ => false

def specLookupTensor (ts : List TensorDesc) (idx : Nat) : Option TensorDesc :=
  match ts, idx with
  | [], _ => none
  | h :: _, 0 => some h
  | _ :: t, i + 1 => specLookupTensor t i

def specResolveTensorRefs (refs : List Nat) (ts : List TensorDesc) : Option (List BoundTensor) :=
  match refs with
  | [] => some []
  | idx :: rest =>
      match specLookupTensor ts idx, specResolveTensorRefs rest ts with
      | some td, some more => some ({ idx := idx, td := td } :: more)
      | _, _               => none

def specResolvedFrameCid (frameCid : Nat) (refs : List Nat) (ts : List TensorDesc) : Option ResolvedCid :=
  match specResolveTensorRefs refs ts with
  | none       => none
  | some bound => some { frameCid := frameCid, boundTensors := bound }

-- Frozen Plane C/D specs
def specInRange (x lo hi : Int) : Bool :=
  decide (lo <= x) && decide (x <= hi)

def specPageHasInRange (k0 : Int) (ks : List Int) (lo hi : Int) : Bool :=
  specInRange k0 lo hi || ks.any (fun k => specInRange k lo hi)

def specZonesDisjointFrom (zs : List Zone) (prevMax : Int) : Bool :=
  match zs with
  | [] => true
  | z :: rest => decide (prevMax <= z.minK) && decide (z.minK <= z.maxK) &&
      specZonesDisjointFrom rest z.maxK

def specZonesDisjoint (zs : List Zone) : Bool :=
  match zs with
  | [] => true
  | z :: rest => decide (z.minK <= z.maxK) && specZonesDisjointFrom rest z.maxK

def specFilterZonesAt (zs : List Zone) (idx : Nat) (lo hi : Int) : List Nat :=
  match zs with
  | [] => []
  | z :: rest =>
      if lo <= z.maxK && z.minK <= hi then
        idx :: specFilterZonesAt rest (idx + 1) lo hi
      else
        specFilterZonesAt rest (idx + 1) lo hi

def isAtomicLeaf (d : Digest) : Prop :=
  (∃ p, d = .hLeaf00 p) ∨ (∃ bl m, d = .hTensor03 bl m)

def specWalValidPrefix (payloads : List Nat) (startSeq : Nat) (tail : List WalRec) : List WalRec :=
  match payloads with
  | [] => tail
  | p :: rest => { valid := true, seq := startSeq, payload := p } ::
      specWalValidPrefix rest (startSeq + 1) tail

def specWalRecoverMode (recs : List WalRec) (expSeq : Nat) (recoverTorn : Bool) : WalResult :=
  match recs with
  | [] => .walOk []
  | r :: rest =>
      if r.valid then
        if r.seq == expSeq then
          match specWalRecoverMode rest (expSeq + 1) recoverTorn with
          | .walOk ps => .walOk (r.payload :: ps)
          | .walErr   => .walErr
        else
          .walErr
      else
        match rest with
        | [] => if recoverTorn then .walOk [] else .walErr
        | _ :: _ => .walErr

def specSpRuntime (exp file : Option Nat) : Bool :=
  match exp, file with
  | none, _        => true
  | some _, none   => false
  | some e, some g => e == g

def ivfMakeSeqRecs (ids : List Nat) (startSeq : Nat) : List IvfWalRec :=
  match ids with
  | [] => []
  | v :: rest => { seq := startSeq, vecId := v } :: ivfMakeSeqRecs rest (startSeq + 1)

-- Frozen Fixed-Width Machine-Word Specs (de-circularizes `L37` completely!)
def specIsTaintedByte (a : UInt8) : Bool :=
  (a &&& 0x84) != 0

def specPropagateByte (base : UInt8) (ps : List UInt8) : UInt8 :=
  if specIsTaintedByte base || ps.any specIsTaintedByte then
    base ||| 0x80
  else
    base

def specIsValidHeaderMetaByte (auth epi flags : UInt8) : Bool :=
  let low4 := auth &&& 0x0F
  let res3 := auth &&& 0x70
  flags == 0 && res3 == 0 && 1 <= low4 && low4 <= 15 && 1 <= epi && epi <= 3

def specZigzag64 (i : Int64) : UInt64 :=
  if i.toInt >= 0 then
    UInt64.ofNat (2 * i.toInt.toNat)
  else
    UInt64.ofNat (2 * (-i.toInt - 1).toNat + 1)

def specIsNaNBits (b : UInt64) : Bool :=
  ((b &&& 0x7FF0000000000000) == 0x7FF0000000000000) && ((b &&& 0x000FFFFFFFFFFFFF) != 0)

def specCanonF64Bits (b : UInt64) : UInt64 :=
  if b == 0x8000000000000000 then
    0
  else if specIsNaNBits b then
    0x7FF8000000000000
  else
    b

def specVerifyCanonF64Bits (b : UInt64) : Bool :=
  if b == 0x8000000000000000 then
    false
  else if specIsNaNBits b then
    b == 0x7FF8000000000000
  else
    true

-- =============================================================================
-- GROUP 1: PLANE D TRUST, STICKY LEDGER, GATE, HEADER META & MCP (15 LAWS)
-- =============================================================================

/-- L01: Exact non-circular `isTainted` (`ext || tainted`), `propagate` biconditional,
    AND bit-7 (`tainted = true`) pinning even when `base.ext = true`. -/
def L01_Prop : Prop :=
  ∀ (c : Nat) (e t : Bool) (base : Auth) (ps : List Auth),
    isTainted { cleanBits := c, ext := e, tainted := t } = (e || t) ∧
    isTainted (propagate base ps) = (specTainted base || specAnyTainted ps) ∧
    (propagate base ps).tainted = (specTainted base || specAnyTainted ps)

/-- L02: Exact biconditional `deriveChain`: tainted iff root OR ANY intermediate hop is tainted. -/
def L02_Prop : Prop :=
  ∀ (hops : List Auth) (root : Auth),
    isTainted (deriveChain hops root) = (specTainted root || specAnyTainted hops)

/-- L03: Sticky re-registration holds for BOTH `ext=true, tainted=false` AND `tainted=true`. -/
def L03_Prop : Prop :=
  ∀ (cid oc : Nat) (old : Auth) (rest : List LedgerNode) (base : Auth) (ps : List Nat),
    isTainted (ledgerRecord ({ cid := cid, auth := { cleanBits := oc, ext := true, tainted := false } } :: rest) cid base ps) = true ∧
    isTainted (ledgerRecord ({ cid := cid, auth := specSetTainted old } :: rest) cid base ps) = true

/-- L04: Unknown parent at ANY index fails closed on ARBITRARY ledgers (empty OR non-empty). -/
def L04_Prop : Prop :=
  ∀ (lg : List LedgerNode) (cid : Nat) (base : Auth) (pPre : List Nat) (unk : Nat) (pTail : List Nat),
    specLedgerHasCid lg unk = false →
    isTainted (ledgerRecord lg cid base (pPre ++ unk :: pTail)) = true

/-- L05: `isPrivileged` matches frozen spec across all 5 actions & empty args fail closed. -/
def L05_Prop : Prop :=
  ∀ (act : Action) (lg : List LedgerNode) (toks : List DeclassToken) (hasPk : Bool) (now : Nat),
    isPrivileged act = specIsPrivileged act ∧
    (specIsPrivileged act = true → checkGateSignedPk lg act [] toks hasPk now = false)

/-- L06a: Wrong-action token rejected across ALL mismatched action pairs and BOTH `ext`/`tainted`. -/
def L06a_Prop : Prop :=
  ∀ (a1 a2 : Action) (cid oc : Nat) (oe ot : Bool) (expiry now : Nat),
    specIsPrivileged a1 = true →
    (oe || ot) = true →
    a1 ≠ a2 →
    checkGateSigned [{ cid := cid, auth := { cleanBits := oc, ext := oe, tainted := ot } }]
      a1 [cid] [{ cid := cid, act := a2, expiry := expiry, sigValid := true }] now = false

/-- L06b: Token for `c1` NEVER authorizes distinct tainted `c2 ≠ c1` (neither `[c1, c2]` nor `[c2]`). -/
def L06b_Prop : Prop :=
  ∀ (act : Action) (c1 c2 : Nat) (o1 : Auth) (oc2 : Nat) (oe2 ot2 : Bool) (exp now : Nat),
    specIsPrivileged act = true →
    c1 ≠ c2 →
    (oe2 || ot2) = true →
    let lg := [{ cid := c1, auth := specSetTainted o1 },
               { cid := c2, auth := { cleanBits := oc2, ext := oe2, tainted := ot2 } }]
    let toks := [{ cid := c1, act := act, expiry := exp, sigValid := true }]
    checkGateSigned lg act [c1, c2] toks now = false ∧
    checkGateSigned lg act [c2] toks now = false

/-- L15: BOTH `propagate` and `ledgerRecord` preserve `cleanBits` and `ext` identically. -/
def L15_Prop : Prop :=
  ∀ (base : Auth) (ps : List Auth) (lg : List LedgerNode) (cid : Nat) (pids : List Nat),
    (propagate base ps).cleanBits = base.cleanBits ∧
    (propagate base ps).ext = base.ext ∧
    (ledgerRecord lg cid base pids).cleanBits = base.cleanBits ∧
    (ledgerRecord lg cid base pids).ext = base.ext

/-- L16: Deep sticky taint behind arbitrary prefix, universal clean-derivation liveness,
    AND multi-step stateful `ledgerInsert` sticky monotonicity across arbitrary updates! -/
def L16_Prop : Prop :=
  ∀ (prefixLg rest : List LedgerNode) (cid oc : Nat) (oe ot : Bool) (base : Auth) (ps : List Nat)
    (lg : List LedgerNode) (bc : Nat) (cleanPs : List Nat) (target : Nat),
    (specLedgerHasCid prefixLg cid = false → (oe || ot) = true →
      isTainted (ledgerRecord (prefixLg ++ { cid := cid, auth := { cleanBits := oc, ext := oe, tainted := ot } } :: rest) cid base ps) = true) ∧
    (specExistingTainted (specLedgerLookup lg cid) = false →
      specParentsTainted cleanPs lg = false →
      isTainted (ledgerRecord lg cid { cleanBits := bc, ext := false, tainted := false } cleanPs) = false) ∧
    (specExistingTainted (specLedgerLookup lg target) = true →
      specExistingTainted (specLedgerLookup (ledgerInsert lg cid base ps) target) = true)

/-- L17: Exact NON-CIRCULAR biconditional of BOTH `checkGateSignedPk` AND 5-arg `checkGateSigned`
    against frozen `specGate` + deep-token liveness! -/
def L17_Prop : Prop :=
  ∀ (lg : List LedgerNode) (act : Action) (args : List Nat) (toks : List DeclassToken)
    (hasPk : Bool) (now cid : Nat) (old : Auth) (tpre ttail : List DeclassToken) (d : Nat),
    checkGateSignedPk lg act args toks hasPk now = specGate lg act args toks hasPk now ∧
    checkGateSigned lg act args toks now = specGate lg act args toks true now ∧
    checkGateSignedPk lg .readOnly [] [] false now = true ∧
    checkGateSignedPk [{ cid := cid, auth := specSetTainted old }] act [cid]
      (tpre ++ { cid := cid, act := act, expiry := now + d, sigValid := true } :: ttail) true now = true

/-- L18: Expired tokens (`now = exp + d + 1`), forged signatures, and missing `owner_pubkey` ALL block. -/
def L18_Prop : Prop :=
  ∀ (act : Action) (cid : Nat) (old : Auth) (exp d now : Nat),
    specIsPrivileged act = true →
    let lg := [{ cid := cid, auth := specSetTainted old }]
    checkGateSigned lg act [cid] [{ cid := cid, act := act, expiry := exp, sigValid := true }] (exp + d + 1) = false ∧
    checkGateSigned lg act [cid] [{ cid := cid, act := act, expiry := exp, sigValid := false }] now = false ∧
    checkGateSignedPk lg act [cid] [{ cid := cid, act := act, expiry := now + d, sigValid := true }] false now = false

/-- L19: Unauthorized/unknown/tainted `evilCid` at ANY argument index blocks on EVERY privileged action. -/
def L19_Prop : Prop :=
  ∀ (act : Action) (pfx : List Nat) (evilCid : Nat) (tl : List Nat)
    (lg : List LedgerNode) (toks : List DeclassToken) (hasPk : Bool) (now : Nat),
    specIsPrivileged act = true →
    specArgOk evilCid lg act toks hasPk now = false →
    checkGateSignedPk lg act (pfx ++ evilCid :: tl) toks hasPk now = false

/-- L25: Exact NON-CIRCULAR biconditional of `ledgerRecord` (incl. bit-7 `tainted` pinning) + deep bad-parent fail-closed. -/
def L25_Prop : Prop :=
  ∀ (lg : List LedgerNode) (cid : Nat) (base : Auth) (ps pPre : List Nat) (badP : Nat) (pTail : List Nat),
    isTainted (ledgerRecord lg cid base ps) =
      (specTainted base || specExistingTainted (specLedgerLookup lg cid) || specParentsTainted ps lg) ∧
    (ledgerRecord lg cid base ps).tainted =
      (specTainted base || specExistingTainted (specLedgerLookup lg cid) || specParentsTainted ps lg) ∧
    (specOptTainted (specLedgerLookup lg badP) = true →
      isTainted (ledgerRecord lg cid base (pPre ++ badP :: pTail)) = true)

/-- L26: Header metadata validation exact biconditional + boundary rejections. -/
def L26_Prop : Prop :=
  ∀ (low4 res3 epi flags r e f : Nat),
    isValidHeaderMeta low4 res3 epi flags = specValidHeaderMeta low4 res3 epi flags ∧
    isValidHeaderMeta 0 0 1 0 = false ∧
    isValidHeaderMeta (16 + r) 0 1 0 = false ∧
    isValidHeaderMeta 2 (r + 1) 1 0 = false ∧
    isValidHeaderMeta 2 0 0 0 = false ∧
    isValidHeaderMeta 2 0 (4 + e) 0 = false ∧
    isValidHeaderMeta 2 0 1 (f + 1) = false ∧
    isValidHeaderMeta 1 0 1 0 = true ∧
    isValidHeaderMeta 15 0 3 0 = true

/-- L27: Exact Ed25519 action-code bijection (`0..=4`, `>=5` rejected) AND full 6-arg biconditional
    MCP tool classifier specification (policy override > verb > allowRo|builtinRo|(trustAnn&&hint) > default-deny)! -/
def L27_Prop : Prop :=
  ∀ (a b : Action) (k : Nat) (pol verb : Option Action) (allowRo builtinRo trustAnn hint : Bool),
    actionToCode a = specActionToCode a ∧
    actionFromCode (actionToCode a) = some a ∧
    (a ≠ b → actionToCode a ≠ actionToCode b) ∧
    actionFromCode (5 + k) = none ∧
    classifyMcpTool pol verb allowRo builtinRo trustAnn hint =
      specClassifyMcpTool pol verb allowRo builtinRo trustAnn hint ∧
    specIsPrivileged (classifyMcpTool none none false false false hint) = true ∧
    classifyMcpTool pol verb allowRo builtinRo false hint =
      classifyMcpTool pol verb allowRo builtinRo false false

-- =============================================================================
-- GROUP 2: PLANE B 6-SYMBOL MEALY TRANSDUCER, R2 TABLES (`F9`), COMPACT, EXTRACT (7 LAWS)
-- =============================================================================

/-- L07: Full 6-symbol Mealy sanitizer eliminates `<<<`, `>>>`, `<|`, `|>`, AND all `spoof`. -/
def L07_Prop : Prop :=
  ∀ (xs : List Sym), hasForbidden (sanitizeFences xs) = false

/-- L08: Identity on every input already free of `<<<`, `>>>`, `<|`, `|>`, and `spoof`. -/
def L08_Prop : Prop :=
  ∀ (xs : List Sym), hasForbidden xs = false → sanitizeFences xs = xs

/-- L20: True content, original-escape & bounded-expansion preservation (using FROZEN `specStripEsc`/`specStripSpoof`)! -/
def L20_Prop : Prop :=
  ∀ (xs : List Sym),
    specStripEsc (sanitizeFences xs) = specStripEsc (specStripSpoof xs) ∧
    List.Sublist (specStripSpoof xs) (sanitizeFences xs) ∧
    (sanitizeFences xs).length <= 2 * (specStripSpoof xs).length

/-- L21: Structured `R2` rendering (`R2Scalar`, multi-cell `R2Table` `F4`+`F9`, `R2TensorRef` `F8`)
    NEVER emits forbidden sequences AND preserves BOTH `specStripEsc` AND full `List.Sublist` (original `\` kept)! -/
def L21_Prop : Prop :=
  ∀ (field : R2Field) (sk sv tk tcol tc1 tc2 rk rsp : List Sym),
    hasForbidden (renderR2Field field) = false ∧
    specStripEsc (renderR2Field (.scalar sk sv)) =
      specStripEsc (specStripSpoof sk) ++ (.other :: specStripEsc (specStripSpoof sv)) ∧
    List.Sublist (specStripSpoof sk ++ (.other :: specStripSpoof sv))
      (renderR2Field (.scalar sk sv)) ∧
    specStripEsc (renderR2Field (.table tk tcol tc1 tc2)) =
      specStripEsc (specStripSpoof tk) ++ (.other :: specStripEsc (specStripSpoof tcol) ++
        (.other :: specStripEsc (specStripSpoof tc1) ++ (.pipe :: specStripEsc (specStripSpoof tc2)))) ∧
    List.Sublist (specStripSpoof tk ++ (.other :: specStripSpoof tcol ++
        (.other :: specStripSpoof tc1 ++ (.pipe :: specStripSpoof tc2))))
      (renderR2Field (.table tk tcol tc1 tc2)) ∧
    specStripEsc (renderR2Field (.tensorRef rk rsp)) =
      specStripEsc (specStripSpoof rk) ++ (.other :: .lt :: .other :: specStripEsc (specStripSpoof rsp) ++ [.gt]) ∧
    List.Sublist (specStripSpoof rk ++ (.other :: .lt :: .other :: specStripSpoof rsp ++ [.gt]))
      (renderR2Field (.tensorRef rk rsp))

/-- L28: `R2-Compact` & `virtualAtlasMode` fail closed to `fullMealyFence` on untrusted/synth/tainted/mixed;
    prefix truncation preserves forbidden-sequence safety! -/
def L28_Prop : Prop :=
  ∀ (a : Auth) (c k n : Nat) (e t : Bool) (xs : List Sym),
    r2CompactMode { cleanBits := 1, ext := false, tainted := false } = .compactHeader ∧
    r2CompactMode { cleanBits := 2, ext := false, tainted := false } = .compactHeader ∧
    r2CompactMode { cleanBits := 0, ext := e, tainted := t } = .fullMealyFence ∧
    r2CompactMode { cleanBits := c, ext := true, tainted := t } = .fullMealyFence ∧
    r2CompactMode { cleanBits := c, ext := e, tainted := true } = .fullMealyFence ∧
    r2CompactMode { cleanBits := 3 + k, ext := e, tainted := t } = .fullMealyFence ∧
    virtualAtlasMode a = r2CompactMode a ∧
    hasForbidden ((sanitizeFences xs).take n) = false

/-- L29: Universal R2 table cell marker injectivity, universal `spoof`-stripping invariance (`F9`),
    and content preservation across ALL `raw : List Sym`! -/
def L29_Prop : Prop :=
  ∀ (c : TableCell) (raw : List Sym),
    cellTagHasForbidden (encodeTableCell c) = false ∧
    encodeTableCell .cellNull ≠ encodeTableCell (.cellStr raw) ∧
    encodeTableCell .cellMissing ≠ encodeTableCell (.cellStr raw) ∧
    encodeTableCell .cellNull ≠ encodeTableCell .cellMissing ∧
    encodeTableCell (.cellStr raw) = encodeTableCell (.cellStr (specStripSpoof raw)) ∧
    (specStripSpoof raw = [] → encodeTableCell (.cellStr raw) = .outEmptyQuoted) ∧
    (specStripSpoof raw = [.other] → encodeTableCell (.cellStr raw) = .outQuotedStr [.other]) ∧
    (specStripSpoof raw = [.lt, .other, .gt] →
      encodeTableCell (.cellStr raw) = .outQuotedStr [.lt, .other, .gt]) ∧
    specStripEsc (cellTagContent (encodeTableCell (.cellStr raw))) = specStripEsc (specStripSpoof raw) ∧
    List.Sublist (specStripSpoof raw) (cellTagContent (encodeTableCell (.cellStr raw)))

/-- L30: Strict lens tier validation (`0..=3`, `>=4` rejected) + universal `extractField`
    (empty path, path composition, deep map lookup, arbitrary array indexing, OOB & type mismatch fail-closed)! -/
def L30_Prop : Prop :=
  ∀ (t k a idx : Nat) (v : ValTree) (preMap postMap : List (Nat × ValTree))
    (preArr postArr : List ValTree) (seg : PathSeg) (p1 p2 : List PathSeg),
    isValidLensTier t = decide (t <= 3) ∧
    isValidLensTier (4 + k) = false ∧
    extractField v [] = some v ∧
    extractField v (p1 ++ p2) = (extractField v p1).bind (fun u => extractField u p2) ∧
    (specMapHasKey preMap k = false →
      extractField (.vMap (preMap ++ (k, v) :: postMap)) [.pKey k] = some v) ∧
    extractField (.vArr (preArr ++ v :: postArr)) [.pIdx preArr.length] = some v ∧
    extractField (.vArr preArr) [.pIdx (preArr.length + k)] = none ∧
    extractField (.vMap preMap) [.pIdx idx] = none ∧
    extractField (.vArr preArr) [.pKey k] = none ∧
    extractField (.vAtom a) (seg :: p1) = none

-- =============================================================================
-- GROUP 3: PLANE A CANONICAL WIRE, FLOAT64, MAP/DICT, SENTINELS & RCID (7 LAWS)
-- =============================================================================

/-- L09: Canonical integer round-trip identity. -/
def L09_Prop : Prop :=
  ∀ (lim ulim : Nat) (v : LogicalInt),
    decodeCanonInt lim ulim (encodeCanonInt lim ulim v) = some v

/-- L10: Zero-alloc verifier matches BOTH frozen `specValidWire` AND `decodeCanonInt`. -/
def L10_Prop : Prop :=
  ∀ (lim ulim : Nat) (w : WireInt),
    verifyCanonIntZeroAlloc lim ulim w = specValidWire lim ulim w ∧
    verifyCanonIntZeroAlloc lim ulim w = (decodeCanonInt lim ulim w).isSome

/-- L11: Canonical wire uniqueness (every accepted wire int re-encodes identically). -/
def L11_Prop : Prop :=
  ∀ (lim ulim : Nat) (w : WireInt) (v : LogicalInt),
    decodeCanonInt lim ulim w = some v → encodeCanonInt lim ulim v = w

/-- L22: Exact NON-CIRCULAR biconditional of `verifyMapKeys` & `allDictKeysUsed` + deep OOB/unsorted/missing-key rejection. -/
def L22_Prop : Prop :=
  ∀ (nkeys b d m : Nat) (ks pfx tl refs : List Nat),
    verifyMapKeys nkeys ks = specStrictlyAscFrom ks 0 nkeys ∧
    allDictKeysUsed nkeys refs = specAllLtIn nkeys refs ∧
    verifyMapKeys nkeys (pfx ++ (nkeys + d) :: tl) = false ∧
    verifyMapKeys nkeys (pfx ++ (b + d) :: b :: tl) = false ∧
    (m < nkeys → refs.contains m = false → allDictKeysUsed nkeys refs = false)

/-- L31: De-circularized Canonical `Float64`: exact equality against frozen `specCanonicalizeF64` /
    `specVerifyCanonF64`, idempotence, and strict rejection of `-0.0` and non-canonical NaNs! -/
def L31_Prop : Prop :=
  ∀ (f : FloatKind) (p : Nat),
    canonicalizeF64 f = specCanonicalizeF64 f ∧
    verifyCanonF64 f = specVerifyCanonF64 f ∧
    canonicalizeF64 (canonicalizeF64 f) = canonicalizeF64 f ∧
    verifyCanonF64 .fNegZero = false ∧
    verifyCanonF64 (.fNonCanonNaN p) = false

/-- L32: Lossless JSON sentinel round-trip + malformed sentinel fail-closed (`F6`). -/
def L32_Prop : Prop :=
  ∀ (v : SentinelVal) (r c a dt kk p : Nat),
    parseSentinelJson (emitSentinelJson v) = some v ∧
    parseSentinelJson (.jBytesObj false r) = none ∧
    parseSentinelJson (.jFloatObj false c) = none ∧
    parseSentinelJson (.jTensorObj false a dt) = none ∧
    parseSentinelJson (.jMapWrap false kk p) = none

/-- L33: Universal `resolvedFrameCid`: exact frozen spec, universal repack invariance (`ts ++ extra`),
    universal OOB rejection at ANY ref position, and arbitrary-position frame/tensor binding sensitivity! -/
def L33_Prop : Prop :=
  ∀ (fcid l m d : Nat) (refs rPre rTail : List Nat) (ts pre post extra : List TensorDesc) (r : ResolvedCid),
    resolvedFrameCid fcid refs ts = specResolvedFrameCid fcid refs ts ∧
    (resolvedFrameCid fcid refs ts = some r → resolvedFrameCid fcid refs (ts ++ extra) = some r) ∧
    resolvedFrameCid fcid (rPre ++ (ts.length + d) :: rTail) ts = none ∧
    (∀ (t : ResolvedCid), resolvedFrameCid fcid refs ts = some t →
      resolvedFrameCid fcid refs ts ≠ resolvedFrameCid (fcid + d + 1) refs ts) ∧
    (resolvedFrameCid fcid (rPre ++ pre.length :: rTail) (pre ++ { byteLen := l, merkle4m := m } :: post) ≠
      resolvedFrameCid fcid (rPre ++ pre.length :: rTail) (pre ++ { byteLen := l + d + 1, merkle4m := m } :: post) ∨
      resolvedFrameCid fcid (rPre ++ pre.length :: rTail) (pre ++ { byteLen := l, merkle4m := m } :: post) = none) ∧
    (resolvedFrameCid fcid [pre.length] (pre ++ { byteLen := l, merkle4m := m } :: post) ≠
      resolvedFrameCid fcid [pre.length] (pre ++ { byteLen := l + d + 1, merkle4m := m } :: post)) ∧
    (resolvedFrameCid fcid [pre.length] (pre ++ { byteLen := l, merkle4m := m } :: post) ≠
      resolvedFrameCid fcid [pre.length] (pre ++ { byteLen := l, merkle4m := m + d + 1 } :: post))

-- =============================================================================
-- GROUP 4: `CWZM` BINARY-SKIP, MERKLE, `.cwwal`, `LiveIvfStore` & `CWSP` (8 LAWS)
-- =============================================================================

/-- L12: Exact `zoneOverlaps` biconditional + single-page zero false-negative over signed `Int` keys. -/
def L12_Prop : Prop :=
  ∀ (mn mx k0 lo hi : Int) (ks : List Int),
    zoneOverlaps { minK := mn, maxK := mx } lo hi = (decide (lo <= mx) && decide (mn <= hi)) ∧
    (specPageHasInRange k0 ks lo hi = true → zoneOverlaps (buildZone k0 ks) lo hi = true)

/-- L23: Multi-page signed-Int `CWZM` binary-skip + early-break matches frozen `specFilterZonesAt`
    EXACTLY whenever `lo <= hi` and zones are disjoint monotonic, AND returns `[]` on inverted `hi < lo`! -/
def L23_Prop : Prop :=
  ∀ (zs : List Zone) (lo hi : Int),
    (lo <= hi → specZonesDisjoint zs = true →
      queryKeyRangePages zs lo hi = specFilterZonesAt zs 0 lo hi) ∧
    (hi < lo → queryKeyRangePages zs lo hi = [])

/-- L13: RFC 6962 `0x02`/`0x04` domain separation, exact `shardMerkleRoot` count+tree binding,
    AND direct `merkleTree` promote-odd non-collision! -/
def L13_Prop : Prop :=
  ∀ (f1 f2 : List Digest) (t0 : Digest) (trest : List Digest) (a b c : Nat),
    shardMerkleRoot f1 [] = .hRoot02 f1.length (merkleTree f1) ∧
    shardMerkleRoot f2 (t0 :: trest) =
      .hRoot04 f2.length (merkleTree f2) (trest.length + 1) (merkleTree (t0 :: trest)) ∧
    shardMerkleRoot f1 [] ≠ shardMerkleRoot f2 (t0 :: trest) ∧
    merkleTree [.hLeaf00 a, .hLeaf00 b, .hLeaf00 c] ≠
      merkleTree [.hLeaf00 a, .hLeaf00 b, .hLeaf00 c, .hLeaf00 c] ∧
    merkleTree [] = .hEmpty ∧
    merkleTree [.hLeaf00 a] = .hLeaf00 a ∧
    merkleTree [.hLeaf00 a, .hLeaf00 b] = .hNode01 (.hLeaf00 a) (.hLeaf00 b)

/-- L34: Merkle leaf-payload sensitivity (incl. frame binding inside `shardMerkleRoot`) & `hTensor03` binding. -/
def L34_Prop : Prop :=
  ∀ (a b d bl m4m : Nat) (fs : List Digest),
    merkleTree [.hLeaf00 a, .hLeaf00 b] ≠ merkleTree [.hLeaf00 (a + d + 1), .hLeaf00 b] ∧
    merkleTree [.hLeaf00 a, .hLeaf00 b] ≠ merkleTree [.hLeaf00 a, .hLeaf00 (b + d + 1)] ∧
    shardMerkleRoot [.hLeaf00 a] [] ≠ shardMerkleRoot [.hLeaf00 (a + d + 1)] [] ∧
    shardMerkleRoot fs [.hTensor03 bl m4m] ≠ shardMerkleRoot fs [.hTensor03 bl (m4m + d + 1)]

/-- L14: Complete `.cwwal` recovery: exact biconditional against frozen `specWalRecoverMode`
    (covering BOTH rollback `badSeq < expSeq` AND forward gap `badSeq > expSeq`, arbitrary `tornSeq`,
    and BOTH `recoverTorn ∈ {true, false}`), plus explicit named safety conjuncts! -/
def L14_Prop : Prop :=
  ∀ (recs : List WalRec) (pfx : List Nat) (startSeq tornSeq badSeq tp bp : Nat)
    (after : WalRec) (more : List WalRec) (mode : Bool),
    walRecoverMode recs startSeq mode = specWalRecoverMode recs startSeq mode ∧
    walRecoverMode (specWalValidPrefix pfx startSeq []) startSeq mode = .walOk pfx ∧
    walRecoverMode (specWalValidPrefix pfx startSeq [{ valid := false, seq := tornSeq, payload := tp }]) startSeq true = .walOk pfx ∧
    walRecoverMode (specWalValidPrefix pfx startSeq [{ valid := false, seq := tornSeq, payload := tp }]) startSeq false = .walErr ∧
    walRecoverMode (specWalValidPrefix pfx startSeq ({ valid := false, seq := tornSeq, payload := bp } :: after :: more)) startSeq mode = .walErr ∧
    (badSeq ≠ startSeq + pfx.length →
      walRecoverMode (specWalValidPrefix pfx startSeq [{ valid := true, seq := badSeq, payload := tp }]) startSeq mode = .walErr)

/-- L24: Universally quantified sequential WAL replay (homomorphism, tombstone revokes target & preserves
    ANY bystander `b ≠ c`, frame revives with exact count `1` & preserves ANY bystander) + `StatePage` `runtime_id`. -/
def L24_Prop : Prop :=
  ∀ (ops1 ops2 : List WalOp) (live : List Nat) (c b : Nat) (expRt fileRt : Option Nat),
    walReplayLive (ops1 ++ ops2) live = walReplayLive ops2 (walReplayLive ops1 live) ∧
    (c ∉ walReplayLive (ops1 ++ [.opTomb c]) live) ∧
    (b ≠ c → (b ∈ walReplayLive (ops1 ++ [.opTomb c]) live ↔ b ∈ walReplayLive ops1 live)) ∧
    (walReplayLive (ops1 ++ [.opFrame c]) live).count c = 1 ∧
    (b ≠ c → (b ∈ walReplayLive (ops1 ++ [.opFrame c]) live ↔ b ∈ walReplayLive ops1 live)) ∧
    checkStatepageRuntime expRt fileRt = specSpRuntime expRt fileRt

/-- L35: `LiveIvfStore` crash-safe `.cwgen` generation compaction & `CWAL v2` watermark (`F10`). -/
def L35_Prop : Prop :=
  ∀ (base compIds postIds : List Nat) (s0 : Nat),
    let exp := base ++ compIds ++ postIds
    let w := s0 + compIds.length
    liveIvfRecover base s0 (ivfMakeSeqRecs (compIds ++ postIds) s0) = exp ∧
    liveIvfRecover (base ++ compIds) w (ivfMakeSeqRecs (compIds ++ postIds) s0) = exp ∧
    liveIvfRecover (base ++ compIds) w (ivfMakeSeqRecs postIds w) = exp

/-- L36: `StatePage` HMAC-first error precedence (`F13`), all 9 exhaustive branches. -/
def L36_Prop : Prop :=
  ∀ (k : Nat) (mv hv : Bool),
    statepageParsePrecedence true 1 false hv = .spAuthMacMismatch ∧
    statepageParsePrecedence true 0 mv hv = .spAuthMacMismatch ∧
    statepageParsePrecedence true (k + 2) mv hv = .spAuthMacMismatch ∧
    statepageParsePrecedence false 1 mv hv = .spAuthMacMismatch ∧
    statepageParsePrecedence true 1 true true = .spOk ∧
    statepageParsePrecedence true 1 true false = .spInvalidContract ∧
    statepageParsePrecedence false 0 mv true = .spOk ∧
    statepageParsePrecedence false 0 mv false = .spInvalidContract ∧
    statepageParsePrecedence false (k + 2) mv hv = .spNonCanonicalFlags

-- =============================================================================
-- GROUP 5: LEAN-EXCLUSIVE STRENGTHENINGS (3 LAWS: `L37`..`L39`)
-- =============================================================================

/-- L37 (Lean-Exclusive, De-Circularized): Exact fixed-width Rust machine-word theorems pinned to
    frozen literal specs in `Cowrie.Laws`:
    1. `UInt8` `isTaintedByte` (`0x84`), `propagateByte` (`0x80` sticky + `0x7F` origin), & `isValidHeaderMetaByte`,
    2. Exact 64-bit `Int64 ↔ UInt64` ZigZag bijection pinned to arithmetic `specZigzag64` (incl. `Int64.minValue`),
    3. Exact 64-bit IEEE-754 `UInt64` `canonicalizeF64Bits` / `verifyCanonF64Bits` pinned to frozen masks + idempotence! -/
def L37_Prop : Prop :=
  (∀ (a base epi flags : UInt8) (ps : List UInt8),
    isTaintedByte a = specIsTaintedByte a ∧
    propagateByte base ps = specPropagateByte base ps ∧
    isValidHeaderMetaByte a epi flags = specIsValidHeaderMetaByte a epi flags ∧
    specIsTaintedByte (propagateByte base ps) = (specIsTaintedByte base || ps.any specIsTaintedByte) ∧
    (propagateByte base ps &&& 0x7F) = (base &&& 0x7F)) ∧
  (∀ (i : Int64), zigzagEncode64 i = specZigzag64 i ∧ zigzagDecode64 (zigzagEncode64 i) = i) ∧
  (∀ (u : UInt64), zigzagEncode64 (zigzagDecode64 u) = u) ∧
  (∀ (b : UInt64),
    canonicalizeF64Bits b = specCanonF64Bits b ∧
    verifyCanonF64Bits b = specVerifyCanonF64Bits b ∧
    verifyCanonF64Bits (canonicalizeF64Bits b) = true ∧
    canonicalizeF64Bits (canonicalizeF64Bits b) = canonicalizeF64Bits b ∧
    (verifyCanonF64Bits b = true ↔ canonicalizeF64Bits b = b) ∧
    verifyCanonF64Bits 0x8000000000000000 = false)

/-- L38 (Lean-Exclusive): Fuel-free well-founded RFC 6962 UNIVERSAL Merkle injectivity across ALL
    lengths `0, 1, 2, ..., ∞`: zero structural collisions for ANY atomic-leaf frame & tensor lists! -/
def L38_Prop : Prop :=
  (∀ (xs ys : List Digest),
    (∀ d ∈ xs, isAtomicLeaf d) →
    (∀ d ∈ ys, isAtomicLeaf d) →
    merkleTree xs = merkleTree ys → xs = ys) ∧
  (∀ (f1 t1 f2 t2 : List Digest),
    (∀ d ∈ f1, isAtomicLeaf d) → (∀ d ∈ t1, isAtomicLeaf d) →
    (∀ d ∈ f2, isAtomicLeaf d) → (∀ d ∈ t2, isAtomicLeaf d) →
    shardMerkleRoot f1 t1 = shardMerkleRoot f2 t2 → f1 = f2 ∧ t1 = t2)

/-- L39 (Lean-Exclusive): Declarative `List.Infix` (`<:+:`) non-occurrence of `<<<`, `>>>`, `<|`, `|>`
    over BOTH full output AND every prefix `.take n` (justifying `F17` cap truncation!),
    zero `spoof`, `List.Sublist` preservation (frozen `specStripSpoof`), bounded expansion `<= 2x`, AND idempotence! -/
def L39_Prop : Prop :=
  ∀ (xs : List Sym) (n : Nat),
    ¬ ([.lt, .lt, .lt] <:+: (sanitizeFences xs).take n) ∧
    ¬ ([.gt, .gt, .gt] <:+: (sanitizeFences xs).take n) ∧
    ¬ ([.lt, .pipe] <:+: (sanitizeFences xs).take n) ∧
    ¬ ([.pipe, .gt] <:+: (sanitizeFences xs).take n) ∧
    (∀ s ∈ sanitizeFences xs, s ≠ .spoof) ∧
    List.Sublist (specStripSpoof xs) (sanitizeFences xs) ∧
    (sanitizeFences xs).length <= 2 * (specStripSpoof xs).length ∧
    sanitizeFences (sanitizeFences xs) = sanitizeFences xs

/-- Bundled witness aggregating all 40 machine-checked laws. -/
structure CowrieSpec : Prop where
  l01  : L01_Prop
  l02  : L02_Prop
  l03  : L03_Prop
  l04  : L04_Prop
  l05  : L05_Prop
  l06a : L06a_Prop
  l06b : L06b_Prop
  l07  : L07_Prop
  l08  : L08_Prop
  l09  : L09_Prop
  l10  : L10_Prop
  l11  : L11_Prop
  l12  : L12_Prop
  l13  : L13_Prop
  l14  : L14_Prop
  l15  : L15_Prop
  l16  : L16_Prop
  l17  : L17_Prop
  l18  : L18_Prop
  l19  : L19_Prop
  l20  : L20_Prop
  l21  : L21_Prop
  l22  : L22_Prop
  l23  : L23_Prop
  l24  : L24_Prop
  l25  : L25_Prop
  l26  : L26_Prop
  l27  : L27_Prop
  l28  : L28_Prop
  l29  : L29_Prop
  l30  : L30_Prop
  l31  : L31_Prop
  l32  : L32_Prop
  l33  : L33_Prop
  l34  : L34_Prop
  l35  : L35_Prop
  l36  : L36_Prop
  l37  : L37_Prop
  l38  : L38_Prop
  l39  : L39_Prop

end Cowrie.Laws
