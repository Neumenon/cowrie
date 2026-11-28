/-
  Cowrie/Trust.lean — Plane D Executable Reference Kernel (Lean 4.34, Init-only)

  Models exact Rust `UInt8` authority bitfields (`trust.rs` / `canonical.rs`), sticky multi-step
  `Ledger`, fail-closed `owner_pubkey` Ed25519 capability gate, Plane A/D header metadata validation,
  RFC 8032 `COWRIE_DECLASSIFY_V1` action codes (`ed25519.rs`), and default-deny MCP tool classifier.
-/

namespace Cowrie.Trust

-- Exact Rust `UInt8` authority constants (`canonical.rs`, `trust.rs`)
def AUTH_SYS_POLICY    : UInt8 := 0x01
def AUTH_VERIFIED_TOOL : UInt8 := 0x02
def AUTH_UNTRUSTED_EXT : UInt8 := 0x04
def AUTH_MODEL_SYNTH   : UInt8 := 0x08
def BIT_TAINTED        : UInt8 := 0x80

/-- Exact `UInt8` taint predicate matching `trust.rs::is_tainted`: `(auth & 0x84) != 0`. -/
def isTaintedByte (a : UInt8) : Bool :=
  (a &&& (BIT_TAINTED ||| AUTH_UNTRUSTED_EXT)) != 0

/-- Exact `UInt8` causal taint propagation matching `trust.rs::propagate_authority`. -/
def propagateByte (base : UInt8) (parents : List UInt8) : UInt8 :=
  if isTaintedByte base || parents.any isTaintedByte then
    base ||| BIT_TAINTED
  else
    base

/-- Exact `UInt8` header metadata validation matching `canonical.rs::verify_header_meta`. -/
def isValidHeaderMetaByte (auth epi flags : UInt8) : Bool :=
  let low4 := auth &&& 0x0F
  let res3 := auth &&& 0x70
  flags == 0 && res3 == 0 && 1 <= low4 && low4 <= 15 && 1 <= epi && epi <= 3

-- Structured algebraic `Auth` matching `SPEC.bend` for 1-to-1 Bend <-> Lean correspondence
structure Auth where
  cleanBits : Nat
  ext       : Bool
  tainted   : Bool
  deriving DecidableEq, Repr

def isTainted (a : Auth) : Bool :=
  a.ext || a.tainted

def setTainted (a : Auth) : Auth :=
  { a with tainted := true }

def propApply (flag : Bool) (base : Auth) : Auth :=
  if flag then setTainted base else base

def anyTainted (ps : List Auth) : Bool :=
  ps.any isTainted

def propagate (base : Auth) (ps : List Auth) : Auth :=
  propApply (isTainted base || anyTainted ps) base

def deriveChain (hops : List Auth) (cur : Auth) : Auth :=
  match hops with
  | [] => cur
  | stepBase :: rest => deriveChain rest (propagate stepBase [cur])

structure LedgerNode where
  cid  : Nat
  auth : Auth
  deriving DecidableEq, Repr

def ledgerLookup (lg : List LedgerNode) (cid : Nat) : Option Auth :=
  match lg with
  | [] => none
  | n :: rest => if n.cid == cid then some n.auth else ledgerLookup rest cid

def optAuthTainted (ma : Option Auth) : Bool :=
  match ma with
  | none   => true
  | some a => isTainted a

def existingTainted (ma : Option Auth) : Bool :=
  match ma with
  | none   => false
  | some a => isTainted a

def parentsTainted (ps : List Nat) (lg : List LedgerNode) : Bool :=
  ps.any (fun p => optAuthTainted (ledgerLookup lg p))

def ledgerRecord (lg : List LedgerNode) (cid : Nat) (base : Auth) (ps : List Nat) : Auth :=
  let oldT := existingTainted (ledgerLookup lg cid)
  let parT := parentsTainted ps lg
  propApply (isTainted base || oldT || parT) base

/-- Stateful multi-step ledger insertion (models `TrustLedger::record` appending/updating state). -/
def ledgerInsert (lg : List LedgerNode) (cid : Nat) (base : Auth) (ps : List Nat) : List LedgerNode :=
  { cid := cid, auth := ledgerRecord lg cid base ps } :: lg

inductive Action where
  | readOnly
  | shellExec
  | credRead
  | fsWrite
  | netExfil
  deriving DecidableEq, Repr

def isPrivileged (act : Action) : Bool :=
  match act with
  | .readOnly => false
  | _         => true

structure DeclassToken where
  cid      : Nat
  act      : Action
  expiry   : Nat
  sigValid : Bool
  deriving DecidableEq, Repr

def tokenMatches (tok : DeclassToken) (cid : Nat) (act : Action) (now : Nat) : Bool :=
  tok.sigValid && (tok.cid == cid) && (tok.act == act) && decide (now <= tok.expiry)

def hasValidToken (toks : List DeclassToken) (cid : Nat) (act : Action) (now : Nat) : Bool :=
  toks.any (fun tk => tokenMatches tk cid act now)

def argAuthorized (cid : Nat) (lg : List LedgerNode) (act : Action)
    (toks : List DeclassToken) (hasPk : Bool) (now : Nat) : Bool :=
  if optAuthTainted (ledgerLookup lg cid) then
    hasPk && hasValidToken toks cid act now
  else
    true

def checkGateSignedPk (lg : List LedgerNode) (act : Action) (args : List Nat)
    (toks : List DeclassToken) (hasPk : Bool) (now : Nat) : Bool :=
  if !isPrivileged act then
    true
  else
    match args with
    | [] => false
    | _  => args.all (fun c => argAuthorized c lg act toks hasPk now)

def checkGateSigned (lg : List LedgerNode) (act : Action) (args : List Nat)
    (toks : List DeclassToken) (now : Nat) : Bool :=
  checkGateSignedPk lg act args toks true now

def isValidHeaderMeta (low4 res3 epi flags : Nat) : Bool :=
  (flags == 0) && (res3 == 0) && decide (1 <= low4) && decide (low4 <= 15) &&
    decide (1 <= epi) && decide (epi <= 3)

def actionToCode (act : Action) : Nat :=
  match act with
  | .readOnly  => 0
  | .shellExec => 1
  | .credRead  => 2
  | .fsWrite   => 3
  | .netExfil  => 4

def actionFromCode (code : Nat) : Option Action :=
  match code with
  | 0 => some .readOnly
  | 1 => some .shellExec
  | 2 => some .credRead
  | 3 => some .fsWrite
  | 4 => some .netExfil
  | _ => none

def classifyMcpTool (polOverride verbPriv : Option Action)
    (allowRo builtinRo trustAnn serverRoHint : Bool) : Action :=
  match polOverride, verbPriv with
  | some act, _     => act
  | none, some vact => vact
  | none, none =>
      if allowRo || builtinRo || (trustAnn && serverRoHint) then
        .readOnly
      else
        .shellExec

end Cowrie.Trust
