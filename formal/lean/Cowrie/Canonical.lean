/-
  Cowrie/Canonical.lean — Plane A Canonical Wire, Exact 64-bit ZigZag & IEEE-754 Float64,
  Map Key Sorting & `UnusedDictKey`, Lossless JSON Sentinels, & `resolved_frame_cid`
  (Lean 4.34, Init-only)
-/

namespace Cowrie.Canonical

-- Parameterized canonical integer bijection matching `SPEC.bend` / `canonical.rs`
inductive LogicalInt where
  | lPos (n : Nat)
  | lNeg (m : Nat)
  | lBigUint (delta : Nat)
  deriving DecidableEq, Repr

inductive WireInt where
  | wFix (n : Nat)
  | wTagIntPos (n : Nat)
  | wTagIntNeg (m : Nat)
  | wTagUint (u : Nat)
  deriving DecidableEq, Repr

def encodeCanonInt (lim ulim : Nat) (v : LogicalInt) : WireInt :=
  match v with
  | .lPos n     => if n < lim then .wFix n else .wTagIntPos n
  | .lNeg m     => .wTagIntNeg m
  | .lBigUint d => .wTagUint (ulim + d)

def decodeCanonInt (lim ulim : Nat) (w : WireInt) : Option LogicalInt :=
  match w with
  | .wFix n       => if n < lim then some (.lPos n) else none
  | .wTagIntPos n => if n < lim then none else some (.lPos n)
  | .wTagIntNeg m => some (.lNeg m)
  | .wTagUint u   => if u < ulim then none else some (.lBigUint (u - ulim))

def verifyCanonIntZeroAlloc (lim ulim : Nat) (w : WireInt) : Bool :=
  match w with
  | .wFix n       => decide (n < lim)
  | .wTagIntPos n => decide (!(n < lim))
  | .wTagIntNeg _ => true
  | .wTagUint u   => decide (!(u < ulim))

-- Exact 64-bit ZigZag bijectivity (`canonical.rs::zigzag_encode` / `zigzag_decode`)!
def zigzagEncode64 (i : Int64) : UInt64 :=
  UInt64.ofBitVec ((i.toBitVec <<< 1) ^^^ (i.toBitVec.sshiftRight 63))

def zigzagDecode64 (u : UInt64) : Int64 :=
  Int64.ofBitVec ((u.toBitVec >>> 1) ^^^ (0 - (u.toBitVec &&& 1#64)))

-- Exact IEEE-754 64-bit Float canonicalization (`canonical.rs:168-178, 564-580`)!
def CANON_QNAN_BITS : UInt64 := 0x7FF8000000000000
def NEG_ZERO_BITS   : UInt64 := 0x8000000000000000
def EXP_MASK_BITS   : UInt64 := 0x7FF0000000000000
def FRAC_MASK_BITS  : UInt64 := 0x000FFFFFFFFFFFFF

def isNaNBits (b : UInt64) : Bool :=
  ((b &&& EXP_MASK_BITS) == EXP_MASK_BITS) && ((b &&& FRAC_MASK_BITS) != 0)

def canonicalizeF64Bits (b : UInt64) : UInt64 :=
  if b == NEG_ZERO_BITS then
    0
  else if isNaNBits b then
    CANON_QNAN_BITS
  else
    b

def verifyCanonF64Bits (b : UInt64) : Bool :=
  if b == NEG_ZERO_BITS then
    false
  else if isNaNBits b then
    b == CANON_QNAN_BITS
  else
    true

-- Algebraic `FloatKind` matching `SPEC.bend` (`L31`)
inductive FloatKind where
  | fPosZero
  | fNegZero
  | fFinite (bits : Nat)
  | fInf (pos : Bool)
  | fCanonQNaN
  | fNonCanonNaN (payload : Nat)
  deriving DecidableEq, Repr

def canonicalizeF64 (f : FloatKind) : FloatKind :=
  match f with
  | .fPosZero       => .fPosZero
  | .fNegZero       => .fPosZero
  | .fFinite b      => .fFinite b
  | .fInf p         => .fInf p
  | .fCanonQNaN     => .fCanonQNaN
  | .fNonCanonNaN _ => .fCanonQNaN

def verifyCanonF64 (f : FloatKind) : Bool :=
  match f with
  | .fNegZero       => false
  | .fNonCanonNaN _ => false
  | _               => true

-- Map Key Strictly-Ascending Index Verifier & `KeyBitset` Unused-Key Verifier (`L22`)
def verifyMapKeysFrom (ks : List Nat) (minAllowed nkeys : Nat) : Bool :=
  match ks with
  | [] => true
  | k :: rest =>
      decide (minAllowed <= k) && decide (k < nkeys) && verifyMapKeysFrom rest (k + 1) nkeys

def verifyMapKeys (nkeys : Nat) (ks : List Nat) : Bool :=
  verifyMapKeysFrom ks 0 nkeys

def allDictKeysUsed (nkeys : Nat) (refs : List Nat) : Bool :=
  match nkeys with
  | 0 => true
  | pk + 1 => refs.contains pk && allDictKeysUsed pk refs

-- Lossless JSON Sentinels (`$map`, `$float`, `$bytes`, `$tensor`, `L32`, `F6`)
inductive SentinelVal where
  | svPlain (tag : Nat)
  | svBytes (raw : Nat)
  | svFloatSpecial (code : Nat)
  | svTensor (arena dtype : Nat)
  | svMapPlain (payload : Nat)
  | svMapReserved (keyKind payload : Nat)
  deriving DecidableEq, Repr

inductive JsonSentinel where
  | jPlain (tag : Nat)
  | jBytesObj (ok : Bool) (raw : Nat)
  | jFloatObj (ok : Bool) (code : Nat)
  | jTensorObj (ok : Bool) (arena dtype : Nat)
  | jMapPlain (payload : Nat)
  | jMapWrap (ok : Bool) (keyKind payload : Nat)
  deriving DecidableEq, Repr

def emitSentinelJson (v : SentinelVal) : JsonSentinel :=
  match v with
  | .svPlain t         => .jPlain t
  | .svBytes r         => .jBytesObj true r
  | .svFloatSpecial c  => .jFloatObj true c
  | .svTensor a dt     => .jTensorObj true a dt
  | .svMapPlain p      => .jMapPlain p
  | .svMapReserved k p => .jMapWrap true k p

def parseSentinelJson (j : JsonSentinel) : Option SentinelVal :=
  match j with
  | .jPlain t               => some (.svPlain t)
  | .jBytesObj true r       => some (.svBytes r)
  | .jBytesObj false _      => none
  | .jFloatObj true c       => some (.svFloatSpecial c)
  | .jFloatObj false _      => none
  | .jTensorObj true a dt   => some (.svTensor a dt)
  | .jTensorObj false _ _   => none
  | .jMapPlain p            => some (.svMapPlain p)
  | .jMapWrap true k p      => some (.svMapReserved k p)
  | .jMapWrap false _ _     => none

-- Plane A/C `resolved_frame_cid` (`shard.rs:432-474`, `L33`)
structure TensorDesc where
  byteLen  : Nat
  merkle4m : Nat
  deriving DecidableEq, Repr

structure BoundTensor where
  idx : Nat
  td  : TensorDesc
  deriving DecidableEq, Repr

structure ResolvedCid where
  frameCid     : Nat
  boundTensors : List BoundTensor
  deriving DecidableEq, Repr

def lookupTensor (ts : List TensorDesc) (idx : Nat) : Option TensorDesc :=
  match ts, idx with
  | [], _ => none
  | h :: _, 0 => some h
  | _ :: t, i + 1 => lookupTensor t i

def resolveTensorRefs (refs : List Nat) (ts : List TensorDesc) : Option (List BoundTensor) :=
  match refs with
  | [] => some []
  | idx :: rest =>
      match lookupTensor ts idx, resolveTensorRefs rest ts with
      | some td, some more => some ({ idx := idx, td := td } :: more)
      | _, _               => none

def resolvedFrameCid (frameCid : Nat) (refs : List Nat) (ts : List TensorDesc) : Option ResolvedCid :=
  match resolveTensorRefs refs ts with
  | none       => none
  | some bound => some { frameCid := frameCid, boundTensors := bound }

end Cowrie.Canonical
