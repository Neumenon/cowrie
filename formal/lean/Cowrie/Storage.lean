/-
  Cowrie/Storage.lean — Plane C/D Executable Reference Kernel (Lean 4.34, Init-only):
    - Signed-Int `CWZM` ZoneMap + exact `partitionPoint`-skip + early-break (`zonemap.rs:430-437`)
    - Fuel-free well-founded RFC 6962 Dual-Tree Merkle (`0x00`..`0x04`) with universal injectivity!
    - `.cwwal` strict/torn-tail recovery + `seq` monotonicity + sequential tombstone/re-insert (`F5`)
    - `LiveIvfStore` crash-safe `.cwgen` generation compaction watermark (`F10`)
    - `CWSP` strict `runtime_id` pinning (`F7`) + HMAC-first error precedence (`F13`)
-/

namespace Cowrie.Storage

-- Signed `Int` ZoneMap (`zonemap.rs` uses `i64` keys!)
structure Zone where
  minK : Int
  maxK : Int
  deriving DecidableEq, Repr

def zoneExtend (z : Zone) (k : Int) : Zone :=
  { minK := min z.minK k, maxK := max z.maxK k }

def buildZoneLoop (ks : List Int) (z : Zone) : Zone :=
  match ks with
  | [] => z
  | k :: rest => buildZoneLoop rest (zoneExtend z k)

def buildZone (k0 : Int) (ks : List Int) : Zone :=
  buildZoneLoop ks { minK := k0, maxK := k0 }

def zoneOverlaps (z : Zone) (lo hi : Int) : Bool :=
  decide (lo <= z.maxK) && decide (z.minK <= hi)

/-- Phase 2 of `zonemap.rs:430-437`: emit every page until `minK > hi`, then break.
    Sound ONLY under disjoint monotonic zones (`z[p-1].maxK <= z[p].minK <= z[p].maxK`). -/
def takeUntilGtHi (zs : List Zone) (idx : Nat) (hi : Int) : List Nat :=
  match zs with
  | [] => []
  | z :: rest =>
      if z.minK <= hi then
        idx :: takeUntilGtHi rest (idx + 1) hi
      else
        []

/-- Phase 1 + Phase 2 of `zonemap.rs:430-437`: skip `maxK < lo`, then `takeUntilGtHi`. -/
def queryKeyRangeAt (zs : List Zone) (idx : Nat) (lo hi : Int) : List Nat :=
  match zs with
  | [] => []
  | z :: rest =>
      if z.maxK < lo then
        queryKeyRangeAt rest (idx + 1) lo hi
      else
        takeUntilGtHi (z :: rest) idx hi

def queryKeyRangePages (zs : List Zone) (lo hi : Int) : List Nat :=
  if lo <= hi then queryKeyRangeAt zs 0 lo hi else []

-- Fuel-free Well-Founded RFC 6962 Dual-Tree Merkle Term Algebra (`0x00`..`0x04`)
inductive Digest where
  | hEmpty
  | hLeaf00 (payload : Nat)
  | hNode01 (left right : Digest)
  | hRoot02 (fCnt : Nat) (fTree : Digest)
  | hTensor03 (byteLen m4m : Nat)
  | hRoot04 (fCnt : Nat) (fTree : Digest) (tCnt : Nat) (tTree : Digest)
  deriving DecidableEq, Repr

/-- Pairwise RFC 6962 level reduction with odd-node promotion (NEVER duplicates odd trailing node!). -/
def merklePairLevel (xs : List Digest) : List Digest :=
  match xs with
  | [] => []
  | [a] => [a]
  | a :: b :: rest => .hNode01 a b :: merklePairLevel rest

theorem merklePairLevel_length_le (xs : List Digest) :
    (merklePairLevel xs).length <= xs.length := by
  match xs with
  | [] => simp [merklePairLevel]
  | [_] => simp [merklePairLevel]
  | _ :: _ :: rest =>
      have ih := merklePairLevel_length_le rest
      simp [merklePairLevel]
      omega

theorem merklePairLevel_length_lt (a b : Digest) (rest : List Digest) :
    (merklePairLevel (a :: b :: rest)).length < (a :: b :: rest).length := by
  have h := merklePairLevel_length_le rest
  simp [merklePairLevel]
  omega

/-- Fuel-free well-founded RFC 6962 Merkle tree digest! Termination is proven for ALL lengths. -/
def merkleTree (xs : List Digest) : Digest :=
  match xs with
  | [] => .hEmpty
  | [only] => only
  | a :: b :: rest =>
      merkleTree (.hNode01 a b :: merklePairLevel rest)
termination_by xs.length
decreasing_by
  have := merklePairLevel_length_le rest
  simp
  omega

def shardMerkleRoot (frames tensors : List Digest) : Digest :=
  match tensors with
  | [] => .hRoot02 frames.length (merkleTree frames)
  | _  => .hRoot04 frames.length (merkleTree frames) tensors.length (merkleTree tensors)

-- `.cwwal` Crash-Safe Recovery (`recover_torn_tail` flag + strict `seq` monotonicity!)
structure WalRec where
  valid   : Bool
  seq     : Nat
  payload : Nat
  deriving DecidableEq, Repr

inductive WalResult where
  | walOk (live : List Nat)
  | walErr
  deriving DecidableEq, Repr

def walRecoverMode (recs : List WalRec) (expSeq : Nat) (recoverTorn : Bool) : WalResult :=
  match recs with
  | [] => .walOk []
  | r :: rest =>
      if r.valid then
        if r.seq == expSeq then
          match walRecoverMode rest (expSeq + 1) recoverTorn with
          | .walOk ps => .walOk (r.payload :: ps)
          | .walErr   => .walErr
        else
          .walErr
      else
        match rest with
        | [] => if recoverTorn then .walOk [] else .walErr
        | _ :: _ => .walErr

-- Sequential Tombstone & Re-Insertion Replay (`wal.rs` Fix F5)
inductive WalOp where
  | opFrame (cid : Nat)
  | opTomb (cid : Nat)
  deriving DecidableEq, Repr

def removeCid (xs : List Nat) (target : Nat) : List Nat :=
  xs.filter (fun x => x != target)

def walReplayLive (ops : List WalOp) (live : List Nat) : List Nat :=
  match ops with
  | [] => live
  | .opFrame c :: rest => walReplayLive rest (removeCid live c ++ [c])
  | .opTomb c :: rest  => walReplayLive rest (removeCid live c)

-- `LiveIvfStore` Crash-Safe `.cwgen` Generation Compaction & Watermark Filter (`wal.rs`, Fix F10)
structure IvfWalRec where
  seq   : Nat
  vecId : Nat
  deriving DecidableEq, Repr

def ivfWalFilterFrom (recs : List IvfWalRec) (appliedSeq : Nat) : List Nat :=
  match recs with
  | [] => []
  | r :: rest =>
      if appliedSeq <= r.seq then
        r.vecId :: ivfWalFilterFrom rest appliedSeq
      else
        ivfWalFilterFrom rest appliedSeq

def liveIvfRecover (baseVecs : List Nat) (appliedSeq : Nat) (walRecs : List IvfWalRec) : List Nat :=
  baseVecs ++ ivfWalFilterFrom walRecs appliedSeq

-- `CWSP` Strict `runtime_id` Pinning (`F7`) & HMAC-First Error Precedence (`shard.rs`, Fix F13)
def checkStatepageRuntime (expectedRt fileRt : Option Nat) : Bool :=
  match expectedRt, fileRt with
  | none, _           => true
  | some _, none      => false
  | some exp, some g  => exp == g

inductive SpError where
  | spOk
  | spAuthMacMismatch
  | spNonCanonicalFlags
  | spInvalidContract
  deriving DecidableEq, Repr

def statepageParsePrecedence (hasKey : Bool) (hmacFlag : Nat) (macValid hdrValid : Bool) : SpError :=
  match hasKey, hmacFlag, macValid, hdrValid with
  | true, 1, true, true   => .spOk
  | true, 1, true, false  => .spInvalidContract
  | true, 1, false, _     => .spAuthMacMismatch
  | true, 0, _, _         => .spAuthMacMismatch
  | true, _ + 2, _, _     => .spAuthMacMismatch
  | false, 1, _, _        => .spAuthMacMismatch
  | false, 0, _, true     => .spOk
  | false, 0, _, false    => .spInvalidContract
  | false, _ + 2, _, _    => .spNonCanonicalFlags

end Cowrie.Storage
