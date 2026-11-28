/-
  Cowrie/Fence.lean — Plane B 6-Symbol Mealy Transducer, Multi-Cell R2 (`F9`),
  R2-Compact/VirtualAtlas Trust Tiering, & `extract_field` (Lean 4.34, Init-only)
-/
import Cowrie.Trust

namespace Cowrie.Fence

open Cowrie.Trust

inductive Sym where
  | lt
  | gt
  | pipe
  | esc
  | spoof
  | other
  deriving DecidableEq, Repr

inductive Run2 where
  | r0 | r1 | r2
  deriving DecidableEq, Repr

inductive PrevSym where
  | pLt | pPipe | pOther
  deriving DecidableEq, Repr

/-- Full 6-symbol Mealy transducer matching `lens.rs::sanitize_r2_fences` (`F1` + `F9`):
    strips `spoof` (zero-width/Bidi/C0/C1/Tags) BEFORE run/prev transitions;
    escapes `<<<`, `>>>`, `<|`, and `|>`. -/
def sanitizeStep (xs : List Sym) (rLt rGt : Run2) (prev : PrevSym) : List Sym :=
  match xs with
  | [] => []
  | .spoof :: rest => sanitizeStep rest rLt rGt prev
  | .lt :: rest =>
      match rLt with
      | .r0 => .lt :: sanitizeStep rest .r1 .r0 .pLt
      | .r1 => .lt :: sanitizeStep rest .r2 .r0 .pLt
      | .r2 => .esc :: .lt :: sanitizeStep rest .r1 .r0 .pLt
  | .gt :: rest =>
      match prev with
      | .pPipe => .esc :: .gt :: sanitizeStep rest .r0 .r1 .pOther
      | _ =>
          match rGt with
          | .r0 => .gt :: sanitizeStep rest .r0 .r1 .pOther
          | .r1 => .gt :: sanitizeStep rest .r0 .r2 .pOther
          | .r2 => .esc :: .gt :: sanitizeStep rest .r0 .r1 .pOther
  | .pipe :: rest =>
      match prev with
      | .pLt => .esc :: .pipe :: sanitizeStep rest .r0 .r0 .pPipe
      | _    => .pipe :: sanitizeStep rest .r0 .r0 .pPipe
  | .esc :: rest =>
      .esc :: sanitizeStep rest .r0 .r0 .pOther
  | .other :: rest =>
      .other :: sanitizeStep rest .r0 .r0 .pOther

def sanitizeFences (xs : List Sym) : List Sym :=
  sanitizeStep xs .r0 .r0 .pOther

def stripSpoof (xs : List Sym) : List Sym :=
  xs.filter (fun s => s != .spoof)

def stripEsc (xs : List Sym) : List Sym :=
  xs.filter (fun s => s != .esc)

inductive R2Field where
  | scalar (key val : List Sym)
  | table (key colHdr cell1 cell2 : List Sym)
  | tensorRef (key space : List Sym)
  deriving DecidableEq, Repr

/-- Structured R2 renderer covering scalars, hoisted multi-cell `R2Table` joined by `|` (`F4`+`F9`),
    and `<T:...@space>` `R2TensorRef` (`F8`). -/
def renderR2Field (f : R2Field) : List Sym :=
  match f with
  | .scalar k v =>
      sanitizeFences k ++ (.other :: sanitizeFences v)
  | .table k col c1 c2 =>
      let row := sanitizeFences (sanitizeFences c1 ++ (.pipe :: sanitizeFences c2))
      sanitizeFences k ++ (.other :: sanitizeFences col ++ (.other :: row))
  | .tensorRef k sp =>
      sanitizeFences k ++ (.other :: sanitizeFences (.lt :: .other :: sanitizeFences sp ++ [.gt]))

inductive LensMode where
  | compactHeader
  | fullMealyFence
  deriving DecidableEq, Repr

/-- `R2-Compact` & `project_virtual_atlas` fail-closed trust gate (`lens.rs:395-407, 688`). -/
def r2CompactMode (a : Auth) : LensMode :=
  if !a.ext && !a.tainted && (a.cleanBits == 1 || a.cleanBits == 2) then
    .compactHeader
  else
    .fullMealyFence

def virtualAtlasMode (a : Auth) : LensMode :=
  r2CompactMode a

inductive TableCell where
  | cellNull
  | cellMissing
  | cellStr (raw : List Sym)
  deriving DecidableEq, Repr

inductive CellTag where
  | outNull
  | outMissing
  | outEmptyQuoted
  | outQuotedStr (s : List Sym)
  | outPlainStr (s : List Sym)
  deriving DecidableEq, Repr

def isNullOrMissingStr (xs : List Sym) : Bool :=
  match xs with
  | [.lt, .other, .gt] => true
  | [.other]           => true
  | _                  => false

/-- `lens.rs::cell_escape` with `F9` fix: strips `spoof` BEFORE empty/marker checks. -/
def encodeTableCell (c : TableCell) : CellTag :=
  match c with
  | .cellNull    => .outNull
  | .cellMissing => .outMissing
  | .cellStr raw =>
      let eff := stripSpoof raw
      match eff with
      | [] => .outEmptyQuoted
      | _  =>
          if isNullOrMissingStr eff then
            .outQuotedStr (sanitizeFences eff)
          else
            .outPlainStr (sanitizeFences eff)

def isValidLensTier (tier : Nat) : Bool :=
  decide (tier <= 3)

inductive PathSeg where
  | pKey (k : Nat)
  | pIdx (i : Nat)
  deriving DecidableEq, Repr

inductive ValTree where
  | vAtom (n : Nat)
  | vArr (items : List ValTree)
  | vMap (entries : List (Nat × ValTree))
  deriving Repr

def mapFindVal (entries : List (Nat × ValTree)) (key : Nat) : Option ValTree :=
  match entries with
  | [] => none
  | (ek, v) :: rest => if ek == key then some v else mapFindVal rest key

def listNthVal (items : List ValTree) (idx : Nat) : Option ValTree :=
  match items, idx with
  | [], _ => none
  | h :: _, 0 => some h
  | _ :: t, i + 1 => listNthVal t i

def extractStep (cur : ValTree) (seg : PathSeg) : Option ValTree :=
  match cur, seg with
  | .vMap es, .pKey k => mapFindVal es k
  | .vArr xs, .pIdx i => listNthVal xs i
  | _, _              => none

def extractFieldOpt (path : List PathSeg) (cur : Option ValTree) : Option ValTree :=
  match path, cur with
  | [], c => c
  | _, none => none
  | seg :: rest, some v => extractFieldOpt rest (extractStep v seg)

def extractField (cur : ValTree) (path : List PathSeg) : Option ValTree :=
  extractFieldOpt path (some cur)

end Cowrie.Fence
