/-
  Main.lean — Native Streaming Oracle Binary (`cowrie-lean-oracle`) for 3-Way Differential Lockstep
  (Lean 4.34 Verified Reference Kernel <-> Bend 2.0.35 `.mjs` <-> Rust/WASM `cowrie_core.wasm`)

  Protocol (line-oriented stdin -> stdout):
    PROPAGATE <base_u8> [p1 p2 ...]           -> <out_u8> <0|1 tainted>
    HEADER_META <low4> <res3> <epi> <flags>   -> <0|1>
    SANITIZE <sym_str>                        -> <out_sym_str> (L=< G=> P=| E=\ H=hidden O=other)
    R2_COMPACT <auth_u8>                      -> <compact|fence>
    WIRE_INT <kind> <lim> <ulim> <val>        -> <0|1 valid>
    F64_BITS <u64_dec>                        -> <canon_u64_dec> <0|1 valid>
    ZIGZAG <i64_dec>                          -> <u64_dec> <rt_i64_dec>
    MAP_KEYS <nkeys> [k1 k2 ...]              -> <0|1 valid_all_used>
    CWZM <lo> <hi> <mn0:mx0> ...              -> <comma_separated_indices_or_empty>
    IVF_RECOVER <applied_seq> <s0> <base_csv> <wal_csv> -> <csv>
    SP_PRECEDENCE <0|1 has_key> <hmac_flag> <0|1 mac_ok> <0|1 hdr_ok> -> <SpOk|SpAuthMacMismatch|SpNonCanonicalFlags|SpInvalidContract>
-/
import Cowrie

open Cowrie.Trust
open Cowrie.Fence
open Cowrie.Canonical
open Cowrie.Storage

private def parseSymChar (c : Char) : Sym :=
  match c with
  | 'L' => .lt
  | 'G' => .gt
  | 'P' => .pipe
  | 'E' => .esc
  | 'H' => .spoof
  | _   => .other

private def fmtSymChar (s : Sym) : Char :=
  match s with
  | .lt    => 'L'
  | .gt    => 'G'
  | .pipe  => 'P'
  | .esc   => 'E'
  | .spoof => 'H'
  | .other => 'O'

private def parseCsvNats (s : String) : List Nat :=
  if s == "-" || s.isEmpty then []
  else (s.splitOn ",").filterMap String.toNat?

private def fmtCsvNats (xs : List Nat) : String :=
  if xs.isEmpty then "-" else String.intercalate "," (xs.map toString)

private def byteToAuth (b : Nat) : Auth :=
  { cleanBits := b &&& 0x0B
    ext       := (b &&& 0x04) != 0
    tainted   := (b &&& 0x80) != 0 }

private def handleLine (line : String) : String :=
  let parts := (line.trimAscii.toString.splitOn " ").filter (fun s => !s.isEmpty)
  match parts with
  | [] => ""
  | "PROPAGATE" :: baseStr :: rest =>
      let b : UInt8 := UInt8.ofNat (baseStr.toNat?.getD 0)
      let ps : List UInt8 := rest.map (fun s => UInt8.ofNat (s.toNat?.getD 0))
      let out := propagateByte b ps
      let t := if isTaintedByte out then "1" else "0"
      s!"{out.toNat} {t}"
  | ["HEADER_META", l4, r3, ep, fl] =>
      let ok := isValidHeaderMeta (l4.toNat?.getD 0) (r3.toNat?.getD 0)
        (ep.toNat?.getD 0) (fl.toNat?.getD 0)
      if ok then "1" else "0"
  | ["HEADER_META_BYTE", authS, epS, flS] =>
      let ok := isValidHeaderMetaByte (UInt8.ofNat (authS.toNat?.getD 0))
        (UInt8.ofNat (epS.toNat?.getD 0)) (UInt8.ofNat (flS.toNat?.getD 0))
      if ok then "1" else "0"
  | ["MCP_CLASSIFY", polS, verbS, arS, brS, taS, hrS] =>
      let parseAct (s : String) : Option Action :=
        match s with
        | "none" => none
        | "ro"   => some .readOnly
        | "sh"   => some .shellExec
        | "cr"   => some .credRead
        | "fw"   => some .fsWrite
        | "ne"   => some .netExfil
        | _      => none
      let a := classifyMcpTool (parseAct polS) (parseAct verbS)
        (arS == "1") (brS == "1") (taS == "1") (hrS == "1")
      toString (actionToCode a)
  | ["SANITIZE", codeStr] =>
      let syms := if codeStr == "-" then [] else codeStr.toList.map parseSymChar
      let out := sanitizeFences syms
      if out.isEmpty then "-" else String.ofList (out.map fmtSymChar)
  | ["R2_COMPACT", authStr] =>
      let a := byteToAuth (authStr.toNat?.getD 0)
      match r2CompactMode a with
      | .compactHeader  => "compact"
      | .fullMealyFence => "fence"
  | ["WIRE_INT", kind, limS, ulimS, valS] =>
      let lim := limS.toNat?.getD 128
      let ulim := ulimS.toNat?.getD 160
      let v := valS.toNat?.getD 0
      let w : WireInt :=
        match kind with
        | "WFix"       => .wFix v
        | "WTagIntPos" => .wTagIntPos v
        | "WTagIntNeg" => .wTagIntNeg v
        | _            => .wTagUint v
      if verifyCanonIntZeroAlloc lim ulim w then "1" else "0"
  | ["F64_BITS", bitsStr] =>
      let b : UInt64 := UInt64.ofNat (bitsStr.toNat?.getD 0)
      let c := canonicalizeF64Bits b
      let ok := if verifyCanonF64Bits b then "1" else "0"
      s!"{c.toNat} {ok}"
  | ["ZIGZAG", iStr] =>
      let iv : Int := iStr.toInt?.getD 0
      let i64 : Int64 := Int64.ofInt iv
      let enc := zigzagEncode64 i64
      let dec := zigzagDecode64 enc
      s!"{enc.toNat} {dec.toInt}"
  | "MAP_KEYS" :: nkStr :: rest =>
      let nk := nkStr.toNat?.getD 0
      let ks := rest.filterMap String.toNat?
      let ok := verifyMapKeys nk ks && allDictKeysUsed nk ks
      if ok then "1" else "0"
  | "CWZM" :: loStr :: hiStr :: zStrs =>
      let lo := loStr.toInt?.getD 0
      let hi := hiStr.toInt?.getD 0
      let zs : List Zone := zStrs.filterMap fun zs =>
        match zs.splitOn ":" with
        | [a, b] =>
            match a.toInt?, b.toInt? with
            | some mn, some mx => some { minK := mn, maxK := mx }
            | _, _ => none
        | _ => none
      fmtCsvNats (queryKeyRangePages zs lo hi)
  | ["IVF_RECOVER", appStr, s0Str, baseCsv, walCsv] =>
      let app := appStr.toNat?.getD 0
      let s0 := s0Str.toNat?.getD 0
      let base := parseCsvNats baseCsv
      let walIds := parseCsvNats walCsv
      let recs := Cowrie.Laws.ivfMakeSeqRecs walIds s0
      fmtCsvNats (liveIvfRecover base app recs)
  | ["SP_PRECEDENCE", hk, hf, mv, hv] =>
      let e := statepageParsePrecedence (hk == "1") (hf.toNat?.getD 0) (mv == "1") (hv == "1")
      match e with
      | .spOk                => "SpOk"
      | .spAuthMacMismatch   => "SpAuthMacMismatch"
      | .spNonCanonicalFlags => "SpNonCanonicalFlags"
      | .spInvalidContract   => "SpInvalidContract"
  | _ => "ERR_UNKNOWN_CMD"

partial def loop (stdin stdout : IO.FS.Stream) : IO Unit := do
  let line ← stdin.getLine
  if line.isEmpty then
    return ()
  let resp := handleLine line
  if !resp.isEmpty then
    stdout.putStrLn resp
  loop stdin stdout

def main : IO Unit := do
  let stdin ← IO.getStdin
  let stdout ← IO.getStdout
  loop stdin stdout
  stdout.flush
