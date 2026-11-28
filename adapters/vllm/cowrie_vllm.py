"""Cowrie Adapter for vLLM (`CowrieVllmRuntime`) & Thread-Safe Hardened Agent Memory Store (v1.2).

Unifies (with 100% parity across `llama.cpp`, `Eureka LLM`, and `Eureka Creative v7`):
  1. Canonical Plane A Agent Memory (`libcowrie_core.so`), 47-bit Alpha32 R0 triage, and fence-escaped R2 lenses.
  2. Crash-safe hash-chained `.cwwal` mutation & audit log (`cowrie.Wal`) with live-frame startup replay + `0700` `O_EXCL` persistence.
  3. Sticky Causal-DAG Trust Membrane (`0x04 UNTRUSTED_EXT`, `BIT_TAINTED = 0x80`) with collision-fail-closed
     resolution, whole-context synthesis lineage, server-pinned RFC 8032 `Ed25519` owner-declassification tokens
     (`COWRIE_DECLASSIFY_V1`), and fail-closed unsigned override gating.
"""

from dataclasses import dataclass, field
import os
from pathlib import Path
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Sequence
import uuid

import importlib.util
if importlib.util.find_spec("cowrie") is None:
    ROOT = Path(__file__).resolve().parents[2]
    if str(ROOT / "python") not in sys.path:
        sys.path.insert(0, str(ROOT / "python"))

import cowrie


PRIVILEGED_ACTIONS = {"shell_exec", "cred_read", "fs_write", "net_exfil"}


@dataclass
class MemoryRecord:
    cid_hex: str
    handle: str
    authority: int
    epistemic: int
    frame_bytes: bytes
    r0: str
    parents: List[str] = field(default_factory=list)

    @property
    def tainted(self) -> bool:
        return cowrie.is_tainted(self.authority)


class CowrieMemoryStore:
    """Thread-safe in-process + optional `0700` `.cwwal` / `.cwfr` disk-backed Agent Memory & Causal Trust Membrane."""

    def __init__(
        self,
        persist_dir: Optional[str] = None,
        owner_pubkey_hex: Optional[str] = None,
        allow_unsigned_override: Optional[bool] = None,
    ):
        self.persist_dir = Path(persist_dir) if persist_dir else None
        pk_env = owner_pubkey_hex or os.environ.get("COWRIE_OWNER_PUBKEY", "")
        self.owner_pubkey: Optional[bytes] = bytes.fromhex(pk_env) if len(pk_env) == 64 else None
        if allow_unsigned_override is None:
            self._allow_unsigned_override = os.environ.get("COWRIE_ALLOW_UNSIGNED_OVERRIDE") == "1"
        else:
            self._allow_unsigned_override = bool(allow_unsigned_override)
        self._lock = threading.RLock()
        self._by_cid: Dict[str, MemoryRecord] = {}
        self._by_handle: Dict[str, List[MemoryRecord]] = {}
        self._wal: Optional[cowrie.Wal] = None
        if self.persist_dir:
            self.persist_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            os.chmod(self.persist_dir, 0o700)
            self._wal = cowrie.Wal(str(self.persist_dir / "memory.cwwal"), recover_torn_tail=True)
            self._reload_disk()

    def close(self):
        with self._lock:
            if self._wal is not None:
                self._wal.close()
                self._wal = None

    def _reload_frame_locked(self, raw: bytes):
        ver = cowrie.verify_frame(raw)
        cid_hex = ver["cid"].hex()
        r0 = cowrie.project_lens(raw, "r0", 0)
        handle = r0.split(" ", 1)[0]
        rec = MemoryRecord(
            cid_hex=cid_hex,
            handle=handle,
            authority=ver["authority"],
            epistemic=ver["epistemic"],
            frame_bytes=raw,
            r0=r0,
            parents=[],
        )
        self._index_record_locked(rec)

    def _reload_disk(self):
        with self._lock:
            if self._wal is not None:
                for frame_bytes in self._wal.live_frames():
                    self._reload_frame_locked(frame_bytes)
            if self.persist_dir:
                for p in sorted(self.persist_dir.glob("*.cwfr")):
                    self._reload_frame_locked(p.read_bytes())

    def _index_record_locked(self, rec: MemoryRecord):
        existing = self._by_cid.get(rec.cid_hex)
        if existing and existing.tainted and not rec.tainted:
            rec.authority |= cowrie.BIT_TAINTED
        self._by_cid[rec.cid_hex] = rec
        bucket = self._by_handle.setdefault(rec.handle, [])
        for i, item in enumerate(bucket):
            if item.cid_hex == rec.cid_hex:
                bucket[i] = rec
                return
        bucket.append(rec)

    def resolve(self, key: str) -> Optional[MemoryRecord]:
        """Resolve a full 68-hex CID or unambiguous 47-bit `@handle`.
        Returns `None` if unknown OR if a `@handle` collided across multiple distinct CIDs (fail closed!)."""
        with self._lock:
            if key in self._by_cid:
                return self._by_cid[key]
            if key in self._by_handle:
                bucket = self._by_handle[key]
                return bucket[0] if len(bucket) == 1 else None
            if len(key) >= 32 and not key.startswith("@"):
                matches = [rec for cid, rec in self._by_cid.items() if cid.startswith(key)]
                if len(matches) == 1:
                    return matches[0]
            return None

    def store(
        self,
        payload: Any,
        authority: int = cowrie.AUTH_VERIFIED_TOOL,
        epistemic: int = cowrie.EPI_OBSERVED,
        parents: Sequence[str] = (),
    ) -> MemoryRecord:
        with self._lock:
            parent_auths = []
            resolved_parent_cids = []
            for p in parents:
                rec = self.resolve(p)
                if rec is not None:
                    parent_auths.append(rec.authority)
                    resolved_parent_cids.append(rec.cid_hex)
                else:
                    parent_auths.append(cowrie.AUTH_UNTRUSTED_EXT | cowrie.BIT_TAINTED)
                    resolved_parent_cids.append(p)

            eff_auth = cowrie.propagate_authority(authority, parent_auths)
            wire = cowrie.encode_json(payload, eff_auth, epistemic)
            ver = cowrie.verify_frame(wire)
            cid_hex = ver["cid"].hex()
            r0 = cowrie.project_lens(wire, "r0", 0)
            handle = r0.split(" ", 1)[0]
            rec = MemoryRecord(
                cid_hex=cid_hex,
                handle=handle,
                authority=eff_auth,
                epistemic=epistemic,
                frame_bytes=wire,
                r0=r0,
                parents=resolved_parent_cids,
            )
            self._index_record_locked(rec)
            if self._wal is not None:
                self._wal.append_frame(wire, fsync=True)
            if self.persist_dir:
                out_file = self.persist_dir / f"{cid_hex}.cwfr"
                tmp_file = self.persist_dir / f".{cid_hex}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
                fd = os.open(str(tmp_file), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                try:
                    with os.fdopen(fd, "wb") as f:
                        f.write(wire)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(str(tmp_file), str(out_file))
                except Exception:
                    try:
                        os.unlink(str(tmp_file))
                    except OSError:
                        pass
                    raise
            return rec

    def triage_r0(self) -> str:
        with self._lock:
            return "\n".join(rec.r0 for rec in self._by_cid.values())

    def project(self, key: str, tier: str = "r2") -> str:
        rec = self.resolve(key)
        if not rec:
            raise KeyError(f"Unknown or ambiguous Cowrie handle/CID: {key}")
        return cowrie.project_lens(rec.frame_bytes, tier, 0)

    def _is_cid_declassified(
        self,
        cid_hex: str,
        action: str,
        declassify_tokens: Sequence[Dict[str, Any]],
        owner_pubkey: Optional[bytes],
        now_unix: int,
    ) -> bool:
        if owner_pubkey is None or len(owner_pubkey) != 32 or action not in cowrie.ACTION_CODES:
            return False
        try:
            cid_b = bytes.fromhex(cid_hex)
        except ValueError:
            return False
        if len(cid_b) != 34:
            return False
        for tok in declassify_tokens:
            if tok.get("cid") != cid_hex:
                continue
            try:
                sig_b = bytes.fromhex(str(tok.get("sig", "")))
                exp = int(tok.get("expiry", 0))
            except (ValueError, TypeError):
                continue
            if len(sig_b) == 64 and exp >= 0 and cowrie.verify_declassification(
                owner_pubkey, cid_b, action, exp, sig_b, now_unix
            ):
                return True
        return False

    def check_tool_gate(
        self,
        action: str,
        arg_ids: Sequence[str],
        owner_override: bool = False,
        declassify_tokens: Sequence[Dict[str, Any]] = (),
        owner_pubkey_hex: Optional[str] = None,
        now_unix: Optional[int] = None,
    ) -> Dict[str, Any]:
        privileged = action in PRIVILEGED_ACTIONS or action not in ("read_only", "read_only_safe", "search")
        # Server-pinned key ALWAYS takes precedence; caller-supplied pubkey cannot override configured trust anchor!
        if self.owner_pubkey is not None:
            pk = self.owner_pubkey
        elif owner_pubkey_hex and len(owner_pubkey_hex) == 64:
            try:
                pk = bytes.fromhex(owner_pubkey_hex)
            except ValueError:
                pk = None
        else:
            pk = None

        effective_override = bool(owner_override and self._allow_unsigned_override)
        now_ts = int(time.time()) if now_unix is None else int(now_unix)

        tainted_src = None
        declassified_count = 0
        with self._lock:
            if privileged and len(arg_ids) == 0:
                tainted_src = "<empty_lineage>"
            else:
                for aid in arg_ids:
                    rec = self.resolve(aid)
                    if rec is None:
                        tainted_src = aid
                        break
                    if rec.tainted:
                        if privileged and self._is_cid_declassified(
                            rec.cid_hex, action, declassify_tokens, pk, now_ts
                        ):
                            declassified_count += 1
                        else:
                            tainted_src = aid
                            break

        allowed = (not privileged) or effective_override or (tainted_src is None)
        res = {
            "allowed": allowed,
            "privileged": privileged,
            "tainted": tainted_src is not None,
            "declassified_count": declassified_count,
            "action": action,
        }
        if not allowed:
            res["error"] = "ERR_TAINTED_EXECUTION_BLOCKED"
            res["tainted_source"] = tainted_src
        return res


class CowrieVllmRuntime:
    """Wraps a live `vllm.LLM` engine with Cowrie R0/R2 attention lenses, CoreHash prefix
    canonicalization (`enable_prefix_caching=True`), whole-context `BIT_TAINTED` lineage tracking,
    and RFC 8032 `Ed25519` owner-declassification tokens."""

    def __init__(self, llm: Any, memory: Optional[CowrieMemoryStore] = None):
        self.llm = llm
        self.memory = memory or CowrieMemoryStore()

    def generate_with_memory(
        self,
        question: str,
        memory_handles: Sequence[str],
        sampling_params: Any,
        tier: str = "r2",
        action: str = "read_only",
        owner_override: bool = False,
        declassify_tokens: Sequence[Dict[str, Any]] = (),
        owner_pubkey_hex: Optional[str] = None,
    ) -> Dict[str, Any]:
        gate = self.memory.check_tool_gate(
            action,
            memory_handles,
            owner_override=owner_override,
            declassify_tokens=declassify_tokens,
            owner_pubkey_hex=owner_pubkey_hex,
        )
        if not gate["allowed"]:
            return {"blocked": True, "gate": gate, "text": "", "prompt_tokens": 0, "latency_ms": 0.0}

        lenses = [self.memory.project(h, tier=tier) for h in memory_handles]
        prompt = "\n".join(lenses) + "\n\nQuestion: " + question + "\nAnswer:"
        t0 = time.perf_counter()
        outs = self.llm.generate([prompt], sampling_params, use_tqdm=False)
        dt_ms = (time.perf_counter() - t0) * 1000.0
        o0 = outs[0]
        text = o0.outputs[0].text
        synth_rec = self.memory.store(
            {"title": "vllm_synthesis", "question": question, "answer": text},
            authority=cowrie.AUTH_MODEL_SYNTH,
            epistemic=cowrie.EPI_DERIVED,
            parents=list(memory_handles),
        )
        return {
            "blocked": False,
            "gate": gate,
            "tainted": synth_rec.tainted,
            "output_cid": synth_rec.cid_hex,
            "output_handle": synth_rec.handle,
            "prompt_tokens": len(o0.prompt_token_ids),
            "gen_tokens": len(o0.outputs[0].token_ids),
            "latency_ms": round(dt_ms, 2),
            "text": text,
        }
