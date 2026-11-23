"""Cowrie Adapter for vLLM (`CowrieVllmRuntime`) & Universal Agent Memory Store.

Unifies:
  1. Canonical Plane A Agent Memory (`libcowrie_core.so`), Alpha32 R0 triage, and adaptive R2 lenses.
  2. Sticky Causal-DAG Trust Membrane (`0x04 UNTRUSTED_EXT`, `BIT_TAINTED = 0x80`) gating privileged tool calls.
  3. CoreHash-keyed prefix canonicalization for vLLM `enable_prefix_caching=True` + 16 KB StatePage helpers.
"""

import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

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
    """In-process + optional disk-backed Agent Memory & Causal Trust Membrane over `libcowrie_core.so`."""

    def __init__(self, persist_dir: Optional[str] = None):
        self.persist_dir = Path(persist_dir) if persist_dir else None
        if self.persist_dir:
            self.persist_dir.mkdir(parents=True, exist_ok=True)
        self._by_cid: Dict[str, MemoryRecord] = {}
        self._by_handle: Dict[str, MemoryRecord] = {}

    def resolve(self, key: str) -> Optional[MemoryRecord]:
        if key in self._by_handle:
            return self._by_handle[key]
        if key in self._by_cid:
            return self._by_cid[key]
        for cid, rec in self._by_cid.items():
            if len(key) >= 8 and cid.startswith(key):
                return rec
        return None

    def store(
        self,
        payload: Any,
        authority: int = cowrie.AUTH_VERIFIED_TOOL,
        epistemic: int = cowrie.EPI_OBSERVED,
        parents: Sequence[str] = (),
    ) -> MemoryRecord:
        parent_auths = []
        for p in parents:
            rec = self.resolve(p)
            parent_auths.append(rec.authority if rec else (cowrie.AUTH_UNTRUSTED_EXT | cowrie.BIT_TAINTED))
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
            parents=list(parents),
        )
        self._by_cid[cid_hex] = rec
        self._by_handle[handle] = rec
        if self.persist_dir:
            (self.persist_dir / f"{cid_hex[:16]}.cwfr").write_bytes(wire)
        return rec

    def triage_r0(self) -> str:
        return "\n".join(rec.r0 for rec in self._by_cid.values())

    def project(self, key: str, tier: str = "r2") -> str:
        rec = self.resolve(key)
        if not rec:
            raise KeyError(f"Unknown Cowrie handle/CID: {key}")
        return cowrie.project_lens(rec.frame_bytes, tier, 0)

    def check_tool_gate(
        self,
        action: str,
        arg_ids: Sequence[str],
        owner_override: bool = False,
    ) -> Dict[str, Any]:
        privileged = action in PRIVILEGED_ACTIONS
        tainted_src = None
        for aid in arg_ids:
            rec = self.resolve(aid)
            if rec is None or rec.tainted:
                tainted_src = aid
                break
        allowed = (not privileged) or owner_override or (tainted_src is None)
        res = {
            "allowed": allowed,
            "privileged": privileged,
            "tainted": tainted_src is not None,
            "action": action,
        }
        if not allowed:
            res["error"] = "ERR_TAINTED_EXECUTION_BLOCKED"
            res["tainted_source"] = tainted_src
        return res


class CowrieVllmRuntime:
    """Wraps a live `vllm.LLM` engine with Cowrie R0/R2 attention lenses, CoreHash prefix
    canonicalization (`enable_prefix_caching=True`), and `BIT_TAINTED` causal trust gating."""

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
    ) -> Dict[str, Any]:
        gate = self.memory.check_tool_gate(action, memory_handles, owner_override=owner_override)
        if not gate["allowed"]:
            return {"blocked": True, "gate": gate, "text": "", "prompt_tokens": 0, "latency_ms": 0.0}

        lenses = [self.memory.project(h, tier=tier) for h in memory_handles]
        prompt = "\n".join(lenses) + "\n\nQuestion: " + question + "\nAnswer:"
        t0 = time.perf_counter()
        outs = self.llm.generate([prompt], sampling_params, use_tqdm=False)
        dt_ms = (time.perf_counter() - t0) * 1000.0
        o0 = outs[0]
        return {
            "blocked": False,
            "gate": gate,
            "tainted": gate["tainted"],
            "prompt_tokens": len(o0.prompt_token_ids),
            "gen_tokens": len(o0.outputs[0].token_ids),
            "latency_ms": round(dt_ms, 2),
            "text": o0.outputs[0].text,
        }
