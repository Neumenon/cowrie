"""Shared zero-dependency Cowrie tool virtualization, R2-Compact lens, and causal taint guard.

Backs the LangGraph, PydanticAI, OpenAI Agents SDK, and LanceDB connectors with exact
parity to `cowrie mcp-proxy --virtualize`.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import functools
import inspect
import json
import os
import re
import threading
import time
from typing import Any, Callable, Iterable, Optional, Sequence

import cowrie

DEFAULT_VIRTUALIZE_BYTES = 16_384
MAX_VIRTUAL_ENTRIES = 512

LOCAL_READONLY_TOOLS = frozenset(
    {
        "read_file", "get_file", "list_dir", "list_directory", "list_files", "ls",
        "stat", "grep", "rg", "search_files", "find_files", "inspect", "status",
        "get_status", "ping", "echo", "add", "get_time", "health", "help",
        "describe", "version", "list_tables", "query_readonly",
    }
)

_RESERVED_SENTINELS = frozenset({"$bytes", "$tensor", "$float", "$map"})

_RE_SHELL = re.compile(r"(shell|bash|exec|command|eval|spawn|terminal)")
_RE_CRED = re.compile(r"(cred|secret|token|passwd|apikey|api_key)")
_RE_WRITE = re.compile(r"(write|delete|remove|edit|patch|chmod|create_file|mkdir)")
_RE_NET = re.compile(r"(post|send|upload|exfil|webhook|http|fetch|url|curl|web|browser|scrape|email|mail|slack)")
_RE_UNTRUSTED = re.compile(
    r"(web|fetch|http|url|browser|scrape|search|email|mail|slack|issue|external|curl|github|jira|rss)"
)


class CowrieTaintBlockedError(PermissionError):
    """Raised when a tainted agent context attempts an unsigned privileged tool call (`ERR_TAINTED_EXECUTION_BLOCKED`)."""

    def __init__(self, tool_name: str, action: str, tainted_cids: list[str]):
        self.tool_name = tool_name
        self.action = action
        self.tainted_cids = list(tainted_cids)
        super().__init__(
            f"ERR_TAINTED_EXECUTION_BLOCKED: tool={tool_name!r} action={action!r} "
            f"tainted_cids={self.tainted_cids[:3]}"
        )


@dataclass(frozen=True)
class TaintBlockContext:
    """Context passed to `on_taint_block` HITL approval callbacks before a tainted privileged call."""

    tool_name: str
    action: str
    tainted_cids: list[str]
    now_unix: int


def make_ed25519_hitl_callback(
    seed_32: bytes,
    approver_fn: Callable[[TaintBlockContext], Any],
    *,
    ttl_secs: int = 300,
) -> Callable[[TaintBlockContext], Any]:
    """Build an `on_taint_block` callback that asks `approver_fn(ctx)` and signs scoped RFC 8032 Ed25519 tokens.

    Supports both sync and `async def` `approver_fn` returning bool (or `{"approved": True}`).
    Every returned token is still cryptographically verified by `CowrieToolGuard` against `owner_pubkey`.
    """
    if not isinstance(seed_32, (bytes, bytearray)) or len(seed_32) != 32:
        raise ValueError("seed_32 must be exactly 32 bytes")
    seed_bytes = bytes(seed_32)

    def _mint(ctx: TaintBlockContext, decision: Any) -> Optional[list[dict[str, Any]]]:
        approved = decision.get("approved") is True if isinstance(decision, dict) else bool(decision)
        if not approved:
            return None
        expiry = ctx.now_unix + max(1, int(ttl_secs))
        tokens: list[dict[str, Any]] = []
        for cid_hex in ctx.tainted_cids:
            sig = cowrie.sign_declassification(seed_bytes, bytes.fromhex(cid_hex), ctx.action, expiry)
            tokens.append({"cid": cid_hex, "expiry": expiry, "sig": sig.hex()})
        return tokens

    if inspect.iscoroutinefunction(approver_fn):
        async def _async_cb(ctx: TaintBlockContext) -> Optional[list[dict[str, Any]]]:
            decision = await approver_fn(ctx)
            return _mint(ctx, decision)

        return _async_cb

    def _sync_cb(ctx: TaintBlockContext) -> Optional[list[dict[str, Any]]]:
        decision = approver_fn(ctx)
        return _mint(ctx, decision)

    return _sync_cb


@dataclass(frozen=True)
class SealedToolOutput:
    """Canonical Cowrie-sealed tool result with deterministic handle, CID, and bounded prompt rendering."""

    tool_name: str
    frame: bytes
    cid_hex: str
    handle: str
    authority: int
    tainted: bool
    virtualized: bool
    raw_bytes: int
    rendered_text: str


@dataclass
class _VirtualEntry:
    cid: bytes
    cid_hex: str
    frame: bytes
    authority: int


class CowrieToolGuard:
    """Thread-safe tool output virtualizer, R2-Compact projector, and causal 0x80 / Ed25519 gate."""

    def __init__(
        self,
        *,
        virtualize_bytes: int = DEFAULT_VIRTUALIZE_BYTES,
        compact_r2: bool = True,
        mode: str = "enforce",
        on_taint_block: Optional[Callable[[TaintBlockContext], Any]] = None,
        owner_pubkey_hex: Optional[str] = None,
        audit_wal_path: Optional[str] = None,
        allow_readonly: Iterable[str] = (),
        untrusted_sources: Iterable[str] = (),
        privileged_overrides: Optional[dict[str, str]] = None,
        max_virtual_entries: int = MAX_VIRTUAL_ENTRIES,
        atlas_max_chars: int = 1800,
    ):
        if mode not in ("enforce", "observe"):
            raise ValueError(f"mode must be 'enforce' or 'observe', got {mode!r}")
        self.virtualize_bytes = max(0, int(virtualize_bytes))
        self.compact_r2 = bool(compact_r2)
        self.mode = mode
        self.on_taint_block = on_taint_block
        self.atlas_max_chars = int(atlas_max_chars)
        self.max_virtual_entries = max(1, int(max_virtual_entries))

        pk_hex = owner_pubkey_hex or os.environ.get("COWRIE_OWNER_PUBKEY", "")
        self.owner_pubkey: Optional[bytes] = bytes.fromhex(pk_hex) if len(pk_hex) == 64 else None

        self.allow_readonly: set[str] = set(allow_readonly)
        self.untrusted_sources: set[str] = set(untrusted_sources)
        self.privileged_overrides: dict[str, str] = dict(privileged_overrides or {})

        self._lock = threading.RLock()
        self._tainted_cids: dict[str, bytes] = {}
        self._virtual_store: dict[str, _VirtualEntry] = {}
        self._virtual_order: deque[str] = deque()
        self._wal: Optional[cowrie.Wal] = (
            cowrie.Wal(audit_wal_path, recover_torn_tail=True) if audit_wal_path else None
        )
        self._stats = {
            "calls": 0,
            "virtualized": 0,
            "blocked": 0,
            "would_block": 0,
            "hitl_approved": 0,
            "bytes_in": 0,
            "bytes_out": 0,
        }

    def close(self) -> None:
        with self._lock:
            if self._wal is not None:
                self._wal.close()
                self._wal = None

    def __enter__(self) -> "CowrieToolGuard":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    @property
    def is_tainted(self) -> bool:
        with self._lock:
            return bool(self._tainted_cids)

    @property
    def tainted_cids(self) -> list[str]:
        with self._lock:
            return list(self._tainted_cids.keys())

    @property
    def stats(self) -> dict[str, int]:
        with self._lock:
            return dict(self._stats)

    def reset_taint(self) -> None:
        """Clear session taint state (e.g., when starting an independent user turn/thread)."""
        with self._lock:
            self._tainted_cids.clear()

    def classify_tool(self, tool_name: str) -> str:
        if tool_name in self.privileged_overrides:
            return self.privileged_overrides[tool_name]
        lower = tool_name.lower()
        if _RE_SHELL.search(lower):
            return "shell_exec"
        if _RE_CRED.search(lower):
            return "cred_read"
        if _RE_WRITE.search(lower):
            return "fs_write"
        if _RE_NET.search(lower):
            return "net_exfil"
        if tool_name in self.allow_readonly or lower in LOCAL_READONLY_TOOLS:
            return "read_only"
        # Default-deny: unknown tools require explicit allow_readonly, clean lineage, or Ed25519 token.
        return "shell_exec"

    def is_untrusted_source(self, tool_name: str) -> bool:
        if tool_name in self.untrusted_sources:
            return True
        return bool(_RE_UNTRUSTED.search(tool_name.lower()))

    def _verify_all_tokens(
        self,
        tainted_items: list[tuple[str, bytes]],
        resolved_action: str,
        tokens: Sequence[dict[str, Any]],
        now: int,
    ) -> bool:
        if self.owner_pubkey is None:
            return False
        for cid_hex, cid_bytes in tainted_items:
            matched = False
            for tok in tokens:
                if not isinstance(tok, dict):
                    continue
                if str(tok.get("cid", "")).lower() != cid_hex:
                    continue
                sig_hex = str(tok.get("sig", ""))
                expiry = tok.get("expiry")
                if len(sig_hex) != 128 or not isinstance(expiry, int) or isinstance(expiry, bool):
                    continue
                try:
                    if cowrie.verify_declassification(
                        self.owner_pubkey,
                        cid_bytes,
                        resolved_action,
                        expiry,
                        bytes.fromhex(sig_hex),
                        now,
                    ):
                        matched = True
                        break
                except (ValueError, cowrie.CowrieError):
                    continue
            if not matched:
                return False
        return True

    def check_before_call(
        self,
        tool_name: str,
        *,
        action: Optional[str] = None,
        declassify_tokens: Optional[Sequence[dict[str, Any]]] = None,
        now_unix: Optional[int] = None,
    ) -> str:
        """Verify causal taint policy before invoking `tool_name`.

        In `mode="observe"`, logs/counts `would_block` and allows execution.
        In `mode="enforce"`, invokes `on_taint_block(TaintBlockContext)` if configured (cryptographically
        verifying any returned RFC 8032 Ed25519 tokens), otherwise raises `CowrieTaintBlockedError`.
        """
        resolved_action = action or self.classify_tool(tool_name)
        if resolved_action == "read_only":
            return resolved_action

        with self._lock:
            tainted_items = list(self._tainted_cids.items())
        if not tainted_items:
            return resolved_action

        now = int(time.time()) if now_unix is None else int(now_unix)
        if self._verify_all_tokens(tainted_items, resolved_action, list(declassify_tokens or []), now):
            return resolved_action

        if self.mode == "observe":
            with self._lock:
                self._stats["would_block"] += 1
            return resolved_action

        if self.on_taint_block is not None:
            ctx = TaintBlockContext(
                tool_name=tool_name,
                action=resolved_action,
                tainted_cids=[c for c, _ in tainted_items],
                now_unix=now,
            )
            cb_res = self.on_taint_block(ctx)
            if inspect.isawaitable(cb_res):
                raise RuntimeError(
                    "Async `on_taint_block` callback used inside sync tool call; use `async def` tool or sync callback"
                )
            if isinstance(cb_res, (list, tuple)) and self._verify_all_tokens(
                tainted_items, resolved_action, cb_res, now
            ):
                with self._lock:
                    self._stats["hitl_approved"] += 1
                return resolved_action

        with self._lock:
            self._stats["blocked"] += 1
        raise CowrieTaintBlockedError(tool_name, resolved_action, [c for c, _ in tainted_items])

    async def acheck_before_call(
        self,
        tool_name: str,
        *,
        action: Optional[str] = None,
        declassify_tokens: Optional[Sequence[dict[str, Any]]] = None,
        now_unix: Optional[int] = None,
    ) -> str:
        """Async counterpart to `check_before_call`, supporting `async def` `on_taint_block` HITL callbacks."""
        resolved_action = action or self.classify_tool(tool_name)
        if resolved_action == "read_only":
            return resolved_action

        with self._lock:
            tainted_items = list(self._tainted_cids.items())
        if not tainted_items:
            return resolved_action

        now = int(time.time()) if now_unix is None else int(now_unix)
        if self._verify_all_tokens(tainted_items, resolved_action, list(declassify_tokens or []), now):
            return resolved_action

        if self.mode == "observe":
            with self._lock:
                self._stats["would_block"] += 1
            return resolved_action

        if self.on_taint_block is not None:
            ctx = TaintBlockContext(
                tool_name=tool_name,
                action=resolved_action,
                tainted_cids=[c for c, _ in tainted_items],
                now_unix=now,
            )
            cb_res = self.on_taint_block(ctx)
            if inspect.isawaitable(cb_res):
                cb_res = await cb_res
            if isinstance(cb_res, (list, tuple)) and self._verify_all_tokens(
                tainted_items, resolved_action, cb_res, now
            ):
                with self._lock:
                    self._stats["hitl_approved"] += 1
                return resolved_action

        with self._lock:
            self._stats["blocked"] += 1
        raise CowrieTaintBlockedError(tool_name, resolved_action, [c for c, _ in tainted_items])

    def _to_cowrie_safe(self, obj: Any, depth: int = 0) -> Any:
        """Convert arbitrary Python/JSON data into strict RFC 8259 Cowrie-safe values.

        Handles `NaN`/`Infinity`/`-0.0`, escapes hostile reserved `$`-sentinel keys via `$map`,
        and bounds recursion depth safely below Cowrie's `MAX_DEPTH = 64` DoS limit.
        """
        import math

        if depth > 56:
            return obj if isinstance(obj, str) else json.dumps(obj, default=str)
        if obj is None or isinstance(obj, (bool, int, str)):
            return obj
        if isinstance(obj, float):
            if math.isnan(obj):
                return {"$float": "NaN"}
            if math.isinf(obj):
                return {"$float": "Infinity" if obj > 0 else "-Infinity"}
            return 0.0 if obj == 0.0 else obj
        if isinstance(obj, (bytes, bytearray)):
            return {"$bytes": bytes(obj).hex()}
        if isinstance(obj, (list, tuple)):
            return [self._to_cowrie_safe(x, depth + 1) for x in obj]
        if isinstance(obj, dict):
            out = {str(k): self._to_cowrie_safe(v, depth + 1) for k, v in obj.items()}
            if any(k in _RESERVED_SENTINELS for k in out):
                return {"$map": out}
            return out
        return str(obj)

    def _normalize_payload(self, output: Any) -> tuple[Any, int, bool]:
        if isinstance(output, str):
            raw_bytes = len(output.encode("utf-8"))
            stripped = output.strip()
            if (stripped.startswith("{") and stripped.endswith("}")) or (
                stripped.startswith("[") and stripped.endswith("]")
            ):
                try:
                    parsed = json.loads(stripped)
                    return self._to_cowrie_safe(parsed), raw_bytes, True
                except (json.JSONDecodeError, RecursionError):
                    pass
            return {"text": output}, raw_bytes, False
        if isinstance(output, (dict, list)):
            safe = self._to_cowrie_safe(output)
            raw_s = json.dumps(safe, separators=(",", ":"), allow_nan=False)
            return safe, len(raw_s.encode("utf-8")), True
        if isinstance(output, (bytes, bytearray)):
            s = bytes(output).decode("utf-8", errors="replace")
            return {"text": s}, len(output), False
        safe = self._to_cowrie_safe({"value": output})
        raw_s = json.dumps(safe, separators=(",", ":"), allow_nan=False)
        return safe, len(raw_s.encode("utf-8")), True

    def seal_output(
        self,
        tool_name: str,
        output: Any,
        *,
        authority: Optional[int] = None,
        untrusted: Optional[bool] = None,
    ) -> SealedToolOutput:
        """Seal a tool output into a canonical COWR frame, update taint/WAL, and project compact/virtual text."""
        if authority is None:
            is_ext = self.is_untrusted_source(tool_name) if untrusted is None else bool(untrusted)
            auth = cowrie.AUTH_UNTRUSTED_EXT if is_ext else cowrie.AUTH_VERIFIED_TOOL
        else:
            auth = int(authority)

        payload, raw_bytes, was_structured = self._normalize_payload(output)
        try:
            frame = cowrie.encode_json(payload, authority=auth, epistemic=cowrie.EPI_OBSERVED)
        except cowrie.CowrieError:
            # Fail-closed fallback (e.g. extreme depth/key-size bomb): seal raw text envelope with same authority!
            fallback_text = output if isinstance(output, str) else json.dumps(payload, default=str)
            frame = cowrie.encode_json({"text": fallback_text}, authority=auth, epistemic=cowrie.EPI_OBSERVED)
            was_structured = False
        ver = cowrie.verify_frame(frame)
        cid_bytes: bytes = ver["cid"]
        cid_hex = cid_bytes.hex()
        handle = cowrie.alpha_handle(cid_bytes)
        tainted = cowrie.is_tainted(auth)

        virtualized = self.virtualize_bytes > 0 and raw_bytes >= self.virtualize_bytes
        if virtualized:
            rendered = cowrie.project_virtual_atlas(frame, max_chars=self.atlas_max_chars)
        elif self.compact_r2 and (was_structured or tainted):
            rendered = cowrie.project_r2_compact(frame)
        else:
            rendered = output if isinstance(output, str) else json.dumps(payload, separators=(",", ":"))

        with self._lock:
            self._stats["calls"] += 1
            self._stats["bytes_in"] += raw_bytes
            self._stats["bytes_out"] += len(rendered.encode("utf-8"))
            if tainted:
                self._tainted_cids[cid_hex] = cid_bytes
            if self._wal is not None:
                self._wal.append_frame(frame, fsync=False)
            if virtualized:
                self._stats["virtualized"] += 1
                if handle not in self._virtual_store:
                    if len(self._virtual_order) >= self.max_virtual_entries:
                        old = self._virtual_order.popleft()
                        self._virtual_store.pop(old, None)
                    self._virtual_order.append(handle)
                self._virtual_store[handle] = _VirtualEntry(
                    cid=cid_bytes, cid_hex=cid_hex, frame=frame, authority=auth
                )

        return SealedToolOutput(
            tool_name=tool_name,
            frame=frame,
            cid_hex=cid_hex,
            handle=handle,
            authority=auth,
            tainted=tainted,
            virtualized=virtualized,
            raw_bytes=raw_bytes,
            rendered_text=rendered,
        )

    def cowrie_get(
        self,
        handle: str,
        pointer: Optional[str] = None,
        grep: Optional[str] = None,
        lines: Optional[list[int]] = None,
        offset: int = 0,
        max_chars: int = 2400,
    ) -> str:
        """Retrieve an exact RFC 6901 pointer, line range, or grep slice from a virtualized Cowrie handle."""
        with self._lock:
            entry = self._virtual_store.get(handle)
        if entry is None:
            raise KeyError(f"Unknown virtualized Cowrie handle: {handle!r}")
        tainted = cowrie.is_tainted(entry.authority)
        if tainted:
            with self._lock:
                self._tainted_cids[entry.cid_hex] = entry.cid
        slice_text = cowrie.query_frame_slice(
            entry.frame,
            pointer=pointer,
            grep=grep,
            lines=lines,
            offset=offset,
            max_chars=max_chars,
        )
        if tainted:
            return (
                f"<<<COWRIE:SLICE {handle} AUTH=0x{entry.authority:02x}(UNTRUSTED_EXT|TAINTED)>>>\n"
                f"{slice_text}\n<<<END:COWRIE>>>"
            )
        return slice_text

    def wrap_callable(
        self,
        fn: Callable[..., Any],
        *,
        name: Optional[str] = None,
        untrusted: Optional[bool] = None,
        action: Optional[str] = None,
        return_sealed: bool = False,
    ) -> Callable[..., Any]:
        """Wrap any sync or async tool function with causal taint checks, virtualization, and R2-Compact projection."""
        tool_name = name or getattr(fn, "__name__", "tool")

        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def _async_wrapped(*args: Any, **kwargs: Any) -> Any:
                tokens = kwargs.pop("_cowrie_declassify", None)
                await self.acheck_before_call(tool_name, action=action, declassify_tokens=tokens)
                raw = await fn(*args, **kwargs)
                sealed = self.seal_output(tool_name, raw, untrusted=untrusted)
                return sealed if return_sealed else sealed.rendered_text

            return _async_wrapped

        @functools.wraps(fn)
        def _sync_wrapped(*args: Any, **kwargs: Any) -> Any:
            tokens = kwargs.pop("_cowrie_declassify", None)
            self.check_before_call(tool_name, action=action, declassify_tokens=tokens)
            raw = fn(*args, **kwargs)
            sealed = self.seal_output(tool_name, raw, untrusted=untrusted)
            return sealed if return_sealed else sealed.rendered_text

        return _sync_wrapped
