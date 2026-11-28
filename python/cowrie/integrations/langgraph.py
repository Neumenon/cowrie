"""Drop-in Cowrie adapter for LangGraph & LangChain tools.

Zero-migration usage:
    from cowrie.integrations.langgraph import CowrieLangGraphGuard

    guard = CowrieLangGraphGuard(virtualize_bytes=16_384, audit_wal_path="agent.cwwal")
    safe_tools = guard.wrap_tools([web_search, run_shell, read_file])
    # Pass `safe_tools` directly to `ToolNode(safe_tools)` or `create_react_agent(llm, safe_tools)`.
"""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Iterable, Optional

import cowrie
from cowrie.integrations.guard import (
    CowrieTaintBlockedError,
    CowrieToolGuard,
    TaintBlockContext,
    make_ed25519_hitl_callback,
)


def make_langgraph_interrupt_approver(
    seed_32: bytes,
    *,
    ttl_secs: int = 300,
    interrupt_fn: Optional[Callable[[dict[str, Any]], Any]] = None,
) -> Callable[[TaintBlockContext], Any]:
    """Create an `on_taint_block` callback wired directly into LangGraph's `interrupt()` HITL pause/resume.

    When a tainted node attempts a privileged tool call, LangGraph pauses with an approval payload
    containing `(tool, action, tainted_cids)`. Resuming with `Command(resume={"approved": True})`
    mints scoped RFC 8032 Ed25519 declassification tokens and resumes the tool call seamlessly.
    """
    def _ask_human(ctx: TaintBlockContext) -> Any:
        fn = interrupt_fn
        if fn is None:
            from langgraph.types import interrupt as _lg_interrupt
            fn = _lg_interrupt
        return fn(
            {
                "kind": "cowrie_taint_approval",
                "tool": ctx.tool_name,
                "action": ctx.action,
                "tainted_cids": list(ctx.tainted_cids),
            }
        )

    return make_ed25519_hitl_callback(seed_32, _ask_human, ttl_secs=ttl_secs)


class CowrieLangGraphGuard(CowrieToolGuard):
    """LangGraph / LangChain tool wrapper, `ToolNode` middleware, and `cowrie_get` provider."""

    def make_cowrie_get_tool(self) -> Any:
        """Return a `cowrie_get` tool ready for LangGraph `ToolNode` (StructuredTool when langchain_core is installed)."""
        def cowrie_get(
            handle: str,
            pointer: Optional[str] = None,
            grep: Optional[str] = None,
            lines: Optional[list[int]] = None,
            offset: int = 0,
            max_chars: int = 2400,
        ) -> str:
            """Retrieve an exact RFC 6901 JSON Pointer, line range, or grep slice from a virtualized Cowrie handle."""
            return self.cowrie_get(
                handle=handle,
                pointer=pointer,
                grep=grep,
                lines=lines,
                offset=offset,
                max_chars=max_chars,
            )

        try:
            from langchain_core.tools import StructuredTool
            return StructuredTool.from_function(
                func=cowrie_get,
                name="cowrie_get",
                description=(
                    "Retrieve an exact RFC 6901 JSON Pointer, line range, or bounded grep slice "
                    "from a virtualized Cowrie handle (preserving causal taint)."
                ),
            )
        except ImportError:
            return cowrie_get

    def wrap_tool(
        self,
        tool: Any,
        *,
        untrusted: Optional[bool] = None,
        action: Optional[str] = None,
    ) -> Any:
        """Wrap either a LangChain `BaseTool`/`StructuredTool` or a plain sync/async Python callable."""
        if callable(tool) and not hasattr(tool, "_run"):
            return self.wrap_callable(tool, untrusted=untrusted, action=action)

        tool_name = getattr(tool, "name", None) or getattr(tool, "__name__", "tool")
        orig_func = getattr(tool, "func", None)
        orig_coro = getattr(tool, "coroutine", None)

        if orig_func is not None or orig_coro is not None:
            # StructuredTool / @tool path: shallow copy preserving schema/metadata
            wrapped = tool.model_copy() if hasattr(tool, "model_copy") else tool
            if orig_func is not None:
                wrapped.func = self.wrap_callable(
                    orig_func, name=tool_name, untrusted=untrusted, action=action
                )
            if orig_coro is not None:
                wrapped.coroutine = self.wrap_callable(
                    orig_coro, name=tool_name, untrusted=untrusted, action=action
                )
            return wrapped

        # Generic BaseTool subclass overriding `_run` / `_arun`
        orig_run = tool._run
        orig_arun = getattr(tool, "_arun", None)

        @functools.wraps(orig_run)
        def _guarded_run(*args: Any, **kwargs: Any) -> Any:
            tokens = kwargs.pop("_cowrie_declassify", None)
            self.check_before_call(tool_name, action=action, declassify_tokens=tokens)
            raw = orig_run(*args, **kwargs)
            return self.seal_output(tool_name, raw, untrusted=untrusted).rendered_text

        object.__setattr__(tool, "_run", _guarded_run)

        if orig_arun is not None and inspect.iscoroutinefunction(orig_arun):
            @functools.wraps(orig_arun)
            async def _guarded_arun(*args: Any, **kwargs: Any) -> Any:
                tokens = kwargs.pop("_cowrie_declassify", None)
                self.check_before_call(tool_name, action=action, declassify_tokens=tokens)
                raw = await orig_arun(*args, **kwargs)
                return self.seal_output(tool_name, raw, untrusted=untrusted).rendered_text

            object.__setattr__(tool, "_arun", _guarded_arun)

        return tool

    def wrap_tools(self, tools: Iterable[Any], *, include_cowrie_get: bool = True) -> list[Any]:
        """Wrap an iterable of LangGraph/LangChain tools and optionally append `cowrie_get`."""
        wrapped = [self.wrap_tool(t) for t in tools]
        if include_cowrie_get and self.virtualize_bytes > 0:
            names = {getattr(t, "name", getattr(t, "__name__", "")) for t in wrapped}
            if "cowrie_get" not in names:
                wrapped.append(self.make_cowrie_get_tool())
        return wrapped

    def wrap_tool_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        """Drop-in middleware for LangGraph `ToolNode(tools, wrap_tool_call=guard.wrap_tool_call)`."""
        tc = getattr(request, "tool_call", request)
        tool_name = tc.get("name", "") if isinstance(tc, dict) else getattr(tc, "name", "")
        args = tc.get("args", {}) if isinstance(tc, dict) else getattr(tc, "args", {})
        tokens = args.get("_cowrie_declassify") if isinstance(args, dict) else None
        self.check_before_call(tool_name, declassify_tokens=tokens)

        result = handler(request)
        if hasattr(result, "content"):
            sealed = self.seal_output(tool_name, result.content)
            try:
                result.content = sealed.rendered_text
            except Exception:
                if hasattr(result, "model_copy"):
                    result = result.model_copy(update={"content": sealed.rendered_text})
            return result
        return self.seal_output(tool_name, result).rendered_text

    async def awrap_tool_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        """Async middleware for LangGraph `ToolNode(tools, awrap_tool_call=guard.awrap_tool_call)`."""
        tc = getattr(request, "tool_call", request)
        tool_name = tc.get("name", "") if isinstance(tc, dict) else getattr(tc, "name", "")
        args = tc.get("args", {}) if isinstance(tc, dict) else getattr(tc, "args", {})
        tokens = args.get("_cowrie_declassify") if isinstance(args, dict) else None
        await self.acheck_before_call(tool_name, declassify_tokens=tokens)

        result = handler(request)
        if inspect.isawaitable(result):
            result = await result
        if hasattr(result, "content"):
            sealed = self.seal_output(tool_name, result.content)
            try:
                result.content = sealed.rendered_text
            except Exception:
                if hasattr(result, "model_copy"):
                    result = result.model_copy(update={"content": sealed.rendered_text})
            return result
        return self.seal_output(tool_name, result).rendered_text


class CowrieAuditRecorder:
    """Hash-chained `.cwwal` checkpoint & tool-audit recorder for LangGraph runs."""

    def __init__(self, wal_path: str):
        self._wal = cowrie.Wal(wal_path, recover_torn_tail=True)

    def record_step(
        self,
        thread_id: str,
        node: str,
        state: dict[str, Any],
        *,
        authority: int = cowrie.AUTH_VERIFIED_TOOL,
        fsync: bool = False,
    ) -> tuple[int, str]:
        frame = cowrie.encode_json(
            {"thread_id": thread_id, "node": node, "state": state},
            authority=authority,
            epistemic=cowrie.EPI_OBSERVED,
        )
        seq, cid = self._wal.append_frame(frame, fsync=fsync)
        return seq, cid.hex()

    def replay_frames(self) -> list[dict[str, Any]]:
        import json
        return [json.loads(cowrie.decode_to_json(f)) for f in self._wal.live_frames()]

    def compact_to_shard(self, out_shard_path: str) -> str:
        return self._wal.compact_to_shard(out_shard_path).hex()

    def close(self) -> None:
        self._wal.close()

    def __enter__(self) -> "CowrieAuditRecorder":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


__all__ = [
    "CowrieAuditRecorder",
    "CowrieLangGraphGuard",
    "CowrieTaintBlockedError",
    "make_langgraph_interrupt_approver",
]
