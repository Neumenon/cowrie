"""Drop-in Cowrie adapter for the OpenAI Agents SDK (`openai-agents`).

Zero-migration usage:
    from agents import Agent, function_tool
    from cowrie.integrations.openai_agents import CowrieOpenAIAgentsGuard

    guard = CowrieOpenAIAgentsGuard(virtualize_bytes=16_384, audit_wal_path="session.cwwal")
    tools = guard.wrap_tools([web_fetch_tool, run_shell_tool])
    agent = Agent(name="Ops", tools=tools)
    # Optionally attach `hooks=guard.make_run_hooks()` to `Runner.run(agent, ..., hooks=...)`.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import inspect
import json
from typing import Any, Callable, Iterable, Optional

from cowrie.integrations.guard import CowrieTaintBlockedError, CowrieToolGuard


@dataclass
class _DuckFunctionTool:
    """Minimal duck-typed fallback matching OpenAI Agents SDK `FunctionTool` when `openai-agents` is not installed."""

    name: str
    description: str
    params_json_schema: dict[str, Any]
    on_invoke_tool: Callable[[Any, str], Any]


class CowrieRunHooks:
    """OpenAI Agents SDK `RunHooks`-compatible lifecycle hooks enforcing Cowrie causal taint & WAL auditing."""

    def __init__(self, guard: CowrieToolGuard):
        self.guard = guard

    async def on_tool_start(self, context: Any, agent: Any, tool: Any) -> None:
        tool_name = getattr(tool, "name", None) or getattr(tool, "__name__", "tool")
        tokens = getattr(context, "cowrie_declassify_tokens", None)
        await self.guard.acheck_before_call(tool_name, declassify_tokens=tokens)

    async def on_tool_end(self, context: Any, agent: Any, tool: Any, result: str) -> None:
        tool_name = getattr(tool, "name", None) or getattr(tool, "__name__", "tool")
        # Record provenance & taint in guard ledger/WAL even for tools not wrapped individually.
        self.guard.seal_output(tool_name, result)


class CowrieOpenAIAgentsGuard(CowrieToolGuard):
    """OpenAI Agents SDK `FunctionTool` wrapper, `cowrie_get` tool factory, and `RunHooks` adapter."""

    def make_cowrie_get_tool(self) -> Any:
        """Create a `cowrie_get` `FunctionTool` (using `agents.function_tool` if installed, else duck-typed)."""
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
            from agents import function_tool
            return function_tool(cowrie_get)
        except ImportError:
            async def _on_invoke(_ctx: Any, args_json: str) -> str:
                parsed = json.loads(args_json) if args_json else {}
                return cowrie_get(**parsed)

            return _DuckFunctionTool(
                name="cowrie_get",
                description=(
                    "Retrieve an exact RFC 6901 JSON Pointer, line range, or bounded grep slice "
                    "from a virtualized Cowrie handle (preserving causal taint)."
                ),
                params_json_schema={
                    "type": "object",
                    "required": ["handle"],
                    "properties": {
                        "handle": {"type": "string"},
                        "pointer": {"type": "string"},
                        "grep": {"type": "string"},
                        "lines": {"type": "array", "items": {"type": "integer"}},
                        "offset": {"type": "integer"},
                        "max_chars": {"type": "integer"},
                    },
                },
                on_invoke_tool=_on_invoke,
            )

    def wrap_function_tool(
        self,
        tool: Any,
        *,
        untrusted: Optional[bool] = None,
        action: Optional[str] = None,
    ) -> Any:
        """Wrap an OpenAI Agents SDK `FunctionTool` (via `on_invoke_tool`) or a plain callable."""
        if callable(tool) and not hasattr(tool, "on_invoke_tool"):
            return self.wrap_callable(tool, untrusted=untrusted, action=action)

        tool_name = getattr(tool, "name", "tool")
        orig_invoke = tool.on_invoke_tool

        async def _guarded_invoke(ctx: Any, args_json: str) -> str:
            tokens = None
            clean_args_json = args_json
            if isinstance(args_json, str) and args_json.strip().startswith("{"):
                try:
                    parsed = json.loads(args_json)
                    if isinstance(parsed, dict) and "_cowrie_declassify" in parsed:
                        tokens = parsed.pop("_cowrie_declassify")
                        clean_args_json = json.dumps(parsed, separators=(",", ":"))
                except json.JSONDecodeError:
                    pass
            if tokens is None:
                tokens = getattr(ctx, "cowrie_declassify_tokens", None)
            await self.acheck_before_call(tool_name, action=action, declassify_tokens=tokens)
            res = orig_invoke(ctx, clean_args_json)
            if inspect.isawaitable(res):
                res = await res
            return self.seal_output(tool_name, res, untrusted=untrusted).rendered_text

        try:
            return replace(tool, on_invoke_tool=_guarded_invoke)
        except TypeError:
            object.__setattr__(tool, "on_invoke_tool", _guarded_invoke)
            return tool

    def wrap_tools(self, tools: Iterable[Any], *, include_cowrie_get: bool = True) -> list[Any]:
        wrapped = [self.wrap_function_tool(t) for t in tools]
        if include_cowrie_get and self.virtualize_bytes > 0:
            names = {getattr(t, "name", getattr(t, "__name__", "")) for t in wrapped}
            if "cowrie_get" not in names:
                wrapped.append(self.make_cowrie_get_tool())
        return wrapped

    def make_run_hooks(self) -> CowrieRunHooks:
        return CowrieRunHooks(self)


__all__ = [
    "CowrieOpenAIAgentsGuard",
    "CowrieRunHooks",
    "CowrieTaintBlockedError",
]
