"""Drop-in Cowrie adapter for PydanticAI agents & tools.

Zero-migration usage:
    from pydantic_ai import Agent
    from cowrie.integrations.pydantic_ai import CowriePydanticAIGuard

    guard = CowriePydanticAIGuard(virtualize_bytes=16_384)
    agent = Agent("openai:gpt-4.1")
    guard.register_cowrie_get(agent)

    @agent.tool_plain
    @guard.tool(untrusted=True)
    def web_search(query: str) -> dict:
        ...

    @agent.tool_plain
    @guard.tool(action="shell_exec")
    def run_shell(cmd: str) -> str:
        ...
"""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Optional

from cowrie.integrations.guard import CowrieTaintBlockedError, CowrieToolGuard


class CowriePydanticAIGuard(CowrieToolGuard):
    """PydanticAI tool decorator & `cowrie_get` registrar preserving `RunContext` signatures."""

    def tool(
        self,
        fn: Optional[Callable[..., Any]] = None,
        *,
        name: Optional[str] = None,
        untrusted: Optional[bool] = None,
        action: Optional[str] = None,
    ) -> Any:
        """Decorator wrapping sync or async PydanticAI tools (compatible with `@agent.tool` and `@agent.tool_plain`)."""
        def _decorate(func: Callable[..., Any]) -> Callable[..., Any]:
            tool_name = name or getattr(func, "__name__", "tool")
            wrapped = self.wrap_callable(func, name=tool_name, untrusted=untrusted, action=action)
            # Explicitly preserve original signature so PydanticAI's RunContext & schema inspection work unchanged.
            wrapped.__signature__ = inspect.signature(func)  # type: ignore[attr-defined]
            return wrapped

        return _decorate(fn) if fn is not None else _decorate

    def register_cowrie_get(self, agent: Any) -> Callable[..., str]:
        """Register `cowrie_get` on a PydanticAI `Agent` (or return the plain callable if `agent` is None)."""
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

        if agent is not None:
            if hasattr(agent, "tool_plain"):
                agent.tool_plain(cowrie_get)
            elif hasattr(agent, "tool"):
                @functools.wraps(cowrie_get)
                def _with_ctx(_ctx: Any, handle: str, pointer: Optional[str] = None, grep: Optional[str] = None,
                              lines: Optional[list[int]] = None, offset: int = 0, max_chars: int = 2400) -> str:
                    return cowrie_get(handle, pointer=pointer, grep=grep, lines=lines, offset=offset, max_chars=max_chars)
                agent.tool(_with_ctx)
        return cowrie_get


__all__ = [
    "CowriePydanticAIGuard",
    "CowrieTaintBlockedError",
]
