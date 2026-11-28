"""Drop-in Cowrie ecosystem connectors for Layer 3 AI frameworks.

All integrations are zero-hard-dependency (they work cleanly whether or not optional
framework packages are installed) and share the single audited `libcowrie_core` engine.

Available connectors:
  - `cowrie.integrations.guard`         : Shared `CowrieToolGuard` (virtualize + R2-Compact + 0x80 taint + Ed25519)
  - `cowrie.integrations.langgraph`     : LangGraph `ToolNode` / LangChain tool wrapper & `.cwwal` audit recorder
  - `cowrie.integrations.pydantic_ai`   : PydanticAI `@guard.tool` decorator & `cowrie_get` registrar
  - `cowrie.integrations.openai_agents` : OpenAI Agents SDK `FunctionTool` wrapper & `RunHooks`
  - `cowrie.integrations.diffusers`     : Hugging Face Diffusers `CWSP` canonical prompt-embedding cache
  - `cowrie.integrations.lancedb`       : LanceDB -> Cowrie sealed `CWRF`/MCP frame cookbook helper
"""

from cowrie.integrations.guard import (
    CowrieTaintBlockedError,
    CowrieToolGuard,
    SealedToolOutput,
    TaintBlockContext,
    make_ed25519_hitl_callback,
)

__all__ = [
    "CowrieTaintBlockedError",
    "CowrieToolGuard",
    "SealedToolOutput",
    "TaintBlockContext",
    "make_ed25519_hitl_callback",
]
