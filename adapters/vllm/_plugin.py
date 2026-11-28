"""Lightweight `vllm.general_plugins` entry point for `CowrieKVConnectorV1`.

Kept separate from `cowrie_kv_connector.py` so vLLM startup never eagerly imports `torch`
or loads `libcowrie_core.so` unless `CowrieKVConnectorV1` is actually requested.
"""

from __future__ import annotations


def register_vllm_plugin() -> None:
    """Idempotently register `CowrieKVConnectorV1` with vLLM's `KVConnectorFactory`."""
    try:
        from vllm.distributed.kv_transfer.kv_connector.factory import KVConnectorFactory
    except Exception:
        return

    registry = getattr(KVConnectorFactory, "_registry", {})
    if "CowrieKVConnectorV1" in registry:
        return

    try:
        KVConnectorFactory.register_connector(
            "CowrieKVConnectorV1",
            "cowrie_vllm_adapters.cowrie_kv_connector",
            "CowrieKVConnectorV1",
        )
    except ValueError:
        # Already registered in this process
        pass
