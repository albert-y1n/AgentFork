import asyncio
from types import SimpleNamespace

import pytest

from agentfork.criu_backend import CriuBtrfs, CheckpointRestoreError


def backend(tools, *, fails=False, probe=None):
    adapter = CriuBtrfs.__new__(CriuBtrfs)
    adapter.validation_probe = probe

    async def list_tools():
        return tools

    async def call(name, **kwargs):
        assert name == "shell"
        return SimpleNamespace(is_error=fails, output=kwargs["command"].removeprefix("printf %s "))

    return adapter, SimpleNamespace(backend=SimpleNamespace(list_tools=list_tools), call=call)


def test_empty_tool_list_cannot_qualify():
    adapter, child = backend([])
    with pytest.raises(CheckpointRestoreError, match="no tools"):
        asyncio.run(adapter.validate_child(child))


def test_runtime_metadata_alone_does_not_qualify_broken_execution():
    adapter, child = backend([{"name": "shell"}], fails=True)
    with pytest.raises(CheckpointRestoreError, match="execute"):
        asyncio.run(adapter.validate_child(child))


def test_application_can_reject_runtime_healthy_restore():
    async def application(child):
        return False

    adapter, child = backend([{"name": "shell"}], probe=application)
    with pytest.raises(CheckpointRestoreError, match="Application"):
        asyncio.run(adapter.validate_child(child))


def test_actual_tool_execution_is_recorded():
    adapter, child = backend([{"name": "shell"}])
    value = asyncio.run(adapter.validate_child(child))
    assert value["passed"] and value["tool_execution_verified"]
    assert not value["application_probe_verified"]
