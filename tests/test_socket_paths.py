from pathlib import Path
import os

import pytest

from agentfork_sandbox.podman_pool import socket_directory


def test_long_store_uses_stable_short_socket_directory():
    store = Path("/home/research") / ("deep-workspace-" * 12) / "store"
    chosen = socket_directory(store)
    assert len(os.fsencode(str(chosen) + "/agentfork-000000000000/runtime.sock")) < 108
    assert chosen == socket_directory(store)
    assert chosen != socket_directory(store / "other")


def test_short_store_keeps_existing_socket_layout():
    assert socket_directory(Path("/store")) == Path("/store/sockets")


def test_explicit_overlong_socket_directory_is_rejected():
    with pytest.raises(ValueError, match="too long"):
        socket_directory(Path("/store"), "/tmp/" + "x" * 100)
