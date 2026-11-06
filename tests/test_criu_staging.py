import asyncio
from pathlib import Path
from types import SimpleNamespace
import pytest

from agentfork.criu_staging import CompressedStaging


def stage(tmp_path):
    root = tmp_path / "compressed"
    metadata = tmp_path / "metadata"
    static = metadata / "container/userdata"
    static.mkdir(parents=True)

    async def run(*args):
        pass

    return CompressedStaging(root, metadata, run), static


def test_unrelated_paths_and_container_names_are_rejected(tmp_path):
    cache, static = stage(tmp_path)
    for sid, path in [
        ("agentfork-abc", static),
        ("../other", static),
        ("agentfork-123456abcdef", tmp_path / "outside"),
    ]:
        with pytest.raises(ValueError):
            cache.paths(sid, path)


def test_existing_checkpoint_images_are_never_hidden_or_deleted(tmp_path):
    cache, static = stage(tmp_path)
    images = static / "checkpoint"
    images.mkdir()
    p = images / "pages.img"
    p.write_bytes(b"needed parent recovery bytes")
    with pytest.raises(ValueError, match="hide existing"):
        asyncio.run(cache.ensure("agentfork-123456abcdef", static))
    assert p.read_bytes() == b"needed parent recovery bytes"


def test_foreign_mount_is_not_adopted_or_unmounted(tmp_path, monkeypatch):
    cache, static = stage(tmp_path)
    sid = "agentfork-123456abcdef"
    target = static / "checkpoint"
    target.mkdir()
    monkeypatch.setattr("agentfork.criu_staging.os.path.ismount", lambda path: True)
    calls = []

    async def run(argv):
        calls.append(argv)

    cache.run = run
    with pytest.raises(ValueError, match="unrelated"):
        asyncio.run(cache.ensure(sid, static))
    assert calls == []


def test_clear_keeps_bind_root_and_ignores_symlink_destinations(tmp_path, monkeypatch):
    cache, static = stage(tmp_path)
    sid = "agentfork-123456abcdef"
    source, target = cache.paths(sid, static)
    source.mkdir(parents=True)
    target.mkdir()
    (target / "pages.img").write_bytes(b"pages")
    outside = tmp_path / "preserved"
    outside.write_bytes(b"not scratch")
    (target / "link").symlink_to(outside)
    cache.bindings[sid] = (source, target)
    monkeypatch.setattr(cache, "owns_mount", lambda *args: True)
    assert cache.clear(sid, static) and target.is_dir() and list(target.iterdir()) == []
    assert outside.read_bytes() == b"not scratch"


def test_changed_mount_cannot_be_cleared_or_released(tmp_path, monkeypatch):
    cache, static = stage(tmp_path)
    sid = "agentfork-123456abcdef"
    cache.bindings[sid] = cache.paths(sid, static)
    monkeypatch.setattr(cache, "owns_mount", lambda *args: False)
    with pytest.raises(ValueError):
        cache.clear(sid, static)
    with pytest.raises(ValueError):
        asyncio.run(cache.release(sid))
    assert sid in cache.bindings


def test_persisted_binding_can_be_released_after_controller_registry_is_lost(tmp_path, monkeypatch):
    cache, static = stage(tmp_path)
    sid = "agentfork-123456abcdef"
    source, target = cache.paths(sid, static)
    source.mkdir(parents=True)
    target.mkdir()
    (source / "pages.img").write_bytes(b"ephemeral pages")
    cache.journal(sid, source, target)
    assert cache.bindings == {}
    monkeypatch.setattr(cache, "owns_mount", lambda *args: True)
    calls = []

    async def run(argv):
        calls.append(argv)

    cache.run = run
    asyncio.run(cache.release(sid))
    assert calls == [["umount", target]] and not source.exists()
    assert not (cache.root / (sid + ".binding.json")).exists()
