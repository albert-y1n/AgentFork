import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from agentfork.prepared_environment import record_prepared, reuse_prepared, enable_compressed_bases


def setup(tmp_path):
    snapshots = tmp_path / "snapshots"
    template = snapshots / "prepared"
    (template / "usr/local/bin").mkdir(parents=True)
    (template / "usr/local/bin/tb40-claude").write_bytes(b"original CLI")
    (snapshots / "prepared.json").write_text('{"config":{"WorkingDir":"/app"}}')
    task = tmp_path / "task"
    task.mkdir()
    (task / "task.toml").write_text('version = "1"')
    directory = tmp_path / "output"
    directory.mkdir()

    async def btrfs(*args):
        return "ro=true\n"

    case = SimpleNamespace(
        name="task",
        path=task,
        directory=directory,
        image="image@sha256:abc",
        cwd="/app",
        user="1000",
        environment_env={"APP_MODE": "original"},
        engine=SimpleNamespace(
            pool=SimpleNamespace(store=SimpleNamespace(snapshots=snapshots, _btrfs=btrfs))
        ),
        recovery={"retained_snapshots": ["prepared"]},
        event=lambda *a, **k: None,
    )
    record_prepared(case, "prepared")
    return case, template


def test_reuse_keeps_original_template_and_activated_environment(tmp_path):
    case, template = setup(tmp_path)
    case.cwd = "/wrong"
    case.user = "wrong"
    case.environment_env = {}
    assert asyncio.run(reuse_prepared(case))
    assert case.prepared == "prepared" and case.base is None
    assert (case.cwd, case.user, case.environment_env) == ("/app", "1000", {"APP_MODE": "original"})


@pytest.mark.parametrize("failure", ["task", "cli", "config", "missing", "writable"])
def test_invalid_template_never_silently_reimports_or_changes_context(tmp_path, failure):
    case, template = setup(tmp_path)
    if failure == "task":
        (case.path / "task.toml").write_text('version="2"')
    elif failure == "cli":
        (template / "usr/local/bin/tb40-claude").write_bytes(b"changed")
    elif failure == "config":
        template.with_name("prepared.json").write_text("{}")
    elif failure == "missing":
        case.recovery["retained_snapshots"] = []
    elif failure == "writable":

        async def writable(*args):
            return "ro=false\n"

        case.engine.pool.store._btrfs = writable
    with pytest.raises(ValueError):
        asyncio.run(reuse_prepared(case))
    assert not hasattr(case, "prepared")


def test_fresh_case_does_not_reuse_old_template(tmp_path):
    case, _ = setup(tmp_path)
    case.recovery = None
    assert not asyncio.run(reuse_prepared(case))


def test_legacy_case_without_explicit_record_requires_normal_preparation(tmp_path):
    case, _ = setup(tmp_path)
    (case.directory / "prepared-environment.json").unlink()
    assert not asyncio.run(reuse_prepared(case))


def test_compression_is_set_on_empty_base_before_extraction(tmp_path):
    events = []

    async def create(path):
        events.append(("create", path))

    async def btrfs(*args):
        events.append(("property", args))

    async def delete(path):
        events.append(("delete", path))

    store = SimpleNamespace(bases=tmp_path / "bases", create=create, _btrfs=btrfs, delete=delete)
    enable_compressed_bases(store)
    path = store.bases / "image"
    asyncio.run(store.create(path))
    assert events == [("create", path), ("property", ("property", "set", str(path), "compression", "zstd"))]
    events.clear()
    asyncio.run(store.create(tmp_path / "sandboxes/child"))
    assert len(events) == 1 and events[0][0] == "create"


def test_failed_compression_configuration_removes_only_new_empty_base(tmp_path):
    deleted = []

    async def create(path):
        pass

    async def btrfs(*args):
        raise RuntimeError("property failed")

    async def delete(path):
        deleted.append(path)

    store = SimpleNamespace(bases=tmp_path / "bases", create=create, _btrfs=btrfs, delete=delete)
    enable_compressed_bases(store)
    path = store.bases / "new-image"
    with pytest.raises(RuntimeError):
        asyncio.run(store.create(path))
    assert deleted == [path]
