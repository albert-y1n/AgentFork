import asyncio, json, time
from types import SimpleNamespace
from agentfork.campaign import Case, store_is_available


def test_interrupted_case_retains_snapshots_and_resumable_context(tmp_path):
    async def check():
        snapshots = tmp_path / "snapshots"
        (snapshots / "kept").mkdir(parents=True)
        case = Case.__new__(Case)
        case.directory = tmp_path
        case.snapshots = ["kept"]
        case.outcomes = {}
        case.config = {}
        case.image = "probe"
        case.base = None
        case.preserved = False
        case.event = lambda *a, **k: None
        (tmp_path / "run-configuration.json").write_text(json.dumps({"controller_revision": "test"}))
        deleted = []

        async def delete(sid):
            deleted.append(sid)

        async def podman(*a):
            return ""

        pool = SimpleNamespace(store=SimpleNamespace(snapshots=snapshots), delete_snapshot=delete)
        case.engine = SimpleNamespace(pool=pool)
        case.podman = podman
        case.rollouts = {
            "root-0": SimpleNamespace(
                points=[
                    {"step": 4, "snapshot_id": "kept", "memory_checkpoint": {"validation": {"passed": True}}}
                ],
                request_count=8,
                api=[{"complete": True}],
                elapsed_before=5,
                started_at=time.time() - 2,
                sandbox=None,
            )
        }
        assert await case.preserve("disk reserve")
        await case.close()
        recovery = json.loads((tmp_path / "recovery.json").read_text())
        assert recovery["state"] == "queued" and recovery["retained_snapshots"] == ["kept"]
        assert recovery["resume_attempts"]["root-0"]["point"]["step"] == 4
        assert recovery["resume_attempts"]["root-0"]["elapsed_seconds"] >= 7
        assert not deleted and (snapshots / "kept").exists()

    asyncio.run(check())


def test_unavailable_mount_keeps_context_and_snapshot_references(tmp_path):
    async def check():
        store = tmp_path / "mounted"
        snapshots = store / "snapshots"
        (snapshots / "kept").mkdir(parents=True)
        output = tmp_path / "output"
        output.mkdir()
        (output / "run-configuration.json").write_text(json.dumps({"controller_revision": "test"}))
        history = output / "request-history"
        history.write_bytes(b"original raw trajectory")
        deleted = []

        async def delete(sid):
            deleted.append(sid)

        async def podman(*args):
            return ""

        pool = SimpleNamespace(store=SimpleNamespace(path=store, snapshots=snapshots), delete_snapshot=delete)
        engine = SimpleNamespace(pool=pool, store_device=store.stat().st_dev)
        case = Case.__new__(Case)
        case.directory = output
        case.snapshots = ["kept"]
        case.outcomes = {}
        case.config = {}
        case.image = "probe"
        case.base = None
        case.preserved = False
        case.event = lambda *a, **k: None
        case.engine = engine
        case.podman = podman
        case.rollouts = {
            "root-0": SimpleNamespace(
                points=[
                    {"step": 7, "snapshot_id": "kept", "memory_checkpoint": {"validation": {"passed": True}}}
                ],
                request_count=15,
                api=[{"complete": True}],
                elapsed_before=11,
                started_at=time.time() - 2,
                sandbox=None,
            )
        }
        assert store_is_available(engine)
        hidden = tmp_path / "temporarily-invisible"
        store.rename(hidden)
        assert not store_is_available(engine)
        assert await case.preserve("mount unavailable")
        await case.close()
        recovery = json.loads((output / "recovery.json").read_text())
        assert recovery["requires_store_validation"]
        assert recovery["retained_snapshots"] == ["kept"]
        assert recovery["resume_attempts"]["root-0"]["point"]["step"] == 7
        assert recovery["resume_attempts"]["root-0"]["request_count"] == 15
        assert recovery["resume_attempts"]["root-0"]["elapsed_seconds"] >= 13
        hidden.rename(store)
        assert store_is_available(engine)
        assert all((snapshots / sid).is_dir() for sid in recovery["retained_snapshots"])
        assert not deleted and history.read_bytes() == b"original raw trajectory"

    asyncio.run(check())


def test_unmounted_host_directory_is_not_a_checkpoint_store(tmp_path):
    pool = SimpleNamespace(store=SimpleNamespace(path=tmp_path))
    engine = SimpleNamespace(pool=pool, store_device=tmp_path.stat().st_dev + 1)
    assert tmp_path.is_dir() and not store_is_available(engine)
