import asyncio
import importlib.util
import json
import os
from pathlib import Path
from types import SimpleNamespace

import agentfork.campaign as campaign

candidate = os.environ.get("AF_TB_CAMPAIGN_CANDIDATE")
if candidate:
    spec = importlib.util.spec_from_file_location("transaction_candidate", candidate)
    campaign = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(campaign)
Case = campaign.Case


def shell_case(tmp_path, engine):
    directory = tmp_path / "case"
    directory.mkdir(exist_ok=True)
    (directory / "run-configuration.json").write_text(json.dumps({"controller_revision": "test"}))
    case = Case.__new__(Case)
    case.directory = directory
    case.engine = engine
    case.rollouts = {}
    case.snapshots = []
    case.outcomes = {}
    case.recovery = None
    case.preserved = False
    case.config = {}
    case.image = "test-image"
    case.base = None
    case.event = lambda *a, **kw: None
    return case


def test_image_cleanup_waits_for_another_cases_import(tmp_path):
    async def check():
        engine = SimpleNamespace()
        first = shell_case(tmp_path, engine)
        second = shell_case(tmp_path, engine)
        started, finish = asyncio.Event(), asyncio.Event()
        events = []

        async def preparing():
            events.append("import-start")
            started.set()
            await finish.wait()
            events.append("import-end")

        async def podman(*args):
            events.append("image-rm")
            return ""

        first._prepare = preparing
        second.podman = podman
        preparing_task = asyncio.create_task(first.prepare())
        await started.wait()
        cleanup_task = asyncio.create_task(second.close())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert events == ["import-start"]
        finish.set()
        await asyncio.gather(preparing_task, cleanup_task)
        assert events == ["import-start", "import-end", "image-rm"]

    asyncio.run(check())


def test_two_case_imports_are_serialized(tmp_path):
    async def check():
        engine = SimpleNamespace()
        a, b = shell_case(tmp_path, engine), shell_case(tmp_path, engine)
        started, finish = asyncio.Event(), asyncio.Event()
        events = []

        async def first():
            events.append("first-start")
            started.set()
            await finish.wait()
            events.append("first-end")

        async def second():
            events.append("second-start")

        a._prepare, b._prepare = first, second
        one = asyncio.create_task(a.prepare())
        await started.wait()
        two = asyncio.create_task(b.prepare())
        await asyncio.sleep(0)
        assert events == ["first-start"]
        finish.set()
        await asyncio.gather(one, two)
        assert events == ["first-start", "first-end", "second-start"]

    asyncio.run(check())


def test_failed_import_releases_the_shared_lock(tmp_path):
    async def check():
        engine = SimpleNamespace()
        a, b = shell_case(tmp_path, engine), shell_case(tmp_path, engine)

        async def fail():
            raise RuntimeError("registry failure")

        called = []

        async def succeed():
            called.append(True)

        a._prepare, b._prepare = fail, succeed
        try:
            await a.prepare()
        except RuntimeError:
            pass
        await asyncio.wait_for(b.prepare(), 1)
        assert called == [True]

    asyncio.run(check())


def inherited_case(tmp_path):
    snapshots = tmp_path / "snapshots"
    (snapshots / "kept").mkdir(parents=True)
    deleted = []

    async def delete(sid):
        deleted.append(sid)

    async def podman(*args):
        return ""

    engine = SimpleNamespace(
        pool=SimpleNamespace(store=SimpleNamespace(snapshots=snapshots), delete_snapshot=delete)
    )
    case = shell_case(tmp_path, engine)
    case.podman = podman
    case.snapshots = ["kept"]
    case.recovery = {
        "retained_snapshots": ["kept"],
        "resume_attempts": {
            "root-0": {
                "point": {"step": 181, "snapshot_id": "kept"},
                "points": [],
                "elapsed_seconds": 3827.3,
                "request_count": 365,
                "api": [],
            }
        },
    }
    return case, deleted


def test_preparation_failure_preserves_inherited_context_and_budget(tmp_path):
    async def check():
        case, deleted = inherited_case(tmp_path)
        assert await case.preserve("image pull failed before Rollout creation")
        await case.close()
        saved = json.loads((case.directory / "recovery.json").read_text())
        resume = saved["resume_attempts"]["root-0"]
        assert resume["point"]["step"] == 181
        assert resume["elapsed_seconds"] == 3827.3 and resume["request_count"] == 365
        assert saved["retained_snapshots"] == ["kept"] and not deleted

    asyncio.run(check())


def test_incomplete_close_protects_inherited_snapshots_without_preserve(tmp_path):
    async def check():
        case, deleted = inherited_case(tmp_path)
        await case.close()
        assert not deleted and case.preserved

    asyncio.run(check())


def test_completed_resumed_case_keeps_its_snapshots_by_default(tmp_path):
    async def check():
        case, deleted = inherited_case(tmp_path)
        (case.directory / "result.json").write_text(json.dumps({"status": "complete"}))
        await case.close()
        assert not deleted

    asyncio.run(check())


def test_explicit_completed_checkpoint_discard_releases_snapshots(tmp_path):
    async def check():
        case, deleted = inherited_case(tmp_path)
        case.args = SimpleNamespace(discard_completed_checkpoints=True)
        (case.directory / "result.json").write_text(json.dumps({"status": "complete"}))
        await case.close()
        assert deleted == ["kept"]

    asyncio.run(check())


def test_held_recovery_never_reaches_fresh_case_creation(tmp_path):
    async def check():
        import ast

        source = Path(candidate) if candidate else Path(campaign.__file__)
        tree = ast.parse(source.read_text())
        worker = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == "worker")
        folder = tmp_path / "held"
        folder.mkdir()
        (folder / "recovery-hold.json").write_text("{}")
        trajectory = folder / "native.jsonl"
        trajectory.write_text("original trajectory")
        queue = asyncio.Queue()
        queue.put_nowait({"task": "held", "path": str(folder)})
        results = []

        def forbidden_case(*args, **kwargs):
            raise AssertionError("A held recovery was restarted")

        scope = {
            "queue": queue,
            "args": SimpleNamespace(output=tmp_path),
            "engine": SimpleNamespace(),
            "store_is_available": lambda e: True,
            "results": results,
            "summary": lambda: None,
            "json": json,
            "time": __import__("time"),
            "Case": forbidden_case,
        }
        exec(compile(ast.Module(body=[worker], type_ignores=[]), "worker_hold_test", "exec"), scope)
        await scope["worker"]()
        assert results == [{"task": "held", "status": "recovery_held"}]
        assert trajectory.read_text() == "original trajectory"

    asyncio.run(check())
