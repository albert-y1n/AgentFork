import json, os
from types import SimpleNamespace
from agentfork.archive_successes import ROOT, archive_completed, main
import pytest


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def test_completed_success_archives_all_trials_and_keeps_source_and_symlinks(tmp_path):
    campaign = tmp_path / "runs"
    source = campaign / "task"
    archive = tmp_path / "archive"
    save(
        source / "result.json",
        {
            "status": "complete",
            "agentfork_resolved": True,
            "outcomes": {"root-0": {"reward": 0}, "branch-r1-0": {"reward": 1, "grade": {"reward": 1}}},
        },
    )
    save(
        source / "attempts/branch-r1-0/grade/grade.json",
        {
            "status": "graded",
            "reward": 1,
            "original_task": str((ROOT / "datasets/terminal-bench/task").resolve()),
        },
    )
    save(source / "attempts/root-0/trajectory.json", {"failed_trial": "retained"})
    os.symlink("root-0/trajectory.json", source / "attempts/latest")
    before = (source / "result.json").read_bytes()
    new, count = archive_completed(campaign, archive)
    assert len(new) == count == 1
    assert (source / "result.json").read_bytes() == before
    for path in [source / "result.json", source / "attempts/root-0/trajectory.json"]:
        assert os.path.samestat(path.stat(), (archive / "task" / path.relative_to(source)).stat())
    assert os.readlink(archive / "task/attempts/latest") == "root-0/trajectory.json"
    assert archive_completed(campaign, archive) == ([], 1)


def test_claimed_success_without_matching_official_grade_is_not_published(tmp_path):
    campaign = tmp_path / "runs"
    source = campaign / "task"
    archive = tmp_path / "archive"
    save(
        source / "result.json",
        {
            "status": "complete",
            "agentfork_resolved": True,
            "outcomes": {"root-0": {"reward": 1, "grade": {"reward": 1}}},
        },
    )
    save(source / "attempts/root-0/grade/grade.json", {"status": "graded", "reward": 0})
    with pytest.raises(RuntimeError):
        archive_completed(campaign, archive)
    assert not (archive / "task").exists()
    assert (source / "result.json").exists()


def test_incomplete_success_does_not_archive_mutating_trajectory(tmp_path):
    campaign = tmp_path / "runs"
    source = campaign / "task"
    archive = tmp_path / "archive"
    save(source / "result.json", {"status": "running", "agentfork_resolved": True})
    assert archive_completed(campaign, archive) == ([], 0)


def test_grade_for_another_task_does_not_validate_claimed_success(tmp_path):
    campaign = tmp_path / "runs"
    source = campaign / "task"
    archive = tmp_path / "archive"
    save(
        source / "result.json",
        {
            "status": "complete",
            "agentfork_resolved": True,
            "outcomes": {"root-0": {"reward": 1, "grade": {"reward": 1}}},
        },
    )
    save(
        source / "attempts/root-0/grade/grade.json",
        {"status": "graded", "reward": 1, "original_task": "/different/task"},
    )
    with pytest.raises(RuntimeError):
        archive_completed(campaign, archive)
    assert not (archive / "task").exists()


def test_one_shot_cannot_overwrite_live_archiver_identity_or_health(tmp_path):
    campaign = tmp_path / "runs"
    campaign.mkdir()
    archive = tmp_path / "archive"
    (campaign / "archive.pid").write_text("777\n")
    status = {"state": "watching", "pid": 777, "updated_at": 100}
    save(campaign / "archive-status.json", status)
    main(SimpleNamespace(campaign=campaign, archive_root=archive, once=True))
    assert (campaign / "archive.pid").read_text() == "777\n"
    assert json.loads((campaign / "archive-status.json").read_text()) == status
    assert json.loads((campaign / "archive-once-status.json").read_text())["state"] == "complete"


def test_one_shot_without_daemon_does_not_register_an_expected_live_process(tmp_path):
    campaign = tmp_path / "runs"
    campaign.mkdir()
    archive = tmp_path / "archive"
    main(SimpleNamespace(campaign=campaign, archive_root=archive, once=True))
    assert not (campaign / "archive.pid").exists()
    assert not (campaign / "archive-status.json").exists()
    assert json.loads((campaign / "archive-once-status.json").read_text())["archived_tasks"] == 0


def test_encoded_checkpoint_archive_survives_removing_its_source(tmp_path):
    import asyncio, shutil
    from agentfork.checkpoint_storage import compact_checkpoint, digest, checkpoint_archive

    campaign = tmp_path / "runs"
    source = campaign / "task"
    archive = tmp_path / "archive"
    base = source / "attempts/root-0/checkpoint-artifacts/00001/checkpoint.tar.gz"
    target = source / "attempts/root-0/checkpoint-artifacts/00002/checkpoint.tar.gz"
    base.parent.mkdir(parents=True)
    target.parent.mkdir(parents=True)
    data = os.urandom(2 * 2**20)
    base.write_bytes(data)
    target.write_bytes(data[:-4096] + b"x" * 4096)
    expected = digest(target)
    original = target.read_bytes()
    points = [{"memory_checkpoint": {"archive": str(p), "archive_sha256": digest(p)}} for p in [base, target]]
    save(source / "attempts/root-0/checkpoints.json", points)
    result = compact_checkpoint(target, base, case=source, expected_sha=expected, reference_sha=digest(base))
    assert result["status"] == "encoded"
    save(
        source / "result.json",
        {
            "status": "complete",
            "agentfork_resolved": True,
            "outcomes": {"root-0": {"reward": 1, "grade": {"reward": 1}}},
        },
    )
    save(
        source / "attempts/root-0/grade/grade.json",
        {
            "status": "graded",
            "reward": 1,
            "original_task": str((ROOT / "datasets/terminal-bench/task").resolve()),
        },
    )
    assert archive_completed(campaign, archive)[1] == 1
    manifest = json.loads((archive / "task/archive-manifest.json").read_text())
    assert manifest["encoded_checkpoints"][0]["original_sha256"] == expected
    relative = target.relative_to(source)
    shutil.rmtree(source)

    async def check():
        async with checkpoint_archive(
            archive / "task" / relative, expected, scratch=tmp_path / "scratch"
        ) as restored:
            assert restored.read_bytes() == original

    asyncio.run(check())


def test_corrupt_encoded_checkpoint_is_not_published_as_a_success_archive(tmp_path):
    from agentfork.checkpoint_storage import compact_checkpoint, digest

    campaign = tmp_path / "runs"
    source = campaign / "task"
    archive = tmp_path / "archive"
    base = source / "attempts/root-0/checkpoint-artifacts/00001/checkpoint.tar.gz"
    target = source / "attempts/root-0/checkpoint-artifacts/00002/checkpoint.tar.gz"
    base.parent.mkdir(parents=True)
    target.parent.mkdir(parents=True)
    data = os.urandom(2 * 2**20)
    base.write_bytes(data)
    target.write_bytes(data[:-1024] + b"x" * 1024)
    save(
        source / "attempts/root-0/checkpoints.json",
        [{"memory_checkpoint": {"archive": str(target), "archive_sha256": digest(target)}}],
    )
    compact_checkpoint(target, base, case=source, expected_sha=digest(target), reference_sha=digest(base))
    base.write_bytes(b"corruption")
    save(
        source / "result.json",
        {
            "status": "complete",
            "agentfork_resolved": True,
            "outcomes": {"root-0": {"reward": 1, "grade": {"reward": 1}}},
        },
    )
    save(
        source / "attempts/root-0/grade/grade.json",
        {
            "status": "graded",
            "reward": 1,
            "original_task": str((ROOT / "datasets/terminal-bench/task").resolve()),
        },
    )
    with pytest.raises(ValueError, match="checksum changed"):
        archive_completed(campaign, archive)
    assert not (archive / "task").exists()
