import asyncio, json, os, shutil
from collections import namedtuple

import pytest
import agentfork.checkpoint_payloads as payloads
from agentfork.checkpoint_storage import checkpoint_archive, encoded_info, digest
from test_checkpoint_storage import files, compact


def test_relocation_keeps_open_readers_and_exact_checkpoint_bytes(tmp_path, monkeypatch):
    monkeypatch.setattr(payloads, "PAYLOAD_ROOT", tmp_path / "retained")
    case, reference, target, original = files(tmp_path)
    expected = digest(target)
    compact(case, reference, target)
    value, _, packed = encoded_info(target)
    with packed.open("rb") as reader:
        before = reader.read()
        reader.seek(0)
        moved = payloads.relocate_payload(packed, value["packed_sha256"], reserve_bytes=0)
        assert moved["status"] == "relocated" and packed.is_symlink()
        assert reader.read() == before == packed.read_bytes()

    async def check():
        async with checkpoint_archive(target, expected, scratch=tmp_path / "scratch") as restored:
            assert restored.read_bytes() == original

    asyncio.run(check())


@pytest.mark.parametrize("failure", ["source_hash", "existing_object", "capacity"])
def test_relocation_failures_leave_original_intact(tmp_path, monkeypatch, failure):
    monkeypatch.setattr(payloads, "PAYLOAD_ROOT", tmp_path / "retained")
    source = tmp_path / "packed.zst"
    source.write_bytes(os.urandom(8192))
    before = source.read_bytes()
    expected = digest(source)
    if failure == "capacity":
        monkeypatch.setattr(
            payloads.shutil, "disk_usage", lambda *args: namedtuple("usage", "total used free")(10, 10, 0)
        )
        assert payloads.relocate_payload(source, expected)["status"] == "waiting_host_capacity"
    else:
        if failure == "source_hash":
            expected = "a" * 64
        else:
            objects = payloads.PAYLOAD_ROOT / "objects"
            objects.mkdir(parents=True)
            (objects / (expected + ".zst")).write_bytes(b"corrupt")
        with pytest.raises(ValueError, match="checksum changed"):
            payloads.relocate_payload(source, expected, reserve_bytes=0)
    assert not source.is_symlink() and source.read_bytes() == before


def test_archive_pin_keeps_payload_after_source_and_object_removal(tmp_path, monkeypatch):
    import agentfork.archive_successes as archive

    monkeypatch.setattr(payloads, "PAYLOAD_ROOT", tmp_path / "retained")
    monkeypatch.setattr(archive, "PAYLOAD_ROOT", payloads.PAYLOAD_ROOT)
    # Exercise pin ownership independently of the workstation's current disk
    # pressure; the explicit capacity rejection has its own test above.
    monkeypatch.setattr(
        payloads.shutil,
        "disk_usage",
        lambda *args: namedtuple("usage", "total used free")(64 * 2**30, 16 * 2**30, 48 * 2**30),
    )
    campaign = tmp_path / "runs"
    case, reference, target, original = files(campaign)
    expected = digest(target)
    index = case / "attempts/root-0/checkpoints.json"
    index.write_text(
        json.dumps([{"memory_checkpoint": {"archive": str(target), "archive_sha256": expected}}])
    )
    compact(case, reference, target)
    value, _, packed = encoded_info(target)
    moved = payloads.relocate_case_payloads(case)
    assert len(moved) == 1
    result = {
        "status": "complete",
        "agentfork_resolved": True,
        "outcomes": {"root-0": {"reward": 1, "grade": {"reward": 1}}},
    }
    (case / "result.json").write_text(json.dumps(result))
    grade = case / "attempts/root-0/grade/grade.json"
    grade.parent.mkdir()
    grade.write_text(
        json.dumps(
            {
                "status": "graded",
                "reward": 1,
                "original_task": str((archive.ROOT / "datasets/terminal-bench/case").resolve()),
            }
        )
    )
    archive_root = tmp_path / "archive"
    assert archive.archive_completed(campaign, archive_root)[1] == 1
    manifest = json.loads((archive_root / "case/archive-manifest.json").read_text())
    pin = manifest["external_checkpoint_payloads"][0]
    assert pin["sha256"] == value["packed_sha256"] and pin["bytes"] == value["packed_bytes"]
    relative = target.relative_to(case)
    object_path = packed.resolve()
    shutil.rmtree(case)
    object_path.unlink()

    async def check():
        async with checkpoint_archive(
            archive_root / "case" / relative, expected, scratch=tmp_path / "scratch"
        ) as restored:
            assert restored.read_bytes() == original

    asyncio.run(check())
