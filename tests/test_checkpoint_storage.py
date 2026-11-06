import asyncio
import json
import os
from pathlib import Path
import threading

import pytest

import agentfork.checkpoint_storage as storage


def files(tmp_path):
    case = tmp_path / "case"
    group = case / "attempts/root-0/checkpoint-artifacts"
    reference = group / "00001/checkpoint.tar.gz"
    target = group / "00002/checkpoint.tar.gz"
    reference.parent.mkdir(parents=True)
    target.parent.mkdir(parents=True)
    original = os.urandom(2 * 2**20)
    reference.write_bytes(original)
    changed = original[: 2**20] + b"x" * 1024 + original[2**20 + 1024 :]
    target.write_bytes(changed)
    return case, reference, target, changed


def compact(case, reference, target):
    return storage.compact_checkpoint(
        target,
        reference,
        case=case,
        expected_sha=storage.digest(target),
        reference_sha=storage.digest(reference),
    )


def test_reference_storage_reconstructs_identical_bytes_and_cleans_scratch(tmp_path):
    case, reference, target, original = files(tmp_path)
    expected = storage.digest(target)
    result = compact(case, reference, target)
    assert result["status"] == "encoded" and result["byte_exact_verified"]
    assert not target.exists() and reference.is_file()
    assert storage.stored_bytes(target) < len(original) / 4

    async def check():
        async with storage.checkpoint_archive(target, expected, scratch=tmp_path / "scratch") as materialized:
            assert materialized.read_bytes() == original
            temporary = materialized
        assert not temporary.exists()

    asyncio.run(check())


def test_full_archive_compatibility_and_hash_guard(tmp_path):
    case, reference, target, original = files(tmp_path)

    async def check():
        async with storage.checkpoint_archive(target, storage.digest(target)) as materialized:
            assert materialized == target and materialized.read_bytes() == original
        with pytest.raises(ValueError, match="checksum changed"):
            async with storage.checkpoint_archive(target, "wrong"):
                pytest.fail("Corrupt original was accepted")

    asyncio.run(check())


def test_full_references_remain_pinned_and_never_form_a_chain(tmp_path):
    case, reference, target, _ = files(tmp_path)
    compact(case, reference, target)
    earlier = reference.parent.parent / "00000/checkpoint.tar.gz"
    earlier.parent.mkdir()
    earlier.write_bytes(reference.read_bytes())
    result = storage.compact_checkpoint(
        reference,
        earlier,
        case=case,
        expected_sha=storage.digest(reference),
        reference_sha=storage.digest(earlier),
    )
    assert result["status"] == "pinned_full_reference"
    assert reference.is_file() and not storage.sidecar(reference).exists()


@pytest.mark.parametrize("damage", ["packed", "reference", "identity", "path"])
def test_encoded_archive_rejects_corruption_and_wrong_dependencies(tmp_path, damage):
    case, reference, target, _ = files(tmp_path)
    expected = storage.digest(target)
    compact(case, reference, target)
    value, _, packed = storage.encoded_info(target)
    if damage == "packed":
        packed.write_bytes(b"corrupt")
    elif damage == "reference":
        reference.write_bytes(b"corrupt")
    elif damage == "identity":
        expected = "another-point"
    else:
        value["reference"] = "../../../../outside"
        storage.sidecar(target).write_text(json.dumps(value))

    async def check():
        with pytest.raises(ValueError):
            async with storage.checkpoint_archive(target, expected):
                pytest.fail("Damaged checkpoint was accepted")

    asyncio.run(check())


def test_no_saving_or_completed_case_keeps_the_original(tmp_path):
    case, reference, target, _ = files(tmp_path)
    target.write_bytes(os.urandom(2 * 2**20))
    before = target.read_bytes()
    result = compact(case, reference, target)
    assert result["status"] == "insufficient_saving" and target.read_bytes() == before
    assert not storage.sidecar(target).exists() and not list(target.parent.glob("*.zst"))
    (case / "result.json").write_text("{}")
    assert compact(case, reference, target)["status"] == "completed_case_unchanged"
    assert target.read_bytes() == before


def test_an_existing_reader_prevents_publication_until_its_lease_ends(tmp_path, monkeypatch):
    case, reference, target, original = files(tmp_path)
    expected = storage.digest(target)
    waiting = threading.Event()
    real = storage.fcntl.flock

    def observed(descriptor, operation):
        if operation == storage.fcntl.LOCK_EX and Path(descriptor.name) == storage.lease_path(target):
            waiting.set()
        return real(descriptor, operation)

    monkeypatch.setattr(storage.fcntl, "flock", observed)

    async def check():
        async with storage.checkpoint_archive(target, expected) as opened:
            worker = asyncio.create_task(asyncio.to_thread(compact, case, reference, target))
            assert await asyncio.to_thread(waiting.wait, 10)
            assert not worker.done() and opened.read_bytes() == original
        result = await worker
        assert result["status"] == "encoded"
        async with storage.checkpoint_archive(target, expected) as decoded:
            assert decoded.read_bytes() == original

    asyncio.run(check())


def test_hardlinked_archived_originals_are_not_rewritten(tmp_path):
    case, reference, target, original = files(tmp_path)
    os.link(target, target.parent / "archived-copy")
    assert compact(case, reference, target)["status"] == "ineligible_source"
    assert target.read_bytes() == original and not storage.sidecar(target).exists()
