import hashlib, io, json, tarfile
from pathlib import Path
from types import SimpleNamespace

from agentfork.supervise_rerun import startup_output_reserve
import agentfork.supervise_rerun as supervise_rerun


def retained(tmp_path):
    folder = tmp_path / "task"
    folder.mkdir()
    archive = folder / "checkpoint.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        entry = tarfile.TarInfo("checkpoint/pages-1.img")
        entry.size = 4096
        tar.addfile(entry, io.BytesIO(b"x" * 4096))
    memory = {"archive": str(archive), "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest()}
    (folder / "recovery.json").write_text(
        json.dumps({"resume_attempts": {"root-0": {"point": {"memory_checkpoint": memory}}}})
    )
    return folder, archive


def test_small_verified_restore_has_a_12_gib_floor(tmp_path):
    retained(tmp_path)
    assert startup_output_reserve(tmp_path, ["task"]) == 12 * 2**30


def test_unknown_large_or_corrupt_exports_keep_the_conservative_reserve(tmp_path):
    folder, archive = retained(tmp_path)
    assert startup_output_reserve(tmp_path, ["task"], max_small_archive_bytes=1) == 24 * 2**30
    archive.write_bytes(b"corrupted")
    assert startup_output_reserve(tmp_path, ["task"]) == 24 * 2**30
    archive.unlink()
    assert startup_output_reserve(tmp_path, ["task"]) == 24 * 2**30


def test_completed_stale_recovery_does_not_inflate_startup_storage(tmp_path):
    folder, archive = retained(tmp_path)
    # A completed case with an invalid recovery is ignored while a valid,
    # small case determines the reserve.
    done = tmp_path / "done"
    done.mkdir()
    (done / "result.json").write_text("{}")
    (done / "recovery.json").write_text("invalid")
    assert startup_output_reserve(tmp_path, ["task", "done"]) == 12 * 2**30
    archive.unlink()
    (folder / "result.json").write_text("{}")
    assert startup_output_reserve(tmp_path, ["task", "done"]) == 24 * 2**30


def test_raw_staging_size_not_compressed_size_sets_the_budget(tmp_path, monkeypatch):
    folder, archive = retained(tmp_path)

    class LargeRawArchive:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def __iter__(self):
            return iter([SimpleNamespace(size=14 * 2**30, isfile=lambda: True)])

    monkeypatch.setattr("agentfork.supervise_rerun.tarfile.open", lambda *args, **kwargs: LargeRawArchive())
    assert startup_output_reserve(tmp_path, ["task"]) >= 24 * 2**30


def launch_profile(tmp_path, monkeypatch):
    controller = tmp_path / "controller"
    source = controller / "agentfork"
    source.mkdir(parents=True)
    hashes = {}
    for name in ["campaign.py", "criu_backend.py", "criu_staging.py"]:
        p = source / name
        p.write_text(name)
        hashes[name] = hashlib.sha256(p.read_bytes()).hexdigest()
    monkeypatch.setattr(supervise_rerun, "ROOT", controller)
    monkeypatch.setattr(supervise_rerun, "PACKAGE", source)
    launch = {
        "argv": ["controller", "--checkpoint-backend", "criu"],
        "compressed_checkpoint_staging": {"backend": "btrfs-zstd-bind", "implementation_sha256": hashes},
    }
    (tmp_path / "launch.json").write_text(json.dumps(launch))
    return source


def test_verified_compressed_staging_budgets_output_exports_instead_of_raw_pages(tmp_path, monkeypatch):
    retained(tmp_path)
    launch_profile(tmp_path, monkeypatch)
    assert startup_output_reserve(tmp_path, ["task"]) == 9 * 2**30


def test_modified_controller_cannot_claim_a_compressed_staging_budget(tmp_path, monkeypatch):
    retained(tmp_path)
    source = launch_profile(tmp_path, monkeypatch)
    (source / "criu_backend.py").write_text("different implementation")
    assert startup_output_reserve(tmp_path, ["task"]) == 12 * 2**30


def test_corrupt_export_still_uses_conservative_reserve_with_verified_profile(tmp_path, monkeypatch):
    _, archive = retained(tmp_path)
    launch_profile(tmp_path, monkeypatch)
    archive.write_bytes(b"corrupt")
    assert startup_output_reserve(tmp_path, ["task"]) == 24 * 2**30


def test_malformed_capacity_profile_fails_closed(tmp_path):
    retained(tmp_path)
    (tmp_path / "launch.json").write_text('{"compressed_checkpoint_staging":true}')
    assert startup_output_reserve(tmp_path, ["task"]) == 12 * 2**30


def test_disk_only_launch_cannot_claim_the_criu_staging_budget(tmp_path, monkeypatch):
    retained(tmp_path)
    launch_profile(tmp_path, monkeypatch)
    p = tmp_path / "launch.json"
    launch = json.loads(p.read_text())
    launch["argv"][-1] = "disk"
    p.write_text(json.dumps(launch))
    assert startup_output_reserve(tmp_path, ["task"]) == 12 * 2**30
