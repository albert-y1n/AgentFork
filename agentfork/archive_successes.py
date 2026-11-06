"""Preserve completed, officially graded successes without copying file data."""

from __future__ import annotations
from agentfork.config import task_root
import argparse, fcntl, hashlib, json, os, shutil, time
from pathlib import Path
from agentfork.checkpoint_storage import case_lock, encoded_info, SUFFIX
from agentfork.checkpoint_payloads import pin_for_archive, PAYLOAD_ROOT

from agentfork.config import ROOT


def read(path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def write(path, value):
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def archive_case(source, archive_root):
    with case_lock(source):
        return _archive_case(source, archive_root)


def _archive_case(source, archive_root):
    result = read(source / "result.json", {})
    if result.get("status") != "complete" or not result.get("agentfork_resolved"):
        return None
    task = source.name
    destination = archive_root / task
    if destination.exists():
        manifest = read(destination / "archive-manifest.json")
        if not manifest or manifest.get("task") != task or manifest.get("reward") != 1:
            raise RuntimeError("Existing archive lacks a valid manifest: " + str(destination))
        recorded = manifest.get("source_result_sha256") or next(
            (row["sha256"] for row in manifest.get("files", []) if row["path"] == "result.json"), None
        )
        if recorded != digest(source / "result.json") or recorded != digest(destination / "result.json"):
            raise RuntimeError("Existing archive and source result differ: " + str(destination))
        return manifest
    successful = []
    proofs = []
    for name, outcome in result.get("outcomes", {}).items():
        if outcome.get("reward") != 1:
            continue
        grade_path = source / "attempts" / name / "grade/grade.json"
        grade = read(grade_path, {})
        if grade.get("status") != "graded" or grade.get("reward") != 1:
            raise RuntimeError("Success has no matching official grade: " + str(grade_path))
        if grade.get("original_task") != str((task_root(ROOT) / task).resolve()):
            raise RuntimeError("Official grade names another task: " + str(grade_path))
        if outcome.get("grade", {}).get("reward") != 1:
            raise RuntimeError("Outcome grade disagrees with success: " + str(source))
        successful.append(name)
        proofs.append({"path": str(grade_path.relative_to(source)), "sha256": digest(grade_path)})
    if not successful:
        raise RuntimeError("Completed success has no verified successful attempt: " + str(source))
    result_hash = digest(source / "result.json")
    stage = archive_root / (".partial-" + task + "-" + str(time.time_ns()))
    # All mutations below are in a fresh archive directory. The source is never
    # edited, chmodded, moved or removed, including on verification failure.
    shutil.copytree(source, stage, copy_function=os.link, symlinks=True)
    files = []
    symlinks = []
    encoded = []
    external_payloads = []
    for path in sorted(stage.rglob("*")):
        relative = path.relative_to(stage)
        original = source / relative
        if path.is_symlink():
            assert original.is_symlink() and os.readlink(path) == os.readlink(original)
            pin = pin_for_archive(original, archive_root, task, relative)
            if pin:
                # The archive owns an independent hardlink. Its payload stays
                # available even if the source and canonical object are removed.
                path.unlink()
                path.symlink_to(pin["target"])
                external_payloads.append(pin)
            symlinks.append({"path": str(relative), "target": os.readlink(path)})
        elif path.is_file():
            assert os.path.samestat(path.stat(), original.stat()), relative
            files.append({"path": str(relative), "bytes": path.stat().st_size, "sha256": digest(path)})
    # Relative reference paths remain valid after archiving, including after
    # the original source directory is removed. Verify both stored payloads.
    expected = {
        str(Path(p["memory_checkpoint"]["archive"]).relative_to(source)): p["memory_checkpoint"][
            "archive_sha256"
        ]
        for points in source.glob("attempts/*/checkpoints.json")
        for p in read(points, [])
        if p.get("memory_checkpoint") and Path(p["memory_checkpoint"]["archive"]).is_relative_to(source)
    }
    for metadata in stage.glob("attempts/*/checkpoint-artifacts/*/checkpoint.tar.gz" + SUFFIX):
        virtual = metadata.with_name(metadata.name[: -len(SUFFIX)])
        relative = str(virtual.relative_to(stage))
        if relative not in expected:
            raise RuntimeError("Encoded archive has no matching point metadata: " + relative)
        value, reference, packed = encoded_info(virtual, expected_sha=expected[relative])
        encoded.append(
            {
                "path": relative,
                "original_sha256": value["original_sha256"],
                "original_bytes": value["original_bytes"],
                "packed_bytes": value["packed_bytes"],
                "reference": str(reference.relative_to(stage.resolve())),
                "byte_exact_verified": value["byte_exact_verified"],
            }
        )
    if digest(source / "result.json") != result_hash:
        raise RuntimeError("Task result changed while archiving: " + str(source))
    manifest = {
        "task": task,
        "source": str(source.resolve()),
        "archive": str(destination.resolve()),
        "created_at": time.time(),
        "method": "same-filesystem hardlinks; every regular file verified to share the source inode and recorded with SHA256",
        "verified_file_count": len(files),
        "logical_bytes": sum(row["bytes"] for row in files) + sum(row["bytes"] for row in external_payloads),
        "verified_successful_attempts": successful,
        "reward": 1,
        "source_result_sha256": result_hash,
        "grade_proofs": proofs,
        "files": files,
        "symlinks": symlinks,
        "encoded_checkpoints": encoded,
        "external_checkpoint_payloads": external_payloads,
        "checkpoint_payload_store": str(PAYLOAD_ROOT.resolve()),
        "preservation_scope": "Archive directory plus listed external payload pins; retain both when migrating or backing up.",
    }
    write(stage / "archive-manifest.json", manifest)
    stage.rename(destination)
    return manifest


def archive_completed(campaign, archive_root):
    archive_root.mkdir(parents=True, exist_ok=True)
    with (archive_root / ".archive.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        index = read(archive_root / "successful-trajectories.json", {"tasks": []})
        tasks = {row["task"]: row for row in index["tasks"]}
        new = []
        for result in sorted(campaign.glob("*/result.json")):
            already = (archive_root / result.parent.name / "archive-manifest.json").exists()
            manifest = archive_case(result.parent, archive_root)
            if manifest:
                tasks[manifest["task"]] = {
                    key: manifest[key]
                    for key in [
                        "task",
                        "source",
                        "archive",
                        "created_at",
                        "verified_file_count",
                        "logical_bytes",
                        "verified_successful_attempts",
                        "reward",
                    ]
                }
                if not already:
                    new.append(tasks[manifest["task"]])
        write(archive_root / "successful-trajectories.json", {"tasks": list(tasks.values())})
        return new, len(tasks)


def main(args):
    os.umask(0o077)
    args.campaign = args.campaign.resolve()
    args.archive_root = args.archive_root.resolve()
    if not args.once:
        (args.campaign / "archive.pid").write_text(str(os.getpid()) + "\n")
    status_path = args.campaign / ("archive-once-status.json" if args.once else "archive-status.json")
    while True:
        started = time.time()
        try:
            new, count = archive_completed(args.campaign, args.archive_root)
            status = {
                "state": "complete" if args.once else "watching",
                "pid": os.getpid(),
                "updated_at": time.time(),
                "archived_tasks": count,
                "new_archives": new,
                "seconds": time.time() - started,
            }
            if new:
                print(json.dumps(status), flush=True)
        except Exception as error:
            status = {
                "state": "needs_attention",
                "pid": os.getpid(),
                "updated_at": time.time(),
                "error": f"{type(error).__name__}: {error}",
            }
            print(json.dumps(status), flush=True)
            if args.once:
                raise
        finally:
            if "status" in locals():
                write(status_path, status)
        if args.once:
            return
        time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("campaign", type=Path)
    parser.add_argument("archive_root", type=Path)
    parser.add_argument("--once", action="store_true")
    main(parser.parse_args())
