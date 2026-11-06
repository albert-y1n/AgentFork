"""Compact published, incomplete-case CRIU exports without discarding points."""

from __future__ import annotations
from agentfork.config import PACKAGE
import argparse, fcntl, hashlib, json, os, shutil, time
from pathlib import Path

from agentfork.checkpoint_storage import compact_checkpoint, sidecar, ROOT
from agentfork.checkpoint_payloads import relocate_case_payloads
from agentfork.health_watch import process_info


def read(path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def write(path, value):
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def compatible(campaign):
    configuration = read(campaign / "configuration.json", {})
    if not configuration.get("checkpoint_reference_storage_supported"):
        return False
    for name in ["checkpoint_storage.py", "criu_backend.py"]:
        expected = configuration.get("implementation_sha256", {}).get(name)
        if expected != hashlib.sha256((PACKAGE / name).read_bytes()).hexdigest():
            return False
    return process_info(campaign / "controller.pid", PACKAGE / "campaign.py", campaign)["alive"]


def compact_pass(
    campaign, *, minimum_bytes=64 * 2**20, keep_latest=2, baseline_stride=8, max_files=12, progress=None
):
    encoded = []
    saved = 0
    configuration = read(campaign / "configuration.json", {})
    for task in configuration.get("task_names", []):
        case = campaign / task
        if (case / "result.json").exists():
            continue
        for index in sorted(case.glob("attempts/*/checkpoints.json")):
            points = sorted(read(index, []), key=lambda value: value["step"])
            heavy = [
                p for p in points if p.get("memory_checkpoint", {}).get("archive_bytes", 0) >= minimum_bytes
            ]
            eligible = (
                {p["step"] for p in points[:-keep_latest]} if keep_latest else {p["step"] for p in points}
            )
            for position, point in enumerate(heavy):
                memory = point["memory_checkpoint"]
                archive = Path(memory["archive"])
                if point["step"] not in eligible or sidecar(archive).exists() or not archive.is_file():
                    continue
                if position % baseline_stride == 0:
                    continue
                candidates = [
                    p
                    for p in heavy[:position]
                    if Path(p["memory_checkpoint"]["archive"]).is_file()
                    and not sidecar(p["memory_checkpoint"]["archive"]).exists()
                    and p["memory_checkpoint"]["archive_bytes"] < 2**31
                ]
                if not candidates:
                    continue
                reference = candidates[-1]["memory_checkpoint"]
                # A major working-set change establishes a new full reference.
                if reference["archive_bytes"] < memory["archive_bytes"] * 0.7:
                    continue
                if progress:
                    progress({"phase": "encoding", "task": task, "step": point["step"]})
                result = compact_checkpoint(
                    archive,
                    Path(reference["archive"]),
                    case=case,
                    expected_sha=memory["archive_sha256"],
                    reference_sha=reference["archive_sha256"],
                )
                if progress:
                    progress(
                        {"phase": "encoded", "task": task, "step": point["step"], "result": result["status"]}
                    )
                if result["status"] == "encoded":
                    row = {"task": task, "attempt": index.parent.name, "step": point["step"], **result}
                    encoded.append(row)
                    saved += result["saved_allocated_bytes"]
                    print(json.dumps(row), flush=True)
                    if len(encoded) >= max_files:
                        return encoded, saved
    return encoded, saved


def relocate_pass(campaign, *, progress=None, target_free=40 * 2**30, max_files=64):
    """Balance immutable payloads across disks, preserving host reserve."""
    if campaign.stat().st_dev == ROOT.stat().st_dev:
        return []
    moved = []
    for task in read(campaign / "configuration.json", {}).get("task_names", []):
        if shutil.disk_usage(campaign).free >= target_free:
            break
        moved.extend(
            relocate_case_payloads(campaign / task, progress=progress, max_files=max_files - len(moved))
        )
        if len(moved) >= max_files:
            break
    return moved


def main(args):
    campaign = args.campaign.resolve()
    offline_lock = None
    if args.offline:
        if not args.once:
            raise ValueError("Offline compaction must be one-shot")
        controller = process_info(campaign / "controller.pid", PACKAGE / "campaign.py", campaign)
        supervisor = process_info(campaign / "supervisor.pid", PACKAGE / "supervise_rerun.py", campaign)
        if controller["alive"] or not supervisor["paused"]:
            raise RuntimeError("Offline mode requires a stopped controller and a verified paused supervisor")
        offline_lock = (campaign / "agentfork.campaign.lock").open("a")
        fcntl.flock(offline_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if not args.once:
        (campaign / "compactor.pid").write_text(str(os.getpid()) + "\n")
    status_path = campaign / ("compaction-once-status.json" if args.once else "compaction-status.json")
    while True:
        started = time.time()

        def progress(current):
            write(
                status_path,
                {"state": "working", "updated_at": time.time(), "pid": os.getpid(), "current": current},
            )

        try:
            if not args.offline and not compatible(campaign):
                status = {
                    "state": "waiting_compatible_controller",
                    "updated_at": time.time(),
                    "pid": os.getpid(),
                }
            else:
                moved = relocate_pass(campaign, progress=progress)
                encoded, saved = compact_pass(campaign, max_files=args.max_files, progress=progress)
                moved.extend(relocate_pass(campaign, progress=progress))
                status = {
                    "state": "complete" if args.once else "watching",
                    "updated_at": time.time(),
                    "pid": os.getpid(),
                    "encoded_points": len(encoded),
                    "saved_allocated_bytes": saved,
                    "seconds": time.time() - started,
                    "relocated_payloads": len(moved),
                    "relocated_bytes": sum(row["bytes"] for row in moved),
                }
        except Exception as error:
            status = {
                "state": "needs_attention",
                "updated_at": time.time(),
                "pid": os.getpid(),
                "error": repr(error),
            }
            print(json.dumps(status), flush=True)
            if args.once:
                raise
        finally:
            if "status" in locals():
                write(status_path, status)
        if args.once:
            return
        time.sleep(args.interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign", type=Path)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--interval", type=float, default=30)
    parser.add_argument("--max-files", type=int, default=12)
    main(parser.parse_args())
