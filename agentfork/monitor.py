"""Maintain a local progress JSON while a detached campaign process is alive."""

from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path

from agentfork.metrics import report
from agentfork.rollout import write_json
from agentfork.checkpoint_storage import stored_bytes, sidecar


def read(path: Path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def identity(pid: int):
    try:
        parts = Path(f"/proc/{pid}/stat").read_text().split()
        return None if parts[2] == "Z" else parts[21]
    except OSError:
        return None


def resources(root: Path, campaign: Path):
    config = read(campaign / "configuration.json", {})
    store = Path(config.get("btrfs_store", root / "storage/btrfs"))
    image = Path(config.get("btrfs_backing_file", root / "storage/btrfs.img"))
    points = [point for path in campaign.glob("*/attempts/*/checkpoints.json") for point in read(path, [])]
    stat = image.stat()
    stored = 0
    unknown = 0
    encoded = 0
    for point in points:
        memory = point.get("memory_checkpoint", {})
        if not memory.get("archive"):
            continue
        try:
            stored += stored_bytes(memory["archive"])
            encoded += int(sidecar(memory["archive"]).exists() and not Path(memory["archive"]).is_file())
        except (OSError, ValueError, KeyError):
            unknown += 1
    return {
        "time": time.time(),
        "host_available_bytes": shutil.disk_usage(root).free,
        "btrfs_available_bytes": shutil.disk_usage(store).free,
        "output_available_bytes": shutil.disk_usage(campaign).free,
        "btrfs_backing_logical_bytes": stat.st_size,
        "btrfs_backing_allocated_bytes": stat.st_blocks * 512,
        "checkpoints_created": len(points),
        "checkpoint_capture_seconds": sum(p["capture_seconds"] for p in points),
        "criu_archive_bytes": sum(p.get("memory_checkpoint", {}).get("archive_bytes", 0) for p in points),
        "criu_archive_stored_bytes": stored if not unknown else None,
        "criu_archive_encoded_points": encoded,
        "criu_archive_unmeasured_points": unknown,
        "checkpoint_backend": config.get("checkpoint_backend", "disk"),
        "measurement_note": "Backing allocation includes images, live sandboxes and retained snapshots. CRIU archive_bytes is the original logical gzip size; archive_stored_bytes measures its stored full/reference representations.",
    }


def task_activity(root, configuration):
    active, queued_resume = [], []
    started = configuration.get("controller_started_at", 0)
    for task in configuration["task_names"]:
        folder = root / task
        if (folder / "result.json").exists():
            continue
        if read(folder / "recovery.json", {}).get("state") == "queued":
            queued_resume.append(task)
            continue
        if (folder / "error.json").exists():
            continue
        events = folder / "events.jsonl"
        if not events.exists():
            continue
        lines = events.read_text().splitlines()
        event = json.loads(lines[-1]) if lines else {}
        if event.get("time", events.stat().st_mtime) < started:
            continue
        attempts = []
        for path in sorted((folder / "attempts").glob("*/api-usage.json")):
            requests = read(path, [])
            attempts.append(
                {
                    "attempt": path.parent.name,
                    "completed_api_requests": sum(row.get("complete") is True for row in requests),
                    "failed_provider_attempts": sum(row.get("complete") is False for row in requests),
                    "last_response": max(
                        (
                            row.get("started_at", 0) + row.get("seconds", 0)
                            for row in requests
                            if row.get("complete") is True
                        ),
                        default=None,
                    ),
                }
            )
        active.append({"task": task, "last_event": event, "attempts": attempts})
    return active, queued_resume


def main(args):
    original = identity(args.pid)
    while True:
        alive = original is not None and identity(args.pid) == original
        metrics = report(args.root)
        active, queued_resume = task_activity(args.root, metrics["configuration"])
        state = (
            "running"
            if alive
            else "finished"
            if metrics["completed_tasks"] == metrics["planned_tasks"]
            else "stopped_incomplete"
        )
        write_json(
            args.root / "progress.json",
            {
                "state": state,
                "pid": args.pid,
                "updated_at": time.time(),
                "planned_tasks": metrics["planned_tasks"],
                "completed_tasks": metrics["completed_tasks"],
                "baseline_resolved": metrics["baseline_resolved"],
                "agentfork_resolved": metrics["agentfork_resolved"],
                "infrastructure_error_count": len(metrics["infrastructure_errors"]),
                "active": active,
                "queued_resume": queued_resume,
            },
        )
        # The campaign owns metrics.json. Keep this process's writes separate
        # so both processes never replace the same temporary file.
        write_json(args.root / "live-metrics.json", metrics)
        sample = resources(Path(__file__).resolve().parents[2], args.root)
        write_json(args.root / "resources.json", sample)
        with (args.root / "resource-samples.jsonl").open("a") as stream:
            stream.write(json.dumps(sample) + "\n")
        if not alive:
            return
        time.sleep(15)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--pid", type=int, required=True)
    main(parser.parse_args())
