"""Observe live campaign failures, including errors inside unfinished attempts.

This watcher never restarts a live controller or changes benchmark results.
The existing supervisor owns campaign retries; a missing progress monitor can
be replaced after its process has been confirmed absent.
"""

from __future__ import annotations
import sys

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import time
from collections import Counter
from pathlib import Path

from agentfork.config import ROOT


def read(path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def write(path, data):
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def process_info(pid_file, script, campaign, *, proc=Path("/proc")):
    """Check a live process and its command, rather than trusting a PID file."""
    try:
        pid = int(pid_file.read_text().strip())
        folder = proc / str(pid)
        fields = (folder / "stat").read_text().rpartition(") ")[2].split()
        args = (folder / "cmdline").read_bytes().decode().split("\0")
        matches = str(script.resolve()) in args and str(campaign.resolve()) in args
        return {
            "pid": pid,
            "alive": matches and fields[0] not in ("Z", "X"),
            "paused": matches and fields[0] in ("T", "t"),
            "start_ticks": fields[19],
            "command_matches": matches,
        }
    except (OSError, ValueError, IndexError):
        return {"alive": False, "paused": False}


def issue(kind, relative, **extra):
    data = {"kind": kind, "path": str(relative), **extra}
    data["id"] = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
    return data


def inspect(campaign, *, now=None, process_reader=process_info):
    now = time.time() if now is None else now
    config = read(campaign / "configuration.json", {})
    revision_started = config.get("controller_started_at", (campaign / "configuration.json").stat().st_mtime)
    progress = read(campaign / "progress.json", {})
    roles = {
        role: process_reader(campaign / f"{role}.pid", Path(__file__).with_name(script), campaign)
        for role, script in {
            "controller": "campaign.py",
            "monitor": "monitor.py",
            "supervisor": "supervise_rerun.py",
        }.items()
    }
    problems, active, completed, passed = [], [], 0, 0
    reviewer_activity = []
    archive = read(campaign / "archive-status.json")
    if archive is not None:
        archive = {
            **archive,
            "process": process_reader(
                campaign / "archive.pid", Path(__file__).with_name("archive_successes.py"), campaign
            ),
        }
        if not archive["process"]["alive"]:
            problems.append(issue("trajectory_archiver_absent", "archive.pid"))
        if archive.get("state") == "needs_attention":
            problems.append(
                issue("trajectory_archive_error", "archive-status.json", error=archive.get("error"))
            )
        elif now - archive.get("updated_at", 0) > 180:
            problems.append(issue("trajectory_archive_stale", "archive-status.json"))
    compaction = read(campaign / "compaction-status.json")
    if compaction is not None:
        compaction = {
            **compaction,
            "process": process_reader(
                campaign / "compactor.pid", Path(__file__).with_name("compact_checkpoints.py"), campaign
            ),
        }
        if not compaction["process"]["alive"]:
            problems.append(issue("checkpoint_compactor_absent", "compactor.pid"))
        if compaction.get("state") == "needs_attention":
            problems.append(
                issue("checkpoint_compaction_error", "compaction-status.json", error=compaction.get("error"))
            )
        elif now - compaction.get("updated_at", 0) > 180:
            problems.append(issue("checkpoint_compaction_stale", "compaction-status.json"))
    checkpoints, validated = Counter(), Counter()
    failures, current_failures = Counter(), Counter()
    for name in config.get("task_names", []):
        folder = campaign / name
        result = read(folder / "result.json")
        error = read(folder / "error.json")
        if result:
            completed += 1
            passed += bool(result.get("agentfork_resolved"))
        if error:
            failures["task_error"] += 1
            problems.append(
                {
                    **issue("task_error", f"{name}/error.json"),
                    "occurred_at": (folder / "error.json").stat().st_mtime,
                }
            )
        if not result and not error and roles["controller"]["alive"]:
            for path in folder.glob("review/round-*/*.inflight.json"):
                value = read(path, {})
                if value.get("state") != "receiving" or value.get("started_at", 0) < revision_started:
                    continue
                match = re.match(r"analysis-(.+)-try-", path.name)
                round_number = path.parent.name.removeprefix("round-")
                reviewer_activity.append(
                    {
                        "task": name,
                        "round": int(round_number) if round_number.isdigit() else None,
                        "stage": "analysis" if match else "plan",
                        "attempt": match[1] if match else None,
                        "started_at": value["started_at"],
                        "updated_at": value.get("updated_at"),
                        "last_byte_at": value.get("last_byte_at"),
                        "bytes_received": value.get("bytes_received", 0),
                    }
                )
        if not result and not error and (folder / "events.jsonl").exists():
            latest = (folder / "events.jsonl").stat().st_mtime
            for pattern in (
                "review/**/*.json",
                "attempts/*/api-usage.json",
                "attempts/*/checkpoints.json",
                "attempts/*/requests/*.json.gz",
            ):
                for path in folder.glob(pattern):
                    try:
                        latest = max(latest, path.stat().st_mtime)
                    except FileNotFoundError:
                        pass
            if latest >= revision_started:
                active.append(
                    {"task": name, "last_activity_at": latest, "idle_seconds": max(0, now - latest)}
                )
                if roles["controller"]["alive"] and now - latest > 1800:
                    problems.append(issue("activity_needs_inspection", name))
        for path in folder.glob("attempts/*/checkpoints.json"):
            for point in read(path, []) or []:
                memory = point.get("memory_checkpoint", {})
                revision = memory.get("backend_revision", "unknown")
                checkpoints[revision] += 1
                if memory.get("validation", {}).get("passed"):
                    validated[revision] += 1
        for pattern, kind in [
            ("attempts/*/checkpoint-artifacts/*/capture-error.json", "checkpoint_capture_error"),
            ("attempts/*/gateway-error.json", "context_or_gateway_error"),
            ("attempts/*/outcome.json", "attempt_infrastructure_error"),
        ]:
            for path in folder.glob(pattern):
                value = read(path)
                if value is None or (
                    kind == "attempt_infrastructure_error" and value.get("status") != "infrastructure_error"
                ):
                    continue
                failures[kind] += 1
                restored = str(value.get("fallback", "")).startswith("restored prior coherent environment")
                problems.append(
                    {
                        **issue(kind, path.relative_to(campaign)),
                        "resolved": restored,
                        "historical": bool(result),
                        "occurred_at": path.stat().st_mtime,
                    }
                )
        # A native transcript boundary can fail before capture-error.json is
        # written. Its only durable record may be actor-restarts or outcome.
        for attempt in (folder / "attempts").glob("*"):
            represented = {
                int(path.parent.name)
                for path in attempt.glob("checkpoint-artifacts/*/capture-error.json")
                if path.parent.name.isdigit()
            }
            seen_steps = set()
            for filename in ["actor-restarts.json", "outcome.json"]:
                source = attempt / filename
                for error in read(source, {}).get("checkpoint_failures", []):
                    match = re.match(r"^step\s+(\d+):", str(error))
                    if not match:
                        continue
                    step = int(match[1])
                    if step in represented or step in seen_steps:
                        continue
                    seen_steps.add(step)
                    failures["checkpoint_capture_error"] += 1
                    path = attempt / "checkpoint-artifacts" / f"{step:05d}"
                    problems.append(
                        {
                            **issue("checkpoint_capture_error", path.relative_to(campaign)),
                            "source": str(source.relative_to(campaign)),
                            "stage": "native_results_not_durable"
                            if "matching native results not durable" in str(error)
                            else "capture",
                            "historical": bool(result),
                            "resolved": False,
                            # Old string lists have no per-error timestamps. Do
                            # not relabel them as failures in the current revision.
                            "occurred_at": 0,
                            "timestamp_unknown": True,
                        }
                    )
        for path in [*folder.glob("attempts/*/api-usage.json"), folder / "review-usage.json"]:
            rows = read(path, []) or []
            for index, row in enumerate(rows):
                if row.get("complete") is False:
                    cancelled = row.get("error") == "CancelledError"
                    kind = "request_cancelled" if cancelled else "provider_failed_attempt"
                    failures[kind] += 1
                    recovered = any(
                        later.get("complete") and later.get("request") == row.get("request")
                        for later in rows[index + 1 :]
                    )
                    started = row.get("started_at")
                    ended = (
                        started + max(0, row.get("seconds", 0)) if isinstance(started, (int, float)) else None
                    )
                    # Cancellation can also mean a live client disconnected or
                    # timed out. Only cancellations completed before this
                    # controller started belong to its historical activity.
                    old_cancellation = cancelled and ended is not None and ended < revision_started
                    problems.append(
                        {
                            **issue(kind, path.relative_to(campaign), row=index, error_type=row.get("error")),
                            "resolved": bool(recovered),
                            "historical": bool(result or old_cancellation),
                            "occurred_at": ended if ended is not None else path.stat().st_mtime,
                            **(
                                {"cancellation_reason": row["cancellation_reason"]}
                                if row.get("cancellation_reason")
                                else {}
                            ),
                        }
                    )
        for path in folder.glob("review/round-*/*.response-*.json"):
            value = read(path, {})
            choices = value.get("choices") or []
            if not choices:
                continue
            choice = choices[0]
            if choice.get("finish_reason") not in ("length", "max_tokens"):
                continue
            failures["review_output_exhausted"] += 1
            analysis = re.match(r"analysis-(.+)-try-\d+(?:-resume-\d+)?\.response-", path.name)
            recovered = (
                (folder / "attempts" / analysis[1] / "analysis.json").exists()
                if analysis
                else (path.parent / "plan.json").exists()
            )
            problems.append(
                {
                    **issue("review_output_exhausted", path.relative_to(campaign)),
                    "resolved": recovered,
                    "historical": bool(result),
                    "occurred_at": path.stat().st_mtime,
                }
            )
    current_failures.update(
        item["kind"]
        for item in problems
        if item["kind"] in failures and item.get("occurred_at", 0) >= revision_started
    )
    resolutions = read(campaign / "health-resolutions.json", {})
    for problem in problems:
        resolution = resolutions.get(problem["id"], {})
        resolved_through = resolution.get("resolved_through")
        if resolution.get("state") == "resolved" and (
            resolved_through is None or problem.get("occurred_at", 0) <= resolved_through
        ):
            problem.update(resolved=True, resolution=resolution.get("reason"))
    all_done = bool(config.get("task_names")) and completed == len(config["task_names"])
    supervisor = read(campaign / "supervisor.json", {})
    if not all_done:
        if not roles["controller"]["alive"]:
            problems.append(issue("controller_absent", "controller.pid"))
        for role in ("monitor", "supervisor"):
            if not roles[role]["alive"]:
                problems.append(issue(f"{role}_absent", f"{role}.pid"))
        if any(role.get("paused") for role in roles.values()):
            problems.append(issue("process_paused", "controller.pid"))
        if roles["controller"]["alive"] and now - progress.get("updated_at", 0) > 90:
            problems.append(issue("progress_stale", "progress.json"))
    storage = {}
    for label, location, reserve in [
        ("host", ROOT, 8 * 2**30),
        ("output", campaign, 12 * 2**30),
        ("btrfs", Path(config.get("btrfs_store", ROOT / "storage/btrfs")), 2 * 2**30),
    ]:
        try:
            free = shutil.disk_usage(location).free
            storage[label] = {"free_bytes": free, "reserve_bytes": reserve}
            if free < reserve and not all_done:
                problems.append(issue("storage_reserve", label))
        except OSError:
            problems.append(issue("storage_unavailable", label))
    waiting_storage = supervisor.get("state") == "waiting_storage" and roles["supervisor"]["alive"]
    waiting_provider = supervisor.get("state") == "waiting_provider" and roles["supervisor"]["alive"]
    archive_attention = any(
        item["kind"].startswith("trajectory_") and not item.get("resolved") and not item.get("historical")
        for item in problems
    )
    return {
        "updated_at": now,
        "watcher_pid": os.getpid(),
        "controller_revision": config.get("controller_revision"),
        "state": "needs_attention"
        if all_done and archive_attention
        else "finished"
        if all_done
        else "waiting_provider"
        if waiting_provider
        else "waiting_storage"
        if waiting_storage
        else "needs_attention"
        if any(not item.get("resolved") and not item.get("historical") for item in problems)
        else "warnings"
        if problems
        else "healthy",
        "processes": roles,
        "planned_tasks": len(config.get("task_names", [])),
        "completed_tasks": completed,
        "agentfork_resolved": passed,
        "active": active,
        "reviewer_activity": reviewer_activity,
        "trajectory_archive": archive,
        "checkpoint_compaction": compaction,
        "checkpoints_by_backend_revision": dict(checkpoints),
        "validated_checkpoints_by_backend_revision": dict(validated),
        "failure_counts": dict(failures),
        "current_revision_failure_counts": dict(current_failures),
        "untimed_failure_counts": dict(
            Counter(item["kind"] for item in problems if item.get("timestamp_unknown"))
        ),
        "issues": problems,
        "storage": storage,
        "policy": "Observation only for controllers; no timeout-based restart. Missing progress monitors may be replaced. Failed provider attempts remain visible after successful retries.",
    }


def replace_missing_monitor(campaign, health):
    if not health["processes"]["controller"]["alive"] or health["processes"]["monitor"]["alive"]:
        return None
    # Recheck immediately before starting; do not duplicate a live monitor.
    if process_info(campaign / "agentfork.monitor.pid", Path(__file__).with_name("monitor.py"), campaign)[
        "alive"
    ]:
        return None
    controller = process_info(campaign / "controller.pid", Path(__file__).with_name("campaign.py"), campaign)
    if not controller["alive"] or controller.get("start_ticks") != health["processes"]["controller"].get(
        "start_ticks"
    ):
        return None
    # A missing/stale PID file alone does not prove a monitor has exited.
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            args = (entry / "cmdline").read_bytes().decode().split("\0")
            fields = (entry / "stat").read_text().rpartition(") ")[2].split()
            if (
                str(Path(__file__).with_name("monitor.py")) in args
                and str(campaign) in args
                and fields[0] not in ("Z", "X")
            ):
                (campaign / "agentfork.monitor.pid").write_text(entry.name + "\n")
                return {"action": "rediscovered_live_monitor", "pid": int(entry.name), "time": time.time()}
        except (OSError, IndexError, UnicodeError):
            pass
    with (campaign / "agentfork.monitor.log").open("ab") as log:
        child = subprocess.Popen(
            [
                str(Path(sys.executable)),
                "-u",
                str(Path(__file__).with_name("monitor.py")),
                str(campaign),
                "--pid",
                str(health["processes"]["controller"]["pid"]),
            ],
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    (campaign / "agentfork.monitor.pid").write_text(str(child.pid) + "\n")
    return {"action": "replaced_missing_monitor", "pid": child.pid, "time": time.time()}


def main(args):
    os.umask(0o077)
    campaign = args.output.resolve()
    lock = (campaign / "health-watch.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    seen = set(read(campaign / "health-watch-state.json", {}).get("seen", []))
    missing_since, previous_controller = None, None
    while True:
        health = inspect(campaign)
        fresh = [item for item in health["issues"] if item["id"] not in seen]
        controller = health["processes"]["controller"]
        controller_key = (controller.get("pid"), controller.get("start_ticks"))
        if (
            controller_key != previous_controller
            or health["processes"]["monitor"]["alive"]
            or not controller["alive"]
        ):
            missing_since = None
        previous_controller = controller_key
        if controller["alive"] and not health["processes"]["monitor"]["alive"]:
            missing_since = time.monotonic() if missing_since is None else missing_since
        # Let the supervisor finish publishing a replacement controller and its
        # monitor before doing any repair of our own.
        recovery = (
            replace_missing_monitor(campaign, health)
            if not args.once and missing_since is not None and time.monotonic() - missing_since >= 30
            else None
        )
        if recovery:
            missing_since = None
        if recovery:
            with (campaign / "health-actions.jsonl").open("a") as stream:
                stream.write(json.dumps(recovery) + "\n")
        if fresh:
            with (campaign / "health-alerts.jsonl").open("a") as stream:
                for item in fresh:
                    stream.write(json.dumps({"detected_at": time.time(), **item}) + "\n")
            seen.update(item["id"] for item in fresh)
            write(campaign / "health-watch-state.json", {"seen": sorted(seen)})
        write(campaign / "health.json", health)
        print(
            json.dumps(
                {
                    "time": health["updated_at"],
                    "state": health["state"],
                    "new_issues": fresh,
                    "action": recovery,
                }
            ),
            flush=True,
        )
        if args.once or health["state"] == "finished":
            return
        time.sleep(args.interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--interval", type=float, default=15)
    parser.add_argument("--once", action="store_true")
    main(parser.parse_args())
