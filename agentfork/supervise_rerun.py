"""Retry infrastructure failures after a AgentFork-only campaign pass finishes."""

from __future__ import annotations
from agentfork.config import task_root
from agentfork.config import PACKAGE
import sys
import argparse, fcntl, hashlib, json, os, shutil, subprocess, tarfile, time
from pathlib import Path
from agentfork.config import load_provider
from agentfork.rollout import ROOT, write_json


def identity(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().split()
        return None if fields[2] == "Z" else fields[21]
    except OSError:
        return None


def current_pass(root):
    """Resume the persisted pass sequence without overwriting prior audit files."""
    previous = root / "supervisor.json"
    indexes = [json.loads(previous.read_text()).get("campaign_pass", 1)] if previous.exists() else [1]
    indexes.extend(
        int(path.stem) for path in (root / "controller-passes").glob("*.json") if path.stem.isdigit()
    )
    return max(indexes)


def verified_compressed_staging_launch(root):
    """Accept a storage profile only for the exact next controller implementation."""
    try:
        launch = json.loads((root / "launch.json").read_text())
        profile = launch["compressed_checkpoint_staging"]
        if not isinstance(profile, dict) or profile.get("backend") != "btrfs-zstd-bind":
            return False
        argv = launch.get("argv", [])
        if "--checkpoint-backend" not in argv or argv[argv.index("--checkpoint-backend") + 1] != "criu":
            return False
        return all(
            profile["implementation_sha256"][name]
            == hashlib.sha256((PACKAGE / name).read_bytes()).hexdigest()
            for name in ["campaign.py", "criu_backend.py", "criu_staging.py"]
        )
    except (OSError, ValueError, KeyError, TypeError, IndexError):
        return False


def startup_output_reserve(root, names, *, max_small_archive_bytes=512 * 2**20):
    """Budget exact raw restore staging for small retained checkpoints.

    Unknown or large exports retain the conservative 24 GiB requirement. CRIU
    restores run serially; a verified small export reserves its raw tar member
    sizes plus 2 GiB temporary headroom above the controller's 8 GiB floor.
    """
    peak = 0
    compressed_peak = 0
    found = False
    try:
        for name in names:
            folder = root / name
            if (folder / "result.json").exists():
                continue
            recovery = folder / "recovery.json"
            if not recovery.exists():
                continue
            for attempt in json.loads(recovery.read_text()).get("resume_attempts", {}).values():
                memory = attempt["point"]["memory_checkpoint"]
                archive = Path(memory["archive"])
                if not archive.is_file() or archive.stat().st_size > max_small_archive_bytes:
                    return 24 * 2**30
                with archive.open("rb") as stream:
                    if hashlib.file_digest(stream, "sha256").hexdigest() != memory["archive_sha256"]:
                        return 24 * 2**30
                with tarfile.open(archive, "r:gz") as tar:
                    raw = sum(member.size for member in tar if member.isfile())
                peak = max(peak, raw + archive.stat().st_size)
                found = True
                compressed_peak = max(compressed_peak, archive.stat().st_size)
    except (OSError, ValueError, KeyError, tarfile.TarError, EOFError):
        return 24 * 2**30
    if found and verified_compressed_staging_launch(root):
        # Raw CRIU pages are on the compressed Btrfs mount. The output filesystem
        # retains only portable exports and encoding buffers: two bounded exports
        # plus 512 MiB above its unchanged 8 GiB controller stop floor.
        return max(9 * 2**30, 8 * 2**30 + 2 * compressed_peak + 512 * 2**20)
    return max(12 * 2**30, 10 * 2**30 + peak) if found else 24 * 2**30


def retain_verified_success(folder):
    """A sibling infra error must never erase an independently graded success."""
    if (folder / "result.json").exists():
        return False
    outcomes = {}
    proof = []
    for p in sorted(folder.glob("attempts/*/outcome.json")):
        grade = p.parent / "grade/grade.json"
        if not grade.exists():
            continue
        row = json.loads(p.read_text())
        g = json.loads(grade.read_text())
        if g.get("status") != "graded" or g.get("original_task") != str(
            (task_root(ROOT) / folder.name).resolve()
        ):
            continue
        if row.get("reward") != g.get("reward"):
            continue
        outcomes[p.parent.name] = row
        if g.get("reward") == 1:
            proof.append({"path": str(grade), "sha256": hashlib.sha256(grade.read_bytes()).hexdigest()})
    if not proof:
        return False
    error = folder / "error.json"
    if error.exists():
        saved = folder / "infrastructure-history" / f"{time.time_ns()}.json"
        saved.parent.mkdir(exist_ok=True)
        shutil.copy2(error, saved)
    write_json(
        folder / "result.json",
        {
            "task": folder.name,
            "status": "complete",
            "mode": "agentfork-only",
            "baseline_attempts": 0,
            "baseline_resolved": None,
            "baseline_pass_at_4": None,
            "agentfork_resolved": True,
            "root_resolved": outcomes.get("root-0", {}).get("reward") == 1,
            "outcomes": outcomes,
            "agentfork_archive": list(outcomes),
            "finalization": "Verified graded success retained despite sibling infrastructure errors; no missing trial is counted as a failed trial.",
            "success_grade_proofs": proof,
        },
    )
    return True


def main(args):
    os.umask(0o077)
    root = args.output.resolve()
    lock = (root / "supervisor.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    launch = json.loads((root / "launch.json").read_text())
    configuration = json.loads((root / "configuration.json").read_text())
    names = configuration["task_names"]
    assert configuration["mode"] == "agentfork-only" and configuration["baseline_attempts"] == 0
    (root / "supervisor.pid").write_text(str(os.getpid()) + "\n")
    pid = int((root / "controller.pid").read_text())
    pass_index = current_pass(root)
    while True:
        stamp = identity(pid)
        while stamp and identity(pid) == stamp:
            write_json(
                root / "supervisor.json",
                {
                    "state": "watching",
                    "controller_pid": pid,
                    "campaign_pass": pass_index,
                    "max_passes": args.max_passes,
                    "updated_at": time.time(),
                },
            )
            time.sleep(10)
        # The previous monitor writes a final sample before the next one takes over.
        monitor_file = root / "agentfork.monitor.pid"
        if monitor_file.exists():
            monitor = int(monitor_file.read_text())
            deadline = time.time() + 25
            while identity(monitor) and time.time() < deadline:
                time.sleep(1)
        for name in names:
            retain_verified_success(root / name)
        pending = [name for name in names if not (root / name / "result.json").exists()]
        if not pending or pass_index >= args.max_passes:
            write_json(
                root / "supervisor.json",
                {
                    "state": "complete" if not pending else "retry_limit_reached",
                    "campaign_pass": pass_index,
                    "remaining": pending,
                    "updated_at": time.time(),
                },
            )
            return
        from agentfork.provider_health import probe

        provider = load_provider()
        while True:
            health = probe(provider, root)
            if health["state"] == "available":
                break
            write_json(
                root / "supervisor.json",
                {
                    "state": "waiting_provider",
                    "remaining": pending,
                    "campaign_pass": pass_index,
                    "max_passes": args.max_passes,
                    "updated_at": time.time(),
                    "provider_http_status": health.get("http_status"),
                    "provider_error_type": health.get("error_type"),
                },
            )
            progress = root / "progress.json"
            if progress.exists():
                data = json.loads(progress.read_text())
                data.update(state="waiting_provider", updated_at=time.time())
                write_json(progress, data)
            time.sleep(60)
        config = json.loads((root / "configuration.json").read_text())
        store = Path(config.get("btrfs_store", ROOT / "storage/btrfs"))
        # Raw CRIU pages, rather than gzip size alone, determine restore staging.
        output_reserve = startup_output_reserve(root, names)
        while (
            shutil.disk_usage(ROOT).free < 8 * 2**30
            or shutil.disk_usage(root).free < output_reserve
            or shutil.disk_usage(store).free < 2 * 2**30
        ):
            write_json(
                root / "supervisor.json",
                {
                    "state": "waiting_storage",
                    "remaining": pending,
                    "campaign_pass": pass_index,
                    "updated_at": time.time(),
                    "host_free": shutil.disk_usage(ROOT).free,
                    "output_free": shutil.disk_usage(root).free,
                    "store_free": shutil.disk_usage(store).free,
                    "output_reserve_bytes": output_reserve,
                },
            )
            progress = root / "progress.json"
            if progress.exists():
                data = json.loads(progress.read_text())
                data.update(state="waiting_storage", updated_at=time.time())
                write_json(progress, data)
            time.sleep(30)
        pass_index += 1
        history = root / "controller-passes"
        history.mkdir(exist_ok=True)
        write_json(
            history / f"{pass_index}.json",
            {
                "tasks_to_retry": pending,
                "started_at": time.time(),
                "argv": launch["argv"],
                "completed_results_reused": len(names) - len(pending),
                "output_reserve_bytes": output_reserve,
            },
        )
        with (root / "controller.log").open("ab") as log:
            child = subprocess.Popen(
                launch["argv"],
                cwd=ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )
        pid = child.pid
        (root / "controller.pid").write_text(str(pid) + "\n")
        with (root / "agentfork.monitor.log").open("ab") as log:
            monitor = subprocess.Popen(
                [str(Path(sys.executable)), "-u", str(PACKAGE / "monitor.py"), str(root), "--pid", str(pid)],
                cwd=ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )
        (root / "agentfork.monitor.pid").write_text(str(monitor.pid) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--max-passes", type=int, default=12)
    main(parser.parse_args())
