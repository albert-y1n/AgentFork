"""Reuse the immutable pre-agent template when resuming a retained case."""

from pathlib import Path
import hashlib
import json
import os
import time
import uuid

from agentfork_sandbox.btrfs_store import config_path


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def record_prepared(case, snapshot_id, *, provenance=None):
    snapshot = case.engine.pool.store.snapshots / snapshot_id
    record = {
        "format": "agentfork-prepared-environment-v1",
        "task": case.name,
        "image": case.image,
        "snapshot_id": snapshot_id,
        "task_definition_sha256": sha(case.path / "task.toml"),
        "claude_sha256": sha(snapshot / "usr/local/bin/tb40-claude"),
        "snapshot_config_sha256": sha(config_path(snapshot)),
        "cwd": case.cwd,
        "user": case.user,
        "environment_env": case.environment_env,
        "created_at": time.time(),
    }
    if provenance:
        record["provenance"] = provenance
    path = case.directory / "prepared-environment.json"
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    with temporary.open("x") as stream:
        json.dump(record, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return record


async def reuse_prepared(case):
    path = case.directory / "prepared-environment.json"
    if not getattr(case, "recovery", None) or not path.exists():
        return False
    record = json.loads(path.read_text())
    sid = record["snapshot_id"]
    if Path(sid).name != sid or sid in (".", ".."):
        raise ValueError("Invalid prepared snapshot identity")
    snapshot = case.engine.pool.store.snapshots / sid
    if (
        record.get("format") != "agentfork-prepared-environment-v1"
        or record["task"] != case.name
        or record["image"] != case.image
        or record["task_definition_sha256"] != sha(case.path / "task.toml")
    ):
        raise ValueError("Prepared template does not match this task definition")
    if sid not in case.recovery.get("retained_snapshots", []) or not snapshot.is_dir():
        raise ValueError("Prepared template is missing from retained snapshots")
    if (
        sha(config_path(snapshot)) != record["snapshot_config_sha256"]
        or sha(snapshot / "usr/local/bin/tb40-claude") != record["claude_sha256"]
    ):
        raise ValueError("Prepared template identity changed")
    flags = await case.engine.pool.store._btrfs("property", "get", "-ts", str(snapshot), "ro")
    if flags.strip() != "ro=true":
        raise ValueError("Prepared template must remain read-only")
    case.base = None
    case.prepared = sid
    case.cwd, case.user = record["cwd"], record["user"]
    case.environment_env = record["environment_env"]
    case.event("prepared_template_reused", snapshot=sid, cwd=case.cwd, user=case.user)
    return True


def enable_compressed_bases(store):
    """Set compression on each new base subvolume before image extraction.

    A Btrfs subvolume does not inherit its parent's compression property. Only
    newly created, empty base subvolumes are changed; retained points stay intact.
    """
    create = store.create

    async def compressed_create(path):
        await create(path)
        if Path(path).parent == store.bases:
            try:
                await store._btrfs("property", "set", str(path), "compression", "zstd")
            except BaseException:
                await store.delete(path)
                raise

    store.create = compressed_create
