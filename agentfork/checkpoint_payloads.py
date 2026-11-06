"""Retain immutable encoded payloads on the host disk, with archive pins.

Only Zstandard payload files move. Full dictionary references remain siblings
of their checkpoint manifests. Atomic symlink replacement preserves bytes for
both existing open readers and subsequent restores.
"""

from pathlib import Path
import hashlib, os, shutil, uuid

from agentfork.checkpoint_storage import ROOT, digest, case_lock, encoded_info, sidecar

PAYLOAD_ROOT = ROOT / "storage/retained-checkpoint-payloads"


def managed_payload(path):
    path = Path(path)
    return path.is_symlink() and path.resolve().is_relative_to((PAYLOAD_ROOT / "objects").resolve())


def relocate_payload(payload, expected_sha, *, reserve_bytes=16 * 2**30):
    payload = Path(payload)
    if payload.is_symlink() or not payload.is_file() or payload.stat().st_nlink != 1:
        return {"status": "unchanged"}
    objects = PAYLOAD_ROOT / "objects"
    objects.mkdir(parents=True, exist_ok=True, mode=0o700)
    size = payload.stat().st_size
    if shutil.disk_usage(objects).free < size + reserve_bytes:
        return {"status": "waiting_host_capacity"}
    if digest(payload) != expected_sha:
        raise ValueError("Checkpoint payload checksum changed before relocation")
    destination = objects / (expected_sha + ".zst")
    if not destination.exists():
        temporary = objects / (".copy-" + uuid.uuid4().hex)
        try:
            shutil.copy2(payload, temporary)
            if digest(temporary) != expected_sha:
                raise ValueError("Relocated checkpoint payload differs")
            with temporary.open("rb") as stream:
                os.fsync(stream.fileno())
            temporary.chmod(0o400)
            try:
                os.link(temporary, destination)
            except FileExistsError:
                pass
        finally:
            temporary.unlink(missing_ok=True)
    if digest(destination) != expected_sha:
        raise ValueError("Existing retained payload checksum changed")
    descriptor = os.open(objects, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    link = payload.with_name(payload.name + ".move-" + uuid.uuid4().hex)
    link.symlink_to(destination)
    link.replace(payload)
    descriptor = os.open(payload.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return {
        "status": "relocated",
        "bytes": size,
        "sha256": expected_sha,
        "source": str(payload),
        "retained": str(destination),
    }


def pin_for_archive(payload, archive_root, task, relative):
    payload = Path(payload)
    if not managed_payload(payload):
        return None
    target = payload.resolve()
    expected = target.stem
    if len(expected) != 64 or digest(target) != expected:
        raise ValueError("Retained checkpoint payload checksum changed")
    relative = Path(relative)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("Invalid archive payload path")
    archive_id = hashlib.sha256(str(Path(archive_root).resolve()).encode()).hexdigest()[:16]
    pin = PAYLOAD_ROOT / "archive-pins" / archive_id / task / relative
    pin.parent.mkdir(parents=True, exist_ok=True)
    if not pin.exists():
        os.link(target, pin)
    if digest(pin) != expected:
        raise ValueError("Pinned archive payload differs")
    descriptor = os.open(pin.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return {"path": str(relative), "target": str(pin), "sha256": expected, "bytes": pin.stat().st_size}


def relocate_case_payloads(case, *, progress=None, max_files=64):
    case = Path(case)
    moved = []
    with case_lock(case):
        if (case / "result.json").exists():
            return moved
        candidates = []
        for index in case.glob("attempts/*/checkpoints.json"):
            import json

            for point in json.loads(index.read_text()):
                memory = point.get("memory_checkpoint", {})
                archive = memory.get("archive")
                if not archive or not sidecar(archive).exists():
                    continue
                value, _, payload = encoded_info(archive, expected_sha=memory["archive_sha256"], verify=False)
                if not payload.is_symlink() and payload.is_file():
                    candidates.append((payload.stat().st_size, payload, value["packed_sha256"]))
        for _, payload, expected in sorted(candidates, reverse=True):
            if progress:
                progress({"phase": "relocating_payload", "path": str(payload)})
            row = relocate_payload(payload, expected)
            if row["status"] == "waiting_host_capacity":
                break
            if row["status"] == "relocated":
                moved.append(row)
                if progress:
                    progress(row)
                if len(moved) >= max_files:
                    break
    return moved
