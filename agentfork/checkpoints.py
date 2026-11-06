"""Separate historical restore validation from current material availability."""

import argparse
import hashlib
import json
from pathlib import Path


def checksum(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def assess(point, store, *, verify_hashes=True):
    """Check a trusted local point descriptor; this does not perform a live restore.

    A previous successful restore cannot prove that payloads remain on disk or
    that the current kernel can restore them. Only a new backend.fork does that.
    """
    memory = point.get("memory_checkpoint", point)
    checks = {"historically_validated": memory.get("validation", {}).get("passed") is True}
    errors = []
    snapshot = point.get("snapshot_id", memory.get("disk_snapshot"))
    # Do not interpret an absent or escaping snapshot identifier as the store.
    safe = (
        isinstance(snapshot, str)
        and bool(snapshot)
        and snapshot not in {".", ".."}
        and Path(snapshot).name == snapshot
    )
    checks["disk_snapshot_present"] = bool(safe and (Path(store) / "snapshots" / snapshot).is_dir())
    runtime = memory.get("runtime_binary")
    if runtime:
        binary = Path(runtime["path"])
        checks["runtime_present"] = binary.is_file()
        if verify_hashes and binary.is_file():
            checks["runtime_hash"] = checksum(binary) == runtime.get("sha256")
    for field, digest_field in [("native_path", "sha256"), ("request_path", None)]:
        if field not in point:
            continue
        path = Path(point[field])
        checks[field + "_present"] = path.is_file()
        if verify_hashes and checks[field + "_present"] and digest_field:
            checks[field + "_hash"] = checksum(path) == point.get(digest_field)
    archive_value = memory.get("archive")
    archive = Path(archive_value) if archive_value else None
    checks["memory_payload_present"] = bool(archive and archive.is_file())
    if archive and not archive.is_file():
        from agentfork.checkpoint_storage import encoded_info, sidecar

        if sidecar(archive).is_file():
            try:
                encoded_info(archive, expected_sha=memory.get("archive_sha256"), verify=verify_hashes)
                checks["memory_payload_present"] = True
            except (OSError, ValueError, KeyError) as error:
                errors.append(type(error).__name__ + ": " + str(error))
    elif archive and verify_hashes:
        checks["memory_payload_hash"] = checksum(archive) == memory.get("archive_sha256")
    available = all(checks.values())
    return {
        "step": point.get("step"),
        "status": ("available_verified" if verify_hashes else "available_unverified")
        if available
        else "unavailable",
        "currently_available": available,
        "live_restore_performed": False,
        "checks": checks,
        "errors": errors,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("descriptor", type=Path)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--skip-hashes", action="store_true")
    args = parser.parse_args(argv)
    value = json.loads(args.descriptor.read_text())
    points = value if isinstance(value, list) else [value]
    results = [assess(p, args.store, verify_hashes=not args.skip_hashes) for p in points]
    print(json.dumps(results, indent=2))
    if any(not r["currently_available"] for r in results):
        raise SystemExit(1)
