"""Pin compatible runtime binaries in host-owned snapshot metadata."""

import hashlib
import json
from pathlib import Path

from agentfork_sandbox.btrfs_store import config_path
from agentfork_sandbox.podman_containers import RUNTIME_PATH

from agentfork.config import ROOT

STATIC_SHA = "dc0b25a83de038befba88a753245761a0cb78b0094f9091c75c6a5ddc9614f89"
# New installations build a static runtime by default. Legacy snapshots retain their explicit pins.
PROFILES = {}

KEY = "agentfork_runtime"


def bind_runtime(argv, rootfs, default_binary, *, profiles=None):
    """Keep a published snapshot's binary even when future defaults change.

    Legacy images without a compatibility profile retain their original mount
    verbatim. The metadata lives beside the rootfs, outside the task container.
    """
    metadata = config_path(Path(rootfs))
    record = json.loads(metadata.read_text())
    profile = record.get(KEY)
    if profile is None:
        digest = record.get("image", "").split("@")[-1]
        profile = (PROFILES if profiles is None else profiles).get(digest)
    if profile is None:
        return argv
    binary = Path(profile["path"])
    if not binary.is_absolute() or not binary.is_file():
        raise ValueError("Pinned runtime binary is missing")
    with binary.open("rb") as stream:
        actual = hashlib.file_digest(stream, "sha256").hexdigest()
    if actual != profile["sha256"]:
        raise ValueError("Pinned runtime binary checksum changed")
    original = str(default_binary) + ":" + RUNTIME_PATH + ":ro"
    if argv.count(original) != 1:
        raise ValueError("Cannot identify the original runtime bind mount")
    values = list(argv)
    values[values.index(original)] = str(binary) + ":" + RUNTIME_PATH + ":ro"
    if record.get(KEY) is None:
        pin_runtime(rootfs, profile)
    return values


def pin_runtime(rootfs, profile):
    metadata = config_path(Path(rootfs))
    record = json.loads(metadata.read_text())
    if record.get(KEY) is not None and record[KEY] != profile:
        raise ValueError("Snapshot already pins a different runtime binary")
    record[KEY] = profile
    temporary = metadata.with_name(metadata.name + ".runtime.tmp")
    temporary.write_text(json.dumps(record, indent=2) + "\n")
    temporary.replace(metadata)


def pinned_runtime(rootfs):
    return json.loads(config_path(Path(rootfs)).read_text()).get(KEY)
