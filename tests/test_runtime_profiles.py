import hashlib
import json
from pathlib import Path

import pytest

from agentfork.runtime_profiles import bind_runtime, pin_runtime, KEY
from agentfork_sandbox.btrfs_store import config_path
from agentfork_sandbox.podman_containers import RUNTIME_PATH


def fixture(tmp_path):
    rootfs = tmp_path / "sandbox"
    rootfs.mkdir()
    config_path(rootfs).write_text(json.dumps({"image": "example/task@sha256:old-libc"}))
    binary = tmp_path / "static-v1"
    binary.write_bytes(b"version one")
    profile = {"path": str(binary), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
    default = tmp_path / "legacy-dynamic"
    argv = ["-v", str(default) + ":" + RUNTIME_PATH + ":ro", "--rootfs", str(rootfs)]
    return rootfs, binary, profile, default, argv


def test_legacy_snapshot_keeps_original_mount_and_metadata(tmp_path):
    rootfs, _, _, default, argv = fixture(tmp_path)
    before = config_path(rootfs).read_bytes()
    assert bind_runtime(argv, rootfs, default, profiles={}) == argv
    assert config_path(rootfs).read_bytes() == before


def test_compatible_image_pins_its_binary_outside_the_rootfs(tmp_path):
    rootfs, binary, profile, default, argv = fixture(tmp_path)
    result = bind_runtime(argv, rootfs, default, profiles={"sha256:old-libc": profile})
    assert str(binary) + ":" + RUNTIME_PATH + ":ro" in result
    assert json.loads(config_path(rootfs).read_text())[KEY] == profile


def test_existing_checkpoint_uses_old_binary_after_profile_upgrade(tmp_path):
    rootfs, binary, profile, default, argv = fixture(tmp_path)
    bind_runtime(argv, rootfs, default, profiles={"sha256:old-libc": profile})
    upgraded = tmp_path / "static-v2"
    upgraded.write_bytes(b"new program with a different memory layout")
    new = {"path": str(upgraded), "sha256": hashlib.sha256(upgraded.read_bytes()).hexdigest()}
    result = bind_runtime(argv, rootfs, default, profiles={"sha256:old-libc": new})
    assert str(binary) + ":" + RUNTIME_PATH + ":ro" in result
    assert json.loads(config_path(rootfs).read_text())[KEY] == profile


def test_changed_binary_cannot_silently_restore_a_checkpoint(tmp_path):
    rootfs, binary, profile, default, argv = fixture(tmp_path)
    bind_runtime(argv, rootfs, default, profiles={"sha256:old-libc": profile})
    binary.write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="checksum changed"):
        bind_runtime(argv, rootfs, default, profiles={})


def test_failed_mount_resolution_does_not_publish_a_pin(tmp_path):
    rootfs, _, profile, default, _ = fixture(tmp_path)
    before = config_path(rootfs).read_bytes()
    with pytest.raises(ValueError, match="bind mount"):
        bind_runtime([], rootfs, default, profiles={"sha256:old-libc": profile})
    assert config_path(rootfs).read_bytes() == before


def test_implicit_legacy_runtime_beats_a_newly_added_image_profile(tmp_path):
    rootfs, _, static, default, argv = fixture(tmp_path)
    default.write_bytes(b"original dynamic program from the legacy checkpoint")
    legacy = {"path": str(default), "sha256": hashlib.sha256(default.read_bytes()).hexdigest()}
    pin_runtime(rootfs, legacy)
    assert bind_runtime(argv, rootfs, default, profiles={"sha256:old-libc": static}) == argv
    assert json.loads(config_path(rootfs).read_text())[KEY] == legacy
