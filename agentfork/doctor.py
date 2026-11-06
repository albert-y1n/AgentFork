"""Inspect host prerequisites without changing mounts, services, or firewall rules."""

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path


def inspect(store):
    checks = {"rootful": os.geteuid() == 0}
    tools = {
        name: shutil.which(name)
        for name in ["podman", "criu", "runc", "btrfs", "nsenter", "ip", "iptables", "ip6tables", "zstd"]
    }
    checks["tools_available"] = all(tools.values())
    filesystem = None
    if Path(store).exists() and shutil.which("findmnt"):
        result = subprocess.run(
            ["findmnt", "-n", "-o", "FSTYPE,OPTIONS", "--target", str(store)], capture_output=True, text=True
        )
        filesystem = result.stdout.strip()
    checks["btrfs_store"] = bool(filesystem and filesystem.split()[0] == "btrfs")
    checks["subvolume_deletion_enabled"] = bool(filesystem and "user_subvol_rm_allowed" in filesystem)
    criu = None
    if tools["criu"]:
        result = subprocess.run([tools["criu"], "check"], capture_output=True, text=True, timeout=30)
        criu = {"exit_code": result.returncode, "stderr": result.stderr[-2000:]}
    checks["criu_kernel_support"] = bool(criu is not None and criu["exit_code"] == 0)
    return {
        "ready": all(checks.values()),
        "checks": checks,
        "tools": tools,
        "filesystem": filesystem,
        "criu": criu,
        "kvm_required": False,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", type=Path, required=True)
    result = inspect(parser.parse_args(argv).store)
    print(json.dumps(result, indent=2))
    if not result["ready"]:
        raise SystemExit(1)
