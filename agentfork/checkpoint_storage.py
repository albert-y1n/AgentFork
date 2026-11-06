"""Lossless reference compression of immutable CRIU export files.

Point metadata continues to identify the original gzip bytes and SHA256. A
sidecar records a Zstandard patch against a full sibling export. References
remain full files, so restoration has no dependency chain. Reader leases and
a separate case lock serialize publication with restores and success archives.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import uuid

from agentfork.config import ROOT

FORMAT = "criu-gzip-zstd-reference-v1"
SUFFIX = ".ref.json"


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def sidecar(archive):
    archive = Path(archive)
    return archive.with_name(archive.name + SUFFIX)


def lease_path(archive):
    archive = Path(archive)
    return archive.with_name(archive.name + ".storage.lock")


@contextmanager
def case_lock(case):
    case = Path(case)
    locks = case.parent / ".checkpoint-storage-locks"
    locks.mkdir(exist_ok=True)
    with (locks / (case.name + ".lock")).open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def encoded_info(archive, *, expected_sha=None, verify=True):
    """Validate a reference description without materializing its original."""
    archive = Path(archive)
    value = json.loads(sidecar(archive).read_text())
    if value.get("format") != FORMAT:
        raise ValueError("Unknown encoded checkpoint format")
    if expected_sha is not None and value["original_sha256"] != expected_sha:
        raise ValueError("Encoded checkpoint identity differs from point metadata")
    reference = (archive.parent / value["reference"]).resolve()
    packed = archive.parent / value["packed"]
    if (
        not reference.is_relative_to(archive.parent.parent.resolve())
        or Path(value["packed"]).name != value["packed"]
        or reference == archive.resolve()
    ):
        raise ValueError("Invalid encoded checkpoint dependency path")
    if not value.get("byte_exact_verified"):
        raise ValueError("Encoded checkpoint has no byte-exact validation")
    if not reference.is_file():
        raise ValueError("Full checkpoint reference is missing")
    if verify:
        if digest(reference) != value["reference_sha256"]:
            raise ValueError("Checkpoint reference checksum changed")
        if digest(packed) != value["packed_sha256"]:
            raise ValueError("Encoded checkpoint checksum changed")
    return value, reference, packed


@asynccontextmanager
async def checkpoint_archive(archive, expected_sha, *, scratch=None):
    """Yield exactly the original bytes; never block the gateway on a lease."""
    archive = Path(archive)
    lockpath = lease_path(archive)
    try:
        lock = lockpath.open("rb")
    except FileNotFoundError:
        lock = lockpath.open("a+b")
    temporary = None
    try:
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                await asyncio.sleep(0.05)
        if archive.is_file():
            if await asyncio.to_thread(digest, archive) != expected_sha:
                raise ValueError("CRIU checkpoint archive checksum changed")
            yield archive
            return
        value, reference, packed = await asyncio.to_thread(encoded_info, archive, expected_sha=expected_sha)
        parent = Path(scratch or ROOT / "storage/checkpoint-materialize")
        parent.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(prefix="restore-", dir=parent)
        restored = Path(temporary.name) / "checkpoint.tar.gz"
        process = await asyncio.create_subprocess_exec(
            "zstd",
            "-q",
            "-d",
            "--long=31",
            "--patch-from=" + str(reference),
            str(packed.resolve(strict=True)),
            "-o",
            str(restored),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr = await process.communicate()
        except BaseException:
            if process.returncode is None:
                process.kill()
            await process.communicate()
            raise
        if process.returncode:
            raise RuntimeError("Cannot decode checkpoint: " + stderr.decode(errors="replace")[-2000:])
        if (
            restored.stat().st_size != value["original_bytes"]
            or await asyncio.to_thread(digest, restored) != expected_sha
        ):
            raise ValueError("Decoded checkpoint differs from its original bytes")
        yield restored
    finally:
        if temporary is not None:
            temporary.cleanup()
        lock.close()


def stored_bytes(archive):
    archive = Path(archive)
    if archive.is_file():
        return archive.stat().st_size
    value, _, packed = encoded_info(archive, verify=False)
    return packed.stat().st_size + sidecar(archive).stat().st_size


def compact_checkpoint(
    archive, reference, *, expected_sha, reference_sha, case=None, minimum_saving=0.1, scratch=None
):
    """Publish only an independently decoded, hash-verified representation."""
    archive, reference = Path(archive), Path(reference)
    if archive.parent.parent.resolve() != reference.parent.parent.resolve():
        raise ValueError("Reference must be a sibling checkpoint in the same attempt")
    # Serialize all encoders for this case, including the success archiver.
    case = Path(case or archive.parent.parent)
    with case_lock(case):
        if (case / "result.json").exists():
            return {"status": "completed_case_unchanged"}
        if sidecar(archive).exists():
            value, _, _ = encoded_info(archive, expected_sha=expected_sha)
            return {"status": "already_encoded", **value}
        if (
            not archive.is_file()
            or archive.is_symlink()
            or archive.stat().st_nlink != 1
            or archive == reference
        ):
            return {"status": "ineligible_source"}
        if not reference.is_file() or sidecar(reference).exists() or reference.stat().st_size >= 2**31:
            return {"status": "ineligible_reference"}
        # A full reference cannot itself be compacted later.
        for metadata in archive.parent.parent.glob("*/checkpoint.tar.gz" + SUFFIX):
            other = json.loads(metadata.read_text())
            if (metadata.parent / other["reference"]).resolve() == archive.resolve():
                return {"status": "pinned_full_reference"}
        if digest(archive) != expected_sha or digest(reference) != reference_sha:
            raise ValueError("Original checkpoint or reference checksum changed")
        parent = Path(scratch or ROOT / "storage/checkpoint-materialize")
        parent.mkdir(parents=True, exist_ok=True)
        original_size = archive.stat().st_size
        original_allocated = archive.stat().st_blocks * 512
        packed = archive.with_name(archive.name + ".ref-" + uuid.uuid4().hex + ".zst")
        started = time.time()
        published = False
        try:
            result = subprocess.run(
                [
                    "nice",
                    "-n",
                    "19",
                    "zstd",
                    "-q",
                    "-1",
                    "-T1",
                    "--long=31",
                    "--patch-from=" + str(reference),
                    str(archive),
                    "-o",
                    str(packed),
                ],
                capture_output=True,
                text=True,
                timeout=1200,
            )
            if result.returncode:
                raise RuntimeError("Cannot encode checkpoint: " + result.stderr[-2000:])
            if packed.stat().st_size >= original_size * (1 - minimum_saving):
                return {
                    "status": "insufficient_saving",
                    "original_bytes": original_size,
                    "packed_bytes": packed.stat().st_size,
                }
            with tempfile.TemporaryDirectory(prefix="verify-", dir=parent) as temp:
                restored = Path(temp) / "checkpoint.tar.gz"
                result = subprocess.run(
                    [
                        "nice",
                        "-n",
                        "19",
                        "zstd",
                        "-q",
                        "-d",
                        "--long=31",
                        "--patch-from=" + str(reference),
                        str(packed),
                        "-o",
                        str(restored),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=1200,
                )
                if (
                    result.returncode
                    or restored.stat().st_size != original_size
                    or digest(restored) != expected_sha
                ):
                    raise ValueError("Encoded checkpoint failed byte-exact validation")
            if (case / "result.json").exists():
                return {"status": "completed_case_unchanged"}
            value = {
                "format": FORMAT,
                "original_bytes": original_size,
                "original_sha256": expected_sha,
                "reference": os.path.relpath(reference, archive.parent),
                "reference_sha256": reference_sha,
                "packed": packed.name,
                "packed_bytes": packed.stat().st_size,
                "packed_sha256": digest(packed),
                "byte_exact_verified": True,
                "verified_at": time.time(),
            }
            # A fork may already be reading the full archive. Hold its lease
            # only for publication, never while encoding or verifying.
            with lease_path(archive).open("a+b") as lease:
                fcntl.flock(lease, fcntl.LOCK_EX)
                if digest(archive) != expected_sha:
                    raise ValueError("Original changed before encoding publication")
                for path in [reference, packed]:
                    with path.open("rb") as stream:
                        os.fsync(stream.fileno())
                metadata = sidecar(archive)
                temporary = metadata.with_name(metadata.name + ".tmp-" + uuid.uuid4().hex)
                with temporary.open("x") as stream:
                    json.dump(value, stream, indent=2)
                    stream.write("\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                temporary.replace(metadata)
                published = True
                archive.unlink()
                descriptor = os.open(archive.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            return {
                "status": "encoded",
                "saved_allocated_bytes": original_allocated - packed.stat().st_blocks * 512,
                "seconds": time.time() - started,
                **value,
            }
        finally:
            if not published:
                packed.unlink(missing_ok=True)
