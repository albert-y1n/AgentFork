"""Compressed CRIU page-image scratch, mounted at Podman's original path."""

from pathlib import Path
import os
import json
import re
import shutil
import uuid


class CompressedStaging:
    def __init__(self, root, metadata_root, run):
        self.root = Path(root).resolve()
        self.metadata_root = Path(metadata_root).resolve()
        self.run = run
        self.bindings = {}

    def paths(self, sid, static):
        if not re.fullmatch(r"agentfork-[0-9a-f]{12}", sid):
            raise ValueError("Unexpected container identity for CRIU scratch")
        static = Path(static)
        if not static.resolve().is_relative_to(self.metadata_root):
            raise ValueError("CRIU scratch target escapes the Podman metadata root")
        source, target = self.root / sid, static / "checkpoint"
        if source.is_symlink() or target.is_symlink():
            raise ValueError("CRIU scratch directories must not be symlinks")
        return source, target

    def owns_mount(self, source, target):
        return os.path.ismount(target) and source.exists() and os.path.samestat(source.stat(), target.stat())

    def journal(self, sid, source, target):
        path = self.root / (sid + ".binding.json")
        temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
        with temporary.open("x") as stream:
            json.dump({"sid": sid, "source": str(source), "static": str(target.parent)}, stream)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    async def ensure(self, sid, static):
        source, target = self.paths(sid, static)
        if os.path.ismount(target):
            if not self.owns_mount(source, target):
                raise ValueError("Refusing to replace an unrelated metadata mount")
            self.bindings[sid] = (source, target)
            self.journal(sid, source, target)
            return
        if target.exists() and any(target.iterdir()):
            raise ValueError("Refusing to hide existing CRIU images below a bind mount")
        self.root.mkdir(parents=True, exist_ok=True)
        source.mkdir(exist_ok=True)
        if any(source.iterdir()):
            raise ValueError("Unclaimed compressed CRIU scratch is not empty")
        await self.run(["btrfs", "property", "set", source, "compression", "zstd"])
        target.mkdir(exist_ok=True)
        await self.run(["mount", "--bind", source, target])
        if not self.owns_mount(source, target):
            raise RuntimeError("CRIU scratch bind mount identity differs")
        self.bindings[sid] = (source, target)
        try:
            self.journal(sid, source, target)
        except BaseException:
            await self.release(sid)
            raise

    def clear(self, sid, static):
        source, target = self.paths(sid, static)
        if sid not in self.bindings:
            return False
        if not self.owns_mount(source, target):
            raise ValueError("Managed CRIU scratch mount disappeared or changed")
        for child in target.iterdir():
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
        return True

    async def release(self, sid):
        binding = self.bindings.get(sid)
        if binding is None:
            journal = self.root / (sid + ".binding.json")
            if not journal.exists():
                return
            record = json.loads(journal.read_text())
            source, target = self.paths(sid, record["static"])
            if record["sid"] != sid or record["source"] != str(source):
                raise ValueError("CRIU scratch ownership journal differs")
            binding = (source, target)
        source, target = binding
        if not self.owns_mount(source, target):
            raise ValueError("Refusing to unmount an unexpected CRIU scratch target")
        await self.run(["umount", target])
        self.bindings.pop(sid, None)
        shutil.rmtree(source)
        (self.root / (sid + ".binding.json")).unlink(missing_ok=True)
