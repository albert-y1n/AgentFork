"""Coherent Btrfs/CRIU checkpoints with isolated, restore-tested branches."""

from __future__ import annotations
from agentfork.config import PACKAGE
import asyncio, hashlib, ipaddress, json, secrets, shutil, struct, tarfile, time
from contextlib import asynccontextmanager
from contextvars import ContextVar
from pathlib import Path
from agentfork_sandbox.btrfs_store import config_path
from agentfork_sandbox.podman_images import read_config


class UnsupportedCheckpoint(RuntimeError):
    def __init__(self, reason, **details):
        self.reason, self.details = reason, details
        super().__init__(reason + ": " + json.dumps(details))


class CheckpointRestoreError(RuntimeError):
    pass


class ParentRestoreError(RuntimeError):
    def __init__(self, message, snapshot):
        self.unpublished_snapshot = snapshot
        super().__init__(message)


async def command(argv):
    started = time.monotonic()
    proc = await asyncio.create_subprocess_exec(
        *map(str, argv), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), 300)
    except BaseException:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
        raise
    return dict(
        argv=list(map(str, argv)),
        exit_code=proc.returncode,
        stdout=stdout.decode(errors="replace"),
        stderr=stderr.decode(errors="replace"),
        seconds=time.monotonic() - started,
    )


async def checked(argv):
    result = await command(argv)
    if result["exit_code"]:
        raise RuntimeError(json.dumps(result))
    return result


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def tcp_sockets(pid, protocol="tcp"):
    result = []
    for name, version in [(protocol, 4), (protocol + "6", 6)]:
        for line in Path(f"/proc/{pid}/net/{name}").read_text().splitlines()[1:]:
            f = line.split()

            def address(raw):
                host, port = raw.split(":")
                b = bytes.fromhex(host)
                b = b[::-1] if version == 4 else b"".join(b[i : i + 4][::-1] for i in range(0, 16, 4))
                ip = ipaddress.ip_address(b)
                if version == 6 and ip.ipv4_mapped:
                    ip = ip.ipv4_mapped
                return str(ip), int(port, 16)

            local, lport = address(f[1])
            remote, rport = address(f[2])
            result.append(
                dict(
                    local=local,
                    local_port=lport,
                    remote=remote,
                    remote_port=rport,
                    state=int(f[3], 16),
                    inode=f[9],
                )
            )
    return result


def external_datagrams(sockets, owned, ips):
    return [
        s
        for s in sockets
        if s["inode"] in owned
        and s["remote_port"]
        and not ipaddress.ip_address(s["remote"]).is_loopback
        and s["remote"] not in ips
    ]


class CriuBtrfs:
    def __init__(
        self,
        pool,
        root: Path,
        image: str,
        *,
        validate=True,
        podman=None,
        metadata_root=None,
        config_file=None,
        init_process=False,
        compressed_staging=False,
        validation_probe=None,
        init_path=None,
    ):
        self.pool, self.root, self.image = pool, root, image
        self.validation_probe = validation_probe
        self.init_path = (
            init_path
            or shutil.which("catatonit")
            or next(
                (
                    str(p)
                    for p in [
                        Path("/usr/libexec/podman/catatonit"),
                        Path("/usr/lib/podman/catatonit"),
                        root / "tools/podman/podman-linux-amd64/usr/local/lib/podman/catatonit",
                    ]
                    if p.is_file()
                ),
                "catatonit",
            )
        )
        self.podman = Path(podman) if podman else root / "tools/podman-agentfork"
        self.validate = validate
        self.keepers = {}
        self.identities = {}
        self.metadata_root = (
            Path(metadata_root) if metadata_root else root / "storage/podman/graph/overlay-containers"
        )
        from agentfork.criu_staging import CompressedStaging

        self.staging = (
            CompressedStaging(pool.store.path / "criu-staging", self.metadata_root, checked)
            if compressed_staging
            else None
        )
        self.init_process = init_process
        config = Path(config_file) if config_file else PACKAGE / "resources/criu.conf"
        self.config_file = config
        self.io_lock = asyncio.Lock()
        self.in_io = ContextVar("criu_io_" + secrets.token_hex(4), default=False)
        original_argv = pool._containers._run_argv

        def annotated(*args, **kwargs):
            init = ["--init", "--init-path", str(self.init_path)] if self.init_process else []
            from agentfork.runtime_profiles import bind_runtime

            values = [
                *init,
                "--annotation",
                "org.criu.config=" + str(config),
                *original_argv(*args, **kwargs),
            ]
            rootfs = args[1] if len(args) > 1 else kwargs["rootfs"]
            return bind_runtime(values, rootfs, pool._containers.runtime_bin)

        pool._containers._run_argv = annotated
        original_destroy = pool.destroy

        async def destroy(*sandboxes):
            keepers = [self.keepers.get(s.sandbox_id) for s in sandboxes]
            if self.staging:
                for sandbox in sandboxes:
                    if sandbox.sandbox_id:
                        await self.staging.release(sandbox.sandbox_id)
            await original_destroy(*sandboxes)
            for keeper in keepers:
                if keeper:
                    await original_destroy(keeper)
            for sid in list(self.keepers):
                if self.keepers[sid] in keepers:
                    self.keepers.pop(sid, None)
                    self.identities.pop(sid, None)

        pool.destroy = destroy

    async def inspect(self, sid):
        return json.loads((await checked([self.podman, "inspect", sid]))["stdout"])[0]

    @asynccontextmanager
    async def io(self):
        if self.in_io.get():
            yield
        else:
            async with self.io_lock:
                token = self.in_io.set(True)
                try:
                    yield
                finally:
                    self.in_io.reset(token)

    async def clear_staging(self, sid):
        config = await self.inspect(sid)
        static = Path(config["StaticDir"])
        if not static.is_relative_to(self.metadata_root):
            raise ValueError("Unexpected checkpoint staging directory")
        for name in ["checkpoint", "artifacts", "devshm-checkpoint.tar", "stats-dump", "stats-restore"]:
            p = static / name
            if name == "checkpoint" and self.staging and self.staging.clear(sid, static):
                continue
            if p.is_dir() and not p.is_symlink():
                shutil.rmtree(p)
            else:
                p.unlink(missing_ok=True)

    @asynccontextmanager
    async def network_gate(self, sid):
        info = await self.inspect(sid)
        pid = info["State"]["Pid"]
        if Path(f"/proc/{pid}/ns/net").stat().st_ino == Path("/proc/self/ns/net").stat().st_ino:
            raise UnsupportedCheckpoint("host_network_namespace")
        chain = "AFCRIU" + secrets.token_hex(4)
        installed = []
        try:
            # Existing requests may finish, while new off-container TCP peers
            # cannot become connected between classification and the dump.
            for binary in ["iptables", "ip6tables"]:
                await self.ns(pid, binary, "-N", chain)
                installed.append(binary)
                await self.ns(pid, binary, "-A", chain, "-o", "lo", "-j", "RETURN")
                await self.ns(pid, binary, "-A", chain, "-p", "udp", "-j", "DROP")
                await self.ns(
                    pid, binary, "-A", chain, "-p", "tcp", "-m", "conntrack", "--ctstate", "NEW", "-j", "DROP"
                )
                await self.ns(pid, binary, "-I", "OUTPUT", "1", "-j", chain)
            yield
        finally:
            current = await self.inspect(sid)
            current_pid = current["State"]["Pid"]
            if current_pid:
                for binary in installed:
                    for args in [("-D", "OUTPUT", "-j", chain), ("-F", chain), ("-X", chain)]:
                        await command(["nsenter", "--target", str(current_pid), "--net", "--", binary, *args])

    async def ns(self, pid, *argv):
        return await checked(["nsenter", "--target", str(pid), "--net", "--", *map(str, argv)])

    async def addresses(self, pid):
        row = await self.ns(pid, "ip", "-j", "address", "show")
        return sorted(
            {
                a["local"]
                for interface in json.loads(row["stdout"])
                for a in interface.get("addr_info", [])
                if not ipaddress.ip_address(a["local"]).is_loopback
                and not ipaddress.ip_address(a["local"]).is_link_local
            }
        )

    async def logs(self, sid, directory):
        try:
            config = await self.inspect(sid)
        except Exception:
            return
        directory.mkdir(parents=True, exist_ok=True)
        for name in ["dump.log", "restore.log"]:
            path = Path(config["StaticDir"]) / name
            if path.exists():
                shutil.copy2(path, directory / name)

    async def preflight(self, config, directory):
        sid = config["Name"].lstrip("/")
        pid = config["State"]["Pid"]
        row = await checked([self.podman, "top", sid, "hpid,pid,ppid,args"])
        roots = []
        traced = []
        hpids = []
        for line in row["stdout"].splitlines()[1:]:
            f = line.split(None, 3)
            if len(f) < 3:
                continue
            hpids.append(f[0])
            if f[2] == "0" and f[1] != "1":
                roots.append(line.strip())
            try:
                status = dict(
                    x.split(":", 1)
                    for x in Path("/proc", f[0], "status").read_text().splitlines()
                    if ":" in x
                )
                if int(status.get("TracerPid", "0")):
                    traced.append({"pid": f[1], "tracer_host_pid": status["TracerPid"].strip()})
            except FileNotFoundError:
                pass
        if config.get("ExecIDs") or roots:
            raise UnsupportedCheckpoint(
                "active_exec_outside_pid1_tree", exec_ids=config.get("ExecIDs", []), roots=roots
            )
        if traced:
            raise UnsupportedCheckpoint("ptrace_attached", processes=traced)
        ips = await self.addresses(pid)
        # Give short requests time to close; never consume their data or close
        # application sockets behind the application's back.
        deadline = time.monotonic() + 1
        while True:
            sockets = tcp_sockets(pid)
            owned = set()
            for hpid in hpids:
                try:
                    for fd in Path("/proc", hpid, "fd").iterdir():
                        try:
                            target = fd.readlink().as_posix()
                        except FileNotFoundError:
                            continue
                        if target.startswith("socket:["):
                            owned.add(target[8:-1])
                except FileNotFoundError:
                    pass
            active = [s for s in sockets if s["inode"] in owned and s["state"] in {1, 2, 3, 4, 5, 8, 9, 11}]
            datagrams = external_datagrams(tcp_sockets(pid, "udp"), owned, ips)
            if datagrams:
                raise UnsupportedCheckpoint("external_udp", sockets=datagrams)
            external = [
                s
                for s in active
                if not ipaddress.ip_address(s["remote"]).is_loopback and s["remote"] not in ips
            ]
            unowned = []
            for s in active:
                if s in external:
                    continue
                reverse = [
                    p
                    for p in sockets
                    if p["local"] == s["remote"]
                    and p["remote"] == s["local"]
                    and p["local_port"] == s["remote_port"]
                    and p["remote_port"] == s["local_port"]
                    and p["inode"] in owned
                ]
                if not reverse:
                    unowned.append(s)
            if not external and not unowned:
                break
            if time.monotonic() >= deadline:
                raise UnsupportedCheckpoint(
                    "external_tcp" if external else "tcp_peer_outside_process_tree",
                    sockets=external or unowned,
                )
            await asyncio.sleep(0.1)
        return ips, sockets

    async def capture(self, sandbox, directory):
        async with self.io():
            async with self.network_gate(sandbox.require_id()):
                return await self._capture(sandbox, directory)

    async def _capture(self, sandbox, directory):
        started = time.monotonic()
        directory.mkdir(parents=True, exist_ok=True)
        sid = sandbox.require_id()
        await sandbox.backend.close()
        sandbox.backend = self.pool._handle(sid, sandbox.agent_id).backend
        config = await self.inspect(sid)
        pid1_comm = Path("/proc", str(config["State"]["Pid"]), "comm").read_text().strip()
        try:
            ips, sockets = await self.preflight(config, directory)
        except UnsupportedCheckpoint as e:
            (directory / "preflight-rejection.json").write_text(
                json.dumps({"reason": e.reason, **e.details}, indent=2) + "\n"
            )
            raise
        host = config["HostConfig"]
        resources = {}
        if host.get("Memory"):
            resources["memory_mb"] = host["Memory"] // 2**20
        if host.get("CpuQuota") and host.get("CpuPeriod"):
            resources["cpu"] = host["CpuQuota"] / host["CpuPeriod"]
        archive = directory / "checkpoint.tar.gz"
        # Failed dumps may leave partial image tables. Never mix those files
        # into a later dump or its source restore.
        await self.clear_staging(sid)
        if self.staging:
            await self.staging.ensure(sid, Path(config["StaticDir"]))
        dumped = await command(
            [
                self.podman,
                "container",
                "checkpoint",
                "--keep",
                "--file-locks",
                "--tcp-established",
                "--export",
                archive,
                "--compress",
                "gzip",
                "--ignore-rootfs",
                "--ignore-volumes",
                sid,
            ]
        )
        (directory / "dump-command.json").write_text(json.dumps(dumped, indent=2) + "\n")
        if dumped["exit_code"]:
            await self.logs(sid, directory)
            state = await self.inspect(sid)
            # Export can fail after CRIU has already stopped the source.
            # Restore it before reporting an ineligible checkpoint.
            if not state["State"]["Running"]:
                recovery = await command(
                    [self.podman, "container", "restore", "--keep", "--file-locks", "--tcp-established", sid]
                )
                (directory / "failed-export-parent-recovery.json").write_text(
                    json.dumps(recovery, indent=2) + "\n"
                )
                if recovery["exit_code"]:
                    raise RuntimeError("Failed export also failed to resume parent: " + json.dumps(recovery))
            await self.clear_staging(sid)
            raise RuntimeError(json.dumps(dumped))
        snapshot = None
        try:
            snapshot = await self.pool.snapshot(sandbox)
        finally:
            resumed = await command(
                [self.podman, "container", "restore", "--keep", "--file-locks", "--tcp-established", sid]
            )
            (directory / "parent-restore.json").write_text(json.dumps(resumed, indent=2) + "\n")
            if resumed["exit_code"]:
                await self.logs(sid, directory)
                raise ParentRestoreError(
                    "Parent restore failed: " + json.dumps(resumed), snapshot.id if snapshot else None
                )
        if not ips:
            # Podman's network=none source restore can recreate its namespace
            # with lo down. Unix RPC still works, hiding broken local services.
            current = await self.inspect(sid)
            await self.ns(current["State"]["Pid"], "ip", "link", "set", "lo", "up")
        point = dict(
            container=sid,
            disk_snapshot=snapshot.id,
            archive=str(archive.resolve()),
            archive_bytes=archive.stat().st_size,
            archive_sha256=digest(archive),
            capture_seconds=dumped["seconds"],
            parent_resume_seconds=resumed["seconds"],
            resources=resources,
            environment=dict(x.split("=", 1) for x in config["Config"]["Env"]),
            network_addresses=ips,
            tcp_established=True,
            backend_revision="criu-v6" if self.init_process else "criu-v5",
            pid1_comm=pid1_comm,
            link_remap=True,
            config_sha256=digest(self.config_file),
            ghost_limit_bytes=256 * 2**20,
        )
        point["network_identity"] = self.identities.get(
            sid, {"hostname": config["Config"].get("Hostname"), "primary_ip": ips[0] if ips else None}
        )
        if self.staging:
            point["staging_backend"] = "btrfs-zstd-bind"
        from agentfork.runtime_profiles import pinned_runtime

        runtime = pinned_runtime(self.pool.store.sandboxes / sid)
        if runtime:
            point["runtime_binary"] = runtime
        # Eager restore has copied all pages back into process memory. The
        # portable archive is durable; retaining a second raw page tree here
        # would consume the host system disk at every checkpoint.
        await self.clear_staging(sid)
        try:
            if self.validate:
                child, _ = await self.fork(point, directory / "validation", validation=True)
                try:
                    point["validation"] = await self.validate_child(child)
                finally:
                    await self.pool.destroy(child)
            point["capture_total_seconds"] = time.monotonic() - started
            (directory / "point.json").write_text(json.dumps(point, indent=2) + "\n")
            return point
        except BaseException:
            await self.pool.delete_snapshot(snapshot.id)
            raise

    async def fork(self, point, directory, *, validation=False):
        async with self.io():
            try:
                return await self._fork(point, directory, validation=validation)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                raise CheckpointRestoreError(str(error)) from error

    async def qualify(self, point, directory):
        # Legacy metadata only tested tools/list; upgrading requires a fresh
        # execution probe, and an old success never bypasses missing storage.
        source = self.pool.store.snapshots / point["disk_snapshot"]
        if not source.is_dir():
            raise CheckpointRestoreError("Disk snapshot is missing")
        if point.get("validation", {}).get("passed") and point["validation"].get("tool_execution_verified"):
            return
        child, _ = await self.fork(point, directory, validation=True)
        try:
            point["validation"] = await self.validate_child(child)
        finally:
            await self.pool.destroy(child)

    async def validate_child(self, child):
        tools = await child.backend.list_tools()
        if not tools:
            raise CheckpointRestoreError("Restored runtime returned no tools")
        marker = "agentfork-restore-" + secrets.token_hex(8)
        result = await child.call("shell", command="printf %s " + marker)
        if result.is_error or result.output != marker:
            raise CheckpointRestoreError("Restored runtime cannot execute a shell tool")
        if self.validation_probe is not None:
            # An application probe runs only in the isolated throwaway child.
            # It must signal failure explicitly with False or an exception.
            if await self.validation_probe(child) is False:
                raise CheckpointRestoreError("Application restore probe failed")
        return {
            "passed": True,
            "kind": "isolated_child_restore",
            "tools_available": True,
            "tool_execution_verified": True,
            "application_probe_verified": self.validation_probe is not None,
            "network": "non-loopback egress blocked before restore",
        }

    async def _fork(self, point, directory, *, validation=False):
        from agentfork.checkpoint_storage import checkpoint_archive

        started = time.monotonic()
        directory.mkdir(parents=True, exist_ok=True)
        async with checkpoint_archive(point["archive"], point["archive_sha256"]) as archive:
            preparation = time.monotonic() - started
            sandbox, result = await self._fork_archive(point, directory, archive, validation=validation)
            result["archive_materialization_seconds"] = preparation
            result["fork_total_seconds"] = time.monotonic() - started
            (directory / "fork.json").write_text(json.dumps(result, indent=2) + "\n")
            return sandbox, result

    async def _fork_archive(self, point, directory, archive, *, validation=False):
        started = time.monotonic()
        directory.mkdir(parents=True, exist_ok=True)
        sid = "agentfork-" + secrets.token_hex(6)
        rootfs = self.pool.store.sandboxes / sid
        source = self.pool.store.snapshots / point["disk_snapshot"]
        keeper = None
        await self.pool.store.snapshot(source, rootfs)
        shutil.copyfile(config_path(source), config_path(rootfs))
        socket_dir = self.pool.socket_dir / sid
        socket_dir.mkdir()
        try:
            from agentfork.runtime_profiles import pinned_runtime, pin_runtime

            if point.get("runtime_binary"):
                if pinned_runtime(rootfs) != point["runtime_binary"]:
                    raise ValueError("Snapshot runtime identity differs from memory checkpoint")
            else:
                # Older checkpoints implicitly used the original dynamic binary.
                # Pin that identity before new image compatibility rules run.
                original_binary = self.pool._containers.runtime_bin
                pin_runtime(rootfs, {"path": str(original_binary), "sha256": digest(original_binary)})
            # A passive namespace keeper makes the branch network independent
            # of the task's stop/restore lifecycle. All old application IPs are
            # local aliases, while the bridge/uplink remains unique per branch.
            keeper = await self.pool.spawn(
                point["disk_snapshot"],
                resources={"cpu": 0.25, "memory_mb": 128},
                env={"AF_CRIU_NETWORK_KEEPER": "1"},
            )
            info = await self.inspect(keeper.require_id())
            pid = info["State"]["Pid"]
            ips = point.get("network_addresses", [])
            identity = point.get("network_identity", {})
            if not ips:
                # v3 migration: source IPs are available in the exported OCI
                # config's network.status, even after the source is removed.
                with tarfile.open(archive, "r:*") as tar:
                    try:
                        network = json.load(tar.extractfile("network.status")) or {}
                    except KeyError:
                        network = {}
                ips = [
                    subnet["ipnet"].split("/")[0]
                    for net in network.values()
                    for interface in net.get("interfaces", {}).values()
                    for subnet in interface.get("subnets", [])
                ]
                with tarfile.open(archive, "r:*") as tar:
                    spec = json.load(tar.extractfile("spec.dump"))
                identity = {"hostname": spec.get("hostname"), "primary_ip": ips[0] if ips else None}
            for ip in ips:
                address = ipaddress.ip_address(ip)
                if not address.is_loopback and not address.is_link_local:
                    await self.ns(
                        pid,
                        "ip",
                        "-6" if address.version == 6 else "-4",
                        "address",
                        "replace",
                        str(address) + ("/128" if address.version == 6 else "/32"),
                        "dev",
                        "lo",
                    )
            if validation:
                for binary in ["iptables", "ip6tables"]:
                    await self.ns(pid, binary, "-A", "OUTPUT", "-o", "lo", "-j", "ACCEPT")
                    await self.ns(pid, binary, "-P", "OUTPUT", "DROP")
            argv = self.pool._containers._run_argv(
                sid, rootfs, socket_dir, read_config(rootfs), point["environment"], point["resources"]
            )
            if point.get("pid1_comm") not in {"podman-init", "catatonit"} and "--init" in argv:
                # Existing checkpoints retain their original process tree and
                # mount layout. The init policy applies to fresh environments.
                argv.remove("--init")
                index = argv.index("--init-path")
                del argv[index : index + 2]
            if identity.get("hostname"):
                argv[:0] = ["--hostname", identity["hostname"]]
                if identity.get("primary_ip"):
                    argv[:0] = ["--add-host", identity["hostname"] + ":" + identity["primary_ip"]]
            # Join the namespace directly: container:<keeper> records a Podman
            # dependency, whose checkpoint exporter refuses second-generation
            # snapshots *after* stopping the source.
            argv[argv.index("--network") + 1] = f"ns:/proc/{pid}/ns/net"
            await checked([self.podman, "create", *argv])
            await checked([self.podman, "start", sid])
            await checked([self.podman, "stop", "--time", "1", sid])
            (socket_dir / "runtime.sock").unlink(missing_ok=True)
            created = await self.inspect(sid)
            static = Path(created["StaticDir"])
            if not static.is_relative_to(self.metadata_root):
                raise ValueError("Unexpected Podman metadata directory")
            if self.staging:
                await self.staging.ensure(sid, static)
            with tarfile.open(archive, "r:*") as tar:
                members = [
                    m
                    for m in tar
                    if m.name.split("/", 1)[0]
                    in {"checkpoint", "artifacts", "devshm-checkpoint.tar", "stats-dump"}
                ]
                tar.extractall(static, members=members, filter="data")
            (static / "network.status").write_text("{}")
            restored = await checked(
                [
                    self.podman,
                    "container",
                    "restore",
                    "--keep",
                    "--file-locks",
                    "--tcp-established",
                    "--ignore-static-ip",
                    "--ignore-static-mac",
                    sid,
                ]
            )
            active = await self.inspect(sid)
            await self.ns(active["State"]["Pid"], "ip", "link", "set", "lo", "up")
            sandbox = self.pool._handle(sid, "")
            self.pool._sandboxes.pop(keeper.require_id())
            self.pool._sandboxes[sid] = sandbox
            self.pool._sandboxes[keeper.require_id()] = keeper
            self.pool._online[sid] = True
            self.keepers[sid] = keeper
            self.identities[sid] = identity
            await self.pool._containers.wait_for_runtime(sandbox)
            await self.clear_staging(sid)
            result = dict(
                container=sid,
                network_keeper=keeper.require_id(),
                rootfs=str(rootfs),
                socket_dir=str(socket_dir),
                network_addresses=ips,
                restore_seconds=restored["seconds"],
                fork_total_seconds=time.monotonic() - started,
                validation=validation,
            )
            (directory / "fork.json").write_text(json.dumps(result, indent=2) + "\n")
            return sandbox, result
        except BaseException as error:
            (directory / "fork-error.json").write_text(json.dumps({"error": str(error)}, indent=2) + "\n")
            await self.logs(sid, directory)
            if sid in self.pool._sandboxes:
                await self.pool.destroy(self.pool._sandboxes[sid])
            else:
                if self.staging:
                    await self.staging.release(sid)
                await self.pool._containers.discard(sid)
                if keeper:
                    await self.pool.destroy(keeper)
            raise
