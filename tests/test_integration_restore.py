"""Opt-in real CRIU test; each run owns its own Btrfs subdirectory and container IDs."""

import asyncio
import json
import os
from pathlib import Path
import shlex
import time

import pytest

from agentfork.config import PACKAGE
from agentfork.criu_backend import CriuBtrfs
from agentfork_sandbox import PodmanPool

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("AGENTFORK_INTEGRATION") != "1",
        reason="explicit integration host configuration required",
    ),
]

SERVER = """from http.server import BaseHTTPRequestHandler,HTTPServer
counter=41
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  global counter
  if self.path == '/inc':counter+=100
  value=str(counter).encode();self.send_response(200);self.end_headers();self.wfile.write(value)
 def log_message(self,*args):pass
HTTPServer(('127.0.0.1',18766),Handler).serve_forever()
"""


def test_memory_files_and_two_generations_restore():
    async def check():
        workspace = Path(os.environ["AGENTFORK_TEST_WORKSPACE"]).resolve()
        store = Path(os.environ["AGENTFORK_TEST_STORE"]) / ("release-test-" + str(time.time_ns()))
        store.mkdir()
        output = Path(os.environ["AGENTFORK_TEST_OUTPUT"])
        output.mkdir(parents=True, exist_ok=True)
        wrapper = Path(os.environ["AGENTFORK_TEST_PODMAN"])
        pool = PodmanPool(store, Path(os.environ["AGENTFORK_TEST_RUNTIME"]), podman_bin=str(wrapper))

        # No host bridges or host firewall changes are needed by this local-only probe.
        async def no_network(sid):
            pass

        pool._containers._create_network = no_network
        pool._containers._remove_network = no_network
        original = pool._containers._run_argv

        def argv(*args, **kwargs):
            values = original(*args, **kwargs)
            values[values.index("--network") + 1] = "none"
            return values

        pool._containers._run_argv = argv
        backend = CriuBtrfs(
            pool,
            workspace,
            "docker.io/library/python:3.11-slim",
            podman=wrapper,
            metadata_root=Path(os.environ["AGENTFORK_TEST_METADATA"]),
            config_file=PACKAGE / "resources/criu.conf",
            init_process=True,
            compressed_staging=True,
        )
        points = []
        checks = {}
        finished = False

        async def shell(sandbox, command):
            result = await sandbox.call("shell", command=command)
            assert not result.is_error, result.output
            return result.output.strip()

        async def counter(sandbox, route="state"):
            script = (
                "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:18766/"
                + route
                + "').read().decode())"
            )
            return int(await shell(sandbox, "python3 -c " + shlex.quote(script)))

        try:
            parent = await pool.spawn(backend.image)
            setup = (
                "from pathlib import Path\nimport subprocess,time,urllib.request\np=Path('/tmp/agentfork-counter.py');p.write_text("
                + repr(SERVER)
                + ")\nsubprocess.Popen(['python3',str(p)],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)\nfor _ in range(100):\n try:urllib.request.urlopen('http://127.0.0.1:18766/state').close();break\n except OSError:time.sleep(.05)\nelse:raise RuntimeError('server startup failed')\n"
            )
            await shell(parent, "python3 -c " + shlex.quote(setup))
            await shell(parent, "printf before > /tmp/agentfork-file")
            first = await backend.capture(parent, output / "first")
            points.append(first)
            checks["actual_child_tool_probe"] = first["validation"]["tool_execution_verified"]
            assert await counter(parent, "inc") == 141
            await shell(parent, "printf parent > /tmp/agentfork-file")
            child, _ = await backend.fork(first, output / "child")
            checks["process_memory_restored"] = await counter(child) == 41
            checks["filesystem_restored"] = await shell(child, "cat /tmp/agentfork-file") == "before"
            await counter(child, "inc")
            await shell(child, "printf child > /tmp/agentfork-file")
            checks["parent_isolation"] = (
                await counter(parent) == 141 and await shell(parent, "cat /tmp/agentfork-file") == "parent"
            )
            second = await backend.capture(child, output / "second")
            points.append(second)
            grandchild, _ = await backend.fork(second, output / "grandchild")
            checks["second_generation_memory"] = await counter(grandchild) == 141
            checks["second_generation_filesystem"] = (
                await shell(grandchild, "cat /tmp/agentfork-file") == "child"
            )
            await counter(grandchild, "inc")
            checks["second_generation_isolation"] = (
                await counter(child) == 141 and await counter(grandchild) == 241
            )
            assert all(checks.values()), checks
            finished = True
        finally:
            await pool.close()
            for point in reversed(points):
                await pool.delete_snapshot(point["disk_snapshot"])
            checks["staging_journals_released"] = not list((store / "criu-staging").glob("*.binding.json"))
            (output / "result.json").write_text(
                json.dumps(
                    {"passed": finished and all(checks.values()), "checks": checks, "real_model_calls": 0},
                    indent=2,
                )
                + "\n"
            )

    asyncio.run(check())
