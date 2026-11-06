"""Fork a restore-tested environment; no model API or benchmark dataset needed."""

import argparse
import asyncio
from pathlib import Path

from agentfork.config import ROOT
from agentfork.criu_backend import CriuBtrfs
from agentfork_sandbox import PodmanPool


async def main(args):
    pool = PodmanPool(args.store, args.runtime, podman_bin=str(ROOT / "tools/podman-agentfork"))
    backend = CriuBtrfs(pool, ROOT, args.image, init_process=True, compressed_staging=True)
    point = None
    try:
        parent = await pool.spawn(args.image)
        result = await parent.call("shell", command="printf before > /tmp/agentfork-example")
        assert not result.is_error
        point = await backend.capture(parent, args.output / "checkpoint")
        await parent.call("shell", command="printf parent > /tmp/agentfork-example")
        child, _ = await backend.fork(point, args.output / "child")
        result = await child.call("shell", command="cat /tmp/agentfork-example")
        assert not result.is_error and result.output == "before"
        await child.call("shell", command="printf child > /tmp/agentfork-example")
        result = await parent.call("shell", command="cat /tmp/agentfork-example")
        assert not result.is_error and result.output == "parent"
        print("Verified restored file state, tool execution, and independent parent/child writes.")
    finally:
        await pool.close()
        # This example deliberately releases its disposable checkpoint.
        if point:
            await pool.delete_snapshot(point["disk_snapshot"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--image", default="docker.io/library/python:3.11-slim")
    parser.add_argument("--output", type=Path, default=ROOT / "runs/fork-example")
    asyncio.run(main(parser.parse_args()))
