"""Portable workspace configuration; credentials are read only from the environment."""

import os
from pathlib import Path

PACKAGE = Path(__file__).resolve().parent
ROOT = Path(os.environ.get("AGENTFORK_HOME", Path.cwd())).expanduser().resolve()


def task_root(workspace=ROOT):
    return Path(os.environ.get("AGENTFORK_TASKS", Path(workspace) / "datasets/terminal-bench")).resolve()


def load_provider():
    names = {"api_key": "AGENTFORK_API_KEY", "base_url": "AGENTFORK_BASE_URL", "model": "AGENTFORK_MODEL"}
    missing = [name for name in names.values() if not os.environ.get(name)]
    if missing:
        raise ValueError("Set the following environment variables: " + ", ".join(missing))
    result = {field: os.environ[name] for field, name in names.items()}
    result["base_url"] = result["base_url"].rstrip("/")
    return result


def task_inventory(directory=None):
    """Discover local Harbor task definitions; never download or modify a task."""
    import hashlib
    import tomllib

    folder = Path(directory) if directory else task_root()
    rows = []
    for path in sorted(folder.glob("*/task.toml")):
        raw = path.read_bytes()
        value = tomllib.loads(raw.decode())
        env = value.get("environment", {})
        rows.append(
            {
                "task": path.parent.name,
                "path": str(path.parent.resolve()),
                "gpus": env.get("gpus", 0),
                "compose": bool(list(path.parent.glob("environment/*compose*.y*ml"))),
                "image": env.get("docker_image"),
                "hash": hashlib.sha256(raw).hexdigest(),
            }
        )
    if not rows:
        raise ValueError("No Harbor task.toml files found in " + str(folder))
    return rows
