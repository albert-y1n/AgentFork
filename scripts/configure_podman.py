"""Create workspace-local Podman configuration, without changing host services."""

import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil


def main():
    root = Path(os.environ.get("AGENTFORK_HOME", Path.cwd())).resolve()
    wrapper = root / "tools/podman-agentfork"
    if wrapper.exists():
        raise SystemExit("Refusing to replace existing Podman configuration: " + str(wrapper))
    binary = shutil.which("podman")
    if not binary:
        raise SystemExit("Install Podman and runc first; podman is not on PATH")
    runc = shutil.which("runc")
    if not runc:
        raise SystemExit("runc is required for the CRIU backend")
    state = root / "storage/podman"
    state.mkdir(parents=True, exist_ok=True)
    tools = root / "tools"
    tools.mkdir(exist_ok=True)
    identity = hashlib.sha256(str(root).encode()).hexdigest()[:12]
    runroot = "/run/agentfork-" + identity
    conf = state / "containers.conf"
    # JSON quoted strings are also valid basic strings in the generated TOML.
    quote = json.dumps
    conf.write_text(
        '[network]\nnetwork_backend = "netavark"\nnetwork_config_dir = '
        + quote(str(state / "networks"))
        + '\n\n[engine]\ncgroup_manager = "cgroupfs"\nruntime = "runc"\nevents_logger = "file"\n'
        + "tmp_dir = "
        + quote(runroot + "/libpod")
        + "\nstatic_dir = "
        + quote(str(state / "libpod"))
        + "\nvolume_path = "
        + quote(str(state / "volumes"))
        + "\n[engine.runtimes]\nrunc = ["
        + quote(runc)
        + "]\n"
    )
    storage = state / "storage.conf"
    storage.write_text(
        '[storage]\ndriver = "overlay"\ngraphroot = '
        + quote(str(state / "graph"))
        + "\nrunroot = "
        + quote(runroot + "/run")
        + "\n"
    )
    wrapper.write_text(
        "#!/bin/sh\nset -eu\nexport CONTAINERS_CONF="
        + shlex.quote(str(conf))
        + "\nexport CONTAINERS_STORAGE_CONF="
        + shlex.quote(str(storage))
        + '\nif [ "$(id -u)" = 0 ] && [ "${1:-}" = unshare ]; then shift; exec "$@"; fi\n'
        + 'if [ "${1:-}" = run ] || [ "${1:-}" = create ]; then action=$1; shift; exec '
        + shlex.quote(binary)
        + ' "$action" --pids-limit=-1 "$@"; fi\nexec '
        + shlex.quote(binary)
        + ' "$@"\n'
    )
    wrapper.chmod(0o755)
    print("Created workspace-local Podman wrapper:", wrapper)


if __name__ == "__main__":
    main()
