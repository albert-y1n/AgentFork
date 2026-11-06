# Reproduce a real restore

This test does not use a model API or a benchmark verifier. It creates a private Btrfs store subdirectory, starts a local Python service inside a container, checkpoints its process memory and files, forks a child, and checkpoints/restores a second generation. It checks isolation and releases its own temporary mounts and snapshots.

First build the runtime and configure the workspace-local Podman wrapper as described in README. Run on a trusted Linux host with the required root privileges and a working `criu check`. The configured Podman store must be separate from other experiments if you want to avoid shared image-cache activity.

```bash
export AGENTFORK_HOME="$PWD"
export AGENTFORK_TEST_WORKSPACE="$AGENTFORK_HOME"
export AGENTFORK_TEST_STORE=/your/mounted/btrfs/store
export AGENTFORK_TEST_OUTPUT="$AGENTFORK_HOME/runs/restore-test"
export AGENTFORK_TEST_PODMAN="$AGENTFORK_HOME/tools/podman-agentfork"
export AGENTFORK_TEST_METADATA="$AGENTFORK_HOME/storage/podman/graph/overlay-containers"
export AGENTFORK_TEST_RUNTIME="$AGENTFORK_HOME/runtime/agentfork-runtime"
AGENTFORK_INTEGRATION=1 .venv/bin/pytest -q tests/test_integration_restore.py -s
```

The host may download the Python container image. Container probe traffic remains local: no host bridges are created by this test, and model endpoints are never called. Results are written under `AGENTFORK_TEST_OUTPUT`; test failure is never reported as success just because cleanup passed.

The eight result checks cover child tool execution, restored process memory, restored files, parent isolation, second-generation memory, second-generation files, second-generation isolation, and staging-journal cleanup. This small test is a prerequisite check, not a substitute for testing your application's resources and restore probe.
