# Third-party code

The vendored `agentfork_sandbox/`, Go runtime, `agentfork/prompts.py`, `agentfork/request_size.py`, and review prompt YAML originate from the Apache-2.0 project identified in NOTICE, revision `6b73b753d27d1829630f0f840e11d749f4f49dac`.

Changes include package and runtime module renaming, AgentFork interface names, local resource loading, integration with the checkpoint backend, short Unix socket path handling, formatting, and documentation. Original copyright and attribution notices remain in NOTICE; the Apache-2.0 license is in LICENSE.

The runtime build downloads a checksum-pinned ripgrep archive from its upstream release using `runtime/tools/ripgrep/fetch.sh`. That release and other Go/Python dependencies keep their own licenses. Claude Code, task datasets, Docker/Podman image contents, model services and research trajectories are not redistributed.
