# Validation, coverage, and limits

The backend uses conservative preflight followed by a real child restore. It does not predict support solely from a tool name or claim that CRIU can serialize arbitrary Linux state.

## Evidence

The standalone release passes 177 unit/regression tests (with the host-dependent integration test skipped by default), its real integration test, lint/format checks, and both Go test packages. The development deployment's pre-release suite passed 184 tests. Its trajectory indexes recorded 4,169 historically restore-validated checkpoint entries when audited on 2026-11-06. That count includes historical entries and does **not** mean all those filesystem snapshots are still retained, or that they passed this release's stronger shell-execution probe.

Earlier real, isolated restore probes exercised these scenarios:

| State or workload | Evidence and support boundary |
| --- | --- |
| Files, process memory, file offsets, shared memory | Restored state and independent sibling mutations checked |
| Local TCP service connections | Both endpoints in the captured process tree; restored in independent namespaces |
| Browser page state and local WebSockets | Dedicated live-browser probes; arbitrary browser/device combinations are not guaranteed |
| CPU PyTorch training with local Gloo workers | Weights, RNG, loss and continuing collective operations checked in dedicated probes |
| QEMU userspace emulation with TCG | Guest RAM/registers and continued execution checked; not KVM device migration |
| POSIX semaphores | Restored operations, sibling independence and repeated capture checked |
| Native actor context and restart control | Tool-result prefix integrity, reasoning/tool arguments, output-limit continuation and single-use branch instructions tested |
| Compressed memory exports and staging | Byte-exact materialization, second-generation restore and cleanup checked |

Historical successful probes are compatibility evidence for the tested host, not a support certification for a new host. The source archive contains tests, not private research trajectories or their original memory payloads. [Run the included integration test](integration.md) on your own host.

## Release hardening

This release adds a real shell execution probe after child restoration, rejects empty tool lists, allows application restore probes, distinguishes current payload availability from historical validation, rejects connected external UDP peers, and preserves completed-task filesystem snapshots by default.

An independent real restore test of the renamed release also exposed Linux Unix-socket path length limits in deep workspaces. The pool now automatically uses a short private socket directory for long default paths and rejects an explicitly overlong socket path before container creation. The real test then passed all eight checks: tool execution, process memory, filesystem state, parent isolation, second-generation memory, second-generation filesystem, second-generation isolation, and staging journal cleanup, with zero model calls.

The deployment previously had a cleanup bug capable of removing inherited snapshots when preparation failed before rollouts were initialized. The preservation guards and regression tests are included. Those guards prevent the known future failure; they cannot reconstruct snapshots already lost. Retention is necessary, and a host checkpoint store is not a substitute for an independent backup.

## Unsupported or conditional cases

| Case | Behavior or limitation |
| --- | --- |
| External TCP or connected external UDP | Rejected; the external peer is outside the snapshot |
| GPU/CUDA and device-driver state | Not supported or validated by this backend |
| Multiple containers, remote databases and distributed peers | No coordinated atomic snapshot; excluded from the benchmark adapter |
| Attached debugger or active exec outside PID 1 tree | Rejected conservatively |
| Nested Btrfs subvolumes inside a task filesystem | Refused by the filesystem snapshot implementation |
| Shared host mounts, external filesystems and remote side effects | Outside the captured root filesystem; not rolled back |
| Unsupported kernel resources, CRIU failures or parent resume failure | No new checkpoint published; explicit error and recovery path |
| Cross-host or changed runtime/kernel migration | No portability guarantee; verify and restore-test again |
| Full disk, lost mount, deleted payload or missing runtime | Checkpoint becomes unavailable; do not infer availability from old metadata |
| Application state corruption with a healthy tool runtime | Requires an application-specific probe to detect |

There is no measured universal false-positive/false-negative rate, and a successful restore does not prove future task success. The main remaining constraints are resource coverage, external-state boundaries, application validation, retention, and disk capacity.
