# Checkpoint semantics

## Admission, capture, and publication

1. The gateway observes a completed tool-result boundary before forwarding the next model request. A request with no completed tool results does not create a checkpoint.
2. The corresponding Claude native JSONL prefix must be durable and contain the required tool-result IDs. Control messages inserted only to resume Claude are removed without deleting real task messages.
3. The current Claude invocation is stopped and task daemons remain in the container's PID 1 process tree. The next model request has not been sent.
4. Preflight rejects detached `exec` trees, attached tracers, external TCP peers, connected external UDP peers, and TCP peers outside the captured process tree. Network gating prevents new off-container TCP connections and off-container UDP traffic during classification and capture.
5. CRIU stops and exports the process tree. While it is stopped, Btrfs creates a read-only filesystem snapshot. The parent is then restored. Failed export or source restore has an explicit error/recovery path.
6. A disposable child is restored with old application IPs aliased inside a separate network namespace. Off-container egress is blocked before restore. A nonempty tool list and a successful actual shell invocation are required. An optional application probe can reject an otherwise healthy runtime.
7. Only successful capture plus validation publishes a branch point. Failure logs remain available and the controller either continues the intact parent without a new point, or restores an older coherent point if the parent stopped.

The actor's native session, exact model prefix, request index, elapsed budget, and branch instruction state are resumed together. Branch instructions must be consumed once across restarts.

## Recovery materials

A CRIU gzip export is **not** a full backup of the environment. Recovery requires all of:

- The matching read-only Btrfs root filesystem snapshot and image configuration.
- The original memory export, or its verified reference-compressed representation and full dictionary export.
- The exact native transcript prefix and saved model request.
- The pinned runtime binary where recorded, plus compatible host kernel, CPU features, Podman/runc/CRIU and mount configuration.

Reference compression never chains patches: its dictionary remains a full export. Restores verify original SHA256 bytes. Reader leases serialize compaction and restores. Optional external payload stores require retaining their archive pins as well as the source archive directory.

`agentfork audit` checks current material availability, separate from a historical successful restore. It does not emulate a host or execute an application probe. `backend.fork` performs the actual restoration.

## Application probes

```python
async def app_probe(child):
    result = await child.call("shell", command="curl -fsS http://127.0.0.1:5000/health")
    return not result.is_error and result.output.strip() == "ok"

backend = CriuBtrfs(pool, workspace, image, validation_probe=app_probe)
```

Probes run in a throwaway child. They must use only captured local state, fail explicitly with `False` or an exception, and finish promptly. Runtime health alone cannot establish application correctness.

## Retention and time

The library retains snapshots until explicitly deleted. The benchmark controller keeps completed task snapshots by default. The opt-in discard flag removes those filesystem snapshots and therefore ends recoverability, even though transcripts and exported memory may remain.

Restored timers, wall-clock reads, external locks, TTLs, credentials and remote transactions do not become transactional. Isolation prevents validation from replaying network side effects, but cannot undo earlier external actions. There is no guarantee of bit-identical future behavior or recovery after arbitrary host/disk failure.
