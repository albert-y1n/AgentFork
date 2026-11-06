"""Claude Code rollouts checkpointed by AgentFork's real Podman/btrfs backend."""

from __future__ import annotations

import asyncio
import os
import gzip
import hashlib
import json
import re
import signal
import time
import uuid
from copy import deepcopy
from pathlib import Path

import aiohttp
from aiohttp import web
from agentfork.claude_context import restore_model_prefix, split_resume_control, text_only
from agentfork.protocol import native_cut, result_ids, send_message
from agentfork.provider_io import fetch as fetch_provider, ProviderError, RETRY_STATUS

from agentfork.config import ROOT

CLAUDE = Path(
    os.environ.get(
        "AGENTFORK_CLAUDE_BINARY",
        ROOT / "tools/claude-code/node_modules/@anthropic-ai/claude-code/bin/claude.exe",
    )
)

MAX_OUTPUT_TOKENS = int(os.environ.get("AGENTFORK_MAX_OUTPUT_TOKENS", "128000"))
PROVIDER_POLL_SECONDS = 10
NATIVE_TIMEOUT_ENV = {
    "API_TIMEOUT_MS": "3600000",
    "CLAUDE_STREAM_FIRST_BYTE_TIMEOUT_MS": "1800000",
    "CLAUDE_STREAM_IDLE_TIMEOUT_MS": "1800000",
    "CLAUDE_BYTE_STREAM_IDLE_TIMEOUT_MS": "1800000",
}


def write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def save_request(path: Path, body: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        json.dump(body, stream, ensure_ascii=False)


def read_request(path: str | Path) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        return json.load(stream)


def completed_tool_boundary(messages):
    durable = [m for m in messages if m.get("role") != "system"]
    if not durable or durable[-1].get("role") != "user":
        return None
    content = durable[-1].get("content", [])
    if not isinstance(content, list):
        return None
    ids = tuple(sorted(b.get("tool_use_id", "") for b in content if b.get("type") == "tool_result"))
    return ids or None


async def process(
    argv,
    *,
    timeout=1800,
    output: Path | None = None,
    stderr_output: Path | None = None,
    on_started=None,
):
    proc = await asyncio.create_subprocess_exec(
        *map(str, argv),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    if on_started:
        on_started(proc)
    try:
        if output is None:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout)
        else:
            output.parent.mkdir(parents=True, exist_ok=True)

            async def drain_stdout():
                opener = gzip.open if output.suffix == ".gz" else open
                with opener(output, "wb") as stream:
                    while chunk := await proc.stdout.read(65536):
                        stream.write(chunk)

            async def drain_stderr():
                data = bytearray()
                if stderr_output:
                    stderr_output.parent.mkdir(parents=True, exist_ok=True)
                stream = stderr_output.open("wb") if stderr_output else None
                try:
                    while chunk := await proc.stderr.read(65536):
                        data.extend(chunk)
                        if len(data) > 4 * 1024**2:
                            del data[: -4 * 1024**2]
                        if stream:
                            stream.write(chunk)
                            stream.flush()
                finally:
                    if stream:
                        stream.close()
                return bytes(data)

            _, stderr = await asyncio.wait_for(asyncio.gather(drain_stdout(), drain_stderr()), timeout)
            await proc.wait()
            stdout = b""
        return (
            proc.returncode,
            stdout.decode(errors="replace"),
            stderr.decode(errors="replace"),
        )
    except BaseException:
        if proc.returncode is None:
            proc.send_signal(signal.SIGINT)
            try:
                await asyncio.wait_for(proc.wait(), 10)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
        raise


def clean_native(data: bytes, marker: str | None) -> bytes:
    """Remove only this branch's headless resume exchange and repair UUID links."""
    rows = [json.loads(line) for line in data.splitlines()]
    deleted: set[str] = set()
    previous_message = None
    for index, row in enumerate(rows):
        message = row.get("message", {})
        count, remainder = (
            split_resume_control(message, marker) if marker and row.get("type") == "user" else (0, message)
        )
        if count > 1:
            raise ValueError("Duplicated native resume control marker")
        if count:
            if remainder is None:
                deleted.add(row.get("uuid"))
            else:
                row["message"] = remainder
            if previous_message is not None:
                prior = rows[previous_message]
                if (
                    prior.get("type") == "assistant"
                    and text_only(prior.get("message", {})) == "No response requested."
                ):
                    deleted.add(prior.get("uuid"))
        if row.get("type") in ("user", "assistant"):
            previous_message = index
    parents = {row.get("uuid"): row.get("parentUuid") for row in rows if row.get("uuid")}
    kept = []
    for row in rows:
        if row.get("uuid") in deleted:
            continue
        parent = row.get("parentUuid")
        while parent in deleted:
            parent = parents.get(parent)
        if "parentUuid" in row:
            row["parentUuid"] = parent
        kept.append(row)
    return b"".join((json.dumps(row, ensure_ascii=False) + "\n").encode() for row in kept)


def cli_result(path: Path):
    final = None
    try:
        with gzip.open(path, "rt") as stream:
            for line in stream:
                if '"type":"result"' not in line and '"type": "result"' not in line:
                    continue
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if row.get("type") == "result":
                    final = row
    except (OSError, EOFError):
        pass
    return final


def response_message(raw: bytes) -> tuple[dict, dict, bool]:
    message: dict = {}
    usage: dict = {}
    blocks: dict[int, dict] = {}
    partial: dict[int, str] = {}
    complete = False
    stream_error = False
    sse_type = ""
    for line in raw.decode(errors="replace").splitlines():
        if not line:
            sse_type = ""
        if line.startswith("event:"):
            sse_type = line[6:].strip()
        if not line.startswith("data:"):
            continue
        try:
            event = json.loads(line[5:].strip())
        except ValueError:
            continue
        kind = event.get("type")
        if sse_type == "error" or kind == "error" or ("error" in event and not kind):
            stream_error = True
        if kind == "message_start":
            message = deepcopy(event["message"])
            usage.update(message.get("usage", {}))
        elif kind == "content_block_start":
            blocks[event["index"]] = deepcopy(event["content_block"])
        elif kind == "content_block_delta":
            index, delta = event["index"], event["delta"]
            block = blocks.setdefault(index, {})
            if delta.get("type") == "text_delta":
                block["text"] = block.get("text", "") + delta["text"]
            elif delta.get("type") == "thinking_delta":
                block["thinking"] = block.get("thinking", "") + delta["thinking"]
            elif delta.get("type") == "signature_delta":
                block["signature"] = block.get("signature", "") + delta["signature"]
            elif delta.get("type") == "input_json_delta":
                partial[index] = partial.get(index, "") + delta["partial_json"]
        elif kind == "message_delta":
            usage.update(event.get("usage", {}))
            message.update(event.get("delta", {}))
        elif kind == "message_stop":
            complete = True
    for index, value in partial.items():
        try:
            blocks[index]["input"] = json.loads(value)
        except ValueError:
            blocks[index]["input"] = {"_malformed_json": value}
    message["content"] = [blocks[k] for k in sorted(blocks)]
    return message, usage, complete and not stream_error


class Rollout:
    def __init__(
        self,
        engine,
        task,
        name: str,
        directory: Path,
        *,
        checkpoints=False,
        origin=None,
        starting_turn=None,
        max_turns=None,
        resume=None,
    ):
        self.engine, self.task, self.name, self.directory = (
            engine,
            task,
            name,
            directory,
        )
        self.directory.mkdir(parents=True, exist_ok=True)
        self.checkpoints_enabled = checkpoints
        self.resume = resume
        self.report_origin = origin
        self.origin, self.starting_turn = (
            (resume["point"] if resume else origin),
            starting_turn,
        )
        # A resumable branch point follows completed tool results, including
        # the reviewer's initial commands. They are already in its native
        # history and filesystem state; restarting the controller must not
        # inject or execute that bootstrap turn a second time.
        self.starting_turn_sent = bool(resume and starting_turn is not None)
        self.max_turns = max_turns
        self.marker = "__TB40_RESUME_" + uuid.uuid4().hex + "__" if self.origin else None
        self.token = uuid.uuid4().hex
        self.session_id = str(uuid.uuid4())
        self.sandbox = None
        self.request_count = resume["request_count"] if resume else 0
        self.resume_request = self.request_count
        self.points: list[dict] = list(resume["points"]) if resume else []
        # Unpublished captures also own their artifact directory. A resumed
        # actor must not overwrite the failed capture immediately after its
        # last valid point, including that capture's native transcript.
        existing_steps = (
            [
                int(path.name)
                for path in (self.directory / "checkpoint-artifacts").glob("*")
                if path.is_dir() and path.name.isdigit()
            ]
            if resume
            else []
        )
        self.local_step = max([p["step"] for p in self.points] + existing_steps, default=0)
        self.api: list[dict] = list(resume["api"]) if resume else []
        self.elapsed_before = (
            max(0, resume.get("elapsed_seconds", time.time() - resume["started_at"])) if resume else 0
        )
        if resume:
            write_json(self.directory / "api-usage.json", self.api)
        self.latest_request = None
        self.latest_response = None
        self.checkpoint_failures: list[str] = []
        self.last_tool_boundary = None
        self.context_origin = self.origin
        self.memory_backend = getattr(engine, "memory_backend", None)
        self.handoff_event = asyncio.Event()
        self.pending_capture = None
        self.restart_count = (
            max(
                (
                    int(p.name.split(".")[0]) + 1
                    for p in (self.directory / "cli-segments").glob("*.jsonl.gz")
                    if p.name.split(".")[0].isdigit()
                ),
                default=0,
            )
            if resume
            else 0
        )
        self.provider_recovery_restarts = 0
        self.skip_restart_capture = False
        self.environment_recoveries = []
        self.started_at = None

    async def recover_environment(self, directory):
        """Roll back both environment and conversation after source restore fails."""
        old = self.sandbox
        errors = []
        candidates = list(reversed(self.points))
        if self.origin and all(p.get("snapshot_id") != self.origin.get("snapshot_id") for p in candidates):
            candidates.append(self.origin)
        for point in candidates:
            if point.get("eligible") is False or not point.get("memory_checkpoint"):
                continue
            try:
                restored, _ = await self.memory_backend.fork(
                    point["memory_checkpoint"], directory / "fallback" / str(point["step"])
                )
            except Exception as error:
                errors.append(str(error))
                continue
            self.sandbox = restored
            self.context_origin = point
            self.marker = "__TB40_RESUME_" + uuid.uuid4().hex + "__"
            self.skip_restart_capture = True
            if point is self.origin and self.starting_turn and not self.resume:
                self.starting_turn_sent = False
            self.environment_recoveries.append(
                {"restored_step": point["step"], "failed_step": self.local_step, "time": time.time()}
            )
            write_json(self.directory / "environment-recoveries.json", self.environment_recoveries)
            if old and old.sandbox_id:
                try:
                    await self.engine.pool.destroy(old)
                except Exception as error:
                    self.environment_recoveries[-1]["old_container_cleanup_error"] = str(error)
                    write_json(self.directory / "environment-recoveries.json", self.environment_recoveries)
            return
        raise RuntimeError("No prior checkpoint could restore the failed source: " + str(errors[-2:]))

    async def capture(self, body: dict, request_path: Path):
        if not self.checkpoints_enabled:
            return
        boundary = completed_tool_boundary(body["messages"])
        if boundary is None or boundary == self.last_tool_boundary:
            return
        required = result_ids(body["messages"])
        if not required:
            return
        self.local_step += 1
        rootfs = self.engine.pool.store.sandboxes / self.sandbox.require_id()
        native_root = rootfs / "opt/tb40-claude/projects"
        deadline = time.monotonic() + 10
        cut, candidate = None, None
        while time.monotonic() < deadline:
            files = sorted(
                native_root.glob("*/*.jsonl"),
                key=lambda p: p.stat().st_mtime_ns,
                reverse=True,
            )
            for path in files:
                try:
                    cut = native_cut(path, required)
                except (ValueError, OSError):
                    continue
                if cut:
                    candidate = path
                    break
            if cut:
                break
            await asyncio.sleep(0.05)
        if not cut:
            self.checkpoint_failures.append(f"step {self.local_step}: matching native results not durable")
            return
        cut = clean_native(cut, self.marker)
        checkpoint_directory = self.directory / "checkpoint-artifacts" / f"{self.local_step:05d}"
        checkpoint_directory.mkdir(parents=True, exist_ok=True)
        native = checkpoint_directory / "native.jsonl"
        native.write_bytes(cut)
        (self.directory / "native.jsonl").write_bytes(cut)
        point = {
            "attempt": self.name,
            "step": self.local_step,
            "request_path": str(request_path.resolve()),
            "native_path": str(native.resolve()),
            "native_relative": str(candidate.relative_to(rootfs)),
            "session_id": candidate.stem,
            "byte_length": len(cut),
            "sha256": hashlib.sha256(cut).hexdigest(),
            "message_count": len(body["messages"]),
            "cwd": next(
                (
                    row["cwd"]
                    for row in reversed([json.loads(line) for line in cut.splitlines()])
                    if isinstance(row.get("cwd"), str) and row["cwd"].startswith("/")
                ),
                self.task.cwd,
            ),
        }
        if self.memory_backend:
            # No upstream model request has started. The controller terminates
            # this CLI invocation, leaving task daemons adopted by PID 1, then
            # snapshots the environment with no actor API socket or exec root.
            released = asyncio.Event()
            self.pending_capture = (point, checkpoint_directory, boundary, released)
            self.handoff_event.set()
            return released
        started = time.monotonic()
        snapshot = await self.engine.pool.snapshot(self.sandbox)
        self.task.snapshots.append(snapshot.id)
        point.update(
            snapshot_id=snapshot.id, capture_seconds=time.monotonic() - started, snapshot_backend="btrfs-disk"
        )
        self.points.append(point)
        self.last_tool_boundary = boundary
        write_json(self.directory / "checkpoints.json", self.points)

    async def handle(self, request: web.Request):
        raw_body = await request.json()
        if raw_body.get("model") != self.engine.model:
            raise web.HTTPBadRequest(text="Unexpected model; fallback is disabled")
        if request.path.endswith("count_tokens"):
            return await self.engine.count_tokens(raw_body)
        index = self.request_count
        self.request_count += 1
        body = deepcopy(raw_body)
        if self.context_origin:
            save_request(self.directory / "requests" / f"{index:05d}-raw.json.gz", raw_body)
            body = restore_model_prefix(body, read_request(self.context_origin["request_path"]), self.marker)
        body["model"] = self.engine.model
        body["temperature"] = 1.0
        body["thinking"] = {"type": "adaptive"}
        body["output_config"] = {**body.get("output_config", {}), "effort": "max"}
        request_path = self.directory / "requests" / f"{index:05d}.json.gz"
        save_request(request_path, body)
        self.latest_request = body
        skip_capture = self.skip_restart_capture
        self.skip_restart_capture = False
        if index > 0 and not skip_capture and not (self.resume and index == self.resume_request):
            try:
                handoff = await self.capture(body, request_path)
                if handoff:
                    # The socket is intentionally abandoned before CRIU. Never
                    # forward this request upstream or count it as model usage.
                    await handoff.wait()
                    return web.Response(status=204)
            except Exception as error:
                self.checkpoint_failures.append(f"step {self.local_step}: {type(error).__name__}: {error}")
        if not getattr(self, "starting_turn_sent", False) and self.starting_turn is not None:
            self.starting_turn_sent = True
            turn = self.starting_turn
            command = "\n".join("(\n" + command + "\n)" for command in turn["commands"])
            message = {
                "id": "msg_review_" + uuid.uuid4().hex,
                "type": "message",
                "role": "assistant",
                "model": self.engine.model,
                "content": [
                    # As in AgentFork render_reasoning(), serialize the reviewer's
                    # authored reasoning in the reasoning model's native field.
                    # This is guided text, not a claimed actor completion.
                    {"type": "thinking", "thinking": turn["content"]},
                    {
                        "type": "tool_use",
                        "id": "toolu_review_" + uuid.uuid4().hex,
                        "name": "Bash",
                        "input": {
                            "command": command,
                            "description": "Check the hypothesis, apply the repair and verify",
                            "timeout": 600000,
                        },
                    },
                ],
                "stop_reason": "tool_use",
                "stop_sequence": None,
                "usage": {"input_tokens": 0, "output_tokens": 0},
            }
            request["body"] = body
            return await send_message(request, message)
        headers = {
            "Authorization": "Bearer " + self.engine.key,
            "x-api-key": self.engine.key,
            "anthropic-version": request.headers.get("anthropic-version", "2023-06-01"),
            "Accept-Encoding": "identity",
        }
        if request.headers.get("anthropic-beta"):
            headers["anthropic-beta"] = request.headers["anthropic-beta"]
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        response_started = False
        cancellation_reason = None

        def client_disconnected():
            return request.transport is None or request.transport.is_closing()

        async def status(code):
            nonlocal response_started, cancellation_reason
            if client_disconnected():
                cancellation_reason = "native_client_disconnected"
                raise asyncio.CancelledError
            if code == 200 and not response_started:
                await response.prepare(request)
                response_started = True

        def validate(raw):
            message, usage, complete = response_message(raw)
            if not complete:
                raise ValueError("Provider SSE ended without message_stop")
            return message, usage

        async def record(row, raw):
            row["request"] = index
            if row.get("error") == "CancelledError" and cancellation_reason:
                row["cancellation_reason"] = cancellation_reason
            suffix = ".sse.gz" if row["complete"] else f"-provider-attempt-{row['provider_attempt']}.sse.gz"
            with gzip.open(self.directory / "requests" / (f"{index:05d}" + suffix), "wb") as stream:
                stream.write(raw)
            if row["complete"]:
                row["usage"] = response_message(raw)[1]
                if getattr(self.engine, "campaign_directory", None):
                    from agentfork.provider_health import save_state

                    save_state(
                        self.engine.campaign_directory, "available", source="actor_gateway", status=200
                    )
            self.api.append(row)
            write_json(self.directory / "api-usage.json", self.api)

        operation = asyncio.create_task(
            fetch_provider(
                self.engine.client,
                self.engine.base_url + "/v1/messages",
                body,
                headers,
                validate=validate,
                on_attempt=record,
                on_status=status,
            )
        )
        try:
            while True:
                try:
                    (message, usage), raw = await asyncio.wait_for(
                        asyncio.shield(operation), PROVIDER_POLL_SECONDS
                    )
                    break
                except asyncio.TimeoutError:
                    if operation.done():
                        raise
                    if client_disconnected():
                        cancellation_reason = "native_client_disconnected"
                        raise asyncio.CancelledError
                    if response_started:
                        await response.write(b": waiting for complete provider response\n\n")
            self.latest_response = message
            await response.write(raw)
            await response.write_eof()
            return response
        except Exception as error:
            if client_disconnected():
                cancellation_reason = "native_client_disconnected"
                raise asyncio.CancelledError from error
            if getattr(self.engine, "campaign_directory", None) and (
                isinstance(error, (aiohttp.ClientError, asyncio.TimeoutError))
                or isinstance(error, ProviderError)
                and error.status in RETRY_STATUS | {401, 403}
            ):
                from agentfork.provider_health import save_state

                save_state(
                    self.engine.campaign_directory,
                    "unavailable",
                    source="actor_gateway",
                    status=getattr(error, "status", None),
                    error_type=type(error).__name__,
                )
            if not response_started:
                return web.json_response(
                    {"type": "error", "error": {"type": "api_error", "message": str(error)}},
                    status=getattr(error, "status", 502),
                )
            await response.write(
                (
                    "event: error\ndata: "
                    + json.dumps(
                        {
                            "type": "error",
                            "error": {
                                "type": "overloaded_error",
                                "message": "Provider transport failed after bounded retries",
                            },
                        }
                    )
                    + "\n\n"
                ).encode()
            )
            await response.write_eof()
            return response
        finally:
            if not operation.done():
                if client_disconnected():
                    cancellation_reason = "native_client_disconnected"
                operation.cancel()
                await asyncio.gather(operation, return_exceptions=True)

    @property
    def actor_pid_file(self):
        return "/opt/tb40-claude/actor-" + self.token + ".pid"

    async def stop_actor(self, actor):
        rootfs = self.engine.pool.store.sandboxes / self.sandbox.require_id()
        pid_path = rootfs / self.actor_pid_file.lstrip("/")
        pid = int(pid_path.read_text().strip())
        if pid <= 1:
            raise ValueError("Invalid actor PID")
        for sig in ("INT", "TERM", "KILL"):
            code, _, error = await process(
                [
                    ROOT / "tools/podman-agentfork",
                    "exec",
                    "--user",
                    "root",
                    self.sandbox.require_id(),
                    "sh",
                    "-c",
                    'kill -"$1" "$2"',
                    "tb40-quiesce",
                    sig,
                    str(pid),
                ],
                timeout=15,
            )
            if code and not actor.done():
                raise RuntimeError("Cannot quiesce Claude Code: " + error[-1000:])
            try:
                await asyncio.wait_for(asyncio.shield(actor), 5)
                return
            except asyncio.TimeoutError:
                pass
        raise RuntimeError("Claude Code did not stop at its tool boundary")

    async def run_native(self):
        if not self.memory_backend:
            return await self.invoke_cli(
                self.directory / "claude-output.jsonl.gz", self.directory / "claude-stderr.txt"
            )
        segments = self.directory / "cli-segments"
        segments.mkdir(exist_ok=True)
        actor = None
        errors = []
        try:
            async with asyncio.timeout(max(1, self.task.timeout - self.elapsed_before)):
                while True:
                    self.handoff_event.clear()
                    output = segments / f"{self.restart_count:05d}.jsonl.gz"
                    actor = asyncio.create_task(self.invoke_cli(output, output.with_suffix(".stderr")))
                    boundary = asyncio.create_task(self.handoff_event.wait())
                    try:
                        await asyncio.wait([actor, boundary], return_when=asyncio.FIRST_COMPLETED)
                        if not self.handoff_event.is_set():
                            code, stdout, stderr = await actor
                            errors.append(stderr)
                            if (
                                code
                                and self.context_origin
                                and self.api
                                and not self.api[-1].get("complete")
                                and self.provider_recovery_restarts < 2
                            ):
                                self.provider_recovery_restarts += 1
                                self.restart_count += 1
                                self.marker = "__TB40_RESUME_" + uuid.uuid4().hex + "__"
                                self.skip_restart_capture = True
                                write_json(
                                    self.directory / "provider-recovery.json",
                                    {
                                        "restarts": self.provider_recovery_restarts,
                                        "source": "last completed tool-result context; existing environment retained",
                                    },
                                )
                                continue
                            return code, stdout, "\n".join(errors)
                        point, directory, tool_boundary, released = self.pending_capture
                        await self.stop_actor(actor)
                        errors.append((await actor)[2])
                        released.set()
                        # The saved native prefix predates CLI shutdown, which
                        # can append an interruption event. invoke_cli truncates
                        # back to this exact, checksummed tool-result prefix.
                        self.context_origin = point
                        self.marker = "__TB40_RESUME_" + uuid.uuid4().hex + "__"
                        self.skip_restart_capture = True
                        rolled_back = False
                        try:
                            memory = await self.memory_backend.capture(self.sandbox, directory)
                        except Exception as error:
                            if getattr(error, "unpublished_snapshot", None):
                                self.task.snapshots.append(error.unpublished_snapshot)
                            self.checkpoint_failures.append(
                                f"step {point['step']}: {type(error).__name__}: {error}"
                            )
                            write_json(
                                directory / "capture-error.json",
                                {
                                    "error": str(error),
                                    "eligible": False,
                                    "fallback": "none; actor resumes without adding a branch point",
                                },
                            )
                            # Failed CRIU dumps must leave the original running.
                            # A stopped container is an infrastructure error.
                            code, state, _ = await process(
                                [
                                    ROOT / "tools/podman-agentfork",
                                    "inspect",
                                    "--format",
                                    "{{.State.Running}}",
                                    self.sandbox.require_id(),
                                ]
                            )
                            if code or state.strip() != "true":
                                await self.recover_environment(directory)
                                write_json(
                                    directory / "capture-error.json",
                                    {
                                        "error": str(error),
                                        "eligible": False,
                                        "fallback": "restored prior coherent environment and native conversation",
                                        "restored_step": self.context_origin["step"],
                                    },
                                )
                                rolled_back = True
                        else:
                            point.update(
                                snapshot_id=memory["disk_snapshot"],
                                memory_checkpoint=memory,
                                snapshot_backend="btrfs-criu",
                                capture_seconds=memory["capture_total_seconds"],
                            )
                            self.task.snapshots.append(memory["disk_snapshot"])
                            self.points.append(point)
                            write_json(self.directory / "checkpoints.json", self.points)
                        self.last_tool_boundary = (
                            completed_tool_boundary(
                                read_request(self.context_origin["request_path"])["messages"]
                            )
                            if rolled_back
                            else tool_boundary
                        )
                        self.restart_count += 1
                        write_json(
                            self.directory / "actor-restarts.json",
                            {
                                "restarts": self.restart_count,
                                "last_step": point["step"],
                                "checkpoint_failures": self.checkpoint_failures,
                            },
                        )
                    finally:
                        boundary.cancel()
                        await asyncio.gather(boundary, return_exceptions=True)
        finally:
            if actor and not actor.done():
                actor.cancel()
                await asyncio.gather(actor, return_exceptions=True)
            if self.pending_capture:
                self.pending_capture[3].set()
            # Concatenated gzip streams preserve every invocation, including
            # controlled shutdowns; cli_result reads the final invocation.
            with (self.directory / "claude-output.jsonl.gz").open("wb") as target:
                for part in sorted(segments.glob("*.jsonl.gz")):
                    with part.open("rb") as source:
                        while chunk := source.read(1024**2):
                            target.write(chunk)

    async def invoke_cli(self, output, stderr_output):
        if self.context_origin:
            data = Path(self.context_origin["native_path"]).read_bytes()[: self.context_origin["byte_length"]]
            if hashlib.sha256(data).hexdigest() != self.context_origin["sha256"]:
                raise ValueError("Parent native history checksum changed")
            target = (
                self.engine.pool.store.sandboxes
                / self.sandbox.require_id()
                / self.context_origin["native_relative"]
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            cwd = self.context_origin.get("cwd", self.task.cwd)
            project = re.sub(r"[^A-Za-z0-9-]", "-", cwd)
            current_project = (
                self.engine.pool.store.sandboxes
                / self.sandbox.require_id()
                / "opt/tb40-claude/projects"
                / project
                / (self.context_origin["session_id"] + ".jsonl")
            )
            if current_project != target:
                current_project.parent.mkdir(parents=True, exist_ok=True)
                current_project.write_bytes(data)
                code, _, stderr = await process(
                    [
                        ROOT / "tools/podman-agentfork",
                        "exec",
                        "--user",
                        "root",
                        self.sandbox.require_id(),
                        "chown",
                        "-R",
                        self.task.user,
                        "/"
                        + str(
                            current_project.parent.relative_to(
                                self.engine.pool.store.sandboxes / self.sandbox.require_id()
                            )
                        ),
                    ]
                )
                if code:
                    raise RuntimeError("Cannot restore native project ownership: " + stderr[-1000:])
        env = {
            **NATIVE_TIMEOUT_ENV,
            "CLAUDE_CONFIG_DIR": "/opt/tb40-claude",
            "ANTHROPIC_API_KEY": self.token,
            "ANTHROPIC_BASE_URL": f"http://host.containers.internal:{self.engine.port}/{self.token}",
            "ANTHROPIC_MODEL": self.engine.model,
            "ANTHROPIC_CUSTOM_MODEL_OPTION": self.engine.model,
            "ANTHROPIC_DEFAULT_HAIKU_MODEL": self.engine.model,
            "ANTHROPIC_DEFAULT_SONNET_MODEL": self.engine.model,
            "ANTHROPIC_DEFAULT_OPUS_MODEL": self.engine.model,
            "CLAUDE_CODE_MAX_CONTEXT_TOKENS": os.environ.get("AGENTFORK_CONTEXT_TOKENS", "1000000"),
            "CLAUDE_CODE_MAX_OUTPUT_TOKENS": str(MAX_OUTPUT_TOKENS),
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            "CLAUDE_CODE_DISABLE_BACKGROUND_TASKS": "1",
            "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1",
            "IS_SANDBOX": "1",
        }
        argv = [
            str(ROOT / "tools/podman-agentfork"),
            "exec",
            "--workdir",
            self.context_origin.get("cwd", self.task.cwd) if self.context_origin else self.task.cwd,
        ]
        if self.task.user:
            argv += ["--user", self.task.user]
        for key, value in env.items():
            argv += ["--env", f"{key}={value}"]
        argv += [
            self.sandbox.require_id(),
            "sh",
            "-c",
            'echo $$ > "$1"; shift; exec "$@"',
            "tb40-native",
            self.actor_pid_file,
            "/usr/local/bin/tb40-claude",
            "--print",
            "--verbose",
            "--output-format",
            "stream-json",
            "--dangerously-skip-permissions",
            "--model",
            self.engine.model,
            "--tools",
            "Bash,Read,Write,Edit,Glob,Grep",
            "--strict-mcp-config",
            "--mcp-config",
            '{"mcpServers":{}}',
            "--setting-sources",
            "user",
            "--disable-slash-commands",
            "--effort",
            "max",
        ]
        argv += [
            "--append-system-prompt",
            (
                "Execute shell commands synchronously. Background services may persist across tool calls; save useful progress in files. Work only from the task and your own observations; do not obtain private verifier or reference-solution files."
                if self.memory_backend
                else "Execute shell commands synchronously. Do not leave background processes running across tool calls. Save persistent progress in files. Work only from the task and your own observations; do not obtain private verifier or reference-solution files."
            ),
        ]
        if self.max_turns:
            argv += ["--max-turns", str(self.max_turns)]
        if self.context_origin:
            argv += [
                "--resume",
                self.context_origin["session_id"],
                "--fork-session",
                self.marker,
            ]
        else:
            argv += ["--session-id", self.session_id, self.task.instruction]
        return await process(
            argv,
            timeout=max(1, self.task.timeout - self.elapsed_before),
            output=output,
            stderr_output=stderr_output,
        )

    async def run(self):
        started = time.monotonic()
        self.started_at = time.time()
        if self.origin and self.origin.get("memory_checkpoint"):
            if not self.memory_backend:
                raise ValueError("Memory checkpoint requires CRIU backend")
            self.sandbox, _ = await self.memory_backend.fork(
                self.origin["memory_checkpoint"], self.directory / "restore"
            )
        else:
            if self.origin and self.memory_backend:
                raise ValueError("Refusing disk-only branch in CRIU campaign")
            self.sandbox = await self.engine.pool.spawn(
                self.origin["snapshot_id"] if self.origin else self.task.prepared,
                resources=self.task.resources,
                env=getattr(self.task, "environment_env", {}),
            )
        self.engine.active[self.token] = self
        try:
            code, _, stderr = await self.run_native()
            (self.directory / "claude-stderr.txt").write_text(stderr)
            result = cli_result(self.directory / "claude-output.jsonl.gz")
            if result:
                write_json(self.directory / "claude-result.json", result)
            limited = result and result.get("api_error") == "max_output_tokens"
            report = {
                "name": self.name,
                "status": "completed" if code == 0 else "output_limit" if limited else "agent_error",
                "termination_reason": result.get("api_error") if result else None,
                "generation_max_output_tokens": MAX_OUTPUT_TOKENS,
                "generation_cap_history": sorted(
                    {
                        MAX_OUTPUT_TOKENS,
                        *([self.resume.get("previous_output_cap", 32768)] if self.resume else []),
                    }
                ),
                "exit_code": code,
                "wall_seconds": self.elapsed_before + time.monotonic() - started,
                "requests": self.request_count,
                "checkpoints": len(self.points),
                "snapshot_backend": "btrfs-criu" if self.memory_backend else "btrfs-disk",
                "actor_restarts": self.restart_count,
                "provider_recovery_restarts": self.provider_recovery_restarts,
                "environment_recoveries": self.environment_recoveries,
                "origin": self.report_origin,
                "resumed_from": self.resume["point"] if self.resume else None,
                "continued_from_revision": self.resume.get("previous_controller_revision")
                if self.resume
                else None,
                "starting_turn": self.starting_turn,
                "starting_turn_consumed": self.starting_turn_sent,
                "checkpoint_failures": self.checkpoint_failures,
                "container": self.sandbox.require_id(),
            }
            write_json(self.directory / "agentfork.rollout.json", report)
            return report
        except BaseException as error:
            write_json(
                self.directory / "agentfork.rollout.json",
                {
                    "name": self.name,
                    "status": "infrastructure_error",
                    "error": f"{type(error).__name__}: {error}",
                    "wall_seconds": time.monotonic() - started,
                },
            )
            raise
        finally:
            self.engine.active.pop(self.token, None)


class Engine:
    def __init__(self, pool, provider: dict):
        self.pool = pool
        self.model, self.key, self.base_url = (
            provider["model"],
            provider["api_key"],
            provider["base_url"],
        )
        self.active: dict[str, Rollout] = {}
        self.base_users: dict[str, int] = {}

    async def start(self):
        self.client = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=900)
        )
        app = web.Application(client_max_size=64 * 1024**2)

        async def dispatch(request):
            rollout = self.active.get(request.match_info["token"])
            if rollout is None or request.headers.get("x-api-key") != rollout.token:
                raise web.HTTPUnauthorized()
            try:
                return await rollout.handle(request)
            except (ValueError, KeyError) as error:
                write_json(
                    rollout.directory / "gateway-error.json",
                    {"type": type(error).__name__, "message": str(error)},
                )
                return web.json_response(
                    {
                        "type": "error",
                        "error": {
                            "type": "invalid_request_error",
                            "message": str(error),
                        },
                    },
                    status=400,
                )

        app.router.add_post("/{token}/v1/messages", dispatch)
        app.router.add_post("/{token}/v1/messages/count_tokens", dispatch)
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "0.0.0.0", 0)
        await self.site.start()
        self.port = self.site._server.sockets[0].getsockname()[1]

    async def count_tokens(self, body):
        async with self.client.post(
            self.base_url + "/v1/messages/count_tokens",
            json=body,
            headers={"x-api-key": self.key, "anthropic-version": "2023-06-01"},
        ) as response:
            data = await response.read()
            if response.status == 404:
                # Estimate only for CLI bookkeeping, never used as measured API usage.
                return web.json_response({"input_tokens": (len(json.dumps(body).encode()) + 2) // 3})
            return web.Response(body=data, status=response.status, content_type="application/json")

    async def close(self):
        await self.runner.cleanup()
        await self.client.close()
