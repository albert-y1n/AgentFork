"""Run AgentFork-only or paired searches on the TB4 single-container CPU subset."""

from __future__ import annotations
from agentfork.config import PACKAGE, load_provider, task_inventory

import argparse
import asyncio
import fcntl
import gzip
import hashlib
import json
import os
import re
import shutil
import signal
import sys
import time
import tomllib
import traceback
from copy import deepcopy
from pathlib import Path

from agentfork.metrics import report as metrics_report
from agentfork.provider_io import fetch as fetch_provider, chat_completion
from agentfork.criu_backend import CheckpointRestoreError
from agentfork.review_budget import fit_analyst, fit_reviewer
from agentfork.rollout import (
    CLAUDE,
    MAX_OUTPUT_TOKENS,
    NATIVE_TIMEOUT_ENV,
    ROOT,
    Engine,
    Rollout,
    process,
    read_request,
    response_message,
    write_json,
)
from agentfork.prompts import load_prompts
from agentfork_sandbox import PodmanPool
from agentfork_sandbox.btrfs_store import config_path
from agentfork_sandbox.podman_images import read_config

HARBOR_PYTHON = os.environ.get("AGENTFORK_HARBOR_PYTHON", sys.executable)
CONTROLLER_REVISION = "agentfork-0.1.0"
# Review generation has an independent provider-configurable budget.
REVIEW_MAX_OUTPUT_TOKENS = int(os.environ.get("AGENTFORK_REVIEW_MAX_OUTPUT_TOKENS", "128000"))
REVIEW_MAX_RESPONSE_BYTES = 256 * 2**20


def store_is_available(engine):
    """A disappeared mount must not look like an empty checkpoint directory."""
    expected = getattr(engine, "store_device", None)
    if expected is None:
        return True
    try:
        return engine.pool.store.path.stat().st_dev == expected
    except OSError:
        return False


def image_cache_lock(engine):
    lock = getattr(engine, "image_cache_lock", None)
    if lock is None:
        lock = engine.image_cache_lock = asyncio.Lock()
    return lock


def clipped(text: str, limit=12000) -> str:
    if len(text) <= limit:
        return text
    return text[: limit // 2] + f"\n…[{len(text) - limit} chars elided]…\n" + text[-limit // 2 :]


def compact_history(value):
    if isinstance(value, dict):
        return {k: compact_history(v) for k, v in value.items() if k not in ("signature", "cache_control")}
    if isinstance(value, list):
        return [compact_history(v) for v in value]
    if isinstance(value, str):
        return clipped(value, 6000)
    return value


def parse_json(text: str) -> dict:
    block = re.search(r"```(?:branch-plan|json)?\s*\n?(.*?)```", text, re.DOTALL)
    if block:
        text = block.group(1)
    start, end = text.find("{"), text.rfind("}")
    data = json.loads(text[start : end + 1])
    if not isinstance(data, dict):
        raise TypeError("Expected an object")
    return data


async def together(*jobs):
    """Cancel and join siblings before a failed task's sandboxes are cleaned."""
    async with asyncio.TaskGroup() as group:
        tasks = [group.create_task(job) for job in jobs]
    return [task.result() for task in tasks]


async def all_attempts(*jobs):
    """Keep independent trials running when a sibling has an infrastructure error."""
    outcomes = await asyncio.gather(*jobs, return_exceptions=True)
    errors = [value for value in outcomes if isinstance(value, BaseException)]
    if errors:
        raise BaseExceptionGroup("Attempt infrastructure errors", errors)
    return outcomes


def flatten_errors(error):
    if isinstance(error, BaseExceptionGroup):
        return [leaf for nested in error.exceptions for leaf in flatten_errors(nested)]
    return [{"type": type(error).__name__, "message": str(error)}]


class Case:
    def __init__(self, engine: Engine, path: Path, directory: Path, args):
        self.engine, self.path, self.directory, self.args = (
            engine,
            path,
            directory,
            args,
        )
        self.name = path.name
        self.config = tomllib.loads((path / "task.toml").read_text())
        self.instruction = (path / "instruction.md").read_text()
        self.image = self.config["environment"]["docker_image"]
        self.environment_env = self.config["environment"].get("env", {})
        self.resources = {
            k: self.config["environment"][k] for k in ("cpus", "memory_mb") if k in self.config["environment"]
        }
        if "cpus" in self.resources:
            self.resources["cpu"] = self.resources.pop("cpus")
        self.timeout = self.config.get("agent", {}).get("timeout_sec", 28800)
        self.snapshots = []
        self.rollouts: dict[str, Rollout] = {}
        self.outcomes: dict[str, dict] = {}
        self.reviews = []
        self.preserved = False
        self.prompts = load_prompts(PACKAGE / "resources/prompts.yaml")
        self.slots = asyncio.Semaphore(args.concurrency)
        self.directory.mkdir(parents=True, exist_ok=True)
        recovery = self.directory / "recovery.json"
        self.recovery = json.loads(recovery.read_text()) if recovery.exists() else None
        if self.recovery:
            self.recovery["state"] = "active"
            write_json(recovery, self.recovery)
            self.snapshots.extend(self.recovery["retained_snapshots"])
            reviews = self.directory / "review-usage.json"
            self.reviews = json.loads(reviews.read_text()) if reviews.exists() else []

    def event(self, kind: str, **fields):
        record = {"time": time.time(), "task": self.name, "event": kind, **fields}
        print(json.dumps(record, ensure_ascii=False), flush=True)
        with (self.directory / "events.jsonl").open("a") as stream:
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        write_json(self.args.output / "status.json", {**record, "pid": os.getpid()})

    async def podman(self, *args, timeout=1800):
        code, stdout, stderr = await process([ROOT / "tools/podman-agentfork", *args], timeout=timeout)
        if code:
            raise RuntimeError(f"podman {args[0]} failed ({code}): {stderr[-2500:]}")
        return stdout

    async def prepare(self):
        # Pull/import and garbage collection share Podman's layer database.
        async with image_cache_lock(self.engine):
            await self._prepare()

    async def _prepare(self):
        self.event("preparing")
        from agentfork.prepared_environment import record_prepared, reuse_prepared

        if await reuse_prepared(self):
            return
        self.base = await self.engine.pool.prepare(self.image)
        key = str(self.base)
        self.engine.base_users[key] = self.engine.base_users.get(key, 0) + 1
        cfg = read_config(self.base)
        self.cwd = cfg.get("WorkingDir") or "/"
        self.user = cfg.get("User") or "root"
        stage = await self.engine.pool.spawn(self.image, resources=self.resources, env=self.environment_env)
        try:
            sid = stage.require_id()
            entrypoint = cfg.get("Entrypoint") or []
            if (
                entrypoint
                and Path(entrypoint[0]).name == "tini"
                and entrypoint[1:] == ["--"]
                and getattr(getattr(self.engine, "memory_backend", None), "init_process", False)
            ):
                write_json(
                    self.directory / "entrypoint.json",
                    {
                        "command": entrypoint,
                        "strategy": "Delegate the image init-only entrypoint to the backend catatonit init; preserve declared environment",
                        "init_delegated": True,
                    },
                )
                entrypoint = []
            if entrypoint:
                allowed = {
                    "docker-entrypoint.sh",
                    "_entrypoint.sh",
                    "__cacert_entrypoint.sh",
                }
                if len(entrypoint) != 1 or Path(entrypoint[0]).name not in allowed:
                    raise RuntimeError(f"Unsupported image startup wrapper: {entrypoint}")
                resolved = await self.podman(
                    "exec", sid, "sh", "-c", 'command -v "$1"', "resolve", entrypoint[0]
                )
                script = await self.podman("exec", sid, "cat", resolved.strip())
                if not re.search(r'exec\s+"?\$@', script):
                    raise RuntimeError("Image startup wrapper does not delegate to its command")
                code, env_text, detail = await process(
                    [
                        ROOT / "tools/podman-agentfork",
                        "exec",
                        "--user",
                        self.user,
                        sid,
                        *entrypoint,
                        "/bin/sh",
                        "-c",
                        "printf '\\036TB40_ENV\\036'; exec /usr/bin/env -0",
                    ],
                    timeout=120,
                )
                if code:
                    raise RuntimeError(f"Image startup initialization failed: {detail[-1500:]}")
                if "\x1eTB40_ENV\x1e" not in env_text:
                    raise RuntimeError("Image startup did not reach the environment capture command")
                env_text = env_text.split("\x1eTB40_ENV\x1e", 1)[1]
                activated = dict(item.split("=", 1) for item in env_text.split("\0") if "=" in item)
                for key in ["HOSTNAME", "PWD", "SHLVL", "_"]:
                    activated.pop(key, None)
                self.environment_env = {**self.environment_env, **activated}
                write_json(
                    self.directory / "entrypoint.json",
                    {
                        "command": entrypoint,
                        "script_sha256": hashlib.sha256(script.encode()).hexdigest(),
                        "activated_keys": sorted(activated),
                        "strategy": "Run command-delegating image wrapper once; preserve initialized disk and exported environment on all fresh/restored sandboxes",
                    },
                )
            await self.podman(
                "exec",
                "--user",
                "root",
                sid,
                "sh",
                "-c",
                "mkdir -p /opt/tb40-claude /logs/artifacts; chmod 1777 /opt/tb40-claude /logs/artifacts",
            )
            await self.engine.pool.upload_file(stage, CLAUDE, "/usr/local/bin/tb40-claude")
            await self.podman(
                "exec",
                "--user",
                "root",
                sid,
                "sh",
                "-c",
                "chmod 755 /usr/local/bin/tb40-claude; /usr/local/bin/tb40-claude --version",
            )
            # The settings directory is created by the actual image user so
            # later native transcripts and their forks retain valid ownership.
            await self.podman(
                "exec",
                "--user",
                self.user,
                sid,
                "sh",
                "-c",
                'printf \'%s\' \'{"effortLevel":"high","permissions":{"defaultMode":"bypassPermissions"}}\' > /opt/tb40-claude/settings.json',
            )
            snapshot = await self.engine.pool.snapshot(stage)
            self.prepared = snapshot.id
            self.snapshots.append(snapshot.id)
            record_prepared(self, snapshot.id)
            self.event("prepared", cwd=self.cwd, user=self.user)
        finally:
            await self.engine.pool.destroy(stage)

        # All later task containers use the prepared Btrfs snapshot. The
        # original overlay image is only a reproducible import cache. Release
        # it under disk pressure; no --force means a concurrent native image
        # user retains its reference instead of being disturbed.
        if shutil.disk_usage(self.directory).free < 24 * 2**30:
            try:
                await self.podman("image", "rm", self.image)
            except Exception as error:
                self.event("prepared_image_cache_retained", detail=clipped(str(error), 2000))
            else:
                self.event("prepared_image_cache_released", image=self.image)

    async def grade(self, sandbox, output: Path):
        self.event("grading", attempt=output.parent.name)
        code, _, stderr = await process(
            [
                HARBOR_PYTHON,
                PACKAGE / "harbor_grade.py",
                "--task",
                self.path,
                "--container",
                sandbox.require_id(),
                "--output",
                output,
            ],
            timeout=self.config["verifier"].get("timeout_sec", 900) + 1800,
            output=output.with_suffix(".log"),
        )
        path = output / "grade.json"
        if not path.exists():
            raise RuntimeError(f"Harbor grade returned {code}: {stderr[-2000:]}")
        result = json.loads(path.read_text())
        if code or result["status"] != "graded":
            raise RuntimeError(result.get("error", f"Harbor grade exited {code}"))
        return result

    async def gate(self, role):
        output = self.directory / "gate" / role
        expected_reward = 1 if role == "oracle" else 0
        existing = output / "grade/grade.json"
        if existing.exists():
            previous = json.loads(existing.read_text())
            if previous.get("status") == "graded" and previous.get("reward") == expected_reward:
                return previous
        if self.args.gate_cache:
            cached = self.args.gate_cache / self.name / "gate" / role / "grade/grade.json"
            if cached.exists():
                previous = json.loads(cached.read_text())
                if (
                    previous.get("status") == "graded"
                    and previous.get("reward") == expected_reward
                    and previous.get("original_task") == str(self.path.resolve())
                ):
                    self.event("gate_reused", role=role, artifact=str(cached.resolve()))
                    return previous
        sandbox = await self.engine.pool.spawn(
            self.prepared, resources=self.resources, env=self.environment_env
        )
        try:
            if role == "oracle":
                await self.podman(
                    "cp",
                    str(self.path / "solution"),
                    f"{sandbox.require_id()}:/solution",
                )
                oracle_env = []
                if self.name == "cad-model":
                    # build123d 0.10 allows ocp_gordon>=0.1.17, but current
                    # ocp_gordon 0.3.1 requires an incompatible OCP API. Pin
                    # its declared compatible release for the reference gate
                    # only; actor environments and task sources are unchanged.
                    await self.podman(
                        "exec",
                        "--user",
                        "root",
                        sandbox.require_id(),
                        "sh",
                        "-c",
                        "printf '%s\\n' 'ocp_gordon==0.1.17' > /tmp/tb40-oracle-constraints.txt",
                    )
                    oracle_env = [
                        "--env",
                        "PIP_CONSTRAINT=/tmp/tb40-oracle-constraints.txt",
                    ]
                    write_json(
                        output / "dependency-compatibility.json",
                        {
                            "reference_only": True,
                            "constraint": "ocp_gordon==0.1.17",
                            "reason": "build123d==0.10.0 requires OCP 7.8; newer unbounded ocp_gordon releases import OCP APIs absent in that required version",
                        },
                    )
                code, _, stderr = await process(
                    [
                        ROOT / "tools/podman-agentfork",
                        "exec",
                        "--user",
                        self.user,
                        "--workdir",
                        self.cwd,
                        *oracle_env,
                        sandbox.require_id(),
                        "sh",
                        "-c",
                        "bash /solution/solve.sh",
                    ],
                    timeout=self.timeout,
                    output=output / "oracle.log",
                    stderr_output=output / "oracle-stderr.log",
                )
                if code:
                    self.event("oracle_script_nonzero", exit_code=code, detail=stderr[-1000:])
            return await self.grade(sandbox, output / "grade")
        finally:
            await self.engine.pool.destroy(sandbox)

    async def attempt(self, name, *, checkpoints=False, origin=None, starting_turn=None):
        async with self.slots:
            resume_info = self.recovery.get("resume_attempts", {}).get(name) if self.recovery else None
            saved = self.directory / "attempts" / name / "outcome.json"
            if (
                self.recovery
                and saved.exists()
                and json.loads(saved.read_text()).get("grade", {}).get("status") == "graded"
            ):
                # The controller was switched between actor invocations.
                # Reuse completed trials and their retained snapshots; empty,
                # blocked trials have no outcome and start normally below.
                outcome = json.loads(saved.read_text())
                outcome.setdefault(
                    "generation_max_output_tokens",
                    self.recovery.get("previous_configuration", {}).get("max_output_tokens", 32768),
                )
                folder = saved.parent
                rollout = Rollout(self.engine, self, name, folder, checkpoints=checkpoints)
                rollout.points = (
                    json.loads((folder / "checkpoints.json").read_text())
                    if (folder / "checkpoints.json").exists()
                    else []
                )
                rollout.request_count = outcome["requests"]
                requests = sorted(
                    p
                    for p in (folder / "requests").glob("*.json.gz")
                    if re.fullmatch(r"\d{5}\.json\.gz", p.name)
                )
                rollout.latest_request = read_request(requests[-1])
                response_path = requests[-1].with_name(requests[-1].name.replace(".json.gz", ".sse.gz"))
                if response_path.exists():
                    import gzip

                    rollout.latest_response = response_message(gzip.open(response_path, "rb").read())[0]
                self.rollouts[name] = rollout
                self.outcomes[name] = outcome
                self.event(
                    "rollout_reused",
                    attempt=name,
                    reward=outcome["reward"],
                    original_generation_cap=outcome["generation_max_output_tokens"],
                )
                return outcome
            if self.recovery:
                folder = self.directory / "attempts" / name
                if list((folder / "requests").glob("*.json.gz")) and not resume_info:
                    raise RuntimeError(
                        "An ungraded prior actor request cannot be overwritten during recovery"
                    )
                history = folder / "before-controller-switch"
                for filename in [
                    "outcome.json",
                    "agentfork.rollout.json",
                    "claude-output.jsonl.gz",
                    "claude-stderr.txt",
                ]:
                    source = folder / filename
                    if source.exists():
                        history.mkdir(parents=True, exist_ok=True)
                        shutil.move(source, history / filename)
            rollout = Rollout(
                self.engine,
                self,
                name,
                self.directory / "attempts" / name,
                checkpoints=checkpoints,
                origin=origin,
                starting_turn=starting_turn,
                max_turns=self.args.max_turns,
                resume=resume_info,
            )
            self.rollouts[name] = rollout
            self.event(
                "rollout_started",
                attempt=name,
                origin=origin and {k: origin[k] for k in ("attempt", "step")},
            )
            try:
                try:
                    report = await rollout.run()
                except CheckpointRestoreError as error:
                    if origin is None:
                        raise
                    origin["eligible"] = False
                    origin["restore_error"] = str(error)
                    parent = self.rollouts[origin["attempt"]]
                    write_json(parent.directory / "checkpoints.json", parent.points)
                    outcome = {
                        "name": name,
                        "status": "restore_failed",
                        "reward": None,
                        "error": str(error),
                        "origin": origin,
                        "model_attempt_executed": False,
                    }
                    self.outcomes[name] = outcome
                    write_json(rollout.directory / "outcome.json", outcome)
                    self.event("branch_restore_failed", attempt=name, error=str(error))
                    return outcome
                if report["status"] not in ("completed", "output_limit"):
                    # Native CLI errors are infrastructure errors, never zeros.
                    raise RuntimeError(
                        f"Claude Code exited {report['exit_code']}; inspect {rollout.directory}"
                    )
                grade = await self.grade(rollout.sandbox, rollout.directory / "grade")
                outcome = {**report, "reward": grade["reward"], "grade": grade}
                self.outcomes[name] = outcome
                write_json(rollout.directory / "outcome.json", outcome)
                self.event(
                    "rollout_finished",
                    attempt=name,
                    reward=grade["reward"],
                    requests=rollout.request_count,
                    checkpoints=len(rollout.points),
                )
                return outcome
            finally:
                if rollout.sandbox is not None:
                    await self.engine.pool.destroy(rollout.sandbox)

    async def review_call(self, prompt: str, output: Path):
        if output.with_suffix(".request.json").exists():
            output = output.with_name(output.name + "-resume-" + str(time.time_ns()))
        body = {
            "model": self.engine.model,
            "messages": [{"role": "user", "content": prompt}],
            "thinking": {"type": "enabled"},
            "reasoning_effort": "max",
            "temperature": 1,
            "max_tokens": REVIEW_MAX_OUTPUT_TOKENS,
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        write_json(output.with_suffix(".request.json"), body)

        async def progress(row):
            write_json(
                output.with_suffix(".inflight.json"), {"state": "receiving", "updated_at": time.time(), **row}
            )

        async def record(row, raw):
            row["request"] = str(output)
            row["usage"] = {}
            if not raw.lstrip().startswith(b"{"):
                with gzip.open(
                    output.with_suffix(f".response-{row['provider_attempt']}.sse.gz"), "wb"
                ) as stream:
                    stream.write(raw)
            try:
                value = chat_completion(raw)
                write_json(output.with_suffix(f".response-{row['provider_attempt']}.json"), value)
                if row["complete"]:
                    row["usage"] = value.get("usage", {})
                    row["finish_reason"] = value["choices"][0].get("finish_reason")
            except ValueError:
                output.with_suffix(f".response-{row['provider_attempt']}.partial").write_bytes(raw)
            self.reviews.append(row)
            write_json(self.directory / "review-usage.json", self.reviews)
            write_json(
                output.with_suffix(".inflight.json"),
                {
                    "state": "complete" if row["complete"] else "failed",
                    "updated_at": time.time(),
                    "bytes_received": len(raw),
                    **row,
                },
            )

        def validate(raw):
            value = chat_completion(raw)
            if not value.get("choices"):
                raise ValueError("Reviewer returned no choices")
            return value

        data, _ = await fetch_provider(
            self.engine.client,
            self.engine.base_url + "/v1/chat/completions",
            body,
            {"Authorization": "Bearer " + self.engine.key},
            validate=validate,
            on_attempt=record,
            on_progress=progress,
            max_response_bytes=REVIEW_MAX_RESPONSE_BYTES,
        )
        content = data["choices"][0]["message"].get("content") or ""
        if not content.strip():
            raise ValueError(
                "Reviewer returned no JSON content; finish_reason="
                + str(data["choices"][0].get("finish_reason"))
            )
        return parse_json(content)

    def report(self, name):
        rollout, outcome = self.rollouts[name], self.outcomes[name]
        messages = deepcopy(rollout.latest_request["messages"])
        if rollout.latest_response:
            messages.append(
                {
                    "role": "assistant",
                    "content": rollout.latest_response.get("content", []),
                }
            )
        logs = Path(outcome["grade"]["verifier_path"])
        diagnostic = "\n".join(
            clipped(p.read_text(errors="replace"), 20000) for p in sorted(logs.glob("*.txt"))
        )
        return {
            "name": name,
            "reward": outcome["reward"],
            "verdict": diagnostic,
            "available_steps": [p["step"] for p in rollout.points if p.get("eligible", True)],
            "native_history": {
                "messages": compact_history(messages),
                "prefix_message_counts": {str(p["step"]): p["message_count"] for p in rollout.points},
                "workspace": {"cwd": self.cwd},
            },
            "assistant_turn_given": outcome.get("starting_turn"),
        }

    async def plan(self, names, round_index, count):
        folder = self.directory / "review" / f"round-{round_index}"
        folder.mkdir(parents=True, exist_ok=True)
        backend = getattr(self.engine, "memory_backend", None)
        if backend:
            for name in names:
                rollout = self.rollouts[name]
                for point in rollout.points:
                    memory = point.get("memory_checkpoint")
                    if (
                        not memory
                        or point.get("eligible") is False
                        or memory.get("validation", {}).get("passed")
                    ):
                        continue
                    try:
                        await backend.qualify(
                            memory, rollout.directory / "migration-validation" / str(point["step"])
                        )
                        point["eligible"] = True
                    except CheckpointRestoreError as error:
                        point.update(eligible=False, restore_error=str(error))
                write_json(rollout.directory / "checkpoints.json", rollout.points)
        if self.recovery and (folder / "plan.json").exists():
            cached = json.loads((folder / "plan.json").read_text())["branches"]
            if all(
                b["base"] in names
                and any(
                    p["step"] == b["branch_step"] and p.get("eligible", True)
                    for p in self.rollouts[b["base"]].points
                )
                for b in cached
            ):
                self.event("review_reused", round=round_index)
                return cached
        reports = []
        for name in names:
            report = self.report(name)
            if not report["available_steps"]:
                continue
            analysis_path = self.directory / "attempts" / name / "analysis.json"
            if analysis_path.exists():
                report["analysis"] = json.loads(analysis_path.read_text())
            else:
                prompt, budget = fit_analyst(
                    self.prompts.analyst_case,
                    problem=self.instruction,
                    verdict=report["verdict"],
                    native=report["native_history"],
                    steps=report["available_steps"],
                )
                write_json(folder / f"analysis-{name}.budget.json", budget)
                for retry in range(3):
                    try:
                        report["analysis"] = await self.review_call(
                            prompt, folder / f"analysis-{name}-try-{retry}"
                        )
                        break
                    except (ValueError, KeyError, TypeError) as error:
                        if retry == 2:
                            raise
                        prompt += f"\nReturn the complete concise JSON object. Previous output was invalid: {error}.\n"
                write_json(analysis_path, report["analysis"])
            reports.append(report)
        if not reports:
            return []
        prompt, budget = fit_reviewer(
            self.prompts.reviewer_assistant_turn,
            problem=self.instruction,
            rows=reports,
            count_rule=self.prompts.reviewer_count_rule_adaptive.format(count=count),
        )
        prompt = prompt.replace("mini-swe-agent", "Claude Code")
        write_json(folder / "prompt-budget.json", budget)
        prompt += "\nAdapter note: the actor also has native Read/Write/Edit/Glob/Grep tools, but author your starting turn as commands only. Commands will execute in order as separate subshells inside one Bash invocation, to prevent parallel execution.\n"
        for retry in range(3):
            try:
                plan = await self.review_call(prompt, folder / f"plan-{retry}")
                branches = plan["branches"]
                if not isinstance(branches, list) or len(branches) > count:
                    raise ValueError("Too many branches or branches is not a list")
                used_names = set()
                for branch in branches:
                    parent = self.rollouts[branch["base"]]
                    if branch["base"] not in names or not any(
                        p["step"] == branch["branch_step"] and p.get("eligible", True) for p in parent.points
                    ):
                        raise ValueError("Branch must select an available step in the supplied archive")
                    if (
                        not branch["content"].strip()
                        or not branch["commands"]
                        or not all(isinstance(c, str) and c.strip() for c in branch["commands"])
                    ):
                        raise ValueError("Starting turn needs content and nonempty command strings")
                    if branch["name"] in used_names:
                        raise ValueError("Branch names must be unique")
                    used_names.add(branch["name"])
                write_json(folder / "plan.json", plan)
                return branches
            except (KeyError, ValueError, TypeError) as error:
                prompt += f"\nThe plan was rejected: {error}. Correct it and return the complete branch-plan JSON. No branch has executed.\n"
        raise RuntimeError("No valid reviewer plan after three attempts")

    async def run(self):
        await self.prepare()
        oracle, nop = await together(self.gate("oracle"), self.gate("nop"))
        write_json(self.directory / "gate.json", {"oracle": oracle, "nop": nop})
        if oracle["reward"] != 1 or nop["reward"] != 0:
            raise RuntimeError(
                f"Original oracle/nop gate failed: oracle={oracle['reward']}, nop={nop['reward']}"
            )
        root = await self.attempt("root-0", checkpoints=True)
        archive = ["root-0"]

        async def independent():
            return await all_attempts(
                *(self.attempt(f"root-{i}") for i in range(1, self.args.baseline_attempts))
            )

        async def agentfork():
            if root["reward"] == 1:
                return
            for round_index, count in enumerate(self.args.branches, 1):
                self.event("review_started", round=round_index)
                branches = await self.plan(archive, round_index, count)
                if not branches:
                    self.event("search_stopped", reason="no_eligible_branch_plan")
                    break
                new_names = []
                jobs = []
                for index, branch in enumerate(branches):
                    parent = self.rollouts[branch["base"]]
                    origin = next(p for p in parent.points if p["step"] == branch["branch_step"])
                    name = f"branch-r{round_index}-{index}"
                    new_names.append(name)
                    jobs.append(
                        self.attempt(
                            name,
                            checkpoints=True,
                            origin=origin,
                            starting_turn={
                                "content": branch["content"],
                                "commands": branch["commands"],
                            },
                        )
                    )
                outcomes = await all_attempts(*jobs)
                archive.extend(
                    name for name in new_names if self.outcomes[name].get("status") != "restore_failed"
                )
                if any(o["reward"] == 1 for o in outcomes):
                    break

        agentfork_only = getattr(self.args, "mode", "paired") == "agentfork-only"
        if agentfork_only:
            await agentfork()
        else:
            await all_attempts(independent(), agentfork())
        baseline = [] if agentfork_only else [o for n, o in self.outcomes.items() if n.startswith("root-")]
        searched = [self.outcomes[n] for n in archive]
        restore_failures = [n for n, o in self.outcomes.items() if o.get("status") == "restore_failed"]
        if restore_failures and not any(o["reward"] == 1 for o in searched):
            raise RuntimeError(
                "Search incomplete because branches failed to restore: " + ", ".join(restore_failures)
            )
        result = {
            "task": self.name,
            "status": "complete",
            "mode": "agentfork-only" if agentfork_only else "paired",
            "baseline_attempts": len(baseline),
            "baseline_resolved": None if agentfork_only else any(o["reward"] == 1 for o in baseline),
            "baseline_pass_at_4": any(o["reward"] == 1 for o in baseline) if len(baseline) == 4 else None,
            "agentfork_resolved": any(o["reward"] == 1 for o in searched),
            "root_resolved": root["reward"] == 1,
            "outcomes": self.outcomes,
            "agentfork_archive": archive,
            "unexecuted_restore_failures": restore_failures,
            "prompts_sha256": self.prompts.sha256,
        }
        write_json(self.directory / "result.json", result)
        self.event(
            "task_completed",
            baseline_pass_at_4=result["baseline_pass_at_4"],
            agentfork_resolved=result["agentfork_resolved"],
        )
        return result

    async def preserve(self, reason):
        """Keep coherent checkpoints on interruption instead of deleting progress."""
        resumes = {}
        store_available = store_is_available(self.engine)
        for name, rollout in self.rollouts.items():
            if self.outcomes.get(name, {}).get("grade", {}).get("status") == "graded":
                continue
            points = [
                p
                for p in rollout.points
                if p.get("eligible", True)
                and (not store_available or (self.engine.pool.store.snapshots / p["snapshot_id"]).is_dir())
            ]
            if not points:
                continue
            resumes[name] = {
                "point": points[-1],
                "points": points,
                "request_count": rollout.request_count,
                "api": rollout.api,
                "started_at": rollout.started_at or time.time(),
                "elapsed_seconds": rollout.elapsed_before
                + max(0, time.time() - (rollout.started_at or time.time())),
                "starting_turn_consumed": bool(getattr(rollout, "starting_turn_sent", False)),
                "previous_output_cap": MAX_OUTPUT_TOKENS,
                "previous_controller_revision": json.loads(
                    (self.directory / "run-configuration.json").read_text()
                ).get("controller_revision", CONTROLLER_REVISION),
            }
        # Preparation or initial restore may fail before a Rollout exists.
        # Those failures must keep the inherited context and its original budget.
        for name, inherited in (getattr(self, "recovery", None) or {}).get("resume_attempts", {}).items():
            if name in resumes or self.outcomes.get(name, {}).get("grade", {}).get("status") == "graded":
                continue
            if (
                not store_available
                or (self.engine.pool.store.snapshots / inherited["point"]["snapshot_id"]).is_dir()
            ):
                resumes[name] = deepcopy(inherited)
        if not resumes and not any(
            o.get("grade", {}).get("status") == "graded" for o in self.outcomes.values()
        ):
            return False
        retained = [
            sid
            for sid in self.snapshots
            if not store_available or (self.engine.pool.store.snapshots / sid).is_dir()
        ]
        write_json(
            self.directory / "recovery.json",
            {
                "state": "queued",
                "reason": reason,
                "created_at": time.time(),
                "retained_snapshots": retained,
                "resume_attempts": resumes,
                "requires_store_validation": not store_available,
                "previous_configuration": json.loads((self.directory / "run-configuration.json").read_text()),
            },
        )
        self.preserved = True
        self.event(
            "checkpoints_retained", reason=reason, snapshots=len(retained), resumable_attempts=list(resumes)
        )
        return True

    async def close(self):
        # Last-resort protection if preservation itself could not finish.
        if getattr(self, "recovery", None) and not (self.directory / "result.json").exists():
            self.preserved = True
        for rollout in self.rollouts.values():
            if rollout.sandbox and rollout.sandbox.sandbox_id:
                await self.engine.pool.destroy(rollout.sandbox)
        keep_completed = (self.directory / "result.json").exists() and not getattr(
            getattr(self, "args", None), "discard_completed_checkpoints", False
        )
        for snapshot_id in reversed([] if self.preserved or keep_completed else self.snapshots):
            try:
                await self.engine.pool.delete_snapshot(snapshot_id)
            except Exception as error:
                self.event("cleanup_warning", snapshot=snapshot_id, error=str(error))
        if getattr(self, "base", None) and self.base.exists():
            key = str(self.base)
            self.engine.base_users[key] -= 1
            if self.engine.base_users[key] == 0:
                await self.engine.pool.store.delete(self.base)
                config_path(self.base).unlink(missing_ok=True)
        async with image_cache_lock(self.engine):
            try:
                await self.podman("image", "rm", self.image)
            except Exception:
                pass
        verifier_image = self.config.get("verifier", {}).get("environment", {}).get("docker_image")
        if verifier_image:
            code, image_id, _ = await process(
                ["docker", "image", "inspect", "--format", "{{.Id}}", verifier_image]
            )
            if code == 0 and image_id.strip() not in self.engine.preexisting_docker_images:
                await process(["docker", "image", "rm", verifier_image])


async def main(args):
    os.umask(0o077)
    if args.mode == "agentfork-only":
        args.baseline_attempts = 0
    elif args.baseline_attempts < 1:
        raise ValueError("Paired mode requires at least one baseline attempt")
    if args.checkpoint_backend == "criu":
        os.environ["PATH"] = str(ROOT / "tools/criu-bin") + ":" + os.environ.get("PATH", "")
    provider = load_provider()
    if not CLAUDE.is_file():
        raise ValueError("Set AGENTFORK_CLAUDE_BINARY to a native Claude Code executable")
    version_code, native_version, _ = await process([CLAUDE, "--version"], timeout=30)
    if version_code:
        raise ValueError("Configured native Claude executable cannot run on this host")
    rows = task_inventory()
    selected = [
        r for r in rows if not r["gpus"] and not r["compose"] and (not args.tasks or r["task"] in args.tasks)
    ]
    if not selected:
        raise ValueError("No selected single-container CPU tasks")
    priority = [
        "mvcc-lsm-compaction",
        "session-window-debug",
        "html-js-filter",
        "interleaved-vigenere",
        "bun-sourcemap-leak",
        "wal-recovery-ordering",
        "vf2-speedup-networkx",
        "rs-archive-clone",
    ]

    def queue_priority(row):
        folder = args.output / row["task"]
        recovery = folder / "recovery.json"
        if recovery.exists():
            data = json.loads(recovery.read_text())
            if (data.get("retained_snapshots") or data.get("retry_before_new")) and all(
                (args.btrfs_store / "snapshots" / sid).is_dir() for sid in data.get("retained_snapshots", [])
            ):
                return 0
        if (folder / "error.json").exists():
            return 1
        return 3 if (folder / "events.jsonl").exists() or (folder / "retry.json").exists() else 2

    selected.sort(
        key=lambda row: (
            queue_priority(row),
            priority.index(row["task"]) if row["task"] in priority else len(priority),
            row["task"],
        )
    )
    args.output.mkdir(parents=True, exist_ok=True)
    lock = (args.output / "agentfork.campaign.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    previous_config = args.output / "configuration.json"
    if previous_config.exists():
        previous = json.loads(previous_config.read_text())
        stamp = str(time.time_ns())
        write_json(args.output / "configuration-history" / f"{stamp}.json", previous)
        for row in selected:
            path = args.output / row["task"]
            if path.exists() and not (path / "run-configuration.json").exists():
                write_json(path / "run-configuration.json", previous)
    write_json(
        args.output / "configuration.json",
        {
            "dataset": "terminal-bench/terminal-bench@4.0.0",
            "model": provider["model"],
            "claude_code": native_version.strip(),
            "claude_binary_sha256": hashlib.sha256(CLAUDE.read_bytes()).hexdigest(),
            "context_window": int(os.environ.get("AGENTFORK_CONTEXT_TOKENS", "1000000")),
            "thinking_effort": "max",
            "criu_configuration_sha256": hashlib.sha256(
                (PACKAGE / "resources/criu.conf").read_bytes()
            ).hexdigest(),
            "max_output_tokens": MAX_OUTPUT_TOKENS,
            "native_client_timeouts_ms": {key: int(value) for key, value in NATIVE_TIMEOUT_ENV.items()},
            "cancel_provider_on_native_disconnect": True,
            "review_max_output_tokens": REVIEW_MAX_OUTPUT_TOKENS,
            "review_token_parameter": "max_tokens",
            "review_transport": "openai_sse",
            "review_max_response_bytes": REVIEW_MAX_RESPONSE_BYTES,
            "controller_revision": CONTROLLER_REVISION,
            "controller_started_at": time.time(),
            "mode": args.mode,
            "checkpoint_backend": args.checkpoint_backend,
            "checkpoint_reference_storage_supported": True,
            "checkpoint_payload_relocation_supported": True,
            "prepared_template_reuse_supported": True,
            "new_image_base_compression": "zstd",
            "criu_policy": {
                "validate_child_restore": True,
                "init_new_containers": True,
                "internal_tcp": True,
                "ghost_limit_bytes": 268435456,
                "external_tcp": "reject",
                "network_identity": "original application IP aliases in a separate namespace",
                "checkpoint_staging": "btrfs-zstd-bind",
            },
            "btrfs_store": str(args.btrfs_store.resolve()),
            "btrfs_backing_file": str(args.btrfs_backing_file.resolve()),
            "review_prompt_budget_chars": 400000,
            "baseline_attempts": args.baseline_attempts,
            "branches": args.branches,
            "concurrency": args.concurrency,
            "task_workers": args.task_workers,
            "max_turns": args.max_turns,
            "task_names": [r["task"] for r in selected],
            "shared_root": "root-0 belongs only to AgentFork; independent baseline disabled"
            if args.mode == "agentfork-only"
            else "root-0 is counted in both methods",
            "implementation_sha256": {
                p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(Path(__file__).parent.glob("*.py"))
            },
        },
    )
    pool = PodmanPool(
        args.btrfs_store,
        Path(os.environ.get("AGENTFORK_RUNTIME_BINARY", ROOT / "runtime/agentfork-runtime")),
        podman_bin=str(ROOT / "tools/podman-agentfork"),
    )
    from agentfork.prepared_environment import enable_compressed_bases

    enable_compressed_bases(pool.store)
    engine = Engine(pool, provider)
    engine.campaign_directory = args.output
    engine.store_device = pool.store.path.stat().st_dev
    if args.checkpoint_backend == "criu":
        from agentfork.criu_backend import CriuBtrfs

        engine.memory_backend = CriuBtrfs(pool, ROOT, "", init_process=True, compressed_staging=True)
    _, existing_images, _ = await process(["docker", "image", "ls", "--no-trunc", "--format", "{{.ID}}"])
    engine.preexisting_docker_images = set(existing_images.split())
    await engine.start()
    results = []
    queue = asyncio.Queue()
    for row in selected:
        queue.put_nowait(row)

    def summary():
        write_json(
            args.output / "summary.json",
            {
                "planned": len(selected),
                "finished": len(results),
                "results": results,
                "updated_at": time.time(),
            },
        )
        write_json(args.output / "agentfork.metrics.json", metrics_report(args.output))

    async def worker():
        while not queue.empty():
            from agentfork.provider_health import unavailable

            if unavailable(args.output):
                print(
                    json.dumps(
                        {
                            "time": time.time(),
                            "event": "queue_waiting_provider",
                            "reason": "Provider retries exhausted; preserve current trials before starting fresh tasks",
                        }
                    ),
                    flush=True,
                )
                return
            if not store_is_available(engine):
                raise RuntimeError(
                    "Checkpoint store mount disappeared; stopping the queue and preserving current tasks"
                )
            row = queue.get_nowait()
            path = args.output / row["task"]
            if (path / "result.json").exists():
                results.append(json.loads((path / "result.json").read_text()))
                summary()
                continue
            if (path / "recovery-hold.json").exists():
                results.append({"task": row["task"], "status": "recovery_held"})
                print(
                    json.dumps({"time": time.time(), "task": row["task"], "event": "task_held_for_recovery"}),
                    flush=True,
                )
                summary()
                continue
            if shutil.disk_usage(ROOT).free < 8 * 1024**3:
                raise RuntimeError("Less than 8 GiB host disk remains; refusing to start another task")
            recovery_path = path / "recovery.json"
            recovery = json.loads(recovery_path.read_text()) if recovery_path.exists() else None
            valid_recovery = bool(
                recovery
                and all(
                    (pool.store.snapshots / sid).is_dir() for sid in recovery.get("retained_snapshots", [])
                )
            )
            if recovery and recovery.get("resume_attempts") and not valid_recovery:
                write_json(
                    path / "recovery-hold.json",
                    {
                        "state": "held_for_recovery",
                        "at": time.time(),
                        "reason": "Retained disk snapshots are missing; preserve the trajectory instead of starting over",
                    },
                )
                results.append({"task": row["task"], "status": "recovery_held"})
                summary()
                continue
            if valid_recovery and (path / "error.json").exists():
                previous = path / "infrastructure-history" / f"{time.time_ns()}.json"
                previous.parent.mkdir(exist_ok=True)
                shutil.move(path / "error.json", previous)
            unfinished = (path / "events.jsonl").exists() and not valid_recovery
            if unfinished:
                old_error = (
                    json.loads((path / "error.json").read_text())
                    if (path / "error.json").exists()
                    else {
                        "status": "interrupted",
                        "error": "Previous controller stopped without a retained environment; archive before retry",
                    }
                )
                archive = args.output / "failed-attempts" / f"{row['task']}-{time.time_ns()}"
                archive.parent.mkdir(exist_ok=True)
                shutil.move(path, archive)
                write_json(
                    archive / "archive-location.json",
                    {
                        "original_directory": str(path.resolve()),
                        "archive_directory": str(archive.resolve()),
                        "note": "Original metadata and trajectory files retained; resolve old absolute paths using this directory mapping.",
                    },
                )
                diagnostics = (
                    json.loads((args.output / "debug-diagnostics.json").read_text())
                    if (args.output / "debug-diagnostics.json").exists()
                    else {}
                )
                write_json(
                    path / "retry.json",
                    {
                        "previous_error": old_error,
                        "archive": str(archive.resolve()),
                        "diagnosis": diagnostics.get("tasks", {}).get(row["task"]),
                        "started_at": time.time(),
                    },
                )
            write_json(
                path / "run-configuration.json",
                json.loads((args.output / "configuration.json").read_text()),
            )
            case = Case(engine, Path(row["path"]), path, args)
            try:
                results.append(await case.run())
            except asyncio.CancelledError:
                await case.preserve("controller_interrupted; resume from last coherent checkpoint")
                raise
            except Exception as error:
                case.event("task_error", error=f"{type(error).__name__}: {error}")
                result = {
                    "task": row["task"],
                    "status": "infrastructure_error",
                    "error": f"{type(error).__name__}: {error}",
                    "causes": flatten_errors(error),
                }
                (path / "traceback.txt").write_text("".join(traceback.format_exception(error)))
                write_json(path / "error.json", result)
                from agentfork.supervise_rerun import retain_verified_success

                if retain_verified_success(path):
                    results.append(json.loads((path / "result.json").read_text()))
                else:
                    await case.preserve(str(error))
                    results.append(result)
                if args.fail_fast:
                    raise
            finally:
                await case.close()
            summary()

    async def disk_guard():
        while True:
            if not store_is_available(engine):
                write_json(
                    args.output / "storage-stop.json",
                    {
                        "time": time.time(),
                        "reason": "checkpoint_store_mount_unavailable",
                        "store": str(pool.store.path),
                        "expected_device": engine.store_device,
                    },
                )
                raise RuntimeError(
                    "Checkpoint store mount disappeared; preserving current tasks before continuing"
                )
            if (
                shutil.disk_usage(ROOT).free < 6 * 1024**3
                or shutil.disk_usage(pool.store.path).free < 1024**3
                or shutil.disk_usage(args.output).free < 8 * 1024**3
            ):
                write_json(
                    args.output / "storage-stop.json",
                    {
                        "time": time.time(),
                        "host_free": shutil.disk_usage(ROOT).free,
                        "store_free": shutil.disk_usage(pool.store.path).free,
                        "output_free": shutil.disk_usage(args.output).free,
                    },
                )
                raise RuntimeError(
                    "Storage reserve reached; cancelling current tasks before the filesystem fills"
                )
            await asyncio.sleep(5)

    current = asyncio.current_task()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, current.cancel)
    watcher = asyncio.create_task(disk_guard())
    jobs = asyncio.create_task(together(*(worker() for _ in range(args.task_workers))))
    try:
        done, _ = await asyncio.wait([jobs, watcher], return_when=asyncio.FIRST_COMPLETED)
        for job in done:
            await job
    finally:
        jobs.cancel()
        watcher.cancel()
        await asyncio.gather(jobs, watcher, return_exceptions=True)
        summary()
        await engine.close()
        await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--tasks", nargs="*")
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--task-workers", type=int, default=2)
    parser.add_argument("--baseline-attempts", type=int, default=4)
    parser.add_argument("--mode", choices=["paired", "agentfork-only"], default="agentfork-only")
    parser.add_argument("--checkpoint-backend", choices=["disk", "criu"], default="criu")
    parser.add_argument("--btrfs-store", type=Path, default=ROOT / "storage/btrfs")
    parser.add_argument("--btrfs-backing-file", type=Path, default=ROOT / "storage/btrfs.img")
    parser.add_argument("--branches", type=int, nargs="*", default=[4, 3])
    parser.add_argument("--max-turns", type=int)
    parser.add_argument(
        "--discard-completed-checkpoints",
        action="store_true",
        help="Explicitly discard completed task disk snapshots; transcripts are retained",
    )
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--gate-cache", type=Path)
    asyncio.run(main(parser.parse_args()))
