"""Use Harbor's own artifact collector and separate verifier on a Podman rollout.

Run with the pinned Harbor interpreter, not the experiment's Python environment.
No agent environment is started by Harbor; only its original verifier is run.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path

from harbor.environments.base import BaseEnvironment, ExecResult
from harbor.models.trial.config import (
    AgentConfig,
    EnvironmentConfig,
    TaskConfig,
    TrialConfig,
)
from harbor.trial.trial import Trial

from agentfork.config import ROOT


class ArtifactSource(BaseEnvironment):
    def __init__(self, container: str, trial: Trial, *, podman_bin=None):
        self.container = container
        self.podman_bin = Path(podman_bin or ROOT / "tools/podman-agentfork")
        super().__init__(
            environment_dir=trial.task.paths.environment_dir,
            environment_name=trial.task.short_name,
            session_id=container,
            trial_paths=trial.paths,
            task_env_config=trial.task.config.environment,
        )

    @staticmethod
    def type() -> str:
        return "tb40-podman-artifact-source"

    def _validate_definition(self):
        return

    async def command(self, *arguments: str, timeout: float = 1800) -> ExecResult:
        proc = await asyncio.create_subprocess_exec(
            str(self.podman_bin),
            *arguments,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout)
        except BaseException:
            proc.kill()
            await proc.wait()
            raise
        return ExecResult(
            stdout=stdout.decode(errors="replace"),
            stderr=stderr.decode(errors="replace"),
            return_code=proc.returncode,
        )

    async def start(self, force_build: bool = False):
        return

    async def stop(self, delete: bool = True):
        return

    async def exec(self, command: str, cwd=None, env=None, timeout_sec=None, user=None):
        args = ["exec"]
        if user is not None:
            args += ["--user", str(user)]
        if cwd:
            args += ["--workdir", cwd]
        for key, value in (env or {}).items():
            args += ["--env", f"{key}={value}"]
        return await self.command(*args, self.container, "sh", "-c", command, timeout=timeout_sec or 1800)

    async def download_file(self, source_path, target_path):
        Path(target_path).parent.mkdir(parents=True, exist_ok=True)
        result = await self.command("cp", f"{self.container}:{source_path}", str(target_path))
        if result.return_code:
            raise RuntimeError(result.stderr)

    async def download_dir(self, source_dir, target_dir):
        Path(target_dir).mkdir(parents=True, exist_ok=True)
        result = await self.command("cp", f"{self.container}:{source_dir.rstrip('/')}/.", str(target_dir))
        if result.return_code:
            raise RuntimeError(result.stderr)

    async def upload_file(self, source_path, target_path):
        raise NotImplementedError("Artifact source is read-only to the grader")

    async def upload_dir(self, source_dir, target_dir):
        raise NotImplementedError("Artifact source is read-only to the grader")


async def run(args):
    args.output.mkdir(parents=True, exist_ok=True)
    config = TrialConfig(
        task=TaskConfig(path=args.task.resolve()),
        agent=AgentConfig(name="nop"),
        environment=EnvironmentConfig(type="docker"),
        trial_name="grade-" + hashlib.sha256(str(args.output.resolve()).encode()).hexdigest()[:16],
        trials_dir=args.output.resolve(),
    )
    trial = await Trial.create(config)
    try:
        if args.artifacts is not None:
            artifacts = args.artifacts.resolve()
        else:
            artifacts = trial.paths.artifacts_dir
            source = ArtifactSource(args.container, trial, podman_bin=args.podman_bin)
            trial.agent_environment = source
            # Match Harbor's normal trial path: official collect hooks run
            # before artifact download, with its main/sidecar ordering.
            await trial._collect_artifacts_phased(artifacts_dir=artifacts)
            manifest_path = artifacts / "manifest.json"
            (args.output / "collection.json").write_text(
                manifest_path.read_text() if manifest_path.exists() else "[]"
            )
        result = await trial._run_separate_verifier(
            key="grade",
            timeout_sec=trial._verifier_timeout_sec,
            artifacts_dir=artifacts,
            user=trial.task.config.verifier.user,
        )
        report = {
            "status": "graded",
            "rewards": result.rewards,
            "reward": float(result.rewards.get("reward", 0)),
            "harbor_version": "0.20.0",
            "original_task": str(args.task.resolve()),
            "artifact_path": str(artifacts),
            "verifier_path": str(trial.paths.verifier_dir),
            "grader_revision": "harbor-0.20.0-official-collect-v1",
            "grader_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        }
    except Exception as error:
        report = {
            "status": "infrastructure_error",
            "error": f"{type(error).__name__}: {error}",
        }
        raise
    finally:
        if "report" in locals():
            (args.output / "grade.json").write_text(json.dumps(report, indent=2) + "\n")
        trial._close_logger_handler()
    print(json.dumps(report))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", type=Path, required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--container")
    source.add_argument("--artifacts", type=Path)
    parser.add_argument("--podman-bin", type=Path, default=ROOT / "tools/podman-agentfork")
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(run(parser.parse_args()))
