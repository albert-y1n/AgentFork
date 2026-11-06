import asyncio
import gzip
import json
import sys

import pytest

from agentfork.campaign import Case, all_attempts, flatten_errors
from agentfork.review_budget import fit_analyst, fit_reviewer
from agentfork.rollout import cli_result, completed_tool_boundary, process


def history(count=400):
    return {
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "thinking", "thinking": "x" * 4000},
                    {"type": "tool_use", "input": {"command": "y" * 2000}},
                ],
            }
            for _ in range(count)
        ],
        "prefix_message_counts": {"1": 20, "2": 180, "3": count},
    }


def test_anthropic_review_budget_keeps_checkpoint_offsets_and_saved_history():
    native = history()
    before = json.dumps(native)
    rows = [
        {
            "name": name,
            "native_history": native,
            "analysis": {"branch_candidates": [{"step": 2}]},
            "available_steps": [1, 2, 3],
        }
        for name in ["root-0", "branch-r1-0", "branch-r1-1", "branch-r1-2"]
    ]
    prompt, budget = fit_reviewer(
        "{problem}\n{reports}\n{count_rule}",
        problem="task",
        rows=rows,
        count_rule="up to 3",
    )
    assert budget["chars_before"] > 1_000_000
    assert len(prompt) <= 400_000
    reduced = json.loads(prompt.split("\n", 1)[1].rsplit("\n", 1)[0])
    assert all(
        row["native_history"]["prefix_message_counts"] == native["prefix_message_counts"] for row in reduced
    )
    assert any(m["role"] == "omitted" for row in reduced for m in row["native_history"]["messages"])
    assert json.dumps(native) == before


def test_analyst_can_fit_a_long_native_trace_without_editing_actor_history():
    native = history(800)
    prompt, record = fit_analyst(
        "{problem} {verdict} {transcript} {checkpoint_steps} {lo} {hi} {candidate_limit}",
        problem="task",
        verdict="failed",
        native=native,
        steps=[1, 2, 3],
    )
    assert len(prompt) <= 400_000
    assert record["chars_before"] > 400_000
    assert "omitted" in prompt
    assert len(native["messages"]) == 800


def test_one_infrastructure_failure_does_not_cancel_healthy_siblings():
    async def check():
        completed = []

        async def fail():
            raise RuntimeError("actual leaf error")

        async def sibling():
            await asyncio.sleep(0.01)
            completed.append(True)

        with pytest.raises(ExceptionGroup) as caught:
            await all_attempts(fail(), sibling())
        assert completed == [True]
        assert flatten_errors(caught.value) == [{"type": "RuntimeError", "message": "actual leaf error"}]

    asyncio.run(check())


def test_subprocess_drains_stderr_while_stdout_is_still_open(tmp_path):
    async def check():
        out, err = tmp_path / "stdout.gz", tmp_path / "stderr.txt"
        code, _, stderr = await process(
            [
                sys.executable,
                "-c",
                "import sys;sys.stderr.write('x'*500000);sys.stderr.flush();print('finished')",
            ],
            output=out,
            stderr_output=err,
            timeout=5,
        )
        assert code == 0
        assert gzip.open(out, "rt").read() == "finished\n"
        assert len(stderr) == err.stat().st_size == 500000

    asyncio.run(check())


def test_native_cli_output_limit_is_recoverable_from_saved_result(tmp_path):
    path = tmp_path / "claude-output.jsonl.gz"
    with gzip.open(path, "wt") as stream:
        stream.write(json.dumps({"type": "assistant", "message": "partial"}) + "\n")
        stream.write(
            json.dumps({"type": "result", "is_error": True, "api_error": "max_output_tokens"}) + "\n"
        )
    assert cli_result(path)["api_error"] == "max_output_tokens"


def test_checkpoint_boundary_requires_completed_tools_and_ignores_environment_attachments():
    history = [
        {
            "role": "user",
            "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "done"}],
        },
        {"role": "system", "content": "# Environment"},
    ]
    assert completed_tool_boundary(history) == ("t1",)
    assert (
        completed_tool_boundary(
            history
            + [
                {
                    "role": "assistant",
                    "content": [{"type": "thinking", "thinking": "unfinished"}],
                }
            ]
        )
        is None
    )


def test_controller_rollover_reuses_completed_trial_without_another_actor_call(
    tmp_path,
):
    from types import SimpleNamespace

    folder = tmp_path / "attempts/root-0"
    requests = folder / "requests"
    requests.mkdir(parents=True)
    (folder / "outcome.json").write_text(
        json.dumps(
            {
                "name": "root-0",
                "reward": 0,
                "requests": 1,
                "status": "completed",
                "grade": {"status": "graded", "reward": 0},
            }
        )
    )
    (folder / "checkpoints.json").write_text(json.dumps([{"step": 1, "snapshot_id": "retained-copy"}]))
    with gzip.open(requests / "00000.json.gz", "wt") as stream:
        json.dump({"messages": [{"role": "user", "content": "task"}]}, stream)
    case = Case.__new__(Case)
    case.directory, case.recovery = (
        tmp_path,
        {"previous_configuration": {"max_output_tokens": 32768}},
    )
    case.engine, case.slots, case.outcomes, case.rollouts = (
        SimpleNamespace(),
        asyncio.Semaphore(1),
        {},
        {},
    )
    case.event = lambda *args, **kwargs: None
    result = asyncio.run(case.attempt("root-0", checkpoints=True))
    assert result["reward"] == 0
    assert result["generation_max_output_tokens"] == 32768
    assert case.rollouts["root-0"].points[0]["snapshot_id"] == "retained-copy"
    assert case.rollouts["root-0"].sandbox is None


def test_output_budget_termination_still_grades_artifacts(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import agentfork.campaign as campaign

    graded = []

    class LimitedRollout:
        def __init__(self, *args, **kwargs):
            self.directory = tmp_path / "attempts/root-0"
            self.directory.mkdir(parents=True)
            self.sandbox = object()
            self.request_count, self.points = 1, []

        async def run(self):
            return {"status": "output_limit", "exit_code": 1}

    async def destroy(sandbox):
        pass

    async def grade(sandbox, output):
        graded.append(True)
        return {"reward": 0, "status": "graded"}

    monkeypatch.setattr(campaign, "Rollout", LimitedRollout)
    case = Case.__new__(Case)
    case.directory, case.recovery = tmp_path, None
    case.engine = SimpleNamespace(pool=SimpleNamespace(destroy=destroy))
    case.args = SimpleNamespace(max_turns=None)
    case.slots, case.outcomes, case.rollouts = asyncio.Semaphore(1), {}, {}
    case.grade, case.event = grade, lambda *a, **k: None
    outcome = asyncio.run(case.attempt("root-0"))
    assert graded == [True]
    assert outcome["status"] == "output_limit" and outcome["reward"] == 0
