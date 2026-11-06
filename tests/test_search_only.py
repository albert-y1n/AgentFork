import asyncio, json
from types import SimpleNamespace
from agentfork.campaign import Case
from agentfork.metrics import report


def test_agentfork_only_never_starts_independent_roots_and_can_recover(tmp_path):
    async def check():
        case = Case.__new__(Case)
        case.name = "probe"
        case.directory = tmp_path
        case.args = SimpleNamespace(mode="agentfork-only", baseline_attempts=4, branches=[1])
        case.outcomes = {}
        case.rollouts = {}
        case.prompts = SimpleNamespace(sha256={})
        started = []

        async def prepare():
            pass

        async def gate(name):
            return {"reward": int(name == "oracle")}

        async def attempt(name, **kwargs):
            started.append(name)
            assert name == "root-0" or name.startswith("branch-")
            case.outcomes[name] = {"reward": int(name != "root-0")}
            case.rollouts[name] = SimpleNamespace(points=[{"step": 1}])
            return case.outcomes[name]

        async def plan(*args):
            return [{"base": "root-0", "branch_step": 1, "content": "repair", "commands": ["true"]}]

        case.prepare = prepare
        case.gate = gate
        case.attempt = attempt
        case.plan = plan
        case.event = lambda *a, **k: None
        result = await case.run()
        assert started == ["root-0", "branch-r1-0"]
        assert result["agentfork_resolved"] and not result["root_resolved"]
        assert result["baseline_attempts"] == 0
        assert result["baseline_resolved"] is result["baseline_pass_at_4"] is None

    asyncio.run(check())


def test_agentfork_only_metrics_have_no_baseline_score_or_baseline_cost(tmp_path):
    (tmp_path / "configuration.json").write_text(
        json.dumps({"mode": "agentfork-only", "baseline_attempts": 0, "task_names": ["probe"]})
    )
    folder = tmp_path / "probe"
    folder.mkdir()
    (folder / "result.json").write_text(
        json.dumps(
            {
                "baseline_resolved": None,
                "baseline_pass_at_4": None,
                "agentfork_resolved": True,
                "root_resolved": True,
                "agentfork_archive": ["root-0"],
            }
        )
    )
    actor = folder / "attempts/root-0"
    actor.mkdir(parents=True)
    (actor / "api-usage.json").write_text(
        json.dumps([{"complete": True, "usage": {"input_tokens": 10, "output_tokens": 3}}])
    )
    result = report(tmp_path)
    assert result["baseline_resolved"] is None and result["baseline_enabled"] is False
    assert result["baseline_usage"] == {}
    assert result["agentfork_usage"]["input_tokens"] == 10
    assert result["agentfork_resolved"] == 1


def test_verified_success_survives_sibling_infrastructure_error(tmp_path):
    from agentfork.supervise_rerun import retain_verified_success, ROOT

    folder = tmp_path / "probe"
    attempt = folder / "attempts/branch-r1-0"
    grade = attempt / "grade"
    grade.mkdir(parents=True)
    (folder / "error.json").write_text(json.dumps({"error": "sibling provider stream truncated"}))
    (attempt / "outcome.json").write_text(json.dumps({"name": "branch-r1-0", "reward": 1}))
    (grade / "grade.json").write_text(
        json.dumps(
            {
                "status": "graded",
                "reward": 1,
                "original_task": str((ROOT / "datasets/terminal-bench/probe").resolve()),
            }
        )
    )
    assert retain_verified_success(folder)
    result = json.loads((folder / "result.json").read_text())
    assert result["agentfork_resolved"] and result["baseline_resolved"] is None
    assert result["success_grade_proofs"]
    assert (folder / "error.json").exists()
    assert list((folder / "infrastructure-history").glob("*.json"))
    assert not retain_verified_success(folder)


def test_claimed_success_with_mismatched_verifier_is_never_promoted(tmp_path):
    from agentfork.supervise_rerun import retain_verified_success, ROOT

    folder = tmp_path / "probe"
    attempt = folder / "attempts/root-0"
    grade = attempt / "grade"
    grade.mkdir(parents=True)
    (attempt / "outcome.json").write_text(json.dumps({"reward": 1}))
    (grade / "grade.json").write_text(
        json.dumps(
            {
                "status": "graded",
                "reward": 0,
                "original_task": str((ROOT / "datasets/terminal-bench/probe").resolve()),
            }
        )
    )
    assert not retain_verified_success(folder)
    assert not (folder / "result.json").exists()


def test_restore_failure_does_not_cancel_successful_search_sibling(tmp_path):
    async def check():
        case = Case.__new__(Case)
        case.name = "probe"
        case.directory = tmp_path
        case.args = SimpleNamespace(mode="agentfork-only", baseline_attempts=0, branches=[2])
        case.outcomes = {}
        case.rollouts = {}
        case.prompts = SimpleNamespace(sha256={})

        async def prepare():
            pass

        async def gate(name):
            return {"reward": int(name == "oracle")}

        async def attempt(name, **kwargs):
            if name == "branch-r1-0":
                row = {"reward": None, "status": "restore_failed"}
            else:
                row = {"reward": int(name == "branch-r1-1"), "status": "completed"}
            case.outcomes[name] = row
            case.rollouts[name] = SimpleNamespace(points=[{"step": 1}])
            return row

        async def plan(*args):
            return [
                {"base": "root-0", "branch_step": 1, "content": "repair", "commands": ["true"]}
                for _ in range(2)
            ]

        case.prepare = prepare
        case.gate = gate
        case.attempt = attempt
        case.plan = plan
        case.event = lambda *a, **k: None
        result = await case.run()
        assert result["agentfork_resolved"]
        assert result["unexecuted_restore_failures"] == ["branch-r1-0"]
        assert "branch-r1-0" not in result["agentfork_archive"]
        assert result["outcomes"]["branch-r1-0"]["reward"] is None

    asyncio.run(check())


def test_supervisor_restart_preserves_pass_sequence(tmp_path):
    import json
    from agentfork.supervise_rerun import current_pass

    assert current_pass(tmp_path) == 1
    history = tmp_path / "controller-passes"
    history.mkdir()
    (history / "2.json").write_text("{}")
    (history / "3.json").write_text("{}")
    (tmp_path / "supervisor.json").write_text(json.dumps({"campaign_pass": 2}))
    assert current_pass(tmp_path) == 3
    (tmp_path / "supervisor.json").write_text(json.dumps({"campaign_pass": 4}))
    assert current_pass(tmp_path) == 4
