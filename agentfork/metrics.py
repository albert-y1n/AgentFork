"""Read measured provider usage and task outcomes from an experiment directory."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def actor_usage(directory: Path) -> dict:
    result = {
        "requests": 0,
        "input_tokens": 0,
        "cached_input_tokens": 0,
        "output_tokens": 0,
        "incomplete_requests": 0,
    }
    path = directory / "api-usage.json"
    if not path.exists():
        return result
    for row in json.loads(path.read_text()):
        usage = row.get("usage", {})
        cache = usage.get("cache_read_input_tokens", 0)
        result["requests"] += 1
        result["input_tokens"] += (
            usage.get("input_tokens", 0) + cache + usage.get("cache_creation_input_tokens", 0)
        )
        result["cached_input_tokens"] += cache
        result["output_tokens"] += usage.get("output_tokens", 0)
        result["incomplete_requests"] += not row.get("complete", False)
    return result


def add(*values: dict) -> dict:
    keys = set().union(*(v.keys() for v in values))
    return {key: sum(v.get(key, 0) for v in values) for key in sorted(keys)}


def report(root: Path) -> dict:
    configuration = json.loads((root / "configuration.json").read_text())
    result = {
        "configuration": configuration,
        "baseline_enabled": configuration.get("mode", "paired") != "agentfork-only",
        "planned_tasks": len(configuration["task_names"]),
        "completed_tasks": 0,
        "baseline_resolved": 0,
        "agentfork_resolved": 0,
        "root_resolved": 0,
        "recovered_failed_roots": 0,
        "infrastructure_errors": [],
        "tasks": [],
        "baseline_usage": {},
        "agentfork_usage": {},
        "protocol_revisions": {},
    }
    for name in configuration["task_names"]:
        folder = root / name
        path = folder / "result.json"
        if not path.exists():
            error = folder / "error.json"
            if error.exists():
                result["infrastructure_errors"].append(json.loads(error.read_text()))
            continue
        task = json.loads(path.read_text())
        task_configuration = folder / "run-configuration.json"
        task_config = (
            json.loads(task_configuration.read_text()) if task_configuration.exists() else configuration
        )
        revision = task_config.get("controller_revision", "initial-v1")
        result["protocol_revisions"][revision] = result["protocol_revisions"].get(revision, 0) + 1
        result["completed_tasks"] += 1
        baseline = task.get("baseline_resolved", task.get("baseline_pass_at_4"))
        result["baseline_resolved"] += bool(baseline)
        result["agentfork_resolved"] += bool(task["agentfork_resolved"])
        result["root_resolved"] += bool(task["root_resolved"])
        result["recovered_failed_roots"] += bool(task["agentfork_resolved"] and not task["root_resolved"])
        baseline_usage = add(
            *(
                actor_usage(folder / "attempts" / f"root-{i}")
                for i in range(configuration["baseline_attempts"])
            )
        )
        agentfork_usage = add(
            *(actor_usage(folder / "attempts" / attempt) for attempt in task["agentfork_archive"])
        )
        reviews = folder / "review-usage.json"
        review_usage = {"input_tokens": 0, "output_tokens": 0, "review_requests": 0}
        if reviews.exists():
            for row in json.loads(reviews.read_text()):
                review_usage["input_tokens"] += row["usage"].get("prompt_tokens", 0)
                review_usage["output_tokens"] += row["usage"].get("completion_tokens", 0)
                review_usage["review_requests"] += 1
        agentfork_usage = add(agentfork_usage, review_usage)
        result["baseline_usage"] = add(result["baseline_usage"], baseline_usage)
        result["agentfork_usage"] = add(result["agentfork_usage"], agentfork_usage)
        result["tasks"].append(
            {
                "task": name,
                "controller_revision": revision,
                "max_output_tokens": task_config.get("max_output_tokens", 32768),
                "baseline_resolved": baseline,
                "agentfork_resolved": task["agentfork_resolved"],
                "baseline_usage": baseline_usage,
                "agentfork_usage": agentfork_usage,
            }
        )
    result["score_denominator_note"] = (
        "Infrastructure errors and unfinished tasks remain explicit. Final comparison requires all selected task gates and attempts to finish."
    )
    result["revision_note"] = (
        "Original completed trials remain versioned; repaired trials use the recorded v2 context fitting and 128000-token output cap. Archived infrastructure attempts are retained separately and are not silently counted as task failures."
    )
    if not result["baseline_enabled"]:
        result["baseline_resolved"] = None
        result["score_denominator_note"] = (
            "AgentFork-only rerun of historically unsolved or unstarted tasks; "
            "previous successes are excluded. This selected cohort is not a full-dataset comparison. "
            "Independent Pass@4 is disabled."
        )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    data = report(args.root)
    (args.root / "agentfork.metrics.json").write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
    print(
        json.dumps(
            {k: v for k, v in data.items() if k not in ("configuration", "tasks")},
            indent=2,
        )
    )
