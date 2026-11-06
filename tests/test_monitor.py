import json
from agentfork.monitor import task_activity


def save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


def test_old_queued_work_is_not_shown_as_running_and_failed_calls_are_not_completions(tmp_path):
    for task, started in [("current", 110), ("old", 50), ("resumable", 50)]:
        save(tmp_path / task / "events.jsonl", {"event": "rollout_started", "time": started})
    save(tmp_path / "resumable/recovery.json", {"state": "queued"})
    save(tmp_path / "resumable/error.json", {"status": "infrastructure_error", "error": "prior mount outage"})
    save(
        tmp_path / "current/attempts/root-0/api-usage.json",
        [
            {"request": 1, "complete": False, "started_at": 111, "seconds": 5},
            {"request": 1, "complete": True, "started_at": 117, "seconds": 3},
            {"request": 2, "complete": False, "started_at": 121, "seconds": 5},
        ],
    )
    active, queued = task_activity(
        tmp_path, {"task_names": ["current", "old", "resumable"], "controller_started_at": 100}
    )
    assert [row["task"] for row in active] == ["current"]
    assert queued == ["resumable"]
    attempt = active[0]["attempts"][0]
    assert attempt["completed_api_requests"] == 1
    assert attempt["failed_provider_attempts"] == 2
    assert attempt["last_response"] == 120
