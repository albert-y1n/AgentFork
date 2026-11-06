import json
import os
from pathlib import Path
from collections import namedtuple
import pytest
import agentfork.health_watch as health_watch
from agentfork.health_watch import inspect, process_info


@pytest.fixture(autouse=True)
def ample_test_disk_capacity(monkeypatch):
    # Failure classification tests must not depend on this host's live jobs.
    monkeypatch.setattr(
        health_watch.shutil,
        "disk_usage",
        lambda *args: namedtuple("usage", "total used free")(64 * 2**30, 16 * 2**30, 48 * 2**30),
    )


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def live(*args):
    return {"alive": True, "paused": False, "pid": 123, "start_ticks": "1"}


def campaign(tmp_path):
    save(tmp_path / "configuration.json", {"task_names": ["example"], "btrfs_store": str(tmp_path)})
    save(tmp_path / "progress.json", {"updated_at": 100})
    return tmp_path


def test_provider_wait_is_explicit_while_controller_is_stopped(tmp_path):
    root = campaign(tmp_path)
    save(root / "supervisor.json", {"state": "waiting_provider", "provider_http_status": 502})

    def waiting(pid_file, *args):
        return {"alive": pid_file.name == "supervisor.pid", "paused": False}

    result = inspect(root, now=100, process_reader=waiting)
    assert result["state"] == "waiting_provider"
    assert any(x["kind"] == "controller_absent" for x in result["issues"])


def test_checkpoint_compaction_error_and_stale_heartbeat_are_visible(tmp_path):
    root = campaign(tmp_path)
    save(
        root / "compaction-status.json",
        {"state": "needs_attention", "updated_at": 100, "error": "reference checksum mismatch"},
    )
    health = inspect(root, now=100, process_reader=live)
    assert any(row["kind"] == "checkpoint_compaction_error" for row in health["issues"])
    assert health["checkpoint_compaction"]["process"]["alive"]
    save(root / "compaction-status.json", {"state": "watching", "updated_at": 100})
    later = inspect(root, now=300, process_reader=live)
    assert any(row["kind"] == "checkpoint_compaction_stale" for row in later["issues"])


def test_expected_checkpoint_compactor_cannot_disappear_silently(tmp_path):
    root = campaign(tmp_path)
    save(root / "compaction-status.json", {"state": "watching", "updated_at": 100})

    def processes(pid_file, *args):
        return {"alive": pid_file.name != "compactor.pid", "paused": False}

    health = inspect(root, now=100, process_reader=processes)
    assert any(row["kind"] == "checkpoint_compactor_absent" for row in health["issues"])


def test_unfinished_attempt_capture_failure_is_visible_before_task_exits(tmp_path):
    root = campaign(tmp_path)
    save(
        root / "example/attempts/root-0/checkpoint-artifacts/00001/capture-error.json",
        {"error": "dump failed"},
    )
    health = inspect(root, now=100, process_reader=live)
    assert health["state"] == "needs_attention"
    assert health["failure_counts"] == {"checkpoint_capture_error": 1}
    assert health["issues"][0]["path"].endswith("capture-error.json")
    assert not (root / "example/error.json").exists()


def test_native_boundary_failure_without_capture_error_file_is_detected_and_deduplicated(tmp_path):
    root = campaign(tmp_path)
    attempt = root / "example/attempts/root-0"
    row = {"checkpoint_failures": ["step 4: matching native results not durable"]}
    save(attempt / "actor-restarts.json", row)
    first = inspect(root, now=100, process_reader=live)
    assert first["state"] == "needs_attention"
    assert first["failure_counts"]["checkpoint_capture_error"] == 1
    assert first["issues"][0]["stage"] == "native_results_not_durable"
    save(attempt / "outcome.json", {"status": "completed", **row})
    second = inspect(root, now=100, process_reader=live)
    assert second["failure_counts"]["checkpoint_capture_error"] == 1
    assert first["issues"][0]["id"] == second["issues"][0]["id"]
    assert second["untimed_failure_counts"]["checkpoint_capture_error"] == 1


def test_capture_error_file_and_actor_failure_list_count_as_one_failure(tmp_path):
    root = campaign(tmp_path)
    attempt = root / "example/attempts/root-0"
    save(attempt / "actor-restarts.json", {"checkpoint_failures": ["step 4: RuntimeError: dump failed"]})
    save(attempt / "checkpoint-artifacts/00004/capture-error.json", {"error": "dump failed"})
    health = inspect(root, now=100, process_reader=live)
    assert health["failure_counts"]["checkpoint_capture_error"] == 1
    assert not health["untimed_failure_counts"]


def test_retried_provider_error_stays_visible_but_is_marked_recovered(tmp_path):
    root = campaign(tmp_path)
    path = root / "example/attempts/root-0/api-usage.json"
    failed = {"request": 4, "complete": False, "error": "TimeoutError"}
    save(path, [failed, {"request": 5, "complete": True}])
    before = inspect(root, now=100, process_reader=live)
    assert before["state"] == "needs_attention"
    save(path, [failed, {"request": 4, "complete": True}])
    after = inspect(root, now=100, process_reader=live)
    assert after["state"] == "warnings"
    assert after["issues"][0]["resolved"]
    assert before["issues"][0]["id"] == after["issues"][0]["id"]


def test_parent_fallback_is_recorded_without_hiding_capture_failure(tmp_path):
    root = campaign(tmp_path)
    save(
        root / "example/attempts/root-0/checkpoint-artifacts/00001/capture-error.json",
        {"fallback": "restored prior coherent environment and native conversation"},
    )
    health = inspect(root, now=100, process_reader=live)
    assert health["state"] == "warnings"
    assert health["failure_counts"]["checkpoint_capture_error"] == 1


def test_none_fallback_is_not_reported_as_a_recovered_checkpoint(tmp_path):
    root = campaign(tmp_path)
    save(
        root / "example/attempts/root-0/checkpoint-artifacts/00001/capture-error.json",
        {"fallback": "none; actor resumes without adding a branch point"},
    )
    health = inspect(root, now=100, process_reader=live)
    assert health["state"] == "needs_attention"
    problem = health["issues"][0]
    assert not problem["resolved"]
    save(
        root / "health-resolutions.json",
        {
            problem["id"]: {
                "state": "resolved",
                "reason": "Network contention cleared and subsequent restore validated",
            }
        },
    )
    after = inspect(root, now=100, process_reader=live)
    assert after["state"] == "warnings"
    assert after["failure_counts"]["checkpoint_capture_error"] == 1


def test_reviewer_length_exhaustion_detected_even_when_http_succeeded(tmp_path):
    root = campaign(tmp_path)
    path = root / "example/review/round-1/plan-0.response-0.json"
    save(path, {"choices": [{"finish_reason": "length", "message": {"content": ""}}]})
    health = inspect(root, now=100, process_reader=live)
    assert health["state"] == "needs_attention"
    assert health["failure_counts"]["review_output_exhausted"] == 1
    save(path.parent / "plan.json", {"branches": []})
    assert inspect(root, now=100, process_reader=live)["state"] == "warnings"


def test_resumed_task_counts_new_errors_without_relabeling_old_retries(tmp_path):
    root = campaign(tmp_path)
    save(
        root / "configuration.json",
        {
            "task_names": ["example"],
            "controller_started_at": 100,
            "controller_revision": "v6",
            "btrfs_store": str(root),
        },
    )
    save(root / "example/run-configuration.json", {"controller_revision": "v5"})
    save(
        root / "example/attempts/root-0/api-usage.json",
        [
            {"complete": False, "started_at": 50, "request": 1},
            {"complete": True, "started_at": 51, "request": 1},
            {"complete": False, "started_at": 110, "request": 2},
        ],
    )
    health = inspect(root, now=120, process_reader=live)
    assert health["failure_counts"]["provider_failed_attempt"] == 2
    assert health["current_revision_failure_counts"]["provider_failed_attempt"] == 1


def test_cancelled_upgrade_request_keeps_usage_unknown_without_false_api_failure(tmp_path):
    root = campaign(tmp_path)
    save(
        root / "configuration.json",
        {"task_names": ["example"], "controller_started_at": 100, "btrfs_store": str(root)},
    )
    save(
        root / "example/review-usage.json",
        [
            {
                "request": "plan",
                "complete": False,
                "error": "CancelledError",
                "usage_unknown": True,
                "started_at": 50,
                "seconds": 20,
            }
        ],
    )
    health = inspect(root, now=100, process_reader=live)
    assert "provider_failed_attempt" not in health["failure_counts"]
    assert health["failure_counts"]["request_cancelled"] == 1
    assert health["issues"][0]["historical"]


def test_live_client_cancellation_is_an_alert_even_without_a_task_error(tmp_path):
    root = campaign(tmp_path)
    save(
        root / "configuration.json",
        {"task_names": ["example"], "controller_started_at": 100, "btrfs_store": str(root)},
    )
    save(
        root / "example/attempts/root-0/api-usage.json",
        [
            {"request": 10, "complete": False, "error": "CancelledError", "started_at": 110, "seconds": 610},
            {"request": 11, "complete": False, "error": "CancelledError", "started_at": 710, "seconds": 610},
        ],
    )
    health = inspect(root, now=1400, process_reader=live)
    assert health["state"] == "needs_attention"
    assert health["current_revision_failure_counts"] == {"request_cancelled": 2}
    assert all(not item["historical"] for item in health["issues"] if item["kind"] == "request_cancelled")


def test_cancellation_spanning_controller_start_is_not_silenced(tmp_path):
    root = campaign(tmp_path)
    save(
        root / "configuration.json",
        {"task_names": ["example"], "controller_started_at": 100, "btrfs_store": str(root)},
    )
    save(
        root / "example/review-usage.json",
        [
            {
                "request": "plan",
                "complete": False,
                "error": "CancelledError",
                "started_at": 50,
                "seconds": 60,
            },
        ],
    )
    health = inspect(root, now=120, process_reader=live)
    assert health["state"] == "needs_attention"
    assert not health["issues"][0]["historical"]
    assert health["current_revision_failure_counts"] == {"request_cancelled": 1}


def test_cancellation_without_timing_evidence_is_not_assumed_to_be_maintenance(tmp_path):
    root = campaign(tmp_path)
    save(
        root / "example/review-usage.json",
        [{"request": "plan", "complete": False, "error": "CancelledError"}],
    )
    health = inspect(root, now=100, process_reader=live)
    assert health["state"] == "needs_attention"
    assert not health["issues"][0]["historical"]


def test_live_reviewer_stage_is_reported_and_old_receiving_files_are_excluded(tmp_path):
    root = campaign(tmp_path)
    save(
        root / "configuration.json",
        {"task_names": ["example"], "controller_started_at": 100, "btrfs_store": str(root)},
    )
    folder = root / "example/review/round-2"
    save(
        folder / "analysis-root-0-try-0.inflight.json",
        {"state": "receiving", "started_at": 50, "updated_at": 110, "bytes_received": 100},
    )
    save(
        folder / "analysis-branch-r1-2-try-0-resume-123.inflight.json",
        {
            "state": "receiving",
            "started_at": 105,
            "updated_at": 110,
            "last_byte_at": 110,
            "bytes_received": 1234,
        },
    )
    health = inspect(root, now=110, process_reader=live)
    assert len(health["reviewer_activity"]) == 1
    row = health["reviewer_activity"][0]
    assert (row["task"], row["round"], row["stage"], row["attempt"]) == (
        "example",
        2,
        "analysis",
        "branch-r1-2",
    )
    assert row["bytes_received"] == 1234 and row["last_byte_at"] == 110
    stopped = inspect(root, now=110, process_reader=lambda *a: {"alive": False})
    assert not stopped["reviewer_activity"]


def test_partial_json_does_not_crash_watcher_or_invent_failures(tmp_path):
    root = campaign(tmp_path)
    path = root / "example/attempts/root-0/api-usage.json"
    path.parent.mkdir(parents=True)
    path.write_text('[{"complete":')
    health = inspect(root, now=100, process_reader=live)
    assert health["state"] == "healthy"
    assert not health["failure_counts"]


def test_resolution_for_old_gateway_error_cannot_hide_a_new_occurrence(tmp_path):
    root = campaign(tmp_path)
    path = root / "example/attempts/root-0/gateway-error.json"
    save(path, {"message": "old failure"})
    os.utime(path, (100, 100))
    before = inspect(root, now=110, process_reader=live)
    problem = next(row for row in before["issues"] if row["kind"] == "context_or_gateway_error")
    save(
        root / "health-resolutions.json",
        {problem["id"]: {"state": "resolved", "resolved_through": 100, "reason": "Fixed and verified"}},
    )
    assert inspect(root, now=110, process_reader=live)["state"] == "warnings"
    save(path, {"message": "new failure"})
    os.utime(path, (120, 120))
    after = inspect(root, now=130, process_reader=live)
    assert after["state"] == "needs_attention"
    assert not next(row for row in after["issues"] if row["kind"] == "context_or_gateway_error")["resolved"]


def test_previous_controller_pending_task_does_not_raise_live_idle_alarm(tmp_path):
    root = campaign(tmp_path)
    save(
        root / "configuration.json",
        {"task_names": ["example"], "controller_started_at": 100, "btrfs_store": str(root)},
    )
    path = root / "example/events.jsonl"
    save(path, {"event": "rollout_started", "time": 50})
    os.utime(path, (50, 50))
    health = inspect(root, now=5000, process_reader=live)
    assert not health["active"]
    assert all(row["kind"] != "activity_needs_inspection" for row in health["issues"])


def test_failed_success_archive_is_visible_in_campaign_health(tmp_path):
    root = campaign(tmp_path)
    save(
        root / "archive-status.json",
        {"state": "needs_attention", "updated_at": 100, "error": "Digest mismatch"},
    )
    health = inspect(root, now=110, process_reader=live)
    assert health["state"] == "needs_attention"
    assert health["trajectory_archive"]["process"]["alive"]
    assert any(row["kind"] == "trajectory_archive_error" for row in health["issues"])


def test_archiver_absence_checks_actual_process_and_leaves_controller_running(tmp_path):
    root = campaign(tmp_path)
    save(root / "archive-status.json", {"state": "watching", "updated_at": 100})

    def roles(pid_file, script, campaign):
        return {"alive": False, "paused": False} if script.name == "archive_successes.py" else live()

    health = inspect(root, now=110, process_reader=roles)
    assert health["state"] == "needs_attention"
    assert health["processes"]["controller"]["alive"]
    assert any(row["kind"] == "trajectory_archiver_absent" for row in health["issues"])


def test_finished_benchmark_does_not_hide_failed_success_archiving(tmp_path):
    root = campaign(tmp_path)
    save(root / "example/result.json", {"agentfork_resolved": True})
    save(
        root / "archive-status.json",
        {"state": "needs_attention", "updated_at": 100, "error": "Digest mismatch"},
    )
    health = inspect(root, now=110, process_reader=live)
    assert health["completed_tasks"] == health["planned_tasks"] == 1
    assert health["state"] == "needs_attention"


def test_task_completion_requires_all_configured_tasks_and_keeps_grade_result(tmp_path):
    root = campaign(tmp_path)
    save(root / "example/result.json", {"agentfork_resolved": True})
    health = inspect(root, now=100, process_reader=lambda *a: {"alive": False})
    assert health["state"] == "finished"
    assert health["completed_tasks"] == health["agentfork_resolved"] == 1
    assert not health["issues"]


def test_stale_progress_does_not_imply_controller_is_dead(tmp_path):
    health = inspect(campaign(tmp_path), now=300, process_reader=live)
    assert health["processes"]["controller"]["alive"]
    assert any(i["kind"] == "progress_stale" for i in health["issues"])


def test_process_check_rejects_reused_pid_and_zombie(tmp_path):
    proc = tmp_path / "proc/45"
    proc.mkdir(parents=True)
    pid_file, script, root = tmp_path / "controller.pid", tmp_path / "campaign.py", tmp_path / "run"
    pid_file.write_text("45")
    (proc / "stat").write_text("45 (python with spaces) S " + " ".join(["0"] * 18 + ["1234"]))
    (proc / "cmdline").write_bytes(b"python\0unrelated.py\0")
    assert not process_info(pid_file, script, root, proc=tmp_path / "proc")["alive"]
    (proc / "cmdline").write_bytes(("python\0" + str(script) + "\0--output\0" + str(root) + "\0").encode())
    assert process_info(pid_file, script, root, proc=tmp_path / "proc")["alive"]
    (proc / "stat").write_text((proc / "stat").read_text().replace(") S ", ") Z "))
    assert not process_info(pid_file, script, root, proc=tmp_path / "proc")["alive"]
