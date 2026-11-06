import json

from agentfork.rollout import clean_native, response_message


def test_resume_capture_preserves_artifacts_from_a_failed_later_step(tmp_path):
    import asyncio, time
    from types import SimpleNamespace
    from agentfork.rollout import Rollout

    async def check():
        directory = tmp_path / "attempt"
        failed = directory / "checkpoint-artifacts/00008"
        failed.mkdir(parents=True)
        (failed / "native.jsonl").write_bytes(b"original failed capture")
        (failed / "capture-error.json").write_text('{"error":"original mount outage"}')
        root = tmp_path / "sandboxes/sandbox"
        native = root / "opt/tb40-claude/projects/-app/session.jsonl"
        native.parent.mkdir(parents=True)
        messages = [
            {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "tool", "name": "Bash", "input": {"command": "true"}}],
            },
            {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "tool", "content": "real result"}],
            },
        ]
        rows = [
            {"type": "assistant", "uuid": "a", "parentUuid": None, "cwd": "/app", "message": messages[0]},
            {"type": "user", "uuid": "u", "parentUuid": "a", "cwd": "/app", "message": messages[1]},
        ]
        native.write_text("".join(json.dumps(row) + "\n" for row in rows))

        async def snapshot(sandbox):
            return SimpleNamespace(id="new-snapshot")

        pool = SimpleNamespace(store=SimpleNamespace(sandboxes=root.parent), snapshot=snapshot)
        engine = SimpleNamespace(pool=pool)
        task = SimpleNamespace(cwd="/app", snapshots=[])
        point = {"step": 7, "snapshot_id": "previous"}
        resume = {
            "point": point,
            "points": [point],
            "api": [],
            "request_count": 15,
            "started_at": time.time(),
            "elapsed_seconds": 2,
        }
        rollout = Rollout(engine, task, "root-0", directory, checkpoints=True, resume=resume)
        rollout.sandbox = SimpleNamespace(require_id=lambda: "sandbox")
        request = directory / "request.json"
        request.write_text(json.dumps({"messages": messages}))
        await rollout.capture({"messages": messages}, request)
        assert (failed / "native.jsonl").read_bytes() == b"original failed capture"
        assert json.loads((failed / "capture-error.json").read_text())["error"] == "original mount outage"
        assert rollout.points[-1]["step"] == 9
        assert (directory / "checkpoint-artifacts/00009/native.jsonl").is_file()
        assert task.snapshots == ["new-snapshot"]

    asyncio.run(check())


def test_merged_resume_control_keeps_continuation_and_repairs_parent_link():
    rows = [
        {"type": "user", "uuid": "u", "parentUuid": None, "message": {"role": "user", "content": "task"}},
        {
            "type": "assistant",
            "uuid": "r",
            "parentUuid": "u",
            "message": {"role": "assistant", "content": "No response requested."},
        },
        {
            "type": "user",
            "uuid": "m",
            "parentUuid": "r",
            "message": {
                "role": "user",
                "content": [
                    {"type": "text", "text": "CONTROL\n"},
                    {"type": "text", "text": "Continue after output limit."},
                ],
            },
        },
    ]
    cleaned = [
        json.loads(line)
        for line in clean_native(
            b"".join((json.dumps(row) + "\n").encode() for row in rows), "CONTROL"
        ).splitlines()
    ]
    assert [row["uuid"] for row in cleaned] == ["u", "m"]
    assert cleaned[-1]["parentUuid"] == "u"
    assert cleaned[-1]["message"]["content"] == [{"type": "text", "text": "Continue after output limit."}]


def test_fork_control_messages_are_removed_and_native_parent_links_are_repaired():
    rows = [
        {
            "type": "user",
            "uuid": "u",
            "parentUuid": None,
            "message": {"role": "user", "content": "original task"},
        },
        {
            "type": "assistant",
            "uuid": "r",
            "parentUuid": "u",
            "message": {"role": "assistant", "content": "No response requested."},
        },
        {
            "type": "user",
            "uuid": "m",
            "parentUuid": "r",
            "message": {"role": "user", "content": "RESUME_CONTROL"},
        },
        {
            "type": "assistant",
            "uuid": "a",
            "parentUuid": "m",
            "message": {
                "role": "assistant",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "real-call",
                        "name": "Bash",
                        "input": {"command": "cat answer"},
                    }
                ],
            },
        },
        {
            "type": "user",
            "uuid": "o",
            "parentUuid": "a",
            "message": {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "real-call",
                        "content": "real output",
                    }
                ],
            },
        },
    ]
    source = b"".join((json.dumps(r) + "\n").encode() for r in rows)
    cleaned = [json.loads(line) for line in clean_native(source, "RESUME_CONTROL").splitlines()]
    assert [r["uuid"] for r in cleaned] == ["u", "a", "o"]
    assert cleaned[1]["parentUuid"] == "u"
    assert cleaned[2]["message"] == rows[4]["message"]
    assert clean_native(clean_native(source, "RESUME_CONTROL"), "RESUME_CONTROL") == clean_native(
        source, "RESUME_CONTROL"
    )


def test_provider_thinking_tool_arguments_and_real_usage_survive_sse_parsing():
    events = [
        {
            "type": "message_start",
            "message": {
                "role": "assistant",
                "usage": {"input_tokens": 30, "cache_read_input_tokens": 70},
            },
        },
        {
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": ""},
        },
        {
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "reason"},
        },
        {
            "type": "content_block_start",
            "index": 1,
            "content_block": {
                "type": "tool_use",
                "id": "t",
                "name": "Bash",
                "input": {},
            },
        },
        {
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": '{"command":'},
        },
        {
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": '"true"}'},
        },
        {
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 23},
        },
        {"type": "message_stop"},
    ]
    raw = "\n\n".join("data: " + json.dumps(e) for e in events).encode()
    message, usage, complete = response_message(raw)
    assert complete
    assert usage == {
        "input_tokens": 30,
        "cache_read_input_tokens": 70,
        "output_tokens": 23,
    }
    assert message["content"][0]["thinking"] == "reason"
    assert message["content"][1]["input"] == {"command": "true"}
    assert not response_message(raw.rsplit(b"data:", 1)[0])[2]
