"""Check trace boundaries, actual SSE reconstruction and read-only file scope."""

import gzip
import json
import hashlib

import pytest
from aiohttp import web

from agentfork.viewer.server import Store, normalize, parse_response, redact
import agentfork.viewer.server as server


def audit_fixture(tmp_path, monkeypatch, *, managed=True):
    root = tmp_path / "campaign"
    parent = root / "sample/attempts/root-0"
    child = root / "sample/attempts/branch-r1-0"
    parent.mkdir(parents=True)
    child.mkdir(parents=True)
    (root / "configuration.json").write_text(json.dumps({"task_names": ["sample"]}))
    store = Store(root)
    monkeypatch.setattr(store, "trace", lambda *args: {"origin": {"attempt": "root-0", "step": 1}})
    managed_root = tmp_path / "managed-native"
    managed_root.mkdir()
    monkeypatch.setattr(server, "MANAGED_NATIVE_ROOT", managed_root)
    native = parent / "checkpoint-artifacts/00001/native.jsonl"
    native.parent.mkdir(parents=True)
    payload = b'{"original":"native prefix"}\n'
    retained = (managed_root if managed else tmp_path) / "payload.jsonl"
    retained.write_bytes(payload)
    native.symlink_to(retained)
    point = {
        "step": 1,
        "request_path": str(parent / "requests/00001.json.gz"),
        "native_path": str(native),
        "byte_length": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    (parent / "checkpoints.json").write_text(json.dumps([point]))
    for folder, index in [(parent, 1), (child, 0)]:
        (folder / "requests").mkdir()
        with gzip.open(folder / "requests" / f"{index:05d}.json.gz", "wt") as stream:
            json.dump({"messages": [{"role": "user", "content": "original"}]}, stream)
    (child / "api-usage.json").write_text(json.dumps([{"request": 0, "complete": True, "http_status": 200}]))
    return store, parent, point, retained


def test_audit_accepts_exact_managed_checkpoint_native_symlink(tmp_path, monkeypatch):
    store, _, _, _ = audit_fixture(tmp_path, monkeypatch)
    audit = store.audit("sample", "branch-r1-0")
    assert audit["available"] and audit["native_prefix_hash_valid"]


def test_audit_rejects_native_symlink_outside_retention_store(tmp_path, monkeypatch):
    store, _, _, _ = audit_fixture(tmp_path, monkeypatch, managed=False)
    with pytest.raises(web.HTTPBadRequest):
        store.audit("sample", "branch-r1-0")


def test_audit_rejects_forged_pointer_to_another_managed_file(tmp_path, monkeypatch):
    store, parent, point, retained = audit_fixture(tmp_path, monkeypatch)
    other = retained.with_name("other.jsonl")
    other.write_bytes(retained.read_bytes())
    point["native_path"] = str(other)
    (parent / "checkpoints.json").write_text(json.dumps([point]))
    with pytest.raises(web.HTTPBadRequest):
        store.audit("sample", "branch-r1-0")


def test_branch_turns_keep_inherited_history_and_actual_checkpoint_boundary():
    messages = [
        {"role": "user", "content": "task"},
        {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": "read",
                    "name": "Read",
                    "input": {"file_path": "/app/a"},
                }
            ],
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": "read",
                    "content": [{"type": "text", "text": "early"}],
                }
            ],
        },
        {
            "role": "assistant",
            "content": [
                {"type": "thinking", "thinking": "new plan"},
                {
                    "type": "tool_use",
                    "id": "fix",
                    "name": "Bash",
                    "input": {"command": "fix"},
                },
            ],
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": "fix",
                    "content": "changed",
                    "is_error": True,
                }
            ],
        },
        {"role": "assistant", "content": [{"type": "text", "text": "finished"}]},
    ]
    turns = normalize(messages, [{"step": 1, "message_count": 5}], {"message_count": 3})
    assert [t["inherited"] for t in turns] == [True, False, False]
    assert [t["guided"] for t in turns] == [False, True, False]
    assert turns[0]["tools"][0]["output"] == "early"
    assert turns[1]["tools"][0]["output"] == "changed"
    assert turns[1]["tools"][0]["is_error"]
    assert [t["checkpoints"] for t in turns] == [[], [1], []]


def test_persisted_sse_reconstructs_thinking_and_split_tool_json(tmp_path):
    path = tmp_path / "response.sse.gz"
    rows = [
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
                "name": "Bash",
                "id": "x",
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
            "delta": {"type": "input_json_delta", "partial_json": '"pwd"}'},
        },
    ]

    def save(items):
        with gzip.open(path, "wt") as stream:
            stream.write("\n\n".join("data: " + json.dumps(row) for row in items))

    save(rows)
    assert parse_response(path) is None
    save(rows + [{"type": "message_stop"}])
    content = parse_response(path)["content"]
    assert content[0]["thinking"] == "reason"
    assert content[1]["input"] == {"command": "pwd"}


def test_only_configured_tasks_and_attempts_are_readable(tmp_path):
    (tmp_path / "configuration.json").write_text(json.dumps({"task_names": ["example"]}))
    store = Store(tmp_path)
    with pytest.raises(web.HTTPNotFound):
        store.task_path("../../.private")
    with pytest.raises(web.HTTPNotFound):
        store.attempt_path("example", "../../secrets")


def test_secrets_are_redacted_in_nested_data_and_terminal_output():
    secret = "sk-" + "x" * 30
    result = redact(
        {
            "api_key": "hidden",
            "nested": [{"text": f"curl -H 'Authorization: Bearer {secret}'"}],
            "env": "ANTHROPIC_AUTH_TOKEN=abcdef123456789\nPATH=/bin",
        }
    )
    assert result["api_key"] == "[REDACTED]"
    assert secret not in result["nested"][0]["text"]
    assert "abcdef123456789" not in result["env"]
    assert "PATH=/bin" in result["env"]


def test_campaign_switching_is_allowlisted_and_keeps_history_readable(tmp_path):
    import asyncio
    from aiohttp.test_utils import TestClient, TestServer
    from agentfork.viewer.server import create_app

    async def check():
        current, history = tmp_path / "current", tmp_path / "history"
        for path, task in [(current, "new-task"), (history, "old-task")]:
            path.mkdir()
            (path / "configuration.json").write_text(json.dumps({"task_names": [task]}))
        (current / "health.json").write_text(
            json.dumps({"state": "needs_attention", "failure_counts": {"checkpoint_capture_error": 1}})
        )
        app = create_app(
            current,
            {
                "criu": {"path": str(current), "label": "Current"},
                "history": {"path": str(history), "label": "History"},
            },
        )
        async with TestClient(TestServer(app)) as client:
            response = await client.get("/api/overview")
            assert response.status == 200
            current_data = await response.json()
            assert current_data["tasks"][0]["name"] == "new-task"
            assert current_data["health"]["failure_counts"]["checkpoint_capture_error"] == 1
            response = await client.get("/api/overview?campaign=history")
            data = await response.json()
            assert data["tasks"][0]["name"] == "old-task"
            assert data["campaign_id"] == "history"
            assert data["health"] is None
            assert len(data["campaigns"]) == 2
            response = await client.get("/api/overview?campaign=/etc")
            assert response.status == 404

    asyncio.run(check())
