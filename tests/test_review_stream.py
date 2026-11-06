import asyncio
import json
from types import SimpleNamespace

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from agentfork.campaign import Case, REVIEW_MAX_OUTPUT_TOKENS
from agentfork.provider_io import chat_completion


def stream(*events):
    return "".join("data: " + json.dumps(e) + "\n\n" for e in events).encode()


def test_reasoning_final_json_and_usage_survive_stream_reconstruction():
    raw = (
        stream(
            {"choices": [{"delta": {"reasoning_content": "reason"}}]},
            {"choices": [{"delta": {"content": '{"ok":'}}]},
            {"choices": [{"delta": {"content": "true}"}, "finish_reason": "stop"}]},
            {"choices": [], "usage": {"completion_tokens": 9}},
        )
        + b"data: [DONE]\n\n"
    )
    result = chat_completion(raw)
    assert result["choices"][0]["message"] == {
        "role": "assistant",
        "content": '{"ok":true}',
        "reasoning_content": "reason",
    }
    assert result["usage"]["completion_tokens"] == 9


def test_partial_stream_or_late_error_never_becomes_a_branch_plan():
    raw = stream({"choices": [{"delta": {"content": '{"branches":[]}'}}]})
    with pytest.raises(ValueError, match="finish_reason"):
        chat_completion(raw)
    raw += stream(
        {"choices": [{"delta": {}, "finish_reason": "stop"}]}, {"error": {"message": "stream failed"}}
    )
    with pytest.raises(ValueError, match="error"):
        chat_completion(raw)


def test_length_exhaustion_remains_visible_for_retry_and_monitoring():
    result = chat_completion(
        stream({"choices": [{"delta": {"reasoning_content": "unfinished"}, "finish_reason": "length"}]})
    )
    assert result["choices"][0]["finish_reason"] == "length"
    assert result["choices"][0]["message"]["content"] == ""


def test_review_request_uses_supported_cap_streams_and_preserves_previous_files(tmp_path):
    async def check():
        received = []

        async def endpoint(request):
            received.append(await request.json())
            return web.Response(
                body=stream(
                    {"choices": [{"delta": {"content": '{"branches":[]}'}, "finish_reason": "stop"}]},
                    {"choices": [], "usage": {"completion_tokens": 4}},
                ),
                content_type="text/event-stream",
            )

        app = web.Application()
        app.router.add_post("/v1/chat/completions", endpoint)
        output = tmp_path / "plan-0"
        old = output.with_suffix(".request.json")
        old.write_text("old request must remain")
        async with TestServer(app) as server, aiohttp.ClientSession() as client:
            case = SimpleNamespace(
                directory=tmp_path,
                reviews=[],
                engine=SimpleNamespace(
                    client=client,
                    base_url=str(server.make_url("")).rstrip("/"),
                    key="offline",
                    model="offline",
                ),
            )
            result = await Case.review_call(case, "Return a plan", output)
        assert result == {"branches": []}
        assert old.read_text() == "old request must remain"
        assert len(list(tmp_path.glob("plan-0-resume-*.request.json"))) == 1
        assert received[0]["stream"] and received[0]["reasoning_effort"] == "max"
        assert received[0]["max_tokens"] == REVIEW_MAX_OUTPUT_TOKENS
        assert "max_completion_tokens" not in received[0]
        assert case.reviews[0]["usage"]["completion_tokens"] == 4
        assert list(tmp_path.glob("*.sse.gz"))

    asyncio.run(check())
