"""A controller restart must continue the saved branch, not replay its guide."""

import asyncio
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import time

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer
import pytest

from agentfork.protocol import send_message
from agentfork.rollout import Rollout, response_message, save_request


GUIDE = {"content": "Use the proposed repair once.", "commands": ["increment-counter"]}


@pytest.mark.parametrize("resuming", [False, True])
def test_fresh_branch_injects_guide_but_resumed_branch_calls_provider(tmp_path, resuming):
    async def check():
        received = []

        async def provider(request):
            body = await request.json()
            received.append(body)
            request["body"] = body
            return await send_message(
                request,
                {
                    "id": "msg_upstream",
                    "type": "message",
                    "role": "assistant",
                    "model": "offline",
                    "content": [{"type": "text", "text": "Continue from saved state."}],
                    "stop_reason": "end_turn",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 1, "output_tokens": 1},
                },
            )

        upstream = web.Application()
        upstream.router.add_post("/v1/messages", provider)
        parent = {
            "model": "offline",
            "stream": True,
            "system": [{"type": "text", "text": "original system"}],
            "messages": [
                {"role": "user", "content": "original task"},
                {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "toolu_review_original" if resuming else "old-tool",
                            "name": "Bash",
                            "input": {"command": "increment-counter" if resuming else "inspect"},
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "toolu_review_original" if resuming else "old-tool",
                            "content": "done",
                        }
                    ],
                },
            ],
        }
        saved = tmp_path / "parent.json.gz"
        save_request(saved, parent)
        point = {"step": 3, "request_path": str(saved), "snapshot_id": "anchor", "memory_checkpoint": {}}
        resume = (
            {
                "point": point,
                "request_count": 9,
                "points": [point],
                "api": [],
                "elapsed_seconds": 123,
                "started_at": time.time() - 123,
            }
            if resuming
            else None
        )
        async with TestServer(upstream) as server, aiohttp.ClientSession() as client:
            engine = SimpleNamespace(
                model="offline", key="offline", client=client, base_url=str(server.make_url("")).rstrip("/")
            )
            actor = Rollout(
                engine,
                SimpleNamespace(),
                "branch",
                tmp_path / "branch",
                origin=point,
                starting_turn=GUIDE,
                resume=resume,
            )
            body = deepcopy(parent)
            body["messages"].append({"role": "user", "content": actor.marker})
            gateway = web.Application()
            gateway.router.add_post("/v1/messages", actor.handle)
            async with TestServer(gateway) as gateway_server, aiohttp.ClientSession() as native:
                response = await native.post(gateway_server.make_url("/v1/messages"), json=body)
                assert response.status == 200
                message, _, complete = response_message(await response.read())
                assert complete
            if resuming:
                assert message["id"] == "msg_upstream" and len(received) == 1
                assert received[0]["messages"] == parent["messages"]
                assert len(actor.api) == 1 and actor.api[0]["complete"]
                assert actor.elapsed_before == 123 and actor.request_count == 10
            else:
                assert message["id"].startswith("msg_review_") and received == []
                assert actor.api == []
            assert actor.starting_turn_sent

    asyncio.run(check())


@pytest.mark.parametrize("resuming", [False, True])
def test_environment_fallback_replays_guide_only_for_pre_branch_parent(tmp_path, resuming):
    async def check():
        point = {"step": 3, "snapshot_id": "anchor", "memory_checkpoint": {"archive": "unused"}}
        resume = (
            {
                "point": point,
                "request_count": 9,
                "points": [],
                "api": [],
                "elapsed_seconds": 123,
                "started_at": time.time() - 123,
            }
            if resuming
            else None
        )

        async def fork(*args, **kwargs):
            return SimpleNamespace(), {}

        actor = Rollout(
            SimpleNamespace(memory_backend=SimpleNamespace(fork=fork)),
            SimpleNamespace(),
            "branch",
            tmp_path / "branch",
            origin=point,
            starting_turn=GUIDE,
            resume=resume,
        )
        actor.starting_turn_sent = True
        await actor.recover_environment(tmp_path / "failed-capture")
        assert actor.context_origin is actor.origin
        assert actor.starting_turn_sent is resuming
        assert actor.elapsed_before == (123 if resuming else 0)

    asyncio.run(check())
