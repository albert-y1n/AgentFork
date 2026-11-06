import asyncio
import json
from types import SimpleNamespace

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer
import pytest
import agentfork.rollout as rollout


@pytest.mark.parametrize("send_headers", [False, True])
@pytest.mark.parametrize("cancel_handler", [False, True])
def test_native_disconnect_cancels_upstream_without_retries(
    tmp_path, monkeypatch, send_headers, cancel_handler
):
    monkeypatch.setattr(rollout, "PROVIDER_POLL_SECONDS", 0.02)

    async def exercise():
        calls = []
        started = asyncio.Event()
        disconnected = asyncio.Event()

        async def provider(request):
            await request.json()
            calls.append(1)
            response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            if send_headers:
                await response.prepare(request)
                await response.write(b": incomplete response\n\n")
            started.set()
            try:
                while request.transport is not None and not request.transport.is_closing():
                    await asyncio.sleep(0.01)
            finally:
                # aiohttp may cancel the server handler immediately when its
                # client closes, before the polling loop sees a closing socket.
                disconnected.set()
            raise asyncio.CancelledError

        upstream = web.Application()
        upstream.router.add_post("/v1/messages", provider)
        async with TestServer(upstream) as upstream_server, aiohttp.ClientSession() as provider_client:
            actor = object.__new__(rollout.Rollout)
            actor.engine = SimpleNamespace(
                model="offline",
                key="offline",
                base_url=str(upstream_server.make_url("")).rstrip("/"),
                client=provider_client,
            )
            actor.directory = tmp_path
            actor.request_count = 0
            actor.context_origin = None
            actor.skip_restart_capture = False
            actor.starting_turn = None
            actor.api = []
            gateway = web.Application()
            gateway.router.add_post("/v1/messages", actor.handle)

            class GatewayServer(TestServer):
                async def _make_runner(self, **kwargs):
                    # Production currently does not cancel handlers on a
                    # disconnect; aiohttp's TestServer enables it by default.
                    kwargs["handler_cancellation"] = cancel_handler
                    return await super()._make_runner(**kwargs)

            async with GatewayServer(gateway) as gateway_server:
                async with aiohttp.ClientSession() as native_client:
                    pending = asyncio.create_task(
                        native_client.post(
                            gateway_server.make_url("/v1/messages"),
                            json={
                                "model": "offline",
                                "messages": [{"role": "user", "content": "Wait"}],
                                "stream": True,
                            },
                        )
                    )
                    await asyncio.wait_for(started.wait(), 2)
                    if send_headers:
                        response = await asyncio.wait_for(pending, 2)
                        response.close()
                    else:
                        pending.cancel()
                        await asyncio.gather(pending, return_exceptions=True)
                await asyncio.wait_for(disconnected.wait(), 2)
                for _ in range(100):
                    if actor.api:
                        break
                    await asyncio.sleep(0.01)
                assert len(calls) == 1
                assert len(actor.api) == 1
                row = actor.api[0]
                assert row["complete"] is False and row["error"] == "CancelledError"
                assert row["usage_unknown"] is True
                assert row["cancellation_reason"] == "native_client_disconnected"
                assert json.loads((tmp_path / "api-usage.json").read_text()) == actor.api

    asyncio.run(exercise())
