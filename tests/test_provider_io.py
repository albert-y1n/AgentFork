import asyncio, json
import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer
from agentfork.provider_io import fetch
from agentfork.provider_io import ProviderError
import pytest
from agentfork.rollout import response_message


def test_partial_sse_is_retried_before_any_completion_is_returned():
    async def check():
        count = 0
        rows = []
        complete = b'data: {"type":"message_stop"}\n\n'

        async def endpoint(request):
            nonlocal count
            count += 1
            if count == 1:
                r = web.StreamResponse()
                await r.prepare(request)
                await r.write(
                    b'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"discard me"}}\n\n'
                )
                request.transport.close()
                return r
            return web.Response(body=complete)

        async def record(row, raw):
            rows.append((row, raw))

        def validate(raw):
            if not response_message(raw)[2]:
                raise ValueError("incomplete")
            return raw

        app = web.Application()
        app.router.add_post("/v1/messages", endpoint)
        async with TestServer(app) as server, aiohttp.ClientSession() as client:
            value, raw = await fetch(
                client, str(server.make_url("/v1/messages")), {}, {}, validate=validate, on_attempt=record
            )
        assert count == 2 and raw == value == complete
        assert rows[0][0]["complete"] is False and rows[0][0]["usage_unknown"]
        assert rows[1][0]["complete"] is True

    asyncio.run(check())


def test_truncated_reviewer_json_is_retried_and_attempts_are_bounded():
    async def check():
        count = 0
        rows = []

        async def endpoint(request):
            nonlocal count
            count += 1
            return web.Response(body=b"{bad" if count < 3 else b'{"choices":[1]}')

        async def record(row, raw):
            rows.append(row)

        app = web.Application()
        app.router.add_post("/", endpoint)
        async with TestServer(app) as server, aiohttp.ClientSession() as client:
            data, _ = await fetch(
                client, str(server.make_url("/")), {}, {}, validate=json.loads, on_attempt=record
            )
        assert data == {"choices": [1]} and count == 3
        assert [r["complete"] for r in rows] == [False, False, True]

    asyncio.run(check())


def test_error_event_after_message_stop_is_not_a_successful_completion():
    raw = b'data: {"type":"message_start","message":{"role":"assistant"}}\n\ndata: {"type":"message_stop"}\n\nevent: error\ndata: {"error":"stream_read_error"}\n\n'
    assert not response_message(raw)[2]


def test_response_limit_is_per_call_and_overflow_is_recorded_without_retries():
    async def check():
        rows = []
        requests = 0
        body = b"x" * 512

        async def endpoint(request):
            nonlocal requests
            requests += 1
            return web.Response(body=body)

        async def record(row, raw):
            rows.append(row)

        app = web.Application()
        app.router.add_post("/", endpoint)
        async with TestServer(app) as server, aiohttp.ClientSession() as client:
            with pytest.raises(ProviderError, match="exceeded"):
                await fetch(
                    client,
                    str(server.make_url("/")),
                    {},
                    {},
                    validate=lambda raw: raw,
                    on_attempt=record,
                    max_response_bytes=128,
                )
            assert requests == 1 and rows[-1]["complete"] is False and rows[-1]["usage_unknown"]
            value, raw = await fetch(
                client,
                str(server.make_url("/")),
                {},
                {},
                validate=lambda raw: raw,
                on_attempt=record,
                max_response_bytes=1024,
            )
            assert value == raw == body and requests == 2 and rows[-1]["complete"]

    asyncio.run(check())
