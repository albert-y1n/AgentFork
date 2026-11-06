"""Retry transport failures before exposing any partial model completion."""

from __future__ import annotations
import asyncio, json, time
import aiohttp

RETRY_STATUS = {408, 429, 500, 502, 503, 504}


class ProviderError(RuntimeError):
    def __init__(self, status, message):
        self.status = status
        super().__init__(message)


def chat_completion(raw):
    """Reconstruct a complete OpenAI chat stream, keeping reasoning and usage."""
    if raw.lstrip().startswith(b"{"):
        return json.loads(raw)
    message = {"role": "assistant", "content": "", "reasoning_content": ""}
    fragments = {"content": [], "reasoning_content": []}
    result = {"choices": [{"index": 0, "message": message, "finish_reason": None}], "usage": {}}
    complete = False
    for line in raw.decode().splitlines():
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            continue
        event = json.loads(data)
        if event.get("error"):
            raise ValueError("Reviewer stream returned an error: " + str(event["error"])[:1000])
        for key in ["id", "model", "created"]:
            if key in event:
                result[key] = event[key]
        if event.get("usage"):
            result["usage"] = event["usage"]
        for choice in event.get("choices") or []:
            if choice.get("index", 0) != 0:
                continue
            delta = choice.get("delta") or {}
            for key in ["content", "reasoning_content"]:
                if delta.get(key):
                    fragments[key].append(delta[key])
            if choice.get("finish_reason"):
                result["choices"][0]["finish_reason"] = choice["finish_reason"]
                complete = True
    if not complete:
        raise ValueError("Reviewer stream ended without finish_reason")
    for key, parts in fragments.items():
        message[key] = "".join(parts)
    return result


async def fetch(
    client,
    url,
    body,
    headers,
    *,
    validate,
    on_attempt,
    on_status=None,
    on_progress=None,
    attempts=3,
    max_response_bytes=64 * 2**20,
):
    for attempt in range(attempts):
        started = time.monotonic()
        raw = bytearray()
        row = {"provider_attempt": attempt, "started_at": time.time()}
        retryable = True
        try:
            last_progress = 0
            if on_progress:
                await on_progress({**row, "bytes_received": 0})
            async with client.post(url, json=body, headers=headers) as response:
                row["http_status"] = response.status
                if on_status:
                    await on_status(response.status)
                async for chunk in response.content.iter_any():
                    raw.extend(chunk)
                    if on_progress and time.monotonic() - last_progress >= 5:
                        await on_progress({**row, "bytes_received": len(raw), "last_byte_at": time.time()})
                        last_progress = time.monotonic()
                    if len(raw) > max_response_bytes:
                        retryable = False
                        raise ProviderError(502, f"Provider response exceeded {max_response_bytes} bytes")
                if response.status != 200:
                    retryable = response.status in RETRY_STATUS
                    raise ProviderError(response.status, bytes(raw).decode(errors="replace")[:2000])
                value = validate(bytes(raw))
                row.update(complete=True, seconds=time.monotonic() - started)
                await on_attempt(row, bytes(raw))
                return value, bytes(raw)
        except asyncio.CancelledError:
            row.update(
                complete=False, error="CancelledError", usage_unknown=True, seconds=time.monotonic() - started
            )
            await on_attempt(row, bytes(raw))
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, ProviderError) as error:
            row.update(
                complete=False,
                error=type(error).__name__,
                error_detail=str(error)[:2000],
                usage_unknown=True,
                seconds=time.monotonic() - started,
            )
            await on_attempt(row, bytes(raw))
            if not retryable or attempt == attempts - 1:
                raise
            await asyncio.sleep(min(2, 0.5 * 2**attempt))
