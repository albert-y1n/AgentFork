"""Claude native transcript boundaries and Anthropic SSE encoding."""

from pathlib import Path
import json
from aiohttp import web


def result_ids(messages: list[dict]) -> set[str]:
    return {
        block["tool_use_id"]
        for message in messages
        for block in (message.get("content") if isinstance(message.get("content"), list) else [])
        if block.get("type") == "tool_result"
    }


def native_cut(path: Path, required: set[str]) -> bytes | None:
    data = path.read_bytes()
    confirmed: set[str] = set()
    offset = 0
    for line in data.splitlines(keepends=True):
        if not line.endswith(b"\n"):
            break
        event = json.loads(line)
        confirmed |= result_ids([event.get("message", {})])
        offset += len(line)
        if required <= confirmed:
            return data[:offset]
    return None


async def send_message(request: web.Request, message: dict) -> web.StreamResponse:
    if not request["body"].get("stream"):
        return web.json_response(message)
    response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
    await response.prepare(request)

    async def emit(event: str, value: dict) -> None:
        await response.write(f"event: {event}\ndata: {json.dumps(value)}\n\n".encode())

    await emit(
        "message_start",
        {
            "type": "message_start",
            "message": {**message, "content": [], "stop_reason": None},
        },
    )
    for index, block in enumerate(message["content"]):
        if block["type"] == "tool_use":
            initial = {**block, "input": {}}
        elif block["type"] == "thinking":
            initial = {"type": "thinking", "thinking": ""}
        else:
            initial = {"type": "text", "text": ""}
        await emit(
            "content_block_start",
            {"type": "content_block_start", "index": index, "content_block": initial},
        )
        if block["type"] == "tool_use":
            delta = {
                "type": "input_json_delta",
                "partial_json": json.dumps(block["input"]),
            }
        elif block["type"] == "thinking":
            delta = {"type": "thinking_delta", "thinking": block["thinking"]}
        else:
            delta = {"type": "text_delta", "text": block["text"]}
        await emit(
            "content_block_delta",
            {"type": "content_block_delta", "index": index, "delta": delta},
        )
        await emit("content_block_stop", {"type": "content_block_stop", "index": index})
    await emit(
        "message_delta",
        {
            "type": "message_delta",
            "delta": {"stop_reason": message["stop_reason"], "stop_sequence": None},
            "usage": {"output_tokens": message.get("usage", {}).get("output_tokens", 0)},
        },
    )
    await emit("message_stop", {"type": "message_stop"})
    await response.write_eof()
    return response
