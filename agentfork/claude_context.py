"""Keep Claude Code's resume control exchange out of the actor's model input."""

from __future__ import annotations

from copy import deepcopy
import re
from typing import Any


def semantic_content(value: Any) -> Any:
    """Ignore cache placement, which the CLI moves when a history grows."""
    if isinstance(value, dict):
        return {
            k: deepcopy(v) if k == "input" else semantic_content(v)
            for k, v in value.items()
            if k != "cache_control"
        }
    if isinstance(value, list):
        return [semantic_content(v) for v in value]
    return value


def text_only(message: dict) -> str | None:
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list) and len(content) == 1 and content[0].get("type") == "text":
        return content[0].get("text")
    return None


def split_resume_control(message: dict, marker: str):
    """Remove the exact control block while retaining CLI continuation text.

    After max_tokens, Claude Code can coalesce its continuation prompt with
    the resume input, adding one newline to the control block.
    """
    if message.get("role") != "user":
        return 0, message
    content = message.get("content")
    if isinstance(content, str):
        return (1, None) if content in (marker, marker + "\n") else (0, message)
    if not isinstance(content, list):
        return 0, message
    controls = [
        index
        for index, block in enumerate(content)
        if block.get("type") == "text" and block.get("text") in (marker, marker + "\n")
    ]
    if not controls:
        return 0, message
    remaining = [deepcopy(block) for index, block in enumerate(content) if index not in controls]
    return len(controls), ({**deepcopy(message), "content": remaining} if remaining else None)


_IMAGE_HINT = re.compile(
    r"\[Image: original \d+x\d+, displayed at \d+x\d+\. "
    r"Multiply coordinates by \d+(?:\.\d+)? to map to original image\.\]"
)

_DATE_CHANGE = re.compile(
    r"The date has changed\. Today's date is now \d{4}-\d{2}-\d{2}\. "
    r"No need to announce the new date — the user's own clock shows it\."
)


def cli_date_reminder(message):
    return message.get("role") == "system" and bool(_DATE_CHANGE.fullmatch(text_only(message) or ""))


def durable_history(history):
    """Compare durable observations; regenerate only known CLI attachments.

    File-change reminders and image-coordinate hints are not written to the
    native transcript by Claude Code. The original saved request remains the
    authoritative model input, including those attachments.
    """
    result = []
    for message in history:
        text = text_only(message) or ""
        if message.get("role") == "system" and (
            cli_date_reminder(message)
            or text.startswith("# Environment")
            or (
                text.startswith("Note: ")
                and " changed on disk since you last read it. That's usually deliberate, " in text
            )
        ):
            continue
        message = deepcopy(message)
        content = message.get("content")
        if message.get("role") == "user" and isinstance(content, list):
            has_image = any(
                block.get("type") == "tool_result"
                and isinstance(block.get("content"), list)
                and any(item.get("type") == "image" for item in block["content"])
                for block in content
            )
            if has_image:
                message["content"] = [
                    block
                    for block in content
                    if not (block.get("type") == "text" and _IMAGE_HINT.fullmatch(block.get("text", "")))
                ]
        # Removing the CLI-only exchange can leave consecutive user messages.
        # Native replay coalesces their blocks without changing block order or
        # content. Compare that form; forward the original saved prefix below.
        if result and result[-1].get("role") == message.get("role") == "user":

            def blocks(value):
                return [{"type": "text", "text": value}] if isinstance(value, str) else value

            result[-1]["content"] = blocks(result[-1].get("content", [])) + blocks(message.get("content", []))
        else:
            result.append(message)
    return result


def restore_model_prefix(payload: dict, parent_request: dict, marker: str) -> dict:
    """Validate the recovered prefix and strip the CLI-only resume exchange.

    Claude Code requires a new input when resuming headlessly. On a prefix
    ending in tool results it can also add 'No response requested.'. Neither
    is an actor observation. Remove only the known exchange at the expected
    position, then require the inherited history to match before forwarding.
    The caller keeps both raw and canonical requests for audit.
    """
    messages = deepcopy(payload["messages"])
    prefix = parent_request["messages"]
    controls = [
        (i, count, remainder)
        for i, message in enumerate(messages)
        for count, remainder in [split_resume_control(message, marker)]
        if count
    ]
    if sum(count for _, count, _ in controls) != 1:
        raise ValueError("The expected headless resume control marker is missing or duplicated")
    cursor, _, remainder = controls[0]
    boundary = cursor
    if (
        boundary
        and messages[boundary - 1].get("role") == "assistant"
        and text_only(messages[boundary - 1]) == "No response requested."
    ):
        boundary -= 1

    if semantic_content(durable_history(messages[:boundary])) != semantic_content(durable_history(prefix)):
        raise ValueError("Claude Code changed the inherited model conversation")
    if semantic_content(payload.get("system")) != semantic_content(parent_request.get("system")):
        raise ValueError("Claude Code changed the checkpoint's system context")
    # The CLI regenerates its date-change attachment after the resume input.
    # Keep the saved original in the inherited prefix, and omit only an exact
    # duplicate before any new actor/user turn. A genuinely newer date stays.
    inherited_dates = [semantic_content(message) for message in prefix if cli_date_reminder(message)]
    suffix = []
    leading_system = True
    for message in messages[cursor + 1 :]:
        if leading_system and cli_date_reminder(message) and semantic_content(message) in inherited_dates:
            continue
        if message.get("role") != "system":
            leading_system = False
        suffix.append(message)
    return {**deepcopy(payload), "messages": deepcopy(prefix) + ([remainder] if remainder else []) + suffix}
