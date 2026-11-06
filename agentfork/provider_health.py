"""Gate new benchmark work on verified provider recovery, without logging keys."""

from pathlib import Path
import json
import os
import time
import uuid

import httpx


def save_state(directory, state, *, source, status=None, error_type=None):
    path = Path(directory) / "provider-health.json"
    value = {"at": time.time(), "state": state, "source": source, "http_status": status}
    if error_type:
        value["error_type"] = error_type
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    with temporary.open("x") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    return value


def unavailable(directory):
    try:
        return (
            json.loads((Path(directory) / "provider-health.json").read_text()).get("state") == "unavailable"
        )
    except (OSError, ValueError):
        return False


def probe(provider, directory, *, timeout=25):
    """A tiny same-model completion checks inference, not just gateway uptime."""
    headers = {
        "Authorization": "Bearer " + provider["api_key"],
        "x-api-key": provider["api_key"],
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    body = {
        "model": provider["model"],
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "Reply OK."}],
    }
    try:
        with httpx.Client(timeout=timeout, trust_env=False) as client:
            response = client.post(
                provider["base_url"].rstrip("/") + "/v1/messages", headers=headers, json=body
            )
            valid = response.status_code == 200 and response.json().get("type") == "message"
            return save_state(
                directory,
                "available" if valid else "unavailable",
                source="supervisor_inference_probe",
                status=response.status_code,
            )
    except Exception as error:
        return save_state(
            directory, "unavailable", source="supervisor_inference_probe", error_type=type(error).__name__
        )
