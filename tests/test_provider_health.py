import json
from types import SimpleNamespace

import pytest
import agentfork.provider_health as health


@pytest.mark.parametrize("status,valid", [(200, True), (200, False), (502, False), (401, False)])
def test_inference_probe_gates_work_and_never_records_credentials(tmp_path, monkeypatch, status, valid):
    calls = []

    class Client:
        def __init__(self, **kwargs):
            assert kwargs["trust_env"] is False

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def post(self, url, **kwargs):
            calls.append((url, kwargs))
            return SimpleNamespace(status_code=status, json=lambda: {"type": "message" if valid else "error"})

    monkeypatch.setattr(health.httpx, "Client", Client)
    provider = {
        "base_url": "https://provider.invalid",
        "api_key": "private-test-key",
        "model": "original-model",
    }
    result = health.probe(provider, tmp_path)
    assert result["state"] == ("available" if status == 200 and valid else "unavailable")
    assert health.unavailable(tmp_path) == (result["state"] == "unavailable")
    assert calls[0][1]["json"]["model"] == "original-model"
    assert "private-test-key" not in (tmp_path / "provider-health.json").read_text()
    assert "private-test-key" not in json.dumps(result)


def test_transport_exception_is_recorded_without_exception_message(tmp_path, monkeypatch):
    def fail(**kwargs):
        raise RuntimeError("sensitive backend text")

    monkeypatch.setattr(health.httpx, "Client", fail)
    result = health.probe(
        {"base_url": "https://provider.invalid", "api_key": "private-key", "model": "same"}, tmp_path
    )
    assert result["state"] == "unavailable" and result["error_type"] == "RuntimeError"
    assert "sensitive" not in json.dumps(result)


def test_success_clears_the_queue_outage_flag_without_removing_history(tmp_path):
    failed = health.save_state(tmp_path, "unavailable", source="actor_gateway", status=502)
    assert health.unavailable(tmp_path)
    good = health.save_state(tmp_path, "available", source="actor_gateway", status=200)
    assert not health.unavailable(tmp_path) and good["at"] >= failed["at"]
