import pytest

from agentfork.config import load_provider, task_inventory
from agentfork.viewer.server import redact


def test_provider_missing_credentials_error_only_names_variables(monkeypatch):
    for name in ["AGENTFORK_API_KEY", "AGENTFORK_BASE_URL", "AGENTFORK_MODEL"]:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValueError, match="AGENTFORK_API_KEY"):
        load_provider()


def test_provider_is_environment_driven(monkeypatch):
    monkeypatch.setenv("AGENTFORK_API_KEY", "local-test-key")
    monkeypatch.setenv("AGENTFORK_BASE_URL", "http://localhost:9000/")
    monkeypatch.setenv("AGENTFORK_MODEL", "local-test-model")
    assert load_provider() == {
        "api_key": "local-test-key",
        "base_url": "http://localhost:9000",
        "model": "local-test-model",
    }


def test_inventory_discovers_without_changing_tasks(tmp_path):
    folder = tmp_path / "example"
    folder.mkdir()
    definition = folder / "task.toml"
    original = b'[environment]\ngpus=0\ndocker_image="example/image:tag"\n'
    definition.write_bytes(original)
    value = task_inventory(tmp_path)
    assert value[0]["task"] == "example" and not value[0]["gpus"] and not value[0]["compose"]
    assert definition.read_bytes() == original


def test_viewer_redacts_github_credentials_in_strings():
    assert redact("ghp_" + "x" * 36) == "[REDACTED]"
    assert redact("github_pat_" + "x" * 40) == "[REDACTED]"
