from agentfork.config import PACKAGE
import hashlib, json
from pathlib import Path
from types import SimpleNamespace

import agentfork.compact_checkpoints as compaction


def test_compaction_requires_a_live_reader_with_matching_implementation(tmp_path, monkeypatch):
    configuration = {
        "checkpoint_reference_storage_supported": True,
        "implementation_sha256": {
            name: hashlib.sha256((compaction.PACKAGE / name).read_bytes()).hexdigest()
            for name in ["checkpoint_storage.py", "criu_backend.py"]
        },
    }
    path = tmp_path / "configuration.json"
    path.write_text(json.dumps(configuration))
    monkeypatch.setattr(compaction, "process_info", lambda *args: {"alive": True})
    assert compaction.compatible(tmp_path)
    configuration["implementation_sha256"]["criu_backend.py"] = "another-reader"
    path.write_text(json.dumps(configuration))
    assert not compaction.compatible(tmp_path)
    configuration["checkpoint_reference_storage_supported"] = False
    path.write_text(json.dumps(configuration))
    assert not compaction.compatible(tmp_path)


def test_one_shot_does_not_replace_compactor_daemon_identity_or_status(tmp_path, monkeypatch):
    (tmp_path / "compactor.pid").write_text("777\n")
    status = {"state": "watching", "pid": 777, "updated_at": 100}
    (tmp_path / "compaction-status.json").write_text(json.dumps(status))
    (tmp_path / "configuration.json").write_text(json.dumps({"task_names": []}))
    monkeypatch.setattr(compaction, "compatible", lambda *args: True)
    compaction.main(SimpleNamespace(campaign=tmp_path, offline=False, once=True, max_files=12, interval=30))
    assert (tmp_path / "compactor.pid").read_text() == "777\n"
    assert json.loads((tmp_path / "compaction-status.json").read_text()) == status
    assert json.loads((tmp_path / "compaction-once-status.json").read_text())["state"] == "complete"
