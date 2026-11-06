import hashlib

from agentfork.checkpoints import assess


def point(tmp_path):
    memory = tmp_path / "memory.tar.gz"
    memory.write_bytes(b"original image")
    return {
        "step": 5,
        "snapshot_id": "snapshot-5",
        "memory_checkpoint": {
            "archive": str(memory),
            "archive_sha256": hashlib.sha256(memory.read_bytes()).hexdigest(),
            "validation": {"passed": True},
        },
    }


def test_historical_validation_does_not_hide_deleted_disk(tmp_path):
    value = assess(point(tmp_path), tmp_path)
    assert value["checks"]["historically_validated"]
    assert not value["currently_available"]


def test_corrupt_payload_fails_even_with_valid_marker(tmp_path):
    value = point(tmp_path)
    (tmp_path / "snapshots/snapshot-5").mkdir(parents=True)
    (tmp_path / "memory.tar.gz").write_bytes(b"corruption")
    assert not assess(value, tmp_path)["currently_available"]


def test_available_materials_do_not_claim_a_new_restore(tmp_path):
    value = point(tmp_path)
    (tmp_path / "snapshots/snapshot-5").mkdir(parents=True)
    result = assess(value, tmp_path)
    assert result["status"] == "available_verified"
    assert not result["live_restore_performed"]


def test_snapshot_identifier_cannot_escape_store(tmp_path):
    value = point(tmp_path)
    value["snapshot_id"] = "../memory.tar.gz"
    assert not assess(value, tmp_path)["checks"]["disk_snapshot_present"]
