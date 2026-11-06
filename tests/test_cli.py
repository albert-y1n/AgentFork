import subprocess
import sys


def test_run_help_dispatches_to_campaign_without_credentials():
    result = subprocess.run(
        [sys.executable, "-m", "agentfork", "run", "--help"], capture_output=True, text=True
    )
    assert result.returncode == 0
    assert "--task-workers" in result.stdout and "--discard-completed-checkpoints" in result.stdout


def test_view_help_dispatches_to_viewer():
    result = subprocess.run(
        [sys.executable, "-m", "agentfork", "view", "--help"], capture_output=True, text=True
    )
    assert result.returncode == 0
    assert "--campaign" in result.stdout and "--port" in result.stdout
