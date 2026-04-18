from __future__ import annotations

import json
import os
import subprocess


def test_run_cli_fails_fast_when_ingestion_key_is_missing() -> None:
    env = os.environ.copy()
    env.update(
        {
            "BUYER_DISCOVERY_USE_SAMPLE_SNAPSHOTS": "true",
            "BUYER_DISCOVERY_INTERNAL_API_KEY": "",
        }
    )

    result = subprocess.run(
        ["python3", "run.py", "--town", "Guntur"],
        cwd="/home/amaresh/projects/cold-storage-buyer-discovery",
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 1
    assert result.stdout == ""
    assert json.loads(result.stderr) == {
        "error": "BUYER_DISCOVERY_INTERNAL_API_KEY must be configured before ingestion.",
        "ingestRequested": False,
    }
