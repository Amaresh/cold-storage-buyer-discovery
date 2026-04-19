from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent.parent


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
        cwd=ROOT,
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


def test_run_cli_market_intel_uses_named_high_rate_scenario() -> None:
    env = os.environ.copy()
    env.update(
        {
            "BUYER_DISCOVERY_USE_SAMPLE_SNAPSHOTS": "true",
        }
    )

    result = subprocess.run(
        ["python3", "run.py", "--market-intel", "--market-scenario", "high-rate"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["scenario"] == "high-rate"
    assert payload["ingestRequested"] is False
    assert [signal["direction"] for signal in payload["marketIntelligenceRequest"]["signals"]] == [
        "SELL",
        "SELL",
    ]


def test_run_cli_market_intel_baseline_requires_ingestion_key_without_dry_run() -> None:
    env = os.environ.copy()
    env.update(
        {
            "BUYER_DISCOVERY_USE_SAMPLE_SNAPSHOTS": "true",
            "BUYER_DISCOVERY_INTERNAL_API_KEY": "",
        }
    )

    result = subprocess.run(
        ["python3", "run.py", "--market-intel"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 1
    assert result.stdout == ""
    assert json.loads(result.stderr) == {
        "error": "BUYER_DISCOVERY_INTERNAL_API_KEY must be configured before market-intelligence ingestion.",
        "ingestRequested": False,
    }


def test_run_cli_market_intel_dry_run_skips_ingestion_for_baseline() -> None:
    env = os.environ.copy()
    env.update(
        {
            "BUYER_DISCOVERY_USE_SAMPLE_SNAPSHOTS": "true",
            "BUYER_DISCOVERY_INTERNAL_API_KEY": "",
        }
    )

    result = subprocess.run(
        ["python3", "run.py", "--market-intel", "--dry-run"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["scenario"] == "baseline"
    assert payload["ingestRequested"] is False
