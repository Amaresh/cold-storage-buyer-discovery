from dataclasses import replace

from src.common.models import SourceConfig, TownSeed
from src.crawler.source_registry import SourceRegistry
from src.crawler.source_policy import SOURCE_POLICIES


def test_schedule_jobs_only_for_enabled_sources() -> None:
    registry = SourceRegistry(
        (
            SourceConfig(name="trade_board", enabled=False, priority=1),
            SourceConfig(name="directory_seed", enabled=True, priority=2),
            SourceConfig(name="broker_notes", enabled=True, priority=3),
        )
    )

    jobs = registry.schedule_jobs(
        (TownSeed(name="Guntur", state="Andhra Pradesh", priority=1),),
        ("gunj shop",),
    )

    assert [job.source_name for job in jobs] == ["directory_seed", "broker_notes"]
    assert all(job.source_name != "trade_board" for job in jobs)


def test_schedule_jobs_returns_empty_when_every_source_is_disabled() -> None:
    registry = SourceRegistry((SourceConfig(name="trade_board", enabled=False),))

    jobs = registry.schedule_jobs(
        (TownSeed(name="Guntur", state="Andhra Pradesh", priority=1),),
        ("chilli wholesaler",),
    )

    assert jobs == []


def test_schedule_jobs_skips_sources_disabled_by_policy(monkeypatch) -> None:
    monkeypatch.setitem(
        SOURCE_POLICIES,
        "business_directory",
        replace(SOURCE_POLICIES["business_directory"], disabled_reason="Temporarily disabled"),
    )
    registry = SourceRegistry((SourceConfig(name="business_directory", enabled=True),))

    jobs = registry.schedule_jobs(
        (TownSeed(name="Guntur", state="Andhra Pradesh", priority=1),),
        ("chilli wholesaler",),
    )

    assert jobs == []


def test_schedule_jobs_warns_when_source_policy_is_missing(caplog) -> None:
    registry = SourceRegistry((SourceConfig(name="directory_seed", enabled=True),))

    with caplog.at_level("WARNING"):
        jobs = registry.schedule_jobs(
            (TownSeed(name="Guntur", state="Andhra Pradesh", priority=1),),
            ("gunj shop",),
        )

    assert jobs
    assert "No source policy registered for directory_seed" in caplog.text
