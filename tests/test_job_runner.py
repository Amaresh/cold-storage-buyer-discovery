from src.common.models import SourceConfig, TownSeed
from src.crawler.job_runner import build_crawl_jobs


def test_build_crawl_jobs_expands_towns_and_queries_in_deterministic_order() -> None:
    source = SourceConfig(name="directory_seed", enabled=True, priority=1)
    towns = (
        TownSeed(name="Khammam", state="Telangana", priority=2),
        TownSeed(name="Guntur", state="Andhra Pradesh", priority=1),
    )

    jobs = build_crawl_jobs(source, towns, ("chilli wholesaler", "gunj shop", "gunj shop"))

    assert [job.job_id for job in jobs] == [
        "directory-seed:guntur-andhra-pradesh:chilli-wholesaler",
        "directory-seed:guntur-andhra-pradesh:gunj-shop",
        "directory-seed:khammam-telangana:chilli-wholesaler",
        "directory-seed:khammam-telangana:gunj-shop",
    ]
    assert [job.query_text for job in jobs] == [
        "Guntur chilli wholesaler",
        "Guntur gunj shop",
        "Khammam chilli wholesaler",
        "Khammam gunj shop",
    ]


def test_build_crawl_jobs_is_repeatable_for_same_inputs() -> None:
    source = SourceConfig(name="directory_seed", enabled=True, priority=1)
    towns = (TownSeed(name="Guntur", state="Andhra Pradesh", priority=1),)
    queries = ("gunj shop", "chilli wholesaler")

    first = build_crawl_jobs(source, towns, queries)
    second = build_crawl_jobs(source, towns, queries)

    assert first == second
