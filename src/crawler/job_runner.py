"""Expand towns and intent queries into deterministic crawl jobs."""

from __future__ import annotations

from collections.abc import Sequence

from src.common.models import CrawlJob, SourceConfig, TownSeed, slugify


def _ordered_towns(towns: Sequence[TownSeed]) -> tuple[TownSeed, ...]:
    return tuple(
        sorted(
            towns,
            key=lambda town: (
                town.priority,
                town.name.casefold(),
                town.state.casefold(),
            ),
        )
    )


def _unique_queries(query_seeds: Sequence[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for query in query_seeds:
        normalized = " ".join(query.split())
        if not normalized:
            continue
        key = normalized.casefold()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(normalized)
    return tuple(ordered)


class JobRunner:
    """Builds crawl jobs from lightweight source, town, and query inputs."""

    def __init__(self, towns: Sequence[TownSeed], query_seeds: Sequence[str]) -> None:
        self._towns = _ordered_towns(towns)
        self._query_seeds = _unique_queries(query_seeds)

    def build_jobs(self, source: SourceConfig) -> list[CrawlJob]:
        jobs: list[CrawlJob] = []
        for town_index, town in enumerate(self._towns):
            for query_index, intent_query in enumerate(self._query_seeds):
                jobs.append(
                    CrawlJob(
                        job_id=f"{source.slug}:{town.slug}:{slugify(intent_query)}",
                        source_name=source.name,
                        town_name=town.name,
                        town_state=town.state,
                        intent_query=intent_query,
                        query_text=f"{town.name} {intent_query}",
                        priority=(source.priority * 1_000)
                        + (town.priority * 100)
                        + (town_index * 10)
                        + query_index,
                    )
                )
        return jobs


def build_crawl_jobs(
    source: SourceConfig,
    towns: Sequence[TownSeed],
    query_seeds: Sequence[str],
) -> list[CrawlJob]:
    """Convenience wrapper used by the source registry."""
    return JobRunner(towns, query_seeds).build_jobs(source)
