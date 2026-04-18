"""Source registry for the buyer-discovery worker scaffold."""

from __future__ import annotations

from collections.abc import Sequence
import logging

from src.common.models import CrawlJob, SourceConfig, TownSeed
from src.crawler.job_runner import build_crawl_jobs
from src.crawler.source_policy import resolve_source_policy

logger = logging.getLogger(__name__)


class SourceRegistry:
    """Keeps track of configured sources and schedules enabled ones only."""

    def __init__(self, sources: Sequence[SourceConfig]) -> None:
        self._sources = tuple(
            sorted(sources, key=lambda source: (source.priority, source.name.casefold()))
        )

    @property
    def sources(self) -> tuple[SourceConfig, ...]:
        return self._sources

    @property
    def enabled_sources(self) -> tuple[SourceConfig, ...]:
        return tuple(
            source
            for source in self._sources
            if self._is_enabled(source)
        )

    def schedule_jobs(
        self,
        towns: Sequence[TownSeed],
        query_seeds: Sequence[str],
    ) -> list[CrawlJob]:
        jobs: list[CrawlJob] = []
        for source in self.enabled_sources:
            jobs.extend(build_crawl_jobs(source, towns, query_seeds))
        return jobs

    def _is_enabled(self, source: SourceConfig) -> bool:
        if not source.enabled:
            return False
        try:
            return resolve_source_policy(source).is_enabled
        except KeyError:
            # Keep experimental sources schedulable, but always emit a warning because they bypass
            # policy-specific throttling and persistence rules until a real policy is registered.
            logger.warning("No source policy registered for %s; scheduling with config-only enablement.", source.name)
            return True
