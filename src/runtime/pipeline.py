"""Discovery pipeline that runs curated samples through extraction and ingestion mapping."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from config.settings import Settings
from src.common.models import BuyerCandidate, CrawlJob, ScoredBuyerCandidate, TownSeed
from src.crawler.adapters.business_directory import BusinessDirectoryAdapter
from src.crawler.adapters.google_search_seed import GoogleSearchSeedAdapter
from src.crawler.adapters.website_enrichment import WebsiteEnrichmentAdapter
from src.crawler.source_registry import SourceRegistry
from src.exporter.backend_payload import build_ingestion_request
from src.pipeline.normalizer import is_shared_host_domain, normalize_candidates, normalize_domain
from src.pipeline.sanitizer import sanitize_scored_candidates
from src.pipeline.scorer import score_candidates
from src.runtime.live_catalog import LiveSnapshotCatalog
from src.runtime.sample_catalog import HtmlSnapshot, SampleSnapshotCatalog


@dataclass(frozen=True, slots=True)
class DiscoveryRunResult:
    mode: str
    crawl_run_ref: str
    scheduled_jobs: int
    processed_jobs: int
    skipped_jobs: int
    raw_candidate_count: int
    enriched_candidate_count: int
    normalized_candidate_count: int
    sanitized_candidate_count: int
    auto_approved_candidate_count: int
    ingestion_request: dict[str, object]
    scored_candidates: tuple[ScoredBuyerCandidate, ...]
    eligible_candidates: tuple[ScoredBuyerCandidate, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "crawlRunRef": self.crawl_run_ref,
            "scheduledJobs": self.scheduled_jobs,
            "processedJobs": self.processed_jobs,
            "skippedJobs": self.skipped_jobs,
            "rawCandidateCount": self.raw_candidate_count,
            "enrichedCandidateCount": self.enriched_candidate_count,
            "normalizedCandidateCount": self.normalized_candidate_count,
            "sanitizedCandidateCount": self.sanitized_candidate_count,
            "autoApprovedCandidateCount": self.auto_approved_candidate_count,
            "candidates": [
                {
                    "candidateRef": candidate.candidate.candidate_ref,
                    "buyerName": candidate.candidate.business_name,
                    "town": candidate.candidate.town,
                    "confidenceScore": candidate.confidence_score,
                    "reviewState": candidate.review_state,
                    "sources": list(candidate.candidate.source_keys),
                }
                for candidate in self.eligible_candidates
            ],
        }


def _candidate_key(candidate: BuyerCandidate) -> tuple[object, ...]:
    return (
        candidate.source_key.casefold(),
        candidate.business_name.casefold(),
        candidate.source_url.casefold(),
        candidate.town.casefold(),
        candidate.website.casefold(),
        tuple(hint.casefold() for hint in candidate.contact_hints),
    )


def _unique_candidates(candidates: Sequence[BuyerCandidate]) -> list[BuyerCandidate]:
    ordered: list[BuyerCandidate] = []
    seen: set[tuple[object, ...]] = set()
    for candidate in candidates:
        key = _candidate_key(candidate)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(candidate)
    return ordered


class SampleDiscoveryPipeline:
    """Runs either the sample-backed path or the live web-fetch path."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        towns = settings.load_towns()
        town_hints = tuple(town.name for town in towns)
        self._registry = SourceRegistry(settings.sources)
        self._catalog = (
            SampleSnapshotCatalog(settings.sample_snapshots_dir)
            if settings.use_sample_snapshots
                else LiveSnapshotCatalog(
                    google_search_url_template=settings.google_search_url_template,
                    search_fallback_url_template=settings.search_fallback_url_template,
                    search_fallback_url_templates=settings.search_fallback_url_templates,
                    business_directory_url_template=settings.business_directory_url_template,
                    user_agent=settings.http_user_agent,
                    timeout_seconds=settings.fetch_timeout_seconds,
                )
        )
        self._google_adapter = GoogleSearchSeedAdapter(town_hints=town_hints)
        self._directory_adapter = BusinessDirectoryAdapter(town_hints=town_hints)
        self._website_adapter = WebsiteEnrichmentAdapter(town_hints=town_hints)
        self._town_state_by_name = settings.town_state_by_name()

    def schedule_jobs(
        self,
        towns: Sequence[TownSeed],
        query_seeds: Sequence[str],
    ) -> list[CrawlJob]:
        return self._registry.schedule_jobs(towns, query_seeds)

    def run(
        self,
        towns: Sequence[TownSeed],
        query_seeds: Sequence[str],
    ) -> DiscoveryRunResult:
        jobs = self.schedule_jobs(towns, query_seeds)
        raw_candidates: list[BuyerCandidate] = []
        processed_jobs = 0
        skipped_jobs = 0

        for job in jobs:
            snapshot = self._catalog.snapshot_for_job(job)
            if snapshot is None:
                skipped_jobs += 1
                continue
            processed_jobs += 1
            raw_candidates.extend(self._extract_job_candidates(job, snapshot))

        unique_raw_candidates = _unique_candidates(raw_candidates)
        enriched_candidates = _unique_candidates(
            self._extract_website_enrichment(unique_raw_candidates)
        )
        all_candidates = _unique_candidates([*unique_raw_candidates, *enriched_candidates])
        normalized_candidates = normalize_candidates(all_candidates)
        scored_candidates = tuple(score_candidates(normalized_candidates))
        eligible_candidates = tuple(sanitize_scored_candidates(scored_candidates))
        crawl_run_ref = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        ingestion_request = build_ingestion_request(
            eligible_candidates,
            crawl_run_ref=crawl_run_ref,
            state_by_town=self._town_state_by_name,
            discovery_source=self._settings.discovery_source,
        )

        return DiscoveryRunResult(
            mode=self._catalog.mode_name,
            crawl_run_ref=crawl_run_ref,
            scheduled_jobs=len(jobs),
            processed_jobs=processed_jobs,
            skipped_jobs=skipped_jobs,
            raw_candidate_count=len(unique_raw_candidates),
            enriched_candidate_count=len(enriched_candidates),
            normalized_candidate_count=len(normalized_candidates),
            sanitized_candidate_count=len(eligible_candidates),
            auto_approved_candidate_count=sum(
                1 for candidate in eligible_candidates if candidate.auto_approved
            ),
            ingestion_request=ingestion_request,
            scored_candidates=scored_candidates,
            eligible_candidates=eligible_candidates,
        )

    def _extract_job_candidates(
        self,
        job: CrawlJob,
        snapshot: HtmlSnapshot,
    ) -> list[BuyerCandidate]:
        if job.source_name == "google_search_seed":
            candidates = self._google_adapter.extract_candidates(
                snapshot.html,
                search_page_url=snapshot.source_url,
                fallback_town=job.town_name,
            )
        elif job.source_name == "business_directory":
            candidates = self._directory_adapter.extract_candidates(
                snapshot.html,
                directory_page_url=snapshot.source_url,
            )
        else:
            return []
        return self._filter_candidates_for_town(candidates, expected_town=job.town_name)

    def _extract_website_enrichment(
        self,
        candidates: Sequence[BuyerCandidate],
    ) -> list[BuyerCandidate]:
        enriched_candidates: list[BuyerCandidate] = []
        for candidate in candidates:
            if not candidate.website:
                continue
            website_domain = normalize_domain(candidate.website)
            if not website_domain or is_shared_host_domain(website_domain):
                continue
            snapshot = self._catalog.website_snapshot(candidate.website)
            if snapshot is None:
                continue
            enriched_candidates.extend(
                self._filter_candidates_for_town(
                    self._website_adapter.extract_candidates(
                        snapshot.html,
                        website_url=snapshot.source_url,
                    ),
                    expected_town=candidate.town,
                )
            )
        return enriched_candidates

    def _filter_candidates_for_town(
        self,
        candidates: Sequence[BuyerCandidate],
        *,
        expected_town: str,
    ) -> list[BuyerCandidate]:
        expected_key = expected_town.casefold()
        return [
            candidate
            for candidate in candidates
            if candidate.town.casefold() == expected_key
        ]
