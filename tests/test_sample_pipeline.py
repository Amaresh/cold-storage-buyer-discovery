from __future__ import annotations

from src.common.models import BuyerCandidate
from config.settings import Settings
from src.runtime.pipeline import SampleDiscoveryPipeline


def test_sample_pipeline_builds_selected_guntur_candidates_and_ingestion_request() -> None:
    settings = Settings()
    pipeline = SampleDiscoveryPipeline(settings)
    towns = settings.resolve_towns(["Guntur"])

    result = pipeline.run(towns, settings.query_seeds)

    assert result.scheduled_jobs == 4
    assert result.processed_jobs == 4
    assert result.skipped_jobs == 0
    assert result.raw_candidate_count == 2
    assert result.enriched_candidate_count == 2
    assert result.normalized_candidate_count == 2
    assert result.sanitized_candidate_count == 2
    assert result.auto_approved_candidate_count == 2
    assert [candidate.candidate.business_name for candidate in result.scored_candidates] == [
        "Sri Balaji Commission Agent",
        "Sri Lakshmi Gunj Traders",
    ]
    assert [candidate.candidate.business_name for candidate in result.eligible_candidates] == [
        "Sri Balaji Commission Agent",
        "Sri Lakshmi Gunj Traders",
    ]
    assert all(candidate.auto_approved for candidate in result.scored_candidates)
    assert result.ingestion_request["candidates"] and len(result.ingestion_request["candidates"]) == 2
    assert [payload["buyerName"] for payload in result.ingestion_request["candidates"]] == [
        "Sri Balaji Commission Agent",
        "Sri Lakshmi Gunj Traders",
    ]


def test_sample_pipeline_skips_shared_host_website_enrichment() -> None:
    settings = Settings()
    pipeline = SampleDiscoveryPipeline(settings)

    class NoFetchCatalog:
        def website_snapshot(self, website_url: str):
            raise AssertionError(f"Shared host should not be fetched for enrichment: {website_url}")

    pipeline._catalog = NoFetchCatalog()  # type: ignore[attr-defined]

    enriched = pipeline._extract_website_enrichment(
        [
            BuyerCandidate(
                business_name="AGA Commission Agents",
                source_url="https://search.example/aga",
                town="Guntur",
                website="https://dial4trade.com/aga-commission-agents",
                contact_hints=(),
                source_key="google_search_seed",
            )
        ]
    )

    assert enriched == []
