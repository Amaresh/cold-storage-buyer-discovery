"""Backend export contract for scored buyer discovery candidates."""

from __future__ import annotations

from collections.abc import Iterable

from src.common.models import ScoredBuyerCandidate

SCHEMA_VERSION = 1


def export_backend_payload(
    candidate: ScoredBuyerCandidate,
    *,
    tenant_id: str,
    warehouse_id: str,
    crawl_run_ref: str,
) -> dict[str, object]:
    """Serialize a scored discovery candidate for later backend ingestion."""

    normalized = candidate.candidate
    return {
        "schema_version": SCHEMA_VERSION,
        "tenant_id": tenant_id,
        "warehouse_id": warehouse_id,
        "crawl_run_ref": crawl_run_ref,
        "candidate_ref": normalized.candidate_ref,
        "confidence_score": candidate.confidence_score,
        "score_reasons": list(candidate.score_reasons),
        "review_state": candidate.review_state,
        "dedupe_fields": list(normalized.dedupe_fields),
        "business": {
            "name": normalized.business_name,
            "town": normalized.town,
            "website": normalized.website,
            "domain": normalized.domain,
        },
        "contact": {
            "phones": list(normalized.phones),
            "emails": list(normalized.emails),
            "other_hints": list(normalized.other_contact_hints),
        },
        "evidence": [
            {
                "source_key": evidence.source_key,
                "source_url": evidence.source_url,
                "website": evidence.website,
                "contact_hints": list(evidence.contact_hints),
                "source_confidence_class": evidence.source_confidence_class,
                "source_confidence_level": evidence.source_confidence_level,
            }
            for evidence in normalized.evidence
        ],
    }


def export_backend_payloads(
    candidates: Iterable[ScoredBuyerCandidate],
    *,
    tenant_id: str,
    warehouse_id: str,
    crawl_run_ref: str,
) -> list[dict[str, object]]:
    return [
        export_backend_payload(
            candidate,
            tenant_id=tenant_id,
            warehouse_id=warehouse_id,
            crawl_run_ref=crawl_run_ref,
        )
        for candidate in candidates
    ]
