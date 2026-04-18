"""Map scored worker candidates into the live backend ingestion contract."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from src.common.models import ScoredBuyerCandidate

_EVIDENCE_TYPE_BY_CLASS = {
    "discovery_seed": "SEARCH_SEED",
    "third_party_listing": "DIRECTORY_LISTING",
    "first_party_website": "WEBSITE",
    "unknown": "OTHER",
}


def _truncate(value: str, limit: int) -> str:
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def _candidate_notes(candidate: ScoredBuyerCandidate, crawl_run_ref: str) -> str:
    normalized = candidate.candidate
    parts = [
        f"crawl_run_ref={crawl_run_ref}",
        f"candidate_ref={normalized.candidate_ref}",
        f"confidence_score={candidate.confidence_score:.2f}",
        f"review_state={candidate.review_state}",
        f"sources={','.join(normalized.source_keys)}",
    ]
    return _truncate("; ".join(parts), 1000)


def _evidence_details(
    *,
    website: str,
    contact_hints: tuple[str, ...],
    source_confidence_class: str,
) -> str:
    parts = [f"confidence_class={source_confidence_class}"]
    if website:
        parts.append(f"website={website}")
    if contact_hints:
        parts.append(f"contact_hints={','.join(contact_hints)}")
    return _truncate("; ".join(parts), 1000)


def export_backend_payload(
    candidate: ScoredBuyerCandidate,
    *,
    crawl_run_ref: str,
    state_by_town: Mapping[str, str] | None = None,
    discovery_source: str = "buyer-discovery-worker",
) -> dict[str, object]:
    """Serialize one scored discovery candidate for backend ingestion."""

    normalized = candidate.candidate
    state_province = ""
    if state_by_town is not None:
        state_province = state_by_town.get(normalized.town.casefold(), "")

    return {
        "buyerName": normalized.business_name,
        "businessName": normalized.business_name,
        "primaryPhone": normalized.phones[0] if normalized.phones else "",
        "primaryEmail": normalized.emails[0] if normalized.emails else "",
        "city": normalized.town,
        "stateProvince": state_province,
        "discoverySource": discovery_source,
        "discoveryNotes": _candidate_notes(candidate, crawl_run_ref),
        "evidence": [
            {
                "evidenceType": _EVIDENCE_TYPE_BY_CLASS.get(
                    evidence.source_confidence_class,
                    "OTHER",
                ),
                "sourceLabel": evidence.source_key,
                "details": _evidence_details(
                    website=evidence.website,
                    contact_hints=evidence.contact_hints,
                    source_confidence_class=evidence.source_confidence_class,
                ),
                "evidenceUrl": evidence.source_url,
                "capturedAt": crawl_run_ref,
            }
            for evidence in normalized.evidence
        ],
    }


def export_backend_payloads(
    candidates: Iterable[ScoredBuyerCandidate],
    *,
    crawl_run_ref: str,
    state_by_town: Mapping[str, str] | None = None,
    discovery_source: str = "buyer-discovery-worker",
) -> list[dict[str, object]]:
    return [
        export_backend_payload(
            candidate,
            crawl_run_ref=crawl_run_ref,
            state_by_town=state_by_town,
            discovery_source=discovery_source,
        )
        for candidate in candidates
    ]


def build_ingestion_request(
    candidates: Iterable[ScoredBuyerCandidate],
    *,
    crawl_run_ref: str,
    state_by_town: Mapping[str, str] | None = None,
    discovery_source: str = "buyer-discovery-worker",
) -> dict[str, object]:
    return {
        "candidates": export_backend_payloads(
            candidates,
            crawl_run_ref=crawl_run_ref,
            state_by_town=state_by_town,
            discovery_source=discovery_source,
        )
    }
