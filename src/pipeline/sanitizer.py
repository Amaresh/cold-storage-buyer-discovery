"""Strict sanitization gates for scored buyer discovery candidates."""

from __future__ import annotations

from collections.abc import Iterable

from src.common.sanitization import business_name_quality_issues
from src.common.models import ScoredBuyerCandidate


def candidate_sanitization_issues(candidate: ScoredBuyerCandidate) -> tuple[str, ...]:
    normalized = candidate.candidate
    issues = list(business_name_quality_issues(normalized.business_name))
    max_source_level = max(
        (evidence.source_confidence_level for evidence in normalized.evidence),
        default=0,
    )
    has_direct_contact = bool(normalized.phones or normalized.emails)
    distinct_sources = len(set(normalized.source_keys))
    if (
        "not_business_entity" in issues
        and "non_entity_prefix" not in issues
        and normalized.domain
        and (has_direct_contact or distinct_sources >= 2)
        and max_source_level >= 3
    ):
        issues = [issue for issue in issues if issue != "not_business_entity"]
    if max_source_level < 2 and not (has_direct_contact or normalized.domain):
        issues.append("seed_only")
    if not (normalized.domain or normalized.phones or normalized.emails):
        issues.append("no_contact_or_domain")
    return tuple(dict.fromkeys(issues))


def sanitize_scored_candidates(
    candidates: Iterable[ScoredBuyerCandidate],
) -> list[ScoredBuyerCandidate]:
    return [
        candidate
        for candidate in candidates
        if not candidate_sanitization_issues(candidate)
    ]
