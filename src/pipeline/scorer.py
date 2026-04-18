"""Confidence scoring for normalized buyer discovery candidates."""

from __future__ import annotations

from collections.abc import Iterable

from src.common.models import NormalizedBuyerCandidate, ScoredBuyerCandidate

AUTO_APPROVE_THRESHOLD = 0.80
_BASE_SCORE = 0.10
_SOURCE_CONFIDENCE_BONUS = {
    0: 0.00,
    1: 0.10,
    2: 0.20,
    3: 0.35,
}
_CORROBORATION_BONUS = {
    2: 0.15,
    3: 0.20,
}
_DOMAIN_BONUS = 0.15
_WEBSITE_BONUS = 0.05
_PHONE_BONUS = 0.10
_EMAIL_BONUS = 0.10


def score_candidate(candidate: NormalizedBuyerCandidate) -> ScoredBuyerCandidate:
    """Score a normalized candidate and decide whether it can bypass review."""

    score = _BASE_SCORE
    reasons = ["baseline"]
    max_source_level = max(
        (evidence.source_confidence_level for evidence in candidate.evidence),
        default=0,
    )
    if max_source_level:
        score += _SOURCE_CONFIDENCE_BONUS.get(
            max_source_level,
            _SOURCE_CONFIDENCE_BONUS[max(_SOURCE_CONFIDENCE_BONUS)],
        )
        reasons.append(f"max_source_confidence:{max_source_level}")

    distinct_sources = len(set(candidate.source_keys))
    if distinct_sources >= 2:
        bonus = _CORROBORATION_BONUS[3 if distinct_sources >= 3 else 2]
        score += bonus
        reasons.append(f"source_corroboration:{distinct_sources}")

    if candidate.domain:
        score += _DOMAIN_BONUS
        reasons.append("domain")
    if candidate.website:
        score += _WEBSITE_BONUS
        reasons.append("website")
    if candidate.phones:
        score += _PHONE_BONUS
        reasons.append("phone")
    if candidate.emails:
        score += _EMAIL_BONUS
        reasons.append("email")

    confidence_score = min(1.0, round(score, 2))
    review_state = "auto_approved" if confidence_score >= AUTO_APPROVE_THRESHOLD else "needs_review"
    return ScoredBuyerCandidate(
        candidate=candidate,
        confidence_score=confidence_score,
        review_state=review_state,
        score_reasons=tuple(reasons),
    )


def score_candidates(candidates: Iterable[NormalizedBuyerCandidate]) -> list[ScoredBuyerCandidate]:
    return [score_candidate(candidate) for candidate in candidates]
