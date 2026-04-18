"""Source policy definitions for buyer-discovery adapters."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

from src.common.models import BuyerCandidate, SourceConfig

PersistableField = Literal[
    "business_name",
    "source_url",
    "town",
    "website",
    "contact_hints",
    "source_key",
]


@dataclass(frozen=True, slots=True)
class ThrottlePolicy:
    """Static throttling parameters for a source."""

    min_interval_seconds: float
    max_requests_per_hour: int


@dataclass(frozen=True, slots=True)
class SourcePolicy:
    """Rules that govern how a source may be fetched and persisted."""

    source_key: str
    fetch_method: str
    throttling: ThrottlePolicy
    allowed_fields_to_persist: tuple[PersistableField, ...]
    confidence_class: str
    confidence_level: int
    persistence_scope: str = "candidate"
    disabled_reason: str | None = None

    @property
    def is_enabled(self) -> bool:
        return self.disabled_reason is None


SOURCE_POLICIES: dict[str, SourcePolicy] = {
    "google_search_seed": SourcePolicy(
        source_key="google_search_seed",
        fetch_method="search_result_snapshot",
        throttling=ThrottlePolicy(min_interval_seconds=20.0, max_requests_per_hour=120),
        allowed_fields_to_persist=(
            "business_name",
            "source_url",
            "town",
            "website",
            "contact_hints",
        ),
        confidence_class="discovery_seed",
        confidence_level=1,
        persistence_scope="seed_only",
    ),
    "business_directory": SourcePolicy(
        source_key="business_directory",
        fetch_method="directory_listing_snapshot",
        throttling=ThrottlePolicy(min_interval_seconds=10.0, max_requests_per_hour=240),
        allowed_fields_to_persist=(
            "business_name",
            "source_url",
            "town",
            "website",
            "contact_hints",
            "source_key",
        ),
        confidence_class="third_party_listing",
        confidence_level=2,
    ),
    "website_enrichment": SourcePolicy(
        source_key="website_enrichment",
        fetch_method="first_party_website",
        throttling=ThrottlePolicy(min_interval_seconds=5.0, max_requests_per_hour=360),
        allowed_fields_to_persist=(
            "business_name",
            "source_url",
            "town",
            "website",
            "contact_hints",
            "source_key",
        ),
        confidence_class="first_party_website",
        confidence_level=3,
    ),
}


def get_source_policy(source_key: str) -> SourcePolicy:
    return SOURCE_POLICIES[source_key]


def resolve_source_policy(source: SourceConfig | str) -> SourcePolicy:
    if isinstance(source, SourceConfig):
        source_key = source.adapter_key or source.name
        policy = get_source_policy(source_key)
        if source.enabled:
            return policy
        if policy.disabled_reason:
            return policy
        return replace(policy, disabled_reason="Disabled in source configuration")
    return get_source_policy(source)


def persistable_candidate_fields(
    candidate: BuyerCandidate,
    policy: SourcePolicy,
) -> dict[str, object]:
    payload = {
        "business_name": candidate.business_name,
        "source_url": candidate.source_url,
        "town": candidate.town,
        "website": candidate.website,
        "contact_hints": candidate.contact_hints,
        "source_key": candidate.source_key,
    }
    return {
        field_name: value
        for field_name, value in payload.items()
        if field_name in policy.allowed_fields_to_persist and value not in ("", (), None)
    }
