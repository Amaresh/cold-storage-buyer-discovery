"""Approved source policies for worker market-intelligence inputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit

from src.pipeline.normalizer import normalize_domain

MarketIntelSourceKind = Literal["official_board", "chatter"]


@dataclass(frozen=True, slots=True)
class MarketIntelSourcePolicy:
    source_key: str
    source_kind: MarketIntelSourceKind
    allows_direction_signal: bool
    approved_domains: tuple[str, ...] = ()
    allow_fixture_scheme: bool = True


SOURCE_POLICIES: dict[str, MarketIntelSourcePolicy] = {
    "agmarknet_official_board": MarketIntelSourcePolicy(
        source_key="agmarknet_official_board",
        source_kind="official_board",
        allows_direction_signal=True,
        approved_domains=("agmarknet.gov.in",),
    ),
    "telangana_market_board": MarketIntelSourcePolicy(
        source_key="telangana_market_board",
        source_kind="official_board",
        allows_direction_signal=True,
        approved_domains=("emarkets.telangana.gov.in",),
    ),
    "trade_press_digest": MarketIntelSourcePolicy(
        source_key="trade_press_digest",
        source_kind="chatter",
        allows_direction_signal=False,
    ),
    "market_bulletin_commentary": MarketIntelSourcePolicy(
        source_key="market_bulletin_commentary",
        source_kind="chatter",
        allows_direction_signal=False,
    ),
}


def get_market_intel_source_policy(source_key: str) -> MarketIntelSourcePolicy:
    return SOURCE_POLICIES[source_key]


def validate_market_intel_source(
    source_key: str,
    source_url: str,
    *,
    expected_kind: MarketIntelSourceKind,
) -> MarketIntelSourcePolicy:
    policy = get_market_intel_source_policy(source_key)
    if policy.source_kind != expected_kind:
        raise ValueError(
            f"Source {source_key!r} is {policy.source_kind}, expected {expected_kind}."
        )
    if expected_kind == "official_board" and not policy.allows_direction_signal:
        raise ValueError(f"Source {source_key!r} may not drive automatic market direction.")

    scheme = urlsplit(source_url).scheme.casefold()
    if scheme == "fixture" and policy.allow_fixture_scheme:
        return policy
    if scheme not in {"http", "https"}:
        raise ValueError(f"Unsupported source URL scheme for {source_key!r}: {source_url!r}")

    domain = normalize_domain(source_url)
    if policy.approved_domains and not any(
        domain == approved or domain.endswith(f".{approved}")
        for approved in policy.approved_domains
    ):
        raise ValueError(
            f"Source {source_key!r} must use an approved domain, got {source_url!r}."
        )
    return policy
