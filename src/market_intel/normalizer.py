"""Normalization helpers for worker-produced market intelligence."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import re

from src.common.models import slugify
from src.common.sanitization import collapse_whitespace
from src.market_intel.models import (
    AliasRule,
    MarketChatterItem,
    NormalizedMarketChatterItem,
    NormalizedOfficialMarketPriceSnapshot,
    NormalizedWarehouseCarryProfile,
    OfficialMarketPriceSnapshot,
    WarehouseCarryProfile,
)
from src.market_intel.policy import validate_market_intel_source

_ALIAS_TOKEN_RE = re.compile(r"[^a-z0-9]+")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)


def _alias_key(value: str) -> str:
    normalized = collapse_whitespace(value).casefold().replace("&", " and ")
    return collapse_whitespace(_ALIAS_TOKEN_RE.sub(" ", normalized))


def build_alias_lookup(rules: Sequence[AliasRule]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for rule in rules:
        canonical = collapse_whitespace(rule.canonical_name)
        for value in (rule.canonical_name, *rule.aliases):
            key = _alias_key(value)
            if key:
                lookup[key] = canonical
    return lookup


def _default_display(value: str) -> str:
    cleaned = collapse_whitespace(value)
    if not cleaned:
        return ""
    return cleaned.title() if cleaned == cleaned.casefold() else cleaned


def _normalize_label(value: str, alias_lookup: Mapping[str, str]) -> tuple[str, str]:
    key = _alias_key(value)
    if not key:
        return ("", "")
    canonical = alias_lookup.get(key)
    if canonical is None:
        display = _default_display(value)
        canonical_key = _alias_key(display)
        return (display, slugify(canonical_key))
    return (canonical, slugify(_alias_key(canonical)))


def normalize_market_name(
    value: str,
    alias_lookup: Mapping[str, str],
) -> tuple[str, str]:
    return _normalize_label(value, alias_lookup)


def normalize_variety_name(
    value: str,
    alias_lookup: Mapping[str, str],
) -> tuple[str, str]:
    return _normalize_label(value, alias_lookup)


def normalize_price_per_quintal(value: int | float | str) -> int:
    cleaned = re.sub(r"[^\d.]+", "", str(value))
    if not cleaned:
        raise ValueError(f"Unable to parse price value: {value!r}")
    amount = int(round(float(cleaned)))
    if amount <= 0:
        raise ValueError(f"Price per quintal must be positive, got {value!r}")
    return amount


def normalize_carry_profiles(
    profiles: Sequence[WarehouseCarryProfile],
    *,
    variety_alias_lookup: Mapping[str, str],
) -> tuple[NormalizedWarehouseCarryProfile, ...]:
    normalized_profiles: list[NormalizedWarehouseCarryProfile] = []
    seen_varieties: set[str] = set()
    for profile in profiles:
        variety_name, variety_key = normalize_variety_name(
            profile.variety_name,
            variety_alias_lookup,
        )
        if not variety_name or not variety_key:
            raise ValueError(
                f"Carry profile variety could not be normalized: {profile.variety_name!r}"
            )
        if variety_key in seen_varieties:
            raise ValueError(f"Duplicate carry profile for variety {variety_name!r}")
        seen_varieties.add(variety_key)
        normalized_profiles.append(
            NormalizedWarehouseCarryProfile(
                variety_name=variety_name,
                variety_key=variety_key,
                carry_price_per_quintal=normalize_price_per_quintal(
                    profile.carry_price_per_quintal
                ),
                available_bags=max(int(profile.available_bags), 0),
            )
        )
    return tuple(sorted(normalized_profiles, key=lambda profile: profile.variety_key))


def normalize_official_price_snapshots(
    snapshots: Sequence[OfficialMarketPriceSnapshot],
    *,
    market_alias_lookup: Mapping[str, str],
    variety_alias_lookup: Mapping[str, str],
) -> tuple[NormalizedOfficialMarketPriceSnapshot, ...]:
    normalized_snapshots: list[NormalizedOfficialMarketPriceSnapshot] = []
    for snapshot in snapshots:
        validate_market_intel_source(
            snapshot.source_key,
            snapshot.source_url,
            expected_kind="official_board",
        )
        market_name, market_key = normalize_market_name(
            snapshot.market_name,
            market_alias_lookup,
        )
        variety_name, variety_key = normalize_variety_name(
            snapshot.variety_name,
            variety_alias_lookup,
        )
        minimum = normalize_price_per_quintal(snapshot.min_price_per_quintal)
        modal = normalize_price_per_quintal(snapshot.modal_price_per_quintal)
        maximum = normalize_price_per_quintal(snapshot.max_price_per_quintal)
        if not market_name or not market_key or not variety_name or not variety_key:
            raise ValueError(
                "Official board snapshot is missing a normalized market or variety."
            )
        if not (minimum <= modal <= maximum):
            raise ValueError(
                f"Official board snapshot prices must satisfy min <= modal <= max: {snapshot.to_dict()}"
            )
        normalized_snapshots.append(
            NormalizedOfficialMarketPriceSnapshot(
                source_key=snapshot.source_key,
                source_url=snapshot.source_url,
                captured_at=collapse_whitespace(snapshot.captured_at),
                market_name=market_name,
                market_key=market_key,
                variety_name=variety_name,
                variety_key=variety_key,
                min_price_per_quintal=minimum,
                modal_price_per_quintal=modal,
                max_price_per_quintal=maximum,
            )
        )
    return tuple(
        sorted(
            normalized_snapshots,
            key=lambda snapshot: (
                snapshot.captured_at,
                snapshot.variety_key,
                snapshot.market_key,
                snapshot.source_key,
                snapshot.source_url,
            ),
        )
    )


def sanitize_chatter_text(value: str, *, limit: int) -> str:
    no_tags = _HTML_TAG_RE.sub(" ", value)
    no_urls = _URL_RE.sub(" ", no_tags)
    normalized = collapse_whitespace(no_urls)
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def _normalize_names(
    values: Iterable[str],
    alias_lookup: Mapping[str, str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    names: list[str] = []
    keys: list[str] = []
    seen: set[str] = set()
    for value in values:
        name, key = _normalize_label(value, alias_lookup)
        if not name or not key or key in seen:
            continue
        seen.add(key)
        names.append(name)
        keys.append(key)
    return (tuple(names), tuple(keys))


def normalize_market_chatter_items(
    items: Sequence[MarketChatterItem],
    *,
    market_alias_lookup: Mapping[str, str],
    variety_alias_lookup: Mapping[str, str],
) -> tuple[NormalizedMarketChatterItem, ...]:
    normalized_items: list[NormalizedMarketChatterItem] = []
    for item in items:
        validate_market_intel_source(
            item.source_key,
            item.source_url,
            expected_kind="chatter",
        )
        headline = sanitize_chatter_text(item.headline, limit=160)
        summary = sanitize_chatter_text(item.snippet, limit=260)
        if not headline:
            raise ValueError(
                f"Chatter headline is blank after sanitization: {item.to_dict()}"
            )
        market_names, market_keys = _normalize_names(
            item.market_names,
            market_alias_lookup,
        )
        variety_names, variety_keys = _normalize_names(
            item.variety_names,
            variety_alias_lookup,
        )
        normalized_items.append(
            NormalizedMarketChatterItem(
                source_key=item.source_key,
                source_url=item.source_url,
                published_at=collapse_whitespace(item.published_at),
                headline=headline,
                summary=summary,
                market_names=market_names,
                market_keys=market_keys,
                variety_names=variety_names,
                variety_keys=variety_keys,
            )
        )
    return tuple(
        sorted(
            normalized_items,
            key=lambda item: (
                item.published_at,
                item.source_key,
                item.headline.casefold(),
                item.source_url,
            ),
            reverse=True,
        )
    )
