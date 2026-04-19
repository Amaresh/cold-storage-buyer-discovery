"""Serialize market-intelligence signals into backend-friendly payloads."""

from __future__ import annotations

from collections.abc import Iterable

from src.common.models import slugify
from src.market_intel.models import (
    NormalizedMarketChatterItem,
    NormalizedOfficialMarketPriceSnapshot,
    VarietyMarketSignal,
)


def _truncate(value: str, limit: int) -> str:
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def _signal_ref(
    signal: VarietyMarketSignal,
    *,
    intel_source: str,
    warehouse_id: str,
    scenario: str,
) -> str:
    return "|".join(
        (
            intel_source.casefold(),
            slugify(warehouse_id),
            signal.variety_key,
            slugify(scenario),
        )
    )


def export_market_signal_payload(
    signal: VarietyMarketSignal,
    *,
    captured_at: str,
    scenario: str,
    warehouse_id: str,
    intel_source: str,
) -> dict[str, object]:
    return {
        "signalRef": _signal_ref(
            signal,
            intel_source=intel_source,
            warehouse_id=warehouse_id,
            scenario=scenario,
        ),
        "signalSource": intel_source,
        "warehouseId": warehouse_id,
        "scenario": scenario,
        "capturedAt": captured_at,
        "direction": signal.direction,
        "directionBasis": "OFFICIAL_BOARD_ONLY",
        "variety": signal.variety_name,
        "varietyKey": signal.variety_key,
        "availableBags": signal.available_bags,
        "carryPricePerQuintal": signal.carry_price_per_quintal,
        "officialMarketCount": signal.official_market_count,
        "officialMinPricePerQuintal": signal.official_min_price_per_quintal,
        "officialModalPricePerQuintal": signal.official_modal_price_per_quintal,
        "officialMaxPricePerQuintal": signal.official_max_price_per_quintal,
        "priceDeltaPerQuintal": signal.price_delta_per_quintal,
        "priceDeltaPercent": signal.price_delta_percent,
        "comparedMarkets": list(signal.compared_markets),
        "trendNote": _truncate(signal.trend_note, 400),
        "chilliChatter": _truncate(signal.chilli_chatter, 400),
        "officialPriceInputs": [
            {
                "sourceLabel": price_input.source_key,
                "sourceUrl": price_input.source_url,
                "capturedAt": price_input.captured_at,
                "market": price_input.market_name,
                "marketKey": price_input.market_key,
                "variety": price_input.variety_name,
                "varietyKey": price_input.variety_key,
                "minPricePerQuintal": price_input.min_price_per_quintal,
                "modalPricePerQuintal": price_input.modal_price_per_quintal,
                "maxPricePerQuintal": price_input.max_price_per_quintal,
            }
            for price_input in signal.official_price_inputs
        ],
        "chatterInputs": [
            {
                "sourceLabel": chatter_input.source_key,
                "sourceUrl": chatter_input.source_url,
                "publishedAt": chatter_input.published_at,
                "headline": chatter_input.headline,
                "summary": chatter_input.summary,
                "marketKeys": list(chatter_input.market_keys),
                "varietyKeys": list(chatter_input.variety_keys),
            }
            for chatter_input in signal.chatter_inputs
        ],
    }


def _source_label(source_key: str) -> str:
    return " ".join(part.capitalize() for part in source_key.split("_") if part)


def _price_source_type(source_key: str) -> str:
    normalized = source_key.casefold()
    if "agmarknet" in normalized or "mandi" in normalized:
        return "OFFICIAL_MANDI_BOARD"
    if "feed" in normalized:
        return "OFFICIAL_PRICE_FEED"
    return "OFFICIAL_MARKET_BOARD"


def _chatter_type(source_key: str) -> str:
    normalized = source_key.casefold()
    if "bulletin" in normalized or "market_note" in normalized:
        return "MARKET_NOTE"
    if "trade" in normalized:
        return "TRADE_COVERAGE"
    return "NEWS_COVERAGE"


def _official_price_per_kg(price_per_quintal: int) -> float:
    return round(price_per_quintal / 100, 2)


def _flatten_price_inputs(
    signals: Iterable[VarietyMarketSignal],
) -> tuple[NormalizedOfficialMarketPriceSnapshot, ...]:
    ordered: dict[
        tuple[str, str, str, str, str],
        NormalizedOfficialMarketPriceSnapshot,
    ] = {}
    for signal in signals:
        for price_input in signal.official_price_inputs:
            key = (
                price_input.variety_key,
                price_input.market_key,
                price_input.source_key,
                price_input.source_url,
                price_input.captured_at,
            )
            ordered.setdefault(key, price_input)
    return tuple(ordered.values())


def _flatten_chatter_inputs(
    signals: Iterable[VarietyMarketSignal],
) -> tuple[NormalizedMarketChatterItem, ...]:
    ordered: dict[
        tuple[str, str, str, str, tuple[str, ...], tuple[str, ...]],
        NormalizedMarketChatterItem,
    ] = {}
    for signal in signals:
        for chatter_input in signal.chatter_inputs:
            key = (
                chatter_input.source_key,
                chatter_input.source_url,
                chatter_input.published_at,
                chatter_input.headline,
                tuple(chatter_input.market_keys),
                tuple(chatter_input.variety_keys),
            )
            ordered.setdefault(key, chatter_input)
    return tuple(ordered.values())


def _single_or_none(values: tuple[str, ...]) -> str | None:
    return values[0] if len(values) == 1 else None


def build_market_price_snapshot_ingestion_request(
    signals: Iterable[VarietyMarketSignal],
) -> dict[str, object]:
    return {
        "snapshots": [
            {
                "chilliVariety": snapshot.variety_name,
                "marketName": snapshot.market_name,
                "sourceLabel": _source_label(snapshot.source_key),
                "sourceUrl": snapshot.source_url,
                "sourceType": _price_source_type(snapshot.source_key),
                "officialPricePerKg": _official_price_per_kg(snapshot.modal_price_per_quintal),
                "capturedAt": snapshot.captured_at,
            }
            for snapshot in _flatten_price_inputs(signals)
        ]
    }


def build_market_chatter_ingestion_request(
    signals: Iterable[VarietyMarketSignal],
) -> dict[str, object]:
    return {
        "items": [
            {
                "chilliVariety": _single_or_none(item.variety_names),
                "headline": item.headline,
                "summary": _truncate(item.summary, 1000),
                "sourceLabel": _source_label(item.source_key),
                "sourceUrl": item.source_url,
                "chatterType": _chatter_type(item.source_key),
                "publishedAt": item.published_at,
                "capturedAt": item.published_at,
            }
            for item in _flatten_chatter_inputs(signals)
        ]
    }


def build_market_carry_benchmark_ingestion_request(
    signals: Iterable[VarietyMarketSignal],
    *,
    captured_at: str,
) -> dict[str, object]:
    return {
        "benchmarks": [
            {
                "chilliVariety": signal.variety_name,
                "carryPricePerKg": _official_price_per_kg(signal.carry_price_per_quintal),
                "sourceType": "WORKER_PROFILE",
                "weightSource": "PROFILE_FALLBACK",
                "bagCount": signal.available_bags,
                "weightKg": None,
                "capturedAt": captured_at,
            }
            for signal in signals
        ]
    }


def build_market_intelligence_request(
    signals: Iterable[VarietyMarketSignal],
    *,
    captured_at: str,
    scenario: str,
    warehouse_id: str,
    intel_source: str,
) -> dict[str, object]:
    return {
        "capturedAt": captured_at,
        "scenario": scenario,
        "signalSource": intel_source,
        "warehouseId": warehouse_id,
        "signals": [
            export_market_signal_payload(
                signal,
                captured_at=captured_at,
                scenario=scenario,
                warehouse_id=warehouse_id,
                intel_source=intel_source,
            )
            for signal in signals
        ],
    }
