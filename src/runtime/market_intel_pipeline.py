"""Deterministic market-intelligence pipeline for warehouse chilli varieties."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from statistics import median

from config.settings import Settings
from src.exporter.market_intel_payload import build_market_intelligence_request
from src.market_intel.models import (
    NormalizedMarketChatterItem,
    NormalizedOfficialMarketPriceSnapshot,
    NormalizedWarehouseCarryProfile,
    VarietyMarketSignal,
)
from src.market_intel.normalizer import (
    build_alias_lookup,
    normalize_carry_profiles,
    normalize_market_chatter_items,
    normalize_official_price_snapshots,
)
from src.runtime.market_intel_catalog import MarketIntelScenarioCatalog

_PRICE_WATCH_BAND_PERCENT = 5.0
_MIN_OFFICIAL_MARKETS_FOR_DIRECTION = 2


@dataclass(frozen=True, slots=True)
class MarketIntelRunResult:
    mode: str
    scenario: str
    captured_at: str
    official_snapshot_count: int
    chatter_item_count: int
    signal_count: int
    market_intelligence_request: dict[str, object]
    signals: tuple[VarietyMarketSignal, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "scenario": self.scenario,
            "capturedAt": self.captured_at,
            "officialSnapshotCount": self.official_snapshot_count,
            "chatterItemCount": self.chatter_item_count,
            "signalCount": self.signal_count,
            "signals": [
                {
                    "variety": signal.variety_name,
                    "varietyKey": signal.variety_key,
                    "direction": signal.direction,
                    "carryPricePerQuintal": signal.carry_price_per_quintal,
                    "officialModalPricePerQuintal": signal.official_modal_price_per_quintal,
                    "availableBags": signal.available_bags,
                    "comparedMarkets": list(signal.compared_markets),
                }
                for signal in self.signals
            ],
        }


class MarketIntelPipeline:
    """Builds conservative warehouse market signals from approved board fixtures."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._catalog = MarketIntelScenarioCatalog(settings.market_intel_path)
        self._market_alias_lookup = build_alias_lookup(self._catalog.market_rules)
        self._variety_alias_lookup = build_alias_lookup(self._catalog.variety_rules)
        self._carry_profiles = normalize_carry_profiles(
            self._catalog.warehouse_carry_profiles,
            variety_alias_lookup=self._variety_alias_lookup,
        )

    def run(self, *, scenario: str | None = None) -> MarketIntelRunResult:
        selected_scenario = (scenario or self._settings.market_intel_scenario).strip()
        market_scenario = self._catalog.scenario(selected_scenario)
        official_snapshots = normalize_official_price_snapshots(
            market_scenario.official_price_snapshots,
            market_alias_lookup=self._market_alias_lookup,
            variety_alias_lookup=self._variety_alias_lookup,
        )
        chatter_items = normalize_market_chatter_items(
            market_scenario.chatter_items,
            market_alias_lookup=self._market_alias_lookup,
            variety_alias_lookup=self._variety_alias_lookup,
        )
        captured_at = max(
            [snapshot.captured_at for snapshot in official_snapshots]
            + [item.published_at for item in chatter_items],
            default="",
        )
        signals = tuple(
            self._build_signal(profile, official_snapshots, chatter_items)
            for profile in self._carry_profiles
        )
        request = build_market_intelligence_request(
            signals,
            captured_at=captured_at,
            scenario=selected_scenario,
            warehouse_id=self._settings.warehouse_id,
            intel_source=self._settings.market_intel_source,
        )
        return MarketIntelRunResult(
            mode=self._catalog.mode_name,
            scenario=selected_scenario,
            captured_at=captured_at,
            official_snapshot_count=len(official_snapshots),
            chatter_item_count=len(chatter_items),
            signal_count=len(signals),
            market_intelligence_request=request,
            signals=signals,
        )

    def _build_signal(
        self,
        profile: NormalizedWarehouseCarryProfile,
        official_snapshots: Sequence[NormalizedOfficialMarketPriceSnapshot],
        chatter_items: Sequence[NormalizedMarketChatterItem],
    ) -> VarietyMarketSignal:
        price_inputs = tuple(
            snapshot
            for snapshot in official_snapshots
            if snapshot.variety_key == profile.variety_key
        )
        compared_markets = self._ordered_unique(
            snapshot.market_name for snapshot in price_inputs
        )
        official_market_count = len({snapshot.market_key for snapshot in price_inputs})

        official_min = (
            min(snapshot.min_price_per_quintal for snapshot in price_inputs)
            if price_inputs
            else None
        )
        official_modal = (
            int(round(median(snapshot.modal_price_per_quintal for snapshot in price_inputs)))
            if price_inputs
            else None
        )
        official_max = (
            max(snapshot.max_price_per_quintal for snapshot in price_inputs)
            if price_inputs
            else None
        )
        price_delta = (
            official_modal - profile.carry_price_per_quintal
            if official_modal is not None
            else None
        )
        price_delta_percent = (
            round((price_delta / profile.carry_price_per_quintal) * 100, 2)
            if price_delta is not None and profile.carry_price_per_quintal
            else None
        )
        direction = self._direction_for(
            official_market_count=official_market_count,
            price_delta_percent=price_delta_percent,
        )
        matched_chatter = self._matching_chatter_items(
            profile,
            compared_markets=compared_markets,
            chatter_items=chatter_items,
        )
        return VarietyMarketSignal(
            variety_name=profile.variety_name,
            variety_key=profile.variety_key,
            available_bags=profile.available_bags,
            carry_price_per_quintal=profile.carry_price_per_quintal,
            official_market_count=official_market_count,
            official_min_price_per_quintal=official_min,
            official_modal_price_per_quintal=official_modal,
            official_max_price_per_quintal=official_max,
            direction=direction,
            price_delta_per_quintal=price_delta,
            price_delta_percent=price_delta_percent,
            compared_markets=compared_markets,
            trend_note=self._trend_note(
                variety_name=profile.variety_name,
                carry_price_per_quintal=profile.carry_price_per_quintal,
                official_modal_price_per_quintal=official_modal,
                official_market_count=official_market_count,
                price_delta_percent=price_delta_percent,
                direction=direction,
            ),
            chilli_chatter=self._build_chilli_chatter(
                variety_name=profile.variety_name,
                direction=direction,
                chatter_items=matched_chatter,
            ),
            official_price_inputs=price_inputs,
            chatter_inputs=matched_chatter,
        )

    def _matching_chatter_items(
        self,
        profile: NormalizedWarehouseCarryProfile,
        *,
        compared_markets: Sequence[str],
        chatter_items: Sequence[NormalizedMarketChatterItem],
    ) -> tuple[NormalizedMarketChatterItem, ...]:
        compared_market_keys = {self._slug(value) for value in compared_markets}
        selected: list[NormalizedMarketChatterItem] = []
        for item in chatter_items:
            if item.variety_keys and profile.variety_key not in item.variety_keys:
                continue
            if item.market_keys and compared_market_keys and not compared_market_keys.intersection(
                item.market_keys
            ):
                continue
            selected.append(item)
        return tuple(selected[:3])

    def _direction_for(
        self,
        *,
        official_market_count: int,
        price_delta_percent: float | None,
    ) -> str:
        if price_delta_percent is None:
            return "WATCH"
        if official_market_count < _MIN_OFFICIAL_MARKETS_FOR_DIRECTION:
            return "WATCH"
        if price_delta_percent >= _PRICE_WATCH_BAND_PERCENT:
            return "SELL"
        if price_delta_percent <= -_PRICE_WATCH_BAND_PERCENT:
            return "BUY"
        return "WATCH"

    def _trend_note(
        self,
        *,
        variety_name: str,
        carry_price_per_quintal: int,
        official_modal_price_per_quintal: int | None,
        official_market_count: int,
        price_delta_percent: float | None,
        direction: str,
    ) -> str:
        if official_modal_price_per_quintal is None or price_delta_percent is None:
            return (
                f"No approved official board snapshot matched {variety_name}, so keep it on WATCH."
            )
        if official_market_count < _MIN_OFFICIAL_MARKETS_FOR_DIRECTION:
            return (
                f"Only {official_market_count} approved official board matched {variety_name}; "
                "wait for a second board before changing direction."
            )
        if direction == "SELL":
            return (
                f"Approved official boards are holding {variety_name} near INR "
                f"{official_modal_price_per_quintal}/qtl, {abs(price_delta_percent):.2f}% "
                f"above the warehouse carry price of INR {carry_price_per_quintal}/qtl. "
                "Current boards support SELL, but this stays a watchful signal rather than a forecast."
            )
        if direction == "BUY":
            return (
                f"Approved official boards are sitting {abs(price_delta_percent):.2f}% below the "
                f"warehouse carry price of INR {carry_price_per_quintal}/qtl for {variety_name}. "
                "Current boards support BUY, but this stays a watchful signal rather than a forecast."
            )
        return (
            f"Approved official boards place {variety_name} close to the warehouse carry price of INR "
            f"{carry_price_per_quintal}/qtl. Keep WATCH until the spread widens beyond the conservative band."
        )

    def _build_chilli_chatter(
        self,
        *,
        variety_name: str,
        direction: str,
        chatter_items: Sequence[NormalizedMarketChatterItem],
    ) -> str:
        if not chatter_items:
            return (
                f"No separate Chilli Chatter was captured for {variety_name}. "
                f"Official boards alone set {direction}."
            )
        parts: list[str] = []
        for item in chatter_items[:2]:
            if item.summary and item.summary.casefold() != item.headline.casefold():
                parts.append(f"{item.headline}: {item.summary}")
            else:
                parts.append(item.headline)
        joined = "; ".join(part.rstrip(".") for part in parts)
        return f"{joined}. Informational only; official boards alone set {direction}."

    def _ordered_unique(self, values: Iterable[str]) -> tuple[str, ...]:
        seen: set[str] = set()
        ordered: list[str] = []
        for value in values:
            key = value.casefold()
            if key in seen:
                continue
            seen.add(key)
            ordered.append(value)
        return tuple(ordered)

    def _slug(self, value: str) -> str:
        return value.casefold().replace(" ", "-")
