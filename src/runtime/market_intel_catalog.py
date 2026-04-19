"""Loads curated market-intelligence scenarios and alias metadata."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from src.market_intel.models import (
    AliasRule,
    MarketChatterItem,
    OfficialMarketPriceSnapshot,
    WarehouseCarryProfile,
)


@dataclass(frozen=True, slots=True)
class MarketIntelScenario:
    name: str
    official_price_snapshots: tuple[OfficialMarketPriceSnapshot, ...]
    chatter_items: tuple[MarketChatterItem, ...]


class MarketIntelScenarioCatalog:
    """Provides the approved market-intelligence fixtures used by the worker."""

    mode_name = "approved_market_fixtures"

    def __init__(self, config_path: Path) -> None:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        self._market_rules = tuple(
            AliasRule(
                canonical_name=entry["canonical_name"],
                aliases=tuple(entry.get("aliases", ())),
            )
            for entry in payload.get("markets", [])
        )
        self._variety_rules = tuple(
            AliasRule(
                canonical_name=entry["canonical_name"],
                aliases=tuple(entry.get("aliases", ())),
            )
            for entry in payload.get("varieties", [])
        )
        self._warehouse_carry_profiles = tuple(
            WarehouseCarryProfile(
                variety_name=entry["variety_name"],
                carry_price_per_quintal=int(entry["carry_price_per_quintal"]),
                available_bags=int(entry.get("available_bags", 0)),
            )
            for entry in payload.get("warehouse_carry_profiles", [])
        )
        self._scenarios = {
            name: MarketIntelScenario(
                name=name,
                official_price_snapshots=tuple(
                    OfficialMarketPriceSnapshot(
                        source_key=entry["source_key"],
                        source_url=entry["source_url"],
                        captured_at=entry["captured_at"],
                        market_name=entry["market_name"],
                        variety_name=entry["variety_name"],
                        min_price_per_quintal=int(entry["min_price_per_quintal"]),
                        modal_price_per_quintal=int(entry["modal_price_per_quintal"]),
                        max_price_per_quintal=int(entry["max_price_per_quintal"]),
                    )
                    for entry in scenario.get("official_price_snapshots", [])
                ),
                chatter_items=tuple(
                    MarketChatterItem(
                        source_key=entry["source_key"],
                        source_url=entry["source_url"],
                        published_at=entry["published_at"],
                        headline=entry["headline"],
                        snippet=entry.get("snippet", ""),
                        market_names=tuple(entry.get("market_names", ())),
                        variety_names=tuple(entry.get("variety_names", ())),
                    )
                    for entry in scenario.get("chatter_items", [])
                ),
            )
            for name, scenario in payload.get("sample_scenarios", {}).items()
        }

    @property
    def market_rules(self) -> tuple[AliasRule, ...]:
        return self._market_rules

    @property
    def variety_rules(self) -> tuple[AliasRule, ...]:
        return self._variety_rules

    @property
    def warehouse_carry_profiles(self) -> tuple[WarehouseCarryProfile, ...]:
        return self._warehouse_carry_profiles

    @property
    def available_scenarios(self) -> tuple[str, ...]:
        return tuple(self._scenarios)

    def scenario(self, name: str) -> MarketIntelScenario:
        selected = self._scenarios.get(name)
        if selected is None:
            available = ", ".join(self.available_scenarios) or "<none>"
            raise ValueError(
                f"Unknown market-intel scenario {name!r}. Available scenarios: {available}"
            )
        return selected
