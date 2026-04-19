"""Typed market-intelligence models for deterministic worker outputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

MarketDirection = Literal["BUY", "SELL", "WATCH"]


@dataclass(frozen=True, slots=True)
class AliasRule:
    canonical_name: str
    aliases: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class WarehouseCarryProfile:
    variety_name: str
    carry_price_per_quintal: int
    available_bags: int = 0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class OfficialMarketPriceSnapshot:
    source_key: str
    source_url: str
    captured_at: str
    market_name: str
    variety_name: str
    min_price_per_quintal: int
    modal_price_per_quintal: int
    max_price_per_quintal: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class MarketChatterItem:
    source_key: str
    source_url: str
    published_at: str
    headline: str
    snippet: str
    market_names: tuple[str, ...] = ()
    variety_names: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class NormalizedWarehouseCarryProfile:
    variety_name: str
    variety_key: str
    carry_price_per_quintal: int
    available_bags: int = 0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class NormalizedOfficialMarketPriceSnapshot:
    source_key: str
    source_url: str
    captured_at: str
    market_name: str
    market_key: str
    variety_name: str
    variety_key: str
    min_price_per_quintal: int
    modal_price_per_quintal: int
    max_price_per_quintal: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class NormalizedMarketChatterItem:
    source_key: str
    source_url: str
    published_at: str
    headline: str
    summary: str
    market_names: tuple[str, ...] = ()
    market_keys: tuple[str, ...] = ()
    variety_names: tuple[str, ...] = ()
    variety_keys: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class VarietyMarketSignal:
    variety_name: str
    variety_key: str
    available_bags: int
    carry_price_per_quintal: int
    official_market_count: int
    official_min_price_per_quintal: int | None
    official_modal_price_per_quintal: int | None
    official_max_price_per_quintal: int | None
    direction: MarketDirection
    price_delta_per_quintal: int | None
    price_delta_percent: float | None
    compared_markets: tuple[str, ...] = ()
    trend_note: str = ""
    chilli_chatter: str = ""
    official_price_inputs: tuple[NormalizedOfficialMarketPriceSnapshot, ...] = ()
    chatter_inputs: tuple[NormalizedMarketChatterItem, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
