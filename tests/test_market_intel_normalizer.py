from __future__ import annotations

import pytest

from src.market_intel.models import (
    AliasRule,
    MarketChatterItem,
    OfficialMarketPriceSnapshot,
    WarehouseCarryProfile,
)
from src.market_intel.normalizer import (
    build_alias_lookup,
    normalize_carry_profiles,
    normalize_market_chatter_items,
    normalize_official_price_snapshots,
)


@pytest.fixture
def alias_lookups() -> tuple[dict[str, str], dict[str, str]]:
    market_alias_lookup = build_alias_lookup(
        (
            AliasRule("Guntur", ("Guntur Mirchi Yard",)),
            AliasRule("Warangal", ("Warangal Enumamula",)),
        )
    )
    variety_alias_lookup = build_alias_lookup(
        (
            AliasRule("Teja", ("334 Teja",)),
            AliasRule("Byadgi", ("Bedgi",)),
        )
    )
    return (market_alias_lookup, variety_alias_lookup)


def test_market_intel_normalization_maps_aliases_and_sanitizes_chatter(
    alias_lookups: tuple[dict[str, str], dict[str, str]],
) -> None:
    market_alias_lookup, variety_alias_lookup = alias_lookups

    official = normalize_official_price_snapshots(
        [
            OfficialMarketPriceSnapshot(
                source_key="agmarknet_official_board",
                source_url="fixture://official-board/agmarknet/guntur-teja",
                captured_at="2024-07-08T09:00:00Z",
                market_name="Guntur Mirchi Yard",
                variety_name="334 Teja",
                min_price_per_quintal="18,500",
                modal_price_per_quintal="18,950",
                max_price_per_quintal="19,300",
            )
        ],
        market_alias_lookup=market_alias_lookup,
        variety_alias_lookup=variety_alias_lookup,
    )
    chatter = normalize_market_chatter_items(
        [
            MarketChatterItem(
                source_key="trade_press_digest",
                source_url="fixture://trade-press/guntur-teja",
                published_at="2024-07-08T08:00:00Z",
                headline="<b>Tighter arrivals</b> in Guntur https://example.invalid/story",
                snippet="Buyers stayed selective for 334 Teja lots.",
                market_names=("Guntur Mirchi Yard",),
                variety_names=("334 Teja",),
            )
        ],
        market_alias_lookup=market_alias_lookup,
        variety_alias_lookup=variety_alias_lookup,
    )
    carry_profiles = normalize_carry_profiles(
        [
            WarehouseCarryProfile(
                variety_name="Bedgi",
                carry_price_per_quintal=24000,
                available_bags=80,
            )
        ],
        variety_alias_lookup=variety_alias_lookup,
    )

    assert official[0].market_name == "Guntur"
    assert official[0].market_key == "guntur"
    assert official[0].variety_name == "Teja"
    assert official[0].variety_key == "teja"
    assert official[0].modal_price_per_quintal == 18950
    assert chatter[0].headline == "Tighter arrivals in Guntur"
    assert chatter[0].market_keys == ("guntur",)
    assert chatter[0].variety_keys == ("teja",)
    assert carry_profiles[0].variety_name == "Byadgi"
    assert carry_profiles[0].variety_key == "byadgi"


def test_market_intel_normalization_rejects_non_official_direction_source(
    alias_lookups: tuple[dict[str, str], dict[str, str]],
) -> None:
    market_alias_lookup, variety_alias_lookup = alias_lookups

    with pytest.raises(ValueError, match="expected official_board"):
        normalize_official_price_snapshots(
            [
                OfficialMarketPriceSnapshot(
                    source_key="trade_press_digest",
                    source_url="fixture://trade-press/guntur-teja",
                    captured_at="2024-07-08T09:00:00Z",
                    market_name="Guntur Mirchi Yard",
                    variety_name="334 Teja",
                    min_price_per_quintal=18500,
                    modal_price_per_quintal=18950,
                    max_price_per_quintal=19300,
                )
            ],
            market_alias_lookup=market_alias_lookup,
            variety_alias_lookup=variety_alias_lookup,
        )
