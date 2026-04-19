from __future__ import annotations

from config.settings import Settings
from src.market_intel.normalizer import (
    normalize_market_chatter_items,
    normalize_official_price_snapshots,
)
from src.runtime.market_intel_pipeline import MarketIntelPipeline


def test_market_intel_pipeline_builds_conservative_baseline_watch_outputs() -> None:
    settings = Settings()
    pipeline = MarketIntelPipeline(settings)

    result = pipeline.run()

    assert result.mode == "approved_market_fixtures"
    assert result.scenario == "baseline"
    assert result.official_snapshot_count == 4
    assert result.chatter_item_count == 2
    assert result.signal_count == 2
    assert [signal.variety_name for signal in result.signals] == ["Byadgi", "Teja"]
    assert [signal.direction for signal in result.signals] == ["WATCH", "WATCH"]
    assert result.market_intelligence_request["scenario"] == "baseline"
    assert [signal["direction"] for signal in result.market_intelligence_request["signals"]] == [
        "WATCH",
        "WATCH",
    ]
    assert all(
        signal["directionBasis"] == "OFFICIAL_BOARD_ONLY"
        for signal in result.market_intelligence_request["signals"]
    )


def test_market_intel_pipeline_supports_safe_high_and_low_rate_scenarios() -> None:
    settings = Settings()
    pipeline = MarketIntelPipeline(settings)

    high_rate = pipeline.run(scenario="high-rate")
    low_rate = pipeline.run(scenario="low-rate")

    assert [signal.direction for signal in high_rate.signals] == ["SELL", "SELL"]
    assert [signal.direction for signal in low_rate.signals] == ["BUY", "BUY"]
    assert high_rate.market_intelligence_request["scenario"] == "high-rate"
    assert low_rate.market_intelligence_request["scenario"] == "low-rate"
    assert all(
        signal["officialModalPricePerQuintal"] > signal["carryPricePerQuintal"]
        for signal in high_rate.market_intelligence_request["signals"]
    )
    assert all(
        signal["officialModalPricePerQuintal"] < signal["carryPricePerQuintal"]
        for signal in low_rate.market_intelligence_request["signals"]
    )


def test_market_intel_pipeline_keeps_chatter_separate_from_direction() -> None:
    settings = Settings()
    pipeline = MarketIntelPipeline(settings)
    high_scenario = pipeline._catalog.scenario("high-rate")
    low_scenario = pipeline._catalog.scenario("low-rate")
    official_snapshots = normalize_official_price_snapshots(
        high_scenario.official_price_snapshots,
        market_alias_lookup=pipeline._market_alias_lookup,
        variety_alias_lookup=pipeline._variety_alias_lookup,
    )
    supportive_chatter = normalize_market_chatter_items(
        high_scenario.chatter_items,
        market_alias_lookup=pipeline._market_alias_lookup,
        variety_alias_lookup=pipeline._variety_alias_lookup,
    )
    softer_chatter = normalize_market_chatter_items(
        low_scenario.chatter_items,
        market_alias_lookup=pipeline._market_alias_lookup,
        variety_alias_lookup=pipeline._variety_alias_lookup,
    )
    teja_profile = next(
        profile
        for profile in pipeline._carry_profiles
        if profile.variety_key == "teja"
    )

    supportive_signal = pipeline._build_signal(
        teja_profile,
        official_snapshots,
        supportive_chatter,
    )
    softer_signal = pipeline._build_signal(
        teja_profile,
        official_snapshots,
        softer_chatter,
    )

    assert supportive_signal.direction == "SELL"
    assert softer_signal.direction == "SELL"
    assert supportive_signal.chilli_chatter != softer_signal.chilli_chatter
