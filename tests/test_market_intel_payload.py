from __future__ import annotations

from src.exporter.market_intel_payload import (
    build_market_carry_benchmark_ingestion_request,
    build_market_chatter_ingestion_request,
    build_market_price_snapshot_ingestion_request,
    build_market_intelligence_request,
    export_market_signal_payload,
)
from src.market_intel.models import (
    NormalizedMarketChatterItem,
    NormalizedOfficialMarketPriceSnapshot,
    VarietyMarketSignal,
)


def test_export_market_signal_payload_keeps_official_and_chatter_inputs_separate() -> None:
    signal = VarietyMarketSignal(
        variety_name="Teja",
        variety_key="teja",
        available_bags=420,
        carry_price_per_quintal=18500,
        official_market_count=2,
        official_min_price_per_quintal=19800,
        official_modal_price_per_quintal=20350,
        official_max_price_per_quintal=20850,
        direction="SELL",
        price_delta_per_quintal=1850,
        price_delta_percent=10.0,
        compared_markets=("Guntur", "Warangal"),
        trend_note=(
            "Approved official boards are holding Teja above carry price. "
            "Current boards support SELL without forecasting the next move."
        ),
        chilli_chatter=(
            "Tighter arrivals keep spot bids firm in Guntur. Informational only; official boards alone set SELL."
        ),
        official_price_inputs=(
            NormalizedOfficialMarketPriceSnapshot(
                source_key="agmarknet_official_board",
                source_url="fixture://official-board/agmarknet/guntur-teja-high",
                captured_at="2024-07-10T09:00:00Z",
                market_name="Guntur",
                market_key="guntur",
                variety_name="Teja",
                variety_key="teja",
                min_price_per_quintal=19800,
                modal_price_per_quintal=20250,
                max_price_per_quintal=20700,
            ),
            NormalizedOfficialMarketPriceSnapshot(
                source_key="telangana_market_board",
                source_url="fixture://official-board/telangana/warangal-teja-high",
                captured_at="2024-07-10T09:10:00Z",
                market_name="Warangal",
                market_key="warangal",
                variety_name="Teja",
                variety_key="teja",
                min_price_per_quintal=20000,
                modal_price_per_quintal=20450,
                max_price_per_quintal=20850,
            ),
        ),
        chatter_inputs=(
            NormalizedMarketChatterItem(
                source_key="trade_press_digest",
                source_url="fixture://trade-press/high-teja",
                published_at="2024-07-10T08:00:00Z",
                headline="Tighter arrivals keep Teja spot bids firm in Guntur",
                summary="Trade desks mention active checking for brighter lots.",
                market_names=("Guntur",),
                market_keys=("guntur",),
                variety_names=("Teja",),
                variety_keys=("teja",),
            ),
        ),
    )

    payload = export_market_signal_payload(
        signal,
        captured_at="2024-07-10T09:30:00Z",
        scenario="high-rate",
        warehouse_id="guntur-hub",
        intel_source="buyer-discovery-market-intel-worker",
    )

    assert payload == {
        "signalRef": "buyer-discovery-market-intel-worker|guntur-hub|teja|high-rate",
        "signalSource": "buyer-discovery-market-intel-worker",
        "warehouseId": "guntur-hub",
        "scenario": "high-rate",
        "capturedAt": "2024-07-10T09:30:00Z",
        "direction": "SELL",
        "directionBasis": "OFFICIAL_BOARD_ONLY",
        "variety": "Teja",
        "varietyKey": "teja",
        "availableBags": 420,
        "carryPricePerQuintal": 18500,
        "officialMarketCount": 2,
        "officialMinPricePerQuintal": 19800,
        "officialModalPricePerQuintal": 20350,
        "officialMaxPricePerQuintal": 20850,
        "priceDeltaPerQuintal": 1850,
        "priceDeltaPercent": 10.0,
        "comparedMarkets": ["Guntur", "Warangal"],
        "trendNote": (
            "Approved official boards are holding Teja above carry price. "
            "Current boards support SELL without forecasting the next move."
        ),
        "chilliChatter": (
            "Tighter arrivals keep spot bids firm in Guntur. Informational only; official boards alone set SELL."
        ),
        "officialPriceInputs": [
            {
                "sourceLabel": "agmarknet_official_board",
                "sourceUrl": "fixture://official-board/agmarknet/guntur-teja-high",
                "capturedAt": "2024-07-10T09:00:00Z",
                "market": "Guntur",
                "marketKey": "guntur",
                "variety": "Teja",
                "varietyKey": "teja",
                "minPricePerQuintal": 19800,
                "modalPricePerQuintal": 20250,
                "maxPricePerQuintal": 20700,
            },
            {
                "sourceLabel": "telangana_market_board",
                "sourceUrl": "fixture://official-board/telangana/warangal-teja-high",
                "capturedAt": "2024-07-10T09:10:00Z",
                "market": "Warangal",
                "marketKey": "warangal",
                "variety": "Teja",
                "varietyKey": "teja",
                "minPricePerQuintal": 20000,
                "modalPricePerQuintal": 20450,
                "maxPricePerQuintal": 20850,
            },
        ],
        "chatterInputs": [
            {
                "sourceLabel": "trade_press_digest",
                "sourceUrl": "fixture://trade-press/high-teja",
                "publishedAt": "2024-07-10T08:00:00Z",
                "headline": "Tighter arrivals keep Teja spot bids firm in Guntur",
                "summary": "Trade desks mention active checking for brighter lots.",
                "marketKeys": ["guntur"],
                "varietyKeys": ["teja"],
            }
        ],
    }

    assert build_market_intelligence_request(
        [signal],
        captured_at="2024-07-10T09:30:00Z",
        scenario="high-rate",
        warehouse_id="guntur-hub",
        intel_source="buyer-discovery-market-intel-worker",
    ) == {
        "capturedAt": "2024-07-10T09:30:00Z",
        "scenario": "high-rate",
        "signalSource": "buyer-discovery-market-intel-worker",
        "warehouseId": "guntur-hub",
        "signals": [payload],
    }

    assert build_market_price_snapshot_ingestion_request([signal]) == {
        "snapshots": [
            {
                "chilliVariety": "Teja",
                "marketName": "Guntur",
                "sourceLabel": "Agmarknet Official Board",
                "sourceUrl": "fixture://official-board/agmarknet/guntur-teja-high",
                "sourceType": "OFFICIAL_MANDI_BOARD",
                "officialPricePerKg": 202.5,
                "capturedAt": "2024-07-10T09:00:00Z",
            },
            {
                "chilliVariety": "Teja",
                "marketName": "Warangal",
                "sourceLabel": "Telangana Market Board",
                "sourceUrl": "fixture://official-board/telangana/warangal-teja-high",
                "sourceType": "OFFICIAL_MARKET_BOARD",
                "officialPricePerKg": 204.5,
                "capturedAt": "2024-07-10T09:10:00Z",
            },
        ]
    }
    assert build_market_chatter_ingestion_request([signal]) == {
        "items": [
            {
                "chilliVariety": "Teja",
                "headline": "Tighter arrivals keep Teja spot bids firm in Guntur",
                "summary": "Trade desks mention active checking for brighter lots.",
                "sourceLabel": "Trade Press Digest",
                "sourceUrl": "fixture://trade-press/high-teja",
                "chatterType": "TRADE_COVERAGE",
                "publishedAt": "2024-07-10T08:00:00Z",
                "capturedAt": "2024-07-10T08:00:00Z",
            }
        ]
    }
    assert build_market_carry_benchmark_ingestion_request(
        [signal],
        captured_at="2024-07-10T09:30:00Z",
    ) == {
        "benchmarks": [
            {
                "chilliVariety": "Teja",
                "carryPricePerKg": 185.0,
                "sourceType": "WORKER_PROFILE",
                "weightSource": "PROFILE_FALLBACK",
                "bagCount": 420,
                "weightKg": None,
                "capturedAt": "2024-07-10T09:30:00Z",
            }
        ]
    }
