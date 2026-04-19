#!/usr/bin/env python3
"""CLI entry point for the buyer-discovery worker."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config.settings import settings
from src.integration.backend_client import BackendIngestionClient
from src.crawler.source_registry import SourceRegistry
from src.exporter.market_intel_payload import (
    build_market_chatter_ingestion_request,
    build_market_price_snapshot_ingestion_request,
)
from src.runtime.market_intel_pipeline import MarketIntelPipeline
from src.runtime.pipeline import SampleDiscoveryPipeline


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the buyer discovery worker against sample snapshots or live web sources."
    )
    parser.add_argument(
        "--town",
        action="append",
        dest="towns",
        help="Configured town name to include. Repeat to add more.",
    )
    parser.add_argument(
        "--query",
        action="append",
        dest="queries",
        help="Intent query seed to include. Repeat to add more.",
    )
    parser.add_argument(
        "--list-sources",
        action="store_true",
        help="Print configured sources and exit.",
    )
    parser.add_argument(
        "--preview-jobs",
        action="store_true",
        help="Print scheduled jobs and exit without running extraction or ingestion.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run extraction and payload generation without posting to the backend.",
    )
    parser.add_argument(
        "--market-intel",
        action="store_true",
        help="Run the worker market-intelligence lane instead of buyer discovery.",
    )
    parser.add_argument(
        "--market-scenario",
        help="Approved named market-intel scenario fixture to run.",
    )
    args = parser.parse_args()

    registry = SourceRegistry(settings.sources)
    if args.list_sources:
        print(json.dumps([source.to_dict() for source in registry.sources], indent=2))
        return 0

    if args.market_intel:
        try:
            market_result = MarketIntelPipeline(settings).run(scenario=args.market_scenario)
        except ValueError as exc:
            print(
                json.dumps(
                    {
                        "error": str(exc),
                        "ingestRequested": False,
                    },
                    indent=2,
                ),
                file=sys.stderr,
            )
            return 1

        summary = market_result.to_dict()
        summary["ingestRequested"] = False
        summary["marketIntelligenceRequest"] = market_result.market_intelligence_request
        if args.dry_run or market_result.scenario != "baseline":
            print(json.dumps(summary, indent=2))
            return 0
        if not settings.internal_api_key:
            print(
                json.dumps(
                    {
                        "error": (
                            "BUYER_DISCOVERY_INTERNAL_API_KEY must be configured before "
                            "market-intelligence ingestion."
                        ),
                        "ingestRequested": False,
                    },
                    indent=2,
                ),
                file=sys.stderr,
            )
            return 1

        client = BackendIngestionClient(
            base_url=settings.backend_base_url,
            tenant_id=settings.tenant_id,
            warehouse_id=settings.warehouse_id,
            api_header=settings.internal_api_header,
            api_key=settings.internal_api_key,
        )
        summary["ingestRequested"] = True
        summary["ingestion"] = {
            "priceSnapshots": client.ingest_market_price_snapshots(
                build_market_price_snapshot_ingestion_request(market_result.signals)
            ),
            "chatter": client.ingest_market_chatter(
                build_market_chatter_ingestion_request(market_result.signals)
            ),
        }
        print(json.dumps(summary, indent=2))
        return 0

    towns = settings.resolve_towns(args.towns)
    query_seeds = tuple(args.queries or settings.query_seeds)
    jobs = registry.schedule_jobs(towns, query_seeds)
    if args.preview_jobs:
        print(json.dumps([job.to_dict() for job in jobs], indent=2))
        return 0

    pipeline = SampleDiscoveryPipeline(settings)
    result = pipeline.run(towns, query_seeds)
    summary = result.to_dict()
    payload_candidates = result.ingestion_request.get("candidates", [])
    if args.dry_run or not payload_candidates:
        summary["ingestRequested"] = False
        print(json.dumps(summary, indent=2))
        return 0
    if not settings.internal_api_key:
        print(
            json.dumps(
                {
                    "error": "BUYER_DISCOVERY_INTERNAL_API_KEY must be configured before ingestion.",
                    "ingestRequested": False,
                },
                indent=2,
            ),
            file=sys.stderr,
        )
        return 1

    client = BackendIngestionClient(
        base_url=settings.backend_base_url,
        tenant_id=settings.tenant_id,
        warehouse_id=settings.warehouse_id,
        api_header=settings.internal_api_header,
        api_key=settings.internal_api_key,
    )
    summary["ingestRequested"] = True
    summary["ingestion"] = client.ingest(result.ingestion_request)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
