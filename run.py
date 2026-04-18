#!/usr/bin/env python3
"""CLI entry point for the buyer-discovery worker scaffold."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config.settings import settings
from src.crawler.source_registry import SourceRegistry


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Preview deterministic crawl jobs for buyer discovery seeds."
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
    args = parser.parse_args()

    registry = SourceRegistry(settings.sources)
    if args.list_sources:
        print(json.dumps([source.to_dict() for source in registry.sources], indent=2))
        return 0

    towns = settings.resolve_towns(args.towns)
    query_seeds = tuple(args.queries or settings.query_seeds)
    jobs = registry.schedule_jobs(towns, query_seeds)
    print(json.dumps([job.to_dict() for job in jobs], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
