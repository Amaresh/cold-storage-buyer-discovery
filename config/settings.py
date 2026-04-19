"""Configuration for the buyer-discovery worker scaffold."""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

from src.common.models import SourceConfig, TownSeed


def _split_env_list(name: str, default: Sequence[str]) -> tuple[str, ...]:
    raw_value = os.getenv(name, "")
    if not raw_value.strip():
        return tuple(default)
    return tuple(item.strip() for item in raw_value.split(",") if item.strip())


def _env_flag(name: str, default: bool) -> bool:
    raw_value = os.getenv(name, "")
    if not raw_value.strip():
        return default

    normalized = raw_value.strip().casefold()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Unsupported boolean value for {name}: {raw_value!r}")


def _env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name, "")
    if not raw_value.strip():
        return default
    return int(raw_value.strip())


def _default_sources() -> tuple[SourceConfig, ...]:
    return (
        SourceConfig(
            name="google_search_seed",
            enabled=True,
            priority=1,
            adapter_key="google_search_seed",
            notes="Discovery seeds only; do not treat Google results as the durable buyer record.",
        ),
        SourceConfig(
            name="business_directory",
            enabled=True,
            priority=2,
            adapter_key="business_directory",
        ),
    )


@dataclass(slots=True)
class Settings:
    base_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    starter_towns: tuple[str, ...] = field(
        default_factory=lambda: _split_env_list(
            "BUYER_DISCOVERY_STARTER_TOWNS",
            ("Guntur",),
        )
    )
    query_seeds: tuple[str, ...] = field(
        default_factory=lambda: _split_env_list(
            "BUYER_DISCOVERY_QUERY_SEEDS",
            (
                "chilli wholesaler",
                "chilli commission agent",
            ),
        )
    )
    warehouse_hub_town: str = field(
        default_factory=lambda: (
            os.getenv("BUYER_DISCOVERY_WAREHOUSE_HUB_TOWN", "Guntur").strip()
            or "Guntur"
        )
    )
    max_radius_km: int = field(
        default_factory=lambda: _env_int("BUYER_DISCOVERY_MAX_RADIUS_KM", 300)
    )
    sources: tuple[SourceConfig, ...] = field(default_factory=_default_sources)
    google_search_url_template: str = field(
        default_factory=lambda: (
            os.getenv(
                "BUYER_DISCOVERY_GOOGLE_SEARCH_URL_TEMPLATE",
                "https://www.google.com/search?q={query}",
            ).strip()
            or "https://www.google.com/search?q={query}"
        )
    )
    search_fallback_url_template: str = field(
        default_factory=lambda: (
            os.getenv(
                "BUYER_DISCOVERY_SEARCH_FALLBACK_URL_TEMPLATE",
                "https://duckduckgo.com/html/?q={query}",
            ).strip()
            or "https://duckduckgo.com/html/?q={query}"
        )
    )
    search_fallback_url_templates: tuple[str, ...] = field(
        default_factory=lambda: _split_env_list(
            "BUYER_DISCOVERY_SEARCH_FALLBACK_URL_TEMPLATES",
            (),
        )
    )
    business_directory_url_template: str = field(
        default_factory=lambda: os.getenv(
            "BUYER_DISCOVERY_BUSINESS_DIRECTORY_URL_TEMPLATE",
            "",
        ).strip()
    )
    fetch_timeout_seconds: int = field(
        default_factory=lambda: _env_int("BUYER_DISCOVERY_FETCH_TIMEOUT_SECONDS", 20)
    )
    http_user_agent: str = field(
        default_factory=lambda: (
            os.getenv(
                "BUYER_DISCOVERY_HTTP_USER_AGENT",
                (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
            ).strip()
            or (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
        )
    )
    backend_base_url: str = field(
        default_factory=lambda: (
            os.getenv("BUYER_DISCOVERY_BACKEND_BASE_URL", "http://127.0.0.1:9090").strip().rstrip("/")
            or "http://127.0.0.1:9090"
        )
    )
    internal_api_header: str = field(
        default_factory=lambda: (
            os.getenv("BUYER_DISCOVERY_INTERNAL_API_HEADER", "X-API-Key").strip()
            or "X-API-Key"
        )
    )
    internal_api_key: str = field(
        default_factory=lambda: os.getenv("BUYER_DISCOVERY_INTERNAL_API_KEY", "").strip()
    )
    tenant_id: str = field(
        default_factory=lambda: (
            os.getenv("BUYER_DISCOVERY_TENANT_ID", "demo-tenant").strip()
            or "demo-tenant"
        )
    )
    warehouse_id: str = field(
        default_factory=lambda: (
            os.getenv("BUYER_DISCOVERY_WAREHOUSE_ID", "guntur-hub").strip()
            or "guntur-hub"
        )
    )
    discovery_source: str = field(
        default_factory=lambda: (
            os.getenv("BUYER_DISCOVERY_DISCOVERY_SOURCE", "buyer-discovery-worker").strip()
            or "buyer-discovery-worker"
        )
    )
    market_intel_source: str = field(
        default_factory=lambda: (
            os.getenv(
                "BUYER_DISCOVERY_MARKET_INTEL_SOURCE",
                "buyer-discovery-market-intel-worker",
            ).strip()
            or "buyer-discovery-market-intel-worker"
        )
    )
    market_intel_scenario: str = field(
        default_factory=lambda: (
            os.getenv("BUYER_DISCOVERY_MARKET_INTEL_SCENARIO", "baseline").strip()
            or "baseline"
        )
    )
    use_sample_snapshots: bool = field(
        default_factory=lambda: _env_flag("BUYER_DISCOVERY_USE_SAMPLE_SNAPSHOTS", True)
    )

    @property
    def towns_path(self) -> Path:
        return self.base_dir / "config" / "towns.json"

    @property
    def sample_snapshots_dir(self) -> Path:
        return self.base_dir / "config" / "sample_snapshots"

    @property
    def market_intel_path(self) -> Path:
        return self.base_dir / "config" / "market_intel.json"

    def load_towns(self) -> tuple[TownSeed, ...]:
        payload = json.loads(self.towns_path.read_text(encoding="utf-8"))
        return tuple(TownSeed(**entry) for entry in payload.get("towns", []))

    def resolve_towns(self, names: Sequence[str] | None = None) -> tuple[TownSeed, ...]:
        all_towns = self.load_towns()
        if names:
            requested = tuple(name.strip() for name in names if name.strip())
            return self._resolve_named_towns(all_towns, requested)

        radius_towns = self._resolve_radius_towns(all_towns)
        if radius_towns:
            return radius_towns

        requested = tuple(name.strip() for name in self.starter_towns if name.strip())
        return self._resolve_named_towns(all_towns, requested)

    def _resolve_named_towns(
        self,
        all_towns: Sequence[TownSeed],
        requested: Sequence[str],
    ) -> tuple[TownSeed, ...]:
        lookup = {name.casefold() for name in requested}
        resolved = [town for town in all_towns if town.name.casefold() in lookup]
        if not resolved:
            available = ", ".join(sorted(town.name for town in all_towns))
            wanted = ", ".join(requested) or "<none>"
            raise ValueError(
                f"No configured towns matched: {wanted}. Available towns: {available}"
            )
        return tuple(
            sorted(
                resolved,
                key=lambda town: (
                    town.priority,
                    town.name.casefold(),
                    town.state.casefold(),
                ),
            )
        )

    def _resolve_radius_towns(self, all_towns: Sequence[TownSeed]) -> tuple[TownSeed, ...]:
        if not self.warehouse_hub_town.strip():
            return ()

        hub_town = next(
            (
                town
                for town in all_towns
                if town.name.casefold() == self.warehouse_hub_town.casefold()
            ),
            None,
        )
        if hub_town is None:
            available = ", ".join(sorted(town.name for town in all_towns))
            raise ValueError(
                f"No configured town matched warehouse hub {self.warehouse_hub_town!r}. "
                f"Available towns: {available}"
            )
        if not hub_town.has_coordinates:
            raise ValueError(
                f"Configured warehouse hub town {hub_town.name} is missing latitude/longitude."
            )

        distance_by_town: dict[str, float] = {}
        resolved: list[TownSeed] = []
        for town in all_towns:
            distance_km = 0.0 if town.name.casefold() == hub_town.name.casefold() else hub_town.distance_to(town)
            if distance_km is None or distance_km > self.max_radius_km:
                continue
            distance_by_town[town.name.casefold()] = distance_km
            resolved.append(town)

        return tuple(
            sorted(
                resolved,
                key=lambda town: (
                    round(distance_by_town.get(town.name.casefold(), float("inf")), 3),
                    town.priority,
                    town.name.casefold(),
                    town.state.casefold(),
                ),
            )
        )

    def town_state_by_name(self) -> dict[str, str]:
        return {
            town.name.casefold(): town.state
            for town in self.load_towns()
            if town.state
        }


settings = Settings()
