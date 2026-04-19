from __future__ import annotations

from config.settings import Settings


def test_resolve_towns_uses_hub_radius_when_names_are_not_provided(monkeypatch) -> None:
    monkeypatch.setenv("BUYER_DISCOVERY_WAREHOUSE_HUB_TOWN", "Guntur")
    monkeypatch.setenv("BUYER_DISCOVERY_MAX_RADIUS_KM", "300")

    settings = Settings()
    towns = settings.resolve_towns()
    town_names = [town.name for town in towns]

    assert "Guntur" in town_names
    assert "Vijayawada" in town_names
    assert "Khammam" in town_names
    assert "Warangal" in town_names
    assert "Visakhapatnam" not in town_names


def test_resolve_towns_keeps_explicit_selection_even_if_outside_hub_radius(monkeypatch) -> None:
    monkeypatch.setenv("BUYER_DISCOVERY_WAREHOUSE_HUB_TOWN", "Guntur")
    monkeypatch.setenv("BUYER_DISCOVERY_MAX_RADIUS_KM", "300")

    settings = Settings()
    towns = settings.resolve_towns(["Visakhapatnam"])

    assert [town.name for town in towns] == ["Visakhapatnam"]


def test_search_fallback_url_templates_preserve_declared_order(monkeypatch) -> None:
    monkeypatch.setenv(
        "BUYER_DISCOVERY_SEARCH_FALLBACK_URL_TEMPLATES",
        "https://www.startpage.com/do/dsearch?query={query}, https://search.yahoo.com/search?p={query}",
    )

    settings = Settings()

    assert settings.search_fallback_url_templates == (
        "https://www.startpage.com/do/dsearch?query={query}",
        "https://search.yahoo.com/search?p={query}",
    )


def test_market_intel_scenario_can_be_selected_from_env(monkeypatch) -> None:
    monkeypatch.setenv("BUYER_DISCOVERY_MARKET_INTEL_SCENARIO", "low-rate")

    settings = Settings()

    assert settings.market_intel_scenario == "low-rate"
