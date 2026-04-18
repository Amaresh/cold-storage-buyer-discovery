from __future__ import annotations

from pathlib import Path

from src.common.models import SourceConfig
from src.crawler.adapters._utils import extract_contact_hints
from src.crawler.adapters.business_directory import BusinessDirectoryAdapter
from src.crawler.adapters.google_search_seed import GoogleSearchSeedAdapter
from src.crawler.adapters.website_enrichment import WebsiteEnrichmentAdapter
from src.crawler.source_policy import (
    get_source_policy,
    persistable_candidate_fields,
    resolve_source_policy,
)

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
TOWN_HINTS = ("Guntur", "Khammam", "Warangal")


def _read_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def test_google_search_seed_adapter_extracts_seed_candidates() -> None:
    adapter = GoogleSearchSeedAdapter(town_hints=TOWN_HINTS)

    candidates = adapter.extract_candidates(_read_fixture("google_seed_result.html"))

    assert [candidate.business_name for candidate in candidates] == [
        "Sri Lakshmi Gunj Traders",
        "Annapurna Chilli Buyers",
    ]
    assert [candidate.town for candidate in candidates] == ["Guntur", "Khammam"]
    assert candidates[0].source_url == "https://srilakshmi-gunj.example/"
    assert candidates[0].website == "https://srilakshmi-gunj.example/"
    assert candidates[0].contact_hints == ("+91 98765 43210",)


def test_business_directory_adapter_extracts_listing_candidates() -> None:
    adapter = BusinessDirectoryAdapter(town_hints=TOWN_HINTS)

    candidates = adapter.extract_candidates(
        _read_fixture("directory_listing.html"),
        directory_page_url="https://directory.example/chilli-buyers",
    )

    assert [candidate.business_name for candidate in candidates] == [
        "Sri Balaji Commission Agent",
        "Warangal Red Chilli Buyers",
    ]
    assert [candidate.town for candidate in candidates] == ["Guntur", "Warangal"]
    assert candidates[0].source_url == "https://directory.example/listing/sri-balaji-commission-agent"
    assert candidates[0].website == "https://balaji-commission.example/"
    assert candidates[1].contact_hints == ("99887 77665",)


def test_website_enrichment_adapter_extracts_first_party_signals() -> None:
    adapter = WebsiteEnrichmentAdapter(town_hints=TOWN_HINTS)
    html = """
    <html>
      <head>
        <title>Raghavendra Chilli Buyers | Guntur</title>
      </head>
      <body>
        <h1>Raghavendra Chilli Buyers</h1>
        <p>Visit our yard at Guntur main road for chilli auction support.</p>
        <p>Call +91 90123 45678 or email sales@raghavendra.example.</p>
      </body>
    </html>
    """

    candidates = adapter.extract_candidates(html, website_url="https://raghavendra.example/")

    assert len(candidates) == 1
    assert candidates[0].business_name == "Raghavendra Chilli Buyers"
    assert candidates[0].source_url == "https://raghavendra.example/"
    assert candidates[0].town == "Guntur"
    assert candidates[0].contact_hints == (
        "+91 90123 45678",
        "sales@raghavendra.example",
    )


def test_extract_contact_hints_ignores_dates_and_decimal_numbers() -> None:
    hints = extract_contact_hints(
        "Published 2023-01-15",
        "Reference 15-08-1947",
        "Score 3.14159265",
        "Local IP 192.168.100.200",
        "Call +91 90123 45678",
    )

    assert hints == ("+91 90123 45678",)


def test_extract_contact_hints_does_not_treat_email_digits_as_phone_number() -> None:
    hints = extract_contact_hints(
        "Reach us at info9876543210@example.com for auction support."
    )

    assert hints == ("info9876543210@example.com",)


def test_extract_contact_hints_does_not_treat_digit_domain_as_phone_number() -> None:
    hints = extract_contact_hints(
        "Email us at sales@9876543210.com for auction support."
    )

    assert hints == ("sales@9876543210.com",)


def test_extract_contact_hints_does_not_treat_pure_digit_email_local_part_as_phone_number() -> None:
    hints = extract_contact_hints(
        "Reach us at 9876543210@example.com for auction support."
    )

    assert hints == ("9876543210@example.com",)


def test_source_policy_keeps_google_results_seed_only() -> None:
    candidate = GoogleSearchSeedAdapter(town_hints=TOWN_HINTS).extract_candidates(
        _read_fixture("google_seed_result.html")
    )[0]
    policy = get_source_policy("google_search_seed")

    assert policy.fetch_method == "search_result_snapshot"
    assert policy.persistence_scope == "seed_only"
    assert policy.confidence_class == "discovery_seed"
    assert policy.confidence_level == 1
    assert persistable_candidate_fields(candidate, policy) == {
        "business_name": "Sri Lakshmi Gunj Traders",
        "source_url": "https://srilakshmi-gunj.example/",
        "town": "Guntur",
        "website": "https://srilakshmi-gunj.example/",
        "contact_hints": ("+91 98765 43210",),
    }


def test_resolve_source_policy_marks_disabled_sources() -> None:
    policy = resolve_source_policy(
        SourceConfig(
            name="google_search_seed",
            enabled=False,
            adapter_key="google_search_seed",
        )
    )

    assert not policy.is_enabled
    assert policy.disabled_reason == "Disabled in source configuration"
