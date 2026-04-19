from __future__ import annotations

from pathlib import Path

from src.common.models import SourceConfig
from src.crawler.adapters._utils import extract_contact_hints, strip_tags
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


def test_google_search_seed_adapter_skips_obviously_irrelevant_results() -> None:
    adapter = GoogleSearchSeedAdapter(town_hints=TOWN_HINTS)
    html = """
    <html>
      <body>
        <a href="/url?q=https://topgun.example/&sa=U">Top Gun Dealers in Guntur</a>
        <a href="/url?q=https://srilakshmi.example/&sa=U">Sri Lakshmi Spices - Guntur</a>
      </body>
    </html>
    """

    candidates = adapter.extract_candidates(
        html,
        search_page_url="https://www.google.com/search?q=Guntur+gunj+shop",
        fallback_town="Guntur",
    )

    assert [candidate.business_name for candidate in candidates] == ["Sri Lakshmi Spices"]


def test_google_search_seed_adapter_extracts_candidates_from_search_rss() -> None:
    adapter = GoogleSearchSeedAdapter(town_hints=TOWN_HINTS)
    rss = """
    <?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Bing: Guntur chilli wholesaler</title>
        <item>
          <title>Sri Lakshmi Spices - Guntur</title>
          <link>https://srilakshmi.example/</link>
          <description>Guntur chilli wholesalers and commission agents.</description>
        </item>
        <item>
          <title>Guntur - Wikipedia</title>
          <link>https://en.wikipedia.org/wiki/Guntur</link>
          <description>General district information.</description>
        </item>
      </channel>
    </rss>
    """

    candidates = adapter.extract_candidates(
        rss,
        search_page_url="https://www.bing.com/search?format=rss&q=Guntur+chilli+wholesaler",
        fallback_town="Guntur",
    )

    assert [candidate.business_name for candidate in candidates] == ["Sri Lakshmi Spices"]
    assert candidates[0].source_url == "https://srilakshmi.example/"
    assert candidates[0].town == "Guntur"


def test_google_search_seed_adapter_skips_excluded_domains() -> None:
    adapter = GoogleSearchSeedAdapter(town_hints=TOWN_HINTS)
    html = """
    <html>
      <body>
        <a href="https://www.youtube.com/watch?v=1">YouTube Guntur Mirchi Market</a>
        <a href="https://www.justdial.com/Guntur/Commission-Agents-For-Dry-Red-Chilli/nct-12159430">
          Top Commission Agents For Dry Red Chilli in Guntur near me - Justdial
        </a>
      </body>
    </html>
    """

    candidates = adapter.extract_candidates(
        html,
        search_page_url="https://search.brave.com/search?q=Guntur+chilli+commission+agent",
        fallback_town="Guntur",
    )

    assert candidates == []


def test_google_search_seed_adapter_strips_style_blocks_from_startpage_titles() -> None:
    adapter = GoogleSearchSeedAdapter(town_hints=TOWN_HINTS)
    html = """
    <html>
      <body>
        <a class="result-title result-link" href="https://www.justdial.com/Guntur/Commission-Agents-For-Dry-Red-Chilli/nct-12159430">
          <style data-emotion="css i3irj7">
            .css-i3irj7{line-height:18px}
          </style>
          <h2 class="wgl-title css-i3irj7">Popular Commission Agents For Dry Red Chilli in Guntur - Justdial</h2>
        </a>
      </body>
    </html>
    """

    candidates = adapter.extract_candidates(
        html,
        search_page_url="https://www.startpage.com/do/dsearch?query=Guntur+chilli+commission+agent",
        fallback_town="Guntur",
    )

    assert candidates == []


def test_google_search_seed_adapter_resolves_yahoo_redirect_titles() -> None:
    adapter = GoogleSearchSeedAdapter(town_hints=TOWN_HINTS)
    html = """
    <html>
      <body>
        <a href="https://r.search.yahoo.com/_ylt=foo/RV=2/RE=1777762082/RO=10/RU=http%3a%2f%2ffarmicoexports.com%2f/RK=2/RS=bar">
          farmicoexports.com http://farmicoexports.com Farmico Exports
        </a>
      </body>
    </html>
    """

    candidates = adapter.extract_candidates(
        html,
        search_page_url="https://search.yahoo.com/search?p=Guntur+chilli+commission+agent",
        fallback_town="Guntur",
    )

    assert [candidate.business_name for candidate in candidates] == ["Farmico Exports"]
    assert candidates[0].source_url == "http://farmicoexports.com/"
    assert candidates[0].town == "Guntur"


def test_google_search_seed_adapter_decodes_commission_agent_slug_titles() -> None:
    adapter = GoogleSearchSeedAdapter(town_hints=TOWN_HINTS)
    html = """
    <html>
      <body>
        <a href="https://www.indiamart.com/sri-lakshmi-narasimha-commission-agents/">
          sri-lakshmi-narasimha-commission-agents
        </a>
      </body>
    </html>
    """

    candidates = adapter.extract_candidates(
        html,
        search_page_url="https://search.yahoo.com/search?p=Guntur+chilli+commission+agent",
        fallback_town="Guntur",
    )

    assert [candidate.business_name for candidate in candidates] == [
        "Sri Lakshmi Narasimha Commission Agents"
    ]
    assert candidates[0].contact_hints == ()


def test_google_search_seed_adapter_drops_urlish_anchor_text() -> None:
    adapter = GoogleSearchSeedAdapter(town_hints=TOWN_HINTS)
    html = """
    <html>
      <body>
        <a href="https://www.spicesindia.net/us-and-chilli">
          https://www.spicesindia.net/us-and-chilli https://www.spicesindia.net
        </a>
      </body>
    </html>
    """

    candidates = adapter.extract_candidates(
        html,
        search_page_url="https://search.yahoo.com/search?p=Guntur+chilli+wholesaler",
        fallback_town="Guntur",
    )

    assert candidates == []


def test_google_search_seed_adapter_drops_conflicting_location_result_when_only_fallback_town_matches() -> None:
    adapter = GoogleSearchSeedAdapter(town_hints=TOWN_HINTS)
    html = """
    <html>
      <body>
        <a href="https://karnavatispices.example/dry-chilli.html">
          Wholesaler Distributor from Ahmedabad
        </a>
      </body>
    </html>
    """

    candidates = adapter.extract_candidates(
        html,
        search_page_url="https://search.yahoo.com/search?p=Guntur+chilli+wholesaler",
        fallback_town="Guntur",
    )

    assert candidates == []


def test_strip_tags_removes_style_and_script_contents() -> None:
    assert strip_tags(
        "<style>.css{color:red}</style><h2>Sri Lakshmi Traders</h2><script>alert(1)</script>"
    ) == "Sri Lakshmi Traders"


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
    assert candidates[0].contact_hints == ("0863 222 3344",)
    assert candidates[1].contact_hints == ("99887 77665",)


def test_business_directory_adapter_rejects_generic_category_titles() -> None:
    adapter = BusinessDirectoryAdapter(town_hints=TOWN_HINTS)
    html = """
    <html>
      <body>
        <article class="directory-card">
          <a class="listing-link" href="/listing/popular-wholesalers">
            Popular Chilli Powder Wholesalers in Dindi, Guntur
          </a>
          <p class="listing-location">Dindi, Guntur</p>
          <p class="listing-contact">Call +91 90352 76564</p>
        </article>
      </body>
    </html>
    """

    candidates = adapter.extract_candidates(
        html,
        directory_page_url="https://directory.example/chilli-buyers",
    )

    assert candidates == []


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


def test_website_enrichment_adapter_prefers_non_page_title_over_home_heading() -> None:
    adapter = WebsiteEnrichmentAdapter(town_hints=TOWN_HINTS)
    html = """
    <html>
      <head>
        <title>Pramoda Exim: Dry Red Chilli wholesalers, Suppliers & Exporters</title>
      </head>
      <body>
        <h1>Home</h1>
        <p>Guntur-based chilli exporter.</p>
        <p>Call +91 95815 81333 or email pramoda.exim@gmail.com.</p>
      </body>
    </html>
    """

    [candidate] = adapter.extract_candidates(html, website_url="https://pramoda.example/")

    assert candidate.business_name == "Pramoda Exim"
    assert candidate.town == "Guntur"
    assert candidate.contact_hints == (
        "+91 95815 81333",
        "pramoda.exim@gmail.com",
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


def test_extract_contact_hints_rejects_long_product_ids() -> None:
    hints = extract_contact_hints(
        "SKU 2855174005962, call +91 90123 45678 for chilli pricing."
    )

    assert hints == ("+91 90123 45678",)


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
