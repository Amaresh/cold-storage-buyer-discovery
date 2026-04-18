from src.common.models import BuyerCandidate
from src.pipeline.normalizer import (
    normalize_candidates,
    normalize_domain,
    normalize_phone,
    normalize_url,
)


def test_normalize_phone_url_and_domain_helpers() -> None:
    assert normalize_phone("+91 98765 43210") == "+919876543210"
    assert normalize_phone("09876543210") == "+919876543210"
    assert normalize_url("HTTPS://WWW.Example.com/Buyers/?utm_source=ads#section") == (
        "https://example.com/Buyers"
    )
    assert normalize_url("http://example.com:abc/path") == ""
    assert normalize_domain("WWW.Example.com") == "example.com"


def test_normalize_candidates_collapses_cross_source_duplicates_deterministically() -> None:
    candidates = [
        BuyerCandidate(
            business_name="Sri Balaji Commission Agent",
            source_url="https://directory.example/listing/sri-balaji",
            town="Guntur",
            website="https://www.example.com/contact/",
            contact_hints=("99887 77665", "sales@example.com"),
            source_key="business_directory",
        ),
        BuyerCandidate(
            business_name="Sri Balaji Commission Agent",
            source_url="https://www.example.com/about?utm_source=seed",
            town="Guntur",
            website="https://example.com/",
            contact_hints=("+91 99887 77665",),
            source_key="google_search_seed",
        ),
        BuyerCandidate(
            business_name="Sri Balaji Commission Agent",
            source_url="https://example.com",
            town="Guntur",
            website="https://example.com",
            contact_hints=("sales@example.com",),
            source_key="website_enrichment",
        ),
    ]

    normalized = normalize_candidates(candidates)
    reversed_normalized = normalize_candidates(reversed(candidates))

    assert len(normalized) == 1
    assert normalized == reversed_normalized
    candidate = normalized[0]
    assert candidate.candidate_ref == (
        "buyer:guntur:sri-balaji-commission-agent:example-com"
    )
    assert candidate.website == "https://example.com/"
    assert candidate.domain == "example.com"
    assert candidate.dedupe_fields == (
        "domain:example.com",
        "phone:+919988777665",
        "email:sales@example.com",
    )
    assert candidate.phones == ("+919988777665",)
    assert candidate.emails == ("sales@example.com",)
    assert candidate.source_keys == (
        "website_enrichment",
        "business_directory",
        "google_search_seed",
    )
    assert [evidence.source_key for evidence in candidate.evidence] == [
        "website_enrichment",
        "business_directory",
        "google_search_seed",
    ]


def test_normalize_candidates_keeps_conflicting_hotline_matches_separate() -> None:
    normalized = normalize_candidates(
        [
            BuyerCandidate(
                business_name="Alpha Traders",
                source_url="https://alpha.example",
                town="Guntur",
                website="https://alpha.example",
                contact_hints=("90123 45678",),
                source_key="website_enrichment",
            ),
            BuyerCandidate(
                business_name="Beta Exports",
                source_url="https://beta.example",
                town="Khammam",
                website="https://beta.example",
                contact_hints=("90123 45678",),
                source_key="website_enrichment",
            ),
        ]
    )

    assert len(normalized) == 2
    assert [candidate.business_name for candidate in normalized] == [
        "Alpha Traders",
        "Beta Exports",
    ]


def test_normalize_candidates_keeps_same_domain_different_towns_separate() -> None:
    normalized = normalize_candidates(
        [
            BuyerCandidate(
                business_name="ACME Buyers",
                source_url="https://acme.example/guntur",
                town="Guntur",
                website="https://acme.example",
                contact_hints=("90123 45678",),
                source_key="website_enrichment",
            ),
            BuyerCandidate(
                business_name="ACME Buyers",
                source_url="https://acme.example/khammam",
                town="Khammam",
                website="https://acme.example",
                contact_hints=("90123 45678",),
                source_key="website_enrichment",
            ),
        ]
    )

    assert len(normalized) == 2
    assert [candidate.town for candidate in normalized] == ["Guntur", "Khammam"]


def test_normalize_candidates_keeps_same_domain_different_businesses_separate() -> None:
    normalized = normalize_candidates(
        [
            BuyerCandidate(
                business_name="Alpha Agro Exports",
                source_url="https://multibiz.example/alpha",
                town="Guntur",
                website="https://multibiz.example/alpha",
                contact_hints=("90123 45678",),
                source_key="website_enrichment",
            ),
            BuyerCandidate(
                business_name="Beta Chilli Buyers",
                source_url="https://multibiz.example/beta",
                town="Guntur",
                website="https://multibiz.example/beta",
                contact_hints=("90123 45678",),
                source_key="website_enrichment",
            ),
        ]
    )

    assert len(normalized) == 2
    assert [candidate.business_name for candidate in normalized] == [
        "Alpha Agro Exports",
        "Beta Chilli Buyers",
    ]


def test_normalize_candidates_avoids_bridge_merge_across_incompatible_domain_groups() -> None:
    normalized = normalize_candidates(
        [
            BuyerCandidate(
                business_name="Alpha Traders",
                source_url="https://alpha.example/about",
                town="Guntur",
                website="https://alpha.example",
                contact_hints=("90123 45678",),
                source_key="website_enrichment",
            ),
            BuyerCandidate(
                business_name="Alpha Traders",
                source_url="https://directory.example/alpha-traders",
                town="Guntur",
                website="",
                contact_hints=("90123 45678",),
                source_key="business_directory",
            ),
            BuyerCandidate(
                business_name="Beta Exports",
                source_url="https://beta.example/contact",
                town="Guntur",
                website="https://beta.example",
                contact_hints=("90123 45678",),
                source_key="website_enrichment",
            ),
        ]
    )

    assert len(normalized) == 2
    assert [candidate.business_name for candidate in normalized] == [
        "Alpha Traders",
        "Beta Exports",
    ]


def test_normalize_candidates_does_not_promote_seed_source_url_to_canonical_website() -> None:
    normalized = normalize_candidates(
        [
            BuyerCandidate(
                business_name="Alpha Traders",
                source_url="https://facebook.com/alpha-traders",
                town="Guntur",
                website="",
                contact_hints=(),
                source_key="google_search_seed",
            ),
            BuyerCandidate(
                business_name="Beta Exports",
                source_url="https://facebook.com/beta-exports",
                town="Guntur",
                website="",
                contact_hints=(),
                source_key="google_search_seed",
            ),
        ]
    )

    assert len(normalized) == 2
    assert all(candidate.website == "" for candidate in normalized)
    assert all(candidate.domain == "" for candidate in normalized)


def test_normalize_candidates_keeps_shared_host_pages_separate() -> None:
    normalized = normalize_candidates(
        [
            BuyerCandidate(
                business_name="Alpha Traders",
                source_url="https://facebook.com/alpha-traders",
                town="Guntur",
                website="https://facebook.com/alpha-traders",
                contact_hints=(),
                source_key="website_enrichment",
            ),
            BuyerCandidate(
                business_name="Beta Exports",
                source_url="https://facebook.com/beta-exports",
                town="Guntur",
                website="https://facebook.com/beta-exports",
                contact_hints=(),
                source_key="website_enrichment",
            ),
        ]
    )

    assert len(normalized) == 2
    assert [candidate.website for candidate in normalized] == [
        "https://facebook.com/alpha-traders",
        "https://facebook.com/beta-exports",
    ]
    assert all(candidate.domain == "" for candidate in normalized)
