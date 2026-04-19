from src.common.models import BuyerCandidate
from src.pipeline.normalizer import normalize_candidates
from src.pipeline.sanitizer import candidate_sanitization_issues, sanitize_scored_candidates
from src.pipeline.scorer import score_candidate


def _score_candidates(*candidates: BuyerCandidate):
    normalized = normalize_candidates(candidates)
    return [score_candidate(candidate) for candidate in normalized]


def test_sanitize_scored_candidates_drops_seed_only_candidates() -> None:
    [candidate] = _score_candidates(
        BuyerCandidate(
            business_name="Raj Sri Spices and Company",
            source_url="https://facebook.com/raj-sri-spices-and-company",
            town="Guntur",
            website="https://facebook.com/raj-sri-spices-and-company",
            contact_hints=(),
            source_key="google_search_seed",
        )
    )

    assert candidate_sanitization_issues(candidate) == ("seed_only", "no_contact_or_domain")
    assert sanitize_scored_candidates([candidate]) == []


def test_sanitize_scored_candidates_keeps_directory_candidate_with_contact_signal() -> None:
    [candidate] = _score_candidates(
        BuyerCandidate(
            business_name="Sri Balaji Commission Agent",
            source_url="https://directory.example/listing/sri-balaji",
            town="Guntur",
            website="https://balaji-commission.example",
            contact_hints=("+91 99887 77665",),
            source_key="business_directory",
        )
    )

    assert candidate_sanitization_issues(candidate) == ()
    assert sanitize_scored_candidates([candidate]) == [candidate]


def test_sanitize_scored_candidates_keeps_seed_candidate_with_direct_contact_signal() -> None:
    [candidate] = _score_candidates(
        BuyerCandidate(
            business_name="Sri Sai Durga Chillies Exports Guntur",
            source_url="https://srisaidurgachillies.example",
            town="Guntur",
            website="https://srisaidurgachillies.example",
            contact_hints=("+91 95829 62882", "srisaidurgachilliesexports@gmail.com"),
            source_key="google_search_seed",
        )
    )

    assert candidate_sanitization_issues(candidate) == ()
    assert sanitize_scored_candidates([candidate]) == [candidate]


def test_sanitize_scored_candidates_keeps_seed_candidate_with_real_domain_for_review() -> None:
    [candidate] = _score_candidates(
        BuyerCandidate(
            business_name="Raj Sri Spices and Company",
            source_url="https://rajsrispices.example",
            town="Guntur",
            website="https://rajsrispices.example",
            contact_hints=(),
            source_key="google_search_seed",
        )
    )

    assert candidate_sanitization_issues(candidate) == ()
    assert sanitize_scored_candidates([candidate]) == [candidate]


def test_sanitize_scored_candidates_drops_generic_directory_titles() -> None:
    [candidate] = _score_candidates(
        BuyerCandidate(
            business_name="Popular Chilli Powder Wholesalers in Dindi, Guntur",
            source_url="https://directory.example/listing/popular-wholesalers",
            town="Guntur",
            website="",
            contact_hints=("+91 90352 76564",),
            source_key="business_directory",
        )
    )

    assert "seo_phrase" in candidate_sanitization_issues(candidate)
    assert sanitize_scored_candidates([candidate]) == []


def test_sanitize_scored_candidates_drops_page_and_product_titles() -> None:
    candidates = _score_candidates(
        BuyerCandidate(
            business_name="Home",
            source_url="https://pramoda.example/",
            town="Guntur",
            website="https://pramoda.example/",
            contact_hints=("sales@pramoda.example",),
            source_key="website_enrichment",
        ),
        BuyerCandidate(
            business_name="AV - Guntur Chilli Whole (Without Stem), 1 Kg",
            source_url="https://hyperpure.example/av-guntur-chilli",
            town="Guntur",
            website="https://hyperpure.example/av-guntur-chilli",
            contact_hints=("support@hyperpure.example",),
            source_key="website_enrichment",
        )
    )

    home_candidate = next(
        candidate for candidate in candidates if candidate.candidate.business_name == "Home"
    )
    product_candidate = next(
        candidate
        for candidate in candidates
        if candidate.candidate.business_name == "AV - Guntur Chilli Whole (Without Stem), 1 Kg"
    )

    assert "page_label" in candidate_sanitization_issues(home_candidate)
    assert "product_page" in candidate_sanitization_issues(product_candidate)
    assert sanitize_scored_candidates(candidates) == []


def test_sanitize_scored_candidates_keeps_corroborated_first_party_candidate_with_generic_name() -> None:
    [candidate] = _score_candidates(
        BuyerCandidate(
            business_name="Guntur Mirchi",
            source_url="https://gunturmirchi.example",
            town="Guntur",
            website="https://gunturmirchi.example",
            contact_hints=(),
            source_key="google_search_seed",
        ),
        BuyerCandidate(
            business_name="Guntur Red Chillies",
            source_url="https://gunturmirchi.example",
            town="Guntur",
            website="https://gunturmirchi.example",
            contact_hints=("+91 93905 72590", "corporate@gunturmirchi.example"),
            source_key="website_enrichment",
        ),
    )

    assert candidate.candidate.business_name == "Guntur Red Chillies"
    assert candidate_sanitization_issues(candidate) == ()
    assert sanitize_scored_candidates([candidate]) == [candidate]


def test_sanitize_scored_candidates_drops_non_entity_prefix_even_with_contact() -> None:
    [candidate] = _score_candidates(
        BuyerCandidate(
            business_name="Us and the Chilli",
            source_url="https://spicesindia.example/us-and-the-chilli",
            town="Guntur",
            website="https://spicesindia.example/us-and-the-chilli",
            contact_hints=("+91 97013 63424", "samsu@agrocrops.com"),
            source_key="website_enrichment",
        )
    )

    assert "non_entity_prefix" in candidate_sanitization_issues(candidate)
    assert sanitize_scored_candidates([candidate]) == []
