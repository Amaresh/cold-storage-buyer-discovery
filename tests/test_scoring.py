from src.common.models import BuyerCandidate
from src.pipeline.normalizer import normalize_candidates
from src.pipeline.scorer import score_candidate


def test_score_candidate_auto_approves_strong_first_party_backed_candidate() -> None:
    candidate = normalize_candidates(
        [
            BuyerCandidate(
                business_name="Raghavendra Chilli Buyers",
                source_url="https://directory.example/listing/raghavendra",
                town="Guntur",
                website="https://raghavendra.example/contact",
                contact_hints=("90123 45678",),
                source_key="business_directory",
            ),
            BuyerCandidate(
                business_name="Raghavendra Chilli Buyers",
                source_url="https://raghavendra.example",
                town="Guntur",
                website="https://raghavendra.example",
                contact_hints=("90123 45678", "sales@raghavendra.example"),
                source_key="website_enrichment",
            ),
        ]
    )[0]

    scored = score_candidate(candidate)

    assert scored.review_state == "auto_approved"
    assert scored.confidence_score >= 0.8
    assert "max_source_confidence:3" in scored.score_reasons
    assert "source_corroboration:2" in scored.score_reasons


def test_score_candidate_leaves_seed_only_candidate_for_review() -> None:
    candidate = normalize_candidates(
        [
            BuyerCandidate(
                business_name="Annapurna Chilli Buyers",
                source_url="https://annapurna.example/buyers",
                town="Khammam",
                website="https://annapurna.example/buyers",
                contact_hints=(),
                source_key="google_search_seed",
            )
        ]
    )[0]

    scored = score_candidate(candidate)

    assert scored.review_state == "needs_review"
    assert scored.confidence_score == 0.4
    assert scored.auto_approved is False
