from src.common.models import BuyerCandidate
from src.exporter.backend_payload import (
    build_ingestion_request,
    export_backend_payload,
    export_backend_payloads,
)
from src.pipeline.normalizer import normalize_candidates
from src.pipeline.scorer import score_candidate


def test_export_backend_payload_keeps_discovery_contract_focused() -> None:
    normalized = normalize_candidates(
        [
            BuyerCandidate(
                business_name="Sri Balaji Commission Agent",
                source_url="https://directory.example/listing/sri-balaji",
                town="Guntur",
                website="https://example.com/contact",
                contact_hints=("99887 77665",),
                source_key="business_directory",
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
    )[0]
    scored = score_candidate(normalized)

    payload = export_backend_payload(
        scored,
        crawl_run_ref="2024-07-01T10:00:00Z",
        state_by_town={"guntur": "Andhra Pradesh"},
        discovery_source="buyer-discovery-worker",
    )

    assert payload["buyerName"] == "Sri Balaji Commission Agent"
    assert payload["businessName"] == "Sri Balaji Commission Agent"
    assert payload["externalMatchKey"] == (
        "buyer-discovery-worker|guntur|sri-balaji-commission-agent|domain:example.com"
    )
    assert payload["primaryPhone"] == "+919988777665"
    assert payload["primaryEmail"] == "sales@example.com"
    assert payload["city"] == "Guntur"
    assert payload["stateProvince"] == "Andhra Pradesh"
    assert payload["discoverySource"] == "buyer-discovery-worker"
    assert payload["discoveryNotes"] == (
        "crawl_run_ref=2024-07-01T10:00:00Z; "
        "candidate_ref=buyer:guntur:sri-balaji-commission-agent:example-com; "
        "confidence_score=1.00; "
        "review_state=auto_approved; "
        "sources=website_enrichment,business_directory"
    )
    assert payload["evidence"] == [
        {
            "evidenceType": "WEBSITE",
            "sourceLabel": "website_enrichment",
            "details": (
                "confidence_class=first_party_website; "
                "website=https://example.com/; "
                "contact_hints=sales@example.com"
            ),
            "evidenceUrl": "https://example.com/",
            "capturedAt": "2024-07-01T10:00:00Z",
        },
        {
            "evidenceType": "DIRECTORY_LISTING",
            "sourceLabel": "business_directory",
            "details": (
                "confidence_class=third_party_listing; "
                "website=https://example.com/contact; "
                "contact_hints=+919988777665"
            ),
            "evidenceUrl": "https://directory.example/listing/sri-balaji",
            "capturedAt": "2024-07-01T10:00:00Z",
        },
    ]

    assert export_backend_payloads(
        [scored],
        crawl_run_ref="2024-07-01T10:00:00Z",
        state_by_town={"guntur": "Andhra Pradesh"},
        discovery_source="buyer-discovery-worker",
    ) == [payload]

    assert build_ingestion_request(
        [scored],
        crawl_run_ref="2024-07-01T10:00:00Z",
        state_by_town={"guntur": "Andhra Pradesh"},
        discovery_source="buyer-discovery-worker",
    ) == {"candidates": [payload]}


def test_export_backend_payload_truncates_buyer_name_to_backend_limit() -> None:
    normalized = normalize_candidates(
        [
            BuyerCandidate(
                business_name=(
                    "Very Long Buyer Lead Name " * 8
                ).strip(),
                source_url="https://example.com/very-long-buyer",
                town="Guntur",
                website="https://example.com",
                contact_hints=("9876543210",),
                source_key="website_enrichment",
            )
        ]
    )[0]
    scored = score_candidate(normalized)

    payload = export_backend_payload(
        scored,
        crawl_run_ref="2024-07-01T10:00:00Z",
        state_by_town={"guntur": "Andhra Pradesh"},
        discovery_source="buyer-discovery-worker",
    )

    assert len(payload["buyerName"]) == 120
    assert len(payload["businessName"]) == 120
    assert payload["externalMatchKey"] == (
        "buyer-discovery-worker|guntur|very-long-buyer-lead-name-very-long-buyer-lead-name-"
        "very-long-buyer-lead-name-very-long-buyer-lead-name-very-long-buyer-lead-name-very-"
        "long-buyer-lead-name-very-long-buyer-lead-name-very-long-buyer-lead-name|domain:example.com"
    )
    assert payload["buyerName"].endswith("...")
