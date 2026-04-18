from src.common.models import BuyerCandidate
from src.exporter.backend_payload import export_backend_payload, export_backend_payloads
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
        tenant_id="tenant-001",
        warehouse_id="warehouse-guntur-01",
        crawl_run_ref="crawl-run-2024-07-01T10:00:00Z",
    )

    assert payload["schema_version"] == 1
    assert payload["tenant_id"] == "tenant-001"
    assert payload["warehouse_id"] == "warehouse-guntur-01"
    assert payload["crawl_run_ref"] == "crawl-run-2024-07-01T10:00:00Z"
    assert payload["candidate_ref"] == "buyer:guntur:sri-balaji-commission-agent:example-com"
    assert payload["confidence_score"] == scored.confidence_score
    assert payload["score_reasons"] == list(scored.score_reasons)
    assert payload["review_state"] == "auto_approved"
    assert payload["dedupe_fields"] == [
        "domain:example.com",
        "phone:+919988777665",
        "email:sales@example.com",
    ]
    assert payload["business"] == {
        "name": "Sri Balaji Commission Agent",
        "town": "Guntur",
        "website": "https://example.com/",
        "domain": "example.com",
    }
    assert payload["contact"] == {
        "phones": ["+919988777665"],
        "emails": ["sales@example.com"],
        "other_hints": [],
    }
    assert payload["evidence"] == [
        {
            "source_key": "website_enrichment",
            "source_url": "https://example.com/",
            "website": "https://example.com/",
            "contact_hints": ["sales@example.com"],
            "source_confidence_class": "first_party_website",
            "source_confidence_level": 3,
        },
        {
            "source_key": "business_directory",
            "source_url": "https://directory.example/listing/sri-balaji",
            "website": "https://example.com/contact",
            "contact_hints": ["+919988777665"],
            "source_confidence_class": "third_party_listing",
            "source_confidence_level": 2,
        },
    ]
    assert "depositor" not in payload
    assert "lot" not in payload

    assert export_backend_payloads(
        [scored],
        tenant_id="tenant-001",
        warehouse_id="warehouse-guntur-01",
        crawl_run_ref="crawl-run-2024-07-01T10:00:00Z",
    ) == [payload]
