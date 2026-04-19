from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote_plus, urlsplit

from config.settings import Settings
from src.runtime.pipeline import SampleDiscoveryPipeline


class _LiveDiscoveryHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        self.server.paths.append(self.path)  # type: ignore[attr-defined]
        parsed = urlsplit(self.path)
        query = parse_qs(parsed.query).get("q", [""])[0]

        if parsed.path == "/google":
            encoded_site = quote_plus(
                f"http://127.0.0.1:{self.server.server_address[1]}/site/sri-lakshmi"  # type: ignore[attr-defined]
            )
            html = f"""
            <html>
              <body>
                <a href="/url?q={encoded_site}&sa=U">
                  Sri Lakshmi Gunj Traders - {query}
                </a>
              </body>
            </html>
            """
        elif parsed.path == "/site/sri-lakshmi":
            html = """
            <html>
              <head>
                <title>Sri Lakshmi Gunj Traders | Guntur</title>
              </head>
              <body>
                <h1>Sri Lakshmi Gunj Traders</h1>
                <p>Wholesale chilli buyers in Guntur market yard.</p>
                <p>Call +91 98765 43210 or email sales@srilakshmi.example.</p>
              </body>
            </html>
            """
        else:
            self.send_response(404)
            self.end_headers()
            return

        payload = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


def test_live_pipeline_fetches_real_html_when_sample_mode_is_disabled(monkeypatch) -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _LiveDiscoveryHandler)
    server.paths = []  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        monkeypatch.setenv("BUYER_DISCOVERY_USE_SAMPLE_SNAPSHOTS", "false")
        monkeypatch.setenv("BUYER_DISCOVERY_WAREHOUSE_HUB_TOWN", "Guntur")
        monkeypatch.setenv("BUYER_DISCOVERY_MAX_RADIUS_KM", "300")
        monkeypatch.setenv(
            "BUYER_DISCOVERY_GOOGLE_SEARCH_URL_TEMPLATE",
            f"http://127.0.0.1:{server.server_address[1]}/google?q={{query}}",
        )
        monkeypatch.setenv("BUYER_DISCOVERY_SEARCH_FALLBACK_URL_TEMPLATE", "")
        monkeypatch.setenv("BUYER_DISCOVERY_SEARCH_FALLBACK_URL_TEMPLATES", "")
        monkeypatch.setenv("BUYER_DISCOVERY_BUSINESS_DIRECTORY_URL_TEMPLATE", "")

        settings = Settings()
        pipeline = SampleDiscoveryPipeline(settings)

        result = pipeline.run(settings.resolve_towns(["Guntur"]), ("chilli wholesaler",))

        assert result.processed_jobs == 1
        assert result.skipped_jobs == 1
        assert result.raw_candidate_count == 1
        assert result.enriched_candidate_count == 1
        assert result.normalized_candidate_count == 1
        assert result.sanitized_candidate_count == 1
        assert result.auto_approved_candidate_count == 1
        assert [candidate.candidate.business_name for candidate in result.scored_candidates] == [
            "Sri Lakshmi Gunj Traders"
        ]
        assert [candidate.candidate.business_name for candidate in result.eligible_candidates] == [
            "Sri Lakshmi Gunj Traders"
        ]
        assert result.ingestion_request["candidates"] == [
            {
                "externalMatchKey": "buyer-discovery-worker|guntur|sri-lakshmi-gunj-traders|domain:127.0.0.1",
                "buyerName": "Sri Lakshmi Gunj Traders",
                "businessName": "Sri Lakshmi Gunj Traders",
                "primaryPhone": "+919876543210",
                "primaryEmail": "sales@srilakshmi.example",
                "city": "Guntur",
                "stateProvince": "Andhra Pradesh",
                "discoverySource": "buyer-discovery-worker",
                "discoveryNotes": result.ingestion_request["candidates"][0]["discoveryNotes"],
                "evidence": result.ingestion_request["candidates"][0]["evidence"],
            }
        ]
        assert any(path.startswith("/google?q=") for path in server.paths)
        assert "/site/sri-lakshmi" in server.paths
    finally:
        server.shutdown()
        server.server_close()


def test_live_pipeline_keeps_seed_candidate_with_real_domain_for_manual_review(monkeypatch) -> None:
    class _SeedOnlyHandler(_LiveDiscoveryHandler):
        def do_GET(self) -> None:  # noqa: N802
            self.server.paths.append(self.path)  # type: ignore[attr-defined]
            parsed = urlsplit(self.path)
            if parsed.path == "/google":
                html = """
                <html>
                  <body>
                    <a class="result-link" href="https://raj-sri.example/">Raj Sri Spices and Company</a>
                  </body>
                </html>
                """
                payload = html.encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            self.send_response(404)
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), _SeedOnlyHandler)
    server.paths = []  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        monkeypatch.setenv("BUYER_DISCOVERY_USE_SAMPLE_SNAPSHOTS", "false")
        monkeypatch.setenv("BUYER_DISCOVERY_WAREHOUSE_HUB_TOWN", "Guntur")
        monkeypatch.setenv("BUYER_DISCOVERY_MAX_RADIUS_KM", "300")
        monkeypatch.setenv(
            "BUYER_DISCOVERY_GOOGLE_SEARCH_URL_TEMPLATE",
            f"http://127.0.0.1:{server.server_address[1]}/google?q={{query}}",
        )
        monkeypatch.setenv("BUYER_DISCOVERY_SEARCH_FALLBACK_URL_TEMPLATE", "")
        monkeypatch.setenv("BUYER_DISCOVERY_SEARCH_FALLBACK_URL_TEMPLATES", "")
        monkeypatch.setenv("BUYER_DISCOVERY_BUSINESS_DIRECTORY_URL_TEMPLATE", "")

        settings = Settings()
        pipeline = SampleDiscoveryPipeline(settings)

        result = pipeline.run(settings.resolve_towns(["Guntur"]), ("chilli wholesaler",))

        assert result.raw_candidate_count == 1
        assert result.enriched_candidate_count == 0
        assert result.normalized_candidate_count == 1
        assert result.sanitized_candidate_count == 1
        assert [candidate.candidate.business_name for candidate in result.eligible_candidates] == [
            "Raj Sri Spices and Company"
        ]
        assert result.ingestion_request["candidates"] == [
            {
                "externalMatchKey": "buyer-discovery-worker|guntur|raj-sri-spices-and-company|domain:raj-sri.example",
                "buyerName": "Raj Sri Spices and Company",
                "businessName": "Raj Sri Spices and Company",
                "primaryPhone": "",
                "primaryEmail": "",
                "city": "Guntur",
                "stateProvince": "Andhra Pradesh",
                "discoverySource": "buyer-discovery-worker",
                "discoveryNotes": result.ingestion_request["candidates"][0]["discoveryNotes"],
                "evidence": result.ingestion_request["candidates"][0]["evidence"],
            }
        ]
    finally:
        server.shutdown()
        server.server_close()
