from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from src.integration.backend_client import BackendIngestionClient


class _RequestCaptureHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers["Content-Length"])
        self.server.captured_request = {  # type: ignore[attr-defined]
            "path": self.path,
            "headers": dict(self.headers),
            "body": json.loads(self.rfile.read(length).decode()),
        }
        response_payload = (
            {
                "processedCount": 1,
                "candidates": [{"candidateId": 7, "action": "CREATED"}],
            }
            if self.path == "/api/v1/internal/trading/buyer-leads/ingestion"
            else {"processedCount": 1}
        )
        response_body = json.dumps(response_payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


def test_backend_client_posts_ingestion_request_with_internal_headers() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _RequestCaptureHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = BackendIngestionClient(
            base_url=f"http://127.0.0.1:{server.server_address[1]}",
            tenant_id="demo-tenant",
            warehouse_id="guntur-hub",
            api_header="X-API-Key",
            api_key="secret-token",
        )

        response = client.ingest(
            {
                "candidates": [
                    {
                        "buyerName": "Sri Lakshmi Gunj Traders",
                        "discoverySource": "buyer-discovery-worker",
                    }
                ]
            }
        )

        assert response["processedCount"] == 1
        captured = server.captured_request  # type: ignore[attr-defined]
        headers = {key.casefold(): value for key, value in captured["headers"].items()}
        assert captured["path"] == "/api/v1/internal/trading/buyer-leads/ingestion"
        assert headers["x-api-key"] == "secret-token"
        assert headers["x-tenant-id"] == "demo-tenant"
        assert headers["x-warehouse-id"] == "guntur-hub"
        assert captured["body"]["candidates"][0]["buyerName"] == "Sri Lakshmi Gunj Traders"
    finally:
        server.shutdown()
        server.server_close()


def test_backend_client_tolerates_non_json_success_body() -> None:
    class PlainTextHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            response = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), PlainTextHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = BackendIngestionClient(
            base_url=f"http://127.0.0.1:{server.server_address[1]}",
            tenant_id="demo-tenant",
            warehouse_id="guntur-hub",
            api_header="X-API-Key",
            api_key="secret-token",
        )

        response = client.ingest(
            {
                "candidates": [
                    {
                        "buyerName": "Sri Lakshmi Gunj Traders",
                        "discoverySource": "buyer-discovery-worker",
                    }
                ]
            }
        )

        assert response == {
            "processedCount": 1,
            "candidates": [],
            "warning": "non_json_response",
            "rawBody": "OK",
        }
    finally:
        server.shutdown()
        server.server_close()


def test_backend_client_posts_market_price_snapshots_with_internal_headers() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _RequestCaptureHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = BackendIngestionClient(
            base_url=f"http://127.0.0.1:{server.server_address[1]}",
            tenant_id="demo-tenant",
            warehouse_id="guntur-hub",
            api_header="X-API-Key",
            api_key="secret-token",
        )

        response = client.ingest_market_price_snapshots(
            {
                "snapshots": [
                    {
                        "chilliVariety": "Teja",
                        "marketName": "Guntur",
                        "sourceLabel": "Agmarknet Official Board",
                        "sourceUrl": "fixture://official-board/agmarknet/guntur-teja-baseline",
                        "sourceType": "OFFICIAL_MANDI_BOARD",
                        "officialPricePerKg": 189.0,
                        "capturedAt": "2024-07-08T09:00:00Z",
                    }
                ]
            }
        )

        assert response["processedCount"] == 1
        captured = server.captured_request  # type: ignore[attr-defined]
        headers = {key.casefold(): value for key, value in captured["headers"].items()}
        assert captured["path"] == "/api/v1/internal/trading/market-intelligence/price-snapshots"
        assert headers["x-api-key"] == "secret-token"
        assert headers["x-tenant-id"] == "demo-tenant"
        assert headers["x-warehouse-id"] == "guntur-hub"
        assert captured["body"]["snapshots"][0]["chilliVariety"] == "Teja"
    finally:
        server.shutdown()
        server.server_close()


def test_backend_client_posts_market_chatter_with_internal_headers() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _RequestCaptureHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = BackendIngestionClient(
            base_url=f"http://127.0.0.1:{server.server_address[1]}",
            tenant_id="demo-tenant",
            warehouse_id="guntur-hub",
            api_header="X-API-Key",
            api_key="secret-token",
        )

        response = client.ingest_market_chatter(
            {
                "items": [
                    {
                        "chilliVariety": "Teja",
                        "headline": "Spot buyers stay selective in Guntur",
                        "summary": "Commission agents say colour checks remain strict.",
                        "sourceLabel": "Trade Press Digest",
                        "sourceUrl": "fixture://trade-press/baseline-teja",
                        "chatterType": "TRADE_COVERAGE",
                        "publishedAt": "2024-07-08T08:00:00Z",
                        "capturedAt": "2024-07-08T08:00:00Z",
                    }
                ]
            }
        )

        assert response["processedCount"] == 1
        captured = server.captured_request  # type: ignore[attr-defined]
        headers = {key.casefold(): value for key, value in captured["headers"].items()}
        assert captured["path"] == "/api/v1/internal/trading/market-intelligence/chatter"
        assert headers["x-api-key"] == "secret-token"
        assert headers["x-tenant-id"] == "demo-tenant"
        assert headers["x-warehouse-id"] == "guntur-hub"
        assert captured["body"]["items"][0]["headline"] == "Spot buyers stay selective in Guntur"
    finally:
        server.shutdown()
        server.server_close()


def test_backend_client_posts_market_carry_benchmarks_with_internal_headers() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _RequestCaptureHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = BackendIngestionClient(
            base_url=f"http://127.0.0.1:{server.server_address[1]}",
            tenant_id="demo-tenant",
            warehouse_id="guntur-hub",
            api_header="X-API-Key",
            api_key="secret-token",
        )

        response = client.ingest_market_carry_benchmarks(
            {
                "benchmarks": [
                    {
                        "chilliVariety": "Teja",
                        "carryPricePerKg": 185.0,
                        "sourceType": "WORKER_PROFILE",
                        "weightSource": "PROFILE_FALLBACK",
                        "bagCount": 420,
                        "weightKg": None,
                        "capturedAt": "2024-07-08T09:30:00Z",
                    }
                ]
            }
        )

        assert response["processedCount"] == 1
        captured = server.captured_request  # type: ignore[attr-defined]
        headers = {key.casefold(): value for key, value in captured["headers"].items()}
        assert captured["path"] == "/api/v1/internal/trading/market-intelligence/benchmarks"
        assert headers["x-api-key"] == "secret-token"
        assert headers["x-tenant-id"] == "demo-tenant"
        assert headers["x-warehouse-id"] == "guntur-hub"
        assert captured["body"]["benchmarks"][0]["carryPricePerKg"] == 185.0
    finally:
        server.shutdown()
        server.server_close()
