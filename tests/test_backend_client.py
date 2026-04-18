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
        response_body = json.dumps(
            {
                "processedCount": 1,
                "candidates": [{"candidateId": 7, "action": "CREATED"}],
            }
        ).encode()
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
