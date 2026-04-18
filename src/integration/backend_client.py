"""Minimal HTTP client for the backend buyer lead ingestion endpoint."""

from __future__ import annotations

import json
from urllib import error, request


class BackendIngestionClient:
    """POSTs worker-produced buyer lead payloads into the backend."""

    def __init__(
        self,
        *,
        base_url: str,
        tenant_id: str,
        warehouse_id: str,
        api_header: str,
        api_key: str,
        timeout_seconds: int = 30,
    ) -> None:
        self._endpoint = f"{base_url.rstrip('/')}/api/v1/internal/trading/buyer-leads/ingestion"
        self._tenant_id = tenant_id
        self._warehouse_id = warehouse_id
        self._api_header = api_header
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds

    def ingest(self, payload: dict[str, object]) -> dict[str, object]:
        candidates = payload.get("candidates")
        if not isinstance(candidates, list) or not candidates:
            return {"processedCount": 0, "candidates": []}
        if not self._api_key:
            raise ValueError("BUYER_DISCOVERY_INTERNAL_API_KEY must be configured before ingesting.")

        data = json.dumps(payload).encode()
        req = request.Request(
            self._endpoint,
            data=data,
            headers={
                "Content-Type": "application/json",
                self._api_header: self._api_key,
                "X-Tenant-Id": self._tenant_id,
                "X-Warehouse-Id": self._warehouse_id,
            },
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=self._timeout_seconds) as response:
                body = response.read().decode()
        except error.HTTPError as exc:
            detail = exc.read().decode()
            raise RuntimeError(
                f"Backend ingestion failed with HTTP {exc.code}: {detail}"
            ) from exc
        except error.URLError as exc:
            raise RuntimeError(
                f"Backend ingestion request failed: {exc.reason}"
            ) from exc

        if not body:
            return {"processedCount": 0, "candidates": []}

        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {
                "processedCount": len(candidates),
                "candidates": [],
                "warning": "non_json_response",
                "rawBody": body,
            }
