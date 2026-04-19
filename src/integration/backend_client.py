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
        base = base_url.rstrip("/")
        self._buyer_lead_endpoint = f"{base}/api/v1/internal/trading/buyer-leads/ingestion"
        self._market_snapshot_endpoint = f"{base}/api/v1/internal/trading/market-intelligence/price-snapshots"
        self._market_chatter_endpoint = f"{base}/api/v1/internal/trading/market-intelligence/chatter"
        self._tenant_id = tenant_id
        self._warehouse_id = warehouse_id
        self._api_header = api_header
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds

    def ingest(self, payload: dict[str, object]) -> dict[str, object]:
        return self._post(
            self._buyer_lead_endpoint,
            payload,
            item_key="candidates",
            empty_response={"processedCount": 0, "candidates": []},
        )

    def ingest_market_price_snapshots(self, payload: dict[str, object]) -> dict[str, object]:
        return self._post(
            self._market_snapshot_endpoint,
            payload,
            item_key="snapshots",
            empty_response={"processedCount": 0},
        )

    def ingest_market_chatter(self, payload: dict[str, object]) -> dict[str, object]:
        return self._post(
            self._market_chatter_endpoint,
            payload,
            item_key="items",
            empty_response={"processedCount": 0},
        )

    def _post(
        self,
        endpoint: str,
        payload: dict[str, object],
        *,
        item_key: str,
        empty_response: dict[str, object],
    ) -> dict[str, object]:
        items = payload.get(item_key)
        if not isinstance(items, list) or not items:
            return dict(empty_response)
        if not self._api_key:
            raise ValueError("BUYER_DISCOVERY_INTERNAL_API_KEY must be configured before ingesting.")

        data = json.dumps(payload).encode()
        req = request.Request(
            endpoint,
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
            return dict(empty_response)

        try:
            return json.loads(body)
        except json.JSONDecodeError:
            response = dict(empty_response)
            response["processedCount"] = len(items)
            response["warning"] = "non_json_response"
            response["rawBody"] = body
            return response
