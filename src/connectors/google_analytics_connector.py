"""
Google Analytics 4 Connector.

Concrete analytics connector for the GA4 Data API. The connector accepts a
service-account JSON secret (preferred) or a pre-minted OAuth bearer token via
``secret_value`` in the connector factory.
"""
from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from datetime import date, timedelta
from typing import Any, Dict, Optional

from src.connectors.base_connector import APIConnector, ConnectorConfig

logger = logging.getLogger(__name__)


class GoogleAnalyticsConnector(APIConnector):
    """Connector for extracting report data from the Google Analytics 4 Data API."""

    OAUTH_SCOPE = "https://www.googleapis.com/auth/analytics.readonly"

    def __init__(self, config: ConnectorConfig, api_key: str):
        super().__init__(config, api_key)
        self.service_account_json = api_key
        self._access_token: Optional[str] = None
        self.property_id = str(config.extra.get("property_id", "")).strip()
        self.base_url = config.extra.get("base_url", "https://analyticsdata.googleapis.com/v1beta").rstrip("/")

    def validate_connection(self) -> bool:
        """Run a tiny GA4 report to verify credentials and property access."""
        try:
            property_id = self._resolve_property_id({})
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            self._run_report(
                property_id=property_id,
                dimensions=["date"],
                metrics=["sessions"],
                date_ranges=[{"startDate": yesterday, "endDate": yesterday}],
                limit=1,
            )
            self._is_connected = True
            return True
        except Exception as exc:  # pragma: no cover - callers assert boolean health
            logger.error("Google Analytics connection failed for %s: %s", self.config.name, exc)
            self._is_connected = False
            return False

    def extract(self, query_or_endpoint: str = "reports", params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Extract a GA4 report.

        ``query_or_endpoint`` may be a registry endpoint key (``reports`` or
        ``realtime``), a GA4 endpoint path, or omitted. Supported params:
        - property_id: GA4 numeric property id
        - dimensions: list[str]
        - metrics: list[str]
        - date_ranges: GA API date range objects
        - start_date/end_date: convenience date range values
        - limit, offset, order_bys, dimension_filter, metric_filter
        """
        params = params or {}
        property_id = self._resolve_property_id(params)
        endpoint = self._resolve_endpoint(query_or_endpoint, property_id)

        if query_or_endpoint == "realtime" or endpoint.endswith(":runRealtimeReport"):
            payload = self._build_realtime_payload(params)
        else:
            payload = self._build_report_payload(params)

        response = self._make_request("POST", endpoint, payload)
        return {
            "metadata": self.get_metadata(),
            "property_id": property_id,
            "report_type": "realtime" if endpoint.endswith(":runRealtimeReport") else "report",
            "request": payload,
            "response": response,
        }

    def get_schema(self) -> Dict[str, Any]:
        """Return the expected GA4 report response structure."""
        return {
            "dimensionHeaders": [{"name": "string"}],
            "metricHeaders": [{"name": "string", "type": "string"}],
            "rows": [
                {
                    "dimensionValues": [{"value": "string"}],
                    "metricValues": [{"value": "string"}],
                }
            ],
            "rowCount": "integer",
            "metadata": "object",
        }

    def _run_report(
        self,
        property_id: str,
        dimensions: list[str],
        metrics: list[str],
        date_ranges: list[Dict[str, str]],
        limit: int = 10000,
    ) -> Dict[str, Any]:
        endpoint = self._resolve_endpoint("reports", property_id)
        payload = {
            "dimensions": [{"name": dimension} for dimension in dimensions],
            "metrics": [{"name": metric} for metric in metrics],
            "dateRanges": date_ranges,
            "limit": limit,
        }
        return self._make_request("POST", endpoint, payload)

    def _build_report_payload(self, params: Dict[str, Any]) -> Dict[str, Any]:
        dimensions = params.get("dimensions", self.config.extra.get("default_dimensions", ["date"]))
        metrics = params.get("metrics", self.config.extra.get("default_metrics", ["sessions"]))
        date_ranges = params.get("date_ranges")
        if not date_ranges:
            date_ranges = [
                {
                    "startDate": params.get("start_date", "7daysAgo"),
                    "endDate": params.get("end_date", "yesterday"),
                }
            ]

        payload: Dict[str, Any] = {
            "dimensions": [{"name": dimension} for dimension in dimensions],
            "metrics": [{"name": metric} for metric in metrics],
            "dateRanges": date_ranges,
            "limit": params.get("limit", self.config.extra.get("limit", 10000)),
        }
        self._copy_optional_filters(payload, params)
        return payload

    def _build_realtime_payload(self, params: Dict[str, Any]) -> Dict[str, Any]:
        dimensions = params.get("dimensions", ["country", "deviceCategory"])
        metrics = params.get("metrics", ["activeUsers"])
        payload: Dict[str, Any] = {
            "dimensions": [{"name": dimension} for dimension in dimensions],
            "metrics": [{"name": metric} for metric in metrics],
            "limit": params.get("limit", 10000),
        }
        self._copy_optional_filters(payload, params)
        return payload

    def _copy_optional_filters(self, payload: Dict[str, Any], params: Dict[str, Any]) -> None:
        optional_param_map = {
            "offset": "offset",
            "order_bys": "orderBys",
            "dimension_filter": "dimensionFilter",
            "metric_filter": "metricFilter",
            "keep_empty_rows": "keepEmptyRows",
            "currency_code": "currencyCode",
        }
        for source_key, ga_key in optional_param_map.items():
            if source_key in params:
                payload[ga_key] = params[source_key]

    def _resolve_property_id(self, params: Dict[str, Any]) -> str:
        property_id = str(params.get("property_id") or self.property_id or "").strip()
        if not property_id:
            raise ValueError(
                "Google Analytics property_id is required. Add it to connector extra config "
                "or pass params={'property_id': '...'} when extracting."
            )
        return property_id

    def _resolve_endpoint(self, endpoint_key_or_path: str, property_id: str) -> str:
        endpoints = self.config.extra.get("endpoints", {})
        endpoint = endpoints.get(endpoint_key_or_path, endpoint_key_or_path)
        if not endpoint or endpoint == "reports":
            endpoint = "/properties/{property_id}:runReport"
        endpoint = endpoint.format(property_id=property_id)
        return endpoint if endpoint.startswith("/") else f"/{endpoint}"

    def _get_access_token(self) -> str:
        if self._access_token:
            return self._access_token

        secret = (self.service_account_json or "").strip()
        if secret.startswith("Bearer "):
            self._access_token = secret.removeprefix("Bearer ").strip()
            return self._access_token
        if secret.startswith("ya29."):
            self._access_token = secret
            return self._access_token

        try:
            from google.auth.transport.requests import Request  # type: ignore
            from google.oauth2 import service_account  # type: ignore
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise ImportError(
                "google-auth is required for GA4 service-account authentication. "
                "Install the project with the google analytics dependency."
            ) from exc

        credentials_info = json.loads(secret)
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=[self.OAUTH_SCOPE],
        )
        credentials.refresh(Request())
        self._access_token = credentials.token
        return self._access_token

    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make an authenticated request to the GA4 Data API."""
        url = endpoint if endpoint.startswith("http") else f"{self.base_url}{endpoint}"
        payload = json.dumps(params or {}).encode("utf-8") if method.upper() in {"POST", "PUT", "PATCH"} else None
        headers = {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        request = urllib.request.Request(url=url, data=payload, headers=headers, method=method.upper())

        try:
            with urllib.request.urlopen(request, timeout=self.config.extra.get("timeout_seconds", 60)) as response:
                body = response.read().decode("utf-8")
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8")
            logger.error("GA4 API request failed: %s %s - %s", method, url, error_body)
            raise
