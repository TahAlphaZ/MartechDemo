"""
Google Analytics 4 connector.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2 import service_account

from src.connectors.base_connector import APIConnector, ConnectorConfig

logger = logging.getLogger(__name__)


class GoogleAnalyticsConnector(APIConnector):
    """Connector for the GA4 Data API using a service account secret."""

    SCOPES = ("https://www.googleapis.com/auth/analytics.readonly",)

    def __init__(self, config: ConnectorConfig, api_key: str):
        super().__init__(config, api_key)
        self._service_account_info = self._load_service_account_info(api_key)
        self._session: AuthorizedSession | None = None

    def validate_connection(self) -> bool:
        """Validate the service account and optionally confirm the configured property is reachable."""
        try:
            self._build_credentials().refresh(Request())

            property_id = self._resolve_property_id()
            if property_id:
                self._make_request("POST", self._resolve_endpoint("realtime", property_id), {"limit": 1})

            self._is_connected = True
            return True
        except Exception as exc:
            logger.error("Google Analytics connection failed for %s: %s", self.config.name, exc)
            return False

    def extract(self, query_or_endpoint: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Run a report or realtime query and normalize the returned rows."""
        if not self._is_connected:
            self.validate_connection()

        request_params = dict(params or {})
        endpoint_name = query_or_endpoint or "reports"
        property_id = self._get_required_property_id(request_params)
        endpoint = self._resolve_endpoint(endpoint_name, property_id)
        payload = self._build_request_payload(endpoint_name, request_params)
        response = self._make_request("POST", endpoint, payload)
        return self._normalize_response(response, endpoint_name)

    def get_schema(self) -> dict[str, Any]:
        """Return the configured dimensions and metrics for the connector."""
        default_dimensions = self.config.extra.get("default_dimensions", [])
        default_metrics = self.config.extra.get("default_metrics", [])
        return {
            "report_types": list(self.config.extra.get("endpoints", {}).keys()),
            "dimensions": list(default_dimensions),
            "metrics": list(default_metrics),
            "row_metadata": self.get_metadata(),
        }

    def _make_request(
        self, method: str, endpoint: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        session = self._get_session()
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        response = session.request(
            method=method.upper(),
            url=url,
            json=params,
            timeout=int(self.config.extra.get("timeout_seconds", 30)),
        )
        response.raise_for_status()
        return response.json()

    def _get_session(self) -> AuthorizedSession:
        if self._session is None:
            self._session = AuthorizedSession(self._build_credentials())
        return self._session

    def _build_credentials(self) -> service_account.Credentials:
        return service_account.Credentials.from_service_account_info(
            self._service_account_info,
            scopes=self.SCOPES,
        )

    def _load_service_account_info(self, secret_value: str) -> dict[str, Any]:
        try:
            service_account_info = json.loads(secret_value)
        except json.JSONDecodeError as exc:
            raise ValueError("Google Analytics connector secret must be valid service account JSON") from exc

        if not isinstance(service_account_info, dict):
            raise ValueError("Google Analytics connector secret must decode to a JSON object")
        return service_account_info

    def _resolve_property_id(self) -> str | None:
        property_id = self.config.extra.get("property_id")
        if property_id is None:
            return None
        return str(property_id)

    def _get_required_property_id(self, params: dict[str, Any]) -> str:
        property_id = params.pop("property_id", None) or self._resolve_property_id()
        if property_id is None:
            raise ValueError(
                "Google Analytics connector requires a property_id in params or config.extra['property_id']"
            )
        return str(property_id)

    def _resolve_endpoint(self, endpoint_name: str, property_id: str) -> str:
        endpoint_template = self.config.extra.get("endpoints", {}).get(endpoint_name)
        if endpoint_template is None:
            raise ValueError(f"Unsupported Google Analytics endpoint '{endpoint_name}'")
        return str(endpoint_template).format(property_id=property_id)

    def _build_request_payload(self, endpoint_name: str, params: dict[str, Any]) -> dict[str, Any]:
        payload = dict(params)
        payload.setdefault(
            "dimensions",
            self._build_named_items(self.config.extra.get("default_dimensions", [])),
        )
        payload.setdefault(
            "metrics",
            self._build_named_items(self.config.extra.get("default_metrics", [])),
        )

        if endpoint_name == "reports":
            payload.setdefault(
                "dateRanges",
                [{"startDate": "7daysAgo", "endDate": "yesterday"}],
            )

        return payload

    def _build_named_items(self, values: list[str]) -> list[dict[str, str]]:
        return [{"name": value} for value in values]

    def _normalize_response(self, response: dict[str, Any], endpoint_name: str) -> list[dict[str, Any]]:
        dimension_names = [header["name"] for header in response.get("dimensionHeaders", [])]
        metric_names = [header["name"] for header in response.get("metricHeaders", [])]
        rows: list[dict[str, Any]] = []

        for row in response.get("rows", []):
            normalized = {
                name: value.get("value")
                for name, value in zip(dimension_names, row.get("dimensionValues", []))
            }
            normalized.update(
                {
                    name: value.get("value")
                    for name, value in zip(metric_names, row.get("metricValues", []))
                }
            )
            normalized.update(self.get_metadata())
            normalized["_report_type"] = endpoint_name
            rows.append(normalized)

        return rows
