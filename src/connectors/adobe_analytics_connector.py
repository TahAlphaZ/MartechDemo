"""
Adobe Analytics connector.
"""
from __future__ import annotations

import json
import logging
from typing import Any

import requests

from src.connectors.base_connector import APIConnector, ConnectorConfig

logger = logging.getLogger(__name__)


class AdobeAnalyticsConnector(APIConnector):
    """Connector for the Adobe Analytics API using a JSON secret."""

    def __init__(self, config: ConnectorConfig, api_key: str):
        super().__init__(config, api_key)
        self._auth_config = self._load_auth_config(api_key)
        self._session = requests.Session()
        self.base_url = self._resolve_base_url()

    def validate_connection(self) -> bool:
        """Validate the configured credentials against a lightweight endpoint."""
        try:
            self._make_request("GET", self._resolve_endpoint("segments"), {"limit": 1, "page": 0})
            self._is_connected = True
            return True
        except Exception as exc:
            logger.error("Adobe Analytics connection failed for %s: %s", self.config.name, exc)
            return False

    def extract(self, query_or_endpoint: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Run a report or list query and normalize the returned records."""
        if not self._is_connected:
            self.validate_connection()

        request_params = dict(params or {})
        endpoint_name = query_or_endpoint or "reports"
        endpoint = self._resolve_endpoint(endpoint_name)
        method = self._resolve_method(endpoint_name, request_params)
        response = self._make_request(method, endpoint, request_params)
        return self._normalize_response(response, endpoint_name, request_params)

    def get_schema(self) -> dict[str, Any]:
        """Return configured report types and default reporting shape."""
        return {
            "report_types": list(self.config.extra.get("endpoints", {}).keys()),
            "dimensions": list(self.config.extra.get("default_dimensions", [])),
            "metrics": list(self.config.extra.get("default_metrics", [])),
            "row_metadata": self.get_metadata(),
        }

    def _make_request(
        self, method: str, endpoint: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        response = self._session.request(
            method=method.upper(),
            url=url,
            headers=self._build_headers(),
            json=params if method.upper() != "GET" else None,
            params=params if method.upper() == "GET" else None,
            timeout=int(self.config.extra.get("timeout_seconds", 30)),
        )
        response.raise_for_status()
        return response.json()

    def _load_auth_config(self, secret_value: str) -> dict[str, Any]:
        try:
            auth_config = json.loads(secret_value)
        except json.JSONDecodeError as exc:
            raise ValueError("Adobe Analytics connector secret must be valid JSON") from exc

        if not isinstance(auth_config, dict):
            raise ValueError("Adobe Analytics connector secret must decode to a JSON object")
        return auth_config

    def _resolve_company_id(self) -> str:
        company_id = (
            self.config.extra.get("company_id")
            or self._auth_config.get("company_id")
            or self._auth_config.get("global_company_id")
        )
        if company_id is None:
            raise ValueError(
                "Adobe Analytics connector requires company_id in config.extra or the secret payload"
            )
        return str(company_id)

    def _resolve_access_token(self) -> str:
        access_token = self._auth_config.get("access_token") or self._auth_config.get("bearer_token")
        if access_token is None:
            raise ValueError("Adobe Analytics connector secret must include access_token or bearer_token")
        return str(access_token)

    def _resolve_client_id(self) -> str:
        client_id = self._auth_config.get("client_id") or self._auth_config.get("api_key")
        if client_id is None:
            raise ValueError("Adobe Analytics connector secret must include client_id or api_key")
        return str(client_id)

    def _resolve_base_url(self) -> str:
        base_url_template = str(self.config.extra.get("base_url", ""))
        return base_url_template.format(company_id=self._resolve_company_id())

    def _build_headers(self) -> dict[str, str]:
        company_id = self._resolve_company_id()
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._resolve_access_token()}",
            "Content-Type": "application/json",
            "x-api-key": self._resolve_client_id(),
            "x-proxy-global-company-id": company_id,
        }

    def _resolve_endpoint(self, endpoint_name: str) -> str:
        configured_endpoint = self.config.extra.get("endpoints", {}).get(endpoint_name)
        if configured_endpoint is not None:
            return str(configured_endpoint)
        if endpoint_name.startswith("/"):
            return endpoint_name
        raise ValueError(f"Unsupported Adobe Analytics endpoint '{endpoint_name}'")

    def _resolve_method(self, endpoint_name: str, params: dict[str, Any]) -> str:
        method = params.pop("_method", None)
        if method is not None:
            return str(method).upper()
        if endpoint_name == "reports":
            return "POST"
        return "GET"

    def _normalize_response(
        self, response: dict[str, Any], endpoint_name: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if endpoint_name == "reports":
            return self._normalize_report_response(response, params)
        return self._normalize_collection_response(response, endpoint_name)

    def _normalize_report_response(
        self, response: dict[str, Any], params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        metric_names = self._resolve_metric_names(response, params)
        dimension_name = (
            response.get("columns", {}).get("dimension", {}).get("id")
            or self.config.extra.get("default_dimensions", ["dimension"])[0]
        )
        rows: list[dict[str, Any]] = []

        for row in response.get("rows", []):
            normalized = {
                str(dimension_name): row.get("value"),
            }
            normalized.update({name: value for name, value in zip(metric_names, row.get("data", []))})
            normalized.update(self.get_metadata())
            normalized["_report_type"] = "reports"
            rows.append(normalized)

        return rows

    def _resolve_metric_names(self, response: dict[str, Any], params: dict[str, Any]) -> list[str]:
        metrics = params.get("metricContainer", {}).get("metrics", [])
        if metrics:
            return [str(metric.get("id")) for metric in metrics if metric.get("id") is not None]
        return [str(column_id) for column_id in response.get("columns", {}).get("columnIds", [])]

    def _normalize_collection_response(
        self, response: dict[str, Any], endpoint_name: str
    ) -> list[dict[str, Any]]:
        records = (
            response.get("content")
            or response.get("segments")
            or response.get("data")
            or []
        )
        return [
            {
                **record,
                **self.get_metadata(),
                "_report_type": endpoint_name,
            }
            for record in records
            if isinstance(record, dict)
        ]
