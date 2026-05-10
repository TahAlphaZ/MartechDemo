"""
Adobe Analytics connector.
"""
from __future__ import annotations

import json
import logging
import time
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
        self._cached_access_token: str | None = None
        self._cached_access_token_expires_at = 0.0

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
        normalization_context = dict(request_params)
        if endpoint_name == "reports":
            request_params, normalization_context = self._build_report_request_payload(request_params)
        endpoint = self._resolve_endpoint(endpoint_name)
        method = self._resolve_method(endpoint_name, request_params)
        response = self._make_request(method, endpoint, request_params)
        return self._normalize_response(response, endpoint_name, normalization_context)

    def get_schema(self) -> dict[str, Any]:
        """Return configured report types and default reporting shape."""
        return {
            "report_types": list(self.config.extra.get("endpoints", {}).keys()),
            "dimensions": list(self.config.extra.get("default_dimensions", [])),
            "metrics": list(self.config.extra.get("default_metrics", [])),
            "column_aliases": dict(self.config.extra.get("column_aliases", {})),
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
        if access_token is not None:
            return str(access_token)

        if self._cached_access_token and time.time() < self._cached_access_token_expires_at:
            return self._cached_access_token

        client_secret = self._auth_config.get("client_secret")
        token_endpoint = self.config.extra.get("token_endpoint") or self._auth_config.get("token_endpoint")
        scopes = self.config.extra.get("scopes") or self._auth_config.get("scopes")
        if client_secret is None or token_endpoint is None or scopes is None:
            raise ValueError(
                "Adobe Analytics connector secret must include access_token/bearer_token or "
                "client_secret with token_endpoint and scopes for OAuth"
            )

        response = self._session.post(
            str(token_endpoint),
            data={
                "grant_type": "client_credentials",
                "client_id": self._resolve_client_id(),
                "client_secret": str(client_secret),
                "scope": self._normalize_scopes(scopes),
            },
            timeout=int(self.config.extra.get("timeout_seconds", 30)),
        )
        response.raise_for_status()
        token_payload = response.json()
        resolved_token = token_payload.get("access_token")
        if resolved_token is None:
            raise ValueError("Adobe Analytics OAuth token response did not include access_token")

        expires_in = int(token_payload.get("expires_in", 3600))
        self._cached_access_token = str(resolved_token)
        self._cached_access_token_expires_at = time.time() + max(expires_in - 60, 60)
        return self._cached_access_token

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
        dimension_names = self._resolve_dimension_names(response, params)
        rows: list[dict[str, Any]] = []

        for row in response.get("rows", []):
            rows.extend(self._flatten_report_rows(row, dimension_names, metric_names, {}, 0))

        return rows

    def _resolve_metric_names(self, response: dict[str, Any], params: dict[str, Any]) -> list[str]:
        metrics = params.get("metricContainer", {}).get("metrics", [])
        if metrics:
            return [
                self._alias_column_name(str(metric.get("id")))
                for metric in metrics
                if metric.get("id") is not None
            ]
        return [
            self._alias_column_name(str(column_id))
            for column_id in response.get("columns", {}).get("columnIds", [])
        ]

    def _resolve_dimension_names(self, response: dict[str, Any], params: dict[str, Any]) -> list[str]:
        primary_dimension = (
            params.get("dimension")
            or response.get("columns", {}).get("dimension", {}).get("id")
            or self.config.extra.get("default_dimensions", ["dimension"])[0]
        )
        breakdown_dimensions = [str(value) for value in params.get("breakdown_dimensions", [])]
        return [
            self._alias_column_name(str(primary_dimension)),
            *[self._alias_column_name(value) for value in breakdown_dimensions],
        ]

    def _flatten_report_rows(
        self,
        row: dict[str, Any],
        dimension_names: list[str],
        metric_names: list[str],
        dimension_values: dict[str, Any],
        depth: int,
    ) -> list[dict[str, Any]]:
        dimension_name = (
            dimension_names[depth] if depth < len(dimension_names) else f"breakdown_level_{depth}"
        )
        current_values = {
            **dimension_values,
            dimension_name: row.get("value"),
        }

        breakdown_rows = [child for child in row.get("breakdown", []) if isinstance(child, dict)]
        if breakdown_rows:
            flattened: list[dict[str, Any]] = []
            for child in breakdown_rows:
                flattened.extend(
                    self._flatten_report_rows(
                        child,
                        dimension_names,
                        metric_names,
                        current_values,
                        depth + 1,
                    )
                )
            return flattened

        normalized = dict(current_values)
        normalized.update({name: value for name, value in zip(metric_names, row.get("data", []))})
        normalized.update(self.get_metadata())
        normalized["_report_type"] = "reports"
        return [normalized]

    def _build_report_request_payload(
        self, params: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        payload = dict(params)
        breakdown_dimensions = payload.pop("breakdown_dimensions", self.config.extra.get("breakdown_dimensions", []))
        if "rsid" not in payload:
            payload["rsid"] = self._resolve_report_suite_id(payload)
        if "dimension" not in payload:
            payload["dimension"] = self._resolve_primary_dimension()
        if "globalFilters" not in payload:
            payload["globalFilters"] = list(self.config.extra.get("default_global_filters", []))
        if "metricContainer" not in payload:
            payload["metricContainer"] = {
                "metrics": [
                    {"id": metric_name}
                    for metric_name in self.config.extra.get("default_metrics", [])
                ]
            }
        normalization_context = dict(payload)
        normalization_context["breakdown_dimensions"] = list(breakdown_dimensions)
        return payload, normalization_context

    def _resolve_report_suite_id(self, params: dict[str, Any]) -> str:
        report_suite_id = params.get("rsid") or self.config.extra.get("report_suite_id")
        if report_suite_id is None:
            raise ValueError(
                "Adobe Analytics connector requires rsid in params or config.extra['report_suite_id']"
            )
        return str(report_suite_id)

    def _resolve_primary_dimension(self) -> str:
        default_dimensions = self.config.extra.get("default_dimensions", [])
        if not default_dimensions:
            raise ValueError(
                "Adobe Analytics connector requires config.extra['default_dimensions'] for report extraction"
            )
        return str(default_dimensions[0])

    def _alias_column_name(self, column_name: str) -> str:
        return str(self.config.extra.get("column_aliases", {}).get(column_name, column_name))

    def _normalize_scopes(self, scopes: Any) -> str:
        if isinstance(scopes, str):
            return scopes
        if isinstance(scopes, list):
            return " ".join(str(scope) for scope in scopes)
        raise ValueError("Adobe Analytics connector scopes must be a string or list of strings")

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
