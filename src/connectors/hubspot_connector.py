"""
HubSpot connector implementation for marketing CRM data extraction.
Supports contacts, companies, deals, and other HubSpot objects via REST API.
"""
from __future__ import annotations

import importlib
import logging
from typing import Any, Dict, List, Optional

from src.connectors.base_connector import APIConnector, ConnectorConfig

logger = logging.getLogger(__name__)


class HubSpotConnector(APIConnector):
    """API connector backed by HubSpot REST API v3."""

    HUBSPOT_API_BASE = "https://api.hubapi.com"
    DEFAULT_RATE_LIMIT = 10  # requests per second

    def __init__(self, config: ConnectorConfig, api_key: str):
        """Initialize HubSpot connector with API key and base URL."""
        config_base_url = config.extra.get("base_url", self.HUBSPOT_API_BASE)
        super().__init__(config, api_key)
        self.base_url = config_base_url
        self.rate_limit = config.extra.get("rate_limit", self.DEFAULT_RATE_LIMIT)

    @staticmethod
    def _load_requests() -> Any:
        """Load requests library with proper error handling."""
        try:
            return importlib.import_module("requests")
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "requests is required for the HubSpot connector. "
                "Install the project with the 'hubspot' extra to enable it."
            ) from exc

    def validate_connection(self) -> bool:
        """Validate API connection by fetching account info."""
        try:
            response = self._make_request("GET", "/crm/v3/objects/contacts", params={"limit": 1})
            self._is_connected = response is not None
            if self._is_connected:
                logger.info("HubSpot API connection validated")
            return self._is_connected
        except Exception as e:
            logger.error(f"HubSpot connection validation failed: {e}")
            self._is_connected = False
            return False

    def extract(self, query_or_endpoint: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Extract data from HubSpot API endpoint with automatic pagination.
        Endpoint should be relative to base_url (e.g., "/crm/v3/objects/contacts").
        """
        if not self._is_connected:
            self.validate_connection()

        all_data: List[Dict[str, Any]] = []
        after = None

        while True:
            request_params = params.copy() if params else {}
            if after:
                request_params["after"] = after

            try:
                response = self._make_request("GET", query_or_endpoint, params=request_params)
                if response is None:
                    break

                # Extract data from response
                results = response.get("results", [])
                all_data.extend(results)

                # Check for pagination
                paging = response.get("paging", {})
                after = paging.get("next", {}).get("after")
                if not after:
                    break

            except Exception as e:
                logger.error(f"Error fetching from {query_or_endpoint}: {e}")
                break

        return all_data

    def get_schema(self) -> Dict[str, Any]:
        """Return HubSpot data schema/available objects."""
        schema = self.config.extra.get("schema", {})
        if not schema:
            # Default schema for common HubSpot objects
            schema = {
                "objects": [
                    {
                        "name": "contacts",
                        "endpoint": "/crm/v3/objects/contacts",
                        "properties": ["firstname", "lastname", "email", "phone", "lifecyclestage"],
                    },
                    {
                        "name": "companies",
                        "endpoint": "/crm/v3/objects/companies",
                        "properties": ["name", "industry", "website", "country"],
                    },
                    {
                        "name": "deals",
                        "endpoint": "/crm/v3/objects/deals",
                        "properties": ["dealname", "amount", "dealstage", "closedate"],
                    },
                    {
                        "name": "tickets",
                        "endpoint": "/crm/v3/objects/tickets",
                        "properties": ["subject", "content", "hs_ticket_priority", "status"],
                    },
                ]
            }
        return schema

    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make HTTP request to HubSpot API with authentication."""
        requests = self._load_requests()

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        url = f"{self.base_url}{endpoint}"

        try:
            if method == "GET":
                response = requests.get(url, headers=headers, params=params, timeout=30)
            elif method == "POST":
                response = requests.post(url, headers=headers, json=params, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning(f"Rate limit hit on {endpoint}, caller should implement backoff")
            elif e.response.status_code == 401:
                logger.error("Unauthorized: Invalid or expired HubSpot API key")
            else:
                logger.error(f"HTTP error {e.response.status_code}: {e.response.text}")
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
