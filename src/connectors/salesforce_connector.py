import requests
import logging
from typing import Dict, Optional, Any

from src.connectors.base_connector import APIConnector

logger = logging.getLogger(__name__)


class SalesforceConnector(APIConnector):
    """Salesforce REST API Connector using OAuth Bearer authentication."""

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        if endpoint.startswith("http"):
            url = endpoint
        else:
            url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        response = requests.request(method, url, headers=headers, params=params)
        response.raise_for_status()
        json_response = response.json()

        if isinstance(json_response, dict) and "records" in json_response:
            return {
                "data": json_response.get("records", []),
                "next_page_url": json_response.get("nextRecordsUrl"),
            }

        return {
            "data": json_response,
            "next_page_url": None,
        }

    def validate_connection(self) -> bool:
        try:
            self._make_request("GET", "/limits")
            self._is_connected = True
            return True
        except Exception as e:
            logger.error(f"Salesforce connection failed for {self.config.name}: {e}")
            return False

    def get_schema(self) -> Dict[str, Any]:
        return {
            "objects_endpoint": "/sobjects",
            "query_endpoint": "/query",
        }
