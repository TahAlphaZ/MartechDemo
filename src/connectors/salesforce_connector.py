import logging
from typing import Dict, Optional, Any

from src.connectors.base_connector import ConnectorConfig, APIConnector

logger = logging.getLogger(__name__)


class SalesforceConnector(APIConnector):
    """Salesforce REST API Connector (stub implementation for Bronze ingestion)."""

    def __init__(self, config: ConnectorConfig, api_key: str):
        super().__init__(config, api_key)
        self.instance_url = config.extra.get("base_url", "")

    def _make_request(
        self, method: str, endpoint: str, params: Optional[Dict] = None
    ) -> Dict:
        """Stubbed HTTP request (no real API call)."""
        logger.info(
            f"Salesforce API request | method={method} | endpoint={endpoint} | params={params}"
        )
        return {"data": [], "next_page_url": None}

    def validate_connection(self) -> bool:
        """Simulate connection validation."""
        if self.api_key and self.base_url:
            self._is_connected = True
            return True
        return False

    def get_schema(self) -> Dict[str, Any]:
        """Return expected Salesforce entities schema."""
        return {
            "accounts": {},
            "contacts": {},
            "opportunities": {},
        }
