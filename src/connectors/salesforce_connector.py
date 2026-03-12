"""src.connectors.salesforce_connector

Minimal Salesforce connector implementation.

Configuration
-------------
This connector expects its configuration to be provided via ConnectorConfig.extra (loaded from
config/connectors/connector_registry.yaml) and secrets to be referenced from Azure Key Vault.

Expected keys in ConnectorConfig.extra (all optional for construction):
- instance_url: Salesforce instance base URL (e.g. https://your-org.my.salesforce.com)
- api_version: Salesforce REST API version (e.g. v60.0)
- auth_type: typically "oauth_password" or similar (placeholder)
- client_id_secret_name: Key Vault secret name for connected app client id
- client_secret_secret_name: Key Vault secret name for connected app client secret
- username_secret_name: Key Vault secret name for Salesforce username
- password_secret_name: Key Vault secret name for Salesforce password
- token_secret_name: Key Vault secret name for Salesforce security token

Notes
-----
- No Salesforce network calls are made during instantiation.
- validate_connection/extract are intentionally stubbed to keep unit tests offline.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from src.connectors.base_connector import BaseConnector, ConnectorConfig


class SalesforceConnector(BaseConnector):
    """Salesforce connector (stub)."""

    connector_name = "salesforce"

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        # Pull non-secret configuration from the registry extra block.
        extra = config.extra or {}
        self.instance_url: str = extra.get("instance_url", "")
        self.api_version: str = extra.get("api_version", "")

    def validate_connection(self) -> bool:
        """Offline-safe validation.

        Returns True when minimal non-secret configuration is present.
        """
        self._is_connected = bool(self.instance_url)
        return self._is_connected

    def extract(self, query_or_endpoint: str, params: Optional[Dict] = None) -> Any:
        """Extract data from Salesforce.

        This repository's unit tests must remain offline. Real extraction should be implemented
        in a future iteration (OAuth, SOQL/REST, pagination, retries).
        """
        raise NotImplementedError(
            "SalesforceConnector.extract() is not implemented. "
            "Provide an implementation that authenticates via Key Vault referenced secrets and "
            "calls Salesforce REST APIs."
        )

    def get_schema(self) -> Dict[str, Any]:
        """Return expected schema structure.

        Stubbed to return an empty schema; real implementations may return per-object field
        metadata or a static expected schema.
        """
        return {}
