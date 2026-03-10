"""
Base Connector Interface - All data connectors implement this contract.
Subagents: Extend BaseConnector for new data sources.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class ConnectorConfig:
    """Configuration loaded from connector_registry.yaml"""

    name: str
    connector_type: str  # "database", "file", "api", "analytics"
    keyvault_secret: str
    landing_path: str
    extra: Dict[str, Any] = field(default_factory=dict)


class BaseConnector(ABC):
    """
    Abstract base class for all data connectors.

    To add a new connector (e.g., Google Analytics, Adobe):
    1. Create a new file in src/connectors/ (e.g., google_analytics_connector.py)
    2. Inherit from BaseConnector
    3. Implement extract(), validate_connection(), get_schema()
    4. Register in config/connectors/connector_registry.yaml
    5. Write tests in tests/unit/connectors/
    """

    def __init__(self, config: ConnectorConfig) -> None:
        self.config: ConnectorConfig = config
        self._is_connected: bool = False

    @abstractmethod
    def validate_connection(self) -> bool:
        """Test that the data source is reachable. Used in TDD health checks."""
        ...

    @abstractmethod
    def extract(self, query_or_endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Extract data from the source.
        Returns data in a format suitable for landing in the raw zone.
        """
        ...

    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """Return the schema/structure of the source data for validation."""
        ...

    def get_metadata(self) -> Dict[str, str]:
        """Standard metadata attached to every extraction."""
        return {
            "_source_system": self.config.name,
            "_connector_type": self.config.connector_type,
            "_landing_path": self.config.landing_path,
        }


class DatabaseConnector(BaseConnector):
    """
    Base class for all database connectors.
    Swap between Postgres/SQL Server/MySQL by changing connector_registry.yaml.
    """

    def __init__(self, config: ConnectorConfig, connection_string: str) -> None:
        super().__init__(config)
        self.connection_string: str = connection_string

    def validate_connection(self) -> bool:
        test_query = self.config.extra.get("test_query", "SELECT 1")
        try:
            self._execute(test_query)
            self._is_connected = True
            logger.info("Connection validated for %s", self.config.name)
            return True
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("Connection failed for %s: %s", self.config.name, exc)
            return False

    def extract(self, query_or_endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute SQL query and return results as list of dicts."""
        if not self._is_connected:
            self.validate_connection()
        return self._execute(query_or_endpoint, params)

    def get_schema(self) -> Dict[str, Any]:
        """Introspect database schema from information_schema."""
        return self._execute(
            "SELECT table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = 'public'"
        )

    def _execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Override in concrete implementations for actual DB execution."""
        raise NotImplementedError("Subclass must implement _execute")


class APIConnector(BaseConnector):
    """
    Base class for REST API connectors (ecommerce, analytics).
    Handles pagination, rate limiting, and retry logic.
    """

    def __init__(self, config: ConnectorConfig, api_key: str) -> None:
        super().__init__(config)
        self.api_key: str = api_key
        self.base_url: str = str(config.extra.get("base_url", ""))
        self.rate_limit: int = int(config.extra.get("rate_limit", 5))

    def validate_connection(self) -> bool:
        """Ping the API health endpoint."""
        try:
            result = self._make_request("GET", "/")
            self._is_connected = result is not None
            return self._is_connected
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("API connection failed for %s: %s", self.config.name, exc)
            return False

    def extract(self, query_or_endpoint: str, params: Optional[Dict[str, Any]] = None) -> List[Any]:
        """Fetch data from API endpoint with automatic pagination."""
        all_data: List[Any] = []
        next_page: Optional[str] = query_or_endpoint

        while next_page:
            response = self._make_request("GET", next_page, params)
            if not isinstance(response, dict):
                raise ValueError("API response must be a dictionary")

            data = response.get("data", [])
            if not isinstance(data, list):
                raise ValueError("API response 'data' field must be a list")

            all_data.extend(data)
            next_page = response.get("next_page_url")

        return all_data

    def get_schema(self) -> Dict[str, Any]:
        """Return expected schema for the connector's data."""
        schema = self.config.extra.get("schema", {})
        if not isinstance(schema, dict):
            raise ValueError("Configured schema must be a dictionary")
        return schema

    def _make_request(
        self, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Override in concrete implementations for actual HTTP calls."""
        raise NotImplementedError("Subclass must implement _make_request")
