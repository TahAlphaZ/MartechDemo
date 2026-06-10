"""
Base Connector Interface - All data connectors implement this contract.
Subagents: Extend BaseConnector for new data sources.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ConnectorConfig:
    """Configuration loaded from connector_registry.yaml"""
    name: str
    connector_type: str  # "database", "file", "api", "analytics"
    keyvault_secret: str
    landing_path: str
    extra: dict[str, Any] = field(default_factory=dict)


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

    def __init__(self, config: ConnectorConfig):
        self.config = config
        self._is_connected = False

    @abstractmethod
    def validate_connection(self) -> bool:
        """Test that the data source is reachable. Used in TDD health checks."""
        ...

    @abstractmethod
    def extract(self, query_or_endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """
        Extract data from the source.
        Returns data in a format suitable for landing in the raw zone.
        """
        ...

    @abstractmethod
    def get_schema(self) -> dict[str, Any]:
        """Return the schema/structure of the source data for validation."""
        ...

    def get_metadata(self) -> dict[str, str]:
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

    def __init__(self, config: ConnectorConfig, connection_string: str):
        super().__init__(config)
        self.connection_string = connection_string

    def validate_connection(self) -> bool:
        test_query = self.config.extra.get("test_query", "SELECT 1")
        try:
            self._execute(test_query)
            self._is_connected = True
            logger.info(f"Connection validated for {self.config.name}")
            return True
        except Exception as e:
            logger.error(f"Connection failed for {self.config.name}: {e}")
            return False

    def extract(self, query_or_endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """Execute SQL query and return results as list of dicts."""
        if not self._is_connected:
            self.validate_connection()
        return self._execute(query_or_endpoint, params)

    def get_schema(self) -> dict[str, Any]:
        """Introspect database schema from information_schema."""
        return self._execute(
            "SELECT table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = 'public'"
        )

    def _execute(self, query: str, params: dict[str, Any] | None = None) -> Any:
        """Override in concrete implementations for actual DB execution."""
        raise NotImplementedError("Subclass must implement _execute")


class APIConnector(BaseConnector):
    """
    Base class for REST API connectors (ecommerce, analytics).
    Handles pagination, rate limiting, and retry logic.
    """

    def __init__(self, config: ConnectorConfig, api_key: str):
        super().__init__(config)
        self.api_key = api_key
        self.base_url = config.extra.get("base_url", "")
        self.rate_limit = config.extra.get("rate_limit", 5)

    def validate_connection(self) -> bool:
        """Ping the API health endpoint."""
        try:
            # Subclasses implement actual HTTP call
            result = self._make_request("GET", "/")
            self._is_connected = result is not None
            return self._is_connected
        except Exception as e:
            logger.error(f"API connection failed for {self.config.name}: {e}")
            return False

    def extract(self, query_or_endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """Fetch data from API endpoint with automatic pagination."""
        all_data: list[Any] = []
        next_page = query_or_endpoint

        while next_page:
            response = self._make_request("GET", next_page, params)
            all_data.extend(response.get("data", []))
            next_page = response.get("next_page_url")

        return all_data

    def get_schema(self) -> dict[str, Any]:
        """Return expected schema for the connector's data."""
        return self.config.extra.get("schema", {})

    def _make_request(
        self, method: str, endpoint: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Override in concrete implementations for actual HTTP calls."""
        raise NotImplementedError("Subclass must implement _make_request")


class FileConnector(BaseConnector):
    """
    Base class for file-based connectors such as SFTP and blob storage.
    Handles listing and reading files from landing sources.
    """

    def __init__(self, config: ConnectorConfig, credentials: str):
        super().__init__(config)
        self.credentials = credentials

    def validate_connection(self) -> bool:
        """Check that the remote file source can be listed."""
        try:
            self._list_files()
            self._is_connected = True
            logger.info(f"Connection validated for {self.config.name}")
            return True
        except Exception as e:
            logger.error(f"File connection failed for {self.config.name}: {e}")
            return False

    def extract(self, query_or_endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """Read files from the supplied path or from the configured landing path."""
        if not self._is_connected:
            self.validate_connection()
        target_path = query_or_endpoint or self.config.landing_path
        return self._read_files(target_path, params)

    def get_schema(self) -> dict[str, Any]:
        """Return expected schema for landed files when it is configured."""
        return self.config.extra.get("schema", {})

    def _list_files(self) -> list[str]:
        """Override in concrete implementations for remote file listing."""
        raise NotImplementedError("Subclass must implement _list_files")

    def _read_files(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Override in concrete implementations for actual file reads."""
        raise NotImplementedError("Subclass must implement _read_files")
