"""
Connectors module - Data source connectors for the MarTech pipeline.
"""
from src.connectors.base_connector import (
    BaseConnector,
    ConnectorConfig,
    DatabaseConnector,
    APIConnector,
)
from src.connectors.snowflake_connector import SnowflakeConnector
from src.connectors.connector_factory import (
    CONNECTOR_CLASS_MAP,
    load_registry,
    get_active_connectors,
    create_connector,
)

__all__ = [
    "BaseConnector",
    "ConnectorConfig",
    "DatabaseConnector",
    "APIConnector",
    "SnowflakeConnector",
    "CONNECTOR_CLASS_MAP",
    "load_registry",
    "get_active_connectors",
    "create_connector",
]
