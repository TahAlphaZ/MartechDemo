"""
Connector Factory - Dynamically instantiates connectors from registry config.
Subagents: Register new connector classes in CONNECTOR_CLASS_MAP.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from src.connectors.base_connector import (
    APIConnector,
    BaseConnector,
    ConnectorConfig,
    DatabaseConnector,
    FileConnector,
)
from src.connectors.google_analytics_connector import GoogleAnalyticsConnector

# ============================================================
# CONNECTOR CLASS MAP
# Subagents: Add your connector class here after implementing it.
# Format: "registry_key": ConnectorClassName
# ============================================================
CONNECTOR_CLASS_MAP: dict[str, type[BaseConnector]] = {
    # Database connectors
    "postgres": DatabaseConnector,
    "sqlserver": DatabaseConnector,
    "mysql": DatabaseConnector,
    # File connectors
    "sftp": FileConnector,
    "azure_blob": FileConnector,
    # Ecommerce connectors (override with specific classes when ready)
    "shopify": APIConnector,
    "magento": APIConnector,
    "woocommerce": APIConnector,
    # Analytics connectors (override with specific classes when ready)
    "google_analytics": GoogleAnalyticsConnector,
    "adobe_analytics": APIConnector,
    "mixpanel": APIConnector,
}


def load_registry(config_path: str = None) -> dict[str, Any]:
    """Load connector registry YAML config."""
    if config_path is None:
        config_path = str(
            Path(__file__).parent.parent.parent / "config" / "connectors" / "connector_registry.yaml"
        )
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _get_named_connector_config(
    section: dict[str, Any], connector_name: str, section_name: str
) -> dict[str, Any]:
    """Resolve a connector config and fail loudly when the registry is inconsistent."""
    config = section.get("connectors", {}).get(connector_name)
    if config is None:
        raise ValueError(
            f"Active connector '{connector_name}' is not defined in registry section '{section_name}'"
        )
    return config


def get_active_connectors(registry: dict[str, Any]) -> dict[str, ConnectorConfig]:
    """
    Parse registry and return only active connector configs.
    This is what the pipeline uses to determine what to ingest.
    """
    active = {}

    # Database connector
    db_section = registry.get("databases", {})
    active_db = db_section.get("active", "postgres")
    db_config = _get_named_connector_config(db_section, active_db, "databases")
    active[f"db_{active_db}"] = ConnectorConfig(
        name=active_db,
        connector_type="database",
        keyvault_secret=db_config.get("keyvault_secret", ""),
        landing_path=f"raw/database/{active_db}",
        extra=db_config,
    )

    # File storage connector
    fs_section = registry.get("file_storage", {})
    active_fs = fs_section.get("active", "sftp")
    fs_config = _get_named_connector_config(fs_section, active_fs, "file_storage")
    active[f"file_{active_fs}"] = ConnectorConfig(
        name=active_fs,
        connector_type="file",
        keyvault_secret=fs_config.get("keyvault_secret", ""),
        landing_path=fs_config.get("landing_path", "raw/files"),
        extra=fs_config,
    )

    # Ecommerce connectors
    ecom_section = registry.get("ecommerce", {})
    for ecom_name in ecom_section.get("active", []):
        ecom_config = _get_named_connector_config(ecom_section, ecom_name, "ecommerce")
        active[f"ecom_{ecom_name}"] = ConnectorConfig(
            name=ecom_name,
            connector_type="api",
            keyvault_secret=ecom_config.get("keyvault_secret", ""),
            landing_path=ecom_config.get("landing_path", f"raw/ecommerce/{ecom_name}"),
            extra=ecom_config,
        )

    # Analytics connectors
    analytics_section = registry.get("analytics", {})
    for analytics_name in analytics_section.get("active", []):
        analytics_config = _get_named_connector_config(analytics_section, analytics_name, "analytics")
        active[f"analytics_{analytics_name}"] = ConnectorConfig(
            name=analytics_name,
            connector_type="analytics",
            keyvault_secret=analytics_config.get("keyvault_secret", ""),
            landing_path=analytics_config.get("landing_path", f"raw/analytics/{analytics_name}"),
            extra=analytics_config,
        )

    return active


def create_connector(config: ConnectorConfig, secret_value: str) -> BaseConnector:
    """
    Factory method to create connector instance from config.
    Secret is resolved from Key Vault at runtime.
    """
    connector_class = CONNECTOR_CLASS_MAP.get(config.name)
    if connector_class is None:
        raise ValueError(
            f"No connector class registered for '{config.name}'. "
            f"Register it in CONNECTOR_CLASS_MAP in connector_factory.py"
        )

    if issubclass(connector_class, DatabaseConnector):
        return connector_class(config, connection_string=secret_value)
    if issubclass(connector_class, FileConnector):
        return connector_class(config, credentials=secret_value)
    if issubclass(connector_class, APIConnector):
        return connector_class(config, api_key=secret_value)

    raise ValueError(f"Unknown connector base class for '{config.name}'")
