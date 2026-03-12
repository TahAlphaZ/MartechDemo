"""
Connector Factory - Dynamically instantiates connectors from registry config.
Subagents: Register new connector classes in CONNECTOR_CLASS_MAP.
"""
import yaml
from pathlib import Path
from typing import Dict, Any
from src.connectors.base_connector import (
    BaseConnector,
    ConnectorConfig,
    DatabaseConnector,
    APIConnector,
)
from src.connectors.salesforce_connector import SalesforceConnector

# ============================================================
# CONNECTOR CLASS MAP
# Subagents: Add your connector class here after implementing it.
# Format: "registry_key": ConnectorClassName
# ============================================================
CONNECTOR_CLASS_MAP: Dict[str, type] = {
    # Database connectors
    "postgres": DatabaseConnector,
    "sqlserver": DatabaseConnector,
    "mysql": DatabaseConnector,
    # Ecommerce connectors (override with specific classes when ready)
    "shopify": APIConnector,
    "magento": APIConnector,
    "woocommerce": APIConnector,
    # Analytics connectors (override with specific classes when ready)
    "google_analytics": APIConnector,
    "adobe_analytics": APIConnector,
    "mixpanel": APIConnector,
    # CRM connectors
    "salesforce": SalesforceConnector,
}


def load_registry(config_path: str = None) -> Dict[str, Any]:
    """Load connector registry YAML config."""
    if config_path is None:
        config_path = str(
            Path(__file__).parent.parent.parent / "config" / "connectors" / "connector_registry.yaml"
        )
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_active_connectors(registry: Dict[str, Any]) -> Dict[str, ConnectorConfig]:
    """
    Parse registry and return only active connector configs.
    This is what the pipeline uses to determine what to ingest.
    """
    active = {}

    # Database connector
    db_section = registry.get("databases", {})
    active_db = db_section.get("active", "postgres")
    db_config = db_section.get("connectors", {}).get(active_db, {})
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
    fs_config = fs_section.get("connectors", {}).get(active_fs, {})
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
        ecom_config = ecom_section.get("connectors", {}).get(ecom_name, {})
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
        analytics_config = analytics_section.get("connectors", {}).get(analytics_name, {})
        active[f"analytics_{analytics_name}"] = ConnectorConfig(
            name=analytics_name,
            connector_type="analytics",
            keyvault_secret=analytics_config.get("keyvault_secret", ""),
            landing_path=analytics_config.get("landing_path", f"raw/analytics/{analytics_name}"),
            extra=analytics_config,
        )

    # Salesforce connector
    sf_section = registry.get("salesforce", {})
    for sf_name in sf_section.get("active", []):
        sf_config = sf_section.get("connectors", {}).get(sf_name, {})
        active[f"crm_{sf_name}"] = ConnectorConfig(
            name=sf_name,
            connector_type="api",
            keyvault_secret=sf_config.get("keyvault_secret", ""),
            landing_path=sf_config.get("landing_path", f"raw/{sf_name}"),
            extra=sf_config,
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
    elif issubclass(connector_class, APIConnector):
        return connector_class(config, api_key=secret_value)
    elif issubclass(connector_class, SalesforceConnector):
        # Salesforce auth typically uses multiple secrets; this connector is stubbed and
        # does not require secret_value for instantiation.
        return connector_class(config)
    else:
        raise ValueError(f"Unknown connector base class for '{config.name}'")
