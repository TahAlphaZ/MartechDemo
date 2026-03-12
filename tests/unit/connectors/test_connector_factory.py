"""
TDD Tests - Connector Factory
Write tests FIRST, then implement. Tests validate:
- Registry loading and parsing
- Active connector resolution
- Factory instantiation for all connector types
- DB swap (postgres ↔ sqlserver) works transparently
"""
import pytest
from unittest.mock import patch, MagicMock
from src.connectors.connector_factory import (
    load_registry,
    get_active_connectors,
    create_connector,
    CONNECTOR_CLASS_MAP,
)
from src.connectors.base_connector import ConnectorConfig, DatabaseConnector, APIConnector
from src.connectors.salesforce_connector import SalesforceConnector


class TestRegistryLoading:
    """TDD: Registry YAML loads correctly and has expected structure."""

    def test_registry_loads_without_error(self):
        registry = load_registry()
        assert registry is not None
        assert isinstance(registry, dict)

    def test_registry_has_required_sections(self):
        registry = load_registry()
        required_sections = ["databases", "file_storage", "ecommerce", "analytics", "medallion", "salesforce"]
        for section in required_sections:
            assert section in registry, f"Missing required section: {section}"

    def test_each_db_connector_has_required_fields(self):
        registry = load_registry()
        required_fields = ["driver", "arm_linked_service_type", "keyvault_secret", "default_port"]
        for db_name, db_config in registry["databases"]["connectors"].items():
            for field in required_fields:
                assert field in db_config, f"DB '{db_name}' missing field: {field}"

    def test_each_ecommerce_connector_has_endpoints(self):
        registry = load_registry()
        for name, config in registry["ecommerce"]["connectors"].items():
            assert "endpoints" in config, f"Ecommerce '{name}' missing endpoints"
            assert "orders" in config["endpoints"], f"Ecommerce '{name}' missing orders endpoint"

    def test_salesforce_registry_has_salesforce_connector_entry(self):
        registry = load_registry()
        assert "salesforce" in registry
        assert "connectors" in registry["salesforce"]
        assert "salesforce" in registry["salesforce"]["connectors"]


class TestActiveConnectorResolution:
    """TDD: Only active connectors are returned for pipeline execution."""

    def test_default_active_db_is_postgres(self):
        registry = load_registry()
        active = get_active_connectors(registry)
        assert "db_postgres" in active

    def test_switching_db_to_sqlserver(self):
        registry = load_registry()
        registry["databases"]["active"] = "sqlserver"
        active = get_active_connectors(registry)
        assert "db_sqlserver" in active
        assert "db_postgres" not in active

    def test_no_analytics_active_by_default(self):
        registry = load_registry()
        active = get_active_connectors(registry)
        analytics_keys = [k for k in active if k.startswith("analytics_")]
        assert len(analytics_keys) == 0

    def test_enabling_google_analytics(self):
        registry = load_registry()
        registry["analytics"]["active"] = ["google_analytics"]
        active = get_active_connectors(registry)
        assert "analytics_google_analytics" in active

    def test_enabling_multiple_analytics(self):
        registry = load_registry()
        registry["analytics"]["active"] = ["google_analytics", "adobe_analytics"]
        active = get_active_connectors(registry)
        assert "analytics_google_analytics" in active
        assert "analytics_adobe_analytics" in active

    def test_active_connectors_have_valid_config(self):
        registry = load_registry()
        active = get_active_connectors(registry)
        for key, config in active.items():
            assert isinstance(config, ConnectorConfig)
            assert config.name != ""
            assert config.connector_type in ("database", "file", "api", "analytics")

    def test_enabling_salesforce(self):
        registry = load_registry()
        registry["salesforce"]["active"] = ["salesforce"]
        active = get_active_connectors(registry)
        assert "crm_salesforce" in active


class TestConnectorFactory:
    """TDD: Factory creates correct connector types."""

    def test_create_database_connector(self):
        config = ConnectorConfig(
            name="postgres",
            connector_type="database",
            keyvault_secret="test-secret",
            landing_path="raw/db/postgres",
        )
        connector = create_connector(config, "postgresql://test:test@localhost/db")
        assert isinstance(connector, DatabaseConnector)

    def test_create_api_connector(self):
        config = ConnectorConfig(
            name="shopify",
            connector_type="api",
            keyvault_secret="test-secret",
            landing_path="raw/ecommerce/shopify",
            extra={"base_url": "https://test.myshopify.com"},
        )
        connector = create_connector(config, "test-api-key")
        assert isinstance(connector, APIConnector)

    def test_create_salesforce_connector(self):
        config = ConnectorConfig(
            name="salesforce",
            connector_type="api",
            keyvault_secret="",
            landing_path="raw/salesforce",
            extra={
                "instance_url": "https://example.my.salesforce.com",
                "api_version": "v60.0",
            },
        )
        connector = create_connector(config, "")
        assert isinstance(connector, SalesforceConnector)

    def test_unknown_connector_raises(self):
        config = ConnectorConfig(
            name="unknown_platform",
            connector_type="api",
            keyvault_secret="test",
            landing_path="raw/unknown",
        )
        with pytest.raises(ValueError, match="No connector class registered"):
            create_connector(config, "key")

    def test_all_registered_connectors_have_classes(self):
        """Ensure every entry in CONNECTOR_CLASS_MAP points to a valid class."""
        for name, cls in CONNECTOR_CLASS_MAP.items():
            assert issubclass(cls, (DatabaseConnector, APIConnector, SalesforceConnector)), (
                f"Connector '{name}' class must inherit DatabaseConnector, APIConnector, or SalesforceConnector"
            )
