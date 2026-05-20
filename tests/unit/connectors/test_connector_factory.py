"""
TDD Tests - Connector Factory
Write tests FIRST, then implement. Tests validate:
- Registry loading and parsing
- Active connector resolution
- Factory instantiation for all connector types
- DB swap (postgres ↔ sqlserver) works transparently
"""
import pytest
import sys
from unittest.mock import MagicMock
from src.connectors.connector_factory import (
    load_registry,
    get_active_connectors,
    create_connector,
    CONNECTOR_CLASS_MAP,
)
from src.connectors.base_connector import ConnectorConfig, DatabaseConnector, APIConnector
from src.connectors.access_connector import MicrosoftAccessConnector


class TestRegistryLoading:
    """TDD: Registry YAML loads correctly and has expected structure."""

    def test_registry_loads_without_error(self):
        registry = load_registry()
        assert registry is not None
        assert isinstance(registry, dict)

    def test_registry_has_required_sections(self):
        registry = load_registry()
        required_sections = ["databases", "file_storage", "ecommerce", "analytics", "medallion"]
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

    def test_switching_db_to_access(self):
        registry = load_registry()
        registry["databases"]["active"] = "access"
        active = get_active_connectors(registry)
        assert "db_access" in active
        assert active["db_access"].landing_path == "raw/database/access"

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

    def test_create_access_connector(self):
        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="test-secret",
            landing_path="raw/db/access",
            extra={"driver": "Microsoft Access Driver (*.mdb, *.accdb)"},
        )
        connector = create_connector(config, "/tmp/marketing.accdb")
        assert isinstance(connector, MicrosoftAccessConnector)
        assert "DBQ=/tmp/marketing.accdb" in connector.connection_string

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
            assert issubclass(cls, (DatabaseConnector, APIConnector)), (
                f"Connector '{name}' class must inherit DatabaseConnector or APIConnector"
            )


class TestMicrosoftAccessConnector:
    def test_builds_connection_string_from_access_file(self):
        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-secret",
            landing_path="raw/database/access",
            extra={"driver": "Microsoft Access Driver (*.mdb, *.accdb)", "readonly": True},
        )

        connector = MicrosoftAccessConnector(config, "/data/marketing.accdb")

        assert connector.connection_string.startswith("Driver={Microsoft Access Driver (*.mdb, *.accdb)};")
        assert "DBQ=/data/marketing.accdb" in connector.connection_string
        assert "ReadOnly=1" in connector.connection_string

    def test_preserves_explicit_connection_string(self):
        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-secret",
            landing_path="raw/database/access",
        )

        connector = MicrosoftAccessConnector(
            config,
            "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=/data/marketing.accdb;",
        )

        assert connector.connection_string == "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=/data/marketing.accdb;"

    def test_extract_executes_sql_and_maps_rows(self, monkeypatch):
        executed = {}

        class FakeCursor:
            description = [("campaign_id",), ("spend",)]

            def execute(self, query, *params):
                executed["query"] = query
                executed["params"] = params
                return self

            def fetchall(self):
                return [(101, 99.5)]

        class FakeConnection:
            def cursor(self):
                return FakeCursor()

        fake_pyodbc = MagicMock()
        fake_pyodbc.connect.return_value = FakeConnection()
        monkeypatch.setitem(sys.modules, "pyodbc", fake_pyodbc)

        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-secret",
            landing_path="raw/database/access",
        )
        connector = MicrosoftAccessConnector(config, "/data/marketing.accdb")

        result = connector.extract("SELECT campaign_id, spend FROM campaigns WHERE channel = ?", ("email",))

        assert result == [{"campaign_id": 101, "spend": 99.5}]
        assert executed["query"] == "SELECT campaign_id, spend FROM campaigns WHERE channel = ?"
        assert executed["params"] == ("email",)

    def test_get_schema_returns_tables_and_columns(self, monkeypatch):
        class FakeTable:
            def __init__(self, table_name):
                self.table_name = table_name

        class FakeColumn:
            def __init__(self, column_name, type_name, nullable):
                self.column_name = column_name
                self.type_name = type_name
                self.nullable = nullable

        class FakeCursor:
            description = None

            def __init__(self, mode):
                self.mode = mode

            def tables(self, tableType=None):
                assert tableType == "TABLE"
                return [FakeTable("Campaigns"), FakeTable("MSysObjects")]

            def columns(self, table=None):
                if table == "Campaigns":
                    return [
                        FakeColumn("CampaignID", "INTEGER", False),
                        FakeColumn("Spend", "DOUBLE", True),
                    ]
                return []

        class FakeConnection:
            def __init__(self):
                self.calls = 0

            def cursor(self):
                self.calls += 1
                return FakeCursor(self.calls)

        fake_pyodbc = MagicMock()
        fake_pyodbc.connect.return_value = FakeConnection()
        monkeypatch.setitem(sys.modules, "pyodbc", fake_pyodbc)

        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-secret",
            landing_path="raw/database/access",
        )
        connector = MicrosoftAccessConnector(config, "/data/marketing.accdb")

        schema = connector.get_schema()

        assert schema == {
            "tables": [
                {
                    "table_name": "Campaigns",
                    "columns": [
                        {"column_name": "CampaignID", "data_type": "INTEGER", "nullable": False},
                        {"column_name": "Spend", "data_type": "DOUBLE", "nullable": True},
                    ],
                }
            ]
        }
