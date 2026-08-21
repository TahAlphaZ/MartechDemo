"""
Unit Tests - Databricks Connector
TDD tests for Databricks dataset extraction.
Tests validate:
- Connection validation with PAT tokens
- SQL query execution and result formatting
- Schema introspection from Unity Catalog
- Table listing and existence checks
- Row count operations
- Connection lifecycle management
"""
import pytest
from unittest.mock import patch, MagicMock, Mock
from src.connectors.databricks_connector import DatabricksConnector
from src.connectors.base_connector import ConnectorConfig


class TestDatabricksConnectorInitialization:
    """TDD: Connector initializes with proper configuration."""

    def test_connector_initializes_with_config(self):
        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="databricks-pat",
            landing_path="raw/databricks",
            extra={
                "workspace_url": "https://test.databricks.com",
                "http_path": "/sql/1.0/warehouses/abc123",
                "catalog": "hive_metastore",
                "schema": "default",
            },
        )
        connector = DatabricksConnector(config, access_token="test-token")
        assert connector.config.name == "databricks"
        assert connector.access_token == "test-token"
        assert connector.workspace_url == "https://test.databricks.com"
        assert connector.http_path == "/sql/1.0/warehouses/abc123"
        assert connector.catalog == "hive_metastore"
        assert connector.schema == "default"

    def test_connector_with_default_values(self):
        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="databricks-pat",
            landing_path="raw/databricks",
            extra={},
        )
        connector = DatabricksConnector(config, access_token="test-token")
        assert connector.catalog == "hive_metastore"
        assert connector.schema == "default"
        assert connector.http_path == "/sql/1.0/warehouses/default"

    def test_workspace_url_trailing_slash_removed(self):
        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com/"},
        )
        connector = DatabricksConnector(config, access_token="token")
        assert connector.workspace_url == "https://test.databricks.com"


class TestWorkspaceUrlParsing:
    """TDD: Workspace URL parsing extracts hostname correctly."""

    def test_parse_workspace_url_https(self):
        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="token")
        hostname = connector._parse_workspace_url()
        assert hostname == "test.databricks.com"

    def test_parse_workspace_url_http(self):
        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "http://test.databricks.com/path"},
        )
        connector = DatabricksConnector(config, access_token="token")
        hostname = connector._parse_workspace_url()
        assert hostname == "test.databricks.com"

    def test_parse_workspace_url_no_protocol(self):
        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="token")
        hostname = connector._parse_workspace_url()
        assert hostname == "test.databricks.com"


class TestConnectionValidation:
    """TDD: Connection validation tests PAT token and workspace connectivity."""

    @patch("src.connectors.databricks_connector.sql")
    def test_validate_connection_success(self, mock_sql):
        mock_client = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_client.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_client

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="test-token")
        result = connector.validate_connection()

        assert result is True
        assert connector._is_connected is True
        mock_cursor.execute.assert_called_once_with("SELECT 1")

    @patch("src.connectors.databricks_connector.sql")
    def test_validate_connection_failure(self, mock_sql):
        mock_sql.connect.side_effect = Exception("Connection failed")

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="test-token")
        result = connector.validate_connection()

        assert result is False
        assert connector._is_connected is False


class TestDataExtraction:
    """TDD: Extract method returns data as list of dicts."""

    @patch("src.connectors.databricks_connector.sql")
    def test_extract_sql_query_with_results(self, mock_sql):
        mock_client = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock column names
        mock_cursor.description = [("id",), ("name",), ("email",)]
        # Mock rows
        mock_cursor.fetchall.return_value = [
            (1, "John", "john@example.com"),
            (2, "Jane", "jane@example.com"),
        ]
        
        mock_client.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_client

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="test-token")
        connector._is_connected = True

        results = connector.extract("SELECT id, name, email FROM users")

        assert len(results) == 2
        assert results[0] == {"id": 1, "name": "John", "email": "john@example.com"}
        assert results[1] == {"id": 2, "name": "Jane", "email": "jane@example.com"}

    @patch("src.connectors.databricks_connector.sql")
    def test_extract_table_reference(self, mock_sql):
        mock_client = MagicMock()
        mock_cursor = MagicMock()
        
        mock_cursor.description = [("id",), ("value",)]
        mock_cursor.fetchall.return_value = [(1, "test")]
        
        mock_client.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_client

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={
                "workspace_url": "https://test.databricks.com",
                "catalog": "uc_catalog",
                "schema": "prod_schema",
            },
        )
        connector = DatabricksConnector(config, access_token="test-token")
        connector._is_connected = True

        results = connector.extract("my_table")

        # Verify it built qualified table name
        mock_cursor.execute.assert_called_once_with("SELECT * FROM uc_catalog.prod_schema.my_table")
        assert len(results) == 1

    @patch("src.connectors.databricks_connector.sql")
    def test_extract_with_parameters(self, mock_sql):
        mock_client = MagicMock()
        mock_cursor = MagicMock()
        
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "test")]
        
        mock_client.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_client

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="test-token")
        connector._is_connected = True

        params = {"id": 5}
        results = connector.extract("SELECT * FROM users WHERE id = ?", params)

        mock_cursor.execute.assert_called_once_with(
            "SELECT * FROM users WHERE id = ?", params
        )
        assert len(results) == 1

    @patch("src.connectors.databricks_connector.sql")
    def test_extract_empty_result_set(self, mock_sql):
        mock_client = MagicMock()
        mock_cursor = MagicMock()
        
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = []
        
        mock_client.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_client

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="test-token")
        connector._is_connected = True

        results = connector.extract("SELECT * FROM empty_table")

        assert results == []


class TestSchemaIntrospection:
    """TDD: Get schema from Unity Catalog system tables."""

    @patch("src.connectors.databricks_connector.sql")
    def test_get_schema_success(self, mock_sql):
        mock_client = MagicMock()
        mock_cursor = MagicMock()
        
        mock_cursor.description = [("table_name",), ("columns",)]
        mock_cursor.fetchall.return_value = [
            ("users", [{"column_name": "id", "data_type": "int"}, 
                      {"column_name": "name", "data_type": "string"}]),
            ("orders", [{"column_name": "order_id", "data_type": "int"}]),
        ]
        
        mock_client.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_client

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={
                "workspace_url": "https://test.databricks.com",
                "catalog": "uc",
                "schema": "prod",
            },
        )
        connector = DatabricksConnector(config, access_token="test-token")
        connector._is_connected = True

        schema = connector.get_schema()

        assert "users" in schema
        assert "orders" in schema
        assert len(schema) == 2

    @patch("src.connectors.databricks_connector.sql")
    def test_get_schema_empty_catalog(self, mock_sql):
        mock_client = MagicMock()
        mock_cursor = MagicMock()
        
        mock_cursor.description = [("table_name",), ("columns",)]
        mock_cursor.fetchall.return_value = []
        
        mock_client.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_client

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="test-token")
        connector._is_connected = True

        schema = connector.get_schema()

        assert schema == {}

    @patch("src.connectors.databricks_connector.sql")
    def test_get_schema_on_error_returns_empty(self, mock_sql):
        mock_sql.connect.side_effect = Exception("Query failed")

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="test-token")

        schema = connector.get_schema()

        assert schema == {}


class TestTableOperations:
    """TDD: List tables, check existence, count rows."""

    @patch("src.connectors.databricks_connector.sql")
    def test_list_tables(self, mock_sql):
        mock_client = MagicMock()
        mock_cursor = MagicMock()
        
        mock_cursor.fetchall.return_value = [
            ("users",),
            ("orders",),
            ("products",),
        ]
        
        mock_client.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_client

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={
                "workspace_url": "https://test.databricks.com",
                "catalog": "uc",
                "schema": "prod",
            },
        )
        connector = DatabricksConnector(config, access_token="test-token")
        connector._is_connected = True

        tables = connector.list_tables()

        assert tables == ["users", "orders", "products"]
        mock_cursor.execute.assert_called_once_with("SHOW TABLES IN uc.prod")

    @patch("src.connectors.databricks_connector.sql")
    def test_table_exists_true(self, mock_sql):
        mock_client = MagicMock()
        mock_cursor = MagicMock()
        
        mock_cursor.fetchall.return_value = [(100,)]
        
        mock_client.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_client

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="test-token")
        connector._is_connected = True

        exists = connector.table_exists("users")

        assert exists is True

    @patch("src.connectors.databricks_connector.sql")
    def test_table_exists_false(self, mock_sql):
        mock_client = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Table not found")
        
        mock_client.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_client

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="test-token")
        connector._is_connected = True

        exists = connector.table_exists("nonexistent")

        assert exists is False

    @patch("src.connectors.databricks_connector.sql")
    def test_get_row_count(self, mock_sql):
        mock_client = MagicMock()
        mock_cursor = MagicMock()
        
        mock_cursor.description = [("row_count",)]
        mock_cursor.fetchall.return_value = [(1000,)]
        
        mock_client.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_client

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="test-token")
        connector._is_connected = True

        count = connector.get_row_count("users")

        assert count == 1000

    @patch("src.connectors.databricks_connector.sql")
    def test_get_row_count_empty_table(self, mock_sql):
        mock_client = MagicMock()
        mock_cursor = MagicMock()
        
        mock_cursor.description = [("row_count",)]
        mock_cursor.fetchall.return_value = [(0,)]
        
        mock_client.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_client

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="test-token")
        connector._is_connected = True

        count = connector.get_row_count("empty_table")

        assert count == 0


class TestConnectionLifecycle:
    """TDD: Connection open, close, and cleanup."""

    @patch("src.connectors.databricks_connector.sql")
    def test_close_connection(self, mock_sql):
        mock_client = MagicMock()
        mock_cursor = MagicMock()
        
        mock_client.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_client

        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="test-token")
        connector.validate_connection()

        connector.close()

        mock_cursor.close.assert_called_once()
        mock_client.close.assert_called_once()
        assert connector._is_connected is False

    @patch("src.connectors.databricks_connector.sql")
    def test_close_without_connection(self, mock_sql):
        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="test-token")

        # Should not raise exception
        connector.close()
        assert connector._is_connected is False


class TestGetMetadata:
    """TDD: Metadata attached to extractions."""

    def test_get_metadata(self):
        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="databricks-pat",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = DatabricksConnector(config, access_token="test-token")
        metadata = connector.get_metadata()

        assert metadata["_source_system"] == "databricks"
        assert metadata["_connector_type"] == "analytics"
        assert metadata["_landing_path"] == "raw/databricks"


class TestFactoryIntegration:
    """TDD: Databricks connector works with factory."""

    def test_databricks_in_connector_class_map(self):
        from src.connectors.connector_factory import CONNECTOR_CLASS_MAP
        assert "databricks" in CONNECTOR_CLASS_MAP
        assert CONNECTOR_CLASS_MAP["databricks"] is DatabricksConnector

    @patch("src.connectors.databricks_connector.sql")
    def test_factory_creates_databricks_connector(self, mock_sql):
        from src.connectors.connector_factory import create_connector
        
        config = ConnectorConfig(
            name="databricks",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/databricks",
            extra={"workspace_url": "https://test.databricks.com"},
        )
        connector = create_connector(config, "test-pat-token")

        assert isinstance(connector, DatabricksConnector)
        assert connector.access_token == "test-pat-token"
