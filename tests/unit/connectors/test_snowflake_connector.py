"""
TDD Tests - Snowflake Connector
Tests for the Snowflake data warehouse connector.
Covers: connection validation, query execution, schema introspection, data extraction.
"""
import pytest
import json
from unittest.mock import Mock, MagicMock, patch, call
from src.connectors.snowflake_connector import SnowflakeConnector
from src.connectors.base_connector import ConnectorConfig, DatabaseConnector


class TestSnowflakeConnectorInitialization:
    """Test Snowflake connector setup and initialization."""

    def test_snowflake_connector_inherits_from_database_connector(self):
        """Verify SnowflakeConnector is a proper DatabaseConnector subclass."""
        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="snowflake-params",
            landing_path="raw/database/snowflake",
        )
        params = {
            "account": "xy12345.us-east-1",
            "user": "analytics_user",
            "password": "secret",
            "warehouse": "ANALYTICS",
            "database": "MARKETING",
            "schema": "RAW",
            "role": "TRANSFORMER",
        }
        connector = SnowflakeConnector(config, params)
        assert isinstance(connector, DatabaseConnector)

    def test_snowflake_connector_stores_connection_params(self):
        """Verify connection parameters are stored correctly."""
        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/snowflake",
        )
        params = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "warehouse": "TEST_WH",
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "role": "TEST_ROLE",
        }
        connector = SnowflakeConnector(config, params)
        assert connector.account == "test_account"
        assert connector.user == "test_user"
        assert connector.warehouse == "TEST_WH"
        assert connector.database == "TEST_DB"
        assert connector.schema == "TEST_SCHEMA"
        assert connector.role == "TEST_ROLE"

    def test_snowflake_connector_default_schema(self):
        """Verify default schema is PUBLIC when not provided."""
        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/snowflake",
        )
        params = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "warehouse": "TEST_WH",
            "database": "TEST_DB",
        }
        connector = SnowflakeConnector(config, params)
        assert connector.schema == "PUBLIC"
        assert connector.role == "TRANSFORMER"


class TestSnowflakeConnectionValidation:
    """Test connection validation and health checks."""

    @patch("src.connectors.snowflake_connector.snowflake.connector.connect")
    def test_validate_connection_success(self, mock_connect):
        """Test successful connection validation."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"TEST_VALUE": 1}
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/snowflake",
            extra={"test_query": "SELECT 1 as test_value"},
        )
        connector = SnowflakeConnector(config, {"account": "test", "user": "test", "password": "test"})
        result = connector.validate_connection()

        assert result is True
        assert connector._is_connected is True
        mock_connect.assert_called_once()

    @patch("src.connectors.snowflake_connector.snowflake.connector.connect")
    def test_validate_connection_failure(self, mock_connect):
        """Test failed connection validation."""
        mock_connect.side_effect = Exception("Connection refused")

        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/snowflake",
        )
        connector = SnowflakeConnector(config, {"account": "test", "user": "test", "password": "test"})
        result = connector.validate_connection()

        assert result is False
        assert connector._is_connected is False

    @patch("src.connectors.snowflake_connector.snowflake.connector.connect")
    def test_validate_connection_uses_test_query_from_config(self, mock_connect):
        """Verify custom test query is used when provided."""
        custom_test_query = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES"
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"COUNT": 5}
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/snowflake",
            extra={"test_query": custom_test_query},
        )
        connector = SnowflakeConnector(config, {"account": "test", "user": "test", "password": "test"})
        connector.validate_connection()

        mock_cursor.execute.assert_called_with(custom_test_query)


class TestSnowflakeQueryExecution:
    """Test SQL query execution and result handling."""

    @patch("src.connectors.snowflake_connector.snowflake.connector.connect")
    def test_extract_executes_query(self, mock_connect):
        """Test that extract executes SQL query correctly."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {"ID": 1, "NAME": "Campaign A"},
            {"ID": 2, "NAME": "Campaign B"},
        ]
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/snowflake",
        )
        connector = SnowflakeConnector(config, {"account": "test", "user": "test", "password": "test"})
        connector._is_connected = True

        query = "SELECT id, name FROM marketing.campaigns"
        result = connector.extract(query)

        assert len(result) == 2
        assert result[0]["NAME"] == "Campaign A"
        mock_cursor.execute.assert_called_with(query)

    @patch("src.connectors.snowflake_connector.snowflake.connector.connect")
    def test_extract_with_parameters(self, mock_connect):
        """Test query execution with parameters."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [{"ID": 1, "NAME": "Campaign A"}]
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/snowflake",
        )
        connector = SnowflakeConnector(config, {"account": "test", "user": "test", "password": "test"})
        connector._is_connected = True

        query = "SELECT id, name FROM campaigns WHERE status = %s"
        params = ["active"]
        result = connector.extract(query, params)

        assert len(result) == 1
        mock_cursor.execute.assert_called_with(query, params)

    @patch("src.connectors.snowflake_connector.snowflake.connector.connect")
    def test_extract_handles_empty_results(self, mock_connect):
        """Test extraction handles empty result sets."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/snowflake",
        )
        connector = SnowflakeConnector(config, {"account": "test", "user": "test", "password": "test"})
        connector._is_connected = True

        result = connector.extract("SELECT * FROM empty_table")
        assert result == []

    @patch("src.connectors.snowflake_connector.snowflake.connector.connect")
    def test_extract_raises_on_query_error(self, mock_connect):
        """Test extraction raises exception on SQL error."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("SQL Syntax Error")
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/snowflake",
        )
        connector = SnowflakeConnector(config, {"account": "test", "user": "test", "password": "test"})
        connector._is_connected = True

        with pytest.raises(Exception, match="SQL Syntax Error"):
            connector.extract("SELECT * FROM invalid_table")


class TestSnowflakeSchemaIntrospection:
    """Test schema discovery and metadata retrieval."""

    @patch("src.connectors.snowflake_connector.snowflake.connector.connect")
    def test_get_schema_returns_column_metadata(self, mock_connect):
        """Test get_schema returns table and column information."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {
                "TABLE_NAME": "CAMPAIGNS",
                "COLUMN_NAME": "ID",
                "DATA_TYPE": "NUMBER",
                "ORDINAL_POSITION": 1,
                "IS_NULLABLE": "NO",
            },
            {
                "TABLE_NAME": "CAMPAIGNS",
                "COLUMN_NAME": "NAME",
                "DATA_TYPE": "VARCHAR",
                "ORDINAL_POSITION": 2,
                "IS_NULLABLE": "YES",
            },
        ]
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/snowflake",
        )
        connector = SnowflakeConnector(config, {"account": "test", "user": "test", "password": "test", "schema": "RAW"})
        schema = connector.get_schema()

        assert len(schema) == 2
        assert schema[0]["TABLE_NAME"] == "CAMPAIGNS"
        assert schema[0]["DATA_TYPE"] == "NUMBER"

    @patch("src.connectors.snowflake_connector.snowflake.connector.connect")
    def test_get_available_tables_returns_table_list(self, mock_connect):
        """Test get_available_tables returns list of table names."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {"TABLE_NAME": "CAMPAIGNS"},
            {"TABLE_NAME": "CUSTOMERS"},
            {"TABLE_NAME": "PRODUCTS"},
        ]
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/snowflake",
        )
        connector = SnowflakeConnector(config, {"account": "test", "user": "test", "password": "test"})
        tables = connector.get_available_tables()

        assert len(tables) == 3
        assert "CAMPAIGNS" in tables
        assert "CUSTOMERS" in tables


class TestSnowflakeTableOperations:
    """Test table-level operations like sampling and counting."""

    @patch("src.connectors.snowflake_connector.snowflake.connector.connect")
    def test_get_table_sample(self, mock_connect):
        """Test sampling rows from a table."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {"ID": 1, "NAME": "Campaign A"},
            {"ID": 2, "NAME": "Campaign B"},
        ]
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/snowflake",
        )
        connector = SnowflakeConnector(config, {"account": "test", "user": "test", "password": "test"})
        connector._is_connected = True

        sample = connector.get_table_sample("CAMPAIGNS", limit=100)

        assert len(sample) == 2
        mock_cursor.execute.assert_called_with("SELECT * FROM CAMPAIGNS LIMIT 100")

    @patch("src.connectors.snowflake_connector.snowflake.connector.connect")
    def test_get_table_count(self, mock_connect):
        """Test getting row count from a table."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [{"ROW_COUNT": 1542}]
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/snowflake",
        )
        connector = SnowflakeConnector(config, {"account": "test", "user": "test", "password": "test"})
        connector._is_connected = True

        count = connector.get_table_count("CAMPAIGNS")

        assert count == 1542
        mock_cursor.execute.assert_called_with("SELECT COUNT(*) as row_count FROM CAMPAIGNS")


class TestSnowflakeConnectionManagement:
    """Test connection lifecycle and resource cleanup."""

    @patch("src.connectors.snowflake_connector.snowflake.connector.connect")
    def test_close_connection(self, mock_connect):
        """Test closing connection."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/snowflake",
        )
        connector = SnowflakeConnector(config, {"account": "test", "user": "test", "password": "test"})
        connector._connection = mock_connection

        connector.close()

        mock_connection.close.assert_called_once()
        assert connector._connection is None

    @patch("src.connectors.snowflake_connector.snowflake.connector.connect")
    def test_get_metadata(self, mock_connect):
        """Test metadata is attached to extractions."""
        config = ConnectorConfig(
            name="snowflake",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/database/snowflake",
        )
        connector = SnowflakeConnector(config, {"account": "test", "user": "test", "password": "test"})
        metadata = connector.get_metadata()

        assert metadata["_source_system"] == "snowflake"
        assert metadata["_connector_type"] == "database"
        assert metadata["_landing_path"] == "raw/database/snowflake"
