"""
TDD Tests - Access Database Connector
Write tests FIRST, then implement. Tests validate:
- Connection to .mdb/.accdb files via ODBC
- SQL query execution against Access databases
- Schema introspection capabilities
- Error handling for missing files and connection failures
"""
import pytest
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
import pyodbc

from src.connectors.access_connector import AccessConnector
from src.connectors.base_connector import ConnectorConfig, DatabaseConnector


class TestAccessConnectorInitialization:
    """TDD: AccessConnector initializes with proper config."""

    def test_access_connector_inherits_from_database_connector(self):
        """Verify AccessConnector properly extends DatabaseConnector."""
        assert issubclass(AccessConnector, DatabaseConnector)

    def test_access_connector_initialization(self):
        """Test initialization with config."""
        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={
                "driver": "Microsoft Access Driver (*.mdb, *.accdb)",
                "file_path": "/mnt/data/test.accdb",
            },
        )
        connector = AccessConnector(config, connection_string="")
        assert connector.config == config
        assert connector.file_path == "/mnt/data/test.accdb"
        assert connector.driver == "Microsoft Access Driver (*.mdb, *.accdb)"

    def test_access_connector_uses_default_driver(self):
        """Test default driver name when not specified."""
        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={"file_path": "/mnt/data/test.accdb"},
        )
        connector = AccessConnector(config, connection_string="")
        assert "Microsoft Access Driver" in connector.driver


class TestAccessConnectionValidation:
    """TDD: Connection validation works correctly."""

    @patch("pathlib.Path.exists")
    @patch("src.connectors.access_connector.AccessConnector._execute")
    def test_validate_connection_success(self, mock_execute, mock_exists):
        """Test successful connection validation."""
        mock_exists.return_value = True
        mock_execute.return_value = [{"": 1}]

        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={
                "driver": "Microsoft Access Driver (*.mdb, *.accdb)",
                "file_path": "/mnt/data/test.accdb",
                "test_query": "SELECT 1",
            },
        )
        connector = AccessConnector(config, connection_string="")
        result = connector.validate_connection()

        assert result is True
        assert connector._is_connected is True
        mock_execute.assert_called_once_with("SELECT 1")

    @patch("pathlib.Path.exists")
    def test_validate_connection_file_not_found(self, mock_exists):
        """Test validation fails when file doesn't exist."""
        mock_exists.return_value = False

        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={
                "file_path": "/mnt/data/nonexistent.accdb",
            },
        )
        connector = AccessConnector(config, connection_string="")
        result = connector.validate_connection()

        assert result is False

    @patch("pathlib.Path.exists")
    @patch("src.connectors.access_connector.pyodbc.connect")
    def test_validate_connection_odbc_error(self, mock_connect, mock_exists):
        """Test validation fails on ODBC error."""
        mock_exists.return_value = True
        mock_connect.side_effect = pyodbc.Error("Connection failed")

        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={
                "file_path": "/mnt/data/test.accdb",
            },
        )
        connector = AccessConnector(config, connection_string="")
        result = connector.validate_connection()

        assert result is False


class TestAccessDataExtraction:
    """TDD: Extract data from Access database."""

    @patch("src.connectors.access_connector.AccessConnector._execute")
    def test_extract_query_results(self, mock_execute):
        """Test extract returns query results as list of dicts."""
        mock_execute.return_value = [
            {"id": 1, "name": "John"},
            {"id": 2, "name": "Jane"},
        ]

        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={"file_path": "/mnt/data/test.accdb"},
        )
        connector = AccessConnector(config, connection_string="")
        connector._is_connected = True

        result = connector.extract("SELECT * FROM Users")

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["name"] == "John"


class TestAccessSchemaIntrospection:
    """TDD: Schema introspection and metadata."""

    @patch("src.connectors.access_connector.AccessConnector._get_connection")
    def test_get_schema_returns_dict(self, mock_get_connection):
        """Test schema introspection returns table and column info."""
        # Mock cursor and tables
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_get_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Mock tables
        mock_table1 = MagicMock()
        mock_table1.table_name = "Users"
        mock_table2 = MagicMock()
        mock_table2.table_name = "Orders"
        mock_table3 = MagicMock()
        mock_table3.table_name = "~TblProp"  # System table - should be skipped

        mock_cursor.tables.return_value = [mock_table1, mock_table2, mock_table3]

        # Mock columns for Users
        mock_col1 = MagicMock()
        mock_col1.column_name = "UserID"
        mock_col1.type_name = "COUNTER"
        mock_col1.column_size = None
        mock_col1.nullable = False

        mock_col2 = MagicMock()
        mock_col2.column_name = "Name"
        mock_col2.type_name = "TEXT"
        mock_col2.column_size = 255
        mock_col2.nullable = True

        # Mock columns for Orders
        mock_col3 = MagicMock()
        mock_col3.column_name = "OrderID"
        mock_col3.type_name = "COUNTER"
        mock_col3.column_size = None
        mock_col3.nullable = False

        def columns_side_effect(table_name):
            if table_name == "Users":
                return [mock_col1, mock_col2]
            elif table_name == "Orders":
                return [mock_col3]
            return []

        mock_cursor.columns.side_effect = columns_side_effect

        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={"file_path": "/mnt/data/test.accdb"},
        )
        connector = AccessConnector(config, connection_string="")
        schema = connector.get_schema()

        assert isinstance(schema, dict)
        assert "Users" in schema
        assert "Orders" in schema
        assert "~TblProp" not in schema
        assert len(schema["Users"]) == 2
        assert schema["Users"][0]["column_name"] == "UserID"
        assert schema["Users"][0]["data_type"] == "COUNTER"

    @patch("src.connectors.access_connector.AccessConnector._get_connection")
    def test_get_tables_list(self, mock_get_connection):
        """Test retrieving list of table names."""
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_get_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        mock_table1 = MagicMock()
        mock_table1.table_name = "Users"
        mock_table2 = MagicMock()
        mock_table2.table_name = "Orders"
        mock_table3 = MagicMock()
        mock_table3.table_name = "~Sys"  # System table

        mock_cursor.tables.return_value = [mock_table1, mock_table2, mock_table3]

        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={"file_path": "/mnt/data/test.accdb"},
        )
        connector = AccessConnector(config, connection_string="")
        tables = connector.get_tables()

        assert isinstance(tables, list)
        assert "Users" in tables
        assert "Orders" in tables
        assert "~Sys" not in tables

    def test_get_metadata_includes_source_info(self):
        """Test metadata includes source system info."""
        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={"file_path": "/mnt/data/test.accdb"},
        )
        connector = AccessConnector(config, connection_string="")
        metadata = connector.get_metadata()

        assert metadata["_source_system"] == "access"
        assert metadata["_connector_type"] == "database"
        assert metadata["_landing_path"] == "raw/database/access"


class TestAccessSQLExecution:
    """TDD: SQL query execution against Access."""

    @patch("src.connectors.access_connector.AccessConnector._get_connection")
    def test_execute_select_query(self, mock_get_connection):
        """Test executing SELECT query."""
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_get_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Mock query results
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "John"), (2, "Jane")]

        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={"file_path": "/mnt/data/test.accdb"},
        )
        connector = AccessConnector(config, connection_string="")
        results = connector._execute("SELECT * FROM Users")

        assert len(results) == 2
        assert results[0] == {"id": 1, "name": "John"}
        assert results[1] == {"id": 2, "name": "Jane"}
        mock_cursor.execute.assert_called_once_with("SELECT * FROM Users", None)

    @patch("src.connectors.access_connector.AccessConnector._get_connection")
    def test_execute_query_with_params(self, mock_get_connection):
        """Test executing query with parameters."""
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_get_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "John")]

        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={"file_path": "/mnt/data/test.accdb"},
        )
        connector = AccessConnector(config, connection_string="")
        results = connector._execute(
            "SELECT * FROM Users WHERE id = ?",
            {"id": 1},
        )

        assert len(results) == 1
        mock_cursor.execute.assert_called_once_with("SELECT * FROM Users WHERE id = ?", [1])

    @patch("src.connectors.access_connector.AccessConnector._get_connection")
    def test_execute_query_empty_result(self, mock_get_connection):
        """Test handling of empty query results."""
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_get_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = []

        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={"file_path": "/mnt/data/test.accdb"},
        )
        connector = AccessConnector(config, connection_string="")
        results = connector._execute("SELECT * FROM Users WHERE id = ?", {"id": 999})

        assert results == []

    @patch("src.connectors.access_connector.AccessConnector._get_connection")
    def test_execute_query_odbc_error(self, mock_get_connection):
        """Test error handling for ODBC execution errors."""
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_get_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        mock_cursor.execute.side_effect = pyodbc.Error("Query error")

        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={"file_path": "/mnt/data/test.accdb"},
        )
        connector = AccessConnector(config, connection_string="")

        with pytest.raises(pyodbc.Error):
            connector._execute("SELECT * FROM InvalidTable")


class TestAccessConnectionManagement:
    """TDD: Connection lifecycle management."""

    @patch("src.connectors.access_connector.pyodbc.connect")
    def test_connection_reuse(self, mock_connect):
        """Test that connection is reused across multiple queries."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.description = [("id",)]
        mock_cursor.fetchall.return_value = [(1,)]

        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={
                "file_path": "/mnt/data/test.accdb",
            },
        )
        connector = AccessConnector(config, connection_string="")

        # First query
        connector._execute("SELECT 1")
        # Second query
        connector._execute("SELECT 2")

        # Connection should only be created once
        mock_connect.assert_called_once()

    @patch("src.connectors.access_connector.pyodbc.connect")
    def test_close_connection(self, mock_connect):
        """Test closing connection."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = MagicMock()

        config = ConnectorConfig(
            name="access",
            connector_type="database",
            keyvault_secret="access-file-path",
            landing_path="raw/database/access",
            extra={"file_path": "/mnt/data/test.accdb"},
        )
        connector = AccessConnector(config, connection_string="")

        # Get connection
        connector._get_connection()
        assert connector._connection is not None

        # Close it
        connector.close()

        assert connector._connection is None
        mock_connection.close.assert_called_once()


class TestAccessConnectorIntegration:
    """TDD: Integration scenarios."""

    def test_access_connector_registered_in_factory(self):
        """Verify AccessConnector is registered in the connector factory."""
        from src.connectors.connector_factory import CONNECTOR_CLASS_MAP

        assert "access" in CONNECTOR_CLASS_MAP
        assert CONNECTOR_CLASS_MAP["access"] == AccessConnector

    def test_access_in_connector_registry_config(self):
        """Verify Access is configured in connector_registry.yaml."""
        from src.connectors.connector_factory import load_registry

        registry = load_registry()
        assert "access" in registry["databases"]["connectors"]

        access_config = registry["databases"]["connectors"]["access"]
        assert "driver" in access_config
        assert "file_path" in access_config
        assert "keyvault_secret" in access_config
