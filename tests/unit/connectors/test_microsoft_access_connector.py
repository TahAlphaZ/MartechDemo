"""
TDD Tests - Microsoft Access Connector
Tests for the Microsoft Access database connector.
Covers: connection validation, query execution, schema introspection, data extraction.
"""
import pytest
import json
from unittest.mock import Mock, MagicMock, patch, call
from pathlib import Path
from src.connectors.microsoft_access_connector import MicrosoftAccessConnector
from src.connectors.base_connector import ConnectorConfig, DatabaseConnector


class TestMicrosoftAccessConnectorInitialization:
    """Test Microsoft Access connector setup and initialization."""

    def test_microsoft_access_connector_inherits_from_database_connector(self):
        """Verify MicrosoftAccessConnector is a proper DatabaseConnector subclass."""
        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="access-params",
            landing_path="raw/database/microsoft_access",
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            assert isinstance(connector, DatabaseConnector)

    def test_microsoft_access_connector_stores_connection_params(self):
        """Verify connection parameters are stored correctly."""
        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        params = {
            "file_path": "/path/to/marketing.accdb",
            "password": "secret123",
            "readonly": True,
        }
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, params)
            assert connector.file_path == "/path/to/marketing.accdb"
            assert connector.password == "secret123"
            assert connector.readonly is True

    def test_microsoft_access_connector_rejects_missing_file_path(self):
        """Verify connector requires file_path parameter."""
        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with pytest.raises(ValueError, match="file_path is required"):
            MicrosoftAccessConnector(config, {})

    def test_microsoft_access_connector_rejects_missing_file(self):
        """Verify connector validates file existence."""
        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("os.path.exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="Access file not found"):
                MicrosoftAccessConnector(config, {"file_path": "/nonexistent/database.accdb"})

    def test_microsoft_access_connector_accdb_driver_selection(self):
        """Verify correct driver selected for .accdb files."""
        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            assert connector.driver == "{Microsoft Access Driver (*.accdb)}"

    def test_microsoft_access_connector_mdb_driver_selection(self):
        """Verify correct driver selected for .mdb files."""
        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.mdb"})
            assert connector.driver == "{Microsoft Access Driver (*.mdb)}"

    def test_microsoft_access_connector_rejects_unsupported_format(self):
        """Verify connector rejects unsupported file formats."""
        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("os.path.exists", return_value=True):
            with pytest.raises(ValueError, match="Unsupported Access file format"):
                MicrosoftAccessConnector(config, {"file_path": "/path/to/database.xlsx"})

    def test_microsoft_access_connector_requires_pyodbc(self):
        """Verify connector requires pyodbc library."""
        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("src.connectors.microsoft_access_connector.pyodbc", None):
            with pytest.raises(ImportError, match="pyodbc is required"):
                MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})


class TestMicrosoftAccessConnectionValidation:
    """Test connection validation and health checks."""

    @patch("src.connectors.microsoft_access_connector.pyodbc.connect")
    def test_validate_connection_success(self, mock_pyodbc_connect):
        """Test successful connection validation."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"test_value": 1}
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pyodbc_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
            extra={"test_query": "SELECT 1 as test_value"},
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            result = connector.validate_connection()

        assert result is True
        assert connector._is_connected is True
        mock_pyodbc_connect.assert_called_once()

    @patch("src.connectors.microsoft_access_connector.pyodbc.connect")
    def test_validate_connection_failure(self, mock_pyodbc_connect):
        """Test failed connection validation."""
        mock_pyodbc_connect.side_effect = Exception("ODBC connection failed")

        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            result = connector.validate_connection()

        assert result is False
        assert connector._is_connected is False

    @patch("src.connectors.microsoft_access_connector.pyodbc.connect")
    def test_validate_connection_uses_test_query_from_config(self, mock_pyodbc_connect):
        """Verify custom test query is used when provided."""
        custom_test_query = "SELECT COUNT(*) FROM [DatabaseProperties]"
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"COUNT": 5}
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pyodbc_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
            extra={"test_query": custom_test_query},
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            connector.validate_connection()

        mock_cursor.execute.assert_called_with(custom_test_query)


class TestMicrosoftAccessQueryExecution:
    """Test SQL query execution and result handling."""

    @patch("src.connectors.microsoft_access_connector.pyodbc.connect")
    def test_extract_executes_query(self, mock_pyodbc_connect):
        """Test that extract executes SQL query correctly."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("ID",), ("NAME",)]
        mock_cursor.fetchall.return_value = [
            (1, "Campaign A"),
            (2, "Campaign B"),
        ]
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pyodbc_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            connector._is_connected = True

            query = "SELECT [ID], [NAME] FROM [Campaigns]"
            result = connector.extract(query)

        assert len(result) == 2
        assert result[0]["NAME"] == "Campaign A"
        mock_cursor.execute.assert_called_with(query)

    @patch("src.connectors.microsoft_access_connector.pyodbc.connect")
    def test_extract_with_parameters(self, mock_pyodbc_connect):
        """Test query execution with parameters."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("ID",), ("NAME",)]
        mock_cursor.fetchall.return_value = [(1, "Campaign A")]
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pyodbc_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            connector._is_connected = True

            query = "SELECT [ID], [NAME] FROM [Campaigns] WHERE [Status] = ?"
            params = ["active"]
            result = connector.extract(query, params)

        assert len(result) == 1
        mock_cursor.execute.assert_called_with(query, params)

    @patch("src.connectors.microsoft_access_connector.pyodbc.connect")
    def test_extract_handles_empty_results(self, mock_pyodbc_connect):
        """Test extraction handles empty result sets."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("ID",), ("NAME",)]
        mock_cursor.fetchall.return_value = []
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pyodbc_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            connector._is_connected = True

            result = connector.extract("SELECT * FROM [EmptyTable]")

        assert result == []

    @patch("src.connectors.microsoft_access_connector.pyodbc.connect")
    def test_extract_raises_on_query_error(self, mock_pyodbc_connect):
        """Test extraction raises exception on SQL error."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Invalid table name")
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pyodbc_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            connector._is_connected = True

            with pytest.raises(Exception, match="Invalid table name"):
                connector.extract("SELECT * FROM [InvalidTable]")


class TestMicrosoftAccessSchemaIntrospection:
    """Test schema discovery and metadata retrieval."""

    @patch("src.connectors.microsoft_access_connector.pyodbc.connect")
    def test_get_schema_returns_column_metadata(self, mock_pyodbc_connect):
        """Test get_schema returns table and column information."""
        mock_cursor = MagicMock()
        
        # Mock cursor.tables() and cursor.columns()
        mock_table_row = MagicMock()
        mock_table_row.table_name = "Campaigns"
        
        mock_col_1 = MagicMock()
        mock_col_1.table_name = "Campaigns"
        mock_col_1.column_name = "ID"
        mock_col_1.type_name = "INTEGER"
        mock_col_1.ordinal_position = 1
        mock_col_1.nullable = 0
        
        mock_col_2 = MagicMock()
        mock_col_2.table_name = "Campaigns"
        mock_col_2.column_name = "Name"
        mock_col_2.type_name = "TEXT"
        mock_col_2.ordinal_position = 2
        mock_col_2.nullable = 1
        
        mock_cursor.tables.return_value = [mock_table_row]
        mock_cursor.columns.return_value = [mock_col_1, mock_col_2]
        
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pyodbc_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            schema = connector.get_schema()

        assert len(schema) == 2
        assert schema[0]["table_name"] == "Campaigns"
        assert schema[0]["data_type"] == "INTEGER"
        assert schema[1]["nullable"] is True

    @patch("src.connectors.microsoft_access_connector.pyodbc.connect")
    def test_get_available_tables_returns_table_list(self, mock_pyodbc_connect):
        """Test get_available_tables returns list of user tables."""
        mock_cursor = MagicMock()
        
        mock_table_1 = MagicMock()
        mock_table_1.table_name = "Campaigns"
        
        mock_table_2 = MagicMock()
        mock_table_2.table_name = "Customers"
        
        mock_system_table = MagicMock()
        mock_system_table.table_name = "MSysObjects"
        
        mock_cursor.tables.return_value = [mock_table_1, mock_table_2, mock_system_table]
        
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pyodbc_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            tables = connector.get_available_tables()

        assert len(tables) == 2
        assert "Campaigns" in tables
        assert "Customers" in tables
        assert "MSysObjects" not in tables


class TestMicrosoftAccessTableOperations:
    """Test table-level operations like sampling and counting."""

    @patch("src.connectors.microsoft_access_connector.pyodbc.connect")
    def test_get_table_sample(self, mock_pyodbc_connect):
        """Test sampling rows from a table."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("ID",), ("NAME",)]
        mock_cursor.fetchall.return_value = [
            (1, "Campaign A"),
            (2, "Campaign B"),
        ]
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pyodbc_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            connector._is_connected = True

            sample = connector.get_table_sample("Campaigns", limit=100)

        assert len(sample) == 2
        mock_cursor.execute.assert_called_with("SELECT TOP 100 * FROM [Campaigns]")

    @patch("src.connectors.microsoft_access_connector.pyodbc.connect")
    def test_get_table_count(self, mock_pyodbc_connect):
        """Test getting row count from a table."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("row_count",)]
        mock_cursor.fetchall.return_value = [(1542,)]
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pyodbc_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            connector._is_connected = True

            count = connector.get_table_count("Campaigns")

        assert count == 1542
        mock_cursor.execute.assert_called_with("SELECT COUNT(*) as row_count FROM [Campaigns]")


class TestMicrosoftAccessConnectionManagement:
    """Test connection lifecycle and resource cleanup."""

    @patch("src.connectors.microsoft_access_connector.pyodbc.connect")
    def test_close_connection(self, mock_pyodbc_connect):
        """Test closing connection."""
        mock_connection = MagicMock()
        mock_pyodbc_connect.return_value = mock_connection

        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/db/access",
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            connector._connection = mock_connection

            connector.close()

        mock_connection.close.assert_called_once()
        assert connector._connection is None

    @patch("src.connectors.microsoft_access_connector.pyodbc.connect")
    def test_get_metadata(self, mock_pyodbc_connect):
        """Test metadata is attached to extractions."""
        config = ConnectorConfig(
            name="microsoft_access",
            connector_type="database",
            keyvault_secret="test",
            landing_path="raw/database/microsoft_access",
        )
        with patch("os.path.exists", return_value=True):
            connector = MicrosoftAccessConnector(config, {"file_path": "/path/to/database.accdb"})
            metadata = connector.get_metadata()

        assert metadata["_source_system"] == "microsoft_access"
        assert metadata["_connector_type"] == "database"
        assert metadata["_landing_path"] == "raw/database/microsoft_access"
