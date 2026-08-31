"""
TDD Tests - Google Sheets Connector
Tests validate:
- Google Sheets API connection validation
- Data extraction from specific ranges
- Schema detection from headers
- Proper formatting of raw sheet data to dictionaries
- Credential handling and authentication
"""
import json
import pytest
from unittest.mock import patch, MagicMock, Mock
from src.connectors.google_sheets_connector import GoogleSheetsConnector
from src.connectors.base_connector import ConnectorConfig


class TestGoogleSheetsConnectorInitialization:
    """TDD: Connector initializes with valid service account credentials."""

    def test_connector_initializes_with_service_account(self):
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
            extra={"spreadsheet_id": "test-sheet-id", "sheet_name": "Sheet1"},
        )
        service_account = json.dumps({"type": "service_account", "token": "mock_token"})
        
        connector = GoogleSheetsConnector(config, service_account)
        
        assert connector.config == config
        assert connector.service_account_info["type"] == "service_account"
        assert connector.access_token is not None

    def test_invalid_json_credentials_raises_error(self):
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
        )
        invalid_json = "not valid json {{"
        
        with pytest.raises(ValueError, match="Invalid service account JSON format"):
            GoogleSheetsConnector(config, invalid_json)

    def test_connector_parses_dict_credentials(self):
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
        )
        credentials_dict = {"type": "service_account", "token": "test_token"}
        
        connector = GoogleSheetsConnector(config, json.dumps(credentials_dict))
        
        assert connector.service_account_info["type"] == "service_account"


class TestGoogleSheetsConnectionValidation:
    """TDD: Connection validation via Sheets API."""

    def test_validate_connection_success_with_spreadsheet_id(self):
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
            extra={"spreadsheet_id": "test-id"},
        )
        service_account = json.dumps({"type": "service_account", "token": "token"})
        connector = GoogleSheetsConnector(config, service_account)
        
        with patch.object(connector, "_make_request") as mock_request:
            mock_request.return_value = {
                "spreadsheetId": "test-id",
                "properties": {"title": "Test Sheet"},
            }
            
            result = connector.validate_connection()
            
            assert result is True
            assert connector._is_connected is True

    def test_validate_connection_fails_without_spreadsheet_id(self):
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
        )
        service_account = json.dumps({"type": "service_account", "token": "token"})
        connector = GoogleSheetsConnector(config, service_account)
        
        result = connector.validate_connection()
        
        assert result is False
        assert connector._is_connected is False

    def test_validate_connection_handles_api_error(self):
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
            extra={"spreadsheet_id": "test-id"},
        )
        service_account = json.dumps({"type": "service_account", "token": "token"})
        connector = GoogleSheetsConnector(config, service_account)
        
        with patch.object(connector, "_make_request") as mock_request:
            mock_request.side_effect = Exception("API Error")
            
            result = connector.validate_connection()
            
            assert result is False
            assert connector._is_connected is False


class TestGoogleSheetsDataExtraction:
    """TDD: Extract data from Google Sheets via API."""

    def test_extract_data_with_range_notation(self):
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
            extra={"spreadsheet_id": "sheet-123"},
        )
        service_account = json.dumps({"type": "service_account", "token": "token"})
        connector = GoogleSheetsConnector(config, service_account)
        connector._is_connected = True
        
        mock_sheet_data = [
            ["Name", "Email", "Age"],
            ["Alice", "alice@example.com", "28"],
            ["Bob", "bob@example.com", "35"],
        ]
        
        with patch.object(connector, "_get_sheet_values") as mock_values:
            mock_values.return_value = mock_sheet_data
            
            result = connector.extract("Sheet1!A:C", {"spreadsheet_id": "sheet-123"})
            
            assert len(result) == 2
            assert result[0]["Name"] == "Alice"
            assert result[0]["Email"] == "alice@example.com"
            assert result[1]["Name"] == "Bob"

    def test_extract_requires_spreadsheet_id(self):
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
        )
        service_account = json.dumps({"type": "service_account", "token": "token"})
        connector = GoogleSheetsConnector(config, service_account)
        
        with pytest.raises(ValueError, match="spreadsheet_id required"):
            connector.extract("Sheet1!A:C")

    def test_extract_formats_empty_rows(self):
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
            extra={"spreadsheet_id": "sheet-123"},
        )
        service_account = json.dumps({"type": "service_account", "token": "token"})
        connector = GoogleSheetsConnector(config, service_account)
        connector._is_connected = True
        
        # Row with missing columns
        mock_sheet_data = [
            ["Name", "Email", "Age"],
            ["Alice", "alice@example.com"],  # Missing Age
        ]
        
        with patch.object(connector, "_get_sheet_values") as mock_values:
            mock_values.return_value = mock_sheet_data
            
            result = connector.extract("Sheet1!A:C", {"spreadsheet_id": "sheet-123"})
            
            assert result[0]["Age"] is None


class TestGoogleSheetsSchemaDetection:
    """TDD: Detect schema from sheet headers."""

    def test_schema_detection_from_headers(self):
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
            extra={"spreadsheet_id": "sheet-123", "sheet_name": "Sheet1"},
        )
        service_account = json.dumps({"type": "service_account", "token": "token"})
        connector = GoogleSheetsConnector(config, service_account)
        
        with patch.object(connector, "_make_request") as mock_request:
            mock_request.return_value = {
                "values": [["Name", "Email", "Age", "City"]]
            }
            
            schema = connector.get_schema()
            
            assert schema["type"] == "object"
            assert "Name" in schema["properties"]
            assert "Email" in schema["properties"]
            assert "Age" in schema["properties"]
            assert "City" in schema["properties"]
            assert schema["required"] == ["Name", "Email", "Age", "City"]

    def test_schema_returns_config_default_if_no_spreadsheet_id(self):
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
            extra={"schema": {"type": "object", "properties": {"id": {"type": "string"}}}},
        )
        service_account = json.dumps({"type": "service_account", "token": "token"})
        connector = GoogleSheetsConnector(config, service_account)
        
        schema = connector.get_schema()
        
        assert schema == config.extra.get("schema")

    def test_schema_handles_empty_response(self):
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
            extra={"spreadsheet_id": "sheet-123", "sheet_name": "Sheet1"},
        )
        service_account = json.dumps({"type": "service_account", "token": "token"})
        connector = GoogleSheetsConnector(config, service_account)
        
        with patch.object(connector, "_make_request") as mock_request:
            mock_request.return_value = {"values": []}
            
            schema = connector.get_schema()
            
            assert schema == {}


class TestGoogleSheetsMetadata:
    """TDD: Proper metadata attachment to extractions."""

    def test_metadata_includes_sheets_info(self):
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
            extra={"spreadsheet_id": "sheet-123"},
        )
        service_account = json.dumps({"type": "service_account", "token": "token"})
        connector = GoogleSheetsConnector(config, service_account)
        
        metadata = connector.get_metadata()
        
        assert metadata["_source_system"] == "google_sheets"
        assert metadata["_connector_type"] == "sheets"
        assert metadata["_landing_path"] == "raw/sheets/default"
        assert metadata["_sheets_api_version"] == "v4"
        assert metadata["_spreadsheet_id"] == "sheet-123"

    def test_metadata_omits_spreadsheet_id_if_not_configured(self):
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
        )
        service_account = json.dumps({"type": "service_account", "token": "token"})
        connector = GoogleSheetsConnector(config, service_account)
        
        metadata = connector.get_metadata()
        
        assert "_spreadsheet_id" not in metadata


class TestGoogleSheetsFactoryIntegration:
    """TDD: Factory correctly instantiates Google Sheets connectors."""

    def test_factory_creates_google_sheets_connector(self):
        from src.connectors.connector_factory import create_connector
        
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
            extra={"spreadsheet_id": "test-id"},
        )
        service_account = json.dumps({"type": "service_account", "token": "token"})
        
        connector = create_connector(config, service_account)
        
        assert isinstance(connector, GoogleSheetsConnector)

    def test_factory_registers_google_sheets_in_class_map(self):
        from src.connectors.connector_factory import CONNECTOR_CLASS_MAP
        
        assert "google_sheets" in CONNECTOR_CLASS_MAP
        assert CONNECTOR_CLASS_MAP["google_sheets"] == GoogleSheetsConnector


class TestGoogleSheetsRangeNotation:
    """TDD: Support various Google Sheets range notations."""

    def test_range_a_to_z_notation(self):
        """Support A:Z (entire columns)"""
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
            extra={"spreadsheet_id": "sheet-123"},
        )
        service_account = json.dumps({"type": "service_account", "token": "token"})
        connector = GoogleSheetsConnector(config, service_account)
        connector._is_connected = True
        
        with patch.object(connector, "_get_sheet_values") as mock_values:
            mock_values.return_value = [
                ["Col1", "Col2"],
                ["Val1", "Val2"],
            ]
            
            result = connector.extract("Sheet1!A:Z", {"spreadsheet_id": "sheet-123"})
            
            assert len(result) == 1
            mock_values.assert_called_once()

    def test_range_a1_z100_notation(self):
        """Support A1:Z100 (specific range)"""
        config = ConnectorConfig(
            name="google_sheets",
            connector_type="sheets",
            keyvault_secret="sheets-sa-json",
            landing_path="raw/sheets/default",
            extra={"spreadsheet_id": "sheet-123"},
        )
        service_account = json.dumps({"type": "service_account", "token": "token"})
        connector = GoogleSheetsConnector(config, service_account)
        connector._is_connected = True
        
        with patch.object(connector, "_get_sheet_values") as mock_values:
            mock_values.return_value = [
                ["Col1", "Col2"],
                ["Val1", "Val2"],
            ]
            
            connector.extract("Sheet1!A1:Z100", {"spreadsheet_id": "sheet-123"})
            
            mock_values.assert_called_once()
