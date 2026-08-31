"""
TDD Tests - Google Drive Sheets Connector
Write tests FIRST, then implement. Tests validate:
- OAuth2 service account credential parsing
- Google Drive file enumeration
- Google Sheets data extraction
- Batch operations and pagination
- Connection validation
"""
import pytest
import json
from unittest.mock import patch, MagicMock, call
from src.connectors.google_drive_connector import GoogleDriveConnector
from src.connectors.base_connector import ConnectorConfig, APIConnector


class TestGoogleDriveConnectorInit:
    """TDD: Initialization and credential parsing."""

    def test_google_drive_connector_inherits_from_api_connector(self):
        """Ensure GoogleDriveConnector is an APIConnector subclass."""
        assert issubclass(GoogleDriveConnector, APIConnector)

    def test_connector_initialization_with_service_account(self):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={
                "auth_type": "service_account",
                "base_url": "https://www.googleapis.com",
                "drive_api_version": "v3",
                "sheets_api_version": "v4",
                "scopes": [
                    "https://www.googleapis.com/auth/drive.readonly",
                    "https://www.googleapis.com/auth/spreadsheets.readonly",
                ],
            }
        )
        
        service_account_json = json.dumps({
            "type": "service_account",
            "project_id": "test-project",
            "private_key_id": "key123",
            "client_email": "test@test-project.iam.gserviceaccount.com",
        })
        
        connector = GoogleDriveConnector(config, service_account_json)
        assert connector.config.name == "google_drive"
        assert connector.auth_type == "service_account"
        assert connector.drive_api_version == "v3"
        assert connector.sheets_api_version == "v4"

    def test_connector_parses_json_credentials(self):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        service_account_json = json.dumps({
            "type": "service_account",
            "project_id": "test-project",
        })
        
        connector = GoogleDriveConnector(config, service_account_json)
        assert isinstance(connector.credentials, dict)
        assert connector.credentials["project_id"] == "test-project"

    def test_connector_raises_on_invalid_json(self):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        with pytest.raises(ValueError, match="Invalid credentials format"):
            GoogleDriveConnector(config, "invalid json")

    def test_connector_respects_custom_scopes(self):
        custom_scopes = ["https://www.googleapis.com/auth/drive"]
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={
                "auth_type": "service_account",
                "scopes": custom_scopes,
            }
        )
        
        service_account_json = json.dumps({
            "type": "service_account",
            "project_id": "test-project",
        })
        
        connector = GoogleDriveConnector(config, service_account_json)
        assert connector.scopes == custom_scopes


class TestGoogleDriveConnectorConnection:
    """TDD: Connection validation and error handling."""

    @patch.object(GoogleDriveConnector, '_make_request')
    def test_validate_connection_success(self, mock_request):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        connector = GoogleDriveConnector(config, json.dumps({"type": "service_account"}))
        mock_request.return_value = {"files": []}
        
        result = connector.validate_connection()
        assert result is True
        assert connector._is_connected is True

    @patch.object(GoogleDriveConnector, '_make_request')
    def test_validate_connection_failure(self, mock_request):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        connector = GoogleDriveConnector(config, json.dumps({"type": "service_account"}))
        mock_request.side_effect = Exception("API Error")
        
        result = connector.validate_connection()
        assert result is False

    @patch.object(GoogleDriveConnector, '_make_request')
    def test_connection_validation_calls_drive_api(self, mock_request):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={
                "auth_type": "service_account",
                "drive_api_version": "v3"
            }
        )
        
        connector = GoogleDriveConnector(config, json.dumps({"type": "service_account"}))
        mock_request.return_value = {"files": []}
        
        connector.validate_connection()
        
        mock_request.assert_called_once()
        call_args = mock_request.call_args
        assert "/drive/v3/files" in call_args[0]


class TestGoogleSheetExtraction:
    """TDD: Google Sheets data extraction."""

    @patch.object(GoogleDriveConnector, '_make_request')
    @patch.object(GoogleDriveConnector, '_get_sheet_metadata')
    def test_extract_sheet_with_headers(self, mock_metadata, mock_request):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        connector = GoogleDriveConnector(config, json.dumps({"type": "service_account"}))
        
        mock_metadata.return_value = {
            "sheets": [{"properties": {"title": "Sales Data"}}]
        }
        
        mock_request.return_value = {
            "values": [
                ["Date", "Revenue", "Units"],
                ["2024-01-01", "1000", "10"],
                ["2024-01-02", "1200", "12"],
            ]
        }
        
        spreadsheet_id = "1234567890abcdefghijklmnopqrst"
        result = connector._extract_sheet(spreadsheet_id, {})
        
        assert len(result) == 2
        assert result[0] == {"Date": "2024-01-01", "Revenue": "1000", "Units": "10"}
        assert result[1] == {"Date": "2024-01-02", "Revenue": "1200", "Units": "12"}

    @patch.object(GoogleDriveConnector, '_make_request')
    @patch.object(GoogleDriveConnector, '_get_sheet_metadata')
    def test_extract_sheet_with_custom_range(self, mock_metadata, mock_request):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        connector = GoogleDriveConnector(config, json.dumps({"type": "service_account"}))
        
        mock_request.return_value = {
            "values": [
                ["Name", "Email"],
                ["John Doe", "john@example.com"],
            ]
        }
        
        spreadsheet_id = "1234567890abcdefghijklmnopqrst"
        result = connector._extract_sheet(
            spreadsheet_id,
            {"range": "Contacts!A1:B100"}
        )
        
        assert len(result) == 1
        assert result[0]["Name"] == "John Doe"

    @patch.object(GoogleDriveConnector, '_make_request')
    def test_extract_sheet_with_missing_data(self, mock_request):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        connector = GoogleDriveConnector(config, json.dumps({"type": "service_account"}))
        
        # Simulate sparse data (missing columns)
        mock_request.return_value = {
            "values": [
                ["A", "B", "C"],
                ["1", "2"],  # Missing 3rd column
                ["4", "5", "6"],
            ]
        }
        
        spreadsheet_id = "1234567890abcdefghijklmnopqrst"
        result = connector._extract_sheet(spreadsheet_id, {})
        
        assert result[0]["A"] == "1"
        assert result[0]["B"] == "2"
        assert result[0]["C"] is None  # Missing value

    @patch.object(GoogleDriveConnector, '_make_request')
    def test_extract_sheet_empty_sheet(self, mock_request):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        connector = GoogleDriveConnector(config, json.dumps({"type": "service_account"}))
        mock_request.return_value = {}  # No values key
        
        spreadsheet_id = "1234567890abcdefghijklmnopqrst"
        result = connector._extract_sheet(spreadsheet_id, {})
        
        assert result == []


class TestGoogleDriveFileListing:
    """TDD: Google Drive file enumeration and search."""

    @patch.object(GoogleDriveConnector, '_make_request')
    def test_list_files_with_query(self, mock_request):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        connector = GoogleDriveConnector(config, json.dumps({"type": "service_account"}))
        
        mock_request.return_value = {
            "files": [
                {
                    "id": "1abc",
                    "name": "Q1 Sales.xlsx",
                    "mimeType": "application/vnd.google-apps.spreadsheet",
                    "createdTime": "2024-01-01T00:00:00Z",
                    "modifiedTime": "2024-01-15T00:00:00Z",
                    "webViewLink": "https://docs.google.com/spreadsheets/d/1abc",
                },
            ],
            "nextPageToken": None,
        }
        
        result = connector._list_files("name contains 'Sales'", {})
        
        assert len(result) == 1
        assert result[0]["name"] == "Q1 Sales.xlsx"

    @patch.object(GoogleDriveConnector, '_make_request')
    def test_list_spreadsheets_only(self, mock_request):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        connector = GoogleDriveConnector(config, json.dumps({"type": "service_account"}))
        mock_request.return_value = {"files": []}
        
        connector._list_files("", {"file_types": ["spreadsheet"]})
        
        call_args = mock_request.call_args
        params = call_args[1]["params"]
        assert "mimeType='application/vnd.google-apps.spreadsheet'" in params["q"]

    @patch.object(GoogleDriveConnector, '_make_request')
    def test_list_files_pagination(self, mock_request):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        connector = GoogleDriveConnector(config, json.dumps({"type": "service_account"}))
        
        # Mock two pages of results
        mock_request.side_effect = [
            {
                "files": [{"id": "1", "name": "File1"}],
                "nextPageToken": "token123",
            },
            {
                "files": [{"id": "2", "name": "File2"}],
                "nextPageToken": None,
            },
        ]
        
        result = connector._list_files("", {})
        
        assert len(result) == 2
        assert result[0]["name"] == "File1"
        assert result[1]["name"] == "File2"
        assert mock_request.call_count == 2


class TestGoogleDriveExtractMethod:
    """TDD: High-level extract method routing."""

    @patch.object(GoogleDriveConnector, '_extract_sheet')
    def test_extract_routes_to_sheet_extraction(self, mock_extract_sheet):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        connector = GoogleDriveConnector(config, json.dumps({"type": "service_account"}))
        connector._is_connected = True
        mock_extract_sheet.return_value = [{"col": "val"}]
        
        # 44-char spreadsheet ID
        spreadsheet_id = "a" * 44
        connector.extract(spreadsheet_id, {"range": "Sheet1!A1:Z100"})
        
        mock_extract_sheet.assert_called_once()

    @patch.object(GoogleDriveConnector, '_list_files')
    def test_extract_routes_to_file_listing(self, mock_list_files):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        connector = GoogleDriveConnector(config, json.dumps({"type": "service_account"}))
        connector._is_connected = True
        mock_list_files.return_value = []
        
        # Short query string (not a spreadsheet ID)
        connector.extract("name contains 'Sales'", {})
        
        mock_list_files.assert_called_once()


class TestGoogleDriveConnectorSchema:
    """TDD: Schema generation and metadata."""

    def test_get_schema_returns_valid_schema(self):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        connector = GoogleDriveConnector(config, json.dumps({"type": "service_account"}))
        schema = connector.get_schema()
        
        assert schema["type"] == "object"
        assert "properties" in schema
        assert "id" in schema["properties"]
        assert "name" in schema["properties"]
        assert "mimeType" in schema["properties"]

    def test_get_metadata(self):
        config = ConnectorConfig(
            name="google_drive",
            connector_type="api",
            keyvault_secret="google-drive-service-account",
            landing_path="raw/files/google_drive",
            extra={"auth_type": "service_account"}
        )
        
        connector = GoogleDriveConnector(config, json.dumps({"type": "service_account"}))
        metadata = connector.get_metadata()
        
        assert metadata["_source_system"] == "google_drive"
        assert metadata["_connector_type"] == "api"
        assert "raw/files/google_drive" in metadata["_landing_path"]
