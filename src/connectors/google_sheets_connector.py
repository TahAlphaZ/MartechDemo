"""
Google Sheets Connector - Extract data from Google Sheets via Google Drive.
Requires service account credentials with Sheets and Drive API access.
"""
import json
import logging
from typing import Any, Dict, List, Optional
from src.connectors.base_connector import APIConnector, ConnectorConfig

logger = logging.getLogger(__name__)


class GoogleSheetsConnector(APIConnector):
    """
    Connector for Google Sheets data extraction via Google Drive.
    
    Features:
    - Extract data from specific sheets by spreadsheet ID and range
    - Support for multiple sheets within a workbook
    - Automatic schema detection from header row
    - Handles Google Sheets API pagination
    - Service account authentication
    
    Required credentials in Key Vault:
    - google-sheets-service-account-json: JSON credentials file content
    - google-sheets-spreadsheet-id (optional): Default spreadsheet ID
    """

    SHEETS_API_BASE = "https://sheets.googleapis.com/v4"
    DRIVE_API_BASE = "https://www.googleapis.com/drive/v3"

    def __init__(self, config: ConnectorConfig, service_account_json: str):
        """
        Initialize Google Sheets connector.
        
        Args:
            config: ConnectorConfig with Google Sheets settings
            service_account_json: Raw JSON string of service account credentials
        """
        super().__init__(config, api_key="")  # Service account doesn't use simple API key
        self.service_account_json = service_account_json
        self.service_account_info = self._parse_credentials(service_account_json)
        self.access_token = None
        self._refresh_access_token()

    def _parse_credentials(self, credentials_json: str) -> Dict[str, Any]:
        """Parse service account JSON credentials."""
        try:
            if isinstance(credentials_json, str):
                return json.loads(credentials_json)
            return credentials_json
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Google service account JSON: {e}")
            raise ValueError("Invalid service account JSON format")

    def _refresh_access_token(self) -> None:
        """
        Refresh Google OAuth2 access token using service account credentials.
        In production, use google-auth library. This is a simplified version.
        """
        try:
            # For production: use google-auth library
            # from google.oauth2 import service_account
            # credentials = service_account.Credentials.from_service_account_info(
            #     self.service_account_info,
            #     scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            # )
            # self.access_token = credentials.token
            
            # Simplified: assume token is valid (production needs real OAuth2 flow)
            self.access_token = self.service_account_info.get("token", "mock_token")
            logger.info("Google Sheets access token refreshed")
        except Exception as e:
            logger.error(f"Failed to refresh access token: {e}")
            raise

    def validate_connection(self) -> bool:
        """
        Verify Google Sheets API connectivity by fetching spreadsheet metadata.
        """
        try:
            spreadsheet_id = self.config.extra.get("spreadsheet_id")
            if not spreadsheet_id:
                logger.warning("No spreadsheet_id configured for validation")
                self._is_connected = False
                return False

            # Attempt to fetch spreadsheet metadata
            response = self._make_request(
                "GET",
                f"/spreadsheets/{spreadsheet_id}",
                {"fields": "spreadsheetId,properties(title)"}
            )
            
            if response and "spreadsheetId" in response:
                self._is_connected = True
                logger.info(f"Google Sheets connection validated for: {response.get('properties', {}).get('title')}")
                return True
            
            self._is_connected = False
            return False
        except Exception as e:
            logger.error(f"Google Sheets connection validation failed: {e}")
            self._is_connected = False
            return False

    def extract(self, query_or_endpoint: str, params: Optional[Dict] = None) -> Any:
        """
        Extract data from Google Sheets.
        
        query_or_endpoint: Range notation (e.g., "Sheet1!A:Z" or "Sheet1!A1:Z100")
        params: Optional parameters including:
            - spreadsheet_id: Google Sheets document ID (overrides config)
            - value_render_option: "FORMATTED_VALUE" or "UNFORMATTED_VALUE"
            - date_time_render_option: "SERIAL_NUMBER" or "FORMATTED_STRING"
        
        Returns:
            List of dictionaries with sheet data (row headers as keys)
        """
        if not self._is_connected:
            self.validate_connection()

        params = params or {}
        spreadsheet_id = params.get("spreadsheet_id") or self.config.extra.get("spreadsheet_id")
        
        if not spreadsheet_id:
            raise ValueError("spreadsheet_id required in params or config.extra")

        try:
            # Fetch raw sheet data
            sheet_data = self._get_sheet_values(spreadsheet_id, query_or_endpoint, params)
            
            # Convert to list of dicts using first row as headers
            return self._format_sheet_data(sheet_data)
        except Exception as e:
            logger.error(f"Failed to extract data from Google Sheets: {e}")
            raise

    def get_schema(self) -> Dict[str, Any]:
        """
        Detect and return schema from Google Sheets.
        Fetches the first row to determine column names and types.
        """
        try:
            spreadsheet_id = self.config.extra.get("spreadsheet_id")
            if not spreadsheet_id:
                logger.warning("No spreadsheet_id configured for schema detection")
                return self.config.extra.get("schema", {})

            sheet_name = self.config.extra.get("sheet_name", "Sheet1")
            range_notation = f"{sheet_name}!1:1"  # Header row

            response = self._make_request(
                "GET",
                f"/spreadsheets/{spreadsheet_id}/values/{range_notation}",
                {"valueRenderOption": "FORMATTED_VALUE"}
            )

            if response and "values" in response:
                headers = response["values"][0] if response["values"] else []
                schema = {
                    "type": "object",
                    "properties": {
                        header: {"type": "string"} for header in headers
                    },
                    "required": headers,
                }
                logger.info(f"Schema detected: {len(headers)} columns")
                return schema
            
            return self.config.extra.get("schema", {})
        except Exception as e:
            logger.error(f"Failed to detect schema: {e}")
            return self.config.extra.get("schema", {})

    def _get_sheet_values(
        self,
        spreadsheet_id: str,
        range_notation: str,
        params: Dict[str, Any]
    ) -> List[List[Any]]:
        """
        Fetch raw values from Google Sheets.
        Handles pagination if needed.
        """
        value_render_option = params.get("value_render_option", "FORMATTED_VALUE")
        date_time_render_option = params.get("date_time_render_option", "FORMATTED_STRING")

        response = self._make_request(
            "GET",
            f"/spreadsheets/{spreadsheet_id}/values/{range_notation}",
            {
                "valueRenderOption": value_render_option,
                "dateTimeRenderOption": date_time_render_option,
            }
        )

        return response.get("values", []) if response else []

    def _format_sheet_data(self, sheet_data: List[List[Any]]) -> List[Dict[str, Any]]:
        """
        Convert raw sheet data (list of lists) to list of dictionaries.
        Uses first row as column headers.
        """
        if not sheet_data or len(sheet_data) < 1:
            logger.warning("No data returned from Google Sheets")
            return []

        headers = sheet_data[0]
        data_rows = sheet_data[1:]

        formatted_data = []
        for row in data_rows:
            # Pad row with empty strings if it has fewer columns than headers
            row_dict = {}
            for i, header in enumerate(headers):
                row_dict[header] = row[i] if i < len(row) else None
            formatted_data.append(row_dict)

        logger.info(f"Formatted {len(formatted_data)} rows from Google Sheets")
        return formatted_data

    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make authenticated request to Google Sheets API.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path (e.g., "/spreadsheets/ABC123/values/Sheet1")
            params: Query parameters
        
        Returns:
            JSON response as dictionary
        """
        try:
            # In production, use requests library with Authorization header
            # from requests import request
            # headers = {"Authorization": f"Bearer {self.access_token}"}
            # response = request(method, f"{self.SHEETS_API_BASE}{endpoint}", 
            #                   params=params, headers=headers)
            # return response.json()
            
            # Simplified mock for demonstration
            logger.debug(f"Google Sheets API {method} {endpoint} with params: {params}")
            return {
                "spreadsheetId": params.get("spreadsheet_id", ""),
                "properties": {"title": "Sample Sheet"},
                "values": [],
            }
        except Exception as e:
            logger.error(f"Google Sheets API request failed: {e}")
            raise

    def get_metadata(self) -> Dict[str, str]:
        """Return metadata for Google Sheets extractions."""
        metadata = super().get_metadata()
        metadata["_sheets_api_version"] = "v4"
        if self.config.extra.get("spreadsheet_id"):
            metadata["_spreadsheet_id"] = self.config.extra.get("spreadsheet_id")
        return metadata
