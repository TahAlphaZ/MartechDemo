"""
Google Drive Sheets Connector - Extract data from Google Sheets and Drive.
Implements OAuth2 authentication and supports batch operations.
"""
import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
import time

from src.connectors.base_connector import APIConnector, ConnectorConfig

logger = logging.getLogger(__name__)


class GoogleDriveConnector(APIConnector):
    """
    Connector for Google Drive and Google Sheets.
    Handles OAuth2 authentication, sheet extraction, and file enumeration.
    
    Supported operations:
    - List files in Drive
    - Extract data from Google Sheets
    - Batch read operations
    - File metadata retrieval
    """

    def __init__(self, config: ConnectorConfig, api_key: str):
        """
        Initialize Google Drive connector.
        
        Args:
            config: ConnectorConfig with Google Drive settings
            api_key: JSON service account key or OAuth2 access token
        """
        super().__init__(config, api_key)
        self.base_url = config.extra.get("base_url", "https://www.googleapis.com")
        self.drive_api_version = config.extra.get("drive_api_version", "v3")
        self.sheets_api_version = config.extra.get("sheets_api_version", "v4")
        self.scopes = config.extra.get("scopes", [
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/spreadsheets.readonly",
        ])
        
        self._parse_credentials(api_key)
        self._token = None
        self._token_expiry = None

    def _parse_credentials(self, api_key: str) -> None:
        """Parse and validate service account or OAuth credentials."""
        try:
            if isinstance(api_key, str):
                self.credentials = json.loads(api_key)
            else:
                self.credentials = api_key
            
            self.auth_type = self.config.extra.get("auth_type", "service_account")
            logger.info(f"Credentials loaded for auth_type: {self.auth_type}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse credentials JSON: {e}")
            raise ValueError("Invalid credentials format")

    def validate_connection(self) -> bool:
        """Test Google Drive API connectivity."""
        try:
            # Simple test: list files with maxResults=1
            response = self._make_request(
                "GET",
                f"/drive/{self.drive_api_version}/files",
                params={"maxResults": 1, "spaces": "drive"}
            )
            
            self._is_connected = response is not None
            if self._is_connected:
                logger.info(f"Connection validated for {self.config.name}")
            return self._is_connected
        except Exception as e:
            logger.error(f"Connection validation failed for {self.config.name}: {e}")
            return False

    def extract(self, query_or_endpoint: str, params: Optional[Dict] = None) -> Any:
        """
        Extract data from Google Sheets or list Drive files.
        
        Args:
            query_or_endpoint: Spreadsheet ID or file query
            params: Optional parameters for API call
                - sheet_name: Sheet tab name (default: first sheet)
                - range: A1 notation range (e.g., "Sheet1!A1:Z100")
                - include_headers: Include header row (default: True)
        
        Returns:
            List of row dictionaries or file metadata
        """
        if not self._is_connected:
            self.validate_connection()
        
        params = params or {}
        
        # Determine if this is a sheet extraction or file listing
        if len(query_or_endpoint) == 44:  # Typical Google Sheet ID length
            return self._extract_sheet(query_or_endpoint, params)
        else:
            return self._list_files(query_or_endpoint, params)

    def _extract_sheet(self, spreadsheet_id: str, params: Dict) -> List[Dict]:
        """Extract data from a Google Sheet."""
        sheet_name = params.get("sheet_name", None)
        range_notation = params.get("range", None)
        include_headers = params.get("include_headers", True)
        
        try:
            # Get sheet metadata to find first sheet if not specified
            if not sheet_name:
                metadata = self._get_sheet_metadata(spreadsheet_id)
                if metadata and metadata.get("sheets"):
                    sheet_name = metadata["sheets"][0].get("properties", {}).get("title", "Sheet1")
            
            # Build range notation if not provided
            if not range_notation:
                range_notation = f"{sheet_name}!A:Z" if sheet_name else "Sheet1!A:Z"
            
            # Fetch sheet data
            response = self._make_request(
                "GET",
                f"/sheets/{self.sheets_api_version}/spreadsheets/{spreadsheet_id}/values/{range_notation}",
                params={"valueRenderOption": "FORMATTED_VALUE"}
            )
            
            if not response or "values" not in response:
                logger.warning(f"No data found in sheet {spreadsheet_id}")
                return []
            
            values = response["values"]
            
            # Convert to list of dicts with headers
            if include_headers and len(values) > 0:
                headers = values[0]
                rows = [
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                    for row in values[1:]
                ]
                return rows
            else:
                return [{"column_0": row} for row in values]
        
        except Exception as e:
            logger.error(f"Failed to extract sheet {spreadsheet_id}: {e}")
            raise

    def _list_files(self, query: str, params: Dict) -> List[Dict]:
        """List files from Google Drive matching the query."""
        all_files = []
        next_page_token = None
        
        try:
            # Build query filter
            filters = [query] if query else []
            filters.append("trashed=false")
            file_types = params.get("file_types", ["spreadsheet"])
            
            if "spreadsheet" in file_types:
                filters.append("mimeType='application/vnd.google-apps.spreadsheet'")
            
            q = " and ".join(filters)
            
            # Paginate through results
            while True:
                response = self._make_request(
                    "GET",
                    f"/drive/{self.drive_api_version}/files",
                    params={
                        "q": q,
                        "spaces": "drive",
                        "fields": "files(id,name,mimeType,createdTime,modifiedTime,webViewLink)",
                        "maxResults": 100,
                        "pageToken": next_page_token,
                    }
                )
                
                if response and "files" in response:
                    all_files.extend(response["files"])
                
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
            
            logger.info(f"Found {len(all_files)} files matching query: {query}")
            return all_files
        
        except Exception as e:
            logger.error(f"Failed to list Drive files: {e}")
            raise

    def _get_sheet_metadata(self, spreadsheet_id: str) -> Dict:
        """Get metadata about a Google Sheet."""
        try:
            response = self._make_request(
                "GET",
                f"/sheets/{self.sheets_api_version}/spreadsheets/{spreadsheet_id}",
                params={"fields": "sheets(properties(title))"}
            )
            return response
        except Exception as e:
            logger.error(f"Failed to get sheet metadata: {e}")
            return {}

    def get_schema(self) -> Dict[str, Any]:
        """Return expected schema for Google Sheets connector."""
        return {
            "type": "object",
            "properties": {
                "id": {"type": "string", "description": "File/Sheet ID"},
                "name": {"type": "string", "description": "File/Sheet name"},
                "mimeType": {"type": "string", "description": "MIME type (e.g., application/vnd.google-apps.spreadsheet)"},
                "createdTime": {"type": "string", "description": "ISO 8601 creation timestamp"},
                "modifiedTime": {"type": "string", "description": "ISO 8601 modification timestamp"},
                "webViewLink": {"type": "string", "description": "Web-viewable URL"},
            }
        }

    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make HTTP request to Google APIs.
        Implements token refresh and retry logic.
        """
        # In a real implementation, this would use requests library with proper auth
        # For now, this is a stub that would be implemented with google-auth library
        raise NotImplementedError(
            "Subclass or integration layer must implement HTTP requests "
            "using google-auth and requests libraries"
        )
