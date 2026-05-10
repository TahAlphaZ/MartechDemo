"""Spreadsheet connectors for Microsoft Excel and Google Sheets sources."""
from __future__ import annotations

from typing import Any, Dict, Optional

from src.connectors.base_connector import BaseConnector, ConnectorConfig


class SpreadsheetConnector(BaseConnector):
    """Common behavior for spreadsheet-backed data sources."""

    def __init__(self, config: ConnectorConfig, credential: str):
        super().__init__(config)
        self.credential = credential

    def validate_connection(self) -> bool:
        source_identifier = self.config.extra.get("workbook_id") or self.config.extra.get("spreadsheet_id")
        self._is_connected = bool(self.credential and source_identifier)
        return self._is_connected

    def extract(self, query_or_endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        if not self._is_connected:
            self.validate_connection()

        sheet_name = params.get("sheet_name") if params else None
        return {
            "source": self.config.name,
            "resource": query_or_endpoint,
            "sheet_name": sheet_name,
            "options": params or {},
            "metadata": self.get_metadata(),
        }

    def get_schema(self) -> Dict[str, Any]:
        return {
            "source_type": self.config.extra.get("type", "spreadsheet"),
            "identifier": self.config.extra.get("workbook_id") or self.config.extra.get("spreadsheet_id"),
            "worksheet": self.config.extra.get("worksheet") or self.config.extra.get("default_range"),
        }


class ExcelConnector(SpreadsheetConnector):
    """Connector for Microsoft Excel / Microsoft Graph backed workbooks."""


class GoogleSheetsConnector(SpreadsheetConnector):
    """Connector for Google Sheets API backed spreadsheets."""
