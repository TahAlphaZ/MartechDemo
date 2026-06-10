"""
Excel workbook connector.
"""
from __future__ import annotations

import json
import logging
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any
from xml.etree import ElementTree as ET

from src.connectors.base_connector import ConnectorConfig, FileConnector

logger = logging.getLogger(__name__)

SPREADSHEET_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
DOCUMENT_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
XML_NAMESPACES = {
    "ss": SPREADSHEET_NS,
    "r": DOCUMENT_REL_NS,
    "pr": PACKAGE_REL_NS,
}


class ExcelConnector(FileConnector):
    """Connector for reading row-based data from .xlsx and .xlsm workbooks."""

    SUPPORTED_SUFFIXES = {".xlsx", ".xlsm"}

    def __init__(self, config: ConnectorConfig, credentials: str):
        super().__init__(config, credentials)
        self._connection_settings = self._load_connection_settings(credentials)

    def extract(self, query_or_endpoint: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Read one or more workbook files and normalize rows across sheets."""
        if not self._is_connected:
            self.validate_connection()

        request_params = dict(params or {})
        source_path = query_or_endpoint or str(request_params.pop("source_path", "") or "")
        return self._read_files(source_path, request_params)

    def get_schema(self) -> dict[str, Any]:
        """Return the configured workbook parsing options."""
        return {
            "file_pattern": str(self.config.extra.get("file_pattern", "*.xlsx")),
            "sheet_names": list(self.config.extra.get("sheet_names", [])),
            "header_row": int(self.config.extra.get("header_row", 1)),
            "supported_extensions": sorted(self.SUPPORTED_SUFFIXES),
            "row_metadata": self.get_metadata(),
        }

    def _list_files(self) -> list[str]:
        return [str(path) for path in self._resolve_workbook_paths("")]

    def _read_files(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        request_params = dict(params or {})
        sheet_names = self._resolve_sheet_names(request_params)
        header_row = int(request_params.pop("header_row", self.config.extra.get("header_row", 1)))
        workbooks = self._resolve_workbook_paths(path)
        rows: list[dict[str, Any]] = []

        for workbook_path in workbooks:
            rows.extend(self._read_workbook(workbook_path, sheet_names, header_row))

        return rows

    def _load_connection_settings(self, credentials: str) -> dict[str, Any]:
        stripped_credentials = credentials.strip()
        if not stripped_credentials:
            return {}

        try:
            settings = json.loads(stripped_credentials)
        except json.JSONDecodeError:
            return {"base_path": stripped_credentials}

        if not isinstance(settings, dict):
            raise ValueError("Excel connector secret must be a filesystem path or JSON object")
        return settings

    def _resolve_sheet_names(self, params: dict[str, Any]) -> list[str] | None:
        if "sheet_name" in params:
            return [str(params.pop("sheet_name"))]
        if "sheet_names" in params:
            return [str(value) for value in params.pop("sheet_names")]

        configured_sheet_names = self.config.extra.get("sheet_names", [])
        if not configured_sheet_names:
            return None
        return [str(value) for value in configured_sheet_names]

    def _resolve_workbook_paths(self, source_path: str) -> list[Path]:
        resolved_path = self._resolve_source_path(source_path)
        if not resolved_path.exists():
            raise FileNotFoundError(f"Excel source path does not exist: {resolved_path}")

        if resolved_path.is_file():
            self._ensure_supported_workbook(resolved_path)
            return [resolved_path]

        if not resolved_path.is_dir():
            raise ValueError(f"Excel source path must be a file or directory: {resolved_path}")

        file_pattern = str(self.config.extra.get("file_pattern", "*.xlsx"))
        workbooks = sorted(
            path
            for path in resolved_path.glob(file_pattern)
            if path.is_file() and path.suffix.lower() in self.SUPPORTED_SUFFIXES
        )
        if not workbooks:
            raise FileNotFoundError(f"No Excel workbooks found at {resolved_path} matching '{file_pattern}'")
        return workbooks

    def _resolve_source_path(self, source_path: str) -> Path:
        configured_source_path = (
            source_path
            or self.config.extra.get("source_path")
            or self._connection_settings.get("source_path")
            or ""
        )
        base_path = self._connection_settings.get("base_path") or self.config.extra.get("base_path")

        if not configured_source_path and not base_path:
            raise ValueError(
                "Excel connector requires a source path via extract(), config.extra['source_path'], or secret base_path"
            )

        if configured_source_path:
            path = Path(str(configured_source_path)).expanduser()
            if path.is_absolute() or not base_path:
                return path
            return (Path(str(base_path)).expanduser() / path).expanduser()

        return Path(str(base_path)).expanduser()

    def _ensure_supported_workbook(self, path: Path) -> None:
        if path.suffix.lower() not in self.SUPPORTED_SUFFIXES:
            raise ValueError("Excel connector supports only .xlsx and .xlsm workbooks")

    def _read_workbook(
        self, workbook_path: Path, requested_sheet_names: list[str] | None, header_row: int
    ) -> list[dict[str, Any]]:
        with zipfile.ZipFile(workbook_path) as workbook_archive:
            shared_strings = self._read_shared_strings(workbook_archive)
            workbook_sheets = self._read_workbook_sheets(workbook_archive)
            sheet_lookup = {sheet["name"]: sheet for sheet in workbook_sheets}

            if requested_sheet_names is None:
                target_sheets = workbook_sheets
            else:
                missing_sheet_names = [name for name in requested_sheet_names if name not in sheet_lookup]
                if missing_sheet_names:
                    raise ValueError(
                        f"Workbook '{workbook_path.name}' does not contain sheets: {', '.join(missing_sheet_names)}"
                    )
                target_sheets = [sheet_lookup[name] for name in requested_sheet_names]

            rows: list[dict[str, Any]] = []
            for sheet in target_sheets:
                rows.extend(
                    self._read_sheet_rows(
                        workbook_archive,
                        workbook_path.name,
                        sheet_name=str(sheet["name"]),
                        sheet_path=str(sheet["path"]),
                        shared_strings=shared_strings,
                        header_row=header_row,
                    )
                )
            return rows

    def _read_shared_strings(self, workbook_archive: zipfile.ZipFile) -> list[str]:
        try:
            shared_strings_root = ET.fromstring(workbook_archive.read("xl/sharedStrings.xml"))
        except KeyError:
            return []

        shared_strings: list[str] = []
        for string_item in shared_strings_root.findall("ss:si", XML_NAMESPACES):
            shared_strings.append("".join(text or "" for text in string_item.itertext()))
        return shared_strings

    def _read_workbook_sheets(self, workbook_archive: zipfile.ZipFile) -> list[dict[str, str]]:
        workbook_root = ET.fromstring(workbook_archive.read("xl/workbook.xml"))
        relationships_root = ET.fromstring(workbook_archive.read("xl/_rels/workbook.xml.rels"))
        relationship_targets = {
            relationship.attrib["Id"]: relationship.attrib["Target"]
            for relationship in relationships_root.findall("pr:Relationship", XML_NAMESPACES)
        }

        sheets: list[dict[str, str]] = []
        for sheet in workbook_root.findall("ss:sheets/ss:sheet", XML_NAMESPACES):
            relationship_id = sheet.attrib.get(f"{{{DOCUMENT_REL_NS}}}id")
            if relationship_id is None:
                continue

            target = relationship_targets.get(relationship_id)
            if target is None:
                continue

            sheet_path = PurePosixPath(target)
            if not sheet_path.is_absolute():
                sheet_path = PurePosixPath("xl") / sheet_path

            sheets.append({"name": str(sheet.attrib["name"]), "path": str(sheet_path)})
        return sheets

    def _read_sheet_rows(
        self,
        workbook_archive: zipfile.ZipFile,
        workbook_name: str,
        sheet_name: str,
        sheet_path: str,
        shared_strings: list[str],
        header_row: int,
    ) -> list[dict[str, Any]]:
        worksheet_root = ET.fromstring(workbook_archive.read(sheet_path))
        sheet_data = worksheet_root.find("ss:sheetData", XML_NAMESPACES)
        if sheet_data is None:
            return []

        parsed_rows: list[tuple[int, dict[int, Any]]] = []
        for row_position, row in enumerate(sheet_data.findall("ss:row", XML_NAMESPACES), start=1):
            row_number = int(row.attrib.get("r", row_position))
            row_values: dict[int, Any] = {}
            next_column_index = 1

            for cell in row.findall("ss:c", XML_NAMESPACES):
                cell_reference = cell.attrib.get("r")
                column_index = (
                    self._column_index_from_reference(cell_reference) if cell_reference else next_column_index
                )
                row_values[column_index] = self._parse_cell_value(cell, shared_strings)
                next_column_index = column_index + 1

            parsed_rows.append((row_number, row_values))

        return self._normalize_rows(parsed_rows, workbook_name, sheet_name, header_row)

    def _normalize_rows(
        self,
        parsed_rows: list[tuple[int, dict[int, Any]]],
        workbook_name: str,
        sheet_name: str,
        header_row: int,
    ) -> list[dict[str, Any]]:
        header_values = next((values for row_number, values in parsed_rows if row_number == header_row), None)
        if header_values is None:
            raise ValueError(f"Header row {header_row} not found in sheet '{sheet_name}'")

        headers = self._build_headers(header_values)
        rows: list[dict[str, Any]] = []
        for row_number, row_values in parsed_rows:
            if row_number <= header_row or not any(value not in (None, "") for value in row_values.values()):
                continue

            normalized = {header_name: row_values.get(column_index) for column_index, header_name in headers.items()}
            for column_index in sorted(set(row_values) - set(headers)):
                normalized[f"column_{column_index}"] = row_values[column_index]

            normalized.update(self.get_metadata())
            normalized["_file_name"] = workbook_name
            normalized["_sheet_name"] = sheet_name
            normalized["_row_number"] = row_number
            rows.append(normalized)

        return rows

    def _build_headers(self, header_values: dict[int, Any]) -> dict[int, str]:
        headers: dict[int, str] = {}
        seen_headers: dict[str, int] = {}

        for column_index in sorted(header_values):
            raw_header = header_values[column_index]
            base_header = str(raw_header).strip() if raw_header not in (None, "") else f"column_{column_index}"
            occurrence = seen_headers.get(base_header, 0) + 1
            seen_headers[base_header] = occurrence
            header_name = base_header if occurrence == 1 else f"{base_header}_{occurrence}"
            headers[column_index] = header_name

        return headers

    def _parse_cell_value(self, cell: ET.Element, shared_strings: list[str]) -> Any:
        cell_type = cell.attrib.get("t")

        if cell_type == "inlineStr":
            inline_string = cell.find("ss:is", XML_NAMESPACES)
            if inline_string is None:
                return ""
            return "".join(text or "" for text in inline_string.itertext())

        raw_value = cell.findtext("ss:v", default=None, namespaces=XML_NAMESPACES)
        if raw_value is None:
            formula_value = cell.findtext("ss:f", default=None, namespaces=XML_NAMESPACES)
            return formula_value

        if cell_type == "s":
            shared_string_index = int(raw_value)
            return shared_strings[shared_string_index]
        if cell_type == "b":
            return raw_value == "1"
        if cell_type == "str":
            return raw_value
        return self._coerce_scalar(raw_value)

    def _coerce_scalar(self, value: str) -> Any:
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value

    def _column_index_from_reference(self, reference: str) -> int:
        column_letters = "".join(character for character in reference if character.isalpha()).upper()
        column_index = 0
        for character in column_letters:
            column_index = (column_index * 26) + (ord(character) - ord("A") + 1)
        return column_index
