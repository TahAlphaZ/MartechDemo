from __future__ import annotations

import zipfile
from pathlib import Path
from xml.sax.saxutils import escape

import pytest

from src.connectors.base_connector import ConnectorConfig
from src.connectors.excel_connector import ExcelConnector


@pytest.fixture
def excel_config() -> ConnectorConfig:
    return ConnectorConfig(
        name="excel",
        connector_type="file",
        keyvault_secret="excel-source-path",
        landing_path="raw/files/excel",
        extra={
            "file_pattern": "*.xlsx",
            "header_row": 1,
            "sheet_names": ["Sales"],
        },
    )


def test_validate_connection_lists_available_workbooks(excel_config: ConnectorConfig, tmp_path: Path):
    workbook_path = tmp_path / "sales.xlsx"
    _write_workbook(
        workbook_path,
        {
            "Sales": [
                ["order_id", "revenue"],
                ["A-100", 10.5],
            ]
        },
    )

    connector = ExcelConnector(excel_config, str(tmp_path))

    assert connector.validate_connection() is True


def test_extract_reads_rows_from_configured_sheet(excel_config: ConnectorConfig, tmp_path: Path):
    workbook_path = tmp_path / "sales.xlsx"
    _write_workbook(
        workbook_path,
        {
            "Sales": [
                ["order_id", "revenue", "is_returning_customer"],
                ["A-100", 10.5, True],
                ["A-101", 19, False],
            ],
            "Ignored": [
                ["value"],
                ["skip-me"],
            ],
        },
    )

    connector = ExcelConnector(excel_config, str(tmp_path))

    rows = connector.extract("sales.xlsx")

    assert rows == [
        {
            "order_id": "A-100",
            "revenue": 10.5,
            "is_returning_customer": True,
            "_source_system": "excel",
            "_connector_type": "file",
            "_landing_path": "raw/files/excel",
            "_file_name": "sales.xlsx",
            "_sheet_name": "Sales",
            "_row_number": 2,
        },
        {
            "order_id": "A-101",
            "revenue": 19,
            "is_returning_customer": False,
            "_source_system": "excel",
            "_connector_type": "file",
            "_landing_path": "raw/files/excel",
            "_file_name": "sales.xlsx",
            "_sheet_name": "Sales",
            "_row_number": 3,
        },
    ]


def test_extract_allows_sheet_override(excel_config: ConnectorConfig, tmp_path: Path):
    workbook_path = tmp_path / "sales.xlsx"
    _write_workbook(
        workbook_path,
        {
            "Sales": [
                ["order_id", "revenue"],
                ["A-100", 10.5],
            ],
            "Campaigns": [
                ["campaign", "clicks"],
                ["Spring Sale", 120],
            ],
        },
    )

    connector = ExcelConnector(excel_config, str(tmp_path))
    connector._is_connected = True

    rows = connector.extract("sales.xlsx", {"sheet_name": "Campaigns"})

    assert rows == [
        {
            "campaign": "Spring Sale",
            "clicks": 120,
            "_source_system": "excel",
            "_connector_type": "file",
            "_landing_path": "raw/files/excel",
            "_file_name": "sales.xlsx",
            "_sheet_name": "Campaigns",
            "_row_number": 2,
        }
    ]


def test_invalid_secret_payload_raises(excel_config: ConnectorConfig):
    with pytest.raises(ValueError, match="filesystem path or JSON object"):
        ExcelConnector(excel_config, '["not","a","mapping"]')


def _write_workbook(path: Path, sheets: dict[str, list[list[object]]]) -> None:
    shared_strings: list[str] = []
    shared_string_lookup: dict[str, int] = {}
    worksheet_xml: dict[str, str] = {}

    for index, (sheet_name, rows) in enumerate(sheets.items(), start=1):
        worksheet_xml[f"xl/worksheets/sheet{index}.xml"] = _build_sheet_xml(rows, shared_strings, shared_string_lookup)

    workbook_sheets = []
    workbook_relationships = []
    for index, sheet_name in enumerate(sheets, start=1):
        workbook_sheets.append(
            f'<sheet name="{escape(sheet_name)}" sheetId="{index}" r:id="rId{index}"/>'
        )
        workbook_relationships.append(
            f'<Relationship Id="rId{index}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            f'Target="worksheets/sheet{index}.xml"/>'
        )

    shared_strings_xml = "".join(f"<si><t>{escape(value)}</t></si>" for value in shared_strings)
    with zipfile.ZipFile(path, "w") as workbook:
        workbook.writestr(
            "[Content_Types].xml",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                '<Default Extension="xml" ContentType="application/xml"/>'
                '<Override PartName="/xl/workbook.xml" '
                'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
                '<Override PartName="/xl/sharedStrings.xml" '
                'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>'
                + "".join(
                    f'<Override PartName="/xl/worksheets/sheet{index}.xml" '
                    'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
                    for index in range(1, len(sheets) + 1)
                )
                + "</Types>"
            ),
        )
        workbook.writestr(
            "_rels/.rels",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rId1" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
                'Target="xl/workbook.xml"/>'
                "</Relationships>"
            ),
        )
        workbook.writestr(
            "xl/workbook.xml",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
                f"<sheets>{''.join(workbook_sheets)}</sheets>"
                "</workbook>"
            ),
        )
        workbook.writestr(
            "xl/_rels/workbook.xml.rels",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                f"{''.join(workbook_relationships)}"
                "</Relationships>"
            ),
        )
        workbook.writestr(
            "xl/sharedStrings.xml",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                f'count="{len(shared_strings)}" uniqueCount="{len(shared_strings)}">'
                f"{shared_strings_xml}"
                "</sst>"
            ),
        )
        for worksheet_path, worksheet_content in worksheet_xml.items():
            workbook.writestr(worksheet_path, worksheet_content)


def _build_sheet_xml(
    rows: list[list[object]],
    shared_strings: list[str],
    shared_string_lookup: dict[str, int],
) -> str:
    row_xml: list[str] = []
    for row_index, values in enumerate(rows, start=1):
        cells = []
        for column_index, value in enumerate(values, start=1):
            reference = f"{_column_name(column_index)}{row_index}"
            cells.append(_build_cell_xml(reference, value, shared_strings, shared_string_lookup))
        row_xml.append(f'<row r="{row_index}">{"".join(cells)}</row>')

    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"<sheetData>{''.join(row_xml)}</sheetData>"
        "</worksheet>"
    )


def _build_cell_xml(
    reference: str,
    value: object,
    shared_strings: list[str],
    shared_string_lookup: dict[str, int],
) -> str:
    if isinstance(value, bool):
        return f'<c r="{reference}" t="b"><v>{1 if value else 0}</v></c>'
    if isinstance(value, (int, float)):
        return f'<c r="{reference}"><v>{value}</v></c>'

    string_value = str(value)
    shared_index = shared_string_lookup.get(string_value)
    if shared_index is None:
        shared_index = len(shared_strings)
        shared_string_lookup[string_value] = shared_index
        shared_strings.append(string_value)
    return f'<c r="{reference}" t="s"><v>{shared_index}</v></c>'


def _column_name(column_index: int) -> str:
    name = ""
    current = column_index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        name = chr(ord("A") + remainder) + name
    return name
