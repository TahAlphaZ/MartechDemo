"""
Microsoft Access connector implementation for .mdb/.accdb marketing datasets.
"""
from __future__ import annotations

import importlib
from typing import Any, Dict, List, Optional, Sequence, Tuple

from src.connectors.base_connector import ConnectorConfig, DatabaseConnector


class MicrosoftAccessConnector(DatabaseConnector):
    """Database connector backed by an ODBC connection to a local Access file."""

    DEFAULT_DRIVER = "Microsoft Access Driver (*.mdb, *.accdb)"

    def __init__(self, config: ConnectorConfig, connection_string: str):
        normalized_connection_string = self._build_connection_string(connection_string, config)
        super().__init__(config, connection_string=normalized_connection_string)
        self._connection: Any = None

    @classmethod
    def _build_connection_string(cls, value: str, config: ConnectorConfig) -> str:
        raw_value = value.strip()
        if "=" in raw_value and ";" in raw_value:
            return raw_value if raw_value.endswith(";") else f"{raw_value};"

        driver = config.extra.get("odbc_driver", config.extra.get("driver", cls.DEFAULT_DRIVER))
        parts = [f"Driver={{{driver}}}", f"DBQ={raw_value}"]

        if config.extra.get("readonly", True):
            parts.append("ReadOnly=1")

        database_password = config.extra.get("database_password")
        if database_password:
            parts.append(f"PWD={database_password}")

        return ";".join(parts) + ";"

    @staticmethod
    def _load_pyodbc() -> Any:
        try:
            return importlib.import_module("pyodbc")
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "pyodbc is required for the Microsoft Access connector. "
                "Install the project with the 'access' extra to enable it."
            ) from exc

    @staticmethod
    def _normalize_params(params: Optional[Dict[str, Any]]) -> Tuple[Any, ...]:
        if params is None:
            return ()
        if isinstance(params, dict):
            return tuple(params.values())
        if isinstance(params, Sequence) and not isinstance(params, (str, bytes)):
            return tuple(params)
        return (params,)

    def _connect(self) -> Any:
        if self._connection is None:
            pyodbc = self._load_pyodbc()
            self._connection = pyodbc.connect(self.connection_string)
        return self._connection

    def _execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        connection = self._connect()
        cursor = connection.cursor()
        query_params = self._normalize_params(params)

        if query_params:
            cursor.execute(query, *query_params)
        else:
            cursor.execute(query)

        if cursor.description is None:
            return []

        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_schema(self) -> Dict[str, Any]:
        connection = self._connect()
        tables_cursor = connection.cursor()
        schema: List[Dict[str, Any]] = []
        include_system_tables = self.config.extra.get("include_system_tables", False)

        for table in tables_cursor.tables(tableType="TABLE"):
            table_name = getattr(table, "table_name", None) or table[2]
            if not include_system_tables and str(table_name).startswith("MSys"):
                continue

            columns_cursor = connection.cursor()
            columns = [
                {
                    "column_name": getattr(column, "column_name", None) or column[3],
                    "data_type": getattr(column, "type_name", None) or column[5],
                    "nullable": getattr(column, "nullable", None),
                }
                for column in columns_cursor.columns(table=table_name)
            ]
            schema.append({"table_name": table_name, "columns": columns})

        return {"tables": schema}
