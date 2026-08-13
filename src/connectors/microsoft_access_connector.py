"""
Microsoft Access Database Connector
Connects to Microsoft Access (.mdb, .accdb) files for extracting marketing analytics data.
"""
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, List
from dataclasses import dataclass

try:
    import pyodbc
except ImportError:
    pyodbc = None

from src.connectors.base_connector import BaseConnector, ConnectorConfig, DatabaseConnector

logger = logging.getLogger(__name__)


@dataclass
class MicrosoftAccessConfig(ConnectorConfig):
    """Extended config for Microsoft Access-specific settings."""
    file_path: str = ""
    password: str = ""
    readonly: bool = True


class MicrosoftAccessConnector(DatabaseConnector):
    """
    Microsoft Access database connector.
    Supports extracting data from Access tables for the marketing analytics pipeline.
    Works with both .mdb (Access 97-2003) and .accdb (Access 2007+) formats.
    
    Typical usage:
        connector = MicrosoftAccessConnector(config, connection_params)
        data = connector.extract("SELECT * FROM campaigns LIMIT 1000")
        connector.validate_connection()
    """

    def __init__(self, config: ConnectorConfig, connection_params: Dict[str, str]):
        """
        Initialize Microsoft Access connector.
        
        Args:
            config: ConnectorConfig with Microsoft Access settings
            connection_params: Dict with file_path and optional password
        """
        if pyodbc is None:
            raise ImportError(
                "pyodbc is required for Microsoft Access connector. "
                "Install with: pip install pyodbc"
            )
        
        super().__init__(config, connection_string=json.dumps(connection_params))
        self.connection_params = connection_params
        self.file_path = connection_params.get("file_path", "")
        self.password = connection_params.get("password", "")
        self.readonly = connection_params.get("readonly", True)
        
        # Validate file path
        if not self.file_path:
            raise ValueError("file_path is required for Microsoft Access connector")
        
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Access file not found: {self.file_path}")
        
        # Determine Access driver version based on file extension
        file_ext = Path(self.file_path).suffix.lower()
        if file_ext == ".mdb":
            self.driver = "{Microsoft Access Driver (*.mdb)}"
        elif file_ext == ".accdb":
            self.driver = "{Microsoft Access Driver (*.accdb)}"
        else:
            raise ValueError(
                f"Unsupported Access file format: {file_ext}. "
                "Must be .mdb or .accdb"
            )
        
        self._connection = None

    def _get_connection(self):
        """Get or create Microsoft Access connection."""
        if self._connection is None:
            try:
                # Build connection string for ODBC
                connection_str = f"Driver={self.driver};DBQ={os.path.abspath(self.file_path)};"
                
                if self.password:
                    connection_str += f"PWD={self.password};"
                
                if self.readonly:
                    connection_str += "ReadOnly=1;"
                
                self._connection = pyodbc.connect(connection_str, autocommit=False)
                logger.info(f"Microsoft Access connection established: {self.file_path}")
            except Exception as e:
                logger.error(f"Failed to connect to Microsoft Access: {e}")
                raise
        return self._connection

    def validate_connection(self) -> bool:
        """
        Test Microsoft Access connection with a simple query.
        """
        test_query = self.config.extra.get("test_query", "SELECT 1 as test_value")
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(test_query)
            result = cursor.fetchone()
            cursor.close()
            self._is_connected = result is not None
            logger.info(f"Microsoft Access connection validated for {self.config.name}")
            return True
        except Exception as e:
            logger.error(f"Microsoft Access connection validation failed: {e}")
            self._is_connected = False
            return False

    def extract(self, query_or_endpoint: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Execute SQL query and return results as list of dictionaries.
        
        Args:
            query_or_endpoint: SQL query to execute
            params: Optional parameters for parameterized queries
            
        Returns:
            List of dictionaries representing query results
        """
        if not self._is_connected:
            self.validate_connection()
        return self._execute(query_or_endpoint, params)

    def get_schema(self) -> List[Dict[str, Any]]:
        """
        Get schema information for the Access database.
        Returns table and column metadata using ODBC schema functions.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get table names from Access system tables
            tables = []
            for row in cursor.tables(tableType='TABLE'):
                table_name = row.table_name
                # Skip system tables
                if not table_name.startswith('MSys'):
                    tables.append(table_name)
            
            schema_info = []
            
            # Get column info for each table
            for table_name in tables:
                try:
                    for col in cursor.columns(table=table_name):
                        schema_info.append({
                            "table_name": col.table_name,
                            "column_name": col.column_name,
                            "data_type": str(col.type_name),
                            "ordinal_position": col.ordinal_position,
                            "nullable": col.nullable == 1
                        })
                except Exception as e:
                    logger.warning(f"Could not retrieve schema for table {table_name}: {e}")
            
            cursor.close()
            return schema_info
        except Exception as e:
            logger.error(f"Failed to retrieve schema: {e}")
            return []

    def _execute(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Execute SQL query against Microsoft Access.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            List of result rows as dictionaries
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch all results and convert to dicts
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            cursor.close()
            
            logger.debug(f"Query executed successfully, returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def get_table_sample(self, table_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get sample data from an Access table.
        Useful for data profiling and validation.
        
        Args:
            table_name: Name of the table
            limit: Number of rows to sample
            
        Returns:
            Sample data as list of dictionaries
        """
        query = f"SELECT TOP {limit} * FROM [{table_name}]"
        return self.extract(query)

    def get_table_count(self, table_name: str) -> int:
        """
        Get row count for an Access table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Number of rows in table
        """
        query = f"SELECT COUNT(*) as row_count FROM [{table_name}]"
        result = self.extract(query)
        return result[0]["row_count"] if result else 0

    def get_available_tables(self) -> List[str]:
        """
        List all user tables available in the Access database.
        Excludes system tables.
        
        Returns:
            List of table names
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            tables = []
            for row in cursor.tables(tableType='TABLE'):
                # Skip system tables (start with MSys)
                if not row.table_name.startswith('MSys'):
                    tables.append(row.table_name)
            
            cursor.close()
            return sorted(tables)
        except Exception as e:
            logger.error(f"Failed to retrieve available tables: {e}")
            return []

    def close(self):
        """Close Microsoft Access connection."""
        if self._connection:
            try:
                self._connection.close()
                self._connection = None
                logger.info("Microsoft Access connection closed")
            except Exception as e:
                logger.warning(f"Error closing Microsoft Access connection: {e}")

    def __del__(self):
        """Ensure connection is closed on object destruction."""
        self.close()
