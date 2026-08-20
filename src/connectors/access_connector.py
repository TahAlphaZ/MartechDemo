"""
Microsoft Access Database Connector - Read data from .mdb/.accdb files.
Implements BaseConnector for Medallion Architecture ingestion.
"""
import logging
from typing import Any, Dict, List, Optional
from pathlib import Path
import pyodbc

from src.connectors.base_connector import BaseConnector, ConnectorConfig, DatabaseConnector

logger = logging.getLogger(__name__)


class AccessConnector(DatabaseConnector):
    """
    Connector for Microsoft Access databases (.mdb, .accdb files).
    
    Uses ODBC driver to connect to Access files.
    Connection string format: "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=<file_path>"
    
    Example registry config:
        databases:
          active: access
          connectors:
            access:
              driver: "Microsoft Access Driver (*.mdb, *.accdb)"
              file_path: "/mnt/data/sales.accdb"
              keyvault_secret: "access-file-password"  # Optional, for password-protected DBs
              test_query: "SELECT 1"
    """

    def __init__(self, config: ConnectorConfig, connection_string: str):
        super().__init__(config, connection_string)
        self.driver = config.extra.get("driver", "Microsoft Access Driver (*.mdb, *.accdb)")
        self.file_path = config.extra.get("file_path", "")
        self._connection = None

    def validate_connection(self) -> bool:
        """Test connection to Access database file."""
        try:
            if not Path(self.file_path).exists():
                logger.error(f"Access file not found: {self.file_path}")
                return False
                
            test_query = self.config.extra.get("test_query", "SELECT 1")
            self._execute(test_query)
            self._is_connected = True
            logger.info(f"Connection validated for Access database: {self.config.name}")
            return True
        except pyodbc.Error as e:
            logger.error(f"ODBC connection failed for {self.config.name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Connection failed for {self.config.name}: {e}")
            return False

    def extract(self, query_or_endpoint: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Execute SQL query against Access database and return results as list of dicts.
        
        Args:
            query_or_endpoint: SQL query string
            params: Optional query parameters (not used in basic implementation)
            
        Returns:
            List of dictionaries representing table rows
        """
        if not self._is_connected:
            self.validate_connection()
        return self._execute(query_or_endpoint, params)

    def get_schema(self) -> Dict[str, Any]:
        """
        Introspect Access database schema.
        Returns information about all tables and columns.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            schema_info = {}
            
            # Get all tables from Access database
            tables = cursor.tables()
            for table in tables:
                table_name = table.table_name
                if table_name.startswith("~"):  # Skip system tables
                    continue
                    
                # Get columns for each table
                columns = cursor.columns(table_name)
                schema_info[table_name] = [
                    {
                        "column_name": col.column_name,
                        "data_type": col.type_name,
                        "column_size": col.column_size,
                        "nullable": col.nullable,
                    }
                    for col in columns
                ]
            
            return schema_info
        except Exception as e:
            logger.error(f"Failed to introspect schema for {self.config.name}: {e}")
            return {}

    def get_tables(self) -> List[str]:
        """Get list of all table names in the Access database."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            tables = cursor.tables()
            return [table.table_name for table in tables if not table.table_name.startswith("~")]
        except Exception as e:
            logger.error(f"Failed to get tables from {self.config.name}: {e}")
            return []

    def _execute(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Execute SQL query and return results as list of dictionaries.
        
        Args:
            query: SQL query string
            params: Optional parameters (for parameterized queries)
            
        Returns:
            List of dicts with column names as keys
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, list(params.values()))
            else:
                cursor.execute(query)
            
            # Get column names from cursor description
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch all rows and convert to list of dicts
            rows = cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]
            
            return results
        except pyodbc.Error as e:
            logger.error(f"ODBC execution error for {self.config.name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Query execution failed for {self.config.name}: {e}")
            raise

    def _get_connection(self) -> pyodbc.Connection:
        """
        Get or create ODBC connection to Access database.
        Reuses connection if already established.
        """
        if self._connection is None or not self._connection.connected:
            try:
                # Build connection string
                if self.connection_string:
                    # Use provided connection string (from Key Vault)
                    conn_str = self.connection_string
                else:
                    # Build connection string from config
                    password_part = ""
                    if "password" in self.config.extra:
                        password_part = f";PWD={self.config.extra['password']}"
                    
                    conn_str = f"Driver={{{self.driver}}};DBQ={self.file_path}{password_part}"
                
                self._connection = pyodbc.connect(conn_str, autocommit=True)
                logger.debug(f"Established ODBC connection to {self.file_path}")
            except pyodbc.Error as e:
                logger.error(f"Failed to create ODBC connection: {e}")
                raise
        
        return self._connection

    def close(self):
        """Close the database connection."""
        if self._connection is not None:
            try:
                self._connection.close()
                self._connection = None
                logger.info(f"Closed connection to {self.config.name}")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")

    def __del__(self):
        """Ensure connection is closed on garbage collection."""
        self.close()
