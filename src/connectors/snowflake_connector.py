"""
Snowflake Data Warehouse Connector
Connects to Snowflake for extracting marketing analytics data.
"""
import json
import logging
from typing import Any, Dict, Optional, List
from dataclasses import dataclass

import snowflake.connector
from snowflake.connector import DictCursor

from src.connectors.base_connector import BaseConnector, ConnectorConfig, DatabaseConnector

logger = logging.getLogger(__name__)


@dataclass
class SnowflakeConfig(ConnectorConfig):
    """Extended config for Snowflake-specific settings."""
    account: str = ""
    user: str = ""
    password: str = ""
    warehouse: str = ""
    database: str = ""
    schema: str = ""
    role: str = "TRANSFORMER"


class SnowflakeConnector(DatabaseConnector):
    """
    Snowflake data warehouse connector.
    Supports extracting data from Snowflake tables and views for the marketing analytics pipeline.
    
    Typical usage:
        connector = SnowflakeConnector(config, connection_params)
        data = connector.extract("SELECT * FROM marketing.campaigns LIMIT 1000")
        connector.validate_connection()
    """

    def __init__(self, config: ConnectorConfig, connection_params: Dict[str, str]):
        """
        Initialize Snowflake connector.
        
        Args:
            config: ConnectorConfig with Snowflake settings
            connection_params: Dict with account, user, password, warehouse, database, schema
        """
        super().__init__(config, connection_string=json.dumps(connection_params))
        self.connection_params = connection_params
        self.account = connection_params.get("account", "")
        self.user = connection_params.get("user", "")
        self.password = connection_params.get("password", "")
        self.warehouse = connection_params.get("warehouse", "")
        self.database = connection_params.get("database", "")
        self.schema = connection_params.get("schema", "PUBLIC")
        self.role = connection_params.get("role", "TRANSFORMER")
        self._connection = None

    def _get_connection(self):
        """Get or create Snowflake connection."""
        if self._connection is None:
            try:
                self._connection = snowflake.connector.connect(
                    account=self.account,
                    user=self.user,
                    password=self.password,
                    warehouse=self.warehouse,
                    database=self.database,
                    schema=self.schema,
                    role=self.role,
                    client_session_keep_alive=True,
                )
                logger.info(f"Snowflake connection established: {self.account}.{self.database}.{self.schema}")
            except Exception as e:
                logger.error(f"Failed to connect to Snowflake: {e}")
                raise
        return self._connection

    def validate_connection(self) -> bool:
        """
        Test Snowflake connection with a simple query.
        """
        test_query = self.config.extra.get("test_query", "SELECT 1 as test_value")
        try:
            conn = self._get_connection()
            cursor = conn.cursor(DictCursor)
            cursor.execute(test_query)
            result = cursor.fetchone()
            cursor.close()
            self._is_connected = result is not None
            logger.info(f"Snowflake connection validated for {self.config.name}")
            return True
        except Exception as e:
            logger.error(f"Snowflake connection validation failed: {e}")
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
        Get schema information for the target database/schema.
        Returns table and column metadata from information schema.
        """
        schema_query = """
        SELECT 
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            ORDINAL_POSITION,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
        AND TABLE_CATALOG = %s
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """
        try:
            return self._execute(schema_query, [self.schema, self.database])
        except Exception as e:
            logger.error(f"Failed to retrieve schema: {e}")
            return []

    def _execute(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Execute SQL query against Snowflake.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            List of result rows as dictionaries
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor(DictCursor)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Fetch all results
            results = cursor.fetchall()
            cursor.close()
            
            logger.debug(f"Query executed successfully, returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def get_table_sample(self, table_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get sample data from a Snowflake table.
        Useful for data profiling and validation.
        
        Args:
            table_name: Name of the table (optionally schema-qualified)
            limit: Number of rows to sample
            
        Returns:
            Sample data as list of dictionaries
        """
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return self.extract(query)

    def get_table_count(self, table_name: str) -> int:
        """
        Get row count for a Snowflake table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Number of rows in table
        """
        query = f"SELECT COUNT(*) as row_count FROM {table_name}"
        result = self.extract(query)
        return result[0]["ROW_COUNT"] if result else 0

    def get_available_tables(self) -> List[str]:
        """
        List all tables available in the configured schema.
        
        Returns:
            List of table names
        """
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %s
        AND TABLE_CATALOG = %s
        ORDER BY TABLE_NAME
        """
        results = self._execute(query, [self.schema, self.database])
        return [row["TABLE_NAME"] for row in results]

    def close(self):
        """Close Snowflake connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Snowflake connection closed")

    def __del__(self):
        """Ensure connection is closed on object destruction."""
        self.close()
