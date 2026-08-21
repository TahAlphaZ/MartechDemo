"""
Databricks Connector - Extract datasets from Databricks workspaces.
Supports Unity Catalog, SQL queries, and DataFrame operations.
"""
import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from src.connectors.base_connector import BaseConnector, ConnectorConfig

logger = logging.getLogger(__name__)


@dataclass
class DatabricksConnectorConfig(ConnectorConfig):
    """Extended config for Databricks-specific settings."""
    workspace_url: str = ""
    http_path: str = ""
    catalog: str = "hive_metastore"
    schema: str = "default"


class DatabricksConnector(BaseConnector):
    """
    Extract data from Databricks using SQL or Spark operations.
    Supports:
    - Unity Catalog tables
    - External delta tables
    - Temporary views
    - SQL queries with parameters
    - Schema introspection
    """

    def __init__(self, config: ConnectorConfig, access_token: str):
        """
        Initialize Databricks connector.

        Args:
            config: ConnectorConfig with Databricks workspace details
            access_token: Personal Access Token (PAT) or OAuth token from Key Vault
        """
        super().__init__(config)
        self.access_token = access_token
        self.workspace_url = config.extra.get("workspace_url", "").rstrip("/")
        self.http_path = config.extra.get("http_path", "/sql/1.0/warehouses/default")
        self.catalog = config.extra.get("catalog", "hive_metastore")
        self.schema = config.extra.get("schema", "default")

        # Lazy import to avoid dependency on databricks-sql-connector if not used
        self._client = None
        self._cursor = None

    def _get_connection(self):
        """Lazy-load and return Databricks SQL connection."""
        if self._client is None:
            try:
                from databricks import sql
                self._client = sql.connect(
                    server_hostname=self._parse_workspace_url(),
                    http_path=self.http_path,
                    auth_type="pat",
                    personal_access_token=self.access_token,
                )
                self._cursor = self._client.cursor()
            except ImportError:
                logger.error(
                    "databricks-sql-connector not installed. "
                    "Install with: pip install databricks-sql-connector"
                )
                raise
        return self._client, self._cursor

    def _parse_workspace_url(self) -> str:
        """Extract hostname from workspace URL."""
        url = self.workspace_url
        if url.startswith("https://"):
            url = url[8:]
        if url.startswith("http://"):
            url = url[7:]
        return url.split("/")[0]

    def validate_connection(self) -> bool:
        """Test connection to Databricks workspace."""
        try:
            client, cursor = self._get_connection()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            self._is_connected = result is not None
            logger.info(f"Connection validated for Databricks workspace: {self.workspace_url}")
            return True
        except Exception as e:
            logger.error(f"Databricks connection failed: {e}")
            self._is_connected = False
            return False

    def extract(self, query_or_endpoint: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Execute SQL query against Databricks and return results as list of dicts.

        Args:
            query_or_endpoint: SQL query to execute. Can include table reference or SQL SELECT.
            params: Optional query parameters for parameterized queries.

        Returns:
            List of dictionaries representing rows.
        """
        if not self._is_connected:
            self.validate_connection()

        try:
            client, cursor = self._get_connection()

            # Support both fully qualified table names and simple queries
            if not query_or_endpoint.lower().strip().startswith("select"):
                # Assume it's a table reference, build qualified name
                table_ref = query_or_endpoint
                if "." not in table_ref:
                    table_ref = f"{self.catalog}.{self.schema}.{table_ref}"
                query = f"SELECT * FROM {table_ref}"
            else:
                query = query_or_endpoint

            if params:
                # Parameterized query support
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # Fetch all results
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            # Convert to list of dicts
            results = [dict(zip(columns, row)) for row in rows]
            logger.info(f"Extracted {len(results)} rows from Databricks")
            return results

        except Exception as e:
            logger.error(f"Error extracting data from Databricks: {e}")
            raise

    def get_schema(self) -> Dict[str, Any]:
        """
        Retrieve schema information from Unity Catalog or Hive Metastore.

        Returns:
            Dictionary mapping table names to column definitions.
        """
        try:
            client, cursor = self._get_connection()

            # Query system tables for schema information
            schema_query = f"""
            SELECT 
                table_name,
                COLLECT_LIST(
                    NAMED_STRUCT('column_name', column_name, 
                                 'data_type', data_type,
                                 'nullable', 'true')
                ) as columns
            FROM system.information_schema.columns
            WHERE table_catalog = '{self.catalog}'
              AND table_schema = '{self.schema}'
            GROUP BY table_name
            """

            cursor.execute(schema_query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            schema = {}
            for row in rows:
                row_dict = dict(zip(columns, row))
                schema[row_dict.get("table_name")] = row_dict.get("columns", [])

            logger.info(f"Retrieved schema for {len(schema)} tables from Databricks")
            return schema

        except Exception as e:
            logger.warning(f"Could not retrieve schema from Databricks: {e}")
            # Return empty schema on error
            return {}

    def list_tables(self) -> List[str]:
        """List all tables in the configured catalog.schema."""
        try:
            client, cursor = self._get_connection()
            cursor.execute(
                f"SHOW TABLES IN {self.catalog}.{self.schema}"
            )
            tables = [row[0] for row in cursor.fetchall()]
            logger.info(f"Found {len(tables)} tables in {self.catalog}.{self.schema}")
            return tables
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the configured schema."""
        try:
            client, cursor = self._get_connection()
            qualified_name = f"{self.catalog}.{self.schema}.{table_name}"
            cursor.execute(f"SELECT COUNT(*) FROM {qualified_name} LIMIT 1")
            return True
        except Exception:
            return False

    def get_row_count(self, table_name: str) -> int:
        """Get row count for a specific table."""
        try:
            qualified_name = f"{self.catalog}.{self.schema}.{table_name}"
            results = self.extract(f"SELECT COUNT(*) as row_count FROM {qualified_name}")
            if results:
                return int(results[0].get("row_count", 0))
            return 0
        except Exception as e:
            logger.error(f"Error getting row count for {table_name}: {e}")
            return -1

    def close(self):
        """Close Databricks connection."""
        try:
            if self._cursor:
                self._cursor.close()
            if self._client:
                self._client.close()
            self._is_connected = False
            logger.info("Databricks connection closed")
        except Exception as e:
            logger.warning(f"Error closing Databricks connection: {e}")

    def __del__(self):
        """Ensure connection is closed on object destruction."""
        self.close()
