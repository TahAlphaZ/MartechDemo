"""
SQL-driven Gold Transformer

This module provides a small helper class that:
 - fetches a SQL statement stored in an Azure Blob (e.g. blob named "Test-use-case")
 - registers silver Delta tables as temp views
 - executes the SQL against those views using PySpark
 - writes resulting DataFrame into a gold Delta path

Design notes:
 - The blob access layer uses azure.storage.blob when available. Callers should supply
   either a connection string or an (account_url, credential) pair (for DefaultAzureCredential).
 - Table discovery supports two modes:
    1) explicit list of table names provided in config
    2) automatic discovery when silver_base_path is local filesystem (uses os.listdir)
 - This module intentionally avoids secret management; credentials should be resolved
   by the runtime (Key Vault / environment) and passed to the fetch_sql_from_blob call.

Example usage:
    from src.transformations.sql_to_gold import SQLDrivenTransformer

    transformer = SQLDrivenTransformer(spark, config={})
    sql_text = transformer.fetch_sql_from_blob(
        container_name="my-container",
        blob_name="Test-use-case.sql",
        connection_string=os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    )
    transformer.register_silver_tables("/mnt/adls/silver", table_list=["ecommerce_orders", "analytics_sessions"])
    result = transformer.run_sql_to_gold(sql_text, gold_output_path="/mnt/adls/gold/agg_daily_revenue", partition_by=["report_date"]) 

"""
from typing import Any, Dict, List, Optional
import os
import logging

logger = logging.getLogger(__name__)


class SQLDrivenTransformer:
    """Execute SQL fetched from storage against silver tables and write gold output."""

    def __init__(self, spark_session: Any, config: Optional[Dict[str, Any]] = None):
        self.spark = spark_session
        self.config = config or {}

    def fetch_sql_from_blob(
        self,
        container_name: str,
        blob_name: str,
        connection_string: Optional[str] = None,
        account_url: Optional[str] = None,
        credential: Optional[Any] = None,
    ) -> str:
        """Fetch blob contents as text.

        Parameters:
        - container_name: name of the blob container
        - blob_name: blob name (path inside container)
        - connection_string: optional connection string for azure.storage.blob
        - account_url + credential: alternative auth (e.g., DefaultAzureCredential)

        Returns SQL text as str. Raises RuntimeError on failure.
        """
        # Lazy import to avoid hard dependency when not used in unit tests
        try:
            from azure.storage.blob import BlobClient
        except Exception as exc:  # pragma: no cover - dependency/runtime
            raise RuntimeError(
                "azure.storage.blob library is required to fetch SQL from blob storage: "
                f"{exc}"
            )

        if connection_string:
            blob = BlobClient.from_connection_string(
                conn_str=connection_string, container_name=container_name, blob_name=blob_name
            )
        elif account_url and credential:
            # account_url should be like https://<account>.blob.core.windows.net
            blob = BlobClient(account_url=account_url, container_name=container_name, blob_name=blob_name, credential=credential)
        else:
            raise ValueError("Either connection_string or (account_url and credential) must be provided")

        try:
            stream = blob.download_blob()
            content = stream.content_as_text(encoding="utf-8")
            return content
        except Exception as e:
            logger.exception("Failed to download SQL blob %s/%s: %s", container_name, blob_name, e)
            raise RuntimeError(f"Failed to download SQL blob {container_name}/{blob_name}: {e}")

    def register_silver_tables(self, silver_base_path: str, table_list: Optional[List[str]] = None) -> List[str]:
        """Register silver tables as temp views so SQL can reference them by name.

        Parameters:
        - silver_base_path: base file path or mount point where silver delta folders live
        - table_list: explicit list of table names to register. If omitted and silver_base_path
                      is a local filesystem path, the transformer will attempt to auto-discover
                      subdirectories and register them.

        Returns the list of registered table names.
        """
        registered: List[str] = []

        if table_list:
            for t in table_list:
                path = os.path.join(silver_base_path, t)
                logger.info("Loading silver table '%s' from %s", t, path)
                df = self.spark.read.format("delta").load(path)
                df.createOrReplaceTempView(t)
                registered.append(t)
            return registered

        # Attempt local discovery
        try:
            entries = os.listdir(silver_base_path)
        except Exception as e:
            raise RuntimeError(
                "Table list not provided and automatic discovery failed. "
                "For remote stores provide an explicit table_list in the call."
            )

        for entry in entries:
            entry_path = os.path.join(silver_base_path, entry)
            if os.path.isdir(entry_path):
                try:
                    df = self.spark.read.format("delta").load(entry_path)
                    df.createOrReplaceTempView(entry)
                    registered.append(entry)
                except Exception:
                    logger.warning("Skipping entry %s - not a Delta table or unreadable", entry_path)

        if not registered:
            logger.warning("No silver tables were registered from %s", silver_base_path)

        return registered

    def run_sql_to_gold(
        self,
        sql: str,
        gold_output_path: str,
        partition_by: Optional[List[str]] = None,
        mode: str = "overwrite",
    ) -> Dict[str, Any]:
        """Execute SQL and write resulting DataFrame to the gold path.

        Returns metadata dict.
        """
        if not sql or not sql.strip():
            raise ValueError("SQL statement must be provided")

        logger.info("Executing SQL-driven transformation")
        df = self.spark.sql(sql)

        row_count = df.count()

        writer = df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(gold_output_path)

        return {"row_count": row_count, "gold_path": gold_output_path, "status": "success"}
