"""
Raw → Silver Transformation Layer
Handles: deduplication, type casting, null handling, schema conformance.
Designed for Fabric Notebooks (PySpark).
"""
from typing import Dict, List, Optional, Any


class RawToSilverTransformer:
    """
    Transforms raw (bronze) data into cleansed silver tables.

    Subagents: Override transform_source() for source-specific logic.
    The base class handles common patterns (dedup, schema enforcement).
    """

    def __init__(self, spark_session: Any, config: Dict[str, Any]):
        self.spark = spark_session
        self.config = config
        self.dedup_strategy = config.get("dedup_strategy", "latest_by_timestamp")

    def transform(self, source_name: str, raw_path: str, silver_path: str) -> Dict[str, Any]:
        """
        Main entry point. Reads raw, transforms, writes silver.
        Returns metadata dict for logging/monitoring.
        """
        # Step 1: Read raw data
        df_raw = self.spark.read.parquet(raw_path)
        raw_count = df_raw.count()

        # Step 2: Apply common transformations
        df_cleaned = self._apply_common_transforms(df_raw)

        # Step 3: Apply source-specific transforms
        df_transformed = self.transform_source(source_name, df_cleaned)

        # Step 4: Deduplicate
        df_deduped = self._deduplicate(df_transformed, source_name)

        # Step 5: Write to silver zone
        silver_count = df_deduped.count()
        df_deduped.write.format("delta").mode("overwrite").partitionBy("_processing_date").save(silver_path)

        return {
            "source": source_name,
            "raw_count": raw_count,
            "silver_count": silver_count,
            "dedup_removed": raw_count - silver_count,
            "status": "success",
        }

    def transform_source(self, source_name: str, df: Any) -> Any:
        """
        Override for source-specific logic.
        Default: pass-through (no-op).
        """
        return df

    def _apply_common_transforms(self, df: Any) -> Any:
        """Standard cleaning applied to all sources."""
        # Add processing timestamp
        from pyspark.sql import functions as F

        df = df.withColumn("_processing_date", F.current_date())
        df = df.withColumn("_processing_timestamp", F.current_timestamp())

        # Drop fully null rows
        df = df.dropna(how="all")

        # Trim string columns
        for col_name, col_type in df.dtypes:
            if col_type == "string":
                df = df.withColumn(col_name, F.trim(F.col(col_name)))

        return df

    def _deduplicate(self, df: Any, source_name: str) -> Any:
        """Deduplicate based on configured strategy."""
        from pyspark.sql import Window, functions as F

        dedup_keys = self.config.get("dedup_keys", {}).get(source_name)
        if not dedup_keys:
            return df.dropDuplicates()

        if self.dedup_strategy == "latest_by_timestamp":
            ts_col = self.config.get("timestamp_column", "_processing_timestamp")
            window = Window.partitionBy(*dedup_keys).orderBy(F.desc(ts_col))
            df = df.withColumn("_row_num", F.row_number().over(window))
            df = df.filter(F.col("_row_num") == 1).drop("_row_num")

        return df
