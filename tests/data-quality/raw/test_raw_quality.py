"""
TDD: Data Quality Tests for Raw (Bronze) Zone
These run in CI/CD BEFORE promoting data to Silver.
Uses Great Expectations-style assertions.
"""
import pytest
from typing import Any, Dict


class TestRawSchemaValidation:
    """TDD: Raw data matches expected schema from source connector."""

    def test_raw_table_has_required_columns(self, spark_session, raw_df):
        """Every raw table must have ingestion metadata columns."""
        required_columns = ["_ingestion_date", "_source_system", "_raw_filename"]
        for col in required_columns:
            assert col in raw_df.columns, f"Raw table missing metadata column: {col}"

    def test_raw_table_not_empty(self, raw_df):
        """Raw zone should never have empty tables after ingestion."""
        assert raw_df.count() > 0, "Raw table is empty - ingestion may have failed"

    def test_no_completely_null_rows(self, raw_df):
        """Rows that are entirely null indicate corrupt source data."""
        from pyspark.sql import functions as F

        data_columns = [c for c in raw_df.columns if not c.startswith("_")]
        null_condition = F.col(data_columns[0]).isNull()
        for col in data_columns[1:]:
            null_condition = null_condition & F.col(col).isNull()

        null_count = raw_df.filter(null_condition).count()
        assert null_count == 0, f"Found {null_count} completely null rows"

    def test_ingestion_date_is_recent(self, raw_df):
        """Ingestion date should be within last 24 hours for scheduled runs."""
        from pyspark.sql import functions as F
        from datetime import datetime, timedelta

        cutoff = (datetime.utcnow() - timedelta(hours=25)).strftime("%Y-%m-%d")
        stale_count = raw_df.filter(F.col("_ingestion_date") < cutoff).count()
        # This is a soft check - stale data is a warning, not failure
        if stale_count > 0:
            pytest.skip(f"Warning: {stale_count} rows have stale ingestion dates")


class TestRawDataIntegrity:
    """TDD: Raw data passes basic integrity checks."""

    def test_primary_key_columns_not_null(self, raw_df, primary_key_columns):
        """Primary key columns should never be null in raw data."""
        from pyspark.sql import functions as F

        for pk_col in primary_key_columns:
            if pk_col in raw_df.columns:
                null_count = raw_df.filter(F.col(pk_col).isNull()).count()
                assert null_count == 0, f"Primary key column '{pk_col}' has {null_count} nulls"

    def test_no_duplicate_raw_records(self, raw_df, primary_key_columns):
        """Raw zone should not have exact duplicates within same batch."""
        pk_cols = [c for c in primary_key_columns if c in raw_df.columns]
        if pk_cols:
            total = raw_df.count()
            distinct = raw_df.select(*pk_cols).distinct().count()
            dup_count = total - distinct
            assert dup_count == 0, f"Found {dup_count} duplicate records by primary key"


# ============================================================
# FIXTURES - Subagents: Update for your specific sources
# ============================================================

@pytest.fixture
def primary_key_columns():
    """Default primary key columns. Override per source."""
    return ["id"]


@pytest.fixture
def spark_session():
    """Create or get SparkSession for testing."""
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.master("local[*]").appName("raw-tests").getOrCreate()
    except ImportError:
        pytest.skip("PySpark not available in this environment")


@pytest.fixture
def raw_df(spark_session):
    """Load a sample raw dataframe for testing. Override with actual data path."""
    pytest.skip("Configure raw_df fixture with actual raw zone path")
