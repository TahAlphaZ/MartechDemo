"""
TDD: Data Quality Tests for Silver (Staging) Zone
Validates business rules, referential integrity, conformance.
"""
import pytest


class TestSilverSchemaConformance:
    """TDD: Silver tables are cleansed and conform to standard schema."""

    def test_silver_has_processing_metadata(self, silver_df):
        required = ["_processing_date", "_processing_timestamp", "_source_system"]
        for col in required:
            assert col in silver_df.columns, f"Silver missing metadata: {col}"

    def test_no_leading_trailing_whitespace(self, silver_df):
        """String columns should be trimmed in silver."""
        from pyspark.sql import functions as F

        for col_name, col_type in silver_df.dtypes:
            if col_type == "string" and not col_name.startswith("_"):
                untrimmed = silver_df.filter(
                    F.col(col_name) != F.trim(F.col(col_name))
                ).count()
                assert untrimmed == 0, f"Column '{col_name}' has {untrimmed} untrimmed values"

    def test_no_duplicate_records(self, silver_df, dedup_key_columns):
        """Silver zone should be deduplicated."""
        if dedup_key_columns:
            total = silver_df.count()
            distinct = silver_df.select(*dedup_key_columns).distinct().count()
            assert total == distinct, f"Silver has {total - distinct} duplicates"


class TestSilverBusinessRules:
    """TDD: Silver data passes business rule validations."""

    def test_order_amounts_are_positive(self, silver_df):
        """Order amounts should be positive (if orders table)."""
        from pyspark.sql import functions as F

        if "total_amount" in silver_df.columns:
            negative = silver_df.filter(F.col("total_amount") < 0).count()
            assert negative == 0, f"Found {negative} orders with negative amounts"

    def test_email_format_valid(self, silver_df):
        """Email columns should match basic email pattern."""
        from pyspark.sql import functions as F

        if "email" in silver_df.columns:
            invalid = silver_df.filter(
                ~F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
                & F.col("email").isNotNull()
            ).count()
            assert invalid == 0, f"Found {invalid} invalid email addresses"

    def test_dates_are_reasonable(self, silver_df):
        """Date columns should not be in the future or before 2000."""
        from pyspark.sql import functions as F

        date_columns = [c for c, t in silver_df.dtypes if t in ("date", "timestamp") and not c.startswith("_")]
        for col_name in date_columns:
            future = silver_df.filter(F.col(col_name) > F.current_date()).count()
            ancient = silver_df.filter(F.col(col_name) < "2000-01-01").count()
            assert future == 0, f"Column '{col_name}' has {future} future dates"
            assert ancient == 0, f"Column '{col_name}' has {ancient} dates before 2000"


@pytest.fixture
def dedup_key_columns():
    return ["id"]

@pytest.fixture
def silver_df(spark_session):
    pytest.skip("Configure silver_df fixture with actual silver zone path")

@pytest.fixture
def spark_session():
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.master("local[*]").appName("silver-tests").getOrCreate()
    except ImportError:
        pytest.skip("PySpark not available")
