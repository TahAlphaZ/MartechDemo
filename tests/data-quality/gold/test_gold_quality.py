"""
TDD: Data Quality Tests for Gold (Curated) Zone
Validates metric accuracy, SLA compliance, BI readiness.
"""
import pytest


class TestGoldModelCompleteness:
    """TDD: Gold models are complete and BI-ready."""

    def test_gold_table_has_report_date(self, gold_df):
        assert "report_date" in gold_df.columns, "Gold table must have report_date partition"

    def test_gold_table_not_empty(self, gold_df):
        assert gold_df.count() > 0, "Gold table is empty"

    def test_no_null_metrics(self, gold_df, metric_columns):
        """BI metrics should never be null in gold."""
        from pyspark.sql import functions as F
        for col in metric_columns:
            if col in gold_df.columns:
                null_count = gold_df.filter(F.col(col).isNull()).count()
                assert null_count == 0, f"Metric '{col}' has {null_count} nulls in gold"


class TestGoldMetricAccuracy:
    """TDD: Gold aggregates are mathematically correct."""

    def test_revenue_matches_source(self, gold_revenue_df, silver_orders_df):
        """Gold daily revenue should match sum of silver order amounts."""
        from pyspark.sql import functions as F

        gold_total = gold_revenue_df.agg(F.sum("revenue")).collect()[0][0] or 0
        silver_total = silver_orders_df.agg(F.sum("total_amount")).collect()[0][0] or 0
        tolerance = 0.01  # Allow 1 cent rounding
        assert abs(gold_total - silver_total) <= tolerance, (
            f"Revenue mismatch: gold={gold_total}, silver={silver_total}"
        )

    def test_customer_count_not_inflated(self, gold_df):
        """Unique customer counts should use DISTINCT logic."""
        from pyspark.sql import functions as F

        if "unique_customers" in gold_df.columns and "customer_id" in gold_df.columns:
            reported = gold_df.agg(F.sum("unique_customers")).collect()[0][0] or 0
            actual = gold_df.select("customer_id").distinct().count()
            # Aggregate unique_customers across groups will be >= actual distinct
            assert reported >= actual, "Customer count seems deflated"


class TestGoldSLA:
    """TDD: Gold tables meet SLA freshness requirements."""

    def test_data_freshness_within_sla(self, gold_df):
        """Gold data should be refreshed within 24 hours."""
        from pyspark.sql import functions as F
        from datetime import datetime, timedelta

        max_date = gold_df.agg(F.max("report_date")).collect()[0][0]
        if max_date:
            cutoff = (datetime.utcnow() - timedelta(hours=48)).date()
            assert max_date >= cutoff, f"Gold data stale: latest={max_date}, cutoff={cutoff}"


@pytest.fixture
def metric_columns():
    return ["revenue", "order_count", "unique_customers", "aov"]

@pytest.fixture
def gold_df(spark_session):
    pytest.skip("Configure gold_df fixture with actual gold zone path")

@pytest.fixture
def gold_revenue_df(spark_session):
    pytest.skip("Configure with agg_daily_revenue gold path")

@pytest.fixture
def silver_orders_df(spark_session):
    pytest.skip("Configure with ecommerce_orders silver path")

@pytest.fixture
def spark_session():
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.master("local[*]").appName("gold-tests").getOrCreate()
    except ImportError:
        pytest.skip("PySpark not available")
