---
name: add-gold-model
description: >
  Add a new Gold (curated) data model to the MarTech Fabric Pipeline for BI
  dashboards. Creates fact tables, dimension tables, or aggregation models that
  Power BI consumes. Use this skill whenever the client needs a new dashboard
  metric, KPI, report, or analytical view. Triggers on: "add a dashboard for",
  "create revenue report", "add ROAS metric", "new fact table", "customer
  segmentation model", "campaign attribution model", "add KPI to dashboard",
  "new aggregation for BI", "create dim table", "add marketing metric", or
  any request for new analytical outputs from the pipeline.
---

# Add Gold Data Model

You are adding a new curated data model to the Gold zone of the MarTech Fabric
Pipeline. Gold models are the final output layer — they serve Power BI dashboards
with pre-aggregated, business-ready data.

## What You're Building

A Gold model takes cleansed Silver tables and produces one of three types:

- **Fact tables** — transaction-level data (fact_orders, fact_sessions, fact_campaigns)
- **Dimension tables** — entity lookups (dim_customers, dim_products, dim_dates)
- **Aggregation tables** — pre-computed metrics (agg_daily_revenue, agg_campaign_roas)

Each Gold model is registered in the `GOLD_MODEL_REGISTRY`, has a builder method
in the transformer class, and is validated by TDD data quality tests.

## Before You Start

Read these files:

1. `src/transformations/silver_to_gold/transform.py` — the `SilverToGoldTransformer`
   class and `GOLD_MODEL_REGISTRY`. This is where you add your model.
2. `tests/data-quality/gold/test_gold_quality.py` — existing Gold quality tests.
3. `config/connectors/connector_registry.yaml` — the `medallion.gold` section
   for partitioning and retention config.

## Step-by-Step Process

### 1. Define Your Model in GOLD_MODEL_REGISTRY

In `src/transformations/silver_to_gold/transform.py`, add to the registry:

```python
GOLD_MODEL_REGISTRY["your_model_name"] = {
    "silver_sources": ["source_table_1", "source_table_2"],
    "grain": "date_channel",        # What one row represents
    "description": "Clear description of what this model measures",
}
```

Naming conventions:

- `fact_` prefix for transaction-level fact tables
- `dim_` prefix for dimension/lookup tables
- `agg_` prefix for pre-aggregated metrics
- Use snake_case, be descriptive

The `grain` field is critical — it defines what one row represents. Get this wrong
and your metrics will be inflated or deflated. Common grains:

- `order_line_item` — one row per item in an order
- `session` — one row per user session
- `date_channel` — one row per day per marketing channel
- `customer` — one row per unique customer
- `date_campaign` — one row per day per campaign

### 2. Write the Builder Method

Add a method named `_build_{model_name}` to `SilverToGoldTransformer`:

```python
def _build_your_model_name(self, silver_dfs: Dict[str, Any]) -> Any:
    """
    Build your gold model.
    Explain what this model measures and who consumes it.
    """
    from pyspark.sql import functions as F

    # Get required silver sources
    source1 = silver_dfs.get("source_table_1")
    source2 = silver_dfs.get("source_table_2")

    if source1 is None:
        raise ValueError("source_table_1 silver table required for this model")

    # Build the model
    result = (
        source1
        .join(source2, on="join_key", how="left")      # Join sources
        .withColumn("report_date", F.to_date("date"))   # Always include report_date
        .groupBy("report_date", "dimension_col")         # Aggregate to grain
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("id").alias("record_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
    )

    return result
```

Rules for builder methods:

- Always include a `report_date` column — Gold tables partition on this.
- The output grain must match what's declared in the registry.
- Use descriptive column aliases that match BI naming conventions.
- Handle missing optional sources gracefully (check for None).
- Document the join logic — it's the most common source of metric errors.

### 3. Write Tests FIRST (TDD)

Create `tests/data-quality/gold/test_{model_name}_quality.py`:

```python
import pytest
from pyspark.sql import SparkSession, Row
from datetime import date


class TestYourModelName:
    """TDD: Validate your_model_name Gold output."""

    @pytest.fixture
    def spark(self):
        return SparkSession.builder.master("local[*]").appName("test").getOrCreate()

    @pytest.fixture
    def sample_silver_data(self, spark):
        """Create realistic test data for silver sources."""
        return {
            "source_table_1": spark.createDataFrame([
                Row(id=1, customer_id="c1", date="2024-01-01", amount=100.0, channel="organic"),
                Row(id=2, customer_id="c2", date="2024-01-01", amount=200.0, channel="paid"),
                Row(id=3, customer_id="c1", date="2024-01-02", amount=150.0, channel="organic"),
            ]),
        }

    def test_has_report_date(self, gold_df):
        assert "report_date" in gold_df.columns

    def test_grain_is_correct(self, gold_df):
        """Verify one row per date_channel (no duplicates at grain level)."""
        total = gold_df.count()
        distinct = gold_df.select("report_date", "dimension_col").distinct().count()
        assert total == distinct, "Grain violation: duplicate rows at declared grain"

    def test_metrics_are_not_null(self, gold_df):
        from pyspark.sql import functions as F
        for metric in ["total_amount", "record_count", "unique_customers"]:
            nulls = gold_df.filter(F.col(metric).isNull()).count()
            assert nulls == 0, f"Metric '{metric}' has {nulls} nulls"

    def test_amounts_sum_correctly(self, sample_silver_data, spark):
        """Cross-check: gold totals must match silver source totals."""
        from src.transformations.silver_to_gold.transform import SilverToGoldTransformer
        from pyspark.sql import functions as F

        transformer = SilverToGoldTransformer(spark, {})
        gold_df = transformer._build_your_model_name(sample_silver_data)

        gold_total = gold_df.agg(F.sum("total_amount")).collect()[0][0]
        silver_total = sample_silver_data["source_table_1"].agg(F.sum("amount")).collect()[0][0]
        assert abs(gold_total - silver_total) < 0.01, "Revenue mismatch between gold and silver"

    def test_unique_customers_not_inflated(self, gold_df):
        """Sum of unique_customers across groups >= actual distinct customers."""
        from pyspark.sql import functions as F
        # This is a sanity check — unique_customers per group can overlap
        assert gold_df.agg(F.sum("unique_customers")).collect()[0][0] > 0
```

### 4. Update Silver Sources (If Needed)

If your Gold model requires Silver tables that don't exist yet, you need to
create the Silver transformation first:

1. Check `src/transformations/raw_to_silver/transform.py`
2. Add source-specific transform logic in `transform_source()`
3. Add dedup keys in the config
4. Write Silver-level data quality tests

### 5. Create Power BI Semantic Model Definition

Document the expected semantic model for Power BI:

```yaml
# In your model's documentation or config
semantic_model:
  table: your_model_name
  measures:
    - name: Total Revenue
      expression: SUM([total_amount])
      format: "$#,##0.00"
    - name: Customer Count
      expression: SUM([unique_customers])
    - name: AOV
      expression: DIVIDE(SUM([total_amount]), SUM([record_count]))
  relationships:
    - from: dim_date.date_key
      to: your_model_name.report_date
    - from: dim_channel.channel_name
      to: your_model_name.dimension_col
```

## Common Gold Model Patterns

### Attribution Model
```python
# Join sessions (analytics) with orders (ecommerce) on session_id or utm params
# Grain: date_campaign_channel
# Metrics: sessions, conversions, revenue, ROAS, CPA
```

### Customer Lifetime Value
```python
# Aggregate orders per customer with cohort analysis
# Grain: customer_cohort_month
# Metrics: ltv, order_frequency, avg_basket, retention_rate
```

### Product Performance
```python
# Join orders with product catalog
# Grain: date_product_category
# Metrics: units_sold, revenue, margin, return_rate
```

## Completion Checklist

- [ ] Model registered in `GOLD_MODEL_REGISTRY`
- [ ] Builder method `_build_{name}` implemented
- [ ] `report_date` column present
- [ ] Grain documented and tested
- [ ] Metrics validated against Silver sources
- [ ] TDD tests pass
- [ ] Semantic model documented for Power BI
- [ ] End-to-end verified in dev
