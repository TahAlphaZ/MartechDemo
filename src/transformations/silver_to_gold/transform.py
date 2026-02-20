"""
Silver → Gold Transformation Layer
Handles: aggregation, business logic, dimensional modeling for BI.
Designed for Fabric Notebooks (PySpark).
"""
from typing import Any, Dict, List


class SilverToGoldTransformer:
    """
    Transforms cleansed silver tables into curated gold aggregates.
    Gold tables are optimized for Power BI consumption.

    Subagents: Add new gold models by extending GOLD_MODEL_REGISTRY.
    """

    # ============================================================
    # GOLD MODEL REGISTRY
    # Subagents: Add new aggregation models here.
    # Format: "model_name": { "silver_sources": [...], "description": "..." }
    # ============================================================
    GOLD_MODEL_REGISTRY = {
        "fact_orders": {
            "silver_sources": ["ecommerce_orders", "ecommerce_payments"],
            "grain": "order_line_item",
            "description": "Order-level fact table with payment status",
        },
        "fact_sessions": {
            "silver_sources": ["analytics_sessions", "analytics_events"],
            "grain": "session",
            "description": "Session-level fact with channel attribution",
        },
        "dim_customers": {
            "silver_sources": ["ecommerce_customers", "crm_contacts"],
            "grain": "customer",
            "description": "Unified customer dimension across sources",
        },
        "dim_products": {
            "silver_sources": ["ecommerce_products", "pim_catalog"],
            "grain": "product_sku",
            "description": "Product master with category hierarchy",
        },
        "agg_daily_revenue": {
            "silver_sources": ["ecommerce_orders"],
            "grain": "date_channel",
            "description": "Daily revenue by marketing channel for BI dashboards",
        },
        "agg_campaign_performance": {
            "silver_sources": ["analytics_sessions", "ecommerce_orders"],
            "grain": "date_campaign",
            "description": "Campaign-level ROAS and conversion metrics",
        },
    }

    def __init__(self, spark_session: Any, config: Dict[str, Any]):
        self.spark = spark_session
        self.config = config

    def build_model(self, model_name: str, silver_base_path: str, gold_path: str) -> Dict[str, Any]:
        """
        Build a single gold model from silver sources.
        Returns execution metadata.
        """
        model_def = self.GOLD_MODEL_REGISTRY.get(model_name)
        if not model_def:
            raise ValueError(
                f"Unknown gold model: '{model_name}'. "
                f"Register it in GOLD_MODEL_REGISTRY."
            )

        # Load silver sources
        silver_dfs = {}
        for source in model_def["silver_sources"]:
            path = f"{silver_base_path}/{source}"
            try:
                silver_dfs[source] = self.spark.read.format("delta").load(path)
            except Exception:
                silver_dfs[source] = None  # Optional source, may not exist yet

        # Dispatch to model-specific builder
        builder_method = getattr(self, f"_build_{model_name}", None)
        if builder_method:
            df_gold = builder_method(silver_dfs)
        else:
            # Default: union available sources
            available = [df for df in silver_dfs.values() if df is not None]
            if not available:
                raise ValueError(f"No silver data available for model '{model_name}'")
            df_gold = available[0]
            for df in available[1:]:
                df_gold = df_gold.unionByName(df, allowMissingColumns=True)

        # Write gold
        row_count = df_gold.count()
        df_gold.write.format("delta").mode("overwrite").partitionBy("report_date").save(gold_path)

        return {
            "model": model_name,
            "grain": model_def["grain"],
            "row_count": row_count,
            "sources_used": [s for s, df in silver_dfs.items() if df is not None],
            "status": "success",
        }

    def build_all_models(self, silver_base_path: str, gold_base_path: str) -> List[Dict]:
        """Build all registered gold models."""
        results = []
        for model_name in self.GOLD_MODEL_REGISTRY:
            try:
                result = self.build_model(
                    model_name, silver_base_path, f"{gold_base_path}/{model_name}"
                )
                results.append(result)
            except Exception as e:
                results.append({"model": model_name, "status": "failed", "error": str(e)})
        return results

    # ============================================================
    # MODEL BUILDERS - Subagents: Add model-specific logic below
    # ============================================================

    def _build_agg_daily_revenue(self, silver_dfs: Dict[str, Any]) -> Any:
        """Example: Daily revenue aggregate for BI."""
        from pyspark.sql import functions as F

        orders = silver_dfs.get("ecommerce_orders")
        if orders is None:
            raise ValueError("ecommerce_orders silver table required")

        return (
            orders.withColumn("report_date", F.to_date("order_date"))
            .groupBy("report_date", "channel", "source")
            .agg(
                F.count("order_id").alias("order_count"),
                F.sum("total_amount").alias("revenue"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.avg("total_amount").alias("aov"),
            )
        )
