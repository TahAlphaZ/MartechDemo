from src.transformations.silver_to_gold.transform import SilverToGoldTransformer


def test_dim_customers_contains_crm_contacts():
    registry = SilverToGoldTransformer.GOLD_MODEL_REGISTRY
    assert "dim_customers" in registry
    sources = registry["dim_customers"]["silver_sources"]
    assert "crm_contacts" in sources
