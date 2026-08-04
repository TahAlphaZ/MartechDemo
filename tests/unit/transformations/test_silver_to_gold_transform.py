from src.transformations.silver_to_gold.transform import SilverToGoldTransformer


class TestCustomerCountryOverrides:
    def test_jimit_raval_email_maps_to_moz(self):
        assert SilverToGoldTransformer.customer_country_for_email("jimit.raval@humbot") == "MOZ"

    def test_email_lookup_is_case_and_whitespace_insensitive(self):
        assert SilverToGoldTransformer.customer_country_for_email("  Jimit.Raval@Humbot  ") == "MOZ"

    def test_unknown_email_has_no_country_override(self):
        assert SilverToGoldTransformer.customer_country_for_email("someone.else@humbot") is None
