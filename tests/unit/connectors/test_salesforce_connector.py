import pytest

from src.connectors.salesforce_connector import SalesforceConnector
from src.connectors.base_connector import ConnectorConfig, APIConnector
from src.connectors.connector_factory import create_connector


class TestSalesforceConnector:
    def test_salesforce_inherits_api_connector(self):
        config = ConnectorConfig(
            name="salesforce",
            connector_type="api",
            keyvault_secret="salesforce-oauth-token",
            landing_path="raw/crm/salesforce",
            extra={"base_url": "https://test.salesforce.com"},
        )
        connector = SalesforceConnector(config, api_key="test-token")
        assert isinstance(connector, APIConnector)

    def test_validate_connection_success(self):
        config = ConnectorConfig(
            name="salesforce",
            connector_type="api",
            keyvault_secret="salesforce-oauth-token",
            landing_path="raw/crm/salesforce",
            extra={"base_url": "https://test.salesforce.com"},
        )
        connector = SalesforceConnector(config, api_key="valid-token")
        assert connector.validate_connection() is True

    def test_get_schema_contains_entities(self):
        config = ConnectorConfig(
            name="salesforce",
            connector_type="api",
            keyvault_secret="salesforce-oauth-token",
            landing_path="raw/crm/salesforce",
            extra={"base_url": "https://test.salesforce.com"},
        )
        connector = SalesforceConnector(config, api_key="valid-token")
        schema = connector.get_schema()
        assert "accounts" in schema
        assert "contacts" in schema
        assert "opportunities" in schema

    def test_factory_creates_salesforce_connector(self):
        config = ConnectorConfig(
            name="salesforce",
            connector_type="api",
            keyvault_secret="salesforce-oauth-token",
            landing_path="raw/crm/salesforce",
            extra={"base_url": "https://test.salesforce.com"},
        )
        connector = create_connector(config, "valid-token")
        assert isinstance(connector, SalesforceConnector)
