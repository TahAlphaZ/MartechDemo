import pytest
from unittest.mock import MagicMock
from datetime import datetime
from src.connectors.base_connector import ConnectorConfig
from src.connectors.salesforce_connector import SalesForceConnector


def test_build_soql_no_last_sync():
    cfg = ConnectorConfig(name="salesforce", connector_type="api", keyvault_secret="s", landing_path="raw/crm/salesforce")
    connector = SalesForceConnector(cfg, {"client_id": "x", "client_secret": "y", "token_url": "t"})

    soql = connector.build_soql("Contact")
    assert "FROM Contact" in soql
    assert "SystemModstamp" in soql
    assert "WHERE" not in soql.split("ORDER BY")[0]


def test_build_soql_with_last_sync_string_and_datetime():
    cfg = ConnectorConfig(name="salesforce", connector_type="api", keyvault_secret="s", landing_path="raw/crm/salesforce")
    connector = SalesForceConnector(cfg, {"client_id": "x", "client_secret": "y", "token_url": "t"})

    ts = "2024-01-01T00:00:00Z"
    soql = connector.build_soql("Lead", last_sync=ts)
    assert "FROM Lead" in soql
    assert "SystemModstamp" in soql
    assert ts in soql
    assert "ORDER BY SystemModstamp" in soql

    # datetime
    dt = datetime(2024, 1, 1)
    soql2 = connector.build_soql("Account", last_sync=dt)
    assert "FROM Account" in soql2
    assert "SystemModstamp" in soql2
    assert "ORDER BY SystemModstamp" in soql2


def test_extract_pagination_and_ids(monkeypatch):
    cfg = ConnectorConfig(name="salesforce", connector_type="api", keyvault_secret="s", landing_path="raw/crm/salesforce")
    connector = SalesForceConnector(cfg, {"client_id": "x", "client_secret": "y", "token_url": "t"})

    # Skip OAuth
    connector.access_token = "fake"
    connector.instance_url = "https://fake.instance"

    # Prepare paginated responses returned by _make_request
    responses = [
        {"data": [{"Id": "1"}], "next_page_url": "next"},
        {"data": [{"Id": "2"}], "next_page_url": None},
    ]

    calls = {"i": 0}

    def fake_make_request(method, endpoint, params=None):
        i = calls["i"]
        calls["i"] += 1
        return responses[i]

    monkeypatch.setattr(connector, "_make_request", fake_make_request)

    results = connector.extract("Contact", params={"last_sync": "2024-01-01T00:00:00Z"})
    assert isinstance(results, list)
    assert len(results) == 2
    ids = {r["Id"] for r in results}
    assert ids == {"1", "2"}


def test_get_entity_landing_name_mapping():
    cfg = ConnectorConfig(name="salesforce", connector_type="api", keyvault_secret="s", landing_path="raw/crm/salesforce")
    connector = SalesForceConnector(cfg, {"client_id": "x", "client_secret": "y", "token_url": "t"})

    assert connector.get_entity_landing_name("Opportunity") == "crm_deals"
    assert connector.get_entity_landing_name("Account") == "crm_companies"
