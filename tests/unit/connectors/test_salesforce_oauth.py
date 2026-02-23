import pytest
from unittest.mock import MagicMock
import requests
from src.connectors.base_connector import ConnectorConfig
from src.connectors.salesforce_connector import SalesForceConnector


def test_obtain_access_token(monkeypatch):
    config = ConnectorConfig(
        name="salesforce",
        connector_type="api",
        keyvault_secret="salesforce-oauth-secret",
        landing_path="raw/crm/salesforce",
        extra={"token_url": "https://login.salesforce.com/services/oauth2/token"},
    )

    fake_secret = {
        "client_id": "fake_client",
        "client_secret": "fake_secret",
        "token_url": "https://login.salesforce.com/services/oauth2/token",
    }

    # Prepare fake response
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = {
        "access_token": "fake_token",
        "instance_url": "https://fake.instance",
        "expires_in": 3600,
    }

    called = {}

    def fake_post(url, data=None, headers=None):
        called["url"] = url
        called["data"] = data
        called["headers"] = headers
        return fake_resp

    # Patch requests.post used by connector
    monkeypatch.setattr(requests, "post", fake_post)

    connector = SalesForceConnector(config, fake_secret)
    # Call the internal token acquisition
    connector._obtain_access_token()

    assert connector.access_token == "fake_token"
    assert connector.instance_url == "https://fake.instance"
    assert called["url"] == fake_secret["token_url"]
    # Ensure client credentials were posted
    assert called["data"]["client_id"] == fake_secret["client_id"]
    assert called["data"]["client_secret"] == fake_secret["client_secret"]

