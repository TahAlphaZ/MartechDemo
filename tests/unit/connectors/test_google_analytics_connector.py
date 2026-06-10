from __future__ import annotations

import pytest

from src.connectors.base_connector import ConnectorConfig
from src.connectors.google_analytics_connector import GoogleAnalyticsConnector

SERVICE_ACCOUNT_SECRET = (
    '{"type":"service_account","client_email":"test@example.com",'
    '"token_uri":"https://oauth2.googleapis.com/token"}'
)


@pytest.fixture
def ga_config() -> ConnectorConfig:
    return ConnectorConfig(
        name="google_analytics",
        connector_type="analytics",
        keyvault_secret="ga4-service-account-json",
        landing_path="raw/analytics/google_analytics",
        extra={
            "base_url": "https://analyticsdata.googleapis.com/v1beta",
            "property_id": "123456789",
            "endpoints": {
                "reports": "/properties/{property_id}:runReport",
                "realtime": "/properties/{property_id}:runRealtimeReport",
            },
            "default_dimensions": ["date", "sessionSource"],
            "default_metrics": ["sessions", "totalUsers"],
        },
    )


def test_extract_builds_default_report_payload_and_normalizes_rows(
    ga_config: ConnectorConfig, monkeypatch: pytest.MonkeyPatch
):
    connector = GoogleAnalyticsConnector(
        ga_config,
        SERVICE_ACCOUNT_SECRET,
    )

    captured: dict[str, object] = {}

    def fake_validate_connection() -> bool:
        connector._is_connected = True
        return True

    def fake_make_request(method: str, endpoint: str, params: dict[str, object] | None = None) -> dict[str, object]:
        captured["method"] = method
        captured["endpoint"] = endpoint
        captured["params"] = params
        return {
            "dimensionHeaders": [{"name": "date"}, {"name": "sessionSource"}],
            "metricHeaders": [{"name": "sessions"}, {"name": "totalUsers"}],
            "rows": [
                {
                    "dimensionValues": [{"value": "2024-01-01"}, {"value": "google"}],
                    "metricValues": [{"value": "42"}, {"value": "24"}],
                }
            ],
        }

    monkeypatch.setattr(connector, "validate_connection", fake_validate_connection)
    monkeypatch.setattr(connector, "_make_request", fake_make_request)

    rows = connector.extract("reports")

    assert captured["method"] == "POST"
    assert captured["endpoint"] == "/properties/123456789:runReport"
    assert captured["params"] == {
        "dimensions": [{"name": "date"}, {"name": "sessionSource"}],
        "metrics": [{"name": "sessions"}, {"name": "totalUsers"}],
        "dateRanges": [{"startDate": "7daysAgo", "endDate": "yesterday"}],
    }
    assert rows == [
        {
            "date": "2024-01-01",
            "sessionSource": "google",
            "sessions": "42",
            "totalUsers": "24",
            "_source_system": "google_analytics",
            "_connector_type": "analytics",
            "_landing_path": "raw/analytics/google_analytics",
            "_report_type": "reports",
        }
    ]


def test_extract_allows_property_override_for_realtime(
    ga_config: ConnectorConfig, monkeypatch: pytest.MonkeyPatch
):
    connector = GoogleAnalyticsConnector(
        ga_config,
        SERVICE_ACCOUNT_SECRET,
    )
    connector._is_connected = True

    captured: dict[str, object] = {}

    def fake_make_request(method: str, endpoint: str, params: dict[str, object] | None = None) -> dict[str, object]:
        captured["endpoint"] = endpoint
        captured["params"] = params
        return {"dimensionHeaders": [], "metricHeaders": [], "rows": []}

    monkeypatch.setattr(connector, "_make_request", fake_make_request)

    connector.extract(
        "realtime",
        {
            "property_id": "987654321",
            "dimensions": [{"name": "city"}],
            "metrics": [{"name": "activeUsers"}],
            "limit": 10,
        },
    )

    assert captured["endpoint"] == "/properties/987654321:runRealtimeReport"
    assert captured["params"] == {
        "dimensions": [{"name": "city"}],
        "metrics": [{"name": "activeUsers"}],
        "limit": 10,
    }


def test_extract_requires_property_id(monkeypatch: pytest.MonkeyPatch):
    config = ConnectorConfig(
        name="google_analytics",
        connector_type="analytics",
        keyvault_secret="ga4-service-account-json",
        landing_path="raw/analytics/google_analytics",
        extra={
            "base_url": "https://analyticsdata.googleapis.com/v1beta",
            "endpoints": {"reports": "/properties/{property_id}:runReport"},
        },
    )
    connector = GoogleAnalyticsConnector(
        config,
        SERVICE_ACCOUNT_SECRET,
    )
    connector._is_connected = True
    monkeypatch.setattr(connector, "_make_request", lambda *args, **kwargs: {})

    with pytest.raises(ValueError, match="property_id"):
        connector.extract("reports")


def test_invalid_service_account_secret_raises(ga_config: ConnectorConfig):
    with pytest.raises(ValueError, match="valid service account JSON"):
        GoogleAnalyticsConnector(ga_config, "not-json")
