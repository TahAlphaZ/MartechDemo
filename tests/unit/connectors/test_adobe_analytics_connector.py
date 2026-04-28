from __future__ import annotations

import pytest

from src.connectors.adobe_analytics_connector import AdobeAnalyticsConnector
from src.connectors.base_connector import ConnectorConfig

ADOBE_SECRET = (
    '{"client_id":"test-client-id","company_id":"exampleco","access_token":"test-token"}'
)


@pytest.fixture
def adobe_config() -> ConnectorConfig:
    return ConnectorConfig(
        name="adobe_analytics",
        connector_type="analytics",
        keyvault_secret="adobe-analytics-jwt",
        landing_path="raw/analytics/adobe_analytics",
        extra={
            "base_url": "https://analytics.adobe.io/api/{company_id}",
            "endpoints": {
                "reports": "/reports",
                "segments": "/segments",
            },
            "default_dimensions": ["variables/daterangeday"],
            "default_metrics": ["metrics/pageviews", "metrics/visits"],
        },
    )


def test_extract_normalizes_report_rows(
    adobe_config: ConnectorConfig, monkeypatch: pytest.MonkeyPatch
):
    connector = AdobeAnalyticsConnector(adobe_config, ADOBE_SECRET)
    connector._is_connected = True

    captured: dict[str, object] = {}

    def fake_make_request(method: str, endpoint: str, params: dict[str, object] | None = None) -> dict[str, object]:
        captured["method"] = method
        captured["endpoint"] = endpoint
        captured["params"] = params
        return {
            "columns": {
                "dimension": {"id": "variables/daterangeday"},
                "columnIds": ["metrics/pageviews", "metrics/visits"],
            },
            "rows": [
                {
                    "value": "2024-01-01",
                    "data": [42, 21],
                }
            ],
        }

    monkeypatch.setattr(connector, "_make_request", fake_make_request)

    rows = connector.extract(
        "reports",
        {
            "rsid": "example-rsid",
            "globalFilters": [{"type": "dateRange", "dateRange": "2024-01-01/2024-01-31"}],
            "metricContainer": {
                "metrics": [
                    {"id": "metrics/pageviews"},
                    {"id": "metrics/visits"},
                ]
            },
        },
    )

    assert captured["method"] == "POST"
    assert captured["endpoint"] == "/reports"
    assert captured["params"] == {
        "rsid": "example-rsid",
        "globalFilters": [{"type": "dateRange", "dateRange": "2024-01-01/2024-01-31"}],
        "metricContainer": {
            "metrics": [
                {"id": "metrics/pageviews"},
                {"id": "metrics/visits"},
            ]
        },
    }
    assert rows == [
        {
            "variables/daterangeday": "2024-01-01",
            "metrics/pageviews": 42,
            "metrics/visits": 21,
            "_source_system": "adobe_analytics",
            "_connector_type": "analytics",
            "_landing_path": "raw/analytics/adobe_analytics",
            "_report_type": "reports",
        }
    ]


def test_extract_normalizes_segment_collection(
    adobe_config: ConnectorConfig, monkeypatch: pytest.MonkeyPatch
):
    connector = AdobeAnalyticsConnector(adobe_config, ADOBE_SECRET)
    connector._is_connected = True

    captured: dict[str, object] = {}

    def fake_make_request(method: str, endpoint: str, params: dict[str, object] | None = None) -> dict[str, object]:
        captured["method"] = method
        captured["endpoint"] = endpoint
        captured["params"] = params
        return {
            "content": [
                {
                    "id": "seg123",
                    "name": "Paid Search",
                }
            ]
        }

    monkeypatch.setattr(connector, "_make_request", fake_make_request)

    rows = connector.extract("segments", {"limit": 25, "page": 0})

    assert captured["method"] == "GET"
    assert captured["endpoint"] == "/segments"
    assert captured["params"] == {"limit": 25, "page": 0}
    assert rows == [
        {
            "id": "seg123",
            "name": "Paid Search",
            "_source_system": "adobe_analytics",
            "_connector_type": "analytics",
            "_landing_path": "raw/analytics/adobe_analytics",
            "_report_type": "segments",
        }
    ]


def test_connector_builds_required_headers(adobe_config: ConnectorConfig):
    connector = AdobeAnalyticsConnector(adobe_config, ADOBE_SECRET)

    assert connector.base_url == "https://analytics.adobe.io/api/exampleco"
    assert connector._build_headers() == {
        "Accept": "application/json",
        "Authorization": "Bearer test-token",
        "Content-Type": "application/json",
        "x-api-key": "test-client-id",
        "x-proxy-global-company-id": "exampleco",
    }


def test_invalid_secret_raises(adobe_config: ConnectorConfig):
    with pytest.raises(ValueError, match="valid JSON"):
        AdobeAnalyticsConnector(adobe_config, "not-json")


def test_missing_access_token_raises(adobe_config: ConnectorConfig):
    connector = AdobeAnalyticsConnector(
        adobe_config,
        '{"client_id":"test-client-id","company_id":"exampleco"}',
    )

    with pytest.raises(ValueError, match="access_token or bearer_token"):
        connector._build_headers()
