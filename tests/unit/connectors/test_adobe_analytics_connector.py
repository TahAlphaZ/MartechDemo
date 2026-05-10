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
        "dimension": "variables/daterangeday",
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

    with pytest.raises(ValueError, match="access_token/bearer_token"):
        connector._build_headers()


def test_extract_builds_default_power_bi_report_payload(
    adobe_config: ConnectorConfig, monkeypatch: pytest.MonkeyPatch
):
    connector = AdobeAnalyticsConnector(
        ConnectorConfig(
            name=adobe_config.name,
            connector_type=adobe_config.connector_type,
            keyvault_secret=adobe_config.keyvault_secret,
            landing_path=adobe_config.landing_path,
            extra={
                **adobe_config.extra,
                "report_suite_id": "example-rsid",
                "default_global_filters": [{"type": "dateRange", "dateRange": "2024-01-01/2024-01-31"}],
                "column_aliases": {
                    "variables/daterangeday": "report_date",
                    "metrics/pageviews": "page_views",
                    "metrics/visits": "visits",
                },
            },
        ),
        ADOBE_SECRET,
    )
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
            "rows": [{"value": "2024-01-01", "data": [42, 21]}],
        }

    monkeypatch.setattr(connector, "_make_request", fake_make_request)

    rows = connector.extract("reports")

    assert captured["method"] == "POST"
    assert captured["endpoint"] == "/reports"
    assert captured["params"] == {
        "rsid": "example-rsid",
        "dimension": "variables/daterangeday",
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
            "report_date": "2024-01-01",
            "page_views": 42,
            "visits": 21,
            "_source_system": "adobe_analytics",
            "_connector_type": "analytics",
            "_landing_path": "raw/analytics/adobe_analytics",
            "_report_type": "reports",
        }
    ]


def test_extract_flattens_breakdown_rows_for_power_bi(
    adobe_config: ConnectorConfig, monkeypatch: pytest.MonkeyPatch
):
    connector = AdobeAnalyticsConnector(
        ConnectorConfig(
            name=adobe_config.name,
            connector_type=adobe_config.connector_type,
            keyvault_secret=adobe_config.keyvault_secret,
            landing_path=adobe_config.landing_path,
            extra={
                **adobe_config.extra,
                "column_aliases": {
                    "variables/daterangeday": "report_date",
                    "variables/marketingchannel": "marketing_channel",
                    "metrics/pageviews": "page_views",
                },
            },
        ),
        ADOBE_SECRET,
    )
    connector._is_connected = True

    monkeypatch.setattr(
        connector,
        "_make_request",
        lambda *args, **kwargs: {
            "columns": {
                "dimension": {"id": "variables/daterangeday"},
                "columnIds": ["metrics/pageviews"],
            },
            "rows": [
                {
                    "value": "2024-01-01",
                    "breakdown": [
                        {"value": "Paid Search", "data": [12]},
                        {"value": "Email", "data": [7]},
                    ],
                }
            ],
        },
    )

    rows = connector.extract(
        "reports",
        {
            "rsid": "example-rsid",
            "dimension": "variables/daterangeday",
            "breakdown_dimensions": ["variables/marketingchannel"],
        },
    )

    assert rows == [
        {
            "report_date": "2024-01-01",
            "marketing_channel": "Paid Search",
            "page_views": 12,
            "_source_system": "adobe_analytics",
            "_connector_type": "analytics",
            "_landing_path": "raw/analytics/adobe_analytics",
            "_report_type": "reports",
        },
        {
            "report_date": "2024-01-01",
            "marketing_channel": "Email",
            "page_views": 7,
            "_source_system": "adobe_analytics",
            "_connector_type": "analytics",
            "_landing_path": "raw/analytics/adobe_analytics",
            "_report_type": "reports",
        },
    ]


def test_connector_fetches_oauth_access_token(
    adobe_config: ConnectorConfig, monkeypatch: pytest.MonkeyPatch
):
    connector = AdobeAnalyticsConnector(
        ConnectorConfig(
            name=adobe_config.name,
            connector_type=adobe_config.connector_type,
            keyvault_secret=adobe_config.keyvault_secret,
            landing_path=adobe_config.landing_path,
            extra={
                **adobe_config.extra,
                "token_endpoint": "https://ims-na1.adobelogin.com/ims/token/v3",
                "scopes": ["openid", "analytics_read"],
            },
        ),
        '{"client_id":"test-client-id","client_secret":"test-client-secret","company_id":"exampleco"}',
    )

    captured: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"access_token": "oauth-token", "expires_in": 1800}

    def fake_post(url: str, data: dict[str, object], timeout: int) -> FakeResponse:
        captured["url"] = url
        captured["data"] = data
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(connector._session, "post", fake_post)

    assert connector._build_headers()["Authorization"] == "Bearer oauth-token"
    assert captured == {
        "url": "https://ims-na1.adobelogin.com/ims/token/v3",
        "data": {
            "grant_type": "client_credentials",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "scope": "openid analytics_read",
        },
        "timeout": 30,
    }
