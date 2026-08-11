"""
TDD Tests - Google Analytics 4 Connector
Tests validate:
- Connection validation with service account or OAuth token
- Report extraction with various dimensions, metrics, and date ranges
- Realtime report extraction
- Schema output format
- Property ID resolution (config vs runtime override)
- Payload building for standard and realtime reports
- Factory instantiation creates GoogleAnalyticsConnector
"""
import json
import pytest
from unittest.mock import patch, MagicMock, Mock
from datetime import date, timedelta

from src.connectors.base_connector import ConnectorConfig
from src.connectors.google_analytics_connector import GoogleAnalyticsConnector
from src.connectors.connector_factory import create_connector, CONNECTOR_CLASS_MAP


class TestGoogleAnalyticsConnectorRegistration:
    """TDD: Connector is properly registered in the factory."""

    def test_google_analytics_registered_in_map(self):
        """GoogleAnalyticsConnector must be registered, not generic APIConnector."""
        assert "google_analytics" in CONNECTOR_CLASS_MAP
        assert CONNECTOR_CLASS_MAP["google_analytics"] is GoogleAnalyticsConnector

    def test_factory_creates_google_analytics_connector(self):
        """Factory must instantiate GoogleAnalyticsConnector for google_analytics config."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test-secret",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123456789"},
        )
        connector = create_connector(config, "Bearer ya29.test-token")
        assert isinstance(connector, GoogleAnalyticsConnector)
        assert connector.property_id == "123456789"


class TestGoogleAnalyticsConnection:
    """TDD: Connection validation and authentication."""

    def test_connector_initializes_with_config(self):
        """Connector must initialize with ConnectorConfig and API key."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test-secret",
            landing_path="raw/analytics/google_analytics",
            extra={
                "property_id": "987654321",
                "base_url": "https://analyticsdata.googleapis.com/v1beta",
            },
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        assert connector.config is config
        assert connector.property_id == "987654321"
        assert connector.base_url == "https://analyticsdata.googleapis.com/v1beta"

    def test_connector_strips_base_url_trailing_slash(self):
        """Base URL should be normalized without trailing slash."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"base_url": "https://analyticsdata.googleapis.com/v1beta/", "property_id": "123"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        assert not connector.base_url.endswith("/")

    def test_bearer_token_accepted(self):
        """Connector should accept Bearer tokens."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        token = connector._get_access_token()
        assert token == "ya29.test-token"

    def test_raw_oauth_token_accepted(self):
        """Connector should accept raw OAuth tokens starting with ya29."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123"},
        )
        connector = GoogleAnalyticsConnector(config, "ya29.test-token")
        token = connector._get_access_token()
        assert token == "ya29.test-token"

    @patch("src.connectors.google_analytics_connector.urllib.request.urlopen")
    def test_validate_connection_success(self, mock_urlopen):
        """validate_connection should run a test report and return True on success."""
        mock_response = Mock()
        mock_response.read.return_value = b'{"rowCount": 1}'
        mock_response.__enter__.return_value = mock_response
        mock_response.__exit__.return_value = None
        mock_urlopen.return_value = mock_response

        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123456"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        result = connector.validate_connection()
        assert result is True
        assert connector._is_connected is True

    @patch("src.connectors.google_analytics_connector.urllib.request.urlopen")
    def test_validate_connection_failure(self, mock_urlopen):
        """validate_connection should return False on API error."""
        mock_urlopen.side_effect = Exception("Connection failed")

        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123456"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        result = connector.validate_connection()
        assert result is False
        assert connector._is_connected is False


class TestGoogleAnalyticsExtraction:
    """TDD: Report extraction and data retrieval."""

    @patch("src.connectors.google_analytics_connector.urllib.request.urlopen")
    def test_extract_default_report(self, mock_urlopen):
        """extract() should build and execute a standard report."""
        report_data = {
            "dimensionHeaders": [{"name": "date"}],
            "metricHeaders": [{"name": "sessions", "type": "TYPE_INTEGER"}],
            "rows": [{"dimensionValues": [{"value": "20240101"}], "metricValues": [{"value": "100"}]}],
            "rowCount": 1,
        }
        mock_response = Mock()
        mock_response.read.return_value = json.dumps(report_data).encode("utf-8")
        mock_response.__enter__.return_value = mock_response
        mock_response.__exit__.return_value = None
        mock_urlopen.return_value = mock_response

        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123456"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        result = connector.extract("reports")

        assert "response" in result
        assert result["response"]["rowCount"] == 1
        assert result["property_id"] == "123456"
        assert result["report_type"] == "report"

    @patch("src.connectors.google_analytics_connector.urllib.request.urlopen")
    def test_extract_with_custom_dimensions_metrics(self, mock_urlopen):
        """extract() should accept custom dimensions and metrics."""
        report_data = {"rowCount": 0, "rows": []}
        mock_response = Mock()
        mock_response.read.return_value = json.dumps(report_data).encode("utf-8")
        mock_response.__enter__.return_value = mock_response
        mock_response.__exit__.return_value = None
        mock_urlopen.return_value = mock_response

        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123456"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        result = connector.extract(
            "reports",
            params={
                "dimensions": ["country", "deviceCategory"],
                "metrics": ["sessions", "users"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
        )

        assert result["report_type"] == "report"
        assert "request" in result
        payload = result["request"]
        assert len(payload["dimensions"]) == 2
        assert len(payload["metrics"]) == 2

    @patch("src.connectors.google_analytics_connector.urllib.request.urlopen")
    def test_extract_realtime_report(self, mock_urlopen):
        """extract() should handle realtime endpoint differently."""
        realtime_data = {
            "dimensionHeaders": [{"name": "country"}],
            "metricHeaders": [{"name": "activeUsers", "type": "TYPE_INTEGER"}],
            "rowCount": 10,
        }
        mock_response = Mock()
        mock_response.read.return_value = json.dumps(realtime_data).encode("utf-8")
        mock_response.__enter__.return_value = mock_response
        mock_response.__exit__.return_value = None
        mock_urlopen.return_value = mock_response

        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123456"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        result = connector.extract("realtime")

        assert result["report_type"] == "realtime"
        payload = result["request"]
        assert "dateRanges" not in payload

    def test_property_id_from_config(self):
        """Property ID should come from connector config extra."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "999888777"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        resolved_id = connector._resolve_property_id({})
        assert resolved_id == "999888777"

    def test_property_id_override_at_runtime(self):
        """Property ID passed in params should override config."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "111111"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        resolved_id = connector._resolve_property_id({"property_id": "222222"})
        assert resolved_id == "222222"

    def test_property_id_required(self):
        """Missing property_id should raise ValueError."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        with pytest.raises(ValueError, match="property_id is required"):
            connector._resolve_property_id({})

    def test_endpoint_resolution_reports(self):
        """_resolve_endpoint should map 'reports' to standard endpoint."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        endpoint = connector._resolve_endpoint("reports", "123456")
        assert "properties/123456" in endpoint
        assert ":runReport" in endpoint

    def test_endpoint_resolution_realtime(self):
        """_resolve_endpoint should map 'realtime' to realtime endpoint."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        endpoint = connector._resolve_endpoint("realtime", "123456")
        assert ":runRealtimeReport" in endpoint


class TestGoogleAnalyticsPayloadBuilding:
    """TDD: Payload construction for GA4 API requests."""

    def test_build_report_payload_defaults(self):
        """_build_report_payload should use sensible defaults."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={
                "property_id": "123",
                "default_dimensions": ["date"],
                "default_metrics": ["sessions"],
            },
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        payload = connector._build_report_payload({})

        assert len(payload["dimensions"]) == 1
        assert payload["dimensions"][0]["name"] == "date"
        assert len(payload["metrics"]) == 1
        assert payload["metrics"][0]["name"] == "sessions"
        assert "dateRanges" in payload
        assert payload["limit"] == 10000

    def test_build_report_payload_custom_dates(self):
        """_build_report_payload should use custom date ranges."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        date_range = [{"startDate": "2024-01-01", "endDate": "2024-01-31"}]
        payload = connector._build_report_payload({"date_ranges": date_range})

        assert payload["dateRanges"] == date_range

    def test_build_report_payload_with_filters(self):
        """_build_report_payload should include optional filters."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        dimension_filter = {"fieldName": "country", "stringFilter": {"matchType": "EXACT", "value": "US"}}
        payload = connector._build_report_payload({"dimension_filter": dimension_filter})

        assert payload["dimensionFilter"] == dimension_filter

    def test_build_realtime_payload(self):
        """_build_realtime_payload should omit date ranges."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        payload = connector._build_realtime_payload({})

        assert "dateRanges" not in payload
        assert len(payload["dimensions"]) == 2  # country, deviceCategory
        assert len(payload["metrics"]) == 1  # activeUsers

    def test_build_realtime_payload_custom_dimensions(self):
        """_build_realtime_payload should accept custom dimensions."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        payload = connector._build_realtime_payload({"dimensions": ["city", "browser"]})

        assert len(payload["dimensions"]) == 2
        assert payload["dimensions"][0]["name"] == "city"
        assert payload["dimensions"][1]["name"] == "browser"


class TestGoogleAnalyticsSchema:
    """TDD: Schema output format."""

    def test_get_schema_returns_expected_structure(self):
        """get_schema() should return GA4 response structure."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        schema = connector.get_schema()

        required_fields = ["dimensionHeaders", "metricHeaders", "rows", "rowCount", "metadata"]
        for field in required_fields:
            assert field in schema


class TestGoogleAnalyticsMetadata:
    """TDD: Metadata attachment to extractions."""

    def test_get_metadata_standard_fields(self):
        """get_metadata() should include source system, type, and path."""
        config = ConnectorConfig(
            name="google_analytics",
            connector_type="analytics",
            keyvault_secret="test",
            landing_path="raw/analytics/google_analytics",
            extra={"property_id": "123"},
        )
        connector = GoogleAnalyticsConnector(config, "Bearer ya29.test-token")
        metadata = connector.get_metadata()

        assert metadata["_source_system"] == "google_analytics"
        assert metadata["_connector_type"] == "analytics"
        assert metadata["_landing_path"] == "raw/analytics/google_analytics"
