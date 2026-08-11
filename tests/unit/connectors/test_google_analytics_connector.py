"""
Unit tests for Google Analytics Connector.
Tests connection validation, data extraction, and schema generation.
"""
import json
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch, call
from google.auth.credentials import Credentials

from src.connectors.google_analytics_connector import GoogleAnalyticsConnector
from src.connectors.base_connector import ConnectorConfig


@pytest.fixture
def ga_config():
    """Create a test Google Analytics connector config."""
    return ConnectorConfig(
        name="google_analytics",
        connector_type="analytics",
        keyvault_secret="ga-test-secret",
        landing_path="raw/analytics/google_analytics",
        extra={
            "base_url": "https://analyticsreporting.googleapis.com/v4",
            "property_id": "1234567890",
            "dimensions": ["date", "country", "deviceCategory"],
            "metrics": ["sessions", "users", "pageviews"],
            "date_range_days": 1,
            "rate_limit": 10
        }
    )


@pytest.fixture
def mock_service_account_key():
    """Create a mock service account JSON key."""
    return json.dumps({
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "key123",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDU8rTQu3k/jcZi\n-----END PRIVATE KEY-----\n",
        "client_email": "test@test-project.iam.gserviceaccount.com",
        "client_id": "123456789",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token"
    })


@pytest.mark.unit
class TestGoogleAnalyticsConnector:
    """Unit tests for GoogleAnalyticsConnector."""

    def test_connector_initialization(self, ga_config, mock_service_account_key):
        """Test that connector initializes with valid config."""
        with patch('src.connectors.google_analytics_connector.Credentials.from_service_account_info'):
            connector = GoogleAnalyticsConnector(ga_config, mock_service_account_key)
            
            assert connector.config.name == "google_analytics"
            assert connector.property_id == "1234567890"
            assert connector.dimensions == ["date", "country", "deviceCategory"]
            assert connector.metrics == ["sessions", "users", "pageviews"]
            assert connector.date_range_days == 1

    def test_validate_connection_success(self, ga_config, mock_service_account_key):
        """Test successful connection validation."""
        with patch('src.connectors.google_analytics_connector.Credentials.from_service_account_info') as mock_creds, \
             patch('src.connectors.google_analytics_connector.requests.get') as mock_get:
            
            mock_creds.return_value = MagicMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_get.return_value = mock_response
            
            connector = GoogleAnalyticsConnector(ga_config, mock_service_account_key)
            result = connector.validate_connection()
            
            assert result is True
            assert connector._is_connected is True

    def test_validate_connection_failure(self, ga_config, mock_service_account_key):
        """Test connection validation with API error."""
        with patch('src.connectors.google_analytics_connector.Credentials.from_service_account_info') as mock_creds, \
             patch('src.connectors.google_analytics_connector.requests.get') as mock_get:
            
            mock_creds.return_value = MagicMock()
            mock_response = MagicMock()
            mock_response.status_code = 401
            mock_get.return_value = mock_response
            
            connector = GoogleAnalyticsConnector(ga_config, mock_service_account_key)
            result = connector.validate_connection()
            
            assert result is False

    def test_get_schema(self, ga_config, mock_service_account_key):
        """Test schema generation."""
        with patch('src.connectors.google_analytics_connector.Credentials.from_service_account_info'):
            connector = GoogleAnalyticsConnector(ga_config, mock_service_account_key)
            schema = connector.get_schema()
            
            assert schema["dimensions"] == ["date", "country", "deviceCategory"]
            assert schema["metrics"] == ["sessions", "users", "pageviews"]
            assert "sample_row" in schema
            assert schema["sample_row"]["date"] == "<date_value>"
            assert schema["sample_row"]["sessions"] == "<numeric_value>"

    def test_get_metadata(self, ga_config, mock_service_account_key):
        """Test metadata generation."""
        with patch('src.connectors.google_analytics_connector.Credentials.from_service_account_info'):
            connector = GoogleAnalyticsConnector(ga_config, mock_service_account_key)
            metadata = connector.get_metadata()
            
            assert metadata["_source_system"] == "google_analytics"
            assert metadata["_connector_type"] == "analytics"
            assert metadata["_ga_property_id"] == "1234567890"
            assert "_ga_dimensions" in metadata
            assert "_ga_metrics" in metadata

    @patch('src.connectors.google_analytics_connector.requests.post')
    def test_extract_single_page(self, mock_post, ga_config, mock_service_account_key):
        """Test data extraction with single page of results."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "rows": [
                {
                    "dimensions": ["2024-01-01", "US", "mobile"],
                    "metrics": [["100", "50", "200"]]
                },
                {
                    "dimensions": ["2024-01-01", "UK", "desktop"],
                    "metrics": [["150", "75", "300"]]
                }
            ]
        }
        mock_post.return_value = mock_response
        
        with patch('src.connectors.google_analytics_connector.Credentials.from_service_account_info') as mock_creds:
            mock_creds.return_value = MagicMock()
            
            connector = GoogleAnalyticsConnector(ga_config, mock_service_account_key)
            connector._is_connected = True
            connector.access_token = "test_token"
            
            result = connector.extract()
            
            assert len(result) == 2
            assert result[0]["dimensions"] == ["2024-01-01", "US", "mobile"]

    @patch('src.connectors.google_analytics_connector.requests.post')
    @patch('src.connectors.google_analytics_connector.time.sleep')
    def test_extract_multiple_pages(self, mock_sleep, mock_post, ga_config, mock_service_account_key):
        """Test data extraction with pagination."""
        page1_response = MagicMock()
        page1_response.status_code = 200
        page1_response.json.return_value = {
            "rows": [{"dimensions": ["page1"], "metrics": [["100"]]}],
            "nextPageToken": "token_page2"
        }
        
        page2_response = MagicMock()
        page2_response.status_code = 200
        page2_response.json.return_value = {
            "rows": [{"dimensions": ["page2"], "metrics": [["200"]]}]
        }
        
        mock_post.side_effect = [page1_response, page2_response]
        
        with patch('src.connectors.google_analytics_connector.Credentials.from_service_account_info') as mock_creds:
            mock_creds.return_value = MagicMock()
            
            connector = GoogleAnalyticsConnector(ga_config, mock_service_account_key)
            connector._is_connected = True
            connector.access_token = "test_token"
            
            result = connector.extract()
            
            assert len(result) == 2
            assert mock_post.call_count == 2
            mock_sleep.assert_called()

    def test_extract_with_custom_date_range(self, ga_config, mock_service_account_key):
        """Test extraction with custom date range parameters."""
        with patch('src.connectors.google_analytics_connector.Credentials.from_service_account_info') as mock_creds, \
             patch('src.connectors.google_analytics_connector.requests.post') as mock_post:
            
            mock_creds.return_value = MagicMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"rows": []}
            mock_post.return_value = mock_response
            
            connector = GoogleAnalyticsConnector(ga_config, mock_service_account_key)
            connector._is_connected = True
            connector.access_token = "test_token"
            
            start = datetime(2024, 1, 1).date()
            end = datetime(2024, 1, 7).date()
            params = {"start_date": start, "end_date": end}
            
            connector.extract(params=params)
            
            # Verify the API was called with correct date range
            call_args = mock_post.call_args
            payload = call_args.kwargs["json"]
            assert payload["date_ranges"][0]["start_date"] == "2024-01-01"
            assert payload["date_ranges"][0]["end_date"] == "2024-01-07"

    def test_refresh_access_token(self, ga_config, mock_service_account_key):
        """Test OAuth2 token refresh."""
        with patch('src.connectors.google_analytics_connector.Credentials.from_service_account_info') as mock_creds, \
             patch('src.connectors.google_analytics_connector.Request') as mock_request_class:
            
            mock_creds_instance = MagicMock()
            mock_creds.return_value = mock_creds_instance
            
            connector = GoogleAnalyticsConnector(ga_config, mock_service_account_key)
            connector._refresh_access_token()
            
            mock_creds_instance.refresh.assert_called_once()
