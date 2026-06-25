"""
Unit tests for HubSpot connector.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.connectors.base_connector import ConnectorConfig
from src.connectors.hubspot_connector import HubSpotConnector


@pytest.fixture
def hubspot_config():
    """Create a test HubSpot config."""
    return ConnectorConfig(
        name="hubspot",
        connector_type="api",
        keyvault_secret="hubspot-api-key",
        landing_path="raw/crm/hubspot",
        extra={"base_url": "https://api.hubapi.com", "rate_limit": 10},
    )


@pytest.fixture
def hubspot_connector(hubspot_config):
    """Create a test HubSpot connector."""
    return HubSpotConnector(hubspot_config, api_key="test-api-key")


class TestHubSpotConnector:
    """Test suite for HubSpot connector."""

    def test_init(self, hubspot_connector):
        """Test connector initialization."""
        assert hubspot_connector.api_key == "test-api-key"
        assert hubspot_connector.base_url == "https://api.hubapi.com"
        assert hubspot_connector.rate_limit == 10

    def test_validate_connection_success(self, hubspot_connector):
        """Test successful connection validation."""
        mock_response = {"results": [], "paging": {}}

        with patch.object(hubspot_connector, "_make_request", return_value=mock_response):
            result = hubspot_connector.validate_connection()
            assert result is True
            assert hubspot_connector._is_connected is True

    def test_validate_connection_failure(self, hubspot_connector):
        """Test failed connection validation."""
        with patch.object(hubspot_connector, "_make_request", side_effect=Exception("API Error")):
            result = hubspot_connector.validate_connection()
            assert result is False
            assert hubspot_connector._is_connected is False

    def test_extract_single_page(self, hubspot_connector):
        """Test extracting data from single page."""
        mock_response = {
            "results": [
                {"id": "1", "properties": {"firstname": "John", "lastname": "Doe"}},
                {"id": "2", "properties": {"firstname": "Jane", "lastname": "Smith"}},
            ],
            "paging": {},
        }

        with patch.object(hubspot_connector, "_make_request", return_value=mock_response):
            result = hubspot_connector.extract("/crm/v3/objects/contacts")
            assert len(result) == 2
            assert result[0]["id"] == "1"
            assert result[1]["id"] == "2"

    def test_extract_with_pagination(self, hubspot_connector):
        """Test extracting data with pagination."""
        page1 = {
            "results": [
                {"id": "1", "properties": {"firstname": "John"}},
                {"id": "2", "properties": {"firstname": "Jane"}},
            ],
            "paging": {"next": {"after": "cursor123"}},
        }

        page2 = {
            "results": [
                {"id": "3", "properties": {"firstname": "Bob"}},
            ],
            "paging": {},
        }

        with patch.object(
            hubspot_connector,
            "_make_request",
            side_effect=[page1, page2],
        ):
            result = hubspot_connector.extract("/crm/v3/objects/contacts")
            assert len(result) == 3
            assert result[0]["id"] == "1"
            assert result[2]["id"] == "3"

    def test_extract_with_params(self, hubspot_connector):
        """Test extracting with query parameters."""
        mock_response = {"results": [], "paging": {}}

        with patch.object(hubspot_connector, "_make_request", return_value=mock_response) as mock_request:
            params = {"limit": 50, "properties": ["firstname", "email"]}
            hubspot_connector.extract("/crm/v3/objects/contacts", params=params)

            mock_request.assert_called()
            call_args = mock_request.call_args
            assert call_args[1]["params"]["limit"] == 50

    def test_get_schema(self, hubspot_connector):
        """Test schema retrieval."""
        schema = hubspot_connector.get_schema()
        assert "objects" in schema
        assert len(schema["objects"]) > 0

        # Check that default objects are present
        object_names = [obj["name"] for obj in schema["objects"]]
        assert "contacts" in object_names
        assert "companies" in object_names
        assert "deals" in object_names

    def test_get_schema_from_config(self, hubspot_config):
        """Test schema from config extra."""
        custom_schema = {"objects": [{"name": "custom"}]}
        hubspot_config.extra["schema"] = custom_schema

        connector = HubSpotConnector(hubspot_config, api_key="test-key")
        schema = connector.get_schema()
        assert schema == custom_schema

    def test_get_metadata(self, hubspot_connector):
        """Test metadata retrieval."""
        metadata = hubspot_connector.get_metadata()
        assert metadata["_source_system"] == "hubspot"
        assert metadata["_connector_type"] == "api"
        assert metadata["_landing_path"] == "raw/crm/hubspot"

    @patch("src.connectors.hubspot_connector.HubSpotConnector._load_requests")
    def test_make_request_success(self, mock_load_requests, hubspot_connector):
        """Test successful HTTP request."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": []}
        mock_requests = MagicMock()
        mock_requests.get.return_value = mock_response
        mock_load_requests.return_value = mock_requests

        result = hubspot_connector._make_request("GET", "/test")
        assert result == {"results": []}
        mock_requests.get.assert_called_once()

    @patch("src.connectors.hubspot_connector.HubSpotConnector._load_requests")
    def test_make_request_http_error(self, mock_load_requests, hubspot_connector):
        """Test HTTP error handling."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"

        mock_requests = MagicMock()
        mock_error = Exception("HTTP Error")
        mock_error.response = mock_response
        mock_requests.get.side_effect = __import__("requests").exceptions.HTTPError("Unauthorized")
        mock_load_requests.return_value = mock_requests

        # Create a more realistic error
        import requests

        mock_requests.get.side_effect = requests.exceptions.HTTPError("Unauthorized")

        # This should not raise, but return None
        with patch("src.connectors.hubspot_connector.logger"):
            result = hubspot_connector._make_request("GET", "/test")
            assert result is None

    @patch("src.connectors.hubspot_connector.HubSpotConnector._load_requests")
    def test_make_request_timeout(self, mock_load_requests, hubspot_connector):
        """Test request timeout handling."""
        mock_requests = MagicMock()
        import requests

        mock_requests.get.side_effect = requests.exceptions.Timeout("Request timeout")
        mock_load_requests.return_value = mock_requests

        with patch("src.connectors.hubspot_connector.logger"):
            result = hubspot_connector._make_request("GET", "/test")
            assert result is None

    def test_load_requests_success(self):
        """Test successful requests module loading."""
        requests = HubSpotConnector._load_requests()
        assert requests is not None

    def test_load_requests_failure(self):
        """Test requests module not available."""
        with patch("importlib.import_module", side_effect=ModuleNotFoundError("requests not found")):
            with pytest.raises(ModuleNotFoundError, match="requests is required"):
                HubSpotConnector._load_requests()
