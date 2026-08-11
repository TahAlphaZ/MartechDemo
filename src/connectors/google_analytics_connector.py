"""
Google Analytics 4 (GA4) Data Connector.

Extracts data from Google Analytics using the Reporting API v4.
Supports custom dimensions, metrics, segments, and date ranges.
"""
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
import time

import requests
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials

from src.connectors.base_connector import APIConnector, ConnectorConfig

logger = logging.getLogger(__name__)


class GoogleAnalyticsConnector(APIConnector):
    """
    Google Analytics 4 Data Connector.
    
    Configuration via connector_registry.yaml:
    ```yaml
    analytics:
      active:
        - google_analytics
      connectors:
        google_analytics:
          keyvault_secret: "google-analytics-service-account"
          landing_path: "raw/analytics/google_analytics"
          base_url: "https://analyticsreporting.googleapis.com/v4"
          property_id: "1234567890"
          dimensions: ["date", "country", "deviceCategory"]
          metrics: ["sessions", "users", "pageviews"]
          date_range_days: 1
    ```
    """

    def __init__(self, config: ConnectorConfig, api_key: str):
        """
        Initialize Google Analytics connector with service account credentials.
        
        Args:
            config: ConnectorConfig with GA4 property_id and dimensions/metrics
            api_key: JSON service account key (loaded from Key Vault)
        """
        super().__init__(config, api_key)
        self.property_id = config.extra.get("property_id", "")
        self.dimensions = config.extra.get("dimensions", ["date", "country"])
        self.metrics = config.extra.get("metrics", ["sessions", "users"])
        self.date_range_days = config.extra.get("date_range_days", 1)
        self.rate_limit = config.extra.get("rate_limit", 10)
        
        self.credentials = None
        self.access_token = None
        self._initialize_credentials()

    def _initialize_credentials(self) -> None:
        """
        Parse service account JSON and create credentials.
        api_key is the full JSON service account key from Key Vault.
        """
        try:
            import json
            if isinstance(self.api_key, str):
                key_dict = json.loads(self.api_key)
            else:
                key_dict = self.api_key
            
            self.credentials = Credentials.from_service_account_info(
                key_dict,
                scopes=["https://www.googleapis.com/auth/analytics.readonly"]
            )
            logger.info("Google Analytics credentials initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize GA credentials: {e}")
            raise

    def validate_connection(self) -> bool:
        """
        Test connection by making a simple request to GA4 API.
        """
        try:
            if not self.credentials:
                self._initialize_credentials()
            
            self._refresh_access_token()
            
            # Make a simple metadata request to validate connection
            headers = self._get_headers()
            response = requests.get(
                "https://analyticsadmin.googleapis.com/v1beta/properties",
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                self._is_connected = True
                logger.info("Google Analytics connection validated")
                return True
            else:
                logger.warning(f"GA validation returned status {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Google Analytics connection validation failed: {e}")
            return False

    def extract(self, query_or_endpoint: str = "", params: Optional[Dict] = None) -> List[Dict]:
        """
        Extract data from Google Analytics 4.
        
        Args:
            query_or_endpoint: Optional report name or endpoint (unused, uses config)
            params: Optional dict with date_range or other overrides
        
        Returns:
            List of rows with dimensions and metrics
        """
        if not self._is_connected:
            self.validate_connection()
        
        # Parse date range from params or use default
        params = params or {}
        date_range_days = params.get("date_range_days", self.date_range_days)
        end_date = params.get("end_date", datetime.now().date())
        start_date = params.get("start_date", end_date - timedelta(days=date_range_days))
        
        logger.info(f"Extracting GA4 data from {start_date} to {end_date}")
        
        try:
            all_rows = []
            page_size = 100000
            next_page_token = None
            
            while True:
                response = self._fetch_report(
                    start_date=start_date,
                    end_date=end_date,
                    page_token=next_page_token,
                    page_size=page_size
                )
                
                rows = response.get("rows", [])
                all_rows.extend(rows)
                
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
                
                # Respect rate limiting
                time.sleep(1.0 / self.rate_limit)
            
            logger.info(f"Extracted {len(all_rows)} rows from Google Analytics")
            return all_rows
            
        except Exception as e:
            logger.error(f"Failed to extract from Google Analytics: {e}")
            raise

    def _fetch_report(
        self,
        start_date: Any,
        end_date: Any,
        page_token: Optional[str] = None,
        page_size: int = 100000
    ) -> Dict[str, Any]:
        """
        Make a single API request to Google Analytics Reporting API v4.
        Supports pagination via pageToken.
        """
        if not self.access_token:
            self._refresh_access_token()
        
        url = "https://analyticsreporting.googleapis.com/v4/properties/ga4:runReport"
        
        # Format dates as YYYY-MM-DD
        if hasattr(start_date, "strftime"):
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
        else:
            start_date_str = str(start_date)
            end_date_str = str(end_date)
        
        payload = {
            "property": f"properties/{self.property_id}",
            "date_ranges": [
                {
                    "start_date": start_date_str,
                    "end_date": end_date_str
                }
            ],
            "dimensions": [{"name": dim} for dim in self.dimensions],
            "metrics": [{"name": metric} for metric in self.metrics],
            "limit": page_size
        }
        
        if page_token:
            payload["offset"] = int(page_token)
        
        headers = self._get_headers()
        
        logger.debug(f"GA4 API Request: {payload}")
        
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"GA4 API error {response.status_code}: {response.text}")
            response.raise_for_status()
        
        return response.json()

    def _refresh_access_token(self) -> None:
        """Refresh OAuth2 access token using service account credentials."""
        if not self.credentials:
            self._initialize_credentials()
        
        auth_request = Request()
        self.credentials.refresh(auth_request)
        self.access_token = self.credentials.token
        logger.debug("Access token refreshed")

    def _get_headers(self) -> Dict[str, str]:
        """Return HTTP headers with OAuth2 bearer token."""
        if not self.access_token:
            self._refresh_access_token()
        
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def get_schema(self) -> Dict[str, Any]:
        """
        Return schema describing the structure of extracted GA4 data.
        Dimensions and metrics are configured in connector_registry.yaml.
        """
        return {
            "dimensions": self.dimensions,
            "metrics": self.metrics,
            "sample_row": self._generate_sample_row()
        }

    def _generate_sample_row(self) -> Dict[str, Any]:
        """Generate a sample row showing expected data structure."""
        sample = {}
        
        for dim in self.dimensions:
            sample[dim] = f"<{dim}_value>"
        
        for metric in self.metrics:
            sample[metric] = "<numeric_value>"
        
        return sample

    def get_metadata(self) -> Dict[str, str]:
        """Extend base metadata with GA4-specific information."""
        metadata = super().get_metadata()
        metadata.update({
            "_ga_property_id": self.property_id,
            "_ga_dimensions": ",".join(self.dimensions),
            "_ga_metrics": ",".join(self.metrics),
        })
        return metadata
