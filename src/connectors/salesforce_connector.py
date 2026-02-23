"""
SalesForce Connector

Implements OAuth2 token acquisition, SOQL generation with SystemModstamp incremental filter,
pagination-aware extraction and mapping to landing table names.
"""
from typing import Any, Dict, Optional
import json
import time
import logging
from datetime import datetime
from urllib.parse import urlencode

import requests

from src.connectors.base_connector import APIConnector, ConnectorConfig

logger = logging.getLogger(__name__)


class SalesForceConnector(APIConnector):
    """Salesforce REST API connector supporting OAuth2 and incremental SystemModstamp sync.

    Notes:
    - Expects api_key to be a dict or JSON string containing client_id, client_secret, token_url,
      optionally instance_url and api_version.
    - Provides build_soql() to generate queries and extract() which handles pagination.
    """

    DEFAULT_API_VERSION = "v59.0"

    def __init__(self, config: ConnectorConfig, api_key: Any):
        super().__init__(config, api_key)

        # Parse api_key if JSON string
        creds: Dict[str, Any]
        if isinstance(api_key, str):
            try:
                creds = json.loads(api_key)
            except Exception:
                creds = {"client_id": None, "client_secret": None}
        elif isinstance(api_key, dict):
            creds = api_key
        else:
            creds = {"client_id": None, "client_secret": None}

        # Credentials and endpoints
        self._client_id = creds.get("client_id")
        self._client_secret = creds.get("client_secret")
        self._token_url = creds.get("token_url") or self.config.extra.get("token_url")
        self._refresh_token = creds.get("refresh_token")
        self.api_version = creds.get("api_version") or self.config.extra.get("api_version") or self.DEFAULT_API_VERSION

        # Token state
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[float] = None

        # Instance URL (may come from creds or token response)
        self.instance_url: Optional[str] = creds.get("instance_url")

        # Supported sobjects and landing mapping
        self.endpoints = {"Contact": "Contact", "Lead": "Lead", "Opportunity": "Opportunity", "Account": "Account"}
        self.entity_landing_name = {
            "Contact": "crm_contacts",
            "Lead": "crm_leads",
            "Opportunity": "crm_deals",
            "Account": "crm_companies",
        }

    # -------------------------- Authentication --------------------------
    def _obtain_access_token(self) -> None:
        """Obtain OAuth2 token from Salesforce token endpoint.

        Uses client_credentials (if supported) or refresh_token if provided in credentials.
        Sets self.access_token, self.token_expires_at and self.instance_url when available.
        """
        if not self._token_url:
            raise ValueError("No token_url configured for Salesforce connector")

        payload = {
            # Use client credentials flow by default; Salesforce commonly supports password/grant_type flows,
            # but for testability we implement a generic client_credentials / refresh_token switching.
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        if self._refresh_token:
            payload["grant_type"] = "refresh_token"
            payload["refresh_token"] = self._refresh_token

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        logger.debug("Requesting Salesforce token from %s", self._token_url)
        resp = requests.post(self._token_url, data=payload, headers=headers)

        if resp.status_code != 200:
            raise Exception(f"Failed to obtain Salesforce token: {resp.status_code} {resp.text}")

        data = resp.json()
        self.access_token = data.get("access_token")
        expires_in = data.get("expires_in")
        if expires_in:
            try:
                self.token_expires_at = time.time() + int(expires_in)
            except Exception:
                self.token_expires_at = None
        else:
            self.token_expires_at = None

        # Salesforce returns instance_url for building API calls
        if data.get("instance_url"):
            self.instance_url = data.get("instance_url")

        logger.info("Obtained Salesforce access token, instance_url=%s", self.instance_url)

    def _ensure_token(self) -> None:
        """Ensure access token exists and is not expired. Refresh shortly before expiry."""
        if not self.access_token:
            self._obtain_access_token()
            return

        if self.token_expires_at and time.time() > (self.token_expires_at - 30):
            logger.debug("Salesforce token near expiry, refreshing")
            self._obtain_access_token()

    # -------------------------- Request handling --------------------------
    def _make_request(self, method: str, endpoint: Any, params: Optional[Dict] = None) -> Dict:
        """Perform HTTP request to Salesforce.

        endpoint may be:
        - dict with key 'soql': perform query at /query endpoint with q parameter
        - string name of sobject: will build SOQL and execute
        - full URL (starting with http) or path starting with /services/data: used directly
        """
        # Ensure auth
        self._ensure_token()

        headers = {"Authorization": f"Bearer {self.access_token}"}

        # Build URL and params depending on endpoint
        url = None
        request_params = params or {}

        if isinstance(endpoint, dict) and "soql" in endpoint:
            soql = endpoint["soql"]
            if not self.instance_url:
                raise ValueError("instance_url is not set for Salesforce API calls")
            url = f"{self.instance_url}/services/data/{self.api_version}/query"
            request_params = {"q": soql}
        elif isinstance(endpoint, str):
            # full URL
            if endpoint.startswith("http") or endpoint.startswith("/services/data"):
                url = endpoint if endpoint.startswith("http") else f"{self.instance_url}{endpoint}"
            else:
                # treat endpoint as sobject name -> build SOQL
                soql = self.build_soql(endpoint, last_sync=params.get("last_sync") if params else None)
                if not self.instance_url:
                    raise ValueError("instance_url is not set for Salesforce API calls")
                url = f"{self.instance_url}/services/data/{self.api_version}/query"
                request_params = {"q": soql}

        # Default to GET for queries
        if method.upper() == "GET":
            resp = requests.get(url, headers=headers, params=request_params)
        else:
            resp = requests.post(url, headers=headers, data=request_params)

        if resp.status_code >= 400:
            raise Exception(f"Salesforce API request failed: {resp.status_code} {resp.text}")

        return resp.json()

    # -------------------------- SOQL builder --------------------------
    def build_soql(self, sobject: str, last_sync: Optional[Any] = None) -> str:
        """Build a SOQL query for the given sobject. If last_sync provided, filter by SystemModstamp > last_sync.

        last_sync may be an ISO string or datetime. Returns SOQL string.
        """
        sobject = self.normalize_sobject_name(sobject)

        fields_map = {
            "Contact": ["Id", "FirstName", "LastName", "Email", "SystemModstamp"],
            "Lead": ["Id", "FirstName", "LastName", "Email", "SystemModstamp"],
            "Opportunity": ["Id", "Name", "StageName", "Amount", "CloseDate", "AccountId", "SystemModstamp"],
            "Account": ["Id", "Name", "Industry", "BillingCity", "SystemModstamp"],
        }

        fields = fields_map.get(sobject, ["Id", "SystemModstamp"])  # fallback minimal
        select_clause = ", ".join(fields)
        soql = f"SELECT {select_clause} FROM {sobject}"

        if last_sync:
            if isinstance(last_sync, datetime):
                ts = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                ts = str(last_sync)
            # Use single quotes around timestamp in SOQL
            soql += f" WHERE SystemModstamp > '{ts}'"

        soql += " ORDER BY SystemModstamp ASC"
        return soql

    # -------------------------- Extraction --------------------------
    def extract(self, object_name: str, params: Optional[Dict] = None) -> Any:
        """Extract records for the given sobject. Handles pagination.

        Accepts params, e.g., {'last_sync': '2024-01-01T00:00:00Z'}
        """
        params = params or {}
        sobject = self.normalize_sobject_name(object_name)
        last_sync = params.get("last_sync")

        soql = self.build_soql(sobject, last_sync=last_sync)
        query_endpoint = {"soql": soql}

        all_records = []
        next_url = None

        # First request
        resp = self._make_request("GET", query_endpoint)

        # Salesforce returns 'records' + 'nextRecordsUrl' in standard API; support 'data' + 'next_page_url' too
        def _extract_records(r):
            if isinstance(r, dict):
                return r.get("data") or r.get("records") or []
            return []

        records = _extract_records(resp)
        all_records.extend(records)

        # Determine pagination token/url
        next_url = resp.get("next_page_url") or resp.get("nextRecordsUrl")
        # If nextRecordsUrl is a relative path, prefix instance_url
        while next_url:
            # Normalize next URL
            if not next_url.startswith("http"):
                if not self.instance_url:
                    raise ValueError("instance_url required to follow nextRecordsUrl")
                paged_url = f"{self.instance_url}{next_url}"
            else:
                paged_url = next_url

            resp = self._make_request("GET", paged_url)
            records = _extract_records(resp)
            all_records.extend(records)
            next_url = resp.get("next_page_url") or resp.get("nextRecordsUrl")

        return all_records

    # -------------------------- Utilities --------------------------
    def validate_connection(self) -> bool:
        try:
            self._ensure_token()
            # Lightweight query
            soql = "SELECT Id FROM Contact LIMIT 1"
            res = self._make_request("GET", {"soql": soql})
            self._is_connected = res is not None
            return self._is_connected
        except Exception as e:
            logger.error("Salesforce validate_connection failed: %s", e)
            return False

    def get_schema(self) -> Dict[str, Any]:
        return {
            "Contact": ["Id", "FirstName", "LastName", "Email", "SystemModstamp"],
            "Lead": ["Id", "FirstName", "LastName", "Email", "SystemModstamp"],
            "Opportunity": ["Id", "Name", "StageName", "Amount", "CloseDate", "AccountId", "SystemModstamp"],
            "Account": ["Id", "Name", "Industry", "BillingCity", "SystemModstamp"],
        }

    def normalize_sobject_name(self, name: str) -> str:
        if not name:
            return name
        name = name.strip()
        # Accept lowercase inputs
        return name[0].upper() + name[1:]

    def get_entity_landing_name(self, sobject: str) -> str:
        sobject = self.normalize_sobject_name(sobject)
        return self.entity_landing_name.get(sobject, f"crm_{sobject.lower()}")
