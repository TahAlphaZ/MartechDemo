---
name: add-analytics-connector
description: >
  Add a new analytics platform connector (Google Analytics GA4, Adobe Analytics,
  Mixpanel, Segment, Amplitude, etc.) to the MarTech Fabric Pipeline. Use this
  skill whenever a client needs web analytics, marketing analytics, or behavioral
  analytics data flowing into the medallion lakehouse. Triggers on: "add Google
  Analytics", "connect Adobe Analytics", "integrate Mixpanel", "add analytics
  source", "web analytics connector", "GA4 integration", "add tracking data",
  or any request to bring analytics platform data into the pipeline.
---

# Add Analytics API Connector

You are extending the MarTech Fabric Pipeline to ingest data from a new analytics
platform. The pipeline uses a connector registry pattern — all connectors share
a common interface, making new sources plug-and-play.

## What You're Building

An analytics connector pulls metrics and dimensions from platforms like GA4,
Adobe Analytics, or Mixpanel via their REST APIs, and lands that data in the
medallion lakehouse (Raw → Silver → Gold) for downstream BI dashboards.

## Before You Start

Read these files to understand the existing patterns:

1. `config/connectors/connector_registry.yaml` — see the `analytics:` section
   for existing connector definitions. Your new connector follows this exact shape.
2. `src/connectors/base_connector.py` — the `APIConnector` base class you'll extend.
3. `src/connectors/connector_factory.py` — where you register your class in
   `CONNECTOR_CLASS_MAP`.

## Step-by-Step Process

### 1. Register in connector_registry.yaml

Add your platform under the `analytics.connectors` section:

```yaml
analytics:
  active: ["google_analytics"]   # Add your connector name here too
  connectors:
    your_platform:
      type: "rest_api"
      base_url: "https://api.yourplatform.com/v1"
      auth_type: "oauth2"        # or "api_key", "service_account", "jwt"
      keyvault_secret: "your-platform-api-credentials"
      endpoints:
        reports: "/reports"
        events: "/events"
      rate_limit: 10             # requests per second
      landing_path: "raw/analytics/your_platform"
      default_dimensions:
        - "date"
        - "source"
        - "medium"
      default_metrics:
        - "sessions"
        - "users"
        - "conversions"
```

Key fields to get right:

- **auth_type** — determines how the connector authenticates. Check the platform's
  API docs for which auth flow they use.
- **keyvault_secret** — the Key Vault secret name where credentials are stored.
  Never hardcode credentials anywhere.
- **rate_limit** — respect the platform's API rate limits to avoid throttling.
- **landing_path** — always follows pattern `raw/analytics/{platform_name}`.

### 2. Create the Connector Class

Create `src/connectors/{platform}_connector.py`. The class extends `APIConnector`
and handles platform-specific auth, pagination, and response parsing:

```python
"""
{Platform} Analytics Connector
Pulls analytics data via the {Platform} REST API.
"""
import logging
from typing import Any, Dict, List, Optional
from src.connectors.base_connector import APIConnector, ConnectorConfig

logger = logging.getLogger(__name__)


class {Platform}Connector(APIConnector):

    def __init__(self, config: ConnectorConfig, api_key: str):
        super().__init__(config, api_key)
        self.base_url = config.extra.get("base_url", "")

    def validate_connection(self) -> bool:
        try:
            response = self._make_request("GET", "/me")
            self._is_connected = response is not None
            return self._is_connected
        except Exception as e:
            logger.error(f"{self.config.name} connection failed: {e}")
            return False

    def extract(self, query_or_endpoint: str, params: Optional[Dict] = None) -> Any:
        all_data: List[Dict] = []
        page_token = None

        while True:
            request_params = {**(params or {})}
            if page_token:
                request_params["pageToken"] = page_token

            response = self._make_request("GET", query_or_endpoint, request_params)
            rows = response.get("rows", response.get("data", []))
            all_data.extend(rows)

            page_token = response.get("nextPageToken")
            if not page_token:
                break
        return all_data

    def get_schema(self) -> Dict[str, Any]:
        return {
            "dimensions": self.config.extra.get("default_dimensions", []),
            "metrics": self.config.extra.get("default_metrics", []),
        }

    def _make_request(self, method, endpoint, params=None) -> Dict:
        import requests, time
        url = f"{self.base_url}{endpoint}"
        headers = self._get_auth_headers()
        time.sleep(1.0 / self.rate_limit)
        response = requests.request(method, url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def _get_auth_headers(self) -> Dict[str, str]:
        auth_type = self.config.extra.get("auth_type", "api_key")
        if auth_type in ("api_key", "bearer_token"):
            return {"Authorization": f"Bearer {self.api_key}"}
        elif auth_type == "service_account":
            return {"Authorization": f"Bearer {self._get_oauth_token()}"}
        elif auth_type == "jwt":
            return {"Authorization": f"Bearer {self.api_key}"}
        return {}

    def _get_oauth_token(self) -> str:
        raise NotImplementedError("Implement OAuth token exchange for this platform")
```

### 3. Register in the Factory

In `src/connectors/connector_factory.py`, add your class:

```python
from src.connectors.{platform}_connector import {Platform}Connector
CONNECTOR_CLASS_MAP["your_platform"] = {Platform}Connector
```

### 4. Write Tests FIRST (TDD)

Create `tests/unit/connectors/test_{platform}_connector.py`. The tests should
cover connection validation, pagination handling, schema output, and error cases.
See `references/analytics_platforms.md` for platform-specific test scenarios.

Minimum test coverage:

- `test_connector_initializes` — config fields parsed correctly
- `test_validate_connection_success` — happy path with mocked API
- `test_validate_connection_failure` — auth failure handled gracefully
- `test_extract_handles_pagination` — multi-page responses combined
- `test_get_schema_returns_dimensions_and_metrics`
- `test_rate_limiting_applied` — requests respect configured rate limit

### 5. Add Data Quality Tests

Create `tests/data-quality/raw/test_{platform}_raw.py` with checks for:

- Dimension columns present (date, source, medium)
- Date format valid (YYYY-MM-DD)
- Metric columns are numeric
- No completely null rows

### 6. Update ARM + Key Vault

Add platform to `enableAnalyticsConnectors` in the environment parameters file
and store API credentials in Key Vault under the configured secret name.

## Platform Reference

Read `references/analytics_platforms.md` for specifics on GA4, Adobe Analytics,
Mixpanel, Amplitude, and Segment — including auth flows, pagination patterns,
rate limits, and response schemas.

## Completion Checklist

- [ ] Registry entry in `connector_registry.yaml`
- [ ] Connector class extends `APIConnector`
- [ ] Registered in `CONNECTOR_CLASS_MAP`
- [ ] Unit tests pass (connection, extraction, pagination, schema)
- [ ] Data quality tests for raw zone
- [ ] ARM parameter updated
- [ ] Key Vault secret created
- [ ] End-to-end test in dev environment
