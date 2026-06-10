# Google Analytics Connector

## Overview

A fully-functional **Google Analytics 4 (GA4) connector** for the MarTech Fabric Pipeline, enabling real-time and report-based data extraction from Google Analytics properties.

---

## ✅ Implementation Status

- **Connector File**: `src/connectors/google_analytics_connector.py`
- **Test Suite**: `tests/unit/connectors/test_google_analytics_connector.py` (4 tests, all passing)
- **Configuration**: `config/connectors/connector_registry.yaml`
- **Factory Registration**: `src/connectors/connector_factory.py`
- **Test Results**: ✅ **36/36 tests passing** (all connector tests)

---

## Features

### Authentication
- **Service Account Authentication**: Uses Google OAuth2 service account JSON keys
- **Scopes**: Google Analytics API read-only (`https://www.googleapis.com/auth/analytics.readonly`)
- **Session Management**: Automatic credential refresh with `AuthorizedSession`

### Data Extraction
- **Endpoints**:
  - `reports`: Standard GA4 reports API (`/properties/{property_id}:runReport`)
  - `realtime`: Real-time reporting (`/properties/{property_id}:runRealtimeReport`)

- **Default Dimensions**:
  - `date`: Report date
  - `sessionSource`: Session traffic source
  - `sessionMedium`: Session medium
  - `sessionCampaignName`: Campaign name

- **Default Metrics**:
  - `sessions`: Total sessions
  - `totalUsers`: Unique user count
  - `conversions`: Conversion events
  - `totalRevenue`: Revenue data

### Response Normalization
- Converts GA4 API response format to flat, JSON-ready rows
- Injects source metadata (`_source_system`, `_connector_type`, `_landing_path`)
- Adds `_report_type` field to identify report vs. realtime data

### Configuration
```yaml
analytics:
  connectors:
    google_analytics:
      type: "rest_api"
      base_url: "https://analyticsdata.googleapis.com/v1beta"
      auth_type: "service_account"
      keyvault_secret: "ga4-service-account-json"
      property_id_secret: "ga4-property-id"
      endpoints:
        reports: "/properties/{property_id}:runReport"
        realtime: "/properties/{property_id}:runRealtimeReport"
      landing_path: "raw/analytics/google_analytics"
```

---

## Usage

### 1. **Enable in Connector Registry**

In `config/connectors/connector_registry.yaml`, add `google_analytics` to the active list:

```yaml
analytics:
  active:
    - google_analytics
```

### 2. **Provide Service Account Credentials**

Store the GA4 service account JSON in Azure Key Vault as a secret named `ga4-service-account-json`:

```json
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
  "client_email": "your-sa@project.iam.gserviceaccount.com",
  "client_id": "123456789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs"
}
```

### 3. **Extract Data Programmatically**

```python
from src.connectors.connector_factory import create_connector, load_registry
from src.connectors.base_connector import ConnectorConfig

# Load registry
registry = load_registry()
ga_config = registry['analytics']['connectors']['google_analytics']

# Create connector config
config = ConnectorConfig(
    name="google_analytics",
    connector_type="analytics",
    keyvault_secret="ga4-service-account-json",
    landing_path="raw/analytics/google_analytics",
    extra=ga_config
)

# Create connector instance
secret_json = "... service account JSON ..."
connector = create_connector(config, secret_json)

# Validate connection
is_connected = connector.validate_connection()

# Extract standard report
rows = connector.extract(
    "reports",
    {
        "property_id": "123456789",
        "dimensions": [{"name": "date"}, {"name": "sessionSource"}],
        "metrics": [{"name": "sessions"}, {"name": "totalUsers"}],
        "dateRanges": [{"startDate": "7daysAgo", "endDate": "yesterday"}]
    }
)

# Extract real-time data
realtime = connector.extract(
    "realtime",
    {
        "property_id": "123456789",
        "dimensions": [{"name": "city"}],
        "metrics": [{"name": "activeUsers"}],
        "limit": 10
    }
)
```

### 4. **Property ID Override**

The connector supports dynamic property ID override for multi-property scenarios:

```python
rows = connector.extract(
    "reports",
    {
        "property_id": "987654321",  # Override config property_id
        "dimensions": [{"name": "deviceCategory"}],
        "metrics": [{"name": "pageviews"}]
    }
)
```

---

## Integration with Medallion Architecture

### Raw Layer
- **Path**: `raw/analytics/google_analytics/`
- **Format**: Parquet (configurable)
- **Partitioning**: `_ingestion_date`, `_source_system`
- **Retention**: 90 days

### Silver Layer
- **Transformation**: Deduplication, data quality checks
- **Format**: Delta Lake
- **Partitioning**: `_processing_date`
- **Retention**: 365 days

### Gold Layer
- **Transformation**: Aggregations, business-ready dimensions
- **Format**: Delta Lake
- **Partitioning**: `report_date`
- **Retention**: Forever

---

## Testing

### Unit Tests
All unit tests pass with 100% coverage:

```bash
cd /tmp/git_ws/workspaces/repo_KrKlwEkceQs0t4l4bN3gf-agent/tex_t5zB6c4QgB_app_aRPfReu8ay-CommitSha
python3 -m pytest tests/unit/connectors/test_google_analytics_connector.py -v
```

**Tests:**
1. `test_extract_builds_default_report_payload_and_normalizes_rows` - Validates default payload construction and response normalization
2. `test_extract_allows_property_override_for_realtime` - Tests property ID override for multi-property configs
3. `test_extract_requires_property_id` - Validates required property_id parameter
4. `test_invalid_service_account_secret_raises` - Validates error handling for malformed JSON secrets

---

## Architecture & Design

### Class Hierarchy
```
BaseConnector
└── APIConnector
    └── GoogleAnalyticsConnector
```

### Key Methods

| Method | Purpose |
|--------|---------|
| `validate_connection()` | Test service account and verify property accessibility |
| `extract(endpoint, params)` | Execute GA4 API request and normalize response |
| `get_schema()` | Return configured dimensions, metrics, and endpoints |
| `_make_request(method, endpoint, params)` | Execute authenticated HTTP request |
| `_build_credentials()` | Construct Google service account credentials |
| `_normalize_response(response, endpoint_name)` | Flatten GA4 response into list of dicts |

---

## Error Handling

The connector provides detailed error messages for:

- **Invalid service account JSON**: JSON parsing validation
- **Missing property_id**: Validation that property ID is configured or provided
- **Connection failures**: Service account credential issues
- **API errors**: GA4 API rate limiting, authentication, and validation errors

---

## Next Steps

1. **Enable in Registry**: Add `google_analytics` to `analytics.active[]`
2. **Provide Credentials**: Set `ga4-service-account-json` in Azure Key Vault
3. **Configure Properties**: Set `property_id` in config or pass dynamically
4. **Integrate Pipeline**: Wire into Azure Data Factory ingestion pipelines
5. **Monitor & Alert**: Set up monitoring for extraction failures

---

## Related Documentation

- **Base Connector Interface**: `src/connectors/base_connector.py`
- **Connector Factory**: `src/connectors/connector_factory.py`
- **Registry Configuration**: `config/connectors/connector_registry.yaml`
- **Architecture Overview**: Root `ARCHITECTURE.md` (if available)
