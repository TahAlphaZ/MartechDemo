# Google Analytics 4 Connector

## Overview

The Google Analytics 4 (GA4) connector integrates Google Analytics data into the MarTech Fabric data pipeline. It leverages the Google Analytics Reporting API v4 to extract custom dimensions, metrics, segments, and date ranges in a scalable, production-ready manner.

## Features

- **OAuth2 Service Account Authentication**: Secure, application-level authentication without user credentials
- **Flexible Dimensions & Metrics**: Configure any GA4 dimension and metric via YAML config
- **Automatic Pagination**: Handles large datasets exceeding single-page limits
- **Rate Limiting**: Built-in respect for Google Analytics API quotas
- **Custom Date Ranges**: Extract historical or custom-dated data with per-request override
- **Metadata Tagging**: All extracted data includes source system, property ID, and schema info
- **Connection Validation**: Health checks before each extraction to detect authentication or API issues

## Prerequisites

1. **Google Cloud Project** with Analytics API enabled
2. **Service Account** with GA4 read permissions
3. **Service Account Key** stored in Azure Key Vault (JSON format)
4. **GA4 Property ID** (numeric, e.g., 1234567890)

## Setup Instructions

### Step 1: Create Service Account in Google Cloud

```bash
# Set your Google Cloud project ID
PROJECT_ID="your-project-id"
gcloud config set project $PROJECT_ID

# Create service account
gcloud iam service-accounts create ga4-connector \
  --display-name="GA4 Connector Service Account"

# Get service account email
SERVICE_ACCOUNT_EMAIL=$(gcloud iam service-accounts list \
  --filter="displayName:GA4\ Connector" --format='value(email)')

echo $SERVICE_ACCOUNT_EMAIL
```

### Step 2: Grant Permissions

```bash
PROJECT_ID="your-project-id"
SERVICE_ACCOUNT_EMAIL="ga4-connector@${PROJECT_ID}.iam.gserviceaccount.com"

# Grant Viewer role (read-only)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --role="roles/viewer"

# Grant Analytics Read permission
gcloud analytics-admin properties get-iam-policy PROPERTY_ID \
  --grant-role=roles/iam.securityReviewer
```

### Step 3: Create and Export Key

```bash
SERVICE_ACCOUNT_EMAIL="ga4-connector@${PROJECT_ID}.iam.gserviceaccount.com"

# Create JSON key
gcloud iam service-accounts keys create ga4-key.json \
  --iam-account=$SERVICE_ACCOUNT_EMAIL

# View the key
cat ga4-key.json
```

### Step 4: Store Key in Azure Key Vault

```bash
# Upload to Key Vault (as single-line JSON)
az keyvault secret set \
  --vault-name "your-keyvault-name" \
  --name "ga4-service-account-json" \
  --file ga4-key.json

# Cleanup local copy
rm -f ga4-key.json
```

### Step 5: Configure Connector

Edit `config/connectors/connector_registry.yaml`:

```yaml
analytics:
  active: ["google_analytics"]
  connectors:
    google_analytics:
      type: "rest_api"
      base_url: "https://analyticsdata.googleapis.com/v1beta"
      auth_type: "service_account"
      keyvault_secret: "ga4-service-account-json"
      property_id: "1234567890"  # Replace with your GA4 property ID
      endpoints:
        reports: "/properties/{property_id}:runReport"
      landing_path: "raw/analytics/google_analytics"
      dimensions:
        - "date"
        - "sessionSource"
        - "sessionMedium"
        - "sessionCampaignName"
        - "eventName"
      metrics:
        - "sessions"
        - "totalUsers"
        - "conversions"
        - "totalRevenue"
      rate_limit: 10  # Requests per second
      date_range_days: 1  # Default: last 1 day
```

## Usage

### Basic Extraction

```python
from src.connectors.connector_factory import load_registry, get_active_connectors, create_connector
from src.connectors.google_analytics_connector import GoogleAnalyticsConnector

# Load config
registry = load_registry()
active_connectors = get_active_connectors(registry)

# Get GA4 connector config
ga_config = active_connectors['analytics_google_analytics']

# In production, retrieve secret from Key Vault
# This is pseudo-code; actual Key Vault retrieval is handled by the pipeline
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

credential = DefaultAzureCredential()
vault_url = "https://your-keyvault.vault.azure.net/"
client = SecretClient(vault_url=vault_url, credential=credential)
service_account_json = client.get_secret(ga_config.keyvault_secret).value

# Create connector
connector = create_connector(ga_config, service_account_json)

# Validate connection
if connector.validate_connection():
    print("✓ Connected to Google Analytics")
else:
    print("✗ Connection failed")

# Extract data
data = connector.extract()
print(f"Extracted {len(data)} rows")
```

### Custom Date Range

```python
from datetime import datetime

# Extract data for a specific date range
start_date = datetime(2024, 1, 1).date()
end_date = datetime(2024, 1, 31).date()

params = {
    "start_date": start_date,
    "end_date": end_date,
    "date_range_days": 30  # Override default
}

data = connector.extract(params=params)
```

### Schema Inspection

```python
# Get expected schema/structure
schema = connector.get_schema()

print("Dimensions:", schema["dimensions"])
print("Metrics:", schema["metrics"])
print("Sample Row:", schema["sample_row"])
```

## Configuration Reference

| Key | Type | Description |
|-----|------|-------------|
| `property_id` | string | GA4 property ID (numeric) |
| `dimensions` | list | GA4 dimensions to extract (e.g., "date", "sessionSource") |
| `metrics` | list | GA4 metrics to extract (e.g., "sessions", "users") |
| `date_range_days` | int | Default lookback period in days (default: 1) |
| `rate_limit` | int | Max requests per second (default: 10) |
| `keyvault_secret` | string | Key Vault secret containing service account JSON |
| `landing_path` | string | Destination path in raw data zone |

## Supported Dimensions & Metrics

### Common Dimensions
- `date` - Date in YYYYMMDD format
- `sessionSource` - Traffic source
- `sessionMedium` - Traffic medium (organic, cpc, referral, etc.)
- `sessionCampaignName` - Campaign name
- `eventName` - Event type
- `deviceCategory` - Device type (desktop, mobile, tablet)
- `country` - User country

### Common Metrics
- `sessions` - Number of sessions
- `totalUsers` - Number of unique users
- `pageviews` - Number of page views
- `conversions` - Number of conversions
- `totalRevenue` - Total revenue
- `averageSessionDuration` - Avg session length in seconds

See [Google Analytics Dimension & Metric Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema) for complete list.

## Testing

Run the unit tests:

```bash
# All GA4 tests
pytest tests/unit/connectors/test_google_analytics_connector.py -v

# Specific test
pytest tests/unit/connectors/test_google_analytics_connector.py::TestGoogleAnalyticsConnector::test_extract_single_page -v

# With coverage
pytest tests/unit/connectors/test_google_analytics_connector.py --cov=src.connectors.google_analytics_connector
```

## Error Handling

### Authentication Errors
```
Failed to initialize GA credentials: Invalid service account JSON
```
**Solution**: Verify service account JSON is valid and contains all required fields.

### API Rate Limiting
```
GA4 API error 429: Too many requests
```
**Solution**: Reduce `rate_limit` in config or implement exponential backoff.

### Invalid Property ID
```
GA4 API error 400: Invalid property ID
```
**Solution**: Verify property_id matches your GA4 property and service account has access.

### No Data for Date Range
```
Extracted 0 rows from Google Analytics
```
**Solution**: Verify the date range has events tracked in GA4.

## Performance Considerations

- **Pagination**: Automatically batches large result sets (100K rows per page)
- **Rate Limiting**: Default 10 req/sec; adjust based on quota
- **Caching**: Consider caching connector instance to avoid repeated credential initialization
- **Date Range**: Limit lookback period to reduce API calls and data volume

## Troubleshooting

### Enable Debug Logging
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Test API Directly
```bash
# Requires gcloud CLI auth setup
curl -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  "https://analyticsdata.googleapis.com/v1beta/properties/1234567890:runReport" \
  -X POST \
  -d '{
    "date_ranges": [{"start_date": "2024-01-01", "end_date": "2024-01-31"}],
    "dimensions": [{"name": "date"}],
    "metrics": [{"name": "sessions"}]
  }'
```

## Integration with Pipeline

The GA4 connector is automatically discovered by the pipeline when:
1. Enabled in `config/connectors/connector_registry.yaml` (`analytics.active` includes `google_analytics`)
2. Service account credentials stored in Key Vault
3. Pipeline service identity has Key Vault read access

The pipeline will:
1. Load connector config from registry
2. Retrieve service account JSON from Key Vault
3. Instantiate connector and validate connection
4. Extract data on configured schedule
5. Land data in `raw/analytics/google_analytics/` with metadata

## Support

For issues or questions:
- Check Google Analytics [API documentation](https://developers.google.com/analytics/devguides/reporting/data/v1)
- Review Azure Key Vault [authentication guide](https://docs.microsoft.com/en-us/azure/key-vault/general/authentication)
- Inspect logs for detailed error messages
