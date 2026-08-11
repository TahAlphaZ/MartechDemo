# Google Analytics 4 Connector - Quick Start

## What Was Generated

The Google Analytics 4 connector adds full GA4 data ingestion to your MarTech Fabric pipeline with:

1. **Production-Ready Connector** (`src/connectors/google_analytics_connector.py`)
   - OAuth2 service account authentication
   - Automatic pagination for large datasets
   - Rate limiting and error handling
   - Extensible base class compatible with the pipeline framework

2. **Comprehensive Tests** (`tests/unit/connectors/test_google_analytics_connector.py`)
   - Connection validation tests
   - Data extraction (single & multi-page)
   - Custom date range handling
   - Error scenarios

3. **Configuration** (`config/connectors/connector_registry.yaml`)
   - Pre-configured GA4 settings
   - 11 essential dimensions pre-selected
   - 4 core metrics included
   - Customizable rate limits and date ranges

4. **Documentation**
   - Full setup guide (`docs/GA4_CONNECTOR_SETUP.md`)
   - Working examples (`examples/ga4_connector_examples.py`)
   - Troubleshooting and performance tips

## 5-Minute Setup

### 1. Verify Dependencies

The connector uses `google-auth` (OAuth2). Add to pyproject.toml if needed:

```toml
[project.optional-dependencies]
analytics = [
    "google-auth>=2.28",
    "google-auth-oauthlib>=1.2",
]
```

### 2. Create Google Cloud Service Account

```bash
# Create account
gcloud iam service-accounts create ga4-connector

# Grant Analytics read permission to your GA4 property
# (via Google Analytics Admin UI or gcloud)
```

### 3. Store Key in Azure Key Vault

```bash
# Download service account key (ga4-key.json)
az keyvault secret set \
  --vault-name "your-keyvault" \
  --name "ga4-service-account-json" \
  --file ga4-key.json
```

### 4. Update Config

Edit `config/connectors/connector_registry.yaml`:

```yaml
analytics:
  active: ["google_analytics"]  # Enable connector
  connectors:
    google_analytics:
      property_id: "1234567890"  # Your GA4 property ID
```

### 5. Test Connection

```bash
python3 << 'EOF'
from src.connectors.connector_factory import load_registry, get_active_connectors, create_connector
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

# Load config
registry = load_registry()
active = get_active_connectors(registry)
ga_config = active['analytics_google_analytics']

# Get credentials from Key Vault
vault_url = "https://your-keyvault.vault.azure.net/"
client = SecretClient(vault_url, DefaultAzureCredential())
secret = client.get_secret("ga4-service-account-json")

# Create and test
connector = create_connector(ga_config, secret.value)
if connector.validate_connection():
    print("✓ GA4 connector ready!")
    data = connector.extract()
    print(f"✓ Extracted {len(data)} rows")
EOF
```

## What's Included

### Core Files

| File | Purpose |
|------|---------|
| `src/connectors/google_analytics_connector.py` | Main GA4 connector class |
| `tests/unit/connectors/test_google_analytics_connector.py` | 8 unit tests covering all features |
| `src/connectors/connector_factory.py` | Updated factory to register GA4 |
| `config/connectors/connector_registry.yaml` | GA4 configuration & dimensions/metrics |

### Documentation

| File | Purpose |
|------|---------|
| `docs/GA4_CONNECTOR_SETUP.md` | Complete setup guide (10+ scenarios) |
| `examples/ga4_connector_examples.py` | 6 working code examples |
| `README_GA4.md` | This file |

## Common Tasks

### Extract Last 7 Days of Data

```python
from datetime import datetime, timedelta

params = {
    "date_range_days": 7,
    "start_date": datetime.now().date() - timedelta(days=7),
    "end_date": datetime.now().date()
}

data = connector.extract(params=params)
```

### Add Custom Dimensions/Metrics

Update `config/connectors/connector_registry.yaml`:

```yaml
google_analytics:
  dimensions:
    - "date"
    - "eventName"          # New: track specific events
    - "userGeoCountry"     # New: geo-location
  metrics:
    - "sessions"
    - "eventCount"         # New: event frequency
```

### Run Tests

```bash
# All GA4 tests
pytest tests/unit/connectors/test_google_analytics_connector.py -v

# Specific test
pytest tests/unit/connectors/test_google_analytics_connector.py::TestGoogleAnalyticsConnector::test_extract_single_page -v

# With coverage
pytest tests/unit/connectors/test_google_analytics_connector.py --cov
```

## Integration with Pipeline

The connector is auto-discovered when enabled in config. The pipeline handles:

1. ✓ Loading GA4 config from registry
2. ✓ Retrieving service account JSON from Key Vault
3. ✓ Creating connector instance
4. ✓ Validating connection
5. ✓ Extracting data on schedule
6. ✓ Landing in `raw/analytics/google_analytics/`

Data includes automatic metadata:
- `_source_system`: "google_analytics"
- `_ga_property_id`: Your property ID
- `_ga_dimensions`: List of extracted dimensions
- `_ga_metrics`: List of extracted metrics

## Architecture

```
Pipeline
  ├── Connector Factory
  │     ├── Loads connector_registry.yaml
  │     ├── Resolves service account from Key Vault
  │     └── Creates GoogleAnalyticsConnector instance
  │
  ├── GoogleAnalyticsConnector
  │     ├── Authenticates with Google Analytics API
  │     ├── Validates connection
  │     ├── Extracts data with pagination
  │     └── Returns structured rows + metadata
  │
  └── Medallion Zone Processor
        └── Lands data in raw/analytics/google_analytics/
```

## Customization Guide

### Add New Dimension

1. Find dimension name in [GA4 Schema](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema)
2. Update `config/connectors/connector_registry.yaml`:
   ```yaml
   dimensions:
     - "sessionSource"
     - "newDimension"  # Add here
   ```
3. Dimension automatically included in all future extractions

### Add New Metric

Same process as dimensions. Example:

```yaml
metrics:
  - "sessions"
  - "totalUsers"
  - "customEvent1Count"  # Custom event metric
```

### Modify Rate Limiting

```yaml
google_analytics:
  rate_limit: 5  # Reduce from 10 to 5 requests/sec
```

### Change Default Date Range

```yaml
google_analytics:
  date_range_days: 30  # Extract last 30 days by default
```

## Troubleshooting

### "Connection failed"
- Verify service account has GA4 read permission
- Check Key Vault secret name matches config
- Ensure Google Analytics API is enabled in GCP project

### "No rows extracted"
- Verify GA4 property ID is correct
- Check that events exist for the date range in GA4
- Try broader date range

### "API rate limit exceeded"
- Reduce `rate_limit` in config
- Implement exponential backoff in extraction loop

## Next Steps

1. ✓ Follow "5-Minute Setup" section above
2. ✓ Run unit tests to verify installation
3. ✓ Review `docs/GA4_CONNECTOR_SETUP.md` for advanced scenarios
4. ✓ Check `examples/ga4_connector_examples.py` for integration patterns
5. ✓ Enable in pipeline schedule for automated daily ingestion

## Support

- **Setup Issues**: See `docs/GA4_CONNECTOR_SETUP.md` troubleshooting section
- **API Questions**: Consult [Google Analytics API Docs](https://developers.google.com/analytics)
- **Code Issues**: Review unit tests for expected behavior
- **Performance**: See performance section in `docs/GA4_CONNECTOR_SETUP.md`
