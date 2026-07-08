# HubSpot Connector - MarTech Fabric Pipeline

## Overview

The **HubSpot Connector** enables seamless integration of HubSpot CRM data into the MarTech Fabric Pipeline. It provides secure, paginated access to contacts, companies, deals, tickets, and other HubSpot objects via the HubSpot REST API v3.

## Features

- ✅ **HubSpot API v3 Integration**: Native support for all standard HubSpot CRM objects
- ✅ **Automatic Pagination**: Handles cursor-based pagination transparently
- ✅ **Rate Limiting**: Built-in rate limiting awareness (10 requests/second default)
- ✅ **Bearer Token Authentication**: Secure API key management via Azure Key Vault
- ✅ **Schema Introspection**: Exposes available objects and properties
- ✅ **Error Handling**: Graceful handling of auth, rate limit, and network errors
- ✅ **Lazy Dependency Loading**: `requests` module only loaded when needed

## Installation

### 1. Install with HubSpot Extra

```bash
pip install -e '.[hubspot]'
```

This installs the required `requests` library.

### 2. Store HubSpot API Key

Store your HubSpot private API key in Azure Key Vault:

```bash
az keyvault secret set \
  --vault-name your-keyvault \
  --name hubspot-api-key \
  --value "YOUR_HUBSPOT_API_KEY"
```

**Getting Your API Key:**
1. Log in to HubSpot
2. Navigate to **Settings > Integrations > Private Apps**
3. Create a new private app or use existing one
4. Copy the **Private App Access Token**

### 3. Enable HubSpot Connector in Registry

Edit `config/connectors/connector_registry.yaml`:

```yaml
crm:
  active: 
    - hubspot  # Add this line to enable
  connectors:
    hubspot:
      type: "rest_api"
      base_url: "https://api.hubapi.com"
      auth_type: "bearer_token"
      keyvault_secret: "hubspot-api-key"
      endpoints:
        contacts: "/crm/v3/objects/contacts"
        companies: "/crm/v3/objects/companies"
        deals: "/crm/v3/objects/deals"
        tickets: "/crm/v3/objects/tickets"
      rate_limit: 10
      landing_path: "raw/crm/hubspot"
      properties:
        contacts:
          - firstname
          - lastname
          - email
          - phone
          - lifecyclestage
          - hs_lead_status
        companies:
          - name
          - industry
          - website
          - country
          - annualrevenue
        deals:
          - dealname
          - amount
          - dealstage
          - closedate
          - hubspotownerid
        tickets:
          - subject
          - content
          - hs_ticket_priority
          - status
          - hs_ticket_category
```

## Usage

### Basic Example

```python
from src.connectors.connector_factory import create_connector
from src.connectors.base_connector import ConnectorConfig

# Create connector config
hubspot_config = ConnectorConfig(
    name="hubspot",
    connector_type="api",
    keyvault_secret="hubspot-api-key",
    landing_path="raw/crm/hubspot",
    extra={
        "base_url": "https://api.hubapi.com",
        "rate_limit": 10,
    }
)

# Create connector (API key resolved from Key Vault)
connector = create_connector(hubspot_config, secret_value="YOUR_API_KEY")

# Validate connection
if connector.validate_connection():
    print("✅ Connected to HubSpot")
else:
    print("❌ Connection failed")

# Extract contacts with automatic pagination
contacts = connector.extract(
    "/crm/v3/objects/contacts",
    params={
        "limit": 100,
        "properties": ["firstname", "lastname", "email", "phone"]
    }
)
print(f"Fetched {len(contacts)} contacts")
```

### Extract Data from Different Objects

```python
# Fetch contacts
contacts = connector.extract(
    "/crm/v3/objects/contacts",
    params={"limit": 100, "properties": ["firstname", "lastname", "email"]}
)

# Fetch companies
companies = connector.extract(
    "/crm/v3/objects/companies",
    params={"limit": 50, "properties": ["name", "industry", "website"]}
)

# Fetch deals
deals = connector.extract(
    "/crm/v3/objects/deals",
    params={"limit": 100, "properties": ["dealname", "amount", "dealstage"]}
)

# Fetch tickets
tickets = connector.extract(
    "/crm/v3/objects/tickets",
    params={"limit": 100, "properties": ["subject", "content", "status"]}
)
```

### Get Available Schema

```python
schema = connector.get_schema()
for obj in schema['objects']:
    print(f"\nObject: {obj['name']}")
    print(f"Endpoint: {obj['endpoint']}")
    print(f"Properties: {obj['properties']}")
```

### Get Metadata

```python
metadata = connector.get_metadata()
print(f"Source: {metadata['_source_system']}")          # 'hubspot'
print(f"Type: {metadata['_connector_type']}")           # 'api'
print(f"Landing Path: {metadata['_landing_path']}")     # 'raw/crm/hubspot'
```

## Architecture

### Class Hierarchy

```
BaseConnector (ABC)
    └── APIConnector
            └── HubSpotConnector
```

### HubSpotConnector Implementation

**File:** `src/connectors/hubspot_connector.py`

**Key Methods:**

- `__init__(config, api_key)` - Initialize with config and API key
- `validate_connection() -> bool` - Test HubSpot API connectivity
- `extract(endpoint, params) -> List[Dict]` - Fetch data with auto-pagination
- `get_schema() -> Dict` - Return available objects and properties
- `_make_request(method, endpoint, params) -> Dict | None` - Low-level HTTP client
- `_load_requests() -> Module` - Lazy-load requests module

### Pagination Flow

The connector automatically handles HubSpot's cursor-based pagination:

```
Request 1 ──► Page 1 (results + paging.next.after) ──► Accumulate
               after = "cursor123"
                   │
                   ▼
Request 2 ──► Page 2 (results + paging.next.after) ──► Accumulate
               after = None (end)
                   │
                   ▼
Return All Results
```

## Configuration Reference

### Registry YAML (config/connectors/connector_registry.yaml)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | ✅ | Must be "rest_api" |
| `base_url` | string | ✅ | HubSpot API base: https://api.hubapi.com |
| `auth_type` | string | ✅ | Must be "bearer_token" |
| `keyvault_secret` | string | ✅ | Key Vault secret name for API key |
| `endpoints` | dict | ❌ | Optional: endpoint mappings for reference |
| `rate_limit` | int | ❌ | Requests per second (default: 10) |
| `landing_path` | string | ✅ | Azure Fabric path for raw data |
| `properties` | dict | ❌ | Object properties to extract |

### ConnectorConfig Extra Parameters

```python
extra={
    "base_url": "https://api.hubapi.com",    # Optional, defaults to HubSpot API
    "rate_limit": 10,                         # Optional, requests/sec
    "schema": {...}                           # Optional, override default schema
}
```

## Error Handling

The connector gracefully handles:

| Error | Status | Handling |
|-------|--------|----------|
| **401 Unauthorized** | Auth failure | Logs error, returns None |
| **429 Rate Limit** | Rate limited | Logs warning (caller should implement backoff) |
| **Connection Timeout** | Network error | Logs error, returns None |
| **Invalid Endpoint** | 404/5xx | Logs error with status code, returns None |
| **Missing requests module** | Import error | Raises ModuleNotFoundError with installation hint |

## Testing

### Run Unit Tests

```bash
# All HubSpot tests
python3.11 -m pytest tests/unit/connectors/test_hubspot_connector.py -v

# Specific test
python3.11 -m pytest tests/unit/connectors/test_hubspot_connector.py::TestHubSpotConnector::test_extract_with_pagination -v

# With coverage
python3.11 -m pytest tests/unit/connectors/test_hubspot_connector.py --cov=src.connectors.hubspot_connector
```

### Test Coverage

Current test suite includes:
- ✅ Initialization and configuration
- ✅ Connection validation (success & failure)
- ✅ Single-page data extraction
- ✅ Multi-page pagination handling
- ✅ Query parameter passing
- ✅ Schema retrieval (default & custom)
- ✅ Metadata generation
- ✅ HTTP request success/error handling
- ✅ Timeout and network error handling
- ✅ Lazy module loading

**Test Status:** 14/14 passing ✅

## Integration with Pipeline

### 1. Connector Factory Registration

The HubSpot connector is automatically registered in `src/connectors/connector_factory.py`:

```python
CONNECTOR_CLASS_MAP = {
    ...
    "hubspot": HubSpotConnector,
    ...
}
```

### 2. Automatic Instantiation

```python
from src.connectors.connector_factory import create_connector, load_registry

registry = load_registry()
active_connectors = get_active_connectors(registry)

# HubSpot connector is instantiated if enabled in registry
hubspot_conn = active_connectors["crm_hubspot"]
```

### 3. Data Landing

Extracted data is automatically landed in:
- **Path:** `raw/crm/hubspot/{object_type}/{ingestion_date}`
- **Format:** Parquet
- **Partitioning:** By ingestion date and source system
- **Metadata:** Automatically attached (`_source_system`, `_connector_type`, `_landing_path`)

## Example: Full Pipeline Integration

```python
from src.connectors.connector_factory import (
    load_registry, 
    get_active_connectors,
    create_connector
)
from azure.keyvault.secrets import SecretClient

# Load registry
registry = load_registry()

# Get active connectors (includes HubSpot if enabled)
active_configs = get_active_connectors(registry)

# Get secrets from Key Vault
kv_client = SecretClient(...)
hubspot_key = kv_client.get_secret("hubspot-api-key").value

# Create connector
hubspot_config = active_configs["crm_hubspot"]
hubspot_connector = create_connector(hubspot_config, hubspot_key)

# Extract and process
if hubspot_connector.validate_connection():
    contacts = hubspot_connector.extract(
        "/crm/v3/objects/contacts",
        params={"limit": 100}
    )
    
    # Add metadata and land data
    metadata = hubspot_connector.get_metadata()
    # ... land data to Azure Fabric with metadata
else:
    print("HubSpot connection failed")
```

## Troubleshooting

### "requests is required for the HubSpot connector"

**Issue:** Module not found error when extracting data

**Solution:**
```bash
pip install -e '.[hubspot]'
```

### "Unauthorized: Invalid or expired HubSpot API key"

**Issue:** 401 response from HubSpot API

**Solutions:**
1. Verify API key in Key Vault: `az keyvault secret show --vault-name ... --name hubspot-api-key`
2. Generate new private app token in HubSpot
3. Check API key has required permissions (contacts, companies, deals, etc.)

### "Rate limit hit on..."

**Issue:** 429 responses (rate limiting)

**Solution:** Implement exponential backoff in calling code:
```python
import time

max_retries = 5
for attempt in range(max_retries):
    try:
        data = connector.extract(endpoint, params)
        break
    except RateLimitError:
        wait_time = 2 ** attempt
        print(f"Rate limited, waiting {wait_time}s...")
        time.sleep(wait_time)
```

### "Connection validation failed"

**Issue:** Cannot connect to HubSpot API

**Solutions:**
1. Check internet connectivity
2. Verify API endpoint: https://api.hubapi.com
3. Check API key permissions
4. Review logs for detailed error message

## Performance Considerations

- **Pagination:** Large datasets are automatically paginated (default 100 items/page)
- **Rate Limiting:** Default 10 req/sec; adjust in config if needed
- **Connection Pooling:** `requests` library handles HTTP connection pooling
- **Memory:** Pagination loads all results into memory; consider filtering in extraction params

## Security Best Practices

✅ **Do:**
- Store API keys in Azure Key Vault (never in code)
- Use separate API keys for dev/test/prod
- Restrict API key permissions to minimum needed
- Rotate API keys periodically
- Audit API key usage in HubSpot

❌ **Don't:**
- Commit API keys to version control
- Share API keys across environments
- Log full API responses (may contain sensitive data)
- Use admin-level API keys for data extraction

## References

- **HubSpot API Documentation:** https://developers.hubspot.com/docs/api/crm/contacts
- **Private App Setup:** https://developers.hubspot.com/docs/api/private-apps
- **API Rate Limits:** https://developers.hubspot.com/docs/api/usage/rate-limits
- **Available Properties:** https://developers.hubspot.com/docs/crm/properties

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review connector tests: `tests/unit/connectors/test_hubspot_connector.py`
3. Review example usage: `examples/hubspot_connector_example.py`
4. Check pipeline logs for detailed error messages
