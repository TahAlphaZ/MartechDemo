# HubSpot Connector

The HubSpot connector enables seamless integration of HubSpot CRM data into the MarTech Fabric pipeline. It supports extraction of contacts, companies, deals, tickets, and other HubSpot objects via the REST API v3.

## Overview

- **Type**: REST API Connector
- **Authentication**: Bearer Token (API Key)
- **Base URL**: `https://api.hubapi.com`
- **Rate Limit**: 10 requests/second (configurable)
- **Supported Objects**: Contacts, Companies, Deals, Tickets, and more

## Features

✅ **Automatic Pagination** - Seamlessly handles multi-page API responses
✅ **Property Selection** - Query only the properties you need
✅ **Error Handling** - Graceful handling of rate limits, timeouts, and auth errors
✅ **Schema Discovery** - Built-in schema for all major HubSpot objects
✅ **Connection Validation** - Health check before extraction begins
✅ **Metadata Tracking** - Automatic source and landing path metadata

## Installation

### Option 1: Install with HubSpot support
```bash
pip install martech-fabric-pipeline[hubspot]
```

This installs `requests>=2.31` which is required for HTTP communication.

### Option 2: Manual installation
```bash
pip install martech-fabric-pipeline requests>=2.31
```

## Configuration

### 1. Add HubSpot to connector_registry.yaml

Enable HubSpot in the CRM connectors section:

```yaml
crm:
  active:
    - hubspot
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
```

### 2. Store API Key in Key Vault

Store your HubSpot API key in Azure Key Vault:

```bash
az keyvault secret set --vault-name mykeyvault --name "hubspot-api-key" --value "YOUR_HUBSPOT_API_KEY"
```

### 3. Get Your HubSpot API Key

1. Log in to HubSpot
2. Navigate to: Settings > Integrations > Private Apps
3. Create a new private app or use an existing one
4. Copy the API key (starts with `pat-`)

## Usage

### Basic Example

```python
from src.connectors.connector_factory import create_connector
from src.connectors.base_connector import ConnectorConfig

# Create config
config = ConnectorConfig(
    name="hubspot",
    connector_type="api",
    keyvault_secret="hubspot-api-key",
    landing_path="raw/crm/hubspot",
    extra={"base_url": "https://api.hubapi.com", "rate_limit": 10}
)

# Create connector
connector = create_connector(config, secret_value="YOUR_API_KEY")

# Validate connection
if connector.validate_connection():
    print("✅ Connected to HubSpot")
```

### Fetch Contacts

```python
contacts = connector.extract(
    "/crm/v3/objects/contacts",
    params={
        "limit": 100,
        "properties": ["firstname", "lastname", "email", "phone", "lifecyclestage"]
    }
)

for contact in contacts:
    print(contact)
```

### Fetch Companies

```python
companies = connector.extract(
    "/crm/v3/objects/companies",
    params={
        "limit": 50,
        "properties": ["name", "industry", "website", "country"]
    }
)
```

### Fetch Deals

```python
deals = connector.extract(
    "/crm/v3/objects/deals",
    params={
        "limit": 100,
        "properties": ["dealname", "amount", "dealstage", "closedate"]
    }
)
```

## Available Endpoints

- **Contacts**: `/crm/v3/objects/contacts`
- **Companies**: `/crm/v3/objects/companies`
- **Deals**: `/crm/v3/objects/deals`
- **Tickets**: `/crm/v3/objects/tickets`

## References

- [HubSpot API Docs](https://developers.hubspot.com/docs/api/crm)
- [API Key Authentication](https://developers.hubspot.com/docs/api/private-apps)
