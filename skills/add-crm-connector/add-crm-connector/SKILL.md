---
name: add-crm-connector
description: >
  Add a CRM (Customer Relationship Management) connector to the MarTech Fabric
  Pipeline. Integrates platforms like Salesforce, HubSpot, Microsoft Dynamics 365,
  Zoho CRM, or Pipedrive. Use this skill whenever the client needs CRM data
  (contacts, leads, deals, accounts, activities) flowing into the medallion
  lakehouse for unified customer views and BI dashboards. Triggers on: "add
  Salesforce", "connect HubSpot", "integrate CRM", "Dynamics 365 connector",
  "sync CRM data", "customer data from CRM", "add contact/lead/deal data",
  or any request involving CRM platform integration.
---

# Add CRM Connector

You are extending the MarTech Fabric Pipeline to ingest CRM data. CRM connectors
are particularly important in MarTech because they provide the customer backbone —
contacts, leads, deals, and activity history that tie marketing attribution to
actual revenue.

## What You're Building

A CRM connector pulls entity data (contacts, companies, deals, activities) from
a CRM platform's API, lands it in the raw zone, then flows through Silver
(cleansed/conformed) to Gold (unified customer dimension) for BI consumption.

The key challenge with CRM data is that each platform models entities differently.
Your connector maps platform-specific object names to standard entity names so
the Silver layer can apply uniform transformations.

## Before You Start

Read these files:

1. `config/connectors/connector_registry.yaml` — the `ecommerce:` section is
   the closest pattern. CRM connectors follow the same API connector shape.
2. `src/connectors/base_connector.py` — the `APIConnector` base class.
3. `src/connectors/connector_factory.py` — register your class here.
4. `src/transformations/silver_to_gold/transform.py` — see `dim_customers` in
   `GOLD_MODEL_REGISTRY`. Your CRM data feeds this dimension.

## Step-by-Step Process

### 1. Add CRM Section to connector_registry.yaml

CRM connectors get their own top-level section. If `crm:` doesn't exist, add it
after the `analytics:` section:

```yaml
crm:
  active: ["salesforce"]
  connectors:
    salesforce:
      type: "rest_api"
      base_url: "https://{instance}.salesforce.com/services/data/v59.0"
      auth_type: "oauth2"
      keyvault_secret: "salesforce-oauth-credentials"
      endpoints:
        contacts: "/sobjects/Contact"
        leads: "/sobjects/Lead"
        opportunities: "/sobjects/Opportunity"
        accounts: "/sobjects/Account"
        activities: "/sobjects/Task"
        campaigns: "/sobjects/Campaign"
      rate_limit: 100
      landing_path: "raw/crm/salesforce"
      entity_mapping:
        contacts: "Contact"
        leads: "Lead"
        deals: "Opportunity"
        companies: "Account"

    hubspot:
      type: "rest_api"
      base_url: "https://api.hubapi.com/crm/v3"
      auth_type: "bearer_token"
      keyvault_secret: "hubspot-api-key"
      endpoints:
        contacts: "/objects/contacts"
        companies: "/objects/companies"
        deals: "/objects/deals"
        activities: "/objects/engagements"
      rate_limit: 10
      landing_path: "raw/crm/hubspot"
      entity_mapping:
        contacts: "contacts"
        leads: "contacts"
        deals: "deals"
        companies: "companies"

    dynamics365:
      type: "rest_api"
      base_url: "https://{org}.api.crm.dynamics.com/api/data/v9.2"
      auth_type: "oauth2"
      keyvault_secret: "dynamics365-oauth-credentials"
      endpoints:
        contacts: "/contacts"
        leads: "/leads"
        opportunities: "/opportunities"
        accounts: "/accounts"
      rate_limit: 60
      landing_path: "raw/crm/dynamics365"
      entity_mapping:
        contacts: "contacts"
        leads: "leads"
        deals: "opportunities"
        companies: "accounts"
```

The `entity_mapping` field standardizes platform-specific names so the pipeline
can treat all CRMs uniformly downstream.

### 2. Update connector_factory.py

Add CRM handling in `get_active_connectors()`:

```python
# CRM connectors
crm_section = registry.get("crm", {})
for crm_name in crm_section.get("active", []):
    crm_config = crm_section.get("connectors", {}).get(crm_name, {})
    active[f"crm_{crm_name}"] = ConnectorConfig(
        name=crm_name,
        connector_type="crm",
        keyvault_secret=crm_config.get("keyvault_secret", ""),
        landing_path=crm_config.get("landing_path", f"raw/crm/{crm_name}"),
        extra=crm_config,
    )
```

Also update the `create_connector` function to handle `connector_type="crm"`
(it maps to `APIConnector` subclasses).

### 3. Create the CRM Connector Class

CRM connectors have needs beyond generic API connectors:

- **Incremental extraction** — CRM tables are large; pull only records modified
  since the last sync using `lastModifiedDate` filters.
- **Relationship resolution** — CRM objects reference each other by ID (e.g.,
  a Deal has a `contact_id`). Capture foreign keys for Silver-layer joins.
- **Bulk API** — For initial loads (>50k records), use the platform's bulk API
  if available (Salesforce Bulk API 2.0, HubSpot batch reads).
- **Entity mapping** — Translate platform object names to standard entity names.

Create `src/connectors/{platform}_crm_connector.py`:

```python
from typing import Any, Dict, List, Optional
from datetime import datetime
from src.connectors.base_connector import APIConnector, ConnectorConfig


class {Platform}CRMConnector(APIConnector):

    def __init__(self, config: ConnectorConfig, api_key: str):
        super().__init__(config, api_key)
        self.entity_mapping = config.extra.get("entity_mapping", {})

    def extract_entity(self, entity: str, since: Optional[datetime] = None,
                       fields: Optional[List[str]] = None) -> List[Dict]:
        """Extract a CRM entity. Uses standard names mapped to platform objects."""
        platform_entity = self.entity_mapping.get(entity, entity)
        endpoint = self.config.extra.get("endpoints", {}).get(entity, f"/{platform_entity}")
        params = {}
        if since:
            params["modified_since"] = since.isoformat()
        if fields:
            params["properties"] = ",".join(fields)
        return self.extract(endpoint, params)

    def extract_all_entities(self, since: Optional[datetime] = None) -> Dict[str, List]:
        """Extract all configured entities."""
        return {entity: self.extract_entity(entity, since=since)
                for entity in self.entity_mapping}

    def get_schema(self) -> Dict[str, Any]:
        return {
            "contacts": {
                "primary_key": "id",
                "fields": ["id", "email", "first_name", "last_name", "phone",
                           "company_id", "created_date", "last_modified_date"],
                "foreign_keys": {"company_id": "companies.id"},
            },
            "deals": {
                "primary_key": "id",
                "fields": ["id", "name", "amount", "stage", "close_date",
                           "contact_id", "company_id"],
                "foreign_keys": {"contact_id": "contacts.id",
                                 "company_id": "companies.id"},
            },
            "companies": {
                "primary_key": "id",
                "fields": ["id", "name", "domain", "industry", "revenue"],
            },
        }
```

### 4. Write Tests FIRST (TDD)

- `test_entity_mapping_resolves` — standard names → platform objects
- `test_incremental_extraction_filters_by_date`
- `test_extract_all_entities_covers_registry`
- `test_schema_has_foreign_keys`
- `test_bulk_api_used_for_large_datasets` (if applicable)

### 5. Update Gold Model

In `silver_to_gold/transform.py`, update `dim_customers` to include CRM:

```python
GOLD_MODEL_REGISTRY["dim_customers"]["silver_sources"].append("crm_contacts")
GOLD_MODEL_REGISTRY["dim_customers"]["silver_sources"].append("crm_companies")
```

Consider adding a `fact_deals` model for pipeline/revenue analytics.

### 6. Data Quality Tests

- IDs are unique per entity
- Email format valid for contacts
- Deal amounts are positive
- Entity type is tagged in metadata

## Completion Checklist

- [ ] CRM section in `connector_registry.yaml`
- [ ] `get_active_connectors()` handles CRM section
- [ ] Connector class with entity mapping + incremental sync
- [ ] Registered in `CONNECTOR_CLASS_MAP`
- [ ] Unit tests pass
- [ ] `dim_customers` Gold model updated
- [ ] Data quality tests for raw CRM data
- [ ] Key Vault secret stored
- [ ] End-to-end in dev
