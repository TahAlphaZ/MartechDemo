---
name: add-ecommerce-connector
description: >
  Add a new ecommerce platform connector to the MarTech Fabric Pipeline.
  Integrates platforms like Shopify, Magento, WooCommerce, BigCommerce,
  Salesforce Commerce Cloud, or custom ecommerce APIs. Use this skill whenever
  the client needs order, product, or customer data from their ecommerce
  platform flowing into the medallion lakehouse. Triggers on: "add Shopify",
  "connect Magento", "WooCommerce integration", "ecommerce data", "order data
  connector", "product catalog sync", "add online store data", or any request
  involving ecommerce platform data ingestion.
---

# Add Ecommerce Connector

You are extending the MarTech Fabric Pipeline to ingest data from a new ecommerce
platform. Ecommerce connectors are the revenue backbone of MarTech — they provide
order data, product catalogs, and customer purchase history that drive ROI
attribution and BI dashboards.

## What You're Building

An ecommerce connector pulls transactional data (orders, products, customers,
inventory) from a platform's REST API into the raw zone. This data flows through
Silver (cleansed, joined) to Gold (fact_orders, dim_products, agg_daily_revenue)
for Power BI consumption.

## Before You Start

Read these files — the ecommerce section already has working examples:

1. `config/connectors/connector_registry.yaml` — see `ecommerce:` section.
   Shopify is already configured as a reference implementation.
2. `src/connectors/base_connector.py` — the `APIConnector` base class.
3. `src/connectors/connector_factory.py` — registration point.
4. `src/transformations/silver_to_gold/transform.py` — see `fact_orders`,
   `dim_products`, `agg_daily_revenue` models that consume ecommerce data.

## Step-by-Step Process

### 1. Register in connector_registry.yaml

Add under `ecommerce.connectors` (follow the Shopify pattern):

```yaml
ecommerce:
  active:
    - shopify
    - your_platform    # Add to active list
  connectors:
    your_platform:
      base_url: "https://{domain}/api/v2"
      auth_type: "bearer_token"
      keyvault_secret: "your-platform-api-key"
      endpoints:
        orders: "/orders"
        products: "/products"
        customers: "/customers"
        inventory: "/inventory"        # Optional
        refunds: "/refunds"            # Optional
      rate_limit: 5
      landing_path: "raw/ecommerce/your_platform"
      # Ecommerce-specific: currency and pagination config
      default_currency: "USD"
      pagination_type: "cursor"        # or "offset", "page_number"
      max_page_size: 250
```

Key ecommerce-specific fields:

- **pagination_type** — ecommerce APIs paginate differently. Shopify uses cursor,
  Magento uses page numbers, WooCommerce uses offsets. Your connector must handle
  the platform's specific pattern.
- **max_page_size** — the maximum records per API call. Larger = fewer calls.
- **endpoints** — at minimum, include orders, products, and customers.

### 2. Create the Connector Class

Create `src/connectors/{platform}_ecommerce_connector.py`:

```python
from typing import Any, Dict, List, Optional
from datetime import datetime
from src.connectors.base_connector import APIConnector, ConnectorConfig


class {Platform}EcommerceConnector(APIConnector):

    def __init__(self, config: ConnectorConfig, api_key: str):
        super().__init__(config, api_key)
        self.pagination_type = config.extra.get("pagination_type", "cursor")
        self.max_page_size = config.extra.get("max_page_size", 250)

    def extract(self, query_or_endpoint: str, params: Optional[Dict] = None) -> Any:
        """Handle platform-specific pagination."""
        if self.pagination_type == "cursor":
            return self._extract_cursor(query_or_endpoint, params)
        elif self.pagination_type == "offset":
            return self._extract_offset(query_or_endpoint, params)
        elif self.pagination_type == "page_number":
            return self._extract_paged(query_or_endpoint, params)
        return super().extract(query_or_endpoint, params)

    def extract_orders(self, since: Optional[datetime] = None) -> List[Dict]:
        """Extract orders with optional date filter."""
        params = {"limit": self.max_page_size}
        if since:
            params["created_at_min"] = since.isoformat()
        endpoint = self.config.extra.get("endpoints", {}).get("orders", "/orders")
        return self.extract(endpoint, params)

    def extract_products(self) -> List[Dict]:
        endpoint = self.config.extra.get("endpoints", {}).get("products", "/products")
        return self.extract(endpoint, {"limit": self.max_page_size})

    def extract_customers(self, since: Optional[datetime] = None) -> List[Dict]:
        params = {"limit": self.max_page_size}
        if since:
            params["updated_at_min"] = since.isoformat()
        endpoint = self.config.extra.get("endpoints", {}).get("customers", "/customers")
        return self.extract(endpoint, params)

    def _extract_cursor(self, endpoint, params):
        """Cursor-based pagination (Shopify-style)."""
        all_data = []
        next_cursor = None
        while True:
            req_params = {**(params or {})}
            if next_cursor:
                req_params["page_info"] = next_cursor
            response = self._make_request("GET", endpoint, req_params)
            data = response.get("data", response.get("orders", response.get("products", [])))
            all_data.extend(data)
            next_cursor = response.get("next_cursor")
            if not next_cursor:
                break
        return all_data

    def _extract_offset(self, endpoint, params):
        """Offset-based pagination (WooCommerce-style)."""
        all_data, offset = [], 0
        while True:
            req_params = {**(params or {}), "offset": offset}
            response = self._make_request("GET", endpoint, req_params)
            data = response if isinstance(response, list) else response.get("data", [])
            if not data:
                break
            all_data.extend(data)
            offset += len(data)
        return all_data

    def _extract_paged(self, endpoint, params):
        """Page number pagination (Magento-style)."""
        all_data, page = [], 1
        while True:
            req_params = {**(params or {}), "page": page}
            response = self._make_request("GET", endpoint, req_params)
            items = response.get("items", response.get("data", []))
            if not items:
                break
            all_data.extend(items)
            page += 1
        return all_data

    def get_schema(self) -> Dict[str, Any]:
        return {
            "orders": {
                "primary_key": "order_id",
                "fields": ["order_id", "customer_id", "order_date", "total_amount",
                           "currency", "status", "channel", "source", "line_items"],
            },
            "products": {
                "primary_key": "product_id",
                "fields": ["product_id", "title", "sku", "price", "category",
                           "inventory_quantity", "status"],
            },
            "customers": {
                "primary_key": "customer_id",
                "fields": ["customer_id", "email", "first_name", "last_name",
                           "total_orders", "total_spent", "created_at"],
            },
        }
```

### 3. Register and Write Tests

Register in `CONNECTOR_CLASS_MAP`, then write TDD tests covering:

- `test_cursor_pagination` — multi-page cursor traversal
- `test_offset_pagination` — offset increments correctly
- `test_extract_orders_filters_by_date`
- `test_extract_products_returns_all`
- `test_schema_has_line_items` — orders include line-level detail
- `test_handles_api_rate_limit` — respects rate_limit config

### 4. Data Quality Tests

- Order amounts are positive and non-null
- Order dates are valid and not in the future
- Product SKUs are unique
- Customer emails are valid format
- Currency codes are ISO 4217

### 5. Gold Model Verification

Your ecommerce data feeds these existing Gold models — verify they still
produce correct output:

- `fact_orders` — order-level fact table
- `dim_products` — product dimension
- `agg_daily_revenue` — daily revenue by channel

## Completion Checklist

- [ ] Registry entry following Shopify pattern
- [ ] Connector class with correct pagination type
- [ ] Registered in `CONNECTOR_CLASS_MAP`
- [ ] Unit tests (pagination, date filtering, schema)
- [ ] Data quality tests (amounts, dates, uniqueness)
- [ ] Gold models verified
- [ ] Key Vault secret stored
- [ ] End-to-end in dev
