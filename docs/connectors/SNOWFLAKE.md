# Snowflake Connector

The Snowflake connector enables the MarTech pipeline to extract marketing analytics data from Snowflake data warehouses. It extends the `DatabaseConnector` base class and provides specialized functionality for Snowflake's SQL dialect and authentication.

## Features

- **Secure Connection Management**: Uses Key Vault for credential storage
- **SQL Query Execution**: Execute any Snowflake SQL query and retrieve results as Python dictionaries
- **Schema Introspection**: Automatically discover tables and columns in your Snowflake schema
- **Data Sampling**: Extract sample data from large tables for validation
- **Connection Pooling**: Reuses connections for better performance
- **Error Handling**: Comprehensive error logging and exception handling

## Configuration

### Enable Snowflake in Registry

Update `config/connectors/connector_registry.yaml` to activate Snowflake:

```yaml
databases:
  active: snowflake  # Switch from postgres to snowflake
  connectors:
    snowflake:
      driver: "snowflake.connector"
      keyvault_secret: "snowflake-connection-params"
      account_id_secret: "snowflake-account-id"
      warehouse: "ANALYTICS"
      database: "MARKETING"
      schema: "RAW"
      role: "TRANSFORMER"
      test_query: "SELECT 1 as test_value"
```

### Key Vault Secrets

Store connection parameters in Azure Key Vault:

**Secret Name**: `snowflake-connection-params`
```json
{
  "account": "xy12345.us-east-1",
  "user": "analytics_user",
  "password": "your_password",
  "warehouse": "ANALYTICS",
  "database": "MARKETING",
  "schema": "RAW",
  "role": "TRANSFORMER"
}
```

### Connection Parameters

- **account**: Snowflake account identifier (e.g., `xy12345.us-east-1`)
- **user**: Snowflake username
- **password**: Snowflake password
- **warehouse**: Warehouse name (e.g., `ANALYTICS`)
- **database**: Database name (e.g., `MARKETING`)
- **schema**: Schema name (defaults to `PUBLIC`)
- **role**: Snowflake role (defaults to `TRANSFORMER`)

## Usage Examples

### Basic Setup

```python
from src.connectors.connector_factory import load_registry, get_active_connectors, create_connector
from azure.keyvault.secrets import SecretClient

# Load registry
registry = load_registry()
active_connectors = get_active_connectors(registry)

# Get connection parameters from Key Vault
keyvault_client = SecretClient(vault_url="...", credential=credential)
connection_params = keyvault_client.get_secret("snowflake-connection-params")

# Create connector
snowflake_config = active_connectors["db_snowflake"]
connector = create_connector(snowflake_config, connection_params.value)

# Validate connection
if connector.validate_connection():
    print("Connected to Snowflake!")
```

### Query Execution

```python
# Simple SELECT
campaigns = connector.extract("SELECT * FROM MARKETING.CAMPAIGNS LIMIT 1000")
for campaign in campaigns:
    print(f"Campaign: {campaign['ID']} - {campaign['NAME']}")

# Parameterized query
query = "SELECT * FROM MARKETING.CAMPAIGNS WHERE ACTIVE = %s"
active_campaigns = connector.extract(query, ["true"])

# Aggregation query
summary = connector.extract("""
    SELECT 
        DATE_TRUNC('day', CREATED_AT) as date,
        COUNT(*) as campaign_count,
        SUM(BUDGET) as total_budget
    FROM MARKETING.CAMPAIGNS
    GROUP BY DATE_TRUNC('day', CREATED_AT)
    ORDER BY date DESC
""")
```

### Schema Discovery

```python
# Get schema for the connected database
schema_info = connector.get_schema()
for column in schema_info:
    print(f"{column['TABLE_NAME']}.{column['COLUMN_NAME']}: {column['DATA_TYPE']}")

# List available tables
tables = connector.get_available_tables()
print(f"Available tables: {tables}")
```

### Data Sampling and Validation

```python
# Sample 100 rows from a table
sample = connector.get_table_sample("MARKETING.CUSTOMERS", limit=100)
print(f"Sample data: {sample[:5]}")

# Get row count for data profiling
count = connector.get_table_count("MARKETING.ORDERS")
print(f"Total orders: {count}")
```

### Connection Lifecycle

```python
# Connector automatically manages connection pooling
# For explicit cleanup:
connector.close()
```

## Integration with Pipeline

The connector integrates with the Medallion architecture:

1. **Raw Zone**: Data extracted via connector lands in `raw/database/snowflake/{table_name}`
2. **Silver Zone**: Data is deduplicated and validated using latest-by-timestamp strategy
3. **Gold Zone**: Aggregated analytics tables available for reporting

### ETL Example

```python
from src.connectors.connector_factory import load_registry, create_connector

# Pipeline stage 1: Extract from Snowflake
registry = load_registry()
connectors = get_active_connectors(registry)
sf_connector = create_connector(connectors["db_snowflake"], secret_value)

# Extract marketing data
campaigns = sf_connector.extract("SELECT * FROM MARKETING.CAMPAIGNS")
customers = sf_connector.extract("SELECT * FROM MARKETING.CUSTOMERS")
orders = sf_connector.extract("SELECT * FROM MARKETING.ORDERS")

# Stage 2: Land raw data
# (Framework automatically writes to raw zone with metadata)

# Stage 3: Transformation (Silver zone)
# Deduplicate, validate, standardize data

# Stage 4: Analytics (Gold zone)
# Create dimensional models for BI tools
```

## Performance Considerations

### Query Optimization

- Use `LIMIT` clauses for large tables during development
- Leverage Snowflake's native INFORMATION_SCHEMA for schema queries
- Consider materializing frequently-used subsets as views

### Large Exports

For tables exceeding 1M rows, consider:

```python
# Fetch in batches
offset = 0
batch_size = 100000
all_data = []

while True:
    batch = connector.extract(f"""
        SELECT * FROM LARGE_TABLE
        LIMIT {batch_size} OFFSET {offset}
    """)
    if not batch:
        break
    all_data.extend(batch)
    offset += batch_size
```

### Connection Pooling

The connector maintains a single connection per instance:

```python
# Reuse same connector for multiple queries
connector = create_connector(config, params)
data1 = connector.extract("SELECT ...")
data2 = connector.extract("SELECT ...")  # Reuses connection
```

## Error Handling

```python
try:
    data = connector.extract("SELECT * FROM INVALID_TABLE")
except Exception as e:
    print(f"Query failed: {e}")
    # Logs include full error details for debugging
```

## Monitoring and Logging

The connector logs connection events and query execution:

```python
import logging

logger = logging.getLogger("src.connectors.snowflake_connector")
logger.setLevel(logging.DEBUG)  # Get detailed query logs
```

Log messages include:
- Connection establishment/closure
- Connection validation results
- Query execution and row counts
- Error details with full stack traces

## Testing

Unit tests are provided in `tests/unit/connectors/test_snowflake_connector.py`:

```bash
# Run all connector tests
pytest tests/unit/connectors/ -v

# Run only Snowflake tests
pytest tests/unit/connectors/test_snowflake_connector.py -v

# Run with coverage
pytest tests/unit/connectors/test_snowflake_connector.py --cov=src.connectors.snowflake_connector
```

## Troubleshooting

### Connection Issues

```
Error: snowflake.connector.errors.OperationalError
→ Check account ID, username, password in Key Vault
→ Verify Snowflake account is active
→ Check network connectivity to Snowflake cloud
```

### Query Failures

```
Error: SQL Syntax Error
→ Verify query syntax for Snowflake SQL (T-SQL compatible)
→ Use uppercase for object names (MARKETING.CAMPAIGNS)
→ Check schema and table names exist
```

### Permission Errors

```
Error: Insufficient privileges
→ Verify role has SELECT permission on tables
→ Check schema is accessible to role
→ Confirm warehouse is available to role
```

## Migration Guide

To switch from Postgres to Snowflake:

1. Update `connector_registry.yaml`:
   ```yaml
   databases:
     active: snowflake
   ```

2. Set up Snowflake credentials in Key Vault

3. Run validation:
   ```python
   if connector.validate_connection():
       print("Ready to switch!")
   ```

4. No other code changes needed — the factory abstracts the swap

## License

Part of the MarTech Fabric Pipeline project.
