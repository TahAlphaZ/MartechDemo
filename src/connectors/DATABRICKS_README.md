# Databricks Connector

The **Databricks Connector** extracts datasets from Databricks workspaces into the MarTech Fabric pipeline's raw zone. It supports Unity Catalog, SQL queries, and schema introspection.

## Features

- **Unity Catalog Support**: Access tables across catalogs and schemas
- **Flexible Querying**: Execute SQL queries or reference tables directly
- **Schema Introspection**: Automatically discover table schemas from system tables
- **Table Operations**: List tables, check existence, count rows
- **Connection Pooling**: Lazy-load SQL connections for efficiency
- **PAT Authentication**: Secure token-based authentication via Key Vault

## Configuration

Enable the connector in `config/connectors/connector_registry.yaml`:

```yaml
databricks:
  enabled: true  # Set to true to activate
  config:
    workspace_url: "https://your-workspace.databricks.com"
    http_path: "/sql/1.0/warehouses/your-warehouse-id"
    catalog: "hive_metastore"  # or your Unity Catalog name
    schema: "default"
    keyvault_secret: "databricks-personal-access-token"
    landing_path: "raw/databricks"
    tables: []  # Optional: specify tables to extract
```

### Configuration Fields

| Field | Required | Description |
|-------|----------|-------------|
| `workspace_url` | Yes | Databricks workspace URL (e.g., https://your-workspace.databricks.com) |
| `http_path` | Yes | SQL warehouse HTTP path (e.g., /sql/1.0/warehouses/abc123) |
| `catalog` | No | Unity Catalog name (default: hive_metastore) |
| `schema` | No | Default schema (default: default) |
| `keyvault_secret` | Yes | Azure Key Vault secret name storing the PAT token |
| `landing_path` | No | Raw zone path (default: raw/databricks) |
| `tables` | No | Optional list of specific tables to extract |

## Authentication

### Generate a Personal Access Token (PAT)

1. Log in to your Databricks workspace
2. Go to **Settings** → **User Settings** → **Access Tokens**
3. Click **Generate New Token**
4. Copy the token

### Store in Azure Key Vault

```bash
az keyvault secret set \
  --vault-name <your-keyvault> \
  --name "databricks-personal-access-token" \
  --value "<your-pat-token>"
```

## Usage Examples

### 1. Extract Data from a Table

```python
from src.connectors.connector_factory import get_active_connectors, create_connector
from src.keyvault import get_secret

# Load configuration
registry = load_registry()
active = get_active_connectors(registry)
config = active["databricks"]

# Get secret from Key Vault
pat_token = get_secret(config.keyvault_secret)

# Create connector
connector = create_connector(config, pat_token)

# Extract data
data = connector.extract("users")  # Simple table reference
# OR
data = connector.extract("SELECT * FROM users WHERE status = 'active'")  # SQL query
```

### 2. Execute Parameterized Query

```python
query = "SELECT * FROM orders WHERE customer_id = ? AND created_at > ?"
params = [12345, "2024-01-01"]
results = connector.extract(query, params)
```

### 3. List Available Tables

```python
tables = connector.list_tables()
print(f"Available tables: {tables}")
```

### 4. Check Table Existence and Row Count

```python
if connector.table_exists("customers"):
    row_count = connector.get_row_count("customers")
    print(f"Table 'customers' has {row_count} rows")
```

### 5. Retrieve Schema Information

```python
schema = connector.get_schema()
for table_name, columns in schema.items():
    print(f"{table_name}: {columns}")
```

### 6. Validate Connection

```python
if connector.validate_connection():
    print("Connected to Databricks")
else:
    print("Connection failed")
```

## Pipeline Integration

The connector integrates with the pipeline's medallion architecture:

```
Databricks Source
       ↓
    Raw Zone (parquet)
       ↓
   Silver Zone (delta) - Cleaned & Deduplicated
       ↓
    Gold Zone (delta) - Business-ready tables
```

### Automatic Metadata Attachment

All extractions automatically include:

```python
{
  "_source_system": "databricks",
  "_connector_type": "analytics",
  "_landing_path": "raw/databricks",
  "_ingestion_date": "2024-01-20",
  "_ingestion_timestamp": "2024-01-20T15:30:00Z"
}
```

## Error Handling

The connector provides robust error handling:

```python
try:
    data = connector.extract("SELECT * FROM nonexistent_table")
except Exception as e:
    logger.error(f"Extraction failed: {e}")
    # Pipeline continues with next connector
```

Common errors:

| Error | Cause | Solution |
|-------|-------|----------|
| `Connection failed` | Invalid workspace URL or PAT | Verify credentials in Key Vault |
| `Table not found` | Incorrect catalog/schema/table name | Check spelling, list tables first |
| `Query timeout` | Query is too slow | Optimize SQL query, increase warehouse size |
| `Authentication failed` | Expired PAT token | Regenerate token in Databricks Settings |

## Performance Considerations

### Query Optimization

- Use `WHERE` clauses to filter data early
- Leverage Unity Catalog statistics for better query plans
- Consider incremental extraction with `WHERE created_at > ?`

### Warehouse Configuration

- Use appropriate warehouse size for query volume
- Enable auto-stop to reduce costs
- Consider serverless SQL for small, frequent queries

### Network

- Ensure network connectivity from pipeline runner to workspace
- Databricks may require allowlisting of runner IP addresses
- Consider using PrivateLink for enhanced security

## Testing

Run unit tests:

```bash
pytest tests/unit/connectors/test_databricks_connector.py -v
```

Run integration tests (requires Databricks workspace):

```bash
pytest tests/ -m integration -k databricks
```

## Troubleshooting

### Check Connection Manually

```python
connector = DatabricksConnector(config, access_token=pat_token)
if connector.validate_connection():
    print("✓ Connection OK")
else:
    print("✗ Connection failed")
```

### Enable Debug Logging

```python
import logging
logging.getLogger("src.connectors.databricks_connector").setLevel(logging.DEBUG)
```

### Verify Table Access

```python
tables = connector.list_tables()
if "expected_table" in tables:
    print("✓ Table accessible")
else:
    print("✗ Table not found in current catalog/schema")
```

## Limitations

- SQL dialects may differ between Databricks SQL and other systems
- Very large result sets (>1GB) may require pagination in future versions
- Streaming tables are not currently supported
- Requires network connectivity to workspace

## Related Documentation

- [Databricks SQL API](https://docs.databricks.com/api/workspace/sql/endpoints)
- [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/)
- [Pipeline Architecture](../docs/architecture.md)
- [Connector Framework](./base_connector.py)
