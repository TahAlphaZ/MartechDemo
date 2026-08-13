# Microsoft Access Connector

The Microsoft Access connector enables the MarTech pipeline to extract marketing analytics data from Microsoft Access databases (.mdb and .accdb formats). It extends the `DatabaseConnector` base class and provides specialized functionality for Access SQL dialect and ODBC connectivity.

## Features

- **File-Based Database Support**: Works with both legacy .mdb (97-2003) and modern .accdb (2007+) formats
- **ODBC Connectivity**: Uses standard ODBC drivers for broad platform compatibility
- **Secure Connection Management**: Uses Key Vault for credential and file path storage
- **SQL Query Execution**: Execute SQL queries and retrieve results as Python dictionaries
- **Schema Introspection**: Automatically discover tables and columns in your Access database
- **Data Sampling**: Extract sample data from tables for validation and profiling
- **System Table Filtering**: Automatically filters out Access system tables (MSys*)
- **Error Handling**: Comprehensive error logging and exception handling

## Requirements

### System Requirements

- Python 3.10+
- ODBC driver for Microsoft Access installed on the system:
  - **Windows**: Microsoft Access Database Engine (included with Office or available separately)
  - **macOS**: Microsoft ODBC Driver for Access (if available) or alternative like MDBtools
  - **Linux**: mdbtools or similar

### Python Dependencies

```bash
pip install martech-fabric-pipeline[connectors]
# or install directly:
pip install pyodbc>=4.0.39
```

## Configuration

### Enable Microsoft Access in Registry

Update `config/connectors/connector_registry.yaml` to activate Microsoft Access:

```yaml
databases:
  active: microsoft_access  # Switch from postgres to microsoft_access
  connectors:
    microsoft_access:
      driver: "{Microsoft Access Driver (*.accdb)}"
      arm_linked_service_type: "MicrosoftAccess"
      keyvault_secret: "microsoft-access-connection"
      file_format: "accdb"  # accdb (2007+) or mdb (97-2003)
      test_query: "SELECT 1 as test_value"
      readonly: true
```

### Key Vault Secrets

Store connection parameters in Azure Key Vault:

**Secret Name**: `microsoft-access-connection`
```json
{
  "file_path": "/mnt/data/marketing_analytics.accdb",
  "password": "your_access_database_password",
  "readonly": true
}
```

Or for legacy .mdb files:
```json
{
  "file_path": "/mnt/data/legacy_campaigns.mdb",
  "password": "legacy_password",
  "readonly": false
}
```

### Connection Parameters

- **file_path** (required): Full path to the Access database file (.mdb or .accdb)
- **password** (optional): Database password if the Access file is password-protected
- **readonly** (optional, default: true): Set to false only if write operations are needed

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
connection_params = keyvault_client.get_secret("microsoft-access-connection").value

# Create connector
access_config = active_connectors["db_microsoft_access"]
connector = create_connector(access_config, connection_params)

# Validate connection
if connector.validate_connection():
    print("Connected to Microsoft Access!")
```

### Query Execution

```python
# Simple SELECT query
campaigns = connector.extract("SELECT * FROM [Campaigns] LIMIT 1000")
for campaign in campaigns:
    print(f"Campaign: {campaign['ID']} - {campaign['Name']}")

# Query with WHERE clause (note: table and column names in brackets)
query = "SELECT * FROM [Campaigns] WHERE [Status] = ?"
active_campaigns = connector.extract(query, ["Active"])

# Aggregation query
summary = connector.extract("""
    SELECT 
        YEAR([CreatedDate]) as year,
        COUNT(*) as campaign_count,
        SUM([Budget]) as total_budget
    FROM [Campaigns]
    GROUP BY YEAR([CreatedDate])
    ORDER BY year DESC
""")
```

### Schema Discovery

```python
# Get complete schema for the Access database
schema_info = connector.get_schema()
for column in schema_info:
    nullable = "nullable" if column['nullable'] else "not null"
    print(f"{column['table_name']}.{column['column_name']}: {column['data_type']} {nullable}")

# List available user tables (excludes system tables)
tables = connector.get_available_tables()
print(f"Available tables: {tables}")
# Output: ['Campaigns', 'Customers', 'Orders', 'Products', ...]
```

### Data Sampling and Validation

```python
# Sample 100 rows from a table
sample = connector.get_table_sample("Campaigns", limit=100)
print(f"Sample data: {sample[:5]}")

# Get row count for data profiling
count = connector.get_table_count("Campaigns")
print(f"Total campaigns: {count}")

# Check if table exists
if "Orders" in connector.get_available_tables():
    order_count = connector.get_table_count("Orders")
    print(f"Total orders: {order_count}")
```

### Connection Lifecycle

```python
# Connector automatically manages connections
connector = create_connector(config, params)

# Use connector for multiple queries
data1 = connector.extract("SELECT * FROM [Table1]")
data2 = connector.extract("SELECT * FROM [Table2]")

# Close connection when done
connector.close()
```

## Access SQL Dialect Notes

Microsoft Access uses a slightly different SQL syntax from standard SQL. Key differences:

| Feature | Standard SQL | Access SQL |
|---------|------|-----------|
| Top N rows | `LIMIT 10` | `TOP 10` |
| Identifiers | `"table_name"` | `[table_name]` |
| Wildcard | `LIKE '%value%'` | `LIKE '*value*'` |
| String concat | `\|\|` | `&` |
| Date functions | Various | `DATE()`, `YEAR()`, `MONTH()` |

### Query Examples

```python
# Use TOP instead of LIMIT
query = "SELECT TOP 10 * FROM [Campaigns]"
top_campaigns = connector.extract(query)

# Use square brackets for identifiers with spaces
query = "SELECT [Campaign ID], [Campaign Name] FROM [Campaigns]"
data = connector.extract(query)

# Use ampersand for string concatenation
query = "SELECT [FirstName] & ' ' & [LastName] as [Full Name] FROM [Customers]"
names = connector.extract(query)
```

## Integration with Pipeline

The connector integrates with the Medallion architecture:

1. **Raw Zone**: Data extracted via connector lands in `raw/database/microsoft_access/{table_name}`
2. **Silver Zone**: Data is deduplicated and validated using latest-by-timestamp strategy
3. **Gold Zone**: Aggregated analytics tables available for reporting

### ETL Example

```python
from src.connectors.connector_factory import load_registry, create_connector

# Pipeline stage 1: Extract from Microsoft Access
registry = load_registry()
connectors = get_active_connectors(registry)
access_connector = create_connector(connectors["db_microsoft_access"], secret_value)

# Extract marketing data
campaigns = access_connector.extract("SELECT * FROM [Campaigns]")
customers = access_connector.extract("SELECT * FROM [Customers]")
orders = access_connector.extract("SELECT * FROM [Orders]")

# Stage 2: Land raw data
# (Framework automatically writes to raw zone with metadata)

# Stage 3: Transformation (Silver zone)
# Deduplicate, validate, standardize data

# Stage 4: Analytics (Gold zone)
# Create dimensional models for BI tools
```

## Performance Considerations

### Query Optimization

- Use `TOP` clause for large tables during development
- Add indexes on frequently-queried columns in Access
- Consider materializing frequently-used queries as Access views
- Avoid scanning large tables without WHERE clauses

### Large Database Exports

For large Access databases, consider batch extraction:

```python
# Fetch in batches
offset = 0
batch_size = 10000
all_data = []

# Note: Access doesn't support OFFSET, so use ID ranges
query = """
SELECT * FROM [LargeTable] 
WHERE [ID] > ? AND [ID] <= ?
ORDER BY [ID]
"""

current_id = 0
while True:
    batch = connector.extract(query, [current_id, current_id + batch_size])
    if not batch:
        break
    all_data.extend(batch)
    current_id += batch_size
```

### Connection Management

The connector maintains a single connection per instance:

```python
# Reuse same connector for multiple queries (more efficient)
connector = create_connector(config, params)
data1 = connector.extract("SELECT ...")
data2 = connector.extract("SELECT ...")  # Reuses same connection
```

## Error Handling

```python
try:
    data = connector.extract("SELECT * FROM [InvalidTable]")
except Exception as e:
    print(f"Query failed: {e}")
    # Common errors:
    # - "No such table" → table name misspelled or doesn't exist
    # - "Invalid column name" → column name incorrect (case-sensitive in some contexts)
    # - "Syntax error" → check Access SQL syntax
    # - "Database locked" → database in use by another application
```

## Monitoring and Logging

The connector logs connection events and query execution:

```python
import logging

logger = logging.getLogger("src.connectors.microsoft_access_connector")
logger.setLevel(logging.DEBUG)  # Get detailed query logs
```

Log messages include:
- Connection establishment and file validation
- Connection validation results
- Query execution and row counts
- Error details with full stack traces
- System table filtering during schema discovery

## Testing

Unit tests are provided in `tests/unit/connectors/test_microsoft_access_connector.py`:

```bash
# Run all connector tests
pytest tests/unit/connectors/ -v

# Run only Microsoft Access tests
pytest tests/unit/connectors/test_microsoft_access_connector.py -v

# Run with coverage
pytest tests/unit/connectors/test_microsoft_access_connector.py --cov=src.connectors.microsoft_access_connector
```

## Platform-Specific Installation

### Windows

```bash
# Install pyodbc
pip install pyodbc>=4.0.39

# Verify ODBC driver is installed
# Control Panel → Administrative Tools → ODBC Data Sources
# Look for "Microsoft Access Driver (*.mdb; *.accdb)"
```

### macOS

```bash
# Using Homebrew
brew install mdbtools

# or using MacPorts
sudo port install mdbtools

# Install pyodbc
pip install pyodbc>=4.0.39
```

### Linux

```bash
# Ubuntu/Debian
sudo apt-get install mdbtools
sudo apt-get install unixodbc
pip install pyodbc>=4.0.39

# CentOS/RHEL
sudo yum install mdbtools
sudo yum install unixodbc
pip install pyodbc>=4.0.39
```

## Troubleshooting

### Connection Issues

```
Error: pyodbc.OperationalError: ('IM002', '[IM002] [Microsoft][ODBC Driver Manager]')
→ ODBC driver for Microsoft Access not installed
→ Install Microsoft Access Database Engine or MDBtools
→ Verify driver name in connector_registry.yaml

Error: [HY000] [Microsoft] [ODBC Microsoft Access Driver] The Microsoft Jet database engine cannot open the file
→ Check file_path is correct and file exists
→ Verify file is not locked by another application
→ Check file permissions

Error: [HY090] [Microsoft] [ODBC Microsoft Access Driver] Invalid string or buffer length
→ Password contains special characters that need escaping
→ Try storing password as JSON-encoded string in Key Vault
```

### Query Failures

```
Error: Syntax error in query expression
→ Verify table and column names are in square brackets [name]
→ Use TOP instead of LIMIT for result limiting
→ Check for reserved keywords (use brackets to escape)

Error: [42S02] [Microsoft] [ODBC Microsoft Access Driver] Table 'table_name' does not exist
→ Verify table name spelling and case
→ Use get_available_tables() to list valid tables
→ Check table name in brackets: [Table Name]

Error: [42S22] [Microsoft] [ODBC Microsoft Access Driver] The field name is misspelled or does not exist
→ Verify column name spelling
→ Use brackets for columns with spaces: [Column Name]
→ Check schema for exact column names
```

### Database Lock Issues

```
Error: Record is deleted or the database file is corrupted
Error: Could not lock file
→ Close all other applications using the Access file
→ Verify file isn't open in Microsoft Access
→ Check write permissions on database file
→ Set readonly: true if you don't need to modify data
```

## Migration Guide

### From Snowflake to Microsoft Access

1. Update `connector_registry.yaml`:
   ```yaml
   databases:
     active: microsoft_access
   ```

2. Update Key Vault secrets with Access file path

3. Update SQL queries to use Access syntax:
   - Replace `LIMIT` with `TOP`
   - Use `[brackets]` for identifiers instead of quotes
   - Replace `||` concatenation with `&`

4. Run validation:
   ```python
   if connector.validate_connection():
       print("Ready to switch!")
   ```

### From Legacy .mdb to Modern .accdb

1. Update file_path in Key Vault to point to .accdb file
2. Update driver in connector_registry.yaml to use accdb driver
3. Run validation to ensure compatibility
4. No code changes needed — driver selection is automatic

## Limitations

- **No transactions support**: Access connector operates in autocommit mode
- **No stored procedures**: Can only execute SQL queries
- **Single connection per instance**: Multiple concurrent queries not supported
- **Password-protected databases**: Passwords must be stored in Key Vault (never hardcoded)

## License

Part of the MarTech Fabric Pipeline project.
