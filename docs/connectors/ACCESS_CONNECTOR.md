# Microsoft Access Database Connector

## Overview

The `AccessConnector` provides integration with Microsoft Access databases (`.mdb` and `.accdb` files) for the MarTech Fabric Pipeline. It implements the `DatabaseConnector` interface for seamless integration with the Medallion Architecture data ingestion framework.

## Features

- **ODBC-based Connection**: Uses Python's `pyodbc` library to connect to Access databases via ODBC drivers
- **Schema Introspection**: Automatic discovery of tables and columns from Access databases
- **SQL Query Execution**: Execute arbitrary SQL queries against Access data sources
- **Connection Management**: Automatic connection pooling and lifecycle management
- **Error Handling**: Comprehensive error handling for ODBC operations and file validation
- **Metadata Generation**: Automatic metadata tagging for lineage tracking

## Prerequisites

### System Requirements

1. **ODBC Driver for Access**: You must have the Microsoft Access ODBC driver installed
   - **Windows**: Comes with Microsoft Office or can be installed separately
   - **Linux/Mac**: Use `mdb-export` or `mdbtools`, or install `unixODBC` with Access drivers

2. **Python Dependencies**: `pyodbc>=5.0` (automatically installed with this package)

### Installation

```bash
# Install dependencies
pip install pyodbc>=5.0

# Verify ODBC driver is available
python3 << 'EOF'
import pyodbc
print("Available ODBC drivers:")
for driver in pyodbc.drivers():
    if "Access" in driver:
        print(f"  - {driver}")
EOF
```

## Configuration

### Registry Configuration

Add Access database to `config/connectors/connector_registry.yaml`:

```yaml
databases:
  active: access  # Switch to access database
  connectors:
    access:
      driver: "Microsoft Access Driver (*.mdb, *.accdb)"
      file_path: "/mnt/data/sales.accdb"
      keyvault_secret: "access-file-path"
      password: null  # Optional: for password-protected databases
      test_query: "SELECT 1"
      landing_path: "raw/database/access"
```

### Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `driver` | string | No | ODBC driver name (default: "Microsoft Access Driver (*.mdb, *.accdb)") |
| `file_path` | string | Yes | Full path to the `.mdb` or `.accdb` file |
| `keyvault_secret` | string | Yes | Azure Key Vault secret name for credentials |
| `password` | string | No | Password for protected Access databases |
| `test_query` | string | No | Query to validate connection (default: "SELECT 1") |
| `landing_path` | string | No | Raw zone path for Medallion Architecture |

## Usage

### Basic Connection

```python
from src.connectors.connector_factory import create_connector
from src.connectors.base_connector import ConnectorConfig

# Define configuration
config = ConnectorConfig(
    name="access",
    connector_type="database",
    keyvault_secret="access-file-path",
    landing_path="raw/database/access",
    extra={
        "driver": "Microsoft Access Driver (*.mdb, *.accdb)",
        "file_path": "/mnt/data/sales.accdb",
    }
)

# Create connector instance (secret would be resolved from Key Vault)
connector = create_connector(config, secret_value="")

# Validate connection
if connector.validate_connection():
    print("✓ Connected to Access database")
else:
    print("✗ Connection failed")
```

### Query Execution

```python
# Execute SQL query
results = connector.extract("SELECT * FROM Orders WHERE Amount > 100")

# Process results
for row in results:
    print(f"Order ID: {row['OrderID']}, Amount: {row['Amount']}")
```

### Schema Introspection

```python
# Get complete schema
schema = connector.get_schema()

# Get table list
tables = connector.get_tables()
print(f"Available tables: {tables}")

# Access schema details
for table_name, columns in schema.items():
    print(f"\nTable: {table_name}")
    for col in columns:
        print(f"  - {col['column_name']}: {col['data_type']}")
```

### Metadata

```python
# Get automatic metadata
metadata = connector.get_metadata()
# Returns:
# {
#     "_source_system": "access",
#     "_connector_type": "database",
#     "_landing_path": "raw/database/access"
# }
```

## Data Flow

```
Access Database (.accdb)
    ↓
ODBC Driver
    ↓
pyodbc Connection
    ↓
SQL Query Execution
    ↓
Result Transformation (rows → list of dicts)
    ↓
Landing Zone (Medallion: Raw)
```

## Error Handling

The connector handles common errors gracefully:

### File Not Found
```python
try:
    connector = AccessConnector(config, connection_string="")
    connector.validate_connection()
except Exception as e:
    print(f"Access file not found: {e}")
```

### ODBC Connection Error
```python
try:
    connector = AccessConnector(config, connection_string="")
    connector.validate_connection()
except pyodbc.Error as e:
    print(f"ODBC connection failed: {e}")
```

### Query Execution Error
```python
try:
    results = connector.extract("SELECT * FROM InvalidTable")
except pyodbc.Error as e:
    print(f"Query failed: {e}")
```

## Supported Data Types

Common Access data types mapped to Python:

| Access Type | Python Type | Notes |
|------------|------------|-------|
| COUNTER | int | Auto-increment primary key |
| SHORT INTEGER | int | 16-bit integer |
| LONG INTEGER | int | 32-bit integer |
| SINGLE | float | Single precision |
| DOUBLE | float | Double precision |
| CURRENCY | Decimal | Monetary value |
| DATE/TIME | datetime | Date and time |
| TEXT | str | Variable-length text |
| MEMO | str | Large text |
| LOGICAL | bool | Yes/No |
| OLE OBJECT | bytes | Binary data |

## Performance Considerations

### Connection Pooling
The connector automatically reuses connections across multiple queries:

```python
# First query: creates connection
connector.extract("SELECT * FROM Orders")

# Second query: reuses existing connection
connector.extract("SELECT * FROM Customers")
```

### Query Optimization
For large result sets, use WHERE clauses to filter data:

```python
# Good: filtered query
connector.extract("SELECT * FROM Orders WHERE Year(Date) = 2024")

# Avoid: unfiltered large table
connector.extract("SELECT * FROM Orders")
```

### Batch Operations
Use `UNION` or multiple queries for related data extraction:

```python
query = """
    SELECT 'Orders' as source, COUNT(*) as count FROM Orders
    UNION
    SELECT 'Customers' as source, COUNT(*) as count FROM Customers
"""
results = connector.extract(query)
```

## Security

### Credentials Management

Access credentials are managed through Azure Key Vault:

```python
# Key Vault secret contains the file path or connection string
# Do not commit credentials to version control
```

### Connection String Format

ODBC connection strings are constructed securely:

```python
# Basic (no password)
"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=/path/to/file.accdb"

# With password
"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=/path/to/file.accdb;PWD=password"
```

## Testing

### Unit Tests

Comprehensive unit tests are provided in `tests/unit/connectors/test_access_connector.py`:

```bash
pytest tests/unit/connectors/test_access_connector.py -v
```

### Test Coverage

- ✓ Connection validation
- ✓ Schema introspection
- ✓ Query execution
- ✓ Error handling
- ✓ Metadata generation
- ✓ Connection lifecycle management
- ✓ Factory registration

### Integration Tests

To run integration tests against a real Access database:

```bash
# Set environment variables
export ACCESS_DB_PATH="/path/to/test.accdb"

# Run integration tests
pytest tests/integration/connectors/test_access_connector.py -v -m integration
```

## Troubleshooting

### ODBC Driver Not Found

**Error**: `pyodbc.InterfaceError: ('IM002', '[IM002] [unixODBC][Driver Manager]Data source name not found...`

**Solution**:
1. Verify ODBC driver is installed: `odbcinst -j`
2. Check driver is registered in `/etc/odbcinst.ini`
3. Install Microsoft Access driver for your OS

### File Not Found

**Error**: `FileNotFoundError: Access file not found: /path/to/missing.accdb`

**Solution**:
1. Verify file path in configuration
2. Check file permissions (must be readable)
3. Ensure full path is provided (not relative)

### Connection Failed

**Error**: `pyodbc.DatabaseError: ('HY000', '[HY000] [Microsoft][ODBC Access Driver] ...`

**Solution**:
1. Verify file is valid Access database
2. Check if file is open in another application (lock it)
3. Validate ODBC connection string syntax
4. For protected databases, verify password in configuration

## Examples

### Extract All Tables

```python
def extract_all_tables(connector):
    """Extract data from all tables in Access database."""
    tables = connector.get_tables()
    results = {}
    
    for table in tables:
        query = f"SELECT * FROM [{table}]"
        results[table] = connector.extract(query)
    
    return results
```

### Export to Parquet

```python
import pandas as pd

def export_to_parquet(connector, table_name, output_path):
    """Export Access table to Parquet format."""
    data = connector.extract(f"SELECT * FROM [{table_name}]")
    df = pd.DataFrame(data)
    df.to_parquet(output_path, compression='snappy')
```

### Data Quality Check

```python
def validate_table_structure(connector, table_name, expected_columns):
    """Validate table has expected columns."""
    schema = connector.get_schema()
    if table_name not in schema:
        return False, f"Table {table_name} not found"
    
    actual_columns = {col['column_name'] for col in schema[table_name]}
    expected_set = set(expected_columns)
    
    if actual_columns != expected_set:
        return False, f"Column mismatch. Expected: {expected_set}, Got: {actual_columns}"
    
    return True, "✓ Table structure valid"
```

## Related Documentation

- [Base Connector Interface](../base_connector.py)
- [Connector Factory](../connector_factory.py)
- [Connector Registry](../../config/connectors/connector_registry.yaml)
- [Medallion Architecture](../../docs/medallion_architecture.md)
- [Data Ingestion Pipeline](../../docs/pipeline.md)

## Support

For issues or questions:
1. Check existing issues in the repository
2. Review test cases for usage examples
3. Consult the troubleshooting section above
4. Open a new issue with details about your setup

## License

This connector is part of the MarTech Fabric Pipeline project.
