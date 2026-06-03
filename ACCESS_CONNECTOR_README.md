# Microsoft Access Marketing Data Connector

## Overview

The **Microsoft Access Connector** enables seamless integration of `.mdb` and `.accdb` marketing database files into the MarTech Fabric Pipeline. It provides ODBC-based connectivity, schema discovery, and parameterized query execution for Microsoft Access databases.

## Features

✅ **ODBC Connection Support** - Connects to `.mdb` (Access 2003) and `.accdb` (Access 2007+) files  
✅ **Flexible Configuration** - Supports both file paths and full ODBC connection strings  
✅ **Schema Discovery** - Automatically lists tables and columns with type information  
✅ **Parameterized Queries** - Safe SQL execution with parameter binding  
✅ **Error Handling** - Comprehensive error handling with informative messages  
✅ **Read-Only Mode** - Default read-only access for data safety  
✅ **Password Support** - Optional database password authentication  
✅ **System Table Filtering** - Excludes `MSys*` system tables by default  

---

## Installation

### 1. Install Python Dependencies

```bash
pip install -e ".[access]"
```

This installs the required `pyodbc` package.

### 2. Install ODBC Driver

**Windows:**  
The Microsoft Access ODBC driver is typically pre-installed. Verify in:
- Control Panel → Administrative Tools → ODBC Data Sources (64-bit)

**Linux/Mac:**  
Use MDB Tools or install commercial ODBC drivers:
```bash
# Ubuntu/Debian
sudo apt-get install mdbtools unixodbc

# macOS
brew install mdbtools unixodbc
```

---

## Configuration

### Connector Registry Setup

Edit `config/connectors/connector_registry.yaml`:

```yaml
databases:
  active: access  # Switch to Access connector
  connectors:
    access:
      driver: "Microsoft Access Driver (*.mdb, *.accdb)"
      arm_linked_service_type: "Odbc"
      keyvault_secret: "access-db-path-or-connection-string"
      landing_path: "raw/database/access"
      readonly: true
      # Optional: database_password secret for protected databases
```

### Azure Key Vault Secret

Store your Access database path or connection string in Azure Key Vault:

**Option 1: Simple File Path**
```
C:\MarketingData\CRM.accdb
```

**Option 2: Full ODBC Connection String**
```
Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\MarketingData\CRM.accdb;ReadOnly=1;
```

**Option 3: Network Path**
```
\\server\share\MarketingData\CRM.mdb
```

---

## Usage Examples

### Basic Connection and Query

```python
from src.connectors.access_connector import MicrosoftAccessConnector
from src.connectors.base_connector import ConnectorConfig

# Configure the connector
config = ConnectorConfig(
    name="access",
    connector_type="database",
    keyvault_secret="access-db-path",
    landing_path="raw/database/access",
    extra={
        "readonly": True,
        "odbc_driver": "Microsoft Access Driver (*.mdb, *.accdb)"
    }
)

# Create connector instance
db_path = r"C:\MarketingData\CRM.accdb"
connector = MicrosoftAccessConnector(config, db_path)

# Execute a query
results = connector.execute("SELECT * FROM Customers WHERE Status = ?", {"Status": "Active"})

for row in results:
    print(f"Customer: {row['CustomerName']}, Email: {row['Email']}")
```

### Schema Discovery

```python
# Get database schema
schema = connector.get_schema()

for table in schema["tables"]:
    print(f"\nTable: {table['table_name']}")
    for column in table["columns"]:
        print(f"  - {column['column_name']} ({column['data_type']})")
```

### Password-Protected Database

```python
config = ConnectorConfig(
    name="access",
    connector_type="database",
    keyvault_secret="access-db-path",
    landing_path="raw/database/access",
    extra={
        "readonly": True,
        "database_password": "MySecurePassword123"
    }
)

connector = MicrosoftAccessConnector(config, r"C:\MarketingData\SecureCRM.accdb")
```

### Context Manager Usage

```python
with MicrosoftAccessConnector(config, db_path) as conn:
    # Query customer data
    customers = conn.execute("SELECT * FROM Customers WHERE Region = ?", {"Region": "EMEA"})
    
    # Query campaign data
    campaigns = conn.execute("SELECT * FROM Campaigns WHERE StartDate >= ?", {"StartDate": "2024-01-01"})
    
    # Process results
    print(f"Found {len(customers)} customers and {len(campaigns)} campaigns")
```

### Integration with Pipeline

```python
from src.connectors.connector_factory import load_registry, get_active_connectors, create_connector
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

# Load connector registry
registry = load_registry()
active_connectors = get_active_connectors(registry)

# Get Access connector config
access_config = active_connectors.get("db_access")

# Retrieve secret from Key Vault
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url="https://your-keyvault.vault.azure.net/", credential=credential)
db_path = secret_client.get_secret(access_config.keyvault_secret).value

# Create connector
access_connector = create_connector(access_config, db_path)

# Extract marketing data
marketing_data = access_connector.execute("""
    SELECT 
        c.CustomerID,
        c.CustomerName,
        c.Email,
        o.OrderDate,
        o.OrderTotal,
        p.ProductName
    FROM Customers c
    INNER JOIN Orders o ON c.CustomerID = o.CustomerID
    INNER JOIN OrderDetails od ON o.OrderID = od.OrderID
    INNER JOIN Products p ON od.ProductID = p.ProductID
    WHERE o.OrderDate >= ?
""", {"OrderDate": "2024-01-01"})
```

---

## Common Marketing Data Queries

### Customer Segmentation

```python
# Active customers by region
results = connector.execute("""
    SELECT Region, COUNT(*) as CustomerCount, AVG(LifetimeValue) as AvgLTV
    FROM Customers
    WHERE Status = 'Active'
    GROUP BY Region
    ORDER BY CustomerCount DESC
""")
```

### Campaign Performance

```python
# Campaign ROI analysis
results = connector.execute("""
    SELECT 
        c.CampaignName,
        c.Budget,
        SUM(o.OrderTotal) as Revenue,
        COUNT(DISTINCT o.CustomerID) as ConvertedCustomers
    FROM Campaigns c
    LEFT JOIN Orders o ON o.CampaignID = c.CampaignID
    WHERE c.StartDate >= ?
    GROUP BY c.CampaignID, c.CampaignName, c.Budget
""", {"StartDate": "2024-01-01"})
```

### Lead Scoring

```python
# Leads with high engagement scores
results = connector.execute("""
    SELECT 
        l.LeadID,
        l.Email,
        l.Company,
        l.EngagementScore,
        l.LastActivityDate
    FROM Leads l
    WHERE l.EngagementScore >= ?
    AND l.Status = 'Open'
    ORDER BY l.EngagementScore DESC
""", {"EngagementScore": 75})
```

---

## Error Handling

```python
from src.connectors.access_connector import MicrosoftAccessConnector

try:
    connector = MicrosoftAccessConnector(config, db_path)
    results = connector.execute("SELECT * FROM NonExistentTable")
except ModuleNotFoundError as e:
    print(f"Missing dependency: {e}")
    print("Install with: pip install -e '.[access]'")
except Exception as e:
    print(f"Connection or query error: {e}")
```

---

## Configuration Options

### `ConnectorConfig.extra` Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `odbc_driver` | str | `"Microsoft Access Driver (*.mdb, *.accdb)"` | ODBC driver name |
| `readonly` | bool | `True` | Enable read-only mode |
| `database_password` | str | `None` | Database password for protected files |
| `include_system_tables` | bool | `False` | Include `MSys*` tables in schema |

---

## Troubleshooting

### Issue: `pyodbc` Module Not Found

**Solution:**
```bash
pip install -e ".[access]"
```

### Issue: Driver Not Found on Linux

**Solution:** Install MDB Tools ODBC driver:
```bash
sudo apt-get install mdbtools mdbtools-dev unixodbc unixodbc-dev
```

### Issue: Cannot Open `.accdb` File

**Error:** `[IM002] [unixODBC][Driver Manager]Data source name not found`

**Solution:** Use UCanAccess JDBC driver (cross-platform) or Windows environment with native driver.

### Issue: Database is Locked

**Solution:** Ensure no other process has the `.ldb` lock file open. Close Access application if running.

---

## Performance Considerations

1. **Index Usage** - Ensure Access tables have proper indexes on frequently queried columns
2. **Query Optimization** - Use parameterized queries and avoid `SELECT *` for large tables
3. **Batch Processing** - For large datasets, implement pagination with `LIMIT` and `OFFSET`
4. **Network Access** - For network paths (UNC), consider caching or local replication

---

## Security Best Practices

✅ Store database paths in Azure Key Vault, never in code  
✅ Use read-only mode unless write access is explicitly required  
✅ Implement row-level security in Access if sharing credentials  
✅ Rotate database passwords regularly for protected databases  
✅ Audit access logs in production environments  

---

## API Reference

### `MicrosoftAccessConnector`

**Constructor:**
```python
MicrosoftAccessConnector(config: ConnectorConfig, connection_string: str)
```

**Methods:**

- `execute(query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]`  
  Execute SQL query with optional parameters. Returns list of row dictionaries.

- `get_schema() -> Dict[str, Any]`  
  Retrieve database schema (tables and columns).

- `_connect() -> pyodbc.Connection`  
  Establish ODBC connection (internal).

- `_build_connection_string(value: str, config: ConnectorConfig) -> str`  
  Normalize file path or ODBC string (internal).

---

## Testing

Run unit tests:
```bash
pytest tests/unit/connectors/test_connector_factory.py -v
```

---

## Support

For issues or questions:
- **GitHub Issues:** [Project Repository]
- **Documentation:** [Internal Wiki]
- **Team Contact:** martech-pipeline-team@company.com

---

## License

Internal Use Only - Proprietary
