# Marketing Data Connector Architecture

## Executive Summary

This MarTech Fabric Pipeline implements a **extensible connector framework** for ingesting marketing data from multiple sources. Two production-ready connectors are included:

1. **Microsoft Access Connector** - ODBC-based connector for `.mdb` and `.accdb` database files
2. **HubSpot Connector** - REST API connector for CRM data extraction

## Architecture Overview

### Connector Hierarchy

```
BaseConnector (ABC)
├── DatabaseConnector
│   └── MicrosoftAccessConnector
├── APIConnector
│   ├── HubSpotConnector
│   ├── ShopifyConnector (placeholder)
│   ├── MagentoConnector (placeholder)
│   └── WooCommerceConnector (placeholder)
└── FileConnector
    ├── SFTPConnector (placeholder)
    └── AzureBlobConnector (placeholder)
```

### Key Components

#### 1. **Base Connector Interface** (`base_connector.py`)

Abstract base classes define the contract for all connectors:

```python
class BaseConnector(ABC):
    @abstractmethod
    def validate_connection() -> bool
    @abstractmethod
    def extract(query_or_endpoint: str, params: Dict) -> Any
    @abstractmethod
    def get_schema() -> Dict[str, Any]
```

#### 2. **Microsoft Access Connector** (`access_connector.py`)

**Type:** Database Connector  
**Protocol:** ODBC  
**Supported Formats:** `.mdb` (Access 2003), `.accdb` (Access 2007+)

**Features:**
- Flexible connection string handling (file path or full ODBC string)
- Parameterized query execution with parameter binding
- Automatic schema discovery from INFORMATION_SCHEMA
- Read-only mode by default for data safety
- Optional password authentication for protected databases
- System table filtering (excludes `MSys*` tables)

**Key Methods:**
```python
MicrosoftAccessConnector(config, connection_string: str)
  .execute(query: str, params: Dict) -> List[Dict]
  .get_schema() -> Dict[str, Any]
  .validate_connection() -> bool
```

#### 3. **HubSpot Connector** (`hubspot_connector.py`)

**Type:** API Connector  
**Protocol:** REST API v3  
**Rate Limit:** 10 requests/second (configurable)

**Features:**
- Automatic pagination handling for large datasets
- Built-in rate limit enforcement with warning logs
- Comprehensive error handling (401, 429, timeouts)
- Connection validation with account info check
- Pre-configured schema for common HubSpot objects:
  - Contacts
  - Companies
  - Deals
  - Tickets

**Key Methods:**
```python
HubSpotConnector(config, api_key: str)
  .extract(endpoint: str, params: Dict) -> List[Dict]
  .get_schema() -> Dict[str, Any]
  .validate_connection() -> bool
```

#### 4. **Connector Factory** (`connector_factory.py`)

Registry-based factory for dynamic connector instantiation:

```python
# Load configuration
registry = load_registry("config/connectors/connector_registry.yaml")
active_connectors = get_active_connectors(registry)

# Create connector
connector = create_connector(config, secret_value)
```

**Supported Types in CONNECTOR_CLASS_MAP:**
- `access` → MicrosoftAccessConnector
- `hubspot` → HubSpotConnector
- `postgres`, `sqlserver`, `mysql` → DatabaseConnector (generic)
- `sftp`, `azure_blob` → FileConnector (generic)
- `shopify`, `magento`, `woocommerce` → APIConnector (generic)

## Data Flow

```
Configuration (YAML)
  ↓
ConnectorFactory.load_registry()
  ↓
Connector Instance Creation
  ↓
ConnectorConfig + Secret (from Key Vault)
  ↓
{MicrosoftAccessConnector | HubSpotConnector | ...}
  ↓
extract(query_or_endpoint, params)
  ↓
Raw Data → Landing Zone (raw/)
```

## Configuration System

### Connector Registry (`config/connectors/connector_registry.yaml`)

```yaml
databases:
  active: access  # Switch active database connector
  connectors:
    access:
      driver: "Microsoft Access Driver (*.mdb, *.accdb)"
      keyvault_secret: "access-db-path-or-connection-string"
      landing_path: "raw/database/access"
      readonly: true
      
crm:
  active: hubspot
  connectors:
    hubspot:
      base_url: "https://api.hubapi.com"
      keyvault_secret: "hubspot-api-key"
      landing_path: "raw/crm/hubspot"
      rate_limit: 10
```

### ConnectorConfig Dataclass

```python
@dataclass
class ConnectorConfig:
    name: str                              # "access", "hubspot"
    connector_type: str                    # "database", "api", "file"
    keyvault_secret: str                   # Azure Key Vault secret name
    landing_path: str                      # Target path in data lake
    extra: Dict[str, Any] = field(...)     # Connector-specific settings
```

## Usage Patterns

### Direct Instantiation (Development)

```python
from src.connectors.access_connector import MicrosoftAccessConnector
from src.connectors.base_connector import ConnectorConfig

config = ConnectorConfig(
    name="access",
    connector_type="database",
    keyvault_secret="access-db-path",
    landing_path="raw/database/access",
    extra={"readonly": True}
)

db_path = r"C:\MarketingData\CRM.accdb"
connector = MicrosoftAccessConnector(config, db_path)

results = connector.extract(
    "SELECT * FROM Customers WHERE Status = ?",
    {"Status": "Active"}
)
```

### Factory Pattern (Production)

```python
from src.connectors.connector_factory import (
    load_registry, get_active_connectors, create_connector
)
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

# Load configuration
registry = load_registry()
active_connectors = get_active_connectors(registry)

# Get Access connector config
access_config = active_connectors.get("db_access")

# Retrieve secret from Key Vault
credential = DefaultAzureCredential()
secret_client = SecretClient(
    vault_url="https://your-keyvault.vault.azure.net/",
    credential=credential
)
db_path = secret_client.get_secret(access_config.keyvault_secret).value

# Create connector
access_connector = create_connector(access_config, db_path)

# Extract data
marketing_data = access_connector.extract(
    "SELECT * FROM Campaigns WHERE StartDate >= ?",
    {"StartDate": "2024-01-01"}
)
```

## Extension Guide

### Adding a New Connector

1. **Create connector file** in `src/connectors/`

```python
from src.connectors.base_connector import APIConnector, ConnectorConfig

class NewServiceConnector(APIConnector):
    def validate_connection(self) -> bool:
        # Implement connection validation
        pass
    
    def extract(self, endpoint: str, params: Dict) -> List[Dict]:
        # Implement data extraction logic
        pass
    
    def get_schema(self) -> Dict[str, Any]:
        # Return schema definition
        pass
```

2. **Register in factory** (`connector_factory.py`)

```python
CONNECTOR_CLASS_MAP: Dict[str, type] = {
    "newservice": NewServiceConnector,
    # ... existing entries
}
```

3. **Add to registry config** (`connector_registry.yaml`)

```yaml
new_services:
  active: newservice
  connectors:
    newservice:
      keyvault_secret: "newservice-api-key"
      landing_path: "raw/newservice"
```

4. **Write unit tests** in `tests/unit/connectors/`

5. **Document usage** in README

## Connector Lifecycle

### Initialization Phase

1. Load ConnectorConfig from registry
2. Retrieve secret from Azure Key Vault
3. Instantiate connector via factory
4. Connector stores config and credentials

### Validation Phase

1. Call `validate_connection()` before extraction
2. Test database/API connectivity
3. Return boolean status
4. Log connection events

### Extraction Phase

1. Call `extract(query_or_endpoint, params)`
2. Execute query/API call
3. Transform response to list of dicts
4. Return results

### Schema Discovery Phase

1. Call `get_schema()` for table/object information
2. Return metadata for validation
3. Support data quality checks

## Error Handling Strategy

### Database Connectors (Access)

- `ModuleNotFoundError` - Missing pyodbc dependency
- `pyodbc.OperationalError` - Connection failures
- `pyodbc.ProgrammingError` - Invalid SQL syntax
- Custom logger warns about locked files

### API Connectors (HubSpot)

- `HTTPError 401` - Invalid/expired credentials
- `HTTPError 429` - Rate limit exceeded
- `Timeout` - Request timeout (30s default)
- Return `None` on error, caller must handle

## Security Considerations

✅ **Secrets Management:**
- All credentials stored in Azure Key Vault
- Never hardcoded in code or config files
- Runtime retrieval via DefaultAzureCredential

✅ **Access Control:**
- Database connectors default to read-only mode
- API keys passed securely via headers
- Connection strings masked in logs

✅ **Data Validation:**
- Parameterized queries prevent SQL injection
- Schema validation at extraction time
- Row-level security in source systems

## Testing Strategy

### Unit Tests (`tests/unit/connectors/`)

**Test Coverage:**
- Factory instantiation with mocked configs
- HubSpot pagination logic (3-page scenarios)
- Error handling (auth failures, timeouts)
- Schema introspection

**Running Tests:**
```bash
pytest tests/unit/connectors/ -v
pytest tests/unit/connectors/test_hubspot_connector.py -v
pytest tests/unit/connectors/test_connector_factory.py -v
```

### Integration Tests (Optional)

- Real database/API connections
- Marked with `@pytest.mark.integration`
- Skip by default (require credentials)

## Performance Characteristics

| Connector | Throughput | Latency | Pagination | Notes |
|-----------|-----------|---------|-----------|-------|
| **Access** | 1K-10K rows/s | <100ms | Via LIMIT/OFFSET | Local file I/O |
| **HubSpot** | 100-500 rows/s | 200-500ms | Auto (100/page) | Rate limited to 10 req/s |

**Optimization Tips:**
- Index frequently queried columns in Access
- Use parameterized queries to leverage plan cache
- Batch API calls for HubSpot (combine endpoints)
- Implement local caching for reference data

## Dependencies

### Core Dependencies
- `pyyaml` - Config file parsing
- `pydantic` - Data validation
- `requests` - HTTP client (HubSpot)
- `azure-*` - Azure integration

### Optional Dependencies
- `pyodbc[>=5.1]` - Access database connectivity (install via `pip install -e ".[access]"`)
- `requests[>=2.31]` - HubSpot API calls (install via `pip install -e ".[hubspot]"`)

### Development Dependencies
- `pytest` - Testing framework
- `pytest-mock` - Mocking utilities
- `pytest-cov` - Code coverage

## Future Enhancements

- [ ] **Salesforce Connector** - Extend APIConnector for SOQL queries
- [ ] **Google Sheets Connector** - Extend FileConnector for Google Drive access
- [ ] **Webhook Connectors** - Event-driven data ingestion
- [ ] **Change Data Capture** - Track incremental changes in source systems
- [ ] **Retry Logic** - Exponential backoff for transient failures
- [ ] **Connection Pooling** - Reuse database connections across jobs
- [ ] **Async Operations** - Non-blocking I/O for high-throughput scenarios

## Support & Documentation

- **README Files:**
  - `ACCESS_CONNECTOR_README.md` - Full Microsoft Access guide
  - `HUBSPOT_CONNECTOR_README.md` - Full HubSpot guide
  - `CONNECTOR_ARCHITECTURE.md` - This document

- **Examples:**
  - `examples/access_connector_example.py` - 5 usage patterns
  - `examples/hubspot_connector_example.py` - API usage patterns

- **Code Documentation:**
  - Inline docstrings in all connector classes
  - Type hints on all public methods
  - Configuration dataclass definitions

## License

Internal Use Only - Proprietary
