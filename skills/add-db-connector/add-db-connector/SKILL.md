---
name: add-db-connector
description: >
  Add or switch a database connector in the MarTech Fabric Pipeline. Supports
  PostgreSQL, SQL Server, MySQL, Oracle, and any JDBC/ODBC-compatible database.
  Use this skill when the client needs to change the source database engine,
  add a new on-premises database, or configure multiple database sources.
  Triggers on: "switch to SQL Server", "change database to MySQL", "add Oracle
  connector", "connect to on-prem database", "add Postgres source", "swap DB
  engine", "add JDBC connector", or any request involving database source changes.
  This is the simplest connector change — often just a one-line config update.
---

# Add / Switch Database Connector

You are modifying the MarTech Fabric Pipeline's database connector. The pipeline
is designed so that swapping databases is a minimal-change operation — the same
interface works for Postgres, SQL Server, MySQL, and more.

## What You're Building

A database connector uses JDBC/ODBC to pull tables from an on-premises or
cloud-hosted relational database into the raw zone. The pipeline's factory
pattern means the ARM template, ADF linked services, and Python code all
adapt automatically when you change the active database type.

## Quick Switch (One-Line Change)

If you're switching between already-configured databases (postgres, sqlserver,
mysql), it's a single line change:

```yaml
# config/connectors/connector_registry.yaml
databases:
  active: sqlserver   # Changed from "postgres" — that's it!
```

Then update the ARM parameter:

```json
// infra/arm-templates/parameters/{env}.parameters.json
"dbConnectorType": { "value": "sqlserver" }
```

And store the connection string in Key Vault:

```bash
az keyvault secret set \
  --vault-name "martech-{env}-kv" \
  --name "sqlserver-connection-string" \
  --value "Server=myserver;Database=mydb;User Id=admin;Password=xxx;"
```

Run tests to verify: `pytest tests/unit/connectors/ -v`

That's it for a switch. The ARM template's linked service type, the ADF pipeline,
and the Python connector all automatically use the correct driver and connection
pattern because of the factory pattern.

## Adding a New Database Engine

If the target database isn't already in the registry (e.g., Oracle, Snowflake,
Redshift), follow the full process:

### 1. Add to connector_registry.yaml

```yaml
databases:
  active: oracle
  connectors:
    oracle:
      driver: "oracle.jdbc.OracleDriver"
      arm_linked_service_type: "Oracle"
      keyvault_secret: "oracle-connection-string"
      default_port: 1521
      jdbc_template: "jdbc:oracle:thin:@{host}:{port}:{database}"
      test_query: "SELECT 1 FROM DUAL"
```

Required fields:

- **driver** — JDBC driver class name
- **arm_linked_service_type** — ADF linked service type (check Azure docs)
- **keyvault_secret** — Key Vault secret name for connection string
- **default_port** — standard port for this engine
- **jdbc_template** — JDBC URL pattern with `{host}`, `{port}`, `{database}` placeholders
- **test_query** — simple query to verify connectivity

### 2. Update ARM Template Variables

In `infra/arm-templates/modules/data-factory.json`, add to the lookup maps:

```json
"dbLinkedServiceType": {
  "postgres": "AzurePostgreSql",
  "sqlserver": "SqlServer",
  "mysql": "MySql",
  "oracle": "Oracle"
},
"dbConnectionStringSecret": {
  "postgres": "postgres-connection-string",
  "sqlserver": "sqlserver-connection-string",
  "mysql": "mysql-connection-string",
  "oracle": "oracle-connection-string"
}
```

Also add `"oracle"` to the `allowedValues` for `dbConnectorType` in both
`data-factory.json` and `main.json`.

### 3. Create Connector Class (If Needed)

For standard JDBC databases, the base `DatabaseConnector` class works as-is.
Only create a subclass if the database has non-standard behavior:

```python
# src/connectors/oracle_connector.py (only if needed)
from src.connectors.base_connector import DatabaseConnector

class OracleConnector(DatabaseConnector):
    def _execute(self, query, params=None):
        """Oracle-specific execution with cx_Oracle driver."""
        import cx_Oracle
        with cx_Oracle.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or {})
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
```

Register in factory:
```python
CONNECTOR_CLASS_MAP["oracle"] = OracleConnector
```

### 4. Write Tests

```python
class TestOracleConnector:
    def test_test_query_is_oracle_specific(self):
        registry = load_registry()
        oracle = registry["databases"]["connectors"]["oracle"]
        assert oracle["test_query"] == "SELECT 1 FROM DUAL"

    def test_jdbc_template_has_oracle_format(self):
        registry = load_registry()
        oracle = registry["databases"]["connectors"]["oracle"]
        assert "thin:@" in oracle["jdbc_template"]

    def test_switching_to_oracle_updates_active(self):
        registry = load_registry()
        registry["databases"]["active"] = "oracle"
        active = get_active_connectors(registry)
        assert "db_oracle" in active
```

### 5. Adding Multiple Database Sources

If the client needs data from multiple databases simultaneously (e.g., Postgres
for CRM + SQL Server for ERP), extend the registry to support a list:

```yaml
databases:
  active:
    - postgres     # CRM database
    - sqlserver    # ERP database
  connectors:
    postgres:
      # ... existing config
      source_label: "crm_db"
    sqlserver:
      # ... existing config
      source_label: "erp_db"
```

Update `get_active_connectors()` to handle active as a list:

```python
active_dbs = db_section.get("active", "postgres")
if isinstance(active_dbs, str):
    active_dbs = [active_dbs]
for db_name in active_dbs:
    # ... create ConnectorConfig for each
```

## Completion Checklist

- [ ] Registry entry with all required fields
- [ ] ARM template variables updated (linked service type, secret name)
- [ ] `allowedValues` updated in ARM parameters
- [ ] Connector class (if non-standard behavior needed)
- [ ] Registered in `CONNECTOR_CLASS_MAP`
- [ ] Unit tests pass
- [ ] Key Vault secret stored
- [ ] Connection validated with `test_query`
