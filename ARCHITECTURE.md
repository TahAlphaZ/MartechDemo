Architecture - SQL-driven Gold Transformations

Overview
--------
This repository implements a Medallion architecture (Raw/Bronze → Silver → Gold) for
a MarTech retail data platform using Azure Fabric and PySpark. The new SQL-driven
transformation component allows analysts to author SQL stored in Azure Blob Storage
and have it executed against registered silver Delta tables to produce gold outputs.

Components
----------
- Raw → Silver (src/transformations/raw_to_silver/transform.py): cleansing, deduplication.
- Silver → Gold (src/transformations/silver_to_gold/transform.py): programmatic builders for
  gold models.
- SQL-driven Gold (src/transformations/sql_to_gold.py): executes SQL fetched from Blob
  (e.g. blob named 'Test-use-case') to produce gold tables. Useful for analyst-driven
  ad-hoc models and porting legacy SQL logic.

Security
--------
- Blob access should be performed with managed identities / DefaultAzureCredential or
  via connection strings stored in Key Vault. This module does not perform secret
  resolution; callers must supply credentials.

Usage
-----
1. Store SQL statements in a blob container. Name the blob clearly (e.g. Test-use-case.sql).
2. From an Azure Notebook or job, instantiate SQLDrivenTransformer with a Spark session.
3. Fetch the SQL using fetch_sql_from_blob and register needed silver tables using
   register_silver_tables. Then call run_sql_to_gold to produce the gold Delta output.

Limitations & Notes
-------------------
- The fetch_sql_from_blob function requires the azure-storage-blob package at runtime.
- Automatic table discovery only works for filesystems accessible via os.listdir. For
  remote stores (ADLS Gen2) provide an explicit table_list when calling register_silver_tables.
- This module writes Delta format outputs and expects the environment to have delta-core/spark configured.

