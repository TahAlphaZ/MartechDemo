Silver layer - cleaned and conformed data

Reads from bronze/{source}/YYYY/MM/DD/ and writes to:
silver/{source}/YYYY/MM/DD/

Transform guidance:
- Parse timestamps to UTC
- Standardize column names
- Deduplicate by primary key
- Coerce types and apply null handling

Use partitioning consistent with bronze to enable efficient queries.
