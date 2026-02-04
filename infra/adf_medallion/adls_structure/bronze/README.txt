Bronze layer - raw ingestion zone

Partitioning convention (batch):
bronze/{source}/YYYY/MM/DD/ - store raw parquet/JSON/CSV files

Partitioning convention (streaming micro-batches):
bronze/streaming/{source}/YYYY/MM/DD/HH/ - store micro-batch files by hour

Recommended file naming:
{source}_YYYYMMDD_HHMMSS.parquet or {source}_YYYYMMDD.parquet

Partition keys: source, year, month, day

Example: ingesting adobe_analytics.csv
- Source: adobe_analytics.csv
- Target path: bronze/adobe_analytics/2024/01/10/adobe_analytics_20240110.parquet

Keep raw copies for reproducibility. Use a _SUCCESS marker file after successful batch writes.
