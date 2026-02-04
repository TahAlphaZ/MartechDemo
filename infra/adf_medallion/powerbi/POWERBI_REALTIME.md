Power BI Real-time & Historical Integration Patterns
==================================================

Recommended patterns for real-time + historical analytics:

1) Pattern A - Stream Analytics → Power BI (Push streaming dataset) + persist to Gold
   - Use ASA to push events to a Power BI streaming dataset for instant visuals (streaming tiles)
   - Also write micro-batches to Gold (parquet) for historical queries via Synapse Serverless or DirectQuery
   - Recommended for immediate KPIs and historical analysis

2) Pattern B - Persist to Gold → Synapse Serverless / Dedicated SQL Pool → DirectQuery
   - Persist events/aggregates to Gold parquet optimized for analytical reads
   - Expose via Synapse Serverless or Dedicated SQL Pool and use DirectQuery in Power BI for near-real-time dashboards
   - Note: DirectQuery has freshness/performance tradeoffs; Premium capacity recommended for heavy workloads

3) Pattern C - Push dataset + scheduled refresh
   - For lower-latency near-real-time you can push to an API-backed dataset and schedule frequent refreshes

Recommended approach: Pattern A (ASA → Power BI for realtime visuals) combined with persisting to Gold for historical/complex analysis.

Example streaming dataset schema (matching ASA output):

- EventEnqueuedUtcTime: datetime
- source: string
- metric1: double
- metric2: double
- payload: string (json)

Guidance:
- Use streaming tiles for KPI cards, gauges, and simple real-time visuals.
- Use Gold parquet + Synapse Serverless for historical reports and complex visuals (DirectQuery or import depending on freshness/size).
- For heavy DirectQuery loads, plan for Premium capacity.

Steps to connect Power BI to ASA:
1. Register an Azure AD app or configure ASA to use a managed identity
2. Grant the app or identity the required Power BI API permissions (Push dataset permissions)
3. Configure Power BI output in ASA with groupName/datasetName/tableName placeholders
4. Test streaming tiles and validate persisted Gold parquet with Synapse queries
