Streaming area - short-term micro-batch storage before processing to silver/gold.

Convention for streaming micro-batches:
streaming/{source}/YYYY/MM/DD/HH/

Store small parquet files per micro-batch and periodically compact to larger files in Gold.
