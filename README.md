# wikimedia-airflow-pipeline
Batch data pipeline using Cloud Composer (Airflow) that fetches Wikipedia edit events, stores raw NDJSON files in GCS, and routes human edits, bot edits, and DLQ events into separate BigQuery tables with deduplication
