-- File: $OUT_DIR/setup_ops_tables.sql
-- Create operations tables for caption bank automation

-- Create ingestion log table
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.caption_ingestion_log_v1` (
  run_id STRING NOT NULL,
  run_ts TIMESTAMP NOT NULL,
  source_rows INT64,
  new_captions_inserted INT64,
  missing_text INT64,
  duplicates_detected INT64,
  window_scanned_days INT64,
  error_count INT64,
  error_sample STRING
)
PARTITION BY DATE(run_ts)
CLUSTER BY run_id;