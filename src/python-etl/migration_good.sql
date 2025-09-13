-- Safe migration from old staging to new ingestion-partitioned staging
WITH src AS (
  SELECT
    Message, Sending_time, Sender, Price,
    SAFE_CAST(Sent AS INT64)      AS Sent,
    SAFE_CAST(Viewed AS INT64)    AS Viewed,
    SAFE_CAST(Purchased AS INT64) AS Purchased,
    CAST(Earnings AS STRING)      AS Earnings,
    IFNULL(message_id, GENERATE_UUID())               AS message_id,
    IFNULL(source_file, 'migration/unknown.xlsx')     AS source_file,
    PARSE_TIMESTAMP('%b %d, %Y at %I:%M %p', Sending_time, 'America/Denver') AS parsed_ts
  FROM `of-scheduler-proj.staging.gmail_etl_daily`
  WHERE sending_date BETWEEN DATE '2018-01-01' AND CURRENT_DATE()
),
good AS (
  SELECT *, DATE(parsed_ts) AS parsed_date
  FROM src
  WHERE parsed_ts IS NOT NULL
    AND EXTRACT(YEAR FROM parsed_ts) BETWEEN 2018 AND EXTRACT(YEAR FROM CURRENT_DATE())
)
INSERT INTO `of-scheduler-proj.layer_02_staging.gmail_events_staging` (
  ingestion_run_id, ingested_at, ingestion_date,
  message_id, source_file,
  Message, Sending_time, Sender, Price, Sent, Viewed, Purchased, Earnings,
  message_sent_ts, message_sent_date
)
SELECT
  'migration-initial' AS ingestion_run_id,
  CURRENT_TIMESTAMP() AS ingested_at,
  CURRENT_DATE()      AS ingestion_date,
  message_id, source_file,
  Message, Sending_time, Sender, Price, Sent, Viewed, Purchased, Earnings,
  parsed_ts, parsed_date
FROM good