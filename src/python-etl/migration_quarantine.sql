INSERT INTO `of-scheduler-proj.ops.quarantine_gmail`
SELECT
  'migration-initial' AS ingestion_run_id,
  CURRENT_TIMESTAMP() AS ingested_at,
  'implausible_event_year_or_unparsable' AS quarantine_reason,
  IFNULL(message_id, 'unknown') AS raw_message_id,
  IFNULL(source_file, 'unknown') AS raw_source_file,
  TO_JSON_STRING(STRUCT(Message, Sending_time, Sender, Price, Sent, Viewed, Purchased, Earnings)) AS raw_data,
  CONCAT('Failed to parse date or year out of range: ', Sending_time) AS error_details,
  CURRENT_TIMESTAMP() AS quarantined_at
FROM (
  SELECT
    Message, Sending_time, Sender, Price, Sent, Viewed, Purchased, Earnings,
    message_id, source_file,
    PARSE_TIMESTAMP('%b %d, %Y at %I:%M %p', Sending_time, 'America/Denver') AS parsed_ts
  FROM `of-scheduler-proj.staging.gmail_etl_daily`
  WHERE sending_date BETWEEN DATE '2018-01-01' AND CURRENT_DATE()
)
WHERE parsed_ts IS NULL
   OR EXTRACT(YEAR FROM parsed_ts) NOT BETWEEN 2018 AND EXTRACT(YEAR FROM CURRENT_DATE())