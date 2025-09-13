-- SMOKE TEST: Verify new architecture
-- 1. Check staging table has data
SELECT 
  'Staging Data' AS test,
  COUNT(*) as row_count,
  COUNT(DISTINCT message_id) as unique_messages,
  MIN(message_sent_date) as min_date,
  MAX(message_sent_date) as max_date
FROM `of-scheduler-proj.layer_02_staging.gmail_events_staging`
WHERE ingestion_date = CURRENT_DATE();

-- 2. Test table function with raw passthrough
SELECT 
  'Table Function' AS test,
  COUNT(*) as normalized_rows,
  COUNT(DISTINCT caption_hash) as unique_captions,
  SUM(revenue_per_send) as total_rps
FROM `of-scheduler-proj.layer_02_staging.fn_gmail_events_normalized`(
  CURRENT_DATE()
);

-- 3. Check fact table population
SELECT 
  'Fact Table' AS test,
  COUNT(*) as fact_rows,
  COUNT(DISTINCT caption_key) as unique_captions,
  SUM(gross_revenue_usd) as total_revenue
FROM `of-scheduler-proj.layer_03_foundation.fact_message_send`
WHERE send_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);

-- 4. Verify monitoring views work
SELECT 
  'Monitoring View' AS test,
  ingestion_date,
  total_messages,
  unique_senders,
  messages_with_nulls
FROM `of-scheduler-proj.ops.v_ingestion_monitoring`
ORDER BY ingestion_date DESC
LIMIT 1;

-- 5. Test backward compatibility view
SELECT 
  'Compatibility View' AS test,
  COUNT(*) as view_rows
FROM `of-scheduler-proj.staging.v_gmail_etl_daily_normalized`
LIMIT 1;