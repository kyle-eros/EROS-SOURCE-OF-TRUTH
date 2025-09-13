-- BigQuery Health Check & CI Probe
-- Run this query to verify all critical views are functioning
WITH health_checks AS (
  -- Core views health
  SELECT 'core.v_weekly_template_7d_pages_final' as view_name, COUNT(*) as row_count
  FROM `of-scheduler-proj.core.v_weekly_template_7d_pages_final`
  WHERE DATE(scheduled_datetime_utc) >= CURRENT_DATE() - 7
  
  UNION ALL
  
  -- Sheets integration health  
  SELECT 'sheets.v_my_day_slots_all_v1' as view_name, COUNT(*) as row_count
  FROM `of-scheduler-proj.sheets.v_my_day_slots_all_v1`
  
  UNION ALL
  
  -- New content signals view health
  SELECT 'core.v_caption_content_signals_v1' as view_name, COUNT(DISTINCT username_std) as row_count
  FROM `of-scheduler-proj.core.v_caption_content_signals_v1`
  
  UNION ALL
  
  -- Staging tables health (with required partition filters)
  SELECT 'staging.gmail_etl_daily' as view_name, COUNT(*) as row_count
  FROM `of-scheduler-proj.staging.gmail_etl_daily`
  WHERE sending_date >= CURRENT_DATE() - 30
  
  UNION ALL
  
  SELECT 'staging.historical_message_staging' as view_name, COUNT(*) as row_count  
  FROM `of-scheduler-proj.staging.historical_message_staging`
  WHERE sending_date >= CURRENT_DATE() - 30
)
SELECT 
  view_name,
  row_count,
  CASE 
    WHEN row_count = 0 THEN '❌ EMPTY'
    WHEN row_count < 10 THEN '⚠️ LOW'
    ELSE '✅ OK'
  END as health_status,
  CURRENT_TIMESTAMP() as checked_at
FROM health_checks
ORDER BY view_name