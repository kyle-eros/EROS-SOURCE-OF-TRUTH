-- =========================================
-- OPS MONITOR: Data Quality Checks
-- =========================================
-- Purpose: Monitor data freshness, quality, and anomalies
-- Runs hourly to ensure pipeline health
-- =========================================

CREATE OR REPLACE TABLE `of-scheduler-proj.ops_monitor.data_quality_checks`
PARTITION BY DATE(check_timestamp)
AS
WITH table_freshness AS (
  -- Check when tables were last updated
  SELECT
    table_schema AS dataset,
    table_name,
    TIMESTAMP_MILLIS(last_modified_time) AS last_modified,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_MILLIS(last_modified_time), MINUTE) AS staleness_minutes,
    row_count,
    size_bytes
  FROM `of-scheduler-proj.region-us.INFORMATION_SCHEMA.TABLE_STORAGE`
  WHERE table_schema LIKE 'layer_%'
),

data_volumes AS (
  -- Check data volumes vs historical averages
  SELECT
    'fact_message_send' AS table_name,
    COUNT(*) AS row_count_today,
    AVG(daily_count) AS avg_daily_count,
    STDDEV(daily_count) AS stddev_daily_count
  FROM (
    SELECT 
      send_date,
      COUNT(*) AS daily_count
    FROM `of-scheduler-proj.layer_03_foundation.fact_message_send`
    WHERE send_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY send_date
  )
),

quality_metrics AS (
  -- Check for data quality issues
  SELECT
    'fact_message_send' AS table_name,
    
    -- Null checks
    COUNTIF(caption_key = 'UNKNOWN') / COUNT(*) AS unknown_caption_rate,
    COUNTIF(creator_key = 'UNKNOWN') / COUNT(*) AS unknown_creator_rate,
    
    -- Anomaly checks
    COUNTIF(messages_purchased > messages_viewed) / COUNT(*) AS purchase_anomaly_rate,
    COUNTIF(messages_viewed > messages_sent) / COUNT(*) AS view_anomaly_rate,
    COUNTIF(gross_revenue_usd < 0) / COUNT(*) AS negative_revenue_rate,
    
    -- Completeness
    COUNTIF(quality_flag != 'valid') / COUNT(*) AS invalid_record_rate
    
  FROM `of-scheduler-proj.layer_03_foundation.fact_message_send`
  WHERE send_date = CURRENT_DATE()
)

SELECT
  CURRENT_TIMESTAMP() AS check_timestamp,
  
  -- Freshness checks
  STRUCT(
    MAX(CASE WHEN table_name = 'fact_message_send' THEN last_modified END) AS fact_table_updated,
    MAX(CASE WHEN table_name = 'dim_caption' THEN last_modified END) AS caption_dim_updated,
    MAX(CASE WHEN table_name = 'dim_creator' THEN last_modified END) AS creator_dim_updated,
    MAX(CASE WHEN table_name = 'feature_store' THEN staleness_minutes END) AS feature_store_staleness_min
  ) AS freshness_metrics,
  
  -- Volume checks
  STRUCT(
    dv.row_count_today,
    dv.avg_daily_count,
    ABS(dv.row_count_today - dv.avg_daily_count) / NULLIF(dv.stddev_daily_count, 0) AS volume_zscore,
    CASE
      WHEN ABS(dv.row_count_today - dv.avg_daily_count) > 3 * dv.stddev_daily_count THEN 'ANOMALY'
      WHEN ABS(dv.row_count_today - dv.avg_daily_count) > 2 * dv.stddev_daily_count THEN 'WARNING'
      ELSE 'NORMAL'
    END AS volume_status
  ) AS volume_metrics,
  
  -- Quality metrics
  STRUCT(
    qm.unknown_caption_rate,
    qm.unknown_creator_rate,
    qm.purchase_anomaly_rate,
    qm.view_anomaly_rate,
    qm.negative_revenue_rate,
    qm.invalid_record_rate,
    CASE
      WHEN qm.invalid_record_rate > 0.1 THEN 'CRITICAL'
      WHEN qm.invalid_record_rate > 0.05 THEN 'WARNING'
      ELSE 'HEALTHY'
    END AS quality_status
  ) AS quality_metrics,
  
  -- Overall health score
  CASE
    WHEN MAX(tf.staleness_minutes) > 120 THEN 'CRITICAL - Stale Data'
    WHEN dv.volume_zscore > 3 THEN 'WARNING - Volume Anomaly'
    WHEN qm.invalid_record_rate > 0.1 THEN 'WARNING - Quality Issues'
    ELSE 'HEALTHY'
  END AS overall_status
  
FROM table_freshness tf
CROSS JOIN data_volumes dv
CROSS JOIN quality_metrics qm
GROUP BY dv.row_count_today, dv.avg_daily_count, dv.stddev_daily_count,
         qm.unknown_caption_rate, qm.unknown_creator_rate, qm.purchase_anomaly_rate,
         qm.view_anomaly_rate, qm.negative_revenue_rate, qm.invalid_record_rate;