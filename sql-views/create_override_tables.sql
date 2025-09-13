-- Create tables for caption override tracking and ML feedback loop
-- Project: of-scheduler-proj

-- 1. Override tracking table
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.caption_overrides` (
  override_timestamp TIMESTAMP NOT NULL,
  scheduler_email STRING NOT NULL,
  username_page STRING NOT NULL,
  slot_time STRING,
  original_caption_id STRING,
  override_caption_id STRING NOT NULL,
  override_reason STRING,
  slot_price FLOAT64,
  performance_tracked BOOLEAN DEFAULT FALSE
)
PARTITION BY DATE(override_timestamp)
CLUSTER BY scheduler_email, username_page;

-- 2. Override feedback table  
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.override_feedback` (
  feedback_timestamp TIMESTAMP NOT NULL,
  scheduler_email STRING NOT NULL,
  username_page STRING NOT NULL,
  override_result STRING, -- SUCCESS/FAILURE/NEUTRAL
  feedback_message STRING,
  revenue_delta FLOAT64,
  open_rate_delta FLOAT64,
  ml_training_completed BOOLEAN DEFAULT FALSE
)
PARTITION BY DATE(feedback_timestamp)
CLUSTER BY scheduler_email;

-- 3. ML training signals table
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.ml_training_signals` (
  signal_timestamp TIMESTAMP NOT NULL,
  signal_type STRING, -- HUMAN_OVERRIDE_SUCCESS, HUMAN_OVERRIDE_FAILURE
  username_page STRING,
  slot_context STRUCT<
    hour_of_day INT64,
    day_of_week INT64,
    price_point FLOAT64,
    page_type STRING
  >,
  original_caption_features STRUCT<
    caption_id STRING,
    predicted_revenue FLOAT64,
    predicted_open_rate FLOAT64
  >,
  override_caption_features STRUCT<
    caption_id STRING,
    actual_revenue FLOAT64,
    actual_open_rate FLOAT64
  >,
  performance_delta STRUCT<
    revenue_impact FLOAT64,
    open_rate_impact FLOAT64
  >,
  scheduler_context STRUCT<
    email STRING,
    experience_days INT64,
    historical_accuracy FLOAT64
  >
)
PARTITION BY DATE(signal_timestamp)
CLUSTER BY signal_type, username_page;

-- 4. View for ML training pipeline
CREATE OR REPLACE VIEW `of-scheduler-proj.ops.v_ml_training_overrides` AS
WITH override_performance AS (
  SELECT 
    o.*,
    f.override_result,
    f.revenue_delta,
    f.open_rate_delta,
    -- Calculate scheduler accuracy
    COUNT(*) OVER (PARTITION BY o.scheduler_email) as total_overrides,
    COUNTIF(f.override_result LIKE 'SUCCESS%') OVER (PARTITION BY o.scheduler_email) as successful_overrides
  FROM `of-scheduler-proj.ops.caption_overrides` o
  LEFT JOIN `of-scheduler-proj.ops.override_feedback` f
    ON f.scheduler_email = o.scheduler_email
    AND f.username_page = o.username_page
    AND DATE(f.feedback_timestamp) = DATE(o.override_timestamp)
)
SELECT 
  *,
  successful_overrides / NULLIF(total_overrides, 0) as scheduler_accuracy,
  CASE 
    WHEN revenue_delta > 10 THEN 'HIGH_VALUE_LEARNING'
    WHEN revenue_delta < -10 THEN 'HIGH_COST_MISTAKE'
    ELSE 'STANDARD_SIGNAL'
  END as training_priority
FROM override_performance
WHERE override_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY);

-- 5. Scheduler performance dashboard view
CREATE OR REPLACE VIEW `of-scheduler-proj.ops.v_scheduler_performance` AS
SELECT 
  scheduler_email,
  COUNT(*) as total_overrides_30d,
  COUNTIF(override_result LIKE 'SUCCESS%') as successful_overrides,
  COUNTIF(override_result LIKE 'FAILURE%') as failed_overrides,
  ROUND(AVG(revenue_delta), 2) as avg_revenue_impact,
  ROUND(SUM(revenue_delta), 2) as total_revenue_impact,
  ROUND(COUNTIF(override_result LIKE 'SUCCESS%') / COUNT(*) * 100, 1) as success_rate,
  ARRAY_AGG(
    STRUCT(
      username_page,
      override_result,
      revenue_delta
    ) 
    ORDER BY ABS(revenue_delta) DESC 
    LIMIT 5
  ) as top_impacts
FROM `of-scheduler-proj.ops.override_feedback`
WHERE feedback_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY scheduler_email
ORDER BY total_revenue_impact DESC;