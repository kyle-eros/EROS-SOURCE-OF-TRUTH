-- =========================================
-- DIMENSION: Creator Master
-- =========================================
-- Purpose: Single source of truth for creator/page information
-- Key improvements:
--   - Consolidated page classifications
--   - Account status tracking
--   - Performance segmentation
-- =========================================

CREATE OR REPLACE TABLE `of-scheduler-proj.layer_03_foundation.dim_creator`
PARTITION BY DATE(valid_from)
CLUSTER BY username, account_status
AS
WITH creator_base AS (
  -- Get unique creators from model profiles
  SELECT DISTINCT
    creator AS username,
    CURRENT_DATE() AS account_created_date,  -- Will use actual data later
    CURRENT_DATE() AS last_activity_date
  FROM `of-scheduler-proj.raw.model_profiles_enhanced`
),

page_classification AS (
  -- Get the most recent page classification
  SELECT 
    username_std AS username,
    page_type AS primary_page_type,
    'active' AS page_state,  -- Default to active
    ARRAY(
      SELECT DISTINCT secondary_type 
      FROM UNNEST(['free', 'vip', 'main']) AS secondary_type
      WHERE secondary_type != page_type
    ) AS secondary_page_types,
    1.0 AS confidence_score,  -- Default confidence
    decided_as_of
  FROM `of-scheduler-proj.mart.page_type_authority_snap`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY username_std 
    ORDER BY decided_as_of DESC
  ) = 1
),

performance_tier AS (
  -- Calculate performance segments based on message facts
  SELECT
    username_std AS username,
    AVG(earnings_usd) AS avg_daily_revenue,
    STDDEV(earnings_usd) AS revenue_volatility,
    COUNT(DISTINCT DATE(sending_ts)) AS active_days,
    -- Segment creators by performance
    CASE
      WHEN AVG(earnings_usd) >= 100 THEN 'whale'
      WHEN AVG(earnings_usd) >= 10 THEN 'high_value'
      WHEN AVG(earnings_usd) >= 1 THEN 'regular'
      WHEN AVG(earnings_usd) > 0 THEN 'casual'
      ELSE 'inactive'
    END AS performance_segment
  FROM `of-scheduler-proj.core.message_facts`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  GROUP BY 1
),

scheduler_assignment AS (
  -- Get current scheduler assignments
  SELECT
    username_std AS username,
    scheduler_name AS scheduler_code,
    scheduler_email,
    CURRENT_DATE() AS assignment_date  -- Default date
  FROM `of-scheduler-proj.ops.scheduler_assignments_v1`
  WHERE is_active = TRUE
)

SELECT
  -- Surrogate key
  GENERATE_UUID() AS creator_key,
  
  -- Natural key
  cb.username,
  
  -- Account information
  CASE
    WHEN pt.performance_segment = 'whale' THEN 'premium'
    WHEN pt.performance_segment = 'high_value' THEN 'vip'
    ELSE 'standard'
  END AS account_type,
  
  -- Page classifications
  STRUCT(
    COALESCE(pc.primary_page_type, 'main') AS primary_type,
    COALESCE(pc.secondary_page_types, []) AS secondary_types,
    COALESCE(pc.confidence_score, 1.0) AS confidence_score,
    pc.decided_as_of AS classification_date
  ) AS page_classifications,
  
  -- Performance metrics
  STRUCT(
    pt.performance_segment,
    pt.avg_daily_revenue,
    pt.revenue_volatility,
    pt.active_days
  ) AS performance_metrics,
  
  -- Account status
  CASE
    WHEN DATE_DIFF(CURRENT_DATE(), cb.last_activity_date, DAY) <= 7 THEN 'active'
    WHEN DATE_DIFF(CURRENT_DATE(), cb.last_activity_date, DAY) <= 30 THEN 'dormant'
    WHEN DATE_DIFF(CURRENT_DATE(), cb.last_activity_date, DAY) <= 90 THEN 'at_risk'
    ELSE 'churned'
  END AS account_status,
  
  -- Scheduler assignment
  STRUCT(
    sa.scheduler_code,
    sa.scheduler_email,
    sa.assignment_date
  ) AS scheduler_info,
  
  -- Important dates
  cb.account_created_date AS created_date,
  cb.last_activity_date AS last_active_date,
  
  -- SCD Type 2 fields
  CURRENT_TIMESTAMP() AS valid_from,
  TIMESTAMP('9999-12-31 23:59:59') AS valid_to,
  TRUE AS is_current_record,
  
  -- Audit fields
  'initial_load' AS etl_source,
  CURRENT_TIMESTAMP() AS etl_timestamp

FROM creator_base cb
LEFT JOIN page_classification pc ON cb.username = pc.username
LEFT JOIN performance_tier pt ON cb.username = pt.username
LEFT JOIN scheduler_assignment sa ON cb.username = sa.username;