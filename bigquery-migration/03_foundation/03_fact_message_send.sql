-- =========================================
-- FACT: Message Send Events
-- =========================================
-- Purpose: Core fact table for all message sending events
-- Grain: One row per caption send event (batch of messages)
-- Key improvements:
--   - Consistent foreign keys to dimensions
--   - Pre-calculated metrics
--   - Optimized partitioning and clustering
-- =========================================

CREATE OR REPLACE TABLE `of-scheduler-proj.layer_03_foundation.fact_message_send`
PARTITION BY send_date
CLUSTER BY creator_key, caption_key, send_date
AS
WITH message_events AS (
  -- Get all message send events from core message facts
  SELECT
    -- Event identifiers
    GENERATE_UUID() AS message_send_key,
    sending_ts AS send_timestamp,
    DATE(sending_ts) AS send_date,
    
    -- Time dimensions
    EXTRACT(HOUR FROM sending_ts) AS time_of_day_utc,
    EXTRACT(DAYOFWEEK FROM sending_ts) AS day_of_week,
    EXTRACT(WEEK FROM sending_ts) AS week_of_year,
    EXTRACT(MONTH FROM sending_ts) AS month_of_year,
    
    -- Keys (to be joined with dimensions)
    CONCAT('CAP_', caption_hash) AS caption_id,
    username_std AS creator_username,
    
    -- Volume metrics
    sent AS messages_sent,
    viewed AS messages_viewed,
    purchased AS messages_purchased,
    
    -- Financial metrics
    price_usd,
    earnings_usd AS gross_revenue_usd,
    earnings_usd * 0.8 AS net_revenue_usd, -- Assuming 20% platform fee
    
    -- Response metrics (will be null for now as these fields don't exist)
    NULL AS time_to_first_view_seconds,
    NULL AS time_to_first_purchase_seconds,
    
    -- Additional context (defaults since columns don't exist)
    'AUTO' AS scheduler_code,
    'scheduled' AS send_type,
    NULL AS campaign_id
    
  FROM `of-scheduler-proj.core.message_facts`
  WHERE sending_ts IS NOT NULL
    AND caption_hash IS NOT NULL
),

-- Join with dimension keys
dimension_keys AS (
  SELECT 
    me.*,
    dc.caption_key,
    dcr.creator_key
  FROM message_events me
  LEFT JOIN `of-scheduler-proj.layer_03_foundation.dim_caption` dc
    ON me.caption_id = dc.caption_id
    AND dc.is_active = TRUE
  LEFT JOIN `of-scheduler-proj.layer_03_foundation.dim_creator` dcr
    ON me.creator_username = dcr.username
    AND dcr.is_current_record = TRUE
)

SELECT
  -- Primary key
  message_send_key,
  
  -- Foreign keys to dimensions
  COALESCE(caption_key, 'UNKNOWN') AS caption_key,
  COALESCE(creator_key, 'UNKNOWN') AS creator_key,
  
  -- Time attributes
  send_timestamp,
  send_date,
  time_of_day_utc,
  day_of_week,
  week_of_year,
  month_of_year,
  
  -- Volume metrics
  messages_sent,
  messages_viewed,
  messages_purchased,
  
  -- Calculated rates
  SAFE_DIVIDE(messages_viewed, messages_sent) AS view_rate,
  SAFE_DIVIDE(messages_purchased, messages_sent) AS purchase_rate,
  SAFE_DIVIDE(messages_purchased, messages_viewed) AS conversion_rate,
  
  -- Financial metrics
  price_usd,
  gross_revenue_usd,
  net_revenue_usd,
  SAFE_DIVIDE(net_revenue_usd, messages_sent) AS revenue_per_send,
  SAFE_DIVIDE(net_revenue_usd, messages_purchased) AS revenue_per_purchase,
  
  -- Response metrics
  STRUCT(
    CAST(NULL AS INT64) AS time_to_first_view_seconds,
    CAST(NULL AS INT64) AS time_to_first_purchase_seconds,
    'unknown' AS response_category,
    SAFE_DIVIDE(messages_sent - messages_viewed, messages_sent) AS bounce_rate
  ) AS response_metrics,
  
  -- Context attributes
  COALESCE(scheduler_code, 'MANUAL') AS scheduler_code,
  COALESCE(send_type, 'unknown') AS send_type,
  campaign_id,
  
  -- Data quality flags
  CASE
    WHEN messages_sent = 0 THEN 'zero_sends'
    WHEN messages_viewed > messages_sent THEN 'view_anomaly'
    WHEN messages_purchased > messages_viewed THEN 'purchase_anomaly'
    WHEN gross_revenue_usd < 0 THEN 'negative_revenue'
    ELSE 'valid'
  END AS quality_flag,
  
  -- Audit fields
  'message_facts_import' AS etl_source,
  CURRENT_TIMESTAMP() AS etl_timestamp
  
FROM dimension_keys;