-- =========================================
-- STAGING: Message Events
-- =========================================
-- Purpose: Clean and standardize raw message event data
-- Key transformations:
--   - Data type standardization
--   - Null handling
--   - Outlier detection
--   - Deduplication
-- =========================================

CREATE OR REPLACE TABLE `of-scheduler-proj.layer_02_staging.stg_message_events`
PARTITION BY event_date
CLUSTER BY username_std, caption_hash
AS
WITH raw_events AS (
  -- Pull from existing message facts
  SELECT
    -- Identifiers
    COALESCE(message_id, GENERATE_UUID()) AS message_event_id,
    caption_hash,
    username_std,
    
    -- Timestamps
    sending_ts,
    DATE(sending_ts) AS event_date,
    
    -- Metrics (with null handling)
    COALESCE(sent, 0) AS messages_sent,
    COALESCE(viewed, 0) AS messages_viewed,
    COALESCE(purchased, 0) AS messages_purchased,
    
    -- Financial (with validation)
    CASE 
      WHEN price_usd < 0 THEN 0
      WHEN price_usd > 1000 THEN 1000  -- Cap at reasonable max
      ELSE COALESCE(price_usd, 0)
    END AS price_usd,
    
    CASE
      WHEN earnings_usd < 0 THEN 0
      WHEN earnings_usd > price_usd * sent THEN price_usd * purchased  -- Fix impossible earnings
      ELSE COALESCE(earnings_usd, 0)
    END AS revenue_usd,
    
    -- Additional fields (defaults since not in source)
    'AUTO' AS scheduler_code,
    NULL AS campaign_id,
    
    -- Data quality scoring
    CASE
      WHEN caption_hash IS NULL THEN 'missing_caption'
      WHEN username_std IS NULL THEN 'missing_user'
      WHEN sending_ts IS NULL THEN 'missing_timestamp'
      WHEN sent = 0 AND (viewed > 0 OR purchased > 0) THEN 'zero_send_anomaly'
      WHEN viewed > sent THEN 'view_count_anomaly'
      WHEN purchased > viewed AND viewed > 0 THEN 'purchase_count_anomaly'
      WHEN earnings_usd > price_usd * sent THEN 'revenue_anomaly'
      ELSE 'clean'
    END AS quality_status,
    
    -- Source tracking
    'message_facts' AS source_table,
    CURRENT_DATE() AS source_date
    
  FROM `of-scheduler-proj.core.message_facts`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
),

-- Remove duplicates
deduplicated AS (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY message_event_id, caption_hash, username_std, sending_ts
        ORDER BY source_date DESC, quality_status
      ) AS row_num
    FROM raw_events
  )
  WHERE row_num = 1
),

-- Add computed fields
enriched AS (
  SELECT
    *,
    
    -- Performance metrics
    SAFE_DIVIDE(messages_viewed, messages_sent) AS view_rate,
    SAFE_DIVIDE(messages_purchased, messages_sent) AS purchase_rate,
    SAFE_DIVIDE(revenue_usd, messages_sent) AS revenue_per_send,
    
    -- Time-based features
    EXTRACT(HOUR FROM sending_ts) AS hour_utc,
    EXTRACT(DAYOFWEEK FROM sending_ts) AS day_of_week,
    FORMAT_TIMESTAMP('%Y-%m', sending_ts) AS year_month,
    
    -- Categorizations
    CASE
      WHEN price_usd = 0 THEN 'free'
      WHEN price_usd < 5 THEN 'low'
      WHEN price_usd < 20 THEN 'medium'
      WHEN price_usd < 50 THEN 'high'
      ELSE 'premium'
    END AS price_tier,
    
    CASE
      WHEN messages_sent < 100 THEN 'small'
      WHEN messages_sent < 1000 THEN 'medium'
      WHEN messages_sent < 5000 THEN 'large'
      ELSE 'mega'
    END AS send_size_category
    
  FROM deduplicated
)

SELECT
  -- Core fields
  message_event_id,
  caption_hash,
  username_std,
  sending_ts,
  event_date,
  
  -- Volume metrics
  messages_sent,
  messages_viewed,
  messages_purchased,
  
  -- Financial metrics
  price_usd,
  revenue_usd,
  
  -- Calculated metrics
  view_rate,
  purchase_rate,
  revenue_per_send,
  
  -- Time dimensions
  hour_utc,
  day_of_week,
  year_month,
  
  -- Categories
  price_tier,
  send_size_category,
  
  -- Context
  scheduler_code,
  campaign_id,
  
  -- Data quality
  quality_status,
  
  -- Audit
  source_table,
  source_date,
  CURRENT_TIMESTAMP() AS staging_timestamp
  
FROM enriched
WHERE quality_status IN ('clean', 'view_count_anomaly')  -- Allow some known anomalies
  AND messages_sent > 0  -- Must have actual sends
  AND sending_ts < CURRENT_TIMESTAMP()  -- No future dates
  AND sending_ts > TIMESTAMP('2020-01-01');  -- No ancient data

-- Add table metadata
ALTER TABLE `of-scheduler-proj.layer_02_staging.stg_message_events`
SET OPTIONS(
  description="Staging table for message events. Cleaned, validated, and deduplicated data from raw message facts. Includes data quality flags and computed metrics."
);