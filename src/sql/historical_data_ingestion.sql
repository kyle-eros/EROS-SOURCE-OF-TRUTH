-- =====================================================
-- HISTORICAL DATA INGESTION - FILTERED APPROACH
-- Load MASS.MESSAGE.STATs.csv for ACTIVE CREATORS ONLY
-- =====================================================

-- Step 1: Create staging table for historical data
CREATE OR REPLACE TABLE `of-scheduler-proj.staging.historical_message_staging` (
  message_text STRING,
  username_raw STRING,
  sending_time STRING,
  price_usd_raw STRING,
  earnings_usd NUMERIC,
  sent INTEGER,
  viewed INTEGER,
  purchased INTEGER,
  view_ratio FLOAT64,
  sent_buy_ratio FLOAT64,
  viewed_buy_ratio FLOAT64,
  message_type STRING,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Step 2: Create filtered historical data view (ACTIVE CREATORS ONLY)
CREATE OR REPLACE VIEW `of-scheduler-proj.staging.v_historical_filtered` AS
WITH active_creators AS (
  SELECT DISTINCT username_std 
  FROM `of-scheduler-proj.core.active_overrides` 
  WHERE include = TRUE
),
normalized_historical AS (
  SELECT 
    h.*,
    -- Normalize username to match current system
    CASE 
      WHEN LOWER(h.username_raw) = 'misslexa' THEN 'miss lexa'
      WHEN LOWER(h.username_raw) = 'itskassielee' THEN 'itskassielee'
      WHEN LOWER(h.username_raw) = 'oliviahansley' THEN 'olivia hansley'
      WHEN LOWER(h.username_raw) = 'tessatan' THEN 'tessatan'
      WHEN LOWER(h.username_raw) = 'michellegxoxo' THEN 'michelle gxoxo'
      ELSE LOWER(REPLACE(h.username_raw, '_', ' '))
    END AS username_normalized,
    
    -- Generate message_id and caption_hash
    GENERATE_UUID() AS message_id,
    SHA256(h.message_text) AS caption_hash,
    
    -- Parse sending time
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', h.sending_time) AS sending_ts,
    DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', h.sending_time)) AS sending_date,
    
    -- Parse price
    CASE 
      WHEN h.price_usd_raw = '0' THEN 0
      ELSE CAST(h.price_usd_raw AS NUMERIC)
    END AS price_usd,
    
    -- Generate row key
    CONCAT(
      'hist_',
      GENERATE_UUID(),
      '_',
      FORMAT_TIMESTAMP('%Y%m%d', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', h.sending_time))
    ) AS row_key_v1
    
  FROM `of-scheduler-proj.staging.historical_message_staging` h
  WHERE h.message_text IS NOT NULL 
    AND h.sending_time IS NOT NULL
    AND h.earnings_usd >= 0
)
SELECT 
  n.*
FROM normalized_historical n
JOIN active_creators a ON n.username_normalized = a.username_std;

-- Step 3: Preview data quality and overlap
SELECT 
  'Historical Data Quality Check' AS check_type,
  COUNT(*) AS total_records,
  COUNT(DISTINCT username_normalized) AS unique_creators,
  MIN(sending_ts) AS earliest_date,
  MAX(sending_ts) AS latest_date,
  SUM(earnings_usd) AS total_revenue,
  AVG(earnings_usd) AS avg_revenue_per_msg
FROM `of-scheduler-proj.staging.v_historical_filtered`;

-- Step 4: Check overlap with current active creators
SELECT 
  'Creator Overlap Analysis' AS analysis_type,
  h.username_normalized,
  COUNT(*) AS historical_messages,
  SUM(h.earnings_usd) AS historical_revenue,
  MAX(h.sending_ts) AS last_historical_message
FROM `of-scheduler-proj.staging.v_historical_filtered` h
GROUP BY h.username_normalized
ORDER BY historical_messages DESC;