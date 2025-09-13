-- =========================================
-- USERNAME MAPPING: Standardize Creator Names
-- =========================================
-- Purpose: Extract and map new usernames from Gmail ETL data
-- Schedule: Run after Gmail ETL (5:30 AM and 5:30 PM)
-- =========================================

-- Insert new username mappings discovered from messages
INSERT INTO `of-scheduler-proj.raw.username_mapping` (
  mapping_id,
  raw_username,
  normalized_username,
  base_username,
  standard_username,
  account_type,
  created_at,
  updated_at
)
WITH extracted_usernames AS (
  -- Extract usernames from recent messages
  SELECT DISTINCT
    TRIM(REGEXP_EXTRACT(Message, r'^([^:]+):')) AS raw_username,
    Sender,
    COUNT(*) AS message_count,
    SUM(Sent) AS total_sent,
    SUM(Earnings) AS total_earnings,
    MAX(sending_ts) AS last_seen
  FROM `of-scheduler-proj.staging.v_gmail_etl_daily_deduped`  -- Use deduped view
  WHERE sending_date >= DATE_SUB(CURRENT_DATE("America/Denver"), INTERVAL 30 DAY)  -- Anchored to Denver
    AND Message IS NOT NULL
    AND REGEXP_CONTAINS(Message, r'^[^:]+:.*$')  -- Has username:caption format
  GROUP BY raw_username, Sender
),
new_usernames AS (
  -- Filter out already mapped usernames
  SELECT
    eu.*
  FROM extracted_usernames eu
  WHERE NOT EXISTS (
    SELECT 1
    FROM `of-scheduler-proj.raw.username_mapping` um
    WHERE LOWER(TRIM(um.raw_username)) = LOWER(TRIM(eu.raw_username))
  )
  AND eu.raw_username IS NOT NULL
  AND LENGTH(TRIM(eu.raw_username)) > 2  -- Minimum username length
)
SELECT
  GENERATE_UUID() AS mapping_id,
  raw_username,
  -- Normalized username (lowercase, trim spaces)
  LOWER(TRIM(raw_username)) AS normalized_username,
  -- Base username (remove special chars, keep alphanumeric)
  LOWER(REGEXP_REPLACE(
    REGEXP_REPLACE(TRIM(raw_username), r'[^a-zA-Z0-9]', ''),
    r'_+', '_'
  )) AS base_username,
  -- Standard username (lowercase, underscore for spaces)
  LOWER(REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(TRIM(raw_username), r'\s+', '_'),  -- Replace spaces with underscore
      r'[^a-z0-9_]', '_'  -- Replace special chars with underscore
    ),
    r'_+', '_'  -- Replace multiple underscores with single
  )) AS standard_username,
  -- Determine account type based on patterns and earnings
  CASE
    WHEN REGEXP_CONTAINS(LOWER(raw_username), r'(free|trial)') THEN 'free'
    WHEN REGEXP_CONTAINS(LOWER(raw_username), r'(vip|premium|paid)') THEN 'paid'
    WHEN total_earnings > 1000 THEN 'premium'
    WHEN total_earnings > 100 THEN 'standard'
    ELSE 'basic'
  END AS account_type,
  CURRENT_TIMESTAMP() AS created_at,
  CURRENT_TIMESTAMP() AS updated_at
FROM new_usernames
WHERE raw_username IS NOT NULL;

-- =========================================
-- Update dim_creator with new creators
-- =========================================
INSERT INTO `of-scheduler-proj.layer_03_foundation.dim_creator` (
  creator_key,
  username,
  account_type,
  account_status,
  created_date,
  last_active_date,
  valid_from,
  valid_to,
  is_current_record,
  etl_source,
  etl_timestamp
)
WITH new_creators AS (
  SELECT
    um.standard_username AS creator_key,
    um.standard_username AS username,
    um.account_type,
    'active' AS account_status,
    DATE(um.created_at) AS created_date,
    DATE(um.updated_at) AS last_active_date,
    CURRENT_TIMESTAMP() AS valid_from,
    TIMESTAMP('9999-12-31 23:59:59') AS valid_to,
    TRUE AS is_current_record,
    'gmail_etl' AS etl_source,
    CURRENT_TIMESTAMP() AS etl_timestamp
  FROM `of-scheduler-proj.raw.username_mapping` um
  WHERE NOT EXISTS (
    SELECT 1
    FROM `of-scheduler-proj.layer_03_foundation.dim_creator` dc
    WHERE dc.creator_key = um.standard_username
  )
  AND um.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
)
SELECT * FROM new_creators;