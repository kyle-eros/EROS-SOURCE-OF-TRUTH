-- =========================================
-- CAPTION EXTRACTION: Gmail ETL → Caption Library
-- =========================================
-- Purpose: Extract new captions from messages and add to library
-- Schedule: Run after Gmail ETL (5:30 AM and 5:30 PM)
-- =========================================

-- Insert new captions that don't exist in the library
INSERT INTO `of-scheduler-proj.raw.caption_library` (
  caption_id,
  caption_hash,
  caption_text,
  caption_type,
  last_used_by,
  last_used_date,
  last_used_page,
  created_at,
  updated_at,
  times_used,
  price_last_sent,
  length_cat,
  explicitness,
  theme_tags
)
WITH new_captions AS (
  -- Extract unique captions from recent staging data
  SELECT DISTINCT
    -- Extract caption text from message (after the username:)
    TRIM(REGEXP_EXTRACT(Message, r':\s*(.*)$')) AS caption_text,
    -- Extract username from message (before the :)
    TRIM(REGEXP_EXTRACT(Message, r'^([^:]+):')) AS username,
    MAX(sending_ts) AS last_used_timestamp,
    -- Performance metrics for metadata
    SUM(Sent) AS total_sent,
    SUM(Purchased) AS total_purchased,
    SUM(Earnings) AS total_earnings,
    AVG(CAST(REGEXP_REPLACE(IFNULL(Price, '0'), r'[\$,]', '') AS FLOAT64)) AS avg_price,
    COUNT(*) AS usage_count
  FROM `of-scheduler-proj.staging.v_gmail_etl_daily_deduped`  -- Use deduped view
  WHERE sending_date >= DATE_SUB(CURRENT_DATE("America/Denver"), INTERVAL 30 DAY)  -- Anchored to Denver
    AND Message IS NOT NULL
    AND Message != ''
    AND REGEXP_CONTAINS(Message, r'^[^:]+:.*$')  -- Has username:caption format
  GROUP BY caption_text, username
),
captions_to_add AS (
  -- Filter out captions that already exist
  SELECT
    nc.*,
    TO_HEX(MD5(LOWER(TRIM(nc.caption_text)))) AS caption_hash
  FROM new_captions nc
  WHERE NOT EXISTS (
    SELECT 1 
    FROM `of-scheduler-proj.raw.caption_library` cl
    WHERE LOWER(TRIM(cl.caption_text)) = LOWER(TRIM(nc.caption_text))
  )
  AND LENGTH(TRIM(nc.caption_text)) > 5  -- Minimum caption length
)
SELECT
  GENERATE_UUID() AS caption_id,
  caption_hash,
  caption_text,
  -- Classify caption type based on content
  CASE
    WHEN REGEXP_CONTAINS(caption_text, r'(?i)(tip|ppv|pay|buy|purchase|exclusive|unlock)') THEN 'ppv'
    WHEN REGEXP_CONTAINS(caption_text, r'(?i)(sale|discount|offer|promo|deal|limited)') THEN 'promo'
    WHEN REGEXP_CONTAINS(caption_text, r'(?i)(hello|hi|hey|morning|night|welcome)') THEN 'greeting'
    WHEN REGEXP_CONTAINS(caption_text, r'(?i)(click|link|bio|check out|swipe)') THEN 'cta'
    WHEN REGEXP_CONTAINS(caption_text, r'(?i)(free|freebie|gift)') THEN 'free'
    WHEN REGEXP_CONTAINS(caption_text, r'(?i)(custom|personal|request)') THEN 'custom'
    WHEN REGEXP_CONTAINS(caption_text, r'(?i)(love|baby|daddy|sexy|hot|naughty)') THEN 'flirty'
    ELSE 'general'
  END AS caption_type,
  username AS last_used_by,
  last_used_timestamp AS last_used_date,
  username AS last_used_page,  -- Same as username for now
  CURRENT_TIMESTAMP() AS created_at,
  CURRENT_TIMESTAMP() AS updated_at,
  usage_count AS times_used,
  CAST(avg_price AS NUMERIC) AS price_last_sent,
  -- Length category
  CASE
    WHEN LENGTH(caption_text) < 50 THEN 'short'
    WHEN LENGTH(caption_text) < 150 THEN 'medium'
    WHEN LENGTH(caption_text) < 300 THEN 'long'
    ELSE 'very_long'
  END AS length_cat,
  -- Explicitness rating (simple heuristic)
  CASE
    WHEN REGEXP_CONTAINS(LOWER(caption_text), r'(nude|naked|explicit|xxx|porn)') THEN 'explicit'
    WHEN REGEXP_CONTAINS(LOWER(caption_text), r'(sexy|hot|naughty|tease|lingerie)') THEN 'suggestive'
    ELSE 'safe'
  END AS explicitness,
  -- Theme tags (comma-separated)
  ARRAY_TO_STRING(
    ARRAY(
      SELECT tag FROM UNNEST([
        IF(REGEXP_CONTAINS(caption_text, r'(?i)(tip|ppv|pay)'), 'monetization', NULL),
        IF(REGEXP_CONTAINS(caption_text, r'(?i)(sale|discount|offer)'), 'promotion', NULL),
        IF(REGEXP_CONTAINS(caption_text, r'(?i)(hello|hi|hey)'), 'greeting', NULL),
        IF(REGEXP_CONTAINS(caption_text, r'(?i)(love|baby|daddy)'), 'flirty', NULL),
        IF(REGEXP_CONTAINS(caption_text, r'#\w+'), 'hashtags', NULL),
        IF(REGEXP_CONTAINS(caption_text, r'[\x{1F300}-\x{1FAFF}]|[\x{2600}-\x{27BF}]'), 'emojis', NULL),
        IF(REGEXP_CONTAINS(caption_text, r'(?i)(http|www\.|\.com|link\.bio)'), 'links', NULL)
      ]) AS tag
      WHERE tag IS NOT NULL
    ),
    ','
  ) AS theme_tags
FROM captions_to_add
WHERE caption_text IS NOT NULL
  AND LENGTH(TRIM(caption_text)) > 5;