-- File: $OUT_DIR/v_new_captions_inflow.sql
-- New-caption detector view with source prioritization

CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_new_captions_inflow_v1` AS
WITH 
-- Prioritized source collection
prioritized_sources AS (
  -- Priority 1: Caption library upload (if exists and has data)
  SELECT
    s.caption_text,
    s.caption_hash,
    s.last_used_page AS username_page,
    CURRENT_TIMESTAMP() AS discovered_at,
    'caption_library_upload' AS src_name,
    1 as source_priority
  FROM `of-scheduler-proj.staging.caption_library_upload` s
  WHERE s.caption_text IS NOT NULL
    AND LENGTH(TRIM(s.caption_text)) > 0
    AND 1=0  -- Disable for now since table may not exist
  
  UNION ALL
  
  -- Priority 2: Gmail ETL normalized (using discovered column names)
  SELECT
    n.Message AS caption_text,
    n.caption_hash,
    COALESCE(
      REGEXP_EXTRACT(n.username_raw, r'^([^_]+)'),  -- Extract base username
      n.username_std
    ) AS username_page,
    CURRENT_TIMESTAMP() AS discovered_at,
    'gmail_etl_normalized' AS src_name,
    2 as source_priority
  FROM `of-scheduler-proj.staging.gmail_etl_normalized` n
  WHERE n.Message IS NOT NULL
    AND LENGTH(TRIM(n.Message)) > 0
    AND n.loaded_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
),

-- Dedupe by text, keeping highest priority source
deduped_sources AS (
  SELECT 
    caption_text,
    caption_hash,
    username_page,
    discovered_at,
    src_name
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY `of-scheduler-proj.util.canonicalize_caption`(caption_text) 
        ORDER BY source_priority
      ) AS rn
    FROM prioritized_sources
  )
  WHERE rn = 1
),

-- Add canonical hashes and features
canon AS (
  SELECT
    caption_text,
    COALESCE(caption_hash, `of-scheduler-proj.util.caption_hash_v2`(caption_text)) AS caption_hash,
    `of-scheduler-proj.util.caption_hash_v2`(caption_text) AS caption_hash_v2,
    username_page,
    discovered_at,
    src_name,
    -- Feature columns for auditing
    `of-scheduler-proj.util.length_bin`(caption_text) AS length_bin,
    `of-scheduler-proj.util.emoji_bin`(caption_text) AS emoji_bin,
    `of-scheduler-proj.util.has_cta`(caption_text) AS has_cta,
    `of-scheduler-proj.util.has_urgency`(caption_text) AS has_urgency,
    `of-scheduler-proj.util.ends_with_question`(caption_text) AS ends_with_question
  FROM deduped_sources
  WHERE caption_text IS NOT NULL
    AND LENGTH(TRIM(caption_text)) > 0
    AND caption_text NOT LIKE '%�%'  -- Mojibake guard
    AND caption_text NOT LIKE '%\ufffd%'  -- Replacement character guard
    AND LENGTH(TRIM(caption_text)) >= 3  -- Minimum length filter
    AND LENGTH(TRIM(caption_text)) <= 2000  -- Maximum length filter
),

-- Existing bank entries
bank AS (
  SELECT DISTINCT caption_hash, caption_hash_v2 
  FROM `of-scheduler-proj.raw.caption_library`
)

-- Final output: new captions only
SELECT c.*
FROM canon c
LEFT JOIN bank b
  ON b.caption_hash = c.caption_hash 
  OR b.caption_hash_v2 = c.caption_hash_v2
WHERE b.caption_hash IS NULL 
  AND b.caption_hash_v2 IS NULL;