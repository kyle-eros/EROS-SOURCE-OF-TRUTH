-- =========================================
-- DIMENSION: Caption Master
-- =========================================
-- Purpose: Single source of truth for all caption metadata
-- Key improvements:
--   - Consistent caption_id as primary key
--   - Pre-joined page type information
--   - SCD Type 2 for tracking changes
-- =========================================

CREATE OR REPLACE TABLE `of-scheduler-proj.layer_03_foundation.dim_caption`
PARTITION BY DATE(valid_from)
CLUSTER BY creator_username, caption_category
AS
WITH caption_base AS (
  -- Get distinct captions from existing caption library
  SELECT DISTINCT
    caption_hash,
    -- Generate consistent caption_id
    CONCAT('CAP_', caption_hash) AS caption_id,
    caption_text,
    last_used_by AS creator_username,
    -- Standardize category names
    CASE 
      WHEN LOWER(caption_type) LIKE '%ppv%' THEN 'ppv'
      WHEN LOWER(caption_type) LIKE '%sub%' THEN 'subscription'
      WHEN LOWER(caption_type) LIKE '%tip%' THEN 'tip_menu'
      WHEN LOWER(caption_type) LIKE '%free%' THEN 'free'
      ELSE COALESCE(caption_type, 'standard')
    END AS caption_category,
    -- Extract theme tags from text
    ARRAY(
      SELECT DISTINCT tag
      FROM UNNEST(REGEXP_EXTRACT_ALL(LOWER(caption_text), r'#(\w+)')) AS tag
      WHERE LENGTH(tag) > 2
    ) AS content_tags,
    -- Explicitness scoring (1-5 scale)
    CASE
      WHEN explicitness = 'explicit' THEN 5
      WHEN explicitness = 'suggestive' THEN 4
      WHEN explicitness = 'moderate' THEN 3
      WHEN explicitness = 'mild' THEN 2
      ELSE 1
    END AS explicitness_level,
    created_at AS first_seen_timestamp,
    last_used_date AS last_updated_timestamp
  FROM `of-scheduler-proj.raw.caption_library`
  WHERE caption_hash IS NOT NULL
    AND last_used_by IS NOT NULL
),

page_type_mapping AS (
  -- Get the latest page type for each creator
  SELECT 
    username_std AS creator_username,
    page_type,
    'active' AS page_state,  -- Default to active for now
    decided_as_of
  FROM `of-scheduler-proj.mart.page_type_authority_snap`
  WHERE decided_as_of = (
    SELECT MAX(decided_as_of) 
    FROM `of-scheduler-proj.mart.page_type_authority_snap`
  )
)

SELECT
  -- Surrogate key for versioning
  GENERATE_UUID() AS caption_key,
  
  -- Natural key
  cb.caption_id,
  cb.caption_hash,
  
  -- Caption attributes
  cb.caption_text,
  cb.caption_category,
  cb.content_tags,
  cb.explicitness_level,
  
  -- Creator information
  cb.creator_username,
  COALESCE(ptm.page_type, 'main') AS creator_page_type,
  COALESCE(ptm.page_state, 'active') AS creator_page_state,
  
  -- Computed fields for easy filtering
  CONCAT(cb.creator_username, '__', COALESCE(ptm.page_type, 'main')) AS username_page,
  LENGTH(cb.caption_text) AS caption_length,
  ARRAY_LENGTH(cb.content_tags) AS tag_count,
  
  -- Metadata
  TRUE AS is_active,
  cb.first_seen_timestamp AS created_timestamp,
  CURRENT_TIMESTAMP() AS valid_from,
  TIMESTAMP('9999-12-31 23:59:59') AS valid_to,
  
  -- Audit fields
  'initial_load' AS etl_source,
  CURRENT_TIMESTAMP() AS etl_timestamp

FROM caption_base cb
LEFT JOIN page_type_mapping ptm
  ON cb.creator_username = ptm.creator_username;