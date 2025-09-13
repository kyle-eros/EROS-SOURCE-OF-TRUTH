CREATE OR REPLACE VIEW `of-scheduler-proj.layer_05_ml.v_caption_safe_candidates`
OPTIONS(description="Refactored view to filter caption candidates based on creator-specific content profiles (e.g., explicitness, themes).")
AS
WITH creator_profiles AS (
  -- This logic would need to be migrated to a new ops_config table.
  -- For now, we assume a placeholder or default profile.
  SELECT
    creator_key,
    'explicit' AS max_explicitness, -- Placeholder
    ['main'] AS allowed_types,      -- Placeholder
    CAST(NULL AS STRING) AS blocked_themes -- Placeholder
  FROM `of-scheduler-proj.layer_03_foundation.dim_creator`
  WHERE is_current_record = TRUE
)
SELECT
  feat.*
FROM `of-scheduler-proj.layer_05_ml.v_caption_features` feat
JOIN `of-scheduler-proj.layer_03_foundation.dim_creator` c
  ON feat.creator_username = c.username AND c.is_current_record = TRUE
JOIN creator_profiles p ON c.creator_key = p.creator_key
WHERE
  -- Explicitness matching
  (p.max_explicitness = 'explicit' OR
   (p.max_explicitness = 'moderate' AND feat.explicitness_level <= 2) OR
   (p.max_explicitness = 'mild' AND feat.explicitness_level <= 1))
  -- Theme filtering would be added here if profile table existed
;