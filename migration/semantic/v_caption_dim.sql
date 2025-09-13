CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_caption_dim`
OPTIONS(description="Refactored caption dimension view pointing to the new centralized caption dimension table.")
AS
SELECT
  caption_id,
  caption_hash,
  caption_text,
  caption_category AS caption_type,
  CASE
    WHEN explicitness_level = 1 THEN 'mild'
    WHEN explicitness_level = 2 THEN 'moderate'
    WHEN explicitness_level = 3 THEN 'explicit'
    ELSE 'pending_review'
  END AS explicitness,
  content_tags AS theme_tags,
  creator_username AS username_std
FROM `of-scheduler-proj.layer_03_foundation.dim_caption`
WHERE is_active = TRUE;