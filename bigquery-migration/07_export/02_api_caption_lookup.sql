-- =========================================
-- EXPORT: API Caption Lookup
-- =========================================
-- Purpose: Simple view for quick caption lookups
-- Used by Apps Script and other applications
-- =========================================

CREATE OR REPLACE VIEW `of-scheduler-proj.layer_07_export.api_caption_lookup` AS
WITH caption_metrics AS (
  -- Get latest performance metrics
  SELECT
    caption_id,
    MAX(temporal_features.last_used_timestamp) AS last_used,
    AVG(performance_features.rps_smoothed) AS avg_rps,
    AVG(performance_features.confidence_score) AS avg_confidence,
    MIN(cooldown_features.is_eligible) AS is_available
  FROM `of-scheduler-proj.layer_05_ml.feature_store`
  WHERE computed_date = CURRENT_DATE()
  GROUP BY caption_id
)

SELECT
  -- Caption identifiers
  dc.caption_id,
  dc.caption_hash,
  
  -- Caption content
  dc.caption_text,
  dc.caption_category,
  dc.caption_length,
  
  -- Creator info
  dc.creator_username,
  dc.creator_page_type,
  dc.username_page,
  
  -- Performance tier
  CASE
    WHEN cm.avg_rps >= 1.0 THEN 'top'
    WHEN cm.avg_rps >= 0.5 THEN 'good'
    WHEN cm.avg_rps >= 0.2 THEN 'average'
    WHEN cm.avg_rps IS NULL THEN 'new'
    ELSE 'explore'
  END AS performance_tier,
  
  -- Availability
  COALESCE(cm.is_available, TRUE) AS is_available,
  cm.last_used AS last_used_timestamp,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), cm.last_used, DAY) AS days_since_use,
  
  -- Metrics
  ROUND(COALESCE(cm.avg_rps, 0), 2) AS expected_rps,
  ROUND(COALESCE(cm.avg_confidence, 0), 2) AS confidence_score,
  
  -- Status
  dc.is_active AS is_active
  
FROM `of-scheduler-proj.layer_03_foundation.dim_caption` dc
LEFT JOIN caption_metrics cm
  ON dc.caption_id = cm.caption_id
WHERE dc.is_active = TRUE;