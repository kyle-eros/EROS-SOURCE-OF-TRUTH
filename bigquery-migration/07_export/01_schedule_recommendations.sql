-- =========================================
-- EXPORT: Schedule Recommendations
-- =========================================
-- Purpose: Simple, clean interface for Apps Script
-- Provides ranked caption recommendations per slot
-- =========================================

CREATE OR REPLACE TABLE `of-scheduler-proj.layer_07_export.schedule_recommendations`
PARTITION BY schedule_date
CLUSTER BY username_page, schedule_hour
AS
WITH latest_rankings AS (
  -- Get the most recent ML rankings
  SELECT
    username_page,
    caption_id,
    slot_timestamp,
    slot_date AS schedule_date,
    slot_hour AS schedule_hour,
    rank,
    score,
    expected_rps,
    confidence,
    days_since_use,
    recommendation_tier,
    is_exploration
  FROM `of-scheduler-proj.layer_05_ml.ml_ranker`
  WHERE slot_date >= CURRENT_DATE()
    AND slot_date < DATE_ADD(CURRENT_DATE(), INTERVAL 2 DAY)
),

caption_details AS (
  -- Join with caption dimension for text
  SELECT
    lr.*,
    dc.caption_text,
    dc.caption_category,
    dc.creator_username,
    dc.creator_page_type
  FROM latest_rankings lr
  JOIN `of-scheduler-proj.layer_03_foundation.dim_caption` dc
    ON lr.caption_id = dc.caption_id
    AND dc.is_active = TRUE
)

SELECT
  -- Scheduling identifiers
  username_page,
  creator_username,
  creator_page_type,
  schedule_date,
  schedule_hour,
  
  -- Caption information
  caption_id,
  SUBSTR(caption_text, 1, 200) AS caption_preview,  -- Truncate for readability
  caption_category,
  
  -- Ranking and scoring
  rank AS recommendation_rank,
  ROUND(score, 3) AS ml_score,
  ROUND(expected_rps, 2) AS predicted_rps,
  ROUND(confidence, 2) AS confidence_score,
  
  -- Metadata
  days_since_use AS days_since_last_send,
  recommendation_tier,
  is_exploration AS is_exploration_pick,
  
  -- Versioning
  'v2.0' AS model_version,
  CURRENT_TIMESTAMP() AS generated_at
  
FROM caption_details
WHERE rank <= 5;  -- Top 5 recommendations per slot