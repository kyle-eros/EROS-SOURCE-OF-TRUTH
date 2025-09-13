-- =========================================
-- ML RANKER
-- =========================================
-- Purpose: Final ranking logic combining features with config
-- Produces ranked caption recommendations per slot
-- =========================================

CREATE OR REPLACE VIEW `of-scheduler-proj.layer_05_ml.ml_ranker` AS
WITH schedule_slots AS (
  -- Generate schedule slots (next 24 hours)
  SELECT
    username_page,
    slot_timestamp,
    DATE(slot_timestamp) AS slot_date,
    EXTRACT(HOUR FROM slot_timestamp) AS slot_hour,
    EXTRACT(DAYOFWEEK FROM slot_timestamp) AS slot_dow
  FROM (
    SELECT DISTINCT username_page
    FROM `of-scheduler-proj.layer_05_ml.feature_store`
    WHERE computed_date = CURRENT_DATE()
  ) u
  CROSS JOIN (
    SELECT timestamp AS slot_timestamp
    FROM UNNEST(
      GENERATE_TIMESTAMP_ARRAY(
        CURRENT_TIMESTAMP(),
        TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR),
        INTERVAL 1 HOUR
      )
    ) AS timestamp
  )
),

config AS (
  -- Get ML configuration (will be replaced with config table)
  SELECT
    'active' AS page_state,
    0.4 AS w_performance,
    0.2 AS w_exploration,
    0.2 AS w_recency,
    0.2 AS w_stability,
    10 AS epsilon_percent,  -- 10% random exploration
    2.0 AS ucb_c,           -- UCB exploration constant
    6 AS min_cooldown_hours,
    0.8 AS fatigue_threshold
),

eligible_captions AS (
  -- Get eligible captions with features
  SELECT
    fs.*,
    ss.slot_timestamp,
    ss.slot_date,
    ss.slot_hour,
    ss.slot_dow,
    cfg.*
  FROM `of-scheduler-proj.layer_05_ml.feature_store` fs
  JOIN schedule_slots ss
    ON fs.username_page = ss.username_page
  CROSS JOIN config cfg
  WHERE fs.computed_date = CURRENT_DATE()
    AND fs.cooldown_features.is_eligible = TRUE
),

scored_captions AS (
  SELECT
    username_page,
    caption_id,
    slot_timestamp,
    slot_date,
    slot_hour,
    
    -- Component scores
    composite_scores.rps_percentile AS performance_score,
    exploration_features.ucb_bonus * ucb_c AS exploration_score,
    EXP(-0.1 * temporal_features.hours_since_use / 24) AS recency_score,
    performance_features.stability_score AS stability_score,
    
    -- Weighted combination
    (
      w_performance * composite_scores.rps_percentile +
      w_exploration * exploration_features.ucb_bonus * ucb_c +
      w_recency * EXP(-0.1 * temporal_features.hours_since_use / 24) +
      w_stability * performance_features.stability_score
    ) AS base_score,
    
    -- Apply epsilon-greedy exploration
    CASE
      -- Use hash for deterministic randomization
      WHEN MOD(ABS(FARM_FINGERPRINT(CONCAT(caption_id, CAST(slot_timestamp AS STRING)))), 100) < epsilon_percent
      THEN RAND()  -- Pure random for exploration
      ELSE (
        w_performance * composite_scores.rps_percentile +
        w_exploration * exploration_features.ucb_bonus * ucb_c +
        w_recency * EXP(-0.1 * temporal_features.hours_since_use / 24) +
        w_stability * performance_features.stability_score
      )
    END AS final_score,
    
    -- Temporal bonus (boost if it's the best hour for this caption)
    CASE
      WHEN slot_hour = temporal_features.best_hour THEN 1.2
      WHEN ABS(slot_hour - temporal_features.best_hour) <= 2 THEN 1.1
      ELSE 1.0
    END AS temporal_multiplier,
    
    -- Features for debugging
    performance_features,
    exploration_features,
    temporal_features,
    statistical_features,
    cooldown_features,
    composite_scores
    
  FROM eligible_captions
),

ranked_captions AS (
  SELECT
    *,
    final_score * temporal_multiplier AS adjusted_score,
    
    -- Ranking
    ROW_NUMBER() OVER (
      PARTITION BY username_page, slot_timestamp
      ORDER BY final_score * temporal_multiplier DESC
    ) AS rank,
    
    -- Percentile within slot
    PERCENT_RANK() OVER (
      PARTITION BY username_page, slot_timestamp
      ORDER BY final_score * temporal_multiplier
    ) AS score_percentile
    
  FROM scored_captions
)

SELECT
  -- Core fields
  username_page,
  caption_id,
  slot_timestamp,
  slot_date,
  slot_hour,
  rank,
  
  -- Scores
  ROUND(adjusted_score, 4) AS score,
  ROUND(score_percentile * 100, 2) AS score_percentile,
  ROUND(performance_score, 4) AS performance_component,
  ROUND(exploration_score, 4) AS exploration_component,
  ROUND(recency_score, 4) AS recency_component,
  ROUND(stability_score, 4) AS stability_component,
  
  -- Key metrics for display
  ROUND(performance_features.rps_smoothed, 2) AS expected_rps,
  ROUND(performance_features.confidence_score, 3) AS confidence,
  ROUND(temporal_features.hours_since_use / 24, 1) AS days_since_use,
  performance_features.sends_30d AS recent_sends,
  
  -- Recommendation category
  CASE
    WHEN rank = 1 THEN 'primary'
    WHEN rank <= 3 THEN 'secondary'
    WHEN rank <= 5 THEN 'backup'
    ELSE 'alternative'
  END AS recommendation_tier,
  
  -- Exploration flag
  CASE
    WHEN exploration_score > performance_score THEN TRUE
    ELSE FALSE
  END AS is_exploration,
  
  -- Debug info (can be removed in production)
  STRUCT(
    performance_features.rps_30d,
    performance_features.trend_slope,
    exploration_features.ucb_bonus,
    temporal_features.best_hour,
    cooldown_features.fatigue_score
  ) AS debug_info
  
FROM ranked_captions
WHERE rank <= 10;  -- Keep top 10 per slot

-- Add view description
-- Note: BigQuery doesn't support ALTER VIEW SET OPTIONS, so this is a comment
-- Description: "ML ranking view that combines feature store with configuration to produce ranked caption recommendations for each scheduling slot. Includes exploration/exploitation balance and temporal optimization."