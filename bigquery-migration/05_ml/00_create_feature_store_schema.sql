-- =========================================
-- CREATE ML FEATURE STORE SCHEMA
-- =========================================
-- Run this ONCE to create the table structure
-- =========================================

CREATE TABLE IF NOT EXISTS `of-scheduler-proj.layer_05_ml.feature_store` (
  caption_id STRING,
  creator_username STRING,
  creator_page_type STRING,
  username_page STRING,
  caption_category STRING,
  performance_features STRUCT<
    rps_30d FLOAT64,
    rps_weighted FLOAT64,
    rps_smoothed FLOAT64,
    purchase_rate_30d FLOAT64,
    purchase_rate_smoothed FLOAT64,
    revenue_30d FLOAT64,
    sends_30d INT64,
    trend_slope FLOAT64,
    stability_score FLOAT64,
    confidence_score FLOAT64
  >,
  statistical_features STRUCT<
    median_rps FLOAT64,
    p75_rps FLOAT64,
    p25_rps FLOAT64,
    rps_stddev FLOAT64,
    iqr FLOAT64,
    coefficient_variation FLOAT64
  >,
  exploration_features STRUCT<
    ucb_bonus FLOAT64,
    exploration_temperature FLOAT64,
    novelty_bonus FLOAT64,
    smoothing_weight FLOAT64
  >,
  temporal_features STRUCT<
    best_hour INT64,
    best_hour_rps FLOAT64,
    best_day_of_week INT64,
    hours_since_use INT64,
    last_used_timestamp TIMESTAMP,
    active_days_30d INT64
  >,
  cooldown_features STRUCT<
    base_cooldown_hours FLOAT64,
    fatigue_score FLOAT64,
    is_eligible BOOL
  >,
  composite_scores STRUCT<
    base_score FLOAT64,
    exploration_score FLOAT64,
    rps_percentile FLOAT64,
    category_percentile FLOAT64
  >,
  computed_at TIMESTAMP,
  computed_date DATE
)
PARTITION BY computed_date
CLUSTER BY username_page, caption_id
OPTIONS(
  description="ML feature store with comprehensive features for caption ranking. Includes volume-weighted metrics, Bayesian smoothing, UCB exploration, temporal patterns, and dynamic cooldowns. Updated incrementally daily."
);