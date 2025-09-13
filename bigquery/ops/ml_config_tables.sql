-- =====================================================
-- ML CONFIGURATION TABLES WITH SEED DATA
-- =====================================================
-- Project: of-scheduler-proj
-- Purpose: Centralized configuration for ML ranking parameters
-- No hardcoded constants allowed in views - all configs here
-- =====================================================

-- 1. ML Ranking Weights by Page State
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.ml_ranking_weights_v1` (
  page_state STRING NOT NULL,
  w_rps FLOAT64 NOT NULL,
  w_open FLOAT64 NOT NULL,
  w_buy FLOAT64 NOT NULL,
  w_dowhod FLOAT64 NOT NULL,
  w_price FLOAT64 NOT NULL,
  w_novelty FLOAT64 NOT NULL,
  w_momentum FLOAT64 NOT NULL,
  ucb_c FLOAT64 NOT NULL COMMENT 'UCB exploration constant',
  epsilon FLOAT64 NOT NULL COMMENT 'Epsilon-greedy exploration rate',
  updated_at TIMESTAMP NOT NULL,
  updated_by STRING
) 
PARTITION BY DATE(updated_at)
CLUSTER BY page_state;

-- Seed with balanced weights
MERGE `of-scheduler-proj.ops.ml_ranking_weights_v1` T
USING (
  SELECT * FROM UNNEST([
    STRUCT('Harvest' AS page_state, 0.35 AS w_rps, 0.20 AS w_open, 0.15 AS w_buy, 
           0.10 AS w_dowhod, 0.10 AS w_price, 0.05 AS w_novelty, 0.05 AS w_momentum,
           1.2 AS ucb_c, 0.05 AS epsilon, CURRENT_TIMESTAMP() AS updated_at, 
           'ml_config_init' AS updated_by),
    STRUCT('Build', 0.28, 0.18, 0.14, 0.12, 0.12, 0.08, 0.08, 1.5, 0.10, 
           CURRENT_TIMESTAMP(), 'ml_config_init'),
    STRUCT('Recover', 0.22, 0.18, 0.18, 0.12, 0.12, 0.09, 0.09, 1.7, 0.15, 
           CURRENT_TIMESTAMP(), 'ml_config_init'),
    STRUCT('Balance', 0.30, 0.20, 0.15, 0.10, 0.10, 0.075, 0.075, 1.4, 0.08, 
           CURRENT_TIMESTAMP(), 'ml_config_init')
  ])
) S
ON T.page_state = S.page_state
WHEN NOT MATCHED THEN INSERT ROW;

-- 2. Explore/Exploit Configuration
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.explore_exploit_config_v1` (
  config_key STRING NOT NULL,
  min_obs_for_exploit INT64 NOT NULL COMMENT 'Min observations before exploitation',
  max_explorer_share FLOAT64 NOT NULL COMMENT 'Max % of slots for exploration',
  cold_start_days INT64 NOT NULL COMMENT 'Days to treat as cold-start',
  thompson_sampling_enabled BOOL NOT NULL DEFAULT FALSE,
  ucb_enabled BOOL NOT NULL DEFAULT TRUE,
  decay_factor FLOAT64 NOT NULL DEFAULT 0.95 COMMENT 'Time decay for historical data',
  updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(updated_at);

-- Seed exploration config
MERGE `of-scheduler-proj.ops.explore_exploit_config_v1` T
USING (
  SELECT 'default' AS config_key, 
         30 AS min_obs_for_exploit,
         0.25 AS max_explorer_share,
         7 AS cold_start_days,
         FALSE AS thompson_sampling_enabled,
         TRUE AS ucb_enabled,
         0.95 AS decay_factor,
         CURRENT_TIMESTAMP() AS updated_at
) S
ON T.config_key = S.config_key
WHEN NOT MATCHED THEN INSERT ROW;

-- 3. Cooldown Override Rules
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.cooldown_overrides_v1` (
  page_id STRING NOT NULL,
  caption_id STRING,
  min_cooldown_hours INT64 NOT NULL DEFAULT 168,
  max_weekly_uses INT64 DEFAULT 1,
  effective_from DATE NOT NULL,
  effective_to DATE,
  reason STRING,
  updated_at TIMESTAMP NOT NULL
)
PARTITION BY effective_from
CLUSTER BY page_id;

-- 4. Fallback Caption Configuration
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.fallback_config_v1` (
  page_type STRING NOT NULL,
  fallback_caption_id STRING NOT NULL,
  fallback_caption_text STRING NOT NULL,
  fallback_price FLOAT64 NOT NULL,
  reason_code STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

-- Seed fallback captions
MERGE `of-scheduler-proj.ops.fallback_config_v1` T
USING (
  SELECT * FROM UNNEST([
    STRUCT('main' AS page_type, 
           'FALLBACK_001' AS fallback_caption_id,
           '🔥 Something special just for you! Check it out 💋' AS fallback_caption_text,
           9.99 AS fallback_price,
           'no_eligible_caption' AS reason_code,
           CURRENT_TIMESTAMP() AS updated_at),
    STRUCT('vip', 
           'FALLBACK_002',
           '✨ Exclusive content dropping now! Don\'t miss this 🎁',
           14.99,
           'no_eligible_caption',
           CURRENT_TIMESTAMP())
  ])
) S
ON T.page_type = S.page_type AND T.reason_code = S.reason_code
WHEN NOT MATCHED THEN INSERT ROW;

-- 5. Price Elasticity Bands
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.price_bands_v1` (
  tier STRING NOT NULL,
  page_type STRING NOT NULL,
  min_price FLOAT64 NOT NULL,
  max_price FLOAT64 NOT NULL,
  optimal_price FLOAT64 NOT NULL,
  ladder_steps INT64 NOT NULL DEFAULT 3,
  ladder_increment FLOAT64 NOT NULL DEFAULT 2.0,
  updated_at TIMESTAMP NOT NULL
)
CLUSTER BY tier, page_type;

-- Seed price bands
MERGE `of-scheduler-proj.ops.price_bands_v1` T
USING (
  SELECT * FROM UNNEST([
    -- Premium tier
    STRUCT('premium' AS tier, 'main' AS page_type, 25.0 AS min_price, 
           55.0 AS max_price, 38.0 AS optimal_price, 3 AS ladder_steps, 
           3.0 AS ladder_increment, CURRENT_TIMESTAMP() AS updated_at),
    STRUCT('premium', 'vip', 35.0, 75.0, 50.0, 3, 5.0, CURRENT_TIMESTAMP()),
    -- Standard tier
    STRUCT('standard', 'main', 15.0, 35.0, 24.0, 3, 2.0, CURRENT_TIMESTAMP()),
    STRUCT('standard', 'vip', 20.0, 45.0, 32.0, 3, 3.0, CURRENT_TIMESTAMP()),
    -- Budget tier
    STRUCT('budget', 'main', 8.0, 20.0, 14.0, 3, 1.5, CURRENT_TIMESTAMP()),
    STRUCT('budget', 'vip', 12.0, 28.0, 19.0, 3, 2.0, CURRENT_TIMESTAMP())
  ])
) S
ON T.tier = S.tier AND T.page_type = S.page_type
WHEN NOT MATCHED THEN INSERT ROW;

-- 6. Quality Thresholds for Monitoring
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.quality_thresholds_v1` (
  metric_name STRING NOT NULL,
  min_threshold FLOAT64,
  max_threshold FLOAT64,
  alert_enabled BOOL NOT NULL DEFAULT TRUE,
  auto_rollback_enabled BOOL NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMP NOT NULL
);

-- Seed quality thresholds
MERGE `of-scheduler-proj.ops.quality_thresholds_v1` T
USING (
  SELECT * FROM UNNEST([
    STRUCT('cooldown_violation_rate' AS metric_name, NULL AS min_threshold, 
           0.01 AS max_threshold, TRUE AS alert_enabled, TRUE AS auto_rollback_enabled,
           CURRENT_TIMESTAMP() AS updated_at),
    STRUCT('caption_diversity_rate', 0.85, NULL, TRUE, FALSE, CURRENT_TIMESTAMP()),
    STRUCT('fallback_usage_rate', NULL, 0.03, TRUE, TRUE, CURRENT_TIMESTAMP()),
    STRUCT('score_normalization', 0.0, 100.0, TRUE, FALSE, CURRENT_TIMESTAMP()),
    STRUCT('bytes_billed_daily', NULL, 10737418240.0, TRUE, FALSE, CURRENT_TIMESTAMP()), -- 10GB
    STRUCT('null_caption_rate', NULL, 0.001, TRUE, TRUE, CURRENT_TIMESTAMP())
  ])
) S
ON T.metric_name = S.metric_name
WHEN NOT MATCHED THEN INSERT ROW;

-- 7. Cost Budget Configuration
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.cost_budget_v1` (
  budget_type STRING NOT NULL,
  max_bytes_per_query INT64 NOT NULL,
  max_bytes_per_day INT64 NOT NULL,
  alert_threshold_pct FLOAT64 NOT NULL DEFAULT 0.8,
  updated_at TIMESTAMP NOT NULL
);

-- Seed cost budgets
MERGE `of-scheduler-proj.ops.cost_budget_v1` T
USING (
  SELECT 'production' AS budget_type,
         5368709120 AS max_bytes_per_query, -- 5GB per query
         53687091200 AS max_bytes_per_day,  -- 50GB per day
         0.8 AS alert_threshold_pct,
         CURRENT_TIMESTAMP() AS updated_at
) S
ON T.budget_type = S.budget_type
WHEN NOT MATCHED THEN INSERT ROW;