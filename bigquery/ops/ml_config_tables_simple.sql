-- =====================================================
-- ML CONFIGURATION TABLES - SIMPLIFIED VERSION
-- =====================================================

-- 1. ML Ranking Weights
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.ml_ranking_weights_v1` (
  page_state STRING,
  w_rps FLOAT64,
  w_open FLOAT64,
  w_buy FLOAT64,
  w_dowhod FLOAT64,
  w_price FLOAT64,
  w_novelty FLOAT64,
  w_momentum FLOAT64,
  ucb_c FLOAT64,
  epsilon FLOAT64,
  updated_at TIMESTAMP,
  updated_by STRING
);

INSERT INTO `of-scheduler-proj.ops.ml_ranking_weights_v1`
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
WHERE NOT EXISTS (SELECT 1 FROM `of-scheduler-proj.ops.ml_ranking_weights_v1`);

-- 2. Explore/Exploit Configuration
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.explore_exploit_config_v1` (
  config_key STRING,
  min_obs_for_exploit INT64,
  max_explorer_share FLOAT64,
  cold_start_days INT64,
  thompson_sampling_enabled BOOL,
  ucb_enabled BOOL,
  decay_factor FLOAT64,
  updated_at TIMESTAMP
);

INSERT INTO `of-scheduler-proj.ops.explore_exploit_config_v1`
SELECT 'default', 30, 0.25, 7, FALSE, TRUE, 0.95, CURRENT_TIMESTAMP()
FROM (SELECT 1)
WHERE NOT EXISTS (SELECT 1 FROM `of-scheduler-proj.ops.explore_exploit_config_v1`);

-- 3. Cooldown settings
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.core.cooldown_settings_v1` (
  min_cooldown_hours INT64,
  max_weekly_uses INT64,
  updated_at TIMESTAMP
);

INSERT INTO `of-scheduler-proj.core.cooldown_settings_v1`
SELECT 168, 3, CURRENT_TIMESTAMP()
FROM (SELECT 1)
WHERE NOT EXISTS (SELECT 1 FROM `of-scheduler-proj.core.cooldown_settings_v1`);

-- 4. Quality Thresholds
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.quality_thresholds_v1` (
  metric_name STRING,
  min_threshold FLOAT64,
  max_threshold FLOAT64,
  alert_enabled BOOL,
  auto_rollback_enabled BOOL,
  updated_at TIMESTAMP
);

INSERT INTO `of-scheduler-proj.ops.quality_thresholds_v1`
SELECT * FROM UNNEST([
  STRUCT('cooldown_violation_rate' AS metric_name, NULL AS min_threshold, 
         0.01 AS max_threshold, TRUE AS alert_enabled, TRUE AS auto_rollback_enabled,
         CURRENT_TIMESTAMP() AS updated_at),
  STRUCT('caption_diversity_rate', 0.85, NULL, TRUE, FALSE, CURRENT_TIMESTAMP()),
  STRUCT('fallback_usage_rate', NULL, 0.03, TRUE, TRUE, CURRENT_TIMESTAMP()),
  STRUCT('score_normalization', 0.0, 100.0, TRUE, FALSE, CURRENT_TIMESTAMP()),
  STRUCT('null_caption_rate', NULL, 0.001, TRUE, TRUE, CURRENT_TIMESTAMP())
])
WHERE NOT EXISTS (SELECT 1 FROM `of-scheduler-proj.ops.quality_thresholds_v1`);

-- 5. Fallback config
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.fallback_config_v1` (
  page_type STRING,
  fallback_caption_id STRING,
  fallback_caption_text STRING,
  fallback_price FLOAT64,
  reason_code STRING,
  updated_at TIMESTAMP
);

INSERT INTO `of-scheduler-proj.ops.fallback_config_v1`
SELECT * FROM UNNEST([
  STRUCT('main' AS page_type, 'FALLBACK_001' AS fallback_caption_id,
         '🔥 Something special just for you! Check it out 💋' AS fallback_caption_text,
         9.99 AS fallback_price, 'no_eligible_caption' AS reason_code,
         CURRENT_TIMESTAMP() AS updated_at),
  STRUCT('vip', 'FALLBACK_002', '✨ Exclusive content dropping now!', 
         14.99, 'no_eligible_caption', CURRENT_TIMESTAMP())
])
WHERE NOT EXISTS (SELECT 1 FROM `of-scheduler-proj.ops.fallback_config_v1`);

-- 6. Price bands
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.price_bands_v1` (
  tier STRING,
  page_type STRING,
  min_price FLOAT64,
  max_price FLOAT64,
  optimal_price FLOAT64,
  ladder_steps INT64,
  ladder_increment FLOAT64,
  updated_at TIMESTAMP
);

INSERT INTO `of-scheduler-proj.ops.price_bands_v1`
SELECT * FROM UNNEST([
  STRUCT('premium' AS tier, 'main' AS page_type, 25.0 AS min_price, 
         55.0 AS max_price, 38.0 AS optimal_price, 3 AS ladder_steps, 
         3.0 AS ladder_increment, CURRENT_TIMESTAMP() AS updated_at),
  STRUCT('premium', 'vip', 35.0, 75.0, 50.0, 3, 5.0, CURRENT_TIMESTAMP()),
  STRUCT('standard', 'main', 15.0, 35.0, 24.0, 3, 2.0, CURRENT_TIMESTAMP()),
  STRUCT('standard', 'vip', 20.0, 45.0, 32.0, 3, 3.0, CURRENT_TIMESTAMP()),
  STRUCT('budget', 'main', 8.0, 20.0, 14.0, 3, 1.5, CURRENT_TIMESTAMP()),
  STRUCT('budget', 'vip', 12.0, 28.0, 19.0, 3, 2.0, CURRENT_TIMESTAMP())
])
WHERE NOT EXISTS (SELECT 1 FROM `of-scheduler-proj.ops.price_bands_v1`);

-- 7. Scheduler assignments
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops.scheduler_assignments_v1` (
  username_std STRING,
  username_page STRING,
  scheduler_name STRING,
  scheduler_email STRING,
  is_active BOOL,
  updated_at TIMESTAMP
);

INSERT INTO `of-scheduler-proj.ops.scheduler_assignments_v1`
SELECT * FROM UNNEST([
  STRUCT('corvettemykala' AS username_std, 'corvettemykala__main' AS username_page,
         'scheduler_1' AS scheduler_name, 'scheduler1@agency.com' AS scheduler_email,
         TRUE AS is_active, CURRENT_TIMESTAMP() AS updated_at),
  STRUCT('del', 'del__main', 'scheduler_1', 'scheduler1@agency.com', TRUE, CURRENT_TIMESTAMP()),
  STRUCT('kay', 'kay__main', 'scheduler_2', 'scheduler2@agency.com', TRUE, CURRENT_TIMESTAMP())
])
WHERE NOT EXISTS (SELECT 1 FROM `of-scheduler-proj.ops.scheduler_assignments_v1`);