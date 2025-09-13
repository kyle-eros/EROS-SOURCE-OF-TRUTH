#!/bin/bash

echo "========================================="
echo "EXECUTING BIGQUERY ML LAYER UPGRADE - PR1"
echo "========================================="

# Section 1: Create ops_config dataset
echo ""
echo "1. Creating ops_config dataset..."
bq mk --dataset --location=US --description="Operations configuration tables" of-scheduler-proj:ops_config 2>/dev/null || echo "Dataset ops_config already exists"

# Section 2: Create and seed configuration tables
echo ""
echo "2. Creating configuration tables..."
bq query --use_legacy_sql=false --location=US << 'SQL'
-- Feature flags table
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops_config.feature_flags` (
  flag_name STRING NOT NULL,
  is_enabled BOOL NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

-- Seed feature flags
MERGE `of-scheduler-proj.ops_config.feature_flags` AS target
USING (
  SELECT 'enable_weekly_planner' AS flag_name, TRUE AS is_enabled, CURRENT_TIMESTAMP() AS updated_at
  UNION ALL SELECT 'enable_scam_risk', TRUE, CURRENT_TIMESTAMP()
  UNION ALL SELECT 'enable_explicitness_caps', FALSE, CURRENT_TIMESTAMP()
  UNION ALL SELECT 'enable_thompson_true', FALSE, CURRENT_TIMESTAMP()
) AS source
ON target.flag_name = source.flag_name
WHEN MATCHED THEN UPDATE SET
  is_enabled = source.is_enabled,
  updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT ROW;

-- Tier slot packs table
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops_config.tier_slot_packs` (
  tier STRING NOT NULL,
  anchors_per_day INT64 NOT NULL,
  supports_per_day INT64 NOT NULL,
  min_spacing_minutes INT64 NOT NULL,
  jitter_minutes INT64 NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

-- Seed tier slot packs
MERGE `of-scheduler-proj.ops_config.tier_slot_packs` AS target
USING (
  SELECT 'high' AS tier, 2 AS anchors_per_day, 3 AS supports_per_day, 180 AS min_spacing_minutes, 30 AS jitter_minutes, CURRENT_TIMESTAMP() AS updated_at
  UNION ALL SELECT 'medium', 2, 2, 240, 45, CURRENT_TIMESTAMP()
  UNION ALL SELECT 'low', 1, 2, 360, 60, CURRENT_TIMESTAMP()
) AS source
ON target.tier = source.tier
WHEN MATCHED THEN UPDATE SET
  anchors_per_day = source.anchors_per_day,
  supports_per_day = source.supports_per_day,
  min_spacing_minutes = source.min_spacing_minutes,
  jitter_minutes = source.jitter_minutes,
  updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT ROW;

-- Scam guardrails table
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops_config.scam_guardrails` (
  guardrail_name STRING NOT NULL,
  threshold_value FLOAT64 NOT NULL,
  action STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

-- Seed scam guardrails
MERGE `of-scheduler-proj.ops_config.scam_guardrails` AS target
USING (
  SELECT 'min_spacing_minutes' AS guardrail_name, 180.0 AS threshold_value, 'enforce' AS action, CURRENT_TIMESTAMP() AS updated_at
  UNION ALL SELECT 'max_ppv_per_day_high', 5.0, 'cap', CURRENT_TIMESTAMP()
  UNION ALL SELECT 'max_ppv_per_day_medium', 3.0, 'cap', CURRENT_TIMESTAMP()
  UNION ALL SELECT 'max_ppv_per_day_low', 2.0, 'cap', CURRENT_TIMESTAMP()
  UNION ALL SELECT 'max_bumps_per_day', 2.0, 'cap', CURRENT_TIMESTAMP()
  UNION ALL SELECT 'or_drop_brake_pct', 0.25, 'lighten', CURRENT_TIMESTAMP()
  UNION ALL SELECT 'rps_drop_brake_pct', 0.30, 'lighten', CURRENT_TIMESTAMP()
  UNION ALL SELECT 'family_cooldown_days', 3.0, 'enforce', CURRENT_TIMESTAMP()
) AS source
ON target.guardrail_name = source.guardrail_name
WHEN MATCHED THEN UPDATE SET
  threshold_value = source.threshold_value,
  action = source.action,
  updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT ROW;

-- ML params bandit table
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.ops_config.ml_params_bandit` (
  tier STRING NOT NULL,
  alpha FLOAT64 NOT NULL,
  beta FLOAT64 NOT NULL,
  epsilon FLOAT64 NOT NULL,
  ucb_c FLOAT64 NOT NULL,
  base_cooldown_hours FLOAT64 NOT NULL,
  use_true_thompson BOOL NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

-- Seed ML params
MERGE `of-scheduler-proj.ops_config.ml_params_bandit` AS target
USING (
  SELECT 'high' AS tier, 1.0 AS alpha, 1.0 AS beta, 0.05 AS epsilon, 2.0 AS ucb_c, 24.0 AS base_cooldown_hours, FALSE AS use_true_thompson, CURRENT_TIMESTAMP() AS updated_at
  UNION ALL SELECT 'medium', 1.0, 1.0, 0.10, 1.5, 36.0, FALSE, CURRENT_TIMESTAMP()
  UNION ALL SELECT 'low', 1.0, 1.0, 0.15, 1.0, 48.0, FALSE, CURRENT_TIMESTAMP()
) AS source
ON target.tier = source.tier
WHEN MATCHED THEN UPDATE SET
  alpha = source.alpha,
  beta = source.beta,
  epsilon = source.epsilon,
  ucb_c = source.ucb_c,
  base_cooldown_hours = source.base_cooldown_hours,
  use_true_thompson = source.use_true_thompson,
  updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT ROW;

SELECT 'Configuration tables created and seeded' AS status;
SQL

echo ""
echo "3. Creating UDFs..."
bq query --use_legacy_sql=false --location=US << 'SQL'
-- Deterministic hash uniform function
CREATE OR REPLACE FUNCTION `of-scheduler-proj.layer_05_ml.fn_hash_uniform`(s STRING)
RETURNS FLOAT64
AS (
  LEAST(ABS(FARM_FINGERPRINT(s)) / 9223372036854775807.0, 1.0 - 1e-12)
);

-- Cooldown end calculation
CREATE OR REPLACE FUNCTION `of-scheduler-proj.layer_05_ml.fn_cooldown_end`(
  last_sent_ts TIMESTAMP,
  uses_today INT64,
  base_hours FLOAT64
)
RETURNS TIMESTAMP
AS (
  TIMESTAMP_ADD(last_sent_ts, INTERVAL CAST(base_hours * POW(1.5, uses_today) * 3600 AS INT64) SECOND)
);

-- UCB bonus calculation
CREATE OR REPLACE FUNCTION `of-scheduler-proj.layer_05_ml.fn_ucb_bonus`(
  total_trials INT64,
  item_trials INT64,
  c FLOAT64
)
RETURNS FLOAT64
AS (
  c * SQRT(SAFE.LN(GREATEST(total_trials, 1)) * 2.0 / GREATEST(item_trials, 1))
);

-- Beta sample function (deterministic)
CREATE OR REPLACE FUNCTION `of-scheduler-proj.layer_05_ml.fn_beta_sample`(
  alpha FLOAT64,
  beta FLOAT64,
  seed STRING,
  use_true_thompson BOOL
)
RETURNS FLOAT64
AS (
  CASE
    WHEN use_true_thompson THEN
      -- Placeholder for true Thompson sampling
      alpha / (alpha + beta) + 0.01 * `of-scheduler-proj.layer_05_ml.fn_hash_uniform`(seed)
    ELSE
      -- Deterministic fallback
      alpha / (alpha + beta) + 0.01 * `of-scheduler-proj.layer_05_ml.fn_hash_uniform`(seed)
  END
);

SELECT 'UDFs created successfully' AS status;
SQL

echo ""
echo "Script execution completed!"
echo "========================================="
