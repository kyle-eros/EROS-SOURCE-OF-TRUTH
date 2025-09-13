#!/bin/bash

echo "========================================="
echo "FIXING TVFs WITH CORRECT PARAMETERS"
echo "========================================="

echo ""
echo "1. Fixing fn_hash_uniform to match expected signature..."
bq query --use_legacy_sql=false --location=US << 'SQL'
-- Fix fn_hash_uniform signature
CREATE OR REPLACE FUNCTION `of-scheduler-proj.layer_05_ml.fn_hash_uniform`(s STRING)
RETURNS FLOAT64
AS (
  LEAST(ABS(FARM_FINGERPRINT(s)) / 9223372036854775807.0, 1.0 - 1e-12)
);

-- Fix fn_beta_sample signature
CREATE OR REPLACE FUNCTION `of-scheduler-proj.layer_05_ml.fn_beta_sample`(
  alpha FLOAT64,
  beta FLOAT64,
  seed STRING
)
RETURNS FLOAT64
AS (
  -- Deterministic fallback (no true Thompson in PR-1)
  alpha / (alpha + beta) + 0.01 * `of-scheduler-proj.layer_05_ml.fn_hash_uniform`(seed)
);

SELECT 'UDFs fixed' AS status;
SQL

echo ""
echo "2. Creating fixed tvf_rank_captions..."
bq query --use_legacy_sql=false --location=US << 'SQL'
CREATE OR REPLACE TABLE FUNCTION `of-scheduler-proj.layer_05_ml.tvf_rank_captions`(
  page_key STRING,
  now_ts TIMESTAMP,
  k INT64,
  mode STRING,
  p_opts STRUCT<min_date DATE, seed_scope STRING>
)
AS (
  WITH cfg AS (
    SELECT
      ff.is_enabled AS enable_thompson,
      CASE
        WHEN p_opts IS NULL THEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 7 DAY)
        ELSE p_opts.min_date
      END AS min_date,
      COALESCE(
        CASE WHEN p_opts IS NULL THEN NULL ELSE p_opts.seed_scope END,
        CONCAT(page_key, ':', FORMAT_TIMESTAMP('%Y-%m-%dT%H', now_ts))
      ) AS seed_scope
    FROM `of-scheduler-proj.ops_config.feature_flags` ff
    WHERE ff.flag_name = 'enable_thompson_true'
  ),
  base AS (
    SELECT
      r.page_key,
      r.caption_id,
      r.performance_features.rps_smoothed AS baseline_mean,
      r.performance_features.sends_30d AS trials,
      CAST(ROUND(COALESCE(r.performance_features.purchase_rate_30d, 0.0) * COALESCE(r.performance_features.sends_30d,0))) AS INT64 AS successes,
      r.temporal_features.last_used_timestamp,
      r.temporal_features.hours_since_use,
      r.cooldown_features.is_eligible,
      r.alpha, r.beta, r.epsilon, r.ucb_c, r.base_cooldown_hours, r.use_true_thompson,
      r.uses_today,
      c.enable_thompson,
      c.seed_scope
    FROM `of-scheduler-proj.layer_05_ml.v_rank_ready` r
    CROSS JOIN cfg c
    WHERE r.page_key = page_key
      AND r.computed_date >= c.min_date
      AND r.cooldown_features.is_eligible = TRUE
  ),
  scored AS (
    SELECT
      page_key,
      caption_id,
      baseline_mean,
      trials,
      successes,
      CASE mode
        WHEN 'ucb' THEN
          baseline_mean + `of-scheduler-proj.layer_05_ml.fn_ucb_bonus`(SUM(trials) OVER (), trials, ucb_c)
        WHEN 'epsilon_greedy' THEN
          CASE
            WHEN `of-scheduler-proj.layer_05_ml.fn_hash_uniform`(CONCAT(seed_scope, ':', caption_id)) < epsilon
            THEN `of-scheduler-proj.layer_05_ml.fn_hash_uniform`(CONCAT(seed_scope, ':', caption_id, ':explore'))
            ELSE baseline_mean
          END
        WHEN 'thompson' THEN
          `of-scheduler-proj.layer_05_ml.fn_beta_sample`(
            alpha + successes,
            beta + trials - successes,
            CONCAT(seed_scope, ':', caption_id)
          )
        ELSE baseline_mean
      END AS final_score,
      `of-scheduler-proj.layer_05_ml.fn_ucb_bonus`(SUM(trials) OVER (), trials, ucb_c) AS ucb_bonus,
      0.0 AS cooldown_penalty,
      0.0 AS novelty,
      mode AS mode_used
    FROM base
  ),
  ranked AS (
    SELECT
      page_key, caption_id, final_score,
      STRUCT(baseline_mean, ucb_bonus, cooldown_penalty, novelty, mode_used, ARRAY<STRING>[] AS reason_codes) AS score_card,
      ROW_NUMBER() OVER (ORDER BY final_score DESC, caption_id) AS rn
    FROM scored
  )
  SELECT page_key, caption_id, final_score, score_card
  FROM ranked
  WHERE rn <= k
);

SELECT 'tvf_rank_captions fixed' AS status;
SQL

echo ""
echo "3. Creating fixed tvf_weekly_template..."
bq query --use_legacy_sql=false --location=US << 'SQL'
CREATE OR REPLACE TABLE FUNCTION `of-scheduler-proj.layer_07_export.tvf_weekly_template`(
  input_page_key STRING,
  week_start DATE,
  tz STRING,          -- ignored (UTC only in PR-1)
  k_per_day INT64,
  p_opts STRUCT<risk_override INT64, force_tier STRING>
)
AS (
  WITH cfg AS (
    SELECT
      COALESCE(p_opts.force_tier, i.recommended_tier) AS tier,
      COALESCE(p_opts.risk_override, 0) AS risk_override,
      i.recommended_tier,
      b.or_slump_pct,
      b.night_owl_idx
    FROM `of-scheduler-proj.layer_04_semantic.v_page_intensity_7d` i
    LEFT JOIN `of-scheduler-proj.layer_04_semantic.v_page_behavior_28d` b
      ON i.page_key = b.page_key
    WHERE i.page_key = input_page_key
  ),
  tier_config AS (
    SELECT
      t.anchors_per_day,
      t.supports_per_day,
      t.min_spacing_minutes,
      t.jitter_minutes,
      c.tier,
      c.or_slump_pct,
      c.night_owl_idx
    FROM cfg c
    JOIN `of-scheduler-proj.ops_config.tier_slot_packs` t ON c.tier = t.tier
  ),
  dow_profile AS (
    SELECT dow_utc, hour_utc, rps_lift, hour_rank_in_day
    FROM `of-scheduler-proj.layer_04_semantic.v_page_dow_hod_profile_90d`
    WHERE page_key = input_page_key
  ),
  daily_schedule AS (
    SELECT
      DATE_ADD(week_start, INTERVAL day_offset DAY) AS slot_date,
      EXTRACT(DAYOFWEEK FROM DATE_ADD(week_start, INTERVAL day_offset DAY)) AS dow,
      tc.anchors_per_day, tc.supports_per_day, tc.min_spacing_minutes, tc.jitter_minutes
    FROM tier_config tc
    CROSS JOIN UNNEST(GENERATE_ARRAY(0, 6)) AS day_offset
  ),
  -- Simplified slot generation
  all_slots AS (
    SELECT
      ds.slot_date,
      CASE 
        WHEN dp.hour_rank_in_day <= ds.anchors_per_day THEN 'ANCHOR'
        ELSE 'SUPPORT'
      END AS slot_type,
      dp.hour_utc AS slot_hour,
      CAST(FLOOR(`of-scheduler-proj.layer_05_ml.fn_hash_uniform`(
        CONCAT(input_page_key, ':', CAST(ds.slot_date AS STRING), ':', CAST(dp.hour_utc AS STRING))
      ) * ds.jitter_minutes) AS INT64) AS jitter_min,
      dp.rps_lift,
      dp.hour_rank_in_day
    FROM daily_schedule ds
    JOIN dow_profile dp ON ds.dow = dp.dow_utc
    WHERE dp.hour_rank_in_day <= ds.anchors_per_day + ds.supports_per_day
  ),
  candidates AS (
    SELECT page_key, caption_id, final_score, score_card
    FROM `of-scheduler-proj.layer_05_ml.tvf_rank_captions`(
      input_page_key,
      TIMESTAMP(week_start),
      k_per_day * 7,
      'ucb',
      NULL
    )
  ),
  slotted_candidates AS (
    SELECT
      s.slot_date, s.slot_type,
      TIME(s.slot_hour, MOD(s.jitter_min, 60), 0) AS local_time,
      c.caption_id, c.final_score,
      ROW_NUMBER() OVER (PARTITION BY s.slot_date, s.slot_type ORDER BY c.final_score DESC, c.caption_id) AS rn
    FROM all_slots s
    CROSS JOIN candidates c
  )
SELECT
  slot_date,
  slot_type,
  local_time,
  ARRAY_AGG(STRUCT(caption_id, final_score) ORDER BY final_score DESC, caption_id) AS caption_pool,
  STRUCT(0.0 AS baseline_mean, 0.0 AS ucb_bonus, 0.0 AS cooldown_penalty, 0.0 AS novelty, 'weekly_planner' AS mode_used, ARRAY<STRING>[] AS reason_codes) AS score_card
FROM slotted_candidates
WHERE rn <= k_per_day
GROUP BY 1,2,3
);

SELECT 'tvf_weekly_template fixed' AS status;
SQL

echo ""
echo "TVF fixes completed!"
echo "========================================="
