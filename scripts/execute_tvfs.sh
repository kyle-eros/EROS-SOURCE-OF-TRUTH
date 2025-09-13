#!/bin/bash

echo "========================================="
echo "CREATING TABLE FUNCTIONS (FIXED)"
echo "========================================="

echo ""
echo "1. Creating tvf_rank_captions..."
bq query --use_legacy_sql=false --location=US << 'SQL'
-- Ranking TVF (fixed LIMIT issue)
CREATE OR REPLACE TABLE FUNCTION `of-scheduler-proj.layer_05_ml.tvf_rank_captions`(
  page_key STRING,
  now_ts TIMESTAMP,
  k INT64,
  mode STRING,
  p_opts STRUCT<min_date DATE, seed_scope STRING>
)
AS (
  WITH config AS (
    SELECT
      ff.is_enabled AS enable_thompson,
      COALESCE(p_opts.min_date, DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AS min_date,
      COALESCE(p_opts.seed_scope, CONCAT(page_key, ':', FORMAT_TIMESTAMP('%Y-%m-%dT%H', now_ts))) AS seed_scope
    FROM `of-scheduler-proj.ops_config.feature_flags` ff
    WHERE ff.flag_name = 'enable_thompson_true'
  ),
  base AS (
    SELECT
      r.page_key,
      r.caption_id,
      r.performance_features.rps_smoothed AS baseline_mean,
      r.performance_features.sends_30d AS trials,
      r.performance_features.revenue_30d AS successes,
      r.temporal_features.last_used_timestamp,
      r.temporal_features.hours_since_use,
      r.cooldown_features.is_eligible,
      r.alpha,
      r.beta,
      r.epsilon,
      r.ucb_c,
      r.base_cooldown_hours,
      r.use_true_thompson,
      r.uses_today,
      c.enable_thompson,
      c.seed_scope
    FROM `of-scheduler-proj.layer_05_ml.v_rank_ready` r
    CROSS JOIN config c
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
      CASE mode
        WHEN 'ucb' THEN 
          baseline_mean + `of-scheduler-proj.layer_05_ml.fn_ucb_bonus`(
            SUM(trials) OVER (), trials, ucb_c
          )
        WHEN 'epsilon_greedy' THEN
          CASE 
            WHEN `of-scheduler-proj.layer_05_ml.fn_hash_uniform`(
              CONCAT(seed_scope, ':', caption_id)
            ) < epsilon THEN
              `of-scheduler-proj.layer_05_ml.fn_hash_uniform`(
                CONCAT(seed_scope, ':', caption_id, ':explore')
              )
            ELSE baseline_mean
          END
        WHEN 'thompson' THEN
          `of-scheduler-proj.layer_05_ml.fn_beta_sample`(
            alpha + successes,
            beta + trials - successes,
            CONCAT(seed_scope, ':', caption_id),
            use_true_thompson AND enable_thompson
          )
        ELSE baseline_mean
      END AS final_score,
      `of-scheduler-proj.layer_05_ml.fn_ucb_bonus`(
        SUM(trials) OVER (), trials, ucb_c
      ) AS ucb_bonus,
      0.0 AS cooldown_penalty,
      0.0 AS novelty,
      mode AS mode_used
    FROM base
  ),
  ranked AS (
    SELECT
      page_key,
      caption_id,
      final_score,
      STRUCT(
        baseline_mean,
        ucb_bonus,
        cooldown_penalty,
        novelty,
        mode_used,
        ARRAY<STRING>[] AS reason_codes
      ) AS score_card,
      ROW_NUMBER() OVER (ORDER BY final_score DESC, caption_id) AS rn
    FROM scored
  )
  SELECT
    page_key,
    caption_id,
    final_score,
    score_card
  FROM ranked
  WHERE rn <= k
);

SELECT 'tvf_rank_captions created' AS status;
SQL

echo ""
echo "2. Creating tvf_weekly_template..."
bq query --use_legacy_sql=false --location=US << 'SQL'
-- Weekly template TVF (fixed ambiguous column)
CREATE OR REPLACE TABLE FUNCTION `of-scheduler-proj.layer_07_export.tvf_weekly_template`(
  input_page_key STRING,
  week_start DATE,
  tz STRING,  -- Ignored, always use UTC
  k_per_day INT64,
  p_opts STRUCT<risk_override INT64, force_tier STRING>
)
AS (
  WITH config AS (
    SELECT
      COALESCE(p_opts.force_tier, i.recommended_tier) AS tier,
      COALESCE(p_opts.risk_override, 0) AS risk_override,
      i.recommended_tier,
      b.scam_risk_score,
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
      c.scam_risk_score,
      c.night_owl_idx
    FROM config c
    JOIN `of-scheduler-proj.ops_config.tier_slot_packs` t ON c.tier = t.tier
  ),
  dow_profile AS (
    SELECT
      dow_utc,
      hour_utc,
      rps_lift,
      hour_rank_in_day
    FROM `of-scheduler-proj.layer_04_semantic.v_page_dow_hod_profile_90d`
    WHERE page_key = input_page_key
  ),
  daily_schedule AS (
    SELECT
      DATE_ADD(week_start, INTERVAL day_offset DAY) AS slot_date,
      day_offset,
      EXTRACT(DAYOFWEEK FROM DATE_ADD(week_start, INTERVAL day_offset DAY)) AS dow,
      tc.anchors_per_day,
      tc.supports_per_day,
      tc.min_spacing_minutes,
      tc.jitter_minutes,
      tc.scam_risk_score,
      tc.night_owl_idx
    FROM tier_config tc
    CROSS JOIN UNNEST(GENERATE_ARRAY(0, 6)) AS day_offset
  ),
  anchor_slots AS (
    SELECT
      ds.slot_date,
      'ANCHOR' AS slot_type,
      dp.hour_utc AS slot_hour,
      CAST(dp.hour_utc * 60 + 
        MOD(ABS(FARM_FINGERPRINT(CONCAT(input_page_key, ':', CAST(ds.slot_date AS STRING), ':', CAST(dp.hour_utc AS STRING)))), 
            ds.jitter_minutes) AS INT64) AS slot_minutes,
      dp.rps_lift,
      ROW_NUMBER() OVER (PARTITION BY ds.slot_date ORDER BY dp.hour_rank_in_day) AS anchor_rank
    FROM daily_schedule ds
    JOIN dow_profile dp ON ds.dow = dp.dow_utc
    WHERE dp.hour_rank_in_day <= ds.anchors_per_day
  ),
  support_slots AS (
    SELECT
      ds.slot_date,
      'SUPPORT' AS slot_type,
      dp.hour_utc AS slot_hour,
      CAST(dp.hour_utc * 60 + 
        MOD(ABS(FARM_FINGERPRINT(CONCAT(input_page_key, ':', CAST(ds.slot_date AS STRING), ':', CAST(dp.hour_utc AS STRING), ':support'))), 
            ds.jitter_minutes) AS INT64) AS slot_minutes,
      dp.rps_lift,
      ROW_NUMBER() OVER (PARTITION BY ds.slot_date ORDER BY dp.rps_lift DESC) AS support_rank
    FROM daily_schedule ds
    JOIN dow_profile dp ON ds.dow = dp.dow_utc
    WHERE dp.hour_rank_in_day > ds.anchors_per_day
      AND dp.hour_rank_in_day <= ds.anchors_per_day + ds.supports_per_day
  ),
  all_slots AS (
    SELECT * FROM anchor_slots
    UNION ALL
    SELECT * FROM support_slots WHERE support_rank <= 3
  ),
  candidates AS (
    SELECT
      page_key,
      caption_id,
      final_score,
      score_card
    FROM `of-scheduler-proj.layer_05_ml.tvf_rank_captions`(
      input_page_key,
      TIMESTAMP(week_start),
      k_per_day * 7,
      'ucb',
      NULL
    )
  )
  SELECT
    s.slot_date,
    s.slot_type,
    TIME(s.slot_hour, MOD(s.slot_minutes, 60), 0) AS local_time,
    ARRAY_AGG(
      STRUCT(c.caption_id, c.final_score)
      ORDER BY c.final_score DESC
      LIMIT k_per_day
    ) AS caption_pool,
    STRUCT(
      0.0 AS baseline_mean,
      0.0 AS ucb_bonus,
      0.0 AS cooldown_penalty,
      0.0 AS novelty,
      'weekly_planner' AS mode_used,
      ARRAY<STRING>[] AS reason_codes
    ) AS score_card
  FROM all_slots s
  CROSS JOIN candidates c
  GROUP BY 1, 2, 3
);

SELECT 'tvf_weekly_template created' AS status;
SQL

echo ""
echo "TVF creation completed!"
echo "========================================="
