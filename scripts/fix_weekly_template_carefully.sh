#!/bin/bash

echo "========================================="
echo "CAREFULLY FIXING tvf_weekly_template"
echo "========================================="

# First, let's create the TVF with proper column alignment
bq query --use_legacy_sql=false --location=US << 'SQL'
-- Weekly template TVF with careful column alignment
CREATE OR REPLACE TABLE FUNCTION
`of-scheduler-proj.layer_07_export.tvf_weekly_template`(
  input_page_key STRING,
  week_start DATE,
  tz STRING,              -- Ignored, PR-1 is UTC-only
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
    JOIN `of-scheduler-proj.ops_config.tier_slot_packs` t
      ON c.tier = t.tier
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
      EXTRACT(DAYOFWEEK FROM DATE_ADD(week_start, INTERVAL day_offset DAY)) AS dow,
      tc.anchors_per_day,
      tc.supports_per_day,
      tc.min_spacing_minutes,
      tc.jitter_minutes
    FROM tier_config tc
    CROSS JOIN UNNEST(GENERATE_ARRAY(0, 6)) AS day_offset
  ),
  top_hours AS (
    SELECT
      ds.slot_date, 
      ds.dow, 
      dp.hour_utc, 
      dp.rps_lift, 
      dp.hour_rank_in_day, 
      ds.jitter_minutes,
      ds.min_spacing_minutes,
      ds.anchors_per_day,
      ds.supports_per_day
    FROM daily_schedule ds
    JOIN dow_profile dp
      ON ds.dow = dp.dow_utc
  ),
  -- First anchor: best hour of the day
  first_anchor AS (
    SELECT
      slot_date,
      'ANCHOR' AS slot_type,
      hour_utc AS slot_hour,
      CAST(FLOOR(`of-scheduler-proj.layer_05_ml.fn_hash_uniform`(
            CONCAT(input_page_key, ':', CAST(slot_date AS STRING), ':', CAST(hour_utc AS STRING), ':a1')
            ) * jitter_minutes) AS INT64) AS jitter_min,
      rps_lift
    FROM top_hours
    WHERE hour_rank_in_day = 1
  ),
  -- Second anchor candidates: must be at least 4h away from first anchor
  second_anchor_candidates AS (
    SELECT
      t.slot_date, 
      t.hour_utc, 
      t.rps_lift, 
      t.jitter_minutes, 
      t.hour_rank_in_day,
      -- Calculate cyclic distance from first anchor
      LEAST(
        ABS(t.hour_utc - f.slot_hour), 
        24 - ABS(t.hour_utc - f.slot_hour)
      ) AS cyclic_hour_dist_from_a1
    FROM top_hours t
    JOIN first_anchor f 
      ON t.slot_date = f.slot_date
    WHERE t.hour_rank_in_day > 1
      AND t.anchors_per_day >= 2  -- Only create second anchor if tier requires it
  ),
  second_anchor AS (
    SELECT
      slot_date,
      'ANCHOR' AS slot_type,
      hour_utc AS slot_hour,
      CAST(FLOOR(`of-scheduler-proj.layer_05_ml.fn_hash_uniform`(
            CONCAT(input_page_key, ':', CAST(slot_date AS STRING), ':', CAST(hour_utc AS STRING), ':a2')
            ) * jitter_minutes) AS INT64) AS jitter_min,
      rps_lift
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY slot_date ORDER BY hour_rank_in_day) AS rn
      FROM second_anchor_candidates
      WHERE cyclic_hour_dist_from_a1 >= 4   -- ≥4 hours apart
    )
    WHERE rn = 1
  ),
  -- Combine anchors (ensuring same column structure)
  anchor_slots AS (
    SELECT 
      slot_date,
      slot_type,
      slot_hour,
      jitter_min,
      rps_lift
    FROM first_anchor
    
    UNION ALL
    
    SELECT 
      slot_date,
      slot_type,
      slot_hour,
      jitter_min,
      rps_lift
    FROM second_anchor
  ),
  -- Support slots: hours after anchors, with deterministic jitter
  support_slots_raw AS (
    SELECT
      t.slot_date,
      'SUPPORT' AS slot_type,
      t.hour_utc AS slot_hour,
      CAST(FLOOR(`of-scheduler-proj.layer_05_ml.fn_hash_uniform`(
            CONCAT(input_page_key, ':', CAST(t.slot_date AS STRING), ':', CAST(t.hour_utc AS STRING), ':sup')
            ) * t.jitter_minutes) AS INT64) AS jitter_min,
      t.rps_lift,
      ROW_NUMBER() OVER (PARTITION BY t.slot_date ORDER BY t.rps_lift DESC) AS support_rank,
      t.min_spacing_minutes,
      t.supports_per_day
    FROM top_hours t
    WHERE t.hour_rank_in_day > t.anchors_per_day
  ),
  -- Filter supports: keep only top N and those that don't violate min spacing
  support_slots AS (
    SELECT 
      slot_date,
      slot_type,
      slot_hour,
      jitter_min,
      rps_lift
    FROM (
      SELECT
        s.slot_date,
        s.slot_type,
        s.slot_hour,
        s.jitter_min,
        s.rps_lift,
        s.support_rank,
        (s.slot_hour * 60 + s.jitter_min) AS s_min_of_day,
        s.min_spacing_minutes
      FROM support_slots_raw s
      WHERE s.support_rank <= s.supports_per_day
    ) s
    WHERE NOT EXISTS (
      SELECT 1
      FROM anchor_slots a
      WHERE a.slot_date = s.slot_date
        AND LEAST(
          ABS((s.slot_hour * 60 + s.jitter_min) - (a.slot_hour * 60 + a.jitter_min)),
          1440 - ABS((s.slot_hour * 60 + s.jitter_min) - (a.slot_hour * 60 + a.jitter_min))
        ) < s.min_spacing_minutes
    )
  ),
  -- Combine all slots (now with matching columns)
  all_slots AS (
    SELECT * FROM anchor_slots
    UNION ALL
    SELECT * FROM support_slots
  ),
  -- Get ranking candidates from the ML ranker
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
      NULL  -- Use default 7-day lookback
    )
  ),
  -- Assign candidates to slots with row number for limiting
  slotted_candidates AS (
    SELECT
      s.slot_date,
      s.slot_type,
      TIME(s.slot_hour, MOD(s.jitter_min, 60), 0) AS local_time,
      c.caption_id,
      c.final_score,
      ROW_NUMBER() OVER (
        PARTITION BY s.slot_date, s.slot_type
        ORDER BY c.final_score DESC, c.caption_id
      ) AS rn
    FROM all_slots s
    CROSS JOIN candidates c
  )
  -- Final aggregation with RN-based filtering
  SELECT
    slot_date,
    slot_type,
    local_time,
    ARRAY_AGG(
      STRUCT(caption_id, final_score)
      ORDER BY final_score DESC, caption_id
    ) AS caption_pool,
    STRUCT(
      0.0 AS baseline_mean,
      0.0 AS ucb_bonus,
      0.0 AS cooldown_penalty,
      0.0 AS novelty,
      'weekly_planner' AS mode_used,
      ARRAY<STRING>[] AS reason_codes
    ) AS score_card
  FROM slotted_candidates
  WHERE rn <= k_per_day  -- Filter BEFORE aggregation to avoid LIMIT issue
  GROUP BY slot_date, slot_type, local_time
);

SELECT 'tvf_weekly_template created successfully' AS status;
SQL

echo ""
echo "Testing the function..."
bq query --use_legacy_sql=false --location=US << 'SQL'
-- Quick test to verify it compiles and runs
SELECT 
  COUNT(*) as slot_count,
  COUNT(DISTINCT slot_date) as days,
  COUNT(DISTINCT slot_type) as slot_types
FROM `of-scheduler-proj.layer_07_export.tvf_weekly_template`(
  'test_page',
  DATE_TRUNC(CURRENT_DATE('UTC'), WEEK(MONDAY)),
  'UTC',
  3,
  NULL
)
LIMIT 1;
SQL

echo ""
echo "Completed!"
echo "========================================="
