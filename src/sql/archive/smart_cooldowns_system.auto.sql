-- EROS Smart Cooldowns (auto-clean)
-- Keys: caption_hash, username_page
-- NOTE: Alias caption_hash AS caption_id for Apps Script compatibility.

CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_smart_cooldowns_v1` AS
WITH base AS (
  SELECT
    c.caption_hash,
    r.username_page,
    c.explicitness,
    f.fatigue_score,
    COALESCE(r.rps_eb_price, 0) AS rps,
    r.score_final,
    r.slot_dt_local,
    lu.last_used_ts,
    COUNT(DISTINCT r.username_page) OVER (PARTITION BY c.caption_hash) AS pages_using_caption,
    COALESCE(s.engagement_prediction_score, 0.5) AS engagement_score,
    -- v_page_engagement_patterns_v1 is keyed by username_std; derive std from username_page by stripping any "__suffix"
    COALESCE(ep.volume_tier, 'MINIMAL_VOLUME') AS page_volume_tier
  FROM `of-scheduler-proj.core.caption_dim` c
  JOIN `of-scheduler-proj.mart.caption_rank_next24_v3_tbl` r
    ON r.caption_hash = c.caption_hash
  LEFT JOIN `of-scheduler-proj.core.caption_fatigue_scores_v1` f
    ON f.caption_hash = c.caption_hash AND f.username_page = r.username_page
  LEFT JOIN `of-scheduler-proj.core.v_caption_last_used_v3` lu
    ON lu.caption_hash = c.caption_hash AND lu.username_page = r.username_page
  LEFT JOIN `of-scheduler-proj.core.v_caption_sentiment_v1` s
    ON s.caption_hash = c.caption_hash
  /* If engagement patterns view exists it will join, else column stays default. */
  LEFT JOIN `of-scheduler-proj.core.v_page_engagement_patterns_v1` ep
    ON ep.username_std = REGEXP_EXTRACT(r.username_page, '^(.*?)(?:__|$)')
),
quart AS (
  SELECT
    APPROX_QUANTILES(rps, 100)[OFFSET(75)] AS rps_p75,
    APPROX_QUANTILES(rps, 100)[OFFSET(90)] AS rps_p90,
    APPROX_QUANTILES(score_final, 100)[OFFSET(75)] AS score_p75
  FROM base
),
calc AS (
  SELECT
    b.*,
    q.rps_p75, q.rps_p90, q.score_p75,
    AVG(b.score_final) OVER (
      PARTITION BY b.caption_hash
      ORDER BY b.slot_dt_local
      ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_score,
    CASE
      WHEN b.rps > q.rps_p90 THEN 2
      WHEN b.rps > q.rps_p75 THEN 3
      WHEN b.explicitness = 'explicit'  THEN 10
      WHEN b.explicitness = 'moderate'  THEN 7
      WHEN b.explicitness IN ('mild','gfe-implied') THEN 5
      ELSE 5
    END AS base_cooldown_days,
    CASE
      WHEN COALESCE(b.fatigue_score,0) >= 0.8 THEN 1.8
      WHEN COALESCE(b.fatigue_score,0) >= 0.5 THEN 1.4
      WHEN COALESCE(b.fatigue_score,0) >= 0.3 THEN 1.2
      ELSE 1.0
    END AS fatigue_multiplier,
    CASE
      WHEN b.pages_using_caption > 5 THEN 1.5
      WHEN b.pages_using_caption > 3 THEN 1.3
      WHEN b.pages_using_caption > 1 THEN 1.1
      ELSE 1.0
    END AS cross_page_multiplier,
    CASE
      WHEN b.page_volume_tier = 'HIGH_VOLUME'   THEN 0.8
      WHEN b.page_volume_tier = 'MEDIUM_VOLUME' THEN 0.9
      WHEN b.page_volume_tier = 'LOW_VOLUME'    THEN 1.0
      ELSE 1.2
    END AS volume_adjustment
  FROM base b
  CROSS JOIN quart q
)
SELECT
  caption_hash AS caption_id,      -- back-compat for JS
  caption_hash,
  username_page,
  explicitness,
  fatigue_score,
  last_used_ts,
  pages_using_caption,
  rolling_7d_score,
  engagement_score,
  page_volume_tier,
  base_cooldown_days,
  fatigue_multiplier,
  cross_page_multiplier,
  volume_adjustment,
  GREATEST(
    CAST(base_cooldown_days * fatigue_multiplier * cross_page_multiplier * volume_adjustment AS INT64),
    1
  ) AS final_cooldown_days,
  CASE
    WHEN last_used_ts IS NULL THEN CURRENT_TIMESTAMP()
    ELSE TIMESTAMP_ADD(
      last_used_ts,
      INTERVAL CAST(base_cooldown_days * fatigue_multiplier * cross_page_multiplier * volume_adjustment AS INT64) DAY
    )
  END AS available_after,
  (rolling_7d_score > score_p75 AND COALESCE(fatigue_score,0) < 0.3 AND engagement_score > 0.6)
    AS cooldown_override_suggested,
  (rolling_7d_score > score_p75 AND rps > rps_p75 AND COALESCE(fatigue_score,0) < 0.5)
    AS emergency_override_eligible,
  CASE
    WHEN rps > rps_p90 THEN 'ELITE'
    WHEN rps > rps_p75 THEN 'HIGH_PERFORMER'
    WHEN rps > 0      THEN 'AVERAGE'
    ELSE 'UNTESTED'
  END AS performance_tier,
  CURRENT_TIMESTAMP() AS calculated_at
FROM calc
;

-- Helper functions
CREATE OR REPLACE FUNCTION `of-scheduler-proj.core.is_caption_available`(
  caption_key STRING,
  username_page_key STRING
) AS (
  EXISTS (
    SELECT 1 FROM `of-scheduler-proj.core.v_smart_cooldowns_v1`
    WHERE caption_hash = caption_key
      AND username_page = username_page_key
      AND available_after <= CURRENT_TIMESTAMP()
  )
);

CREATE OR REPLACE FUNCTION `of-scheduler-proj.core.get_caption_available_time`(
  caption_key STRING,
  username_page_key STRING
) AS (
  (SELECT available_after
   FROM `of-scheduler-proj.core.v_smart_cooldowns_v1`
   WHERE caption_hash = caption_key
     AND username_page = username_page_key
   LIMIT 1)
);