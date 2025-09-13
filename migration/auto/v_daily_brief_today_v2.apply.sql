CREATE OR REPLACE VIEW `of-scheduler-proj.mart.v_daily_brief_today` AS
WITH hz AS (
  SELECT
    l.username_std,
    l.hod_local,
    -- time-decay weight: recent messages count more
    SUM(earnings_usd * EXP(-TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), l.sending_ts, DAY)/60.0)) AS score
  FROM `of-scheduler-proj.mart.v_messages_local_180d` l
  GROUP BY l.username_std, l.hod_local
),
best_hours AS (
  SELECT username_std, ARRAY_AGG(hod_local ORDER BY score DESC LIMIT 5) AS best_hours_local
  FROM hz
  GROUP BY username_std
),
price_band AS (
  SELECT
    username_std,
    APPROX_QUANTILES(price_usd, 20)[OFFSET(8)]  AS p25,
    APPROX_QUANTILES(price_usd, 20)[OFFSET(10)] AS p50,
    APPROX_QUANTILES(price_usd, 20)[OFFSET(14)] AS p75
  FROM `of-scheduler-proj.layer_04_semantic.v_message_facts_by_page`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
  GROUP BY username_std
),
recent_caption_use AS (
  SELECT username_std, caption_hash, MAX(DATE(sending_ts)) AS last_used_date
  FROM `of-scheduler-proj.layer_04_semantic.v_message_facts_by_page`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
  GROUP BY username_std, caption_hash
),
caption_perf AS (
  SELECT username_std, caption_hash,
         SUM(earnings_usd) AS cap_rev,
         COUNT(*)          AS cap_msgs
  FROM `of-scheduler-proj.layer_04_semantic.v_message_facts_by_page`
  WHERE caption_hash IS NOT NULL
    AND sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
  GROUP BY username_std, caption_hash
),
candidates AS (
  SELECT
    cd.username_std,
    cd.caption_id,
    cd.caption_hash,
    cd.caption_text,
    cd.caption_type,
    cd.explicitness,
    cd.theme_tags,
    COALESCE(cp.cap_rev, 0) AS hist_revenue,
    COALESCE(rcu.last_used_date, DATE '1900-01-01') AS last_used_date
  FROM `of-scheduler-proj.layer_04_semantic.v_caption_dim` cd
  LEFT JOIN caption_perf cp USING (username_std, caption_hash)
  LEFT JOIN recent_caption_use rcu USING (username_std, caption_hash)
),
top_captions AS (
  SELECT
    username_std,
    ARRAY_AGG(STRUCT(caption_id, caption_text, caption_type, explicitness, theme_tags, hist_revenue)
              ORDER BY (DATE_DIFF(CURRENT_DATE(), last_used_date, DAY) >= 28) DESC,
                       hist_revenue DESC
              LIMIT 10) AS caption_suggestions
  FROM candidates
  GROUP BY username_std
),
avoid_last7 AS (
  SELECT username_std, ARRAY_AGG(DISTINCT caption_hash) AS avoid_caption_hashes_7d
  FROM `of-scheduler-proj.layer_04_semantic.v_message_facts_by_page`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY username_std
)
SELECT
  p.username_std,
  p.assigned_scheduler,
  s.page_state,
  s.state_note,
  COALESCE(bh.best_hours_local, []) AS best_hours_local,
  STRUCT(pb.p25, pb.p50, pb.p75)     AS price_band_suggested,
  COALESCE(tc.caption_suggestions, []) AS caption_suggestions,
  COALESCE(a.avoid_caption_hashes_7d, []) AS avoid_caption_hashes_7d
FROM `of-scheduler-proj.layer_04_semantic.v_page_dim` p
LEFT JOIN best_hours  bh USING (username_std)
LEFT JOIN price_band  pb USING (username_std)
LEFT JOIN top_captions tc USING (username_std)
LEFT JOIN avoid_last7 a  USING (username_std)
LEFT JOIN `of-scheduler-proj.ops_config.page_state` s USING (username_std)
