CREATE OR REPLACE VIEW `of-scheduler-proj.mart.v_weekly_template_audit` AS
WITH q AS (
  SELECT * FROM `of-scheduler-proj.mart.v_daily_quota_policy_v3`
),
w AS (
  SELECT username_std, weight_volume, weight_price, weight_hours, exploration_rate, updated_at
  FROM `of-scheduler-proj.ops_config.page_personalization_weights`
),
dow AS (  -- total DOW score for context
  SELECT username_std, dow_local AS dow, SUM(score) AS dow_score
  FROM `of-scheduler-proj.mart.v_mm_dow_hod_180d_local_v2`
  GROUP BY username_std, dow_local
),
pp AS (
  SELECT username_std, p35, p50, p60, p80, p90, price_mode, corr_price_rev
  FROM `of-scheduler-proj.mart.v_mm_price_profile_90d_v2`
)
SELECT
  t.username_std,
  t.scheduler_name,
  t.date_local,
  MOD(EXTRACT(DAYOFWEEK FROM t.date_local) + 5, 7) AS dow,  -- 0=Mon..6=Sun
  q.ppv_quota,
  q.hour_pool,
  q.is_burst_dow,
  w.weight_volume, w.weight_hours, w.weight_price, w.exploration_rate,
  pp.p35, pp.p50, pp.p60, pp.p80, pp.p90, pp.price_mode, pp.corr_price_rev,
  d.dow_score,
  t.slot_rank,
  t.hod_local,
  t.price_usd,
  t.planned_local_datetime,
  t.scheduled_datetime_utc
FROM `of-scheduler-proj.mart.weekly_template_7d_latest` t
LEFT JOIN q  ON q.username_std = t.username_std
            AND q.dow = MOD(EXTRACT(DAYOFWEEK FROM t.date_local) + 5, 7)
LEFT JOIN w  ON w.username_std = t.username_std
LEFT JOIN pp ON pp.username_std = t.username_std
LEFT JOIN dow d ON d.username_std = t.username_std
               AND d.dow = MOD(EXTRACT(DAYOFWEEK FROM t.date_local) + 5, 7)
ORDER BY t.username_std, t.date_local, t.slot_rank
