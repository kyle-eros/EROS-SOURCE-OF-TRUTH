WITH days_to_check AS (           -- the page-days we actually plan
  SELECT DISTINCT username_std, date_local
  FROM `of-scheduler-proj.mart.weekly_template_7d_latest`
),

-- clamp + swap windows once, using the same rules as the planner
pd0 AS (
  SELECT
    username_std,
    CASE WHEN min_hod IS NULL AND max_hod IS NULL THEN 0
         WHEN min_hod IS NULL THEN 0
         ELSE GREATEST(0, LEAST(23, CAST(min_hod AS INT64))) END AS min0,
    CASE WHEN min_hod IS NULL AND max_hod IS NULL THEN 23
         WHEN max_hod IS NULL THEN 23
         ELSE GREATEST(0, LEAST(23, CAST(max_hod AS INT64))) END AS max0
  FROM `of-scheduler-proj.layer_04_semantic.v_page_dim`
  WHERE COALESCE(LOWER(CAST(is_active AS STRING)) IN ('true','t','1','yes','y'), TRUE)
),
pd AS (
  SELECT
    username_std,
    CASE WHEN min0 IS NOT NULL AND max0 IS NOT NULL AND min0 > max0 THEN max0 ELSE min0 END AS min_hod_eff,
    CASE WHEN min0 IS NOT NULL AND max0 IS NOT NULL AND min0 > max0 THEN min0 ELSE max0 END AS max_hod_eff
  FROM pd0
),

-- policy quota by DOW (0=Mon..6=Sun like the planner)
policy AS (
  SELECT username_std, dow, ppv_quota
  FROM `of-scheduler-proj.mart.v_daily_quota_policy_v3`
),

base AS (
  SELECT
    d.username_std,
    d.date_local,
    p.min_hod_eff AS min_h,
    p.max_hod_eff AS max_h,
    q.ppv_quota   AS quota,
    MOD(EXTRACT(DAYOFWEEK FROM d.date_local) + 5, 7) AS dow
  FROM days_to_check d
  JOIN pd p USING (username_std)
  LEFT JOIN policy q
    ON q.username_std = d.username_std
   AND q.dow         = MOD(EXTRACT(DAYOFWEEK FROM d.date_local) + 5, 7)
),

calc AS (
  SELECT
    username_std,
    date_local,
    dow,
    quota,
    min_h,
    max_h,
    (max_h - min_h) AS window_width,
    GREATEST(0, 2 * (COALESCE(quota, 0) - 1)) AS width_needed_2h
  FROM base
)

SELECT *
FROM calc
WHERE quota IS NOT NULL
  AND window_width < width_needed_2h   -- impossible to satisfy ≥2h with this quota/window
ORDER BY username_std, date_local
