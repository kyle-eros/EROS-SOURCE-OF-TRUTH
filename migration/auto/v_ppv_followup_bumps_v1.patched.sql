WITH cfg AS (
  SELECT
    CAST(COALESCE(MAX(IF(setting_key = 'randomize_offset_minutes', setting_val, NULL)), '45') AS INT64) AS jitter_m
  FROM `of-scheduler-proj.ops_config.settings_modeling`
),
ppv AS (
  SELECT t.username_std, t.tz, t.date_local, t.slot_rank, t.hod_local, t.price_usd
  FROM `of-scheduler-proj.mart.weekly_template_7d_latest` t
  WHERE t.price_usd > 0
),
level AS (
  SELECT
    p.*,
    CASE
      WHEN p.price_usd >= COALESCE(pr.p90, p.price_usd) THEN 'premium'
      WHEN p.price_usd >= COALESCE(pr.p50, p.price_usd) THEN 'mid'
      ELSE 'teaser'
    END AS ppv_level
  FROM ppv p
  LEFT JOIN `of-scheduler-proj.mart.v_mm_price_profile_90d_v2` pr USING (username_std)
),
rules AS (
  -- order_i = 0 are "pre" bumps; positive order_i are follow-ups
  SELECT 'teaser'   AS lvl, 0 AS order_i, -9999 AS min_off, -9999 AS max_off, FALSE AS is_real  -- no pre for teaser
  UNION ALL SELECT 'teaser', 1,  20,  45, TRUE
  UNION ALL SELECT 'mid',    0, -20, -15, TRUE
  UNION ALL SELECT 'mid',    1,  15,  20, TRUE
  UNION ALL SELECT 'mid',    2,  45,  45, TRUE
  UNION ALL SELECT 'premium',0, -20, -15, TRUE
  UNION ALL SELECT 'premium',1,  20,  30, TRUE
  UNION ALL SELECT 'premium',2,  40,  55, TRUE
),
base AS (
  SELECT
    l.username_std,
    l.tz,
    l.date_local,
    l.slot_rank,
    l.hod_local,
    l.ppv_level,
    r.order_i,
    r.is_real,
    DATETIME(l.date_local, TIME(l.hod_local, 0, 0)) AS base_slot_dt_local,

    -- Deterministic pick in [min_off, max_off] using a stable key
    CAST(ROUND(
      r.min_off + MOD(
        ABS(FARM_FINGERPRINT(CONCAT(
          CAST(l.username_std AS STRING),'|',
          CAST(l.date_local   AS STRING),'|',
          CAST(l.slot_rank    AS STRING),'|',
          CAST(r.order_i      AS STRING)
        ))),
        (r.max_off - r.min_off + 1)
      )
    ) AS INT64) AS picked_min
  FROM level l
  JOIN rules r ON r.lvl = l.ppv_level
  WHERE r.is_real = TRUE
),
expanded AS (
  SELECT
    b.*,
    DATETIME_ADD(b.base_slot_dt_local, INTERVAL b.picked_min MINUTE) AS planned_dt_local
  FROM base b
),
jittered AS (
  SELECT
    e.*,
    -- Deterministic jitter in [-jitter_m, +jitter_m] using a separate key namespace ("|J")
    DATETIME_ADD(
      e.planned_dt_local,
      INTERVAL CAST(
        MOD(
          ABS(FARM_FINGERPRINT(CONCAT(
            CAST(e.username_std AS STRING),'|',
            CAST(e.date_local   AS STRING),'|',
            CAST(e.slot_rank    AS STRING),'|',
            CAST(e.order_i      AS STRING),'|','J'
          ))),
          (2 * (SELECT jitter_m FROM cfg) + 1)
        ) - (SELECT jitter_m FROM cfg) AS INT64
      ) MINUTE
    ) AS planned_dt_local_j
  FROM expanded e
)
SELECT
  j.username_std,
  j.date_local,
  j.slot_rank,
  j.ppv_level AS ppv_level,
  CASE WHEN j.order_i = 0 AND j.ppv_level IN ('mid','premium') THEN 'ppv_pre_teaser'
       ELSE 'ppv_followup' END AS activity_type,
  EXTRACT(HOUR   FROM j.planned_dt_local_j) AS hod_local,
  EXTRACT(MINUTE FROM j.planned_dt_local_j) AS minute_local,
  j.planned_dt_local_j AS planned_local_datetime,
  TIMESTAMP(j.planned_dt_local_j, pd.tz)    AS scheduled_datetime_utc
FROM jittered j
JOIN `of-scheduler-proj.layer_04_semantic.v_page_dim` pd USING (username_std)
