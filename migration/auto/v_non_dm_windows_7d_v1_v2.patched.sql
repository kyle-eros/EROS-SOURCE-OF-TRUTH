WITH cfg AS (
  SELECT
    -- CSVs from settings
    (SELECT setting_val FROM `of-scheduler-proj.ops_config.settings_modeling`
     WHERE setting_key='drip_slots_csv'        LIMIT 1) AS drip_csv,
    (SELECT setting_val FROM `of-scheduler-proj.ops_config.settings_modeling`
     WHERE setting_key='renewal_times_csv'     LIMIT 1) AS renew_csv,
    (SELECT setting_val FROM `of-scheduler-proj.ops_config.settings_modeling`
     WHERE setting_key='link_drop_times_csv'   LIMIT 1) AS link_csv,
    CAST((SELECT setting_val FROM `of-scheduler-proj.ops_config.settings_modeling`
          WHERE setting_key='randomize_offset_minutes' LIMIT 1) AS INT64) AS rand_off
),
pages AS (
  SELECT username_std, COALESCE(tz,'UTC') AS tz
  FROM `of-scheduler-proj.layer_04_semantic.v_page_dim`
  WHERE COALESCE(LOWER(CAST(is_active AS STRING)) IN ('true','t','1','yes','y'), TRUE)
),
days AS (
  SELECT p.username_std, p.tz,
         DATE_ADD(CURRENT_DATE(p.tz), INTERVAL d DAY) AS d,
         d AS day_idx
  FROM pages p
  CROSS JOIN UNNEST(GENERATE_ARRAY(0,6)) AS d
),
-- mark paid (renewals only for these)
paid AS (
  SELECT username_std, COALESCE(renew_on_pct,0) > 0 AS is_paid
  FROM `of-scheduler-proj.layer_02_staging.creator_stats_latest`
),

/* -------- DripSet: "HH:MM|TYPE" where TYPE in {MM, Wall} -------- */
drip_tokens AS (
  SELECT
    dt.username_std, dt.tz, dt.d, dt.day_idx,
    SPLIT(tok, '|')[OFFSET(0)] AS hhmm,
    SPLIT(tok, '|')[OFFSET(1)] AS ch   -- 'MM' or 'Wall'
  FROM days dt, cfg, UNNEST(SPLIT(cfg.drip_csv, ',')) AS tok
),
drip AS (
  SELECT
    username_std,
    -- local DATETIME (no tz arg)
    DATETIME(d, PARSE_TIME('%H:%M', hhmm)) AS base_dt_local,
    ch AS channel,  -- 'MM' or 'Wall'
    -- deterministic jitter in [-rand_off, +rand_off]
    CAST(
      MOD(ABS(FARM_FINGERPRINT(CONCAT(username_std,'|',CAST(d AS STRING),'|DRIP|',hhmm,'|',ch))),
          2*(SELECT rand_off FROM cfg)+1
      ) - (SELECT rand_off FROM cfg)
      AS INT64
    ) AS minute_jitter,
    tz
  FROM drip_tokens
),

/* -------- Renewals: paid pages only, times listed in renew_csv -------- */
renew_tokens AS (
  SELECT dt.username_std, dt.tz, dt.d, dt.day_idx, tok AS hhmm
  FROM days dt, cfg, UNNEST(SPLIT(cfg.renew_csv, ',')) AS tok
  JOIN paid p USING (username_std)
  WHERE p.is_paid = TRUE
),
renew AS (
  SELECT
    username_std,
    DATETIME(d, PARSE_TIME('%H:%M', hhmm)) AS base_dt_local,
    'Renewal' AS channel,
    CAST(
      MOD(ABS(FARM_FINGERPRINT(CONCAT(username_std,'|',CAST(d AS STRING),'|RENEW|',hhmm))),
          2*(SELECT rand_off FROM cfg)+1
      ) - (SELECT rand_off FROM cfg)
      AS INT64
    ) AS minute_jitter,
    tz
  FROM renew_tokens
),

/* -------- Link drops: windows like "HH:MM-HH:MM" → pick a minute inside -------- */
link_tokens AS (
  SELECT
    dt.username_std, dt.tz, dt.d, dt.day_idx,
    SPLIT(tok, '-')[OFFSET(0)] AS hhmm_start,
    SPLIT(tok, '-')[OFFSET(1)] AS hhmm_end
  FROM days dt, cfg, UNNEST(SPLIT(cfg.link_csv, ',')) AS tok
),
link_picked AS (
  SELECT
    lt.username_std,
    -- local window start/end as DATETIME (no tz)
    DATETIME(lt.d, PARSE_TIME('%H:%M', lt.hhmm_start)) AS win_start_dt,
    DATETIME(lt.d, PARSE_TIME('%H:%M', lt.hhmm_end))   AS win_end_dt,
    'LinkDrop' AS channel,
    lt.tz
  FROM link_tokens lt
),
link_final AS (
  SELECT
    username_std,
    DATETIME_ADD(win_start_dt,
      INTERVAL CAST(MOD(
        ABS(FARM_FINGERPRINT(CONCAT(username_std,'|',CAST(win_start_dt AS STRING),'|link'))),
        GREATEST(DATETIME_DIFF(win_end_dt, win_start_dt, MINUTE), 1)
      ) AS INT64) MINUTE
    ) AS base_dt_local,
    'LinkDrop' AS channel,
    0 AS minute_jitter,
    tz
  FROM link_picked
),

/* -------- Union + compute outputs -------- */
unioned AS (
  SELECT * FROM drip
  UNION ALL SELECT * FROM renew
  UNION ALL SELECT * FROM link_final
),
with_dt AS (
  SELECT
    u.username_std,
    -- final local datetime
    DATETIME_ADD(u.base_dt_local, INTERVAL u.minute_jitter MINUTE) AS planned_local_datetime,
    u.channel,
    u.tz
  FROM unioned u
)
SELECT
  w.username_std,
  DATE(w.planned_local_datetime) AS date_local,
  EXTRACT(HOUR FROM w.planned_local_datetime) AS hod_local,
  -- map channel to a normalized kind for ops
  CASE
    WHEN w.channel='MM'    THEN 'drip_mm'
    WHEN w.channel='Wall'  THEN 'drip_wall'
    WHEN w.channel='Renewal' THEN 'renewal'
    ELSE 'link_drop'
  END AS slot_kind,
  w.channel,
  w.planned_local_datetime,
  -- if you need UTC for automation, compute it here
  TIMESTAMP(w.planned_local_datetime, w.tz) AS scheduled_datetime_utc
FROM with_dt w
ORDER BY username_std, planned_local_datetime, slot_kind
