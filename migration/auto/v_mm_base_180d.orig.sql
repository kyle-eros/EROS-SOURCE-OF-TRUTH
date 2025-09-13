SELECT
  LOWER(TRIM(m.username_std)) AS username_std,
  m.sending_ts,
  DATETIME(m.sending_ts, p.tz) AS sending_dt_local,
  DATE(m.sending_ts, p.tz)     AS date_local,
  EXTRACT(HOUR FROM DATETIME(m.sending_ts, p.tz)) AS hod_local,
  MOD(EXTRACT(DAYOFWEEK FROM DATE(m.sending_ts, p.tz)) + 5, 7) AS dow_local,  -- Mon=0..Sun=6
  SAFE_CAST(m.price_usd    AS FLOAT64) AS price_usd,
  SAFE_CAST(m.earnings_usd AS FLOAT64) AS earnings_usd,
  SAFE_CAST(m.sent         AS INT64)   AS sent,
  SAFE_CAST(m.viewed       AS INT64)   AS viewed,
  SAFE_CAST(m.purchased    AS INT64)   AS purchased,
  CASE WHEN m.price_usd IS NOT NULL AND m.price_usd > 0 THEN 1 ELSE 0 END AS is_ppv
FROM `of-scheduler-proj.core.message_facts` m
JOIN `of-scheduler-proj.core.page_dim` p USING (username_std)
WHERE m.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
