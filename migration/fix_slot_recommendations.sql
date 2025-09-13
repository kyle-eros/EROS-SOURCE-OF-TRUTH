CREATE OR REPLACE VIEW `of-scheduler-proj.mart.v_slot_recommendations_next24_v3` AS
WITH pages AS (
  SELECT v.username_page, v.username_std, COALESCE(pd.tz,'UTC') AS tz
  FROM `of-scheduler-proj.layer_04_semantic.v_pages` v
  LEFT JOIN `of-scheduler-proj.layer_04_semantic.v_page_dim` pd USING (username_std)
),
nowz AS (
  SELECT username_page, username_std, tz, DATETIME(CURRENT_TIMESTAMP(), tz) AS now_local
  FROM pages
),
grid AS (
  SELECT n.username_page, n.username_std, n.tz,
         DATETIME_TRUNC(n.now_local, HOUR) + INTERVAL h HOUR AS slot_dt_local
  FROM nowz n, UNNEST(GENERATE_ARRAY(0,23)) AS h
),
feat AS (
  SELECT
    g.username_page, g.username_std, g.tz,
    MOD(EXTRACT(DAYOFWEEK FROM g.slot_dt_local) + 5, 7) AS dow,
    CAST(FORMAT_DATETIME('%H', g.slot_dt_local) AS INT64) AS hod,
    g.slot_dt_local
  FROM grid g
),
best_price AS (
  SELECT s.username_page, s.dow, s.hod, s.slot_score_base,
         p.price_q AS best_ppv_price,
         p.p_buy_eb, p.rps_eb, p.rps_lcb
  FROM `of-scheduler-proj.mart.v_slot_scorecard_v3` s
  LEFT JOIN `of-scheduler-proj.mart.v_ppv_price_reco_lcb_28d_v3` p
    ON p.username_page=s.username_page AND p.dow=s.dow AND p.hod=s.hod
),
quota AS (
  SELECT username_std, dow, ppv_quota AS max_sends_today
  FROM `of-scheduler-proj.mart.v_daily_quota_policy_v3`
)
SELECT
  f.username_page,
  f.slot_dt_local,
  f.dow, f.hod,
  b.slot_score_base,
  b.p_buy_eb  AS best_ppv_buy_rate,
  b.rps_eb,
  b.rps_lcb,
  -- paid/free gate
  CASE
    WHEN NOT COALESCE(pp.is_paid_sub, FALSE) THEN 'free'
    WHEN b.rps_lcb IS NOT NULL AND b.rps_lcb >= COALESCE(sc.rps_free,0) THEN 'ppv'
    ELSE 'free'
  END AS reco_dm_type,
  CASE
    WHEN NOT COALESCE(pp.is_paid_sub, FALSE) THEN 0
    WHEN b.rps_lcb IS NOT NULL AND b.rps_lcb >= COALESCE(sc.rps_free,0) THEN IFNULL(b.best_ppv_price,0)
    ELSE 0
  END AS reco_price_usd
FROM feat f
LEFT JOIN best_price b USING (username_page, dow, hod)
LEFT JOIN `of-scheduler-proj.mart.v_slot_scorecard_v3` sc USING (username_page, dow, hod)
LEFT JOIN quota q
  ON q.username_std=f.username_std AND q.dow=f.dow
LEFT JOIN `of-scheduler-proj.layer_04_semantic.v_page_paid_status` pp
  ON pp.username_std = f.username_std
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY f.username_page, DATE(f.slot_dt_local)
  ORDER BY b.slot_score_base DESC, f.slot_dt_local
) <= COALESCE(q.max_sends_today, 4);