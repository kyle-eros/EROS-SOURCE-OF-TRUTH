WITH base AS (
  SELECT username_page, slot_dt_local
  FROM `of-scheduler-proj.mart.v_slot_recommendations_next24_v3`
),
dh AS (
  SELECT
    b.username_page,
    b.slot_dt_local,
    MOD(EXTRACT(DAYOFWEEK FROM b.slot_dt_local) + 5, 7) AS dow,
    CAST(FORMAT_DATETIME('%H', b.slot_dt_local) AS INT64) AS hod
  FROM base b
),
price AS (
  SELECT s.username_page, s.dow, s.hod, p.price_q AS reco_price_usd
  FROM `of-scheduler-proj.mart.v_slot_scorecard_v3` s
  LEFT JOIN `of-scheduler-proj.mart.v_ppv_price_reco_lcb_28d_v3` p
    ON p.username_page = s.username_page
   AND p.dow = s.dow
   AND p.hod = s.hod
),
paid AS (
  SELECT username_page, is_paid
  FROM `of-scheduler-proj.layer_04_semantic.v_page_paid_status`
)
SELECT
  dh.username_page,
  dh.slot_dt_local,
  dh.dow, dh.hod,
  CASE WHEN pr.reco_price_usd > 0 THEN 'ppv' ELSE 'free' END AS reco_dm_type,
  IFNULL(pr.reco_price_usd, 0) AS reco_price_usd
FROM dh
LEFT JOIN price pr USING (username_page, dow, hod)
LEFT JOIN paid  p  USING (username_page)
WHERE NOT (p.is_paid = FALSE AND pr.reco_price_usd > 0)
