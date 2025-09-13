CREATE OR REPLACE VIEW `of-scheduler-proj.mart.v_page_volume_profile_28d` AS
WITH daily AS (
  SELECT mf.username_std,
         DATE(mf.sending_ts, pd.tz) AS date_local,
         COUNTIF(mf.price_usd > 0) AS ppv_cnt,
         SUM(CASE WHEN mf.price_usd > 0 THEN mf.earnings_usd ELSE 0 END) AS rev_ppv
  FROM `of-scheduler-proj.layer_04_semantic.message_facts` mf
  JOIN `of-scheduler-proj.layer_04_semantic.v_page_dim` pd USING (username_std)
  WHERE mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)
  GROUP BY mf.username_std, date_local
),
elastic AS (
  SELECT
    username_std,
    AVG(ppv_cnt) AS avg_ppv_per_day,
    APPROX_QUANTILES(ppv_cnt, 20)[OFFSET(18)] AS q90_ppv_per_day,
    CORR(ppv_cnt, rev_ppv) AS corr_vol_rev,
    CASE WHEN VAR_POP(ppv_cnt)=0 THEN 0
         ELSE COVAR_POP(ppv_cnt, rev_ppv)/VAR_POP(ppv_cnt) END AS slope_rev_per_ppv
  FROM daily GROUP BY username_std
),
sends AS (
  SELECT mf.username_std,
         DATE(mf.sending_ts, pd.tz) AS date_local,
         ROW_NUMBER() OVER (PARTITION BY mf.username_std, DATE(mf.sending_ts, pd.tz) ORDER BY mf.sending_ts) AS rn,
         COUNT(*) OVER  (PARTITION BY mf.username_std, DATE(mf.sending_ts, pd.tz)) AS n_sends,
         CASE WHEN mf.price_usd > 0 THEN mf.earnings_usd ELSE 0 END AS rev
  FROM `of-scheduler-proj.layer_04_semantic.message_facts` mf
  JOIN `of-scheduler-proj.layer_04_semantic.v_page_dim` pd USING (username_std)
  WHERE mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)
    AND mf.price_usd > 0
),
fatigue AS (
  SELECT username_std,
         SAFE_DIVIDE(AVG(CASE WHEN rn=n_sends THEN rev END),
                     NULLIF(AVG(CASE WHEN rn=1 THEN rev END),0)) AS fatigue_ratio
  FROM sends WHERE n_sends >= 2 GROUP BY username_std
)
SELECT
  e.username_std,
  e.avg_ppv_per_day,
  e.q90_ppv_per_day,
  e.corr_vol_rev,
  e.slope_rev_per_ppv,
  COALESCE(f.fatigue_ratio,1.0) AS fatigue_ratio,
  CASE
    WHEN e.slope_rev_per_ppv >= 12 AND COALESCE(f.fatigue_ratio,1.0) >= 0.70 THEN 1.50
    WHEN e.slope_rev_per_ppv >=  8 AND COALESCE(f.fatigue_ratio,1.0) >= 0.65 THEN 1.30
    WHEN e.slope_rev_per_ppv >=  4                                    THEN 1.15
    WHEN e.slope_rev_per_ppv <=  1 OR COALESCE(f.fatigue_ratio,1.0) < 0.50 THEN 0.90
    ELSE 1.00
  END AS volume_boost
FROM elastic e LEFT JOIN fatigue f USING (username_std)
