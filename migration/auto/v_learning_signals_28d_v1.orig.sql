WITH mf AS (
  SELECT
    username_std,
    DATE(TIMESTAMP_TRUNC(sending_ts, DAY)) AS d,
    SAFE_CAST(price_usd    AS NUMERIC) AS price_usd,
    SAFE_CAST(earnings_usd AS NUMERIC) AS earnings_usd
  FROM `of-scheduler-proj.core.message_facts`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)
),
by_page AS (
  SELECT
    username_std,
    COUNT(*) AS sends_28d,
    SUM(earnings_usd) AS earnings_28d,
    SAFE_DIVIDE(SUM(earnings_usd), COUNT(*)) AS rps_28d,
    APPROX_QUANTILES(price_usd, 101)[OFFSET(50)] AS p50_price,
    COUNTIF(earnings_usd > 0) / COUNT(*) AS sell_rate
  FROM mf
  GROUP BY username_std
),
trend AS (
  SELECT
    a.username_std,
    SAFE_DIVIDE(a.earnings, GREATEST(a.sends,1)) AS rps_recent,
    SAFE_DIVIDE(b.earnings, GREATEST(b.sends,1)) AS rps_prev,
    SAFE_DIVIDE(a.earnings - b.earnings, NULLIF(b.earnings,0)) AS earnings_lift_ratio
  FROM (
    SELECT username_std, COUNT(*) AS sends, SUM(earnings_usd) AS earnings
    FROM mf
    WHERE d >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    GROUP BY username_std
  ) a
  FULL JOIN (
    SELECT username_std, COUNT(*) AS sends, SUM(earnings_usd) AS earnings
    FROM mf
    WHERE d < DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
      AND d >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
    GROUP BY username_std
  ) b USING (username_std)
)
SELECT
  p.username_std, p.sends_28d, p.earnings_28d, p.rps_28d, p.p50_price, p.sell_rate,
  t.rps_recent, t.rps_prev, t.earnings_lift_ratio
FROM by_page p
LEFT JOIN trend t USING (username_std)
