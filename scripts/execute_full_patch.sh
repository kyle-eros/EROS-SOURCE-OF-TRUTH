#!/bin/bash

echo "========================================="
echo "APPLYING FULL PATCH - FIXING ALL ISSUES"
echo "========================================="

echo ""
echo "1. Recreating semantic views with ratio-of-sums and UTC..."
bq query --use_legacy_sql=false --location=US << 'SQL'
-- Page DOW × HOD profile (90d)
CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_page_dow_hod_profile_90d` AS
WITH base AS (
  SELECT
    username_std AS page_key,
    EXTRACT(DAYOFWEEK FROM event_date) AS dow_utc,
    hour_utc,
    SAFE_DIVIDE(SUM(CAST(revenue_usd AS FLOAT64)), NULLIF(SUM(messages_sent), 0)) AS rps_actual, -- ratio-of-sums
    COUNT(DISTINCT event_date) AS sample_days
  FROM `of-scheduler-proj.layer_02_staging.stg_message_events`
  WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 90 DAY) AND CURRENT_DATE('UTC')
  GROUP BY 1,2,3
),
avg_rps AS (
  SELECT page_key, AVG(rps_actual) AS avg_rps_page
  FROM base
  GROUP BY 1
)
SELECT
  b.page_key, b.dow_utc, b.hour_utc, b.rps_actual,
  SAFE_DIVIDE(b.rps_actual, NULLIF(a.avg_rps_page, 0)) AS rps_lift,
  b.sample_days,
  RANK() OVER (PARTITION BY b.page_key, b.dow_utc ORDER BY b.rps_actual DESC) AS hour_rank_in_day
FROM base b
JOIN avg_rps a USING (page_key);

-- Page behavior 28d
CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_page_behavior_28d` AS
WITH recent AS (
  SELECT
    username_std AS page_key,
    SAFE_DIVIDE(SUM(CAST(revenue_usd AS FLOAT64)), NULLIF(SUM(messages_sent), 0)) AS rps_28d, -- ratio-of-sums
    SAFE_DIVIDE(SUM(messages_purchased), NULLIF(SUM(messages_sent), 0)) AS ppv_conversion_rate, -- ratio-of-sums
    COUNT(DISTINCT event_date) AS active_days
  FROM `of-scheduler-proj.layer_02_staging.stg_message_events`
  WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 28 DAY) AND CURRENT_DATE('UTC')
  GROUP BY 1
),
prior AS (
  SELECT
    username_std AS page_key,
    SAFE_DIVIDE(SUM(CAST(revenue_usd AS FLOAT64)), NULLIF(SUM(messages_sent), 0)) AS rps_prior_28d
  FROM `of-scheduler-proj.layer_02_staging.stg_message_events`
  WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 56 DAY)
                      AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 29 DAY)
  GROUP BY 1
),
night_activity AS (
  SELECT
    username_std AS page_key,
    SUM(CASE WHEN hour_utc BETWEEN 20 AND 23 OR hour_utc BETWEEN 0 AND 2
             THEN CAST(revenue_usd AS FLOAT64) ELSE 0 END) AS night_revenue,
    SUM(CAST(revenue_usd AS FLOAT64)) AS total_revenue
  FROM `of-scheduler-proj.layer_02_staging.stg_message_events`
  WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 28 DAY) AND CURRENT_DATE('UTC')
  GROUP BY 1
)
SELECT
  r.page_key,
  r.rps_28d,
  r.ppv_conversion_rate,
  r.active_days,
  SAFE_DIVIDE(r.rps_28d - p.rps_prior_28d, NULLIF(p.rps_prior_28d, 0)) AS or_slump_pct,
  SAFE_DIVIDE(n.night_revenue, NULLIF(n.total_revenue, 0)) > 0.35 AS night_owl_idx,
  0.0 AS cohort_currency,  -- placeholder
  0.0 AS refund_rate       -- placeholder
FROM recent r
LEFT JOIN prior p USING (page_key)
LEFT JOIN night_activity n USING (page_key);

-- Page intensity 7d (ratio-of-sums, placeholders kept)
CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_page_intensity_7d` AS
WITH base AS (
  SELECT
    username_std AS page_key,
    SAFE_DIVIDE(SUM(CAST(revenue_usd AS FLOAT64)), NULLIF(SUM(messages_sent), 0)) AS rps_7d, -- ratio-of-sums
    SUM(CAST(revenue_usd AS FLOAT64)) AS total_earnings_7d,
    SAFE_DIVIDE(SUM(CAST(revenue_usd AS FLOAT64)), NULLIF(SUM(messages_purchased), 0)) AS avg_spend_per_txn,
    -- proxies until fan-level exists
    SAFE_DIVIDE(SUM(CAST(revenue_usd AS FLOAT64)), NULLIF(COUNT(DISTINCT CONCAT(username_std, event_date)), 0)) AS avg_earn_per_fan,
    COUNT(DISTINCT event_date) AS active_fans,
    0.7 AS renew_on_pct -- placeholder
  FROM `of-scheduler-proj.layer_02_staging.stg_message_events`
  WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 7 DAY) AND CURRENT_DATE('UTC')
  GROUP BY 1
)
SELECT
  page_key, rps_7d, total_earnings_7d, avg_spend_per_txn, avg_earn_per_fan, active_fans, renew_on_pct,
  (0.3 * SAFE.LN(1 + total_earnings_7d)
   + 0.2 * SAFE.LN(1 + avg_spend_per_txn)
   + 0.2 * SAFE.LN(1 + avg_earn_per_fan)
   + 0.2 * renew_on_pct
   + 0.1 * SAFE.LN(1 + active_fans)) AS intensity_score,
  CASE
    WHEN (0.3 * SAFE.LN(1 + total_earnings_7d)
        + 0.2 * SAFE.LN(1 + avg_spend_per_txn)
        + 0.2 * SAFE.LN(1 + avg_earn_per_fan)
        + 0.2 * renew_on_pct
        + 0.1 * SAFE.LN(1 + active_fans)) > 5.0 THEN 'high'
    WHEN (0.3 * SAFE.LN(1 + total_earnings_7d)
        + 0.2 * SAFE.LN(1 + avg_spend_per_txn)
        + 0.2 * SAFE.LN(1 + avg_earn_per_fan)
        + 0.2 * renew_on_pct
        + 0.1 * SAFE.LN(1 + active_fans)) > 3.0 THEN 'medium'
    ELSE 'low'
  END AS recommended_tier
FROM base;

SELECT 'Semantic views recreated with ratio-of-sums' AS status;
SQL
