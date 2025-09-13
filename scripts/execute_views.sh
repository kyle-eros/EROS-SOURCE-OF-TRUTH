#!/bin/bash

echo "========================================="
echo "CREATING SEMANTIC VIEWS (FIXED)"
echo "========================================="

echo ""
echo "1. Creating semantic views with correct column names..."
bq query --use_legacy_sql=false --location=US << 'SQL'
-- Page DOW x HOD profile using actual columns
CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_page_dow_hod_profile_90d` AS
WITH base AS (
  SELECT
    username_std AS page_key,
    day_of_week AS dow_utc,
    hour_utc,
    -- Use revenue_per_send directly as RPS
    AVG(CAST(revenue_per_send AS FLOAT64)) AS rps_actual,
    COUNT(DISTINCT event_date) AS sample_days
  FROM `of-scheduler-proj.layer_02_staging.stg_message_events`
  WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  GROUP BY 1, 2, 3
),
avg_rps AS (
  SELECT
    page_key,
    AVG(rps_actual) AS avg_rps_page
  FROM base
  GROUP BY 1
)
SELECT
  b.page_key,
  b.dow_utc,
  b.hour_utc,
  b.rps_actual,
  SAFE_DIVIDE(b.rps_actual, NULLIF(a.avg_rps_page, 0)) AS rps_lift,
  b.sample_days,
  RANK() OVER (PARTITION BY b.page_key, b.dow_utc ORDER BY b.rps_actual DESC) AS hour_rank_in_day
FROM base b
JOIN avg_rps a ON b.page_key = a.page_key;

-- Page behavior 28d using actual columns
CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_page_behavior_28d` AS
WITH recent AS (
  SELECT
    username_std AS page_key,
    -- Calculate RPS using revenue_per_send
    AVG(CAST(revenue_per_send AS FLOAT64)) AS rps_28d,
    AVG(purchase_rate) AS ppv_conversion_rate,
    COUNT(DISTINCT event_date) AS active_days
  FROM `of-scheduler-proj.layer_02_staging.stg_message_events`
  WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
  GROUP BY 1
),
prior AS (
  SELECT
    username_std AS page_key,
    AVG(CAST(revenue_per_send AS FLOAT64)) AS rps_prior_28d
  FROM `of-scheduler-proj.layer_02_staging.stg_message_events`
  WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 56 DAY) 
    AND DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY)
  GROUP BY 1
),
night_activity AS (
  SELECT
    username_std AS page_key,
    SUM(CASE WHEN hour_utc BETWEEN 20 AND 23 OR hour_utc BETWEEN 0 AND 2 
        THEN CAST(revenue_usd AS FLOAT64) ELSE 0 END) AS night_revenue,
    SUM(CAST(revenue_usd AS FLOAT64)) AS total_revenue
  FROM `of-scheduler-proj.layer_02_staging.stg_message_events`
  WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
  GROUP BY 1
)
SELECT
  r.page_key,
  r.rps_28d,
  r.ppv_conversion_rate,
  r.active_days,
  SAFE_DIVIDE(r.rps_28d - p.rps_prior_28d, NULLIF(p.rps_prior_28d, 0)) AS or_slump_pct,
  SAFE_DIVIDE(n.night_revenue, NULLIF(n.total_revenue, 0)) > 0.35 AS night_owl_idx,
  0.0 AS cohort_currency,  -- Placeholder (no fan age data)
  0.0 AS refund_rate  -- Placeholder (no refund data)
FROM recent r
LEFT JOIN prior p ON r.page_key = p.page_key
LEFT JOIN night_activity n ON r.page_key = n.page_key;

-- Page intensity 7d with hysteresis using actual columns
CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_page_intensity_7d` AS
WITH base AS (
  SELECT
    username_std AS page_key,
    AVG(CAST(revenue_per_send AS FLOAT64)) AS rps_7d,
    SUM(CAST(revenue_usd AS FLOAT64)) AS total_earnings_7d,
    SAFE_DIVIDE(
      SUM(CAST(revenue_usd AS FLOAT64)), 
      NULLIF(SUM(messages_purchased), 0)
    ) AS avg_spend_per_txn,
    -- Since we don't have fan_id, use messages_purchased as proxy
    SAFE_DIVIDE(
      SUM(CAST(revenue_usd AS FLOAT64)), 
      NULLIF(COUNT(DISTINCT CONCAT(username_std, event_date)), 0)
    ) AS avg_earn_per_fan,
    COUNT(DISTINCT event_date) AS active_fans,  -- Use active days as proxy
    0.7 AS renew_on_pct  -- Placeholder
  FROM `of-scheduler-proj.layer_02_staging.stg_message_events`
  WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  GROUP BY 1
)
SELECT
  page_key,
  rps_7d,
  total_earnings_7d,
  avg_spend_per_txn,
  avg_earn_per_fan,
  active_fans,
  renew_on_pct,
  -- Intensity score calculation
  (0.3 * SAFE.LN(1 + total_earnings_7d) +
   0.2 * SAFE.LN(1 + avg_spend_per_txn) +
   0.2 * SAFE.LN(1 + avg_earn_per_fan) +
   0.2 * renew_on_pct +
   0.1 * SAFE.LN(1 + active_fans)) AS intensity_score,
  -- Tier assignment with hysteresis
  CASE
    WHEN (0.3 * SAFE.LN(1 + total_earnings_7d) +
          0.2 * SAFE.LN(1 + avg_spend_per_txn) +
          0.2 * SAFE.LN(1 + avg_earn_per_fan) +
          0.2 * renew_on_pct +
          0.1 * SAFE.LN(1 + active_fans)) > 5.0 THEN 'high'
    WHEN (0.3 * SAFE.LN(1 + total_earnings_7d) +
          0.2 * SAFE.LN(1 + avg_spend_per_txn) +
          0.2 * SAFE.LN(1 + avg_earn_per_fan) +
          0.2 * renew_on_pct +
          0.1 * SAFE.LN(1 + active_fans)) > 3.0 THEN 'medium'
    ELSE 'low'
  END AS recommended_tier
FROM base;

SELECT 'Semantic views created successfully' AS status;
SQL

echo ""
echo "2. Creating feature store extension view..."
bq query --use_legacy_sql=false --location=US << 'SQL'
-- Feature store extension view
CREATE OR REPLACE VIEW `of-scheduler-proj.layer_05_ml.feature_store_ext` AS
WITH latest_features AS (
  SELECT
    username_page,
    caption_id,
    computed_date,
    performance_features,
    statistical_features,
    exploration_features,
    temporal_features,
    cooldown_features,
    composite_scores,
    RANK() OVER (PARTITION BY username_page, caption_id ORDER BY computed_date DESC) AS rn
  FROM `of-scheduler-proj.layer_05_ml.feature_store`
  WHERE computed_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
),
behavior AS (
  SELECT * FROM `of-scheduler-proj.layer_04_semantic.v_page_behavior_28d`
),
intensity AS (
  SELECT * FROM `of-scheduler-proj.layer_04_semantic.v_page_intensity_7d`
)
SELECT
  f.username_page,
  f.caption_id,
  f.computed_date,
  f.performance_features,
  f.statistical_features,
  f.exploration_features,
  f.temporal_features,
  f.cooldown_features,
  f.composite_scores,
  COALESCE(b.or_slump_pct * 0.5, 0.0) AS scam_risk_score,
  b.night_owl_idx,
  CASE 
    WHEN i.avg_spend_per_txn > 50 THEN 'premium'
    WHEN i.avg_spend_per_txn > 20 THEN 'standard'
    ELSE 'budget'
  END AS price_stance_bucket,
  0 AS family_recent_uses,  -- Placeholder
  0 AS cross_page_usage,  -- Placeholder
  i.renew_on_pct
FROM latest_features f
LEFT JOIN behavior b ON f.username_page = b.page_key
LEFT JOIN intensity i ON f.username_page = i.page_key
WHERE f.rn = 1;

SELECT 'Feature store extension view created' AS status;
SQL

echo ""
echo "Views creation completed!"
echo "========================================="
