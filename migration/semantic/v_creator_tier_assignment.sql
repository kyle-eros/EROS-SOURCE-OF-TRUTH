CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_creator_tier_assignment` OPTIONS(description="Calculates 'Intensity Score' and assigns a performance tier using stable, persisted thresholds from ops_config, with a dynamic fallback.") AS
WITH
base_health AS (
  SELECT
    page_id AS creator_key,
    LOG10(1 + active_fans_7d) AS log_active_fans,
    message_net_7d,
    avg_earn_per_fan_7d,
    avg_spend_per_txn_7d,
    total_earnings_7d,
    renew_on_rate_7d,
    -expired_change_7d AS neg_expired_change_7d
  FROM `of-scheduler-proj.layer_04_semantic.v_page_health_7d`
),
z_scores AS (
  SELECT
    creator_key,
    (log_active_fans - AVG(log_active_fans) OVER()) / NULLIF(STDDEV_SAMP(log_active_fans) OVER(), 0) AS z_active,
    (message_net_7d - AVG(message_net_7d) OVER()) / NULLIF(STDDEV_SAMP(message_net_7d) OVER(), 0) AS z_msgnet,
    (avg_earn_per_fan_7d - AVG(avg_earn_per_fan_7d) OVER()) / NULLIF(STDDEV_SAMP(avg_earn_per_fan_7d) OVER(), 0) AS z_apf,
    (avg_spend_per_txn_7d - AVG(avg_spend_per_txn_7d) OVER()) / NULLIF(STDDEV_SAMP(avg_spend_per_txn_7d) OVER(), 0) AS z_spend,
    (total_earnings_7d - AVG(total_earnings_7d) OVER()) / NULLIF(STDDEV_SAMP(total_earnings_7d) OVER(), 0) AS z_total,
    (renew_on_rate_7d - AVG(renew_on_rate_7d) OVER()) / NULLIF(STDDEV_SAMP(renew_on_rate_7d) OVER(), 0) AS z_renew,
    (neg_expired_change_7d - AVG(neg_expired_change_7d) OVER()) / NULLIF(STDDEV_SAMP(neg_expired_change_7d) OVER(), 0) AS z_churn
  FROM base_health
),
intensity_score AS (
  SELECT
    creator_key,
    (0.30 * COALESCE(z_active, 0) + 0.20 * COALESCE(z_msgnet, 0) + 0.15 * COALESCE(z_apf, 0) + 0.10 * COALESCE(z_spend, 0) + 0.15 * COALESCE(z_total, 0) + 0.05 * COALESCE(z_renew, 0) + 0.05 * COALESCE(z_churn, 0)) AS intensity
  FROM z_scores
),
thresholds_config AS (
  SELECT * FROM `of-scheduler-proj.ops_config.tier_thresholds`
  WHERE computed_date = (SELECT MAX(computed_date) FROM `of-scheduler-proj.ops_config.tier_thresholds`)
),
thresholds_dynamic AS (
  SELECT
    CURRENT_DATE('UTC') AS computed_date,
    COUNT(s.creator_key) AS population_count,
    APPROX_QUANTILES(s.intensity, 100)[OFFSET(40)] AS q40,
    APPROX_QUANTILES(s.intensity, 100)[OFFSET(60)] AS q60,
    APPROX_QUANTILES(s.intensity, 100)[OFFSET(90)] AS q90,
    APPROX_QUANTILES(h.active_fans_7d, 100)[OFFSET(70)] AS af_p70,
    APPROX_QUANTILES(h.message_net_7d, 100)[OFFSET(60)] AS msg_p60,
    APPROX_QUANTILES(h.message_net_7d, 100)[OFFSET(70)] AS msg_p70,
    APPROX_QUANTILES(h.total_earnings_7d, 100)[OFFSET(70)] AS tot_p70
  FROM intensity_score s
  JOIN `of-scheduler-proj.layer_04_semantic.v_page_health_7d` h ON s.creator_key = h.page_id
),
thresholds AS (
  SELECT * FROM thresholds_config
  UNION ALL
  SELECT * FROM thresholds_dynamic WHERE NOT EXISTS (SELECT 1 FROM thresholds_config)
)
SELECT
  h.page_id AS creator_key,
  s.intensity,
  CASE
    WHEN h.message_net_7d = 0 AND h.total_earnings_7d = 0 AND h.active_fans_7d < 100 THEN 'LOW'
    WHEN s.intensity >= t.q90 AND h.active_fans_7d >= t.af_p70 AND (h.message_net_7d >= t.msg_p70 OR h.total_earnings_7d >= t.tot_p70) THEN 'POWER'
    WHEN s.intensity >= t.q60 AND (h.message_net_7d >= t.msg_p60 OR h.active_fans_7d >= 0.8 * t.af_p70) THEN 'HIGH'
    WHEN s.intensity >= t.q40 THEN 'MED'
    ELSE 'LOW'
  END AS recommended_tier,
  CURRENT_TIMESTAMP() AS computed_at
FROM `of-scheduler-proj.layer_04_semantic.v_page_health_7d` h
JOIN intensity_score s ON h.page_id = s.creator_key
CROSS JOIN thresholds t;