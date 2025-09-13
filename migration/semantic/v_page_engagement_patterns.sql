CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_page_engagement_patterns`
OPTIONS(description="Refactored view to analyze engagement patterns (conversion, revenue) by time of day and day of week.")
AS
WITH hourly AS (
  SELECT
    creator_key,
    time_of_day_utc AS hour,
    day_of_week AS dow,
    AVG(conversion_rate) AS conversion_rate,
    AVG(price_usd) AS avg_price,
    AVG(net_revenue_usd) AS avg_earnings,
    STDDEV(net_revenue_usd) AS earnings_volatility,
    COUNT(*) AS n_messages,
    SUM(net_revenue_usd) AS total_earnings
  FROM `of-scheduler-proj.layer_03_foundation.fact_message_send`
  WHERE send_date >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 89 DAY)
  GROUP BY 1, 2, 3
  HAVING n_messages >= 3
),
aggregated AS (
  SELECT
    creator_key,
    ARRAY_AGG(STRUCT(hour, dow, conversion_rate, avg_earnings, n_messages) ORDER BY conversion_rate DESC LIMIT 10) AS top_conversion_windows,
    1 - SAFE_DIVIDE(SQRT(AVG(POW(earnings_volatility, 2))), NULLIF(AVG(avg_earnings), 0)) AS revenue_consistency,
    SAFE_DIVIDE(COUNT(DISTINCT CONCAT(hour, '-', dow)), 168.0) AS schedule_coverage,
    CORR(avg_price, conversion_rate) AS price_elasticity,
    MAX(conversion_rate) AS peak_conversion_rate,
    AVG(conversion_rate) AS avg_conversion_rate,
    MAX(avg_earnings) AS peak_earnings_per_message,
    SUM(total_earnings) AS total_90d_earnings,
    SUM(n_messages) AS total_90d_messages
  FROM hourly
  GROUP BY 1
)
SELECT
  c.username AS username_std,
  a.* EXCEPT (creator_key)
FROM aggregated a
JOIN `of-scheduler-proj.layer_03_foundation.dim_creator` c
  ON a.creator_key = c.creator_key AND c.is_current_record = TRUE;