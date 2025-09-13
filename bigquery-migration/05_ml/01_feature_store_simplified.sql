-- =========================================
-- ML FEATURE STORE (SIMPLIFIED)
-- =========================================
-- Purpose: Comprehensive feature engineering for ML ranking
-- Simplified version without correlated subqueries
-- =========================================

CREATE OR REPLACE TABLE `of-scheduler-proj.layer_05_ml.feature_store`
PARTITION BY DATE(computed_at)
CLUSTER BY username_page, caption_id
AS
WITH base_features AS (
  -- Get latest performance metrics for each caption
  SELECT
    cp.caption_id,
    cp.creator_username,
    cp.creator_page_type,
    cp.username_page,
    cp.caption_category,
    
    -- Recent performance (last 30 days)
    AVG(cp.revenue_per_send) AS rps_30d,
    AVG(cp.purchase_rate) AS purchase_rate_30d,
    SUM(cp.total_messages_sent) AS sends_30d,
    SUM(cp.total_revenue) AS revenue_30d,
    COUNT(DISTINCT cp.metric_date) AS active_days_30d,
    
    -- Trend metrics
    AVG(cp.rps_trend_slope) AS trend_slope,
    AVG(cp.stability_score) AS stability_score,
    
    -- Time-decayed metrics (exponential decay with 14-day half-life)
    SUM(cp.revenue_per_send * EXP(-0.0495 * DATE_DIFF(CURRENT_DATE(), cp.metric_date, DAY))) / 
    NULLIF(SUM(EXP(-0.0495 * DATE_DIFF(CURRENT_DATE(), cp.metric_date, DAY))), 0) AS rps_weighted,
    
    -- Recency
    MAX(cp.metric_date) AS last_used_date,
    DATE_DIFF(CURRENT_DATE(), MAX(cp.metric_date), DAY) AS days_since_use,
    
    -- Statistical distribution
    APPROX_QUANTILES(cp.revenue_per_send, 100)[OFFSET(50)] AS median_rps,
    APPROX_QUANTILES(cp.revenue_per_send, 100)[OFFSET(75)] AS p75_rps,
    APPROX_QUANTILES(cp.revenue_per_send, 100)[OFFSET(25)] AS p25_rps,
    STDDEV(cp.revenue_per_send) AS rps_stddev
    
  FROM `of-scheduler-proj.layer_04_semantic.caption_performance_daily` cp
  WHERE cp.metric_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY 1,2,3,4,5
),

global_stats AS (
  -- Calculate global statistics separately
  SELECT
    AVG(rps_30d) AS global_avg_rps,
    AVG(purchase_rate_30d) AS global_avg_purchase_rate,
    APPROX_QUANTILES(sends_30d, 100)[OFFSET(50)] AS global_median_sends,
    SUM(sends_30d) AS total_sends
  FROM base_features
),

category_stats AS (
  -- Calculate category-level statistics
  SELECT
    caption_category,
    AVG(rps_30d) AS category_avg_rps,
    AVG(purchase_rate_30d) AS category_avg_purchase_rate
  FROM base_features
  GROUP BY caption_category
),

enriched_features AS (
  SELECT
    bf.*,
    gs.global_avg_rps,
    gs.global_avg_purchase_rate,
    gs.global_median_sends,
    gs.total_sends,
    cs.category_avg_rps,
    cs.category_avg_purchase_rate,
    
    -- Bayesian smoothed metrics
    (bf.rps_30d * bf.sends_30d + cs.category_avg_rps * 100) / (bf.sends_30d + 100) AS rps_smoothed,
    (bf.purchase_rate_30d * bf.sends_30d + cs.category_avg_purchase_rate * 100) / (bf.sends_30d + 100) AS purchase_rate_smoothed,
    
    -- Confidence score based on sample size and stability
    LEAST(1.0, bf.sends_30d / 1000) * COALESCE(bf.stability_score, 0.5) AS confidence_score,
    
    -- Upper Confidence Bound (UCB) for exploration
    SQRT(2 * LN(1 + gs.total_sends) / GREATEST(1, bf.sends_30d)) AS ucb_bonus,
    
    -- Temperature for softmax exploration
    1 / (1 + EXP(-0.1 * (30 - LEAST(30, bf.days_since_use)))) AS exploration_temperature,
    
    -- Novelty score
    CASE
      WHEN bf.sends_30d < 10 THEN 1.0
      WHEN bf.sends_30d < 50 THEN 0.5
      WHEN bf.sends_30d < 100 THEN 0.2
      ELSE 0.0
    END AS novelty_bonus,
    
    -- Dynamic cooldown
    GREATEST(
      6,  -- Minimum 6 hours
      6 * POW(1.5, LEAST(5, bf.active_days_30d / 7))
    ) AS base_cooldown_hours,
    
    -- Fatigue score
    1 / (1 + EXP(-0.5 * (bf.active_days_30d - 15))) AS fatigue_score
    
  FROM base_features bf
  CROSS JOIN global_stats gs
  LEFT JOIN category_stats cs ON bf.caption_category = cs.caption_category
)

SELECT
  -- Identifiers
  caption_id,
  creator_username,
  creator_page_type,
  username_page,
  caption_category,
  
  -- Performance features
  STRUCT(
    rps_30d,
    COALESCE(rps_weighted, rps_30d) AS rps_weighted,
    rps_smoothed,
    purchase_rate_30d,
    purchase_rate_smoothed,
    revenue_30d,
    sends_30d,
    trend_slope,
    COALESCE(stability_score, 0.5) AS stability_score,
    confidence_score
  ) AS performance_features,
  
  -- Statistical features
  STRUCT(
    median_rps,
    p75_rps,
    p25_rps,
    rps_stddev,
    p75_rps - p25_rps AS iqr,
    SAFE_DIVIDE(rps_stddev, NULLIF(median_rps, 0)) AS coefficient_variation
  ) AS statistical_features,
  
  -- Exploration features
  STRUCT(
    ucb_bonus,
    exploration_temperature,
    novelty_bonus,
    sends_30d / (sends_30d + 100) AS smoothing_weight
  ) AS exploration_features,
  
  -- Temporal features
  STRUCT(
    CAST(NULL AS INT64) AS best_hour,  -- Simplified for now
    0.0 AS best_hour_rps,
    CAST(NULL AS INT64) AS best_day_of_week,
    days_since_use,
    last_used_date,
    active_days_30d
  ) AS temporal_features,
  
  -- Cooldown features
  STRUCT(
    base_cooldown_hours,
    fatigue_score,
    CASE
      WHEN days_since_use * 24 < 6 THEN FALSE
      WHEN fatigue_score > 0.8 THEN FALSE
      WHEN sends_30d < 10 THEN TRUE
      ELSE days_since_use * 24 > base_cooldown_hours
    END AS is_eligible
  ) AS cooldown_features,
  
  -- Composite scores
  STRUCT(
    -- Base score (weighted combination)
    0.4 * SAFE_DIVIDE(rps_smoothed, NULLIF(global_avg_rps, 0)) +
    0.3 * COALESCE(stability_score, 0.5) +
    0.2 * (1 - fatigue_score) +
    0.1 * confidence_score AS base_score,
    
    -- Exploration-adjusted score
    (0.4 * SAFE_DIVIDE(rps_smoothed, NULLIF(global_avg_rps, 0)) +
     0.3 * COALESCE(stability_score, 0.5) +
     0.2 * (1 - fatigue_score) +
     0.1 * confidence_score) * (1 + 0.3 * ucb_bonus) AS exploration_score,
    
    -- Normalized percentile scores
    PERCENT_RANK() OVER (PARTITION BY username_page ORDER BY rps_smoothed) AS rps_percentile,
    PERCENT_RANK() OVER (PARTITION BY caption_category ORDER BY rps_smoothed) AS category_percentile
  ) AS composite_scores,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS computed_at,
  DATE(CURRENT_TIMESTAMP()) AS computed_date
  
FROM enriched_features;